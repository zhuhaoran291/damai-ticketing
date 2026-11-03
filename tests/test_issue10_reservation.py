"""Exercise the V4 reservation script against a disposable local Redis server."""

import concurrent.futures
import json
from pathlib import Path
import shutil
import socket
import subprocess
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "damai-server/damai-program-service/src/main/resources/lua/programDataCreateOrderResolution.lua"


@unittest.skipUnless(shutil.which("redis-server") and shutil.which("redis-cli"), "Redis tools unavailable")
class ReservationRecoveryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            cls.port = sock.getsockname()[1]
        cls.server = subprocess.Popen(
            ["redis-server", "--port", str(cls.port), "--bind", "127.0.0.1",
             "--save", "", "--appendonly", "no"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for _ in range(50):
            try:
                if cls.redis("PING") == "PONG":
                    break
            except subprocess.CalledProcessError:
                time.sleep(0.1)
        else:
            cls.server.terminate()
            cls.server.wait(timeout=5)
            raise RuntimeError("Could not start disposable Redis")
        cls.lua = SCRIPT.read_text(encoding="utf-8")

    @classmethod
    def tearDownClass(cls):
        cls.server.terminate()
        cls.server.wait(timeout=5)

    @classmethod
    def redis(cls, *args):
        return subprocess.check_output(
            ["redis-cli", "-p", str(cls.port), "--raw", *map(str, args)],
            text=True, stderr=subprocess.DEVNULL,
        ).strip()

    def setup_seat(self, program):
        seat = {"id": 1, "ticketCategoryId": 7, "rowCode": 1, "colCode": 1,
                "sellStatus": 1, "price": 10}
        remain = f"remain:{program}:7"
        free = f"seat:free:{program}:7"
        self.redis("HSET", remain, "7", "1")
        self.redis("HSET", free, "1", json.dumps(seat))
        return [{"programTicketRemainNumberHashKey": remain, "ticketCategoryId": 7,
                 "ticketCount": 1, "seatNoSoldHashKey": free}]

    def test_concurrent_duplicates_reserve_once_and_reuse_pending_order(self):
        ticket = self.setup_seat(100)
        request = {"programId": 100, "userId": 9, "ticketCategoryId": 7,
                   "ticketCount": 1, "ticketUserIdList": [11]}
        keys = ["2", "seat:free:%s:%s", "seat:lock:%s:%s", "100", "pending", "active"]

        def reserve(number):
            return json.loads(self.redis("EVAL", self.lua, "6", *keys, json.dumps(ticket),
                                         "[]", json.dumps(request), number, "9:100"))

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            results = list(pool.map(reserve, range(1000, 1010)))
        winners = [result for result in results if "purchaseSeatList" in result]
        pending_ids = {result["pendingOrderNumber"] for result in results if "pendingOrderNumber" in result}
        self.assertEqual(1, len(winners))
        self.assertEqual(1, len(pending_ids))
        order_number = pending_ids.pop()
        self.assertEqual("1", self.redis("HLEN", "pending"))
        self.assertEqual("0", self.redis("HGET", "remain:100:7", "7"))
        stored = json.loads(self.redis("HGET", "pending", order_number))
        self.assertEqual(9, stored["request"]["userId"])
        self.assertEqual(1, stored["seats"][0]["id"])

    def test_legacy_reservation_does_not_require_pending_keys(self):
        ticket = self.setup_seat(200)
        result = json.loads(self.redis("EVAL", self.lua, "4", "2", "seat:free:%s:%s",
                                       "seat:lock:%s:%s", "200", json.dumps(ticket), "[]"))
        self.assertEqual(0, result["code"])
        self.assertEqual(1, len(result["purchaseSeatList"]))

    def test_request_id_still_reuses_order_after_pending_record_is_cleared(self):
        ticket = self.setup_seat(300)
        request = {"programId": 300, "userId": 9, "ticketCategoryId": 7,
                   "ticketCount": 1, "ticketUserIdList": [11], "requestId": "request-1"}
        keys = ["2", "seat:free:%s:%s", "seat:lock:%s:%s", "300",
                "pending-300", "active-300", "request-300"]
        args = [json.dumps(ticket), "[]", json.dumps(request), "123", "9:300"]
        first = json.loads(self.redis("EVAL", self.lua, "7", *keys, *args))
        self.assertEqual(0, first["code"])
        self.redis("HDEL", "pending-300", "123")
        self.redis("HDEL", "active-300", "9:300")
        args[3] = "456"
        repeated = json.loads(self.redis("EVAL", self.lua, "7", *keys, *args))
        self.assertEqual("123", repeated["pendingOrderNumber"])
        self.assertEqual("0", self.redis("HGET", "remain:300:7", "7"))

    def test_invalid_pending_key_does_not_consume_stock(self):
        ticket = self.setup_seat(400)
        self.redis("SET", "invalid-pending", "not-a-hash")
        keys = ["2", "seat:free:%s:%s", "seat:lock:%s:%s", "400", "invalid-pending", "active-400"]
        result = self.redis("EVAL", self.lua, "6", *keys, json.dumps(ticket), "[]", "{}", "123", "9:400")
        self.assertIn("pending order key must be a hash", result)
        self.assertEqual("1", self.redis("HGET", "remain:400:7", "7"))

    def test_second_request_id_also_remembers_the_existing_pending_order(self):
        ticket = self.setup_seat(500)
        request = {"programId": 500, "userId": 9, "ticketCategoryId": 7,
                   "ticketCount": 1, "ticketUserIdList": [11]}
        keys = ["2", "seat:free:%s:%s", "seat:lock:%s:%s", "500",
                "pending-500", "active-500", "request-first"]
        args = [json.dumps(ticket), "[]", json.dumps(request), "123", "9:500"]
        self.assertEqual(0, json.loads(self.redis("EVAL", self.lua, "7", *keys, *args))["code"])
        keys[6] = "request-second"
        args[3] = "456"
        repeated = json.loads(self.redis("EVAL", self.lua, "7", *keys, *args))
        self.assertEqual("123", repeated["pendingOrderNumber"])
        self.redis("HDEL", "pending-500", "123")
        self.redis("HDEL", "active-500", "9:500")
        self.assertEqual("123", self.redis("GET", "request-second"))


if __name__ == "__main__":
    unittest.main()
