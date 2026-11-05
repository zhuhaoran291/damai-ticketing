"""Run with python tests/test_issue10_release.py; starts an isolated local Redis."""
import json
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LUA = ROOT / 'damai-server/damai-program-service/src/main/resources/lua'
RESERVE = (LUA / 'programDataCreateOrderResolution.lua').read_text(encoding='utf-8')
RELEASE = (LUA / 'programDataReleaseUnsentReservation.lua').read_text(encoding='utf-8')
LEDGER_LUA = ROOT / 'damai-redis-tool-framework/damai-redis-framework/src/main/resources/lua'
CLAIM = (LEDGER_LUA / 'orderReservationClaim.lua').read_text(encoding='utf-8')
COMPLETE = (LEDGER_LUA / 'orderReservationComplete.lua').read_text(encoding='utf-8')
SEND_STATUS = (LEDGER_LUA / 'orderReservationSendStatus.lua').read_text(encoding='utf-8')
FAILURE = (LEDGER_LUA / 'orderReservationConsumeFailure.lua').read_text(encoding='utf-8')


class Redis:
    def __init__(self, port):
        self.port = port

    def call(self, *args):
        payload = b'*%d\r\n' % len(args)
        for arg in args:
            value = str(arg).encode('utf-8')
            payload += b'$%d\r\n' % len(value) + value + b'\r\n'
        with socket.create_connection(('127.0.0.1', self.port), timeout=3) as sock:
            sock.sendall(payload)
            stream = sock.makefile('rb')
            line = stream.readline()
            if not line:
                raise RuntimeError('Redis returned an empty response')
            kind, content = line[:1], line[1:-2]
            if kind == b'-':
                raise RuntimeError(content.decode())
            if kind == b':':
                return int(content)
            if kind == b'$':
                length = int(content)
                return None if length < 0 else stream.read(length).decode()
            if kind == b'+':
                return content.decode()
            raise RuntimeError('Unsupported Redis response')


@unittest.skipUnless(shutil.which('redis-server'), 'redis-server is required')
class ReservationReleaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.TemporaryDirectory(prefix='issue10-redis-')
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            cls.port = sock.getsockname()[1]
        cls.server = subprocess.Popen([
            shutil.which('redis-server'), '--port', str(cls.port), '--bind', '127.0.0.1',
            '--save', '', '--appendonly', 'no', '--dir', cls.tempdir.name,
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        cls.redis = Redis(cls.port)
        for _ in range(100):
            if cls.server.poll() is not None:
                raise RuntimeError('isolated Redis exited on startup')
            try:
                if cls.redis.call('PING') == 'PONG':
                    return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError('isolated Redis did not start')

    @classmethod
    def tearDownClass(cls):
        cls.server.terminate()
        cls.server.wait(timeout=5)
        cls.tempdir.cleanup()

    def setUp(self):
        self.redis.call('FLUSHDB')  # private, freshly started Redis process
        self.redis.call('HSET', 'remain:4:6', '6', 2)
        for sid in (81, 82):
            seat = {'id': sid, 'programId': 4, 'ticketCategoryId': 6,
                    'rowCode': 1, 'colCode': sid - 80, 'price': 100, 'sellStatus': 1}
            self.redis.call('HSET', 'available:4:6', sid, json.dumps(seat))

    def reserve(self, order, timeout_ms=30000):
        category = [{'programTicketRemainNumberHashKey': 'remain:4:6',
                     'ticketCategoryId': 6, 'ticketCount': 1,
                     'seatNoSoldHashKey': 'available:4:6'}]
        request = json.dumps({'programId': 4, 'userId': 10000, 'ticketUserIdList': [9000001]})
        result = self.redis.call('EVAL', RESERVE, 6, '2', 'available:%s:%s', 'locked:%s:%s', '4',
                                 'ledger', 'due', json.dumps(category), '[]', order, request, timeout_ms)
        return json.loads(result)['purchaseSeatList'][0]['id']

    def category(self, seat_id):
        return {'remainHashKey': 'remain:4:6', 'ticketCategoryId': '6',
                'seatLockHashKey': 'locked:4:6', 'seatNoSoldHashKey': 'available:4:6',
                'seatIds': [str(seat_id)]}

    def release(self, order, categories, reason='PRE_SEND_FAILURE'):
        return self.redis.call('EVAL', RELEASE, 2, 'ledger', 'due', order, json.dumps(categories), reason)

    def claim(self, order):
        return self.redis.call('EVAL', CLAIM, 1, 'ledger', order)

    def state(self, order):
        return json.loads(self.redis.call('HGET', 'ledger', order))

    def test_release_twice_and_old_release_after_new_order(self):
        first = '2118147077585252356'  # keep the 64-bit ID as a string in Lua
        second = '2118147077585252357'
        seat = self.reserve(first)
        locked = json.loads(self.redis.call('HGET', 'locked:4:6', seat))
        self.assertEqual(first, locked['reservationOrderNumber'])
        self.assertEqual(1, self.release(first, [self.category(seat)]))
        self.assertEqual(0, self.release(first, [self.category(seat)]))
        self.assertEqual('2', self.redis.call('HGET', 'remain:4:6', '6'))
        reserved_again = self.reserve(second)
        self.assertEqual(seat, reserved_again)
        self.assertEqual(0, self.release(first, [self.category(seat)]))
        self.assertEqual('1', self.redis.call('HGET', 'remain:4:6', '6'))
        locked = json.loads(self.redis.call('HGET', 'locked:4:6', seat))
        self.assertEqual(second, locked['reservationOrderNumber'])
        self.assertEqual(1, self.release(second, [self.category(seat)]))
        self.assertEqual('2', self.redis.call('HGET', 'remain:4:6', '6'))

    def test_invalid_later_category_does_not_partially_release(self):
        seat = self.reserve('order-a')
        bad = {'remainHashKey': 'missing', 'ticketCategoryId': '7',
               'seatLockHashKey': 'locked:4:7', 'seatNoSoldHashKey': 'available:4:7',
               'seatIds': ['1']}
        with self.assertRaises(RuntimeError):
            self.release('order-a', [self.category(seat), bad])
        self.assertEqual('1', self.redis.call('HGET', 'remain:4:6', '6'))
        self.assertIsNotNone(self.redis.call('HGET', 'locked:4:6', seat))

    def test_legacy_v3_reservation_does_not_match_v4_owner(self):
        category = [{'programTicketRemainNumberHashKey': 'remain:4:6',
                     'ticketCategoryId': 6, 'ticketCount': 1,
                     'seatNoSoldHashKey': 'available:4:6'}]
        result = self.redis.call('EVAL', RESERVE, 4, '2', 'available:%s:%s', 'locked:%s:%s', '4',
                                 json.dumps(category), '[]')
        seat = json.loads(result)['purchaseSeatList'][0]['id']
        locked = json.loads(self.redis.call('HGET', 'locked:4:6', seat))
        self.assertNotIn('reservationOrderNumber', locked)
        self.assertEqual(-1, self.release('order-a', [self.category(seat)]))
        self.assertEqual('1', self.redis.call('HGET', 'remain:4:6', '6'))

    def test_expired_unknown_closes_and_late_consumer_cannot_create(self):
        order = '2118147077585252390'
        seat = self.reserve(order, 80)
        record = self.state(order)
        self.assertEqual('PENDING', record['state'])
        self.assertEqual('NOT_STARTED', record['sendStatus'])
        time.sleep(0.12)
        self.assertEqual(0, self.claim(order))  # expired but not yet closed
        self.assertEqual(1, self.release(order, [self.category(seat)], 'EXPIRED'))
        self.assertEqual('CANCELLED', self.state(order)['state'])
        self.assertEqual(0, self.claim(order))  # late Kafka message is fenced
        self.assertEqual('2', self.redis.call('HGET', 'remain:4:6', '6'))

    def test_consumer_claim_before_deadline_prevents_expiry_release(self):
        order = '2118147077585252391'
        seat = self.reserve(order, 150)
        self.assertEqual(1, self.claim(order))
        self.assertEqual('PROCESSING', self.state(order)['state'])
        time.sleep(0.2)
        self.assertEqual(-1, self.release(order, [self.category(seat)], 'EXPIRED'))
        self.assertEqual(1, self.claim(order))  # recovery may retry the same order
        self.assertEqual(1, self.redis.call('EVAL', COMPLETE, 2, 'ledger', 'due', order))
        self.assertEqual('COMPLETED', self.state(order)['state'])
        self.assertEqual(0, self.claim(order))  # duplicate is ignored after completion
        self.assertEqual('1', self.redis.call('HGET', 'remain:4:6', '6'))

    def test_consumer_failure_is_recorded_and_can_be_completed_after_retry(self):
        order = '2118147077585252394'
        self.reserve(order)
        self.assertEqual(1, self.claim(order))
        self.redis.call('EVAL', FAILURE, 1, 'ledger', order)
        self.assertEqual('FAILED_RETRYABLE', self.state(order)['consumeStatus'])
        self.assertEqual(1, self.claim(order))
        self.assertEqual(1, self.redis.call('EVAL', COMPLETE, 2, 'ledger', 'due', order))
        self.redis.call('EVAL', FAILURE, 1, 'ledger', order)
        self.assertEqual('SUCCEEDED', self.state(order)['consumeStatus'])

    def test_claim_and_close_are_mutually_exclusive(self):
        order = '2118147077585252393'
        seat = self.reserve(order)
        barrier = Barrier(2)
        def claim():
            barrier.wait()
            return self.claim(order)
        def close():
            barrier.wait()
            return self.release(order, [self.category(seat)])
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(claim)
            second = pool.submit(close)
            claim_result, close_result = first.result(), second.result()
        state = self.state(order)['state']
        if state == 'PROCESSING':
            self.assertEqual((1, -1), (claim_result, close_result))
            self.assertEqual('1', self.redis.call('HGET', 'remain:4:6', '6'))
        else:
            self.assertEqual('CANCELLED', state)
            self.assertEqual((0, 1), (claim_result, close_result))
            self.assertEqual('2', self.redis.call('HGET', 'remain:4:6', '6'))

    def test_send_status_does_not_regress_after_ack_or_close(self):
        order = '2118147077585252392'
        seat = self.reserve(order)
        self.redis.call('EVAL', SEND_STATUS, 1, 'ledger', order, 'ACKNOWLEDGED')
        self.redis.call('EVAL', SEND_STATUS, 1, 'ledger', order, 'UNKNOWN')
        self.assertEqual('ACKNOWLEDGED', self.state(order)['sendStatus'])
        self.release(order, [self.category(seat)])
        self.redis.call('EVAL', SEND_STATUS, 1, 'ledger', order, 'UNKNOWN')
        self.assertEqual('CANCELLED', self.state(order)['state'])
        self.assertEqual('ACKNOWLEDGED', self.state(order)['sendStatus'])

    def test_duplicate_seat_ids_cannot_increase_stock_twice(self):
        seat = self.reserve('order-a')
        category = self.category(seat)
        category['seatIds'].append(str(seat))
        with self.assertRaises(RuntimeError):
            self.release('order-a', [category])
        self.assertEqual('1', self.redis.call('HGET', 'remain:4:6', '6'))
        self.assertIsNotNone(self.redis.call('HGET', 'locked:4:6', seat))


if __name__ == '__main__':
    unittest.main(verbosity=2)
