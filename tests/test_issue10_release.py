"""Run with python tests/test_issue10_release.py; starts an isolated local Redis."""
import json
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LUA = ROOT / 'damai-server/damai-program-service/src/main/resources/lua'
RESERVE = (LUA / 'programDataCreateOrderResolution.lua').read_text(encoding='utf-8')
RELEASE = (LUA / 'programDataReleaseUnsentReservation.lua').read_text(encoding='utf-8')


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

    def reserve(self, order):
        category = [{'programTicketRemainNumberHashKey': 'remain:4:6',
                     'ticketCategoryId': 6, 'ticketCount': 1,
                     'seatNoSoldHashKey': 'available:4:6'}]
        result = self.redis.call('EVAL', RESERVE, 4, '2', 'available:%s:%s', 'locked:%s:%s', '4',
                                 json.dumps(category), '[]', order)
        return json.loads(result)['purchaseSeatList'][0]['id']

    def category(self, seat_id):
        return {'remainHashKey': 'remain:4:6', 'ticketCategoryId': '6',
                'seatLockHashKey': 'locked:4:6', 'seatNoSoldHashKey': 'available:4:6',
                'seatIds': [str(seat_id)]}

    def release(self, order, categories):
        return self.redis.call('EVAL', RELEASE, 1, '#', order, json.dumps(categories))

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
        self.assertEqual(0, self.release('order-a', [self.category(seat)]))
        self.assertEqual('1', self.redis.call('HGET', 'remain:4:6', '6'))

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
