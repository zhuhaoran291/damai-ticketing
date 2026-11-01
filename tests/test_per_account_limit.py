#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回归用例：每账号限购（对应 Issue #8）

断言：同一账号对同一节目，累计购买数量达到「每账号限购」后，下一单必须被拒绝，
且拒绝原因必须是「该账户购买数量大于限制」（50009）。

设计的两个要点：
  1. 不假设账号从 0 开始计数。先查当前已购数，再据此推导期望结果，
     避免历史订单把用例变成「一上来就被拒」的假通过。
  2. 不只看「有没有被拒」，还断言拒绝原因。限购失效时常见的是
     一路放行，另一种误判是被别的规则（库存不足、参数错误）挡住，
     只断言「被拒」会把后一种情况错当成修复成功。

前置条件：
  - 本地服务已启动，网关地址默认 http://127.0.0.1:6085
  - 测试账号存在，且至少有一个观演人

用法：
  python tests/test_per_account_limit.py
  python tests/test_per_account_limit.py --program 8 --category 10 --limit 6
  python tests/test_per_account_limit.py --base http://127.0.0.1:6085 --mobile 13900000002

退出码：0 通过，1 失败，2 环境不满足被跳过。
"""

import argparse
import sys

import requests

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_SKIP = 2

CODE_LIMIT_OVER = "50009"  # BaseCode.PER_ACCOUNT_PURCHASE_COUNT_OVER_LIMIT


class Client:

    def __init__(self, base):
        self.base = base.rstrip("/")
        self.session = requests.Session()
        self.token = None

    def post(self, path, body=None, timeout=20):
        headers = {"no_verify": "true"}
        if self.token:
            headers["token"] = self.token
        resp = self.session.post(self.base + path, json=body, headers=headers, timeout=timeout)
        try:
            return resp.json()
        except ValueError:
            return {"http": resp.status_code, "text": resp.text[:300]}


def login(client, mobile, password):
    resp = client.post("/user/user/login", {"code": "0001", "mobile": mobile, "password": password})
    if str(resp.get("code")) != "0":
        raise RuntimeError("登录失败：%s" % resp)
    client.token = resp["data"]["token"]
    return int(resp["data"]["userId"])


def first_ticket_user(client, user_id):
    resp = client.post("/user/ticket/user/list", {"userId": user_id})
    users = resp.get("data") or []
    if not users:
        raise RuntimeError("测试账号没有观演人，请先创建")
    return int(users[0]["id"])


def purchased_count(client, user_id, program_id):
    resp = client.post("/order/order/account/order/count", {"userId": user_id, "programId": program_id})
    data = resp.get("data") or {}
    return int(data.get("count") or 0)


def create_order(client, program_id, category_id, ticket_user_id, user_id):
    return client.post("/program/program/order/create/v1", {
        "programId": program_id,
        "userId": user_id,
        "ticketUserIdList": [ticket_user_id],
        "ticketCategoryId": category_id,
        "ticketCount": 1,
    })


def cancel_order(client, order_number):
    if not order_number:
        return
    client.post("/order/order/cancel", {"orderNumber": order_number})


def run(args):
    client = Client(args.base)
    user_id = login(client, args.mobile, args.password)
    ticket_user_id = first_ticket_user(client, user_id)

    before = purchased_count(client, user_id, args.program)
    print("账号 %s（userId=%s）在节目 %s 的初始已购：%s 张，限购 %s 张"
          % (args.mobile, user_id, args.program, before, args.limit))

    if before >= args.limit:
        print("SKIP：初始已购已达上限，请先取消该账号在该节目的历史订单再运行本用例")
        return EXIT_SKIP, []

    created = []
    reject_code = None
    reject_message = None
    try:
        # 多给两次机会，容忍「计数缓存短暂落后于数据库」的情况
        for i in range(args.limit - before + 2):
            resp = create_order(client, args.program, args.category, ticket_user_id, user_id)
            code = str(resp.get("code"))
            if code == "0":
                created.append(resp.get("data"))
                print("  第 %d 次下单：成功，订单号 %s" % (i + 1, resp.get("data")))
            else:
                reject_code = code
                reject_message = resp.get("message")
                print("  第 %d 次下单：被拒 code=%s message=%s" % (i + 1, code, reject_message))
                break
    finally:
        for order_number in created:
            cancel_order(client, order_number)
        if created:
            print("已取消本次创建的 %d 笔订单" % len(created))

    failures = []
    if reject_code is None:
        failures.append("连续下单始终没有被拒绝：限购规则未生效")
    elif reject_code != CODE_LIMIT_OVER:
        failures.append("被拒但原因不对：期望 %s，实际 %s（%s）"
                        % (CODE_LIMIT_OVER, reject_code, reject_message))

    allowed = len(created)
    if allowed + before > args.limit:
        failures.append("成功下单 %d 笔，加上初始已购 %d 笔超过限购 %d"
                        % (allowed, before, args.limit))

    return (EXIT_FAIL if failures else EXIT_OK), failures


def main():
    parser = argparse.ArgumentParser(description="每账号限购回归用例（Issue #8）")
    parser.add_argument("--base", default="http://127.0.0.1:6085/damai")
    parser.add_argument("--mobile", default="13900000002")
    parser.add_argument("--password", default="Test123456")
    parser.add_argument("--program", type=int, default=8, help="被限购的节目 id")
    parser.add_argument("--category", type=int, default=10, help="票档 id")
    parser.add_argument("--limit", type=int, default=6, help="该节目的每账号限购数")
    args = parser.parse_args()

    try:
        code, failures = run(args)
    except Exception as exc:  # noqa: BLE001
        print("用例执行失败：%s" % exc)
        return EXIT_FAIL

    print("-" * 60)
    if code == EXIT_OK:
        print("PASS：累计达到 %s 张后，下一单被正确拒绝" % args.limit)
    elif code == EXIT_SKIP:
        print("SKIP：环境不满足，未做判定")
    else:
        print("FAIL：")
        for item in failures:
            print("  - " + item)
    return code


if __name__ == "__main__":
    sys.exit(main())
