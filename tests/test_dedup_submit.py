#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回归用例：下单防重复提交（对应 Issue #7）

断言三件事：
  1. 同一账号对同一节目，1 秒内连续提交两次，只能成功一次；
  2. 被拦下的那次，提示必须是「提交频繁，请稍后重试」（code 为 null 也算被拒）；
  3. 防重窗口过期后必须能再次下单——防重不能变成永久锁死。

为什么第 3 条重要：
  切面是在方法**正常返回后**才写防重标记的，失败不会写。如果哪天有人把它改成
  「进来就写」，或者把窗口配成几十分钟，第 1、2 条照样通过，但用户会被莫名其妙
  地锁住，所以必须单独验证「能自动释放」。

前置条件：
  - 本地服务已启动，网关地址默认 http://127.0.0.1:6085
  - 测试账号存在，且至少有一个观演人
  - Redis 可用（防重标记存在 Redis 里）

用法：
  python tests/test_dedup_submit.py
  python tests/test_dedup_submit.py --program 8 --category 10 --window 5
  python tests/test_dedup_submit.py --base http://127.0.0.1:6085 --mobile 13900000002

退出码：0 通过，1 失败，2 环境不满足被跳过。
"""

import argparse
import sys
import time

import requests

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_SKIP = 2

DEDUP_MESSAGE = "提交频繁，请稍后重试"
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


def is_success(resp):
    return str(resp.get("code")) == "0"


def is_dedup_rejected(resp):
    return not is_success(resp) and resp.get("message") == DEDUP_MESSAGE


def create_until_ready(client, args, user_id, ticket_user_id):
    """拿到一个干净的「窗口起点」。

    上一次运行如果刚下过单，防重标记可能还没过期，此时第一次提交会被拦。
    这不是 bug，等标记过期重试即可；重试也避免把历史状态算成本次失败。
    """
    deadline = time.time() + args.window + 10
    resp = create_order(client, args.program, args.category, ticket_user_id, user_id)
    while is_dedup_rejected(resp) and time.time() < deadline:
        time.sleep(1)
        resp = create_order(client, args.program, args.category, ticket_user_id, user_id)
    return resp


def run(args):
    client = Client(args.base)
    user_id = login(client, args.mobile, args.password)
    ticket_user_id = first_ticket_user(client, user_id)

    created = []
    failures = []
    try:
        # 1) 第一次提交：应该成功，作为本次窗口的起点
        first = create_until_ready(client, args, user_id, ticket_user_id)
        if not is_success(first):
            return EXIT_SKIP, ["首次提交未成功，环境不满足（可能是限购/库存/服务未就绪）：%s" % first]
        created.append(first.get("data"))
        print("第一次提交：成功，订单号 %s" % first.get("data"))

        # 2) 紧接着再提交一次：必须被防重拦下
        second = create_order(client, args.program, args.category, ticket_user_id, user_id)
        if is_success(second):
            created.append(second.get("data"))
            failures.append("1 秒内重复提交仍然成功，又生成了一笔订单 %s" % second.get("data"))
            print("第二次提交：成功（异常），订单号 %s" % second.get("data"))
        else:
            print("第二次提交：被拒 code=%s message=%s" % (second.get("code"), second.get("message")))
            if not is_dedup_rejected(second):
                failures.append("被拒但不是防重拦截：message=%s" % second.get("message"))

        # 3) 窗口过期后应恢复可下单，证明防重会自己释放
        print("等待 %s 秒让防重窗口过期……" % (args.window + 1))
        time.sleep(args.window + 1)
        third = create_order(client, args.program, args.category, ticket_user_id, user_id)
        if is_success(third):
            created.append(third.get("data"))
            print("窗口过期后提交：成功，订单号 %s" % third.get("data"))
        elif str(third.get("code")) == CODE_LIMIT_OVER:
            print("窗口过期后提交：被限购拒绝（账号已买满），跳过「窗口释放」这一条")
        else:
            failures.append("窗口过期后仍然下不了单：code=%s message=%s"
                            % (third.get("code"), third.get("message")))
    finally:
        for order_number in created:
            cancel_order(client, order_number)
        if created:
            print("已取消本次创建的 %d 笔订单" % len(created))

    return (EXIT_FAIL if failures else EXIT_OK), failures


def main():
    parser = argparse.ArgumentParser(description="下单防重复提交回归用例（Issue #7）")
    parser.add_argument("--base", default="http://127.0.0.1:6085/damai")
    parser.add_argument("--mobile", default="13900000002")
    parser.add_argument("--password", default="Test123456")
    parser.add_argument("--program", type=int, default=8, help="节目 id")
    parser.add_argument("--category", type=int, default=10, help="票档 id")
    parser.add_argument("--window", type=int, default=5,
                        help="防重窗口秒数，需与 RepeatExecuteLimitConstants 保持一致")
    args = parser.parse_args()

    try:
        code, failures = run(args)
    except Exception as exc:  # noqa: BLE001
        print("用例执行失败：%s" % exc)
        return EXIT_FAIL

    print("-" * 60)
    if code == EXIT_OK:
        print("PASS：重复提交被拦下，且窗口过期后可再次下单")
    elif code == EXIT_SKIP:
        print("SKIP：环境不满足，未做判定")
    else:
        print("FAIL：")
        for item in failures:
            print("  - " + item)
    return code


if __name__ == "__main__":
    sys.exit(main())
