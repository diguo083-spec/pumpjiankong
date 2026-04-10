
import base64
import hashlib
import hmac
import json
import os
import threading
import time
import urllib.parse
from datetime import datetime, timezone, timedelta

import pymysql
import requests
import websocket
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

from condition_query import (
    find_condition_candidates,
    find_first_condition_candidate_for_mint,
    find_second_condition_candidates,
    find_first_second_condition_candidate_for_mint,
)

# =========================
# 基础配置
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

API_KEY = os.getenv("HELIUS_API_KEY", "").strip()
WS_URL = f"wss://mainnet.helius-rpc.com/?api-key={API_KEY}"
RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={API_KEY}"

PUMP_PROGRAM_ID = os.getenv("PUMP_PROGRAM_ID", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").strip()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1").strip()
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root").strip()
DB_PASSWORD = os.getenv("DB_PASSWORD", "").strip()
DB_NAME = os.getenv("DB_NAME", "pump_monitor").strip()

WEB_PORT = int(os.getenv("WEB_PORT", "5001"))

DINGTALK_WEBHOOK = os.getenv("DINGTALK_WEBHOOK", "").strip()
DINGTALK_SECRET = os.getenv("DINGTALK_SECRET", "").strip()
DINGTALK_KEYWORD = os.getenv("DINGTALK_KEYWORD", "").strip()

SECOND_DINGTALK_WEBHOOK = os.getenv("SECOND_DINGTALK_WEBHOOK", "").strip()
SECOND_DINGTALK_SECRET = os.getenv("SECOND_DINGTALK_SECRET", "").strip()
SECOND_DINGTALK_KEYWORD = os.getenv("SECOND_DINGTALK_KEYWORD", "").strip()

app = Flask(__name__)


def now_dt():
    return datetime.now()


def now_str():
    return now_dt().strftime("%Y-%m-%d %H:%M:%S")


def parse_dt(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, str):
        return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
    return None


def utc_ms_to_local_str(ms):
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return now_str()


def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def lamports_to_sol(x):
    return (x or 0) / 1_000_000_000


def get_conn(db_name=DB_NAME):
    kwargs = {
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "charset": "utf8mb4",
        "autocommit": False,
        "cursorclass": pymysql.cursors.DictCursor
    }
    if db_name:
        kwargs["database"] = db_name
    return pymysql.connect(**kwargs)


def ensure_column_exists(conn, table_name, column_name, alter_sql):
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM `{table_name}` LIKE %s", (column_name,))
        row = cur.fetchone()
        if row is None:
            cur.execute(alter_sql)


def init_db():
    conn = get_conn(None)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
        conn.commit()
    finally:
        conn.close()

    conn = get_conn(DB_NAME)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS token_stats (
                    mint VARCHAR(100) PRIMARY KEY,
                    creator VARCHAR(100) NULL,
                    created_time DATETIME NULL,
                    created_slot BIGINT NULL,
                    settled_60s TINYINT(1) DEFAULT 0,
                    pushed_dingtalk TINYINT(1) DEFAULT 0,
                    first_seen DATETIME NULL,
                    last_seen DATETIME NULL,
                    buy_count INT DEFAULT 0,
                    sell_count INT DEFAULT 0,
                    buy_sol DOUBLE DEFAULT 0,
                    sell_sol DOUBLE DEFAULT 0,
                    updated_at DATETIME NULL,
                    INDEX idx_creator (creator),
                    INDEX idx_created_time (created_time),
                    INDEX idx_created_slot (created_slot),
                    INDEX idx_settled_60s (settled_60s),
                    INDEX idx_pushed_dingtalk (pushed_dingtalk),
                    INDEX idx_last_seen (last_seen)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    signature VARCHAR(120) UNIQUE,
                    event_time DATETIME NULL,
                    slot BIGINT NULL,
                    side VARCHAR(10) NULL,
                    signer VARCHAR(100) NULL,
                    mint VARCHAR(100) NULL,
                    token_amount DOUBLE DEFAULT 0,
                    sol_amount DOUBLE DEFAULT 0,
                    price_sol DOUBLE DEFAULT 0,
                    gas_fee_sol DOUBLE DEFAULT 0,
                    created_at DATETIME NULL,
                    INDEX idx_trades_mint (mint),
                    INDEX idx_trades_event_time (event_time),
                    INDEX idx_trades_signature (signature),
                    INDEX idx_trades_slot (slot),
                    INDEX idx_trades_signer (signer),
                    INDEX idx_trades_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS failed_trades (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    signature VARCHAR(120) UNIQUE,
                    mint VARCHAR(100) NULL,
                    event_time DATETIME NULL,
                    slot BIGINT NULL,
                    signer VARCHAR(100) NULL,
                    gas_fee_sol DOUBLE DEFAULT 0,
                    created_at DATETIME NULL,
                    INDEX idx_failed_mint (mint),
                    INDEX idx_failed_event_time (event_time),
                    INDEX idx_failed_slot (slot),
                    INDEX idx_failed_signer (signer)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS failed_trades_v2 (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    signature VARCHAR(120) NOT NULL,
                    mint VARCHAR(100) NOT NULL,
                    event_time DATETIME NULL,
                    slot BIGINT NULL,
                    signer VARCHAR(100) NULL,
                    gas_fee_sol DOUBLE DEFAULT 0,
                    created_at DATETIME NULL,
                    UNIQUE KEY uniq_failed_sig_mint (signature, mint),
                    INDEX idx_failed_v2_mint (mint),
                    INDEX idx_failed_v2_event_time (event_time),
                    INDEX idx_failed_v2_slot (slot),
                    INDEX idx_failed_v2_signer (signer),
                    INDEX idx_failed_v2_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS failed_trade_tasks (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    mint VARCHAR(100) NOT NULL,
                    created_slot BIGINT NOT NULL,
                    end_slot BIGINT NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    retries INT DEFAULT 0,
                    last_scan_slot BIGINT NULL,
                    last_error VARCHAR(255) NULL,
                    created_at DATETIME NULL,
                    updated_at DATETIME NULL,
                    UNIQUE KEY uniq_failed_task_mint (mint),
                    INDEX idx_failed_task_status (status),
                    INDEX idx_failed_task_created_slot (created_slot),
                    INDEX idx_failed_task_end_slot (end_slot)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS buyer_profit_60s (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    mint VARCHAR(100) NOT NULL,
                    buyer VARCHAR(100) NOT NULL,
                    created_slot BIGINT NULL,
                    buy_sol DOUBLE DEFAULT 0,
                    sell_sol DOUBLE DEFAULT 0,
                    profit_sol DOUBLE DEFAULT 0,
                    window_start DATETIME NULL,
                    window_end DATETIME NULL,
                    settled_at DATETIME NULL,
                    created_at DATETIME NULL,
                    UNIQUE KEY uniq_mint_buyer (mint, buyer),
                    INDEX idx_buyer_profit_mint (mint),
                    INDEX idx_buyer_profit_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS push_config (
                    id INT PRIMARY KEY,
                    min_profit_sol DOUBLE DEFAULT 0,
                    min_gas_sol DOUBLE DEFAULT 0,
                    dingtalk_enabled TINYINT(1) DEFAULT 1,
                    updated_at DATETIME NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)


            cur.execute("""
                CREATE TABLE IF NOT EXISTS second_push_config (
                    id INT PRIMARY KEY,
                    min_profit_sol DOUBLE DEFAULT 0,
                    min_gas_sol DOUBLE DEFAULT 0,
                    dingtalk_enabled TINYINT(1) DEFAULT 1,
                    updated_at DATETIME NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS blacklist_addresses (
                    address VARCHAR(100) PRIMARY KEY,
                    note VARCHAR(255) NULL,
                    created_at DATETIME NULL,
                    updated_at DATETIME NULL,
                    INDEX idx_blacklist_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS cleanup_config (
                    id INT PRIMARY KEY,
                    cleanup_hours DOUBLE DEFAULT 24,
                    updated_at DATETIME NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

        ensure_column_exists(
            conn,
            "token_stats",
            "creator",
            "ALTER TABLE token_stats ADD COLUMN creator VARCHAR(100) NULL AFTER mint"
        )
        ensure_column_exists(
            conn,
            "token_stats",
            "created_time",
            "ALTER TABLE token_stats ADD COLUMN created_time DATETIME NULL AFTER creator"
        )
        ensure_column_exists(
            conn,
            "token_stats",
            "created_slot",
            "ALTER TABLE token_stats ADD COLUMN created_slot BIGINT NULL AFTER created_time"
        )
        ensure_column_exists(
            conn,
            "token_stats",
            "settled_60s",
            "ALTER TABLE token_stats ADD COLUMN settled_60s TINYINT(1) DEFAULT 0 AFTER created_slot"
        )
        ensure_column_exists(
            conn,
            "token_stats",
            "pushed_dingtalk",
            "ALTER TABLE token_stats ADD COLUMN pushed_dingtalk TINYINT(1) DEFAULT 0 AFTER settled_60s"
        )
        ensure_column_exists(
            conn,
            "token_stats",
            "pushed_second_dingtalk",
            "ALTER TABLE token_stats ADD COLUMN pushed_second_dingtalk TINYINT(1) DEFAULT 0 AFTER pushed_dingtalk"
        )
        ensure_column_exists(
            conn,
            "trades",
            "slot",
            "ALTER TABLE trades ADD COLUMN slot BIGINT NULL AFTER event_time"
        )
        ensure_column_exists(
            conn,
            "trades",
            "gas_fee_sol",
            "ALTER TABLE trades ADD COLUMN gas_fee_sol DOUBLE DEFAULT 0 AFTER price_sol"
        )

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO push_config (id, min_profit_sol, min_gas_sol, dingtalk_enabled, updated_at)
                VALUES (1, 0, 0, 1, %s)
                ON DUPLICATE KEY UPDATE id = id
            """, (now_str(),))
            cur.execute("""
                INSERT INTO second_push_config (id, min_profit_sol, min_gas_sol, dingtalk_enabled, updated_at)
                VALUES (1, 0, 0, 1, %s)
                ON DUPLICATE KEY UPDATE id = id
            """, (now_str(),))
            cur.execute("""
                INSERT INTO cleanup_config (id, cleanup_hours, updated_at)
                VALUES (1, 24, %s)
                ON DUPLICATE KEY UPDATE id = id
            """, (now_str(),))

        conn.commit()
    finally:
        conn.close()


def get_push_config(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, min_profit_sol, min_gas_sol, dingtalk_enabled, updated_at
            FROM push_config
            WHERE id = 1
        """)
        row = cur.fetchone()
        if row:
            return row
        return {
            "id": 1,
            "min_profit_sol": 0,
            "min_gas_sol": 0,
            "dingtalk_enabled": 1,
            "updated_at": None
        }


def update_push_config(conn, min_profit_sol, min_gas_sol, dingtalk_enabled):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO push_config (id, min_profit_sol, min_gas_sol, dingtalk_enabled, updated_at)
            VALUES (1, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                min_profit_sol = VALUES(min_profit_sol),
                min_gas_sol = VALUES(min_gas_sol),
                dingtalk_enabled = VALUES(dingtalk_enabled),
                updated_at = VALUES(updated_at)
        """, (min_profit_sol, min_gas_sol, dingtalk_enabled, now_str()))



def get_second_push_config(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, min_profit_sol, min_gas_sol, dingtalk_enabled, updated_at
            FROM second_push_config
            WHERE id = 1
        """)
        row = cur.fetchone()
        if row:
            return row
        return {
            "id": 1,
            "min_profit_sol": 0,
            "min_gas_sol": 0,
            "dingtalk_enabled": 1,
            "updated_at": None
        }


def update_second_push_config(conn, min_profit_sol, min_gas_sol, dingtalk_enabled):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO second_push_config (id, min_profit_sol, min_gas_sol, dingtalk_enabled, updated_at)
            VALUES (1, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                min_profit_sol = VALUES(min_profit_sol),
                min_gas_sol = VALUES(min_gas_sol),
                dingtalk_enabled = VALUES(dingtalk_enabled),
                updated_at = VALUES(updated_at)
        """, (min_profit_sol, min_gas_sol, dingtalk_enabled, now_str()))


def get_cleanup_config(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, cleanup_hours, updated_at
            FROM cleanup_config
            WHERE id = 1
        """)
        row = cur.fetchone()
        if row:
            return row
        return {"id": 1, "cleanup_hours": 24, "updated_at": None}


def update_cleanup_config(conn, cleanup_hours):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cleanup_config (id, cleanup_hours, updated_at)
            VALUES (1, %s, %s)
            ON DUPLICATE KEY UPDATE
                cleanup_hours = VALUES(cleanup_hours),
                updated_at = VALUES(updated_at)
        """, (cleanup_hours, now_str()))


def list_blacklist_addresses(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT address, note, created_at, updated_at
            FROM blacklist_addresses
            ORDER BY created_at DESC, address ASC
        """)
        return cur.fetchall()


def add_blacklist_address(conn, address, note=""):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO blacklist_addresses (address, note, created_at, updated_at)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                note = VALUES(note),
                updated_at = VALUES(updated_at)
        """, (address, note or None, now_str(), now_str()))


def delete_blacklist_address(conn, address):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM blacklist_addresses WHERE address = %s", (address,))
        return cur.rowcount


def build_dingtalk_url_with_secret(webhook, secret):
    if not webhook:
        raise RuntimeError("未配置钉钉 Webhook")

    if not secret:
        return webhook

    timestamp = str(int(time.time() * 1000))
    string_to_sign = f"{timestamp}\n{secret}"
    hmac_code = hmac.new(
        secret.encode("utf-8"),
        string_to_sign.encode("utf-8"),
        digestmod=hashlib.sha256
    ).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    return f"{webhook}&timestamp={timestamp}&sign={sign}"


def send_dingtalk_text_to_robot(content, webhook, secret="", keyword=""):
    url = build_dingtalk_url_with_secret(webhook, secret)
    final_content = f"{keyword}\n{content}" if keyword else content
    payload = {"msgtype": "text", "text": {"content": final_content}}
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if data.get("errcode") != 0:
        raise RuntimeError(f"钉钉返回异常: {data}")
    return data


def send_dingtalk_text(content):
    return send_dingtalk_text_to_robot(
        content=content,
        webhook=DINGTALK_WEBHOOK,
        secret=DINGTALK_SECRET,
        keyword=DINGTALK_KEYWORD,
    )


def send_condition_match_dingtalk(candidate, min_profit_sol, min_gas_sol):
    content = (
        "Pump 条件命中提醒\n"
        f"代币合约: {candidate['mint']}\n"
        f"创建者: {candidate['creator']}\n"
        f"创建时间: {candidate['created_time']}\n"
        f"创建区块: {candidate['created_slot']}\n"
        f"符合条件的交易者: {candidate['buyer']}\n"
        f"利润(SOL): {candidate['profit_sol']}\n"
        f"首笔买入Gas(SOL): {candidate['gas_sol']}\n"
        f"区块: {candidate['buyer_slot']}\n"
        f"首笔买入时间: {candidate['first_buy_time']}\n"
        f"当前推送阈值 => 最低利润: {min_profit_sol}, 最低首笔Gas: {min_gas_sol}"
    )
    return send_dingtalk_text(content)


def process_dingtalk_push_for_mint(conn, mint):
    cfg = get_push_config(conn)

    if not int(cfg.get("dingtalk_enabled") or 0):
        return

    with conn.cursor() as cur:
        cur.execute("""
            SELECT pushed_dingtalk
            FROM token_stats
            WHERE mint = %s
        """, (mint,))
        row = cur.fetchone()

    if not row:
        return
    if int(row.get("pushed_dingtalk") or 0) == 1:
        return

    candidate = find_first_condition_candidate_for_mint(
        conn=conn,
        mint=mint,
        min_profit_sol=float(cfg.get("min_profit_sol") or 0),
        min_gas_sol=float(cfg.get("min_gas_sol") or 0)
    )

    if not candidate:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE token_stats
                SET pushed_dingtalk = 1,
                    updated_at = %s
                WHERE mint = %s
            """, (now_str(), mint))
        conn.commit()
        return

    send_condition_match_dingtalk(
        candidate=candidate,
        min_profit_sol=float(cfg.get("min_profit_sol") or 0),
        min_gas_sol=float(cfg.get("min_gas_sol") or 0)
    )

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE token_stats
            SET pushed_dingtalk = 1,
                updated_at = %s
            WHERE mint = %s
        """, (now_str(), mint))
    conn.commit()


def is_pump_buy_or_sell(logs):
    if not logs:
        return None
    for log in logs:
        s = str(log)
        if "Instruction: Buy" in s:
            return "BUY"
        if "Instruction: Sell" in s:
            return "SELL"
    return None


def has_create_v2(logs):
    if not logs:
        return False
    for log in logs:
        if "Instruction: CreateV2" in str(log):
            return True
    return False


def extract_signer(account_keys):
    for item in account_keys or []:
        if isinstance(item, dict) and item.get("signer") is True:
            return item.get("pubkey")
        if isinstance(item, str):
            continue
    return None


def value_exists_in_obj(obj, target):
    if target in (None, ""):
        return False
    if isinstance(obj, dict):
        for k, v in obj.items():
            if value_exists_in_obj(k, target) or value_exists_in_obj(v, target):
                return True
        return False
    if isinstance(obj, list):
        return any(value_exists_in_obj(x, target) for x in obj)
    return str(obj) == str(target)


def normalize_token_balances(items):
    result = []
    for x in items or []:
        mint = x.get("mint")
        owner = x.get("owner")
        ui = safe_get(x, "uiTokenAmount", "uiAmount")
        result.append({
            "mint": mint,
            "owner": owner,
            "ui_amount": float(ui or 0),
        })
    return result


def calc_token_changes(pre_balances, post_balances):
    pre_map = {}
    post_map = {}

    for x in normalize_token_balances(pre_balances):
        key = (x["owner"], x["mint"])
        pre_map[key] = x["ui_amount"]

    for x in normalize_token_balances(post_balances):
        key = (x["owner"], x["mint"])
        post_map[key] = x["ui_amount"]

    all_keys = set(pre_map.keys()) | set(post_map.keys())
    changes = []

    for key in all_keys:
        owner, mint = key
        pre_amt = pre_map.get(key, 0.0)
        post_amt = post_map.get(key, 0.0)
        diff = post_amt - pre_amt
        if abs(diff) > 0:
            changes.append({
                "owner": owner,
                "mint": mint,
                "pre": pre_amt,
                "post": post_amt,
                "diff": diff,
            })

    return changes


def calc_sol_change(meta, account_keys, signer):
    if not signer:
        return None

    pre_balances = meta.get("preBalances") or []
    post_balances = meta.get("postBalances") or []

    signer_index = None
    for i, item in enumerate(account_keys or []):
        if isinstance(item, dict) and item.get("pubkey") == signer:
            signer_index = i
            break

    if signer_index is None:
        return None
    if signer_index >= len(pre_balances) or signer_index >= len(post_balances):
        return None

    return lamports_to_sol(post_balances[signer_index] - pre_balances[signer_index])


def choose_main_token_change(changes, signer, side):
    candidates = []

    for c in changes:
        owner = c["owner"]
        mint = c["mint"]
        diff = c["diff"]

        if not mint:
            continue

        if side == "BUY" and diff > 0:
            score = 0
            if owner == signer:
                score += 10
            score += min(abs(diff), 5)
            candidates.append((score, c))

        elif side == "SELL" and diff < 0:
            score = 0
            if owner == signer:
                score += 10
            score += min(abs(diff), 5)
            candidates.append((score, c))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def choose_create_token_change(changes, signer):
    candidates = []

    for c in changes:
        owner = c["owner"]
        mint = c["mint"]
        diff = c["diff"]

        if not mint or diff <= 0:
            continue

        score = 0
        if owner == signer:
            score += 10
        score += min(abs(diff), 5)
        candidates.append((score, c))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def rpc_call(method, params):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    resp = requests.post(RPC_URL, json=payload, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"RPC错误: {data['error']}")
    return data.get("result")


def fetch_transaction_by_signature(signature):
    return rpc_call("getTransaction", [
        signature,
        {
            "encoding": "jsonParsed",
            "maxSupportedTransactionVersion": 0,
            "commitment": "confirmed"
        }
    ])


def fetch_recent_program_signatures(limit=1000):
    return rpc_call("getSignaturesForAddress", [
        PUMP_PROGRAM_ID,
        {
            "limit": limit,
            "commitment": "confirmed"
        }
    ]) or []


def fetch_current_slot():
    return rpc_call("getSlot", [{"commitment": "confirmed"}])


def find_failed_trade_mint_by_slot_and_result(slot, result):
    if slot is None:
        return None

    lower_slot = max(int(slot) - 2, 0)
    upper_slot = int(slot)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT mint, created_slot
                FROM token_stats
                WHERE created_slot BETWEEN %s AND %s
                ORDER BY created_slot DESC
            """, (lower_slot, upper_slot))
            rows = cur.fetchall()
    finally:
        conn.close()

    for row in rows:
        mint = row.get("mint")
        created_slot = row.get("created_slot")
        if not mint or created_slot is None:
            continue
        created_slot = int(created_slot)
        if not (created_slot <= int(slot) <= created_slot + 2):
            continue
        if value_exists_in_obj(result, mint):
            return mint
    return None


def parse_failed_realtime_message(msg):
    params = msg.get("params", {}) or {}
    result = params.get("result", {}) or {}
    if not result:
        return None

    signature = result.get("signature")
    slot = result.get("slot")

    tx = result.get("transaction", {}) or {}
    meta = tx.get("meta", {}) or {}
    if not meta.get("err"):
        return None

    transaction = tx.get("transaction", {}) or {}
    message = transaction.get("message", {}) or {}
    account_keys = message.get("accountKeys", []) or []

    mint = find_failed_trade_mint_by_slot_and_result(slot, result)
    if not mint:
        return None

    signer = extract_signer(account_keys)
    if not signer and account_keys:
        first = account_keys[0]
        if isinstance(first, dict):
            signer = first.get("pubkey")
        elif isinstance(first, str):
            signer = first

    block_time = result.get("blockTime")
    event_time = utc_ms_to_local_str(block_time * 1000) if block_time else now_str()
    gas_fee_sol = lamports_to_sol(meta.get("fee"))

    return {
        "signature": signature,
        "mint": mint,
        "event_time": event_time,
        "slot": slot,
        "signer": signer,
        "gas_fee_sol": gas_fee_sol,
    }


def parse_failed_signature_brief(signature, mint):
    result = fetch_transaction_by_signature(signature)
    if not result:
        return None

    slot = result.get("slot")
    block_time = result.get("blockTime")

    tx = result.get("transaction", {}) or {}
    meta = tx.get("meta", {}) or {}
    transaction = tx.get("transaction", {}) or {}
    message = transaction.get("message", {}) or {}
    account_keys = message.get("accountKeys", []) or []

    signer = extract_signer(account_keys)
    gas_fee_sol = lamports_to_sol(meta.get("fee"))
    event_time = utc_ms_to_local_str(block_time * 1000) if block_time else now_str()

    return {
        "signature": signature,
        "mint": mint,
        "event_time": event_time,
        "slot": slot,
        "signer": signer,
        "gas_fee_sol": gas_fee_sol,
    }


def save_failed_trade(parsed):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO failed_trades_v2 (
                    signature, mint, event_time, slot, signer, gas_fee_sol, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                parsed["signature"],
                parsed["mint"],
                parsed["event_time"],
                parsed["slot"],
                parsed["signer"],
                parsed["gas_fee_sol"],
                now_str()
            ))
        conn.commit()
        return True
    except pymysql.err.IntegrityError:
        conn.rollback()
        return False
    finally:
        conn.close()


def enqueue_failed_trade_task(parsed_create):
    mint = parsed_create["mint"]
    created_slot = parsed_create.get("slot")
    if created_slot is None:
        return

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO failed_trade_tasks (
                    mint, created_slot, end_slot, status, retries, last_scan_slot, last_error, created_at, updated_at
                ) VALUES (%s, %s, %s, 'pending', 0, NULL, NULL, %s, %s)
                ON DUPLICATE KEY UPDATE
                    created_slot = VALUES(created_slot),
                    end_slot = VALUES(end_slot),
                    status = 'pending',
                    updated_at = VALUES(updated_at)
            """, (
                mint,
                created_slot,
                created_slot + 2,
                now_str(),
                now_str()
            ))
        conn.commit()
        print(f"[{now_str()}] 已登记失败交易任务: mint={mint}, slot_range=[{created_slot},{created_slot + 2}]")
    finally:
        conn.close()


def process_failed_trade_tasks_once():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, mint, created_slot, end_slot, status, retries
                FROM failed_trade_tasks
                WHERE status = 'pending'
                ORDER BY id ASC
                LIMIT 100
            """)
            tasks = cur.fetchall()

        if not tasks:
            return

        try:
            current_slot = fetch_current_slot()
        except Exception as e:
            current_slot = None
            print(f"[{now_str()}] 当前slot获取失败: {e}")

        try:
            sig_rows = fetch_recent_program_signatures(limit=1000)
        except Exception as e:
            print(f"[{now_str()}] 失败交易签名拉取异常: {e}")
            return

        for task in tasks:
            task_id = task["id"]
            mint = task["mint"]
            created_slot = int(task["created_slot"])
            end_slot = int(task["end_slot"])
            retries = int(task.get("retries") or 0)

            matched_rows = []
            for row in sig_rows:
                slot = row.get("slot")
                signature = row.get("signature")
                err = row.get("err")
                if signature and err and slot is not None and created_slot <= int(slot) <= end_slot:
                    matched_rows.append(row)

            inserted_count = 0
            for row in matched_rows:
                signature = row.get("signature")
                try:
                    parsed = parse_failed_signature_brief(signature, mint)
                    if not parsed:
                        continue
                    if save_failed_trade(parsed):
                        inserted_count += 1
                except Exception as e:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE failed_trade_tasks
                            SET retries = %s,
                                last_scan_slot = %s,
                                last_error = %s,
                                updated_at = %s
                            WHERE id = %s
                        """, (retries + 1, current_slot, str(e)[:255], now_str(), task_id))
                    conn.commit()
                    print(f"[{now_str()}] 失败交易签名解析异常: mint={mint}, signature={signature}, err={e}")

            done = False
            if current_slot is not None and current_slot >= end_slot + 2:
                done = True
            if retries >= 60:
                done = True

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE failed_trade_tasks
                    SET retries = %s,
                        last_scan_slot = %s,
                        last_error = NULL,
                        status = %s,
                        updated_at = %s
                    WHERE id = %s
                """, (
                    retries + 1,
                    current_slot,
                    'done' if done else 'pending',
                    now_str(),
                    task_id
                ))
            conn.commit()

            if matched_rows or done:
                print(
                    f"[{now_str()}] 失败任务扫描: mint={mint}, slot_range=[{created_slot},{end_slot}], "
                    f"matched={len(matched_rows)}, inserted={inserted_count}, current_slot={current_slot}, done={done}"
                )
    finally:
        conn.close()


def failed_trade_worker_loop():
    while True:
        try:
            process_failed_trade_tasks_once()
        except Exception as e:
            print(f"[{now_str()}] 失败交易线程异常: {e}")
        time.sleep(2)


def save_create_event(parsed_create):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            mint = parsed_create["mint"]
            creator = parsed_create["creator"]
            created_time = parsed_create["created_time"]
            created_slot = parsed_create["slot"]

            cur.execute("SELECT * FROM token_stats WHERE mint = %s", (mint,))
            row = cur.fetchone()

            if row is None:
                cur.execute("""
                    INSERT INTO token_stats (
                        mint, creator, created_time, created_slot, settled_60s, pushed_dingtalk, first_seen, last_seen,
                        buy_count, sell_count, buy_sol, sell_sol, updated_at
                    ) VALUES (%s, %s, %s, %s, 0, 0, %s, %s, 0, 0, 0, 0, %s)
                """, (
                    mint, creator, created_time, created_slot,
                    created_time, created_time, now_str()
                ))
            else:
                final_creator = row.get("creator") or creator
                final_created = row.get("created_time") or created_time
                final_slot = row.get("created_slot") if row.get("created_slot") is not None else created_slot

                cur.execute("""
                    UPDATE token_stats
                    SET creator = %s,
                        created_time = %s,
                        created_slot = %s,
                        first_seen = COALESCE(first_seen, %s),
                        updated_at = %s
                    WHERE mint = %s
                """, (
                    final_creator,
                    final_created,
                    final_slot,
                    created_time,
                    now_str(),
                    mint
                ))
        conn.commit()
    finally:
        conn.close()


def save_trade(parsed):
    """
    pump7.0 主规则保持不变：
    1. 只有 mint 已经存在于 token_stats，才允许入库。
    2. 只入库创建时间起 60 秒内的交易。
    3. event_time 必须是链上实际 blockTime 转出的时间。
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            mint = parsed["mint"]
            side = parsed["side"]
            sol_amount = abs(parsed["sol_amount"] or 0)
            event_time = parsed["time"]

            cur.execute("SELECT * FROM token_stats WHERE mint = %s", (mint,))
            row = cur.fetchone()

            if row is None:
                conn.rollback()
                return False

            created_time = row.get("created_time")
            if created_time is None or event_time is None:
                conn.rollback()
                return False

            created_dt = parse_dt(created_time)
            event_dt = parse_dt(event_time)
            if created_dt is None or event_dt is None:
                conn.rollback()
                return False

            delta_seconds = (event_dt - created_dt).total_seconds()

            if delta_seconds < 0 or delta_seconds > 60:
                conn.rollback()
                return False

            try:
                cur.execute("""
                    INSERT INTO trades (
                        signature, event_time, slot, side, signer, mint,
                        token_amount, sol_amount, price_sol, gas_fee_sol, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    parsed["signature"],
                    parsed["time"],
                    parsed["slot"],
                    parsed["side"],
                    parsed["signer"],
                    parsed["mint"],
                    parsed["token_amount"],
                    parsed["sol_amount"],
                    parsed["price_sol"],
                    parsed["gas_fee_sol"],
                    now_str()
                ))
            except pymysql.err.IntegrityError:
                conn.rollback()
                return False

            buy_count = row["buy_count"] + (1 if side == "BUY" else 0)
            sell_count = row["sell_count"] + (1 if side == "SELL" else 0)
            buy_sol = row["buy_sol"] + (sol_amount if side == "BUY" else 0)
            sell_sol = row["sell_sol"] + (sol_amount if side == "SELL" else 0)

            cur.execute("""
                UPDATE token_stats
                SET last_seen = %s, buy_count = %s, sell_count = %s,
                    buy_sol = %s, sell_sol = %s, updated_at = %s
                WHERE mint = %s
            """, (
                parsed["time"], buy_count, sell_count,
                buy_sol, sell_sol, now_str(), mint
            ))

        conn.commit()
        return True
    finally:
        conn.close()




def send_second_condition_match_dingtalk(candidate, min_profit_sol, min_gas_sol):
    content = (
        "Pump 第二逻辑命中提醒\n"
        f"代币合约: {candidate['mint']}\n"
        f"创建者: {candidate['creator']}\n"
        f"创建时间: {candidate['created_time']}\n"
        f"创建区块: {candidate['created_slot']}\n"
        f"符合条件的交易者: {candidate['buyer']}\n"
        f"利润(SOL): {candidate['profit_sol']}\n"
        f"首笔买入Gas(SOL): {candidate['gas_sol']}\n"
        f"区块: {candidate['buyer_slot']}\n"
        f"首笔买入时间: {candidate['first_buy_time']}\n"
        f"当前推送阈值 => 最低利润: {min_profit_sol}, 最低首笔Gas: {min_gas_sol}"
    )
    return send_dingtalk_text_to_robot(
        content=content,
        webhook=SECOND_DINGTALK_WEBHOOK,
        secret=SECOND_DINGTALK_SECRET,
        keyword=SECOND_DINGTALK_KEYWORD,
    )


def process_second_dingtalk_push_for_mint(conn, mint):
    cfg = get_second_push_config(conn)
    if not int(cfg.get("dingtalk_enabled") or 0):
        return

    with conn.cursor() as cur:
        cur.execute("""
            SELECT pushed_second_dingtalk
            FROM token_stats
            WHERE mint = %s
        """, (mint,))
        row = cur.fetchone()

    if not row:
        return
    if int(row.get("pushed_second_dingtalk") or 0) == 1:
        return

    blacklist_rows = list_blacklist_addresses(conn)
    blacklist_addresses = [x.get("address") for x in blacklist_rows if x.get("address")]

    candidate = find_first_second_condition_candidate_for_mint(
        conn=conn,
        mint=mint,
        min_profit_sol=float(cfg.get("min_profit_sol") or 0),
        min_gas_sol=float(cfg.get("min_gas_sol") or 0),
        blacklist_addresses=blacklist_addresses,
    )

    if candidate:
        send_second_condition_match_dingtalk(
            candidate=candidate,
            min_profit_sol=float(cfg.get("min_profit_sol") or 0),
            min_gas_sol=float(cfg.get("min_gas_sol") or 0),
        )

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE token_stats
            SET pushed_second_dingtalk = 1,
                updated_at = %s
            WHERE mint = %s
        """, (now_str(), mint))
    conn.commit()


def parse_create_message(msg):
    params = msg.get("params", {})
    result = params.get("result", {})

    tx = result.get("transaction", {})
    meta = tx.get("meta", {}) or {}
    transaction = tx.get("transaction", {}) or {}
    message = transaction.get("message", {}) or {}
    account_keys = message.get("accountKeys", []) or []

    logs = meta.get("logMessages", []) or []
    if not has_create_v2(logs):
        return None

    signer = extract_signer(account_keys)

    token_changes = calc_token_changes(
        meta.get("preTokenBalances", []),
        meta.get("postTokenBalances", [])
    )
    main_change = choose_create_token_change(token_changes, signer)
    if not main_change:
        return None

    block_time = result.get("blockTime")
    created_time = utc_ms_to_local_str(block_time * 1000) if block_time else now_str()

    return {
        "mint": main_change["mint"],
        "creator": signer,
        "created_time": created_time,
        "signature": result.get("signature"),
        "slot": result.get("slot"),
    }


def parse_transaction_message(msg):
    params = msg.get("params", {})
    result = params.get("result", {})

    signature = result.get("signature")
    slot = result.get("slot")

    tx = result.get("transaction", {})
    meta = tx.get("meta", {}) or {}
    transaction = tx.get("transaction", {}) or {}

    message = transaction.get("message", {}) or {}
    account_keys = message.get("accountKeys", []) or []

    logs = meta.get("logMessages", []) or []
    side = is_pump_buy_or_sell(logs)
    if side not in ("BUY", "SELL"):
        return None

    signer = extract_signer(account_keys)

    token_changes = calc_token_changes(
        meta.get("preTokenBalances", []),
        meta.get("postTokenBalances", [])
    )
    main_change = choose_main_token_change(token_changes, signer, side)
    if not main_change:
        return None

    sol_change = calc_sol_change(meta, account_keys, signer)
    if sol_change is None:
        return None

    block_time = result.get("blockTime")
    event_time = utc_ms_to_local_str(block_time * 1000) if block_time else now_str()

    token_amount = abs(main_change["diff"])
    sol_amount = abs(sol_change)
    price_sol = (sol_amount / token_amount) if token_amount > 0 else None
    gas_fee_sol = lamports_to_sol(meta.get("fee"))

    return {
        "time": event_time,
        "slot": slot,
        "signature": signature,
        "side": side,
        "signer": signer,
        "mint": main_change["mint"],
        "token_amount": token_amount,
        "sol_amount": sol_amount,
        "price_sol": price_sol,
        "gas_fee_sol": gas_fee_sol,
    }


def settle_buyer_profit_60s_once():
    """
    结算逻辑：
    - 找到已经过了创建后 60 秒、且未结算的币
    - 找出 created_slot / +1 / +2 这三个区块中的 BUY 买家
    - 用该买家在这 60 秒内的卖出 SOL - 买入 SOL 作为利润
    - 结算完成后按当前推送参数筛选并决定是否推送钉钉
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT mint, created_time, created_slot
                FROM token_stats
                WHERE settled_60s = 0
                  AND created_time IS NOT NULL
            """)
            tokens = cur.fetchall()

        now_time = now_dt()

        for token in tokens:
            mint = token["mint"]
            created_time = parse_dt(token["created_time"])
            created_slot = token.get("created_slot")

            if created_time is None or created_slot is None:
                continue

            window_end = created_time + timedelta(seconds=60)
            if now_time < window_end:
                continue

            buyers = []
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT signer
                    FROM trades
                    WHERE mint = %s
                      AND side = 'BUY'
                      AND slot IN (%s, %s, %s)
                """, (mint, created_slot, created_slot + 1, created_slot + 2))
                buyer_rows = cur.fetchall()
                buyers = [x["signer"] for x in buyer_rows if x.get("signer")]

            with conn.cursor() as cur:
                for buyer in buyers:
                    cur.execute("""
                        SELECT
                            SUM(CASE WHEN side='BUY' THEN sol_amount ELSE 0 END) AS buy_sol,
                            SUM(CASE WHEN side='SELL' THEN sol_amount ELSE 0 END) AS sell_sol
                        FROM trades
                        WHERE mint = %s
                          AND signer = %s
                          AND event_time >= %s
                          AND event_time <= %s
                    """, (mint, buyer, created_time, window_end))
                    agg = cur.fetchone() or {}

                    buy_sol = float(agg.get("buy_sol") or 0)
                    sell_sol = float(agg.get("sell_sol") or 0)
                    profit_sol = sell_sol - buy_sol

                    cur.execute("""
                        INSERT INTO buyer_profit_60s (
                            mint, buyer, created_slot, buy_sol, sell_sol, profit_sol,
                            window_start, window_end, settled_at, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            created_slot = VALUES(created_slot),
                            buy_sol = VALUES(buy_sol),
                            sell_sol = VALUES(sell_sol),
                            profit_sol = VALUES(profit_sol),
                            window_start = VALUES(window_start),
                            window_end = VALUES(window_end),
                            settled_at = VALUES(settled_at)
                    """, (
                        mint, buyer, created_slot, buy_sol, sell_sol, profit_sol,
                        created_time, window_end, now_time, now_time
                    ))

                cur.execute("""
                    UPDATE token_stats
                    SET settled_60s = 1,
                        updated_at = %s
                    WHERE mint = %s
                """, (now_str(), mint))

            conn.commit()
            print(f"[SETTLED_60S] mint={mint} created_slot={created_slot} buyers={len(buyers)}")

            try:
                process_dingtalk_push_for_mint(conn, mint)
            except Exception as push_err:
                print(f"[{now_str()}] 钉钉推送异常: mint={mint}, err={push_err}")

            try:
                process_second_dingtalk_push_for_mint(conn, mint)
            except Exception as push_err:
                print(f"[{now_str()}] 第二钉钉推送异常: mint={mint}, err={push_err}")

    finally:
        conn.close()


def cleanup_older_than_hours_once(cleanup_hours=24, optimize_tables=False):
    cleanup_hours = float(cleanup_hours or 24)
    cutoff = now_dt() - timedelta(hours=cleanup_hours)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT mint
                FROM token_stats
                WHERE created_time IS NOT NULL
                  AND created_time < %s
                """,
                (cutoff,)
            )
            old_mints = [str((row or {}).get("mint") or "").strip() for row in cur.fetchall() or []]
            old_mints = [m for m in old_mints if m]

            if not old_mints:
                result = {
                    "deleted_mints": 0,
                    "deleted_profit": 0,
                    "deleted_trades": 0,
                    "deleted_failed": 0,
                    "deleted_failed_legacy": 0,
                    "deleted_failed_tasks": 0,
                    "deleted_tokens": 0,
                    "optimized_tables": [],
                    "cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
                    "cleanup_hours": cleanup_hours,
                }
                print(f"[CLEANUP] hours={cleanup_hours}, cutoff={result['cutoff']}, matched_mints=0")
                return result

            placeholders = ",".join(["%s"] * len(old_mints))

            cur.execute(f"DELETE FROM buyer_profit_60s WHERE mint IN ({placeholders})", old_mints)
            deleted_profit = cur.rowcount

            cur.execute(f"DELETE FROM trades WHERE mint IN ({placeholders})", old_mints)
            deleted_trades = cur.rowcount

            cur.execute(f"DELETE FROM failed_trades_v2 WHERE mint IN ({placeholders})", old_mints)
            deleted_failed = cur.rowcount

            deleted_failed_legacy = 0
            try:
                cur.execute(f"DELETE FROM failed_trades WHERE mint IN ({placeholders})", old_mints)
                deleted_failed_legacy = cur.rowcount
            except Exception:
                deleted_failed_legacy = 0

            cur.execute(f"DELETE FROM failed_trade_tasks WHERE mint IN ({placeholders})", old_mints)
            deleted_failed_tasks = cur.rowcount

            cur.execute(f"DELETE FROM token_stats WHERE mint IN ({placeholders})", old_mints)
            deleted_tokens = cur.rowcount

        conn.commit()

        optimized_tables = []
        if optimize_tables and (deleted_profit or deleted_trades or deleted_failed or deleted_failed_legacy or deleted_failed_tasks or deleted_tokens):
            tables_to_optimize = [
                "buyer_profit_60s",
                "trades",
                "failed_trades_v2",
                "failed_trade_tasks",
                "token_stats",
            ]
            if deleted_failed_legacy:
                tables_to_optimize.insert(3, "failed_trades")
            with conn.cursor() as cur:
                for table_name in tables_to_optimize:
                    try:
                        cur.execute(f"OPTIMIZE TABLE {table_name}")
                        optimized_tables.append(table_name)
                    except Exception as opt_err:
                        print(f"[CLEANUP] OPTIMIZE {table_name} 失败: {opt_err}")
            conn.commit()

        result = {
            "deleted_mints": len(old_mints),
            "deleted_profit": deleted_profit,
            "deleted_trades": deleted_trades,
            "deleted_failed": deleted_failed,
            "deleted_failed_legacy": deleted_failed_legacy,
            "deleted_failed_tasks": deleted_failed_tasks,
            "deleted_tokens": deleted_tokens,
            "optimized_tables": optimized_tables,
            "cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
            "cleanup_hours": cleanup_hours,
        }

        print(
            f"[CLEANUP] hours={cleanup_hours}, cutoff={result['cutoff']}, deleted_mints={len(old_mints)}, "
            f"deleted_profit={deleted_profit}, deleted_trades={deleted_trades}, "
            f"deleted_failed={deleted_failed}, deleted_failed_legacy={deleted_failed_legacy}, "
            f"deleted_failed_tasks={deleted_failed_tasks}, deleted_tokens={deleted_tokens}, "
            f"optimized={','.join(optimized_tables) if optimized_tables else 'none'}"
        )
        return result
    finally:
        conn.close()


def maintenance_loop():
    while True:
        try:
            settle_buyer_profit_60s_once()
            conn = get_conn()
            try:
                cfg = get_cleanup_config(conn)
            finally:
                conn.close()
            cleanup_older_than_hours_once(float(cfg.get("cleanup_hours") or 24), optimize_tables=False)
        except Exception as e:
            print(f"[{now_str()}] 维护任务异常: {e}")
        time.sleep(10)


def on_open(ws):
    print(f"[{now_str()}] 已连接 Helius WebSocket")

    sub_msg = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "transactionSubscribe",
        "params": [
            {
                "accountInclude": [PUMP_PROGRAM_ID],
                "failed": False,
                "vote": False
            },
            {
                "commitment": "processed",
                "encoding": "jsonParsed",
                "transactionDetails": "full",
                "showRewards": False,
                "maxSupportedTransactionVersion": 0
            }
        ]
    }

    ws.send(json.dumps(sub_msg))
    print(f"[{now_str()}] 已订阅 Pump 买卖交易")


def on_open_failed(ws):
    print(f"[{now_str()}] 已连接 Helius 失败交易 WebSocket")

    sub_msg = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "transactionSubscribe",
        "params": [
            {
                "accountInclude": [PUMP_PROGRAM_ID],
                "failed": True,
                "vote": False
            },
            {
                "commitment": "processed",
                "encoding": "jsonParsed",
                "transactionDetails": "full",
                "showRewards": False,
                "maxSupportedTransactionVersion": 0
            }
        ]
    }

    ws.send(json.dumps(sub_msg))
    print(f"[{now_str()}] 已订阅 Pump 失败交易")


def on_message(ws, message):
    try:
        data = json.loads(message)

        if "result" in data and "id" in data and "params" not in data:
            print(f"[{now_str()}] 订阅成功：{data['result']}")
            return

        parsed_create = parse_create_message(data)
        if parsed_create:
            save_create_event(parsed_create)
            print(f"[{parsed_create['created_time']}] CREATE mint={parsed_create['mint']} sig={parsed_create['signature']}")

        parsed = parse_transaction_message(data)
        if not parsed:
            return

        inserted = save_trade(parsed)
        if not inserted:
            return

        print("=" * 100)
        print(f"[{parsed['time']}] {parsed['side']}")
        print(f"Mint: {parsed['mint']}")
        print(f"钱包: {parsed['signer']}")
        print(f"SOL数量: {parsed['sol_amount']}")
        print(f"Token数量: {parsed['token_amount']}")
        print(f"价格(SOL): {parsed['price_sol']}")
        print(f"签名: {parsed['signature']}")
        print(f"区块: {parsed['slot']}")
        print(f"Gas费(SOL): {parsed['gas_fee_sol']:.6f}")
        print("=" * 100)

    except Exception as e:
        print(f"[{now_str()}] 消息解析异常: {e}")


def on_message_failed(ws, message):
    try:
        data = json.loads(message)

        if "result" in data and "id" in data and "params" not in data:
            print(f"[{now_str()}] 失败交易订阅成功：{data['result']}")
            return

        parsed = parse_failed_realtime_message(data)
        if not parsed:
            return

        inserted = save_failed_trade(parsed)
        if not inserted:
            return

        print("=" * 100)
        print(f"[{parsed['event_time']}] FAILED")
        print(f"Mint: {parsed['mint']}")
        print(f"钱包: {parsed['signer']}")
        print(f"签名: {parsed['signature']}")
        print(f"区块: {parsed['slot']}")
        print(f"Gas费(SOL): {float(parsed['gas_fee_sol'] or 0):.6f}")
        print("=" * 100)
    except Exception as e:
        print(f"[{now_str()}] 失败交易消息解析异常: {e}")


def on_error(ws, error):
    print(f"[{now_str()}] WebSocket错误: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"[{now_str()}] 连接关闭: code={close_status_code}, msg={close_msg}")


def run_ws_forever():
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=60, ping_timeout=10)
        except KeyboardInterrupt:
            print("手动停止")
            break
        except Exception as e:
            print(f"[{now_str()}] 运行异常，3秒后重连: {e}")
        time.sleep(3)


def run_failed_ws_forever():
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open_failed,
                on_message=on_message_failed,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=60, ping_timeout=10)
        except KeyboardInterrupt:
            print("手动停止")
            break
        except Exception as e:
            print(f"[{now_str()}] 失败交易运行异常，3秒后重连: {e}")
        time.sleep(3)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/tokens")
def api_tokens():
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 20))
    keyword = request.args.get("keyword", "").strip()

    offset = (page - 1) * page_size
    conn = get_conn()

    try:
        with conn.cursor() as cur:
            if keyword:
                cur.execute(
                    "SELECT COUNT(*) AS cnt FROM token_stats WHERE mint LIKE %s",
                    (f"%{keyword}%",)
                )
                total = cur.fetchone()["cnt"]

                cur.execute("""
                    SELECT
                        ts.*,
                        COALESCE(ft.failed_count, 0) AS failed_count
                    FROM token_stats ts
                    LEFT JOIN (
                        SELECT mint, COUNT(*) AS failed_count
                        FROM failed_trades_v2
                        GROUP BY mint
                    ) ft ON ts.mint = ft.mint
                    WHERE ts.mint LIKE %s
                    ORDER BY (ts.created_time IS NULL) ASC, ts.created_time DESC, ts.last_seen DESC
                    LIMIT %s OFFSET %s
                """, (f"%{keyword}%", page_size, offset))
                rows = cur.fetchall()
            else:
                cur.execute("SELECT COUNT(*) AS cnt FROM token_stats")
                total = cur.fetchone()["cnt"]

                cur.execute("""
                    SELECT
                        ts.*,
                        COALESCE(ft.failed_count, 0) AS failed_count
                    FROM token_stats ts
                    LEFT JOIN (
                        SELECT mint, COUNT(*) AS failed_count
                        FROM failed_trades_v2
                        GROUP BY mint
                    ) ft ON ts.mint = ft.mint
                    ORDER BY (ts.created_time IS NULL) ASC, ts.created_time DESC, ts.last_seen DESC
                    LIMIT %s OFFSET %s
                """, (page_size, offset))
                rows = cur.fetchall()

        for row in rows:
            row["success_count"] = int(row.get("buy_count") or 0) + int(row.get("sell_count") or 0)
            row["total_count"] = row["success_count"] + int(row.get("failed_count") or 0)

        return jsonify({
            "success": True,
            "data": rows,
            "total": total,
            "page": page,
            "page_size": page_size
        })
    finally:
        conn.close()


@app.route("/api/trades")
def api_trades():
    mint = request.args.get("mint", "").strip()
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 100))
    offset = (page - 1) * page_size

    if not mint:
        return jsonify({"success": False, "message": "缺少 mint 参数"}), 400

    conn = get_conn()

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT created_time, created_slot FROM token_stats WHERE mint = %s", (mint,))
            token_row = cur.fetchone()
            created_slot = token_row["created_slot"] if token_row else None

            cur.execute("SELECT COUNT(*) AS cnt FROM trades WHERE mint = %s", (mint,))
            normal_total = cur.fetchone()["cnt"]

            cur.execute("SELECT COUNT(*) AS cnt FROM failed_trades_v2 WHERE mint = %s", (mint,))
            failed_total = cur.fetchone()["cnt"]

            total = normal_total + failed_total

            cur.execute("""
                SELECT
                    signature,
                    event_time,
                    slot,
                    side,
                    signer,
                    mint,
                    token_amount,
                    sol_amount,
                    price_sol,
                    gas_fee_sol,
                    0 AS is_failed
                FROM trades
                WHERE mint = %s
                UNION ALL
                SELECT
                    signature,
                    event_time,
                    slot,
                    'FAILED' AS side,
                    signer,
                    mint,
                    NULL AS token_amount,
                    NULL AS sol_amount,
                    NULL AS price_sol,
                    gas_fee_sol,
                    1 AS is_failed
                FROM failed_trades_v2
                WHERE mint = %s
                ORDER BY event_time DESC
                LIMIT %s OFFSET %s
            """, (mint, mint, page_size, offset))
            rows = cur.fetchall()

            cur.execute("""
                SELECT
                    COUNT(*) as total_count,
                    SUM(CASE WHEN side='BUY' THEN 1 ELSE 0 END) as buy_count,
                    SUM(CASE WHEN side='SELL' THEN 1 ELSE 0 END) as sell_count,
                    SUM(CASE WHEN side='BUY' THEN sol_amount ELSE 0 END) as buy_sol,
                    SUM(CASE WHEN side='SELL' THEN sol_amount ELSE 0 END) as sell_sol
                FROM trades
                WHERE mint = %s
            """, (mint,))
            summary = cur.fetchone() or {}
            summary["success_count"] = int(summary.get("total_count") or 0)
            summary["total_count"] = total

        data = []
        for row in rows:
            slot = row.get("slot")
            highlight_blue = False
            if not int(row.get("is_failed") or 0):
                if created_slot is not None and slot is not None and slot in (created_slot, created_slot + 1, created_slot + 2):
                    highlight_blue = True

            item = dict(row)
            item["highlight_blue"] = highlight_blue
            data.append(item)

        return jsonify({
            "success": True,
            "summary": {
                **(summary if summary else {}),
                "failed_count": failed_total,
                "created_time": token_row["created_time"].strftime("%Y-%m-%d %H:%M:%S") if token_row and token_row.get("created_time") else None,
                "created_slot": created_slot
            },
            "data": data,
            "total": total,
            "page": page,
            "page_size": page_size
        })
    finally:
        conn.close()


@app.route("/api/buyer_profit_60s")
def api_buyer_profit_60s():
    mint = request.args.get("mint", "").strip()
    if not mint:
        return jsonify({"success": False, "message": "缺少 mint 参数"}), 400

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT mint, buyer, created_slot, buy_sol, sell_sol, profit_sol,
                       window_start, window_end, settled_at
                FROM buyer_profit_60s
                WHERE mint = %s
                ORDER BY profit_sol DESC, buyer ASC
            """, (mint,))
            rows = cur.fetchall()

        return jsonify({"success": True, "data": rows})
    finally:
        conn.close()


@app.route("/api/condition_query", methods=["POST"])
def api_condition_query():
    data = request.get_json(silent=True) or {}
    min_profit_sol = float(data.get("min_profit_sol", 0) or 0)
    min_gas_sol = float(data.get("min_gas_sol", 0) or 0)
    page = int(data.get("page", 1) or 1)
    page_size = int(data.get("page_size", 20) or 20)

    conn = get_conn()
    try:
        rows = find_condition_candidates(
            conn=conn,
            min_profit_sol=min_profit_sol,
            min_gas_sol=min_gas_sol
        )
        total = len(rows)
        start = (page - 1) * page_size
        end = start + page_size
        paged = rows[start:end]

        return jsonify({
            "success": True,
            "count": total,
            "page": page,
            "page_size": page_size,
            "data": paged
        })
    finally:
        conn.close()


@app.route("/api/push_config", methods=["GET"])
def api_get_push_config():
    conn = get_conn()
    try:
        cfg = get_push_config(conn)
        return jsonify({"success": True, "data": cfg})
    finally:
        conn.close()


@app.route("/api/push_config", methods=["POST"])
def api_update_push_config():
    data = request.get_json(silent=True) or {}
    min_profit_sol = float(data.get("min_profit_sol", 0) or 0)
    min_gas_sol = float(data.get("min_gas_sol", 0) or 0)
    dingtalk_enabled = 1 if bool(data.get("dingtalk_enabled", True)) else 0

    conn = get_conn()
    try:
        update_push_config(
            conn=conn,
            min_profit_sol=min_profit_sol,
            min_gas_sol=min_gas_sol,
            dingtalk_enabled=dingtalk_enabled
        )
        conn.commit()
        return jsonify({"success": True, "message": "推送参数更新成功"})
    finally:
        conn.close()


@app.route("/api/test_dingtalk_push", methods=["POST"])
def api_test_dingtalk_push():
    conn = get_conn()
    try:
        cfg = get_push_config(conn)
    finally:
        conn.close()

    content = (
        "Pump 钉钉推送测试\n"
        f"时间: {now_str()}\n"
        f"当前最低利润(SOL): {cfg.get('min_profit_sol')}\n"
        f"当前最低首笔Gas(SOL): {cfg.get('min_gas_sol')}\n"
        f"推送开关: {'开启' if int(cfg.get('dingtalk_enabled') or 0) == 1 else '关闭'}"
    )

    try:
        send_dingtalk_text(content)
        return jsonify({"success": True, "message": "钉钉测试推送成功"})
    except Exception as e:
        return jsonify({"success": False, "message": f"钉钉测试推送失败: {e}"}), 500


@app.route("/api/overview")
def api_overview():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM token_stats")
            token_count = int((cur.fetchone() or {}).get("cnt") or 0)
            cur.execute("SELECT COUNT(*) AS cnt FROM trades")
            success_count = int((cur.fetchone() or {}).get("cnt") or 0)
            cur.execute("SELECT COUNT(*) AS cnt FROM failed_trades_v2")
            failed_count = int((cur.fetchone() or {}).get("cnt") or 0)
            cur.execute("SELECT COUNT(DISTINCT mint) AS cnt FROM buyer_profit_60s")
            settled_60s_count = int((cur.fetchone() or {}).get("cnt") or 0)
        return jsonify({
            "success": True,
            "data": {
                "token_count": token_count,
                "success_count": success_count,
                "failed_count": failed_count,
                "total_count": success_count + failed_count,
                "settled_60s_count": settled_60s_count,
            }
        })
    finally:
        conn.close()


@app.route("/api/second_condition_query", methods=["POST"])
def api_second_condition_query():
    data = request.get_json(silent=True) or {}
    min_profit_sol = float(data.get("min_profit_sol", 0) or 0)
    min_gas_sol = float(data.get("min_gas_sol", 0) or 0)
    page = int(data.get("page", 1) or 1)
    page_size = int(data.get("page_size", 20) or 20)

    conn = get_conn()
    try:
        blacklist_rows = list_blacklist_addresses(conn)
        blacklist_addresses = [x.get("address") for x in blacklist_rows if x.get("address")]
        result = find_second_condition_candidates(
            conn=conn,
            min_profit_sol=min_profit_sol,
            min_gas_sol=min_gas_sol,
            blacklist_addresses=blacklist_addresses,
        )
        rows = result.get("rows") or []
        total = len(rows)
        start = (page - 1) * page_size
        end = start + page_size
        paged = rows[start:end]

        return jsonify({
            "success": True,
            "count": total,
            "page": page,
            "page_size": page_size,
            "first_stage_count": int(result.get("first_stage_count") or 0),
            "blacklisted_count": int(result.get("blacklisted_count") or 0),
            "final_count": int(result.get("final_count") or 0),
            "data": paged,
        })
    finally:
        conn.close()


@app.route("/api/second_push_config", methods=["GET"])
def api_get_second_push_config():
    conn = get_conn()
    try:
        cfg = get_second_push_config(conn)
        return jsonify({"success": True, "data": cfg})
    finally:
        conn.close()


@app.route("/api/second_push_config", methods=["POST"])
def api_update_second_push_config():
    data = request.get_json(silent=True) or {}
    min_profit_sol = float(data.get("min_profit_sol", 0) or 0)
    min_gas_sol = float(data.get("min_gas_sol", 0) or 0)
    dingtalk_enabled = 1 if bool(data.get("dingtalk_enabled", True)) else 0

    conn = get_conn()
    try:
        update_second_push_config(conn, min_profit_sol, min_gas_sol, dingtalk_enabled)
        conn.commit()
        return jsonify({"success": True, "message": "第二逻辑推送参数更新成功"})
    finally:
        conn.close()


@app.route("/api/test_second_dingtalk_push", methods=["POST"])
def api_test_second_dingtalk_push():
    conn = get_conn()
    try:
        cfg = get_second_push_config(conn)
    finally:
        conn.close()

    content = (
        "Pump 第二逻辑钉钉推送测试\n"
        f"时间: {now_str()}\n"
        f"当前最低利润(SOL): {cfg.get('min_profit_sol')}\n"
        f"当前最低首笔Gas(SOL): {cfg.get('min_gas_sol')}\n"
        f"推送开关: {'开启' if int(cfg.get('dingtalk_enabled') or 0) == 1 else '关闭'}"
    )
    try:
        send_dingtalk_text_to_robot(
            content=content,
            webhook=SECOND_DINGTALK_WEBHOOK,
            secret=SECOND_DINGTALK_SECRET,
            keyword=SECOND_DINGTALK_KEYWORD,
        )
        return jsonify({"success": True, "message": "第二钉钉测试推送成功"})
    except Exception as e:
        return jsonify({"success": False, "message": f"第二钉钉测试推送失败: {e}"}), 500


@app.route("/api/blacklist", methods=["GET"])
def api_blacklist_list():
    conn = get_conn()
    try:
        rows = list_blacklist_addresses(conn)
        return jsonify({"success": True, "data": rows})
    finally:
        conn.close()


@app.route("/api/blacklist/add", methods=["POST"])
def api_blacklist_add():
    data = request.get_json(silent=True) or {}
    address = str(data.get("address") or "").strip()
    note = str(data.get("note") or "").strip()
    if not address:
        return jsonify({"success": False, "message": "地址不能为空"}), 400
    conn = get_conn()
    try:
        add_blacklist_address(conn, address, note)
        conn.commit()
        return jsonify({"success": True, "message": "黑名单保存成功"})
    finally:
        conn.close()


@app.route("/api/blacklist/delete", methods=["POST"])
def api_blacklist_delete():
    data = request.get_json(silent=True) or {}
    address = str(data.get("address") or "").strip()
    if not address:
        return jsonify({"success": False, "message": "地址不能为空"}), 400
    conn = get_conn()
    try:
        deleted = delete_blacklist_address(conn, address)
        conn.commit()
        return jsonify({"success": True, "message": "删除成功", "deleted": deleted})
    finally:
        conn.close()


@app.route("/api/cleanup_config", methods=["GET"])
def api_get_cleanup_config():
    conn = get_conn()
    try:
        cfg = get_cleanup_config(conn)
        return jsonify({"success": True, "data": cfg})
    finally:
        conn.close()


@app.route("/api/cleanup_config", methods=["POST"])
def api_update_cleanup_config():
    data = request.get_json(silent=True) or {}
    cleanup_hours = float(data.get("cleanup_hours", 24) or 24)
    if cleanup_hours <= 0:
        return jsonify({"success": False, "message": "清理小时数必须大于0"}), 400
    conn = get_conn()
    try:
        update_cleanup_config(conn, cleanup_hours)
        conn.commit()
        return jsonify({"success": True, "message": "清理配置更新成功"})
    finally:
        conn.close()


@app.route("/api/run_cleanup", methods=["POST"])
def api_run_cleanup():
    conn = get_conn()
    try:
        cfg = get_cleanup_config(conn)
    finally:
        conn.close()
    try:
        result = cleanup_older_than_hours_once(float(cfg.get("cleanup_hours") or 24), optimize_tables=False)
        return jsonify({"success": True, "message": "清理完成", "data": result})
    except Exception as e:
        return jsonify({"success": False, "message": f"清理失败: {e}"}), 500


def start_background_ws():
    t = threading.Thread(target=run_ws_forever, daemon=True)
    t.start()


def start_maintenance_worker():
    t = threading.Thread(target=maintenance_loop, daemon=True)
    t.start()


def start_failed_trade_worker():
    t = threading.Thread(target=run_failed_ws_forever, daemon=True)
    t.start()


if __name__ == "__main__":
    init_db()
    start_background_ws()
    start_maintenance_worker()
    start_failed_trade_worker()
    app.run(host="0.0.0.0", port=WEB_PORT, debug=False)
