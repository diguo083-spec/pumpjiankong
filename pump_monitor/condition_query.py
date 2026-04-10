def find_condition_candidates(conn, min_profit_sol, min_gas_sol):
    """
    查询已完成60秒利润结算的代币，在 created_slot / created_slot+1 / created_slot+2 三个区块中，
    找到同时满足：
    1. profit_sol >= min_profit_sol
    2. 首笔买入 gas_fee_sol >= min_gas_sol
    的交易者。

    同一个币若有多位符合，只返回最早买入的一位。
    """

    results = []

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                ts.mint,
                ts.creator,
                ts.created_time,
                ts.created_slot,
                bp.buyer,
                bp.profit_sol
            FROM token_stats ts
            JOIN buyer_profit_60s bp ON ts.mint = bp.mint
            WHERE ts.settled_60s = 1
              AND ts.created_slot IS NOT NULL
              AND bp.profit_sol >= %s
            ORDER BY ts.created_time DESC
        """, (min_profit_sol,))
        base_rows = cur.fetchall()

    grouped = {}
    for row in base_rows:
        grouped.setdefault(row["mint"], []).append(row)

    for mint, rows in grouped.items():
        matched = []

        for row in rows:
            buyer = row["buyer"]
            created_slot = row["created_slot"]

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        event_time,
                        slot,
                        gas_fee_sol
                    FROM trades
                    WHERE mint = %s
                      AND signer = %s
                      AND side = 'BUY'
                      AND slot IN (%s, %s, %s)
                    ORDER BY event_time ASC, id ASC
                    LIMIT 1
                """, (
                    mint,
                    buyer,
                    created_slot,
                    created_slot + 1,
                    created_slot + 2
                ))
                first_buy = cur.fetchone()

            if not first_buy:
                continue

            first_buy_gas_sol = float(first_buy.get("gas_fee_sol") or 0)
            if first_buy_gas_sol < min_gas_sol:
                continue

            matched.append({
                "mint": mint,
                "creator": row.get("creator"),
                "created_time": row.get("created_time").strftime("%Y-%m-%d %H:%M:%S") if row.get("created_time") else None,
                "created_slot": created_slot,
                "buyer": buyer,
                "profit_sol": float(row.get("profit_sol") or 0),
                "gas_sol": first_buy_gas_sol,
                "buyer_slot": first_buy.get("slot"),
                "first_buy_time": first_buy.get("event_time"),
            })

        if matched:
            matched.sort(key=lambda x: (x["first_buy_time"], x["buyer_slot"] or 0))
            chosen = matched[0]

            if chosen.get("first_buy_time"):
                chosen["first_buy_time"] = chosen["first_buy_time"].strftime("%Y-%m-%d %H:%M:%S")

            results.append(chosen)

    results.sort(key=lambda x: (x["first_buy_time"] or "", x["created_slot"] or 0))
    return results


def find_first_condition_candidate_for_mint(conn, mint, min_profit_sol, min_gas_sol):
    """
    专门给自动推送用：
    查询单个 mint 是否存在符合条件的交易者。
    若多位符合，返回最早买入的一位。
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                ts.mint,
                ts.creator,
                ts.created_time,
                ts.created_slot,
                bp.buyer,
                bp.profit_sol
            FROM token_stats ts
            JOIN buyer_profit_60s bp ON ts.mint = bp.mint
            WHERE ts.mint = %s
              AND ts.settled_60s = 1
              AND ts.created_slot IS NOT NULL
              AND bp.profit_sol >= %s
        """, (mint, min_profit_sol))
        rows = cur.fetchall()

    matched = []

    for row in rows:
        buyer = row["buyer"]
        created_slot = row["created_slot"]

        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    event_time,
                    slot,
                    gas_fee_sol
                FROM trades
                WHERE mint = %s
                  AND signer = %s
                  AND side = 'BUY'
                  AND slot IN (%s, %s, %s)
                ORDER BY event_time ASC, id ASC
                LIMIT 1
            """, (
                mint,
                buyer,
                created_slot,
                created_slot + 1,
                created_slot + 2
            ))
            first_buy = cur.fetchone()

        if not first_buy:
            continue

        first_buy_gas_sol = float(first_buy.get("gas_fee_sol") or 0)
        if first_buy_gas_sol < min_gas_sol:
            continue

        matched.append({
            "mint": mint,
            "creator": row.get("creator"),
            "created_time": row.get("created_time").strftime("%Y-%m-%d %H:%M:%S") if row.get("created_time") else None,
            "created_slot": created_slot,
            "buyer": buyer,
            "profit_sol": float(row.get("profit_sol") or 0),
            "gas_sol": first_buy_gas_sol,
            "buyer_slot": first_buy.get("slot"),
            "first_buy_time": first_buy.get("event_time").strftime("%Y-%m-%d %H:%M:%S") if first_buy.get("event_time") else None,
        })

    if not matched:
        return None

    matched.sort(key=lambda x: (x["first_buy_time"] or "", x["buyer_slot"] or 0))
    return matched[0]


def _fetch_first_three_block_addresses(conn, mint, created_slot):
    slot_a = created_slot
    slot_b = created_slot + 1
    slot_c = created_slot + 2
    addresses = set()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT signer
            FROM trades
            WHERE mint = %s
              AND signer IS NOT NULL
              AND signer <> ''
              AND slot IN (%s, %s, %s)
        """, (mint, slot_a, slot_b, slot_c))
        for row in cur.fetchall():
            if row.get("signer"):
                addresses.add(row["signer"])

        cur.execute("""
            SELECT DISTINCT signer
            FROM failed_trades_v2
            WHERE mint = %s
              AND signer IS NOT NULL
              AND signer <> ''
              AND slot IN (%s, %s, %s)
        """, (mint, slot_a, slot_b, slot_c))
        for row in cur.fetchall():
            if row.get("signer"):
                addresses.add(row["signer"])

    return addresses


def find_second_condition_candidates(conn, min_profit_sol, min_gas_sol, blacklist_addresses):
    blacklist_set = {x for x in (blacklist_addresses or []) if x}
    base_rows = find_condition_candidates(conn, min_profit_sol, min_gas_sol)

    final_rows = []
    blacklisted_count = 0

    for row in base_rows:
        created_slot = row.get("created_slot")
        mint = row.get("mint")
        if created_slot is None or not mint:
            final_rows.append(row)
            continue

        hit_addresses = sorted(_fetch_first_three_block_addresses(conn, mint, int(created_slot)) & blacklist_set)
        if hit_addresses:
            blacklisted_count += 1
            continue

        final_rows.append(row)

    return {
        "first_stage_count": len(base_rows),
        "blacklisted_count": blacklisted_count,
        "final_count": len(final_rows),
        "rows": final_rows,
    }


def find_first_second_condition_candidate_for_mint(conn, mint, min_profit_sol, min_gas_sol, blacklist_addresses):
    candidate = find_first_condition_candidate_for_mint(conn, mint, min_profit_sol, min_gas_sol)
    if not candidate:
        return None

    blacklist_set = {x for x in (blacklist_addresses or []) if x}
    created_slot = candidate.get("created_slot")
    if created_slot is None:
        return candidate

    hit_addresses = sorted(_fetch_first_three_block_addresses(conn, mint, int(created_slot)) & blacklist_set)
    if hit_addresses:
        return None

    return candidate
