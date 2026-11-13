import os
import time
import json
import hmac
import hashlib
import threading
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, request, jsonify, Response, render_template_string

app = Flask(__name__)

STATE_FILE = "bearskimmer_v3_2_state.json"
GATE_HOST = "https://api.gateio.ws"
GATE_PREFIX = "/api/v4"

BEAR_UNIVERSE = [
    "SUI_USDT", "SOL_USDT", "DOGE_USDT", "XRP_USDT", "ADA_USDT",
    "NEAR_USDT", "AVAX_USDT", "LINK_USDT", "FET_USDT", "DOT_USDT",
    "ARB_USDT", "OP_USDT", "TRX_USDT", "PEPE_USDT", "WIF_USDT", "RENDER_USDT"
]

HTTP_SESSION = requests.Session()
adapter = HTTPAdapter(
    pool_connections=24,
    pool_maxsize=24,
    max_retries=Retry(total=2, backoff_factor=0.15, status_forcelist=[502, 503, 504])
)
HTTP_SESSION.mount("https://", adapter)

CONTRACT_META_CACHE = {}
BEAR_RADAR_CACHE = {}


def fmt_px(val):
    v = float(val or 0.0)
    if v == 0:
        return "$0.0000"
    if v < 0.0001:
        return f"${v:.8f}"
    if v < 0.01:
        return f"${v:.6f}"
    return f"${v:.4f}"


DEFAULT_STATE = {
    "mode": "PAPER",
    "auto_pilot": True,
    "max_short_slots": 5,
    "slot_size_usdt": 100.0,
    "step_up_trigger_pct": 2.0,     # When coin pumps +2.0% above last entry
    "step_up_scale_pct": 50.0,      # Adds 50% of the coin's initial investment ($50 on a $100 short, $5 on a $10 short!)
    "max_step_ups": 2,
    "min_bear_score": 68,
    "skim_profit_pct": 1.00,
    "trail_bounce_pct": 0.22,
    "futures_fee_buffer_pct": 0.12,
    "api_key": os.environ.get("GATE_API_KEY", "").strip(),
    "api_secret": os.environ.get("GATE_API_SECRET", "").strip(),
    "api_connected": False,
    "api_status_msg": "Not Connected — Paste Gate.io Keys Below for Live Mode",
    "live_futures_usdt": 0.0,
    "paper_usdt_balance": 1000.0,
    "paper_starting_bankroll": 1000.0,
    "short_positions": {},
    "blocked_pairs": [],
    "radar": [],
    "stats": {
        "paper_skims": 0,
        "paper_profit_usdt": 0.0,
        "live_skims": 0,
        "live_profit_usdt": 0.0
    },
    "skim_ledger": [],
    "logs": [],
    "cycle_count": 0,
    "last_scan_ms": 0,
    "last_update": "--:--:--",
    "last_worker_heartbeat": 0.0
}

state_lock = threading.Lock()
cycle_lock = threading.Lock()
worker_started = False
STATE = json.loads(json.dumps(DEFAULT_STATE))


def add_log(msg):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    STATE["logs"].insert(0, f"[{ts}] {msg}")
    STATE["logs"] = STATE["logs"][:120]


def load_state():
    global STATE
    for fn in [
        STATE_FILE,
        "bearskimmer_v3_1_state.json",
        "bearskimmer_v3_0_state.json",
        "bearskimmer_v2_0_state.json"
    ]:
        if os.path.exists(fn):
            try:
                with open(fn, "r") as f:
                    saved = json.load(f)
                    sk = (saved.get("api_key") or "").strip()
                    ss = (saved.get("api_secret") or "").strip()
                    STATE.update(saved)
                    if sk:
                        STATE["api_key"] = sk
                    if ss:
                        STATE["api_secret"] = ss
                    STATE.setdefault("step_up_trigger_pct", 2.0)
                    STATE.setdefault("step_up_scale_pct", 50.0)
                    STATE.setdefault("max_step_ups", 2)
                    STATE.setdefault("paper_starting_bankroll", 1000.0)
                    STATE.setdefault("blocked_pairs", [])
                    # Backfill base_slot_usdt on existing positions so proportional step-up works immediately
                    for p in STATE.get("short_positions", {}).values():
                        p.setdefault("base_slot_usdt", float(p.get("margin_usdt", 100.0)))
                break
            except Exception as e:
                add_log(f"State load notice: {e}")

    ek = os.environ.get("GATE_API_KEY", "").strip()
    es = os.environ.get("GATE_API_SECRET", "").strip()
    if ek:
        STATE["api_key"] = ek
    if es:
        STATE["api_secret"] = es


def save_state():
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(STATE, f, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception:
        pass


def gate_sign(method, url, query_string="", body_string="", custom_key=None, custom_secret=None):
    key = (custom_key if custom_key else STATE.get("api_key", "")).strip()
    secret = (custom_secret if custom_secret else STATE.get("api_secret", "")).strip()
    t = str(int(time.time()))
    hashed_payload = hashlib.sha512(body_string.encode("utf-8")).hexdigest()
    s = f"{method}\n{url}\n{query_string}\n{hashed_payload}\n{t}"
    sign = hmac.new(secret.encode("utf-8"), s.encode("utf-8"), hashlib.sha512).hexdigest()
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "KEY": key,
        "Timestamp": t,
        "SIGN": sign
    }


def fetch_futures_tickers(pairs):
    r = HTTP_SESSION.get(f"{GATE_HOST}{GATE_PREFIX}/futures/usdt/tickers", timeout=6)
    r.raise_for_status()
    wanted = set(pairs)
    out = {}
    for t in r.json():
        c = t.get("contract")
        if c in wanted:
            last = float(t.get("last") or 0.0)
            if last > 0:
                out[c] = {
                    "last": last,
                    "change_24h": float(t.get("change_percentage") or 0.0),
                    "funding_rate": float(t.get("funding_rate") or 0.0) * 100.0
                }
    return out


def get_contract_meta(contract):
    now = time.time()
    cached = CONTRACT_META_CACHE.get(contract)
    if not cached or (now - cached["ts"] > 3600):
        r = HTTP_SESSION.get(f"{GATE_HOST}{GATE_PREFIX}/futures/usdt/contracts/{contract}", timeout=5)
        if r.status_code == 200:
            info = r.json()
            cached = {"quanto_multiplier": float(info.get("quanto_multiplier") or 1.0), "ts": now}
        else:
            cached = {"quanto_multiplier": 1.0, "ts": now}
        CONTRACT_META_CACHE[contract] = cached
    return cached


def fetch_futures_usdt_balance(custom_key=None, custom_secret=None):
    url = f"{GATE_PREFIX}/futures/usdt/accounts"
    headers = gate_sign("GET", url, "", "", custom_key, custom_secret)
    r = HTTP_SESSION.get(f"{GATE_HOST}{url}", headers=headers, timeout=5)
    if r.status_code != 200:
        raise ValueError(f"Gate.io Futures HTTP {r.status_code}: {r.text[:80]}")
    return float(r.json().get("available") or 0.0)


def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(0.0, d))
        losses.append(max(0.0, -d))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100.0 - (100.0 / (1.0 + rs))


def compute_bear_score(pair, tk):
    now = time.time()
    cached = BEAR_RADAR_CACHE.get(pair)
    if cached and (now - cached["ts"] < 14.0):
        cached["price"] = tk["last"]
        return cached

    price = tk["last"]
    rsi_14 = 50.0
    ask_sell_pct = 50.0
    upper_wick = False

    try:
        c_resp = HTTP_SESSION.get(
            f"{GATE_HOST}{GATE_PREFIX}/futures/usdt/candlesticks",
            params={"contract": pair, "interval": "5m", "limit": 25},
            timeout=3.5
        )
        if c_resp.status_code == 200:
            raw = c_resp.json()
            closes = [float(c["c"]) for c in raw if "c" in c]
            highs = [float(c["h"]) for c in raw if "h" in c]
            lows = [float(c["l"]) for c in raw if "l" in c]
            if len(closes) >= 16:
                closes[-1] = price
                rsi_14 = calc_rsi(closes, 14)
                c_rng = max(1e-12, highs[-1] - lows[-1])
                upper_wick = ((highs[-1] - price) / c_rng) >= 0.35
    except Exception:
        pass

    try:
        ob_resp = HTTP_SESSION.get(
            f"{GATE_HOST}{GATE_PREFIX}/futures/usdt/order_book",
            params={"contract": pair, "limit": 15},
            timeout=3.0
        )
        if ob_resp.status_code == 200:
            ob = ob_resp.json()
            bids = sum(float(b["p"]) * float(b["s"]) for b in ob.get("bids", []))
            asks = sum(float(a["p"]) * float(a["s"]) for a in ob.get("asks", []))
            if (bids + asks) > 0:
                ask_sell_pct = (asks / (bids + asks)) * 100.0
    except Exception:
        pass

    if rsi_14 < 38:
        s_rsi = 5.0
    elif 46 <= rsi_14 <= 74:
        s_rsi = 38.0
    else:
        s_rsi = 25.0

    s_ob = max(4.0, min(35.0, ((ask_sell_pct - 35.0) / 35.0) * 35.0))
    s_rej = (15.0 if upper_wick else 6.0) + (12.0 if tk["change_24h"] < 0 else 4.0)
    bear_score = int(round(max(1.0, min(99.0, s_rsi + s_ob + s_rej))))

    if bear_score >= 74:
        sig = "SNIPE SHORT TOP"
    elif bear_score >= 66:
        sig = "BEAR REJECTION"
    elif rsi_14 < 38:
        sig = "OVERSOLD — WAIT"
    else:
        sig = "NEUTRAL"

    res = {
        "pair": pair,
        "price": price,
        "change_24h": round(tk["change_24h"], 2),
        "bear_score": bear_score,
        "rsi_14": round(rsi_14, 1),
        "ask_sell_pct": round(ask_sell_pct, 1),
        "signal": sig,
        "ts": now
    }
    BEAR_RADAR_CACHE[pair] = res
    return res


def place_futures_short_order(contract, size_contracts):
    url = f"{GATE_PREFIX}/futures/usdt/orders"
    body = {"contract": contract, "size": int(size_contracts), "price": "0", "tif": "ioc"}
    if size_contracts > 0:
        body["reduce_only"] = True
    body_str = json.dumps(body)
    headers = gate_sign("POST", url, "", body_str)
    r = HTTP_SESSION.post(f"{GATE_HOST}{url}", headers=headers, data=body_str, timeout=7)
    return r.status_code, r.json()


# =============================================================================
# MAIN EXECUTION ENGINE (PROPORTIONAL % STEP-UP BAR MOVER)
# =============================================================================
def run_bear_cycle(force_manual=False):
    if not cycle_lock.acquire(blocking=force_manual):
        return

    t0 = time.time()
    try:
        with state_lock:
            mode = STATE.get("mode", "PAPER")
            auto_pilot = bool(STATE.get("auto_pilot", True))
            max_slots = int(STATE.get("max_short_slots", 5))
            slot_size = float(STATE.get("slot_size_usdt", 100.0))
            step_trigger_pct = float(STATE.get("step_up_trigger_pct", 2.0))
            step_scale_pct = float(STATE.get("step_up_scale_pct", 50.0))
            max_steps = int(STATE.get("max_step_ups", 2))
            min_bear = int(STATE.get("min_bear_score", 68))
            skim_pct = float(STATE.get("skim_profit_pct", 1.00))
            trail_bounce = float(STATE.get("trail_bounce_pct", 0.22))
            fee_buf = float(STATE.get("futures_fee_buffer_pct", 0.12))
            has_keys = bool(STATE.get("api_key") and STATE.get("api_secret"))
            active_pairs = list(STATE["short_positions"].keys())
            blocked_set = set(STATE.get("blocked_pairs", []))

        if has_keys:
            try:
                fut_usdt = fetch_futures_usdt_balance()
                with state_lock:
                    STATE["api_connected"] = True
                    STATE["live_futures_usdt"] = fut_usdt
                    STATE["api_status_msg"] = f"Gate.io Futures Connected (Available: ${fut_usdt:.2f} USDT)"
            except Exception as e:
                with state_lock:
                    STATE["api_connected"] = False
                    STATE["api_status_msg"] = f"API Notice: {str(e)[:50]}"

        all_pairs = list(set(BEAR_UNIVERSE + active_pairs))
        try:
            tk_map = fetch_futures_tickers(all_pairs)
        except Exception:
            return

        radar_list = []
        for pair in BEAR_UNIVERSE:
            tk = tk_map.get(pair)
            if tk:
                r_item = compute_bear_score(pair, tk).copy()
                r_item["is_blocked"] = (pair in blocked_set)
                radar_list.append(r_item)
        radar_list.sort(key=lambda x: x["bear_score"], reverse=True)

        required_drop_pct = skim_pct + fee_buf

        for pair in list(STATE["short_positions"].keys()):
            with state_lock:
                pos = STATE["short_positions"].get(pair)
            if not pos or pair not in tk_map:
                continue

            live_px = tk_map[pair]["last"]
            entry_px = float(pos["entry_price"])
            margin_usdt = float(pos["margin_usdt"])
            base_slot = float(pos.get("base_slot_usdt") or margin_usdt)
            steps_done = int(pos.get("step_ups_done", 0))
            last_step_px = float(pos.get("last_step_price") or entry_px)

            # PROPORTIONAL STEP-UP AMOUNT (e.g. 50% of $100 slot = $50 top-up; 50% of $10 slot = $5 top-up!)
            dynamic_step_add_usdt = round(max(2.0, base_slot * (step_scale_pct / 100.0)), 2)
            pos["next_step_add_usdt"] = dynamic_step_add_usdt

            pump_from_last_pct = ((live_px - last_step_px) / last_step_px) * 100.0
            if steps_done < max_steps and pump_from_last_pct >= step_trigger_pct:
                if mode == "PAPER" and STATE.get("paper_usdt_balance", 0.0) >= dynamic_step_add_usdt:
                    with state_lock:
                        if pair not in STATE["short_positions"]:
                            continue
                        old_entry = entry_px
                        old_units = margin_usdt / entry_px
                        add_units = dynamic_step_add_usdt / live_px
                        new_margin = round(margin_usdt + dynamic_step_add_usdt, 4)
                        new_avg_entry = new_margin / (old_units + add_units)

                        STATE["paper_usdt_balance"] = round(STATE["paper_usdt_balance"] - dynamic_step_add_usdt, 4)
                        pos["margin_usdt"] = new_margin
                        pos["entry_price"] = new_avg_entry
                        pos["last_step_price"] = live_px
                        pos["step_ups_done"] = steps_done + 1

                        entry_px = new_avg_entry
                        margin_usdt = new_margin
                        new_target = entry_px * (1.0 - (required_drop_pct / 100.0))
                        add_log(
                            f"🔼 [PROPORTIONAL BAR RAISE #{steps_done+1}][{pair}] Pumped +{pump_from_last_pct:.1f}% to {fmt_px(live_px)}! "
                            f"Added ${dynamic_step_add_usdt:.0f} ({step_scale_pct:.0f}% of ${base_slot:.0f}) -> "
                            f"Entry Bar raised {fmt_px(old_entry)} → {fmt_px(entry_px)} (New Target: {fmt_px(new_target)})"
                        )
                elif mode == "LIVE" and has_keys and STATE.get("live_futures_usdt", 0.0) >= dynamic_step_add_usdt:
                    meta = get_contract_meta(pair)
                    c_val = live_px * meta["quanto_multiplier"]
                    add_contracts = max(1, int(dynamic_step_add_usdt / max(1e-9, c_val)))
                    actual_add_usdt = round(add_contracts * c_val, 4)
                    code, _ = place_futures_short_order(pair, -add_contracts)
                    if code in (200, 201):
                        with state_lock:
                            old_entry = entry_px
                            old_units = margin_usdt / entry_px
                            add_units = actual_add_usdt / live_px
                            new_margin = round(margin_usdt + actual_add_usdt, 4)
                            new_avg_entry = new_margin / (old_units + add_units)
                            STATE["live_futures_usdt"] = max(0.0, STATE["live_futures_usdt"] - actual_add_usdt)
                            pos["contracts"] = int(pos.get("contracts", 1)) + add_contracts
                            pos["margin_usdt"] = new_margin
                            pos["entry_price"] = new_avg_entry
                            pos["last_step_price"] = live_px
                            pos["step_ups_done"] = steps_done + 1
                            entry_px = new_avg_entry
                            margin_usdt = new_margin
                            add_log(
                                f"🔼 [LIVE BAR RAISE #{steps_done+1}][{pair}] Added ${actual_add_usdt:.2f} short -> "
                                f"Entry Bar raised {fmt_px(old_entry)} → {fmt_px(entry_px)}"
                            )

            pos["last_price"] = live_px
            target_buyback_px = entry_px * (1.0 - (required_drop_pct / 100.0))
            pos["target_buyback_price"] = target_buyback_px

            coin_move_pct = ((live_px - entry_px) / entry_px) * 100.0
            raw_drop_gain_pct = ((entry_px - live_px) / entry_px) * 100.0
            net_pnl_pct = raw_drop_gain_pct - fee_buf
            net_pnl_usdt = margin_usdt * (net_pnl_pct / 100.0)

            pos["coin_move_pct"] = round(coin_move_pct, 2)
            pos["pnl_pct"] = round(net_pnl_pct, 2)
            pos["pnl_usdt"] = round(net_pnl_usdt, 4)
            pos["current_equity_usdt"] = round(margin_usdt + net_pnl_usdt, 4)

            should_close_short = False

            if live_px <= target_buyback_px:
                if not pos.get("trailing_active"):
                    pos["trailing_active"] = True
                    pos["lowest_price"] = live_px
                    pos["status"] = f"🔥 Target Hit! Trailing Waterfall Lock Armed (+{net_pnl_pct:.2f}%)"
                else:
                    if live_px < float(pos.get("lowest_price", live_px)):
                        pos["lowest_price"] = live_px
                    low_watermark = float(pos["lowest_price"])
                    bounce_ceiling_px = min(target_buyback_px, low_watermark * (1.0 + (trail_bounce / 100.0)))
                    if live_px >= bounce_ceiling_px:
                        should_close_short = True
                    else:
                        pos["status"] = f"🩸 Riding Waterfall (+{net_pnl_pct:.2f}%) | Lock @ {fmt_px(bounce_ceiling_px)}"
            else:
                if pos.get("trailing_active") and net_pnl_pct >= skim_pct:
                    should_close_short = True
                else:
                    pos["trailing_active"] = False
                    need_drop = max(0.0, ((live_px - target_buyback_px) / live_px) * 100.0)
                    next_step_px = last_step_px * (1.0 + (step_trigger_pct / 100.0))
                    if int(pos.get("step_ups_done", 0)) < max_steps:
                        pos["status"] = (
                            f"Need -{need_drop:.2f}% drop | "
                            f"Adds +${dynamic_step_add_usdt:.0f} ({step_scale_pct:.0f}%) if price hits {fmt_px(next_step_px)}"
                        )
                    else:
                        pos["status"] = f"Bar Raised {pos.get('step_ups_done')}x (Max) — Waiting for -{need_drop:.2f}% drop"

            if should_close_short:
                profit_usdt = max(0.01, round(margin_usdt * (max(skim_pct, net_pnl_pct) / 100.0), 4))
                if mode == "PAPER":
                    with state_lock:
                        if pair not in STATE["short_positions"]:
                            continue
                        STATE["paper_usdt_balance"] = round(
                            STATE.get("paper_usdt_balance", 0.0) + margin_usdt + profit_usdt, 4
                        )
                        STATE["stats"]["paper_skims"] += 1
                        STATE["stats"]["paper_profit_usdt"] = round(
                            STATE["stats"]["paper_profit_usdt"] + profit_usdt, 4
                        )
                        STATE["skim_ledger"].insert(0, {
                            "time": datetime.utcnow().strftime("%H:%M:%S"),
                            "pair": f"{pair} (PAPER)",
                            "drop": f"-{raw_drop_gain_pct:.2f}%",
                            "short_to_cover": f"{fmt_px(entry_px)} → {fmt_px(live_px)}",
                            "profit": f"+${profit_usdt:.4f}"
                        })
                        add_log(
                            f"🩸💎 [BEAR SKIM WON][{pair}] Covered ${margin_usdt:.0f} Short @ {fmt_px(live_px)} | "
                            f"Banked +${profit_usdt:.4f}!"
                        )
                        STATE["short_positions"].pop(pair, None)
                elif mode == "LIVE" and has_keys:
                    contracts = int(pos.get("contracts", 1))
                    code, _ = place_futures_short_order(pair, abs(contracts))
                    if code in (200, 201):
                        with state_lock:
                            STATE["stats"]["live_skims"] += 1
                            STATE["stats"]["live_profit_usdt"] = round(
                                STATE["stats"]["live_profit_usdt"] + profit_usdt, 4
                            )
                            STATE["skim_ledger"].insert(0, {
                                "time": datetime.utcnow().strftime("%H:%M:%S"),
                                "pair": f"{pair} (LIVE)",
                                "drop": f"-{raw_drop_gain_pct:.2f}%",
                                "short_to_cover": f"{fmt_px(entry_px)} → {fmt_px(live_px)}",
                                "profit": f"+${profit_usdt:.4f}"
                            })
                            add_log(
                                f"🩸💎 [LIVE BEAR SKIM][{pair}] Closed Short @ {fmt_px(live_px)} | "
                                f"Banked +${profit_usdt:.4f}!"
                            )
                            STATE["short_positions"].pop(pair, None)

        # 3) OPEN NEW AUTONOMOUS SHORTS (SKIPS BLOCKED COINS)
        with state_lock:
            current_ap = bool(STATE.get("auto_pilot", True))
            current_open_cnt = len(STATE["short_positions"])

        if current_ap and current_open_cnt < max_slots:
            for cand in radar_list:
                with state_lock:
                    if not STATE.get("auto_pilot", True):
                        break
                    if len(STATE["short_positions"]) >= max_slots:
                        break
                    c_pair = cand["pair"]
                    if c_pair in STATE["short_positions"] or c_pair in STATE.get("blocked_pairs", []):
                        continue
                if cand["bear_score"] < min_bear:
                    break

                c_price = cand["price"]
                target_px = c_price * (1.0 - (required_drop_pct / 100.0))
                step_add_preview = round(max(2.0, slot_size * (step_scale_pct / 100.0)), 2)

                if mode == "PAPER":
                    with state_lock:
                        if (
                            not STATE.get("auto_pilot", True)
                            or len(STATE["short_positions"]) >= max_slots
                            or c_pair in STATE["short_positions"]
                            or c_pair in STATE.get("blocked_pairs", [])
                        ):
                            continue
                        avail = float(STATE.get("paper_usdt_balance", 1000.0))
                        if avail < slot_size:
                            break
                        STATE["paper_usdt_balance"] = round(avail - slot_size, 4)
                        STATE["short_positions"][c_pair] = {
                            "pair": c_pair,
                            "base_slot_usdt": slot_size,
                            "margin_usdt": slot_size,
                            "next_step_add_usdt": step_add_preview,
                            "current_equity_usdt": slot_size,
                            "entry_price": c_price,
                            "last_step_price": c_price,
                            "step_ups_done": 0,
                            "last_price": c_price,
                            "target_buyback_price": target_px,
                            "coin_move_pct": 0.0,
                            "pnl_pct": -fee_buf,
                            "pnl_usdt": round(-slot_size * (fee_buf / 100.0), 4),
                            "bear_score": cand["bear_score"],
                            "trailing_active": False,
                            "lowest_price": c_price,
                            "status": f"1x Short Opened (Score {cand['bear_score']}/100)"
                        }
                        add_log(
                            f"🩸 [AUTO-SHORT][{c_pair}] ${slot_size:.0f} @ {fmt_px(c_price)} "
                            f"(Target: {fmt_px(target_px)} | Step-Up: +${step_add_preview:.0f})"
                        )
                elif mode == "LIVE" and has_keys:
                    with state_lock:
                        if c_pair in STATE.get("blocked_pairs", []):
                            continue
                    avail_fut = float(STATE.get("live_futures_usdt", 0.0))
                    if avail_fut < slot_size:
                        break
                    meta = get_contract_meta(c_pair)
                    c_val = c_price * meta["quanto_multiplier"]
                    num_contracts = max(1, int(slot_size / max(1e-9, c_val)))
                    actual_notional = round(num_contracts * c_val, 4)
                    code, _ = place_futures_short_order(c_pair, -num_contracts)
                    if code in (200, 201):
                        with state_lock:
                            STATE["live_futures_usdt"] = max(0.0, avail_fut - actual_notional)
                            STATE["short_positions"][c_pair] = {
                                "pair": c_pair,
                                "contracts": num_contracts,
                                "base_slot_usdt": actual_notional,
                                "margin_usdt": actual_notional,
                                "next_step_add_usdt": round(actual_notional * (step_scale_pct / 100.0), 2),
                                "current_equity_usdt": actual_notional,
                                "entry_price": c_price,
                                "last_step_price": c_price,
                                "step_ups_done": 0,
                                "last_price": c_price,
                                "target_buyback_price": target_px,
                                "coin_move_pct": 0.0,
                                "pnl_pct": -fee_buf,
                                "pnl_usdt": round(-actual_notional * (fee_buf / 100.0), 4),
                                "bear_score": cand["bear_score"],
                                "trailing_active": False,
                                "lowest_price": c_price,
                                "status": f"Live 1x Short ({num_contracts} contracts)"
                            }
                            add_log(f"🩸 [LIVE SHORT][{c_pair}] ${actual_notional:.2f} @ {fmt_px(c_price)}")

        with state_lock:
            b_set = set(STATE.get("blocked_pairs", []))
            for item in radar_list:
                item["is_blocked"] = (item["pair"] in b_set)
            STATE["radar"] = radar_list[:12]
            STATE["skim_ledger"] = STATE["skim_ledger"][:50]
            STATE["cycle_count"] = STATE.get("cycle_count", 0) + 1
            STATE["last_scan_ms"] = int((time.time() - t0) * 1000)
            STATE["last_update"] = datetime.utcnow().strftime("%H:%M:%S")
            STATE["last_worker_heartbeat"] = time.time()
            save_state()

    finally:
        cycle_lock.release()


def background_worker():
    load_state()
    add_log("🩸 BearSkimmer v3.2 PRO Online — Proportional % Step-Up Bar Mover + Closed-Coin Blocker Active.")
    while True:
        run_bear_cycle(force_manual=False)
        time.sleep(3.5)


def ensure_worker_alive():
    global worker_started
    now = time.time()
    if not worker_started or (now - STATE.get("last_worker_heartbeat", 0) > 14):
        worker_started = True
        STATE["last_worker_heartbeat"] = now
        threading.Thread(target=background_worker, daemon=True).start()


@app.before_request
def check_worker():
    ensure_worker_alive()


# =============================================================================
# PWA MOBILE INSTALLER ENDPOINTS
# =============================================================================
@app.route("/manifest.json")
def pwa_manifest():
    return jsonify({
        "name": "BearSkimmer v3.2 PRO",
        "short_name": "BearSkimmer",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#060309",
        "theme_color": "#ff2a5f",
        "icons": [{"src": "/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any maskable"}]
    })


@app.route("/icon.svg")
def pwa_icon():
    svg = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
      <rect width="512" height="512" rx="115" fill="#12071c"/>
      <circle cx="256" cy="256" r="210" fill="none" stroke="#ff2a5f" stroke-width="18"/>
      <path d="M150 185 L245 280 L300 225 L380 325" fill="none" stroke="#ff2a5f" stroke-width="36" stroke-linecap="round"/>
      <polygon points="395,340 395,255 310,340" fill="#00f5a0"/>
    </svg>"""
    return Response(svg, mimetype="image/svg+xml")


@app.route("/sw.js")
def pwa_sw():
    return Response("self.addEventListener('fetch', e => {});", mimetype="application/javascript")


# =============================================================================
# API ENDPOINTS
# =============================================================================
@app.route("/api/state")
def api_state():
    if STATE["cycle_count"] == 0:
        run_bear_cycle(force_manual=False)
    with state_lock:
        safe = STATE.copy()
        safe["api_key"] = ""
        safe["api_secret"] = ""
        return jsonify(safe)


@app.route("/api/scan_now", methods=["POST"])
def api_scan_now():
    run_bear_cycle(force_manual=True)
    return jsonify({"ok": True})


@app.route("/api/toggle_autopilot", methods=["POST"])
def api_toggle_autopilot():
    with state_lock:
        STATE["auto_pilot"] = not bool(STATE.get("auto_pilot", True))
        add_log(f"🤖 Bear Auto-Pilot switched to: {'ON' if STATE['auto_pilot'] else 'MANUAL (PAUSED)'}")
        save_state()
    return jsonify({"ok": True})


@app.route("/api/toggle_mode", methods=["POST"])
def api_toggle_mode():
    with state_lock:
        STATE["mode"] = "LIVE" if STATE.get("mode") == "PAPER" else "PAPER"
        add_log(f"🔀 Switched BearSkimmer Mode to: {STATE['mode']}")
        save_state()
    run_bear_cycle(force_manual=True)
    return jsonify({"ok": True})


@app.route("/api/reset_usdt_balance", methods=["POST"])
def api_reset_usdt_balance():
    d = request.json or {}
    raw_amt = d.get("amount_usdt", 1000.0)
    keep_profits = bool(d.get("keep_profits", False))
    try:
        new_bal = max(10.0, float(raw_amt))
    except Exception:
        new_bal = 1000.0

    with state_lock:
        STATE["paper_usdt_balance"] = round(new_bal, 4)
        STATE["paper_starting_bankroll"] = round(new_bal, 4)
        STATE["short_positions"] = {}
        if not keep_profits:
            STATE["blocked_pairs"] = []
            STATE["stats"]["paper_skims"] = 0
            STATE["stats"]["paper_profit_usdt"] = 0.0
            STATE["skim_ledger"] = []
            add_log(f"💰 Full Reset: Paper Bankroll set to ${new_bal:.2f} USDT & cleared all slots/blocks!")
        else:
            STATE["auto_pilot"] = False
            add_log(
                f"♻️ Cleared all active slots, restored ${new_bal:.2f} USDT, and paused Auto-Pilot!"
            )
        save_state()
    return jsonify({"ok": True})


@app.route("/api/reset_profit_vault", methods=["POST"])
def api_reset_profit_vault():
    with state_lock:
        STATE["stats"]["paper_skims"] = 0
        STATE["stats"]["paper_profit_usdt"] = 0.0
        STATE["stats"]["live_skims"] = 0
        STATE["stats"]["live_profit_usdt"] = 0.0
        STATE["skim_ledger"] = []
        add_log("🧹 Reset Profit Vault & Skim Ledger to $0.0000.")
        save_state()
    return jsonify({"ok": True})


@app.route("/api/manual_short", methods=["POST"])
def api_manual_short():
    d = request.json or {}
    raw_pair = (d.get("pair") or "").strip().upper().replace("/", "_").replace("-", "_")
    if not raw_pair:
        return jsonify({"ok": False, "error": "Enter a Token Symbol first (e.g. SUI, PEPE, DOGE)"}), 400
    pair = raw_pair if "_" in raw_pair else f"{raw_pair}_USDT"

    raw_amt = d.get("amount_usdt")
    custom_size = float(raw_amt) if (raw_amt and float(raw_amt) >= 2.0) else float(STATE.get("slot_size_usdt", 100.0))

    with state_lock:
        if pair in STATE["short_positions"]:
            return jsonify({"ok": False, "error": f"{pair} is already in your Active Shorts"}), 400
        if pair in STATE.get("blocked_pairs", []):
            STATE["blocked_pairs"].remove(pair)

    try:
        tk_map = fetch_futures_tickers([pair])
        if pair not in tk_map:
            return jsonify({"ok": False, "error": f"Futures contract {pair} not found on Gate.io"}), 400
        c_price = tk_map[pair]["last"]
        fee_buf = float(STATE.get("futures_fee_buffer_pct", 0.12))
        req_drop = float(STATE.get("skim_profit_pct", 1.0)) + fee_buf
        target_px = c_price * (1.0 - (req_drop / 100.0))
        scale_pct = float(STATE.get("step_up_scale_pct", 50.0))
        step_add_preview = round(max(2.0, custom_size * (scale_pct / 100.0)), 2)

        with state_lock:
            avail = float(STATE.get("paper_usdt_balance", 1000.0))
            if STATE["mode"] == "PAPER" and avail < custom_size:
                return jsonify({"ok": False, "error": f"Insufficient Free Paper USDT (${avail:.2f})"}), 400

            if STATE["mode"] == "PAPER":
                STATE["paper_usdt_balance"] = round(avail - custom_size, 4)
                STATE["short_positions"][pair] = {
                    "pair": pair,
                    "base_slot_usdt": custom_size,
                    "margin_usdt": custom_size,
                    "next_step_add_usdt": step_add_preview,
                    "current_equity_usdt": custom_size,
                    "entry_price": c_price,
                    "last_step_price": c_price,
                    "step_ups_done": 0,
                    "last_price": c_price,
                    "target_buyback_price": target_px,
                    "coin_move_pct": 0.0,
                    "pnl_pct": -fee_buf,
                    "pnl_usdt": round(-custom_size * (fee_buf / 100.0), 4),
                    "bear_score": 85,
                    "trailing_active": False,
                    "lowest_price": c_price,
                    "status": "Commanded Custom 1x Short"
                }
                add_log(
                    f"⚡ [CUSTOM SHORT][{pair}] Shorted ${custom_size:.2f} @ {fmt_px(c_price)} "
                    f"(Target: {fmt_px(target_px)} | Step-Up: +${step_add_preview:.0f})"
                )
                save_state()
                return jsonify({"ok": True})
            else:
                avail_fut = float(STATE.get("live_futures_usdt", 0.0))
                if avail_fut < custom_size:
                    return jsonify({"ok": False, "error": f"Insufficient Gate.io Futures USDT (${avail_fut:.2f})"}), 400
                meta = get_contract_meta(pair)
                c_val = c_price * meta["quanto_multiplier"]
                num_c = max(1, int(custom_size / max(1e-9, c_val)))
                actual_notional = round(num_c * c_val, 4)
                code, resp = place_futures_short_order(pair, -num_c)
                if code in (200, 201):
                    STATE["live_futures_usdt"] = max(0.0, avail_fut - actual_notional)
                    STATE["short_positions"][pair] = {
                        "pair": pair,
                        "contracts": num_c,
                        "base_slot_usdt": actual_notional,
                        "margin_usdt": actual_notional,
                        "next_step_add_usdt": round(actual_notional * (scale_pct / 100.0), 2),
                        "current_equity_usdt": actual_notional,
                        "entry_price": c_price,
                        "last_step_price": c_price,
                        "step_ups_done": 0,
                        "last_price": c_price,
                        "target_buyback_price": target_px,
                        "coin_move_pct": 0.0,
                        "pnl_pct": -fee_buf,
                        "pnl_usdt": round(-actual_notional * (fee_buf / 100.0), 4),
                        "bear_score": 85,
                        "trailing_active": False,
                        "lowest_price": c_price,
                        "status": f"Live Custom Short ({num_c} contracts)"
                    }
                    add_log(f"⚡ [LIVE CUSTOM SHORT][{pair}] Shorted ${actual_notional:.2f} @ {fmt_px(c_price)}")
                    save_state()
                    return jsonify({"ok": True})
                return jsonify({"ok": False, "error": str(resp)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/step_up_now", methods=["POST"])
def api_step_up_now():
    pair = (request.json or {}).get("pair", "")
    with state_lock:
        pos = STATE["short_positions"].get(pair)
        if not pos:
            return jsonify({"ok": False, "error": "Position not found"}), 400

        base_slot = float(pos.get("base_slot_usdt") or pos.get("margin_usdt", 100.0))
        scale_pct = float(STATE.get("step_up_scale_pct", 50.0))
        step_add = round(max(2.0, base_slot * (scale_pct / 100.0)), 2)

        if STATE.get("paper_usdt_balance", 0.0) < step_add:
            return jsonify({
                "ok": False,
                "error": f"Need ${step_add:.2f} ({scale_pct:.0f}% of ${base_slot:.0f}) in Free Step-Up USDT Reserve! Close an extra slot to free up cash."
            }), 400

        live_px = float(pos["last_price"])
        old_entry = float(pos["entry_price"])
        old_margin = float(pos["margin_usdt"])
        req_drop = float(STATE.get("skim_profit_pct", 1.0)) + float(STATE.get("futures_fee_buffer_pct", 0.12))

        old_units = old_margin / old_entry
        add_units = step_add / live_px
        new_margin = round(old_margin + step_add, 4)
        new_entry = new_margin / (old_units + add_units)
        new_target = new_entry * (1.0 - (req_drop / 100.0))

        STATE["paper_usdt_balance"] = round(STATE["paper_usdt_balance"] - step_add, 4)
        pos["margin_usdt"] = new_margin
        pos["entry_price"] = new_entry
        pos["last_step_price"] = live_px
        pos["target_buyback_price"] = new_target
        pos["step_ups_done"] = int(pos.get("step_ups_done", 0)) + 1

        add_log(
            f"🔼 [MANUAL PROPORTIONAL BAR RAISE][{pair}] Added ${step_add:.0f} ({scale_pct:.0f}%) @ {fmt_px(live_px)} -> "
            f"Entry Bar raised {fmt_px(old_entry)} → {fmt_px(new_entry)} (New Target: {fmt_px(new_target)})"
        )
        save_state()
    run_bear_cycle(force_manual=True)
    return jsonify({"ok": True})


@app.route("/api/close_short_slot", methods=["POST"])
def api_close_short_slot():
    pair = (request.json or {}).get("pair", "")
    with state_lock:
        pos = STATE["short_positions"].pop(pair, None)
        if pair and pair not in STATE.get("blocked_pairs", []):
            STATE.setdefault("blocked_pairs", []).append(pair)

        if pos:
            if STATE.get("mode") == "PAPER":
                refund = float(pos.get("current_equity_usdt", pos.get("margin_usdt", 0.0)))
                STATE["paper_usdt_balance"] = round(STATE.get("paper_usdt_balance", 0.0) + refund, 4)
                add_log(
                    f"🛑 Closed {pair} short (Refunded ${refund:.2f} to Free Reserve) & BLOCKED Auto-Pilot from re-buying {pair}!"
                )
            elif STATE.get("mode") == "LIVE" and STATE.get("api_key"):
                contracts = int(pos.get("contracts", 1))
                place_futures_short_order(pair, abs(contracts))
                add_log(f"🛑 Closed LIVE {pair} short & BLOCKED Auto-Pilot from re-buying {pair}!")
            save_state()
    return jsonify({"ok": True})


@app.route("/api/unblock_pair", methods=["POST"])
def api_unblock_pair():
    pair = (request.json or {}).get("pair", "")
    with state_lock:
        if pair == "ALL":
            STATE["blocked_pairs"] = []
            add_log("🔓 Unblocked all coins for Auto-Pilot.")
        elif pair in STATE.get("blocked_pairs", []):
            STATE["blocked_pairs"].remove(pair)
            add_log(f"🔓 Unblocked {pair} — Auto-Pilot is allowed to trade it again.")
        save_state()
    return jsonify({"ok": True})


@app.route("/api/update_config", methods=["POST"])
def api_update_config():
    d = request.json or {}
    with state_lock:
        if d.get("slot_size_usdt"):
            STATE["slot_size_usdt"] = max(2.0, float(d["slot_size_usdt"]))
        if d.get("max_short_slots"):
            STATE["max_short_slots"] = max(1, min(12, int(d["max_short_slots"])))
        if d.get("skim_profit_pct"):
            STATE["skim_profit_pct"] = max(0.2, float(d["skim_profit_pct"]))
        if d.get("step_up_trigger_pct"):
            STATE["step_up_trigger_pct"] = max(0.5, float(d["step_up_trigger_pct"]))
        if d.get("step_up_scale_pct"):
            STATE["step_up_scale_pct"] = max(10.0, min(200.0, float(d["step_up_scale_pct"])))
        if d.get("min_bear_score"):
            STATE["min_bear_score"] = max(40, min(95, int(d["min_bear_score"])))
        add_log(
            f"⚙️ Saved Engine Parameters: Base=${STATE['slot_size_usdt']:.0f} | Max Slots={STATE['max_short_slots']} | "
            f"Skim=+{STATE['skim_profit_pct']}% | Step-Up=+{STATE['step_up_trigger_pct']}% (Adds {STATE['step_up_scale_pct']:.0f}% of Slot)"
        )
        save_state()
    run_bear_cycle(force_manual=True)
    return jsonify({"ok": True})


@app.route("/api/sync_keys", methods=["POST"])
def api_sync_keys():
    d = request.json or {}
    rk = (d.get("api_key") or "").strip().replace(" ", "").replace("\n", "").replace("\r", "")
    rs = (d.get("api_secret") or "").strip().replace(" ", "").replace("\n", "").replace("\r", "")
    with state_lock:
        if rk and "Saved" not in rk:
            STATE["api_key"] = rk
        if rs and "Saved" not in rs:
            STATE["api_secret"] = rs
        ak = STATE.get("api_key", "")
        asec = STATE.get("api_secret", "")
    if not ak or not asec:
        return jsonify({"ok": False, "error": "Paste both Gate.io API Key and Secret"}), 400
    try:
        u = fetch_futures_usdt_balance(ak, asec)
        with state_lock:
            STATE["api_connected"] = True
            STATE["live_futures_usdt"] = u
            STATE["api_status_msg"] = f"Verified! Gate.io Futures USDT: ${u:.2f}"
            add_log(f"🔗 Gate.io v4 Futures API Verified! Available USDT: ${u:.2f}")
            save_state()
        return jsonify({"ok": True, "usdt_balance": u})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)[:80]}), 400


# =============================================================================
# PEAK INSTITUTIONAL UI — BEARSKIMMER v3.2 PRO
# =============================================================================
HTML_UI = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <meta name="theme-color" content="#ff2a5f" />
  <link rel="manifest" href="/manifest.json" />
  <link rel="icon" type="image/svg+xml" href="/icon.svg" />
  <title>BearSkimmer v3.2 PRO | Proportional Step-Up Short Engine</title>
  <style>
    :root {
      --bg: #060309;
      --card: rgba(18, 11, 26, 0.94);
      --border: #311c42;
      --crimson: #ff2a5f;
      --neon: #00f5a0;
      --cyan: #00d9f5;
      --amber: #f59e0b;
      --purple: #a855f7;
      --text: #f8fafc;
      --muted: #9ca3af;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', -apple-system, system-ui, sans-serif; }
    body {
      background: radial-gradient(circle at 50% 0%, #2b0920 0%, #060309 75%);
      color: var(--text); padding: 14px; min-height: 100vh;
    }
    .wrap { max-width: 1240px; margin: 0 auto; }

    .hero {
      background: linear-gradient(135deg, rgba(255, 42, 95, 0.16), rgba(168, 85, 247, 0.12));
      border: 1px solid rgba(255, 42, 95, 0.45); border-radius: 18px; padding: 16px 20px; margin-bottom: 14px;
      box-shadow: 0 14px 36px rgba(0,0,0,0.65);
    }
    .hero-top { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 12px; }
    .logo { font-size: 1.5rem; font-weight: 900; display: flex; align-items: center; gap: 10px; letter-spacing: 0.4px; }
    .badge { background: linear-gradient(135deg, var(--crimson), #ff6b35); color: #fff; font-size: 0.72rem; font-weight: 900; padding: 3px 10px; border-radius: 99px; }
    .sub { color: var(--muted); font-size: 0.78rem; margin-top: 4px; }

    .btn-row { display: flex; flex-wrap: wrap; gap: 8px; }
    .btn { cursor: pointer; border: none; border-radius: 10px; padding: 9px 14px; font-weight: 800; font-size: 0.77rem; transition: all 0.12s ease; }
    .btn:active { transform: scale(0.97); }
    .btn-install { background: linear-gradient(135deg, var(--neon), var(--cyan)); color: #022c22; box-shadow: 0 0 14px rgba(0,245,160,0.3); }
    .btn-crimson { background: linear-gradient(135deg, var(--crimson), #e11d48); color: #fff; }
    .btn-amber { background: linear-gradient(135deg, #f59e0b, #d97706); color: #1e1b4b; }
    .btn-outline { background: rgba(255, 42, 95, 0.15); border: 1px solid var(--crimson); color: #fda4af; }

    .strip {
      display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center;
      margin-top: 12px; padding-top: 12px; border-top: 1px solid rgba(255,255,255,0.08);
      font-size: 0.77rem; color: var(--muted); gap: 10px;
    }

    .top-panels { display: grid; grid-template-columns: 1.4fr 1fr; gap: 12px; margin-bottom: 14px; }
    @media (max-width: 920px) { .top-panels { grid-template-columns: 1fr; } }

    .panel-card {
      background: linear-gradient(135deg, rgba(20, 11, 29, 0.96), rgba(34, 11, 29, 0.9));
      border: 1px solid #451c3c; border-radius: 15px; padding: 14px 16px;
    }
    .sec-title { font-size: 0.82rem; font-weight: 900; text-transform: uppercase; color: #fda4af; margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center; }
    .api-grid { display: grid; grid-template-columns: 1fr 1fr auto; gap: 8px; align-items: end; }
    @media (max-width: 640px) { .api-grid { grid-template-columns: 1fr; } }

    .grid-4 { display: grid; grid-template-columns: repeat(auto-fit, minmax(225px, 1fr)); gap: 12px; margin-bottom: 14px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 15px; padding: 15px 16px; }
    .lbl { font-size: 0.7rem; font-weight: 800; text-transform: uppercase; color: var(--muted); display: flex; justify-content: space-between; align-items: center; }
    .val { font-size: 1.45rem; font-weight: 900; margin: 6px 0 4px; font-variant-numeric: tabular-nums; }
    .c-crimson { color: var(--crimson); }
    .c-neon { color: var(--neon); }
    .c-cyan { color: var(--cyan); }
    .c-amber { color: var(--amber); }

    .grid-main { display: grid; grid-template-columns: 1.15fr 1fr; gap: 14px; margin-bottom: 14px; }
    @media (max-width: 960px) { .grid-main { grid-template-columns: 1fr; } }

    .pos-card {
      background: linear-gradient(180deg, rgba(33, 15, 42, 0.96), rgba(16, 8, 22, 0.96));
      border: 1px solid #4a1f43; border-radius: 14px; padding: 13px 15px; margin-bottom: 10px;
    }
    .step-pill { background: rgba(0, 217, 245, 0.16); border: 1px solid var(--cyan); color: var(--cyan); font-size: 0.68rem; font-weight: 800; padding: 2px 8px; border-radius: 99px; }
    .short-pill { background: rgba(255, 42, 95, 0.2); border: 1px solid var(--crimson); color: #fda4af; font-size: 0.68rem; font-weight: 800; padding: 2px 8px; border-radius: 99px; }

    .metrics-box {
      background: rgba(6, 3, 9, 0.65); border: 1px solid #2d173b; border-radius: 10px; padding: 9px 11px; margin: 8px 0;
      display: grid; grid-template-columns: 1fr 1fr; gap: 6px; font-size: 0.75rem;
    }
    .track { height: 8px; background: #050208; border-radius: 99px; overflow: hidden; border: 1px solid #2d173b; margin: 7px 0; }
    .fill { height: 100%; width: 0%; background: linear-gradient(90deg, var(--crimson), var(--neon)); transition: width 0.35s ease; }

    .btn-step {
      background: rgba(0, 245, 160, 0.16); border: 1px solid var(--neon); color: var(--neon);
      padding: 5px 10px; border-radius: 8px; font-size: 0.72rem; font-weight: 800; cursor: pointer;
    }
    .btn-del {
      background: rgba(255, 42, 95, 0.16); border: 1px solid var(--crimson); color: #fda4af;
      padding: 5px 10px; border-radius: 8px; font-size: 0.72rem; font-weight: 800; cursor: pointer;
    }

    .blocked-bar {
      background: rgba(245, 158, 11, 0.12); border: 1px solid rgba(245, 158, 11, 0.4);
      border-radius: 10px; padding: 8px 12px; margin-bottom: 10px; font-size: 0.74rem;
      display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 8px;
    }
    .unblock-chip {
      background: rgba(255,255,255,0.08); border: 1px solid var(--amber); color: #fde68a;
      padding: 3px 8px; border-radius: 6px; font-size: 0.68rem; font-weight: 800; cursor: pointer;
    }

    .chip-bar { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 10px; }
    .chip { background: #1c1026; border: 1px solid #3c204f; color: #e4e4e7; font-size: 0.7rem; font-weight: 800; padding: 5px 10px; border-radius: 99px; cursor: pointer; }
    .chip:hover { border-color: var(--crimson); color: #fff; }

    .input-grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 10px; }
    .input-grid-3 { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 10px; margin-bottom: 10px; }
    label { display: block; font-size: 0.7rem; color: var(--muted); margin-bottom: 4px; font-weight: 700; }
    input { width: 100%; background: #060309; border: 1px solid #3c204f; color: var(--text); padding: 9px 11px; border-radius: 9px; font-size: 0.83rem; font-weight: 700; }

    table { width: 100%; border-collapse: collapse; font-size: 0.77rem; }
    th, td { padding: 8px 6px; text-align: left; border-bottom: 1px solid #24142e; font-variant-numeric: tabular-nums; }
    th { color: var(--muted); font-size: 0.67rem; text-transform: uppercase; }
    .logs { background: #040207; border: 1px solid var(--border); border-radius: 11px; padding: 11px; height: 210px; overflow-y: auto; font-family: monospace; font-size: 0.74rem; color: #e4e4e7; line-height: 1.55; }
  </style>
</head>
<body>
<div class="wrap">

  <!-- HERO HEADER -->
  <div class="hero">
    <div class="hero-top">
      <div>
        <div class="logo">🩸 BEARSKIMMER <span class="badge">v3.2 PROPORTIONAL STEP-UP</span></div>
        <div class="sub">Proportional % Bar-Raising Engine • Closed-Coin Auto-Block Protection • 1-Tap Phone App</div>
      </div>
      <div class="btn-row">
        <button class="btn btn-install" onclick="installApp()" id="installBtn">📲 Install Phone App</button>
        <button class="btn btn-crimson" onclick="toggleAP()" id="apBtn">🤖 Auto-Pilot: ON</button>
        <button class="btn btn-amber" onclick="toggleMode()" id="modeBtn">🔀 Mode: PAPER</button>
        <button class="btn btn-outline" onclick="manualScan()">⚡ Sweep Market</button>
      </div>
    </div>
    <div class="strip">
      <div id="apiStatus" style="font-weight:800; color:var(--neon);">● Paper Simulation Active</div>
      <div>Proportional Step-Up: <strong>Coin Pumps +<span id="ruleTrig">2.0</span>% → Adds <span id="ruleScale">50</span>% of Slot Size (e.g. +$50 on $100) to Raise Bar!</strong></div>
      <div>Last Sweep: <strong id="lastUpdate">--:--:--</strong> (<strong class="c-crimson" id="scanMs">0</strong>ms | Cycle #<span id="cycleNum">0</span>)</div>
    </div>
  </div>

  <!-- API KEY SYNC & USDT BANKROLL RESET PANELS -->
  <div class="top-panels">
    <div class="panel-card">
      <div class="sec-title">
        <span>🔑 Gate.io v4 Futures API Connection</span>
        <span id="apiRightBadge" style="font-size:0.7rem;color:var(--amber);">Paste Keys for Live Mode</span>
      </div>
      <div class="api-grid">
        <div>
          <label>Gate.io v4 API Key</label>
          <input type="password" id="apiKeyInput" placeholder="Paste Gate.io API Key..." autocomplete="off" />
        </div>
        <div>
          <label>Gate.io v4 API Secret</label>
          <input type="password" id="apiSecretInput" placeholder="Paste Gate.io API Secret..." autocomplete="off" />
        </div>
        <button class="btn btn-install" style="height:38px;" onclick="syncApiKeys()" id="syncBtn">🔗 Sync Keys</button>
      </div>
    </div>

    <div class="panel-card">
      <div class="sec-title">
        <span>💰 Reset / Set Virtual USDT Balance</span>
        <button class="btn-del" style="padding:3px 8px;font-size:0.66rem;" onclick="resetProfitVault()">🧹 Zero Profit Vault</button>
      </div>
      <div style="display:grid; grid-template-columns: 1fr auto auto; gap:8px; align-items:end;">
        <div>
          <label>Starting Paper USDT Balance ($)</label>
          <input type="number" id="resetUsdtInput" step="50" value="1000" placeholder="e.g. 1000" />
        </div>
        <button class="btn btn-crimson" style="height:38px;" onclick="resetUsdtBalance(false)">🔄 Full Reset</button>
        <button class="btn btn-outline" style="height:38px;" onclick="resetUsdtBalance(true)">♻️ Clear & Pause Auto</button>
      </div>
    </div>
  </div>

  <!-- 4 SCOREBOARD CARDS -->
  <div class="grid-4">
    <div class="card">
      <div class="lbl">Active Short Margin Deployed</div>
      <div class="val c-crimson" id="deployedVal">$0.00</div>
      <div class="sub">Open 1x Shorts: <strong id="posCount">0</strong> / <span id="maxPos">5</span></div>
    </div>
    <div class="card">
      <div class="lbl">Total Portfolio (Shorts + Free Cash)</div>
      <div class="val c-cyan" id="totalPort">$1000.0000</div>
      <div class="sub">Free Step-Up USDT Reserve: <strong id="freeUsdt" class="c-neon">$500.00</strong></div>
    </div>
    <div class="card">
      <div class="lbl">Bear Skim Profit Vault</div>
      <div class="val c-neon" id="profitVault">+$0.0000</div>
      <div class="sub">Completed Red-Candle Wins: <strong id="skimCount">0</strong></div>
    </div>
    <div class="card">
      <div class="lbl">Target Drop & Step-Up Scale</div>
      <div class="val c-amber" id="dropNeededBox">-1.12% Drop</div>
      <div class="sub">+<span id="cardTrig">2.0</span>% Pump Adds <strong>+<span id="cardScale">50</span>% of Slot</strong></div>
    </div>
  </div>

  <!-- MAIN WORKSPACE -->
  <div class="grid-main">
    <div class="card">
      <div class="sec-title">
        <span>🩸 Active 1x Short Positions (Win When Price Falls ↓)</span>
        <span style="font-size:0.7rem;color:var(--cyan);">Closing a Coin Blocks Auto-Rebuy</span>
      </div>
      <div id="blockedBanner" style="display:none;" class="blocked-bar"></div>
      <div id="positionsBox"></div>
    </div>

    <div>
      <!-- MANUAL SHORT COMMAND DECK -->
      <div class="card" style="margin-bottom:14px;">
        <div class="sec-title">➕ Command Custom 1x Short Position</div>
        <div class="chip-bar">
          <span class="chip" onclick="fillPair('SUI_USDT')">SUI</span>
          <span class="chip" onclick="fillPair('PEPE_USDT')">PEPE</span>
          <span class="chip" onclick="fillPair('DOGE_USDT')">DOGE</span>
          <span class="chip" onclick="fillPair('SOL_USDT')">SOL</span>
          <span class="chip" onclick="fillPair('FET_USDT')">FET</span>
          <span class="chip" onclick="fillPair('AVAX_USDT')">AVAX</span>
          <span class="chip" onclick="fillPair('DOT_USDT')">DOT</span>
          <span class="chip" onclick="fillPair('ARB_USDT')">ARB</span>
          <span class="chip" onclick="fillPair('WIF_USDT')">WIF</span>
          <span class="chip" onclick="fillPair('NEAR_USDT')">NEAR</span>
          <span class="chip" onclick="fillPair('XRP_USDT')">XRP</span>
          <span class="chip" onclick="fillPair('ADA_USDT')">ADA</span>
        </div>
        <div class="input-grid-2">
          <div>
            <label>Token Symbol (Any Gate.io Futures Pair)</label>
            <input type="text" id="customPairInput" placeholder="e.g. SUI, PEPE, DOGE" />
          </div>
          <div>
            <label>Short Amount to Deploy (USDT)</label>
            <input type="number" id="customAmtInput" step="10" value="100" placeholder="e.g. 100" />
          </div>
        </div>
        <button class="btn btn-crimson" style="width:100%;padding:11px;" onclick="launchCustomShort()">🩸 Launch Custom 1x Short Now</button>
      </div>

      <!-- ENGINE PARAMETERS -->
      <div class="card">
        <div class="sec-title">⚙️ Auto-Pilot & Proportional Step-Up Settings</div>
        <div class="input-grid-3">
          <div>
            <label>Auto-Short Size ($)</label>
            <input type="number" id="cfgSize" step="10" value="100" />
          </div>
          <div>
            <label>Max Active Shorts</label>
            <input type="number" id="cfgMaxPos" step="1" value="5" />
          </div>
          <div>
            <label>Skim Profit Target (%)</label>
            <input type="number" id="cfgTarget" step="0.1" value="1.0" />
          </div>
        </div>
        <div class="input-grid-3">
          <div>
            <label>Step-Up Trigger Pump (%)</label>
            <input type="number" id="cfgStepTrig" step="0.2" value="2.0" />
          </div>
          <div>
            <label>Step-Up Add Scale (% of Slot)</label>
            <input type="number" id="cfgStepScale" step="5" value="50" />
          </div>
          <div>
            <label>Min Bear Score (40-95)</label>
            <input type="number" id="cfgBear" step="1" value="68" />
          </div>
        </div>
        <button class="btn btn-install" style="width:100%;padding:11px;" onclick="saveConfig()">💾 Save Engine & Proportional Step-Up Settings</button>
      </div>
    </div>
  </div>

  <!-- BOTTOM WORKSPACE -->
  <div class="grid-main">
    <div class="card">
      <div class="sec-title">
        <span>📡 16-Market Bearish Rejection Radar</span>
        <span style="font-size:0.7rem;color:var(--muted);">Score >= 68 Auto-Shorts (Unless Blocked)</span>
      </div>
      <div style="overflow-x:auto; max-height:260px;">
        <table>
          <thead>
            <tr>
              <th>Contract</th>
              <th>Live Price (24h)</th>
              <th>Bear Score</th>
              <th>5m RSI</th>
              <th>Sellers %</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody id="radarBody"></tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <div class="sec-title">💎 Verified Bear Skim Profit Ledger</div>
      <div style="max-height:260px; overflow-y:auto;">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Contract</th>
              <th>Drop</th>
              <th>Entry Bar → Covered</th>
              <th>Profit</th>
            </tr>
          </thead>
          <tbody id="ledgerBody"></tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="card">
    <div class="sec-title">📡 Real-Time Short & Step-Up Telemetry Logs</div>
    <div class="logs" id="logBox"></div>
  </div>

</div>

<script>
  let deferredPrompt = null;
  window.addEventListener('beforeinstallprompt', (e) => {
    e.preventDefault();
    deferredPrompt = e;
  });

  async function installApp() {
    if (deferredPrompt) {
      deferredPrompt.prompt();
      deferredPrompt = null;
    } else {
      alert("To install BearSkimmer Pro on your phone: Tap your browser menu (⋮) and select 'Install app' or 'Add to Home screen'.");
    }
  }

  function fmtPx(val) {
    const v = Number(val || 0);
    if (v === 0) return '$0.0000';
    if (v < 0.0001) return '$' + v.toFixed(8);
    if (v < 0.01) return '$' + v.toFixed(6);
    return '$' + v.toFixed(4);
  }

  function fillPair(p) {
    document.getElementById('customPairInput').value = p;
  }

  async function syncApiKeys() {
    const btn = document.getElementById('syncBtn');
    const k = document.getElementById('apiKeyInput').value.trim();
    const s = document.getElementById('apiSecretInput').value.trim();
    btn.innerText = 'Syncing...';
    const r = await fetch('/api/sync_keys', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ api_key: k, api_secret: s })
    });
    const d = await r.json();
    if (d.ok) {
      document.getElementById('apiKeyInput').value = '';
      document.getElementById('apiSecretInput').value = '';
      btn.innerText = '✅ Synced';
    } else {
      alert('API Notice: ' + (d.error || 'Check keys'));
      btn.innerText = '❌ Retry';
    }
    await refresh();
    setTimeout(() => { btn.innerText = '🔗 Sync Keys'; }, 2500);
  }

  async function resetUsdtBalance(keepProfits) {
    const amt = document.getElementById('resetUsdtInput').value || 1000;
    await fetch('/api/reset_usdt_balance', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ amount_usdt: amt, keep_profits: keepProfits })
    });
    await refresh();
  }

  async function resetProfitVault() {
    await fetch('/api/reset_profit_vault', { method: 'POST' });
    await refresh();
  }

  async function launchCustomShort(pairOverride) {
    const pair = pairOverride || document.getElementById('customPairInput').value.trim();
    const amt = document.getElementById('customAmtInput').value || 100;
    if (!pair) {
      alert('Select or type a token symbol first.');
      return;
    }
    const r = await fetch('/api/manual_short', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ pair, amount_usdt: amt })
    });
    const d = await r.json();
    if (!d.ok) alert(d.error || 'Could not open short');
    await refresh();
  }

  async function stepUpNow(pair) {
    const r = await fetch('/api/step_up_now', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ pair })
    });
    const d = await r.json();
    if (!d.ok) alert(d.error);
    await refresh();
  }

  async function closeSlot(pair) {
    await fetch('/api/close_short_slot', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ pair })
    });
    await refresh();
  }

  async function unblockPair(pair) {
    await fetch('/api/unblock_pair', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ pair })
    });
    await refresh();
  }

  async function toggleAP() {
    await fetch('/api/toggle_autopilot', { method: 'POST' });
    await refresh();
  }

  async function toggleMode() {
    await fetch('/api/toggle_mode', { method: 'POST' });
    await refresh();
  }

  async function manualScan() {
    await fetch('/api/scan_now', { method: 'POST' });
    await refresh();
  }

  async function saveConfig() {
    await fetch('/api/update_config', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        slot_size_usdt: document.getElementById('cfgSize').value,
        max_short_slots: document.getElementById('cfgMaxPos').value,
        skim_profit_pct: document.getElementById('cfgTarget').value,
        step_up_trigger_pct: document.getElementById('cfgStepTrig').value,
        step_up_scale_pct: document.getElementById('cfgStepScale').value,
        min_bear_score: document.getElementById('cfgBear').value
      })
    });
    await refresh();
  }

  let cfgLoaded = false;
  async function refresh() {
    try {
      const r = await fetch('/api/state');
      const s = await r.json();

      if (!cfgLoaded) {
        document.getElementById('cfgSize').value = s.slot_size_usdt || 100;
        document.getElementById('cfgMaxPos').value = s.max_short_slots || 5;
        document.getElementById('cfgTarget').value = s.skim_profit_pct || 1.0;
        document.getElementById('cfgStepTrig').value = s.step_up_trigger_pct || 2.0;
        document.getElementById('cfgStepScale').value = s.step_up_scale_pct || 50;
        document.getElementById('cfgBear').value = s.min_bear_score || 68;
        document.getElementById('resetUsdtInput').value = s.paper_starting_bankroll || 1000;
        cfgLoaded = true;
      }

      document.getElementById('lastUpdate').innerText = s.last_update;
      document.getElementById('scanMs').innerText = s.last_scan_ms;
      document.getElementById('cycleNum').innerText = s.cycle_count;

      const trigVal = Number(s.step_up_trigger_pct || 2.0).toFixed(1);
      const scaleVal = Number(s.step_up_scale_pct || 50.0).toFixed(0);
      document.getElementById('ruleTrig').innerText = trigVal;
      document.getElementById('cardTrig').innerText = trigVal;
      document.getElementById('ruleScale').innerText = scaleVal;
      document.getElementById('cardScale').innerText = scaleVal;

      const needDrop = (Number(s.skim_profit_pct || 1.0) + Number(s.futures_fee_buffer_pct || 0.12)).toFixed(2);
      document.getElementById('dropNeededBox').innerText = '-' + needDrop + '% Drop';

      const apBtn = document.getElementById('apBtn');
      apBtn.innerText = s.auto_pilot ? '🤖 Auto-Pilot: ON' : '✋ Auto-Pilot: PAUSED';
      apBtn.className = s.auto_pilot ? 'btn btn-crimson' : 'btn btn-outline';

      const modeBtn = document.getElementById('modeBtn');
      modeBtn.innerText = s.mode === 'LIVE' ? '🔀 Mode: LIVE FUTURES' : '🔀 Mode: PAPER';
      modeBtn.className = s.mode === 'LIVE' ? 'btn btn-install' : 'btn btn-amber';

      document.getElementById('apiStatus').innerText = '● ' + (s.api_status_msg || '');
      document.getElementById('apiRightBadge').innerText = s.api_connected ? '✅ Gate.io Connected' : 'Paste Keys for Live Mode';

      const blocked = s.blocked_pairs || [];
      const bBanner = document.getElementById('blockedBanner');
      if (blocked.length > 0) {
        bBanner.style.display = 'flex';
        const chips = blocked.map(bp =>
          `<span class="unblock-chip" onclick="unblockPair('${bp}')">🚫 ${bp} (Tap to Unblock)</span>`
        ).join(' ');
        bBanner.innerHTML = `
          <div><strong>Closed & Blocked from Auto-Buy:</strong> ${chips}</div>
          <button class="unblock-chip" onclick="unblockPair('ALL')">🔓 Unblock All</button>
        `;
      } else {
        bBanner.style.display = 'none';
      }

      const posMap = s.short_positions || {};
      const keys = Object.keys(posMap);
      let sumEq = 0;
      keys.forEach(k => { sumEq += Number(posMap[k].current_equity_usdt || 0); });

      const freeCash = s.mode === 'LIVE' ? Number(s.live_futures_usdt || 0) : Number(s.paper_usdt_balance || 0);
      document.getElementById('deployedVal').innerText = '$' + sumEq.toFixed(2);
      document.getElementById('posCount').innerText = keys.length;
      document.getElementById('maxPos').innerText = s.max_short_slots || 5;
      document.getElementById('totalPort').innerText = '$' + (sumEq + freeCash).toFixed(4);
      document.getElementById('freeUsdt').innerText = '$' + freeCash.toFixed(2);

      const prof = s.mode === 'LIVE' ? s.stats.live_profit_usdt : s.stats.paper_profit_usdt;
      const skims = s.mode === 'LIVE' ? s.stats.live_skims : s.stats.paper_skims;
      document.getElementById('profitVault').innerText = '+$' + Number(prof || 0).toFixed(4);
      document.getElementById('skimCount').innerText = skims || 0;

      const pBox = document.getElementById('positionsBox');
      if (keys.length === 0) {
        pBox.innerHTML = `<div style="text-align:center;color:#9ca3af;padding:22px;font-size:0.82rem;">No active shorts — Auto-Pilot is ${s.auto_pilot ? 'scanning unblocked coins' : 'PAUSED (Command a coin on the right)'}.</div>`;
      } else {
        pBox.innerHTML = keys.map(k => {
          const p = posMap[k];
          const pnl = Number(p.pnl_pct || 0);
          const pnlSign = pnl >= 0 ? '+' : '';
          const pnlUsdt = Number(p.pnl_usdt || 0);
          const steps = Number(p.step_ups_done || 0);
          const prog = Math.max(0, Math.min(100, (pnl / Number(s.skim_profit_pct || 1.0)) * 100));
          const dynStepAdd = Number(p.next_step_add_usdt || (Number(p.base_slot_usdt || p.margin_usdt || 100) * (Number(s.step_up_scale_pct || 50) / 100)));

          return `
            <div class="pos-card">
              <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px;">
                <div style="display:flex;align-items:center;gap:6px;">
                  <strong style="font-size:1rem;">${p.pair}</strong>
                  <span class="short-pill">$${Number(p.margin_usdt).toFixed(0)} Short</span>
                  <span class="step-pill">Bar Raised ${steps}x</span>
                </div>
                <div style="font-weight:900;font-size:0.9rem;color:${pnl >= 0 ? '#00f5a0' : '#f59e0b'}">
                  ${pnlSign}${pnl.toFixed(2)}% (${pnlSign}$${pnlUsdt.toFixed(2)})
                </div>
              </div>

              <div class="metrics-box">
                <div>Entry Bar: <strong>${fmtPx(p.entry_price)}</strong></div>
                <div>Buyback Target: <strong class="c-neon">${fmtPx(p.target_buyback_price)}</strong></div>
                <div>Live Price: <strong>${fmtPx(p.last_price)}</strong></div>
                <div>Coin Move: <strong style="color:${p.coin_move_pct <= 0 ? '#00f5a0' : '#f59e0b'}">${p.coin_move_pct > 0 ? '+' : ''}${Number(p.coin_move_pct || 0).toFixed(2)}%</strong></div>
              </div>

              <div style="display:flex;justify-content:space-between;font-size:0.71rem;color:var(--muted);">
                <span>${p.status}</span>
                <strong class="c-neon">${prog.toFixed(0)}% to Skim</strong>
              </div>
              <div class="track"><div class="fill" style="width:${prog.toFixed(1)}%"></div></div>

              <div style="display:flex;justify-content:space-between;align-items:center;margin-top:8px;">
                <button class="btn-step" onclick="stepUpNow('${p.pair}')">🔼 Raise Bar (+$${dynStepAdd.toFixed(0)} / ${scaleVal}%)</button>
                <button class="btn-del" onclick="closeSlot('${p.pair}')">✖ Close & Block Rebuy</button>
              </div>
            </div>
          `;
        }).join('');
      }

      const rb = document.getElementById('radarBody');
      rb.innerHTML = (s.radar || []).map(item => {
        const chgSign = item.change_24h >= 0 ? '+' : '';
        return `
          <tr>
            <td><strong>${item.pair}</strong> ${item.is_blocked ? '<span style="color:#f59e0b;font-size:0.65rem;">(BLOCKED)</span>' : ''}</td>
            <td>${fmtPx(item.price)} <span style="color:${item.change_24h < 0 ? '#ff2a5f' : '#00f5a0'}">(${chgSign}${item.change_24h}%)</span></td>
            <td><span class="short-pill">${item.bear_score}/100</span></td>
            <td>${item.rsi_14}</td>
            <td>${item.ask_sell_pct}%</td>
            <td><button class="btn btn-outline" style="padding:4px 9px;font-size:0.7rem;" onclick="launchCustomShort('${item.pair}')">🩸 Short</button></td>
          </tr>
        `;
      }).join('');

      const lb = document.getElementById('ledgerBody');
      if (s.skim_ledger && s.skim_ledger.length > 0) {
        lb.innerHTML = s.skim_ledger.map(t => `
          <tr>
            <td>${t.time}</td>
            <td><strong>${t.pair}</strong></td>
            <td class="c-crimson"><strong>${t.drop}</strong></td>
            <td>${t.short_to_cover}</td>
            <td class="c-neon"><strong>${t.profit}</strong></td>
          </tr>
        `).join('');
      } else {
        lb.innerHTML = `<tr><td colspan="5" style="text-align:center;color:#9ca3af;padding:18px;">Waiting for next red-candle skim...</td></tr>`;
      }

      document.getElementById('logBox').innerHTML = (s.logs || []).join('<br/>');
    } catch (e) {}
  }

  setInterval(refresh, 2500);
  refresh();
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML_UI)


if __name__ == "__main__":
    ensure_worker_alive()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
