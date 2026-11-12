import os
import time
import math
import json
import hmac
import hashlib
import threading
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, request, jsonify, render_template_string

app = Flask(__name__)

STATE_FILE = "bearskimmer_v1_state.json"
GATE_HOST = "https://api.gateio.ws"
GATE_PREFIX = "/api/v4"

# Liquid Mid/Large-Cap Futures Universe (Safe from micro-cap random +100% squeezes)
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
    "mode": "PAPER",  # Starts in $100 Paper Simulation Mode
    "auto_pilot": True,
    "max_short_slots": 4,
    "slot_size_usdt": 12.0,
    "min_bear_score": 68,
    "skim_profit_pct": 1.00,     # +1.00% Net Profit on the Short
    "trail_bounce_pct": 0.22,    # Locks profit if price bounces +0.22% from bottom
    "futures_fee_buffer_pct": 0.12,  # Gate.io Futures round-trip taker fee (~0.10%-0.12%)
    "api_key": os.environ.get("GATE_API_KEY", "").strip(),
    "api_secret": os.environ.get("GATE_API_SECRET", "").strip(),
    "api_connected": False,
    "api_status_msg": "Paper Mode Active ($100 Sim) — Paste Keys for Live Futures",
    "live_futures_usdt": 0.0,
    "paper_usdt_balance": 100.0,
    "short_positions": {},
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
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                saved = json.load(f)
                sk = (saved.get("api_key") or "").strip()
                ss = (saved.get("api_secret") or "").strip()
                STATE.update(saved)
                if sk:
                    STATE["api_key"] = sk
                if ss:
                    STATE["api_secret"] = ss
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
    """Fetches real-time Gate.io USDT Perpetual Futures tickers."""
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
                    "mark_price": float(t.get("mark_price") or last),
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
            cached = {
                "quanto_multiplier": float(info.get("quanto_multiplier") or 1.0),
                "order_size_min": int(info.get("order_size_min") or 1),
                "ts": now
            }
        else:
            cached = {"quanto_multiplier": 1.0, "order_size_min": 1, "ts": now}
        CONTRACT_META_CACHE[contract] = cached
    return cached


def fetch_futures_usdt_balance(custom_key=None, custom_secret=None):
    url = f"{GATE_PREFIX}/futures/usdt/accounts"
    headers = gate_sign("GET", url, "", "", custom_key, custom_secret)
    r = HTTP_SESSION.get(f"{GATE_HOST}{url}", headers=headers, timeout=5)
    if r.status_code != 200:
        raise ValueError(f"Gate.io Futures HTTP {r.status_code}: {r.text[:80]}")
    data = r.json()
    return float(data.get("available") or 0.0)


# =============================================================================
# BEARISH BREAKDOWN RADAR (FINDS WEAK COINS READY TO DROP)
# =============================================================================
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
    """
    Computes a 0-100 'Bearish Breakdown Score':
    - High score (70+) when Order Book Ask Walls (Sellers) > Bid Walls (Buyers),
      5m candle is rejecting below its EMA/High, and RSI is in the sweet spot (35 to 68)
      where it still has plenty of room to fall without being at rock bottom.
    """
    now = time.time()
    cached = BEAR_RADAR_CACHE.get(pair)
    if cached and (now - cached["ts"] < 14.0):
        cached["price"] = tk["last"]
        return cached

    price = tk["last"]
    rsi_14 = 50.0
    ask_sell_pct = 50.0
    below_ema = True
    red_candle = False

    try:
        c_resp = HTTP_SESSION.get(
            f"{GATE_HOST}{GATE_PREFIX}/futures/usdt/candlesticks",
            params={"contract": pair, "interval": "5m", "limit": 30},
            timeout=3.5
        )
        if c_resp.status_code == 200:
            raw = c_resp.json()
            closes = [float(c["c"]) for c in raw if "c" in c]
            opens = [float(c["o"]) for c in raw if "o" in c]
            if len(closes) >= 16:
                closes[-1] = price
                rsi_14 = calc_rsi(closes, 14)
                sma_10 = sum(closes[-10:]) / 10.0
                below_ema = price <= sma_10
                red_candle = price < opens[-1]
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

    # 1) RSI Bearish Room-to-Fall Score (0-35 pts)
    # Don't short if RSI < 28 (already oversold bounce risk). Best short zone is RSI 42-68 rejecting downward.
    if rsi_14 < 28:
        s_rsi = 8.0
    elif 40 <= rsi_14 <= 68:
        s_rsi = 34.0
    elif 32 <= rsi_14 < 40:
        s_rsi = 25.0
    else:
        s_rsi = 18.0

    # 2) Order-Book Seller Pressure (Ask Walls > Bid Walls) (0-35 pts)
    s_ob = max(5.0, min(35.0, ((ask_sell_pct - 35.0) / 35.0) * 35.0))

    # 3) Downtrend & Red Candle Momentum (0-30 pts)
    s_trend = (16.0 if below_ema else 6.0) + (14.0 if red_candle else 5.0)
    if tk["change_24h"] < -1.0:
        s_trend = min(30.0, s_trend + 5.0)

    bear_score = int(round(max(1.0, min(99.0, s_rsi + s_ob + s_trend))))

    if bear_score >= 74:
        signal = "STRONG SHORT DUMP"
    elif bear_score >= 65:
        signal = "BEARISH BREAKDOWN"
    elif rsi_14 < 28:
        signal = "TOO OVERSOLD — WAIT"
    else:
        signal = "NEUTRAL CHOP"

    res = {
        "pair": pair,
        "price": price,
        "change_24h": round(tk["change_24h"], 2),
        "funding_rate": round(tk["funding_rate"], 4),
        "bear_score": bear_score,
        "rsi_14": round(rsi_14, 1),
        "ask_sell_pct": round(ask_sell_pct, 1),
        "signal": signal,
        "ts": now
    }
    BEAR_RADAR_CACHE[pair] = res
    return res


def place_futures_short_order(contract, size_contracts):
    """
    Places a Gate.io USDT Futures Market Order.
    Negative size = Open Short; Positive size with reduce_only = Buy Back / Close Short.
    """
    url = f"{GATE_PREFIX}/futures/usdt/orders"
    body = {
        "contract": contract,
        "size": int(size_contracts),
        "price": "0",
        "tif": "ioc"
    }
    if size_contracts > 0:
        body["reduce_only"] = True
    body_str = json.dumps(body)
    headers = gate_sign("POST", url, "", body_str)
    r = HTTP_SESSION.post(f"{GATE_HOST}{url}", headers=headers, data=body_str, timeout=7)
    return r.status_code, r.json()


# =============================================================================
# MAIN BEARSKIMMER CYCLE (SHORT HIGH -> BUY BACK LOW ON DUMPS)
# =============================================================================
def run_bear_cycle(force_manual=False):
    if not cycle_lock.acquire(blocking=force_manual):
        return

    t0 = time.time()
    try:
        with state_lock:
            mode = STATE.get("mode", "PAPER")
            auto_pilot = bool(STATE.get("auto_pilot", True))
            max_slots = int(STATE.get("max_short_slots", 4))
            slot_size = float(STATE.get("slot_size_usdt", 12.0))
            min_bear = int(STATE.get("min_bear_score", 68))
            skim_pct = float(STATE.get("skim_profit_pct", 1.00))
            trail_bounce = float(STATE.get("trail_bounce_pct", 0.22))
            fee_buf = float(STATE.get("futures_fee_buffer_pct", 0.12))
            has_keys = bool(STATE.get("api_key") and STATE.get("api_secret"))
            active_pairs = list(STATE["short_positions"].keys())

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
                    STATE["api_status_msg"] = f"API Notice: {str(e)[:55]}"

        all_pairs = list(set(BEAR_UNIVERSE + active_pairs))
        try:
            tk_map = fetch_futures_tickers(all_pairs)
        except Exception as e:
            add_log(f"Futures ticker notice: {str(e)[:55]}")
            return

        # 1) SCORE ALL 16 PAIRS FOR BEARISH BREAKDOWNS
        radar_list = []
        for pair in BEAR_UNIVERSE:
            tk = tk_map.get(pair)
            if not tk:
                continue
            radar_list.append(compute_bear_score(pair, tk))

        radar_list.sort(key=lambda x: x["bear_score"], reverse=True)

        # 2) MANAGE ACTIVE SHORT POSITIONS (PROFIT WHEN PRICE DROPS!)
        required_drop_pct = skim_pct + fee_buf  # e.g. 1.00% + 0.12% = -1.12% price drop needed

        for pair in list(STATE["short_positions"].keys()):
            pos = STATE["short_positions"].get(pair)
            if not pos or pair not in tk_map:
                continue

            live_px = tk_map[pair]["last"]
            entry_px = float(pos["entry_price"])
            margin_usdt = float(pos["margin_usdt"])

            pos["last_price"] = live_px
            # Target Buyback Price is LOWER than Entry Price!
            target_buyback_px = entry_px * (1.0 - (required_drop_pct / 100.0))
            pos["target_buyback_price"] = target_buyback_px

            # Short PnL Math: When live_px < entry_px, PnL is POSITIVE!
            raw_drop_pct = ((entry_px - live_px) / entry_px) * 100.0
            net_pnl_pct = raw_drop_pct - fee_buf
            net_pnl_usdt = margin_usdt * (net_pnl_pct / 100.0)

            pos["price_drop_pct"] = round(raw_drop_pct, 2)
            pos["pnl_pct"] = round(net_pnl_pct, 2)
            pos["pnl_usdt"] = round(net_pnl_usdt, 4)
            pos["current_equity_usdt"] = round(margin_usdt + net_pnl_usdt, 4)

            should_close_short = False

            # Check if price dropped below Target Buyback Price
            if live_px <= target_buyback_px:
                if not pos.get("trailing_active"):
                    pos["trailing_active"] = True
                    pos["lowest_price"] = live_px
                    pos["status"] = f"🔥 Trailing Dump Lock Armed @ {fmt_px(live_px)} (+{net_pnl_pct:.2f}% Profit)"
                else:
                    if live_px < float(pos.get("lowest_price", live_px)):
                        pos["lowest_price"] = live_px
                    low_watermark = float(pos["lowest_price"])
                    # Ceiling trigger: if price bounces +0.22% off the bottom, cash out immediately!
                    bounce_ceiling_px = min(target_buyback_px, low_watermark * (1.0 + (trail_bounce / 100.0)))
                    if live_px >= bounce_ceiling_px:
                        should_close_short = True
                    else:
                        pos["status"] = (
                            f"🩸 Riding Red Waterfall (+{net_pnl_pct:.2f}%) | "
                            f"Bottom {fmt_px(low_watermark)} | Lock {fmt_px(bounce_ceiling_px)}"
                        )
            else:
                if pos.get("trailing_active") and net_pnl_pct >= skim_pct:
                    should_close_short = True
                else:
                    pos["trailing_active"] = False
                    pos["status"] = (
                        f"Shorting — Waiting for -{required_drop_pct:.2f}% Drop "
                        f"(Now: {net_pnl_pct:+.2f}% | Target: {fmt_px(target_buyback_px)})"
                    )

            if should_close_short:
                profit_usdt = max(0.01, round(margin_usdt * (max(skim_pct, net_pnl_pct) / 100.0), 4))
                if mode == "PAPER":
                    with state_lock:
                        STATE["paper_usdt_balance"] = round(
                            STATE.get("paper_usdt_balance", 0.0) + margin_usdt + profit_usdt, 4
                        )
                        STATE["stats"]["paper_skims"] += 1
                        STATE["stats"]["paper_profit_usdt"] = round(
                            STATE["stats"]["paper_profit_usdt"] + profit_usdt, 4
                        )
                        STATE["skim_ledger"].insert(0, {
                            "time": datetime.utcnow().strftime("%H:%M:%S"),
                            "pair": f"{pair} (PAPER SHORT)",
                            "drop": f"-{raw_drop_pct:.2f}%",
                            "short_to_cover": f"Short {fmt_px(entry_px)} → Covered {fmt_px(live_px)}",
                            "profit": f"+${profit_usdt:.4f}"
                        })
                        add_log(
                            f"🩸💎 [BEAR SKIM WON][{pair}] Price dumped -{raw_drop_pct:.2f}% "
                            f"({fmt_px(entry_px)} → {fmt_px(live_px)}) | Banked +${profit_usdt:.4f}!"
                        )
                        STATE["short_positions"].pop(pair, None)
                elif mode == "LIVE" and has_keys:
                    contracts = int(pos.get("contracts", 1))
                    code, resp = place_futures_short_order(pair, abs(contracts))
                    if code in (200, 201):
                        with state_lock:
                            STATE["stats"]["live_skims"] += 1
                            STATE["stats"]["live_profit_usdt"] = round(
                                STATE["stats"]["live_profit_usdt"] + profit_usdt, 4
                            )
                            STATE["skim_ledger"].insert(0, {
                                "time": datetime.utcnow().strftime("%H:%M:%S"),
                                "pair": f"{pair} (LIVE SHORT)",
                                "drop": f"-{raw_drop_pct:.2f}%",
                                "short_to_cover": f"Short {fmt_px(entry_px)} → Covered {fmt_px(live_px)}",
                                "profit": f"+${profit_usdt:.4f}"
                            })
                            add_log(
                                f"🩸💎 [LIVE BEAR SKIM][{pair}] Closed Short @ {fmt_px(live_px)} | "
                                f"Profit Locked: +${profit_usdt:.4f}!"
                            )
                            STATE["short_positions"].pop(pair, None)

        # 3) OPEN NEW AUTONOMOUS SHORTS ON WEAK COINS (IF AUTO-PILOT ON)
        if auto_pilot and len(STATE["short_positions"]) < max_slots:
            for cand in radar_list:
                with state_lock:
                    if len(STATE["short_positions"]) >= max_slots:
                        break
                    c_pair = cand["pair"]
                    if c_pair in STATE["short_positions"]:
                        continue
                if cand["bear_score"] < min_bear:
                    break

                c_price = cand["price"]
                target_px = c_price * (1.0 - (required_drop_pct / 100.0))

                if mode == "PAPER":
                    with state_lock:
                        if len(STATE["short_positions"]) >= max_slots or c_pair in STATE["short_positions"]:
                            continue
                        avail = float(STATE.get("paper_usdt_balance", 100.0))
                        if avail < slot_size:
                            break
                        STATE["paper_usdt_balance"] = round(avail - slot_size, 4)
                        STATE["short_positions"][c_pair] = {
                            "pair": c_pair,
                            "margin_usdt": slot_size,
                            "current_equity_usdt": slot_size,
                            "entry_price": c_price,
                            "last_price": c_price,
                            "target_buyback_price": target_px,
                            "price_drop_pct": 0.0,
                            "pnl_pct": 0.0,
                            "pnl_usdt": 0.0,
                            "bear_score": cand["bear_score"],
                            "trailing_active": False,
                            "lowest_price": c_price,
                            "status": f"1x Short Opened (Bear Score {cand['bear_score']}/100)"
                        }
                        add_log(
                            f"🩸 [PAPER AUTO-SHORT][{c_pair}] Bear Score {cand['bear_score']}/100 "
                            f"(Sellers: {cand['ask_sell_pct']}% | RSI: {cand['rsi_14']}) — "
                            f"Shorted ${slot_size:.2f} @ {fmt_px(c_price)} (Target: {fmt_px(target_px)})"
                        )
                elif mode == "LIVE" and has_keys:
                    avail_fut = float(STATE.get("live_futures_usdt", 0.0))
                    if avail_fut < slot_size:
                        break
                    meta = get_contract_meta(c_pair)
                    mult = meta["quanto_multiplier"]
                    contract_val_usdt = c_price * mult
                    num_contracts = max(1, int(slot_size / max(1e-9, contract_val_usdt)))
                    actual_notional = round(num_contracts * contract_val_usdt, 4)
                    code, resp = place_futures_short_order(c_pair, -num_contracts)
                    if code in (200, 201):
                        with state_lock:
                            STATE["live_futures_usdt"] = max(0.0, avail_fut - actual_notional)
                            STATE["short_positions"][c_pair] = {
                                "pair": c_pair,
                                "contracts": num_contracts,
                                "margin_usdt": actual_notional,
                                "current_equity_usdt": actual_notional,
                                "entry_price": c_price,
                                "last_price": c_price,
                                "target_buyback_price": target_px,
                                "price_drop_pct": 0.0,
                                "pnl_pct": 0.0,
                                "pnl_usdt": 0.0,
                                "bear_score": cand["bear_score"],
                                "trailing_active": False,
                                "lowest_price": c_price,
                                "status": f"Live 1x Short Active ({num_contracts} contracts)"
                            }
                            add_log(
                                f"🩸 [LIVE AUTO-SHORT][{c_pair}] Shorted {num_contracts} contracts "
                                f"(${actual_notional:.2f}) @ {fmt_px(c_price)}"
                            )

        with state_lock:
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
    add_log("🩸 BearSkimmer v1.0 Online — Autonomous 1x Bearish Short Skimmer ($100 Paper Sim Ready).")
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
        add_log(f"🤖 Bear Auto-Pilot switched to: {'ON' if STATE['auto_pilot'] else 'MANUAL'}")
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


@app.route("/api/reset_paper", methods=["POST"])
def api_reset_paper():
    with state_lock:
        STATE["paper_usdt_balance"] = 100.0
        STATE["short_positions"] = {}
        STATE["stats"]["paper_skims"] = 0
        STATE["stats"]["paper_profit_usdt"] = 0.0
        STATE["skim_ledger"] = []
        add_log("🔄 Reset Paper Bear Lab to fresh $100.00 USDT!")
        save_state()
    run_bear_cycle(force_manual=True)
    return jsonify({"ok": True})


@app.route("/api/manual_short", methods=["POST"])
def api_manual_short():
    pair = (request.json or {}).get("pair", "").strip().upper()
    if not pair:
        return jsonify({"ok": False, "error": "Missing pair"}), 400
    with state_lock:
        if pair in STATE["short_positions"]:
            return jsonify({"ok": False, "error": f"Already shorting {pair}"}), 400
    try:
        tk_map = fetch_futures_tickers([pair])
        c_price = tk_map[pair]["last"]
        slot_size = float(STATE.get("slot_size_usdt", 12.0))
        req_drop = float(STATE.get("skim_profit_pct", 1.0)) + float(STATE.get("futures_fee_buffer_pct", 0.12))
        target_px = c_price * (1.0 - (req_drop / 100.0))
        with state_lock:
            avail = float(STATE.get("paper_usdt_balance", 100.0))
            if avail < slot_size:
                return jsonify({"ok": False, "error": f"Insufficient Paper USDT (${avail:.2f})"}), 400
            STATE["paper_usdt_balance"] = round(avail - slot_size, 4)
            STATE["short_positions"][pair] = {
                "pair": pair,
                "margin_usdt": slot_size,
                "current_equity_usdt": slot_size,
                "entry_price": c_price,
                "last_price": c_price,
                "target_buyback_price": target_px,
                "price_drop_pct": 0.0,
                "pnl_pct": 0.0,
                "pnl_usdt": 0.0,
                "bear_score": 80,
                "trailing_active": False,
                "lowest_price": c_price,
                "status": "Manual 1x Short Opened"
            }
            add_log(f"⚡ [MANUAL SHORT][{pair}] Shorted ${slot_size:.2f} @ {fmt_px(c_price)} (Target: {fmt_px(target_px)})")
            save_state()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/close_short_slot", methods=["POST"])
def api_close_short_slot():
    pair = (request.json or {}).get("pair", "")
    with state_lock:
        pos = STATE["short_positions"].pop(pair, None)
        if pos and STATE.get("mode") == "PAPER":
            STATE["paper_usdt_balance"] = round(
                STATE.get("paper_usdt_balance", 0.0) + float(pos.get("current_equity_usdt", 0.0)), 4
            )
            add_log(f"Closed & untracked {pair} short slot.")
            save_state()
    return jsonify({"ok": True})


@app.route("/api/update_config", methods=["POST"])
def api_update_config():
    d = request.json or {}
    with state_lock:
        if d.get("slot_size_usdt"):
            STATE["slot_size_usdt"] = max(2.0, float(d["slot_size_usdt"]))
        if d.get("max_short_slots"):
            STATE["max_short_slots"] = max(1, min(10, int(d["max_short_slots"])))
        if d.get("min_bear_score"):
            STATE["min_bear_score"] = max(40, min(95, int(d["min_bear_score"])))
        if d.get("skim_profit_pct"):
            STATE["skim_profit_pct"] = max(0.2, float(d["skim_profit_pct"]))
        add_log(
            f"⚙️ Updated Bear Config: Slot=${STATE['slot_size_usdt']:.2f} | "
            f"Max Slots={STATE['max_short_slots']} | Min Bear={STATE['min_bear_score']} | "
            f"Skim=+{STATE['skim_profit_pct']}%"
        )
        save_state()
    run_bear_cycle(force_manual=True)
    return jsonify({"ok": True})


@app.route("/api/sync_keys", methods=["POST"])
def api_sync_keys():
    d = request.json or {}
    rk = (d.get("api_key") or "").strip()
    rs = (d.get("api_secret") or "").strip()
    with state_lock:
        if rk:
            STATE["api_key"] = rk
        if rs:
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
            add_log(f"🔗 Gate.io Futures API Verified! Available USDT: ${u:.2f}")
            save_state()
        return jsonify({"ok": True, "usdt_balance": u})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)[:80]}), 400


# =============================================================================
# BEARSKIMMER v1.0 CRIMSON SHORT TERMINAL UI
# =============================================================================
HTML_UI = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>BearSkimmer v1.0 | Autonomous 1x Short Skimmer</title>
  <style>
    :root {
      --bg: #07040a;
      --card: rgba(18, 11, 24, 0.94);
      --border: #2e1d3a;
      --crimson: #ff2a5f;
      --neon: #00f5a0;
      --amber: #f59e0b;
      --cyan: #00d9f5;
      --text: #f8fafc;
      --muted: #a1a1aa;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', system-ui, sans-serif; }
    body { background: radial-gradient(circle at 50% 0%, #2a0a1e 0%, #07040a 75%); color: var(--text); padding: 14px; min-height: 100vh; }
    .wrap { max-width: 1220px; margin: 0 auto; }

    .header {
      background: linear-gradient(135deg, rgba(255, 42, 95, 0.16), rgba(168, 85, 247, 0.1));
      border: 1px solid rgba(255, 42, 95, 0.45); border-radius: 16px; padding: 16px 20px; margin-bottom: 14px;
      box-shadow: 0 12px 32px rgba(0,0,0,0.6);
    }
    .header-top { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 12px; }
    .logo { font-size: 1.55rem; font-weight: 900; display: flex; align-items: center; gap: 10px; }
    .badge { background: linear-gradient(135deg, var(--crimson), #ff6b35); color: #fff; font-size: 0.72rem; font-weight: 900; padding: 3px 10px; border-radius: 99px; }
    .sub { color: var(--muted); font-size: 0.79rem; margin-top: 4px; }

    .btn-row { display: flex; flex-wrap: wrap; gap: 8px; }
    .btn { cursor: pointer; border: none; border-radius: 10px; padding: 9px 14px; font-weight: 800; font-size: 0.78rem; }
    .btn-crimson { background: linear-gradient(135deg, var(--crimson), #e11d48); color: #fff; }
    .btn-neon { background: linear-gradient(135deg, var(--neon), var(--cyan)); color: #022c22; }
    .btn-amber { background: linear-gradient(135deg, #f59e0b, #d97706); color: #1e1b4b; }
    .btn-outline { background: rgba(255, 42, 95, 0.15); border: 1px solid var(--crimson); color: #fda4af; }

    .strip {
      display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center;
      margin-top: 12px; padding-top: 12px; border-top: 1px solid rgba(255,255,255,0.08);
      font-size: 0.78rem; color: var(--muted); gap: 10px;
    }

    .grid-4 { display: grid; grid-template-columns: repeat(auto-fit, minmax(225px, 1fr)); gap: 12px; margin-bottom: 14px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 15px 16px; }
    .lbl { font-size: 0.7rem; font-weight: 800; text-transform: uppercase; color: var(--muted); display: flex; justify-content: space-between; }
    .val { font-size: 1.45rem; font-weight: 900; margin: 6px 0 4px; font-variant-numeric: tabular-nums; }
    .c-crimson { color: var(--crimson); }
    .c-neon { color: var(--neon); }
    .c-cyan { color: var(--cyan); }
    .c-amber { color: var(--amber); }

    .grid-main { display: grid; grid-template-columns: 1.25fr 1fr; gap: 14px; margin-bottom: 14px; }
    @media (max-width: 920px) { .grid-main { grid-template-columns: 1fr; } }

    .sec-title { font-size: 0.84rem; font-weight: 900; text-transform: uppercase; color: #fda4af; margin-bottom: 11px; display: flex; justify-content: space-between; align-items: center; }
    table { width: 100%; border-collapse: collapse; font-size: 0.78rem; }
    th, td { padding: 8px 6px; text-align: left; border-bottom: 1px solid #24142e; font-variant-numeric: tabular-nums; }
    th { color: var(--muted); font-size: 0.68rem; text-transform: uppercase; }

    .score-pill { padding: 3px 8px; border-radius: 6px; font-weight: 900; font-size: 0.73rem; }
    .sc-bear { background: rgba(255, 42, 95, 0.22); color: #fda4af; border: 1px solid var(--crimson); }
    .sc-mid { background: rgba(245, 158, 11, 0.16); color: var(--amber); }

    .pos-card {
      background: linear-gradient(180deg, rgba(32, 14, 38, 0.95), rgba(15, 8, 20, 0.95));
      border: 1px solid #4a1d3d; border-radius: 12px; padding: 12px 14px; margin-bottom: 10px;
    }
    .input-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; margin-bottom: 10px; }
    label { display: block; font-size: 0.71rem; color: var(--muted); margin-bottom: 4px; font-weight: 700; }
    input { width: 100%; background: #07040a; border: 1px solid #3b1d47; color: var(--text); padding: 9px 10px; border-radius: 9px; font-size: 0.84rem; font-weight: 700; }
    .logs { background: #050208; border: 1px solid #2e1d3a; border-radius: 11px; padding: 11px; height: 210px; overflow-y: auto; font-family: monospace; font-size: 0.75rem; color: #e4e4e7; line-height: 1.55; }
  </style>
</head>
<body>
<div class="wrap">
  <div class="header">
    <div class="header-top">
      <div>
        <div class="logo">🩸 BEARSKIMMER <span class="badge">v1.0 INVERSE SHORT ENGINE</span></div>
        <div class="sub">Profits When Coins Drop • 1x Perpetual Futures Short Skimmer • Inverted Trailing Waterfall Lock</div>
      </div>
      <div class="btn-row">
        <button class="btn btn-crimson" onclick="toggleAutoPilot()" id="apBtn">🤖 Bear Auto-Pilot: ON</button>
        <button class="btn btn-amber" onclick="toggleMode()" id="modeBtn">🔀 Mode: PAPER ($100 SIM)</button>
        <button class="btn btn-outline" onclick="resetPaper()">🔄 Reset $100 Paper Lab</button>
        <button class="btn btn-outline" onclick="manualScan()">⚡ Sweep Red Market</button>
      </div>
    </div>
    <div class="strip">
      <div id="apiStatus" style="font-weight:800; color:var(--neon);">● Paper Mode Active ($100 Virtual Short Bankroll)</div>
      <div>Rule: <strong>Short High → Wait for -1.12% Price Drop → Cover Low & Bank +1.00% Cash</strong></div>
      <div>Last Sweep: <strong id="lastUpdate">--:--:--</strong> (<strong class="c-crimson" id="scanMs">0</strong>ms | Cycle #<span id="cycleNum">0</span>)</div>
    </div>
  </div>

  <div class="grid-4">
    <div class="card">
      <div class="lbl">Active Short Margin Deployed</div>
      <div class="val c-crimson" id="deployedVal">$0.00</div>
      <div class="sub">Open 1x Shorts: <strong id="posCount">0</strong> / <span id="maxPos">4</span></div>
    </div>
    <div class="card">
      <div class="lbl">Total Portfolio (Shorts + Free USDT)</div>
      <div class="val c-neon" id="totalPort">$100.0000</div>
      <div class="sub">Free USDT Reserve: <strong id="freeUsdt">$100.00</strong></div>
    </div>
    <div class="card">
      <div class="lbl">Bear Skim Profit Vault</div>
      <div class="val c-neon" id="profitVault">+$0.0000</div>
      <div class="sub">Red-Candle Skims Won: <strong id="skimCount">0</strong></div>
    </div>
    <div class="card">
      <div class="lbl">Gate.io Futures Fee Advantage</div>
      <div class="val c-cyan">-1.12% Drop</div>
      <div class="sub">Needed for <strong>+1.00% Net Profit</strong> (0.12% Fee)</div>
    </div>
  </div>

  <div class="grid-main">
    <div class="card">
      <div class="sec-title">
        <span>📡 16-Market Bearish Breakdown Radar (Live Futures)</span>
        <span style="font-size:0.7rem;color:var(--muted);">Score >= 68 Triggers Auto-Short</span>
      </div>
      <div style="overflow-x:auto;">
        <table>
          <thead>
            <tr>
              <th>Contract</th>
              <th>Live Price (24h)</th>
              <th>Bear Score</th>
              <th>5m RSI</th>
              <th>Book Sellers %</th>
              <th>Bear Signal</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody id="radarBody"></tbody>
        </table>
      </div>
    </div>

    <div>
      <div class="card" style="margin-bottom:14px;">
        <div class="sec-title">🩸 Active 1x Short Positions (Win When Price Falls)</div>
        <div id="positionsBox"></div>
      </div>

      <div class="card">
        <div class="sec-title">⚙️ BearSkimmer Parameters</div>
        <div class="input-row">
          <div>
            <label>Short Size per Slot ($)</label>
            <input type="number" id="cfgSize" step="0.5" value="12.0" />
          </div>
          <div>
            <label>Max Concurrent Shorts</label>
            <input type="number" id="cfgMaxPos" step="1" value="4" />
          </div>
          <div>
            <label>Min Bear Score (0-95)</label>
            <input type="number" id="cfgBear" step="1" value="68" />
          </div>
          <div>
            <label>Net Skim Profit (%)</label>
            <input type="number" id="cfgTarget" step="0.1" value="1.00" />
          </div>
        </div>
        <button class="btn btn-crimson" style="width:100%;" onclick="saveConfig()">💾 Save BearSkimmer Settings</button>
      </div>
    </div>
  </div>

  <div class="grid-main">
    <div class="card">
      <div class="sec-title">📡 Live Short Execution Telemetry</div>
      <div class="logs" id="logBox"></div>
    </div>
    <div class="card">
      <div class="sec-title">💎 Completed Bear Skims (Cash Banked From Dumps)</div>
      <div style="max-height:210px; overflow-y:auto;">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Contract</th>
              <th>Dump %</th>
              <th>Short Entry → Covered Low</th>
              <th>Profit</th>
            </tr>
          </thead>
          <tbody id="ledgerBody"></tbody>
        </table>
      </div>
    </div>
  </div>
</div>

<script>
  function fmtPx(val) {
    const v = Number(val || 0);
    if (v === 0) return '$0.0000';
    if (v < 0.0001) return '$' + v.toFixed(8);
    if (v < 0.01) return '$' + v.toFixed(6);
    return '$' + v.toFixed(4);
  }

  async function toggleAutoPilot() {
    await fetch('/api/toggle_autopilot', { method: 'POST' });
    await refresh();
  }

  async function toggleMode() {
    await fetch('/api/toggle_mode', { method: 'POST' });
    await refresh();
  }

  async function resetPaper() {
    await fetch('/api/reset_paper', { method: 'POST' });
    await refresh();
  }

  async function manualScan() {
    await fetch('/api/scan_now', { method: 'POST' });
    await refresh();
  }

  async function shortNow(pair) {
    const r = await fetch('/api/manual_short', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ pair })
    });
    const d = await r.json();
    if (!d.ok) alert(d.error || 'Could not open short');
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

  async function saveConfig() {
    await fetch('/api/update_config', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        slot_size_usdt: document.getElementById('cfgSize').value,
        max_short_slots: document.getElementById('cfgMaxPos').value,
        min_bear_score: document.getElementById('cfgBear').value,
        skim_profit_pct: document.getElementById('cfgTarget').value
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
        document.getElementById('cfgSize').value = s.slot_size_usdt || 12;
        document.getElementById('cfgMaxPos').value = s.max_short_slots || 4;
        document.getElementById('cfgBear').value = s.min_bear_score || 68;
        document.getElementById('cfgTarget').value = s.skim_profit_pct || 1.0;
        cfgLoaded = true;
      }

      document.getElementById('lastUpdate').innerText = s.last_update;
      document.getElementById('scanMs').innerText = s.last_scan_ms;
      document.getElementById('cycleNum').innerText = s.cycle_count;

      const apBtn = document.getElementById('apBtn');
      apBtn.innerText = s.auto_pilot ? '🤖 Bear Auto-Pilot: ON' : '✋ Bear Auto-Pilot: MANUAL';

      const modeBtn = document.getElementById('modeBtn');
      modeBtn.innerText = s.mode === 'LIVE' ? '🔀 Mode: LIVE FUTURES' : '🔀 Mode: PAPER ($100 SIM)';
      modeBtn.className = s.mode === 'LIVE' ? 'btn btn-neon' : 'btn btn-amber';

      const posMap = s.short_positions || {};
      const posKeys = Object.keys(posMap);
      let sumEq = 0;
      posKeys.forEach(k => { sumEq += Number(posMap[k].current_equity_usdt || 0); });

      const freeCash = s.mode === 'LIVE' ? Number(s.live_futures_usdt || 0) : Number(s.paper_usdt_balance || 0);
      document.getElementById('deployedVal').innerText = '$' + sumEq.toFixed(2);
      document.getElementById('posCount').innerText = posKeys.length;
      document.getElementById('maxPos').innerText = s.max_short_slots || 4;
      document.getElementById('totalPort').innerText = '$' + (sumEq + freeCash).toFixed(4);
      document.getElementById('freeUsdt').innerText = '$' + freeCash.toFixed(2);

      const prof = s.mode === 'LIVE' ? s.stats.live_profit_usdt : s.stats.paper_profit_usdt;
      const skims = s.mode === 'LIVE' ? s.stats.live_skims : s.stats.paper_skims;
      document.getElementById('profitVault').innerText = '+$' + Number(prof || 0).toFixed(4);
      document.getElementById('skimCount').innerText = skims || 0;

      const rb = document.getElementById('radarBody');
      rb.innerHTML = (s.radar || []).map(item => {
        const sc = item.bear_score;
        const cls = sc >= 68 ? 'sc-bear' : 'sc-mid';
        const chgSign = item.change_24h >= 0 ? '+' : '';
        return `
          <tr>
            <td><strong>${item.pair}</strong></td>
            <td>${fmtPx(item.price)} <span style="color:${item.change_24h < 0 ? '#ff2a5f' : '#00f5a0'}">(${chgSign}${item.change_24h}%)</span></td>
            <td><span class="score-pill ${cls}">${sc} / 100</span></td>
            <td>${item.rsi_14}</td>
            <td>${item.ask_sell_pct}%</td>
            <td style="font-weight:800; color:${sc >= 68 ? '#ff2a5f' : '#a1a1aa'}">${item.signal}</td>
            <td><button class="btn btn-outline" style="padding:4px 9px;font-size:0.7rem;" onclick="shortNow('${item.pair}')">🩸 Short</button></td>
          </tr>
        `;
      }).join('');

      const pBox = document.getElementById('positionsBox');
      if (posKeys.length === 0) {
        pBox.innerHTML = `<div style="color:#71717a;text-align:center;padding:18px;font-size:0.8rem;">Scanning for Bear Score >= ${s.min_bear_score} to open 1x Shorts...</div>`;
      } else {
        pBox.innerHTML = posKeys.map(k => {
          const p = posMap[k];
          const pnlSign = p.pnl_pct >= 0 ? '+' : '';
          return `
            <div class="pos-card">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
                <div><strong>${p.pair}</strong> <span class="score-pill sc-bear">1x SHORT ($${Number(p.margin_usdt).toFixed(2)})</span></div>
                <div style="font-weight:900;color:${p.pnl_pct >= 0 ? '#00f5a0' : '#f59e0b'}">
                  Now: ${fmtPx(p.last_price)} (${pnlSign}${p.pnl_pct}% Profit)
                </div>
              </div>
              <div style="font-size:0.74rem;color:#a1a1aa;display:flex;justify-content:space-between;margin-bottom:6px;">
                <span>Shorted High @ <strong>${fmtPx(p.entry_price)}</strong></span>
                <span>Buyback Target @ <strong class="c-neon">${fmtPx(p.target_buyback_price)}</strong></span>
                <button onclick="closeSlot('${p.pair}')" style="background:none;border:none;color:#ff2a5f;cursor:pointer;font-weight:800;font-size:0.7rem;">✖ Close</button>
              </div>
              <div style="font-size:0.72rem;color:#e4e4e7;">${p.status}</div>
            </div>
          `;
        }).join('');
      }

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
        lb.innerHTML = `<tr><td colspan="5" style="text-align:center;color:#71717a;padding:16px;">Waiting for first -1.12% red-candle dump to skim profit...</td></tr>`;
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
