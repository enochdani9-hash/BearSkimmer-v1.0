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

STATE_FILE = "bearskimmer_v2_0_state.json"
GATE_HOST = "https://api.gateio.ws"
GATE_PREFIX = "/api/v4"

BEAR_UNIVERSE = [
    "SUI_USDT", "SOL_USDT", "DOGE_USDT", "XRP_USDT", "ADA_USDT",
    "NEAR_USDT", "AVAX_USDT", "LINK_USDT", "FET_USDT", "DOT_USDT",
    "ARB_USDT", "OP_USDT", "TRX_USDT", "PEPE_USDT", "WIF_USDT", "RENDER_USDT"
]

HTTP_SESSION = requests.Session()
adapter = HTTPAdapter(
    pool_connections=20,
    pool_maxsize=20,
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
    "max_short_slots": 5,        # Keeps 50% cash free so Auto-Step-Up can raise the bar!
    "slot_size_usdt": 10.0,
    "step_up_trigger_pct": 2.0,  # When coin pumps +2.0%, bot adds $5 top-up short to raise the bar
    "step_up_size_usdt": 5.0,
    "max_step_ups": 2,           # Up to 2 automatic bar-raises per coin
    "min_bear_score": 68,
    "skim_profit_pct": 1.00,
    "trail_bounce_pct": 0.22,
    "futures_fee_buffer_pct": 0.12,
    "api_key": os.environ.get("GATE_API_KEY", "").strip(),
    "api_secret": os.environ.get("GATE_API_SECRET", "").strip(),
    "api_connected": False,
    "api_status_msg": "Paper Mode ($100 Sim)",
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
    STATE["logs"] = STATE["logs"][:80]


def load_state():
    global STATE
    for fn in [STATE_FILE, "bearskimmer_v1_2_state.json", "bearskimmer_v1_state.json"]:
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
                    STATE.setdefault("step_up_size_usdt", 5.0)
                    STATE.setdefault("max_step_ups", 2)
                    # Cap max slots to 5 if previously 10 so reserve exists for step-up
                    if int(STATE.get("max_short_slots", 5)) > 6:
                        STATE["max_short_slots"] = 5
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
                    "change_24h": float(t.get("change_percentage") or 0.0)
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
        raise ValueError(f"Gate.io HTTP {r.status_code}")
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
    elif 48 <= rsi_14 <= 75:
        s_rsi = 38.0
    else:
        s_rsi = 25.0

    s_ob = max(4.0, min(35.0, ((ask_sell_pct - 35.0) / 35.0) * 35.0))
    s_rej = (15.0 if upper_wick else 6.0) + (12.0 if tk["change_24h"] < 0 else 4.0)
    bear_score = int(round(max(1.0, min(99.0, s_rsi + s_ob + s_rej))))

    res = {
        "pair": pair,
        "price": price,
        "change_24h": round(tk["change_24h"], 2),
        "bear_score": bear_score,
        "rsi_14": round(rsi_14, 1),
        "ask_sell_pct": round(ask_sell_pct, 1),
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
# MAIN CYCLE (WITH AUTO-STEP-UP BAR MOVER WHEN COINS PUMP +2%)
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
            slot_size = float(STATE.get("slot_size_usdt", 10.0))
            step_trigger_pct = float(STATE.get("step_up_trigger_pct", 2.0))
            step_add_usdt = float(STATE.get("step_up_size_usdt", 5.0))
            max_steps = int(STATE.get("max_step_ups", 2))
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
                    STATE["api_status_msg"] = f"Gate.io Connected (${fut_usdt:.2f} USDT)"
            except Exception as e:
                with state_lock:
                    STATE["api_connected"] = False
                    STATE["api_status_msg"] = f"API Notice: {str(e)[:45]}"

        all_pairs = list(set(BEAR_UNIVERSE + active_pairs))
        try:
            tk_map = fetch_futures_tickers(all_pairs)
        except Exception:
            return

        radar_list = []
        for pair in BEAR_UNIVERSE:
            tk = tk_map.get(pair)
            if tk:
                radar_list.append(compute_bear_score(pair, tk))
        radar_list.sort(key=lambda x: x["bear_score"], reverse=True)

        required_drop_pct = skim_pct + fee_buf

        for pair in list(STATE["short_positions"].keys()):
            pos = STATE["short_positions"].get(pair)
            if not pos or pair not in tk_map:
                continue

            live_px = tk_map[pair]["last"]
            entry_px = float(pos["entry_price"])
            margin_usdt = float(pos["margin_usdt"])
            steps_done = int(pos.get("step_ups_done", 0))
            last_step_px = float(pos.get("last_step_price") or entry_px)

            # -----------------------------------------------------------------
            # AUTO-STEP-UP BAR MOVER:
            # If the coin pumps +2.0% above our last entry, add a $5 top-up
            # short at the higher price to pull both our Entry Bar & Target UP!
            # -----------------------------------------------------------------
            pump_from_last_pct = ((live_px - last_step_px) / last_step_px) * 100.0
            if steps_done < max_steps and pump_from_last_pct >= step_trigger_pct:
                if mode == "PAPER" and STATE.get("paper_usdt_balance", 0.0) >= step_add_usdt:
                    with state_lock:
                        old_entry = entry_px
                        old_units = margin_usdt / entry_px
                        add_units = step_add_usdt / live_px
                        new_margin = round(margin_usdt + step_add_usdt, 4)
                        new_avg_entry = new_margin / (old_units + add_units)

                        STATE["paper_usdt_balance"] = round(STATE["paper_usdt_balance"] - step_add_usdt, 4)
                        pos["margin_usdt"] = new_margin
                        pos["entry_price"] = new_avg_entry
                        pos["last_step_price"] = live_px
                        pos["step_ups_done"] = steps_done + 1

                        entry_px = new_avg_entry
                        margin_usdt = new_margin
                        new_target = entry_px * (1.0 - (required_drop_pct / 100.0))
                        add_log(
                            f"🔼 [BAR STEPPED UP #{steps_done+1}][{pair}] Coin pumped +{pump_from_last_pct:.1f}% to {fmt_px(live_px)}! "
                            f"Added ${step_add_usdt:.0f} short -> Entry Bar raised {fmt_px(old_entry)} → {fmt_px(entry_px)} "
                            f"(New Target: {fmt_px(new_target)})"
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
                    pos["status"] = f"🔥 Target Hit! Trailing Profit Lock Armed (+{net_pnl_pct:.2f}%)"
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
                            f"Need -{need_drop:.2f}% drop to win | "
                            f"Auto-Raises Bar if price hits {fmt_px(next_step_px)}"
                        )
                    else:
                        pos["status"] = f"Bar Raised {pos.get('step_ups_done')}x — Waiting for -{need_drop:.2f}% drop"

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
                            "pair": pair,
                            "drop": f"-{raw_drop_gain_pct:.2f}%",
                            "short_to_cover": f"{fmt_px(entry_px)} → {fmt_px(live_px)}",
                            "profit": f"+${profit_usdt:.4f}"
                        })
                        add_log(
                            f"💎 [BEAR SKIM WON][{pair}] Covered @ {fmt_px(live_px)} | Banked +${profit_usdt:.4f}!"
                        )
                        STATE["short_positions"].pop(pair, None)

        # 3) OPEN NEW AUTONOMOUS SHORTS (MAX 5 SO 50% CASH STAYS IN STEP-UP RESERVE)
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
                        add_log(f"🩸 [NEW SHORT][{c_pair}] ${slot_size:.0f} @ {fmt_px(c_price)} (Target: {fmt_px(target_px)})")

        with state_lock:
            STATE["radar"] = radar_list[:8]
            STATE["skim_ledger"] = STATE["skim_ledger"][:40]
            STATE["cycle_count"] = STATE.get("cycle_count", 0) + 1
            STATE["last_scan_ms"] = int((time.time() - t0) * 1000)
            STATE["last_update"] = datetime.utcnow().strftime("%H:%M:%S")
            STATE["last_worker_heartbeat"] = time.time()
            save_state()

    finally:
        cycle_lock.release()


def background_worker():
    load_state()
    add_log("🩸 BearSkimmer v2.0 Online — Auto-Step-Up Bar Mover (+2% Pump Rescuer) Active.")
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


@app.route("/manifest.json")
def pwa_manifest():
    return jsonify({
        "name": "BearSkimmer v2.0",
        "short_name": "BearSkimmer",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#07040a",
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


@app.route("/api/state")
def api_state():
    if STATE["cycle_count"] == 0:
        run_bear_cycle(force_manual=False)
    with state_lock:
        safe = STATE.copy()
        safe["api_key"] = ""
        safe["api_secret"] = ""
        return jsonify(safe)


@app.route("/api/step_up_now", methods=["POST"])
def api_step_up_now():
    """Manual 1-tap button to raise the Entry & Target bar on any coin that pumped."""
    pair = (request.json or {}).get("pair", "")
    with state_lock:
        pos = STATE["short_positions"].get(pair)
        if not pos:
            return jsonify({"ok": False, "error": "Position not found"}), 400
        step_add = float(STATE.get("step_up_size_usdt", 5.0))
        if STATE.get("paper_usdt_balance", 0.0) < step_add:
            return jsonify({"ok": False, "error": f"Need ${step_add:.2f} in Free USDT Reserve to Step-Up Bar"}), 400

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
            f"🔼 [MANUAL BAR RAISE][{pair}] Added ${step_add:.0f} @ {fmt_px(live_px)} -> "
            f"Bar moved {fmt_px(old_entry)} → {fmt_px(new_entry)} (New Target: {fmt_px(new_target)})"
        )
        save_state()
    run_bear_cycle(force_manual=True)
    return jsonify({"ok": True})


@app.route("/api/reset_paper", methods=["POST"])
def api_reset_paper():
    with state_lock:
        # Keep existing profit vault if user just wants to free up the 10 stuck slots, or reset cleanly
        STATE["paper_usdt_balance"] = 100.0 + float(STATE["stats"].get("paper_profit_usdt", 0.0))
        STATE["short_positions"] = {}
        STATE["max_short_slots"] = 5
        add_log("🔄 Rebalanced slots to 5 Max (keeping your $2+ profit intact & 50% cash in Step-Up Reserve!).")
        save_state()
    run_bear_cycle(force_manual=True)
    return jsonify({"ok": True})


@app.route("/api/close_short_slot", methods=["POST"])
def api_close_short_slot():
    pair = (request.json or {}).get("pair", "")
    with state_lock:
        pos = STATE["short_positions"].pop(pair, None)
        if pos and STATE.get("mode") == "PAPER":
            STATE["paper_usdt_balance"] = round(
                STATE.get("paper_usdt_balance", 0.0) + float(pos.get("current_equity_usdt", 0.0)), 4
            )
            add_log(f"Closed {pair} short slot.")
            save_state()
    return jsonify({"ok": True})


@app.route("/api/toggle_autopilot", methods=["POST"])
def api_toggle_autopilot():
    with state_lock:
        STATE["auto_pilot"] = not bool(STATE.get("auto_pilot", True))
        save_state()
    return jsonify({"ok": True})


# =============================================================================
# CLEAN, SIMPLIFIED MOBILE-FIRST UI (BEARSKIMMER v2.0)
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
  <title>BearSkimmer v2.0 | Simple Auto-Step-Up Short Bot</title>
  <style>
    :root {
      --bg: #08050c;
      --card: #130c1c;
      --border: #2c1a3d;
      --crimson: #ff2a5f;
      --neon: #00f5a0;
      --amber: #f59e0b;
      --cyan: #00d9f5;
      --text: #f8fafc;
      --muted: #9ca3af;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', system-ui, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 12px; max-width: 720px; margin: 0 auto; }

    .top-bar {
      background: linear-gradient(135deg, rgba(255,42,95,0.18), rgba(19,12,28,0.95));
      border: 1px solid rgba(255,42,95,0.45); border-radius: 16px; padding: 14px 16px; margin-bottom: 12px;
    }
    .title-row { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px; }
    .logo { font-size: 1.25rem; font-weight: 900; display: flex; align-items: center; gap: 8px; }
    .badge { background: var(--crimson); color: #fff; font-size: 0.68rem; padding: 3px 8px; border-radius: 99px; }
    .sub { font-size: 0.75rem; color: var(--muted); margin-top: 4px; }

    .btn-row { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }
    .btn { cursor: pointer; border: none; border-radius: 10px; padding: 9px 12px; font-weight: 800; font-size: 0.76rem; flex: 1; text-align: center; }
    .btn-neon { background: linear-gradient(135deg, var(--neon), var(--cyan)); color: #022c22; }
    .btn-crimson { background: var(--crimson); color: #fff; }
    .btn-outline { background: rgba(255,42,95,0.14); border: 1px solid var(--crimson); color: #fda4af; }

    .score-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 12px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 13px 14px; }
    .lbl { font-size: 0.68rem; font-weight: 800; text-transform: uppercase; color: var(--muted); }
    .val { font-size: 1.35rem; font-weight: 900; margin: 4px 0; font-variant-numeric: tabular-nums; }
    .c-neon { color: var(--neon); }
    .c-crimson { color: var(--crimson); }
    .c-cyan { color: var(--cyan); }

    .sec-title { font-size: 0.82rem; font-weight: 900; text-transform: uppercase; color: #fda4af; margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center; }
    .pos-card {
      background: #191024; border: 1px solid #3d204f; border-radius: 12px; padding: 12px 14px; margin-bottom: 10px;
    }
    .step-badge {
      background: rgba(0, 217, 245, 0.15); border: 1px solid var(--cyan); color: var(--cyan);
      font-size: 0.68rem; font-weight: 800; padding: 2px 7px; border-radius: 6px;
    }
    .bar-box {
      background: #0d0814; border: 1px solid #2c1a3d; border-radius: 9px; padding: 8px 10px; margin: 8px 0;
      display: flex; justify-content: space-between; align-items: center; font-size: 0.76rem;
    }
    .btn-step {
      background: rgba(0, 245, 160, 0.16); border: 1px solid var(--neon); color: var(--neon);
      padding: 5px 10px; border-radius: 8px; font-size: 0.71rem; font-weight: 800; cursor: pointer;
    }
    table { width: 100%; border-collapse: collapse; font-size: 0.76rem; }
    th, td { padding: 8px 6px; text-align: left; border-bottom: 1px solid #24142e; }
    th { color: var(--muted); font-size: 0.66rem; text-transform: uppercase; }
    .logs { background: #050208; border: 1px solid var(--border); border-radius: 10px; padding: 10px; height: 150px; overflow-y: auto; font-family: monospace; font-size: 0.72rem; color: #d4d4d8; line-height: 1.5; }
  </style>
</head>
<body>

  <div class="top-bar">
    <div class="title-row">
      <div class="logo">🩸 BEARSKIMMER <span class="badge">v2.0 AUTO-STEP-UP</span></div>
      <div style="font-size:0.72rem;color:var(--neon);font-weight:800;" id="statusTxt">● Scanning Live</div>
    </div>
    <div class="sub">If a coin pumps +2%, the bot automatically adds $5 at the higher price to raise your Entry & Target Bar!</div>
    <div class="btn-row">
      <button class="btn btn-neon" onclick="installApp()" id="installBtn">📲 Install App</button>
      <button class="btn btn-crimson" onclick="toggleAP()" id="apBtn">🤖 Auto-Pilot: ON</button>
      <button class="btn btn-outline" onclick="rebalanceSlots()">🔄 Free Stuck Slots (Keep Profit)</button>
    </div>
  </div>

  <div class="score-grid">
    <div class="card">
      <div class="lbl">Bear Profit Vault</div>
      <div class="val c-neon" id="profitVault">+$0.0000</div>
      <div class="sub">Completed Wins: <strong id="winCount">0</strong></div>
    </div>
    <div class="card">
      <div class="lbl">Total Portfolio Value</div>
      <div class="val c-cyan" id="totalPort">$100.00</div>
      <div class="sub">Step-Up Cash Reserve: <strong id="freeCash">$50.00</strong></div>
    </div>
  </div>

  <div class="card" style="margin-bottom:12px;">
    <div class="sec-title">
      <span>🩸 Active Shorts (<span id="posCount">0</span>/<span id="maxPos">5</span>)</span>
      <span style="font-size:0.7rem;color:var(--cyan);">Auto-Raises Bar on +2% Pump</span>
    </div>
    <div id="positionsBox"></div>
  </div>

  <div class="card" style="margin-bottom:12px;">
    <div class="sec-title">💎 Recent Profit Skims Banked</div>
    <div style="max-height:190px; overflow-y:auto;">
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Coin</th>
            <th>Drop</th>
            <th>Entry Bar → Sold</th>
            <th>Profit</th>
          </tr>
        </thead>
        <tbody id="ledgerBody"></tbody>
      </table>
    </div>
  </div>

  <div class="card">
    <div class="sec-title">📡 Live Bot Activity Log</div>
    <div class="logs" id="logBox"></div>
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
      alert("Tap your browser's top-right menu (⋮) and tap 'Add to Home screen' or 'Install app'.");
    }
  }

  function fmtPx(val) {
    const v = Number(val || 0);
    if (v === 0) return '$0.0000';
    if (v < 0.0001) return '$' + v.toFixed(8);
    if (v < 0.01) return '$' + v.toFixed(6);
    return '$' + v.toFixed(4);
  }

  async function toggleAP() {
    await fetch('/api/toggle_autopilot', { method: 'POST' });
    await refresh();
  }

  async function rebalanceSlots() {
    await fetch('/api/reset_paper', { method: 'POST' });
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

  async function refresh() {
    try {
      const r = await fetch('/api/state');
      const s = await r.json();

      document.getElementById('statusTxt').innerText = '● Live Sweep ' + s.last_update;
      document.getElementById('apBtn').innerText = s.auto_pilot ? '🤖 Auto-Pilot: ON' : '✋ Auto-Pilot: PAUSED';

      const posMap = s.short_positions || {};
      const keys = Object.keys(posMap);
      let sumEq = 0;
      keys.forEach(k => { sumEq += Number(posMap[k].current_equity_usdt || 0); });

      const free = Number(s.paper_usdt_balance || 0);
      document.getElementById('profitVault').innerText = '+$' + Number(s.stats.paper_profit_usdt || 0).toFixed(4);
      document.getElementById('winCount').innerText = s.stats.paper_skims || 0;
      document.getElementById('totalPort').innerText = '$' + (sumEq + free).toFixed(2);
      document.getElementById('freeCash').innerText = '$' + free.toFixed(2);
      document.getElementById('posCount').innerText = keys.length;
      document.getElementById('maxPos').innerText = s.max_short_slots || 5;

      const pBox = document.getElementById('positionsBox');
      if (keys.length === 0) {
        pBox.innerHTML = `<div style="text-align:center;color:#9ca3af;padding:16px;font-size:0.8rem;">Scanning for high-score short setups...</div>`;
      } else {
        pBox.innerHTML = keys.map(k => {
          const p = posMap[k];
          const pnl = Number(p.pnl_pct || 0);
          const pnlSign = pnl >= 0 ? '+' : '';
          const steps = Number(p.step_ups_done || 0);

          return `
            <div class="pos-card">
              <div style="display:flex;justify-content:space-between;align-items:center;">
                <div>
                  <strong>${p.pair}</strong>
                  <span class="step-badge">$${Number(p.margin_usdt).toFixed(0)} Short (Bar Raised ${steps}x)</span>
                </div>
                <div style="font-weight:900;color:${pnl >= 0 ? '#00f5a0' : '#f59e0b'}">
                  ${pnlSign}${pnl.toFixed(2)}% ($${Number(p.pnl_usdt || 0).toFixed(2)})
                </div>
              </div>

              <div class="bar-box">
                <div>
                  <div>Entry Bar: <strong>${fmtPx(p.entry_price)}</strong> → Target: <strong class="c-neon">${fmtPx(p.target_buyback_price)}</strong></div>
                  <div style="color:#9ca3af;font-size:0.7rem;margin-top:2px;">Live Price: ${fmtPx(p.last_price)} (${p.status})</div>
                </div>
                <div style="display:flex;gap:6px;align-items:center;">
                  <button class="btn-step" onclick="stepUpNow('${p.pair}')">🔼 Raise Bar (+$5)</button>
                  <button onclick="closeSlot('${p.pair}')" style="background:none;border:none;color:#ff2a5f;font-weight:900;cursor:pointer;">✖</button>
                </div>
              </div>
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
            <td class="c-crimson">${t.drop}</td>
            <td>${t.short_to_cover}</td>
            <td class="c-neon"><strong>${t.profit}</strong></td>
          </tr>
        `).join('');
      } else {
        lb.innerHTML = `<tr><td colspan="5" style="text-align:center;color:#9ca3af;padding:14px;">No closed skims yet.</td></tr>`;
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
