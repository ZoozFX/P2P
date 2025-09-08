import os
import time
import logging
import requests
import threading
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor

# ---------------------- config (from env) ----------------------
BINANCE_P2P_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
ROWS_PER_REQUEST = 20
TIMEOUT = 10
MAX_SCAN_PAGES = 60
SLEEP_BETWEEN_PAGES = 0.09
SLEEP_BETWEEN_PAIRS = 0.05

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Backward-compatible but main logic uses per-pair active states
ALERT_TTL_SECONDS = int(os.getenv("ALERT_TTL_SECONDS", "0"))
ALERT_DEDUP_MODE = os.getenv("ALERT_DEDUP_MODE", "exact").lower()

REFRESH_EVERY = int(os.getenv("REFRESH_EVERY", "60"))
Profit_Threshold_percent = float(os.getenv("PROFIT_THRESHOLD_PERCENT", "0.4"))

# New env vars to control update behavior while active:
# If ALERT_UPDATE_ON_ANY_CHANGE="1" -> send update whenever any numeric value changed (no threshold)
# Otherwise use thresholds:
# ALERT_UPDATE_MIN_DELTA_PERCENT: minimal absolute difference in spread (percentage points) to trigger an update while active (default 0.01)
# ALERT_UPDATE_PRICE_CHANGE_PERCENT: minimal relative price change (%) for buy or sell price to trigger update while active (default 0.05)
ALERT_UPDATE_ON_ANY_CHANGE = os.getenv("ALERT_UPDATE_ON_ANY_CHANGE", "0").strip()
ALERT_UPDATE_MIN_DELTA_PERCENT = float(os.getenv("ALERT_UPDATE_MIN_DELTA_PERCENT", "0.01"))
ALERT_UPDATE_PRICE_CHANGE_PERCENT = float(os.getenv("ALERT_UPDATE_PRICE_CHANGE_PERCENT", "0.05"))

currency_list = ["EGP", "GBP", "EUR", "USD"]
payment_methods_map = {
    "EGP": ["InstaPay", "Vodafonecash"],
    "GBP": ["SkrillMoneybookers"],
    "EUR": ["SkrillMoneybookers"],
    "USD": ["SkrillMoneybookers"]
}
friendly_pay_names = {
    "SkrillMoneybookers": "Skrill (Moneybookers)",
    "Skrill": "Skrill",
    "InstaPay": "InstaPay",
    "Vodafonecash": "Vodafonecash"
}

PAIRS_ENV = os.getenv("PAIRS", "").strip()
SELECTED_CURRENCY = os.getenv("SELECTED_CURRENCY", "ALL").strip().upper()
SELECTED_METHOD = os.getenv("SELECTED_METHOD", "ALL").strip()
MIN_LIMIT_THRESHOLDS_ENV = os.getenv("MIN_LIMIT_THRESHOLDS", "").strip()

# ---------------------- logging ----------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ---------------------- helpers ----------------------
def parse_thresholds(env_str, defaults):
    out = {k: float("inf") for k in defaults}
    if not env_str:
        return out
    parts = [p.strip() for p in env_str.replace(",", ";").split(";") if p.strip()]
    for p in parts:
        if "=" in p or ":" in p:
            sep = "=" if "=" in p else ":"
            k, v = p.split(sep, 1)
            k = k.strip().upper()
            try:
                out[k] = float(v.strip())
            except Exception:
                logging.warning(f"Cannot parse threshold for {k}: {v}")
    return out

def parse_pairs_env(env_str, thresholds, default_map):
    pairs = []
    if not env_str:
        return pairs
    groups = [g.strip() for g in env_str.split(";") if g.strip()]
    for g in groups:
        if ":" in g:
            cur, methods_s = g.split(":", 1)
            cur = cur.strip().upper()
            method_list = [m.strip() for m in methods_s.split(",") if m.strip()]
            if len(method_list) == 1 and method_list[0].upper() == "ALL":
                method_list = default_map.get(cur, [])
        else:
            cur = g.strip().upper()
            method_list = default_map.get(cur, [])
        for m in method_list:
            pairs.append((cur, m, thresholds.get(cur, float("inf"))))
    return pairs

min_limit_thresholds = parse_thresholds(MIN_LIMIT_THRESHOLDS_ENV, currency_list)

pairs_to_monitor = []
if PAIRS_ENV:
    pairs_to_monitor = parse_pairs_env(PAIRS_ENV, min_limit_thresholds, payment_methods_map)
else:
    if SELECTED_CURRENCY == "ALL":
        for cur in currency_list:
            methods = payment_methods_map.get(cur, [])
            for m in methods:
                pairs_to_monitor.append((cur, m, min_limit_thresholds.get(cur, float("inf"))))
    else:
        cur = SELECTED_CURRENCY
        methods = payment_methods_map.get(cur, [])
        if not methods:
            logging.error(f"No methods for {cur}. Exiting.")
            raise SystemExit(1)
        if SELECTED_METHOD and SELECTED_METHOD.upper() != "ALL":
            if SELECTED_METHOD not in methods:
                logging.error(f"Method {SELECTED_METHOD} not valid for {cur}. Exiting.")
                raise SystemExit(1)
            methods = [SELECTED_METHOD]
        for m in methods:
            pairs_to_monitor.append((cur, m, min_limit_thresholds.get(cur, float("inf"))))

if not pairs_to_monitor:
    logging.error("No currency/payment pairs selected. Exiting.")
    raise SystemExit(1)

# ---------------------- HTTP session ----------------------
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[429,500,502,503,504])
session.mount("https://", HTTPAdapter(max_retries=retries))
HEADERS = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0 (compatible; ArbitrageChecker/1.0)"}

# ---------------------- helpers: fetch & alerts ----------------------
def fetch_page_raw(fiat, pay_type, trade_type, page, rows=ROWS_PER_REQUEST):
    payload = {"asset": "USDT","fiat": fiat,"tradeType": trade_type,"payTypes": [pay_type],"page": page,"rows": rows}
    try:
        r = session.post(BINANCE_P2P_URL, json=payload, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json().get("data") or []
    except Exception as e:
        logging.debug(f"Network error {fiat} {pay_type} {trade_type} p{page}: {e}")
        return []

def find_first_ad(fiat, pay_type, trade_type, page_limit_threshold, rows=ROWS_PER_REQUEST):
    for page in range(1, MAX_SCAN_PAGES + 1):
        items = fetch_page_raw(fiat, pay_type, trade_type, page, rows=rows)
        if not items:
            break
        for entry in items:
            adv = entry.get("adv") or {}
            try:
                price = float(adv.get("price") or 0)
                min_lim = float(adv.get("minSingleTransAmount") or 0)
                max_lim = float(adv.get("dynamicMaxSingleTransAmount") or 0)
            except Exception:
                continue
            if min_lim <= page_limit_threshold:
                return {"trade_type": trade_type,"currency": fiat,"payment_method": pay_type,
                        "price": price,"min_limit": min_lim,"max_limit": max_lim}
        time.sleep(SLEEP_BETWEEN_PAGES)
    return None

def format_currency_flag(cur):
    flags = {"EGP": "🇪🇬","GBP": "🇬🇧","EUR": "🇪🇺","USD": "🇺🇸"}
    return flags.get(cur, "")

def send_telegram_alert(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.info("Telegram not set; skipping. Msg:\n" + message)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID,"text": message,"parse_mode": "HTML","disable_web_page_preview": True}
    try:
        r = session.post(url, json=payload, timeout=TIMEOUT)
        r.raise_for_status()
        logging.info("Telegram alert sent.")
    except Exception as e:
        logging.error(f"Telegram error: {e}")

def build_alert_message(cur, pay_friendly, buy, sell, spread_percent):
    flag = format_currency_flag(cur)
    abs_diff = buy["price"] - sell["price"]
    direction = "💰" if spread_percent > 0 else "🔽"
    sign = "+" if spread_percent > 0 else ""
    return (
        f"🚨 <b>Arbitrage Alert {flag} — {cur} ({pay_friendly})</b>\n\n"
        f"🔴 <b>Sell</b>: <code>{buy['price']:.4f} {cur}</code>\n"
        f"   min: {buy['min_limit']:.2f} — max: {buy['max_limit']:.2f}\n\n"
        f"🟢 <b>Buy</b>: <code>{sell['price']:.4f} {cur}</code>\n"
        f"   min: {sell['min_limit']:.2f} — max: {sell['max_limit']:.2f}\n\n"
        f"{direction} <b>Spread:</b> {sign}{spread_percent:.2f}%  (<code>{abs_diff:.4f} {cur}</code>)\n\n"
        f"💥 <i>Good Luck!</i>"
    )

def build_update_message(cur, pay_friendly, buy, sell, spread_percent, prev_spread=None):
    flag = format_currency_flag(cur)
    abs_diff = buy["price"] - sell["price"]
    direction = "🔁" if spread_percent >= 0 else "🔽"
    sign = "+" if spread_percent > 0 else ""
    prev_text = f" (prev {prev_spread:.2f}%)" if prev_spread is not None else ""
    return (
        f"🔁 <b>Arbitrage Update {flag} — {cur} ({pay_friendly})</b>\n\n"
        f"🔴 <b>Sell</b>: <code>{buy['price']:.4f} {cur}</code>\n"
        f"🟢 <b>Buy</b>: <code>{sell['price']:.4f} {cur}</code>\n\n"
        f"{direction} <b>Spread:</b> {sign}{spread_percent:.2f}%{prev_text}  (<code>{abs_diff:.4f} {cur}</code>)\n\n"
        f"ℹ️ Still active — will notify on further meaningful changes."
    )

def build_end_message(cur, pay_friendly, buy, sell, spread_percent):
    flag = format_currency_flag(cur)
    abs_diff = buy["price"] - sell["price"]
    return (
        f"✅ <b>Arbitrage ENDED {flag} — {cur} ({pay_friendly})</b>\n\n"
        f"Last recorded spread: {spread_percent:.2f}%  (<code>{abs_diff:.4f} {cur}</code>)\n"
        f"Last prices — Sell: <code>{buy['price']:.4f}</code>, Buy: <code>{sell['price']:.4f}</code>\n\n"
        f"ℹ️ Will notify again when spread ≥ {Profit_Threshold_percent:.2f}%."
    )

# ---------------------- per-pair active state + last-sent snapshot ----------------------
active_states = {}  # pair_key -> dict: active(bool), last_spread, last_buy_price, last_sell_price, since, last_sent_spread, last_sent_buy, last_sent_sell, last_sent_time
active_states_lock = threading.Lock()

def get_active_state(pair_key):
    with active_states_lock:
        rec = active_states.get(pair_key)
        if not rec:
            return {
                "active": False, "last_spread": None, "last_buy_price": None, "last_sell_price": None,
                "since": None, "last_sent_spread": None, "last_sent_buy": None, "last_sent_sell": None, "last_sent_time": None
            }
        return rec.copy()

def set_active_state_snapshot(pair_key, *, active=None, last_spread=None, last_buy_price=None, last_sell_price=None, mark_sent=False):
    with active_states_lock:
        rec = active_states.get(pair_key) or {
            "active": False, "last_spread": None, "last_buy_price": None, "last_sell_price": None,
            "since": None, "last_sent_spread": None, "last_sent_buy": None, "last_sent_sell": None, "last_sent_time": None
        }
        if active is not None:
            rec["active"] = bool(active)
            rec["since"] = time.time() if active else None
        if last_spread is not None:
            rec["last_spread"] = float(last_spread)
        if last_buy_price is not None:
            rec["last_buy_price"] = float(last_buy_price)
        if last_sell_price is not None:
            rec["last_sell_price"] = float(last_sell_price)
        if mark_sent:
            rec["last_sent_spread"] = rec.get("last_spread")
            rec["last_sent_buy"] = rec.get("last_buy_price")
            rec["last_sent_sell"] = rec.get("last_sell_price")
            rec["last_sent_time"] = time.time()
        active_states[pair_key] = rec

# ---------------------- update decision logic ----------------------
def relative_change_percent(old, new):
    try:
        if old == 0:
            return abs(new - old) * 100.0  # fallback absolute percentage-like
        return abs((new - old) / old) * 100.0
    except Exception:
        return float("inf")

def should_send_update(pair_state, new_spread, new_buy, new_sell):
    """
    Return True if an update message should be sent while the pair is active.
    Logic:
     - If ALERT_UPDATE_ON_ANY_CHANGE == "1" => send if ANY numeric value changed compared to last_sent_* (or if no last_sent exists)
     - Otherwise, send if:
         * abs(new_spread - last_sent_spread) >= ALERT_UPDATE_MIN_DELTA_PERCENT
         OR
         * relative change in buy or sell price >= ALERT_UPDATE_PRICE_CHANGE_PERCENT
       If no last_sent exists, treat as send (so initial active start will already be handled separately).
    """
    # if no last_sent -> send (but start edge already sends start and marks sent)
    last_sent_spread = pair_state.get("last_sent_spread")
    last_sent_buy = pair_state.get("last_sent_buy")
    last_sent_sell = pair_state.get("last_sent_sell")

    # If no snapshot yet, don't block (caller should mark_sent after sending)
    if last_sent_spread is None and last_sent_buy is None and last_sent_sell is None:
        return True

    # If configured to send on any change:
    if ALERT_UPDATE_ON_ANY_CHANGE == "1":
        if (last_sent_spread != new_spread) or (last_sent_buy != new_buy) or (last_sent_sell != new_sell):
            return True
        return False

    # Compare spread absolute difference (percentage points)
    if last_sent_spread is None:
        spread_diff = abs(new_spread)
    else:
        spread_diff = abs(new_spread - last_sent_spread)
    if spread_diff >= ALERT_UPDATE_MIN_DELTA_PERCENT:
        return True

    # Compare relative price changes
    if last_sent_buy is not None and relative_change_percent(last_sent_buy, new_buy) >= ALERT_UPDATE_PRICE_CHANGE_PERCENT:
        return True
    if last_sent_sell is not None and relative_change_percent(last_sent_sell, new_sell) >= ALERT_UPDATE_PRICE_CHANGE_PERCENT:
        return True

    return False

# ---------------------- core processing ----------------------
paytype_variants_map = {"SkrillMoneybookers": ["SkrillMoneybookers","Skrill","Skrill (Moneybookers)"]}

def process_pair(currency, method, threshold):
    variants = paytype_variants_map.get(method, [method])
    for variant in variants:
        buy = find_first_ad(currency, variant, "BUY", threshold)
        sell = find_first_ad(currency, variant, "SELL", threshold)
        if not buy or not sell:
            logging.info(f"{currency} {variant}: missing side. Skip.")
            continue

        try:
            spread_percent = ((buy["price"] / sell["price"]) - 1) * 100
        except Exception as e:
            logging.warning(f"Spread error {currency} {variant}: {e}")
            continue

        pay_friendly = friendly_pay_names.get(variant, variant)
        logging.info(f"{currency} {variant}: buy {buy['price']:.4f} sell {sell['price']:.4f} spread {spread_percent:.2f}%")

        pair_key = f"{currency}|{variant}"
        state = get_active_state(pair_key)
        was_active = state["active"]

        # RISING EDGE: became active
        if spread_percent >= Profit_Threshold_percent:
            if not was_active:
                # send "start" arbitrage alert (rising edge)
                message = build_alert_message(currency, pay_friendly, buy, sell, spread_percent)
                send_telegram_alert(message)
                logging.info(f"Start alert sent for {pair_key} (spread {spread_percent:.2f}%)")
                # mark active and mark as last_sent snapshot so future updates compare to this snapshot
                set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                          last_buy_price=buy["price"], last_sell_price=sell["price"], mark_sent=True)
            else:
                # already active: decide whether to send an update
                if should_send_update(state, spread_percent, buy["price"], sell["price"]):
                    # send update
                    prev_sent_spread = state.get("last_sent_spread")
                    update_msg = build_update_message(currency, pay_friendly, buy, sell, spread_percent, prev_spread=prev_sent_spread)
                    send_telegram_alert(update_msg)
                    logging.info(f"Update alert sent for {pair_key} (spread {spread_percent:.2f}%)")
                    # mark snapshot as sent
                    set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                              last_buy_price=buy["price"], last_sell_price=sell["price"], mark_sent=True)
                else:
                    # no update to send, but refresh last observed snapshot (not last_sent)
                    set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                              last_buy_price=buy["price"], last_sell_price=sell["price"], mark_sent=False)
        else:
            # FALLING EDGE: was active and now below threshold -> send single "ended" message
            if was_active:
                end_msg = build_end_message(currency, pay_friendly, buy, sell, spread_percent)
                send_telegram_alert(end_msg)
                logging.info(f"End alert sent for {pair_key} (spread {spread_percent:.2f}%)")
                # set inactive and reset last_sent snapshot (so next rising edge sends start even if same values)
                set_active_state_snapshot(pair_key, active=False, last_spread=spread_percent,
                                          last_buy_price=buy["price"], last_sell_price=sell["price"], mark_sent=False)
            else:
                # remain inactive; update observed snapshot
                set_active_state_snapshot(pair_key, active=False, last_spread=spread_percent,
                                          last_buy_price=buy["price"], last_sell_price=sell["price"], mark_sent=False)

        # once processed a successful variant, stop trying other variants
        break

# ---------------------- main loop (wrapped) ----------------------
def run_monitor_loop():
    logging.info(f"Monitoring: {pairs_to_monitor}. Every {REFRESH_EVERY}s")
    logging.info(f"Threshold: {Profit_Threshold_percent}%. Update-any={ALERT_UPDATE_ON_ANY_CHANGE}, "
                 f"min-delta={ALERT_UPDATE_MIN_DELTA_PERCENT}%, price-delta={ALERT_UPDATE_PRICE_CHANGE_PERCENT}%")
    try:
        while True:
            start_ts = time.time()
            with ThreadPoolExecutor(max_workers=min(len(pairs_to_monitor), 6)) as ex:
                futures = [ex.submit(process_pair, cur, m, thr) for cur,m,thr in pairs_to_monitor]
                for f in futures:
                    try:
                        f.result()
                    except Exception as e:
                        logging.error(f"Proc error: {e}")
                    time.sleep(SLEEP_BETWEEN_PAIRS)
            elapsed = time.time() - start_ts
            time.sleep(max(0, REFRESH_EVERY - elapsed))
    except Exception:
        logging.exception("run_monitor_loop crashed")
    except KeyboardInterrupt:
        logging.info("Stopped by user.")

def start_worker():
    run_monitor_loop()

if __name__ == "__main__":
    start_worker()
