# updated arbitrage_bot.py
# - dynamically discovers payment methods per fiat from Binance P2P (adv.tradeMethods)
# - optionally discovers fiat list from p2p.army (set DISCOVER_ALL_FIATS=0 to disable)
# - scans BUY and SELL sides and requires >= MIN_ADS_REQUIRED (default 5) on both sides
# - default per-currency min limit is DEFAULT_MIN_LIMIT (100) unless overridden by MIN_LIMIT_THRESHOLDS env
# - validates user-specified methods against the discovered list to avoid typos

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
ROWS_PER_REQUEST = int(os.getenv("ROWS_PER_REQUEST", "20"))
TIMEOUT = int(os.getenv("TIMEOUT", "10"))
MAX_SCAN_PAGES = int(os.getenv("MAX_SCAN_PAGES", "60"))
SLEEP_BETWEEN_PAGES = float(os.getenv("SLEEP_BETWEEN_PAGES", "0.09"))
SLEEP_BETWEEN_PAIRS = float(os.getenv("SLEEP_BETWEEN_PAIRS", "0.05"))
DEFAULT_MIN_LIMIT = float(os.getenv("DEFAULT_MIN_LIMIT", "100"))
MIN_ADS_REQUIRED = int(os.getenv("MIN_ADS_REQUIRED", "5"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_IMAGE_URL = os.getenv("TELEGRAM_IMAGE_URL", "https://i.ibb.co/67XZq1QL/212.png").strip()

ALERT_TTL_SECONDS = int(os.getenv("ALERT_TTL_SECONDS", "0"))
ALERT_DEDUP_MODE = os.getenv("ALERT_DEDUP_MODE", "exact").lower()

REFRESH_EVERY = int(os.getenv("REFRESH_EVERY", "60"))
Profit_Threshold_percent = float(os.getenv("PROFIT_THRESHOLD_PERCENT", "0.4"))

ALERT_UPDATE_ON_ANY_CHANGE = os.getenv("ALERT_UPDATE_ON_ANY_CHANGE", "0").strip()
ALERT_UPDATE_MIN_DELTA_PERCENT = float(os.getenv("ALERT_UPDATE_MIN_DELTA_PERCENT", "0.01"))
ALERT_UPDATE_PRICE_CHANGE_PERCENT = float(os.getenv("ALERT_UPDATE_PRICE_CHANGE_PERCENT", "0.05"))

PAIRS_ENV = os.getenv("PAIRS", "").strip()
SELECTED_CURRENCY = os.getenv("SELECTED_CURRENCY", "ALL").strip().upper()
SELECTED_METHOD = os.getenv("SELECTED_METHOD", "ALL").strip()
MIN_LIMIT_THRESHOLDS_ENV = os.getenv("MIN_LIMIT_THRESHOLDS", "").strip()
DISCOVER_ALL_FIATS = os.getenv("DISCOVER_ALL_FIATS", "1").strip()  # 1=try p2p.army -> list of fiats, 0=use currency_list

# initial small default list (used as fallback)
currency_list = ["EGP", "GBP", "EUR", "USD"]

# friendly names map (fallbacks)
friendly_pay_names = {
    "SkrillMoneybookers": "Skrill",
    "Skrill": "Skrill",
    "InstaPay": "InstaPay",
    "Vodafonecash": "Vodafonecash"
}

# ---------------------- logging ----------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ---------------------- helpers ----------------------
def parse_thresholds(env_str, defaults, default_min=DEFAULT_MIN_LIMIT):
    out = {k: float(default_min) for k in defaults}
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
            pairs.append((cur, m, thresholds.get(cur, float(DEFAULT_MIN_LIMIT))))
    return pairs

min_limit_thresholds = parse_thresholds(MIN_LIMIT_THRESHOLDS_ENV, currency_list, default_min=DEFAULT_MIN_LIMIT)

# ---------------------- HTTP session ----------------------
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[429,500,502,503,504])
session.mount("https://", HTTPAdapter(max_retries=retries))
HEADERS = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0 (compatible; ArbitrageChecker/1.0)"}

# ---------------------- fetch ----------------------
def fetch_page_raw(fiat, pay_type, trade_type, page, rows=ROWS_PER_REQUEST):
    payload = {"asset": "USDT", "fiat": fiat, "tradeType": trade_type, "page": page, "rows": rows, "merchantCheck": False}
    # if pay_type is falsy, send empty list to mean 'all payment methods' (Binance accepts [] as no filter)
    payload["payTypes"] = [pay_type] if pay_type else []
    try:
        r = session.post(BINANCE_P2P_URL, json=payload, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json().get("data") or []
    except Exception as e:
        logging.debug(f"Network error {fiat} {pay_type} {trade_type} p{page}: {e}")
        return []

# ---------------------- discovery helpers ----------------------
def discover_payment_methods(fiat, sample_pages=3):
    """Query a few pages (BUY/SELL) for fiat and gather unique payment method identifiers and readable names.
    Returns dict: identifier -> display_name
    """
    found = {}
    for trade_type in ("BUY", "SELL"):
        for page in range(1, sample_pages + 1):
            items = fetch_page_raw(fiat, None, trade_type, page, rows=ROWS_PER_REQUEST)
            if not items:
                break
            for entry in items:
                adv = entry.get("adv") or {}
                # adv['tradeMethods'] is used by many community parsers (see adv.tradeMethods[*].identifier / tradeMethodName)
                for tm in adv.get("tradeMethods", []) or []:
                    identifier = tm.get("identifier") or tm.get("payType") or tm.get("tradeMethodName")
                    name = tm.get("tradeMethodName") or identifier
                    if identifier:
                        found[identifier] = name
            time.sleep(SLEEP_BETWEEN_PAGES)
    return found


def get_all_fiats_from_p2p_army():
    """Optional: try to fetch the fiat list from p2p.army (fast way to discover many fiat codes).
    If it fails we fall back to the builtin currency_list.
    """
    try:
        r = session.get("https://p2p.army/v1/api/get_p2p_fiats", timeout=5)
        r.raise_for_status()
        j = r.json()
        rows = j.get("rows") or []
        if rows:
            logging.info(f"Discovered {len(rows)} fiats from p2p.army")
            return [r.upper() for r in rows]
    except Exception as e:
        logging.debug(f"Failed fetching fiats from p2p.army: {e}")
    return currency_list

# ---------------------- messaging ----------------------
def format_currency_flag(cur):
    flags = {"EGP": "🇪🇬", "GBP": "🇬🇧", "EUR": "🇪🇺", "USD": "🇺🇸"}
    return flags.get(cur, "")

# sendMessage / sendPhoto helper: robust attempts (URL -> upload -> fallback text)
def send_telegram_alert(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.info("Telegram token/chat not set; skipping send. Message preview:\n" + message)
        return

    caption = message if len(message) <= 1024 else (message[:1020] + "...")
    photo_url = TELEGRAM_IMAGE_URL or None

    if photo_url:
        try:
            sendphoto_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            payload = {"chat_id": TELEGRAM_CHAT_ID, "photo": photo_url, "caption": caption, "parse_mode": "HTML"}
            r = session.post(sendphoto_url, data=payload, timeout=TIMEOUT)
            try:
                jr = r.json()
            except Exception:
                jr = None
            if r.ok and jr and jr.get("ok"):
                logging.info("Telegram photo alert sent (via URL).")
                return
            else:
                logging.warning(f"sendPhoto(via URL) failed status={r.status_code} json={jr} text={r.text}")
        except Exception as e:
            logging.warning(f"sendPhoto(via URL) exception: {e}")

        # upload file fallback
        try:
            img_resp = session.get(photo_url, timeout=10)
            img_resp.raise_for_status()
            content_type = img_resp.headers.get("content-type", "image/png")
            files = {"photo": ("image.png", img_resp.content, content_type)}
            data = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption, "parse_mode": "HTML"}
            r2 = session.post(sendphoto_url, data=data, files=files, timeout=TIMEOUT)
            try:
                jr2 = r2.json()
            except Exception:
                jr2 = None
            if r2.ok and jr2 and jr2.get("ok"):
                logging.info("Telegram photo alert sent (uploaded file).")
                return
            else:
                logging.warning(f"sendPhoto(upload) failed status={r2.status_code} json={jr2} text={r2.text}")
        except Exception as e:
            logging.warning(f"sendPhoto(upload) exception: {e}")

    # text fallback
    try:
        sendmsg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload2 = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
        r3 = session.post(sendmsg_url, json=payload2, timeout=TIMEOUT)
        r3.raise_for_status()
        logging.info("Telegram alert sent (text fallback).")
    except Exception as e:
        logging.error(f"Failed to send Telegram message: {e}")

# ---------------------- message builders ----------------------
ZOOZ_LINK = 'https://zoozfx.com'
ZOOZ_HTML = f'©️<a href="{ZOOZ_LINK}">ZoozFX</a>'

def build_alert_message(cur, pay_friendly, sell, buy, spread_percent):
    flag = format_currency_flag(cur)
    abs_diff = abs(buy["price"] - sell["price"])
    sign = "+" if spread_percent > 0 else ""
    return (
        f"🚨 Alert {flag} — {cur} ({pay_friendly})\n\n"
        f"🔴 Sell: <code>{buy['price']:.4f} {cur}</code>\n"
        f"🟢 Buy: <code>{sell['price']:.4f} {cur}</code>\n\n"
        f"💰 Spread: {sign}{spread_percent:.2f}%  (<code>{abs_diff:.4f} {cur}</code>)\n\n"
        f"💥 Good Luck! {ZOOZ_HTML}"
    )

def build_update_message(cur, pay_friendly, sell, buy, spread_percent):
    flag = format_currency_flag(cur)
    abs_diff = abs(buy["price"] - sell["price"])
    sign = "+" if spread_percent > 0 else ""
    return (
        f"🔁 Update {flag} — {cur} ({pay_friendly})\n\n"
        f"🔴 Sell: <code>{buy['price']:.4f} {cur}</code>\n"
        f"🟢 Buy: <code>{sell['price']:.4f} {cur}</code>\n\n"
        f"💰 Spread: {sign}{spread_percent:.2f}%  (<code>{abs_diff:.4f} {cur}</code>)\n\n"
        f"💥 Good Luck! {ZOOZ_HTML}"
    )

def build_end_message(cur, pay_friendly, sell, buy, spread_percent):
    flag = format_currency_flag(cur)
    abs_diff = abs(buy["price"] - sell["price"])
    sign = "+" if spread_percent > 0 else ""
    return (
        f"✅ Ended {flag} — {cur} ({pay_friendly})\n\n"
        f"🔴 Sell: <code>{buy['price']:.4f} {cur}</code>\n"
        f"🟢 Buy: <code>{sell['price']:.4f} {cur}</code>\n\n"
        f"💰 Spread: {sign}{spread_percent:.2f}%  (<code>{abs_diff:.4f} {cur}</code>)\n\n"
        f"💥 Good Luck! {ZOOZ_HTML}"
    )

# ---------------------- per-pair active state + last-sent snapshot ----------------------
active_states = {}
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
            rec["last_sent_spread"] = float(last_spread) if last_spread is not None else rec.get("last_spread")
            rec["last_sent_buy"] = float(last_buy_price) if last_buy_price is not None else rec.get("last_buy_price")
            rec["last_sent_sell"] = float(last_sell_price) if last_sell_price is not None else rec.get("last_sell_price")
            rec["last_sent_time"] = time.time()
        active_states[pair_key] = rec

# ---------------------- update logic ----------------------
def relative_change_percent(old, new):
    try:
        if old == 0:
            return abs(new - old) * 100.0
        return abs((new - old) / old) * 100.0
    except Exception:
        return float("inf")

def should_send_update(pair_state, new_spread, new_buy, new_sell):
    last_sent_spread = pair_state.get("last_sent_spread")
    last_sent_buy = pair_state.get("last_sent_buy")
    last_sent_sell = pair_state.get("last_sent_sell")

    if last_sent_spread is None and last_sent_buy is None and last_sent_sell is None:
        return True

    if ALERT_UPDATE_ON_ANY_CHANGE == "1":
        if (last_sent_spread != new_spread) or (last_sent_buy != new_buy) or (last_sent_sell != new_sell):
            return True
        return False

    if last_sent_spread is None:
        spread_diff = abs(new_spread)
    else:
        spread_diff = abs(new_spread - last_sent_spread)
    if spread_diff >= ALERT_UPDATE_MIN_DELTA_PERCENT:
        return True

    if last_sent_buy is not None and relative_change_percent(last_sent_buy, new_buy) >= ALERT_UPDATE_PRICE_CHANGE_PERCENT:
        return True
    if last_sent_sell is not None and relative_change_percent(last_sent_sell, new_sell) >= ALERT_UPDATE_PRICE_CHANGE_PERCENT:
        return True

    return False

# ---------------------- core processing ----------------------

def get_counts_and_first_prices(fiat, pay_type, threshold):
    """Return (buy_count, buy_first_price, sell_count, sell_first_price). Requires minSingleTransAmount <= threshold to count.
    Scans pages up to MAX_SCAN_PAGES but stops early when MIN_ADS_REQUIRED is met for a side.
    """
    def scan_side(trade_type):
        count = 0
        first_price = None
        for page in range(1, MAX_SCAN_PAGES + 1):
            items = fetch_page_raw(fiat, pay_type, trade_type, page, rows=ROWS_PER_REQUEST)
            if not items:
                break
            for entry in items:
                adv = entry.get("adv") or {}
                try:
                    price = float(adv.get("price") or 0)
                    min_lim = float(adv.get("minSingleTransAmount") or 0)
                except Exception:
                    continue
                if min_lim <= threshold:
                    count += 1
                    if first_price is None:
                        first_price = price
            if count >= MIN_ADS_REQUIRED:
                break
            time.sleep(SLEEP_BETWEEN_PAGES)
        return count, first_price

    buy_count, buy_price = scan_side("BUY")
    sell_count, sell_price = scan_side("SELL")
    return buy_count, buy_price, sell_count, sell_price

# paytype display mapping container (filled by discovery step)
methods_display_map = {}

def process_pair(currency, method_identifier, threshold):
    # method_identifier is the identifier returned by Binance adv.tradeMethods[*].identifier (or similar)
    buy_count, buy_price, sell_count, sell_price = get_counts_and_first_prices(currency, method_identifier, threshold)

    logging.info(f"{currency} {method_identifier}: buy_count={buy_count} sell_count={sell_count} first_buy={buy_price} first_sell={sell_price}")

    if buy_count < MIN_ADS_REQUIRED or sell_count < MIN_ADS_REQUIRED:
        logging.debug(f"{currency} {method_identifier}: not enough ads (need {MIN_ADS_REQUIRED})")
        return

    if not buy_price or not sell_price:
        logging.debug(f"{currency} {method_identifier}: missing price on one side")
        return

    try:
        spread_percent = ((buy_price / sell_price) - 1) * 100
    except Exception as e:
        logging.warning(f"Spread error {currency} {method_identifier}: {e}")
        return

    pay_friendly = methods_display_map.get((currency, method_identifier)) or friendly_pay_names.get(method_identifier, method_identifier)
    logging.info(f"{currency} {method_identifier}: buy(display) {sell_price:.4f} sell(display) {buy_price:.4f} spread {spread_percent:.2f}%")

    pair_key = f"{currency}|{method_identifier}"
    state = get_active_state(pair_key)
    was_active = state["active"]

    if spread_percent >= Profit_Threshold_percent:
        if not was_active:
            message = build_alert_message(currency, pay_friendly, {"price": sell_price}, {"price": buy_price}, spread_percent)
            send_telegram_alert(message)
            logging.info(f"Start alert sent for {pair_key} (spread {spread_percent:.2f}%)")
            set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                      last_buy_price=sell_price, last_sell_price=buy_price, mark_sent=True)
        else:
            if should_send_update(state, spread_percent, sell_price, buy_price):
                update_msg = build_update_message(currency, pay_friendly, {"price": sell_price}, {"price": buy_price}, spread_percent)
                send_telegram_alert(update_msg)
                logging.info(f"Update alert sent for {pair_key} (spread {spread_percent:.2f}%)")
                set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                          last_buy_price=sell_price, last_sell_price=buy_price, mark_sent=True)
            else:
                set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                          last_buy_price=sell_price, last_sell_price=buy_price, mark_sent=False)
    else:
        if was_active:
            end_msg = build_end_message(currency, pay_friendly, {"price": sell_price}, {"price": buy_price}, spread_percent)
            send_telegram_alert(end_msg)
            logging.info(f"End alert sent for {pair_key} (spread {spread_percent:.2f}%)")
            set_active_state_snapshot(pair_key, active=False, last_spread=spread_percent,
                                      last_buy_price=sell_price, last_sell_price=buy_price, mark_sent=False)
        else:
            set_active_state_snapshot(pair_key, active=False, last_spread=spread_percent,
                                      last_buy_price=sell_price, last_sell_price=buy_price, mark_sent=False)

# ---------------------- main loop ----------------------

def build_pairs_to_monitor():
    """Discover currencies (optional) and payment methods, validate user env and return list of (currency, method_identifier, threshold).
    Also populates methods_display_map for friendly names.
    """
    # decide currencies to monitor
    if SELECTED_CURRENCY == "ALL":
        if DISCOVER_ALL_FIATS == "1":
            fiats = get_all_fiats_from_p2p_army()
        else:
            fiats = currency_list
    else:
        fiats = [SELECTED_CURRENCY]

    # override thresholds map: ensure all fiats have a threshold (default DEFAULT_MIN_LIMIT)
    thresholds = parse_thresholds(MIN_LIMIT_THRESHOLDS_ENV, fiats, default_min=DEFAULT_MIN_LIMIT)

    pairs = []
    for cur in fiats:
        cur = cur.upper()
        try:
            discovered = discover_payment_methods(cur, sample_pages=2)
        except Exception as e:
            logging.warning(f"Failed to discover payment methods for {cur}: {e}")
            discovered = {}

        # store display names
        for ident, name in discovered.items():
            methods_display_map[(cur, ident)] = name

        if SELECTED_METHOD and SELECTED_METHOD.upper() != "ALL":
            # user requested specific method; try to match it (exact or case-insensitive search in discovered names)
            req = SELECTED_METHOD
            matched = []
            for ident, name in discovered.items():
                if req == ident or req.lower() == ident.lower() or req.lower() == name.lower() or req.lower() in name.lower():
                    matched.append((cur, ident, thresholds.get(cur, DEFAULT_MIN_LIMIT)))
            if matched:
                pairs.extend(matched)
            else:
                logging.warning(f"Requested method {SELECTED_METHOD} not found for {cur}; skipping {cur}")
        else:
            # add all discovered methods
            for ident in discovered.keys():
                pairs.append((cur, ident, thresholds.get(cur, DEFAULT_MIN_LIMIT)))

    # If PAIRS_ENV is provided, parse it and use it as authoritative (but validate methods)
    if PAIRS_ENV:
        explicit = parse_pairs_env(PAIRS_ENV, thresholds, {k: list(v.keys()) for k, v in [(c, discover_payment_methods(c, sample_pages=1)) for c in fiats]})
        # explicit returns (cur, method_name, thr) where method_name may not be canonical identifier
        pairs = []
        for cur, m, thr in explicit:
            # try to match m to discovered ident for this cur
            discovered = {ident: methods_display_map.get((cur, ident)) or ident for (cur_k, ident) in methods_display_map.keys() if cur_k == cur}
            matched_ident = None
            for ident, name in discovered.items():
                if m == ident or m.lower() == ident.lower() or m.lower() == name.lower() or m.lower() in name.lower():
                    matched_ident = ident
                    break
            if matched_ident:
                pairs.append((cur, matched_ident, thr))
            else:
                logging.warning(f"PAIRS env method {m} for {cur} not found on Binance; skipping")

    # final dedup
    seen = set()
    final = []
    for cur, ident, thr in pairs:
        key = (cur, ident)
        if key in seen:
            continue
        seen.add(key)
        final.append((cur, ident, thr))

    if not final:
        logging.error("No currency/payment pairs selected after discovery/validation. Exiting.")
        raise SystemExit(1)

    logging.info(f"Monitoring {len(final)} pairs (sample): {final[:10]}")
    return final


def run_monitor_loop():
    global pairs_to_monitor
    logging.info("Starting discovery & validation of currencies/payment methods...")
    pairs_to_monitor = build_pairs_to_monitor()
    logging.info(f"Monitoring: {pairs_to_monitor}. Every {REFRESH_EVERY}s")
    logging.info(f"Threshold: {Profit_Threshold_percent}%. Update-any={ALERT_UPDATE_ON_ANY_CHANGE}, "
                 f"min-delta={ALERT_UPDATE_MIN_DELTA_PERCENT}%, price-delta={ALERT_UPDATE_PRICE_CHANGE_PERCENT}%")
    try:
        while True:
            start_ts = time.time()
            with ThreadPoolExecutor(max_workers=min(len(pairs_to_monitor), 10)) as ex:
                futures = [ex.submit(process_pair, cur, m, thr) for cur, m, thr in pairs_to_monitor]
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
