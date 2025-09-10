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

# تحديث قائمة العملات لتشمل جميع العملات المذكورة
currency_list = ["USD", "CAD", "NZD", "AUD", "GBP", "JPY", "EUR", "EGP", "MAD", "SAR", "AED", "KWD", "DZD"]

# تحديث طرق الدفع حسب العملة
payment_methods_map = {
    "EGP": [
        "Ahlibank", "alBaraka", "AlexBank", "ALMASHREQBank", "ArabAfricanBank", 
        "ArabBank", "ArabTunisianBank", "AraratBank", "BANK", "BankAlEtihad", 
        "BankTransferMena", "BanqueduCaire", "BanqueMisr", "Cashapp", "CIB", 
        "CIBBank", "CreditAgricole", "EasyPay", "EmiratesNBD", "EtisalatCash", 
        "FirstIraqiBank", "FPS", "HSBCBankEgypt", "InstaPay", "KFH", "klivvr", 
        "NBE", "NBK", "OrangeCash", "OrangeMoney", "qaheracash", "QatarNationalBank", 
        "QNB", "SpecificBank", "SWIFT", "telda", "Vodafonecash", "wepay", 
        "WesternUnion", "ZAINCASH"
    ],
    # باقي العملات تستخدم طرق الدفع التالية
    "USD": ["SkrillMoneybookers", "NETELLER", "AirTM", "DukascopyBank"],
    "CAD": ["SkrillMoneybookers", "NETELLER", "AirTM", "DukascopyBank"],
    "NZD": ["SkrillMoneybookers", "NETELLER", "AirTM", "DukascopyBank"],
    "AUD": ["SkrillMoneybookers", "NETELLER", "AirTM", "DukascopyBank"],
    "GBP": ["SkrillMoneybookers", "NETELLER", "AirTM", "DukascopyBank"],
    "JPY": ["SkrillMoneybookers", "NETELLER", "AirTM", "DukascopyBank"],
    "EUR": ["SkrillMoneybookers", "NETELLER", "AirTM", "DukascopyBank"],
    "MAD": ["SkrillMoneybookers", "NETELLER", "AirTM", "DukascopyBank"],
    "SAR": ["SkrillMoneybookers", "NETELLER", "AirTM", "DukascopyBank"],
    "AED": ["SkrillMoneybookers", "NETELLER", "AirTM", "DukascopyBank"],
    "KWD": ["SkrillMoneybookers", "NETELLER", "AirTM", "DukascopyBank"],
    "DZD": ["SkrillMoneybookers", "NETELLER", "AirTM", "DukascopyBank"]
}

friendly_pay_names = {
    "SkrillMoneybookers": "Skrill",
    "Skrill": "Skrill",
    "InstaPay": "InstaPay",
    "Vodafonecash": "Vodafonecash",
    "NETELLER": "NETELLER",
    "AirTM": "AirTM",
    "DukascopyBank": "Dukascopy Bank",
    "Ahlibank": "Ahlibank",
    "alBaraka": "alBaraka",
    "AlexBank": "AlexBank",
    "ALMASHREQBank": "ALMASHREQ Bank",
    "ArabAfricanBank": "Arab African Bank",
    "ArabBank": "Arab Bank",
    "ArabTunisianBank": "Arab Tunisian Bank",
    "AraratBank": "Ararat Bank",
    "BANK": "BANK",
    "BankAlEtihad": "Bank Al Etihad",
    "BankTransferMena": "Bank Transfer Mena",
    "BanqueduCaire": "Banque du Caire",
    "BanqueMisr": "Banque Misr",
    "Cashapp": "Cash App",
    "CIB": "CIB",
    "CIBBank": "CIB Bank",
    "CreditAgricole": "Credit Agricole",
    "EasyPay": "EasyPay",
    "EmiratesNBD": "Emirates NBD",
    "EtisalatCash": "Etisalat Cash",
    "FirstIraqiBank": "First Iraqi Bank",
    "FPS": "FPS",
    "HSBCBankEgypt": "HSBC Bank Egypt",
    "InstaPay": "InstaPay",
    "KFH": "KFH",
    "klivvr": "klivvr",
    "NBE": "NBE",
    "NBK": "NBK",
    "OrangeCash": "Orange Cash",
    "OrangeMoney": "Orange Money",
    "qaheracash": "qahera cash",
    "QatarNationalBank": "Qatar National Bank",
    "QNB": "QNB",
    "SpecificBank": "Specific Bank",
    "SWIFT": "SWIFT",
    "telda": "telda",
    "Vodafonecash": "Vodafone Cash",
    "wepay": "wepay",
    "WesternUnion": "Western Union",
    "ZAINCASH": "ZAIN CASH"
}

PAIRS_ENV = os.getenv("PAIRS", "").strip()
SELECTED_CURRENCY = os.getenv("SELECTED_CURRENCY", "ALL").strip().upper()
SELECTED_METHOD = os.getenv("SELECTED_METHOD", "ALL").strip()
MIN_LIMIT_THRESHOLDS_ENV = os.getenv("MIN_LIMIT_THRESHOLDS", "").strip()

# ---------------------- logging ----------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ---------------------- helpers ----------------------
def parse_thresholds(env_str, defaults):
    DEFAULT_MIN_LIMIT = float(os.getenv("DEFAULT_MIN_LIMIT", "100"))
    out = {k: DEFAULT_MIN_LIMIT for k in defaults}

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

# ---------------------- fetch ----------------------
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

# ---------------------- messaging ----------------------
def format_currency_flag(cur):
    flags = {
        "EGP": "🇪🇬", "GBP": "🇬🇧", "EUR": "🇪🇺", "USD": "🇺🇸",
        "CAD": "🇨🇦", "NZD": "🇳🇿", "AUD": "🇦🇺", "JPY": "🇯🇵",
        "MAD": "🇲🇦", "SAR": "🇸🇦", "AED": "🇦🇪", "KWD": "🇰🇼",
        "DZD": "🇩🇿"
    }
    return flags.get(cur, "")

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

        try:
            img_resp = session.get(photo_url, timeout=10)
            img_resp.raise_for_status()
            content_type = img_resp.headers.get("content-type", "image/png")
            files = {"photo": ("zoozfx.png", img_resp.content, content_type)}
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
paytype_variants_map = {
    "SkrillMoneybookers": ["SkrillMoneybookers","Skrill","Skrill (Moneybookers)"],
    "NETELLER": ["NETELLER"],
    "AirTM": ["AirTM"],
    "DukascopyBank": ["DukascopyBank"],
    # إضافة جميع طرق الدفع للجنيه المصري
    "Ahlibank": ["Ahlibank"],
    "alBaraka": ["alBaraka"],
    "AlexBank": ["AlexBank"],
    "ALMASHREQBank": ["ALMASHREQBank"],
    "ArabAfricanBank": ["ArabAfricanBank"],
    "ArabBank": ["ArabBank"],
    "ArabTunisianBank": ["ArabTunisianBank"],
    "AraratBank": ["AraratBank"],
    "BANK": ["BANK"],
    "BankAlEtihad": ["BankAlEtihad"],
    "BankTransferMena": ["BankTransferMena"],
    "BanqueduCaire": ["BanqueduCaire"],
    "BanqueMisr": ["BanqueMisr"],
    "Cashapp": ["Cashapp"],
    "CIB": ["CIB"],
    "CIBBank": ["CIBBank"],
    "CreditAgricole": ["CreditAgricole"],
    "EasyPay": ["EasyPay"],
    "EmiratesNBD": ["EmiratesNBD"],
    "EtisalatCash": ["EtisalatCash"],
    "FirstIraqiBank": ["FirstIraqiBank"],
    "FPS": ["FPS"],
    "HSBCBankEgypt": ["HSBCBankEgypt"],
    "InstaPay": ["InstaPay"],
    "KFH": ["KFH"],
    "klivvr": ["klivvr"],
    "NBE": ["NBE"],
    "NBK": ["NBK"],
    "OrangeCash": ["OrangeCash"],
    "OrangeMoney": ["OrangeMoney"],
    "qaheracash": ["qaheracash"],
    "QatarNationalBank": ["QatarNationalBank"],
    "QNB": ["QNB"],
    "SpecificBank": ["SpecificBank"],
    "SWIFT": ["SWIFT"],
    "telda": ["telda"],
    "Vodafonecash": ["Vodafonecash"],
    "wepay": ["wepay"],
    "WesternUnion": ["WesternUnion"],
    "ZAINCASH": ["ZAINCASH"]
}

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
        logging.info(f"{currency} {variant}: buy(display) {sell['price']:.4f} sell(display) {buy['price']:.4f} spread {spread_percent:.2f}%")

        pair_key = f"{currency}|{variant}"
        state = get_active_state(pair_key)
        was_active = state["active"]

        if spread_percent >= Profit_Threshold_percent:
            if not was_active:
                message = build_alert_message(currency, pay_friendly, sell, buy, spread_percent)
                send_telegram_alert(message)
                logging.info(f"Start alert sent for {pair_key} (spread {spread_percent:.2f}%)")
                set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                          last_buy_price=sell["price"], last_sell_price=buy["price"], mark_sent=True)
            else:
                if should_send_update(state, spread_percent, sell["price"], buy["price"]):
                    update_msg = build_update_message(currency, pay_friendly, sell, buy, spread_percent)
                    send_telegram_alert(update_msg)
                    logging.info(f"Update alert sent for {pair_key} (spread {spread_percent:.2f}%)")
                    set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                              last_buy_price=sell["price"], last_sell_price=buy["price"], mark_sent=True)
                else:
                    set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                              last_buy_price=sell["price"], last_sell_price=buy["price"], mark_sent=False)
        else:
            if was_active:
                end_msg = build_end_message(currency, pay_friendly, sell, buy, spread_percent)
                send_telegram_alert(end_msg)
                logging.info(f"End alert sent for {pair_key} (spread {spread_percent:.2f}%)")
                set_active_state_snapshot(pair_key, active=False, last_spread=spread_percent,
                                          last_buy_price=sell["price"], last_sell_price=buy["price"], mark_sent=False)
            else:
                set_active_state_snapshot(pair_key, active=False, last_spread=spread_percent,
                                          last_buy_price=sell["price"], last_sell_price=buy["price"], mark_sent=False)

        break

# ---------------------- main loop ----------------------
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
