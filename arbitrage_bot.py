import os
import time
import logging
import random
import math
import requests
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------- config (env-friendly) ----------------------
BINANCE_P2P_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
ROWS_PER_REQUEST = int(os.getenv("ROWS_PER_REQUEST", "20"))
TIMEOUT = int(os.getenv("TIMEOUT", "10"))
MAX_SCAN_PAGES = int(os.getenv("MAX_SCAN_PAGES", "8"))  # reduce default pages to avoid many requests

# delays that control request pacing and staggering
SLEEP_BETWEEN_PAGES = float(os.getenv("SLEEP_BETWEEN_PAGES", "0.15"))
SLEEP_BETWEEN_PAIRS = float(os.getenv("SLEEP_BETWEEN_PAIRS", "0.30"))  # used to stagger task submission

# rate-limiter / backoff tuning
REQUESTS_PER_MINUTE = int(os.getenv("REQUESTS_PER_MINUTE", "20"))  # target requests per minute (global)
MAX_CONCURRENT_WORKERS = int(os.getenv("MAX_CONCURRENT_WORKERS", "2"))  # threadpool size
MAX_FETCH_RETRIES_ON_429 = int(os.getenv("MAX_FETCH_RETRIES_ON_429", "5"))
INITIAL_BACKOFF_SECONDS = float(os.getenv("INITIAL_BACKOFF_SECONDS", "1.0"))
MAX_BACKOFF_SECONDS = float(os.getenv("MAX_BACKOFF_SECONDS", "60.0"))
JITTER_FACTOR = float(os.getenv("JITTER_FACTOR", "0.25"))  # fraction of backoff to randomize
MAX_CONSECUTIVE_429_BEFORE_COOLDOWN = int(os.getenv("MAX_CONSECUTIVE_429_BEFORE_COOLDOWN", "8"))
EXTENDED_COOLDOWN_SECONDS = int(os.getenv("EXTENDED_COOLDOWN_SECONDS", "300"))  # 5 minutes

# tolerance for float comparisons to avoid duplicate alerts due to tiny rounding diffs
ALERT_VALUE_TOLERANCE = float(os.getenv("ALERT_VALUE_TOLERANCE", "0.0001"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_IMAGE_URL = os.getenv("TELEGRAM_IMAGE_URL", "https://i.ibb.co/67XZq1QL/212.png").strip()
TELEGRAM_IMAGE_FILE_ID = os.getenv("TELEGRAM_IMAGE_FILE_ID", "").strip()  # preferred

ALERT_TTL_SECONDS = int(os.getenv("ALERT_TTL_SECONDS", "0"))
ALERT_DEDUP_MODE = os.getenv("ALERT_DEDUP_MODE", "exact").lower()

REFRESH_EVERY = int(os.getenv("REFRESH_EVERY", "120"))
PROFIT_THRESHOLD_PERCENT = float(os.getenv("PROFIT_THRESHOLD_PERCENT", "3"))  # example default 3%

ALERT_UPDATE_ON_ANY_CHANGE = os.getenv("ALERT_UPDATE_ON_ANY_CHANGE", "1").strip()
ALERT_UPDATE_MIN_DELTA_PERCENT = float(os.getenv("ALERT_UPDATE_MIN_DELTA_PERCENT", "0.01"))
ALERT_UPDATE_PRICE_CHANGE_PERCENT = float(os.getenv("ALERT_UPDATE_PRICE_CHANGE_PERCENT", "0.05"))

DEFAULT_MIN_LIMIT = float(os.getenv("DEFAULT_MIN_LIMIT", "100"))
MIN_LIMIT_THRESHOLDS_ENV = os.getenv("MIN_LIMIT_THRESHOLDS", "").strip()  # e.g. "EGP=90;USD=50"
PAIRS_ENV = os.getenv("PAIRS", "").strip()
SELECTED_CURRENCY = os.getenv("SELECTED_CURRENCY", "ALL").strip().upper()
SELECTED_METHOD = os.getenv("SELECTED_METHOD", "ALL").strip()

# ---------------------- static lists ----------------------
currency_list = ["USD", "CAD", "NZD", "AUD", "GBP", "JPY", "EUR", "EGP", "MAD", "SAR", "AED", "KWD", "DZD"]

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
    "SkrillMoneybookers": "Skrill", "Skrill": "Skrill", "NETELLER": "NETELLER", "AirTM": "AirTM",
    "DukascopyBank": "Dukascopy Bank", "Ahlibank": "Ahlibank", "BanqueMisr": "Banque Misr",
}

# ---------------------- logging ----------------------
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# ---------------------- helpers ----------------------
def safe_float(val, default=0.0):
    try:
        if val is None:
            return float(default)
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip().replace(",", "")
        return float(s.split()[0])
    except Exception:
        return float(default)

def parse_thresholds(env_str, defaults):
    out = {k: float(DEFAULT_MIN_LIMIT) for k in defaults}
    if not env_str:
        return out
    parts = [p.strip() for p in env_str.replace(",", ";").split(";") if p.strip()]
    for p in parts:
        if "=" in p or ":" in p:
            sep = "=" if "=" in p else ":"
            k, v = p.split(sep, 1)
            try:
                out[k.strip().upper()] = float(v.strip())
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
# keep original retry for non-429 transient server errors too
retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))
HEADERS = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0 (compatible; ArbitrageChecker/1.0)"}

# ---------------------- global rate-limiter state (token bucket) ----------------------
token_bucket = {
    "tokens": float(max(1, REQUESTS_PER_MINUTE)),  # start with full bucket
    "last_refill": time.time()
}
token_lock = threading.Lock()

def acquire_token_blocking():
    """
    Token bucket: refill tokens proportional to elapsed seconds.
    Blocks until at least 1 token is available, then consumes and returns.
    """
    while True:
        with token_lock:
            now = time.time()
            elapsed = now - token_bucket["last_refill"]
            if elapsed > 0:
                # refill proportionally
                refill = (elapsed / 60.0) * REQUESTS_PER_MINUTE
                if refill >= 1.0:
                    token_bucket["tokens"] = min(float(REQUESTS_PER_MINUTE), token_bucket["tokens"] + refill)
                    token_bucket["last_refill"] = now
            if token_bucket["tokens"] >= 1.0:
                token_bucket["tokens"] -= 1.0
                return True
            # compute time until next token
            # each token interval seconds = 60 / REQUESTS_PER_MINUTE
            sec_per_token = 60.0 / max(1, REQUESTS_PER_MINUTE)
            to_sleep = sec_per_token
        logging.debug(f"Token bucket empty, sleeping {to_sleep:.3f}s")
        time.sleep(to_sleep)

# also keep a min-interval enforcement to avoid microbursts
last_request_ts = [0.0]
last_request_lock = threading.Lock()
consecutive_429_count = 0
consecutive_429_lock = threading.Lock()

MIN_INTERVAL_BASE = max(0.0, 60.0 / max(1, REQUESTS_PER_MINUTE))

def rate_limit_wait():
    """
    Enforce min-interval between requests and adapt multiplier when 429s happen.
    """
    with consecutive_429_lock:
        c429 = consecutive_429_count

    # multiplier grows with repeated 429s but cap it to avoid extreme long multipliers
    cap = 64
    if c429 <= 2:
        multiplier = 1.0
    else:
        multiplier = min(cap, 2 ** (c429 - 2))

    effective_min_interval = MIN_INTERVAL_BASE * multiplier
    # small random jitter to break pattern
    jitter = random.uniform(0, min(0.25 * effective_min_interval, 0.5))

    with last_request_lock:
        now = time.time()
        elapsed = now - last_request_ts[0]
        if elapsed < (effective_min_interval + jitter):
            to_sleep = (effective_min_interval + jitter) - elapsed
            logging.debug(f"Rate limiter: sleeping {to_sleep:.3f}s to respect min interval (mult={multiplier})")
            time.sleep(to_sleep)
        last_request_ts[0] = time.time()

# ---------------------- helpers for value comparison ----------------------
def values_close(a, b, tol=ALERT_VALUE_TOLERANCE):
    """
    Return True if a and b are effectively equal within absolute tolerance tol.
    Handles None: if either is None -> False (not close).
    """
    if a is None or b is None:
        return False
    try:
        return math.isclose(float(a), float(b), rel_tol=0.0, abs_tol=tol)
    except Exception:
        return False

# ---------------------- fetch (FIRST matching ad logic) with smart backoff ----------------------
def fetch_page_raw(fiat, pay_type, trade_type, page, rows=ROWS_PER_REQUEST):
    """
    Robust fetch with:
      - token bucket + min-interval (rate_limit_wait)
      - retries on network errors
      - special handling for 429 with exponential backoff + jitter + Retry-After
    Returns list of items (or empty list on failure).
    """
    global consecutive_429_count

    payload = {"asset": "USDT", "fiat": fiat, "tradeType": trade_type, "payTypes": [pay_type], "page": page, "rows": rows}

    for attempt in range(1, MAX_FETCH_RETRIES_ON_429 + 1):
        # acquire global token and wait minimum interval
        acquire_token_blocking()
        rate_limit_wait()
        try:
            r = session.post(BINANCE_P2P_URL, json=payload, headers=HEADERS, timeout=TIMEOUT)
            # handle explicit 429
            if r.status_code == 429:
                with consecutive_429_lock:
                    consecutive_429_count += 1
                    c429_local = consecutive_429_count

                # Check Retry-After header
                ra = None
                try:
                    ra_hdr = r.headers.get("Retry-After")
                    if ra_hdr:
                        ra = float(ra_hdr)
                except Exception:
                    ra = None

                # exponential backoff based on attempt
                backoff = min(MAX_BACKOFF_SECONDS, INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1)))
                jitter = random.uniform(0, JITTER_FACTOR * backoff)
                wait = backoff + jitter
                # prefer server suggestion if larger
                if ra and ra > wait:
                    wait = ra + random.uniform(0, 1.0)

                logging.warning(f"Received 429 for {fiat}/{pay_type}/{trade_type} p{page} (attempt {attempt}/{MAX_FETCH_RETRIES_ON_429}). Sleeping {wait:.2f}s (consec429={c429_local})")

                # If we have many consecutive 429s, trigger extended cooldown
                if c429_local >= MAX_CONSECUTIVE_429_BEFORE_COOLDOWN:
                    logging.warning(f"High consecutive 429s ({c429_local}) — entering extended cooldown for {EXTENDED_COOLDOWN_SECONDS}s")
                    time.sleep(EXTENDED_COOLDOWN_SECONDS)
                else:
                    time.sleep(wait)
                continue

            # non-429: try to raise for other HTTP errors
            r.raise_for_status()

            # success -> reset consecutive_429_count
            with consecutive_429_lock:
                consecutive_429_count = 0

            try:
                j = r.json()
                return j.get("data") or []
            except Exception:
                logging.debug(f"Failed to parse JSON response for {fiat}/{pay_type}/{trade_type} p{page}")
                return []
        except requests.RequestException as e:
            logging.debug(f"Network error {fiat} {pay_type} {trade_type} p{page} attempt {attempt}: {e}")
            if attempt < MAX_FETCH_RETRIES_ON_429:
                backoff = min(MAX_BACKOFF_SECONDS, INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1)))
                jitter = random.uniform(0, JITTER_FACTOR * backoff)
                wait = backoff + jitter
                logging.debug(f"Retrying after {wait:.2f}s...")
                time.sleep(wait)
                continue
            else:
                return []
    return []

def find_first_ad(fiat, pay_type, trade_type, page_limit_threshold, rows=ROWS_PER_REQUEST):
    """
    Return the FIRST advert (in page order) whose minSingleTransAmount <= threshold.
    """
    for page in range(1, MAX_SCAN_PAGES + 1):
        items = fetch_page_raw(fiat, pay_type, trade_type, page, rows=rows)
        if not items:
            logging.debug(f"[find_first_ad] no items returned for {fiat}/{pay_type}/{trade_type} p{page} (stopping page scan).")
            break
        for entry in items:
            adv = entry.get("adv") or {}
            try:
                price = safe_float(adv.get("price") or 0.0)
                min_lim = safe_float(adv.get("minSingleTransAmount") or adv.get("minSingleTransAmountDisplay") or 0.0)
                max_lim = safe_float(adv.get("dynamicMaxSingleTransAmount") or adv.get("maxSingleTransAmount") or 0.0)
            except Exception:
                continue

            # verbose debug for each advert we inspect (helps verify computation)
            advertiser = entry.get("advertiser") or {}
            nick = advertiser.get("nickName") or advertiser.get("nick") or advertiser.get("userNo") or ""
            logging.debug(f"[first-search] {fiat}/{pay_type}/{trade_type} p{page} price={price} min={min_lim} max={max_lim} adv_by={nick} thr={page_limit_threshold}")

            if min_lim <= page_limit_threshold:
                # return first matching ad
                advertiser = entry.get("advertiser") or {}
                logging.debug(f"[first-search-match] {fiat}/{pay_type}/{trade_type} p{page} -> price={price} min={min_lim} adv_by={nick}")
                return {
                    "trade_type": trade_type,
                    "currency": fiat,
                    "payment_method": pay_type,
                    "price": price,
                    "min_limit": min_lim,
                    "max_limit": max_lim,
                    "advertiser": advertiser
                }
        time.sleep(SLEEP_BETWEEN_PAGES)
    return None

# ---------------------- messaging ----------------------
def format_currency_flag(cur):
    flags = {"EGP":"🇪🇬","GBP":"🇬🇧","EUR":"🇪🇺","USD":"🇺🇸","CAD":"🇨🇦","NZD":"🇳🇿","AUD":"🇦🇺","JPY":"🇯🇵","MAD":"🇲🇦","SAR":"🇸🇦","AED":"🇦🇪","KWD":"🇰🇼","DZD":"🇩🇿"}
    return flags.get(cur, "")

def send_telegram_alert(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.info("Telegram token/chat not set; skipping send. Message preview:\n" + message)
        return False

    sendphoto_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    sendmsg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    caption = message if len(message) <= 1024 else (message[:1020] + "...")

    # Try file_id first (most reliable)
    if TELEGRAM_IMAGE_FILE_ID:
        try:
            payload = {"chat_id": TELEGRAM_CHAT_ID, "photo": TELEGRAM_IMAGE_FILE_ID, "caption": caption, "parse_mode": "HTML"}
            r = session.post(sendphoto_url, data=payload, timeout=TIMEOUT)
            try:
                jr = r.json()
            except Exception:
                jr = None
            if r.ok and jr and jr.get("ok"):
                logging.info("Telegram photo alert sent (file_id).")
                return True
            else:
                logging.warning(f"sendPhoto(file_id) failed status={r.status_code} json={jr} text={getattr(r,'text','')}")
        except Exception as e:
            logging.warning(f"sendPhoto(file_id) exception: {e}")

    # Then try URL
    if TELEGRAM_IMAGE_URL:
        try:
            payload = {"chat_id": TELEGRAM_CHAT_ID, "photo": TELEGRAM_IMAGE_URL, "caption": caption, "parse_mode": "HTML"}
            r = session.post(sendphoto_url, data=payload, timeout=TIMEOUT)
            try:
                jr = r.json()
            except Exception:
                jr = None
            if r.ok and jr and jr.get("ok"):
                logging.info("Telegram photo alert sent (via URL).")
                return True
            else:
                logging.warning(f"sendPhoto(via URL) failed status={r.status_code} json={jr} text={getattr(r,'text','')}")
        except Exception as e:
            logging.warning(f"sendPhoto(via URL) exception: {e}")
        # upload fallback
        try:
            img_resp = session.get(TELEGRAM_IMAGE_URL, timeout=10)
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
                return True
            else:
                logging.warning(f"sendPhoto(upload) failed status={r2.status_code} json={jr2} text={getattr(r2,'text','')}")
        except Exception as e:
            logging.warning(f"sendPhoto(upload) exception: {e}")

    # Text fallback
    try:
        payload2 = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
        r3 = session.post(sendmsg_url, json=payload2, timeout=TIMEOUT)
        try:
            jr3 = r3.json()
        except Exception:
            jr3 = None
        if r3.ok:
            logging.info(f"Telegram text alert sent. resp={jr3}")
            return True
        else:
            logging.warning(f"Telegram sendMessage failed status={r3.status_code} json={jr3} text={getattr(r3,'text','')}")
            return False
    except Exception as e:
        logging.error(f"Failed to send Telegram message: {e}")
        return False

# ---------------------- message builders ----------------------
ZOOZ_LINK = 'https://zoozfx.com'
ZOOZ_HTML = f'©️<a href="{ZOOZ_LINK}">ZoozFX</a>'

def build_alert_message(cur, pay_friendly, seller_ad, buyer_ad, spread_percent):
    flag = format_currency_flag(cur)
    abs_diff = abs(buyer_ad["price"] - seller_ad["price"])
    sign = "+" if spread_percent > 0 else ""
    return (
        f"🚨 Alert {flag} — {cur} ({pay_friendly})\n\n"
        f"🔴 Sell: <code>{buyer_ad['price']:.4f} {cur}</code>\n"
        f"🟢 Buy: <code>{seller_ad['price']:.4f} {cur}</code>\n\n"
        f"💰 Spread: {sign}{spread_percent:.2f}%  (<code>{abs_diff:.4f} {cur}</code>)\n\n"
        f"💥 Good Luck! {ZOOZ_HTML}"
    )

def build_update_message(cur, pay_friendly, seller_ad, buyer_ad, spread_percent):
    flag = format_currency_flag(cur)
    abs_diff = abs(buyer_ad["price"] - seller_ad["price"])
    sign = "+" if spread_percent > 0 else ""
    return (
        f"🔁 Update {flag} — {cur} ({pay_friendly})\n\n"
        f"🔴 Sell: <code>{buyer_ad['price']:.4f} {cur}</code>\n"
        f"🟢 Buy: <code>{seller_ad['price']:.4f} {cur}</code>\n\n"
        f"💰 Spread: {sign}{spread_percent:.2f}%  (<code>{abs_diff:.4f} {cur}</code>)\n\n"
        f"💥 Good Luck! {ZOOZ_HTML}"
    )

def build_end_message(cur, pay_friendly, seller_ad, buyer_ad, spread_percent):
    flag = format_currency_flag(cur)
    abs_diff = abs(buyer_ad["price"] - seller_ad["price"])
    sign = "+" if spread_percent > 0 else ""
    return (
        f"❌ Ended {flag} — {cur} ({pay_friendly})\n\n"
        f"🔴 Sell: <code>{buyer_ad['price']:.4f} {cur}</code>\n"
        f"🟢 Buy: <code>{seller_ad['price']:.4f} {cur}</code>\n\n"
        f"💰 Spread: {sign}{spread_percent:.2f}%  (<code>{abs_diff:.4f} {cur}</code>)\n\n"
        f"💥 Good Luck! {ZOOZ_HTML}"
    )

# ---------------------- state & locks ----------------------
active_states = {}
active_states_lock = threading.Lock()
pair_locks = {}

def get_pair_lock(pair_key):
    with active_states_lock:
        lock = pair_locks.get(pair_key)
        if lock is None:
            lock = threading.Lock()
            pair_locks[pair_key] = lock
        return lock

def get_active_state(pair_key):
    with active_states_lock:
        rec = active_states.get(pair_key)
        if not rec:
            return {
                "active": False,
                "last_spread": None,
                "last_buy_price": None,
                "last_sell_price": None,
                "since": None,
                "last_sent_spread": None,
                "last_sent_buy": None,
                "last_sent_sell": None,
                "last_sent_time": None
            }
        return rec.copy()

def set_active_state_snapshot(pair_key, *, active=None, last_spread=None, last_buy_price=None, last_sell_price=None, mark_sent=False):
    with active_states_lock:
        rec = active_states.get(pair_key) or {
            "active": False,
            "last_spread": None,
            "last_buy_price": None,
            "last_sell_price": None,
            "since": None,
            "last_sent_spread": None,
            "last_sent_buy": None,
            "last_sent_sell": None,
            "last_sent_time": None
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
            # IMPORTANT: correct assignment keys
            rec["last_sent_spread"] = float(last_spread) if last_spread is not None else rec.get("last_sent_spread")
            rec["last_sent_buy"] = float(last_buy_price) if last_buy_price is not None else rec.get("last_sent_buy")
            rec["last_sent_sell"] = float(last_sell_price) if last_sell_price is not None else rec.get("last_sent_sell")
            rec["last_sent_time"] = time.time()
        active_states[pair_key] = rec

# ---------------------- update logic ----------------------
def relative_change_percent(old, new):
    try:
        if old is None or old == 0:
            return float("inf")
        return abs((new - old) / old) * 100.0
    except Exception:
        return float("inf")

def should_send_update(pair_state, new_spread, new_buy, new_sell):
    """
    Return True if the last-sent values differ (beyond tolerance) from new values,
    OR if we've never sent anything before.
    This ensures we only send when spread or buy or sell changed.
    """
    last_sent_spread = pair_state.get("last_sent_spread")
    last_sent_buy = pair_state.get("last_sent_buy")
    last_sent_sell = pair_state.get("last_sent_sell")

    # if we never sent anything before -> allow
    if last_sent_spread is None and last_sent_buy is None and last_sent_sell is None:
        return True

    if ALERT_UPDATE_ON_ANY_CHANGE == "1":
        # Use tolerant comparisons to avoid floats-precision false positives
        spread_changed = not values_close(last_sent_spread, new_spread)
        buy_changed = not values_close(last_sent_buy, new_buy)
        sell_changed = not values_close(last_sent_sell, new_sell)
        return spread_changed or buy_changed or sell_changed

    # legacy logic when ALERT_UPDATE_ON_ANY_CHANGE != "1"
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

def can_send_start(pair_state):
    last_sent_time = pair_state.get("last_sent_time")
    if last_sent_time is None or ALERT_TTL_SECONDS <= 0:
        return True
    return (time.time() - last_sent_time) >= ALERT_TTL_SECONDS

# ---------------------- core processing (FIRST-ad logic) ----------------------
paytype_variants_map = {
    "SkrillMoneybookers": ["SkrillMoneybookers","Skrill","Skrill (Moneybookers)"],
    "NETELLER": ["NETELLER"],
    "AirTM": ["AirTM"],
    "DukascopyBank": ["DukascopyBank"],
}

def process_pair(currency, method, threshold):
    variants = paytype_variants_map.get(method, [method])
    for variant in variants:
        pair_key = f"{currency}|{variant}"
        lock = get_pair_lock(pair_key)
        with lock:
            # first search BUY page, then SELL page (first matching ad each)
            buyer_ad = find_first_ad(currency, variant, "BUY", threshold)
            seller_ad = find_first_ad(currency, variant, "SELL", threshold)

            logging.debug(f"[found] {pair_key} buyer_ad={buyer_ad} seller_ad={seller_ad}")

            if not buyer_ad or not seller_ad:
                logging.debug(f"{pair_key}: missing buyer or seller ad (buyer_found={bool(buyer_ad)} seller_found={bool(seller_ad)}).")
                continue

            try:
                # buyer_ad.price comes from BUY page => this is price you CAN SELL at (we call it sell_price)
                # seller_ad.price comes from SELL page => this is price you CAN BUY at (we call it buy_price)
                sell_price = float(buyer_ad["price"])
                buy_price = float(seller_ad["price"])
                spread_percent = ((sell_price / buy_price) - 1.0) * 100.0
            except Exception as e:
                logging.warning(f"Spread calc error for {pair_key}: {e}")
                continue

            pay_friendly = friendly_pay_names.get(variant, variant)
            logging.info(f"{pair_key} sell_price(from BUY page)={sell_price:.4f} buy_price(from SELL page)={buy_price:.4f} spread={spread_percent:.2f}% thr={threshold} min_sell={buyer_ad['min_limit']:.2f} min_buy={seller_ad['min_limit']:.2f}")

            state = get_active_state(pair_key)
            was_active = state["active"]

            # decide start / update / end (send only one message per cycle)
            if spread_percent >= PROFIT_THRESHOLD_PERCENT:
                # Start
                if not was_active:
                    # Only send start if values changed (or never sent) AND TTL allows
                    if not should_send_update(state, spread_percent, buy_price, sell_price):
                        logging.debug(f"{pair_key}: Start suppressed (duplicate values). Marking active without sending.")
                        # mark active observed but do NOT mark as last_sent
                        set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                                  last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=False)
                    else:
                        if can_send_start(state):
                            msg = build_alert_message(currency, pay_friendly, seller_ad, buyer_ad, spread_percent)
                            sent = send_telegram_alert(msg)
                            if sent:
                                logging.info(f"Start alert sent for {pair_key} (spread {spread_percent:.2f}%)")
                                set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                                          last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=True)
                            else:
                                logging.warning(f"Failed to send start alert for {pair_key}")
                        else:
                            logging.debug(f"Start suppressed by TTL for {pair_key}")
                            set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                                      last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=False)
                else:
                    # Update?
                    if should_send_update(state, spread_percent, buy_price, sell_price):
                        msg = build_update_message(currency, pay_friendly, seller_ad, buyer_ad, spread_percent)
                        sent = send_telegram_alert(msg)
                        if sent:
                            logging.info(f"Update alert sent for {pair_key} (spread {spread_percent:.2f}%)")
                            set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                                      last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=True)
                        else:
                            logging.warning(f"Failed to send update for {pair_key}")
                    else:
                        # no update; refresh last observed values (but don't overwrite last_sent_*)
                        set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                                  last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=False)
            else:
                # End
                if was_active:
                    # Only send end if values have changed since last sent (prevents duplicate end messages)
                    if should_send_update(state, spread_percent, buy_price, sell_price):
                        msg = build_end_message(currency, pay_friendly, seller_ad, buyer_ad, spread_percent)
                        sent = send_telegram_alert(msg)
                        if sent:
                            logging.info(f"End alert sent for {pair_key} (spread {spread_percent:.2f}%)")
                            set_active_state_snapshot(pair_key, active=False, last_spread=spread_percent,
                                                      last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=True)
                        else:
                            logging.warning(f"Failed to send end alert for {pair_key}")
                    else:
                        logging.debug(f"{pair_key}: End suppressed (duplicate values). Marking inactive without sending.")
                        set_active_state_snapshot(pair_key, active=False, last_spread=spread_percent,
                                                  last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=False)
                else:
                    set_active_state_snapshot(pair_key, active=False, last_spread=spread_percent,
                                              last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=False)

        # processed one variant -> stop trying other variants
        break

# ---------------------- main loop ----------------------
def run_monitor_loop():
    logging.info(f"Monitoring {len(pairs_to_monitor)} pairs. Every {REFRESH_EVERY}s. Profit threshold={PROFIT_THRESHOLD_PERCENT}%. Workers={MAX_CONCURRENT_WORKERS} RPM={REQUESTS_PER_MINUTE}")
    try:
        while True:
            start_ts = time.time()

            # submit tasks with staggering to avoid burst creation
            futures = []
            with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_WORKERS) as ex:
                for cur, m, thr in pairs_to_monitor:
                    futures.append(ex.submit(process_pair, cur, m, thr))
                    # stagger task start to avoid simultaneous bursts
                    time.sleep(SLEEP_BETWEEN_PAIRS)

                # wait for completion and surface exceptions
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception as e:
                        logging.error(f"Proc error: {e}")

            elapsed = time.time() - start_ts
            sleep_for = max(0, REFRESH_EVERY - elapsed)
            logging.debug(f"Cycle done in {elapsed:.2f}s, sleeping {sleep_for:.2f}s until next cycle")
            time.sleep(sleep_for)
    except KeyboardInterrupt:
        logging.info("Stopped by user.")
    except Exception:
        logging.exception("run_monitor_loop crashed")

def start_worker():
    run_monitor_loop()

if __name__ == "__main__":
    start_worker()
