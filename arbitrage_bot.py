import os
import time
import logging
import random
import math
import re
import requests
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------- config (env-friendly) ----------------------
BINANCE_P2P_URL = os.getenv("BINANCE_P2P_URL", "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search")
ROWS_PER_REQUEST = int(os.getenv("ROWS_PER_REQUEST", "20"))
FAST_PROBE_ROWS = int(os.getenv("FAST_PROBE_ROWS", "1"))  # rows for the fast probe
TIMEOUT = int(os.getenv("TIMEOUT", "10"))
MAX_SCAN_PAGES = int(os.getenv("MAX_SCAN_PAGES", "8"))

# delays that control request pacing and staggering
SLEEP_BETWEEN_PAGES = float(os.getenv("SLEEP_BETWEEN_PAGES", "0.15"))
SLEEP_BETWEEN_PAIRS = float(os.getenv("SLEEP_BETWEEN_PAIRS", "0.30"))

# rate-limiter / backoff tuning
REQUESTS_PER_MINUTE = int(os.getenv("REQUESTS_PER_MINUTE", "20"))
MAX_CONCURRENT_WORKERS = int(os.getenv("MAX_CONCURRENT_WORKERS", "2"))
MAX_FETCH_RETRIES_ON_429 = int(os.getenv("MAX_FETCH_RETRIES_ON_429", "5"))
INITIAL_BACKOFF_SECONDS = float(os.getenv("INITIAL_BACKOFF_SECONDS", "1.0"))
MAX_BACKOFF_SECONDS = float(os.getenv("MAX_BACKOFF_SECONDS", "60.0"))
JITTER_FACTOR = float(os.getenv("JITTER_FACTOR", "0.25"))
MAX_CONSECUTIVE_429_BEFORE_COOLDOWN = int(os.getenv("MAX_CONSECUTIVE_429_BEFORE_COOLDOWN", "8"))
EXTENDED_COOLDOWN_SECONDS = int(os.getenv("EXTENDED_COOLDOWN_SECONDS", "300"))

# tolerance for float comparisons to avoid duplicate alerts due to tiny rounding diffs
ALERT_VALUE_TOLERANCE = float(os.getenv("ALERT_VALUE_TOLERANCE", "0.0001"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_IMAGE_URL = os.getenv("TELEGRAM_IMAGE_URL", "https://i.ibb.co/67XZq1QL/212.png").strip()
TELEGRAM_IMAGE_FILE_ID = os.getenv("TELEGRAM_IMAGE_FILE_ID", "").strip()

ALERT_TTL_SECONDS = int(os.getenv("ALERT_TTL_SECONDS", "0"))
ALERT_DEDUP_MODE = os.getenv("ALERT_DEDUP_MODE", "exact").lower()

REFRESH_EVERY = int(os.getenv("REFRESH_EVERY", "120"))
PROFIT_THRESHOLD_PERCENT = float(os.getenv("PROFIT_THRESHOLD_PERCENT", "3"))

ALERT_UPDATE_ON_ANY_CHANGE = os.getenv("ALERT_UPDATE_ON_ANY_CHANGE", "1").strip()
ALERT_UPDATE_MIN_DELTA_PERCENT = float(os.getenv("ALERT_UPDATE_MIN_DELTA_PERCENT", "0.01"))
ALERT_UPDATE_PRICE_CHANGE_PERCENT = float(os.getenv("ALERT_UPDATE_PRICE_CHANGE_PERCENT", "0.05"))

# new envs: min & max defaults + per-currency thresholds
DEFAULT_MIN_LIMIT = float(os.getenv("DEFAULT_MIN_LIMIT", "100"))
MIN_LIMIT_THRESHOLDS_ENV = os.getenv("MIN_LIMIT_THRESHOLDS", "").strip()
DEFAULT_MAX_LIMIT = float(os.getenv("DEFAULT_MAX_LIMIT", "0"))  # 0 == no max constraint by default
MAX_LIMIT_THRESHOLDS_ENV = os.getenv("MAX_LIMIT_THRESHOLDS", "").strip()

PAIRS_ENV = os.getenv("PAIRS", "").strip()
SELECTED_CURRENCY = os.getenv("SELECTED_CURRENCY", "ALL").strip().upper()
SELECTED_METHOD = os.getenv("SELECTED_METHOD", "ALL").strip()

# New envs requested:
PAYMENT_METHODS_ENV = os.getenv("PAYMENT_METHODS", "").strip()  # comma-separated whitelist (e.g. "Skrill,NETELLER")
EXCLUDE_PAYMENT_METHODS_ENV = os.getenv("EXCLUDE_PAYMENT_METHODS", "").strip()  # comma-separated exclude list
PROFIT_THRESHOLDS_ENV = os.getenv("PROFIT_THRESHOLDS", "").strip()  # e.g. "GBP:Skrill=3.5;EUR=3;Skrill=3.2;DEFAULT=3"

# ---------------------- static lists ----------------------
currency_list = ["USD", "GBP", "EUR", "EGP", "MAD", "SAR"]

payment_methods_map = {
    "EGP": [
        "AlexBank", "ALMASHREQBank", "ArabAfricanBank","BANK",
        "BanqueMisr", "CIB","CIBBank", "EtisalatCash",
        "InstaPay", "klivvr","OrangeCash", "OrangeMoney",
        "telda", "Vodafonecash", "wepay"
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


def parse_thresholds(env_str, currencies, default_value):
    """
    Parse env string like "GBP=200;EUR=150" into dict { 'GBP':200.0, 'EUR':150.0 }.
    `currencies` is iterable of currency keys to pre-populate defaults.
    `default_value` is used to fill unspecified currencies.
    """
    out = {k: float(default_value) for k in currencies}
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


def parse_profit_thresholds(env_str):
    """
    Parse PROFIT_THRESHOLDS into lookup maps.
    Supported forms:
      - CUR:METHOD=val   (e.g. GBP:Skrill=3.5)
      - CUR=val          (e.g. GBP=3)
      - METHOD=val       (e.g. Skrill=3)
      - DEFAULT=val
    """
    def norm_cur(c): return c.strip().upper()
    def norm_method(m): return re.sub(r'[^0-9a-z]', '', m.strip().lower())

    exact = {}   # (cur, method_norm) -> float
    cur_map = {} # cur -> float
    method_map = {} # method_norm -> float
    default = None

    if not env_str:
        return exact, cur_map, method_map, default

    parts = [p.strip() for p in env_str.replace(",", ";").split(";") if p.strip()]
    for p in parts:
        if "=" not in p:
            continue
        k, v = p.split("=", 1)
        k = k.strip()
        try:
            val = float(v.strip())
        except Exception:
            logging.warning(f"Invalid profit value for {k}: {v}")
            continue
        if ":" in k:
            cur, method = [x.strip() for x in k.split(":", 1)]
            exact[(norm_cur(cur), norm_method(method))] = val
        else:
            if k.strip().upper() == "DEFAULT":
                default = val
            elif k.strip().upper() in [c.upper() for c in currency_list]:
                cur_map[norm_cur(k)] = val
            else:
                method_map[norm_method(k)] = val
    return exact, cur_map, method_map, default


def normalize_method_name(m):
    if not m:
        return ""
    return re.sub(r'[^0-9a-z]', '', str(m).strip().lower())


# ---------------------- parse new envs ----------------------
exact_profit_map, profit_cur_map, profit_method_map, profit_default = parse_profit_thresholds(PROFIT_THRESHOLDS_ENV)

allowed_methods_set = set([normalize_method_name(x) for x in PAYMENT_METHODS_ENV.split(",") if x.strip()]) if PAYMENT_METHODS_ENV else set()
exclude_methods_set = set([normalize_method_name(x) for x in EXCLUDE_PAYMENT_METHODS_ENV.split(",") if x.strip()]) if EXCLUDE_PAYMENT_METHODS_ENV else set()

def method_allowed(method):
    """Return True if method is allowed given whitelist/exclude. Matches against friendly names too."""
    norm = normalize_method_name(method)
    friendly = normalize_method_name(friendly_pay_names.get(method, ""))
    candidates = {norm, friendly}
    if allowed_methods_set:
        # must match at least one allowed token
        if not (candidates & allowed_methods_set):
            return False
    if exclude_methods_set and (candidates & exclude_methods_set):
        return False
    return True

def get_profit_threshold(cur, method):
    """Resolve profit threshold for given currency and method using PROFIT_THRESHOLDS rules and fallbacks."""
    ncur = (cur or "").strip().upper()
    nmethod = normalize_method_name(method or "")
    key = (ncur, nmethod)
    if key in exact_profit_map:
        return exact_profit_map[key]
    if ncur in profit_cur_map:
        return profit_cur_map[ncur]
    if nmethod in profit_method_map:
        return profit_method_map[nmethod]
    if profit_default is not None:
        return profit_default
    return float(PROFIT_THRESHOLD_PERCENT)


# ---------------------- HTTP session ----------------------
session = requests.Session()
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
    while True:
        with token_lock:
            now = time.time()
            elapsed = now - token_bucket["last_refill"]
            if elapsed > 0:
                refill = (elapsed / 60.0) * REQUESTS_PER_MINUTE
                token_bucket["tokens"] = min(float(REQUESTS_PER_MINUTE), token_bucket["tokens"] + refill)
                token_bucket["last_refill"] = now

            if token_bucket["tokens"] >= 1.0:
                token_bucket["tokens"] -= 1.0
                return True

            sec_per_token = 60.0 / max(1, REQUESTS_PER_MINUTE)
            to_sleep = sec_per_token
        logging.debug(f"Token bucket empty, sleeping {to_sleep:.3f}s")
        time.sleep(to_sleep)

last_request_ts = [0.0]
last_request_lock = threading.Lock()
consecutive_429_count = 0
consecutive_429_lock = threading.Lock()

MIN_INTERVAL_BASE = max(0.0, 60.0 / max(1, REQUESTS_PER_MINUTE))


def rate_limit_wait():
    with consecutive_429_lock:
        c429 = consecutive_429_count

    cap = 64
    if c429 <= 2:
        multiplier = 1.0
    else:
        multiplier = min(cap, 2 ** (c429 - 1))

    effective_min_interval = MIN_INTERVAL_BASE * multiplier
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
    if a is None or b is None:
        return False
    try:
        return math.isclose(float(a), float(b), rel_tol=0.0, abs_tol=tol)
    except Exception:
        return False

# ---------------------- fetch (FIRST matching ad logic) with smart backoff ----------------------

def fetch_page_raw(fiat, pay_type, trade_type, page, rows=ROWS_PER_REQUEST):
    global consecutive_429_count

    payload = {"asset": "USDT", "fiat": fiat, "tradeType": trade_type, "payTypes": [pay_type], "page": page, "rows": rows}

    for attempt in range(1, MAX_FETCH_RETRIES_ON_429 + 1):
        acquire_token_blocking()
        rate_limit_wait()
        try:
            r = session.post(BINANCE_P2P_URL, json=payload, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 429:
                with consecutive_429_lock:
                    consecutive_429_count += 1
                    c429_local = consecutive_429_count

                ra = None
                try:
                    ra_hdr = r.headers.get("Retry-After")
                    if ra_hdr:
                        ra = float(ra_hdr)
                except Exception:
                    ra = None

                backoff = min(MAX_BACKOFF_SECONDS, INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1)))
                jitter = random.uniform(0, JITTER_FACTOR * backoff)
                wait = backoff + jitter
                if ra and ra > wait:
                    wait = ra + random.uniform(0, 1.0)

                logging.warning(f"Received 429 for {fiat}/{pay_type}/{trade_type} p{page} (attempt {attempt}/{MAX_FETCH_RETRIES_ON_429}). Sleeping {wait:.2f}s (consec429={c429_local})")

                if c429_local >= MAX_CONSECUTIVE_429_BEFORE_COOLDOWN:
                    logging.warning(f"High consecutive 429s ({c429_local}) — entering extended cooldown for {EXTENDED_COOLDOWN_SECONDS}s")
                    time.sleep(EXTENDED_COOLDOWN_SECONDS)
                else:
                    time.sleep(wait)
                continue

            r.raise_for_status()

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

def find_first_ad(fiat, pay_type, trade_type, page_limit_min_threshold, page_limit_max_threshold=None, rows=ROWS_PER_REQUEST):
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

            advertiser = entry.get("advertiser") or {}
            nick = advertiser.get("nickName") or advertiser.get("nick") or advertiser.get("userNo") or ""
            logging.debug(
                f"[first-search] {fiat}/{pay_type}/{trade_type} p{page} price={price} "
                f"min={min_lim} max={max_lim} adv_by={nick} thr_min={page_limit_min_threshold} thr_max={page_limit_max_threshold}"
            )

            # check min <= min_threshold
            min_ok = (min_lim <= page_limit_min_threshold)
            # check max >= max_threshold, but treat 0 as "no max constraint"
            if page_limit_max_threshold is None:
                max_ok = True
            else:
                max_ok = (page_limit_max_threshold == 0) or (max_lim >= page_limit_max_threshold)

            if min_ok and max_ok:
                logging.debug(f"[first-search-match] {fiat}/{pay_type}/{trade_type} p{page} -> price={price} min={min_lim} max={max_lim} adv_by={nick}")
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


def _try_send_photo(payload_data, files=None):
    sendphoto_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        if files is not None:
            r = session.post(sendphoto_url, data=payload_data, files=files, timeout=TIMEOUT)
        else:
            r = session.post(sendphoto_url, data=payload_data, timeout=TIMEOUT)
        try:
            jr = r.json()
        except Exception:
            jr = None
        if r.ok and jr and jr.get("ok"):
            return True, jr, r.status_code
        return False, jr or getattr(r, 'text', ''), r.status_code
    except Exception as e:
        return False, str(e), None


def send_telegram_alert(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.info("Telegram token/chat not set; skipping send. Message preview:\n" + message)
        return False

    max_photo_attempts = 3

    if TELEGRAM_IMAGE_FILE_ID:
        payload = {"chat_id": TELEGRAM_CHAT_ID, "photo": TELEGRAM_IMAGE_FILE_ID, "caption": (message if len(message) <= 1024 else (message[:1020] + "...")), "parse_mode": "HTML"}
        for attempt in range(1, max_photo_attempts + 1):
            ok, jr_or_text, status = _try_send_photo(payload)
            if ok:
                logging.info("Telegram photo alert sent (file_id).")
                return True
            logging.warning(f"sendPhoto(file_id) attempt {attempt}/{max_photo_attempts} failed status={status} resp={jr_or_text}")
            time.sleep(0.5 * attempt + random.uniform(0, 0.3))

    if TELEGRAM_IMAGE_URL:
        payload = {"chat_id": TELEGRAM_CHAT_ID, "photo": TELEGRAM_IMAGE_URL, "caption": (message if len(message) <= 1024 else (message[:1020] + "...")), "parse_mode": "HTML"}
        for attempt in range(1, max_photo_attempts + 1):
            ok, jr_or_text, status = _try_send_photo(payload)
            if ok:
                logging.info("Telegram photo alert sent (via URL).")
                return True
            logging.warning(f"sendPhoto(via URL) attempt {attempt}/{max_photo_attempts} failed status={status} resp={jr_or_text}")
            time.sleep(0.5 * attempt + random.uniform(0, 0.3))

        try:
            img_resp = session.get(TELEGRAM_IMAGE_URL, timeout=10)
            img_resp.raise_for_status()
            content_type = img_resp.headers.get("content-type", "image/png")
            files = {"photo": ("zoozfx.png", img_resp.content, content_type)}
            data = {"chat_id": TELEGRAM_CHAT_ID, "caption": (message if len(message) <= 1024 else (message[:1020] + "...")), "parse_mode": "HTML"}
            for attempt in range(1, max_photo_attempts + 1):
                ok, jr_or_text, status = _try_send_photo(data, files=files)
                if ok:
                    logging.info("Telegram photo alert sent (uploaded file).")
                    return True
                logging.warning(f"sendPhoto(upload) attempt {attempt}/{max_photo_attempts} failed status={status} resp={jr_or_text}")
                time.sleep(0.5 * attempt + random.uniform(0, 0.3))
        except Exception as e:
            logging.warning(f"sendPhoto(upload) exception: {e}")

    try:
        sendmsg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
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

def _make_hashtag(cur, method):
    if not cur:
        cur_token = ""
    else:
        cur_token = str(cur).strip().upper()
    method_label = friendly_pay_names.get(method, method) if method else method
    if not method_label:
        method_label = ""
    token = re.sub(r'[^0-9A-Za-z]+', '_', str(method_label).strip()).strip('_')
    if len(token) > 30:
        token = token[:30].rstrip('_')
    if not cur_token or not token:
        return ""
    return f"#{cur_token}_{token}"


def build_alert_message(cur, pay_friendly, seller_ad, buyer_ad, spread_percent):
    flag = format_currency_flag(cur)
    abs_diff = abs(buyer_ad["price"] - seller_ad["price"])
    sign = "+" if spread_percent > 0 else ""
    method_name = (seller_ad.get("payment_method") or buyer_ad.get("payment_method") or pay_friendly)
    hashtag = _make_hashtag(cur, method_name)
    hashtag_line = (hashtag) if hashtag else ""
    
    seller_price = seller_ad['price']
    buyer_price = buyer_ad['price']
    part1 = (100 - (100 * (1 / 69))) / seller_price
    part2 = 100 / buyer_price
    profit_value = part1 - part2
    sign = "+" if spread_percent >= 0 else "-"
    
    return (
        f"🚨 Alert {flag} ★ {hashtag_line} ★\n\n"
        f"🔴 Sell: <code>{buyer_ad['price']:.4f} {cur}</code>\n"
        f"🟢 Buy: <code>{seller_ad['price']:.4f} {cur}</code>\n\n"
        f"🔥 <b>Spread: {sign}{spread_percent:.2f}%  (<code>{abs_diff:.4f} {cur}</code>)</b>\n\n"
        f"💰 Profit: <code>{profit_value:.4f} %</code>\n\n"
        f"➤ {ZOOZ_HTML} ⭐️"
    )


def build_update_message(cur, pay_friendly, seller_ad, buyer_ad, spread_percent):
    flag = format_currency_flag(cur)
    abs_diff = abs(buyer_ad["price"] - seller_ad["price"])
    sign = "+" if spread_percent > 0 else ""
    method_name = (seller_ad.get("payment_method") or buyer_ad.get("payment_method") or pay_friendly)
    hashtag = _make_hashtag(cur, method_name)
    hashtag_line = (hashtag) if hashtag else ""

    seller_price = seller_ad['price']
    buyer_price = buyer_ad['price']
    part1 = (100 - (100 * (1 / 69))) / seller_price
    part2 = 100 / buyer_price
    profit_value = part1 - part2
    sign = "+" if spread_percent >= 0 else "-"
    
    return (
        f"🔁 Update {flag} ★ {hashtag_line} ★\n\n"
        f"🔴 Sell: <code>{buyer_ad['price']:.4f} {cur}</code>\n"
        f"🟢 Buy: <code>{seller_ad['price']:.4f} {cur}</code>\n\n"
        f"🔥 <b>Spread: {sign}{spread_percent:.2f}%  (<code>{abs_diff:.4f} {cur}</code>)</b>\n\n"
        f"💰 Profit: <code>{profit_value:.4f} %</code>\n\n"
        f"➤ {ZOOZ_HTML} ⭐️"
    )


def build_end_message(cur, pay_friendly, seller_ad, buyer_ad, spread_percent):
    flag = format_currency_flag(cur)
    abs_diff = abs(buyer_ad["price"] - seller_ad["price"])
    sign = "+" if spread_percent > 0 else ""
    method_name = (seller_ad.get("payment_method") or buyer_ad.get("payment_method") or pay_friendly)
    hashtag = _make_hashtag(cur, method_name)
    hashtag_line = (hashtag) if hashtag else ""

    seller_price = seller_ad['price']
    buyer_price = buyer_ad['price']
    part1 = (100 - (100 * (1 / 69))) / seller_price
    part2 = 100 / buyer_price
    profit_value = part1 - part2
    sign = "+" if spread_percent >= 0 else "-"
    
    return (
        f"❌ Ended {flag} ★ {hashtag_line} ★\n\n"
        f"🔴 Sell: <code>{buyer_ad['price']:.4f} {cur}</code>\n"
        f"🟢 Buy: <code>{seller_ad['price']:.4f} {cur}</code>\n\n"
        f"❌ <b>Spread: {sign}{spread_percent:.2f}%  (<code>{abs_diff:.4f} {cur}</code>)</b>\n\n"
        f"💰 Profit: <code>{profit_value:.4f} %</code>\n\n"
        f"➤ {ZOOZ_HTML} ⭐️"
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
                "last_sent_time": None,
                "last_message_type": None,
                "last_sent_signature": None
            }
        return rec.copy()

def set_active_state_snapshot(pair_key, *, active=None, last_spread=None, last_buy_price=None, last_sell_price=None, mark_sent=False, last_sent_signature=None, last_message_type=None):
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
            "last_sent_time": None,
            "last_message_type": None,
            "last_sent_signature": None
        }
        if active is not None:
            rec["active"] = bool(active)
            rec["since"] = time.time() if active else None
        if last_spread is not None:
            try:
                rec["last_spread"] = float(last_spread)
            except Exception:
                rec["last_spread"] = last_spread
        if last_buy_price is not None:
            try:
                rec["last_buy_price"] = float(last_buy_price)
            except Exception:
                rec["last_buy_price"] = last_buy_price
        if last_sell_price is not None:
            try:
                rec["last_sell_price"] = float(last_sell_price)
            except Exception:
                rec["last_sell_price"] = last_sell_price
        if mark_sent:
            rec["last_sent_spread"] = float(last_spread) if last_spread is not None else rec.get("last_sent_spread")
            rec["last_sent_buy"] = float(last_buy_price) if last_buy_price is not None else rec.get("last_sent_buy")
            rec["last_sent_sell"] = float(last_sell_price) if last_sell_price is not None else rec.get("last_sent_sell")
            rec["last_sent_time"] = time.time()
            if last_message_type is not None:
                rec["last_message_type"] = last_message_type
            if last_sent_signature is not None:
                rec["last_sent_signature"] = last_sent_signature
        active_states[pair_key] = rec

# ---------------------- update logic ----------------------

def relative_change_percent(old, new):
    try:
        if old is None or old == 0:
            return float("inf")
        return abs((new - old) / old) * 100.0
    except Exception:
        return float("inf")

def compute_signature(spread, buy, sell, tol=ALERT_VALUE_TOLERANCE):
    if tol <= 0:
        tol = 1e-8
    try:
        s_bin = int(round(spread / tol))
    except Exception:
        s_bin = None
    try:
        b_bin = int(round(buy / tol))
    except Exception:
        b_bin = None
    try:
        sel_bin = int(round(sell / tol))
    except Exception:
        sel_bin = None
    return (s_bin, b_bin, sel_bin)

def should_send_update(pair_state, new_spread, new_buy, new_sell, signature=None):
    last_sent_spread = pair_state.get("last_sent_spread")
    last_sent_buy = pair_state.get("last_sent_buy")
    last_sent_sell = pair_state.get("last_sent_sell")

    if ALERT_DEDUP_MODE == 'exact' and signature is not None:
        last_sig = pair_state.get('last_sent_signature')
        if last_sig is not None and last_sig == signature:
            logging.debug(f"Dedup: signature match -> suppressing send (sig={signature})")
            return False

    if last_sent_spread is None and last_sent_buy is None and last_sent_sell is None:
        return True

    if ALERT_UPDATE_ON_ANY_CHANGE == "1":
        spread_changed = not values_close(last_sent_spread, new_spread)
        buy_changed = not values_close(last_sent_buy, new_buy)
        sell_changed = not values_close(last_sent_sell, new_sell)
        return spread_changed or buy_changed or sell_changed

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

# ---------------------- core processing (FIRST-ad logic + fast-probe) ----------------------
paytype_variants_map = {
    "SkrillMoneybookers": ["SkrillMoneybookers","Skrill","Skrill (Moneybookers)"],
    "NETELLER": ["NETELLER"],
    "AirTM": ["AirTM"],
    "DukascopyBank": ["DukascopyBank"],
}

def fast_probe_ads(currency, variant, min_threshold, max_threshold):
    """
    Cheap probe: fetch top (page=1, rows=FAST_PROBE_ROWS) for BUY and SELL.
    Now checks both min and max thresholds (max_threshold==0 means ignore max).
    """
    buy_items = fetch_page_raw(currency, variant, "BUY", 1, rows=FAST_PROBE_ROWS)
    sell_items = fetch_page_raw(currency, variant, "SELL", 1, rows=FAST_PROBE_ROWS)

    if not buy_items or not sell_items:
        return None, None

    b = buy_items[0]
    s = sell_items[0]
    adv_b = b.get("adv") or {}
    adv_s = s.get("adv") or {}
    buyer_price = safe_float(adv_b.get("price") or 0.0)
    buyer_min = safe_float(adv_b.get("minSingleTransAmount") or adv_b.get("minSingleTransAmountDisplay") or 0.0)
    buyer_max = safe_float(adv_b.get("dynamicMaxSingleTransAmount") or adv_b.get("maxSingleTransAmount") or 0.0)
    seller_price = safe_float(adv_s.get("price") or 0.0)
    seller_min = safe_float(adv_s.get("minSingleTransAmount") or adv_s.get("minSingleTransAmountDisplay") or 0.0)
    seller_max = safe_float(adv_s.get("dynamicMaxSingleTransAmount") or adv_s.get("maxSingleTransAmount") or 0.0)

    min_ok = (buyer_min <= min_threshold and seller_min <= min_threshold)
    # treat max_threshold==0 as "no max constraint"
    max_ok = (max_threshold == 0) or (buyer_max >= max_threshold and seller_max >= max_threshold)

    if min_ok and max_ok and seller_price > 0:
        spread_percent = ((buyer_price / seller_price) - 1.0) * 100.0
        if spread_percent >= PROFIT_THRESHOLD_PERCENT:
            buyer_ad = {"trade_type":"BUY","currency":currency,"payment_method":variant,"price":buyer_price,"min_limit":buyer_min,"max_limit":buyer_max,"advertiser":b.get("advertiser")}
            seller_ad = {"trade_type":"SELL","currency":currency,"payment_method":variant,"price":seller_price,"min_limit":seller_min,"max_limit":seller_max,"advertiser":s.get("advertiser")}
            return buyer_ad, seller_ad
    return None, None

def process_pair(currency, method, min_threshold, max_threshold):
    variants = paytype_variants_map.get(method, [method])
    for variant in variants:
        pair_key = f"{currency}|{variant}"
        lock = get_pair_lock(pair_key)
        with lock:
            buyer_ad = None
            seller_ad = None
            try:
                buyer_ad, seller_ad = fast_probe_ads(currency, variant, min_threshold, max_threshold)
            except Exception as e:
                logging.debug(f"fast_probe failed for {pair_key}: {e}")

            if not buyer_ad or not seller_ad:
                with ThreadPoolExecutor(max_workers=2) as ex:
                    fut_b = ex.submit(find_first_ad, currency, variant, "BUY", min_threshold, max_threshold)
                    fut_s = ex.submit(find_first_ad, currency, variant, "SELL", min_threshold, max_threshold)
                    try:
                        buyer_ad = fut_b.result()
                    except Exception as e:
                        logging.debug(f"buyer fetch error for {pair_key}: {e}")
                        buyer_ad = None
                    try:
                        seller_ad = fut_s.result()
                    except Exception as e:
                        logging.debug(f"seller fetch error for {pair_key}: {e}")
                        seller_ad = None

            logging.debug(f"[found] {pair_key} buyer_ad={buyer_ad} seller_ad={seller_ad}")

            if not buyer_ad or not seller_ad:
                logging.debug(f"{pair_key}: missing buyer or seller ad (buyer_found={bool(buyer_ad)} seller_found={bool(seller_ad)}).")
                continue

            try:
                sell_price = float(buyer_ad["price"])  # price from BUY page (what you can sell at)
                buy_price = float(seller_ad["price"])  # price from SELL page (what you can buy at)
                spread_percent = ((sell_price / buy_price) - 1.0) * 100.0
            except Exception as e:
                logging.warning(f"Spread calc error for {pair_key}: {e}")
                continue

            profit_thresh = get_profit_threshold(currency, variant)
            pay_friendly = friendly_pay_names.get(variant, variant)
            logging.info(f"{pair_key} sell_price(from BUY page)={sell_price:.4f} buy_price(from SELL page)={buy_price:.4f} spread={spread_percent:.2f}% profit_thr={profit_thresh} min_thr={min_threshold} max_thr={max_threshold} min_sell={buyer_ad.get('min_limit',0):.2f} min_buy={seller_ad.get('min_limit',0):.2f} max_sell={buyer_ad.get('max_limit',0):.2f} max_buy={seller_ad.get('max_limit',0):.2f}")

            state = get_active_state(pair_key)
            was_active = state["active"]
            current_sig = compute_signature(spread_percent, buy_price, sell_price)

            if spread_percent >= profit_thresh:
                if not was_active:
                    if not should_send_update(state, spread_percent, buy_price, sell_price, signature=current_sig):
                        logging.debug(f"{pair_key}: Start suppressed (duplicate values). Marking active without sending.")
                        set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                                  last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=False)
                    else:
                        if can_send_start(state):
                            msg = build_alert_message(currency, pay_friendly, seller_ad, buyer_ad, spread_percent)
                            sent = send_telegram_alert(msg)
                            if sent:
                                logging.info(f"Start alert sent for {pair_key} (spread {spread_percent:.2f}%)")
                                set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                                          last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=True, last_sent_signature=current_sig, last_message_type='start')
                            else:
                                logging.warning(f"Failed to send start alert for {pair_key}")
                        else:
                            logging.debug(f"Start suppressed by TTL for {pair_key}")
                            set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                                      last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=False)
                else:
                    if should_send_update(state, spread_percent, buy_price, sell_price, signature=current_sig):
                        msg = build_update_message(currency, pay_friendly, seller_ad, buyer_ad, spread_percent)
                        sent = send_telegram_alert(msg)
                        if sent:
                            logging.info(f"Update alert sent for {pair_key} (spread {spread_percent:.2f}%)")
                            set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                                      last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=True, last_sent_signature=current_sig, last_message_type='update')
                        else:
                            logging.warning(f"Failed to send update for {pair_key}")
                    else:
                        set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                                  last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=False)
            else:
                if was_active:
                    if should_send_update(state, spread_percent, buy_price, sell_price, signature=current_sig):
                        msg = build_end_message(currency, pay_friendly, seller_ad, buyer_ad, spread_percent)
                        sent = send_telegram_alert(msg)
                        if sent:
                            logging.info(f"End alert sent for {pair_key} (spread {spread_percent:.2f}%)")
                            set_active_state_snapshot(pair_key, active=False, last_spread=spread_percent,
                                                      last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=True, last_sent_signature=current_sig, last_message_type='end')
                        else:
                            logging.warning(f"Failed to send end alert for {pair_key}")
                    else:
                        logging.debug(f"{pair_key}: End suppressed (duplicate values). Marking inactive without sending.")
                        set_active_state_snapshot(pair_key, active=False, last_spread=spread_percent,
                                                  last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=False)
                else:
                    set_active_state_snapshot(pair_key, active=False, last_spread=spread_percent,
                                              last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=False)

        break  # only process first matching variant

# ---------------------- main loop ----------------------

def build_pairs_to_monitor():
    """
    Construct list of tuples: (currency, method, min_threshold, max_threshold)
    """
    local_pairs = []
    min_limit_thresholds = parse_thresholds(MIN_LIMIT_THRESHOLDS_ENV, currency_list, DEFAULT_MIN_LIMIT)
    max_limit_thresholds = parse_thresholds(MAX_LIMIT_THRESHOLDS_ENV, currency_list, DEFAULT_MAX_LIMIT)

    if PAIRS_ENV:
        # If you have a parse_pairs_env implementation, adapt it to include both thresholds.
        # For backward compatibility here we'll assume parse_pairs_env returns (cur,method, minthr) as before.
        local_pairs = parse_pairs_env(PAIRS_ENV, min_limit_thresholds, payment_methods_map)
        # convert to include max thresholds
        full_pairs = []
        for cur, m, minthr in local_pairs:
            maxthr = max_limit_thresholds.get(cur, DEFAULT_MAX_LIMIT)
            full_pairs.append((cur, m, minthr, maxthr))
        local_pairs = full_pairs
    else:
        if SELECTED_CURRENCY == "ALL":
            for cur in currency_list:
                methods = payment_methods_map.get(cur, [])
                for m in methods:
                    minthr = min_limit_thresholds.get(cur, DEFAULT_MIN_LIMIT)
                    maxthr = max_limit_thresholds.get(cur, DEFAULT_MAX_LIMIT)
                    local_pairs.append((cur, m, minthr, maxthr))
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
                minthr = min_limit_thresholds.get(cur, DEFAULT_MIN_LIMIT)
                maxthr = max_limit_thresholds.get(cur, DEFAULT_MAX_LIMIT)
                local_pairs.append((cur, m, minthr, maxthr))

    # apply whitelist/exclude filters
    filtered = []
    for cur, m, minthr, maxthr in local_pairs:
        if not method_allowed(m):
            logging.debug(f"Filtered out {cur}|{m} by PAYMENT_METHODS/EXCLUDE settings")
            continue
        filtered.append((cur, m, minthr, maxthr))
    if not filtered:
        logging.error("No currency/payment pairs selected after applying PAYMENT_METHODS filter. Exiting.")
        raise SystemExit(1)
    return filtered

pairs_to_monitor = build_pairs_to_monitor()

def run_monitor_loop():
    logging.info(f"Monitoring {len(pairs_to_monitor)} pairs. Every {REFRESH_EVERY}s. Workers={MAX_CONCURRENT_WORKERS} RPM={REQUESTS_PER_MINUTE}")
    try:
        while True:
            start_ts = time.time()

            futures = []
            with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_WORKERS) as ex:
                for cur, m, minthr, maxthr in pairs_to_monitor:
                    futures.append(ex.submit(process_pair, cur, m, minthr, maxthr))
                    time.sleep(SLEEP_BETWEEN_PAIRS)

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
