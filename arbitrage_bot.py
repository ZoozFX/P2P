#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Improved / fixed ZoozFX P2P monitor
 - Fixes duplicate "Alert -> Update -> Alert" behaviour by adding stronger dedup / message-type tracking
 - Adds a signature-based deduplication (uses ALERT_VALUE_TOLERANCE)
 - Stores last_message_type and last_sent_signature in state so we never re-send a "Start" while the pair is still active
 - Keeps original behaviour otherwise; minimal invasive changes

Additional features added:
 - Startup cleanup: before the first alert, attempt to delete previous messages (via getUpdates) except the
   first photo message found from the user; use that photo's file_id to populate TELEGRAM_IMAGE_FILE_ID.
 - Add hashtag at end of every telegram message: #<CURRENCY>_<PAYMETHOD> (sanitized)
 - Per-pair profit threshold via PROFIT_THRESHOLDS env var
 - Global filter for enabled payment methods via ENABLED_PAY_METHODS env var
"""

import os
import time
import logging
import random
import math
import requests
import threading
import re
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

DEFAULT_MIN_LIMIT = float(os.getenv("DEFAULT_MIN_LIMIT", "100"))
MIN_LIMIT_THRESHOLDS_ENV = os.getenv("MIN_LIMIT_THRESHOLDS", "").strip()
PAIRS_ENV = os.getenv("PAIRS", "").strip()
SELECTED_CURRENCY = os.getenv("SELECTED_CURRENCY", "ALL").strip().upper()
SELECTED_METHOD = os.getenv("SELECTED_METHOD", "ALL").strip()

# New envs:
PROFIT_THRESHOLDS_ENV = os.getenv("PROFIT_THRESHOLDS", "").strip()  # e.g. "GBP:Skrill=2;MAD=5;USD=ALL=3"
ENABLED_PAY_METHODS_ENV = os.getenv("ENABLED_PAY_METHODS", "").strip()  # e.g. "Skrill,NETELLER"

# ---------------------- static lists ----------------------
currency_list = ["USD", "CAD", "NZD", "AUD", "GBP", "JPY", "EUR", "EGP", "MAD", "SAR", "AED", "KWD", "DZD"]

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


# ---------------------- profit thresholds parsing ----------------------
def parse_profit_thresholds(env_str):
    """
    Parse strings like:
      "GBP:Skrill=2;MAD=5;USD:ALL=3"
    Returns dict with keys (CUR, METHOD_UPPER) where METHOD_UPPER may be 'ALL'.
    """
    out = {}
    if not env_str:
        return out
    parts = [p.strip() for p in re.split(r"[,;]", env_str) if p.strip()]
    for p in parts:
        if "=" not in p:
            continue
        lhs, rhs = p.split("=", 1)
        lhs = lhs.strip()
        try:
            val = float(rhs.strip())
        except Exception:
            logging.warning(f"Invalid profit threshold value: {rhs} in entry {p}")
            continue
        if ":" in lhs:
            cur, method = lhs.split(":", 1)
            cur = cur.strip().upper()
            method = method.strip().upper()
            out[(cur, method)] = val
        else:
            cur = lhs.strip().upper()
            out[(cur, "ALL")] = val
    return out


def get_profit_threshold_for_pair(cur, method, profit_map):
    # lookup order: (CUR,METHOD) -> (CUR,ALL) -> default global PROFIT_THRESHOLD_PERCENT
    if not cur:
        return PROFIT_THRESHOLD_PERCENT
    key1 = (cur.upper(), method.upper())
    if key1 in profit_map:
        return profit_map[key1]
    key2 = (cur.upper(), "ALL")
    if key2 in profit_map:
        return profit_map[key2]
    return PROFIT_THRESHOLD_PERCENT


# ---------------------- build pairs to monitor ----------------------
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
            sel_method = SELECTED_METHOD
            # allow friendly match
            if sel_method not in methods and all(friendly_pay_names.get(m, m) != sel_method for m in methods):
                logging.error(f"Method {SELECTED_METHOD} not valid for {cur}. Exiting.")
                raise SystemExit(1)
            # filter for the actual method(s) matching SELECTED_METHOD
            methods = [m for m in methods if m == sel_method or friendly_pay_names.get(m) == sel_method]
        for m in methods:
            pairs_to_monitor.append((cur, m, min_limit_thresholds.get(cur, float("inf"))))

# Apply ENABLED_PAY_METHODS filter if provided
ENABLED_PAY_METHODS = set([s.strip() for s in re.split(r"[,;]", ENABLED_PAY_METHODS_ENV) if s.strip()])
if ENABLED_PAY_METHODS:
    allowed_upper = {s.upper() for s in ENABLED_PAY_METHODS}
    def method_allowed(m):
        return (m.upper() in allowed_upper) or (friendly_pay_names.get(m, m).upper() in allowed_upper)
    before = len(pairs_to_monitor)
    pairs_to_monitor = [p for p in pairs_to_monitor if method_allowed(p[1])]
    logging.info(f"ENABLED_PAY_METHODS filter applied: kept {len(pairs_to_monitor)}/{before} pairs")

if not pairs_to_monitor:
    logging.error("No currency/payment pairs selected. Exiting.")
    raise SystemExit(1)

# parse profit thresholds map
profit_thresholds_map = parse_profit_thresholds(PROFIT_THRESHOLDS_ENV)

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
    Token bucket: refill tokens proportional to elapsed seconds (fractional allowed).
    Blocks until at least 1 token is available, then consumes and returns.
    """
    while True:
        with token_lock:
            now = time.time()
            elapsed = now - token_bucket["last_refill"]
            if elapsed > 0:
                # refill proportionally (allow fractional refill so tokens accumulate smoothly)
                refill = (elapsed / 60.0) * REQUESTS_PER_MINUTE
                token_bucket["tokens"] = min(float(REQUESTS_PER_MINUTE), token_bucket["tokens"] + refill)
                token_bucket["last_refill"] = now

            if token_bucket["tokens"] >= 1.0:
                token_bucket["tokens"] -= 1.0
                return True

            # compute time until next token (seconds per token)
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
        # acquire global token and wait minimum interval
        acquire_token_blocking()
        rate_limit_wait()
        try:
            r = session.post(BINANCE_P2P_URL, json=payload, headers=HEADERS, timeout=TIMEOUT)
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


def find_first_ad(fiat, pay_type, trade_type, page_limit_threshold, rows=ROWS_PER_REQUEST):
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
            logging.debug(f"[first-search] {fiat}/{pay_type}/{trade_type} p{page} price={price} min={min_lim} max={max_lim} adv_by={nick} thr={page_limit_threshold}")

            if min_lim <= page_limit_threshold:
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


def _try_send_photo(payload_data, files=None):
    """Attempt a single sendPhoto request. Return tuple(ok_bool, response_json_or_text, status_code)
    """
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

# startup cleanup flag and lock
did_startup_cleanup = False
startup_cleanup_lock = threading.Lock()

def startup_cleanup_preserve_first_photo():
    """
    Attempt to fetch pending updates, find the first photo message from the target chat and preserve it,
    delete other messages returned by getUpdates for that chat (best-effort). Also update TELEGRAM_IMAGE_FILE_ID
    if not set.
    This runs once (protected by startup_cleanup_lock) before the first alert send.
    """
    global did_startup_cleanup, TELEGRAM_IMAGE_FILE_ID
    with startup_cleanup_lock:
        if did_startup_cleanup:
            return
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            logging.info("Telegram config missing; skipping startup cleanup")
            did_startup_cleanup = True
            return

        try:
            chat_id_int = int(TELEGRAM_CHAT_ID)
        except Exception:
            logging.warning("Invalid TELEGRAM_CHAT_ID; skipping startup cleanup")
            did_startup_cleanup = True
            return

        updates = []
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            r = session.get(url, params={"limit": 100, "timeout": 0}, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json() or {}
            updates = data.get("result", []) or []
        except Exception as e:
            logging.warning(f"startup cleanup getUpdates failed: {e}")
            updates = []

        if not updates:
            logging.info("startup cleanup: no updates returned by getUpdates.")
            did_startup_cleanup = True
            return

        # find earliest photo message in this chat
        updates_sorted = sorted(updates, key=lambda u: u.get("update_id", 0))
        preserved_message_id = None
        preserved_file_id = None
        for u in updates_sorted:
            msg = u.get("message") or u.get("edited_message")
            if not msg:
                continue
            chat = msg.get("chat", {})
            if chat.get("id") != chat_id_int:
                continue
            # look for photo
            if "photo" in msg and msg.get("photo"):
                photos = msg.get("photo")
                # keep largest
                photo_obj = photos[-1]
                preserved_file_id = photo_obj.get("file_id")
                preserved_message_id = msg.get("message_id")
                break

        if preserved_message_id:
            # update TELEGRAM_IMAGE_FILE_ID if empty
            if not TELEGRAM_IMAGE_FILE_ID and preserved_file_id:
                TELEGRAM_IMAGE_FILE_ID = preserved_file_id
                logging.info(f"Discovered TELEGRAM_IMAGE_FILE_ID from preserved photo: {TELEGRAM_IMAGE_FILE_ID}")

            # delete everything else (best-effort)
            for u in updates_sorted:
                msg = u.get("message") or u.get("edited_message")
                if not msg:
                    continue
                chat = msg.get("chat", {})
                if chat.get("id") != chat_id_int:
                    continue
                mid = msg.get("message_id")
                if mid == preserved_message_id:
                    continue
                try:
                    del_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteMessage"
                    resp = session.post(del_url, json={"chat_id": chat_id_int, "message_id": mid}, timeout=TIMEOUT)
                    if resp.ok:
                        logging.info(f"Deleted message {mid} in chat {chat_id_int}")
                    else:
                        logging.debug(f"deleteMessage failed for {mid}: status={resp.status_code} resp={getattr(resp,'text',None)}")
                except Exception as e:
                    logging.debug(f"deleteMessage exception for {mid}: {e}")
                time.sleep(0.05)

            # clear updates offset so getUpdates won't return again
            try:
                last_update_id = updates_sorted[-1].get("update_id", 0)
                session.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates", params={"offset": last_update_id + 1}, timeout=TIMEOUT)
            except Exception:
                pass
        else:
            logging.info("No photo message found to preserve during startup cleanup; nothing deleted.")
        did_startup_cleanup = True


def send_telegram_alert(message):
    # run startup cleanup once before first send
    try:
        if not did_startup_cleanup:
            startup_cleanup_preserve_first_photo()
    except Exception as e:
        logging.debug(f"startup cleanup error (ignored): {e}")

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.info("Telegram token/chat not set; skipping send. Message preview:\n" + message)
        return False

    # Try file_id first with small retry loop to avoid immediate fallback to text when transient network errors happen
    max_photo_attempts = 3

    if TELEGRAM_IMAGE_FILE_ID:
        payload = {"chat_id": TELEGRAM_CHAT_ID, "photo": TELEGRAM_IMAGE_FILE_ID, "caption": (message if len(message) <= 1024 else (message[:1020] + "...")), "parse_mode": "HTML"}
        for attempt in range(1, max_photo_attempts + 1):
            ok, jr_or_text, status = _try_send_photo(payload)
            if ok:
                logging.info("Telegram photo alert sent (file_id).")
                return True
            logging.warning(f"sendPhoto(file_id) attempt {attempt}/{max_photo_attempts} failed status={status} resp={jr_or_text}")
            # small backoff before retrying the photo
            time.sleep(0.5 * attempt + random.uniform(0, 0.3))

    # Then try URL with small retries
    if TELEGRAM_IMAGE_URL:
        payload = {"chat_id": TELEGRAM_CHAT_ID, "photo": TELEGRAM_IMAGE_URL, "caption": (message if len(message) <= 1024 else (message[:1020] + "...")), "parse_mode": "HTML"}
        for attempt in range(1, max_photo_attempts + 1):
            ok, jr_or_text, status = _try_send_photo(payload)
            if ok:
                logging.info("Telegram photo alert sent (via URL).")
                return True
            logging.warning(f"sendPhoto(via URL) attempt {attempt}/{max_photo_attempts} failed status={status} resp={jr_or_text}")
            time.sleep(0.5 * attempt + random.uniform(0, 0.3))

        # upload fallback (download then upload) with a couple of retries
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

    # Text fallback (last resort)
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

def build_hashtag(cur, pay_friendly):
    # sanitize pay_friendly to alnum (remove spaces/punctuation), uppercase/lowercase kept as-is
    token = re.sub(r'[^A-Za-z0-9]', '', str(pay_friendly))
    return f"#{cur}_{token}" if token else f"#{cur}"

def build_alert_message(cur, pay_friendly, seller_ad, buyer_ad, spread_percent):
    flag = format_currency_flag(cur)
    abs_diff = abs(buyer_ad["price"] - seller_ad["price"])
    sign = "+" if spread_percent > 0 else ""
    hashtag = build_hashtag(cur, pay_friendly)
    return (
        f"🚨 Alert {flag} — #{cur} ({pay_friendly})\n\n"
        f"🔴 Sell: <code>{buyer_ad['price']:.4f} {cur}</code>\n"
        f"🟢 Buy: <code>{seller_ad['price']:.4f} {cur}</code>\n\n"
        f"💰 Spread: {sign}{spread_percent:.2f}%  (<code>{abs_diff:.4f} {cur}</code>)\n\n"
        f"💥 Good Luck! {ZOOZ_HTML}\n\n"
        f"{hashtag}"
    )

def build_update_message(cur, pay_friendly, seller_ad, buyer_ad, spread_percent):
    flag = format_currency_flag(cur)
    abs_diff = abs(buyer_ad["price"] - seller_ad["price"])
    sign = "+" if spread_percent > 0 else ""
    hashtag = build_hashtag(cur, pay_friendly)
    return (
        f"🔁 Update {flag} — #{cur} ({pay_friendly})\n\n"
        f"🔴 Sell: <code>{buyer_ad['price']:.4f} {cur}</code>\n"
        f"🟢 Buy: <code>{seller_ad['price']:.4f} {cur}</code>\n\n"
        f"💰 Spread: {sign}{spread_percent:.2f}%  (<code>{abs_diff:.4f} {cur}</code>)\n\n"
        f"💥 Good Luck! {ZOOZ_HTML}\n\n"
        f"{hashtag}"
    )

def build_end_message(cur, pay_friendly, seller_ad, buyer_ad, spread_percent):
    flag = format_currency_flag(cur)
    abs_diff = abs(buyer_ad["price"] - seller_ad["price"])
    sign = "+" if spread_percent > 0 else ""
    hashtag = build_hashtag(cur, pay_friendly)
    return (
        f"❌ Ended {flag} — #{cur} ({pay_friendly})\n\n"
        f"🔴 Sell: <code>{buyer_ad['price']:.4f} {cur}</code>\n"
        f"🟢 Buy: <code>{seller_ad['price']:.4f} {cur}</code>\n\n"
        f"💰 Spread: {sign}{spread_percent:.2f}%  (<code>{abs_diff:.4f} {cur}</code>)\n\n"
        f"💥 Good Luck! {ZOOZ_HTML}\n\n"
        f"{hashtag}"
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
                # new fields for robust dedup / state tracking
                "last_message_type": None,  # 'start','update','end'
                "last_sent_signature": None
            }
        # return a shallow copy so caller can't mutate shared dict
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
            # update last_sent_* values and time
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
    """Compute a lightweight signature (tuple of rounded bins) used for exact de-dup.
    signature bins values by tolerance which avoids minor float noise causing new messages.
    """
    if tol <= 0:
        # fallback to a reasonably small tolerance
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
    """
    Decide whether an update (or start) message should be sent.
    Uses signature-based de-duplication + previous thresholds.
    """
    last_sent_spread = pair_state.get("last_sent_spread")
    last_sent_buy = pair_state.get("last_sent_buy")
    last_sent_sell = pair_state.get("last_sent_sell")

    # If signature provided and dedup mode is exact, do a quick signature check
    if ALERT_DEDUP_MODE == 'exact' and signature is not None:
        last_sig = pair_state.get('last_sent_signature')
        if last_sig is not None and last_sig == signature:
            logging.debug(f"Dedup: signature match -> suppressing send (sig={signature})")
            return False

    # initial send when nothing has been sent yet
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


def fast_probe_ads(currency, variant, threshold):
    """
    Cheap probe: fetch top (page=1, rows=FAST_PROBE_ROWS) for BUY and SELL.
    If both top ads satisfy min_limit <= threshold and the spread looks good, return them.
    Otherwise return (None, None) to fall back to full search.
    """
    # perform BUY and SELL fetches (we do them sequentially - they are cheap because rows=1)
    buy_items = fetch_page_raw(currency, variant, "BUY", 1, rows=FAST_PROBE_ROWS)
    sell_items = fetch_page_raw(currency, variant, "SELL", 1, rows=FAST_PROBE_ROWS)

    if not buy_items or not sell_items:
        return None, None

    # extract first adv from each
    b = buy_items[0]
    s = sell_items[0]
    adv_b = b.get("adv") or {}
    adv_s = s.get("adv") or {}
    buyer_price = safe_float(adv_b.get("price") or 0.0)
    buyer_min = safe_float(adv_b.get("minSingleTransAmount") or adv_b.get("minSingleTransAmountDisplay") or 0.0)
    seller_price = safe_float(adv_s.get("price") or 0.0)
    seller_min = safe_float(adv_s.get("minSingleTransAmount") or adv_s.get("minSingleTransAmountDisplay") or 0.0)

    # if min limits fit threshold on both sides -> compute spread
    if buyer_min <= threshold and seller_min <= threshold and seller_price > 0:
        spread_percent = ((buyer_price / seller_price) - 1.0) * 100.0
        if spread_percent >= PROFIT_THRESHOLD_PERCENT:
            buyer_ad = {"trade_type":"BUY","currency":currency,"payment_method":variant,"price":buyer_price,"min_limit":buyer_min,"advertiser":b.get("advertiser")}
            seller_ad = {"trade_type":"SELL","currency":currency,"payment_method":variant,"price":seller_price,"min_limit":seller_min,"advertiser":s.get("advertiser")}
            return buyer_ad, seller_ad
    return None, None


def process_pair(currency, method, threshold):
    variants = paytype_variants_map.get(method, [method])
    for variant in variants:
        pair_key = f"{currency}|{variant}"
        lock = get_pair_lock(pair_key)
        with lock:
            # fast probe: quick low-cost check of top results
            buyer_ad = None
            seller_ad = None
            try:
                buyer_ad, seller_ad = fast_probe_ads(currency, variant, threshold)
            except Exception as e:
                logging.debug(f"fast_probe failed for {pair_key}: {e}")

            # if fast_probe didn't find a ready opportunity, fall back to full (first-ad) scans
            if not buyer_ad or not seller_ad:
                # do BUY and SELL scans in parallel to save wall time (subject to token bucket)
                with ThreadPoolExecutor(max_workers=2) as ex:
                    fut_b = ex.submit(find_first_ad, currency, variant, "BUY", threshold)
                    fut_s = ex.submit(find_first_ad, currency, variant, "SELL", threshold)
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
                # careful: buyer_ad is from BUY page (what you can sell at)
                sell_price = float(buyer_ad["price"])  # price from BUY page (what you can sell at)
                buy_price = float(seller_ad["price"])  # price from SELL page (what you can buy at)
                spread_percent = ((sell_price / buy_price) - 1.0) * 100.0
            except Exception as e:
                logging.warning(f"Spread calc error for {pair_key}: {e}")
                continue

            pay_friendly = friendly_pay_names.get(variant, variant)
            # determine profit threshold for this pair (currency + method variant)
            profit_thr = get_profit_threshold_for_pair(currency, pay_friendly, profit_thresholds_map)
            logging.info(f"{pair_key} sell_price(from BUY page)={sell_price:.4f} buy_price(from SELL page)={buy_price:.4f} spread={spread_percent:.2f}% thr={profit_thr} min_sell={buyer_ad['min_limit']:.2f} min_buy={seller_ad['min_limit']:.2f}")

            state = get_active_state(pair_key)
            was_active = state["active"]

            # compute signature for dedup
            current_sig = compute_signature(spread_percent, buy_price, sell_price)

            # decide start / update / end (send only one message per cycle)
            if spread_percent >= profit_thr:
                # Opportunity exists
                if not was_active:
                    # We are not currently active -> candidate for START
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
                    # already active -> candidate for UPDATE
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
                        # update suppressed but keep active state and last values
                        set_active_state_snapshot(pair_key, active=True, last_spread=spread_percent,
                                                  last_buy_price=buy_price, last_sell_price=sell_price, mark_sent=False)
            else:
                # Not an opportunity: ensure we transition to inactive (END) when appropriate
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

        # processed variant -> break
        break

# ---------------------- main loop ----------------------

def run_monitor_loop():
    logging.info(f"Monitoring {len(pairs_to_monitor)} pairs. Every {REFRESH_EVERY}s. Default Profit threshold={PROFIT_THRESHOLD_PERCENT}%. Workers={MAX_CONCURRENT_WORKERS} RPM={REQUESTS_PER_MINUTE}")
    try:
        while True:
            start_ts = time.time()

            futures = []
            with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_WORKERS) as ex:
                for cur, m, thr in pairs_to_monitor:
                    futures.append(ex.submit(process_pair, cur, m, thr))
                    # small optional stagger — if you have token bucket this is less critical
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
