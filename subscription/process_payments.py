#!/usr/bin/env python3
"""
Quantex Subscription Manager — GitHub Actions Edition
No server needed. Runs entirely via GitHub Actions cron.

Payment flow: user picks plan → bot generates UPI QR/deep-link → user pays
and sends a screenshot → admin taps Approve in Telegram → bot activates the
subscriber, records the payment, DMs the group invite link.

This script:
1. Polls Telegram for messages and inline-button taps
2. Activates subscriptions in subscribers.json on admin approval
3. Sends Telegram group invite links to new subscribers
4. Sends WhatsApp community invite link
5. Sweeps expired subscribers and removes them from the group
6. Commits updated JSON back to the repo

Environment Variables (set as GitHub Secrets):
  TELEGRAM_BOT_TOKEN     — Telegram bot token
  TELEGRAM_GROUP_ID      — Subscriber group chat ID
  TELEGRAM_ADMIN_CHAT_ID — Admin notification chat ID
  WHATSAPP_INVITE_LINK   — WhatsApp community invite link
  UPI_ID                 — Receiving UPI VPA (required for /subscribe)
  UPI_NAME               — Optional display name on the UPI deep-link
"""

import os
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
import requests

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).parent
SUBSCRIBERS_FILE = BASE_DIR / "subscribers.json"
PAYMENTS_FILE = BASE_DIR / "payments.json"

# Telegram  (.strip() to remove accidental whitespace from GitHub Secrets)
# Also strip stray quotes — a common foot-gun when secrets are pasted with
# surrounding "" or '' characters, which previously caused /approve to silently
# fail the admin check because '"123"' != '123'.
def _clean_secret(value: str) -> str:
    v = value.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
        v = v[1:-1].strip()
    return v

TELEGRAM_BOT_TOKEN = _clean_secret(os.environ.get("TELEGRAM_BOT_TOKEN", ""))
TELEGRAM_GROUP_ID = _clean_secret(os.environ.get("TELEGRAM_GROUP_ID", ""))
TELEGRAM_ADMIN_CHAT_ID = _clean_secret(os.environ.get("TELEGRAM_ADMIN_CHAT_ID", ""))


def is_admin(chat_id, user_id) -> bool:
    """Admin gate: match either the chat_id (private DM) or the sender's user_id
    against TELEGRAM_ADMIN_CHAT_ID. Previously only chat_id was checked, which
    silently no-op'd /approve whenever the env var was unset/quoted/mismatched."""
    if not TELEGRAM_ADMIN_CHAT_ID:
        return False
    return str(chat_id) == TELEGRAM_ADMIN_CHAT_ID or str(user_id) == TELEGRAM_ADMIN_CHAT_ID

# WhatsApp
WHATSAPP_INVITE_LINK = os.environ.get("WHATSAPP_INVITE_LINK", "").strip()

# Plans — single source of truth for pricing. Both the Telegram menu text
# and the "1"/"2"/"3" reply mapping are derived from this dict, so prices
# can't drift between display and what we actually charge.
# `months` is the period denominator for the per-month display in the menu.
PLANS = {
    99:  {"name": "monthly",   "days": 30,  "months": 1,  "label": "Monthly",   "menu_key": "1"},
    199: {"name": "quarterly", "days": 90,  "months": 3,  "label": "Quarterly", "menu_key": "2"},
    499: {"name": "yearly",    "days": 365, "months": 12, "label": "Yearly",    "menu_key": "3"},
}

# Stable iteration order for display + menu mapping.
PLAN_ORDER = [99, 199, 499]

# All plans deliver the same feature set — they only differ in duration
# (and therefore per-month effective price). Rendered once at the top of
# the plan menu so the user sees the value prop before the price list.
# Mirror of the bullets in website/index.html — keep these in sync if you
# rephrase a feature.
PLAN_FEATURES = [
    "📄 Daily pre-market PDF report",
    "💬 Telegram group access",
    "🔍 500+ stocks scanned daily",
    "📊 15+ sectoral analysis",
    "📈 Weekly market outlook",
    "📅 Earnings calendar updates",
    "🔄 Sector rotation signals",
    "💎 Exclusive stock ideas",
    "🌅 End-of-day quick scan",
    "🛟 Direct admin support",
]


def subscription_plans_text():
    """Render the Telegram subscription menu from PLANS so display & charge can't drift."""
    lines = ["📊 <b>Quantex Subscription Plans</b>"]
    # All plans get the same features — show the list once at the top so
    # the plan cards below stay tight (just price + per-month equivalent).
    lines.append("\n<b>What you get with every plan:</b>")
    for feature in PLAN_FEATURES:
        lines.append(f"   {feature}")
    lines.append("\n<b>Choose a duration:</b>")
    emojis = {"1": "1️⃣", "2": "2️⃣", "3": "3️⃣"}
    fire = {"yearly": " 🔥"}
    for amount in PLAN_ORDER:
        p = PLANS[amount]
        period = {1: "month", 3: "quarter", 12: "year"}.get(p["months"], "period")
        per_mo = round(amount / p["months"]) if p["months"] > 1 else None
        suffix = f" (₹{per_mo}/mo)" if per_mo is not None else ""
        lines.append(
            f"{emojis[p['menu_key']]} <b>{p['label']}</b> — ₹{amount}/{period}{suffix}{fire.get(p['name'], '')}"
        )
    lines.append("\nReply with the plan number:")
    for amount in PLAN_ORDER:
        p = PLANS[amount]
        lines.append(f"<b>{p['menu_key']}</b> for {p['label']}")
    return "\n".join(lines)

# {"1": 99, "2": 199, "3": 499} — derived from PLANS, not hardcoded.
PLAN_MENU_MAP = {p["menu_key"]: amount for amount, p in PLANS.items()}

TRIAL_DAYS = 3

# UPI Payment Config — UPI_ID is required, UPI_NAME is optional display hint.
# No hardcoded fallbacks: a misconfigured secret should fail loudly, not silently
# route to someone else's VPA.
UPI_ID = os.environ.get("UPI_ID", "").strip()
UPI_NAME = os.environ.get("UPI_NAME", "").strip()


# ═══════════════════════════════════════════════════════════════
# DATA HELPERS
# ═══════════════════════════════════════════════════════════════

PENDING_ORDERS_FILE = BASE_DIR / "pending_orders.json"

def load_json(path):
    if path.exists():
        return json.loads(path.read_text())
    return []

def save_json(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False))

def generate_order_id():
    """Generate unique short order ID like QTX-A3K7."""
    import random, string
    chars = string.ascii_uppercase + string.digits
    code = ''.join(random.choices(chars, k=4))
    return f"QTX-{code}"

def build_upi_link(upi_id, name, amount, order_id):
    """Build a properly URL-encoded upi:// deep-link.
    `name` is optional — omitted from the link if empty/None."""
    from urllib.parse import quote

    if not upi_id:
        raise ValueError("UPI_ID is not configured (set the UPI_ID GitHub secret)")

    parts = [f"pa={quote(str(upi_id), safe='')}"]
    if name:
        parts.append(f"pn={quote(str(name), safe='')}")
    parts.append(f"am={quote(str(amount), safe='')}")
    parts.append("cu=INR")
    parts.append(f"tn={quote(str(order_id), safe='')}")
    return "upi://pay?" + "&".join(parts)


# Public HTTPS redirect page that hands off to upi://. Telegram on mobile
# refuses to open bare upi:// links from message text reliably, but it
# opens https:// fine — and the page does the upi:// hand-off in the
# browser, which the OS dispatches to GPay/PhonePe/etc.
# Override via PAY_REDIRECT_URL env if you ever move the page elsewhere.
PAY_REDIRECT_URL = os.environ.get(
    "PAY_REDIRECT_URL", "https://quantex.cibronix.com/pay.html"
).strip()


def build_upi_https_link(upi_id, name, amount, order_id):
    """Build the HTTPS redirect URL that lands on pay.html and forwards to upi://.
    Uses the same query params as the upi:// scheme so pay.html can rebuild
    the deep-link cleanly."""
    from urllib.parse import quote
    if not upi_id:
        raise ValueError("UPI_ID is not configured (set the UPI_ID GitHub secret)")
    parts = [f"pa={quote(str(upi_id), safe='')}"]
    if name:
        parts.append(f"pn={quote(str(name), safe='')}")
    parts.append(f"am={quote(str(amount), safe='')}")
    parts.append("cu=INR")
    parts.append(f"tn={quote(str(order_id), safe='')}")
    return f"{PAY_REDIRECT_URL}?" + "&".join(parts)

def generate_upi_qr(upi_id, name, amount, order_id):
    """Generate UPI QR code image and return the file path."""
    try:
        import qrcode

        upi_string = build_upi_link(upi_id, name, amount, order_id)

        qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_H, box_size=10, border=4)
        qr.add_data(upi_string)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")

        qr_path = BASE_DIR / f"qr_{order_id}.png"
        img.save(str(qr_path))
        return str(qr_path)
    except Exception as e:
        # Surface the full traceback — silent failures here cost us hours.
        import traceback as _tb
        print(f"   QR generation error: {type(e).__name__}: {e}")
        _tb.print_exc()
        return None

def send_photo(chat_id, photo_path, caption=""):
    """Send a photo to a Telegram chat."""
    if not TELEGRAM_BOT_TOKEN:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
        with open(photo_path, "rb") as f:
            resp = requests.post(url, data={
                "chat_id": chat_id,
                "caption": caption,
                "parse_mode": "HTML",
            }, files={"photo": f}, timeout=30)
        return resp.status_code == 200
    except Exception as e:
        print(f"   Send photo error: {e}")
        return False

def load_pending_orders():
    """Load pending UPI payment orders."""
    return load_json(PENDING_ORDERS_FILE)

def save_pending_orders(orders):
    """Save pending UPI payment orders."""
    save_json(PENDING_ORDERS_FILE, orders)


# ═══════════════════════════════════════════════════════════════
# TELEGRAM HELPERS
# ═══════════════════════════════════════════════════════════════

def telegram_api(method, data=None):
    if not TELEGRAM_BOT_TOKEN:
        print("   !! TELEGRAM_BOT_TOKEN is empty")
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    try:
        resp = requests.post(url, json=data, timeout=30)
        result = resp.json()
        if not result.get("ok"):
            # Debug: print token length to help diagnose secret issues
            print(f"   Telegram error: {result.get('description', '')} "
                  f"(token length={len(TELEGRAM_BOT_TOKEN)}, "
                  f"starts_with={TELEGRAM_BOT_TOKEN[:4]}..., "
                  f"method={method})")
        return result
    except Exception as e:
        print(f"   Telegram exception: {e}")
        return None

def send_message(chat_id, text, reply_markup=None):
    """Send a Telegram message. `reply_markup` is an optional dict for inline
    keyboards (e.g. {"inline_keyboard": [[{"text": ..., "callback_data": ...}]]})."""
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return telegram_api("sendMessage", payload)


def answer_callback(callback_query_id, text="", alert=False):
    """Acknowledge a callback_query so Telegram clears the loading spinner."""
    return telegram_api("answerCallbackQuery", {
        "callback_query_id": callback_query_id,
        "text": text,
        "show_alert": alert,
    })


def edit_message_reply_markup(chat_id, message_id, reply_markup=None):
    """Strip or replace inline buttons on a previously-sent message."""
    payload = {"chat_id": chat_id, "message_id": message_id}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return telegram_api("editMessageReplyMarkup", payload)

def create_invite_link(expire_hours=72):
    """Create a single-use Telegram group invite link.

    Uses time.time() instead of datetime.utcnow().timestamp() — the latter
    treats a naive UTC datetime as local time and produces a wrong Unix
    timestamp on any non-UTC host, which can result in expire_date in the
    past (link born already-expired). time.time() is timezone-immune.
    """
    if not TELEGRAM_GROUP_ID:
        print("   create_invite_link: TELEGRAM_GROUP_ID is empty")
        return None
    import time as _time
    expire_date = int(_time.time()) + int(expire_hours * 3600)
    result = telegram_api("createChatInviteLink", {
        "chat_id": TELEGRAM_GROUP_ID,
        "member_limit": 1,
        "expire_date": expire_date,
    })
    if result and result.get("ok"):
        link = result["result"]["invite_link"]
        # Log every successful creation — we want a paper trail when users
        # report "expired on first tap" so we can correlate.
        print(f"   create_invite_link OK: expire_date={expire_date} link={link}")
        return link
    err = (result or {}).get("description", "no response")
    print(f"   create_invite_link FAILED: {err}")
    return None

def unban_member(user_id):
    """Unban a user (idempotent — Telegram noops if they aren't banned).

    Critical to call BEFORE issuing a fresh invite link to a returning
    customer: Telegram displays "this invite link has expired" to banned
    users even on brand-new links, which makes the link look broken when
    the real issue is the residual ban from a previous expiry sweep.
    """
    if not TELEGRAM_GROUP_ID or not user_id:
        return False
    try:
        result = telegram_api("unbanChatMember", {
            "chat_id": TELEGRAM_GROUP_ID,
            "user_id": int(user_id),
            "only_if_banned": True,
        })
        return bool(result and result.get("ok"))
    except (ValueError, TypeError) as e:
        print(f"   unban_member: bad user_id {user_id!r}: {e}")
        return False


def remove_from_group(user_id):
    """Remove user from Telegram group, then unban so they can rejoin later.

    Returns True only if BOTH the ban and the unban succeeded. The previous
    version returned True after a successful ban regardless of whether the
    unban worked — leaving users silently banned and producing the
    "invite link has expired" symptom downstream when they tried to rejoin.
    """
    if not TELEGRAM_GROUP_ID or not user_id:
        return False
    ban = telegram_api("banChatMember", {
        "chat_id": TELEGRAM_GROUP_ID,
        "user_id": int(user_id),
        "revoke_messages": False,
    })
    if not (ban and ban.get("ok")):
        return False
    if not unban_member(user_id):
        # Ban succeeded but unban didn't — user is now stuck banned.
        # Notify admin so they can intervene before the user gets stranded.
        print(f"   ⚠️ remove_from_group: ban OK but unban FAILED for {user_id}")
        notify_admin(
            f"⚠️ <b>Unban failed for user</b> <code>{user_id}</code>\n"
            f"They are now banned in the group. Manually unban them in "
            f"the group settings or they will see \"link expired\" on every "
            f"future invite link."
        )
        return False
    return True

def notify_admin(msg):
    if TELEGRAM_ADMIN_CHAT_ID:
        send_message(TELEGRAM_ADMIN_CHAT_ID, msg)


# ═══════════════════════════════════════════════════════════════
# (Instamojo polling removed — UPI flow in process_telegram_updates is the
# active payment path: user sends screenshot → admin taps Approve →
# approve_order_action records the payment and DMs the invite link.)
# ═══════════════════════════════════════════════════════════════



# ═══════════════════════════════════════════════════════════════
# 2. CHECK EXPIRY — SEND ALERTS & REMOVE EXPIRED
# ═══════════════════════════════════════════════════════════════

def check_expiry():
    """Send expiry alerts and remove expired subscribers."""
    print("\n" + "="*60)
    print("  QUANTEX — EXPIRY CHECK")
    print("="*60)

    subscribers = load_json(SUBSCRIBERS_FILE)
    now = datetime.utcnow()
    alerts_sent = 0
    removed = 0

    for sub in subscribers:
        if sub.get("status") != "active":
            continue

        end = datetime.fromisoformat(sub["subscription_end"])
        days_left = (end - now).days

        # ── EXPIRED — REMOVE ──
        if days_left < 0:
            print(f"\n>> EXPIRED: {sub['name']} ({sub.get('plan', '?')})")

            if sub.get("telegram_user_id"):
                # Send farewell
                send_message(sub["telegram_user_id"],
                    f"👋 Hi <b>{sub['name']}</b>,\n\n"
                    f"Your Quantex subscription has expired.\n"
                    f"You've been removed from the subscriber group.\n\n"
                    f"Resubscribe anytime to rejoin!\n"
                    f"We'd love to have you back! 📈"
                )
                # Remove from group
                ok = remove_from_group(sub["telegram_user_id"])
                print(f"   Telegram removal: {'OK' if ok else 'FAILED'}")

            sub["status"] = "expired"
            sub["updated_at"] = now.isoformat()

            notify_admin(
                f"🚪 <b>Subscription Expired</b>\n\n"
                f"Name: {sub['name']}\n"
                f"Plan: {sub.get('plan', '?')}\n"
                f"Phone: {sub.get('phone', '?')}"
            )
            removed += 1

        # ── EXPIRING IN 1-7 DAYS — SEND ALERT ──
        elif 0 <= days_left <= 7:
            end_str = end.strftime('%d %b %Y')

            if days_left == 0:
                urgency = "⚠️ LAST DAY"
            elif days_left <= 3:
                urgency = "⏰ EXPIRING SOON"
            else:
                urgency = "📋 REMINDER"

            alert = (
                f"{urgency}\n\n"
                f"Hi <b>{sub['name']}</b>,\n\n"
                f"Your Quantex subscription expires on <b>{end_str}</b> "
                f"(<b>{days_left} day{'s' if days_left != 1 else ''}</b> left).\n\n"
                f"Renew to keep receiving daily pre-market reports!"
            )

            if sub.get("telegram_user_id"):
                send_message(sub["telegram_user_id"], alert)
                print(f">> ALERT: {sub['name']} — {days_left}d left (DM sent)")
            else:
                print(f">> ALERT: {sub['name']} — {days_left}d left (no user_id)")

            alerts_sent += 1

    save_json(SUBSCRIBERS_FILE, subscribers)

    summary = f"📊 <b>Daily Expiry Check</b>\n\nAlerts sent: {alerts_sent}\nExpired & removed: {removed}"
    notify_admin(summary)

    print(f"\n>> Summary: {alerts_sent} alerts, {removed} removed\n")
    return alerts_sent, removed


# ═══════════════════════════════════════════════════════════════
# 3. APPROVAL ACTION (shared between /approve text command & inline button tap)
# ═══════════════════════════════════════════════════════════════

def approve_order_action(order_id, subscribers):
    """Run the full approval flow for `order_id`.

    Mutates `subscribers` in place (caller saves it). Writes pending_orders.json
    and payments.json. Sends the user their welcome message + invite link.

    Returns (success: bool, admin_message: str). The caller is responsible for
    delivering admin_message back to whoever invoked the action.
    """
    pending = load_pending_orders()
    order = None
    for p in pending:
        if p["order_id"] == order_id and p["status"] == "pending":
            order = p
            break

    if not order:
        return (False, f"❌ Order <code>{order_id}</code> not found or already processed.")

    now = datetime.utcnow()
    plan_days = order["plan_days"]
    sub_end = now + timedelta(days=plan_days)
    sub_user_id = order["user_id"]
    sub_username = order["username"]

    existing = None
    for s in subscribers:
        if s.get("telegram_user_id") == sub_user_id:
            existing = s
            break

    if existing:
        # Stack days on top if they're still active and not yet expired.
        if existing.get("status") == "active":
            current_end = datetime.fromisoformat(existing["subscription_end"])
            if current_end > now:
                sub_end = current_end + timedelta(days=plan_days)
        existing["status"] = "active"
        existing["plan"] = order["plan"]
        existing["subscription_start"] = now.isoformat()
        existing["subscription_end"] = sub_end.isoformat()
        existing["telegram_user_id"] = sub_user_id
        existing["telegram_username"] = sub_username
        existing["updated_at"] = now.isoformat()
    else:
        subscribers.append({
            "name": order["first_name"],
            "email": "",
            "phone": "",
            "telegram_username": sub_username,
            "telegram_user_id": sub_user_id,
            "plan": order["plan"],
            "status": "active",
            "subscription_start": now.isoformat(),
            "subscription_end": sub_end.isoformat(),
            "trial_used": True,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        })

    # Record payment
    payments = load_json(PAYMENTS_FILE)
    payments.append({
        "order_id": order_id,
        "amount": order["amount"],
        "plan": order["plan"],
        "user_id": sub_user_id,
        "username": sub_username,
        "name": order["first_name"],
        "method": "UPI",
        "status": "activated",
        "activated_at": now.isoformat(),
    })
    save_json(PAYMENTS_FILE, payments)

    # Mark order approved (saved back to file)
    order["status"] = "approved"
    order["approved_at"] = now.isoformat()
    save_pending_orders(pending)

    # CRITICAL: unban the user first. If they were previously kicked by the
    # expiry sweep and the unban silently failed at that time, they are still
    # banned now — and Telegram will show "this invite link has expired" on
    # every brand-new link we issue. unban_member is idempotent.
    if sub_user_id:
        unban_member(sub_user_id)

    # Build the user's confirmation + invite link
    invite = create_invite_link(expire_hours=72)
    sub_msg = (
        f"✅ <b>Payment Confirmed!</b>\n\n"
        f"Plan: <b>{order['plan'].title()}</b>\n"
        f"Valid till: <b>{sub_end.strftime('%d %b %Y')}</b>\n\n"
    )
    if invite:
        sub_msg += f"📲 <b>Join the group:</b>\n{invite}\n\n"
    if WHATSAPP_INVITE_LINK:
        sub_msg += f"💬 <b>WhatsApp:</b>\n{WHATSAPP_INVITE_LINK}\n\n"
    sub_msg += "Reports delivered every weekday at 8:00 AM IST! 📈"
    send_message(order["chat_id"], sub_msg)

    admin_msg = (
        f"✅ <b>Approved!</b>\n\n"
        f"Order: <code>{order_id}</code>\n"
        f"User: {order['first_name']} (@{sub_username})\n"
        f"Plan: {order['plan'].title()} — ₹{order['amount']}\n"
        f"Expires: {sub_end.strftime('%d %b %Y')}\n"
        + ("📲 Invite link delivered." if invite else "⚠️ No invite link — check TELEGRAM_GROUP_ID.")
    )
    return (True, admin_msg)


def approve_keyboard(order_id):
    """Inline keyboard with one-tap Approve / Reject buttons."""
    return {
        "inline_keyboard": [[
            {"text": f"✅ Approve {order_id}", "callback_data": f"approve:{order_id}"},
            {"text": "❌ Reject",              "callback_data": f"reject:{order_id}"},
        ]]
    }


# ═══════════════════════════════════════════════════════════════
# 4. HANDLE TELEGRAM BOT UPDATES (for /trial and /status)
# ═══════════════════════════════════════════════════════════════

def process_telegram_updates():
    """
    Poll Telegram bot for new messages.
    Handles: /start trial, /trial, /status
    Captures telegram_user_id for existing subscribers.
    """
    print("\n" + "="*60)
    print("  QUANTEX — TELEGRAM BOT UPDATES")
    print("="*60)

    if not TELEGRAM_BOT_TOKEN:
        print("!! Telegram bot token not set")
        return

    subscribers = load_json(SUBSCRIBERS_FILE)

    # Get updates (use offset stored in a file)
    offset_file = BASE_DIR / ".telegram_offset"
    offset = int(offset_file.read_text().strip()) if offset_file.exists() else 0

    result = telegram_api("getUpdates", {"offset": offset, "timeout": 5, "limit": 100})
    if not result or not result.get("ok"):
        print("!! Failed to get Telegram updates")
        return

    updates = result.get("result", [])
    print(f">> {len(updates)} new updates")

    # Per-run dedupe map for callback_query taps. If admin tapped the same
    # Approve button N times before this run, we'd otherwise process the
    # first as a real approval and the rest as "already processed" replies.
    # Keyed by action ("approve"/"reject") -> set of order_ids handled.
    processed_in_run: dict = {}

    for update in updates:
        offset = update["update_id"] + 1

        # ── Inline keyboard tap (callback_query) ──
        # Order matters here. Telegram shows a loading spinner on the tapped
        # button until it receives answerCallbackQuery (or ~10s passes). The
        # admin's perception of "did anything happen?" depends entirely on
        # how fast we get the spinner to clear and the buttons to disappear.
        # So we ACK + strip buttons FIRST, run the slow approval LAST.
        # Cron at 1-minute granularity means there's already a 0-60s gap
        # between tap and bot processing — don't add seconds of API calls
        # on top of that before giving any visual feedback.
        cq = update.get("callback_query")
        if cq:
            try:
                cq_id = cq["id"]
                cq_data = cq.get("data", "") or ""
                cq_user_id = str(cq["from"]["id"])
                cq_chat_id = cq.get("message", {}).get("chat", {}).get("id", cq_user_id)
                cq_msg_id = cq.get("message", {}).get("message_id")

                if not is_admin(cq_chat_id, cq_user_id):
                    answer_callback(cq_id, "Unauthorized", alert=True)
                    continue

                # Dedupe: when admin tapped the same Approve button multiple
                # times before this run, Telegram queues N callback_queries.
                # Within this single run, we only act on the first; the rest
                # get a quiet "already processed" toast and skip the slow
                # path so admin doesn't get N "Failed" replies.
                processed_in_run.setdefault("approve", set())
                processed_in_run.setdefault("reject", set())

                if cq_data.startswith("approve:"):
                    cb_order_id = cq_data.split(":", 1)[1].strip().upper()

                    if cb_order_id in processed_in_run["approve"]:
                        answer_callback(cq_id, "Already processed in this run")
                        continue
                    processed_in_run["approve"].add(cb_order_id)

                    # 1. ACK FAST — clears Telegram's loading spinner so the
                    #    admin sees their tap registered. Toast text shows
                    #    "Approving…" so it's obvious work is in progress.
                    answer_callback(cq_id, "⏳ Approving…")

                    # 2. Strip buttons IMMEDIATELY so admin can't tap again
                    #    while the slow work below is still running.
                    if cq_msg_id:
                        edit_message_reply_markup(cq_chat_id, cq_msg_id,
                                                  reply_markup={"inline_keyboard": []})

                    # 3. Slow path — creates invite link, DMs the user,
                    #    writes 3 JSON files. Buttons are already gone, the
                    #    admin already knows it's processing.
                    success, admin_msg = approve_order_action(cb_order_id, subscribers)

                    # 4. Final confirmation message in admin's chat.
                    send_message(cq_chat_id, admin_msg)

                elif cq_data.startswith("reject:"):
                    cb_order_id = cq_data.split(":", 1)[1].strip().upper()

                    if cb_order_id in processed_in_run["reject"]:
                        answer_callback(cq_id, "Already processed in this run")
                        continue
                    processed_in_run["reject"].add(cb_order_id)

                    answer_callback(cq_id, "Rejecting…")
                    if cq_msg_id:
                        edit_message_reply_markup(cq_chat_id, cq_msg_id,
                                                  reply_markup={"inline_keyboard": []})

                    pending = load_pending_orders()
                    found = False
                    for p in pending:
                        if p["order_id"] == cb_order_id and p["status"] == "pending":
                            p["status"] = "rejected"
                            p["rejected_at"] = datetime.utcnow().isoformat()
                            found = True
                            break
                    save_pending_orders(pending)
                    if found:
                        send_message(cq_chat_id, f"❌ Order <code>{cb_order_id}</code> marked rejected.")
                    else:
                        send_message(cq_chat_id, f"⚠️ Order <code>{cb_order_id}</code> was not pending — no change.")
                else:
                    answer_callback(cq_id, "")
            except Exception as e:
                print(f"   Callback error: {type(e).__name__}: {e}")
            continue

        msg = update.get("message", {})

        if not msg or msg.get("chat", {}).get("type") != "private":
            continue

        chat_id = msg["chat"]["id"]
        user_id = str(msg["from"]["id"])
        username = msg.get("from", {}).get("username", "")
        first_name = msg.get("from", {}).get("first_name", "")
        text = msg.get("text", "").strip()

        # ── /start or /start trial ──
        if text.startswith("/start"):
            arg = text.replace("/start", "").strip()

            if arg == "trial":
                # Check if already a subscriber
                existing = None
                for s in subscribers:
                    if s.get("telegram_user_id") == user_id or s.get("telegram_username") == username:
                        existing = s
                        break

                if existing and existing.get("trial_used"):
                    send_message(chat_id,
                        f"Hi {first_name}, you've already used your free trial.\n\n"
                        f"Subscribe to continue receiving reports!"
                    )
                elif existing and existing.get("status") == "active":
                    end = datetime.fromisoformat(existing["subscription_end"])
                    send_message(chat_id,
                        f"✅ You already have an active subscription!\n\n"
                        f"Plan: {existing.get('plan', '?')}\n"
                        f"Expires: {end.strftime('%d %b %Y')}\n"
                        f"Days left: {max(0, (end - datetime.utcnow()).days)}"
                    )
                else:
                    # Start trial
                    now = datetime.utcnow()
                    trial_end = now + timedelta(days=TRIAL_DAYS)

                    if existing:
                        existing["status"] = "active"
                        existing["plan"] = "trial"
                        existing["subscription_start"] = now.isoformat()
                        existing["subscription_end"] = trial_end.isoformat()
                        existing["trial_used"] = True
                        existing["telegram_user_id"] = user_id
                        existing["telegram_username"] = username
                        existing["updated_at"] = now.isoformat()
                    else:
                        subscribers.append({
                            "name": first_name,
                            "email": "",
                            "phone": "",
                            "telegram_username": username,
                            "telegram_user_id": user_id,
                            "plan": "trial",
                            "status": "active",
                            "subscription_start": now.isoformat(),
                            "subscription_end": trial_end.isoformat(),
                            "trial_used": True,
                            "created_at": now.isoformat(),
                            "updated_at": now.isoformat(),
                        })

                    # Idempotent unban — see approve_order_action note. A user
                    # restarting after a past expiry sweep would otherwise see
                    # "link expired" on this brand-new invite.
                    unban_member(user_id)
                    # Create invite link
                    invite = create_invite_link(expire_hours=48)

                    welcome = (
                        f"🎉 <b>Welcome to Quantex Scanner!</b>\n\n"
                        f"Hi <b>{first_name}</b>, your 3-day free trial starts now!\n\n"
                    )
                    if invite:
                        welcome += f"📲 <b>Join the group:</b>\n{invite}\n\n"
                    else:
                        welcome += (
                            f"📲 You will be auto-added to the Telegram subscriber "
                            f"group within the next 10 minutes.\n\n"
                        )
                    if WHATSAPP_INVITE_LINK:
                        welcome += f"💬 <b>WhatsApp Community:</b>\n{WHATSAPP_INVITE_LINK}\n\n"
                    welcome += (
                        f"Your trial ends on <b>{trial_end.strftime('%d %b %Y')}</b>.\n"
                        f"Reports are delivered every weekday at 8:00 AM IST.\n\n"
                        f"Enjoy your first report tomorrow morning! 📈"
                    )
                    send_message(chat_id, welcome)

                    notify_admin(
                        f"🆕 <b>New Trial Signup!</b>\n\n"
                        f"Name: {first_name}\n"
                        f"Username: @{username}\n"
                        f"Trial ends: {trial_end.strftime('%d %b %Y')}"
                    )

            elif arg == "subscribe":
                # Route to subscribe flow — handled below in /subscribe block
                send_message(chat_id, subscription_plans_text())
            else:
                # Regular /start
                send_message(chat_id,
                    f"👋 Welcome to <b>Quantex Scanner Bot</b>!\n\n"
                    f"Commands:\n"
                    f"/trial — Start your 3-day free trial\n"
                    f"/subscribe — Subscribe to a paid plan\n"
                    f"/status — Check your subscription\n"
                    f"/help — Get help\n\n"
                    f"Get daily pre-market reports for Indian stocks! 📊"
                )

        # ── /trial ──
        elif text == "/trial":
            # Redirect to /start trial logic
            existing = None
            for s in subscribers:
                if s.get("telegram_user_id") == user_id or s.get("telegram_username") == username:
                    existing = s
                    break

            if existing and existing.get("trial_used"):
                send_message(chat_id,
                    f"You've already used your free trial.\n"
                    f"Subscribe to keep receiving reports!"
                )
            elif existing and existing.get("status") == "active":
                send_message(chat_id, "✅ You already have an active subscription!")
            else:
                now = datetime.utcnow()
                trial_end = now + timedelta(days=TRIAL_DAYS)

                if existing:
                    existing["status"] = "active"
                    existing["plan"] = "trial"
                    existing["subscription_start"] = now.isoformat()
                    existing["subscription_end"] = trial_end.isoformat()
                    existing["trial_used"] = True
                    existing["telegram_user_id"] = user_id
                    existing["telegram_username"] = username
                else:
                    subscribers.append({
                        "name": first_name,
                        "email": "",
                        "phone": "",
                        "telegram_username": username,
                        "telegram_user_id": user_id,
                        "plan": "trial",
                        "status": "active",
                        "subscription_start": now.isoformat(),
                        "subscription_end": trial_end.isoformat(),
                        "trial_used": True,
                        "created_at": now.isoformat(),
                        "updated_at": now.isoformat(),
                    })

                # Idempotent unban — see approve_order_action note.
                unban_member(user_id)
                invite = create_invite_link(expire_hours=48)
                msg_text = f"🎉 Trial started! Ends on {trial_end.strftime('%d %b %Y')}.\n\n"
                if invite:
                    msg_text += f"📲 Join the group: {invite}\n\n"
                else:
                    msg_text += (
                        f"📲 You will be auto-added to the Telegram subscriber "
                        f"group within the next 10 minutes.\n\n"
                    )
                if WHATSAPP_INVITE_LINK:
                    msg_text += f"💬 WhatsApp: {WHATSAPP_INVITE_LINK}\n"
                send_message(chat_id, msg_text)
                notify_admin(f"🆕 Trial: {first_name} (@{username})")

        # ── /status ──
        elif text == "/status":
            sub = None
            for s in subscribers:
                if s.get("telegram_user_id") == user_id or s.get("telegram_username") == username:
                    sub = s
                    break

            if sub and sub.get("status") == "active":
                end = datetime.fromisoformat(sub["subscription_end"])
                days_left = max(0, (end - datetime.utcnow()).days)
                send_message(chat_id,
                    f"✅ <b>Active Subscription</b>\n\n"
                    f"Plan: <b>{sub.get('plan', '?').title()}</b>\n"
                    f"Expires: <b>{end.strftime('%d %b %Y')}</b>\n"
                    f"Days left: <b>{days_left}</b>"
                )
            else:
                send_message(chat_id,
                    f"❌ No active subscription.\n\n"
                    f"Use /trial for a free 3-day trial!"
                )

        # ── /help ──
        elif text == "/help":
            help_text = (
                f"📊 <b>Quantex Scanner Bot</b>\n\n"
                f"/trial — Start 3-day free trial\n"
                f"/subscribe — Subscribe to a paid plan\n"
                f"/status — Check subscription status\n"
                f"/whoami — Show your chat_id / user_id\n"
                f"/help — Show this message\n\n"
                f"After your trial, subscribe to keep receiving "
                f"daily pre-market reports at 8:00 AM IST!"
            )
            if is_admin(chat_id, user_id):
                help_text += (
                    "\n\n<b>Admin commands</b>\n"
                    "/pending — List pending orders\n"
                    "/approve QTX-XXXX — Approve a pending order\n"
                    "/sendinvite USER_ID — Send a group invite to a user"
                )
            send_message(chat_id, help_text)

        # ── /subscribe or /start subscribe ──
        elif text in ("/subscribe", "/start subscribe") or (text.startswith("/start") and "subscribe" in text):
            send_message(chat_id, subscription_plans_text())

        # ── Plan selection (1, 2, 3) ──
        elif text in PLAN_MENU_MAP:
            amount = PLAN_MENU_MAP[text]
            plan = PLANS[amount]
            order_id = generate_order_id()

            # Save pending order
            pending = load_pending_orders()
            pending.append({
                "order_id": order_id,
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "chat_id": chat_id,
                "amount": amount,
                "plan": plan["name"],
                "plan_days": plan["days"],
                "status": "pending",
                "created_at": datetime.utcnow().isoformat(),
            })
            # Remove expired pending orders (older than 24 hours)
            cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
            pending = [p for p in pending if p.get("created_at", "") > cutoff or p.get("status") == "approved"]
            save_pending_orders(pending)

            upi_link = build_upi_link(UPI_ID, UPI_NAME, amount, order_id)
            https_link = build_upi_https_link(UPI_ID, UPI_NAME, amount, order_id)
            # HTML-escape both links for safe inclusion in <a href="…">.
            # Without this, Telegram's HTML parser sees the unescaped `&` as
            # the start of an entity and silently truncates the href at the
            # first `&` — which breaks UPI deep-links (everything after `pa=…`
            # gets dropped, including amount and note).
            from html import escape as _html_escape
            upi_link_attr = _html_escape(upi_link, quote=True)
            https_link_attr = _html_escape(https_link, quote=True)

            # Generate and send QR code
            qr_path = generate_upi_qr(UPI_ID, UPI_NAME, amount, order_id)
            if qr_path:
                send_photo(chat_id, qr_path,
                    f"💳 <b>Scan to Pay — {plan['label']} (₹{amount})</b>\n"
                    f"Order ID: <code>{order_id}</code>\n\n"
                    f"Scan this QR code with any UPI app (GPay/PhonePe/Paytm)"
                )
                # Clean up QR file
                try:
                    os.remove(qr_path)
                except Exception:
                    pass

            send_message(chat_id,
                f"💳 <b>Payment Details</b>\n\n"
                f"Plan: <b>{plan['label']} (₹{amount})</b>\n"
                f"Order ID: <code>{order_id}</code>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>UPI ID:</b> <code>{UPI_ID}</code>\n"
                f"<b>Amount:</b> ₹{amount}\n"
                f"<b>Note/Remark:</b> <code>{order_id}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📱 <b>Steps:</b>\n"
                # Step 1 inlines the HTTPS pay link as the alternative to QR
                # scanning, so users have one action: scan OR tap. Both lead
                # to the same UPI payment. The HTTPS link goes to pay.html
                # which hands off to upi:// in the browser — Telegram on
                # mobile blocks bare upi:// links from message text but opens
                # https:// cleanly, then the OS dispatches the upi:// to
                # GPay/PhonePe/Paytm.
                f'1. Scan the QR code above, OR '
                f'📲 <a href="{https_link_attr}">Tap here to pay via UPI app</a>\n'
                f"2. Send payment screenshot here\n\n"
                f"⏰ Your subscription will be activated within 5 minutes after verification, "
                f"and you'll receive your private Telegram group invite link here as soon as "
                f"the payment is verified."
            )

            notify_admin(
                f"🛒 <b>New Order Created</b>\n\n"
                f"Order: <code>{order_id}</code>\n"
                f"User: {first_name} (@{username})\n"
                f"Plan: {plan['label']} — ₹{amount}\n"
                f"Status: ⏳ Awaiting payment"
            )

        # ── /whoami — anyone can call this. Diagnostic for the admin to verify
        # their chat_id matches TELEGRAM_ADMIN_CHAT_ID. Without this it's painful
        # to debug why /approve "does nothing".
        elif text == "/whoami":
            admin_set = "yes" if TELEGRAM_ADMIN_CHAT_ID else "no"
            admin_match = is_admin(chat_id, user_id)
            send_message(chat_id,
                f"🪪 <b>Identity</b>\n\n"
                f"chat_id: <code>{chat_id}</code>\n"
                f"user_id: <code>{user_id}</code>\n"
                f"username: @{username or '—'}\n\n"
                f"Admin env set: <b>{admin_set}</b>\n"
                f"You are admin: <b>{'yes' if admin_match else 'no'}</b>"
            )

        # ── Admin: /diag — full bot+group health check. Run this when users
        # report "invite link expired" on first tap. It checks the four
        # things that can cause that symptom: bot identity, group config,
        # bot's admin status / invite permission in the group, and a live
        # invite-link creation round-trip with the actual API response.
        elif text == "/diag":
            if not is_admin(chat_id, user_id):
                send_message(chat_id, "⚠️ Unauthorized. Send /whoami to see your ids.")
            else:
                lines = ["🔬 <b>Diagnostics</b>\n"]
                # 1. Bot identity
                me = telegram_api("getMe")
                if me and me.get("ok"):
                    bot = me["result"]
                    lines.append(f"• Bot: @{bot.get('username','?')} (id <code>{bot.get('id')}</code>)")
                    bot_id = bot.get("id")
                else:
                    lines.append(f"• Bot: ❌ getMe failed — {(me or {}).get('description','no response')}")
                    bot_id = None
                # 2. Group config
                if not TELEGRAM_GROUP_ID:
                    lines.append("• Group: ❌ TELEGRAM_GROUP_ID is empty")
                else:
                    chat_info = telegram_api("getChat", {"chat_id": TELEGRAM_GROUP_ID})
                    if chat_info and chat_info.get("ok"):
                        c = chat_info["result"]
                        lines.append(
                            f"• Group: <code>{TELEGRAM_GROUP_ID}</code> "
                            f"type=<b>{c.get('type','?')}</b> title={c.get('title','?')!r}"
                        )
                        if c.get("join_to_send_messages"):
                            lines.append("  ⚠️ join_to_send_messages is ON")
                        if c.get("join_by_request"):
                            lines.append("  ⚠️ join_by_request is ON — links require admin approval")
                    else:
                        lines.append(
                            f"• Group: ❌ getChat failed — {(chat_info or {}).get('description','no response')}"
                        )
                # 3. Bot's admin rights inside the group
                if bot_id and TELEGRAM_GROUP_ID:
                    member = telegram_api("getChatMember", {
                        "chat_id": TELEGRAM_GROUP_ID, "user_id": bot_id
                    })
                    if member and member.get("ok"):
                        m = member["result"]
                        status = m.get("status", "?")
                        lines.append(f"• Bot in group: status=<b>{status}</b>")
                        if status == "administrator":
                            lines.append(
                                f"  can_invite_users=<b>{m.get('can_invite_users')}</b> "
                                f"can_restrict_members=<b>{m.get('can_restrict_members')}</b>"
                            )
                            if not m.get("can_invite_users"):
                                lines.append("  ❌ Missing can_invite_users — links will be born expired.")
                        else:
                            lines.append("  ❌ Bot is not an administrator. Make it admin.")
                    else:
                        lines.append(
                            f"• Bot in group: ❌ getChatMember failed — "
                            f"{(member or {}).get('description','no response')}"
                        )
                # 4. Live link round-trip
                test_link = create_invite_link(expire_hours=1)
                if test_link:
                    lines.append(f"• Test link: ✅ <code>{test_link}</code>")
                    lines.append("  (Single-use, valid 1h. Tap it yourself — if THIS shows expired, the bug is in Telegram's link issuance, not the user.)")
                else:
                    lines.append("• Test link: ❌ create_invite_link returned None (see workflow logs)")
                send_message(chat_id, "\n".join(lines))

        # ── Admin: /approve ORDER_ID ──
        # NOTE: gate is split from the text match. If text starts with /approve
        # but the sender isn't admin, we now reply explicitly instead of
        # silently consuming the message (which previously left admins thinking
        # the bot had ignored their /approve and left orders stuck in pending).
        elif text.startswith("/approve"):
            if not is_admin(chat_id, user_id):
                print(f"   /approve from non-admin: chat_id={chat_id} user_id={user_id} "
                      f"expected={TELEGRAM_ADMIN_CHAT_ID!r}")
                send_message(chat_id,
                    f"⚠️ <b>Unauthorized.</b>\n\n"
                    f"Your chat_id: <code>{chat_id}</code>\n"
                    f"Your user_id: <code>{user_id}</code>\n\n"
                    f"If you are the admin, set TELEGRAM_ADMIN_CHAT_ID to one of "
                    f"the values above (no quotes) in repo Secrets and re-run."
                )
                # fall through — no further handling for this message
                if username:
                    for s in subscribers:
                        if s.get("telegram_username") == username and not s.get("telegram_user_id"):
                            s["telegram_user_id"] = user_id
                            s["updated_at"] = datetime.utcnow().isoformat()
                continue

            # Strip optional @bot_username suffix from the command token.
            parts = text.split()
            if parts and "@" in parts[0]:
                parts[0] = parts[0].split("@", 1)[0]

            if len(parts) < 2:
                # Admin probably tapped a /approve link. Re-send the pending
                # orders WITH inline keyboards so they can one-tap approve.
                pending = load_pending_orders()
                active_pending = [p for p in pending if p.get("status") == "pending"]
                if not active_pending:
                    send_message(chat_id, "No pending orders right now.")
                else:
                    send_message(chat_id, "👇 <b>Tap to approve:</b>")
                    for p in active_pending[:10]:
                        send_message(
                            chat_id,
                            (
                                f"<code>{p['order_id']}</code> — {p['first_name']} "
                                f"(@{p.get('username') or '—'})\n"
                                f"{p['plan'].title()} ₹{p['amount']} | {p['created_at'][:16]}"
                            ),
                            reply_markup=approve_keyboard(p["order_id"]),
                        )
            else:
                approve_order_id = parts[1].upper()
                _ok, admin_msg = approve_order_action(approve_order_id, subscribers)
                send_message(chat_id, admin_msg)

        # ── Admin: /pending — show pending orders, each with one-tap buttons ──
        elif text == "/pending":
            if not is_admin(chat_id, user_id):
                send_message(chat_id, "⚠️ Unauthorized. Send /whoami to see your ids.")
            else:
                pending = load_pending_orders()
                active_pending = [p for p in pending if p.get("status") == "pending"]
                if not active_pending:
                    send_message(chat_id, "No pending orders.")
                else:
                    send_message(chat_id, f"🛒 <b>Pending Orders ({len(active_pending)})</b>")
                    for p in active_pending:
                        send_message(
                            chat_id,
                            (
                                f"<code>{p['order_id']}</code> — {p['first_name']} "
                                f"(@{p.get('username') or '—'})\n"
                                f"{p['plan'].title()} ₹{p['amount']} | {p['created_at'][:16]}"
                            ),
                            reply_markup=approve_keyboard(p["order_id"]),
                        )

        # ── Admin: /sendinvite USER_ID — one-shot recovery for cases where
        # /approve previously fell through silently and the user never got a
        # group invite link. Creates a fresh single-use 72h link and DMs it.
        elif text.startswith("/sendinvite"):
            if not is_admin(chat_id, user_id):
                send_message(chat_id, "⚠️ Unauthorized. Send /whoami to see your ids.")
            else:
                parts = text.split()
                if len(parts) < 2:
                    send_message(chat_id, "Usage: /sendinvite USER_ID")
                else:
                    target_user_id = parts[1].strip()
                    # Idempotent unban first — see approve_order_action note.
                    unban_member(target_user_id)
                    invite = create_invite_link(expire_hours=72)
                    if not invite:
                        send_message(chat_id, "❌ Could not create invite link (check TELEGRAM_GROUP_ID and bot permissions).")
                    else:
                        sent = send_message(target_user_id,
                            f"📲 <b>Your Quantex group invite</b>\n\n{invite}\n\n"
                            f"This is a single-use link valid for 72 hours."
                        )
                        if sent and sent.get("ok"):
                            send_message(chat_id, f"✅ Invite sent to <code>{target_user_id}</code>")
                        else:
                            err = (sent or {}).get("description", "unknown error")
                            send_message(chat_id, f"❌ Could not DM <code>{target_user_id}</code>: {err}")

        # ── Photo received (payment screenshot) ──
        elif msg.get("photo"):
            # User sent a screenshot — notify admin
            pending = load_pending_orders()
            user_order = None
            for p in reversed(pending):
                if p.get("user_id") == user_id and p.get("status") == "pending":
                    user_order = p
                    break

            if user_order:
                # Forward the photo to admin
                try:
                    telegram_api("forwardMessage", {
                        "chat_id": TELEGRAM_ADMIN_CHAT_ID,
                        "from_chat_id": chat_id,
                        "message_id": msg["message_id"],
                    })
                except Exception:
                    pass

                # Admin notification with one-tap Approve / Reject buttons.
                # Tapping "✅ Approve" runs the full approval flow (activates
                # subscriber, records payment, generates invite link, DMs the
                # user) — no copy-paste, no /approve typing.
                if TELEGRAM_ADMIN_CHAT_ID:
                    send_message(
                        TELEGRAM_ADMIN_CHAT_ID,
                        (
                            f"📸 <b>Payment Screenshot Received!</b>\n\n"
                            f"Order: <code>{user_order['order_id']}</code>\n"
                            f"User: {first_name} (@{username or '—'})\n"
                            f"Plan: {user_order['plan'].title()} — ₹{user_order['amount']}"
                        ),
                        reply_markup=approve_keyboard(user_order["order_id"]),
                    )

                send_message(chat_id,
                    f"✅ Screenshot received! Your payment is being verified.\n\n"
                    f"Order ID: <code>{user_order['order_id']}</code>\n\n"
                    f"You'll receive your private Telegram group invite link here within "
                    f"5 minutes — as soon as the payment is verified."
                )
            else:
                send_message(chat_id,
                    f"Thanks for the screenshot! To subscribe, first use /subscribe to select a plan."
                )

        # ── Capture telegram_user_id for existing subscribers ──
        if username:
            for s in subscribers:
                if s.get("telegram_username") == username and not s.get("telegram_user_id"):
                    s["telegram_user_id"] = user_id
                    s["updated_at"] = datetime.utcnow().isoformat()
                    print(f"   Captured user_id {user_id} for @{username}")

    # Save offset and subscribers
    offset_file.write_text(str(offset))
    save_json(SUBSCRIBERS_FILE, subscribers)
    print(f">> Telegram updates processed")


# ═══════════════════════════════════════════════════════════════
# 4. STATS
# ═══════════════════════════════════════════════════════════════

def print_stats():
    """Print subscription statistics."""
    subscribers = load_json(SUBSCRIBERS_FILE)
    payments = load_json(PAYMENTS_FILE)

    active = [s for s in subscribers if s.get("status") == "active"]
    trial = [s for s in active if s.get("plan") == "trial"]
    paid = [s for s in active if s.get("plan") != "trial"]
    expired = [s for s in subscribers if s.get("status") == "expired"]
    revenue = sum(p.get("amount", 0) for p in payments if p.get("status") == "activated")

    print(f"\n{'='*40}")
    print(f"  QUANTEX STATS")
    print(f"{'='*40}")
    print(f"  Total signups:     {len(subscribers)}")
    print(f"  Active:            {len(active)}")
    print(f"  Trial users:       {len(trial)}")
    print(f"  Paid subscribers:  {len(paid)}")
    print(f"  Expired:           {len(expired)}")
    print(f"  Total revenue:     ₹{revenue:,.0f}")
    print(f"  Payments recorded: {len(payments)}")
    print()


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python process_payments.py expiry   — Check expiry + remove")
        print("  python process_payments.py telegram — Process bot messages")
        print("  python process_payments.py all      — Run telegram + stats")
        print("  python process_payments.py stats    — Show stats")
        sys.exit(1)

    cmd = sys.argv[1]

    # Back-compat: "payments" used to mean "poll Instamojo". Instamojo path is
    # gone — UPI flow runs inside process_telegram_updates. Forward old "payments"
    # invocations there so any cron jobs / docs still using the old name keep working.
    if cmd in ("payments", "telegram"):
        process_telegram_updates()
    elif cmd == "expiry":
        check_expiry()
    elif cmd == "all":
        process_telegram_updates()
        print_stats()
    elif cmd == "stats":
        print_stats()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
