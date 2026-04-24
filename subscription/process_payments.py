#!/usr/bin/env python3
"""
Quantex Subscription Manager — GitHub Actions Edition
No server needed. Runs entirely via GitHub Actions cron.

This script:
1. Polls Instamojo API for new payments
2. Matches payments to subscribers
3. Activates subscriptions in subscribers.json
4. Sends Telegram group invite links to new subscribers
5. Sends WhatsApp community invite link
6. Commits updated JSON back to the repo

Environment Variables (set as GitHub Secrets):
  INSTAMOJO_API_KEY      — Instamojo API key
  INSTAMOJO_AUTH_TOKEN   — Instamojo auth token
  INSTAMOJO_ENV          — 'production' or 'test' (default: test)
  TELEGRAM_BOT_TOKEN     — Telegram bot token
  TELEGRAM_GROUP_ID      — Subscriber group chat ID
  TELEGRAM_ADMIN_CHAT_ID — Admin notification chat ID
  WHATSAPP_INVITE_LINK   — WhatsApp community invite link
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

# Instamojo  (.strip() to remove accidental whitespace from GitHub Secrets)
INSTAMOJO_API_KEY = os.environ.get("INSTAMOJO_API_KEY", "").strip()
INSTAMOJO_AUTH_TOKEN = os.environ.get("INSTAMOJO_AUTH_TOKEN", "").strip()
INSTAMOJO_ENV = os.environ.get("INSTAMOJO_ENV", "test").strip()

if INSTAMOJO_ENV == "production":
    INSTAMOJO_BASE = "https://www.instamojo.com/api/1.1"
else:
    INSTAMOJO_BASE = "https://test.instamojo.com/api/1.1"

# Telegram  (.strip() to remove accidental whitespace from GitHub Secrets)
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_GROUP_ID = os.environ.get("TELEGRAM_GROUP_ID", "").strip()
TELEGRAM_ADMIN_CHAT_ID = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "").strip()

# WhatsApp
WHATSAPP_INVITE_LINK = os.environ.get("WHATSAPP_INVITE_LINK", "").strip()

# Plans — must match your Instamojo payment link titles/amounts
PLANS = {
    99:   {"name": "monthly",   "days": 30,  "label": "Monthly"},
    299:  {"name": "quarterly", "days": 90,  "label": "Quarterly"},
    799:  {"name": "yearly",    "days": 365, "label": "Yearly"},
}

TRIAL_DAYS = 3

# UPI Payment Config
UPI_ID = os.environ.get("UPI_ID", "jiten.choudhary373@oksbi").strip()
UPI_NAME = os.environ.get("UPI_NAME", "JITENDRA CHOUDHARY").strip()


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

def generate_upi_qr(upi_id, name, amount, order_id):
    """Generate UPI QR code image and return the file path."""
    try:
        import qrcode
        from io import BytesIO

        upi_string = f"upi://pay?pa={upi_id}&pn={name}&am={amount}&cu=INR&tn={order_id}"

        qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_H, box_size=10, border=4)
        qr.add_data(upi_string)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")

        qr_path = BASE_DIR / f"qr_{order_id}.png"
        img.save(str(qr_path))
        return str(qr_path)
    except Exception as e:
        print(f"   QR generation error: {e}")
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
# INSTAMOJO API
# ═══════════════════════════════════════════════════════════════

def instamojo_headers():
    return {
        "X-Api-Key": INSTAMOJO_API_KEY,
        "X-Auth-Token": INSTAMOJO_AUTH_TOKEN,
    }

def fetch_recent_payments():
    """Fetch recent payments from Instamojo API."""
    if not INSTAMOJO_API_KEY or not INSTAMOJO_AUTH_TOKEN:
        print("!! Instamojo credentials not set. Skipping payment fetch.")
        return []

    url = f"{INSTAMOJO_BASE}/payments/"
    try:
        resp = requests.get(url, headers=instamojo_headers(), timeout=30)
        if resp.ok:
            data = resp.json()
            payments = data.get("payments", [])
            print(f">> Fetched {len(payments)} payments from Instamojo")
            return payments
        else:
            print(f"!! Instamojo API error: {resp.status_code} {resp.text[:200]}")
            return []
    except Exception as e:
        print(f"!! Instamojo fetch error: {e}")
        return []


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

def send_message(chat_id, text):
    return telegram_api("sendMessage", {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    })

def create_invite_link(expire_hours=72):
    """Create a single-use Telegram group invite link."""
    if not TELEGRAM_GROUP_ID:
        return None
    result = telegram_api("createChatInviteLink", {
        "chat_id": TELEGRAM_GROUP_ID,
        "member_limit": 1,
        "expire_date": int((datetime.utcnow() + timedelta(hours=expire_hours)).timestamp()),
    })
    if result and result.get("ok"):
        return result["result"]["invite_link"]
    return None

def remove_from_group(user_id):
    """Remove user from Telegram group, then unban so they can rejoin later."""
    if not TELEGRAM_GROUP_ID or not user_id:
        return False
    result = telegram_api("banChatMember", {
        "chat_id": TELEGRAM_GROUP_ID,
        "user_id": int(user_id),
        "revoke_messages": False,
    })
    if result and result.get("ok"):
        telegram_api("unbanChatMember", {
            "chat_id": TELEGRAM_GROUP_ID,
            "user_id": int(user_id),
            "only_if_banned": True,
        })
        return True
    return False

def notify_admin(msg):
    if TELEGRAM_ADMIN_CHAT_ID:
        send_message(TELEGRAM_ADMIN_CHAT_ID, msg)


# ═══════════════════════════════════════════════════════════════
# 1. PROCESS NEW PAYMENTS
# ═══════════════════════════════════════════════════════════════

def process_payments():
    """Check Instamojo for new payments and activate subscriptions."""
    print("\n" + "="*60)
    print("  QUANTEX — PROCESS NEW PAYMENTS")
    print("="*60)

    subscribers = load_json(SUBSCRIBERS_FILE)
    known_payments = load_json(PAYMENTS_FILE)
    known_ids = {p["payment_id"] for p in known_payments}

    new_payments = fetch_recent_payments()
    activated = 0

    for pay in new_payments:
        pid = pay.get("payment_id", "")
        status = pay.get("status", "")

        # Skip already processed or failed payments
        if pid in known_ids or status != "Credit":
            continue

        amount = float(pay.get("amount", 0))
        buyer_name = pay.get("buyer_name", "Unknown")
        buyer_email = pay.get("buyer_email", "")
        buyer_phone = pay.get("buyer_phone", "")

        print(f"\n>> New payment: {pid}")
        print(f"   Name: {buyer_name}, Email: {buyer_email}, Phone: {buyer_phone}")
        print(f"   Amount: ₹{amount}, Status: {status}")

        # Determine plan from amount
        amount_int = int(amount)
        plan_info = PLANS.get(amount_int)
        if not plan_info:
            print(f"   !! Unknown amount ₹{amount} — skipping")
            notify_admin(f"⚠️ Unknown payment amount ₹{amount} from {buyer_name} ({buyer_email})")
            # Still record it
            known_payments.append({
                "payment_id": pid,
                "amount": amount,
                "buyer_name": buyer_name,
                "buyer_email": buyer_email,
                "buyer_phone": buyer_phone,
                "status": "unknown_amount",
                "processed_at": datetime.utcnow().isoformat(),
            })
            continue

        now = datetime.utcnow()

        # Find existing subscriber by email or phone
        sub = None
        for s in subscribers:
            if s.get("email") == buyer_email or s.get("phone") == buyer_phone:
                sub = s
                break

        if sub:
            # Extend existing subscription
            current_end = datetime.fromisoformat(sub["subscription_end"])
            if current_end < now:
                current_end = now
            new_end = current_end + timedelta(days=plan_info["days"])
            sub["plan"] = plan_info["name"]
            sub["status"] = "active"
            sub["subscription_end"] = new_end.isoformat()
            sub["updated_at"] = now.isoformat()
            sub["name"] = buyer_name  # Update in case changed
            print(f"   Extended {buyer_name} — {plan_info['label']} until {new_end.strftime('%d %b %Y')}")
        else:
            # New subscriber
            new_end = now + timedelta(days=plan_info["days"])
            sub = {
                "name": buyer_name,
                "email": buyer_email,
                "phone": buyer_phone,
                "telegram_username": "",
                "telegram_user_id": "",
                "plan": plan_info["name"],
                "status": "active",
                "subscription_start": now.isoformat(),
                "subscription_end": new_end.isoformat(),
                "trial_used": False,
                "created_at": now.isoformat(),
                "updated_at": now.isoformat(),
            }
            subscribers.append(sub)
            print(f"   New subscriber: {buyer_name} — {plan_info['label']} until {new_end.strftime('%d %b %Y')}")

        # Create Telegram invite link
        invite_link = create_invite_link(expire_hours=72)

        # Send welcome message with invite links
        welcome = (
            f"🎉 <b>Welcome to Quantex Scanner!</b>\n\n"
            f"Hi <b>{buyer_name}</b>, your <b>{plan_info['label']}</b> subscription is now active!\n\n"
        )
        if invite_link:
            welcome += f"📲 <b>Join Telegram Group:</b>\n{invite_link}\n\n"
        if WHATSAPP_INVITE_LINK:
            welcome += f"💬 <b>Join WhatsApp Community:</b>\n{WHATSAPP_INVITE_LINK}\n\n"
        welcome += (
            f"Your subscription is valid until <b>{new_end.strftime('%d %b %Y')}</b>.\n"
            f"Reports are delivered every weekday at 8:00 AM IST.\n\n"
            f"Send /status to check your subscription anytime."
        )

        # Send via email notification (Instamojo sends receipt automatically)
        # Send to Telegram if we have their user_id
        if sub.get("telegram_user_id"):
            send_message(sub["telegram_user_id"], welcome)

        # Notify admin
        notify_admin(
            f"💰 <b>New Payment!</b>\n\n"
            f"Name: {buyer_name}\n"
            f"Plan: {plan_info['label']} (₹{amount_int})\n"
            f"Email: {buyer_email}\n"
            f"Phone: {buyer_phone}\n"
            f"Active until: {new_end.strftime('%d %b %Y')}\n"
            f"Payment ID: {pid}"
        )

        # Record payment
        known_payments.append({
            "payment_id": pid,
            "amount": amount,
            "plan": plan_info["name"],
            "buyer_name": buyer_name,
            "buyer_email": buyer_email,
            "buyer_phone": buyer_phone,
            "status": "activated",
            "subscription_end": new_end.isoformat(),
            "processed_at": now.isoformat(),
        })

        activated += 1

    # Save
    save_json(SUBSCRIBERS_FILE, subscribers)
    save_json(PAYMENTS_FILE, known_payments)

    print(f"\n>> Processed: {activated} new subscriptions activated")
    return activated


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
# 3. HANDLE TELEGRAM BOT UPDATES (for /trial and /status)
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

    for update in updates:
        offset = update["update_id"] + 1
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
                send_message(chat_id,
                    f"📊 <b>Quantex Subscription Plans</b>\n\n"
                    f"1️⃣ <b>Monthly</b> — ₹149/month\n"
                    f"2️⃣ <b>Quarterly</b> — ₹399/quarter (₹133/mo)\n"
                    f"3️⃣ <b>Yearly</b> — ₹999/year (₹83/mo) 🔥\n\n"
                    f"Reply with the plan number:\n"
                    f"<b>1</b> for Monthly\n"
                    f"<b>2</b> for Quarterly\n"
                    f"<b>3</b> for Yearly"
                )
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
            send_message(chat_id,
                f"📊 <b>Quantex Scanner Bot</b>\n\n"
                f"/trial — Start 3-day free trial\n"
                f"/subscribe — Subscribe to a paid plan\n"
                f"/status — Check subscription status\n"
                f"/help — Show this message\n\n"
                f"After your trial, subscribe to keep receiving "
                f"daily pre-market reports at 8:00 AM IST!"
            )

        # ── /subscribe or /start subscribe ──
        elif text in ("/subscribe", "/start subscribe") or (text.startswith("/start") and "subscribe" in text):
            send_message(chat_id,
                f"📊 <b>Quantex Subscription Plans</b>\n\n"
                f"1️⃣ <b>Monthly</b> — ₹99/month\n"
                f"2️⃣ <b>Quarterly</b> — ₹299/quarter (₹100/mo)\n"
                f"3️⃣ <b>Yearly</b> — ₹799/year (₹67/mo) 🔥\n\n"
                f"Reply with the plan number:\n"
                f"<b>1</b> for Monthly\n"
                f"<b>2</b> for Quarterly\n"
                f"<b>3</b> for Yearly"
            )

        # ── Plan selection (1, 2, 3) ──
        elif text in ("1", "2", "3"):
            plan_map = {"1": 99, "2": 299, "3": 799}
            amount = plan_map[text]
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

            upi_link = f"upi://pay?pa={UPI_ID}&pn={UPI_NAME}&am={amount}&cu=INR&tn={order_id}"

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
                f"1. Scan the QR code above, OR\n"
                f"2. Pay ₹{amount} to <code>{UPI_ID}</code>\n"
                f"3. Add <code>{order_id}</code> in payment note/remark\n"
                f"4. Send payment screenshot here\n\n"
                f"⏰ Your subscription will be activated within 10 minutes after verification.\n\n"
                f"<a href='{upi_link}'>📲 Click to Pay via UPI</a>"
            )

            notify_admin(
                f"🛒 <b>New Order Created</b>\n\n"
                f"Order: <code>{order_id}</code>\n"
                f"User: {first_name} (@{username})\n"
                f"Plan: {plan['label']} — ₹{amount}\n"
                f"Status: ⏳ Awaiting payment"
            )

        # ── Admin: /approve ORDER_ID ──
        elif text.startswith("/approve ") and str(chat_id) == TELEGRAM_ADMIN_CHAT_ID:
            parts = text.split()
            if len(parts) < 2:
                send_message(chat_id, "Usage: /approve QTX-XXXX")
            else:
                approve_order_id = parts[1].upper()
                pending = load_pending_orders()
                order = None
                for p in pending:
                    if p["order_id"] == approve_order_id and p["status"] == "pending":
                        order = p
                        break

                if not order:
                    send_message(chat_id, f"❌ Order <code>{approve_order_id}</code> not found or already processed.")
                else:
                    # Activate subscription
                    now = datetime.utcnow()
                    plan_days = order["plan_days"]
                    sub_end = now + timedelta(days=plan_days)

                    # Find or create subscriber
                    sub_user_id = order["user_id"]
                    sub_username = order["username"]
                    existing = None
                    for s in subscribers:
                        if s.get("telegram_user_id") == sub_user_id:
                            existing = s
                            break

                    if existing:
                        # Extend if already active
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
                        "order_id": approve_order_id,
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

                    # Mark order as approved
                    order["status"] = "approved"
                    order["approved_at"] = now.isoformat()
                    save_pending_orders(pending)

                    # Create invite link for subscriber
                    invite = create_invite_link(expire_hours=72)

                    # Notify subscriber
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

                    # Confirm to admin
                    send_message(chat_id,
                        f"✅ <b>Approved!</b>\n\n"
                        f"Order: <code>{approve_order_id}</code>\n"
                        f"User: {order['first_name']} (@{sub_username})\n"
                        f"Plan: {order['plan'].title()} — ₹{order['amount']}\n"
                        f"Expires: {sub_end.strftime('%d %b %Y')}"
                    )

        # ── Admin: /pending — show pending orders ──
        elif text == "/pending" and str(chat_id) == TELEGRAM_ADMIN_CHAT_ID:
            pending = load_pending_orders()
            active_pending = [p for p in pending if p.get("status") == "pending"]
            if not active_pending:
                send_message(chat_id, "No pending orders.")
            else:
                msg_text = f"🛒 <b>Pending Orders ({len(active_pending)})</b>\n\n"
                for p in active_pending:
                    msg_text += (
                        f"<code>{p['order_id']}</code> — {p['first_name']} (@{p['username']})\n"
                        f"   {p['plan'].title()} ₹{p['amount']} | {p['created_at'][:16]}\n"
                        f"   → /approve {p['order_id']}\n\n"
                    )
                send_message(chat_id, msg_text)

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

                notify_admin(
                    f"📸 <b>Payment Screenshot Received!</b>\n\n"
                    f"Order: <code>{user_order['order_id']}</code>\n"
                    f"User: {first_name} (@{username})\n"
                    f"Plan: {user_order['plan'].title()} — ₹{user_order['amount']}\n\n"
                    f"To approve: /approve {user_order['order_id']}"
                )

                send_message(chat_id,
                    f"✅ Screenshot received! Your payment is being verified.\n\n"
                    f"Order ID: <code>{user_order['order_id']}</code>\n"
                    f"You'll receive confirmation within 10 minutes."
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
        print("  python process_payments.py payments   — Poll Instamojo + activate")
        print("  python process_payments.py expiry     — Check expiry + remove")
        print("  python process_payments.py telegram   — Process bot messages")
        print("  python process_payments.py all        — Run everything")
        print("  python process_payments.py stats      — Show stats")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "payments":
        process_payments()
    elif cmd == "expiry":
        check_expiry()
    elif cmd == "telegram":
        process_telegram_updates()
    elif cmd == "all":
        process_telegram_updates()
        process_payments()
        print_stats()
    elif cmd == "stats":
        print_stats()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
