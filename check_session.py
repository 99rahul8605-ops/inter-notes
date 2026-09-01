"""
Session string check karne ke liye:
    python check_session.py

Batayega: session USER ki hai ya BOT ki, kis account ki hai, phone number.
"""

import asyncio
import base64
import os
import struct
import sys

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

DC_IPS = {
    1: "149.154.175.53",
    2: "149.154.167.51",
    3: "149.154.175.100",
    4: "149.154.167.91",
    5: "91.108.56.130",
}


def pyrogram_to_telethon(value: str) -> StringSession:
    data = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    for fmt in (">B?256sI?", ">BI?256sQ?"):
        if len(data) < struct.calcsize(fmt):
            continue
        try:
            unpacked = struct.unpack(fmt, data[: struct.calcsize(fmt)])
        except struct.error:
            continue
        dc_id, auth_key = unpacked[0], None
        for u in unpacked:
            if isinstance(u, bytes) and len(u) == 256:
                auth_key = u
        if auth_key and dc_id in DC_IPS:
            ts = StringSession()
            ts.set_dc(dc_id, DC_IPS[dc_id], 443)
            ts.auth_key = auth_key
            return ts
    raise ValueError("Pyrogram session format samajh nahi aaya")


def load_session(value: str):
    if not value:
        return None
    try:
        return StringSession(value)
    except Exception:
        pass
    try:
        print("ℹ️ Pyrogram session detect hui — convert kar raha hoon...")
        return pyrogram_to_telethon(value.strip())
    except Exception:
        return None


async def main():
    load_dotenv()
    value = os.environ.get("SESSION_STRING", "").strip()
    if not value:
        print("❌ .env me SESSION_STRING khali hai.")
        return
    if not os.environ.get("API_ID"):
        print("❌ .env me API_ID/API_HASH nahi hai.")
        return

    session = load_session(value)
    if session is None:
        print("❌ Session string parse hi nahi hui — format samajh nahi aaya.")
        sys.exit(1)

    client = TelegramClient(
        session, int(os.environ["API_ID"]), os.environ["API_HASH"]
    )
    await client.connect()
    if not await client.is_user_authorized():
        print("❌ Session authorized nahi hai (expired ya galat).")
        await client.disconnect()
        return

    me = await client.get_me()
    kind = "🤖 BOT" if getattr(me, "bot", False) else "👤 USER (normal account)"
    print("=" * 50)
    print(f"Account type : {kind}")
    print(f"Naam         : {me.first_name} {me.last_name or ''}")
    print(f"Username     : @{me.username}" if me.username else "Username     : —")
    if getattr(me, "phone", None):
        print(f"Phone        : {me.phone}")
    print("=" * 50)
    if getattr(me, "bot", False):
        print("\n⚠️ YE SESSION BOT KI HAI — isse channel history nahi milegi!")
        print("   Fix: `python setup.py` chalao aur APNE phone number se login")
        print("   karo (bot token NAHI). Jo session mile use .env me daalo.")
    else:
        print("\n✅ Ye user session hai — purane notes backfill ho sakte hain.")
        print("   Ab bot restart karo: python bot.py")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
