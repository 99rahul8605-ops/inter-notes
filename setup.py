"""
ONE-TIME SETUP — bas ek baar chalao:
    python setup.py

Ye script:
1. Agar .env nahi hai to API_ID/API_HASH/BOT_TOKEN/CHANNEL poochh kar
   .env bana degi (values aapke paas pehle se hon to nahi poochhegi).
2. Ek baar Telegram login karayegi (phone number + code) aur
   SESSION_STRING khud .env me likh degi.

Iske baad bot ko seedha `python bot.py` se chalao — purane notes bhi
automatic index ho jayenge, phir kabhi setup ki zaroorat nahi.
"""

import asyncio
import os

from dotenv import load_dotenv, set_key
from telethon import TelegramClient
from telethon.sessions import StringSession

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")


def ask(prompt, current):
    """Current value hai to skip, warna poochh lo."""
    if current:
        print(f"  ✔ {prompt}: pehle se set hai, skip.")
        return current
    while True:
        val = input(f"  {prompt}: ").strip()
        if val:
            return val
        print("  ⚠️ Khali nahi chhod sakte, dobara likho.")


def ask_optional(prompt, current, default=""):
    if current:
        print(f"  ✔ {prompt}: pehle se set hai, skip.")
        return current
    val = input(f"  {prompt} [{default}] (Enter = default): ").strip()
    return val or default


async def main():
    print("=" * 50)
    print("📚 Notes Search Bot — One-Time Setup")
    print("=" * 50)

    load_dotenv(ENV_PATH)
    api_id = os.environ.get("API_ID", "")
    api_hash = os.environ.get("API_HASH", "")
    bot_token = os.environ.get("BOT_TOKEN", "")
    channel = os.environ.get("CHANNEL", "")

    print("\n[1/3] Telegram config check kar rahe hain...")
    api_id = ask("API_ID (my.telegram.org se)", api_id)
    api_hash = ask("API_HASH (my.telegram.org se)", api_hash)
    bot_token = ask("BOT_TOKEN (@BotFather se)", bot_token)
    channel = ask_optional("CHANNEL (jaise @notes_channel ya -100...)",
                           channel, "@my_notes_channel")
    set_key(ENV_PATH, "API_ID", api_id)
    set_key(ENV_PATH, "API_HASH", api_hash)
    set_key(ENV_PATH, "BOT_TOKEN", bot_token)
    set_key(ENV_PATH, "CHANNEL", channel)
    print("  ✅ .env me config save ho gayi.")

    print("\n[2/3] Ek baar Telegram login (purane notes ke liye)...")
    client = TelegramClient(
        StringSession(), int(api_id), api_hash,
        device_model="NotesBotSetup",
    )
    await client.start()  # phone number + login code yahin poochhega
    session_string = client.session.save()
    await client.disconnect()
    set_key(ENV_PATH, "SESSION_STRING", session_string)
    print("  ✅ Login ho gaya! SESSION_STRING .env me save ho gayi.")

    print("\n[3/3] Ho gaya!")
    print("-" * 50)
    print("✅ Setup complete. Ab bot chalao:")
    print("     python bot.py")
    print("\nBot khud karega:")
    print("  • Channel ke purane notes index (pehli baar, automatic)")
    print("  • Har startup pe sirf missing notes fetch karega")
    print("  • Naye posts real-time index karega (bot channel me admin hona chahiye)")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
