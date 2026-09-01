"""
Ek baar chalao, Telegram login karo, aur jo SESSION_STRING mile use
.env file me daalo. Isse bot channel ka PURANA history (backfill)
index kar sakta hai. Sirf setup ke liye chahiye, baad me nahi.
"""

import asyncio
import os

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

load_dotenv()


async def main():
    api_id = int(os.environ["API_ID"])
    api_hash = os.environ["API_HASH"]
    async with TelegramClient(StringSession(), api_id, api_hash) as client:
        session_string = client.session.save()
        print("\n✅ Login ho gaya! Neeche wali line copy karke .env me daalo:\n")
        print(f"SESSION_STRING={session_string}\n")


if __name__ == "__main__":
    asyncio.run(main())
