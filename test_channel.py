import asyncio
import os
from dotenv import load_dotenv
from telethon import TelegramClient

load_dotenv()

async def main():
    api_id = int(os.getenv("API_ID"))
    api_hash = os.getenv("API_HASH")
    channel_id = int(os.getenv("CHANNEL"))

    # check_session.py mein jo session use hai,
    # yahan wahi session configuration use karna zaroori hai.
    client = TelegramClient("user_session", api_id, api_hash)

    await client.start()
    try:
        entity = await client.get_entity(channel_id)
        print("✅ Channel connected:", entity.title)
    except Exception as e:
        print(f"❌ {type(e).__name__}: {e}")
    finally:
        await client.disconnect()

asyncio.run(main())
