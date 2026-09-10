"""
Telegram Notes Search Bot (CA Aspirants ke liye)
=================================================
Ek channel se CA study notes (PDFs/images/messages) ko index karta hai
aur group me accurate search karke deta hai — e.g. "costing marginal
costing", "audit ch 3", "ind as 116" jaisi queries se.

Kaise kaam karta hai:
1. Bot ko notes channel me ADMIN banao  -> channel ka har naya post
   automatically index ho jata hai.
2. Bot ko group me add karo -> koi bhi `/find <query>` likhe ya
   inline `@botusername <query>` use kare, bot sabse accurate results
   bhejega (file/text copy karke).
3. Purane messages index karne ke liye ek USER account se one-time
   backfill hota hai (bots channel history read nahi kar sakte).
   Iske liye `python make_session.py` chala kar SESSION_STRING banao
   aur .env me daalo. (Optional - naye posts to bot khud index karta hai.)
4. Admin commands (`/stats`, `/debug`, `/reindex`, `/broadcast`) sirf
   tumhare liye kaam karein iske liye `.env` me
   `ADMIN_IDS=<tumhari_telegram_user_id>` set karo (apni ID @userinfobot
   se pata karo). Comma se multiple admins bhi daal sakte ho.
5. User activity (/stats, /broadcast) SQLite me store hoti hai by
   default — koi extra setup nahi chahiye. Agar tumhare paas MongoDB hai
   (jaise multi-instance ya shared-dashboard setup ke liye), `.env` me
   `MONGO_URI=mongodb+srv://...` daal do — bot automatically Mongo
   (async `motor` driver) use karne lagega. `pip install motor` chalao.

Search accuracy:
- SQLite FTS5 full-text index (token + prefix matching)
- Fuzzy reranking (token overlap + SequenceMatcher ratio)
- Typo-correction fallback (word ke beech me spelling mistake ho tab bhi)
- File names bhi search hote hain (photo/document caption ke saath)
"""

import asyncio
import base64
import difflib
import hashlib
import os
import re
import sqlite3
import struct
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta

from dotenv import load_dotenv
from telethon import Button, TelegramClient, events, utils
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.types import DocumentAttributeFilename

try:
    from motor.motor_asyncio import AsyncIOMotorClient
except ImportError:
    AsyncIOMotorClient = None

load_dotenv()

# ---- Single-instance lock: do bot processes me updates batwate nahi,
# ---- aur purana process purane buttons handle karta rehta hai (confusion)
_LOCK_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "notes_bot.lock"
)
try:
    import fcntl as _fcntl
    _lockfile = open(_LOCK_PATH, "w")
    try:
        _fcntl.flock(_lockfile.fileno(), _fcntl.LOCK_EX | _fcntl.LOCK_NB)
    except BlockingIOError:
        print("❌ Bot PEHLE SE chal raha hai (doosra process)!\n"
              "   Pehle usko band karo: pkill -f bot.py\n"
              "   Fir check karo: ps aux | grep bot.py")
        raise SystemExit(1)
except ImportError:
    try:
        import msvcrt as _msvcrt
        _lockfile = open(_LOCK_PATH, "w")
        try:
            _msvcrt.locking(_lockfile.fileno(), _msvcrt.LK_NBLCK, 1)
        except OSError:
            print("❌ Bot PEHLE SE chal raha hai (doosra process)! Usko band karo.")
            raise SystemExit(1)
    except ImportError:
        pass  # locking supported nahi, aage badho

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
# Channel jahan notes hain (username jaise "@my_notes_channel" ya numeric id "-100...")
CHANNEL = os.environ.get("CHANNEL", "")
# Group jahan search karna hai (optional; khali chhodo to har group me kaam karega)
GROUP_ID = os.environ.get("GROUP_ID", "")
# Optional: purane messages backfill karne ke liye user session
SESSION_STRING = os.environ.get("SESSION_STRING", "")
# Admin commands (/stats, /debug, /reindex) sirf inhi Telegram user IDs ke
# liye kaam karenge. Comma-separated numeric IDs, e.g. "123456789,987654321"
# Apni Telegram user ID pata karne ke liye @userinfobot ko message karo.
ADMIN_IDS = {
    int(x) for x in os.environ.get("ADMIN_IDS", "").replace(" ", "").split(",") if x
}

# User activity tracking (/stats, /broadcast) ke liye storage backend.
# Default SQLite hai (same DB file, koi extra setup nahi). Agar `.env` me
# MONGO_URI daaloge, to Mongo (motor — async driver) use hoga — ye
# multi-instance/shared setups ke liye better hai (SQLite file ek jagah
# locked rehti hai, Mongo network se sab instances share kar sakte hain).
MONGO_URI = os.environ.get("MONGO_URI", "")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "notes_bot")
mongo_users = None
mongo_daily = None  # per-day distinct-user check-ins, /stats ke DAU/WAU trend ke liye
if MONGO_URI:
    if AsyncIOMotorClient is None:
        print("⚠️ MONGO_URI set hai lekin 'motor' package install nahi hai. "
              "`pip install motor` karo. Filhaal SQLite fallback use ho raha hai.")
    else:
        _mongo_db = AsyncIOMotorClient(MONGO_URI)[MONGO_DB_NAME]
        mongo_users = _mongo_db["users"]
        mongo_daily = _mongo_db["daily_activity"]


def is_admin(event) -> bool:
    return event.sender_id in ADMIN_IDS


async def _deny_if_not_admin(event) -> bool:
    """True return kare to caller turant return kar de (access denied)."""
    if is_admin(event):
        return False
    if not ADMIN_IDS:
        await event.respond(
            "⛔ **Access Denied**\n"
            "Ye admin-only command hai, lekin `.env` me `ADMIN_IDS` set "
            "nahi hai — isliye filhaal koi bhi ise use nahi kar sakta.\n\n"
            "Apni Telegram user ID @userinfobot se pata karke `.env` me "
            "`ADMIN_IDS=<apni_id>` add karein, phir bot restart karein."
        )
    else:
        await event.respond("⛔ **Access Denied** — ye command sirf admins ke liye hai.")
    return True


# ---------- Rate limiting (flood/spam protection) ----------
# Ek user X second me sirf N searches kar sakta hai. Isse koi ek user
# bot ko flood karke sabke liye slow/down na kar de.
RATE_LIMIT_COUNT = int(os.environ.get("RATE_LIMIT_COUNT", "5"))    # max searches
RATE_LIMIT_WINDOW = int(os.environ.get("RATE_LIMIT_WINDOW", "10"))  # ...per N sec
_rate_buckets: dict = defaultdict(deque)
_rate_last_warned: dict = {}

# File-send (note tap karke actual file/photo bhejna) ke liye ALAG, thoda
# strict limit — kyunki ye search se zyada heavy operation hai (bandwidth +
# Telegram API calls). Isse "same file baar-baar mangna" wala spam rukta hai.
FILE_RATE_LIMIT_COUNT = int(os.environ.get("FILE_RATE_LIMIT_COUNT", "5"))
FILE_RATE_LIMIT_WINDOW = int(os.environ.get("FILE_RATE_LIMIT_WINDOW", "20"))
_file_rate_buckets: dict = defaultdict(deque)


def _check_limit(buckets: dict, user_id: int, count: int, window: int) -> bool:
    """True = allowed. False = is user abhi rate-limited hai."""
    if user_id in ADMIN_IDS:
        return True
    now = time.time()
    bucket = buckets[user_id]
    while bucket and now - bucket[0] > window:
        bucket.popleft()
    if len(bucket) >= count:
        return False
    bucket.append(now)
    return True


def _check_rate_limit(user_id: int) -> bool:
    return _check_limit(_rate_buckets, user_id, RATE_LIMIT_COUNT, RATE_LIMIT_WINDOW)


def _check_file_rate_limit(user_id: int) -> bool:
    return _check_limit(_file_rate_buckets, user_id, FILE_RATE_LIMIT_COUNT, FILE_RATE_LIMIT_WINDOW)


async def _record_daily_activity(uid: int, now: int):
    """DAU/WAU trend ke liye: ek user ek din me sirf EK baar count ho
    (chahe wo us din search bhi kare aur file bhi maange). SQLite path me
    commit yahan nahi hota — _track_user ke end me ek saath ho jaata hai."""
    day = datetime.fromtimestamp(now).strftime("%Y-%m-%d")
    if mongo_daily is not None:
        await mongo_daily.update_one(
            {"_id": f"{uid}:{day}"},
            {"$setOnInsert": {"user_id": uid, "day": day}},
            upsert=True,
        )
    else:
        db.execute(
            "INSERT OR IGNORE INTO daily_activity (user_id, day) VALUES (?,?)",
            (uid, day),
        )


async def _track_user(event, kind: str):
    """Har allowed search/file-request pe user ki activity record karo
    (/stats me dikhane ke liye). kind = 'search' ya 'file'.
    Backend: Mongo (agar MONGO_URI set hai) warna SQLite fallback."""
    uid = event.sender_id
    if uid is None:
        return
    now = int(time.time())
    username = None
    try:
        sender = await event.get_sender()
        username = getattr(sender, "username", None) or getattr(sender, "first_name", None)
    except Exception:
        pass
    col = "search_count" if kind == "search" else "file_count"
    other_col = "file_count" if kind == "search" else "search_count"
    await _record_daily_activity(uid, now)

    if mongo_users is not None:
        set_fields = {"last_seen": now}
        if username:
            set_fields["username"] = username
        await mongo_users.update_one(
            {"_id": uid},
            {
                "$set": set_fields,
                "$setOnInsert": {"first_seen": now, other_col: 0},
                "$inc": {col: 1},
            },
            upsert=True,
        )
        return

    row = db.execute("SELECT user_id FROM users WHERE user_id=?", (uid,)).fetchone()
    if row:
        db.execute(
            f"UPDATE users SET last_seen=?, username=COALESCE(?, username), "
            f"{col} = {col} + 1 WHERE user_id=?",
            (now, username, uid),
        )
    else:
        db.execute(
            "INSERT INTO users (user_id, username, first_seen, last_seen, "
            "search_count, file_count) VALUES (?,?,?,?,?,?)",
            (uid, username, now, now, 1 if kind == "search" else 0,
             1 if kind == "file" else 0),
        )
    db.commit()


async def _rate_limit_notice(respond_fn, user_id: int):
    """Warning sirf kabhi kabhi bhejo (har blocked request pe nahi),
    warna warning khud hi spam ban jayegi."""
    now = time.time()
    last = _rate_last_warned.get(user_id, 0)
    if now - last < RATE_LIMIT_WINDOW:
        return
    _rate_last_warned[user_id] = now
    try:
        await respond_fn(
            f"⏳ **Rate limit reached** — {RATE_LIMIT_WINDOW} second me max "
            f"{RATE_LIMIT_COUNT} searches allowed hain. Thodi der baad dobara try karein."
        )
    except Exception:
        pass


MAX_RESULTS = int(os.environ.get("MAX_RESULTS", "5"))
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "5"))  # ek page pe itne results
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "notes_index.db")

bot = TelegramClient("notes_bot", API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# Telegram ke production data centers (Pyrogram -> Telethon conversion ke liye)
DC_IPS = {
    1: "149.154.175.53",
    2: "149.154.167.51",
    3: "149.154.175.100",
    4: "149.154.167.91",
    5: "91.108.56.130",
}


def pyrogram_to_telethon(value: str) -> StringSession:
    """Pyrogram session string Telethon StringSession me badlo.
    Dono same MTProto auth key use karte hain, isliye safe hai."""
    data = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    # Pyrogram v2 packed formats (dc_id, ..., auth_key(256s), ...)
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
    """Telethon format hai to as-is, Pyrogram format hai to convert karke."""
    if not value:
        return None
    try:
        return StringSession(value)
    except Exception:
        pass
    try:
        print("ℹ️ Pyrogram session detect hui — Telethon format me convert kar raha hoon...")
        return pyrogram_to_telethon(value.strip())
    except Exception:
        print("⚠️ SESSION_STRING na Telethon na Pyrogram format me hai. "
              "`python setup.py` chala kar nayi session bana lo. "
              "Filhaal purane notes backfill off rahega.")
        return None


user = (
    TelegramClient(load_session(SESSION_STRING), API_ID, API_HASH)
    if SESSION_STRING
    else None
)

# ---------------------------------------------------------------- database

db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row
db.executescript(
    """
    CREATE TABLE IF NOT EXISTS notes (
        channel_id   INTEGER,
        msg_id       INTEGER,
        title        TEXT,      -- file name ya pehli line (display ke liye)
        text         TEXT,      -- caption/poora text (search ke liye)
        file_type    TEXT,
        date         INTEGER,
        content_hash TEXT,      -- same content repost = duplicate detect
        PRIMARY KEY (channel_id, msg_id)
    );
    CREATE VIRTUAL TABLE IF NOT EXISTS notes_fts USING fts5(
        title, text,
        content='notes', content_rowid='rowid',
        tokenize='unicode61 remove_diacritics 2'
    );
    CREATE TRIGGER IF NOT EXISTS notes_ai AFTER INSERT ON notes BEGIN
        INSERT INTO notes_fts(rowid, title, text)
        VALUES (new.rowid, new.title, new.text);
    END;
    CREATE TRIGGER IF NOT EXISTS notes_ad AFTER DELETE ON notes BEGIN
        INSERT INTO notes_fts(notes_fts, rowid, title, text)
        VALUES ('delete', old.rowid, old.title, old.text);
    END;
    """
)
# FTS5 ki vocab table — index ke saare distinct words list karne ke liye
# (typo-correction fallback me use hoti hai, jab word ke BEECH me spelling
# mistake ho jise prefix/substring match pakad nahi paata)
db.execute(
    "CREATE VIRTUAL TABLE IF NOT EXISTS notes_vocab USING fts5vocab(notes_fts, 'row')"
)
# User activity tracking (/stats me dikhane ke liye) — same SQLite DB me,
# koi alag DB/service (Mongo waghera) ki zaroorat nahi.
db.execute(
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id       INTEGER PRIMARY KEY,
        username      TEXT,
        first_seen    INTEGER,
        last_seen     INTEGER,
        search_count  INTEGER DEFAULT 0,
        file_count    INTEGER DEFAULT 0
    )
    """
)
# Har user ka har-din ek check-in (/stats me Daily/Weekly Active Users
# trend dikhane ke liye) — MongoDB backend me isi ka equivalent
# "daily_activity" collection hai (upar dekho).
db.execute(
    """
    CREATE TABLE IF NOT EXISTS daily_activity (
        user_id INTEGER,
        day     TEXT,
        PRIMARY KEY (user_id, day)
    )
    """
)
db.execute("CREATE INDEX IF NOT EXISTS idx_daily_activity_day ON daily_activity(day)")
# Admin-adjustable runtime config (/settings se change hota hai) — .env
# ki values sirf DEFAULT/startup value hain, yahan se override persist
# hoti hai (bot restart ke baad bhi apni value yaad rehti hai).
db.execute(
    """
    CREATE TABLE IF NOT EXISTS settings (
        key        TEXT PRIMARY KEY,
        value      TEXT,
        updated_at INTEGER
    )
    """
)
# Purani DB migration: content_hash column + unique index (dedupe ke liye)
_cols = [r[1] for r in db.execute("PRAGMA table_info(notes)")]
if "content_hash" not in _cols:
    db.execute("ALTER TABLE notes ADD COLUMN content_hash TEXT")
    db.commit()


def _text_hash(title: str, text: str):
    """Sirf-text notes (ya purane migration rows) ke liye normalized text hash."""
    norm = re.sub(r"\s+", " ", f"{title} {text}".lower()).strip()
    norm = re.sub(r"#\d+", "", norm)  # "note #123" fallback title me id na aaye
    return hashlib.sha1(norm.encode()).hexdigest() if norm else None


def content_hash(message, title: str, text: str):
    """Same content repost pakadne ke liye fingerprint.
    Photo/document ho to Telegram ki apni file id use karo — isse caption
    ho ya na ho, wahi file dobara post hone par turant duplicate pakda
    jayega. Sirf-text posts ke liye normalized text hash use hota hai."""
    if message is not None and message.file:
        media_id = getattr(message.file.media, "id", None)
        if media_id is not None:
            return hashlib.sha1(f"file:{media_id}".encode()).hexdigest()
    return _text_hash(title, text)


# Purane rows ka hash backfill
for _row in db.execute(
    "SELECT rowid, title, text FROM notes WHERE content_hash IS NULL"
):
    db.execute(
        "UPDATE notes SET content_hash=? WHERE rowid=?",
        (_text_hash(_row["title"], _row["text"]), _row["rowid"]),
    )
# Unique index banao; purane duplicates pehle saaf karo (naya version rakho)
try:
    db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_notes_hash ON notes(content_hash)"
    )
    db.commit()
except sqlite3.IntegrityError:
    db.execute(
        """
        DELETE FROM notes WHERE content_hash IS NOT NULL AND rowid NOT IN (
            SELECT rowid FROM (
                SELECT rowid,
                       ROW_NUMBER() OVER (
                           PARTITION BY content_hash
                           ORDER BY date DESC, rowid DESC
                       ) rn
                FROM notes WHERE content_hash IS NOT NULL
            ) WHERE rn = 1
        )
        """
    )
    db.commit()
    db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_notes_hash ON notes(content_hash)"
    )
    db.commit()


# ---------------------------------------------------------------- settings catalog
# In env-vars ko `/settings` se bot ke through hi change kar sakte hain
# (bina .env edit kiye, bina restart kiye). Sirf "safe" tunable knobs yahan
# hain — API_ID/API_HASH/BOT_TOKEN/SESSION_STRING/MONGO_URI/CHANNEL jaanbujh
# kar shamil nahi hain, kyunki inhe badalne ke liye reconnect/restart chahiye
# ya ye sensitive credentials hain jo chat me nahi bhejni chahiye.
SETTING_DEFS = {
    "RATE_LIMIT_COUNT": {
        "label": "Search rate limit — count", "type": int, "min": 1, "max": 100,
        "hint": "Ek user X second me max kitni searches kar sakta hai",
    },
    "RATE_LIMIT_WINDOW": {
        "label": "Search rate limit — window (sec)", "type": int, "min": 1, "max": 3600,
        "hint": "Upar wali limit kitne second ke window me apply hoti hai",
    },
    "FILE_RATE_LIMIT_COUNT": {
        "label": "File rate limit — count", "type": int, "min": 1, "max": 100,
        "hint": "Ek user X second me max kitni files maang sakta hai",
    },
    "FILE_RATE_LIMIT_WINDOW": {
        "label": "File rate limit — window (sec)", "type": int, "min": 1, "max": 3600,
        "hint": "Upar wali limit kitne second ke window me apply hoti hai",
    },
    "MAX_RESULTS": {
        "label": "Inline results limit", "type": int, "min": 1, "max": 50,
        "hint": "Inline mode (@botname query) me max kitne results dikhein",
    },
    "PAGE_SIZE": {
        "label": "Results per page", "type": int, "min": 1, "max": 20,
        "hint": "/find ke paginated list me ek page pe kitne notes dikhein",
    },
    "GROUP_ID": {
        "label": "Search-allowed group ID", "type": "optional_str",
        "hint": "Khali chhodo to bot sab groups me search karega. Set karne "
                "ke liye numeric group ID (jaise -100123456789) bhejo.",
    },
}


def _setting_display_value(key: str):
    return globals().get(key)


def _format_setting_value(key: str, value) -> str:
    if key == "GROUP_ID":
        return value if value else "*(khali — sab groups)*"
    return str(value)


def _load_persisted_settings():
    """Startup pe DB me persisted overrides (agar hain) .env defaults ke
    upar apply karo, taaki `/settings`/`/addadmin`/`/deladmin` se ki gayi
    changes restart ke baad bhi yaad rahein."""
    for row in db.execute("SELECT key, value FROM settings"):
        key, raw = row["key"], row["value"]
        if key == "ADMIN_IDS":
            try:
                globals()["ADMIN_IDS"] = {int(x) for x in raw.split(",") if x.strip()}
            except ValueError:
                pass
            continue
        spec = SETTING_DEFS.get(key)
        if not spec:
            continue  # purani/anjaan key, ignore
        try:
            globals()[key] = int(raw) if spec["type"] is int else raw
        except (TypeError, ValueError):
            continue


def _save_admin_ids():
    """ADMIN_IDS set ko settings table me persist karo (comma-separated)."""
    db.execute(
        "INSERT INTO settings (key, value, updated_at) VALUES (?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        ("ADMIN_IDS", ",".join(str(x) for x in sorted(ADMIN_IDS)), int(time.time())),
    )
    db.commit()


def _apply_setting(key: str, raw_value: str):
    """Naya value validate karke apply + persist karo.
    Returns (ok: bool, message: str)."""
    spec = SETTING_DEFS.get(key)
    if not spec:
        return False, f"❌ Unknown setting: `{key}`"

    raw_value = raw_value.strip()
    if spec["type"] is int:
        try:
            value = int(raw_value)
        except ValueError:
            return False, f"❌ `{key}` ke liye number chahiye (jaise `10`)."
        if not (spec["min"] <= value <= spec["max"]):
            return False, (
                f"❌ `{key}` sirf {spec['min']} se {spec['max']} ke beech "
                f"ho sakta hai."
            )
    elif spec["type"] == "optional_str":
        value = "" if raw_value in ("-", "off", "none", "khali") else raw_value
        if value:
            try:
                int(value)  # GROUP_ID numeric hona chahiye
            except ValueError:
                return False, "❌ Group ID numeric hona chahiye (jaise `-100123456789`)."
    else:
        value = raw_value

    globals()[key] = value
    db.execute(
        "INSERT INTO settings (key, value, updated_at) VALUES (?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        (key, str(value), int(time.time())),
    )
    db.commit()
    return True, f"✅ `{key}` update ho gaya: **{_format_setting_value(key, value)}**"


_load_persisted_settings()

# Admin `/settings` me ek setting edit karne ke liye tap kare -> agla plain
# text message uska naya value maana jaata hai (DM me). key = admin user_id.
_pending_setting_edit: dict = {}


def _settings_overview_text() -> str:
    lines = ["⚙️ **Bot Settings**", "Neeche kisi setting ko tap karke value badlein.\n"]
    for key, spec in SETTING_DEFS.items():
        val = _format_setting_value(key, _setting_display_value(key))
        lines.append(f"• **{spec['label']}** (`{key}`): {val}")
    return "\n".join(lines)


def _settings_buttons():
    return [
        [Button.inline(f"✏️ {spec['label']}", f"cfg:{key}")]
        for key, spec in SETTING_DEFS.items()
    ]


@bot.on(events.NewMessage(pattern=r"^/settings(@\w+)?"))
async def on_settings(event):
    if await _deny_if_not_admin(event):
        return
    if not event.is_private:
        await event.respond("⚙️ Settings sirf DM me admin ke liye available hain — bot ko DM karein.")
        return
    await event.respond(_settings_overview_text(), buttons=_settings_buttons(), link_preview=False)


@bot.on(events.CallbackQuery(pattern=r"^cfg:(\w+)$"))
async def on_settings_pick(event):
    if not is_admin(event):
        await event.answer("⛔ Ye admin-only hai.", alert=True)
        return
    key = event.pattern_match.group(1).decode() if isinstance(event.pattern_match.group(1), bytes) \
        else event.pattern_match.group(1)
    spec = SETTING_DEFS.get(key)
    if not spec:
        await event.answer("❌ Unknown setting.", alert=True)
        return
    _pending_setting_edit[event.sender_id] = key
    current = _format_setting_value(key, _setting_display_value(key))
    await event.answer()
    await event.respond(
        f"✏️ **{spec['label']}** (`{key}`)\n"
        f"Current value: **{current}**\n"
        f"{spec['hint']}\n\n"
        f"Naya value reply me bhejein (ya /cancel likhein):"
    )


@bot.on(events.NewMessage(pattern=r"^/cancel(@\w+)?"))
async def on_settings_cancel(event):
    if event.is_private and _pending_setting_edit.pop(event.sender_id, None):
        await event.respond("Cancelled — koi change nahi hui.")


@bot.on(events.NewMessage(pattern=r"^/setconfig(@\w+)?(\s+.*)?$"))
async def on_setconfig(event):
    """Power-user shortcut: `/settings` ke tap-and-reply flow ke bina,
    ek line me directly value set karo. Example: `/setconfig MAX_RESULTS 8`"""
    if await _deny_if_not_admin(event):
        return
    args = (event.pattern_match.group(2) or "").strip().split(maxsplit=1)
    if len(args) != 2:
        keys = ", ".join(f"`{k}`" for k in SETTING_DEFS)
        await event.respond(
            "Usage: `/setconfig <KEY> <VALUE>`\n\n"
            f"Available keys: {keys}\n\n"
            "Ya `/settings` chalayein tap-and-edit ke liye."
        )
        return
    key, value = args[0].upper(), args[1]
    _, msg = _apply_setting(key, value)
    await event.respond(msg)


async def _resolve_target_id(event, arg: str):
    """Explicit numeric ID diya ho to wahi use karo, warna reply kiye gaye
    message ke sender ko target maano. None = na argument, na reply."""
    if arg:
        try:
            return int(arg.split()[0])
        except ValueError:
            return None
    reply = await event.get_reply_message()
    if reply and reply.sender_id:
        return reply.sender_id
    return None


@bot.on(events.NewMessage(pattern=r"^/addadmin(@\w+)?(\s+.*)?$"))
async def on_addadmin(event):
    if await _deny_if_not_admin(event):
        return
    arg = (event.pattern_match.group(2) or "").strip()
    target_id = await _resolve_target_id(event, arg)
    if target_id is None:
        await event.respond(
            "Usage: `/addadmin <user_id>` ya kisi user ke message ko "
            "**reply** karke bina argument ke `/addadmin` chalayein.\n"
            "User ID @userinfobot se pata kar sakte hain."
        )
        return
    if target_id in ADMIN_IDS:
        await event.respond(f"ℹ️ `{target_id}` pehle se admin hai.")
        return
    ADMIN_IDS.add(target_id)
    _save_admin_ids()
    await event.respond(f"✅ `{target_id}` ko admin bana diya gaya. Total admins: **{len(ADMIN_IDS)}**")


@bot.on(events.NewMessage(pattern=r"^/deladmin(@\w+)?(\s+.*)?$"))
async def on_deladmin(event):
    if await _deny_if_not_admin(event):
        return
    arg = (event.pattern_match.group(2) or "").strip()
    target_id = await _resolve_target_id(event, arg)
    if target_id is None:
        await event.respond(
            "Usage: `/deladmin <user_id>` ya kisi admin ke message ko "
            "**reply** karke bina argument ke `/deladmin` chalayein."
        )
        return
    if target_id not in ADMIN_IDS:
        await event.respond(f"ℹ️ `{target_id}` admin nahi hai.")
        return
    if len(ADMIN_IDS) <= 1:
        await event.respond(
            "⛔ Last admin ko remove nahi kar sakte — isse koi bhi admin "
            "command use nahi kar payega. Pehle koi doosra admin add karein."
        )
        return
    ADMIN_IDS.discard(target_id)
    _save_admin_ids()
    note = " (aapne khud ko remove kar diya)" if target_id == event.sender_id else ""
    await event.respond(
        f"✅ `{target_id}` ko admin list se hata diya gaya{note}. "
        f"Total admins: **{len(ADMIN_IDS)}**"
    )


@bot.on(events.NewMessage(pattern=r"^/admins(@\w+)?"))
async def on_admins(event):
    if await _deny_if_not_admin(event):
        return
    lines = [f"👑 **Admins** ({len(ADMIN_IDS)})"]
    lines += [f"• `{uid}`" for uid in sorted(ADMIN_IDS)]
    lines.append(
        "\n`/addadmin <id>` · `/deladmin <id>` "
        "(ya kisi user ke message ko reply karke bina id ke)"
    )
    await event.respond("\n".join(lines))


def note_title(message, text: str) -> str:
    """File name ya pehli meaningful line ko title banao."""
    if message.document:
        for attr in message.document.attributes:
            if isinstance(attr, DocumentAttributeFilename) and attr.file_name:
                return attr.file_name
    for line in text.splitlines():
        if line.strip():
            return line.strip()[:120]
    return f"note #{message.id}"


def add_note(message, text: str, channel_id: int) -> bool:
    text = (text or "").strip()
    if not text and not message.file:
        return False
    title = note_title(message, text)
    h = content_hash(message, title, text)
    db.execute(
        "INSERT OR REPLACE INTO notes VALUES (?,?,?,?,?,?,?)",
        (
            channel_id,
            message.id,
            title,
            text,
            ("photo" if message.photo else "document" if message.document
             else "text"),
            int(message.date.timestamp()),
            h,
        ),
    )
    db.commit()
    return True


# ---------------------------------------------------------------- search

STOPWORDS = {"the", "a", "an", "of", "for", "and", "or", "in", "on", "to",
             "ka", "ki", "ke", "hai", "me", "se", "kya", "pdf",
             "notes", "note", "chapter", "ch",
             # CA students aksar aise filler words ke saath maangte hain
             "please", "send", "share", "download", "link", "bhejo", "de", "do"}


def tokens(q: str):
    raw = re.findall(r"[\w\u0900-\u097F]+", q.lower())
    return [t for t in raw if t not in STOPWORDS] or raw


@dataclass
class Result:
    row: sqlite3.Row
    score: float


# Pagination STATELESS hai — koi alag DB table nahi chahiye.
# Query khud message ke text me hoti hai ("🔎 "query" — N results"),
# isliye Next/Prev click hone par wahi query message se nikaal ke
# live search dobara chala dete hain. Isse bot restart, redeploy, ya
# doosra process chalne se bhi buttons kabhi "stale" nahi hote.

_QUERY_RE = re.compile(
    r'🔎\s*(?:\*\*)?["“](.+?)["”](?:\*\*)?\s*[—–-]\s*'
    r'\d+\s+results\s*\(Page\s+\d+\s*/\s*\d+\)',
    re.IGNORECASE,
)


def _query_from_message_text(text: str):
    if not text:
        return None

    text = text.strip()
    m = _QUERY_RE.search(text)
    if m:
        return m.group(1).strip()

    # Fallback: heading ki first line me quotes ke andar query nikaalo.
    first_line = text.splitlines()[0]
    m = re.search(r'["“](.+?)["”]', first_line)
    return m.group(1).strip() if m else None


def display_title(r) -> str:
    """Button me caption ki pehli line dikhao; caption na ho to file name."""
    cap = (r["text"] or "").strip()
    if cap:
        return cap.splitlines()[0][:60]
    return (r["title"] or "")[:60]


def _page_buttons(query: str, results, offset: int, owner_id: int):
    """Ek page ke result buttons + Prev/Next pager.
    owner_id = jisne /find chalaya tha — sirf wahi in buttons ko use kar
    payega (group me doosre log click karke try karein to warning milegi)."""
    total = len(results)
    npages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = offset // PAGE_SIZE + 1
    chunk = results[offset:offset + PAGE_SIZE]
    buttons = [
        [Button.inline(f"📄 {display_title(res.row)}",
                       f"note:{res.row['channel_id']}:{res.row['msg_id']}:{owner_id}")]
        for res in chunk
    ]
    if npages > 1:
        pager = []
        if page > 1:
            pager.append(Button.inline("◀ Prev", f"pg:{offset - PAGE_SIZE}:{owner_id}"))
        if page < npages:
            pager.append(Button.inline("Next ▶", f"pg:{offset + PAGE_SIZE}:{owner_id}"))
        buttons.append(pager)
    text = (f"🔎 **\"{query}\"** — {total} results (Page {page}/{npages})\n"
            f"Required note par tap karein:")
    return text, buttons


def _closest_terms(token: str, limit: int = 3, min_ratio: float = 0.72):
    """Index ke vocab me se token ke closest-spelling words dhoondo —
    ye asli typo-correction hai (jaise 'acounting' -> 'accounting'),
    isse alag hai FTS prefix match jo sirf shuru se match karta hai."""
    tl = len(token)
    if tl < 3:
        return []
    rows = db.execute("SELECT DISTINCT term FROM notes_vocab").fetchall()
    scored = []
    for r in rows:
        term = r["term"]
        if abs(len(term) - tl) > 2:
            continue
        ratio = difflib.SequenceMatcher(None, token, term).ratio()
        if ratio >= min_ratio:
            scored.append((ratio, term))
    scored.sort(reverse=True)
    return [t for _, t in scored[:limit]]


def search(query: str, limit: int = None):
    """FTS5 se candidates -> fuzzy score se rerank -> top results."""
    if limit is None:
        limit = MAX_RESULTS  # runtime value (settings se badal sakta hai)
    toks = tokens(query)
    if not toks:
        return []

    # 1) FTS5 candidate set: har token ke saath prefix match
    match = " OR ".join(f'"{t}"*' for t in toks)
    try:
        candidates = db.execute(
            """SELECT n.* FROM notes_fts f
               JOIN notes n ON n.rowid = f.rowid
               WHERE notes_fts MATCH ?
               ORDER BY bm25(notes_fts) LIMIT 200""",
            (match,),
        ).fetchall()
    except sqlite3.OperationalError:
        candidates = []

    # 2) LIKE fallback (chhote DB / typos ke liye ek safety net)
    if len(candidates) < limit:
        like = "%" + "%".join(toks[:3]) + "%"
        seen = {r["msg_id"] for r in candidates}
        for r in db.execute(
            "SELECT * FROM notes WHERE title LIKE ? OR text LIKE ? "
            "ORDER BY date DESC LIMIT 100",
            (like, like),
        ):
            if r["msg_id"] not in seen:
                candidates.append(r)

    # 3) Typo-correction fallback: prefix/substring dono fail ho gaye
    #    (matlab galat letter WORD KE BEECH me hai, jaise "chemstry"),
    #    to index ke actual words me se closest spelling dhoondo aur
    #    unse dobara try karo.
    if not candidates:
        corrected = set()
        for t in toks:
            corrected.update(_closest_terms(t))
        if corrected:
            match2 = " OR ".join(f'"{t}"*' for t in corrected)
            try:
                candidates = db.execute(
                    """SELECT n.* FROM notes_fts f
                       JOIN notes n ON n.rowid = f.rowid
                       WHERE notes_fts MATCH ?
                       ORDER BY bm25(notes_fts) LIMIT 200""",
                    (match2,),
                ).fetchall()
            except sqlite3.OperationalError:
                candidates = []

    if not candidates:
        return []

    # 3) Rerank — AND semantics: SAARE keywords ka milna zaroori,
    #    warna "law handwritten" pe doosre subject ke "handwritten" notes aa jate
    q_lower = query.lower()
    qlen = max(len(q_lower), 1)
    scored = []
    for r in candidates:
        hay = f"{r['title']} {r['text']}".lower()
        if not hay:
            continue
        hay_tokens = set(re.findall(r"[\w\u0900-\u097F]+", hay))
        title_lower = r["title"].lower()
        tok_scores = []
        for t in toks:
            best = 0.0
            for ht in hay_tokens:
                if ht == t:
                    best = 1.0
                    break
                # Chhote tokens (law, eco, bot...) fuzzy me galat match karte
                # hain (law~flow) — inke liye sirf near-exact count karo
                if len(t) <= 4:
                    if abs(len(ht) - len(t)) <= 1:
                        s = difflib.SequenceMatcher(None, t, ht).ratio()
                        best = max(best, s)
                elif abs(len(ht) - len(t)) <= max(len(t), len(ht)) // 2:
                    s = difflib.SequenceMatcher(None, t, ht).ratio()
                    best = max(best, s)
            tok_scores.append(best)
        min_tok = min(tok_scores)                      # sabse kamzor keyword
        coverage = sum(s >= 0.85 for s in tok_scores) / len(toks)
        avg_tok = sum(tok_scores) / len(tok_scores)
        # Title me kitne keywords exact aaye — user subject ka naam pehle
        # likhta hai, isliye title match sabse bharosemand signal hai
        title_cov = sum(t in title_lower for t in toks) / len(toks)
        phrase = difflib.SequenceMatcher(
            None, q_lower, hay[: qlen * 3]
        ).ratio()
        # Gate: har keyword ka koi na koi decent match hona hi chahiye
        if min_tok < 0.7 and coverage < 1.0:
            continue
        score = (
            2.0 * min_tok
            + 2.5 * coverage
            + 1.5 * avg_tok
            + 3.0 * title_cov
            + 0.5 * (1.0 / (1.0 + abs(time.time() - r["date"]) / 86400 * 30))
        )
        scored.append(Result(r, score))

    # Sabse relevant matches hi list me aayenge (upar wala score-gate),
    # lekin unme se DISPLAY ORDER naye se purane (date descending) hoga —
    # taaki latest upload hamesha list me sabse upar dikhe.
    scored.sort(key=lambda x: (-x.row["date"], -x.score))

    # Duplicate content (same content_hash — e.g. same file repost hua tha
    # purane index-time bug ki wajah se) ko list me sirf ek baar dikhao.
    # DB me dono rows rehte hain, bas display se extra copy hata di jaati hai.
    seen_hash = set()
    deduped = []
    for res in scored:
        h = res.row["content_hash"]
        if h is not None:
            if h in seen_hash:
                continue
            seen_hash.add(h)
        deduped.append(res)

    return deduped[:limit]


# ---------------------------------------------------------------- delivery

async def send_result(chat, res: Result):
    r = res.row
    try:
        # Message COPY karo — ismein media + ORIGINAL caption, aur text
        # posts ke formatting entities (jaise hyperlinks) sab preserve
        # rehte hain (naya plain text banane se hyperlink toot jata tha).
        msg = await bot.get_messages(r["channel_id"], ids=r["msg_id"])
        if msg and (msg.file or (msg.message and msg.message.strip())):
            await bot.send_message(chat, msg)
            return
    except Exception:
        pass  # original message fetch fail hua to plain text fallback
    await bot.send_message(chat, f"📄 {r['title']}\n{r['text'][:500]}", link_preview=False)


async def reply_search(chat, query: str, owner_id: int):
    results = search(query, limit=50)
    if not results:
        total = db.execute("SELECT COUNT(*) c FROM notes").fetchone()["c"]
        if total == 0:
            await bot.send_message(
                chat,
                "❌ **Index Khali Hai**\n"
                "Abhi tak koi notes indexed nahi hain, isliye result nahi mila.\n\n"
                "**Setup verify karein:**\n"
                "1. Bot ko notes **channel me admin** banayein (tabhi naye posts "
                "index hote hain).\n"
                "2. Purane posts ke liye `python make_session.py` chala kar "
                "SESSION_STRING banayein aur `.env` me add karke bot restart karein.\n"
                "3. `.env` me `CHANNEL` sahi set hai? (jaise `@my_notes_channel`)\n"
                "4. Detailed status ke liye `/debug` chalayein.",
                link_preview=False,
            )
        else:
            await bot.send_message(
                chat,
                f"❌ **\"{query}\"** ke liye koi result nahi mila (index me {total} notes hain).\n\n"
                "**Try karein:**\n"
                "• Spelling ek baar check kar lein\n"
                "• Poora sentence na likhein — sirf 1-2 **keyword** use karein "
                "(jaise subject ka naam ya file ka koi khaas shabd)\n"
                "• Chhota/short word try karein (jaise \"depreciation\" ki jagah \"deprec\")\n\n"
                "Tip: `/debug` se index ka status check kar sakte hain.",
                link_preview=False,
            )
        return
    # Direct file spam nahi — pehle list dikhao (paginated), user tap kare
    text, buttons = _page_buttons(query, results, 0, owner_id)
    await bot.send_message(chat, text, buttons=buttons, link_preview=False)


async def _deny_if_not_owner(event, owner_id: int) -> bool:
    """True return kare to caller turant return kar de. Sirf jisne search
    kiya tha (ya admin) hi buttons use kar sakta hai — group me doosre
    users click karke try karein to warning milti hai."""
    if event.sender_id == owner_id or is_admin(event):
        return False
    await event.answer(
        "⛔ Ye buttons sirf search karne wale user ke liye hain.\n"
        "Khud search karne ke liye apna `/find <query>` bhejein.",
        alert=True,
    )
    return True


@bot.on(events.CallbackQuery(pattern=r"^pg:(\d+):(-?\d+)$"))
async def on_page(event):
    offset = int(event.pattern_match.group(1))
    owner_id = int(event.pattern_match.group(2))
    if await _deny_if_not_owner(event, owner_id):
        return
    if not _check_rate_limit(event.sender_id):
        await event.answer(
            f"⏳ Rate limit reached — {RATE_LIMIT_WINDOW}s me max "
            f"{RATE_LIMIT_COUNT} taps allowed hain.",
            alert=True,
        )
        return
    msg = await event.get_message()
    message_text = None
    if msg:
        message_text = (
            getattr(msg, "raw_text", None)
            or getattr(msg, "text", None)
            or getattr(msg, "message", None)
        )
    query = _query_from_message_text(message_text)
    if query is None:
        await event.answer(
            "⚠️ Ye message expired/corrupt hai — `/find` dobara chalayein.",
            alert=True,
        )
        return
    results = search(query, limit=50)
    if not results:
        await event.answer(
            "Is query ke liye ab koi result nahi mila (notes delete ho gaye honge). "
            "`/find` dobara chalayein.",
            alert=True,
        )
        return
    text, buttons = _page_buttons(query, results, offset, owner_id)
    await event.edit(text, buttons=buttons, link_preview=False)
    await event.answer()


@bot.on(events.CallbackQuery(pattern=r"^note:(-?\d+):(\d+):(-?\d+)$"))
async def on_note_pick(event):
    cid = int(event.pattern_match.group(1))
    mid = int(event.pattern_match.group(2))
    owner_id = int(event.pattern_match.group(3))
    if await _deny_if_not_owner(event, owner_id):
        return
    if not _check_file_rate_limit(event.sender_id):
        await event.answer(
            f"⏳ Rate limit reached — {FILE_RATE_LIMIT_WINDOW}s me max "
            f"{FILE_RATE_LIMIT_COUNT} files allowed hain.",
            alert=True,
        )
        return
    r = db.execute(
        "SELECT * FROM notes WHERE channel_id=? AND msg_id=?", (cid, mid)
    ).fetchone()
    if not r:
        await event.answer(
            "Ye note ab index me nahi hai (purana result tha) — dobara search karein.",
            alert=True,
        )
        return
    await event.answer()
    await _track_user(event, "file")
    await send_result(event.chat_id, Result(r, 0.0))


# ---------------------------------------------------------------- events

@bot.on(events.Album)
async def on_album(event):
    """Media group (album) ko ek hi note ke roop me index karo."""
    text = " ".join(m.message or "" for m in event.messages)
    first = event.messages[0]
    if first.chat_id == await channel_id():
        add_note(first, text, first.chat_id)


async def channel_id():
    if not CHANNEL:
        return None
    if CHANNEL not in _chan_cache:
        try:
            ent = await bot.get_entity(CHANNEL)
        except ValueError:
            if user:
                ent = await user.get_entity(CHANNEL)
            else:
                return None
        _chan_cache[CHANNEL] = utils.get_peer_id(ent)
    return _chan_cache[CHANNEL]


_chan_cache = {}


@bot.on(events.NewMessage(chats=CHANNEL if CHANNEL else None))
async def on_channel_post(event):
    """Bot channel me admin hai -> har naya post auto-index."""
    if event.is_channel:
        add_note(event.message, event.message.message, event.chat_id)


@bot.on(events.NewMessage(pattern=r"^/(start|help)(@\w+)?"))
async def on_start(event):
    text = (
        "📚 **CA Notes Search Bot**\n"
        "Channel ke notes turant search karein aur access karein.\n\n"
        "**Kaise use karein:**\n"
        "• `/find <topic>` — e.g. `/find costing ch 5 marginal costing`\n"
        "• Inline mode: `@yourbotname <topic>` (kisi bhi chat me)"
    )
    if event.is_private:
        text += "\n• Yahan DM me seedha topic type karein, `/find` likhna zaroori nahi"
    if is_admin(event):
        text += (
            "\n\n**Admin Commands:**\n"
            "• `/stats` — usage statistics aur active-user trend\n"
            "• `/debug` — configuration status (search kaam na kare tab check karein)\n"
            "• `/settings` — rate limits, page size waghera bot ke through hi "
            "change karein (bina .env edit kiye)\n"
            "• `/setconfig <KEY> <VALUE>` — settings ko ek line me set karein\n"
            "• `/admins`, `/addadmin <id>`, `/deladmin <id>` — admin list manage "
            "karein (kisi user ko reply karke bina id ke bhi chalta hai)\n"
            "• `/reindex` — channel ki missing history index karein "
            "(USER_SESSION zaroori hai)\n"
            "• `/broadcast [--g] [--f] [--p]` — kisi message ko reply karke "
            "sabko bhejein (details ke liye bina reply ke `/broadcast` chalayein)"
        )
    await event.respond(text, link_preview=False)


@bot.on(events.NewMessage(pattern=r"^/find(@\w+)?\s+(.+)"))
async def on_find(event):
    # DM me bot hamesha kaam karega. GROUP_ID sirf groups ke liye restrict
    # karta hai (agar set hai to bot un dusre groups me chup rahega,
    # lekin private chat kabhi block nahi hogi).
    if GROUP_ID and not event.is_private and event.chat_id != int(GROUP_ID):
        return
    if not _check_rate_limit(event.sender_id):
        await _rate_limit_notice(event.respond, event.sender_id)
        return
    query = event.pattern_match.group(2).strip()
    await _track_user(event, "search")
    async with bot.action(event.chat_id, "typing"):
        await reply_search(event.chat_id, query, event.sender_id)


@bot.on(events.NewMessage(
    func=lambda e: e.is_private and not e.out and bool((e.raw_text or "").strip())
    and not e.raw_text.strip().startswith("/")
))
async def on_dm_plain_text(event):
    """DM me user seedha query type kare (bina /find likhe) to bhi search
    ho jaye — groups me ye kaam nahi karta (wahan spam ho jayega),
    isliye sirf private chat tak limited hai. `not e.out` zaroori hai
    warna bot apne hi bheje results ko naye query samajh ke loop kar dega."""
    # Agar admin ne `/settings` se koi setting edit karne ke liye tap kiya
    # tha, to ye agla message uska naya value hai — search mat karo.
    pending_key = _pending_setting_edit.get(event.sender_id)
    if pending_key and is_admin(event):
        del _pending_setting_edit[event.sender_id]
        ok, msg = _apply_setting(pending_key, event.raw_text.strip())
        await event.respond(msg)
        return

    if not _check_rate_limit(event.sender_id):
        await _rate_limit_notice(event.respond, event.sender_id)
        return
    query = event.raw_text.strip()
    await _track_user(event, "search")
    async with bot.action(event.chat_id, "typing"):
        await reply_search(event.chat_id, query, event.sender_id)


def _last_n_days(n: int = 7):
    """Aaj samet peeche ke n dino ki 'YYYY-MM-DD' list, oldest pehle."""
    today = datetime.now().date()
    return [(today - timedelta(days=i)).isoformat() for i in range(n - 1, -1, -1)]


def _day_label(day_str: str) -> str:
    """'2026-09-09' -> 'Tue 09' (chat me short/readable dikhane ke liye)."""
    return datetime.strptime(day_str, "%Y-%m-%d").strftime("%a %d")


def _bar(value: int, vmax: int, width: int = 10) -> str:
    """Text-only horizontal bar (Telegram me image nahi bhej sakte,
    isliye block characters se mini bar-chart banate hain)."""
    if vmax <= 0:
        return "░" * width
    filled = round((value / vmax) * width)
    return "█" * filled + "░" * (width - filled)


async def _daily_active_counts(days: list) -> dict:
    """days: 'YYYY-MM-DD' strings. Returns {day: distinct_active_users}."""
    if mongo_daily is not None:
        counts = {}
        for d in days:
            counts[d] = await mongo_daily.count_documents({"day": d})
        return counts
    placeholders = ",".join("?" * len(days))
    rows = db.execute(
        f"SELECT day, COUNT(DISTINCT user_id) c FROM daily_activity "
        f"WHERE day IN ({placeholders}) GROUP BY day",
        days,
    ).fetchall()
    found = {r["day"]: r["c"] for r in rows}
    return {d: found.get(d, 0) for d in days}


async def _distinct_active_in_range(start_day: str, end_day_exclusive: str) -> int:
    """start_day tak (inclusive), end_day tak (exclusive) — distinct users."""
    if mongo_daily is not None:
        users_set = await mongo_daily.distinct(
            "user_id", {"day": {"$gte": start_day, "$lt": end_day_exclusive}}
        )
        return len(users_set)
    row = db.execute(
        "SELECT COUNT(DISTINCT user_id) c FROM daily_activity "
        "WHERE day>=? AND day<?",
        (start_day, end_day_exclusive),
    ).fetchone()
    return row["c"]


@bot.on(events.NewMessage(pattern=r"^/stats(@\w+)?"))
async def on_stats(event):
    if await _deny_if_not_admin(event):
        return
    n = db.execute("SELECT COUNT(*) c FROM notes").fetchone()["c"]

    if mongo_users is not None:
        total_users = await mongo_users.count_documents({})
        agg = await mongo_users.aggregate([
            {"$group": {"_id": None,
                        "searches": {"$sum": "$search_count"},
                        "files": {"$sum": "$file_count"}}}
        ]).to_list(length=1)
        total_searches = agg[0]["searches"] if agg else 0
        total_files = agg[0]["files"] if agg else 0
        top_docs = await mongo_users.aggregate([
            {"$addFields": {"_activity": {"$add": [
                {"$ifNull": ["$search_count", 0]},
                {"$ifNull": ["$file_count", 0]},
            ]}}},
            {"$sort": {"_activity": -1}},
            {"$limit": 5},
        ]).to_list(length=5)
        top = [
            {"user_id": d["_id"], "username": d.get("username"),
             "search_count": d.get("search_count", 0),
             "file_count": d.get("file_count", 0)}
            for d in top_docs
        ]
    else:
        total_users = db.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        total_searches = db.execute(
            "SELECT COALESCE(SUM(search_count),0) c FROM users").fetchone()["c"]
        total_files = db.execute(
            "SELECT COALESCE(SUM(file_count),0) c FROM users").fetchone()["c"]
        top = db.execute(
            "SELECT user_id, username, search_count, file_count FROM users "
            "ORDER BY (search_count + file_count) DESC LIMIT 5"
        ).fetchall()

    # ---- Daily/Weekly Active Users trend (last 7 din) ----
    days = _last_n_days(7)
    daily_counts = await _daily_active_counts(days)
    vmax = max(daily_counts.values()) if daily_counts else 0

    today = datetime.now().date()
    this_week_start = (today - timedelta(days=6)).isoformat()
    next_day = (today + timedelta(days=1)).isoformat()
    prev_week_start = (today - timedelta(days=13)).isoformat()

    wau_this = await _distinct_active_in_range(this_week_start, next_day)
    wau_prev = await _distinct_active_in_range(prev_week_start, this_week_start)
    if wau_prev > 0:
        change = ((wau_this - wau_prev) / wau_prev) * 100
        if change > 0:
            trend = f"↑ {change:.0f}% vs pichla hafta"
        elif change < 0:
            trend = f"↓ {abs(change):.0f}% vs pichla hafta"
        else:
            trend = "→ same as pichla hafta"
    else:
        trend = "↑ naya" if wau_this else "—"

    lines = [
        "📊 **Bot Stats**",
        "━━━━━━━━━━━━━━━━━━",
        f"📚 Notes indexed: **{n}**",
        f"👥 Unique users (all-time): **{total_users}**",
        f"🔍 Total searches: **{total_searches}**",
        f"📁 Total file downloads: **{total_files}**",
        f"💾 Backend: {'MongoDB' if mongo_users is not None else 'SQLite'}",
        "",
        "📈 **Daily Active Users** (pichle 7 din)",
    ]
    for d in days:
        c = daily_counts.get(d, 0)
        lines.append(f"`{_day_label(d)}` {_bar(c, vmax)} {c}")
    lines.append(f"\n📅 Weekly active users: **{wau_this}** ({trend})")

    if top:
        lines.append("\n🏆 **Top active users**")
        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        for i, u in enumerate(top, 1):
            rank = medals.get(i, f"{i}.")
            name = f"@{u['username']}" if u["username"] else f"`{u['user_id']}`"
            lines.append(
                f"{rank} {name} — 🔍{u['search_count']} searches · "
                f"📁{u['file_count']} files"
            )

    await event.respond("\n".join(lines), link_preview=False)


@bot.on(events.NewMessage(pattern=r"^/debug(@\w+)?"))
async def on_debug(event):
    if await _deny_if_not_admin(event):
        return
    total = db.execute("SELECT COUNT(*) c FROM notes").fetchone()["c"]
    last = db.execute("SELECT MAX(date) d FROM notes").fetchone()["d"]
    last_str = (
        time.strftime("%d %b %Y %H:%M", time.localtime(last)) if last else "—"
    )
    lines = ["🔧 **Debug Status**", f"• Index me notes: **{total}**",
             f"• Last indexed: {last_str}"]
    lines.append(f"• CHANNEL set: {'✅ ' + CHANNEL if CHANNEL else '❌ khali hai (.env me bharo)'}")
    lines.append(f"• USER_SESSION: {'✅ hai' if SESSION_STRING else '❌ nahi hai (purane notes index nahi honge)'}")
    lines.append(f"• GROUP_ID filter: {GROUP_ID if GROUP_ID else 'off (sab groups me chalta hai)'} (DM me bot hamesha kaam karta hai)")
    lines.append(f"• ADMIN_IDS: {len(ADMIN_IDS)} configured" if ADMIN_IDS
                 else "• ADMIN_IDS: ❌ khali hai — admin commands abhi kisi ke liye kaam nahi karenge")
    lines.append(f"• Rate limit: {RATE_LIMIT_COUNT} searches / {RATE_LIMIT_WINDOW}s per user (admins exempt)")
    lines.append(f"• File rate limit: {FILE_RATE_LIMIT_COUNT} files / {FILE_RATE_LIMIT_WINDOW}s per user (admins exempt)")
    lines.append(f"• User-data backend: {'MongoDB (' + MONGO_DB_NAME + ')' if mongo_users is not None else 'SQLite (local file)'}")
    if CHANNEL:
        cid = await channel_id()
        if cid:
            n_ch = db.execute("SELECT COUNT(*) c FROM notes WHERE channel_id=?",
                              (cid,)).fetchone()["c"]
            lines.append(f"• Channel ke indexed notes: **{n_ch}**")
            if n_ch == 0:
                lines.append(
                    "⚠️ Channel se abhi tak ek bhi note index nahi hua. "
                    "Verify karein: bot channel me **admin** hai? "
                    "Session string set hai? "
                    "Channel ke posts me caption/text hai?"
                )
        else:
            lines.append("⚠️ CHANNEL resolve nahi hua — bot channel ka username/ID "
                         "nahi dekh pa raha. Bot ko channel me add karein ya ID "
                         "numeric (-100...) form me likhein.")
    await event.respond("\n".join(lines), link_preview=False)


@bot.on(events.NewMessage(pattern=r"^/reindex(@\w+)?"))
async def on_reindex(event):
    if await _deny_if_not_admin(event):
        return
    if not user or not user.is_connected():
        await event.respond(no_session_msg())
        return
    me = await user.get_me() if await user.is_user_authorized() else None
    if me and getattr(me, "bot", False):
        await event.respond(
            "⚠️ Ye session ek bot ki hai — history backfill bot account se nahi hota.\n"
            "`python setup.py` chala kar apne user account se session banayein."
        )
        return
    await event.respond("🔄 Channel history index ho rahi hai — thoda time lagega.")
    count, scanned = await backfill()
    if count is None:
        await event.respond("❌ CHANNEL resolve nahi hua user session se — `.env` check karein.")
        return
    await event.respond(f"✅ **Reindex Complete** — {count} naye notes index hue ({scanned} messages scan hue).")


@bot.on(events.NewMessage(pattern=r"^/broadcast(@\w+)?(\s+.*)?$"))
async def on_broadcast(event):
    """Admin kisi bhi message (text/sticker/photo/document/poll — kuch bhi)
    ko REPLY karke `/broadcast [flags]` chalaye, wahi message as-is
    (formatting/media sab intact) sabko bhej diya jaata hai.

    Default: sabhi individual USERS ko DM me bhejta hai.

    Flags:
      --g   GROUP ko bhi bhejo, users ke saath (dono — DM + group)
      --f   FORWARD karo ("Forwarded from" tag ke saath); default = clean copy
      --p   Bhejne ke baad us chat me PIN bhi kar do
    """
    if await _deny_if_not_admin(event):
        return

    reply = await event.get_reply_message()
    if not reply:
        await event.respond(
            "⚠️ Jo message broadcast karna hai, usko **reply** karke "
            "`/broadcast` chalayein.\n\n"
            "Default: sabhi individual **users** ko DM me bheja jaata hai.\n\n"
            "**Flags:**\n"
            "• `--g` — group ko bhi bhejein (users ke saath dono)\n"
            "• `--f` — forward karein (\"Forwarded from\" tag ke saath); "
            "default me clean copy jaati hai\n"
            "• `--p` — bhejne ke baad us chat me pin bhi kar dein\n\n"
            "Example: `/broadcast --g --p`"
        )
        return

    args = (event.pattern_match.group(2) or "").split()
    use_group_too = "--g" in args
    use_forward = "--f" in args
    use_pin = "--p" in args

    if mongo_users is not None:
        targets = [doc["_id"] async for doc in mongo_users.find({}, {"_id": 1})]
    else:
        targets = [r["user_id"] for r in db.execute("SELECT user_id FROM users").fetchall()]
    group_warning = ""
    if use_group_too:
        if GROUP_ID:
            targets.append(int(GROUP_ID))
        else:
            group_warning = (
                "\n⚠️ `--g` diya gaya tha, lekin `.env` me `GROUP_ID` set nahi hai "
                "— isliye sirf users ko bheja gaya."
            )
    if not targets:
        await event.respond(
            "⚠️ Abhi tak koi user tracked nahi hai — kisi ne bhi search/find "
            "use nahi kiya, isliye DM list khali hai."
            + group_warning
        )
        return

    status = await event.respond(f"📢 **Broadcast shuru ho raha hai** — {len(targets)} recipients{group_warning}")
    sent, failed = 0, 0
    for i, chat_id in enumerate(targets, 1):
        for attempt in range(2):  # ek retry FloodWait ke baad
            try:
                if use_forward:
                    sent_msg = await bot.forward_messages(chat_id, reply)
                else:
                    sent_msg = await bot.send_message(chat_id, reply)
                if use_pin:
                    m = sent_msg[0] if isinstance(sent_msg, list) else sent_msg
                    try:
                        await bot.pin_message(chat_id, m, notify=False)
                    except Exception:
                        pass  # pin fail ho (permission waghera) to bhi msg to gaya
                sent += 1
                break
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 1)
                continue
            except Exception:
                failed += 1
                break
        await asyncio.sleep(0.05)  # thoda gap, Telegram flood limits se bachne ke liye
        if i % 25 == 0:
            try:
                await status.edit(f"📢 Broadcast in progress — {i}/{len(targets)} "
                                   f"({sent} sent, {failed} failed)")
            except Exception:
                pass

    await status.edit(
        f"✅ **Broadcast Complete**\n"
        f"Sent: **{sent}** · Failed: **{failed}** · Total recipients: {len(targets)}"
        + group_warning
    )


def no_session_msg():
    return (
        "⚠️ Purani history index karne ke liye USER_SESSION zaroori hai.\n"
        "README dekhein: `python make_session.py` chala kar session banayein, "
        "`.env` me SESSION_STRING add karein, phir bot restart karein."
    )


async def backfill():
    """Channel ka history index karo — poora nahi, sirf last indexed
    message ke BAAD wale (incremental), taaki repeat runs fast ho."""
    try:
        entity = await user.get_entity(CHANNEL)
    except Exception:
        return None, 0
    cid = utils.get_peer_id(entity)  # marked id (-100...), bot events jaise hi
    row = db.execute(
        "SELECT MAX(msg_id) m FROM notes WHERE channel_id=?", (cid,)
    ).fetchone()
    min_id = (row["m"] or 0) if row else 0
    count, scanned = 0, 0
    async for msg in user.iter_messages(entity, min_id=min_id, reverse=True):
        if add_note(msg, msg.message, cid):
            count += 1
        scanned += 1
        if scanned % 100 == 0:
            await asyncio.sleep(1)  # flood control
    return count, scanned


async def auto_backfill_on_start():
    """Startup pe automatic: agar SESSION_STRING hai to channel ka
    missing history khud index kar lo (pehli baar poora, baad me sirf naya)."""
    if not user:
        return
    await user.connect()
    if not await user.is_user_authorized():
        print("⚠️ SESSION_STRING valid nahi hai — backfill off rahega.")
        return
    me = await user.get_me()
    if getattr(me, "bot", False):
        print("⚠️ Ye SESSION_STRING ek BOT ki hai (user account ki nahi)!\n"
              "   Bots ko Telegram channel history read karne ki permission\n"
              "   nahi deta, isliye purane notes nahi milenge.\n"
               "   Fix: `python setup.py` chalao aur APNE phone number se\n"
              "   login karke user session banao. Bot baaki sab kaam karta rahega.")
        return
    if not CHANNEL:
        print("⚠️ CHANNEL .env me set nahi hai — backfill skip.")
        return
    print("🔄 Startup backfill chal raha hai (missing old notes)...")
    try:
        count, scanned = await backfill()
        if count is None:
            print("❌ CHANNEL resolve nahi hua user session se.")
        else:
            print(f"✅ Backfill done: {count} naye notes index hue ({scanned} scanned).")
    except Exception as e:
        # Backfill fail ho to bhi bot chalta rahe (naye posts to pakdega hi)
        print(f"⚠️ Backfill fail hua ({type(e).__name__}: {e})\n"
              "   Bot chalta rahega; baad me /reindex try karo.")


# ---------------------------------------------------------------- inline

async def inline_handler(event):
    builder = event.builder
    results = []
    if event.text:
        for res in search(event.text, limit=MAX_RESULTS):
            r = res.row
            body = r["title"] + ("\n" + r["text"][:300] if r["text"] else "")
            results.append(builder.article(r["title"][:80], text=body))
    if results:
        await event.answer(results)
    else:
        await event.answer(
            [builder.article("Koi result nahi mila", text="Doosre keywords try karein.")],
            cache_time=1,
        )


bot.add_event_handler(inline_handler, events.InlineQuery())


async def main():
    await auto_backfill_on_start()
    print("✅ Notes Search Bot chal raha hai...")
    await bot.run_until_disconnected()


if __name__ == "__main__":
    bot.loop.run_until_complete(main())
