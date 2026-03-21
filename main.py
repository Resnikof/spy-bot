"""
Legal Archive Bot v6
• Скачивает фото/видео/голосовые/кружочки/документы на диск
• Структура: archives/{owner_id}/{chat_id}_{name}/chat.json + media/
• Уведомления об удалении и редактировании (фикс)
• Промокоды, подписка, админ-панель
"""

import logging
import aiosqlite
import aiofiles
import json
import re
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

from telegram import (
    Update, Message,
    InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand,
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    BusinessConnectionHandler, CallbackQueryHandler,
    ContextTypes, TypeHandler, filters,
)

# ══════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════

from config import (
    BOT_TOKEN, ADMIN_IDS, ADMIN_PROMO_KEY,
    SERVER_URL, DB_FILE, PROMOS
)
ARCHIVE_DIR = Path("archives")
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s — %(message)s", level=logging.INFO)
logger = logging.getLogger("LegalBot")


# ══════════════════════════════════════════════════════
#  УТИЛИТЫ
# ══════════════════════════════════════════════════════

def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def now_dt() -> datetime:
    return datetime.now(timezone.utc)

def safe_name(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", str(name))[:40]

def esc(text: str) -> str:
    for ch in ["_", "*", "`", "["]:
        text = str(text).replace(ch, f"\\{ch}")
    return text

def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

def detect_media(msg: Message) -> str:
    if msg.photo:      return "📷 фото"
    if msg.video:      return "🎥 видео"
    if msg.voice:      return "🎙 голосовое"
    if msg.document:   return "📎 документ"
    if msg.sticker:    return "🎭 стикер"
    if msg.audio:      return "🎵 аудио"
    if msg.video_note: return "📹 кружочек"
    return ""

def sender_info(msg: Message) -> tuple[str, str, int | None]:
    s = msg.from_user
    if not s:
        return "неизвестно", "—", None
    return (f"{s.first_name or ''} {s.last_name or ''}".strip(),
            f"@{s.username}" if s.username else "—", s.id)


# ══════════════════════════════════════════════════════
#  СКАЧИВАНИЕ МЕДИА
# ══════════════════════════════════════════════════════

async def download_media(msg: Message, owner_id: int, chat_id: int, chat_name: str) -> str | None:
    """Скачать медиафайл, вернуть относительный путь или None."""
    media_dir = ARCHIVE_DIR / str(owner_id) / f"{chat_id}_{safe_name(chat_name)}" / "media"
    media_dir.mkdir(parents=True, exist_ok=True)

    file_obj  = None
    extension = "bin"

    if msg.photo:
        file_obj  = await msg.photo[-1].get_file()
        extension = "jpg"
    elif msg.video:
        file_obj  = await msg.video.get_file()
        extension = "mp4"
    elif msg.voice:
        file_obj  = await msg.voice.get_file()
        extension = "ogg"
    elif msg.video_note:
        file_obj  = await msg.video_note.get_file()
        extension = "mp4"
    elif msg.audio:
        file_obj  = await msg.audio.get_file()
        extension = msg.audio.mime_type.split("/")[-1] if msg.audio.mime_type else "mp3"
    elif msg.document:
        file_obj  = await msg.document.get_file()
        fname     = msg.document.file_name or f"doc_{msg.message_id}"
        extension = fname.rsplit(".", 1)[-1] if "." in fname else "bin"
    elif msg.sticker:
        file_obj  = await msg.sticker.get_file()
        extension = "webp"

    if not file_obj:
        return None

    filename  = f"{msg.message_id}_{msg.date.strftime('%H%M%S') if msg.date else 'x'}.{extension}"
    save_path = media_dir / filename
    await file_obj.download_to_drive(str(save_path))
    return str(save_path)


# ══════════════════════════════════════════════════════
#  ФАЙЛОВЫЙ АРХИВ
# ══════════════════════════════════════════════════════

def chat_folder(owner_id: int, chat_id: int, chat_name: str) -> Path:
    d = ARCHIVE_DIR / str(owner_id) / f"{chat_id}_{safe_name(chat_name)}"
    d.mkdir(parents=True, exist_ok=True)
    return d

async def _json_append(path: Path, record: dict):
    data = []
    if path.exists():
        try:
            async with aiofiles.open(path, "r", encoding="utf-8") as f:
                c = await f.read()
                data = json.loads(c) if c.strip() else []
        except Exception:
            data = []
    data.append(record)
    async with aiofiles.open(path, "w", encoding="utf-8") as f:
        await f.write(json.dumps(data, ensure_ascii=False, indent=2))

async def file_append_message(owner_id, chat_id, chat_name, record):
    await _json_append(chat_folder(owner_id, chat_id, chat_name) / "chat.json", record)

async def file_append_event(owner_id, chat_id, chat_name, record):
    await _json_append(chat_folder(owner_id, chat_id, chat_name) / "events.json", record)

async def file_get_chat(owner_id: int, chat_id: int) -> list[dict]:
    p = ARCHIVE_DIR / str(owner_id)
    if not p.exists():
        return []
    for d in p.iterdir():
        if d.is_dir() and d.name.startswith(f"{chat_id}_"):
            path = d / "chat.json"
            if path.exists():
                async with aiofiles.open(path, "r", encoding="utf-8") as f:
                    c = await f.read()
                    return json.loads(c) if c.strip() else []
    return []

async def file_get_all_chats(owner_id: int) -> list[dict]:
    p = ARCHIVE_DIR / str(owner_id)
    result = []
    if not p.exists():
        return result
    for d in sorted(p.iterdir()):
        if not d.is_dir():
            continue
        msgs, evs = [], []
        cj = d / "chat.json"
        ej = d / "events.json"
        if cj.exists():
            async with aiofiles.open(cj, "r", encoding="utf-8") as f:
                c = await f.read()
                msgs = json.loads(c) if c.strip() else []
        if ej.exists():
            async with aiofiles.open(ej, "r", encoding="utf-8") as f:
                c = await f.read()
                evs = json.loads(c) if c.strip() else []
        # Считаем медиафайлы
        media_dir   = d / "media"
        media_count = len(list(media_dir.iterdir())) if media_dir.exists() else 0
        parts = d.name.split("_", 1)
        result.append({
            "chat_id":      parts[0],
            "chat_name":    parts[1].replace("_", " ") if len(parts) > 1 else parts[0],
            "msg_count":    len(msgs),
            "events_count": len(evs),
            "media_count":  media_count,
            "last_msg":     msgs[-1] if msgs else None,
            "folder":       d.name,
        })
    return result


# ══════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ══════════════════════════════════════════════════════

async def db_init():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT, full_name TEXT,
            registered_at TEXT,
            sub_until TEXT,
            is_admin INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS business_connections (
            connection_id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            connected_at TEXT
        );
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL, chat_name TEXT,
            message_id INTEGER NOT NULL,
            from_id INTEGER, from_name TEXT, from_username TEXT,
            text TEXT, media_type TEXT, media_path TEXT, sent_at TEXT,
            UNIQUE(owner_id, chat_id, message_id)
        );
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id INTEGER NOT NULL, event_type TEXT,
            chat_id INTEGER, chat_name TEXT, message_id INTEGER,
            original_text TEXT, new_text TEXT,
            from_id INTEGER, from_name TEXT, from_username TEXT,
            happened_at TEXT
        );
        CREATE TABLE IF NOT EXISTS promos (
            code TEXT PRIMARY KEY,
            days INTEGER NOT NULL,
            max_uses INTEGER DEFAULT -1,
            used_count INTEGER DEFAULT 0,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS promo_uses (
            user_id INTEGER, code TEXT,
            PRIMARY KEY(user_id, code)
        );
        """)
        await db.commit()
        for code, days in PROMOS.items():
            await db.execute("INSERT OR IGNORE INTO promos VALUES (?,?,-1,0,?)", (code, days, ts()))
        await db.commit()
    logger.info("БД готова.")

async def db_get_user(uid: int) -> dict | None:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(
            "SELECT user_id,username,full_name,registered_at,sub_until,is_admin FROM users WHERE user_id=?", (uid,)
        ) as cur:
            row = await cur.fetchone()
            if not row: return None
            return {"user_id": row[0], "username": row[1], "full_name": row[2],
                    "registered_at": row[3], "sub_until": row[4], "is_admin": row[5]}

async def db_register_user(uid, uname, fname):
    async with aiosqlite.connect(DB_FILE) as db:
        adm = 1 if uid in ADMIN_IDS else 0
        await db.execute(
            "INSERT OR IGNORE INTO users (user_id,username,full_name,registered_at,is_admin) VALUES (?,?,?,?,?)",
            (uid, uname, fname, ts(), adm)
        )
        await db.commit()

async def db_is_registered(uid) -> bool:
    return await db_get_user(uid) is not None

async def db_has_sub(uid: int) -> bool:
    if uid in ADMIN_IDS: return True
    u = await db_get_user(uid)
    if not u or not u["sub_until"]: return False
    try:
        return datetime.fromisoformat(u["sub_until"]) > now_dt()
    except Exception:
        return False

async def db_sub_until(uid: int) -> str:
    u = await db_get_user(uid)
    if not u or not u["sub_until"]: return "нет"
    try:
        exp = datetime.fromisoformat(u["sub_until"])
        if exp < now_dt(): return "истекла"
        return f"{exp.strftime('%Y-%m-%d')} (осталось {(exp - now_dt()).days} дн.)"
    except Exception:
        return u["sub_until"]

async def db_give_sub(uid: int, days: int):
    u    = await db_get_user(uid)
    base = now_dt()
    if u and u["sub_until"]:
        try:
            exp = datetime.fromisoformat(u["sub_until"])
            if exp > base: base = exp
        except Exception:
            pass
    new_exp = (base + timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE users SET sub_until=? WHERE user_id=?", (new_exp, uid))
        await db.commit()

async def db_set_admin(uid: int, val: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE users SET is_admin=? WHERE user_id=?", (val, uid))
        await db.commit()

async def db_get_all_users(limit=500) -> list[dict]:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(
            "SELECT user_id,username,full_name,registered_at,sub_until,is_admin FROM users ORDER BY registered_at DESC LIMIT ?", (limit,)
        ) as cur:
            rows = await cur.fetchall()
            return [{"user_id": r[0], "username": r[1], "full_name": r[2],
                     "registered_at": r[3], "sub_until": r[4], "is_admin": r[5]} for r in rows]

async def db_add_conn(conn_id, uid):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR REPLACE INTO business_connections VALUES (?,?,?)", (conn_id, uid, ts()))
        await db.commit()

async def db_del_conn(conn_id):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM business_connections WHERE connection_id=?", (conn_id,))
        await db.commit()

async def db_get_owner(conn_id) -> int | None:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT user_id FROM business_connections WHERE connection_id=?", (conn_id,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else None

async def db_get_conns(uid) -> list:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT connection_id,connected_at FROM business_connections WHERE user_id=?", (uid,)) as cur:
            return await cur.fetchall()

async def db_save_msg(owner_id, msg: Message, media_path: str | None = None):
    s     = msg.from_user
    name  = f"{s.first_name or ''} {s.last_name or ''}".strip() if s else "?"
    uname = f"@{s.username}" if s and s.username else "—"
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO messages "
            "(owner_id,chat_id,chat_name,message_id,from_id,from_name,from_username,text,media_type,media_path,sent_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (owner_id, msg.chat.id, msg.chat.full_name or str(msg.chat.id),
             msg.message_id, s.id if s else None, name, uname,
             msg.text or msg.caption or "", detect_media(msg), media_path,
             msg.date.strftime("%Y-%m-%d %H:%M:%S UTC") if msg.date else ts())
        )
        await db.commit()

async def db_get_msg(owner_id, chat_id, msg_id) -> dict | None:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(
            "SELECT from_id,from_name,from_username,text,media_type,media_path,sent_at FROM messages "
            "WHERE owner_id=? AND chat_id=? AND message_id=?",
            (owner_id, chat_id, msg_id)
        ) as cur:
            row = await cur.fetchone()
            return {"from_id": row[0], "from_name": row[1], "from_username": row[2],
                    "text": row[3], "media_type": row[4], "media_path": row[5], "sent_at": row[6]} if row else None

async def db_save_event(owner_id, etype, chat_id, chat_name, msg_id, orig, new, from_id, fname, funame):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT INTO events (owner_id,event_type,chat_id,chat_name,message_id,"
            "original_text,new_text,from_id,from_name,from_username,happened_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (owner_id, etype, chat_id, chat_name, msg_id, orig, new, from_id, fname, funame, ts())
        )
        await db.commit()

async def db_get_events(owner_id, limit=20) -> list[dict]:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(
            "SELECT event_type,chat_name,message_id,original_text,new_text,from_id,from_name,happened_at"
            " FROM events WHERE owner_id=? ORDER BY happened_at DESC LIMIT ?",
            (owner_id, limit)
        ) as cur:
            return [{"type": r[0], "chat_name": r[1], "mid": r[2], "original": r[3],
                     "new": r[4], "from_id": r[5], "from_name": r[6], "at": r[7]}
                    for r in await cur.fetchall()]

async def db_get_promo(code) -> dict | None:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT code,days,used_count FROM promos WHERE code=?", (code,)) as cur:
            row = await cur.fetchone()
            return {"code": row[0], "days": row[1], "used_count": row[2]} if row else None

async def db_use_promo(uid, code) -> tuple[bool, str]:
    promo = await db_get_promo(code)
    if not promo: return False, "❌ Промокод не найден."
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT 1 FROM promo_uses WHERE user_id=? AND code=?", (uid, code)) as cur:
            if await cur.fetchone(): return False, "❌ Вы уже использовали этот промокод."
        await db.execute("INSERT INTO promo_uses VALUES (?,?)", (uid, code))
        await db.execute("UPDATE promos SET used_count=used_count+1 WHERE code=?", (code,))
        await db.commit()
    await db_give_sub(uid, promo["days"])
    return True, f"✅ Промокод активирован! +{promo['days']} дней подписки."

async def db_add_promo(code, days):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR REPLACE INTO promos VALUES (?,?,0,?)", (code, days, ts()))
        await db.commit()

async def db_del_promo(code):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM promos WHERE code=?", (code,))
        await db.commit()

async def db_get_all_promos() -> list[dict]:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT code,days,used_count,created_at FROM promos ORDER BY created_at DESC") as cur:
            return [{"code": r[0], "days": r[1], "used_count": r[2], "created_at": r[3]}
                    for r in await cur.fetchall()]


# ══════════════════════════════════════════════════════
#  АРХИВИРОВАНИЕ
# ══════════════════════════════════════════════════════

async def archive_message(owner_id: int, msg: Message):
    """Сохранить сообщение + скачать медиа."""
    cname = msg.chat.full_name or str(msg.chat.id)

    # Скачиваем медиа
    media_path = None
    if any([msg.photo, msg.video, msg.voice, msg.video_note,
            msg.audio, msg.document, msg.sticker]):
        try:
            media_path = await download_media(msg, owner_id, msg.chat.id, cname)
        except Exception as e:
            logger.warning(f"Медиа не скачалось: {e}")

    await db_save_msg(owner_id, msg, media_path)

    s     = msg.from_user
    name  = f"{s.first_name or ''} {s.last_name or ''}".strip() if s else "?"
    uname = f"@{s.username}" if s and s.username else "—"

    record = {
        "message_id":    msg.message_id,
        "from_id":       s.id if s else None,
        "from_name":     name,
        "from_username": uname,
        "text":          msg.text or msg.caption or "",
        "media":         detect_media(msg),
        "media_path":    media_path,
        "sent_at":       msg.date.strftime("%Y-%m-%d %H:%M:%S UTC") if msg.date else ts(),
        "edited":        False,
        "deleted":       False,
    }
    await file_append_message(owner_id, msg.chat.id, cname, record)

async def archive_event(owner_id, etype, chat_id, chat_name,
                        msg_id, orig, new, from_id, fname, funame):
    await db_save_event(owner_id, etype, chat_id, chat_name,
                        msg_id, orig, new, from_id, fname, funame)
    await file_append_event(owner_id, chat_id, chat_name, {
        "event_type": etype, "message_id": msg_id,
        "from_id": from_id, "from_name": fname, "from_username": funame,
        "original_text": orig, "new_text": new, "happened_at": ts(),
    })


# ══════════════════════════════════════════════════════
#  КЛАВИАТУРЫ
# ══════════════════════════════════════════════════════

def kb_main(uid: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📂 Мои чаты",     callback_data="menu:chats"),
         InlineKeyboardButton("⚠️ События",       callback_data="menu:events")],
        [InlineKeyboardButton("🔑 Промокод",      callback_data="menu:promo"),
         InlineKeyboardButton("👤 Мой профиль",   callback_data="menu:profile")],
        [InlineKeyboardButton("🌐 Веб-панель",
                              web_app=WebAppInfo(url=f"{SERVER_URL}/?uid={uid}"))],
    ]
    if is_admin(uid):
        rows.append([InlineKeyboardButton("🛡 Админ-панель", callback_data="admin:panel")])
    return InlineKeyboardMarkup(rows)

def kb_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 Пользователи",    callback_data="admin:users:0"),
         InlineKeyboardButton("🔑 Промокоды",       callback_data="admin:promos")],
        [InlineKeyboardButton("➕ Создать промокод", callback_data="admin:newpromo"),
         InlineKeyboardButton("🎁 Выдать подписку", callback_data="admin:givedays")],
        [InlineKeyboardButton("◀️ Главное меню",    callback_data="menu:main")],
    ])

def kb_back_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="menu:main")]])

def kb_back_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Панель", callback_data="admin:panel")]])


# ══════════════════════════════════════════════════════
#  КОМАНДЫ
# ══════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await db_register_user(user.id, user.username or "",
                           f"{user.first_name or ''} {user.last_name or ''}".strip())
    has_sub = await db_has_sub(user.id)
    sub_str = await db_sub_until(user.id)
    status  = f"✅ Подписка до: {sub_str}" if has_sub else "❌ Подписки нет — введите промокод"
    adm_str = " 🛡 Админ" if is_admin(user.id) else ""

    # Удалить предыдущее меню если есть
    prev_id = context.user_data.get("menu_msg_id")
    if prev_id:
        try:
            await context.bot.delete_message(chat_id=user.id, message_id=prev_id)
        except Exception:
            pass

    sent = await update.message.reply_text(
        f"👋 Привет, {esc(user.first_name or 'друг')}!{adm_str}\n"
        f"🆔 ID: {user.id}\n📊 {status}\n\nВыберите действие:",
        reply_markup=kb_main(user.id),
    )
    try:
        await update.message.delete()
    except Exception:
        pass
    context.user_data["menu_msg_id"] = sent.message_id

async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await db_register_user(user.id, user.username or "",
                           f"{user.first_name or ''} {user.last_name or ''}".strip())
    has_sub = await db_has_sub(user.id)
    sub_str = await db_sub_until(user.id)
    status  = f"✅ Подписка до: {sub_str}" if has_sub else "❌ Подписки нет — введите промокод"
    adm_str = " 🛡 Админ" if is_admin(user.id) else ""

    prev_id = context.user_data.get("menu_msg_id")
    if prev_id:
        try:
            await context.bot.delete_message(chat_id=user.id, message_id=prev_id)
        except Exception:
            pass

    sent = await update.message.reply_text(
        f"👋 Привет, {esc(user.first_name or 'друг')}!{adm_str}\n"
        f"🆔 ID: {user.id}\n📊 {status}\n\nВыберите действие:",
        reply_markup=kb_main(user.id),
    )
    try:
        await update.message.delete()
    except Exception:
        pass
    context.user_data["menu_msg_id"] = sent.message_id


# ══════════════════════════════════════════════════════
#  CALLBACK — главное меню
# ══════════════════════════════════════════════════════

async def cb_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = q.from_user.id
    await q.answer()

    if q.data == "menu:main":
        has_sub = await db_has_sub(uid)
        sub_str = await db_sub_until(uid)
        status  = f"✅ Подписка до: {sub_str}" if has_sub else "❌ Подписки нет"
        adm_str = " 🛡 Админ" if is_admin(uid) else ""
        await q.edit_message_text(
            f"👤 *Главное меню*{adm_str}\n🆔 `{uid}`\n📊 {status}",
            parse_mode="Markdown", reply_markup=kb_main(uid)
        )

    elif q.data == "menu:profile":
        u       = await db_get_user(uid)
        sub_str = await db_sub_until(uid)
        chats   = await file_get_all_chats(uid)
        conns   = await db_get_conns(uid)
        total_media = sum(c["media_count"] for c in chats)
        await q.edit_message_text(
            f"👤 *Ваш профиль*\n━━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 ID: `{uid}`\n"
            f"📛 Ник: @{esc(u['username'] or '—')}\n"
            f"📅 Регистрация: {u['registered_at']}\n"
            f"📡 Business подключений: {len(conns)}\n"
            f"🔔 Подписка: {sub_str}\n"
            f"💬 Чатов: {len(chats)}\n"
            f"🖼 Медиафайлов: {total_media}",
            parse_mode="Markdown", reply_markup=kb_back_main()
        )

    elif q.data == "menu:promo":
        await q.edit_message_text(
            "🔑 *Введите промокод*\n\nОтправьте промокод следующим сообщением:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Отмена", callback_data="menu:main")]])
        )
        context.user_data["await_promo"] = True

    elif q.data == "menu:chats":
        chats = await file_get_all_chats(uid)
        if not chats:
            await q.edit_message_text(
                "📂 *Архив пуст*\n\nПодключите Business Mode и начните переписку.",
                parse_mode="Markdown", reply_markup=kb_back_main()
            )
            return
        rows = []
        for c in chats:
            flag  = "⚠️ " if c["events_count"] > 0 else ""
            media = f" 🖼{c['media_count']}" if c["media_count"] > 0 else ""
            label = f"{flag}💬 {c['chat_name']} ({c['msg_count']} сообщ.{media})"
            rows.append([InlineKeyboardButton(label, callback_data=f"chat:{c['chat_id']}:{uid}")])
        rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
        await q.edit_message_text(
            "📂 *Архивированные чаты:*", parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(rows)
        )

    elif q.data == "menu:events":
        has_sub = await db_has_sub(uid)
        if not has_sub:
            await q.edit_message_text(
                "🔒 *Раздел доступен по подписке*",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔑 Ввести промокод", callback_data="menu:promo")],
                    [InlineKeyboardButton("◀️ Назад",           callback_data="menu:main")],
                ])
            )
            return
        events = await db_get_events(uid)
        if not events:
            await q.edit_message_text("✅ Подозрительных событий нет.", reply_markup=kb_back_main())
            return
        lines = ["⚠️ *Последние события:*\n"]
        for e in events:
            icon = "🚨" if e["type"] == "deleted" else "✏️"
            act  = "УДАЛЕНО" if e["type"] == "deleted" else "ИЗМЕНЕНО"
            lines.append(
                f"{icon} *{act}* | {esc(e['chat_name'])}\n"
                f"   👤 {esc(e['from_name'])} | 🆔 `{e['from_id'] or '?'}`\n"
                f"   🕐 {e['at']}\n"
                f"   Было: `{esc(str(e['original'] or '—')[:100])}`\n"
            )
        text = "\n".join(lines)
        for i in range(0, len(text), 3800):
            kb = kb_back_main() if i + 3800 >= len(text) else None
            if i == 0:
                await q.edit_message_text(text[i:i+3800], parse_mode="Markdown", reply_markup=kb)
            else:
                await q.message.reply_text(text[i:i+3800], parse_mode="Markdown", reply_markup=kb)


# ══════════════════════════════════════════════════════
#  CALLBACK — просмотр чата
# ══════════════════════════════════════════════════════

async def cb_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, chat_id_s, target_uid_s = q.data.split(":")
    chat_id    = int(chat_id_s)
    target_uid = int(target_uid_s)
    viewer_uid = q.from_user.id

    if viewer_uid != target_uid and not is_admin(viewer_uid):
        await q.answer("⛔️ Нет доступа.", show_alert=True)
        return

    msgs = await file_get_chat(target_uid, chat_id)
    if not msgs:
        await q.edit_message_text("❌ Чат пуст.", reply_markup=kb_back_main())
        return

    lines = [f"📋 *Чат {chat_id}*\n"]
    for m in msgs:
        status  = " ✏️" if m.get("edited") else (" 🗑" if m.get("deleted") else "")
        content = m.get("text") or m.get("media") or "[пусто]"
        fid     = f" | 🆔 `{m['from_id']}`" if m.get("from_id") else ""
        media_s = f"\n  📁 `{m['media_path']}`" if m.get("media_path") else ""
        lines.append(
            f"[{m['sent_at']}]{status}\n"
            f"  👤 {esc(m['from_name'])} {m['from_username']}{fid}\n"
            f"  💬 {esc(str(content)[:200])}{media_s}\n"
        )

    back_cb = f"admin:userchats:{target_uid}" if viewer_uid != target_uid else "menu:chats"
    text = "\n".join(lines)
    for i in range(0, len(text), 3800):
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data=back_cb)]]) \
             if i + 3800 >= len(text) else None
        if i == 0:
            await q.edit_message_text(text[i:i+3800], parse_mode="Markdown", reply_markup=kb)
        else:
            await q.message.reply_text(text[i:i+3800], parse_mode="Markdown", reply_markup=kb)


# ══════════════════════════════════════════════════════
#  CALLBACK — АДМИН
# ══════════════════════════════════════════════════════

async def cb_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = q.from_user.id
    await q.answer()

    if not is_admin(uid):
        await q.answer("⛔️ Нет доступа.", show_alert=True)
        return

    parts  = q.data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "panel":
        users = await db_get_all_users(limit=1000)
        active, expired, never = 0, 0, 0
        for u in users:
            if not u["sub_until"]:
                never += 1
            else:
                try:
                    if datetime.fromisoformat(u["sub_until"]) > now_dt():
                        active += 1
                    else:
                        expired += 1
                except Exception:
                    never += 1
        await q.edit_message_text(
            f"🛡 *Админ-панель*\n━━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 Всего пользователей: *{len(users)}*\n\n"
            f"🟢 С активной подпиской: *{active}*\n"
            f"🟡 Подписка истекла: *{expired}*\n"
            f"🔴 Никогда не покупали: *{never}*\n\n"
            f"🕐 {ts()}",
            parse_mode="Markdown", reply_markup=kb_admin()
        )

    elif action == "users":
        page  = int(parts[2]) if len(parts) > 2 else 0
        users = await db_get_all_users(limit=1000)
        chunk = users[page*10:(page+1)*10]
        rows  = []
        for u in chunk:
            has_s = await db_has_sub(u["user_id"])
            icon  = "🟢" if has_s else "🔴"
            adm   = "🛡" if u["is_admin"] else ""
            label = f"{icon}{adm} {u['full_name'] or u['username'] or str(u['user_id'])}"
            rows.append([InlineKeyboardButton(label, callback_data=f"admin:user:{u['user_id']}")])
        nav = []
        if page > 0:          nav.append(InlineKeyboardButton("◀️", callback_data=f"admin:users:{page-1}"))
        if (page+1)*10 < len(users): nav.append(InlineKeyboardButton("▶️", callback_data=f"admin:users:{page+1}"))
        if nav: rows.append(nav)
        rows.append([InlineKeyboardButton("◀️ Панель", callback_data="admin:panel")])
        await q.edit_message_text(
            f"👥 *Пользователи* (стр. {page+1})\n🟢 подписка есть | 🔴 нет | 🛡 админ",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(rows)
        )

    elif action == "user":
        target  = int(parts[2])
        u       = await db_get_user(target)
        if not u:
            await q.edit_message_text("❌ Не найден.", reply_markup=kb_back_admin())
            return
        sub_str     = await db_sub_until(target)
        chats       = await file_get_all_chats(target)
        conns       = await db_get_conns(target)
        total_media = sum(c["media_count"] for c in chats)
        adm_str     = "🛡 Да" if u["is_admin"] else "Нет"
        await q.edit_message_text(
            f"👤 *Пользователь*\n━━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 ID: `{u['user_id']}`\n"
            f"📛 Ник: @{esc(u['username'] or '—')}\n"
            f"📝 Имя: {esc(u['full_name'] or '—')}\n"
            f"📅 В боте с: {u['registered_at']}\n"
            f"🔔 Подписка: {sub_str}\n"
            f"📡 Подключений: {len(conns)}\n"
            f"💬 Чатов: {len(chats)} | 🖼 Медиа: {total_media}\n"
            f"🛡 Админ: {adm_str}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💬 Чаты", callback_data=f"admin:userchats:{target}"),
                 InlineKeyboardButton("🎁 Выдать дни", callback_data=f"admin:giveuser:{target}")],
                [InlineKeyboardButton(
                    "🛡 Сделать админом" if not u["is_admin"] else "❌ Снять админа",
                    callback_data=f"admin:toggleadmin:{target}"
                )],
                [InlineKeyboardButton("◀️ К списку", callback_data="admin:users:0")],
            ])
        )

    elif action == "userchats":
        target = int(parts[2])
        chats  = await file_get_all_chats(target)
        u      = await db_get_user(target)
        name   = u["full_name"] or u["username"] or str(target) if u else str(target)
        if not chats:
            await q.edit_message_text(f"📂 У {esc(name)} нет чатов.", reply_markup=kb_back_admin())
            return
        rows = []
        for c in chats:
            flag  = "⚠️ " if c["events_count"] > 0 else ""
            media = f" 🖼{c['media_count']}" if c["media_count"] > 0 else ""
            label = f"{flag}💬 {c['chat_name']} ({c['msg_count']} сообщ.{media})"
            rows.append([InlineKeyboardButton(label, callback_data=f"chat:{c['chat_id']}:{target}")])
        rows.append([InlineKeyboardButton("◀️ К пользователю", callback_data=f"admin:user:{target}")])
        await q.edit_message_text(
            f"💬 *Чаты* {esc(name)}:", parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(rows)
        )

    elif action == "toggleadmin":
        target  = int(parts[2])
        u       = await db_get_user(target)
        if u:
            new_val = 0 if u["is_admin"] else 1
            await db_set_admin(target, new_val)
            if new_val and target not in ADMIN_IDS: ADMIN_IDS.append(target)
            elif not new_val and target in ADMIN_IDS: ADMIN_IDS.remove(target)
        await q.answer("✅ Готово!")
        q.data = f"admin:user:{target}"
        await cb_admin(update, context)

    elif action == "giveuser":
        target = int(parts[2])
        context.user_data["give_days_target"] = target
        context.user_data["await_give_days"]  = True
        await q.edit_message_text(
            f"🎁 Пользователь `{target}`\n\nСколько дней добавить?",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Отмена", callback_data=f"admin:user:{target}")]])
        )

    elif action == "givedays":
        context.user_data["await_give_days_id"] = True
        await q.edit_message_text(
            "🎁 *Выдать подписку*\n\nВведите Telegram ID пользователя:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Отмена", callback_data="admin:panel")]])
        )

    elif action == "promos":
        promos = await db_get_all_promos()
        if not promos:
            await q.edit_message_text("🔑 Промокодов нет.", reply_markup=kb_back_admin())
            return
        lines = ["🔑 *Все промокоды:*\n"]
        for p in promos:
            limit = f"{p['used_count']}/{p['max_uses']}" if p["max_uses"] != -1 else f"{p['used_count']}/∞"
            exhaust = " ✅ исчерпан" if p["max_uses"] != -1 and p["used_count"] >= p["max_uses"] else ""
            lines.append(f"• `{p['code']}` — {p['days']} дн. | использован {limit}{exhaust}")
        rows = []
        for p in promos:
            rows.append([InlineKeyboardButton(
                f"ℹ️ {p['code']}  🗑 удалить",
                callback_data=f"admin:delpromo:{p['code']}"
            )])
        rows.append([InlineKeyboardButton("◀️ Панель", callback_data="admin:panel")])
        await q.edit_message_text("\n".join(lines), parse_mode="Markdown",
                                  reply_markup=InlineKeyboardMarkup(rows))

    elif action == "newpromo":
        context.user_data["await_new_promo"] = True
        await q.edit_message_text(
            "➕ *Новый промокод*\n\nШаг 1: введите код (например: VIP2025):",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Отмена", callback_data="admin:panel")]])
        )

    elif action == "delpromo":
        code = parts[2]
        await db_del_promo(code)
        await q.answer(f"✅ {code} удалён.")
        q.data = "admin:promos"
        await cb_admin(update, context)


# ══════════════════════════════════════════════════════
#  ТЕКСТОВЫЕ СООБЩЕНИЯ — промокоды, ввод данных
# ══════════════════════════════════════════════════════

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Игнорируем business_message — у них свой хэндлер
    if update.business_message:
        return

    uid  = update.effective_user.id
    text = update.message.text.strip()
    ud   = context.user_data

    if ud.pop("await_promo", False):
        if text == ADMIN_PROMO_KEY:
            if uid not in ADMIN_IDS:
                ADMIN_IDS.append(uid)
                await db_set_admin(uid, 1)
            await update.message.reply_text("🛡 *Доступ к админ-панели получен!*",
                                            parse_mode="Markdown", reply_markup=kb_main(uid))
            return
        ok, msg_text = await db_use_promo(uid, text.upper())
        sub_str = await db_sub_until(uid)
        await update.message.reply_text(f"{msg_text}\n📅 Подписка до: {sub_str}",
                                        parse_mode="Markdown", reply_markup=kb_main(uid))
        return

    if ud.pop("await_new_promo", False) and is_admin(uid):
        ud["new_promo_code"]       = text.upper()
        ud["await_new_promo_days"] = True
        await update.message.reply_text(
            f"➕ Код: `{text.upper()}`\n\nШаг 2: сколько дней подписки?",
            parse_mode="Markdown"
        )
        return

    if ud.pop("await_new_promo_days", False) and is_admin(uid):
        try:
            days = int(text)
            ud["new_promo_days"]      = days
            ud["await_new_promo_max"] = True
            await update.message.reply_text(
                f"➕ Код: `{ud.get('new_promo_code')}` | Дней: {days}\n\n"
                f"Шаг 3: максимум использований?\n"
                f"Введите число или `0` для безлимитного:",
                parse_mode="Markdown"
            )
        except ValueError:
            await update.message.reply_text("❌ Введите число.", reply_markup=kb_admin())
        return

    if ud.pop("await_new_promo_max", False) and is_admin(uid):
        try:
            max_uses = int(text)
            code     = ud.pop("new_promo_code", "PROMO")
            days     = ud.pop("new_promo_days", 30)
            if max_uses <= 0:
                max_uses = -1  # безлимит
            await db_add_promo(code, days, max_uses)
            limit_str = "безлимитный" if max_uses == -1 else f"макс. {max_uses} раз"
            await update.message.reply_text(
                f"✅ Промокод {code} создан!\n"
                f"📅 Дней: {days} | 🔢 Лимит: {limit_str}",
                reply_markup=kb_admin()
            )
        except ValueError:
            await update.message.reply_text("❌ Введите число.", reply_markup=kb_admin())
        return

    if ud.pop("await_give_days_id", False) and is_admin(uid):
        try:
            target = int(text)
            ud["give_days_target"] = target
            ud["await_give_days"]  = True
            await update.message.reply_text(f"🎁 Пользователь `{target}`\n\nСколько дней?",
                                            parse_mode="Markdown")
        except ValueError:
            await update.message.reply_text("❌ Введите числовой ID.", reply_markup=kb_admin())
        return

    if ud.pop("await_give_days", False) and is_admin(uid):
        target = ud.pop("give_days_target", None)
        try:
            days    = int(text)
            await db_give_sub(target, days)
            sub_str = await db_sub_until(target)
            await update.message.reply_text(
                f"✅ Пользователю `{target}` добавлено {days} дней.\n📅 До: {sub_str}",
                parse_mode="Markdown", reply_markup=kb_admin()
            )
        except ValueError:
            await update.message.reply_text("❌ Введите число.", reply_markup=kb_admin())
        return


# ══════════════════════════════════════════════════════
#  BUSINESS HANDLERS
# ══════════════════════════════════════════════════════

async def on_business_connection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn     = update.business_connection
    owner_id = conn.user.id
    await db_register_user(owner_id, conn.user.username or "",
                           f"{conn.user.first_name or ''} {conn.user.last_name or ''}".strip())
    if conn.is_enabled:
        await db_add_conn(conn.id, owner_id)
        await context.bot.send_message(
            chat_id=owner_id,
            text="✅ *Архивирование подключено*\n\n"
                 "• 📝 Сохраняю все сообщения и медиа\n"
                 "• 🚨 Удаления — по подписке\n"
                 "• ✏️ Редактирования — по подписке\n\n"
                 "Нажмите /menu",
            parse_mode="Markdown"
        )
    else:
        await db_del_conn(conn.id)
        await context.bot.send_message(chat_id=owner_id, text="⛔️ Архивирование отключено.")

async def on_new_business_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.business_message
    if not msg:
        return
    conn_id  = getattr(msg, "business_connection_id", None)
    if not conn_id:
        return
    owner_id = await db_get_owner(conn_id)
    if not owner_id:
        logger.warning(f"on_new: owner не найден conn={conn_id}")
        return
    await archive_message(owner_id, msg)
    logger.info(f"[{owner_id}] ✉️ chat={msg.chat.id} msg={msg.message_id} media={detect_media(msg)}")

async def on_edited_business_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.edited_business_message
    if not msg:
        return
    conn_id  = getattr(msg, "business_connection_id", None)
    if not conn_id:
        logger.warning(f"on_edited: нет business_connection_id")
        return
    owner_id = await db_get_owner(conn_id)
    if not owner_id:
        logger.warning(f"on_edited: owner не найден conn={conn_id}")
        return

    name, uname, from_id = sender_info(msg)
    chat_name = msg.chat.full_name or str(msg.chat.id)
    new_text  = msg.text or msg.caption or detect_media(msg) or "[пусто]"
    original  = await db_get_msg(owner_id, msg.chat.id, msg.message_id)
    orig_text = original["text"] if original else "_(до запуска бота)_"

    await archive_event(owner_id, "edited", msg.chat.id, chat_name,
                        msg.message_id, orig_text, new_text, from_id, name, uname)
    await archive_message(owner_id, msg)

    logger.info(f"[{owner_id}] ✏️ chat={msg.chat.id} msg={msg.message_id}")

    if await db_has_sub(owner_id):
        await context.bot.send_message(
            chat_id=owner_id,
            text=(
                f"✏️ СООБЩЕНИЕ ОТРЕДАКТИРОВАНО\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 {name} {uname}\n"
                f"🆔 {from_id or '?'}\n"
                f"💬 {chat_name} | 🕐 {ts()}\n\n"
                f"📋 БЫЛО:\n{orig_text[:300]}\n\n"
                f"📋 СТАЛО:\n{new_text[:300]}"
            )
        )

async def on_any_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ловим deleted_business_messages через TypeHandler."""
    deleted = getattr(update, "deleted_business_messages", None)
    if not deleted:
        return
    owner_id = await db_get_owner(deleted.business_connection_id)
    if not owner_id:
        return

    chat_name = deleted.chat.full_name or str(deleted.chat.id)
    lines     = []

    for msg_id in deleted.message_ids:
        stored = await db_get_msg(owner_id, deleted.chat.id, msg_id)
        orig   = stored["text"]          if stored else "_(не сохранено)_"
        fname  = stored["from_name"]     if stored else "?"
        funame = stored["from_username"] if stored else "—"
        fid    = stored["from_id"]       if stored else None

        await archive_event(owner_id, "deleted", deleted.chat.id, chat_name,
                            msg_id, orig, "", fid, fname, funame)
        lines.append(
            f"🔢 ID {msg_id}\n"
            f"   👤 {fname} {funame} | 🆔 {fid or '?'}\n"
            f"   💬 {str(orig)[:200]}"
        )

    if await db_has_sub(owner_id):
        await context.bot.send_message(
            chat_id=owner_id,
            text=(
                f"🚨 СООБЩЕНИЕ УДАЛЕНО\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"💬 {chat_name} | 🕐 {ts()}\n"
                f"📊 Удалено: {len(deleted.message_ids)} сообщ.\n\n"
                f"📋 СОДЕРЖИМОЕ:\n\n" + "\n\n".join(lines)
            )
        )


# ══════════════════════════════════════════════════════
#  ЗАПУСК
# ══════════════════════════════════════════════════════

async def post_init(app: Application):
    ARCHIVE_DIR.mkdir(exist_ok=True)
    await db_init()
    await app.bot.set_my_commands([
        BotCommand("start", "▶️ Главное меню"),
        BotCommand("menu",  "📋 Меню"),
    ])
    logger.info("✅ Legal Archive Bot v6 запущен.")

def main():
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu",  cmd_menu))

    app.add_handler(CallbackQueryHandler(cb_menu,  pattern=r"^menu:"))
    app.add_handler(CallbackQueryHandler(cb_admin, pattern=r"^admin:"))
    app.add_handler(CallbackQueryHandler(cb_chat,  pattern=r"^chat:"))

    app.add_handler(BusinessConnectionHandler(on_business_connection))

    # Business сообщения — отдельные хэндлеры с точными фильтрами
    app.add_handler(MessageHandler(
        filters.UpdateType.BUSINESS_MESSAGE, on_new_business_message
    ))
    app.add_handler(MessageHandler(
        filters.UpdateType.EDITED_BUSINESS_MESSAGE, on_edited_business_message
    ))

    # Обычные текстовые (промокоды и т.д.) — только не business
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & ~filters.UpdateType.BUSINESS_MESSAGE,
        on_text
    ))

    # Удаления — через TypeHandler (единственный способ)
    app.add_handler(TypeHandler(Update, on_any_update))

    app.run_polling(allowed_updates=[
        "message", "callback_query",
        "business_connection", "business_message",
        "edited_business_message", "deleted_business_messages",
    ])

if __name__ == "__main__":
    import asyncio
    asyncio.set_event_loop(asyncio.new_event_loop())
    main()