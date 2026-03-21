from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import aiosqlite
import asyncio
import os

app = Flask(__name__)
CORS(app)

DB_FILE = "archive.db"

def run_async(coro):
    """Запуск async функции из sync Flask."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

# ── Отдаём webapp.html ──────────────────────────────
@app.route("/")
def index():
    return send_from_directory(".", "webapp.html")

# ── Список чатов ────────────────────────────────────
@app.route("/api/chats/<int:uid>")
def chats(uid):
    async def _get():
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute(
                "SELECT chat_id, chat_name, COUNT(*) as cnt, MAX(sent_at) as last "
                "FROM messages WHERE owner_id=? GROUP BY chat_id ORDER BY last DESC",
                (uid,)
            ) as cur:
                rows = await cur.fetchall()
                return [{"chat_id": r[0], "name": r[1], "cnt": r[2], "last": r[3]}
                        for r in rows]
    return jsonify(run_async(_get()))

# ── Сообщения чата ──────────────────────────────────
@app.route("/api/msgs/<int:uid>/<int:chat_id>")
def msgs(uid, chat_id):
    async def _get():
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute(
                "SELECT from_name, from_username, text, media_type, sent_at "
                "FROM messages WHERE owner_id=? AND chat_id=? ORDER BY sent_at ASC",
                (uid, chat_id)
            ) as cur:
                rows = await cur.fetchall()
                return [{"name": r[0], "uname": r[1], "text": r[2],
                         "media": r[3], "at": r[4]} for r in rows]
    return jsonify(run_async(_get()))

# ── События (удаления/изменения) ────────────────────
@app.route("/api/events/<int:uid>")
def events(uid):
    async def _get():
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute(
                "SELECT event_type, chat_name, message_id, original_text, "
                "new_text, from_name, from_username, happened_at "
                "FROM events WHERE owner_id=? ORDER BY happened_at DESC LIMIT 100",
                (uid,)
            ) as cur:
                rows = await cur.fetchall()
                return [{"type": r[0], "chat": r[1], "mid": r[2],
                         "orig": r[3], "new_text": r[4], "who": r[5],
                         "uname": r[6], "at": r[7]} for r in rows]
    return jsonify(run_async(_get()))

# ── Статистика ──────────────────────────────────────
@app.route("/api/stats/<int:uid>")
def stats(uid):
    async def _get():
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM messages WHERE owner_id=?", (uid,)
            ) as cur:
                total = (await cur.fetchone())[0]
            async with db.execute(
                "SELECT COUNT(*) FROM events WHERE owner_id=? AND event_type='deleted'", (uid,)
            ) as cur:
                deleted = (await cur.fetchone())[0]
            async with db.execute(
                "SELECT COUNT(*) FROM events WHERE owner_id=? AND event_type='edited'", (uid,)
            ) as cur:
                edited = (await cur.fetchone())[0]
            async with db.execute(
                "SELECT COUNT(DISTINCT chat_id) FROM messages WHERE owner_id=?", (uid,)
            ) as cur:
                chats = (await cur.fetchone())[0]
        return {"total": total, "deleted": deleted, "edited": edited, "chats": chats}
    return jsonify(run_async(_get()))

if __name__ == "__main__":
    print("🌐 Сервер запущен: http://localhost:8080")
    app.run(host="0.0.0.0", port=8080, debug=False)