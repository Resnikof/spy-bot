"""
Веб-сервер для WebApp панели
Запускать отдельно: python server.py
"""
import json
import os
import aiosqlite
from pathlib import Path
from datetime import datetime, timezone
from aiohttp import web

from config import DB_FILE, ADMIN_IDS, SERVER_PORT as PORT
ARCHIVE_DIR = Path("archives")

# ══════════════════════════════════════════════════════
#  УТИЛИТЫ
# ══════════════════════════════════════════════════════

def now_dt():
    return datetime.now(timezone.utc)

async def db_get_user(uid):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(
            "SELECT user_id,username,full_name,registered_at,sub_until,is_admin FROM users WHERE user_id=?", (uid,)
        ) as cur:
            row = await cur.fetchone()
            return {"user_id": row[0], "username": row[1], "full_name": row[2],
                    "registered_at": row[3], "sub_until": row[4], "is_admin": row[5]} if row else None

async def db_has_sub(uid):
    if uid in ADMIN_IDS: return True
    u = await db_get_user(uid)
    if not u or not u["sub_until"]: return False
    try:
        return datetime.fromisoformat(u["sub_until"]) > now_dt()
    except: return False

async def db_sub_until(uid):
    u = await db_get_user(uid)
    if not u or not u["sub_until"]: return "нет"
    try:
        exp = datetime.fromisoformat(u["sub_until"])
        if exp < now_dt(): return "истекла"
        return f"{exp.strftime('%Y-%m-%d')} (осталось {(exp - now_dt()).days} дн.)"
    except: return "нет"

async def db_get_all_users():
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(
            "SELECT user_id,username,full_name,registered_at,sub_until,is_admin FROM users ORDER BY registered_at DESC"
        ) as cur:
            rows = await cur.fetchall()
            return [{"user_id": r[0], "username": r[1], "full_name": r[2],
                     "registered_at": r[3], "sub_until": r[4], "is_admin": r[5]} for r in rows]

async def db_get_stats():
    users = await db_get_all_users()
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
            except:
                never += 1
    return {"total": len(users), "active": active, "expired": expired, "never": never}

async def get_chats(owner_id):
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
            with open(cj, "r", encoding="utf-8") as f:
                try: msgs = json.load(f)
                except: msgs = []
        if ej.exists():
            with open(ej, "r", encoding="utf-8") as f:
                try: evs = json.load(f)
                except: evs = []
        media_dir   = d / "media"
        media_files = list(media_dir.iterdir()) if media_dir.exists() else []
        parts = d.name.split("_", 1)
        result.append({
            "chat_id":      parts[0],
            "chat_name":    parts[1].replace("_", " ") if len(parts) > 1 else parts[0],
            "msg_count":    len(msgs),
            "events_count": len(evs),
            "media_count":  len(media_files),
            "last_msg":     msgs[-1] if msgs else None,
            "folder":       d.name,
        })
    return result

async def get_messages(owner_id, chat_id):
    p = ARCHIVE_DIR / str(owner_id)
    if not p.exists():
        return []
    for d in p.iterdir():
        if d.is_dir() and d.name.startswith(f"{chat_id}_"):
            cj = d / "chat.json"
            if cj.exists():
                with open(cj, "r", encoding="utf-8") as f:
                    try: return json.load(f)
                    except: return []
    return []

# ══════════════════════════════════════════════════════
#  CORS middleware
# ══════════════════════════════════════════════════════

async def cors_middleware(app, handler):
    async def middleware(request):
        if request.method == "OPTIONS":
            return web.Response(headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
            })
        response = await handler(request)
        response.headers["Access-Control-Allow-Origin"] = "*"
        return response
    return middleware

# ══════════════════════════════════════════════════════
#  API ROUTES
# ══════════════════════════════════════════════════════

async def api_me(request):
    uid = int(request.rel_url.query.get("uid", 0))
    if not uid:
        return web.json_response({"error": "no uid"}, status=400)
    u = await db_get_user(uid)
    if not u:
        return web.json_response({"error": "not found"}, status=404)
    has_sub  = await db_has_sub(uid)
    sub_str  = await db_sub_until(uid)
    chats    = await get_chats(uid)
    return web.json_response({
        "user_id":   u["user_id"],
        "username":  u["username"] or "",
        "full_name": u["full_name"] or "",
        "registered_at": u["registered_at"],
        "sub_until": sub_str,
        "has_sub":   has_sub,
        "is_admin":  uid in ADMIN_IDS or bool(u["is_admin"]),
        "chat_count": len(chats),
    })

async def api_chats(request):
    uid = int(request.rel_url.query.get("uid", 0))
    if not uid:
        return web.json_response({"error": "no uid"}, status=400)
    chats = await get_chats(uid)
    return web.json_response(chats)

async def api_messages(request):
    uid     = int(request.rel_url.query.get("uid", 0))
    chat_id = request.rel_url.query.get("chat_id", "")
    viewer  = int(request.rel_url.query.get("viewer", 0))
    if not uid or not chat_id:
        return web.json_response({"error": "missing params"}, status=400)
    # Проверка прав
    if viewer != uid and viewer not in ADMIN_IDS:
        return web.json_response({"error": "forbidden"}, status=403)
    msgs = await get_messages(uid, chat_id)
    return web.json_response(msgs)

async def api_users(request):
    viewer = int(request.rel_url.query.get("viewer", 0))
    if viewer not in ADMIN_IDS:
        return web.json_response({"error": "forbidden"}, status=403)
    users = await db_get_all_users()
    result = []
    for u in users:
        has_s = await db_has_sub(u["user_id"])
        chats = await get_chats(u["user_id"])
        result.append({**u, "has_sub": has_s, "chat_count": len(chats)})
    return web.json_response(result)

async def api_stats(request):
    viewer = int(request.rel_url.query.get("viewer", 0))
    if viewer not in ADMIN_IDS:
        return web.json_response({"error": "forbidden"}, status=403)
    stats = await db_get_stats()
    return web.json_response(stats)

async def serve_media(request):
    """Отдать медиафайл по пути archives/{uid}/{folder}/media/{file}"""
    uid    = request.match_info["uid"]
    folder = request.match_info["folder"]
    fname  = request.match_info["filename"]
    path   = ARCHIVE_DIR / uid / folder / "media" / fname
    if not path.exists():
        return web.Response(status=404)
    return web.FileResponse(path)

async def serve_webapp(request):
    return web.FileResponse("webapp.html")

# ══════════════════════════════════════════════════════
#  ЗАПУСК
# ══════════════════════════════════════════════════════

def main():
    app = web.Application(middlewares=[cors_middleware])
    app.router.add_get("/",                                    serve_webapp)
    app.router.add_get("/webapp.html",                        serve_webapp)
    app.router.add_get("/api/me",                             api_me)
    app.router.add_get("/api/chats",                          api_chats)
    app.router.add_get("/api/messages",                       api_messages)
    app.router.add_get("/api/users",                          api_users)
    app.router.add_get("/api/stats",                          api_stats)
    app.router.add_get("/media/{uid}/{folder}/{filename}",    serve_media)
    print(f"✅ Сервер запущен на порту {PORT}")
    web.run_app(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()