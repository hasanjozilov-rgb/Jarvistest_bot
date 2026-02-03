import os
import asyncio
import logging
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiohttp import web

from config import BOT_TOKEN
import db
from admin_handler import admin_router
from user_handler import user_router, group_router, inline_router
from utils import add_reaction, answer_with_effect

class TrafficMiddleware(BaseMiddleware):
    def __init__(self):
        self.count = 0
        self.logger = logging.getLogger("traffic")
        super().__init__()
    async def __call__(self, handler, event, data):
        self.count += 1
        try:
            u = getattr(event, "from_user", None)
            chat = getattr(event, "chat", None)
            txt = getattr(event, "text", None)
            self.logger.info(f"req#{self.count} uid={getattr(u,'id',None)} chat={getattr(chat,'id',None)} text_len={len(txt) if isinstance(txt,str) else 0}")
        except Exception:
            pass
        return await handler(event, data)

class ReactionMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            from aiogram.types import Message
            if isinstance(event, Message):
                bot = data.get("bot")
                await add_reaction(bot, event.chat.id, event.message_id, "🎉", is_big=False)
                await answer_with_effect(event, "🎉", effect="celebration")
        except Exception:
            pass
        return await handler(event, data)

async def start_http_server():
    async def handle(request):
        return web.Response(text="OK")
    app = web.Application()
    app.add_routes([web.get('/', handle)])
    runner = web.AppRunner(app)
    await runner.setup()
    # DEFAULT PORT = 10000, but honor environment PORT if provided (e.g., Render)
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logging.info(f"HTTP server started on port {port}")

async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    db.init_db()
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    dp.update.middleware(TrafficMiddleware())
    dp.update.middleware(ReactionMiddleware())
    dp.include_router(admin_router)
    dp.include_router(user_router)
    dp.include_router(group_router)
    dp.include_router(inline_router)

    # Start the HTTP server (binds to PORT or default 10000) so hosting detects an open port
    await start_http_server()

    # Start polling (this blocks until cancelled)
    await dp.start_polling(bot, drop_pending_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
