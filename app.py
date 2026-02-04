import asyncio
import logging
import signal
import sys
from aiogram import Bot, Dispatcher, BaseMiddleware
from config import BOT_TOKEN
import db
from admin_handler import admin_router
from user_handler import user_router, group_router, inline_router
from utils import add_reaction, answer_with_effect
import os
from aiohttp import web

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

# ✅ Signal handlers qo'shildi
def setup_signal_handlers():
    """SIGTERM va SIGINT ni handle qilish"""
    loop = asyncio.get_event_loop()
    
    def shutdown_handler(signame):
        logging.info(f"Received {signame}, shutting down...")
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
    
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, signame),
            lambda s=signame: shutdown_handler(s)
        )

async def health_check(request):
    """Worker uchun oddiy health check"""
    return web.Response(text="OK")

async def start_http_server():
    """Worker rejimida minimal HTTP server"""
    app = web.Application()
    app.router.add_get('/health', health_check)
    app.router.add_get('/', health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logging.info(f"HTTP server started on port {port}")
    return runner

async def main():
    # ✅ Logging sozlamalari
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # ✅ Signal handlers o'rnatish
    setup_signal_handlers()
    
    # ✅ Database va botni ishga tushirish
    db.init_db()
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    
    # Middleware'lar
    dp.update.middleware(TrafficMiddleware())
    dp.update.middleware(ReactionMiddleware())
    
    # Router'lar
    dp.include_router(admin_router)
    dp.include_router(user_router)
    dp.include_router(group_router)
    dp.include_router(inline_router)
    
    try:
        # ✅ HTTP server va botni parallel ishga tushirish
        logging.info("Starting bot...")
        
        # Agar health check kerak bo'lsa, serverni ishga tushiramiz
        if os.environ.get("ENABLE_HEALTH_CHECK", "true").lower() == "true":
            runner = await start_http_server()
        
        # Botni polling qilish
        await dp.start_polling(bot, drop_pending_updates=True)
        
    except asyncio.CancelledError:
        logging.info("Bot shutdown requested")
    except Exception as e:
        logging.error(f"Bot crashed: {e}", exc_info=True)
        raise
    finally:
        # ✅ Tozalash ishlari
        logging.info("Cleaning up...")
        await bot.session.close()
        if 'runner' in locals():
            await runner.cleanup()

if __name__ == "__main__":
    # ✅ Graceful shutdown uchun
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
