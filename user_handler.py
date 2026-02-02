from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineQuery, InlineQueryResultCachedDocument, \
    InlineQueryResultCachedAudio, InlineQueryResultArticle, InputTextMessageContent, InlineKeyboardMarkup, \
    InlineKeyboardButton
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
import db
from keyboards import main_menu, back_menu, numbers_keyboard, categories_keyboard, group_numbers_keyboard, \
    paged_numbers_keyboard
from keyboards import book_actions_keyboard, profile_menu, popular_menu
from utils import check_membership, join_channels_keyboard, bot_signature, fmt_duration, fmt_size, \
    blockquote, \
    add_reaction, type_icon, answer_with_effect, bot_link, deep_link_for_book
from config import ADMIN_IDS, BOT_USERNAME, FORWARD_GROUP_ID, ADMIN_CONTACT_URL, BOOK_HELP_CHANNEL_LINK, \
    CARD_NUMBER, CARD_HOLDER
from aiogram.exceptions import TelegramRetryAfter
from aiogram.enums import ChatAction
import asyncio
import random

user_router = Router()
user_router.message.filter(F.chat.type == "private")
inline_router = Router()


class SearchState(StatesGroup):
    paging = State()


class GroupSearchState(StatesGroup):
    paging = State()


class CatState(StatesGroup):
    paging = State()


class TopState(StatesGroup):
    paging = State()


class RecentState(StatesGroup):
    paging = State()


class SavedState(StatesGroup):
    paging = State()


class RandomState(StatesGroup):
    paging = State()


@user_router.message(CommandStart())
async def start(message: Message):
    db.upsert_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    if db.is_blocked(message.from_user.id):
        await answer_with_effect(message, blockquote("🚫 Siz admin tomonidan bloklangansiz 😡😈👿"), effect="dislike")
        return
    ok = await check_membership(message.bot, message.from_user.id)
    if not ok:
        txt = blockquote("❌ Kechirasiz botimizdan foydalanishdan oldin ushbu kanalga a‘zo bo‘lishingiz kerak 🤬.")
        await answer_with_effect(message, txt, reply_markup=join_channels_keyboard(), effect="dislike")
        return
    if message.text and "book_" in message.text:
        try:
            payload = message.text.split(" ", 1)[1] if " " in message.text else ""
            if payload.startswith("book_"):
                bid = int(payload.split("_")[1])
                b = db.get_book(bid)
                if b:
                    parts = db.list_book_parts(bid)
                    total_size = b[5] or 0
                    total_dur = b[6] or 0
                    download = b[8] or 0
                    base = f"{b[1]} — {b[2]}"
                    deep = deep_link_for_book(bid)
                    buy = b[9] if len(b) > 9 else None
                    if b[4] == "pdf":
                        info = f"⬇️ Yuklashlar: {download}\n📦 Hajm: {fmt_size(total_size)}"
                        for idx, p in enumerate(parts, start=1):
                            await message.bot.send_chat_action(message.chat.id, ChatAction.UPLOAD_DOCUMENT)
                            await asyncio.sleep(1)
                            caption = f"{base}{bot_signature()}\n{blockquote(info)}"
                            saved_flag = db.is_book_saved(message.from_user.id, bid)
                            resp = await message.answer_document(p[1], caption=caption, parse_mode="HTML",
                                                                 reply_markup=book_actions_keyboard(bid, deep, buy,
                                                                                                    saved=saved_flag))
                            await add_reaction(message.bot, message.chat.id, resp.message_id, "🎉", is_big=False)
                    else:
                        info = f"⏱ Davomiylik: {fmt_duration(total_dur)}\n⬇️ Yuklashlar: {download}\n📦 Hajm: {fmt_size(total_size)}"
                        for idx, p in enumerate(parts, start=1):
                            await message.bot.send_chat_action(message.chat.id, ChatAction.UPLOAD_DOCUMENT)
                            await asyncio.sleep(1)
                            caption = f"{base}{bot_signature()}\n{blockquote(info)}"
                            saved_flag = db.is_book_saved(message.from_user.id, bid)
                            resp = await message.answer_audio(p[1], caption=caption, parse_mode="HTML",
                                                              reply_markup=book_actions_keyboard(bid, deep, buy,
                                                                                                 saved=saved_flag))
                            await add_reaction(message.bot, message.chat.id, resp.message_id, "🎉", is_big=False)
                    db.inc_download(bid)
                    return
        except Exception:
            pass
    await message.answer(blockquote(
        " Assalom-u alaykum. Kutubxona botiga xush kelibsiz! Kitob qidirishdan oldin kuling yoki jilmaying 😁😃😄😁🙃🙂😉"),
        parse_mode="HTML", reply_markup=main_menu())


@user_router.message(Command("donate"))
async def donate_cmd(message: Message):
    txt = (f"<b>Bot rivojiga o'z hissangizni qo'shing</b>\n"
           f"💳 Karta raqami: <code>{CARD_NUMBER}</code> [ {CARD_HOLDER} ]")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Nusxalash 📋", copy_text=CARD_NUMBER)]
    ])
    await message.answer(txt, reply_markup=kb, parse_mode="HTML")


@user_router.message(F.text == "🔎 Qidiruv")
async def search_entry(message: Message):
    if db.is_blocked(message.from_user.id):
        await answer_with_effect(message, blockquote("🚫 Siz admin tomonidan bloklangansiz 😡😈👿"), effect="dislike")
        return
    await message.answer(blockquote("Faqat kitob nomi yoki faqat muallifni kiriting"), parse_mode="HTML", )


@user_router.message(F.text == "🗂 Kategoriyalar")
async def categories(message: Message):
    if db.is_blocked(message.from_user.id):
        await answer_with_effect(message, blockquote("🚫 Siz admin tomonidan bloklangansiz 😡😈👿"), effect="dislike")
        return
    cats = db.list_categories()
    if not cats:
        await message.answer("🫩 Kategoriyalar mavjud emas")
        return
    kb = categories_keyboard(cats)
    await message.answer(blockquote("😲🫢😀 Kategoriya tanlang"), parse_mode="HTML", reply_markup=kb)


@user_router.callback_query(F.data.startswith("cat:"))
async def cat_pick(cb: CallbackQuery, state: FSMContext):
    if db.is_blocked(cb.from_user.id):
        await answer_with_effect(cb.message, blockquote("🚫 Siz admin tomonidan bloklangansiz 😡😈👿"), effect="dislike")
        await cb.answer()

        return
    ok = await check_membership(cb.message.bot, cb.from_user.id)
    if not ok:
        await answer_with_effect(cb.message, blockquote("❌ Avval kanalga a‘zo bo‘ling 🤬"),
                                 reply_markup=join_channels_keyboard(), effect="dislike")
        await cb.answer()
        return
    cid = int(cb.data.split(":")[1])
    rows = db.books_by_category(cid, limit=10000)
    if not rows:
        await cb.message.answer(blockquote("🫩 Bu kategoriyada kitob yo‘q"), parse_mode="HTML")
        await cb.answer()
        return
    random.shuffle(rows)
    start = 0
    end = min(start + 10, len(rows))
    page_rows = rows[start:end]
    items = [(r[1], f"pick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=False, has_next=(end < len(rows)))
    header = f"Natija: {len(rows)} ta. Ko‘rsatilayapdi: [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}  {r[4]} 📥" for idx, r in enumerate(page_rows, start=1)])
    await answer_with_effect(cb.message, blockquote(header), effect="celebration")
    await cb.message.answer(body, reply_markup=kb)
    await state.update_data(cat_rows=rows, cat_page=0)
    await state.set_state(CatState.paging)
    await cb.answer()


@user_router.callback_query(CatState.paging, F.data == "sp:next")
async def cat_next(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    rows = data.get("cat_rows", [])
    page = int(data.get("cat_page", 0)) + 1
    total = len(rows)
    start = page * 10
    if start >= total:
        await cb.answer()
        return
    end = min(start + 10, total)
    page_rows = rows[start:end]
    items = [(r[1], f"pick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=True, has_next=(end < total))
    header = f"Natija: {total} ta. Ko‘rsatilayapdi: [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}  {r[4]} 📥" for idx, r in enumerate(page_rows, start=1)])
    await cb.message.edit_text(blockquote(header), parse_mode="HTML")
    await cb.message.answer(body, reply_markup=kb)
    await state.update_data(cat_page=page)
    await cb.answer()


@user_router.callback_query(CatState.paging, F.data == "sp:prev")
async def cat_prev(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    rows = data.get("cat_rows", [])
    page = max(int(data.get("cat_page", 0)) - 1, 0)
    total = len(rows)
    start = page * 10
    end = min(start + 10, total)
    page_rows = rows[start:end]
    items = [(r[1], f"pick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=(page > 0), has_next=(end < total))
    header = f"Natija: {total} ta. Ko‘rsatilayapdi: [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}  {r[4]} 📥" for idx, r in enumerate(page_rows, start=1)])
    await cb.message.edit_text(blockquote(header), parse_mode="HTML")
    await cb.message.answer(body, reply_markup=kb)
    await state.update_data(cat_page=page)
    await cb.answer()


@user_router.message(F.text == "📊 Statistika")
async def stats(message: Message):
    if db.is_blocked(message.from_user.id):
        await answer_with_effect(message, blockquote("🚫 Siz admin tomonidan bloklangansiz 😡😈👿"), effect="dislike")
        return
    ok = await check_membership(message.bot, message.from_user.id)
    if not ok:
        await answer_with_effect(message, blockquote("❌ Avval kanalga a‘zo bo‘ling 🤬"),
                                 reply_markup=join_channels_keyboard(), effect="dislike")
        return
    a, p = db.stats_counts()
    lines = [f"Audio: {a}", f"PDF: {p}", "", "Kategoriyalar:"]
    cats = db.list_categories()
    for cid, name in cats:
        rows = db.books_by_category(cid, limit=10000)
        total = len(rows)
        pdf_count = sum(1 for r in rows if r[3] == "pdf")
        audio_count = total - pdf_count
        lines.append(f"• {name}: {total} ta (Audio: {audio_count}, PDF: {pdf_count})")
    await message.answer(blockquote("\n".join(lines)), parse_mode="HTML")


@user_router.message(F.text == "❓ Yordam")
async def help(message: Message):
    if db.is_blocked(message.from_user.id):
        await answer_with_effect(message, blockquote("🚫 Siz admin tomonidan bloklangansiz 😡😈👿"), effect="dislike")
        return
    ok = await check_membership(message.bot, message.from_user.id)
    if not ok:
        await answer_with_effect(message, blockquote("❌ Avval kanalga a‘zo bo‘ling 🤬"),
                                 reply_markup=join_channels_keyboard(), effect="dislike")
        return
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    txt = ("<blockquote>Botdan foydalanish:\n"
           "1) Qidiruvga faqat kitob nomi yoki faqat muallif kiriting.\n"
           "2) Kategoriyalar bo‘yicha tanlang.\n"
           "3) Tanlangan kitobni bosib oling.\n\n"
           "Xususiyatlar:\n"
           "• Audio va PDF kitoblar\n"
           "• Top 10, Yaqinda yuklanganlar, Tasodifiy ro‘yxatlar\n"
           "• Avtomatik qidiruv va topilmagan so‘rovlar statistikasi\n"
           "• Guruhda #so‘z bilan tezkor qidiruv\n"
           "</blockquote> 🔎📚🏆")
    kb = None
    if ADMIN_CONTACT_URL:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👨‍💼 Admin bilan bog‘lanish", url=ADMIN_CONTACT_URL)]
        ])
    await message.answer(txt, parse_mode="HTML", reply_markup=kb)


@user_router.message(F.text == "⭐ Mashxur kitoblar")
async def popular_entry(message: Message):
    if db.is_blocked(message.from_user.id):
        await answer_with_effect(message, blockquote("🚫 Siz admin tomonidan bloklangansiz 😡😈👿"), effect="dislike")
        return
    await message.answer(blockquote("Mashxur bo‘limdan tanlang"), parse_mode="HTML", reply_markup=popular_menu())


@user_router.message(F.text == "👤 Mening profilim")
async def profile_entry(message: Message):
    if db.is_blocked(message.from_user.id):
        await answer_with_effect(message, blockquote("🚫 Siz admin tomonidan bloklangansiz 😡😈👿"), effect="dislike")
        return
    await message.answer(blockquote("Profil bo‘limidan tanlang"), parse_mode="HTML", reply_markup=profile_menu())


@user_router.message(F.text == "🏠 Asosiy menyu")
async def profile_back_to_main(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("🏠 Asosiy menyu", reply_markup=main_menu())


@user_router.message(F.text == "🆔 Foydalanuvchi ID")
async def show_user_id(message: Message):
    await message.answer(blockquote(f"Sizning ID: {message.from_user.id}"), parse_mode="HTML")


@user_router.message(F.text == "🏆 Top 10 kitoblar")
async def top10_entry(message: Message, state: FSMContext):
    if db.is_blocked(message.from_user.id):
        await answer_with_effect(message, blockquote("🚫 Siz admin tomonidan bloklangansiz 😡😈👿"), effect="dislike")
        return
    rows = db.top_books(10)
    if not rows:
        await message.answer(blockquote("🫩 Top kitoblar mavjud emas"), parse_mode="HTML")
        return
    start = 0
    end = min(start + 5, len(rows))
    page_rows = rows[start:end]
    items = [(r[1], f"pick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=False, has_next=(end < len(rows)))
    header = f"Natija: {len(rows)} ta. Ko‘rsatilayapdi: [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}" for idx, r in enumerate(page_rows, start=1)])
    await answer_with_effect(message, blockquote(header), effect="celebration")
    await message.answer(body, reply_markup=kb)
    await state.update_data(top_rows=rows, top_page=0)
    await state.set_state(TopState.paging)


@user_router.callback_query(TopState.paging, F.data == "sp:next")
async def top_next(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    rows = data.get("top_rows", [])
    page = int(data.get("top_page", 0)) + 1
    total = len(rows)
    start = page * 5
    if start >= total:
        await cb.answer()
        return
    end = min(start + 5, total)
    page_rows = rows[start:end]
    items = [(r[1], f"pick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=True, has_next=(end < total))
    header = f"Natija: {total} ta. Ko‘rsatilayapdi: [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}" for idx, r in enumerate(page_rows, start=1)])
    await cb.message.edit_text(blockquote(header), parse_mode="HTML")
    await cb.message.answer(body, reply_markup=kb)
    await state.update_data(top_page=page)
    await cb.answer()


@user_router.callback_query(TopState.paging, F.data == "sp:prev")
async def top_prev(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    rows = data.get("top_rows", [])
    page = max(int(data.get("top_page", 0)) - 1, 0)
    total = len(rows)
    start = page * 5
    end = min(start + 5, total)
    page_rows = rows[start:end]
    items = [(r[1], f"pick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=(page > 0), has_next=(end < total))
    header = f"Natija: {total} ta. Ko‘rsatilayapdi: [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}" for idx, r in enumerate(page_rows, start=1)])
    await cb.message.edit_text(blockquote(header), parse_mode="HTML")
    await cb.message.answer(body, reply_markup=kb)
    await state.update_data(top_page=page)
    await cb.answer()


@user_router.message(F.text == "🆕 Yaqinda 20 ta")
async def recent20_entry(message: Message, state: FSMContext):
    if db.is_blocked(message.from_user.id):
        await answer_with_effect(message, blockquote("🚫 Siz admin tomonidan bloklangansiz 😡😈👿"), effect="dislike")
        return
    rows = db.recent_books(20)
    if not rows:
        await message.answer(blockquote("🫩 Yaqinda yuklangan kitoblar yo‘q"), parse_mode="HTML")
        return
    start = 0
    end = min(start + 10, len(rows))
    page_rows = rows[start:end]
    items = [(r[1], f"pick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=False, has_next=(end < len(rows)))
    header = f"Natija: {len(rows)} ta. Ko‘rsatilayapdi: [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}" for idx, r in enumerate(page_rows, start=1)])
    await answer_with_effect(message, blockquote(header), effect="celebration")
    await message.answer(body, reply_markup=kb)
    await state.update_data(recent_rows=rows, recent_page=0)
    await state.set_state(RecentState.paging)


@user_router.message(F.text == "🆕 Yaqinda yuklanganlar")
async def recent_yuklangan_entry(message: Message, state: FSMContext):
    await recent20_entry(message, state)


@user_router.message(F.text == "🎲 Tasodifiy kitoblar")
async def random_entry(message: Message, state: FSMContext):
    if db.is_blocked(message.from_user.id):
        await answer_with_effect(message, blockquote("🚫 Siz admin tomonidan bloklangansiz 😡😈👿"), effect="dislike")
        return
    rows = db.random_books(30)
    if not rows:
        await message.answer(blockquote("🫩 Kitoblar mavjud emas"), parse_mode="HTML")
        return
    start = 0
    end = min(start + 10, len(rows))
    page_rows = rows[start:end]
    items = [(r[1], f"pick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=False, has_next=(end < len(rows)))
    header = f"Tasodifiy: {len(rows)} dan [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}" for idx, r in enumerate(page_rows, start=1)])
    await answer_with_effect(message, blockquote(header), effect="celebration")
    await message.answer(body, reply_markup=kb)
    await state.update_data(random_rows=rows, random_page=0)
    await state.set_state(RandomState.paging)


@user_router.callback_query(RandomState.paging, F.data == "sp:next")
async def random_next(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    rows = data.get("random_rows", [])
    page = int(data.get("random_page", 0)) + 1
    total = len(rows)
    start = page * 10
    if start >= total:
        await cb.answer()
        return
    end = min(start + 10, total)
    page_rows = rows[start:end]
    items = [(r[1], f"pick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=True, has_next=(end < total))
    header = f"Tasodifiy: {total} dan [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}" for idx, r in enumerate(page_rows, start=1)])
    await cb.message.edit_text(blockquote(header), parse_mode="HTML")
    await cb.message.answer(body, reply_markup=kb)
    await state.update_data(random_page=page)
    await cb.answer()


@user_router.callback_query(RandomState.paging, F.data == "sp:prev")
async def random_prev(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    rows = data.get("random_rows", [])
    page = max(int(data.get("random_page", 0)) - 1, 0)
    total = len(rows)
    start = page * 10
    end = min(start + 10, total)
    page_rows = rows[start:end]
    items = [(r[1], f"pick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=(page > 0), has_next=(end < total))
    header = f"Tasodifiy: {total} dan [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}" for idx, r in enumerate(page_rows, start=1)])
    await cb.message.edit_text(blockquote(header), parse_mode="HTML")
    await cb.message.answer(body, reply_markup=kb)
    await state.update_data(random_page=page)
    await cb.answer()


@user_router.callback_query(RecentState.paging, F.data == "sp:next")
async def recent_next(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    rows = data.get("recent_rows", [])
    page = int(data.get("recent_page", 0)) + 1
    total = len(rows)
    start = page * 10
    if start >= total:
        await cb.answer()
        return
    end = min(start + 10, total)
    page_rows = rows[start:end]
    items = [(r[1], f"pick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=True, has_next=(end < total))
    header = f"Natija: {total} ta. Ko‘rsatilayapdi: [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}" for idx, r in enumerate(page_rows, start=1)])
    await cb.message.edit_text(blockquote(header), parse_mode="HTML")
    await cb.message.answer(body, reply_markup=kb)
    await state.update_data(recent_page=page)
    await cb.answer()


@user_router.callback_query(RecentState.paging, F.data == "sp:prev")
async def recent_prev(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    rows = data.get("recent_rows", [])
    page = max(int(data.get("recent_page", 0)) - 1, 0)
    total = len(rows)
    start = page * 10
    end = min(start + 10, total)
    page_rows = rows[start:end]
    items = [(r[1], f"pick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=(page > 0), has_next=(end < total))
    header = f"Natija: {total} ta. Ko‘rsatilayapdi: [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}" for idx, r in enumerate(page_rows, start=1)])
    await cb.message.edit_text(blockquote(header), parse_mode="HTML")
    await cb.message.answer(body, reply_markup=kb)
    await state.update_data(recent_page=page)
    await cb.answer()


@user_router.callback_query(F.data.startswith("pick:"))
async def pick_book(cb: CallbackQuery):
    if db.is_blocked(cb.from_user.id):
        await answer_with_effect(cb.message, "Siz bloklangansiz 😡", effect="dislike")
        await cb.answer()
        return
    bid = int(cb.data.split(":")[1])
    b = db.get_book(bid)
    parts = db.list_book_parts(bid)
    total_size = b[5] or 0
    total_dur = b[6] or 0
    download = b[8] or 0
    total_parts = len(parts)
    base = f"{b[1]} — {b[2]}"
    deep = deep_link_for_book(bid)
    buy = b[9] if len(b) > 9 else None
    is_saved = db.is_book_saved(cb.from_user.id, bid)
    if b[4] == "pdf":
        info = f"⬇️ Yuklashlar: {download}\n📦 Hajm: {fmt_size(total_size)}"
        for idx, p in enumerate(parts, start=1):
            await cb.message.bot.send_chat_action(cb.message.chat.id, ChatAction.UPLOAD_DOCUMENT)
            await asyncio.sleep(1)
            if total_parts == 1:
                caption = f"{base}{bot_signature()}\n{blockquote(info)}"
            else:
                pinfo = f"{info}\n🧩 Qism: {idx}/{total_parts}\n📦 Qism hajmi: {fmt_size(p[3])}"
                caption = f"{base}{bot_signature()}\n{blockquote(pinfo)}"
            try:
                resp = await cb.message.answer_document(p[1], caption=caption, parse_mode="HTML",
                                                        reply_markup=book_actions_keyboard(bid, deep, buy,
                                                                                           saved=is_saved))
                await add_reaction(cb.message.bot, cb.message.chat.id, resp.message_id, "🎉", is_big=False)
            except Exception:
                await cb.message.answer("😵‍💫 Faylni yuborishda xatolik yuz berdi. 🥵 Keyinroq qaytadan urinib ko'ring")
    else:
        info = f"⏱ Davomiylik: {fmt_duration(total_dur)}\n⬇️ Yuklashlar: {download}\n📦 Hajm: {fmt_size(total_size)}"
        for idx, p in enumerate(parts, start=1):
            await cb.message.bot.send_chat_action(cb.message.chat.id, ChatAction.UPLOAD_DOCUMENT)
            await asyncio.sleep(1)
            if total_parts == 1:
                caption = f"{base}{bot_signature()}\n{blockquote(info)}"
            else:
                pinfo = f"{info}\n🧩 Qism: {idx}/{total_parts}\n⏱ Qism davomiyligi: {fmt_duration(p[4])}"
                caption = f"{base}{bot_signature()}\n{blockquote(pinfo)}"
            try:
                resp = await cb.message.answer_audio(p[1], caption=caption, parse_mode="HTML",
                                                     reply_markup=book_actions_keyboard(bid, deep, buy, saved=is_saved))
                await add_reaction(cb.message.bot, cb.message.chat.id, resp.message_id, "🎉", is_big=False)
            except Exception:
                await cb.message.answer("😵‍💫 Audio yuborishda xatolik yuz berdi. Keyinroq qayta urinib ko'ring")
    db.inc_download(bid)
    await cb.answer()


@user_router.message(F.sticker | F.video | F.voice | F.photo | F.animation | F.video_note)
async def non_text_search_warning(message: Message):
    if db.is_blocked(message.from_user.id):
        await answer_with_effect(message, blockquote("🚫 Siz admin tomonidan bloklangansiz 😡😈👿"), effect="dislike")
        return
    ok = await check_membership(message.bot, message.from_user.id)
    if not ok:
        await answer_with_effect(message, blockquote("❌ Avval kanalga a‘zo bo‘ling 🤬"),
                                 reply_markup=join_channels_keyboard(), effect="dislike")
        return
    txt = ("Qidiruv faqat matnli xabarlar bilan ishlaydi.\n"
           "Stiker, video, audio, rasm va ovoz xabarlari orqali qidiruv qilinmaydi.\n"
           "Iltimos, matn yuboring.")
    await answer_with_effect(message, blockquote(txt), effect="dislike")


@user_router.message(F.text & ~F.text.in_(
    ["🔎 Qidiruv", "🗂 Kategoriyalar", "📊 Statistika", "❓ Yordam", "🏆 Top kitoblar", "🏠 Asosiy menyu", "⬅️ Ortga",
     "❌ Bekor qilish", "👤 Mening profilim", "💾 Saqlangan kitoblar", "📜 Istaklarim", "🆔 Foydalanuvchi ID",
     "⭐ Mashxur kitoblar"]))
async def search_fallback(message: Message, state: FSMContext):
    if db.is_blocked(message.from_user.id):
        await answer_with_effect(message, "Siz bloklangansiz 😡", effect="fire")
        return
    ok = await check_membership(message.bot, message.from_user.id)
    if not ok:
        await answer_with_effect(message, "Avval guruhga qo‘shiling 🤬", reply_markup=join_channels_keyboard(),
                                 effect="dislike")
        return
    q = message.text.strip()
    if q.lower() in ["rahmat", "katta rahmat", "rahmat katta", "rahmat kotta", "rahmat kottakon", "kottakon rahmat",
                     "raxmat", "thank you", "thank", "rahmat jarvis", "jarvis rahmat", "yashavor jarvis", "malatsi"]:
        await add_reaction(message.bot, message.chat.id, message.message_id, "🤗", is_big=False)
        await answer_with_effect(message, blockquote(f"Sizga ham rahmat. 😊 Yordamim tekganidan hursandman"
                                                     f"\n 🙏🏻 Iltimos siz ham bizga yordam bering"
                                                     f"Bot linkini do'stlaringizga ham uzating\n{bot_link()}"),
                                 effect="love")
        return
    if q.lower() in ["Jarvis", "alo", "jarvis shu yerdasmisan", "kitob topishda yordam ber",
                     "jarvis kitob topishda yordam ber", "jarvis", "salom", "jarvis menga kitob topib ber",
                     "menga kitob topib ber" "help", "jarvis qayerdasan", "kitob kerak", "salom jarvis"]:
        await add_reaction(message.bot, message.chat.id, message.message_id, "🥱", is_big=False)
        await answer_with_effect(message, blockquote(
            f" 🫡 Shu yerdaman, kechirasiz ko'zim ketib qolibdi🥴😴🥱🫢😄 qanday kitob kerak"), effect="fire")
        return
    rows = db.search_books(q, limit=100)
    if not rows:
        db.save_missing_query(message.from_user.id, q)
        kb = None
        txt = blockquote(
            "Siz qidirgan kitob topilmadi 😞bu haqda adminga xabar yuboraman. Kitob tez kunda qo'shilishi mumkin.")
        if BOOK_HELP_CHANNEL_LINK:
            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            txt = blockquote("Siz qidirgan kitob topilmadi 😞\nKitob buyurtma qilish")
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="📢 Kanal", url=BOOK_HELP_CHANNEL_LINK)]])
        await answer_with_effect(message, txt, reply_markup=kb)
        await add_reaction(message.bot, message.chat.id, message.message_id, "😢", is_big=True)
        return
    await add_reaction(message.bot, message.chat.id, message.message_id, "🫡", is_big=False)
    await state.update_data(search_query=q, search_rows=rows, search_page=0)
    start = 0
    end = min(start + 10, len(rows))
    page_rows = rows[start:end]
    items = [(r[1], f"pick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=False, has_next=(end < len(rows)))
    header = f"Natija: {len(rows)} ta. Ko‘rsatilayapdi: [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}  {r[4]} 📥" for idx, r in enumerate(page_rows, start=1)])
    await answer_with_effect(message, blockquote(header), effect="celebration")
    await message.answer(body, reply_markup=kb)
    await state.set_state(SearchState.paging)


@user_router.callback_query(SearchState.paging, F.data == "sp:next")
async def search_next(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    rows = data.get("search_rows", [])
    if not rows:
        # fallback if state lost
        q = data.get("search_query", "")
        rows = db.search_books(q, limit=100)
        await state.update_data(search_rows=rows)

    page = int(data.get("search_page", 0)) + 1
    total = len(rows)
    start = page * 10
    if start >= total:
        await cb.answer()
        return
    end = min(start + 10, total)
    page_rows = rows[start:end]
    items = [(r[1], f"pick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=True, has_next=(end < total))
    header = f"Natija: {total} ta. Ko‘rsatilayapdi: [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}  {r[4]} 📥" for idx, r in enumerate(page_rows, start=1)])
    await cb.message.edit_text(blockquote(header), parse_mode="HTML")
    await cb.message.answer(body, reply_markup=kb)
    await state.update_data(search_page=page)
    await cb.answer()


@user_router.callback_query(SearchState.paging, F.data == "sp:prev")
async def search_prev(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    rows = data.get("search_rows", [])
    if not rows:
        q = data.get("search_query", "")
        rows = db.search_books(q, limit=100)
        await state.update_data(search_rows=rows)

    page = max(int(data.get("search_page", 0)) - 1, 0)
    total = len(rows)
    start = page * 10
    end = min(start + 10, total)
    page_rows = rows[start:end]
    items = [(r[1], f"pick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=(page > 0), has_next=(end < total))
    header = f"Natija: {total} ta. Ko‘rsatilayapdi: [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}  {r[4]} 📥" for idx, r in enumerate(page_rows, start=1)])
    await cb.message.edit_text(blockquote(header), parse_mode="HTML")
    await cb.message.answer(body, reply_markup=kb)
    await state.update_data(search_page=page)
    await cb.answer()


@user_router.message(F.audio | F.document)
async def forward_user_files(message: Message, bot):
    if db.is_blocked(message.from_user.id):
        await answer_with_effect(message, "Siz bloklangansiz 😡 ", effect="dislike")
        return
    ok = await check_membership(message.bot, message.from_user.id)
    if not ok:
        await answer_with_effect(message, blockquote("❌ Avval guruhga a‘zo bo‘ling 🤬"),
                                 reply_markup=join_channels_keyboard(), effect="dislike")
        return
    if message.audio or message.document:
        try:
            if message.audio:
                db.save_user_upload(message.from_user.id, "audio", message.audio.file_id,
                                    size=message.audio.file_size or 0, duration_seconds=message.audio.duration or 0)
            else:
                db.save_user_upload(message.from_user.id, "pdf", message.document.file_id,
                                    size=message.document.file_size or 0, duration_seconds=0)
        except Exception:
            pass
        try:
            if FORWARD_GROUP_ID:
                await bot.copy_message(chat_id=FORWARD_GROUP_ID, from_chat_id=message.chat.id,
                                       message_id=message.message_id)
                await asyncio.sleep(0.02)
        except Exception:
            pass
        await add_reaction(message.bot, message.chat.id, message.message_id, "🤝", is_big=False)
        await answer_with_effect(message, blockquote(
            "Yuborgan kitobningiz uchun rahmat. Tez oradan ko‘rib chiqilib serverga yuklanadi"), effect="like")


group_router = Router()


@group_router.message(F.chat.type.in_(["group", "supergroup", "channel"]))
async def group_search(message: Message, state: FSMContext):
    if not message.text:
        return
    if message.text.strip().lower() in ["rahmat", "katta rahmat", "rahmat katta", "raxmat katta", "rahmat kottakon",
                                        "kottakon rahmat", "raxmat", "thank", "rahmat jarvis", "jarvis rahmat",
                                        "yashavor jarvis", "malatsi", "voy rahamt",
                                        "voy rahmat anchadan beri izlab yurgandim", "voy raxamt topa olmayotgandim",
                                        "voy rahmat topa olmayotgandim", "voy kotta rahmat"]:
        await add_reaction(message.bot, message.chat.id, message.message_id, "😇", is_big=False)
        me = await message.bot.get_me()
        bot_username = me.username
        txt = (f"Ollohim ilmingizni ziyoda qilsin 😊\n"
               f"Guruhda kitobxonlar ko'payishi uchun bot linkini do'stlaringizga ham ulashing 😉\n"
               f"@{bot_username}")
        share_url = f"https://t.me/share/url?url=https://t.me/{bot_username}"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="♻️ Ulashish", url=share_url)]
        ])
        await message.reply(txt, reply_markup=kb, parse_mode="HTML")
        return
    if message.text.strip().lower() in ["tezkor chaqruv", "Bilmayman", "17"]:
        await add_reaction(message.bot, message.chat.id, message.message_id, "⚡️", is_big=False)
        await message.reply(blockquote("Meni favqulotda kimdir chaqirdi 🏩⛑🚨"), parse_mode="HTML")
        await message.reply(
            " Men barcha ishlarimni tashlab sizga yordamga keldim. kitobnizing boshiga # belgin qo'yib qidirng")
        return
    if message.text.strip().lower() in ["jarvis", "jarvis shu yerdasmisan?", "kitob topishda yordam ber",
                                        "jarvis kitob topishda yordam ber", "jarvis yordaming kerak",
                                        "jarvis menga kitob topib ber", "menga kitob topib bering", "help",
                                        "jarvis qayerdasan", "kitob kerak", "dangasalik qilma jarvis",
                                        "kitob topib bering", "pdf kitob kerak", "shu kitobning pdf varyaniti bormi",
                                        "shu kitobning audiosi bormi", "shu kitobning audiosi bormi?",
                                        "bormi shu kitob?", "audio bormi?", "audiosi bormi?", "pdf bormi?",
                                        "jarvis yordam"]:
        await add_reaction(message.bot, message.chat.id, message.message_id, "🥱", is_big=False)
        await message.reply(blockquote("😴🫩😳🫣🤭 ko'zim ketib qolibdi🤠.......Jarvis eshitadi."), parse_mode="HTML")
        await message.reply("'#' so'z bilan kitob muallifi yoki nomini yozing")
        return
    if message.text.strip().lower() in ["Jarvis", "assalomu alaykum", "assalom-u alaykum", "salom", "ассалому алайкум",
                                        "salom jarvis", "jarvis salom"]:
        await add_reaction(message.bot, message.chat.id, message.message_id, "🥰", is_big=False)
        await message.reply(blockquote(
            "Assalom-u alakum qanday kitob qidiraypsiz? Guruhda kitob qidirishni bilansizmi? Agar bilmasangiz {jarvis yordam} deb yozing"),
            parse_mode="HTML")
        return
    if not message.text.startswith("#"):
        return
    txt = message.text
    parts = [p.strip() for p in txt.split("#") if p.strip()]
    found = {}
    for term in parts:
        res = db.search_books(term, limit=100)
        for r in res:
            found[r[0]] = r
    rows = list(found.values())
    if not rows:
        try:
            uid = getattr(message.from_user, "id", 0) or 0
            for term in parts:
                db.save_missing_query(uid, term)
        except Exception:
            pass
        await add_reaction(message.bot, message.chat.id, message.message_id, "🤷‍♂️", is_big=True)
        return

    await state.update_data(group_search_rows=rows, group_search_page=0)

    start = 0
    end = min(10, len(rows))
    page_rows = rows[start:end]
    items = [(r[1], f"gpick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=False, has_next=(end < len(rows)), prev_cb="gsp:prev",
                                next_cb="gsp:next", add_back=False)

    header = f"Natija: {len(rows)} ta. Ko‘rsatilayapdi: [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}  {r[4]} 📥" for idx, r in enumerate(page_rows, start=1)])
    full_text = f"{blockquote(header)}\n{body}"
    await add_reaction(message.bot, message.chat.id, message.message_id, "👌", is_big=True)
    resp = await message.answer(full_text, reply_markup=kb, parse_mode="HTML")
    await add_reaction(message.bot, message.chat.id, resp.message_id, "😎", is_big=False)
    await state.set_state(GroupSearchState.paging)


@group_router.callback_query(GroupSearchState.paging, F.data == "gsp:next")
async def group_search_next(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    rows = data.get("group_search_rows", [])
    if not rows:
        await cb.answer("Eski qidiruv natijalari topilmadi", show_alert=True)
        return

    page = int(data.get("group_search_page", 0)) + 1
    total = len(rows)
    start = page * 10
    if start >= total:
        await cb.answer()
        return
    end = min(start + 10, total)
    page_rows = rows[start:end]
    items = [(r[1], f"gpick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=True, has_next=(end < total), prev_cb="gsp:prev", next_cb="gsp:next",
                                add_back=False)
    header = f"Natija: {total} ta. Ko‘rsatilayapdi: [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}  {r[4]} 📥" for idx, r in enumerate(page_rows, start=1)])
    full_text = f"{blockquote(header)}\n{body}"
    await cb.message.edit_text(full_text, reply_markup=kb, parse_mode="HTML")
    await state.update_data(group_search_page=page)
    await cb.answer()


@group_router.callback_query(GroupSearchState.paging, F.data == "gsp:prev")
async def group_search_prev(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    rows = data.get("group_search_rows", [])
    if not rows:
        await cb.answer("Eski qidiruv natijalari topilmadi", show_alert=True)
        return

    page = max(int(data.get("group_search_page", 0)) - 1, 0)
    total = len(rows)
    start = page * 10
    end = min(start + 10, total)
    page_rows = rows[start:end]
    items = [(r[1], f"gpick:{r[0]}") for r in page_rows]
    kb = paged_numbers_keyboard(items, has_prev=(page > 0), has_next=(end < total), prev_cb="gsp:prev",
                                next_cb="gsp:next", add_back=False)
    header = f"Natija: {total} ta. Ko‘rsatilayapdi: [{start + 1} - {end}]"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}  {r[4]} 📥" for idx, r in enumerate(page_rows, start=1)])
    full_text = f"{blockquote(header)}\n{body}"
    await cb.message.edit_text(full_text, reply_markup=kb, parse_mode="HTML")
    await state.update_data(group_search_page=page)
    await cb.answer()


# @group_router.message(F.sticker | F.video | F.voice | F.photo | F.animation | F.video_note)
# async def group_non_text_warning(message: Message):
#     txt = ("Guruhda qidiruv faqat matnli xabarlar bilan ishlaydi.\n"
#            "Stiker, video, audio, rasm va ovoz xabarlari orqali qidiruv qilinmaydi.\n"
#            "Iltimos, matn yuboring. Qidiruv uchun '#so‘z' bilan yozing.")
#     await message.reply(blockquote(txt), parse_mode="HTML")

@user_router.callback_query(F.data == "verify_join")
async def verify_join(cb: CallbackQuery):
    ok = await check_membership(cb.message.bot, cb.from_user.id)
    if ok:
        await cb.message.answer(blockquote("Rahmat! 😇 Endi botdan bemalol foydalanishingiz mumkin."), parse_mode="HTML",
                                reply_markup=main_menu())
    else:
        await cb.message.answer(blockquote("❌ Hali a‘zo bo‘lmadingiz. Avval qo‘shiling 🤬."), parse_mode="HTML",
                                reply_markup=join_channels_keyboard())
    await cb.answer()


@group_router.message(Command("chatid"))
async def chat_id(message: Message):
    await message.answer(str(message.chat.id))


@group_router.callback_query(F.data.startswith("gpick:"))
async def group_pick(cb: CallbackQuery):
    try:
        await cb.answer()
    except Exception:
        pass
    bid = int(cb.data.split(":")[1])
    b = db.get_book(bid)
    parts = db.list_book_parts(bid)
    total_size = b[5] or 0
    total_dur = b[6] or 0
    download = b[8] or 0
    total_parts = len(parts)
    base = f"{b[1]} — {b[2]}"
    deep = deep_link_for_book(bid)
    buy = b[9] if len(b) > 9 else None
    if b[4] == "pdf":
        info = f"⬇️ Yuklashlar: {download}\n📦 Hajm: {fmt_size(total_size)}"
        for idx, p in enumerate(parts, start=1):
            sent = False
            while not sent:
                try:
                    await cb.message.bot.send_chat_action(cb.message.chat.id, ChatAction.UPLOAD_DOCUMENT)
                    await asyncio.sleep(2)
                    if total_parts == 1:
                        caption = f"{base}{bot_signature()}\n{blockquote(info)}"
                    else:
                        pinfo = f"{info}\n🧩 Qism: {idx}/{total_parts}\n📦 Qism hajmi: {fmt_size(p[3])}"
                        caption = f"{base}{bot_signature()}\n{blockquote(pinfo)}"
                    resp = await cb.message.answer_document(p[1], caption=caption, parse_mode="HTML",
                                                            reply_markup=book_actions_keyboard(bid, deep, buy,
                                                                                               include_save=False))
                    await add_reaction(cb.message.bot, cb.message.chat.id, resp.message_id, "✍️", is_big=False)
                    sent = True
                except TelegramRetryAfter as e:
                    await asyncio.sleep(e.retry_after + 1)
                except Exception as e:
                    await cb.message.answer(
                        "😵‍💫 Faylni yuborishda xatolik yuz berdi. 🥵 Keyinroq qaytadan urinib ko'ring")
                    sent = True
    else:
        info = f"⏱ Davomiylik: {fmt_duration(total_dur)}\n⬇️ Yuklashlar: {download}\n📦 Hajm: {fmt_size(total_size)}"
        for idx, p in enumerate(parts, start=1):
            sent = False
            while not sent:
                try:
                    await cb.message.bot.send_chat_action(cb.message.chat.id, ChatAction.UPLOAD_DOCUMENT)
                    await asyncio.sleep(2)
                    if total_parts == 1:
                        caption = f"{base}{bot_signature()}\n{blockquote(info)}"
                    else:
                        pinfo = f"{info}\n🧩 Qism: {idx}/{total_parts}\n⏱ Qism davomiyligi: {fmt_duration(p[4])}"
                        caption = f"{base}{bot_signature()}\n{blockquote(pinfo)}"
                    resp = await cb.message.answer_audio(p[1], caption=caption, parse_mode="HTML",
                                                         reply_markup=book_actions_keyboard(bid, deep, buy,
                                                                                            include_save=False))
                    await add_reaction(cb.message.bot, cb.message.chat.id, resp.message_id, "✍️", is_big=False)
                    sent = True
                except TelegramRetryAfter as e:
                    await asyncio.sleep(e.retry_after + 1)
                except Exception as e:
                    await cb.message.answer(
                        "😵‍💫 Audio yuborishda xatolik yuz berdi. 🥵 Keyinroq qaytadan urinib ko'ring")
                    sent = True
    db.inc_download(bid)


@user_router.callback_query(F.data == "back")
async def back_generic(cb: CallbackQuery):
    await cb.message.answer("🏠 Asosiy menyu", reply_markup=main_menu())
    await cb.answer()


@user_router.message(F.text.in_(["🏠 Asosiy menyu", "⬅️ Ortga"]))
async def back_text(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("🏠 Asosiy menyu", reply_markup=main_menu())


@user_router.message(F.text == "❌ Bekor qilish")
async def cancel_text(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Bekor qilindi", reply_markup=main_menu())


@user_router.callback_query(F.data.startswith("save:"))
async def save_book(cb: CallbackQuery, state: FSMContext):
    if db.is_blocked(cb.from_user.id):
        await cb.answer("Bloklangan", show_alert=True)
        return
    try:
        bid = int(cb.data.split(":")[1])
        if db.is_book_saved(cb.from_user.id, bid):
            await cb.answer("Bu kitob allaqachon saqlangan ✅", show_alert=False)
            txt = "Bu kitob allaqachon saqlangan. Ko‘rish uchun “💾 Saqlangan kitoblar” menyusini tanlang."
            await cb.message.answer(txt)
            return
        cnt = db.user_saved_count(cb.from_user.id) if hasattr(db, "user_saved_count") else 0
        if cnt >= 7:
            await cb.answer("Saqlash limiti 7 ta. Avval birini o‘chiring.", show_alert=True)
            txt = "Saqlash limiti 7 ta. Avval saqlanganlardan birini o‘chirib keyin saqlang."
            await cb.message.answer(txt)
            return
        db.add_saved_book(cb.from_user.id, bid)
        await cb.answer("Saqlandi ✅", show_alert=False)
        txt = "Kitob saqlangan kitoblarga muvaffaqiyatli saqlandi. Ko‘rish uchun “💾 Saqlangan kitoblar” menyusini tanlang."
        await cb.message.answer(txt)
    except Exception:
        await cb.answer("Saqlashda xatolik", show_alert=True)


@user_router.callback_query(F.data.startswith("rm:"))
async def remove_saved(cb: CallbackQuery, state: FSMContext):
    try:
        bid = int(cb.data.split(":")[1])
        db.remove_saved_book(cb.from_user.id, bid)
        await cb.answer("Saqlanganlardan o‘chirildi ✅", show_alert=False)
        cur_state = await state.get_state()
        if cur_state == SavedState.paging:
            data = await state.get_data()
            offset = int(data.get("saved_offset", 0))
            rows = db.list_saved_books(cb.from_user.id, offset=offset, limit=10)
            if not rows and offset > 0:
                offset = max(offset - 10, 0)
                rows = db.list_saved_books(cb.from_user.id, offset=offset, limit=10)
            items = [(r[1], f"pick:{r[0]}") for r in rows]
            kb = paged_numbers_keyboard(items, has_prev=(offset > 0), has_next=(len(rows) == 10), prev_cb="ss:prev",
                                        next_cb="ss:next")
            header = f"Sahifa: {(offset // 10) + 1}"
            body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}  {r[4]} 📥" for idx, r in
                              enumerate(rows, start=1)]) if rows else "Hozircha saqlangan kitob yo‘q"
            await cb.message.answer(blockquote(header), parse_mode="HTML")
            await cb.message.answer(body, reply_markup=kb if rows else back_menu())
            await state.update_data(saved_offset=offset)
    except Exception:
        await cb.answer("O‘chirishda xatolik", show_alert=True)


@user_router.message(F.text == "💾 Saqlanganlar")
async def saved_list_entry(message: Message, state: FSMContext):
    if db.is_blocked(message.from_user.id):
        await answer_with_effect(message, blockquote("🚫 Siz admin tomonidan bloklangansiz 😡😈👿"), effect="dislike")
        return
    rows = db.list_saved_books(message.from_user.id, offset=0, limit=10)
    if not rows:
        await message.answer(blockquote("Hozircha saqlangan kitob yo‘q"), parse_mode="HTML", reply_markup=back_menu())
        return
    items = [(r[1], f"pick:{r[0]}") for r in rows]
    kb = paged_numbers_keyboard(items, has_prev=False, has_next=(len(rows) == 10), prev_cb="ss:prev", next_cb="ss:next")
    header = f"Sizning saqlangan kitoblaringizdan birini tanlang"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}  {r[4]} 📥" for idx, r in enumerate(rows, start=1)])
    await message.answer(blockquote(header), parse_mode="HTML", reply_markup=back_menu())
    await message.answer(body, reply_markup=kb)
    await state.update_data(saved_offset=0)
    await state.set_state(SavedState.paging)


@user_router.message(F.text == "💾 Saqlangan kitoblar")
async def saved_list_entry_profile(message: Message, state: FSMContext):
    await saved_list_entry(message, state)


@user_router.callback_query(SavedState.paging, F.data == "ss:next")
async def saved_next(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    offset = int(data.get("saved_offset", 0)) + 10
    rows = db.list_saved_books(cb.from_user.id, offset=offset, limit=10)
    if not rows:
        await cb.answer()
        return
    items = [(r[1], f"pick:{r[0]}") for r in rows]
    kb = paged_numbers_keyboard(items, has_prev=True, has_next=(len(rows) == 10), prev_cb="ss:prev", next_cb="ss:next")
    header = f"Sahifa: {(offset // 10) + 1}"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}  {r[4]} 📥" for idx, r in enumerate(rows, start=1)])
    await cb.message.edit_text(blockquote(header), parse_mode="HTML")
    await cb.message.answer(body, reply_markup=kb)
    await state.update_data(saved_offset=offset)
    await cb.answer()


@user_router.callback_query(F.data.startswith("buy:"))
async def buy_missing(cb: CallbackQuery):
    await cb.answer("Kitob hali sotuvda emas", show_alert=True)


@inline_router.inline_query()
async def inline_share(iq: InlineQuery):
    q = (iq.query or "").strip()
    results = []
    if q.startswith("book_"):
        try:
            bid = int(q.split("_", 1)[1])
            b = db.get_book(bid)
            if b:
                parts = db.list_book_parts(bid)
                base = f"{b[1]} — {b[2]}"
                total_size = b[5] or 0
                total_dur = b[6] or 0
                download = b[8] or 0
                total_parts = len(parts)
                if b[4] == "pdf":
                    for idx, p in enumerate(parts, start=1):
                        title = f"{base} (PDF {idx})"
                        if total_parts == 1:
                            info = f"⬇️ Yuklashlar: {download}\n📦 Hajm: {fmt_size(total_size)}"
                        else:
                            info = f"⬇️ Yuklashlar: {download}\n📦 Hajm: {fmt_size(total_size)}\n🧩 Qism: {idx}/{total_parts}\n📦 Qism hajmi: {fmt_size(p[3])}"
                        caption = f"{base}{bot_signature()}\n{blockquote(info)}"
                        results.append(InlineQueryResultCachedDocument(
                            id=f"doc_{p[0]}",
                            title=title,
                            document_file_id=p[1],
                            caption=caption,
                            parse_mode="HTML"
                        ))
                else:
                    for idx, p in enumerate(parts, start=1):
                        title = f"{base} (Audio {idx})"
                        if total_parts == 1:
                            info = f"⏱ Davomiylik: {fmt_duration(total_dur)}\n⬇️ Yuklashlar: {download}\n📦 Hajm: {fmt_size(total_size)}"
                        else:
                            info = f"⏱ Davomiylik: {fmt_duration(total_dur)}\n⬇️ Yuklashlar: {download}\n📦 Hajm: {fmt_size(total_size)}\n🧩 Qism: {idx}/{total_parts}\n⏱ Qism davomiyligi: {fmt_duration(p[4])}"
                        caption = f"{base}{bot_signature()}\n{blockquote(info)}"
                        results.append(InlineQueryResultCachedAudio(
                            id=f"aud_{p[0]}",
                            title=title,
                            audio_file_id=p[1],
                            caption=caption,
                            parse_mode="HTML"
                        ))
        except Exception:
            results = []
    else:
        if len(q) >= 2:
            rows = db.search_books(q, limit=10)
            for r in rows:
                deep = deep_link_for_book(r[0])
                text = f"{type_icon(r[3])}{r[1]} — {r[2]}\n{deep}"
                results.append(InlineQueryResultArticle(
                    id=f"art_{r[0]}",
                    title=f"{r[1]} — {r[2]}",
                    input_message_content=InputTextMessageContent(message_text=text, parse_mode="HTML"),
                    description="Ulashish uchun bosib yuboring"
                ))
    await iq.answer(results=results, is_personal=True, cache_time=1)


@user_router.callback_query(SavedState.paging, F.data == "ss:prev")
async def saved_prev(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    offset = max(int(data.get("saved_offset", 0)) - 10, 0)
    rows = db.list_saved_books(cb.from_user.id, offset=offset, limit=10)
    if not rows:
        await cb.answer()
        return
    items = [(r[1], f"pick:{r[0]}") for r in rows]
    kb = paged_numbers_keyboard(items, has_prev=(offset > 0), has_next=(len(rows) == 10), prev_cb="ss:prev",
                                next_cb="ss:next")
    header = f"Sahifa: {(offset // 10) + 1}"
    body = "\n".join([f"{idx}. {type_icon(r[3])}{r[1]} — {r[2]}  {r[4]} 📥" for idx, r in enumerate(rows, start=1)])
    await cb.message.edit_text(blockquote(header), parse_mode="HTML")
    await cb.message.answer(body, reply_markup=kb)
    await state.update_data(saved_offset=offset)
    await cb.answer()
