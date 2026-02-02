import asyncio
import logging
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from config import ADMIN_IDS
import db
from keyboards import admin_menu, admin_category_menu, admin_book_actions, back_menu, numbers_keyboard, \
    categories_keyboard, choice_keyboard
from utils import bot_signature, blockquote
from keyboards import choice_keyboard

admin_router = Router()


class AddCategoryState(StatesGroup):
    name = State()


class UploadBookState(StatesGroup):
    choose_type = State()
    choose_category = State()
    title = State()
    author = State()
    choose_mode = State()
    receiving_parts = State()
    confirm = State()
    buy_decide = State()
    buy_value = State()


class EditBookState(StatesGroup):
    query = State()
    pick = State()
    field = State()
    value = State()


class DeleteBookState(StatesGroup):
    query = State()
    pick = State()


class BroadcastState(StatesGroup):
    text = State()


class BlockState(StatesGroup):
    user_id = State()
    action = State()


@admin_router.message(Command("admin"))
async def admin_start(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Admin panel", reply_markup=admin_menu())


@admin_router.message(F.text == "📁 Kategoriyalar")
async def show_categories(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    cats = db.list_categories()
    kb = admin_category_menu(cats)
    await message.answer("Kategoriyalar", reply_markup=back_menu("🛡 Admin menyu"))
    await message.answer("Boshqaruv", reply_markup=None)
    await message.answer("Tanlang", reply_markup=kb)


@admin_router.callback_query(F.data == "addcat")
async def addcat(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return
    await cb.message.answer("Kategoriya nomini kiriting", reply_markup=back_menu("🛡 Admin menyu"))
    await state.set_state(AddCategoryState.name)
    await cb.answer()


@admin_router.message(AddCategoryState.name)
async def addcat_name(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    name = message.text.strip()
    if name.lower() in ("🛡 admin menyu", "⬅️ ortga", "❌ bekor qilish"):
        await state.clear()
        await admin_start(message)
        return
    db.add_category(name)
    await message.answer("Qo‘shildi")
    await state.clear()
    await show_categories(message)


@admin_router.callback_query(F.data.startswith("delcat:"))
async def delcat(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return
    cid = int(cb.data.split(":")[1])
    db.delete_category(cid)
    await cb.message.answer("O‘chirildi")
    await cb.answer()


@admin_router.message(F.text == "📚 Kitoblar boshqaruvi")
async def books_menu(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Kitoblar boshqaruvi", reply_markup=admin_book_actions())


@admin_router.message(F.text.in_(["🛡 Admin menyu", "⬅️ Ortga", "back"]))
async def admin_back(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("🛡 Admin menyu", reply_markup=admin_menu())


@admin_router.message(F.text == "➕ Yangi kitob yuklash")
async def upload_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    kb = choice_keyboard([("🎧 Audio", "type:audio"), ("📄 PDF", "type:pdf")])
    await message.answer(blockquote("Turini tanlang"), parse_mode="HTML", reply_markup=kb)
    await state.set_state(UploadBookState.choose_type)


@admin_router.callback_query(UploadBookState.choose_type, F.data.startswith("type:"))
async def upload_type(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return
    t = cb.data.split(":")[1]
    await state.update_data(type=t)
    kb = choice_keyboard([], add_back=True, add_finish=True)
    await cb.message.answer(blockquote("Fayllarni yuboring. Yakunlash uchun tugmadan foydalaning."), parse_mode="HTML",
                            reply_markup=kb)
    await state.set_state(UploadBookState.receiving_parts)
    await cb.answer()


@admin_router.message(UploadBookState.receiving_parts)
async def receive_parts(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    data = await state.get_data()
    type_ = data["type"]
    book_id = data.get("book_id")
    if not book_id:
        book_id = db.create_book(data.get("title", ""), data.get("author", ""), None, type_)
        await state.update_data(book_id=book_id)
    parts = data.get("parts_count", 0) + 1
    if message.document and type_ == "pdf":
        file_id = message.document.file_id
        size = message.document.file_size or 0
        if db.file_exists_in_server(file_id):
            await state.update_data(pending_part={"file_id": file_id, "size": size, "duration": 0, "parts": parts, "type": "pdf"})
            kb = choice_keyboard([("Yuklash", "dup:yes"), ("Bekor qilish", "dup:no")], add_back=True, back_code="admin_back", back_text="🛡 Admin menyu")
            await message.answer(blockquote("ℹ️ Bu fayl avval yuklangan. Qayta yuklashni xohlaysizmi?"), parse_mode="HTML", reply_markup=kb)
            return
        db.add_book_part(book_id, file_id, parts, size=size, duration_seconds=0)
        logging.getLogger("upload").info(f"part={parts} book_id={book_id} type=pdf size={size}")
    elif message.audio and type_ == "audio":
        file_id = message.audio.file_id
        size = message.audio.file_size or 0
        duration = message.audio.duration or 0
        if db.file_exists_in_server(file_id):
            await state.update_data(pending_part={"file_id": file_id, "size": size, "duration": duration, "parts": parts, "type": "audio"})
            kb = choice_keyboard([("Yuklash", "dup:yes"), ("Bekor qilish", "dup:no")], add_back=True, back_code="admin_back", back_text="🛡 Admin menyu")
            await message.answer(blockquote("ℹ️ Bu audio avval yuklangan. Qayta yuklashni xohlaysizmi?"), parse_mode="HTML", reply_markup=kb)
            return
        db.add_book_part(book_id, file_id, parts, size=size, duration_seconds=duration)
        logging.getLogger("upload").info(f"part={parts} book_id={book_id} type=audio size={size} dur={duration}")
    else:
        await message.answer("Noto‘g‘ri turdagi fayl")
        return
    await state.update_data(parts_count=parts)
    await message.answer(blockquote(f"Qism {parts} qabul qilindi"), parse_mode="HTML")


@admin_router.callback_query(UploadBookState.receiving_parts, F.data == "finish")
async def finish_upload_cb(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return
    await cb.message.answer(blockquote("Kitob nomini kiriting"), parse_mode="HTML")
    await state.set_state(UploadBookState.title)
    await cb.answer()


@admin_router.message(UploadBookState.title)
async def upload_title(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    title = message.text.strip()
    await state.update_data(title=title)
    await message.answer(blockquote("Muallifni kiriting"), parse_mode="HTML")
    await state.set_state(UploadBookState.author)


@admin_router.message(UploadBookState.author)
async def upload_author(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    author = message.text.strip()
    await state.update_data(author=author)
    cats = db.list_categories()
    await message.answer(blockquote("Kategoriya tanlang"), parse_mode="HTML", reply_markup=categories_keyboard(cats))
    await state.set_state(UploadBookState.choose_category)


@admin_router.callback_query(UploadBookState.choose_category, F.data.startswith("cat:"))
async def upload_cat(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return
    cid = int(cb.data.split(":")[1])
    await state.update_data(category_id=cid)
    kb = choice_keyboard([("🔗 Havola qo‘yish", "buylink:add"), ("⏭ Tashlab ketish", "buylink:skip")], add_back=True,
                         back_code="admin_back", back_text="🛡 Admin menyu")
    await cb.message.answer(blockquote("Sotib olish havolasini qo‘shasizmi?"), parse_mode="HTML", reply_markup=kb)
    await state.set_state(UploadBookState.buy_decide)
    await cb.answer()


@admin_router.callback_query(UploadBookState.buy_decide, F.data == "buylink:add")
async def upload_buy_add(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return
    await cb.message.answer("Havola URL ni kiriting (https://...)")
    await state.set_state(UploadBookState.buy_value)
    await cb.answer()


@admin_router.callback_query(UploadBookState.buy_decide, F.data == "buylink:skip")
async def upload_buy_skip(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return
    kb = choice_keyboard([("✅ Yuklashni yakunlash", "confirm:yes"), ("❌ Bekor qilish", "confirm:no")], add_back=True,
                         back_code="admin_back", back_text="🛡 Admin menyu")
    await cb.message.answer(blockquote("Yuklashni tasdiqlaysizmi?"), parse_mode="HTML", reply_markup=kb)
    await state.set_state(UploadBookState.confirm)
    await cb.answer()


@admin_router.message(UploadBookState.buy_value)
async def upload_buy_value(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    link = message.text.strip()
    data = await state.get_data()
    book_id = data.get("book_id")
    if book_id and link:
        try:
            db.set_purchase_link(book_id, link)
            await message.answer("Havola saqlandi")
        except Exception:
            await message.answer("Havolani saqlashda xatolik")
    kb = choice_keyboard([("✅ Yuklashni yakunlash", "confirm:yes"), ("❌ Bekor qilish", "confirm:no")], add_back=True,
                         back_code="admin_back", back_text="🛡 Admin menyu")
    await message.answer(blockquote("Yuklashni tasdiqlaysizmi?"), parse_mode="HTML", reply_markup=kb)
    await state.set_state(UploadBookState.confirm)


@admin_router.callback_query(UploadBookState.confirm, F.data.startswith("confirm:"))
async def upload_confirm(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return
    data = await state.get_data()
    book_id = data["book_id"]
    if cb.data.endswith("no"):
        db.delete_book(book_id)
        await cb.message.answer("Bekor qilindi")
        await state.clear()
        await cb.answer()
        return
    db.update_book_meta(book_id, title=data.get("title", ""), author=data.get("author", ""),
                        category_id=data.get("category_id"))
    logging.getLogger("upload").info(
        f"finalize book_id={book_id} title={data.get('title', '')} author={data.get('author', '')} category_id={data.get('category_id')}")
    await cb.message.answer(blockquote("Yuklash yakunlandi"), parse_mode="HTML")
    await state.clear()
    await cb.answer()
    await books_menu(cb.message)

@admin_router.callback_query(UploadBookState.receiving_parts, F.data.startswith("dup:"))
async def upload_duplicate_decide(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return
    data = await state.get_data()
    book_id = data.get("book_id")
    pend = data.get("pending_part")
    if not pend or not book_id:
        await cb.answer()
        return
    if cb.data.endswith("no"):
        await state.update_data(pending_part=None)
        await cb.message.answer("Qayta yuklash bekor qilindi")
        await cb.answer()
        return
    parts = pend["parts"]
    if pend["type"] == "pdf":
        db.add_book_part(book_id, pend["file_id"], parts, size=pend["size"], duration_seconds=0)
        logging.getLogger("upload").info(f"dup-confirm part={parts} book_id={book_id} type=pdf size={pend['size']}")
    else:
        db.add_book_part(book_id, pend["file_id"], parts, size=pend["size"], duration_seconds=pend["duration"])
        logging.getLogger("upload").info(f"dup-confirm part={parts} book_id={book_id} type=audio size={pend['size']} dur={pend['duration']}")
    await state.update_data(parts_count=parts, pending_part=None)
    await cb.message.answer(blockquote(f"Qism {parts} qabul qilindi"), parse_mode="HTML")
    await cb.answer()


@admin_router.callback_query(F.data == "admin_back")
async def go_back(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    if cb.message:
        await cb.message.delete()
        await cb.message.answer("🛡 Admin menyu", reply_markup=admin_menu())
    else:
        await cb.message.answer("🛡 Admin menyu")
    await cb.answer()


@admin_router.message(F.text == "❌ Topilmagan kitoblar")
async def show_missing(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    rows = db.list_missing_queries_agg(limit=100)
    if not rows:
        await message.answer("Topilmagan so‘rovlar yo‘q")
        return
    out = "\n".join([f"{idx}. {r[0]} — {r[1]} ta" for idx, r in enumerate(rows, start=1)])
    kb = choice_keyboard([("Ko‘rildi", "clear_missing")], add_back=True, back_code="admin_back",
                         back_text="🛡 Admin menyu")
    await message.answer(out, reply_markup=kb)


@admin_router.callback_query(F.data == "clear_missing")
async def clear_missing(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return
    db.clear_missing_queries()
    await cb.message.answer("Ro‘yxat tozalandi")
    await cb.answer()


@admin_router.message(F.text == "✏️ Kitobni tahrirlash")
async def edit_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Kitob nomi yoki muallif bo‘yicha qidiruv so‘zi kiriting",
                         reply_markup=back_menu("🛡 Admin menyu"))
    await state.set_state(EditBookState.query)


@admin_router.message(EditBookState.query)
async def edit_query(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    q = message.text.strip()
    if q in ("🛡 Admin menyu", "⬅️ Ortga", "❌ Bekor qilish"):
        await state.clear()
        await books_menu(message)
        return
    rows = db.search_books(q, limit=10)
    if not rows:
        await message.answer("Topilmadi")
        return
    items = [(f"{i[1]}", f"pickedit:{i[0]}") for i in rows]
    kb = numbers_keyboard(items, back_code="admin_back", back_text="🛡 Admin menyu")
    txt = "\n".join([f"{idx}. {r[1]} — {r[2]} ({r[3]})" for idx, r in enumerate(rows, start=1)])
    await message.answer(txt, reply_markup=kb)
    await state.set_state(EditBookState.pick)


@admin_router.callback_query(EditBookState.pick, F.data.startswith("pickedit:"))
async def pick_edit(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return
    bid = int(cb.data.split(":")[1])
    await state.update_data(book_id=bid)
    kb = choice_keyboard([("Nom", "ef:title"), ("Muallif", "ef:author"), ("Kategoriya", "ef:category"),
                          ("Sotib olish havolasi", "ef:buy")], add_back=True, back_code="admin_back",
                         back_text="🛡 Admin menyu")
    await cb.message.answer("Tahrir maydonini tanlang", reply_markup=kb)
    await state.set_state(EditBookState.field)
    await cb.answer()


@admin_router.callback_query(EditBookState.field, F.data.startswith("ef:"))
async def edit_field_cb(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return
    f = cb.data.split(":")[1]
    await state.update_data(field=f)
    if f == "buy":
        await cb.message.answer("Yangi havola URL kiriting (o‘chirish uchun '-')")
    elif f == "category":
        cats = db.list_categories()
        await cb.message.answer(blockquote("Kategoriya tanlang"), parse_mode="HTML", reply_markup=categories_keyboard(cats))
    else:
        await cb.message.answer("Yangi qiymatni kiriting")
    await state.set_state(EditBookState.value)
    await cb.answer()


@admin_router.message(EditBookState.field)
async def edit_field(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    f = message.text.strip().lower()
    if f not in ("title", "author", "category"):
        await message.answer("title/author/category dan birini kiriting")
        return
    await state.update_data(field=f)
    if f == "category":
        cats = db.list_categories()
        await message.answer(blockquote("Kategoriya tanlang"), parse_mode="HTML", reply_markup=categories_keyboard(cats))
    else:
        await message.answer("Yangi qiymatni kiriting")
    await state.set_state(EditBookState.value)


@admin_router.message(EditBookState.value)
async def edit_value(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    v = message.text.strip()
    data = await state.get_data()
    bid = data["book_id"]
    f = data["field"]
    if f == "title":
        db.update_book_meta(bid, title=v)
    elif f == "author":
        db.update_book_meta(bid, author=v)
    elif f == "category":
        try:
            cid = int(v)
            db.update_book_meta(bid, category_id=cid)
        except:
            await message.answer("Kategoriya ID noto‘g‘ri")
            return
    elif f == "buy":
        if v.strip() == "-" or v.strip().lower() in ("o'chirish", "ochirish", "remove", "delete"):
            db.clear_purchase_link(bid)
            await message.answer("Havola o‘chirildi")
        else:
            db.set_purchase_link(bid, v)
            await message.answer("Havola yangilandi")
    await message.answer("Yangilandi")
    await state.clear()
    await books_menu(message)

@admin_router.callback_query(EditBookState.value, F.data.startswith("cat:"))
async def edit_cat_value_cb(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return
    cid = int(cb.data.split(":")[1])
    data = await state.get_data()
    bid = data["book_id"]
    db.update_book_meta(bid, category_id=cid)
    await cb.message.answer("Kategoriya yangilandi")
    await state.clear()
    await cb.answer()
    await books_menu(cb.message)


@admin_router.message(F.text == "🗑 Kitobni o‘chirish")
async def delete_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Qidiruv so‘zi kiriting", reply_markup=back_menu())
    await state.set_state(DeleteBookState.query)


@admin_router.message(DeleteBookState.query)
async def delete_query(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    q = message.text.strip()
    if q in ("🛡 Admin menyu", "⬅️ Ortga", "❌ Bekor qilish"):
        await state.clear()
        await books_menu(message)
        return
    rows = db.search_books(q, limit=10)
    if not rows:
        await message.answer("Topilmadi")
        return
    items = [(f"{i[1]}", f"pickdel:{i[0]}") for i in rows]
    kb = numbers_keyboard(items, back_code="admin_back", back_text="🛡 Admin menyu")
    txt = "\n".join([f"{idx}. {r[1]} — {r[2]} ({r[3]})" for idx, r in enumerate(rows, start=1)])
    await message.answer(txt, reply_markup=kb)
    await state.set_state(DeleteBookState.pick)


@admin_router.callback_query(DeleteBookState.pick, F.data.startswith("pickdel:"))
async def pick_delete(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return
    bid = int(cb.data.split(":")[1])
    b = db.get_book(bid)
    if not b:
        await cb.answer("Bu kitob allaqachon o'chirilgan!", show_alert=True)
        return

    db.delete_book(bid)
    await cb.message.answer(f"🗑 <b>Kitob o‘chirildi:</b>\n🆔 ID: {b[0]}\n📘 Nomi: {b[1]}\n✍️ Muallif: {b[2]}",
                            parse_mode="HTML")
    await cb.answer("O‘chirildi")
    # State tozalanmaydi, shunda admin ro'yxatdan boshqa kitoblarni ham o'chirishi mumkin


@admin_router.message(F.text == "📣 Reklama/Elon")
async def broadcast_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Uzatiladigan xabar matnini kiriting", reply_markup=back_menu("🛡 Admin menyu"))
    await state.set_state(BroadcastState.text)


@admin_router.message(BroadcastState.text)
async def broadcast_send(message: Message, state: FSMContext, bot):
    if message.from_user.id not in ADMIN_IDS:
        return
    conn = db.connect()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users")
    users = [row[0] for row in cur.fetchall()]
    conn.close()
    sent = 0
    for uid in users:
        try:
            await bot.copy_message(chat_id=uid, from_chat_id=message.chat.id, message_id=message.message_id)
            await asyncio.sleep(0.02)
            sent += 1
        except:
            pass
    await message.answer(f"Yuborildi: {sent} ta")
    await state.clear()


@admin_router.message(F.text == "📈 Statistika")
async def admin_stats(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    c = db.get_user_count()
    await message.answer(f"Foydalanuvchilar soni: {c}")


@admin_router.message(F.text == "🚫 Bloklash/Blokdan chiqarish")
async def block_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Foydalanuvchi ID kiriting", reply_markup=back_menu("🛡 Admin menyu"))
    await state.set_state(BlockState.user_id)


@admin_router.message(BlockState.user_id)
async def block_user(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    t = message.text.strip()
    if t.lower() in ("🛡 admin menyu", "⬅️ ortga", "❌ bekor qilish", "back", "/start", "start"):
        await state.clear()
        await admin_start(message)
        return
    try:
        uid = int(t)
    except:
        await message.answer("ID noto‘g‘ri")
        return
    await state.update_data(uid=uid)
    await message.answer("Amal kiriting: block/unblock")
    await state.set_state(BlockState.action)


@admin_router.message(BlockState.action)
async def block_action(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    act = message.text.strip().lower()
    if act in ("🏠 asosiy menyu", "⬅️ ortga", "❌ bekor qilish", "back", "/start", "start"):
        await state.clear()
        await admin_start(message)
        return
    data = await state.get_data()
    uid = data["uid"]
    if act == "block":
        db.set_block(uid, True)
        await message.answer("Bloklandi")
    elif act == "unblock":
        db.set_block(uid, False)
        await message.answer("Blokdan chiqarildi")
    else:
        await message.answer("block yoki unblock kiriting")
        return
    await state.clear()
