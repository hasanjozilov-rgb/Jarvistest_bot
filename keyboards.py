from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

def main_menu():
    kb = [
        [KeyboardButton(text="🔎 Qidiruv"), KeyboardButton(text="🗂 Kategoriyalar")],
        [KeyboardButton(text="⭐ Mashxur kitoblar"), KeyboardButton(text="👤 Mening profilim")],
        [KeyboardButton(text="📊 Statistika"), KeyboardButton(text="❓ Yordam")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def back_menu(back_text: str = "🏠 Asosiy menyu"):
    kb = [[KeyboardButton(text=back_text, ), KeyboardButton(text="❌ Bekor qilish")]]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def choice_keyboard(pairs, add_back=True, add_finish=False, back_code="back", back_text: str = "🏠 Asosiy menyu"):
    rows = []
    for text, data in pairs:
        rows.append([InlineKeyboardButton(text=text, callback_data=data)])
    if add_finish:
        rows.append([InlineKeyboardButton(text="✅ Yakunlash", callback_data="finish")])
    if add_back:
        rows.append([InlineKeyboardButton(text=back_text, callback_data=back_code)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def numbers_keyboard(items, back_code="back", back_text: str = "🏠 Asosiy menyu"):
    rows = []
    row = []
    for i, (label, data) in enumerate(items, start=1):
        row.append(InlineKeyboardButton(text=str(i), callback_data=data))
        if i % 5 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text=back_text, callback_data=back_code)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def group_numbers_keyboard(items):
    rows = []
    row = []
    for i, (label, data) in enumerate(items, start=1):
        row.append(InlineKeyboardButton(text=str(i), callback_data=data))
        if i % 5 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)

def categories_keyboard(cats, back_code: str = "back", back_text: str = "🏠 Asosiy menyu"):
    rows = []
    row = []
    for i, (cid, name) in enumerate(cats, start=1):
        row.append(InlineKeyboardButton(text=name, callback_data=f"cat:{cid}"))
        if i % 2 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text=back_text, callback_data=back_code)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_menu():
    kb = [
        [KeyboardButton(text="📁 Kategoriyalar"), KeyboardButton(text="📚 Kitoblar boshqaruvi")],
        [KeyboardButton(text="📈 Statistika"), KeyboardButton(text="📣 Reklama/Elon")],
        [KeyboardButton(text="❌ Topilmagan kitoblar")],
        [KeyboardButton(text="🚫 Bloklash/Blokdan chiqarish"), KeyboardButton(text="🏠 Asosiy menyu")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def admin_category_menu(cats):
    rows = []
    for cid, name in cats:
        rows.append([InlineKeyboardButton(text=f"❌ {name}", callback_data=f"delcat:{cid}")])
    rows.append([InlineKeyboardButton(text="➕ Kategoriya qo‘shish", callback_data="addcat")])
    rows.append([InlineKeyboardButton(text="🛡 Admin menyu", callback_data="admin_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_book_actions():
    kb = [
        [KeyboardButton(text="➕ Yangi kitob yuklash")],
        [KeyboardButton(text="✏️ Kitobni tahrirlash"), KeyboardButton(text="🗑 Kitobni o‘chirish")],
        [KeyboardButton(text="🛡 Admin menyu")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def paged_numbers_keyboard(items, has_prev: bool, has_next: bool, prev_cb="sp:prev", next_cb="sp:next", add_back=True, back_text: str = "🏠 Asosiy menyu"):
    rows = []
    row = []
    for i, (label, data) in enumerate(items, start=1):
        row.append(InlineKeyboardButton(text=str(i), callback_data=data))
        if i % 5 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    nav = []
    if has_prev:
        nav.append(InlineKeyboardButton(text="◄", callback_data=prev_cb))
    if has_next:
        nav.append(InlineKeyboardButton(text="►", callback_data=next_cb))
    if nav:
        rows.append(nav)
    if add_back:
        rows.append([InlineKeyboardButton(text=back_text, callback_data="back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def book_actions_keyboard(book_id: int, deep_link: str, purchase_link: str | None, saved: bool=False, include_save: bool=True):
    rows = []
    share_btn = InlineKeyboardButton(text="🔗 Ulashish", switch_inline_query=f"book_{book_id}")
    row = [share_btn]
    if include_save:
        row.append(InlineKeyboardButton(text="💾 Saqlash", callback_data=f"save:{book_id}"))
    if purchase_link:
        row.append(InlineKeyboardButton(text="🛒 Sotib olish", url=purchase_link))
    if include_save and saved:
        row.append(InlineKeyboardButton(text="🗑 O‘chirish", callback_data=f"rm:{book_id}"))
    rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)

def popular_menu():
    kb = [
        [KeyboardButton(text="🏆 Top 10 kitoblar")],
        [KeyboardButton(text="🆕 Yaqinda yuklanganlar")],
        [KeyboardButton(text="🎲 Tasodifiy kitoblar")],
        [KeyboardButton(text="🏠 Asosiy menyu")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def profile_menu():
    kb = [
        [KeyboardButton(text="💾 Saqlangan kitoblar")],
        [KeyboardButton(text="🆔 Foydalanuvchi ID")],
        [KeyboardButton(text="🏠 Asosiy menyu")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
