from aiogram import Bot
from config import REQUIRED_CHANNELS, BOT_USERNAME, REQUIRED_CHAT_ID, REQUIRED_JOIN_LINK, EFFECT_CELEBRATION_ID, EFFECT_LOVE_ID, EFFECT_LIKE_ID, EFFECT_DISLIKE_ID, EFFECT_FIRE_ID
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReactionTypeEmoji
from aiogram.exceptions import TelegramBadRequest
import math
from typing import Optional

async def check_membership(bot: Bot, user_id: int):
    if REQUIRED_CHAT_ID:
        try:
            m = await bot.get_chat_member(chat_id=int(REQUIRED_CHAT_ID), user_id=user_id)
            return m.status in ("creator", "administrator", "member")
        except:
            return False
    if not REQUIRED_CHANNELS:
        return True
    for ch in REQUIRED_CHANNELS:
        if ch.startswith("http"):
            continue
        try:
            m = await bot.get_chat_member(chat_id=ch, user_id=user_id)
            if m.status in ("creator", "administrator", "member"):
                continue
            else:
                return False
        except TelegramBadRequest:
            return False
    return True

def join_channels_keyboard():
    kb = []
    if REQUIRED_JOIN_LINK:
        kb.append([InlineKeyboardButton(text="Guruhga qo‘shilish", url=REQUIRED_JOIN_LINK)])
    else:
        for ch in REQUIRED_CHANNELS:
            if ch.lstrip("-").isdigit():
                continue
            url = ch if ch.startswith("http") else f"https://t.me/{ch.replace('@','')}"
            kb.append([InlineKeyboardButton(text="Guruhga qo‘shilish", url=url)])
    kb.append([InlineKeyboardButton(text="✅ Tasdiqlash", callback_data="verify_join")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def bot_signature():
    return f"\n\n@{BOT_USERNAME}" if BOT_USERNAME else ""

def bot_link():
    return f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else ""

def deep_link_for_book(book_id: int):
    if not BOT_USERNAME:
        return ""
    return f"https://t.me/{BOT_USERNAME}?start=book_{book_id}"

def fmt_duration(seconds: int):
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h:
        return f"{h} soat {m} min {s} sek"
    if m:
        return f"{m} min {s} sek"
    return f"{s} sek"

def fmt_size(bytes_: int):
    if bytes_ is None:
        return "—"
    units = ["B", "KB", "MB", "GB"]
    i = 0
    val = float(bytes_)
    while val >= 1024 and i < len(units) - 1:
        val /= 1024.0
        i += 1
    return f"{val:.1f} {units[i]}"

def blockquote(text: str):
    return f"<blockquote>{text}</blockquote>"

def type_icon(type_: str) -> str:
    return "🎧 " if type_ == "audio" else "📖 "

async def add_reaction(bot: Bot, chat_id: int, message_id: int, emoji: str, is_big: bool = False) -> bool:
    try:
        await bot.set_message_reaction(
            chat_id=chat_id,
            message_id=message_id,
            reaction=[ReactionTypeEmoji(emoji=emoji)],
            is_big=is_big
        )
        return True
    except Exception:
        try:
            await bot.set_message_reaction(
                chat_id=chat_id,
                message_id=message_id,
                reaction=[{"type": "emoji", "emoji": emoji}],
                is_big=is_big
            )
            return True
        except Exception:
            return False

async def answer_with_effect(message, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None, effect: Optional[str] = None):
    eff_id = None
    if effect == "celebration":
        eff_id = EFFECT_CELEBRATION_ID or None
    elif effect == "love":
        eff_id = EFFECT_LOVE_ID or None
    elif effect == "like":
        eff_id = EFFECT_LIKE_ID or None
    elif effect == "dislike":
        eff_id = EFFECT_DISLIKE_ID or None
    elif effect == "fire":
        eff_id = EFFECT_FIRE_ID or None
    try:
        if eff_id:
            return await message.answer(text, parse_mode="HTML", reply_markup=reply_markup, message_effect_id=eff_id)
    except Exception:
        pass
    return await message.answer(text, parse_mode="HTML", reply_markup=reply_markup)
