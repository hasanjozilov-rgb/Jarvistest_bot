import os
from dotenv import load_dotenv

ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=ENV_PATH)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]
REQUIRED_CHANNELS = [x.strip() for x in os.getenv("REQUIRED_CHANNELS", "").split(",") if x.strip()]
BOT_USERNAME = os.getenv("BOT_USERNAME", "")
REQUIRED_CHAT_ID = os.getenv("REQUIRED_CHAT_ID", "").strip()
REQUIRED_JOIN_LINK = os.getenv("REQUIRED_JOIN_LINK", "").strip()
EFFECT_CELEBRATION_ID = os.getenv("EFFECT_CELEBRATION_ID", "").strip()
EFFECT_LOVE_ID = os.getenv("EFFECT_LOVE_ID", "").strip()
EFFECT_LIKE_ID = os.getenv("EFFECT_LIKE_ID", "").strip()
EFFECT_DISLIKE_ID = os.getenv("EFFECT_DISLIKE_ID", "").strip()
EFFECT_FIRE_ID = os.getenv("EFFECT_FIRE_ID", "").strip()
_fg = os.getenv("FORWARD_GROUP_ID", "").strip()
FORWARD_GROUP_ID = int(_fg) if _fg.lstrip("-").isdigit() else 0
ADMIN_CONTACT_URL = os.getenv("ADMIN_CONTACT_URL", "").strip()
BOOK_HELP_CHANNEL_LINK = os.getenv("BOOK_HELP_CHANNEL_LINK", "").strip()
CARD_NUMBER = os.getenv("CARD_NUMBER", "8600 0000 0000 0000").strip()
CARD_HOLDER = os.getenv("CARD_HOLDER", "").strip()
