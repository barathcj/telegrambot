# config.py
import logging
from dotenv import load_dotenv
import os

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

# Telegram
load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

if TOKEN is None:
    raise ValueError("Missing .env file")

UNAUTHORIZED_MESSAGE = "Unauthorized: I don’t know you."

# Options formatting
STRIKES_PER_EXPIRY = 30
MIN_COIN_PREMIUM = 0.0010

# Messaging limits
MAX_LEN = 4000
EXPIRIES_PER_MESSAGE = 3

# --- Talos (WebSocket) ---
PRIME_TALOS_WS_URL         = os.getenv("PRIME_TALOS_WS_URL", "")          # e.g. wss://tal-59.prod.talostrading.com/ws/v1
PRIME_TALOS_API_KEY        = os.getenv("PRIME_TALOS_API_KEY", "")
PRIME_TALOS_API_SECRET     = os.getenv("PRIME_TALOS_API_SECRET", "")

ASIA_TALOS_WS_URL          = os.getenv("ASIA_TALOS_WS_URL", "")
ASIA_TALOS_API_KEY         = os.getenv("ASIA_TALOS_API_KEY", "")
ASIA_TALOS_API_SECRET      = os.getenv("ASIA_TALOS_API_SECRET", "")

TALOS_CHAT_ID        = int(os.getenv("TALOS_CHAT_ID", "0")) if os.getenv("TALOS_CHAT_ID") else None

# Optional: filter only a specific user at the Talos server (if your tenant supports it)
TALOS_SUBSCRIBE_USER = os.getenv("TALOS_SUBSCRIBE_USER", "") or None

# Optional: exclude some users client-side (case-insensitive), e.g. BitGO-API
TALOS_EXCLUDE_USERS  = {u.strip() for u in os.getenv("TALOS_EXCLUDE_USERS", "BITGO-API").split(",") if u.strip()}

# Optional: also push each per-execution fill
TALOS_SHOW_PER_EXEC_FILL = (os.getenv("TALOS_SHOW_PER_EXEC_FILL", "false").lower() == "true")