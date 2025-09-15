# auth.py
from html import escape
from telegram import Update
from telegram.ext import ContextTypes, ApplicationHandlerStop

from config import ALLOWED_CHAT_IDS, ALLOWED_USER_IDS, UNAUTHORIZED_MESSAGE

def _is_allowed(update: Update) -> bool:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return False
    return (chat.id in ALLOWED_CHAT_IDS) and (user.id in ALLOWED_USER_IDS)

def require_auth(fn):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not _is_allowed(update):
            chat = update.effective_chat
            if chat:
                try:
                    await chat.send_message(UNAUTHORIZED_MESSAGE)
                except Exception:
                    pass
            return
        return await fn(update, context, *args, **kwargs)
    return wrapper

async def _guard_commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Allow /whoami everywhere
    msg = update.effective_message
    text = (msg.text or msg.caption or "").strip()
    first_token = text.split()[0] if text else ""
    cmd = first_token.split("@")[0].lower() if first_token.startswith("/") else ""
    if cmd == "/whoami":
        return
    # Block everything else if unauthorized
    if not _is_allowed(update):
        chat = update.effective_chat
        if chat:
            try:
                await chat.send_message(UNAUTHORIZED_MESSAGE)
            except Exception:
                pass
        raise ApplicationHandlerStop

async def whoami_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    txt = (
        f"chat.id = {chat.id}\n"
        f"chat.type = {chat.type}\n"
        f"user.id = {user.id} ({user.full_name})"
    )
    await update.message.chat.send_message(f"<pre>{escape(txt)}</pre>", parse_mode="HTML")
