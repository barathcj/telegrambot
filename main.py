# main.py
import logging
from config import TOKEN


from config import (
    PRIME_TALOS_WS_URL, PRIME_TALOS_API_KEY, PRIME_TALOS_API_SECRET, 
    ASIA_TALOS_WS_URL, ASIA_TALOS_API_KEY, ASIA_TALOS_API_SECRET, 
    TALOS_CHAT_ID, TALOS_SUBSCRIBE_USER, TALOS_EXCLUDE_USERS, 
    TALOS_SHOW_PER_EXEC_FILL
)
from talos_watcher_fn import start_talos_watcher

from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
from auth import _guard_commands, whoami_cmd
from deribit_handlers import (
    start_cmd, ping_cmd, spot_cmd,
    bsbtc_cmd, ssbtc_cmd, bseth_cmd, sseth_cmd,
    fsbtc_cmd, fseth_cmd, option_cmd, talos
)
from binance_handlers import (
    bspot_cmd, bfund_cmd, bfundhist_cmd
)

async def on_error(update, context):
    try:
        logging.error("Handler error: %s", context.error)
    except Exception:
        pass


def main():
    if not TOKEN:
        raise RuntimeError("Set your Telegram TOKEN in config.py")

    app = ApplicationBuilder().token(TOKEN).build()

    # BitGo Prime Delaware (own creds)
    start_talos_watcher(
        name="prime",
        tg_token=TOKEN, chat_id=TALOS_CHAT_ID,
        ws_url=PRIME_TALOS_WS_URL, api_key=PRIME_TALOS_API_KEY, api_secret=PRIME_TALOS_API_SECRET,
        exclude_users={"BITGO-API"},
        account_label="BitGo Prime Delaware",
    )

    # BitGo Asia (own creds; cover both SG/HK under one watcher label)
    start_talos_watcher(
        name="asia",
        tg_token=TOKEN, chat_id=TALOS_CHAT_ID,
        ws_url=ASIA_TALOS_WS_URL, api_key=ASIA_TALOS_API_KEY, api_secret=ASIA_TALOS_API_SECRET,
        exclude_users={"BITGO-API"},
        account_label="BitGo Asia",
        subaccount_filter={"Bitgo SG", "Bitgo HK"},  # only pass these two
    )

    # Guard first
    app.add_handler(MessageHandler(filters.COMMAND, _guard_commands), group=0)

    # Deribit handlers
    app.add_handler(CommandHandler("start", start_cmd), group=1)
    app.add_handler(CommandHandler("talos", talos), group=1)
    app.add_handler(CommandHandler("help", start_cmd), group=1)
    app.add_handler(CommandHandler("ping", ping_cmd), group=1)
    app.add_handler(CommandHandler("spot", spot_cmd), group=1)
    app.add_handler(CommandHandler("bsbtc", bsbtc_cmd), group=1)
    app.add_handler(CommandHandler("ssbtc", ssbtc_cmd), group=1)
    app.add_handler(CommandHandler("bseth", bseth_cmd), group=1)
    app.add_handler(CommandHandler("sseth", sseth_cmd), group=1)
    app.add_handler(CommandHandler("fsbtc", fsbtc_cmd), group=1)
    app.add_handler(CommandHandler("fseth", fseth_cmd), group=1)
    app.add_handler(CommandHandler(
        ["sceth","bceth","speth","bpeth","scbtc","bcbtc","spbtc","bpbtc"], option_cmd
    ), group=1)

    # Binance handlers
    app.add_handler(CommandHandler("bspot", bspot_cmd), group=1)
    app.add_handler(CommandHandler("bfund", bfund_cmd), group=1)
    app.add_handler(CommandHandler("bfundhist", bfundhist_cmd), group=1)

    # whoami (exempted in guard)
    app.add_handler(CommandHandler("whoami", whoami_cmd), group=1)

    app.add_error_handler(on_error)
    logging.info("bot starting…")
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
