# deribit_handlers.py
from html import escape
from telegram import Update
from telegram.ext import ContextTypes
from providers.deribit import get_index_price
from futures import fs_matrix_table, render_swap_table
from options import render_option_chain_sections, pack_sections_into_messages
from auth import require_auth

@require_auth
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.chat.send_message("hey! i’m online. try /ping, /spot, /bsbtc 30")

@require_auth
async def talos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.chat.send_message("Talos Connected!")

@require_auth
async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.chat.send_message("pong")

@require_auth
async def spot_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = context.args or []
    ccy = (parts[0].upper() if parts else "BTC")
    price = get_index_price(ccy)
    if price is None:
        await update.message.chat.send_message(f"Couldn't fetch {ccy} index right now.")
        return
    await update.message.chat.send_message(f"{ccy} spot ≈ {price:,.2f} USD")

async def _run_swap(update: Update, context: ContextTypes.DEFAULT_TYPE, side: str, coin: str):

    #Parse inputs
    parts = context.args or []
    raw = (parts[0] if parts else "0").strip()
    spread = float(raw)/10000.0 if raw.isdigit() else 0.0

    #Get spot price
    spot = get_index_price(coin)
    if spot is None:
        await update.message.chat.send_message(f"Couldn't fetch {coin} spot.")
        return
    spot_int = int(round(float(spot)))

    #Get future price
    main_html, delta_html = render_swap_table(coin, spot_int, side, spread)
    await update.message.chat.send_message(main_html, parse_mode="HTML")
    if delta_html:
        await update.message.chat.send_message(delta_html, parse_mode="HTML")

@require_auth
async def bsbtc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_swap(update, context, side="b", coin="BTC")

@require_auth
async def ssbtc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_swap(update, context, side="s", coin="BTC")

@require_auth
async def bseth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_swap(update, context, side="b", coin="ETH")

@require_auth
async def sseth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_swap(update, context, side="s", coin="ETH")

@require_auth
async def fsbtc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    html = fs_matrix_table("BTC")
    await update.message.chat.send_message(html, parse_mode="HTML")

@require_auth
async def fseth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    html = fs_matrix_table("ETH")
    await update.message.chat.send_message(html, parse_mode="HTML")

@require_auth
async def option_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text[1:]
    parts = raw.split()
    cmd = parts[0]
    arg = parts[1] if len(parts) > 1 else "0"
    try:
        spread = float(arg) / 100.0
    except ValueError:
        spread = 0.0
    side = "sell" if cmd[0].lower() == "s" else "buy"
    opt_type = "C" if cmd[1].lower() == "c" else "P"
    coin = "ETH" if "eth" in cmd else "BTC"
    sections = render_option_chain_sections(coin, side, opt_type, spread)
    messages = pack_sections_into_messages(sections)
    chat = update.message.chat
    for msg in messages:
        await chat.send_message(text=f"<pre>{escape(msg)}</pre>", parse_mode="HTML")
