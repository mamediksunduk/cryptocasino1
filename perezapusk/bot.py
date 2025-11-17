import os
import logging
import asyncio
import re
import random
import time
import uuid
import math
from decimal import Decimal
from typing import Optional, Dict, Tuple
from sqlite3 import IntegrityError

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter, CommandObject, CommandStart, or_f
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv

import aiosqlite
import aiogram.exceptions
from database import (
    Database,
    InsufficientFundsError,
    CheckAlreadyActivatedError,
    CheckAlreadyCashedError,
    CheckNotFoundError,
    CheckPermissionError
)
from games import (
    CubeGame, GameResult, TwoDiceGame, RockPaperScissorsGame,
    BasketballGame, DartsGame, SlotsGame, BowlingGame, CustomEmojiGame, FootballGame
)
from cryptopay import CryptoPayAPI
from middlewares.subscription import SubscriptionMiddleware
from contests import (
    init_contests,
    check_contests_schedule,
    router as contests_router,
    process_bet_for_contests,
    format_contest_message,
    get_contest_keyboard
)
import admin

swap_assets = ["USDT"]

class GameStates(StatesGroup):
    CHOOSE_GAME = State()
    CHOOSE_BET_TYPE = State()
    CHOOSE_BALANCE = State()
    ENTER_AMOUNT = State()

class WalletStates(StatesGroup):
    DEPOSIT_AMOUNT = State()
    WITHDRAW_AMOUNT = State()

class CheckStates(StatesGroup):
    CREATE_AMOUNT = State()
    CREATE_TARGET_USER = State()
    CREATE_ACTIVATIONS = State()
    ACTIVATE_PASSWORD = State()
    SET_PASSWORD = State()
    SET_TURNOVER = State()
    SET_WAGERING = State()
    SET_COMMENT = State()
    SET_TARGET_USER = State()  # Для привязки пользователя в меню управления

load_dotenv()
logging.basicConfig(level=logging.INFO)

bot = Bot(token=os.getenv('BOT_TOKEN'), default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
db = Database()
crypto_pay = CryptoPayAPI(os.getenv('CRYPTO_PAY_TOKEN'), testnet=False)

bot_username = None
async def get_bot_username():
    global bot_username
    if bot_username:
        return bot_username
    bot_me = await bot.get_me()
    bot_username = bot_me.username
    return bot_username

CASINO_NAME = os.getenv('CASINO_NAME', 'GlacialCasino')
CASINO_EMOJI = os.getenv('CASINO_EMOJI', '🎲')

INVOICE_URL = os.getenv('INVOICE_URL', "https://t.me/vemorr")
LOGS_ID = int(os.getenv('LOGS_ID', '-1002361786257'))
BETS_ID = int(os.getenv('BETS_ID', '-1002403460000'))

SUPPORT_LINK = os.getenv('SUPPORT_LINK', "https://t.me/vemorr")
ADAPTER_LINK = os.getenv('ADAPTER_LINK', "https://t.me/vemorr")
RULES_LINK = os.getenv('RULES_LINK', "https://t.me/vemorr")
CHAT_LINK = os.getenv('CHAT_LINK', "https://t.me/+lOmGQ05okK5hMzli")
TUTORIAL_LINK = os.getenv('TUTORIAL_LINK', "https://t.me/vemorr")
NEWS_LINK = os.getenv('NEWS_LINK', "https://t.me/+ObCMDgP2L4BhMDBi")
REFERRALS_PER_PAGE = 20
INVALID_AMOUNT_FORMAT_MSG = "❌ Неверный формат суммы\nПример: 5.50"

def sanitize_nickname(text: str) -> str:
    if not text:
        return ''
    text = text.strip()
    if text.startswith('@'):
        return text
    if ' @' in text:
        return text.split(' @')[0].strip()
    if '@' in text:
        parts = text.split('@')
        if len(parts) > 1 and parts[1].strip():
            return parts[0].strip()
    if '#' in text:
        return text.split('#')[0].strip()
    sanitized = re.sub(r'[\[\]{}<>|\\/"\'`~$%^&*()=+]', '', text)
    return sanitized.strip()

async def links():
    return (
        f'<blockquote>'
        f'<b><a href="{TUTORIAL_LINK}">Как сделать ставку</a></b> • '
        f'<b><a href="{CHAT_LINK}">Наш чат</a></b> • '
        f'<b><a href="{RULES_LINK}">Пользовательское соглашение</a></b> • '
        f'<b><a href="{SUPPORT_LINK}">Поддержка</a></b> • '
        f'<b><a href="https://t.me/CasinoDepovBot">Бот</a></b>'
        f'</blockquote>'
    )

MAIN_MENU_BUTTONS = [
    "🎲 Сделать ставку",
    "💰 Кошелек",
    "👥 Реферальная система",
    "🧾 Чеки",
    "👤 Профиль"
]

def create_main_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎲 Сделать ставку")],
            [KeyboardButton(text="💰 Кошелек"), KeyboardButton(text="👤 Профиль")],
            [KeyboardButton(text="🧾 Чеки"), KeyboardButton(text="👥 Реферальная система")]
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )

def create_games_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎲", callback_data="game_cube"),
            InlineKeyboardButton(text="🎲🎲", callback_data="game_two_dice"),
            InlineKeyboardButton(text="🏀", callback_data="game_basketball"),
            InlineKeyboardButton(text="🎯", callback_data="game_darts")
        ],
        [
            InlineKeyboardButton(text="🎳", callback_data="game_bowling"),
            InlineKeyboardButton(text="🎰", callback_data="game_slots"),
            InlineKeyboardButton(text="⚽", callback_data="game_football"),
            InlineKeyboardButton(text="👊✌️🖐", callback_data="game_rock_paper_scissors")
        ],
        [InlineKeyboardButton(text="✨ Авторские игры", callback_data="game_custom")]
    ])

async def send_check_created_message(message: types.Message, text: str, check_id: str):
    bot_username = await get_bot_username()
    check_link = f"https://t.me/{bot_username}?start=check_{check_id}"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="↪️ Поделиться чеком", switch_inline_query=check_id),
                InlineKeyboardButton(text="📋 Копировать ссылку", copy_text={"text": check_link})
            ],
            [
                InlineKeyboardButton(text="⚙️ Управление чеком", callback_data=f"manage_check_{check_id}")
            ]
        ]
    )
    await message.answer(
        text,
        reply_markup=keyboard,
        parse_mode="HTML"
    )

async def send_check_management_message(chat_id: int, text: str, check_id: str):
    bot_username = await get_bot_username()
    check_link = f"https://t.me/{bot_username}?start=check_{check_id}"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="↪️ Поделиться чеком", switch_inline_query=check_id),
                InlineKeyboardButton(text="📋 Копировать ссылку", copy_text={"text": check_link})
            ],
            [
                InlineKeyboardButton(text="⚙️ Управление чеком", callback_data=f"manage_check_{check_id}")
            ]
        ]
    )
    await bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@dp.message(Command("profile"), StateFilter("*"))
@dp.message(F.text == "👤 Профиль", StateFilter("*"))
async def handle_show_profile(message: types.Message, state: FSMContext):
    await show_profile_logic(message.chat.id, message.from_user.id, state)

async def show_profile_logic(chat_id: int, user_id: int, state: FSMContext):
    await state.clear()
    user = await db.get_user(user_id)
    if not user:
        user_info = await bot.get_chat(user_id)
        username = user_info.username or user_info.full_name
        await db.create_user(user_id, username, user_info.full_name)
        user = await db.get_user(user_id)
    stats = await db.get_user_stats(user_id)
    profile_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏆 Топ", callback_data="leaderboard_turnover_all")],
        [InlineKeyboardButton(text="🎁 Бонус", callback_data="bonus_program")]
    ])
    await bot.send_video(
        chat_id=chat_id,
        video=types.FSInputFile("profile.mp4"),
        caption=(
            f"👤 <b>Профиль</b>\n\n"
            f"💰 <b>Баланс:</b> <code>{user['balance']:.2f}$</code>\n\n"
            f"🎮 <b>Игр сыграно:</b> <code>{stats['total_games']}</code>\n"
            f"🏆 <b>Побед:</b> <code>{stats['wins']}</code> | 😔 <b>Поражений:</b> <code>{stats['losses']}</code>\n"
            f"📈 <b>Процент побед:</b> <code>{stats['win_rate']:.1f}%</code>\n"
            f"💸 <b>Оборот:</b> <code>{stats['turnover']:.2f}$</code>"
        ),
        reply_markup=profile_keyboard,
        parse_mode="HTML"
    )

async def activate_check_logic(message: types.Message, check_id: str, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.full_name
    full_name = message.from_user.full_name or username

    user = await db.get_user(user_id)
    if user:
        await db.update_user(user_id, {"username": username, "full_name": full_name})
    else:
        await db.create_user(user_id, username, full_name)

    check = await db.get_check(check_id)
    if not check:
        await message.answer("❌ Этот чек недействителен.")
        await state.clear()
        return

    if check.get("status") == "cashed":
        await message.answer("❌ Этот чек уже был использован.")
        await state.clear()
        return

    if check.get("is_multi") and await db.has_user_activated_check(check_id, user_id):
        await message.answer("❌ Вы уже активировали этот чек.")
        await state.clear()
        return

    notification_text = ""
    activator_mention = f'<b><a href="tg://user?id={user_id}">{full_name}</a></b>'
    comment = check.get("comment")

    if check.get("target_user_id") and check["target_user_id"] != user_id and check.get("creator_id") != user_id:
        await message.answer("❌ Этот чек предназначен для другого пользователя.")
        await state.clear()
        return

    if check.get("is_multi"):
        activations_total = int(check.get("activations_total", 0))
        activations_count = await db.get_check_activations_count(check_id)
        if activations_total and activations_count >= activations_total:
            await message.answer("❌ Этот чек уже был полностью использован.")
            await state.clear()
            return

    if check.get("premium_only") and not getattr(message.from_user, "is_premium", False):
        await message.answer("⭐ Этот чек доступен только для пользователей Telegram Premium.")
        await state.clear()
        return

    if check.get("required_turnover", 0) > 0:
        stats = await db.get_user_stats(user_id)
        if stats.get("turnover", 0) < check["required_turnover"]:
            await message.answer(
                f"💸 Для активации этого чека ваш игровой оборот должен быть не менее {check['required_turnover']:.2f}$.\n"
                f"Ваш текущий оборот: {stats.get('turnover', 0):.2f}$."
            )
            await state.clear()
            return

    try:
        activation_result = await db.activate_check_atomic(check_id, user_id)
    except CheckAlreadyActivatedError:
        await message.answer("❌ Вы уже активировали этот чек.")
        await state.clear()
        return
    except CheckAlreadyCashedError:
        await message.answer("❌ Этот чек уже был использован.")
        await state.clear()
        return
    except CheckNotFoundError:
        await message.answer("❌ Этот чек недействителен.")
        await state.clear()
        return
    except IntegrityError:
        await message.answer("❌ Не удалось активировать чек. Попробуйте позже.")
        await state.clear()
        return

    updated_check = activation_result["check"]
    amount_activated = activation_result["amount"]
    remaining_activations = activation_result["remaining_activations"]
    credited_to_bonus = activation_result.get("credited_to_bonus", False)
    wager_requirement = activation_result.get("wager_requirement", Decimal('0'))
    comment = updated_check.get("comment")
    is_multi = bool(updated_check.get("is_multi"))
    creator_id = updated_check.get("creator_id")

    if is_multi:
        text = f"<b>👨‍👩‍👧‍👦 Вы активировали мульти-чек на {amount_activated:.2f}$</b>"
        if comment:
            text += f"\n💬 {comment}"
        if creator_id and creator_id != user_id:
            notification_text = (
                f"{activator_mention} активировал(а) ваш чек на <b>{amount_activated:.2f}$</b>.\n"
                f"Осталось активаций: {remaining_activations}/{updated_check['activations_total']}"
            )
    else:
        if not updated_check.get("target_user_id"):
            text = f"<b>🧾 Вы активировали чек на {amount_activated:.2f}$</b>"
        else:
            text = f"<b>🔒 Вы активировали приватный чек на {amount_activated:.2f}$</b>"
        if comment:
            text += f"\n💬 {comment}"
        if creator_id and creator_id != user_id:
            notification_text = (
                f"{activator_mention} активировал(а) ваш чек на <b>{amount_activated:.2f}$</b>."
            )

    if credited_to_bonus and wager_requirement > 0:
        text += (
            "\n\n"
            "🔒 <i>Средства зачислены на бонусный баланс.</i>\n"
            f"🔥 <i>Необходимо отыграть <code>{wager_requirement:.2f}$</code>.</i>"
        )
        if creator_id and creator_id != user_id and notification_text:
            notification_text += f"\n\n🔥 Требуется отыгрыш: <code>{wager_requirement:.2f}$</code>."

    await message.answer(text, parse_mode="HTML")

    if creator_id and creator_id != user_id and notification_text:
        try:
            await bot.send_message(
                creator_id,
                notification_text,
                parse_mode="HTML"
            )
        except Exception as e:
            logging.warning(f"Failed to notify check creator: {e}")
    await state.clear()

@dp.message(Command("wallet"), StateFilter("*"))
@dp.message(F.text == "💰 Кошелек", StateFilter("*"))
async def show_wallet(message: types.Message, state: FSMContext):
    await state.clear()
    user = await db.get_user(message.from_user.id)
    if not user:
        user_info = message.from_user
        await db.create_user(user_info.id, user_info.username or user_info.full_name, user_info.full_name)
        user = await db.get_user(user_info.id)
    balance = max(user.get("balance", Decimal("0")), Decimal('0'))
    bonus_balance = max(user.get("bonus_balance", Decimal("0")), Decimal('0'))
    if balance <= 0:
        await db.clean_empty_wagerings(message.from_user.id)
    wagering_info = await db.get_user_wagering_info(message.from_user.id)
    available = max(balance - bonus_balance, Decimal('0'))
    extra_parts = [
        f"🔒 <b>Бонусный баланс:</b> <code>{bonus_balance:.2f}$</code>",
        f"💸 <b>Доступно к выводу:</b> <code>{available:.2f}$</code>"
    ]
    if wagering_info and wagering_info['left'] > 0:
        left = wagering_info['left']
        total = wagering_info['total'] if wagering_info['total'] > 0 else left
        extra_parts.append(f"🔥 <b>Отыгрыш:</b> осталось <code>{left:.2f}$</code> из <code>{total:.2f}$</code>")
    extra = "\n" + "\n".join(extra_parts) if extra_parts else ""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Пополнить", callback_data="deposit")],
        [InlineKeyboardButton(text="💸 Вывести", callback_data="withdraw")]
    ])
    await message.answer_video(
        video=types.FSInputFile("wallet.mp4"),
        caption=(
            f"💰 <b>Кошелек</b>\n\n"
            f"💵 <b>Ваш баланс:</b> <code>{balance:.2f}$</code>{extra}\n\n"
            f"Выберите действие:"
        ),
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@dp.message(CommandStart(deep_link=True))
async def ref_start(message: types.Message, command: CommandObject, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.full_name
    full_name = message.from_user.full_name
    args = command.args

    user = await db.get_user(user_id)
    if user:
        await db.update_user(user_id, {"username": username, "full_name": full_name})
    else:
        await db.create_user(user_id, username, full_name)
        user = await db.get_user(user_id)  # получить свежие данные

    if not args:
        return

    if args.startswith("userstats_"):
        arg_split = args.split("_")
        if len(arg_split) > 1 and arg_split[1].isdigit():
            target_user_id = int(arg_split[1])
            await show_user_stats(message.chat.id, target_user_id)
        else:
            await message.answer("❌ Пользователь не найден.")
        return

    if args == "games":
        await choose_game(message, state)
        return

    if args == "wallet":
        await show_wallet(message, state)
        return

    if args.isdigit():
        referrer_id = int(args)
        if user_id == referrer_id:
            await cmd_start(message, state)
            return
        ref_user = await db.get_user(referrer_id)
        if not ref_user:
            await cmd_start(message, state)
            return
        if not user.get("referrer_id"):
            await db.update_user(user_id, {"referrer_id": referrer_id})
            await db.update_ref_count(referrer_id, 1)
            await bot.send_message(
                chat_id=ref_user['user_id'],
                text=f"👤 У вас новый реферал: <code>{username}</code>",
                parse_mode="HTML"
            )
        await cmd_start(message, state)
        return

    if args.startswith("check_"):
        check_id = args.split("_", 1)[1]
        check = await db.get_check(check_id)
        if not check:
            await message.answer("❌ Этот чек недействителен.")
            return

        if not check.get("is_multi") and check.get("status") == "cashed":
            await message.answer("❌ Этот чек уже был использован.")
            return

        if check.get("target_user_id") and check["target_user_id"] != user_id and check.get("creator_id") != user_id:
            await message.answer("❌ Этот чек предназначен для другого пользователя.")
            return

        if check.get("password"):
            await state.update_data(check_id=check_id, args=args)
            await state.set_state(CheckStates.ACTIVATE_PASSWORD)
            await message.answer("🔑 Этот чек защищен паролем. Введите пароль для активации:")
            return

        if check.get("premium_only") and not getattr(message.from_user, "is_premium", False):
            await message.answer("⭐ Этот чек доступен только для пользователей Telegram Premium.")
            return

        if check.get("required_turnover", 0) > 0:
            stats = await db.get_user_stats(user_id)
            if stats.get("turnover", 0) < check["required_turnover"]:
                await message.answer(
                    f"💸 Для активации этого чека ваш игровой оборот должен быть не менее {check['required_turnover']:.2f}$.\n"
                    f"Ваш текущий оборот: {stats.get('turnover', 0):.2f}$."
                )
                return

        if check.get("is_multi"):
            if await db.has_user_activated_check(check_id, user_id):
                await message.answer("❌ Вы уже активировали этот чек.")
                return

            activations_count = await db.get_check_activations_count(check_id)
            if activations_count >= check.get("activations_total", 0):
                await message.answer("❌ Этот чек уже был полностью использован.")
                return

        await activate_check_logic(message, check_id, state)
        return

    if args == "ref":
        await show_referral(message, state)
        return

@dp.message(CheckStates.ACTIVATE_PASSWORD)
async def ask_check_password(message: types.Message, state: FSMContext):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_check_password")]
        ]
    )
    await message.answer(
        "🔑 Этот чек защищен паролем. Введите пароль для активации:",
        reply_markup=keyboard
    )

@dp.message(CheckStates.ACTIVATE_PASSWORD, F.text)
async def process_check_activation_password(message: types.Message, state: FSMContext):
    if not message.text or message.text.startswith("/") or message.text in MAIN_MENU_BUTTONS:
        return
    password = message.text
    data = await state.get_data()
    check_id = data.get("check_id")
    check = await db.get_check(check_id)
    if not check or check.get("password") != password:
        await message.answer("❌ Неверный пароль. Попробуйте еще раз.")
        return

    user_id = message.from_user.id
    if check.get("premium_only") and not getattr(message.from_user, "is_premium", False):
        await message.answer("⭐ Этот чек доступен только для пользователей Telegram Premium.")
        await state.clear()
        return

    if check.get("required_turnover", 0) > 0:
        stats = await db.get_user_stats(user_id)
        if stats.get("turnover", 0) < check["required_turnover"]:
            await message.answer(
                f"💸 Для активации этого чека ваш игровой оборот должен быть не менее {check['required_turnover']:.2f}$.\n"
                f"Ваш текущий оборот: {stats.get('turnover', 0):.2f}$."
            )
            await state.clear()
            return

    await activate_check_logic(message, check_id, state)

@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.full_name
    full_name = message.from_user.full_name

    user = await db.get_user(user_id)
    if user:
        await db.update_user(user_id, {"username": username, "full_name": full_name})
    else:
        await db.create_user(user_id, username, full_name)

    welcome_text = (
        f"🎰 <b>Добро пожаловать в {CASINO_NAME}!</b>\n\n"
        f"<blockquote><b>Мы предлагаем лучшие азартные игры с честными выплатами. "
        f"Испытайте свою удачу и сорвите джекпот!</b></blockquote>"
    )

    await message.answer_video(
        video=types.FSInputFile("menu.mp4"),
        caption=welcome_text,
        reply_markup=create_main_keyboard(),
        parse_mode="HTML"
    )

@dp.message(
    or_f(
        Command("profile"), F.text == "👤 Профиль",
        Command("wallet"), F.text == "💰 Кошелек",
        Command("ref"), F.text == "👥 Реферальная система",
        Command("games"), F.text == "🎲 Сделать ставку",
        Command("checks"), F.text == "🧾 Чеки"
    ),
    StateFilter("*")
)
async def universal_main_menu_handler(message: types.Message, state: FSMContext):
    await state.clear()
    text = message.text or ""
    if text == "👤 Профиль" or text.startswith("/profile"):
        await handle_show_profile(message, state)
        return
    if text == "💰 Кошелек" or text.startswith("/wallet"):
        await show_wallet(message, state)
        return
    if text == "👥 Реферальная система" or text.startswith("/ref"):
        await show_referral(message, state)
        return
    if text == "🎲 Сделать ставку" or text.startswith("/games"):
        await choose_game(message, state)
        return
    if text == "🧾 Чеки" or text.startswith("/checks"):
        await show_checks_menu(message, state)
        return
    if text.startswith("/start"):
        await cmd_start(message, state)
        return
    await message.answer("Главное меню", reply_markup=create_main_keyboard())

@dp.callback_query(lambda c: c.data == "back_to_wallet")
async def back_to_wallet(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    user = await db.get_user(callback_query.from_user.id)
    balance = max(user.get("balance", Decimal("0")), Decimal('0'))
    bonus_balance = max(user.get("bonus_balance", Decimal("0")), Decimal('0'))
    if balance <= 0:
        await db.clean_empty_wagerings(callback_query.from_user.id)
    wagering_info = await db.get_user_wagering_info(callback_query.from_user.id)
    available = max(balance - bonus_balance, Decimal('0'))
    extra_parts = [
        f"🔒 <b>Бонусный баланс:</b> <code>{bonus_balance:.2f}$</code>",
        f"💸 <b>Доступно к выводу:</b> <code>{available:.2f}$</code>"
    ]
    if wagering_info and wagering_info['left'] > 0:
        left = wagering_info['left']
        total = wagering_info['total'] if wagering_info['total'] > 0 else left
        extra_parts.append(f"🔥 <b>Отыгрыш:</b> осталось <code>{left:.2f}$</code> из <code>{total:.2f}$</code>")
    extra = "\n" + "\n".join(extra_parts) if extra_parts else ""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Пополнить", callback_data="deposit")],
        [InlineKeyboardButton(text="💸 Вывести", callback_data="withdraw")]
    ])
    await callback_query.message.edit_caption(
        caption=(
            f"💰 <b>Кошелек</b>\n\n"
            f"💵 <b>Ваш баланс:</b> <code>{balance:.2f}$</code>{extra}\n\n"
            f"Выберите действие:"
        ),
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await callback_query.answer()

@dp.message(Command("checks"), StateFilter("*"))
@dp.message(F.text == "🧾 Чеки", StateFilter("*"))
async def show_checks_menu(message: types.Message, state: FSMContext):
    await state.clear()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать чек", callback_data="create_check_start")],
        [InlineKeyboardButton(text="⚙️ Управление чеками", callback_data="manage_checks_list_0")],
        [InlineKeyboardButton(text="📤 Создать из чата", switch_inline_query="")]
    ])
    await message.answer_video(
        video=types.FSInputFile("checks.mp4"),
        caption=(
            "🧾 <b>Чеки</b>\n\n"
            "Создавайте чеки и делитесь ими с другими пользователями!\n\n"
            "📋 <b>Доступные действия:</b>\n\n"
            "➕ <b>Создать чек</b> — пошаговое создание чека в этом диалоге\n"
            "⚙️ <b>Управление чеками</b> — просмотр и настройка ваших чеков\n"
            "📤 <b>Создать из чата</b> — создание чека прямо в другом чате"
        ),
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@dp.callback_query(lambda c: c.data.startswith("manage_checks_list_"))
async def show_user_checks_list(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    offset_str = callback_query.data.rsplit("_", 1)[-1]
    offset = int(offset_str) if offset_str.isdigit() else 0
    user_id = callback_query.from_user.id
    checks = await db.get_user_checks(user_id, limit=5, offset=offset)
    seen = set()
    checks = [check for check in checks if check.get('check_id') and not (check.get('check_id') in seen or seen.add(check.get('check_id')))]
    total_count = await db.count_user_checks(user_id)
    page = (offset // 5) + 1
    max_page = max(1, (total_count + 4) // 5)
    if not checks and offset == 0:
        await callback_query.answer("У вас еще нет созданных чеков.", show_alert=True)
        return
    text = (
        f"🧾 <b>Ваши чеки</b>\n\n"
        f"📋 <b>Всего чеков:</b> {total_count}\n"
        f"📄 <b>Страница:</b> {page}/{max_page}\n\n"
        f"<b>Выберите чек для управления:</b>"
    )
    keyboard_buttons = []
    for check in checks:
        check_type = "👨‍👩‍👧‍👦 Мульти" if check.get('is_multi') else ("🔒 Приватный" if check.get('target_user_id') else "🧾 Чек")
        amount = check.get('amount', 0)
        status_emoji = "✅" if check.get('status') == 'active' else "❌"
        button_text = f"{check_type} {amount:.2f}$ {status_emoji}"
        keyboard_buttons.append([InlineKeyboardButton(
            text=button_text,
            callback_data=f"manage_check_{check['check_id']}"
        )])
    nav_buttons = []
    if offset > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data=f"manage_checks_list_{max(0, offset-5)}"))
    nav_buttons.append(InlineKeyboardButton(text=f"{page}/{max_page}", callback_data="noop_page"))
    if offset + 5 < total_count:
        nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"manage_checks_list_{offset+5}"))
    if nav_buttons:
        keyboard_buttons.append(nav_buttons)
    keyboard_buttons.append([InlineKeyboardButton(text="🔙 В меню чеков", callback_data="back_to_checks_menu")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    if offset == 0 and callback_query.message:
        try:
            await callback_query.message.delete()
        except Exception:
            pass
        await callback_query.message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    else:
        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "back_to_checks_menu")
async def back_to_checks_menu(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    if callback_query.message:
        try:
            await callback_query.message.delete()
        except Exception:
            pass
    await show_checks_menu(callback_query.message, state)
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "create_check_start")
async def create_check_start(callback_query: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧾 Чек", callback_data="create_public_check")],
        [InlineKeyboardButton(text="👨‍👩‍👧‍👦 Мульти-чек", callback_data="create_multi_check")]
    ])
    await callback_query.message.edit_caption(
        caption=(
            "🧾 <b>Создание чека</b>\n\n"
            "Выберите тип чека:\n\n"
            "🧾 <b>Чек</b> — обычный чек, который может активировать любой пользователь\n"
            "👨‍👩‍👧‍👦 <b>Мульти-чек</b> — чек, который можно активировать несколько раз"
        ),
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "create_public_check")
async def start_public_check(callback_query: types.CallbackQuery, state: FSMContext):
    user = await db.get_user(callback_query.from_user.id)
    balance = user['balance'] if user and 'balance' in user else Decimal('0')
    bonus_locked = Decimal(str(user.get('bonus_balance', Decimal('0'))))
    clean_balance = max(balance - bonus_locked, Decimal('0'))
    await callback_query.message.edit_caption(
        caption=(
            f"🧾 <b>Создание чека</b>\n\n"
            f"💵 <b>Ваш баланс:</b> <code>{balance:.2f}$</code>\n"
            f"💸 <b>Доступно:</b> <code>{clean_balance:.2f}$</code>\n\n"
            f"📝 <b>Введите сумму для создания чека:</b>\n\n"
            f"<i>Пример: 10.50</i>"
        ),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="create_check_start")]
        ]),
        parse_mode="HTML"
    )
    await state.update_data(check_type='public')
    await state.set_state(CheckStates.CREATE_AMOUNT)
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "create_multi_check")
async def start_multi_check(callback_query: types.CallbackQuery, state: FSMContext):
    user = await db.get_user(callback_query.from_user.id)
    balance = user['balance'] if user and 'balance' in user else Decimal('0')
    bonus_locked = Decimal(str(user.get('bonus_balance', Decimal('0'))))
    clean_balance = max(balance - bonus_locked, Decimal('0'))
    await callback_query.message.edit_caption(
        caption=(
            f"👨‍👩‍👧‍👦 <b>Создание мульти-чека</b>\n\n"
            f"💵 <b>Ваш баланс:</b> <code>{balance:.2f}$</code>\n"
            f"💸 <b>Доступно:</b> <code>{clean_balance:.2f}$</code>\n\n"
            f"📝 <b>Введите общую сумму для создания чека:</b>\n\n"
            f"<i>Эта сумма будет разделена между всеми активациями</i>\n"
            f"<i>Пример: 100.00</i>"
        ),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="create_check_start")]
        ]),
        parse_mode="HTML"
    )
    await state.update_data(check_type='multi')
    await state.set_state(CheckStates.CREATE_AMOUNT)
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "create_private_check")
async def start_private_check(callback_query: types.CallbackQuery, state: FSMContext):
    user = await db.get_user(callback_query.from_user.id)
    balance = user['balance'] if user and 'balance' in user else Decimal('0')
    await callback_query.message.edit_caption(
        caption=(
            f"🔒 <b>Создание приватного чека</b>\n\n"
            f"Ваш баланс: <code>{balance:.2f}$</code>\n\n"
            f"Введите сумму для создания чека:"
        ),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_checks_menu")]
        ]),
        parse_mode="HTML"
    )
    await state.update_data(check_type='private')
    await state.set_state(CheckStates.CREATE_AMOUNT)
    await callback_query.answer()

@dp.message(CheckStates.CREATE_AMOUNT)
async def process_check_amount(message: types.Message, state: FSMContext):
    if not (message.text and not message.text.startswith('/') and message.text not in MAIN_MENU_BUTTONS):
        return
    user = await db.get_user(message.from_user.id)
    balance = user['balance'] if user and 'balance' in user else Decimal('0')
    bonus_locked = Decimal(str(user.get('bonus_balance', Decimal('0'))))
    clean_balance = max(balance - bonus_locked, Decimal('0'))  
    balance = max(balance, Decimal('0'))
    text = message.text.strip().replace(',', '.')
    try:
        amount = Decimal(text)
    except Exception:
        await message.answer(INVALID_AMOUNT_FORMAT_MSG)
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть больше нуля.")
        return
    if amount > clean_balance:
        await message.answer(f"❌ Недостаточно средств для создания чека.\nДоступно для создания чека: {clean_balance:.2f}$")
        return
    await state.update_data(amount=amount)
    data = await state.get_data()
    check_type = data.get('check_type')
    if check_type == 'public':
        check_id = str(uuid.uuid4())
        user_id = message.from_user.id
        try:
            await db.create_check_atomic(
                check_id,
                user_id,
                amount,
                target_user_id=None,
                is_multi=False
            )
        except InsufficientFundsError:
            await message.answer("❌ Недостаточно средств для создания чека. Попробуйте снова.")
            return
        text = (
            f"✅ <b>Чек успешно создан!</b>\n\n"
            f"🧾 <b>Тип:</b> Чек\n"
            f"💰 <b>Сумма:</b> <code>{amount:.2f}$</code>\n\n"
            f"<i>Вы можете настроить чек в меню управления</i>"
        )
        await send_check_management_message(message.chat.id, text, check_id)
        await state.clear()
        return
    if check_type == 'multi':
        await message.answer(
            f"👨‍👩‍👧‍👦 <b>Мульти-чек</b>\n\n"
            f"💰 <b>Общая сумма:</b> <code>{amount:.2f}$</code>\n\n"
            f"📝 <b>Введите количество активаций (от 2 до 100):</b>\n\n"
            f"<i>Каждый пользователь получит: {amount / Decimal('2'):.2f}$ - {amount / Decimal('100'):.2f}$</i>",
            parse_mode="HTML"
        )
        await state.set_state(CheckStates.CREATE_ACTIVATIONS)

@dp.message(CheckStates.CREATE_ACTIVATIONS)
async def process_check_activations(message: types.Message, state: FSMContext):
    if not (message.text and message.text.isdigit() and not message.text.startswith('/') and message.text not in MAIN_MENU_BUTTONS):
        if message.text and not message.text.isdigit():
            await message.answer("❌ Введите целое число.")
        return
    activations = int(message.text)
    if not 2 <= activations <= 100:
        await message.answer("❌ Количество активаций должно быть от 2 до 100.")
        return
    data = await state.get_data()
    amount = data.get('amount')
    user_id = message.from_user.id
    check_id = str(uuid.uuid4())
    try:
        await db.create_check_atomic(
            check_id,
            user_id,
            amount,
            is_multi=True,
            activations_total=activations
        )
    except InsufficientFundsError:
        await message.answer("❌ Недостаточно средств для создания мульти-чека.")
        await state.clear()
        return
    amount_per_user = amount / activations
    text = (
        f"✅ <b>Мульти-чек успешно создан!</b>\n\n"
        f"👨‍👩‍👧‍👦 <b>Тип:</b> Мульти-чек\n"
        f"💰 <b>Общая сумма:</b> <code>{amount:.2f}$</code>\n"
        f"👥 <b>Активаций:</b> <code>{activations}</code>\n"
        f"💵 <b>На каждого:</b> <code>{amount_per_user:.2f}$</code>\n\n"
        f"<i>Вы можете настроить чек в меню управления</i>"
    )
    await send_check_management_message(
        message.chat.id,
        text,
        check_id
    )
    await state.clear()

@dp.message(CheckStates.CREATE_TARGET_USER)
async def process_check_target(message: types.Message, state: FSMContext):
    if not (message.text and not message.text.startswith('/') and message.text not in MAIN_MENU_BUTTONS):
        return
    target_user_str = message.text.strip().lstrip('@')
    target_user = None
    if target_user_str.isdigit():
        target_user = await db.get_user(int(target_user_str))
    else:
        target_user = await db.get_user_by_username(target_user_str)
    if not target_user:
        await message.answer("❌ Пользователь не найден. Проверьте правильность введённых данных и попробуйте снова.")
        return
    data = await state.get_data()
    amount = data.get('amount')
    user_id = message.from_user.id
    check_id = str(uuid.uuid4())
    try:
        await db.create_check_atomic(
            check_id,
            user_id,
            amount,
            target_user_id=target_user['user_id'],
            is_multi=False
        )
    except InsufficientFundsError:
        await message.answer("❌ Недостаточно средств для создания приватного чека.")
        await state.clear()
        return
    username = target_user.get('username', '')
    target_display_name = f"@{username}" if username and ' ' not in username else username
    text = f"<b>🔒 Приватный чек для {target_display_name} на {amount:.2f}$</b>"
    await send_check_management_message(
        message.chat.id,
        text,
        check_id
    )
    await state.clear()

@dp.callback_query(lambda c: c.data == "deposit")
async def start_deposit(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.edit_caption(
        caption=(
            "💰 <b>Пополнение баланса</b>\n\n"
            "Введите сумму для пополнения в долларах:\n\n"
            "<b>Минимум:</b> 0.3$\n"
            "<b>Максимум:</b> 1000$"
        ),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_wallet")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(WalletStates.DEPOSIT_AMOUNT)
    await callback_query.answer()

@dp.message(WalletStates.DEPOSIT_AMOUNT)
async def process_deposit_amount(message: types.Message, state: FSMContext):
    if not message.text or message.text.startswith('/') or message.text in MAIN_MENU_BUTTONS:
        return
    text = message.text.strip().replace(',', '.')
    try:
        amount = Decimal(text)
    except Exception:
        await message.answer(INVALID_AMOUNT_FORMAT_MSG)
        return
    if amount < Decimal('0.3'):
        await message.answer("❌ Минимальная сумма пополнения: 0.3$")
        return
    if amount > Decimal('1000'):
        await message.answer("❌ Максимальная сумма пополнения: 1000$")
        return
    user_id = message.from_user.id
    timestamp = int(time.time())
    payload = f"deposit_{amount}_{user_id}_{timestamp}"
    try:
        invoice_data = await crypto_pay.create_invoice(
            asset="USDT",
            amount=str(amount),
            description=f"Пополнение баланса в {CASINO_NAME}",
            payload=payload,
            allow_comments=False
        )
    except Exception as e:
        logging.error(f"Ошибка создания инвойса: {e}")
        await message.answer("Ошибка при создании счета. Попробуйте позже.")
        return
    result = invoice_data.get('result', {})
    pay_url = result.get('bot_invoice_url') or result.get('pay_url')
    invoice_id = result.get('invoice_id', 'unknown')
    try:
        await bot.send_message(
            chat_id=LOGS_ID,
            text=(
                "💵 <b>Создан счет на пополнение</b>\n\n"
                f"<b>Пользователь:</b> {message.from_user.mention_html()}\n"
                f"<b>ID:</b> <code>{message.from_user.id}</code>\n"
                f"<b>Сумма:</b> <code>{amount} USDT</code>\n"
                f"<b>Invoice ID:</b> <code>{invoice_id}</code>\n"
                f"<b>Payload:</b> <code>{payload}</code>"
            ),
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Ошибка отправки сообщения в лог: {e}")
    await message.answer(
        f"✅ <b>Счет на пополнение создан!</b>\n\n"
        f"💰 <b>Сумма:</b> {amount} USDT\n\n"
        f"После оплаты баланс будет пополнен автоматически\n\n"
        f"<i>Счет будет действителен 30 минут.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"💳 Оплатить {amount} USDT", url=pay_url)]
        ]),
        parse_mode="HTML"
    )
    asyncio.create_task(delete_invoice_later(invoice_id, message.from_user.id, amount, "USDT"))
    await state.clear()

async def delete_invoice_later(invoice_id, user_id, amount_in_asset, asset):
    await asyncio.sleep(1800)
    try:
        invoices = await crypto_pay.get_invoices(status="active")
        items = invoices.get("result", {}).get("items", [])
        invoice = next((i for i in items if str(i.get("invoice_id")) == str(invoice_id)), None)
        if invoice:
            await crypto_pay._make_request("POST", "deleteInvoice", json={"invoice_id": invoice_id})
            await bot.send_message(
                user_id,
                f"❌ <b>Счет на пополнение {amount_in_asset} {asset} был автоматически удалён через 30 минут из-за не оплаты.</b>",
                parse_mode="HTML"
            )
    except Exception as e:
        logging.error(f"Ошибка удаления инвойса {invoice_id}: {e}")

@dp.callback_query(lambda c: c.data == "withdraw")
async def start_withdraw(callback_query: types.CallbackQuery, state: FSMContext):
    user = await db.get_user(callback_query.from_user.id)
    balance = user.get('balance', Decimal('0'))
    bonus_balance = user.get('bonus_balance', Decimal('0'))
    available = max(balance - bonus_balance, Decimal('0'))
    if available < Decimal('2.0'):
        await callback_query.answer(
            f"❌ Недостаточно средств\nВаш баланс: {balance:.2f}$\nДоступно к выводу: {available:.2f}$\nМинимум: 2.00$",
            show_alert=True
        )
        return
    wagering_info = await db.get_user_wagering_info(callback_query.from_user.id)
    left = wagering_info['left'] if wagering_info else Decimal('0')
    total = wagering_info['total'] if wagering_info else Decimal('0')
    caption_text = (
        f"💸 <b>Вывод средств</b>\n\n"
        f"Ваш баланс: <code>{balance:.2f}$</code>\n"
        f"Доступно к выводу: <code>{available:.2f}$</code>\n"
        f"🔒 Бонусный баланс: <code>{bonus_balance:.2f}$</code>\n"
    )
    if wagering_info and left > 0:
        total_display = total if total > 0 else left
        caption_text += f"🔥 Отыгрыш: осталось <code>{left:.2f}$</code> из <code>{total_display:.2f}$</code>\n\n"
    else:
        caption_text += "\n"
    caption_text += (
        "Введите сумму для вывода:\n\n"
        "<b>Минимум:</b> 2.00$\n"
        f"<b>Максимум:</b> {available:.2f}$"
    )
    await callback_query.message.edit_caption(
        caption=caption_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_wallet")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(WalletStates.WITHDRAW_AMOUNT)
    await callback_query.answer()

@dp.message(WalletStates.WITHDRAW_AMOUNT)
async def process_withdraw(message: types.Message, state: FSMContext):
    if not message.text or message.text.startswith('/') or message.text in MAIN_MENU_BUTTONS:
        return
    user = await db.get_user(message.from_user.id)
    balance = user.get('balance', Decimal('0'))
    bonus_balance = user.get('bonus_balance', Decimal('0'))
    available = max(balance - bonus_balance, Decimal('0'))
    clean_balance = available
    text = message.text.strip().replace(',', '.')
    try:
        amount = Decimal(text)
    except Exception:
        await message.answer(INVALID_AMOUNT_FORMAT_MSG)
        await state.clear()
        return
    if amount < Decimal('2.0'):
        await message.answer("❌ Минимальная сумма вывода: 2.00$")
        await state.clear()
        return
    if amount > clean_balance:
        await message.answer(f"❌ Недостаточно средств для вывода.\nДоступно для вывода: {clean_balance:.2f}$")
        await state.clear()
        return
    await db.update_balance(message.from_user.id, -amount)
    await message.answer(
        f"<b>✅ Ваш вывод добавлен в очередь</b>",
        parse_mode="HTML"
    )
    check_result = await create_payment_check(amount)
    if check_result and 'check_link' in check_result:
        await message.answer(
            f"✅ <b>Вывод успешно обработан!</b>\n\n"
            f"💰 <b>Сумма:</b> <code>{amount}$</code>\n\n"
            f"Заберите средства по кнопке ниже:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"Забрать {amount:.2f}$", url=check_result['check_link'])]
            ]),
            parse_mode="HTML"
        )
    else:
        await db.update_balance(message.from_user.id, amount)
        await message.answer(
            "🚫 <b>Вывод отменен по техническим причинам.</b>\n"
            "<blockquote><b>⚠️ В данный момент вывод не может быть обработан, попробуйте позже.\n"
            "💸 Ваши средства возвращены на баланс.</b></blockquote>",
            parse_mode="HTML"
        )
    await bot.send_message(
        chat_id=LOGS_ID,
        text=(
            "💸 <b>ВЫВОД СРЕДСТВ</b>\n\n"
            f"<b>Пользователь:</b> {message.from_user.mention_html()}\n"
            f"<b>ID:</b> <code>{message.from_user.id}</code>\n"
            f"<b>Сумма:</b> <code>{amount}$</code>"
        ),
        parse_mode="HTML"
    )
    await state.clear()

@dp.message(Command("ref"), StateFilter('*'))
@dp.message(F.text == "👥 Реферальная система", StateFilter('*'))
async def show_referral(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    if not user:
        info = message.from_user
        await db.create_user(info.id, info.username or info.full_name, info.full_name)
        user = await db.get_user(info.id)
    bot_username = await get_bot_username()
    ref_link = f"https://t.me/{bot_username}?start={user_id}"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Копировать ссылку", copy_text={"text": ref_link})],
            [InlineKeyboardButton(text="💸 Вывести баланс", callback_data="withdraw_ref_balance")],
            [InlineKeyboardButton(text="👥 Мои рефералы", callback_data="show_my_referrals")]
        ]
    )
    await message.answer_video(
        video=types.FSInputFile("ref.mp4"),
        caption=(
            f"👥 <b>Реферальная программа {CASINO_NAME}</b>\n\n"
            f"💰 <b>Баланс:</b> <code>{user.get('ref_balance', 0):.2f}$</code>\n"
            f"💎 <b>Всего заработано:</b> <code>{user.get('ref_earnings', 0):.2f}$</code>\n"
            f"👥 <b>Рефералов:</b> <code>{user.get('ref_count', 0)}</code>\n"
            f'<b><a href="{ref_link}">🔗 Ваша ссылка</a></b>\n\n'
            f"<b>Вы получаете +15% с выигрыша друга!</b>"
        ),
        reply_markup=keyboard,
        parse_mode="HTML",
        disable_web_page_preview=True
    )

@dp.callback_query(lambda c: c.data == "show_my_referrals" or c.data.startswith("referrals_page_"))
async def show_my_referrals(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    if callback_query.data.startswith("referrals_page_"):
        try:
            page = int(callback_query.data.split("_")[-1])
        except Exception:
            page = 0
    else:
        page = 0
    referrals = await db.get_user_referrals(user_id)
    total = len(referrals)
    pages = max(1, (total + REFERRALS_PER_PAGE - 1) // REFERRALS_PER_PAGE)
    start = page * REFERRALS_PER_PAGE
    end = start + REFERRALS_PER_PAGE
    current_referrals = referrals[start:end]
    if not referrals:
        text = "<b>У вас пока нет рефералов.</b>"
    else:
        text = f"<b>Ваши рефералы (стр. {page+1}/{pages}):</b>\n" + "\n".join([
            f"<code>{r['user_id']}</code> | @{r['username'] or '-'} | {sanitize_nickname(r['full_name'])}" for r in current_referrals
        ])
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data=f"referrals_page_{page-1}"))
    nav_buttons.append(InlineKeyboardButton(text=f"{page+1}/{pages}", callback_data="noop_ref_page"))
    if end < total:
        nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"referrals_page_{page+1}"))
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            nav_buttons if nav_buttons else [],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_ref_menu")]
        ]
    )
    try:
        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception:
        await callback_query.message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "back_to_ref_menu")
async def back_to_ref_menu(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.delete()
    class DummyMessage:
        def __init__(self, chat, from_user, answer_video):
            self.chat = chat
            self.from_user = from_user
            self.answer_video = answer_video
    await show_referral(DummyMessage(callback_query.message.chat, callback_query.from_user, callback_query.message.answer_video), state)
    await callback_query.answer()

def get_bet_keyboard(amount):
    betting_channel_url = "https://t.me/+MglBkaT0amdlZGRi"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Канал ставок", url=betting_channel_url)],
            [
                InlineKeyboardButton(text="⬇️", callback_data=f"decrease_bet_{amount}"),
                InlineKeyboardButton(text="🔁", callback_data="repeat_bet"),
                InlineKeyboardButton(text="⬆️", callback_data=f"increase_bet_{amount}")
            ],
            [InlineKeyboardButton(text="🎮 В меню игр", callback_data="new_bet")]
        ]
    )

def parse_message(message: types.Message) -> Optional[dict]:
    comment = game = name = user_id = amount = asset = None
    if message.entities and message.entities[0].user:
        user = message.entities[0].user
        name = user.full_name
        msg_text = message.text[len(name):].replace("🪙", "").split("💬")[0]
        if '@' in name:
            name = re.sub(r'@[\w]+', '***', name)
        user_id = int(user.id)
        if "отправил(а)" in msg_text:
            parts = msg_text.split("отправил(а)")
            if len(parts) > 1:
                asset_split = parts[1].split()
                if len(asset_split) > 1:
                    asset = asset_split[1]
        amount = Decimal('0')
        if "($" in msg_text:
            amount_part = msg_text.split("($")[1].split(').')[0].replace(',', "")
            if amount_part.replace('.', '', 1).isdigit():
                amount = Decimal(amount_part)
        if '💬' in message.text:
            comment = message.text.split("💬 ")[1].lower()
        else:
            comment = None
            game = None
    if comment is not None:
        game = comment.replace("ё", "е").replace(" ", "")
    return {
        'id': user_id,
        'name': name,
        'usd_amount': amount,
        'asset': asset,
        'comment': comment,
        'game': game
    }

def parse_invoice_payload(payload: str, user_id: int, amount: Decimal, username: str) -> Optional[dict]:
    parts = payload.split('_')
    if len(parts) < 4:
        return None
    game_type = parts[0]
    bet_type = '_'.join(parts[1:-2])
    try:
        payload_amount = Decimal(parts[-2])
        payload_user_id = int(parts[-1])
    except Exception:
        return None
    if payload_user_id != user_id or payload_amount != amount:
        return None
    return {
        'id': user_id,
        'name': username,
        'usd_amount': amount,
        'asset': 'USDT',
        'comment': bet_type,
        'game': bet_type,
        'is_bot_bet': True
    }

def parse_game_type_and_bet(comment: str):
    comment = comment.lower().replace(" ", "").replace("ё", "е")
    game_bets = {
        "basketball": {"мимо", "гол", "чистыйгол", "застрял"},
        "darts": {"белое", "красное", "яблочко", "промах"},
        "slots": {"казик", "слоты", "777", "джекпот"},
        "bowling": {"боулинг", "страйк", "боулпобеда", "боулпоражение", "боулпромах"},
        "cube": {"чет", "нечет", "больше", "меньше", "плинко", "1", "2", "3", "4", "5", "6", "сектор1", "сектор2", "сектор3"},
        "two_dice": {"ничья", "победа1", "победа2", "2чет", "2нечет", "2меньше", "2больше", "произведение18"},
        "rock_paper_scissors": {"камень", "ножницы", "бумага"},
        "football": {"футгол", "футпромах"}
    }
    for game, bets in game_bets.items():
        if comment in bets:
            return game, comment
    return None, None

def get_russian_names(game_type: str, bet_type: str) -> tuple[str, str]:
    game_name_map = {
        "cube": "🎲 кубик",
        "two_dice": "🎲 2 кубика",
        "rps": "👊✌️🖐 КНБ",
        "rock_paper_scissors": "👊✌️🖐 КНБ",
        "basketball": "🏀 баскетбол",
        "darts": "🎯 дартс",
        "slots": "🎰 слоты",
        "bowling": "🎳 боулинг",
        "football": "⚽ футбол",
        "custom": "✨ авторская"
    }
    bet_name_map = {
        "чет": "четное", "нечет": "нечетное", "больше": "больше", "меньше": "меньше",
        "плинко": "плинко", "победа1": "победа 1", "победа2": "победа 2", "ничья": "ничья", 
        "камень": "камень", "ножницы": "ножницы", "бумага": "бумага",
        "гол": "попадание", "мимо": "промах", "чистыйгол": "чистый гол", "застрял": "мяч застрял",
        "белое": "белое", "красное": "красное", "яблочко": "яблочко", "промах": "промах",
        "казик": "прокрут", "слоты": "прокрут", "777": "прокрут", "джекпот": "прокрут",
        "боулинг": "бросок", "боул": "бросок", "страйк": "страйк", "спэр": "спэр",
        "боулпобеда": "победа", "боулпоражение": "поражение",
        "2чет": "2 чет", "2нечет": "2 нечет", "2меньше": "2 меньше", "2больше": "2 больше", "произведение18": "произведение ≥ 18",
        "сектор1": "сектор 1", "сектор2": "сектор 2", "сектор3": "сектор 3",
        "футгол": "гол", "футпромах": "промах",
        "custom1": "📞 x2", "custom2": "🌈 x3", "custom3": "🎮 x5", "custom4": "💣 x10",
        "custom5": "🔮 x20", "custom6": "🔭 x30", "custom7": "📱 x50", "custom8": "🚀 x100"
    }
    if not game_type:
        return "—", bet_type
    game_name = game_name_map.get(game_type, str(game_type).lower())
    if game_type == 'cube' and bet_type in {'1', '2', '3', '4', '5', '6'}:
        bet_name = bet_type
    else:
        bet_name = bet_name_map.get(bet_type, bet_type)
    return game_name, bet_name

@dp.message(Command("games"), StateFilter('*'))
@dp.message(F.text == "🎲 Сделать ставку", StateFilter('*'))
async def choose_game(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer_video(
        video=types.FSInputFile("games.mp4"),
        caption=(
            "🎮 <b>Выберите игру:</b>\n\n"
            f"<b>После выбора игры и исхода ваша ставка сыграет в нашем <a href=\"https://t.me/+MglBkaT0amdlZGRi\">канале ставок</a>.</b>"
        ),
        reply_markup=create_games_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(GameStates.CHOOSE_GAME)

@dp.callback_query(lambda c: c.data.startswith("game_"))
async def choose_bet_type(callback_query: types.CallbackQuery, state: FSMContext):
    game_type = callback_query.data.replace("game_", "")
    await state.update_data(game_type=game_type)
    bet_options = {
        "cube": {
            "title": "🎲 Кубик",
            "options": [
                ("Четное", "чет"),
                ("Нечетное", "нечет"),
                ("Больше", "больше"),
                ("Меньше", "меньше"),
                ("Сектор 1", "сектор1"),
                ("Сектор 2", "сектор2"),
                ("Сектор 3", "сектор3"),
                ("1", "1"),
                ("2", "2"),
                ("3", "3"),
                ("4", "4"),
                ("5", "5"),
                ("6", "6"),
                ("Плинко", "плинко"),
            ]
        },
        "two_dice": {
            "title": "🎲🎲 Два кубика",
            "options": [
                ("Ничья", "ничья"),
                ("Победа 1", "победа1"),
                ("Победа 2", "победа2"),
                ("2 Чет", "2чет"),
                ("2 Нечет", "2нечет"),
                ("2 Меньше", "2меньше"),
                ("2 Больше", "2больше"),
                ("Произведение ≥ 18", "произведение18")
            ]
        },
        "rock_paper_scissors": {
            "title": "👊 Камень-ножницы-бумага",
            "options": [
                ("👊 Камень", "камень"),
                ("✋ Бумага", "бумага"),
                ("✌️ Ножницы", "ножницы")
            ]
        },
        "basketball": {
            "title": "🏀 Баскетбол",
            "options": [
                ("Попадание", "гол"),
                ("Промах", "мимо"),
                ("Чистый гол", "чистыйгол"),
                ("Мяч застрял", "застрял")
            ]
        },
        "darts": {
            "title": "🎯 Дартс",
            "options": [
                ("Белое", "белое"),
                ("Красное", "красное"),
                ("Яблочко", "яблочко"),
                ("Промах", "промах")
            ]
        },
        "slots": {
            "title": "🎰 Слоты",
            "options": [
                ("🎰 Крутить", "казик")
            ]
        },
        "bowling": {
            "title": "🎳 Боулинг",
            "options": [
                ("Бросок", "боулинг"),
                ("Страйк", "страйк"),
                ("Промах", "боулпромах"),
                ("Победа", "боулпобеда"),
                ("Поражение", "боулпоражение")
            ]
        },
        "football": {
            "title": "⚽ Футбол",
            "options": [
                ("Гол", "футгол"),
                ("Промах", "футпромах")
            ]
        },
        "custom": {
            "title": "✨ Авторские игры",
            "options": [
                ("📞 x2", "custom1"), ("🌈 x3", "custom2"), ("🎮 x5", "custom3"), ("💣 x10", "custom4"),
                ("🔮 x20", "custom5"), ("🔭 x30", "custom6"), ("📱 x50", "custom7"), ("🚀 x100", "custom8")
            ]
        },
    }
    game_info = bet_options.get(game_type)
    if not game_info:
        await callback_query.answer("Игра не найдена", show_alert=True)
        return
    bet_text = f"{game_info['title']}\n\n<b>Выберите тип ставки:</b>"
    options = game_info['options']
    if game_type == "custom":
        keyboard_buttons = [
            [InlineKeyboardButton(text=opt[0], callback_data=f"bet_{opt[1]}") for opt in options[i:i+4]]
            for i in range(0, len(options), 4)
        ]
    elif game_type == "cube":
        option_map = {opt[1]: opt[0] for opt in options}

        def cube_button(key: str) -> InlineKeyboardButton:
            return InlineKeyboardButton(text=option_map[key], callback_data=f"bet_{key}")

        keyboard_buttons = [
            [cube_button("чет"), cube_button("нечет")],
            [cube_button("больше"), cube_button("меньше")],
            [cube_button("сектор1"), cube_button("сектор2")],
            [cube_button("сектор3")],
            [cube_button("1"), cube_button("2"), cube_button("3")],
            [cube_button("4"), cube_button("5"), cube_button("6")],
            [cube_button("плинко")]
        ]
    else:
        keyboard_buttons = [
            [InlineKeyboardButton(text=options[i][0], callback_data=f"bet_{options[i][1]}")] +
            ([InlineKeyboardButton(text=options[i+1][0], callback_data=f"bet_{options[i+1][1]}")] if i + 1 < len(options) else [])            for i in range(0, len(options), 2)
        ]
    keyboard_buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_games")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    msg = callback_query.message
    try:
        await msg.edit_caption(
            caption=bet_text,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
    except aiogram.exceptions.TelegramBadRequest as e:
        try:
            await msg.edit_text(
                bet_text,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        except aiogram.exceptions.TelegramBadRequest as e2:
            pass
    await state.set_state(GameStates.CHOOSE_BET_TYPE)
    await callback_query.answer()

def build_balance_choice_keyboard(clean_balance: Decimal, bonus_balance: Decimal) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text=f"💵 Основной ({clean_balance:.2f}$)", callback_data="choose_balance_main")]
    ]
    if bonus_balance > Decimal('0'):
        buttons.append([InlineKeyboardButton(text=f"💎 Бонусный ({bonus_balance:.2f}$)", callback_data="choose_balance_bonus")])
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_bet_type")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def show_amount_prompt(target_message: types.Message, balance: Decimal, bonus_balance: Decimal, balance_type: str, back_callback: str):
    balance = Decimal(str(balance))
    bonus_balance = Decimal(str(bonus_balance))
    clean_balance = max(balance - bonus_balance, Decimal('0'))
    if balance_type == 'bonus':
        available = min(bonus_balance, balance)
        balance_label = "Бонусный"
    else:
        available = clean_balance
        balance_label = "Основной"
    amount_text = (
        f"💰 <b>Введите сумму ставки в долларах</b>\n\n"
        f"<blockquote>"
        f"<b>Баланс:</b> <code>{balance:.2f}$</code>\n"
        f"<b>{balance_label} доступно:</b> <code>{available:.2f}$</code>\n"
        f"<b>Минимальная ставка:</b> 0.30$\n"
        f"<b>Максимальная ставка:</b> 1000$\n"
        f"<b>Пример:</b> 5.50"
        f"</blockquote>"
    )
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data=back_callback)]
        ]
    )
    try:
        await target_message.edit_caption(
            caption=amount_text,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
    except aiogram.exceptions.TelegramBadRequest:
        await target_message.edit_text(
            amount_text,
            reply_markup=keyboard,
            parse_mode="HTML"
        )

async def create_payment_check(amount: Decimal, description: str = None) -> dict:
    if not description:
        description = f"Выигрыш {amount}$ в {CASINO_NAME}"
    balance_data = await crypto_pay.get_balance()
    balances = balance_data.get('result', [])
    usdt_balance = next((Decimal(balance.get('available', '0')) for balance in balances if balance.get('currency_code', '').upper() == 'USDT'), Decimal('0'))
    if usdt_balance <= 0:
        usdt_balance = Decimal('0.31')
    if usdt_balance < amount:
        await bot.send_message(
            chat_id=LOGS_ID,
            text=f"⚠️ <b>Недостаточно средств для создания чека</b>\n"
                 f"<b>Требуется:</b> <code>{amount}$</code>\n"
                 f"<b>Доступно:</b> <code>{usdt_balance}$</code>",
            parse_mode="HTML"
        )
        return None
    result = await crypto_pay.create_check(
        asset="USDT",
        amount=str(amount),
        description=description,
        hidden_message=f"Поздравляем с выигрышем в {CASINO_NAME}!"
    )
    await bot.send_message(
        chat_id=LOGS_ID,
        text=f"💸 <b>СОЗДАН ЧЕК НА ВЫПЛАТУ</b>\n\n"
             f"<b>Сумма:</b> <code>{amount}$</code>\n"
             f"<b>Новый баланс казны:</b> <code>{usdt_balance - amount}$</code>",
        parse_mode="HTML"
    )
    if result.get('ok') and 'result' in result:
        check_data = result['result']
        return {
            'check_id': check_data.get('check_id'),
            'check_link': check_data.get('bot_check_url'),
            'amount': check_data.get('amount')
        }
    return None

async def send_bet_error(chat_id: int, name: str):
    error_text = (
        f"<code>{name}</code> <b>Произошла ошибка!</b> \n\n"
        f"<blockquote><b>Возможные причины</b> \n"
        f"• <b>Не указан комментарий</b>\n"
        f"• <b>Некорректный комментарий</b>\n"
        f"• <b>Обратитесь в поддержку за выплатой</b></blockquote>"
    )
    await bot.send_message(
        chat_id=chat_id,
        text=error_text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Обратиться в поддержку", url=SUPPORT_LINK)]
        ])
    )

async def edit_message_or_inline(callback_query: types.CallbackQuery, text: str, reply_markup: InlineKeyboardMarkup = None, parse_mode="HTML"):
    if callback_query.inline_message_id:
        await bot.edit_message_text(
            text,
            inline_message_id=callback_query.inline_message_id,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
        return
    if callback_query.message:
        await callback_query.message.edit_text(
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )

async def get_management_menu(check_id: str, requester_id: Optional[int] = None):
    check = await db.get_check(check_id)
    if not check:
        return "Чек не найден.", None
    
    # Получаем информацию о привязанном пользователе
    target_user_id = check.get('target_user_id')
    target_user_info = "—"
    if target_user_id:
        target_user = await db.get_user(target_user_id)
        if target_user:
            username = target_user.get('username', '')
            full_name = target_user.get('full_name', '')
            if username:
                target_user_info = f"@{username}"
            elif full_name:
                target_user_info = sanitize_nickname(full_name)
            else:
                target_user_info = f"ID {target_user_id}"
    
    password_status = "✅ Установлен" if check.get('password') else "❌ Не установлен"
    turnover_status = f"{check.get('required_turnover', 0):.2f}$" if Decimal(str(check.get('required_turnover', 0))) > 0 else "—"
    premium_status = "✅ Да" if check.get('premium_only') else "❌ Нет"
    wagering_status = f"x{check.get('wagering_multiplier', 0)}" if check.get('wagering_multiplier', 0) and Decimal(str(check.get('wagering_multiplier', 0))) > 0 else "—"
    comment_status = check.get('comment') or '—'
    
    check_type = "👨‍👩‍👧‍👦 Мульти-чек" if check.get('is_multi') else "🧾 Чек"
    if target_user_id:
        check_type = "🔒 Приватный чек"
    
    text = (
        f"⚙️ <b>Управление чеком</b>\n\n"
        f"📋 <b>Тип:</b> {check_type}\n"
        f"💰 <b>Сумма:</b> <code>{check.get('amount', 0):.2f}$</code>\n"
        f"👤 <b>Привязан к:</b> {target_user_info}\n\n"
        f"<b>⚙️ Настройки:</b>\n"
        f"🔑 <b>Пароль:</b> {password_status}\n"
        f"💸 <b>Требуемый оборот:</b> {turnover_status}\n"
        f"⭐ <b>Только для Premium:</b> {premium_status}\n"
        f"🔥 <b>Отыгрыш:</b> {wagering_status}\n"
        f"💬 <b>Комментарий:</b> {comment_status}\n\n"
        f"<b>Выберите действие:</b>"
    )
    bot_username = await get_bot_username()
    check_link = f"https://t.me/{bot_username}?start=check_{check_id}"
    
    show_wager_controls = False
    if requester_id:
        try:
            show_wager_controls = await admin.is_admin(requester_id)
        except Exception:
            show_wager_controls = False
    
    action_rows = [
        [
            InlineKeyboardButton(text="🔑 Пароль", callback_data=f"set_password_{check_id}"),
            InlineKeyboardButton(text="💸 Оборот", callback_data=f"set_turnover_{check_id}")
        ],
        [
            InlineKeyboardButton(text="⭐ Premium", callback_data=f"set_premium_{check_id}"),
            InlineKeyboardButton(text="👤 Пользователь", callback_data=f"set_target_user_{check_id}")
        ]
    ]
    third_row = []
    if show_wager_controls:
        third_row.append(InlineKeyboardButton(text="🔥 Отыгрыш", callback_data=f"set_wagering_{check_id}"))
    third_row.append(InlineKeyboardButton(text="💬 Комментарий", callback_data=f"set_comment_{check_id}"))
    if third_row:
        action_rows.append(third_row)
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=action_rows + [
            [
                InlineKeyboardButton(text="↪️ Поделиться", switch_inline_query=f"{check_id}"),
                InlineKeyboardButton(text="📋 Копировать", copy_text={"text": check_link})
            ],
            [InlineKeyboardButton(text="❌ Удалить чек", callback_data=f"confirm_delete_check_{check_id}")],
            [InlineKeyboardButton(text="🔙 Назад к списку", callback_data="manage_checks_list_0")]
        ]
    )
    return text, keyboard

@dp.callback_query(lambda c: c.data.startswith("bet_"))
async def choose_balance(callback_query: types.CallbackQuery, state: FSMContext):
    bet_type = callback_query.data[4:]
    await state.update_data(bet_type=bet_type)
    user = await db.get_user(callback_query.from_user.id)
    if not user:
        await callback_query.answer("Профиль не найден", show_alert=True)
        return
    balance = Decimal(str(user.get('balance', 0) or 0))
    bonus_balance = Decimal(str(user.get('bonus_balance', 0) or 0))
    clean_balance = max(balance - bonus_balance, Decimal('0'))
    msg = callback_query.message
    if bonus_balance > Decimal('0'):
        keyboard = build_balance_choice_keyboard(clean_balance, bonus_balance)
        caption = (
            f"💼 <b>Выберите баланс для ставки</b>\n\n"
            f"<blockquote>"
            f"💵 <b>Основной:</b> <code>{clean_balance:.2f}$</code>\n"
            f"💎 <b>Бонусный:</b> <code>{bonus_balance:.2f}$</code>"
            f"</blockquote>"
        )
        try:
            await msg.edit_caption(
                caption=caption,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        except aiogram.exceptions.TelegramBadRequest:
            await msg.edit_text(
                caption,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        await state.update_data(balance_selection_skipped=False)
        await state.set_state(GameStates.CHOOSE_BALANCE)
    else:
        await state.update_data(balance_type='main', balance_selection_skipped=True)
        await show_amount_prompt(msg, balance, bonus_balance, 'main', back_callback="back_to_bet_type")
        await state.set_state(GameStates.ENTER_AMOUNT)
    await callback_query.answer()

@dp.callback_query(lambda c: c.data in ("choose_balance_main", "choose_balance_bonus"), GameStates.CHOOSE_BALANCE)
async def handle_balance_choice(callback_query: types.CallbackQuery, state: FSMContext):
    balance_type = 'bonus' if callback_query.data.endswith("bonus") else 'main'
    user = await db.get_user(callback_query.from_user.id)
    if not user:
        await callback_query.answer("Профиль не найден", show_alert=True)
        return
    balance = Decimal(str(user.get('balance', 0) or 0))
    bonus_balance = Decimal(str(user.get('bonus_balance', 0) or 0))
    await state.update_data(balance_type=balance_type, balance_selection_skipped=False)
    await show_amount_prompt(
        callback_query.message,
        balance,
        bonus_balance,
        balance_type,
        back_callback="back_to_balance"
    )
    await state.set_state(GameStates.ENTER_AMOUNT)
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "back_to_bet_type")
async def back_to_bet_type(callback_query: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    game_type = data.get('game_type')
    if not game_type:
        await callback_query.answer("Сообщение устарело", show_alert=True)
        return
    class DummyCallback:
        def __init__(self, message, from_user, game_type):
            self.data = f"game_{game_type}"
            self.message = message
            self.from_user = from_user
        async def answer(self, *a, **kw):
            pass
    await choose_bet_type(DummyCallback(callback_query.message, callback_query.from_user, game_type), state)

@dp.callback_query(lambda c: c.data == "back_to_balance")
async def back_to_balance(callback_query: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if data.get('balance_selection_skipped'):
        await back_to_bet_type(callback_query, state)
        return
    user = await db.get_user(callback_query.from_user.id)
    if not user:
        await callback_query.answer("Профиль не найден", show_alert=True)
        return
    balance = Decimal(str(user.get('balance', 0) or 0))
    bonus_balance = Decimal(str(user.get('bonus_balance', 0) or 0))
    clean_balance = max(balance - bonus_balance, Decimal('0'))
    keyboard = build_balance_choice_keyboard(clean_balance, bonus_balance)
    caption = (
        f"💼 <b>Выберите баланс для ставки</b>\n\n"
        f"<blockquote>"
        f"💵 <b>Основной:</b> <code>{clean_balance:.2f}$</code>\n"
        f"💎 <b>Бонусный:</b> <code>{bonus_balance:.2f}$</code>"
        f"</blockquote>"
    )
    try:
        await callback_query.message.edit_caption(
            caption=caption,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
    except aiogram.exceptions.TelegramBadRequest:
        await callback_query.message.edit_text(
            caption,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
    await state.set_state(GameStates.CHOOSE_BALANCE)
    await callback_query.answer()

user_last_bet_time = {}

@dp.message(GameStates.ENTER_AMOUNT)
async def create_bet_from_balance(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    wait_time = 10
    last_time = user_last_bet_time.get(user_id, 0)
    now = time.time()
    if now - last_time < wait_time:
        seconds_left = int(wait_time - (now - last_time))
        await message.answer(f'⏳ Флуд-контроль. Сделайте ставку через {seconds_left} секунд')
        await state.clear()
        return

    try:
        amount = Decimal(message.text.strip().replace(',', '.'))
    except Exception:
        await message.answer(INVALID_AMOUNT_FORMAT_MSG)
        await state.clear()
        return

    if not Decimal('0.30') <= amount <= Decimal('1000'):
        await message.answer("❌ Минимальная сумма ставки: 0.30$" if amount < Decimal('0.30') else "❌ Максимальная сумма ставки: 1000$")
        return
    user = await db.get_user(message.from_user.id)
    if not user:
        await db.create_user(message.from_user.id, message.from_user.full_name, message.from_user.full_name)
        await message.answer(
            "❌ <b>Ваш профиль не найден</b>\n\n"
            "Мы создали новый профиль для вас. Пожалуйста, пополните баланс для совершения ставок.",
            parse_mode="HTML"
        )
        await state.clear()
        return
    state_data = await state.get_data()
    game_type = state_data.get('game_type')
    bet_type = state_data.get('bet_type', 'unknown')
    balance_type = state_data.get('balance_type', 'main')
    balance = Decimal(str(user.get('balance', 0) or 0))
    bonus_balance = Decimal(str(user.get('bonus_balance', 0) or 0))
    clean_balance = max(balance - bonus_balance, Decimal('0'))
    bonus_available = min(bonus_balance, balance)
    if balance_type == 'bonus':
        available_funds = bonus_available
        if available_funds < amount:
            await message.answer(
                f"❌ <b>Недостаточно бонусных средств</b>\n\n"
                f"Бонусный баланс: <code>{bonus_balance:.2f}$</code>\n"
                f"Требуется: <code>{amount:.2f}$</code>",
                parse_mode="HTML"
            )
            await state.clear()
            return
        success = await db.deduct_bonus_funds(message.from_user.id, amount)
        if not success:
            await message.answer("❌ Не удалось списать бонусные средства. Попробуйте ещё раз.", parse_mode="HTML")
            await state.clear()
            return
    else:
        available_funds = clean_balance
        if available_funds < amount:
            await message.answer(
                f"❌ <b>Недостаточно средств</b>\n\n"
                f"Доступно: <code>{available_funds:.2f}$</code>\n"
                f"Требуется: <code>{amount:.2f}$</code>",
                parse_mode="HTML"
            )
            await state.clear()
            return
        await db.update_balance(message.from_user.id, -amount)
    is_bonus_bet = (balance_type == 'bonus')
    await state.update_data(last_bet_amount=amount, last_balance_type=balance_type)
    await db.remove_wagering_if_balance_negative(message.from_user.id)
    await db.add_transaction(user_id=message.from_user.id, amount=-amount, type='game', game_type=game_type)
    await db.add_to_queue(user_id=message.from_user.id, amount=amount, game=game_type, bet_type=bet_type, is_bonus_bet=is_bonus_bet)
    game_name_rus, bet_type_rus = get_russian_names(game_type, bet_type)
    keyboard = get_bet_keyboard(amount)
    await bot.send_message(
        chat_id=message.from_user.id,
        text=(
            f"✅ <b>Ставка принята!</b>\n\n"
            f"<blockquote>"
            f"🎮 <b>Игра:</b> {game_name_rus}\n"
            f"🎯 <b>Ставка:</b> {bet_type_rus}\n"
            f"💳 <b>Баланс:</b> {'Бонусный' if is_bonus_bet else 'Основной'}\n"
            f"💰 <b>Сумма:</b> {amount:.2f}$\n\n"
            f"Результат смотрите в канале ставок"
            f"</blockquote>"
        ),
        parse_mode="HTML",
        reply_markup=keyboard
    )
    asyncio.create_task(process_bet_queue())
    await state.clear()
    user_last_bet_time[user_id] = time.time()

processing_bet = False

async def process_bet_queue():
    global processing_bet
    if processing_bet:
        return
    processing_bet = True
    logging.info("[BET_QUEUE] process_bet_queue started")
    while True:
        bet = await db.get_next_bet()
        if not bet:
            break
        user = await db.get_user(bet['user_id'])
        data = {
            'id': bet['user_id'],
            'name': user.get('full_name', f"User {bet['user_id']}") or f"User {bet['user_id']}",
            'usd_amount': bet['amount'],
            'asset': 'USDT',
            'comment': bet['bet_type'],
            'game': bet['game'],
            'queue_id': bet['id'],
            'is_bonus_bet': bool(bet.get('is_bonus_bet'))
        }
        try:
            await process_bet(data)
            pending_bet = await db.get_user_pending_bet(bet['user_id'])
            if pending_bet and pending_bet.get('id') == bet['id']:
                await db.mark_user_pending_bets_processed(bet['user_id'])
        except Exception as e:
            logging.error(f"[BET_QUEUE] Ошибка в process_bet_queue: {e}")
    processing_bet = False
    logging.info("[BET_QUEUE] process_bet_queue finished")

@dp.callback_query(lambda c: c.data == "new_bet")
async def new_bet(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.delete()
    await choose_game(callback_query.message, state)
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "repeat_bet")
async def repeat_bet_callback(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id

    pending_bet = await db.get_user_pending_bet(user_id)
    if pending_bet:
        await callback_query.answer("❗️ У вас уже есть необработанная ставка. Дождитесь завершения предыдущей!", show_alert=True)
        return

    wait_time = 10
    last_time = user_last_bet_time.get(user_id, 0)
    now = time.time()
    if now - last_time < wait_time:
        seconds_left = int(wait_time - (now - last_time))
        await callback_query.answer(f'⏳ Флуд-контроль. Сделайте ставку через {seconds_left} секунд', show_alert=True)
        return

    last_bet = await db.get_last_bet(user_id)
    if not last_bet or not last_bet.get('bet_type'):
        await callback_query.answer("Нет предыдущей ставки для повторения.", show_alert=True)
        return
    state_data = await state.get_data()
    last_bet_amount = state_data.get('last_bet_amount')
    if last_bet_amount is not None:
        amount = last_bet_amount
    else:
        amount = last_bet['amount']
    game = last_bet.get('game') or last_bet.get('game_type')
    bet_type = last_bet.get('bet_type')
    raw_bonus_flag = last_bet.get('is_bonus_bet')
    try:
        is_bonus_bet = bool(int(raw_bonus_flag))
    except (TypeError, ValueError):
        is_bonus_bet = bool(raw_bonus_flag)
    user = await db.get_user(user_id)
    balance = Decimal(str(user.get('balance', 0) or 0))
    bonus_balance = Decimal(str(user.get('bonus_balance', 0) or 0))
    clean_balance = max(balance - bonus_balance, Decimal('0'))
    bonus_available = min(bonus_balance, balance)
    if is_bonus_bet:
        if bonus_available < amount:
            await callback_query.answer("❌ Недостаточно бонусных средств для повторения ставки.", show_alert=True)
            return
        success = await db.deduct_bonus_funds(user_id, amount)
        if not success:
            await callback_query.answer("❌ Не удалось списать бонусные средства.", show_alert=True)
            return
        balance_type = 'bonus'
    else:
        if clean_balance < amount:
            await callback_query.answer("❌ Недостаточно средств для повторения ставки.", show_alert=True)
            return
        await db.update_balance(user_id, -amount)
        balance_type = 'main'
    await db.mark_user_pending_bets_processed(user_id)
    await db.add_transaction(user_id=user_id, amount=-amount, type='game', game_type=game)
    await db.add_to_queue(user_id=user_id, amount=amount, game=game, bet_type=bet_type, is_bonus_bet=is_bonus_bet)
    await state.update_data(game_type=game, bet_type=bet_type, last_bet_amount=amount, last_balance_type=balance_type)
    game_name_rus, bet_type_rus = get_russian_names(game, bet_type)
    keyboard = get_bet_keyboard(amount)
    await bot.send_message(
        chat_id=user_id,
        text=(
            f"🔁 <b>Ставка повторена!</b>\n\n"
            f"<blockquote>"
            f"🎮 <b>Игра:</b> {game_name_rus}\n"
            f"🎯 <b>Ставка:</b> {bet_type_rus}\n"
            f"💳 <b>Баланс:</b> {'Бонусный' if is_bonus_bet else 'Основной'}\n"
            f"💰 <b>Сумма:</b> {amount:.2f}$\n\n"
            f"Результат смотрите в канале ставок"
            f"</blockquote>"
        ),
        parse_mode="HTML",
        reply_markup=keyboard
    )
    asyncio.create_task(process_bet_queue())
    await callback_query.answer("Ставка повторена!")
    user_last_bet_time[user_id] = time.time()

async def get_bet_state(callback_query, state):
    state_data = await state.get_data()
    game_type = state_data.get('game_type')
    bet_type = state_data.get('bet_type')
    last_bet_amount = state_data.get('last_bet_amount')
    if not game_type or not bet_type:
        user_id = callback_query.from_user.id
        pending_bet = await db.get_user_pending_bet(user_id)
        if pending_bet:
            game_type = pending_bet.get('game') or pending_bet.get('game_type')
            bet_type = pending_bet.get('bet_type')
            last_bet_amount = pending_bet.get('amount')
        else:
            last_bet = await db.get_last_bet(user_id)
            if last_bet:
                game_type = last_bet.get('game') or last_bet.get('game_type')
                bet_type = last_bet.get('bet_type')
                last_bet_amount = last_bet.get('amount')
        if game_type and bet_type:
            await state.update_data(game_type=game_type, bet_type=bet_type, last_bet_amount=last_bet_amount)
    return game_type, bet_type, last_bet_amount

@dp.callback_query(lambda c: c.data.startswith("increase_bet_"))
async def increase_bet(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        current_amount = Decimal(callback_query.data.split("_")[-1])
    except Exception:
        _, _, current_amount = await get_bet_state(callback_query, state)
        if not current_amount:
            current_amount = Decimal('1')
    new_amount = current_amount * 2
    if new_amount > 1000:
        await callback_query.answer("Максимальная ставка: 1000$", show_alert=True)
        return
    user = await db.get_user(callback_query.from_user.id)
    balance = user.get('balance', Decimal('0'))
    if balance < new_amount:
        await callback_query.answer("Недостаточно средств", show_alert=True)
        return
    game_type, bet_type, _ = await get_bet_state(callback_query, state)
    if not game_type or not bet_type:
        await callback_query.answer("Ошибка: не выбрана игра или тип ставки.", show_alert=True)
        return
    await state.update_data(last_bet_amount=new_amount, game_type=game_type, bet_type=bet_type)
    keyboard = get_bet_keyboard(new_amount)
    game_name_rus, bet_type_rus = get_russian_names(game_type, bet_type)
    await callback_query.message.edit_text(
        f"🔁 <b>Ставка увеличена!</b>\n\n"
        f"<blockquote>"
        f"🎮 <b>Игра:</b> {game_name_rus}\n"
        f"🎯 <b>Ставка:</b> {bet_type_rus}\n"
        f"💰 <b>Сумма:</b> {new_amount:.2f}$\n\n"
        f"Результат смотрите в канале ставок"
        f"</blockquote>",
        parse_mode="HTML",
        reply_markup=keyboard
    )
    await callback_query.answer(f"Сумма изменена: {new_amount:.2f}$")

@dp.callback_query(lambda c: c.data.startswith("decrease_bet_"))
async def decrease_bet(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        current_amount = Decimal(callback_query.data.split("_")[-1])
    except Exception:
        _, _, current_amount = await get_bet_state(callback_query, state)
        if not current_amount:
            current_amount = Decimal('1')
    new_amount = current_amount / 2
    if new_amount < Decimal('0.3'):
        await callback_query.answer("Минимальная ставка: 0.30$", show_alert=True)
        return
    game_type, bet_type, _ = await get_bet_state(callback_query, state)
    if not game_type or not bet_type:
        await callback_query.answer("Ошибка: не выбрана игра или тип ставки.", show_alert=True)
        return
    await state.update_data(last_bet_amount=new_amount, game_type=game_type, bet_type=bet_type)
    keyboard = get_bet_keyboard(new_amount)
    game_name_rus, bet_type_rus = get_russian_names(game_type, bet_type)
    await callback_query.message.edit_text(
        f"🔁 <b>Ставка уменьшена!</b>\n\n"
        f"<blockquote>"
        f"🎮 <b>Игра:</b> {game_name_rus}\n"
        f"🎯 <b>Ставка:</b> {bet_type_rus}\n"
        f"💰 <b>Сумма:</b> {new_amount:.2f}$\n\n"
        f"Результат смотрите в канале ставок"
        f"</blockquote>",
        parse_mode="HTML",
        reply_markup=keyboard
    )
    await callback_query.answer(f"Сумма изменена: {new_amount:.2f}$")

@dp.callback_query(lambda c: c.data == "back_to_games")
async def back_to_games(callback_query: types.CallbackQuery, state: FSMContext):
    games_text = (
        "🎮 <b>Выберите игру:</b>\n\n"
        f"<b>После выбора игры и исхода ваша ставка сыграет в нашем <a href=\"https://t.me/+MglBkaT0amdlZGRi\">канале ставок</a>.</b>"
    )
    msg = callback_query.message
    try:
        await msg.edit_caption(
            caption=games_text,
            reply_markup=create_games_keyboard(),
            parse_mode="HTML"
        )
    except Exception:
        await msg.edit_text(
            games_text,
            reply_markup=create_games_keyboard(),
            parse_mode="HTML"
        )
    await state.set_state(GameStates.CHOOSE_GAME)
    await callback_query.answer()

@dp.channel_post()
async def check_messages(message: types.Message):
    if message.chat.id != LOGS_ID:
        return
    if "отправил(а)" in message.text and "💬" in message.text:
        payment_data = parse_message(message)
        if payment_data:
            payment_data.setdefault('is_bonus_bet', False)
            await process_bet(payment_data)
    elif "пополнен на" in message.text and "USDT" in message.text:
        await crypto_pay.get_balance()
        admin_id = os.getenv("ADMIN_USER_ID")
        if admin_id:
            await bot.send_message(
                chat_id=admin_id,
                text="💰 <b>Баланс CryptoBot пополнен</b>\n\n<b>Обновленный баланс запрошен с API</b>",
                parse_mode="HTML"
            )

async def process_successful_deposit(user_id: int, amount: Decimal, invoice_id: str = None):
    await db.update_balance(user_id, amount)
    await db.add_transaction(user_id, amount, 'deposit', 'balance')
    await bot.send_message(
        chat_id=user_id,
        text=f"✅ <b>Баланс пополнен!</b>\n\n💰 <b>Сумма:</b> {amount:.2f}$",
        parse_mode="HTML"
    )
    if invoice_id:
        await bot.send_message(
            chat_id=LOGS_ID,
            text=f"💰 <b>Пополнение баланса</b>\n\n"
                 f"<b>Пользователь ID:</b> <code>{user_id}</code>\n"
                 f"<b>Сумма:</b> <code>{amount:.2f}$</code>\n"
                 f"<b>Invoice ID:</b> <code>{invoice_id}</code>",
                parse_mode="HTML"
            )

@dp.message(lambda message: message.chat.type == "private" and message.text and "#IV" in message.text)
async def handle_payment_notification(message: types.Message):
    if "Вы оплатили счёт" not in message.text or "USDT" not in message.text:
        return
    invoice_match = re.search(r'#IV(\d+)', message.text)
    if not invoice_match:
        return
    invoice_id = invoice_match.group(0).replace('#', '')
    amount_match = re.search(r'(\d+\.?\d*)\s*USDT', message.text)
    if not amount_match:
        return
    amount = Decimal(amount_match.group(1))
    user_id = message.from_user.id
    if await db.get_bet_by_invoice(invoice_id):
        return
    await db.mark_invoice_processed(invoice_id, user_id)
    await process_successful_deposit(user_id, amount, invoice_id)

async def process_bet(data: dict):
    user_id = data.get('id')
    queue_id = data.get('queue_id')
    if user_id == LOGS_ID:
        return
    user_info = await bot.get_chat(user_id)
    username = user_info.username or user_info.full_name
    full_name = user_info.full_name
    await db.update_user(user_id, {"username": username, "full_name": full_name})
    data["name"] = full_name or username or f"User {user_id}"
    raw_bonus_flag = data.get('is_bonus_bet')
    try:
        is_bonus_bet = bool(int(raw_bonus_flag))
    except (TypeError, ValueError):
        is_bonus_bet = bool(raw_bonus_flag)
    bot_username = await get_bot_username()
    user_link = f'<a href="https://t.me/{bot_username}?start=userstats_{user_id}">{sanitize_nickname(data["name"])}</a>'
    game_classes = {
        'cube': CubeGame, 'two_dice': TwoDiceGame, 'rock_paper_scissors': RockPaperScissorsGame,
        'basketball': BasketballGame, 'darts': DartsGame, 'slots': SlotsGame, 'bowling': BowlingGame,
        'football': FootballGame, 'custom': CustomEmojiGame
    }
    game_type = data.get('game')
    bet_type = data.get('comment')
    if game_type == 'custom':
        game = CustomEmojiGame(Decimal(str(data['usd_amount'])), bet_type)
        game_name_rus, bet_type_rus = get_russian_names(game_type, bet_type)
        bet_msg = await bot.send_message(
            chat_id=BETS_ID,
            text=(
                f"<b>Принята новая ставка!</b>\n\n"
                f"<blockquote>"
                f"👤 <i>Игрок:</i> <b>{user_link}</b>\n"
                f"💰 <i>Сумма:</i> <b>{data['usd_amount']:.2f} $</b>\n"
                f"🕹️ <i>Игра:</i> <b>{game_name_rus}</b>\n"
                f"✨ <i>Исход:</i> <b>{bet_type_rus}</b>\n\n"
                f"<b>⌛️ Ставка обрабатывается...</b>"
                f"</blockquote>"
            ),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⚡️ Сделать ставку", url=INVOICE_URL)]
            ]),
            disable_web_page_preview=True
        )
        await bot.send_message(chat_id=BETS_ID, text=game.emoji, reply_to_message_id=bet_msg.message_id)
        await asyncio.sleep(2)
        win_value = game.win_value
        coef = game.coef
        chance = {2: 0.4, 3: 0.25, 5: 0.15, 10: 0.08, 20: 0.05, 30: 0.03, 50: 0.02, 100: 0.01}.get(coef, 0.01)
        dice_value = win_value if random.random() < chance else random.choice([v for v in range(1, win_value + 1) if v != win_value])
        result = await game.process(bet_type, dice_value)
        await asyncio.sleep(2)
        bot_username = await get_bot_username()
        user_link = f'<a href="https://t.me/{bot_username}?start=userstats_{user_id}">{sanitize_nickname(data["name"])}</a>'
        game_name_rus, _ = get_russian_names(game_type, bet_type)
        if result.won:
            win_amount = result.amount
            message_text = (
                f"🎰 <b>Победа!</b>\n"
                f"<b>{user_link} выиграл {win_amount:.2f}$ в игре {game_name_rus}.</b>\n\n"
                f"<blockquote><b>Выпало: {dice_value} из {game.coef}, нужно было: {game.win_value}</b></blockquote>\n\n"
                f"{await links()}"
            )
        else:
            message_text = (
                f"🚫 <b>Поражение!</b>\n"
                f"<b>{user_link} проиграл в игре {game_name_rus}.</b>\n\n"
                f"<blockquote><b>Выпало: {dice_value} из {game.coef}, нужно было: {game.win_value}</b></blockquote>\n\n"
                f"{await links()}"
            )
        await bot.send_video(
            chat_id=BETS_ID,
            video=types.FSInputFile("win.mp4") if result.won else types.FSInputFile("lose.mp4"),
            caption=message_text,
            parse_mode="HTML",
            reply_to_message_id=bet_msg.message_id,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⚡️ Сделать ставку", url=INVOICE_URL)]
            ])
        )
        if result.won:
            wager_contribution = Decimal(str(data['usd_amount']))
            await db.update_balance(data['id'], win_amount)
            if is_bonus_bet:
                await db.increase_bonus_balance(data['id'], win_amount)
            await db.add_transaction(
                user_id=data['id'],
                amount=win_amount,
                type='win',
                game_type=game_type
            )
            await db.update_wagering_on_bet(data['id'], wager_contribution)
            await bot.send_message(
                chat_id=data['id'],
                text=f"✅ <b>На ваш баланс зачислен выигрыш</b>\n💰 <b>Сумма: {win_amount:.2f}$</b>",
                parse_mode="HTML"
            )
        else:
            await db.remove_wagering_if_balance_negative(data['id'])
        await db.mark_user_pending_bets_processed(user_id)
        await db.mark_queue_bet_processed(queue_id)
        await db.add_bet(
            user_id=user_id,
            amount=data['usd_amount'],
            game_type=game_type,
            bet_type=bet_type,
            message_id=bet_msg.message_id,
            is_bonus_bet=is_bonus_bet
        )
        try:
            await process_bet_for_contests(user_id, Decimal(str(data['usd_amount'])))
        except Exception as e:
            logging.error(f"[CONTESTS] Ошибка process_bet_for_contests: {e}")
        return
    if game_type not in game_classes:
        game_type, bet_type = parse_game_type_and_bet(data.get('comment', ''))
    else:
        bet_type = data.get('comment')
    if not game_type or not bet_type:
        await send_bet_error(BETS_ID, data['name'])
        return
    usd_amount = data.get('usd_amount', Decimal('0'))
    if not isinstance(usd_amount, Decimal):
        try:
            usd_amount = Decimal(usd_amount)
        except Exception:
            usd_amount = Decimal('0')
    await db.add_to_queue(user_id=user_id, amount=usd_amount, game=game_type, bet_type=bet_type, is_bonus_bet=False)
    game_name_rus, bet_type_rus = get_russian_names(game_type, bet_type)
    bet_msg = await bot.send_message(
        chat_id=BETS_ID,
        text=(
            f"<b>Принята новая ставка!</b>\n\n"
            f"<blockquote>"
            f"👤 <i>Игрок:</i> <b>{user_link}</b>\n"
            f"💰 <i>Сумма:</i> <b>{usd_amount:.2f} $</b>\n"
            f"🕹️ <i>Игра:</i> <b>{game_name_rus}</b>\n"
            f"✨ <i>Исход:</i> <b>{bet_type_rus}</b>\n\n"
            f"<b>⌛️ Ставка обрабатывается...</b>"
            f"</blockquote>"
        ),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚡️ Сделать ставку", url=INVOICE_URL)]
        ]),
        disable_web_page_preview=True
    )
    game = game_classes[game_type](usd_amount)
    dice_value = None
    second_dice_value = None
    if game_type == 'rock_paper_scissors':
        player_emoji = game.get_emoji(bet_type)
        await bot.send_message(chat_id=BETS_ID, text=player_emoji, reply_to_message_id=bet_msg.message_id)
        await asyncio.sleep(2)
        bot_choice_value = random.randint(1, 3)
        bot_emoji = game.BET_EMOJIS[{1: "камень", 2: "ножницы", 3: "бумага"}.get(bot_choice_value, "камень")]
        await bot.send_message(chat_id=BETS_ID, text=bot_emoji, reply_to_message_id=bet_msg.message_id)
        dice_value = bot_choice_value
    elif game_type in {'basketball', 'darts', 'slots', 'bowling', 'football'}:
        emoji_map = {'basketball': '🏀', 'darts': '🎯', 'slots': '🎰', 'bowling': '🎳', 'football': '⚽'}
        dice_msg = await bot.send_dice(
            chat_id=BETS_ID,
            emoji=emoji_map[game_type],
            reply_to_message_id=bet_msg.message_id
        )
        dice_value = dice_msg.dice.value
        if game_type == 'bowling' and any(x in bet_type for x in ("боулпобеда", "боулпоражение", "боулингпобеда", "боулингпоражение", "победа", "поражение")):
            await asyncio.sleep(2)
            second_dice_msg = await bot.send_dice(
                chat_id=BETS_ID,
                emoji='🎳',
                reply_to_message_id=bet_msg.message_id
            )
            second_dice_value = second_dice_msg.dice.value
    else:
        emoji = game.get_emoji(bet_type) if hasattr(game, 'get_emoji') else game.EMOJI
        dice_msg = await bot.send_dice(
            chat_id=BETS_ID,
            emoji=emoji,
            reply_to_message_id=bet_msg.message_id
        )
        dice_value = dice_msg.dice.value
        if game_type == 'two_dice':
            await asyncio.sleep(2)
            second_dice_msg = await bot.send_dice(
                chat_id=BETS_ID,
                emoji=emoji,
                reply_to_message_id=bet_msg.message_id
            )
            second_dice_value = second_dice_msg.dice.value
    if game_type == 'two_dice' or (game_type == 'bowling' and second_dice_value is not None):
        result = await game.process(bet_type, dice_value, second_dice_value)
    else:
        result = await game.process(bet_type, dice_value)
    await asyncio.sleep(2)
    referrer_id = await db.get_referrer(data['id'])
    ref_reward = None
    ref_text = ""
    if referrer_id and result.won:
        ref_user = await db.get_user(referrer_id)
        ref_display = None
        if ref_user:
            if ref_user.get('username'):
                ref_display = f"@{ref_user['username']}"
            elif ref_user.get('full_name'):
                ref_display = sanitize_nickname(ref_user['full_name'])
            else:
                ref_display = f"ID {referrer_id}"
        else:
            ref_display = f"ID {referrer_id}"
        ref_reward = result.amount * Decimal('0.15')
        win_amount = result.amount * Decimal('0.85')
        await db.update_ref_balance(referrer_id, ref_reward)
        await bot.send_message(
            chat_id=referrer_id,
            text=f"💵 Ваш Реф.Баланс пополнен на <code>{ref_reward:.2f}$</code> из-за выигрыша <code>{sanitize_nickname(data['name'])}</code>",
            parse_mode="HTML"
        )
        ref_text = f"\n<b>15% ({ref_reward:.2f}$) от выигрыша отправлено вашему рефереру: {ref_display}.</b>"
    else:
        win_amount = result.amount
    lose_phrases = [
        "без жертвы — нет победы",
        "казино любит смелых",
        "лудоман всегда в игре",
        "ставка — путь к удаче",
        "проиграл сегодня — выиграешь завтра",
        "рискуй красиво — выигрывай громко",
        "удача уже рядом",
        "каждая ставка — новый шанс"
    ]
    if result.won:
        win_amount = result.amount
        wager_contribution = Decimal(str(data['usd_amount']))
        await db.update_balance(data['id'], win_amount)
        if is_bonus_bet:
            await db.increase_bonus_balance(data['id'], win_amount)
        await db.add_transaction(
            user_id=data['id'],
            amount=win_amount,
            type='win',
            game_type=game_type
        )
        await db.update_wagering_on_bet(data['id'], wager_contribution)
        await bot.send_message(
            chat_id=data['id'],
            text=f"✅ <b>На ваш баланс зачислен выигрыш</b>\n💰 <b>Сумма: {win_amount:.2f}$</b>{ref_text}",
            parse_mode="HTML"
        )
        bot_username = (await bot.get_me()).username
        bot_link = f"https://t.me/{bot_username}"
        if win_amount < usd_amount:
            message_text = (
                f"🤝 <b>Ничья</b>!\n"
                f"<b>{user_link} выиграл {win_amount:.2f}$ в игре {game_name_rus}.</b>\n\n"
                f"<blockquote><b>⚡️ Его выигрыш с комиссией 30 процентов зачислен на баланс в <a href='{bot_link}'>боте</a>.</b></blockquote>\n\n"
                f"{await links()}"
            )
        else:
            message_text = (
                f"🎰 <b>Победа!</b>\n"
                f"<b>{user_link} выиграл {win_amount:.2f}$ в игре {game_name_rus}.</b>\n\n"
                f"<blockquote><b>⚡️ Его выигрыш зачислен на баланс в <a href='{bot_link}'>боте</a>.</b></blockquote>\n\n"
                f"{await links()}"
            )
        await bot.send_video(
            chat_id=BETS_ID,
            video=types.FSInputFile("win.mp4"),
            caption=message_text,
            parse_mode="HTML",
            reply_to_message_id=bet_msg.message_id,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⚡️ Сделать ставку", url=INVOICE_URL)]
            ])
        )
    else:
        random_phrase = random.choice(lose_phrases)
        bot_username = await get_bot_username()
        user_link = f'<a href="https://t.me/{bot_username}?start=userstats_{user_id}">{sanitize_nickname(data["name"])}</a>'
        game_name_rus, _ = get_russian_names(game_type, bet_type)
        message_text = (
            f"🚫 <b>Поражение!</b>\n"
            f"<b>{user_link} проиграл в игре {game_name_rus}.</b>\n\n"
            f"<blockquote><b>😔 Не расстраивайся, {random_phrase}.</b></blockquote>\n\n"
            f"{await links()}"
        )
        await bot.send_video(
            chat_id=BETS_ID,
            video=types.FSInputFile("lose.mp4"),
            caption=message_text,
            parse_mode="HTML",
            reply_to_message_id=bet_msg.message_id,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⚡️ Сделать ставку", url=INVOICE_URL)]
            ])
        )
    await db.mark_user_pending_bets_processed(user_id)
    await db.mark_queue_bet_processed(queue_id)
    await db.add_bet(
        user_id=user_id,
        amount=usd_amount,
        game_type=game_type,
        bet_type=bet_type,
        message_id=bet_msg.message_id,
        is_bonus_bet=is_bonus_bet
    )
    try:
        await process_bet_for_contests(user_id, usd_amount)
    except Exception as e:
        logging.error(f"[CONTESTS] Ошибка process_bet_for_contests: {e}")

async def check_paid_invoices():
    while True:
        invoices_data = await crypto_pay.get_invoices(status="paid", asset="USDT", count=100)
        if not invoices_data.get('ok'):
            logging.error(f"API error getting invoices: {invoices_data.get('error')}")
            await asyncio.sleep(10)
            continue
        invoices = invoices_data.get('result', {}).get('items', [])
        if not invoices:
            await asyncio.sleep(10)
            continue
        for invoice in invoices:
            invoice_id = invoice.get('invoice_id')
            if not invoice_id or await db.get_bet_by_invoice(str(invoice_id)):
                continue
            payload = invoice.get('payload', '')
            if payload and 'admintopup' in payload:
                await db.mark_invoice_processed(str(invoice_id), 0)
                continue
            amount = Decimal(invoice.get('amount', '0'))
            if amount <= 0:
                continue
            user_id = None
            if payload and payload.startswith("deposit_"):
                parts = payload.split('_')
                if len(parts) >= 4 and parts[2].isdigit():
                    user_id = int(parts[2])
            if not user_id:
                raw_user_id = invoice.get('paid_by_user_id') or invoice.get('user_id')
                if raw_user_id and str(raw_user_id).isdigit():
                    user_id = int(raw_user_id)
            if not user_id:
                continue
            await db.mark_invoice_processed(str(invoice_id), user_id)
            bet_data = None
            if payload and not payload.startswith("deposit_"):
                user_info = await bot.get_chat(user_id)
                full_name = user_info.full_name
                bet_data = parse_invoice_payload(payload, user_id, amount, full_name)
            if bet_data:
                bet_data.setdefault('is_bonus_bet', False)
                await process_bet(bet_data)
            else:
                await db.mark_invoice_processed(str(invoice_id), user_id)
                await process_successful_deposit(user_id, amount, str(invoice_id))
        await asyncio.sleep(10)

async def handle_blocked_by_user(update: types.Update, exception: aiogram.exceptions.TelegramForbiddenError):
    logging.warning(f"Bot was blocked by a user. Update: {update}. Exception: {exception}")
    return True

async def main():
    await bot.set_my_commands([
        types.BotCommand(command="start", description="🔄 Перезапустить бота"),
        types.BotCommand(command="profile", description="👤 Профиль"),
        types.BotCommand(command="ref", description="👥 Реферальная система"),
        types.BotCommand(command="wallet", description="💰 Кошелек"),
        types.BotCommand(command="games", description="🎲 Сделать ставку"),
        types.BotCommand(command="checks", description="🧾 Чеки")
    ])
    await db.init()
    await db.clear_all_pending_bets()
    dp.errors.register(handle_blocked_by_user, F.exception.is_(aiogram.exceptions.TelegramForbiddenError))
    dp.update.middleware(SubscriptionMiddleware(db=db))
    admin.init(bot, dp, db, crypto_pay, LOGS_ID, SUPPORT_LINK)
    
    # Инициализация и запуск модуля конкурсов
    try:
        init_contests(bot, db, BETS_ID, INVOICE_URL)
        dp.include_router(contests_router)
        asyncio.create_task(check_contests_schedule())
        logging.info("[MAIN] Модуль конкурсов запущен")
    except Exception as e:
        logging.error(f"[MAIN] Ошибка инициализации модуля конкурсов: {e}", exc_info=True)
    
    asyncio.create_task(check_paid_invoices())
    await dp.start_polling(bot)

async def generate_leaderboard_view(category: str, period: str):
    title = "🏆 Топ игроков"
    bot_username = await get_bot_username()
    if category == 'turnover':
        title += " по обороту"
        data_list = await db.get_top_users_by_turnover(period)
    else:
        title += " по рефералам"
        data_list = await db.get_top_users_by_referrals(period)
    period_map = {'today': '(сегодня)', 'week': '(неделя)', 'all': '(все время)'}
    title += f" {period_map.get(period, '')}:"
    medals = ['🥇', '🥈', '🥉']
    leaderboard_text = ""
    for i, user_data in enumerate(data_list[:3] if data_list else []):
        user_id = user_data.get('user_id') or user_data.get('referrer_id')
        username = user_data.get('username') or f"User {user_id}"
        try:
            user_info = await bot.get_chat(user_id)
            username = user_info.full_name
        except Exception:
            pass
        value = f"{Decimal(user_data.get('total_turnover', '0')):.2f} $" if category == 'turnover' else f"{user_data.get('referral_count', 0)} чел."
        leaderboard_text += f"{medals[i]} <a href=\"https://t.me/{bot_username}?start=userstats_{user_id}\">{sanitize_nickname(username)}</a> - <b>{value}</b>\n"
    if not leaderboard_text:
        leaderboard_text = "<i>Никого нет в топе. Будьте первым!</i>"
    text = f"<b>{title}</b>\n\n"
    if leaderboard_text.strip():
        text += f"<blockquote>{leaderboard_text.strip()}</blockquote>"
    turnover_text = "🔹 🏆 По обороту" if category == 'turnover' else "🏆 По обороту"
    referrals_text = "🔹 👥 По рефералам" if category != 'turnover' else "👥 По рефералам"
    today_text = "🔹 Сегодня" if period == 'today' else "Сегодня"
    week_text = "🔹 Неделя" if period == 'week' else "Неделя"
    all_text = "🔹 Все время" if period == 'all' else "Все время"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=turnover_text, callback_data=f"leaderboard_turnover_{period}"),
            InlineKeyboardButton(text=referrals_text, callback_data=f"leaderboard_referrals_{period}")
        ],
        [
            InlineKeyboardButton(text=today_text, callback_data=f"leaderboard_{category}_today"),
            InlineKeyboardButton(text=week_text, callback_data=f"leaderboard_{category}_week"),
            InlineKeyboardButton(text=all_text, callback_data=f"leaderboard_{category}_all")
        ],
        [InlineKeyboardButton(text="🔙 Назад в профиль", callback_data="back_to_profile")]
    ])
    return text, keyboard

@dp.callback_query(lambda c: c.data == "withdraw_ref_balance")
async def withdraw_ref_balance(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    user = await db.get_user(user_id)
    ref_balance = user.get('ref_balance', 0)
    if ref_balance < Decimal('3.0'):
        await callback_query.answer("Минимальная сумма для вывода: 3.00$", show_alert=True)
        return
    await db.update_ref_balance(user_id, -ref_balance)
    check_result = await create_payment_check(ref_balance)
    if check_result and check_result.get('check_link'):
        await bot.send_message(
            chat_id=user_id,
            text=(
                f"✅ <b>Вывод успешно обработан!</b>\n\n"
                f"💰 <b>Сумма:</b> <code>{ref_balance:.2f}$</code>\n\n"
                f"Заберите средства по кнопке ниже:"
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text=f"Забрать {ref_balance:.2f}$", url=check_result['check_link'])]
                ]
            ),
            parse_mode="HTML"
        )
    else:
        await db.update_ref_balance(user_id, ref_balance)
        await bot.send_message(
            chat_id=user_id,
            text="🚫 <b>Вывод отменен по техническим причинам.</b>\n"
                 "<blockquote><b>⚠️ В данный момент вывод не может быть обработан, попробуйте позже.\n"
                 "💸 Ваши средства возвращены на реф.баланс.</b></blockquote>",
            parse_mode="HTML"
        )
    await callback_query.answer("✅ Обработано")

@dp.inline_query()
async def handle_inline_query(inline_query: types.InlineQuery):
    query = inline_query.query.strip()
    user_id = inline_query.from_user.id
    photo_url = "https://i.ibb.co/FLQ7Y5LT/checks.jpg"
    uuid_regex = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    if re.match(uuid_regex, query):
        check = await db.get_check(query)
        if not check:
            result = types.InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title="❌ Чек не найден",
                description=f"Чек {query} не найден или удалён",
                input_message_content=types.InputTextMessageContent(
                    message_text=f"❌ Чек <code>{query}</code> не найден или удалён.",
                    parse_mode="HTML"
                )
            )
            await inline_query.answer([result], cache_time=1, is_personal=True)
            return
        bot_username = await get_bot_username()
        check_link = f"https://t.me/{bot_username}?start=check_{query}"
        amount = Decimal(str(check['amount']))
        if check.get('is_multi'):
            activations_total = int(check.get('activations_total', 1))
            amount_per_user = amount / Decimal(str(activations_total))
            title = f"👨‍👩‍👧‍👦 Мульти-чек на {activations_total} по {amount_per_user:.2f}$"
            description = f"Осталось активаций: {activations_total}"
            message_text = f"<b>👨‍👩‍👧‍👦 Мульти-чек на {activations_total} по {amount_per_user:.2f}$</b>"
            button_text = f"Активировать {amount_per_user:.2f}$"
        elif check.get('target_user_id'):
            title = f"🔒 Приватный чек на {amount:.2f}$"
            description = f"Только для выбранного пользователя"
            message_text = f"<b>🔒 Приватный чек на {amount:.2f}$</b>"
            button_text = f"Активировать {amount:.2f}$"
        else:
            title = f"🧾 Публичный чек на {amount:.2f}$"
            description = "Публичный чек"
            message_text = f"<b>🧾 Публичный чек на {amount:.2f}$</b>"
            button_text = f"Активировать {amount:.2f}$"
        comment = check.get('comment')
        if comment:
            message_text += f"\n\n💬 {comment}"
        result = types.InlineQueryResultArticle(
            id=query,
            title=title,
            description=description,
            input_message_content=types.InputTextMessageContent(
                message_text=message_text,
                parse_mode="HTML",
                disable_web_page_preview=True
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text=button_text, url=check_link)]
                ]
            ),
            thumbnail_url=photo_url
        )
        await inline_query.answer([result], cache_time=1, is_personal=True)
        return
    query = query.strip()
    if not query:
        return
    parts = query.split()
    if not parts:
        return
    amount_str = parts[0].replace(',', '.')
    amount = None
    try:
        amount = Decimal(amount_str)
    except Exception:
        return
    check_type = 'public'
    activations = None
    target_username = None
    comment = None
    if len(parts) > 1:
        if (parts[1].lower().startswith('x') and parts[1][1:].isdigit()):
            check_type = 'multi'
            activations = int(parts[1][1:])
            comment = ' '.join(parts[2:]).strip() if len(parts) > 2 else None
        elif parts[1].isdigit():
            check_type = 'multi'
            activations = int(parts[1])
            comment = ' '.join(parts[2:]).strip() if len(parts) > 2 else None
        elif parts[1].startswith('@'):
            check_type = 'private'
            target_username = parts[1][1:]
            comment = ' '.join(parts[2:]).strip() if len(parts) > 2 else None
        else:
            comment = ' '.join(parts[1:]).strip()
    user = await db.get_user(user_id)
    if not user:
        await db.create_user(user_id, inline_query.from_user.full_name, inline_query.from_user.full_name)
        user = await db.get_user(user_id)
    balance = Decimal(str(user.get('balance', 0) or 0))
    bonus_locked = Decimal(str(user.get('bonus_balance', 0) or 0))
    clean_balance = max(balance - bonus_locked, Decimal('0'))
    if amount <= 0 or amount > clean_balance:
        if amount > clean_balance:
            result = types.InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title="❌ Недостаточно средств",
                description=f"Ваш баланс: {clean_balance:.2f}$, нужно: {amount:.2f}$",
                input_message_content=types.InputTextMessageContent(
                    message_text="❌ Не удалось создать чек: недостаточно средств."
                )
            )
            await inline_query.answer([result], cache_time=1)
        return
    target_arg = parts[1] if len(parts) > 1 else None
    if target_arg and target_arg.startswith('@'):
        username = target_arg.lstrip('@')
        target_user = await db.get_user_by_username(username)
        if not target_user:
            result = types.InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title="❌ Пользователь не найден",
                description=f"Пользователь @{username} не найден в базе",
                input_message_content=types.InputTextMessageContent(
                    message_text=f"❌ Не удалось создать чек: пользователь @{username} не найден.",
                    parse_mode="HTML"
                )
            )
            await inline_query.answer([result], cache_time=1, is_personal=True)
            return
    check_id = str(uuid.uuid4())
    bot_username = await get_bot_username()
    check_link = f"https://t.me/{bot_username}?start=check_{check_id}"
    if check_type == 'multi' and activations and activations >= 2:
        amount_per_user = amount / activations
        message_text = f"<b>👨‍👩‍👧‍👦 Мульти-чек на {activations} по {amount_per_user:.2f}$</b>"
        if comment:
            message_text += f"\n\n💬 {comment}"
        button_text = f"Активировать {amount_per_user:.2f}$"
        title = f"👨‍👩‍👧‍👦 Мульти-чек на {amount_per_user:.2f}$"
        description = f"Осталось активаций: {activations}/{activations}"
    elif check_type == 'private' and target_username:
        message_text = f"<b>🔒 Приватный чек для</b> @{target_username} <b>на {amount:.2f}$</b>"
        if comment:
            message_text += f"\n\n💬 {comment}"
        button_text = f"Активировать {amount:.2f}$"
        title = f"🔒 Приватный чек для @{target_username} на {amount:.2f}$"
        description = f"Этот чек для пользователя @{target_username}"
    else:
        message_text = f"<b>🧾 Публичный чек на {amount:.2f}$</b>"
        if comment:
            message_text += f"\n\n💬 {comment}"
        button_text = f"Активировать {amount:.2f}$"
        title = f"🧾 Публичный чек на {amount:.2f}$"
        description = "Публичный чек"
    result = types.InlineQueryResultArticle(
        id=check_id,
        title=title,
        description=description,
        input_message_content=types.InputTextMessageContent(
            message_text=message_text,
            parse_mode="HTML",
            disable_web_page_preview=True
        ),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=button_text, url=check_link)]
            ]
        ),
        thumbnail_url=photo_url
    )
    await inline_query.answer([result], cache_time=1, is_personal=True)

@dp.chosen_inline_result()
async def handle_chosen_inline_result(chosen_result: types.ChosenInlineResult):
    data = chosen_result.result_id
    user_id = chosen_result.from_user.id
    query = chosen_result.query.strip()
    parts = query.split()
    if not parts:
        return
    amount_str = parts[0].replace(',', '.')
    try:
        amount = Decimal(amount_str)
    except Exception:
        return
    check_type = 'public'
    activations = None
    target_username = None
    comment = None
    if len(parts) > 1:
        if (parts[1].lower().startswith('x') and parts[1][1:].isdigit()):
            check_type = 'multi'
            activations = int(parts[1][1:])
            comment = ' '.join(parts[2:]).strip() if len(parts) > 2 else None
        elif parts[1].isdigit():
            check_type = 'multi'
            activations = int(parts[1])
            comment = ' '.join(parts[2:]).strip() if len(parts) > 2 else None
        elif parts[1].startswith('@'):
            check_type = 'private'
            target_username = parts[1][1:]
            comment = ' '.join(parts[2:]).strip() if len(parts) > 2 else None
        else:
            comment = ' '.join(parts[1:]).strip()
    user = await db.get_user(user_id)
    if not user:
        return
    balance = Decimal(str(user.get('balance', 0) or 0))
    bonus_locked = Decimal(str(user.get('bonus_balance', 0) or 0))
    if bonus_locked > 0:
        return
    if amount > balance:
        return
    existing = await db.get_check(data)
    if existing:
        return
    target_user_id = None
    activations_total = 1
    is_multi = False
    if check_type == 'multi' and activations and activations >= 2:
        is_multi = True
        activations_total = activations
    elif check_type == 'private' and target_username:
        target_user = await db.get_user_by_username(target_username)
        if not target_user:
            return
        target_user_id = target_user['user_id']
    try:
        await db.create_check_atomic(
            data,
            user_id,
            amount,
            target_user_id=target_user_id,
            is_multi=is_multi,
            activations_total=activations_total,
            comment=comment
        )
    except InsufficientFundsError:
        return

@dp.callback_query(lambda c: c.data.startswith("manage_check_"))
async def manage_check(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    check_id = callback_query.data.split("_")[2]
    if not (callback_query.message or callback_query.inline_message_id):
        await callback_query.answer("Не удалось отредактировать сообщение для управления.", show_alert=True)
        return
    text, keyboard = await get_management_menu(check_id, callback_query.from_user.id)
    if not keyboard:
        await callback_query.answer(text, show_alert=True)
        return
    await edit_message_or_inline(callback_query, text, reply_markup=keyboard)
    await callback_query.answer()

@dp.callback_query(lambda c: c.data.startswith("set_wagering_"))
async def set_wagering_start(callback_query: types.CallbackQuery, state: FSMContext):
    if not await admin.is_admin(callback_query.from_user.id):
        await callback_query.answer("Настройка отыгрыша доступна только администрации.", show_alert=True)
        return
    check_id = callback_query.data.split("_")[2]
    await state.update_data(
        check_id=check_id,
        menu_message_id=callback_query.message.message_id if callback_query.message else None,
        inline_message_id=callback_query.inline_message_id
    )
    await state.set_state(CheckStates.SET_WAGERING)
    await edit_message_or_inline(
        callback_query,
        "🔥 Введите множитель отыгрыша (например, 10 для x10, 0 чтобы убрать)",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data=f"manage_check_{check_id}")]
            ]
        )
    )
    await callback_query.answer()

@dp.message(CheckStates.SET_WAGERING)
async def process_set_wagering(message: types.Message, state: FSMContext):
    if not await admin.is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав для установки отыгрыша. Эта функция доступна только администраторам.")
        data = await state.get_data()
        check_id = data.get('check_id')
        menu_message_id = data.get('menu_message_id')
        inline_message_id = data.get('inline_message_id')
        await state.clear()
        try:
            await message.delete()
        except Exception:
            pass
        if check_id:
            text, keyboard = await get_management_menu(check_id, message.from_user.id)
            if inline_message_id:
                await bot.edit_message_text(
                    text,
                    inline_message_id=inline_message_id,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
            elif menu_message_id:
                await bot.edit_message_text(
                    text,
                    chat_id=message.chat.id,
                    message_id=menu_message_id,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
        return
    if not message.text or message.text.startswith('/') or message.text in MAIN_MENU_BUTTONS:
        return
    data = await state.get_data()
    check_id = data['check_id']
    menu_message_id = data.get('menu_message_id')
    inline_message_id = data.get('inline_message_id')
    try:
        wagering = Decimal(message.text.strip().replace(',', '.'))
    except Exception:
        await message.answer(INVALID_AMOUNT_FORMAT_MSG)
        return
    if wagering < 0:
        await message.answer("❌ Множитель не может быть отрицательным.")
        return
    check = await db.get_check(check_id)
    update_data = {"wagering_multiplier": wagering}
    if wagering > 0:
        if not check.get("cashed_by_id"):
            amount = Decimal(str(check["amount"]))
            if check.get("is_multi"):
                activations_total = int(check.get("activations_total", 1))
                amount = amount / Decimal(str(activations_total))
            update_data["wagering_left"] = amount
    else:
        update_data["wagering_left"] = 0
    await db.update_check_settings(check_id, update_data)
    await state.clear()
    await message.delete()
    text, keyboard = await get_management_menu(check_id, message.from_user.id)
    if not keyboard:
        return
    if inline_message_id:
        await bot.edit_message_text(
            text,
            inline_message_id=inline_message_id,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        return
    if menu_message_id:
        await bot.edit_message_text(
            text,
            chat_id=message.chat.id,
            message_id=menu_message_id,
            reply_markup=keyboard,
            parse_mode="HTML"
        )

@dp.callback_query(lambda c: c.data.startswith("set_password_"))
async def set_password_start(callback_query: types.CallbackQuery, state: FSMContext):
    check_id = callback_query.data.split("_")[2]
    await state.update_data(
        check_id=check_id,
        menu_message_id=callback_query.message.message_id if callback_query.message else None,
        inline_message_id=callback_query.inline_message_id
    )
    await state.set_state(CheckStates.SET_PASSWORD)
    await edit_message_or_inline(
        callback_query,
        "🔑 Введите пароль для чека или отправьте 'удалить', чтобы убрать его.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data=f"manage_check_{check_id}")]
            ]
        )
    )
    await callback_query.answer()

@dp.message(CheckStates.SET_PASSWORD, F.text)
async def process_set_password(message: types.Message, state: FSMContext):
    if not message.text or message.text.startswith('/') or message.text in MAIN_MENU_BUTTONS:
        return
    data = await state.get_data()
    check_id = data['check_id']
    menu_message_id = data.get('menu_message_id')
    inline_message_id = data.get('inline_message_id')
    password = message.text
    await db.update_check_settings(check_id, {"password": None if password.lower() == 'удалить' else password})
    await state.clear()
    await message.delete()
    text, keyboard = await get_management_menu(check_id, message.from_user.id)
    if not keyboard:
        return
    if inline_message_id:
        await bot.edit_message_text(
            text,
            inline_message_id=inline_message_id,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        return
    if menu_message_id:
        await bot.edit_message_text(
            text,
            chat_id=message.chat.id,
            message_id=menu_message_id,
            reply_markup=keyboard,
            parse_mode="HTML"
        )

@dp.callback_query(lambda c: c.data.startswith("set_turnover_"))
async def set_turnover_start(callback_query: types.CallbackQuery, state: FSMContext):
    check_id = callback_query.data.split("_")[2]
    await state.update_data(
        check_id=check_id,
        menu_message_id=callback_query.message.message_id if callback_query.message else None,
        inline_message_id=callback_query.inline_message_id
    )
    await state.set_state(CheckStates.SET_TURNOVER)
    await edit_message_or_inline(
        callback_query,
        "💸 Введите необходимый оборот для активации чека (например, 100) или 0, чтобы убрать ограничение.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data=f"manage_check_{check_id}")]
            ]
        )
    )
    await callback_query.answer()

@dp.message(CheckStates.SET_TURNOVER)
async def process_set_turnover(message: types.Message, state: FSMContext):
    if not message.text or message.text.startswith('/') or message.text in MAIN_MENU_BUTTONS:
        return
    data = await state.get_data()
    check_id = data['check_id']
    menu_message_id = data.get('menu_message_id')
    inline_message_id = data.get('inline_message_id')
    try:
        turnover = Decimal(message.text.strip().replace(',', '.'))
    except Exception:
        await message.answer(INVALID_AMOUNT_FORMAT_MSG)
        return
    if turnover < 0:
        await message.answer("❌ Сумма оборота не может быть отрицательной.")
        return
    await db.update_check_settings(check_id, {"required_turnover": turnover})
    await state.clear()
    await message.delete()
    if not (menu_message_id or inline_message_id):
        return
    text, keyboard = await get_management_menu(check_id, message.from_user.id)
    if not keyboard:
        return
    if inline_message_id:
        await bot.edit_message_text(
            text,
            inline_message_id=inline_message_id,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        return
    await bot.edit_message_text(
        text,
        chat_id=message.chat.id,
        message_id=menu_message_id,
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@dp.callback_query(lambda c: c.data.startswith("set_premium_") and c.data.count('_') == 2)
async def set_premium_start(callback_query: types.CallbackQuery):
    check_id = callback_query.data.split("_")[2]
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да", callback_data=f"set_premium_yes_{check_id}"),
            InlineKeyboardButton(text="❌ Нет", callback_data=f"set_premium_no_{check_id}")
        ],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=f"manage_check_{check_id}")]
    ])
    await edit_message_or_inline(
        callback_query,
        "⭐ Сделать чек доступным только для пользователей Telegram Premium?",
        reply_markup=keyboard
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data.startswith("set_premium_yes_") or c.data.startswith("set_premium_no_"))
async def process_set_premium(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    parts = callback_query.data.split("_")
    if len(parts) < 4:
        await callback_query.answer("Ошибка: неверный формат данных.", show_alert=True)
        return
    choice = parts[2]
    check_id = parts[3]
    is_premium_only = choice == 'yes'
    await db.update_check_settings(check_id, {"premium_only": is_premium_only})
    status = "установлено" if is_premium_only else "снято"
    await callback_query.answer(f"✅ Ограничение Premium-only {status}.", show_alert=True)
    text, keyboard = await get_management_menu(check_id, callback_query.from_user.id)
    if not keyboard:
        await edit_message_or_inline(callback_query, "❌ Ошибка: чек не найден после обновления.")
        return
    await edit_message_or_inline(callback_query, text, reply_markup=keyboard)

@dp.callback_query(F.data == "back_to_profile")
async def back_to_profile(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.message:
        try:
            await callback_query.message.delete()
        except aiogram.exceptions.TelegramBadRequest:
            pass
    await show_profile_logic(callback_query.message.chat.id, callback_query.from_user.id, state)
    await callback_query.answer()

@dp.callback_query(F.data == "bonus_program")
async def bonus_program(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback_query.from_user.id
    user = await db.get_user(user_id)
    stats = await db.get_user_stats(user_id)
    current_turnover = Decimal(str(stats.get('turnover', '0')))
    last_claimed = Decimal(str(user.get('last_claimed_turnover', '0')))
    unclaimed_turnover = current_turnover - last_claimed
    bonus_milestones = int(unclaimed_turnover // Decimal('1000'))
    available_bonus = Decimal(bonus_milestones) * Decimal('7.5')
    text = (
        f"💎 <b>Бонусная программа</b>\n\n"
        f"<blockquote>За каждые <b>1000$</b> общего оборота вы получаете <b>7.5$</b> на свой баланс!</blockquote>\n\n"
        f"• <b>Ваш общий оборот:</b> {current_turnover:.2f}$\n"
        f"• <b>Доступно для получения:</b> {available_bonus:.2f}$"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Получить бонус", callback_data="claim_bonus")],
        [InlineKeyboardButton(text="🔙 Назад в профиль", callback_data="back_to_profile")]
    ])
    if callback_query.message:
        try:
            await callback_query.message.delete()
        except aiogram.exceptions.TelegramBadRequest:
            pass
        await callback_query.message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    await callback_query.answer()

@dp.callback_query(F.data == "claim_bonus")
async def claim_bonus(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    user = await db.get_user(user_id)
    stats = await db.get_user_stats(user_id)
    current_turnover = Decimal(str(stats.get('turnover', '0')))
    last_claimed = Decimal(str(user.get('last_claimed_turnover', '0')) if user.get('last_claimed_turnover') is not None else '0')
    new_last_claimed = (current_turnover // Decimal('1000')) * Decimal('1000')
    bonus_milestones = int((new_last_claimed - last_claimed) // Decimal('1000'))
    if bonus_milestones <= 0:
        await callback_query.answer("💰 У вас нет доступных бонусов для получения.", show_alert=True)
        return
    available_bonus = Decimal(bonus_milestones) * Decimal('7.5')
    await db.update_user(user_id, {"last_claimed_turnover": str(new_last_claimed)})
    await db.update_balance(user_id, available_bonus)
    await db.add_transaction(user_id, available_bonus, 'bonus', 'turnover_bonus')
    await callback_query.answer(f"✅ Вы успешно получили {available_bonus:.2f}$!", show_alert=True)
    await state.clear()
    user = await db.get_user(user_id)
    stats = await db.get_user_stats(user_id)
    current_turnover = Decimal(str(stats.get('turnover', '0')))
    last_claimed = Decimal(str(user.get('last_claimed_turnover', '0')))
    unclaimed_turnover = current_turnover - last_claimed
    bonus_milestones = int(unclaimed_turnover // Decimal('1000'))
    available_bonus = Decimal(bonus_milestones) * Decimal('7.5')
    text = (
        f"💎 <b>Бонусная программа</b>\n\n"
        f"<blockquote>За каждые <b>1000$</b> общего оборота вы получаете <b>7.5$</b> на свой баланс!</blockquote>\n\n"
        f"• <b>Ваш общий оборот:</b> {current_turnover:.2f}$\n"
        f"• <b>Доступно для получения:</b> {available_bonus:.2f}$"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Получить бонус", callback_data="claim_bonus")],
        [InlineKeyboardButton(text="🔙 Назад в профиль", callback_data="back_to_profile")]
    ])
    if callback_query.message:
        try:
            await callback_query.message.edit_text(text, reply_markup=keyboard)
        except aiogram.exceptions.TelegramBadRequest:
            pass

@dp.callback_query(F.data.startswith("leaderboard_"))
async def show_leaderboard(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    parts = callback_query.data.split("_")
    if len(parts) < 3:
        await callback_query.answer("Ошибка: неверный формат данных.", show_alert=True)
        return
    _, category, period = parts
    text, keyboard = await generate_leaderboard_view(category, period)
    msg = callback_query.message
    if getattr(msg, "video", None):
        try:
            await msg.delete()
        except aiogram.exceptions.TelegramBadRequest:
            pass
        await msg.answer(text, reply_markup=keyboard, parse_mode="HTML", disable_web_page_preview=True)
    else:
        try:
            await msg.edit_text(text, reply_markup=keyboard, parse_mode="HTML", disable_web_page_preview=True)
        except aiogram.exceptions.TelegramBadRequest as e:
            if "message is not modified" in str(e).lower():
                await callback_query.answer()
            else:
                logging.error(f"Leaderboard edit error: {e}")
            return
    await callback_query.answer()

@dp.callback_query(lambda c: c.data.startswith("confirm_delete_check_"))
async def confirm_delete_check(callback_query: types.CallbackQuery, state: FSMContext):
    check_id = callback_query.data.split("_")[-1]
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"delete_check_final_{check_id}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"manage_check_{check_id}")]
    ])
    await edit_message_or_inline(
        callback_query,
        "⚠️ <b>Вы уверены, что хотите удалить этот чек?</b>\n\n"
        "Чек будет безвозвратно удален, а средства вернутся на ваш основной баланс.",
        reply_markup=keyboard,
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data.startswith("delete_check_final_"))
async def delete_check_final(callback_query: types.CallbackQuery, state: FSMContext):
    check_id = callback_query.data.split("_")[-1]
    user_id = callback_query.from_user.id
    check = await db.get_check(check_id)
    if not check or check.get('creator_id') != user_id:
        await callback_query.answer("❌ Ошибка: чек не найден или у вас нет прав на его удаление.", show_alert=True)
        return
    if check.get('status') == 'cashed':
        await callback_query.answer("❌ Этот чек уже активирован и не может быть удален.", show_alert=True)
        text, keyboard = await get_management_menu(check_id, callback_query.from_user.id)
        if keyboard:
            await edit_message_or_inline(callback_query, text, reply_markup=keyboard)
        return
    try:
        refund_amount = await db.delete_check_with_refund(check_id, user_id)
    except CheckAlreadyCashedError:
        await callback_query.answer("❌ Этот чек уже активирован и не может быть удалён.", show_alert=True)
        text, keyboard = await get_management_menu(check_id, callback_query.from_user.id)
        if keyboard:
            await edit_message_or_inline(callback_query, text, reply_markup=keyboard)
        return
    except CheckNotFoundError:
        await callback_query.answer("❌ Чек не найден.", show_alert=True)
        return
    except CheckPermissionError:
        await callback_query.answer("❌ У вас нет прав на удаление этого чека.", show_alert=True)
        return

    await callback_query.answer(
        f"✅ Чек удалён.\n💸 Возвращено: {refund_amount:.2f}$",
        show_alert=True
    )
    checks_left = await db.count_user_checks(user_id)
    if checks_left == 0:
        try:
            await callback_query.message.delete()
        except Exception:
            pass
        return

    await show_user_checks_list(callback_query, state)

async def show_user_stats(chat_id: int, user_id: int):
    user = await db.get_user(user_id)
    if not user:
        await bot.send_message(chat_id, "Пользователь не найден.")
        return
    stats = await db.get_user_stats(user_id)
    async with aiosqlite.connect(db.db_path, **db.connect_params) as conn:
        async with conn.execute(
            "SELECT game_type, COUNT(*) as cnt FROM transactions WHERE user_id = ? AND type = 'game' GROUP BY game_type ORDER BY cnt DESC LIMIT 1",
            (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            fav_game = row[0] if row else None
    reg_date = user.get('created_at', '—')
    display_name = user.get('full_name') or user.get('username') or f"Пользователь {user_id}"
    display_name = sanitize_nickname(display_name)
    fav_game_rus = '—'
    if fav_game:
        fav_game_rus, _ = get_russian_names(fav_game, '')
    text = (
        f"👤 <b>Статистика игрока {display_name}</b>\n\n"
        f"Любимая игра: <b>{fav_game_rus}</b>\n"
        f"Всего игр: <b>{stats['total_games']}</b>\n"
        f"С нами с: <b>{reg_date}</b>"
    )
    await bot.send_message(chat_id, text, parse_mode="HTML")

@dp.callback_query(lambda c: c.data.startswith("refresh_top_"))
async def refresh_top_callback(callback_query: types.CallbackQuery):
    """Обновление топа конкурса по запросу пользователя"""
    try:
        contest_id = int(callback_query.data.split("_")[-1])
        contest = await db.get_contest_by_id(contest_id)
        if not contest:
            await callback_query.answer("❌ Конкурс не найден", show_alert=True)
            return
        
        message_id = contest.get("channel_message_id")
        if not message_id:
            await callback_query.answer("❌ Нет сообщения конкурса", show_alert=True)
            return
        
        new_caption_full = await format_contest_message(db, contest)
        new_keyboard = await get_contest_keyboard(contest)
        
        try:
            await bot.edit_message_caption(
                chat_id=BETS_ID,
                message_id=message_id,
                caption=new_caption_full,
                parse_mode="HTML",
                reply_markup=new_keyboard
            )
            await callback_query.answer("✅ Топ обновлён!", show_alert=False)
        except Exception as e:
            err = str(e).lower()
            if "message is not modified" in err:
                await callback_query.answer("ℹ️ Топ уже актуален!", show_alert=False)
            elif "flood control exceeded" in err or "too many requests" in err:
                await callback_query.answer("⏳ Слишком часто! Попробуйте позже.", show_alert=True)
            else:
                logging.error(f"[CONTESTS] Ошибка при обновлении топа конкурса #{contest_id}: {e}")
                await callback_query.answer("❌ Ошибка при обновлении топа", show_alert=True)
                
    except Exception as e:
        logging.error(f"[CONTESTS] Критическая ошибка в refresh_top_callback: {e}", exc_info=True)
        await callback_query.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("contest_finished_"))
async def contest_finished_callback(callback_query: types.CallbackQuery):
    await callback_query.answer("Конкурс уже завершён", show_alert=True)

@dp.callback_query(lambda c: c.data == "cancel_check_password")
async def cancel_check_password_callback(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    data = await state.get_data()
    check_id = data.get('check_id')
    if check_id:
        text, keyboard = await get_management_menu(check_id, callback_query.from_user.id)
        await edit_message_or_inline(callback_query, text, reply_markup=keyboard)
    else:
        await callback_query.message.edit_text("Действие отменено.")
    await callback_query.answer("Действие отменено.", show_alert=False)

@dp.callback_query(lambda c: c.data.startswith("set_comment_"))
async def set_comment_start(callback_query: types.CallbackQuery, state: FSMContext):
    check_id = callback_query.data.split("_")[2]
    await state.update_data(
        check_id=check_id,
        menu_message_id=callback_query.message.message_id if callback_query.message else None,
        inline_message_id=callback_query.inline_message_id
    )
    await state.set_state(CheckStates.SET_COMMENT)
    await edit_message_or_inline(
        callback_query,
        "💬 Введите новый комментарий для чека (или '-' чтобы удалить):",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data=f"manage_check_{check_id}")]
            ]
        )
    )
    await callback_query.answer()

@dp.message(CheckStates.SET_COMMENT)
async def process_set_comment(message: types.Message, state: FSMContext):
    if not message.text or message.text.startswith('/') or message.text in MAIN_MENU_BUTTONS:
        return
    data = await state.get_data()
    check_id = data['check_id']
    menu_message_id = data.get('menu_message_id')
    inline_message_id = data.get('inline_message_id')
    comment = message.text.strip()
    if comment == '-' or comment == 'удалить':
        comment = None
    await db.update_check_settings(check_id, {"comment": comment})
    await state.clear()
    await message.delete()
    text, keyboard = await get_management_menu(check_id, message.from_user.id)
    if not keyboard:
        return
    if inline_message_id:
        await bot.edit_message_text(
            text,
            inline_message_id=inline_message_id,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        return
    if menu_message_id:
        await bot.edit_message_text(
            text,
            chat_id=message.chat.id,
            message_id=menu_message_id,
            reply_markup=keyboard,
            parse_mode="HTML"
        )

@dp.callback_query(lambda c: c.data.startswith("set_target_user_"))
async def set_target_user_start(callback_query: types.CallbackQuery, state: FSMContext):
    check_id = callback_query.data.split("_")[3]
    check = await db.get_check(check_id)
    if not check:
        await callback_query.answer("Чек не найден", show_alert=True)
        return
    
    current_target = check.get('target_user_id')
    current_info = "—"
    if current_target:
        target_user = await db.get_user(current_target)
        if target_user:
            username = target_user.get('username', '')
            full_name = target_user.get('full_name', '')
            if username:
                current_info = f"@{username}"
            elif full_name:
                current_info = sanitize_nickname(full_name)
            else:
                current_info = f"ID {current_target}"
    
    await state.update_data(
        check_id=check_id,
        menu_message_id=callback_query.message.message_id if callback_query.message else None,
        inline_message_id=callback_query.inline_message_id
    )
    await state.set_state(CheckStates.SET_TARGET_USER)
    
    keyboard_buttons = [
        [InlineKeyboardButton(text="🔙 Назад", callback_data=f"manage_check_{check_id}")]
    ]
    if current_target:
        keyboard_buttons.insert(0, [InlineKeyboardButton(text="❌ Убрать привязку", callback_data=f"remove_target_user_{check_id}")])
    
    await edit_message_or_inline(
        callback_query,
        (
            f"👤 <b>Привязка пользователя к чеку</b>\n\n"
            f"<b>Текущий пользователь:</b> {current_info}\n\n"
            f"📝 <b>Введите ID или юзернейм пользователя:</b>\n\n"
            f"<i>Пример: @username или 123456789</i>\n"
            f"<i>Или отправьте '-' чтобы убрать привязку</i>"
        ),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data.startswith("remove_target_user_"))
async def remove_target_user(callback_query: types.CallbackQuery, state: FSMContext):
    check_id = callback_query.data.split("_")[3]
    check = await db.get_check(check_id)
    if not check or check.get('creator_id') != callback_query.from_user.id:
        await callback_query.answer("❌ У вас нет прав на изменение этого чека.", show_alert=True)
        return
    
    await db.update_check_settings(check_id, {"target_user_id": None})
    await callback_query.answer("✅ Привязка пользователя убрана", show_alert=True)
    text, keyboard = await get_management_menu(check_id, callback_query.from_user.id)
    if keyboard:
        await edit_message_or_inline(callback_query, text, reply_markup=keyboard)

@dp.message(CheckStates.SET_TARGET_USER)
async def process_set_target_user(message: types.Message, state: FSMContext):
    if not message.text or message.text.startswith('/') or message.text in MAIN_MENU_BUTTONS:
        return
    
    data = await state.get_data()
    check_id = data['check_id']
    menu_message_id = data.get('menu_message_id')
    inline_message_id = data.get('inline_message_id')
    
    check = await db.get_check(check_id)
    if not check or check.get('creator_id') != message.from_user.id:
        await message.answer("❌ У вас нет прав на изменение этого чека.")
        await state.clear()
        return
    
    target_user_str = message.text.strip().lstrip('@')
    
    # Если пользователь хочет убрать привязку
    if target_user_str == '-' or target_user_str.lower() == 'удалить' or target_user_str.lower() == 'убрать':
        await db.update_check_settings(check_id, {"target_user_id": None})
        await message.answer("✅ Привязка пользователя убрана")
        await state.clear()
        await message.delete()
        text, keyboard = await get_management_menu(check_id, message.from_user.id)
        if not keyboard:
            return
        if inline_message_id:
            await bot.edit_message_text(
                text,
                inline_message_id=inline_message_id,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            return
        if menu_message_id:
            await bot.edit_message_text(
                text,
                chat_id=message.chat.id,
                message_id=menu_message_id,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        return
    
    # Ищем пользователя
    target_user = None
    if target_user_str.isdigit():
        target_user = await db.get_user(int(target_user_str))
    else:
        target_user = await db.get_user_by_username(target_user_str)
    
    if not target_user:
        await message.answer(
            "❌ Пользователь не найден.\n\n"
            "Проверьте правильность введённых данных:\n"
            "• ID пользователя (число)\n"
            "• Юзернейм (без @)\n\n"
            "Или отправьте '-' чтобы убрать привязку"
        )
        return
    
    await db.update_check_settings(check_id, {"target_user_id": target_user['user_id']})
    
    username = target_user.get('username', '')
    full_name = target_user.get('full_name', '')
    target_display = f"@{username}" if username else (sanitize_nickname(full_name) if full_name else f"ID {target_user['user_id']}")
    
    await message.answer(f"✅ Пользователь привязан: {target_display}")
    await state.clear()
    await message.delete()
    
    text, keyboard = await get_management_menu(check_id, message.from_user.id)
    if not keyboard:
        return
    if inline_message_id:
        await bot.edit_message_text(
            text,
            inline_message_id=inline_message_id,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        return
    if menu_message_id:
        await bot.edit_message_text(
            text,
            chat_id=message.chat.id,
            message_id=menu_message_id,
            reply_markup=keyboard,
            parse_mode="HTML"
        )

if __name__ == "__main__":
    asyncio.run(main())
