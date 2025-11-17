import logging
import os
import asyncio
import time
import aiogram.exceptions
from aiogram import Bot, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from decimal import Decimal, InvalidOperation
from typing import Optional, Dict, List, Tuple
from contests import create_contest_types_keyboard, format_contest_message, get_contest_keyboard
from datetime import datetime, timedelta

class AdminStates(StatesGroup):
    EDIT_USER = State()
    SEARCH_USERS = State()
    CONFIRM_DELETE = State()
    BROADCAST = State()
    BROADCAST_BUTTONS = State()
    ADD_BALANCE = State()
    ADD_SUB_CHANNEL_ID = State()
    ADD_SUB_CHANNEL_URL = State()
    ADD_SUB_BUTTON_TEXT = State()
    ADD_ADMIN_ID = State()

class ContestAdminStates(StatesGroup):
    CREATE_TYPE = State()
    CREATE_DURATION = State()
    CREATE_PRIZE = State()
    CREATE_TOP_LIMIT = State()

bot: Optional[Bot] = None
db = None
dp = None
crypto_pay = None
LOGS_ID: Optional[int] = None
SUPPORT_LINK: Optional[str] = None

def _format_user_info_list(users: List[Dict]) -> str:
    return "".join(
        f"<code>{user['user_id']}</code> | {user['username']}\n"
        f"Баланс: <code>{user.get('balance', 0):.2f}$</code>\n"
        f"Реф.баланс: <code>{user['ref_balance']:.2f}$</code>\n"
        f"Заработано: <code>{user['ref_earnings']:.2f}$</code>\n"
        f"Рефералов: <code>{user['ref_count']}</code>\n"
        f"Пригласил: <code>{user.get('referrer_username', 'нет')}</code>\n"
        f"Кого пригласил: <code>{', '.join([str(u['user_id']) + ' (' + str(u.get('username', 'нет')) + ')' for u in user.get('invited_users', [])]) if user.get('invited_users') else '—'}</code>\n"
        f"Дата: <code>{user['created_at']}</code>\n\n"
        for user in users
    )

def create_user_management_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Баланс", callback_data=f"edit_balance_{user_id}"),
            InlineKeyboardButton(text="Реф.Баланс", callback_data=f"edit_ref_balance_{user_id}")
        ],
        [
            InlineKeyboardButton(text="Реф.Заработок", callback_data=f"edit_ref_earnings_{user_id}"),
            InlineKeyboardButton(text="Реф.Счетчик", callback_data=f"edit_ref_count_{user_id}")
        ],
        [
            InlineKeyboardButton(text="Удалить", callback_data=f"delete_user_{user_id}"),
            InlineKeyboardButton(text="Назад", callback_data="admin_users")
        ]
    ])

def get_admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Управление пользователями", callback_data="admin_users")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="💰 CryptoBot", callback_data="admin_cryptobot")],
        [InlineKeyboardButton(text="📨 Рассылка", callback_data="broadcast")],
        [InlineKeyboardButton(text="📢 Каналы подписки", callback_data="admin_sub_channels")],
        [InlineKeyboardButton(text="🏆 Конкурсы", callback_data="admin_contests")],
        [InlineKeyboardButton(text="🧹 Очистить балансы", callback_data="admin_clear_balances")]
    ])

def _get_broadcast_preview(data: Dict) -> Tuple[str, InlineKeyboardMarkup]:
    buttons = data.get('buttons', [])
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить кнопку", callback_data="add_button")],
        [
            InlineKeyboardButton(text="✅ Начать рассылку", callback_data="start_sending"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_broadcast")
        ]
    ])
    preview_text = "📨 Предпросмотр сообщения:\n\n"
    preview_text += data.get('text', '') if data.get('message_type') == "text" else (data.get('text') or "Медиа-сообщение")
    if buttons:
        preview_text += "\n\n🔗 Добавленные кнопки:" + "".join(f"\n• {btn['text']} -> {btn['url']}" for btn in buttons)
    return preview_text, keyboard

def init(bot_instance, dp_instance, db_instance, cryptopay_instance, logs_id, support_link):
    global bot, db, dp, crypto_pay, LOGS_ID, SUPPORT_LINK
    bot = bot_instance
    dp = dp_instance
    db = db_instance
    crypto_pay = cryptopay_instance
    LOGS_ID = logs_id
    SUPPORT_LINK = support_link
    setup_handlers()

async def is_admin(user_id: int) -> bool:
    return str(user_id) == os.getenv("ADMIN_USER_ID")

async def cmd_admin(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("👑 <b>Админ-панель</b>", reply_markup=get_admin_panel_keyboard(), parse_mode="HTML")

async def show_users(callback_query: types.CallbackQuery):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    users = await db.get_all_users(limit=10)
    if hasattr(db, 'get_users_invited_by'):
        for user in users:
            user['invited_users'] = await db.get_users_invited_by(user['user_id'])
    else:
        for user in users:
            user['invited_users'] = []
    text = "<b>Управление пользователями</b>\n\n" + _format_user_info_list(users)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Поиск", callback_data="search_users")],
        [InlineKeyboardButton(text="Следующая", callback_data="users_next_10")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_admin")]
    ])
    await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback_query.answer()

async def show_admin_stats(callback_query: types.CallbackQuery):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    stats = await db.get_admin_stats()
    text = (
        "<b>Статистика</b>\n\n"
        f"<blockquote><b>Пользователи:</b>\n"
        f"• Всего: <code>{stats['total_users']}</code>\n"
        f"• Сегодня: <code>{stats['today_users']}</code>\n"
        f"• За неделю: <code>{stats['week_users']}</code></blockquote>\n\n"
        f"<blockquote><b>Игры сегодня:</b>\n"
        f"• Всего: <code>{stats['today_games']}</code>\n"
        f"• Выиграно: <code>{stats['today_wins']}</code>\n"
        f"• Проиграно: <code>{stats['today_losses']}</code>\n"
        f"• Оборот: <code>{stats['today_turnover']:.2f}$</code>\n"
        f"• Прибыль: <code>{stats.get('today_profit', 0):.2f}$</code></blockquote>\n\n"
        f"<blockquote><b>Игры за неделю:</b>\n"
        f"• Всего: <code>{stats['week_games']}</code>\n"
        f"• Выиграно: <code>{stats['week_wins']}</code>\n"
        f"• Проиграно: <code>{stats['week_losses']}</code>\n"
        f"• Оборот: <code>{stats['week_turnover']:.2f}$</code>\n"
        f"• Прибыль: <code>{stats.get('week_profit', 0):.2f}$</code></blockquote>"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Обновить", callback_data="admin_stats")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_admin")]
    ])
    try:
        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception:
        pass
    await callback_query.answer()

async def back_to_admin_panel(callback_query: types.CallbackQuery):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    await callback_query.message.edit_text("👑 <b>Админ-панель</b>", reply_markup=get_admin_panel_keyboard(), parse_mode="HTML")
    await callback_query.answer()

async def search_users_cmd(callback_query: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback_query.from_user.id):
        return
    await state.set_state(AdminStates.SEARCH_USERS)
    await callback_query.message.answer("Введите ID или username пользователя:")
    await callback_query.answer()

async def process_user_search(message: types.Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    search_query = message.text.lstrip('@')
    users = await db.search_users(search_query)
    if not users:
        await message.answer("Пользователи не найдены")
        await state.clear()
        return
    text = "<b>Результаты поиска:</b>\n\n" + _format_user_info_list(users)
    keyboard = create_user_management_keyboard(users[0]['user_id'])
    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    await state.clear()

async def handle_edit_user(callback_query: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback_query.from_user.id):
        return
    _, field, user_id = callback_query.data.split("_", 2)
    await state.update_data(field=field, user_id=user_id)
    await state.set_state(AdminStates.EDIT_USER)
    field_names = {
        "balance": "баланс",
        "ref_balance": "реферальный баланс",
        "ref_earnings": "заработок с рефералов",
        "ref_count": "количество рефералов",
        "referrer": "ID пригласившего"
    }
    await callback_query.message.answer(
        f"✏️ Введите новое значение для поля '{field_names.get(field, field)}'.\n\n"
        "Используйте `+` или `-` для увеличения/уменьшения значения (например, `+10` или `-5.5`).\n"
        "Чтобы установить точное значение, введите число без знака."
    )
    await callback_query.answer()

async def process_edit_user(message: types.Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    data = await state.get_data()
    field = data['field']
    user_id = int(data['user_id'])
    try:
        raw_value = message.text.strip()
        is_decimal = 'balance' in field or 'earnings' in field
        user = await db.get_user(user_id)
        if not user:
            await message.answer("❌ Пользователь не найден.")
            await state.clear()
            return
        current_value = user.get(field, Decimal('0') if is_decimal else 0)
        if raw_value.startswith('+') or raw_value.startswith('-'):
            value_to_change = Decimal(raw_value[1:]) if is_decimal else int(raw_value[1:])
            new_value = current_value + value_to_change if raw_value.startswith('+') else current_value - value_to_change
        else:
            new_value = Decimal(raw_value) if is_decimal else int(raw_value)
        updates = {field: new_value}
        if await db.update_user(user_id, updates):
            await message.answer(f"✅ Значение поля '{field}' успешно обновлено.")
            updated_user = await db.get_user(user_id)
            referrer_username = "нет"
            if updated_user.get('referrer_id'):
                referrer_user = await db.get_user(updated_user['referrer_id'])
                if referrer_user:
                    referrer_username = referrer_user.get('username', updated_user['referrer_id'])
            text = (
                f"👤 Пользователь <code>{updated_user['user_id']}</code>\n"
                f"💰 Баланс: <code>{updated_user['balance']:.2f}$</code>\n"
                f"🔄 Реф.баланс: <code>{updated_user['ref_balance']:.2f}$</code>\n"
                f"💎 Заработано: <code>{updated_user['ref_earnings']:.2f}$</code>\n"
                f"👥 Рефералов: <code>{updated_user['ref_count']}</code>\n"
                f"🔗 Пригласил: <code>{referrer_username}</code>"
            )
            keyboard = create_user_management_keyboard(user_id)
            await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
        else:
            await message.answer("❌ Ошибка при обновлении")
    except (ValueError, InvalidOperation):
        await message.answer("❌ Неверный формат значения. Убедитесь, что вы вводите число.")
    await state.clear()

async def confirm_delete_user(callback_query: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback_query.from_user.id):
        return
    user_id = int(callback_query.data.split("_")[2])
    await state.update_data(user_id=user_id)
    await state.set_state(AdminStates.CONFIRM_DELETE)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да", callback_data=f"confirm_delete_{user_id}"),
            InlineKeyboardButton(text="❌ Нет", callback_data="cancel_delete")
        ]
    ])
    await callback_query.message.answer(
        f"⚠️ Вы уверены, что хотите удалить пользователя <code>{user_id}</code>?",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await callback_query.answer()

async def process_delete_user(callback_query: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback_query.from_user.id):
        return
    user_id = int(callback_query.data.split("_")[2])
    if await db.delete_user(user_id):
        await callback_query.message.answer(f"✅ Пользователь {user_id} удален")
    else:
        await callback_query.message.answer("❌ Ошибка при удалении пользователя")
    await state.clear()
    await callback_query.answer()

async def cancel_delete_user(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback_query.message.answer("❌ Удаление отменено")
    await callback_query.answer()

async def show_more_users(callback_query: types.CallbackQuery):
    if not await is_admin(callback_query.from_user.id):
        return
    offset = int(callback_query.data.split("_")[2])
    users = await db.get_all_users(limit=10, offset=offset)
    if not users:
        await callback_query.answer("Больше пользователей нет")
        return
    text = "<b>👥 Управление пользователями</b>\n\n" + _format_user_info_list(users)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск", callback_data="search_users")],
        [
            InlineKeyboardButton(text="◀️ Предыдущая", callback_data=f"users_next_{max(0, offset-10)}"),
            InlineKeyboardButton(text="▶️ Следующая", callback_data=f"users_next_{offset+10}")
        ],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_admin")]
    ])
    try:
        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception:
        pass
    await callback_query.answer()

async def start_broadcast(callback_query: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_broadcast")]]
    )
    await callback_query.message.edit_text(
        "📨 Отправьте сообщение для рассылки.\n\n"
        "Поддерживаются все типы сообщений (текст, фото, видео и т.д.).\n"
        "После отправки сообщения, вы сможете добавить кнопки к нему.",
        reply_markup=keyboard
    )
    await state.set_state(AdminStates.BROADCAST)
    await callback_query.answer()

async def cancel_broadcast(callback_query: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    await callback_query.message.edit_text("👑 <b>Админ-панель</b>", reply_markup=get_admin_panel_keyboard(), parse_mode="HTML")
    await callback_query.answer()

async def handle_broadcast_message(message: types.Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    await state.update_data(
        message_type=message.content_type,
        text=message.text if message.content_type == "text" else message.caption,
        file_id=getattr(message, message.content_type, {}).file_id if message.content_type != "text" else None,
        parse_mode="HTML" if message.content_type == "text" else None
    )
    data = await state.get_data()
    preview_text, keyboard = _get_broadcast_preview(data)
    await message.answer(preview_text, reply_markup=keyboard)
    await state.set_state(AdminStates.BROADCAST_BUTTONS)

async def add_broadcast_button(callback_query: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    data = await state.get_data()
    buttons = data.get('buttons', [])
    if len(buttons) >= 10:
        await callback_query.answer("Достигнут лимит кнопок (10)", show_alert=True)
        return
    await callback_query.message.edit_text(
        "🔗 Отправьте кнопку в формате:\n"
        "<code>Текст кнопки | https://example.com</code>\n\n"
        "Пример:\n"
        "<code>Наш канал | https://t.me/channel</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_add_button")]
        ])
    )
    await state.set_state(AdminStates.BROADCAST_BUTTONS)
    await callback_query.answer()

async def cancel_add_button(callback_query: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    data = await state.get_data()
    preview_text, keyboard = _get_broadcast_preview(data)
    await callback_query.message.edit_text(preview_text, reply_markup=keyboard)
    await callback_query.answer()

async def handle_button_input(message: types.Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    if "|" not in message.text:
        await message.reply(
            "❌ Неверный формат. Используйте:\n<code>Текст кнопки | https://example.com</code>",
            parse_mode="HTML"
        )
        return
    text, url = [x.strip() for x in message.text.split("|", 1)]
    if not url.startswith(("http://", "https://", "t.me/", "tg://")):
        await message.reply("❌ Неверный формат ссылки. Ссылка должна начинаться с http://, https://, t.me/ или tg://")
        return
    data = await state.get_data()
    buttons = data.get('buttons', [])
    buttons.append({"text": text, "url": url})
    await state.update_data(buttons=buttons)
    data = await state.get_data()
    preview_text, keyboard = _get_broadcast_preview(data)
    await message.answer(preview_text, reply_markup=keyboard, disable_web_page_preview=True)

async def process_broadcast(callback_query: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    data = await state.get_data()
    buttons = data.get('buttons', [])
    inline_buttons = [
        [InlineKeyboardButton(text=btn['text'], url=btn['url']) for btn in buttons[i:i+2]]
        for i in range(0, len(buttons), 2)
    ]
    keyboard = InlineKeyboardMarkup(inline_keyboard=inline_buttons) if buttons else None
    users = await db.get_all_users(limit=1000000)
    total_users = len(users)
    if not total_users:
        await callback_query.message.edit_text("Не найдено пользователей для рассылки.")
        await state.clear()
        return
    status_message = await callback_query.message.edit_text(
        f"📨 Рассылка начата...\n\n"
        f"⏳ Всего пользователей: {total_users}\n"
    )
    start_time = time.time()
    successful = failed = blocked = deleted = 0
    for i, user in enumerate(users, 1):
        try:
            if data['message_type'] == "text":
                await bot.send_message(user['user_id'], data['text'], parse_mode=data['parse_mode'], reply_markup=keyboard)
            else:
                method = getattr(bot, f"send_{data['message_type']}")
                await method(user['user_id'], data['file_id'], caption=data['text'], reply_markup=keyboard)
            successful += 1
        except aiogram.exceptions.TelegramForbiddenError:
            blocked += 1
        except aiogram.exceptions.TelegramBadRequest as e:
            if "chat not found" in str(e).lower():
                deleted += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
            logging.error(f"Ошибка при рассылке пользователю {user['user_id']}: {e}")
        if i % 20 == 0 or i == total_users:
            elapsed = int(time.time() - start_time)
            progress = (i / total_users) * 100
            try:
                await status_message.edit_text(
                    f"📨 Рассылка в процессе...\n\n"
                    f"⏳ Всего: {total_users}\n"
                    f"✅ Отправлено: {successful}\n"
                    f"❌ Ошибок: {failed}\n"
                    f"🚫 Заблокировали: {blocked}\n"
                    f"🗑 Удалили: {deleted}\n"
                    f"⏱ Время: {elapsed} сек\n"
                    f"📊 Прогресс: {progress:.1f}%"
                )
            except aiogram.exceptions.TelegramBadRequest:
                pass
        await asyncio.sleep(0.05)
    elapsed = int(time.time() - start_time)
    speed = total_users / elapsed if elapsed > 0 else 0
    await status_message.edit_text(
        f"✅ Рассылка завершена\n\n"
        f"📊 Статистика:\n"
        f"👥 Всего: {total_users}\n"
        f"✅ Успешно: {successful}\n"
        f"❌ Ошибок: {failed}\n"
        f"🚫 Заблокировали: {blocked}\n"
        f"🗑 Удалённые: {deleted}\n"
        f"⏱ Время: {elapsed} сек\n"
        f"⚡️ Скорость: {speed:.1f} сооб/сек\n\n"
        f"📈 Успех: {(successful/total_users*100):.1f}%",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Назад", callback_data="back_to_admin")]])
    )
    await state.clear()

async def show_cryptobot_balance(callback_query: types.CallbackQuery):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    try:
        balance_data = await crypto_pay.get_balance()
        balances = balance_data.get('result', [])
        balance_text = "<b>Баланс CryptoBot</b>\n\n"
        if balances:
            for balance in balances:
                balance_text += f"<b>{balance.get('currency_code', '')}:</b> <code>{float(balance.get('available', 0)):.2f}</code>\n"
        else:
            balance_text += "Нет доступных балансов"
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Пополнить", callback_data="add_cryptobot_balance")],
            [InlineKeyboardButton(text="🧾 Активные чеки", callback_data="admin_checks")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_cryptobot_balance")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_admin")]
        ])
        await callback_query.message.edit_text(balance_text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Error getting CryptoBot balance: {e}")
        await callback_query.message.edit_text(
            f"<b>Ошибка при получении баланса</b>\n\nПричина: {str(e)}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Повторить", callback_data="refresh_cryptobot_balance")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_admin")]
            ]),
            parse_mode="HTML"
        )
    await callback_query.answer()

async def admin_show_checks(callback_query: types.CallbackQuery):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    try:
        checks_data = await crypto_pay.get_checks(status="active", asset="USDT")
        checks = checks_data.get('result', {}).get('items', [])
        if not checks:
            await callback_query.message.edit_text(
                "<b>Нет активных чеков</b>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_cryptobot")]]),
                parse_mode="HTML"
            )
            return
        text = "<b>Активные чеки USDT</b>\n\n"
        keyboard_buttons = []
        for check in checks[:10]:
            text += f"<b>Сумма:</b> <code>{check.get('amount')}</code> | <b>ID:</b> <code>{check.get('check_id')}</code> | <b>Статус:</b> <code>{check.get('status')}</code>\n"
            keyboard_buttons.append([InlineKeyboardButton(text=f"❌ Удалить {check.get('amount')}$", callback_data=f"admin_delete_check_{check.get('check_id')}")])
        keyboard_buttons.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_refresh_checks")])
        keyboard_buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin_cryptobot")])
        await callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons), parse_mode="HTML")
    except Exception as e:
        await callback_query.message.edit_text(
            f"❌ <b>Ошибка при получении чеков:</b> {e}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_cryptobot")]]),
            parse_mode="HTML"
        )
    await callback_query.answer()

async def admin_delete_check(callback_query: types.CallbackQuery):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    check_id = int(callback_query.data.split('_')[-1])
    try:
        result = await crypto_pay.delete_check(check_id)
        if result.get('ok'):
            await callback_query.answer("Чек удалён", show_alert=True)
        else:
            await callback_query.answer(f"Ошибка: {result.get('error', {}).get('message', 'Не удалось удалить чек')}", show_alert=True)
    except Exception as e:
        await callback_query.answer(f"Ошибка: {e}", show_alert=True)
    await admin_show_checks(callback_query)

async def admin_refresh_checks(callback_query: types.CallbackQuery):
    await admin_show_checks(callback_query)

async def add_cryptobot_balance(callback_query: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.ADD_BALANCE)
    await callback_query.message.edit_text(
        "<b>💳 Пополнение баланса CryptoBot</b>\n\nВведите сумму пополнения в USDT:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_add_balance")]])
    )
    await callback_query.answer()

async def process_add_balance(message: types.Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    try:
        amount = Decimal(message.text)
        if amount <= 0:
            await message.answer("❌ Сумма должна быть больше 0")
            return
        invoice_data = await crypto_pay.create_invoice(asset="USDT", amount=str(amount), description=f"Пополнение баланса CryptoBot на {amount} USDT", hidden_message="Спасибо за пополнение!")
        pay_url = invoice_data.get('result', {}).get('pay_url')
        if not pay_url:
            raise Exception("Не удалось создать счет для оплаты")
        await message.answer(
            f"✅ <b>Счет на пополнение создан</b>\n\n<b>Сумма:</b> <code>{amount}$</code>\n<b>Валюта:</b> <code>USDT</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Оплатить", url=pay_url)],
                [InlineKeyboardButton(text="🔙 Вернуться в админ-панель", callback_data="back_to_admin")]
            ])
        )
        await bot.send_message(
            chat_id=LOGS_ID,
            text=f"💳 <b>Создан счет на пополнение CryptoBot</b>\n\n"
                 f"<b>Администратор:</b> {message.from_user.mention_html()}\n"
                 f"<b>Сумма:</b> <code>{amount}$</code>\n"
                 f"<b>Валюта:</b> <code>USDT</code>",
            parse_mode="HTML"
        )
    except (ValueError, InvalidOperation):
        await message.answer("❌ <b>Ошибка:</b> введите корректную сумму\n<i>Пример: 100.50</i>", parse_mode="HTML")
    except Exception as e:
        logging.error(f"Error creating invoice: {e}")
        await message.answer(f"❌ <b>Ошибка при создании счета</b>\n\nПричина: {str(e)}", parse_mode="HTML")
    finally:
        await state.clear()

async def cancel_add_balance(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.edit_text(
        "<b>Пополнение отменено</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Вернуться в админ-панель", callback_data="back_to_admin")]])
    )
    await callback_query.answer()
    await state.clear()

async def refresh_cryptobot_balance(callback_query: types.CallbackQuery):
    await show_cryptobot_balance(callback_query)

async def admin_sub_channels(update: types.Update):
    if not await is_admin(update.from_user.id):
        return
    channels = await db.get_subscription_channels()
    text = "<b>📢 Управление каналами для подписки</b>\n\n"
    if not channels:
        text += "Каналы для обязательной подписки еще не добавлены."
    keyboard_buttons = [
        [InlineKeyboardButton(text=f"❌ Удалить {channel['button_text']}", callback_data=f"delete_sub_channel_{channel['channel_id']}")]
        for channel in channels
    ]
    for channel in channels:
        text += f"• {channel['button_text']}: {channel['channel_url']} (<code>{channel['channel_id']}</code>)\n"
    keyboard_buttons.append([InlineKeyboardButton(text="➕ Добавить канал", callback_data="add_sub_channel")])
    keyboard_buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_admin")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    if isinstance(update, types.CallbackQuery):
        await update.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML", disable_web_page_preview=True)
        await update.answer()
    elif isinstance(update, types.Message):
        await update.answer(text, reply_markup=keyboard, parse_mode="HTML", disable_web_page_preview=True)

async def add_sub_channel_start(callback_query: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback_query.from_user.id):
        return
    await state.set_state(AdminStates.ADD_SUB_CHANNEL_ID)
    await callback_query.message.edit_text("Отправьте ID канала (например, -100123456789).")
    await callback_query.answer()

async def add_sub_channel_id(message: types.Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    try:
        channel_id = int(message.text)
        await state.update_data(channel_id=channel_id)
        await state.set_state(AdminStates.ADD_SUB_CHANNEL_URL)
        await message.answer("Теперь отправьте URL канала (например, https://t.me/your_channel).")
    except ValueError:
        await message.answer("ID канала должен быть числом. Попробуйте снова.")

async def add_sub_channel_url(message: types.Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    if not message.text.startswith("https://t.me/"):
        await message.answer("URL должен начинаться с https://t.me/. Попробуйте снова.")
        return
    await state.update_data(channel_url=message.text)
    await state.set_state(AdminStates.ADD_SUB_BUTTON_TEXT)
    await message.answer("Теперь отправьте текст для кнопки (например, 'Новостной канал').")

async def add_sub_button_text(message: types.Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    data = await state.get_data()
    try:
        await db.add_subscription_channel(channel_id=data['channel_id'], channel_url=data['channel_url'], button_text=message.text)
        await message.answer("✅ Канал успешно добавлен!")
    except Exception as e:
        await message.answer(f"❌ Произошла ошибка при добавлении канала: {e}")
    await state.clear()
    await admin_sub_channels(message)

async def delete_sub_channel(callback_query: types.CallbackQuery):
    if not await is_admin(callback_query.from_user.id):
        return
    channel_id = int(callback_query.data.split('_')[-1])
    try:
        await db.delete_subscription_channel(channel_id)
        await callback_query.answer("Канал удален.", show_alert=True)
    except Exception as e:
        await callback_query.answer(f"Ошибка: {e}", show_alert=True)
    await admin_sub_channels(callback_query)

async def admin_clear_balances_confirm(callback_query: types.CallbackQuery):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="admin_clear_balances_confirmed")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_admin")]
    ])
    await callback_query.message.edit_text(
        "⚠️ Вы уверены, что хотите обнулить балансы всех пользователей? Это действие необратимо!",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await callback_query.answer()

async def admin_clear_balances_do(callback_query: types.CallbackQuery):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    await db.clear_all_user_balances()
    await callback_query.message.edit_text(
        "✅ Балансы всех пользователей успешно обнулены!",
        reply_markup=get_admin_panel_keyboard(),
        parse_mode="HTML"
    )
    await callback_query.answer()

async def show_admin_contests(callback_query: types.CallbackQuery):
    if not await is_admin(callback_query.from_user.id):
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Новый конкурс", callback_data="admin_create_contest")],
        [InlineKeyboardButton(text="Активные", callback_data="admin_active_contests"), InlineKeyboardButton(text="Завершённые", callback_data="admin_completed_contests")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_admin")]
    ])
    await callback_query.message.edit_text("<b>🏆 Админка конкурсов</b>", reply_markup=keyboard, parse_mode="HTML")
    await callback_query.answer()

async def admin_create_contest_start(callback_query: types.CallbackQuery, state: FSMContext):
    await state.set_state(ContestAdminStates.CREATE_TYPE)
    await callback_query.message.edit_text("Выберите тип конкурса:", reply_markup=create_contest_types_keyboard())
    await callback_query.answer()

async def admin_create_contest_type(callback_query: types.CallbackQuery, state: FSMContext):
    contest_type = callback_query.data.replace("contest_type_", "")
    await state.update_data(type=contest_type)
    await state.set_state(ContestAdminStates.CREATE_DURATION)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="6 часов", callback_data="contest_duration_6")],
        [InlineKeyboardButton(text="12 часов", callback_data="contest_duration_12")],
        [InlineKeyboardButton(text="24 часа", callback_data="contest_duration_24")],
        [InlineKeyboardButton(text="7 дней", callback_data="contest_duration_168")]
    ])
    await callback_query.message.edit_text("Выберите длительность конкурса:", reply_markup=keyboard)
    await callback_query.answer()

async def admin_create_contest_duration(callback_query: types.CallbackQuery, state: FSMContext):
    hours = int(callback_query.data.split('_')[-1])
    await state.update_data(duration=hours)
    await state.set_state(ContestAdminStates.CREATE_PRIZE)
    await callback_query.message.edit_text("Введите сумму приза (например, 100):")
    await callback_query.answer()

async def admin_create_contest_prize(message: types.Message, state: FSMContext):
    try:
        prize = Decimal(message.text.replace(",", "."))
        if prize <= 0:
            raise ValueError
    except Exception:
        await message.answer("❌ Введите корректную сумму приза")
        return
    await state.update_data(prize=str(prize))
    await state.set_state(ContestAdminStates.CREATE_TOP_LIMIT)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Топ-3", callback_data="top_limit_3")],
        [InlineKeyboardButton(text="Топ-5", callback_data="top_limit_5")],
        [InlineKeyboardButton(text="Топ-10", callback_data="top_limit_10")]
    ])
    await message.answer("Сколько участников показывать в топе?", reply_markup=keyboard)

async def admin_create_contest_top_limit_btn(callback_query: types.CallbackQuery, state: FSMContext):
    top_limit = int(callback_query.data.split("_")[-1])
    data = await state.get_data()
    contest_type = data["type"]
    hours = data["duration"]
    prize = data["prize"]
    end_time = (datetime.now() + timedelta(hours=hours)).isoformat()
    contest_id = await db.create_contest(
        type=contest_type,
        title=f"Конкурс {contest_type}",
        description="",
        prize=prize,
        end_time=end_time,
        status='active'
    )
    await db.update_contest_settings(contest_id, {"top_limit": top_limit})
    BETS_ID = int(os.getenv("BETS_ID", "-1002403460000"))
    msg = await bot.send_photo(
        chat_id=BETS_ID,
        photo=FSInputFile("depov.jpg"),
        caption=await format_contest_message(db, {
            'id': contest_id,
            'type': contest_type,
            'prize': prize,
            'description': '',
            'end_time': end_time,
            'status': 'active',
            'winner_id': None,
            'top_limit': top_limit
        }),
        parse_mode="HTML",
        reply_markup=await get_contest_keyboard({'id': contest_id, 'bet_channel_url': None, 'bot_deeplink': None})
    )
    try:
        await bot.pin_chat_message(BETS_ID, msg.message_id, disable_notification=True)
    except Exception as e:
        logging.error(f"Ошибка закрепления конкурса: {e}")
    await db.set_contest_channel_message(contest_id, msg.message_id)
    await callback_query.message.answer("✅ Конкурс создан, опубликован и закреплён!")
    await state.clear()
    await callback_query.answer()

async def admin_active_contests(callback_query: types.CallbackQuery):
    contests = await db.get_active_contests()
    if not contests:
        await callback_query.message.edit_text(
            "Нет активных конкурсов.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_contests")]]))
        await callback_query.answer()
        return
    text = "<b>Активные конкурсы:</b>\n\n"
    keyboard_buttons = [
        [
            InlineKeyboardButton(text="Завершить", callback_data=f"admin_finish_contest_{c['id']}"),
            InlineKeyboardButton(text="Удалить", callback_data=f"admin_delete_contest_{c['id']}")
        ]
        for c in contests
    ]
    for c in contests:
        text += f"ID: <code>{c['id']}</code> | {c['type']} | Приз: {c['prize']}$ | До: {c['end_time']}\n"
    keyboard_buttons.append([InlineKeyboardButton(text="Назад", callback_data="admin_contests")])
    await callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons), parse_mode="HTML")
    await callback_query.answer()

async def admin_completed_contests(callback_query: types.CallbackQuery):
    contests = await db.get_completed_contests()
    if not contests:
        await callback_query.message.edit_text(
            "Нет завершённых конкурсов.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_contests")]]))
        await callback_query.answer()
        return
    text = "<b>Завершённые конкурсы:</b>\n\n"
    for c in contests:
        text += f"ID: <code>{c['id']}</code> | {c['type']} | Приз: {c['prize']}$ | До: {c['end_time']}\n"
    await callback_query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_contests")]]),
        parse_mode="HTML"
    )
    await callback_query.answer()

async def admin_finish_contest(callback_query: types.CallbackQuery):
    contest_id = int(callback_query.data.split('_')[-1])
    contest = await db.get_contest_by_id(contest_id)
    if not contest or contest['status'] == 'completed':
        await callback_query.answer("Конкурс уже завершён или не найден", show_alert=True)
        return
    winner = await db.get_contest_winner(contest_id, contest['type'])
    await db.complete_contest(contest_id, winner['user_id'] if winner else None)
    if winner:
        try:
            prize = Decimal(contest['prize'])
            await db.update_balance(winner['user_id'], prize)
            await bot.send_message(winner['user_id'], f"🎉 Поздравляем! Вы выиграли конкурс и получили {prize:.2f}$ на баланс!")
        except Exception as e:
            logging.error(f"Ошибка начисления приза победителю: {e}")
    try:
        await bot.edit_message_text(
            await format_contest_message(db, {**contest, 'winner_id': winner['user_id'] if winner else None, 'status': 'completed'}),
            chat_id=int(os.getenv("BETS_ID", "-1002403460000")),
            message_id=contest['channel_message_id'],
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Ошибка обновления сообщения конкурса: {e}")
    await callback_query.answer("Конкурс завершён!", show_alert=True)
    await admin_active_contests(callback_query)

async def admin_delete_contest(callback_query: types.CallbackQuery):
    contest_id = int(callback_query.data.split('_')[-1])
    contest = await db.get_contest_by_id(contest_id)
    if not contest:
        await callback_query.answer("Конкурс не найден", show_alert=True)
        return
    try:
        if contest['channel_message_id']:
            await bot.delete_message(int(os.getenv("BETS_ID", "-1002403460000")), contest['channel_message_id'])
    except Exception as e:
        logging.error(f"Ошибка удаления сообщения конкурса: {e}")
    try:
        await db.delete_contest(contest_id)
    except Exception as e:
        logging.error(f"Ошибка удаления конкурса из базы: {e}")
        await callback_query.answer("Ошибка удаления из базы", show_alert=True)
        return
    await callback_query.answer("Конкурс удалён!", show_alert=True)
    await admin_active_contests(callback_query)

def setup_handlers():
    dp.message.register(cmd_admin, Command("admin"))
    dp.callback_query.register(show_users, F.data == "admin_users")
    dp.callback_query.register(show_admin_stats, F.data == "admin_stats")
    dp.callback_query.register(back_to_admin_panel, F.data == "back_to_admin")
    dp.callback_query.register(search_users_cmd, F.data == "search_users")
    dp.message.register(process_user_search, AdminStates.SEARCH_USERS)
    dp.callback_query.register(handle_edit_user, F.data.startswith("edit_"))
    dp.message.register(process_edit_user, AdminStates.EDIT_USER)
    dp.callback_query.register(confirm_delete_user, F.data.startswith("delete_user_"))
    dp.callback_query.register(process_delete_user, F.data.startswith("confirm_delete_"))
    dp.callback_query.register(cancel_delete_user, F.data == "cancel_delete")
    dp.callback_query.register(show_more_users, F.data.startswith("users_next_"))
    dp.callback_query.register(start_broadcast, F.data == "broadcast")
    dp.callback_query.register(cancel_broadcast, F.data == "cancel_broadcast")
    dp.message.register(handle_broadcast_message, AdminStates.BROADCAST)
    dp.callback_query.register(add_broadcast_button, F.data == "add_button")
    dp.callback_query.register(cancel_add_button, F.data == "cancel_add_button")
    dp.message.register(handle_button_input, AdminStates.BROADCAST_BUTTONS)
    dp.callback_query.register(process_broadcast, F.data == "start_sending")
    dp.callback_query.register(show_cryptobot_balance, F.data == "admin_cryptobot")
    dp.callback_query.register(admin_show_checks, F.data == "admin_checks")
    dp.callback_query.register(admin_delete_check, F.data.startswith("admin_delete_check_"))
    dp.callback_query.register(admin_refresh_checks, F.data == "admin_refresh_checks")
    dp.callback_query.register(add_cryptobot_balance, F.data == "add_cryptobot_balance")
    dp.message.register(process_add_balance, AdminStates.ADD_BALANCE)
    dp.callback_query.register(cancel_add_balance, F.data == "cancel_add_balance")
    dp.callback_query.register(refresh_cryptobot_balance, F.data == "refresh_cryptobot_balance")
    dp.callback_query.register(admin_sub_channels, F.data == "admin_sub_channels")
    dp.callback_query.register(add_sub_channel_start, F.data == "add_sub_channel")
    dp.message.register(add_sub_channel_id, AdminStates.ADD_SUB_CHANNEL_ID)
    dp.message.register(add_sub_channel_url, AdminStates.ADD_SUB_CHANNEL_URL)
    dp.message.register(add_sub_button_text, AdminStates.ADD_SUB_BUTTON_TEXT)
    dp.callback_query.register(delete_sub_channel, F.data.startswith("delete_sub_channel_"))
    dp.callback_query.register(admin_clear_balances_confirm, F.data == "admin_clear_balances")
    dp.callback_query.register(admin_clear_balances_do, F.data == "admin_clear_balances_confirmed")
    dp.callback_query.register(show_admin_contests, F.data == "admin_contests")
    dp.callback_query.register(admin_create_contest_start, F.data == "admin_create_contest")
    dp.callback_query.register(admin_create_contest_type, F.data.startswith("contest_type_"), ContestAdminStates.CREATE_TYPE)
    dp.callback_query.register(admin_create_contest_duration, F.data.startswith("contest_duration_"), ContestAdminStates.CREATE_DURATION)
    dp.message.register(admin_create_contest_prize, ContestAdminStates.CREATE_PRIZE)
    dp.callback_query.register(admin_active_contests, F.data == "admin_active_contests")
    dp.callback_query.register(admin_completed_contests, F.data == "admin_completed_contests")
    dp.callback_query.register(admin_finish_contest, F.data.startswith("admin_finish_contest_"))
    dp.callback_query.register(admin_delete_contest, F.data.startswith("admin_delete_contest_"))
    dp.callback_query.register(admin_create_contest_top_limit_btn, F.data.startswith("top_limit_"), ContestAdminStates.CREATE_TOP_LIMIT)