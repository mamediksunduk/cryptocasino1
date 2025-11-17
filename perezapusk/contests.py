import logging
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal

from aiogram import types, Router, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext

import pytz

# Эти переменные будут установлены при инициализации
INVOICE_URL = None
bot = None
db = None
BETS_ID = None

def init_contests(bot_instance, db_instance, bets_id, invoice_url):
    """Инициализация модуля конкурсов"""
    global bot, db, BETS_ID, INVOICE_URL
    bot = bot_instance
    db = db_instance
    BETS_ID = bets_id
    INVOICE_URL = invoice_url
    logging.info("[CONTESTS] Модуль конкурсов инициализирован")

def create_contest_types_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Самая крупная ставка", callback_data="contest_type_biggest_bet")],
        [InlineKeyboardButton(text="Самый крупный оборот", callback_data="contest_type_biggest_turnover")]
    ])

def get_prizes_list(prize_total, count):
    prize_total = Decimal(prize_total)
    if count == 1:
        return [prize_total]
    if count == 2:
        p1 = (prize_total * Decimal('0.65')).quantize(Decimal('1.'))
        return [p1, prize_total - p1]
    if count == 3:
        p1 = (prize_total * Decimal('0.5')).quantize(Decimal('1.'))
        p2 = (prize_total * Decimal('0.3')).quantize(Decimal('1.'))
        return [p1, p2, prize_total - p1 - p2]
    if count == 4:
        p1 = (prize_total * Decimal('0.4')).quantize(Decimal('1.'))
        p2 = (prize_total * Decimal('0.25')).quantize(Decimal('1.'))
        p3 = (prize_total * Decimal('0.2')).quantize(Decimal('1.'))
        return [p1, p2, p3, prize_total - p1 - p2 - p3]
    if count == 5:
        p1 = (prize_total * Decimal('0.35')).quantize(Decimal('1.'))
        p2 = (prize_total * Decimal('0.25')).quantize(Decimal('1.'))
        p3 = (prize_total * Decimal('0.18')).quantize(Decimal('1.'))
        p4 = (prize_total * Decimal('0.12')).quantize(Decimal('1.'))
        return [p1, p2, p3, p4, prize_total - p1 - p2 - p3 - p4]
    base = []
    percents = [0.35, 0.25, 0.18, 0.12, 0.10]
    for i in range(min(count, 5)):
        base.append((prize_total * Decimal(str(percents[i]))).quantize(Decimal('1.')))
    rest = prize_total - sum(base)
    rest_count = count - 5
    if rest_count > 0:
        rest_prize = (rest / rest_count).quantize(Decimal('1.'))
        prizes = base + [rest_prize] * rest_count
        prizes[-1] += prize_total - sum(prizes)
        return prizes
    return base

def to_moscow(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    return dt.astimezone(pytz.timezone("Europe/Moscow"))

async def format_contest_participants(db, contest_id: int) -> str:
    participants = await db.get_contest_participants(contest_id, limit=3)
    if not participants:
        return "Пока нет участников"
    return "Наш топ ⤵️\n" + "".join(
        f"№ {i}\nИгрок: " + (p.get('full_name') or p.get('username') or ('User ' + str(p.get('user_id')))) +
        f" (<a href=\"https://t.me/CasinoDepovBot?start=userstats_{p.get('user_id')}\">профиль</a>)\n"
        f"Сумма: {p.get('value')}$\n\n"
        for i, p in enumerate(participants, 1)
    )

async def format_contest_message(db, contest: dict) -> str:
    contest_type_text = "на самую крупную ставку" if contest.get("type") == "biggest_bet" else "на самый крупный оборот"
    top_limit = int(contest.get('top_limit', 3))
    participants = await db.get_contest_participants(contest.get("id"), limit=top_limit)
    prizes = get_prizes_list(contest.get('prize'), top_limit)
    description = (
        "Сделать самую крупную ставку до конца конкурса и попасть в топ!"
        if contest.get("type") == "biggest_bet"
        else "Набрать как можно больший оборот до конца конкурса и попасть в топ!"
    )
    message = ""
    if contest.get("status") == "completed":
        message += "❗️ <b>Конкурс завершён</b>\n\n"
    message += f"<b>🏛 Конкурс от Depov Casino {contest_type_text}!</b>\n\n"
    message += f"📝 <b>Суть конкурса:</b> {description}\n"
    message += f"💰 <b>Призовой фонд:</b> <b>{contest.get('prize')}$</b>\n"
    try:
        dt = datetime.fromisoformat(contest.get('end_time'))
        dt_msk = to_moscow(dt)
        end_time_str = dt_msk.strftime('%d.%m.%Y %H:%M (МСК)')
    except Exception:
        end_time_str = contest.get('end_time')
    message += f"🗓 <b>До:</b> <code>{end_time_str}</code>\n\n"
    message += "".join(f"<b>{i}. {prize}$</b>\n" for i, prize in enumerate(prizes, 1))
    message += "\n<b>🏆 Наш топ:</b>\n"
    if participants:
        message += "<blockquote>"
        for i in range(top_limit):
            if i < len(participants):
                p = participants[i]
                user_id = p.get("user_id")
                nickname = p.get("full_name") or p.get("username") or f"User {user_id}"
                value = p.get("value")
                message += f"<b>{i+1}.</b> <a href=\"https://t.me/CasinoDepovBot?start=userstats_{user_id}\">{nickname}</a> — <b>{value:.2f}$</b>\n"
            else:
                message += f"<b>{i+1}.</b> — <i>место свободно</i>\n"
        message += "</blockquote>"
    else:
        message += "<i>Пока нет участников</i>"
    return message

async def get_contest_keyboard(contest: dict) -> InlineKeyboardMarkup:
    contest_id = contest.get('id')
    is_completed = contest.get('status') == 'completed'
    buttons = [
        [InlineKeyboardButton(text="🏛 Сделать ставку", url=INVOICE_URL)],
        [InlineKeyboardButton(text="🎲 Сделать ставку через бота", url="https://t.me/CasinoDepovBot?start=games")],
        [InlineKeyboardButton(
            text="🔄 Обновить топ",
            callback_data=f"{'contest_finished' if is_completed else 'refresh_top'}_{contest_id}"
        )]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def update_contest_message(contest_id: int):
    """Обновляет сообщение конкурса в канале"""
    if not all([bot, db, BETS_ID]):
        logging.error("[CONTESTS] Модуль не инициализирован")
        return
    
    try:
        contest = await db.get_contest_by_id(contest_id)
        if not contest or not contest.get("channel_message_id"):
            return
        
        message_text = await format_contest_message(db, contest)
        keyboard = await get_contest_keyboard(contest)
        
        try:
            await bot.edit_message_caption(
                chat_id=BETS_ID,
                message_id=contest.get("channel_message_id"),
                caption=message_text,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        except Exception:
            try:
                await bot.edit_message_text(
                    message_text,
                    chat_id=BETS_ID,
                    message_id=contest.get("channel_message_id"),
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
            except Exception as e:
                logging.error(f"[CONTESTS] Ошибка обновления сообщения конкурса #{contest_id}: {e}")
    except Exception as e:
        logging.error(f"[CONTESTS] Критическая ошибка в update_contest_message: {e}", exc_info=True)

async def check_contests_schedule():
    """Основной цикл проверки и обновления конкурсов"""
    if not all([bot, db, BETS_ID]):
        logging.error("[CONTESTS] Модуль не инициализирован, цикл не запущен")
        return
    
    logging.info("[CONTESTS] Запущен цикл проверки конкурсов")
    last_top_update = {}
    
    while True:
        try:
            contests = await db.get_active_contests()
            now = datetime.now(pytz.UTC)
            
            for contest in contests:
                try:
                    end_time = datetime.fromisoformat(contest.get('end_time'))
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=pytz.UTC)
                except Exception as e:
                    logging.error(f"[CONTESTS] Ошибка парсинга времени окончания конкурса #{contest.get('id')}: {e}")
                    continue
                
                contest_id = contest.get('id')
                top_limit = int(contest.get('top_limit', 3)) if contest.get('top_limit') else 3
                
                # Проверяем, завершился ли конкурс
                if now >= end_time and contest.get('status') == 'active':
                    logging.info(f"[CONTESTS] Завершается конкурс #{contest_id}")
                    
                    # Получаем участников и призы
                    participants = await db.get_contest_participants(contest_id, top_limit)
                    prize = Decimal(contest.get('prize'))
                    winners_count = min(top_limit, len(participants))
                    winners = participants[:winners_count] if winners_count > 0 else []
                    prizes = get_prizes_list(prize, winners_count) if winners_count > 0 else []
                    
                    # Сохраняем победителей
                    winner_ids = [str(w.get('user_id')) for w in winners]
                    await db.complete_contest(contest_id, ",".join(winner_ids) if winner_ids else None)
                    
                    # Выдаем призы
                    for idx, winner in enumerate(winners):
                        user_id = winner.get('user_id')
                        user = await db.get_user(user_id)
                        if not user:
                            logging.warning(f"[CONTESTS] Пользователь {user_id} не найден для выдачи приза")
                            continue
                        
                        prize_for_winner = prizes[idx] if idx < len(prizes) else Decimal('0')
                        if prize_for_winner <= 0:
                            continue
                        
                        # Зачисляем приз
                        await db.update_balance(user_id, prize_for_winner)
                        await db.add_transaction(user_id, prize_for_winner, 'contest_prize', contest.get('type'))
                        
                        # Уведомляем победителя
                        try:
                            contest_name = contest.get('title') or ('Самая крупная ставка' if contest.get('type') == 'biggest_bet' else 'Самый крупный оборот')
                            await bot.send_message(
                                user_id,
                                f"<b>🎉 Поздравляем! Вы выиграли в конкурсе!</b>\n\n"
                                f"<b>📋 Конкурс:</b> {contest_name}\n"
                                f"<b>🏆 Место:</b> {idx+1}\n"
                                f"<b>💰 Приз:</b> {prize_for_winner:.2f}$\n\n"
                                f"Приз зачислен на ваш баланс!",
                                parse_mode="HTML"
                            )
                            logging.info(f"[CONTESTS] Приз {prize_for_winner:.2f}$ выдан пользователю {user_id} (место {idx+1})")
                        except Exception as e:
                            logging.error(f"[CONTESTS] Ошибка отправки уведомления победителю {user_id}: {e}")
                    
                    # Если не было участников
                    if not winners:
                        logging.info(f"[CONTESTS] Конкурс #{contest_id} завершён без участников")
                    
                    # Обновляем сообщение конкурса
                    await update_contest_message(contest_id)
                    last_top_update[contest_id] = now
                    
                else:
                    last_update = last_top_update.get(contest_id)
                    if not last_update or (now - last_update).total_seconds() > 3600:
                        await update_contest_message(contest_id)
                        last_top_update[contest_id] = now
                        
        except Exception as e:
            logging.error(f"[CONTESTS] Ошибка в цикле проверки конкурсов: {e}", exc_info=True)
        
        await asyncio.sleep(60)

async def process_bet_for_contests(user_id: int, amount: Decimal):
    """Обрабатывает ставку для всех активных конкурсов"""
    if not db:
        return
    
    try:
        contests = await db.get_active_contests()
        for contest in contests:
            try:
                contest_id = contest.get('id')
                contest_type = contest.get('type')
                
                # Обновляем участие в конкурсе
                await db.update_contest_participant(contest_id, user_id, float(amount), contest_type)
                
                logging.debug(f"[CONTESTS] Ставка {amount}$ от пользователя {user_id} обработана для конкурса #{contest_id}")
            except Exception as e:
                logging.error(f"[CONTESTS] Ошибка обработки ставки для конкурса #{contest.get('id')}: {e}")
                
    except Exception as e:
        logging.error(f"[CONTESTS] Критическая ошибка в process_bet_for_contests: {e}", exc_info=True)

router = Router()

@router.message(Command("newcontest"))
async def simple_create_contest(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Выберите тип конкурса:", reply_markup=create_contest_types_keyboard())
    await state.set_data({"step": "type"})

@router.callback_query(F.data.startswith("contest_type_"))
async def simple_type_selected(callback_query: types.CallbackQuery, state: FSMContext):
    contest_type = callback_query.data.replace("contest_type_", "")
    await state.update_data(type=contest_type, step="duration")
    await callback_query.message.edit_text("Введите длительность конкурса в минутах (например, 60):")
    await callback_query.answer()

@router.message(lambda m, s: s.get_data().get("step") == "duration")
async def simple_duration_entered(message: types.Message, state: FSMContext):
    """Обработка ввода длительности конкурса"""
    try:
        minutes = int(message.text)
        if not (1 <= minutes <= 10080):  # До 7 дней
            raise ValueError
    except Exception:
        await message.answer("❌ Введите число от 1 до 10080 (минуты, максимум 7 дней)")
        return
    
    await state.update_data(duration=minutes, step="prize")
    await message.answer("💰 Введите сумму приза (например, 100):")

@router.message(lambda m, s: s.get_data().get("step") == "prize")
async def simple_prize_entered(message: types.Message, state: FSMContext):
    """Обработка ввода суммы приза и создание конкурса"""
    if not all([bot, db, BETS_ID]):
        await message.answer("❌ Ошибка: модуль конкурсов не инициализирован")
        await state.clear()
        return
    
    try:
        prize = Decimal(message.text.replace(",", "."))
        if prize <= 0:
            raise ValueError
    except Exception:
        await message.answer("❌ Введите корректную сумму приза (например: 100 или 50.5)")
        return
    
    try:
        data = await state.get_data()
        contest_type = data.get("type")
        minutes = data.get("duration")
        
        # Вычисляем время окончания
        moscow_tz = pytz.timezone("Europe/Moscow")
        now_utc = datetime.now(pytz.UTC)
        now_msk = now_utc.astimezone(moscow_tz)
        end_time_msk = now_msk + timedelta(minutes=minutes)
        end_time_utc = end_time_msk.astimezone(pytz.UTC)
        end_time = end_time_utc.replace(tzinfo=pytz.UTC).isoformat()
        
        # Создаем конкурс в БД
        contest_id = await db.create_contest(
            type=contest_type,
            title=f"Конкурс {'на самую крупную ставку' if contest_type == 'biggest_bet' else 'на самый крупный оборот'}",
            description="",
            prize=str(prize),
            end_time=end_time,
            status='active'
        )
        
        # Формируем сообщение
        contest_data = {
            'id': contest_id,
            'type': contest_type,
            'prize': str(prize),
            'description': '',
            'end_time': end_time,
            'status': 'active',
            'winner_id': None,
            'top_limit': 3
        }
        
        message_text = await format_contest_message(db, contest_data)
        keyboard = await get_contest_keyboard(contest_data)
        
        # Публикуем в канале
        msg = await bot.send_message(
            chat_id=BETS_ID,
            text=message_text,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        
        # Сохраняем ID сообщения
        await db.set_contest_channel_message(contest_id, msg.message_id)
        
        await message.answer(
            f"✅ <b>Конкурс создан и опубликован!</b>\n\n"
            f"📋 ID конкурса: {contest_id}\n"
            f"💰 Приз: {prize}$\n"
            f"⏱ Длительность: {minutes} мин",
            parse_mode="HTML"
        )
        
        logging.info(f"[CONTESTS] Создан конкурс #{contest_id} типа {contest_type} с призом {prize}$ на {minutes} минут")
        
    except Exception as e:
        logging.error(f"[CONTESTS] Ошибка создания конкурса: {e}", exc_info=True)
        await message.answer("❌ Ошибка при создании конкурса. Попробуйте позже.")
    
    await state.clear()
