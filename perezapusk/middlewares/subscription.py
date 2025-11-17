import os
from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware, Bot
from aiogram.types import Update, InlineKeyboardMarkup, InlineKeyboardButton, User
from database import Database
import logging

class SubscriptionMiddleware(BaseMiddleware):
    def __init__(self, db: Database):
        self.db = db

    async def _send_subscription_prompt(self, event, not_subscribed):
        buttons = [
            [InlineKeyboardButton(text=ch['button_text'], url=ch['channel_url'])]
            for ch in not_subscribed
        ]
        buttons.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="check_subscription")])
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        text = "Для использования бота, пожалуйста, подпишитесь на наши каналы:"
        try:
            if getattr(event, "message", None):
                await event.message.answer(text, reply_markup=keyboard)
            elif getattr(event, "callback_query", None):
                await event.callback_query.message.answer(text, reply_markup=keyboard)
                await event.callback_query.answer()
        except Exception as e:
            logging.error(f"Failed to send subscription message: {e}")

    async def _answer_callback(self, callback_query, text, alert=False, delete_message=False):
        try:
            await callback_query.answer(text, show_alert=alert)
            if delete_message:
                await callback_query.message.delete()
        except Exception as e:
            logging.error(f"Failed to handle callback: {e}")

    async def __call__(
        self,
        handler: Callable[[Update, Dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: Dict[str, Any],
    ) -> Any:
        user: User = data.get("event_from_user")
        if not user:
            return await handler(event, data)

        admin_id = os.getenv("ADMIN_USER_ID")
        if admin_id and str(user.id) == admin_id:
            return await handler(event, data)

        channels = await self.db.get_subscription_channels()
        if not channels:
            return await handler(event, data)

        bot: Bot = data.get("bot")
        not_subscribed = []
        for channel in channels:
            try:
                member = await bot.get_chat_member(chat_id=channel['channel_id'], user_id=user.id)
                if member.status in ("left", "kicked"):
                    not_subscribed.append(channel)
            except Exception as e:
                logging.error(
                    f"Could not check subscription for channel {channel.get('channel_id')}. Assuming not subscribed. Error: {e}"
                )
                not_subscribed.append(channel)

        callback_query = getattr(event, "callback_query", None)
        callback_data = getattr(callback_query, "data", None) if callback_query else None

        if not_subscribed:
            if callback_data == "check_subscription":
                await self._answer_callback(callback_query, "Вы все еще не подписаны на все каналы.", alert=True)
                return
            await self._send_subscription_prompt(event, not_subscribed)
            return

        if callback_data == "check_subscription":
            await self._answer_callback(callback_query, "Спасибо за подписку!", alert=True, delete_message=True)
            return

        return await handler(event, data)