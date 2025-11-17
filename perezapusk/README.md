# @wmamed

## Установка

1. Создайте виртуальное окружение и установите зависимости:
   ```bash
   python -m venv venv
   source venv/bin/activate  # На Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. Скопируйте пример файла окружения:
   ```bash
   cp .env.example .env
   ```

3. Настройте конфигурацию в файле `.env`:
   - `BOT_TOKEN`: Токен Telegram бота от @BotFather
   - `CRYPTO_PAY_TOKEN`: Токен CryptoPay от @send
   - `ADMIN_USER_ID`: Ваш Telegram ID
   - `DATABASE_URL`: Путь к базе данных (database.db)

4. Запустите бота:
   ```bash
   python bot.py
   ```

# @wmamed

## Новое: Конкурсы (Contests)
- Поддержка конкурсов на самую крупную ставку и оборот
- Фото конкурса через ссылку (imgur)
- Красивый стиль Depov Casino
- Админка для управления конкурсами
- Топ участников с профилями