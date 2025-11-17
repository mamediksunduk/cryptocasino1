import aiosqlite
import os
from decimal import Decimal
from typing import Optional, List, Dict
import logging
from cryptopay import CryptoPayAPI
import sqlite3


class DatabaseError(Exception):
    """Base class for database-related exceptions."""


class InsufficientFundsError(DatabaseError):
    """Raised when a user does not have enough clean balance for an operation."""


class CheckNotFoundError(DatabaseError):
    """Raised when an operation references a non-existent check."""


class CheckAlreadyActivatedError(DatabaseError):
    """Raised when a user attempts to activate the same check twice."""


class CheckAlreadyCashedError(DatabaseError):
    """Raised when a check has already been cashed/fully activated."""


class CheckPermissionError(DatabaseError):
    """Raised when a user attempts to manipulate a check they do not own."""

def adapt_decimal(d: Decimal) -> str:
    return str(d)

def convert_decimal(b: bytes) -> Decimal:
    return Decimal(b.decode())

aiosqlite.register_adapter(Decimal, adapt_decimal)
aiosqlite.register_converter("DECIMAL", convert_decimal)

class Database:
    def __init__(self, db_path: str = "database.db"):
        self.db_path = db_path
        self.connect_params = {"detect_types": sqlite3.PARSE_DECLTYPES}

    async def _get_clean_balance_snapshot(self, db, user_id: int):
        async with db.execute(
            "SELECT balance, bonus_balance FROM users WHERE user_id = ?",
            (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if not row:
            return None, None, None
        balance = Decimal(str(row[0]))
        locked = Decimal(str(row[1] or 0))
        clean_balance = balance - locked
        if clean_balance < Decimal('0'):
            clean_balance = Decimal('0')
        return balance, locked, clean_balance

    async def _apply_bonus_lock_in_tx(self, db, user_id: int, amount: Decimal, multiplier: Decimal) -> Decimal:
        amount = Decimal(str(amount))
        multiplier = Decimal(str(multiplier))
        if amount <= 0 or multiplier <= 0:
            return Decimal('0')
        requirement = amount * multiplier
        await db.execute(
            """
            UPDATE users
            SET bonus_balance = COALESCE(bonus_balance, 0) + ?,
                bonus_wager_left = COALESCE(bonus_wager_left, 0) + ?,
                bonus_wager_total = COALESCE(bonus_wager_total, 0) + ?
            WHERE user_id = ?
            """,
            (amount, requirement, requirement, user_id)
        )
        return requirement

    async def _release_bonus_if_completed_in_tx(self, db, user_id: int, left: Decimal):
        if left <= 0:
            await db.execute(
                """
                UPDATE users
                SET bonus_wager_left = 0,
                    bonus_wager_total = 0,
                    bonus_balance = 0
                WHERE user_id = ?
                """,
                (user_id,)
            )

    async def _consume_bonus_wager_in_tx(self, db, user_id: int, bet_amount: Decimal):
        bet_amount = Decimal(str(bet_amount))
        if bet_amount <= 0:
            return
        async with db.execute(
            "SELECT bonus_wager_left, bonus_wager_total FROM users WHERE user_id = ?",
            (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if not row:
            return
        left = Decimal(str(row[0] or '0'))
        if left <= 0:
            return
        new_left = left - bet_amount
        if new_left <= 0:
            await self._release_bonus_if_completed_in_tx(db, user_id, Decimal('0'))
        else:
            await db.execute(
                "UPDATE users SET bonus_wager_left = ? WHERE user_id = ?",
                (new_left, user_id)
            )
    async def create_check_atomic(
        self,
        check_id: str,
        creator_id: int,
        amount: Decimal,
        *,
        target_user_id: Optional[int] = None,
        is_multi: bool = False,
        activations_total: int = 1,
        comment: Optional[str] = None
    ) -> Dict:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE")
            balance, locked, clean_balance = await self._get_clean_balance_snapshot(db, creator_id)
            if balance is None:
                await db.rollback()
                raise CheckPermissionError("USER_NOT_FOUND")
            amount_dec = Decimal(str(amount))
            if amount_dec <= 0:
                await db.rollback()
                raise ValueError("Amount must be positive")
            if amount_dec > clean_balance:
                await db.rollback()
                raise InsufficientFundsError("NOT_ENOUGH_FUNDS")
            if is_multi and activations_total < 2:
                await db.rollback()
                raise ValueError("Multi checks must have at least 2 activations")
            await db.execute(
                """
                INSERT INTO checks (
                    check_id, creator_id, amount, target_user_id, is_multi, activations_total, comment
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (check_id, creator_id, amount_dec, target_user_id, int(is_multi), activations_total, comment)
            )
            await db.execute(
                "UPDATE users SET balance = balance - ? WHERE user_id = ?",
                (amount_dec, creator_id)
            )
            await db.commit()
            async with db.execute("SELECT * FROM checks WHERE check_id = ?", (check_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else {}

    async def activate_check_atomic(self, check_id: str, user_id: int) -> Dict:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute("SELECT * FROM checks WHERE check_id = ?", (check_id,)) as cursor:
                row = await cursor.fetchone()
            if not row:
                await db.rollback()
                raise CheckNotFoundError("CHECK_NOT_FOUND")
            check = dict(row)
            if check.get("status") == "cashed":
                await db.rollback()
                raise CheckAlreadyCashedError("CHECK_ALREADY_CASHED")
            multiplier = Decimal(str(check.get("wagering_multiplier") or '0'))
            amount_total = Decimal(str(check.get("amount") or '0'))
            if amount_total <= 0:
                await db.rollback()
                raise ValueError("Invalid check amount")
            is_multi = bool(check.get("is_multi"))
            activations_total = int(check.get("activations_total") or 1)
            if activations_total <= 0:
                activations_total = 1
            remaining_activations = 0
            amount_to_credit = Decimal('0')
            credited_to_bonus = False
            requirement_added = Decimal('0')
            if is_multi:
                async with db.execute(
                    "SELECT 1 FROM check_activations WHERE check_id = ? AND user_id = ?",
                    (check_id, user_id)
                ) as cursor:
                    if await cursor.fetchone():
                        await db.rollback()
                        raise CheckAlreadyActivatedError("ALREADY_ACTIVATED")
                async with db.execute(
                    "SELECT COUNT(*) FROM check_activations WHERE check_id = ?",
                    (check_id,)
                ) as cursor:
                    activations_count = (await cursor.fetchone())[0]
                if activations_count >= activations_total:
                    await db.rollback()
                    raise CheckAlreadyCashedError("NO_ACTIVATIONS_LEFT")
                if user_id == check.get("creator_id"):
                    amount_to_credit = amount_total
                    await db.execute(
                        "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                        (amount_to_credit, user_id)
                    )
                    wagering_left = amount_to_credit * multiplier if multiplier > 0 else Decimal('0')
                    await db.execute(
                        "INSERT INTO check_activations (check_id, user_id, wagering_left, wagering_total) VALUES (?, ?, ?, ?)",
                        (check_id, user_id, wagering_left, wagering_left)
                    )
                    await db.execute(
                        "UPDATE checks SET status = 'cashed', cashed_by_id = ?, cashed_at = CURRENT_TIMESTAMP WHERE check_id = ?",
                        (user_id, check_id)
                    )
                    remaining_activations = 0
                else:
                    amount_to_credit = amount_total / Decimal(str(activations_total))
                    await db.execute(
                        "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                        (amount_to_credit, user_id)
                    )
                    wagering_left = amount_to_credit * multiplier if multiplier > 0 else Decimal('0')
                    await db.execute(
                        "INSERT INTO check_activations (check_id, user_id, wagering_left, wagering_total) VALUES (?, ?, ?, ?)",
                        (check_id, user_id, wagering_left, wagering_left)
                    )
                    activations_count += 1
                    remaining_activations = max(0, activations_total - activations_count)
                    if activations_count >= activations_total:
                        await db.execute(
                            "UPDATE checks SET status = 'cashed', cashed_by_id = 0, cashed_at = CURRENT_TIMESTAMP WHERE check_id = ?",
                            (check_id,)
                        )
            else:
                amount_to_credit = amount_total
                await db.execute(
                    "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                    (amount_to_credit, user_id)
                )
                wagering_left = amount_to_credit * multiplier if multiplier > 0 else Decimal('0')
                await db.execute(
                    "INSERT INTO check_activations (check_id, user_id, wagering_left, wagering_total) VALUES (?, ?, ?, ?)",
                    (check_id, user_id, wagering_left, wagering_left)
                )
                await db.execute(
                    "UPDATE checks SET status = 'cashed', cashed_by_id = ?, cashed_at = CURRENT_TIMESTAMP WHERE check_id = ?",
                    (user_id, check_id)
                )
                remaining_activations = 0
            if multiplier > 0 and amount_to_credit > 0:
                requirement_added = await self._apply_bonus_lock_in_tx(db, user_id, amount_to_credit, multiplier)
                credited_to_bonus = requirement_added > 0
            await db.commit()
            async with db.execute("SELECT * FROM checks WHERE check_id = ?", (check_id,)) as cursor:
                updated_check = dict(await cursor.fetchone())
            return {
                "check": updated_check,
                "amount": amount_to_credit,
                "remaining_activations": remaining_activations,
                "credited_to_bonus": credited_to_bonus,
                "wager_requirement": requirement_added
            }

    async def delete_check_with_refund(self, check_id: str, user_id: int) -> Decimal:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute("SELECT * FROM checks WHERE check_id = ?", (check_id,)) as cursor:
                row = await cursor.fetchone()
            if not row:
                await db.rollback()
                raise CheckNotFoundError("CHECK_NOT_FOUND")
            check = dict(row)
            if check.get("creator_id") != user_id:
                await db.rollback()
                raise CheckPermissionError("NOT_CREATOR")
            if check.get("status") == "cashed":
                await db.rollback()
                raise CheckAlreadyCashedError("CHECK_ALREADY_CASHED")
            refund_amount = Decimal('0')
            amount_total = Decimal(str(check.get("amount") or '0'))
            if check.get("is_multi"):
                activations_total = int(check.get("activations_total") or 1)
                if activations_total <= 0:
                    activations_total = 1
                async with db.execute(
                    "SELECT COUNT(*) FROM check_activations WHERE check_id = ?",
                    (check_id,)
                ) as cursor:
                    activations_count = (await cursor.fetchone())[0]
                amount_per_activation = amount_total / Decimal(str(activations_total))
                refund_amount = amount_total - (Decimal(activations_count) * amount_per_activation)
            else:
                refund_amount = amount_total
            if refund_amount < 0:
                refund_amount = Decimal('0')
            await db.execute("DELETE FROM check_activations WHERE check_id = ?", (check_id,))
            await db.execute("DELETE FROM checks WHERE check_id = ?", (check_id,))
            if refund_amount > 0:
                await db.execute(
                    "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                    (refund_amount, user_id)
                )
            await db.commit()
            return refund_amount

    async def recalc_all_user_stats(self):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            async with db.execute("SELECT user_id FROM users") as cursor:
                user_ids = [row[0] for row in await cursor.fetchall()]
            for user_id in user_ids:
                await self.get_user_stats(user_id)
            await db.commit()

    async def init(self):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    full_name TEXT,
                    balance DECIMAL DEFAULT '0.0',
                    bonus_balance DECIMAL DEFAULT '0.0',
                    bonus_wager_left DECIMAL DEFAULT '0.0',
                    bonus_wager_total DECIMAL DEFAULT '0.0',
                    ref_balance DECIMAL DEFAULT '0.0',
                    ref_earnings DECIMAL DEFAULT '0.0',
                    ref_count INTEGER DEFAULT 0,
                    referrer_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_claimed_turnover DECIMAL DEFAULT '0.0',
                    FOREIGN KEY (referrer_id) REFERENCES users(user_id)
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount DECIMAL,
                    type TEXT,
                    game_type TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount DECIMAL,
                    game TEXT,
                    bet_type TEXT,
                    is_bonus_bet INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount DECIMAL,
                    network TEXT,
                    address TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS bets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount DECIMAL,
                    game_type TEXT,
                    bet_type TEXT,
                    is_bonus_bet INTEGER DEFAULT 0,
                    message_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed INTEGER DEFAULT 0,
                    processed_at TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS win_check_tokens (
                    token TEXT PRIMARY KEY,
                    user_id INTEGER,
                    amount DECIMAL,
                    used INTEGER DEFAULT 0
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS processed_invoices (
                    invoice_id TEXT PRIMARY KEY,
                    user_id INTEGER,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS checks (
                    check_id TEXT PRIMARY KEY,
                    creator_id INTEGER,
                    amount DECIMAL,
                    status TEXT DEFAULT 'active',
                    cashed_by_id INTEGER,
                    target_user_id INTEGER,
                    is_multi BOOLEAN DEFAULT 0,
                    activations_total INTEGER DEFAULT 1,
                    password TEXT,
                    required_turnover DECIMAL DEFAULT '0',
                    premium_only BOOLEAN DEFAULT 0,
                    wagering_multiplier DECIMAL DEFAULT 0,
                    wagering_left DECIMAL DEFAULT 0,
                    comment TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    cashed_at TIMESTAMP,
                    FOREIGN KEY (creator_id) REFERENCES users(user_id),
                    FOREIGN KEY (cashed_by_id) REFERENCES users(user_id),
                    FOREIGN KEY (target_user_id) REFERENCES users(user_id)
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS check_activations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    check_id TEXT,
                    user_id INTEGER,
                    activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    wagering_left DECIMAL DEFAULT 0,
                    wagering_total DECIMAL DEFAULT 0,
                    FOREIGN KEY (check_id) REFERENCES checks(check_id),
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    UNIQUE(check_id, user_id)
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS contests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type TEXT,
                    title TEXT,
                    description TEXT,
                    prize TEXT,
                    end_time TEXT,
                    status TEXT DEFAULT 'active',
                    winner_id INTEGER,
                    channel_message_id INTEGER
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS contest_participants (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contest_id INTEGER,
                    user_id INTEGER,
                    value REAL DEFAULT 0,
                    UNIQUE(contest_id, user_id)
                )
            """)
            async with db.execute("PRAGMA table_info(checks)") as cursor:
                columns = [row[1] for row in await cursor.fetchall()]
                if 'password' not in columns:
                    await db.execute("ALTER TABLE checks ADD COLUMN password TEXT")
                if 'required_turnover' not in columns:
                    await db.execute("ALTER TABLE checks ADD COLUMN required_turnover DECIMAL DEFAULT '0'")
                if 'premium_only' not in columns:
                    await db.execute("ALTER TABLE checks ADD COLUMN premium_only BOOLEAN DEFAULT 0")
                if 'wagering_multiplier' not in columns:
                    await db.execute("ALTER TABLE checks ADD COLUMN wagering_multiplier DECIMAL DEFAULT 0")
                if 'wagering_left' not in columns:
                    await db.execute("ALTER TABLE checks ADD COLUMN wagering_left DECIMAL DEFAULT 0")
                if 'comment' not in columns:
                    await db.execute("ALTER TABLE checks ADD COLUMN comment TEXT")
                await db.commit()
            await db.execute("""
                CREATE TABLE IF NOT EXISTS subscription_channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id INTEGER NOT NULL UNIQUE,
                    channel_url TEXT NOT NULL,
                    button_text TEXT NOT NULL
                )
            """)
            is_migration_needed = False
            try:
                async with db.execute("PRAGMA table_info(checks)") as cursor:
                    columns = [row[1] for row in await cursor.fetchall()]
                    if 'target_user_id' not in columns or 'is_multi' not in columns:
                        is_migration_needed = True
            except aiosqlite.OperationalError as e:
                if "no such table: checks" not in str(e):
                    raise
            if is_migration_needed:
                logging.info("Old 'checks' table detected. Migrating schema by recreating it.")
                await db.execute("DROP TABLE IF EXISTS checks;")
                await db.execute("DROP TABLE IF EXISTS check_activations;")
                await db.execute("""
                    CREATE TABLE checks (
                        check_id TEXT PRIMARY KEY,
                        creator_id INTEGER,
                        amount DECIMAL,
                        status TEXT DEFAULT 'active',
                        cashed_by_id INTEGER,
                        target_user_id INTEGER,
                        is_multi BOOLEAN DEFAULT 0,
                        activations_total INTEGER DEFAULT 1,
                        password TEXT,
                        required_turnover DECIMAL DEFAULT '0',
                        premium_only BOOLEAN DEFAULT 0,
                        wagering_multiplier DECIMAL DEFAULT 0,
                        wagering_left DECIMAL DEFAULT 0,
                        comment TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        cashed_at TIMESTAMP,
                        FOREIGN KEY (creator_id) REFERENCES users(user_id),
                        FOREIGN KEY (cashed_by_id) REFERENCES users(user_id),
                        FOREIGN KEY (target_user_id) REFERENCES users(user_id)
                    )
                """)
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS check_activations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        check_id TEXT,
                        user_id INTEGER,
                        activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        wagering_left DECIMAL DEFAULT 0,
                        wagering_total DECIMAL DEFAULT 0,
                        FOREIGN KEY (check_id) REFERENCES checks(check_id),
                        FOREIGN KEY (user_id) REFERENCES users(user_id),
                        UNIQUE(check_id, user_id)
                    )
                """)
                logging.info("'checks' table recreated with new schema.")
            async with db.execute("PRAGMA table_info(users)") as cursor:
                columns = [row[1] for row in await cursor.fetchall()]
                if 'last_claimed_turnover' not in columns:
                    await db.execute("ALTER TABLE users ADD COLUMN last_claimed_turnover DECIMAL DEFAULT '0.0'")
                    await db.commit()
                if 'full_name' not in columns:
                    await db.execute("ALTER TABLE users ADD COLUMN full_name TEXT")
                    await db.commit()
                if 'bonus_balance' not in columns:
                    await db.execute("ALTER TABLE users ADD COLUMN bonus_balance DECIMAL DEFAULT '0.0'")
                if 'bonus_wager_left' not in columns:
                    await db.execute("ALTER TABLE users ADD COLUMN bonus_wager_left DECIMAL DEFAULT '0.0'")
                if 'bonus_wager_total' not in columns:
                    await db.execute("ALTER TABLE users ADD COLUMN bonus_wager_total DECIMAL DEFAULT '0.0'")
                await db.commit()
            await db.commit()
            async with db.execute("PRAGMA table_info(queue)") as cursor:
                queue_columns = [row[1] for row in await cursor.fetchall()]
                if 'is_bonus_bet' not in queue_columns:
                    await db.execute("ALTER TABLE queue ADD COLUMN is_bonus_bet INTEGER DEFAULT 0")
                    await db.commit()
            async with db.execute("PRAGMA table_info(contests)") as cursor:
                columns = [row[1] for row in await cursor.fetchall()]
                if 'top_limit' not in columns:
                    await db.execute("ALTER TABLE contests ADD COLUMN top_limit INTEGER DEFAULT 3")
            await db.commit()
            async with db.execute("PRAGMA table_info(bets)") as cursor:
                columns = [row[1] for row in await cursor.fetchall()]
                if 'bet_type' not in columns:
                    await db.execute("ALTER TABLE bets ADD COLUMN bet_type TEXT")
                    await db.commit()
                if 'is_bonus_bet' not in columns:
                    await db.execute("ALTER TABLE bets ADD COLUMN is_bonus_bet INTEGER DEFAULT 0")
                    await db.commit()
            async with db.execute("PRAGMA table_info(check_activations)") as cursor:
                columns = [row[1] for row in await cursor.fetchall()]
                if 'wagering_left' not in columns:
                    await db.execute("ALTER TABLE check_activations ADD COLUMN wagering_left DECIMAL DEFAULT 0")
                if 'wagering_total' not in columns:
                    await db.execute("ALTER TABLE check_activations ADD COLUMN wagering_total DECIMAL DEFAULT 0")
            await db.commit()

    async def get_user(self, user_id: int) -> Optional[Dict]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def create_user(self, user_id: int, username: str, full_name: str = None, referrer_id: Optional[int] = None) -> None:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                "INSERT OR REPLACE INTO users (user_id, username, full_name, referrer_id) VALUES (?, ?, ?, ?)",
                (user_id, username, full_name, referrer_id)
            )
            await db.commit()
            if referrer_id:
                await self.update_ref_count(referrer_id, 1)

    async def update_balance(self, user_id: int, amount: Decimal) -> bool:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                """
                UPDATE users
                SET balance = MAX(balance + ?, 0)
                WHERE user_id = ?
                """,
                (amount, user_id)
            )
            await db.commit()
            return True

    async def deduct_bonus_funds(self, user_id: int, amount: Decimal) -> bool:
        amount = Decimal(str(amount))
        if amount <= 0:
            return True
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute(
                "SELECT balance, bonus_balance FROM users WHERE user_id = ?",
                (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
            if not row:
                await db.rollback()
                return False
            balance = Decimal(str(row['balance'] or '0'))
            bonus_balance = Decimal(str(row['bonus_balance'] or '0'))
            if balance < amount or bonus_balance < amount:
                await db.rollback()
                return False
            await db.execute(
                """
                UPDATE users
                SET balance = balance - ?,
                    bonus_balance = bonus_balance - ?
                WHERE user_id = ?
                """,
                (amount, amount, user_id)
            )
            await db.commit()
            return True

    async def refund_bonus_funds(self, user_id: int, amount: Decimal) -> bool:
        amount = Decimal(str(amount))
        if amount <= 0:
            return True
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                """
                UPDATE users
                SET balance = balance + ?,
                    bonus_balance = bonus_balance + ?
                WHERE user_id = ?
                """,
                (amount, amount, user_id)
            )
            await db.commit()
            return True

    async def increase_bonus_balance(self, user_id: int, amount: Decimal) -> bool:
        amount = Decimal(str(amount))
        if amount == 0:
            return True
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                """
                UPDATE users
                SET bonus_balance = CASE
                    WHEN COALESCE(bonus_balance, 0) + ? < 0 THEN 0
                    ELSE COALESCE(bonus_balance, 0) + ?
                END
                WHERE user_id = ?
                """,
                (amount, amount, user_id)
            )
            await db.commit()
            return True

    async def update_ref_balance(self, user_id: int, amount: Decimal) -> bool:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            if amount > 0:
                await db.execute(
                    "UPDATE users SET ref_balance = ref_balance + ?, ref_earnings = ref_earnings + ? WHERE user_id = ?",
                    (amount, amount, user_id)
                )
            else:
                await db.execute(
                    "UPDATE users SET ref_balance = ref_balance + ? WHERE user_id = ?",
                    (amount, user_id)
                )
            await db.commit()
            return True

    async def update_ref_count(self, user_id: int, count_increment: int) -> bool:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                "UPDATE users SET ref_count = ref_count + ? WHERE user_id = ?",
                (count_increment, user_id)
            )
            await db.commit()
            return True

    async def get_referrer(self, user_id: int) -> Optional[int]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            async with db.execute("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                return row[0] if row and row[0] else None

    async def add_to_queue(self, user_id: int, amount: Decimal, game: str, bet_type: str, is_bonus_bet: bool = False) -> int:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            cursor = await db.execute(
                "INSERT INTO queue (user_id, amount, game, bet_type, is_bonus_bet) VALUES (?, ?, ?, ?, ?)",
                (user_id, amount, game, bet_type, 1 if is_bonus_bet else 0)
            )
            await db.commit()
            return cursor.lastrowid

    async def get_next_bet(self) -> Optional[Dict]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM queue WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1"
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def mark_bet_processed(self, bet_id: int) -> bool:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                "UPDATE bets SET processed = 1, processed_at = datetime('now') WHERE id = ?",
                (bet_id,)
            )
            await db.commit()
            return True

    async def add_transaction(self, user_id: int, amount: Decimal, type: str, game_type: Optional[str] = None) -> None:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                "INSERT INTO transactions (user_id, amount, type, game_type) VALUES (?, ?, ?, ?)",
                (user_id, amount, type, game_type)
            )
            await db.commit()

    async def get_user_transactions(self, user_id: int, limit: int = 10) -> List[Dict]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM transactions WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
                (user_id, limit)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_user_stats(self, user_id: int) -> dict:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM transactions WHERE user_id = ? AND type = 'game'", (user_id,))
            total_games = (await cursor.fetchone())[0] or 0
            cursor = await db.execute("SELECT COUNT(*) FROM transactions WHERE user_id = ? AND type = 'win'", (user_id,))
            wins = (await cursor.fetchone())[0] or 0
            losses = total_games - wins
            cursor = await db.execute("SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE user_id = ? AND type = 'win'", (user_id,))
            total_won = Decimal(str((await cursor.fetchone())[0] or '0'))
            cursor = await db.execute("SELECT COALESCE(SUM(ABS(amount)), 0) FROM transactions WHERE user_id = ? AND type = 'game'", (user_id,))
            turnover = Decimal(str((await cursor.fetchone())[0] or '0'))
            cursor = await db.execute("SELECT COALESCE(SUM(ABS(amount)), 0) FROM transactions WHERE user_id = ? AND type = 'game' AND amount < 0", (user_id,))
            total_lost = Decimal(str((await cursor.fetchone())[0] or '0'))
            win_rate = (wins / total_games * 100) if total_games > 0 else 0
            return {
                'total_games': total_games,
                'wins': wins,
                'losses': losses,
                'win_rate': win_rate,
                'turnover': turnover,
                'total_won': total_won,
                'total_lost': total_lost
            }

    async def create_withdrawal(self, user_id: int, amount: Decimal, network: str, address: str) -> int:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            cursor = await db.execute(
                "INSERT INTO withdrawals (user_id, amount, network, address) VALUES (?, ?, ?, ?)",
                (user_id, amount, network, address)
            )
            await db.commit()
            return cursor.lastrowid

    async def get_pending_withdrawals(self) -> List[Dict]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT w.*, u.username FROM withdrawals w JOIN users u ON w.user_id = u.user_id WHERE w.status = 'pending' ORDER BY w.created_at ASC"
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def mark_withdrawal_processed(self, withdrawal_id: int) -> None:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                "UPDATE withdrawals SET status = 'processed', processed_at = CURRENT_TIMESTAMP WHERE id = ?",
                (withdrawal_id,)
            )
            await db.commit()

    async def cancel_withdrawal(self, withdrawal_id: int) -> None:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            async with db.execute("SELECT user_id, amount FROM withdrawals WHERE id = ?", (withdrawal_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    user_id, amount = row
                    await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
                    await db.execute(
                        "UPDATE withdrawals SET status = 'cancelled', processed_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (withdrawal_id,)
                    )
                    await db.commit()

    async def get_user_withdrawals(self, user_id: int, limit: int = 10) -> List[Dict]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM withdrawals WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
                (user_id, limit)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_admin_stats(self) -> Dict:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            stats = {}
            async with db.execute(
                """
                SELECT 
                    COUNT(*) as total_users,
                    SUM(CASE WHEN date(created_at) >= date('now', 'start of day') THEN 1 ELSE 0 END) as today_users,
                    SUM(CASE WHEN date(created_at) >= date('now', '-6 days', 'start of day') THEN 1 ELSE 0 END) as week_users
                FROM users
                """
            ) as cursor:
                user_stats = await cursor.fetchone()
                stats.update(dict(user_stats) if user_stats else {'total_users': 0, 'today_users': 0, 'week_users': 0})
            async with db.execute(
                """
                SELECT 
                    COUNT(CASE WHEN type = 'game' THEN 1 END) as games,
                    COUNT(CASE WHEN type = 'win' THEN 1 END) as wins,
                    COALESCE(SUM(CASE WHEN type = 'win' THEN amount ELSE 0 END), 0) as winnings,
                    COALESCE(SUM(CASE WHEN type = 'game' THEN ABS(amount) ELSE 0 END), 0) as turnover
                FROM transactions 
                WHERE date(created_at) >= date('now', 'start of day') AND type IN ('game', 'win')
                """
            ) as cursor:
                today_stats_raw = await cursor.fetchone()
                if today_stats_raw and today_stats_raw['games'] is not None:
                    stats['today_games'] = today_stats_raw['games']
                    stats['today_wins'] = today_stats_raw['wins']
                    stats['today_losses'] = max(0, stats['today_games'] - stats['today_wins'])
                    today_turnover = Decimal(str(today_stats_raw['turnover']))
                    today_winnings = Decimal(str(today_stats_raw['winnings']))
                    stats['today_turnover'] = today_turnover
                    stats['today_profit'] = today_turnover - today_winnings
                else:
                    stats.update({'today_games': 0, 'today_wins': 0, 'today_losses': 0, 'today_turnover': Decimal('0'), 'today_profit': Decimal('0')})
            async with db.execute(
                """
                SELECT 
                    COUNT(CASE WHEN type = 'game' THEN 1 END) as games,
                    COUNT(CASE WHEN type = 'win' THEN 1 END) as wins,
                    COALESCE(SUM(CASE WHEN type = 'win' THEN amount ELSE 0 END), 0) as winnings,
                    COALESCE(SUM(CASE WHEN type = 'game' THEN ABS(amount) ELSE 0 END), 0) as turnover
                FROM transactions 
                WHERE date(created_at) >= date('now', '-6 days', 'start of day') AND type IN ('game', 'win')
                """
            ) as cursor:
                week_stats_raw = await cursor.fetchone()
                if week_stats_raw and week_stats_raw['games'] is not None:
                    stats['week_games'] = week_stats_raw['games']
                    stats['week_wins'] = week_stats_raw['wins']
                    stats['week_losses'] = max(0, stats['week_games'] - stats['week_wins'])
                    week_turnover = Decimal(str(week_stats_raw['turnover']))
                    week_winnings = Decimal(str(week_stats_raw['winnings']))
                    stats['week_turnover'] = week_turnover
                    stats['week_profit'] = week_turnover - week_winnings
                else:
                    stats.update({'week_games': 0, 'week_wins': 0, 'week_losses': 0, 'week_turnover': Decimal('0'), 'week_profit': Decimal('0')})
            return stats

    async def get_all_users(self, limit: int = 100, offset: int = 0) -> List[Dict]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT u.*, (SELECT username FROM users WHERE user_id = u.referrer_id) as referrer_username FROM users u ORDER BY u.created_at DESC LIMIT ? OFFSET ?",
                (limit, offset)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def update_user(self, user_id: int, updates: Dict) -> bool:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            fields = [f"{key} = ?" for key in updates]
            values = list(updates.values())
            if not fields:
                return False
            query = f"UPDATE users SET {', '.join(fields)} WHERE user_id = ?"
            values.append(user_id)
            await db.execute(query, values)
            await db.commit()
            return True

    async def delete_user(self, user_id: int) -> bool:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute("DELETE FROM transactions WHERE user_id = ?", (user_id,))
            await db.execute("DELETE FROM withdrawals WHERE user_id = ?", (user_id,))
            await db.execute("DELETE FROM queue WHERE user_id = ?", (user_id,))
            await db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
            await db.commit()
            return True

    async def search_users(self, query: str) -> List[Dict]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT u.*, (SELECT username FROM users WHERE user_id = u.referrer_id) as referrer_username FROM users u WHERE u.username LIKE ? OR CAST(u.user_id AS TEXT) LIKE ? ORDER BY u.created_at DESC LIMIT 50",
                (f"%{query}%", f"%{query}%")
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def add_bet(self, user_id: int, amount: Decimal, game_type: str, bet_type: str, message_id: int, is_bonus_bet: bool = False) -> int:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            cursor = await db.execute(
                "INSERT INTO bets (user_id, amount, game_type, bet_type, is_bonus_bet, message_id, created_at, processed) VALUES (?, ?, ?, ?, ?, ?, datetime('now'), 0)",
                (user_id, amount, game_type, bet_type, 1 if is_bonus_bet else 0, message_id)
            )
            await db.commit()
            return cursor.lastrowid

    async def get_current_balance(self) -> Decimal:
        try:
            crypto_pay = CryptoPayAPI(os.getenv('CRYPTO_PAY_TOKEN'), testnet=False)
            balance_data = await crypto_pay.get_balance()
            balances = balance_data.get('result', [])
            for balance in balances:
                if balance.get('currency_code', '').upper() == 'USDT':
                    return Decimal(balance.get('available', '0'))
            return Decimal('0')
        except Exception as e:
            logging.error(f"Error getting current balance: {e}")
            return Decimal('0')

    async def save_win_check_token(self, token: str, user_id: int, amount: Decimal):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                "INSERT OR REPLACE INTO win_check_tokens (token, user_id, amount, used) VALUES (?, ?, ?, 0)",
                (token, user_id, amount)
            )
            await db.commit()

    async def get_win_check_token(self, token: str):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM win_check_tokens WHERE token = ? AND used = 0",
                (token,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def mark_win_check_token_used(self, token: str):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                "UPDATE win_check_tokens SET used = 1 WHERE token = ?",
                (token,)
            )
            await db.commit()

    async def get_bet_by_invoice(self, invoice_id: str):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM processed_invoices WHERE invoice_id = ?",
                (invoice_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def mark_invoice_processed(self, invoice_id: str, user_id: int):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                "INSERT OR IGNORE INTO processed_invoices (invoice_id, user_id) VALUES (?, ?)",
                (invoice_id, user_id)
            )
            await db.commit()

    async def create_check(self, check_id: str, creator_id: int, amount: Decimal, target_user_id: Optional[int] = None, is_multi: bool = False, activations_total: int = 1, comment: Optional[str] = None) -> None:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                "INSERT INTO checks (check_id, creator_id, amount, target_user_id, is_multi, activations_total, comment) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (check_id, creator_id, amount, target_user_id, is_multi, activations_total, comment)
            )
            await db.commit()

    async def get_check(self, check_id: str) -> Optional[Dict]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM checks WHERE check_id = ?", (check_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def cash_check(self, check_id: str, cashed_by_id: int) -> None:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                "UPDATE checks SET status = 'cashed', cashed_by_id = ?, cashed_at = CURRENT_TIMESTAMP WHERE check_id = ?",
                (cashed_by_id, check_id)
            )
            await db.commit()

    async def clean_empty_wagerings(self, user_id: int):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                """
                UPDATE users
                SET bonus_wager_left = 0,
                    bonus_wager_total = 0,
                    bonus_balance = 0
                WHERE user_id = ? AND bonus_wager_left <= 0
                """,
                (user_id,)
            )
            await db.commit()

    async def add_check_activation(self, check_id: str, user_id: int, wagering_left: Decimal = Decimal('0'), wagering_total: Decimal = Decimal('0')):
        await self.clean_empty_wagerings(user_id)
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                "INSERT INTO check_activations (check_id, user_id, wagering_left, wagering_total) VALUES (?, ?, ?, ?)",
                (check_id, user_id, wagering_left, wagering_total)
            )
            await db.commit()

    async def get_check_activations_count(self, check_id: str) -> int:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            async with db.execute("SELECT COUNT(*) FROM check_activations WHERE check_id = ?", (check_id,)) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    async def has_user_activated_check(self, check_id: str, user_id: int) -> bool:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            async with db.execute("SELECT 1 FROM check_activations WHERE check_id = ? AND user_id = ?", (check_id, user_id)) as cursor:
                return await cursor.fetchone() is not None

    async def get_user_by_username(self, username: str) -> Optional[Dict]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM users WHERE username = ?",
                (username,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def update_check_settings(self, check_id: str, settings: Dict) -> bool:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            allowed_fields = ['password', 'required_turnover', 'premium_only', 'wagering_multiplier', 'wagering_left', 'comment', 'target_user_id']
            fields = [f"{key} = ?" for key in settings if key in allowed_fields]
            values = [settings[key] for key in settings if key in allowed_fields]
            if not fields:
                return False
            query = f"UPDATE checks SET {', '.join(fields)} WHERE check_id = ?"
            values.append(check_id)
            await db.execute(query, values)
            await db.commit()
            return True

    async def get_user_checks(self, creator_id: int, limit: int = 5, offset: int = 0) -> List[Dict]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM checks WHERE creator_id = ? AND status = 'active' ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (creator_id, limit, offset)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_top_users_by_turnover(self, period: str, limit: int = 10) -> List[Dict]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            period_filter = ""
            if period == 'today':
                period_filter = "AND date(t.created_at) = date('now')"
            elif period == 'week':
                period_filter = "AND date(t.created_at) >= date('now', '-7 days')"
            query = f"""
                SELECT 
                    u.user_id,
                    u.username,
                    COALESCE(SUM(ABS(t.amount)), 0.0) as total_turnover
                FROM transactions t
                JOIN users u ON t.user_id = u.user_id
                WHERE t.type = 'game'
                {period_filter}
                GROUP BY u.user_id
                HAVING total_turnover > 0
                ORDER BY CAST(total_turnover AS REAL) DESC
                LIMIT ?
            """
            async with db.execute(query, (limit,)) as cursor:
                rows = await cursor.fetchall()
                return [
                    {**dict(row), 'total_turnover': Decimal(str(row['total_turnover']))}
                    for row in rows
                ]

    async def get_top_users_by_referrals(self, period: str, limit: int = 10) -> List[Dict]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            if period == 'all':
                join_clause = "LEFT JOIN users r ON r.referrer_id = u.user_id"
            else:
                if period == 'today':
                    date_filter = "date(r.created_at) = date('now')"
                elif period == 'week':
                    date_filter = "date(r.created_at) >= date('now', '-7 days')"
                elif period == 'month':
                    date_filter = "date(r.created_at) >= date('now', '-1 month')"
                else:
                    date_filter = "1=1"
                join_clause = f"LEFT JOIN users r ON r.referrer_id = u.user_id AND {date_filter}"
            query = f'''
                SELECT 
                    u.user_id,
                    u.username,
                    COUNT(r.user_id) as referral_count
                FROM users u
                {join_clause}
                GROUP BY u.user_id
                HAVING referral_count > 0
                ORDER BY referral_count DESC
                LIMIT ?
            '''
            async with db.execute(query, (limit,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def add_subscription_channel(self, channel_id: int, channel_url: str, button_text: str):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                "INSERT INTO subscription_channels (channel_id, channel_url, button_text) VALUES (?, ?, ?)",
                (channel_id, channel_url, button_text)
            )
            await db.commit()

    async def get_subscription_channels(self) -> List[Dict]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM subscription_channels") as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def delete_subscription_channel(self, channel_id: int):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute("DELETE FROM subscription_channels WHERE channel_id = ?", (channel_id,))
            await db.commit()

    async def delete_check(self, check_id: str) -> bool:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute("DELETE FROM check_activations WHERE check_id = ?", (check_id,))
            await db.execute("DELETE FROM checks WHERE check_id = ?", (check_id,))
            await db.commit()
            return True

    async def get_user_pending_bet(self, user_id: int) -> Optional[Dict]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM queue WHERE user_id = ? AND status = 'pending' LIMIT 1",
                (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def mark_user_pending_bets_processed(self, user_id: int):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                "UPDATE queue SET status = 'processed' WHERE user_id = ? AND status = 'pending'",
                (user_id,)
            )
            await db.commit()

    async def clear_all_pending_bets(self):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute("UPDATE queue SET status = 'processed' WHERE status = 'pending'")
            await db.commit()

    async def mark_queue_bet_processed(self, queue_id: int):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute(
                "UPDATE queue SET status = 'processed' WHERE id = ?",
                (queue_id,)
            )
            await db.commit()

    async def clear_all_user_balances(self):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute("UPDATE users SET balance = 0")
            await db.commit()

    async def create_contest(self, type, title, description, prize, end_time, status='active'):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            cursor = await db.execute(
                "INSERT INTO contests (type, title, description, prize, end_time, status) VALUES (?, ?, ?, ?, ?, ?)",
                (type, title, description, prize, end_time, status)
            )
            await db.commit()
            return cursor.lastrowid

    async def get_contest_by_id(self, contest_id):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM contests WHERE id = ?", (contest_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def get_active_contests(self):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM contests WHERE status = 'active'") as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def get_completed_contests(self):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM contests WHERE status = 'completed'") as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def set_contest_channel_message(self, contest_id, message_id):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute("UPDATE contests SET channel_message_id = ? WHERE id = ?", (message_id, contest_id))
            await db.commit()

    async def update_contest_participant(self, contest_id, user_id, value, contest_type):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            if contest_type == 'biggest_bet':
                await db.execute(
                    """
                    INSERT INTO contest_participants (contest_id, user_id, value)
                    VALUES (?, ?, ?)
                    ON CONFLICT(contest_id, user_id) DO UPDATE SET
                        value = MAX(contest_participants.value, excluded.value)
                    """,
                    (contest_id, user_id, value)
                )
            else:
                await db.execute(
                    """
                    INSERT INTO contest_participants (contest_id, user_id, value)
                    VALUES (?, ?, ?)
                    ON CONFLICT(contest_id, user_id) DO UPDATE SET
                        value = contest_participants.value + excluded.value
                    """,
                    (contest_id, user_id, value)
                )
            await db.commit()

    async def get_contest_participants(self, contest_id, limit=3):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT cp.user_id, cp.value, u.username, u.full_name FROM contest_participants cp JOIN users u ON cp.user_id = u.user_id WHERE cp.contest_id = ? ORDER BY cp.value DESC LIMIT ?",
                (contest_id, limit)
            ) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def get_contest_winner(self, contest_id, contest_type):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT user_id, value FROM contest_participants WHERE contest_id = ? ORDER BY value DESC LIMIT 1", (contest_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def complete_contest(self, contest_id, winner_id):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute("UPDATE contests SET status = 'completed', winner_id = ? WHERE id = ?", (winner_id, contest_id))
            await db.commit()

    async def delete_contest(self, contest_id: int) -> bool:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute("DELETE FROM contest_participants WHERE contest_id = ?", (contest_id,))
            await db.execute("DELETE FROM contests WHERE id = ?", (contest_id,))
            await db.commit()
            return True

    async def update_contest_settings(self, contest_id: int, settings: Dict) -> bool:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            fields = [f"{key} = ?" for key in settings if key in ['top_limit', 'custom_link', 'bet_channel_url', 'bot_deeplink']]
            values = [settings[key] for key in settings if key in ['top_limit', 'custom_link', 'bet_channel_url', 'bot_deeplink']]
            if not fields:
                return False
            query = f"UPDATE contests SET {', '.join(fields)} WHERE id = ?"
            values.append(contest_id)
            await db.execute(query, values)
            await db.commit()
            return True

    async def get_users_invited_by(self, referrer_id: int) -> list:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT user_id, username FROM users WHERE referrer_id = ? ORDER BY created_at ASC",
                (referrer_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def debug_referral_system(self) -> dict:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            total_users = (await cursor.fetchone())[0]
            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE ref_count > 0")
            users_with_refs = (await cursor.fetchone())[0]
            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE referrer_id IS NOT NULL")
            users_as_refs = (await cursor.fetchone())[0]
            cursor = await db.execute(
                "SELECT user_id, username, ref_count FROM users WHERE ref_count > 0 ORDER BY ref_count DESC LIMIT 5"
            )
            top_refs = await cursor.fetchall()
            return {
                'total_users': total_users,
                'users_with_refs': users_with_refs,
                'users_as_refs': users_as_refs,
                'top_refs': [{'user_id': row[0], 'username': row[1], 'ref_count': row[2]} for row in top_refs]
            }

    async def get_last_bet(self, user_id: int) -> Optional[Dict]:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM bets WHERE user_id = ? ORDER BY created_at DESC LIMIT 1",
                (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def count_user_checks(self, creator_id: int) -> int:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM checks WHERE creator_id = ? AND status = 'active'",
                (creator_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    async def get_user_wagering_info(self, user_id: int) -> dict:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT bonus_wager_left, bonus_wager_total FROM users WHERE user_id = ?",
                (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return {'left': Decimal('0'), 'total': Decimal('0')}
                left = Decimal(str(row['bonus_wager_left'] or '0'))
                total = Decimal(str(row['bonus_wager_total'] or '0'))
                if total <= Decimal('0'):
                    total = left
                return {'left': left, 'total': total}

    async def set_wagering_left_on_cash(self, check_id: str, amount: Decimal, multiplier: Decimal):
        total_to_wager = amount * multiplier
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute("UPDATE checks SET wagering_left = ? WHERE check_id = ?", (total_to_wager, check_id))
            await db.commit()

    async def update_wagering_on_bet(self, user_id: int, bet_amount: Decimal):
        bet_amount = Decimal(str(bet_amount))
        if bet_amount <= 0:
            return
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            await db.execute("BEGIN IMMEDIATE")
            await self._consume_bonus_wager_in_tx(db, user_id, bet_amount)
            await db.commit()

    async def remove_wagering_if_balance_negative(self, user_id: int):
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT balance, bonus_balance FROM users WHERE user_id = ?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return
                balance = Decimal(str(row['balance'] or '0'))
                bonus_balance = Decimal(str(row['bonus_balance'] or '0'))
            if bonus_balance > 0 and balance <= bonus_balance:
                await db.execute(
                    """
                    UPDATE users
                    SET bonus_balance = 0,
                        bonus_wager_left = 0,
                        bonus_wager_total = 0
                    WHERE user_id = ?
                    """,
                    (user_id,)
                )
                await db.commit()

    async def get_user_referrals(self, user_id: int) -> list:
        async with aiosqlite.connect(self.db_path, **self.connect_params) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT user_id, username, full_name FROM users WHERE referrer_id = ? ORDER BY created_at ASC", (user_id,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]