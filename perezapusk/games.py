import random
from decimal import Decimal
from typing import Optional
from dataclasses import dataclass

@dataclass
class GameResult:
    won: bool
    amount: Decimal
    message: str
    emoji: str
    value: Optional[int] = None

class Game:
    EMOJI = "🎲"

    def __init__(self, bet_amount: Decimal):
        self.bet_amount = bet_amount

    async def process(self, bet_type: str, dice_value: int) -> GameResult:
        raise NotImplementedError

    def get_emoji(self, bet_type: str) -> str:
        return self.EMOJI

class CubeGame(Game):
    EMOJI = "🎲"

    async def process(self, bet_type: str, dice_value: int) -> GameResult:
        bet_type = bet_type.lower().replace(" ", "")
        if bet_type in ("чет", "нечет"):
            is_even = dice_value % 2 == 0
            if (bet_type == "чет" and is_even) or (bet_type == "нечет" and not is_even):
                win_amount = self.bet_amount * Decimal('1.85')
                return GameResult(True, win_amount, f"🎲 Выпало число {dice_value}!\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎲 Выпало число {dice_value}!\nВы проиграли!", self.EMOJI, dice_value)
        if bet_type in ("больше", "меньше"):
            if (bet_type == "больше" and dice_value > 3) or (bet_type == "меньше" and dice_value <= 3):
                win_amount = self.bet_amount * Decimal('1.85')
                return GameResult(True, win_amount, f"🎲 Выпало число {dice_value}!\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎲 Выпало число {dice_value}!\nВы проиграли!", self.EMOJI, dice_value)
        if bet_type in ("сектор1", "сектор2", "сектор3", "с1", "с2", "с3"):
            sector_map = {
                "сектор1": "1", "с1": "1",
                "сектор2": "2", "с2": "2",
                "сектор3": "3", "с3": "3"
            }
            sector = sector_map[bet_type]
            sector_numbers = {
                "1": (1, 2),
                "2": (3, 4),
                "3": (5, 6)
            }
            if dice_value in sector_numbers[sector]:
                win_amount = self.bet_amount * Decimal('2.5')
                return GameResult(True, win_amount, f"🎲 Выпало число {dice_value}!\nСектор {sector} выиграл!\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎲 Выпало число {dice_value}!\nВы проиграли!", self.EMOJI, dice_value)
        if bet_type in ("1", "2", "3", "4", "5", "6"):
            if str(dice_value) == bet_type:
                win_amount = self.bet_amount * Decimal('4')
                return GameResult(True, win_amount, f"🎲 Выпало число {dice_value}!\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎲 Выпало число {dice_value}!\nВы проиграли!", self.EMOJI, dice_value)
        if bet_type in ("плинко", "пл", "plinko"):
            multipliers = {1: Decimal('0'), 2: Decimal('0.3'), 3: Decimal('0.9'),
                           4: Decimal('1.1'), 5: Decimal('1.4'), 6: Decimal('1.95')}
            mult = multipliers.get(dice_value, Decimal('0'))
            if mult > 0:
                win_amount = self.bet_amount * mult
                return GameResult(True, win_amount, f"🎲 Выпало число {dice_value}!\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎲 Выпало число {dice_value}!\nВы проиграли!", self.EMOJI, dice_value)
        return GameResult(False, Decimal('0'), f"🎲 Выпало число {dice_value}!\nВы проиграли!", self.EMOJI, dice_value)

class TwoDiceGame(Game):
    EMOJI = "🎲"

    async def roll_second_dice(self) -> int:
        return random.randint(1, 6)

    async def process(self, bet_type: str, dice_value: int, second_dice_value: int = None) -> GameResult:
        bet_type = bet_type.lower().replace(" ", "")
        dice1 = dice_value
        dice2 = second_dice_value if second_dice_value is not None else await self.roll_second_dice()
        if bet_type == "ничья":
            if dice1 == dice2:
                win_amount = self.bet_amount * Decimal('3')
                return GameResult(True, win_amount, f"🎲 Выпало {dice1} и {dice2}! Ничья — выигрыш {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎲 Выпало {dice1} и {dice2}!\nВы проиграли!", self.EMOJI, dice_value)
        if bet_type == "победа1":
            if dice1 > dice2:
                win_amount = self.bet_amount * Decimal('1.85')
                return GameResult(True, win_amount, f"🎲 Выпало {dice1} и {dice2}!\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
            if dice1 == dice2:
                win_amount = self.bet_amount * Decimal('0.7')
                return GameResult(True, win_amount, f"🎲 Выпало {dice1} и {dice2}! Ничья — возврат с комиссией 30%: {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎲 Выпало {dice1} и {dice2}!\nВы проиграли!", self.EMOJI, dice_value)
        if bet_type == "победа2":
            if dice2 > dice1:
                win_amount = self.bet_amount * Decimal('1.85')
                return GameResult(True, win_amount, f"🎲 Выпало {dice1} и {dice2}!\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
            if dice1 == dice2:
                win_amount = self.bet_amount * Decimal('0.7')
                return GameResult(True, win_amount, f"🎲 Выпало {dice1} и {dice2}! Ничья — возврат с комиссией 30%: {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎲 Выпало {dice1} и {dice2}!\nВы проиграли!", self.EMOJI, dice_value)
        if bet_type == "2чет":
            if dice1 % 2 == 0 and dice2 % 2 == 0:
                win_amount = self.bet_amount * Decimal('2.5')
                return GameResult(True, win_amount, f"🎲 Оба кубика четные! Выпало {dice1} и {dice2}. Выигрыш {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎲 Выпало {dice1} и {dice2}. Оба должны быть четные! Вы проиграли!", self.EMOJI, dice_value)
        if bet_type == "2нечет":
            if dice1 % 2 == 1 and dice2 % 2 == 1:
                win_amount = self.bet_amount * Decimal('2.5')
                return GameResult(True, win_amount, f"🎲 Оба кубика нечетные! Выпало {dice1} и {dice2}. Выигрыш {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎲 Выпало {dice1} и {dice2}. Оба должны быть нечетные! Вы проиграли!", self.EMOJI, dice_value)
        if bet_type == "2меньше":
            if dice1 < 4 and dice2 < 4:
                win_amount = self.bet_amount * Decimal('2.5')
                return GameResult(True, win_amount, f"🎲 Оба кубика меньше 4! Выпало {dice1} и {dice2}. Выигрыш {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎲 Выпало {dice1} и {dice2}. Оба должны быть < 4! Вы проиграли!", self.EMOJI, dice_value)
        if bet_type == "2больше":
            if dice1 > 3 and dice2 > 3:
                win_amount = self.bet_amount * Decimal('2.5')
                return GameResult(True, win_amount, f"🎲 Оба кубика больше 3! Выпало {dice1} и {dice2}. Выигрыш {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎲 Выпало {dice1} и {dice2}. Оба должны быть > 3! Вы проиграли!", self.EMOJI, dice_value)
        if bet_type == "произведение18":
            if dice1 * dice2 >= 18:
                win_amount = self.bet_amount * Decimal('3')
                return GameResult(True, win_amount, f"🎲 Произведение {dice1}*{dice2} >= 18! Выигрыш {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎲 Произведение {dice1}*{dice2} < 18! Вы проиграли!", self.EMOJI, dice_value)
        return GameResult(False, Decimal('0'), f"🎲 Выпало {dice1} и {dice2}!\nВы проиграли!", self.EMOJI, dice_value)

class RockPaperScissorsGame(Game):
    EMOJI = "👊"

    ROCK_EMOJI = "👊"
    PAPER_EMOJI = "✋"
    SCISSORS_EMOJI = "✌️"

    BET_EMOJIS = {
        "камень": ROCK_EMOJI,
        "бумага": PAPER_EMOJI,
        "ножницы": SCISSORS_EMOJI,
        "rock": ROCK_EMOJI,
        "paper": PAPER_EMOJI,
        "scissors": SCISSORS_EMOJI,
        "к": ROCK_EMOJI,
        "б": PAPER_EMOJI,
        "н": SCISSORS_EMOJI,
        "r": ROCK_EMOJI,
        "p": PAPER_EMOJI,
        "s": SCISSORS_EMOJI,
    }

    RULES = {
        "камень": ["ножницы"],
        "бумага": ["камень"],
        "ножницы": ["бумага"],
    }

    def get_emoji(self, bet_type: str) -> str:
        bet_type = bet_type.lower().replace(" ", "")
        return self.BET_EMOJIS.get(bet_type, self.EMOJI)

    async def process(self, bet_type: str, bot_choice_value: int) -> GameResult:
        bet_type = bet_type.lower().replace(" ", "")
        bet_mapping = {
            "к": "камень", "б": "бумага", "н": "ножницы",
            "r": "камень", "p": "бумага", "s": "ножницы",
            "rock": "камень", "paper": "бумага", "scissors": "ножницы"
        }
        player_choice = bet_mapping.get(bet_type, bet_type)
        if player_choice not in ("камень", "бумага", "ножницы"):
            return GameResult(False, Decimal('0'), "❌", self.EMOJI, bot_choice_value)
        bot_choices = {1: "камень", 2: "ножницы", 3: "бумага"}
        bot_choice = bot_choices.get(bot_choice_value, "камень")
        player_emoji = self.BET_EMOJIS[player_choice]
        if player_choice == bot_choice:
            win_amount = self.bet_amount * Decimal('0.7')
            return GameResult(True, win_amount, f"{player_emoji}", self.EMOJI, bot_choice_value)
        if bot_choice in self.RULES.get(player_choice, []):
            win_amount = self.bet_amount * Decimal('2.5')
            return GameResult(True, win_amount, f"{player_emoji}", self.EMOJI, bot_choice_value)
        return GameResult(False, Decimal('0'), f"{player_emoji}", self.EMOJI, bot_choice_value)

class BasketballGame(Game):
    EMOJI = "🏀"

    async def process(self, bet_type: str, dice_value: int) -> GameResult:
        bet_type = bet_type.lower().replace(" ", "")
        goal_words = ("гол", "попадание", "goal", "hit", "score")
        miss_words = ("промах", "мимо", "miss")
        is_goal = dice_value in (4, 5)
        if bet_type == "чистыйгол":
            if dice_value == 5:
                win_amount = self.bet_amount * Decimal('3.5')
                return GameResult(True, win_amount, f"🏀 Чистый гол! Выпало {dice_value}. Выигрыш {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🏀 Выпало {dice_value}. Нужно было 5! Вы проиграли!", self.EMOJI, dice_value)
        if bet_type == "застрял":
            if dice_value == 3:
                win_amount = self.bet_amount * Decimal('3.5')
                return GameResult(True, win_amount, f"🏀 Мяч застрял! Выпало {dice_value}. Выигрыш {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🏀 Выпало {dice_value}. Нужно было 3! Вы проиграли!", self.EMOJI, dice_value)
        if any(word in bet_type for word in goal_words) and is_goal:
            win_amount = self.bet_amount * Decimal('1.85')
            return GameResult(True, win_amount, f"🏀 Попадание! Выпало {dice_value}\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
        if any(word in bet_type for word in miss_words) and not is_goal:
            win_amount = self.bet_amount * Decimal('1.4')
            return GameResult(True, win_amount, f"🏀 Промах! Выпало {dice_value}\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
        return GameResult(False, Decimal('0'), f"🏀 Выпало {dice_value}\nВы проиграли!", self.EMOJI, dice_value)

class DartsGame(Game):
    EMOJI = "🎯"

    async def process(self, bet_type: str, dice_value: int) -> GameResult:
        bet_type = bet_type.lower().replace(" ", "")
        if bet_type in ("промах", "мимо"):
            if dice_value == 1:
                win_amount = self.bet_amount * Decimal('2.5')
                return GameResult(True, win_amount, f"🎯 Промах! Выпало {dice_value}\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎯 Выпало {dice_value}\nВы проиграли!", self.EMOJI, dice_value)
        if bet_type == "белое":
            if dice_value in (3, 5):
                win_amount = self.bet_amount * Decimal('1.85')
                return GameResult(True, win_amount, f"🎯 Белое! Выпало {dice_value}\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎯 Выпало {dice_value}\nВы проиграли!", self.EMOJI, dice_value)
        if bet_type == "красное":
            if dice_value in (2, 4):
                win_amount = self.bet_amount * Decimal('1.85')
                return GameResult(True, win_amount, f"🎯 Красное! Выпало {dice_value}\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎯 Выпало {dice_value}\nВы проиграли!", self.EMOJI, dice_value)
        if bet_type == "яблочко":
            if dice_value == 6:
                win_amount = self.bet_amount * Decimal('2.5')
                return GameResult(True, win_amount, f"🎯 Яблочко! Выпало {dice_value}\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎯 Выпало {dice_value}\nВы проиграли!", self.EMOJI, dice_value)
        return GameResult(False, Decimal('0'), f"🎯 Выпало {dice_value}\nВы проиграли!", self.EMOJI, dice_value)

class SlotsGame(Game):
    EMOJI = "🎰"

    async def process(self, bet_type: str, dice_value: int) -> GameResult:
        if dice_value == 64:
            win_amount = self.bet_amount * Decimal('10')
            return GameResult(True, win_amount, f"🎰 Джекпот! 777!\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
        if dice_value == 1:
            win_amount = self.bet_amount * Decimal('5')
            return GameResult(True, win_amount, f"🎰 Джекпот! BAR!\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
        if dice_value in (43, 22, 52, 27, 38):
            win_amount = self.bet_amount * Decimal('5')
            return GameResult(True, win_amount, f"🎰 Три одинаковых!\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
        return GameResult(False, Decimal('0'), "🎰 Неудачная комбинация.\nВы проиграли!", self.EMOJI, dice_value)

class BowlingGame(Game):
    EMOJI = "🎳"

    async def process(self, bet_type: str, dice_value: int, second_dice_value: int = None) -> GameResult:
        bet_type = bet_type.lower().replace(" ", "")
        if bet_type in ("боулпобеда", "боулпоражение") and second_dice_value is not None:
            if bet_type == "боулпобеда":
                if dice_value > second_dice_value:
                    win_amount = self.bet_amount * Decimal('1.85')
                    return GameResult(True, win_amount, f"🎳 Дуэль: {dice_value} vs {second_dice_value}\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
                if dice_value == second_dice_value:
                    win_amount = self.bet_amount * Decimal('0.7')
                    return GameResult(True, win_amount, f"🎳 Дуэль: {dice_value} vs {second_dice_value}! Ничья — ставка возвращается с комиссией 30%: {win_amount}$!", self.EMOJI, dice_value)
                return GameResult(False, Decimal('0'), f"🎳 Дуэль: {dice_value} vs {second_dice_value}\nВы проиграли!", self.EMOJI, dice_value)
            if bet_type == "боулпоражение":
                if dice_value < second_dice_value:
                    win_amount = self.bet_amount * Decimal('1.85')
                    return GameResult(True, win_amount, f"🎳 Дуэль: {dice_value} vs {second_dice_value}\nВы выиграли {win_amount}$!", self.EMOJI, dice_value)
                if dice_value == second_dice_value:
                    win_amount = self.bet_amount * Decimal('0.7')
                    return GameResult(True, win_amount, f"🎳 Дуэль: {dice_value} vs {second_dice_value}! Ничья — ставка возвращается с комиссией 30%: {win_amount}$!", self.EMOJI, dice_value)
                return GameResult(False, Decimal('0'), f"🎳 Дуэль: {dice_value} vs {second_dice_value}\nВы проиграли!", self.EMOJI, dice_value)
        if bet_type in ("боул", "боулинг"):
            if dice_value == 1:
                return GameResult(False, Decimal('0'), f"🎳 Выпало {dice_value}. Поражение!", self.EMOJI, dice_value)
            if dice_value == 2:
                win_amount = self.bet_amount * Decimal('0.4')
                return GameResult(True, win_amount, f"🎳 Выпало {dice_value}. Выигрыш x0.4! {win_amount}$", self.EMOJI, dice_value)
            if dice_value == 3:
                win_amount = self.bet_amount * Decimal('0.9')
                return GameResult(True, win_amount, f"🎳 Выпало {dice_value}. Выигрыш x0.9! {win_amount}$", self.EMOJI, dice_value)
            if dice_value == 4:
                win_amount = self.bet_amount * Decimal('1.3')
                return GameResult(True, win_amount, f"🎳 Выпало {dice_value}. Выигрыш x1.3! {win_amount}$", self.EMOJI, dice_value)
            if dice_value == 5:
                win_amount = self.bet_amount * Decimal('1.6')
                return GameResult(True, win_amount, f"🎳 Выпало {dice_value}. Выигрыш x1.6! {win_amount}$", self.EMOJI, dice_value)
            if dice_value == 6:
                win_amount = self.bet_amount * Decimal('1.95')
                return GameResult(True, win_amount, f"🎳 Страйк! Выпало {dice_value}. Джекпот x1.95! {win_amount}$", self.EMOJI, dice_value)
            return GameResult(False, Decimal('0'), f"🎳 Выпало {dice_value}. Вы проиграли!", self.EMOJI, dice_value)
        if bet_type == "страйк" and dice_value == 6:
            win_amount = self.bet_amount * Decimal('4')
            return GameResult(True, win_amount, f"🎳 Страйк! Выпало {dice_value}. Вы выиграли {win_amount}$!", self.EMOJI, dice_value)
        if bet_type == "боулпромах" and dice_value == 1:
            win_amount = self.bet_amount * Decimal('4')
            return GameResult(True, win_amount, f"🎳 Промах! Выпало {dice_value}. Вы выиграли {win_amount}$!", self.EMOJI, dice_value)
        return GameResult(False, Decimal('0'), f"🎳 Выпало {dice_value}. Вы проиграли!", self.EMOJI, dice_value)

class FootballGame(Game):
    EMOJI = "⚽"
    
    async def process(self, bet_type: str, dice_value: int) -> GameResult:
        is_goal = dice_value in [3, 4, 5]
        is_miss = dice_value in [1, 2]
        
        if bet_type == "футгол" and is_goal:
            return GameResult(True, self.bet_amount * Decimal('1.4'), f"⚽ Гол! Выпало {dice_value} - это гол!", self.EMOJI, dice_value)
        elif bet_type == "футпромах" and is_miss:
            return GameResult(True, self.bet_amount * Decimal('1.85'), f"⚽ Промах! Выпало {dice_value} - это промах!", self.EMOJI, dice_value)
        else:
            result_text = f"⚽ {'Гол' if is_goal else 'Промах'}! Выпало {dice_value} - это не {'гол' if bet_type == 'футгол' else 'промах'}."
            return GameResult(False, Decimal('0'), result_text, self.EMOJI, dice_value)

class CustomEmojiGame(Game):
    EMOJI_MAP = {
        'custom1': {'emoji': '📞', 'coef': 2},
        'custom2': {'emoji': '🌈', 'coef': 3},
        'custom3': {'emoji': '🎮', 'coef': 5},
        'custom4': {'emoji': '💣', 'coef': 10},
        'custom5': {'emoji': '🔮', 'coef': 20},
        'custom6': {'emoji': '🔭', 'coef': 30},
        'custom7': {'emoji': '📱', 'coef': 50},
        'custom8': {'emoji': '🚀', 'coef': 100},
    }
    def __init__(self, bet_amount: Decimal, game_key: str):
        super().__init__(bet_amount)
        self.game_key = game_key
        self.emoji = self.EMOJI_MAP[game_key]['emoji']
        self.coef = self.EMOJI_MAP[game_key]['coef']
        self.win_value = self.coef

    async def process(self, bet_type: str, dice_value: int = None) -> GameResult:
        coef = self.coef
        chances = {2: 0.4, 3: 0.25, 5: 0.15, 10: 0.08, 20: 0.05, 30: 0.03, 50: 0.02, 100: 0.01}
        if dice_value is None:
            if random.random() < chances.get(coef, 0.01):
                dice_value = coef
            else:
                dice_value = random.randint(1, coef - 1)
        if dice_value == coef:
            win_amount = self.bet_amount * Decimal(str(coef))
            return GameResult(True, win_amount, f"{self.emoji} Выпало: {dice_value} из {coef}, нужно было: {coef}\nВы выиграли {win_amount}$!", self.emoji, dice_value)
        return GameResult(False, Decimal('0'), f"{self.emoji} Выпало: {dice_value} из {coef}, нужно было: {coef}\nВы проиграли!", self.emoji, dice_value)