import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package, "--quiet"])

try:
    import aiogram
except ImportError:
    print("Installing aiogram...")
    install("aiogram==3.7.0")
    print("aiogram installed!")

import os
import json
import logging
import asyncio
import time
from datetime import datetime, date
from typing import Dict, List, Optional

from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    Message, CallbackQuery, ChatMemberUpdated
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.client.default import DefaultBotProperties

# ─────────────────────────── CONFIGURATION ────────────────────────────
BOT_TOKEN = "8532241109:AAENWI8I_czvRO2hlCC9ecCdAMkulNptGII"
ADMIN_ID   = 6910883192
DB_FILE    = "db.json"

# ─────────────────────────── LOGGING ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("BotLogger")

# ─────────────────────────── DATABASE ─────────────────────────────────
class Database:
    def __init__(self, filename: str):
        self.filename = filename
        self.data = self._load()

    def _default(self) -> dict:
        return {
            "global_sponsor": {
                "name": "Основной спонсор",
                "link": "https://t.me/subcoinnnews",
                "channel_id": -1002133687597,
                "active": True,
            },
            "chats": {},
            "users": {},
            "pro_requests": [],
            "stats": {
                "total_messages_checked": 0,
                "total_messages_deleted": 0,
                "total_users": 0,
                "total_chats": 0,
                "total_sponsors": 0,
                "today_users": {},        # date -> set of user_ids (stored as list)
            },
        }

    def _load(self) -> dict:
        if os.path.exists(self.filename):
            try:
                with open(self.filename, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading DB: {e}")
        return self._default()

    def save(self):
        try:
            with open(self.filename, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error saving DB: {e}")

    # ── User helpers ──────────────────────────────────────────────────
    def get_user(self, user_id: int) -> dict:
        uid = str(user_id)
        if uid not in self.data["users"]:
            self.data["users"][uid] = {
                "subscriptions": {},
                "first_seen": datetime.now().isoformat(),
            }
            self.data["stats"]["total_users"] += 1
            self.save()
        # track today's unique users
        today = date.today().isoformat()
        today_users = self.data["stats"].setdefault("today_users", {})
        today_list  = today_users.setdefault(today, [])
        if uid not in today_list:
            today_list.append(uid)
            self.save()
        return self.data["users"][uid]

    def count_today_users(self) -> int:
        today = date.today().isoformat()
        return len(self.data["stats"].get("today_users", {}).get(today, []))


db = Database(DB_FILE)

# ─────────────────────────── BOT SETUP ────────────────────────────────

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp  = Dispatcher(storage=MemoryStorage())
router = Router()

# ─────────────────────────── SUBSCRIPTION CACHE ───────────────────────
# sub_cache[user_id][channel_id] = (is_subscribed: bool, timestamp: float)
sub_cache: Dict[int, Dict[int, tuple]] = {}

async def check_subscription(user_id: int, channel_id: int) -> bool:
    # Always cast to int — JSON may load numbers as int but stay safe
    channel_id = int(channel_id)
    now = time.time()
    user_cache = sub_cache.get(user_id, {})
    if channel_id in user_cache:
        status, ts = user_cache[channel_id]
        if now - ts < 2:             # 2-second cache
            return status
    try:
        member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
        is_sub = member.status in ("member", "administrator", "creator")
    except Exception as e:
        logger.warning(f"check_subscription({user_id}, {channel_id}): {e}")
        is_sub = False
    sub_cache.setdefault(user_id, {})[channel_id] = (is_sub, now)
    return is_sub

def invalidate_sponsor_cache(channel_id: int):
    """Remove all cached entries for a channel so next check hits Telegram API."""
    channel_id = int(channel_id)
    for uid in list(sub_cache.keys()):
        sub_cache[uid].pop(channel_id, None)

# pending_sponsor[user_id] = target_chat_id
# Хранит: кто нажал кнопку "добавить бота в спонсор" и для какого чата ждём
pending_sponsor: Dict[int, str] = {}

# bot_sub_cache[user_id][bot_token] = (is_started: bool, timestamp: float)
bot_sub_cache: Dict[int, Dict[str, tuple]] = {}

async def check_bot_subscription(user_id: int, bot_token: str) -> bool:
    """Check if user has started the bot by trying to send them a chat action."""
    now = time.time()
    user_cache = bot_sub_cache.get(user_id, {})
    if bot_token in user_cache:
        status, ts = user_cache[bot_token]
        if now - ts < 2:
            return status

    is_started = False
    temp_bot = None
    try:
        temp_bot = Bot(token=bot_token, default=DefaultBotProperties(parse_mode="HTML"))
        # send_chat_action работает только если пользователь запустил бота
        await temp_bot.send_chat_action(chat_id=user_id, action="typing")
        is_started = True
        logger.info(f"check_bot_subscription({user_id}): user HAS started the bot")
    except TelegramForbiddenError:
        # Бот заблокирован пользователем или не был запущен
        is_started = False
        logger.info(f"check_bot_subscription({user_id}): user has NOT started the bot (Forbidden)")
    except Exception as e:
        # Любая другая ошибка — считаем что не запускал
        is_started = False
        logger.warning(f"check_bot_subscription({user_id}): error {e}")
    finally:
        if temp_bot is not None:
            try:
                await temp_bot.session.close()
            except Exception:
                pass

    bot_sub_cache.setdefault(user_id, {})[bot_token] = (is_started, now)
    return is_started

# ─────────────────────────── FSM STATES ───────────────────────────────
class BotStates(StatesGroup):
    # user flows
    waiting_for_chat_forward      = State()
    waiting_for_chat_id           = State()
    waiting_for_welcome_text      = State()
    waiting_for_sponsor_forward   = State()
    waiting_for_sponsor_link      = State()
    waiting_for_sponsor_id        = State()
    waiting_for_sponsor_id_link   = State()
    waiting_for_edit_sp_link      = State()
    waiting_for_edit_sp_id        = State()
    waiting_for_sp_timer_duration = State()
    waiting_for_sp_timer_datetime = State()
    waiting_for_bot_token         = State()
    # admin flows
    waiting_for_broadcast         = State()
    waiting_for_users_broadcast   = State()
    waiting_for_global_name       = State()
    waiting_for_global_link       = State()
    waiting_for_global_id         = State()

# ─────────────────────────── KEYBOARD HELPERS ─────────────────────────
def back_btn(cb: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text="🔙 Назад", callback_data=cb)

def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    """Обычное пользовательское меню (одинаково для всех включая админа)."""
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="➕ Добавить чат",          callback_data="add_chat"))
    b.row(InlineKeyboardButton(text="📋 Мои чаты",              callback_data="my_chats"))
    b.row(InlineKeyboardButton(text="🤝 Запросить спонсорство", callback_data="req_sponsorship"))
    b.row(InlineKeyboardButton(text="ℹ️ Помощь",                callback_data="help"))
    return b.as_markup()

def admin_menu_kb() -> InlineKeyboardMarkup:
    """Админ-панель."""
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="📊 Статистика",         callback_data="admin_stats"))
    b.row(InlineKeyboardButton(text="⚙️ Глобальный спонсор", callback_data="admin_global"))
    b.row(InlineKeyboardButton(text="⚡ Управление режимами", callback_data="admin_modes"))
    b.row(InlineKeyboardButton(text="🗂 Все чаты",            callback_data="admin_all_chats"))
    b.row(InlineKeyboardButton(text="📝 Запросы на PRO",         callback_data="admin_pro_reqs"))
    b.row(InlineKeyboardButton(text="📢 Рассылка в чаты",      callback_data="admin_broadcast_start"))
    b.row(InlineKeyboardButton(text="✉️ Рассылка пользователям", callback_data="admin_users_broadcast_start"))
    return b.as_markup()

# ══════════════════════════════════════════════════════════════════════
#  SECTION 1 — START / MAIN MENU
# ══════════════════════════════════════════════════════════════════════
@router.message(CommandStart(), F.chat.type == "private")
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    db.get_user(message.from_user.id)
    text = (
        "👋 <b>Привет!</b>\n\n"
        "Я бот-модератор подписок — помогаю владельцам чатов проверять подписку "
        "участников на каналы и группы прямо в Telegram.\n\n"
        "<b>Что умею:</b>\n"
        "• Удалять сообщения тех, кто не подписан на спонсоров\n"
        "• Отправлять предупреждение с кнопкой подписки\n"
        "• Добавлять спонсорами каналы, группы и даже ботов\n"
        "• Работать в режимах <b>FREE</b> (с глобальным спонсором) и <b>PRO</b> (только ваши спонсоры)\n\n"
        "<b>Быстрый старт:</b>\n"
        "1. Нажмите <b>➕ Добавить чат</b>\n"
        "2. Добавьте бота в свою группу как администратора\n"
        "3. Добавьте спонсоров через <b>📋 Мои чаты</b>\n\n"
        "Если хотите чтобы ваш канал или бот стал спонсором — нажмите <b>🤝 Запросить спонсорство</b> 👇"
    )
    await message.answer(text, reply_markup=main_menu_kb(message.from_user.id))

@router.message(Command("admin"), F.chat.type == "private")
async def cmd_admin(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return await message.answer("❌ У вас нет доступа к админ-панели.")
    await state.clear()
    await message.answer("🔐 <b>Админ-панель</b>\n\nДобро пожаловать!", reply_markup=admin_menu_kb())

@router.callback_query(F.data == "noop")
async def noop_handler(callback: CallbackQuery):
    await callback.answer()

@router.callback_query(F.data == "main_menu")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("🏠 Главное меню", reply_markup=main_menu_kb(callback.from_user.id))

@router.callback_query(F.data == "admin_menu")
async def back_to_admin(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    await state.clear()
    await callback.message.edit_text("🔐 <b>Админ-панель</b>", reply_markup=admin_menu_kb())

# ══════════════════════════════════════════════════════════════════════
#  SECTION 2 — HELP
# ══════════════════════════════════════════════════════════════════════
@router.callback_query(F.data == "help")
async def help_handler(callback: CallbackQuery):
    text = (
        "ℹ️ <b>Инструкция по использованию</b>\n\n"

        "➕ <b>Добавление чата</b>\n"
        "Нажмите <b>➕ Добавить чат</b> → кнопка откроет Telegram с выбором группы. "
        "Бот автоматически добавится с правами администратора и зарегистрирует чат.\n"
        "Альтернатива: напишите <code>/add</code> прямо в чате где уже есть бот-администратор.\n\n"

        "📢 <b>Добавление спонсора</b>\n"
        "Зайдите в <b>📋 Мои чаты</b> → выберите чат → <b>➕ Добавить спонсора</b>.\n"
        "Доступны три способа:\n"
        "• Кнопка <b>📢 Добавить в канал</b> или <b>👥 Добавить в чат</b> — бот добавится "
        "администратором и спонсор зарегистрируется автоматически\n"
        "• <b>📨 Переслать сообщение</b> — перешлите любое сообщение из канала-спонсора\n"
        "• <b>🆔 Ввести ID вручную</b> — введите числовой ID или @username\n"
        "• <b>🤖 Добавить бота</b> — вставьте API-токен бота, бот проверит запустил ли его пользователь\n\n"
        "Для публичных каналов ссылка подставляется автоматически. "
        "Для приватных — бот создаёт пригласительную ссылку сам.\n\n"

        "⚡ <b>Режимы FREE и PRO</b>\n"
        "• <b>FREE</b> — проверяется глобальный спонсор + ваши спонсоры\n"
        "• <b>PRO</b> — глобальный спонсор отключён, проверяются только ваши спонсоры\n"
        "Запросить PRO можно через кнопку <b>⚡ Запросить PRO режим</b> в управлении чатом.\n\n"

        "🛡 <b>Как работает модерация</b>\n"
        "При каждом сообщении бот проверяет подписку. Если пользователь не подписан — "
        "сообщение удаляется и отправляется предупреждение с кнопками подписки. "
        "Как только подпишется — сможет писать снова.\n\n"

        "🆔 <b>Узнать ID чата</b>\n"
        "Напишите <code>/id</code> в нужном чате — бот пришлёт ID в чат и в личку."
    )
    b = InlineKeyboardBuilder()
    b.row(back_btn("main_menu"))
    await callback.message.edit_text(text, reply_markup=b.as_markup())

@router.callback_query(F.data == "req_sponsorship")
async def req_sponsorship(callback: CallbackQuery):
    user = callback.from_user
    username = f"@{user.username}" if user.username else f'<a href="tg://user?id={user.id}">{user.first_name}</a>'
    try:
        await bot.send_message(
            ADMIN_ID,
            f"🤝 <b>Запрос на спонсорство</b>\n\n"
            f"👤 От: {username} (<code>{user.id}</code>)\n"
            f"📅 Дата: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
    except Exception:
        pass
    await callback.answer("✅ Запрос отправлен администратору! Он свяжется с вами.", show_alert=True)

# ══════════════════════════════════════════════════════════════════════
#  SECTION 3 — ADD CHAT
# ══════════════════════════════════════════════════════════════════════

async def _save_chat(chat_id: int, title: str, owner_id: int, owner_username: str) -> str:
    """Save chat to DB and return status: 'added' or 'exists'."""
    chat_id_str = str(chat_id)
    if chat_id_str in db.data["chats"]:
        return "exists"
    db.data["chats"][chat_id_str] = {
        "owner_id":       owner_id,
        "owner_username": owner_username or "",
        "title":          title,
        "welcome_text":   "Привет! Подпишись на спонсоров, чтобы писать в этом чате!",
        "mode":           "FREE",
        "sponsors":       [],
        "enabled":        True,
        "added_at":       datetime.now().isoformat(),
    }
    db.data["stats"]["total_chats"] += 1
    db.save()
    logger.info(f"Chat added: {title} ({chat_id}) by user {owner_id}")
    return "added"

async def _check_bot_admin(chat_id: int) -> tuple:
    """Returns (ok: bool, error_text: str)."""
    try:
        me = await bot.get_me()
        bot_member = await bot.get_chat_member(chat_id, me.id)
        can_delete = getattr(bot_member, "can_delete_messages", False)
        if not can_delete:
            return False, "❌ Я должен быть администратором в этом чате <b>с правом удаления сообщений</b>.\nВыдайте права и попробуйте снова."
        return True, ""
    except Exception as e:
        return False, f"❌ Не удалось проверить доступ к чату: {e}"

@router.callback_query(F.data == "add_chat")
async def add_chat_start(callback: CallbackQuery, state: FSMContext):
    me = await bot.get_me()
    bot_username = me.username
    add_link = (
        f"https://t.me/{bot_username}?startgroup&admin="
        "post_messages+delete_messages+invite_users"
    )
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="➕ Добавить бота в чат (с правами)", url=add_link))
    b.row(InlineKeyboardButton(text="🆔 Уже добавлен — ввести Chat ID", callback_data="add_chat_by_id"))
    b.row(back_btn("main_menu"))
    await callback.message.edit_text(
        "➕ <b>Добавление чата</b>\n\n"
        "<b>Шаг 1.</b> Нажмите кнопку <b>➕ Добавить бота в чат</b> — "
        "Telegram откроется с выбором группы. Выберите нужную и бот сразу добавится "
        "с правами администратора.\n\n"
        "<b>Шаг 2.</b> Бот автоматически зарегистрирует чат и пришлёт вам уведомление в личку — "
        "ничего дополнительно вводить не нужно.\n\n"
        "─────────────────────\n"
        "Если бот <b>уже есть</b> в чате как администратор:\n"
        "• Напишите <code>/add</code> прямо в чате\n"
        "• Или введите Chat ID кнопкой ниже (узнать ID: <code>/id</code> в чате)",
        reply_markup=b.as_markup()
    )

# ── Add by ID ─────────────────────────────────────────────────────────
@router.callback_query(F.data == "add_chat_by_id")
async def add_chat_by_id_start(callback: CallbackQuery, state: FSMContext):
    b = InlineKeyboardBuilder()
    b.row(back_btn("add_chat"))
    await callback.message.edit_text(
        "🆔 <b>Добавление по Chat ID</b>\n\n"
        "Введите ID чата (например: <code>-1001234567890</code>)\n\n"
        "Узнать ID чата: напишите <code>/id</code> в нужном чате.",
        reply_markup=b.as_markup()
    )
    await state.set_state(BotStates.waiting_for_chat_id)

@router.message(BotStates.waiting_for_chat_id, F.chat.type == "private")
async def process_chat_id_input(message: Message, state: FSMContext):
    raw = message.text.strip() if message.text else ""
    try:
        chat_id = int(raw)
    except ValueError:
        return await message.answer("❌ Неверный формат. Введите числовой ID, например <code>-1001234567890</code>.")

    try:
        chat_info = await bot.get_chat(chat_id)
    except Exception as e:
        return await message.answer(f"❌ Не удалось получить информацию о чате: {e}\nПроверьте ID и убедитесь, что бот добавлен в чат.")

    if chat_info.type not in ("group", "supergroup"):
        return await message.answer("❌ Это не группа или супергруппа.")

    ok, err = await _check_bot_admin(chat_id)
    if not ok:
        return await message.answer(err)

    result = await _save_chat(chat_id, chat_info.title, message.from_user.id, message.from_user.username or "")
    await state.clear()
    if result == "exists":
        await message.answer(f"ℹ️ Чат <b>{chat_info.title}</b> уже добавлен.", reply_markup=main_menu_kb(message.from_user.id))
    else:
        await message.answer(
            f"✅ Чат <b>{chat_info.title}</b> успешно добавлен!\n\n"
            "Теперь добавьте спонсоров через раздел 📋 Мои чаты.",
            reply_markup=main_menu_kb(message.from_user.id)
        )

# ── /add command in group ──────────────────────────────────────────────
@router.message(Command("add"), F.chat.type.in_({"group", "supergroup"}))
async def cmd_add_in_chat(message: Message):
    chat_id = message.chat.id

    ok, err = await _check_bot_admin(chat_id)
    if not ok:
        return await message.reply(err)

    result = await _save_chat(
        chat_id,
        message.chat.title,
        message.from_user.id,
        message.from_user.username or ""
    )
    if result == "exists":
        await message.reply(f"ℹ️ Этот чат уже зарегистрирован в системе.")
    else:
        await message.reply(
            f"✅ Чат <b>{message.chat.title}</b> успешно добавлен!\n\n"
            f"👤 Владелец: @{message.from_user.username or message.from_user.first_name}\n\n"
            f"Теперь настройте спонсоров в личных сообщениях бота: @{(await bot.get_me()).username}"
        )

# ── /id command — works in groups AND private ──────────────────────────
@router.message(Command("id"), F.chat.type == "private")
async def cmd_get_id_private(message: Message):
    await message.answer(
        f"👤 <b>Ваш личный ID:</b> <code>{message.from_user.id}</code>\n\n"
        "Чтобы узнать ID группы — напишите <code>/id</code> прямо в той группе."
    )

@router.message(Command("id"), F.chat.type.in_({"group", "supergroup"}))
async def cmd_get_id_group(message: Message):
    chat_id = message.chat.id
    text = (
        f"🆔 <b>ID этого чата:</b> <code>{chat_id}</code>\n"
        f"📛 Название: <b>{message.chat.title}</b>"
    )
    await message.reply(text)
    try:
        await bot.send_message(
            message.from_user.id,
            f"🆔 <b>ID чата «{message.chat.title}»:</b>\n<code>{chat_id}</code>\n\n"
            "Используйте этот ID для добавления чата через кнопку 🆔 в боте."
        )
    except Exception:
        pass

# ── old forward handler (kept as fallback) ────────────────────────────
@router.message(BotStates.waiting_for_chat_forward, F.chat.type == "private")
async def process_chat_forward(message: Message, state: FSMContext):
    await message.answer(
        "ℹ️ Пересылка сообщений из групп больше не работает в Telegram.\n\n"
        "Используйте один из способов:\n"
        "• Команда <code>/add</code> прямо в чате\n"
        "• Кнопку 🆔 Добавить по Chat ID (узнайте ID командой <code>/id</code> в чате)",
        reply_markup=main_menu_kb(message.from_user.id)
    )
    await state.clear()

# ══════════════════════════════════════════════════════════════════════
#  SECTION 4 — MY CHATS
# ══════════════════════════════════════════════════════════════════════
@router.callback_query(F.data == "my_chats")
async def list_my_chats(callback: CallbackQuery):
    b = InlineKeyboardBuilder()
    count = 0
    for cid, cdata in db.data["chats"].items():
        if cdata["owner_id"] == callback.from_user.id:
            sponsors_n = len(cdata["sponsors"])
            b.row(InlineKeyboardButton(
                text=f"{cdata['title']} | {cdata['mode']} | 📢{sponsors_n}",
                callback_data=f"manage_chat_{cid}"
            ))
            count += 1
    if count == 0:
        b.row(InlineKeyboardButton(text="➕ Добавить первый чат", callback_data="add_chat"))
        b.row(back_btn("main_menu"))
        return await callback.message.edit_text(
            "У вас пока нет добавленных чатов.\nНажмите кнопку ниже, чтобы добавить.",
            reply_markup=b.as_markup()
        )
    b.row(back_btn("main_menu"))
    await callback.message.edit_text("📋 <b>Ваши чаты:</b>\nВыберите чат для управления:", reply_markup=b.as_markup())

# ── Single chat management ─────────────────────────────────────────────
def chat_manage_kb(chat_id: str, mode: str, from_admin: bool = False) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="✏️ Изменить приветствие",  callback_data=f"edit_welcome_{chat_id}"))
    b.row(InlineKeyboardButton(text="➕ Добавить спонсора",      callback_data=f"add_sponsor_{chat_id}"))
    b.row(InlineKeyboardButton(text="📋 Список спонсоров",       callback_data=f"list_sponsors_{chat_id}"))
    b.row(InlineKeyboardButton(text="⚙️ Информация о чате",      callback_data=f"chat_info_{chat_id}"))
    if not from_admin and mode == "FREE":
        b.row(InlineKeyboardButton(text="⚡ Запросить PRO режим", callback_data=f"req_pro_{chat_id}"))
    elif not from_admin and mode == "PRO":
        b.row(InlineKeyboardButton(text="✅ Режим: PRO", callback_data="noop"))
    back_cb = "admin_all_chats" if from_admin else "my_chats"
    b.row(back_btn(back_cb))
    return b.as_markup()



@router.callback_query(F.data.startswith("manage_chat_"))
async def manage_chat(callback: CallbackQuery):
    chat_id = callback.data.replace("manage_chat_", "")
    cdata = db.data["chats"].get(chat_id)
    if not cdata:
        return await callback.answer("Чат не найден.", show_alert=True)
    if cdata["owner_id"] != callback.from_user.id and callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)

    active_sponsors = sum(1 for s in cdata["sponsors"] if s.get("active"))
    text = (
        f"⚙️ <b>Управление чатом:</b> {cdata['title']}\n"
        f"🆔 ID: <code>{chat_id}</code>\n"
        f"⚡ Режим: <b>{cdata['mode']}</b>\n"
        f"📢 Спонсоров: {len(cdata['sponsors'])} (активных: {active_sponsors})\n\n"
        f"📝 Приветствие:\n<i>{cdata['welcome_text']}</i>"
    )
    from_admin = callback.from_user.id == ADMIN_ID
    await callback.message.edit_text(text, reply_markup=chat_manage_kb(chat_id, cdata["mode"], from_admin=from_admin))

# ── Chat info ────────────────────────────────────────────────────────
@router.callback_query(F.data.startswith("chat_info_"))
async def chat_info(callback: CallbackQuery):
    chat_id = callback.data.replace("chat_info_", "")
    cdata = db.data["chats"].get(chat_id)
    if not cdata:
        return await callback.answer("Чат не найден.", show_alert=True)

    added_at = cdata.get("added_at", "—")
    active_sponsors = sum(1 for s in cdata["sponsors"] if s.get("active"))
    text = (
        f"ℹ️ <b>Информация о чате</b>\n\n"
        f"📛 Название: <b>{cdata['title']}</b>\n"
        f"🆔 ID: <code>{chat_id}</code>\n"
        f"👤 Владелец: @{cdata.get('owner_username') or cdata['owner_id']}\n"
        f"⚡ Режим: <b>{cdata['mode']}</b>\n"
        f"📢 Спонсоров всего: {len(cdata['sponsors'])}\n"
        f"✅ Активных спонсоров: {active_sponsors}\n"
        f"📅 Добавлен: {added_at[:10] if added_at != '—' else '—'}\n"
        f"🟢 Включён: {'Да' if cdata.get('enabled', True) else 'Нет'}"
    )
    b = InlineKeyboardBuilder()
    b.row(back_btn(f"manage_chat_{chat_id}"))
    await callback.message.edit_text(text, reply_markup=b.as_markup())

# ══════════════════════════════════════════════════════════════════════
#  SECTION 5 — WELCOME TEXT
# ══════════════════════════════════════════════════════════════════════
@router.callback_query(F.data.startswith("edit_welcome_"))
async def edit_welcome_start(callback: CallbackQuery, state: FSMContext):
    chat_id = callback.data.replace("edit_welcome_", "")
    cdata = db.data["chats"].get(chat_id)
    if not cdata:
        return await callback.answer("Чат не найден.", show_alert=True)
    await state.update_data(editing_chat_id=chat_id)
    await state.set_state(BotStates.waiting_for_welcome_text)
    b = InlineKeyboardBuilder()
    b.row(back_btn(f"manage_chat_{chat_id}"))
    await callback.message.edit_text(
        f"✏️ <b>Изменение приветствия</b>\n\n"
        f"Текущий текст:\n<i>{cdata['welcome_text']}</i>\n\n"
        "Отправьте новый текст приветствия.\n"
        "<b>Совет:</b> используйте @username — бот автоматически упомянет нарушителя.",
        reply_markup=b.as_markup()
    )

@router.message(BotStates.waiting_for_welcome_text, F.chat.type == "private")
async def process_welcome_text(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get("editing_chat_id")
    if not chat_id or chat_id not in db.data["chats"]:
        return await message.answer("Ошибка: чат не найден.")
    db.data["chats"][chat_id]["welcome_text"] = message.text
    db.save()
    await state.clear()
    logger.info(f"Welcome text updated for chat {chat_id}")
    await message.answer(
        "✅ Приветствие обновлено!",
        reply_markup=main_menu_kb(message.from_user.id)
    )

# ══════════════════════════════════════════════════════════════════════
#  SECTION 6 — SPONSORS
# ══════════════════════════════════════════════════════════════════════

async def _do_add_sponsor(chat_id: str, channel_id: int, title: str, link: Optional[str], owner_id: int,
                          sponsor_type: str = "channel", bot_token: Optional[str] = None) -> str:
    """Add sponsor to chat. Returns 'added' or 'exists'."""
    cdata = db.data["chats"].get(chat_id)
    if not cdata:
        return "no_chat"
    for s in cdata["sponsors"]:
        if s["channel_id"] == channel_id:
            return "exists"
    entry = {
        "channel_id":    channel_id,
        "title":         title,
        "link":          link,
        "active":        True,
        "type":          sponsor_type,
        "added_at":      datetime.now().isoformat(),
    }
    if sponsor_type == "bot" and bot_token:
        entry["bot_token"] = bot_token
    cdata["sponsors"].append(entry)
    db.data["stats"]["total_sponsors"] += 1
    db.save()
    logger.info(f"Sponsor added: {title} ({channel_id}) type={sponsor_type} to chat {chat_id} by {owner_id}")
    return "added"

# ── Entry point — show method choice ──────────────────────────────────
@router.callback_query(F.data.startswith("add_sponsor_"))
async def add_sponsor_start(callback: CallbackQuery, state: FSMContext):
    chat_id = callback.data.replace("add_sponsor_", "")
    cdata = db.data["chats"].get(chat_id)
    if not cdata:
        return await callback.answer("Чат не найден.", show_alert=True)
    await state.update_data(sponsor_chat_id=chat_id)
    me = await bot.get_me()
    admin_rights = "post_messages+delete_messages+invite_users"
    link_channel = f"https://t.me/{me.username}?startchannel&admin={admin_rights}"
    link_group   = f"https://t.me/{me.username}?startgroup&admin={admin_rights}"
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="📢 Добавить бота в канал-спонсор", url=link_channel))
    b.row(InlineKeyboardButton(text="👥 Добавить бота в чат-спонсор",   url=link_group))
    b.row(InlineKeyboardButton(text="📨 Переслать сообщение из канала",  callback_data=f"add_sp_forward_{chat_id}"))
    b.row(InlineKeyboardButton(text="🆔 Ввести ID канала/чата вручную",  callback_data=f"add_sp_by_id_{chat_id}"))
    b.row(InlineKeyboardButton(text="🤖 Добавить бота как спонсора",      callback_data=f"add_sp_bot_{chat_id}"))
    b.row(back_btn(f"manage_chat_{chat_id}"))
    # Запоминаем что этот пользователь ждёт добавления спонсора для данного чата
    pending_sponsor[callback.from_user.id] = chat_id
    await callback.message.edit_text(
        "📢 <b>Добавление спонсора</b>\n\n"
        "<b>Автоматически:</b>\n"
        "1️⃣ Нажмите <b>📢 Добавить в канал</b> или <b>👥 Добавить в чат</b>\n"
        "2️⃣ Выберите канал или чат-спонсор в Telegram\n"
        "3️⃣ Назначьте бота администратором — спонсор добавится автоматически\n\n"
        "<b>Вручную:</b>\n"
        "📨 <b>Переслать сообщение</b> — перешлите любое сообщение из канала\n"
        "🆔 <b>Ввести ID</b> — введите числовой ID или @username канала/группы\n"
        "🤖 <b>Добавить бота</b> — вставьте API-токен бота-спонсора\n\n"
        "Ссылка для кнопки подписки подставляется автоматически. "
        "Для приватных каналов — бот создаёт её сам.",
        reply_markup=b.as_markup()
    )

# ── Method 1: forward from channel ────────────────────────────────────
@router.callback_query(F.data.startswith("add_sp_forward_"))
async def add_sp_forward_start(callback: CallbackQuery, state: FSMContext):
    chat_id = callback.data.replace("add_sp_forward_", "")
    await state.update_data(sponsor_chat_id=chat_id)
    await state.set_state(BotStates.waiting_for_sponsor_forward)
    b = InlineKeyboardBuilder()
    b.row(back_btn(f"add_sponsor_{chat_id}"))
    await callback.message.edit_text(
        "📨 <b>Добавление спонсора через пересылку</b>\n\n"
        "Перешлите боту любое сообщение из <b>канала-спонсора</b>.\n\n"
        "⚠️ Работает только для каналов. Для групп используйте добавление по ID.\n\n"
        "<b>Важно:</b> бот должен быть администратором в канале-спонсоре.",
        reply_markup=b.as_markup()
    )

@router.message(BotStates.waiting_for_sponsor_forward, F.chat.type == "private")
async def process_sponsor_forward(message: Message, state: FSMContext):
    if not message.forward_from_chat:
        return await message.answer(
            "❌ Нужно именно <b>переслать</b> сообщение из канала.\n\n"
            "Если хотите добавить группу — используйте добавление по ID:\n"
            "зайдите в группу, напишите <code>/id</code>, скопируйте ID и вернитесь."
        )
    channel = message.forward_from_chat
    try:
        me = await bot.get_me()
        bot_member = await bot.get_chat_member(channel.id, me.id)
        is_admin = bot_member.status in ("administrator", "creator")
        if not is_admin:
            return await message.answer(
                "❌ Бот должен быть <b>администратором</b> в канале-спонсоре.\n"
                "Добавьте бота в канал как администратора и попробуйте снова."
            )
    except Exception as e:
        return await message.answer(f"❌ Не удалось проверить доступ к каналу: {e}")

    username = getattr(channel, "username", None)
    auto_link = f"https://t.me/{username}" if username else None

    # Публичный канал — сохраняем сразу без вопросов
    if auto_link:
        data = await state.get_data()
        chat_id = data["sponsor_chat_id"]
        result = await _do_add_sponsor(chat_id, channel.id, channel.title, auto_link, message.from_user.id)
        await state.clear()
        if result == "exists":
            await message.answer(f"ℹ️ <b>{channel.title}</b> уже добавлен как спонсор.",
                                 reply_markup=main_menu_kb(message.from_user.id))
        elif result == "no_chat":
            await message.answer("❌ Чат не найден.", reply_markup=main_menu_kb(message.from_user.id))
        else:
            b = InlineKeyboardBuilder()
            b.row(InlineKeyboardButton(text="➕ Добавить ещё спонсора", callback_data=f"add_sponsor_{chat_id}"))
            b.row(InlineKeyboardButton(text="📋 Список спонсоров",       callback_data=f"list_sponsors_{chat_id}"))
            b.row(back_btn("my_chats"))
            await message.answer(
                f"✅ Спонсор <b>{channel.title}</b> добавлен!\n"
                f"🔗 Ссылка: {auto_link}",
                reply_markup=b.as_markup()
            )
        return

    # Приватный канал — пробуем создать ссылку сами
    auto_invite = None
    try:
        invite = await bot.create_chat_invite_link(channel.id)
        auto_invite = invite.invite_link
    except Exception:
        pass

    if auto_invite:
        data = await state.get_data()
        chat_id = data["sponsor_chat_id"]
        result = await _do_add_sponsor(chat_id, channel.id, channel.title, auto_invite, message.from_user.id)
        await state.clear()
        if result == "exists":
            await message.answer(f"ℹ️ <b>{channel.title}</b> уже добавлен как спонсор.",
                                 reply_markup=main_menu_kb(message.from_user.id))
        elif result == "no_chat":
            await message.answer("❌ Чат не найден.", reply_markup=main_menu_kb(message.from_user.id))
        else:
            b = InlineKeyboardBuilder()
            b.row(InlineKeyboardButton(text="➕ Добавить ещё спонсора", callback_data=f"add_sponsor_{chat_id}"))
            b.row(InlineKeyboardButton(text="📋 Список спонсоров",       callback_data=f"list_sponsors_{chat_id}"))
            b.row(back_btn("my_chats"))
            await message.answer(
                f"✅ Спонсор <b>{channel.title}</b> добавлен!\n"
                f"🔗 Пригласительная ссылка создана автоматически.",
                reply_markup=b.as_markup()
            )
        return

    # Не удалось создать ссылку — просим вручную
    await state.update_data(
        sponsor_channel_id=channel.id,
        sponsor_channel_title=channel.title,
        sponsor_default_link=None,
    )
    await state.set_state(BotStates.waiting_for_sponsor_link)
    await message.answer(
        f"✅ Канал найден: <b>{channel.title}</b>\n\n"
        "⚠️ <b>Канал приватный</b> и не удалось создать ссылку автоматически.\n\n"
        "Отправьте пригласительную ссылку вручную (например: <code>https://t.me/+xxxxxxxx</code>)."
    )

@router.message(BotStates.waiting_for_sponsor_link, F.chat.type == "private")
async def process_sponsor_link(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id    = data["sponsor_chat_id"]
    channel_id = data["sponsor_channel_id"]
    title      = data["sponsor_channel_title"]

    link = message.text.strip() if message.text else ""
    if not link or not link.startswith("http"):
        return await message.answer(
            "❌ Ссылка обязательна для приватного канала.\n\n"
            "Отправьте корректную ссылку, например: <code>https://t.me/+xxxxxxxx</code>"
        )

    result = await _do_add_sponsor(chat_id, channel_id, title, link, message.from_user.id)
    await state.clear()
    if result == "exists":
        await message.answer(f"ℹ️ <b>{title}</b> уже добавлен как спонсор.",
                             reply_markup=main_menu_kb(message.from_user.id))
    elif result == "no_chat":
        await message.answer("❌ Чат не найден.", reply_markup=main_menu_kb(message.from_user.id))
    else:
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(text="➕ Добавить ещё спонсора", callback_data=f"add_sponsor_{chat_id}"))
        b.row(InlineKeyboardButton(text="📋 Список спонсоров",       callback_data=f"list_sponsors_{chat_id}"))
        b.row(back_btn("my_chats"))
        await message.answer(f"✅ Спонсор <b>{title}</b> добавлен!", reply_markup=b.as_markup())

# ── Method 2: by ID ───────────────────────────────────────────────────
@router.callback_query(F.data.startswith("add_sp_by_id_"))
async def add_sp_by_id_start(callback: CallbackQuery, state: FSMContext):
    chat_id = callback.data.replace("add_sp_by_id_", "")
    await state.update_data(sponsor_chat_id=chat_id)
    await state.set_state(BotStates.waiting_for_sponsor_id)
    b = InlineKeyboardBuilder()
    b.row(back_btn(f"add_sponsor_{chat_id}"))
    await callback.message.edit_text(
        "🆔 <b>Добавление спонсора по ID</b>\n\n"
        "Введите ID канала или группы-спонсора.\n\n"
        "<b>Как узнать ID:</b>\n"
        "• Для группы — зайдите в неё и напишите <code>/id</code>\n"
        "• Для публичного канала — можно использовать @username вместо ID\n\n"
        "Пример ID: <code>-1001234567890</code>\n"
        "Пример username: <code>@mychannel</code>",
        reply_markup=b.as_markup()
    )

@router.message(BotStates.waiting_for_sponsor_id, F.chat.type == "private")
async def process_sponsor_id_input(message: Message, state: FSMContext):
    raw = message.text.strip() if message.text else ""
    # support @username too
    try:
        if raw.startswith("@") or (not raw.lstrip("-").isdigit()):
            chat_info = await bot.get_chat(raw)
        else:
            chat_info = await bot.get_chat(int(raw))
    except Exception as e:
        return await message.answer(
            f"❌ Не удалось найти канал/чат: <code>{e}</code>\n\n"
            "Проверьте ID и убедитесь что бот добавлен туда как администратор."
        )

    # check bot is admin
    try:
        me = await bot.get_me()
        bot_member = await bot.get_chat_member(chat_info.id, me.id)
        is_admin = bot_member.status in ("administrator", "creator")
        if not is_admin:
            return await message.answer(
                f"❌ Бот должен быть <b>администратором</b> в <b>{chat_info.title}</b>.\n"
                "Добавьте бота туда как администратора и попробуйте снова."
            )
    except Exception as e:
        return await message.answer(f"❌ Ошибка проверки прав: {e}")

    username = getattr(chat_info, "username", None)
    auto_link = f"https://t.me/{username}" if username else None
    data = await state.get_data()
    chat_id = data["sponsor_chat_id"]

    # Публичный — сохраняем сразу
    if auto_link:
        result = await _do_add_sponsor(chat_id, chat_info.id, chat_info.title, auto_link, message.from_user.id)
        await state.clear()
        if result == "exists":
            await message.answer(f"ℹ️ <b>{chat_info.title}</b> уже добавлен как спонсор.",
                                 reply_markup=main_menu_kb(message.from_user.id))
        elif result == "no_chat":
            await message.answer("❌ Чат не найден.", reply_markup=main_menu_kb(message.from_user.id))
        else:
            b2 = InlineKeyboardBuilder()
            b2.row(InlineKeyboardButton(text="➕ Добавить ещё спонсора", callback_data=f"add_sponsor_{chat_id}"))
            b2.row(InlineKeyboardButton(text="📋 Список спонсоров",       callback_data=f"list_sponsors_{chat_id}"))
            b2.row(back_btn("my_chats"))
            await message.answer(
                f"✅ Спонсор <b>{chat_info.title}</b> добавлен!\n"
                f"🔗 Ссылка: {auto_link}",
                reply_markup=b2.as_markup()
            )
        return

    # Приватный — пробуем создать ссылку сами
    auto_invite = None
    try:
        invite = await bot.create_chat_invite_link(chat_info.id)
        auto_invite = invite.invite_link
    except Exception:
        pass

    if auto_invite:
        result = await _do_add_sponsor(chat_id, chat_info.id, chat_info.title, auto_invite, message.from_user.id)
        await state.clear()
        if result == "exists":
            await message.answer(f"ℹ️ <b>{chat_info.title}</b> уже добавлен как спонсор.",
                                 reply_markup=main_menu_kb(message.from_user.id))
        elif result == "no_chat":
            await message.answer("❌ Чат не найден.", reply_markup=main_menu_kb(message.from_user.id))
        else:
            b2 = InlineKeyboardBuilder()
            b2.row(InlineKeyboardButton(text="➕ Добавить ещё спонсора", callback_data=f"add_sponsor_{chat_id}"))
            b2.row(InlineKeyboardButton(text="📋 Список спонсоров",       callback_data=f"list_sponsors_{chat_id}"))
            b2.row(back_btn("my_chats"))
            await message.answer(
                f"✅ Спонсор <b>{chat_info.title}</b> добавлен!\n"
                f"🔗 Пригласительная ссылка создана автоматически.",
                reply_markup=b2.as_markup()
            )
        return

    # Не удалось — просим вручную
    await state.update_data(
        sponsor_channel_id=chat_info.id,
        sponsor_channel_title=chat_info.title,
        sponsor_default_link=None,
    )
    await state.set_state(BotStates.waiting_for_sponsor_id_link)
    await message.answer(
        f"✅ Найдено: <b>{chat_info.title}</b>\n\n"
        "⚠️ <b>Приватный</b> — не удалось создать ссылку автоматически.\n\n"
        "Отправьте пригласительную ссылку вручную (например: <code>https://t.me/+xxxxxxxx</code>)."
    )

@router.message(BotStates.waiting_for_sponsor_id_link, F.chat.type == "private")
async def process_sponsor_id_link(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id    = data["sponsor_chat_id"]
    channel_id = data["sponsor_channel_id"]
    title      = data["sponsor_channel_title"]

    link = message.text.strip() if message.text else ""
    if not link or not link.startswith("http"):
        return await message.answer(
            "❌ Ссылка обязательна для приватного канала/группы.\n\n"
            "Отправьте корректную ссылку, например: <code>https://t.me/+xxxxxxxx</code>"
        )

    result = await _do_add_sponsor(chat_id, channel_id, title, link, message.from_user.id)
    await state.clear()
    if result == "exists":
        await message.answer(f"ℹ️ <b>{title}</b> уже добавлен как спонсор.",
                             reply_markup=main_menu_kb(message.from_user.id))
    elif result == "no_chat":
        await message.answer("❌ Чат не найден.", reply_markup=main_menu_kb(message.from_user.id))
    else:
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(text="➕ Добавить ещё спонсора", callback_data=f"add_sponsor_{chat_id}"))
        b.row(InlineKeyboardButton(text="📋 Список спонсоров",       callback_data=f"list_sponsors_{chat_id}"))
        b.row(back_btn("my_chats"))
        await message.answer(f"✅ Спонсор <b>{title}</b> добавлен!", reply_markup=b.as_markup())





# ── Method 3: add bot as sponsor ─────────────────────────────────────
@router.callback_query(F.data.startswith("add_sp_bot_"))
async def add_sp_bot_start(callback: CallbackQuery, state: FSMContext):
    chat_id = callback.data.replace("add_sp_bot_", "")
    await state.update_data(sponsor_chat_id=chat_id)
    await state.set_state(BotStates.waiting_for_bot_token)
    b = InlineKeyboardBuilder()
    b.row(back_btn(f"add_sponsor_{chat_id}"))
    await callback.message.edit_text(
        "🤖 <b>Добавление бота как спонсора</b>\n\n"
        "Отправьте <b>API токен</b> бота-спонсора.\n\n"
        "Где взять токен: откройте @BotFather → выберите бота → API Token\n\n"
        "⚠️ Токен будет использоваться только для проверки подписки (запущен ли бот у пользователя).",
        reply_markup=b.as_markup()
    )

@router.message(BotStates.waiting_for_bot_token, F.chat.type == "private")
async def process_bot_token(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data["sponsor_chat_id"]
    token = message.text.strip() if message.text else ""

    # Проверяем токен
    try:
        temp_bot = Bot(token=token, default=DefaultBotProperties(parse_mode="HTML"))
        bot_info = await temp_bot.get_me()
        await temp_bot.session.close()
    except Exception as e:
        return await message.answer(
            f"❌ Неверный токен или бот недоступен: <code>{e}</code>\n\n"
            "Проверьте токен и попробуйте снова."
        )

    title = bot_info.full_name or bot_info.username or "Бот"
    bot_id = bot_info.id
    link = f"https://t.me/{bot_info.username}" if bot_info.username else None

    result = await _do_add_sponsor(chat_id, bot_id, title, link, message.from_user.id,
                                    sponsor_type="bot", bot_token=token)
    await state.clear()
    if result == "exists":
        await message.answer(f"ℹ️ <b>{title}</b> уже добавлен как спонсор.",
                             reply_markup=main_menu_kb(message.from_user.id))
    elif result == "no_chat":
        await message.answer("❌ Чат не найден.", reply_markup=main_menu_kb(message.from_user.id))
    else:
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(text="➕ Добавить ещё спонсора", callback_data=f"add_sponsor_{chat_id}"))
        b.row(InlineKeyboardButton(text="📋 Список спонсоров",       callback_data=f"list_sponsors_{chat_id}"))
        b.row(back_btn("my_chats"))
        await message.answer(
            f"✅ Бот <b>{title}</b> добавлен как спонсор!\n"
            f"Пользователи должны запустить @{bot_info.username} чтобы писать в чате.",
            reply_markup=b.as_markup()
        )

# ── List sponsors ─────────────────────────────────────────────────────
@router.callback_query(F.data.startswith("list_sponsors_"))
async def list_sponsors(callback: CallbackQuery):
    chat_id = callback.data.replace("list_sponsors_", "")
    cdata = db.data["chats"].get(chat_id)
    if not cdata:
        return await callback.answer("Чат не найден.", show_alert=True)

    sponsors = cdata["sponsors"]
    if not sponsors:
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(text="➕ Добавить спонсора", callback_data=f"add_sponsor_{chat_id}"))
        b.row(back_btn(f"manage_chat_{chat_id}"))
        return await callback.message.edit_text(
            "📋 Спонсоров пока нет.", reply_markup=b.as_markup()
        )

    b = InlineKeyboardBuilder()
    for idx, s in enumerate(sponsors):
        status_icon = "✅" if s["active"] else "❌"
        b.row(InlineKeyboardButton(
            text=f"{status_icon} {s['title']}",
            callback_data=f"sp_menu_{chat_id}_{idx}"
        ))
    b.row(InlineKeyboardButton(text="➕ Добавить спонсора", callback_data=f"add_sponsor_{chat_id}"))
    b.row(back_btn(f"manage_chat_{chat_id}"))
    await callback.message.edit_text(
        "📋 <b>Спонсоры чата</b>\n\n"
        "Нажмите на спонсора для управления им:",
        reply_markup=b.as_markup()
    )

# ── Single sponsor menu ───────────────────────────────────────────────
@router.callback_query(F.data.startswith("sp_menu_"))
async def sponsor_menu(callback: CallbackQuery):
    parts = callback.data.replace("sp_menu_", "").rsplit("_", 1)
    chat_id, idx = parts[0], int(parts[1])
    cdata = db.data["chats"].get(chat_id)
    if not cdata or idx >= len(cdata["sponsors"]):
        return await callback.answer("Спонсор не найден.", show_alert=True)

    s = cdata["sponsors"][idx]
    status_icon = "✅ Активен" if s["active"] else "❌ Отключён"
    toggle_text = "🔴 Отключить" if s["active"] else "🟢 Включить"
    link_text = s.get("link") or "не указана"

    sponsor_type = s.get("type", "channel")
    type_label = "🤖 Бот" if sponsor_type == "bot" else "📢 Канал/группа"
    id_label = "ID бота" if sponsor_type == "bot" else "ID канала"

    text = (
        f"⚙️ <b>Спонсор: {s['title']}</b>\n\n"
        f"Тип: {type_label}\n"
        f"🆔 {id_label}: <code>{s['channel_id']}</code>\n"
        f"🔗 Ссылка: {link_text}\n"
        f"Статус: {status_icon}"
    )
    # Показать таймер если установлен
    timer_info = ""
    expire_at = s.get("expire_at")
    if expire_at:
        exp_dt = datetime.fromisoformat(expire_at)
        now_dt = datetime.now()
        if exp_dt > now_dt:
            timer_info = f"\n⏱ Авто-отключение: {exp_dt.strftime('%H:%M %d.%m.%Y')}"
        else:
            timer_info = "\n⏱ Таймер истёк"
    text += timer_info

    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text=toggle_text,                     callback_data=f"sp_toggle_{chat_id}_{idx}"))
    b.row(InlineKeyboardButton(text="⏱ Авто-отключение",             callback_data=f"sp_timer_{chat_id}_{idx}"))
    b.row(InlineKeyboardButton(text="🔗 Изменить ссылку",            callback_data=f"sp_edit_link_{chat_id}_{idx}"))
    b.row(InlineKeyboardButton(text="🆔 Изменить ID канала",         callback_data=f"sp_edit_id_{chat_id}_{idx}"))
    b.row(InlineKeyboardButton(text="🗑 Удалить спонсора",           callback_data=f"sp_delete_{chat_id}_{idx}"))
    b.row(back_btn(f"list_sponsors_{chat_id}"))
    await callback.message.edit_text(text, reply_markup=b.as_markup())


# ── Sponsor timer ─────────────────────────────────────────────────────
@router.callback_query(F.data.startswith("sp_timer_dur_"))
async def sp_timer_dur_start(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.replace("sp_timer_dur_", "").rsplit("_", 1)
    chat_id, idx = parts[0], int(parts[1])
    await state.update_data(sp_timer_chat_id=chat_id, sp_timer_idx=idx)
    await state.set_state(BotStates.waiting_for_sp_timer_duration)
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="15 мин",  callback_data=f"sp_timer_q_15m_{chat_id}_{idx}"),
        InlineKeyboardButton(text="30 мин",  callback_data=f"sp_timer_q_30m_{chat_id}_{idx}"),
        InlineKeyboardButton(text="1 час",   callback_data=f"sp_timer_q_1h_{chat_id}_{idx}"),
    )
    b.row(
        InlineKeyboardButton(text="6 часов", callback_data=f"sp_timer_q_6h_{chat_id}_{idx}"),
        InlineKeyboardButton(text="12 часов",callback_data=f"sp_timer_q_12h_{chat_id}_{idx}"),
        InlineKeyboardButton(text="24 часа", callback_data=f"sp_timer_q_24h_{chat_id}_{idx}"),
    )
    b.row(
        InlineKeyboardButton(text="2 дня",   callback_data=f"sp_timer_q_2d_{chat_id}_{idx}"),
        InlineKeyboardButton(text="7 дней",  callback_data=f"sp_timer_q_7d_{chat_id}_{idx}"),
        InlineKeyboardButton(text="30 дней", callback_data=f"sp_timer_q_30d_{chat_id}_{idx}"),
    )
    b.row(back_btn(f"sp_timer_{chat_id}_{idx}"))
    await callback.message.edit_text(
        "⏳ <b>Таймер — через сколько отключить?</b>\n\n"
        "Выберите быстрый вариант или введите вручную:\n"
        "• <code>30</code> — через 30 минут\n"
        "• <code>3h</code> — через 3 часа\n"
        "• <code>2d</code> — через 2 дня",
        reply_markup=b.as_markup()
    )

@router.callback_query(F.data.startswith("sp_timer_q_"))
async def sp_timer_quick(callback: CallbackQuery):
    raw = callback.data.replace("sp_timer_q_", "")
    parts = raw.split("_", 1)
    time_code = parts[0]
    rest = parts[1].rsplit("_", 1)
    chat_id, idx = rest[0], int(rest[1])
    from datetime import timedelta
    units = {"m": 1, "h": 60, "d": 1440}
    unit = time_code[-1]
    amount = int(time_code[:-1])
    expire_dt = datetime.now() + timedelta(minutes=amount * units.get(unit, 1))
    cdata = db.data["chats"].get(chat_id)
    if not cdata or idx >= len(cdata["sponsors"]):
        return await callback.answer("Ошибка.", show_alert=True)
    cdata["sponsors"][idx]["expire_at"] = expire_dt.isoformat()
    db.save()
    await callback.answer(f"✅ Таймер: {expire_dt.strftime('%H:%M %d.%m.%Y')}", show_alert=True)
    callback.data = f"sp_menu_{chat_id}_{idx}"
    await sponsor_menu(callback)

@router.message(BotStates.waiting_for_sp_timer_duration, F.chat.type == "private")
async def sp_timer_dur_input(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data["sp_timer_chat_id"]
    idx     = data["sp_timer_idx"]
    txt     = message.text.strip().lower() if message.text else ""
    from datetime import timedelta
    try:
        if txt.endswith("d"):
            delta = timedelta(days=int(txt[:-1]))
        elif txt.endswith("h"):
            delta = timedelta(hours=int(txt[:-1]))
        else:
            delta = timedelta(minutes=int(txt))
    except Exception:
        return await message.answer(
            "❌ Неверный формат.\n\n"
            "Примеры: <code>30</code> (мин), <code>3h</code> (часа), <code>2d</code> (дня)"
        )
    expire_dt = datetime.now() + delta
    cdata = db.data["chats"].get(chat_id)
    if not cdata or idx >= len(cdata["sponsors"]):
        await state.clear()
        return await message.answer("Ошибка: спонсор не найден.")
    cdata["sponsors"][idx]["expire_at"] = expire_dt.isoformat()
    db.save()
    await state.clear()
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="⚙️ К спонсору", callback_data=f"sp_menu_{chat_id}_{idx}"))
    await message.answer(
        f"✅ Таймер установлен!\n\nСпонсор отключится: <b>{expire_dt.strftime('%H:%M %d.%m.%Y')}</b>",
        reply_markup=b.as_markup()
    )

@router.callback_query(F.data.startswith("sp_timer_dt_"))
async def sp_timer_dt_start(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.replace("sp_timer_dt_", "").rsplit("_", 1)
    chat_id, idx = parts[0], int(parts[1])
    await state.update_data(sp_timer_chat_id=chat_id, sp_timer_idx=idx)
    await state.set_state(BotStates.waiting_for_sp_timer_datetime)
    b = InlineKeyboardBuilder()
    b.row(back_btn(f"sp_timer_{chat_id}_{idx}"))
    await callback.message.edit_text(
        "📅 <b>Дата и время отключения</b>\n\n"
        "Введите в формате: <code>ЧЧ:ММ ДД ММ ГГГГ</code>\n\n"
        "Примеры:\n"
        "• <code>12:00 14 3 2026</code>\n"
        "• <code>23:59 31 12 2026</code>",
        reply_markup=b.as_markup()
    )

@router.message(BotStates.waiting_for_sp_timer_datetime, F.chat.type == "private")
async def sp_timer_dt_input(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data["sp_timer_chat_id"]
    idx     = data["sp_timer_idx"]
    txt     = message.text.strip() if message.text else ""
    try:
        expire_dt = datetime.strptime(txt, "%H:%M %d %m %Y")
    except Exception:
        return await message.answer(
            "❌ Неверный формат.\n\n"
            "Используйте: <code>ЧЧ:ММ ДД ММ ГГГГ</code>\n"
            "Пример: <code>12:00 14 3 2026</code>"
        )
    if expire_dt <= datetime.now():
        return await message.answer("❌ Указанная дата уже прошла. Введите будущую дату.")
    cdata = db.data["chats"].get(chat_id)
    if not cdata or idx >= len(cdata["sponsors"]):
        await state.clear()
        return await message.answer("Ошибка: спонсор не найден.")
    cdata["sponsors"][idx]["expire_at"] = expire_dt.isoformat()
    db.save()
    await state.clear()
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="⚙️ К спонсору", callback_data=f"sp_menu_{chat_id}_{idx}"))
    await message.answer(
        f"✅ Таймер установлен!\n\nСпонсор отключится: <b>{expire_dt.strftime('%H:%M %d.%m.%Y')}</b>",
        reply_markup=b.as_markup()
    )

@router.callback_query(F.data.startswith("sp_timer_cancel_"))
async def sp_timer_cancel(callback: CallbackQuery):
    parts = callback.data.replace("sp_timer_cancel_", "").rsplit("_", 1)
    chat_id, idx = parts[0], int(parts[1])
    cdata = db.data["chats"].get(chat_id)
    if not cdata or idx >= len(cdata["sponsors"]):
        return await callback.answer("Ошибка.", show_alert=True)
    cdata["sponsors"][idx].pop("expire_at", None)
    db.save()
    await callback.answer("✅ Таймер отменён.", show_alert=True)
    callback.data = f"sp_menu_{chat_id}_{idx}"
    await sponsor_menu(callback)

@router.callback_query(F.data.startswith("sp_timer_"))
async def sp_timer_menu(callback: CallbackQuery):
    parts = callback.data.replace("sp_timer_", "").rsplit("_", 1)
    chat_id, idx = parts[0], int(parts[1])
    cdata = db.data["chats"].get(chat_id)
    if not cdata or idx >= len(cdata["sponsors"]):
        return await callback.answer("Ошибка.", show_alert=True)
    s = cdata["sponsors"][idx]
    expire_at = s.get("expire_at")
    current = ""
    if expire_at:
        exp_dt = datetime.fromisoformat(expire_at)
        if exp_dt > datetime.now():
            current = f"\n\nТекущий таймер: <b>{exp_dt.strftime('%H:%M %d.%m.%Y')}</b>"
        else:
            current = "\n\nТаймер уже истёк."
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="⏳ Таймер (через N минут/часов/дней)", callback_data=f"sp_timer_dur_{chat_id}_{idx}"))
    b.row(InlineKeyboardButton(text="📅 Конкретная дата и время",           callback_data=f"sp_timer_dt_{chat_id}_{idx}"))
    if expire_at:
        b.row(InlineKeyboardButton(text="❌ Отменить таймер",               callback_data=f"sp_timer_cancel_{chat_id}_{idx}"))
    b.row(back_btn(f"sp_menu_{chat_id}_{idx}"))
    await callback.message.edit_text(
        f"⏱ <b>Авто-отключение спонсора «{s['title']}»</b>{current}\n\nВыберите способ установки таймера:",
        reply_markup=b.as_markup()
    )

# ── Toggle active ─────────────────────────────────────────────────────
@router.callback_query(F.data.startswith("sp_toggle_"))
async def toggle_sponsor(callback: CallbackQuery):
    parts = callback.data.replace("sp_toggle_", "").rsplit("_", 1)
    chat_id, idx = parts[0], int(parts[1])
    cdata = db.data["chats"].get(chat_id)
    if not cdata or idx >= len(cdata["sponsors"]):
        return await callback.answer("Ошибка.", show_alert=True)
    cdata["sponsors"][idx]["active"] = not cdata["sponsors"][idx]["active"]
    db.save()
    status = "активирован" if cdata["sponsors"][idx]["active"] else "деактивирован"
    logger.info(f"Sponsor {cdata['sponsors'][idx]['title']} {status} in chat {chat_id}")
    await callback.answer(f"Спонсор {status}.")
    await sponsor_menu(callback)

# ── Delete sponsor ────────────────────────────────────────────────────
@router.callback_query(F.data.startswith("sp_delete_"))
async def delete_sponsor_confirm(callback: CallbackQuery):
    parts = callback.data.replace("sp_delete_", "").rsplit("_", 1)
    chat_id, idx = parts[0], int(parts[1])
    cdata = db.data["chats"].get(chat_id)
    if not cdata or idx >= len(cdata["sponsors"]):
        return await callback.answer("Ошибка.", show_alert=True)
    s = cdata["sponsors"][idx]
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="✅ Да, удалить",  callback_data=f"sp_del_yes_{chat_id}_{idx}"),
        InlineKeyboardButton(text="❌ Отмена",        callback_data=f"sp_menu_{chat_id}_{idx}")
    )
    await callback.message.edit_text(
        f"🗑 <b>Удалить спонсора «{s['title']}»?</b>\n\nЭто действие необратимо.",
        reply_markup=b.as_markup()
    )

@router.callback_query(F.data.startswith("sp_del_yes_"))
async def delete_sponsor_execute(callback: CallbackQuery):
    parts = callback.data.replace("sp_del_yes_", "").rsplit("_", 1)
    chat_id, idx = parts[0], int(parts[1])
    cdata = db.data["chats"].get(chat_id)
    if not cdata or idx >= len(cdata["sponsors"]):
        return await callback.answer("Ошибка.", show_alert=True)
    title = cdata["sponsors"][idx]["title"]
    cid   = cdata["sponsors"][idx]["channel_id"]
    cdata["sponsors"].pop(idx)
    db.save()
    invalidate_sponsor_cache(cid)
    logger.info(f"Sponsor {title} deleted from chat {chat_id}")
    await callback.answer(f"Спонсор «{title}» удалён.", show_alert=True)
    # fake callback.data to refresh list
    callback.data = f"list_sponsors_{chat_id}"
    await list_sponsors(callback)

# ── Edit sponsor link ─────────────────────────────────────────────────
@router.callback_query(F.data.startswith("sp_edit_link_"))
async def edit_sponsor_link_start(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.replace("sp_edit_link_", "").rsplit("_", 1)
    chat_id, idx = parts[0], int(parts[1])
    cdata = db.data["chats"].get(chat_id)
    if not cdata or idx >= len(cdata["sponsors"]):
        return await callback.answer("Ошибка.", show_alert=True)
    s = cdata["sponsors"][idx]
    await state.update_data(edit_sp_chat_id=chat_id, edit_sp_idx=idx)
    await state.set_state(BotStates.waiting_for_edit_sp_link)
    b = InlineKeyboardBuilder()
    b.row(back_btn(f"sp_menu_{chat_id}_{idx}"))
    await callback.message.edit_text(
        f"🔗 <b>Изменение ссылки спонсора «{s['title']}»</b>\n\n"
        f"Текущая ссылка: {s.get('link') or 'не указана'}\n\n"
        "Введите новую ссылку или /skip чтобы убрать ссылку:",
        reply_markup=b.as_markup()
    )

@router.message(BotStates.waiting_for_edit_sp_link, F.chat.type == "private")
async def edit_sponsor_link_save(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data["edit_sp_chat_id"]
    idx     = data["edit_sp_idx"]
    cdata = db.data["chats"].get(chat_id)
    if not cdata or idx >= len(cdata["sponsors"]):
        await state.clear()
        return await message.answer("Ошибка: спонсор не найден.")
    new_link = None if (message.text and message.text.strip() == "/skip")                else (message.text.strip() if message.text else None)
    cdata["sponsors"][idx]["link"] = new_link
    db.save()
    await state.clear()
    title = cdata["sponsors"][idx]["title"]
    logger.info(f"Sponsor {title} link updated in chat {chat_id}")
    await message.answer(
        f"✅ Ссылка спонсора «{title}» обновлена!",
        reply_markup=main_menu_kb(message.from_user.id)
    )

# ── Edit sponsor channel ID ───────────────────────────────────────────
@router.callback_query(F.data.startswith("sp_edit_id_"))
async def edit_sponsor_id_start(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.replace("sp_edit_id_", "").rsplit("_", 1)
    chat_id, idx = parts[0], int(parts[1])
    cdata = db.data["chats"].get(chat_id)
    if not cdata or idx >= len(cdata["sponsors"]):
        return await callback.answer("Ошибка.", show_alert=True)
    s = cdata["sponsors"][idx]
    await state.update_data(edit_sp_chat_id=chat_id, edit_sp_idx=idx)
    await state.set_state(BotStates.waiting_for_edit_sp_id)
    b = InlineKeyboardBuilder()
    b.row(back_btn(f"sp_menu_{chat_id}_{idx}"))
    await callback.message.edit_text(
        f"🆔 <b>Изменение ID канала спонсора «{s['title']}»</b>\n\n"
        f"Текущий ID: <code>{s['channel_id']}</code>\n\n"
        "Введите новый ID канала (например: <code>-1001234567890</code>)\n"
        "или @username для публичного канала:",
        reply_markup=b.as_markup()
    )

@router.message(BotStates.waiting_for_edit_sp_id, F.chat.type == "private")
async def edit_sponsor_id_save(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data["edit_sp_chat_id"]
    idx     = data["edit_sp_idx"]
    cdata = db.data["chats"].get(chat_id)
    if not cdata or idx >= len(cdata["sponsors"]):
        await state.clear()
        return await message.answer("Ошибка: спонсор не найден.")

    raw = message.text.strip() if message.text else ""
    try:
        if raw.startswith("@") or not raw.lstrip("-").isdigit():
            chat_info = await bot.get_chat(raw)
        else:
            chat_info = await bot.get_chat(int(raw))
        new_id = chat_info.id
        new_title = chat_info.title
    except Exception as e:
        return await message.answer(f"❌ Не удалось найти канал: {e}\nПроверьте ID и попробуйте снова.")

    old_id = cdata["sponsors"][idx]["channel_id"]
    cdata["sponsors"][idx]["channel_id"] = new_id
    cdata["sponsors"][idx]["title"] = new_title
    db.save()
    invalidate_sponsor_cache(old_id)
    invalidate_sponsor_cache(new_id)
    await state.clear()
    logger.info(f"Sponsor ID changed {old_id} -> {new_id} in chat {chat_id}")
    await message.answer(
        f"✅ ID спонсора обновлён!\n"
        f"Теперь это канал: <b>{new_title}</b> (<code>{new_id}</code>)",
        reply_markup=main_menu_kb(message.from_user.id)
    )



# ══════════════════════════════════════════════════════════════════════
#  SECTION 7 — PRO REQUEST (user)
# ══════════════════════════════════════════════════════════════════════
@router.callback_query(F.data.startswith("req_pro_"))
async def request_pro(callback: CallbackQuery):
    chat_id = callback.data.replace("req_pro_", "")
    cdata = db.data["chats"].get(chat_id)
    if not cdata:
        return await callback.answer("Чат не найден.", show_alert=True)
    if cdata["owner_id"] != callback.from_user.id:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    if cdata.get("mode") == "PRO":
        return await callback.answer("Чат уже в PRO режиме!", show_alert=True)

    # Проверка дубликата
    for req in db.data.get("pro_requests", []):
        if str(req["chat_id"]) == chat_id:
            return await callback.answer("Запрос уже отправлен, ожидайте рассмотрения.", show_alert=True)

    db.data.setdefault("pro_requests", []).append({
        "chat_id":        int(chat_id),
        "owner_id":       callback.from_user.id,
        "owner_username": callback.from_user.username or "",
        "chat_title":     cdata["title"],
        "created_at":     datetime.now().isoformat(),
    })
    db.save()
    logger.info(f"PRO request from user {callback.from_user.id} for chat {chat_id}")

    try:
        await bot.send_message(
            ADMIN_ID,
            f"📝 <b>Новый запрос на PRO</b>\n\n"
            f"💬 Чат: <b>{cdata['title']}</b> (<code>{chat_id}</code>)\n"
            f"👤 Владелец: @{callback.from_user.username or callback.from_user.id}\n"
            f"📅 Дата: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
    except Exception:
        pass

    await callback.answer("✅ Запрос на PRO отправлен администратору!", show_alert=True)

# ══════════════════════════════════════════════════════════════════════
#  SECTION 8 — ADMIN: STATS
# ══════════════════════════════════════════════════════════════════════
@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    s     = db.data["stats"]
    chats = db.data["chats"]
    free  = sum(1 for c in chats.values() if c["mode"] == "FREE")
    pro   = sum(1 for c in chats.values() if c["mode"] == "PRO")

    all_sponsors = [s for c in chats.values() for s in c.get("sponsors", [])]
    active_spon  = sum(1 for sp in all_sponsors if sp.get("active"))
    inactive_spon= len(all_sponsors) - active_spon

    gs       = db.data["global_sponsor"]
    gs_status= "✅ Активен" if gs["active"] else "❌ Отключён"
    today_u  = db.count_today_users()

    text = (
        "📊 <b>СТАТИСТИКА БОТА</b>\n\n"
        f"👥 <b>Пользователи:</b>\n"
        f"• Всего в базе: {s['total_users']:,}\n"
        f"• Уникальных за сегодня: {today_u}\n\n"
        f"💬 <b>Чаты:</b>\n"
        f"• Всего чатов: {len(chats)}\n"
        f"• FREE режим: {free}\n"
        f"• PRO режим: {pro}\n\n"
        f"📢 <b>Спонсоры:</b>\n"
        f"• Глобальный спонсор: {gs_status}\n"
        f"• Всего добавлено спонсоров: {s['total_sponsors']}\n"
        f"• Активных спонсоров: {active_spon}\n"
        f"• Неактивных: {inactive_spon}\n\n"
        f"📈 <b>Активность:</b>\n"
        f"• Проверено сообщений: {s['total_messages_checked']:,}\n"
        f"• Удалено сообщений: {s['total_messages_deleted']:,}"
    )
    b = InlineKeyboardBuilder()
    b.row(back_btn("admin_menu"))
    await callback.message.edit_text(text, reply_markup=b.as_markup())

# ══════════════════════════════════════════════════════════════════════
#  SECTION 9 — ADMIN: GLOBAL SPONSOR
# ══════════════════════════════════════════════════════════════════════
@router.callback_query(F.data == "admin_global")
async def admin_global_menu(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    gs = db.data["global_sponsor"]
    status = "✅ Активен" if gs["active"] else "❌ Отключён"
    text = (
        f"⚙️ <b>Глобальный спонсор</b>\n\n"
        f"📛 Название: {gs['name']}\n"
        f"🔗 Ссылка: {gs['link']}\n"
        f"🆔 ID канала: <code>{gs['channel_id']}</code>\n"
        f"Статус: {status}"
    )
    toggle_text = "🔴 Выключить" if gs["active"] else "🟢 Включить"
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="✏️ Изменить название",  callback_data="global_edit_name"))
    b.row(InlineKeyboardButton(text="🔗 Изменить ссылку",    callback_data="global_edit_link"))
    b.row(InlineKeyboardButton(text="🆔 Изменить ID канала", callback_data="global_edit_id"))
    b.row(InlineKeyboardButton(text=toggle_text,             callback_data="global_toggle"))
    b.row(back_btn("admin_menu"))
    await callback.message.edit_text(text, reply_markup=b.as_markup())

@router.callback_query(F.data == "global_toggle")
async def global_toggle(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    gs = db.data["global_sponsor"]
    gs["active"] = not gs["active"]
    db.save()
    # Invalidate cache so changes take effect immediately
    invalidate_sponsor_cache(gs["channel_id"])
    status = "включён" if gs["active"] else "отключён"
    logger.info(f"Global sponsor {status} by admin")
    await callback.answer(f"Глобальный спонсор {status}.")
    await admin_global_menu(callback)

@router.callback_query(F.data == "global_edit_name")
async def global_edit_name_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    await state.set_state(BotStates.waiting_for_global_name)
    await callback.message.edit_text("Введите новое <b>название</b> глобального спонсора:")

@router.message(BotStates.waiting_for_global_name, F.chat.type == "private")
async def global_edit_name(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    db.data["global_sponsor"]["name"] = message.text.strip()
    db.save()
    await state.clear()
    logger.info(f"Global sponsor name updated: {message.text.strip()}")
    await message.answer("✅ Название обновлено!", reply_markup=main_menu_kb(ADMIN_ID))

@router.callback_query(F.data == "global_edit_link")
async def global_edit_link_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    await state.set_state(BotStates.waiting_for_global_link)
    await callback.message.edit_text("Введите новую <b>ссылку</b> для кнопки глобального спонсора:")

@router.message(BotStates.waiting_for_global_link, F.chat.type == "private")
async def global_edit_link(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    db.data["global_sponsor"]["link"] = message.text.strip()
    db.save()
    await state.clear()
    await message.answer("✅ Ссылка обновлена!", reply_markup=main_menu_kb(ADMIN_ID))

@router.callback_query(F.data == "global_edit_id")
async def global_edit_id_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    await state.set_state(BotStates.waiting_for_global_id)
    await callback.message.edit_text(
        "Введите новый <b>ID канала</b> глобального спонсора "
        "(формат: <code>-100XXXXXXXXXX</code>):"
    )

@router.message(BotStates.waiting_for_global_id, F.chat.type == "private")
async def global_edit_id(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    try:
        new_id = int(message.text.strip())
    except ValueError:
        return await message.answer("❌ Неверный формат ID. Введите число, например <code>-1001234567890</code>.")
    old_id = db.data["global_sponsor"].get("channel_id")
    db.data["global_sponsor"]["channel_id"] = new_id
    db.save()
    await state.clear()
    # Invalidate cache for both old and new channel so checks happen immediately
    if old_id:
        invalidate_sponsor_cache(old_id)
    invalidate_sponsor_cache(new_id)
    logger.info(f"Global sponsor channel_id changed: {old_id} -> {new_id}")
    await message.answer("✅ ID канала обновлён!", reply_markup=main_menu_kb(ADMIN_ID))

# ══════════════════════════════════════════════════════════════════════
#  SECTION 11 — ADMIN: PRO REQUESTS
# ══════════════════════════════════════════════════════════════════════
@router.callback_query(F.data == "admin_pro_reqs")
async def admin_pro_reqs(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    reqs = db.data.get("pro_requests", [])
    b = InlineKeyboardBuilder()
    if not reqs:
        b.row(back_btn("admin_menu"))
        return await callback.message.edit_text("📝 Нет активных запросов на PRO.", reply_markup=b.as_markup())
    for i, req in enumerate(reqs):
        title = req.get("chat_title", str(req["chat_id"]))
        owner = req.get("owner_username") or str(req.get("owner_id", "?"))
        b.row(InlineKeyboardButton(
            text=f"📋 {title} (@{owner})",
            callback_data=f"view_pro_req_{i}"
        ))
    b.row(back_btn("admin_menu"))
    await callback.message.edit_text(
        f"📝 <b>Запросы на PRO</b> ({len(reqs)} шт.)\n\nВыберите запрос:",
        reply_markup=b.as_markup()
    )

@router.callback_query(F.data.startswith("view_pro_req_"))
async def view_pro_req(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    idx = int(callback.data.replace("view_pro_req_", ""))
    reqs = db.data.get("pro_requests", [])
    if idx >= len(reqs):
        return await callback.answer("Запрос не найден.", show_alert=True)
    req = reqs[idx]
    chat_id = str(req["chat_id"])
    cdata = db.data["chats"].get(chat_id, {})
    title = req.get("chat_title", cdata.get("title", "—"))
    owner_un = req.get("owner_username", "")
    created = req.get("created_at", "—")[:16].replace("T", " ")
    current_mode = cdata.get("mode", "FREE")
    text = (
        f"📋 <b>Запрос на PRO</b>\n\n"
        f"💬 Чат: <b>{title}</b>\n"
        f"🆔 ID: <code>{chat_id}</code>\n"
        f"👤 Владелец: @{owner_un or '—'} (<code>{req['owner_id']}</code>)\n"
        f"⚡ Текущий режим: <b>{current_mode}</b>\n"
        f"📅 Дата заявки: {created}"
    )
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="🔗 Связаться с владельцем", url=f"tg://user?id={req['owner_id']}"))
    b.row(
        InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_pro_{idx}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_pro_{idx}")
    )
    b.row(back_btn("admin_pro_reqs"))
    await callback.message.edit_text(text, reply_markup=b.as_markup())

@router.callback_query(F.data.startswith("approve_pro_"))
async def approve_pro(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    idx = int(callback.data.replace("approve_pro_", ""))
    reqs = db.data.get("pro_requests", [])
    if idx >= len(reqs):
        return await callback.answer("Запрос не найден.", show_alert=True)
    req = reqs.pop(idx)
    chat_id = str(req["chat_id"])
    if chat_id in db.data["chats"]:
        db.data["chats"][chat_id]["mode"] = "PRO"
    db.save()
    logger.info(f"PRO approved for chat {chat_id} by admin")
    try:
        title = db.data["chats"].get(chat_id, {}).get("title", chat_id)
        await bot.send_message(
            req["owner_id"],
            f"🎉 <b>Запрос одобрен!</b>\n\nЧат <b>{title}</b> переведён в режим <b>PRO</b>.\n\n"
            "Теперь глобальный спонсор для этого чата отключён."
        )
    except Exception:
        pass
    await callback.answer("✅ PRO режим одобрен!", show_alert=True)
    callback.data = "admin_pro_reqs"
    await admin_pro_reqs(callback)

@router.callback_query(F.data.startswith("reject_pro_"))
async def reject_pro(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    idx = int(callback.data.replace("reject_pro_", ""))
    reqs = db.data.get("pro_requests", [])
    if idx >= len(reqs):
        return await callback.answer("Запрос не найден.", show_alert=True)
    req = reqs.pop(idx)
    db.save()
    logger.info(f"PRO request rejected for chat {req['chat_id']} by admin")
    try:
        title = db.data["chats"].get(str(req["chat_id"]), {}).get("title", str(req["chat_id"]))
        await bot.send_message(
            req["owner_id"],
            f"❌ <b>Запрос отклонён.</b>\n\n"
            f"Запрос на PRO для чата <b>{title}</b> был отклонён.\n"
            "Свяжитесь с администратором для получения подробностей."
        )
    except Exception:
        pass
    await callback.answer("❌ Запрос отклонён.", show_alert=True)
    callback.data = "admin_pro_reqs"
    await admin_pro_reqs(callback)

# ══════════════════════════════════════════════════════════════════════
#  SECTION 10 — ADMIN: MODE MANAGEMENT
# ══════════════════════════════════════════════════════════════════════
@router.callback_query(F.data == "admin_modes")
async def admin_modes(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    chats = db.data["chats"]
    if not chats:
        b = InlineKeyboardBuilder()
        b.row(back_btn("main_menu"))
        return await callback.message.edit_text("Чатов пока нет.", reply_markup=b.as_markup())

    b = InlineKeyboardBuilder()
    for cid, cdata in chats.items():
        mode_icon = "⚡" if cdata["mode"] == "PRO" else "🆓"
        b.row(InlineKeyboardButton(
            text=f"{mode_icon} {cdata['title']} → переключить",
            callback_data=f"switch_mode_{cid}"
        ))
    b.row(back_btn("admin_menu"))
    await callback.message.edit_text(
        "⚡ <b>Управление режимами чатов</b>\n\n"
        "Нажмите на чат, чтобы переключить режим (FREE ↔ PRO):",
        reply_markup=b.as_markup()
    )

@router.callback_query(F.data.startswith("switch_mode_"))
async def switch_mode(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    chat_id = callback.data.replace("switch_mode_", "")
    cdata = db.data["chats"].get(chat_id)
    if not cdata:
        return await callback.answer("Чат не найден.", show_alert=True)

    old_mode = cdata["mode"]
    new_mode = "PRO" if old_mode == "FREE" else "FREE"
    cdata["mode"] = new_mode

    # remove pending pro request if switching to PRO
    if new_mode == "PRO":
        db.data["pro_requests"] = [
            r for r in db.data["pro_requests"] if str(r["chat_id"]) != chat_id
        ]

    db.save()
    logger.info(f"Chat {chat_id} mode switched: {old_mode} → {new_mode} by admin")
    await callback.answer(f"Режим чата «{cdata['title']}» изменён на {new_mode}.", show_alert=True)
    await admin_modes(callback)



# ══════════════════════════════════════════════════════════════════════
#  SECTION 11 — ADMIN: ALL CHATS
# ══════════════════════════════════════════════════════════════════════
@router.callback_query(F.data == "admin_all_chats")
async def admin_all_chats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    chats = db.data["chats"]
    if not chats:
        b = InlineKeyboardBuilder()
        b.row(back_btn("main_menu"))
        return await callback.message.edit_text("Чатов пока нет.", reply_markup=b.as_markup())
    b = InlineKeyboardBuilder()
    for cid, cdata in chats.items():
        mode_icon = "⚡" if cdata["mode"] == "PRO" else "🆓"
        sp_count = len(cdata.get("sponsors", []))
        owner = cdata.get("owner_username") or str(cdata.get("owner_id", "?"))
        b.row(InlineKeyboardButton(
            text=f"{mode_icon} {cdata['title']} | @{owner} | 📢{sp_count}",
            callback_data=f"admin_chat_{cid}"
        ))
    b.row(back_btn("admin_menu"))
    await callback.message.edit_text(
        f"🗂 <b>Все чаты</b> ({len(chats)} шт.)\n\nВыберите чат для управления:",
        reply_markup=b.as_markup()
    )

@router.callback_query(F.data.startswith("admin_chat_"))
async def admin_manage_single_chat(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    chat_id = callback.data.replace("admin_chat_", "")
    cdata = db.data["chats"].get(chat_id)
    if not cdata:
        return await callback.answer("Чат не найден.", show_alert=True)
    active_sponsors = sum(1 for s in cdata["sponsors"] if s.get("active"))
    owner = cdata.get("owner_username") or str(cdata.get("owner_id", "?"))
    text = (
        f"⚙️ <b>Управление чатом:</b> {cdata['title']}\n"
        f"🆔 ID: <code>{chat_id}</code>\n"
        f"👤 Владелец: @{owner}\n"
        f"⚡ Режим: <b>{cdata['mode']}</b>\n"
        f"📢 Спонсоров: {len(cdata['sponsors'])} (активных: {active_sponsors})\n\n"
        f"📝 Приветствие:\n<i>{cdata['welcome_text']}</i>"
    )
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="✏️ Изменить приветствие",  callback_data=f"edit_welcome_{chat_id}"))
    b.row(InlineKeyboardButton(text="➕ Добавить спонсора",      callback_data=f"add_sponsor_{chat_id}"))
    b.row(InlineKeyboardButton(text="📋 Список спонсоров",       callback_data=f"list_sponsors_{chat_id}"))
    mode_toggle = "⚡ → FREE" if cdata["mode"] == "PRO" else "⚡ → PRO"
    b.row(InlineKeyboardButton(text=f"🔄 Режим: {mode_toggle}",  callback_data=f"admin_toggle_mode_{chat_id}"))
    b.row(InlineKeyboardButton(text="🗑 Удалить чат",             callback_data=f"admin_delete_chat_{chat_id}"))
    b.row(back_btn("admin_all_chats"))
    await callback.message.edit_text(text, reply_markup=b.as_markup())

@router.callback_query(F.data.startswith("admin_toggle_mode_"))
async def admin_toggle_mode(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    chat_id = callback.data.replace("admin_toggle_mode_", "")
    cdata = db.data["chats"].get(chat_id)
    if not cdata:
        return await callback.answer("Чат не найден.", show_alert=True)
    old_mode = cdata["mode"]
    cdata["mode"] = "PRO" if old_mode == "FREE" else "FREE"
    db.save()
    logger.info(f"Admin toggled chat {chat_id} mode: {old_mode} → {cdata['mode']}")
    await callback.answer(f"Режим изменён на {cdata['mode']}.", show_alert=True)
    callback.data = f"admin_chat_{chat_id}"
    await admin_manage_single_chat(callback)

@router.callback_query(F.data.startswith("admin_delete_chat_"))
async def admin_delete_chat_confirm(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    chat_id = callback.data.replace("admin_delete_chat_", "")
    cdata = db.data["chats"].get(chat_id)
    if not cdata:
        return await callback.answer("Чат не найден.", show_alert=True)
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"admin_del_yes_{chat_id}"),
        InlineKeyboardButton(text="❌ Отмена",       callback_data=f"admin_chat_{chat_id}")
    )
    await callback.message.edit_text(
        f"🗑 <b>Удалить чат «{cdata['title']}»?</b>\n\nЭто удалит чат и всех его спонсоров из базы.",
        reply_markup=b.as_markup()
    )

@router.callback_query(F.data.startswith("admin_del_yes_"))
async def admin_delete_chat_execute(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    chat_id = callback.data.replace("admin_del_yes_", "")
    cdata = db.data["chats"].pop(chat_id, None)
    if cdata:
        db.save()
        logger.info(f"Admin deleted chat {chat_id} ({cdata.get('title')})")
        await callback.answer(f"Чат «{cdata['title']}» удалён.", show_alert=True)
    callback.data = "admin_all_chats"
    await admin_all_chats(callback)

# ══════════════════════════════════════════════════════════════════════
#  SECTION 12 — ADMIN: BROADCAST
# ══════════════════════════════════════════════════════════════════════
@router.callback_query(F.data == "admin_broadcast_start")
async def broadcast_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    free_count = sum(1 for c in db.data["chats"].values() if c["mode"] == "FREE")
    b = InlineKeyboardBuilder()
    b.row(back_btn("admin_menu"))
    await callback.message.edit_text(
        f"📢 <b>Рассылка в FREE-чаты</b>\n\n"
        f"Будет отправлено в <b>{free_count}</b> чат(ов) с режимом FREE.\n\n"
        "Введите текст сообщения для рассылки:",
        reply_markup=b.as_markup()
    )
    await state.set_state(BotStates.waiting_for_broadcast)

@router.message(BotStates.waiting_for_broadcast, F.chat.type == "private")
async def broadcast_preview(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    await state.update_data(broadcast_text=message.text)
    free_chats = [cid for cid, c in db.data["chats"].items() if c["mode"] == "FREE"]
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text=f"✅ Отправить в {len(free_chats)} FREE-чатов", callback_data="broadcast_confirm"),
        InlineKeyboardButton(text="❌ Отменить", callback_data="main_menu")
    )
    await message.answer(
        f"📋 <b>Предпросмотр рассылки:</b>\n\n{message.text}\n\n"
        f"Подтвердите отправку в <b>{len(free_chats)}</b> чат(ов):",
        reply_markup=b.as_markup()
    )

@router.callback_query(F.data == "broadcast_confirm")
async def broadcast_confirm(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    data = await state.get_data()
    text = data.get("broadcast_text", "")
    await state.clear()

    free_chats = [(cid, c) for cid, c in db.data["chats"].items() if c["mode"] == "FREE"]
    total = len(free_chats)
    success = 0
    fail    = 0

    progress_msg = await callback.message.edit_text(
        f"📢 Рассылка начата...\n\nОтправлено 0/{total} чатов..."
    )

    for i, (cid, _) in enumerate(free_chats, 1):
        try:
            await bot.send_message(int(cid), text)
            success += 1
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            logger.warning(f"Broadcast failed for chat {cid}: {e}")
            fail += 1
        except Exception as e:
            logger.error(f"Broadcast error for chat {cid}: {e}")
            fail += 1

        if i % 10 == 0:
            try:
                await progress_msg.edit_text(
                    f"📢 Рассылка...\n\nОтправлено {i}/{total} чатов..."
                )
            except Exception:
                pass
        await asyncio.sleep(0.05)  # rate limiting

    logger.info(f"Broadcast done: {success} success, {fail} failed")
    b = InlineKeyboardBuilder()
    b.row(back_btn("admin_menu"))
    await progress_msg.edit_text(
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"✅ Успешно отправлено: {success}\n"
        f"❌ Не удалось: {fail}",
        reply_markup=b.as_markup()
    )

# ══════════════════════════════════════════════════════════════════════
#  SECTION 12b — ADMIN: BROADCAST TO USERS (личка)
# ══════════════════════════════════════════════════════════════════════
@router.callback_query(F.data == "admin_users_broadcast_start")
async def users_broadcast_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    users_count = len(db.data["users"])
    b = InlineKeyboardBuilder()
    b.row(back_btn("admin_menu"))
    await callback.message.edit_text(
        f"✉️ <b>Рассылка пользователям в личку</b>\n\n"
        f"Пользователей в базе: <b>{users_count}</b>\n\n"
        "⚠️ Получат только те, кто когда-либо писал боту в личку.\n"
        "Те кто заблокировал бота — будут пропущены.\n\n"
        "Введите текст сообщения:",
        reply_markup=b.as_markup()
    )
    await state.set_state(BotStates.waiting_for_users_broadcast)

@router.message(BotStates.waiting_for_users_broadcast, F.chat.type == "private")
async def users_broadcast_preview(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    await state.update_data(users_broadcast_text=message.text)
    users_count = len(db.data["users"])
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text=f"✅ Отправить {users_count} пользователям", callback_data="users_broadcast_confirm"),
        InlineKeyboardButton(text="❌ Отменить", callback_data="admin_menu")
    )
    await message.answer(
        f"📋 <b>Предпросмотр:</b>\n\n{message.text}\n\n"
        f"Подтвердите отправку <b>{users_count}</b> пользователям:",
        reply_markup=b.as_markup()
    )

@router.callback_query(F.data == "users_broadcast_confirm")
async def users_broadcast_confirm(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Доступ запрещён.", show_alert=True)
    data = await state.get_data()
    text = data.get("users_broadcast_text", "")
    await state.clear()

    users = list(db.data["users"].keys())
    total   = len(users)
    success = 0
    fail    = 0
    blocked = 0

    progress_msg = await callback.message.edit_text(
        f"✉️ Рассылка начата...\n\nОтправлено 0/{total}..."
    )

    for i, uid_str in enumerate(users, 1):
        try:
            await bot.send_message(int(uid_str), text)
            success += 1
        except TelegramForbiddenError:
            blocked += 1
        except Exception as e:
            logger.warning(f"Users broadcast failed for {uid_str}: {e}")
            fail += 1

        if i % 20 == 0:
            try:
                await progress_msg.edit_text(
                    f"✉️ Рассылка...\n\nОтправлено {i}/{total}..."
                )
            except Exception:
                pass
        await asyncio.sleep(0.05)

    logger.info(f"Users broadcast done: {success} ok, {blocked} blocked, {fail} failed")
    b = InlineKeyboardBuilder()
    b.row(back_btn("admin_menu"))
    await progress_msg.edit_text(
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"✅ Доставлено: {success}\n"
        f"🚫 Заблокировали бота: {blocked}\n"
        f"❌ Другие ошибки: {fail}",
        reply_markup=b.as_markup()
    )

# ══════════════════════════════════════════════════════════════════════
#  SECTION 13 — GROUP MESSAGE MODERATION (core logic)
# ══════════════════════════════════════════════════════════════════════
@router.message(F.chat.type.in_({"group", "supergroup"}))
async def handle_group_message(message: Message):
    # Skip bots and service messages without from_user
    if not message.from_user or message.from_user.is_bot:
        return

    uid = message.from_user.id
    chat_id_str = str(message.chat.id)

    # Skip if chat not registered
    cdata = db.data["chats"].get(chat_id_str)
    if not cdata:
        logger.info(f"[MOD] chat {chat_id_str} not in DB — skip")
        return
    if cdata.get("enabled", True) == False:
        logger.info(f"[MOD] chat {chat_id_str} disabled — skip")
        return

    # ── Collect active sponsors ────────────────────────────────────────
    db.data["stats"]["total_messages_checked"] += 1
    db.save()

    sponsors_to_check = []

    # Глобальный спонсор — только для FREE чатов
    if cdata.get("mode", "FREE") == "FREE":
        gs = db.data["global_sponsor"]
        if gs.get("active"):
            sponsors_to_check.append({
                "channel_id": int(gs["channel_id"]),
                "name":  gs.get("name", "Спонсор"),
                "link":  gs.get("link"),
                "title": gs.get("name", "Спонсор"),
            })
            logger.info(f"[MOD] FREE mode — added global sponsor {gs['channel_id']}")

    # Спонсоры чата — для ВСЕХ режимов (FREE и PRO)
    for s in cdata.get("sponsors", []):
        if s.get("active"):
            sponsors_to_check.append({
                "channel_id": int(s["channel_id"]),
                "name":       s.get("title", "Спонсор"),
                "link":       s.get("link"),
                "title":      s.get("title", "Спонсор"),
                "type":       s.get("type", "channel"),
                "bot_token":  s.get("bot_token"),
            })
            logger.info(f"[MOD] added chat sponsor {s['channel_id']} ({s.get('title')}) type={s.get('type','channel')}")

    logger.info(f"[MOD] ========================================")
    logger.info(f"[MOD] user={uid} chat={chat_id_str} mode={cdata['mode']}")
    logger.info(f"[MOD] all sponsors in DB: {cdata.get('sponsors', [])}")
    logger.info(f"[MOD] sponsors_to_check={[s['channel_id'] for s in sponsors_to_check]}")
    logger.info(f"[MOD] ========================================")

    if not sponsors_to_check:
        logger.info(f"[MOD] no active sponsors for chat {chat_id_str} — skip")
        return

    # ── Check subscriptions one by one ────────────────────────────────
    missing = []
    for s in sponsors_to_check:
        cid = s["channel_id"]
        sponsor_type = s.get("type", "channel")
        try:
            if sponsor_type == "bot" and s.get("bot_token"):
                is_sub = await check_bot_subscription(uid, s["bot_token"])
            else:
                is_sub = await check_subscription(uid, cid)
        except Exception as e:
            logger.error(f"[MOD] check exception user={uid} sponsor={cid} type={sponsor_type}: {e}")
            is_sub = False
        logger.info(f"[MOD] user={uid} sponsor={cid} ({s['name']}) type={sponsor_type} subscribed={is_sub}")
        if not is_sub:
            missing.append(s)

    if not missing:
        logger.info(f"[MOD] user={uid} subscribed to all — message kept")
        return

    logger.info(f"[MOD] user={uid} missing={[s['channel_id'] for s in missing]} — deleting message")

    # ── Delete message ─────────────────────────────────────────────────
    try:
        await message.delete()
        db.data["stats"]["total_messages_deleted"] += 1
        db.save()
        logger.info(f"[MOD] message deleted in chat {chat_id_str}")
    except TelegramBadRequest as e:
        logger.warning(f"[MOD] TelegramBadRequest deleting in {chat_id_str}: {e}")
    except TelegramForbiddenError as e:
        logger.warning(f"[MOD] TelegramForbiddenError deleting in {chat_id_str}: {e} — bot not admin?")
    except Exception as e:
        logger.error(f"[MOD] Unexpected error deleting in {chat_id_str}: {e}")

    # ── Send warning with subscribe buttons ─────────────────────────────
    mention = f"@{message.from_user.username}" if message.from_user.username \
              else f'<a href="tg://user?id={uid}">{message.from_user.first_name}</a>'

    welcome = cdata.get("welcome_text", "Подпишись на спонсоров, чтобы писать в этом чате!")
    text = f"{mention}, {welcome}"

    b = InlineKeyboardBuilder()
    has_buttons = False
    for s in missing:
        link = s.get("link")
        name = s.get("name") or s.get("title") or "Канал"
        logger.info(f"[MOD] sponsor in missing: name={name} link={link} channel_id={s.get('channel_id')}")
        if link:
            b.row(InlineKeyboardButton(text=f"📢 {name}", url=link))
            has_buttons = True
        else:
            text += f"\n📢 {name} (нет ссылки)"

    kb = b.as_markup() if has_buttons else None
    logger.info(f"[MOD] has_buttons={has_buttons} kb={'set' if kb else 'None'}")
    try:
        await message.answer(text, reply_markup=kb)
        logger.info(f"[MOD] warning sent in chat {chat_id_str} for user {uid}")
    except Exception as e:
        logger.error(f"Could not send welcome message in {chat_id_str}: {e}")

    db.save()

# ══════════════════════════════════════════════════════════════════════
#  SECTION 14 — AUTO REGISTER / AUTO SPONSOR when bot becomes admin
# ══════════════════════════════════════════════════════════════════════
@router.my_chat_member()
async def on_bot_chat_member_update(event: ChatMemberUpdated):
    """Срабатывает когда статус бота в чате/канале изменился."""
    new_status = event.new_chat_member.status
    chat       = event.chat
    added_by   = event.from_user
    chat_id_str = str(chat.id)

    if new_status in ("administrator", "creator"):

        # ── Проверяем: пользователь ждёт добавления спонсора? ──────────
        target_chat_id = pending_sponsor.get(added_by.id)

        if target_chat_id and target_chat_id in db.data["chats"]:
            # Это спонсор-канал или спонсор-чат
            pending_sponsor.pop(added_by.id, None)
            username = getattr(chat, "username", None)
            if username:
                auto_link = f"https://t.me/{username}"
            else:
                # Приватный — пробуем создать ссылку
                try:
                    invite = await bot.create_chat_invite_link(chat.id)
                    auto_link = invite.invite_link
                    logger.info(f"[AUTO-SPONSOR] created invite link for {chat.id}: {auto_link}")
                except Exception as e:
                    auto_link = None
                    logger.warning(f"[AUTO-SPONSOR] could not create invite link for {chat.id}: {e}")

            target_title = db.data["chats"][target_chat_id].get("title", target_chat_id)

            # Если ссылки нет — НЕ сохраняем спонсора, просим ввести ссылку
            if not auto_link:
                try:
                    b = InlineKeyboardBuilder()
                    b.row(InlineKeyboardButton(text="➕ Добавить вручную по ID", callback_data=f"add_sp_by_id_{target_chat_id}"))
                    await bot.send_message(
                        added_by.id,
                        f"⚠️ <b>{chat.title}</b> — приватный канал/чат.\n\n"
                        f"Не удалось создать ссылку автоматически (нет права invite_users).\n\n"
                        f"Добавьте спонсора вручную через кнопку ниже и укажите пригласительную ссылку.",
                        reply_markup=b.as_markup()
                    )
                except Exception as e:
                    logger.warning(f"[AUTO-SPONSOR] notify failed: {e}")
                return

            result = await _do_add_sponsor(
                target_chat_id, chat.id, chat.title, auto_link, added_by.id
            )
            logger.info(f"[AUTO-SPONSOR] {chat.id} ({chat.title}) added as sponsor to {target_chat_id}, result={result}")

            try:
                b = InlineKeyboardBuilder()
                b.row(InlineKeyboardButton(text="📋 Список спонсоров", callback_data=f"list_sponsors_{target_chat_id}"))
                b.row(InlineKeyboardButton(text="➕ Добавить ещё",      callback_data=f"add_sponsor_{target_chat_id}"))
                if result == "added":
                    text = (
                        f"✅ <b>{chat.title}</b> добавлен как спонсор в чат <b>{target_title}</b>!\n"
                        f"🔗 Ссылка: {auto_link}"
                    )
                elif result == "exists":
                    text = f"ℹ️ <b>{chat.title}</b> уже был спонсором чата <b>{target_title}</b>."
                else:
                    text = "❌ Не удалось добавить спонсора. Попробуйте вручную."
                await bot.send_message(added_by.id, text, reply_markup=b.as_markup())
            except Exception as e:
                logger.warning(f"[AUTO-SPONSOR] notify failed: {e}")
            return

        # ── Это не спонсор — регистрируем как основной чат ─────────────
        # Только группы и супергруппы (не каналы)
        if chat.type not in ("group", "supergroup"):
            return

        can_delete = getattr(event.new_chat_member, "can_delete_messages", False)
        if not can_delete:
            try:
                await bot.send_message(
                    added_by.id,
                    f"⚠️ Бот добавлен в <b>{chat.title}</b> как администратор, "
                    "но у него нет права <b>удаления сообщений</b>.\n\n"
                    "Выдайте это право — иначе бот не сможет удалять сообщения нарушителей."
                )
            except Exception:
                pass
            return

        if chat_id_str in db.data["chats"]:
            logger.info(f"[AUTO] chat {chat_id_str} already in DB — skip")
            return

        result = await _save_chat(chat.id, chat.title, added_by.id, added_by.username or "")
        if result == "added":
            logger.info(f"[AUTO] chat {chat.id} ({chat.title}) auto-registered, owner={added_by.id}")
            try:
                b = InlineKeyboardBuilder()
                b.row(InlineKeyboardButton(text="📋 Настроить спонсоров", callback_data=f"manage_chat_{chat_id_str}"))
                await bot.send_message(
                    added_by.id,
                    f"✅ <b>Чат «{chat.title}» автоматически зарегистрирован!</b>\n\n"
                    f"🆔 ID: <code>{chat.id}</code>\n\n"
                    "Теперь добавьте спонсоров — нажмите кнопку ниже или перейдите в 📋 Мои чаты.",
                    reply_markup=b.as_markup()
                )
            except Exception as e:
                logger.warning(f"[AUTO] could not notify owner {added_by.id}: {e}")

    # Бот удалён или покинул чат
    elif new_status in ("kicked", "left"):
        if chat_id_str in db.data["chats"]:
            db.data["chats"][chat_id_str]["enabled"] = False
            db.save()
            logger.info(f"[AUTO] bot removed from {chat_id_str} — chat disabled")
        # Убираем из pending если вдруг там висит
        pending_sponsor.pop(added_by.id, None)

# ══════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════
async def check_sponsor_timers():
    """Каждую минуту отключает спонсоров с истёкшим таймером."""
    while True:
        try:
            now = datetime.now()
            for chat_id, cdata in list(db.data["chats"].items()):
                for idx, s in enumerate(cdata.get("sponsors", [])):
                    expire_at = s.get("expire_at")
                    if not expire_at or not s.get("active"):
                        continue
                    try:
                        if datetime.fromisoformat(expire_at) <= now:
                            s["active"] = False
                            db.save()
                            logger.info(f"[TIMER] Sponsor {s['title']} in chat {chat_id} auto-disabled")
                            owner_id = cdata.get("owner_id")
                            if owner_id:
                                try:
                                    await bot.send_message(
                                        owner_id,
                                        f"⏱ <b>Таймер сработал</b>\n\n"
                                        f"Спонсор <b>{s['title']}</b> в чате <b>{cdata['title']}</b> автоматически отключён."
                                    )
                                except Exception:
                                    pass
                    except Exception as e:
                        logger.warning(f"[TIMER] error {chat_id}[{idx}]: {e}")
        except Exception as e:
            logger.error(f"[TIMER] task error: {e}")
        await asyncio.sleep(60)

async def main():
    dp.include_router(router)
    logger.info("Bot is starting...")
    asyncio.create_task(check_sponsor_timers())
    await dp.start_polling(bot, allowed_updates=["message", "callback_query", "my_chat_member"])

if __name__ == "__main__":
    import fcntl, sys, os

    lock_file_path = "/tmp/tgbot_8532241109.lock"
    lock_file = open(lock_file_path, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        logger.error("Бот уже запущен! Закройте другой экземпляр и попробуйте снова.")
        sys.exit(1)

    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")
    finally:
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()
        try:
            os.remove(lock_file_path)
        except Exception:
            pass
