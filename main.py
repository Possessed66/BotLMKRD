import os
import json
import pickle
import io 
import re
import gc
import asyncio
import inspect
import logging
import psutil
import traceback
import time
import threading
import sqlite3
import gspread.utils
import uuid
import tempfile
import httpx
import aiohttp
from aiohttp_socks import ProxyConnector
from aiogram.client.session.aiohttp import AiohttpSession
from contextlib import contextmanager, closing, suppress
from aiogram.exceptions import TelegramBadRequest
from aiogram.utils.markdown import markdown_decoration
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from aiogram.types import ReplyKeyboardRemove, File, BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.exceptions import TelegramForbiddenError
from aiogram.filters import Command
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import APIError, SpreadsheetNotFound
from cachetools import LRUCache
from rating_module import process_csv_and_update_ratings
from pathlib import Path
from import_holidays import import_holidays_from_csv



# ===================== ГЛОБАЛЬНАЯ ОБРАБОТКА ОШИБОК =====================
from aiogram.fsm.storage.base import StorageKey

@contextmanager
def get_db_connection():
    """Контекстный менеджер для безопасного подключения к SQLite."""
    conn = None
    try:
        # Проверка существования файла БД
        if not os.path.exists(DB_PATH):
             # Можно добавить логирование ошибки, если файл обязателен
             logging.critical(f"❌ Файл базы данных не найден: {DB_PATH}")
             raise FileNotFoundError(f"Файл базы данных не найден: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row  # Позволяет обращаться к колонкам по имени
        yield conn
    except sqlite3.Error as e:
        logging.error(f"Ошибка подключения к БД: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


async def global_error_handler(event: types.ErrorEvent, bot: Bot):
    """Централизованный обработчик всех необработанных исключений"""
    exception = event.exception
    update = event.update
    
    # Получаем идентификатор пользователя
    user_id = None
    if update.message:
        user_id = update.message.from_user.id
    elif update.callback_query:
        user_id = update.callback_query.from_user.id
    
    # Формируем сообщение об ошибке
    error_type = type(exception).__name__
    error_message = str(exception) or "Без описания"
    traceback_str = "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
    
    # Логируем в консоль
    logging.critical(
        f"ГЛОБАЛЬНАЯ ОШИБКА [user:{user_id}]\n"
        f"Type: {error_type}\n"
        f"Message: {error_message}\n"
        f"Traceback:\n{traceback_str}"
    )
    
    # Логируем в Google Sheets
    if user_id:
        try:
            logs_sheet.append_row([
                datetime.now().strftime("%d.%m.%Y %H:%M"),
                str(user_id),
                "CRITICAL_ERROR",
                f"{error_type}: {error_message[:200]}"
            ])
        except Exception as log_ex:
            logging.error(f"Ошибка при логировании: {str(log_ex)}")
    
    # Уведомляем администраторов
    for admin_id in ADMINS:
        try:
            await bot.send_message(
                admin_id,
                f"🚨 <b>Критическая ошибка</b>\n"
                f"• Пользователь: {user_id}\n"
                f"• Тип: {error_type}\n"
                f"• Сообщение: {error_message}\n\n"
                f"<code>{traceback_str[:3500]}</code>",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
    
    # Отправляем сообщение пользователю
    if user_id:
        try:
            await bot.send_message(
                user_id,
                "⚠️ Произошла непредвиденная ошибка. Администратор уведомлен.\n"
                "Попробуйте позже или начните заново с команды /start",
                reply_markup=ReplyKeyboardRemove()
            )
        except Exception:
            pass
    
    # Очищаем состояние пользователя
    if user_id:
        try:
            state = FSMContext(
                storage=dp.storage,
                key=StorageKey(
                    bot_id=bot.id,
                    chat_id=user_id,
                    user_id=user_id
                )
            )
            await state.clear()
        except Exception:
            pass
    
    return True

# ===================== ПРОФИЛИРОВАНИЕ ПАМЯТИ =====================

def init_tracemalloc():
    """Безопасная инициализация трассировки памяти"""
    global MEMORY_PROFILING_ENABLED
    if not tracemalloc.is_tracing():
        try:
            tracemalloc.start()
            MEMORY_PROFILING_ENABLED = True
            logging.info("Tracemalloc initialized")
        except Exception as e:
            logging.error(f"Failed to start tracemalloc: {e}")

def stop_tracemalloc():
    """Остановка трассировки памяти"""
    global MEMORY_PROFILING_ENABLED
    if tracemalloc.is_tracing():
        tracemalloc.stop()
        MEMORY_PROFILING_ENABLED = False
        logging.info("Tracemalloc stopped")

async def memory_monitor():
    """Мониторинг использования памяти (запускается только если включен флаг)"""
    while MEMORY_PROFILING_ENABLED:
        try:
            process = psutil.Process()
            mem_info = process.memory_info()
            
            logging.info(
                f"Memory: RSS={mem_info.rss / 1024 / 1024:.2f}MB, "
                f"VMS={mem_info.vms / 1024 / 1024:.2f}MB"
            )
            
            if tracemalloc.is_tracing():
                snapshot = tracemalloc.take_snapshot()
                top_stats = snapshot.statistics('lineno')[:5]
                for i, stat in enumerate(top_stats):
                    logging.info(
                        f"Alloc {i+1}: {stat.size / 1024:.2f}KB in {stat.count} blocks "
                        f"at {stat.traceback.format()[-1]}"
                    )
            
            await asyncio.sleep(1200)  # 20 минут
            
        except Exception as e:
            logging.error(f"Memory monitor error: {str(e)}")
            await asyncio.sleep(60)




# ===================== КОНФИГУРАЦИЯ =====================

DB_PATH = os.path.join(os.path.dirname(__file__), 'articles.db')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

# Сервисный режим
SERVICE_MODE = True
ADMINS = [122086799]
worker_running = True
reminder_running = True

# Кэширование
CACHE_TTL = 43200  # 12 часов
cache = LRUCache(maxsize=500)

MEMORY_PROFILING_ENABLED = False

# Загрузка переменных окружения
from dotenv import load_dotenv
load_dotenv('secret.env')

# Проверка обязательных переменных
session = None
if PROXY_URL:
    
    connector = ProxyConnector.from_url(PROXY_URL)
    
    aiohttp_session = aiohttp.ClientSession(connector=connector)
    
    session = AiohttpSession(aiohttp_session)
    print(f"✅ Бот запущен через прокси: {PROXY_URL}")
else:
    print("⚠️ Бот запущен без прокси (прямое соединение)")

# Создание бота с кастомной сессией
bot = Bot(
    token=BOT_TOKEN,
    session=session  # ← Передаём AiohttpSession
)

dp = Dispatcher() 




GOOGLE_CREDENTIALS_FILE = "/root/BotLMKRD/google-credentials.json"
with open(GOOGLE_CREDENTIALS_FILE, "r", encoding="utf-8") as f:
    GOOGLE_CREDS = json.load(f)


# Конфигурация Google Sheets

SPREADSHEET_NAME = "ShopBotDataKRD"
STATSS_SHEET_NAME = "Статистика_Пользователей"
ORDERS_SPREADSHEET_NAME = "Заказы МЗ 0.2KRD"
USERS_SHEET_NAME = "Пользователи"
GAMMA_CLUSTER_SHEET = "Гамма кластер"
TASKS_SHEET_NAME = "Задачи"
LOGS_SHEET = "Логи"
MANAGERS_SHEET_NAME = "МЗ"

# ===================== ИНИЦИАЛИЗАЦИЯ =====================
credentials = Credentials.from_service_account_info(
    GOOGLE_CREDS,
    scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
)
client = gspread.authorize(credentials)

bot = Bot(
    token=BOT_TOKEN,
    session=http_client,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()
dp.errors.register(global_error_handler)


# Инициализация таблиц
try:
    main_spreadsheet = client.open(SPREADSHEET_NAME)
    users_sheet = main_spreadsheet.worksheet(USERS_SHEET_NAME)
    logs_sheet = main_spreadsheet.worksheet(LOGS_SHEET)
    orders_spreadsheet = client.open(ORDERS_SPREADSHEET_NAME)
    gamma_cluster_sheet = orders_spreadsheet.worksheet(GAMMA_CLUSTER_SHEET)
    print("✅ Google Sheets успешно инициализированы")
except Exception as e:
    print(f"❌ Ошибка инициализации Google Sheets: {str(e)}")
    exit()


# ===================== СОСТОЯНИЯ FSM =====================
class Registration(StatesGroup):
    name = State()
    surname = State()
    position = State()
    shop = State()


class OrderStates(StatesGroup):
    article_input = State()
    shop_selection = State()
    shop_input = State()
    quantity_input = State()
    order_reason_input = State()
    confirmation = State()
    quantity_input_for_top0 = State()
    reason_input_for_top0 = State()
    waiting_for_quantities_list = State()
    batch_order_reason_input = State()
    batch_confirmation = State()
    batch_shop_selection = State()

class InfoRequest(StatesGroup):
    article_input = State()
    waiting_for_action = State()

class AdminBroadcast(StatesGroup):
    message_input = State()
    target_selection = State()
    manual_ids = State()
    confirmation = State()


class TaskStates(StatesGroup):
    # Состояния для добавления задач
    add_text = State()
    add_link = State()
    add_deadline = State()
    # Состояния для удаления задач
    delete_task = State()
    confirm_delete = State()
    # Состояния для отправки задач
    select_action = State()  # Выбор действия (отправить/статистика)
    select_tasks = State()   # Выбор задач для отправки
    input_task_ids = State() # Ввод ID задач вручную
    select_audience = State() # Выбор аудитории
    input_position = State()
    input_manual_ids = State() # Ввод ID пользователей
    confirmation = State()
    
    # Состояния для статистики
    view_stats = State()     # Просмотр статистики
    input_task_id_for_details = State() # Ввод ID для детализации
    review_selection = State()


class ManagerApprovalStates(StatesGroup):
    awaiting_reject_comment = State()


class FeedbackStates(StatesGroup):
    waiting_for_feedback = State()


# ===================== КЛАВИАТУРЫ =====================
def create_keyboard(buttons: List[str], sizes: tuple, resize=True, one_time=False) -> types.ReplyKeyboardMarkup:
    """Универсальный конструктор клавиатур"""
    builder = ReplyKeyboardBuilder()
    for button in buttons:
        builder.button(text=button)
    builder.adjust(*sizes)
    return builder.as_markup(
        resize_keyboard=resize,
        one_time_keyboard=one_time
    )

def main_menu_keyboard(user_id: int = None) -> types.ReplyKeyboardMarkup:
    """Главное меню с учетом прав"""
    buttons = ["📋 Запрос информации", "🛒 Заказ под клиента", "📞 Обратная связь",]
    if user_id and user_id in ADMINS:
        buttons.append("🛠 Админ-панель")
    return create_keyboard(buttons, (2, 1, 1))

def article_input_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(
        [ "❌ Отмена"],
        (1)
    )

def shop_selection_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(
        ["Использовать мой магазин", "Выбрать другой", "❌ Отмена"],
        (2, 1)
    )

def confirm_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(
        ["✅ Подтвердить", "✏️ Исправить количество", "❌ Отмена"],
        (2, 1)
    )

def admin_panel_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(
        ["📊 Статистика", "📢 Рассылка", "🔄 Обновить кэш", "🔧 Сервисный режим", "📊 Дамп памяти", "📝 Управление задачами", "🔙 Главное меню"],
        (3, 2, 2)
    )

def service_mode_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(
        ["🟢 Включить сервисный режим", "🔴 Выключить сервисный режим", "🔙 Назад"],
        (2, 1)
    )

def cancel_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(["❌ Отмена"], (1,))


def broadcast_target_keyboard():
    return create_keyboard(
        ["Всем пользователям", "По магазинам", "По отделам", "Вручную", "❌ Отмена"],
        (2, 2, 1)
    )

def broadcast_confirmation_keyboard():
    return create_keyboard(
        ["✅ Подтвердить рассылку", "❌ Отмена"],
        (2,)
    )

def tasks_admin_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(
        ["➕ Добавить задачу", "🗑️ Удалить задачу", "📤 Отправить список", "📊 Статистика выполнения", "🔙 Назад"],
        (2, 2, 1)
    )

def get_task_keyboard(task_id: str) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(
        text="✅ Выполнено", 
        callback_data=f"task_done:{task_id}"
    )
    return builder.as_markup()


def quick_shop_selection_keyboard() -> types.ReplyKeyboardMarkup:
    """Клавиатура для быстрого выбора из 3 магазинов."""
    builder = ReplyKeyboardBuilder()
    builder.button(text="🏪 Магазин 8")
    builder.button(text="🏪 Магазин 92")
    builder.button(text="🏪 Магазин 147")
    builder.button(text="🏪 Магазин 150")
    builder.button(text="🏪 Магазин 165")
    builder.button(text="🏪 Магазин 255")
    builder.button(text="❌ Отмена") # Добавим кнопку отмены
    builder.adjust(3,3,1) # 3 кнопки в первом ряду, 1 во втором
    return builder.as_markup(resize_keyboard=True)


# ===================== СЕРВИСНЫЕ ФУНКЦИИ =====================
async def notify_admins(message: str) -> None:
    """Уведомление администраторов"""
    for admin_id in ADMINS:
        with suppress(TelegramForbiddenError):
            await bot.send_message(admin_id, message)

async def toggle_service_mode(enable: bool) -> None:
    """Включение/выключение сервисного режима"""
    global SERVICE_MODE
    SERVICE_MODE = enable
    status = "ВКЛЮЧЕН" if enable else "ВЫКЛЮЧЕН"
    await notify_admins(f"🛠 Сервисный режим {status}")

async def get_user_data(user_id: str) -> Optional[Dict[str, Any]]:
    """Получение данных пользователя (ОПТИМИЗИРОВАНО: без pickle)"""
    try:
        cache_key = f"user_{user_id}"
        # Сначала проверяем кэш конкретного пользователя
        if cache_key in cache:
            user_data = cache[cache_key]
            if all(key in user_data for key in ['shop', 'name', 'position']):
                return user_data
            else:
                cache.pop(cache_key, None)
        
        # Загрузка данных из Google Sheets
        # --- ИСПРАВЛЕНО: Берем объект напрямую из кэша ---
        users_records = cache.get("users_data")
        
        # Если кэш пуст, загружаем из сети
        if not users_records:
            users_records = users_sheet.get_all_records()
            # Кэшируем сам объект (список словарей), а не байты
            cache["users_data"] = users_records
            logging.info("Кэш пользователей загружен из сети.")
        
        for user in users_records:
            if str(user.get("ID пользователя", "")).strip() == str(user_id).strip():
                user_data = {
                    'shop': user.get("Номер магазина", "") or "Не указан",
                    'name': user.get("Имя", "") or "Не указано",
                    'surname': user.get("Фамилия", "") or "Не указано",
                    'position': user.get("Должность", "") or "Не указана"
                }
                cache[cache_key] = user_data
                return user_data
        
        return None
    except Exception as e:
        logging.error(f"Ошибка получения данных пользователя: {str(e)}")
        return None


async def log_error(user_id: str, error: str) -> None:
    """Логирование ошибок"""
    try:
        logs_sheet.append_row([
            datetime.now().strftime("%d.%m.%Y %H:%M"),
            user_id,
            "ERROR",
            error
        ])
    except Exception as e:
        logging.error(f"Ошибка логирования: {str(e)}")

async def log_user_activity(user_id: str, command: str, event_type: str = "command") -> None:
    """Логирование действий пользователя"""
    try:
        user_data = await get_user_data(str(user_id))
        if not user_data:
            return
            
        record = [
            datetime.now().strftime("%d.%m.%Y"),
            datetime.now().strftime("%H:%M:%S"),
            str(user_id),
            user_data.get('name', ''),
            user_data.get('surname', ''),
            user_data.get('position', ''),
            user_data.get('shop', ''),
            command,
            event_type
        ]
        
        stats_sheet = main_spreadsheet.worksheet(STATSS_SHEET_NAME)
        stats_sheet.append_row(record)
    except Exception as e:
        logging.error(f"Ошибка логирования активности: {str(e)}")


@dp.message(F.from_user.id.in_(ADMINS), F.text == "/queue_stats")
async def handle_queue_stats(message: types.Message):
    """Показывает статистику очереди заказов админу."""
    try:
        stats = get_order_queue_stats()
        response = (
            f"📊 *Статистика очереди заказов:*\n"
            f"• В ожидании: `{stats['pending']}`\n"
            f"• В обработке: `{stats['processing']}`\n"
            f"• Успешно обработано: `{stats['completed']}`\n"
            f"• С ошибками: `{stats['failed']}`\n"
        )
        await message.answer(response, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"Ошибка при получении статистики очереди: {e}")
        await message.answer("❌ Ошибка получения статистики.")
        




@dp.message(Command("upload_holidays"))
async def handle_upload_holidays_command(message: types.Message):
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав для выполнения этой команды.")
        return

    await message.answer("📁 Отправьте CSV-файл с данными о каникулах поставщиков (табуляция).")

# --- Обработчик получения файла ---
@dp.message(lambda m: m.document and m.document.mime_type == 'text/csv')
async def handle_holidays_file(message: types.Message):
    
    if message.from_user.id not in ADMINS:
         await message.answer("❌ У вас нет прав для выполнения этой команды.")
         return

    document = message.document
    file_id = document.file_id
    file_name = document.file_name

    # Скачиваем файл
    file = await bot.get_file(file_id)
    file_path = file.file_path

    # Создаём временный файл
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
        await bot.download_file(file_path, temp_file.name)
        temp_csv_path = temp_file.name

    try:
        # Импортируем данные
        updated_count = import_holidays_from_csv(temp_csv_path)
        await message.answer(f"✅ Файл '{file_name}' успешно загружен и обработан. Обновлено {updated_count} записей в базе данных.")
    except Exception as e:
        logging.error(f"❌ Ошибка при обработке файла: {e}")
        await message.answer(f"❌ Ошибка при обработке файла: {str(e)}")
    finally:
        # Удаляем временный файл
        os.unlink(temp_csv_path)


def format_holidays_ranges(holidays):
    """
    Принимает список дат (datetime.date), возвращает строку вида:
    "19.12.2025 - 25.12.2025, 27.12.2025, 30.12.2025 - 07.01.2026"
    """
    if not holidays:
        return "нет"

    sorted_dates = sorted(holidays)
    ranges = []
    start = sorted_dates[0]
    end = sorted_dates[0]

    for d in sorted_dates[1:]:
        if d == end + timedelta(days=1):
            # Продолжаем диапазон
            end = d
        else:
            # Заканчиваем предыдущий диапазон
            if start == end:
                ranges.append(start.strftime("%d.%m.%Y"))
            else:
                ranges.append(f"{start.strftime('%d.%m.%Y')} - {end.strftime('%d.%m.%Y')}")
            # Начинаем новый
            start = d
            end = d

    # Не забываем последний
    if start == end:
        ranges.append(start.strftime("%d.%m.%Y"))
    else:
        ranges.append(f"{start.strftime('%d.%m.%Y')} - {end.strftime('%d.%m.%Y')}")

    return ", ".join(ranges)



# ===================== ЗАДАЧИ =====================
def normalize_task_row(task_id: str, row: dict) -> dict:
    return {
        "text": row.get("Текст", ""),
        "creator_initials": row.get("Инициалы", ""),
        "deadline": row.get("Дедлайн", ""),
        "link": str(row.get("Ссылка", "")).strip(),
        "statuses": row.get("Статусы", ""),
    }


def get_tasks_sheet():
    """Возвращает лист с задачами"""
    return main_spreadsheet.worksheet(TASKS_SHEET_NAME)


def format_task_message(task_id: str, task: dict) -> str:
    """
    Форматирует сообщение задачи для отправки пользователю.
    - Текст задачи вставляется как есть (пользователь отвечает за Markdown).
    - Поддерживает несколько ссылок, разделённых запятыми.
    - Ссылки отображаются как "Ссылка на документ" с нумерацией при необходимости.
    - Не использует markdown_decoration.quote для основного текста.
    """
    lines = []

    # 1. Заголовок с ID задачи
    lines.append(f"📌 *Задача #{task_id}*")

    # 2. Основной текст задачи (без экранирования)
    task_text = task.get('text', '')
    lines.append(f"▫️ {task_text}")

    # 3. Создатель
    creator = task.get('creator_initials', '')
    # Минимальное экранирование для полей, которые могут содержать базовые символы Markdown
    if creator:
        escaped_creator = creator.replace('*', '\\*').replace('_', '\\_').replace('`', '\\`').replace('[', '\\[')
        lines.append(f"👤 Создал: {escaped_creator}")

    # 4. Дедлайн
    deadline = task.get('deadline', '')
    if deadline:
        escaped_deadline = deadline.replace('*', '\\*').replace('_', '\\_').replace('`', '\\`').replace('[', '\\[')
        lines.append(f"⏰ *Дедлайн:* {escaped_deadline}")
    else:
        lines.append("⏳ *Дедлайн:* Не установлен")

    # 5. Ссылки (обработка нескольких)
    links_raw = task.get('link', '').strip()
    if links_raw:
        # Разбиваем по запятым, очищаем и фильтруем пустые
        link_list = [link_part.strip() for link_part in links_raw.split(',') if link_part.strip()]
        
        if link_list:
            if len(link_list) == 1:
                # Одна ссылка - без нумерации в тексте
                link = link_list[0]
                # Экранируем скобки в URL для корректной работы Markdown-ссылки
                escaped_url = link.replace('(', '\\(').replace(')', '\\)')
                lines.append(f"🔗 [Ссылка на задачу]({escaped_url})")
            else:
                # Несколько ссылок - с нумерацией
                lines.append("🔗 *Ссылки на документы:*")
                for i, link in enumerate(link_list, 1):
                    # Экранируем скобки в URL
                    escaped_url = link.replace('(', '\\(').replace(')', '\\)')
                    lines.append(f"  [{i}. Ссылка на документ]({escaped_url})")
        # Если после разбора список пуст, можно ничего не добавлять или добавить сообщение об ошибке
        # else:
        #     lines.append("🔗 Ссылки: Нет (ошибка формата)")
    # Если поле 'link' пустое, ничего не добавляем

    return "\n".join(lines)
    

async def get_user_initials(user_id: int) -> str:
    """Возвращает инициалы пользователя (например, 'И.Иванов')"""
    user_data = await get_user_data(str(user_id))
    if not user_data:
        return "Аноним"
    name = user_data.get("name", "")
    surname = user_data.get("surname", "")
    return f"{name}.{surname}" if name else surname


async def save_task(
    task_id: str,
    text: str,
    creator_id: int,
    creator_initials: str,
    link: str = None,
    deadline: str = None
):
    """Сохранение задачи в Google Sheets"""
    sheet = get_tasks_sheet()
    sheet.append_row([
        task_id,
        text,
        link,
        deadline,
        creator_id,
        creator_initials,
        datetime.now().strftime("%d.%m.%Y %H:%M"),
        "",
        json.dumps({"user_ids": []})  # Пустой список для статусов
    ])


# --- Исправленный фрагмент load_tasks ---
async def load_tasks() -> Dict[str, Dict[str, Any]]:
    """
    Загрузка задач из Google Sheets.
    Возвращает словарь: {task_id: {task_data}}
    task_data включает: text, link, deadline, creator_initials, creator_id, assigned_to, completed_by
    """
    sheet = get_tasks_sheet()
    tasks = {}
    try:
        expected_headers = [
        "ID задачи", "Текст", "Ссылка", "Дедлайн",
        "ID создателя", "Инициалы", "Создано",
        "Назначена", "Статусы"]
        records = sheet.get_all_records(expected_headers=expected_headers)
        logging.info(f"Загружено {len(records)} строк из Google Sheets для задач.")
        for row in records:
            task_id = str(row.get("ID задачи", "")).strip()
            if not task_id:
                continue # Пропускаем строки без ID
            
            # Обработка назначенных пользователей
            assigned_raw = str(row.get("Назначена", "")).strip()
            logging.debug(f"Задача {task_id}: Сырое значение 'Назначена' = '{assigned_raw}' (тип: {type(assigned_raw)})")
            if assigned_raw:
                # Разбиваем строку, очищаем и фильтруем ID
                assigned_user_ids = [
                    uid_str for uid_str in
                    (uid.strip() for uid in assigned_raw.split(","))
                    if uid_str.isdigit()
                ]
                logging.debug(f"Задача {task_id}: Обработанные ID 'Назначена' = {assigned_user_ids} (типы: {[type(uid) for uid in assigned_user_ids]})")
                
            else:
                assigned_user_ids = []

            # --- Исправлено и Улучшено: Обработка выполненных пользователей с поддержкой старого формата ---
            completed_user_ids = []
            statuses_raw = str(row.get("Статусы", "{}")).strip()
            logging.debug(f"Задача {task_id}: Сырой статус = '{statuses_raw}'")
            if statuses_raw:
                try:
                    statuses_data = json.loads(statuses_raw)
                    # logging.debug(f"Задача {task_id}: Распарсенный статус = {statuses_data} (тип: {type(statuses_data)})")
                    if isinstance(statuses_data, dict):
                        # Новый формат: {"completed_by": [...]}
                        if "completed_by" in statuses_data:
                            completed_user_ids = statuses_data.get("completed_by", [])
                            # logging.debug(f"Задача {task_id}: Найден ключ 'completed_by': {completed_user_ids}")
                        # Старый формат: {"user_ids": [...]}
                        elif "user_ids" in statuses_data:
                            logging.info(f"Задача {task_id} использует устаревший формат 'user_ids'.")
                            completed_user_ids = statuses_data.get("user_ids", [])
                            # logging.debug(f"Задача {task_id}: Найден ключ 'user_ids': {completed_user_ids}")
                        # Если ни один ключ не найден, оставляем пустой список
                        else:
                             completed_user_ids = []
                             # logging.debug(f"Задача {task_id}: Ключи в статусе не найдены.")
                        # Убедимся, что это список строк ID
                        completed_user_ids = [str(uid).strip() for uid in completed_user_ids if str(uid).strip()]
                        # logging.debug(f"Задача {task_id}: Финальный список completed_by = {completed_user_ids}")
                    else:
                        # Если statuses_data не словарь (например, пустой список или что-то еще)
                        logging.warning(f"Неверная структура 'Статусы' для задачи {task_id} (не словарь): {statuses_data}. Считается пустым.")
                        completed_user_ids = []
                except (json.JSONDecodeError, TypeError, ValueError) as e:
                    logging.warning(f"Ошибка парсинга 'Статусы' для задачи {task_id}: {e}. Считается пустым.")
                    completed_user_ids = []
       
            
            tasks[task_id] = {
                "text": str(row.get("Текст", "")).strip(),
                "link": str(row.get("Ссылка", "")).strip(),
                "deadline": str(row.get("Дедлайн", "")).strip(),
                "creator_initials": str(row.get("Инициалы", "")).strip(),
                "creator_id": str(row.get("ID создателя", "")).strip(),
                "assigned_to": assigned_user_ids,
                # --- Исправлено: Теперь всегда используем ключ "completed_by" в памяти ---
                "completed_by": completed_user_ids, 
            }
            
            logging.debug(f"Задача {task_id}: Финальное значение 'assigned_to' = {tasks[task_id]['assigned_to']} (типы: {[type(uid) for uid in tasks[task_id]['assigned_to']]})")
        logging.info(f"✅ Загружено {len(tasks)} задач из Google Sheets")
        
    except Exception as e: # <-- Этот except корректно завершает блок try
        logging.error(f"Ошибка загрузки задач из Google Sheets: {e}", exc_info=True)
        
        
        
    
    return tasks # <-- Эта строка должна быть на уровне функции, вне блока try...except


# --- Исправленный фрагмент assign_tasks_to_users ---
async def assign_tasks_to_users(task_ids: list[str], user_ids: list[int], sheet=None):
    """
    Назначает задачи пользователям, обновляя столбец "Назначена" (H) в Google Sheets.
    Args:
        task_ids (list[str]): Список ID задач для обновления.
        user_ids (list[int]): Список ID пользователей, которым назначаются задачи.
        sheet (gspread.Worksheet, optional): Лист задач. Если None, будет получен заново.
    """
    if not task_ids or not user_ids:
        logging.info("Нет задач или пользователей для назначения.")
        return
    try:
        if sheet is None:
            sheet = get_tasks_sheet()
        # Преобразуем user_ids в строку, разделенную запятыми
        assigned_users_str = ", ".join(map(str, user_ids))
        # Получаем все значения столбца ID задачи (A)
        task_id_col_values = sheet.col_values(1) # 1 = столбец A
        # Создаем словарь {task_id: row_number}
        task_id_to_row = {str(task_id_col_values[i]).strip(): i + 1 for i in range(len(task_id_col_values))}
        batch_updates = []
        updated_count = 0
        for task_id in task_ids:
            row_number = task_id_to_row.get(str(task_id))
            if row_number:
                # --- Исправлено: Столбец "Назначена" это H (8) ---
                assigned_column_index = 8 # H = 8
                range_label = gspread.utils.rowcol_to_a1(row_number, assigned_column_index)
                batch_updates.append({
                    'range': range_label,
                    'values': [[assigned_users_str]]
                })
                updated_count += 1
            else:
                logging.warning(f"Строка для задачи {task_id} не найдена при назначении.")
        if batch_updates:
            # Выполняем пакетное обновление
            sheet.batch_update(batch_updates)
            logging.info(f"✅ Назначено {updated_count} задач {len(user_ids)} пользователям.")
        else:
            logging.warning("Не найдено строк для обновления при назначении задач.")
    except gspread.exceptions.APIError as e:
        logging.error(f"API ошибка Google Sheets при назначении задач: {e}")
    except Exception as e:
        logging.error(f"Ошибка при назначении задач пользователям: {e}", exc_info=True)


@dp.message(F.text == "📝 Управление задачами")
async def handle_task_menu(message: types.Message):
    if message.from_user.id not in ADMINS:
        return
    await message.answer("📝 Управление задачами:", reply_markup=tasks_admin_keyboard())

@dp.message(F.text == "➕ Добавить задачу")
async def add_task_text(message: types.Message, state: FSMContext):
    await message.answer("📝 Введите текст задачи:", reply_markup=cancel_keyboard())
    await state.set_state(TaskStates.add_text)

@dp.message(TaskStates.add_text, F.text == "❌ Отмена")
@dp.message(TaskStates.add_text, F.text == "/start")
async def cancel_add_task_text(message: types.Message, state: FSMContext):
    await state.clear()
    # Определяем, какую клавиатуру отправить в зависимости от команды или кнопки
    if message.text == "/start":
        # Возврат в главное меню пользователя
        await message.answer("Действие отменено.", reply_markup=main_menu_keyboard(message.from_user.id))
    else:
        # Возврат в меню управления задачами
        await message.answer("Добавление задачи отменено.", reply_markup=tasks_admin_keyboard())

@dp.message(TaskStates.add_text)
async def add_task_link(message: types.Message, state: FSMContext):
    await state.update_data(text=message.text)
    await message.answer("🔗 Пришлите ссылку на Google Sheets (или /skip):", reply_markup=cancel_keyboard())
    await state.set_state(TaskStates.add_link)

# --- Добавлены хендлеры отмены для TaskStates.add_link ---
@dp.message(TaskStates.add_link, F.text == "❌ Отмена")
@dp.message(TaskStates.add_link, F.text == "/start")
async def cancel_add_task_link(message: types.Message, state: FSMContext):
    await state.clear()
    if message.text == "/start":
        await message.answer("Действие отменено.", reply_markup=main_menu_keyboard(message.from_user.id))
    else:
        await message.answer("Добавление задачи отменено.", reply_markup=tasks_admin_keyboard())

@dp.message(TaskStates.add_link)
async def add_task_deadline(message: types.Message, state: FSMContext):
    link = message.text if message.text != "/skip" else None
    await state.update_data(link=link)
    await message.answer("📅 Укажите дедлайн (ДД.ММ.ГГГГ или /skip):", reply_markup=cancel_keyboard())
    await state.set_state(TaskStates.add_deadline)

# --- Добавлены хендлеры отмены для TaskStates.add_deadline ---
@dp.message(TaskStates.add_deadline, F.text == "❌ Отмена")
@dp.message(TaskStates.add_deadline, F.text == "/start")
async def cancel_add_task_deadline(message: types.Message, state: FSMContext):
    await state.clear()
    if message.text == "/start":
        await message.answer("Действие отменено.", reply_markup=main_menu_keyboard(message.from_user.id))
    else:
        await message.answer("Добавление задачи отменено.", reply_markup=tasks_admin_keyboard())

@dp.message(TaskStates.add_deadline)
async def save_task_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    deadline = message.text if message.text != "/skip" else None
    
    # Проверка формата даты
    if deadline and not re.match(r"^\d{2}\.\d{2}\.\d{4}$", deadline):
        await message.answer("❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ или /skip для пропуска:", reply_markup=cancel_keyboard())
        # Не меняем состояние, позволяем повторный ввод
        return
    
    task_id = str(int(time.time()))
    creator_initials = await get_user_initials(message.from_user.id)
    
    await save_task(
        task_id=task_id,
        text=data["text"],
        creator_id=message.from_user.id,
        creator_initials=creator_initials,
        link=data.get("link"),
        deadline=deadline
    )
    
    await message.answer(
        f"✅ Задача добавлена!\n"
        f"ID: `{task_id}`\n"
        f"Дедлайн: {deadline if deadline else 'не установлен'}",
        parse_mode='Markdown', # Указываем parse_mode
        reply_markup=tasks_admin_keyboard()
    )
    await state.clear()


async def delete_task(task_id: str, admin_user_id: int) -> bool:
    """
    Удаляет задачу из Google Sheets по её ID.
    
    Args:
        task_id (str): ID задачи для удаления.
        admin_user_id (int): ID администратора, инициировавшего удаление (для логирования).

    Returns:
        bool: True, если задача успешно удалена, False в противном случае.
    """
    try:
        sheet = get_tasks_sheet()
        # Находим ячейку с task_id в первом столбце (ID задачи)
        cell = sheet.find(task_id, in_column=1) # Ищем в столбце A (индекс 1)
        
        if not cell:
            logging.warning(f"Попытка удаления несуществующей задачи {task_id} админом {admin_user_id}")
            return False

        # Удаляем всю строку
        sheet.delete_rows(cell.row)
        logging.info(f"Задача {task_id} успешно удалена админом {admin_user_id}")
        return True

    except gspread.exceptions.APIError as e:
        # Ошибки Google Sheets API
        logging.error(f"Ошибка Google Sheets API при удалении задачи {task_id} админом {admin_user_id}: {e}")
        return False
    except Exception as e:
        # Другие ошибки
        logging.error(f"Неожиданная ошибка при удалении задачи {task_id} админом {admin_user_id}: {e}", exc_info=True)
        return False


@dp.message(F.text == "🗑️ Удалить задачу")
async def delete_task_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMINS:
        return
    tasks = await load_tasks()
    if not tasks:
        await message.answer("❌ Нет задач для удаления.", reply_markup=tasks_admin_keyboard())
        return
    # Формируем список задач
    tasks_list = "\n".join([f"ID: `{tid}` — {task['text'][:50]}{'...' if len(task['text']) > 50 else ''}" for tid, task in tasks.items()])
    await message.answer(
        f"📝 *Список задач:*\n"
        f"{tasks_list}\n\n"
        f"✏️ *Введите ID задачи для удаления:*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=cancel_keyboard()
    )
    await state.set_state(TaskStates.delete_task)


@dp.message(TaskStates.delete_task)
async def delete_task_handler(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMINS:
        return
    task_id = message.text.strip()
    tasks = await load_tasks() # Перезагружаем, чтобы убедиться в актуальности
    task = tasks.get(task_id)
    if not task:
        await message.answer("❌ Задача с таким ID не найдена.", reply_markup=tasks_admin_keyboard())
        await state.clear()
        return

    # Сохраняем ID задачи для подтверждения
    await state.update_data(task_id_to_delete=task_id, task_text_to_delete=task['text'])
    
    # Создаем inline-клавиатуру для подтверждения
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, удалить", callback_data=f"confirm_delete:{task_id}")
    builder.button(text="❌ Нет, отмена", callback_data="cancel_delete")
    builder.adjust(2)
    
    await message.answer(
        f"❓ *Вы уверены, что хотите удалить задачу?*\n"
        f"ID: `{task_id}`\n"
        f"Текст: {task['text'][:100]}{'...' if len(task['text']) > 100 else ''}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=builder.as_markup()
    )
    await state.set_state(TaskStates.confirm_delete)


@dp.callback_query(TaskStates.confirm_delete, F.data.startswith("confirm_delete:"))
async def confirm_delete_task(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMINS:
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
        
    data = await state.get_data()
    task_id_to_delete = data.get('task_id_to_delete')
    task_id_from_callback = callback.data.split(":")[1]

    if task_id_to_delete != task_id_from_callback:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        await state.clear()
        return

    success = await delete_task(task_id_to_delete, callback.from_user.id)
    if success:
        await callback.message.edit_text(f"✅ Задача `{task_id_to_delete}` успешно удалена.", parse_mode=ParseMode.MARKDOWN)
        # Или отправить новое сообщение и удалить старое, если edit_text не подходит
        # await callback.message.delete()
        # await callback.message.answer(f"✅ Задача `{task_id_to_delete}` успешно удалена.", reply_markup=tasks_admin_keyboard())
    else:
        await callback.message.edit_text("❌ Не удалось удалить задачу. Возможно, она не существует или у вас нет прав.", parse_mode=ParseMode.MARKDOWN)
        # await callback.message.answer("❌ Не удалось удалить задачу...", reply_markup=tasks_admin_keyboard())
    
    await state.clear()
    # Отправляем обновленное меню задач (если не редактировали сообщение выше)
    # await callback.message.answer("📝 Управление задачами:", reply_markup=tasks_admin_keyboard())
    await callback.answer() # Закрываем уведомление о нажатии


@dp.callback_query(TaskStates.confirm_delete, F.data == "cancel_delete")
async def cancel_delete_task(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMINS:
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text("❌ Удаление задачи отменено.", parse_mode=ParseMode.MARKDOWN)
    # await callback.message.answer("❌ Удаление задачи отменено.", reply_markup=tasks_admin_keyboard())
    await callback.answer()


@dp.message(F.text == "📤 Отправить список")
async def send_tasks_menu(message: types.Message, state: FSMContext):
    tasks = await load_tasks()
    if not tasks:
        await message.answer("❌ Нет задач для отправки.", reply_markup=tasks_admin_keyboard())
        return
    
    await state.update_data(tasks=tasks)
    keyboard = create_keyboard(
        ["Отправить все", "Выбрать задачи", "🔙 Назад"],
        (2, 1)
    )
    await message.answer("Выберите действие:", reply_markup=keyboard)
    await state.set_state(TaskStates.select_action)

@dp.message(TaskStates.select_action, F.text == "Отправить все")
async def send_all_tasks(message: types.Message, state: FSMContext):
    data = await state.get_data()
    tasks = data.get("tasks")

    if not tasks:
        await message.answer("❌ Нет задач для отправки.")
        await state.clear()
        return

    await state.update_data(selected_tasks=tasks)

    await message.answer(
        f"✅ Выбраны все задачи: {len(tasks)} шт.\nВыберите аудиторию:",
        reply_markup=create_keyboard(
            ["Всем пользователям", "По должности", "Вручную", "🔙 Назад"],
            (2, 2)
        )
    )
    await state.set_state(TaskStates.select_audience)


@dp.message(TaskStates.select_action, F.text == "Выбрать задачи")
async def select_action_to_send(message: types.Message, state: FSMContext):
    data = await state.get_data()
    tasks = data['tasks']
    
    tasks_list = "\n".join([f"{task_id}: {task['text']}" for task_id, task in tasks.items()])
    await message.answer(
        f"Введите ID задач через запятую:\n{tasks_list}",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(TaskStates.input_task_ids)


@dp.message(TaskStates.input_task_ids)
async def process_task_ids(message: types.Message, state: FSMContext):
    data = await state.get_data()
    all_tasks = data['tasks']
    
    # Нормализуем ввод: удаляем пробелы и пустые значения
    input_ids = [tid.strip() for tid in message.text.split(",") if tid.strip()]
    
    if not input_ids:
        await state.clear()
        await message.answer("❌ Не указано ни одного ID задачи.")
        await message.answer("↩️ Возврат в меню задач.", reply_markup=tasks_admin_keyboard())
        return
    
    # Преобразуем все ID к строковому типу для сравнения
    all_task_ids = {str(k): v for k, v in all_tasks.items()}
    
    # Фильтруем задачи
    valid_tasks = {}
    invalid_ids = []
    
    for input_id in input_ids:
        if input_id in all_task_ids:
            valid_tasks[input_id] = all_task_ids[input_id]
        else:
            invalid_ids.append(input_id)
    
    if not valid_tasks:
        await state.clear() 
        await message.answer("❌ Не найдено ни одной действительной задачи.")
        await message.answer("↩️ Возврат в меню задач.", reply_markup=tasks_admin_keyboard())
        return
    
    # Сообщаем о невалидных ID (если есть)
    if invalid_ids:
        await message.answer(
            f"⚠️ Не найдены задачи с ID: {', '.join(invalid_ids)}\n"
            f"Будут отправлены только действительные задачи.",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await asyncio.sleep(2)  # Даем время прочитать сообщение
    
    await state.update_data(selected_tasks=valid_tasks)
    
    # Переходим к выбору аудитории
    await message.answer(
        f"✅ Готово к отправке: {len(valid_tasks)} задач\n"
        "Выберите аудиторию:",
        reply_markup=create_keyboard(["Всем пользователям","По должности", "Вручную", "🔙 Назад"], (2, 2))
    )
    await state.set_state(TaskStates.select_audience)


@dp.message(TaskStates.select_audience, F.text == "Всем пользователям")
async def send_to_all_users(message: types.Message, state: FSMContext):
    user_ids = users_sheet.col_values(1)[1:] # Предполагаем, что ID в первом столбце, без заголовка
    await state.update_data(user_ids=user_ids)
    # --- Изменено: Переход в новое состояние ---
    await state.set_state(TaskStates.review_selection)
    await review_selection_summary(message, state)


@dp.message(TaskStates.select_audience, F.text == "По должности")
async def ask_for_position_filter(message: types.Message, state: FSMContext):
    await message.answer("👥 Введите должность:", reply_markup=cancel_keyboard())
    await state.set_state(TaskStates.input_position)


@dp.message(TaskStates.select_audience, F.text == "Вручную")
async def ask_for_manual_ids(message: types.Message, state: FSMContext):
    """Обработчик кнопки 'Вручную' в меню выбора аудитории."""
    logging.info(f"Пользователь {message.from_user.id} выбрал 'Вручную' в состоянии select_audience. Текст сообщения: '{message.text}'")
    # Добавим явную проверку состояния перед ответом
    current_state = await state.get_state()
    logging.info(f"Текущее состояние перед ответом: {current_state}")
    
    try:
        await message.answer("🔢 Введите ID пользователей через запятую (например: 123456789, 987654321):", reply_markup=cancel_keyboard())
        await state.set_state(TaskStates.input_manual_ids)
        logging.info(f"Состояние успешно изменено на input_manual_ids для пользователя {message.from_user.id}")
    except Exception as e:
        logging.error(f"Ошибка в ask_for_manual_ids для пользователя {message.from_user.id}: {e}", exc_info=True)
        # Отправим сообщение об ошибке, если что-то пошло не так
        try:
            await message.answer("❌ Произошла ошибка. Попробуйте снова или выберите другой способ.", reply_markup=tasks_admin_keyboard())
            await state.clear()
        except:
            pass


@dp.message(TaskStates.input_position)
async def process_position_filter(message: types.Message, state: FSMContext):
    position_input = message.text.strip().lower()
    try:
        users_data = pickle.loads(cache.get("users_data", b"[]"))
        matched_user_ids = [
            str(u["ID пользователя"])
            for u in users_data
            if str(u.get("Должность", "")).strip().lower() == position_input
        ]
        if not matched_user_ids:
            await message.answer("❌ Пользователи с такой должностью не найдены.", reply_markup=tasks_admin_keyboard())
            await state.clear() # <-- Важно: очищать состояние при ошибке
            return
        await state.update_data(user_ids=matched_user_ids)
        # --- Изменено: Переход в новое состояние ---
        await state.set_state(TaskStates.review_selection)
        await review_selection_summary(message, state)
    except Exception as e:
        logging.error(f"Ошибка при фильтрации по должности: {str(e)}")
        await message.answer("❌ Ошибка обработки должности", reply_markup=tasks_admin_keyboard())
        await state.clear()


@dp.message(TaskStates.input_manual_ids, F.text == "❌ Отмена")
async def cancel_manual_user_input(message: types.Message, state: FSMContext):
    """Обработчик отмены при ручном вводе ID пользователей."""
    await state.clear()
    # Отправляем сообщение об отмене и показываем клавиатуру меню задач
    # Убедитесь, что функция tasks_admin_keyboard() определена и доступна
    await message.answer("❌ Ввод ID пользователей отменён.", reply_markup=tasks_admin_keyboard())

@dp.message(TaskStates.input_manual_ids)
async def handle_manual_user_ids(message: types.Message, state: FSMContext):
    user_ids = [uid.strip() for uid in message.text.split(",") if uid.strip().isdigit()]
    if not user_ids:
        await message.answer("❌ Нет валидных ID. Попробуйте снова.", reply_markup=cancel_keyboard())
        # Не очищаем состояние, позволяем повторный ввод
        return
    await state.update_data(user_ids=user_ids)
    # --- Изменено: Переход в новое состояние ---
    await state.set_state(TaskStates.review_selection)
    await review_selection_summary(message, state)


async def review_selection_summary(message: types.Message, state: FSMContext):
    """
    Отправляет администратору сводку по выбранной аудитории и задачам перед финальным подтверждением.
    """
    data = await state.get_data()
    user_ids = data.get("user_ids", [])
    selected_tasks = data.get("selected_tasks", {})
    
    if not user_ids or not selected_tasks:
        await message.answer("❌ Ошибка: Нет данных для подтверждения (пользователи или задачи).", reply_markup=tasks_admin_keyboard())
        await state.clear()
        return

    # Формируем сводку
    summary_lines = [
        "🔍 *Предварительный просмотр рассылки*:",
        f"• *Задач для отправки:* {len(selected_tasks)}",
        f"• *Пользователей для рассылки:* {len(user_ids)}",
        "",
        "*Выбранные задачи:*"
    ]
    # Ограничиваем список задач, если их много
    task_items = list(selected_tasks.items())
    for task_id, task in task_items[:5]: # Показываем первые 5
        summary_lines.append(f"  • `#{task_id}`: {task['text'][:50]}{'...' if len(task['text']) > 50 else ''}")
    if len(task_items) > 5:
        summary_lines.append(f"  ... и ещё {len(task_items) - 5} задач(и).")

    summary_lines.append("")
    summary_lines.append("Вы уверены, что хотите отправить эти задачи этой аудитории?")

    summary_text = "\n".join(summary_lines)
    
    # Отправляем сводку и клавиатуру подтверждения
    await message.answer(
        summary_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=create_keyboard(["📤 Подтвердить отправку", "❌ Отмена"], (2,))
    )

async def send_selected_tasks(selected_tasks: dict, user_ids: list):
    results = {"success": 0, "failed": 0}
    
    for user_id in user_ids:
        try:
            for task_id, task in selected_tasks.items():
                await bot.send_message(
                    user_id,
                    format_task_message(task_id, task),
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_task_keyboard(task_id))
            results["success"] += 1
            logging.info(f"Sent tasks to {user_id}")
        except Exception as e:
            results["failed"] += 1
            logging.error(f"Error sending to {user_id}: {str(e)}")
    
    return results


@dp.message(TaskStates.input_manual_ids)
async def handle_manual_user_ids(message: types.Message, state: FSMContext):
    user_ids = [uid.strip() for uid in message.text.split(",") if uid.strip().isdigit()]

    if not user_ids:
        await message.answer("❌ Нет валидных ID. Попробуйте снова.")
        return

    await state.update_data(user_ids=user_ids)

    await message.answer(
        f"✅ Указано ID: {len(user_ids)}\n📤 Подтвердите отправку задач.",
        reply_markup=create_keyboard(["📤 Подтвердить отправку", "❌ Отмена"], (2,))
    )
    await state.set_state(TaskStates.confirmation)


@dp.message(F.text == "🔙 Назад")
async def handle_back_from_tasks(message: types.Message, state: FSMContext):
    """Обработчик кнопки Назад в меню задач"""
    await state.clear()
    await message.answer("🔙 Возврат в админ-панель", 
                        reply_markup=admin_panel_keyboard())


@dp.message(TaskStates.select_audience, F.text == "❌ Отмена")
async def cancel_sending(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Рассылка отменена", reply_markup=tasks_admin_keyboard())


async def run_in_thread(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


@dp.callback_query(F.data.startswith("task_done:"))
async def mark_task_done(callback: types.CallbackQuery):
    # ⚡ быстрый ответ, чтобы query не протух
    await callback.answer("⏳ Обрабатываю...", show_alert=False)

    task_id = callback.data.split(":")[1]
    user_id = callback.from_user.id
    sheet = get_tasks_sheet()

    try:
        # ⚡ поиск в отдельном потоке
        cell = await run_in_thread(sheet.find, task_id)
        if not cell:
            await callback.message.answer("❌ Задача не найдена")
            return

        # ⚡ чтение ячейки в отдельном потоке
        statuses_raw = await run_in_thread(sheet.cell, cell.row, 9)
        statuses_raw = statuses_raw.value if statuses_raw else ""

        try:
            statuses_data = json.loads(statuses_raw) if statuses_raw.strip() else {}
        except (json.JSONDecodeError, TypeError):
            logging.warning(f"Неверный формат JSON для задачи {task_id} в строке {cell.row}. Создаю новый.")
            statuses_data = {}

        if "completed_by" not in statuses_data:
            statuses_data["completed_by"] = []

        if str(user_id) in statuses_data["completed_by"]:
            await callback.message.answer("✅ Уже отмечено")
            return

        statuses_data["completed_by"].append(str(user_id))

        # ⚡ запись в отдельном потоке
        await run_in_thread(sheet.update_cell, cell.row, 9, json.dumps(statuses_data, ensure_ascii=False))

        await callback.message.answer("✅ Отмечено как выполнено")

        try:
            new_markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="✔️ Выполнено", callback_data="task_already_done")]
            ])
            await callback.message.edit_reply_markup(reply_markup=new_markup)
        except Exception as e:
            logging.warning(f"Не удалось обновить сообщение задачи {task_id}: {e}")

    except Exception as e:
        logging.error(f"Ошибка отметки задачи {task_id} пользователем {user_id}: {str(e)}", exc_info=True)
        await callback.message.answer("❌ Ошибка выполнения. Попробуйте позже.")
        

@dp.callback_query(F.data == "task_already_done")
async def handle_already_done(callback: types.CallbackQuery):
    await callback.answer("✅ Задача уже выполнена вами.", show_alert=True)


# --- Исправленный фрагмент check_deadlines ---
async def check_deadlines():
    """
    Проверка просроченных задач и уведомление пользователей,
    которым задача была назначена, но которые её НЕ ВЫПОЛНИЛИ.
    Проверка происходит раз в сутки.
    """
    while True:
        try:
            logging.info("🔍 Начало проверки просроченных задач...")
            tasks = await load_tasks() # Используем исправленную load_tasks
            if not tasks:
                logging.info("📭 Нет задач для проверки.")
                await asyncio.sleep(86400) # Ждем 24 часа
                continue

            today_date = datetime.now().date()
            notified_count = 0

            for task_id, task in tasks.items():
                deadline_str = task.get("deadline")
                if not deadline_str:
                    continue # Пропускаем задачи без дедлайна

                try:
                    # Преобразуем строку дедлайна в объект date
                    deadline_date = datetime.strptime(deadline_str, "%d.%m.%Y").date()
                except ValueError as e:
                    logging.warning(f"Неверный формат даты для задачи {task_id} ('{deadline_str}'): {e}")
                    continue

                # Проверяем, просрочена ли задача
                if deadline_date < today_date:
                    logging.info(f"⏰ Найдена просроченная задача {task_id}: {task['text']}")

                    # - Исправлено: Используем ключи из исправленной load_tasks -
                    # Получаем множества ID
                    assigned_users = set(task.get("assigned_to", []))
                    completed_users = set(task.get("completed_by", [])) # <-- Исправленный ключ

                    # Находим пользователей для уведомления: назначены, но не выполнили
                    users_to_notify = assigned_users - completed_users

                    if not users_to_notify:
                        logging.info(f"📭 По задаче {task_id} нет пользователей для уведомления "
                                    f"(все выполнили ({len(completed_users)}) или никто не назначен ({len(assigned_users)})).")
                        continue

                    # --- ИЗМЕНЕНО: Формируем сообщение с помощью format_task_message ---
                    # Это включит текст, создателя, дедлайн и ссылки в том же формате, что и обычные задачи
                    try:
                        # Вызываем вашу существующую функцию форматирования
                        notification_text_markdown = format_task_message(task_id, task)
                        # Добавляем префикс/постфикс, если нужно
                        full_notification_text = f"🚨 *Просроченная задача!*\n\n{notification_text_markdown}"
                    except Exception as format_e:
                        # На случай, если format_task_message как-то сломается, fallback
                        logging.error(f"Ошибка форматирования уведомления для задачи {task_id} с format_task_message: {format_e}")
                        full_notification_text = (
                            f"🚨 *Просроченная задача!*\n"
                            f"📌 *Задача #{task_id}:* {task.get('text', 'Текст не указан')}\n"
                            f"📅 *Дедлайн был:* {deadline_str}\n"
                            )
                    # -------------------------------------------------------------------

                    # Отправляем уведомления
                    for user_id_str in users_to_notify:
                        try:
                            user_id_int = int(user_id_str)
                            
                        
                            
                            task_kb = get_task_keyboard(task_id) 
                            
                            await bot.send_message(
                                user_id_int,
                                full_notification_text, # Используем сформированный текст
                                parse_mode=ParseMode.MARKDOWN, # Используем Markdown
                                reply_markup=task_kb # Прикрепляем кнопку "Выполнено"
                            )
            
                            
                            logging.info(f"✉️ Уведомление о просроченной задаче {task_id} отправлено пользователю {user_id_int}")
                            notified_count += 1
                            # Небольшая пауза между сообщениями для соблюдения лимитов API
                            await asyncio.sleep(0.1) 
                        except ValueError:
                            logging.error(f"Неверный формат ID пользователя '{user_id_str}' для задачи {task_id}")
                        except Exception as send_e: # Ловим другие ошибки отправки (заблокировал бота и т.д.)
                             logging.error(f"Ошибка отправки уведомления о просроченной задаче {task_id} пользователю {user_id_str}: {send_e}")

            logging.info(f"🔚 Проверка просроченных задач завершена. Отправлено {notified_count} уведомлений.")
        except Exception as e:
            logging.error(f"Критическая ошибка в check_deadlines: {e}", exc_info=True)
        
        # Ждем 24 часа до следующей проверки
        await asyncio.sleep(86400) 


@dp.message(TaskStates.review_selection, F.text == "📤 Подтвердить отправку") 
async def confirm_task_dispatch(message: types.Message, state: FSMContext):
    """
    Подтверждение и выполнение рассылки задач.
    Перед отправкой назначает задачи выбранным пользователям.
    """
    data = await state.get_data()
    user_ids = data.get("user_ids", [])
    selected_tasks = data.get("selected_tasks", {})
    # --- Проверка наличия данных ---
    if not user_ids or not selected_tasks:
        await message.answer("❌ Нет получателей или задач для отправки.", reply_markup=tasks_admin_keyboard())
        await state.clear()
        return
    wait_msg = await message.answer("🔄 Начинаю процесс отправки задач...") 
    try:
        task_ids_to_assign = list(selected_tasks.keys())
        # Преобразуем user_ids из строки (как они хранятся в state) в int
        user_ids_int = [int(uid) for uid in user_ids if uid.isdigit()]
        if task_ids_to_assign and user_ids_int:
            # Назначаем задачи пользователям в Google Sheets
            # Передаем sheet, чтобы не переоткрывать соединение
            sheet = get_tasks_sheet() 
            await assign_tasks_to_users(task_ids_to_assign, user_ids_int, sheet=sheet)
            await message.answer("✅ Задачи успешно назначены выбранным пользователям.")
        else:
            logging.warning("Нет корректных ID задач или пользователей для назначения.")
    except Exception as e:
        logging.error(f"Ошибка при назначении задач: {e}")
        # Можно решить, продолжать ли рассылку в случае ошибки назначения
        await message.answer("⚠️ Ошибка при назначении задач, но рассылка продолжится.")
    success = 0
    failed = 0
    total_attempts = len(user_ids) * len(selected_tasks)
    if total_attempts > 100: # <-- Пример: для больших рассылок показываем прогресс
         progress_msg = await message.answer(f"📨 Отправка... (0/{len(user_ids)})")
    for i, uid in enumerate(user_ids):
        for task_id, task in selected_tasks.items():
            try:  
                await bot.send_message(
                    int(uid),
                    format_task_message(task_id, task),
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_task_keyboard(task_id)
                )
                success += 1
                await asyncio.sleep(0.05) # немного уменьшена пауза
            except Exception as e:
                logging.warning(f"Ошибка отправки задачи {task_id} пользователю {uid}: {e}")
                failed += 1
        # Обновляем прогресс, если нужно
        if total_attempts > 100 and (i + 1) % 10 == 0: # <-- Обновляем каждые 10 пользователей
            try:
                await progress_msg.edit_text(f"📨 Отправка... ({i+1}/{len(user_ids)})")
            except:
                pass # Игнорируем ошибки редактирования прогресса
    # Финальный отчет
    report = f"📊 Отправка завершена:\n• Пользователей: {len(user_ids)}\n• Задач каждому: {len(selected_tasks)}\n• Успешных отправок: {success}\n• Ошибок: {failed}"
    await message.answer(report, reply_markup=tasks_admin_keyboard())
    await state.clear()
    # Удаляем сообщения о прогрессе, если они были
    if total_attempts > 100:
         try:
             await wait_msg.delete()
             await progress_msg.delete()
         except:
             pass


@dp.message(TaskStates.review_selection, F.text == "❌ Отмена") # <-- Новый фильтр
async def cancel_task_dispatch(message: types.Message, state: FSMContext):
    await message.answer("❌ Отправка отменена", reply_markup=tasks_admin_keyboard())
    await state.clear()

@dp.message(Command(commands=['debug_on']))
async def cmd_debug_on(message: types.Message):
    if message.from_user.id not in ADMINS:
        return
    init_tracemalloc()
    # Запускаем мониторинг один раз при включении, если нужно
    # asyncio.create_task(memory_monitor()) # Можно раскомментировать, если хотите активный мониторинг в консоль
    await message.answer("🟢 Режим отладки памяти ВКЛЮЧЕН. tracemalloc запущен.")

@dp.message(Command(commands=['debug_off']))
async def cmd_debug_off(message: types.Message):
    if message.from_user.id not in ADMINS:
        return
    stop_tracemalloc()
    await message.answer("🔴 Режим отладки памяти ВЫКЛЮЧЕН. tracemalloc остановлен.")

@dp.message(Command(commands=['gc_collect']))
async def cmd_gc_collect(message: types.Message):
    if message.from_user.id not in ADMINS:
        return
    collected = gc.collect()
    await message.answer(f"🗑️ Сборка мусора выполнена. Удалено объектов: {collected}")


# --- Рефакторинг handle_mytasks ---
@dp.message(Command("mytasks"))
async def handle_mytasks(message: types.Message):
    user_id = str(message.from_user.id) # Преобразуем в строку для сравнения
    try:
        # 1. Загружаем ВСЕ задачи с помощью универсальной функции
        all_tasks = await load_tasks() # <-- Используем load_tasks

        # 2. Фильтруем задачи: оставляем только НЕВЫПОЛНЕННЫЕ пользователем
        pending_tasks = []
        # all_tasks это словарь {task_id: task_data}
        for task_id, task_data in all_tasks.items():
             # task_data уже содержит корректно распарсенный список completed_by
             completed_users_list = task_data.get("completed_by", [])
             if user_id not in completed_users_list:
                 # task_data уже содержит нужные поля (text, deadline и т.д.)
                 # Можно использовать его напрямую
                 pending_tasks.append((task_id, task_data)) 
                 # Если по какой-то причине нужно использовать normalize_task_row, 
                 # можно, но это менее эффективно, чем использовать task_data напрямую.
                 # pending_tasks.append((task_id, normalize_task_row(task_id, task_data))) 

        if not pending_tasks:
            await message.answer("✅ У вас нет незавершённых задач.")
            return

        total_pending = len(pending_tasks)
        shown_count = min(5, total_pending)
        await message.answer(f"📋 У вас {total_pending} незавершенных задач(и). Показываю первые {shown_count}:")

        # 3. Отправляем отфильтрованные задачи
        for task_id, task in pending_tasks[:5]: # показываем максимум 5
            # Убедитесь, что format_task_message работает с форматом task_data из load_tasks
            msg = format_task_message(task_id, task) 
            try:
                await message.answer(
                    msg,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_task_keyboard(task_id)
                )
            except Exception as e:
                logging.error(f"Ошибка отправки задачи {task_id} пользователю {user_id}: {e}")
                await message.answer(f"⚠️ Ошибка при отображении задачи {task_id}")

        if total_pending > 5:
            await message.answer(
                f"ℹ️ Показаны первые 5 задач. Осталось ещё {total_pending - 5}. "
                f"Проверяйте регулярно или обратитесь к администратору за полным списком."
            )

    except Exception as e:
        logging.error(f"Ошибка в /mytasks для пользователя {message.from_user.id}: {str(e)}", exc_info=True)
        await message.answer("❌ Не удалось загрузить ваши задачи. Попробуйте позже.")


@dp.message(F.text == "📊 Статистика выполнения") # <-- Слушает из любого состояния
async def handle_stats_from_main_menu(message: types.Message, state: FSMContext):
    """Обработчик кнопки '📊 Статистика выполнения' из основного меню задач."""
    if message.from_user.id not in ADMINS:
        return
    try:
        logging.info(f"Запрос статистики от администратора {message.from_user.id}")
        tasks = await load_tasks() # Загружаем задачи
        logging.info(f"load_tasks вернул {len(tasks) if tasks else 0} задач. Тип: {type(tasks)}")
        # Логируем пример первой задачи для проверки структуры
        if tasks:
            first_task_id, first_task = next(iter(tasks.items()))
            logging.info(f"Пример задачи (ID: {first_task_id}): {first_task}")
            logging.info(f"  completed_by: {first_task.get('completed_by', 'N/A')} (тип: {type(first_task.get('completed_by', 'N/A'))})")
            logging.info(f"  assigned_to: {first_task.get('assigned_to', 'N/A')} (тип: {type(first_task.get('assigned_to', 'N/A'))})")
            
        if not tasks:
            await message.answer("📭 Нет задач для отображения статистики.", reply_markup=tasks_admin_keyboard())
            return
        # Сохраняем задачи в состоянии
        await state.update_data(tasks=tasks)
        
        # --- Логика из show_stats_menu ---
        stats_lines = ["📊 *Статистика выполнения задач:*"]
        for task_id, task in tasks.items():
            completed_count = len(task.get('completed_by', []))
            assigned_count = len(task.get('assigned_to', []))
            # Логируем подсчет для каждой задачи
            logging.info(f"Задача {task_id}: completed={completed_count}, assigned={assigned_count}")
            # Более информативная строка
            stats_line = f"🔹 `#{task_id}`: {task['text'][:30]}{'...' if len(task['text']) > 30 else ''} - ✅ {completed_count}/{assigned_count if assigned_count > 0 else '?'}"
            stats_lines.append(stats_line)
        
        stats_text = "\n".join(stats_lines) # <-- Исправлено: используем \n
        logging.info(f"Сформированный текст статистики (длина: {len(stats_text)}): {stats_text[:200]}...")
        # Проверяем длину, если слишком длинная, можно разбить на части или предложить выбор задачи
        if len(stats_text) > 4000: # Примерный лимит
             stats_text = stats_text[:3900] + "\n... (список обрезан)"
             logging.warning("Текст статистики был обрезан из-за превышения лимита длины.")
             
        await message.answer(
            stats_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=create_keyboard(["Детали по задаче", "🔙 Назад"], (1,))
        )
        await state.set_state(TaskStates.view_stats)
        logging.info("Статистика успешно отправлена.")
        
    except Exception as e:
        logging.error(f"Ошибка при запросе статистики из главного меню для пользователя {message.from_user.id}: {e}", exc_info=True)
        await message.answer("❌ Ошибка загрузки статистики.", reply_markup=tasks_admin_keyboard())
    

@dp.message(TaskStates.view_stats, F.text == "Детали по задаче")
async def ask_for_task_details(message: types.Message, state: FSMContext):
    data = await state.get_data()
    tasks = data['tasks']
    if not tasks:
         await message.answer("📭 Нет задач для детализации.", reply_markup=tasks_admin_keyboard())
         await state.set_state(TaskStates.select_action)
         return
    # Предлагаем список ID для удобства
    task_ids = list(tasks.keys())
    await message.answer(
        f"✏️ Введите ID задачи для детализации:\n"
        f"Доступные ID: {', '.join(task_ids) if len(task_ids) <= 10 else ', '.join(task_ids[:10]) + '...'}",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(TaskStates.input_task_id_for_details)




# --- Исправленный фрагмент show_task_details с markdown_decoration ---
@dp.message(TaskStates.input_task_id_for_details)
async def show_task_details(message: types.Message, state: FSMContext):
    input_task_id = str(message.text.strip())
    data = await state.get_data()
    tasks = data['tasks']
    string_keyed_tasks = {str(k): v for k, v in tasks.items()}

    if input_task_id not in string_keyed_tasks:
        similar_ids = [tid for tid in string_keyed_tasks.keys() if input_task_id in tid or tid in input_task_id]
        if similar_ids:
            # Используем markdown_decoration.quote для безопасного отображения ID в сообщении
            escaped_input_id = markdown_decoration.quote(input_task_id)
            escaped_similar_ids = [markdown_decoration.quote(sid) for sid in similar_ids[:3]]
            await message.answer(
                f"❌ Задача с ID `{escaped_input_id}` не найдена.\n"
                f"Возможно, вы имели в виду: {', '.join(escaped_similar_ids)}?",
                parse_mode=ParseMode.MARKDOWN, # parse_mode можно оставить, так как мы экранировали
                reply_markup=cancel_keyboard()
            )
            # Не очищаем state, позволяем повторный ввод
            return
        else:
            await message.answer("❌ Задача не найдена.", reply_markup=tasks_admin_keyboard())
            await state.clear()
            return

    task = string_keyed_tasks[input_task_id]
    
    # --- Улучшено и Исправлено: Получаем имена назначенных и выполнивших с экранированием ---
    assigned_user_names = []
    for user_id_str in task.get('assigned_to', []):
        try:
            initials = await get_user_initials(int(user_id_str))
            # Экранируем данные, полученные из внешних источников
            escaped_initials = markdown_decoration.quote(initials)
            escaped_user_id = markdown_decoration.quote(user_id_str)
            assigned_user_names.append(f"{escaped_initials} (ID: {escaped_user_id})")
        except (ValueError, TypeError) as e:
            logging.warning(f"Неверный формат ID пользователя '{user_id_str}' для задачи {input_task_id} (назначенные): {e}")
            # Экранируем ID даже в случае ошибки
            escaped_user_id = markdown_decoration.quote(user_id_str)
            assigned_user_names.append(f"ID: {escaped_user_id} (Ошибка)")
        except Exception as e:
            logging.error(f"Ошибка получения инициалов для ID {user_id_str} (назначенные): {e}")
            # Экранируем ID даже в случае ошибки
            escaped_user_id = markdown_decoration.quote(user_id_str)
            assigned_user_names.append(f"ID: {escaped_user_id} (Ошибка загрузки)")

    completed_user_names = []
    for user_id_str in task.get('completed_by', []): # Используем исправленный ключ
        try:
            initials = await get_user_initials(int(user_id_str))
            # Экранируем данные, полученные из внешних источников
            escaped_initials = markdown_decoration.quote(initials)
            escaped_user_id = markdown_decoration.quote(user_id_str)
            completed_user_names.append(f"{escaped_initials} (ID: {escaped_user_id})")
        except (ValueError, TypeError) as e:
            logging.warning(f"Неверный формат ID пользователя '{user_id_str}' для задачи {input_task_id} (выполнившие): {e}")
            # Экранируем ID даже в случае ошибки
            escaped_user_id = markdown_decoration.quote(user_id_str)
            completed_user_names.append(f"ID: {escaped_user_id} (Ошибка)")
        except Exception as e:
            logging.error(f"Ошибка получения инициалов для ID {user_id_str} (выполнившие): {e}")
            # Экранируем ID даже в случае ошибки
            escaped_user_id = markdown_decoration.quote(user_id_str)
            completed_user_names.append(f"ID: {escaped_user_id} (Ошибка загрузки)")

    # --- Исправлено: Экранируем данные из задачи ---
    escaped_task_id = markdown_decoration.quote(input_task_id)
    escaped_task_text = markdown_decoration.quote(task['text'])
    # Для link, deadline, creator_initials также желательно экранировать, если они могут содержать спецсимволы
    escaped_task_link = markdown_decoration.quote(task.get('link', 'Нет') if task.get('link') else 'Нет')
    escaped_task_deadline = markdown_decoration.quote(task.get('deadline', 'Не установлен'))
    escaped_creator_initials = markdown_decoration.quote(task.get('creator_initials', 'Неизвестно'))

    # --- Исправлено: Используем \n для переносов строк ---
    response_lines = [
        f"📋 *Детали задачи #{escaped_task_id}*:", # ID уже экранирован
        f"📌 *Текст:* {escaped_task_text}", # Текст экранирован
        f"👤 *Создал:* {escaped_creator_initials}", # Инициалы экранированы
        f"🔗 *Ссылка:* {escaped_task_link}", # Ссылка экранирована
        f"📅 *Дедлайн:* {escaped_task_deadline}", # Дедлайн экранирован
        f"📬 *Назначена ({len(assigned_user_names)}):*",
        ("\n".join(assigned_user_names) if assigned_user_names else "Никто не назначен"), # \n между именами
        f"✅ *Выполнили ({len(completed_user_names)}):*",
        ("\n".join(completed_user_names) if completed_user_names else "Никто не выполнил") # \n между именами
    ]
    
    # --- Исправлено: Используем \n для объединения строк ---
    response = "\n".join(response_lines) 
    
    # Проверка длины сообщения (по-прежнему актуальна)
    if len(response) > 4096:
        # Можно разбить на несколько сообщений или обрезать
        response = response[:4000] + "\n... (сообщение обрезано)"
        
    await message.answer(response, parse_mode=ParseMode.MARKDOWN, reply_markup=tasks_admin_keyboard())
    await state.clear()


##======================================Множественный Ввод==============================


@dp.message(OrderStates.waiting_for_quantities_list, F.text)
async def process_quantities_list(message: types.Message, state: FSMContext):
    raw_text = message.text.strip()
    parts = re.split(r'[,\n\r]+', raw_text)
    quantities = []
    for part in parts:
        part = part.strip()
        if part.isdigit():
            quantities.append(int(part))

    # Получаем артикулы из state
    data = await state.get_data()
    articles = data.get('art_list', [])

    if len(articles) != len(quantities):
        await message.answer(f"❌ Количество артикулов ({len(articles)}) и количеств ({len(quantities)}) не совпадает.")
        return

    # Соединяем в список словарей
    order_items = []
    for article, qty in zip(articles, quantities):
        order_items.append({
            'article': article,
            'quantity': qty
        })

    # Сохраняем список артикулов и количеств в state
    await state.update_data(batch_order_items=order_items)

    # Запрашиваем магазин (как в старой логике)
    await message.answer("📌 Выберите магазин для заказа:", reply_markup=quick_shop_selection_keyboard())
    await state.set_state(OrderStates.batch_shop_selection) # Новое состояние


@dp.message(OrderStates.batch_shop_selection)
async def process_batch_shop_selection(message: types.Message, state: FSMContext):
    # Словарь для сопоставления текста кнопки с номером магазина
    shop_mapping = {
        "🏪 Магазин 8": "8",
        "🏪 Магазин 92": "92",
        "🏪 Магазин 147": "147",
        "🏪 Магазин 150": "150",
        "🏪 Магазин 165": "165",
        "🏪 Магазин 255": "255"
    }

    if message.text in shop_mapping:
        selected_shop = shop_mapping[message.text]
        await state.update_data(selected_shop=selected_shop)
        # Переходим к получению информации и выводу
        await continue_batch_order_process(message, state)
    elif message.text == "❌ Отмена":
        await message.answer("❌ Выбор магазина отменен.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.clear()
    else:
        await message.answer(
            "❌ Неверный выбор. Пожалуйста, выберите один из вариантов:",
            reply_markup=quick_shop_selection_keyboard()
        )



async def continue_batch_order_process(message: types.Message, state: FSMContext):
    """Продолжение обработки заказа списка артикулов после выбора магазина"""
    data = await state.get_data()
    order_items = data.get('batch_order_items', []) # Получаем список из state
    selected_shop = data.get('selected_shop')

    if not order_items or not selected_shop:
        await message.answer("❌ Ошибка данных заказа.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.clear()
        return

    await message.answer(f"🔄 Загружаю информацию о {len(order_items)} товарах...")

    valid_items = []
    results_found = 0
    results_not_found = 0

    for item in order_items:
        product_info = await get_product_info(item['article'], selected_shop)
        if product_info:
            top_status = product_info.get('Топ в магазине', '0')
            is_top_0 = (top_status == '0')
            # --- ПОЛУЧАЕМ отдел из product_info ---
            item_department = product_info.get('Отдел', 'Не указано').strip()
            # --- /ПОЛУЧАЕМ ---
            valid_items.append({
                'article': item['article'],
                'quantity': item['quantity'],
                'name': product_info['Название'],
                'delivery_date': product_info['Дата поставки'],
                'top_0': is_top_0,
                'department': item_department,  # <-- Сохраняем
                'supplier_name': product_info.get('Поставщик', 'Не указано').strip()
            })
            results_found += 1
        else:
            await message.answer(f"❌ Артикул {item['article']} не найден в магазине {selected_shop}.")
            results_not_found += 1

    if not valid_items:
        await message.answer("❌ Ни один из артикулов не может быть заказан.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.clear()
        return

    # Сохраняем валидный список
    await state.update_data(valid_items=valid_items)

    # Выводим краткий список
    summary_lines = []
    for item in valid_items:
        marker = "⚠️ (ТОП 0)" if item['top_0'] else ""
        summary_lines.append(f"📦 {item['article']} - {item['name']} - x{item['quantity']} - {item['delivery_date']} {marker}")
    summary_text = "\n".join(summary_lines)
    await message.answer(summary_text)

    # Проверяем, есть ли среди них ТОП 0
    has_top_0 = any(item['top_0'] for item in valid_items)

    if has_top_0:
        await message.answer("⚠️ Среди артикулов есть ТОП 0. Необходимо одобрение МЗ.")
    await message.answer("Введите причину заказа.")

    await state.set_state(OrderStates.batch_order_reason_input)


@dp.message(OrderStates.batch_order_reason_input, F.text)
async def process_batch_order_reason(message: types.Message, state: FSMContext):

    if not message.text:
        await message.answer("❌ Ожидается текстовое сообщение. Пожалуйста, введите причину заказа.")
        return

    
    reason = message.text.strip()
    data = await state.get_data()
    valid_items = data.get('valid_items', [])
    selected_shop = data.get('selected_shop')
    user_id = str(message.from_user.id)

    await state.update_data(order_reason=reason)

    confirm_lines = ["🔎 Проверьте данные заказа:"]
    confirm_lines.append(f"🏪 Магазин: {selected_shop}")
    confirm_lines.append("📦 Товары:")
    for item in valid_items:
        top_marker = " ⚠️(ТОП 0)" if item['top_0'] else ""
        confirm_lines.append(f"  - {item['article']} ({item['name']}) x{item['quantity']} - {item['delivery_date']}{top_marker}")
    confirm_lines.append(f"📝 Причина: {reason}")
    confirm_text = "\n".join(confirm_lines)

    await message.answer(confirm_text, reply_markup=confirm_keyboard())
    await state.set_state(OrderStates.batch_confirmation)


@dp.message(OrderStates.batch_confirmation, F.text.lower() == "✅ подтвердить")
async def confirm_batch_order(message: types.Message, state: FSMContext):
    data = await state.get_data()
    valid_items = data.get('valid_items', [])
    selected_shop = data.get('selected_shop')
    reason = data.get('order_reason', '')
    user_id = str(message.from_user.id)

    # Получаем информацию о пользователе
    user_data = await get_user_data(str(user_id))
    user_name = f"{user_data.get('name', 'Не указано')} {user_data.get('surname', '')}".strip() or "Не указано"
    user_position = user_data.get('position', 'Не указана')

    enqueued_count = 0
    skipped_count = 0

    # --- Обработка ВСЕХ артикулов --- (и ТОП 0, и обычные)
    for item in valid_items:
        # --- Получаем информацию о товаре ---
        product_info = await get_product_info(item['article'], selected_shop)
        if not product_info:
            logging.error(f"❌ Товар {item['article']} не найден в магазине {selected_shop} при добавлении в очередь.")
            skipped_count += 1
            continue

        # --- Получаем отдел товара ---
        item_department = product_info.get('Отдел', 'Не указано').strip()
        if item_department == 'Не указано' or not item_department:
            logging.error(f"❌ Отдел не указан для артикула {item['article']} при добавлении в очередь.")
            skipped_count += 1
            continue

        # --- Подготовим словарь с данными для одного заказа ---
        single_order_data = {
            'selected_shop': selected_shop,
            'article': item['article'],
            'order_reason': reason, # <-- Одна причина для всех
            'quantity': item['quantity'],
            'department': item_department, # <-- Отдел товара
            'user_name': user_name,
            'user_position': user_position,
            'product_name': product_info['Название'],
            'supplier_name': product_info.get('Поставщик', 'Не указано').strip(),
            'order_date': product_info.get('Дата заказа', 'N/A'), # или 'N/A'
            'delivery_date': product_info.get('Дата поставки', 'N/A'), # или 'N/A'
            'top_0': product_info.get('Топ в магазине', '0') == '0', # <-- Определяем здесь
            'batch_order': True # <-- Флаг, что это из множественного ввода
        }

        # --- Добавляем в очередь ---
        success_enqueue = await add_order_to_queue(user_id, single_order_data)
        if success_enqueue:
            enqueued_count += 1
        else:
            logging.error(f"❌ Не удалось добавить в очередь артикул {item['article']}")
            skipped_count += 1

    # --- Итоговое сообщение пользователю ---
    summary_parts = []
    if enqueued_count > 0:
        summary_parts.append(f"✅ {enqueued_count} артикулов отправлено в очередь.")
    if skipped_count > 0:
        summary_parts.append(f"❌ {skipped_count} артикулов пропущено (ошибка).")
    if not summary_parts:
        summary_parts.append("❌ Ничего не было отправлено.")

    await message.answer(" ".join(summary_parts), reply_markup=main_menu_keyboard(message.from_user.id))
    await state.clear()


# =======================РАБОТА С ЗАПРОСАМИ =======================

def initialize_approval_requests_table():
    """Создает таблицу для хранения запросов на одобрение заказа ТОП 0."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            # 1. Создание таблицы (без изменений, IF NOT EXISTS предотвратит ошибку, если таблица уже есть)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS approval_requests (
                    request_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    manager_id INTEGER NOT NULL,
                    department TEXT NOT NULL,
                    article TEXT NOT NULL,
                    shop TEXT NOT NULL,
                    product_name TEXT,
                    product_supplier TEXT,
                    status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'approved', 'rejected'
                    user_data TEXT NOT NULL,
                    manager_message_id INTEGER,
                    reject_comment TEXT, -- <-- Новый столбец
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # 2. Проверка и добавление столбца reject_comment, если его нет
            # Получаем информацию о столбцах таблицы
            cursor.execute("PRAGMA table_info(approval_requests)")
            columns = [info[1] for info in cursor.fetchall()] # info[1] - это имя столбца

            if 'reject_comment' not in columns:
                logging.info("Добавление столбца 'reject_comment' в таблицу 'approval_requests'...")
                cursor.execute("ALTER TABLE approval_requests ADD COLUMN reject_comment TEXT")
                logging.info("Столбец 'reject_comment' успешно добавлен.")
            else:
                logging.debug("Столбец 'reject_comment' уже существует в таблице 'approval_requests'.")

            # 3. Создание индекса (без изменений)
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_user_status ON approval_requests(user_id, status)
            ''')

            conn.commit()
            logging.info("✅ Таблица approval_requests проверена/инициализирована")
    except Exception as e:
        logging.error(f"❌ Ошибка инициализации/обновления таблицы approval_requests: {e}")
        # Важно не подавлять эту ошибку, чтобы знать о проблемах со схемой БД


def get_manager_id_by_department(department: str) -> Optional[int]:
    """Получает ID менеджера по названию отдела из кэша."""
    try:
        managers_data_pickled = cache.get("managers_data")
        if not managers_data_pickled:
            logging.warning("Кэш менеджеров пуст.")
            return None

        managers_records = pickle.loads(managers_data_pickled)

        # Предполагаемая структура листа "МЗ":
        # "ID менеджера" | "Отдел" | "Имя" | "Фамилия"
        # Для корректной работы убедитесь, что форматы совпадают.
        # Приведение к str для надежности сравнения.
        for record in managers_records:
            if str(record.get("Отдел")) == str(department):
                manager_id_raw = record.get("ID менеджера")
                if manager_id_raw:
                    try:
                        # Преобразуем ID из Google Sheets в int
                        manager_info = {
                            "id": int(manager_id_raw),
                            "first_name": record.get("Имя", ""),     # Новое поле
                            "last_name": record.get("Фамилия", ""),  # Новое поле
                            "department": record.get("Отдел", "")
                        }
                        return manager_info
                    except (ValueError, TypeError):
                        logging.warning(f"Некорректный ID менеджера в записи: {record}")
                        continue
        logging.info(f"Менеджер для отдела '{department}' не найден в кэше.")
        return None
    except Exception as e:
        logging.error(f"Ошибка получения информации о менеджере по отделу '{department}': {e}")
        return None


async def create_approval_request(
    request_id: str,
    user_id: int,
    manager_id: int,
    department: str,
    article: str,
    shop: str,
    product_name: str,
    product_supplier: str,
    user_data: Dict[str, Any]
) -> bool:
    """Создает запись запроса на одобрение в БД."""
    try:
        serialized_data = json.dumps(user_data, ensure_ascii=False, default=str) # Сериализуем данные FSM
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO approval_requests 
                (request_id, user_id, manager_id, department, article, shop, product_name, product_supplier, user_data) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                request_id, user_id, manager_id, department, article,
                shop, product_name, product_supplier, serialized_data
            ))
            conn.commit()
            logging.info(f"✅ Создан запрос на одобрение {request_id} для пользователя {user_id}")
            return True
    except Exception as e:
        logging.error(f"❌ Ошибка создания запроса на одобрение для {user_id}: {e}")
        return False


# Примечание: get_pending_approval_request не используется в текущей логике, но пусть будет
async def get_pending_approval_request(user_id: int) -> Optional[dict]:
    """Получает последний незавершенный (ожидающий) запрос пользователя."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM approval_requests 
                WHERE user_id = ? AND status = 'pending' 
                ORDER BY created_at DESC LIMIT 1
            ''', (user_id,))
            row = cursor.fetchone()
            if row:
                # Преобразуем sqlite3.Row в словарь и десериализуем user_data
                result = dict(row)
                try:
                    result['user_data'] = json.loads(result['user_data'])
                except json.JSONDecodeError as e:
                    logging.error(f"❌ Ошибка десериализации user_data для запроса {result['request_id']}: {e}")
                    result['user_data'] = {}
                return result
            return None
    except Exception as e:
        logging.error(f"❌ Ошибка получения ожидающего запроса для {user_id}: {e}")
        return None


async def update_approval_request_status(
    request_id: str,
    status: str,
    manager_message_id: Optional[int] = None,
    reject_comment: Optional[str] = None
) -> bool:
    """Обновляет статус запроса на одобрение."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            # Формируем SQL-запрос динамически в зависимости от переданных параметров
            # Это базовый пример. В production лучше использовать более безопасные методы,
            # например, отдельные подготовленные выражения для каждой комбинации параметров.

            # Определяем, какие параметры переданы, чтобы построить корректный SQL

            set_parts = ["status = ?", "updated_at = CURRENT_TIMESTAMP"]
            params = [status] # status всегда передается

            if manager_message_id is not None:
                set_parts.append("manager_message_id = ?")
                params.append(manager_message_id)

            if reject_comment is not None:
                set_parts.append("reject_comment = ?")
                params.append(reject_comment)

            # Добавляем request_id в конец списка параметров для WHERE
            params.append(request_id)

            sql = f"UPDATE approval_requests SET {', '.join(set_parts)} WHERE request_id = ?"

            cursor.execute(sql, params)
            conn.commit()

            if cursor.rowcount > 0:
                logging.info(f"✅ Статус запроса {request_id} обновлен на '{status}'")
                # Если был передан комментарий при отказе, логируем его
                if status == 'rejected' and reject_comment:
                    logging.info(f"   Причина отказа: {reject_comment}")
                return True
            else:
                logging.warning(f"⚠️ Запрос {request_id} не найден для обновления статуса")
                return False
    except Exception as e:
        logging.error(f"❌ Ошибка обновления статуса запроса {request_id}: {e}")
        return False


async def get_approval_request_by_id(request_id: str) -> Optional[dict]:
    """Получает запрос по его ID."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM approval_requests WHERE request_id = ?', (request_id,))
            row = cursor.fetchone()
            if row:
                result = dict(row)
                try:
                    result['user_data'] = json.loads(result['user_data'])
                except json.JSONDecodeError as e:
                    logging.error(f"❌ Ошибка десериализации user_data для запроса {request_id}: {e}")
                    result['user_data'] = {}
                return result
            return None
    except Exception as e:
        logging.error(f"❌ Ошибка получения запроса {request_id}: {e}")
        return None


async def delete_approval_request(request_id: str) -> bool:
    """Удаляет запись запроса из БД."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM approval_requests WHERE request_id = ?', (request_id,))
            conn.commit()
            if cursor.rowcount > 0:
                logging.info(f"✅ Запрос {request_id} удален из БД")
                return True
            else:
                logging.warning(f"⚠️ Запрос {request_id} не найден для удаления")
                return False
    except Exception as e:
        logging.error(f"❌ Ошибка удаления запроса {request_id}: {e}")
        return False


@dp.callback_query(F.data.startswith("approve:") | F.data.startswith("start_reject:"))
async def handle_manager_approval(callback: types.CallbackQuery, state: FSMContext):
    """Обработка нажатий кнопок одобрения/отказа менеджера."""
    action, request_id = callback.data.split(":", 1)
    manager_id = callback.from_user.id

    # --- Получение запроса из БД ---
    request_data = await get_approval_request_by_id(request_id)
    if not request_data:
        await callback.answer("❌ Запрос не найден.", show_alert=True)
        return

    if request_data['manager_id'] != manager_id:
        await callback.answer("❌ Это не ваш запрос.", show_alert=True)
        return

    if request_data['status'] != 'pending':
        await callback.answer(f"❌ Запрос уже {request_data['status']}.", show_alert=True)
        # Обновляем сообщение менеджера
        try:
            status_text = "одобрен" if request_data['status'] == 'approved' else "отклонен"
        # ИСПОЛЬЗУЕМ callback.message.text ВМЕСТО callback.message.text_markdown_v2
            original_text = callback.message.text or "Запрос на одобрение"
            await callback.message.edit_text(
                f"{original_text}\n\n<i>Статус уже изменен: {status_text}</i>",
                parse_mode='HTML'
            )
        except Exception as e:
            logging.warning(f"Не удалось отредактировать сообщение менеджера: {e}")
        return

    user_id = request_data['user_id']
    article = request_data['article']
    product_name = request_data['product_name']
    shop = request_data['shop']

    if action == "approve":
        # --- Одобрение ---
        success = await update_approval_request_status(request_id, 'approved')
        if success:
            # --- Уведомление пользователя ---
            user_message = (
                f"✅ <b>Менеджер одобрил заказ артикула {article} {product_name} для магазина {shop}.</b>\n"
                f"Нажмите кнопку ниже, чтобы продолжить оформление заказа."
            )
            # Используем InlineKeyboardBuilder
            builder = InlineKeyboardBuilder()
            builder.button(text="🔁 Продолжить заказ", callback_data=f"continue_order:{request_id}")
            user_kb = builder.as_markup()

            try:
                await bot.send_message(chat_id=user_id, text=user_message, reply_markup=user_kb, parse_mode='HTML')
                await callback.answer("✅ Заказ одобрен. Пользователь уведомлен.", show_alert=True)

                # --- Обновление сообщения менеджера ---
                try:
                # ИСПОЛЬЗУЕМ callback.message.text ВМЕСТО callback.message.text_markdown_v2
                    original_text = callback.message.text or "Запрос на одобрение"
                    await callback.message.edit_text(
                        f"{original_text}\n\n✅ <b>Одобрено</b>",
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logging.warning(f"Не удалось отредактировать сообщение менеджера (одобрение): {e}")

            except Exception as e:
                logging.error(f"❌ Не удалось отправить уведомление пользователю {user_id}: {e}")
                await callback.answer("✅ Заказ одобрен, но не удалось уведомить пользователя.", show_alert=True)
        else:
            await callback.answer("❌ Ошибка при обновлении статуса запроса.", show_alert=True)

    elif action in ["start_reject", "request_reject"]:
        # --- Запрашиваем комментарий у менеджера ---
        try:
            # Редактируем сообщение менеджера, убирая кнопки и запрашивая комментарий
            original_text = callback.message.text or "Запрос на одобрение"
            await callback.message.edit_text(
                f"{original_text}\n\n📝 <b>Введите причину отказа в следующем сообщении:</b>",
                parse_mode='HTML'
                # reply_markup=None # Убираем кнопки
            )
            # Сохраняем request_id в состоянии менеджера
            await state.set_state(ManagerApprovalStates.awaiting_reject_comment)
            # Сохраняем request_id в данных состояния, чтобы потом его использовать
            # Можно использовать user_id менеджера как ключ, но проще использовать FSMContext напрямую
            # Для простоты, сохраним в данных состояния текущего менеджера
            await state.update_data(current_reject_request_id=request_id)

            await callback.answer("Введите причину отказа в следующем сообщении.", show_alert=True)
        except Exception as e:
            logging.error(f"❌ Ошибка при запросе комментария от менеджера {manager_id}: {e}")
            await callback.answer("❌ Ошибка. Не удалось запросить комментарий.", show_alert=True)

        else:
            await callback.answer("❌ Ошибка при обновлении статуса запроса.", show_alert=True)



@dp.message(ManagerApprovalStates.awaiting_reject_comment)
async def handle_manager_reject_comment(message: types.Message, state: FSMContext):
    """Обработка текстового комментария менеджера при отказе."""
    manager_id = message.from_user.id
    reject_comment = message.text.strip()

    if not reject_comment:
        await message.answer("📝 Комментарий не может быть пустым. Пожалуйста, введите причину отказа:")
        return

    # Получаем request_id из состояния менеджера
    manager_state_data = await state.get_data()
    request_id = manager_state_data.get('current_reject_request_id')

    if not request_id:
        await message.answer("❌ Ошибка состояния. Пожалуйста, начните процесс заново.")
        await state.clear() # Очищаем состояние менеджера
        return

    # --- Получение запроса из БД для проверки ---
    request_data = await get_approval_request_by_id(request_id)
    if not request_data:
        await message.answer("❌ Запрос не найден.")
        await state.clear()
        return

    # Проверка, что менеджер пытается обработать свой запрос
    if request_data['manager_id'] != manager_id:
        await message.answer("❌ Это не ваш запрос.")
        await state.clear()
        return

    # Проверка, что запрос еще не обработан
    if request_data['status'] != 'pending':
        status_text = "одобрен" if request_data['status'] == 'approved' else "отклонен"
        await message.answer(f"❌ Запрос уже {status_text}.")
        await state.clear()
        return

    # --- Отказ с комментарием ---
    # Обновляем статус запроса в БД, добавляя комментарий
    # Предполагается, что update_approval_request_status была изменена для поддержки reject_comment
    success = await update_approval_request_status(request_id, 'rejected', reject_comment=reject_comment)

    if success:
        user_id = request_data['user_id']
        article = request_data['article']
        shop = request_data['shop']
        product_name = request_data.get('product_name', 'Неизвестно') # На всякий случай
        rejecting_manager_id = request_data['manager_id']

        department_from_request = request_data.get('department', 'Неизвестный отдел')
        rejecting_manager_info = get_manager_id_by_department(department_from_request)

        if rejecting_manager_info:
            rejecting_manager_name = (
                f"{rejecting_manager_info.get('first_name', 'N/A')} "
                f"{rejecting_manager_info.get('last_name', 'N/A')}".strip()
            ) or "Неизвестный менеджер"
        else:
            # Если не удалось получить информацию (например, кэш пуст или изменился)
            rejecting_manager_name = f"Менеджер отдела {department_from_request}"
        # --- Уведомление пользователя с комментарием ---
        user_message = (
            f"❌ <b>{rejecting_manager_name} отказал в заказе артикула {article} для магазина {shop}.</b>\n"
            f"<b>Название товара:</b> {product_name}\n"
            f"<b>Причина отказа:</b> {reject_comment}"
        )
        try:
            # Отправляем уведомление пользователю
            await bot.send_message(
                chat_id=user_id,
                text=user_message,
                reply_markup=main_menu_keyboard(user_id), # Или другая клавиатура для пользователя
                parse_mode='HTML'
            )

            # Подтверждение менеджеру
            await message.answer(
                "✅ Отказ оформлен. Пользователь уведомлен с комментарием.",
                reply_markup=main_menu_keyboard(message.from_user.id) # Или основная клавиатура менеджера
            )

        except Exception as e:
            logging.error(f"❌ Не удалось отправить уведомление об отказе пользователю {user_id}: {e}")
            await message.answer(
                "✅ Отказ оформлен, но не удалось уведомить пользователя.",
                reply_markup=main_menu_keyboard(message.from_user.id)
            )
    else:
        # Ошибка при обновлении статуса в БД
        await message.answer(
            "❌ Ошибка при оформлении отказа в базе данных.",
            reply_markup=main_menu_keyboard(message.from_user.id)
        )

    # В любом случае (успех или ошибка) очищаем состояние менеджера
    await state.clear()


@dp.callback_query(F.data.startswith("continue_order:"))
async def handle_continue_order(callback: types.CallbackQuery, state: FSMContext):
    """Обработка нажатия кнопки 'Продолжить заказ' пользователем."""
    _, request_id = callback.data.split(":", 1)
    user_id = callback.from_user.id

    # --- Получение запроса из БД ---
    request_data = await get_approval_request_by_id(request_id)
    if not request_data:
        await callback.answer("❌ Запрос не найден или уже обработан.", show_alert=True)
        try:
            await callback.message.delete()
        except Exception:
            pass
        return

    if request_data['user_id'] != user_id:
        await callback.answer("❌ Это не ваш запрос.", show_alert=True)
        return

    if request_data['status'] != 'approved':
        if request_data['status'] == 'rejected':
             await callback.answer("❌ Менеджер отказал в этом заказе.", show_alert=True)
        else:
             await callback.answer("❌ Запрос еще не одобрен менеджером.", show_alert=True)
        return

    # --- Восстановление данных FSM ---
    user_data_snapshot = request_data['user_data']
    try:
        await state.set_data(user_data_snapshot)
        logging.info(f"✅ Данные FSM для пользователя {user_id} восстановлены из запроса {request_id}")

        article = user_data_snapshot.get('article', 'N/A')
        product_name = user_data_snapshot.get('product_name', 'N/A')
        department = user_data_snapshot.get('department', 'N/A')
        quantity = user_data_snapshot.get('quantity', 'N/A')

        # --- Удаление записи из БД ---
        await delete_approval_request(request_id)

        resume_message = (
                f"✅ <b>Заказ ТОП 0 одобрен менеджером.</b>\n"
                f"Продолжаем оформление заказа.\n\n"
                f"<b>Информация о товаре:</b>\n"
                f"📦 Артикул: {article}\n"
                f"🏷️ Название: {product_name}\n"
                f"🔢 Отдел: {department}\n"
                f"🔢 Кол-во: {quantity}\n\n"
                f"📝 Введите причину заказа:"
            )
        # --- Отправка уведомления пользователю ---
        await callback.answer("✅ Данные восстановлены. Продолжаем заказ...", show_alert=True)

        # --- Продолжение процесса заказа ---
        # После восстановления данных, переходим к вводу причины заказа
        # Удаляем предыдущее сообщение с кнопкой (опционально)
        try:
            await callback.message.delete()
        except Exception:
            pass

        await callback.message.answer(resume_message, parse_mode='HTML', reply_markup=cancel_keyboard())
        await state.set_state(OrderStates.order_reason_input)

    except Exception as e:
        logging.error(f"❌ Ошибка восстановления данных FSM для пользователя {user_id} из запроса {request_id}: {e}")
        await callback.answer("❌ Ошибка восстановления данных. Обратитесь к администратору.", show_alert=True)
        await delete_approval_request(request_id)
        await state.clear()



async def check_and_remind_overdue_approvals(bot_instance: Bot):
    """
    Проверяет, есть ли непросроченные запросы на одобрение ТОП 0 старше 2 дней,
    и отправляет напоминания менеджерам.
    Теперь включает список ID всех просроченных запросов для удобства поиска.
    """
    logging.info("🔍 Начало проверки просроченных запросов на одобрение ТОП 0...")
    try:
        # Вычисляем дату, раньше которой считаем запросы просроченными
        cutoff_time = datetime.now() - timedelta(days=2)

        with get_db_connection() as conn:
            cursor = conn.cursor()
            # Выбираем request_id, manager_id и другие поля, если понадобятся
            # Фильтруем по статусу и дате создания
            # Сортировка по manager_id позволяет группировать в цикле
            cursor.execute("""
                SELECT request_id, manager_id, user_id, article, created_at
                FROM approval_requests
                WHERE status = 'pending' AND created_at < ?
                ORDER BY manager_id, created_at
            """, (cutoff_time.isoformat(),))
            overdue_requests = cursor.fetchall()

        if not overdue_requests:
            logging.info("📭 Нет просроченных запросов на одобрение.")
            return

        # Группируем запросы по manager_id
        manager_requests = {}
        for req in overdue_requests:
            # req - это sqlite3.Row, можно обращаться по имени столбца
            manager_id = req['manager_id']
            if manager_id not in manager_requests:
                manager_requests[manager_id] = []
            # Добавляем всю информацию о запросе, если понадобится в сообщении
            manager_requests[manager_id].append({
                'request_id': req['request_id'],
                'user_id': req['user_id'],
                'article': req['article'],
                'created_at': req['created_at'],
            })

        notified_count = 0
        for manager_id, requests in manager_requests.items():
            try:
                # Формируем сообщение
                count = len(requests)
                
                # --- Формируем список ID запросов ---
                # Соединяем ID в одну строку, разделённую запятыми
                # Это делает их легко копируемыми и идентифицируемыми в чате
                request_id_list = ', '.join([req['request_id'] for req in requests])

                # Новый вариант: список всех ID
                reminder_text = (
                    f"⏰ <b>Напоминание о запросах на одобрение ТОП 0</b>\n\n"
                    f"У вас <b>{count}</b> просроченных запросов старше 2 дней.\n\n"
                    f"<b>Список ID запросов:</b>\n<code>{request_id_list}</code>\n\n"
                    f"Пожалуйста, проверьте и обработайте их в боте."
                    
                )

                # Отправляем сообщение менеджеру
                await bot_instance.send_message(chat_id=manager_id, text=reminder_text, parse_mode='HTML')
                notified_count += 1
                logging.info(f"🔔 Напоминание отправлено менеджеру {manager_id} о {count} запросах (IDs: {request_id_list}).")
                # Делаем небольшую паузу, чтобы не спамить Telegram API
                await asyncio.sleep(0.1)
            except Exception as e:
                logging.error(f"❌ Не удалось отправить напоминание менеджеру {manager_id}: {e}")
                # Здесь можно добавить логику повторной отправки или уведомления админов

        logging.info(f"✅ Отправлено {notified_count} напоминаний менеджерам о просроченных запросах.")

    except Exception as e:
        logging.error(f"❌ Ошибка в задаче check_and_remind_overdue_approvals: {e}")



async def run_reminder_task(bot_instance: Bot):
    """Циклический запуск задачи проверки просроченных запросов."""
    global reminder_running
    logging.info("🚀 Запущена задача напоминаний о просроченных запросах ТОП 0.")
    while reminder_running:
        try:
            # Запускаем проверку
            await check_and_remind_overdue_approvals(bot_instance)
            # Ждем 24 часа перед следующей проверкой
            logging.info("⏰ Задача напоминаний ожидает 24 часа...")
            await asyncio.sleep(86400) # 86400 секунд = 24 часа
        except asyncio.CancelledError:
            logging.info("🛑 Задача напоминаний о просроченных запросах отменена.")
            break
        except Exception as e:
            logging.error(f"🔥 Критическая ошибка в задаче напоминаний: {e}", exc_info=True)
            # Ждем перед перезапуском цикла, чтобы не уйти в бесконечный крэш
            await asyncio.sleep(300) # 5 минут


# =============================ОЧЕРЕДЬ ЗАКАЗОВ=================================

def initialize_order_queue_table():
    """Создает таблицу для очереди заказов."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # Создаем таблицу очереди заказов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS order_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    order_data TEXT NOT NULL, -- JSON-строка с данными заказа
                    status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed'
                    attempt_count INTEGER DEFAULT 0, -- Количество попыток обработки
                    last_attempt TIMESTAMP, -- Дата/время последней попытки
                    error_message TEXT, -- Сообщение об ошибке последней попытки
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP NULL -- Дата/время успешной обработки
                )
            ''')
            
            # Создаем индекс для быстрого поиска ожидающих заказов
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_order_queue_status ON order_queue(status)
            ''')
            
            conn.commit()
            logging.info("✅ Таблица order_queue инициализирована")
    except Exception as e:
        logging.error(f"❌ Ошибка инициализации таблицы order_queue: {e}")


# --- Функция для добавления заказа в очередь ---
async def add_order_to_queue(user_id: int, order_data: dict) -> bool:
    """Добавляет заказ в очередь на обработку."""
    try:
        
        serialized_data = json.dumps(order_data, ensure_ascii=False, default=str)
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO order_queue (user_id, order_data, status)
                VALUES (?, ?, 'pending')
            ''', (user_id, serialized_data))
            conn.commit()
            logging.info(f"✅ Заказ для пользователя {user_id} добавлен в очередь (ID записи: {cursor.lastrowid})")
            return True
    except Exception as e:
        logging.error(f"❌ Ошибка добавления заказа в очередь для пользователя {user_id}: {e}")
        return False


# --- Функция для получения заказов из очереди ---
def get_pending_orders(limit: int = 10) -> list:
    """Получает список заказов, ожидающих обработки."""
    MAX_ATTEMPTS = 5
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, user_id, order_data, attempt_count
                FROM order_queue 
                WHERE (status = 'pending' OR (status = 'failed' AND attempt_count < ?))
                ORDER BY created_at ASC
                LIMIT ?
            ''', (MAX_ATTEMPTS, limit))
            rows = cursor.fetchall()
            orders = []
            for row in rows:
                try:
                    order_data = json.loads(row['order_data']) if isinstance(row['order_data'], str) else row['order_data']
                    orders.append({
                        'id': row['id'],
                        'user_id': row['user_id'],
                        'order_data': order_data,
                        'attempt_count': row['attempt_count']
                    })
                except json.JSONDecodeError as je:
                    logging.error(f"❌ Ошибка десериализации order_data для записи {row['id']}: {je}")
                    # Можно пометить заказ как 'failed' здесь, если нужно
            return orders
    except Exception as e:
        logging.error(f"❌ Ошибка получения заказов из очереди: {e}")
        return []


# --- Функция для обновления статуса заказа ---
def update_order_status(order_id: int, status: str, error_message: str = None) -> bool:
    """Обновляет статус заказа в очереди."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            if status == 'completed':
                cursor.execute('''
                    UPDATE order_queue 
                    SET status = ?, processed_at = CURRENT_TIMESTAMP, last_attempt = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (status, order_id))
            elif status == 'failed':
                cursor.execute('''
                    UPDATE order_queue 
                    SET status = ?, error_message = ?, attempt_count = attempt_count + 1, last_attempt = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (status, error_message, order_id))
            else: # pending или processing
                cursor.execute('''
                    UPDATE order_queue 
                    SET status = ?, last_attempt = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (status, order_id))
            conn.commit()
            if cursor.rowcount > 0:
                logging.info(f"✅ Статус заказа {order_id} обновлен на '{status}'")
                return True
            else:
                logging.warning(f"⚠️ Заказ {order_id} не найден для обновления статуса")
                return False
    except Exception as e:
        logging.error(f"❌ Ошибка обновления статуса заказа {order_id}: {e}")
        return False

# --- Функция для получения количества заказов по статусам (для мониторинга) ---
def get_order_queue_stats() -> dict:
    """Получает статистику по очереди заказов."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT status, COUNT(*) as count
                FROM order_queue
                GROUP BY status
            ''')
            rows = cursor.fetchall()
            stats = {row['status']: row['count'] for row in rows}
            # Добавляем нули для отсутствующих статусов
            for status in ['pending', 'processing', 'completed', 'failed']:
                stats.setdefault(status, 0)
            return stats
    except Exception as e:
        logging.error(f"❌ Ошибка получения статистики очереди заказов: {e}")
        return {'pending': 0, 'processing': 0, 'completed': 0, 'failed': 0}


async def process_order_queue(bot_instance):
    """Фоновый обработчик очереди заказов."""
    logging.info("🚀 Запущен обработчик очереди заказов (воркер)")
    
    while worker_running:
        try:
            logging.info("🔄 process_order_queue: Начало цикла обработки очереди.")
            # --- Получаем заказы из очереди ---
            pending_orders = get_pending_orders(limit=5) # Обрабатываем до 5 заказов за раз
            
            logging.info(f"📥 process_order_queue: Получено {len(pending_orders)} заказов из get_pending_orders.")
            
            if not pending_orders:
                logging.info("📭 process_order_queue: Очередь пуста или нет подходящих заказов. Сплю 120 секунд.")
                # Если заказов нет, немного ждем
                await asyncio.sleep(120) # Пауза 2 минуты (в вашем коде 120)
                continue
            
            logging.info(f"📥 Найдено {len(pending_orders)} заказов для обработки.")
            
            # --- Обрабатываем каждый заказ по очереди ---
            for order_item in pending_orders:
                order_id = order_item['id']
                user_id = order_item['user_id']
                order_data = order_item['order_data']
                attempt_count = order_item['attempt_count']
                max_retries = 5
                
                # Помечаем заказ как "в обработке"
                update_order_status(order_id, 'processing')
                logging.info(f"⚙️ Начало обработки заказа ID {order_id} (попытка {attempt_count + 1}) для пользователя {user_id}...")
                
                try:
                    # --- ОСНОВНАЯ ОПЕРАЦИЯ ЗАПИСИ В GOOGLE SHEETS ---
                    department_sheet = orders_spreadsheet.worksheet(order_data['department'])
                    next_row = len(department_sheet.col_values(1)) + 1

                    updates = [
                        {'range': f'A{next_row}', 'values': [[order_data['selected_shop']]]},
                        {'range': f'B{next_row}', 'values': [[int(order_data['article'])]]},
                        {'range': f'C{next_row}', 'values': [[order_data['order_reason']]]},
                        {'range': f'D{next_row}', 'values': [[datetime.now().strftime("%d.%m.%Y %H:%M")]]},
                        {'range': f'E{next_row}', 'values': [[f"{order_data['user_name']}, {order_data['user_position']}"]]},
                        {'range': f'K{next_row}', 'values': [[int(order_data['quantity'])]]},
                        {'range': f'R{next_row}', 'values': [[user_id]]}
                    ]
                    department_sheet.batch_update(updates)
                    
                    
                   
                    update_order_status(order_id, 'completed')
                    logging.info(f"✅ Заказ ID {order_id} для пользователя {user_id} успешно записан в таблицу {order_data['department']} строка {next_row}")
                    
                    # (Опционально) Уведомляем пользователя об успехе
                    # try:
                    #     await bot_instance.send_message(user_id, "✅ Ваш заказ успешно записан в таблицу!")
                    # except Exception as user_notify_err:
                    #     logging.warning(f"Не удалось уведомить пользователя {user_id} об успешной записи заказа: {user_notify_err}")
                    
                except Exception as e:
                    # --- ОШИБКА ---
                    error_msg = str(e)
                    logging.error(f"❌ Ошибка при записи заказа ID {order_id} для пользователя {user_id}: {error_msg}", exc_info=True)
                    
                    # Обновляем статус на 'failed' и сохраняем ошибку
                    update_order_status(order_id, 'failed', error_message=error_msg)
                    
                    # Если это была последняя попытка, уведомляем админа
                    if attempt_count + 1 >= max_retries:
                        for admin_id in ADMINS:
                            try:
                                admin_msg = (
                                    f"🚨 Окончательная ошибка записи заказа из очереди!\n"
                                    f"• ID записи в БД: <code>{order_id}</code>\n"
                                    f"• Пользователь: <code>{user_id}</code>\n"
                                    f"• Артикул: <code>{order_data.get('article', 'N/A')}</code>\n"
                                    f"• Магазин: <code>{order_data.get('selected_shop', 'N/A')}</code>\n"
                                    f"• Ошибка: <pre>{error_msg[:300]}</pre>"
                                )
                                await bot_instance.send_message(admin_id, admin_msg, parse_mode='HTML')
                            except Exception as admin_notify_err:
                                logging.error(f"Не удалось уведомить админа {admin_id}: {admin_notify_err}")
                                
            # Небольшая пауза между циклами обработки
            await asyncio.sleep(1)
            
        except asyncio.CancelledError:
            logging.info("🛑 Обработчик очереди заказов отменен.")
            break
        except Exception as worker_error:
            logging.critical(f"🔥 Критическая ошибка в обработчике очереди заказов: {worker_error}", exc_info=True)
            # Можно отправить уведомление админам о краше воркера
            for admin_id in ADMINS:
                try:
                    await bot_instance.send_message(admin_id, f"🔥 Критическая ошибка в обработчике очереди заказов: {worker_error}")
                except:
                    pass
            # Ждем перед повторным запуском цикла
            await asyncio.sleep(10)




# =============================ПАРСЕР=================================
  
def parse_supplier_data(record: dict) -> Dict[str, Any]:
    """Парсинг данных поставщика"""
    order_days = []
    for key in ['День выхода заказа', 'День выхода заказа 2', 'День выхода заказа 3']:
        value = str(record.get(key, '')).strip()
        if value and value.isdigit():
            order_days.append(int(value))
    
    delivery_days = str(record.get('Срок доставки в магазин', '0')).strip()

    # --- Парсим каникулы ---
    holidays_str = str(record.get('Каникулы список', '')).strip()
    holidays = set()
    if holidays_str:
        for date_str in holidays_str.split(','):
            date_str = date_str.strip()
            if date_str:
                try:
                    date_obj = datetime.strptime(date_str, "%d.%m.%Y").date()
                    holidays.add(date_obj)
                except ValueError:
                    logging.warning(f"⚠️ Некорректный формат даты каникул: {date_str}")

    # --- Парсим исключения (заказы внутри каникул) ---
    exceptions_str = str(record.get('Исключения список', '')).strip()  # <-- НОВОЕ ПОЛЕ
    exceptions = set()
    if exceptions_str:
        for date_str in exceptions_str.split(','):
            date_str = date_str.strip()
            if date_str:
                try:
                    date_obj = datetime.strptime(date_str, "%d.%m.%Y").date()
                    exceptions.add(date_obj)
                except ValueError:
                    logging.warning(f"⚠️ Некорректный формат даты исключения: {date_str}")

    return {
        'supplier_id': str(record.get('Номер осн. пост.', '')),
        'order_days': sorted(list(set(order_days))),
        'delivery_days': int(delivery_days) if delivery_days.isdigit() else 0,
        'holidays': holidays,
        'exceptions': exceptions  # <-- Добавляем исключения
    }


def calculate_delivery_date(supplier_data: dict) -> Tuple[str, str]:
    today = datetime.now().date()
    current_weekday = today.isoweekday()

    order_days = supplier_data['order_days']
    holidays = supplier_data.get('holidays', set())
    exceptions = supplier_data.get('exceptions', set())

    # --- 1. Найти ближайший день заказа ---
    candidate_date = today
    while True:
        # Проверяем, является ли день заказа по графику
        candidate_weekday = candidate_date.isoweekday()
        is_scheduled_order_day = candidate_weekday in order_days

        # Проверяем, является ли день исключением
        is_exception = candidate_date in exceptions

        # Если это исключение — можно заказать, даже если не день заказа
        if is_exception:
            order_date = candidate_date
            break

        # Если это день заказа и не каникулы — можно заказать
        if is_scheduled_order_day and candidate_date not in holidays:
            order_date = candidate_date
            break

        # Иначе — идём дальше
        candidate_date += timedelta(days=1)

    # --- 2. Рассчитать дату поставки ---
    delivery_date = order_date
    days_added = 0
    while days_added < supplier_data['delivery_days']:
        next_day = delivery_date + timedelta(days=1)
        # Если следующий день — каникулы, но не исключение — пропускаем
        if next_day in holidays and next_day not in exceptions:
            delivery_date = next_day
            continue
        # Иначе — засчитываем день
        delivery_date = next_day
        days_added += 1

    return (
        order_date.strftime("%d.%m.%Y"),
        delivery_date.strftime("%d.%m.%Y")
    )



async def get_product_info(article: str, shop: str) -> Optional[Dict[str, Any]]:
    """Получение информации о товаре с расширенным логированием, используя SQLite"""
    try:
        stack = inspect.stack()
        # Обычно нас интересует вызывающая функция, которая находится под текущей (stack[1])
        # stack[0] - это текущая функция get_product_info
        caller_frame = stack[1] 
        caller_info = f"{caller_frame.filename}:{caller_frame.lineno} in {caller_frame.function}"
        logging.info(f"🔍 [TRACE] get_product_info вызвана из: {caller_info}")
        
        for i, frame in enumerate(stack[1:4]): # Показать 3 уровня вызова
             logging.info(f"  {i+1}. {frame.filename}:{frame.lineno} in {frame.function}")
        
        logging.info(f"🔍 Поиск товара: артикул={article}, магазин={shop}")
        
        # === 1. Получение данных товара из SQLite ===
        product_data = await get_product_data_from_db(article, shop)
        
        if not product_data:
            logging.warning(f"Товар не найден в БД: артикул={article}, магазин={shop}")
            return None
            
        logging.info(f"Найден товар: {product_data.get('Название', 'Неизвестно')}")

        # === 2. Получение данных поставщика из SQLite ===
        supplier_id = str(product_data.get("Номер осн. пост.", "")).strip()
        logging.info(f"ID поставщика: {supplier_id}")
        
        if not supplier_id:
             # Если поставщик не указан, возвращаем базовую информацию
             logging.info("Номер поставщика отсутствует, возвращаю базовую информацию")
             return {
                 'Артикул': article,
                 'Название': product_data.get('Название', ''),
                 'Отдел': str(product_data.get('Отдел', '')),
                 'Магазин': shop,
                 'Поставщик': 'Товар РЦ', # Или другое значение по умолчанию
                 'Топ в магазине': product_data.get('Топ в магазине', '0'),
                 'Дата заказа': 'Не определена (поставщик не найден)',      
                 'Дата поставки': 'Не определена (поставщик не найден)',    
             }

        supplier_data = await get_supplier_data_from_db(supplier_id, shop)
        
        # === 3. Обработка случая, если поставщик не найден ===
        if not supplier_data:
            logging.info("Поставщик не найден в БД, используется резервная информация")
            return {
                'Артикул': article,
                'Название': product_data.get('Название', ''),
                'Отдел': str(product_data.get('Отдел', '')),
                'Магазин': shop,
                'Поставщик': 'Товар РЦ', # Или product_data.get('Название осн. пост.', 'Не указано').strip()
                'Топ в магазине': product_data.get('Топ в магазине', '0'),
                'Дата заказа': 'Не определена (поставщик не найден)',      
                'Дата поставки': 'Не определена (поставщик не найден)',
            }

        # === 4. Парсинг данных поставщика и расчет дат ===
        # Парсинг данных поставщика (используем существующую функцию)
        parsed_supplier = parse_supplier_data(supplier_data)
        order_date, delivery_date = calculate_delivery_date(parsed_supplier)
        holidays = parsed_supplier.get('holidays', set())
        exceptions = parsed_supplier.get('exceptions', set())
        
        # === 5. Формирование итогового результата ===
        result = {
            'Артикул': article,
            'Название': product_data.get('Название', ''),
            'Отдел': str(product_data.get('Отдел', '')),
            'Магазин': shop,
            'Поставщик': supplier_data.get("Название осн. пост.", "Не указано").strip(),
            'Дата заказа': order_date,
            'Дата поставки': delivery_date,
            'Номер поставщика': supplier_id,
            'Топ в магазине': product_data.get('Топ в магазине', '0'),
            # --информация о каникулах ---
            'Каникулы': list(holidays) if holidays else None,
            'Исключения': list(exceptions) if exceptions else None,   
        }
        
        logging.info(f"Успешно получена информация: {result}")
        return result
        
    except Exception as e:
        logging.exception(f"Критическая ошибка в get_product_info: {str(e)}")
        return None




async def preload_cache() -> None:
    try:
        
        users_records = users_sheet.get_all_records()
        # --- ИСПРАВЛЕНО: Храним объект ---
        cache["users_data"] = users_records 
        
        managers_sheet = main_spreadsheet.worksheet(MANAGERS_SHEET_NAME)
        managers_records = managers_sheet.get_all_records()
        # --- ИСПРАВЛЕНО: Храним объект ---
        cache["managers_data"] = managers_records
        
        cache_size_users = len(str(users_records)) / 1024 / 1024 # Примерная оценка
        logging.info(f"✅ Кэш пользователей загружен. Размер ~ {cache_size_users:.2f} MB")
        logging.info("✅ Кэш успешно загружен (без pickle)")
    except Exception as e:
        logging.error(f"Ошибка загрузки кэша: {str(e)}")
        

# === Заменить полностью функцию get_product_data_from_db ===
async def get_product_data_from_db(article: str, shop: str) -> Optional[Dict[str, Any]]:
    """
    Получение данных о товаре из SQLite по составному ключу (full_key).

    Args:
        article (str): Артикул товара.
        shop (str): Номер магазина.

    Returns:
        Optional[Dict[str, Any]]: Словарь с данными товара или None, если не найден.
    """
    try:
        # Формируем составной ключ для точного поиска
        full_key_exact = f"{article}{shop}"
        logging.info(f"🔍 Поиск по full_key: '{full_key_exact}'")

        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 1. Поиск с точным совпадением по full_key
            cursor.execute("""
                SELECT full_key, store_number, department, article_code, name, gamma, 
                       supplier_code, supplier_name, is_top_store
                FROM articles 
                WHERE full_key = ?
            """, (full_key_exact,))
            
            row = cursor.fetchone()
            
            if row:
                # Преобразуем sqlite3.Row в словарь
                logging.info(f"✅ Найден товар по точному full_key '{full_key_exact}': {row['name']}")
                # Отображаем имена столбцов из БД в имена, ожидаемые get_product_info
                return {
                    "Магазин": row['store_number'],
                    "Отдел": row['department'],
                    "Артикул": row['article_code'],
                    "Название": row['name'],
                    "Гамма": row['gamma'],
                    "Номер осн. пост.": row['supplier_code'],
                    "Название осн. пост.": row['supplier_name'],
                    "Топ в магазине": str(row['is_top_store'])
                }
            
            # 2. Если не найден по точному ключу, ищем по артикулу в начале full_key
            # Формируем шаблон поиска: full_key начинается с артикула
            article_prefix = f"{article}%"
            logging.info(f"Товар с full_key '{full_key_exact}' не найден, ищу по артикулу '{article}' в начале full_key...")
            
            cursor.execute("""
                SELECT full_key, store_number, department, article_code, name, gamma, 
                       supplier_code, supplier_name, is_top_store
                FROM articles 
                WHERE full_key LIKE ?
                ORDER BY full_key -- Сортируем для получения какого-либо результата
                LIMIT 1
            """, (article_prefix,))
            
            row = cursor.fetchone()
            
            if row:
                found_key = row['full_key']
                found_shop = row['store_number']
                logging.info(f"✅ Найден товар по артикулу в full_key: full_key='{found_key}', магазин={found_shop}, название={row['name']}")
                # Отображаем имена столбцов из БД в имена, ожидаемые get_product_info
                return {
                    "Магазин": row['store_number'],
                    "Отдел": row['department'],
                    "Артикул": row['article_code'],
                    "Название": row['name'],
                    "Гамма": row['gamma'],
                    "Номер осн. пост.": row['supplier_code'],
                    "Название осн. пост.": row['supplier_name'],
                    "Топ в магазине": str(row['is_top_store'])
                }
            else:
                logging.warning(f"❌ Товар с артикулом '{article}' не найден даже по артикулу в full_key")
                
            return None
            
    except sqlite3.Error as e:
        logging.error(f"Ошибка запроса к БД (get_product_data_from_db): {e}")
        return None
    except Exception as e:
        logging.error(f"Неожиданная ошибка в get_product_data_from_db: {e}")
        return None


async def get_supplier_data_from_db(supplier_id: str, shop: str) -> Optional[Dict[str, Any]]:
    """
    Получение данных о поставщике и сроках поставки из SQLite.

    Args:
        supplier_id (str): Номер основного поставщика.
        shop (str): Номер магазина (для выбора правильной таблицы).

    Returns:
        Optional[Dict[str, Any]]: Словарь с данными поставщика или None, если не найден.
    """
    # Нормализуем supplier_id
    supplier_id = str(supplier_id).strip()
    if not supplier_id:
        return None

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # Формируем имя таблицы поставщиков (как в Google Sheets)
            supplier_table_name = f"Даты выходов заказов {shop}"

            # Определяем части SQL-запроса
            select_clause = '''
                SELECT "Номер осн. пост.", "Название осн. пост.", "Срок доставки в магазин",
                       "День выхода заказа", "День выхода заказа 2", "День выхода заказа 3",
                       "Каникулы список", "Исключения список"
            '''
            from_clause = f'FROM "{supplier_table_name}"'
            where_clause = 'WHERE "Номер осн. пост." = ?'

            # Собираем полный запрос
            query = f"{select_clause} {from_clause} {where_clause}"
            
            cursor.execute(query, (supplier_id,))
            
            row = cursor.fetchone()
            
            if row:
                # Преобразуем sqlite3.Row в словарь
                return dict(row)
            else:
                logging.info(f"Поставщик {supplier_id} не найден в таблице '{supplier_table_name}'")
                return None
                
    except sqlite3.Error as e:
        logging.error(f"Ошибка запроса к БД (get_supplier_data_from_db): {e}")
        return None
    except Exception as e:
        logging.error(f"Неожиданная ошибка в get_supplier_data_from_db: {e}")
        return None


# ===================== MIDDLEWARES =====================
@dp.update.middleware()
async def service_mode_middleware(handler, event, data):
    """Проверка сервисного режима"""
    if SERVICE_MODE and (hasattr(event, 'message') or hasattr(event, 'callback_query')):
        user_id = (
            event.message.from_user.id 
            if hasattr(event, 'message') and event.message 
            else (event.callback_query.from_user.id if hasattr(event, 'callback_query') and event.callback_query else None)
        )
        if user_id and user_id not in ADMINS:
            msg = "⏳ Бот в режиме обслуживания. Попробуйте позже."
            
            # --- ОТВЕЧАЕМ НА CALLBACK_QUERY СРАЗУ, ЕСЛИ ОН ЕСТЬ ---
            if hasattr(event, 'callback_query') and event.callback_query:
                try:
                    # Отвечаем на запрос, чтобы не превысить таймаут
                    await event.callback_query.answer(msg, show_alert=True)
                    # logging.info(f"Отправлен ответ на callback_query для user_id {user_id} в сервисном режиме.")
                except Exception as e:
                    # Если ответить не удалось (например, из-за таймаута, который уже наступил из-за другого middleware),
                    # просто логируем и не прерываем цепочку (handler не вызываем).
                    logging.warning(f"Не удалось ответить на callback_query в service_mode_middleware: {e}")
                    return # Важно: не вызываем handler, так как пользователю уже отправлено сообщение.
            # ----------------------------------------------------
            
            # --- ОТПРАВЛЯЕМ СООБЩЕНИЕ (ЭТО МОЖЕТ БЫТЬ МЕДЛЕННО, НО callback_query УЖЕ ОТВЕЧЕН) ---
            if hasattr(event, 'message') and event.message:
                try:
                    await event.message.answer(msg)
                    # logging.info(f"Отправлено сообщение о сервисном режиме user_id {user_id}.")
                except Exception as e:
                    logging.warning(f"Не удалось отправить сообщение о сервисном режиме: {e}")
            # ----------------------------------------------------
            
            # Не вызываем следующий middleware/handler
            return 
    # Если сервисный режим выключен или пользователь - админ, продолжаем
    return await handler(event, data)

@dp.update.middleware()
async def activity_tracker_middleware(handler, event, data):
   
    state = data.get('state')

    if state:
        try:
            
            response = await handler(event, data)

            
            current_state_data = await state.get_data()
            # Обновляем метку времени
            current_state_data['last_activity'] = datetime.now().isoformat()
            # Сохраняем обновлённые данные обратно в состояние
            await state.set_data(current_state_data)

            # Возвращаем ответ, который дал handler
            return response

        except Exception as e:
         
            logging.error(f"Ошибка в activity_tracker_middleware: {type(e).__name__}: {e}")
            
            raise 

    else:
        
        return await handler(event, data)


# ===================== АВТОМАТИЧЕСКАЯ ОЧИСТКА СОСТОЯНИЙ =====================
async def state_cleanup_task():
    """Фоновая задача для очистки устаревших состояний с логированием"""
    while True:
        try:
            now = datetime.now()
            cleared_count = 0
            
            if hasattr(dp.storage, 'storage'):
                states = dp.storage.storage
                logging.info(f"Проверка состояний: {len(states)} активных сессий")
                
                for key, state_record in list(states.items()):
                    if not hasattr(state_record, 'data') or not isinstance(state_record.data, dict):
                        continue
                    
                    data = state_record.data
                    last_activity_str = data.get('last_activity')
                    
                    if not last_activity_str:
                        continue
                    
                    try:
                        last_activity = datetime.fromisoformat(last_activity_str)
                        inactivity = (now - last_activity).total_seconds() / 60
                        
                        if inactivity > 30:
                            user_id = key.user_id
                            logging.info(f"Очистка состояния: пользователь {user_id}, неактивен {inactivity:.1f} мин")
                            await dp.storage.set_state(key=key, state=None)
                            await dp.storage.set_data(key=key, data={})
                            del states[key]
                            try:
                                await bot.send_message(
                                    user_id,
                                    "🕒 Сессия была автоматически завершена из-за неактивности.",
                                    reply_markup=main_menu_keyboard(user_id) # Отправляем главную клавиатуру
                                    )
                                
                                logging.info(f"Пользователю {user_id} отправлено сообщение о сбросе сессии.")
                            
                            except Exception as e:
        # TelegramForbiddenError (пользователь заблокировал бота), 
        # TelegramRetryAfter, и т.д.
                                logging.warning(f"Не удалось отправить сообщение о сбросе сессии пользователю {user_id}: {e}")    
                        cleared_count += 1
                            
                    except (TypeError, ValueError) as e:
                        logging.error(f"Ошибка формата времени: {str(e)}")
                
                if cleared_count > 0:
                    logging.info(f"Автоочистка: очищено {cleared_count} состояний")
            
            await asyncio.sleep(900)
                
        except Exception as e:
            logging.exception(f"Ошибка в задаче очистки состояний: {str(e)}")
            await asyncio.sleep(300)


# ===================== ОБРАБОТЧИКИ КОМАНД =====================
@dp.message(F.text == "/checkip")
async def check_ip(message: Message):
    try:
        # Используем aiohttp для запроса через тот же прокси
        connector = ProxyConnector.from_url(PROXY_URL)
        async with aiohttp.ClientSession(connector=connector) as sess:
            async with sess.get("https://httpbin.org/ip", timeout=10) as response:
                data = await response.json()
                await message.answer(f"🌍 Мой IP: {data['origin']}")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(Command("start"))
async def start_handler(message: types.Message, state: FSMContext):
    """Обработчик команды /start"""
    # Инициализируем активность в состоянии
    await state.update_data(last_activity=datetime.now().isoformat())
    
    user_data = await get_user_data(str(message.from_user.id))
    if user_data:
        await message.answer("ℹ️ Вы в главном меню:", 
                            reply_markup=main_menu_keyboard(message.from_user.id))
        return
    
    await message.answer("👋 Добро пожаловать! Введите ваше имя:", 
                        reply_markup=types.ReplyKeyboardRemove())
    await log_user_activity(message.from_user.id, "/start", "registration")
    await state.set_state(Registration.name)

# Регистрация пользователя
@dp.message(Registration.name)
async def process_name(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat(), name=message.text.strip())
    await message.answer("📝 Введите вашу фамилию:")
    await state.set_state(Registration.surname)

@dp.message(Registration.surname)
async def process_surname(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat(), surname=message.text.strip())
    await message.answer("💼 Введите вашу должность:")
    await state.set_state(Registration.position)

@dp.message(Registration.position)
async def process_position(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat(), position=message.text.strip())
    await message.answer("🏪 Введите номер магазина (только цифры, без нулей):")
    await state.set_state(Registration.shop)


@dp.message(Registration.shop)
async def process_shop(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    shop_raw = message.text.strip()

    if not shop_raw.isdigit():
        await message.answer("❌ Номер магазина должен быть числом! Повторите ввод:")
        return
        
    shop = str(int(shop_raw))
    
    data = await state.get_data()

    # --- Проверяем наличие обязательных полей ---
    required_fields = ['name', 'surname', 'position']
    missing_fields = [field for field in required_fields if field not in data or not data[field]]

    if missing_fields:
        logging.error(f"Пользователь {message.from_user.id} не прошёл регистрацию: отсутствуют поля: {missing_fields}")
        await message.answer(
            "❌ Произошла ошибка при регистрации: отсутствуют необходимые данные. Попробуйте начать регистрацию заново (/start).",
            reply_markup=main_menu_keyboard(message.from_user.id)
        )
        await state.clear() # Убедимся, что состояние очищено
        return

    # --- Если всё ок, продолжаем ---
    users_sheet.append_row([
        str(message.from_user.id),
        data['name'],
        data['surname'],
        data['position'],
        shop,
        datetime.now().strftime("%d.%m.%Y %H:%M")
    ])
    cache.pop(f"user_{message.from_user.id}", None)  # Сброс кэша пользователя

    try:
        # Перезагружаем только кэш пользователей
        users_records = users_sheet.get_all_records()
        cache["users_data"] = users_records
        logging.info(f"✅ Кэш пользователей обновлен после регистрации пользователя {message.from_user.id}")
    except Exception as e:
        logging.error(f"Ошибка обновления кэша пользователей после регистрации {message.from_user.id}: {e}")

    await message.answer("✅ Регистрация завершена!",
                         reply_markup=main_menu_keyboard(message.from_user.id))
    await state.clear()


# Навигация
@dp.message(F.text.in_(["↩️ Назад", "🔙 Главное меню"]))
async def handle_back(message: types.Message, state: FSMContext):
    """Обработчик возврата в меню"""
    await state.clear()
    await message.answer("🔙 Возврат в главное меню", 
                        reply_markup=main_menu_keyboard(message.from_user.id))

@dp.message(F.text.casefold() == "отмена")
@dp.message(F.text == "❌ Отмена")
async def cancel_handler(message: types.Message, state: FSMContext):
    """Универсальный обработчик отмены с обновлением активности"""
    # Обновляем активность перед обработкой
    await state.update_data(last_activity=datetime.now().isoformat())
    
    current_state = await state.get_state()
    if current_state:
        await state.clear()
        await message.answer("🔄 Операция отменена", 
                            reply_markup=main_menu_keyboard(message.from_user.id))


@dp.message(F.text == "📞 Обратная связь")
async def handle_feedback_start(message: types.Message, state: FSMContext):
    """Начало процесса обратной связи."""
    await message.answer(
        "📝 Пожалуйста, введите ваше сообщение, пожелание или предложение. "
        "Это будет отправлено анонимно администраторам.",
        reply_markup=cancel_keyboard() # Используем клавиатуру с кнопкой "Отмена"
    )
    await state.set_state(FeedbackStates.waiting_for_feedback)

@dp.message(FeedbackStates.waiting_for_feedback, F.text)
async def handle_feedback_message(message: types.Message, state: FSMContext):
    """Получение и отправка сообщения обратной связи администраторам."""
    feedback_text = message.text
    
    
    user_id = message.from_user.id
    user_first_name = message.from_user.first_name or "N/A" # На всякий случай, если first_name None
    user_last_name = message.from_user.last_name or ""
    user_full_name = f"{user_first_name} {user_last_name}".strip() or "N/A"
    

    # --- Формируем текст уведомления администраторам ---
    # Теперь включает ID и ФИО
    admin_notification = (
        f"📢 <b>Новое сообщение обратной связи</b>\n"
        f"👤 <b>От:</b> {user_full_name} (ID: {user_id})\n" 
        f"💬 <b>Сообщение:</b>\n{feedback_text}"
    )
    

    for admin_id in ADMINS:
        try:
            await bot.send_message(chat_id=admin_id, text=admin_notification, parse_mode='HTML')
            # --- Изменяем логирование ---
            logging.info(f"Сообщение обратной связи от {user_full_name} (ID: {user_id}) отправлено администратору {admin_id}")
        except Exception as e:
            logging.error(f"Не удалось отправить сообщение обратной связи от {user_full_name} (ID: {user_id}) администратору {admin_id}: {e}")


    # Уведомляем пользователя об успешной отправке
    await message.answer(
        "✅ Спасибо за ваше сообщение! Оно анонимно отправлено администраторам.",
        reply_markup=main_menu_keyboard(message.from_user.id) # Возвращаем к главному меню
    )
    await state.clear() # Очищаем состояние


@dp.message(FeedbackStates.waiting_for_feedback, F.text == "❌ Отмена")
async def cancel_feedback(message: types.Message, state: FSMContext):
    """Отмена отправки обратной связи."""
    await message.answer(
        "❌ Отправка сообщения отменена.",
        reply_markup=main_menu_keyboard(message.from_user.id) # Возвращаем к главному меню
    )
    await state.clear()


@dp.message(Command(commands=['upload_ratings'])) # Импортируйте Command из aiogram
async def cmd_upload_ratings_start(message: types.Message):
    """Обработчик команды /upload_ratings. Запрашивает файл."""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав для выполнения этой команды.")
        return

    await message.answer("📤 Пожалуйста, отправьте CSV-файл с данными для обновления рейтингов.")
    # Здесь можно использовать FSM, чтобы бот "знал", что ожидает файл
    # Но для простоты пока просто ожидаем следующее сообщение с файлом


@dp.message(F.document) # Обработчик любого документа
async def handle_csv_document(message: types.Message):
    """Обработчик полученного документа. Проверяет, является ли он CSV и вызывает модуль."""
    # Проверяем, является ли отправитель администратором
    if message.from_user.id not in ADMINS:
        # await message.answer("❌ У вас нет прав для загрузки файлов.")
        # Просто игнорируем, чтобы не спамить
        return

    # Проверяем, является ли документ CSV
    document = message.document
    file_extension = Path(document.file_name).suffix.lower()

    if file_extension != '.csv':
         await message.answer("❌ Неверный формат файла. Ожидается файл .csv")
         return

    # Скачиваем файл
    # Создаём уникальное имя файла, чтобы избежать конфликта
    import tempfile
    
    # Лучше использовать временную директорию или директорю, доступную боту
    # и убедиться, что у бота есть права на запись/чтение/удаление
    temp_dir = "/tmp" # Пример. Используйте директорию, доступную вашему боту.
    # Или, например, os.path.join(os.path.dirname(__file__), 'temp_uploads')
    temp_csv_path = os.path.join(temp_dir, f"temp_ratings_{message.from_user.id}_{document.file_id}.csv")

    try:
        file = await bot.get_file(document.file_id)
        await bot.download_file(file.file_path, temp_csv_path)
        logging.info(f"CSV файл загружен: {temp_csv_path}")

        # Вызываем функцию из модуля
        await message.answer("🔄 Обрабатываю файл...")
        # process_csv_and_update_ratings ожидает путь к файлу
        # Запускаем в отдельном потоке, если операция тяжёлая
        # asyncio.run не нужен внутри обработчика aiogram
        process_csv_and_update_ratings(temp_csv_path)

        await message.answer("✅ Рейтинги успешно обновлены на основе загруженного файла.")

    except Exception as e:
        logging.error(f"Ошибка при обработке CSV: {e}")
        await message.answer(f"❌ Произошла ошибка при обработке файла: {str(e)}")

    finally:
        # Удаляем временный файл после обработки (успешной или неудачной)
        try:
            if os.path.exists(temp_csv_path):
                os.remove(temp_csv_path)
                logging.info(f"Временный файл удален: {temp_csv_path}")
        except OSError as e:
            logging.warning(f"Не удалось удалить временный файл {temp_csv_path}: {e}")


@dp.message(Command(commands=['reset_ratings']))
async def cmd_reset_ratings(message: types.Message):
    """Обработчик команды /reset_ratings. Удаляет все данные рейтингов."""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав для выполнения этой команды.")
        return

    # Импортируем движок из модуля, чтобы не создавать его дважды
    # и использовать тот же путь к БД
    try:
        # Импортируем engine из rating_module
        # (это может быть не лучшая практика, если engine создаётся при импорте,
        # но в данном случае просто для тестовой команды подойдёт)
        # Лучше вынести engine в отдельный файл, например, db_config.py
        # и импортировать оттуда.
        # Пока оставим так, но имейте в виду.
        from rating_module import engine
        from sqlalchemy import text
    except ImportError:
        logging.error("Не удалось импортировать engine из rating_module.")
        await message.answer("❌ Ошибка: не удалось подключиться к базе данных рейтингов.")
        return

    try:
        with engine.connect() as conn:
            trans = conn.begin()
            # Удаляем данные из weekly_data
            conn.execute(text("DELETE FROM weekly_data;"))
            # Удаляем данные из weeks (опционально, если хотите очистить календарь)
            # conn.execute(text("DELETE FROM weeks;"))
            # Если НЕ удаляете weeks, то следующая вставка в weeks при новой загрузке
            # не будет конфликтовать с уже существующими week_start_date.
            # Если УДАЛЯЕТЕ weeks, то при следующей загрузке все даты будут вставлены заново.
            # Обычно, для тестирования, можно удалять и weeks тоже.
            conn.execute(text("DELETE FROM weeks;"))
            trans.commit()
            logging.info("Команда /reset_ratings: все данные из weekly_data и weeks удалены.")
        await message.answer("✅ Все данные рейтингов успешно удалены из базы данных.")
    except Exception as e:
        logging.error(f"Ошибка при удалении данных рейтингов: {e}")
        await message.answer(f"❌ Произошла ошибка при очистке базы данных: {str(e)}")
        


# Заказ товара
@dp.message(F.text == "🛒 Заказ под клиента")
async def handle_client_order(message: types.Message, state: FSMContext):
    """Начало оформления заказа"""
    await state.update_data(last_activity=datetime.now().isoformat())
    user_data = await get_user_data(str(message.from_user.id))
    
    if not user_data:
        await message.answer("❌ Сначала пройдите регистрацию через /start")
        return
    
    await state.update_data(
        shop=user_data['shop'],
        user_name=user_data['name'],
        user_position=user_data['position']
    )
    
    await message.answer("🔢 Введите артикул товара:", 
                         reply_markup=cancel_keyboard())
    await log_user_activity(message.from_user.id, "Заказ под клиента", "order")
    await state.set_state(OrderStates.article_input)


@dp.message(OrderStates.article_input)
async def process_article_input(message: types.Message, state: FSMContext):
    """Обработка введенного артикула (одного или списка)"""
    if message.photo:
        await message.answer("📸 Распознавание штрих-кодов отключено. Введите артикул вручную.")
        return

    raw_input = message.text.strip()

    # Парсим ввод на список артикулов
    parts = re.split(r'[,\n\r]+', raw_input)
    articles = []
    for part in parts:
        cleaned = re.sub(r'\D', '', part.strip())
        if len(cleaned) >= 4 and len(cleaned) <= 10:
            articles.append(cleaned)

    if not articles:
        await message.answer("❌ Неверный формат артикула(ов). Артикул должен состоять из 4-10 цифр.")
        return

    if len(articles) == 1:
        # --- СТАРАЯ ЛОГИКА ---
        article = articles[0]
        await state.update_data(article=article)
        await message.answer("📌 Выберите магазин для заказа:", reply_markup=quick_shop_selection_keyboard())
        await state.set_state(OrderStates.shop_selection)

    else:
        # --- НОВАЯ ЛОГИКА ---
        # Сохраняем список артикулов в state
        await state.update_data(art_list=articles)
        await message.answer(f"🔍 Найдено {len(articles)} артикулов. Введите количество для каждого (в том же порядке, через запятую или с новой строки).")
        # Переходим к вводу количеств
        await state.set_state(OrderStates.waiting_for_quantities_list)


@dp.message(OrderStates.shop_selection)
async def process_shop_selection(message: types.Message, state: FSMContext):
    """Обработка выбора магазина из 3 вариантов"""
    user_data = await get_user_data(str(message.from_user.id))
    
    # Словарь для сопоставления текста кнопки с номером магазина
    shop_mapping = {
        "🏪 Магазин 8": "8",
        "🏪 Магазин 92": "92",
        "🏪 Магазин 147": "147",
        "🏪 Магазин 150": "150",
        "🏪 Магазин 165": "165",
        "🏪 Магазин 255": "255"
    }
    
    if message.text in shop_mapping:
        selected_shop = shop_mapping[message.text]
        await state.update_data(selected_shop=selected_shop)
        await continue_order_process(message, state)
    elif message.text == "❌ Отмена":
        await message.answer("❌ Выбор магазина отменен.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.clear()
    else:
        # Если пользователь ввел что-то другое или нажал не ту кнопку
        await message.answer(
            "❌ Неверный выбор. Пожалуйста, выберите один из вариантов:",
            reply_markup=quick_shop_selection_keyboard()
        )
        

async def continue_order_process(message: types.Message, state: FSMContext):
    """Продолжение обработки заказа после выбора магазина"""
    data = await state.get_data()
    article = data.get('article')
    selected_shop = data.get('selected_shop')

    await message.answer("🔄 Загружаю информацию о товаре...")
    product_info = await get_product_info(article, selected_shop)

    if not product_info:
        await message.answer("❌ Товар не доступен к заказу обратись к МУТЗ", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.clear()
        return

    response = (
        f"🏪 Магазин: {selected_shop}\n"
        f"📦 Артикул: {product_info['Артикул']}\n"
        f"🏷️ Название: {product_info['Название']}\n"
        f"🏭 Поставщик: {product_info['Поставщик']}\n"
        f"📅 Дата заказа: {product_info['Дата заказа']}\n"
        f"🚚 Дата поставки: {product_info['Дата поставки']}\n"
    )

    # --- НОВОЕ: Информация о каникулах ---
    holidays = product_info.get('Каникулы', None)
    exceptions = product_info.get('Исключения', None)

    if holidays:
        holiday_dates = holiday_ranges = format_holidays_ranges(holidays)
        response += f"\n⚠️ Поставщик находится в каникулах: {holiday_dates}"
        if exceptions:
            exception_dates = ", ".join(d.strftime("%d.%m.%Y") for d in sorted(exceptions))
            response += f"\n✅ Но принимает заказы: {exception_dates}"
    # --- /НОВОЕ ---

    # Сохраняем информацию о товаре в state
    await state.update_data(
        product_name=product_info['Название'],
        department=product_info['Отдел'],
        supplier_name=product_info['Поставщик'],
        order_date=product_info['Дата заказа'],
        delivery_date=product_info['Дата поставки'],
        top_in_shop=product_info.get('Топ в магазине', '0'),
        holidays=product_info.get('Каникулы', []),
        exceptions=product_info.get('Исключения', [])
    )

    await message.answer(response)

    # --- Проверка ТОП 0 ---
    if product_info.get('Топ в магазине', '0') == '0':
        # ПОКАЗАТЬ ПРЕДУПРЕЖДЕНИЕ О ТОП 0
        await message.answer(
            "⚠️ <b>Внимание, артикул в ТОП 0!</b>\n"
            "Для оформления заказа потребуется одобрение МЗ.",
            parse_mode='HTML'
        )
        # ПЕРЕХОДИМ К ВВОДУ КОЛИЧЕСТВА ДЛЯ ТОП 0
        await message.answer("🔢 Введите количество товара:", reply_markup=cancel_keyboard())
        await state.set_state(OrderStates.quantity_input_for_top0)
    else:
        # НЕ ТОП 0 - ВВОД КОЛИЧЕСТВА КАК РАНЬШЕ
        await message.answer("🔢 Введите количество товара:", reply_markup=cancel_keyboard())
        await state.set_state(OrderStates.quantity_input)
        

@dp.message(OrderStates.quantity_input_for_top0)
async def process_quantity_input_for_top0(message: types.Message, state: FSMContext):
    """Обработка введенного количества для ТОП 0"""
    try:
        quantity = int(message.text)
        if quantity <= 0:
            await message.answer("❌ Количество должно быть положительным числом. Пожалуйста, введите еще раз:")
            return
    except ValueError:
        await message.answer("❌ Некорректный формат количества. Пожалуйста, введите целое число:")
        return

    # Сохраняем количество
    await state.update_data(quantity=quantity)

    # Переходим к вводу причины
    await message.answer("📝 Введите причину заказа:", reply_markup=cancel_keyboard())
    await state.set_state(OrderStates.reason_input_for_top0)


@dp.message(OrderStates.reason_input_for_top0)
async def process_reason_input_for_top0(message: types.Message, state: FSMContext):
    """Обработка причины заказа для ТОП 0"""
    reason = message.text.strip()
    if not reason:
        await message.answer("❌ Причина не может быть пустой. Пожалуйста, введите причину заказа:")
        return

    # Сохраняем причину
    await state.update_data(order_reason=reason)

    # --- Получение информации о товаре ---
    data = await state.get_data()
    article = data['article']
    selected_shop = data['selected_shop']
    product_info = await get_product_info(article, selected_shop) # Нужно снова получить, если не сохранено
    if not product_info:
        await message.answer("❌ Не удалось получить информацию о товаре. Попробуйте позже.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.clear()
        return

    product_name = product_info['Название']
    product_supplier = product_info['Поставщик']
    department = product_info['Отдел']
    quantity = data['quantity'] # Уже проверен и сохранен
    reason = data['order_reason'] # Уже проверена и сохранена

    # --- Получение ID менеджера из кэша ---
    manager_info = get_manager_id_by_department(department)
    if not manager_info:
        await message.answer("❌ Не удалось определить менеджера для отдела товара. Свяжитесь с администратором.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.clear()
        logging.warning(f"Менеджер для отдела '{department}' не найден в кэше МЗ.")
        return

    manager_id = manager_info['id']
    if manager_id is None:
        logging.error(f"Ошибка: manager_id для отдела '{department}' оказался None! manager_info: {manager_info}")
        await message.answer("❌ Произошла ошибка при определении менеджера. Попробуйте позже или свяжитесь с администратором.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.clear()
        return
        
    manager_first_name = manager_info.get('first_name', 'N/A')
    manager_last_name = manager_info.get('last_name', 'N/A')
    manager_full_name = f"{manager_first_name} {manager_last_name}".strip() or "Не указано"

    # --- Создание записи в БД ---
    request_id = str(uuid.uuid4())
    current_state_data = await state.get_data()
    success_db_create = await create_approval_request(
        request_id=request_id,
        user_id=message.from_user.id,
        manager_id=manager_id,
        department=department,
        article=article,
        shop=selected_shop,
        product_name=product_name,
        product_supplier=product_supplier,
        user_data=current_state_data # Передаем ВСЕ текущие данные FSM
    )

    if not success_db_create:
        await message.answer("❌ Ошибка при создании запроса на одобрение. Попробуйте позже.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.clear()
        return

    # --- Формирование сообщения для менеджера (с причиной!) ---
    manager_message = (
        f"🚨 <b>Запрос на одобрение заказа ТОП 0</b>\n"
        f"👤 Пользователь: @{message.from_user.username or 'N/A'} (ID: {message.from_user.id})\n"
        f"🏪 Магазин: {selected_shop}\n"
        f"📦 Артикул: {article}\n"
        f"🏷️ Название: {product_name}\n"
        f"🔢 Кол-во: {quantity}\n" # Добавляем количество
        f"🏭 Поставщик: {product_supplier}\n"
        f"🔢 Отдел: {department}\n"
        f"📝 Причина заказа: {reason}\n\n"  # <-- Добавляем причину!
        f"Запрос ID: <code>{request_id}</code>"
    )
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Одобрить", callback_data=f"approve:{request_id}")
    builder.button(text="❌ Отказать", callback_data=f"start_reject:{request_id}")
    builder.adjust(2)
    manager_kb = builder.as_markup()

    # --- Отправка сообщения менеджеру ---
    try:
        sent_message = await bot.send_message(chat_id=manager_id, text=manager_message, reply_markup=manager_kb, parse_mode='HTML')
        await update_approval_request_status(request_id, 'pending', sent_message.message_id)
    except Exception as e:
        logging.error(f"❌ Не удалось отправить запрос менеджеру {manager_id}: {e}")
        await message.answer("❌ Не удалось отправить запрос менеджеру. Попробуйте позже.", reply_markup=main_menu_keyboard(message.from_user.id))
        await delete_approval_request(request_id) # Удаляем запрос, если сообщение не отправилось
        await state.clear()
        return

    # --- Сообщение пользователю ---
    await message.answer(
        "✅ Запрос на одобрение отправлен МЗ.\n"
        f"МЗ отдела № {department} <b>{manager_full_name}</b> рассмотрит ваш запрос (артикул {article}, кол-во {quantity}, причина: {reason}).\n"
        f"Вы можете продолжить работу с ботом. После одобрения вы получите уведомление.",
        parse_mode='HTML',
        reply_markup=main_menu_keyboard(message.from_user.id)
    )

    # --- Очистка состояния пользователя ---
    await state.clear()
    return # Завершаем обработку, заказ приостановлен



@dp.message(OrderStates.quantity_input)
async def process_quantity_input(message: types.Message, state: FSMContext):
    """Обработка введенного количества (только для НЕ ТОП 0)"""
    try:
        quantity = int(message.text)
        if quantity <= 0:
            await message.answer("❌ Количество должно быть положительным числом. Пожалуйста, введите еще раз:")
            return
    except ValueError:
        await message.answer("❌ Некорректный формат количества. Пожалуйста, введите целое число:")
        return

    # Сохраняем количество
    await state.update_data(quantity=quantity)

    # Переходим к вводу причины (для НЕ ТОП 0)
    await message.answer("📝 Введите причину заказа:", reply_markup=cancel_keyboard())
    await state.set_state(OrderStates.order_reason_input)
    

@dp.message(OrderStates.order_reason_input)
async def process_order_reason(message: types.Message, state: FSMContext):
    
    if not message.text:
        await message.answer("❌ Ожидается текстовое сообщение. Пожалуйста, введите причину заказа.")
        return
        
    reason = message.text.strip()
    await state.update_data(order_reason=reason)
    data = await state.get_data()
    selected_shop = data.get('selected_shop')
    
    # Формируем сообщение подтверждения (без логики ТОП 0)
    response = (
        "🔎 Проверьте данные заказа:\n"
        f"🏪 Магазин: {selected_shop}\n"
        f"📦 Артикул: {data['article']}\n"
        f"🏷️ Название: {data['product_name']}\n"
        f"🏭 Поставщик: {data['supplier_name']}\n" # Используем supplier_name
        f"📅 Дата заказа: {data['order_date']}\n"
        f"🚚 Дата поставки: {data['delivery_date']}\n"
        f"🔢 Кол-во: {data['quantity']}\n"
        f"📝 Причина: {reason}\n"
    )
    
    holidays = data.get('holidays', [])  
    exceptions = data.get('exceptions', [])

    if holidays:
        holiday_dates = holiday_ranges = format_holidays_ranges(holidays)
        response += f"\n⚠️ Поставщик находится в каникулах: {holiday_dates}"
        if exceptions:
            exception_dates = ", ".join(d.strftime("%d.%m.%Y") for d in sorted(exceptions))
            response += f"\n✅ Но принимает заказы: {exception_dates}"

    
    await message.answer(response, reply_markup=confirm_keyboard())
    await state.set_state(OrderStates.confirmation)


@dp.message(OrderStates.confirmation, F.text == "✅ Подтвердить")
async def final_confirmation(message: types.Message, state: FSMContext):
    """Подтверждение заказа с добавлением в очередь."""
    
    # --- Получаем данные заказа ---
    await state.update_data(last_activity=datetime.now().isoformat())
    data = await state.get_data()
    
    # Проверка обязательных полей
    required_fields = ['selected_shop', 'article', 'order_reason', 'quantity', 'department', 'user_name', 'user_position']
    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        error_msg = f"❌ Ошибка оформления заказа: отсутствуют обязательные поля {missing_fields}. Попробуйте заново."
        logging.error(f"final_confirmation: {error_msg} для user_id={message.from_user.id}")
        await message.answer(error_msg, reply_markup=main_menu_keyboard(message.from_user.id))
        await state.clear()
        return

    # --- НЕМЕДЛЕННО информируем пользователя ---
    await message.answer("✅ Заказ отправлен!", 
                         reply_markup=main_menu_keyboard(message.from_user.id))
    
    
    # --- Добавляем заказ в очередь ---
    user_id = message.from_user.id
    data_copy = data.copy() # Создаем копию для передачи в БД
    
    success_enqueue = await add_order_to_queue(user_id, data_copy)
    
    if not success_enqueue:
        # Если не удалось добавить в очередь, логируем критическую ошибку
        critical_error_msg = f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось добавить заказ пользователя {user_id} в очередь БД!"
        logging.critical(critical_error_msg)
        # Отправляем уведомление админам
        for admin_id in ADMINS:
            try:
                await bot.send_message(
                    admin_id, 
                    f"🚨 {critical_error_msg}\nАртикул: {data.get('article', 'N/A')}, Магазин: {data.get('selected_shop', 'N/A')}"
                )
            except Exception as notify_err:
                logging.error(f"Не удалось уведомить админа {admin_id}: {notify_err}")
    else:
        logging.info(f"Заказ пользователя {user_id} успешно поставлен в очередь.")
    
    await state.clear()



@dp.message(OrderStates.confirmation, F.text == "✏️ Исправить количество")
async def correct_quantity(message: types.Message, state: FSMContext):
    """Корректировка количества"""
    await message.answer("🔢 Введите новое количество:", 
                        reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(OrderStates.quantity_input)


@dp.message(InfoRequest.waiting_for_action, F.text == "🛒 Заказать этот товар")
async def initiate_order_from_info(message: types.Message, state: FSMContext):
    """Инициирует процесс заказа на основе информации, полученной ранее."""
    user_id = message.from_user.id
    logging.info(f"Пользователь {user_id} выбрал 'Заказать этот товар' из информации.")

    # Получаем данные товара из состояния
    data = await state.get_data()
    article = data.get('article')
    # selected_shop НЕ используется, так как мы переходим к выбору магазина

    if not article:
        logging.error(f"Артикул отсутствует в состоянии при попытке заказать из информации для user_id {user_id}.")
        await message.answer("❌ Произошла ошибка при подготовке заказа. Попробуйте снова.")
        await state.clear()
        return

    # --- ОБЯЗАТЕЛЬНО: Получаем и сохраняем информацию о пользователе ---
    user_data = await get_user_data(user_id)
    if not user_data:
        logging.error(f"Профиль не найден для {user_id} при попытке начать заказ из информации.")
        await message.answer("❌ Ваш профиль не найден. Пройдите регистрацию через /start")
        await state.clear()
        return

    user_name = f"{user_data.get('name', 'N/A')} {user_data.get('surname', 'N/A')}".strip() or "Неизвестный пользователь"
    user_position = user_data.get('position', 'Не указана')
    # --------------------------------------------------------

    # Обновляем last_activity
    await state.update_data(last_activity=datetime.now().isoformat())

    # Сохраняем артикул И информацию о пользователе в состоянии для следующего шага
    await state.update_data(
        article=article,
        user_name=user_name,      
        user_position=user_position # <-- Сохраняем должность
    )

    # --- Переход к выбору магазина ---
    await message.answer("📌 Выберите магазин для заказа:", reply_markup=quick_shop_selection_keyboard())
    await state.set_state(OrderStates.shop_selection)
    # ---------------------------

    logging.info(f"Инициирован процесс заказа для товара {article} из состояния информации. Ожидается выбор магазина.")

# --- Хендлер для возврата в главное меню из состояния ожидания действия ---
@dp.message(InfoRequest.waiting_for_action, F.text == "🏠 В главное меню")
async def cancel_info_and_return_home(message: types.Message, state: FSMContext):
    """Отменяет ожидание действия и возвращает в главное меню."""
    await message.answer("Вы в главном меню.", reply_markup=main_menu_keyboard(message.from_user.id))
    await state.clear() # Очищаем всё состояние после завершения сценария


@dp.message(InfoRequest.waiting_for_action, F.text == "🔄 Повторить ввод артикула")
async def repeat_article_input(message: types.Message, state: FSMContext):
    """Повторный ввод артикула без выхода из раздела информации."""
    user_id = message.from_user.id
    logging.info(f"Пользователь {user_id} выбрал 'Повторить ввод артикула'.")

    # Очищаем только информацию о предыдущем артикуле, но остаёмся в сценарии информации
    # Не очищаем всё состояние, чтобы не потерять shop, если он был сохранён ранее в этом сеансе
    # Просто удалим ключи, связанные с конкретным товаром
    current_data = await state.get_data()
    # Удаляем только те ключи, которые относятся к предыдущему запросу
    keys_to_remove = ['article', 'product_name', 'department', 'supplier_name', 'order_date', 'delivery_date', 'top_in_shop', 'selected_shop'] # Добавьте сюда все ключи, которые хотите сбросить
    new_data = {k: v for k, v in current_data.items() if k not in keys_to_remove}
    await state.set_data(new_data)

    # Запрашиваем новый артикул
    await message.answer("🔢 Введите новый артикул товара:", reply_markup=cancel_keyboard())
    await state.set_state(InfoRequest.article_input)
    
# --- Опционально: Обработчик для неожиданного текста в состоянии waiting_for_action ---
@dp.message(InfoRequest.waiting_for_action)
async def unexpected_input_waiting_action(message: types.Message):
    """Обработка неожиданного ввода в состоянии ожидания действия."""
    await message.answer("Пожалуйста, используйте кнопки 'Заказать этот товар' или 'В главное меню'.")


# Запрос информации о товаре
@dp.message(F.text == "📋 Запрос информации")
async def handle_info_request(message: types.Message, state: FSMContext):
    """Обработчик запроса информации с защитой от потери данных"""
    try:
        await state.update_data(last_activity=datetime.now().isoformat())
        await log_user_activity(message.from_user.id, "Запрос информации", "info")
        
        # Получаем данные пользователя
        user_data = await get_user_data(str(message.from_user.id))
        if not user_data:
            await message.answer("❌ Сначала пройдите регистрацию через /start")
            return
        
        # Проверяем наличие магазина в профиле
        shop = user_data.get('shop', 'Не указан')
        if shop == "Не указан":
            await message.answer("❌ В вашем профиле не указан магазин. Обратитесь к администратору.")
            return
        
        # Сохраняем магазин в состоянии
        await state.set_data({
            'shop': shop,
            'last_activity': datetime.now().isoformat()
        })
        
        await message.answer("🔢 Введите артикул товара:", reply_markup=cancel_keyboard())
        await state.set_state(InfoRequest.article_input)
        
    except Exception as e:
        logging.error(f"Ошибка в начале запроса информации: {str(e)}")
        await message.answer("⚠️ Произошла ошибка. Попробуйте позже.")
        await state.clear()


@dp.message(InfoRequest.article_input)
async def process_info_request(message: types.Message, state: FSMContext):
    """Обработка запроса информации о товаре с дополнительной защитой"""
    try:
        # Получаем данные состояния
        data = await state.get_data()
        user_id = str(message.from_user.id)
        
        # Проверяем наличие магазина в данных состояния
        if 'shop' not in data:
            # Логируем предупреждение
            logging.warning(f"Магазин отсутствует в состоянии для {user_id}")
            
            # Если нет в состоянии, пробуем получить из профиля
            user_data = await get_user_data(user_id)
            if not user_data:
                logging.warning(f"Профиль не найден для {user_id}")
                await message.answer("❌ Ваш профиль не найден. Пройдите регистрацию через /start")
                await state.clear()
                return
                
            shop = user_data.get('shop', 'Не указан')
            if shop == "Не указан":
                logging.warning(f"Магазин не указан в профиле для {user_id}")
                await message.answer("❌ В вашем профиле не указан магазин. Обратитесь к администратору.")
                await state.clear()
                return
            else:
                # Обновляем состояние
                await state.update_data(shop=shop)
                logging.info(f"Магазин {shop} восстановлен из профиля для {user_id}")
        else:
            shop = data['shop']
            logging.info(f"Используется магазин {shop} из состояния для {user_id}")
        
        # Обработка ввода
        article = None
        if message.photo:
            await message.answer("📸 Распознавание штрих-кодов отключено. Введите артикул вручную.")
            return
        else:
            # Логирование ручного ввода
            logging.info(f"Ручной ввод артикула: {message.text} (пользователь: {user_id})")
            
            # Обработка текста
            article = message.text.strip()
            if not re.match(r'^\d{4,10}$', article):
                await message.answer("❌ Неверный формат артикула.")
                return
        
        # Поиск информации о товаре
        logging.info(f"Поиск информации о товаре {article} для магазина {shop} (пользователь: {user_id})")
        await message.answer("🔄 Поиск информации о товаре...")
        product_info = await get_product_info(article, shop)
        
        if not product_info:
            logging.warning(f"Товар {article} не найден для магазина {shop} (пользователь: {user_id})")
            await message.answer("❌ Товар не доступен к заказу обратись к МУТЗ", reply_markup=main_menu_keyboard(message.from_user.id))
            await state.clear()
            return

        await state.update_data(
            article=article,
            selected_shop=shop, # Используем shop как selected_shop
            product_name=product_info['Название'],
            department=product_info['Отдел'], # Убедитесь, что ключи совпадают с вашими
            supplier_name=product_info['Поставщик'],
            order_date=product_info['Дата заказа'],
            delivery_date=product_info['Дата поставки'],
            top_in_shop=product_info.get('Топ в магазине', '0') # Убедитесь, что ключ совпадает
        )
        # Формирование ответа
        response = (
            f"🔍 Информация о товаре:\n"
            f"🏪 Магазин: {shop}\n"
            f"📦 Артикул: {product_info['Артикул']}\n"
            f"🏷️ Название: {product_info['Название']}\n"
            f"🔢 Отдел: {product_info['Отдел']}\n"
            f"📅 Ближайшая дата заказа: {product_info['Дата заказа']}\n"
            f"🚚 Ожидаемая дата поставки: {product_info['Дата поставки']}\n"
            f"🏭 Поставщик: {product_info['Поставщик']}" 
        )
        
        # Добавляем предупреждение для ТОП 0
        top_status = product_info.get('Топ в магазине', '0')
        if top_status == '0':
            response += "\n\n⚠️ <b>ВНИМАНИЕ: Артикул в ТОП 0!</b>\nСвяжитесь с менеджером для уточнения информации"

        elif top_status in ['1', '2']:
            response += f"\n\n✅ <b>Статус: ТОП {top_status}</b>\nМожно заказать."
        
        holidays = product_info.get('Каникулы', None)
        exceptions = product_info.get('Исключения', None)

        if holidays:
            holiday_ranges = format_holidays_ranges(holidays)
            response += f"\n⚠️ Поставщик находится в каникулах: {holiday_ranges}"
            if exceptions:
                exception_dates = ", ".join(d.strftime("%d.%m.%Y") for d in sorted(exceptions))
                response += f"\n✅ Но принимает заказы: {exception_dates}"

        builder = ReplyKeyboardBuilder()
        builder.button(text="🛒 Заказать этот товар")
        builder.button(text="🔄 Повторить ввод артикула")
        builder.button(text="🏠 В главное меню")
        action_kb = builder.as_markup(resize_keyboard=True)

        # Отправляем сообщение с информацией и клавиатурой
        await message.answer(response, reply_markup=action_kb)

        # Устанавливаем новое состояние, ожидая действия пользователя
        await state.set_state(InfoRequest.waiting_for_action)

        logging.info(f"Успешно обработан запрос информации для товара {article} (пользователь: {user_id}). Предложено действие.")

    except Exception as e:
        logging.error(f"Ошибка в обработчике информации: {str(e)}", exc_info=True)
        await message.answer("⚠️ Произошла ошибка при обработке запроса. Попробуйте позже.")
        await state.clear()



##=============================ОБРАБОТЧИКИ АДМИН ПАНЕЛИ====================================
@dp.message(F.text == "🛠 Админ-панель")
async def handle_admin_panel(message: types.Message):
    """Панель администратора"""
    if message.from_user.id not in ADMINS:
        await message.answer("⛔ У вас нет прав доступа")
        return
    
    await message.answer("🛠 Панель администратора:", 
                        reply_markup=admin_panel_keyboard())


@dp.message(F.text == "📊 Статистика")
async def handle_admin_stats(message: types.Message):
    """Статистика бота"""
    if message.from_user.id not in ADMINS:
        return
    
    try:
        # Получаем данные о пользователях из кэша
        users_data = pickle.loads(cache.get("users_data", b"[]"))
        users_count = len(users_data) if users_data else 0
        
        # ПРЯМОЕ ОБРАЩЕНИЕ К GOOGLE SHEETS ДЛЯ СТАТИСТИКИ
        stats_sheet = main_spreadsheet.worksheet(STATSS_SHEET_NAME)
        stats_records = stats_sheet.get_all_records()
        
        # Считаем количество заказов
        orders_count = sum(1 for r in stats_records if r.get('Тип события') == 'order')
        
        # Получаем системные метрики
        cpu_usage = psutil.cpu_percent()
        memory_usage = psutil.virtual_memory().percent
        
        response = (
            f"📊 Статистика бота:\n\n"
            f"• Пользователей: {users_count}\n"
            f"• Заказов оформлено: {orders_count}\n"
            f"• Логов действий: {len(stats_records)}\n\n"
            f"⚙️ Состояние сервера:\n"
            f"• Загрузка CPU: {cpu_usage}%\n"
            f"• Использование RAM: {memory_usage}%\n"
            f"• Сервисный режим: {'ВКЛ' if SERVICE_MODE else 'ВЫКЛ'}"
        )
        await message.answer(response, reply_markup=admin_panel_keyboard())
        
    except Exception as e:
        logging.error(f"Ошибка при получении статистики: {str(e)}")
        await message.answer(f"❌ Ошибка при получении статистики: {str(e)}", 
                            reply_markup=admin_panel_keyboard())



@dp.message(F.text == "📊 Дамп памяти")
async def handle_memory_dump(message: types.Message):
    """Генерация дампа памяти для анализа (текстовый вариант)"""
    if message.from_user.id not in ADMINS:
        return
    
    wait_msg = await message.answer("🔄 Формирование отчета о памяти...")
    
    try:
        # Формируем текстовый отчет
        report = []
        process = psutil.Process()
        mem_info = process.memory_info()
        
        # Основная информация
        report.append(f"<b>📊 Отчет об использовании памяти</b>")
        report.append(f"• Время: {datetime.now().strftime('%H:%M:%S')}")
        report.append(f"• RSS: {mem_info.rss / 1024 / 1024:.2f} MB")
        report.append(f"• VMS: {mem_info.vms / 1024 / 1024:.2f} MB")
        
        # Информация о процессах
        report.append("\n<b>🔢 Процессная информация:</b>")
        report.append(f"• Потоков: {process.num_threads()}")
        report.append(f"• Дескрипторов: {process.num_fds()}")
        
        # Топ объектов в памяти
        report.append("\n<b>📦 Топ объектов в памяти:</b>")
        common_types = objgraph.most_common_types(limit=15)
        for i, (obj_type, count) in enumerate(common_types, 1):
            report.append(f"{i}. {obj_type}: {count}")
        
        # Собираем полный отчет
        full_report = "\n".join(report)
        
        # Разбиваем отчет на части по 4000 символов
        for i in range(0, len(full_report), 4000):
            part = full_report[i:i+4000]
            await message.answer(
                part,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
        
        await wait_msg.delete()
        
    except Exception as e:
        logging.error(f"Ошибка при создании дампа памяти: {str(e)}")
        await message.answer(f"❌ Ошибка: {str(e)}")
        with suppress(Exception):
            await wait_msg.delete()


##===============РАССЫЛКА=================

@dp.message(F.text == "📢 Рассылка")
async def handle_broadcast_menu(message: types.Message, state: FSMContext):
    """Начало процесса рассылки"""
    if message.from_user.id not in ADMINS:
        return
    
    await message.answer(
        "✉️ Введите сообщение для рассылки (можно с медиа-вложениями):",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await state.set_state(AdminBroadcast.message_input)

@dp.message(AdminBroadcast.message_input)
async def process_broadcast_message(message: types.Message, state: FSMContext):
    """Обработка сообщения для рассылки"""
    # Сохраняем контент в зависимости от типа
    content = {
        'text': message.html_text,
        'media': None,
        'type': 'text'
    }
    
    if message.photo:
        content.update({
            'type': 'photo',
            'media': message.photo[-1].file_id,
            'caption': message.caption
        })
    elif message.document:
        content.update({
            'type': 'document',
            'media': message.document.file_id,
            'caption': message.caption
        })
    
    await state.update_data(content=content)
    
    # Предпросмотр сообщения
    preview_text = "✉️ Предпросмотр сообщения:\n"
    if content['type'] == 'text':
        preview_text += content['text']
    else:
        preview_text += f"[{content['type'].upper()}] {content.get('caption', '')}"
    
    await message.answer(
        preview_text,
        reply_markup=broadcast_target_keyboard()
    )
    await state.set_state(AdminBroadcast.target_selection)

@dp.message(AdminBroadcast.target_selection)
async def handle_target_selection(message: types.Message, state: FSMContext):
    """Обработка выбора целевой аудитории"""
    if message.text == "Всем пользователям":
        await state.update_data(target="all")
        await message.answer("✅ Отправить всем пользователям", 
                            reply_markup=broadcast_confirmation_keyboard())
        await state.set_state(AdminBroadcast.confirmation)
    elif message.text == "Вручную":
        await message.answer("🔢 Введите ID пользователей через запятую:")
        await state.set_state(AdminBroadcast.manual_ids)
    elif message.text == "❌ Отмена":
        await state.clear()
        await message.answer("❌ Рассылка отменена", reply_markup=admin_panel_keyboard())
    else:
        await message.answer("❌ Неверный выбор. Пожалуйста, используйте кнопки.", 
                            reply_markup=broadcast_target_keyboard())

@dp.message(AdminBroadcast.manual_ids)
async def process_manual_ids(message: types.Message, state: FSMContext):
    """Обработка ручного ввода ID"""
    user_ids = [id.strip() for id in message.text.split(",") if id.strip().isdigit()]
    
    if not user_ids:
        await message.answer("❌ Неверный формат ID. Повторите ввод:")
        return
    
    await state.update_data(target="manual", user_ids=user_ids)
    await message.answer(f"✅ ID пользователей введены ({len(user_ids)} шт.)", 
                        reply_markup=broadcast_confirmation_keyboard())
    await state.set_state(AdminBroadcast.confirmation)

@dp.message(AdminBroadcast.confirmation, F.text == "✅ Подтвердить рассылку")
async def confirm_broadcast(message: types.Message, state: FSMContext):
    """Подтверждение и запуск рассылки"""
    data = await state.get_data()
    content = data['content']
    target = data.get('target', 'all')
    user_ids = data.get('user_ids', [])
    
    # Получаем список всех пользователей для рассылки
    if target == "all":
        users_data = pickle.loads(cache.get("users_data", b"[]"))
        user_ids = [str(user['ID пользователя']) for user in users_data if user.get('ID пользователя')]
    elif target == "manual":
        # Уже есть user_ids
        pass
    
    if not user_ids:
        await message.answer("❌ Нет пользователей для рассылки")
        await state.clear()
        return
    
    # Записываем в логи
    try:
        logs_sheet.append_row([
            datetime.now().strftime("%d.%m.%Y %H:%M"),
            message.from_user.id,
            "BROADCAST",
            f"Type: {content['type']}, Users: {len(user_ids)}"
        ])
    except Exception as e:
        logging.error(f"Ошибка логирования рассылки: {str(e)}")
    
    await message.answer(f"🔄 Начинаю рассылку для {len(user_ids)} пользователей...", 
                        reply_markup=admin_panel_keyboard())
    
    # Запускаем асинхронную рассылку
    asyncio.create_task(send_broadcast(content, user_ids))
    
    await state.clear()

async def send_broadcast(content: dict, user_ids: list):
    """Асинхронная отправка рассылки"""
    success = 0
    failed = 0
    errors = []
    
    for user_id in user_ids:
        try:
            if not user_id.strip():
                continue
                
            if content['type'] == 'text':
                await bot.send_message(int(user_id), content['text'])
            elif content['type'] == 'photo':
                await bot.send_photo(
                    int(user_id),
                    photo=content['media'],
                    caption=content.get('caption', '')
                )
            elif content['type'] == 'document':
                await bot.send_document(
                    int(user_id),
                    document=content['media'],
                    caption=content.get('caption', '')
                )
            
            success += 1
            await asyncio.sleep(0.1)  # Защита от ограничений
        except TelegramForbiddenError:
            failed += 1  # Пользователь заблокировал бота
        except Exception as e:
            failed += 1
            logging.error(f"Ошибка рассылки для {user_id}: {str(e)}")
            if not isinstance(e, (TelegramBadRequest, TimeoutError)):
                raise
    
    # Отправляем отчет администратору
    report = (
        f"📊 Результаты рассылки:\n"
        f"• Всего получателей: {len(user_ids)}\n"
        f"• Успешно: {success}\n"
        f"• Не удалось: {failed}"
    )
    
    if errors:
        unique_errors = set(errors)
        report += f"\n\nОсновные ошибки:\n" + "\n".join([f"- {e}" for e in list(unique_errors)[:3]])
    
    try:
        await bot.send_message(ADMINS[0], report)
    except Exception as e:
        logging.error(f"Не удалось отправить отчет: {str(e)}")


##===============ОБРАБОТЧИКИ=================

@dp.message(F.text == "🔄 Обновить кэш")
async def handle_cache_refresh(message: types.Message):
    """Обновление кэша"""
    if message.from_user.id not in ADMINS:
        return
    
    try:
        cache.clear()
        await preload_cache()
        await message.answer("✅ Кэш успешно обновлен!", 
                            reply_markup=admin_panel_keyboard())
    except Exception as e:
        await message.answer(f"❌ Ошибка обновления кэша: {str(e)}", 
                            reply_markup=admin_panel_keyboard())

@dp.message(F.text == "🔧 Сервисный режим")
async def handle_service_mode_menu(message: types.Message):
    """Управление сервисным режимом"""
    if message.from_user.id not in ADMINS:
        return
    
    status = "🟢 ВКЛЮЧЕН" if SERVICE_MODE else "🔴 ВЫКЛЮЧЕН"
    await message.answer(
        f"🛠 Текущий статус сервисного режима: {status}\nВыберите действие:",
        reply_markup=service_mode_keyboard()
    )

@dp.message(F.text == "🟢 Включить сервисный режим")
async def enable_service_mode(message: types.Message):
    """Включение сервисного режима"""
    if message.from_user.id not in ADMINS:
        return
    
    global SERVICE_MODE
    SERVICE_MODE = True
    await message.answer("✅ Сервисный режим включен", 
                        reply_markup=admin_panel_keyboard())

@dp.message(F.text == "🔴 Выключить сервисный режим")
async def disable_service_mode(message: types.Message):
    """Выключение сервисного режима"""
    if message.from_user.id not in ADMINS:
        return
    
    global SERVICE_MODE
    SERVICE_MODE = False
    await message.answer("✅ Сервисный режим выключен", 
                        reply_markup=admin_panel_keyboard())


# ===================== ЗАПУСК ПРИЛОЖЕНИЯ =====================
async def scheduled_cache_update():
    """Плановое обновление кэша"""
    while True:
        await asyncio.sleep(3600 * 12)  # Каждые 12 часов
        try:
            await preload_cache()
            logging.info("✅ Кэш успешно обновлен по расписанию")
        except Exception as e:
            logging.error(f"Ошибка обновления кэша: {str(e)}")

async def startup():
    """Инициализация при запуске"""
    logging.info("🟢 Бот запускается...")
    try:  
        await preload_cache()
        asyncio.create_task(scheduled_cache_update())
        asyncio.create_task(state_cleanup_task())
        asyncio.create_task(check_deadlines())
        asyncio.create_task(run_reminder_task(bot))
        worker_task = asyncio.create_task(process_order_queue(bot))
        logging.info("✅ Фоновый обработчик очереди заказов запущен.")
        logging.info("✅ Кэш загружен, задачи запущены")

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        startup_message = f"✅ Бот успешно перезапущен и работает.\nВремя запуска: {current_time}"

        for admin_id in ADMINS:
            try:
                await bot.send_message(chat_id=admin_id, text=startup_message)
                logging.info(f"✅ Уведомление о запуске отправлено админу {admin_id}")
            except Exception as e:
                logging.error(f"❌ Не удалось отправить уведомление админу {admin_id}: {e}")

    
    except Exception as e:
        logging.critical(f"🚨 Критическая ошибка запуска: {str(e)}")
        raise

async def shutdown():
    """Завершение работы"""
    try:
        await bot.session.close()
        await dp.storage.close()
        logging.info("✅ Ресурсы успешно освобождены")
    except Exception as e:
        logging.error(f"Ошибка при завершении: {str(e)}")

async def main():
    """Главная функция запуска"""
    try:
        await startup()
        initialize_approval_requests_table()
        initialize_order_queue_table()
        logging.info("✅ Бот запущен в режиме поллинга")
        await dp.start_polling(bot, skip_updates=True)
    except KeyboardInterrupt:
        logging.info("🛑 Бот остановлен пользователем")
    except Exception as e:
        # Ловим только критические ошибки запуска (не из обработчиков)
        logging.critical(f"🚨 Критическая ошибка запуска: {str(e)}\n{traceback.format_exc()}")
        # Уведомляем администраторов
        for admin_id in ADMINS:
            try:
                await bot.send_message(
                    admin_id,
                    f"🚨 Бот упал при запуске:\n{str(e)}\n\n{traceback.format_exc()[:3000]}"
                )
            except Exception:
                pass
    finally:
        await shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🛑 Бот остановлен пользователем")
    except Exception as e:
        logging.critical(f"🚨 Критическая ошибка: {str(e)}")
    finally:
        asyncio.run(shutdown())
