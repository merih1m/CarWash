import asyncio
import asyncpg
import os
import re
from aiogram import Bot, Dispatcher, Router, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from datetime import datetime, timedelta
from dotenv import load_dotenv

API_TOKEN = os.getenv("API_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
BUFFER_MINUTES = 1


if not API_TOKEN:
    raise RuntimeError("API_TOKEN відсутній у .env")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL відсутній у .env")

print("API_TOKEN:", "***" if API_TOKEN else None)
print("DATABASE_URL:", "***" if DATABASE_URL else None)

# --- Aiogram ---
bot = Bot(token=API_TOKEN)
dp = Dispatcher()
router = Router()

# --- Константи ---
MAIN_ADMIN_ID = 863294823
user_booking: dict[int, dict] = {}

# --- Пул з'єднань PostgreSQL ---
pool: asyncpg.Pool | None = None

# ---------- ІНІЦІАЛІЗАЦІЯ БД ----------
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS programs (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE,
    duration INTEGER NOT NULL,
    price NUMERIC(12,2) DEFAULT 0,
    description TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS admins (
    user_id BIGINT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    phone_number TEXT,
    first_name TEXT,
    last_name TEXT,
    registered_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bookings (
    id SERIAL PRIMARY KEY,
    user_id BIGINT,
    username TEXT,
    phone_number TEXT,
    program_id INTEGER REFERENCES programs(id) ON DELETE SET NULL,
    car_number TEXT,
    booking_datetime TIMESTAMP WITHOUT TIME ZONE,
    status TEXT DEFAULT 'scheduled',        -- scheduled | in_progress | finished
    actual_start TIMESTAMP,
    actual_end TIMESTAMP
);

-- Міграції для старих БД (дружні до повторного запуску)
ALTER TABLE programs
    ADD COLUMN IF NOT EXISTS price NUMERIC(12,2) DEFAULT 0;
ALTER TABLE programs
    ADD COLUMN IF NOT EXISTS description TEXT DEFAULT '';
"""

async def init_db():
    async with pool.acquire() as conn:
        # Виконуємо як один скрипт
        await conn.execute(CREATE_TABLES_SQL)

# ---------- ХЕЛПЕРИ ДЛЯ БД ----------
async def db_fetch(query: str, *args):
    async with pool.acquire() as conn:
        return await conn.fetch(query, *args)

async def db_fetchrow(query: str, *args):
    async with pool.acquire() as conn:
        return await conn.fetchrow(query, *args)

async def db_execute(query: str, *args):
    async with pool.acquire() as conn:
        return await conn.execute(query, *args)

async def save_user(user: types.User, phone_number: str | None = None):
    await db_execute(
        """
        INSERT INTO users (user_id, username, phone_number, first_name, last_name)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (user_id) DO UPDATE
        SET username = EXCLUDED.username,
            phone_number = COALESCE(EXCLUDED.phone_number, users.phone_number),
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name
        """,
        user.id,
        user.username,
        phone_number,
        user.first_name,
        user.last_name
    )
# ---------- ДОПОМІЖНЕ ----------
async def get_programs():
    rows = await db_fetch(
        "SELECT id, name, duration, price, description FROM programs ORDER BY id"
    )
    # перетворимо у звичайні tuples
    return [(r["id"], r["name"], r["duration"], float(r["price"] or 0), r["description"] or "") for r in rows]

async def is_admin(user_id: int) -> bool:
    if user_id == MAIN_ADMIN_ID:
        return True
    row = await db_fetchrow("SELECT 1 FROM admins WHERE user_id=$1", user_id)
    return bool(row)


async def notify_next_after_buffer(finished_booking_id: int):
    finished_booking = await db_fetchrow(
        "SELECT booking_datetime FROM bookings WHERE id=$1", finished_booking_id
    )
    if not finished_booking:
        return

    next_booking = await db_fetchrow(
        """
        SELECT b.id, b.user_id, b.car_number, b.booking_datetime
        FROM bookings b
        WHERE b.status='scheduled' AND b.booking_datetime > $1
        ORDER BY b.booking_datetime ASC, b.id ASC
        LIMIT 1
        """,
        finished_booking["booking_datetime"]
    )

    if not next_booking:
        return

    # ✅ Перевіряємо, що це той самий день
    if next_booking["booking_datetime"].date() != finished_booking["booking_datetime"].date():
        return

    notify_time = finished_booking["booking_datetime"] + timedelta(minutes=BUFFER_MINUTES)
    now = datetime.now()
    delay_seconds = max((notify_time - now).total_seconds(), 0)

    async def delayed_notify():
        await asyncio.sleep(delay_seconds)
        await send_notify(
            next_booking["user_id"],
            f"🚗 Ваша мийка (ID {next_booking['id']}, авто {next_booking['car_number']}) "
            f"може розпочатися раніше. Будь ласка, приїжджайте за можливості."
        )

    asyncio.create_task(delayed_notify())


async def send_notify(user_id: int, text: str):
        try:
            await bot.send_message(user_id, text)
        except Exception as e:
            print(f"Помилка надсилання повідомлення користувачу {user_id}: {e}")


async def get_available_hours(program_id: int, booking_date: datetime.date):
    # 1) Тривалість програми
    dur_row = await db_fetchrow("SELECT duration FROM programs WHERE id=$1", program_id)
    if not dur_row:
        return []
    duration = int(dur_row["duration"] or 0)

    # 2) Робочі години
    WORK_START_HOUR = 9
    WORK_END_HOUR = 21  # кінець робочого дня (початок слота < 19:00)
    day_start = datetime.combine(booking_date, datetime.min.time())
    work_start = day_start + timedelta(hours=WORK_START_HOUR)
    work_end = day_start + timedelta(hours=WORK_END_HOUR)

    # 3) Мінімально допустимий старт (щоб не пропонувати минуле)
    now = datetime.now()
    min_allowed_start = work_start
    if booking_date == now.date():
        min_allowed_start = max(work_start, now + timedelta(minutes=BUFFER_MINUTES))

    # 4) Кандидати (починаємо кожну годину, як і було)
    candidate_starts = [
        day_start + timedelta(hours=h)
        for h in range(WORK_START_HOUR, WORK_END_HOUR)  # 9:00..18:00 включно
    ]

    # 5) Бронювання цього дня (для перевірки перетинів)
    booked = await db_fetch(
        """
        SELECT b.booking_datetime AS bdt, p.duration AS pdur, b.status, b.actual_start, b.actual_end
        FROM bookings b
        LEFT JOIN programs p ON p.id = b.program_id
        WHERE b.booking_datetime::date = $1
        """,
        booking_date,
    )

    available = []
    for start in candidate_starts:
        # пропускаємо те, що раніше за мінімально допустимий старт
        if start < min_allowed_start:
            continue

        end = start + timedelta(minutes=duration)

        # не дозволяємо слоти, які закінчуються після кінця робочого дня
        if end > work_end:
            continue

        # перевірка перетинів з урахуванням статусів і буфера
        conflict = False
        for rec in booked:
            if rec["status"] in ("in_progress", "finished"):
                b_start = rec["actual_start"] or rec["bdt"]
                b_end = rec["actual_end"] or (b_start + timedelta(minutes=int(rec["pdur"] or 0)))
            else:
                b_start = rec["bdt"]
                b_end = b_start + timedelta(minutes=int(rec["pdur"] or 0))

            # буфер навколо зайнятого інтервалу
            b_start -= timedelta(minutes=BUFFER_MINUTES)
            b_end += timedelta(minutes=BUFFER_MINUTES)

            if not (end <= b_start or start >= b_end):
                conflict = True
                break

        if not conflict:
            available.append(start.strftime("%H:%M"))

    return available



def generate_date_buttons(days_ahead=7):
    today = datetime.today().date()
    buttons = [[KeyboardButton(text=(today + timedelta(days=i)).strftime("%d.%m.%Y"))] for i in range(days_ahead)]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

# --- Головне меню ---
main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🧾 Програми"), KeyboardButton(text="📝 Записати авто")],
        [KeyboardButton(text="📋 Мої записи")],
    ],
    resize_keyboard=True
)
@router.message(lambda m: m.text == "ℹ️ Допомога")
async def btn_help(message: types.Message):
    await help_command(message)

@router.message(lambda m: m.text == "🧾 Програми")
async def btn_programs(message: types.Message):
    await show_programs(message)

@router.message(lambda m: m.text == "📝 Записати авто")
async def btn_book(message: types.Message):
    await book_program(message)

@router.message(lambda m: m.text == "📋 Мої записи")
async def btn_my_bookings(message: types.Message):
    await my_bookings(message)


# ---------- КОМАНДИ ----------
@router.message(Command("start"))
async def start(message: types.Message):
    await save_user(message.from_user)
    await message.answer(
        "Привіт! Я бот для запису на мийку. Використай кнопки нижче або команду /help.",
        reply_markup=main_menu
    )



@router.message(Command("help"))
async def help_command(message: types.Message):
    base_text = (
        "📌 Доступні команди:\n\n"
        "/start - почати спілкування\n"
        "/help - список команд\n"
        "/programs - програми мийки\n"
        "/book - записати авто\n"
        "/my_bookings - перегляд активних записів авто\n"
    )
    admin_text = ""
    if await is_admin(message.from_user.id):
        admin_text = (
            "\n🛠 Адмін-команди:\n"
            "/show_booking [дата|user_id|номер авто] - показати бронювання\n"
            "/start_wash <ID> - почати мийку \n"
            "/finish_wash <ID> - завершить мийку\n"
            "/add_program <назва> <год:хв:сек> <ціна> <опис>\n"
            "/edit <ID> [<dd.mm.yyyy>] [<HH:MM>] [status <scheduled|in_progress|finished>]\n"
            "/delete <ID> - видалити бронювання\n"
            "/edit_program <id> <name|duration|price|description> <нове значення>\n"
            "/show_statistic <дата_початку> [<дата_кінця>]\n"
            "/add_admin <user_id>\n"
            "/del_admin <user_id>\n"
            "/admins - список адмінів\n"
        )
    await message.answer(base_text + admin_text)

@router.message(Command("users"))
async def list_users(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Немає прав")
        return

    rows = await db_fetch("SELECT user_id, username, phone_number, first_name, last_name, registered_at FROM users ORDER BY registered_at DESC")

    if not rows:
        await message.answer("📭 Немає зареєстрованих користувачів")
        return

    text = "📋 Користувачі:\n\n"
    for r in rows[:50]:  # обмежимо вивід, щоб не було занадто довгого тексту
        text += (
            f"🆔 {r['user_id']} | @{r['username'] or '—'}\n"
            f"👤 {r['first_name'] or ''} {r['last_name'] or ''}\n"
            f"📞 {r['phone_number'] or '—'}\n"
            f"📅 {r['registered_at'].strftime('%d.%m.%Y %H:%M')}\n"
            f"-----------------------------\n"
        )

    await message.answer(text)
@router.message(Command("programs"))
async def show_programs(message: types.Message):
    programs = await get_programs()
    if not programs:
        await message.answer("Програми ще не додані.")
        return

    text = "Програми мийки:\n\n"
    for p in programs:
        program_id, name, duration, price, description = p
        hours = duration // 60
        minutes = duration % 60
        if hours > 0:
            time_str = f"{hours} год {'{} хв'.format(minutes) if minutes > 0 else ''}"
        else:
            time_str = f"{minutes} хв"
        text += (
            f"{program_id} - {name}\n"
            f"🕒 {time_str} | 💵 {price:.2f} грн\n"
            f"📄 {description}\n"
            f"-----------------------------\n"
        )
    await message.answer(text)

@router.message(Command("my_bookings"))
async def my_bookings(message: types.Message):
    user_id = message.from_user.id

    rows = await db_fetch(
        """
        SELECT b.id, p.name AS program_name, b.car_number, b.booking_datetime, b.status
        FROM bookings b
        LEFT JOIN programs p ON p.id = b.program_id
        WHERE b.user_id=$1 AND b.status IN ('scheduled', 'in_progress')
        ORDER BY b.booking_datetime ASC
        """,
        user_id
    )

    if not rows:
        await message.answer("📭 У вас немає активних записів.")
        return

    text = "📋 Ваші активні записи:\n\n"
    for r in rows:
        booking_time = r["booking_datetime"].strftime("%d.%m.%Y %H:%M")
        text += (
            f"🆔 ID: {r['id']}\n"
            f"🧾 Програма: {r['program_name']}\n"
            f"🚗 Авто: {r['car_number']}\n"
            f"📅 Час: {booking_time}\n"
            f"📌 Статус: {r['status']}\n"
            f"-----------------------------\n"
        )

    await message.answer(text)


@router.message(Command("start_wash"))
async def start_wash(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Немає прав")
        return

    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("⚠ Використання: /start_wash <ID>")
        return

    booking_id = int(parts[1])
    row = await db_fetchrow("SELECT status FROM bookings WHERE id=$1", booking_id)
    if not row:
        await message.answer("❌ Такого бронювання не існує")
        return

    status = row["status"]
    if status == "in_progress":
        await message.answer("⚠ Мийка вже почата")
        return
    elif status == "finished":
        await message.answer("⚠ Мийка вже завершена")
        return

    # Починаємо мийку
    await db_execute(
        "UPDATE bookings SET status='in_progress', actual_start=NOW() WHERE id=$1",
        booking_id
    )
    await message.answer(f"✅ Мийка {booking_id} почалась!")

@router.message(Command("finish_wash"))
async def finish_wash(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Немає прав")
        return

    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("⚠ Використання: /finish_wash <ID>")
        return

    booking_id = int(parts[1])
    row = await db_fetchrow("SELECT status FROM bookings WHERE id=$1", booking_id)
    if not row:
        await message.answer("❌ Такого бронювання не існує")
        return

    status = row["status"]
    if status == "finished":
        await message.answer("⚠ Мийка вже завершена")
        return

    # Якщо мийка ще не стартувала, стартуємо автоматично
    await db_execute(
        """
        UPDATE bookings
        SET status='finished',
            actual_start=COALESCE(actual_start, NOW()),
            actual_end=NOW()
        WHERE id=$1
        """,
        booking_id
    )
    await message.answer(f"✅ Мийка {booking_id} завершена!")

    # Повідомлення наступних у черзі після BUFFER
    await notify_next_after_buffer(booking_id)


@router.message(Command("show_statistic"))
async def show_statistic(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Немає прав")
        return

    args = message.text.split()
    start_date = None
    end_date = None

    try:
        if len(args) == 1:
            # Без аргументів -> сьогодні
            start_date = datetime.today().date()
            end_date = start_date
        elif len(args) == 2:
            # тільки початкова дата
            start_date = datetime.strptime(args[1], "%d.%m.%Y").date()
            end_date = datetime.today().date()
        elif len(args) >= 3:
            start_date = datetime.strptime(args[1], "%d.%m.%Y").date()
            end_date = datetime.strptime(args[2], "%d.%m.%Y").date()
    except Exception:
        await message.answer("⚠ Використання: /show_statistic <дата_початку> [дата_кінця]\nПриклад: /show_statistic 01.08.2025 25.08.2025")
        return

    rows = await db_fetch(
        """
        SELECT p.name, COUNT(*) AS cnt, SUM(p.price) AS total
        FROM bookings b
        LEFT JOIN programs p ON p.id = b.program_id
        WHERE b.booking_datetime::date BETWEEN $1 AND $2
        GROUP BY p.name
        ORDER BY cnt DESC
        """,
        start_date, end_date
    )

    total_count = sum(r["cnt"] for r in rows)
    total_sum = sum(float(r["total"] or 0) for r in rows)

    if not rows:
        await message.answer(f"📭 Немає бронювань з {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}")
        return

    text = (
        f"📊 Статистика з {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}\n\n"
        f"🔢 Всього записів: {total_count}\n"
        f"💵 Загальна сума: {total_sum:.2f} грн\n\n"
        f"Розбивка по програмах:\n"
    )

    for r in rows:
        text += f"▫ {r['name']}: {r['cnt']} раз(ів), {float(r['total'] or 0):.2f} грн\n"

    await message.answer(text)


@router.message(Command("add_program"))
async def add_program(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Немає прав")
        return

    # Очікуємо: /add_program <назва> <год:хв:сек> <ціна> <опис>
    parts = message.text.split(maxsplit=4)
    if len(parts) < 5:
        await message.answer("⚠ Використання: /add_program <назва> <год:хв:сек> <ціна> <опис>")
        return

    name = parts[1]
    duration_str = parts[2]
    price_str = parts[3]
    description = parts[4]

    try:
        h, m, s = map(int, duration_str.split(":"))
        duration_minutes = h * 60 + m + s // 60
        price = float(price_str)
    except Exception:
        await message.answer("❌ Невірний формат часу або ціни")
        return

    try:
        await db_execute(
            "INSERT INTO programs (name, duration, price, description) VALUES ($1, $2, $3, $4)",
            name, duration_minutes, price, description
        )
        await message.answer(f"✅ Додано '{name}' ({duration_minutes} хв, {price:.2f} грн)\n📄 {description}")
    except asyncpg.UniqueViolationError:
        await message.answer("❌ Програма вже існує")
    except Exception as e:
        await message.answer(f"⚠ Помилка додавання: {e}")

@router.message(Command("edit_program"))
async def edit_program(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Немає прав")
        return

    parts = message.text.split(maxsplit=3)
    if len(parts) < 4:
        await message.answer("⚠ Використання: /edit_program <id> <name|duration|price|description> <нове значення>")
        return

    try:
        program_id = int(parts[1])
        field = parts[2].lower()
        new_value = parts[3]
    except Exception:
        await message.answer("❌ Невірний формат")
        return

    if field not in ["name", "duration", "price", "description"]:
        await message.answer("❌ Можна змінювати лише: name, duration, price, description")
        return

    try:
        if field == "duration":
            h, m, s = map(int, new_value.split(":"))
            new_value_casted = h * 60 + m + s // 60
        elif field == "price":
            new_value_casted = float(new_value)
        else:
            new_value_casted = new_value

        await db_execute(f"UPDATE programs SET {field}=$1 WHERE id=$2", new_value_casted, program_id)
        await message.answer(f"✏ Програму {program_id} змінено: {field} = {new_value_casted}")
    except Exception as e:
        await message.answer(f"⚠ Помилка: {e}")

@router.message(Command("add_admin"))
async def add_admin(message: types.Message):
    if message.from_user.id != MAIN_ADMIN_ID:
        await message.answer("❌ Лише головний адмін може додавати адмінів")
        return

    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("⚠ Використання: /add_admin <user_id>")
        return

    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("❌ user_id має бути числом")
        return

    try:
        await db_execute("INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", user_id)
        await message.answer(f"✅ {user_id} доданий у адміни")
    except Exception as e:
        await message.answer(f"⚠ Помилка: {e}")

@router.message(Command("del_admin"))
async def del_admin(message: types.Message):
    if message.from_user.id != MAIN_ADMIN_ID:
        await message.answer("❌ Лише головний адмін може видаляти адмінів")
        return

    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("⚠ Використання: /del_admin <user_id>")
        return

    try:
        target_id = int(parts[1])
    except ValueError:
        await message.answer("❌ user_id має бути числом")
        return

    await db_execute("DELETE FROM admins WHERE user_id=$1", target_id)
    await message.answer(f"🗑 {target_id} видалений з адмінів")

@router.message(Command("admins"))
async def list_admins(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Немає прав")
        return

    rows = await db_fetch("SELECT user_id FROM admins ORDER BY user_id")
    admins_ids = [MAIN_ADMIN_ID] + [r["user_id"] for r in rows]

    text = "📋 Адміни:\n"
    for admin_id in admins_ids:
        try:
            user = await bot.get_chat(admin_id)
            username = f"@{user.username}" if user.username else "Не вказано"
        except Exception:
            username = "Не вказано"
        text += f"{admin_id} | {username}\n"

    await message.answer(text)

@router.message(Command("show_booking"))
async def show_booking(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Немає прав")
        return

    parts = message.text.split(maxsplit=1)
    filter_clause = ""
    params = []
    include_finished = False

    if len(parts) > 1:
        arg = parts[1]
        include_finished = True  # якщо вказаний параметр → показуємо finished теж

        # Спробуємо розпізнати дату (dd.mm.yyyy)
        try:
            date = datetime.strptime(arg, "%d.%m.%Y").date()
            filter_clause = "AND b.booking_datetime::date = $1"
            params.append(date)
        except ValueError:
            # Якщо це число → вважаємо user_id
            if arg.isdigit():
                filter_clause = "AND b.user_id = $1"
                params.append(int(arg))
            else:
                # Інакше фільтруємо по номеру авто (LIKE)
                filter_clause = "AND b.car_number ILIKE $1"
                params.append(f"%{arg}%")

    # якщо параметрів нема → показуємо лише активні
    status_condition = "" if include_finished else "AND b.status != 'finished'"

    query = f"""
        SELECT b.id, b.user_id, b.username, b.phone_number, p.name AS program_name,
               b.car_number, b.booking_datetime, b.status, b.actual_start, b.actual_end
        FROM bookings b
        LEFT JOIN programs p ON p.id = b.program_id
        WHERE TRUE {status_condition} {filter_clause}
        ORDER BY
            CASE WHEN b.status='in_progress' THEN 0 ELSE 1 END,
            b.booking_datetime ASC
    """

    rows = await db_fetch(query, *params)

    if not rows:
        await message.answer("📭 Немає бронювань за цим фільтром")
        return

    text = "📋 Бронювання:\n\n"
    for r in rows:
        booking_time = r["booking_datetime"]
        status = r["status"]
        actual_start = r["actual_start"].strftime("%H:%M") if r["actual_start"] else "—"
        actual_end = r["actual_end"].strftime("%H:%M") if r["actual_end"] else "—"

        text += (
            f"ID: {r['id']} | Статус: {status}\n"
            f"👤 UserID: {r['user_id']} | @{r['username']}\n"
            f"📞 {r['phone_number']}\n"
            f"🚗 {r['car_number']}\n"
            f"📅 Заплановано: {booking_time.strftime('%d.%m.%Y %H:%M')}\n"
            f"🕒 Початок: {actual_start} | Кінець: {actual_end}\n"
            f"🧾 Програма: {r['program_name']}\n"
            f"--------------------------------------------\n"
        )

    await message.answer(text)

@router.message(Command("delete"))
async def delete_booking(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Немає прав")
        return

    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("⚠ Використання: /delete <ID>")
        return

    booking_id = int(parts[1])
    row = await db_fetchrow("SELECT id, user_id, car_number FROM bookings WHERE id=$1", booking_id)
    if not row:
        await message.answer("❌ Такого бронювання не існує")
        return

    await db_execute("DELETE FROM bookings WHERE id=$1", booking_id)
    await message.answer(f"🗑 Бронювання {booking_id} видалено")

    # 🔔 Повідомляємо користувача
    await send_notify(
        row["user_id"],
        f"❌ Ваше бронювання 🚗 {row['car_number']} було скасовано адміністратором."
    )

@dp.message(Command("edit"))
async def cmd_edit(message: types.Message):
    try:
        parts = message.text.split()

        if len(parts) < 2:
            await message.answer("❌ Використання: /edit <id> [<dd.mm.yyyy>] [<HH:MM>] [status <scheduled|in_progress|finished>]")
            return

        booking_id = int(parts[1])
        booking = await db_fetchrow("SELECT * FROM bookings WHERE id=$1", booking_id)

        if not booking:
            await message.answer(f"❌ Замовлення #{booking_id} не знайдено.")
            return

        new_date = None
        new_time = None
        new_status = None

        # --- парсимо аргументи ---
        i = 2
        while i < len(parts):
            part = parts[i]

            # статус
            if part.lower() == "status":
                if i + 1 >= len(parts):
                    await message.answer("❌ Вкажіть статус після 'status'")
                    return
                candidate = parts[i + 1].lower()
                if candidate not in ("scheduled", "in_progress", "finished"):
                    await message.answer("❌ Невірний статус! Використовуйте: scheduled, in_progress, finished")
                    return
                new_status = candidate
                i += 2
                continue

            # дата
            try:
                parsed_date = datetime.strptime(part, "%d.%m.%Y").date()
                new_date = parsed_date
                i += 1
                continue
            except ValueError:
                pass

            # час
            try:
                time_str = part.replace(".", ":")  # підтримка 17.41 і 17:41
                parsed_time = datetime.strptime(time_str, "%H:%M").time()
                new_time = parsed_time
                i += 1
                continue
            except ValueError:
                pass

            await message.answer(f"❌ Невідомий параметр: {part}")
            return

        # --- оновлюємо дані ---
        updates = []
        params = []

        if new_date or new_time:
            current_dt = booking["booking_datetime"]

            if new_date and new_time:
                updated_dt = datetime.combine(new_date, new_time)
            elif new_date:
                updated_dt = datetime.combine(new_date, current_dt.time())
            elif new_time:
                updated_dt = datetime.combine(current_dt.date(), new_time)
            else:
                updated_dt = current_dt

            updates.append("booking_datetime=$1")
            params.append(updated_dt)

        if new_status:
            updates.append(f"status=${len(params)+1}")
            params.append(new_status)

        if not updates:
            await message.answer("ℹ️ Нічого не змінено. Додайте дату, час або статус.")
            return

        query = f"UPDATE bookings SET {', '.join(updates)} WHERE id=${len(params)+1}"
        params.append(booking_id)

        await db_execute(query, *params)

        # повідомлення користувачу
        booking = await db_fetchrow("SELECT user_id FROM bookings WHERE id=$1", booking_id)
        if booking:
            text_notify = f"🔔 Ваше замовлення #{booking_id} було змінено."
            if new_date or new_time:
                text_notify += f"\n📅 Нова дата/час: {updated_dt.strftime('%d.%m.%Y %H:%M')}"
            if new_status:
                text_notify += f"\n📌 Новий статус: {new_status}"
            await send_notify(booking["user_id"], text_notify)

        # відповідь адміна
        text_admin = f"✅ Замовлення #{booking_id} змінено."
        if new_date or new_time:
            text_admin += f"\n📅 Нова дата/час: {updated_dt.strftime('%d.%m.%Y %H:%M')}"
        if new_status:
            text_admin += f"\n📌 Новий статус: {new_status}"

        await message.answer(text_admin)

    except Exception as e:
        await message.answer(f"⚠️ Помилка: {e}")

# ---------- БРОНЮВАННЯ ДЛЯ КОРИСТУВАЧІВ ----------
@router.message(Command("book"))
async def book_program(message: types.Message):
    programs = await get_programs()
    if not programs:
        await message.answer("Програми ще не додані.")
        return
    buttons = [[KeyboardButton(text=f"{p[0]} - {p[1]}")] for p in programs]
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    await message.answer("Оберіть програму:", reply_markup=keyboard)
    user_booking[message.from_user.id] = {}

@router.message()
async def process_booking(message: types.Message):
    user_id = message.from_user.id
    if user_id not in user_booking:
        return
    data = user_booking[user_id]


    # 1) Програма
    if "program_id" not in data:
        try:
            data["program_id"] = int(message.text.split(" - ")[0])
        except Exception:
            await message.answer("Оберіть програму кнопкою.")
            return
        await message.answer("Оберіть дату:", reply_markup=generate_date_buttons())
        return

    # 2) Дата
    if "booking_date" not in data:
        try:
            date = datetime.strptime(message.text, "%d.%m.%Y").date()
            if date < datetime.today().date():
                raise ValueError
            data["booking_date"] = date
        except Exception:
            await message.answer("❌ Невірна дата")
            return

        hours = await get_available_hours(data["program_id"], data["booking_date"])
        if not hours:
            await message.answer("❌ Немає вільних годин, оберіть іншу дату", reply_markup=generate_date_buttons())
            data.pop("booking_date")
            return

        buttons = [[KeyboardButton(text=h)] for h in hours]
        await message.answer("Оберіть годину:", reply_markup=ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True))
        return

    # 3) Час
    if "booking_time" not in data:
        hours = await get_available_hours(data["program_id"], data["booking_date"])
        if message.text not in hours:
            await message.answer("❌ Ця година вже зайнята")
            return
        data["booking_time"] = message.text
        await message.answer("Введіть номер авто:", reply_markup=ReplyKeyboardRemove())
        return

    # 4) Авто
    if "car_number" not in data:
        if not re.match(r"^[A-ZА-ЯІЇЄ]{2}\d{4}[A-ZА-ЯІЇЄ]{2}$", message.text.upper()):
            await message.answer("❌ Невірний формат номера. Приклад: AA1234BB")
            return
        data["car_number"] = message.text.upper()
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📞 Поділитися номером", request_contact=True)]],
            resize_keyboard=True
        )
        await message.answer("Надішліть свій номер телефону:", reply_markup=kb)
        return

    # 5) Телефон
    if "phone_number" not in data:
        phone_number = None

        # Якщо користувач поділився контактом
        if message.contact and message.contact.phone_number:
            phone_number = message.contact.phone_number

        # Якщо користувач ввів вручну
        elif message.text:
            digits = "".join([c for c in message.text if c.isdigit()])
            if len(digits) >= 9:  # мінімальна довжина
                if not message.text.startswith("+"):
                    phone_number = "+" + digits
                else:
                    phone_number = message.text
            else:
                kb = ReplyKeyboardMarkup(
                    keyboard=[[KeyboardButton(text="📞 Поділитися номером", request_contact=True)]],
                    resize_keyboard=True
                )
                await message.answer("❌ Невірний формат номера. Введіть ще раз або скористайтесь кнопкою.", reply_markup=kb)
                return


        if not phone_number:
            kb = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="📞 Поділитися номером", request_contact=True)]],
                resize_keyboard=True
            )
            await message.answer("Будь ласка, надішліть свій номер телефону (через кнопку або вручну):", reply_markup=kb)
            return

        # 🔹 Оновлюємо дані
        data["phone_number"] = phone_number
        await save_user(message.from_user, phone_number)

        booking_dt = datetime.combine(
            data["booking_date"],
            datetime.strptime(data["booking_time"], "%H:%M").time()
        )
        username = message.from_user.username or "Не вказано"

        await db_execute(
            """
            INSERT INTO bookings (user_id, username, phone_number, program_id, car_number, booking_datetime)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            user_id,
            username,
            data["phone_number"],
            data["program_id"],
            data["car_number"],
            booking_dt,
        )

        await message.answer(
            f"✅ Запис підтверджено:\n"
            f"📅 {data['booking_date']} ⏰ {data['booking_time']}\n"
            f"🚗 {data['car_number']}\n"
            f"📞 {data['phone_number']}",
            reply_markup=main_menu
        )
        user_booking.pop(user_id, None)
        return



# ---------- СТАРТ ----------
async def main():
    global pool
    # SSL для Supabase зазвичай не потрібен явно в URI, але якщо у вас вимагає — додайте ?sslmode=require
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    await init_db()
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
