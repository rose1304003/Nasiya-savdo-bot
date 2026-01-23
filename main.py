#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Nasiya Savdo Xizmatlari So'rovnoma Bot (v2)
✅ 3 languages: Uzbek (Latin), Russian, English
✅ Region buttons for Uzbekistan (paginated)
✅ PostgreSQL database (primary storage)
✅ CSV backup
✅ Optional Google Sheets integration
✅ Admin export: /export, /stats
✅ Based on Central Bank survey questionnaire
"""

import os
import csv
import tempfile
import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("nasiya_survey_bot")

# ---------------- Optional dotenv ----------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---------------- Google Sheets JSON setup (optional) ----------------
json_env = os.getenv("GOOGLE_SHEETS_JSON_CONTENT")
if json_env and not os.getenv("GOOGLE_SHEETS_JSON"):
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    tmp.write(json_env.encode("utf-8"))
    tmp.flush()
    os.environ["GOOGLE_SHEETS_JSON"] = tmp.name
    log.info("Wrote GOOGLE_SHEETS_JSON to temp file: %s", tmp.name)

# ---------------- Configuration ----------------
CSV_PATH = os.environ.get("CSV_PATH", "nasiya_survey_responses.csv")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")


def parse_admin_ids(raw: Optional[str]) -> List[int]:
    if not raw:
        return []
    out: List[int] = []
    for p in raw.split(","):
        p = p.strip()
        if not p:
            continue
        try:
            out.append(int(p))
        except ValueError:
            continue
    return out


ADMIN_IDS: List[int] = parse_admin_ids(os.getenv("ADMIN_IDS"))

# ---------------- PostgreSQL Database ----------------
db_pool = None


async def init_db():
    """Initialize PostgreSQL connection pool and create tables."""
    global db_pool

    if not DATABASE_URL:
        log.warning("DATABASE_URL not set, PostgreSQL disabled")
        return

    try:
        import asyncpg
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)

        async with db_pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS survey_responses (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    user_id BIGINT,
                    username VARCHAR(255),
                    language VARCHAR(10),

                    -- I. Respondent profile
                    region_city VARCHAR(255),
                    region_city_id VARCHAR(10),
                    age_group VARCHAR(50),
                    gender VARCHAR(20),
                    employment VARCHAR(100),
                    income VARCHAR(100),

                    -- II. Usage
                    freq_3m VARCHAR(50),
                    months_using INTEGER,
                    company_name VARCHAR(255),
                    avg_purchase VARCHAR(100),
                    product_types TEXT,

                    -- III. Multiple obligations
                    multi_company_use VARCHAR(10),
                    multi_company_debt VARCHAR(10),
                    income_share_percent INTEGER,
                    debt_burden_checked VARCHAR(10),
                    missed_payment VARCHAR(10),

                    -- IV. Transparency
                    total_cost_clear VARCHAR(10),
                    fees_explained VARCHAR(10),
                    schedule_given VARCHAR(10),

                    -- V. Difficulties
                    difficulty_reason VARCHAR(100),
                    borrowed_for_payments VARCHAR(10),
                    cut_essential_spending VARCHAR(10),
                    used_for_cash_need VARCHAR(10),

                    -- VI. Collection practices
                    contact_methods TEXT,
                    aggressive_collection VARCHAR(10),

                    -- VII. Complaints & trust
                    complaint_submitted VARCHAR(10),
                    complaint_resolved VARCHAR(10),
                    satisfaction_1_5 INTEGER,
                    recommend VARCHAR(10),

                    -- VIII. Financial awareness
                    read_contract VARCHAR(10),
                    know_limit VARCHAR(10),
                    impulse_buying VARCHAR(10),
                    need_stricter_regulation VARCHAR(50)
                )
            ''')

            # Create index for faster queries
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_survey_created_at
                ON survey_responses(created_at)
            ''')
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_survey_user_id
                ON survey_responses(user_id)
            ''')

        log.info("PostgreSQL initialized successfully")
    except ImportError:
        log.error("asyncpg not installed. Run: pip install asyncpg")
    except Exception as e:
        log.error(f"PostgreSQL init error: {e}")


async def save_to_db(data: Dict[str, Any]) -> bool:
    """Save survey response to PostgreSQL."""
    global db_pool

    if not db_pool:
        return False

    try:
        # Convert lists to semicolon-separated strings
        product_types = data.get("product_types", [])
        if isinstance(product_types, (list, set, tuple)):
            product_types = "; ".join(str(x) for x in product_types)

        contact_methods = data.get("contact_methods", [])
        if isinstance(contact_methods, (list, set, tuple)):
            contact_methods = "; ".join(str(x) for x in contact_methods)

        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO survey_responses (
                    user_id, username, language,
                    region_city, region_city_id, age_group, gender, employment, income,
                    freq_3m, months_using, company_name, avg_purchase, product_types,
                    multi_company_use, multi_company_debt, income_share_percent,
                    debt_burden_checked, missed_payment,
                    total_cost_clear, fees_explained, schedule_given,
                    difficulty_reason, borrowed_for_payments, cut_essential_spending,
                    used_for_cash_need,
                    contact_methods, aggressive_collection,
                    complaint_submitted, complaint_resolved, satisfaction_1_5, recommend,
                    read_contract, know_limit, impulse_buying, need_stricter_regulation
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                    $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                    $31, $32, $33, $34, $35
                )
            ''',
                data.get("user_id"),
                data.get("username"),
                data.get("language"),
                data.get("region_city"),
                data.get("region_city_id"),
                data.get("age_group"),
                data.get("gender"),
                data.get("employment"),
                data.get("income"),
                data.get("freq_3m"),
                data.get("months_using"),
                data.get("company_name"),
                data.get("avg_purchase"),
                product_types,
                data.get("multi_company_use"),
                data.get("multi_company_debt"),
                data.get("income_share_percent"),
                data.get("debt_burden_checked"),
                data.get("missed_payment"),
                data.get("total_cost_clear"),
                data.get("fees_explained"),
                data.get("schedule_given"),
                data.get("difficulty_reason"),
                data.get("borrowed_for_payments"),
                data.get("cut_essential_spending"),
                data.get("used_for_cash_need"),
                contact_methods,
                data.get("aggressive_collection"),
                data.get("complaint_submitted"),
                data.get("complaint_resolved"),
                data.get("satisfaction_1_5"),
                data.get("recommend"),
                data.get("read_contract"),
                data.get("know_limit"),
                data.get("impulse_buying"),
                data.get("need_stricter_regulation"),
            )
        return True
    except Exception as e:
        log.error(f"PostgreSQL save error: {e}")
        return False


async def get_stats() -> Dict[str, Any]:
    """Get survey statistics from PostgreSQL."""
    global db_pool

    if not db_pool:
        return {}

    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval('SELECT COUNT(*) FROM survey_responses')
            today = await conn.fetchval('''
                SELECT COUNT(*) FROM survey_responses
                WHERE created_at >= CURRENT_DATE
            ''')
            week = await conn.fetchval('''
                SELECT COUNT(*) FROM survey_responses
                WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
            ''')

            # Top regions
            regions = await conn.fetch('''
                SELECT region_city, COUNT(*) as cnt
                FROM survey_responses
                WHERE region_city IS NOT NULL
                GROUP BY region_city
                ORDER BY cnt DESC
                LIMIT 5
            ''')

            # Satisfaction average
            avg_satisfaction = await conn.fetchval('''
                SELECT ROUND(AVG(satisfaction_1_5)::numeric, 2)
                FROM survey_responses
                WHERE satisfaction_1_5 IS NOT NULL
            ''')

            return {
                "total": total or 0,
                "today": today or 0,
                "week": week or 0,
                "top_regions": [(r["region_city"], r["cnt"]) for r in regions],
                "avg_satisfaction": float(avg_satisfaction) if avg_satisfaction else 0,
            }
    except Exception as e:
        log.error(f"PostgreSQL stats error: {e}")
        return {}


async def export_db_to_csv() -> Optional[str]:
    """Export all PostgreSQL data to a CSV file."""
    global db_pool

    if not db_pool:
        return None

    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM survey_responses ORDER BY created_at')

            if not rows:
                return None

            export_path = "/tmp/survey_export.csv"
            with open(export_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                # Write headers
                writer.writerow(rows[0].keys())
                # Write data
                for row in rows:
                    writer.writerow(row.values())

            return export_path
    except Exception as e:
        log.error(f"PostgreSQL export error: {e}")
        return None


# ---------------- i18n ----------------
LANGS = {"uz": "O'zbek", "ru": "Русский", "en": "English"}

T = {
    "choose_lang": {
        "uz": "Tilni tanlang:",
        "ru": "Выберите язык:",
        "en": "Choose language:",
    },
    "start": {
        "uz": "Assalomu alaykum! 🏦\n\nNasiya savdo xizmatlari foydalanuvchilari uchun so'rovnomani boshlaymiz.",
        "ru": "Здравствуйте! 🏦\n\nНачнём опрос для пользователей услуг «Насия савдо».",
        "en": "Hello! 🏦\n\nLet's start the survey for users of installment trade services (Nasiya Savdo).",
    },
    "btn_start": {"uz": "Boshlash ✅", "ru": "Начать ✅", "en": "Start ✅"},
    "btn_done": {"uz": "Tayyor ✅", "ru": "Готово ✅", "en": "Done ✅"},
    "invalid": {
        "uz": "Noto'g'ri javob. Iltimos, tugmalar orqali tanlang yoki to'g'ri formatda kiriting.",
        "ru": "Некорректный ответ. Пожалуйста, выберите кнопкой или введите в правильном формате.",
        "en": "Invalid answer. Please use buttons or enter a valid value.",
    },
    "saved": {
        "uz": "Rahmat! So'rovnoma yakunlandi ✅\n\nSizning javoblaringiz muvaffaqiyatli saqlandi.",
        "ru": "Спасибо! Опрос завершён ✅\n\nВаши ответы успешно сохранены.",
        "en": "Thank you! The survey is completed ✅\n\nYour responses have been saved successfully.",
    },
    "saved_not_used": {
        "uz": "Katta rahmat! So'rov yakunlandi ✅",
        "ru": "Большое спасибо! Опрос завершён ✅",
        "en": "Thank you very much! The survey is completed ✅",
    },
    "export_only_admin": {
        "uz": "Kechirasiz, bu buyruq faqat adminlar uchun.",
        "ru": "Извините, команда только для админов.",
        "en": "Sorry, this command is for admins only.",
    },
    "no_data": {
        "uz": "Hali ma'lumot yo'q.",
        "ru": "Данных пока нет.",
        "en": "No data yet.",
    },
    "section_1": {
        "uz": "📋 **I. Respondent profili**",
        "ru": "📋 **I. Профиль респондента**",
        "en": "📋 **I. Respondent Profile**",
    },
    "section_2": {
        "uz": "📊 **II. Nasiya savdo xizmatlaridan foydalanish**",
        "ru": "📊 **II. Использование услуг «Насия савдо»**",
        "en": "📊 **II. Usage of Installment Trade Services**",
    },
    "section_3": {
        "uz": "💳 **III. Bir nechta majburiyatlar va ortiqcha qarzdorlik**",
        "ru": "💳 **III. Множественные обязательства и чрезмерная задолженность**",
        "en": "💳 **III. Multiple Obligations and Over-indebtedness**",
    },
    "section_4": {
        "uz": "🔍 **IV. Shaffoflik va tushunarlilik**",
        "ru": "🔍 **IV. Прозрачность и понятность**",
        "en": "🔍 **IV. Transparency and Clarity**",
    },
    "section_5": {
        "uz": "⚠️ **V. To'lov qiyinchiliklari va moliyaviy bosim**",
        "ru": "⚠️ **V. Трудности с платежами и финансовое давление**",
        "en": "⚠️ **V. Payment Difficulties and Financial Pressure**",
    },
    "section_6": {
        "uz": "📞 **VI. Qarzni undirish amaliyoti**",
        "ru": "📞 **VI. Практика взыскания долга**",
        "en": "📞 **VI. Debt Collection Practices**",
    },
    "section_7": {
        "uz": "📝 **VII. Shikoyatlar va ishonch**",
        "ru": "📝 **VII. Жалобы и доверие**",
        "en": "📝 **VII. Complaints and Trust**",
    },
    "section_8": {
        "uz": "🎓 **VIII. Moliyaviy xabardorlik va xulq-atvor**",
        "ru": "🎓 **VIII. Финансовая осведомлённость и поведение**",
        "en": "🎓 **VIII. Financial Awareness and Behavior**",
    },
}


def tr(lang: str, key: str) -> str:
    lang = lang if lang in LANGS else "uz"
    return T.get(key, {}).get(lang, T.get(key, {}).get("uz", key))


# ---------------- Uzbekistan regions (buttons) ----------------
UZB_REGIONS = [
    {"id": "qr", "uz": "Qoraqalpog'iston R.", "ru": "Республика Каракалпакстан", "en": "Republic of Karakalpakstan"},
    {"id": "an", "uz": "Andijon", "ru": "Андижанская", "en": "Andijan"},
    {"id": "bu", "uz": "Buxoro", "ru": "Бухарская", "en": "Bukhara"},
    {"id": "ji", "uz": "Jizzax", "ru": "Джизакская", "en": "Jizzakh"},
    {"id": "qa", "uz": "Qashqadaryo", "ru": "Кашкадарьинская", "en": "Kashkadarya"},
    {"id": "na", "uz": "Navoiy", "ru": "Навоийская", "en": "Navoi"},
    {"id": "nm", "uz": "Namangan", "ru": "Наманганская", "en": "Namangan"},
    {"id": "sa", "uz": "Samarqand", "ru": "Самаркандская", "en": "Samarkand"},
    {"id": "su", "uz": "Surxondaryo", "ru": "Сурхандарьинская", "en": "Surkhandarya"},
    {"id": "si", "uz": "Sirdaryo", "ru": "Сырдарьинская", "en": "Syrdarya"},
    {"id": "ta", "uz": "Toshkent vil.", "ru": "Ташкентская обл.", "en": "Tashkent Region"},
    {"id": "tk", "uz": "Toshkent shahri", "ru": "г. Ташкент", "en": "Tashkent City"},
    {"id": "fa", "uz": "Farg'ona", "ru": "Ферганская", "en": "Fergana"},
    {"id": "xo", "uz": "Xorazm", "ru": "Хорезмская", "en": "Khorezm"},
]


# ---------------- Google Sheets helper (optional) ----------------

def try_gs_save_row(
    spreadsheet_name: str,
    worksheet_name: str,
    row: Dict[str, Any],
    headers: List[str],
    keys: List[str],
) -> str:
    try:
        gs_path = os.environ.get("GOOGLE_SHEETS_JSON")
        if not gs_path:
            return "GOOGLE_SHEETS_JSON not set"

        import gspread
        from google.oauth2.service_account import Credentials

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(gs_path, scopes=scopes)
        gc = gspread.authorize(creds)

        try:
            sh = gc.open(spreadsheet_name)
        except gspread.SpreadsheetNotFound:
            sh = gc.create(spreadsheet_name)

        try:
            ws = sh.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=worksheet_name, rows=1000, cols=200)

        values = ws.get_all_values()
        if not values:
            ws.append_row(headers)

        row_data: List[str] = []
        for k in keys:
            v = row.get(k, "")
            if isinstance(v, (list, set, tuple)):
                v = "; ".join(str(x) for x in v)
            row_data.append("" if v is None else str(v))

        ws.append_row(row_data)
        return ""
    except Exception as e:
        log.error("Google Sheets error: %s", e)
        return str(e)


# ---------------- Survey definition ----------------
YESNO = {
    "uz": ["Ha", "Yo'q"],
    "ru": ["Да", "Нет"],
    "en": ["Yes", "No"],
}

# Options that indicate user never used nasiya services (for early termination)
NOT_USED_OPTIONS = {
    "uz": "Umuman foydalanmagan",
    "ru": "Не пользовался(ась)",
    "en": "Did not use",
}

# Updated SURVEY section - replace the existing SURVEY list in main.py

SURVEY: List[Dict[str, Any]] = [
    # ======== I. Respondent profile ========
    {
        "id": "_section_1",
        "kind": "section",
        "text": {"uz": "section_1", "ru": "section_1", "en": "section_1"},
    },
    {
        "id": "region_city",
        "kind": "region",
        "text": {
            "uz": "1️⃣ Yashash hududi (viloyat / shahar):",
            "ru": "1️⃣ Регион проживания (область / город):",
            "en": "1️⃣ Place of residence (region / city):",
        },
    },
    {
        "id": "age_group",
        "kind": "choice",
        "text": {
            "uz": "2️⃣ Yosh guruhi:",
            "ru": "2️⃣ Возрастная группа:",
            "en": "2️⃣ Age group:",
        },
        "options": {
            "uz": ["18 yoshgacha", "18–24", "25–34", "35–44", "45–54", "55 va undan yuqori"],
            "ru": ["до 18", "18–24", "25–34", "35–44", "45–54", "55 и старше"],
            "en": ["Under 18", "18–24", "25–34", "35–44", "45–54", "55 and above"],
        },
    },
    {
        "id": "gender",
        "kind": "choice",
        "text": {"uz": "3️⃣ Jins:", "ru": "3️⃣ Пол:", "en": "3️⃣ Gender:"},
        "options": {"uz": ["Erkak", "Ayol"], "ru": ["Мужчина", "Женщина"], "en": ["Male", "Female"]},
    },
    {
        "id": "employment",
        "kind": "choice",
        "text": {"uz": "4️⃣ Bandlik holati:", "ru": "4️⃣ Занятость:", "en": "4️⃣ Employment status:"},
        "options": {
            "uz": ["Ishlaydi (rasmiy)", "Ishlaydi (norasmiy)", "O'zini-o'zi band qilgan", "Talaba", "Nafaqada", "Ishsiz", "Boshqa"],
            "ru": ["Работаю (официально)", "Работаю (неофициально)", "Самозанятый(ая)", "Студент(ка)", "На пенсии", "Безработный(ая)", "Другое"],
            "en": ["Employed (formal)", "Employed (informal)", "Self-employed", "Student", "Retired", "Unemployed", "Other"],
        },
    },
    {
        "id": "income",
        "kind": "choice",
        "text": {"uz": "5️⃣ O'rtacha oylik daromadi:", "ru": "5️⃣ Средний ежемесячный доход:", "en": "5️⃣ Average monthly income:"},
        "options": {
            "uz": ["0–2 mln so'm", "2–5 mln so'm", "5–10 mln so'm", "10–20 mln so'm", "20 mln so'mdan yuqori"],
            "ru": ["0–2 млн сум", "2–5 млн сум", "5–10 млн сум", "10–20 млн сум", "более 20 млн сум"],
            "en": ["0–2 mln UZS", "2–5 mln UZS", "5–10 mln UZS", "10–20 mln UZS", "Above 20 mln UZS"],
        },
    },

    # ======== II. Usage ========
    {
        "id": "_section_2",
        "kind": "section",
        "text": {"uz": "section_2", "ru": "section_2", "en": "section_2"},
    },
    {
        "id": "ever_used",
        "kind": "choice",
        "text": {
            "uz": "6️⃣ Nasiya savdo xizmatidan foydalanganmisiz?",
            "ru": "6️⃣ Пользовались ли услугой «Насия савдо»?",
            "en": "6️⃣ Have you used installment trade services?",
        },
        "options": YESNO,
        "skip_if_not_used": True,
    },
    {
        "id": "freq_1y",
        "kind": "choice",
        "text": {
            "uz": "7️⃣ So'ngi 1 yilda nasiya savdo xizmatidan nechta marta foydalangansiz?",
            "ru": "7️⃣ Сколько раз за последний год пользовались услугой?",
            "en": "7️⃣ How many times in the last year have you used it?",
        },
        "options": {
            "uz": ["1 marta", "2–3 marta", "4–5 marta", "6 va undan ko'p"],
            "ru": ["1 раз", "2–3 раза", "4–5 раз", "6 и более"],
            "en": ["Once", "2–3 times", "4–5 times", "6 or more"],
        },
    },
    {
        "id": "usage_duration",
        "kind": "choice",
        "text": {
            "uz": "8️⃣ Nasiya savdo xizmatini qancha muddatga foydalangansiz?",
            "ru": "8️⃣ Как долго пользуетесь услугой?",
            "en": "8️⃣ How long have you been using it?",
        },
        "options": {
            "uz": ["1 oy", "3 oy", "6 oy", "9 oy", "12 oy", "18 oy", "24 oy", "24 oydan yuqori"],
            "ru": ["1 месяц", "3 месяца", "6 месяцев", "9 месяцев", "12 месяцев", "18 месяцев", "24 месяца", "более 24 месяцев"],
            "en": ["1 month", "3 months", "6 months", "9 months", "12 months", "18 months", "24 months", "Over 24 months"],
        },
    },
    {
        "id": "company_name",
        "kind": "text",
        "text": {
            "uz": "9️⃣ Qaysi nasiya savdo kompaniyalari xizmatlaridan foydalangansiz?",
            "ru": "9️⃣ Какими компаниями «Насия савдо» пользовались?",
            "en": "9️⃣ Which Nasiya Savdo companies have you used?",
        },
        "hint": {"uz": "Bir nechta kompaniya nomini yozish mumkin", "ru": "Можно указать несколько компаний", "en": "You can list multiple companies"},
    },
    {
        "id": "avg_purchase",
        "kind": "choice",
        "text": {"uz": "🔟 O'rtacha bitta xarid summasi (so'm):", "ru": "🔟 Средняя сумма одной покупки:", "en": "🔟 Average purchase amount:"},
        "options": {
            "uz": ["1 mln so'mgacha", "3 mln so'mgacha", "5 mln so'mgacha", "10 mln so'mgacha", "50 mln so'mgacha", "100 mln so'mgacha", "500 mln so'mdan ortiq"],
            "ru": ["до 1 млн", "до 3 млн", "до 5 млн", "до 10 млн", "до 50 млн", "до 100 млн", "более 500 млн"],
            "en": ["Up to 1 mln", "Up to 3 mln", "Up to 5 mln", "Up to 10 mln", "Up to 50 mln", "Up to 100 mln", "Above 500 mln"],
        },
    },
    {
        "id": "product_types",
        "kind": "multi",
        "max_select": 9,
        "text": {
            "uz": "1️⃣1️⃣ Nasiya savdo orqali asosan qaysi mahsulot/xizmatlarni xarid qilasiz?\n(bir nechta variant tanlash mumkin)",
            "ru": "1️⃣1️⃣ Какие товары/услуги покупаете чаще всего?\n(можно несколько)",
            "en": "1️⃣1️⃣ What do you mostly buy?\n(multiple choice allowed)",
        },
        "options": {
            "uz": ["Elektronika", "Kiyim-kechak", "Maishiy texnika", "Oziq-ovqat", "Qurilish mahsulotlari", "Sayohat / xizmatlar", "Avtomashina", "Ko'chmas mulk (turar / noturar joy)", "Boshqa"],
            "ru": ["Электроника", "Одежда", "Бытовая техника", "Продукты питания", "Строит. товары", "Путешествия / услуги", "Автомобиль", "Недвижимость", "Другое"],
            "en": ["Electronics", "Clothing", "Home appliances", "Food", "Construction goods", "Travel / services", "Car", "Real estate", "Other"],
        },
    },

    # ======== III. Multiple obligations / over-indebtedness ========
    {
        "id": "_section_3",
        "kind": "section",
        "text": {"uz": "section_3", "ru": "section_3", "en": "section_3"},
    },
    {
        "id": "multi_company_use",
        "kind": "choice",
        "text": {
            "uz": "1️⃣2️⃣ Bir vaqtning o'zida bir nechta nasiya savdo kompaniyasi xizmatidan foydalanasizmi?",
            "ru": "1️⃣2️⃣ Пользуетесь ли сразу несколькими компаниями?",
            "en": "1️⃣2️⃣ Do you use multiple companies at the same time?",
        },
        "options": YESNO,
    },
    {
        "id": "multi_company_debt",
        "kind": "choice",
        "text": {
            "uz": "1️⃣3️⃣ Hozirda bir nechta nasiya savdo kompaniyalari oldida qarzdorligingiz bormi?",
            "ru": "1️⃣3️⃣ Есть ли у вас долги перед несколькими компаниями?",
            "en": "1️⃣3️⃣ Do you currently have debts to multiple companies?",
        },
        "options": YESNO,
    },
    {
        "id": "income_share_percent",
        "kind": "percent",
        "text": {
            "uz": "1️⃣4️⃣ Nasiya savdo bo'yicha oylik to'lovlaringiz daromadingizning taxminan necha foizini tashkil etadi?",
            "ru": "1️⃣4️⃣ Какой примерно процент дохода уходит на ежемесячные платежи?",
            "en": "1️⃣4️⃣ Approx. what % of your income goes to monthly payments?",
        },
        "hint": {"uz": "0 dan 100 gacha son kiriting (%)", "ru": "Введите число 0–100 (%)", "en": "Enter a number 0–100 (%)"},
    },
    {
        "id": "debt_burden_checked",
        "kind": "choice",
        "text": {
            "uz": "1️⃣5️⃣ Nasiya savdo orqali mahsulot/xizmatlar xarid qilganingizda qarz yuki darajangiz hisobga olinganmi?",
            "ru": "1️⃣5️⃣ Учитывали ли вашу долговую нагрузку при покупке?",
            "en": "1️⃣5️⃣ Was your debt burden considered at purchase?",
        },
        "options": YESNO,
    },
    {
        "id": "missed_payment",
        "kind": "choice",
        "text": {
            "uz": "1️⃣6️⃣ Nasiya savdo bo'yicha to'lovni kechiktirgan yoki o'tkazib yuborgan holat bo'lganmi?",
            "ru": "1️⃣6️⃣ Были ли просрочки/пропуски платежей?",
            "en": "1️⃣6️⃣ Have you delayed or missed a payment?",
        },
        "options": YESNO,
    },

    # ======== IV. Transparency ========
    {
        "id": "_section_4",
        "kind": "section",
        "text": {"uz": "section_4", "ru": "section_4", "en": "section_4"},
    },
    {
        "id": "total_cost_clear",
        "kind": "choice",
        "text": {
            "uz": "1️⃣7️⃣ Xarid qilishdan oldin umumiy to'lov summasi sizga tushunarli bo'lganmi?",
            "ru": "1️⃣7️⃣ Было ли понятно, какая итоговая стоимость до покупки?",
            "en": "1️⃣7️⃣ Was the total cost clear before purchase?",
        },
        "options": YESNO,
    },
    {
        "id": "fees_explained",
        "kind": "choice",
        "text": {
            "uz": "1️⃣8️⃣ Foizlar va qo'shimcha to'lovlar oldindan aniq tushuntirilganmi?",
            "ru": "1️⃣8️⃣ Объяснили ли заранее проценты и дополнительные платежи?",
            "en": "1️⃣8️⃣ Were interest and extra fees explained in advance?",
        },
        "options": YESNO,
    },
    {
        "id": "schedule_given",
        "kind": "choice",
        "text": {
            "uz": "1️⃣9️⃣ To'lov jadvali (muddatlar va summalar) sizga berilganmi?",
            "ru": "1️⃣9️⃣ Выдали ли график платежей (сроки и суммы)?",
            "en": "1️⃣9️⃣ Were you given a payment schedule (dates and amounts)?",
        },
        "options": YESNO,
    },

    # ======== V. Difficulties / financial pressure ========
    {
        "id": "_section_5",
        "kind": "section",
        "text": {"uz": "section_5", "ru": "section_5", "en": "section_5"},
    },
    {
        "id": "difficulty_reason",
        "kind": "choice",
        "text": {
            "uz": "2️⃣0️⃣ Agar to'lovda qiyinchilik bo'lgan bo'lsa, asosiy sabab nima edi?",
            "ru": "2️⃣0️⃣ Если были трудности с оплатой, какова основная причина?",
            "en": "2️⃣0️⃣ If you had payment difficulties, what was the main reason?",
        },
        "options": {
            "uz": ["Daromad kamayishi", "Ish yo'qotilishi", "Narxlar oshishi", "Sog'liq bilan bog'liq sabablar", "Boshqa"],
            "ru": ["Снижение дохода", "Потеря работы", "Рост цен", "Проблемы со здоровьем", "Другое"],
            "en": ["Income decreased", "Job loss", "Prices increased", "Health reasons", "Other"],
        },
    },
    {
        "id": "borrowed_for_payments",
        "kind": "choice",
        "text": {
            "uz": "2️⃣1️⃣ Nasiya savdo to'lovlarini amalga oshirish uchun boshqa qarz olganmisiz?",
            "ru": "2️⃣1️⃣ Брали ли вы другой займ, чтобы оплатить платежи?",
            "en": "2️⃣1️⃣ Did you borrow elsewhere to make payments?",
        },
        "options": YESNO,
    },
    {
        "id": "cut_essential_spending",
        "kind": "choice",
        "text": {
            "uz": "2️⃣2️⃣ Nasiya savdo sababli asosiy (zarur) xarajatlaringizni qisqartirganmisiz?",
            "ru": "2️⃣2️⃣ Сокращали ли вы необходимые расходы из-за платежей?",
            "en": "2️⃣2️⃣ Did you cut essential spending due to installment payments?",
        },
        "options": YESNO,
    },
    {
        "id": "used_for_cash_need",
        "kind": "choice",
        "text": {
            "uz": "2️⃣3️⃣ Pul ehtiyojlaringiz uchun nasiya savdo xizmatidan foydalanganmisiz?",
            "ru": "2️⃣3️⃣ Использовали ли «насия» из-за нехватки денег/нужды в средствах?",
            "en": "2️⃣3️⃣ Did you use installment services due to cash needs?",
        },
        "options": YESNO,
    },

    # ======== VI. Collection practices ========
    {
        "id": "_section_6",
        "kind": "section",
        "text": {"uz": "section_6", "ru": "section_6", "en": "section_6"},
    },
    {
        "id": "contact_methods",
        "kind": "multi",
        "max_select": 6,
        "text": {
            "uz": "2️⃣4️⃣ Nasiya savdo kompaniyasi siz bilan qanday aloqa qilgan?\n(bir nechta variant tanlash mumkin)",
            "ru": "2️⃣4️⃣ Какими способами компания связывалась с вами?\n(можно несколько)",
            "en": "2️⃣4️⃣ How did the company contact you?\n(multiple choice allowed)",
        },
        "options": {
            "uz": ["SMS", "Avtomatik hisobdan yechish (avtospisaniya)", "Mobil ilova orqali bildirishnoma", "Telefon qo'ng'iroqlari", "Tashqi kollektor", "Sud orqali"],
            "ru": ["SMS", "Автосписание", "Уведомление в приложении", "Телефонные звонки", "Внешний коллектор", "Через суд"],
            "en": ["SMS", "Auto-debit", "In-app notification", "Phone calls", "External collector", "Through court"],
        },
    },
    {
        "id": "aggressive_collection",
        "kind": "choice",
        "text": {
            "uz": "2️⃣5️⃣ Sizga nisbatan agressiv yoki bosim o'tkazuvchi undirish holatlari bo'lganmi?",
            "ru": "2️⃣5️⃣ Были ли случаи агрессивного/давящего взыскания?",
            "en": "2️⃣5️⃣ Was there aggressive or pressuring collection?",
        },
        "options": YESNO,
    },

    # ======== VII. Complaints & trust ========
    {
        "id": "_section_7",
        "kind": "section",
        "text": {"uz": "section_7", "ru": "section_7", "en": "section_7"},
    },
    {
        "id": "complaint_submitted",
        "kind": "choice",
        "text": {
            "uz": "2️⃣6️⃣ Nasiya savdo kompaniyasiga shikoyat berganmisiz?",
            "ru": "2️⃣6️⃣ Подавали ли вы жалобу компании?",
            "en": "2️⃣6️⃣ Did you submit a complaint to the company?",
        },
        "options": YESNO,
    },
    {
        "id": "complaint_resolved",
        "kind": "choice",
        "text": {
            "uz": "2️⃣7️⃣ Agar shikoyat bergan bo'lsangiz, u hal qilinganmi?",
            "ru": "2️⃣7️⃣ Если жаловались, решилась ли проблема?",
            "en": "2️⃣7️⃣ If yes, was it resolved?",
        },
        "options": YESNO,
    },
    {
        "id": "satisfaction_1_5",
        "kind": "choice",
        "text": {
            "uz": "2️⃣8️⃣ Nasiya savdo xizmatlaridan umumiy qoniqish darajangiz:\n(1 – umuman qoniqmayman, 5 – to'liq qoniqaman)",
            "ru": "2️⃣8️⃣ Общая удовлетворённость услугами:\n(1 – совсем не удовлетворён, 5 – полностью удовлетворён)",
            "en": "2️⃣8️⃣ Overall satisfaction with services:\n(1 – not satisfied at all, 5 – fully satisfied)",
        },
        "options": {"uz": ["1", "2", "3", "4", "5"], "ru": ["1", "2", "3", "4", "5"], "en": ["1", "2", "3", "4", "5"]},
    },
    {
        "id": "recommend",
        "kind": "choice",
        "text": {
            "uz": "2️⃣9️⃣ Nasiya savdo xizmatlarini boshqalarga tavsiya qilarmidingiz?",
            "ru": "2️⃣9️⃣ Порекомендовали бы другим?",
            "en": "2️⃣9️⃣ Would you recommend it to others?",
        },
        "options": YESNO,
    },

    # ======== VIII. Financial awareness / behavior ========
    {
        "id": "_section_8",
        "kind": "section",
        "text": {"uz": "section_8", "ru": "section_8", "en": "section_8"},
    },
    {
        "id": "read_contract",
        "kind": "choice",
        "text": {
            "uz": "3️⃣0️⃣ Shartnoma shartlarini o'qib chiqqanmisiz?",
            "ru": "3️⃣0️⃣ Читали ли условия договора?",
            "en": "3️⃣0️⃣ Did you read the contract terms?",
        },
        "options": YESNO,
    },
    {
        "id": "know_limit",
        "kind": "choice",
        "text": {
            "uz": "3️⃣1️⃣ Sizga ajratilgan kredit limitini bilasizmi?",
            "ru": "3️⃣1️⃣ Знаете ли вы свой кредитный лимит?",
            "en": "3️⃣1️⃣ Do you know your assigned credit limit?",
        },
        "options": YESNO,
    },
    {
        "id": "impulse_buying",
        "kind": "choice",
        "text": {
            "uz": "3️⃣2️⃣ Nasiya savdo xizmatlari odatda rejalashtirilmagan (impulsiv) xaridlarni ko'paytiradi, deb hisoblaysizmi?",
            "ru": "3️⃣2️⃣ Считаете ли, что «насия» увеличивает импульсивные покупки?",
            "en": "3️⃣2️⃣ Do you think installment services increase impulse buying?",
        },
        "options": YESNO,
    },
    {
        "id": "need_stricter_regulation",
        "kind": "choice",
        "text": {
            "uz": "3️⃣3️⃣ Sizningcha, nasiya savdo bozorini qat'iyroq tartibga solish zarurmi?",
            "ru": "3️⃣3️⃣ Нужно ли более строго регулировать рынок?",
            "en": "3️⃣3️⃣ Is stricter regulation necessary?",
        },
        "options": {
            "uz": ["Zarur", "Betaraf", "Zarur emas"],
            "ru": ["Нужно", "Нейтрально", "Не нужно"],
            "en": ["Necessary", "Neutral", "Not necessary"],
        },
    },
]

# CSV headers (human) + keys (internal)
CSV_HEADERS_UZ = [
    "timestamp", "user_id", "username", "language",
    "Yashash hududi (viloyat/shahar)", "Hudud ID",
    "Yosh guruhi", "Jins", "Bandlik holati", "O'rtacha oylik daromad",
    "Oxirgi 3 oy chastotasi", "Foydalanish muddati (oy)", "Kompaniya", "O'rtacha xarid summasi",
    "Asosiy mahsulot/xizmatlar",
    "Bir nechta kompaniya (foydalanadi)", "Bir nechta kompaniya (qarz)", "Daromadga nisbatan %", "Qarz yuki hisobga olinganmi", "Kechikish bo'lganmi",
    "Total cost tushunarli", "Foiz/qo'shimcha to'lovlar tushuntirilgan", "To'lov jadvali berilgan",
    "Qiyinchilik sababi", "To'lov uchun boshqa qarz", "Zarur xarajatni qisqartirdi", "Pul ehtiyoji uchun ishlatgan",
    "Aloqa usullari", "Agressiv undirish",
    "Shikoyat bergan", "Shikoyat hal qilingan", "Qoniqish (1-5)", "Tavsiya qiladi",
    "Shartnoma o'qigan", "Limitni biladi", "Impulsiv xaridlarni ko'paytiradi", "Qattiqroq tartibga solish",
]
CSV_KEYS = [
    "timestamp", "user_id", "username", "language",
    "region_city", "region_city_id",
    "age_group", "gender", "employment", "income",
    "freq_3m", "months_using", "company_name", "avg_purchase",
    "product_types",
    "multi_company_use", "multi_company_debt", "income_share_percent", "debt_burden_checked", "missed_payment",
    "total_cost_clear", "fees_explained", "schedule_given",
    "difficulty_reason", "borrowed_for_payments", "cut_essential_spending", "used_for_cash_need",
    "contact_methods", "aggressive_collection",
    "complaint_submitted", "complaint_resolved", "satisfaction_1_5", "recommend",
    "read_contract", "know_limit", "impulse_buying", "need_stricter_regulation",
]

# ---------------- Conversation states ----------------
LANG, SURVEY_FLOW = range(2)

# ---------------- Helpers ----------------

def get_lang(ctx: ContextTypes.DEFAULT_TYPE) -> str:
    return ctx.user_data.get("lang", "uz")


def kb_lang() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("O'zbek 🇺🇿", callback_data="lang:uz")],
        [InlineKeyboardButton("Русский 🇷🇺", callback_data="lang:ru")],
        [InlineKeyboardButton("English 🇬🇧", callback_data="lang:en")],
    ])


def kb_choice(lang: str, qid: str, options: List[str]) -> InlineKeyboardMarkup:
    rows = []
    for idx, opt in enumerate(options):
        rows.append([InlineKeyboardButton(opt, callback_data=f"ans:{qid}:{idx}")])
    return InlineKeyboardMarkup(rows)


def kb_multi(lang: str, qid: str, options: List[str], selected: set, done_label: str) -> InlineKeyboardMarkup:
    rows = []
    for idx, opt in enumerate(options):
        mark = "✅ " if idx in selected else ""
        rows.append([InlineKeyboardButton(f"{mark}{opt}", callback_data=f"mul:{qid}:{idx}")])
    rows.append([InlineKeyboardButton(done_label, callback_data=f"mul_done:{qid}")])
    return InlineKeyboardMarkup(rows)


def kb_regions(lang: str, page: int = 0, per_page: int = 8) -> InlineKeyboardMarkup:
    total = len(UZB_REGIONS)
    if total == 0:
        return InlineKeyboardMarkup([[InlineKeyboardButton("—", callback_data="noop")]])

    max_page = (total + per_page - 1) // per_page
    page = max(0, min(page, max_page - 1))

    start = page * per_page
    end = min(start + per_page, total)
    chunk = UZB_REGIONS[start:end]

    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for r in chunk:
        label = r.get(lang, r["uz"])
        row.append(InlineKeyboardButton(label, callback_data=f"reg:{r['id']}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"regpage:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{max_page}", callback_data="noop"))
    if end < total:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"regpage:{page+1}"))
    rows.append(nav)

    return InlineKeyboardMarkup(rows)


def ensure_csv_headers():
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(CSV_HEADERS_UZ)


def append_csv(row: Dict[str, Any]):
    ensure_csv_headers()
    out = []
    for k in CSV_KEYS:
        v = row.get(k, "")
        if isinstance(v, (list, set, tuple)):
            v = "; ".join(str(x) for x in v)
        out.append("" if v is None else str(v))
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(out)


def normalize_number(s: str) -> Optional[int]:
    s = s.strip().replace(" ", "")
    if s == "" or not s.isdigit():
        return None
    return int(s)


def is_not_used_answer(ans: str, lang: str) -> bool:
    """Check if the answer indicates user never used nasiya services."""
    not_used_values = {
        value.strip().lower()
        for value in NOT_USED_OPTIONS.values()
        if isinstance(value, str)
    }
    ans_normalized = ans.strip().lower()
    return ans_normalized in not_used_values


def normalize_multi_selection(selected_raw: List[Any], options: List[str]) -> set:
    selected_indices: set[int] = set()
    for item in selected_raw:
        if isinstance(item, int) and 0 <= item < len(options):
            selected_indices.add(item)
            continue
        if isinstance(item, str):
            if item.isdigit():
                idx = int(item)
                if 0 <= idx < len(options):
                    selected_indices.add(idx)
                    continue
            if item in options:
                selected_indices.add(options.index(item))
    return selected_indices


async def send_question(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(ctx)
    i = int(ctx.user_data.get("q_index", 0))

    # Skip section markers but show section headers
    while i < len(SURVEY) and SURVEY[i]["kind"] == "section":
        section_key = SURVEY[i]["text"].get(lang, SURVEY[i]["text"].get("uz", ""))
        section_text = tr(lang, section_key)
        await update.effective_chat.send_message(section_text, parse_mode="Markdown")
        i += 1
        ctx.user_data["q_index"] = i

    if i >= len(SURVEY):
        await finalize(update, ctx)
        return

    q = SURVEY[i]
    qid = q["id"]
    kind = q["kind"]

    text = q["text"].get(lang, q["text"].get("uz", ""))
    hint = q.get("hint", {}).get(lang)
    full_text = text + (f"\n\n💬 {hint}" if hint else "")

    if kind == "choice":
        opts = q["options"].get(lang, q["options"].get("uz", []))
        await update.effective_chat.send_message(full_text, reply_markup=kb_choice(lang, qid, opts))
        return

    if kind == "multi":
        opts = q["options"].get(lang, q["options"].get("uz", []))
        selected_raw = ctx.user_data.get(f"multi:{qid}", [])
        selected = normalize_multi_selection(selected_raw, opts)
        await update.effective_chat.send_message(
            full_text,
            reply_markup=kb_multi(lang, qid, opts, selected, tr(lang, "btn_done")),
        )
        return

    if kind == "region":
        page = int(ctx.user_data.get("region_page", 0))
        await update.effective_chat.send_message(full_text, reply_markup=kb_regions(lang, page=page))
        return

    # text / number / percent
    await update.effective_chat.send_message(full_text, reply_markup=ReplyKeyboardRemove())


# ---------------- Handlers ----------------
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text(tr("uz", "choose_lang"), reply_markup=kb_lang())
    return LANG


async def on_lang(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    if not data.startswith("lang:"):
        return LANG

    lang = data.split(":", 1)[1]
    ctx.user_data["lang"] = lang

    await query.message.reply_text(tr(lang, "start"))
    await query.message.reply_text(
        tr(lang, "btn_start"),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(tr(lang, "btn_start"), callback_data="go:start")]]),
    )
    return SURVEY_FLOW


async def on_go_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    ctx.user_data["q_index"] = 0
    ctx.user_data["region_page"] = 0
    ctx.user_data["answers"] = {
        "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
        "user_id": update.effective_user.id if update.effective_user else "",
        "username": update.effective_user.username if update.effective_user else "",
        "language": get_lang(ctx),
    }
    await send_question(update, ctx)
    return SURVEY_FLOW


async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lang = get_lang(ctx)
    data = query.data or ""

    if data == "noop":
        return SURVEY_FLOW

    i = int(ctx.user_data.get("q_index", 0))

    # Skip section markers
    while i < len(SURVEY) and SURVEY[i]["kind"] == "section":
        i += 1
        ctx.user_data["q_index"] = i

    if i >= len(SURVEY):
        await finalize(update, ctx)
        return ConversationHandler.END

    q = SURVEY[i]
    qid = q["id"]
    kind = q["kind"]

    # --- REGION paging ---
    if data.startswith("regpage:") and kind == "region":
        page = int(data.split(":", 1)[1])
        ctx.user_data["region_page"] = page
        await query.message.edit_reply_markup(reply_markup=kb_regions(lang, page=page))
        return SURVEY_FLOW

    # --- REGION select ---
    if data.startswith("reg:") and kind == "region":
        rid = data.split(":", 1)[1]
        reg = next((r for r in UZB_REGIONS if r["id"] == rid), None)
        if not reg:
            await query.message.reply_text(tr(lang, "invalid"))
            return SURVEY_FLOW

        ctx.user_data["answers"]["region_city_id"] = rid
        ctx.user_data["answers"][qid] = reg.get(lang, reg["uz"])

        ctx.user_data["region_page"] = 0
        ctx.user_data["q_index"] = i + 1
        await send_question(update, ctx)
        return SURVEY_FLOW

    # --- single choice ---
    if data.startswith("ans:") and kind == "choice":
        parts = data.split(":", 2)
        ans = None
        if len(parts) == 3:
            _, qid_from, idx_text = parts
            if qid_from != qid:
                return SURVEY_FLOW
            if idx_text.isdigit():
                idx = int(idx_text)
                opts = q["options"].get(lang, q["options"].get("uz", []))
                if 0 <= idx < len(opts):
                    ans = opts[idx]
        elif len(parts) == 2:
            ans = parts[1]

        if ans is None:
            await query.message.reply_text(tr(lang, "invalid"))
            return SURVEY_FLOW

        ctx.user_data["answers"][qid] = ans

        # Convert satisfaction to integer
        if qid == "satisfaction_1_5":
            try:
                ctx.user_data["answers"][qid] = int(ans)
            except ValueError:
                pass

        # Check if this is the freq_3m question and user selected "Did not use"
        if qid == "freq_3m" and q.get("skip_if_not_used") and is_not_used_answer(ans, lang):
            # End survey early with special message
            await finalize_not_used(update, ctx)
            return ConversationHandler.END

        ctx.user_data["q_index"] = i + 1
        await send_question(update, ctx)
        return SURVEY_FLOW

    # --- multi toggle ---
    if data.startswith("mul:") and kind == "multi":
        _, qid2, opt = data.split(":", 2)
        if qid2 != qid:
            return SURVEY_FLOW

        opts = q["options"].get(lang, q["options"].get("uz", []))
        key = f"multi:{qid}"
        selected_raw = ctx.user_data.get(key, [])
        selected = normalize_multi_selection(selected_raw, opts)

        if opt.isdigit():
            idx = int(opt)
            if idx in selected:
                selected.remove(idx)
            else:
                if len(selected) < int(q.get("max_select", 7)):
                    selected.add(idx)
        else:
            if opt in opts:
                idx = opts.index(opt)
                if idx in selected:
                    selected.remove(idx)
                else:
                    if len(selected) < int(q.get("max_select", 7)):
                        selected.add(idx)

        ctx.user_data[key] = list(selected)

        await query.message.edit_reply_markup(reply_markup=kb_multi(lang, qid, opts, selected, tr(lang, "btn_done")))
        return SURVEY_FLOW

    # --- multi done ---
    if data.startswith("mul_done:") and kind == "multi":
        qid2 = data.split(":", 1)[1]
        if qid2 != qid:
            return SURVEY_FLOW
        opts = q["options"].get(lang, q["options"].get("uz", []))
        selected_raw = ctx.user_data.get(f"multi:{qid}", [])
        selected = normalize_multi_selection(selected_raw, opts)
        ctx.user_data["answers"][qid] = [opts[idx] for idx in sorted(selected) if 0 <= idx < len(opts)]
        ctx.user_data["q_index"] = i + 1
        await send_question(update, ctx)
        return SURVEY_FLOW

    await query.message.reply_text(tr(lang, "invalid"))
    return SURVEY_FLOW


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(ctx)
    i = int(ctx.user_data.get("q_index", 0))

    # Skip section markers
    while i < len(SURVEY) and SURVEY[i]["kind"] == "section":
        i += 1
        ctx.user_data["q_index"] = i

    if i >= len(SURVEY):
        await finalize(update, ctx)
        return ConversationHandler.END

    q = SURVEY[i]
    qid = q["id"]
    kind = q["kind"]
    msg = (update.message.text or "").strip()

    if kind == "text":
        if len(msg) < 1:
            await update.message.reply_text(tr(lang, "invalid"))
            return SURVEY_FLOW
        ctx.user_data["answers"][qid] = msg
        ctx.user_data["q_index"] = i + 1
        await send_question(update, ctx)
        return SURVEY_FLOW

    if kind == "number":
        n = normalize_number(msg)
        if n is None:
            await update.message.reply_text(tr(lang, "invalid"))
            return SURVEY_FLOW
        mn = int(q.get("min", -10**9))
        mx = int(q.get("max", 10**9))
        if n < mn or n > mx:
            await update.message.reply_text(tr(lang, "invalid"))
            return SURVEY_FLOW
        ctx.user_data["answers"][qid] = n
        ctx.user_data["q_index"] = i + 1
        await send_question(update, ctx)
        return SURVEY_FLOW

    if kind == "percent":
        n = normalize_number(msg)
        if n is None or n < 0 or n > 100:
            await update.message.reply_text(tr(lang, "invalid"))
            return SURVEY_FLOW
        ctx.user_data["answers"][qid] = n
        ctx.user_data["q_index"] = i + 1
        await send_question(update, ctx)
        return SURVEY_FLOW

    await update.message.reply_text(tr(lang, "invalid"))
    return SURVEY_FLOW


async def finalize_not_used(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """End survey early when user has never used nasiya services."""
    lang = get_lang(ctx)
    answers = ctx.user_data.get("answers", {})

    # 1. Save to PostgreSQL (primary)
    db_saved = await save_to_db(answers)
    if db_saved:
        log.info("Response saved to PostgreSQL (not used)")
    else:
        log.warning("PostgreSQL save failed, using CSV backup")

    # 2. Save to CSV (backup)
    try:
        append_csv(answers)
        log.info("Response saved to CSV (not used)")
    except Exception as e:
        log.error("CSV save error: %s", e)

    # 3. Optional: Google Sheets
    gs_name = os.getenv("GOOGLE_SHEET_NAME", "").strip()
    gs_ws = os.getenv("GOOGLE_SHEET_WORKSHEET", "Responses").strip()
    if gs_name:
        err = try_gs_save_row(gs_name, gs_ws, answers, CSV_HEADERS_UZ, CSV_KEYS)
        if err:
            log.warning("Google Sheets not saved: %s", err)
        else:
            log.info("Response saved to Google Sheets (not used)")

    # Send special "thank you" message for non-users
    await update.effective_chat.send_message(tr(lang, "saved_not_used"), reply_markup=ReplyKeyboardRemove())
    ctx.user_data.clear()


async def finalize(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(ctx)
    answers = ctx.user_data.get("answers", {})

    # 1. Save to PostgreSQL (primary)
    db_saved = await save_to_db(answers)
    if db_saved:
        log.info("Response saved to PostgreSQL")
    else:
        log.warning("PostgreSQL save failed, using CSV backup")

    # 2. Save to CSV (backup)
    try:
        append_csv(answers)
        log.info("Response saved to CSV")
    except Exception as e:
        log.error("CSV save error: %s", e)

    # 3. Optional: Google Sheets
    gs_name = os.getenv("GOOGLE_SHEET_NAME", "").strip()
    gs_ws = os.getenv("GOOGLE_SHEET_WORKSHEET", "Responses").strip()
    if gs_name:
        err = try_gs_save_row(gs_name, gs_ws, answers, CSV_HEADERS_UZ, CSV_KEYS)
        if err:
            log.warning("Google Sheets not saved: %s", err)
        else:
            log.info("Response saved to Google Sheets")

    await update.effective_chat.send_message(tr(lang, "saved"), reply_markup=ReplyKeyboardRemove())
    ctx.user_data.clear()


async def cmd_export(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(ctx)
    uid = update.effective_user.id if update.effective_user else 0
    if uid not in ADMIN_IDS:
        await update.message.reply_text(tr(lang, "export_only_admin"))
        return

    # Try PostgreSQL export first
    export_path = await export_db_to_csv()
    if export_path and os.path.exists(export_path):
        await update.message.reply_document(
            document=open(export_path, "rb"),
            filename="survey_export_db.csv",
            caption="📊 Nasiya survey export (PostgreSQL)",
        )
        return

    # Fallback to local CSV
    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        await update.message.reply_text(tr(lang, "no_data"))
        return

    await update.message.reply_document(
        document=open(CSV_PATH, "rb"),
        filename=os.path.basename(CSV_PATH),
        caption="📊 Nasiya survey export (CSV backup)",
    )


async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(ctx)
    uid = update.effective_user.id if update.effective_user else 0
    if uid not in ADMIN_IDS:
        await update.message.reply_text(tr(lang, "export_only_admin"))
        return

    stats = await get_stats()
    if not stats:
        await update.message.reply_text(tr(lang, "no_data"))
        return

    text = f"""📊 **So'rovnoma statistikasi**\n\n📈 Jami javoblar: {stats.get('total', 0)}\n📅 Bugun: {stats.get('today', 0)}\n📆 Oxirgi 7 kun: {stats.get('week', 0)}\n⭐ O'rtacha qoniqish: {stats.get('avg_satisfaction', 0)}/5\n\n🏆 **Top hududlar:**\n"""
    for region, count in stats.get("top_regions", []):
        text += f"  • {region}: {count}\n"

    await update.message.reply_text(text, parse_mode="Markdown")


def build_app():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            LANG: [CallbackQueryHandler(on_lang, pattern=r"^lang:")],
            SURVEY_FLOW: [
                CallbackQueryHandler(on_go_start, pattern=r"^go:start$"),
                CallbackQueryHandler(on_callback),
                MessageHandler(filters.TEXT & ~filters.COMMAND, on_text),
            ],
        },
        fallbacks=[CommandHandler("start", cmd_start)],
        allow_reentry=True,
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler("export", cmd_export))
    app.add_handler(CommandHandler("stats", cmd_stats))
    return app


async def main():
    # Initialize database
    await init_db()

    # Build and run bot
    app = build_app()
    log.info("Bot started.")

    # Initialize and start
    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    # Keep running
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
