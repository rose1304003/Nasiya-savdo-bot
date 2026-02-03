#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Nasiya Savdo Xizmatlari So'rovnoma Bot (v3)
✅ 3 languages: Uzbek (Latin), Russian, English
✅ Region buttons for Uzbekistan (paginated)
✅ PostgreSQL database (primary storage)
✅ CSV backup
✅ Optional Google Sheets integration
✅ Admin export: /export, /stats
✅ Based on updated Central Bank survey questionnaire
✅ NEW: "No" branch (6.1–6.10) for non-users — no longer ends early
✅ NEW: Complaint reason sub-question (25.1) when complaint = Yes
✅ NEW: Updated companies list, age groups, income brackets
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
                CREATE TABLE IF NOT EXISTS survey_responses_v3 (
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

                    -- II. Usage (branch marker)
                    ever_used VARCHAR(10),

                    -- === "No" branch: 6.1 – 6.10 ===
                    heard_before VARCHAR(10),
                    trust_level VARCHAR(100),
                    terms_understandable VARCHAR(100),
                    is_useful VARCHAR(100),
                    decision_factors TEXT,
                    would_use_if_better VARCHAR(50),
                    best_for_whom VARCHAR(100),
                    needed_sectors TEXT,
                    nu_impulse_buying VARCHAR(10),
                    nu_need_regulation VARCHAR(100),

                    -- === "Yes" branch ===
                    freq_1y VARCHAR(50),
                    usage_duration TEXT,
                    company_name TEXT,
                    avg_purchase VARCHAR(100),
                    product_types TEXT,

                    -- III. Multiple obligations
                    multi_company_use VARCHAR(10),
                    income_share_percent VARCHAR(50),
                    debt_burden_checked VARCHAR(10),

                    -- IV. Transparency
                    contract_terms_clear VARCHAR(10),
                    total_cost_clear VARCHAR(10),
                    schedule_given VARCHAR(10),

                    -- V. Payment difficulties
                    missed_payment VARCHAR(10),
                    difficulty_reason VARCHAR(100),
                    borrowed_for_payments VARCHAR(10),
                    cut_essential_spending VARCHAR(10),
                    used_for_cash_need VARCHAR(10),

                    -- VI. Collection practices
                    contact_methods TEXT,
                    aggressive_collection VARCHAR(10),

                    -- VII. Complaints & trust
                    complaint_submitted VARCHAR(10),
                    complaint_reason TEXT,
                    complaint_resolved VARCHAR(10),
                    satisfaction_1_5 INTEGER,
                    recommend VARCHAR(10),

                    -- VIII. Financial awareness
                    read_contract VARCHAR(10),
                    know_limit VARCHAR(10),
                    impulse_buying VARCHAR(10),
                    need_stricter_regulation VARCHAR(100)
                )
            ''')

            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_survey_v3_created_at
                ON survey_responses_v3(created_at)
            ''')
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_survey_v3_user_id
                ON survey_responses_v3(user_id)
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
        def to_str(val):
            if isinstance(val, (list, set, tuple)):
                return "; ".join(str(x) for x in val)
            return val

        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO survey_responses_v3 (
                    user_id, username, language,
                    region_city, region_city_id, age_group, gender, employment, income,
                    ever_used,
                    heard_before, trust_level, terms_understandable, is_useful,
                    decision_factors, would_use_if_better, best_for_whom, needed_sectors,
                    nu_impulse_buying, nu_need_regulation,
                    freq_1y, usage_duration, company_name, avg_purchase, product_types,
                    multi_company_use, income_share_percent, debt_burden_checked,
                    contract_terms_clear, total_cost_clear, schedule_given,
                    missed_payment, difficulty_reason, borrowed_for_payments,
                    cut_essential_spending, used_for_cash_need,
                    contact_methods, aggressive_collection,
                    complaint_submitted, complaint_reason, complaint_resolved,
                    satisfaction_1_5, recommend,
                    read_contract, know_limit, impulse_buying, need_stricter_regulation
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                    $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                    $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
                    $41, $42, $43, $44, $45, $46, $47
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
                data.get("ever_used"),
                data.get("heard_before"),
                data.get("trust_level"),
                data.get("terms_understandable"),
                data.get("is_useful"),
                to_str(data.get("decision_factors")),
                data.get("would_use_if_better"),
                data.get("best_for_whom"),
                to_str(data.get("needed_sectors")),
                data.get("nu_impulse_buying"),
                data.get("nu_need_regulation"),
                data.get("freq_1y"),
                to_str(data.get("usage_duration")),
                to_str(data.get("company_name")),
                data.get("avg_purchase"),
                to_str(data.get("product_types")),
                data.get("multi_company_use"),
                data.get("income_share_percent"),
                data.get("debt_burden_checked"),
                data.get("contract_terms_clear"),
                data.get("total_cost_clear"),
                data.get("schedule_given"),
                data.get("missed_payment"),
                data.get("difficulty_reason"),
                data.get("borrowed_for_payments"),
                data.get("cut_essential_spending"),
                data.get("used_for_cash_need"),
                to_str(data.get("contact_methods")),
                data.get("aggressive_collection"),
                data.get("complaint_submitted"),
                to_str(data.get("complaint_reason")),
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
            total = await conn.fetchval('SELECT COUNT(*) FROM survey_responses_v3')
            today = await conn.fetchval('''
                SELECT COUNT(*) FROM survey_responses_v3
                WHERE created_at >= CURRENT_DATE
            ''')
            week = await conn.fetchval('''
                SELECT COUNT(*) FROM survey_responses_v3
                WHERE created_at >= CURRENT_DATE - INTERVAL \'7 days\'
            ''')

            regions = await conn.fetch('''
                SELECT region_city, COUNT(*) as cnt
                FROM survey_responses_v3
                WHERE region_city IS NOT NULL
                GROUP BY region_city
                ORDER BY cnt DESC
                LIMIT 5
            ''')

            avg_satisfaction = await conn.fetchval('''
                SELECT ROUND(AVG(satisfaction_1_5)::numeric, 2)
                FROM survey_responses_v3
                WHERE satisfaction_1_5 IS NOT NULL
            ''')

            # Count users vs non-users
            users_count = await conn.fetchval('''
                SELECT COUNT(*) FROM survey_responses_v3
                WHERE ever_used IN ('Ha', 'Да', 'Yes')
            ''')
            non_users_count = await conn.fetchval('''
                SELECT COUNT(*) FROM survey_responses_v3
                WHERE ever_used IN ('Yo''q', 'Нет', 'No')
            ''')

            return {
                "total": total or 0,
                "today": today or 0,
                "week": week or 0,
                "top_regions": [(r["region_city"], r["cnt"]) for r in regions],
                "avg_satisfaction": float(avg_satisfaction) if avg_satisfaction else 0,
                "users_count": users_count or 0,
                "non_users_count": non_users_count or 0,
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
            rows = await conn.fetch('SELECT * FROM survey_responses_v3 ORDER BY created_at')

            if not rows:
                return None

            export_path = "/tmp/survey_export_v3.csv"
            with open(export_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(rows[0].keys())
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
        "uz": "Assalomu alaykum! 🏦\n\nNasiya savdo xizmatlari foydalanuvchilari (mijozlar) uchun so'rovnomani boshlaymiz.",
        "ru": "Здравствуйте! 🏦\n\nНачнём опрос для пользователей (клиентов) услуг «Насия савдо».",
        "en": "Hello! 🏦\n\nLet's start the survey for users (clients) of installment trade services (Nasiya Savdo).",
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
    "type_other_company": {
        "uz": "✏️ Iltimos, boshqa tashkilot nomini yozing:",
        "ru": "✏️ Пожалуйста, введите название другой организации:",
        "en": "✏️ Please type the name of the other company:",
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
    "section_2_no": {
        "uz": "📊 **Nasiya savdo xizmatlari haqida fikringiz**",
        "ru": "📊 **Ваше мнение об услугах «Насия савдо»**",
        "en": "📊 **Your opinion about installment trade services**",
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
        "uz": "⚠️ **V. To'lov bilan bog'liq muammolar**",
        "ru": "⚠️ **V. Проблемы с платежами**",
        "en": "⚠️ **V. Payment Difficulties**",
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
    "note_nasiya": {
        "uz": "💡 *Izoh: Nasiya savdo xizmati — xaridor va xizmat ko'rsatuvchi o'rtasida tuziladigan kelishuv asosida sotib olingan tovar (ish, xizmat) qiymatini muayyan vaqt davomida bir martada yoki bo'lib-bo'lib to'lash.*",
        "ru": "💡 *Примечание: Услуга «Насия савдо» — это оплата стоимости товара (работы, услуги), приобретённого на основании договора между покупателем и поставщиком, единовременно или в рассрочку в течение определённого срока.*",
        "en": "💡 *Note: Installment trade service — payment for goods (work, services) purchased under an agreement between buyer and provider, paid in full or in installments over a set period.*",
    },
}


def tr(lang: str, key: str) -> str:
    lang = lang if lang in LANGS else "uz"
    return T.get(key, {}).get(lang, T.get(key, {}).get("uz", key))


# ---------------- Uzbekistan regions ----------------
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


# ================================================================
#                      SURVEY DEFINITION
# ================================================================

YESNO = {
    "uz": ["Ha", "Yo'q"],
    "ru": ["Да", "Нет"],
    "en": ["Yes", "No"],
}

# ========= SHARED: Section I — Respondent profile (Q1-Q5) =========

SURVEY_PROFILE: List[Dict[str, Any]] = [
    {
        "id": "_section_1",
        "kind": "section",
        "text": {"uz": "section_1", "ru": "section_1", "en": "section_1"},
    },
    {
        "id": "region_city",
        "kind": "region",
        "text": {
            "uz": "1️⃣ Yashash hududingiz (viloyat):",
            "ru": "1️⃣ Регион проживания (область):",
            "en": "1️⃣ Place of residence (region):",
        },
    },
    {
        "id": "age_group",
        "kind": "choice",
        "text": {
            "uz": "2️⃣ Yoshingiz:",
            "ru": "2️⃣ Ваш возраст:",
            "en": "2️⃣ Your age:",
        },
        "options": {
            "uz": ["18 yoshgacha", "18–25", "26–35", "36–45", "46–55", "55 dan yuqori"],
            "ru": ["до 18", "18–25", "26–35", "36–45", "46–55", "старше 55"],
            "en": ["Under 18", "18–25", "26–35", "36–45", "46–55", "Above 55"],
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
        "text": {"uz": "4️⃣ Bandlik holatingiz:", "ru": "4️⃣ Ваша занятость:", "en": "4️⃣ Employment status:"},
        "options": {
            "uz": ["Ishlaydi (rasmiy)", "Ishlaydi (norasmiy)", "Tadbirkor", "O'zini-o'zi band qilgan", "Talaba", "Nafaqada", "Ishsiz"],
            "ru": ["Работаю (официально)", "Работаю (неофициально)", "Предприниматель", "Самозанятый(ая)", "Студент(ка)", "На пенсии", "Безработный(ая)"],
            "en": ["Employed (formal)", "Employed (informal)", "Entrepreneur", "Self-employed", "Student", "Retired", "Unemployed"],
        },
    },
    {
        "id": "income",
        "kind": "choice",
        "text": {"uz": "5️⃣ O'rtacha oylik daromadingiz:", "ru": "5️⃣ Средний ежемесячный доход:", "en": "5️⃣ Average monthly income:"},
        "options": {
            "uz": ["2 mln so'mgacha", "2–5 mln so'm", "5–10 mln so'm", "10–20 mln so'm", "20–50 mln so'm", "50 mln so'mdan yuqori"],
            "ru": ["до 2 млн сум", "2–5 млн сум", "5–10 млн сум", "10–20 млн сум", "20–50 млн сум", "более 50 млн сум"],
            "en": ["Up to 2 mln UZS", "2–5 mln UZS", "5–10 mln UZS", "10–20 mln UZS", "20–50 mln UZS", "Above 50 mln UZS"],
        },
    },
]

# ========= BRANCHING: Q6 — ever used? =========

Q_EVER_USED: Dict[str, Any] = {
    "id": "ever_used",
    "kind": "choice",
    "text": {
        "uz": "6️⃣ Nasiya savdo xizmatidan foydalanganmisiz?\n\n💡 _Izoh: Nasiya savdo xizmati — xaridor va xizmat ko'rsatuvchi o'rtasida tuziladigan kelishuv asosida sotib olingan tovar (ish, xizmat) qiymatini muayyan vaqt davomida bir martada yoki bo'lib-bo'lib to'lash._",
        "ru": "6️⃣ Пользовались ли вы услугой «Насия савдо»?\n\n💡 _Примечание: Услуга «Насия савдо» — это оплата стоимости товара (работы, услуги), приобретённого на основании договора между покупателем и поставщиком, единовременно или в рассрочку в течение определённого срока._",
        "en": "6️⃣ Have you used installment trade services (Nasiya Savdo)?\n\n💡 _Note: Installment trade service — payment for goods (work, services) purchased under an agreement between buyer and provider, paid in full or in installments over a set period._",
    },
    "options": YESNO,
    "is_branch_question": True,
}


# ========= "NO" BRANCH: 6.1 – 6.10 (non-users) =========

SURVEY_NO_BRANCH: List[Dict[str, Any]] = [
    {
        "id": "_section_2_no",
        "kind": "section",
        "text": {"uz": "section_2_no", "ru": "section_2_no", "en": "section_2_no"},
    },
    {
        "id": "heard_before",
        "kind": "choice",
        "text": {
            "uz": "6.1. Nasiya savdo xizmati haqida avval eshitganmisiz?",
            "ru": "6.1. Слышали ли вы раньше об услуге «Насия савдо»?",
            "en": "6.1. Have you heard about installment trade services before?",
        },
        "options": YESNO,
    },
    {
        "id": "trust_level",
        "kind": "choice",
        "text": {
            "uz": "6.2. Nasiya savdo xizmatiga ishonchingiz bormi?",
            "ru": "6.2. Доверяете ли вы услуге «Насия савдо»?",
            "en": "6.2. Do you trust installment trade services?",
        },
        "options": {
            "uz": ["Ha, ishonchim bor", "Qisman ishonaman", "Yo'q, ishonchim yo'q"],
            "ru": ["Да, доверяю", "Частично доверяю", "Нет, не доверяю"],
            "en": ["Yes, I trust them", "Partially trust", "No, I don't trust them"],
        },
    },
    {
        "id": "terms_understandable",
        "kind": "choice",
        "text": {
            "uz": "6.3. Nasiya savdo xizmati shartlari siz uchun tushunarlimi?",
            "ru": "6.3. Понятны ли вам условия услуги «Насия савдо»?",
            "en": "6.3. Are the terms of installment trade services understandable to you?",
        },
        "options": {
            "uz": ["Ha, to'liq tushunarli", "Qisman tushunarli", "Yo'q, tushunarsiz"],
            "ru": ["Да, полностью понятны", "Частично понятны", "Нет, непонятны"],
            "en": ["Yes, fully clear", "Partially clear", "No, unclear"],
        },
    },
    {
        "id": "decision_factors",
        "kind": "multi",
        "max_select": 7,
        "text": {
            "uz": "6.4. Nasiya savdodan foydalanish qaroriga asosan nima ta'sir qiladi?\n(bir nechta javob tanlash mumkin)",
            "ru": "6.4. Что в основном влияет на решение воспользоваться «Насия савдо»?\n(можно несколько)",
            "en": "6.4. What mainly influences the decision to use installment trade?\n(multiple choice allowed)",
        },
        "options": {
            "uz": [
                "Foiz stavkasi / qo'shimcha to'lovlar",
                "Kredit tarixi salbiy bo'lishiga qaramasdan, foydalana olish imkoniyati",
                "Moslashuvchan to'lov muddati",
                "Kompaniyaning ishonchliligi",
                "Rasmiylashtirishning osonligi",
                "Do'kon / mahsulot turi",
                "Tavsiyalar (do'stlar, oila)",
            ],
            "ru": [
                "Процентная ставка / доп. платежи",
                "Возможность пользоваться даже с плохой кредитной историей",
                "Гибкий срок оплаты",
                "Надёжность компании",
                "Простота оформления",
                "Тип магазина / товара",
                "Рекомендации (друзья, семья)",
            ],
            "en": [
                "Interest rate / extra fees",
                "Ability to use despite bad credit history",
                "Flexible payment terms",
                "Company reliability",
                "Ease of application",
                "Store / product type",
                "Recommendations (friends, family)",
            ],
        },
    },
    {
        "id": "needed_sectors",
        "kind": "multi",
        "max_select": 10,
        "text": {
            "uz": "6.5. Nasiya savdo xizmatini qaysi sohalarda ko'proq kerak deb hisoblaysiz?\n(bir nechta javob tanlash mumkin)",
            "ru": "6.5. В каких сферах больше нужна услуга «Насия савдо»?\n(можно несколько)",
            "en": "6.5. In which sectors do you think installment trade is most needed?\n(multiple choice allowed)",
        },
        "options": {
            "uz": [
                "Elektronika",
                "Maishiy texnika",
                "Mebel va jihoz",
                "Qurilish va ta'mirlash",
                "Oziq-ovqat mahsulotlari",
                "Kiyim-kechak",
                "Sayohat / xizmatlar",
                "Avtomashina",
                "Ko'chmas mulk (turar / noturar joy)",
                "Boshqa",
            ],
            "ru": [
                "Электроника",
                "Бытовая техника",
                "Мебель и оборудование",
                "Строительство и ремонт",
                "Продукты питания",
                "Одежда",
                "Путешествия / услуги",
                "Автомобиль",
                "Недвижимость",
                "Другое",
            ],
            "en": [
                "Electronics",
                "Home appliances",
                "Furniture & equipment",
                "Construction & renovation",
                "Food products",
                "Clothing",
                "Travel / services",
                "Car",
                "Real estate",
                "Other",
            ],
        },
    },
    {
        "id": "nu_impulse_buying",
        "kind": "choice",
        "text": {
            "uz": "6.6. Nasiya savdo xizmatlari odatda rejalashtirilmagan xaridlarni ko'paytiradi, deb hisoblaysizmi?",
            "ru": "6.6. Считаете ли вы, что услуги «Насия савдо» обычно увеличивают незапланированные покупки?",
            "en": "6.6. Do you think installment trade services typically increase unplanned purchases?",
        },
        "options": YESNO,
    },
    {
        "id": "nu_need_regulation",
        "kind": "choice",
        "text": {
            "uz": "6.7. Sizningcha, nasiya savdo bozori davlat tomonidan tartibga solinishi zarurmi?",
            "ru": "6.7. Нужно ли, по-вашему, государственное регулирование рынка «Насия савдо»?",
            "en": "6.7. In your opinion, should the installment trade market be regulated by the government?",
        },
        "options": {
            "uz": ["Zarur", "Zarur emas", "Javob berishga qiynalaman"],
            "ru": ["Нужно", "Не нужно", "Затрудняюсь ответить"],
            "en": ["Necessary", "Not necessary", "Hard to say"],
        },
    },
]


# ========= "YES" BRANCH: Q7-Q33 (users) =========

SURVEY_YES_BRANCH: List[Dict[str, Any]] = [
    {
        "id": "_section_2",
        "kind": "section",
        "text": {"uz": "section_2", "ru": "section_2", "en": "section_2"},
    },
    {
        "id": "freq_1y",
        "kind": "choice",
        "text": {
            "uz": "7️⃣ So'ngi 1 yil davomida nasiya savdo xizmatidan necha marta foydalangansiz?",
            "ru": "7️⃣ Сколько раз за последний год пользовались услугой «Насия савдо»?",
            "en": "7️⃣ How many times in the last year have you used it?",
        },
        "options": {
            "uz": ["1 marta", "2 marta", "3 marta", "4 va undan ko'p"],
            "ru": ["1 раз", "2 раза", "3 раза", "4 и более"],
            "en": ["Once", "Twice", "3 times", "4 or more"],
        },
    },
    {
        "id": "usage_duration",
        "kind": "multi",
        "max_select": 7,
        "text": {
            "uz": "8️⃣ Nasiya savdo xizmatini odatda necha oyga olasiz?\n(bir nechta javob tanlash mumkin)",
            "ru": "8️⃣ На какой срок обычно берёте рассрочку?\n(можно несколько)",
            "en": "8️⃣ For how many months do you usually take installments?\n(multiple choice allowed)",
        },
        "options": {
            "uz": ["1 oygacha", "3 oygacha", "6 oygacha", "12 oygacha", "18 oygacha", "24 oygacha", "24 oydan yuqori"],
            "ru": ["до 1 месяца", "до 3 месяцев", "до 6 месяцев", "до 12 месяцев", "до 18 месяцев", "до 24 месяцев", "более 24 месяцев"],
            "en": ["Up to 1 month", "Up to 3 months", "Up to 6 months", "Up to 12 months", "Up to 18 months", "Up to 24 months", "Over 24 months"],
        },
    },
    {
        "id": "company_name",
        "kind": "multi",
        "max_select": 11,
        "has_other": True,
        "text": {
            "uz": "9️⃣ Qaysi nasiya savdo tashkilotlari xizmatlaridan foydalangansiz?\n(bir nechta javob tanlash mumkin)",
            "ru": "9️⃣ Услугами каких организаций «Насия савдо» пользовались?\n(можно несколько)",
            "en": "9️⃣ Which installment trade companies have you used?\n(multiple choice allowed)",
        },
        "options": {
            "uz": ["Alif nasiya", "Uzum nasiya", "TBC nasiya", "AllGood nasiya", "Texnomart", "Ishonch", "Mediapark", "Idea", "Yandex split", "Asaxiy", "Boshqa"],
            "ru": ["Alif nasiya", "Uzum nasiya", "TBC nasiya", "AllGood nasiya", "Texnomart", "Ishonch", "Mediapark", "Idea", "Yandex split", "Asaxiy", "Другое"],
            "en": ["Alif nasiya", "Uzum nasiya", "TBC nasiya", "AllGood nasiya", "Texnomart", "Ishonch", "Mediapark", "Idea", "Yandex split", "Asaxiy", "Other"],
        },
    },
    {
        "id": "avg_purchase",
        "kind": "choice",
        "text": {"uz": "🔟 O'rtacha bitta xarid summasi:", "ru": "🔟 Средняя сумма одной покупки:", "en": "🔟 Average single purchase amount:"},
        "options": {
            "uz": ["1 mln so'mgacha", "1–5 mln so'm", "6–10 mln so'm", "11–50 mln so'm", "50 mln so'mdan ortiq"],
            "ru": ["до 1 млн сум", "1–5 млн сум", "6–10 млн сум", "11–50 млн сум", "более 50 млн сум"],
            "en": ["Up to 1 mln UZS", "1–5 mln UZS", "6–10 mln UZS", "11–50 mln UZS", "Above 50 mln UZS"],
        },
    },
    {
        "id": "product_types",
        "kind": "multi",
        "max_select": 10,
        "text": {
            "uz": "1️⃣1️⃣ Nasiya savdo orqali asosan qaysi mahsulot/xizmatlarni xarid qilasiz?\n(bir nechta javob tanlash mumkin)",
            "ru": "1️⃣1️⃣ Какие товары/услуги покупаете чаще всего?\n(можно несколько)",
            "en": "1️⃣1️⃣ What do you mostly buy via installments?\n(multiple choice allowed)",
        },
        "options": {
            "uz": ["Elektronika", "Maishiy texnika", "Mebel va jihoz", "Qurilish va ta'mirlash", "Oziq-ovqat mahsulotlari", "Kiyim-kechak", "Sayohat / xizmatlar", "Avtomashina", "Ko'chmas mulk (turar / noturar joy)", "Boshqa"],
            "ru": ["Электроника", "Бытовая техника", "Мебель и оборудование", "Строительство и ремонт", "Продукты питания", "Одежда", "Путешествия / услуги", "Автомобиль", "Недвижимость", "Другое"],
            "en": ["Electronics", "Home appliances", "Furniture & equipment", "Construction & renovation", "Food products", "Clothing", "Travel / services", "Car", "Real estate", "Other"],
        },
    },

    # ======== III. Multiple obligations ========
    {
        "id": "_section_3",
        "kind": "section",
        "text": {"uz": "section_3", "ru": "section_3", "en": "section_3"},
    },
    {
        "id": "multi_company_use",
        "kind": "choice",
        "text": {
            "uz": "1️⃣2️⃣ Bir vaqtning o'zida bir nechta nasiya savdo tashkiloti xizmatidan foydalanasizmi?",
            "ru": "1️⃣2️⃣ Пользуетесь ли сразу несколькими организациями «Насия савдо»?",
            "en": "1️⃣2️⃣ Do you use multiple installment trade companies at the same time?",
        },
        "options": YESNO,
    },
    {
        "id": "income_share_percent",
        "kind": "choice",
        "text": {
            "uz": "1️⃣3️⃣ Nasiya savdo bo'yicha oylik to'lovlaringiz daromadingizning taxminan necha foizini tashkil etadi?",
            "ru": "1️⃣3️⃣ Какой примерно процент дохода уходит на ежемесячные платежи по «Насия савдо»?",
            "en": "1️⃣3️⃣ What % of your income goes to monthly installment payments?",
        },
        "options": {
            "uz": ["10–25 foiz", "26–50 foiz", "51–100 foiz", "100 foizdan yuqori"],
            "ru": ["10–25%", "26–50%", "51–100%", "более 100%"],
            "en": ["10–25%", "26–50%", "51–100%", "Over 100%"],
        },
    },
    {
        "id": "debt_burden_checked",
        "kind": "choice",
        "text": {
            "uz": "1️⃣4️⃣ Nasiya savdo orqali mahsulot/xizmatlar xarid qilganingizda qarz yuki darajangiz hisobga olinganmi?\n\n💡 _Izoh: Qarz yuki – oylik qarz to'lovlaringiz oylik daromadingizning qancha qismini tashkil etishi._",
            "ru": "1️⃣4️⃣ Учитывалась ли ваша долговая нагрузка при покупке?\n\n💡 _Примечание: Долговая нагрузка — доля ежемесячных выплат по долгам в вашем ежемесячном доходе._",
            "en": "1️⃣4️⃣ Was your debt burden considered at purchase?\n\n💡 _Note: Debt burden — the share of monthly debt payments relative to your monthly income._",
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
        "id": "contract_terms_clear",
        "kind": "choice",
        "text": {
            "uz": "1️⃣5️⃣ Xarid qilishdan oldin shartnoma shartlari sizga aniq tushuntirilganmi?",
            "ru": "1️⃣5️⃣ Были ли вам понятно разъяснены условия договора до покупки?",
            "en": "1️⃣5️⃣ Were the contract terms clearly explained to you before purchase?",
        },
        "options": YESNO,
    },
    {
        "id": "total_cost_clear",
        "kind": "choice",
        "text": {
            "uz": "1️⃣6️⃣ Xarid qilishdan oldin umumiy to'lov summasi sizga tushunarli bo'lganmi?",
            "ru": "1️⃣6️⃣ Была ли вам понятна общая сумма оплаты до покупки?",
            "en": "1️⃣6️⃣ Was the total payment amount clear to you before purchase?",
        },
        "options": YESNO,
    },
    {
        "id": "schedule_given",
        "kind": "choice",
        "text": {
            "uz": "1️⃣7️⃣ To'lov jadvali (muddatlar va summalar) sizga berilganmi?",
            "ru": "1️⃣7️⃣ Был ли вам предоставлен график платежей (сроки и суммы)?",
            "en": "1️⃣7️⃣ Were you given a payment schedule (dates and amounts)?",
        },
        "options": YESNO,
    },

    # ======== V. Payment difficulties ========
    {
        "id": "_section_5",
        "kind": "section",
        "text": {"uz": "section_5", "ru": "section_5", "en": "section_5"},
    },
    {
        "id": "missed_payment",
        "kind": "choice",
        "text": {
            "uz": "1️⃣8️⃣ Nasiya savdo bo'yicha oylik to'lovni kechiktirib yoki o'tkazib yuborganmisiz?",
            "ru": "1️⃣8️⃣ Были ли просрочки/пропуски ежемесячных платежей?",
            "en": "1️⃣8️⃣ Have you delayed or missed a monthly payment?",
        },
        "options": YESNO,
    },
    {
        "id": "difficulty_reason",
        "kind": "choice",
        "text": {
            "uz": "1️⃣9️⃣ Agar to'lovda qiyinchilik holati kuzatilgan bo'lsa, asosiy sababi nimada?",
            "ru": "1️⃣9️⃣ Если были трудности с оплатой, какова основная причина?",
            "en": "1️⃣9️⃣ If you had payment difficulties, what was the main reason?",
        },
        "options": {
            "uz": ["Daromadning kamayishi", "Ish yo'qotilishi", "Oylik to'lov daromaddan yuqoriligi", "Narxlar oshishi", "Sog'liq bilan bog'liq sabablar", "Boshqa"],
            "ru": ["Снижение дохода", "Потеря работы", "Ежемесячный платёж выше дохода", "Рост цен", "Проблемы со здоровьем", "Другое"],
            "en": ["Income decreased", "Job loss", "Monthly payment exceeds income", "Prices increased", "Health reasons", "Other"],
        },
    },
    {
        "id": "borrowed_for_payments",
        "kind": "choice",
        "text": {
            "uz": "2️⃣0️⃣ Nasiya savdo to'lovlarini amalga oshirish uchun boshqa qarz olganmisiz?",
            "ru": "2️⃣0️⃣ Брали ли вы другой займ, чтобы оплатить платежи «Насия савдо»?",
            "en": "2️⃣0️⃣ Did you borrow elsewhere to make installment payments?",
        },
        "options": YESNO,
    },
    {
        "id": "cut_essential_spending",
        "kind": "choice",
        "text": {
            "uz": "2️⃣1️⃣ Nasiya savdo sababli asosiy (zarur) xarajatlaringizni qisqartirganmisiz?",
            "ru": "2️⃣1️⃣ Сокращали ли вы необходимые расходы из-за платежей «Насия савдо»?",
            "en": "2️⃣1️⃣ Did you cut essential spending due to installment payments?",
        },
        "options": YESNO,
    },
    {
        "id": "used_for_cash_need",
        "kind": "choice",
        "text": {
            "uz": "2️⃣2️⃣ Nasiya savdo xizmatidan tovar xarid qilishdan tashqari, pul yetishmovchiligini qoplash yoki shoshilinch moliyaviy ehtiyojlar uchun ham foydalanganmisiz?",
            "ru": "2️⃣2️⃣ Использовали ли «Насия савдо» не только для покупок, но и для покрытия нехватки средств или срочных финансовых нужд?",
            "en": "2️⃣2️⃣ Did you use installment services not only for purchases, but also to cover cash shortages or urgent financial needs?",
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
            "uz": "2️⃣3️⃣ Nasiya savdo kompaniyasi qarzni undirishda qanday usuldan foydalangan?\n(bir nechta javob tanlash mumkin)",
            "ru": "2️⃣3️⃣ Какими способами компания взыскивала долг?\n(можно несколько)",
            "en": "2️⃣3️⃣ What methods did the company use for debt collection?\n(multiple choice allowed)",
        },
        "options": {
            "uz": ["SMS xabarnomasi", "Telefon qo'ng'iroqlari", "Mobil ilova orqali bildirishnoma", "Avtomatik hisobdan yechish (avtospisaniya)", "Tashqi kollektor", "Sud orqali"],
            "ru": ["SMS-уведомления", "Телефонные звонки", "Уведомления в приложении", "Автосписание", "Внешний коллектор", "Через суд"],
            "en": ["SMS notifications", "Phone calls", "In-app notifications", "Auto-debit", "External collector", "Through court"],
        },
    },
    {
        "id": "aggressive_collection",
        "kind": "choice",
        "text": {
            "uz": "2️⃣4️⃣ Sizga nisbatan agressiv yoki bosim o'tkazuvchi undirish holatlari bo'lganmi?",
            "ru": "2️⃣4️⃣ Были ли случаи агрессивного или давящего взыскания в вашем отношении?",
            "en": "2️⃣4️⃣ Was there aggressive or pressuring collection towards you?",
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
            "uz": "2️⃣5️⃣ Nasiya savdo tashkilotiga shikoyat qilganmisiz?",
            "ru": "2️⃣5️⃣ Подавали ли вы жалобу организации «Насия савдо»?",
            "en": "2️⃣5️⃣ Did you submit a complaint to the installment trade company?",
        },
        "options": YESNO,
        "has_sub_if_yes": True,
    },
    # Sub-question: shown only if complaint_submitted = Yes
    {
        "id": "complaint_reason",
        "kind": "multi",
        "max_select": 5,
        "text": {
            "uz": "25.1. Shikoyatingizning sababi nima?\n(bir nechta javob tanlash mumkin)",
            "ru": "25.1. Какова причина вашей жалобы?\n(можно несколько)",
            "en": "25.1. What was the reason for your complaint?\n(multiple choice allowed)",
        },
        "options": {
            "uz": [
                "Yashirin to'lovlar va jarimalar",
                "Mijoz roziligisiz to'lov muddati yoki summaning o'zgartirilishi",
                "Mijoz ma'lumotlarining roziligisiz uchinchi shaxsga berilishi",
                "Mijoz hisobidan ruxsatsiz pul yechilishi",
                "Boshqa",
            ],
            "ru": [
                "Скрытые платежи и штрафы",
                "Изменение срока/суммы платежа без согласия клиента",
                "Передача данных клиента третьим лицам без согласия",
                "Списание средств со счёта без разрешения клиента",
                "Другое",
            ],
            "en": [
                "Hidden fees and penalties",
                "Payment term/amount changed without client consent",
                "Client data shared with third parties without consent",
                "Funds deducted from account without permission",
                "Other",
            ],
        },
        "conditional_on": "complaint_submitted",
        "conditional_value_yes": True,
    },
    {
        "id": "complaint_resolved",
        "kind": "choice",
        "text": {
            "uz": "25.2. Shikoyatingiz ijobiy hal qilinganmi?",
            "ru": "25.2. Была ли ваша жалоба удовлетворительно решена?",
            "en": "25.2. Was your complaint resolved satisfactorily?",
        },
        "options": YESNO,
        "conditional_on": "complaint_submitted",
        "conditional_value_yes": True,
    },

    # ======== VIII. Financial awareness ========
    {
        "id": "_section_8",
        "kind": "section",
        "text": {"uz": "section_8", "ru": "section_8", "en": "section_8"},
    },
    {
        "id": "impulse_buying",
        "kind": "choice",
        "text": {
            "uz": "2️⃣6️⃣ Nasiya savdo xizmatlari odatda rejalashtirilmagan xaridlarni ko'paytiradi deb hisoblaysizmi?",
            "ru": "2️⃣6️⃣ Считаете ли, что услуги «Насия савдо» обычно увеличивают незапланированные покупки?",
            "en": "2️⃣6️⃣ Do you think installment services increase unplanned purchases?",
        },
        "options": YESNO,
    },
    {
        "id": "need_stricter_regulation",
        "kind": "choice",
        "text": {
            "uz": "2️⃣7️⃣ Sizningcha, nasiya savdo bozori davlat tomonidan tartibga solinishi zarurmi?",
            "ru": "2️⃣7️⃣ Нужно ли, по-вашему, государственное регулирование рынка «Насия савдо»?",
            "en": "2️⃣7️⃣ Should the installment trade market be regulated by the government?",
        },
        "options": {
            "uz": ["Zarur", "Zarur emas", "Javob berishga qiynalaman"],
            "ru": ["Нужно", "Не нужно", "Затрудняюсь ответить"],
            "en": ["Necessary", "Not necessary", "Hard to say"],
        },
    },
]


# ================================================================
#  Dynamic survey builder — at runtime we pick the right branch
# ================================================================

def build_survey(branch: str) -> List[Dict[str, Any]]:
    """Build the full survey list based on the branch."""
    base = list(SURVEY_PROFILE) + [Q_EVER_USED]
    if branch == "no":
        return base + SURVEY_NO_BRANCH
    else:
        return base + SURVEY_YES_BRANCH


# CSV headers and keys (combined for both branches)
CSV_HEADERS_UZ = [
    "timestamp", "user_id", "username", "language",
    "Yashash hududi", "Hudud ID", "Yosh guruhi", "Jins", "Bandlik holati", "O'rtacha oylik daromad",
    "Foydalanganmi",
    # No-branch
    "Avval eshitganmi", "Ishonch darajasi", "Shartlar tushunarli", "Foydali deb hisoblaydi",
    "Qaror omillari", "Yaxshiroq shartlarda foydalanadi", "Kimlar uchun maqbul", "Kerakli sohalar",
    "Impulsiv xarid (nofoydalanuvchi)", "Tartibga solish zarur (nofoydalanuvchi)",
    # Yes-branch
    "Chastota (1 yil)", "Foydalanish muddati", "Kompaniyalar", "O'rtacha xarid summasi", "Mahsulot turlari",
    "Bir nechta kompaniya", "Daromadga nisbatan %", "Qarz yuki hisobga olingan",
    "Shartnoma shartlari aniq", "Umumiy summa tushunarli", "To'lov jadvali berilgan",
    "Kechikish bo'lgan", "Qiyinchilik sababi", "To'lov uchun boshqa qarz",
    "Zarur xarajatni qisqartirdi", "Pul ehtiyoji uchun",
    "Aloqa usullari", "Agressiv undirish",
    "Shikoyat bergan", "Shikoyat sababi", "Shikoyat hal qilingan",
    "Qoniqish (1-5)", "Tavsiya qiladi",
    "Shartnoma o'qigan", "Limitni biladi", "Impulsiv xarid", "Tartibga solish",
]

CSV_KEYS = [
    "timestamp", "user_id", "username", "language",
    "region_city", "region_city_id", "age_group", "gender", "employment", "income",
    "ever_used",
    # No-branch
    "heard_before", "trust_level", "terms_understandable", "is_useful",
    "decision_factors", "would_use_if_better", "best_for_whom", "needed_sectors",
    "nu_impulse_buying", "nu_need_regulation",
    # Yes-branch
    "freq_1y", "usage_duration", "company_name", "avg_purchase", "product_types",
    "multi_company_use", "income_share_percent", "debt_burden_checked",
    "contract_terms_clear", "total_cost_clear", "schedule_given",
    "missed_payment", "difficulty_reason", "borrowed_for_payments",
    "cut_essential_spending", "used_for_cash_need",
    "contact_methods", "aggressive_collection",
    "complaint_submitted", "complaint_reason", "complaint_resolved",
    "satisfaction_1_5", "recommend",
    "read_contract", "know_limit", "impulse_buying", "need_stricter_regulation",
]


# ---------------- Conversation states ----------------
LANG, SURVEY_FLOW = range(2)


# ---------------- Helpers ----------------

def get_lang(ctx: ContextTypes.DEFAULT_TYPE) -> str:
    return ctx.user_data.get("lang", "uz")


def get_survey(ctx: ContextTypes.DEFAULT_TYPE) -> List[Dict[str, Any]]:
    """Return the current survey branch for this user."""
    branch = ctx.user_data.get("branch", "yes")
    return build_survey(branch)


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


def normalize_multi_selection(selected_raw: List[Any], options: List[str]) -> set:
    selected_indices: set = set()
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


def is_yes_answer(ans: str, lang: str) -> bool:
    ans_norm = (ans or "").strip().lower()
    yes_map = {
        "uz": {"ha"},
        "ru": {"да"},
        "en": {"yes"},
    }
    return ans_norm in yes_map.get(lang, set())


def is_no_answer(ans: str, lang: str) -> bool:
    ans_norm = (ans or "").strip().lower()
    no_map = {
        "uz": {"yo'q", "yoq"},
        "ru": {"нет"},
        "en": {"no"},
    }
    return ans_norm in no_map.get(lang, set())


def should_skip_conditional(q: Dict[str, Any], answers: Dict[str, Any], lang: str) -> bool:
    """Check if a conditional question should be skipped."""
    cond_on = q.get("conditional_on")
    if not cond_on:
        return False

    cond_val = answers.get(cond_on, "")
    if q.get("conditional_value_yes"):
        # Show only if the conditional field answer is "Yes"
        return not is_yes_answer(str(cond_val), lang)
    return False


# ---------------- Question sender ----------------

async def send_question(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(ctx)
    survey = get_survey(ctx)
    i = int(ctx.user_data.get("q_index", 0))
    answers = ctx.user_data.get("answers", {})

    # Skip section markers (show headers) and conditional questions
    while i < len(survey):
        q = survey[i]
        if q["kind"] == "section":
            section_key = q["text"].get(lang, q["text"].get("uz", ""))
            section_text = tr(lang, section_key)
            await update.effective_chat.send_message(section_text, parse_mode="Markdown")
            i += 1
            ctx.user_data["q_index"] = i
            continue

        # Skip conditional questions if condition not met
        if should_skip_conditional(q, answers, lang):
            i += 1
            ctx.user_data["q_index"] = i
            continue

        break

    if i >= len(survey):
        await finalize(update, ctx)
        return

    q = survey[i]
    qid = q["id"]
    kind = q["kind"]

    text = q["text"].get(lang, q["text"].get("uz", ""))
    hint = q.get("hint", {}).get(lang)
    full_text = text + (f"\n\n💬 {hint}" if hint else "")

    if kind == "choice":
        opts = q["options"].get(lang, q["options"].get("uz", []))
        await update.effective_chat.send_message(full_text, reply_markup=kb_choice(lang, qid, opts), parse_mode="Markdown")
        return

    if kind == "multi":
        opts = q["options"].get(lang, q["options"].get("uz", []))
        selected_raw = ctx.user_data.get(f"multi:{qid}", [])
        selected = normalize_multi_selection(selected_raw, opts)
        await update.effective_chat.send_message(
            full_text,
            reply_markup=kb_multi(lang, qid, opts, selected, tr(lang, "btn_done")),
            parse_mode="Markdown",
        )
        return

    if kind == "region":
        page = int(ctx.user_data.get("region_page", 0))
        await update.effective_chat.send_message(full_text, reply_markup=kb_regions(lang, page=page))
        return

    # text / number / percent
    await update.effective_chat.send_message(full_text, reply_markup=ReplyKeyboardRemove(), parse_mode="Markdown")


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
    ctx.user_data["branch"] = "yes"  # default, will change if user says No to Q6
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

    survey = get_survey(ctx)
    i = int(ctx.user_data.get("q_index", 0))
    answers = ctx.user_data.get("answers", {})

    # Skip section markers and conditional
    while i < len(survey):
        q = survey[i]
        if q["kind"] == "section":
            i += 1
            ctx.user_data["q_index"] = i
            continue
        if should_skip_conditional(q, answers, lang):
            i += 1
            ctx.user_data["q_index"] = i
            continue
        break

    if i >= len(survey):
        await finalize(update, ctx)
        return ConversationHandler.END

    q = survey[i]
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

        # ✅ BRANCH at Q6: ever_used
        if q.get("is_branch_question"):
            if is_no_answer(ans, lang):
                ctx.user_data["branch"] = "no"
                # Rebuild survey for this user with "no" branch
                new_survey = build_survey("no")
                # Set q_index to the question after ever_used in the "no" branch
                for j, sq in enumerate(new_survey):
                    if sq["id"] == "ever_used":
                        ctx.user_data["q_index"] = j + 1
                        break
                await send_question(update, ctx)
                return SURVEY_FLOW
            else:
                ctx.user_data["branch"] = "yes"
                new_survey = build_survey("yes")
                for j, sq in enumerate(new_survey):
                    if sq["id"] == "ever_used":
                        ctx.user_data["q_index"] = j + 1
                        break
                await send_question(update, ctx)
                return SURVEY_FLOW

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
        selected_labels = [opts[idx] for idx in sorted(selected) if 0 <= idx < len(opts)]
        ctx.user_data["answers"][qid] = selected_labels

        # Check if "Boshqa/Другое/Other" was selected AND question has_other flag
        if q.get("has_other"):
            other_labels = {"boshqa", "другое", "other"}
            has_other_selected = any(lbl.lower() in other_labels for lbl in selected_labels)
            if has_other_selected:
                # Enter "waiting for other text" state
                ctx.user_data["waiting_other_for"] = qid
                await query.message.reply_text(tr(lang, "type_other_company"))
                return SURVEY_FLOW

        ctx.user_data["q_index"] = i + 1
        await send_question(update, ctx)
        return SURVEY_FLOW

    await query.message.reply_text(tr(lang, "invalid"))
    return SURVEY_FLOW


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(ctx)
    survey = get_survey(ctx)
    i = int(ctx.user_data.get("q_index", 0))
    answers = ctx.user_data.get("answers", {})
    msg = (update.message.text or "").strip()

    # Handle "Boshqa/Other" text input for multi-select questions
    waiting_qid = ctx.user_data.get("waiting_other_for")
    if waiting_qid:
        if len(msg) < 1:
            await update.message.reply_text(tr(lang, "invalid"))
            return SURVEY_FLOW

        # Replace "Boshqa/Другое/Other" in the saved answers with the typed text
        current_answers = ctx.user_data["answers"].get(waiting_qid, [])
        other_labels = {"boshqa", "другое", "other"}
        updated = []
        for item in current_answers:
            if item.lower() in other_labels:
                updated.append(msg)  # Replace with user's typed text
            else:
                updated.append(item)
        ctx.user_data["answers"][waiting_qid] = updated

        # Clear the waiting state and move on
        del ctx.user_data["waiting_other_for"]
        ctx.user_data["q_index"] = i + 1
        await send_question(update, ctx)
        return SURVEY_FLOW

    # Skip section markers and conditionals
    while i < len(survey):
        q = survey[i]
        if q["kind"] == "section":
            i += 1
            ctx.user_data["q_index"] = i
            continue
        if should_skip_conditional(q, answers, lang):
            i += 1
            ctx.user_data["q_index"] = i
            continue
        break

    if i >= len(survey):
        await finalize(update, ctx)
        return ConversationHandler.END

    q = survey[i]
    qid = q["id"]
    kind = q["kind"]

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

    export_path = await export_db_to_csv()
    if export_path and os.path.exists(export_path):
        await update.message.reply_document(
            document=open(export_path, "rb"),
            filename="survey_export_v3.csv",
            caption="📊 Nasiya survey export (PostgreSQL)",
        )
        return

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

    text = (
        f"📊 **So'rovnoma statistikasi**\n\n"
        f"📈 Jami javoblar: {stats.get('total', 0)}\n"
        f"📅 Bugun: {stats.get('today', 0)}\n"
        f"📆 Oxirgi 7 kun: {stats.get('week', 0)}\n"
        f"✅ Foydalanganlar: {stats.get('users_count', 0)}\n"
        f"❌ Foydalanmaganlar: {stats.get('non_users_count', 0)}\n"
        f"⭐ O'rtacha qoniqish: {stats.get('avg_satisfaction', 0)}/5\n\n"
        f"🏆 **Top hududlar:**\n"
    )
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
    await init_db()
    app = build_app()
    log.info("Bot started (v3 — with non-user branch).")

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

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
