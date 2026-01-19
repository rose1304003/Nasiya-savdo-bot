#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Nasiya Savdo Xizmatlari So'rovnoma Bot
✅ 3 languages: Uzbek (Latin), Russian, English
✅ Region buttons for Uzbekistan (paginated)
✅ Saves to CSV (+ optional Google Sheets)
✅ Admin export: /export
"""

import os
import csv
import tempfile
import logging
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

CSV_PATH = os.environ.get("CSV_PATH", "nasiya_survey_responses.csv")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

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

# ---------------- i18n ----------------
LANGS = {"uz": "O‘zbek", "ru": "Русский", "en": "English"}

T = {
    "choose_lang": {
        "uz": "Tilni tanlang:",
        "ru": "Выберите язык:",
        "en": "Choose language:",
    },
    "start": {
        "uz": "Assalomu alaykum! Nasiya savdo xizmatlari bo‘yicha so‘rovnomani boshlaymiz.",
        "ru": "Здравствуйте! Начнём опрос по услугам «Насия савдо».",
        "en": "Hello! Let’s start the survey about installment trade services (Nasiya Savdo).",
    },
    "btn_start": {"uz": "Boshlash ✅", "ru": "Начать ✅", "en": "Start ✅"},
    "btn_done": {"uz": "Tayyor ✅", "ru": "Готово ✅", "en": "Done ✅"},
    "invalid": {
        "uz": "Noto‘g‘ri javob. Iltimos, tugmalar orqali tanlang yoki to‘g‘ri formatda kiriting.",
        "ru": "Некорректный ответ. Пожалуйста, выберите кнопкой или введите в правильном формате.",
        "en": "Invalid answer. Please use buttons or enter a valid value.",
    },
    "saved": {
        "uz": "Rahmat! So‘rovnoma yakunlandi ✅",
        "ru": "Спасибо! Опрос завершён ✅",
        "en": "Thank you! The survey is completed ✅",
    },
    "export_only_admin": {
        "uz": "Kechirasiz, bu buyruq faqat adminlar uchun.",
        "ru": "Извините, команда только для админов.",
        "en": "Sorry, this command is for admins only.",
    },
    "no_data": {
        "uz": "Hali ma’lumot yo‘q.",
        "ru": "Данных пока нет.",
        "en": "No data yet.",
    },
}

def tr(lang: str, key: str) -> str:
    lang = lang if lang in LANGS else "uz"
    return T.get(key, {}).get(lang, T.get(key, {}).get("uz", key))

# ---------------- Uzbekistan regions (buttons) ----------------
UZB_REGIONS = [
    {"id": "qr",  "uz": "Qoraqalpog‘iston R.", "ru": "Республика Каракалпакстан", "en": "Republic of Karakalpakstan"},
    {"id": "an",  "uz": "Andijon",             "ru": "Андижанская",              "en": "Andijan"},
    {"id": "bu",  "uz": "Buxoro",              "ru": "Бухарская",               "en": "Bukhara"},
    {"id": "ji",  "uz": "Jizzax",              "ru": "Джизакская",              "en": "Jizzakh"},
    {"id": "qa",  "uz": "Qashqadaryo",         "ru": "Кашкадарьинская",         "en": "Kashkadarya"},
    {"id": "na",  "uz": "Navoiy",              "ru": "Навоийская",              "en": "Navoi"},
    {"id": "nm",  "uz": "Namangan",            "ru": "Наманганская",            "en": "Namangan"},
    {"id": "sa",  "uz": "Samarqand",           "ru": "Самаркандская",           "en": "Samarkand"},
    {"id": "su",  "uz": "Surxondaryo",         "ru": "Сурхандарьинская",        "en": "Surkhandarya"},
    {"id": "si",  "uz": "Sirdaryo",            "ru": "Сырдарьинская",           "en": "Syrdarya"},
    {"id": "ta",  "uz": "Toshkent vil.",       "ru": "Ташкентская обл.",        "en": "Tashkent Region"},
    {"id": "tk",  "uz": "Toshkent shahri",     "ru": "г. Ташкент",              "en": "Tashkent City"},
    {"id": "fa",  "uz": "Farg‘ona",            "ru": "Ферганская",              "en": "Fergana"},
    {"id": "xo",  "uz": "Xorazm",              "ru": "Хорезмская",              "en": "Khorezm"},
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
    "uz": ["Ha", "Yo‘q"],
    "ru": ["Да", "Нет"],
    "en": ["Yes", "No"],
}

SURVEY: List[Dict[str, Any]] = [
    # I. Respondent profile
    {
        "id": "region_city",
        "kind": "region",  # ✅ buttons (UZB regions)
        "text": {
            "uz": "1) Yashash hududi (viloyat / shahar):",
            "ru": "1) Регион проживания (область / город):",
            "en": "1) Place of residence (region / city):",
        },
    },
    {
        "id": "age_group",
        "kind": "choice",
        "text": {
            "uz": "2) Yosh guruhi:",
            "ru": "2) Возрастная группа:",
            "en": "2) Age group:",
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
        "text": {"uz": "3) Jins:", "ru": "3) Пол:", "en": "3) Gender:"},
        "options": {"uz": ["Erkak", "Ayol"], "ru": ["Мужчина", "Женщина"], "en": ["Male", "Female"]},
    },
    {
        "id": "employment",
        "kind": "choice",
        "text": {"uz": "4) Bandlik holati:", "ru": "4) Занятость:", "en": "4) Employment status:"},
        "options": {
            "uz": ["Ishlaydi (rasmiy)", "Ishlaydi (norasmiy)", "O‘zini-o‘zi band qilgan", "Talaba", "Nafaqada", "Ishsiz", "Boshqa"],
            "ru": ["Работаю (официально)", "Работаю (неофициально)", "Самозанятый(ая)", "Студент(ка)", "На пенсии", "Безработный(ая)", "Другое"],
            "en": ["Employed (formal)", "Employed (informal)", "Self-employed", "Student", "Retired", "Unemployed", "Other"],
        },
    },
    {
        "id": "income",
        "kind": "choice",
        "text": {"uz": "5) O‘rtacha oylik daromadingiz:", "ru": "5) Средний ежемесячный доход:", "en": "5) Average monthly income:"},
        "options": {
            "uz": ["0–2 mln so‘m", "2–5 mln so‘m", "5–10 mln so‘m", "10–20 mln so‘m", "20 mln so‘mdan yuqori"],
            "ru": ["0–2 млн сум", "2–5 млн сум", "5–10 млн сум", "10–20 млн сум", "более 20 млн сум"],
            "en": ["0–2 mln UZS", "2–5 mln UZS", "5–10 mln UZS", "10–20 mln UZS", "Above 20 mln UZS"],
        },
    },

    # II. Usage
    {
        "id": "freq_3m",
        "kind": "choice",
        "text": {
            "uz": "6) Oxirgi 3 oyda nasiya savdo xizmatidan foydalanish chastotasi:",
            "ru": "6) Частота использования за последние 3 месяца:",
            "en": "6) Frequency of use in the last 3 months:",
        },
        "options": {
            "uz": ["1 marta", "2–3 marta", "4–5 marta", "6 va undan ko‘p", "Umuman foydalanmagan"],
            "ru": ["1 раз", "2–3 раза", "4–5 раз", "6 и более", "Не пользовался(ась)"],
            "en": ["Once", "2–3 times", "4–5 times", "6 or more", "Did not use"],
        },
    },
    {
        "id": "months_using",
        "kind": "number",
        "min": 0,
        "max": 240,
        "text": {
            "uz": "7) Nasiya savdo xizmatlaridan qancha vaqtdan beri foydalanasiz? (oylarda)",
            "ru": "7) Сколько времени вы пользуетесь услугой? (в месяцах)",
            "en": "7) How long have you been using it? (in months)",
        },
        "hint": {"uz": "Masalan: 6", "ru": "Напр.: 6", "en": "E.g.: 6"},
    },
    {
        "id": "company_name",
        "kind": "text",
        "text": {
            "uz": "8) Qaysi nasiya savdo kompaniya xizmatidan foydalanasiz?",
            "ru": "8) Какой компанией (сервисом) вы пользуетесь?",
            "en": "8) Which Nasiya Savdo company/service do you use?",
        },
        "hint": {"uz": "Kompaniya nomini yozing", "ru": "Укажите название", "en": "Type the company name"},
    },
    {
        "id": "avg_purchase",
        "kind": "choice",
        "text": {"uz": "9) O‘rtacha bitta xarid summasi:", "ru": "9) Средняя сумма одной покупки:", "en": "9) Average purchase amount:"},
        "options": {
            "uz": ["5 mln so‘mgacha", "10 mln so‘mgacha", "50 mln so‘mgacha", "100 mln so‘mgacha", "500 mln so‘mdan ortiq"],
            "ru": ["до 5 млн", "до 10 млн", "до 50 млн", "до 100 млн", "более 500 млн"],
            "en": ["Up to 5 mln", "Up to 10 mln", "Up to 50 mln", "Up to 100 mln", "Above 500 mln"],
        },
    },
    {
        "id": "product_types",
        "kind": "multi",
        "max_select": 3,
        "text": {
            "uz": "10) Asosan qaysi mahsulot/xizmatlarni xarid qilasiz? (bir nechta tanlash mumkin)",
            "ru": "10) Какие товары/услуги вы покупаете чаще всего? (можно несколько)",
            "en": "10) What do you mostly buy? (multiple choice)",
        },
        "options": {
            "uz": ["Elektronika", "Kiyim-kechak", "Maishiy texnika", "Oziq-ovqat", "Qurilish mahsulotlari / avto ehtiyot qismlar", "Sayohat / xizmatlar", "Boshqa"],
            "ru": ["Электроника", "Одежда", "Бытовая техника", "Продукты питания", "Строит. товары / автозапчасти", "Путешествия / услуги", "Другое"],
            "en": ["Electronics", "Clothing", "Home appliances", "Food", "Construction goods / auto parts", "Travel / services", "Other"],
        },
    },

    # III. Multiple obligations / over-indebtedness
    {"id": "multi_company_use", "kind": "choice", "text": {
        "uz": "11) Bir vaqtning o‘zida bir nechta kompaniya xizmatidan foydalanasizmi?",
        "ru": "11) Пользуетесь ли сразу несколькими компаниями?",
        "en": "11) Do you use multiple companies at the same time?",
    }, "options": YESNO},
    {"id": "multi_company_debt", "kind": "choice", "text": {
        "uz": "12) Hozirda bir nechta kompaniyalar oldida qarzdorligingiz bormi?",
        "ru": "12) Есть ли у вас долги перед несколькими компаниями?",
        "en": "12) Do you currently have debts to multiple companies?",
    }, "options": YESNO},
    {
        "id": "income_share_percent",
        "kind": "percent",
        "text": {
            "uz": "13) Oylik to‘lovlaringiz daromadingizning taxminan necha foizini tashkil etadi? (%)",
            "ru": "13) Какой примерно процент дохода уходит на ежемесячные платежи? (%)",
            "en": "13) Approx. what % of your income goes to monthly payments? (%)",
        },
        "hint": {"uz": "0 dan 100 gacha son kiriting", "ru": "Введите число 0–100", "en": "Enter a number 0–100"},
    },
    {"id": "debt_burden_checked", "kind": "choice", "text": {
        "uz": "14) Xarid paytida qarz yuki darajangiz hisobga olinganmi?",
        "ru": "14) Учитывали ли вашу долговую нагрузку при покупке?",
        "en": "14) Was your debt burden considered at purchase?",
    }, "options": YESNO},
    {"id": "missed_payment", "kind": "choice", "text": {
        "uz": "15) To‘lovni kechiktirgan yoki o‘tkazib yuborgan holat bo‘lganmi?",
        "ru": "15) Были ли просрочки/пропуски платежей?",
        "en": "15) Have you delayed or missed a payment?",
    }, "options": YESNO},

    # IV. Transparency
    {"id": "total_cost_clear", "kind": "choice", "text": {
        "uz": "16) Xariddan oldin umumiy to‘lov summasi (total cost) tushunarli bo‘lganmi?",
        "ru": "16) Было ли понятно, какая итоговая стоимость (total cost) до покупки?",
        "en": "16) Was the total cost clear before purchase?",
    }, "options": YESNO},
    {"id": "fees_explained", "kind": "choice", "text": {
        "uz": "17) Foizlar va qo‘shimcha to‘lovlar oldindan aniq tushuntirilganmi?",
        "ru": "17) Объяснили ли заранее проценты и дополнительные платежи?",
        "en": "17) Were interest and extra fees explained in advance?",
    }, "options": YESNO},
    {"id": "schedule_given", "kind": "choice", "text": {
        "uz": "18) To‘lov jadvali (muddatlar va summalar) berilganmi?",
        "ru": "18) Выдали ли график платежей (сроки и суммы)?",
        "en": "18) Were you given a payment schedule (dates and amounts)?",
    }, "options": YESNO},

    # V. Difficulties / financial pressure
    {
        "id": "difficulty_reason",
        "kind": "choice",
        "text": {
            "uz": "19) Agar to‘lovda qiyinchilik bo‘lgan bo‘lsa, asosiy sabab nima edi?",
            "ru": "19) Если были трудности с оплатой, какова основная причина?",
            "en": "19) If you had payment difficulties, what was the main reason?",
        },
        "options": {
            "uz": ["Daromad kamayishi", "Ish yo‘qotilishi", "Narxlar oshishi", "Sog‘liq bilan bog‘liq sabablar", "Boshqa"],
            "ru": ["Снижение дохода", "Потеря работы", "Рост цен", "Проблемы со здоровьем", "Другое"],
            "en": ["Income decreased", "Job loss", "Prices increased", "Health reasons", "Other"],
        },
    },
    {"id": "borrowed_for_payments", "kind": "choice", "text": {
        "uz": "20) To‘lovlarni amalga oshirish uchun boshqa qarz olganmisiz?",
        "ru": "20) Брали ли вы другой займ, чтобы оплатить платежи?",
        "en": "20) Did you borrow elsewhere to make payments?",
    }, "options": YESNO},
    {"id": "cut_essential_spending", "kind": "choice", "text": {
        "uz": "21) Nasiya savdo sababli zarur xarajatlaringizni qisqartirganmisiz?",
        "ru": "21) Сокращали ли вы необходимые расходы из-за платежей?",
        "en": "21) Did you cut essential spending due to installment payments?",
    }, "options": YESNO},
    {"id": "used_for_cash_need", "kind": "choice", "text": {
        "uz": "22) Pul ehtiyojingiz uchun nasiya savdodan foydalanganmisiz?",
        "ru": "22) Использовали ли «насия» из-за нехватки денег/нужды в средствах?",
        "en": "22) Did you use installment services due to cash needs?",
    }, "options": YESNO},

    # VI. Collection practices
    {
        "id": "contact_methods",
        "kind": "multi",
        "max_select": 3,
        "text": {
            "uz": "23) Kompaniya siz bilan qanday aloqa qilgan? (bir nechta tanlash mumkin)",
            "ru": "23) Какими способами компания связывалась с вами? (можно несколько)",
            "en": "23) How did the company contact you? (multiple choice)",
        },
        "options": {
            "uz": ["SMS", "Avtomatik hisobdan yechish (avtospisaniya)", "Mobil ilova orqali bildirishnoma", "Telefon qo‘ng‘iroqlari", "Tashqi kollektor", "Sud orqali"],
            "ru": ["SMS", "Автосписание", "Уведомление в приложении", "Телефонные звонки", "Внешний коллектор", "Через суд"],
            "en": ["SMS", "Auto-debit", "In-app notification", "Phone calls", "External collector", "Through court"],
        },
    },
    {"id": "aggressive_collection", "kind": "choice", "text": {
        "uz": "24) Agressiv yoki bosim o‘tkazuvchi undirish holatlari bo‘lganmi?",
        "ru": "24) Были ли случаи агрессивного/давящего взыскания?",
        "en": "24) Was there aggressive or pressuring collection?",
    }, "options": YESNO},

    # VII. Complaints & trust
    {"id": "complaint_submitted", "kind": "choice", "text": {
        "uz": "25) Kompaniyaga shikoyat berganmisiz?",
        "ru": "25) Подавали ли вы жалобу компании?",
        "en": "25) Did you submit a complaint to the company?",
    }, "options": YESNO},
    {"id": "complaint_resolved", "kind": "choice", "text": {
        "uz": "26) Shikoyat bergan bo‘lsangiz, u hal qilinganmi?",
        "ru": "26) Если жаловались, решилась ли проблема?",
        "en": "26) If yes, was it resolved?",
    }, "options": YESNO},
    {
        "id": "satisfaction_1_5",
        "kind": "choice",
        "text": {
            "uz": "27) Umumiy qoniqish darajangiz (1–5):",
            "ru": "27) Общая удовлетворённость (1–5):",
            "en": "27) Overall satisfaction (1–5):",
        },
        "options": {"uz": ["1", "2", "3", "4", "5"], "ru": ["1", "2", "3", "4", "5"], "en": ["1", "2", "3", "4", "5"]},
    },
    {"id": "recommend", "kind": "choice", "text": {
        "uz": "28) Boshqalarga tavsiya qilarmidingiz?",
        "ru": "28) Порекомендовали бы другим?",
        "en": "28) Would you recommend it to others?",
    }, "options": YESNO},

    # VIII. Financial awareness / behavior
    {"id": "read_contract", "kind": "choice", "text": {
        "uz": "29) Shartnoma shartlarini o‘qib chiqqanmisiz?",
        "ru": "29) Читали ли условия договора?",
        "en": "29) Did you read the contract terms?",
    }, "options": YESNO},
    {"id": "know_limit", "kind": "choice", "text": {
        "uz": "30) Ajratilgan limitni bilasizmi?",
        "ru": "30) Знаете ли вы свой лимит?",
        "en": "30) Do you know your assigned limit?",
    }, "options": YESNO},
    {"id": "impulse_buying", "kind": "choice", "text": {
        "uz": "31) Nasiya savdo impulsiv xaridlarni ko‘paytiradi deb hisoblaysizmi?",
        "ru": "31) Считаете ли, что «насия» увеличивает импульсивные покупки?",
        "en": "31) Do you think installment services increase impulse buying?",
    }, "options": YESNO},
    {
        "id": "need_stricter_regulation",
        "kind": "choice",
        "text": {
            "uz": "32) Sizningcha, bozorni qat’iyroq tartibga solish zarurmi?",
            "ru": "32) Нужно ли более строго регулировать рынок?",
            "en": "32) Is stricter regulation necessary?",
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
        [InlineKeyboardButton("O‘zbek 🇺🇿", callback_data="lang:uz")],
        [InlineKeyboardButton("Русский 🇷🇺", callback_data="lang:ru")],
        [InlineKeyboardButton("English 🇬🇧", callback_data="lang:en")],
    ])

def kb_choice(lang: str, options: List[str]) -> InlineKeyboardMarkup:
    rows = []
    for opt in options:
        rows.append([InlineKeyboardButton(opt, callback_data=f"ans:{opt}")])
    return InlineKeyboardMarkup(rows)

def kb_multi(lang: str, qid: str, options: List[str], selected: set, done_label: str) -> InlineKeyboardMarkup:
    rows = []
    for opt in options:
        mark = "✅ " if opt in selected else ""
        rows.append([InlineKeyboardButton(f"{mark}{opt}", callback_data=f"mul:{qid}:{opt}")])
    rows.append([InlineKeyboardButton(done_label, callback_data=f"mul_done:{qid}")])
    return InlineKeyboardMarkup(rows)

def kb_regions(lang: str, page: int = 0, per_page: int = 8) -> InlineKeyboardMarkup:
    # 2 columns x 4 rows = 8 per page
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

async def send_question(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(ctx)
    i = int(ctx.user_data.get("q_index", 0))

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
        await update.effective_chat.send_message(full_text, reply_markup=kb_choice(lang, opts))
        return

    if kind == "multi":
        opts = q["options"].get(lang, q["options"].get("uz", []))
        selected = set(ctx.user_data.get(f"multi:{qid}", []))
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

        # Save localized label + stable id (best for analysis)
        ctx.user_data["answers"]["region_city_id"] = rid
        ctx.user_data["answers"][qid] = reg.get(lang, reg["uz"])

        ctx.user_data["region_page"] = 0
        ctx.user_data["q_index"] = i + 1
        await send_question(update, ctx)
        return SURVEY_FLOW

    # --- single choice ---
    if data.startswith("ans:") and kind == "choice":
        ans = data.split(":", 1)[1]
        ctx.user_data["answers"][qid] = ans
        ctx.user_data["q_index"] = i + 1
        await send_question(update, ctx)
        return SURVEY_FLOW

    # --- multi toggle ---
    if data.startswith("mul:") and kind == "multi":
        _, qid2, opt = data.split(":", 2)
        if qid2 != qid:
            return SURVEY_FLOW

        key = f"multi:{qid}"
        selected = set(ctx.user_data.get(key, []))
        if opt in selected:
            selected.remove(opt)
        else:
            if len(selected) < int(q.get("max_select", 3)):
                selected.add(opt)
        ctx.user_data[key] = list(selected)

        opts = q["options"].get(lang, q["options"].get("uz", []))
        await query.message.edit_reply_markup(reply_markup=kb_multi(lang, qid, opts, selected, tr(lang, "btn_done")))
        return SURVEY_FLOW

    # --- multi done ---
    if data.startswith("mul_done:") and kind == "multi":
        qid2 = data.split(":", 1)[1]
        if qid2 != qid:
            return SURVEY_FLOW
        selected = ctx.user_data.get(f"multi:{qid}", [])
        ctx.user_data["answers"][qid] = selected
        ctx.user_data["q_index"] = i + 1
        await send_question(update, ctx)
        return SURVEY_FLOW

    await query.message.reply_text(tr(lang, "invalid"))
    return SURVEY_FLOW

async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(ctx)
    i = int(ctx.user_data.get("q_index", 0))
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

    # If user typed while buttons are expected
    await update.message.reply_text(tr(lang, "invalid"))
    return SURVEY_FLOW

async def finalize(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(ctx)
    answers = ctx.user_data.get("answers", {})

    # Save to CSV
    try:
        append_csv(answers)
    except Exception as e:
        log.error("CSV save error: %s", e)

    # Optional: Google Sheets
    gs_name = os.getenv("GOOGLE_SHEET_NAME", "").strip()
    gs_ws = os.getenv("GOOGLE_SHEET_WORKSHEET", "Responses").strip()
    if gs_name:
        err = try_gs_save_row(gs_name, gs_ws, answers, CSV_HEADERS_UZ, CSV_KEYS)
        if err:
            log.warning("Google Sheets not saved: %s", err)

    await update.effective_chat.send_message(tr(lang, "saved"), reply_markup=ReplyKeyboardRemove())
    ctx.user_data.clear()

async def cmd_export(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(ctx)
    uid = update.effective_user.id if update.effective_user else 0
    if uid not in ADMIN_IDS:
        await update.message.reply_text(tr(lang, "export_only_admin"))
        return

    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        await update.message.reply_text(tr(lang, "no_data"))
        return

    await update.message.reply_document(
        document=open(CSV_PATH, "rb"),
        filename=os.path.basename(CSV_PATH),
        caption="Nasiya survey export (CSV)",
    )

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
    return app

def main():
    app = build_app()
    log.info("Bot started.")
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
