import os
import re
import json
import math
import asyncio
import aiohttp
from typing import Any, Dict, List, Tuple
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY не задан. Добавь его в .env")

INPUT_FILE = "raw_reviews_db.json"
OUTPUT_FILE = "review_enrichment_v2.json"
MODEL = "gpt-4o-mini"

# Настройки
BATCH_SIZE = 8
BATCH_PAUSE = 1.25
MAX_REVIEWS_PER_ITEM = 12
MAX_REVIEW_TEXT_LEN = 700
REQUEST_TIMEOUT = 120

# ------------------------------------------------------------
# PROMPT
# ------------------------------------------------------------

SYSTEM_PROMPT = """
Ты — эксперт по кальянным табакам и миксологии.

Тебе дадут JSON с объектами вкусов табака.
По каждому вкусу нужно вернуть только валидный JSON, без markdown и пояснений.

Формат ответа:

{
  "items": [
    {
      "brand": "Black Burn",
      "flavor": "Peach Yogurt",
      "url": "https://...",
      "summary": {
        "profiles": ["fruit", "creamy", "dessert"],
        "sweetness": 4,
        "sourness": 1,
        "cooling": 0,
        "strength_feel": 2,
        "heat_resistance": 2,
        "mixability": 5,
        "longevity": 3,
        "complexity": 3
      },
      "usage_model": {
        "solo_score": 5,
        "mix_score": 5,
        "tool_score": 2,
        "dominance": 2,
        "versatility": 4
      },
      "behavior_model": {
        "overheat_risk": 5,
        "flavor_stability": 2,
        "chemical_risk": 1,
        "dryness": 1,
        "harshness": 1
      },
      "mix_guidance": {
        "recommended_share_min": 20,
        "recommended_share_max": 60,
        "best_usage": ["solo", "mix"],
        "best_pairings": ["манго", "банан", "персик"],
        "avoid_pairings": ["жесткая мята"]
      },
      "editorial": {
        "pros": ["точное попадание во вкус", "сливочный профиль"],
        "cons": ["легко перегреть", "аромка быстро улетает"],
        "setup_notes": ["не любит сильный жар", "лучше без колпака"],
        "one_liner": "Нежный сливочно-персиковый йогурт, вкусный, но чувствительный к жару."
      }
    }
  ]
}

ПРАВИЛА:
1. Отвечай только валидным JSON.
2. Все числовые поля summary/usage_model/behavior_model строго в диапазоне 0..5.
3. recommended_share_min/max — это проценты 0..100.
4. profiles — только из списка:
   ["berry", "citrus", "fruit", "tropical", "dessert", "creamy", "floral", "spicy", "beverage", "tobacco", "fresh", "herbal", "candy", "sour"]
5. best_usage — только из списка:
   ["solo", "mix", "solo_and_mix", "tool"]
6. pros/cons/setup_notes/best_pairings/avoid_pairings — максимум 3 коротких элемента.
7. one_liner — максимум 140 символов.
8. Если данных мало, делай осторожные выводы.
9. Не тащи лишнюю воду. Нужна плотная, рабочая выжимка под продукт.
"""

# ------------------------------------------------------------
# НОРМАЛИЗАЦИЯ / САНИТАЙЗ
# ------------------------------------------------------------

ALLOWED_PROFILES = {
    "berry", "citrus", "fruit", "tropical", "dessert", "creamy",
    "floral", "spicy", "beverage", "tobacco", "fresh", "herbal",
    "candy", "sour"
}
ALLOWED_USAGE = {"solo", "mix", "solo_and_mix", "tool"}


def clamp_int(value: Any, min_v: int, max_v: int, default: int = 0) -> int:
    try:
        num = int(round(float(value)))
    except Exception:
        num = default
    return max(min_v, min(max_v, num))


def clamp_float(value: Any, min_v: float, max_v: float, default: float = 0.0) -> float:
    try:
        num = float(value)
    except Exception:
        num = default
    return max(min_v, min(max_v, num))


def norm_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def norm_key(brand: str, flavor: str, url: str = "") -> str:
    return f"{norm_text(brand).lower()}|{norm_text(flavor).lower()}|{norm_text(url).lower()}"


def ensure_list_strings(value: Any, limit: int = 3) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        s = norm_text(item)
        if s and s not in out:
            out.append(s)
    return out[:limit]


def sanitize_summary(obj: Dict[str, Any]) -> Dict[str, Any]:
    profiles = [p for p in ensure_list_strings(obj.get("profiles", []), 5) if p in ALLOWED_PROFILES]
    return {
        "profiles": profiles,
        "sweetness": clamp_int(obj.get("sweetness", 0), 0, 5, 0),
        "sourness": clamp_int(obj.get("sourness", 0), 0, 5, 0),
        "cooling": clamp_int(obj.get("cooling", 0), 0, 5, 0),
        "strength_feel": clamp_int(obj.get("strength_feel", 2), 0, 5, 2),
        "heat_resistance": clamp_int(obj.get("heat_resistance", 2), 0, 5, 2),
        "mixability": clamp_int(obj.get("mixability", 3), 0, 5, 3),
        "longevity": clamp_int(obj.get("longevity", 3), 0, 5, 3),
        "complexity": clamp_int(obj.get("complexity", 3), 0, 5, 3),
    }


def sanitize_usage_model(obj: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "solo_score": clamp_int(obj.get("solo_score", 3), 0, 5, 3),
        "mix_score": clamp_int(obj.get("mix_score", 3), 0, 5, 3),
        "tool_score": clamp_int(obj.get("tool_score", 1), 0, 5, 1),
        "dominance": clamp_int(obj.get("dominance", 2), 0, 5, 2),
        "versatility": clamp_int(obj.get("versatility", 3), 0, 5, 3),
    }


def sanitize_behavior_model(obj: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "overheat_risk": clamp_int(obj.get("overheat_risk", 2), 0, 5, 2),
        "flavor_stability": clamp_int(obj.get("flavor_stability", 3), 0, 5, 3),
        "chemical_risk": clamp_int(obj.get("chemical_risk", 1), 0, 5, 1),
        "dryness": clamp_int(obj.get("dryness", 1), 0, 5, 1),
        "harshness": clamp_int(obj.get("harshness", 1), 0, 5, 1),
    }


def sanitize_mix_guidance(obj: Dict[str, Any]) -> Dict[str, Any]:
    best_usage = [u for u in ensure_list_strings(obj.get("best_usage", []), 3) if u in ALLOWED_USAGE]

    min_share = clamp_int(obj.get("recommended_share_min", 15), 0, 100, 15)
    max_share = clamp_int(obj.get("recommended_share_max", 50), 0, 100, 50)
    if min_share > max_share:
        min_share, max_share = max_share, min_share

    return {
        "recommended_share_min": min_share,
        "recommended_share_max": max_share,
        "best_usage": best_usage,
        "best_pairings": ensure_list_strings(obj.get("best_pairings", []), 3),
        "avoid_pairings": ensure_list_strings(obj.get("avoid_pairings", []), 3),
    }


def sanitize_editorial(obj: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "pros": ensure_list_strings(obj.get("pros", []), 3),
        "cons": ensure_list_strings(obj.get("cons", []), 3),
        "setup_notes": ensure_list_strings(obj.get("setup_notes", []), 3),
        "one_liner": norm_text(obj.get("one_liner", ""))[:140],
    }

# ------------------------------------------------------------
# ЧИСТКА ОТЗЫВОВ
# ------------------------------------------------------------

BAD_SUBSTRINGS = [
    "самые обсуждаемые вкусы недели",
    "подробнее о ",
    "рейтинг табаков",
    "рейтинг брендов",
    "рейтинг линеек",
    "новые отзывы",
    "контакты",
    "политика конфиденциальности",
    "условия использования",
    "h treviews",
    "htreviews",
    "copyright",
    "все права защищены",
    "ответить"
]


def is_bad_review_text(text: str) -> bool:
    t = norm_text(text).lower()
    if not t:
        return True
    if len(t) < 15:
        return True
    if sum(1 for x in BAD_SUBSTRINGS if x in t) >= 1:
        return True
    return False


def dedupe_reviews(reviews: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for r in reviews:
        text = norm_text(r.get("text", ""))
        text_key = re.sub(r"\s+", " ", text.lower())
        if text_key in seen:
            continue
        seen.add(text_key)
        out.append(r)
    return out


def clean_reviews(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    reviews = item.get("raw_reviews", []) or []
    cleaned = []

    for r in reviews:
        text = norm_text(r.get("text", ""))
        author = norm_text(r.get("author", ""))
        score = clamp_float(r.get("score", 0), 0, 5, 0)
        date = norm_text(r.get("date", ""))

        if is_bad_review_text(text):
            continue

        cleaned.append({
            "author": author,
            "score": score,
            "date": date,
            "text": text[:MAX_REVIEW_TEXT_LEN]
        })

    cleaned = dedupe_reviews(cleaned)
    return cleaned[:MAX_REVIEWS_PER_ITEM]

# ------------------------------------------------------------
# ЛОКАЛЬНЫЕ SIGNAL COUNTS
# ------------------------------------------------------------

SIGNAL_PATTERNS = {
    "solo_mentions": [
        r"\bсоло\b", r"в соло", r"solo"
    ],
    "mix_mentions": [
        r"\bмикс", r"в микс", r"для миксов", r"mix"
    ],
    "overheat_mentions": [
        r"перегрев", r"перегреть", r"не любит жар", r"сильн[а-я]+ жар", r"горит", r"вылетает"
    ],
    "chemical_mentions": [
        r"химоз", r"мылит", r"пластик", r"мыло", r"шампун"
    ],
    "longevity_mentions": [
        r"долго держ", r"держится", r"долго кур", r"быстро пропадает", r"быстро улетает", r"быстро теряется"
    ],
    "dominance_mentions": [
        r"перебива", r"забива[ею]т все", r"забивает", r"не теряется", r"доминирует"
    ],
}


def count_signals(reviews: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {k: 0 for k in SIGNAL_PATTERNS.keys()}
    for r in reviews:
        text = norm_text(r.get("text", "")).lower()
        for signal_name, patterns in SIGNAL_PATTERNS.items():
            if any(re.search(p, text, flags=re.IGNORECASE) for p in patterns):
                counts[signal_name] += 1
    return counts


def build_review_count_bucket(reviews_count: int) -> str:
    if reviews_count >= 100:
        return "very_high"
    if reviews_count >= 40:
        return "high"
    if reviews_count >= 15:
        return "medium"
    if reviews_count >= 5:
        return "low"
    return "very_low"


def compute_confidence(reviews_count: int, signals: Dict[str, int]) -> Dict[str, Any]:
    signal_sum = sum(signals.values())

    # signal_confidence 0..5
    signal_conf = min(5, int(round(signal_sum / 3))) if signal_sum > 0 else 1

    # summary_confidence — больше зависит от reviews_count, но учитывает сигналы
    if reviews_count >= 100:
        base = 5
    elif reviews_count >= 40:
        base = 4
    elif reviews_count >= 15:
        base = 3
    elif reviews_count >= 5:
        base = 2
    else:
        base = 1

    if signal_sum >= 10:
        base += 1

    return {
        "review_count_bucket": build_review_count_bucket(reviews_count),
        "signal_confidence": max(1, min(5, signal_conf)),
        "summary_confidence": max(1, min(5, base)),
    }

# ------------------------------------------------------------
# PAYLOAD
# ------------------------------------------------------------

def build_item_payload(item: Dict[str, Any]) -> Dict[str, Any]:
    cleaned_reviews = clean_reviews(item)
    signals = count_signals(cleaned_reviews)

    compact_reviews = []
    for r in cleaned_reviews:
        compact_reviews.append({
            "score": r.get("score", 0),
            "text": r.get("text", "")
        })

    return {
        "brand": norm_text(item.get("brand", "")),
        "flavor": norm_text(item.get("flavor", "")),
        "line": norm_text(item.get("line", "")),
        "url": norm_text(item.get("url", "")),
        "description": norm_text(item.get("description", "")),
        "categories": item.get("categories", []) or [],
        "rating": clamp_float(item.get("rating", 0), 0, 5, 0),
        "reviews_count": int(item.get("reviews_count", 0) or 0),
        "official_strength": norm_text(item.get("official_strength", "")),
        "perceived_strength": norm_text(item.get("perceived_strength", "")),
        "status": norm_text(item.get("status", "")),
        "review_signals_local": signals,
        "reviews_sample": compact_reviews
    }

# ------------------------------------------------------------
# OPENAI
# ------------------------------------------------------------

async def call_openai_json(session: aiohttp.ClientSession, batch_payload: List[Dict[str, Any]]) -> Dict[str, Any]:
    user_prompt = json.dumps({"items": batch_payload}, ensure_ascii=False)

    for attempt in range(5):
        try:
            async with session.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": MODEL,
                    "response_format": {"type": "json_object"},
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0.15,
                },
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            ) as resp:
                data = await resp.json()

                if resp.status == 429:
                    wait_time = 8 * (attempt + 1)
                    print(f"⏳ 429 rate limit, жду {wait_time}с...")
                    await asyncio.sleep(wait_time)
                    continue

                if resp.status >= 400:
                    print(f"⚠️ OpenAI error {resp.status}: {data}")
                    await asyncio.sleep(3 + attempt)
                    continue

                content = data["choices"][0]["message"]["content"]
                return json.loads(content)

        except Exception as e:
            print(f"⚠️ Ошибка запроса, попытка {attempt + 1}: {e}")
            await asyncio.sleep(2 ** attempt)

    return {"items": []}

# ------------------------------------------------------------
# CHECKPOINT
# ------------------------------------------------------------

def load_checkpoint() -> Tuple[List[Dict[str, Any]], set]:
    if not os.path.exists(OUTPUT_FILE):
        return [], set()

    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        existing = json.load(f)

    done = {
        norm_key(x.get("brand", ""), x.get("flavor", ""), x.get("url", ""))
        for x in existing
    }
    return existing, done


def save_checkpoint(data: List[Dict[str, Any]]) -> None:
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

async def main() -> None:
    if not os.path.exists(INPUT_FILE):
        raise RuntimeError(f"Не найден входной файл: {INPUT_FILE}")

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        raw_db = json.load(f)

    ready, done_keys = load_checkpoint()

    remaining = [
        item for item in raw_db
        if norm_key(item.get("brand", ""), item.get("flavor", ""), item.get("url", "")) not in done_keys
    ]

    print(f"📦 Всего записей в raw: {len(raw_db)}")
    print(f"✅ Уже готово по checkpoint: {len(ready)}")
    print(f"⏳ Осталось обработать: {len(remaining)}")

    if not remaining:
        print("Новых записей нет.")
        return

    batches = [remaining[i:i + BATCH_SIZE] for i in range(0, len(remaining), BATCH_SIZE)]

    async with aiohttp.ClientSession() as session:
        for i, batch in enumerate(batches, start=1):
            print(f"\n🔄 Батч {i}/{len(batches)} | элементов: {len(batch)}")

            payload = [build_item_payload(item) for item in batch]
            result = await call_openai_json(session, payload)

            # индекс результата модели
            result_items = result.get("items", []) or []
            llm_map: Dict[str, Dict[str, Any]] = {}

            for item in result_items:
                key = norm_key(item.get("brand", ""), item.get("flavor", ""), item.get("url", ""))
                llm_map[key] = item

            for raw_item in batch:
                key = norm_key(raw_item.get("brand", ""), raw_item.get("flavor", ""), raw_item.get("url", ""))

                cleaned_reviews = clean_reviews(raw_item)
                local_signals = count_signals(cleaned_reviews)
                confidence = compute_confidence(int(raw_item.get("reviews_count", 0) or 0), local_signals)

                llm_item = llm_map.get(key, {})

                summary = sanitize_summary(llm_item.get("summary", {}))
                usage_model = sanitize_usage_model(llm_item.get("usage_model", {}))
                behavior_model = sanitize_behavior_model(llm_item.get("behavior_model", {}))
                mix_guidance = sanitize_mix_guidance(llm_item.get("mix_guidance", {}))
                editorial = sanitize_editorial(llm_item.get("editorial", {}))

                record = {
                    "brand": norm_text(raw_item.get("brand", "")),
                    "flavor": norm_text(raw_item.get("flavor", "")),
                    "line": norm_text(raw_item.get("line", "")),
                    "url": norm_text(raw_item.get("url", "")),
                    "summary": summary,
                    "usage_model": usage_model,
                    "behavior_model": behavior_model,
                    "mix_guidance": mix_guidance,
                    "review_signals": local_signals,
                    "confidence": confidence,
                    "editorial": editorial,
                }

                ready.append(record)

            save_checkpoint(ready)
            print(f"💾 Сохранено: {len(ready)}")
            await asyncio.sleep(BATCH_PAUSE)

    print(f"\n🎉 Готово: {OUTPUT_FILE}")
    print(f"Итоговых записей: {len(ready)}")


if __name__ == "__main__":
    asyncio.run(main())
