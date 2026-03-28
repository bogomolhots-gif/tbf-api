import os
import json
import asyncio
import aiohttp
from typing import Any, Dict, List
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY не задан. Добавь его в .env")

INPUT_FILE = "raw_reviews_db.json"
OUTPUT_FILE = "review_enrichment_v1.json"
MODEL = "gpt-4o-mini"

BATCH_SIZE = 10
BATCH_PAUSE = 1.2
MAX_REVIEWS_PER_ITEM = 12


SYSTEM_PROMPT = """
Ты — эксперт по кальянным табакам.

Тебе дадут JSON-объекты по вкусам табака.
Для каждого вкуса нужно по отзывам и описанию вернуть краткую структурированную выжимку.

Верни JSON-объект формата:

{
  "items": [
    {
      "brand": "Black Burn",
      "flavor": "Peach Yogurt",
      "summary": {
        "profiles": ["fruit", "creamy", "dessert"],
        "sweetness": 4,
        "sourness": 1,
        "cooling": 0,
        "strength_feel": 2,
        "heat_resistance": 2,
        "mixability": 5,
        "longevity": 3,
        "complexity": 3,
        "best_usage": ["solo", "mix"],
        "pros": ["точное попадание во вкус", "сливочный профиль"],
        "cons": ["легко перегреть", "аромка быстро улетает"],
        "best_pairings": ["манго", "банан", "персик"],
        "setup_notes": ["не любит перегрев", "лучше без сильного колпака"],
        "one_liner": "Нежный сливочно-персиковый йогурт, сильный вкус, но чувствительный к жару."
      }
    }
  ]
}

ПРАВИЛА:
- Отвечай только валидным JSON.
- Все numeric fields строго 0..5.
- profiles — только из списка:
  ["berry", "citrus", "fruit", "tropical", "dessert", "creamy", "floral", "spicy", "beverage", "tobacco", "fresh", "herbal", "candy", "sour"]
- best_usage — только из:
  ["solo", "mix", "solo_and_mix", "tool"]
- pros/cons/best_pairings/setup_notes — максимум 3 коротких элемента в каждом массиве.
- one_liner — максимум 140 символов.
- Если данных мало, делай осторожный вывод.
"""


def clamp_0_5(value: Any, default: int = 0) -> int:
    try:
        v = int(round(float(value)))
    except Exception:
        v = default
    return max(0, min(5, v))


def ensure_list_strings(value: Any, limit: int = 3) -> List[str]:
    if not isinstance(value, list):
        return []
    out = []
    for x in value:
        s = str(x).strip()
        if s and s not in out:
            out.append(s)
    return out[:limit]


def sanitize_summary(obj: Dict[str, Any]) -> Dict[str, Any]:
    allowed_profiles = {
        "berry", "citrus", "fruit", "tropical", "dessert", "creamy",
        "floral", "spicy", "beverage", "tobacco", "fresh", "herbal",
        "candy", "sour"
    }
    allowed_usage = {"solo", "mix", "solo_and_mix", "tool"}

    profiles = [p for p in ensure_list_strings(obj.get("profiles", []), limit=5) if p in allowed_profiles]
    best_usage = [u for u in ensure_list_strings(obj.get("best_usage", []), limit=3) if u in allowed_usage]

    return {
        "profiles": profiles,
        "sweetness": clamp_0_5(obj.get("sweetness", 0)),
        "sourness": clamp_0_5(obj.get("sourness", 0)),
        "cooling": clamp_0_5(obj.get("cooling", 0)),
        "strength_feel": clamp_0_5(obj.get("strength_feel", 2), default=2),
        "heat_resistance": clamp_0_5(obj.get("heat_resistance", 2), default=2),
        "mixability": clamp_0_5(obj.get("mixability", 3), default=3),
        "longevity": clamp_0_5(obj.get("longevity", 3), default=3),
        "complexity": clamp_0_5(obj.get("complexity", 3), default=3),
        "best_usage": best_usage,
        "pros": ensure_list_strings(obj.get("pros", []), limit=3),
        "cons": ensure_list_strings(obj.get("cons", []), limit=3),
        "best_pairings": ensure_list_strings(obj.get("best_pairings", []), limit=3),
        "setup_notes": ensure_list_strings(obj.get("setup_notes", []), limit=3),
        "one_liner": str(obj.get("one_liner", "")).strip()[:140],
    }


def build_item_payload(item: Dict[str, Any]) -> Dict[str, Any]:
    reviews = item.get("raw_reviews", []) or []
    compact_reviews = []

    for r in reviews[:MAX_REVIEWS_PER_ITEM]:
        compact_reviews.append({
            "score": r.get("score", 0),
            "text": str(r.get("text", "")).strip()[:500]
        })

    return {
        "brand": item.get("brand", ""),
        "flavor": item.get("flavor", ""),
        "line": item.get("line", ""),
        "description": item.get("description", ""),
        "categories": item.get("categories", []),
        "rating": item.get("rating", 0),
        "reviews_count": item.get("reviews_count", 0),
        "official_strength": item.get("official_strength", ""),
        "perceived_strength": item.get("perceived_strength", ""),
        "reviews_sample": compact_reviews,
    }


async def call_openai_json(session: aiohttp.ClientSession, batch_payload: List[Dict[str, Any]]) -> Dict[str, Any]:
    user_prompt = json.dumps({"items": batch_payload}, ensure_ascii=False)

    for attempt in range(4):
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
                    "temperature": 0.2,
                },
                timeout=aiohttp.ClientTimeout(total=90)
            ) as resp:
                if resp.status == 429:
                    wait_time = 8 * (attempt + 1)
                    print(f"⏳ 429 rate limit, жду {wait_time}с...")
                    await asyncio.sleep(wait_time)
                    continue

                data = await resp.json()

                if resp.status >= 400:
                    print(f"⚠️ OpenAI error {resp.status}: {data}")
                    await asyncio.sleep(3 + attempt)
                    continue

                content = data["choices"][0]["message"]["content"]
                return json.loads(content)

        except Exception as e:
            print(f"⚠️ Ошибка запроса попытка {attempt + 1}: {e}")
            await asyncio.sleep(2 ** attempt)

    return {"items": []}


async def main():
    if not os.path.exists(INPUT_FILE):
        raise RuntimeError(f"Не найден входной файл: {INPUT_FILE}")

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        raw_db = json.load(f)

    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            ready = json.load(f)
        done_keys = {f"{x['brand']}|{x['flavor']}" for x in ready}
        print(f"✅ Найден checkpoint: {len(ready)} уже обработано")
    except FileNotFoundError:
        ready = []
        done_keys = set()

    remaining = [x for x in raw_db if f"{x.get('brand')}|{x.get('flavor')}" not in done_keys]
    print(f"📦 Всего записей: {len(raw_db)}")
    print(f"⏳ Осталось обработать: {len(remaining)}")

    if not remaining:
        print("Новых записей нет.")
        return

    batches = [remaining[i:i + BATCH_SIZE] for i in range(0, len(remaining), BATCH_SIZE)]

    async with aiohttp.ClientSession() as session:
        for i, batch in enumerate(batches, start=1):
            print(f"\n🔄 Батч {i}/{len(batches)} | элементов: {len(batch)}")

            payload = [build_item_payload(x) for x in batch]
            result = await call_openai_json(session, payload)

            result_items = result.get("items", [])
            result_map = {
                f"{x.get('brand', '')}|{x.get('flavor', '')}": x.get("summary", {})
                for x in result_items
            }

            for item in batch:
                key = f"{item.get('brand')}|{item.get('flavor')}"
                summary_raw = result_map.get(key, {})
                summary = sanitize_summary(summary_raw)

                ready.append({
                    "brand": item.get("brand", ""),
                    "flavor": item.get("flavor", ""),
                    "line": item.get("line", ""),
                    "url": item.get("url", ""),
                    "rating": item.get("rating", 0),
                    "reviews_count": item.get("reviews_count", 0),
                    "summary": summary,
                })

            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(ready, f, ensure_ascii=False, indent=2)

            print(f"💾 Сохранено: {len(ready)}")
            await asyncio.sleep(BATCH_PAUSE)

    print(f"\n🎉 Готово: {OUTPUT_FILE}")
    print(f"Итоговых записей: {len(ready)}")


if __name__ == "__main__":
    asyncio.run(main())