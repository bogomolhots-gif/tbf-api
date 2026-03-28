import json
import os
import random
import re
import time
from typing import Any, Dict, List, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

LINKS_FILE = "urls.txt"
OUTPUT_FILE = "raw_reviews_db.json"

HEADLESS = True
SLOW_MO = 0
PAGE_TIMEOUT_MS = 60000
WAIT_AFTER_LOAD_MS = 2500
SAVE_EVERY = 5


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def parse_float(text: str) -> float:
    text = str(text).replace(",", ".")
    m = re.search(r"\d+(?:\.\d+)?", text)
    return float(m.group(0)) if m else 0.0


def load_existing() -> Tuple[List[Dict[str, Any]], set]:
    if not os.path.exists(OUTPUT_FILE):
        return [], set()

    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    seen = {x.get("url", "") for x in data}
    return data, seen


def save_results(results: List[Dict[str, Any]]) -> None:
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)


def extract_title_parts(title: str) -> Tuple[str, str]:
    flavor = ""
    brand = ""
    m = re.search(r"Вкус\s+(.+?)\s+табака\s+(.+?)(?:\s+\||$)", title, re.IGNORECASE)
    if m:
        flavor = clean_text(m.group(1))
        brand = clean_text(m.group(2))
    return brand, flavor


def normalize_text_for_search(text: str) -> str:
    return clean_text(text).lower().replace("ё", "е")


def extract_info_value_from_text(body_text: str, label: str) -> str:
    """
    Более надежный способ, чем по .object_info_item:
    ищем строку метки и берем следующий осмысленный токен.
    """
    lines = [clean_text(x) for x in body_text.split("\n") if clean_text(x)]
    label_norm = normalize_text_for_search(label)

    for i, line in enumerate(lines):
        if normalize_text_for_search(line) == label_norm:
            # берем следующую непустую строку
            if i + 1 < len(lines):
                value = lines[i + 1]
                if value not in {"?", "—", "-"}:
                    return value

            # fallback: иногда значение через строку
            if i + 2 < len(lines):
                value = lines[i + 2]
                if value not in {"?", "—", "-"}:
                    return value

    return ""


def extract_reviews_count(full_text: str) -> int:
    m = re.search(r"Отзывы\s*\((\d+)\)", full_text, re.IGNORECASE)
    return int(m.group(1)) if m else 0


def extract_categories_from_text(body_text: str) -> List[str]:
    lines = [clean_text(x) for x in body_text.split("\n") if clean_text(x)]

    known = {
        "Фруктовый", "Ягодный", "Цитрусовый", "Десертный", "Сливочный",
        "Свежий", "Тропический", "Цветочный", "Пряный", "Напитки",
        "Кислый", "Сладкий", "Травяной", "Ореховый"
    }

    found = []
    for line in lines:
        if line in known and line not in found:
            found.append(line)
    return found


def is_bad_description_line(line: str, flavor: str, brand: str) -> bool:
    line_clean = clean_text(line)
    line_low = normalize_text_for_search(line_clean)

    if len(line_clean) < 40:
        return True

    bad_exact = {
        "бренд", "линейка", "страна", "крепость официальная", "крепость по оценкам",
        "статус", "отзывы", "дата", "рейтинг", "ответить", "самые обсуждаемые вкусы недели"
    }
    if line_low in bad_exact:
        return True

    if "отзывы, рейтинг вкусов, крепость" in line_low:
        return True

    if flavor and normalize_text_for_search(flavor) == line_low:
        return True

    if brand and normalize_text_for_search(brand) == line_low:
        return True

    if line_low.startswith("вкус ") and " табака " in line_low:
        return True

    if re.fullmatch(r"\d+(?:\.\d+)?", line_clean):
        return True

    return False


def extract_description_from_text(body_text: str, flavor: str, brand: str) -> str:
    """
    Ищем описание в верхней части страницы, до блока отзывов и инфоблока.
    """
    lines = [clean_text(x) for x in body_text.split("\n") if clean_text(x)]

    # ограничиваем поиск верхней частью страницы
    stop_markers = {
        "Бренд", "Линейка", "Страна", "Крепость официальная", "Крепость по оценкам",
        "Статус", "Отзывы"
    }

    head_lines = []
    for line in lines:
        if line in stop_markers:
            break
        head_lines.append(line)

    candidates = []
    for line in head_lines:
        if is_bad_description_line(line, flavor, brand):
            continue
        candidates.append(line)

    # предпочитаем самые "описательные" строки
    for line in candidates:
        low = normalize_text_for_search(line)
        if any(token in low for token in [
            "вкус", "аромат", "слад", "кисл", "слив", "персик", "йогур",
            "цедр", "ягод", "мякот", "сочн", "цветоч", "конфет", "мед",
            "ананас", "лимон", "апельсин", "холод", "свеж"
        ]):
            return line

    if candidates:
        return max(candidates, key=len)

    return ""


def extract_reviews_block_lines(body_text: str) -> List[str]:
    """
    Берем только блок отзывов:
    от 'Отзывы (N)' до 'Самые обсуждаемые вкусы недели' или футера.
    """
    lines = [clean_text(x) for x in body_text.split("\n") if clean_text(x)]

    start_idx = None
    for i, line in enumerate(lines):
        if re.search(r"Отзывы\s*\(\d+\)", line, re.IGNORECASE):
            start_idx = i
            break

    if start_idx is None:
        return []

    review_lines = lines[start_idx + 1:]

    stop_phrases = [
        "Самые обсуждаемые вкусы недели",
        "ПОДРОБНЕЕ О ",
        "Общественный рейтинг табаков для кальяна",
        "Рейтинг табаков",
        "Рейтинг брендов",
        "Рейтинг линеек",
        "Зал славы",
        "Новые отзывы",
        "Новости",
        "Контакты",
        "Маркетинговые материалы",
        "Условия использования",
        "Политика конфиденциальности",
        "HTReviews ©",
        "Copyright Hookah Tobacco Reviews",
    ]

    cleaned = []
    for line in review_lines:
        if any(stop in line for stop in stop_phrases):
            break
        cleaned.append(line)

    return cleaned


def is_bad_author(author: str) -> bool:
    author = clean_text(author)

    if not author:
        return True

    bad_exact = {
        "Ответить", "Новый участник", "Дата ↘", "Дата ↗", "Рейтинг ↘", "Рейтинг ↗"
    }
    if author in bad_exact:
        return True

    # мусор типа 3.2k / 14.1k / 25.9k
    if re.fullmatch(r"\d+(?:\.\d+)?k", author.lower()):
        return True

    # просто число 23 / 1 / 43 — чаще всего это счетчик, не имя
    if re.fullmatch(r"\d+", author):
        return True

    return False


def sanitize_review_text(text: str) -> str:
    text = clean_text(text)

    cut_markers = [
        "ПОДРОБНЕЕ О ",
        "Общественный рейтинг табаков для кальяна",
        "Рейтинг табаков",
        "Рейтинг брендов",
        "Рейтинг линеек",
        "Зал славы",
        "Новые отзывы",
        "Новости",
        "Контакты",
        "Маркетинговые материалы",
        "Условия использования",
        "Политика конфиденциальности",
        "HTReviews ©",
        "Copyright Hookah Tobacco Reviews",
        "Самые обсуждаемые вкусы недели",
    ]

    for marker in cut_markers:
        pos = text.find(marker)
        if pos != -1:
            text = text[:pos].strip()

    return clean_text(text)


def extract_reviews_from_text(body_text: str, reviews_count: int = 0) -> List[Dict[str, Any]]:
    lines = extract_reviews_block_lines(body_text)

    reviews = []
    i = 0

    while i < len(lines):
        if i + 3 >= len(lines):
            break

        username = lines[i]
        maybe_meta = lines[i + 1]
        maybe_rating = lines[i + 2]
        maybe_text = lines[i + 3]

        # пропускаем мусорные записи
        if is_bad_author(username):
            i += 1
            continue

        if username.startswith("@"):
            i += 1
            continue

        if not re.fullmatch(r"\d+(?:\.\d+)?", maybe_rating):
            i += 1
            continue

        score_value = parse_float(maybe_rating)
        if not (1.0 <= score_value <= 5.0):
            i += 1
            continue

        if len(maybe_text) < 20:
            i += 1
            continue

        review_text_parts = [maybe_text]
        j = i + 4

        while j < len(lines):
            cur = lines[j]

            if cur == "Ответить":
                break
            if cur.startswith("@"):
                break
            if cur == "Новый участник":
                break
            if re.fullmatch(r"\d+(?:\.\d+)?", cur):
                break
            if "Самые обсуждаемые вкусы недели" in cur:
                break
            if "ПОДРОБНЕЕ О " in cur:
                break
            if is_bad_author(cur):
                break

            # если похоже на начало следующего отзыва
            if j + 2 < len(lines):
                next_author = lines[j]
                next_rating = lines[j + 2]
                if (not is_bad_author(next_author)) and re.fullmatch(r"\d+(?:\.\d+)?", next_rating):
                    break

            review_text_parts.append(cur)
            j += 1

        review_text = sanitize_review_text(" ".join(review_text_parts))

        if len(review_text) >= 40:
            reviews.append({
                "author": username,
                "score": score_value,
                "date": "",
                "text": review_text,
            })

        i = j + 1

    # дедуп по автору+тексту
    deduped = []
    seen = set()
    for r in reviews:
        key = (r["author"].lower(), r["text"].lower())
        if key not in seen:
            deduped.append(r)
            seen.add(key)

    # не берем больше официального числа отзывов
    if reviews_count and len(deduped) > reviews_count:
        deduped = deduped[:reviews_count]

    return deduped[:50]


def extract_rating_from_reviews(reviews: List[Dict[str, Any]]) -> float:
    scores = []
    for r in reviews:
        score = r.get("score", 0)
        try:
            score = float(score)
        except Exception:
            continue

        if 1.0 <= score <= 5.0:
            scores.append(score)

    if not scores:
        return 0.0

    return round(sum(scores) / len(scores), 2)


def parse_page(page, url: str) -> Dict[str, Any]:
    page.goto(url, wait_until="networkidle", timeout=PAGE_TIMEOUT_MS)
    page.wait_for_timeout(WAIT_AFTER_LOAD_MS)

    title = clean_text(page.title())
    brand_from_title, flavor_from_title = extract_title_parts(title)

    body_text = page.locator("body").inner_text()

    brand = extract_info_value_from_text(body_text, "Бренд") or brand_from_title
    line = extract_info_value_from_text(body_text, "Линейка")
    official_strength = extract_info_value_from_text(body_text, "Крепость официальная")
    perceived_strength = extract_info_value_from_text(body_text, "Крепость по оценкам")
    status = extract_info_value_from_text(body_text, "Статус")

    reviews_count = extract_reviews_count(body_text)
    raw_reviews = extract_reviews_from_text(body_text, reviews_count=reviews_count)
    rating = extract_rating_from_reviews(raw_reviews)
    categories = extract_categories_from_text(body_text)
    description = extract_description_from_text(body_text, flavor_from_title, brand)

    return {
        "brand": brand or "Unknown",
        "flavor": flavor_from_title or "Unknown",
        "line": line,
        "url": url,
        "description": description,
        "categories": categories,
        "rating": rating,
        "reviews_count": reviews_count,
        "official_strength": official_strength,
        "perceived_strength": perceived_strength,
        "status": status,
        "raw_reviews": raw_reviews,
    }


def main() -> None:
    if not os.path.exists(LINKS_FILE):
        raise RuntimeError("Файл со ссылками не найден: {0}".format(LINKS_FILE))

    with open(LINKS_FILE, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    if not urls:
        raise RuntimeError("urls.txt пустой")

    results, seen = load_existing()
    urls = [u for u in urls if u not in seen]

    print("Всего ссылок к обработке: {0}".format(len(urls)))

    if not urls:
        print("Новых ссылок нет.")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO)
        context = browser.new_context(
            viewport={"width": 1440, "height": 2200},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="ru-RU",
        )

        page = context.new_page()

        for idx, url in enumerate(urls, start=1):
            try:
                data = parse_page(page, url)
                results.append(data)

                print(
                    "✅ {0}/{1} | {2} | {3} | rating={4} | reviews={5} | raw={6}".format(
                        idx,
                        len(urls),
                        data["brand"],
                        data["flavor"],
                        data["rating"],
                        data["reviews_count"],
                        len(data["raw_reviews"]),
                    )
                )

                if idx % SAVE_EVERY == 0:
                    save_results(results)
                    print("💾 Checkpoint: {0}".format(len(results)))

                time.sleep(random.uniform(1.0, 2.0))

            except PlaywrightTimeoutError:
                print("⏳ Timeout | {0}".format(url))
            except Exception as e:
                print("⚠️ Error | {0} | {1}".format(url, e))

        save_results(results)
        browser.close()

    print("\n🎉 Готово. Файл: {0}".format(OUTPUT_FILE))
    print("Итоговых записей: {0}".format(len(results)))


if __name__ == "__main__":
    main()