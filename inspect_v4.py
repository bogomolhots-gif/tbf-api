import json
import os
import sys
from typing import Any, Dict, List, Optional


DB_FILE = "tobacco_intelligence_v4.json"


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def norm_text(value: Any) -> str:
    return clean_text(value).lower().replace("ё", "е")


def load_db(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        raise RuntimeError(f"Файл не найден: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise RuntimeError("Ожидался список объектов")

    return data


def get_review_intel(item: Dict[str, Any]) -> Dict[str, Any]:
    return item.get("review_intelligence", {}) or {}


def get_summary(item: Dict[str, Any]) -> Dict[str, Any]:
    return get_review_intel(item).get("summary", {}) or {}


def get_usage_model(item: Dict[str, Any]) -> Dict[str, Any]:
    return get_review_intel(item).get("usage_model", {}) or {}


def get_behavior_model(item: Dict[str, Any]) -> Dict[str, Any]:
    return get_review_intel(item).get("behavior_model", {}) or {}


def get_mix_guidance(item: Dict[str, Any]) -> Dict[str, Any]:
    return get_review_intel(item).get("mix_guidance", {}) or {}


def get_editorial(item: Dict[str, Any]) -> Dict[str, Any]:
    return get_review_intel(item).get("editorial", {}) or {}


def get_confidence(item: Dict[str, Any]) -> Dict[str, Any]:
    return get_review_intel(item).get("confidence", {}) or {}


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def short_card(item: Dict[str, Any], idx: Optional[int] = None) -> str:
    summary = get_summary(item)
    usage = get_usage_model(item)

    brand = clean_text(item.get("brand"))
    flavor = clean_text(item.get("flavor"))
    rating = safe_float(item.get("rating", 0))
    profiles = ", ".join(summary.get("profiles", [])[:3]) or "-"
    mixability = safe_int(summary.get("mixability", 0))
    solo_score = safe_int(usage.get("solo_score", 0))
    mix_score = safe_int(usage.get("mix_score", 0))

    prefix = f"[{idx}] " if idx is not None else ""
    return (
        f"{prefix}{brand} | {flavor} | "
        f"rating={rating:.2f} | profiles={profiles} | "
        f"mixability={mixability} | solo={solo_score} | mix={mix_score}"
    )


def print_item_full(item: Dict[str, Any]) -> None:
    summary = get_summary(item)
    usage = get_usage_model(item)
    behavior = get_behavior_model(item)
    guidance = get_mix_guidance(item)
    editorial = get_editorial(item)
    confidence = get_confidence(item)

    print("\n" + "─" * 80)
    print(f"Бренд: {clean_text(item.get('brand'))}")
    print(f"Вкус: {clean_text(item.get('flavor'))}")
    print(f"Линейка: {clean_text(item.get('line'))}")
    print(f"URL: {clean_text(item.get('url'))}")
    print(f"Rating: {safe_float(item.get('rating', 0)):.2f}")
    print(f"Reviews: {safe_int(item.get('reviews_count', 0))}")
    print(f"Merge: {item.get('review_enrichment_merged', False)}")

    print("\n[SUMMARY]")
    print(f"profiles: {summary.get('profiles', [])}")
    print(f"sweetness: {summary.get('sweetness', 0)}")
    print(f"sourness: {summary.get('sourness', 0)}")
    print(f"cooling: {summary.get('cooling', 0)}")
    print(f"strength_feel: {summary.get('strength_feel', 0)}")
    print(f"heat_resistance: {summary.get('heat_resistance', 0)}")
    print(f"mixability: {summary.get('mixability', 0)}")
    print(f"longevity: {summary.get('longevity', 0)}")
    print(f"complexity: {summary.get('complexity', 0)}")

    print("\n[USAGE MODEL]")
    print(f"solo_score: {usage.get('solo_score', 0)}")
    print(f"mix_score: {usage.get('mix_score', 0)}")
    print(f"tool_score: {usage.get('tool_score', 0)}")
    print(f"dominance: {usage.get('dominance', 0)}")
    print(f"versatility: {usage.get('versatility', 0)}")

    print("\n[BEHAVIOR MODEL]")
    print(f"overheat_risk: {behavior.get('overheat_risk', 0)}")
    print(f"flavor_stability: {behavior.get('flavor_stability', 0)}")
    print(f"chemical_risk: {behavior.get('chemical_risk', 0)}")
    print(f"dryness: {behavior.get('dryness', 0)}")
    print(f"harshness: {behavior.get('harshness', 0)}")

    print("\n[MIX GUIDANCE]")
    print(f"recommended_share_min: {guidance.get('recommended_share_min', 0)}")
    print(f"recommended_share_max: {guidance.get('recommended_share_max', 0)}")
    print(f"best_usage: {guidance.get('best_usage', [])}")
    print(f"best_pairings: {guidance.get('best_pairings', [])}")
    print(f"avoid_pairings: {guidance.get('avoid_pairings', [])}")

    print("\n[EDITORIAL]")
    print(f"pros: {editorial.get('pros', [])}")
    print(f"cons: {editorial.get('cons', [])}")
    print(f"setup_notes: {editorial.get('setup_notes', [])}")
    print(f"one_liner: {editorial.get('one_liner', '')}")

    print("\n[CONFIDENCE]")
    print(f"review_count_bucket: {confidence.get('review_count_bucket', '')}")
    print(f"signal_confidence: {confidence.get('signal_confidence', 0)}")
    print(f"summary_confidence: {confidence.get('summary_confidence', 0)}")
    print("─" * 80 + "\n")


def search_by_text(data: List[Dict[str, Any]], query: str) -> List[Dict[str, Any]]:
    q = norm_text(query)
    results = []

    for item in data:
        hay = " | ".join([
            clean_text(item.get("brand")),
            clean_text(item.get("flavor")),
            clean_text(item.get("line")),
            clean_text(item.get("url")),
            " ".join(get_summary(item).get("profiles", [])),
            " ".join(get_mix_guidance(item).get("best_pairings", [])),
            clean_text(get_editorial(item).get("one_liner", "")),
        ])
        if q in norm_text(hay):
            results.append(item)

    return results


def filter_by_brand(data: List[Dict[str, Any]], brand: str) -> List[Dict[str, Any]]:
    b = norm_text(brand)
    return [x for x in data if norm_text(x.get("brand")) == b]


def sort_top_rating(data: List[Dict[str, Any]], limit: int = 20) -> List[Dict[str, Any]]:
    return sorted(
        data,
        key=lambda x: (
            safe_float(x.get("rating", 0)),
            safe_int(x.get("reviews_count", 0))
        ),
        reverse=True
    )[:limit]


def sort_top_mixability(data: List[Dict[str, Any]], limit: int = 20) -> List[Dict[str, Any]]:
    return sorted(
        data,
        key=lambda x: (
            safe_int(get_summary(x).get("mixability", 0)),
            safe_float(x.get("rating", 0)),
            safe_int(x.get("reviews_count", 0))
        ),
        reverse=True
    )[:limit]


def sort_top_solo(data: List[Dict[str, Any]], limit: int = 20) -> List[Dict[str, Any]]:
    return sorted(
        data,
        key=lambda x: (
            safe_int(get_usage_model(x).get("solo_score", 0)),
            safe_float(x.get("rating", 0)),
            safe_int(x.get("reviews_count", 0))
        ),
        reverse=True
    )[:limit]


def sort_top_tool(data: List[Dict[str, Any]], limit: int = 20) -> List[Dict[str, Any]]:
    return sorted(
        data,
        key=lambda x: (
            safe_int(get_usage_model(x).get("tool_score", 0)),
            safe_int(get_summary(x).get("mixability", 0)),
            safe_float(x.get("rating", 0))
        ),
        reverse=True
    )[:limit]


def stats(data: List[Dict[str, Any]]) -> None:
    merged_count = sum(1 for x in data if x.get("review_enrichment_merged"))
    brands = sorted({clean_text(x.get("brand")) for x in data if clean_text(x.get("brand"))})
    print("\n" + "─" * 80)
    print(f"Всего записей: {len(data)}")
    print(f"С enrichment: {merged_count}")
    print(f"Брендов: {len(brands)}")
    print(f"Первые 20 брендов: {brands[:20]}")
    print("─" * 80 + "\n")


def choose_from_results(results: List[Dict[str, Any]]) -> None:
    if not results:
        print("Ничего не найдено.")
        return

    print("")
    for i, item in enumerate(results[:30], start=1):
        print(short_card(item, i))

    while True:
        raw = input("\nВведи номер карточки для полного просмотра (Enter = назад): ").strip()
        if not raw:
            return
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= min(len(results), 30):
                print_item_full(results[idx - 1])
                return
        print("Некорректный ввод.")


def menu() -> None:
    data = load_db(DB_FILE)

    while True:
        print("\n=== INSPECT V4 ===")
        print("1. Статистика")
        print("2. Поиск по тексту")
        print("3. Фильтр по бренду")
        print("4. Топ по rating")
        print("5. Топ по mixability")
        print("6. Топ для solo")
        print("7. Топ tool / mix вкусов")
        print("8. Открыть вкус по точному названию")
        print("0. Выход")

        choice = input("Выбери пункт: ").strip()

        if choice == "1":
            stats(data)

        elif choice == "2":
            query = input("Поиск: ").strip()
            results = search_by_text(data, query)
            choose_from_results(results)

        elif choice == "3":
            brand = input("Бренд: ").strip()
            results = filter_by_brand(data, brand)
            choose_from_results(results)

        elif choice == "4":
            results = sort_top_rating(data, limit=30)
            choose_from_results(results)

        elif choice == "5":
            results = sort_top_mixability(data, limit=30)
            choose_from_results(results)

        elif choice == "6":
            results = sort_top_solo(data, limit=30)
            choose_from_results(results)

        elif choice == "7":
            results = sort_top_tool(data, limit=30)
            choose_from_results(results)

        elif choice == "8":
            exact = input("Точное название вкуса: ").strip()
            found = [x for x in data if norm_text(x.get("flavor")) == norm_text(exact)]
            choose_from_results(found)

        elif choice == "0":
            print("Выход.")
            return

        else:
            print("Не понял команду.")


if __name__ == "__main__":
    try:
        menu()
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
        sys.exit(0)
    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)
