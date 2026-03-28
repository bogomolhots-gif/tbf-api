import json
import os
from typing import Any, Dict, List, Tuple


BASE_FILE = "tobacco_intelligence_v3.json"
ENRICH_FILE = "review_enrichment_v2.json"
OUTPUT_FILE = "tobacco_intelligence_v4.json"


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def norm_text(value: Any) -> str:
    return clean_text(value).lower().replace("ё", "е")


def make_url_key(url: Any) -> str:
    return clean_text(url)


def make_name_key(brand: Any, flavor: Any) -> str:
    return f"{norm_text(brand)}|{norm_text(flavor)}"


def load_json(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        raise RuntimeError(f"Файл не найден: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise RuntimeError(f"Ожидался список объектов в {path}")

    return data


def build_indexes(enrich_data: List[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    by_url: Dict[str, Dict[str, Any]] = {}
    by_name: Dict[str, Dict[str, Any]] = {}

    for item in enrich_data:
        url_key = make_url_key(item.get("url"))
        name_key = make_name_key(item.get("brand"), item.get("flavor"))

        if url_key and url_key not in by_url:
            by_url[url_key] = item

        if name_key and name_key not in by_name:
            by_name[name_key] = item

    return by_url, by_name


def merge_record(base_item: Dict[str, Any], enrich_item: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base_item)

    merged["review_intelligence"] = {
        "summary": enrich_item.get("summary", {}),
        "usage_model": enrich_item.get("usage_model", {}),
        "behavior_model": enrich_item.get("behavior_model", {}),
        "mix_guidance": enrich_item.get("mix_guidance", {}),
        "review_signals": enrich_item.get("review_signals", {}),
        "confidence": enrich_item.get("confidence", {}),
        "editorial": enrich_item.get("editorial", {}),
    }

    merged["review_enrichment_version"] = "v2"
    merged["review_enrichment_merged"] = True

    # Если в базе нет line, а в enrichment есть — дотягиваем
    if not clean_text(merged.get("line")) and clean_text(enrich_item.get("line")):
        merged["line"] = clean_text(enrich_item.get("line"))

    return merged


def main() -> None:
    base_data = load_json(BASE_FILE)
    enrich_data = load_json(ENRICH_FILE)

    enrich_by_url, enrich_by_name = build_indexes(enrich_data)

    merged_data: List[Dict[str, Any]] = []

    matched_by_url = 0
    matched_by_name = 0
    unmatched = 0

    unmatched_examples = []

    for base_item in base_data:
        url_key = make_url_key(base_item.get("url"))
        name_key = make_name_key(base_item.get("brand"), base_item.get("flavor"))

        enrich_item = None

        if url_key and url_key in enrich_by_url:
            enrich_item = enrich_by_url[url_key]
            matched_by_url += 1
        elif name_key in enrich_by_name:
            enrich_item = enrich_by_name[name_key]
            matched_by_name += 1

        if enrich_item:
            merged_data.append(merge_record(base_item, enrich_item))
        else:
            item_copy = dict(base_item)
            item_copy["review_enrichment_merged"] = False
            merged_data.append(item_copy)

            unmatched += 1
            if len(unmatched_examples) < 20:
                unmatched_examples.append({
                    "brand": clean_text(base_item.get("brand")),
                    "flavor": clean_text(base_item.get("flavor")),
                    "url": clean_text(base_item.get("url")),
                })

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, ensure_ascii=False, indent=2)

    print("\n" + "─" * 60)
    print(f"✅ Готово: {OUTPUT_FILE}")
    print(f"📦 Базовых записей: {len(base_data)}")
    print(f"🧠 Enrichment записей: {len(enrich_data)}")
    print(f"🔗 Сматчено по URL: {matched_by_url}")
    print(f"🔗 Сматчено по brand+flavor: {matched_by_name}")
    print(f"⚠️ Не сматчено: {unmatched}")

    if unmatched_examples:
        print("\nПримеры несматченных записей:")
        for x in unmatched_examples:
            print(f"- {x['brand']} | {x['flavor']} | {x['url']}")


if __name__ == "__main__":
    main()
