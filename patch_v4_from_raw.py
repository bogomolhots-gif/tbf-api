import json
import os
from typing import Any, Dict, List


V4_FILE = "tobacco_intelligence_v4.json"
RAW_FILE = "raw_reviews_db.json"
OUTPUT_FILE = "tobacco_intelligence_v4.json"


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def norm_text(value: Any) -> str:
    text = clean_text(value).lower().replace("ё", "е")
    text = text.replace("–", "-").replace("—", "-")
    return text


def norm_brand(value: Any) -> str:
    text = norm_text(value)
    text = text.replace("-", " ")
    return " ".join(text.split())


def norm_flavor(value: Any) -> str:
    text = norm_text(value)
    text = text.replace("\ufeff", "")
    return " ".join(text.split())


def load_json(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        raise RuntimeError(f"Файл не найден: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise RuntimeError(f"Ожидался список объектов в {path}")
    return data


def apply_patch(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
    dst["rating"] = src.get("rating", dst.get("rating", 0))
    dst["reviews_count"] = src.get("reviews_count", dst.get("reviews_count", 0))
    dst["official_strength"] = src.get("official_strength", dst.get("official_strength", ""))
    dst["perceived_strength"] = src.get("perceived_strength", dst.get("perceived_strength", ""))
    dst["status"] = src.get("status", dst.get("status", ""))
    dst["categories"] = src.get("categories", dst.get("categories", []))
    dst["description"] = src.get("description", dst.get("description", ""))


def main() -> None:
    v4 = load_json(V4_FILE)
    raw = load_json(RAW_FILE)

    raw_by_url: Dict[str, Dict[str, Any]] = {}
    raw_by_name: Dict[str, Dict[str, Any]] = {}

    for item in raw:
        url = clean_text(item.get("url"))
        if url and url not in raw_by_url:
            raw_by_url[url] = item

        name_key = f"{norm_brand(item.get('brand'))}|{norm_flavor(item.get('flavor'))}"
        if name_key not in raw_by_name:
            raw_by_name[name_key] = item

    patched_by_url = 0
    patched_by_name = 0
    missing = 0
    missing_examples = []

    for item in v4:
        src = None

        url = clean_text(item.get("url"))
        if url and url in raw_by_url:
            src = raw_by_url[url]
            patched_by_url += 1
        else:
            name_key = f"{norm_brand(item.get('brand'))}|{norm_flavor(item.get('flavor'))}"
            if name_key in raw_by_name:
                src = raw_by_name[name_key]
                patched_by_name += 1

        if src:
            apply_patch(item, src)
        else:
            missing += 1
            if len(missing_examples) < 20:
                missing_examples.append({
                    "brand": clean_text(item.get("brand")),
                    "flavor": clean_text(item.get("flavor")),
                    "url": clean_text(item.get("url")),
                })

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(v4, f, ensure_ascii=False, indent=2)

    print("─" * 60)
    print(f"✅ patched by url: {patched_by_url}")
    print(f"✅ patched by brand+flavor: {patched_by_name}")
    print(f"⚠️ still missing: {missing}")
    print(f"💾 saved: {OUTPUT_FILE}")

    if missing_examples:
        print("\nПримеры несматченных:")
        for x in missing_examples:
            print(f"- {x['brand']} | {x['flavor']} | {x['url']}")


if __name__ == "__main__":
    main()
