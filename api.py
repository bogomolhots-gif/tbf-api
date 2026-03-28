from typing import Any, Dict, List
import json
from pathlib import Path

from fastapi import Body, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "tobacco_intelligence_v4.json"

app = FastAPI(title="TBF Mixology API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def load_db() -> List[Dict[str, Any]]:
    if not DB_PATH.exists():
        return []
    with open(DB_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

DB = load_db()

def norm(value: Any) -> str:
    return str(value or "").strip().lower()

def item_key(item: Dict[str, Any]) -> str:
    return f"{norm(item.get('brand'))}::{norm(item.get('flavor'))}"

def get_summary(item: Dict[str, Any]) -> Dict[str, Any]:
    return item.get("review_intelligence", {}).get("summary", {}) or {}

def get_usage(item: Dict[str, Any]) -> Dict[str, Any]:
    return item.get("review_intelligence", {}).get("usage_model", {}) or {}

def get_guidance(item: Dict[str, Any]) -> Dict[str, Any]:
    return item.get("review_intelligence", {}).get("mix_guidance", {}) or {}

def get_profiles(item: Dict[str, Any]) -> List[str]:
    profiles = get_summary(item).get("profiles", []) or []
    return [norm(x) for x in profiles]

def num(value: Any, default: float = 0) -> float:
    try:
        return float(value)
    except Exception:
        return default

def strength_match(item: Dict[str, Any], strength: str) -> bool:
    feel = num(get_summary(item).get("strength_feel", 0))
    strength = norm(strength)
    if strength == "light":
        return feel <= 2
    if strength == "medium":
        return 2 <= feel <= 3.5
    if strength == "strong":
        return feel >= 3.5
    return True

@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "items": len(DB)}

@app.get("/search")
def search_flavors(
    q: str = Query(..., min_length=1),
    limit: int = Query(30, ge=1, le=100),
) -> Dict[str, Any]:
    query = norm(q)

    results = []
    for item in DB:
        brand = norm(item.get("brand"))
        flavor = norm(item.get("flavor"))
        description = norm(item.get("description"))
        profiles = " ".join(get_profiles(item))

        haystack = f"{brand} {flavor} {description} {profiles}"
        if query in haystack:
            results.append(item)

    return {"count": len(results[:limit]), "items": results[:limit]}

@app.get("/flavors/top/rating")
def top_rating(limit: int = Query(30, ge=1, le=100)) -> Dict[str, Any]:
    items = sorted(DB, key=lambda x: num(x.get("rating", 0)), reverse=True)
    return {"count": len(items[:limit]), "items": items[:limit]}

@app.get("/flavors/top/solo")
def top_solo(limit: int = Query(30, ge=1, le=100)) -> Dict[str, Any]:
    items = sorted(
        DB,
        key=lambda x: (
            num(get_usage(x).get("solo_score", 0)),
            num(x.get("rating", 0)),
        ),
        reverse=True,
    )
    return {"count": len(items[:limit]), "items": items[:limit]}

@app.get("/flavors/top/tool")
def top_tool(limit: int = Query(30, ge=1, le=100)) -> Dict[str, Any]:
    items = sorted(
        DB,
        key=lambda x: (
            num(get_usage(x).get("tool_score", 0)),
            num(get_summary(x).get("mixability", 0)),
            num(x.get("rating", 0)),
        ),
        reverse=True,
    )
    return {"count": len(items[:limit]), "items": items[:limit]}

@app.get("/mix/recommend")
def mix_recommend(
    profile: str = Query("fruit"),
    strength: str = Query("medium"),
    limit: int = Query(6, ge=1, le=20),
) -> Dict[str, Any]:
    profile = norm(profile)
    strength = norm(strength)

    candidates = [
        x for x in DB
        if profile in get_profiles(x) and strength_match(x, strength)
    ]

    if len(candidates) < 3:
        candidates = [x for x in DB if profile in get_profiles(x)]

    if len(candidates) < 3:
        candidates = DB[:]

    candidates = sorted(
        candidates,
        key=lambda x: (
            num(get_summary(x).get("mixability", 0)),
            num(get_usage(x).get("mix_score", 0)),
            num(x.get("rating", 0)),
        ),
        reverse=True,
    )

    bases = sorted(
        candidates,
        key=lambda x: (
            num(get_usage(x).get("solo_score", 0)),
            num(get_summary(x).get("longevity", 0)),
            num(x.get("rating", 0)),
        ),
        reverse=True,
    )

    accents = sorted(
        candidates,
        key=lambda x: (
            num(get_usage(x).get("mix_score", 0)),
            num(get_summary(x).get("complexity", 0)),
            num(x.get("rating", 0)),
        ),
        reverse=True,
    )

    tools = sorted(
        DB,
        key=lambda x: (
            num(get_usage(x).get("tool_score", 0)),
            num(get_summary(x).get("mixability", 0)),
            num(x.get("rating", 0)),
        ),
        reverse=True,
    )

    mixes = []
    used = set()

    for base in bases[:10]:
        for accent in accents[:12]:
            if item_key(base) == item_key(accent):
                continue

            tool = next(
                (t for t in tools if item_key(t) not in {item_key(base), item_key(accent)}),
                None,
            )

            recipe = [
                {"brand": base.get("brand", ""), "flavor": base.get("flavor", ""), "percent": 60},
                {"brand": accent.get("brand", ""), "flavor": accent.get("flavor", ""), "percent": 30},
            ]

            if tool:
                recipe.append(
                    {"brand": tool.get("brand", ""), "flavor": tool.get("flavor", ""), "percent": 10}
                )

            signature = tuple(sorted(f"{r['brand']}::{r['flavor']}" for r in recipe))
            if signature in used:
                continue

            used.add(signature)
            mixes.append({
                "name": " / ".join(r["flavor"] for r in recipe),
                "note": "Микс без привязки к бару",
                "recipe": recipe,
            })

            if len(mixes) >= limit:
                return {"count": len(mixes), "items": mixes}

    return {"count": len(mixes), "items": mixes}

@app.post("/mix/from-bar")
def mix_from_bar(payload: dict = Body(...)) -> Dict[str, Any]:
    items = payload.get("items", []) or []
    profile = norm(payload.get("profile", "fruit"))
    strength = norm(payload.get("strength", "medium"))
    limit = int(payload.get("limit", 6) or 6)

    if not items:
        return {"count": 0, "items": [], "message": "Бар пуст"}

    candidates = [
        x for x in items
        if profile in get_profiles(x) and strength_match(x, strength)
    ]

    if len(candidates) < 2:
        candidates = [x for x in items if profile in get_profiles(x)]

    if len(candidates) < 2:
        candidates = items[:]

    candidates = sorted(
        candidates,
        key=lambda x: (
            num(get_summary(x).get("mixability", 0)),
            num(get_usage(x).get("mix_score", 0)),
            num(x.get("rating", 0)),
        ),
        reverse=True,
    )

    bases = sorted(
        candidates,
        key=lambda x: (
            num(get_usage(x).get("solo_score", 0)),
            num(get_summary(x).get("longevity", 0)),
            num(x.get("rating", 0)),
        ),
        reverse=True,
    )

    accents = sorted(
        candidates,
        key=lambda x: (
            num(get_usage(x).get("mix_score", 0)),
            num(get_summary(x).get("complexity", 0)),
            num(x.get("rating", 0)),
        ),
        reverse=True,
    )

    tools = sorted(
        items,
        key=lambda x: (
            num(get_usage(x).get("tool_score", 0)),
            num(get_summary(x).get("mixability", 0)),
            num(x.get("rating", 0)),
        ),
        reverse=True,
    )

    mixes = []
    used = set()

    for base in bases[:10]:
        for accent in accents[:12]:
            if item_key(base) == item_key(accent):
                continue

            tool = next(
                (t for t in tools if item_key(t) not in {item_key(base), item_key(accent)}),
                None,
            )

            recipe = [
                {"brand": base.get("brand", ""), "flavor": base.get("flavor", ""), "percent": 60},
                {"brand": accent.get("brand", ""), "flavor": accent.get("flavor", ""), "percent": 30},
            ]

            if tool:
                recipe.append(
                    {"brand": tool.get("brand", ""), "flavor": tool.get("flavor", ""), "percent": 10}
                )

            signature = tuple(sorted(f"{r['brand']}::{r['flavor']}" for r in recipe))
            if signature in used:
                continue

            used.add(signature)
            mixes.append({
                "name": " / ".join(r["flavor"] for r in recipe),
                "note": "Микс из твоего бара",
                "recipe": recipe,
            })

            if len(mixes) >= limit:
                return {"count": len(mixes), "items": mixes}

    return {"count": len(mixes), "items": mixes}

@app.post("/mix/from-anchor")
def mix_from_anchor(payload: dict = Body(...)) -> Dict[str, Any]:
    anchor = payload.get("anchor") or {}
    items = payload.get("items", []) or []
    profile = norm(payload.get("profile", "fruit"))
    strength = norm(payload.get("strength", "medium"))
    limit = int(payload.get("limit", 6) or 6)

    if not anchor or not items:
        return {"count": 0, "items": [], "message": "Недостаточно данных"}

    anchor_key = item_key(anchor)
    anchor_profiles = set(get_profiles(anchor))
    anchor_guidance = get_guidance(anchor)
    anchor_pairings = [norm(x) for x in anchor_guidance.get("best_pairings", []) or []]

    others = [x for x in items if item_key(x) != anchor_key]

    if not others:
        return {"count": 0, "items": [], "message": "В баре только один вкус"}

    def candidate_score(item: Dict[str, Any]) -> tuple:
        profiles = set(get_profiles(item))
        shared_profile_bonus = 1 if profile in profiles else 0
        overlap_bonus = len(anchor_profiles.intersection(profiles))
        pairing_bonus = 0
        joined = f"{norm(item.get('brand'))} {norm(item.get('flavor'))} {' '.join(profiles)}"
        if any(p and p in joined for p in anchor_pairings):
            pairing_bonus = 2

        return (
            pairing_bonus,
            shared_profile_bonus,
            overlap_bonus,
            num(get_summary(item).get("mixability", 0)),
            num(get_usage(item).get("mix_score", 0)),
            num(item.get("rating", 0)),
        )

    filtered = [x for x in others if strength_match(x, strength)]

    if len(filtered) < 2:
        filtered = others[:]

    ranked = sorted(filtered, key=candidate_score, reverse=True)

    mixes = []
    used = set()

    for accent in ranked[:15]:
        if item_key(accent) == anchor_key:
            continue

        possible_tools = [
            x for x in ranked
            if item_key(x) not in {anchor_key, item_key(accent)}
        ]

        tool = possible_tools[0] if possible_tools else None

        recipe = [
            {"brand": anchor.get("brand", ""), "flavor": anchor.get("flavor", ""), "percent": 60},
            {"brand": accent.get("brand", ""), "flavor": accent.get("flavor", ""), "percent": 30},
        ]

        if tool:
            recipe.append(
                {"brand": tool.get("brand", ""), "flavor": tool.get("flavor", ""), "percent": 10}
            )

        signature = tuple(sorted(f"{r['brand']}::{r['flavor']}" for r in recipe))
        if signature in used:
            continue

        used.add(signature)
        mixes.append({
            "name": f"{anchor.get('flavor', '')} + {accent.get('flavor', '')}",
            "note": f"Микс вокруг вкуса {anchor.get('flavor', '')}",
            "recipe": recipe,
        })

        if len(mixes) >= limit:
            break

    return {"count": len(mixes), "items": mixes}
