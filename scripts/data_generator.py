from __future__ import annotations

import argparse
import json
import os
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Iterator, List, Optional


ACTIONS: List[str] = ["view", "add_to_cart", "purchase"]
DEVICE_TYPES: List[str] = ["mobile", "desktop", "tablet"]


@dataclass(frozen=True)
class Product:
    product_id: str
    category: str
    price: float


def _utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc)


def generate_products(seed: int, categories: List[str], products_per_category: int) -> List[Product]:
    rng = random.Random(seed)
    products: List[Product] = []
    for cat in categories:
        for i in range(products_per_category):
            product_id = f"{cat.replace(' ', '_').lower()}_{i:04d}"
            # Heavier-tailed price distribution.
            base = rng.lognormvariate(mu=3.0, sigma=0.35)
            price = round(max(5.0, min(9999.0, base)), 2)
            products.append(Product(product_id=product_id, category=cat, price=price))
    return products


def weighted_choice(rng: random.Random, choices: List[str], weights: List[float]) -> str:
    return rng.choices(choices, weights=weights, k=1)[0]


def generate_session_events(
    rng: random.Random,
    user_id: str,
    session_id: str,
    start_ts: datetime,
    session_length_minutes: int,
    products: List[Product],
    categories: List[str],
) -> Iterator[Dict[str, object]]:
    """Generate realistic event sequences for a single session.

    Rules (roughly):
    - Most sessions start with one or more views.
    - Probability of add_to_cart increases with view count.
    - Probability of purchase increases with add_to_cart presence.
    """

    # Pick a category "intent" for the session.
    intent_category = weighted_choice(rng, categories, [0.25, 0.2, 0.2, 0.15, 0.2])
    possible_products = [p for p in products if p.category == intent_category]
    chosen_product = rng.choice(possible_products)

    num_events = rng.randint(3, 30)

    # Create event timestamps across the session window.
    end_ts = start_ts + timedelta(minutes=session_length_minutes)
    window_seconds = max(1, int((end_ts - start_ts).total_seconds()))

    view_count_target = rng.randint(1, 18)

    has_added = False
    has_purchased = False

    for idx in range(num_events):
        # Ensure increasing intent as the session progresses.
        progress = idx / max(1, (num_events - 1))
        view_bias = (1.0 - progress) * 0.9 + progress * 0.2

        # Determine action.
        # Add-to-cart becomes more likely after enough views.
        # Purchase becomes likely only if add_to_cart happened.
        if not has_purchased:
            # approximate view count using idx and bias
            if idx < view_count_target:
                base_action = weighted_choice(rng, ["view", "add_to_cart"], [0.88 * view_bias, 0.12])
            else:
                base_action = weighted_choice(rng, ["view", "add_to_cart"], [0.45, 0.55 * (0.3 + progress)])

            if base_action == "add_to_cart" and not has_added:
                has_added = rng.random() < (0.35 + 0.45 * progress)
                action = "add_to_cart" if has_added else "view"
            else:
                action = base_action

            # Purchase transition.
            if action == "view" and has_added:
                # occasional post-add views before purchase.
                action = weighted_choice(rng, ["view", "purchase"], [0.92, 0.08 + 0.25 * progress])

            if action == "add_to_cart" and not has_added:
                has_added = True

            if action == "purchase":
                has_purchased = True
        else:
            # After purchase, keep only views (or terminate early).
            action = weighted_choice(rng, ["view"], [1.0])

        event_ts = start_ts + timedelta(seconds=rng.randint(0, window_seconds))
        event_ts = min(event_ts, end_ts)

        device_type = weighted_choice(rng, DEVICE_TYPES, [0.55, 0.4, 0.05])

        product_id = chosen_product.product_id if rng.random() > 0.12 else rng.choice(possible_products).product_id
        product_meta = next(p for p in possible_products if p.product_id == product_id)

        price = product_meta.price if rng.random() > 0.02 else round(product_meta.price * rng.uniform(0.6, 1.4), 2)

        yield {
            "user_id": user_id,
            "session_id": session_id,
            "timestamp": _utc(event_ts).isoformat(),
            "action": action,
            "product_id": product_id,
            "category": product_meta.category,
            "price": price,
            "device_type": device_type,
        }

        # Small chance to end session early after purchase.
        if has_purchased and rng.random() < 0.25:
            break


def iter_records(
    rng: random.Random,
    num_users: int,
    sessions_per_user: int,
    start_datetime: datetime,
    days: int,
    products: List[Product],
    categories: List[str],
) -> Iterator[Dict[str, object]]:
    """Generate clickstream records."""

    for user_idx in range(num_users):
        user_id = f"user_{user_idx:06d}"
        for _ in range(sessions_per_user):
            session_id = str(uuid.uuid4())

            # Spread sessions over the date range.
            day_offset = rng.randint(0, days - 1)
            minute_offset = rng.randint(0, 24 * 60 - 1)
            start_ts = start_datetime + timedelta(days=day_offset, minutes=minute_offset)

            session_length_minutes = rng.randint(1, 180)

            yield from generate_session_events(
                rng=rng,
                user_id=user_id,
                session_id=session_id,
                start_ts=start_ts,
                session_length_minutes=session_length_minutes,
                products=products,
                categories=categories,
            )


def write_jsonl_files(
    output_dir: str,
    records_iter: Iterable[Dict[str, object]],
    records_per_file: int,
    max_records: Optional[int],
) -> List[str]:
    os.makedirs(output_dir, exist_ok=True)
    file_paths: List[str] = []

    part_idx = 0
    current_count = 0
    current_path = os.path.join(output_dir, f"part-{part_idx:05d}.json")
    f = open(current_path, "w", encoding="utf-8")
    file_paths.append(current_path)

    written = 0
    try:
        for rec in records_iter:
            if max_records is not None and written >= max_records:
                break
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            current_count += 1
            written += 1
            if current_count >= records_per_file:
                f.close()
                part_idx += 1
                current_count = 0
                current_path = os.path.join(output_dir, f"part-{part_idx:05d}.json")
                f = open(current_path, "w", encoding="utf-8")
                file_paths.append(current_path)
    finally:
        f.close()

    return file_paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce clickstream JSON.")
    parser.add_argument("--output-dir", type=str, default="data/raw/clickstream")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--num-users", type=int, default=2000)
    parser.add_argument("--sessions-per-user", type=int, default=10)
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--start-date", type=str, default="2024-01-01")
    parser.add_argument("--products-per-category", type=int, default=120)
    parser.add_argument("--records-per-file", type=int, default=25000)
    parser.add_argument("--max-records", type=int, default=120000)

    args = parser.parse_args()

    categories = ["electronics", "home_kitchen", "fashion", "sports_outdoors", "books"]
    products = generate_products(seed=args.seed, categories=categories, products_per_category=args.products_per_category)

    rng = random.Random(args.seed)
    start_datetime = _utc(datetime.strptime(args.start_date, "%Y-%m-%d"))

    records_iter = iter_records(
        rng=rng,
        num_users=args.num_users,
        sessions_per_user=args.sessions_per_user,
        start_datetime=start_datetime,
        days=args.days,
        products=products,
        categories=categories,
    )

    out_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), args.output_dir)
    paths = write_jsonl_files(
        output_dir=out_dir,
        records_iter=records_iter,
        records_per_file=args.records_per_file,
        max_records=args.max_records,
    )

    print(f"Generated {len(paths)} file(s) under: {out_dir}")


if __name__ == "__main__":
    main()

