#!/usr/bin/env python3
"""
Scrape WTA player profiles from a CSV list of player names.

This script uses the core scraper in `scrape_player_profiles.py`, but provides
WTA-focused defaults and name-to-slug resolution from the Tennis Abstract
player list.

Usage examples:
  python scripts/scrape_wta_profiles.py
  python scripts/scrape_wta_profiles.py --limit 25
  python scripts/scrape_wta_profiles.py --players-csv ./new_data/charting/wta_players.csv
  python scripts/scrape_wta_profiles.py --force --delay 2.0
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
from pathlib import Path

from scrape_player_profiles import (
    BASE_URL,
    REQUEST_DELAY,
    discover_from_playerlist,
    load_progress,
    save_progress,
    scrape_player,
)


def normalize_name(value: str) -> str:
    """Normalize player names for robust matching."""
    ascii_value = (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    return re.sub(r"[^a-z0-9]+", "", ascii_value.lower())


def slug_from_name(name: str) -> str:
    """Fallback slug builder when a name is not present in playerlist.js."""
    ascii_value = (
        unicodedata.normalize("NFKD", name)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    return re.sub(r"[^A-Za-z0-9]+", "", ascii_value)


def load_names(players_csv: Path) -> list[str]:
    if not players_csv.exists():
        raise FileNotFoundError(f"Players CSV not found: {players_csv}")

    names: list[str] = []
    with open(players_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames or []
        name_col = "name" if "name" in columns else (columns[0] if columns else None)
        if not name_col:
            raise ValueError(f"CSV has no readable columns: {players_csv}")
        for row in reader:
            name = (row.get(name_col) or "").strip()
            if name:
                names.append(name)
    return names


def resolve_wta_players(names: list[str]) -> tuple[list[dict], list[str]]:
    """Resolve names to WTA player objects compatible with scrape_player()."""
    discovered = discover_from_playerlist(tour_filter="WTA")
    by_name = {normalize_name(p["name"]): p for p in discovered}

    players: list[dict] = []
    unresolved: list[str] = []
    seen_slugs: set[str] = set()

    for name in names:
        normalized = normalize_name(name)
        player = by_name.get(normalized)

        if player is None:
            slug = slug_from_name(name)
            if not slug:
                unresolved.append(name)
                continue
            player = {
                "slug": slug,
                "name": name,
                "tour": "WTA",
                "profile_url": f"{BASE_URL}/cgi-bin/wplayer.cgi?p={slug}",
            }
            unresolved.append(name)

        if player["slug"] in seen_slugs:
            continue
        seen_slugs.add(player["slug"])
        players.append(player)

    return players, unresolved


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scrape WTA profiles from a CSV of player names."
    )
    parser.add_argument(
        "--players-csv",
        default="./new_data/charting/wta_players.csv",
        help="CSV with a name column (default: ./new_data/charting/wta_players.csv)",
    )
    parser.add_argument(
        "--output-dir",
        default="./new_data/profiles/WTA",
        help="Output directory (default: ./new_data/profiles/WTA)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max players to scrape (0 = all)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=REQUEST_DELAY,
        help=f"Seconds between requests (default: {REQUEST_DELAY})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-scrape players even if already completed",
    )
    return parser


def main():
    args = build_arg_parser().parse_args()

    players_csv = Path(args.players_csv)
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    names = load_names(players_csv)
    if args.limit > 0:
        names = names[: args.limit]

    players, unresolved = resolve_wta_players(names)

    index_path = output_root / "player_index.json"
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(players, f, indent=2, ensure_ascii=False)

    if unresolved:
        unresolved_path = output_root / "unresolved_names.txt"
        with open(unresolved_path, "w", encoding="utf-8") as f:
            for name in unresolved:
                f.write(name + "\n")

    progress = load_progress(output_root)
    completed = set(progress.get("completed", []))
    failed = set(progress.get("failed", []))

    remaining = [
        p for p in players
        if args.force or p["slug"] not in completed
    ]

    print("\n" + "=" * 60)
    print("🎾  WTA profile scrape")
    print(f"    Players in CSV : {len(names)}")
    print(f"    Resolved       : {len(players)}")
    print(f"    To scrape      : {len(remaining)}")
    print(f"    Output         : {output_root.resolve()}")
    if unresolved:
        print(f"    Fallback slugs : {len(unresolved)} (see unresolved_names.txt)")
    print()

    for idx, player in enumerate(remaining, 1):
        total_done = len(completed) + idx
        pct = (total_done / max(len(players), 1)) * 100
        print(f"[{total_done:4d}/{len(players)}] ({pct:5.1f}%)  {player['name']:35s}", end="  ")

        ok = scrape_player(player, output_root, delay=args.delay)
        if ok:
            print("✅")
            completed.add(player["slug"])
            failed.discard(player["slug"])
        else:
            print("❌")
            failed.add(player["slug"])

        progress = {
            "completed": sorted(completed),
            "failed": sorted(failed),
        }
        save_progress(output_root, progress)

    print("\n" + "=" * 60)
    print("📊 WTA scrape complete")
    print(f"   ✅ Success : {len(completed)}")
    print(f"   ❌ Failed  : {len(failed)}")
    print(f"📁 Output: {output_root.resolve()}")


if __name__ == "__main__":
    main()
