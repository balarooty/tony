#!/usr/bin/env python3
"""
Predict tennis match outcome from local player profile data.

Uses profile metrics from:
  - bio.json
  - last52_splits.csv (preferred)
  - career_splits.csv (fallback)
  - year_end_rankings.csv
  - recent_results.csv

Outputs probabilities for:
  - match winner
  - total sets
  - total games

Examples:
  python scripts/predict_match_outcome.py --tour ATP --player1 NovakDjokovic --player2 CarlosAlcaraz --surface Hard
  python scripts/predict_match_outcome.py --tour WTA --player1 IgaSwiatek --player2 ArynaSabalenka --surface Hard --sims 30000
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


DEFAULT_PROFILES_ROOT = Path("./new_data/profiles")


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def normalize_text(value: str) -> str:
    ascii_value = (
        unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    )
    return re.sub(r"[^a-z0-9]+", "", ascii_value.lower())


def parse_float(raw: object) -> Optional[float]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text or text in {"-", "--", "—"}:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def parse_pct(raw: object) -> Optional[float]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text or text in {"-", "--", "—"}:
        return None
    text = text.replace("%", "").replace(",", "")
    try:
        return float(text) / 100.0
    except ValueError:
        return None


def load_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def slugify_identifier(identifier: str) -> str:
    ascii_value = (
        unicodedata.normalize("NFKD", identifier).encode("ascii", "ignore").decode("ascii")
    )
    return re.sub(r"[^A-Za-z0-9]+", "", ascii_value)


def resolve_player_dir(profiles_tour_root: Path, identifier: str) -> Path:
    direct = profiles_tour_root / identifier
    if direct.exists() and direct.is_dir():
        return direct

    slug = slugify_identifier(identifier)
    by_slug = profiles_tour_root / slug
    if by_slug.exists() and by_slug.is_dir():
        return by_slug

    norm_id = normalize_text(identifier)
    for child in profiles_tour_root.iterdir():
        if not child.is_dir():
            continue
        if normalize_text(child.name) == norm_id:
            return child
        bio = child / "bio.json"
        if bio.exists():
            data = load_json(bio)
            fullname = str(data.get("fullname", "")).strip()
            if fullname and normalize_text(fullname) == norm_id:
                return child

    raise FileNotFoundError(
        f"Could not resolve player '{identifier}' in {profiles_tour_root}"
    )


def choose_surface_row(rows: List[Dict[str, str]], surface: str) -> Optional[Dict[str, str]]:
    if not rows:
        return None

    target = surface.strip().lower()
    for row in rows:
        split = str(row.get("Split", "")).strip().lower()
        if split == target:
            return row

    # Fallback to common rows if specific surface isn't available.
    priority = ["hard", "clay", "grass", "best of 3", "best of 5"]
    for split_name in priority:
        for row in rows:
            split = str(row.get("Split", "")).strip().lower()
            if split == split_name:
                return row

    return rows[0]


def game_prob_from_point_prob(s: float) -> float:
    # Probability server wins game from deuce scoring, from point probability s.
    s = clamp(s, 1e-6, 1 - 1e-6)
    # Win before deuce
    w0 = s ** 4
    w15 = 4 * (s ** 4) * (1 - s)
    w30 = 10 * (s ** 4) * ((1 - s) ** 2)
    # Reach deuce at 3-3
    deuce = 20 * (s ** 3) * ((1 - s) ** 3)
    win_from_deuce = (s * s) / ((s * s) + ((1 - s) * (1 - s)))
    return w0 + w15 + w30 + deuce * win_from_deuce


@dataclass
class PlayerMetrics:
    player_id: str
    fullname: str
    rank: float
    elo: float
    hold_pct: float
    break_pct: float
    spw: float
    rpw: float
    dr: float
    recent_dr: float


def extract_current_elo(year_end_rows: List[Dict[str, str]]) -> Optional[float]:
    if not year_end_rows:
        return None
    for row in year_end_rows:
        year_label = str(row.get("Year", ""))
        if year_label.lower().startswith("current"):
            elo = parse_float(row.get("Elo"))
            if elo is not None:
                return elo
    # fallback first row Elo
    return parse_float(year_end_rows[0].get("Elo"))


def extract_recent_dr(recent_rows: List[Dict[str, str]], n: int = 12) -> Optional[float]:
    values: List[float] = []
    for row in recent_rows[:n]:
        dr = parse_float(row.get("DR"))
        if dr is not None:
            values.append(dr)
    if not values:
        return None
    return sum(values) / len(values)


def extract_player_metrics(player_dir: Path, surface: str) -> PlayerMetrics:
    bio = load_json(player_dir / "bio.json")
    last52 = load_csv(player_dir / "last52_splits.csv")
    career = load_csv(player_dir / "career_splits.csv")
    year_end = load_csv(player_dir / "year_end_rankings.csv")
    recent = load_csv(player_dir / "recent_results.csv")

    row = choose_surface_row(last52, surface)
    if row is None:
        row = choose_surface_row(career, surface)
    if row is None:
        raise ValueError(f"No usable splits rows for player directory: {player_dir}")

    hold_pct = parse_pct(row.get("Hld%"))
    break_pct = parse_pct(row.get("Brk%"))
    spw = parse_pct(row.get("SPW"))
    rpw = parse_pct(row.get("RPW"))
    dr = parse_float(row.get("DR"))

    # Fallbacks if selected row has missing fields.
    fallback_row = choose_surface_row(career, surface)
    if fallback_row:
        hold_pct = hold_pct if hold_pct is not None else parse_pct(fallback_row.get("Hld%"))
        break_pct = break_pct if break_pct is not None else parse_pct(fallback_row.get("Brk%"))
        spw = spw if spw is not None else parse_pct(fallback_row.get("SPW"))
        rpw = rpw if rpw is not None else parse_pct(fallback_row.get("RPW"))
        dr = dr if dr is not None else parse_float(fallback_row.get("DR"))

    hold_pct = hold_pct if hold_pct is not None else 0.75
    break_pct = break_pct if break_pct is not None else 0.25
    spw = spw if spw is not None else 0.62
    rpw = rpw if rpw is not None else 0.38
    dr = dr if dr is not None else 1.0

    rank = parse_float(bio.get("currentrank"))
    rank = rank if rank is not None else 250.0

    elo = extract_current_elo(year_end)
    elo = elo if elo is not None else 1800.0

    recent_dr = extract_recent_dr(recent)
    recent_dr = recent_dr if recent_dr is not None else dr

    return PlayerMetrics(
        player_id=player_dir.name,
        fullname=str(bio.get("fullname") or player_dir.name),
        rank=rank,
        elo=elo,
        hold_pct=hold_pct,
        break_pct=break_pct,
        spw=spw,
        rpw=rpw,
        dr=dr,
        recent_dr=recent_dr,
    )


def tiebreak_server(first_server: int, point_idx: int) -> int:
    if point_idx == 0:
        return first_server
    block = (point_idx - 1) // 2
    if block % 2 == 0:
        return 3 - first_server
    return first_server


def simulate_tiebreak(
    first_server: int,
    p1_serve_point: float,
    p2_serve_point: float,
    rng: random.Random,
) -> int:
    p1_points = 0
    p2_points = 0
    point_idx = 0
    while True:
        server = tiebreak_server(first_server, point_idx)
        if server == 1:
            p1_wins_point = rng.random() < p1_serve_point
        else:
            p1_wins_point = rng.random() >= p2_serve_point

        if p1_wins_point:
            p1_points += 1
        else:
            p2_points += 1

        point_idx += 1
        if (p1_points >= 7 or p2_points >= 7) and abs(p1_points - p2_points) >= 2:
            return 1 if p1_points > p2_points else 2


def simulate_set(
    first_server: int,
    p1_hold: float,
    p2_hold: float,
    p1_serve_point: float,
    p2_serve_point: float,
    rng: random.Random,
) -> Tuple[int, int, int]:
    games1 = 0
    games2 = 0
    server = first_server

    while True:
        if games1 == 6 and games2 == 6:
            winner = simulate_tiebreak(server, p1_serve_point, p2_serve_point, rng)
            if winner == 1:
                games1 = 7
            else:
                games2 = 7
            next_set_first_server = 3 - server
            return winner, games1 + games2, next_set_first_server

        if server == 1:
            p1_wins_game = rng.random() < p1_hold
        else:
            p1_wins_game = rng.random() >= p2_hold

        if p1_wins_game:
            games1 += 1
        else:
            games2 += 1

        server = 3 - server

        if (games1 >= 6 or games2 >= 6) and abs(games1 - games2) >= 2:
            winner = 1 if games1 > games2 else 2
            return winner, games1 + games2, server


def simulate_match(
    best_of: int,
    p1_hold: float,
    p2_hold: float,
    p1_serve_point: float,
    p2_serve_point: float,
    rng: random.Random,
) -> Tuple[int, int, int]:
    sets_to_win = best_of // 2 + 1
    sets1 = 0
    sets2 = 0
    total_games = 0
    first_server = 1 if rng.random() < 0.5 else 2

    while sets1 < sets_to_win and sets2 < sets_to_win:
        winner, set_games, next_server = simulate_set(
            first_server, p1_hold, p2_hold, p1_serve_point, p2_serve_point, rng
        )
        total_games += set_games
        first_server = next_server
        if winner == 1:
            sets1 += 1
        else:
            sets2 += 1

    winner = 1 if sets1 > sets2 else 2
    total_sets = sets1 + sets2
    return winner, total_sets, total_games


def summarize_distribution(counter: Dict[int, int], total: int) -> Dict[str, float]:
    return {str(k): v / total for k, v in sorted(counter.items(), key=lambda x: x[0])}


def build_pre_match_probabilities(
    p1: PlayerMetrics,
    p2: PlayerMetrics,
) -> Tuple[float, float, float, float]:
    # Serve-point matchup adjustment from own SPW and opponent RPW.
    p1_serve_point_raw = 0.5 * p1.spw + 0.5 * (1.0 - p2.rpw)
    p2_serve_point_raw = 0.5 * p2.spw + 0.5 * (1.0 - p1.rpw)

    # Game-level hold from observed hold/break and from point model.
    p1_hold_from_rates = 0.5 * p1.hold_pct + 0.5 * (1.0 - p2.break_pct)
    p2_hold_from_rates = 0.5 * p2.hold_pct + 0.5 * (1.0 - p1.break_pct)
    p1_hold_from_points = game_prob_from_point_prob(p1_serve_point_raw)
    p2_hold_from_points = game_prob_from_point_prob(p2_serve_point_raw)

    p1_hold = 0.6 * p1_hold_from_rates + 0.4 * p1_hold_from_points
    p2_hold = 0.6 * p2_hold_from_rates + 0.4 * p2_hold_from_points

    # Small skill adjustment from Elo + recent DR.
    elo_shift = clamp((p1.elo - p2.elo) / 4000.0, -0.04, 0.04)
    dr_shift = clamp((p1.recent_dr - p2.recent_dr) / 20.0, -0.02, 0.02)
    total_shift = elo_shift + dr_shift

    p1_hold = clamp(p1_hold + total_shift, 0.45, 0.95)
    p2_hold = clamp(p2_hold - total_shift, 0.45, 0.95)
    p1_serve_point = clamp(p1_serve_point_raw + total_shift * 0.6, 0.45, 0.80)
    p2_serve_point = clamp(p2_serve_point_raw - total_shift * 0.6, 0.45, 0.80)

    return p1_hold, p2_hold, p1_serve_point, p2_serve_point


def predict(
    p1: PlayerMetrics,
    p2: PlayerMetrics,
    surface: str,
    best_of: int,
    sims: int,
    seed: int,
) -> Dict:
    p1_hold, p2_hold, p1_serve_point, p2_serve_point = build_pre_match_probabilities(p1, p2)
    rng = random.Random(seed)

    winner_counts: Dict[int, int] = {1: 0, 2: 0}
    sets_counts: Dict[int, int] = {}
    games_counts: Dict[int, int] = {}
    total_sets_acc = 0
    total_games_acc = 0

    for _ in range(sims):
        winner, total_sets, total_games = simulate_match(
            best_of, p1_hold, p2_hold, p1_serve_point, p2_serve_point, rng
        )
        winner_counts[winner] += 1
        sets_counts[total_sets] = sets_counts.get(total_sets, 0) + 1
        games_counts[total_games] = games_counts.get(total_games, 0) + 1
        total_sets_acc += total_sets
        total_games_acc += total_games

    p1_win = winner_counts[1] / sims
    p2_win = winner_counts[2] / sims
    expected_sets = total_sets_acc / sims
    expected_games = total_games_acc / sims

    most_likely_sets = max(sets_counts.items(), key=lambda x: x[1])[0]
    most_likely_games = max(games_counts.items(), key=lambda x: x[1])[0]

    top_total_games = sorted(games_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    return {
        "inputs": {
            "player1": p1.fullname,
            "player2": p2.fullname,
            "surface": surface,
            "best_of": best_of,
            "simulations": sims,
            "seed": seed,
        },
        "model_features": {
            "player1": {
                "rank": p1.rank,
                "elo": p1.elo,
                "hold_pct": p1.hold_pct,
                "break_pct": p1.break_pct,
                "spw": p1.spw,
                "rpw": p1.rpw,
                "recent_dr": p1.recent_dr,
            },
            "player2": {
                "rank": p2.rank,
                "elo": p2.elo,
                "hold_pct": p2.hold_pct,
                "break_pct": p2.break_pct,
                "spw": p2.spw,
                "rpw": p2.rpw,
                "recent_dr": p2.recent_dr,
            },
            "derived": {
                "p1_hold": p1_hold,
                "p2_hold": p2_hold,
                "p1_serve_point": p1_serve_point,
                "p2_serve_point": p2_serve_point,
            },
        },
        "prediction": {
            "winner": p1.fullname if p1_win >= p2_win else p2.fullname,
            "winner_probabilities": {
                p1.fullname: p1_win,
                p2.fullname: p2_win,
            },
            "total_sets_distribution": summarize_distribution(sets_counts, sims),
            "total_games_distribution": summarize_distribution(games_counts, sims),
            "top_total_games": [
                {"games": games, "probability": count / sims}
                for games, count in top_total_games
            ],
            "most_likely_total_sets": most_likely_sets,
            "most_likely_total_games": most_likely_games,
            "expected_total_sets": expected_sets,
            "expected_total_games": expected_games,
        },
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Predict tennis match winner / total sets / total games from profile data."
    )
    parser.add_argument("--tour", choices=["ATP", "WTA"], required=True)
    parser.add_argument("--player1", required=True, help="Player 1 slug or full name")
    parser.add_argument("--player2", required=True, help="Player 2 slug or full name")
    parser.add_argument("--surface", choices=["Hard", "Clay", "Grass"], default="Hard")
    parser.add_argument("--best-of", type=int, choices=[3, 5], default=3)
    parser.add_argument("--sims", type=int, default=20000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--profiles-root",
        default=str(DEFAULT_PROFILES_ROOT),
        help="Root profiles directory containing ATP/ and WTA/",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional JSON output file path",
    )
    return parser


def main():
    args = build_arg_parser().parse_args()

    profiles_root = Path(args.profiles_root)
    tour_root = profiles_root / args.tour
    if not tour_root.exists():
        raise FileNotFoundError(f"Tour directory not found: {tour_root}")

    p1_dir = resolve_player_dir(tour_root, args.player1)
    p2_dir = resolve_player_dir(tour_root, args.player2)
    if p1_dir == p2_dir:
        raise ValueError("player1 and player2 resolve to the same profile.")

    p1 = extract_player_metrics(p1_dir, args.surface)
    p2 = extract_player_metrics(p2_dir, args.surface)

    result = predict(
        p1=p1,
        p2=p2,
        surface=args.surface,
        best_of=args.best_of,
        sims=args.sims,
        seed=args.seed,
    )

    text = json.dumps(result, indent=2, ensure_ascii=False)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text + "\n", encoding="utf-8")
        print(f"Saved prediction JSON to {output_path.resolve()}")
    else:
        print(text)


if __name__ == "__main__":
    main()
