#!/usr/bin/env python3
"""
Tennis Abstract Comprehensive Player Profile Scraper
=====================================================
Scrapes ALL data from tennisabstract.com player profile pages.

Supports two entry points:
  1. Direct player URL:  https://www.tennisabstract.com/cgi-bin/player.cgi?p=PaulJubb
  2. Charting meta page: https://www.tennisabstract.com/charting/meta.html
     → discovers player links → scrapes each profile

Data sections scraped (matching all screenshots):
  - Bio / profile info (from inline JS variables)
  - Recent Results table
  - Tour-Level Seasons (year-by-year)
  - Challenger Seasons
  - Recent Titles and Finals
  - Year-End Rankings (ATP rank, Elo, hElo, cElo, gElo, doubles)
  - Major and Recent Events
  - Career Tour-Level Splits (surface, round, opponent type, etc.)
  - Last 52 Weeks Tour-Level Splits
  - Winners and Unforced Errors (charting)
  - Serve Speed
  - Key Points (pbp-points)
  - Key Games  (pbp-games)
  - Point-by-Point Stats (pbp-stats)
  - Match Charting: Serve
  - Match Charting: Return
  - Match Charting: Rally
  - Match Charting: Tactics
  - Most Frequent Head-to-Heads
  - Titles and Finals (full list)
  - Doubles / Mixed Doubles
  - Challenger splits (career + last 52w)

Output layout:
  output/
  └── profiles/
      └── PaulJubb/
          ├── bio.json
          ├── recent_results.csv
          ├── tour_years.csv
          ├── chall_years.csv
          ├── recent_finals.csv
          ├── year_end_rankings.csv
          ├── recent_events.csv
          ├── career_splits.csv
          ├── last52_splits.csv
          ├── winners_errors.csv
          ├── serve_speed.csv
          ├── pbp_points.csv
          ├── pbp_games.csv
          ├── pbp_stats.csv
          ├── mcp_serve.csv
          ├── mcp_return.csv
          ├── mcp_rally.csv
          ├── mcp_tactics.csv
          ├── head_to_heads.csv
          ├── titles_finals.csv
          ├── doubles.csv
          ├── mixed_doubles.csv
          ├── career_splits_chall.csv
          └── last52_splits_chall.csv

Usage:
  # Scrape a single player directly
  python tennis_abstract_scraper.py --player PaulJubb

  # Scrape a single player, specifying tour explicitly
  python tennis_abstract_scraper.py --player PaulJubb --tour ATP

  # Discover players from charting meta page and scrape all
  python tennis_abstract_scraper.py --from-meta

  # Discover players from charting meta page, limit to first 20
  python tennis_abstract_scraper.py --from-meta --limit 20

  # Scrape only WTA players discovered from meta page
  python tennis_abstract_scraper.py --from-meta --tour WTA

  # Resume interrupted run (already-completed players are skipped)
  python tennis_abstract_scraper.py --from-meta

  # Custom output directory
  python tennis_abstract_scraper.py --player PaulJubb --output-dir ./my_data

  # Slower request rate to be polite
  python tennis_abstract_scraper.py --from-meta --delay 2.0
"""

import os
import re
import sys
import time
import json
import csv
import argparse
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────

BASE_URL = "https://www.tennisabstract.com"
META_URL = f"{BASE_URL}/charting/meta.html"
PLAYERLIST_URL = f"{BASE_URL}/mwplayerlist.js"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

REQUEST_DELAY = 1.5  # seconds between HTTP requests (be polite!)

# ── Table IDs in the JS fragment ──────────────────────────────────────────────
# Maps internal table-id → friendly CSV filename stem
TABLE_MAP = {
    "recent-results":       "recent_results",
    "tour-years":           "tour_years",
    "chall-years":          "chall_years",
    "recent-finals":        "recent_finals",
    "year-end-rankings":    "year_end_rankings",
    "recent-events":        "recent_events",
    "career-splits":        "career_splits",
    "last52-splits":        "last52_splits",
    "winners-errors":       "winners_errors",
    "serve-speed":          "serve_speed",
    "pbp-points":           "pbp_points",
    "pbp-games":            "pbp_games",
    "pbp-stats":            "pbp_stats",
    "mcp-serve":            "mcp_serve",
    "mcp-return":           "mcp_return",
    "mcp-rally":            "mcp_rally",
    "mcp-tactics":          "mcp_tactics",
    "head-to-heads":        "head_to_heads",
    "titles-finals":        "titles_finals",
    "doubles":              "doubles",
    "mixed-doubles":        "mixed_doubles",
    "career-splits-chall":  "career_splits_chall",
    "last52-splits-chall":  "last52_splits_chall",
}

# ── Bio variables embedded as JS on the player page ───────────────────────────
BIO_VARS = [
    "fullname", "lastname", "currentrank", "peakrank",
    "peakfirst", "peaklast", "dob", "ht", "hand", "backhand",
    "country", "shortlist", "careerjs", "active", "lastdate",
    "twitter", "current_dubs", "peak_dubs", "peakfirst_dubs",
    "liverank", "chartagg", "itf_id", "atp_id", "dc_id",
    "wiki_id", "elo_rating", "elo_rank",
]


# ─────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────

def fetch(url: str, retries: int = 3, delay: float = REQUEST_DELAY) -> str | None:
    """Fetch URL text with retry + rate-limit delay. Returns None on failure."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            time.sleep(delay)
            return resp.text
        except requests.RequestException as exc:
            wait = 2 ** attempt
            print(f"  ⚠  Attempt {attempt+1}/{retries} failed for {url}: {exc}")
            if attempt < retries - 1:
                time.sleep(wait)
    return None


# ─────────────────────────────────────────────
# Player discovery
# ─────────────────────────────────────────────

def discover_from_meta(tour_filter: str = "both") -> list[dict]:
    """
    Discover player slugs from the charting meta page.
    The meta page lists players with links like:
        /cgi-bin/player.cgi?p=PaulJubb
        /cgi-bin/wplayer.cgi?p=IgaSwiatek
    Returns list of dicts: {slug, name, tour, profile_url}
    """
    print(f"🔍 Fetching charting meta page: {META_URL}")
    html = fetch(META_URL)
    if not html:
        print("❌ Failed to fetch meta page")
        sys.exit(1)

    soup = BeautifulSoup(html, "html.parser")
    players = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        # ATP player link
        if "/cgi-bin/player.cgi" in href:
            qs = parse_qs(urlparse(href).query)
            slug = qs.get("p", [None])[0]
            if slug and slug not in seen:
                seen.add(slug)
                if tour_filter in ("both", "ATP"):
                    players.append({
                        "slug": slug,
                        "name": a.get_text(strip=True) or slug,
                        "tour": "ATP",
                        "profile_url": urljoin(BASE_URL, href),
                    })
        # WTA player link
        elif "/cgi-bin/wplayer.cgi" in href:
            qs = parse_qs(urlparse(href).query)
            slug = qs.get("p", [None])[0]
            if slug and slug not in seen:
                seen.add(slug)
                if tour_filter in ("both", "WTA"):
                    players.append({
                        "slug": slug,
                        "name": a.get_text(strip=True) or slug,
                        "tour": "WTA",
                        "profile_url": urljoin(BASE_URL, href),
                    })

    print(f"✅ Discovered {len(players)} players from meta page")
    return players


def discover_from_playerlist(tour_filter: str = "both") -> list[dict]:
    """
    Alternative: discover players from the mwplayerlist.js autocomplete file.
    Returns list of dicts: {slug, name, tour, profile_url}
    """
    print(f"🔍 Fetching playerlist.js: {PLAYERLIST_URL}")
    raw = fetch(PLAYERLIST_URL)
    if not raw:
        print("❌ Failed to fetch player list")
        sys.exit(1)

    match = re.search(r'var playerlist=\[(.*?)\];', raw, re.DOTALL)
    if not match:
        print("❌ Could not parse player list JS")
        sys.exit(1)

    entries = re.findall(r'"(\([MW]\)\s+[^"]+)"', match.group(1))
    players = []
    seen: set[str] = set()

    for entry in entries:
        gender = entry[1]          # M or W
        name = entry[4:].strip()   # skip "(M) " prefix
        slug = name.replace(" ", "")
        tour = "ATP" if gender == "M" else "WTA"

        if slug in seen:
            continue
        seen.add(slug)

        if tour_filter not in ("both", tour):
            continue

        cgi = "player.cgi" if tour == "ATP" else "wplayer.cgi"
        players.append({
            "slug": slug,
            "name": name,
            "tour": tour,
            "profile_url": f"{BASE_URL}/cgi-bin/{cgi}?p={slug}",
        })

    print(f"✅ Found {len(players)} players in playerlist.js")
    return players


# ─────────────────────────────────────────────
# Parsing helpers
# ─────────────────────────────────────────────

def parse_bio(html: str) -> dict:
    """Extract JS-embedded bio variables from player page HTML."""
    bio = {}
    for var in BIO_VARS:
        # Handles:  var fullname = 'Paul Jubb';
        #           var currentrank = 287;
        #           var current_dubs = '"UNR"';
        pattern = rf"var\s+{var}\s*=\s*(.+?);"
        m = re.search(pattern, html)
        if not m:
            continue
        raw = m.group(1).strip()

        # Strip outer quotes (single or double)
        if (raw.startswith("'") and raw.endswith("'")) or \
           (raw.startswith('"') and raw.endswith('"')):
            raw = raw[1:-1]

        # Try numeric conversion
        try:
            bio[var] = float(raw) if "." in raw else int(raw)
        except (ValueError, TypeError):
            bio[var] = raw

    return bio


def parse_jsfrag_tables(js_content: str) -> dict[str, dict]:
    """
    Extract all HTML tables from the player JS fragment.

    The fragment file contains:
        var player_frag = `...big HTML block...`;

    Returns dict keyed by TABLE_MAP friendly name →
        {"headers": [...], "rows": [[...], ...]}
    """
    # Try backtick template literal first, then single/double quote
    for pattern in [
        r'var\s+player_frag\s*=\s*`(.*?)`\s*;',
        r"var\s+player_frag\s*=\s*'(.*?)'\s*;",
        r'var\s+player_frag\s*=\s*"(.*?)"\s*;',
    ]:
        m = re.search(pattern, js_content, re.DOTALL)
        if m:
            html_content = m.group(1)
            break
    else:
        # Fallback: maybe the entire file IS the HTML fragment
        html_content = js_content

    soup = BeautifulSoup(html_content, "html.parser")
    results = {}

    for table_id, friendly_name in TABLE_MAP.items():
        table = soup.find("table", id=table_id)
        if not table:
            continue

        headers = _extract_headers(table)
        rows = _extract_rows(table)

        if headers or rows:
            results[friendly_name] = {"headers": headers, "rows": rows}

    return results


def _extract_headers(table) -> list[str]:
    """Pull column headers from <thead> or first <tr> with <th> elements."""
    thead = table.find("thead")
    if thead:
        for tr in thead.find_all("tr"):
            ths = tr.find_all("th")
            if ths:
                return [th.get_text(separator=" ", strip=True) for th in ths]

    first_row = table.find("tr")
    if first_row:
        ths = first_row.find_all("th")
        if ths:
            return [th.get_text(separator=" ", strip=True) for th in ths]

    return []


def _extract_rows(table) -> list[list[str]]:
    """Extract all data rows from <tbody> (or table itself)."""
    tbody = table.find("tbody")
    container = tbody if tbody else table
    rows = []
    for tr in container.find_all("tr"):
        cells = tr.find_all("td")
        if cells:
            rows.append([c.get_text(separator=" ", strip=True) for c in cells])
    return rows


# ─────────────────────────────────────────────
# Core scrape logic
# ─────────────────────────────────────────────

def scrape_player(player: dict, output_root: Path, delay: float = REQUEST_DELAY) -> bool:
    """
    Scrape one player's full profile and save results.
    Returns True on success.
    """
    slug = player["slug"]
    tour = player["tour"]
    profile_url = player["profile_url"]

    # Determine URLs
    jsfrag_url = f"{BASE_URL}/jsfrags/{slug}.js"

    player_dir = output_root / slug
    player_dir.mkdir(parents=True, exist_ok=True)

    print(f"  → Fetching profile page …")
    html = fetch(profile_url, delay=delay)
    if not html:
        _write_error(player_dir, f"Failed to fetch profile page: {profile_url}")
        return False

    # ── Bio ──────────────────────────────────────────────────────────────────
    bio = parse_bio(html)
    bio["slug"] = slug
    bio["tour"] = tour
    bio["profile_url"] = profile_url
    bio["jsfrag_url"] = jsfrag_url

    # Also try to extract the player's display name from the page <title>
    title_m = re.search(r"<title>(.*?)</title>", html, re.IGNORECASE)
    if title_m:
        bio["page_title"] = title_m.group(1).strip()

    bio_path = player_dir / "bio.json"
    with open(bio_path, "w", encoding="utf-8") as f:
        json.dump(bio, f, indent=2, ensure_ascii=False)
    print(f"     bio.json → {len(bio)} fields")

    # ── JS fragment (tables) ─────────────────────────────────────────────────
    print(f"  → Fetching JS fragment …")
    js_content = fetch(jsfrag_url, delay=delay)

    if not js_content:
        print(f"  ⚠  No JS fragment found at {jsfrag_url}")
        _write_error(player_dir, f"No JS fragment: {jsfrag_url}", append=True)
        return bool(bio)  # partial success if we at least got bio

    tables = parse_jsfrag_tables(js_content)

    saved_tables = 0
    for friendly_name, data in tables.items():
        csv_path = player_dir / f"{friendly_name}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if data["headers"]:
                writer.writerow(data["headers"])
            writer.writerows(data["rows"])
        row_count = len(data["rows"])
        print(f"     {friendly_name}.csv → {row_count} rows")
        saved_tables += 1

    if saved_tables == 0:
        print(f"  ⚠  JS fragment fetched but no tables parsed — check fragment format")
        # Save raw fragment for debugging
        with open(player_dir / "_raw_jsfrag.js", "w", encoding="utf-8") as f:
            f.write(js_content[:50_000])  # first 50 kB for inspection

    return True


def _write_error(player_dir: Path, msg: str, append: bool = False):
    mode = "a" if append else "w"
    with open(player_dir / "_errors.txt", mode, encoding="utf-8") as f:
        f.write(msg + "\n")


# ─────────────────────────────────────────────
# Progress / resume support
# ─────────────────────────────────────────────

def load_progress(output_root: Path) -> dict:
    p = output_root / ".progress.json"
    if p.exists():
        with open(p) as f:
            return json.load(f)
    return {"completed": [], "failed": []}


def save_progress(output_root: Path, progress: dict):
    p = output_root / ".progress.json"
    with open(p, "w") as f:
        json.dump(progress, f, indent=2)


# ─────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Tennis Abstract comprehensive player profile scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    source = p.add_mutually_exclusive_group()
    source.add_argument(
        "--player", metavar="SLUG",
        help="Scrape a single player by slug, e.g. PaulJubb"
    )
    source.add_argument(
        "--from-meta", action="store_true",
        help="Discover players from charting/meta.html and scrape all"
    )
    source.add_argument(
        "--from-playerlist", action="store_true",
        help="Discover players from mwplayerlist.js and scrape all"
    )
    p.add_argument(
        "--tour", choices=["ATP", "WTA", "both"], default="ATP",
        help="Which tour to scrape (only relevant for discovery modes; default: ATP)"
    )
    p.add_argument(
        "--limit", type=int, default=0,
        help="Max players to scrape in discovery mode (0 = all)"
    )
    p.add_argument(
        "--output-dir", default="./output/profiles",
        help="Root output directory (default: ./output/profiles)"
    )
    p.add_argument(
        "--delay", type=float, default=REQUEST_DELAY,
        help=f"Seconds between requests (default: {REQUEST_DELAY})"
    )
    p.add_argument(
        "--force", action="store_true",
        help="Re-scrape even if player already completed"
    )
    return p


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    # ── Single player mode ───────────────────────────────────────────────────
    if args.player:
        slug = args.player
        tour = args.tour if args.tour != "both" else "ATP"
        cgi = "player.cgi" if tour == "ATP" else "wplayer.cgi"
        player = {
            "slug": slug,
            "name": slug,
            "tour": tour,
            "profile_url": f"{BASE_URL}/cgi-bin/{cgi}?p={slug}",
        }
        print(f"\n🎾 Scraping: {slug} ({tour})")
        print(f"   Profile : {player['profile_url']}")
        print(f"   Output  : {output_root / slug}")
        print()
        ok = scrape_player(player, output_root, delay=args.delay)
        if ok:
            player_dir = output_root / slug
            files = sorted(player_dir.iterdir())
            print(f"\n✅ Done! Saved {len(files)} file(s):")
            for fp in files:
                size_kb = fp.stat().st_size / 1024
                print(f"   📄 {fp.name:35s} {size_kb:6.1f} KB")
        else:
            print("\n❌ Scrape failed — check _errors.txt in output directory")
        return

    # ── Discovery mode ───────────────────────────────────────────────────────
    if args.from_meta:
        players = discover_from_meta(tour_filter=args.tour)
    elif args.from_playerlist:
        players = discover_from_playerlist(tour_filter=args.tour)
    else:
        parser.print_help()
        print("\n⚠  Please specify --player SLUG, --from-meta, or --from-playerlist")
        sys.exit(1)

    if args.limit > 0:
        players = players[:args.limit]

    progress = load_progress(output_root)

    # Save player index
    index_path = output_root / "player_index.json"
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(players, f, indent=2, ensure_ascii=False)

    remaining = [
        p for p in players
        if args.force or p["slug"] not in progress["completed"]
    ]

    print(f"\n{'='*60}")
    print(f"🎾  Tennis Abstract scrape")
    print(f"    Total    : {len(players)}")
    print(f"    Done     : {len(progress['completed'])}")
    print(f"    To scrape: {len(remaining)}")
    est = len(remaining) * args.delay * 2 / 60
    print(f"    Est. time: ~{est:.0f} min  (2 requests/player × {args.delay}s delay)")
    print(f"    Output   : {output_root.resolve()}")
    print()

    for idx, player in enumerate(remaining, 1):
        total_done = len(progress["completed"]) + idx
        pct = total_done / len(players) * 100
        print(f"[{total_done:4d}/{len(players)}] ({pct:5.1f}%)  {player['name']:40s}", end="  ")

        ok = scrape_player(player, output_root, delay=args.delay)
        if ok:
            print("✅")
            if player["slug"] not in progress["completed"]:
                progress["completed"].append(player["slug"])
        else:
            print("❌")
            if player["slug"] not in progress["failed"]:
                progress["failed"].append(player["slug"])

        save_progress(output_root, progress)

    print(f"\n{'='*60}")
    print(f"📊 Scrape complete!")
    print(f"   ✅ Success : {len(progress['completed'])}")
    print(f"   ❌ Failed  : {len(progress['failed'])}")
    if progress["failed"]:
        print(f"   Failed slugs: {', '.join(progress['failed'][:20])}")
    print(f"\n📁 Output: {output_root.resolve()}")


if __name__ == "__main__":
    main()