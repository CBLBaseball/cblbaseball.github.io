#!/usr/bin/env python3
"""Update CBL Free Agent Pool leaderboards using FanGraphs' leaders JSON endpoint.

Why you kept getting only ~10 rows:
- FanGraphs enforces a per-request row cap in some situations (even if `pageitems` is large).
- The reliable workaround is to request the data in **player-id batches** and merge the results.

This script:
- Splits the player list into batches (default 40 IDs/request)
- Fetches each batch and merges rows (deduped by player id when available)
- Normalizes hitters to fixed columns (AVG/OBP/SLG/OPS to 3 decimals)

Endpoint:
  https://www.fangraphs.com/api/leaders/major-league/data
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

OUT_DIR = Path("data/fa")
OUT_DIR.mkdir(parents=True, exist_ok=True)

SEASON = 2025
API = "https://www.fangraphs.com/api/leaders/major-league/data"

SEGMENTS = {
  "hit": [
    26546,
    27915,
    19455,
    23802,
    16925,
    15654,
    30028,
    21496,
    16398,
    33280,
    29646,
    35108,
    30038,
    21587,
    31912,
    21547,
    25629,
    21535,
    19458,
    25183,
    25705,
    23395,
    23695,
    31363,
    24878,
    23372,
    25477,
    29830,
    31661,
    26202,
    31370,
    28253,
    27690,
    23968,
    27501,
    26374,
    26244,
    29844,
    27963,
    24605,
    10655,
    18054,
    27789,
    22766,
    19960,
    29571,
    19877,
    30063,
    26148,
    26143,
    10200,
    29949,
    31396,
    25999,
    31583,
    19562
  ],
  "sp": [
    31764,
    13050,
    26056,
    20370,
    31815,
    19736,
    30113,
    31312,
    26171,
    27932,
    31623,
    23301,
    19666,
    26482,
    19222,
    14120,
    31475,
    23735,
    17732,
    15094,
    26440,
    13580,
    16358,
    20629
  ],
  "rp": [
    31764,
    25327,
    29633,
    26203,
    13050,
    26136,
    21345,
    33568,
    27695,
    19804,
    31815,
    19736,
    13607,
    30161,
    9174,
    30113,
    30206,
    27662,
    15256,
    27481,
    22113,
    31312,
    25873,
    33248,
    27974,
    26260,
    22176,
    27626,
    19586,
    23324,
    27583,
    21863,
    19835,
    20546,
    26259,
    27932,
    21212,
    18674,
    23301,
    19666,
    26482,
    16631,
    19205,
    30016,
    24591,
    16128,
    24590,
    13190,
    27984,
    25957,
    27271,
    21924,
    15514,
    29615,
    20515,
    19222,
    14120,
    31884,
    23811,
    25839,
    24710,
    26353,
    22288,
    29770,
    20827,
    17732,
    19281,
    29564,
    33821,
    15094,
    24094,
    26440,
    13580,
    26344,
    26285,
    20629,
    20379
  ]
}
HITTER_COLS = ["Bats", "Name", "Age", "Team", "Season", "G", "AB", "PA", "H", "2B", "3B", "HR", "R", "RBI", "BB", "SO", "HBP", "SB", "CS", "AVG", "OBP", "SLG", "OPS"]
BATCH_SIZE = 40

def chunk(lst: List[int], n: int) -> List[List[int]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def call_api(params: Dict[str, Any], tries: int = 6) -> Dict[str, Any]:
    delay = 2.0
    last_err: Optional[Exception] = None
    for attempt in range(1, tries + 1):
        try:
            r = requests.get(API, params=params, timeout=60, headers={
                "User-Agent": "Mozilla/5.0 (CBL dashboard bot)",
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://www.fangraphs.com/leaders/major-league",
            })
            if r.status_code in (429, 403, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if attempt == tries:
                raise
            time.sleep(delay)
            delay = min(delay * 1.8, 20.0)
    raise last_err or RuntimeError("Unknown error")

def leaders_params(players: List[int], stats: str, month: int) -> Dict[str, Any]:
    return {
        "ind": "0",
        "lg": "all",
        "pos": "all",
        "qual": "0",
        "season": str(SEASON),
        "season1": str(SEASON),
        "stats": stats,        # bat | pit
        "month": str(month),   # 0 all; 13 vs L; 14 vs R
        "players": ",".join(map(str, players)),
        "team": "0,ts",
        "rost": "0",
        "type": "8",
        "sortcol": "17",
        "sortdir": "default",
        "pageitems": "500",    # enough for a batch
        "pagenum": "1",
        "filter": "",
    }

def normalize_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = payload.get("data")
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    for key in ("rows", "result", "results"):
        v = payload.get(key)
        if isinstance(v, list):
            return [r for r in v if isinstance(r, dict)]
    return []

def first_present(row: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in row:
            return row.get(k)
    return ""

def fmt3(v: Any) -> Any:
    if v is None or v == "":
        return ""
    try:
        fv = float(v)
        return f"{fv:.3f}"
    except Exception:
        return v

def normalize_hitter(row: Dict[str, Any]) -> Dict[str, Any]:
    mapped: Dict[str, Any] = {}
    mapped["Bats"] = first_present(row, ["Bats", "Bat", "B"])
    mapped["Name"] = first_present(row, ["Name", "Player", "playerName", "PlayerName"])
    mapped["Age"] = first_present(row, ["Age"])
    mapped["Team"] = first_present(row, ["Team", "Tm", "TeamName", "AbbName"])
    mapped["Season"] = first_present(row, ["Season", "season", "Year"])
    mapped["G"] = first_present(row, ["G", "Games"])
    mapped["AB"] = first_present(row, ["AB"])
    mapped["PA"] = first_present(row, ["PA"])
    mapped["H"] = first_present(row, ["H", "Hits"])
    mapped["2B"] = first_present(row, ["2B", "Doubles"])
    mapped["3B"] = first_present(row, ["3B", "Triples"])
    mapped["HR"] = first_present(row, ["HR", "HomeRuns"])
    mapped["R"] = first_present(row, ["R", "Runs"])
    mapped["RBI"] = first_present(row, ["RBI"])
    mapped["BB"] = first_present(row, ["BB"])
    mapped["SO"] = first_present(row, ["SO", "K", "Ks"])
    mapped["HBP"] = first_present(row, ["HBP"])
    mapped["SB"] = first_present(row, ["SB"])
    mapped["CS"] = first_present(row, ["CS"])
    mapped["AVG"] = fmt3(first_present(row, ["AVG", "BA", "Avg"]))
    mapped["OBP"] = fmt3(first_present(row, ["OBP"]))
    mapped["SLG"] = fmt3(first_present(row, ["SLG"]))
    mapped["OPS"] = fmt3(first_present(row, ["OPS"]))
    return {k: mapped.get(k, "") for k in HITTER_COLS}

def save_json(name: str, rows: List[Dict[str, Any]]):
    (OUT_DIR / f"{name}.json").write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")

def merge_rows(all_rows: List[Dict[str, Any]], new_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    merged: List[Dict[str, Any]] = []
    for r in all_rows + new_rows:
        pid = first_present(r, ["playerid", "PlayerId", "playerId", "ID", "id"])
        key = str(pid).strip() if pid not in ("", None) else (str(r.get("Name","")).strip() + "|" + str(r.get("Team","")).strip())
        if key in seen:
            continue
        seen.add(key)
        merged.append(r)
    return merged

def fetch_batched(players: List[int], stats: str, month: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    batches = chunk(players, BATCH_SIZE)
    for i, batch in enumerate(batches, start=1):
        payload = call_api(leaders_params(batch, stats, month))
        batch_rows = normalize_rows(payload)
        rows = merge_rows(rows, batch_rows)
        time.sleep(0.6)
    return rows

def fetch_and_save(out_name: str, seg_key: str, stats: str, month: int):
    players = SEGMENTS[seg_key]
    rows = fetch_batched(players, stats, month)
    if stats == "bat":
        rows = [normalize_hitter(r) for r in rows]
    save_json(out_name, rows)
    print(f"Saved {out_name}: {len(rows)} rows")

def main():
    tasks = [
        ("hit_bat_all", "hit", "bat", 0),
        ("hit_bat_lhp", "hit", "bat", 13),
        ("hit_bat_rhp", "hit", "bat", 14),

        ("sp_pit_all", "sp", "pit", 0),
        ("sp_pit_lhb", "sp", "pit", 13),
        ("sp_pit_rhb", "sp", "pit", 14),

        ("rp_pit_all", "rp", "pit", 0),
        ("rp_pit_lhb", "rp", "pit", 13),
        ("rp_pit_rhb", "rp", "pit", 14),
    ]
    for out_name, seg_key, stats, month in tasks:
        fetch_and_save(out_name, seg_key, stats, month)
        time.sleep(1.2)

if __name__ == "__main__":
    main()
