"""
Fenway fit: which non-Red Sox hitters and pitchers would gain or lose most
from playing half their games at Fenway Park.

One file, start to finish. Press Run in VS Code (or: python fenway_fit.py).

  1. Download regular-season Statcast data from 2021 into fenway.duckdb
     (only months not already downloaded; the first run takes ~20 minutes).
  2. Build the batted-ball table and run data checks.
  3. Measure Fenway's effect by exit velocity, launch angle and direction.
  4. Score every active player and print the top 10 hitters and pitchers.
  5. Print what helps/hurts at Fenway, then an out-of-sample test.

Needs: pip install duckdb pybaseball
"""
import datetime as dt
import sys
from pathlib import Path

import duckdb

HERE = Path(__file__).parent
DB = HERE / "fenway.duckdb"
FIRST_SEASON = 2021   # 2020 and earlier are ignored everywhere
SHRINK = 25           # phantom road balls added to each Fenway bucket
WOBA_SCALE = 1.25     # wOBA points per run, per plate appearance
MIN_BATTED_BALLS = 300


# ---------------------------------------------------------------- 1. download

COLUMNS = {
    "game_pk": "INTEGER", "at_bat_number": "INTEGER", "pitch_number": "INTEGER",
    "game_date": "DATE", "game_year": "INTEGER", "game_type": "VARCHAR",
    "home_team": "VARCHAR", "away_team": "VARCHAR", "inning_topbot": "VARCHAR",
    "batter": "INTEGER", "pitcher": "INTEGER", "stand": "VARCHAR",
    "p_throws": "VARCHAR", "type": "VARCHAR", "events": "VARCHAR",
    "bb_type": "VARCHAR", "launch_speed": "DOUBLE", "launch_angle": "DOUBLE",
    "hc_x": "DOUBLE", "hc_y": "DOUBLE", "hit_distance_sc": "DOUBLE",
    "woba_value": "DOUBLE", "woba_denom": "DOUBLE",
}


def months(first_season, today):
    """(start, end) date pairs covering March through November of each season."""
    for year in range(first_season, today.year + 1):
        for month in range(3, 12):
            start = dt.date(year, month, 1)
            if start > today:
                return
            nxt = dt.date(year, month + 1, 1)
            yield start, min(nxt - dt.timedelta(days=1), today)


def pull_data(con):
    cols = ", ".join(f"{c} {t}" for c, t in COLUMNS.items())
    con.execute(f"""CREATE TABLE IF NOT EXISTS raw_statcast ({cols},
                    PRIMARY KEY (game_pk, at_bat_number, pitch_number))""")
    con.execute("CREATE TABLE IF NOT EXISTS pull_log (month_start DATE PRIMARY KEY)")
    done = {r[0] for r in con.execute("SELECT month_start FROM pull_log").fetchall()}

    today = dt.date.today()
    todo = [m for m in months(FIRST_SEASON, today) if m[0] not in done]
    if not todo:
        print("Data is up to date.")
        return

    from pybaseball import cache, statcast
    cache.enable()
    for start, end in todo:
        print(f"{start:%Y-%m}: downloading...", flush=True)
        df = statcast(start.isoformat(), end.isoformat(), verbose=False)
        if df is not None and len(df):
            df = df[df["game_type"] == "R"][list(COLUMNS)]
            df = df.drop_duplicates(["game_pk", "at_bat_number", "pitch_number"])
            con.register("chunk", df)
            con.execute("INSERT OR IGNORE INTO raw_statcast SELECT * FROM chunk")
            con.unregister("chunk")
        # The current month is still in progress, so don't mark it finished.
        if end < today:
            con.execute("INSERT INTO pull_log VALUES (?)", [start])
    total = con.execute("SELECT COUNT(*) FROM raw_statcast").fetchone()[0]
    print(f"raw_statcast has {total:,} pitches.")


# ------------------------------------------------------- 2. batted balls + checks

BATTED_BALLS_SQL = f"""
CREATE OR REPLACE TABLE batted_balls AS
SELECT game_pk, game_date, game_year AS season, home_team,
       home_team = 'BOS' AS is_fenway,
       batter, pitcher,
       CASE WHEN inning_topbot = 'Top' THEN away_team ELSE home_team END AS batting_team,
       CASE WHEN inning_topbot = 'Top' THEN home_team ELSE away_team END AS pitching_team,
       stand, launch_speed, launch_angle,
       -- Direction from home plate in degrees: negative = left field. Not flipped
       -- for handedness, because the walls care which field, not pull vs. oppo.
       DEGREES(ATAN2(hc_x - 125.42, 198.27 - hc_y)) AS spray_angle,
       woba_value
FROM raw_statcast
WHERE type = 'X'
  AND game_year >= {FIRST_SEASON}
  AND launch_speed BETWEEN 0 AND 125
  AND launch_angle BETWEEN -90 AND 90
  AND woba_value BETWEEN 0 AND 2.2;
"""

# Each check returns a number. The lambda says whether that number is OK.
CHECKS = [
    ("No duplicate pitches",
     """SELECT COUNT(*) FROM (
            SELECT game_pk, at_bat_number, pitch_number FROM raw_statcast
            GROUP BY game_pk, at_bat_number, pitch_number HAVING COUNT(*) > 1) d""",
     lambda n: n == 0),
    ("Exit velocity present and between 0 and 125 mph",
     """SELECT COUNT(*) FROM batted_balls
        WHERE launch_speed IS NULL OR launch_speed NOT BETWEEN 0 AND 125""",
     lambda n: n == 0),
    ("Launch angle present and between -90 and 90",
     """SELECT COUNT(*) FROM batted_balls
        WHERE launch_angle IS NULL OR launch_angle NOT BETWEEN -90 AND 90""",
     lambda n: n == 0),
    ("wOBA value present and between 0 and 2.2",
     """SELECT COUNT(*) FROM batted_balls
        WHERE woba_value IS NULL OR woba_value NOT BETWEEN 0 AND 2.2""",
     lambda n: n == 0),
    ("Fenway share of batted balls is 2% to 5% (1 park of 30 = 3.3%)",
     "SELECT 100.0 * AVG(CASE WHEN is_fenway THEN 1 ELSE 0 END) FROM batted_balls",
     lambda pct: 2 <= pct <= 5),
    (f"No batted balls before {FIRST_SEASON}",
     f"SELECT COUNT(*) FROM batted_balls WHERE season < {FIRST_SEASON}",
     lambda n: n == 0),
    ("Every season has at least 100,000 batted balls",
     "SELECT MIN(n) FROM (SELECT season, COUNT(*) AS n FROM batted_balls GROUP BY season) s",
     lambda n: n >= 100_000),
]


def validate(con):
    ok = True
    for name, sql, good in CHECKS:
        value = con.execute(sql).fetchone()[0]
        passed = value is not None and good(value)
        ok &= passed
        shown = f"{value:,.2f}" if isinstance(value, float) else value
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}  (got {shown})")
    return ok


# ------------------------------------------------------ 3. Fenway effect by bucket

# Buckets: 5 mph exit velo (115+ pooled), 5 degree launch angle, 15 degree
# direction slices from the left-field line to the right-field line.
BINNED_SQL = """
CREATE OR REPLACE TEMP TABLE binned AS
SELECT *,
    LEAST(FLOOR(launch_speed / 5), 23)::INTEGER AS ev_bin,
    FLOOR(launch_angle / 5)::INTEGER AS la_bin,
    CASE WHEN spray_angle IS NULL THEN -99
         ELSE LEAST(GREATEST(FLOOR(spray_angle / 15), -3), 2)::INTEGER END AS spray_bin
FROM batted_balls;
"""


def effect_sql(table, where="TRUE"):
    """Fenway's wOBA in each bucket, shrunk toward the road rate, minus road."""
    return f"""
    CREATE OR REPLACE TEMP TABLE {table} AS
    WITH counts AS (
        SELECT ev_bin, la_bin, spray_bin,
               COUNT(*) FILTER (WHERE is_fenway) AS n_fenway,
               COUNT(*) FILTER (WHERE NOT is_fenway) AS n_road,
               COALESCE(SUM(woba_value) FILTER (WHERE is_fenway), 0) AS fenway_woba_sum,
               AVG(woba_value) FILTER (WHERE NOT is_fenway) AS road_woba
        FROM binned WHERE {where} GROUP BY ALL)
    SELECT *,
           CASE WHEN road_woba IS NULL THEN 0
                ELSE (fenway_woba_sum + {SHRINK} * road_woba) / (n_fenway + {SHRINK})
                     - road_woba END AS effect
    FROM counts;
    """


# ------------------------------------------------------------ 4. player scores

def fit_sql(table, player_col, team_col, score):
    # Runs per season = effect per ball x balls per season x half the games at
    # home / wOBA scale. Only players active in the latest season.
    extra = ""
    if player_col == "batter":
        extra = """,
           COUNT(*) FILTER (WHERE b.is_fenway) AS fenway_batted_balls,
           AVG(b.woba_value) FILTER (WHERE b.is_fenway)
             - AVG(b.woba_value) FILTER (WHERE NOT b.is_fenway) AS actual_gap"""
    return f"""
    CREATE OR REPLACE TEMP TABLE {table} AS
    WITH per AS (
        SELECT b.{player_col} AS player_id,
               COUNT(*) AS batted_balls,
               COUNT(DISTINCT b.season) AS seasons,
               MAX(b.season) AS last_season,
               ARG_MAX(b.{team_col}, b.game_date) AS latest_team,
               AVG(e.effect) AS {score}_per_bb{extra}
        FROM binned b JOIN park_effect e USING (ev_bin, la_bin, spray_bin)
        GROUP BY b.{player_col})
    SELECT *,
           {score}_per_bb * (batted_balls / seasons) * 0.5 / {WOBA_SCALE}
             AS {score}_runs_per_season
    FROM per
    WHERE batted_balls >= {MIN_BATTED_BALLS}
      AND last_season = (SELECT MAX(season) FROM batted_balls);
    """


def add_names(con):
    """Make a players table (id -> name) using pybaseball's ID lookup."""
    ids = [r[0] for r in con.execute(
        "SELECT player_id FROM hitter_fit UNION SELECT player_id FROM pitcher_fit"
    ).fetchall()]
    con.execute("CREATE OR REPLACE TEMP TABLE players (player_id INTEGER, name VARCHAR)")
    if not ids:
        return
    from pybaseball import playerid_reverse_lookup
    names = playerid_reverse_lookup([int(i) for i in ids], key_type="mlbam")
    # pybaseball returns lowercase names, so title-casing is the best available.
    names["name"] = names["name_first"].str.title() + " " + names["name_last"].str.title()
    players = (names[["key_mlbam", "name"]]
               .rename(columns={"key_mlbam": "player_id"})
               .drop_duplicates("player_id"))
    con.register("players_df", players)
    con.execute("INSERT INTO players SELECT * FROM players_df")
    con.unregister("players_df")


LEADERBOARDS = {
    "Hitters who benefit most from Fenway (not on the Red Sox)": """
        SELECT COALESCE(p.name, CAST(h.player_id AS VARCHAR)),
               h.latest_team, ROUND(h.boost_runs_per_season, 1)
        FROM hitter_fit h LEFT JOIN players p USING (player_id)
        WHERE h.latest_team IS DISTINCT FROM 'BOS'
        ORDER BY h.boost_runs_per_season DESC
        LIMIT 10""",

    "Pitchers Fenway hurts least (not on the Red Sox)": """
        SELECT COALESCE(p.name, CAST(f.player_id AS VARCHAR)),
               f.latest_team, ROUND(-f.penalty_runs_per_season, 1)
        FROM pitcher_fit f LEFT JOIN players p USING (player_id)
        WHERE f.latest_team IS DISTINCT FROM 'BOS'
        ORDER BY f.penalty_runs_per_season ASC
        LIMIT 10""",
}


def print_leaderboards(con):
    for title, sql in LEADERBOARDS.items():
        print(f"\n{title}")
        for name, team, runs in con.execute(sql).fetchall():
            print(f"  {name} ({team}): {runs:+.1f} runs/season")


# ------------------------------------------------- 5. what helps / hurts + tests

PROFILE_DIMENSIONS = [
    # (bin column, label, bin width, unit)
    ("spray_bin", "Direction (negative = left field)", 15, "deg"),
    ("la_bin", "Launch angle", 5, "deg"),
    ("ev_bin", "Exit velocity", 5, "mph"),
]


def print_park_profile(con, min_fenway_balls=200):
    """Extra wOBA per batted ball at Fenway vs. the same contact elsewhere.

    The road rate is weighted by Fenway's own mix of the other two dimensions,
    so each row compares like with like.
    """
    print("\nWhat helps and hurts at Fenway (extra wOBA per batted ball)")
    for col, label, width, unit in PROFILE_DIMENSIONS:
        print(f"\n  {label}")
        rows = con.execute(f"""
            SELECT {col}, SUM(n_fenway),
                   SUM(fenway_woba_sum) / SUM(n_fenway),
                   SUM(road_woba * n_fenway) / SUM(n_fenway)
            FROM park_effect
            WHERE road_woba IS NOT NULL AND n_fenway > 0 AND {col} <> -99
            GROUP BY 1 HAVING SUM(n_fenway) >= ? ORDER BY 1""",
            [min_fenway_balls]).fetchall()
        for b, n, fen, road in rows:
            gap = fen - road
            bar = ("+" if gap > 0 else "-") * min(round(abs(gap) * 200), 20)
            print(f"    {b * width:+4d} to {b * width + width:+4d} {unit}:"
                  f" {gap:+.3f}  {bar:<20} ({n:,} Fenway balls)")


def pearson(pairs):
    if len(pairs) < 3:
        return None
    xs, ys = zip(*pairs)
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    cov = sum((x - mx) * (y - my) for x, y in pairs)
    vx = sum((x - mx) ** 2 for x in xs) ** 0.5
    vy = sum((y - my) ** 2 for y in ys) ** 0.5
    return None if vx == 0 or vy == 0 else cov / (vx * vy)


def backtest(con):
    """In-sample: predicted boost vs. each hitter's real Fenway-minus-road gap."""
    rows = con.execute("""
        SELECT boost_per_bb, actual_gap FROM hitter_fit
        WHERE fenway_batted_balls >= 150 AND actual_gap IS NOT NULL""").fetchall()
    r = pearson(rows)
    print("\nBacktest (in-sample):")
    if r is None:
        print("  Not enough hitters with 150+ Fenway batted balls.")
    else:
        print(f"  {len(rows)} hitters, correlation = {r:.2f} (positive = right direction)")


def out_of_sample_test(con, last_train_season=2024):
    """Learn the Fenway effect from earlier seasons, test it on later ones."""
    train = f"season <= {last_train_season}"
    con.execute(effect_sql("effect_train", train))
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE test_fenway AS
        WITH road_test AS (
            SELECT ev_bin, la_bin, spray_bin, AVG(woba_value) AS road_woba
            FROM binned WHERE NOT ({train}) AND NOT is_fenway GROUP BY ALL)
        SELECT b.*, e.effect AS predicted, b.woba_value - r.road_woba AS actual
        FROM binned b
        JOIN road_test r USING (ev_bin, la_bin, spray_bin)
        JOIN effect_train e USING (ev_bin, la_bin, spray_bin)
        WHERE NOT ({train}) AND b.is_fenway""")

    print(f"\nOut-of-sample test: effect learned from {FIRST_SEASON}-{last_train_season},"
          f" tested on later Fenway batted balls")
    n, r = con.execute("SELECT COUNT(*), CORR(predicted, actual) FROM test_fenway").fetchone()
    print(f"  {n:,} balls, correlation = {r:.3f}. Grouped by predicted effect:")
    for q, pred, act, k in con.execute("""
        SELECT q, AVG(predicted), AVG(actual), COUNT(*) FROM (
            SELECT NTILE(5) OVER (ORDER BY predicted) AS q, predicted, actual
            FROM test_fenway) GROUP BY q ORDER BY q""").fetchall():
        print(f"    quintile {q}: predicted {pred:+.3f}   actual {act:+.3f}   ({k:,} balls)")


# ------------------------------------------------------------------------ main

def main():
    con = duckdb.connect(str(DB))

    print("Step 1: download")
    pull_data(con)

    print("\nStep 2: data checks")
    con.execute(BATTED_BALLS_SQL)
    if not validate(con):
        sys.exit("Validation failed; fix the data before trusting the results.")

    con.execute(BINNED_SQL)
    con.execute(effect_sql("park_effect"))
    con.execute(fit_sql("hitter_fit", "batter", "batting_team", "boost"))
    con.execute(fit_sql("pitcher_fit", "pitcher", "pitching_team", "penalty"))
    add_names(con)

    print_leaderboards(con)
    print_park_profile(con)
    backtest(con)
    out_of_sample_test(con)


if __name__ == "__main__":
    main()

# Boston-Red-Sox
