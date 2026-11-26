"""The dashboard: gold's earthquakes on a map, by magnitude, by province and by month.

    make dashboard      (uv run streamlit run src/quake_lake/dashboard.py)

Reads `$GOLD_DB`, else lake/gold.duckdb, from the repository root, as dbt writes it.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import pydeck as pdk
import streamlit as st

# The fact and its province: every chart is computed from these rows, so one set of
# filters drives them all (about 27,000 rows, read in 0.04 s). dbt writes the fact before
# its tests run, so a failed build can leave rows they would refuse; such a row still
# counts: under its province code if dim_province lacks it, as unknown if it has none.
QUERY = """
select
    f.event_id, f.origin_time_utc, f.latitude, f.longitude, f.depth_km, f.depth_class,
    f.magnitude, coalesce(f.place, 'place unknown') as place, f.in_indonesia,
    coalesce(d.province, f.province_code, 'province unknown') as province
from fct_earthquakes as f
left join dim_province as d using (province_code)
order by f.magnitude nulls first  -- the largest are drawn last, on top
"""
# One blue, darker with depth (an ordinal ramp: steps 300, 450 and 650 of the dataviz
# reference palette's blue); the chart series is the same blue.
DEPTH_RGB = {"shallow": [109, 167, 236], "intermediate": [42, 120, 214], "deep": [16, 66, 129]}
SERIES = "#2a78d6"
MAX_MAGNITUDE = 10.0  # silver quarantines anything larger


@st.cache_data(show_spinner=False, max_entries=1)  # a rebuilt gold replaces the old
def load(path: str, version: int) -> pd.DataFrame:
    """Gold's earthquakes; `version` is the file's mtime, so a rebuilt gold loads again."""
    # Read-only and closed at once: dbt can't write gold while another process has it open.
    with duckdb.connect(path, read_only=True) as con:
        return con.sql(QUERY).df()


def leaderboard(quakes: pd.DataFrame) -> pd.DataFrame:
    """Provinces by number of earthquakes, with M5+ and the largest."""
    return (
        quakes.assign(m5_plus=quakes.magnitude >= 5)
        .groupby("province", as_index=False)
        .agg(
            earthquakes=("event_id", "size"),
            m5_plus=("m5_plus", "sum"),
            largest=("magnitude", "max"),
        )
        .sort_values(["earthquakes", "province"], ascending=[False, True], ignore_index=True)
    )


def monthly(quakes: pd.DataFrame) -> pd.DataFrame:
    """Earthquakes per UTC month, a quiet month as 0."""
    counts = quakes.set_index("origin_time_utc").resample("MS").size()
    return counts.rename_axis("month").reset_index(name="earthquakes")


def magnitudes(quakes: pd.DataFrame) -> pd.DataFrame:
    """Earthquakes per magnitude, to the nearest 0.1 (no magnitude: left out)."""
    counts = quakes.magnitude.dropna().round(1).value_counts().sort_index()
    return counts.rename_axis("magnitude").reset_index(name="earthquakes")


def main() -> None:
    st.set_page_config(page_title="quake-lake: Indonesian earthquakes", layout="wide")
    path = os.environ.get("GOLD_DB", "lake/gold.duckdb")
    try:
        quakes = load(path, Path(path).stat().st_mtime_ns)
    except (OSError, duckdb.Error) as e:
        st.error(
            f"Can't read gold at `{path}`: {e}\n\n"
            "Build it with `make gold`; if dbt is writing it right now, reload in a few seconds."
        )
        st.stop()
    if quakes.magnitude.isna().all():  # no rows, or none with a magnitude to filter on
        st.info(f"Gold at `{path}` has no earthquakes with a magnitude yet.")
        st.stop()

    with st.sidebar:
        st.header("Filters")
        first, last = (int(y) for y in quakes.origin_time_utc.dt.year.agg(["min", "max"]))
        if first < last:
            years = st.slider("Years (UTC)", first, last, (first, last))
        else:  # a slider needs two different ends
            years = (first, last)
            st.caption(f"Years (UTC): {first} only")
        lowest = float(quakes.magnitude.min())
        min_mag = st.slider(
            "Minimum magnitude", lowest, MAX_MAGNITUDE, lowest, step=0.1, format="%.1f"
        )
        abroad = st.toggle(
            "Include earthquakes USGS places in another country",
            help="USGS names the Philippines, Timor-Leste, Papua New Guinea, ... as the nearest "
            "country for about a quarter of the earthquakes in its box around Indonesia.",
        )

    shown = quakes[quakes.origin_time_utc.dt.year.between(*years)]
    if min_mag > lowest:  # at the lowest, keep the few with no magnitude too
        shown = shown[shown.magnitude >= min_mag]
    if not abroad:
        shown = shown[shown.in_indonesia]

    st.title("Indonesian earthquakes")
    newest = quakes.origin_time_utc.max()
    st.caption(
        f"{years[0]} to {years[1]}, M{min_mag:.1f} and up"
        f"{', with the ones USGS places abroad' if abroad else ''}. "
        f"Newest event in gold: {newest:%Y-%m-%d %H:%M} UTC. "
        "Data: USGS Earthquake Hazards Program and BMKG."
    )
    if shown.empty:
        st.info("No earthquake matches these filters.")
        return

    kpi = st.columns(4)
    kpi[0].metric("Earthquakes", f"{len(shown):,}")
    kpi[1].metric("M5 and up", f"{(shown.magnitude >= 5).sum():,}")
    kpi[2].metric("M6 and up", f"{(shown.magnitude >= 6).sum():,}")
    if shown.magnitude.notna().any():
        top = shown.loc[shown.magnitude.idxmax()]
        when = f"{top.origin_time_utc:%Y-%m-%d} UTC, {top.place}"
        kpi[3].metric("Largest", f"M{top.magnitude:.1f}", help=when)
    else:
        kpi[3].metric("Largest", "?", help="None of these earthquakes has a magnitude.")

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Where")
        # Only what the map draws: every rerun sends it to the browser, as indented JSON.
        mag = shown.magnitude.map(lambda m: "?" if pd.isna(m) else f"{m:.1f}")
        tip = "M" + mag + ", " + shown.origin_time_utc.dt.strftime("%Y-%m-%d %H:%M UTC")
        tip += "\n" + shown.place + "\n"
        tip += (shown.depth_km.round(1) + 0.0).astype(str) + " km deep"  # + 0.0: no "-0.0"
        points = pd.DataFrame(
            {
                "lon": shown.longitude.round(3),  # about 100 m
                "lat": shown.latitude.round(3),
                "r": (4000 * 2 ** (shown.magnitude.fillna(3) - 3)).round(),  # m, x2 per unit
                "color": shown.depth_class.map(DEPTH_RGB),
                "tip": tip,
            }
        )
        layer = pdk.Layer(
            "ScatterplotLayer",
            points,
            get_position=["lon", "lat"],
            get_fill_color="color",
            get_radius="r",
            radius_min_pixels=1,
            opacity=0.5,
            pickable=True,
        )
        st.pydeck_chart(
            pdk.Deck(
                layers=[layer],
                initial_view_state=pdk.ViewState(latitude=-2.5, longitude=118, zoom=2.9),
                tooltip={"text": "{tip}"},
                map_style=None,
            ),
            height=460,
        )
        st.caption(
            "Circle size grows with magnitude. Depth, light to dark: shallow (under 70 km), "
            "intermediate (70 to 300 km), deep (300 km or more)."
        )
    with right:
        st.subheader("By province")
        st.dataframe(
            leaderboard(shown),
            hide_index=True,
            height=460,
            column_config={
                "province": "Province",
                "earthquakes": st.column_config.NumberColumn("Earthquakes", format="%d"),
                "m5_plus": st.column_config.NumberColumn("M5+", format="%d"),
                "largest": st.column_config.NumberColumn("Largest", format="M%.1f"),
            },
        )
        st.caption("The province of the nearest regency reference point.")

    left, right = st.columns(2)
    with left:
        st.subheader("Per month")
        st.line_chart(
            monthly(shown), x="month", y="earthquakes", color=SERIES, x_label="", y_label=""
        )
        st.caption("Earthquakes per UTC month.")
    with right:
        st.subheader("By magnitude")
        st.bar_chart(magnitudes(shown), x="magnitude", y="earthquakes", color=SERIES, y_label="")
        st.caption(
            "Earthquakes per 0.1 of magnitude. Below the peak, the global catalogue misses "
            "more and more of the small ones, which are in fact the most common."
        )


if __name__ == "__main__":  # streamlit run executes this file as __main__
    main()
