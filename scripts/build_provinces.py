"""Build src/quake_lake/provinces.csv: one reference point per regency, labelled with its province.

Source: OCHA's Indonesia administrative boundaries (COD-AB, from BPS and BIG), the
attribute workbook, sheet ``idn_admin2``: 522 regencies, cities and large lakes, each
with a centre point. License CC BY-IGO 3.0, credited in the README. The workbook still
has the 34 provinces of 2020, so this script applies the 2022 split of Papua and Papua
Barat into six provinces (UU 14, 15, 16 and 29 of 2022), regency by regency; the
mapping matches Wikidata's ``located in`` statements (checked on 2026-11-22).

Run it only to refresh the table (needs network and openpyxl):

    uv run --with openpyxl python scripts/build_provinces.py [path/to/idn_admin_boundaries.xlsx]
"""

from __future__ import annotations

import csv
import sys
import urllib.request
from pathlib import Path

import openpyxl

URL = (
    "https://data.humdata.org/dataset/84a1d98a-790b-4d66-9d14-bbfa48500802/resource/"
    "ba60b4d3-fd55-4fcf-b1bd-adf7e4bdba90/download/idn_admin_boundaries.xlsx"
)
OUT = Path(__file__).resolve().parents[1] / "src" / "quake_lake" / "provinces.csv"

# BPS province code (the workbook's adm1_pcode) -> ISO 3166-2 code and name.
PROVINCES = {
    "ID11": ("ID-AC", "Aceh"),
    "ID12": ("ID-SU", "Sumatera Utara"),
    "ID13": ("ID-SB", "Sumatera Barat"),
    "ID14": ("ID-RI", "Riau"),
    "ID15": ("ID-JA", "Jambi"),
    "ID16": ("ID-SS", "Sumatera Selatan"),
    "ID17": ("ID-BE", "Bengkulu"),
    "ID18": ("ID-LA", "Lampung"),
    "ID19": ("ID-BB", "Kepulauan Bangka Belitung"),
    "ID21": ("ID-KR", "Kepulauan Riau"),
    "ID31": ("ID-JK", "DKI Jakarta"),
    "ID32": ("ID-JB", "Jawa Barat"),
    "ID33": ("ID-JT", "Jawa Tengah"),
    "ID34": ("ID-YO", "DI Yogyakarta"),
    "ID35": ("ID-JI", "Jawa Timur"),
    "ID36": ("ID-BT", "Banten"),
    "ID51": ("ID-BA", "Bali"),
    "ID52": ("ID-NB", "Nusa Tenggara Barat"),
    "ID53": ("ID-NT", "Nusa Tenggara Timur"),
    "ID61": ("ID-KB", "Kalimantan Barat"),
    "ID62": ("ID-KT", "Kalimantan Tengah"),
    "ID63": ("ID-KS", "Kalimantan Selatan"),
    "ID64": ("ID-KI", "Kalimantan Timur"),
    "ID65": ("ID-KU", "Kalimantan Utara"),
    "ID71": ("ID-SA", "Sulawesi Utara"),
    "ID72": ("ID-ST", "Sulawesi Tengah"),
    "ID73": ("ID-SN", "Sulawesi Selatan"),
    "ID74": ("ID-SG", "Sulawesi Tenggara"),
    "ID75": ("ID-GO", "Gorontalo"),
    "ID76": ("ID-SR", "Sulawesi Barat"),
    "ID81": ("ID-MA", "Maluku"),
    "ID82": ("ID-MU", "Maluku Utara"),
    "ID91": ("ID-PB", "Papua Barat"),
    "ID94": ("ID-PA", "Papua"),
}
NEW_2022 = {
    "ID-PS": ("Papua Selatan", ["Merauke", "Boven Digoel", "Mappi", "Asmat"]),
    "ID-PT": (
        "Papua Tengah",
        ["Nabire", "Puncak Jaya", "Paniai", "Mimika", "Puncak", "Dogiyai", "Intan Jaya", "Deiyai"],
    ),
    "ID-PE": (
        "Papua Pegunungan",
        [
            "Jayawijaya",
            "Pegunungan Bintang",
            "Yahukimo",
            "Tolikara",
            "Mamberamo Tengah",
            "Yalimo",
            "Lanny Jaya",
            "Nduga",
        ],
    ),
    "ID-PD": (
        "Papua Barat Daya",
        ["Kota Sorong", "Sorong", "Sorong Selatan", "Raja Ampat", "Tambrauw", "Maybrat"],
    ),
}


def main(argv: list[str]) -> int:
    xlsx = Path(argv[0]) if argv else Path("idn_admin_boundaries.xlsx")
    if not xlsx.exists():
        print(f"downloading {URL} -> {xlsx}")
        urllib.request.urlretrieve(URL, xlsx)
    moved = {
        (parent, regency): (code, name)
        for code, (name, regencies) in NEW_2022.items()
        for parent in ("ID91", "ID94")
        for regency in regencies
    }
    sheet = openpyxl.load_workbook(xlsx, read_only=True)["idn_admin2"]
    rows = sheet.iter_rows(values_only=True)
    col = {name: i for i, name in enumerate(next(rows))}
    out = []
    for r in rows:
        parent, regency = r[col["adm1_pcode"]], r[col["adm2_name"]]
        code, name = moved.get((parent, regency), PROVINCES[parent])
        lat, lon = float(r[col["center_lat"]]), float(r[col["center_lon"]])
        out.append((code, name, regency, round(lat, 4), round(lon, 4)))
    # A renamed regency would silently stay in its old province.
    lost = {r for _, rs in NEW_2022.values() for r in rs} - {o[2] for o in out}
    if lost:
        raise SystemExit(f"regencies of the 2022 split not in the workbook: {sorted(lost)}")
    out.sort()
    with OUT.open("w", newline="\n", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["province_code", "province", "regency", "latitude", "longitude"])
        w.writerows(out)
    print(f"{len(out)} reference points, {len({o[0] for o in out})} provinces -> {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
