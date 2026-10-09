"""
Bandingkan versi-versi harian sebuah dataset alert GFW untuk satu AOI, lalu
laporkan kapan piksel baru ditambahkan dan tanggal alert apa yang dibawanya.

Cara pakai:
    export GFW_API_KEY="..."          (Windows: set GFW_API_KEY=...)
    python gfw_version_diff.py

Keluaran (folder version_diff/):
    additions_by_version.csv  ringkasan per versi: jumlah piksel baru, rentang
                              tanggal alert, jumlah yang bertanggal > 30 hari
                              sebelum versi tersebut
    new_pixels.csv            setiap piksel baru: versi pertama muncul, tanggal
                              alert, jeda (hari), confidence per sistem
    raw/<versi>.csv           hasil mentah tiap versi
"""

import json
import os
import sys
import time

import pandas as pd
import requests

# "gfw_integrated_alerts" atau "gfw_integrated_dist_alerts"; bisa diatur lewat
# environment variable GFW_DATASET (dipakai oleh workflow GitHub Actions).
DATASET = os.environ.get("GFW_DATASET") or "gfw_integrated_alerts"
AOI_PATH = "data/aoi_v26.json"
START_DATE = "2025-01-01"
FIRST_VERSION = "v20260908"            # versi paling awal yang dibandingkan
OLD_THRESHOLD_DAYS = 30                # batas "bertanggal lama"
OUT_DIR = "version_diff"

BASE = "https://data-api.globalforestwatch.org/dataset"
API_KEY = os.environ.get("GFW_API_KEY")


def list_versions():
    r = requests.get(f"{BASE}/{DATASET}", timeout=60)
    r.raise_for_status()
    versions = sorted(r.json()["data"]["versions"])
    return [v for v in versions if v >= FIRST_VERSION]


def version_date(version):
    # "v20260920" atau "v20260920.1" -> 2026-09-20
    return pd.to_datetime(version[1:9], format="%Y%m%d")


def fetch_version(version, geometry):
    date_field = f"{DATASET}__date"
    conf_field = f"{DATASET}__confidence"
    end_date = version_date(version).strftime("%Y-%m-%d")

    sql = f"""
    SELECT longitude, latitude, {date_field}, {conf_field},
           umd_glad_landsat_alerts__confidence,
           umd_glad_sentinel2_alerts__confidence,
           wur_radd_alerts__confidence
    FROM results
    WHERE {date_field} >= '{START_DATE}' AND {date_field} <= '{end_date}'
    """

    resp = requests.post(
        f"{BASE}/{DATASET}/{version}/query",
        headers={"x-api-key": API_KEY, "Content-Type": "application/json"},
        json={"geometry": geometry, "sql": sql},
        timeout=300,
    )

    if resp.status_code != 200:
        print(f"  [{version}] GAGAL {resp.status_code}: {resp.text[:200]}")
        return None

    df = pd.DataFrame(resp.json().get("data", []))
    if df.empty:
        return pd.DataFrame({"key": pd.Series(dtype=str),
                             "alert_date": pd.Series(dtype="datetime64[ns]")})

    df = df.rename(columns={
        date_field: "alert_date",
        conf_field: "conf_integrated",
        "umd_glad_landsat_alerts__confidence": "conf_gladl",
        "umd_glad_sentinel2_alerts__confidence": "conf_glads2",
        "wur_radd_alerts__confidence": "conf_radd",
    })
    df["alert_date"] = pd.to_datetime(df["alert_date"], errors="coerce")
    df["key"] = (
        df["latitude"].round(6).astype(str) + "," + df["longitude"].round(6).astype(str)
    )
    return df.drop_duplicates("key")


def main():
    if not API_KEY:
        sys.exit("Set dulu environment variable GFW_API_KEY.")

    with open(AOI_PATH) as f:
        geometry = json.load(f)["features"][0]["geometry"]

    os.makedirs(os.path.join(OUT_DIR, "raw"), exist_ok=True)

    versions = list_versions()
    print(f"{len(versions)} versi: {versions[0]} .. {versions[-1]}")

    prev = None          # hasil versi sebelumnya yang berhasil diambil
    prev_version = None
    summary, new_rows = [], []

    for version in versions:
        df = fetch_version(version, geometry)
        time.sleep(1)
        if df is None:
            continue

        df.to_csv(os.path.join(OUT_DIR, "raw", f"{version}.csv"), index=False)

        if prev is not None:
            vdate = version_date(version)
            added = df[~df["key"].isin(prev["key"])].copy()
            removed = prev[~prev["key"].isin(df["key"])]

            # Tanggal alert berubah pada piksel yang sudah ada
            merged = df.merge(prev[["key", "alert_date"]], on="key", suffixes=("", "_prev"))
            date_changed = int((merged["alert_date"] != merged["alert_date_prev"]).sum())

            added["first_seen_version"] = version
            added["previous_version"] = prev_version
            added["lag_days"] = (vdate - added["alert_date"]).dt.days
            new_rows.append(added)

            summary.append({
                "version": version,
                "previous_version": prev_version,
                "total_pixels": len(df),
                "new_pixels": len(added),
                "removed_pixels": len(removed),
                "date_changed_pixels": date_changed,
                "new_min_alert_date": added["alert_date"].min().date() if len(added) else "",
                "new_max_alert_date": added["alert_date"].max().date() if len(added) else "",
                "new_median_lag_days": added["lag_days"].median() if len(added) else "",
                f"new_older_than_{OLD_THRESHOLD_DAYS}d": int((added["lag_days"] > OLD_THRESHOLD_DAYS).sum()),
            })
            print(f"  [{version}] total {len(df)} | baru {len(added)} | hilang {len(removed)}")
        else:
            print(f"  [{version}] total {len(df)} (versi dasar)")

        prev, prev_version = df, version

    if not summary:
        sys.exit("Kurang dari dua versi yang berhasil diambil; tidak ada yang bisa dibandingkan.")

    pd.DataFrame(summary).to_csv(os.path.join(OUT_DIR, "additions_by_version.csv"), index=False)

    cols = ["latitude", "longitude", "alert_date", "first_seen_version", "previous_version",
            "lag_days", "conf_integrated", "conf_gladl", "conf_glads2", "conf_radd"]
    new_pixels = pd.concat(new_rows, ignore_index=True)
    new_pixels = new_pixels[[c for c in cols if c in new_pixels.columns]]
    new_pixels.sort_values("lag_days", ascending=False).to_csv(
        os.path.join(OUT_DIR, "new_pixels.csv"), index=False
    )

    print(f"\nSelesai. Lihat folder '{OUT_DIR}/'.")


if __name__ == "__main__":
    main()
