"""
Forest Alert Pipeline — dengan Disturbance Alerts
===================================================
Dataset tambahan:
  - umd_glad_landsat_alerts  → GLAD-L (Landsat-based disturbance, 30m resolusi)
  - umd_glad_sentinel2_alerts → GLAD-S2 (Sentinel-2, 10m resolusi)
  - wur_radd_alerts           → RADD (radar-based disturbance)

Semua dataset di atas adalah "disturbance alerts" dari GFW dan di-fuse
bersama INTEGRATED alert menjadi satu layer terpadu dengan confidence scoring.

Kolom output tambahan:
  - Disturbance_Type  : jenis disturbance (DEFORESTATION / DEGRADATION / UNKNOWN)
  - Alert_Type        : GLAD_L / GLAD_S2 / RADD / INTEGRATED
  - Disturbance_Date  : tanggal disturbance (bisa beda dari alert date)
"""

import os
import json
import logging
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
import gspread

from datetime import datetime, timedelta, timezone
from shapely.geometry import shape
from shapely.strtree import STRtree
from sklearn.cluster import DBSCAN
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
API_KEY              = os.getenv("GFW_API_KEY", "912b99d5-ecc2-47aa-86fe-1f986b9b070b")
SPREADSHEET_ID       = os.getenv("SPREADSHEET_ID", "1UW3uOFcLr4AQFBp_VMbEXk37_Vb5DekHU-_9QSkskCo")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json")

AOI_PATH     = "data/aoi.json"
DESA_PATH    = "data/Desa.json"
PEMILIK_PATH = "data/PemilikLahan.json"
BLOK_PATH    = "data/blok.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

START_DATE = "2025-01-01"

CLUSTER_EPS_METER   = 50
CLUSTER_MIN_SAMPLES = 1

BBOX_COORDS = [
    [110.15497, 0.67329],
    [110.38332, 0.67329],
    [110.38332, 0.91435],
    [110.15497, 0.91435],
    [110.15497, 0.67329],
]

# Mapping confidence integer GFW → label
CONF_MAP = {
    1: "low",
    2: "high",
    3: "very_high",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

GFW_BASE = "https://data-api.globalforestwatch.org/dataset"


# ─────────────────────────────────────────
# 1. FETCH
# ─────────────────────────────────────────
def _hit_api(dataset: str, sql: str) -> pd.DataFrame:
    url = f"{GFW_BASE}/{dataset}/latest/query"
    try:
        resp = requests.post(
            url,
            headers={"x-api-key": API_KEY},
            json={
                "geometry": {"type": "Polygon", "coordinates": [BBOX_COORDS]},
                "sql": sql,
            },
            timeout=90,
        )
        resp.raise_for_status()
        return pd.DataFrame(resp.json().get("data", []))
    except Exception as e:
        log.warning("API error [%s]: %s", dataset, e)
        return pd.DataFrame()


def fetch_all_alerts(start_date: str = START_DATE) -> pd.DataFrame:
    wib   = timezone(timedelta(hours=7))
    today = datetime.now(wib).strftime("%Y-%m-%d")
    log.info("=== FETCH (start=%s, end=%s) ===", start_date, today)

    frames = []

    # ── 1. Integrated Alert (GLAD-L + GLAD-S2 + RADD fused oleh GFW) ──
    df = _hit_api(
        "gfw_integrated_alerts",
        f"""
        SELECT longitude, latitude,
               gfw_integrated_alerts__date       AS date,
               gfw_integrated_alerts__confidence AS confidence_raw
        FROM results
        WHERE gfw_integrated_alerts__date >= '{start_date}'
          AND gfw_integrated_alerts__date <= '{today}'
        """,
    )
    if not df.empty:
        df["source"]           = "INTEGRATED"
        df["disturbance_type"] = "UNKNOWN"
        frames.append(df)

    # ── 2. GLAD Landsat (disturbance 30m — deforestation & degradation) ──
    df = _hit_api(
        "umd_glad_landsat_alerts",
        f"""
        SELECT longitude, latitude,
               umd_glad_landsat_alerts__date       AS date,
               umd_glad_landsat_alerts__confidence AS confidence_raw,
               is__umd_regional_primary_forest_2001 AS is_primary_forest
        FROM results
        WHERE umd_glad_landsat_alerts__date >= '{start_date}'
          AND umd_glad_landsat_alerts__date <= '{today}'
        """,
    )
    if not df.empty:
        df["source"]           = "GLAD_L"
        # Disturbance di primary forest → DEFORESTATION, sisanya DEGRADATION
        df["disturbance_type"] = np.where(
            df.get("is_primary_forest", False) == True,
            "DEFORESTATION",
            "DEGRADATION",
        )
        frames.append(df)

    # ── 3. GLAD Sentinel-2 (disturbance 10m — lebih detail) ──
    df = _hit_api(
        "umd_glad_sentinel2_alerts",
        f"""
        SELECT longitude, latitude,
               umd_glad_sentinel2_alerts__date       AS date,
               umd_glad_sentinel2_alerts__confidence AS confidence_raw
        FROM results
        WHERE umd_glad_sentinel2_alerts__date >= '{start_date}'
          AND umd_glad_sentinel2_alerts__date <= '{today}'
        """,
    )
    if not df.empty:
        df["source"]           = "GLAD_S2"
        df["disturbance_type"] = "UNKNOWN"
        frames.append(df)

    # ── 4. RADD (radar-based, sensitif asap/awan) ──
    df = _hit_api(
        "wur_radd_alerts",
        f"""
        SELECT longitude, latitude,
               wur_radd_alerts__date       AS date,
               wur_radd_alerts__confidence AS confidence_raw
        FROM results
        WHERE wur_radd_alerts__date >= '{start_date}'
          AND wur_radd_alerts__date <= '{today}'
        """,
    )
    if not df.empty:
        df["source"]           = "RADD"
        df["disturbance_type"] = "UNKNOWN"
        frames.append(df)

    if not frames:
        log.error("Semua API gagal / tidak ada data.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined["date"]       = pd.to_datetime(combined["date"], errors="coerce")
    combined["confidence_raw"] = pd.to_numeric(
        combined.get("confidence_raw", np.nan), errors="coerce"
    ).fillna(1).astype(int)

    combined[["latitude", "longitude"]] = combined[["latitude", "longitude"]].apply(
        pd.to_numeric, errors="coerce"
    )
    combined = combined.dropna(subset=["latitude", "longitude", "date"])

    log.info("Total raw alerts: %d", len(combined))
    return combined


# ─────────────────────────────────────────
# 2. FUSION  (STRtree — spatial index)
# ─────────────────────────────────────────
def _label_confidence(n_sources: int, max_conf_raw: int) -> str:
    """
    Gabungkan jumlah source dan confidence raw tertinggi untuk scoring final.
    """
    if n_sources >= 3 or max_conf_raw >= 3:
        return "very_high"
    elif n_sources == 2 or max_conf_raw == 2:
        return "high"
    return "low"


def fuse_sources(df: pd.DataFrame, radius_m: int = 50, days_window: int = 5) -> pd.DataFrame:
    log.info("=== FUSION (radius=%dm, window=±%dd) ===", radius_m, days_window)

    gdf = gpd.GeoDataFrame(
        df.copy(),
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326",
    ).to_crs(epsg=32749)

    gdf["lon_orig"] = df["longitude"].values
    gdf["lat_orig"] = df["latitude"].values
    gdf["date_ns"]  = gdf["date"].astype(np.int64)
    day_ns          = days_window * 86_400 * 1_000_000_000

    tree = STRtree(gdf.geometry)
    records = []

    for _, row in gdf.iterrows():
        buf           = row.geometry.buffer(radius_m)
        cand_idx      = tree.query(buf)
        time_mask     = abs(gdf.iloc[cand_idx]["date_ns"] - row["date_ns"]) <= day_ns
        nearby        = gdf.iloc[cand_idx[time_mask]]

        sources       = sorted(nearby["source"].unique().tolist())
        max_conf_raw  = int(nearby["confidence_raw"].max())
        conf_label    = _label_confidence(len(sources), max_conf_raw)

        # Ambil disturbance_type yang paling informatif (bukan UNKNOWN jika ada)
        dist_types    = nearby["disturbance_type"].unique().tolist()
        dist_type     = next(
            (t for t in dist_types if t != "UNKNOWN"), "UNKNOWN"
        )

        records.append({
            "latitude":          row["lat_orig"],
            "longitude":         row["lon_orig"],
            "Alert_Date":        row["date"],
            "Alert_Confidence":  conf_label,
            "Confidence_Raw":    max_conf_raw,
            "Source_Detail":     ",".join(sources),
            "Source_Count":      len(sources),
            "Alert_Type":        row["source"],
            "Disturbance_Type":  dist_type,
        })

    df_out = (
        pd.DataFrame(records)
        .drop_duplicates(subset=["latitude", "longitude", "Alert_Date", "Alert_Type"])
        .reset_index(drop=True)
    )

    log.info("After fusion: %d", len(df_out))
    return df_out


# ─────────────────────────────────────────
# 3. AOI CLIP
# ─────────────────────────────────────────
def clip_with_aoi(df: pd.DataFrame) -> pd.DataFrame:
    log.info("=== AOI CLIP ===")

    with open(AOI_PATH) as f:
        aoi_geom = shape(json.load(f)["features"][0]["geometry"])

    aoi_gdf = gpd.GeoDataFrame(geometry=[aoi_geom], crs="EPSG:4326")
    gdf     = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326",
    )

    clipped = gpd.clip(gdf, aoi_gdf).drop(columns=["geometry"]).reset_index(drop=True)
    log.info("After AOI clip: %d", len(clipped))
    return clipped


# ─────────────────────────────────────────
# 4. SPATIAL JOIN
# ─────────────────────────────────────────
def intersect_with_geojson(df: pd.DataFrame) -> gpd.GeoDataFrame:
    log.info("=== SPATIAL JOIN ===")

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326",
    )

    layers = [
        (DESA_PATH,    "nama_kel", "Desa"),
        (PEMILIK_PATH, "Owner",    "Owner"),
        (BLOK_PATH,    "Blok",     "Blok"),
    ]

    for path, src_col, dst_col in layers:
        try:
            layer = gpd.read_file(path)[[src_col, "geometry"]]
            gdf   = gpd.sjoin(gdf, layer, how="left", predicate="intersects")
            gdf   = gdf.drop(columns=["index_right"], errors="ignore")
            gdf   = gdf.rename(columns={src_col: dst_col})
        except Exception as e:
            log.warning("Gagal join layer %s: %s", path, e)
            gdf[dst_col] = "Unknown"

    for col in ["Owner", "Desa", "Blok"]:
        gdf[col] = gdf[col].fillna("Unknown")

    log.info("After spatial join: %d", len(gdf))
    return gdf


# ─────────────────────────────────────────
# 5. CLUSTER  (DBSCAN per Owner)
# ─────────────────────────────────────────
def cluster_points_by_owner(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    log.info("=== CLUSTER (eps=%dm) ===", CLUSTER_EPS_METER)

    gdf_utm  = gdf.to_crs(epsg=32749).copy()
    all_parts = []

    # Default values
    gdf_utm["Cluster_ID"]   = "NO_CLUSTER"
    gdf_utm["Cluster_X"]    = gdf_utm.geometry.x
    gdf_utm["Cluster_Y"]    = gdf_utm.geometry.y
    gdf_utm["Jumlah_Titik"] = 1

    for owner, group_idx in gdf_utm.groupby("Owner").groups.items():
        group = gdf_utm.loc[group_idx].copy()

        if len(group) == 0:
            all_parts.append(group)
            continue

        xy     = np.column_stack([group.geometry.x, group.geometry.y])
        labels = DBSCAN(
            eps=CLUSTER_EPS_METER,
            min_samples=CLUSTER_MIN_SAMPLES,
            algorithm="ball_tree",
            metric="euclidean",
        ).fit_predict(xy)

        group["_label"] = labels

        for lbl, sub in group.groupby("_label"):
            cid           = f"{owner}_C{lbl}" if lbl != -1 else f"{owner}_NOISE"
            centroid_utm  = sub.geometry.unary_union.centroid
            centroid_4326 = gpd.GeoSeries([centroid_utm], crs=32749).to_crs(4326).iloc[0]

            group.loc[sub.index, "Cluster_ID"]   = cid
            group.loc[sub.index, "Cluster_X"]    = centroid_4326.x
            group.loc[sub.index, "Cluster_Y"]    = centroid_4326.y
            group.loc[sub.index, "Jumlah_Titik"] = len(sub)

        all_parts.append(group.drop(columns=["_label"]))

    result = pd.concat(all_parts).to_crs(4326)
    log.info("After cluster: %d rows", len(result))
    return result


# ─────────────────────────────────────────
# 6. GOOGLE SHEET
# ─────────────────────────────────────────
OUTPUT_COLS = [
    "latitude", "longitude",
    "Alert_Date", "Alert_Confidence", "Confidence_Raw",
    "Alert_Type", "Disturbance_Type",
    "Source_Detail", "Source_Count",
    "Desa", "Owner", "Blok",
    "Cluster_ID", "Cluster_Y", "Cluster_X", "Jumlah_Titik",
]


def _write_year_sheet(client, df_year: pd.DataFrame, year: int):
    sheet_name = str(year)
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
        sheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        sheet = client.open_by_key(SPREADSHEET_ID).add_worksheet(
            title=sheet_name,
            rows=max(len(df_year) + 10, 1000),
            cols=len(OUTPUT_COLS) + 2,
        )

    existing_cols = [c for c in OUTPUT_COLS if c in df_year.columns]
    out  = df_year[existing_cols].fillna("").astype(str)
    data = [existing_cols] + out.values.tolist()
    sheet.append_rows(data, value_input_option="USER_ENTERED")
    log.info("  Sheet '%s' → %d baris", sheet_name, len(out))


def overwrite_google_sheet(gdf: gpd.GeoDataFrame):
    log.info("=== GOOGLE SHEET ===")

    creds  = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)

    df = gdf.drop(columns=["geometry"], errors="ignore").copy()
    df["Alert_Date"] = pd.to_datetime(df["Alert_Date"], errors="coerce")
    df = df.dropna(subset=["Alert_Date"])
    df["_year"]      = df["Alert_Date"].dt.year
    df["Alert_Date"] = df["Alert_Date"].dt.strftime("%Y-%m-%d")

    for year, group in df.groupby("_year"):
        _write_year_sheet(client, group.drop(columns=["_year"]), int(year))

    log.info("Selesai tulis ke Google Sheet (%d tahun)", df["_year"].nunique())


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    try:
        df_raw = fetch_all_alerts()
        if df_raw.empty:
            log.error("Tidak ada data dari API. Pipeline berhenti.")
            return

        df_fused = fuse_sources(df_raw)

        df_clipped = clip_with_aoi(df_fused)
        if df_clipped.empty:
            log.error("Tidak ada titik dalam AOI. Pipeline berhenti.")
            return

        gdf_joined    = intersect_with_geojson(df_clipped)
        gdf_clustered = cluster_points_by_owner(gdf_joined)
        if gdf_clustered.empty:
            log.error("Cluster kosong. Pipeline berhenti.")
            return

        overwrite_google_sheet(gdf_clustered)
        log.info("✅ Pipeline selesai.")

    except Exception as e:
        log.exception("Pipeline gagal: %s", e)
        raise


if __name__ == "__main__":
    main()
