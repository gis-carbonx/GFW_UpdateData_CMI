
import os
import json
import logging
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
import gspread

from datetime import datetime, timedelta, timezone
from shapely.geometry import shape, Point
from shapely.strtree import STRtree
from sklearn.cluster import DBSCAN
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
API_KEY        = os.getenv("GFW_API_KEY", "912b99d5-ecc2-47aa-86fe-1f986b9b070b")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1UW3uOFcLr4AQFBp_VMbEXk37_Vb5DekHU-_9QSkskCo")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json")

AOI_PATH     = "data/aoi.json"
DESA_PATH    = "data/Desa.json"
PEMILIK_PATH = "data/PemilikLahan.json"
BLOK_PATH    = "data/blok.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

START_DATE = "2025-01-01"

# DBSCAN: radius 50 meter, minimal 1 titik per cluster
CLUSTER_EPS_METER = 50
CLUSTER_MIN_SAMPLES = 1

# Bounding box AOI untuk query API (xmin, ymin, xmax, ymax)
BBOX_COORDS = [
    [110.15497, 0.67329],
    [110.38332, 0.67329],
    [110.38332, 0.91435],
    [110.15497, 0.91435],
    [110.15497, 0.67329],
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────
# 1. FETCH
# ─────────────────────────────────────────
def _hit_api(url: str, sql: str) -> pd.DataFrame:
    """POST ke GFW Data API, kembalikan DataFrame kosong jika gagal."""
    try:
        resp = requests.post(
            url,
            headers={"x-api-key": API_KEY},
            json={"geometry": {"type": "Polygon", "coordinates": [BBOX_COORDS]}, "sql": sql},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])
        return pd.DataFrame(data)
    except Exception as e:
        log.warning("API error [%s]: %s", url, e)
        return pd.DataFrame()


def fetch_all_alerts(start_date: str = START_DATE) -> pd.DataFrame:
    wib = timezone(timedelta(hours=7))
    today = datetime.now(wib).strftime("%Y-%m-%d")

    log.info("=== FETCH (start=%s, end=%s) ===", start_date, today)

    df_int = _hit_api(
        "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/latest/query",
        f"""
        SELECT longitude, latitude,
               gfw_integrated_alerts__date          AS date,
               gfw_integrated_alerts__confidence    AS confidence
        FROM results
        WHERE gfw_integrated_alerts__date >= '{start_date}'
          AND gfw_integrated_alerts__date <= '{today}'
        """,
    )
    df_int["source"] = "INTEGRATED"

    df_glad = _hit_api(
        "https://data-api.globalforestwatch.org/dataset/glad_alerts/latest/query",
        f"""
        SELECT longitude, latitude, alert_date AS date
        FROM results
        WHERE alert_date >= '{start_date}'
          AND alert_date <= '{today}'
        """,
    )
    df_glad["source"] = "GLAD"

    df_radd = _hit_api(
        "https://data-api.globalforestwatch.org/dataset/radd_alerts/latest/query",
        f"""
        SELECT longitude, latitude, alert_date AS date
        FROM results
        WHERE alert_date >= '{start_date}'
          AND alert_date <= '{today}'
        """,
    )
    df_radd["source"] = "RADD"

    df = pd.concat([df_int, df_glad, df_radd], ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude", "date"])
    df[["latitude", "longitude"]] = df[["latitude", "longitude"]].apply(pd.to_numeric, errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])

    log.info("Total raw alerts: %d", len(df))
    return df


# ─────────────────────────────────────────
# 2. FUSION  (STRtree — O(n log n))
# ─────────────────────────────────────────
def _label_confidence(n_sources: int) -> str:
    if n_sources >= 3:
        return "very_high"
    elif n_sources == 2:
        return "high"
    return "low"


def fuse_sources(df: pd.DataFrame, radius_m: int = 50, days_window: int = 5) -> pd.DataFrame:
    """
    Untuk setiap titik, cari titik lain dalam radius_m meter dan ±days_window hari.
    Catat berapa source berbeda yang overlap → confidence.
    Pakai STRtree agar tidak O(n²).
    Koordinat output tetap dalam EPSG:4326.
    """
    log.info("=== FUSION (radius=%dm, window=±%dd) ===", radius_m, days_window)

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326",
    ).to_crs(epsg=32749)

    # Simpan koordinat asli (WGS84) sebelum reproject
    gdf["lon_orig"] = df["longitude"].values
    gdf["lat_orig"] = df["latitude"].values
    gdf["date_num"] = gdf["date"].astype(np.int64)  # nanoseconds

    tree = STRtree(gdf.geometry)

    records = []
    day_ns = days_window * 86_400 * 1_000_000_000  # timedelta → nanoseconds

    for i, row in gdf.iterrows():
        buf = row.geometry.buffer(radius_m)
        candidate_idx = tree.query(buf)  # spatial candidates

        # Filter waktu
        mask = abs(gdf.iloc[candidate_idx]["date_num"] - row["date_num"]) <= day_ns
        nearby = gdf.iloc[candidate_idx[mask]]

        sources = sorted(nearby["source"].unique().tolist())
        conf = _label_confidence(len(sources))

        # Gunakan confidence asli dari Integrated jika tersedia
        orig_conf = row.get("confidence", None)

        records.append({
            "latitude":         row["lat_orig"],
            "longitude":        row["lon_orig"],
            "Integrated_Date":  row["date"],
            "Integrated_Alert": conf,
            "Confidence_Orig":  orig_conf if pd.notna(orig_conf) else "",
            "Source_Detail":    ",".join(sources),
            "Source_Count":     len(sources),
        })

    df_out = (
        pd.DataFrame(records)
        .drop_duplicates(subset=["latitude", "longitude", "Integrated_Date"])
        .reset_index(drop=True)
    )

    log.info("After fusion: %d", len(df_out))
    return df_out


# ─────────────────────────────────────────
# 3. AOI CLIP  (gpd.clip — vectorized)
# ─────────────────────────────────────────
def clip_with_aoi(df: pd.DataFrame) -> pd.DataFrame:
    log.info("=== AOI CLIP ===")

    with open(AOI_PATH) as f:
        aoi_geom = shape(json.load(f)["features"][0]["geometry"])

    aoi_gdf = gpd.GeoDataFrame(geometry=[aoi_geom], crs="EPSG:4326")

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326",
    )

    clipped = gpd.clip(gdf, aoi_gdf).drop(columns=["geometry"]).reset_index(drop=True)
    log.info("After AOI clip: %d", len(clipped))
    return clipped


# ─────────────────────────────────────────
# 4. SPATIAL JOIN  (drop index_right tiap step)
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
            gdf = gpd.sjoin(gdf, layer, how="left", predicate="intersects")
            # drop index_right agar sjoin berikutnya tidak konflik
            gdf = gdf.drop(columns=["index_right"], errors="ignore")
            gdf = gdf.rename(columns={src_col: dst_col})
        except Exception as e:
            log.warning("Gagal join layer %s: %s", path, e)
            gdf[dst_col] = "Unknown"

    gdf["Owner"] = gdf["Owner"].fillna("Unknown")
    gdf["Desa"]  = gdf["Desa"].fillna("Unknown")
    gdf["Blok"]  = gdf["Blok"].fillna("Unknown")

    log.info("After spatial join: %d", len(gdf))
    return gdf


# ─────────────────────────────────────────
# 5. CLUSTER  (DBSCAN per owner)
# ─────────────────────────────────────────
def cluster_points_by_owner(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    DBSCAN dalam proyeksi UTM (meter).
    Cluster dibuat per Owner (bukan per Owner+Tanggal agar hotspot
    lintas hari tetap tergabung secara spasial).
    Kolom Integrated_Date tetap disimpan per titik.
    """
    log.info("=== CLUSTER (eps=%dm) ===", CLUSTER_EPS_METER)

    gdf_utm = gdf.to_crs(epsg=32749).copy()
    coords   = np.column_stack([gdf_utm.geometry.x, gdf_utm.geometry.y])

    gdf_utm["Cluster_ID"]    = "NO_CLUSTER"
    gdf_utm["Cluster_X"]     = gdf_utm.geometry.centroid.to_crs(4326).x
    gdf_utm["Cluster_Y"]     = gdf_utm.geometry.centroid.to_crs(4326).y
    gdf_utm["Jumlah_Titik"]  = 1

    all_parts = []

    for owner, group_idx in gdf_utm.groupby("Owner").groups.items():
        group = gdf_utm.loc[group_idx].copy()

        if len(group) == 0:
            all_parts.append(group)
            continue

        xy = np.column_stack([group.geometry.x, group.geometry.y])
        labels = DBSCAN(
            eps=CLUSTER_EPS_METER,
            min_samples=CLUSTER_MIN_SAMPLES,
            algorithm="ball_tree",
            metric="euclidean",
        ).fit_predict(xy)

        group["_label"] = labels

        for lbl, sub in group.groupby("_label"):
            cid = f"{owner}_C{lbl}" if lbl != -1 else f"{owner}_NOISE"
            centroid_4326 = sub.geometry.unary_union.centroid
            centroid_pt   = gpd.GeoSeries([centroid_4326], crs=32749).to_crs(4326).iloc[0]

            group.loc[sub.index, "Cluster_ID"]   = cid
            group.loc[sub.index, "Cluster_X"]    = centroid_pt.x
            group.loc[sub.index, "Cluster_Y"]    = centroid_pt.y
            group.loc[sub.index, "Jumlah_Titik"] = len(sub)

        group = group.drop(columns=["_label"])
        all_parts.append(group)

    result = pd.concat(all_parts).to_crs(4326)
    log.info("After cluster: %d rows", len(result))
    return result


# ─────────────────────────────────────────
# 6. GOOGLE SHEET  (pisah per tahun)
# ─────────────────────────────────────────
OUTPUT_COLS = [
    "latitude", "longitude", "Integrated_Date", "Integrated_Alert",
    "Confidence_Orig", "Source_Detail", "Source_Count",
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
            title=sheet_name, rows=max(len(df_year) + 10, 1000), cols=20
        )

    existing_cols = [c for c in OUTPUT_COLS if c in df_year.columns]
    out = df_year[existing_cols].fillna("").astype(str)
    data = [existing_cols] + out.values.tolist()
    sheet.append_rows(data, value_input_option="USER_ENTERED")
    log.info("  Sheet '%s' → %d baris", sheet_name, len(out))


def overwrite_google_sheet(gdf: gpd.GeoDataFrame):
    log.info("=== GOOGLE SHEET ===")

    creds  = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)

    df = gdf.drop(columns=["geometry"], errors="ignore").copy()
    df["Integrated_Date"] = pd.to_datetime(df["Integrated_Date"], errors="coerce")
    df = df.dropna(subset=["Integrated_Date"])
    df["_year"] = df["Integrated_Date"].dt.year
    df["Integrated_Date"] = df["Integrated_Date"].dt.strftime("%Y-%m-%d")

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

        gdf_joined = intersect_with_geojson(df_clipped)
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
