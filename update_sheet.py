import os
import requests
import pandas as pd
import geopandas as gpd
import gspread
import json
import time
from shapely.geometry import shape, box, mapping
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone
import numpy as np

API_KEY = "0f883c77-4038-4107-b35a-5be8e736fe5a"
SPREADSHEET_ID = "1UW3uOFcLr4AQFBp_VMbEXk37_Vb5DekHU-_9QSkskCo"
LOG_SHEET_NAME = "Log_Update"

# Dataset GFW yang di-query.
#   "gfw_integrated_dist_alerts" = Global integrated disturbance alerts
#       (GLAD-L + GLAD-S2 + RADD + DIST-ALERT) -> sama dengan layer di website
#   "gfw_integrated_alerts"      = Integrated deforestation alerts (koleksi lama,
#       GLAD-L + GLAD-S2 + RADD saja)
GFW_DATASET = "gfw_integrated_dist_alerts"

AOI_PATH = "data/aoi_v26.json"
DESA_PATH = "data/Desa.json"
PEMILIK_PATH = "data/penggarap_v26.json"
BLOK_PATH = "data/blok_v26.json"

LULC_URL = "https://drive.google.com/uc?export=download&id=1v02RLW8-iDjfsXBjcv4ukaFwjKXYVPNl"
LULC_PATH = "data/lulc_v26.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

START_DATE = "2026-08-01"

# GFW membatasi ukuran satu respons (sekitar 6 MB). Kueri dipecah per bulan;
# bila masih terlalu besar, rentang tanggal dibelah dua, lalu area dibelah dua.
MAX_AREA_SPLITS = 6

# Jumlah baris per permintaan saat menulis ke Google Sheets.
SHEET_CHUNK_ROWS = 5000


def load_aoi_geometry(aoi_path):
    with open(aoi_path, "r") as f:
        aoi_geojson = json.load(f)

    feature = aoi_geojson["features"][0]
    geom_dict = feature["geometry"]
    geom_shape = shape(geom_dict)

    print(f"AOI dimuat: {aoi_path} | tipe: {geom_dict['type']}")

    return geom_shape, geom_dict


def download_lulc_if_needed():
    os.makedirs("data", exist_ok=True)

    if not os.path.exists(LULC_PATH):
        print("Downloading LULC data dari Google Drive...")

        r = requests.get(LULC_URL)

        if r.status_code == 200:
            with open(LULC_PATH, "wb") as f:
                f.write(r.content)

            print("LULC berhasil didownload.")
        else:
            raise Exception(f"Gagal download LULC. Status code: {r.status_code}")

    else:
        print("LULC sudah tersedia lokal.")


def _month_ranges(start, end):
    """Pecah rentang tanggal menjadi potongan per bulan kalender."""
    ranges = []
    cur = start
    while cur <= end:
        nxt = (cur.replace(day=1) + timedelta(days=32)).replace(day=1)
        ranges.append((cur, min(nxt - timedelta(days=1), end)))
        cur = nxt
    return ranges


def _polygon_parts(geom):
    """Ambil hanya bagian poligon dari hasil potongan geometri."""
    if geom.is_empty:
        return None
    if geom.geom_type in ("Polygon", "MultiPolygon"):
        return geom
    if geom.geom_type == "GeometryCollection":
        polys = [g for g in geom.geoms if g.geom_type in ("Polygon", "MultiPolygon")]
        if polys:
            merged = polys[0]
            for p in polys[1:]:
                merged = merged.union(p)
            return merged
    return None


def _split_area(geom):
    """Belah geometri menjadi dua di sisi terpanjang kotak pembatasnya."""
    minx, miny, maxx, maxy = geom.bounds
    if (maxx - minx) >= (maxy - miny):
        mid = (minx + maxx) / 2
        halves = [box(minx, miny, mid, maxy), box(mid, miny, maxx, maxy)]
    else:
        mid = (miny + maxy) / 2
        halves = [box(minx, miny, maxx, mid), box(minx, mid, maxx, maxy)]
    parts = [_polygon_parts(geom.intersection(h)) for h in halves]
    return [p for p in parts if p is not None]


def _query_gfw(geom, start, end, url, headers, date_field, conf_field):
    """Satu kueri ke GFW. Mengembalikan (rows, None) atau (None, pesan_error_5xx)."""

    sql = f"""
    SELECT
        longitude,
        latitude,
        {date_field},
        {conf_field},
        umd_glad_landsat_alerts__confidence,
        umd_glad_sentinel2_alerts__confidence,
        wur_radd_alerts__confidence
    FROM results
    WHERE {date_field} >= '{start:%Y-%m-%d}'
      AND {date_field} <= '{end:%Y-%m-%d}'
    """

    resp = requests.post(
        url,
        headers=headers,
        json={"geometry": mapping(geom), "sql": sql},
        timeout=300
    )

    if resp.status_code == 200:
        return resp.json().get("data", []), None

    if resp.status_code >= 500:
        # Respons terlalu besar / timeout: bisa dicoba lagi dengan potongan lebih kecil
        return None, f"{resp.status_code}: {resp.text[:200]}"

    # 4xx = kueri atau akses salah; memecah kueri tidak akan menolong
    raise RuntimeError(f"GFW menolak kueri [{resp.status_code}]: {resp.text[:300]}")


def _fetch_range(geom, start, end, url, headers, date_field, conf_field, area_depth=0):
    """Ambil satu rentang; pecah otomatis bila respons terlalu besar."""

    rows, err = _query_gfw(geom, start, end, url, headers, date_field, conf_field)

    if err is None:
        return rows

    if start < end:
        mid = start + (end - start) // 2
        print(f"  {start} s.d. {end} terlalu besar, dibelah per tanggal")
        return (
            _fetch_range(geom, start, mid, url, headers, date_field, conf_field, area_depth)
            + _fetch_range(geom, mid + timedelta(days=1), end, url, headers, date_field, conf_field, area_depth)
        )

    if area_depth >= MAX_AREA_SPLITS:
        raise RuntimeError(f"GFW tetap gagal untuk {start} setelah area dipecah {area_depth} kali: {err}")

    print(f"  {start} masih terlalu besar, area dibelah (tingkat {area_depth + 1})")
    rows = []
    for part in _split_area(geom):
        rows += _fetch_range(part, start, end, url, headers, date_field, conf_field, area_depth + 1)
    return rows


def fetch_gfw_data(aoi_shape):

    wib = timezone(timedelta(hours=7))

    today = datetime.now(wib).date()

    start_date = datetime.strptime(START_DATE, "%Y-%m-%d").date()

    date_field = f"{GFW_DATASET}__date"
    conf_field = f"{GFW_DATASET}__confidence"

    url = f"https://data-api.globalforestwatch.org/dataset/{GFW_DATASET}/latest/query"

    headers = {
        "x-api-key": API_KEY,
        "Content-Type": "application/json"
    }

    print(f"\nFetching {GFW_DATASET}: {start_date} → {today} ...")

    data = []

    for m_start, m_end in _month_ranges(start_date, today):
        rows = _fetch_range(aoi_shape, m_start, m_end, url, headers, date_field, conf_field)
        print(f"  {m_start:%Y-%m}: {len(rows)} baris")
        data += rows

    if not data:
        print("Tidak ada data dari GFW.")
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # Piksel di garis belah area bisa terambil dua kali
    df = df.drop_duplicates(subset=["longitude", "latitude"]).reset_index(drop=True)

    df.rename(columns={
        date_field: "Date",
        conf_field: "Conf_Integrated",
        "umd_glad_landsat_alerts__confidence": "Conf_GLADL",
        "umd_glad_sentinel2_alerts__confidence": "Conf_GLADS2",
        "wur_radd_alerts__confidence": "Conf_RADD",
    }, inplace=True)

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    print(f"[OK] {len(df)} baris | terbaru: {df['Date'].max().date()}")

    print("\nRingkasan confidence integrated:")
    print(df["Conf_Integrated"].value_counts().to_string())

    return df


def intersect_with_geojson(
    df,
    desa_path,
    pemilik_path,
    blok_path
):

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    )

    desa = gpd.read_file(desa_path)[["nama_kel", "geometry"]]
    pemilik = gpd.read_file(pemilik_path)[["Owner", "geometry"]]
    blok = gpd.read_file(blok_path)[["Blok", "geometry"]]

    download_lulc_if_needed()

    lulc = gpd.read_file(LULC_PATH)[["Class", "geometry"]]

    layers = [desa, pemilik, blok, lulc]

    for layer in layers:

        if layer.crs is None:
            layer.set_crs("EPSG:4326", inplace=True)

        else:
            layer.to_crs("EPSG:4326", inplace=True)

    gdf = gpd.sjoin(
        gdf,
        desa,
        how="left",
        predicate="within"
    ).rename(columns={"nama_kel": "Desa"})

    gdf.drop(columns=["index_right"], inplace=True, errors="ignore")

    gdf = gpd.sjoin(
        gdf,
        pemilik,
        how="left",
        predicate="within"
    )

    gdf.drop(columns=["index_right"], inplace=True, errors="ignore")

    gdf = gpd.sjoin(
        gdf,
        blok,
        how="left",
        predicate="within"
    )

    gdf.drop(columns=["index_right"], inplace=True, errors="ignore")

    gdf = gpd.sjoin(
        gdf,
        lulc,
        how="left",
        predicate="within"
    )

    gdf.drop(columns=["index_right"], inplace=True, errors="ignore")

    gdf.rename(columns={
        "Class": "Penutup_Lahan"
    }, inplace=True)

    gdf = gdf.drop(columns=["geometry"], errors="ignore")

    print(f"\nIntersect selesai: {len(gdf)} baris.")

    print(f"Tanggal maksimum: {pd.to_datetime(gdf['Date']).max().date()}")

    return gdf


def overwrite_google_sheet(df):

    creds = Credentials.from_service_account_file(
        "service_account.json",
        scopes=SCOPES
    )

    client = gspread.authorize(creds)

    sh = client.open_by_key(SPREADSHEET_ID)

    latest_year = pd.to_datetime(
        df["Date"],
        errors="coerce"
    ).dt.year.max()

    sheet_name = str(latest_year)

    keep_cols = [
        "latitude",
        "longitude",
        "Date",
        "Conf_Integrated",
        "Conf_GLADL",
        "Conf_GLADS2",
        "Conf_RADD",
        "Desa",
        "Owner",
        "Blok",
        "Penutup_Lahan"
    ]

    df = df[keep_cols].copy()

    df = df.replace([np.inf, -np.inf], np.nan).fillna("")

    df["Date"] = pd.to_datetime(
        df["Date"],
        errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    df = df.astype(str)

    try:
        sheet = sh.worksheet(sheet_name)

        sheet.clear()

        print(f"\nSheet '{sheet_name}' ditemukan dan dikosongkan.")

    except gspread.exceptions.WorksheetNotFound:

        sheet = sh.add_worksheet(
            title=sheet_name,
            rows=50000,
            cols=20
        )

        print(f"\nSheet '{sheet_name}' dibuat baru.")

    rows = [list(df.columns)] + df.values.tolist()

    # Tulis bertahap supaya satu permintaan tidak terlalu besar
    for i in range(0, len(rows), SHEET_CHUNK_ROWS):
        sheet.append_rows(
            rows[i:i + SHEET_CHUNK_ROWS],
            value_input_option="USER_ENTERED"
        )
        time.sleep(1.5)

    print(f"{len(df)} baris berhasil ditulis ke sheet '{sheet_name}'.")


def update_log(latest_date):

    creds = Credentials.from_service_account_file(
        "service_account.json",
        scopes=SCOPES
    )

    client = gspread.authorize(creds)

    try:
        log_sheet = client.open_by_key(
            SPREADSHEET_ID
        ).worksheet(LOG_SHEET_NAME)

    except gspread.exceptions.WorksheetNotFound:

        log_sheet = client.open_by_key(
            SPREADSHEET_ID
        ).add_worksheet(
            title=LOG_SHEET_NAME,
            rows=10,
            cols=3
        )

    wib = timezone(timedelta(hours=7))

    now_wib = datetime.now(wib).strftime("%Y-%m-%d %H:%M:%S")

    log_sheet.clear()

    log_sheet.append_rows([
        ["Note", "Last Update", "Latest Alert Date"],
        ["Update", now_wib, str(latest_date)]
    ], value_input_option="USER_ENTERED")

    print(f"\nLog diperbarui: {now_wib} | Latest alert: {latest_date}")


if __name__ == "__main__":

    aoi_shape, aoi_geom_dict = load_aoi_geometry(AOI_PATH)

    df = fetch_gfw_data(aoi_shape)

    if not df.empty:

        gdf = intersect_with_geojson(
            df,
            DESA_PATH,
            PEMILIK_PATH,
            BLOK_PATH
        )

        if not gdf.empty:

            overwrite_google_sheet(gdf)

            update_log(gdf["Date"].max())

        else:
            print("Tidak ada hasil intersect.")

    else:
        print("Tidak ada data dari GFW.")
