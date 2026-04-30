import requests
import pandas as pd
import geopandas as gpd
import gspread
import json
from shapely.geometry import shape, Point
from shapely.ops import unary_union
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone
import numpy as np

API_KEY = "912b99d5-ecc2-47aa-86fe-1f986b9b070b"
SPREADSHEET_ID = "1UW3uOFcLr4AQFBp_VMbEXk37_Vb5DekHU-_9QSkskCo"
LOG_SHEET_NAME = "Log_Update"

AOI_PATH = "data/aoi.json"
DESA_PATH = "data/Desa.json"
PEMILIK_PATH = "data/PemilikLahan.json"
BLOK_PATH = "data/blok.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def load_aoi_geometry(aoi_path):
    """Baca aoi.json dan kembalikan (shapely_polygon, geojson_geometry_dict)."""
    with open(aoi_path, "r") as f:
        aoi_geojson = json.load(f)

    feature = aoi_geojson["features"][0]
    geom_dict = feature["geometry"]      
    geom_shape = shape(geom_dict)          

    print(f"AOI dimuat: {aoi_path} | tipe: {geom_dict['type']}")
    return geom_shape, geom_dict


ALERT_DATASETS = [
    {
        "name": "INTEGRATED",
        "dataset": "gfw_integrated_alerts",
        "date_field": "gfw_integrated_alerts__date",
        "conf_field": "gfw_integrated_alerts__confidence",
        "source_label": "INTEGRATED",
        "alert_type": "Deforestation"
    },
    {
        "name": "GLAD-L",
        "dataset": "umd_glad_landsat_alerts",
        "date_field": "umd_glad_landsat_alerts__date",
        "conf_field": "umd_glad_landsat_alerts__confidence",
        "source_label": "GLAD-L",
        "alert_type": "Deforestation"
    },
    {
        "name": "GLAD-S2",
        "dataset": "umd_glad_sentinel2_alerts",
        "date_field": "umd_glad_sentinel2_alerts__date",
        "conf_field": "umd_glad_sentinel2_alerts__confidence",
        "source_label": "GLAD-S2",
        "alert_type": "Deforestation"
    },
    {
        "name": "RADD",
        "dataset": "wur_radd_alerts",
        "date_field": "wur_radd_alerts__date",
        "conf_field": "wur_radd_alerts__confidence",
        "source_label": "RADD",
        "alert_type": "Deforestation"
    },
]


def fetch_single_dataset(cfg, start_date, today, aoi_geom_dict):
    dataset = cfg["dataset"]
    date_f  = cfg["date_field"]
    conf_f  = cfg["conf_field"]
    label   = cfg["source_label"]

    sql = f"""
    SELECT longitude, latitude, {date_f}, {conf_f}
    FROM results
    WHERE {date_f} >= '{start_date}'
      AND {date_f} <= '{today}'
    """

    url     = f"https://data-api.globalforestwatch.org/dataset/{dataset}/latest/query"
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    body    = {"geometry": aoi_geom_dict, "sql": sql}  

    print(f"  → Fetching {label} ...")
    resp = requests.post(url, headers=headers, json=body)

    if resp.status_code != 200:
        print(f"    [ERROR {resp.status_code}] {label}: {resp.text[:200]}")
        return pd.DataFrame()

    data = resp.json().get("data", [])
    if not data:
        print(f"    [INFO] Tidak ada data untuk {label}.")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df.rename(columns={date_f: "Integrated_Date", conf_f: "Integrated_Alert"}, inplace=True)
    df["Integrated_Date"] = pd.to_datetime(df["Integrated_Date"], errors="coerce")
    df["Source"]          = label
    df["Alert_Type"]      = cfg["alert_type"]

    print(f"    [OK] {len(df)} baris | terbaru: {df['Integrated_Date'].max().date()}")
    return df


def fetch_dist_alert(start_date, today, aoi_geom_dict):
    label = "DIST-ALERT"
    sql = f"""
    SELECT longitude, latitude,
           umd_glad_landsat_alerts__date,
           umd_glad_landsat_alerts__confidence
    FROM results
    WHERE umd_glad_landsat_alerts__date >= '{start_date}'
      AND umd_glad_landsat_alerts__date <= '{today}'
    """

    url     = "https://data-api.globalforestwatch.org/dataset/umd_glad_dist_alerts/latest/query"
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    body    = {"geometry": aoi_geom_dict, "sql": sql}  

    print(f"  → Fetching {label} ...")
    resp = requests.post(url, headers=headers, json=body)

    if resp.status_code != 200:
        print(f"    [ERROR {resp.status_code}] {label}: {resp.text[:200]}")
        return pd.DataFrame()

    data = resp.json().get("data", [])
    if not data:
        print(f"    [INFO] Tidak ada data untuk {label}.")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df.rename(columns={
        "umd_glad_landsat_alerts__date":       "Integrated_Date",
        "umd_glad_landsat_alerts__confidence": "Integrated_Alert"
    }, inplace=True)

    df["Integrated_Date"] = pd.to_datetime(df["Integrated_Date"], errors="coerce")
    df["Source"]          = label
    df["Alert_Type"]      = "Disturbance"

    print(f"    [OK] {len(df)} baris | terbaru: {df['Integrated_Date'].max().date()}")
    return df


def fetch_all_gfw_data(aoi_geom_dict):
    wib        = timezone(timedelta(hours=7))
    today      = datetime.now(wib).strftime("%Y-%m-%d")
    start_date = "2026-01-01"

    print(f"\n{'='*60}")
    print(f"Fetching semua dataset GFW: {start_date} → {today}")
    print(f"{'='*60}")

    frames = []
    for cfg in ALERT_DATASETS:
        df = fetch_single_dataset(cfg, start_date, today, aoi_geom_dict)
        if not df.empty:
            frames.append(df)

    df_dist = fetch_dist_alert(start_date, today, aoi_geom_dict)
    if not df_dist.empty:
        frames.append(df_dist)

    if not frames:
        print("Tidak ada data dari semua dataset GFW.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    print(f"\nTotal gabungan: {len(combined)} baris dari {len(frames)} dataset.")
    print("\nRingkasan per Source:")
    print(combined.groupby(["Source", "Alert_Type"]).size().to_string())
    return combined


def intersect_with_geojson(df, desa_path, pemilik_path, blok_path):
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    )

    desa    = gpd.read_file(desa_path)[["nama_kel", "geometry"]]
    pemilik = gpd.read_file(pemilik_path)[["Owner", "geometry"]]
    blok    = gpd.read_file(blok_path)[["Blok", "geometry"]]

    for layer in [desa, pemilik, blok]:
        if layer.crs is None:
            layer.set_crs("EPSG:4326", inplace=True)
        else:
            layer.to_crs("EPSG:4326", inplace=True)

    gdf = gpd.sjoin(gdf, desa, how="left", predicate="within").rename(columns={"nama_kel": "Desa"})
    gdf.drop(columns=["index_right"], inplace=True, errors="ignore")
    gdf = gpd.sjoin(gdf, pemilik, how="left", predicate="within")
    gdf.drop(columns=["index_right"], inplace=True, errors="ignore")
    gdf = gpd.sjoin(gdf, blok, how="left", predicate="within")
    gdf.drop(columns=["index_right"], inplace=True, errors="ignore")

    gdf = gdf.drop(columns=["geometry"], errors="ignore")

    print(f"\nIntersect selesai: {len(gdf)} baris.")
    print(f"Tanggal maksimum setelah intersect: {pd.to_datetime(gdf['Integrated_Date']).max().date()}")
    return gdf


def overwrite_google_sheet(df):
    creds  = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sh     = client.open_by_key(SPREADSHEET_ID)

    latest_year = pd.to_datetime(df["Integrated_Date"], errors="coerce").dt.year.max()
    sheet_name  = str(latest_year)

    keep_cols = [
        "latitude", "longitude", "Integrated_Date", "Integrated_Alert",
        "Source", "Alert_Type",
        "Desa", "Owner", "Blok"
    ]
    df = df[keep_cols].copy()
    df = df.replace([np.inf, -np.inf], np.nan).fillna("")
    df["Integrated_Date"] = pd.to_datetime(
        df["Integrated_Date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    df = df.astype(str)

    try:
        sheet = sh.worksheet(sheet_name)
        sheet.clear()
        print(f"\nSheet '{sheet_name}' ditemukan dan dikosongkan.")
    except gspread.exceptions.WorksheetNotFound:
        sheet = sh.add_worksheet(title=sheet_name, rows=50000, cols=15)
        print(f"\nSheet '{sheet_name}' dibuat baru.")

    sheet.append_rows([list(df.columns)] + df.values.tolist(), value_input_option="USER_ENTERED")
    print(f"{len(df)} baris berhasil ditulis ke sheet '{sheet_name}'.")


def merge_sheets_to_db():
    creds  = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sh     = client.open_by_key(SPREADSHEET_ID)

    sheets_to_merge = ["2023", "2024", "2025", "2026"]
    all_data = []

    print("\nMerge sheet ke Db:")
    for name in sheets_to_merge:
        try:
            ws   = sh.worksheet(name)
            rows = ws.get_all_records()
            if rows:
                all_data.extend(rows)
                print(f"  ✔ {name}: {len(rows)} baris")
        except gspread.exceptions.WorksheetNotFound:
            print(f"  ⚠ Sheet {name} tidak ditemukan, dilewati.")

    if not all_data:
        print("Tidak ada data untuk digabungkan ke Db.")
        return

    df = pd.DataFrame(all_data)
    df = df.replace([np.inf, -np.inf], np.nan).fillna("")
    df = df.drop_duplicates().reset_index(drop=True)

    try:
        db_sheet = sh.worksheet("Db")
        db_sheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        db_sheet = sh.add_worksheet(title="Db", rows=100000, cols=15)

    db_sheet.append_rows([list(df.columns)] + df.values.tolist(), value_input_option="USER_ENTERED")
    print(f"Sheet 'Db' diperbarui: {len(df)} baris total.")


def update_log(latest_date):
    creds  = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)

    try:
        log_sheet = client.open_by_key(SPREADSHEET_ID).worksheet(LOG_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        log_sheet = client.open_by_key(SPREADSHEET_ID).add_worksheet(
            title=LOG_SHEET_NAME, rows=10, cols=3
        )

    wib     = timezone(timedelta(hours=7))
    now_wib = datetime.now(wib).strftime("%Y-%m-%d %H:%M:%S")

    log_sheet.clear()
    log_sheet.append_rows([
        ["Note", "Last Update", "Latest Alert Date"],
        ["Update", now_wib, str(latest_date)]
    ], value_input_option="USER_ENTERED")
    print(f"\nLog diperbarui: {now_wib} | Latest alert: {latest_date}")


if __name__ == "__main__":

    aoi_shape, aoi_geom_dict = load_aoi_geometry(AOI_PATH)

    df = fetch_all_gfw_data(aoi_geom_dict)

    if not df.empty:
        gdf = intersect_with_geojson(df, DESA_PATH, PEMILIK_PATH, BLOK_PATH)
        if not gdf.empty:
            overwrite_google_sheet(gdf)
            merge_sheets_to_db()
            update_log(gdf["Integrated_Date"].max())
        else:
            print("Tidak ada hasil intersect.")
    else:
        print("Tidak ada data dari GFW.")
