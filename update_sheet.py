import requests
import pandas as pd
import geopandas as gpd
import gspread
import json
from shapely.geometry import shape
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone
import numpy as np

API_KEY = "912b99d5-ecc2-47aa-86fe-1f986b9b070b"
SPREADSHEET_ID = "1UW3uOFcLr4AQFBp_VMbEXk37_Vb5DekHU-_9QSkskCo"
LOG_SHEET_NAME = "Log_Update"

AOI_PATH     = "data/aoi.json"
DESA_PATH    = "data/Desa.json"
PEMILIK_PATH = "data/PemilikLahan.json"
BLOK_PATH    = "data/blok.json"

LULC_GDRIVE_FILE_ID = "1uy1VJruyiwsZBcdv5YYRTI9EcAWZVB2O"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def load_aoi_geometry(aoi_path):
    with open(aoi_path, "r") as f:
        aoi_geojson = json.load(f)
    feature   = aoi_geojson["features"][0]
    geom_dict = feature["geometry"]
    geom_shape = shape(geom_dict)
    return geom_shape, geom_dict


def load_lulc_from_gdrive(file_id):
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    resp = requests.get(download_url)
    if resp.status_code != 200:
        raise ConnectionError(f"Gagal mengunduh LULC: HTTP {resp.status_code}")
    geojson_data = resp.json()

    valid_features = [f for f in geojson_data["features"] if f.get("geometry") is not None]
    if not valid_features:
        raise ValueError("Semua fitur LULC memiliki geometry null.")

    lulc = gpd.GeoDataFrame.from_features(valid_features, crs="EPSG:4326")
    if "Class" not in lulc.columns:
        raise ValueError(f"Kolom 'Class' tidak ditemukan. Kolom tersedia: {list(lulc.columns)}")
    return lulc[["Class", "geometry"]]


def fetch_gfw_data(aoi_geom_dict):
    wib        = timezone(timedelta(hours=7))
    today      = datetime.now(wib).strftime("%Y-%m-%d")
    start_date = "2026-01-01"

    sql = f"""
    SELECT
        longitude,
        latitude,
        gfw_integrated_alerts__date,
        gfw_integrated_alerts__confidence,
        umd_glad_landsat_alerts__confidence,
        umd_glad_sentinel2_alerts__confidence,
        wur_radd_alerts__confidence
    FROM results
    WHERE gfw_integrated_alerts__date >= '{start_date}'
      AND gfw_integrated_alerts__date <= '{today}'
    """

    url     = "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/latest/query"
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    body    = {"geometry": aoi_geom_dict, "sql": sql}

    resp = requests.post(url, headers=headers, json=body)
    if resp.status_code != 200:
        return pd.DataFrame()

    data = resp.json().get("data", [])
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df.rename(columns={
        "gfw_integrated_alerts__date":           "Date",
        "gfw_integrated_alerts__confidence":     "Conf_Integrated",
        "umd_glad_landsat_alerts__confidence":   "Conf_GLADL",
        "umd_glad_sentinel2_alerts__confidence": "Conf_GLADS2",
        "wur_radd_alerts__confidence":           "Conf_RADD",
    }, inplace=True)

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df


def intersect_with_geojson(df, desa_path, pemilik_path, blok_path, lulc_gdf):
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    )

    desa    = gpd.read_file(desa_path)[["nama_kel", "geometry"]]
    pemilik = gpd.read_file(pemilik_path)[["Owner", "geometry"]]
    blok    = gpd.read_file(blok_path)[["Blok", "geometry"]]

    for layer in [desa, pemilik, blok, lulc_gdf]:
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

    gdf = gpd.sjoin(gdf, lulc_gdf, how="left", predicate="within")
    gdf.drop(columns=["index_right"], inplace=True, errors="ignore")

    gdf["Class"] = gdf["Class"].fillna("Outside Project Area")
    gdf = gdf.drop(columns=["geometry"], errors="ignore")
    return gdf


def overwrite_google_sheet(df):
    creds  = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sh     = client.open_by_key(SPREADSHEET_ID)

    latest_year = pd.to_datetime(df["Date"], errors="coerce").dt.year.max()
    sheet_name  = str(latest_year)

    keep_cols = [
        "latitude", "longitude", "Date",
        "Conf_Integrated", "Conf_GLADL", "Conf_GLADS2", "Conf_RADD",
        "Desa", "Owner", "Blok", "Class"
    ]
    df = df[keep_cols].copy()
    df = df.replace([np.inf, -np.inf], np.nan).fillna("")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.astype(str)

    try:
        sheet = sh.worksheet(sheet_name)
        sheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        sheet = sh.add_worksheet(title=sheet_name, rows=50000, cols=15)

    sheet.append_rows([list(df.columns)] + df.values.tolist(), value_input_option="USER_ENTERED")


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


if __name__ == "__main__":
    aoi_shape, aoi_geom_dict = load_aoi_geometry(AOI_PATH)
    lulc_gdf = load_lulc_from_gdrive(LULC_GDRIVE_FILE_ID)

    df = fetch_gfw_data(aoi_geom_dict)

    if not df.empty:
        gdf = intersect_with_geojson(df, DESA_PATH, PEMILIK_PATH, BLOK_PATH, lulc_gdf)
        if not gdf.empty:
            overwrite_google_sheet(gdf)
            update_log(gdf["Date"].max())
