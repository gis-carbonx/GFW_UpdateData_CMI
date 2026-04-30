import requests
import pandas as pd
import geopandas as gpd
import gspread
import json
from shapely.geometry import shape
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone
import numpy as np

API_KEY = "YOUR_API_KEY"
SPREADSHEET_ID = "YOUR_SPREADSHEET_ID"
LOG_SHEET_NAME = "Log_Update"

AOI_PATH = "data/aoi.json"
DESA_PATH = "data/Desa.json"
PEMILIK_PATH = "data/PemilikLahan.json"
BLOK_PATH = "data/blok.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# =========================
# LOAD AOI
# =========================
def load_aoi_geometry(aoi_path):
    with open(aoi_path, "r") as f:
        aoi_geojson = json.load(f)
    feature = aoi_geojson["features"][0]
    geom_dict = feature["geometry"]
    geom_shape = shape(geom_dict)
    return geom_shape, geom_dict

# =========================
# FETCH INTEGRATED ALERT
# =========================
def fetch_integrated(aoi_geom_dict):
    wib = timezone(timedelta(hours=7))
    today = datetime.now(wib).strftime("%Y-%m-%d")
    start_date = "2023-01-01"

    sql = f"""
    SELECT longitude, latitude,
           gfw_integrated_alerts__date,
           gfw_integrated_alerts__confidence,
           umd_glad_landsat_alerts__confidence,
           umd_glad_sentinel2_alerts__confidence,
           wur_radd_alerts__confidence
    FROM results
    WHERE gfw_integrated_alerts__date >= '{start_date}'
      AND gfw_integrated_alerts__date <= '{today}'
    """

    url = "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/latest/query"
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    body = {"geometry": aoi_geom_dict, "sql": sql}

    resp = requests.post(url, headers=headers, json=body)
    data = resp.json().get("data", [])

    df = pd.DataFrame(data)
    if df.empty:
        return df

    df.rename(columns={
        "gfw_integrated_alerts__date": "Date",
        "gfw_integrated_alerts__confidence": "Confidence",
    }, inplace=True)

    df["Source"] = "Integrated"
    return df

# =========================
# FETCH DIST ALERT
# =========================
def fetch_disturbance(aoi_geom_dict):
    wib = timezone(timedelta(hours=7))
    today = datetime.now(wib).strftime("%Y-%m-%d")
    start_date = "2023-01-01"

    sql = f"""
    SELECT longitude, latitude,
           alert_date,
           confidence,
           alert_type
    FROM results
    WHERE alert_date >= '{start_date}'
      AND alert_date <= '{today}'
    """

    url = "https://data-api.globalforestwatch.org/dataset/gfw_disturbance_alerts/latest/query"
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    body = {"geometry": aoi_geom_dict, "sql": sql}

    resp = requests.post(url, headers=headers, json=body)
    data = resp.json().get("data", [])

    df = pd.DataFrame(data)
    if df.empty:
        return df

    df.rename(columns={
        "alert_date": "Date",
        "confidence": "Confidence",
        "alert_type": "Dist_Type"
    }, inplace=True)

    df["Source"] = "DIST"
    return df

# =========================
# MERGE BOTH DATA
# =========================
def fetch_all_data(aoi_geom_dict):
    df1 = fetch_integrated(aoi_geom_dict)
    df2 = fetch_disturbance(aoi_geom_dict)

    df = pd.concat([df1, df2], ignore_index=True)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    return df

# =========================
# SPATIAL JOIN
# =========================
def intersect_with_geojson(df):
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    )

    desa = gpd.read_file(DESA_PATH)[["nama_kel", "geometry"]]
    pemilik = gpd.read_file(PEMILIK_PATH)[["Owner", "geometry"]]
    blok = gpd.read_file(BLOK_PATH)[["Blok", "geometry"]]

    for layer in [desa, pemilik, blok]:
        layer.to_crs("EPSG:4326", inplace=True)

    gdf = gpd.sjoin(gdf, desa, how="left", predicate="within").rename(columns={"nama_kel": "Desa"})
    gdf = gpd.sjoin(gdf, pemilik, how="left", predicate="within")
    gdf = gpd.sjoin(gdf, blok, how="left", predicate="within")

    return gdf.drop(columns=["geometry"])

# =========================
# EXPORT
# =========================
def overwrite_google_sheet(df):
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)

    df = df.fillna("").astype(str)

    try:
        sheet = sh.worksheet("All_Data")
        sheet.clear()
    except:
        sheet = sh.add_worksheet(title="All_Data", rows=100000, cols=20)

    sheet.append_rows([list(df.columns)] + df.values.tolist())

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    _, aoi_geom_dict = load_aoi_geometry(AOI_PATH)

    df = fetch_all_data(aoi_geom_dict)

    if not df.empty:
        gdf = intersect_with_geojson(df)
        overwrite_google_sheet(gdf)
        print("DONE")
