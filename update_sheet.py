import requests
import pandas as pd
import geopandas as gpd
import gspread
import json
from shapely.geometry import shape
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone
import numpy as np

# ================= CONFIG =================
API_KEY = "YOUR_API_KEY"
SPREADSHEET_ID = "YOUR_SPREADSHEET_ID"
LOG_SHEET_NAME = "Log_Update"

AOI_PATH = "data/aoi.json"
DESA_PATH = "data/Desa.json"
PEMILIK_PATH = "data/PemilikLahan.json"
BLOK_PATH = "data/blok.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ================= LOAD AOI =================
def load_aoi_geometry(aoi_path):
    with open(aoi_path, "r") as f:
        aoi_geojson = json.load(f)
    feature     = aoi_geojson["features"][0]
    geom_dict   = feature["geometry"]
    geom_shape  = shape(geom_dict)
    print(f"AOI loaded: {geom_dict['type']}")
    return geom_shape, geom_dict

# ================= FETCH GFW INTEGRATED =================
def fetch_gfw_data(aoi_geom_dict):
    wib        = timezone(timedelta(hours=7))
    today      = datetime.now(wib).strftime("%Y-%m-%d")
    start_date = "2023-01-01"

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

    print("Fetching Integrated Alerts...")
    resp = requests.post(url, headers=headers, json=body)

    if resp.status_code != 200:
        print(resp.text)
        return pd.DataFrame()

    data = resp.json().get("data", [])
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    df.rename(columns={
        "gfw_integrated_alerts__date": "Date",
        "gfw_integrated_alerts__confidence": "Conf_Integrated",
        "umd_glad_landsat_alerts__confidence": "Conf_GLADL",
        "umd_glad_sentinel2_alerts__confidence": "Conf_GLADS2",
        "wur_radd_alerts__confidence": "Conf_RADD",
    }, inplace=True)

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Source"] = "Integrated"

    return df

# ================= FETCH DIST ALERT =================
def fetch_dist_alerts(aoi_geom_dict):
    wib        = timezone(timedelta(hours=7))
    today      = datetime.now(wib).strftime("%Y-%m-%d")
    start_date = "2023-01-01"

    sql = f"""
    SELECT
        longitude,
        latitude,
        alert_date,
        alert_magnitude,
        alert_confidence
    FROM results
    WHERE alert_date >= '{start_date}'
      AND alert_date <= '{today}'
    """

    url     = "https://data-api.globalforestwatch.org/dataset/gfw_disturbance_alerts/latest/query"
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    body    = {"geometry": aoi_geom_dict, "sql": sql}

    print("Fetching DIST-ALERT...")
    resp = requests.post(url, headers=headers, json=body)

    if resp.status_code != 200:
        print(resp.text)
        return pd.DataFrame()

    data = resp.json().get("data", [])
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    df.rename(columns={
        "alert_date": "Date",
        "alert_magnitude": "DIST_Magnitude",
        "alert_confidence": "DIST_Confidence"
    }, inplace=True)

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Source"] = "DIST"

    return df

# ================= SPATIAL JOIN =================
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
        layer = layer.to_crs("EPSG:4326")

    gdf = gpd.sjoin(gdf, desa, how="left", predicate="within").rename(columns={"nama_kel": "Desa"})
    gdf = gpd.sjoin(gdf, pemilik, how="left", predicate="within")
    gdf = gpd.sjoin(gdf, blok, how="left", predicate="within")

    gdf = gdf.drop(columns=["geometry", "index_right"], errors="ignore")
    return gdf

# ================= EXPORT TO SHEET =================
def overwrite_google_sheet(df):
    creds  = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sh     = client.open_by_key(SPREADSHEET_ID)

    year = pd.to_datetime(df["Date"]).dt.year.max()
    sheet_name = str(year)

    df = df.replace([np.inf, -np.inf], np.nan).fillna("")
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    try:
        sheet = sh.worksheet(sheet_name)
        sheet.clear()
    except:
        sheet = sh.add_worksheet(title=sheet_name, rows=50000, cols=20)

    sheet.append_rows([list(df.columns)] + df.values.tolist())

# ================= MAIN =================
if __name__ == "__main__":
    aoi_shape, aoi_geom_dict = load_aoi_geometry(AOI_PATH)

    df_main = fetch_gfw_data(aoi_geom_dict)
    df_dist = fetch_dist_alerts(aoi_geom_dict)

    if df_main.empty and df_dist.empty:
        print("No data")
        exit()

    df_all = pd.concat([df_main, df_dist], ignore_index=True)

    gdf = intersect_with_geojson(df_all, DESA_PATH, PEMILIK_PATH, BLOK_PATH)

    overwrite_google_sheet(gdf)

    print("DONE")
