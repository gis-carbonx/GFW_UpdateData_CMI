import requests
import pandas as pd
import geopandas as gpd
import gspread
import json
import numpy as np

from shapely.geometry import shape
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone

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
def load_aoi_geometry(path):
    with open(path, "r") as f:
        geojson = json.load(f)

    feature = geojson["features"][0]
    geom_dict = feature["geometry"]
    geom_shape = shape(geom_dict)

    print(f"AOI loaded: {geom_dict['type']}")
    return geom_shape, geom_dict


# ================= FETCH GFW =================
def fetch_gfw_data(aoi_geom_dict):
    wib = timezone(timedelta(hours=7))
    today = datetime.now(wib).strftime("%Y-%m-%d")
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

    url = "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/latest/query"

    response = requests.post(
        url,
        headers={"x-api-key": API_KEY, "Content-Type": "application/json"},
        json={"geometry": aoi_geom_dict, "sql": sql}
    )

    if response.status_code != 200:
        print(f"ERROR {response.status_code}: {response.text[:200]}")
        return pd.DataFrame()

    data = response.json().get("data", [])
    if not data:
        print("No data from GFW")
        return pd.DataFrame()

    df = pd.DataFrame(data)

    df.rename(columns={
        "gfw_integrated_alerts__date": "Date",
        "gfw_integrated_alerts__confidence": "Conf_Integrated",
        "umd_glad_landsat_alerts__confidence": "Conf_GLADL",
        "umd_glad_sentinel2_alerts__confidence": "Conf_GLADS2",
        "wur_radd_alerts__confidence": "Conf_RADD"
    }, inplace=True)

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    print(f"Fetched {len(df)} rows")
    return df


# ================= SPATIAL JOIN =================
def intersect_data(df):
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    )

    desa = gpd.read_file(DESA_PATH)[["nama_kel", "geometry"]]
    pemilik = gpd.read_file(PEMILIK_PATH)[["Owner", "geometry"]]
    blok = gpd.read_file(BLOK_PATH)[["Blok", "geometry"]]

    for layer in [desa, pemilik, blok]:
        layer.set_crs("EPSG:4326", inplace=True, allow_override=True)

    gdf = gpd.sjoin(gdf, desa, how="left", predicate="within").rename(columns={"nama_kel": "Desa"})
    gdf = gpd.sjoin(gdf, pemilik, how="left", predicate="within")
    gdf = gpd.sjoin(gdf, blok, how="left", predicate="within")

    gdf.drop(columns=["geometry", "index_right"], errors="ignore", inplace=True)

    print(f"Spatial join done: {len(gdf)} rows")
    return gdf


# ================= WRITE TO GOOGLE SHEET =================
def write_to_sheet(df):
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)

    year = str(pd.to_datetime(df["Date"]).dt.year.max())

    columns = [
        "latitude", "longitude", "Date",
        "Conf_Integrated", "Conf_GLADL",
        "Conf_GLADS2", "Conf_RADD",
        "Desa", "Owner", "Blok"
    ]

    df = df[columns].copy()
    df = df.replace([np.inf, -np.inf], np.nan).fillna("")
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    try:
        sheet = sh.worksheet(year)
        sheet.clear()
    except:
        sheet = sh.add_worksheet(title=year, rows=50000, cols=15)

    sheet.append_rows([df.columns.tolist()] + df.values.tolist())
    print(f"Written to sheet {year}")


# ================= MERGE =================
def merge_to_db():
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)

    years = ["2023", "2024", "2025", "2026"]
    all_data = []

    for y in years:
        try:
            rows = sh.worksheet(y).get_all_records()
            all_data.extend(rows)
        except:
            continue

    if not all_data:
        return

    df = pd.DataFrame(all_data).drop_duplicates()

    try:
        db = sh.worksheet("Db")
        db.clear()
    except:
        db = sh.add_worksheet(title="Db", rows=100000, cols=15)

    db.append_rows([df.columns.tolist()] + df.values.tolist())
    print("DB updated")


# ================= LOG =================
def update_log(latest_date):
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)

    try:
        log = client.open_by_key(SPREADSHEET_ID).worksheet(LOG_SHEET_NAME)
    except:
        log = client.open_by_key(SPREADSHEET_ID).add_worksheet(
            title=LOG_SHEET_NAME, rows=10, cols=3
        )

    now = datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d %H:%M:%S")

    log.clear()
    log.append_rows([
        ["Note", "Last Update", "Latest Alert Date"],
        ["Update", now, str(latest_date)]
    ])

    print("Log updated")


# ================= MAIN =================
if __name__ == "__main__":
    _, aoi_geom = load_aoi_geometry(AOI_PATH)

    df = fetch_gfw_data(aoi_geom)

    if not df.empty:
        gdf = intersect_data(df)

        if not gdf.empty:
            write_to_sheet(gdf)
            merge_to_db()
            update_log(gdf["Date"].max())
        else:
            print("No intersect result")
    else:
        print("No GFW data")
