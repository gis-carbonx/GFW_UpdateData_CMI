import os
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

START_DATE = "2023-01-01"
END_DATE   = "2026-12-31"

AOI_PATH = "data/aoi_v26.json"

DESA_PATH = "data/Desa.json"

PEMILIK_PATH = "data/penggarap_v26.json"

BLOK_PATH = "data/blok_v26.json"

LULC_URL = "https://drive.google.com/uc?export=download&id=1v02RLW8-iDjfsXBjcv4ukaFwjKXYVPNl"

LULC_PATH = "data/lulc_v26.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

start_dt = pd.to_datetime(START_DATE)
end_dt = pd.to_datetime(END_DATE)

if start_dt > end_dt:
    raise ValueError("START_DATE lebih besar dari END_DATE")

def load_aoi_geometry(aoi_path):

    with open(aoi_path, "r") as f:
        aoi_geojson = json.load(f)

    feature = aoi_geojson["features"][0]

    geom_dict = feature["geometry"]

    geom_shape = shape(geom_dict)

    print(f"AOI dimuat: {aoi_path}")
    print(f"Tipe geometry: {geom_dict['type']}")

    return geom_shape, geom_dict

def download_lulc_if_needed():

    os.makedirs("data", exist_ok=True)

    if not os.path.exists(LULC_PATH):

        print("Downloading LULC...")

        r = requests.get(LULC_URL)

        if r.status_code == 200:

            with open(LULC_PATH, "wb") as f:
                f.write(r.content)

            print("LULC berhasil didownload.")

        else:
            raise Exception(
                f"Gagal download LULC | Status: {r.status_code}"
            )

    else:
        print("LULC sudah tersedia lokal.")

def fetch_gfw_data(aoi_geom_dict):

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
    WHERE gfw_integrated_alerts__date >= '{START_DATE}'
      AND gfw_integrated_alerts__date <= '{END_DATE}'
    """

    url = "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/latest/query"

    headers = {
        "x-api-key": API_KEY,
        "Content-Type": "application/json"
    }

    body = {
        "geometry": aoi_geom_dict,
        "sql": sql
    }

    print(f"Start Date : {START_DATE}")
    print(f"End Date   : {END_DATE}")

    resp = requests.post(
        url,
        headers=headers,
        json=body
    )

    print(f"\nResponse Status: {resp.status_code}")

    if resp.status_code != 200:

        print("\nERROR RESPONSE:")
        print(resp.text[:1000])

        return pd.DataFrame()

    try:
        response_json = resp.json()

    except Exception as e:

        print(f"Gagal parse JSON: {e}")

        print(resp.text[:1000])

        return pd.DataFrame()

    data = response_json.get("data", [])

    if not data:

        print("\nTidak ada data dari GFW.")

        print("\nRaw response:")
        print(json.dumps(response_json, indent=2)[:2000])

        return pd.DataFrame()

    df = pd.DataFrame(data)

    df.rename(columns={

        "gfw_integrated_alerts__date":
            "Date",

        "gfw_integrated_alerts__confidence":
            "Conf_Integrated",

        "umd_glad_landsat_alerts__confidence":
            "Conf_GLADL",

        "umd_glad_sentinel2_alerts__confidence":
            "Conf_GLADS2",

        "wur_radd_alerts__confidence":
            "Conf_RADD",

    }, inplace=True)

    df["Date"] = pd.to_datetime(
        df["Date"],
        errors="coerce"
    )

    print(f"\n[OK] Total Data: {len(df)}")

    print(f"Tanggal Minimum: {df['Date'].min()}")

    print(f"Tanggal Maximum: {df['Date'].max()}")

    print("\nConfidence Summary:")
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
        geometry=gpd.points_from_xy(
            df.longitude,
            df.latitude
        ),
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

            layer.set_crs(
                "EPSG:4326",
                inplace=True
            )

        else:

            layer.to_crs(
                "EPSG:4326",
                inplace=True
            )

    gdf = gpd.sjoin(
        gdf,
        desa,
        how="left",
        predicate="within"
    )

    gdf.rename(columns={
        "nama_kel": "Desa"
    }, inplace=True)

    gdf.drop(
        columns=["index_right"],
        inplace=True,
        errors="ignore"
    )

    gdf = gpd.sjoin(
        gdf,
        pemilik,
        how="left",
        predicate="within"
    )

    gdf.drop(
        columns=["index_right"],
        inplace=True,
        errors="ignore"
    )

    gdf = gpd.sjoin(
        gdf,
        blok,
        how="left",
        predicate="within"
    )

    gdf.drop(
        columns=["index_right"],
        inplace=True,
        errors="ignore"
    )

    gdf = gpd.sjoin(
        gdf,
        lulc,
        how="left",
        predicate="within"
    )

    gdf.drop(
        columns=["index_right"],
        inplace=True,
        errors="ignore"
    )

    gdf.rename(columns={
        "Class": "Penutup_Lahan"
    }, inplace=True)

    gdf.drop(
        columns=["geometry"],
        inplace=True,
        errors="ignore"
    )

    print(f"\nIntersect selesai.")

    print(f"Jumlah data: {len(gdf)}")

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

    df = df.replace(
        [np.inf, -np.inf],
        np.nan
    ).fillna("")

    df["Date"] = pd.to_datetime(
        df["Date"],
        errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    df = df.astype(str)

    try:

        sheet = sh.worksheet(sheet_name)

        sheet.clear()

        print(f"Sheet '{sheet_name}' ditemukan.")

    except gspread.exceptions.WorksheetNotFound:

        sheet = sh.add_worksheet(
            title=sheet_name,
            rows=50000,
            cols=20
        )

        print(f"Sheet '{sheet_name}' dibuat.")

    sheet.append_rows(
        [list(df.columns)] + df.values.tolist(),
        value_input_option="USER_ENTERED"
    )

    print(f"{len(df)} baris berhasil ditulis.")

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

    now_wib = datetime.now(wib).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    log_sheet.clear()

    log_sheet.append_rows([
        ["Note", "Last Update", "Latest Alert Date"],
        ["Update", now_wib, str(latest_date)]
    ])

    print(f"\nLog berhasil diperbarui.")


if __name__ == "__main__":

    aoi_shape, aoi_geom_dict = load_aoi_geometry(
        AOI_PATH
    )

    df = fetch_gfw_data(
        aoi_geom_dict
    )

    if not df.empty:

        gdf = intersect_with_geojson(
            df,
            DESA_PATH,
            PEMILIK_PATH,
            BLOK_PATH
        )

        if not gdf.empty:

            overwrite_google_sheet(gdf)

            update_log(
                gdf["Date"].max()
            )

        else:

            print("Tidak ada hasil intersect.")

    else:

        print("Tidak ada data dari GFW.")
