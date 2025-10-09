import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.strtree import STRtree
import numpy as np
from google.oauth2.service_account import Credentials
import gspread

API_KEY = "912b99d5-ecc2-47aa-86fe-1f986b9b070b"
SPREADSHEET_ID = "1UW3uOFcLr4AQFBp_VMbEXk37_Vb5DekHU-_9QSkskCo"
SHEET_NAME = "Sheet1"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def fetch_gfw_data():
    geometry = {
        "type": "Polygon",
        "coordinates": [[
            [110.15497, 0.67329],
            [110.38332, 0.67329],
            [110.38332, 0.91435],
            [110.15497, 0.91435],
            [110.15497, 0.67329]
        ]]
    }

    sql = """
    SELECT longitude, latitude, wur_radd_alerts__date, wur_radd_alerts__confidence
    FROM results
    WHERE wur_radd_alerts__date >= '2025-07-01'
    AND wur_radd_alerts__date <= '2025-10-01'
    """

    url = "https://data-api.globalforestwatch.org/dataset/wur_radd_alerts/latest/query"
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    body = {"geometry": geometry, "sql": sql}

    print("Mengambil data GFW...")
    resp = requests.post(url, headers=headers, json=body)

    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text}")
        return gpd.GeoDataFrame()

    data = resp.json().get("data", [])
    if not data:
        print("Tidak ada data ditemukan.")
        return gpd.GeoDataFrame()

    df = pd.DataFrame(data)
    print(f"Berhasil mengambil {len(df)} baris data.")
    return df

def calculate_cluster_area(df):
    print("🔹 Membuat GeoDataFrame dan menghitung cluster luas...")

    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
    gdf = gdf.to_crs(epsg=3857)

    buffers = gdf.geometry.buffer(11.2)
    tree = STRtree(buffers)
    geom_list = list(buffers)

    cluster_id = [-1] * len(geom_list)
    current_cluster = 0

    for i, geom in enumerate(geom_list):
        if cluster_id[i] != -1:
            continue
        cluster_id[i] = current_cluster
        neighbors = tree.query(geom)
        for n in neighbors:
            j = geom_list.index(n)
            if cluster_id[j] == -1 and geom.intersects(n):
                cluster_id[j] = current_cluster
        current_cluster += 1

    gdf["cluster"] = cluster_id

    cluster_areas = (
        gdf.dissolve(by="cluster")
        .buffer(0)
        .area
        .rename("luas_m2")
        .to_dict()
    )
    gdf["luas_ha"] = gdf["cluster"].map(cluster_areas) / 10000
    gdf = gdf.to_crs(epsg=4326)

    print("Berhasil menghitung luas tiap cluster.")
    return gdf

def update_to_google_sheet(df):
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

    sheet.clear()
    if df.empty:
        sheet.update([["Tidak ada data ditemukan."]])
        print("Sheet diperbarui tanpa data.")
        return

    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    print(f"{len(df)} baris berhasil dikirim ke Google Sheet.")

if __name__ == "__main__":
    df = fetch_gfw_data()
    if not df.empty:
        gdf = calculate_cluster_area(df)

        gdf["Desa"] = "Belum diketahui"
        gdf["Owner"] = "Belum diketahui"
        gdf["Penutup Lahan"] = "Belum diketahui"
        gdf["Blok"] = "Belum diketahui"

        df_upload = gdf[[
            "latitude", "longitude", "wur_radd_alerts__date",
            "wur_radd_alerts__confidence", "Desa", "Owner",
            "Penutup Lahan", "Blok", "luas_ha"
        ]].copy()

        update_to_google_sheet(df_upload)
