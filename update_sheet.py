import requests
import pandas as pd
import gspread
import json
import geopandas as gpd
from shapely.geometry import shape, Point
from google.oauth2.service_account import Credentials

API_KEY = "912b99d5-ecc2-47aa-86fe-1f986b9b070b"
SPREADSHEET_ID = "1UW3uOFcLr4AQFBp_VMbEXk37_Vb5DekHU-_9QSkskCo"
SHEET_NAME = "Sheet1"
AOI_PATH = "data/aoi.json"
DESA_PATH = "data/desa.json"
PEMILIK_PATH = "data/PemilikLahan.json"
BLOK_PATH = "data/blok.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def fetch_gfw_data():
    """Fetch GFW RADD alerts data from API"""
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
        return pd.DataFrame()

    data = resp.json().get("data", [])
    if not data:
        print("Tidak ada data ditemukan.")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    print(f"Berhasil mengambil {len(df)} baris data dari API.")
    return df


def clip_with_aoi(df, aoi_path):
    """Clip dataframe points using AOI polygon"""
    try:
        with open(aoi_path, "r") as f:
            aoi_geojson = json.load(f)
        aoi_polygon = shape(aoi_geojson["features"][0]["geometry"])
    except Exception as e:
        print(f"Gagal membaca AOI: {e}")
        return df

    inside = []
    for _, row in df.iterrows():
        point = Point(row["longitude"], row["latitude"])
        if aoi_polygon.contains(point):
            inside.append(row)

    if not inside:
        print("Tidak ada titik dalam area AOI.")
        return pd.DataFrame()

    clipped_df = pd.DataFrame(inside)
    print(f"{len(clipped_df)} titik berada di dalam AOI.")
    return clipped_df


def spatial_join(df):
    """Intersect titik dengan desa, pemilik lahan, dan blok"""
    if df.empty:
        return df

    gdf_points = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")

    gdf_desa = gpd.read_file(DESA_PATH).to_crs("EPSG:4326")
    gdf_pemilik = gpd.read_file(PEMILIK_PATH).to_crs("EPSG:4326")
    gdf_blok = gpd.read_file(BLOK_PATH).to_crs("EPSG:4326")

    gdf_points = gpd.sjoin(gdf_points, gdf_desa[['nama_kel', 'geometry']], how="left", predicate="within")
    gdf_points = gpd.sjoin(gdf_points, gdf_pemilik[['Owner', 'geometry']], how="left", predicate="within", rsuffix="_pemilik")
    gdf_points = gpd.sjoin(gdf_points, gdf_blok[['Blok', 'geometry']], how="left", predicate="within", rsuffix="_blok")

    gdf_points = gdf_points.drop(columns=[col for col in gdf_points.columns if col.startswith('geometry_')], errors='ignore')

    print("Intersect selesai: ditambahkan kolom nama_kel, Owner, dan Blok.")
    return pd.DataFrame(gdf_points.drop(columns="geometry"))


def update_to_google_sheet(df):
    """Update Google Sheet"""
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

    sheet.clear()
    if df.empty:
        sheet.update([["Tidak ada data dalam area AOI."]])
        print("Sheet diperbarui tanpa data.")
        return

    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    print(f"{len(df)} baris berhasil dikirim ke Google Sheet.")


if __name__ == "__main__":
    df = fetch_gfw_data()
    if not df.empty:
        df = clip_with_aoi(df, AOI_PATH)
        df = spatial_join(df)
    update_to_google_sheet(df)
