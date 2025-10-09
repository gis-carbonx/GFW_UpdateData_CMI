import requests
import pandas as pd
import geopandas as gpd
import gspread
import json
from shapely.geometry import shape, Point
from shapely.ops import unary_union
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

API_KEY = "912b99d5-ecc2-47aa-86fe-1f986b9b070b"
SPREADSHEET_ID = "1UW3uOFcLr4AQFBp_VMbEXk37_Vb5DekHU-_9QSkskCo"
SHEET_NAME = "Sheet1"

AOI_PATH = "data/aoi.json"
DESA_PATH = "data/Desa.json"
PEMILIK_PATH = "data/PemilikLahan.json"
BLOK_PATH = "data/blok.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def fetch_gfw_data():
    """Fetch Integrated Alert from GFW API"""
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

    today = datetime.utcnow().date()
    start_date = "2024-01-01"
    end_date = today

    sql = f"""
    SELECT 
        longitude, 
        latitude, 
        gfw_integrated_alerts__date,
        gfw_integrated_alerts__confidence
    FROM results
    WHERE gfw_integrated_alerts__date >= '{start_date}' AND gfw_integrated_alerts__date <= '{end_date}'
    """

    url = "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/latest/query"
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    body = {"geometry": geometry, "sql": sql}


    print("Mengambil Integrated Alert dari GFW...")
    resp = requests.post(url, headers=headers, json=body)
    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text}")
        return pd.DataFrame()

    data = resp.json().get("data", [])
    if not data:
        print("Tidak ada data Integrated Alert ditemukan.")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df = df.rename(columns={
        "gfw_integrated_alerts__date": "Integrated_Date",
        "gfw_integrated_alerts__confidence": "Integrated_Alert"
    })
    df["Integrated_Date"] = pd.to_datetime(df["Integrated_Date"], errors="coerce")
    print(f"Berhasil mengambil {len(df)} baris Integrated Alert dari API.")
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

def intersect_with_geojson(df, desa_path, pemilik_path, blok_path):
    """Tambahkan atribut dari tiga layer GeoJSON"""
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")

    desa = gpd.read_file(desa_path)[["nama_kel", "geometry"]]
    pemilik = gpd.read_file(pemilik_path)[["Owner", "geometry"]]
    blok = gpd.read_file(blok_path)[["Blok", "geometry"]]

    for layer in [desa, pemilik, blok]:
        if layer.crs is not None:
            layer.to_crs("EPSG:4326", inplace=True)
        else:
            layer.set_crs("EPSG:4326", inplace=True)

    gdf = gpd.sjoin(gdf, desa, how="left", predicate="within").drop(columns=["index_right"], errors="ignore")
    gdf = gpd.sjoin(gdf, pemilik, how="left", predicate="within").drop(columns=["index_right"], errors="ignore")
    gdf = gpd.sjoin(gdf, blok, how="left", predicate="within").drop(columns=["index_right"], errors="ignore")

    gdf = gdf.rename(columns={"nama_kel": "Desa"})
    gdf = gdf.loc[:, ~gdf.columns.str.contains("^index")]
    gdf = gdf.sort_values(by="Integrated_Date", ascending=True)
    print("Intersect selesai: kolom Desa, Owner, dan Blok berhasil ditambahkan.")
    return gdf

def cluster_points(gdf):
    """Cluster titik bertampalan dengan buffer 11.2m"""
    print("Melakukan clustering titik...")
    gdf = gdf.sort_values(by="Integrated_Date", ascending=True).reset_index(drop=True)
    gdf = gdf.to_crs(epsg=32749)  # UTM 49N
    gdf["buffer"] = gdf.geometry.buffer(5.6)

    union_poly = unary_union(gdf["buffer"])
    if union_poly.geom_type == "Polygon":
        clusters = [union_poly]
    else:
        clusters = list(union_poly.geoms)

    cluster_gdf = gpd.GeoDataFrame(geometry=clusters, crs=gdf.crs)
    cluster_gdf["Cluster_ID"] = [f"C{str(i+1).zfill(3)}" for i in range(len(cluster_gdf))]

    joined = gpd.sjoin(gdf, cluster_gdf, how="left", predicate="intersects")

    cluster_count = joined.groupby("Cluster_ID").size().reset_index(name="Jumlah_Titik")
    cluster_count["Luas_Ha"] = (cluster_count["Jumlah_Titik"] * (11.2 * 11.2) / 10_000).round(4)

    joined = joined.merge(cluster_count, on="Cluster_ID", how="left")
    joined = joined.to_crs(epsg=4326)
    joined = joined.drop(columns=["buffer", "geometry"], errors="ignore")
    joined = joined.sort_values(by="Integrated_Date", ascending=True).reset_index(drop=True)

    print(f"Clustering selesai: {len(cluster_count)} cluster terbentuk.")
    return joined

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

    if "Integrated_Date" in df.columns:
        df["Integrated_Date"] = pd.to_datetime(df["Integrated_Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    df = df.astype(str)
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    print(f"{len(df)} baris Integrated Alert berhasil dikirim ke Google Sheet.")

if __name__ == "__main__":
    df = fetch_gfw_data()
    if not df.empty:
        df = clip_with_aoi(df, AOI_PATH)
    if not df.empty:
        gdf = intersect_with_geojson(df, DESA_PATH, PEMILIK_PATH, BLOK_PATH)
    if not gdf.empty:
        gdf = cluster_points(gdf)
    update_to_google_sheet(gdf)
