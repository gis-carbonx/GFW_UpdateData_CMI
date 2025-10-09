import pandas as pd
import geopandas as gpd
import requests
import io
from shapely.ops import unary_union
from google.oauth2.service_account import Credentials
from gspread import authorize

SERVICE_ACCOUNT_FILE = "service_account.json"
SPREADSHEET_NAME = "GFW_Alerts"
WORKSHEET_NAME = "Data"
AOI_PATH = "aoi.geojson"
DESA_PATH = "Desa.geojson"
PEMILIK_PATH = "pemilik.geojson"
BLOK_PATH = "blok.geojson"

API_URL = "https://data-api.globalforestwatch.org/dataset/integrated_deforestation_alerts/latest/download/csv"

def fetch_gfw_data():
    print("Mengambil data GFW...")
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.text))

        df = df.rename(columns={
            "wur_radd_alerts__date": "date",
            "wur_radd_alerts__confidence": "confidence",
            "nama_kel": "Desa"
        })

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date", ascending=True)

        print(f"Berhasil mengambil {len(df)} baris data GFW.")
        return df

    except Exception as e:
        print(f"Gagal mengambil data GFW: {e}")
        return pd.DataFrame()

def clip_with_aoi(df, aoi_path):
    print("Melakukan clip data dengan AOI...")
    try:
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
        aoi = gpd.read_file(aoi_path).to_crs("EPSG:4326")
        clipped = gpd.sjoin(gdf, aoi, how="inner", predicate="intersects")

        clipped = clipped.drop(columns=["index_right"], errors="ignore")
        print(f"{len(clipped)} titik berada dalam AOI.")
        return clipped

    except Exception as e:
        print(f"Gagal melakukan clip AOI: {e}")
        return df

def intersect_with_geojson(df, desa_path, pemilik_path, blok_path):
    print("Melakukan intersect dengan data tambahan...")

    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")

    desa = gpd.read_file(desa_path).to_crs("EPSG:4326")
    gdf = gpd.sjoin(gdf, desa[["geometry", "Desa"]], how="left", predicate="intersects")

    pemilik = gpd.read_file(pemilik_path).to_crs("EPSG:4326")
    gdf = gpd.sjoin(gdf, pemilik[["geometry", "Owner"]], how="left", predicate="intersects")

    blok = gpd.read_file(blok_path).to_crs("EPSG:4326")
    gdf = gpd.sjoin(gdf, blok[["geometry", "Blok"]], how="left", predicate="intersects")

    gdf = gdf.drop(columns=["index_right", "index__pemilik", "index__blok"], errors="ignore")

    print("Intersect selesai.")
    return gdf

def cluster_points(df):
    print("Melakukan clustering titik...")

    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
    gdf = gdf.to_crs(epsg=32749)  
    gdf["buffer"] = gdf.geometry.buffer(5.6)

    union_poly = unary_union(gdf["buffer"])

    if union_poly.geom_type == "Polygon":
        cluster_polys = [union_poly]
    else:
        cluster_polys = list(union_poly.geoms)

    cluster_gdf = gpd.GeoDataFrame(geometry=cluster_polys, crs=gdf.crs)
    cluster_gdf["Cluster_ID"] = range(1, len(cluster_gdf) + 1)

    joined = gpd.sjoin(gdf, cluster_gdf, how="left", predicate="intersects")

    cluster_count = joined.groupby("Cluster_ID").size().reset_index(name="Jumlah_Titik")
    cluster_count["Luas_Ha"] = cluster_count["Jumlah_Titik"] * (11.2 * 11.2) / 10_000

    joined = joined.merge(cluster_count, on="Cluster_ID", how="left")

    joined = joined.to_crs("EPSG:4326")
    joined = joined.drop(columns=["buffer", "geometry"], errors="ignore")

    print(f"Clustering selesai: {len(cluster_count)} cluster terbentuk.")
    return joined

def update_to_google_sheet(df):
    print("Mengunggah ke Google Sheet...")

    try:
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = authorize(creds)

        spreadsheet = client.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)

        worksheet.clear()
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())

        print("Data berhasil diunggah ke Google Sheet.")
    except Exception as e:
        print(f"Gagal mengunggah ke Google Sheet: {e}")


if __name__ == "__main__":
    df = fetch_gfw_data()

    if not df.empty:
        df = clip_with_aoi(df, AOI_PATH)
        if not df.empty:
            df = intersect_with_geojson(df, DESA_PATH, PEMILIK_PATH, BLOK_PATH)
            df = cluster_points(df)
            update_to_google_sheet(df)
