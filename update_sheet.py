import requests
import pandas as pd
import geopandas as gpd
import gspread
import json
from shapely.geometry import shape, Point
from shapely.ops import unary_union
from google.oauth2.service_account import Credentials
from datetime import datetime

API_KEY = "912b99d5-ecc2-47aa-86fe-1f986b9b070b"
SPREADSHEET_ID = "1UW3uOFcLr4AQFBp_VMbEXk37_Vb5DekHU-_9QSkskCo"
SHEET_NAME = "Sheet1"

AOI_PATH = "data/aoi.json"
DESA_PATH = "data/Desa.json"
PEMILIK_PATH = "data/PemilikLahan.json"
BLOK_PATH = "data/blok.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def fetch_gfw_data():
    """Fetch Integrated Alert dari GFW API"""
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

    start_date = "2023-01-01"
    end_date = "2023-12-31"

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
    """Clip dataframe points menggunakan AOI polygon"""
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

    gdf = gpd.sjoin(gdf, desa, how="left", predicate="within")
    gdf.drop(columns=[col for col in gdf.columns if "index_right" in col], inplace=True, errors="ignore")

    gdf = gpd.sjoin(gdf, pemilik, how="left", predicate="within")
    gdf.drop(columns=[col for col in gdf.columns if "index_right" in col], inplace=True, errors="ignore")

    gdf = gpd.sjoin(gdf, blok, how="left", predicate="within")
    gdf.drop(columns=[col for col in gdf.columns if "index_right" in col], inplace=True, errors="ignore")

    gdf = gdf.rename(columns={"nama_kel": "Desa"})
    gdf = gdf.loc[:, ~gdf.columns.str.contains("^index")]
    gdf = gdf.sort_values(by="Integrated_Date", ascending=True)
    print("Intersect selesai: kolom Desa, Owner, dan Blok berhasil ditambahkan.")
    return gdf


def cluster_points_by_owner(gdf):
    """Cluster titik bertampalan per Owner dan tanggal dengan centroid"""
    print("Melakukan clustering titik berdasarkan Owner dan tanggal...")

    gdf = gdf.to_crs(epsg=32749)
    cluster_results = []

    for (owner, tanggal), group in gdf.groupby(["Owner", "Integrated_Date"]):
        if pd.isna(owner) or group.empty:
            continue

        group = group.reset_index(drop=True)
        group["buffer"] = group.geometry.buffer(11)

        union_poly = unary_union(group["buffer"])
        if union_poly.is_empty:
            continue

        clusters = [union_poly] if union_poly.geom_type == "Polygon" else list(union_poly.geoms)
        tanggal_str = pd.to_datetime(tanggal).strftime("%Y-%m-%d")

        cluster_gdf = gpd.GeoDataFrame(geometry=clusters, crs=group.crs)
        cluster_gdf["Cluster_ID"] = [
            f"{owner}_{tanggal_str}_{str(i+1).zfill(3)}" for i in range(len(cluster_gdf))
        ]
        cluster_gdf["Cluster_X"] = cluster_gdf.centroid.x
        cluster_gdf["Cluster_Y"] = cluster_gdf.centroid.y

        joined = gpd.sjoin(group, cluster_gdf, how="left", predicate="intersects")
        joined.drop(columns=[col for col in joined.columns if "index_right" in col], inplace=True, errors="ignore")

        cluster_count = joined.groupby("Cluster_ID").size().reset_index(name="Jumlah_Titik")
        cluster_count["Luas_Ha"] = (cluster_count["Jumlah_Titik"] * 10 / 10000).round(4)

        merged = joined.merge(cluster_count, on="Cluster_ID", how="left")
        merged = merged.merge(cluster_gdf[["Cluster_ID", "Cluster_X", "Cluster_Y"]], on="Cluster_ID", how="left")

        cluster_results.append(merged)

    if not cluster_results:
        print("Tidak ada cluster terbentuk.")
        return gdf.to_crs(4326)

    final_gdf = pd.concat(cluster_results, ignore_index=True)
    final_gdf = final_gdf.to_crs(epsg=4326)
    final_gdf["Cluster_X"] = final_gdf["Cluster_X"].round(6)
    final_gdf["Cluster_Y"] = final_gdf["Cluster_Y"].round(6)
    final_gdf.drop(columns=["buffer", "geometry"], inplace=True, errors="ignore")

    desa = gpd.read_file(DESA_PATH)[["nama_kel", "geometry"]].to_crs(epsg=4326)
    cluster_points = gpd.GeoDataFrame(
        final_gdf.drop_duplicates("Cluster_ID"),
        geometry=gpd.points_from_xy(final_gdf["Cluster_X"], final_gdf["Cluster_Y"]),
        crs="EPSG:4326"
    )
    desa_join = gpd.sjoin(cluster_points, desa, how="left", predicate="within")
    desa_map = desa_join[["Cluster_ID", "nama_kel"]].rename(columns={"nama_kel": "Desa_Cluster"})

    final_gdf = final_gdf.merge(desa_map, on="Cluster_ID", how="left")
    final_gdf = final_gdf.sort_values(by=["Owner", "Integrated_Date"]).reset_index(drop=True)

    print(f"Clustering selesai untuk {final_gdf['Owner'].nunique()} pemilik lahan, dengan centroid dan desa cluster ditambahkan.")
    return final_gdf


def update_to_google_sheet(df):
    """Update Google Sheet dengan format tanggal dikenali"""
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

    for col in df.columns:
        df[col] = df[col].astype(str)

    values = [df.columns.values.tolist()] + df.values.tolist()
    sheet.update(values, value_input_option="USER_ENTERED")
    print(f"{len(df)} baris Integrated Alert berhasil dikirim ke Google Sheet dengan centroid dan desa cluster.")


if __name__ == "__main__":
    df = fetch_gfw_data()
    if not df.empty:
        df = clip_with_aoi(df, AOI_PATH)
    if not df.empty:
        gdf = intersect_with_geojson(df, DESA_PATH, PEMILIK_PATH, BLOK_PATH)
    if not gdf.empty:
        gdf = cluster_points_by_owner(gdf)
    update_to_google_sheet(gdf)
