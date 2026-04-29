import requests
import pandas as pd
import geopandas as gpd
import gspread
import json
from shapely.geometry import shape, Point
from shapely.ops import unary_union
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

def fetch_gfw_data_from_jan():
    wib = timezone(timedelta(hours=7))
    today = datetime.now(wib).strftime("%Y-%m-%d")
    start_date = "2026-01-01"

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

    sql = f"""
    SELECT longitude, latitude,
           gfw_integrated_alerts__date,
           gfw_integrated_alerts__confidence
    FROM results
    WHERE gfw_integrated_alerts__date >= '{start_date}'
      AND gfw_integrated_alerts__date <= '{today}'
    """

    url = "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/latest/query"
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    body = {"geometry": geometry, "sql": sql}

    print(f"Fetch GFW: {start_date} → {today}")
    resp = requests.post(url, headers=headers, json=body)

    if resp.status_code != 200:
        print(f"Error API: {resp.status_code}")
        return pd.DataFrame()

    data = resp.json().get("data", [])
    if not data:
        print("Tidak ada data.")
        return pd.DataFrame()

    df = pd.DataFrame(data).rename(columns={
        "gfw_integrated_alerts__date": "Integrated_Date",
        "gfw_integrated_alerts__confidence": "Integrated_Alert"
    })

    df["Integrated_Date"] = pd.to_datetime(df["Integrated_Date"], errors="coerce")

    print(f"Jumlah data: {len(df)}")
    return df

def clip_with_aoi(df, aoi_path):
    with open(aoi_path, "r") as f:
        aoi_geojson = json.load(f)

    aoi_polygon = shape(aoi_geojson["features"][0]["geometry"])

    filtered = [
        row for _, row in df.iterrows()
        if aoi_polygon.contains(Point(row["longitude"], row["latitude"]))
    ]

    if not filtered:
        return pd.DataFrame()

    result = pd.DataFrame(filtered)
    print(f"Inside AOI: {len(result)}")
    return result

def intersect_with_geojson(df, desa_path, pemilik_path, blok_path):
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    )

    desa = gpd.read_file(desa_path)[["nama_kel", "geometry"]]
    pemilik = gpd.read_file(pemilik_path)[["Owner", "geometry"]]
    blok = gpd.read_file(blok_path)[["Blok", "geometry"]]

    for layer in [desa, pemilik, blok]:
        layer.to_crs("EPSG:4326", inplace=True)

    gdf = gpd.sjoin(gdf, desa, how="left", predicate="within") \
             .rename(columns={"nama_kel": "Desa"}) \
             .drop(columns=["index_right"], errors="ignore")

    gdf = gpd.sjoin(gdf, pemilik, how="left", predicate="within") \
             .drop(columns=["index_right"], errors="ignore")

    gdf = gpd.sjoin(gdf, blok, how="left", predicate="within") \
             .drop(columns=["index_right"], errors="ignore")

    print("Intersect selesai")
    return gdf

def cluster_points_by_owner(gdf):
    print("Clustering...")

    gdf = gdf.to_crs(epsg=32749)
    results = []

    for (owner, tanggal), group in gdf.groupby(["Owner", "Integrated_Date"]):
        if pd.isna(owner) or group.empty:
            continue

        group = group.copy()
        group["buffer"] = group.geometry.buffer(11)

        union = unary_union(group["buffer"])
        if union.is_empty:
            continue

        clusters = [union] if union.geom_type == "Polygon" else list(union.geoms)

        cluster_gdf = gpd.GeoDataFrame(geometry=clusters, crs=group.crs)
        tanggal_str = pd.to_datetime(tanggal).strftime("%Y-%m-%d")

        cluster_gdf["Cluster_ID"] = [
            f"{owner}_{tanggal_str}_{str(i+1).zfill(3)}"
            for i in range(len(cluster_gdf))
        ]

        centroid = cluster_gdf.geometry.centroid.to_crs(epsg=4326)
        cluster_gdf["Cluster_Y"] = centroid.y.round(5)
        cluster_gdf["Cluster_X"] = centroid.x.round(5)

        joined = gpd.sjoin(group, cluster_gdf, how="left", predicate="intersects") \
                    .drop(columns=["index_right"], errors="ignore")

        count = joined.groupby("Cluster_ID").size().reset_index(name="Jumlah_Titik")
        count["Luas_Ha"] = (count["Jumlah_Titik"] * 10 / 10000).round(4)

        merged = joined.merge(count, on="Cluster_ID", how="left")
        results.append(merged)

    if not results:
        return gdf.to_crs(4326)

    final = pd.concat(results, ignore_index=True).to_crs(4326)
    final["Luas"] = 10

    print(f"Cluster selesai: {len(final)} baris")
    return final

def add_desa_cluster_column(gdf, desa_path):
    desa = gpd.read_file(desa_path)[["nama_kel", "geometry"]].to_crs(epsg=4326)

    cluster_points = gdf[["Cluster_ID", "Cluster_X", "Cluster_Y"]].drop_duplicates()

    cluster_points = gpd.GeoDataFrame(
        cluster_points,
        geometry=gpd.points_from_xy(cluster_points["Cluster_X"], cluster_points["Cluster_Y"]),
        crs="EPSG:4326"
    )

    joined = gpd.sjoin(cluster_points, desa, how="left", predicate="within") \
                .rename(columns={"nama_kel": "Desa_Cluster"}) \
                .drop(columns=["index_right"], errors="ignore")

    gdf = gdf.merge(joined[["Cluster_ID", "Desa_Cluster"]], on="Cluster_ID", how="left")

    return gdf

def overwrite_google_sheet(df):
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)

    latest_year = pd.to_datetime(df["Integrated_Date"]).dt.year.max()
    sheet_name = str(latest_year)

    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
        sheet.clear()
    except:
        sheet = client.open_by_key(SPREADSHEET_ID).add_worksheet(
            title=sheet_name, rows=1000, cols=20
        )

    keep_cols = [
        "latitude","longitude","Integrated_Date","Integrated_Alert",
        "Desa","Owner","Blok","Cluster_ID",
        "Cluster_Y","Cluster_X","Desa_Cluster",
        "Jumlah_Titik","Luas_Ha","Luas"
    ]

    df = df[keep_cols].copy()
    df = df.replace([np.inf, -np.inf], np.nan).fillna("")
    df["Integrated_Date"] = pd.to_datetime(df["Integrated_Date"]).dt.strftime("%Y-%m-%d")

    sheet.append_rows([list(df.columns)] + df.astype(str).values.tolist())

    print(f"Upload selesai: {len(df)} baris ke sheet {sheet_name}")


def update_log():
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)

    try:
        log_sheet = client.open_by_key(SPREADSHEET_ID).worksheet(LOG_SHEET_NAME)
    except:
        log_sheet = client.open_by_key(SPREADSHEET_ID).add_worksheet(
            title=LOG_SHEET_NAME, rows=10, cols=2
        )

    wib = timezone(timedelta(hours=7))
    now = datetime.now(wib).strftime("%Y-%m-%d %H:%M:%S")

    log_sheet.clear()
    log_sheet.append_rows([
        ["Note", "Last Update"],
        ["Update", now]
    ])

    print(f"Log updated: {now}")


if __name__ == "__main__":
    df = fetch_gfw_data_from_jan()

    if df.empty:
        print("Tidak ada data GFW")
        exit()

    df = clip_with_aoi(df, AOI_PATH)
    if df.empty:
        print("Tidak ada data dalam AOI")
        exit()

    gdf = intersect_with_geojson(df, DESA_PATH, PEMILIK_PATH, BLOK_PATH)
    if gdf.empty:
        print("Intersect kosong")
        exit()

    gdf = cluster_points_by_owner(gdf)
    gdf = add_desa_cluster_column(gdf, DESA_PATH)

    overwrite_google_sheet(gdf)
    update_log()
