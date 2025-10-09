import requests
import pandas as pd
import gspread
import json
import geopandas as gpd
from shapely.geometry import shape, Point, box
from shapely.ops import unary_union
from google.oauth2.service_account import Credentials

API_KEY = "912b99d5-ecc2-47aa-86fe-1f986b9b070b"
SPREADSHEET_ID = "1UW3uOFcLr4AQFBp_VMbEXk37_Vb5DekHU-_9QSkskCo"
SHEET_NAME = "Sheet1"
AOI_PATH = "data/aoi.json"

CELL_SIZE = 11.2  # meter
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def fetch_gfw_data():
    """Ambil data RADD alerts dari GFW API"""
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
    """Potong data titik berdasarkan AOI"""
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


def cluster_points(df):
    """Kelompokkan titik bertampalan (11.2 m persegi) menjadi satu hamparan dan hitung luas totalnya"""
    if df.empty:
        return df

    print("Melakukan clustering titik dan perhitungan luas...")

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
        crs="EPSG:4326"
    )

    gdf = gdf.to_crs(gdf.estimate_utm_crs())

    half = CELL_SIZE / 2

    gdf["square"] = gdf.geometry.apply(lambda p: box(p.x - half, p.y - half, p.x + half, p.y + half))

    gdf_squares = gpd.GeoDataFrame(geometry=gdf["square"])
    gdf_squares["id"] = gdf.index

    sindex = gdf_squares.sindex
    neighbors = {i: set() for i in gdf_squares.index}

    for i, geom in gdf_squares.geometry.items():
        possible = list(sindex.intersection(geom.bounds))
        for j in possible:
            if i != j and geom.intersects(gdf_squares.geometry[j]):
                neighbors[i].add(j)
                neighbors[j].add(i)

    visited = set()
    clusters = {}
    cluster_id = 0
    for i in gdf_squares.index:
        if i not in visited:
            stack = [i]
            members = set()
            while stack:
                node = stack.pop()
                if node not in visited:
                    visited.add(node)
                    members.add(node)
                    stack.extend(neighbors[node] - visited)
            clusters[cluster_id] = members
            cluster_id += 1

    id_to_cluster = {idx: cid for cid, members in clusters.items() for idx in members}
    gdf["cluster_id"] = gdf.index.map(id_to_cluster)

    cluster_areas = []
    for cid, members in clusters.items():
        union_poly = unary_union(gdf.loc[list(members), "square"].tolist())
        area_m2 = union_poly.area
        cluster_areas.append({"cluster_id": cid, "area_m2": area_m2, "area_ha": area_m2 / 10000})

    df_area = pd.DataFrame(cluster_areas)
    gdf = gdf.merge(df_area, on="cluster_id", how="left")

    gdf = gdf.to_crs("EPSG:4326")

    print(f"{len(df_area)} cluster ditemukan.")
    return pd.DataFrame(gdf.drop(columns=["geometry", "square"]))


def update_to_google_sheet(df):
    """Kirim hasil ke Google Sheet"""
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
        if not df.empty:
            df = cluster_points(df)
    update_to_google_sheet(df)
