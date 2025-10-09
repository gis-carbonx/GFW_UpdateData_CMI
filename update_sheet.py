import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import unary_union
from shapely.strtree import STRtree
from google.oauth2.service_account import Credentials
import gspread
import os

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

    gdf["buffer"] = gdf.geometry.buffer(11.2)

    buffers = list(gdf["buffer"])
    cluster_id = [-1] * len(buffers)
    current_cluster = 0

    tree = STRtree(buffers)
    geom_to_index = {id(geom): i for i, geom in enumerate(buffers)}

    for i, geom in enumerate(buffers):
        if cluster_id[i] != -1:
            continue

        group = set()
        to_check = [geom]

        while to_check:
            g = to_check.pop()
            for other in tree.query(g):
                j = geom_to_index[id(other)]
                if j not in group and g.intersects(other):
                    group.add(j)
                    to_check.append(other)

        for j in group:
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
    gdf["luas_ha"] = gdf["cluster"].map(lambda x: cluster_areas[x] / 10000)

    gdf = gdf.to_crs(epsg=4326)
    print("Berhasil menghitung luas tiap cluster.")
    return gdf

def spatial_join_with_layers(gdf_points):
    print("🔹 Menggabungkan dengan data referensi...")

    desa_path = "data/Desa.json"
    pemilik_path = "data/PemilikLahan.json"
    blok_path = "data/blok.json"

    gdf_desa = gpd.read_file(desa_path).to_crs("EPSG:4326")
    gdf_pemilik = gpd.read_file(pemilik_path).to_crs("EPSG:4326")
    gdf_blok = gpd.read_file(blok_path).to_crs("EPSG:4326")

    lulc_url = "https://drive.google.com/uc?export=download&id=1uy1VJruyiwsZBcdv5YYRTI9EcAWZVB2O"
    lulc_path = "data/LULC.json"
    os.makedirs("data", exist_ok=True)
    if not os.path.exists(lulc_path):
        r = requests.get(lulc_url)
        with open(lulc_path, "wb") as f:
            f.write(r.content)
    gdf_lulc = gpd.read_file(lulc_path).to_crs("EPSG:4326")

    gdf_join = gpd.sjoin(gdf_points, gdf_desa[["nama_kel", "geometry"]], predicate="within").drop(columns=["index_right"])
    gdf_join = gpd.sjoin(gdf_join, gdf_pemilik[["Owner", "geometry"]], predicate="within").drop(columns=["index_right"])
    gdf_join = gpd.sjoin(gdf_join, gdf_blok[["Blok", "geometry"]], predicate="within").drop(columns=["index_right"])
    gdf_join = gpd.sjoin(gdf_join, gdf_lulc[["Class23", "geometry"]], predicate="within").drop(columns=["index_right"])

    gdf_result = gdf_join.rename(columns={
        "nama_kel": "Desa",
        "Owner": "Owner",
        "Class23": "Penutup Lahan",
    })

    return gdf_result

def update_to_google_sheet(df):
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

    sheet.clear()
    if df.empty:
        sheet.update([["Tidak ada data ditemukan."]])
        print("Sheet diperbarui tanpa data.")
        return

    df = df.astype(str)
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    print(f"{len(df)} baris berhasil dikirim ke Google Sheet.")

if __name__ == "__main__":
    df = fetch_gfw_data()
    if not df.empty:
        gdf_points = calculate_cluster_area(df)
        gdf_result = spatial_join_with_layers(gdf_points)

        df_upload = gdf_result[[
            "longitude", "latitude", "wur_radd_alerts__date",
            "wur_radd_alerts__confidence", "Desa", "Owner",
            "Blok", "Penutup Lahan", "luas_ha"
        ]].copy()

        update_to_google_sheet(df_upload)
