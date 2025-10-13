import requests
import pandas as pd
import geopandas as gpd
import gspread
import json
from shapely.geometry import shape, Point
from shapely.ops import unary_union
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone

API_KEY = "912b99d5-ecc2-47aa-86fe-1f986b9b070b"
SPREADSHEET_ID = "1UW3uOFcLr4AQFBp_VMbEXk37_Vb5DekHU-_9QSkskCo"
SHEET_NAME = "Sheet1"
LOG_SHEET_NAME = "Log_Update"

AOI_PATH = "data/aoi.json"
DESA_PATH = "data/Desa.json"
PEMILIK_PATH = "data/PemilikLahan.json"
BLOK_PATH = "data/blok.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_last_update_date():
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    try:
        log_sheet = client.open_by_key(SPREADSHEET_ID).worksheet(LOG_SHEET_NAME)
        data = log_sheet.get_all_values()
        if len(data) > 1 and len(data[1]) > 1:
            last_update_str = data[1][1]
            last_update = datetime.strptime(last_update_str, "%Y-%m-%d %H:%M:%S")
            return last_update.date()
    except Exception as e:
        print(f"Tidak menemukan log sebelumnya: {e}")
    return datetime(2024, 1, 1).date()

def fetch_gfw_data_since(last_date):
    wib = timezone(timedelta(hours=7))
    today = datetime.now(wib).strftime("%Y-%m-%d")

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
    SELECT 
        longitude, 
        latitude, 
        gfw_integrated_alerts__date,
        gfw_integrated_alerts__confidence
    FROM results
    WHERE gfw_integrated_alerts__date > '{last_date}' 
      AND gfw_integrated_alerts__date <= '{today}'
    """

    print(f"Mengambil data GFW sejak {last_date} hingga {today}...")
    url = "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/latest/query"
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    body = {"geometry": geometry, "sql": sql}

    resp = requests.post(url, headers=headers, json=body)
    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text}")
        return pd.DataFrame()

    data = resp.json().get("data", [])
    if not data:
        print("Tidak ada data baru dari GFW.")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df = df.rename(columns={
        "gfw_integrated_alerts__date": "Integrated_Date",
        "gfw_integrated_alerts__confidence": "Integrated_Alert"
    })
    df["Integrated_Date"] = pd.to_datetime(df["Integrated_Date"], errors="coerce")
    print(f"Berhasil mengambil {len(df)} baris data baru dari API.")
    return df

def clip_with_aoi(df, aoi_path):
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
    gdf = gpd.sjoin(gdf, pemilik, how="left", predicate="within")
    gdf = gpd.sjoin(gdf, blok, how="left", predicate="within")

    gdf = gdf.rename(columns={"nama_kel": "Desa"})
    gdf = gdf.loc[:, ~gdf.columns.str.contains("^index")]
    gdf = gdf.sort_values(by="Integrated_Date", ascending=True)
    print("Intersect selesai.")
    return gdf

def cluster_points_by_owner(gdf):
    print("Melakukan clustering titik berdasarkan Owner dan tanggal...")
    gdf = gdf.to_crs(epsg=32749)
    cluster_results = []

    for (owner, tanggal), group in gdf.groupby(["Owner", "Integrated_Date"]):
        if pd.isna(owner) or group.empty:
            continue

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

        cluster_gdf_centroid = cluster_gdf.copy()
        cluster_gdf_centroid["geometry"] = cluster_gdf.geometry.centroid
        cluster_gdf_centroid = cluster_gdf_centroid.to_crs(epsg=4326)

        cluster_gdf["Cluster_Y"] = cluster_gdf_centroid.geometry.y.round(5)
        cluster_gdf["Cluster_X"] = cluster_gdf_centroid.geometry.x.round(5)

        joined = gpd.sjoin(group, cluster_gdf, how="left", predicate="intersects")
        cluster_count = joined.groupby("Cluster_ID").size().reset_index(name="Jumlah_Titik")
        cluster_count["Luas_Ha"] = (cluster_count["Jumlah_Titik"] * 10 / 10000).round(4)

        merged = joined.merge(cluster_count, on="Cluster_ID", how="left")
        cluster_results.append(merged)

    if not cluster_results:
        return gdf.to_crs(4326)

    final_gdf = pd.concat(cluster_results, ignore_index=True).to_crs(epsg=4326)
    final_gdf.drop(columns=["buffer", "geometry"], inplace=True, errors="ignore")
    final_gdf = final_gdf.sort_values(by=["Owner", "Integrated_Date"]).reset_index(drop=True)
    print(f"Clustering selesai ({len(final_gdf)} baris).")
    return final_gdf

def append_unique_to_google_sheet(df):
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

    if df.empty:
        print("Tidak ada data baru untuk ditambahkan.")
        return

    existing = pd.DataFrame(sheet.get_all_records())
    if not existing.empty and "longitude" in existing.columns:
        merged = pd.concat([existing, df], ignore_index=True)
        merged.drop_duplicates(subset=["longitude", "latitude", "Integrated_Date"], inplace=True)
        new_rows = merged[~merged.index.isin(existing.index)]
    else:
        new_rows = df

    if new_rows.empty:
        print("Semua data sudah ada, tidak ada yang baru.")
        return

    new_rows["Integrated_Date"] = pd.to_datetime(new_rows["Integrated_Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    new_rows = new_rows.astype(str)

    sheet.append_rows(new_rows.values.tolist(), value_input_option="USER_ENTERED")
    print(f"{len(new_rows)} baris baru berhasil ditambahkan ke Google Sheet.")

def update_last_run_log():
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    try:
        log_sheet = client.open_by_key(SPREADSHEET_ID).worksheet(LOG_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        log_sheet = client.open_by_key(SPREADSHEET_ID).add_worksheet(title=LOG_SHEET_NAME, rows=10, cols=2)
        log_sheet.update([["Last_Update", "Datetime_WIB"]])

    wib = timezone(timedelta(hours=7))
    now_wib = datetime.now(wib).strftime("%Y-%m-%d %H:%M:%S")
    log_sheet.update("A2:B2", [["Update_Run", now_wib]], value_input_option="USER_ENTERED")
    print(f"Log diperbarui: {now_wib}")

if __name__ == "__main__":
    last_date = get_last_update_date()
    df = fetch_gfw_data_since(last_date)

    if not df.empty:
        df = clip_with_aoi(df, AOI_PATH)

        if not df.empty:
            gdf = intersect_with_geojson(df, DESA_PATH, PEMILIK_PATH, BLOK_PATH)

            if not gdf.empty:
                gdf = cluster_points_by_owner(gdf)
                append_unique_to_google_sheet(gdf)
                print("Data baru berhasil ditambahkan ke Google Sheet.")
            else:
                print("Tidak ada hasil intersect dari data terbaru.")
        else:
            print("Tidak ada data dalam area AOI.")
    else:
        print("Tidak ada data baru dari GFW.")
    update_last_run_log()

