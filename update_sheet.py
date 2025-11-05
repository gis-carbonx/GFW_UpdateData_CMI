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
LOG_SHEET_NAME = "Log_Update"

AOI_PATH = "data/aoi.json"
DESA_PATH = "data/Desa.json"
PEMILIK_PATH = "data/PemilikLahan.json"
BLOK_PATH = "data/blok.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def fetch_gfw_data_from_jan():
    wib = timezone(timedelta(hours=7))
    today = datetime.now(wib).strftime("%Y-%m-%d")
    start_date = "2025-01-01"

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
    SELECT longitude, latitude, gfw_integrated_alerts__date, gfw_integrated_alerts__confidence
    FROM results
    WHERE gfw_integrated_alerts__date >= '{start_date}' 
      AND gfw_integrated_alerts__date <= '{today}'
    """

    print(f"Mengambil data GFW dari {start_date} hingga {today}...")
    url = "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/latest/query"
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    body = {"geometry": geometry, "sql": sql}

    resp = requests.post(url, headers=headers, json=body)
    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text}")
        return pd.DataFrame()

    data = resp.json().get("data", [])
    if not data:
        print("Tidak ada data dari GFW.")
        return pd.DataFrame()

    df = pd.DataFrame(data).rename(columns={
        "gfw_integrated_alerts__date": "Integrated_Date",
        "gfw_integrated_alerts__confidence": "Integrated_Alert"
    })
    df["Integrated_Date"] = pd.to_datetime(df["Integrated_Date"], errors="coerce")

    print(f"Berhasil mengambil {len(df)} baris data dari API.")
    print(f"Tanggal data terbaru dari GFW API: {df['Integrated_Date'].max().date()}")
    return df

def clip_with_aoi(df, aoi_path):
    try:
        with open(aoi_path, "r") as f:
            aoi_geojson = json.load(f)
        aoi_polygon = shape(aoi_geojson["features"][0]["geometry"])
    except Exception as e:
        print(f"Gagal membaca AOI: {e}")
        return df

    inside = [
        row for _, row in df.iterrows()
        if aoi_polygon.contains(Point(row["longitude"], row["latitude"]))
    ]

    if not inside:
        print("Tidak ada titik dalam area AOI.")
        return pd.DataFrame()

    clipped_df = pd.DataFrame(inside)
    print(f"{len(clipped_df)} titik berada di dalam AOI.")
    print(f"Tanggal maksimum dalam AOI: {clipped_df['Integrated_Date'].max().date()}")
    return clipped_df

def intersect_with_geojson(df, desa_path, pemilik_path, blok_path):
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")

    desa = gpd.read_file(desa_path)[["nama_kel", "geometry"]]
    pemilik = gpd.read_file(pemilik_path)[["Owner", "geometry"]]
    blok = gpd.read_file(blok_path)[["Blok", "geometry"]]

    for layer in [desa, pemilik, blok]:
        if layer.crs is None:
            layer.set_crs("EPSG:4326", inplace=True)
        else:
            layer.to_crs("EPSG:4326", inplace=True)

    gdf = gpd.sjoin(gdf, desa, how="left", predicate="within").rename(columns={"nama_kel": "Desa"})
    gdf.drop(columns=["index_right"], inplace=True, errors="ignore")

    gdf = gpd.sjoin(gdf, pemilik, how="left", predicate="within")
    gdf.drop(columns=["index_right"], inplace=True, errors="ignore")

    gdf = gpd.sjoin(gdf, blok, how="left", predicate="within")
    gdf.drop(columns=["index_right"], inplace=True, errors="ignore")

    gdf = gdf.sort_values(by="Integrated_Date", ascending=True)
    print("Intersect selesai.")
    print(f"Tanggal maksimum setelah intersect: {gdf['Integrated_Date'].max().date()}")
    return gdf

def cluster_points_by_owner(gdf):
    print("Melakukan clustering titik berdasarkan Owner dan tanggal...")
    gdf = gdf.to_crs(epsg=32749)
    cluster_results = []

    for (owner, tanggal), group in gdf.groupby(["Owner", "Integrated_Date"]):
        if pd.isna(owner) or group.empty:
            continue

        group = group.copy()
        group["buffer"] = group.geometry.buffer(11)
        union_poly = unary_union(group["buffer"])
        if union_poly.is_empty:
            continue

        clusters = [union_poly] if union_poly.geom_type == "Polygon" else list(union_poly.geoms)
        tanggal_str = pd.to_datetime(tanggal).strftime("%Y-%m-%d")

        cluster_gdf = gpd.GeoDataFrame(geometry=clusters, crs=group.crs)
        cluster_gdf["Cluster_ID"] = [f"{owner}_{tanggal_str}_{str(i+1).zfill(3)}" for i in range(len(cluster_gdf))]

        centroid = cluster_gdf.geometry.centroid.to_crs(epsg=4326)
        cluster_gdf["Cluster_Y"] = centroid.y.round(5)
        cluster_gdf["Cluster_X"] = centroid.x.round(5)

        joined = gpd.sjoin(group, cluster_gdf, how="left", predicate="intersects")
        joined.drop(columns=["index_right"], inplace=True, errors="ignore")

        count = joined.groupby("Cluster_ID").size().reset_index(name="Jumlah_Titik")
        count["Luas_Ha"] = (count["Jumlah_Titik"] * 10 / 10000).round(4)

        merged = joined.merge(count, on="Cluster_ID", how="left")
        cluster_results.append(merged)

    if not cluster_results:
        return gdf.to_crs(4326)

    final = pd.concat(cluster_results, ignore_index=True)
    final = final.to_crs(4326)
    final["Luas"] = 10
    print(f"Clustering selesai ({len(final)} baris).")
    print(f"Tanggal maksimum setelah clustering: {final['Integrated_Date'].max()}")
    return final

def add_desa_cluster_column(gdf, desa_path):
    print("Menambahkan kolom Desa_Cluster berdasarkan koordinat Cluster_X dan Cluster_Y...")
    desa = gpd.read_file(desa_path)[["nama_kel", "geometry"]].to_crs(epsg=4326)

    cluster_points = gdf[["Cluster_ID", "Cluster_X", "Cluster_Y"]].drop_duplicates()
    cluster_points = gpd.GeoDataFrame(
        cluster_points,
        geometry=gpd.points_from_xy(cluster_points["Cluster_X"], cluster_points["Cluster_Y"]),
        crs="EPSG:4326"
    )

    joined = gpd.sjoin(cluster_points, desa, how="left", predicate="within")
    joined.rename(columns={"nama_kel": "Desa_Cluster"}, inplace=True)
    joined.drop(columns=["index_right"], inplace=True, errors="ignore")

    gdf = gdf.merge(joined[["Cluster_ID", "Desa_Cluster"]], on="Cluster_ID", how="left")
    print("Kolom Desa_Cluster berhasil ditambahkan.")
    return gdf

def overwrite_google_sheet(df):
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)

    latest_year = pd.to_datetime(df["Integrated_Date"], errors="coerce").dt.year.max()
    sheet_name = str(latest_year)

    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
        sheet.clear()
        print(f"Menulis data ke sheet '{sheet_name}'.")
    except gspread.exceptions.WorksheetNotFound:
        print(f"Sheet '{sheet_name}' belum ada, membuat sheet baru...")
        sheet = client.open_by_key(SPREADSHEET_ID).add_worksheet(title=sheet_name, rows=1000, cols=20)

    if df.empty:
        print("Tidak ada data baru untuk ditulis.")
        return

    df["Integrated_Date"] = pd.to_datetime(df["Integrated_Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.astype(str)

    header = list(df.columns)
    sheet.append_rows([header] + df.values.tolist(), value_input_option="USER_ENTERED")
    print(f"{len(df)} baris baru berhasil ditulis ke Google Sheet ({sheet_name}).")

def merge_sheets_to_db():
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)

    sheets_to_merge = ["2023", "2024", "2025"]
    all_data = []

    for name in sheets_to_merge:
        try:
            ws = sh.worksheet(name)
            rows = ws.get_all_records()
            if rows:
                all_data.extend(rows)
                print(f"✔ Data dari {name} ditambahkan ({len(rows)} baris)")
        except gspread.exceptions.WorksheetNotFound:
            print(f"⚠ Sheet {name} tidak ditemukan, dilewati.")

    if not all_data:
        print("Tidak ada data untuk digabungkan.")
        return

    df = pd.DataFrame(all_data)
    df = df.drop_duplicates().reset_index(drop=True)

    try:
        db_sheet = sh.worksheet("Db")
        db_sheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        db_sheet = sh.add_worksheet(title="Db", rows=1000, cols=20)

    db_sheet.append_rows([list(df.columns)] + df.values.tolist(), value_input_option="USER_ENTERED")
    print(f" Sheet 'Db' berhasil diperbarui ({len(df)} baris total).")
def update_log(start_date, latest_date):
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)

    try:
        log_sheet = client.open_by_key(SPREADSHEET_ID).worksheet(LOG_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        log_sheet = client.open_by_key(SPREADSHEET_ID).add_worksheet(title=LOG_SHEET_NAME, rows=10, cols=2)

    wib = timezone(timedelta(hours=7))
    now_wib = datetime.now(wib).strftime("%d/%m/%Y %H:%M")

    log_sheet.clear()
    log_sheet.append_rows([
        ["Note", "Last Update"],
        ["Update", now_wib]
    ], value_input_option="USER_ENTERED")

    print(f"Log diperbarui: Last Update {now_wib}")

if __name__ == "__main__":
    df = fetch_gfw_data_from_jan()

    if not df.empty:
        df = clip_with_aoi(df, AOI_PATH)
        if not df.empty:
            gdf = intersect_with_geojson(df, DESA_PATH, PEMILIK_PATH, BLOK_PATH)
            if not gdf.empty:
                gdf = cluster_points_by_owner(gdf)
                gdf = add_desa_cluster_column(gdf, DESA_PATH)
                overwrite_google_sheet(gdf)
                merge_sheets_to_db()
                update_log("2025-01-01", gdf["Integrated_Date"].max())
            else:
                print("Tidak ada hasil intersect dari data terbaru.")
        else:
            print("Tidak ada data dalam AOI.")
    else:
        print("Tidak ada data baru dari GFW.")
