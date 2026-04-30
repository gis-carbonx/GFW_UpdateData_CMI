import requests
import pandas as pd
import geopandas as gpd
import gspread
import json
from shapely.geometry import shape
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone
import numpy as np

API_KEY = "912b99d5-ecc2-47aa-86fe-1f986b9b070b"
SPREADSHEET_ID = "1UW3uOFcLr4AQFBp_VMbEXk37_Vb5DekHU-_9QSkskCo"
LOG_SHEET_NAME = "Log_Update"

AOI_PATH = "data/aoi.json"
DESA_PATH = "data/Desa.json"
PEMILIK_PATH = "data/PemilikLahan.json"
BLOK_PATH = "data/blok.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def load_aoi_geometry(aoi_path):
    with open(aoi_path, "r") as f:
        aoi_geojson = json.load(f)
    feature    = aoi_geojson["features"][0]
    geom_dict  = feature["geometry"]
    geom_shape = shape(geom_dict)
    print(f"AOI dimuat: {aoi_path} | tipe: {geom_dict['type']}")
    return geom_shape, geom_dict


# ─── Fetch integrated deforestation alerts (GLAD-L/S2/RADD) ──────────────────
def fetch_integrated_alerts(aoi_geom_dict):
    wib        = timezone(timedelta(hours=7))
    today      = datetime.now(wib).strftime("%Y-%m-%d")
    start_date = "2026-01-01"

    sql = f"""
    SELECT
        longitude,
        latitude,
        gfw_integrated_alerts__date,
        gfw_integrated_alerts__confidence,
        umd_glad_landsat_alerts__confidence,
        umd_glad_sentinel2_alerts__confidence,
        wur_radd_alerts__confidence
    FROM results
    WHERE gfw_integrated_alerts__date >= '{start_date}'
      AND gfw_integrated_alerts__date <= '{today}'
    """

    url     = "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/latest/query"
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    body    = {"geometry": aoi_geom_dict, "sql": sql}

    print(f"\nFetching Integrated Alerts (GLAD-L/S2/RADD): {start_date} → {today} ...")
    resp = requests.post(url, headers=headers, json=body)

    if resp.status_code != 200:
        print(f"[ERROR {resp.status_code}]: {resp.text[:300]}")
        return pd.DataFrame()

    data = resp.json().get("data", [])
    if not data:
        print("Tidak ada data integrated alerts.")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df.rename(columns={
        "gfw_integrated_alerts__date":           "Date",
        "gfw_integrated_alerts__confidence":     "Conf_Integrated",
        "umd_glad_landsat_alerts__confidence":   "Conf_GLADL",
        "umd_glad_sentinel2_alerts__confidence": "Conf_GLADS2",
        "wur_radd_alerts__confidence":           "Conf_RADD",
    }, inplace=True)

    df["Date"]       = pd.to_datetime(df["Date"], errors="coerce")
    df["Alert_Type"] = "Deforestation"

    print(f"[OK] {len(df)} baris | terbaru: {df['Date'].max().date()}")
    print("\nRingkasan Conf_Integrated:")
    print(df["Conf_Integrated"].value_counts().to_string())
    return df


# ─── Fetch DIST-ALERT (umd_glad_dist_alerts) ─────────────────────────────────
def fetch_dist_alerts(aoi_geom_dict):
    wib        = timezone(timedelta(hours=7))
    today      = datetime.now(wib).strftime("%Y-%m-%d")
    start_date = "2026-01-01"

    sql = f"""
    SELECT
        longitude,
        latitude,
        umd_glad_landsat_alerts__date,
        umd_glad_landsat_alerts__confidence
    FROM results
    WHERE umd_glad_landsat_alerts__date >= '{start_date}'
      AND umd_glad_landsat_alerts__date <= '{today}'
    """

    url     = "https://data-api.globalforestwatch.org/dataset/umd_glad_dist_alerts/latest/query"
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    body    = {"geometry": aoi_geom_dict, "sql": sql}

    print(f"\nFetching DIST-ALERT (all-ecosystem disturbance): {start_date} → {today} ...")
    resp = requests.post(url, headers=headers, json=body)

    if resp.status_code != 200:
        print(f"[ERROR {resp.status_code}]: {resp.text[:300]}")
        return pd.DataFrame()

    data = resp.json().get("data", [])
    if not data:
        print("Tidak ada data DIST-ALERT.")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df.rename(columns={
        "umd_glad_landsat_alerts__date":       "Date",
        "umd_glad_landsat_alerts__confidence": "Conf_Integrated",
    }, inplace=True)

    df["Date"]       = pd.to_datetime(df["Date"], errors="coerce")
    df["Alert_Type"] = "Disturbance"

    # Kolom confidence sensor lain dikosongkan — tidak ada di dataset ini
    df["Conf_GLADL"]  = ""
    df["Conf_GLADS2"] = ""
    df["Conf_RADD"]   = ""

    print(f"[OK] {len(df)} baris | terbaru: {df['Date'].max().date()}")
    print("\nRingkasan Conf DIST-ALERT:")
    print(df["Conf_Integrated"].value_counts().to_string())
    return df


# ─── Intersect dengan layer spasial ──────────────────────────────────────────
def intersect_with_geojson(df, desa_path, pemilik_path, blok_path):
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    )

    desa    = gpd.read_file(desa_path)[["nama_kel", "geometry"]]
    pemilik = gpd.read_file(pemilik_path)[["Owner", "geometry"]]
    blok    = gpd.read_file(blok_path)[["Blok", "geometry"]]

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

    gdf = gdf.drop(columns=["geometry"], errors="ignore")

    print(f"\nIntersect selesai: {len(gdf)} baris.")
    print(f"Tanggal maksimum: {pd.to_datetime(gdf['Date']).max().date()}")
    return gdf


# ─── Tulis ke Google Sheet ────────────────────────────────────────────────────
def overwrite_google_sheet(df_integrated, df_dist):
    creds  = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sh     = client.open_by_key(SPREADSHEET_ID)

    keep_cols = [
        "latitude", "longitude", "Date",
        "Alert_Type", "Conf_Integrated",
        "Conf_GLADL", "Conf_GLADS2", "Conf_RADD",
        "Desa", "Owner", "Blok"
    ]

    def prepare(df):
        df = df[keep_cols].copy()
        df = df.replace([np.inf, -np.inf], np.nan).fillna("")
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
        return df.astype(str)

    # ── Sheet Integrated (Deforestation) ──
    year_integrated = pd.to_datetime(df_integrated["Date"], errors="coerce").dt.year.max()
    sheet_name_int  = str(year_integrated)

    try:
        ws_int = sh.worksheet(sheet_name_int)
        ws_int.clear()
        print(f"\nSheet '{sheet_name_int}' dikosongkan.")
    except gspread.exceptions.WorksheetNotFound:
        ws_int = sh.add_worksheet(title=sheet_name_int, rows=50000, cols=15)
        print(f"\nSheet '{sheet_name_int}' dibuat baru.")

    df_int_out = prepare(df_integrated)
    ws_int.append_rows([list(df_int_out.columns)] + df_int_out.values.tolist(), value_input_option="USER_ENTERED")
    print(f"{len(df_int_out)} baris ditulis ke sheet '{sheet_name_int}' (Integrated/Deforestation).")

    # ── Sheet DIST-ALERT (Disturbance) ──
    year_dist      = pd.to_datetime(df_dist["Date"], errors="coerce").dt.year.max()
    sheet_name_dst = f"{year_dist}_DIST"

    try:
        ws_dst = sh.worksheet(sheet_name_dst)
        ws_dst.clear()
        print(f"Sheet '{sheet_name_dst}' dikosongkan.")
    except gspread.exceptions.WorksheetNotFound:
        ws_dst = sh.add_worksheet(title=sheet_name_dst, rows=50000, cols=15)
        print(f"Sheet '{sheet_name_dst}' dibuat baru.")

    df_dst_out = prepare(df_dist)
    ws_dst.append_rows([list(df_dst_out.columns)] + df_dst_out.values.tolist(), value_input_option="USER_ENTERED")
    print(f"{len(df_dst_out)} baris ditulis ke sheet '{sheet_name_dst}' (DIST-ALERT/Disturbance).")


# ─── Merge semua tahun ke sheet Db ───────────────────────────────────────────
def merge_sheets_to_db():
    creds  = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sh     = client.open_by_key(SPREADSHEET_ID)

    # Sheet tahun integrated
    sheets_integrated = ["2023", "2024", "2025", "2026"]
    # Sheet DIST per tahun
    sheets_dist       = ["2023_DIST", "2024_DIST", "2025_DIST", "2026_DIST"]

    def merge_to(sheet_names, target_name):
        all_data = []
        print(f"\nMerge ke '{target_name}':")
        for name in sheet_names:
            try:
                ws   = sh.worksheet(name)
                rows = ws.get_all_records()
                if rows:
                    all_data.extend(rows)
                    print(f"  ✔ {name}: {len(rows)} baris")
            except gspread.exceptions.WorksheetNotFound:
                print(f"  ⚠ {name} tidak ditemukan, dilewati.")

        if not all_data:
            print(f"  Tidak ada data untuk '{target_name}'.")
            return

        df = pd.DataFrame(all_data)
        df = df.replace([np.inf, -np.inf], np.nan).fillna("")
        df = df.drop_duplicates().reset_index(drop=True)

        try:
            ws_target = sh.worksheet(target_name)
            ws_target.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws_target = sh.add_worksheet(title=target_name, rows=100000, cols=15)

        ws_target.append_rows([list(df.columns)] + df.values.tolist(), value_input_option="USER_ENTERED")
        print(f"  Sheet '{target_name}' diperbarui: {len(df)} baris total.")

    merge_to(sheets_integrated, "Db")
    merge_to(sheets_dist,       "Db_DIST")


# ─── Update log ───────────────────────────────────────────────────────────────
def update_log(latest_integrated, latest_dist):
    creds  = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)

    try:
        log_sheet = client.open_by_key(SPREADSHEET_ID).worksheet(LOG_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        log_sheet = client.open_by_key(SPREADSHEET_ID).add_worksheet(
            title=LOG_SHEET_NAME, rows=10, cols=4
        )

    wib     = timezone(timedelta(hours=7))
    now_wib = datetime.now(wib).strftime("%Y-%m-%d %H:%M:%S")

    log_sheet.clear()
    log_sheet.append_rows([
        ["Note", "Last Update", "Latest Integrated Alert", "Latest DIST Alert"],
        ["Update", now_wib, str(latest_integrated), str(latest_dist)]
    ], value_input_option="USER_ENTERED")
    print(f"\nLog diperbarui: {now_wib}")
    print(f"  Latest Integrated : {latest_integrated}")
    print(f"  Latest DIST-ALERT : {latest_dist}")


# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    aoi_shape, aoi_geom_dict = load_aoi_geometry(AOI_PATH)

    df_integrated = fetch_integrated_alerts(aoi_geom_dict)
    df_dist       = fetch_dist_alerts(aoi_geom_dict)

    # Intersect masing-masing secara terpisah
    gdf_integrated = pd.DataFrame()
    gdf_dist       = pd.DataFrame()

    if not df_integrated.empty:
        gdf_integrated = intersect_with_geojson(df_integrated, DESA_PATH, PEMILIK_PATH, BLOK_PATH)
        print(f"\nRingkasan Integrated per Alert_Type:")
        print(gdf_integrated["Alert_Type"].value_counts().to_string())

    if not df_dist.empty:
        gdf_dist = intersect_with_geojson(df_dist, DESA_PATH, PEMILIK_PATH, BLOK_PATH)
        print(f"\nRingkasan DIST-ALERT per Alert_Type:")
        print(gdf_dist["Alert_Type"].value_counts().to_string())

    if not gdf_integrated.empty or not gdf_dist.empty:
        overwrite_google_sheet(gdf_integrated, gdf_dist)
        merge_sheets_to_db()
        update_log(
            gdf_integrated["Date"].max() if not gdf_integrated.empty else "-",
            gdf_dist["Date"].max()       if not gdf_dist.empty       else "-"
        )
    else:
        print("Tidak ada data untuk ditulis.")
