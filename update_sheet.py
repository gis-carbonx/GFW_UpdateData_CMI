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
API_KEY = "912b99d5-ecc2-47aa-86fe-1f986b9b070b"
SPREADSHEET_ID = "1UW3uOFcLr4AQFBp_VMbEXk37_Vb5DekHU-_9QSkskCo"

AOI_PATH = "data/aoi.json"
DESA_PATH = "data/Desa.json"
PEMILIK_PATH = "data/PemilikLahan.json"
BLOK_PATH = "data/blok.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ================= FETCH =================
def fetch_all_alerts():
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

    def hit_api(url, sql):
        resp = requests.post(url, headers={"x-api-key": API_KEY}, json={
            "geometry": geometry,
            "sql": sql
        })
        if resp.status_code != 200:
            print(resp.text)
            return pd.DataFrame()
        return pd.DataFrame(resp.json().get("data", []))

    print("=== FETCH MULTI SOURCE ===")

    # Integrated
    df_int = hit_api(
        "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/latest/query",
        f"""
        SELECT longitude, latitude,
               gfw_integrated_alerts__date AS date,
               gfw_integrated_alerts__confidence AS confidence
        FROM results
        WHERE gfw_integrated_alerts__date >= '{start_date}'
          AND gfw_integrated_alerts__date <= '{today}'
        """
    )
    df_int["Source"] = "INTEGRATED"

    # GLAD
    df_glad = hit_api(
        "https://data-api.globalforestwatch.org/dataset/glad_alerts/latest/query",
        f"""
        SELECT longitude, latitude,
               alert_date AS date
        FROM results
        WHERE alert_date >= '{start_date}'
          AND alert_date <= '{today}'
        """
    )
    df_glad["Source"] = "GLAD"

    # RADD
    df_radd = hit_api(
        "https://data-api.globalforestwatch.org/dataset/radd_alerts/latest/query",
        f"""
        SELECT longitude, latitude,
               alert_date AS date
        FROM results
        WHERE alert_date >= '{start_date}'
          AND alert_date <= '{today}'
        """
    )
    df_radd["Source"] = "RADD"

    df = pd.concat([df_int, df_glad, df_radd], ignore_index=True)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude", "date"])

    print("Total raw:", len(df))
    return df

# ================= FUSION =================
def classify_confidence(n):
    if n >= 3:
        return "very_high"
    elif n == 2:
        return "high"
    else:
        return "low"

def fuse_sources(df):
    print("=== FUSION ===")

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    ).to_crs(epsg=32749)

    records = []

    for _, row in gdf.iterrows():
        buffer = row.geometry.buffer(50)

        subset = gdf[
            (gdf.geometry.intersects(buffer)) &
            (abs((gdf.date - row.date).dt.days) <= 5)
        ]

        sources = sorted(subset["Source"].unique())

        records.append({
            "latitude": row.geometry.y,
            "longitude": row.geometry.x,
            "Integrated_Date": row.date,
            "Integrated_Alert": classify_confidence(len(sources)),
            "Source_Detail": ",".join(sources),
            "Source_Count": len(sources)
        })

    df_out = pd.DataFrame(records).drop_duplicates(
        subset=["latitude","longitude","Integrated_Date"]
    )

    print("After fusion:", len(df_out))
    return df_out

# ================= AOI =================
def clip_with_aoi(df):
    with open(AOI_PATH) as f:
        aoi = shape(json.load(f)["features"][0]["geometry"])

    df = df[
        df.apply(lambda r: aoi.contains(Point(r["longitude"], r["latitude"])), axis=1)
    ]

    print("After AOI:", len(df))
    return df

# ================= INTERSECT =================
def intersect_with_geojson(df):
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")

    desa = gpd.read_file(DESA_PATH)[["nama_kel","geometry"]]
    pemilik = gpd.read_file(PEMILIK_PATH)[["Owner","geometry"]]
    blok = gpd.read_file(BLOK_PATH)[["Blok","geometry"]]

    # FIX: pakai intersects (lebih robust)
    gdf = gpd.sjoin(gdf, desa, how="left", predicate="intersects").rename(columns={"nama_kel":"Desa"})
    gdf = gpd.sjoin(gdf, pemilik, how="left", predicate="intersects")
    gdf = gpd.sjoin(gdf, blok, how="left", predicate="intersects")

    # FIX: jangan hilang data
    gdf["Owner"] = gdf["Owner"].fillna("Unknown")

    print("After intersect:", len(gdf))
    print("Owner kosong:", (gdf["Owner"]=="Unknown").sum())

    return gdf

# ================= CLUSTER =================
def cluster_points_by_owner(gdf):
    print("=== CLUSTER ===")

    gdf = gdf.to_crs(epsg=32749)
    results = []

    for (owner, tanggal), group in gdf.groupby(["Owner","Integrated_Date"]):
        if group.empty:
            continue

        group = group.copy()
        group["buffer"] = group.geometry.buffer(11)
        union_poly = unary_union(group["buffer"])

        clusters = [union_poly] if union_poly.geom_type=="Polygon" else list(union_poly.geoms)

        cluster_gdf = gpd.GeoDataFrame(geometry=clusters, crs=gdf.crs)
        cluster_gdf["Cluster_ID"] = [f"{owner}_{tanggal}_{i+1}" for i in range(len(cluster_gdf))]

        centroid = cluster_gdf.geometry.centroid.to_crs(epsg=4326)
        cluster_gdf["Cluster_Y"] = centroid.y
        cluster_gdf["Cluster_X"] = centroid.x

        joined = gpd.sjoin(group, cluster_gdf, how="left", predicate="intersects")

        count = joined.groupby("Cluster_ID").size().reset_index(name="Jumlah_Titik")
        joined = joined.merge(count, on="Cluster_ID")

        results.append(joined)

    if not results:
        print("Cluster kosong!")
        return gdf.to_crs(4326)

    final = pd.concat(results).to_crs(4326)
    print("After cluster:", len(final))
    return final

# ================= SHEET =================
def overwrite_google_sheet(df):
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    client = gspread.authorize(creds)

    df["Integrated_Date"] = pd.to_datetime(df["Integrated_Date"], errors="coerce")
    df = df.dropna(subset=["Integrated_Date"])

    year = df["Integrated_Date"].dt.year.max()
    sheet_name = str(year)

    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
        sheet.clear()
    except:
        sheet = client.open_by_key(SPREADSHEET_ID).add_worksheet(title=sheet_name, rows=1000, cols=20)

    cols = [
        "latitude","longitude","Integrated_Date","Integrated_Alert",
        "Source_Detail","Source_Count",
        "Desa","Owner","Blok","Cluster_ID",
        "Cluster_Y","Cluster_X","Jumlah_Titik"
    ]

    df = df[cols].fillna("").astype(str)
    data = [cols] + df.values.tolist()

    # aman untuk <5000
    sheet.append_rows(data, value_input_option="USER_ENTERED")

    print("Berhasil tulis:", len(df))

# ================= MAIN =================
if __name__ == "__main__":
    df_raw = fetch_all_alerts()

    if not df_raw.empty:
        df = fuse_sources(df_raw)
        df = clip_with_aoi(df)

        if not df.empty:
            gdf = intersect_with_geojson(df)
            gdf = cluster_points_by_owner(gdf)

            if not gdf.empty:
                overwrite_google_sheet(gdf)
                print("Pipeline selesai.")
            else:
                print("Gagal di cluster.")
        else:
            print("Gagal di AOI.")
    else:
        print("Tidak ada data.")
