import geopandas as gpd
from shapely.geometry import Polygon
from shapely.ops import unary_union
import pyproj

def calculate_cluster_area(df):
    """Kelompokkan titik-titik bertampalan (kotak 11.2 m) -> beri cluster_id dan luas cluster"""

    if df.empty:
        return df

    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")

    gdf = gdf.to_crs(epsg=32749)

    half_size = 11.2 / 2
    gdf["square"] = gdf.geometry.apply(lambda p: Polygon([
        (p.x - half_size, p.y - half_size),
        (p.x + half_size, p.y - half_size),
        (p.x + half_size, p.y + half_size),
        (p.x - half_size, p.y + half_size),
    ]))

    squares_gdf = gpd.GeoDataFrame(geometry=gdf["square"], crs=gdf.crs)

    squares_gdf["cluster_id"] = squares_gdf.buffer(0).unary_union
    clusters = squares_gdf.overlay(squares_gdf, how="union")

    from shapely.strtree import STRtree
    tree = STRtree(squares_gdf.geometry)
    geom_to_idx = {id(geom): i for i, geom in enumerate(squares_gdf.geometry)}

    visited = set()
    cluster_ids = [-1] * len(squares_gdf)
    cluster_num = 0

    for i, geom in enumerate(squares_gdf.geometry):
        if i in visited:
            continue
        cluster_num += 1
        stack = [i]
        while stack:
            j = stack.pop()
            if j in visited:
                continue
            visited.add(j)
            cluster_ids[j] = cluster_num
            for k, candidate in enumerate(tree.query(squares_gdf.geometry[j])):
                if squares_gdf.geometry[j].intersects(candidate):
                    idx = geom_to_idx[id(candidate)]
                    if idx not in visited:
                        stack.append(idx)

    gdf["cluster_id"] = cluster_ids

    cluster_areas = {}
    for cid in set(cluster_ids):
        cluster_polys = gdf[gdf["cluster_id"] == cid]["square"]
        union_poly = unary_union(list(cluster_polys))
        cluster_areas[cid] = round(union_poly.area, 2)  # m²

    gdf["cluster_area_m2"] = gdf["cluster_id"].map(cluster_areas)

    gdf = gdf.drop(columns=["square"])

    print(f"{len(set(cluster_ids))} cluster terbentuk dari {len(gdf)} titik.")
    return pd.DataFrame(gdf.drop(columns="geometry").to_crs(epsg=4326))
