""

from pathlib import Path
from typing import Union
from concurrent.futures import ThreadPoolExecutor
import asyncio
import aiohttp

import numpy as np
import requests
import geopandas as gpd
from shapely.geometry import box
import pandas as pd
from tqdm import tqdm

from geo import to_osgb36


API_URL = "https://environment.data.gov.uk/backend/catalog/api/tiles/collections/survey/search"


def get_england():
    cache_path = Path("data/england.parquet")
    if cache_path.exists():
        england = gpd.read_parquet(cache_path)
    else:        
        england = gpd.read_file(
            "https://martinjc.github.io/UK-GeoJSON/json/eng/topo_eer.json",
            driver="TopoJSON",
        )
        england.crs = "EPSG:4326"
        england.to_parquet(cache_path)
    return england


def tile_gdf(
    gdf,
    side_length: int,
    tile_crs: int = 27700,
    origin_x: int = 0,
    origin_y: int = 0,
    filter_empty: bool = True,
) -> gpd.GeoDataFrame:
    """
    Tile a GeoDataFrame into non-overlapping square tiles equal to side_length

    Args:
        gdf: GeoDataFrame
        side_length: int
            Length of each side of the square tile
        tile_crs: int
            CRS of the tile, the gdf will be reprojected to this CRS
        origin_x: int
            x-coordinate of the origin
        origin_y: int
            y-coordinate of the origin

    Returns:
        GeoDataFrame

    """
    gdf = gdf.to_crs(tile_crs)
    
    # Get the bounds of the GeoDataFrame
    bounds = gdf.total_bounds
    minx, miny, maxx, maxy = bounds

    # confrom the bounding box to the the side_length
    minx = minx - (minx % side_length)
    miny = miny - (miny % side_length)
    maxx = maxx + (side_length - maxx % side_length)
    maxy = maxy + (side_length - maxy % side_length)

    # Create a grid of points using numpy
    x = np.arange(minx, maxx, side_length)
    y = np.arange(miny, maxy, side_length)
    xx, yy = np.meshgrid(x, y)

    # create boxes from the grid by adding the side_length to each point
    xmins = xx.flatten()
    ymins = yy.flatten()
    xmaxs = xmins + side_length
    ymaxs = ymins + side_length

    # Create a GeoDataFrame from the boxes
    
    boxes = gpd.GeoSeries(
        [box(xmin, ymin, xmax, ymax) for xmin, ymin, xmax, ymax in zip(xmins, ymins, xmaxs, ymaxs)]
    )

    box_gdf = gpd.GeoDataFrame(geometry=boxes, crs=gdf.crs)

    if filter_empty:
        # Filter out empty boxes
        gdf["geometry"] = gdf.geometry.make_valid()
        gdf["geometry"] = gdf.buffer(0)

        box_gdf = box_gdf[box_gdf.intersects(gdf.union_all())]
    return box_gdf



def feature_to_request_body(feature):
    return {
        "type": feature["geometry"]["type"],
        "coordinates": feature["geometry"]["coordinates"],
    }

def construct_request_bodies(
        gdf: gpd.GeoDataFrame,
):
    """
    Extract the geojson representation of each geometry in the GeoDataFrame
    """
    crs = 4326
    gdf = gdf.to_crs(crs)
    gdf_geojson = gdf.__geo_interface__
    feature_geojson = [feature_to_request_body(feature) for feature in gdf_geojson["features"]]
    return feature_geojson






def parse_response(results_df:pd.DataFrame):
    
    json_columns = ["product", "year", "resolution", "tile", "label"]
    parsed_json = {}
    for column in json_columns:
        parsed_df = pd.json_normalize(results_df[column])
        parsed_df.columns = [f"{column}_{col}" for col in parsed_df.columns]
        parsed_json[column] = parsed_df
    
    results_df = results_df.drop(columns=json_columns)
    parsed_df_all_cols = pd.concat([results_df] +[df for df in parsed_json.values()], axis=1)
    
    return parsed_df_all_cols

async def ingest_data(tile_gdf: gpd.GeoDataFrame):
    request_bodies = construct_request_bodies(tile_gdf)
    response_dfs = []
    
    async with aiohttp.ClientSession() as session:
        # Create a list of tasks to run concurrently
        tasks = [request_tile(session, API_URL, request_body) for request_body in request_bodies]
        
        # Process responses concurrently
        for future in tqdm(asyncio.as_completed(tasks), total=len(request_bodies)):
            response_df = await future
            response_dfs.append(response_df)
    
    # Parse the responses    
    print("Parsing responses")
    response_df = parse_response(pd.concat(response_dfs, ignore_index=True))

    return response_df

async def request_tile(session: aiohttp.ClientSession, url: str, feature: dict):
    headers = {
        "Content-Type": "application/geo+json"
    }
    async with session.post(url, json=feature, headers=headers) as response:
        response.raise_for_status()
        response_json = await response.json()
        results_df = pd.DataFrame.from_dict(response_json["results"])
    
    return results_df

def parse_geometry(df: pd.DataFrame) -> gpd.GeoDataFrame:
    x, y, resolution = zip(*[to_osgb36(tile) for tile in df.tile_id])

    x = np.array(x)
    y = np.array(y)
    resolution = np.array(resolution)
    resolution = resolution / 2

    xmin = x
    xmax = x + resolution
    ymin = y
    ymax = y + resolution

    geometry = [box(xmin, ymin, xmax, ymax) for xmin, ymin, xmax, ymax in zip(xmin, ymin, xmax, ymax)]

    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:27700")
    return gdf


def main(
        output_path: Union[str, Path] = "data/products-index.parquet"
):
    england = get_england()
    tiles = tile_gdf(england, 50_000)
    

    products_df = asyncio.run(ingest_data(tiles))
    products_df = products_df[~products_df.duplicated()]
    products_gdf = parse_geometry(products_df)

    products_gdf.to_parquet(output_path)

if __name__ == "__main__":
    main()