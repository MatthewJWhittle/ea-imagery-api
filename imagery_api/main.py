""
from pathlib import Path

import rasterio as rio
from rasterio.windows import from_bounds

import numpy as np
from PIL import Image
import io
from fastapi import FastAPI
from fastapi.responses import Response
import mercantile

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World!"}


@app.get("/list_tiles/")
async def list_tiles():
    tile_dir = Path("data/cogs")
    tiles = list(tile_dir.glob("*.tif"))
    return {"count": len(tiles), "tiles": [str(tile) for tile in tiles]}


# Get a window of the first tile (like an xyz tile server)
@app.get(
    "/tile/{z}/{x}/{y}",
    responses={200: {"content": {"image/png": {}}}},
    response_class=Response,
)
async def get_tile(x: int, y: int, z: int):
    print(x, y, z)
    tile = mercantile.Tile(x=x, y=y, z=z)

    tile_dir = Path("data/cogs")
    tile_path = list(tile_dir.glob("*.tif"))[0]
    image_size = 256
    try:
        with rio.open(tile_path) as src:
            data = src.read(
                [1, 2, 3],
                window=from_bounds(
                    *mercantile.xy_bounds(tile), src.transform, image_size, image_size
                ),
                out_shape=(3, image_size, image_size),
            )
            data = np.clip(data, 0, 255).astype(np.uint8)
            mask = (data != 0).any(axis=0).astype(np.uint8) * 255
            data = np.concatenate([data, mask[np.newaxis, :, :]], axis=0)

        image = Image.fromarray(data.transpose(1, 2, 0), "RGBA")

        

        with io.BytesIO() as buf:
            image.save(buf, format="PNG")
            im_bytes = buf.getvalue()

        # return as a png
        headers = {"Content-Disposition": 'inline; filename="test.png"'}

        return Response(im_bytes, headers=headers, media_type="image/png")
    except Exception as e:
        array = np.zeros((256, 256, 4), dtype=np.uint8)

        image = Image.fromarray(array, mode="RGBA")
        with io.BytesIO() as buf:
            image.save(buf, format="PNG")
            im_bytes = buf.getvalue()
        print(e)
        return Response(im_bytes, media_type="image/png")
