from pathlib import Path
import numpy as np
from PIL import Image


def load_image_gray(path: str | Path) -> np.ndarray:
    path = Path(path)
    img = Image.open(path).convert("L")
    return np.array(img, dtype=np.uint8)


def save_image_gray(arr: np.ndarray, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(arr.astype("uint8"), mode="L").save(path)
