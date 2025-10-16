import numpy as np


def corr2(a: np.ndarray, b: np.ndarray) -> float:
    """Calculează coeficientul de corelație între 2 matrici."""
    a = a.astype(np.float64)
    b = b.astype(np.float64)
    am = a - a.mean()
    bm = b - b.mean()
    denom = np.sqrt((am * am).sum() * (bm * bm).sum())
    return 0.0 if denom == 0 else float((am * bm).sum() / denom)


def pixel_correlations(img: np.ndarray) -> dict:
    """Returnează corelațiile pe orizontală, verticală și diagonală."""
    r, c = img.shape
    return {
        "horizontal": corr2(img[:, :c-1], img[:, 1:]),
        "vertical":   corr2(img[:r-1, :], img[1:, :]),
        "diagonal":   corr2(img[:r-1, :c-1], img[1:, 1:]),
    }


def entropy(img: np.ndarray) -> float:
    """Entropia imaginii (0–8 pentru 8-biți)."""
    hist, _ = np.histogram(img, bins=256, range=(0, 256))
    p = hist / hist.sum()
    p = p[p > 0]
    return float(-(p * np.log2(p)).sum())


def npcr_uaci(img1: np.ndarray, img2: np.ndarray) -> tuple[float, float]:
    """Calculează NPCR și UACI între două imagini."""
    assert img1.shape == img2.shape
    diff = img1 != img2
    npcr = 100.0 * diff.sum() / diff.size
    uaci = 100.0 * (np.abs(img1.astype(np.int16) -
                    img2.astype(np.int16)).sum() / (255.0 * diff.size))
    return float(npcr), float(uaci)
