import numpy as np
import hashlib


def logistic_map(x0: float, r: float, n: int) -> np.ndarray:
    x = np.empty(n, dtype=np.float64)
    x[0] = x0
    for i in range(1, n):
        x[i] = r * x[i-1] * (1.0 - x[i-1])
    return x


def henon_map(x0: float, y0: float, a: float, b: float, n: int) -> np.ndarray:
    xy = np.empty((n, 2), dtype=np.float64)
    xy[0] = [x0, y0]
    for i in range(1, n):
        x, y = xy[i-1]
        # clip pentru a preveni overflow
        x_clipped = np.clip(x, -1e6, 1e6)
        xn = 1.0 - a * x_clipped * x_clipped + y
        yn = b * x
        # normalizare dacă valorile devin prea mari
        mag = max(abs(xn), abs(yn))
        xy[i] = [xn/mag if mag > 1e6 else xn, yn/mag if mag > 1e6 else yn]
    return xy


def chebyshev_map(x0: float, n: int) -> np.ndarray:
    x = np.empty(n, dtype=np.float64)
    x[0] = x0
    for i in range(1, n):
        x[i] = 2.0 * x[i-1] * x[i-1] - 1.0
        if x[i] < -1.0 or x[i] > 1.0:
            x[i] = np.tanh(x[i])
    return x


def keystream_from_chaos(seed: bytes, size: int) -> np.ndarray:
    """Mix Logistic + Hénon + Chebyshev -> keystream uint8 determinist din 'seed'."""
    h = hashlib.sha256(seed).digest()
    u = np.frombuffer(h, dtype=np.uint8).astype(np.float64) / 255.0
    x0 = 0.1 + 0.8 * u[0]
    r = 3.6 + 0.39 * u[1]
    hx0, hy0 = 0.1 + 0.8 * u[2], 0.1 + 0.8 * u[3]
    a, b = 1.2 + 0.2 * u[4], 0.2 + 0.2 * u[5]
    cx0 = 2 * (u[6] - 0.5)

    L = logistic_map(x0, r, size)
    H = henon_map(hx0, hy0, a, b, size)[:, 0]
    C = chebyshev_map(cx0, size)

    # Normalize each sequence to [0,1] robustly (avoid div-by-zero and NaNs)
    def safe_normalize(arr):
        arr = np.asarray(arr, dtype=np.float64)
        if not np.isfinite(arr).all():
            # replace non-finite with tanh of value to bound it
            arr = np.where(np.isfinite(arr), arr, np.tanh(arr))
        mn = np.nanmin(arr)
        mx = np.nanmax(arr)
        rng = mx - mn
        if rng <= 0 or not np.isfinite(rng):
            return np.zeros_like(arr, dtype=np.float64)
        return (arr - mn) / (rng + 1e-12)

    Ln = safe_normalize(L)
    Hn = safe_normalize(np.abs(H))
    Cn = safe_normalize((C + 1.0) / 2.0)

    S = (Ln + Hn + Cn) / 3.0
    # ensure values in [0,1)
    S = np.clip(S - np.floor(S), 0.0, 1.0 - 1e-12)
    return np.floor(S * 256.0).astype(np.uint8)
