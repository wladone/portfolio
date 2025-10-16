import numpy as np
from .chaos_maps import keystream_from_chaos


def permute_image(img: np.ndarray, seed: bytes) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Permutare rânduri/coloane prin sortarea keystream-ului (stabilă)."""
    h, w = img.shape
    ks_rows = keystream_from_chaos(seed + b"rows", h)
    ks_cols = keystream_from_chaos(seed + b"cols", w)
    row_perm = np.argsort(ks_rows, kind="mergesort")
    col_perm = np.argsort(ks_cols, kind="mergesort")
    perm = img[row_perm][:, col_perm]
    return perm, row_perm, col_perm


def invert_permutation(p: np.ndarray) -> np.ndarray:
    inv = np.empty_like(p)
    inv[p] = np.arange(p.size, dtype=p.dtype)
    return inv


def diffuse(img: np.ndarray, seed: bytes) -> np.ndarray:
    """Difuzie XOR cu keystream bidimensional (simetric la decriptare)."""
    ks = keystream_from_chaos(seed + b"diff", img.size).reshape(img.shape)
    return (img.astype(np.uint8) ^ ks)


def encrypt(img: np.ndarray, password: str) -> dict:
    seed = password.encode("utf-8")
    perm, rp, cp = permute_image(img, seed)
    cipher = diffuse(perm, seed)
    return {"cipher": cipher, "row_perm": rp, "col_perm": cp}


def decrypt(cipher: np.ndarray, password: str, row_perm: np.ndarray, col_perm: np.ndarray) -> np.ndarray:
    seed = password.encode("utf-8")
    plain_perm = diffuse(cipher, seed)        # XOR inversează
    r_inv = invert_permutation(row_perm)
    c_inv = invert_permutation(col_perm)
    return plain_perm[r_inv][:, c_inv]
