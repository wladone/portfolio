from pathlib import Path
from imcrypt.io_utils import load_image_gray, save_image_gray
from imcrypt.cipher import encrypt, decrypt
from imcrypt.metrics import entropy, pixel_correlations, npcr_uaci

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"


def main():
    DATA.mkdir(parents=True, exist_ok=True)
    src = DATA / "Lena_256.png"      # pune aici imaginea
    enc = DATA / "Lena_256_enc.png"
    dec = DATA / "Lena_256_dec.png"

    pwd = "parolaMeaForta"

    img = load_image_gray(src)
    result = encrypt(img, pwd)
    save_image_gray(result["cipher"], enc)

    print("Entropy(enc):", entropy(result["cipher"]))
    print("Correlations(enc):", pixel_correlations(result["cipher"]))

    plain = decrypt(result["cipher"], pwd,
                    result["row_perm"], result["col_perm"])
    save_image_gray(plain, dec)

    n, u = npcr_uaci(img, result["cipher"])
    print(f"NPCR={n:.2f}%  UACI={u:.2f}%")


if __name__ == "__main__":
    main()
