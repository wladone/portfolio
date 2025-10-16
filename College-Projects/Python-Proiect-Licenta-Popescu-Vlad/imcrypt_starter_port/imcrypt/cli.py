import argparse
import json
import sys
from pathlib import Path
import numpy as np
from .io_utils import load_image_gray, save_image_gray
from .cipher import encrypt, decrypt
from .metrics import pixel_correlations, entropy, npcr_uaci
from .gui import run_gui


def cmd_encrypt(args):
    img = load_image_gray(args.input)
    result = encrypt(img, args.password)
    save_image_gray(result["cipher"], args.output)
    sidecar = Path(args.output).with_suffix(".keys.npz")
    np.savez_compressed(
        sidecar, row_perm=result["row_perm"], col_perm=result["col_perm"])
    print(f"Encrypted -> {Path(args.output)}")
    print(f"Saved keys -> {sidecar}")


def cmd_decrypt(args):
    cipher_img = load_image_gray(args.input)
    data = np.load(args.keys)
    plain = decrypt(cipher_img, args.password,
                    data["row_perm"], data["col_perm"])
    save_image_gray(plain, args.output)
    print(f"Decrypted -> {Path(args.output)}")


def cmd_eval(args):
    img = load_image_gray(args.input)
    out = {
        "entropy": entropy(img),
        "correlations": pixel_correlations(img),
    }
    print(json.dumps(out, indent=2))


def cmd_diff(args):
    img1 = load_image_gray(args.input1)
    img2 = load_image_gray(args.input2)
    npcr, uaci = npcr_uaci(img1, img2)
    print(json.dumps({"NPCR": npcr, "UACI": uaci}, indent=2))


def build_parser():
    p = argparse.ArgumentParser(
        prog="imcrypt", description="Image encryption with chaotic maps (Python port)")
    sub = p.add_subparsers(dest="command", required=True)

    e = sub.add_parser("encrypt", help="Encrypt an image")
    e.add_argument("-i", "--input", required=True,
                   help="cale către imaginea sursă")
    e.add_argument("-o", "--output", required=True,
                   help="cale de ieșire pentru imaginea criptată")
    e.add_argument("-p", "--password", required=True, help="parola/cheia")
    e.set_defaults(func=cmd_encrypt)

    d = sub.add_parser("decrypt", help="Decrypt an image")
    d.add_argument("-i", "--input", required=True, help="imaginea criptată")
    d.add_argument("-k", "--keys", required=True,
                   help="fișierul .keys.npz generat la criptare")
    d.add_argument("-o", "--output", required=True,
                   help="cale de ieșire pentru imaginea decriptată")
    d.add_argument("-p", "--password", required=True,
                   help="parola/cheia (identică)")
    d.set_defaults(func=cmd_decrypt)

    ev = sub.add_parser("eval", help="Entropy + pixel correlations")
    ev.add_argument("-i", "--input", required=True, help="imaginea de evaluat")
    ev.set_defaults(func=cmd_eval)

    df = sub.add_parser("diff", help="Compute NPCR/UACI între două imagini")
    df.add_argument("--input1", required=True)
    df.add_argument("--input2", required=True)
    df.set_defaults(func=cmd_diff)

    g = sub.add_parser("gui", help="Pornește interfața grafică")
    g.set_defaults(func=lambda args: run_gui())

    return p


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv or sys.argv[1:])
    args.func(args)


if __name__ == "__main__":
    main()
