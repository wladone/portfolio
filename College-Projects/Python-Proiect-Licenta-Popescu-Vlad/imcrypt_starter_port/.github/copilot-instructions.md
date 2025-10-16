# Copilot Instructions for imcrypt_starter_port

## Project Overview
- **Purpose:** Encrypt/decrypt grayscale images using chaos-based cryptography (Logistic, Hénon, Chebyshev maps).
- **Main Components:**
  - `imcrypt/chaos_maps.py`: Chaotic keystream generation
  - `imcrypt/cipher.py`: Permutation & diffusion logic
  - `imcrypt/io_utils.py`: Image I/O (grayscale, PNG/JPG)
  - `imcrypt/metrics.py`: Security metrics (entropy, correlations, NPCR/UACI)
  - `imcrypt/cli.py`: CLI entry point (`imcrypt`)
  - `examples/main.py`: Example usage
  - `data/`: Sample images and key files

## Architecture & Data Flow
- **Encryption:**
  1. Load grayscale image (`io_utils.load_image_gray`)
  2. Permute rows/columns using chaos-based keystream (`cipher.permute_image`)
  3. Diffuse with XOR keystream (`cipher.diffuse`)
  4. Save encrypted image and permutation keys
- **Decryption:**
  1. Load encrypted image and keys
  2. Reverse diffusion and permutation
- **Metrics:**
  - Entropy, pixel correlations, NPCR/UACI for image security evaluation

## Developer Workflows
- **Setup (Windows):**
  ```powershell
  py -3 -m venv .venv
  .\.venv\Scripts\Activate.ps1
  pip install -r requirements.txt
  pip install -e .
  ```
- **Run CLI:**
  ```powershell
  python -m imcrypt.cli encrypt -i data/Watch_256.jpg -o data/Watch_256_enc.png -p <password>
  python -m imcrypt.cli decrypt -i data/Watch_256_enc.png -k data/Watch_256_enc.keys.npz -o data/Watch_256_dec.png -p <password>
  python -m imcrypt.cli eval -i data/Watch_256.jpg
  python -m imcrypt.cli diff --input1 data/Watch_256.jpg --input2 data/Watch_256_enc.png
  ```
- **Entrypoint:** `imcrypt.cli:main` (see `pyproject.toml`)

## Conventions & Patterns
- **Images:** Always processed as grayscale (`uint8`).
- **Keys:** Permutation keys saved as `.keys.npz` (NumPy compressed).
- **Password:** Used as seed for chaos maps (UTF-8 encoded).
- **Metrics:** Use provided functions for security analysis, not external tools.
- **No test suite detected:** Add tests in `tests/` if needed; follow CLI usage for manual validation.

## Integration Points
- **External:** Only `numpy` and `pillow` required.
- **No web, database, or network integration.**

## Examples
- See `examples/main.py` for programmatic usage.
- See `data/` for sample images and key files.

---
**If unclear, review `imcrypt/cli.py` for supported commands and arguments.**
