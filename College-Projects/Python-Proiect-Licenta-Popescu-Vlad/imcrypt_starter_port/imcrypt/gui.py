import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from pathlib import Path
import numpy as np
from .io_utils import load_image_gray, save_image_gray
from .cipher import encrypt, decrypt
from .metrics import entropy, pixel_correlations, npcr_uaci
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import json
from PIL import Image, ImageTk


DATA_DIR = Path(__file__).resolve().parents[1] / "data"
ORIG_DIR = DATA_DIR / "original"
ENC_DIR = DATA_DIR / "encrypted"
DEC_DIR = DATA_DIR / "decrypted"


def ensure_dirs():
    """Create required directories if they don't exist."""
    for d in (ORIG_DIR, ENC_DIR, DEC_DIR):
        d.mkdir(parents=True, exist_ok=True)


class MetricsWindow(tk.Toplevel):
    def __init__(self, parent, metrics_data, block_size=8, display_plots=True):
        super().__init__(parent)
        self.title("Security Metrics & Analysis")
        self.geometry("800x600")
        self.block_size = block_size
        self.display_plots = display_plots

        # Notebook pentru taburi
        nb = ttk.Notebook(self)
        nb.pack(fill='both', expand=True, padx=5, pady=5)

        # Tab pentru metrici text
        metrics_tab = ttk.Frame(nb)
        nb.add(metrics_tab, text='Metrics')

        # Format HTML-like pentru metrici
        text = tk.Text(metrics_tab, wrap=tk.WORD, width=80, height=20)
        text.pack(fill='both', expand=True, padx=5, pady=5)
        text.tag_configure('bold', font=('Helvetica', 10, 'bold'))
        text.tag_configure('normal', font=('Helvetica', 10))

        # Formatare frumoasă pentru metrici
        def add_section(title, data):
            text.insert('end', f"\n{title}\n", 'bold')
            text.insert('end', "="*len(title) + "\n", 'normal')
            if isinstance(data, dict):
                for k, v in data.items():
                    if isinstance(v, dict):
                        text.insert('end', f"{k}:\n", 'bold')
                        for k2, v2 in v.items():
                            text.insert('end', f"  {k2}: {v2:.6f}\n", 'normal')
                    else:
                        text.insert('end', f"{k}: {v:.6f}\n", 'normal')
            else:
                text.insert('end', str(data) + "\n", 'normal')

        for stage, data in metrics_data.items():
            if stage != 'npcr_uaci':
                add_section(f"{stage.title()} Image", data)

        if 'npcr_uaci' in metrics_data:
            add_section("Difference Analysis", metrics_data['npcr_uaci'])

        # Export button
        def export_csv():
            path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv")],
                initialdir=DATA_DIR
            )
            if path:
                with open(path, 'w') as f:
                    json.dump(metrics_data, f, indent=2)
                messagebox.showinfo("Export", f"Metrics saved to {path}")

        ttk.Button(metrics_tab, text="Export Metrics",
                   command=export_csv).pack(pady=5)

        # Export current plot (PNG)
        def export_plot():
            if not hasattr(self, 'last_fig') or self.last_fig is None:
                messagebox.showwarning("No plot", "No plot to export")
                return
            p = filedialog.asksaveasfilename(defaultextension='.png', filetypes=[
                                             ('PNG', '*.png')], initialdir=DATA_DIR)
            if p:
                try:
                    self.last_fig.savefig(p)
                    messagebox.showinfo('Saved', f'Plot saved to {p}')
                except Exception as e:
                    messagebox.showerror('Error', str(e))

        ttk.Button(metrics_tab, text='Export Current Plot',
                   command=export_plot).pack(pady=3)

        # Export all plots (recreate and save all figures)
        def export_all_plots():
            saved = []
            try:
                # recreate main plots if available
                if 'original' in metrics_data and 'encrypted' in metrics_data:
                    fig = Figure(figsize=(12, 8))
                    fig.subplots_adjust(hspace=0.4, wspace=0.3)
                    ax1 = fig.add_subplot(221)
                    ax2 = fig.add_subplot(222)
                    ax3 = fig.add_subplot(223)
                    ax4 = fig.add_subplot(224)
                    orig_img = load_image_gray(
                        ORIG_DIR / parent.selected_path.name)
                    enc_img = load_image_gray(
                        ENC_DIR / (parent.selected_path.stem + "_enc.png"))
                    sns.histplot(data=orig_img.ravel(), stat='density',
                                 bins='auto', ax=ax1, color='blue')
                    ax1.set_title('Original Histogram')
                    sns.histplot(data=enc_img.ravel(), stat='density',
                                 bins='auto', ax=ax2, color='orange')
                    ax2.set_title('Encrypted Histogram')
                    x_orig, y_orig = (orig_img.ravel()[:1000], np.roll(
                        orig_img.ravel(), 1)[:1000])
                    x_enc, y_enc = (enc_img.ravel()[:1000], np.roll(
                        enc_img.ravel(), 1)[:1000])
                    sns.scatterplot(x=x_orig, y=y_orig,
                                    alpha=0.5, ax=ax3, color='blue')
                    ax3.set_title('Original Horizontal Correlation')
                    sns.scatterplot(x=x_enc, y=y_enc, alpha=0.5,
                                    ax=ax4, color='orange')
                    ax4.set_title('Encrypted Horizontal Correlation')
                    out = DATA_DIR / \
                        (parent.selected_path.stem + '_plots_all.png')
                    fig.savefig(str(out))
                    saved.append(out.name)

                # extra (entropy heatmap)
                if 'original' in metrics_data:
                    fig2 = Figure(figsize=(8, 4))
                    ax_e = fig2.add_subplot(121)
                    ax_h = fig2.add_subplot(122)
                    orig_img = load_image_gray(
                        ORIG_DIR / parent.selected_path.name)
                    h, w = orig_img.shape
                    bh = bw = getattr(self, 'block_size', 8)
                    ent_vals = []
                    for y in range(0, h, bh):
                        for x in range(0, w, bw):
                            block = orig_img[y:y+bh, x:x+bw]
                            ent_vals.append(entropy(block))
                    ax_e.plot(ent_vals)
                    ax_e.set_title('Entropy per block')
                    corr = pixel_correlations(orig_img)
                    sns.heatmap(np.array([[corr['horizontal'], corr['vertical']], [
                                corr['diagonal'], 0.0]]), annot=True, ax=ax_h, cmap='coolwarm')
                    ax_h.set_title('Correlation heatmap')
                    out2 = DATA_DIR / \
                        (parent.selected_path.stem + '_extra_all.png')
                    fig2.savefig(str(out2))
                    saved.append(out2.name)

                # NPCR/UACI diff
                if 'npcr_uaci' in metrics_data and 'original' in metrics_data and 'encrypted' in metrics_data:
                    orig_img = load_image_gray(
                        ORIG_DIR / parent.selected_path.name)
                    enc_img = load_image_gray(
                        ENC_DIR / (parent.selected_path.stem + '_enc.png'))
                    diff_img = np.abs(orig_img.astype(
                        np.int16) - enc_img.astype(np.int16)).astype(np.uint8)
                    fig3 = Figure(figsize=(6, 4))
                    axd = fig3.add_subplot(121)
                    axh = fig3.add_subplot(122)
                    axd.imshow(diff_img, cmap='gray')
                    axd.axis('off')
                    axd.set_title('Absolute Difference')
                    sns.histplot(diff_img.ravel(), bins='auto',
                                 ax=axh, color='purple')
                    axh.set_title('Difference Histogram')
                    out3 = DATA_DIR / \
                        (parent.selected_path.stem + '_npcr_all.png')
                    fig3.savefig(str(out3))
                    saved.append(out3.name)

                if saved:
                    messagebox.showinfo(
                        'Saved', 'Saved plots: ' + ', '.join(saved))
                else:
                    messagebox.showwarning(
                        'No plots', 'No plots were generated')
            except Exception as e:
                messagebox.showerror('Error', str(e))

        ttk.Button(metrics_tab, text='Export All Plots',
                   command=export_all_plots).pack(pady=3)

        # Tab pentru plots
        plots_tab = ttk.Frame(nb)
        nb.add(plots_tab, text='Plots')

        if 'original' in metrics_data and 'encrypted' in metrics_data:
            # Figure pentru toate plot-urile
            fig = Figure(figsize=(12, 8))
            fig.subplots_adjust(hspace=0.4, wspace=0.3)

            # Histograme
            ax1 = fig.add_subplot(221)
            ax2 = fig.add_subplot(222)

            # Load images
            orig_img = load_image_gray(ORIG_DIR / parent.selected_path.name)
            enc_img = load_image_gray(
                ENC_DIR / (parent.selected_path.stem + "_enc.png"))

            # Histograme
            sns.histplot(data=orig_img.ravel(), stat='density',
                         bins="auto", ax=ax1, color='blue')
            ax1.set_title('Original Histogram')
            sns.histplot(data=enc_img.ravel(), stat='density',
                         bins="auto", ax=ax2, color='orange')
            ax2.set_title('Encrypted Histogram')

            # Correlation plots
            ax3 = fig.add_subplot(223)
            ax4 = fig.add_subplot(224)

            # Sample points pentru corelații (pentru performanță)
            def sample_pairs(img, n=1000):
                h, w = img.shape
                idx = np.random.choice(h*w, n, replace=False)
                x = img.ravel()[idx]
                y = np.roll(img.ravel(), 1)[idx]
                return x, y

            # Horizontal correlation scatter
            x_orig, y_orig = sample_pairs(orig_img)
            sns.scatterplot(x=x_orig, y=y_orig, alpha=0.5,
                            ax=ax3, color='blue')
            ax3.set_title('Original Horizontal Correlation')

            x_enc, y_enc = sample_pairs(enc_img)
            sns.scatterplot(x=x_enc, y=y_enc, alpha=0.5,
                            ax=ax4, color='orange')
            ax4.set_title('Encrypted Horizontal Correlation')

            # Add canvas
            if self.display_plots:
                canvas = FigureCanvasTkAgg(fig, master=plots_tab)
                canvas.draw()
                canvas.get_tk_widget().pack(fill='both', expand=True)
                self.last_fig = fig
            else:
                out = DATA_DIR / (parent.selected_path.stem + '_plots.png')
                fig.savefig(str(out))
                self.last_fig = fig
                ttk.Label(plots_tab, text=f'Plots saved to {out.name}').pack(
                    padx=10, pady=10)

        # Extra plots: entropy evolution and correlation heatmap
        if 'original' in metrics_data:
            extra_tab = ttk.Frame(nb)
            nb.add(extra_tab, text='Extra')
            fig2 = Figure(figsize=(8, 4))
            ax_e = fig2.add_subplot(121)
            ax_h = fig2.add_subplot(122)
            # entropy evolution: split image into blocks
            orig_img = load_image_gray(ORIG_DIR / parent.selected_path.name)
            h, w = orig_img.shape
            bh = bw = getattr(self, 'block_size', 8)
            ent_vals = []
            for y in range(0, h, bh):
                for x in range(0, w, bw):
                    block = orig_img[y:y+bh, x:x+bw]
                    ent_vals.append(entropy(block))
            ax_e.plot(ent_vals)
            ax_e.set_title('Entropy per block')

            corr = pixel_correlations(orig_img)
            sns.heatmap(np.array([[corr['horizontal'], corr['vertical']], [corr['diagonal'], 0.0]]),
                        annot=True, ax=ax_h, cmap='coolwarm')
            ax_h.set_title('Correlation heatmap')
            if self.display_plots:
                canvas2 = FigureCanvasTkAgg(fig2, master=extra_tab)
                canvas2.draw()
                canvas2.get_tk_widget().pack(fill='both', expand=True)
                self.last_fig = fig2
            else:
                out2 = DATA_DIR / (parent.selected_path.stem + '_extra.png')
                fig2.savefig(str(out2))
                self.last_fig = fig2
                ttk.Label(extra_tab, text=f'Extra plots saved to {out2.name}').pack(
                    padx=10, pady=10)


class ImcryptGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        ensure_dirs()
        self.title("imcrypt GUI")
        self.geometry("500x300")

        # Stilizare
        style = ttk.Style()
        style.configure('TButton', padding=5)
        style.configure('TLabel', padding=5)

        # Frame principal
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill='both', expand=True)

        self.selected_path = None
        ttk.Button(main_frame, text="Select Image",
                   command=self.select_image).pack(pady=8)
        self.lbl = ttk.Label(main_frame, text="No image selected")
        self.lbl.pack()

        # Password frame
        pw_frame = ttk.Frame(main_frame)
        pw_frame.pack(pady=8)
        ttk.Label(pw_frame, text="Password:").pack(side='left')
        self.pw = ttk.Entry(pw_frame, show="*")
        self.pw.pack(side='left', padx=5)

        # Block size control for entropy analysis
        block_frame = ttk.Frame(main_frame)
        block_frame.pack(pady=4)
        ttk.Label(block_frame, text="Block size:").pack(side='left')
        self.block_size = tk.Spinbox(
            block_frame, from_=4, to=64, increment=4, width=5)
        self.block_size.pack(side='left', padx=5)

        # Display plots toggle
        self.display_plots_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(block_frame, text='Display plots',
                        variable=self.display_plots_var).pack(side='left', padx=8)

        # Buttons frame
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(pady=8)
        ttk.Button(btn_frame, text="Encrypt",
                   command=self.do_encrypt).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Decrypt",
                   command=self.do_decrypt).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Show Metrics/Plots",
                   command=self.show_metrics).pack(side='left', padx=5)

        # Preview frame
        self.preview_frame = ttk.LabelFrame(
            main_frame, text="Image Preview", padding=5)
        self.preview_frame.pack(fill='x', pady=8)
        self.preview_label = ttk.Label(self.preview_frame)
        self.preview_label.pack()

    def select_image(self):
        p = filedialog.askopenfilename(
            filetypes=[("Images", "*.png;*.jpg;*.jpeg")])
        if not p:
            return
        self.selected_path = Path(p)
        # copy to original folder
        dest = ORIG_DIR / self.selected_path.name
        save_image_gray(load_image_gray(self.selected_path), dest)
        self.lbl.config(text=f"Selected: {self.selected_path.name}")
        # show preview
        try:
            img = Image.open(self.selected_path).convert('L')
            img.thumbnail((200, 200))
            self._tkimg = ImageTk.PhotoImage(img)
            self.preview_label.config(image=self._tkimg)
        except Exception:
            pass

    def do_encrypt(self):
        if not self.selected_path:
            messagebox.showwarning("No image", "Select an image first")
            return
        pwd = self.pw.get()
        if not pwd:
            messagebox.showwarning("No password", "Enter a password")
            return
        img = load_image_gray(ORIG_DIR / self.selected_path.name)
        out = encrypt(img, pwd)
        enc_path = ENC_DIR / (self.selected_path.stem + "_enc.png")
        save_image_gray(out["cipher"], enc_path)
        sidecar = ENC_DIR / (self.selected_path.stem + "_enc.keys.npz")
        np.savez_compressed(
            sidecar, row_perm=out["row_perm"], col_perm=out["col_perm"])
        messagebox.showinfo("Encrypted", f"Saved: {enc_path.name}")
        # update preview with encrypted thumbnail
        try:
            enc_img = Image.open(enc_path).convert('L')
            enc_img.thumbnail((200, 200))
            self._tkimg_enc = ImageTk.PhotoImage(enc_img)
            self.preview_label.config(image=self._tkimg_enc)
        except Exception:
            pass

    def do_decrypt(self):
        if not self.selected_path:
            messagebox.showwarning("No image", "Select an image first")
            return
        pwd = self.pw.get()
        if not pwd:
            messagebox.showwarning("No password", "Enter a password")
            return
        enc_path = ENC_DIR / (self.selected_path.stem + "_enc.png")
        key_path = ENC_DIR / (self.selected_path.stem + "_enc.keys.npz")
        if not enc_path.exists() or not key_path.exists():
            messagebox.showwarning(
                "Missing files", "Encrypted image or keys not found. Run Encrypt first.")
            return
        cipher_img = load_image_gray(enc_path)
        data = np.load(key_path)
        plain = decrypt(cipher_img, pwd, data["row_perm"], data["col_perm"])
        dec_path = DEC_DIR / (self.selected_path.stem + "_dec.png")
        save_image_gray(plain, dec_path)
        messagebox.showinfo("Decrypted", f"Saved: {dec_path.name}")
        # update preview with decrypted thumbnail
        try:
            dec_img = Image.open(dec_path).convert('L')
            dec_img.thumbnail((200, 200))
            self._tkimg_dec = ImageTk.PhotoImage(dec_img)
            self.preview_label.config(image=self._tkimg_dec)
        except Exception:
            pass

    def show_metrics(self):
        if not self.selected_path:
            messagebox.showwarning("No image", "Select an image first")
            return

        # compute metrics for original, encrypted, decrypted (if present)
        orig = ORIG_DIR / self.selected_path.name
        enc = ENC_DIR / (self.selected_path.stem + "_enc.png")
        dec = DEC_DIR / (self.selected_path.stem + "_dec.png")

        results = {}
        if orig.exists():
            oimg = load_image_gray(orig)
            results['original'] = {
                'entropy': entropy(oimg),
                'correlations': pixel_correlations(oimg)
            }

        if enc.exists():
            eimg = load_image_gray(enc)
            results['encrypted'] = {
                'entropy': entropy(eimg),
                'correlations': pixel_correlations(eimg)
            }

        if dec.exists():
            dimg = load_image_gray(dec)
            results['decrypted'] = {
                'entropy': entropy(dimg),
                'correlations': pixel_correlations(dimg)
            }

        # NPCR/UACI original vs encrypted
        if 'original' in results and 'encrypted' in results:
            img1 = load_image_gray(orig)
            img2 = load_image_gray(enc)
            npcr, uaci = npcr_uaci(img1, img2)
            results['npcr_uaci'] = {'NPCR': npcr, 'UACI': uaci}

        # Create metrics window
        # pass block size to metrics window
        try:
            bs = int(self.block_size.get())
        except Exception:
            bs = 8
        display = bool(self.display_plots_var.get())
        metrics_win = MetricsWindow(
            self, results, block_size=bs, display_plots=display)


def run_gui():
    app = ImcryptGUI()
    app.mainloop()
