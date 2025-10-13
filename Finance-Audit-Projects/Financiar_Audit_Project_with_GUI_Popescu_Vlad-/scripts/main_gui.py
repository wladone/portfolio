import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox


SCRIPT_DIR = os.path.dirname(__file__)
PROJECT_DIR = os.path.abspath(os.path.join(
    SCRIPT_DIR, '..'))  # .../audit_project
# .../audit_project/outputs
OUTPUT_DIR = os.path.join(PROJECT_DIR, 'outputs')
os.makedirs(OUTPUT_DIR, exist_ok=True)


class AuditGUI:

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Audit Financiar - GUI")
        self._build_main_window()

    def _build_main_window(self):
        tests = [
            ("Audit Cash",               'cash'),
            ("Audit Payable",            'payable'),
            ("Audit Receivable",         'receivable'),
            ("Audit Cost of Sales",      'cost_of_sales'),
            ("Audit Operating Expenses", 'operating_expenses'),
            ("Audit PPE",                'ppe'),
        ]
        for idx, (label, key) in enumerate(tests):
            btn = tk.Button(
                self.root,
                text=label,
                width=25,
                command=lambda k=key: self._open_test_window(k)
            )
            btn.grid(row=idx, column=0, padx=10, pady=5)

        tk.Button(
            self.root,
            text="Inchide",
            width=25,
            command=self.root.quit
        ).grid(row=len(tests), column=0, padx=10, pady=20)

    def _open_test_window(self, test_key: str):
        win = tk.Toplevel(self.root)
        win.title(f"Test: {test_key}")

        file_var = tk.StringVar()
        tk.Label(win, text="IncarcA fisier CSV:").grid(
            row=0, column=0, padx=10, pady=5)
        tk.Entry(win, textvariable=file_var, width=40).grid(
            row=0, column=1, padx=10, pady=5)
        tk.Button(win, text="Browse...", command=lambda: self._browse_file(file_var))\
          .grid(row=0, column=2, padx=5)
        tk.Button(win, text="Executa test", width=20,
                  command=lambda: self._run_test(test_key, file_var.get()))\
            .grid(row=1, column=1, pady=10)

    def _browse_file(self, var: tk.StringVar):
        filepath = filedialog.askopenfilename(
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if filepath:
            var.set(filepath)

    def _run_test(self, test_key: str, path: str):
        if not path or not os.path.isfile(path):
            messagebox.showerror("Eroare", "Selectati un fisier CSV valid.")
            return

        try:
            df = pd.read_csv(path)
            from auditor import FinancialAuditor
            auditor = FinancialAuditor({test_key: df})

            analysis_map = {
                'cash':               auditor.analyze_cash,
                'payable': lambda: auditor.analyze_generic('payable', ['invoice_amount', 'paid_amount']),
                'receivable': lambda: auditor.analyze_generic('receivable', ['invoice_amount', 'received_amount']),
                'cost_of_sales':      auditor.analyze_cost_of_sales,
                'operating_expenses': auditor.analyze_operating_expenses,
                'ppe':                auditor.analyze_ppe,
            }
            if test_key not in analysis_map:
                messagebox.showerror("Eroare", f"Test necunoscut: {test_key}")
                return

            result_df = analysis_map[test_key]()

            csv_path = os.path.join(OUTPUT_DIR, f"{test_key}_results.csv")
            xlsx_path = os.path.join(OUTPUT_DIR, f"{test_key}_results.xlsx")
            result_df.to_csv(csv_path, index=False)
            result_df.to_excel(xlsx_path, index=False)

            messagebox.showinfo(
                "Succes",
                f"Rezultatele au fost salvate:\n• CSV: {csv_path}\n• Excel: {xlsx_path}"
            )

        except Exception as e:
            messagebox.showerror("Eroare la executie", str(e))


if __name__ == "__main__":
    root = tk.Tk()
    app = AuditGUI(root)
    root.mainloop()
