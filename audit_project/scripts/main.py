from auditor import FinancialAuditor
from utils import load_data, sample_dataframe
import sys
import os

# Adaugă folderul părinte (root-ul proiectului) în sys.path înainte de orice import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


if __name__ == "__main__":
    paths = [
        "data/cash.csv",
        "data/payable.csv",
        "data/receivable.csv",
        "data/cost_of_sales.csv",
        "data/operating_expenses.csv",
        "data/ppe.csv"
    ]

    keys = ["cash", "payable", "receivable",
            "cost_of_sales", "operating_expenses", "ppe"]

    data = load_data(paths, keys)
    auditor = FinancialAuditor(data)

    auditor.analyze_cash()
    auditor.analyze_generic("payable", ["invoice_amount", "paid_amount"])
    auditor.analyze_generic(
        "receivable", ["invoice_amount", "received_amount"])
    auditor.analyze_cost_of_sales()
    auditor.analyze_operating_expenses()
    auditor.analyze_ppe()
