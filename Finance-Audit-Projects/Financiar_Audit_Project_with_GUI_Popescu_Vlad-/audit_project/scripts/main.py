import os
import sys
from auditor import FinancialAuditor
from utils import load_data, sample_dataframe


SCRIPT_DIR = os.path.dirname(__file__)

PROJECT_ROOT = os.path.abspath(os.path.join(
    SCRIPT_DIR, '..'))

DATA_DIR = os.path.join(PROJECT_ROOT, 'data')


sys.path.append(PROJECT_ROOT)

if __name__ == "__main__":

    paths = [
        os.path.join(DATA_DIR, "cash.csv"),
        os.path.join(DATA_DIR, "payable.csv"),
        os.path.join(DATA_DIR, "receivable.csv"),
        os.path.join(DATA_DIR, "cost_of_sales.csv"),
        os.path.join(DATA_DIR, "operating_expenses.csv"),
        os.path.join(DATA_DIR, "ppe.csv"),
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
