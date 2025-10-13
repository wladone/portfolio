from typing import List, Dict
import pandas as pd


class FinancialAuditor:
    def __init__(self, data: Dict[str, pd.DataFrame]):
        self.data = data
        self.results = {}

    def analyze_cash(self) -> pd.DataFrame:
        df = self.data.get("cash")
        if df is not None:
            df["abs_diff"] = abs(
                df["begin_balance"] + df["inflows"] - df["outflows"] - df["end_balance"])
            df["test_result"] = df["abs_diff"] < 1e-2
            self.results["cash"] = df
        return df

    def analyze_generic(self, key: str, columns: List[str]) -> pd.DataFrame:
        df = self.data.get(key)
        if df is not None:
            df = df[columns].copy()
            df["total"] = df.sum(axis=1)
            self.results[key] = df.describe()
        return df

    def analyze_cost_of_sales(self) -> pd.DataFrame:
        df = self.data.get("cost_of_sales")
        if df is not None:
            df["computed_total"] = df["unit_cost"] * df["units_sold"]
            df["diff"] = abs(df["computed_total"] - df["total_cost"])
            df["test_result"] = df["diff"] < 1e-2
            self.results["cost_of_sales"] = df
        return df

    def analyze_operating_expenses(self) -> pd.DataFrame:
        df = self.data.get("operating_expenses")
        if df is not None:
            summary = df.groupby("category")[
                "monthly_cost"].sum().reset_index()
            self.results["operating_expenses"] = summary
        return df

    def analyze_ppe(self) -> pd.DataFrame:
        df = self.data.get("ppe")
        if df is not None:
            df["recalc_nbv"] = df["acquisition_cost"] - df["accum_depreciation"]
            df["test_result"] = abs(
                df["recalc_nbv"] - df["net_book_value"]) < 1e-2
            self.results["ppe"] = df
        return df

    def export_results(self, output_path: str) -> None:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for sheet, result in self.results.items():
                result.to_excel(writer, sheet_name=sheet)
