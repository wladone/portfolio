# scripts/generate_charts.py

import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def ensure_output_dir(path: str):
    os.makedirs(path, exist_ok=True)

def receivable_heatmap(data_path: str, out_path: str):
    df = pd.read_csv(data_path)
    corr = df.corr()
    sns.set()  # seaborn default style
    plt.figure(figsize=(8, 6))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap="viridis")
    plt.title("Receivable Data Correlation Heatmap")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()

def operating_expenses_bar(data_path: str, out_path: str):
    df = pd.read_csv(data_path)
    summary = df.groupby("category")["monthly_cost"].sum().reset_index()
    sns.set()
    plt.figure(figsize=(10, 6))
    sns.barplot(data=summary, x="category", y="monthly_cost")
    plt.xlabel("Category")
    plt.ylabel("Total Monthly Cost")
    plt.title("Operating Expenses by Category")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()

def cost_of_sales_histogram(data_path: str, out_path: str):
    df = pd.read_csv(data_path)
    sns.set()
    plt.figure(figsize=(8, 6))
    sns.histplot(data=df, x="total_cost", bins=30, kde=False)
    plt.xlabel("Total Cost")
    plt.ylabel("Frequency")
    plt.title("Distribution of Cost of Sales")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()

if __name__ == "__main__":
    chart_dir = os.path.join("outputs", "charts")
    ensure_output_dir(chart_dir)

    receivable_heatmap(
        data_path=os.path.join("data", "receivable.csv"),
        out_path=os.path.join(chart_dir, "receivable_heatmap.png")
    )
    operating_expenses_bar(
        data_path=os.path.join("data", "operating_expenses.csv"),
        out_path=os.path.join(chart_dir, "operating_expenses_bar.png")
    )
    cost_of_sales_histogram(
        data_path=os.path.join("data", "cost_of_sales.csv"),
        out_path=os.path.join(chart_dir, "cost_of_sales_hist.png")
    )

    print(f"Charts saved to {chart_dir}")
