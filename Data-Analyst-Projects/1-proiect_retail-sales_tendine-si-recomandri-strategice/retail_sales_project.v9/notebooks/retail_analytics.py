
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

root = Path("..").resolve().parent / "data" / "processed" if (Path.cwd() / ".." / "data" / "processed").exists() else Path("/mnt/data/retail_sales_project/data/processed")
fact = pd.read_csv(root/"fact_sales.csv", parse_dates=["OrderDate"])
dim_prod = pd.read_csv(root/"dim_product.csv")

# 1) Daily sales & MA28
daily = fact.groupby("OrderDate", as_index=False)["NetSales"].sum()
daily["MA28"] = daily["NetSales"].rolling(28, min_periods=7).mean()

plt.figure()
plt.plot(daily["OrderDate"], daily["NetSales"], label="Daily Sales")
plt.plot(daily["OrderDate"], daily["MA28"], label="MA28")
plt.title("Daily NetSales & 28-day Moving Average")
plt.xlabel("Date"); plt.ylabel("NetSales"); plt.legend(); plt.show()

# 2) Monthly totals
daily["YearMonth"] = daily["OrderDate"].dt.to_period("M").astype(str)
monthly = daily.groupby("YearMonth", as_index=False)["NetSales"].sum()

plt.figure()
plt.bar(monthly["YearMonth"], monthly["NetSales"])
plt.title("Monthly NetSales")
plt.xlabel("Year-Month"); plt.ylabel("NetSales"); plt.xticks(rotation=45, ha="right"); plt.show()

# 3) Price vs Qty (category weekly means)
df = fact[fact["Quantity"]>0].copy()
df["UnitPrice"] = df["NetSales"] / df["Quantity"]
df["Week"] = df["OrderDate"] - pd.to_timedelta(df["OrderDate"].dt.weekday, unit='D')
wk = df.groupby(["ProductID","Week"], as_index=False).agg(Qty=("Quantity","sum"), Price=("UnitPrice","mean"))
wk = wk.merge(dim_prod[["ProductID","Category"]], on="ProductID", how="left")
cat_week = wk.groupby(["Category","Week"], as_index=False).agg(Qty=("Qty","mean"), Price=("Price","mean"))

plt.figure()
plt.scatter(cat_week["Price"], cat_week["Qty"])
plt.title("Price vs Quantity (Category-level Weekly Means)")
plt.xlabel("Price (mean)"); plt.ylabel("Quantity (mean)"); plt.show()
