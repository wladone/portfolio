
"""
Elasticity & Pricing Metrics
Loads processed CSVs, aggregates weekly per ProductID, computes log-log price elasticity and promo uplift.
Outputs:
 - data/processed/dim_pricing_metrics.csv
 - data/processed/pricing_recommendations.csv
Run: python elasticity_pricing.py
"""
import pandas as pd
import numpy as np
from pathlib import Path

root = Path(r"/mnt/data/retail_sales_project")
proc = root / "data" / "processed"

sales = pd.read_csv(proc/"fact_sales.csv", parse_dates=["OrderDate"])
prods = pd.read_csv(proc/"dim_product.csv")
camp = pd.read_csv(proc/"dim_campaign.csv")

# Keep positive transactions for elasticity (exclude returns)
df = sales[sales["Quantity"] > 0].copy()
df["UnitPrice"] = df["NetSales"] / df["Quantity"]
df["Week"] = df["OrderDate"] - pd.to_timedelta(df["OrderDate"].dt.weekday, unit='D')
weekly = df.groupby(["ProductID","Week"], as_index=False).agg(
    Qty=("Quantity","sum"),
    Price=("UnitPrice","mean"),
    PromoShare=("CampaignID", lambda x: (x!=6).mean())
)

# Log transform, drop zeros
wk = weekly[(weekly["Qty"]>0) & (weekly["Price"]>0)].copy()
wk["lnQ"] = np.log(wk["Qty"])
wk["lnP"] = np.log(wk["Price"])

def ols_simple(x, y):
    # y = a + b x
    X = np.column_stack([np.ones(len(x)), x])
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    yhat = X @ beta
    ss_tot = ((y - y.mean())**2).sum()
    ss_res = ((y - yhat)**2).sum()
    r2 = 1 - ss_res/ss_tot if ss_tot>0 else np.nan
    return float(beta[1]), float(r2)

rows = []
for pid, g in wk.groupby("ProductID"):
    if len(g) >= 12:  # at least 12 weekly obs
        b, r2 = ols_simple(g["lnP"].values, g["lnQ"].values)
        promo_uplift = np.nan
        try:
            promo_uplift = g.loc[g["PromoShare"]>0, "Qty"].mean() / g.loc[g["PromoShare"]==0, "Qty"].mean() - 1
        except Exception:
            pass
        rows.append([pid, b, r2, len(g), promo_uplift])

elasticity = pd.DataFrame(rows, columns=["ProductID","Elasticity","R2","Obs","PromoUpliftPct"])
elasticity = elasticity.merge(prods[["ProductID","SKU","ProductName","Category","Subcategory","Brand","TargetMargin"]], on="ProductID", how="left")

# Category-level fallback
cat_rows = []
for cat, g in wk.merge(prods[["ProductID","Category"]], on="ProductID").groupby("Category"):
    if len(g) >= 50:
        b, r2 = ols_simple(g["lnP"].values, g["lnQ"].values)
        cat_rows.append([cat, b, r2, len(g)])
cat_el = pd.DataFrame(cat_rows, columns=["Category","CatElasticity","CatR2","CatObs"])

elasticity = elasticity.merge(cat_el, on="Category", how="left")
elasticity["Elasticity_filled"] = elasticity["Elasticity"].fillna(elasticity["CatElasticity"])

# Simple recommendations
def recommend(row):
    e = row["Elasticity_filled"]
    gm = row.get("TargetMargin", 0.3)
    if pd.isna(e):
        return "Keep price; insufficient data"
    if e > -0.8:
        return "Candidate +2–5% price (inelastic)"
    if e < -1.2 and (row.get("PromoUpliftPct",0) < 0.05):
        return "Reduce discounts; focus on value"
    return "Monitor price; A/B small steps"

recs = elasticity.copy()
recs["Recommendation"] = recs.apply(recommend, axis=1)

# Save outputs
elasticity_out = proc/"dim_pricing_metrics.csv"
recs_out = proc/"pricing_recommendations.csv"
elasticity.to_csv(elasticity_out, index=False)
recs.to_csv(recs_out, index=False)

print("Saved:", elasticity_out, recs_out, len(elasticity), "rows")
