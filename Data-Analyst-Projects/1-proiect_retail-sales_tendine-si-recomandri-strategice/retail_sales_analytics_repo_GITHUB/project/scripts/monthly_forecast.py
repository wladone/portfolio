"""
Monthly forecast & metrics.
Generates:
 - data/processed/fact_monthly_sales.csv
 - data/processed/monthly_forecast.csv (next 3 months)
 - data/processed/forecast_backtest.csv (rolling origin)
 - data/processed/forecast_metrics.csv (MAE, Bias)
"""
import pandas as pd
import numpy as np
from pathlib import Path

root = Path(__file__).resolve().parents[1]
proc = root / "data" / "processed"

fact = pd.read_csv(proc/"fact_sales.csv", parse_dates=["OrderDate"])
monthly = fact.groupby(fact["OrderDate"].dt.to_period("M")).agg(NetSales=("NetSales","sum")).reset_index()
monthly["MonthDate"] = monthly["OrderDate"].dt.to_timestamp()
monthly = monthly[["MonthDate","NetSales"]].sort_values("MonthDate")

y = monthly.set_index("MonthDate")["NetSales"].asfreq("MS")
h = 3

def sarimax_monthly(y, steps):
    try:
        import statsmodels.api as sm
        mod = sm.tsa.statespace.SARIMAX(y, order=(1,1,1), seasonal_order=(1,1,1,12),
                                        enforce_stationarity=False, enforce_invertibility=False)
        res = mod.fit(disp=False)
        fc = res.get_forecast(steps=steps)
        mean = fc.predicted_mean
        conf = fc.conf_int()
        out = (pd.DataFrame({"MonthDate": mean.index, "yhat": mean.values,
                             "yhat_lower": conf.iloc[:,0].values, "yhat_upper": conf.iloc[:,1].values})
               .reset_index(drop=True))
        return out
    except Exception:
        return None

fc = sarimax_monthly(y, h)
if fc is None:
    # fallback: seasonal naive (month-of-year average)
    moy = y.groupby(y.index.month).mean()
    idx = pd.date_range(y.index[-1] + pd.offsets.MonthBegin(1), periods=h, freq="MS")
    yhat = [float(moy.get(m, y.tail(3).mean())) for m in idx.month]
    fc = pd.DataFrame({"MonthDate": idx, "yhat": yhat, "yhat_lower": np.nan, "yhat_upper": np.nan})

# backtest (1-step ahead over last up to 6 months)
def backtest(y, folds=6):
    import warnings
    warnings.filterwarnings("ignore")
    rows = []
    try:
        import statsmodels.api as sm
        for i in range(len(y)-folds, len(y)):
            train = y.iloc[:i]
            model = sm.tsa.statespace.SARIMAX(train, order=(1,1,1), seasonal_order=(1,1,1,12),
                                              enforce_stationarity=False, enforce_invertibility=False)
            res = model.fit(disp=False)
            pred = float(res.forecast(steps=1).iloc[-1])
            rows.append({"MonthDate": y.index[i], "Actual": float(y.iloc[i]), "Forecast": pred})
    except Exception:
        for i in range(len(y)-folds, len(y)):
            prev = y.iloc[i-12] if i-12 >= 0 else y.iloc[i-1]
            rows.append({"MonthDate": y.index[i], "Actual": float(y.iloc[i]), "Forecast": float(prev)})
    return pd.DataFrame(rows)

bt = backtest(y, folds=min(6, len(y)))
bt["Error"] = bt["Actual"] - bt["Forecast"]
metrics = pd.DataFrame([{
    "MAE": float(bt["Error"].abs().mean()),
    "Bias": float(bt["Error"].mean()),
    "N_points": int(len(bt))
}] )

# save
monthly.rename(columns={"NetSales":"ActualNetSales"}, inplace=True)
monthly.to_csv(proc/"fact_monthly_sales.csv", index=False)
fc.to_csv(proc/"monthly_forecast.csv", index=False)
bt.to_csv(proc/"forecast_backtest.csv", index=False)
metrics.to_csv(proc/"forecast_metrics.csv", index=False)
print("Saved monthly forecast & metrics to", proc)
