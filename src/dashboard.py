# dashboard.py

import streamlit as st
import pandas as pd
from supabase_io import load_electricity_sales, load_forecasts
import plotly.express as px
from sklearn.metrics import mean_absolute_error, root_mean_squared_error
import numpy as np


# --- Page config ---
st.set_page_config(
    page_title="Electricity Sales Forecast Dashboard",
    page_icon="⚡",
    layout="wide"
)

# --- Header ---
st.title("⚡ Electricity Sales Forecast Dashboard")
st.markdown(
    "Visualizing actual (NJ residential) electricity sales and model forecasts. "
    "Data is updated monthly from the EIA API."
)

# --- Load data ---
sales_df = load_electricity_sales()
forecast_df = load_forecasts()

# Ensure monthly period format (YYYY-MM)
sales_df["month"] = sales_df["period"].dt.to_period("M")
forecast_df["month"] = forecast_df["period"].dt.to_period("M")


# --- Sidebar controls ---
st.sidebar.header("Filters")

# 1. Model Selector
available_models = forecast_df["model"].unique().tolist()
selected_models = st.sidebar.multiselect(
    "Select models:",
    available_models,
    default=available_models
)

# 2. Date range selector
min_month = sales_df["month"].min()
max_month = max(sales_df["month"].max(), forecast_df["month"].max())  # cover both actuals and forecasts
max_month = max_month + 1  # +1 month beyond the last forecast so the line doesn't cut off

# Build sorted list of months as strings
month_options = pd.period_range(min_month, max_month, freq="M")
month_strs = month_options.astype(str)

start_month, end_month = st.sidebar.select_slider(
    "Select Month Range",
    options=month_strs,
    value=(month_strs[-36] if len(month_strs) >= 36 else month_strs[0], month_strs[-1])
)
start_month = pd.Period(start_month, freq="M")
end_month = pd.Period(end_month, freq="M")

# 3. Evaluation period selector
eval_period = st.sidebar.selectbox(
    "Evaluation Period",
    ["Last Month", "Last 3 Months", "Last 6 Months", "Last 12 Months"]
)
period_map = {
    "Last Month": 1,
    "Last 3 Months": 3,
    "Last 6 Months": 6,
    "Last 12 Months": 12,
}

# ---- Filter data ----
sales_df = sales_df[(sales_df["month"] >= start_month) & (sales_df["month"] <= end_month)]
forecast_df = forecast_df[
    (forecast_df["month"] >= start_month) & (forecast_df["month"] <= end_month)
    & (forecast_df["model"].isin(selected_models))
]


# --- Main Graph: Actuals vs Forecasts ---
fig = px.line(sales_df, x=sales_df["month"].astype(str), y="sales", title="Electricity Sales vs Forecasts")

for model in selected_models:
    model_forecast = forecast_df[forecast_df["model"] == model]
    fig.add_scatter(x=model_forecast["month"].astype(str), y=model_forecast["forecast"], mode="lines+markers", name=f"{model} Forecast")

# dynamic theme colors
fig.update_layout(
    template="plotly_dark" if st.get_option("theme.base") == "dark" else "plotly_white",
    xaxis_title="Month",
    yaxis_title="Electricity Sales (MWh)",
)

st.plotly_chart(fig, use_container_width=True)

# ---- Evaluation Metrics ----
st.subheader("Forecast Evaluation Metrics")

# evaluation period
months_back = period_map[eval_period]
end_eval = sales_df["month"].max()
start_eval = end_eval - months_back + 1

eval_sales = sales_df[(sales_df["month"] >= start_eval) & (sales_df["month"] <= end_eval)]
eval_forecasts = forecast_df[(forecast_df["month"] >= start_eval) & (forecast_df["month"] <= end_eval)]

# Calculate metrics
metrics_list = []
for model in selected_models:
    merged = eval_sales.merge(
        eval_forecasts[eval_forecasts["model"] == model],
        on="month", how="inner"
    )
    if len(merged) > 0:
        mae = mean_absolute_error(merged["sales"], merged["forecast"])
        rmse = root_mean_squared_error(merged["sales"], merged["forecast"])
        mape = np.mean(np.abs((merged["sales"] - merged["forecast"]) / merged["sales"])) * 100
        metrics_list.append({"model": model, "MAE": mae, "RMSE": rmse, "MAPE": mape})

if metrics_list:
    metrics_df = pd.DataFrame(metrics_list)
    metrics_df = metrics_df.melt(id_vars="model", var_name="Metric", value_name="Value")

    bar_fig = px.bar(
        metrics_df,
        x="Metric", y="Value", color="model",
        barmode="group",
        title=f"Evaluation Metrics ({eval_period})"
    )
    bar_fig.update_layout(
        template="plotly_dark" if st.get_option("theme.base") == "dark" else "plotly_white"
    )
    st.plotly_chart(bar_fig, use_container_width=True)
else:
    st.info("No data available for the selected evaluation period.")