# src/dashboard.py
"""
Streamlit dashboard for visualizing NJ residential electricity sales and model forecasts.
Loads data from Supabase, provides interactive filters, and displays time series and evaluation metrics.
Updated monthly with new EIA data and forecasts.
"""

import streamlit as st
import pandas as pd
from supabase_io import load_electricity_sales, load_forecasts
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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

# Define a set of colors for the models (adjust as needed)
model_colors = {available_models[0]: '#EF553B', available_models[1]: '#00CC96'} 

for model in selected_models:
    model_forecast = forecast_df[forecast_df["model"] == model]
    fig.add_scatter(x=model_forecast["month"].astype(str), y=model_forecast["forecast"], mode="lines+markers", name=f"{model} Forecast", line=dict(color=model_colors[model]))

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
    df_long = metrics_df.melt(
        id_vars=['model'],
        value_vars=['MAE', 'RMSE', 'MAPE'],
        var_name='Metric',
        value_name='Value'
    )

    # Identify which metrics go on the primary (y1) and secondary (y2) axis
    primary_metrics = ['MAE', 'RMSE']
    secondary_metrics = ['MAPE']

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Get unique models for iteration and colors
    models = list(df_long['model'].unique())
    bar_width = 0.3 # Width of each individual bar

    # Primary Y-axis traces (MAE, RMSE)
    for i, model in enumerate(models):
        df_model = df_long[
            (df_long['model'] == model) & 
            (df_long['Metric'].isin(primary_metrics))
        ]
        
        # Calculate offset group based on model index to place bars side-by-side
        # We use a custom 'offset' here for fine-tuning positioning relative to the metric label.
        # Note: If there are only two models, using barmode='group' in fig.update_layout might be enough, 
        # but with secondary y-axes, explicitly defining 'offset' helps guarantee grouping.
        
        # Set bar offset to position Model A and Model B relative to the x-tick center.
        # For two models (0 and 1): 
        # i=0 (Model A) -> offset = -bar_width/2
        # i=1 (Model B) -> offset = +bar_width/2
        offset = (i - (len(models) - 1) / 2) * bar_width 

        fig.add_trace(
            go.Bar(
                name=f'{model}',
                x=df_model['Metric'],
                y=df_model['Value'],
                marker_color=model_colors[model],
                yaxis='y1',             # Explicitly set to primary y-axis
                offset=offset,          # Custom offset for grouping
                width=bar_width,        # Set bar width
                legendgroup=str(i),     # Group models together in legend
                showlegend=True
            ),
            secondary_y=False,
        )

    # Secondary Y-axis traces (MAPE)
    for i, model in enumerate(models):
        df_model_mape = df_long[
            (df_long['model'] == model) & 
            (df_long['Metric'].isin(secondary_metrics))
        ]
        
        # Use the same offset for grouping consistency at the 'MAPE' x-tick
        offset = (i - (len(models) - 1) / 2) * bar_width
        
        # Add a separate trace for MAPE on the secondary axis
        fig.add_trace(
            go.Bar(
                name=f'{model}',
                x=df_model_mape['Metric'],
                y=df_model_mape['Value'],
                marker_color=model_colors[model],
                yaxis='y2',             # Explicitly set to secondary y-axis
                offset=offset,          # Custom offset for grouping
                width=bar_width,
                legendgroup=str(i),
                showlegend=False      # Prevent duplicate legend entries
            ),
            secondary_y=True,
        )

    # Update layout for chart appearance
    fig.update_layout(
        title_text="Model Performance Metrics",
        xaxis_title="Metric",
        # Set barmode to 'group' to make the offset effective for grouped appearance
        barmode='group', 
        bargap=0.15, # Space between groups
        bargroupgap=0.1, # Space between bars in a group
        legend_title_text="Model",
        
        # Primary Y-axis (MAE, RMSE) configuration
        yaxis=dict(
            title="MAE / RMSE",
            side="left",
            showgrid=True
        ),
        
        # Secondary Y-axis (MAPE) configuration
        yaxis2=dict(
            title="MAPE(%)",
            overlaying="y",  # Crucial for dual y-axes
            side="right",
            showgrid=False # Optional: hide grid to reduce clutter
        )
    )

    st.plotly_chart(fig, use_container_width=True)


    # bar_fig = px.bar(
    #     metrics_df,
    #     x="Metric", y="Value", color="model",
    #     barmode="group",
    #     title=f"Evaluation Metrics ({eval_period})"
    # )
    # bar_fig.update_layout(
    #     template="plotly_dark" if st.get_option("theme.base") == "dark" else "plotly_white"
    # )
    # st.plotly_chart(bar_fig, use_container_width=True)

else:
    st.info("No data available for the selected evaluation period.")