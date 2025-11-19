import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# Same DB URL / schema, but domain is now ride-sharing trips.
st.set_page_config(page_title="Real-Time Ride-Sharing Dashboard", layout="wide")
st.title("🚕 Real-Time Ride-Sharing Trips Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"


@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)


engine = get_engine(DATABASE_URL)


def load_data(status_filter: str | None = None, limit: int = 200) -> pd.DataFrame:
    base_query = "SELECT * FROM orders"
    params: dict = {}
    if status_filter and status_filter != "All":
        base_query += " WHERE status = :status"
        params["status"] = status_filter
    base_query += " ORDER BY order_id DESC LIMIT :limit"
    params["limit"] = limit

    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()


# Sidebar controls
# Keep underlying status strings in sync with producer, but describe them as trip status.
status_options = ["All", "Requested", "Ongoing", "Completed", "Cancelled"]
selected_status = st.sidebar.selectbox("Filter by Trip Status", status_options)
update_interval = st.sidebar.slider(
    "Update Interval (seconds)", min_value=2, max_value=20, value=5
)
limit_records = st.sidebar.number_input(
    "Number of recent trips to load", min_value=50, max_value=2000, value=200, step=50
)

if st.sidebar.button("Refresh now"):
    st.rerun()

placeholder = st.empty()

while True:
    df_trips = load_data(selected_status, limit=int(limit_records))

    with placeholder.container():
        if df_trips.empty:
            st.warning("No trip records found. Waiting for data...")
            time.sleep(update_interval)
            continue

        if "timestamp" in df_trips.columns:
            df_trips["timestamp"] = pd.to_datetime(df_trips["timestamp"])

        # KPIs (renamed for trips)
        total_trips = len(df_trips)
        total_fare = df_trips["value"].sum()
        avg_fare = total_fare / total_trips if total_trips > 0 else 0.0
        completed = len(df_trips[df_trips["status"] == "Completed"])
        cancelled = len(df_trips[df_trips["status"] == "Cancelled"])
        completion_rate = (completed / total_trips * 100) if total_trips > 0 else 0.0

        st.subheader(
            f"Displaying {total_trips} trips (Status filter: {selected_status})"
        )

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Trips", total_trips)
        k2.metric("Total Fare", f"${total_fare:,.2f}")
        k3.metric("Average Fare", f"${avg_fare:,.2f}")
        k4.metric("Completion Rate", f"{completion_rate:,.2f}%")
        k5.metric("Cancelled Trips", cancelled)

        st.markdown("### Recent Trips (Top 10)")
        st.dataframe(df_trips.head(10), use_container_width=True)

        # Charts
        # category -> trip_type; value -> fare_usd
        grouped_type = (
            df_trips.groupby("category")["value"]
            .sum()
            .reset_index()
            .sort_values("value", ascending=False)
        )
        fig_type = px.bar(
            grouped_type,
            x="category",
            y="value",
            title="Total Fare by Trip Type",
            labels={"category": "Trip Type", "value": "Total Fare (USD)"},
        )

        grouped_city = df_trips.groupby("city")["value"].sum().reset_index()
        fig_city = px.pie(
            grouped_city,
            values="value",
            names="city",
            title="Fare Distribution by City",
        )

        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            st.plotly_chart(fig_type, use_container_width=True)
        with chart_col2:
            st.plotly_chart(fig_city, use_container_width=True)

        st.markdown("---")
        st.caption(
            f"Last updated: {datetime.now().isoformat()} • Auto-refresh: {update_interval}s"
        )

    time.sleep(update_interval)
