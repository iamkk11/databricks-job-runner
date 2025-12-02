import streamlit as st
from utils.databricks import trigger_job, monitor_job
from dotenv import load_dotenv
from datetime import datetime,time
import os

# load_dotenv()

# INSTANCE = os.environ['DATABRICKS_SERVER_HOSTNAME']
# TOKEN = os.environ['DATABRICKS_ACCESS_TOKEN']

INSTANCE = st.secrets['DATABRICKS']['DATABRICKS_SERVER_HOSTNAME']
TOKEN = st.secrets['DATABRICKS']['DATABRICKS_ACCESS_TOKEN']


st.set_page_config(
    page_title="read_s4hana",
    # page_icon="👋",
)

st.title("🚀 Pull Data from s4Hana into Databricks")

table_names2 = [
    "aged_debt",
    "aps_iam_api_buser",
    "bank_detail",
    "business_partner",
    "company_code",
    "document_types",
    "fx_rates",
    "gl_account_line_item",
    "material_stock",
    "product",
    "purchase_order",
    "purchase_order_item",
    "purchase_order_schedule_line",
    "real_estate_contract",
    "stock_movement",
    "supplier",
    "trial_balance"
]

table_names = [
    "aged_debt",
    "fx_rates",
    "gl_account_line_item",
    "trial_balance"
]

aged_debt_options = [
    "no",
    "yes"
]



selected_table = st.selectbox(
    "📦 Select a Delta Table",
    options=table_names,
    index=0  # default selection
)

st.success(f"✅ You selected table: `{selected_table}`")


# Use selected_country for filtering, etc.

# st.write(f"🔎 You selected: **{selected_country}**")

aged_debt_2 = 'no'
aged_debt_3 = 'no'

if selected_table == "aged_debt":

    st.write("📅 Aged Debt Table Filters")

    aged_debt_2 = st.selectbox(
        "📦 Select if Aged Debt ClearingDate Filter Applied ",
        options=aged_debt_options,
        index=0  # default selection
    )

    aged_debt_3 = st.selectbox(
        "📦 Select if Aged Debt ClearingDate null and GLAccount eq '12100000' or GLAccount eq '12100005' ",
        options=aged_debt_options,
        index=0  # default selection
    )

st.write("Start and End Time Selections")

selected_date_start = st.date_input("Select Date", value=datetime.today().date(),key='12')
selected_time_start = st.time_input("Select Time", value=time(0, 0),key='434')

# Combine and format
start = datetime.combine(selected_date_start, selected_time_start).strftime("%Y-%m-%dT%H:%M:%S")

st.code(f"start = '{start}'", language="python")

selected_date_end = st.date_input("Select Date", value=datetime.today().date())
selected_time_end = st.time_input("Select Time", value=time(0, 0))

# Combine and format
end = datetime.combine(selected_date_end, selected_time_end).strftime("%Y-%m-%dT%H:%M:%S")

st.code(f"end = '{end}'", language="python")

if st.button("Run Job in Databricks"):

    params = {
        "aged_debt_2" : aged_debt_2,
        "aged_debt_3" : aged_debt_3,
        "table": selected_table,
        "start":start,
        "end" : end
    }

    result = trigger_job(INSTANCE, TOKEN, "826216579129412", params)

    if result:
        run_id = result["run_id"]
        st.success(f"Triggered Job  | Run ID: {run_id}")
        monitor_job(INSTANCE, TOKEN, run_id)
    else:
        st.error("Failed to trigger Job")
