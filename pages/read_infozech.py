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
    page_title="read_infozech",
    page_icon="https://raw.githubusercontent.com/databricks/design-assets/main/logo/icon-red-32.png"
)


st.title("🚀 Pull Data from Ibill into Databricks")

table_names = [
    "advancetowerbilling",
    "advancepreparebillable",
    "amendmentcalc",
    "amendmentquantities",
    "arrearpreparebillable",
    "backbillingpreparebillable",
    "backbillingtowerbilling",
    "baseleaserate",
    "escalationindex",
    "escalationmla",
    "fxrates",
    "sitedetails",
    "tenancybysitecalc",
    "tenancydetails",
    "amendmentpricing"
]

selected_table = st.selectbox(
    "📦 Select a Delta Table",
    options=table_names,
    index=0  # default selection
)

st.success(f"✅ You selected table: `{selected_table}`")

country_codes = {
    "Tanzania": "tz",
    "Malawi": "mw",
    "DRC": "cd",
    "Congo B": "cg",
    "Ghana": "gh",
    "Madagascar": "mg",
    "Senegal": "sn",
    "Oman": "om",
    "South Africa": "za"
}

country_names = ["All"] + sorted(country_codes.keys())

def render_flag_radio(label: str = "🌍 Select Country") -> str:
     
    display_options = []

    for name in country_names:
        if name == "All":
            display_options.append("🌍 All")
        else:
            iso = country_codes[name]
            img_url = f"https://flagcdn.com/16x12/{iso}.png"
            html = f'<img src="{img_url}" width="16" height="12" style="vertical-align:middle;margin-right:8px;"> {name}'
            display_options.append(html)

    # Use index-based keys for stable selection
    selected_idx = st.radio(
        label,
        options=list(range(len(country_names))),
        format_func=lambda i: country_names[i],
        index=0
    )

    st.markdown(f"**Selected:** {display_options[selected_idx]}", unsafe_allow_html=True)

    return country_names[selected_idx]

param_country = render_flag_radio()
# Use selected_country for filtering, etc.

# st.write(f"🔎 You selected: **{selected_country}**")

st.write("📅 Select Creation Date and Time")

selected_date = st.date_input("Select Date", value=datetime.today().date())
selected_time = st.time_input("Select Time", value=time(0, 0))

# Combine and format
creationdate = datetime.combine(selected_date, selected_time).strftime("%d-%m-%Y %H:%M")

st.code(f"creationdate = '{creationdate}'", language="python")

if st.button("Run Job in Databricks"):
    params = {
        "creationdate" : creationdate,
        "country": param_country,
        "table": selected_table
    }
    result = trigger_job(INSTANCE, TOKEN,"227973945273149", params)
    if result:
        run_id = result["run_id"]
        st.success(f"Triggered Job  | Run ID: {run_id}")
        monitor_job(INSTANCE, TOKEN, run_id)
    else:
        st.error("Failed to trigger Job")
