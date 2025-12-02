import requests
import json
import time
import streamlit as st

def trigger_job(instance, token, job_id, params):
    url = f"https://{instance}/api/2.1/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "job_id": job_id,
        "notebook_params": params
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        return response.json()  
    else:
        print(response.status_code)
        print(response)


def get_run_status(instance, token, run_id):
    url = f"https://{instance}/api/2.1/jobs/runs/get?run_id={run_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    response = requests.get(url, headers=headers)
    return response.json() if response.status_code == 200 else None

def monitor_job(instance, token, run_id):
    status = "PENDING"
    progress = st.empty()
    while status not in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        run_data = get_run_status(instance, token, run_id)
        if run_data:
            life_cycle_state = run_data['state']['life_cycle_state']
            result_state = run_data['state'].get('result_state', 'N/A')
            progress.markdown(f"**Status**: {life_cycle_state} | **Result**: {result_state}")
            status = life_cycle_state
        else:
            progress.warning("Error fetching status.")
            break
        time.sleep(5)
    st.success("Job finished.")
