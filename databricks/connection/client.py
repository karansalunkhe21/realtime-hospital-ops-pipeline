# ============================================================
# databricks/connection/client.py
#
# One function: get_client()
# Returns a connected Databricks SDK client.
# Used by the consumer and the notebook runner.
#
# This replaces snowflake/connection.py from the old project.
# ============================================================

from databricks.sdk import WorkspaceClient
from config.settings import DatabricksConfig


def get_client():
    """
    Return a connected Databricks WorkspaceClient.

    Usage:
        client = get_client()
        client.clusters.list()  # example call
    """
    client = WorkspaceClient(
        host  = DatabricksConfig.host,
        token = DatabricksConfig.token,
    )
    return client


def run_notebook(notebook_path: str, params: dict = None):
    """
    Trigger a Databricks notebook to run on the cluster and wait for it.

    Args:
        notebook_path: path inside Databricks workspace e.g. '/hospital/silver'
        params:        optional dict of parameters to pass into the notebook

    Usage:
        run_notebook('/hospital/silver', {'run_date': '2024-06-01'})
    """
    from databricks.sdk.service.jobs import RunNow
    client = get_client()

    settings = {
        "existing_cluster_id": DatabricksConfig.cluster_id,
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": params or {},
        },
    }

    run = client.jobs.submit(run_name=f"run-{notebook_path.split('/')[-1]}", **settings)
    print(f"▶ Notebook '{notebook_path}' started. Run ID: {run.run_id}")

    # Wait until the notebook finishes (blocks until done)
    result = client.jobs.wait_get_run_job_terminated_or_skipped(run_id=run.run_id)
    state  = result.state.result_state.value
    print(f"  {'✅' if state == 'SUCCESS' else '❌'} Finished with state: {state}")
    return state
