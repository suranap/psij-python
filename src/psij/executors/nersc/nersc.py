from typing import Optional, List
import logging
import requests
from pathlib import Path
import inspect
import io
from psij import Job, JobExecutor, JobExecutorConfig, JobStatus, JobState, JobSpec, ResourceSpec
from psij.executors.batch.script_generator import TemplatedScriptGenerator
from psij.executors.remote_job_executor import RemoteJobExecutor, RemoteJobExecutorConfig
from sfapi_client import Client
from sfapi_client.compute import Machine

logger = logging.getLogger(__name__)

class NERSCExecutorConfig(RemoteJobExecutorConfig):
    """Configuration for the NERSC executor."""
    def __init__(self, client_id: str = '', client_private_key: str = '', token: str = ''):
        super().__init__(token=token)
        self.client_id = client_id
        self.client_private_key = client_private_key


class NERSCExecutor(RemoteJobExecutor):
    _NAME_ = "NERSC Executor"
    _VERSION_ = "0.1.0"
    _DESCRIPTION_ = "Executor for the NERSC computing facility"

    def __init__(self, url: Optional[str] = None,
                 config: Optional[NERSCExecutorConfig] = None):
        super().__init__(url=url or "https://api.nersc.gov/api/v1.2", config=config or NERSCExecutorConfig())
        path_to_template = Path(inspect.getfile(TemplatedScriptGenerator)).parent / 'slurm' / 'slurm.mustache'
        self.generator = TemplatedScriptGenerator(self.config, path_to_template)

    def _submit_job(self, job: Job) -> str:
        logger.info(f"Submitting job to NERSC: {job}")
        url = f"{self.url}/compute/jobs/perlmutter"
        
        # This is a placeholder for job script generation
        job_path = "/home/suranap/hostname.sh"
        callback_email = "suranap@stanford.edu"

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.config.token}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {
            "isPath": "true",
            "job": job_path,
            "args": "",
            "callbackTimeout": "0",
            "callbackEmail": callback_email,
            "callbackUrl": "string"
        }

        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        response_data = response.json()
        native_id = response_data.get("jobid")
        if not native_id:
            raise Exception("Failed to get job ID from NERSC")
        logger.info(f"Job submitted to NERSC, received native_id: {native_id}")
        return str(native_id)

    def _cancel_job(self, native_id: str) -> None:
        logger.info(f"Cancelling job {native_id} on NERSC.")
        url = f"{self.url}/compute/jobs/perlmutter/{native_id}"
        headers = {"Authorization": f"Bearer {self.config.token}"}
        response = requests.delete(url, headers=headers)
        response.raise_for_status()
        logger.info(f"Job {native_id} cancellation request sent.")

    def _list_jobs(self) -> List[str]:
        logger.info("Listing jobs on NERSC.")
        url = f"{self.url}/compute/jobs/perlmutter"
        params = {"index": 0, "sacct": "false", "cached": "true"}
        headers = {"Authorization": f"Bearer {self.config.token}"}
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        job_data_list = response.json()
        job_ids = [str(job_info.get("jobid")) for job_info in job_data_list if job_info.get("jobid")]
        logger.info(f"Found {len(job_ids)} jobs.")
        return job_ids

    def _get_job_status(self, native_id: str) -> JobStatus:
        logger.info(f"Getting status for job {native_id} from NERSC.")
        url = f"{self.url}/compute/jobs/perlmutter/{native_id}"
        headers = {"Authorization": f"Bearer {self.config.token}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        job_info = response.json()
        nersc_state = job_info.get("status", "UNKNOWN")
        job_state = self._map_nersc_state_to_psij(nersc_state)
        return JobStatus(job_state)

    def _map_nersc_state_to_psij(self, nersc_state: str) -> JobState:
        mapping = {
            "PENDING": JobState.QUEUED,
            "RUNNING": JobState.ACTIVE,
            "COMPLETED": JobState.COMPLETED,
            "CANCELLED": JobState.CANCELED,
            "FAILED": JobState.FAILED,
            "TIMEOUT": JobState.FAILED,
            "PREEMPTED": JobState.FAILED,
            "NODE_FAIL": JobState.FAILED,
            "SUSPENDED": JobState.QUEUED
        }
        return mapping.get(nersc_state.upper(), JobState.NEW)

    def status(self):
        """Hit the /status endpoint to get general information about NERSC facilities."""
        url = f"{self.url}/status"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        return response
