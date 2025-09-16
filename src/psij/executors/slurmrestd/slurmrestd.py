from typing import Optional, List
from psij import Job, JobStatus, JobState, JobSpec, ResourceSpec
from psij.executors.remote_job_executor import RemoteJobExecutor, RemoteJobExecutorConfig
from slurmrestd_client.exceptions import ApiException
from slurmrestd_client.api_client import ApiClient
from slurmrestd_client.api.slurm_api import SlurmApi
from slurmrestd_client.configuration import Configuration
from slurmrestd_client.models.slurm_v0041_post_job_submit_request import SlurmV0041PostJobSubmitRequest
from slurmrestd_client.models.slurm_v0041_post_job_submit_request_job import SlurmV0041PostJobSubmitRequestJob
from slurmrestd_client.models.slurm_v0041_post_job_submit_request_jobs_inner_time_limit import SlurmV0041PostJobSubmitRequestJobsInnerTimeLimit
import logging

logger = logging.getLogger(__name__)

class SlurmRestAPIExecutorConfig(RemoteJobExecutorConfig):
    """Configuration for the Slurm REST API executor."""
    def __init__(self, token: str = '', verify_ssl: bool = True, user_name: str = 'root'):
        super().__init__(token=token, verify_ssl=verify_ssl)
        self.user_name = user_name

class SlurmRestAPIExecutor(RemoteJobExecutor):
    _NAME_ = "Slurm REST API Executor"
    _VERSION_ = "0.0.1"
    _DESCRIPTION_ = "Executor for the Slurm REST API"

    def __init__(self, url: Optional[str] = None,
                 config: Optional[SlurmRestAPIExecutorConfig] = None):
        super().__init__(url=url, config=config or SlurmRestAPIExecutorConfig())
        configuration = Configuration(host=self.url)
        configuration.verify_ssl = self.config.verify_ssl
        configuration.api_key['token'] = self.config.token
        self.api_client = ApiClient(configuration)
        self.slurm_api = SlurmApi(self.api_client)

    def _get_headers(self):
        return {
            'X-SLURM-USER-TOKEN': self.config.token,
            'X-SLURM-USER-NAME': self.config.user_name
        }

    def _submit_job(self, job: Job) -> str:
        logger.info(f"Submitting job: {job}")
        job_spec: JobSpec = job.spec if job.spec else JobSpec()
        total_minutes = int(job_spec.attributes.duration.total_seconds() // 60)
        node_count = 1
        if job_spec.resources and isinstance(job_spec.resources, ResourceSpec):
            node_count = job_spec.resources.node_count

        job_request = SlurmV0041PostJobSubmitRequest(
            job=SlurmV0041PostJobSubmitRequestJob(
                nodes=str(node_count),
                time_limit=SlurmV0041PostJobSubmitRequestJobsInnerTimeLimit(set=True, number=total_minutes),
                current_working_directory=str(job_spec.directory) if job_spec.directory else None,
                environment=[f'{k}={v}' for k, v in job_spec.environment.items()] if job_spec.environment else None,
                name=job_spec.name,
                script=job_spec.executable
            )
        )
        try:
            response = self.slurm_api.slurm_v0041_post_job_submit(
                slurm_v0041_post_job_submit_request=job_request, _headers=self._get_headers()
            )
            logger.info(f"Job submitted: {response.job_id}")
            return str(response.job_id)
        except ApiException as e:
            logger.error(f"Failed to submit job: {e}")
            raise

    def _cancel_job(self, native_id: str) -> None:
        logger.info(f"Cancelling job {native_id}")
        try:
            self.slurm_api.slurm_v0040_delete_job(native_id, _headers=self._get_headers())
        except ApiException as e:
            logger.error(f"Failed to cancel job {native_id}: {e}")
            raise

    def _list_jobs(self) -> List[str]:
        logger.info("Listing all jobs")
        try:
            response = self.slurm_api.slurm_v0040_get_jobs(_headers=self._get_headers())
            return [str(job_info.job_id) for job_info in response.jobs]
        except ApiException as e:
            logger.error(f"Failed to list jobs: {e}")
            raise

    def _get_job_status(self, native_id: str) -> JobStatus:
        logger.info(f"Getting status for job {native_id}")
        try:
            response = self.slurm_api.slurm_v0040_get_job(native_id, _headers=self._get_headers())
            if response and hasattr(response, 'jobs') and len(response.jobs) > 0:
                job_info = response.jobs[0]
                state = job_info.job_state or ''
                primary_slurm_state = state[0] if isinstance(state, list) and state else state if isinstance(state, str) else ''
                job_state = self._map_slurm_state_to_psij(primary_slurm_state)
                return JobStatus(job_state)
            else:
                raise Exception(f"Job with native_id {native_id} not found.")
        except ApiException as e:
            logger.error(f"Failed to get job status for job {native_id}: {e}")
            raise

    def _map_slurm_state_to_psij(self, slurm_state: str) -> JobState:
        mapping = {
            'PENDING': JobState.QUEUED,
            'CONFIGURING': JobState.NEW,
            'RUNNING': JobState.ACTIVE,
            'COMPLETED': JobState.COMPLETED,
            'CANCELLED': JobState.CANCELED,
            'FAILED': JobState.FAILED,
            'TIMEOUT': JobState.FAILED,
            'PREEMPTED': JobState.FAILED,
            'NODE_FAIL': JobState.FAILED,
            'SUSPENDED': JobState.CANCELED
        }
        ret = mapping.get(slurm_state.upper())
        if not ret:
            raise Exception(f"Invalid job status code: {slurm_state}")
        return ret
