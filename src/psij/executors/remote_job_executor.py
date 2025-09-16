from abc import ABC, abstractmethod
from typing import Optional, List

from psij import Job, JobExecutor, JobExecutorConfig, JobState, JobStatus
from psij.job_executor import logger


class RemoteJobExecutorConfig(JobExecutorConfig):
    """Base configuration for remote executors."""
    def __init__(self, token: str = '', verify_ssl: bool = True):
        super().__init__()
        self.token = token
        self.verify_ssl = verify_ssl


class RemoteJobExecutor(JobExecutor, ABC):
    """An abstract base class for executors that interact with remote REST APIs."""

    def __init__(self, url: Optional[str] = None, config: Optional[RemoteJobExecutorConfig] = None):
        super().__init__(url=url, config=config)

    @abstractmethod
    def _submit_job(self, job: Job) -> str:
        """
        Submits the job to the remote scheduler and returns the native job ID.
        This method will be implemented by the concrete remote executor.
        """
        pass

    def submit(self, job: Job) -> None:
        """Submits a Job to the underlying implementation."""
        self._check_job(job)
        try:
            native_id = self._submit_job(job)
            job._native_id = native_id
            self._set_job_status(job, JobStatus(JobState.QUEUED))
        except Exception as e:
            logger.error(f"Failed to submit job: {e}")
            raise

    @abstractmethod
    def _cancel_job(self, native_id: str) -> None:
        """
        Cancels the job on the remote scheduler.
        """
        pass

    def cancel(self, job: Job) -> None:
        """Cancels a job."""
        if not job.native_id:
            raise Exception("Cannot cancel a job without a native ID.")
        try:
            self._cancel_job(job.native_id)
        except Exception as e:
            logger.error(f"Failed to cancel job {job.native_id}: {e}")
            raise

    @abstractmethod
    def _list_jobs(self) -> List[str]:
        """
        Lists the native IDs of all jobs on the remote scheduler.
        """
        pass

    def list(self) -> List[str]:
        """Lists all jobs known to the backend."""
        try:
            return self._list_jobs()
        except Exception as e:
            logger.error(f"Failed to list jobs: {e}")
            raise

    @abstractmethod
    def _get_job_status(self, native_id: str) -> JobStatus:
        """
        Gets the status of a single job from the remote scheduler.
        """
        pass

    def attach(self, job: Job, native_id: str) -> None:
        """Attaches a job to a native job."""
        if job.status.state != JobState.NEW:
            raise Exception('Job must be in NEW state to be attached.')
        job._native_id = native_id
        try:
            status = self._get_job_status(native_id)
            self._set_job_status(job, status)
        except Exception as e:
            logger.error(f"Failed to attach to job {native_id}: {e}")
            raise

    def _update_job_status(self, job: Job) -> None:
        """Updates the status of a single job."""
        if not job.native_id:
            return
        try:
            status = self._get_job_status(job.native_id)
            self._set_job_status(job, status)
        except Exception as e:
            logger.warning(f"Failed to update status for job {job.native_id}: {e}")
