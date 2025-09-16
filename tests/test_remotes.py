import os
import unittest
from pathlib import Path

from psij import Job, JobSpec, JobExecutor, JobState
from psij.executors.nersc import NERSCExecutorConfig
from psij.executors.slurmrestd import SlurmRestAPIExecutorConfig


@unittest.skipIf(not os.environ.get('NERSC_TOKEN'), 'NERSC_TOKEN not set')
class NerscExecutorTests(unittest.TestCase):
    def test_simple_job(self):
        spec = JobSpec(executable='/bin/hostname')
        job = Job(spec)

        executor = JobExecutor.get_instance(
            'nersc',
            config=NERSCExecutorConfig(token=os.environ['NERSC_TOKEN'])
        )
        executor.submit(job)
        job.wait()

        self.assertEqual(job.status.state, JobState.COMPLETED)


@unittest.skipIf(not os.environ.get('SLURM_TOKEN'), 'SLURM_TOKEN not set')
class SlurmRestExecutorTests(unittest.TestCase):
    def test_simple_job(self):
        spec = JobSpec(executable='/bin/hostname')
        job = Job(spec)

        executor = JobExecutor.get_instance(
            'slurm-rest',
            url='http://localhost:8080',
            config=SlurmRestAPIExecutorConfig(token=os.environ['SLURM_TOKEN'])
        )
        executor.submit(job)
        job.wait()

        self.assertEqual(job.status.state, JobState.COMPLETED)
