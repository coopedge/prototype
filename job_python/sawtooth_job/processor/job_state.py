# state class turn job info into bytes and store in the validator's Radix-Merkle tree
# and turn bytes into job info
# 
# -- stored job info --
# - job id
# - publisher id 
# - worker id
# - working time, it is the time that worker spend on the job
# - deadline
# - base rewards
# - extra rewards
# -----------------------------------------------------------------------------

import hashlib

from sawtooth_sdk.processor.exceptions import InternalError


JOB_NAMESPACE = hashlib.sha512('job'.encode("utf-8")).hexdigest()[0:6]


def _make_job_address(jobId):
    return JOB_NAMESPACE + \
        hashlib.sha512(jobId.encode('utf-8')).hexdigest()[:64]


class Job:
    def __init__(self, jobId, workerId, publisherId, start_time, end_time, deadline, base_rewards, extra_rewards):
        self.jobId = jobId
        self.workerId = workerId
        self.publisherId = publisherId
        self.start_time = start_time
        self.end_time = end_time
        self.deadline = deadline
        self.base_rewards = base_rewards
        self.extra_rewards = extra_rewards


class JobState:

    TIMEOUT = 3

    def __init__(self, context):
        """Constructor.

        Args:
            context (sawtooth_sdk.processor.context.Context): Access to
                validator state from within the transaction processor.
        """
        self._context = context
        self._address_cache = {}

    def set_job(self, jobId, job):
        """Store the job in the validator state.

        Args:
            jobId (str): The id.
            job (Job): The information specifying the current job.
        """
        print('++++++++++++++set job++++++++++++++')
        jobs = self._load_jobs(jobId=jobId)
        # print('+++++++++++++++++++++++++++jobs before set: ')
        # print(jobs)
        jobs[jobId] = job
        print('+++++++++++++++++++++++++++jobs after set: ')
        print(jobs)
        self._store_job(jobId, jobs=jobs)

    def get_job(self, jobId):
        """Get the job associated with jobId.

        Args:
            jobId (str): The ids.

        Returns:
            (Job): All the information specifying a job.
        """

        return self._load_jobs(jobId=jobId).get(jobId)

    def _store_job(self, jobId, jobs):
        address = _make_job_address(jobId)
        # print('+++++++++++++++++++++++++++jobs address: ' + address)
        state_data = self._serialize(jobs)
        # print('state data')
        # print(state_data)
        self._address_cache[address] = state_data

        self._context.set_state(
            {address: state_data},
            timeout=self.TIMEOUT)

    def _load_jobs(self, jobId):
        address = _make_job_address(jobId)

        if address in self._address_cache:
            if self._address_cache[address]:
                serialized_jobs = self._address_cache[address]
                jobs = self._deserialize(serialized_jobs)
            else:
                jobs = {}
        else:
            state_entries = self._context.get_state(
                [address],
                timeout=self.TIMEOUT)
            if state_entries:

                self._address_cache[address] = state_entries[0].data

                jobs = self._deserialize(state_entries[0].data)

            else:
                self._address_cache[address] = None
                jobs = {}

        return jobs

    def _deserialize(self, data):
        """Take bytes stored in state and deserialize them into Python
        Game objects.

        Args:
            data (bytes): The UTF-8 encoded string stored in state.

        Returns:
            (dict): game name (str) keys, Game values.
        """

        jobs = {}
        try:
            for job in data.decode().split("|"):
                jobId, workerId, publisherId, start_time, end_time, deadline, base_rewards, extra_rewards = job.split(",")

                jobs[jobId] = Job(jobId, workerId, start_time, end_time, deadline, base_rewards, extra_rewards)
        except ValueError:
            raise InternalError("Failed to deserialize game data")

        return jobs

    def _serialize(self, jobs):
        """Takes a dict of game objects and serializes them into bytes.

        Args:
            games (dict): game name (str) keys, Game values.

        Returns:
            (bytes): The UTF-8 encoded string stored in state.
        """
        print('++++++++++++serialize job++++++++++++++')
        job_strs = []
        for jobId, job in jobs.items():
            job_str = ",".join(
                [jobId, job.workerId, job.publisherId, job.start_time, job.end_time, job.deadline, job.base_rewards, job.extra_rewards])
            job_strs.append(job_str)
            print('++++++++++++job_strs: ')
            print(job_strs)

        return "|".join(job_strs).encode()
