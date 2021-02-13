# business logic
# -----------------------------------------------------------------------------

import logging


from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.exceptions import InternalError

from sawtooth_job.processor.job_payload import JobPayload
from sawtooth_job.processor.job_state import Job
from sawtooth_job.processor.job_state import JobState
from sawtooth_job.processor.job_state import JOB_NAMESPACE


LOGGER = logging.getLogger(__name__)


class JobTransactionHandler(TransactionHandler):
    # Disable invalid-overridden-method. The sawtooth-sdk expects these to be
    # properties.
    # pylint: disable=invalid-overridden-method
    @property
    def family_name(self):
        return 'job'

    @property
    def family_versions(self):
        return ['1.0']

    @property
    def namespaces(self):
        return [JOB_NAMESPACE]

    # transaction holds the command that is to be executed
    # context stores info about current state
    def apply(self, transaction, context):

        header = transaction.header
        signer = header.signer_public_key

        job_payload = JobPayload.load_job(transaction.payload)

        job_state = JobState(context)

        # create a transaction 
        if job_payload.action == 'create':
            print('+++++++++++++++++creating job +++++++++++++++++++++++')
            job = Job(jobId=job_payload.jobId,
                        workerId=job_payload.workerId,
                        publisherId=job_payload.publisherId,
                        start_time=job_payload.start_time,
                        end_time=job_payload.end_time,
                        deadline=job_payload.deadline,
                        base_rewards=job_payload.base_rewards,
                        extra_rewards=job_payload.extra_rewards)
            print('+++++++++++++++++hanlder: job +++++++++++++++++++')
            print('job id: ' + job_payload.jobId)
            job_state.set_job(job_payload.jobId, job)
            _display("{} created a job: {}.".format(signer[:6], job_payload.jobId))

        # retrive job by id. 
        elif job_payload.action == 'get':
            job = job_state.get_job(job_payload.jobId)

            if job is None:
                raise InvalidTransaction(
                    'Invalid action: Take requires an existing job')

def _display(msg):
    n = msg.count("\n")

    if n > 0:
        msg = msg.split("\n")
        length = max(len(line) for line in msg)
    else:
        length = len(msg)
        msg = [msg]

    # pylint: disable=logging-not-lazy
    LOGGER.debug("+" + (length + 2) * "-" + "+")
    for line in msg:
        LOGGER.debug("+ " + line.center(length) + " +")
    LOGGER.debug("+" + (length + 2) * "-" + "+")