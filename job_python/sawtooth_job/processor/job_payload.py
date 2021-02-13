# 
# -----------------------------------------------------------------------------

from sawtooth_sdk.processor.exceptions import InvalidTransaction


class JobPayload:

    def __init__(self, payload):
        print('+++payload: ')
        print(payload)
        try:
            # load payload
            jobId, workerId, publisherId, start_time, end_time, deadline, base_rewards, extra_rewards, action = payload.decode().split(",")
        except ValueError:
            raise InvalidTransaction("Invalid payload serialization")
        if not jobId:
            raise InvalidTransaction('jobId is required')

        if not workerId:
            raise InvalidTransaction('workerId is required')

        if not publisherId:
            raise InvalidTransaction('publisherId is required')

        if not start_time:
            raise InvalidTransaction('start_time is required')

        if not end_time:
            raise InvalidTransaction('end_time is required')

        if not deadline:
            raise InvalidTransaction('deadline is required')

        if not base_rewards:
            raise InvalidTransaction('base_rewards is required')

        if not extra_rewards:
            raise InvalidTransaction('extra_rewards is required')

        if not action:
            raise InvalidTransaction('Action is required')

        if action not in ('create', 'ggetByIdet', 'getByWorker'):
            raise InvalidTransaction('Invalid action: {}'.format(action))

        self._jobId = jobId
        self._workerId = workerId
        self._publisherId = publisherId
        self._start_time = start_time
        self._end_time = end_time
        self._deadline = deadline
        self._base_rewards = base_rewards
        self._extra_rewards = extra_rewards
        self._action = action

    @staticmethod
    def load_job(payload):
        return JobPayload(payload=payload)

    @property
    def jobId(self):
        return self._jobId

    @property
    def workerId(self):
        return self._workerId

    @property
    def publisherId(self):
        return self._publisherId

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time
    
    @property
    def deadline(self):
        return self._deadline

    @property
    def base_rewards(self):
        return self._base_rewards

    @property
    def extra_rewards(self):
        return self._extra_rewards

    @property
    def action(self):
        return self._action

    
