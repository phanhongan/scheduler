import logging
import arrow

from datetime import datetime
from dateutil import tz

logger = logging.getLogger('scheduler')


class Scheduler(object):
    def __init__(self):
        self.jobs = []

    def get_pending_list(self):
        return [job for job in self.jobs if job.should_run]

    def run_pending(self):
        runnable_jobs = self.get_pending_list()
        for job in sorted(runnable_jobs):
            try:
                logger.info('Running job for scheduler %s', job.id)
                job.run()
            except:
                logger.exception('Unexpected exception while running scheduler %s' % job.id)

    def add_job(self, job):
        assert isinstance(job, ScheduledJob)

        self.jobs.append(job)

    @property
    def next_run(self):
        if not self.jobs:
            return None
        return min(self.jobs).next_run


class ScheduledJob(object):
    def __init__(self, schedule_id, start_time=datetime.now(), timezone='UTC', interval=1, unit='days', end_time=None,
                 last_run=None, job_func=None):
        self.id = schedule_id
        self.start_time = start_time
        self.timezone = timezone
        self.interval = interval  # pause interval * unit between runs
        self.unit = unit  # time units, e.g. 'minutes', 'hours', ...
        self.end_time = end_time

        self.last_run = last_run  # datetime of the last run
        self.next_run = None  # datetime of the next run

        self.job_func = job_func  # the job function to run
        self._setup()

    def __lt__(self, other):
        return self.next_run < other.next_run

    @property
    def tz(self):
        return tz.gettz(self.timezone)

    @property
    def should_run(self):
        return self.next_run and self._arrow(self.next_run) <= arrow.now(self.tz)

    @property
    def has_next_run(self):
        return self.next_run is not None

    def run(self):
        self.last_run = self.next_run
        self._schedule_next_run()
        ret = self.job_func()
        return ret

    def _setup(self):
        self._schedule_next_run()

    def _schedule_next_run(self):
        now = arrow.now(self.tz)

        next_run = self._arrow(self.start_time) if not self.last_run \
            else self._arrow(self.last_run).shift(**{self.unit: self.interval})

        # Let next_run be always greater than now
        if next_run < now:
            if self.unit == 'months':
                next_run = next_run.replace(year=now.year, month=now.month)
            elif self.unit == 'weeks':
                next_run = next_run.replace(year=now.year)
                if next_run < now:
                    next_run = next_run.shift(weeks=now.week - next_run.week)
            elif self.unit == 'days':
                next_run = next_run.replace(year=now.year, month=now.month, day=now.day)
            elif self.unit == 'hours':
                next_run = next_run.replace(year=now.year, month=now.month, day=now.day, hour=now.hour)
            elif self.unit == 'minutes':
                next_run = next_run.replace(year=now.year, month=now.month, day=now.day,
                                            hour=now.hour, minute=now.minute)
            elif self.unit == 'seconds':
                next_run = next_run.replace(year=now.year, month=now.month, day=now.day,
                                            hour=now.hour, minute=now.minute, second=now.second)

        if next_run < now:
            next_run = next_run.shift(**{self.unit: 1})

        self.next_run = None if self.end_time and next_run > self._arrow(self.end_time) else next_run.datetime

    def at_time(self, hour, minute=0):
        assert self.unit in ('days', 'weeks', 'months')

        self.start_time = self._arrow(self.start_time)\
            .replace(hour=hour, minute=minute, second=0, microsecond=0).datetime
        self._setup()

    def at_day_of_month(self, day):
        assert self.unit == 'months'

        self.start_time = self._arrow(self.start_time).replace(day=day).datetime
        self._setup()

    def at_weekday(self, weekday):
        assert self.unit == 'weeks'

        self.start_time = self._arrow(self.start_time).shift(weekday=weekday).datetime
        self._setup()

    def _arrow(self, dt):
        return arrow.get(dt, self.tz)
