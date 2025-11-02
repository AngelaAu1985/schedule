# Example from APScheduler CronTrigger Examples (Response 3)
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from datetime import datetime

def job(name="Job"):
    print(f"{name} running at {datetime.now(pytz.timezone('US/Pacific'))}")

scheduler = BlockingScheduler(timezone="US/Pacific")
scheduler.add_job(
    job,
    CronTrigger(hour=1, minute=30, timezone="US/Pacific"),
    args=["Daily 1:30 AM"],
)
scheduler.start()

scheduler = BlockingScheduler(timezone="US/Pacific")
scheduler.add_job(
    job,
    CronTrigger(hour=2, minute=30, timezone="US/Pacific"),
    args=["Daily 2:30 AM"],
)
scheduler.start()

scheduler = BlockingScheduler(timezone="US/Pacific")
scheduler.add_job(
    job,
    CronTrigger(day_of_week="sun", hour=1, minute=30, timezone="US/Pacific"),
    args=["Sunday 1:30 AM"],
)
scheduler.start()

scheduler = BlockingScheduler(timezone="US/Pacific")
scheduler.add_job(
    job,
    CronTrigger(day_of_week="tue", hour=10, minute=0, week="*/2", timezone="US/Pacific"),
    args=["Every Other Tuesday 10:00 AM"],
)
scheduler.start()

scheduler = BlockingScheduler(timezone="US/Pacific")
scheduler.add_job(
    job,
    CronTrigger(hour=10, minute=30, timezone="US/Pacific"),
    args=["Daily 10:30 AM"],
    misfire_grace_time=3600,  # 1-hour grace period
)
scheduler.start()

# Example from Celery Beat Scheduling (Response 4)
from celery import Celery
from celery.schedules import crontab
from datetime import datetime
import pytz

app = Celery('tasks', broker='redis://localhost:6379/0')

@app.task
def sample_task(message='Hello'):
    print(f"{message} at {datetime.now(pytz.timezone('US/Pacific'))}")

app.conf.beat_schedule = {
    'daily-sample': {
        'task': 'tasks.sample_task',
        'schedule': crontab(hour=10, minute=30),
        'args': ('Daily task',),
        'kwargs': {'timezone': 'US/Pacific'},
    },
}

app.conf.timezone = 'US/Pacific'

@app.task(bind=True, max_retries=3)
def dst_task(self, tz_info='PST'):
    tz = pytz.timezone('US/Pacific')
    now = datetime.now(tz)
    if now.dst():
        tz_info = 'PDT'
    print(f"DST Task running at {now} ({tz_info})")

app.conf.beat_schedule = {
    'dst-fold-example': {
        'task': 'dst_example.dst_task',
        'schedule': crontab(hour=1, minute=30),
    },
}

# Example from RQ Scheduler (Response 5)
from datetime import datetime
from rq import Queue
from rq.job import Job
from rq_scheduler import Scheduler
import pytz

connection = None  # Assumes Redis is running locally
scheduler = Scheduler(connection, interval=10)

q = Queue(connection=connection)

def sample_job(message='Hello'):
    print(f"{message} at {datetime.now(pytz.timezone('US/Pacific'))}")

scheduler.cron(
    sample_job,
    minute='30',
    hour='10',
    queue=q,
    timeout=300,
    tz=pytz.timezone('US/Pacific'),
)

scheduler.cron(
    sample_job,
    hour=1,
    minute=30,
    tz=pytz.timezone('US/Pacific'),
)

# Example from RQ Dashboard Integration (Response 6)
from flask import Flask
import rq
from rq_dashboard import RQDashboard
from rq_scheduler import Scheduler
from datetime import datetime
import pytz

app = Flask(__name__)
app.config.from_object(rq_dashboard.default_config)
app.config['RQ_DASHBOARD_REDIS_URL'] = 'redis://localhost:6379/0'

connection = None
scheduler = Scheduler(connection, interval=10)

q = Queue(connection=connection)

def sample_job(message='Hello'):
    print(f"{message} at {datetime.now(pytz.timezone('US/Pacific'))}")

scheduler.cron(sample_job, hour=10, minute=30, tz=pytz.timezone('US/Pacific'))

RQDashboard(app)

if __name__ == '__main__':
    app.run(debug=True, port=5000)

from rq_dashboard import login
from flask_login import UserMixin

class User(UserMixin):
    def __init__(self, id):
        self.id = id

@login.user_loader
def load_user(id):
    return User(id)

app.config['RQ_DASHBOARD_USERNAME'] = 'admin'
app.config['RQ_DASHBOARD_PASSWORD'] = 'password'

scheduler.cron(
    sample_job,
    hour=1,
    minute=30,
    tz=pytz.timezone('US/Pacific'),
    job_id='dst_fold_job',
)

@app.route('/dst_status')
def dst_status():
    tz = pytz.timezone('US/Pacific')
    now = datetime.now(tz)
    return f"Current Pacific time: {now} (DST: {now.dst()})"

if __name__ == '__main__':
    app.run(debug=True)

# Example from Troubleshooting RQ Dashboard Errors (Response 7)
def safe_job():
    try:
        pass
    except ValueError as e:
        print(f"Timezone error: {e}")
        raise

# Example from Advanced DST Fold Handling (Response 8)
from redis.lock import Lock

def dst_aware_job(message='DST Job'):
    tz = pytz.timezone('US/Pacific')
    now = datetime.now(tz)
    tz_name = 'PDT' if now.dst() else 'PST'
    print(f"{message} at {now} ({tz_name})")
    return tz_name

scheduler.cron(
    dst_aware_job,
    hour=1,
    minute=30,
    tz=pytz.timezone('US/Pacific'),
    job_id='dst_fold_job',
    repeat=2,
)

def dst_safe_job(message='DST Safe'):
    tz = pytz.timezone('US/Pacific')
    lock_key = f"lock:dst_job:{datetime.now(tz).strftime('%Y%m%d%H%M%S')}"
    lock = Lock(connection, lock_key, timeout=300)
    if lock.acquire(blocking_timeout=5):
        try:
            now = datetime.now(tz)
            tz_name = 'PDT' if now.dst() else 'PST'
            print(f"{message} at {now} ({tz_name})")
        finally:
            lock.release()

scheduler.cron(dst_safe_job, hour=1, minute=30, tz=pytz.timezone('US/Pacific'))

def adaptive_job():
    tz = pytz.timezone('US/Pacific')
    now = datetime.now(tz)
    if now.dst():
        print("Skipping PDT run; rescheduling for PST")
        scheduler.cancel('dst_fold_job')
        scheduler.cron(adaptive_job, hour=1, minute=30, tz=tz, job_id='dst_fold_job')
    else:
        print(f"Running in PST at {now}")

app = Flask(__name__)
app.config.from_object(rq_dashboard.default_config)
app.config['RQ_DASHBOARD_REDIS_URL'] = 'redis://localhost:6379/0'
RQDashboard(app)

connection = None
scheduler = Scheduler(connection, interval=10)
q = Queue(connection=connection)

def dst_safe_job(message='DST Safe'):
    tz = pytz.timezone('US/Pacific')
    lock_key = f"lock:dst_job:{datetime.now(tz).strftime('%Y%m%d%H%M%S')}"
    lock = Lock(connection, lock_key, timeout=300)
    if lock.acquire(blocking_timeout=5):
        try:
            now = datetime.now(tz)
            tz_name = 'PDT' if now.dst() else 'PST'
            print(f"{message} at {now} ({tz_name})")
        finally:
            lock.release()

scheduler.cron(
    dst_safe_job,
    hour=1,
    minute=30,
    tz=pytz.timezone('US/Pacific'),
    job_id='dst_fold_job',
)

if __name__ == '__main__':
    app.run(debug=True)

# Example from Timezone Normalization Techniques (Response 9)
def dst_explicit_job():
    tz = pytz.timezone('US/Pacific')
    run_time = tz.localize(datetime(2025, 11, 2, 1, 30), is_dst=False)
    print(f"Running at {run_time}")

scheduler.cron(
    dst_explicit_job,
    hour=1,
    minute=30,
    tz=pytz.timezone('US/Pacific'),
    job_id='dst_explicit_job',
    queue=q,
)

def dynamic_dst_job():
    tz = pytz.timezone('US/Pacific')
    now = datetime.now(tz)
    if now.dst():
        print(f"Skipping PDT run at {now}")
        return 'Skipped'
    print(f"Running in PST at {now}")

scheduler.cron(
    dynamic_dst_job,
    hour=1,
    minute=30,
    tz=pytz.timezone('US/Pacific'),
    job_id='dst_dynamic_job',
)

def locked_dst_job():
    tz = pytz.timezone('US/Pacific')
    lock_key = f"lock:dst_job:{datetime.now(tz).strftime('%Y%m%d%H%M')}"
    lock = Lock(connection, lock_key, timeout=300)
    if lock.acquire(blocking_timeout=5):
        try:
            now = datetime.now(tz)
            print(f"Running at {now} ({'PDT' if now.dst() else 'PST'})")
        finally:
            lock.release()

scheduler.cron(
    locked_dst_job,
    hour=1,
    minute=30,
    tz=pytz.timezone('US/Pacific'),
    job_id='dst_locked_job',
)

def gap_safe_job():
    tz = pytz.timezone('US/Pacific')
    scheduled_time = datetime(2025, 3, 9, 2, 30)
    normalized_time = tz.normalize(tz.localize(scheduled_time))
    print(f"Running at {normalized_time} ({'PDT' if normalized_time.dst() else 'PST'})")

scheduler.cron(
    gap_safe_job,
    hour=2,
    minute=30,
    tz=pytz.timezone('US/Pacific'),
    job_id='dst_gap_job',
)

app = Flask(__name__)
app.config.from_object(rq_dashboard.default_config)
app.config['RQ_DASHBOARD_REDIS_URL'] = 'redis://localhost:6379/0'
RQDashboard(app)

connection = None
scheduler = Scheduler(connection, interval=5)
q = Queue(connection=connection)

logging.getLogger('rq').setLevel(logging.DEBUG)

def dst_normalized_job(message='DST Normalized'):
    tz = pytz.timezone('US/Pacific')
    lock_key = f"lock:dst_job:{datetime.now(tz).strftime('%Y%m%d%H%M')}"
    lock = Lock(connection, lock_key, timeout=300)
    if lock.acquire(blocking_timeout=5):
        try:
            now = tz.normalize(tz.localize(datetime.now()))
            tz_name = 'PDT' if now.dst() else 'PST'
            print(f"{message} at {now} ({tz_name})")
            connection.set(f"job:log:{now}", tz_name)
        finally:
            lock.release()

scheduler.cron(
    dst_normalized_job,
    hour=1,
    minute=30,
    tz=pytz.timezone('US/Pacific'),
    job_id='dst_normalized_job',
    queue=q,
)

if __name__ == '__main__':
    app.run(debug=True)

# Example from UTC Scheduling Benefits (Response 10)
app = Flask(__name__)
app.config.from_object(rq_dashboard.default_config)
app.config['RQ_DASHBOARD_REDIS_URL'] = 'redis://localhost:6379/0'
RQDashboard(app)

connection = None
scheduler = Scheduler(connection, interval=5)
q = Queue(connection=connection)
logging.getLogger('rq').setLevel(logging.DEBUG)

def utc_scheduled_job():
    utc_now = datetime.utcnow().replace(tzinfo=pytz.UTC)
    local_tz = pytz.timezone('US/Pacific')
    local_time = utc_now.astimezone(local_tz)
    print(f"UTC: {utc_now}, Pacific: {local_time} ({'PDT' if local_time.dst() else 'PST'})")
    connection.set(f"job:log:{utc_now}", str(local_time))

scheduler.cron(
    utc_scheduled_job,
    hour=9,
    minute=30,
    tz=pytz.UTC,
    job_id='utc_scheduled_job',
    queue=q,
)

@app.route('/job_logs')
def job_logs():
    keys = connection.keys('job:log:*')
    return {'logs': [connection.get(key).decode() for key in keys]}

if __name__ == '__main__':
    app.run(debug=True)

def utc_job():
    print(f"Running at {datetime.utcnow()} UTC")

scheduler.cron(
    utc_job,
    hour=9,
    minute=30,
    tz=pytz.UTC,
    job_id='pure_utc_job',
)

def locked_utc_job():
    lock = Lock(connection, f"lock:utc_job:{datetime.utcnow().strftime('%Y%m%d%H%M')}")
    if lock.acquire(blocking_timeout=5):
        try:
            print(f"Running at {datetime.utcnow()} UTC")
        finally:
            lock.release()

scheduler.cron(
    locked_utc_job,
    hour=9,
    minute=30,
    tz=pytz.UTC,
    job_id='locked_utc_job',
)

def hybrid_job():
    utc_now = datetime.utcnow().replace(tzinfo=pytz.UTC)
    local_now = utc_now.astimezone(pytz.timezone('US/Pacific'))
    print(f"UTC: {utc_now}, Pacific: {local_now}")
    connection.set(f"job:log:{utc_now}", str(local_now))

scheduler.cron(
    hybrid_job,
    hour=9,
    minute=30,
    tz=pytz.UTC,
)

def dst_fallback_job():
    utc_now = datetime.utcnow().replace(tzinfo=pytz.UTC)
    local_tz = pytz.timezone('US/Pacific')
    try:
        local_time = utc_now.astimezone(local_tz)
        print(f"Running at {local_time} ({'PDT' if local_time.dst() else 'PST'})")
    except pytz.exceptions.AmbiguousTimeError:
        local_time = local_tz.normalize(local_tz.localize(utc_now, is_dst=False))
        print(f"Fallback to {local_time} PST")

# Example from Global Timezone Strategies (Response 11)
app = Flask(__name__)
app.config.from_object(rq_dashboard.default_config)
app.config['RQ_DASHBOARD_REDIS_URL'] = 'redis://localhost:6379/0'
RQDashboard(app)

connection = None
scheduler = Scheduler(connection, interval=5)
q = Queue(connection=connection)
logging.getLogger('rq').setLevel(logging.DEBUG)

def global_multi_tz_job():
    utc_now = datetime.utcnow().replace(tzinfo=pytz.UTC)
    timezones = ['US/Pacific', 'Europe/London', 'Asia/Dubai']
    logs = {}
    for tz_name in timezones:
        tz = pytz.timezone(tz_name)
        try:
            local_time = utc_now.astimezone(tz)
            logs[tz_name] = f"{local_time} ({'DST' if local_time.dst() else 'non-DST'})"
        except pytz.exceptions.AmbiguousTimeError:
            local_time = tz.normalize(tz.localize(utc_now, is_dst=False))
            logs[tz_name] = f"{local_time} (non-DST fallback)"
    connection.set(f"job:log:{utc_now}", str(logs))
    print(f"UTC: {utc_now}, Logs: {logs}")

scheduler.cron(
    global_multi_tz_job,
    hour=9,
    minute=30,
    tz=pytz.UTC,
    job_id='global_multi_tz_job',
    queue=q,
)

@app.route('/global_logs/<tz_name>')
def global_logs(tz_name):
    try:
        tz = pytz.timezone(tz_name)
    except pytz.exceptions.UnknownTimeZoneError:
        return {"error": "Invalid timezone"}, 400
    keys = connection.keys('job:log:*')
    logs = [{key.decode(): eval(connection.get(key).decode())[tz_name] for key in keys}]
    return {'logs': logs}

if __name__ == '__main__':
    app.run(debug=True)

def multi_region_job(region):
    local_tz = pytz.timezone(region)
    local_time = datetime.now(local_tz)
    print(f"Running in {region} at {local_time}")

scheduler.cron(
    multi_region_job,
    hour=1,
    minute=30,
    tz=pytz.timezone('US/Pacific'),
    job_id='us_pacific_job',
    args=['US/Pacific'],
)
scheduler.cron(
    multi_region_job,
    hour=1,
    minute=30,
    tz=pytz.timezone('Europe/London'),
    job_id='europe_london_job',
    args=['Europe/London'],
)

def dynamic_tz_job(user_tz='US/Pacific'):
    tz = pytz.timezone(user_tz)
    utc_now = datetime.utcnow().replace(tzinfo=pytz.UTC)
    local_time = utc_now.astimezone(tz)
    try:
        normalized_time = tz.normalize(tz.localize(local_time))
        print(f"Running in {user_tz} at {normalized_time}")
    except pytz.exceptions.AmbiguousTimeError:
        normalized_time = tz.normalize(tz.localize(local_time, is_dst=False))
        print(f"Fallback to {normalized_time} (non-DST)")

scheduler.cron(
    dynamic_tz_job,
    hour=9,
    minute=30,
    tz=pytz.UTC,
    job_id='dynamic_tz_job',
    args=['US/Pacific'],
)

def locked_global_job():
    lock = Lock(connection, f"lock:global_job:{datetime.utcnow().strftime('%Y%m%d%H%M')}")
    if lock.acquire(blocking_timeout=5):
        try:
            utc_now = datetime.utcnow().replace(tzinfo=pytz.UTC)
            print(f"Running at {utc_now} UTC")
        finally:
            lock.release()

scheduler.cron(
    locked_global_job,
    hour=9,
    minute=30,
    tz=pytz.UTC,
    job_id='locked_global_job',
)

@app.route('/tz_logs/<tz_name>')
def tz_logs(tz_name):
    try:
        tz = pytz.timezone(tz_name)
    except pytz.exceptions.UnknownTimeZoneError:
        return {"error": "Invalid timezone"}, 400
    keys = connection.keys('job:log:*')
    logs = [{key.decode(): connection.get(key).decode().astimezone(tz).isoformat()} for key in keys]
    return {'logs': logs} 
