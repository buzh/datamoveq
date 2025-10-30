import subprocess, threading, time, sqlite3, queue, base64
import logging, logging.handlers
from flask import Flask, request, jsonify

BIND_IP = "127.0.0.1"
BIND_PORT = 15000
NUM_THREADS = 4
MAX_RETRIES = 3
RETRY_INTERVAL = 10
LOG_LEVEL = logging.DEBUG
DB = 'jobs.db'
job_q = queue.Queue()
app = Flask(__name__)

logger = logging.getLogger("datamoveq")
logger.setLevel(LOG_LEVEL)
syslog = logging.handlers.SysLogHandler(address='/dev/log')
formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
syslog.setFormatter(formatter)
logger.addHandler(syslog)

@app.route("/add_job", methods=["POST"])
def api_add_job():
    data = request.get_json()
    job_id = int(data["id"])
    src = base64.b64decode(data["src"]).decode()
    dst = base64.b64decode(data["dst"]).decode()
    add_job(job_id, src, dst)
    return jsonify({"status": "queued", "id": job_id})


def init_db():
    with sqlite3.connect(DB) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY,
            src TEXT, dst TEXT,
            status TEXT DEFAULT 'pending',
            retries INTEGER DEFAULT 0
        )""")

def db_add_job(job_id, src, dst):
    with sqlite3.connect(DB) as db:
        db.execute("INSERT OR REPLACE INTO jobs (id, src, dst, status) VALUES (?, ?, ?, 'pending')",
                   (job_id, src, dst))
        db.commit()

def db_del_job(job_id):
    with sqlite3.connect(DB) as db:
        db.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        db.commit()

def db_set_job_status(job_id, status):
    with sqlite3.connect(DB) as db:
        db.execute("UPDATE jobs SET status=? WHERE id=?", (status, job_id))
        db.commit()

def add_job(job_id, src, dst):
    db_add_job(job_id, src, dst)
    job_q.put((job_id, src, dst))
    logger.debug(f"add_job {job_id} {src} {dst}")

def load_jobs():
    with sqlite3.connect(DB) as db:
        for job_id, src, dst in db.execute("SELECT id, src, dst FROM jobs WHERE status='pending'"):
            logger.debug(f"load_jobs: {job_id}")
            job_q.put((job_id, src, dst))

def retry_failed():
    logger.debug(f"Retry worker started")
    while True:
        with sqlite3.connect(DB) as db:
            for job_id, src, dst, retries in db.execute(
                "SELECT id, src, dst, retries FROM jobs WHERE status='failed'"
            ):
                if retries < MAX_RETRIES:
                    logger.debug(f"RETRY {job_id} (attempt {retries+1})")
                    db.execute("UPDATE jobs SET status='retrying', retries=? WHERE id=?",
                               (retries+1, job_id))
                    job_q.put((job_id, src, dst))
                else:
                    logger.debug(f"Retry {job_id} abandoned after {MAX_RETRIES} attempts")
                    db.execute("UPDATE jobs SET status='abandoned' WHERE id=?", (job_id,))
            db.commit()
        time.sleep(RETRY_INTERVAL)

def worker(thread_id):
    logger.debug(f"Worker started: {thread_id}")
    while True:
        try:
            job_id, src, dst = job_q.get(timeout=3)
        except queue.Empty:
            logger.debug(f"{thread_id}: Idle")
            continue

        db_set_job_status(job_id, 'running') 

        logger.debug(f"{thread_id}: start job {job_id}")
        res = subprocess.run(["bash", "./dummy_rsync.sh", str(job_id), src, dst])

        if res.returncode == 0:
            db_del_job(job_id)
            logger.debug(f"{thread_id}: completed {job_id} successfully")
        else:
            db_set_job_status(job_id, 'failed')
            logger.debug(f"{thread_id}: FAILED {job_id}, return code: {res.returncode}")
            continue

        logger.debug(f"{thread_id}: task done {job_id}")
        job_q.task_done()

def start_workers():
    for _ in range(NUM_THREADS):
        t = threading.Thread(target=worker, args=(_,), daemon=True)
        t.start()
    threading.Thread(target=retry_failed, daemon=True).start()

if __name__ == "__main__":
    init_db()
    load_jobs()
    start_workers()
    app.run(host=BIND_IP, port=BIND_PORT)
