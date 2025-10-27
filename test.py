import subprocess, threading, time, sqlite3, queue, base64
from flask import Flask, request, jsonify

DB = 'jobs.db'
NUM_THREADS = 4
MAX_RETRIES = 3
RETRY_INTERVAL = 60
job_q = queue.Queue()
app = Flask(__name__)

@app.route("/add_job", methods=["POST"])
def api_add_job():
    data = request.get_json()
    job_id = int(data["id"])
    src = base64.b64decode(data["src"]).decode()
    dst = base64.b64decode(data["dst"]).decode()
    add_job(job_id, src, dst)
    job_q.put(job_id, src, dst)
    return jsonify({"status": "queued", "id": job_id})


def init_db():
    with sqlite3.connect(DB) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY,
            src TEXT, dst TEXT,
            status TEXT DEFAULT 'pending',
            retries INTEGER DEFAULT 0,
            queued INTEGER DEFAULT 0
        )""")

def add_job(job_id, src, dst):
    with sqlite3.connect(DB) as db:
        db.execute("INSERT OR REPLACE INTO jobs (id, src, dst, status) VALUES (?, ?, ?, 'pending')",
                   (job_id, src, dst))
        db.commit()
        print(f"add_job {job_id} {src} {dst}")

def load_jobs():
    with sqlite3.connect(DB) as db:
        for row in db.execute("SELECT id, src, dst FROM jobs WHERE status='pending'"):
            job_q.put(row)

def retry_failed():
    while True:
        with sqlite3.connect(DB) as db:
            for job_id, src, dst, retries, queued in db.execute(
                "SELECT id, src, dst, retries, queued FROM jobs WHERE status='failed' AND queued=0"
            ):
                if retries < MAX_RETRIES:
                    print(f"Retry {job_id} (attempt {retries+1})")
                    db.execute("UPDATE jobs SET status='retrying', retries=?, queued=1 WHERE id=?",
                               (retries+1, job_id))
                    job_q.put((job_id, src, dst))
                else:
                    print(f"Retry {job_id} abandoned after {MAX_RETRIES} attempts")
                    db.execute("UPDATE jobs SET status='abandoned' WHERE id=?", (job_id,))
            db.commit()
        time.sleep(RETRY_INTERVAL)

def worker(thread_id):
    while True:
        try:
            job_id, src, dst = job_q.get(timeout=3)
        except queue.Empty:
            print(f"{thread_id}: Idle")
            continue
        
        with sqlite3.connect(DB) as db:
            db.execute("UPDATE jobs SET status='running', queued=1 WHERE id=?", (job_id,))
            db.commit()

        print(f"{thread_id}: start job {job_id}")
        res = subprocess.run(["bash", "./dummy_rsync.sh", str(job_id), src, dst])

        with sqlite3.connect(DB) as db:
            if res.returncode == 0:
                db.execute("DELETE FROM jobs WHERE id=?", (job_id,))
                print(f"{thread_id}: completed {job_id} successfully")
            else:
                db.execute("UPDATE jobs SET status='failed' WHERE id=?", (job_id,))
                print(f"{thread_id}: FAILED {job_id}, return code: {res.returncode}")
            db.commit()
        print(f"{thread_id}: task done {job_id}")
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
    app.run(host="127.0.0.1", port=15000)
