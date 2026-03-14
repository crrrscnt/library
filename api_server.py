import time, os, random, sqlite3
import psutil
from flask import Flask, request, jsonify

app   = Flask(__name__)
PORT  = int(os.environ.get("PORT", 5001))
DB_PATH = "/tmp/library.db"

GENRES   = ["Fantasy","Sci-Fi","History","Romance","Horror","Thriller","Biography"]
STATUSES = ["read","unread"]
FIRST    = ["Александр","Мария","Иван","Анна","Дмитрий","Елена","Сергей","Наталья"]
LAST     = ["Иванов","Петров","Сидоров","Козлов","Смирнов","Попов","Лебедев","Новиков"]
WORDS    = ["великий","тайный","последний","первый","синий","чёрный","золотой",
            "путь","мир","время","свет","тень","огонь","вода","земля","туман"]

_proc = psutil.Process(os.getpid())


def db():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c


def ensure_table():
    with db() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS books (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            title    TEXT NOT NULL,
            author   TEXT NOT NULL,
            year     INTEGER,
            genre    TEXT,
            status   TEXT DEFAULT 'unread',
            added_by INTEGER DEFAULT 1)""")
        c.commit()


# Вызываем ДО первого запроса — работает и с gunicorn, и локально
with app.app_context():
    ensure_table()


def fake_title():
    return " ".join(random.choices(WORDS, k=random.randint(2, 4))).capitalize()


def fake_author():
    return f"{random.choice(FIRST)} {random.choice(LAST)}"


def srv():
    return {
        "server_cpu_pct": round(_proc.cpu_percent(interval=None), 2),
        "server_ram_mb":  round(_proc.memory_info().rss / 1024 / 1024, 2),
    }


@app.route("/")
def index():
    with db() as c:
        cnt = c.execute("SELECT COUNT(*) FROM books").fetchone()[0]
    return jsonify({
        "service":    "КИС Домашняя библиотека",
        "status":     "ok",
        "book_count": cnt,
        "endpoints": [
            "GET  /health",
            "GET  /search?q=...",
            "GET  /search_opt?q=...",
            "GET  /genre_stats",
            "GET  /genre_stats_opt",
            "POST /add_book",
            "GET  /books/count",
            "POST /generate?n=1000",
            "DELETE /books/clear",
            "POST /create_index",
            "POST /drop_index",
        ]
    })


@app.route("/health")
def health():
    with db() as c:
        cnt = c.execute("SELECT COUNT(*) FROM books").fetchone()[0]
    return jsonify({"status":"ok","book_count":cnt,"ts":time.time(),**srv()})


@app.route("/search")
def search():
    q = request.args.get("q", "а")
    t0 = time.perf_counter()
    with db() as c:
        rows = c.execute(
            "SELECT id,title,author FROM books "
            "WHERE title LIKE ? OR author LIKE ? LIMIT 50",
            (f"%{q}%", f"%{q}%")).fetchall()
    return jsonify({"count":len(rows),
                    "db_ms":round((time.perf_counter()-t0)*1000,3),**srv()})


@app.route("/search_opt")
def search_opt():
    q = request.args.get("q", "а")
    t0 = time.perf_counter()
    with db() as c:
        rows = c.execute(
            "SELECT id,title,author FROM books "
            "WHERE title LIKE ? OR author LIKE ? LIMIT 50",
            (f"%{q}%", f"%{q}%")).fetchall()
    return jsonify({"count":len(rows),
                    "db_ms":round((time.perf_counter()-t0)*1000,3),**srv()})


@app.route("/genre_stats")
def genre_stats():
    t0 = time.perf_counter()
    with db() as c:
        genres = c.execute(
            "SELECT DISTINCT genre FROM books WHERE genre IS NOT NULL").fetchall()
        stats = []
        for g in genres:
            cnt = c.execute(
                "SELECT COUNT(*) FROM books WHERE genre=?",
                (g["genre"],)).fetchone()[0]
            stats.append({"genre": g["genre"], "count": cnt})
    return jsonify({"stats":stats,
                    "db_ms":round((time.perf_counter()-t0)*1000,3),**srv()})


@app.route("/genre_stats_opt")
def genre_stats_opt():
    t0 = time.perf_counter()
    with db() as c:
        rows = c.execute(
            "SELECT genre, COUNT(*) as cnt FROM books "
            "WHERE genre IS NOT NULL GROUP BY genre ORDER BY cnt DESC").fetchall()
    return jsonify({"stats":[dict(r) for r in rows],
                    "db_ms":round((time.perf_counter()-t0)*1000,3),**srv()})


@app.route("/add_book", methods=["POST"])
def add_book():
    d = request.json or {}
    t0 = time.perf_counter()
    with db() as c:
        cur = c.execute(
            "INSERT INTO books (title,author,year,genre,status) VALUES (?,?,?,?,?)",
            (d.get("title", fake_title()),
             d.get("author", fake_author()),
             d.get("year",   random.randint(1950, 2024)),
             d.get("genre",  random.choice(GENRES)),
             d.get("status", random.choice(STATUSES))))
        c.commit()
    return jsonify({"ok":True,"id":cur.lastrowid,
                    "db_ms":round((time.perf_counter()-t0)*1000,3),**srv()})


@app.route("/books/count")
def books_count():
    with db() as c:
        cnt = c.execute("SELECT COUNT(*) FROM books").fetchone()[0]
    return jsonify({"count":cnt,**srv()})


@app.route("/generate", methods=["POST"])
def generate():
    n = min(int(request.args.get("n", 1000)), 50000)
    data = [
        (fake_title(), fake_author(),
         random.randint(1950, 2024),
         random.choice(GENRES),
         random.choice(STATUSES))
        for _ in range(n)
    ]
    with db() as c:
        c.executemany(
            "INSERT INTO books (title,author,year,genre,status) VALUES (?,?,?,?,?)",
            data)
        c.commit()
        total = c.execute("SELECT COUNT(*) FROM books").fetchone()[0]
    return jsonify({"generated":n,"total":total,**srv()})


@app.route("/books/clear", methods=["DELETE"])
def clear_books():
    with db() as c:
        c.execute("DELETE FROM books")
        c.commit()
    return jsonify({"ok":True})


@app.route("/create_index", methods=["POST"])
def create_index():
    with db() as c:
        c.execute("CREATE INDEX IF NOT EXISTS idx_title  ON books(title)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_author ON books(author)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_genre  ON books(genre)")
        c.commit()
    return jsonify({"ok":True,"message":"Индексы созданы"})


@app.route("/drop_index", methods=["POST"])
def drop_index():
    with db() as c:
        c.execute("DROP INDEX IF EXISTS idx_title")
        c.execute("DROP INDEX IF EXISTS idx_author")
        c.execute("DROP INDEX IF EXISTS idx_genre")
        c.commit()
    return jsonify({"ok":True,"message":"Индексы удалены"})


if __name__ == "__main__":
    print(f"Сервер: http://0.0.0.0:{PORT}")
    app.run(host="0.0.0.0", port=PORT, threaded=True)
