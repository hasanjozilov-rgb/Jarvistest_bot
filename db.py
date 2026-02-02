import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "data.db")

def connect():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = connect()
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON")
    try:
        cur.execute("PRAGMA journal_mode = WAL")
        cur.execute("PRAGMA synchronous = NORMAL")
    except:
        pass
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        joined_at TEXT,
        is_blocked INTEGER DEFAULT 0
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS categories(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS books(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        author TEXT,
        category_id INTEGER,
        type TEXT,
        total_size INTEGER DEFAULT 0,
        duration_seconds INTEGER DEFAULT 0,
        created_at TEXT,
        downloads INTEGER DEFAULT 0,
        FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE SET NULL
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS book_parts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        book_id INTEGER,
        file_id TEXT,
        part_index INTEGER,
        size INTEGER DEFAULT 0,
        duration_seconds INTEGER DEFAULT 0,
        FOREIGN KEY(book_id) REFERENCES books(id) ON DELETE CASCADE
    )""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_book_parts_book ON book_parts(book_id, part_index)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_books_cat ON books(category_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_books_created ON books(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_books_type ON books(type)")
    # FTS5 full-text search for fast queries
    try:
        cur.execute("CREATE VIRTUAL TABLE IF NOT EXISTS books_fts USING fts5(title, author, content='books', content_rowid='id')")
        # triggers to keep FTS in sync
        cur.execute("""
        CREATE TRIGGER IF NOT EXISTS books_ai AFTER INSERT ON books BEGIN
            INSERT INTO books_fts(rowid, title, author) VALUES (new.id, new.title, new.author);
        END;""")
        cur.execute("""
        CREATE TRIGGER IF NOT EXISTS books_au AFTER UPDATE OF title, author ON books BEGIN
            INSERT INTO books_fts(rowid, title, author) VALUES (new.id, new.title, new.author);
        END;""")
        cur.execute("""
        CREATE TRIGGER IF NOT EXISTS books_ad AFTER DELETE ON books BEGIN
            DELETE FROM books_fts WHERE rowid = old.id;
        END;""")
        # backfill existing rows
        cur.execute("""
        INSERT INTO books_fts(rowid, title, author)
        SELECT id, title, author FROM books
        WHERE NOT EXISTS (SELECT 1 FROM books_fts f WHERE f.rowid = books.id)
        """)
    except:
        pass

    # Add purchase_link column if missing
    try:
        cur.execute("ALTER TABLE books ADD COLUMN purchase_link TEXT")
    except:
        pass
    cur.execute("""
    CREATE TABLE IF NOT EXISTS missing_queries(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        query TEXT,
        created_at TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_uploads(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        type TEXT,
        file_id TEXT,
        size INTEGER DEFAULT 0,
        duration_seconds INTEGER DEFAULT 0,
        created_at TEXT,
        is_seen INTEGER DEFAULT 0
    )""")
    conn.commit()
    conn.close()
    # Ensure auxiliary tables exist
    ensure_saved_books_table()
    ensure_wishes_table()

def upsert_user(user_id, username, first_name):
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
    exists = cur.fetchone()
    if exists:
        cur.execute("UPDATE users SET username=?, first_name=? WHERE user_id=?", (username, first_name, user_id))
    else:
        cur.execute("INSERT INTO users(user_id, username, first_name, joined_at) VALUES(?,?,?,?)",
                    (user_id, username, first_name, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()

def set_block(user_id, blocked):
    conn = connect()
    cur = conn.cursor()
    cur.execute("UPDATE users SET is_blocked=? WHERE user_id=?", (1 if blocked else 0, user_id))
    conn.commit()
    conn.close()

def is_blocked(user_id):
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT is_blocked FROM users WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return bool(row[0]) if row else False

def get_user_count():
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    c = cur.fetchone()[0]
    conn.close()
    return c

def add_category(name):
    conn = connect()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO categories(name) VALUES(?)", (name,))
    conn.commit()
    conn.close()

def delete_category(cat_id):
    conn = connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM categories WHERE id=?", (cat_id,))
    conn.commit()
    conn.close()

def list_categories():
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM categories ORDER BY name")
    rows = cur.fetchall()
    conn.close()
    return rows

def create_book(title, author, category_id, type_):
    conn = connect()
    cur = conn.cursor()
    cur.execute("INSERT INTO books(title, author, category_id, type, created_at) VALUES(?,?,?,?,?)",
                (title, author, category_id, type_, datetime.utcnow().isoformat()))
    book_id = cur.lastrowid
    conn.commit()
    conn.close()
    return book_id

def update_book_meta(book_id, title=None, author=None, category_id=None):
    conn = connect()
    cur = conn.cursor()
    if title is not None:
        cur.execute("UPDATE books SET title=? WHERE id=?", (title, book_id))
    if author is not None:
        cur.execute("UPDATE books SET author=? WHERE id=?", (author, book_id))
    if category_id is not None:
        cur.execute("UPDATE books SET category_id=? WHERE id=?", (category_id, book_id))
    conn.commit()
    conn.close()

def delete_book(book_id):
    conn = connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM books WHERE id=?", (book_id,))
    conn.commit()
    conn.close()

def add_book_part(book_id, file_id, part_index, size=0, duration_seconds=0):
    conn = connect()
    cur = conn.cursor()
    cur.execute("INSERT INTO book_parts(book_id, file_id, part_index, size, duration_seconds) VALUES(?,?,?,?,?)",
                (book_id, file_id, part_index, size, duration_seconds))
    cur.execute("UPDATE books SET total_size = COALESCE(total_size,0) + ?, duration_seconds = COALESCE(duration_seconds,0) + ? WHERE id=?",
                (size, duration_seconds, book_id))
    conn.commit()
    conn.close()

def get_book(book_id):
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT id, title, author, category_id, type, total_size, duration_seconds, created_at, downloads, purchase_link FROM books WHERE id=?", (book_id,))
    b = cur.fetchone()
    conn.close()
    return b

def list_book_parts(book_id):
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT id, file_id, part_index, size, duration_seconds FROM book_parts WHERE book_id=? ORDER BY part_index ASC", (book_id,))
    rows = cur.fetchall()
    conn.close()
    return rows

def inc_download(book_id):
    conn = connect()
    cur = conn.cursor()
    cur.execute("UPDATE books SET downloads = COALESCE(downloads,0) + 1 WHERE id=?", (book_id,))
    conn.commit()
    conn.close()

def search_books(query, limit=20):
    conn = connect()
    cur = conn.cursor()
    # Try FTS5 first for better performance and relevancy
    try:
        cur.execute("""
            SELECT b.id, b.title, b.author, b.type, COALESCE(b.downloads,0) AS downloads
            FROM books_fts f JOIN books b ON b.id = f.rowid
            WHERE books_fts MATCH ?
            ORDER BY bm25(f) ASC, b.downloads DESC, b.created_at DESC
            LIMIT ?
        """, (query, limit))
        rows = cur.fetchall()
        conn.close()
        return rows
    except:
        q = f"%{query.lower()}%"
        cur.execute("""
            SELECT id, title, author, type, COALESCE(downloads,0) AS downloads FROM books
            WHERE lower(title) LIKE ? OR lower(author) LIKE ?
            ORDER BY downloads DESC, created_at DESC
            LIMIT ?
        """, (q, q, limit))
        rows = cur.fetchall()
        conn.close()
        return rows

def books_by_category(cat_id, limit=50):
    conn = connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, title, author, type, COALESCE(downloads,0) AS downloads FROM books
        WHERE category_id=?
        ORDER BY created_at DESC
        LIMIT ?
    """, (cat_id, limit))
    rows = cur.fetchall()
    conn.close()
    return rows

def stats_counts():
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM books WHERE type='audio'")
    audio = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM books WHERE type='pdf'")
    pdf = cur.fetchone()[0]
    conn.close()
    return audio, pdf

def top_books(limit=10):
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT id, title, author, type FROM books ORDER BY downloads DESC, created_at DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows

def recent_books(limit=20):
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT id, title, author, type FROM books ORDER BY datetime(created_at) DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows

def random_books(limit=10):
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT id, title, author, type FROM books ORDER BY RANDOM() LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows

def save_missing_query(user_id, query):
    conn = connect()
    cur = conn.cursor()
    cur.execute("INSERT INTO missing_queries(user_id, query, created_at) VALUES(?,?,?)",
                (user_id, query, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()

def list_missing_queries_agg(limit=50):
    conn = connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT query, COUNT(*) AS cnt, MAX(created_at) AS last_at
        FROM missing_queries
        GROUP BY query
        ORDER BY cnt DESC, last_at DESC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows

def clear_missing_queries():
    conn = connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM missing_queries")
    conn.commit()
    conn.close()

def save_user_upload(user_id, type_, file_id, size=0, duration_seconds=0):
    conn = connect()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO user_uploads(user_id, type, file_id, size, duration_seconds, created_at)
        VALUES(?,?,?,?,?,?)
    """, (user_id, type_, file_id, size, duration_seconds, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()

def list_unseen_uploads(limit=50):
    conn = connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, user_id, type, file_id, size, duration_seconds, created_at
        FROM user_uploads
        WHERE COALESCE(is_seen,0)=0
        ORDER BY datetime(created_at) ASC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows

def mark_all_uploads_seen():
    conn = connect()
    cur = conn.cursor()
    cur.execute("UPDATE user_uploads SET is_seen=1 WHERE COALESCE(is_seen,0)=0")
    conn.commit()
    conn.close()

def file_exists_in_server(file_id: str) -> bool:
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM book_parts WHERE file_id=? LIMIT 1", (file_id,))
    row = cur.fetchone()
    conn.close()
    return bool(row)

# Saved books
def ensure_saved_books_table():
    conn = connect()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS saved_books(
        user_id INTEGER,
        book_id INTEGER,
        created_at TEXT,
        PRIMARY KEY(user_id, book_id),
        FOREIGN KEY(book_id) REFERENCES books(id) ON DELETE CASCADE
    )""")
    conn.commit()
    conn.close()

def add_saved_book(user_id: int, book_id: int):
    ensure_saved_books_table()
    conn = connect()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO saved_books(user_id, book_id, created_at) VALUES(?,?,?)",
                (user_id, book_id, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()

def list_saved_books(user_id: int, offset=0, limit=10):
    ensure_saved_books_table()
    conn = connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT b.id, b.title, b.author, b.type, COALESCE(b.downloads,0) AS downloads
        FROM saved_books s JOIN books b ON b.id = s.book_id
        WHERE s.user_id=?
        ORDER BY datetime(s.created_at) DESC
        LIMIT ? OFFSET ?
    """, (user_id, limit, offset))
    rows = cur.fetchall()
    conn.close()
    return rows

def is_book_saved(user_id: int, book_id: int) -> bool:
    ensure_saved_books_table()
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM saved_books WHERE user_id=? AND book_id=? LIMIT 1", (user_id, book_id))
    row = cur.fetchone()
    conn.close()
    return bool(row)

def remove_saved_book(user_id: int, book_id: int):
    ensure_saved_books_table()
    conn = connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM saved_books WHERE user_id=? AND book_id=?", (user_id, book_id))
    conn.commit()
    conn.close()

def user_saved_count(user_id: int) -> int:
    ensure_saved_books_table()
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM saved_books WHERE user_id=?", (user_id,))
    c = cur.fetchone()[0] or 0
    conn.close()
    return c

# Purchase link helpers
def set_purchase_link(book_id: int, link: str):
    conn = connect()
    cur = conn.cursor()
    cur.execute("UPDATE books SET purchase_link=? WHERE id=?", (link, book_id))
    conn.commit()
    conn.close()

def clear_purchase_link(book_id: int):
    conn = connect()
    cur = conn.cursor()
    cur.execute("UPDATE books SET purchase_link=NULL WHERE id=?", (book_id,))
    conn.commit()
    conn.close()

# Counters for statistics
def saved_books_count():
    ensure_saved_books_table()
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM saved_books")
    c = cur.fetchone()[0]
    conn.close()
    return c

def uploads_count():
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM user_uploads")
    c = cur.fetchone()[0]
    conn.close()
    return c

def missing_queries_count():
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM missing_queries")
    c = cur.fetchone()[0]
    conn.close()
    return c

def total_downloads():
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(downloads),0) FROM books")
    s = cur.fetchone()[0] or 0
    conn.close()
    return s

def ensure_wishes_table():
    conn = connect()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS wishes(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        text TEXT,
        created_at TEXT,
        is_seen INTEGER DEFAULT 0
    )""")
    try:
        cur.execute("ALTER TABLE wishes ADD COLUMN is_seen INTEGER DEFAULT 0")
    except:
        pass
    conn.commit()
    conn.close()

def add_wish(user_id: int, text: str):
    ensure_wishes_table()
    conn = connect()
    cur = conn.cursor()
    cur.execute("INSERT INTO wishes(user_id, text, created_at) VALUES(?,?,?)",
                (user_id, text, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()

def wishes_count():
    ensure_wishes_table()
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM wishes WHERE COALESCE(is_seen,0)=0")
    c = cur.fetchone()[0]
    conn.close()
    return c

def list_wishes(offset=0, limit=50, only_unseen=True):
    ensure_wishes_table()
    conn = connect()
    cur = conn.cursor()
    if only_unseen:
        cur.execute("""
            SELECT id, user_id, text, created_at
            FROM wishes
            WHERE COALESCE(is_seen,0)=0
            ORDER BY datetime(created_at) DESC
            LIMIT ? OFFSET ?
        """, (limit, offset))
    else:
        cur.execute("""
            SELECT id, user_id, text, created_at
            FROM wishes
            ORDER BY datetime(created_at) DESC
            LIMIT ? OFFSET ?
        """, (limit, offset))
    rows = cur.fetchall()
    conn.close()
    return rows

def mark_wish_seen(wish_id: int):
    ensure_wishes_table()
    conn = connect()
    cur = conn.cursor()
    cur.execute("UPDATE wishes SET is_seen=1 WHERE id=?", (wish_id,))
    conn.commit()
    conn.close()

def list_wishes_agg(limit=50, offset=0, only_unseen=True):
    ensure_wishes_table()
    conn = connect()
    cur = conn.cursor()
    if only_unseen:
        cur.execute("""
            SELECT text, COUNT(*) AS cnt
            FROM wishes
            WHERE COALESCE(is_seen,0)=0
            GROUP BY text
            ORDER BY cnt DESC
            LIMIT ? OFFSET ?
        """, (limit, offset))
    else:
        cur.execute("""
            SELECT text, COUNT(*) AS cnt
            FROM wishes
            GROUP BY text
            ORDER BY cnt DESC
            LIMIT ? OFFSET ?
        """, (limit, offset))
    rows = cur.fetchall()
    conn.close()
    return rows
