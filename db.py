import os
import sqlite3
from datetime import datetime

# If you want to use Postgres on Render, set DATABASE_URL env var, e.g.
# DATABASE_URL=postgresql://user:password@host:port/database
DATABASE_URL = os.environ.get("DATABASE_URL")

USE_POSTGRES = bool(DATABASE_URL)

if USE_POSTGRES:
    try:
        import psycopg2
        import psycopg2.extras
    except Exception as e:
        raise RuntimeError(
            "psycopg2 is required for Postgres support. Install psycopg2-binary. Error: %s" % e
        )

DB_PATH = os.path.join(os.path.dirname(__file__), "data.db")


def connect():
    """
    Return a DB-API connection. If DATABASE_URL is set, connect to Postgres,
    otherwise use local SQLite file (data.db).
    """
    if USE_POSTGRES:
        # psycopg2.connect accepts the DATABASE_URL directly
        # Set cursor factory to return tuples (default) which matches existing code expectations
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    else:
        # allow multithreaded access if needed
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        return conn


# Helper to normalize parameter placeholder differences between sqlite ("?") and psycopg2 ("%s")
def _execute(cur, sql, params=None):
    if params is None:
        params = ()
    if USE_POSTGRES:
        # naive replacement of ? with %s for parameters
        sql_pg = sql.replace("?", "%s")
        return cur.execute(sql_pg, params)
    else:
        return cur.execute(sql, params)


def init_db():
    conn = connect()
    cur = conn.cursor()
    if USE_POSTGRES:
        # Create tables for Postgres
        # users - user_id provided externally (e.g. telegram id), so not serial
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS users(
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            joined_at TIMESTAMP,
            is_blocked INTEGER DEFAULT 0
        )"""
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS categories(
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE
        )"""
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS books(
            id SERIAL PRIMARY KEY,
            title TEXT,
            author TEXT,
            category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL,
            type TEXT,
            total_size BIGINT DEFAULT 0,
            duration_seconds INTEGER DEFAULT 0,
            created_at TIMESTAMP,
            downloads INTEGER DEFAULT 0,
            purchase_link TEXT,
            search_vector tsvector
        )"""
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS book_parts(
            id SERIAL PRIMARY KEY,
            book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
            file_id TEXT,
            part_index INTEGER,
            size BIGINT DEFAULT 0,
            duration_seconds INTEGER DEFAULT 0
        )"""
        )

        # Indexes
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_book_parts_book ON book_parts(book_id, part_index)"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_cat ON books(category_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_created ON books(created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_type ON books(type)")

        # Full-text search: use tsvector + GIN index and trigger to keep it updated
        cur.execute(
            """
        CREATE INDEX IF NOT EXISTS idx_books_search_vector ON books USING GIN (search_vector)
        """
        )

        # Create or replace function to update search_vector
        cur.execute(
            """
        CREATE OR REPLACE FUNCTION books_search_vector_trigger() RETURNS trigger AS $$
        begin
            new.search_vector :=
                to_tsvector('simple', coalesce(new.title,'') || ' ' || coalesce(new.author,''));
            return new;
        end
        $$ LANGUAGE plpgsql;
        """
        )

        # Create trigger to call function on insert or update
        cur.execute(
            """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_trigger WHERE tgname = 'books_search_vector_update_trg'
            ) THEN
                CREATE TRIGGER books_search_vector_update_trg
                BEFORE INSERT OR UPDATE ON books
                FOR EACH ROW EXECUTE PROCEDURE books_search_vector_trigger();
            END IF;
        END
        $$;
        """
        )

        # Backfill existing rows
        cur.execute(
            """
        UPDATE books SET search_vector = to_tsvector('simple', coalesce(title,'') || ' ' || coalesce(author,''))
        WHERE search_vector IS NULL
        """
        )

        # Missing queries, uploads, saved_books, wishes tables
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS missing_queries(
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            query TEXT,
            created_at TIMESTAMP
        )"""
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS user_uploads(
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            type TEXT,
            file_id TEXT,
            size BIGINT DEFAULT 0,
            duration_seconds INTEGER DEFAULT 0,
            created_at TIMESTAMP,
            is_seen INTEGER DEFAULT 0
        )"""
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS saved_books(
            user_id BIGINT,
            book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
            created_at TIMESTAMP,
            PRIMARY KEY(user_id, book_id)
        )"""
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS wishes(
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            text TEXT,
            created_at TIMESTAMP,
            is_seen INTEGER DEFAULT 0
        )"""
        )

        conn.commit()
        conn.close()

    else:
        # Original SQLite schema (keeps previous behavior)
        cur.execute("PRAGMA foreign_keys = ON")
        try:
            cur.execute("PRAGMA journal_mode = WAL")
            cur.execute("PRAGMA synchronous = NORMAL")
        except:
            pass
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            joined_at TEXT,
            is_blocked INTEGER DEFAULT 0
        )"""
        )
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS categories(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )"""
        )
        cur.execute(
            """
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
        )"""
        )
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS book_parts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            book_id INTEGER,
            file_id TEXT,
            part_index INTEGER,
            size INTEGER DEFAULT 0,
            duration_seconds INTEGER DEFAULT 0,
            FOREIGN KEY(book_id) REFERENCES books(id) ON DELETE CASCADE
        )"""
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_book_parts_book ON book_parts(book_id, part_index)"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_cat ON books(category_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_created ON books(created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_type ON books(type)")
        # FTS5 full-text search for fast queries (sqlite)
        try:
            cur.execute(
                "CREATE VIRTUAL TABLE IF NOT EXISTS books_fts USING fts5(title, author, content='books', content_rowid='id')"
            )
            # triggers to keep FTS in sync
            cur.execute(
                """
            CREATE TRIGGER IF NOT EXISTS books_ai AFTER INSERT ON books BEGIN
                INSERT INTO books_fts(rowid, title, author) VALUES (new.id, new.title, new.author);
            END;"""
            )
            cur.execute(
                """
            CREATE TRIGGER IF NOT EXISTS books_au AFTER UPDATE OF title, author ON books BEGIN
                INSERT INTO books_fts(rowid, title, author) VALUES (new.id, new.title, new.author);
            END;"""
            )
            cur.execute(
                """
            CREATE TRIGGER IF NOT EXISTS books_ad AFTER DELETE ON books BEGIN
                DELETE FROM books_fts WHERE rowid = old.id;
            END;"""
            )
            # backfill existing rows
            cur.execute(
                """
            INSERT INTO books_fts(rowid, title, author)
            SELECT id, title, author FROM books
            WHERE NOT EXISTS (SELECT 1 FROM books_fts f WHERE f.rowid = books.id)
            """
            )
        except:
            pass

        # Add purchase_link column if missing
        try:
            cur.execute("ALTER TABLE books ADD COLUMN purchase_link TEXT")
        except:
            pass
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS missing_queries(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            query TEXT,
            created_at TEXT
        )"""
        )
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS user_uploads(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            type TEXT,
            file_id TEXT,
            size INTEGER DEFAULT 0,
            duration_seconds INTEGER DEFAULT 0,
            created_at TEXT,
            is_seen INTEGER DEFAULT 0
        )"""
        )
        conn.commit()
        conn.close()

    # Ensure auxiliary tables exist
    ensure_saved_books_table()
    ensure_wishes_table()


def _conn_cursor():
    """
    Helper returning (conn, cur) and ensuring correct cursor factory for psycopg2.
    """
    conn = connect()
    if USE_POSTGRES:
        cur = conn.cursor()
    else:
        cur = conn.cursor()
    return conn, cur


def upsert_user(user_id, username, first_name):
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT user_id FROM users WHERE user_id=?", (user_id,))
    exists = cur.fetchone()
    if exists:
        _execute(cur, "UPDATE users SET username=?, first_name=? WHERE user_id=?", (username, first_name, user_id))
    else:
        _execute(
            cur,
            "INSERT INTO users(user_id, username, first_name, joined_at) VALUES(?,?,?,?)",
            (user_id, username, first_name, datetime.utcnow().isoformat()),
        )
    conn.commit()
    conn.close()


def set_block(user_id, blocked):
    conn, cur = _conn_cursor()
    _execute(cur, "UPDATE users SET is_blocked=? WHERE user_id=?", (1 if blocked else 0, user_id))
    conn.commit()
    conn.close()


def is_blocked(user_id):
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT is_blocked FROM users WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return bool(row[0]) if row else False


def get_user_count():
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT COUNT(*) FROM users")
    c = cur.fetchone()[0]
    conn.close()
    return c


def add_category(name):
    conn, cur = _conn_cursor()
    _execute(cur, "INSERT INTO categories(name) VALUES(?) ON CONFLICT DO NOTHING" if USE_POSTGRES else "INSERT OR IGNORE INTO categories(name) VALUES(?)", (name,))
    conn.commit()
    conn.close()


def delete_category(cat_id):
    conn, cur = _conn_cursor()
    _execute(cur, "DELETE FROM categories WHERE id=?", (cat_id,))
    conn.commit()
    conn.close()


def list_categories():
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT id, name FROM categories ORDER BY name")
    rows = cur.fetchall()
    conn.close()
    return rows


def create_book(title, author, category_id, type_):
    conn, cur = _conn_cursor()
    if USE_POSTGRES:
        # Use RETURNING id to get inserted id
        _execute(
            cur,
            "INSERT INTO books(title, author, category_id, type, created_at) VALUES(?,?,?,?,?) RETURNING id",
            (title, author, category_id, type_, datetime.utcnow().isoformat()),
        )
        book_id = cur.fetchone()[0]
    else:
        _execute(
            cur,
            "INSERT INTO books(title, author, category_id, type, created_at) VALUES(?,?,?,?,?)",
            (title, author, category_id, type_, datetime.utcnow().isoformat()),
        )
        book_id = cur.lastrowid
    conn.commit()
    conn.close()
    return book_id


def update_book_meta(book_id, title=None, author=None, category_id=None):
    conn, cur = _conn_cursor()
    if title is not None:
        _execute(cur, "UPDATE books SET title=? WHERE id=?", (title, book_id))
    if author is not None:
        _execute(cur, "UPDATE books SET author=? WHERE id=?", (author, book_id))
    if category_id is not None:
        _execute(cur, "UPDATE books SET category_id=? WHERE id=?", (category_id, book_id))
    conn.commit()
    conn.close()


def delete_book(book_id):
    conn, cur = _conn_cursor()
    _execute(cur, "DELETE FROM books WHERE id=?", (book_id,))
    conn.commit()
    conn.close()


def add_book_part(book_id, file_id, part_index, size=0, duration_seconds=0):
    conn, cur = _conn_cursor()
    _execute(cur, "INSERT INTO book_parts(book_id, file_id, part_index, size, duration_seconds) VALUES(?,?,?,?,?)",
             (book_id, file_id, part_index, size, duration_seconds))
    # update totals on books
    _execute(cur, "UPDATE books SET total_size = COALESCE(total_size,0) + ?, duration_seconds = COALESCE(duration_seconds,0) + ? WHERE id=?",
             (size, duration_seconds, book_id))
    conn.commit()
    conn.close()


def get_book(book_id):
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT id, title, author, category_id, type, total_size, duration_seconds, created_at, downloads, purchase_link FROM books WHERE id=?", (book_id,))
    b = cur.fetchone()
    conn.close()
    return b


def list_book_parts(book_id):
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT id, file_id, part_index, size, duration_seconds FROM book_parts WHERE book_id=? ORDER BY part_index ASC", (book_id,))
    rows = cur.fetchall()
    conn.close()
    return rows


def inc_download(book_id):
    conn, cur = _conn_cursor()
    _execute(cur, "UPDATE books SET downloads = COALESCE(downloads,0) + 1 WHERE id=?", (book_id,))
    conn.commit()
    conn.close()


def search_books(query, limit=20):
    conn, cur = _conn_cursor()
    if USE_POSTGRES:
        # Use plainto_tsquery for simple search
        # order by rank desc, downloads desc, created_at desc
        sql = """
            SELECT id, title, author, type, COALESCE(downloads,0) AS downloads
            FROM books
            WHERE search_vector @@ plainto_tsquery('simple', ?)
            ORDER BY ts_rank_cd(search_vector, plainto_tsquery('simple', ?)) DESC, downloads DESC, created_at DESC
            LIMIT ?
        """
        _execute(cur, sql, (query, query, limit))
        rows = cur.fetchall()
        conn.close()
        return rows
    else:
        try:
            # sqlite FTS5
            _execute(
                cur,
                """
                SELECT b.id, b.title, b.author, b.type, COALESCE(b.downloads,0) AS downloads
                FROM books_fts f JOIN books b ON b.id = f.rowid
                WHERE books_fts MATCH ?
                ORDER BY bm25(f) ASC, b.downloads DESC, b.created_at DESC
                LIMIT ?
            """,
                (query, limit),
            )
            rows = cur.fetchall()
            conn.close()
            return rows
        except:
            q = f"%{query.lower()}%"
            _execute(
                cur,
                """
                SELECT id, title, author, type, COALESCE(downloads,0) AS downloads FROM books
                WHERE lower(title) LIKE ? OR lower(author) LIKE ?
                ORDER BY downloads DESC, created_at DESC
                LIMIT ?
            """,
                (q, q, limit),
            )
            rows = cur.fetchall()
            conn.close()
            return rows


def books_by_category(cat_id, limit=50):
    conn, cur = _conn_cursor()
    _execute(cur, """
        SELECT id, title, author, type, COALESCE(downloads,0) AS downloads FROM books
        WHERE category_id=?
        ORDER BY created_at DESC
        LIMIT ?
    """, (cat_id, limit))
    rows = cur.fetchall()
    conn.close()
    return rows


def stats_counts():
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT COUNT(*) FROM books WHERE type='audio'")
    audio = cur.fetchone()[0]
    _execute(cur, "SELECT COUNT(*) FROM books WHERE type='pdf'")
    pdf = cur.fetchone()[0]
    conn.close()
    return audio, pdf


def top_books(limit=10):
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT id, title, author, type FROM books ORDER BY downloads DESC, created_at DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows


def recent_books(limit=20):
    conn, cur = _conn_cursor()
    if USE_POSTGRES:
        _execute(cur, "SELECT id, title, author, type FROM books ORDER BY created_at DESC LIMIT ?", (limit,))
    else:
        _execute(cur, "SELECT id, title, author, type FROM books ORDER BY datetime(created_at) DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows


def random_books(limit=10):
    conn, cur = _conn_cursor()
    if USE_POSTGRES:
        _execute(cur, "SELECT id, title, author, type FROM books ORDER BY RANDOM() LIMIT ?", (limit,))
    else:
        _execute(cur, "SELECT id, title, author, type FROM books ORDER BY RANDOM() LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows


def save_missing_query(user_id, query):
    conn, cur = _conn_cursor()
    _execute(cur, "INSERT INTO missing_queries(user_id, query, created_at) VALUES(?,?,?)",
             (user_id, query, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()


def list_missing_queries_agg(limit=50):
    conn, cur = _conn_cursor()
    _execute(cur, """
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
    conn, cur = _conn_cursor()
    _execute(cur, "DELETE FROM missing_queries")
    conn.commit()
    conn.close()


def save_user_upload(user_id, type_, file_id, size=0, duration_seconds=0):
    conn, cur = _conn_cursor()
    _execute(
        cur,
        """
        INSERT INTO user_uploads(user_id, type, file_id, size, duration_seconds, created_at)
        VALUES(?,?,?,?,?,?)
    """,
        (user_id, type_, file_id, size, duration_seconds, datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()


def list_unseen_uploads(limit=50):
    conn, cur = _conn_cursor()
    if USE_POSTGRES:
        _execute(
            cur,
            """
        SELECT id, user_id, type, file_id, size, duration_seconds, created_at
        FROM user_uploads
        WHERE COALESCE(is_seen,0)=0
        ORDER BY created_at ASC
        LIMIT ?
    """,
            (limit,),
        )
    else:
        _execute(
            cur,
            """
        SELECT id, user_id, type, file_id, size, duration_seconds, created_at
        FROM user_uploads
        WHERE COALESCE(is_seen,0)=0
        ORDER BY datetime(created_at) ASC
        LIMIT ?
    """,
            (limit,),
        )
    rows = cur.fetchall()
    conn.close()
    return rows


def mark_all_uploads_seen():
    conn, cur = _conn_cursor()
    _execute(cur, "UPDATE user_uploads SET is_seen=1 WHERE COALESCE(is_seen,0)=0")
    conn.commit()
    conn.close()


def file_exists_in_server(file_id: str) -> bool:
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT 1 FROM book_parts WHERE file_id=? LIMIT 1", (file_id,))
    row = cur.fetchone()
    conn.close()
    return bool(row)


# Saved books
def ensure_saved_books_table():
    conn, cur = _conn_cursor()
    if USE_POSTGRES:
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS saved_books(
            user_id BIGINT,
            book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
            created_at TIMESTAMP,
            PRIMARY KEY(user_id, book_id)
        )"""
        )
    else:
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS saved_books(
            user_id INTEGER,
            book_id INTEGER,
            created_at TEXT,
            PRIMARY KEY(user_id, book_id),
            FOREIGN KEY(book_id) REFERENCES books(id) ON DELETE CASCADE
        )"""
        )
    conn.commit()
    conn.close()


def add_saved_book(user_id: int, book_id: int):
    ensure_saved_books_table()
    conn, cur = _conn_cursor()
    _execute(cur, "INSERT INTO saved_books(user_id, book_id, created_at) VALUES(?,?,?) ON CONFLICT DO NOTHING" if USE_POSTGRES else "INSERT OR IGNORE INTO saved_books(user_id, book_id, created_at) VALUES(?,?,?)",
             (user_id, book_id, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()


def list_saved_books(user_id: int, offset=0, limit=10):
    ensure_saved_books_table()
    conn, cur = _conn_cursor()
    if USE_POSTGRES:
        _execute(
            cur,
            """
        SELECT b.id, b.title, b.author, b.type, COALESCE(b.downloads,0) AS downloads
        FROM saved_books s JOIN books b ON b.id = s.book_id
        WHERE s.user_id=?
        ORDER BY s.created_at DESC
        LIMIT ? OFFSET ?
    """,
            (user_id, limit, offset),
        )
    else:
        _execute(
            cur,
            """
        SELECT b.id, b.title, b.author, b.type, COALESCE(b.downloads,0) AS downloads
        FROM saved_books s JOIN books b ON b.id = s.book_id
        WHERE s.user_id=?
        ORDER BY datetime(s.created_at) DESC
        LIMIT ? OFFSET ?
    """,
            (user_id, limit, offset),
        )
    rows = cur.fetchall()
    conn.close()
    return rows


def is_book_saved(user_id: int, book_id: int) -> bool:
    ensure_saved_books_table()
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT 1 FROM saved_books WHERE user_id=? AND book_id=? LIMIT 1", (user_id, book_id))
    row = cur.fetchone()
    conn.close()
    return bool(row)


def remove_saved_book(user_id: int, book_id: int):
    ensure_saved_books_table()
    conn, cur = _conn_cursor()
    _execute(cur, "DELETE FROM saved_books WHERE user_id=? AND book_id=?", (user_id, book_id))
    conn.commit()
    conn.close()


def user_saved_count(user_id: int) -> int:
    ensure_saved_books_table()
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT COUNT(*) FROM saved_books WHERE user_id=?", (user_id,))
    c = cur.fetchone()[0] or 0
    conn.close()
    return c


# Purchase link helpers
def set_purchase_link(book_id: int, link: str):
    conn, cur = _conn_cursor()
    _execute(cur, "UPDATE books SET purchase_link=? WHERE id=?", (link, book_id))
    conn.commit()
    conn.close()


def clear_purchase_link(book_id: int):
    conn, cur = _conn_cursor()
    _execute(cur, "UPDATE books SET purchase_link=NULL WHERE id=?", (book_id,))
    conn.commit()
    conn.close()


# Counters for statistics
def saved_books_count():
    ensure_saved_books_table()
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT COUNT(*) FROM saved_books")
    c = cur.fetchone()[0]
    conn.close()
    return c


def uploads_count():
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT COUNT(*) FROM user_uploads")
    c = cur.fetchone()[0]
    conn.close()
    return c


def missing_queries_count():
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT COUNT(*) FROM missing_queries")
    c = cur.fetchone()[0]
    conn.close()
    return c


def total_downloads():
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT COALESCE(SUM(downloads),0) FROM books")
    s = cur.fetchone()[0] or 0
    conn.close()
    return s


def ensure_wishes_table():
    conn, cur = _conn_cursor()
    if USE_POSTGRES:
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS wishes(
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            text TEXT,
            created_at TIMESTAMP,
            is_seen INTEGER DEFAULT 0
        )"""
        )
    else:
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS wishes(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            text TEXT,
            created_at TEXT,
            is_seen INTEGER DEFAULT 0
        )"""
        )
        try:
            cur.execute("ALTER TABLE wishes ADD COLUMN is_seen INTEGER DEFAULT 0")
        except:
            pass
    conn.commit()
    conn.close()


def add_wish(user_id: int, text: str):
    ensure_wishes_table()
    conn, cur = _conn_cursor()
    _execute(cur, "INSERT INTO wishes(user_id, text, created_at) VALUES(?,?,?)",
             (user_id, text, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()


def wishes_count():
    ensure_wishes_table()
    conn, cur = _conn_cursor()
    _execute(cur, "SELECT COUNT(*) FROM wishes WHERE COALESCE(is_seen,0)=0")
    c = cur.fetchone()[0]
    conn.close()
    return c


def list_wishes(offset=0, limit=50, only_unseen=True):
    ensure_wishes_table()
    conn, cur = _conn_cursor()
    if only_unseen:
        if USE_POSTGRES:
            _execute(
                cur,
                """
            SELECT id, user_id, text, created_at
            FROM wishes
            WHERE COALESCE(is_seen,0)=0
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        """,
                (limit, offset),
            )
        else:
            _execute(
                cur,
                """
            SELECT id, user_id, text, created_at
            FROM wishes
            WHERE COALESCE(is_seen,0)=0
            ORDER BY datetime(created_at) DESC
            LIMIT ? OFFSET ?
        """,
                (limit, offset),
            )
    else:
        if USE_POSTGRES:
            _execute(
                cur,
                """
            SELECT id, user_id, text, created_at
            FROM wishes
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        """,
                (limit, offset),
            )
        else:
            _execute(
                cur,
                """
            SELECT id, user_id, text, created_at
            FROM wishes
            ORDER BY datetime(created_at) DESC
            LIMIT ? OFFSET ?
        """,
                (limit, offset),
            )
    rows = cur.fetchall()
    conn.close()
    return rows


def mark_wish_seen(wish_id: int):
    ensure_wishes_table()
    conn, cur = _conn_cursor()
    _execute(cur, "UPDATE wishes SET is_seen=1 WHERE id=?", (wish_id,))
    conn.commit()
    conn.close()


def list_wishes_agg(limit=50, offset=0, only_unseen=True):
    ensure_wishes_table()
    conn, cur = _conn_cursor()
    if only_unseen:
        _execute(
            cur,
            """
            SELECT text, COUNT(*) AS cnt
            FROM wishes
            WHERE COALESCE(is_seen,0)=0
            GROUP BY text
            ORDER BY cnt DESC
            LIMIT ? OFFSET ?
        """,
            (limit, offset),
        )
    else:
        _execute(
            cur,
            """
            SELECT text, COUNT(*) AS cnt
            FROM wishes
            GROUP BY text
            ORDER BY cnt DESC
            LIMIT ? OFFSET ?
        """,
            (limit, offset),
        )
    rows = cur.fetchall()
    conn.close()
    return rows
