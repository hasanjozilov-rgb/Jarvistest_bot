# GitHub Copilot Chat Assistant
import os
from datetime import datetime
from typing import List, Tuple, Optional

from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Text, DateTime,
    ForeignKey, func, select, insert, update, delete, and_, text
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError

# Read DATABASE_URL from environment (Render provides this)
DB_URL = os.environ.get("DATABASE_URL")

# Fallback to local sqlite file if DATABASE_URL is not set
if DB_URL:
    engine: Engine = create_engine(DB_URL, future=True)
else:
    sqlite_path = os.path.join(os.path.dirname(__file__), "data.db")
    engine = create_engine(f"sqlite:///{sqlite_path}", future=True, connect_args={"check_same_thread": False})

metadata = MetaData()

# Table definitions (compatible with both SQLite and Postgres)
users = Table(
    "users", metadata,
    Column("user_id", Integer, primary_key=True),
    Column("username", String),
    Column("first_name", String),
    Column("joined_at", String),  # store ISO timestamp
    Column("is_blocked", Integer, default=0)
)

categories = Table(
    "categories", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String, unique=True)
)

books = Table(
    "books", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", String),
    Column("author", String),
    Column("category_id", Integer, ForeignKey("categories.id", ondelete="SET NULL")),
    Column("type", String),
    Column("total_size", Integer, default=0),
    Column("duration_seconds", Integer, default=0),
    Column("created_at", String),  # ISO timestamp
    Column("downloads", Integer, default=0),
    Column("purchase_link", String, nullable=True)
)

book_parts = Table(
    "book_parts", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("book_id", Integer, ForeignKey("books.id", ondelete="CASCADE")),
    Column("file_id", String),
    Column("part_index", Integer),
    Column("size", Integer, default=0),
    Column("duration_seconds", Integer, default=0)
)

missing_queries = Table(
    "missing_queries", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer),
    Column("query", Text),
    Column("created_at", String)
)

user_uploads = Table(
    "user_uploads", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer),
    Column("type", String),
    Column("file_id", String),
    Column("size", Integer, default=0),
    Column("duration_seconds", Integer, default=0),
    Column("created_at", String),
    Column("is_seen", Integer, default=0)
)

saved_books = Table(
    "saved_books", metadata,
    Column("user_id", Integer, primary_key=True),
    Column("book_id", Integer, primary_key=True),
    Column("created_at", String),
)

wishes = Table(
    "wishes", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer),
    Column("text", Text),
    Column("created_at", String),
    Column("is_seen", Integer, default=0)
)

def connect():
    """Return a new connection (SQLAlchemy Connection)."""
    return engine.connect()

def init_db():
    """Create tables if they do not exist."""
    metadata.create_all(engine)

# Helper to convert SQLAlchemy Row to tuple like sqlite cursor results
def _row_to_tuple(row):
    if row is None:
        return None
    return tuple(row)

# ---- CRUD and helper functions ----

def upsert_user(user_id: int, username: Optional[str], first_name: Optional[str]):
    with engine.begin() as conn:
        sel = select(users.c.user_id).where(users.c.user_id == user_id)
        r = conn.execute(sel).fetchone()
        if r:
            stmt = update(users).where(users.c.user_id == user_id).values(username=username, first_name=first_name)
            conn.execute(stmt)
        else:
            stmt = insert(users).values(
                user_id=user_id,
                username=username,
                first_name=first_name,
                joined_at=datetime.utcnow().isoformat()
            )
            conn.execute(stmt)

def set_block(user_id: int, blocked: bool):
    with engine.begin() as conn:
        stmt = update(users).where(users.c.user_id == user_id).values(is_blocked=1 if blocked else 0)
        conn.execute(stmt)

def is_blocked(user_id: int) -> bool:
    with engine.connect() as conn:
        sel = select(users.c.is_blocked).where(users.c.user_id == user_id)
        r = conn.execute(sel).fetchone()
        return bool(r[0]) if r else False

def get_user_count() -> int:
    with engine.connect() as conn:
        sel = select(func.count()).select_from(users)
        return conn.execute(sel).scalar() or 0

def add_category(name: str):
    with engine.begin() as conn:
        # INSERT OR IGNORE semantics
        if engine.dialect.name == "postgresql":
            stmt = insert(categories).values(name=name).on_conflict_do_nothing(index_elements=["name"])
            conn.execute(stmt)
        else:
            # sqlite: use simple insert with try/except
            try:
                conn.execute(insert(categories).values(name=name))
            except:
                pass

def delete_category(cat_id: int):
    with engine.begin() as conn:
        conn.execute(delete(categories).where(categories.c.id == cat_id))

def list_categories() -> List[Tuple]:
    with engine.connect() as conn:
        sel = select(categories.c.id, categories.c.name).order_by(categories.c.name)
        rows = conn.execute(sel).fetchall()
        return [tuple(r) for r in rows]

def create_book(title: str, author: str, category_id: Optional[int], type_: str) -> int:
    with engine.begin() as conn:
        stmt = insert(books).values(
            title=title,
            author=author,
            category_id=category_id,
            type=type_,
            created_at=datetime.utcnow().isoformat()
        )
        result = conn.execute(stmt)
        # inserted_primary_key may be available
        pk = None
        try:
            pk = result.inserted_primary_key[0]
        except:
            # fallback: fetch last inserted id for sqlite
            r = conn.execute(select(books.c.id).order_by(books.c.id.desc()).limit(1)).fetchone()
            pk = r[0] if r else None
        return int(pk) if pk is not None else None

def update_book_meta(book_id: int, title: Optional[str]=None, author: Optional[str]=None, category_id: Optional[int]=None):
    with engine.begin() as conn:
        values = {}
        if title is not None:
            values["title"] = title
        if author is not None:
            values["author"] = author
        if category_id is not None:
            values["category_id"] = category_id
        if values:
            conn.execute(update(books).where(books.c.id == book_id).values(**values))

def delete_book(book_id: int):
    with engine.begin() as conn:
        conn.execute(delete(books).where(books.c.id == book_id))

def add_book_part(book_id: int, file_id: str, part_index: int, size: int=0, duration_seconds: int=0):
    with engine.begin() as conn:
        conn.execute(insert(book_parts).values(
            book_id=book_id, file_id=file_id, part_index=part_index, size=size, duration_seconds=duration_seconds
        ))
        # update aggregate fields on books
        conn.execute(update(books).where(books.c.id == book_id).values(
            total_size=(books.c.total_size + size),
            duration_seconds=(books.c.duration_seconds + duration_seconds)
        ))

def get_book(book_id: int):
    with engine.connect() as conn:
        sel = select(
            books.c.id, books.c.title, books.c.author, books.c.category_id, books.c.type,
            books.c.total_size, books.c.duration_seconds, books.c.created_at, books.c.downloads, books.c.purchase_link
        ).where(books.c.id == book_id)
        r = conn.execute(sel).fetchone()
        return tuple(r) if r else None

def list_book_parts(book_id: int):
    with engine.connect() as conn:
        sel = select(book_parts.c.id, book_parts.c.file_id, book_parts.c.part_index, book_parts.c.size, book_parts.c.duration_seconds).where(book_parts.c.book_id == book_id).order_by(book_parts.c.part_index.asc())
        rows = conn.execute(sel).fetchall()
        return [tuple(r) for r in rows]

def inc_download(book_id: int):
    with engine.begin() as conn:
        conn.execute(update(books).where(books.c.id == book_id).values(downloads=(books.c.downloads + 1)))

def search_books(query: str, limit: int=20):
    q = f"%{query}%"
    with engine.connect() as conn:
        # Use ILIKE on Postgres; ilike() is supported by SQLAlchemy
        try:
            sel = select(books.c.id, books.c.title, books.c.author, books.c.type, books.c.downloads).where(
                or_(books.c.title.ilike(q), books.c.author.ilike(q))
            ).order_by(books.c.downloads.desc(), books.c.created_at.desc()).limit(limit)
        except Exception:
            # if ilike not available, fallback to lower LIKE
            sel = select(books.c.id, books.c.title, books.c.author, books.c.type, books.c.downloads).where(
                or_(func.lower(books.c.title).like(q.lower()), func.lower(books.c.author).like(q.lower()))
            ).order_by(books.c.downloads.desc(), books.c.created_at.desc()).limit(limit)
        rows = conn.execute(sel).fetchall()
        return [tuple(r) for r in rows]

# import or_ used above
from sqlalchemy import or_

def books_by_category(cat_id: int, limit: int=50):
    with engine.connect() as conn:
        sel = select(books.c.id, books.c.title, books.c.author, books.c.type, books.c.downloads).where(books.c.category_id == cat_id).order_by(books.c.created_at.desc()).limit(limit)
        rows = conn.execute(sel).fetchall()
        return [tuple(r) for r in rows]

def stats_counts():
    with engine.connect() as conn:
        audio = conn.execute(select(func.count()).where(books.c.type == 'audio')).scalar() or 0
        pdf = conn.execute(select(func.count()).where(books.c.type == 'pdf')).scalar() or 0
        return int(audio), int(pdf)

def top_books(limit: int=10):
    with engine.connect() as conn:
        sel = select(books.c.id, books.c.title, books.c.author, books.c.type).order_by(books.c.downloads.desc(), books.c.created_at.desc()).limit(limit)
        rows = conn.execute(sel).fetchall()
        return [tuple(r) for r in rows]

def recent_books(limit: int=20):
    with engine.connect() as conn:
        sel = select(books.c.id, books.c.title, books.c.author, books.c.type).order_by(books.c.created_at.desc()).limit(limit)
        rows = conn.execute(sel).fetchall()
        return [tuple(r) for r in rows]

def random_books(limit: int=10):
    with engine.connect() as conn:
        if engine.dialect.name == "postgresql":
            sel = select(books.c.id, books.c.title, books.c.author, books.c.type).order_by(func.random()).limit(limit)
        else:
            sel = select(books.c.id, books.c.title, books.c.author, books.c.type).order_by(text("RANDOM()")).limit(limit)
        rows = conn.execute(sel).fetchall()
        return [tuple(r) for r in rows]

def save_missing_query(user_id: int, query: str):
    with engine.begin() as conn:
        conn.execute(insert(missing_queries).values(user_id=user_id, query=query, created_at=datetime.utcnow().isoformat()))

def list_missing_queries_agg(limit: int=50):
    with engine.connect() as conn:
        stmt = select(missing_queries.c.query, func.count().label("cnt"), func.max(missing_queries.c.created_at).label("last_at")).group_by(missing_queries.c.query).order_by(text("cnt DESC, last_at DESC")).limit(limit)
        rows = conn.execute(stmt).fetchall()
        return [tuple(r) for r in rows]

def clear_missing_queries():
    with engine.begin() as conn:
        conn.execute(delete(missing_queries))

def save_user_upload(user_id: int, type_: str, file_id: str, size: int=0, duration_seconds: int=0):
    with engine.begin() as conn:
        conn.execute(insert(user_uploads).values(user_id=user_id, type=type_, file_id=file_id, size=size, duration_seconds=duration_seconds, created_at=datetime.utcnow().isoformat()))

def list_unseen_uploads(limit: int=50):
    with engine.connect() as conn:
        sel = select(user_uploads.c.id, user_uploads.c.user_id, user_uploads.c.type, user_uploads.c.file_id, user_uploads.c.size, user_uploads.c.duration_seconds, user_uploads.c.created_at).where(func.coalesce(user_uploads.c.is_seen, 0) == 0).order_by(user_uploads.c.created_at.asc()).limit(limit)
        rows = conn.execute(sel).fetchall()
        return [tuple(r) for r in rows]

def mark_all_uploads_seen():
    with engine.begin() as conn:
        conn.execute(update(user_uploads).where(func.coalesce(user_uploads.c.is_seen, 0) == 0).values(is_seen=1))

def file_exists_in_server(file_id: str) -> bool:
    with engine.connect() as conn:
        sel = select(func.count()).select_from(book_parts).where(book_parts.c.file_id == file_id).limit(1)
        c = conn.execute(sel).scalar() or 0
        return bool(c)

def ensure_saved_books_table():
    # tables already created by init_db; keep for API parity
    init_db()

def add_saved_book(user_id: int, book_id: int):
    ensure_saved_books_table()
    with engine.begin() as conn:
        try:
            conn.execute(insert(saved_books).values(user_id=user_id, book_id=book_id, created_at=datetime.utcnow().isoformat()))
        except:
            pass

def list_saved_books(user_id: int, offset: int=0, limit: int=10):
    ensure_saved_books_table()
    with engine.connect() as conn:
        sel = select(books.c.id, books.c.title, books.c.author, books.c.type, func.coalesce(books.c.downloads, 0).label("downloads")).select_from(saved_books.join(books, saved_books.c.book_id == books.c.id)).where(saved_books.c.user_id == user_id).order_by(saved_books.c.created_at.desc()).limit(limit).offset(offset)
        rows = conn.execute(sel).fetchall()
        return [tuple(r) for r in rows]

def is_book_saved(user_id: int, book_id: int) -> bool:
    ensure_saved_books_table()
    with engine.connect() as conn:
        sel = select(func.count()).select_from(saved_books).where(and_(saved_books.c.user_id == user_id, saved_books.c.book_id == book_id)).limit(1)
        c = conn.execute(sel).scalar() or 0
        return bool(c)

def remove_saved_book(user_id: int, book_id: int):
    ensure_saved_books_table()
    with engine.begin() as conn:
        conn.execute(delete(saved_books).where(and_(saved_books.c.user_id == user_id, saved_books.c.book_id == book_id)))

def user_saved_count(user_id: int) -> int:
    ensure_saved_books_table()
    with engine.connect() as conn:
        sel = select(func.count()).select_from(saved_books).where(saved_books.c.user_id == user_id)
        return int(conn.execute(sel).scalar() or 0)

def set_purchase_link(book_id: int, link: str):
    with engine.begin() as conn:
        conn.execute(update(books).where(books.c.id == book_id).values(purchase_link=link))

def clear_purchase_link(book_id: int):
    with engine.begin() as conn:
        conn.execute(update(books).where(books.c.id == book_id).values(purchase_link=None))

def saved_books_count() -> int:
    ensure_saved_books_table()
    with engine.connect() as conn:
        return int(conn.execute(select(func.count()).select_from(saved_books)).scalar() or 0)

def uploads_count() -> int:
    with engine.connect() as conn:
        return int(conn.execute(select(func.count()).select_from(user_uploads)).scalar() or 0)

def missing_queries_count() -> int:
    with engine.connect() as conn:
        return int(conn.execute(select(func.count()).select_from(missing_queries)).scalar() or 0)

def total_downloads() -> int:
    with engine.connect() as conn:
        s = conn.execute(select(func.coalesce(func.sum(books.c.downloads), 0))).scalar() or 0
        return int(s)

def ensure_wishes_table():
    init_db()

def add_wish(user_id: int, text: str):
    ensure_wishes_table()
    with engine.begin() as conn:
        conn.execute(insert(wishes).values(user_id=user_id, text=text, created_at=datetime.utcnow().isoformat()))

def wishes_count() -> int:
    ensure_wishes_table()
    with engine.connect() as conn:
        return int(conn.execute(select(func.count()).select_from(wishes).where(func.coalesce(wishes.c.is_seen, 0) == 0)).scalar() or 0)

def list_wishes(offset: int=0, limit: int=50, only_unseen: bool=True):
    ensure_wishes_table()
    with engine.connect() as conn:
        if only_unseen:
            sel = select(wishes.c.id, wishes.c.user_id, wishes.c.text, wishes.c.created_at).where(func.coalesce(wishes.c.is_seen,0) == 0).order_by(wishes.c.created_at.desc()).limit(limit).offset(offset)
        else:
            sel = select(wishes.c.id, wishes.c.user_id, wishes.c.text, wishes.c.created_at).order_by(wishes.c.created_at.desc()).limit(limit).offset(offset)
        rows = conn.execute(sel).fetchall()
        return [tuple(r) for r in rows]

def mark_wish_seen(wish_id: int):
    ensure_wishes_table()
    with engine.begin() as conn:
        conn.execute(update(wishes).where(wishes.c.id == wish_id).values(is_seen=1))

def list_wishes_agg(limit: int=50, offset: int=0, only_unseen: bool=True):
    ensure_wishes_table()
    with engine.connect() as conn:
        if only_unseen:
            stmt = select(wishes.c.text, func.count().label("cnt")).where(func.coalesce(wishes.c.is_seen,0) == 0).group_by(wishes.c.text).order_by(text("cnt DESC")).limit(limit).offset(offset)
        else:
            stmt = select(wishes.c.text, func.count().label("cnt")).group_by(wishes.c.text).order_by(text("cnt DESC")).limit(limit).offset(offset)
        rows = conn.execute(stmt).fetchall()
        return [tuple(r) for r in rows]

# Initialize DB at import time (optional)
try:
    init_db()
except Exception:
    # don't crash import if DB not reachable at this moment
    pass
