# database.py

import psycopg2
import psycopg2.extras
from flask import g
import os
import sys

def get_db():
    """Application Context 내에서 유일한 DB 연결을 가져옵니다."""
    if 'db' not in g:
        # For production environments like Render, DATABASE_URL is the primary connection string.
        # For local development, the fallback to individual variables is used.
        database_url = os.environ.get('DATABASE_URL')
        if database_url:
            g.db = psycopg2.connect(database_url)
        else:
            # Ensure all required variables are present for local connection
            if not all(os.environ.get(var) for var in ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT']):
                raise ValueError("For local development, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, and DB_PORT must be set.")
            g.db = psycopg2.connect(
                dbname=os.environ.get('DB_NAME'),
                user=os.environ.get('DB_USER'),
                password=os.environ.get('DB_PASSWORD'),
                host=os.environ.get('DB_HOST'),
                port=os.environ.get('DB_PORT')
            )
    return g.db

def get_cursor(db):
    """지정된 DB 연결로부터 DictCursor를 반환합니다."""
    return db.cursor(cursor_factory=psycopg2.extras.DictCursor)

def close_db(exception=None):
    """요청(request)이 끝나면 자동으로 호출되어 DB 연결을 닫습니다."""
    db = g.pop('db', None)
    if db is not None:
        db.close()

def create_standalone_connection():
    """Flask 컨텍스트 없이 독립적인 DB 연결을 생성합니다."""
    database_url = os.environ.get('DATABASE_URL')
    if database_url:
        return psycopg2.connect(database_url)
    else:
        if not all(os.environ.get(var) for var in ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT']):
            raise ValueError("For local development, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, and DB_PORT must be set.")
        return psycopg2.connect(
            dbname=os.environ.get('DB_NAME'),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            host=os.environ.get('DB_HOST'),
            port=os.environ.get('DB_PORT')
        )

def setup_database_standalone():
    """독립 실행형 스크립트에서 테이블을 생성합니다."""
    conn = None
    try:
        print("LOG: [DB Setup] Attempting to connect to the database...")
        conn = create_standalone_connection()
        cursor = get_cursor(conn)
        print("LOG: [DB Setup] Connection successful.")

        print("LOG: [DB Setup] Dropping existing tables (if any)...")
        # cursor.execute("DROP TABLE IF EXISTS subscriptions;")
        cursor.execute("DROP TABLE IF EXISTS contents;")
        print("LOG: [DB Setup] Tables dropped.")

        print("LOG: [DB Setup] Creating 'contents' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS contents (
            content_id TEXT NOT NULL,
            source TEXT NOT NULL,
            content_type TEXT NOT NULL,
            title TEXT NOT NULL,
            status TEXT NOT NULL,
            meta JSONB,
            PRIMARY KEY (content_id, source)
        )""")
        print("LOG: [DB Setup] 'contents' table created or already exists.")

        print("LOG: [DB Setup] Creating 'subscriptions' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL,
            content_id TEXT NOT NULL,
            source TEXT NOT NULL,
            UNIQUE(email, content_id, source)
        )""")
        print("LOG: [DB Setup] 'subscriptions' table created or already exists.")

        # === 🚨 [신규] 통합 보고서 저장을 위한 테이블 생성 ===
        print("LOG: [DB Setup] Creating 'daily_crawler_reports' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_crawler_reports (
            id SERIAL PRIMARY KEY,
            crawler_name TEXT NOT NULL,
            status TEXT NOT NULL,
            report_data JSONB NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )""")
        print("LOG: [DB Setup] 'daily_crawler_reports' table created or already exists.")
        # ================================================

        print("LOG: [DB Setup] Enabling 'pg_trgm' extension...")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

        print("LOG: [DB Setup] Creating GIN index on contents.title...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_contents_title_trgm
            ON contents
            USING gin (title gin_trgm_ops);
        """)
        print("LOG: [DB Setup] 'pg_trgm' setup complete.")

        print("LOG: [DB Setup] Committing changes...")
        conn.commit()
        print("LOG: [DB Setup] Changes committed.")

        cursor.close()
    except psycopg2.Error as e:
        print(f"FATAL: [DB Setup] A database error occurred: {e}", file=sys.stderr)
        # Re-raise the exception to ensure the script exits with a non-zero status code
        raise
    except Exception as e:
        print(f"FATAL: [DB Setup] An unexpected error occurred: {e}", file=sys.stderr)
        raise
    finally:
        if conn:
            conn.close()
            print("LOG: [DB Setup] Connection closed.")
