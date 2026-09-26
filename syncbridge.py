#!/usr/bin/env python3
"""SyncBridge: local-first sync server with persistent SQLite storage."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


@dataclass(frozen=True)
class ChangeEvent:
    table: str
    operation: str
    key: dict[str, Any]
    data: dict[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    occurred_at: float = field(default_factory=time.time)
    user_id: str = ""


class MemoryTarget:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, dict[str, Any]]] = {}
        self.lock = threading.Lock()

    def apply(self, event: ChangeEvent) -> bool:
        table = self.rows.setdefault(event.table, {})
        key = json.dumps(event.key, sort_keys=True)
        with self.lock:
            if event.operation in {"upsert", "insert", "update"}:
                table[key] = {**event.key, **event.data}
            elif event.operation == "delete":
                table.pop(key, None)
            else:
                raise ValueError(f"unsupported operation: {event.operation}")
        return True


class SQLiteStore:
    """Durable store. Its tables map directly to a future PostgreSQL schema."""

    def __init__(self, path: str = "syncbridge.db") -> None:
        self.path = path
        self.lock = threading.Lock()
        self.sessions: dict[str, str] = {}
        self.conn = sqlite3.connect(path, timeout=10, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=10000")
        self.conn.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS users (
          id TEXT PRIMARY KEY, email TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL,
          created_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS events (
          sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL,
          user_id TEXT NOT NULL, table_name TEXT NOT NULL, operation TEXT NOT NULL,
          key_json TEXT NOT NULL, data_json TEXT NOT NULL, occurred_at REAL NOT NULL,
          UNIQUE(user_id, event_id)
        );
        CREATE TABLE IF NOT EXISTS rows (
          user_id TEXT NOT NULL, table_name TEXT NOT NULL, key_json TEXT NOT NULL,
          data_json TEXT NOT NULL, updated_at REAL NOT NULL,
          PRIMARY KEY(user_id, table_name, key_json)
        );
        """)
        self.conn.commit()

    @staticmethod
    def _hash(password: str, salt: bytes | None = None) -> str:
        salt = salt or secrets.token_bytes(16)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 180_000)
        return f"{salt.hex()}${digest.hex()}"

    @classmethod
    def _valid_password(cls, password: str, encoded: str) -> bool:
        salt, expected = encoded.split("$", 1)
        actual = cls._hash(password, bytes.fromhex(salt)).split("$", 1)[1]
        return hmac.compare_digest(actual, expected)

    def register(self, email: str, password: str) -> str:
        if len(password) < 8 or "@" not in email:
            raise ValueError("email is invalid or password is shorter than 8 characters")
        user_id = str(uuid.uuid4())
        try:
            with self.lock:
                self.conn.execute("INSERT INTO users VALUES (?,?,?,?)", (user_id, email.lower(), self._hash(password), time.time()))
                self.conn.commit()
        except sqlite3.IntegrityError as error:
            raise ValueError("email already exists") from error
        return self._new_session(user_id)

    def login(self, email: str, password: str) -> str:
        row = self.conn.execute("SELECT id,password_hash FROM users WHERE email=?", (email.lower(),)).fetchone()
        if not row or not self._valid_password(password, row["password_hash"]):
            raise ValueError("invalid email or password")
        return self._new_session(row["id"])

    def _new_session(self, user_id: str) -> str:
        token = secrets.token_urlsafe(32)
        self.sessions[token] = user_id
        return token

    def user_for_token(self, token: str | None) -> str | None:
        return self.sessions.get(token or "")

    def apply(self, event: ChangeEvent) -> bool:
        key_json = json.dumps(event.key, sort_keys=True, separators=(",", ":"))
        data_json = json.dumps({**event.key, **event.data}, ensure_ascii=False)
        with self.lock:
            try:
                self.conn.execute("INSERT INTO events(event_id,user_id,table_name,operation,key_json,data_json,occurred_at) VALUES(?,?,?,?,?,?,?)", (event.event_id, event.user_id, event.table, event.operation, key_json, event.data and json.dumps(event.data, ensure_ascii=False) or "{}", event.occurred_at))
            except sqlite3.IntegrityError:
                return False
            if event.operation in {"upsert", "insert", "update"}:
                self.conn.execute("INSERT OR REPLACE INTO rows VALUES(?,?,?,?,?)", (event.user_id, event.table, key_json, data_json, event.occurred_at))
            elif event.operation == "delete":
                self.conn.execute("DELETE FROM rows WHERE user_id=? AND table_name=? AND key_json=?", (event.user_id, event.table, key_json))
            else:
                self.conn.rollback()
                raise ValueError(f"unsupported operation: {event.operation}")
            self.conn.commit()
        return True

    def changes_since(self, user_id: str, sequence: int) -> list[dict[str, Any]]:
        rows = self.conn.execute("SELECT * FROM events WHERE user_id=? AND sequence>? ORDER BY sequence", (user_id, sequence)).fetchall()
        return [{"sequence": r["sequence"], "event_id": r["event_id"], "user_id": user_id, "table": r["table_name"], "operation": r["operation"], "key": json.loads(r["key_json"]), "data": json.loads(r["data_json"]), "occurred_at": r["occurred_at"]} for r in rows]


class SyncEngine:
    def __init__(self, target: Any, max_retries: int = 3) -> None:
        self.target, self.max_retries = target, max_retries
        self.processed: set[tuple[str, str]] = set()
        self.failed: list[dict[str, Any]] = []
        self.change_log: list[dict[str, Any]] = []
        self.next_sequence, self.lock = 1, threading.Lock()
        self.stats = {"received": 0, "applied": 0, "duplicates": 0, "failed": 0}

    def submit(self, event: ChangeEvent) -> bool:
        identity = (event.user_id, event.event_id)
        with self.lock:
            self.stats["received"] += 1
            if identity in self.processed:
                self.stats["duplicates"] += 1
                return False
        for attempt in range(1, self.max_retries + 1):
            try:
                applied = self.target.apply(event)
                with self.lock:
                    self.processed.add(identity)
                    if applied:
                        self.stats["applied"] += 1
                        self.change_log.append({"sequence": self.next_sequence, **asdict(event)})
                        self.next_sequence += 1
                return applied
            except Exception as error:
                if attempt == self.max_retries:
                    with self.lock:
                        self.stats["failed"] += 1
                        self.failed.append({"event": asdict(event), "error": str(error)})
                    return False
                time.sleep(0.01 * attempt)
        return False

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {"stats": dict(self.stats), "failed": list(self.failed[-20:])}

    def changes_since(self, sequence: int) -> list[dict[str, Any]]:
        with self.lock:
            return [change for change in self.change_log if change["sequence"] > sequence]


db: SQLiteStore | None = None
engine = SyncEngine(MemoryTarget())


class Handler(BaseHTTPRequestHandler):
    def _json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict[str, Any]:
        size = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(size))

    def _user(self) -> str | None:
        return db.user_for_token(self.headers.get("Authorization", "").removeprefix("Bearer ").strip()) if db else None

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/health":
            self._json(200, {"ok": True, "storage": "sqlite"})
        elif path == "/status":
            self._json(200, engine.snapshot())
        elif path == "/changes":
            user = self._user()
            if not user:
                self._json(401, {"error": "login required"})
                return
            try:
                since = int(parse_qs(urlparse(self.path).query).get("since", [0])[0])
            except ValueError:
                self._json(400, {"error": "since must be an integer"})
                return
            self._json(200, {"changes": db.changes_since(user, since)})
        elif self.path == "/":
            body = DASHBOARD.encode()
            self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
        elif self.path == "/app":
            body = Path(__file__).with_name("indexeddb.html").read_bytes()
            self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            payload = self._body()
            if path in {"/auth/register", "/auth/login"}:
                token = db.register(payload["email"], payload["password"]) if path.endswith("register") else db.login(payload["email"], payload["password"])
                self._json(200, {"token": token, "user_id": db.user_for_token(token)}); return
            user = self._user()
            if path != "/events" or not user:
                self._json(401 if not user else 404, {"error": "login required" if not user else "not found"}); return
            event = ChangeEvent(table=payload["table"], operation=payload["operation"], key=payload["key"], data=payload.get("data", {}), event_id=payload.get("event_id", str(uuid.uuid4())), user_id=user)
            self._json(202, {"accepted": True, "applied": engine.submit(event), "event_id": event.event_id})
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            self._json(400, {"error": str(error)})

    def log_message(self, *_: Any) -> None:
        return


DASHBOARD = """<!doctype html><meta charset='utf-8'><title>SyncBridge</title><style>body{font:16px system-ui;max-width:760px;margin:50px auto;padding:0 20px;background:#101827;color:#e8eef8}h1{font-size:42px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.card{background:#182438;border:1px solid #2d405d;border-radius:14px;padding:16px}.n{font-size:28px;color:#70e1b2}pre{background:#0b1220;padding:18px;overflow:auto}</style><h1>SyncBridge</h1><div class='grid' id='cards'></div><h2>آخر الأخطاء</h2><pre id='errors'>جار التحميل...</pre><script>async function refresh(){const d=await fetch('/status').then(r=>r.json()),s=d.stats;cards.innerHTML=Object.entries(s).map(([k,v])=>`<div class='card'>${k}<div class='n'>${v}</div></div>`).join('');errors.textContent=JSON.stringify(d.failed,null,2)||'لا توجد أخطاء'}refresh();setInterval(refresh,1500)</script>"""


if __name__ == "__main__":
    db = SQLiteStore(os.getenv("SYNCBRIDGE_DB", "syncbridge.db"))
    engine = SyncEngine(db)
    server = ThreadingHTTPServer(("0.0.0.0", 8080), Handler)
    print("SyncBridge listening on http://localhost:8080")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
