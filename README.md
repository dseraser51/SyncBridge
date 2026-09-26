# SyncBridge

[![CI](https://github.com/dseraser51/SyncBridge/actions/workflows/ci.yml/badge.svg)](https://github.com/dseraser51/SyncBridge/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Local-first synchronization for IndexedDB applications.** SyncBridge keeps web apps usable offline, queues local changes, and synchronizes them with a central server when connectivity returns.

**Author and original idea:** عاطف بالهادي (Atif Alhadi)

> Experimental `v0.1.0`: useful for prototypes and internal tools. Review the security checklist before production use.

## Why SyncBridge?

Most web apps stop being useful when the network disappears. SyncBridge provides a small browser SDK and a self-hosted server so developers can build offline-capable tasks, inventory, field-service, and PWA applications without writing their own IndexedDB outbox and sync loop.

## Quick Start

```bash
git clone https://github.com/dseraser51/SyncBridge.git
cd SyncBridge
python3 syncbridge.py
```

Open `http://localhost:8080/app`, choose **إنشاء حساب**, and create a test account. Add a task, then open another browser profile and log in with the same account to verify synchronization.

Run the automated tests:

```bash
python3 -m unittest -v test_syncbridge.py
```

## SDK Usage

```javascript
import { createSyncClient, login } from "./sdk/syncbridge-client.js";

const session = await login("https://your-sync-server.example", email, password);
const sync = createSyncClient({
  serverUrl: "https://your-sync-server.example",
  ...session,
  databaseName: "my-app"
});

const tasks = sync.collection("tasks");
await tasks.put({ id: "task-1", title: "Review report", completed: false });
const localTasks = await tasks.toArray();
sync.start();
```

`put()` writes to IndexedDB first and queues an Outbox event. `start()` periodically pushes pending events and pulls changes from the server. Each user receives an isolated IndexedDB database.

See the complete browser example in [`examples/basic.html`](examples/basic.html) and the SDK reference in [`sdk/README.md`](sdk/README.md).

## Docker

```bash
docker compose up --build
```

The server stores development data in a persistent Docker volume. For local development, `SYNCBRIDGE_DB` can point to any SQLite file.

## API

- `POST /auth/register` creates an account and returns `{ token, user_id }`.
- `POST /auth/login` authenticates an account and returns `{ token, user_id }`.
- `POST /events` accepts an authenticated change event.
- `GET /changes?since=0` returns changes for the authenticated user.
- `GET /health` checks server availability.

## Architecture

```text
React / Vue / Vanilla JS
          |
    SyncBridge SDK
          |
 IndexedDB + Outbox
          |
    SyncBridge API
          |
 SQLite (development) -> PostgreSQL adapter (roadmap)
```

## Security Status

Before production deployment, add HTTPS, expiring sessions, rate limiting, password reset, CSRF protection where applicable, structured audit logs, and a PostgreSQL deployment. The included authentication is intentionally minimal for a v0.1 prototype.

## Project Status

Current: persistent SQLite server, account isolation, IndexedDB Outbox, polling sync, browser SDK, and tests.

Planned: WebSocket transport, PostgreSQL adapter, conflict strategies, npm publication, React/Vue examples, and a hosted demo.

See [`ROADMAP.md`](ROADMAP.md) for details.

## Contributing

Read [`CONTRIBUTING.md`](CONTRIBUTING.md). Bug reports and small improvements are welcome through GitHub Issues.

## License and Ownership

MIT License. Copyright (c) 2026 عاطف بالهادي (Atif Alhadi). See [`LICENSE`](LICENSE).

---

## التوثيق العربي

SyncBridge منصة لمزامنة بيانات IndexedDB بين المتصفح وخادم مركزي مع دعم العمل دون اتصال. يحفظ SDK التغييرات محليًا أولًا، ثم يرسلها تلقائيًا عند عودة الشبكة.

شغّل الخادم:

```bash
python3 syncbridge.py
```

ثم افتح `http://localhost:8080/app` وأنشئ حسابًا من زر **إنشاء حساب**. الاختبارات:

```bash
python3 -m unittest -v test_syncbridge.py
```

المشروع في مرحلة تجريبية، وحقوق الفكرة والتطوير محفوظة باسم **عاطف بالهادي**.
