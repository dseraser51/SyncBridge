/**
 * SyncBridge browser SDK.
 * Copyright (c) 2026 عاطف بالهادي (Atif Alhadi)
 */

const DEFAULT_INTERVAL = 3000;

function id() {
  return globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function requestUrl(base, path) {
  return `${base.replace(/\/$/, "")}${path}`;
}

export async function register(serverUrl, email, password) {
  return authRequest(serverUrl, "/auth/register", email, password);
}

export async function login(serverUrl, email, password) {
  return authRequest(serverUrl, "/auth/login", email, password);
}

async function authRequest(serverUrl, path, email, password) {
  const response = await fetch(requestUrl(serverUrl, path), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password })
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || "Authentication failed");
  return data;
}

export function createSyncClient({ serverUrl = "", token, userId, databaseName = "syncbridge", pollInterval = DEFAULT_INTERVAL, onStatus = () => {} }) {
  if (!token || !userId) throw new Error("token and userId are required");
  const dbName = `${databaseName}-${userId}`;
  let dbPromise;
  let timer;
  let syncing = false;
  let stopped = false;
  let cursor = 0;

  function open() {
    if (dbPromise) return dbPromise;
    dbPromise = new Promise((resolve, reject) => {
      const request = indexedDB.open(dbName, 1);
      request.onupgradeneeded = () => {
        const database = request.result;
        database.createObjectStore("records", { keyPath: "key" });
        database.createObjectStore("outbox", { keyPath: "event_id" });
        database.createObjectStore("meta", { keyPath: "key" });
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    return dbPromise;
  }

  async function transaction(storeNames, mode, action) {
    const database = await open();
    return new Promise((resolve, reject) => {
      const transaction = database.transaction(storeNames, mode);
      let result;
      transaction.oncomplete = () => resolve(result);
      transaction.onerror = () => reject(transaction.error);
      result = action(transaction);
    });
  }

  async function getAll(store) {
    return transaction([store], "readonly", transaction => new Promise((resolve, reject) => {
      const request = transaction.objectStore(store).getAll();
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    }));
  }

  async function put(store, value) {
    return transaction([store], "readwrite", transaction => transaction.objectStore(store).put(value));
  }

  async function remove(store, key) {
    return transaction([store], "readwrite", transaction => transaction.objectStore(store).delete(key));
  }

  async function get(store, key) {
    return transaction([store], "readonly", transaction => new Promise((resolve, reject) => {
      const request = transaction.objectStore(store).get(key);
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    }));
  }

  async function push() {
    for (const event of await getAll("outbox")) {
      const response = await fetch(requestUrl(serverUrl, "/events"), {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify(event)
      });
      if (!response.ok) throw new Error(`Push failed: ${response.status}`);
      await remove("outbox", event.event_id);
    }
  }

  async function pull() {
    const response = await fetch(requestUrl(serverUrl, `/changes?since=${cursor}`), {
      headers: { Authorization: `Bearer ${token}` }
    });
    if (!response.ok) throw new Error(`Pull failed: ${response.status}`);
    const { changes } = await response.json();
    for (const event of changes) {
      const key = `${event.table}:${JSON.stringify(event.key)}`;
      const local = await get("records", key);
      if (!local || (event.data.updated_at || 0) >= (local.value.updated_at || 0)) {
        if (event.operation === "delete") await remove("records", key);
        else await put("records", { key, collection: event.table, value: { ...event.key, ...event.data } });
      }
      cursor = Math.max(cursor, event.sequence);
      await put("meta", { key: "cursor", value: cursor });
    }
  }

  async function sync() {
    if (syncing || stopped) return;
    syncing = true;
    try {
      const savedCursor = await get("meta", "cursor");
      cursor = savedCursor?.value || cursor;
      await push();
      await pull();
      onStatus({ state: "synced", pending: (await getAll("outbox")).length });
    } catch (error) {
      onStatus({ state: "offline", error, pending: (await getAll("outbox")).length });
    } finally {
      syncing = false;
    }
  }

  function collection(name) {
    const keyFor = value => `${name}:${JSON.stringify({ id: value.id })}`;
    return {
      async put(value) {
        if (!value?.id) throw new Error("Records require an id");
        const now = Date.now() / 1000;
        const next = { ...value, updated_at: value.updated_at || now };
        await put("records", { key: keyFor(value), collection: name, value: next });
        await put("outbox", { event_id: id(), table: name, operation: "upsert", key: { id: value.id }, data: next });
        await sync();
        return next;
      },
      async delete(recordId) {
        await remove("records", keyFor({ id: recordId }));
        await put("outbox", { event_id: id(), table: name, operation: "delete", key: { id: recordId }, data: {} });
        await sync();
      },
      async get(recordId) {
        const record = await get("records", keyFor({ id: recordId }));
        return record?.value;
      },
      async toArray() {
        return (await getAll("records")).filter(record => record.collection === name).map(record => record.value);
      }
    };
  }

  return {
    collection,
    sync,
    start() { stopped = false; sync(); timer = setInterval(sync, pollInterval); },
    stop() { stopped = true; clearInterval(timer); },
    close() { stopped = true; clearInterval(timer); dbPromise?.then(database => database.close()); }
  };
}
