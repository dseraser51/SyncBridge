# @syncbridge/client

Browser SDK for local-first IndexedDB synchronization.

## API

```javascript
import { createSyncClient, login, register } from "./syncbridge-client.js";

const session = await login(serverUrl, email, password);
const sync = createSyncClient({ serverUrl, ...session, databaseName: "my-app" });
const notes = sync.collection("notes");

await notes.put({ id: "n-1", body: "Draft offline" });
await notes.get("n-1");
await notes.toArray();
await notes.delete("n-1");

sync.start();
// sync.stop() pauses polling; sync.close() also closes IndexedDB.
```

The SDK requires a server-issued `token` and `user_id`. It uses one IndexedDB database per user and collection name. `onStatus` receives `synced` or `offline` state updates.

This package is currently distributed with the repository. npm publication is planned for a future release.
