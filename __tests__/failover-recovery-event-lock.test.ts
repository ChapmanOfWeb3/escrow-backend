import Database from "better-sqlite3";
import { setDb, runMigrations } from "../src/indexer/db.js";
import {
  initializeNodeHealthTables,
  withNodeEventLock,
  recordNodeFailure,
} from "../src/indexer/failover-recovery.js";

describe("FailoverRecovery – concurrent event lock", () => {
  let testDb: Database.Database;

  beforeAll(() => {
    testDb = new Database(":memory:");
    setDb(testDb);
  });

  afterAll(() => {
    testDb.close();
  });

  beforeEach(() => {
    testDb.exec("PRAGMA foreign_keys = OFF");
    testDb.exec("DROP TABLE IF EXISTS node_failure_events");
    testDb.exec("DROP TABLE IF EXISTS rpc_node_health");
    testDb.exec("DROP TABLE IF EXISTS failover_state");
    testDb.exec("PRAGMA foreign_keys = ON");
    runMigrations();
    initializeNodeHealthTables();
  });

  it("serializes concurrent event notifications so no duplicate entries are created", async () => {
    const nodeUrl = "https://rpc.example.com";

    const notifications = Array.from({ length: 10 }, (_, i) =>
      withNodeEventLock(nodeUrl, () =>
        recordNodeFailure(nodeUrl, `timeout-${i}`)
      )
    );

    await Promise.all(notifications);

    const events = testDb
      .prepare("SELECT retry_count FROM node_failure_events WHERE node_url = ? ORDER BY retry_count")
      .all(nodeUrl) as { retry_count: number }[];

    const retryCounts = events.map((e) => e.retry_count);
    const uniqueRetryCounts = new Set(retryCounts);

    expect(events).toHaveLength(10);
    expect(uniqueRetryCounts.size).toBe(10);
    expect(retryCounts).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });
});
