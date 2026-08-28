import Database from "better-sqlite3";
import { setDb, runMigrations } from "../src/indexer/db.js";
import {
  initializeNodeHealthTables,
  ensureNodeHealthIndexes,
} from "../src/indexer/failover-recovery.js";

describe("FailoverRecovery – index-backed lookups", () => {
  let testDb: Database.Database;

  beforeAll(() => {
    testDb = new Database(":memory:");
    setDb(testDb);
    runMigrations();
    initializeNodeHealthTables();
    ensureNodeHealthIndexes();
  });

  afterAll(() => {
    testDb.close();
  });

  it("creates the expected indexes", () => {
    const indexes = testDb
      .prepare("SELECT name FROM sqlite_master WHERE type = 'index'")
      .all() as { name: string }[];
    const names = indexes.map((i) => i.name);

    expect(names).toContain("idx_rpc_node_health_healthy");
    expect(names).toContain("idx_node_failure_events_node_url_created_at");
  });

  it("uses the index for a node_failure_events lookup by node_url", () => {
    const plan = testDb
      .prepare(
        "EXPLAIN QUERY PLAN SELECT * FROM node_failure_events WHERE node_url = ? ORDER BY created_at"
      )
      .all("https://rpc.example.com") as { detail: string }[];

    const usesIndex = plan.some((row) =>
      row.detail.includes("idx_node_failure_events_node_url_created_at")
    );
    expect(usesIndex).toBe(true);
  });

  it("uses the index for a healthy-node lookup", () => {
    const plan = testDb
      .prepare("EXPLAIN QUERY PLAN SELECT * FROM rpc_node_health WHERE is_healthy = 1")
      .all() as { detail: string }[];

    const usesIndex = plan.some((row) =>
      row.detail.includes("idx_rpc_node_health_healthy")
    );
    expect(usesIndex).toBe(true);
  });
});
