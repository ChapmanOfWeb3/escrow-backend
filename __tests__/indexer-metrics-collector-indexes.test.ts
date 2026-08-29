import Database from "better-sqlite3";
import { setDb, runMigrations, closeDb, insertEvent } from "../src/indexer/db.js";
import {
  INDEXER_METRICS_INDEXES,
  INDEXER_METRICS_QUERIES,
  collectIndexerMetrics,
  explainIndexerMetricsQueryPlan,
  metricsQueryPlanUsesIndex,
  metricsQueryPlanUsesTempBTree,
  verifyIndexerMetricsIndexes,
} from "../src/indexer/indexer_metrics_collector.js";

describe("indexer_metrics_collector – SQLite index structures (#335)", () => {
  let testDb: Database.Database;

  beforeEach(() => {
    testDb = new Database(":memory:");
    setDb(testDb);
    runMigrations();
    // Give the planner rows to work with so plans reflect real lookups.
    for (let ledger = 1; ledger <= 40; ledger++) {
      insertEvent(
        `contract-${ledger % 4}`,
        ["initialized", "funded", "approved"][ledger % 3],
        ledger,
        1_700_000_000 + ledger,
        JSON.stringify({ ledger }),
      );
    }
    testDb
      .prepare("INSERT INTO monitored_contracts (contract_id, active) VALUES (?, 1)")
      .run("contract-0");
  });

  afterEach(() => {
    closeDb();
  });

  function indexNames(): string[] {
    return (
      testDb
        .prepare("SELECT name FROM sqlite_master WHERE type = 'index'")
        .all() as Array<{ name: string }>
    ).map((row) => row.name);
  }

  describe("migrations", () => {
    it("creates every index the collector's lookups depend on", () => {
      const names = indexNames();
      for (const indexName of Object.values(INDEXER_METRICS_INDEXES)) {
        expect(names).toContain(indexName);
      }
    });

    it("records the aggregation index migration as applied", () => {
      const versions = (
        testDb
          .prepare("SELECT version FROM schema_migrations ORDER BY version")
          .all() as Array<{ version: number }>
      ).map((row) => row.version);

      expect(versions).toContain(6);
    });

    it("is idempotent when migrations run twice", () => {
      runMigrations();

      const matching = indexNames().filter(
        (name) => name === INDEXER_METRICS_INDEXES.eventsByType,
      );
      expect(matching).toHaveLength(1);
    });

    it("verifyIndexerMetricsIndexes reports a healthy schema", () => {
      const report = verifyIndexerMetricsIndexes(testDb);

      expect(report.valid).toBe(true);
      expect(report.missing).toEqual([]);
      expect(report.present).toEqual(
        expect.arrayContaining(Object.values(INDEXER_METRICS_INDEXES)),
      );
    });

    it("verifyIndexerMetricsIndexes reports a dropped index", () => {
      testDb.exec(`DROP INDEX ${INDEXER_METRICS_INDEXES.eventsByType}`);

      const report = verifyIndexerMetricsIndexes(testDb);

      expect(report.valid).toBe(false);
      expect(report.missing).toEqual([INDEXER_METRICS_INDEXES.eventsByType]);
    });
  });

  describe("EXPLAIN QUERY PLAN – indexes are used for lookups", () => {
    it("uses idx_events_event_type for the events-by-type aggregation", () => {
      const plan = explainIndexerMetricsQueryPlan(
        INDEXER_METRICS_QUERIES.eventsByType,
        testDb,
      );

      expect(
        metricsQueryPlanUsesIndex(plan, INDEXER_METRICS_INDEXES.eventsByType),
      ).toBe(true);
    });

    it("aggregates without a temporary B-tree", () => {
      const plan = explainIndexerMetricsQueryPlan(
        INDEXER_METRICS_QUERIES.eventsByType,
        testDb,
      );

      expect(metricsQueryPlanUsesTempBTree(plan)).toBe(false);
    });

    it("falls back to a temporary B-tree without the index, proving it is load-bearing", () => {
      testDb.exec(`DROP INDEX ${INDEXER_METRICS_INDEXES.eventsByType}`);

      const plan = explainIndexerMetricsQueryPlan(
        INDEXER_METRICS_QUERIES.eventsByType,
        testDb,
      );

      expect(metricsQueryPlanUsesTempBTree(plan)).toBe(true);
      expect(
        metricsQueryPlanUsesIndex(plan, INDEXER_METRICS_INDEXES.eventsByType),
      ).toBe(false);
    });

    it("uses idx_events_created_at for the newest-event lookup", () => {
      const plan = explainIndexerMetricsQueryPlan(
        INDEXER_METRICS_QUERIES.lastEventAt,
        testDb,
      );

      expect(
        metricsQueryPlanUsesIndex(plan, INDEXER_METRICS_INDEXES.lastEventAt),
      ).toBe(true);
    });

    it("uses idx_monitored_contracts_active for the active-contract count", () => {
      const plan = explainIndexerMetricsQueryPlan(
        INDEXER_METRICS_QUERIES.activeContracts,
        testDb,
      );

      expect(
        metricsQueryPlanUsesIndex(plan, INDEXER_METRICS_INDEXES.activeContracts),
      ).toBe(true);
    });

    it("answers the total-event count from a covering index, never a table scan", () => {
      const plan = explainIndexerMetricsQueryPlan(
        INDEXER_METRICS_QUERIES.totalEvents,
        testDb,
      );

      const details = plan.map((row) => String((row as any).detail));
      expect(details.some((d) => d.includes("COVERING INDEX"))).toBe(true);
      // A bare "SCAN events" with no index would mean a full table read.
      expect(details.some((d) => /SCAN events$/.test(d.trim()))).toBe(false);
    });

    it("resolves the last-ledger lookup through the indexer_state primary key", () => {
      const plan = explainIndexerMetricsQueryPlan(
        INDEXER_METRICS_QUERIES.lastLedger,
        testDb,
      );

      const details = plan.map((row) => String((row as any).detail));
      expect(details.join(" ")).toContain("SEARCH indexer_state");
      expect(details.join(" ")).toContain("sqlite_autoindex_indexer_state_1");
    });

    it("answers the subscription count from a covering index", () => {
      const plan = explainIndexerMetricsQueryPlan(
        INDEXER_METRICS_QUERIES.subscriptions,
        testDb,
      );

      const details = plan.map((row) => String((row as any).detail));
      expect(details.some((d) => d.includes("COVERING INDEX"))).toBe(true);
    });

    it("every collector query is planned without a temporary B-tree", () => {
      for (const sql of Object.values(INDEXER_METRICS_QUERIES)) {
        const plan = explainIndexerMetricsQueryPlan(sql, testDb);
        expect(metricsQueryPlanUsesTempBTree(plan)).toBe(false);
      }
    });
  });

  describe("collected results are unchanged by the index work", () => {
    it("returns the same aggregation the un-indexed query would", () => {
      const metrics = collectIndexerMetrics(testDb);

      const expected = testDb
        .prepare(
          "SELECT event_type, COUNT(*) as count FROM events GROUP BY event_type",
        )
        .all() as Array<{ event_type: string; count: number }>;

      expect(metrics.totalEvents).toBe(40);
      expect(metrics.activeContractsCount).toBe(1);
      for (const row of expected) {
        expect(metrics.eventsByType[row.event_type]).toBe(row.count);
      }
    });
  });
});
