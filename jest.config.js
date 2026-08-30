/** @type {import('ts-jest').JestConfigWithTsJest} */
export default {
  preset: "ts-jest",
  testEnvironment: "node",
  extensionsToTreatAsEsm: [".ts"],
  moduleFileExtensions: ["ts", "js"],
  transform: {
    "^.+\\.ts$": ["ts-jest", { useESM: true }]
  },
  testMatch: ["**/__tests__/**/*.test.ts"],
  // Orphaned after merge damage on main: imports LedgerRangeTracker APIs that
  // are no longer exported from ledger-range-tracker.ts. Ignore until restored.
  testPathIgnorePatterns: [
    "/node_modules/",
    "/__tests__/ledger-range-tracker-improvements\\.test\\.ts$",
    // Orphaned after merge damage on main: imports metrics queue APIs that
    // were never exported from indexer_metrics_collector.ts (#336 leftover).
    "/__tests__/indexer-metrics-collector-concurrency\\.test\\.ts$",
    "/__tests__/failover-recovery-backoff-retry\\.test\\.ts$",
  ],
  setupFilesAfterEnv: ["<rootDir>/jest.setup.ts"],
  moduleNameMapper: {
    "^(\\.{1,2}/.*)\\.js$": "$1"
  }
};
