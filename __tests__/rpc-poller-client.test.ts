import { jest } from "@jest/globals";
import Database from "better-sqlite3";

const mockGetLatestLedger = jest.fn<() => Promise<{ sequence: number }>>();
const mockGetEvents = jest.fn<() => Promise<{ events: unknown[] }>>();

jest.unstable_mockModule("@stellar/stellar-sdk/rpc", () => ({
  Server: class MockServer {
    getLatestLedger = mockGetLatestLedger;
    getEvents = mockGetEvents;
  },
}));

jest.unstable_mockModule("@stellar/stellar-sdk", () => ({
  scValToNative: (val: unknown) => val,
}));

const {
  setDb,
  runMigrations,
  registerContract,
  getLastIndexedLedger,
  getEventsByContract,
} = await import("../src/indexer/db.js");
const { pollEvents } = await import("../src/indexer/poller.js");

const CONTRACT_ID = "CONTRACT-MOCK-RPC";

function makeMockEvent(eventType: string, ledger: number, data: Record<string, unknown>) {
  return {
    contractId: { contractId: () => CONTRACT_ID },
    topic: [eventType],
    ledger,
    ledgerClosedAt: new Date(1_700_000_000_000 + ledger * 1000).toISOString(),
    value: data,
  };
}

describe("rpc_poller_client – in-memory mock integration", () => {
  let testDb: Database.Database;

  beforeAll(() => {
    testDb = new Database(":memory:");
    setDb(testDb);
  });

  afterAll(() => {
    testDb.close();
  });

  beforeEach(() => {
    testDb.exec("DROP TABLE IF EXISTS events");
    testDb.exec("DROP TABLE IF EXISTS indexer_state");
    testDb.exec("DROP TABLE IF EXISTS monitored_contracts");
    testDb.exec("DROP TABLE IF EXISTS schema_migrations");
    testDb.exec("DROP TABLE IF EXISTS webhook_subscriptions");
    runMigrations();
    registerContract(CONTRACT_ID, "mock");
    mockGetLatestLedger.mockReset();
    mockGetEvents.mockReset();
  });

  it("writes simulated RPC events to the database schema", async () => {
    mockGetLatestLedger.mockResolvedValue({ sequence: 105 });
    mockGetEvents.mockResolvedValue({
      events: [
        makeMockEvent("initialized", 101, { client: "GCLIENT" }),
        makeMockEvent("funded", 102, { client: "GCLIENT", amount: "1000" }),
        makeMockEvent("delivered", 103, { freelancer: "GFREELANCER" }),
      ],
    });

    await pollEvents();

    const stored = getEventsByContract(CONTRACT_ID, 1, 10);
    expect(stored.total).toBe(3);
    expect(stored.events.map((e) => e.event_type).sort()).toEqual(
      ["delivered", "funded", "initialized"].sort()
    );
    expect(getLastIndexedLedger()).toBe(105);
  });

  it("advances the ledger pointer even when no matching events are returned", async () => {
    mockGetLatestLedger.mockResolvedValue({ sequence: 50 });
    mockGetEvents.mockResolvedValue({ events: [] });

    await pollEvents();

    expect(getLastIndexedLedger()).toBe(50);
    expect(getEventsByContract(CONTRACT_ID, 1, 10).total).toBe(0);
  });

  it("does not re-poll or duplicate events when the ledger hasn't advanced", async () => {
    mockGetLatestLedger.mockResolvedValue({ sequence: 60 });
    mockGetEvents.mockResolvedValue({
      events: [makeMockEvent("initialized", 55, { client: "GCLIENT" })],
    });

    await pollEvents();
    expect(getEventsByContract(CONTRACT_ID, 1, 10).total).toBe(1);
    expect(mockGetEvents).toHaveBeenCalledTimes(1);

    // Ledger hasn't moved past the last indexed value – should short-circuit
    // before calling getEvents again.
    await pollEvents();
    expect(mockGetEvents).toHaveBeenCalledTimes(1);
    expect(getEventsByContract(CONTRACT_ID, 1, 10).total).toBe(1);
  });
});
