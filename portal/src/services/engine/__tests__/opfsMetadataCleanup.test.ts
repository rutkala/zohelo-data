import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionDefinition } from "../types";
import { opfsDriver } from "../drivers/opfsDriver";
import { cleanupOPFSConnection, testOPFSConnection } from "@/services/duckdb/opfsConnection";

vi.mock("@/services/duckdb/opfsConnection", () => ({
  testOPFSConnection: vi.fn(), cleanupOPFSConnection: vi.fn().mockResolvedValue(undefined),
}));

const definition = { id: "opfs", name: "Workspace", origin: "APP",
  config: { kind: "opfs", path: "metadata.db" } } as ConnectionDefinition<"opfs">;

beforeEach(() => vi.clearAllMocks());

describe("OPFS Landing metadata lifecycle", () => {
  it("removes only exactly marked tables before exposing a reopened engine without Drive auth", async () => {
    const persisted = new Map([["old_files", "zohelo-native-landing-file-metadata:v1"],
      ["user_files", "a user table"]]);
    const operations: string[] = [];
    const connection = { query: vi.fn(async (sql: string) => {
      operations.push(sql);
      if (sql.includes("duckdb_tables()")) return { toArray: () => [...persisted]
        .filter(([, comment]) => comment === "zohelo-native-landing-file-metadata:v1")
        .map(([table_name]) => ({ table_name })) };
      if (sql.startsWith("DROP TABLE")) persisted.delete("old_files");
      return { toArray: () => [] };
    }) };
    const db = {};
    vi.mocked(testOPFSConnection).mockResolvedValue({ db, connection } as never);
    const first = await opfsDriver.connect(definition);
    expect(operations[0]).toContain("duckdb_tables()");
    expect(operations[1]).toBe('DROP TABLE "01_landing"."old_files";');
    expect(persisted.has("old_files")).toBe(false);
    expect(persisted.has("user_files")).toBe(true);
    await first.close();
    operations.length = 0;
    await opfsDriver.connect(definition);
    expect(operations).toHaveLength(1);
  });

  it("fails closed when owned-relation cleanup cannot complete", async () => {
    const db = {};
    const connection = { query: vi.fn().mockRejectedValue(new Error("catalog unavailable")) };
    vi.mocked(testOPFSConnection).mockResolvedValue({ db, connection } as never);
    await expect(opfsDriver.connect(definition)).rejects.toThrow("catalog unavailable");
    expect(cleanupOPFSConnection).toHaveBeenCalledWith(db, connection, "metadata.db");
  });
});
