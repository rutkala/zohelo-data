import { describe, expect, it } from "vitest";
import {
  SQL_RELATION_DRAG_MIME,
  selectAllFromRelation,
  setSqlRelationDragData,
} from "../sqlTableActions";

describe("SQL table actions", () => {
  it("uses the quoted relation unchanged in generated SELECT SQL", () => {
    expect(selectAllFromRelation('"memory"."analytics"."daily ""rates"""')).toBe(
      'SELECT * FROM "memory"."analytics"."daily ""rates""" LIMIT 100;'
    );
  });

  it("writes the quoted relation to the custom and plain-text drag formats", () => {
    const values = new Map<string, string>();
    const transfer = {
      effectAllowed: "none",
      setData: (format: string, value: string) => values.set(format, value),
    } as unknown as DataTransfer;
    const relation = '"03_silver"."nbp_exchange_rates_table_a"';

    setSqlRelationDragData(transfer, relation);

    expect(transfer.effectAllowed).toBe("copy");
    expect(values.get(SQL_RELATION_DRAG_MIME)).toBe(relation);
    expect(values.get("text/plain")).toBe(relation);
  });
});
