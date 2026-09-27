import { describe, expect, it } from "vitest";
import {
  buildCreateViewSql,
  extractCreateViewQuery,
  selectQueryForReferenceResolution,
} from "../createViewSql";

describe("CREATE VIEW SQL composition", () => {
  it("quotes a main-schema view name and retains a single SELECT", () => {
    expect(buildCreateViewSql('Quarterly " rates', "SELECT 1 AS value")).toBe(
      'CREATE VIEW "main"."Quarterly "" rates" AS SELECT 1 AS value;'
    );
  });

  it("accepts a WITH query and places the terminator before a trailing comment", () => {
    expect(buildCreateViewSql("summary", "WITH q AS (SELECT 1) SELECT * FROM q -- result")).toBe(
      'CREATE VIEW "main"."summary" AS WITH q AS (SELECT 1) SELECT * FROM q; -- result'
    );
  });

  it("accepts one existing terminator and ignores comments after it", () => {
    expect(buildCreateViewSql("summary", "SELECT ';' AS marker; /* current query */")).toBe(
      'CREATE VIEW "main"."summary" AS SELECT \';\' AS marker;'
    );
  });

  it("rejects empty names, non-query SQL, batches, and unterminated syntax", () => {
    expect(() => buildCreateViewSql("  ", "SELECT 1")).toThrow("Enter a view name.");
    expect(() => buildCreateViewSql("x", "DELETE FROM values")).toThrow(
      "A view query must be one SELECT or WITH statement."
    );
    expect(() => buildCreateViewSql("x", "SELECT 1; DROP TABLE values")).toThrow(
      "A view query must be one SELECT or WITH statement."
    );
    expect(() => buildCreateViewSql("x", "SELECT 'unfinished")).toThrow(
      "A view query must be one SELECT or WITH statement."
    );
  });
});

describe("CREATE VIEW query extraction", () => {
  it("finds AS structurally around comments, strings, quoted identifiers, and columns", () => {
    const sql = `
      /* CREATE VIEW ignored AS ignored */
      CREATE OR REPLACE TEMP VIEW "main"."named AS view" ("AS", marker) AS
      WITH source AS (
        SELECT 'AS; still a string' AS marker FROM "04_gold"."rates"
      )
      SELECT * FROM source;
    `;
    expect(extractCreateViewQuery(sql)?.trimStart()).toMatch(/^WITH source AS/);
    expect(selectQueryForReferenceResolution(sql)?.trimStart()).toMatch(/^WITH source AS/);
  });

  it("rejects statement tails even when the first statement is a valid view", () => {
    expect(
      extractCreateViewQuery(
        'CREATE VIEW "main"."safe" AS SELECT * FROM "04_gold"."rates"; DROP TABLE safe'
      )
    ).toBeNull();
  });

  it("does not let a backslash mask a statement separator in the strict view builder", () => {
    expect(() => buildCreateViewSql("safe", "SELECT 'a\\'; DROP TABLE safe; -- '\n")).toThrow(
      "A view query must be one SELECT or WITH statement."
    );
  });

  it("keeps semicolons opaque in escaped and dollar-quoted strings", () => {
    expect(selectQueryForReferenceResolution("SELECT E'a\\';still string' AS a")?.trim()).toBe(
      "SELECT E'a\\';still string' AS a"
    );
    expect(selectQueryForReferenceResolution("SELECT $$one;two$$ AS a")?.trim()).toBe(
      "SELECT $$one;two$$ AS a"
    );
  });
});
