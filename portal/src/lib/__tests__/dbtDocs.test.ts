import { describe, expect, it } from "vitest";
import { dbtDocsUrl, isDbtDocsHtml } from "../dbtDocs";

describe("dbt docs deployment helpers", () => {
  it("keeps the docs URL beneath either a root or repository deployment base", () => {
    expect(dbtDocsUrl("/", "https://data.zohelo.com")).toBe(
      "https://data.zohelo.com/docs/index.html"
    );
    expect(dbtDocsUrl("/zohelo-data/", "https://data.zohelo.com")).toBe(
      "https://data.zohelo.com/zohelo-data/docs/index.html"
    );
  });

  it("accepts dbt's generated document and rejects the portal shell", () => {
    expect(isDbtDocsHtml("<!doctype html><title>dbt Docs</title>")).toBe(true);
    expect(isDbtDocsHtml("<!doctype html><title>Duck-UI</title>")).toBe(false);
  });
});
