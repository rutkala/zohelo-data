import { describe, expect, it } from "vitest";
import { dbtDocsUrl, isDbtDocsHtml, populateDbtDocs } from "../dbtDocs";

const template =
  '<!doctype html><title>dbt Docs</title><script>var n={manifest:"MANIFEST.JSON INLINE DATA",catalog:"CATALOG.JSON INLINE DATA"}</script>';
describe("dbt release documentation", () => {
  it("keeps the template URL beneath the deployment base", () => {
    expect(dbtDocsUrl("/", "https://data.zohelo.com")).toBe(
      "https://data.zohelo.com/docs/viewer-template.html"
    );
    expect(dbtDocsUrl("/zohelo-data/", "https://data.zohelo.com")).toBe(
      "https://data.zohelo.com/zohelo-data/docs/viewer-template.html"
    );
  });
  it("rejects the portal fallback and incomplete or repeated static placeholders", () => {
    expect(isDbtDocsHtml(template)).toBe(true);
    expect(isDbtDocsHtml("<title>Duck-UI</title>")).toBe(false);
    expect(() => populateDbtDocs("<title>Duck-UI</title>", {}, {})).toThrow();
    expect(() =>
      populateDbtDocs(template.replace('"CATALOG.JSON INLINE DATA"', "{}"), {}, {})
    ).toThrow();
    expect(() => populateDbtDocs(template + '"MANIFEST.JSON INLINE DATA"', {}, {})).toThrow();
  });
  it("preserves SQL dollar syntax and prevents artifact text from closing a script", () => {
    const hostile = "</script><script>alert(1)</script> $& $` $'  ";
    const html = populateDbtDocs(template, { description: hostile }, { nodes: {} });
    expect(html.match(/<script>/g)).toHaveLength(1);
    expect(html.match(/<\/script>/g)).toHaveLength(1);
    const serialized = html.slice(html.indexOf("manifest:") + 9, html.indexOf(",catalog:"));
    expect(JSON.parse(serialized)).toEqual({ description: hostile });
    expect(html).not.toContain("INLINE DATA");
  });
});
