/** The template contains code only. Its data always comes from the connected release. */
export function dbtDocsUrl(baseUrl: string, origin: string): string {
  return new URL("docs/viewer-template.html", new URL(baseUrl, origin)).toString();
}

export function isDbtDocsHtml(document: string): boolean {
  return /<title\b[^>]*>\s*dbt\s+docs\s*<\/title>/i.test(document);
}

const manifestMarker = '"MANIFEST.JSON INLINE DATA"';
const catalogMarker = '"CATALOG.JSON INLINE DATA"';
const serialize = (value: unknown) =>
  JSON.stringify(value)
    .replace(/</g, "\\u003c")
    .replace(/\u2028/g, "\\u2028")
    .replace(/\u2029/g, "\\u2029");

/** Use dbt's own static-document substitutions, with data escaped as JavaScript JSON. */
export function populateDbtDocs(template: string, manifest: unknown, catalog: unknown): string {
  if (
    !isDbtDocsHtml(template) ||
    template.split(manifestMarker).length !== 2 ||
    template.split(catalogMarker).length !== 2
  ) {
    throw new Error("The catalogue viewer template is unavailable or incompatible.");
  }
  // Callbacks preserve dollar signs in source SQL, descriptions and compiled code.
  return template
    .replace(manifestMarker, () => serialize(manifest))
    .replace(catalogMarker, () => serialize(catalog));
}
