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

/** Keep scripts external to satisfy CSP without allowing inline script execution. */
export function createDbtDocsDocument(html: string): { url: string; dispose: () => void } {
  const urls: string[] = [];
  try {
    const document = new DOMParser().parseFromString(html, "text/html");
    const scripts = Array.from(document.querySelectorAll("script"));
    if (scripts.length !== 1 || scripts[0].src || !scripts[0].textContent) {
      throw new Error("The catalogue viewer has an unexpected script structure.");
    }
    const script = scripts[0];
    const scriptUrl = URL.createObjectURL(
      new Blob([script.textContent!], { type: "text/javascript" })
    );
    urls.push(scriptUrl);
    script.textContent = "";
    script.src = scriptUrl;
    const url = URL.createObjectURL(
      new Blob(["<!doctype html>" + document.documentElement.outerHTML], { type: "text/html" })
    );
    urls.push(url);
    return { url, dispose: () => urls.forEach((item) => URL.revokeObjectURL(item)) };
  } catch (error) {
    urls.forEach((item) => URL.revokeObjectURL(item));
    throw error;
  }
}
