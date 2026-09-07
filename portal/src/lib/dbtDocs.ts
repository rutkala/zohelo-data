/**
 * Resolve the generated dbt document from Vite's deployment base. GitHub Pages
 * serves the portal below a repository path, while local previews use `/`.
 */
export function dbtDocsUrl(baseUrl: string, origin: string): string {
  return new URL("docs/index.html", new URL(baseUrl, origin)).toString();
}

/** dbt's generated index has a stable document title across supported versions. */
export function isDbtDocsHtml(document: string): boolean {
  return /<title\b[^>]*>\s*dbt\s+docs\s*<\/title>/i.test(document);
}
