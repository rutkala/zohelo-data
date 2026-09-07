import { useEffect, useState } from "react";
import { Layers, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useDuckStore } from "@/store";
import { createDbtDocsDocument, dbtDocsUrl, populateDbtDocs } from "@/lib/dbtDocs";
import { loadReleaseDbtArtifacts, prepareDbtManifest } from "@/services/googleDrive/dbtCatalog";
import type { ReleaseCatalogResolution } from "@/services/googleDrive/types";

let templatePromise: Promise<string> | undefined;
function loadTemplate(): Promise<string> {
  templatePromise ??= fetch(dbtDocsUrl(import.meta.env.BASE_URL, window.location.origin))
    .then(async (response) => {
      if (!response.ok)
        throw new Error("The catalogue viewer could not be loaded. Please try again.");
      return response.text();
    })
    .catch((error: unknown) => {
      templatePromise = undefined;
      throw error;
    });
  return templatePromise;
}

// Desktop/mobile workspaces share verified metadata, but own their iframe URLs.
// A different token or resolution always causes new verification.
const documentCache = new WeakMap<
  ReleaseCatalogResolution,
  { token: string; html: Promise<string> }
>();
function loadDocument(release: ReleaseCatalogResolution, token: string): Promise<string> {
  const cached = documentCache.get(release);
  if (cached?.token === token) return cached.html;
  const html = Promise.all([loadTemplate(), loadReleaseDbtArtifacts(release, token)])
    .then(([template, artifacts]) =>
      populateDbtDocs(template, prepareDbtManifest(artifacts.manifest, release), artifacts.catalog)
    )
    .catch((error: unknown) => {
      documentCache.delete(release);
      throw error;
    });
  documentCache.set(release, { token, html });
  return html;
}

export default function CatalogDocsTab() {
  const release = useDuckStore((state) => state.lakehouseRelease);
  const token = useDuckStore((state) => state.googleAuth.token);
  const [attempt, setAttempt] = useState(0);
  const [document, setDocument] = useState<{
    url: string;
    release: ReleaseCatalogResolution;
    token: string;
    attempt: number;
  } | null>(null);
  const [failure, setFailure] = useState<{
    message: string;
    release: ReleaseCatalogResolution;
    token: string;
    attempt: number;
  } | null>(null);
  useEffect(() => {
    let cancelled = false;
    let dispose: (() => void) | undefined;
    if (!token || release?.kind !== "release") return;
    loadDocument(release, token)
      .then((html) => {
        if (cancelled) return;
        const prepared = createDbtDocsDocument(html);
        dispose = prepared.dispose;
        setDocument({ url: prepared.url, release, token, attempt });
      })
      .catch((cause: unknown) => {
        if (!cancelled)
          setFailure({
            message: cause instanceof Error ? cause.message : "The catalogue could not be loaded.",
            release,
            token,
            attempt,
          });
      });
    return () => {
      cancelled = true;
      dispose?.();
    };
  }, [release, token, attempt]);

  const ready =
    document &&
    document.release === release &&
    document.token === token &&
    document.attempt === attempt;
  const error =
    failure?.release === release && failure?.token === token && failure?.attempt === attempt
      ? failure.message
      : null;
  return (
    <section className="flex h-full min-h-0 flex-col bg-background" aria-label="Data catalogue">
      <header className="flex flex-wrap items-center justify-between gap-2 border-b px-4 py-3">
        <div className="flex items-center gap-2">
          <Layers className="h-4 w-4 text-primary" />
          <h1 className="text-sm font-semibold">Data catalogue</h1>
        </div>
        <Button
          variant="outline"
          size="sm"
          disabled={!token || release?.kind !== "release"}
          onClick={() => {
            if (release) documentCache.delete(release);
            templatePromise = undefined;
            setAttempt((value) => value + 1);
          }}
        >
          <RefreshCw className="mr-2 h-3.5 w-3.5" />
          Refresh catalogue
        </Button>
      </header>
      {release?.kind === "release" && (
        <p className="break-all border-b bg-muted/30 px-4 py-2 text-xs text-muted-foreground">
          Sources, tables and lineage · Release {release.manifest.release_id}
        </p>
      )}
      {!token || release?.kind !== "release" ? (
        <p className="p-6 text-sm text-muted-foreground">
          Connect Google Drive to view the catalogue for your published data release.
        </p>
      ) : error ? (
        <div className="m-6 max-w-xl space-y-2" role="alert">
          <p className="font-medium">Data catalogue unavailable</p>
          <p className="text-sm text-muted-foreground">{error}</p>
          <p className="text-sm text-muted-foreground">You can continue using the SQL workspace.</p>
        </div>
      ) : ready ? (
        <iframe
          src={document.url}
          title="Data catalogue — dbt Docs"
          sandbox="allow-scripts"
          className="min-h-0 flex-1 w-full border-0 bg-white"
        />
      ) : (
        <p className="p-6 text-sm text-muted-foreground" role="status">
          Loading verified release documentation…
        </p>
      )}
    </section>
  );
}
