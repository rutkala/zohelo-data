import { useEffect, useRef, useState } from "react";
import { Layers, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useDuckStore } from "@/store";
import { dbtDocsUrl, populateDbtDocs } from "@/lib/dbtDocs";
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

function NativeDbtViewer({ html }: { html: string }) {
  const frame = useRef<HTMLIFrameElement>(null);
  const [error, setError] = useState<string | null>(null);
  const hostUrl = new URL(
    "catalogue-viewer.html",
    new URL(import.meta.env.BASE_URL, window.location.origin)
  ).toString();
  useEffect(() => {
    let nonce: string | null = null;
    const receive = (event: MessageEvent) => {
      if (event.source !== frame.current?.contentWindow || event.origin !== "null") return;
      const message = event.data;
      if (!message || typeof message !== "object") return;
      if (
        message.type === "zohelo-catalogue-viewer-ready" &&
        typeof message.nonce === "string" &&
        !nonce
      ) {
        nonce = message.nonce;
        frame.current?.contentWindow?.postMessage(
          { type: "zohelo-catalogue-viewer-document", nonce, html },
          "*"
        );
      } else if (message.type === "zohelo-catalogue-viewer-error" && nonce === message.nonce) {
        setError(
          typeof message.message === "string"
            ? message.message
            : "The catalogue viewer could not start."
        );
      }
    };
    window.addEventListener("message", receive);
    return () => window.removeEventListener("message", receive);
  }, [html]);
  if (error)
    return (
      <p role="alert" className="p-6 text-sm">
        Data catalogue unavailable: {error}
      </p>
    );
  return (
    <iframe
      ref={frame}
      src={hostUrl}
      onLoad={() =>
        frame.current?.contentWindow?.postMessage({ type: "zohelo-catalogue-viewer-request" }, "*")
      }
      title="Data catalogue — dbt Docs"
      sandbox="allow-scripts"
      className="min-h-0 min-w-0 flex-1 w-full border-0 bg-white"
    />
  );
}

export default function CatalogDocsTab() {
  const release = useDuckStore((state) => state.lakehouseRelease);
  const token = useDuckStore((state) => state.googleAuth.token);
  const [attempt, setAttempt] = useState(0);
  const [document, setDocument] = useState<{
    html: string;
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
    if (!token || release?.kind !== "release") return;
    loadDocument(release, token)
      .then((html) => {
        if (cancelled) return;
        setDocument({ html, release, token, attempt });
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
      <header className="flex shrink-0 items-center justify-between gap-2 border-b px-3 py-2 sm:px-4 sm:py-3">
        <div className="flex min-w-0 items-center gap-2">
          <Layers className="h-4 w-4 text-primary" />
          <h1 className="truncate text-sm font-semibold">Data catalogue</h1>
        </div>
        <Button
          variant="outline"
          size="sm"
          className="shrink-0 px-2 sm:px-3"
          aria-label="Refresh catalogue"
          disabled={!token || release?.kind !== "release"}
          onClick={() => {
            if (release) documentCache.delete(release);
            templatePromise = undefined;
            setAttempt((value) => value + 1);
          }}
        >
          <RefreshCw className="h-3.5 w-3.5 sm:mr-2" />
          <span className="hidden sm:inline">Refresh catalogue</span>
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
        <NativeDbtViewer html={document.html} />
      ) : (
        <p className="p-6 text-sm text-muted-foreground" role="status">
          Loading verified release documentation…
        </p>
      )}
    </section>
  );
}
