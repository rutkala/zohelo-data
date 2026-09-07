/**
 * Catalog & Lineage Tab
 * Displays the generated dbt documentation and lineage graph in an embedded iframe.
 */
import { useEffect, useMemo, useState } from "react";
import { ExternalLink, RefreshCw, Layers } from "lucide-react";
import { Button } from "@/components/ui/button";
import { dbtDocsUrl, isDbtDocsHtml } from "@/lib/dbtDocs";

type DocsStatus = "checking" | "ready" | "error";

export default function CatalogDocsTab() {
  const [iframeKey, setIframeKey] = useState(0);
  const [docsStatus, setDocsStatus] = useState<DocsStatus>("checking");
  const [error, setError] = useState<string | null>(null);
  const docsUrl = useMemo(() => dbtDocsUrl(import.meta.env.BASE_URL, window.location.origin), []);

  useEffect(() => {
    const controller = new AbortController();

    fetch(docsUrl, { signal: controller.signal })
      .then(async (response) => {
        if (!response.ok) {
          throw new Error(`The docs server returned ${response.status}.`);
        }

        const html = await response.text();
        if (!isDbtDocsHtml(html)) {
          throw new Error("The deployed file was not a dbt Docs document.");
        }
      })
      .then(() => {
        if (!controller.signal.aborted) setDocsStatus("ready");
      })
      .catch((cause: unknown) => {
        if (controller.signal.aborted) return;
        setDocsStatus("error");
        setError(cause instanceof Error ? cause.message : "The docs could not be loaded.");
      });

    return () => controller.abort();
  }, [docsUrl, iframeKey]);

  const handleRefresh = () => {
    setDocsStatus("checking");
    setError(null);
    setIframeKey((prev) => prev + 1);
  };

  const handleOpenExternal = () => {
    window.open(docsUrl, "_blank", "noopener,noreferrer");
  };

  return (
    <div className="flex flex-col h-full w-full bg-background overflow-hidden">
      {/* Top action bar */}
      <div className="flex items-center justify-between px-4 py-2 border-b bg-muted/30">
        <div className="flex items-center gap-2">
          <Layers className="h-4 w-4 text-primary" />
          <span className="text-sm font-medium">dbt Catalog &amp; Lineage Graph</span>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            className="h-8 gap-1.5 text-xs"
            onClick={handleRefresh}
          >
            <RefreshCw className="h-3.5 w-3.5" />
            Reload Docs
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="h-8 gap-1.5 text-xs"
            onClick={handleOpenExternal}
            disabled={docsStatus !== "ready"}
          >
            <ExternalLink className="h-3.5 w-3.5" />
            Open in New Window
          </Button>
        </div>
      </div>

      {/* Embedded dbt Docs Frame */}
      <div className="flex-1 w-full h-full relative">
        {docsStatus === "checking" ? (
          <div
            className="flex h-full items-center justify-center text-sm text-muted-foreground"
            role="status"
          >
            Checking generated dbt documentation…
          </div>
        ) : docsStatus === "ready" ? (
          <iframe
            key={iframeKey}
            src={docsUrl}
            title="dbt Catalog & Lineage"
            className="w-full h-full border-none"
          />
        ) : (
          <div className="flex h-full items-center justify-center p-6">
            <div className="max-w-lg space-y-2 text-center" role="alert">
              <p className="font-medium">Generated dbt documentation is unavailable.</p>
              <p className="text-sm text-muted-foreground">
                {error} Redeploy the portal after generating dbt docs, then reload this tab.
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
