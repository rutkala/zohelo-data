/**
 * Catalog & Lineage Tab
 * Displays the generated dbt documentation and lineage graph in an embedded iframe.
 */
import { useState } from "react";
import { ExternalLink, RefreshCw, Layers } from "lucide-react";
import { Button } from "@/components/ui/button";

export default function CatalogDocsTab() {
  const [iframeKey, setIframeKey] = useState(0);
  const docsUrl = "./docs/index.html";

  const handleRefresh = () => {
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
          >
            <ExternalLink className="h-3.5 w-3.5" />
            Open in New Window
          </Button>
        </div>
      </div>

      {/* Embedded dbt Docs Frame */}
      <div className="flex-1 w-full h-full relative">
        <iframe
          key={iframeKey}
          src={docsUrl}
          title="dbt Catalog & Lineage"
          className="w-full h-full border-none"
        />
      </div>
    </div>
  );
}
