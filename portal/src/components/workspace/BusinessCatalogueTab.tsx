import { useState } from "react";
import { useDuckStore } from "@/store";
import { Button } from "@/components/ui/button";
import BusinessCatalogue from "@/components/explorer/BusinessCatalogue";
import CatalogDocsTab from "./CatalogDocsTab";
import { NBP_FX_DIMENSION_JOIN } from "@/lib/nbpSqlExamples";

export default function BusinessCatalogueTab() {
  const release = useDuckStore((state) => state.lakehouseRelease);
  const prepareQuery = useDuckStore((state) => state.prepareLakehouseQuery);
  const isLoading = useDuckStore((state) => state.isLakehouseLoading);
  const status = useDuckStore((state) => state.lakehouseStatusMessage);
  const [technicalDocs, setTechnicalDocs] = useState(false);

  const openReview = () => {
    const state = useDuckStore.getState();
    const existing = state.tabs.find((tab) => tab.type === "review");
    if (existing) state.setActiveTab(existing.id);
    else state.createTab("review", "", "Review & decisions");
  };

  return (
    <div className="flex h-full min-h-0 flex-col">
      <header className="space-y-3 border-b p-4">
        <h1 className="text-xl font-semibold">Business catalogue</h1>
        <p className="text-sm text-muted-foreground">
          Sources, status and lineage for the data release you have connected.
        </p>
        {release?.kind === "release" && (
          <p className="break-all text-xs text-muted-foreground">
            Data release: {release.manifest.release_id}
          </p>
        )}
        <div className="flex flex-wrap gap-2">
          <Button
            variant={technicalDocs ? "outline" : "secondary"}
            onClick={() => setTechnicalDocs(false)}
          >
            Sources, lineage &amp; metrics
          </Button>
          <Button variant="outline" onClick={openReview}>
            Review &amp; decisions
          </Button>
          <Button variant="outline" onClick={() => setTechnicalDocs(true)}>
            Technical dbt docs
          </Button>
          <Button
            disabled={isLoading || release?.kind !== "release"}
            onClick={() =>
              void prepareQuery(
                NBP_FX_DIMENSION_JOIN.tables,
                NBP_FX_DIMENSION_JOIN.sql,
                NBP_FX_DIMENSION_JOIN.title
              )
            }
          >
            Open join example
          </Button>
        </div>
        <p className="text-xs text-muted-foreground">
          The join example loads its fact and dimensions, then opens SQL for you to run. Metrics
          proposals and questions are in Review &amp; decisions.
        </p>
        {status && (
          <p role="status" className="text-xs text-muted-foreground">
            {status}
          </p>
        )}
      </header>
      {technicalDocs ? (
        <div className="flex min-h-0 flex-1 flex-col">
          <p className="border-b p-3 text-xs text-muted-foreground">
            Technical docs describe the deployed code. The business catalogue above describes the
            connected data release.
          </p>
          <div className="min-h-0 flex-1">
            <CatalogDocsTab />
          </div>
        </div>
      ) : (
        <div className="min-h-0 flex-1 overflow-y-auto p-2 sm:p-4">
          <BusinessCatalogue release={release} fullPage />
        </div>
      )}
    </div>
  );
}
