import { useId, useMemo, useState } from "react";
import { ChevronDown, ChevronRight, Maximize2 } from "lucide-react";
import { useDuckStore } from "@/store";
import { Badge } from "@/components/ui/badge";
import type { ReleaseCatalogResolution } from "@/services/googleDrive";

type CatalogueTab = "sources" | "lineage" | "metrics";

const valueOrUnavailable = (value: string | null) => value ?? "Not recorded";
const statusLabel = (value: string) =>
  value
    .split(/[_-]+/)
    .filter(Boolean)
    .map((word) => word[0]?.toUpperCase() + word.slice(1))
    .join(" ");

export default function BusinessCatalogue({
  release,
  fullPage = false,
}: {
  release: ReleaseCatalogResolution | null;
  fullPage?: boolean;
}) {
  const [tab, setTab] = useState<CatalogueTab>("sources");
  const [open, setOpen] = useState(fullPage);
  const contentId = useId();
  const openFullCatalogue = () => {
    const state = useDuckStore.getState();
    const existing = state.tabs.find((item) => item.type === "catalog");
    if (existing) state.setActiveTab(existing.id);
    else state.createTab("catalog", "", "Business catalogue");
  };
  const catalogue = release?.kind === "release" ? release.businessCatalogue : undefined;
  const nodeLabels = useMemo(
    () => new Map(catalogue?.lineage.nodes.map((node) => [node.id, node.label]) ?? []),
    [catalogue]
  );

  if (!catalogue) {
    const message =
      release?.kind === "release" && release.manifest.format_version === 1
        ? "Business catalogue not published for this v1 silver release. Its released tables remain available to query."
        : release?.kind === "legacy"
          ? "Business catalogue not published for this legacy, unversioned data."
          : "Connect Google Drive to view the release business catalogue.";
    return (
      <section className="border-b px-3 py-2 text-xs" aria-label="Business catalogue">
        <h2 className="font-semibold">{fullPage ? "Connect your data" : "Business catalogue"}</h2>
        <p className="mt-1 text-muted-foreground leading-relaxed">{message}</p>
      </section>
    );
  }

  return (
    <section
      className={`border-b px-3 py-2 ${fullPage ? "text-sm" : "text-xs"}`}
      aria-label="Business catalogue"
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        {fullPage ? (
          <h2 className="font-semibold">Published sources and lineage</h2>
        ) : (
          <button
            type="button"
            className="inline-flex items-center gap-1 font-semibold hover:underline"
            aria-expanded={open}
            aria-controls={contentId}
            onClick={() => setOpen((current) => !current)}
          >
            {open ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
            Business catalogue
          </button>
        )}
        <Badge variant="secondary" className="text-[10px]">
          {catalogue.sources.length} source{catalogue.sources.length === 1 ? "" : "s"}
        </Badge>
      </div>
      {!fullPage && (
        <button
          type="button"
          className="mt-2 inline-flex items-center gap-1 text-[11px] underline"
          onClick={openFullCatalogue}
        >
          <Maximize2 className="h-3 w-3" />
          Open full catalogue
        </button>
      )}

      {open && (
        <div id={contentId} className={fullPage ? "mt-4" : "mt-2 max-h-80 overflow-y-auto pr-1"}>
          <div className="flex gap-1" role="tablist" aria-label="Business catalogue views">
            {(["sources", "lineage", "metrics"] as const).map((item) => (
              <button
                key={item}
                type="button"
                role="tab"
                aria-selected={tab === item}
                className={`rounded px-2 py-1 capitalize ${
                  tab === item
                    ? "bg-muted font-medium text-foreground"
                    : "text-muted-foreground hover:bg-muted/60"
                }`}
                onClick={() => setTab(item)}
              >
                {item}
              </button>
            ))}
          </div>

          {tab === "sources" && (
            <div className="mt-2 space-y-2">
              {catalogue.sources.length === 0 ? (
                <p className="text-muted-foreground">
                  No source records are published for this release.
                </p>
              ) : (
                catalogue.sources.map((source) => (
                  <article key={source.source_id} className="rounded border p-2 leading-relaxed">
                    <div className="flex flex-wrap items-center gap-1.5">
                      <strong>{source.name}</strong>
                      <Badge variant="secondary" className="text-[10px]">
                        {statusLabel(source.status)}
                      </Badge>
                    </div>
                    <p className="mt-1 text-muted-foreground">{source.description}</p>
                    <dl
                      className={`mt-2 grid grid-cols-1 gap-x-3 gap-y-1 sm:grid-cols-2 ${fullPage ? "text-sm" : "text-[11px]"}`}
                    >
                      <div>
                        <dt className="text-muted-foreground">Checked through</dt>
                        <dd>{valueOrUnavailable(source.checked_through)}</dd>
                      </div>
                      <div>
                        <dt className="text-muted-foreground">Latest observation</dt>
                        <dd>{valueOrUnavailable(source.latest_observation_date)}</dd>
                      </div>
                      <div>
                        <dt className="text-muted-foreground">Last successful ingestion</dt>
                        <dd className="break-words">
                          {valueOrUnavailable(source.last_successful_ingestion_at)}
                        </dd>
                      </div>
                      <div>
                        <dt className="text-muted-foreground">Last attempt</dt>
                        <dd className="break-words">
                          {valueOrUnavailable(source.last_attempt_at)}
                        </dd>
                      </div>
                      <div>
                        <dt className="text-muted-foreground">Raw responses</dt>
                        <dd>{source.raw_response_count}</dd>
                      </div>
                    </dl>
                  </article>
                ))
              )}
              <p className="text-[11px] text-muted-foreground">
                Status describes this published data snapshot. A last attempt records an attempt,
                not a freshness guarantee.
              </p>
            </div>
          )}

          {tab === "lineage" && (
            <div className="mt-2 space-y-2 leading-relaxed">
              {catalogue.lineage.nodes.length === 0 ? (
                <p className="text-muted-foreground">
                  No lineage nodes are published for this release.
                </p>
              ) : (
                <ul className="space-y-1" aria-label="Lineage nodes">
                  {catalogue.lineage.nodes.map((node) => (
                    <li key={node.id} className="rounded border p-2">
                      <strong>{node.label}</strong>{" "}
                      <span className="text-muted-foreground">
                        · {node.kind} · {node.layer}
                      </span>
                      <p className="text-muted-foreground">{node.description}</p>
                    </li>
                  ))}
                </ul>
              )}
              {catalogue.lineage.edges.length > 0 && (
                <div>
                  <h3 className="font-medium">Dependencies</h3>
                  <ul
                    className="mt-1 space-y-1 text-muted-foreground"
                    aria-label="Lineage dependencies"
                  >
                    {catalogue.lineage.edges.map((edge, index) => (
                      <li key={`${edge.from}:${edge.to}:${index}`} className="break-words">
                        {nodeLabels.get(edge.from) ?? edge.from} →{" "}
                        {nodeLabels.get(edge.to) ?? edge.to}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}

          {tab === "metrics" && (
            <div className="mt-2 leading-relaxed">
              {catalogue.metrics.length === 0 ? (
                <p className="text-muted-foreground">
                  Governed metric definitions are awaiting business approval.
                </p>
              ) : (
                <p className="text-muted-foreground">
                  {catalogue.metrics.length} governed metric definition
                  {catalogue.metrics.length === 1 ? "" : "s"} published for this release.
                </p>
              )}
            </div>
          )}
        </div>
      )}
    </section>
  );
}
