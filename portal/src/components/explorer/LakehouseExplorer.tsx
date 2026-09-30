/**
 * Google Drive Lakehouse Explorer Component
 * Integrates Medallion Architecture (landing, bronze, silver, gold, archive)
 * with DuckDB-WASM in-browser execution.
 */
import { useState } from "react";
import {
  ChevronRight,
  ChevronDown,
  Folder,
  FileSpreadsheet,
  RefreshCw,
  Key,
  LogIn,
  LogOut,
  Layers,
  Loader2,
  Table as TableIcon,
  EllipsisVertical,
} from "lucide-react";
import { useDuckStore } from "@/store";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { qualifyTable } from "@/lib/sqlSanitize";
import { setSqlRelationDragData } from "@/lib/sqlTableActions";
import { TableActions } from "./TableActions";
import {
  isNativeFolder,
  nativePreviewFormat,
  NATIVE_PREVIEW_LIMIT_BYTES,
  type NativeLandingFile,
} from "@/services/googleDrive";

interface LakehouseExplorerProps {
  onSqlAction?: () => void;
}

// Display aliases only: discovery and identity always use the actual Drive objects.
const sourceLabels: Record<string, string> = {
  eurostat: "Eurostat",
  gleif: "GLEIF",
  gugik_prg: "GUGiK PRG",
  gus_bdl: "GUS BDL",
  gus_dbw: "GUS DBW",
  gus_teryt: "GUS TERYT",
  mf_biala_lista: "MF VAT White List",
  nbp: "NBP",
  world_bank_wdi: "World Bank WDI",
  opendata_org: "OpenData.org",
};

export default function LakehouseExplorer({ onSqlAction }: LakehouseExplorerProps) {
  const googleAuth = useDuckStore((s) => s.googleAuth);
  const lakehouseCatalog = useDuckStore((s) => s.lakehouseCatalog);
  const lakehouseRelease = useDuckStore((s) => s.lakehouseRelease);
  const lakehouseLanding = useDuckStore((s) => s.lakehouseLanding);
  const isLakehouseLoading = useDuckStore((s) => s.isLakehouseLoading);
  const lakehouseStatusMessage = useDuckStore((s) => s.lakehouseStatusMessage);
  const activeLakehouseDataset = useDuckStore((s) => s.activeLakehouseDataset);
  const activeLakehouseLayer = useDuckStore((s) => s.activeLakehouseLayer);
  const nativeRoot = useDuckStore((s) => s.nativeLandingRoot);
  const nativeChildren = useDuckStore((s) => s.nativeLandingChildren);
  const nativeLoading = useDuckStore((s) => s.nativeLandingLoading);
  const nativeError = useDuckStore((s) => s.nativeLandingError);
  const nativeSelected = useDuckStore((s) => s.nativeLandingSelected);
  const nativeLinks = useDuckStore((s) => s.nativeLandingLinks);
  const nativeActionError = useDuckStore((s) => s.nativeLandingActionError);
  const nativeMetadataTables = useDuckStore((s) => s.nativeMetadataTables);
  const nativeMetadataLoading = useDuckStore((s) => s.nativeMetadataLoading);
  const nativeMetadataProgress = useDuckStore((s) => s.nativeMetadataProgress);
  const nativeMetadataError = useDuckStore((s) => s.nativeMetadataError);
  const loadNativeMetadataTable = useDuckStore((s) => s.loadNativeMetadataTable);
  const cancelNativeMetadataScan = useDuckStore((s) => s.cancelNativeMetadataScan);
  const loadNativeFolder = useDuckStore((s) => s.loadNativeLandingFolder);
  const verifyNativeFile = useDuckStore((s) => s.verifyNativeLandingFile);
  const previewNativeFile = useDuckStore((s) => s.previewNativeLandingFile);
  const refreshNative = useDuckStore((s) => s.refreshNativeLanding);

  const signInWithGoogle = useDuckStore((s) => s.signInWithGoogle);
  const setManualGoogleToken = useDuckStore((s) => s.setManualGoogleToken);
  const disconnectGoogleDrive = useDuckStore((s) => s.disconnectGoogleDrive);
  const refreshLakehouseCatalog = useDuckStore((s) => s.refreshLakehouseCatalog);
  const toggleLakehouseLayer = useDuckStore((s) => s.toggleLakehouseLayer);
  const toggleLakehouseTable = useDuckStore((s) => s.toggleLakehouseTable);
  const selectLakehouseDataset = useDuckStore((s) => s.selectLakehouseDataset);
  const selectLakehouseFile = useDuckStore((s) => s.selectLakehouseFile);

  const createTab = useDuckStore((s) => s.createTab);
  const executeQuery = useDuckStore((s) => s.executeQuery);

  const [manualToken, setManualToken] = useState("");
  const [popoverOpen, setPopoverOpen] = useState(false);
  const [popupBlocked, setPopupBlocked] = useState(false);
  const [unavailableAction, setUnavailableAction] = useState<string | null>(null);
  const [nativeOpen, setNativeOpen] = useState(true);
  const [nativeDetails, setNativeDetails] = useState<{
    root: typeof nativeRoot;
    id: string;
  } | null>(null);
  const [folderExpansion, setFolderExpansion] = useState<{
    root: typeof nativeRoot;
    ids: ReadonlySet<string>;
  }>(() => ({ root: null, ids: new Set() }));
  const openFolders = folderExpansion.root === nativeRoot ? folderExpansion.ids : new Set<string>();
  const [dbwIndicatorSearch, setDbwIndicatorSearch] = useState("");
  const hasRetainedDbw = lakehouseLanding?.snapshots.some(
    ({ manifest }) => manifest.kind === "retained_bronze_snapshot"
  );
  const retainedDbw = lakehouseLanding?.snapshots.find(
    ({ manifest }) => manifest.kind === "retained_bronze_snapshot"
  );
  const dbwIndicators =
    retainedDbw?.manifest.kind === "retained_bronze_snapshot"
      ? retainedDbw.manifest.indicators
      : [];
  const dbwSearchTerm = dbwIndicatorSearch.trim().toLocaleLowerCase();
  const matchingDbwIndicators = dbwSearchTerm
    ? dbwIndicators
        .filter((indicator) =>
          `${indicator.indicator_id} ${indicator.indicator_name} ${indicator.indicator_name_en} ${indicator.taxonomy_path}`
            .toLocaleLowerCase()
            .includes(dbwSearchTerm)
        )
        .sort((left, right) => {
          const exact = Number(dbwSearchTerm);
          if (Number.isInteger(exact) && left.indicator_id === exact) return -1;
          if (Number.isInteger(exact) && right.indicator_id === exact) return 1;
          return left.indicator_id - right.indicator_id;
        })
    : [];
  const formatBytes = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    const units = ["KiB", "MiB", "GiB", "TiB"];
    let value = bytes / 1024;
    let unit = units[0];
    for (let index = 1; value >= 1024 && index < units.length; index += 1) {
      value /= 1024;
      unit = units[index];
    }
    return `${value.toFixed(value >= 10 ? 1 : 2)} ${unit}`;
  };
  const baseUrl = import.meta.env.BASE_URL === "./" ? "/" : (import.meta.env.BASE_URL ?? "/");
  const privacyUrl = `${baseUrl.replace(/\/$/, "")}/privacy.html`;
  const sourceAccessGuideUrl =
    "https://github.com/rutkala/zohelo-data/blob/main/docs/source-accounts.md";
  const encryptedSecretsUrl = "https://github.com/rutkala/zohelo-data/settings/secrets/actions";

  const handleApplyManualToken = async () => {
    if (!manualToken.trim()) return;
    const ok = await setManualGoogleToken(manualToken.trim());
    if (ok) {
      setManualToken("");
      setPopoverOpen(false);
    }
  };

  const openPreview = async (target: string | null, title: string) => {
    if (!target) return;
    const sql = `SELECT * FROM ${target} LIMIT 50;`;
    // Keep existing SQL drafts and their results together.
    createTab("sql", sql, title);
    const tabId = useDuckStore.getState().activeTabId;
    if (tabId) await executeQuery(sql, tabId);
  };

  const handleSelectDataset = async (layerName: string, tableName: string) => {
    if (isLakehouseLoading) return;
    const target = await selectLakehouseDataset(layerName, tableName);
    await openPreview(target, `${layerName}/${tableName}`);
  };

  const handleSelectFile = async (
    layerName: string,
    tableName: string,
    fileName: string,
    fileId: string
  ) => {
    if (isLakehouseLoading) return;
    const target = await selectLakehouseFile(layerName, tableName, fileId);
    await openPreview(target, `${layerName}/${tableName}/${fileName}`);
  };

  const handleNativeAction = async (file: NativeLandingFile, action: "open" | "download") => {
    // Reserve the tab in the click's user activation; metadata reads are asynchronous.
    const tab = window.open("about:blank", "_blank");
    setPopupBlocked(!tab);
    setUnavailableAction(null);
    const links = await verifyNativeFile(file.parentId, file.id);
    const link = links?.[action];
    if (!link) {
      tab?.close();
      if (links) setUnavailableAction(`${file.id}:${action}`);
      return;
    }
    if (tab) {
      tab.opener = null;
      tab.location.replace(link);
    }
    // Retry the action with popups permitted to recheck current Drive identity.
  };

  const renderNativeFolder = (folderId: string, depth = 0): React.ReactNode => {
    const state = nativeChildren[folderId];
    if (!state) return null;
    return (
      <div className={depth < 3 ? "border-l pl-2" : "border-l pl-1"} data-native-depth={depth}>
        {state.loading && (
          <div role="status" className="text-muted-foreground">
            Loading folder pages…
          </div>
        )}
        {state.error && (
          <div role="alert" className="text-destructive">
            {state.error}{" "}
            <Button size="sm" variant="outline" onClick={() => loadNativeFolder(folderId)}>
              Retry
            </Button>
          </div>
        )}
        {state.loaded && state.files.length === 0 && (
          <div className="text-muted-foreground italic">Empty folder</div>
        )}
        {state.loaded &&
          state.files.map((file) => {
            const folder = isNativeFolder(file);
            const expanded = openFolders.has(file.id);
            const loadedChild = nativeChildren[file.id];
            const preview = nativePreviewFormat(file);
            const selected = nativeSelected === file.id;
            const detailsOpen = nativeDetails?.root === nativeRoot && nativeDetails?.id === file.id;
            const label =
              folder && depth === 0 ? (sourceLabels[file.name] ?? file.name) : file.name;
            const toggleDetails = () =>
              setNativeDetails(detailsOpen ? null : { root: nativeRoot, id: file.id });
            return (
              <div
                key={file.id}
                data-native-id={file.id}
                data-native-folder={folder ? "true" : "false"}
                className="min-w-0"
              >
                <div
                  className={`flex min-w-0 items-center gap-1 rounded hover:bg-muted/60 ${detailsOpen ? "bg-muted/60" : ""}`}
                >
                  {folder ? (
                    <button
                      type="button"
                      aria-expanded={expanded}
                      aria-label={`${expanded ? "Collapse" : "Expand"} ${file.name}`}
                      title={file.name}
                      className="flex min-h-11 sm:min-h-9 flex-1 items-center gap-1.5 py-1 text-left min-w-0"
                      onClick={() => {
                        setFolderExpansion(() => {
                          const next = new Set(openFolders);
                          if (next.has(file.id)) next.delete(file.id);
                          else next.add(file.id);
                          return { root: nativeRoot, ids: next };
                        });
                        if (!loadedChild?.loaded && !loadedChild?.loading)
                          void loadNativeFolder(file.id);
                      }}
                    >
                      {expanded ? (
                        <ChevronDown className="h-3 w-3 shrink-0" />
                      ) : (
                        <ChevronRight className="h-3 w-3 shrink-0" />
                      )}
                      <Folder className="h-4 w-4 shrink-0 text-amber-500" />
                      <span className="break-words min-w-0">{label}</span>
                    </button>
                  ) : (
                    <button
                      type="button"
                      aria-expanded={detailsOpen}
                      aria-label={`File ${file.name}`}
                      title={file.name}
                      className="flex min-h-11 sm:min-h-9 min-w-0 flex-1 items-center gap-1.5 py-1 text-left"
                      onClick={toggleDetails}
                    >
                      <FileSpreadsheet className="h-4 w-4 shrink-0 text-muted-foreground" />
                      <span className="break-all">{file.name}</span>
                    </button>
                  )}
                  {!folder && file.size !== undefined && (
                    <span className="shrink-0 text-[10px] text-muted-foreground">
                      {formatBytes(file.size)}
                    </span>
                  )}
                  <button
                    type="button"
                    aria-label={`Actions for ${file.name}`}
                    aria-expanded={detailsOpen}
                    className="flex h-11 w-9 sm:h-9 shrink-0 items-center justify-center rounded hover:bg-muted"
                    onClick={toggleDetails}
                  >
                    <EllipsisVertical className="h-4 w-4" />
                  </button>
                </div>
                {detailsOpen && (
                  <div className="mb-2 space-y-2 rounded bg-muted/30 p-2 text-[11px]">
                    <div className="flex flex-wrap gap-1.5">
                      {folder && depth === 0 && nativeMetadataTables[file.id] && (
                        <Button
                          size="sm"
                          variant="outline"
                          className="min-h-9 h-auto whitespace-normal text-xs"
                          disabled={!!nativeMetadataLoading}
                          onClick={async () => {
                            const target = await loadNativeMetadataTable(file.id);
                            await openPreview(target, `Landing/${file.name} file metadata`);
                            if (target) onSqlAction?.();
                          }}
                        >
                          {nativeMetadataLoading === file.id
                            ? nativeMetadataProgress?.phase === "cancelling" ? "Cancelling scan…" : "Scanning metadata…"
                            : "Query file metadata"}
                        </Button>
                      )}
                      {nativeMetadataLoading === file.id && (
                        <Button size="sm" variant="outline" className="min-h-9 h-auto text-xs"
                          disabled={nativeMetadataProgress?.phase === "cancelling"}
                          onClick={cancelNativeMetadataScan}>Cancel scan</Button>
                      )}
                      <Button
                        size="sm"
                        variant="outline"
                        className="min-h-9 h-auto whitespace-normal text-xs"
                        onClick={() => void handleNativeAction(file, "open")}
                      >
                        Open in Drive
                      </Button>
                      {file.capabilities?.canDownload && (
                        <Button
                          size="sm"
                          variant="outline"
                          className="min-h-9 h-auto whitespace-normal text-xs"
                          onClick={() => void handleNativeAction(file, "download")}
                        >
                          Download via Drive
                        </Button>
                      )}
                      {!folder && preview && (
                        <Button
                          size="sm"
                          variant="outline"
                          className="min-h-9 h-auto whitespace-normal text-xs"
                          onClick={async () => {
                            const target = await previewNativeFile(file.parentId, file.id);
                            await openPreview(target, `Landing/${file.name}`);
                            if (target) onSqlAction?.();
                          }}
                        >
                          Preview in SQL
                        </Button>
                      )}
                    </div>
                    {folder && depth === 0 && nativeMetadataTables[file.id] && (
                      <p className="break-all text-muted-foreground">
                        SQL: {qualifyTable(undefined, "01_landing", nativeMetadataTables[file.id])}
                      </p>
                    )}
                    {nativeMetadataLoading === file.id && nativeMetadataProgress && (
                      <p role="status" className="text-muted-foreground">
                        {{ folders: "Finding folders", files: "Reading file metadata",
                          verifying: "Checking folders", publishing: "Preparing SQL",
                          cancelling: "Cancelling" }[nativeMetadataProgress.phase]}:
                        {" "}{nativeMetadataProgress.folders.toLocaleString()} folders,
                        {" "}{nativeMetadataProgress.files.toLocaleString()} files,
                        {" "}{nativeMetadataProgress.listPages.toLocaleString()} Drive list pages;
                        {" "}{Math.round((Date.now() - nativeMetadataProgress.startedAtMs) / 1000)}s elapsed.
                      </p>
                    )}
                    {!folder && !preview && (
                      <p className="text-muted-foreground">
                        Preview unavailable for this file. Open or download the original in Drive.
                      </p>
                    )}
                    <details className="text-muted-foreground">
                      <summary className="cursor-pointer py-1">File details</summary>
                      <dl className="mt-1 space-y-1 break-all">
                        <div>
                          <dt className="inline font-medium">Original name: </dt>
                          <dd className="inline">{file.name}</dd>
                        </div>
                        <div>
                          <dt className="inline font-medium">Drive ID: </dt>
                          <dd className="inline">{file.id}</dd>
                        </div>
                        <div>
                          <dt className="inline font-medium">Format: </dt>
                          <dd className="inline">{file.mimeType || "Unknown"}</dd>
                        </div>
                        {file.modifiedTime && (
                          <div>
                            <dt className="inline font-medium">Modified: </dt>
                            <dd className="inline">
                              {new Date(file.modifiedTime).toLocaleString()}
                            </dd>
                          </div>
                        )}
                        {file.version && (
                          <div>
                            <dt className="inline font-medium">Version: </dt>
                            <dd className="inline">{file.version}</dd>
                          </div>
                        )}
                        {(file.sha256Checksum || file.md5Checksum) && (
                          <div>
                            <dt className="inline font-medium">
                              {file.sha256Checksum ? "SHA-256: " : "MD5: "}
                            </dt>
                            <dd className="inline">{file.sha256Checksum || file.md5Checksum}</dd>
                          </div>
                        )}
                      </dl>
                    </details>
                    {selected && nativeActionError && (
                      <div role="alert" className="text-destructive">
                        {nativeActionError}
                      </div>
                    )}
                    {selected && unavailableAction?.startsWith(`${file.id}:`) && (
                      <div role="alert" className="text-destructive">
                        Drive did not provide a safe{" "}
                        {unavailableAction.endsWith(":download") ? "download" : "view"} link for
                        this file.
                      </div>
                    )}
                    {selected && nativeLinks?.fileId === file.id && popupBlocked && (
                      <p className="text-muted-foreground">Allow popups and retry to open Drive.</p>
                    )}
                  </div>
                )}
                {folder && expanded && renderNativeFolder(file.id, depth + 1)}
              </div>
            );
          })}
      </div>
    );
  };

  const dbwSearch = hasRetainedDbw && (
    <div className="space-y-1 border-b px-3 py-2 text-[11px]">
      <div className="font-medium">Dated retained DBW Bronze snapshot</div>
      <div className="text-muted-foreground">
        Search the audited Polish or English taxonomy, then select one published indicator or an
        explicit part. Pending indicators remain visible while publication advances.
      </div>
      <Input
        value={dbwIndicatorSearch}
        onChange={(event) => setDbwIndicatorSearch(event.target.value)}
        placeholder="Search DBW indicator name or ID"
        className="h-7 text-xs"
      />
      <div className="text-muted-foreground">
        {dbwSearchTerm
          ? `${matchingDbwIndicators.length.toLocaleString()} of ${dbwIndicators.length.toLocaleString()} indicators match; showing the first 50.`
          : `${dbwIndicators.length.toLocaleString()} indicators indexed. Enter a name or ID to choose one.`}
      </div>
      {dbwSearchTerm && (
        <div className="max-h-48 space-y-1 overflow-y-auto">
          {matchingDbwIndicators.slice(0, 50).map((indicator) => {
            const base = `gus_dbw_observations__indicator_${indicator.indicator_id}`;
            const total = indicator.parts.reduce((sum, part) => sum + (part.size ?? 0), 0);
            const large = total > 64 * 1024 * 1024;
            return (
              <div key={indicator.indicator_id} className="rounded border px-2 py-1">
                <div className="font-medium">
                  {indicator.indicator_id} ·{" "}
                  {indicator.indicator_name_en || indicator.indicator_name}
                </div>
                <div className="text-muted-foreground">{indicator.taxonomy_path}</div>
                {indicator.status === "pending" ? (
                  <div className="text-amber-600">Pending query-fragment publication</div>
                ) : large ? (
                  <div className="mt-1 flex flex-wrap gap-1">
                    {indicator.parts.map((part, index) => (
                      <Button
                        key={part.id}
                        size="sm"
                        variant="outline"
                        className="h-6 px-2 text-[10px]"
                        onClick={() =>
                          handleSelectDataset("02_bronze", `${base}__part_${index + 1}`)
                        }
                      >
                        Part {index + 1}/{indicator.parts.length} · {formatBytes(part.size ?? 0)}
                      </Button>
                    ))}
                  </div>
                ) : (
                  <Button
                    size="sm"
                    variant="outline"
                    className="mt-1 h-6 px-2 text-[10px]"
                    onClick={() => handleSelectDataset("02_bronze", base)}
                  >
                    Load selected indicator · {formatBytes(total)}
                  </Button>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );

  const renderTables = (layer: (typeof lakehouseCatalog)[number]) => (
    <div className="ml-3 pl-2 border-l border-border/60 space-y-0.5 mt-0.5">
      {layer.children.length === 0 ? (
        <div className="py-1 px-2 text-[11px] text-muted-foreground italic">
          {layer.loaded ? "No datasets found" : "Click to expand & load…"}
        </div>
      ) : (
        layer.children
          .filter((table) => {
            return !table.name.startsWith("gus_dbw_observations__indicator_");
          })
          .map((table) => {
            const isActive =
              table.name === activeLakehouseDataset && layer.name === activeLakehouseLayer;
            const relation = qualifyTable(undefined, layer.name, table.name);
            return (
              <div key={table.name} className="space-y-0.5">
                {/* Table / Dataset Row */}
                <div
                  className={`flex items-center gap-1.5 py-1 px-1.5 rounded cursor-pointer group ${
                    isActive
                      ? "bg-amber-500/15 text-amber-600 dark:text-amber-400 font-semibold"
                      : "hover:bg-muted/60 text-foreground"
                  }`}
                  draggable
                  onDragStart={(event) => setSqlRelationDragData(event.dataTransfer, relation)}
                >
                  <button
                    type="button"
                    className="shrink-0 rounded p-0.5 hover:bg-muted"
                    onClick={(e) => {
                      e.stopPropagation();
                      toggleLakehouseTable(layer.name, table.name);
                    }}
                  >
                    {table.expanded ? (
                      <ChevronDown className="h-3 w-3 text-muted-foreground" />
                    ) : (
                      <ChevronRight className="h-3 w-3 text-muted-foreground" />
                    )}
                  </button>

                  <div
                    className="flex min-w-0 flex-1 items-center gap-1.5"
                    onClick={() => handleSelectDataset(layer.name, table.name)}
                  >
                    <TableIcon className="h-3.5 w-3.5 text-blue-500 shrink-0" />
                    <span
                      className="truncate text-[11px]"
                      title={`${table.label ?? table.name} · SQL: ${table.name}`}
                    >
                      {layer.name === "02_bronze" ? table.name : (table.label ?? table.name)}
                    </span>
                    {table.name === "gus_dbw_observations" && table.children.length === 0 && (
                      <span className="text-[10px] text-muted-foreground">Full table unavailable</span>
                    )}
                  </div>

                  <TableActions
                    relation={relation}
                    displayName={table.name}
                    onSqlAction={onSqlAction}
                  />
                </div>

                {/* Files in Dataset */}
                {table.expanded && table.children && (
                  <div className="ml-4 pl-2 border-l border-border/40 space-y-0.5">
                    {table.children.length === 0 ? (
                      <div className="py-0.5 px-2 text-[10px] text-muted-foreground italic">
                        No data files
                      </div>
                    ) : (
                      table.children.map((file) => (
                        <div
                          key={file.id}
                          className="flex items-center gap-1.5 py-0.5 px-1.5 rounded hover:bg-muted/40 cursor-pointer text-[11px] text-muted-foreground hover:text-foreground"
                          onClick={() =>
                            handleSelectFile(layer.name, table.name, file.name, file.id)
                          }
                        >
                          <FileSpreadsheet className="h-3 w-3 text-emerald-500 shrink-0" />
                          <span className="truncate font-mono">{file.name}</span>
                        </div>
                      ))
                    )}
                  </div>
                )}
              </div>
            );
          })
      )}
    </div>
  );

  return (
    <div className="flex flex-col h-full bg-card text-card-foreground border-b pb-2">
      {/* Lakehouse Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b bg-muted/40">
        <div className="flex items-center gap-2">
          <Layers className="h-4 w-4 text-amber-500" />
          <span className="text-xs font-semibold uppercase tracking-wider">
            Lakehouse (Cloudflare R2)
          </span>
        </div>

        <div className="flex items-center gap-1">
          {/* Refresh button */}
          <Button
            variant="ghost"
            size="icon"
            className="h-7 w-7"
            onClick={() => refreshLakehouseCatalog().catch(() => undefined)}
            disabled={isLakehouseLoading}
            title="Refresh Google Drive Lakehouse"
          >
            <RefreshCw
              className={`h-3.5 w-3.5 ${isLakehouseLoading ? "animate-spin text-amber-500" : ""}`}
            />
          </Button>

          {/* Manual Token Popover */}
          <Popover open={popoverOpen} onOpenChange={setPopoverOpen}>
            <PopoverTrigger asChild>
              <Button
                variant="ghost"
                size="icon"
                className="h-7 w-7"
                title="Set Manual Google Access Token"
              >
                <Key className="h-3.5 w-3.5" />
              </Button>
            </PopoverTrigger>
            <PopoverContent className="w-80 p-3" align="end">
              <div className="space-y-2">
                <div className="flex items-center gap-2 text-xs font-medium">
                  <Key className="h-3.5 w-3.5 text-amber-500" />
                  <span>Manual Google Access Token</span>
                </div>
                <p className="text-[11px] text-muted-foreground">
                  Paste a temporary Google OAuth 2.0 access token to authenticate Drive queries.
                </p>
                <Input
                  type="password"
                  placeholder="ya29.a0AfH6..."
                  value={manualToken}
                  onChange={(e) => setManualToken(e.target.value)}
                  className="h-8 text-xs font-mono"
                />
                <div className="flex justify-end gap-2 pt-1">
                  <Button
                    size="sm"
                    variant="outline"
                    className="h-7 text-xs"
                    onClick={() => setPopoverOpen(false)}
                  >
                    Cancel
                  </Button>
                  <Button
                    size="sm"
                    className="h-7 text-xs bg-amber-600 hover:bg-amber-700 text-white"
                    onClick={handleApplyManualToken}
                  >
                    Apply Token
                  </Button>
                </div>
              </div>
            </PopoverContent>
          </Popover>

          {/* Auth Action */}
          {googleAuth.isAuthenticated ? (
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7 text-muted-foreground hover:text-destructive"
              onClick={disconnectGoogleDrive}
              title="Disconnect Google Drive"
            >
              <LogOut className="h-3.5 w-3.5" />
            </Button>
          ) : (
            <Button
              variant="outline"
              size="sm"
              className="h-7 text-[11px] px-2 border-amber-500/40 text-amber-500 hover:bg-amber-500/10 gap-1"
              onClick={() => signInWithGoogle(true)}
              disabled={isLakehouseLoading}
            >
              <LogIn className="h-3 w-3" />
              Sign in
            </Button>
          )}
        </div>
      </div>

      <div className="px-3 py-1 text-[11px] text-muted-foreground border-b flex items-center gap-1">
        <span>Google sign-in uses read-only Google Drive access.</span>
        <a
          href={privacyUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="underline underline-offset-2 hover:text-foreground"
        >
          Privacy
        </a>
      </div>

      <div className="px-3 py-1 text-[11px] text-muted-foreground border-b flex flex-wrap items-center gap-x-1.5 gap-y-0.5">
        <span>Source access:</span>
        <a
          href={sourceAccessGuideUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="underline underline-offset-2 hover:text-foreground"
        >
          setup guide
        </a>
        <span aria-hidden="true">·</span>
        <a
          href={encryptedSecretsUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="underline underline-offset-2 hover:text-foreground"
        >
          GitHub encrypted secrets
        </a>
      </div>

      {/* Auth Status & Notification Pill */}
      <div className="px-3 py-1.5 bg-muted/20 border-b flex items-center justify-between text-[11px]">
        <div className="flex items-center gap-1.5 truncate">
          {googleAuth.isAuthenticated ? (
            <>
              <span className="h-2 w-2 rounded-full bg-emerald-500 shrink-0" />
              <span className="text-emerald-600 dark:text-emerald-400 font-medium truncate">
                Drive Connected ({googleAuth.authSource === "manual" ? "Manual" : "OAuth"})
              </span>
            </>
          ) : (
            <>
              <span className="h-2 w-2 rounded-full bg-amber-500 shrink-0" />
              <span className="text-muted-foreground truncate">Google Drive not connected</span>
            </>
          )}
        </div>

        <div className="flex items-center gap-1 min-w-0">
          {lakehouseRelease?.kind === "release" &&
            (lakehouseRelease.releases ?? [lakehouseRelease]).map((rel) => (
              <Badge
                key={rel.manifest.release_id}
                variant="secondary"
                className="text-[10px] h-4 font-mono px-1.5 shrink-0 max-w-48 truncate"
                title={`${rel.manifest.release_id} / ${rel.manifest.release_scope} / ${rel.manifest.status}`}
              >
                {rel.manifest.release_id} / {rel.manifest.release_scope} / {rel.manifest.status}
              </Badge>
            ))}
          {lakehouseRelease?.kind === "legacy" && (
            <Badge variant="secondary" className="text-[10px] h-4 font-mono px-1.5 shrink-0">
              legacy / unversioned
            </Badge>
          )}
          {activeLakehouseDataset && (
            <Badge
              variant="secondary"
              className="text-[10px] h-4 font-mono px-1.5 shrink-0"
              title="Active Layer Dataset"
            >
              {activeLakehouseDataset}
            </Badge>
          )}
        </div>
      </div>

      {/* Status or Progress Feedback */}
      {lakehouseStatusMessage && (
        <div
          role="status"
          className="px-3 py-1 bg-amber-500/10 text-amber-600 dark:text-amber-400 text-[11px] flex items-center gap-1.5 border-b"
        >
          {isLakehouseLoading && <Loader2 className="h-3 w-3 animate-spin shrink-0" />}
          <span className="break-words min-w-0">{lakehouseStatusMessage}</span>
        </div>
      )}

      {/* Lakehouse Medallion Layers Tree */}
      <div className="flex-1 overflow-y-auto px-2 py-1 space-y-0.5 text-xs">
        {googleAuth.isAuthenticated && (
          <section aria-label="Landing" className="mb-1 min-w-0">
            <div className="flex items-center gap-1 px-1.5 font-medium">
              <button
                type="button"
                className="flex min-h-11 sm:min-h-9 flex-1 items-center gap-1.5 text-left"
                aria-expanded={nativeOpen}
                onClick={() => setNativeOpen((value) => !value)}
              >
                {nativeOpen ? (
                  <ChevronDown className="h-3.5 w-3.5" />
                ) : (
                  <ChevronRight className="h-3.5 w-3.5" />
                )}
                <Folder className="h-4 w-4 text-amber-500" />
                Landing
              </button>
              <Button
                size="icon"
                variant="ghost"
                className="h-11 w-9 sm:h-9"
                aria-label="Refresh Landing"
                disabled={nativeLoading}
                onClick={() => {
                  setFolderExpansion({ root: null, ids: new Set() });
                  setNativeDetails(null);
                  void refreshNative();
                }}
              >
                <RefreshCw className={`h-3.5 w-3.5 ${nativeLoading ? "animate-spin" : ""}`} />
              </Button>
            </div>
            {nativeOpen && (
              <div className="min-w-0 space-y-1 pl-3">
                <p className="px-2 pb-1 text-[11px] text-muted-foreground">
                  Original files, grouped by source.
                </p>
                {nativeLoading && <div role="status">Finding Landing folder…</div>}
                {nativeError && (
                  <div role="alert" className="text-destructive">
                    {nativeError}{" "}
                    <Button size="sm" variant="outline" onClick={() => void refreshNative()}>
                      Retry
                    </Button>
                  </div>
                )}
                {nativeRoot && renderNativeFolder(nativeRoot.id)}
                {nativeMetadataError && (
                  <div role="alert" className="px-2 text-destructive">
                    {nativeMetadataError}
                  </div>
                )}
                <details className="px-2 pt-1 text-[11px] text-muted-foreground">
                  <summary className="cursor-pointer py-2">About these files</summary>
                  <p className="pb-2">
                    Browse the original files currently stored in Drive. Open or download them
                    through Drive. Supported files up to {formatBytes(NATIVE_PREVIEW_LIMIT_BYTES)}{" "}
                    can be previewed in SQL. Each preview replaces the previous one and is cleared
                    on refresh or disconnect. A folder listing does not establish complete source
                    coverage. Query file metadata on a source folder to scan its full nested tree
                    into a browser-local SQL table. A row describes one original file; Drive
                    creation/modification dates are not provider refresh dates. Shortcuts are
                    excluded.
                  </p>
                </details>
              </div>
            )}
          </section>
        )}
        {lakehouseCatalog
          .filter((layer) => layer.name !== "01_landing")
          .map((layer) => (
            <div key={layer.name} className="select-none">
              {/* Layer Row */}
              <button
                type="button"
                aria-label={layer.name}
                aria-expanded={layer.expanded}
                className={`flex w-full items-center gap-1.5 py-1 px-1.5 rounded text-left hover:bg-muted/70 cursor-pointer ${
                  layer.expanded ? "font-medium" : "text-muted-foreground"
                }`}
                onClick={() => toggleLakehouseLayer(layer.name)}
              >
                {layer.expanded ? (
                  <ChevronDown className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                ) : (
                  <ChevronRight className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                )}
                <Folder className="h-3.5 w-3.5 text-amber-500 shrink-0" />
                <span className="truncate">{layer.name}</span>
                {layer.children.length > 0 && (
                  <span className="ml-auto text-[10px] text-muted-foreground font-mono">
                    {
                      layer.children.filter(
                        (table) => !table.name.startsWith("gus_dbw_observations__indicator_")
                      ).length
                    }
                  </span>
                )}
              </button>

              {/* Datasets / Tables in Layer */}
              {layer.expanded && (
                <>
                  {layer.name === "02_bronze" && dbwSearch}
                  {renderTables(layer)}
                </>
              )}
            </div>
          ))}
      </div>
    </div>
  );
}
