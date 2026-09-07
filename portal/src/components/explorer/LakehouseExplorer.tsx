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
} from "lucide-react";
import { useDuckStore } from "@/store";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { qualifyTable } from "@/lib/sqlSanitize";
import { setSqlRelationDragData } from "@/lib/sqlTableActions";
import { TableActions } from "./TableActions";

interface LakehouseExplorerProps {
  onSqlAction?: () => void;
}

export default function LakehouseExplorer({ onSqlAction }: LakehouseExplorerProps) {
  const googleAuth = useDuckStore((s) => s.googleAuth);
  const lakehouseCatalog = useDuckStore((s) => s.lakehouseCatalog);
  const lakehouseRelease = useDuckStore((s) => s.lakehouseRelease);
  const isLakehouseLoading = useDuckStore((s) => s.isLakehouseLoading);
  const lakehouseStatusMessage = useDuckStore((s) => s.lakehouseStatusMessage);
  const activeLakehouseDataset = useDuckStore((s) => s.activeLakehouseDataset);
  const activeLakehouseLayer = useDuckStore((s) => s.activeLakehouseLayer);

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
  const baseUrl = import.meta.env.BASE_URL === "./" ? "/" : (import.meta.env.BASE_URL ?? "/");
  const privacyUrl = `${baseUrl.replace(/\/$/, "")}/privacy.html`;

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

  return (
    <div className="flex flex-col h-full bg-card text-card-foreground border-b pb-2">
      {/* Lakehouse Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b bg-muted/40">
        <div className="flex items-center gap-2">
          <Layers className="h-4 w-4 text-amber-500" />
          <span className="text-xs font-semibold uppercase tracking-wider">
            Lakehouse (Google Drive)
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
          {lakehouseRelease?.kind === "release" && (
            <Badge
              variant="secondary"
              className="text-[10px] h-4 font-mono px-1.5 shrink-0 max-w-48 truncate"
              title={`${lakehouseRelease.manifest.release_id} / ${lakehouseRelease.manifest.release_scope} / ${lakehouseRelease.manifest.status}`}
            >
              {lakehouseRelease.manifest.release_id} / {lakehouseRelease.manifest.release_scope} /{" "}
              {lakehouseRelease.manifest.status}
            </Badge>
          )}
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
        {lakehouseCatalog.map((layer) => (
          <div key={layer.name} className="select-none">
            {/* Layer Row */}
            <div
              className={`flex items-center gap-1.5 py-1 px-1.5 rounded hover:bg-muted/70 cursor-pointer ${
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
                  {layer.children.length}
                </span>
              )}
            </div>

            {/* Datasets / Tables in Layer */}
            {layer.expanded && (
              <div className="ml-3 pl-2 border-l border-border/60 space-y-0.5 mt-0.5">
                {layer.children.length === 0 ? (
                  <div className="py-1 px-2 text-[11px] text-muted-foreground italic">
                    {layer.loaded ? "No datasets found" : "Click to expand & load…"}
                  </div>
                ) : (
                  layer.children.map((table) => {
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
                          onDragStart={(event) =>
                            setSqlRelationDragData(event.dataTransfer, relation)
                          }
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
                            <span className="truncate font-mono text-[11px]" title={table.name}>
                              {table.name}
                            </span>
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
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
