import { useEffect, useMemo, useState } from "react";
import {
  ChevronRight,
  Copy,
  Download,
  ExternalLink,
  File,
  Folder,
  Home,
  Loader2,
  RefreshCw,
  X,
} from "lucide-react";
import { useDuckStore } from "@/store";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  DRIVE_ROOT,
  findFolderIdByName,
  getR2ObjectUrl,
  listNativeChildrenPage,
  type DriveFileMetadata,
} from "@/services/googleDrive";

const FOLDER_MIME = "application/vnd.google-apps.folder";

type Crumb = {
  id: string;
  name: string;
  path: string;
};

const layerLabels: Record<string, string> = {
  "01_landing": "Landing",
  "02_bronze": "Bronze",
  "03_silver": "Silver",
  "04_gold": "Gold",
  "05_archive": "Archive",
  "06_control": "Control",
};

const formatBytes = (bytes?: number) => {
  if (bytes === undefined) return "";
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let value = bytes / 1024;
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return `${value.toFixed(value >= 10 ? 1 : 2)} ${units[index]}`;
};

const bucketForPath = (path: string) => {
  const root = path.split("/")[0] || "";
  if (root === "01_landing" || root === "05_archive") return "zohelo-landing-prod";
  if (root) return "zohelo-lakehouse-prod";
  return "";
};

const fetchAllChildren = async (folderId: string, token: string) => {
  const files: DriveFileMetadata[] = [];
  let pageToken: string | undefined;
  do {
    const page = await listNativeChildrenPage(folderId, token, pageToken);
    files.push(...page.files);
    pageToken = page.nextPageToken ?? undefined;
  } while (pageToken);
  return files.sort((left, right) => {
    const leftFolder = left.mimeType === FOLDER_MIME ? 0 : 1;
    const rightFolder = right.mimeType === FOLDER_MIME ? 0 : 1;
    if (leftFolder !== rightFolder) return leftFolder - rightFolder;
    return left.name.localeCompare(right.name);
  });
};

type R2LakehouseBrowserProps = {
  fullScreen?: boolean;
  onEnterFullScreen?: () => void;
  onExitFullScreen?: () => void;
};

export default function R2LakehouseBrowser({
  fullScreen = false,
  onEnterFullScreen,
  onExitFullScreen,
}: R2LakehouseBrowserProps) {
  const googleAuth = useDuckStore((state) => state.googleAuth);
  const signInWithGoogle = useDuckStore((state) => state.signInWithGoogle);

  const [crumbs, setCrumbs] = useState<Crumb[]>([]);
  const [entries, setEntries] = useState<DriveFileMetadata[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");

  const current = crumbs[crumbs.length - 1] ?? null;
  const currentBucket = current ? bucketForPath(current.path) : "";

  const visibleEntries = useMemo(() => {
    const needle = filter.trim().toLocaleLowerCase();
    if (!needle) return entries;
    return entries.filter((entry) => entry.name.toLocaleLowerCase().includes(needle));
  }, [entries, filter]);

  const loadFolder = async (folder: Crumb, nextCrumbs: Crumb[]) => {
    if (!googleAuth.token) return;
    setLoading(true);
    setError(null);
    try {
      const next = await fetchAllChildren(folder.id, googleAuth.token);
      setEntries(next);
      setCrumbs(nextCrumbs);
      setFilter("");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  };

  const loadRoot = async () => {
    if (!googleAuth.token) return;
    setLoading(true);
    setError(null);
    try {
      const rootId = await findFolderIdByName(DRIVE_ROOT, "root", googleAuth.token);
      if (!rootId) throw new Error("R2 lakehouse root was not found.");
      const root: Crumb = { id: rootId, name: DRIVE_ROOT, path: "" };
      const next = await fetchAllChildren(rootId, googleAuth.token);
      setCrumbs([root]);
      setEntries(next);
      setFilter("");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!googleAuth.token) {
      setCrumbs([]);
      setEntries([]);
      setError(null);
      return;
    }
    void loadRoot();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [googleAuth.token]);

  const openFolder = (entry: DriveFileMetadata) => {
    if (!current || entry.mimeType !== FOLDER_MIME) return;
    onEnterFullScreen?.();
    const path = current.path ? `${current.path}/${entry.name}` : entry.name;
    const crumb = { id: entry.id, name: entry.name, path };
    void loadFolder(crumb, [...crumbs, crumb]);
  };

  const goToCrumb = (index: number) => {
    const crumb = crumbs[index];
    if (!crumb) return;
    void loadFolder(crumb, crumbs.slice(0, index + 1));
  };

  const openObject = async (entry: DriveFileMetadata, mode: "open" | "download") => {
    if (!googleAuth.token) return;
    const tab = window.open("about:blank", "_blank");
    if (!tab) return;
    tab.opener = null;
    try {
      const url = await getR2ObjectUrl(entry.id, googleAuth.token, mode);
      tab.location.replace(url);
    } catch (err) {
      tab.close();
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  if (!googleAuth.isAuthenticated) {
    return (
      <section
        className={fullScreen ? "flex h-full min-h-0 flex-col bg-card p-6" : "rounded-lg border bg-card p-6"}
        aria-label="R2 lakehouse explorer"
      >
        <div className="flex flex-col items-start gap-3">
          <div>
            <h2 className="text-lg font-semibold">R2 Lakehouse Explorer</h2>
            <p className="text-sm text-muted-foreground">
              Browse Landing, Bronze, Silver, Gold and Archive directly in Cloudflare R2.
            </p>
          </div>
          <Button onClick={() => signInWithGoogle(true)}>Sign in to browse</Button>
        </div>
      </section>
    );
  }

  return (
    <section
      className={
        fullScreen
          ? "flex h-full min-h-0 flex-col bg-card"
          : "overflow-hidden rounded-lg border bg-card"
      }
      aria-label="R2 lakehouse explorer"
    >
      {fullScreen && (
        <div className="flex h-12 shrink-0 items-center justify-between border-b px-4">
          <div className="flex items-center gap-2 font-medium">
            <Folder className="h-4 w-4 text-emerald-600" />
            <span>Files</span>
          </div>
          <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8"
            onClick={onExitFullScreen}
            title="Close explorer and return to Home"
            aria-label="Close explorer"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      )}

      <div className={`flex shrink-0 flex-col gap-3 border-b ${fullScreen ? "px-5 py-4" : "p-4"}`}>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className={fullScreen ? "text-xl font-semibold" : "text-lg font-semibold"}>
              R2 Lakehouse Explorer
            </h2>
            {!fullScreen && (
              <p className="text-sm text-muted-foreground">
                Select a folder to open the full lakehouse explorer.
              </p>
            )}
          </div>
          <Button variant="outline" size="sm" onClick={() => void loadRoot()} disabled={loading}>
            <RefreshCw className={`mr-2 h-4 w-4 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
        </div>

        <div className="flex min-w-0 flex-wrap items-center gap-1 text-sm">
          {crumbs.map((crumb, index) => (
            <div key={crumb.id} className="flex min-w-0 items-center gap-1">
              {index > 0 && <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />}
              <button
                type="button"
                className="max-w-56 truncate rounded px-1.5 py-1 hover:bg-muted"
                onClick={() => goToCrumb(index)}
                title={crumb.path || DRIVE_ROOT}
              >
                {index === 0 ? <Home className="inline h-4 w-4" /> : (layerLabels[crumb.name] ?? crumb.name)}
              </button>
            </div>
          ))}
        </div>

        {current && (
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 rounded bg-muted/40 px-3 py-2 text-xs text-muted-foreground">
            {currentBucket && (
              <span>
                <span className="font-medium text-foreground">Bucket:</span> {currentBucket}
              </span>
            )}
            <span className="min-w-0 break-all font-mono">
              <span className="font-sans font-medium text-foreground">Path:</span>{" "}
              {current.path || "/"}
            </span>
            {current.path && (
              <button
                type="button"
                className="inline-flex items-center gap-1 hover:text-foreground"
                onClick={() => void navigator.clipboard?.writeText(current.path)}
              >
                <Copy className="h-3.5 w-3.5" />
                Copy path
              </button>
            )}
          </div>
        )}

        <Input
          value={filter}
          onChange={(event) => setFilter(event.target.value)}
          placeholder="Filter this folder…"
          className="max-w-md"
        />
      </div>

      {error && (
        <div className="border-b bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {error}
        </div>
      )}

      <div className={fullScreen ? "min-h-0 flex-1 overflow-auto" : "min-h-72 max-h-[32rem] overflow-auto"}>
        {loading && entries.length === 0 ? (
          <div className={`flex items-center justify-center text-muted-foreground ${fullScreen ? "h-full min-h-72" : "min-h-72"}`}>
            <Loader2 className="mr-2 h-5 w-5 animate-spin" />
            Loading R2 folder…
          </div>
        ) : visibleEntries.length === 0 ? (
          <div className={`flex items-center justify-center text-sm text-muted-foreground ${fullScreen ? "h-full min-h-48" : "min-h-48"}`}>
            {filter ? "No matching items." : "This folder is empty."}
          </div>
        ) : (
          <div className="divide-y">
            {visibleEntries.map((entry) => {
              const folder = entry.mimeType === FOLDER_MIME;
              const logicalPath = current?.path ? `${current.path}/${entry.name}` : entry.name;
              const bucket = bucketForPath(logicalPath);
              return (
                <div
                  key={entry.id}
                  className={`grid min-w-[720px] grid-cols-[minmax(320px,1fr)_160px_140px_auto] items-center gap-3 px-5 py-2.5 text-sm ${folder ? "cursor-pointer hover:bg-accent/50" : "hover:bg-muted/30"}`}
                  onClick={() => folder && openFolder(entry)}
                >
                  <div className="flex min-w-0 items-center gap-2">
                    {folder ? (
                      <Folder className="h-4 w-4 shrink-0 text-amber-500" />
                    ) : (
                      <File className="h-4 w-4 shrink-0 text-muted-foreground" />
                    )}
                    <div className="min-w-0">
                      <div className="truncate font-medium" title={logicalPath}>
                        {layerLabels[entry.name] ?? entry.name}
                      </div>
                      <div className="truncate font-mono text-[11px] text-muted-foreground" title={logicalPath}>
                        {logicalPath}
                      </div>
                    </div>
                  </div>
                  <div className="truncate text-xs text-muted-foreground" title={bucket}>
                    {bucket}
                  </div>
                  <div className="text-xs text-muted-foreground">
                    {folder ? "Folder" : formatBytes(entry.size)}
                  </div>
                  <div className="flex justify-end gap-1" onClick={(event) => event.stopPropagation()}>
                    {!folder && (
                      <>
                        <Button variant="ghost" size="sm" onClick={() => void openObject(entry, "open")}>
                          <ExternalLink className="mr-1 h-3.5 w-3.5" />
                          Open
                        </Button>
                        <Button variant="ghost" size="sm" onClick={() => void openObject(entry, "download")}>
                          <Download className="mr-1 h-3.5 w-3.5" />
                          Download
                        </Button>
                      </>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      <div className="shrink-0 border-t px-5 py-2 text-xs text-muted-foreground">
        {visibleEntries.length.toLocaleString()} item{visibleEntries.length === 1 ? "" : "s"}
        {filter && ` of ${entries.length.toLocaleString()}`}
      </div>
    </section>
  );
}
