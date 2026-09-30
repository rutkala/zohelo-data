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
import type { R2ExplorerCrumb } from "@/lib/r2ExplorerTabs";

const FOLDER_MIME = "application/vnd.google-apps.folder";

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

interface R2LakehouseBrowserProps {
  initialCrumbs?: readonly R2ExplorerCrumb[];
}

export default function R2LakehouseBrowser({
  initialCrumbs = [],
}: R2LakehouseBrowserProps) {
  const googleAuth = useDuckStore((state) => state.googleAuth);
  const signInWithGoogle = useDuckStore((state) => state.signInWithGoogle);

  const [crumbs, setCrumbs] = useState<R2ExplorerCrumb[]>([]);
  const [entries, setEntries] = useState<DriveFileMetadata[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");

  const initialKey = useMemo(() => JSON.stringify(initialCrumbs), [initialCrumbs]);
  const current = crumbs[crumbs.length - 1] ?? null;
  const currentPath = current?.path ?? "";
  const currentBucket = bucketForPath(currentPath);

  const visibleEntries = useMemo(() => {
    const needle = filter.trim().toLocaleLowerCase();
    if (!needle) return entries;
    return entries.filter((entry) => entry.name.toLocaleLowerCase().includes(needle));
  }, [entries, filter]);

  const loadFolder = async (folderId: string, nextCrumbs: R2ExplorerCrumb[]) => {
    if (!googleAuth.token) return;
    setLoading(true);
    setError(null);
    try {
      setEntries(await fetchAllChildren(folderId, googleAuth.token));
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
      setEntries(await fetchAllChildren(rootId, googleAuth.token));
      setCrumbs([]);
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
    const requested = initialCrumbs[initialCrumbs.length - 1];
    if (requested) void loadFolder(requested.id, [...initialCrumbs]);
    else void loadRoot();
    // initialKey deliberately represents the immutable tab location.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [googleAuth.token, initialKey]);

  const openFolder = (entry: DriveFileMetadata) => {
    if (entry.mimeType !== FOLDER_MIME) return;
    const path = currentPath ? `${currentPath}/${entry.name}` : entry.name;
    const crumb: R2ExplorerCrumb = { id: entry.id, name: entry.name, path };
    void loadFolder(entry.id, [...crumbs, crumb]);
  };

  const goToCrumb = (index: number) => {
    const crumb = crumbs[index];
    if (!crumb) return;
    void loadFolder(crumb.id, crumbs.slice(0, index + 1));
  };

  const openObject = async (entry: DriveFileMetadata, mode: "open" | "download") => {
    if (!googleAuth.token) return;
    const tab = window.open("about:blank", "_blank");
    if (!tab) return;
    tab.opener = null;
    try {
      tab.location.replace(await getR2ObjectUrl(entry.id, googleAuth.token, mode));
    } catch (err) {
      tab.close();
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  if (!googleAuth.isAuthenticated) {
    return (
      <div className="flex h-full items-center justify-center p-8">
        <div className="max-w-md rounded-lg border bg-card p-6 text-center">
          <Folder className="mx-auto mb-3 h-10 w-10 text-amber-500" />
          <h2 className="text-lg font-semibold">R2 Lakehouse Explorer</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            Sign in to browse the private Cloudflare R2 lakehouse.
          </p>
          <Button className="mt-4" onClick={() => signInWithGoogle(true)}>
            Sign in
          </Button>
        </div>
      </div>
    );
  }

  return (
    <section className="flex h-full min-h-0 flex-col bg-card" aria-label="R2 lakehouse explorer">
      <div className="shrink-0 border-b px-5 py-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <div className="flex items-center gap-2">
              <Folder className="h-5 w-5 text-emerald-600" />
              <h2 className="text-xl font-semibold">Files</h2>
            </div>
            <p className="mt-1 text-sm text-muted-foreground">
              Cloudflare R2 lakehouse
            </p>
          </div>
          <Button variant="outline" size="sm" onClick={() => current ? void loadFolder(current.id, crumbs) : void loadRoot()} disabled={loading}>
            <RefreshCw className={`mr-2 h-4 w-4 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
        </div>

        <div className="mt-4 flex min-w-0 flex-wrap items-center gap-1 text-sm">
          <button
            type="button"
            className="rounded px-1.5 py-1 hover:bg-muted"
            onClick={() => void loadRoot()}
            title={DRIVE_ROOT}
          >
            <Home className="h-4 w-4" />
          </button>
          {crumbs.map((crumb, index) => (
            <div key={crumb.id} className="flex min-w-0 items-center gap-1">
              <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />
              <button
                type="button"
                className="max-w-64 truncate rounded px-1.5 py-1 hover:bg-muted"
                onClick={() => goToCrumb(index)}
                title={crumb.path}
              >
                {layerLabels[crumb.name] ?? crumb.name}
              </button>
            </div>
          ))}
        </div>

        <div className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-1 rounded bg-muted/40 px-3 py-2 text-xs text-muted-foreground">
          {currentBucket && (
            <span>
              <span className="font-medium text-foreground">Bucket:</span> {currentBucket}
            </span>
          )}
          <span className="min-w-0 break-all font-mono">
            <span className="font-sans font-medium text-foreground">Path:</span>{" "}
            {currentPath || "/"}
          </span>
          {currentPath && (
            <button
              type="button"
              className="inline-flex items-center gap-1 hover:text-foreground"
              onClick={() => void navigator.clipboard?.writeText(currentPath)}
            >
              <Copy className="h-3.5 w-3.5" />
              Copy path
            </button>
          )}
        </div>

        <Input
          value={filter}
          onChange={(event) => setFilter(event.target.value)}
          placeholder="Filter this folder…"
          className="mt-3 max-w-lg"
        />
      </div>

      {error && (
        <div className="shrink-0 border-b bg-destructive/10 px-5 py-3 text-sm text-destructive">
          {error}
        </div>
      )}

      <div className="min-h-0 flex-1 overflow-auto">
        <div className="sticky top-0 z-10 grid min-w-[760px] grid-cols-[minmax(340px,1fr)_170px_140px_170px] gap-3 border-b bg-muted/70 px-5 py-2 text-xs font-medium text-muted-foreground backdrop-blur">
          <div>Name</div>
          <div>Bucket</div>
          <div>Size / type</div>
          <div className="text-right">Actions</div>
        </div>

        {loading && entries.length === 0 ? (
          <div className="flex h-full min-h-72 items-center justify-center text-muted-foreground">
            <Loader2 className="mr-2 h-5 w-5 animate-spin" />
            Loading R2 folder…
          </div>
        ) : visibleEntries.length === 0 ? (
          <div className="flex min-h-48 items-center justify-center text-sm text-muted-foreground">
            {filter ? "No matching items." : "This folder is empty."}
          </div>
        ) : (
          <div className="divide-y">
            {visibleEntries.map((entry) => {
              const folder = entry.mimeType === FOLDER_MIME;
              const logicalPath = currentPath ? `${currentPath}/${entry.name}` : entry.name;
              const bucket = bucketForPath(logicalPath);
              return (
                <div
                  key={entry.id}
                  className={`grid min-w-[760px] grid-cols-[minmax(340px,1fr)_170px_140px_170px] items-center gap-3 px-5 py-2.5 text-sm ${folder ? "cursor-pointer hover:bg-accent/50" : "hover:bg-muted/30"}`}
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
