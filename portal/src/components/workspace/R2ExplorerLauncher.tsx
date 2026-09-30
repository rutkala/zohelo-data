import { useEffect, useState } from "react";
import { Folder, Maximize2 } from "lucide-react";
import { useDuckStore } from "@/store";
import { Button } from "@/components/ui/button";
import {
  DRIVE_ROOT,
  findFolderIdByName,
  listNativeChildrenPage,
  type DriveFileMetadata,
} from "@/services/googleDrive";
import {
  decodeR2ExplorerTabState,
  encodeR2ExplorerTabState,
  type R2ExplorerCrumb,
} from "@/lib/r2ExplorerTabs";

const FOLDER_MIME = "application/vnd.google-apps.folder";
const visibleLayers = new Set(["01_landing", "02_bronze", "03_silver", "04_gold", "05_archive"]);
const labels: Record<string, string> = {
  "01_landing": "Landing",
  "02_bronze": "Bronze",
  "03_silver": "Silver",
  "04_gold": "Gold",
  "05_archive": "Archive",
};

export default function R2ExplorerLauncher() {
  const googleAuth = useDuckStore((state) => state.googleAuth);
  const signInWithGoogle = useDuckStore((state) => state.signInWithGoogle);
  const createTab = useDuckStore((state) => state.createTab);
  const tabs = useDuckStore((state) => state.tabs);
  const setActiveTab = useDuckStore((state) => state.setActiveTab);
  const [folders, setFolders] = useState<DriveFileMetadata[]>([]);
  const [error, setError] = useState<string | null>(null);

  const openExplorer = (crumbs: R2ExplorerCrumb[], title: string) => {
    const path = crumbs[crumbs.length - 1]?.path ?? "";
    const existing = tabs.find((tab) => {
      if (tab.type !== "explorer") return false;
      const decoded = decodeR2ExplorerTabState(tab.content);
      return (decoded.crumbs[decoded.crumbs.length - 1]?.path ?? "") === path;
    });
    if (existing) {
      setActiveTab(existing.id);
      return;
    }
    createTab("explorer", encodeR2ExplorerTabState(crumbs), title);
  };

  useEffect(() => {
    if (!googleAuth.token) {
      setFolders([]);
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        setError(null);
        const rootId = await findFolderIdByName(DRIVE_ROOT, "root", googleAuth.token!);
        if (!rootId) throw new Error("R2 lakehouse root was not found.");
        const page = await listNativeChildrenPage(rootId, googleAuth.token!);
        if (!cancelled) {
          setFolders(
            page.files.filter(
              (entry) => entry.mimeType === FOLDER_MIME && visibleLayers.has(entry.name)
            )
          );
        }
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [googleAuth.token]);

  return (
    <section className="rounded-lg border bg-card p-5" aria-label="R2 lakehouse">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold">R2 Lakehouse</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            Open a layer in the full workspace explorer.
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => openExplorer([], "R2 Explorer")}
          disabled={!googleAuth.isAuthenticated}
        >
          <Maximize2 className="mr-2 h-4 w-4" />
          Open explorer
        </Button>
      </div>

      {!googleAuth.isAuthenticated ? (
        <Button className="mt-4" onClick={() => signInWithGoogle(true)}>
          Sign in to browse
        </Button>
      ) : (
        <div className="mt-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-5">
          {folders.map((folder) => {
            const crumb: R2ExplorerCrumb = {
              id: folder.id,
              name: folder.name,
              path: folder.name,
            };
            return (
              <button
                key={folder.id}
                type="button"
                className="flex items-center gap-2 rounded-md border px-3 py-3 text-left text-sm hover:bg-accent"
                onClick={() => openExplorer([crumb], labels[folder.name] ?? folder.name)}
              >
                <Folder className="h-4 w-4 shrink-0 text-amber-500" />
                <span className="truncate font-medium">{labels[folder.name] ?? folder.name}</span>
              </button>
            );
          })}
        </div>
      )}

      {error && <p className="mt-3 text-xs text-destructive">{error}</p>}
    </section>
  );
}
