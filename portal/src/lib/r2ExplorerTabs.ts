export interface R2ExplorerCrumb {
  id: string;
  name: string;
  path: string;
}

interface R2ExplorerTabState {
  version: 1;
  crumbs: R2ExplorerCrumb[];
}

export const encodeR2ExplorerTabState = (crumbs: readonly R2ExplorerCrumb[]): string =>
  JSON.stringify({ version: 1, crumbs } satisfies R2ExplorerTabState);

export const decodeR2ExplorerTabState = (
  content: string | { database?: string; table?: string } | undefined
): R2ExplorerTabState => {
  if (typeof content !== "string" || !content.trim()) return { version: 1, crumbs: [] };
  try {
    const value = JSON.parse(content) as Partial<R2ExplorerTabState>;
    if (value.version !== 1 || !Array.isArray(value.crumbs)) return { version: 1, crumbs: [] };
    const crumbs = value.crumbs.filter(
      (crumb): crumb is R2ExplorerCrumb =>
        !!crumb &&
        typeof crumb.id === "string" &&
        typeof crumb.name === "string" &&
        typeof crumb.path === "string"
    );
    return { version: 1, crumbs };
  } catch {
    return { version: 1, crumbs: [] };
  }
};

export const r2ExplorerTitle = (crumbs: readonly R2ExplorerCrumb[]): string => {
  const current = crumbs[crumbs.length - 1];
  return current ? current.name.replace(/^0\d_/, "") : "R2 Explorer";
};
