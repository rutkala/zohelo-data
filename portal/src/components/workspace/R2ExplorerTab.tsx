import { useMemo } from "react";
import { useDuckStore } from "@/store";
import R2LakehouseBrowser from "@/components/workspace/R2LakehouseBrowser";
import { decodeR2ExplorerTabState } from "@/lib/r2ExplorerTabs";

export default function R2ExplorerTab({ tabId }: { tabId: string }) {
  const content = useDuckStore((state) => state.tabs.find((tab) => tab.id === tabId)?.content);
  const state = useMemo(() => decodeR2ExplorerTabState(content), [content]);
  return <R2LakehouseBrowser initialCrumbs={state.crumbs} />;
}
