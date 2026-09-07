import { useEffect } from "react";
import { useSearchParams } from "react-router";
import { useDuckStore } from "@/store";

/** Open a business page after the profile loads; never execute queries or load data. */
export function useWorkspaceViewFromURL() {
  const [params, setParams] = useSearchParams();
  const profileId = useDuckStore((state) => state.currentProfileId);

  useEffect(() => {
    const view = params.get("view");
    if (!profileId || (view !== "review" && view !== "catalog")) return;
    const state = useDuckStore.getState();
    const existing = state.tabs.find((tab) => tab.type === view);
    if (existing) state.setActiveTab(existing.id);
    else state.createTab(view, "", view === "review" ? "Review & decisions" : "Business catalogue");
    const remaining = new URLSearchParams(params);
    remaining.delete("view");
    setParams(remaining, { replace: true });
  }, [params, setParams, profileId]);
}
