import { useState } from "react";
import { toast } from "sonner";
import { useDuckStore } from "@/store";
import { buildCreateViewSql } from "@/lib/createViewSql";
import { asLocalDuckSession } from "@/services/engine";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

export default function CreateViewDialog({
  open,
  onOpenChange,
  sql,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  sql: string;
}) {
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);
  const [previousOpen, setPreviousOpen] = useState(open);
  if (open !== previousOpen) {
    setPreviousOpen(open);
    if (open) {
      setName("");
      setError(null);
      setCreating(false);
    }
  }

  const create = async () => {
    if (creating) return;
    if (!asLocalDuckSession(useDuckStore.getState().currentSession)) {
      setError("Connect to Browser workspace before creating a session view.");
      return;
    }
    let statement: string;
    try {
      statement = buildCreateViewSql(name, sql);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Enter a name and a single SELECT query.");
      return;
    }
    setCreating(true);
    // Keep the definition and any execution error inspectable in its own tab.
    // CREATE (without OR REPLACE) protects existing workspace relations.
    const store = useDuckStore.getState();
    const tabId = store.createTab("sql", statement, `Create view: ${name.trim()}`);
    onOpenChange(false);
    await store.executeQuery(statement, tabId);
    const result = useDuckStore.getState().tabs.find((tab) => tab.id === tabId)?.result;
    if (result && !result.error)
      toast.success(`View main.${name.trim()} created in Browser workspace`);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-[calc(100vw-2rem)] rounded-lg sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Create a view</DialogTitle>
          <DialogDescription>
            Give this query a name so you can query it from other tabs. The view lives in Browser
            workspace for this database session. Save its SQL to recreate it later.
          </DialogDescription>
        </DialogHeader>
        <form
          onSubmit={(event) => {
            event.preventDefault();
            void create();
          }}
          className="space-y-4"
        >
          <div className="space-y-2">
            <Label htmlFor="new-view-name">View name</Label>
            <Input
              id="new-view-name"
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder="my_fx_quotes"
              autoFocus
              autoComplete="off"
            />
            <p className="text-xs text-muted-foreground">
              Schema: main. Existing tables and views are kept.
            </p>
          </div>
          {error && (
            <p role="alert" className="text-sm text-destructive">
              {error}
            </p>
          )}
          <DialogFooter className="gap-2">
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={!name.trim() || creating}>
              Create view
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
