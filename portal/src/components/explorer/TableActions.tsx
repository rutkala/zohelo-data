import { Fragment, type ReactNode } from "react";
import { Copy, FileInput, MoreVertical, TerminalIcon } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { useDuckStore } from "@/store";
import { queueSqlInsert, selectAllFromRelation } from "@/lib/sqlTableActions";

export interface AdditionalTableAction {
  key: string;
  label: string;
  icon: ReactNode;
  action: () => void | Promise<void>;
  destructive?: boolean;
  separated?: boolean;
}

interface TableActionsProps {
  relation: string;
  displayName: string;
  additionalActions?: AdditionalTableAction[];
  onSqlAction?: () => void;
}

export function TableActions({
  relation,
  displayName,
  additionalActions = [],
  onSqlAction,
}: TableActionsProps) {
  const createTab = useDuckStore((state) => state.createTab);
  const activeTabId = useDuckStore((state) => state.activeTabId);
  const activeTabType = useDuckStore(
    (state) => state.tabs.find((tab) => tab.id === state.activeTabId)?.type
  );

  const queryAsSelect = () => {
    createTab("sql", selectAllFromRelation(relation), displayName);
    onSqlAction?.();
  };

  const insertInEditor = () => {
    if (!activeTabId || activeTabType !== "sql") {
      toast.info("Open a SQL tab first, then insert the table name.");
      return;
    }
    onSqlAction?.();
    queueSqlInsert({ sql: relation, tabId: activeTabId });
  };

  const copyName = async () => {
    try {
      await navigator.clipboard.writeText(relation);
      toast.success("Copied quoted relation name");
    } catch {
      toast.error("Could not copy the relation name");
    }
  };

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          type="button"
          size="icon"
          variant="ghost"
          className="h-9 w-9 shrink-0 md:h-6 md:w-6"
          aria-label={`Actions for ${displayName}`}
          title={`Actions for ${displayName}`}
          onClick={(event) => event.stopPropagation()}
          onKeyDown={(event) => event.stopPropagation()}
        >
          <MoreVertical className="h-4 w-4" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" onClick={(event) => event.stopPropagation()}>
        <DropdownMenuItem onSelect={queryAsSelect}>
          <TerminalIcon className="h-4 w-4" />
          Query as SELECT
        </DropdownMenuItem>
        <DropdownMenuItem onSelect={insertInEditor}>
          <FileInput className="h-4 w-4" />
          Insert in SQL editor
        </DropdownMenuItem>
        <DropdownMenuItem onSelect={() => void copyName()}>
          <Copy className="h-4 w-4" />
          Copy quoted name
        </DropdownMenuItem>
        {additionalActions.map((item) => (
          <Fragment key={item.key}>
            {item.separated && <DropdownMenuSeparator />}
            <DropdownMenuItem
              className={item.destructive ? "text-destructive focus:text-destructive" : undefined}
              onSelect={() => void item.action()}
            >
              {item.icon}
              {item.label}
            </DropdownMenuItem>
          </Fragment>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
