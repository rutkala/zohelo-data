// TreeNode.tsx
import React, { useState, useCallback, useMemo, useEffect } from "react";
import {
  ChevronRight,
  ChevronDown,
  Database,
  Eye,
  Table,
  Copy,
  FileInput,
  FileSpreadsheet,
  Trash,
  TerminalIcon,
  Loader2,
} from "lucide-react";
import {
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuTrigger,
} from "@/components/ui/context-menu";
import { toast } from "sonner";
import { useDuckStore, type ColumnStats } from "@/store";
import { getUiConfig } from "@/lib/appConfig";
import { qualifyTable } from "@/lib/sqlSanitize";
import {
  queueSqlInsert,
  selectAllFromRelation,
  setSqlRelationDragData,
} from "@/lib/sqlTableActions";
import { ColumnNode } from "./ColumnNode";
import { TableActions, type AdditionalTableAction } from "./TableActions";

export interface TreeNodeData {
  /** Display-only label. Actions always use `name`, the real catalog identifier. */
  label?: string;
  /** Brief display-only context for a catalog, table, or view. */
  description?: string;
  name: string;
  type: "database" | "table" | "view";
  /** Schema the table lives in. Anything but "main" is shown as a prefix. */
  schema?: string;
  children?: TreeNodeData[];
  query?: string;
}

interface TreeNodeProps {
  node: TreeNodeData;
  level: number;
  searchTerm: string;
  parentDatabaseName?: string;
  refreshData: () => void;
  onSqlAction?: () => void;
}

const TreeNode: React.FC<TreeNodeProps> = React.memo(
  ({ node, level, searchTerm, parentDatabaseName, refreshData, onSqlAction }) => {
    const [isOpen, setIsOpen] = useState(false);
    const [columnStats, setColumnStats] = useState<ColumnStats[]>([]);
    const [isLoadingStats, setIsLoadingStats] = useState(false);
    const toggleOpen = useCallback(() => setIsOpen((open) => !open), []);

    const createTab = useDuckStore((s) => s.createTab);
    const executeQuery = useDuckStore((s) => s.executeQuery);
    const activeTabId = useDuckStore((s) => s.activeTabId);
    const activeTabType = useDuckStore((s) => s.tabs.find((tab) => tab.id === s.activeTabId)?.type);
    const deleteTable = useDuckStore((s) => s.deleteTable);
    const fetchDatabasesAndTablesInfo = useDuckStore((s) => s.fetchDatabasesAndTablesInfo);
    const fetchTableColumnStats = useDuckStore((s) => s.fetchTableColumnStats);

    // Fetch column stats when table is expanded
    useEffect(() => {
      const loadColumnStats = async () => {
        if (isOpen && node.type === "table" && parentDatabaseName && columnStats.length === 0) {
          setIsLoadingStats(true);
          try {
            const stats = await fetchTableColumnStats(parentDatabaseName, node.name, node.schema);
            setColumnStats(stats);
          } catch (error) {
            console.error("Failed to fetch column stats:", error);
          } finally {
            setIsLoadingStats(false);
          }
        }
      };

      loadColumnStats();
    }, [
      isOpen,
      node.type,
      node.name,
      node.schema,
      parentDatabaseName,
      columnStats.length,
      fetchTableColumnStats,
    ]);

    const getIcon = useMemo(() => {
      switch (node.type) {
        case "database":
          return <Database className="mr-2 h-4 w-4 shrink-0" />;
        case "table":
          return <Table className="mr-2 h-4 w-4 shrink-0" />;
        case "view":
          return <Eye className="mr-2 h-4 w-4 shrink-0" />;
        default:
          return null;
      }
    }, [node.type]);

    const relation = useMemo(
      () =>
        node.type !== "database" && parentDatabaseName
          ? qualifyTable(parentDatabaseName, node.schema, node.name)
          : null,
      [node.type, node.name, node.schema, parentDatabaseName]
    );

    const handleQueryData = useCallback(
      (databaseName: string, tableName: string, schema?: string) => () => {
        const query = selectAllFromRelation(qualifyTable(databaseName, schema, tableName));
        createTab("sql", query, tableName);
        onSqlAction?.();
      },
      [createTab, onSqlAction]
    );

    const handleInsert = useCallback(() => {
      if (!relation) return;
      if (!activeTabId || activeTabType !== "sql") {
        toast.info("Open a SQL tab first, then insert the table name.");
        return;
      }
      onSqlAction?.();
      queueSqlInsert({ sql: relation, tabId: activeTabId });
    }, [relation, activeTabId, activeTabType, onSqlAction]);

    const handleCopy = useCallback(async () => {
      if (!relation) return;
      try {
        await navigator.clipboard.writeText(relation);
        toast.success("Copied quoted relation name");
      } catch {
        toast.error("Could not copy the relation name");
      }
    }, [relation]);

    const handleDeleteTable = useCallback(
      (databaseName: string, tableName: string, schema?: string) => async () => {
        try {
          await deleteTable(tableName, databaseName, schema);
          toast.success(`Table "${tableName}" deleted successfully.`);
          await fetchDatabasesAndTablesInfo();
          refreshData();
        } catch (error) {
          toast.error(
            `Failed to delete table "${tableName}": ${
              error instanceof Error ? error.message : "Unknown error"
            }`
          );
        }
      },
      [deleteTable, fetchDatabasesAndTablesInfo, refreshData]
    );

    const handleShowSchema = useCallback(
      (databaseName: string, tableName: string, schema?: string) => async () => {
        const query = `DESCRIBE ${qualifyTable(databaseName, schema, tableName)}`;

        const tabId = createTab("sql", query, `${tableName} Schema`);
        if (tabId) {
          await executeQuery(query, tabId);
        }
        toast.success(`Showing schema for table "${tableName}"`);
      },
      [createTab, executeQuery]
    );

    const contextMenuOptions = useMemo(() => {
      if (node.type === "database") return [];
      const actions = [
        {
          label: "Query as SELECT",
          icon: <TerminalIcon className="w-4 h-4 mr-2" />,
          action: parentDatabaseName
            ? handleQueryData(parentDatabaseName, node.name, node.schema)
            : () => {
                toast.error("Parent database name is undefined.");
              },
        },
        {
          label: "Insert in SQL editor",
          icon: <FileInput className="w-4 h-4 mr-2" />,
          action: handleInsert,
        },
        {
          label: "Copy quoted name",
          icon: <Copy className="w-4 h-4 mr-2" />,
          action: () => void handleCopy(),
        },
        {
          label: "Show Schema",
          icon: <FileSpreadsheet className="w-4 h-4 mr-2" />,
          action: parentDatabaseName
            ? handleShowSchema(parentDatabaseName, node.name, node.schema)
            : () => {
                toast.error("Parent database name is undefined.");
              },
        },
      ];
      if (node.type === "table" && !getUiConfig().readOnly) {
        actions.push({
          label: "Delete Table",
          icon: <Trash className="w-4 h-4 mr-2" />,
          action: parentDatabaseName
            ? handleDeleteTable(parentDatabaseName, node.name, node.schema)
            : () => {
                toast.error("Parent database name is undefined.");
              },
        });
      }
      return actions;
    }, [
      parentDatabaseName,
      node.type,
      node.name,
      node.schema,
      handleQueryData,
      handleDeleteTable,
      handleShowSchema,
      handleInsert,
      handleCopy,
    ]);

    const additionalActions = useMemo<AdditionalTableAction[]>(() => {
      if (!parentDatabaseName || node.type === "database") return [];
      const actions: AdditionalTableAction[] = [
        {
          key: "schema",
          label: "Show Schema",
          icon: <FileSpreadsheet className="h-4 w-4" />,
          action: handleShowSchema(parentDatabaseName, node.name, node.schema),
        },
      ];
      if (node.type === "table" && !getUiConfig().readOnly) {
        actions.push({
          key: "delete",
          label: "Delete Table",
          icon: <Trash className="h-4 w-4" />,
          action: handleDeleteTable(parentDatabaseName, node.name, node.schema),
          destructive: true,
          separated: true,
        });
      }
      return actions;
    }, [
      parentDatabaseName,
      node.type,
      node.name,
      node.schema,
      handleShowSchema,
      handleDeleteTable,
    ]);

    const matchesSearch = node.name.toLowerCase().includes(searchTerm.toLowerCase());
    const childrenMatchSearch = node.children?.some(
      (child) =>
        child.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
        child.children?.some((grandchild) =>
          grandchild.name.toLowerCase().includes(searchTerm.toLowerCase())
        )
    );

    const shouldRender = !searchTerm || matchesSearch || childrenMatchSearch;

    return shouldRender ? (
      <>
        <ContextMenu>
          <ContextMenuTrigger>
            <div
              role="treeitem"
              aria-expanded={node.children ? isOpen : undefined}
              aria-level={level + 1}
              tabIndex={0}
              draggable={relation !== null}
              onDragStart={(event) => {
                if (relation) setSqlRelationDragData(event.dataTransfer, relation);
              }}
              className={`group flex items-center py-1 px-2 hover:bg-secondary hover:rounded-md cursor-pointer truncate
              ${level > 0 ? "ml-4" : ""}`}
              onClick={toggleOpen}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  toggleOpen();
                }
              }}
            >
              <div className="flex min-w-0 flex-1 items-center">
                {node.children ? (
                  isOpen ? (
                    <ChevronDown className="w-4 h-4 mr-1" />
                  ) : (
                    <ChevronRight className="w-4 h-4 mr-1" />
                  )
                ) : (
                  <div className="w-6 mr-1" />
                )}
                {getIcon}
                <div className="min-w-0 flex-1 text-xs">
                  <p className="truncate" title={node.description ?? node.name}>
                    {" "}
                    {node.type === "database" ? (
                      (node.label ?? node.name)
                    ) : node.schema && node.schema !== "main" ? (
                      <>
                        <span className="text-muted-foreground">{node.schema}.</span>
                        {node.name}
                      </>
                    ) : (
                      node.name
                    )}
                  </p>
                </div>
              </div>
              {relation && (
                <TableActions
                  relation={relation}
                  displayName={node.name}
                  additionalActions={additionalActions}
                  onSqlAction={onSqlAction}
                />
              )}
            </div>
          </ContextMenuTrigger>
          <ContextMenuContent>
            {contextMenuOptions.map((option) => (
              <ContextMenuItem key={option.label} onSelect={option.action}>
                {option.icon}
                {option.label}
              </ContextMenuItem>
            ))}
          </ContextMenuContent>
          {(isOpen || searchTerm) && node.children && (
            <div>
              {node.children.length > 0 ? (
                node.children.map((child) => (
                  <TreeNode
                    key={`${node.type === "database" ? node.name : parentDatabaseName}-${child.schema || "main"}-${child.name}`}
                    node={child}
                    level={level + 1}
                    searchTerm={searchTerm}
                    parentDatabaseName={node.type === "database" ? node.name : parentDatabaseName}
                    refreshData={refreshData}
                    onSqlAction={onSqlAction}
                  />
                ))
              ) : (
                <div className="ml-6 pl-4 text-xs italic text-muted-foreground">
                  Nothing to show
                </div>
              )}
            </div>
          )}

          {/* Render column stats for tables */}
          {isOpen && node.type === "table" && (
            <div>
              {isLoadingStats ? (
                <div className="ml-8 flex items-center gap-2 py-2 text-xs text-muted-foreground">
                  <Loader2 className="w-3 h-3 animate-spin" />
                  <span>Loading column statistics...</span>
                </div>
              ) : columnStats.length > 0 ? (
                columnStats.map((stats) => (
                  <ColumnNode
                    key={stats.column_name}
                    stats={stats}
                    databaseName={parentDatabaseName}
                    tableName={node.name}
                    schema={node.schema}
                  />
                ))
              ) : null}
            </div>
          )}
        </ContextMenu>
      </>
    ) : null;
  }
);

export default TreeNode;
