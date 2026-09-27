import { useEffect, useMemo, useState } from "react";
import { Bookmark, Database, Terminal } from "lucide-react";
import { formatDistanceToNow } from "date-fns";
import { useDuckStore } from "@/store";
import { Button } from "@/components/ui/button";
import { Card, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import Logo from "/logo.png";
import LogoLight from "/logo-light.png";
import {
  getSavedQueries,
  type SavedQuery,
} from "@/services/persistence/repositories/savedQueryRepository";
import PlatformInfoLinks from "@/components/PlatformInfoLinks";
import { useTheme } from "@/components/theme/theme-provider";

const HomeTab = () => {
  const createTab = useDuckStore((state) => state.createTab);
  const error = useDuckStore((state) => state.error);
  const queryHistory = useDuckStore((state) => state.queryHistory);
  const tabs = useDuckStore((state) => state.tabs);
  const setActiveTab = useDuckStore((state) => state.setActiveTab);
  const currentProfileId = useDuckStore((state) => state.currentProfileId);
  const savedQueriesVersion = useDuckStore((state) => state.savedQueriesVersion);
  const lakehouseStatusMessage = useDuckStore((state) => state.lakehouseStatusMessage);
  const { theme } = useTheme();
  const [savedQueries, setSavedQueries] = useState<SavedQuery[] | null>(null);

  const recentItems = useMemo(
    () =>
      queryHistory.slice(0, 6).map((entry) => ({
        query: entry.query,
        executedAt: entry.timestamp,
      })),
    [queryHistory]
  );

  useEffect(() => {
    if (!currentProfileId) return;
    let cancelled = false;
    getSavedQueries(currentProfileId)
      .then((queries) => {
        if (!cancelled) setSavedQueries(queries);
      })
      .catch(console.error);
    return () => {
      cancelled = true;
    };
  }, [currentProfileId, savedQueriesVersion]);

  const openCatalogue = () => {
    const existing = tabs.find((tab) => tab.type === "catalog");
    if (existing) setActiveTab(existing.id);
    else createTab("catalog", "", "Data catalogue");
  };

  return (
    <div className="flex h-full flex-col">
      <div className="mx-auto flex w-full max-w-5xl flex-1 flex-col gap-8 overflow-auto p-6 sm:p-8">
        <header className="flex items-center gap-3">
          <img src={theme === "dark" ? Logo : LogoLight} alt="Zohelo-data" className="h-10" />
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">Zohelo-data</h1>
            <p className="text-sm text-muted-foreground">
              Explore a connected data release with SQL.
            </p>
          </div>
        </header>

        <section
          className="grid gap-4 rounded-lg border bg-card p-5 sm:grid-cols-[1fr_auto] sm:items-center"
          aria-label="Zohelo-data workspace"
        >
          <div>
            <h2 className="text-lg font-semibold">SQL workspace</h2>
            <p className="mt-1 text-sm text-muted-foreground">
              Open a query, inspect its results, and use the data catalogue to find published
              tables.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button onClick={() => createTab("sql", "")}>
              <Terminal className="mr-2 h-4 w-4" />
              New SQL query
            </Button>
            <Button variant="outline" onClick={openCatalogue}>
              <Database className="mr-2 h-4 w-4" />
              Data catalogue
            </Button>
          </div>
          {lakehouseStatusMessage && (
            <p role="status" className="text-xs text-muted-foreground sm:col-span-2">
              {lakehouseStatusMessage}
            </p>
          )}
        </section>

        <Tabs defaultValue="recent" className="space-y-4">
          <TabsList>
            <TabsTrigger value="recent">Recent SQL</TabsTrigger>
            <TabsTrigger value="saved" className="flex items-center gap-2">
              <Bookmark className="h-3.5 w-3.5" />
              Saved SQL
            </TabsTrigger>
          </TabsList>

          <TabsContent value="recent">
            {error ? (
              <Card className="p-4 text-center text-muted-foreground">{error}</Card>
            ) : recentItems.length === 0 ? (
              <Card className="border-dashed p-8 text-center text-muted-foreground">
                No recent SQL queries. Start with a new query when you are ready.
              </Card>
            ) : (
              <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                {recentItems.map((item, index) => (
                  <Card
                    key={`${item.executedAt}-${index}`}
                    className="cursor-pointer transition-colors hover:bg-accent/50"
                    onClick={() => createTab("sql", item.query)}
                  >
                    <CardHeader>
                      <CardTitle className="flex items-center gap-2 text-sm">
                        <Terminal className="h-4 w-4 text-primary" />
                        SQL query
                      </CardTitle>
                      <CardDescription className="truncate font-mono text-xs">
                        {item.query}
                      </CardDescription>
                    </CardHeader>
                    <CardFooter className="text-xs text-muted-foreground">
                      {new Intl.DateTimeFormat("en-US", {
                        day: "numeric",
                        month: "short",
                        year: "numeric",
                        hour: "2-digit",
                        minute: "2-digit",
                      }).format(new Date(item.executedAt))}
                    </CardFooter>
                  </Card>
                ))}
              </div>
            )}
          </TabsContent>

          <TabsContent value="saved">
            {savedQueries === null ? (
              <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                {[1, 2, 3].map((item) => (
                  <Card key={item}>
                    <CardHeader>
                      <Skeleton className="h-4 w-40" />
                      <Skeleton className="h-4 w-full" />
                    </CardHeader>
                  </Card>
                ))}
              </div>
            ) : savedQueries.length === 0 ? (
              <Card className="border-dashed p-8 text-center text-muted-foreground">
                No saved SQL queries yet. Save one from the editor toolbar.
              </Card>
            ) : (
              <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                {savedQueries.map((query) => (
                  <Card
                    key={query.id}
                    className="cursor-pointer transition-colors hover:bg-accent/50"
                    onClick={() => createTab("sql", query.sql_text, query.name)}
                  >
                    <CardHeader>
                      <CardTitle className="flex items-center gap-2 text-sm">
                        <Bookmark className="h-4 w-4 text-primary" />
                        <span className="truncate">{query.name}</span>
                      </CardTitle>
                      <CardDescription className="truncate font-mono text-xs">
                        {query.sql_text}
                      </CardDescription>
                    </CardHeader>
                    <CardFooter className="text-xs text-muted-foreground">
                      {formatDistanceToNow(new Date(query.updated_at), { addSuffix: true })}
                    </CardFooter>
                  </Card>
                ))}
              </div>
            )}
          </TabsContent>
        </Tabs>

        <PlatformInfoLinks className="mt-auto text-xs text-muted-foreground" />
      </div>
    </div>
  );
};

export default HomeTab;
