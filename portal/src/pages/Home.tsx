import { useEffect, useState } from "react";
import { Menu } from "lucide-react";
import DataExplorer from "@/components/explorer/DataExplorer";
import Sidebar from "@/components/layout/Sidebar";
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/ui/resizable";
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { Button } from "@/components/ui/button";
import WorkspaceTabs from "@/components/workspace/WorkspaceTabs";
import CommandPalette from "@/components/command-palette/CommandPalette";
import { DeepLinkLoader } from "@/components/share/DeepLinkLoader";
import JoinSessionDialog from "@/components/collaboration/JoinSessionDialog";
import DashboardShareLoader from "@/components/dashboard/DashboardShareLoader";
import DuckBrainSheet from "@/components/duck-brain/DuckBrainSheet";

export default function Home() {
  const [dataExplorerOpen, setDataExplorerOpen] = useState(false);
  const [isExplorerVisible, setIsExplorerVisible] = useState(true);

  useEffect(() => {
    const desktop = matchMedia("(min-width: 768px)");
    const closeMobileDrawer = (event: MediaQueryListEvent) => {
      if (event.matches) setDataExplorerOpen(false);
    };
    desktop.addEventListener("change", closeMobileDrawer);
    return () => desktop.removeEventListener("change", closeMobileDrawer);
  }, []);

  return (
    <div className="h-screen w-full flex overflow-hidden">
      <CommandPalette />
      {/* Global dialogs stay outside the single, breakpoint-stable workspace. */}
      <DeepLinkLoader />
      {/* Also mounted once: an invite link must not open two dialogs. */}
      <JoinSessionDialog />
      <DashboardShareLoader />
      {/* Duck Brain slide-over: one global mount, store-driven. */}
      <DuckBrainSheet />
      {/* Sidebar - Desktop only */}
      <aside className="hidden md:flex">
        <Sidebar
          isExplorerOpen={isExplorerVisible}
          onToggleExplorer={() => setIsExplorerVisible(!isExplorerVisible)}
        />
      </aside>

      {/* Main Content */}
      <main className="min-w-0 flex-1 flex flex-col overflow-hidden">
        {/* Mobile Data Explorer Drawer */}
        <Sheet open={dataExplorerOpen} onOpenChange={setDataExplorerOpen}>
          <div className="flex shrink-0 items-center border-b bg-background px-2 py-2 md:hidden">
            <SheetTrigger asChild>
              <Button variant="outline" size="sm" className="flex items-center gap-2">
                <Menu className="h-4 w-4" />
                <span className="text-xs">Tables</span>
              </Button>
            </SheetTrigger>
          </div>
          <SheetContent side="left" className="w-[300px] gap-0 p-0 sm:w-[350px]">
            <SheetHeader className="px-4 py-3 border-b">
              <SheetTitle>Data Explorer</SheetTitle>
            </SheetHeader>
            <div className="h-[calc(100%-60px)] overflow-auto">
              <DataExplorer onSqlAction={() => setDataExplorerOpen(false)} />
            </div>
          </SheetContent>
        </Sheet>

        {/* One workspace instance survives mobile/desktop breakpoint changes. */}
        <div className="min-h-0 flex-1">
          <ResizablePanelGroup direction="horizontal">
            {isExplorerVisible && (
              <>
                <ResizablePanel
                  className="hidden overflow-auto md:block"
                  defaultSize={20}
                  minSize={15}
                  maxSize={35}
                >
                  <DataExplorer />
                </ResizablePanel>
                <ResizableHandle withHandle className="hidden md:flex" />
              </>
            )}
            <ResizablePanel
              className="min-w-0 overflow-hidden"
              defaultSize={isExplorerVisible ? 80 : 100}
              minSize={50}
            >
              <WorkspaceTabs />
            </ResizablePanel>
          </ResizablePanelGroup>
        </div>
      </main>
    </div>
  );
}
