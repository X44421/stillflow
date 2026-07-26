import { useState } from "react";
import Canvas from "./components/Canvas";
import PreviewTable from "./components/PreviewTable";
import Inspector from "./components/Inspector";
import { nodes as initialNodes } from "./data";

export default function App() {
  const [nodes, setNodes] = useState(initialNodes);
  const [selectedId, setSelectedId] = useState("ai");
  const [inspectorOpen, setInspectorOpen] = useState(true);

  const selectedNode = nodes.find((n) => n.id === selectedId) ?? null;

  const handleMove = (id: string, x: number, y: number) => {
    setNodes((ns) => ns.map((n) => (n.id === id ? { ...n, x, y } : n)));
  };

  const handleSelect = (id: string) => {
    setSelectedId(id);
    if (id) setInspectorOpen(true);
  };

  return (
    <div className="flex h-screen flex-col overflow-hidden bg-zinc-50 font-sans text-zinc-900 antialiased">
      <div className="flex min-h-0 flex-1">
        <main className="flex min-w-0 flex-1 flex-col">
          <Canvas
            nodes={nodes}
            selectedId={selectedId}
            onSelect={handleSelect}
            onMove={handleMove}
          />
          <div className="h-[335px] shrink-0 px-4">
            <PreviewTable />
          </div>
        </main>
        {inspectorOpen && (
          <Inspector node={selectedNode} onClose={() => setInspectorOpen(false)} />
        )}
      </div>
    </div>
  );
}
