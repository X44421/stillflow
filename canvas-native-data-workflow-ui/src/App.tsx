import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { scaleRef } from "./lib/scale";
import { useWorkspace, WorkspaceProvider } from "./lib/store";
import { GraphCanvas } from "./components/GraphCanvas";
import { CanvasToolbar, Identity, StatusCluster, SurfaceDock, TopRight, ZoomIsland } from "./components/EdgeControls";
import { FilesSurface } from "./components/surfaces/FilesSurface";
import { InspectorSurface } from "./components/surfaces/InspectorSurface";
import { PreviewSurface } from "./components/surfaces/PreviewSurface";
import { RuntimeSurface } from "./components/surfaces/RuntimeSurface";
import { CommandPalette } from "./components/CommandPalette";
import { Icon } from "./lib/icons";

const FRAME_W = 1600;
const FRAME_H = 1000;

function Toast() {
  const { s, d } = useWorkspace();
  useEffect(() => {
    if (!s.toast) return;
    const t = setTimeout(() => d({ t: "toast", v: null }), 2600);
    return () => clearTimeout(t);
  }, [s.toast, d]);
  if (!s.toast) return null;
  return (
    <div className="fade-up absolute bottom-[62px] left-1/2 z-[400] flex h-[30px] -translate-x-1/2 items-center gap-2 rounded-[7px] border border-[#dce2e8] bg-white px-3 text-[11.5px] text-t2 island-shadow">
      <Icon name="check" size={12} className="text-[#4ba66a]" />
      {s.toast}
    </div>
  );
}

function Workspace() {
  const { s, d } = useWorkspace();

  /* runtime simulation */
  useEffect(() => {
    if (s.runtime.status !== "running") return;
    const iv = setInterval(() => d({ t: "tick" }), 110);
    return () => clearInterval(iv);
  }, [s.runtime.status, d]);

  /* quiet collapse on success */
  useEffect(() => {
    if (s.runtime.status !== "complete") return;
    const t = setTimeout(() => {
      if (s.surfaces.runtime.open) d({ t: "mode", id: "runtime", mode: "minimized" });
      d({ t: "runtimeExpanded", v: false });
    }, 2200);
    return () => clearTimeout(t);
  }, [s.runtime.status, s.surfaces.runtime.open, d]);

  /* global shortcuts */
  useEffect(() => {
    const h = (e: KeyboardEvent) => {
      const typing = (e.target as HTMLElement)?.tagName === "INPUT";
      const mod = e.metaKey || e.ctrlKey;
      if (mod && e.key.toLowerCase() === "k") {
        e.preventDefault();
        d({ t: "palette", v: !s.palette });
        return;
      }
      if (e.key === "Escape") {
        d({ t: "palette", v: false });
        d({ t: "menu", v: null });
        return;
      }
      if (mod && e.key === "Enter") {
        e.preventDefault();
        d({ t: "run" });
        return;
      }
      if (typing || mod) return;
      const map: Record<string, () => void> = {
        v: () => d({ t: "tool", v: "select" }),
        h: () => d({ t: "tool", v: "pan" }),
        a: () => d({ t: "tool", v: "add" }),
        c: () => d({ t: "tool", v: "connect" }),
        g: () => d({ t: "tool", v: "group" }),
        t: () => d({ t: "tool", v: "annotate" }),
        m: () => d({ t: "tool", v: "comment" }),
        "!": () => d({ t: "fit" }),
      };
      map[e.key.toLowerCase()]?.();
      if (e.key === "1" && e.shiftKey) d({ t: "fit" });
    };
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  }, [d, s.palette]);

  return (
    <div className="absolute inset-3 overflow-hidden rounded-[12px] border border-[#dce2e8] bg-ws">
      <GraphCanvas />

      <FilesSurface />
      <PreviewSurface id="preview" />
      <PreviewSurface id="preview2" />
      <InspectorSurface />
      <RuntimeSurface />

      <Identity />
      <CanvasToolbar />
      <TopRight />
      <ZoomIsland />
      <SurfaceDock />
      <StatusCluster />

      <Toast />
      <CommandPalette />
    </div>
  );
}

export default function App() {
  const [scale, setScale] = useState(1);
  const wrap = useRef<HTMLDivElement>(null);

  useLayoutEffect(() => {
    const fit = () => {
      const s = Math.min(window.innerWidth / FRAME_W, window.innerHeight / FRAME_H);
      const clamped = Math.max(0.4, Math.min(1.6, s));
      scaleRef.current = clamped;
      setScale(clamped);
    };
    fit();
    window.addEventListener("resize", fit);
    return () => window.removeEventListener("resize", fit);
  }, []);

  return (
    <div ref={wrap} className="fixed inset-0 grid place-items-center overflow-hidden bg-void">
      <div
        className="relative shrink-0"
        style={{ width: FRAME_W, height: FRAME_H, transform: `scale(${scale})`, transformOrigin: "center center" }}
      >
        <WorkspaceProvider>
          <Workspace />
        </WorkspaceProvider>
      </div>
    </div>
  );
}
