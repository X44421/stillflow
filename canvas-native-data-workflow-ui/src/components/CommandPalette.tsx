import { useEffect, useMemo, useRef, useState } from "react";
import { cn } from "../utils/cn";
import { Icon } from "../lib/icons";
import { useWorkspace } from "../lib/store";
import { Kbd } from "./ui";

export function CommandPalette() {
  const { s, d } = useWorkspace();
  const [q, setQ] = useState("");
  const [i, setI] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);

  const commands = useMemo(
    () => [
      { g: "Run", label: "Run pipeline from Customer Dataset", icon: "play", keys: "⌘↵", fn: () => d({ t: "run" }) },
      { g: "Run", label: "Reset runtime state", icon: "refresh", fn: () => d({ t: "resetRun" }) },
      { g: "Run", label: "Expand runtime", icon: "clock", fn: () => (d({ t: "runtimeExpanded", v: true }), d({ t: "openSurface", id: "runtime" })) },
      { g: "Surfaces", label: "Open Files", icon: "folder", keys: "⌘1", fn: () => d({ t: "openSurface", id: "files" }) },
      { g: "Surfaces", label: "Open Inspector", icon: "settings", keys: "⌘2", fn: () => d({ t: "openSurface", id: "inspector" }) },
      { g: "Surfaces", label: "Open Data Preview", icon: "table", keys: "⌘3", fn: () => d({ t: "openSurface", id: "preview" }) },
      {
        g: "Surfaces",
        label: "Compare input / output previews",
        icon: "columns",
        fn: () => {
          d({ t: "openSurface", id: "preview", rect: { x: 436, y: 548, w: 384, h: 356 } });
          d({ t: "openSurface", id: "preview2" });
        },
      },
      { g: "Surfaces", label: "Dock Data Preview to bottom", icon: "dockBottom", fn: () => d({ t: "mode", id: "preview", mode: "docked", dock: "bottom" }) },
      { g: "Objects", label: "Add dataset object", icon: "database", keys: "⌘⇧D", fn: () => d({ t: "addNode", kind: "DATASET", name: "New Dataset", x: 480, y: 400 }) },
      { g: "Objects", label: "Add transform · Remove duplicates", icon: "filter", fn: () => d({ t: "addNode", kind: "FILTER", name: "Remove Duplicates", x: 240, y: 400, metric: "pending run", behavior: "key: content_hash" }) },
      { g: "Objects", label: "Add validation object", icon: "validate", fn: () => d({ t: "addNode", kind: "VALIDATE", name: "Schema Contract", x: 960, y: 400, metric: "0 rules", behavior: "no rules configured" }) },
      { g: "View", label: "Profile the selected dataset", icon: "profile", fn: () => (d({ t: "previewTab", v: "profile" }), d({ t: "openSurface", id: "preview" })) },
      { g: "View", label: "Zoom to fit", icon: "fit", keys: "⇧1", fn: () => d({ t: "fit" }) },
      { g: "View", label: "Zoom to 100%", icon: "zoomIn", fn: () => d({ t: "setZoom", v: 1 }) },
    ],
    [d],
  );

  const filtered = commands.filter((c) => c.label.toLowerCase().includes(q.trim().toLowerCase()));

  useEffect(() => {
    if (s.palette) {
      setQ("");
      setI(0);
      setTimeout(() => inputRef.current?.focus(), 10);
    }
  }, [s.palette]);

  useEffect(() => {
    if (!s.palette) return;
    const h = (e: KeyboardEvent) => {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setI((v) => Math.min(filtered.length - 1, v + 1));
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        setI((v) => Math.max(0, v - 1));
      } else if (e.key === "Enter") {
        e.preventDefault();
        filtered[i]?.fn();
        d({ t: "palette", v: false });
      }
    };
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  }, [s.palette, filtered, i, d]);

  if (!s.palette) return null;

  let lastGroup = "";
  return (
    <div className="absolute inset-0 z-[700]" onMouseDown={() => d({ t: "palette", v: false })}>
      <div className="absolute inset-0 bg-[#171a1f]/20" />
      <div
        onMouseDown={(e) => e.stopPropagation()}
        className="fade-up absolute top-[132px] left-1/2 w-[560px] -translate-x-1/2 overflow-hidden rounded-[10px] border border-[#dce2e8] bg-white shadow-[0_36px_80px_-30px_rgba(0,0,0,0.2)]"
      >
        <div className="flex h-[44px] items-center gap-2.5 border-b border-div px-3.5">
          <Icon name="command" size={14} className="shrink-0 text-t3" />
          <input
            ref={inputRef}
            value={q}
            onChange={(e) => {
              setQ(e.target.value);
              setI(0);
            }}
            placeholder="Search commands, objects and files"
            className="h-full w-full bg-transparent text-[13px] text-t1"
          />
          <Kbd>esc</Kbd>
        </div>
        <div className="scroll max-h-[344px] overflow-y-auto p-1.5">
          {filtered.map((c, idx) => {
            const head = c.g !== lastGroup ? ((lastGroup = c.g), c.g) : null;
            return (
              <div key={c.label}>
                {head && <div className="px-2 pt-2 pb-1 text-[9.5px] font-medium tracking-[0.14em] text-t4 uppercase">{head}</div>}
                <button
                  onMouseEnter={() => setI(idx)}
                  onClick={() => {
                    c.fn();
                    d({ t: "palette", v: false });
                  }}
                  className={cn(
                    "flex h-[30px] w-full items-center gap-2.5 rounded-[6px] px-2 text-left",
                    idx === i ? "bg-[#edf2f6] text-t1" : "text-t2",
                  )}
                >
                  <Icon name={c.icon} size={13} className={idx === i ? "text-t2" : "text-t3"} />
                  <span className="flex-1 text-[12px]">{c.label}</span>
                  {c.keys && <span className="tnum text-[10px] text-t4">{c.keys}</span>}
                </button>
              </div>
            );
          })}
          {!filtered.length && <div className="px-2 py-6 text-center text-[11.5px] text-t4">No matching commands</div>}
        </div>
        <div className="flex h-[28px] items-center gap-3 border-t border-div px-3.5 text-[10px] text-t4">
          <span className="flex items-center gap-1.5">
            <Kbd>↑↓</Kbd> navigate
          </span>
          <span className="flex items-center gap-1.5">
            <Kbd>↵</Kbd> run
          </span>
          <span className="flex-1" />
          <span>DataCleaner OS · 34 objects indexed</span>
        </div>
      </div>
    </div>
  );
}
