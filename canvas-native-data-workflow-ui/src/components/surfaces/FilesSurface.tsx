import { useState } from "react";
import { cn } from "../../utils/cn";
import { Icon } from "../../lib/icons";
import { useWorkspace } from "../../lib/store";
import { OBJECT_GROUPS, RECENT_FILES } from "../../lib/data";
import { Surface } from "../Surface";
import { StatusDot } from "../ui";

const EXT_TONE: Record<string, string> = { csv: "#4ba66a", parquet: "#2196d2", json: "#c58b32" };

export function FilesSurface() {
  const { s, d } = useWorkspace();
  const [expanded, setExpanded] = useState<Record<string, boolean>>({ g1: true, g2: false });
  const q = s.filesQuery.trim().toLowerCase();

  const recent = RECENT_FILES.filter((f) => !q || f.name.toLowerCase().includes(q));
  const groups = OBJECT_GROUPS.map((g) => ({
    ...g,
    children: g.children.filter((c) => !q || c.name.toLowerCase().includes(q)),
  })).filter((g) => !q || g.children.length || g.label.toLowerCase().includes(q));

  return (
    <Surface
      id="files"
      icon="folder"
      title="Files"
      meta="34 objects"
      collapsedLabel="Files · 34 objects"
      extraMenu={[
        { label: "Import file…", icon: "download", keys: "⌘I" },
        { label: "New dataset object", icon: "database" },
        { label: "Show hidden objects", icon: "eye" },
      ]}
    >
      {/* search */}
      <div className="border-b border-div px-2.5 py-2">
        <div className="flex h-[28px] items-center gap-2 rounded-[6px] border border-[#dce2e8] bg-[#f4f6f8] px-2 focus-within:border-[#c9d1d9]">
          <Icon name="search" size={12.5} className="shrink-0 text-t4" />
          <input
            value={s.filesQuery}
            onChange={(e) => d({ t: "filesQuery", v: e.target.value })}
            placeholder="Search files and objects"
            className="w-full bg-transparent text-[11.5px] text-t1 placeholder:text-t4"
          />
          {s.filesQuery && (
            <button onClick={() => d({ t: "filesQuery", v: "" })} className="text-t4 hover:text-t2">
              <Icon name="x" size={11} />
            </button>
          )}
        </div>
      </div>

      <div className="scroll min-h-0 flex-1 overflow-y-auto px-1.5 py-2">
        {/* recent */}
        <div className="px-1.5 pb-1 text-[9.5px] font-medium tracking-[0.14em] text-t4 uppercase">Recent</div>
        {recent.map((f) => (
          <div
            key={f.id}
            draggable
            onDragStart={(e) => {
              e.dataTransfer.effectAllowed = "copy";
              e.dataTransfer.setData(
                "application/x-dcos",
                JSON.stringify({
                  name: f.name.replace(/\.[a-z]+$/, "").replace(/(^|_)(\w)/g, (_m, a, b) => (a ? " " : "") + b.toUpperCase()) + " Dataset",
                  kind: f.kind,
                  metric: f.meta.split(" · ")[0] + " · scanning",
                  behavior: "schema inference pending",
                }),
              );
            }}
            onDoubleClick={() => {
              d({ t: "previewTarget", id: "n1" });
              d({ t: "openSurface", id: "preview" });
            }}
            className="group flex h-[26px] cursor-grab items-center gap-2 rounded-[5px] px-1.5 hover:bg-[#edf2f6] active:cursor-grabbing"
          >
            <span style={{ color: EXT_TONE[f.ext] }} className="shrink-0 opacity-80">
              <Icon name="file" size={12.5} />
            </span>
            <span className="flex-1 truncate text-[11.5px] text-t2 group-hover:text-t1">{f.name}</span>
            <span className="tnum shrink-0 text-[10px] text-t4">{f.meta}</span>
          </div>
        ))}

        {/* objects */}
        <div className="mt-3 px-1.5 pb-1 text-[9.5px] font-medium tracking-[0.14em] text-t4 uppercase">Objects</div>
        {groups.map((g) => {
          const on = expanded[g.id] || (!!q && g.children.length > 0);
          return (
            <div key={g.id}>
              <button
                onClick={() => setExpanded((e) => ({ ...e, [g.id]: !on }))}
                className="group flex h-[26px] w-full items-center gap-2 rounded-[5px] px-1.5 hover:bg-[#edf2f6]"
              >
                <Icon name="chevRight" size={10} className={cn("shrink-0 text-t4 transition-transform", on && "rotate-90")} />
                <Icon name={g.icon} size={12.5} className="shrink-0 text-t3" />
                <span className="flex-1 text-left text-[11.5px] text-t2 group-hover:text-t1">{g.label}</span>
                <span className="tnum shrink-0 text-[10px] text-t4">{g.count}</span>
              </button>
              {on &&
                g.children.map((c) => (
                  <div
                    key={c.id}
                    draggable
                    onDragStart={(e) => {
                      e.dataTransfer.effectAllowed = "copy";
                      e.dataTransfer.setData(
                        "application/x-dcos",
                        JSON.stringify({
                          name: c.name,
                          kind: g.label === "Pipelines" ? "PIPE" : g.label === "Evaluations" ? "VALIDATE" : "DATASET",
                          metric: c.meta,
                          behavior: "instance of shared object",
                        }),
                      );
                    }}
                    onDoubleClick={() => {
                      d({ t: "previewTarget", id: "n1" });
                      d({ t: "openSurface", id: "preview" });
                    }}
                    className="group flex h-[24px] cursor-grab items-center gap-2 rounded-[5px] pr-1.5 pl-[30px] hover:bg-[#edf2f6] active:cursor-grabbing"
                  >
                    {c.status ? <StatusDot status={c.status} size={5} /> : <span className="h-[5px] w-[5px] rounded-full bg-[#dce2e8]" />}
                    <span className="flex-1 truncate text-[11px] text-t3 group-hover:text-t1">{c.name}</span>
                    <span className="tnum shrink-0 text-[10px] text-t4">{c.meta}</span>
                  </div>
                ))}
            </div>
          );
        })}
      </div>

      <div className="flex h-[26px] shrink-0 items-center gap-1.5 border-t border-div px-3 text-[10px] text-t4">
        <Icon name="arrowRight" size={11} />
        Drag a file onto the canvas to create an object
      </div>
    </Surface>
  );
}
