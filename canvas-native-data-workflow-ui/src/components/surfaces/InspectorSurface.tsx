import { useState, type ReactNode } from "react";
import { cn } from "../../utils/cn";
import { Icon } from "../../lib/icons";
import { nodeById, useWorkspace } from "../../lib/store";
import { EVENTS, ICON_BY_KIND, PARAMS, STATUS_COLOR } from "../../lib/data";
import { Surface } from "../Surface";
import { IconBtn, Row, SectionLabel, StatusDot } from "../ui";

function Section({ label, right, children }: { label: string; right?: ReactNode; children: ReactNode }) {
  return (
    <div className="border-b border-div px-3.5 py-3">
      <SectionLabel right={right}>{label}</SectionLabel>
      <div className="mt-1.5">{children}</div>
    </div>
  );
}

function Collapsible({ label, count, children }: { label: string; count?: string; children?: ReactNode }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="border-b border-div">
      <button onClick={() => setOpen((v) => !v)} className="group flex h-[34px] w-full items-center gap-2 px-3.5">
        <Icon name="chevRight" size={10} className={cn("text-t4 transition-transform", open && "rotate-90")} />
        <span className="flex-1 text-left text-[10px] font-medium tracking-[0.13em] text-t4 uppercase group-hover:text-t3">{label}</span>
        {count && <span className="tnum text-[10px] text-t4">{count}</span>}
      </button>
      {open && <div className="px-3.5 pb-3">{children}</div>}
    </div>
  );
}

export function InspectorSurface() {
  const { s, d } = useWorkspace();
  const n = nodeById(s, s.selected);

  if (!n) {
    return (
      <Surface id="inspector" icon="settings" title="Inspector" meta="no selection" collapsedLabel="Inspector · empty">
        <div className="flex flex-1 flex-col items-center justify-center gap-2 px-8 text-center">
          <Icon name="cursor" size={18} className="text-t4" />
          <p className="text-[11.5px] text-t3">Select an object on the canvas to edit its context.</p>
        </div>
      </Surface>
    );
  }

  const failed = n.status === "failed";
  const statusLabel = failed ? "Failed" : n.status === "warning" ? "Warning" : n.status === "running" ? "Running" : n.status === "waiting" ? "Waiting" : "Ready";

  return (
    <Surface
      id="inspector"
      icon="settings"
      title={n.name}
      meta={`${n.kind} · ${statusLabel}`}
      collapsedLabel={`${n.name} · ${n.kind}`}
      headerRight={
        <IconBtn
          icon="lock"
          size={22}
          iconSize={12.5}
          tip={s.inspectorLocked ? "Unlock from object" : "Lock to this object"}
          active={s.inspectorLocked}
          onClick={() => d({ t: "toggleLock" })}
        />
      }
      extraMenu={[
        { label: "Open in preview", icon: "eye", onClick: () => (d({ t: "previewTarget", id: n.id }), d({ t: "openSurface", id: "preview" })) },
        { label: "Copy object ID", icon: "copy" },
        { label: "Reveal in files", icon: "folder" },
      ]}
    >
      {/* object header */}
      <div className="flex items-start gap-3 border-b border-div px-3.5 py-3">
        <span className="mt-[2px] grid h-[26px] w-[26px] shrink-0 place-items-center rounded-[6px] border border-[#dce2e8] bg-[#f4f6f8] text-t2">
          <Icon name={ICON_BY_KIND[n.kind]} size={13} />
        </span>
        <div className="min-w-0 flex-1">
          <div className="truncate text-[14.5px] leading-[18px] font-medium tracking-[-0.012em] text-t1">{n.name}</div>
          <div className="mt-[3px] flex items-center gap-1.5 text-[10.5px] text-t3">
            <span className="tracking-[0.1em]">{n.kind}</span>
            <span className="text-[#c9d1d9]">·</span>
            <StatusDot status={n.status} size={5} />
            <span style={{ color: STATUS_COLOR[n.status] }}>{statusLabel}</span>
          </div>
        </div>
      </div>

      <div className="scroll min-h-0 flex-1 overflow-y-auto">
        {/* error context takes priority */}
        {failed && (
          <div className="border-b border-div bg-[#fdf6f6] px-3.5 py-3">
            <SectionLabel right={<span className="tnum text-[10px] text-[#c95e62]">exit 12</span>}>Error context</SectionLabel>
            <div className="mt-2 rounded-[6px] border border-[#f0d4d4] bg-[#fef9f9] p-2.5">
              <div className="font-mono text-[10.5px] leading-[15px] text-[#c95e62]">SchemaMismatchError</div>
              <p className="mt-1.5 text-[11px] leading-[16px] text-t2">
                Target schema expects column <span className="font-mono text-[10.5px] text-t1">lang</span> (string) but the incoming chunk
                dataset resolved 6 of 7 columns.
              </p>
              <div className="mt-2 flex gap-1.5">
                <button className="h-[24px] rounded-[5px] border border-[#2196d2]/35 bg-[#2196d2]/[0.06] px-2 text-[10.5px] text-[#1686be] hover:bg-[#2196d2]/[0.1]">
                  Map column
                </button>
                <button
                  onClick={() => d({ t: "resetRun" })}
                  className="h-[24px] rounded-[5px] border border-[#dce2e8] px-2 text-[10.5px] text-t2 hover:text-t1"
                >
                  Retry step
                </button>
              </div>
            </div>
          </div>
        )}

        <Section label="Identity">
          <Row k="Type" v={n.kind === "PIPE" ? "Chunk pipeline" : n.kind.toLowerCase()} />
          <Row k="Status" v={<span style={{ color: STATUS_COLOR[n.status] }}>{statusLabel}</span>} />
          <Row k="Object ID" v={n.objectId} mono />
        </Section>

        <Section label="Runtime" right={<span className="tnum text-[10px] text-t4">Run #0{s.runtime.run}</span>}>
          <Row k="Input rows" v="982,188" />
          <Row k="Output chunks" v="3,412,000" />
          <Row k="Last duration" v={n.duration} />
          <Row k="Last execution" v={failed ? <span className="text-[#c95e62]">Failed 12:09:02</span> : "Completed 12:07:41"} />
        </Section>

        <Section
          label="Parameters"
          right={
            <button className="flex items-center gap-1 text-[10px] text-t3 hover:text-t1">
              <Icon name="settings" size={10} />
              Edit
            </button>
          }
        >
          {PARAMS.map((p) => (
            <div key={p.key} className="flex items-center justify-between gap-3 py-[3px]">
              <span className="text-[11px] text-t3">{p.key}</span>
              <span className="flex h-[22px] items-center gap-1 rounded-[5px] border border-[#dce2e8] bg-[#f4f6f8] px-1.5">
                <span className="tnum text-[11.5px] text-t1">{p.value}</span>
                {p.unit && <span className="text-[10px] text-t4">{p.unit}</span>}
              </span>
            </div>
          ))}
        </Section>

        <Section label="Relationships">
          {[
            { k: "Input", v: "Customer Dataset", i: "database" },
            { k: "Output", v: "Chunk Dataset", i: "database" },
            { k: "Downstream", v: "Text Embedding", i: "embed" },
            { k: "Monitor", v: "Customer Quality", i: "validate" },
          ].map((r) => (
            <div key={r.k} className="group flex items-center justify-between gap-3 py-[4px]">
              <span className="text-[11px] text-t3">{r.k}</span>
              <span className="flex items-center gap-1.5 text-[11.5px] text-t2 group-hover:text-t1">
                <Icon name={r.i} size={11} className="text-t4" />
                {r.v}
                <Icon name="chevRight" size={10} className="text-t4 opacity-0 group-hover:opacity-100" />
              </span>
            </div>
          ))}
        </Section>

        <Collapsible label="Recent events" count={String(EVENTS.length)}>
          <div className="space-y-1.5">
            {EVENTS.map((e) => (
              <div key={e.t} className="flex gap-2.5">
                <span className="tnum shrink-0 text-[10px] text-t4">{e.t}</span>
                <span className="text-[11px] leading-[15px] text-t3">{e.label}</span>
              </div>
            ))}
          </div>
        </Collapsible>
        <Collapsible label="Errors" count={failed ? "1" : "0"} />
        <Collapsible label="Lineage" count="4 hops" />
        <Collapsible label="AI insight">
          <p className="text-[11px] leading-[16px] text-t3">
            Chunk overlap of 120 tokens produces 3.4M chunks. Reducing overlap to 80 would cut embedding cost by roughly 14% with a
            0.6% recall impact on the current evaluation set.
          </p>
        </Collapsible>
      </div>
    </Surface>
  );
}
