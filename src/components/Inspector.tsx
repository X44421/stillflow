import { useState } from "react";
import {
  ChevronDown,
  ChevronRight,
  ChevronsUpDown,
  FileText,
  Play,
  Sparkles,
  StickyNote,
  X,
} from "lucide-react";
import { cn } from "../utils/cn";
import type { PipelineNode } from "../data";

function Section({
  title,
  right,
  defaultOpen = false,
  icon,
  children,
}: {
  title: string;
  right?: React.ReactNode;
  defaultOpen?: boolean;
  icon?: React.ReactNode;
  children?: React.ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="border-b border-zinc-200">
      <button
        onClick={() => setOpen((o) => !o)}
        className="flex w-full items-center justify-between px-4 py-3 hover:bg-zinc-50"
      >
        <span className="flex items-center gap-1.5 text-sm font-semibold text-zinc-800">
          {open ? (
            <ChevronDown className="h-3.5 w-3.5 text-zinc-400" />
          ) : (
            <ChevronRight className="h-3.5 w-3.5 text-zinc-400" />
          )}
          {icon}
          {title}
        </span>
        <span className="flex items-center gap-1 text-[13px] text-zinc-400">
          {right}
          {right === undefined && open && <ChevronDown className="h-3.5 w-3.5 opacity-0" />}
        </span>
      </button>
      {open && children && <div className="px-4 pb-4">{children}</div>}
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-start justify-between gap-3 py-1.5">
      <span className="w-32 shrink-0 pt-1.5 text-[13px] text-zinc-500">{label}</span>
      <div className="min-w-0 flex-1">{children}</div>
    </div>
  );
}

export default function Inspector({
  node,
  onClose,
}: {
  node: PipelineNode | null;
  onClose: () => void;
}) {
  const [threshold, setThreshold] = useState(0.75);
  const [model, setModel] = useState("gpt-4o-mini");
  const [field, setField] = useState("product_category");
  const [note, setNote] = useState("");

  return (
    <aside className="flex w-[340px] shrink-0 flex-col border-l border-zinc-200 bg-white">
      <div className="flex shrink-0 items-center justify-between border-b border-zinc-200 px-4 py-3.5">
        <h2 className="text-[15px] font-semibold text-zinc-900">Inspector</h2>
        <button onClick={onClose} className="rounded-md p-1 text-zinc-400 hover:bg-zinc-100 hover:text-zinc-700">
          <X className="h-4 w-4" />
        </button>
      </div>

      {!node ? (
        <div className="flex flex-1 flex-col items-center justify-center gap-2 px-6 text-center">
          <FileText className="h-8 w-8 text-zinc-300" />
          <p className="text-sm text-zinc-400">Select a node to inspect its configuration.</p>
        </div>
      ) : (
        <div className="flex-1 overflow-y-auto">
          {/* Node */}
          <Section title="Node" defaultOpen>
            <div className="flex items-center gap-3 pb-3 pt-1">
              <div className="flex h-9 w-9 items-center justify-center rounded-lg border border-zinc-200 bg-zinc-50">
                <Sparkles className={cn("h-4 w-4", node.kind === "ai" ? "text-violet-500" : "text-zinc-500")} />
              </div>
              <div>
                <p className="text-sm font-semibold text-zinc-900">{node.title}</p>
                <p className="text-xs text-zinc-500">{node.subtitle}</p>
              </div>
            </div>
            <Field label="Node ID">
              <p className="pt-1.5 font-mono text-[13px] text-zinc-800">{node.nodeId}</p>
            </Field>
            <Field label="Description">
              <div className="rounded-lg border border-zinc-200 bg-zinc-50 px-3 py-2 text-[13px] leading-relaxed text-zinc-700">
                {node.description}
              </div>
            </Field>
            <Field label="Status">
              <p className="flex items-center gap-1.5 pt-1.5 text-[13px] text-zinc-800">
                <span className="h-2 w-2 rounded-full bg-emerald-500" />
                {node.status}
              </p>
            </Field>
            <Field label="Created">
              <p className="pt-1.5 text-[13px] text-zinc-800">{node.created}</p>
            </Field>
            <Field label="Updated">
              <p className="pt-1.5 text-[13px] text-zinc-800">{node.updated}</p>
            </Field>
          </Section>

          {/* Configuration */}
          <Section title="Configuration" defaultOpen>
            {node.kind === "ai" ? (
              <>
                <Field label="Model">
                  <div className="relative">
                    <select
                      value={model}
                      onChange={(e) => setModel(e.target.value)}
                      className="w-full appearance-none rounded-lg border border-zinc-200 bg-white px-3 py-1.5 pr-8 text-[13px] text-zinc-800 focus:border-zinc-400 focus:outline-none"
                    >
                      <option>gpt-4o-mini</option>
                      <option>gpt-4o</option>
                      <option>claude-3-5-haiku</option>
                    </select>
                    <ChevronsUpDown className="pointer-events-none absolute right-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-zinc-400" />
                  </div>
                </Field>
                <Field label="Prompt Template">
                  <button className="flex w-full items-center justify-center gap-1.5 rounded-lg border border-zinc-200 bg-white px-3 py-1.5 text-[13px] text-zinc-700 hover:bg-zinc-50">
                    <FileText className="h-3.5 w-3.5" />
                    Edit Prompt
                  </button>
                </Field>
                <Field label="Category Field">
                  <div className="relative">
                    <select
                      value={field}
                      onChange={(e) => setField(e.target.value)}
                      className="w-full appearance-none rounded-lg border border-zinc-200 bg-white px-3 py-1.5 pr-8 text-[13px] text-zinc-800 focus:border-zinc-400 focus:outline-none"
                    >
                      <option>product_category</option>
                      <option>product_name</option>
                      <option>description</option>
                    </select>
                    <ChevronsUpDown className="pointer-events-none absolute right-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-zinc-400" />
                  </div>
                </Field>
                <div className="flex items-center justify-between gap-3 py-1.5">
                  <span className="w-32 shrink-0 text-[13px] leading-tight text-zinc-500">
                    Confidence Threshold
                  </span>
                  <div className="flex flex-1 items-center gap-2">
                    <input
                      type="range"
                      min={0}
                      max={1}
                      step={0.01}
                      value={threshold}
                      onChange={(e) => setThreshold(Number(e.target.value))}
                      className="flex-1 accent-zinc-900"
                    />
                    <span className="w-11 rounded-md border border-zinc-200 px-1.5 py-0.5 text-center text-[12px] text-zinc-700">
                      {threshold.toFixed(2)}
                    </span>
                  </div>
                </div>
                <button className="mt-3 flex w-full items-center justify-center gap-1.5 rounded-lg border border-zinc-300 bg-white py-2 text-sm font-medium text-zinc-800 shadow-sm hover:bg-zinc-50">
                  <Play className="h-3.5 w-3.5" />
                  Test Run
                </button>
              </>
            ) : (
              <p className="rounded-lg border border-dashed border-zinc-200 bg-zinc-50 px-3 py-3 text-[13px] text-zinc-500">
                No configurable options for this step. Connect it in the pipeline and press Run.
              </p>
            )}
          </Section>

          <Section title="Input / Output" />
          <Section title="Data Preview" right={<span>1,103,021 rows</span>} />
          <Section title="Logs" right={<span>No recent logs</span>} />
          <Section title="Notes" defaultOpen icon={<StickyNote className="h-3.5 w-3.5 text-zinc-400" />}>
            <input
              value={note}
              onChange={(e) => setNote(e.target.value)}
              placeholder="Add note..."
              className="w-full rounded-lg border border-zinc-200 bg-white px-3 py-2 text-[13px] text-zinc-800 placeholder:text-zinc-400 focus:border-zinc-400 focus:outline-none"
            />
          </Section>
        </div>
      )}
    </aside>
  );
}
