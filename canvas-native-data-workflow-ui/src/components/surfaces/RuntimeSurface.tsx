import { cn } from "../../utils/cn";
import { Icon } from "../../lib/icons";
import { useWorkspace } from "../../lib/store";
import { RUN_STEPS, STATUS_COLOR } from "../../lib/data";
import { Surface } from "../Surface";
import { StatusDot } from "../ui";

const DURATIONS = ["1.4s", "8.2s", "3m 12s", "11m 04s", "4m 47s", "—"];

export function RuntimeSurface() {
  const { s, d } = useWorkspace();
  const r = s.runtime;
  const total = RUN_STEPS.length;
  const pct = r.status === "complete" ? 100 : Math.round(((r.step + r.stepProgress / 100) / total) * 100);

  const stateOf = (i: number) => {
    if (r.status === "idle") return i < total - 1 ? "complete" : "warning";
    if (r.status === "complete") return "complete";
    if (r.status === "failed") return i < r.step ? "complete" : i === r.step ? "failed" : "waiting";
    return i < r.step ? "complete" : i === r.step ? "running" : "waiting";
  };
  const labelOf = (i: number) => {
    const st = stateOf(i);
    if (st === "running") return `Running ${Math.round(r.stepProgress)}%`;
    if (st === "failed") return "Failed";
    if (st === "waiting") return "Waiting";
    if (st === "warning") return "Warning";
    return "Complete";
  };

  return (
    <Surface
      id="runtime"
      icon="clock"
      title={`Pipeline Run #0${r.run}`}
      meta={r.status === "failed" ? "failed 12:09:02" : r.status === "running" ? "started 12:04:18" : "finished 12:07:41"}
      collapsedLabel={`Run #0${r.run} · ${pct}%`}
      extraMenu={[
        { label: "Re-run pipeline", icon: "play", onClick: () => d({ t: "run" }) },
        { label: "Reset runtime state", icon: "refresh", onClick: () => d({ t: "resetRun" }) },
        { label: "Download logs", icon: "download" },
      ]}
    >
      {/* overall */}
      <div className="border-b border-div px-3.5 py-2.5">
        <div className="flex items-baseline justify-between">
          <span className="flex items-center gap-2 text-[11.5px] text-t2">
            <StatusDot status={r.status === "failed" ? "failed" : r.status === "running" ? "running" : "complete"} pulse={r.status === "running"} />
            {r.status === "failed" ? "Execution halted" : r.status === "running" ? "Executing graph" : "Graph up to date"}
          </span>
          <span className="tnum text-[11px] text-t3">
            {Math.min(r.step + (r.status === "complete" ? 1 : 0), total)} / {total} · {pct}%
          </span>
        </div>
        <div className="mt-2 h-[2px] w-full overflow-hidden rounded-full bg-[#e5e9ed]">
          <div
            className="h-full transition-[width] duration-200"
            style={{ width: `${pct}%`, background: r.status === "failed" ? "#c95e62" : r.status === "running" ? "#2196d2" : "#4ba66a" }}
          />
        </div>
      </div>

      {/* steps */}
      <div className="scroll min-h-0 flex-1 overflow-y-auto px-1.5 py-1.5">
        {RUN_STEPS.map((step, i) => {
          const st = stateOf(i);
          return (
            <div
              key={step.id}
              onClick={() => d({ t: "select", id: step.id })}
              className={cn(
                "group relative flex h-[30px] cursor-default items-center gap-2.5 rounded-[6px] px-2",
                s.selected === step.id ? "bg-[#edf2f6]" : "hover:bg-[#f4f6f8]",
              )}
            >
              <StatusDot status={st} size={5.5} pulse={st === "running"} />
              <span className={cn("flex-1 truncate text-[11.5px]", st === "waiting" ? "text-t4" : "text-t2")}>{step.label}</span>
              <span className="tnum text-[10.5px]" style={{ color: st === "waiting" ? "#b0b8c1" : STATUS_COLOR[st] }}>
                {labelOf(i)}
              </span>
              <span className="tnum w-[48px] text-right text-[10.5px] text-t4">{DURATIONS[i]}</span>
              {st === "running" && (
                <span className="absolute inset-x-2 bottom-[2px] h-[2px] overflow-hidden rounded-full bg-[#e5e9ed]">
                  <span className="block h-full bg-[#2196d2]" style={{ width: `${r.stepProgress}%` }} />
                </span>
              )}
            </div>
          );
        })}
      </div>

      {/* failure block */}
      {r.status === "failed" && (
        <div className="shrink-0 border-t border-div px-3.5 py-2.5">
          <div className="flex items-start gap-2">
            <Icon name="alert" size={13} className="mt-[1px] shrink-0 text-[#c95e62]" />
            <div className="min-w-0 flex-1">
              <div className="text-[11.5px] text-t1">Final Export · SchemaMismatchError</div>
              <div className="mt-[3px] font-mono text-[10.5px] leading-[15px] text-t3">
                missing column `lang` at partition 004/128
              </div>
            </div>
          </div>
          <div className="mt-2.5 flex gap-1.5">
            <button
              onClick={() => d({ t: "run" })}
              className="h-[24px] rounded-[5px] border border-[#2196d2]/35 bg-[#2196d2]/[0.06] px-2 text-[10.5px] text-[#1686be] hover:bg-[#2196d2]/[0.1]"
            >
              Retry from step 06
            </button>
            <button
              onClick={() => d({ t: "resetRun" })}
              className="h-[24px] rounded-[5px] border border-[#dce2e8] px-2 text-[10.5px] text-t2 hover:text-t1"
            >
              Acknowledge
            </button>
          </div>
        </div>
      )}
    </Surface>
  );
}
