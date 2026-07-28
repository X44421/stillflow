import React from 'react';
import { Check, Database, Eye, X } from '../icons/hero';
import { formatRows } from '../utils/format';

export interface ValidationCheck {
  label: string;
  detail: string;
  /** pass: check succeeded; warn: borderline; fail: blocks publishing. */
  state: 'pass' | 'warn' | 'fail';
}

interface AssetPanelProps {
  /** Output dataset created by the last run; null before the first run. */
  assetName: string;
  version: number;
  published: boolean;
  stale: boolean;
  rows: number | null;
  columnCount: number;
  sourceName: string;
  checks: ValidationCheck[];
  onClose: () => void;
  onPreviewOutput: () => void;
  onPublish: () => void;
}

function Section({
  title,
  children,
  last = false,
}: {
  title: string;
  children: React.ReactNode;
  last?: boolean;
}) {
  return (
    <div className={`px-3 py-3 ${last ? '' : 'border-b border-[#edf2f6]'}`}>
      <div className="mb-2">
        <h4 className="text-[10.5px] font-semibold text-[#5e6874]">{title}</h4>
      </div>
      <div className="space-y-1.5">{children}</div>
    </div>
  );
}

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex min-h-[20px] items-center justify-between gap-3">
      <span className="shrink-0 text-[12px] text-[#5e6874]">{label}</span>
      {children}
    </div>
  );
}

function Value({ children }: { children: React.ReactNode }) {
  return (
    <span className="truncate text-[12px] font-medium text-[#171a1f]">
      {children}
    </span>
  );
}

const CHECK_STYLE = {
  pass: { icon: '✓', className: 'text-[#4ba66a]' },
  warn: { icon: '⚠', className: 'text-[#c58b32]' },
  fail: { icon: '✕', className: 'text-[#c95e62]' },
} as const;

/**
 * Context panel for the pipeline's Output Dataset — the asset a cleaning
 * run produces. This is where Validation and Publish live: the point where
 * a filtered table becomes a workspace asset.
 */
const AssetPanel: React.FC<AssetPanelProps> = ({
  assetName,
  version,
  published,
  stale,
  rows,
  columnCount,
  sourceName,
  checks,
  onClose,
  onPreviewOutput,
  onPublish,
}) => {
  const exists = rows !== null;
  const blocked = checks.filter((check) => check.state === 'fail').length;
  const publishable = exists && !stale && blocked === 0 && !published;

  return (
    <div className="relative flex w-[320px] flex-shrink-0 flex-col overflow-hidden rounded-lg border border-[#dce2e8] bg-white">
      <div
        className="flex-1 overflow-y-auto"
        style={{ scrollbarWidth: 'thin', scrollbarColor: '#c9d1d9 transparent' }}
      >
        {/* Header */}
        <div className="flex items-center gap-2.5 border-b border-[#edf2f6] px-3 py-2.5">
          <div className="grid h-7 w-7 shrink-0 place-items-center rounded-md bg-[#f4f6f8] text-[#5e6874]">
            <Database size={15} />
          </div>
          <div className="min-w-0 flex-1">
            <h3 className="truncate text-[13px] leading-[17px] font-semibold text-[#171a1f]">
              {assetName}
            </h3>
            <p className="flex items-center gap-1.5 text-[11px] leading-[14px] text-[#5e6874]">
              Output asset
              <span className="text-[#c9d1d9]">·</span>
              <span
                className={`h-[5px] w-[5px] rounded-full ${
                  published ? 'bg-[#4ba66a]' : exists ? 'bg-[#2196d2]' : 'bg-[#c9d1d9]'
                }`}
              />
              {published ? `Published v${version}` : exists ? `Draft v${version}` : 'Not created'}
            </p>
          </div>
          <button
            onClick={onClose}
            className="grid h-7 w-7 shrink-0 place-items-center rounded-md text-[#5e6874] transition-colors hover:bg-[#edf2f6] hover:text-[#171a1f]"
            aria-label="Close Inspector"
          >
            <X size={15} />
          </button>
        </div>

        {/* Actions */}
        <Section title="Actions">
          <div className="flex gap-1.5">
            <button
              onClick={onPreviewOutput}
              disabled={!exists}
              title={exists ? 'Preview the output rows' : 'Run the pipeline to create the output'}
              className="flex h-8 flex-1 items-center justify-center gap-1.5 rounded-md border border-[#dce2e8] bg-[#f4f6f8] text-[12.5px] font-medium text-[#39434e] transition-colors hover:bg-[#edf2f6] disabled:cursor-not-allowed disabled:opacity-50"
            >
              <Eye size={13} />
              Preview output
            </button>
            <button
              onClick={onPublish}
              disabled={!publishable}
              title={
                published
                  ? 'This version is published'
                  : !exists
                    ? 'Run the pipeline first'
                    : stale
                      ? 'Re-run to refresh the output before publishing'
                      : blocked > 0
                        ? `Blocked by ${blocked} quality check${blocked > 1 ? 's' : ''}`
                        : 'Mark this version as a published asset'
              }
              className="flex h-8 flex-1 items-center justify-center gap-1.5 rounded-md bg-[#2196d2] text-[12.5px] font-medium text-white transition-colors hover:bg-[#1686be] disabled:cursor-not-allowed disabled:bg-[#c9d1d9]"
            >
              <Check size={13} />
              {published ? 'Published' : 'Publish dataset'}
            </button>
          </div>
        </Section>

        {/* Asset */}
        <Section title="Asset">
          <Row label="Version">
            <Value>
              {exists ? `${published ? 'Published' : 'Draft'} v${version}` : '—'}
            </Value>
          </Row>
          <Row label="Rows">
            <Value>{exists ? formatRows(rows) : 'Run the pipeline to create'}</Value>
          </Row>
          <Row label="Columns">
            <Value>{columnCount}</Value>
          </Row>
          <Row label="Produced from">
            <Value>{sourceName}</Value>
          </Row>
          {stale && exists && (
            <p className="pt-1 text-[11px] leading-[15px] text-[#c58b32]">
              Configuration changed after this run — re-run to refresh the output.
            </p>
          )}
        </Section>

        {/* Validation — the quality gate between a run and a published asset. */}
        <Section title="Validation" last>
          {checks.map((check) => {
            const style = CHECK_STYLE[check.state];
            return (
              <div key={check.label} className="flex items-start gap-2">
                <span className={`w-3.5 shrink-0 text-[12px] font-semibold ${style.className}`}>
                  {style.icon}
                </span>
                <div className="min-w-0 flex-1">
                  <div className="text-[12px] text-[#171a1f]">{check.label}</div>
                  <div className="text-[11px] text-[#9099a4]">{check.detail}</div>
                </div>
              </div>
            );
          })}
          <p
            className={`pt-1.5 text-[11.5px] font-medium ${
              blocked > 0 ? 'text-[#c95e62]' : 'text-[#4ba66a]'
            }`}
          >
            {blocked > 0
              ? `Blocked by ${blocked} quality check${blocked > 1 ? 's' : ''}`
              : exists
                ? 'Ready to publish'
                : 'Run the pipeline to validate'}
          </p>
        </Section>
      </div>
    </div>
  );
};

export default AssetPanel;
