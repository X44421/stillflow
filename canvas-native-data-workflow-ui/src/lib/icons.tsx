import type { SVGProps } from "react";

const P: Record<string, React.ReactNode> = {
  cursor: <path d="M4 2.5 12.6 8.9l-3.7.7 2 4.2-1.6.8-2-4.2L4.6 13z" />,
  hand: (
    <>
      <path d="M6.5 8V4.2a1.1 1.1 0 0 1 2.2 0V8" />
      <path d="M8.7 7.6V3.4a1.1 1.1 0 0 1 2.2 0v4.2" />
      <path d="M10.9 7.9V5.2a1.1 1.1 0 0 1 2.2 0v5.1c0 2.4-1.7 4-4 4s-4.6-1.3-4.6-3.6V8.4a1 1 0 0 1 2 0v1.4" />
    </>
  ),
  add: (
    <>
      <rect x="2.5" y="2.5" width="11" height="11" rx="2.5" />
      <path d="M8 5.6v4.8M5.6 8h4.8" />
    </>
  ),
  connect: (
    <>
      <path d="M6.4 9.6 9.6 6.4" />
      <path d="M9.2 4.6 10.4 3.4a2.6 2.6 0 0 1 3.7 3.7l-1.2 1.2" />
      <path d="M6.8 11.4 5.6 12.6a2.6 2.6 0 0 1-3.7-3.7l1.2-1.2" />
    </>
  ),
  group: (
    <>
      <path d="M2.5 5V3.5A1 1 0 0 1 3.5 2.5H5M11 2.5h1.5a1 1 0 0 1 1 1V5M13.5 11v1.5a1 1 0 0 1-1 1H11M5 13.5H3.5a1 1 0 0 1-1-1V11" />
      <rect x="5.6" y="5.6" width="4.8" height="4.8" rx="1" />
    </>
  ),
  annotate: (
    <>
      <path d="M3 4.2V3h10v1.2M8 3v10M6.2 13h3.6" />
    </>
  ),
  comment: (
    <>
      <path d="M13.5 9.2a2 2 0 0 1-2 2H6.2L3 13.4V4.3a2 2 0 0 1 2-2h6.5a2 2 0 0 1 2 2z" />
    </>
  ),
  layout: (
    <>
      <rect x="2.2" y="3" width="4.6" height="4" rx="1" />
      <rect x="9.2" y="9" width="4.6" height="4" rx="1" />
      <path d="M6.8 5h2.6a1 1 0 0 1 1 1v3" />
    </>
  ),
  zoomOut: <path d="M3.5 8h9" />,
  zoomIn: <path d="M8 3.5v9M3.5 8h9" />,
  fit: (
    <>
      <path d="M2.6 5.6V3.4a.8.8 0 0 1 .8-.8h2.2M10.4 2.6h2.2a.8.8 0 0 1 .8.8v2.2M13.4 10.4v2.2a.8.8 0 0 1-.8.8h-2.2M5.6 13.4H3.4a.8.8 0 0 1-.8-.8v-2.2" />
    </>
  ),
  undo: (
    <>
      <path d="M3 6.5h6.2a3.4 3.4 0 0 1 0 6.8H6.4" />
      <path d="m5.6 4 -2.6 2.5L5.6 9" />
    </>
  ),
  history: (
    <>
      <path d="M2.9 8a5.1 5.1 0 1 0 1.6-3.7L2.6 6" />
      <path d="M2.6 3v3h3" />
      <path d="M8 5.6V8l1.8 1.1" />
    </>
  ),
  bell: (
    <>
      <path d="M12.2 10.6V7.2a4.2 4.2 0 1 0-8.4 0v3.4L2.7 12h10.6z" />
      <path d="M6.6 12v.4a1.4 1.4 0 0 0 2.8 0V12" />
    </>
  ),
  help: (
    <>
      <circle cx="8" cy="8" r="5.6" />
      <path d="M6.4 6.3a1.7 1.7 0 1 1 1.9 1.9v.9" />
      <path d="M8.2 11.1h.01" />
    </>
  ),
  search: (
    <>
      <circle cx="7.2" cy="7.2" r="4.2" />
      <path d="m10.4 10.4 2.8 2.8" />
    </>
  ),
  filter: <path d="M2.6 3.4h10.8l-4.2 5v4.4l-2.4-1.3V8.4z" />,
  columns: (
    <>
      <rect x="2.4" y="2.8" width="11.2" height="10.4" rx="1.4" />
      <path d="M6.2 2.8v10.4M9.9 2.8v10.4" />
    </>
  ),
  download: (
    <>
      <path d="M8 2.6v7M5.3 7l2.7 2.7L10.7 7" />
      <path d="M2.8 11.4v1a1 1 0 0 0 1 1h8.4a1 1 0 0 0 1-1v-1" />
    </>
  ),
  more: (
    <>
      <circle cx="3.6" cy="8" r=".95" fill="currentColor" stroke="none" />
      <circle cx="8" cy="8" r=".95" fill="currentColor" stroke="none" />
      <circle cx="12.4" cy="8" r=".95" fill="currentColor" stroke="none" />
    </>
  ),
  moreV: (
    <>
      <circle cx="8" cy="3.6" r=".95" fill="currentColor" stroke="none" />
      <circle cx="8" cy="8" r=".95" fill="currentColor" stroke="none" />
      <circle cx="8" cy="12.4" r=".95" fill="currentColor" stroke="none" />
    </>
  ),
  x: <path d="m4.2 4.2 7.6 7.6M11.8 4.2l-7.6 7.6" />,
  minimize: <path d="M4 11.4h8" />,
  maximize: <rect x="3.6" y="3.6" width="8.8" height="8.8" rx="1.4" />,
  restore: (
    <>
      <rect x="2.8" y="5.6" width="7.6" height="7.6" rx="1.3" />
      <path d="M5.6 5.6V4a1.2 1.2 0 0 1 1.2-1.2h6.2a1.2 1.2 0 0 1 1.2 1.2v6.2A1.2 1.2 0 0 1 13 11.4h-1.4" />
    </>
  ),
  collapse: <path d="m4.4 6.4 3.6 3.4 3.6-3.4" />,
  expand: <path d="m4.4 9.6 3.6-3.4 3.6 3.4" />,
  chevDown: <path d="m4.6 6.6 3.4 3.2 3.4-3.2" />,
  chevRight: <path d="m6.4 4.4 3.4 3.6-3.4 3.6" />,
  lock: (
    <>
      <rect x="3.4" y="7" width="9.2" height="6.4" rx="1.4" />
      <path d="M5.6 7V5.2a2.4 2.4 0 0 1 4.8 0V7" />
    </>
  ),
  play: <path d="M5.4 3.6 12 8l-6.6 4.4z" />,
  eye: (
    <>
      <path d="M1.6 8S4 3.8 8 3.8 14.4 8 14.4 8 12 12.2 8 12.2 1.6 8 1.6 8" />
      <circle cx="8" cy="8" r="1.9" />
    </>
  ),
  database: (
    <>
      <ellipse cx="8" cy="4.2" rx="5.2" ry="1.9" />
      <path d="M2.8 4.2v7.6c0 1 2.3 1.9 5.2 1.9s5.2-.9 5.2-1.9V4.2" />
      <path d="M2.8 8c0 1 2.3 1.9 5.2 1.9s5.2-.9 5.2-1.9" />
    </>
  ),
  pipe: (
    <>
      <rect x="2.4" y="3" width="4" height="4" rx="1" />
      <rect x="9.6" y="9" width="4" height="4" rx="1" />
      <path d="M4.4 7v3a1 1 0 0 0 1 1h4.2" />
    </>
  ),
  embed: (
    <>
      <path d="M8 2.4 9.3 6l3.6 1.3L9.3 8.6 8 12.2 6.7 8.6 3.1 7.3 6.7 6z" />
      <path d="M12.4 11.4v2.2M11.3 12.5h2.2" />
    </>
  ),
  index: (
    <>
      <rect x="2.6" y="2.6" width="4.6" height="4.6" rx="1" />
      <rect x="8.8" y="2.6" width="4.6" height="4.6" rx="1" />
      <rect x="2.6" y="8.8" width="4.6" height="4.6" rx="1" />
      <rect x="8.8" y="8.8" width="4.6" height="4.6" rx="1" />
    </>
  ),
  validate: (
    <>
      <path d="M8 2.2 13 4v4c0 3-2.2 4.9-5 5.8C5.2 12.9 3 11 3 8V4z" />
      <path d="m6 7.9 1.6 1.6L10.4 6.6" />
    </>
  ),
  exportIcon: (
    <>
      <path d="M8 10.4V2.8M5.3 5.5 8 2.8l2.7 2.7" />
      <path d="M2.8 10.6v1.8a1 1 0 0 0 1 1h8.4a1 1 0 0 0 1-1v-1.8" />
    </>
  ),
  file: (
    <>
      <path d="M9 2.4H4.8a1.2 1.2 0 0 0-1.2 1.2v8.8a1.2 1.2 0 0 0 1.2 1.2h6.4a1.2 1.2 0 0 0 1.2-1.2V5.8z" />
      <path d="M9 2.4v3.4h3.4" />
    </>
  ),
  folder: (
    <path d="M2.6 12.2V4.4a1 1 0 0 1 1-1h2.7l1.4 1.7h4.7a1 1 0 0 1 1 1v6.1a1 1 0 0 1-1 1H3.6a1 1 0 0 1-1-1" />
  ),
  clock: (
    <>
      <circle cx="8" cy="8" r="5.6" />
      <path d="M8 4.9V8l2 1.2" />
    </>
  ),
  alert: (
    <>
      <path d="M8 2.6 14 13H2z" />
      <path d="M8 6.5v3M8 11.3h.01" />
    </>
  ),
  check: <path d="m3.4 8.4 3 3 6.2-6.6" />,
  command: (
    <path d="M5.6 2.6a1.8 1.8 0 1 0 0 3.6h4.8a1.8 1.8 0 1 0 0-3.6 1.8 1.8 0 0 0-1.8 1.8v7.2a1.8 1.8 0 1 0 1.8-1.8H5.6a1.8 1.8 0 1 0 1.8 1.8V4.4a1.8 1.8 0 0 0-1.8-1.8" />
  ),
  refresh: (
    <>
      <path d="M13.2 7A5.2 5.2 0 0 0 4 4.6L2.8 5.9" />
      <path d="M2.8 9a5.2 5.2 0 0 0 9.2 2.4l1.2-1.3" />
      <path d="M2.8 2.9v3h3M13.2 13.1v-3h-3" />
    </>
  ),
  dockLeft: (
    <>
      <rect x="2.4" y="2.8" width="11.2" height="10.4" rx="1.4" />
      <path d="M6.6 2.8v10.4" />
    </>
  ),
  dockBottom: (
    <>
      <rect x="2.4" y="2.8" width="11.2" height="10.4" rx="1.4" />
      <path d="M2.4 9.6h11.2" />
    </>
  ),
  arrowRight: <path d="M3 8h9.4M9.2 4.8 12.4 8l-3.2 3.2" />,
  table: (
    <>
      <rect x="2.4" y="2.8" width="11.2" height="10.4" rx="1.4" />
      <path d="M2.4 6.4h11.2M6.6 6.4v6.8" />
    </>
  ),
  chart: (
    <>
      <path d="M2.8 13.2V2.8" />
      <path d="M2.8 13.2h10.4" />
      <path d="M5.4 10.8V8.2M8 10.8V5.4M10.6 10.8V6.9" />
    </>
  ),
  profile: (
    <>
      <path d="M3 12.8V8.4M6.3 12.8V4.6M9.7 12.8v-5M13 12.8V3.2" />
    </>
  ),
  settings: (
    <>
      <circle cx="8" cy="8" r="1.9" />
      <path d="M12.9 9.6a1 1 0 0 0 .2 1.1l.1.1a1.2 1.2 0 1 1-1.7 1.7l-.1-.1a1 1 0 0 0-1.1-.2 1 1 0 0 0-.6.9v.2a1.2 1.2 0 1 1-2.4 0v-.1a1 1 0 0 0-.7-.9 1 1 0 0 0-1.1.2l-.1.1a1.2 1.2 0 1 1-1.7-1.7l.1-.1a1 1 0 0 0 .2-1.1 1 1 0 0 0-.9-.6h-.2a1.2 1.2 0 1 1 0-2.4h.1a1 1 0 0 0 .9-.7 1 1 0 0 0-.2-1.1l-.1-.1a1.2 1.2 0 1 1 1.7-1.7l.1.1a1 1 0 0 0 1.1.2h.1a1 1 0 0 0 .6-.9v-.2a1.2 1.2 0 1 1 2.4 0v.1a1 1 0 0 0 .6.9 1 1 0 0 0 1.1-.2l.1-.1a1.2 1.2 0 1 1 1.7 1.7l-.1.1a1 1 0 0 0-.2 1.1v.1a1 1 0 0 0 .9.6h.2a1.2 1.2 0 1 1 0 2.4h-.1a1 1 0 0 0-.9.6" />
    </>
  ),
  copy: (
    <>
      <rect x="5.6" y="5.6" width="7.8" height="7.8" rx="1.3" />
      <path d="M10.4 5.6V4a1.4 1.4 0 0 0-1.4-1.4H4a1.4 1.4 0 0 0-1.4 1.4v5a1.4 1.4 0 0 0 1.4 1.4h1.6" />
    </>
  ),
  trash: (
    <>
      <path d="M2.8 4.4h10.4M6 4.4V3.2a.8.8 0 0 1 .8-.8h2.4a.8.8 0 0 1 .8.8v1.2" />
      <path d="M4.2 4.4v8a1 1 0 0 0 1 1h5.6a1 1 0 0 0 1-1v-8" />
    </>
  ),
  power: (
    <>
      <path d="M8 2.6v5.2" />
      <path d="M11.6 4.6a5 5 0 1 1-7.2 0" />
    </>
  ),
  link: (
    <>
      <path d="M6.6 8.9a2.4 2.4 0 0 0 3.6.3l1.9-1.9a2.4 2.4 0 0 0-3.4-3.4l-1.1 1" />
      <path d="M9.4 7.1a2.4 2.4 0 0 0-3.6-.3L3.9 8.7a2.4 2.4 0 0 0 3.4 3.4l1.1-1" />
    </>
  ),
  sparkle: (
    <path d="M8 2.2 9.1 5.6 12.5 6.7 9.1 7.8 8 11.2 6.9 7.8 3.5 6.7 6.9 5.6zM12.4 10.8l.5 1.4 1.4.5-1.4.5-.5 1.4-.5-1.4-1.4-.5 1.4-.5z" />
  ),
  branch: (
    <>
      <circle cx="4.4" cy="3.8" r="1.6" />
      <circle cx="4.4" cy="12.2" r="1.6" />
      <circle cx="11.6" cy="8" r="1.6" />
      <path d="M4.4 5.4v5.2M4.4 8h5.6" />
    </>
  ),
  pin: (
    <>
      <path d="M9.4 2.6 13.4 6.6l-1.7.6-2.5 2.5-.3 2.4-4.4-4.4 2.4-.3 2.5-2.5z" />
      <path d="m5.5 10.5-2.6 2.6" />
    </>
  ),
};

export type IconName = keyof typeof P;

export function Icon({
  name,
  size = 16,
  ...rest
}: { name: string; size?: number } & SVGProps<SVGSVGElement>) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.2}
      strokeLinecap="round"
      strokeLinejoin="round"
      shapeRendering="geometricPrecision"
      {...rest}
    >
      {P[name] ?? null}
    </svg>
  );
}
