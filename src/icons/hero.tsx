import { type FC } from 'react';
import * as O from '@heroicons/react/24/outline';
import * as S from '@heroicons/react/24/solid';

type Props = { size?: number; className?: string; strokeWidth?: number; fill?: string };

function wrap(Icon: FC<{ className?: string; style?: React.CSSProperties }>): FC<Props> {
  return ({ size = 24, className, fill }) => {
    const style: React.CSSProperties = { width: size, height: size };
    if (fill && fill !== 'none') { style.fill = fill; style.stroke = 'none'; }
    return <Icon className={className} style={style} />;
  };
}

export const Bell = wrap(O.BellIcon);
export const Check = wrap(O.CheckIcon);
export const CheckCircle2 = wrap(O.CheckCircleIcon);
export const ChevronDown = wrap(O.ChevronDownIcon);
export const ChevronRight = wrap(O.ChevronRightIcon);
export const Clock = wrap(O.ClockIcon);
export const Copy = wrap(O.DocumentDuplicateIcon);
export const Database = wrap(O.CircleStackIcon);
export const Eye = wrap(O.EyeIcon);
export const FileText = wrap(O.DocumentTextIcon);
export const Filter = wrap(O.FunnelIcon);
export const Grid3X3 = wrap(O.Squares2X2Icon);
export const HardDrive = wrap(O.ServerStackIcon);
export const HelpCircle = wrap(O.QuestionMarkCircleIcon);
export const Layers = wrap(O.RectangleStackIcon);
export const LayoutGrid = wrap(O.ViewColumnsIcon);
export const Maximize2 = wrap(O.ArrowsPointingOutIcon);
export const Minimize2 = wrap(O.ArrowsPointingInIcon);
export const Minus = wrap(O.MinusIcon);
export const MoreHorizontal = wrap(O.EllipsisHorizontalIcon);
export const Play = wrap(S.PlayIcon);
export const Plus = wrap(O.PlusIcon);
export const Redo2 = wrap(O.ArrowUturnRightIcon);
export const Search = wrap(O.MagnifyingGlassIcon);
export const Settings = wrap(O.Cog6ToothIcon);
export const Sparkles = wrap(O.SparklesIcon);
export const Undo2 = wrap(O.ArrowUturnLeftIcon);
export const Upload = wrap(O.ArrowUpTrayIcon);
export const X = wrap(O.XMarkIcon);
export const ZoomIn = wrap(O.MagnifyingGlassPlusIcon);

// Circle has no direct outline equivalent; use a thin rounded border via a mini helper
export const Circle: FC<Props> = ({ size = 24, className }) => (
  <span className={className} style={{
    display: 'inline-block',
    width: size, height: size,
    borderRadius: '50%',
    border: '2px solid currentColor',
    boxSizing: 'border-box',
  }} />
);

// Type / text tool — no direct Heroicon; use a simple text cursor bar
export const Type: FC<Props> = ({ size = 24, className }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" className={className}>
    <path d="M4 7V4h16v3" /><path d="M9 20h6" /><path d="M12 4v16" />
  </svg>
);
