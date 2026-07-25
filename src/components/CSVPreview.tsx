import React, { useState, useMemo, useCallback } from 'react';

const CATS = ['Books', 'Electronics', 'Clothing', 'Home', 'Beauty', 'Sports', 'Food', 'Toys', 'Music', 'Software', 'Office', 'Pet'];
const PRODS: Record<string, string[]> = {
  Books: ['Design of Everyday Things', 'Thinking Fast & Slow', 'Sapiens', 'Mono Design Zine'],
  Electronics: ['USB-C Hub 7in1', 'Wireless Mouse M1', 'LED Desk Lamp', 'NC Earbuds'],
  Clothing: ['Merino Tee', 'Denim Jacket', 'Linen Shirt', 'Wool Beanie'],
  Home: ['Ceramic Mug Set', 'Bamboo Cutting Board', 'Linen Throw', 'Scented Candle'],
  Beauty: ['Vitamin C Serum', 'Moisturizer SPF30', 'Bamboo Brush Set', 'Hair Oil'],
  Sports: ['Yoga Mat Premium', 'Resistance Bands', 'Foam Roller', 'Jump Rope Pro'],
  Food: ['Organic Coffee Beans', 'Matcha Powder', 'Hot Sauce Trio', 'Protein Bars'],
  Toys: ['Wooden Puzzle', 'Building Blocks', 'Board Game Mono', 'Stuffed Fox'],
  Music: ['Vinyl Cleaner Kit', 'Headphone Stand', 'Guitar Picks', 'Sheet Music'],
  Software: ['Studio Pro Licence', 'Analytics Licence', 'Design Suite', 'API Credits'],
  Office: ['Standing Desk', 'Ergo Foot Rest', 'Desk Organizer', 'Monitor Arm'],
  Pet: ['Cat Toy Pack', 'Dog Leash Retract', 'Pet Bed Medium', 'Grooming Kit'],
};
const NAMES = ['Alice Chen', 'Bob M.', 'Clara J.', 'David K.', 'Elena R.', 'Felix O.', 'Grace L.', 'Hiro T.', 'Isabel S.', 'James W.', 'Keiko T.', 'Leo A.', 'Maria G.', 'Nina P.', 'Omar H.', 'Priya S.', 'Quinn M.', 'Ravi D.', 'Sofia B.', 'Tao W.', 'Uma P.'];
const H = ['order_id', 'customer_id', 'name', 'category', 'product', 'date', 'amount', 'margin_pct', 'status'];
const TY = ['id', 'id', 'string', 'cat', 'string', 'date', 'num', 'num', 'cat'];

function rnd(i: number, k: number): number {
  return Math.abs(((i * 73856093) ^ (k * 19349663)) % 1000) / 1000;
}
function pick<T>(arr: T[], i: number): T {
  return arr[Math.floor(rnd(i, 1) * arr.length)];
}

function genData() {
  const rows: string[][] = [];
  for (let i = 0; i < 80; i++) {
    const cat = pick(CATS, i + 7);
    const prod = pick(PRODS[cat], i + 5);
    const amt = 12.5 + rnd(i * 3 + 1, 7) * 477.5;
    const mrg = 8.7 + rnd(i * 5 + 2, 11) * 43.6;
    const mval = rnd(i + 2, 13) < 0.05 ? '' : mrg.toFixed(1);
    const stat = rnd(i + 3, 17) < 0.012 ? '' : pick(['Completed', 'Completed', 'Completed', 'Pending', 'Refunded'], i + 11);
    const d = new Date(2026, 0, 12 + Math.floor(rnd(i + 1, 9) * 188));
    rows.push([
      'ORD-' + (10000 + i).toString().slice(-4),
      'CUST-' + String(100 + Math.floor(rnd(i + 2, 3) * 58)).slice(-3),
      pick(NAMES, i + 3),
      cat,
      prod,
      d.toISOString().split('T')[0],
      amt.toFixed(2),
      mval,
      stat,
    ]);
  }
  return rows;
}

const DATA = genData();

const Ink = '#1C1C1A';
const Muted = '#8F8E88';
const Grid = '#DEDDD6';

function SVGEl(tag: string, attrs: Record<string, string>, children?: React.ReactNode): React.ReactElement {
  const merged: Record<string, React.ReactNode> = { ...attrs };
  if (children !== undefined) merged['children'] = children;
  return React.createElement(tag, merged);
}

function BarChart({ data, maxVal }: { data: number[]; maxVal?: number }) {
  const mx = maxVal ?? Math.max(...data);
  const n = data.length;
  const sw = n > 20 ? 2 : n > 10 ? 3 : 5;
  const gap = 1;
  const totalW = n * (sw + gap);
  return (
    <svg viewBox={`0 0 ${totalW + 4} 28`} preserveAspectRatio="xMidYMid meet" style={{ width: '100%', height: 28 }}>
      {data.map((v, i) => {
        const h = Math.max(1, (v / mx) * 22);
        const x = 2 + i * (sw + gap);
        const y = 26 - h;
        return SVGEl('rect', {
          x: String(x),
          y: String(y),
          width: String(sw),
          height: String(h),
          fill: Ink,
          opacity: String(0.5 + rnd(i, 3) * 0.5),
          rx: '1',
        });
      })}
    </svg>
  );
}

function RungChart({ data }: { data: [string, number][] }) {
  const maxV = Math.max(...data.map(d => d[1]));
  const baseY = 140;
  const rungH = 5;
  return (
    <svg viewBox="0 0 300 160" preserveAspectRatio="xMidYMid meet" style={{ width: '100%', height: 160 }}>
      {data.map(([name, v], i) => {
        const y = baseY - i * (rungH + 2);
        const w = (v / maxV) * 180;
        const x = (300 - w) / 2;
        return (
          <g key={name}>
            <rect x={String(x)} y={String(y)} width={String(w)} height={String(rungH)} fill={Ink} rx="1" opacity="0.6" />
            <text x="8" y={String(y + rungH / 2 + 3)} fontSize="7" fill={Muted} fontWeight="600" letterSpacing="0.05em">{name.slice(0, 4).toUpperCase()}</text>
            <text x={String(x - 6)} y={String(y - 2)} fontSize="7" fill={Ink} fontWeight="700" textAnchor="end">{v}</text>
          </g>
        );
      })}
      <line x1="0" y1={String(baseY + 2)} x2="300" y2={String(baseY + 2)} stroke={Grid} strokeWidth="0.8" />
    </svg>
  );
}

function ScatterChart({ data }: { data: [number, number][] }) {
  const xs = data.map(d => d[0]);
  const ys = data.map(d => d[1]);
  const xMin = Math.min(...xs), xMax = Math.max(...xs);
  const yMin = Math.min(...ys), yMax = Math.max(...ys);
  const x0 = 40, x1 = 280, yB = 130, yT = 10;
  const mX = (v: number) => x0 + ((v - xMin) / (xMax - xMin)) * (x1 - x0);
  const mY = (v: number) => yB - ((v - yMin) / (yMax - yMin)) * (yB - yT);
  return (
    <svg viewBox="0 0 320 160" preserveAspectRatio="xMidYMid meet" style={{ width: '100%', height: 160 }}>
      {Array.from({ length: 5 }, (_, g) => {
        const x = x0 + (g / 4) * (x1 - x0);
        return SVGEl('line', { x1: String(x), y1: String(yT), x2: String(x), y2: String(yB), stroke: Grid, strokeWidth: '0.4' });
      })}
      {Array.from({ length: 4 }, (_, g) => {
        const y = yT + ((g + 1) / 5) * (yB - yT);
        return SVGEl('line', { x1: String(x0), y1: String(y), x2: String(x1), y2: String(y), stroke: Grid, strokeWidth: '0.4' });
      })}
      {data.map(([a, m], i) => {
        const cx = mX(a), cy = mY(m);
        return (
          <g key={i}>
            <line x1={String(cx)} y1={String(yB)} x2={String(cx)} y2={String(cy)} stroke={Grid} strokeWidth="0.5" opacity="0.5" />
            <circle cx={String(cx)} cy={String(cy)} r={String(1.5 + rnd(i, 3) * 0.8)} fill={Ink} />
          </g>
        );
      })}
      <text x={String(x0)} y={String(yB + 10)} fontSize="6" fill={Muted} fontWeight="500">$0</text>
      <text x={String(x1)} y={String(yB + 10)} fontSize="6" fill={Muted} fontWeight="500" textAnchor="end">$500</text>
    </svg>
  );
}

const CSVPreview: React.FC = () => {
  const [activeTab, setActiveTab] = useState('summary');
  const [page, setPage] = useState(0);
  const [sortCol, setSortCol] = useState<number | null>(null);
  const [sortDir, setSortDir] = useState(1);
  const PER = 12;

  const sc = useMemo(() => {
    const s: Record<string, number> = {};
    DATA.forEach(r => { const v = r[8]; if (v) s[v] = (s[v] || 0) + 1; });
    return s;
  }, []);

  const profs = useMemo(() => H.map((h, i) => {
    const vals = DATA.map(r => r[i]);
    const nums = vals.map(Number).filter(v => !isNaN(v) && v !== null) as number[];
    const strs = vals.filter((v: string | null) => v !== null);
    const missing = vals.filter((v: string | null) => v === null).length;
    const uniq = new Set(strs).size;
    const min = nums.length ? Math.min(...nums).toFixed(1) : null;
    const max = nums.length ? Math.max(...nums).toFixed(1) : null;
    const mean = nums.length ? (nums.reduce((a, b) => a + b, 0) / nums.length).toFixed(1) : null;
    const dist: number[] = [];
    if (TY[i] === 'cat') {
      const counts: Record<string, number> = {};
      strs.forEach((v: string) => { counts[v] = (counts[v] || 0) + 1; });
      const entries = Object.entries(counts).sort((a, b) => b[1] - a[1]);
      entries.forEach(([, v]) => dist.push(v));
    } else if (TY[i] === 'num') {
      const bins = 8, bw = ((max ? parseFloat(max) : 0) - (min ? parseFloat(min) : 0)) / bins || 1;
      const cts = Array(bins).fill(0);
      nums.forEach(v => cts[Math.min(bins - 1, Math.floor((v - (min ? parseFloat(min) : 0)) / bw))]++);
      dist.push(...cts);
    } else {
      const counts: Record<string, number> = {};
      strs.forEach((v: string) => { counts[v] = (counts[v] || 0) + 1; });
      const entries = Object.entries(counts).sort((a, b) => b[1] - a[1]);
      entries.forEach(([, v]) => dist.push(v));
    }
    return { name: h, type: TY[i], uniq, missing, min, max, mean, total: vals.length, dist, isNum: TY[i] === 'num', isCat: TY[i] === 'cat' };
  }), []);

  const maxU = Math.max(...profs.map(p => p.uniq));

  const sorted = useMemo(() => {
    let s = [...DATA];
    if (sortCol !== null) {
      s.sort((a, b) => {
        const va = a[sortCol], vb = b[sortCol];
        if (va === null && vb === null) return 0;
        if (va === null) return 1;
        if (vb === null) return -1;
        const na = parseFloat(va), nb = parseFloat(vb);
        return !isNaN(na) && !isNaN(nb) ? sortDir * (na - nb) : sortDir * va.localeCompare(vb);
      });
    }
    return s;
  }, [sortCol, sortDir]);

  const tp = Math.ceil(DATA.length / PER);
  const rows = sorted.slice(page * PER, (page + 1) * PER);

  const colChart = useCallback((p: typeof profs[0]) => {
    if (!p.dist || p.dist.length === 0) return null;
    if (p.isCat || (p.dist.length <= 15 && p.dist.every((v: number) => Number.isInteger(v)))) {
      const mx = Math.max(...p.dist);
      const items = p.dist.sort((a: number, b: number) => b - a).slice(0, 8);
      return (
        <div className="mt-2">
          <BarChart data={items} maxVal={mx} />
        </div>
      );
    }
    return null;
  }, []);

  return (
    <div className="h-full flex flex-col">
      {/* Card Header */}
      <div className="flex justify-between items-start mb-3 flex-wrap gap-2">
        <div>
          <div className="text-[15px] font-[800] tracking-[-0.03em] leading-tight">Customer Orders</div>
          <div className="text-[10.5px] text-gray-400 mt-1 leading-relaxed">
            <b>80</b> rows · <b>9</b> columns · <b>4.2 KB</b> · 58 customers · 12 categories · Jan–Jul 2026
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 mb-3 border-b border-gray-200 pb-0">
        {['summary', 'preview', 'schema', 'relations', 'report'].map(name => (
          <button
            key={name}
            onClick={() => setActiveTab(name)}
            className={`h-[34px] px-3 rounded-t-lg border-none bg-transparent font-sans text-[11.5px] font-medium text-gray-400 cursor-pointer transition-colors ${activeTab === name ? 'text-gray-900 font-semibold' : 'hover:text-gray-900 hover:bg-gray-100'}`}
          >
            {name === 'summary' ? 'Summary' : name === 'preview' ? <span>Preview<span className="ml-1 text-[9px] text-gray-300 font-normal">80</span></span> : name === 'schema' ? <span>Schema<span className="ml-1 text-[9px] text-gray-300 font-normal">9</span></span> : name.charAt(0).toUpperCase() + name.slice(1)}
            {activeTab === name && <span className="absolute bottom-0 left-4 right-4 h-[2px] bg-gray-900" />}
          </button>
        ))}
      </div>

      {/* Panels */}
      <div className="flex-1 overflow-y-auto">
        {/* Summary */}
        {activeTab === 'summary' && (
          <div>
            <div className="text-[11.5px] text-gray-400 mb-3">Click any card to explore the relevant view.</div>
            <div className="grid grid-cols-5 gap-3 mb-3">
              <div className="bg-gray-50 rounded-xl p-3 cursor-pointer hover:bg-gray-100 transition-colors" onClick={() => setActiveTab('preview')}>
                <div className="text-[22px] font-[800] leading-tight tracking-[-0.02em]">80</div>
                <div className="text-[10px] text-gray-400 mt-1">records</div>
                <div className="text-[8.5px] text-gray-300 mt-1">view table</div>
                <div className="h-[2.5px] bg-gray-200 rounded-full mt-2 overflow-hidden"><div className="h-full bg-gray-900 rounded-full" style={{ width: '100%' }} /></div>
              </div>
              <div className="bg-gray-50 rounded-xl p-3 cursor-pointer hover:bg-gray-100 transition-colors" onClick={() => setActiveTab('schema')}>
                <div className="text-[22px] font-[800] leading-tight tracking-[-0.02em]">9</div>
                <div className="text-[10px] text-gray-400 mt-1">fields</div>
                <div className="text-[8.5px] text-gray-300 mt-1">view schema</div>
                <div className="h-[2.5px] bg-gray-200 rounded-full mt-2 overflow-hidden"><div className="h-full bg-gray-900 rounded-full" style={{ width: '100%' }} /></div>
              </div>
              <div className="bg-gray-50 rounded-xl p-3">
                <div className="text-[22px] font-[800] leading-tight tracking-[-0.02em]">58</div>
                <div className="text-[10px] text-gray-400 mt-1">customers</div>
                <div className="text-[8.5px] text-gray-300 mt-1">72% repeat rate</div>
                <div className="h-[2.5px] bg-gray-200 rounded-full mt-2 overflow-hidden"><div className="h-full bg-gray-900 rounded-full" style={{ width: '72%' }} /></div>
              </div>
              <div className="bg-gray-50 rounded-xl p-3 cursor-pointer hover:bg-gray-100 transition-colors" onClick={() => setActiveTab('relations')}>
                <div className="text-[22px] font-[800] leading-tight tracking-[-0.02em]">$147.82</div>
                <div className="text-[10px] text-gray-400 mt-1">avg order</div>
                <div className="text-[8.5px] text-gray-300 mt-1">median $124</div>
                <div className="h-[2.5px] bg-gray-200 rounded-full mt-2 overflow-hidden"><div className="h-full bg-gray-900 rounded-full" style={{ width: '82%' }} /></div>
              </div>
              <div className="bg-gray-50 rounded-xl p-3 cursor-pointer hover:bg-gray-100 transition-colors" onClick={() => setActiveTab('report')}>
                <div className="text-[22px] font-[800] leading-tight tracking-[-0.02em]">$28.2k</div>
                <div className="text-[10px] text-gray-400 mt-1">total revenue</div>
                <div className="text-[8.5px] text-gray-300 mt-1">top 10% → 28%</div>
                <div className="h-[2.5px] bg-gray-200 rounded-full mt-2 overflow-hidden"><div className="h-full bg-gray-900 rounded-full" style={{ width: '100%' }} /></div>
              </div>
            </div>
            <div className="flex gap-2.5 flex-wrap">
              <span className="flex items-center gap-1.5 h-[26px] px-3 bg-gray-900 text-white rounded-full text-[11px] font-medium"><span className="w-1.5 h-1.5 bg-white rounded-full" />{(sc.Completed || 0)} Completed</span>
              <span className="flex items-center gap-1.5 h-[26px] px-3 bg-gray-100 text-gray-900 rounded-full text-[11px] font-medium"><span className="w-1.5 h-1.5 bg-gray-900 rounded-full" />{(sc.Pending || 0)} Pending</span>
              <span className="flex items-center gap-1.5 h-[26px] px-3 bg-gray-50 text-gray-500 rounded-full text-[11px] font-medium"><span className="w-1.5 h-1.5 bg-gray-400 rounded-full" />{(sc.Refunded || 0)} Refunded</span>
            </div>
            <div className="mt-3 text-[9px] text-gray-300 tracking-wider font-medium">DATASET · H1 2026 · SYNTHETIC</div>
          </div>
        )}

        {/* Preview Table */}
        {activeTab === 'preview' && (
          <div>
            <div className="text-[11.5px] text-gray-400 mb-3">All 80 records, sortable. <span id="rowInfo">{page * PER + 1}–{Math.min((page + 1) * PER, DATA.length)}</span></div>
            <div className="overflow-x-auto">
              <table className="w-full border-collapse text-[11.5px] whitespace-nowrap">
                <thead>
                  <tr>
                    {H.map((h, i) => (
                      <th
                        key={h}
                        onClick={() => { setSortDir(sortCol === i && sortDir === 1 ? -1 : 1); setSortCol(i); }}
                        className="text-left font-semibold text-gray-400 pb-1.5 pt-2 border-b border-gray-200 text-[10px] tracking-wider uppercase cursor-pointer select-none hover:text-gray-900 transition-colors"
                      >
                        {h}
                        <span className="ml-1 text-[7.5px] text-gray-300 font-normal normal-case tracking-normal">{TY[i]}</span>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r, ri) => (
                    <tr key={ri} className="hover:bg-gray-50 transition-colors">
                      {r.map((v, ci) => (
                        <td key={ci} className={`py-1 px-1.5 border-b border-gray-100${TY[ci] === 'num' ? ' text-right font-medium text-[11px] tabular-nums' : ''}`}>{v !== null ? v : '—'}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="flex items-center gap-2.5 mt-2 text-[11px] text-gray-400">
              <button disabled={page === 0} onClick={() => setPage(p => p - 1)} className="h-6 px-2 rounded-md border border-gray-200 bg-transparent font-sans font-medium text-gray-900 text-[11px] cursor-pointer hover:bg-gray-100 transition-colors disabled:opacity-30 disabled:cursor-default">← Prev</button>
              <span className="text-[11px]">{page + 1} / {tp}</span>
              <button disabled={page >= tp - 1} onClick={() => setPage(p => p + 1)} className="h-6 px-2 rounded-md border border-gray-200 bg-transparent font-sans font-medium text-gray-900 text-[11px] cursor-pointer hover:bg-gray-100 transition-colors disabled:opacity-30 disabled:cursor-default">Next →</button>
            </div>
            <div className="text-[9px] text-gray-300 tracking-wider font-medium mt-1">80 ROWS · SORTABLE · PAGINATED</div>
          </div>
        )}

        {/* Schema */}
        {activeTab === 'schema' && (
          <div>
            <div className="text-[11.5px] text-gray-400 mb-3">Each column's profile plus its value distribution.</div>
            <div className="grid grid-cols-2 gap-2.5">
              {profs.map((p, i) => {
                const fill = p.uniq / maxU * 100;
                const stats = p.type === 'num' ? `<span>range</span> ${p.min}–${p.max} · <span>mean</span> ${p.mean}` : `<span>unique</span> ${p.uniq}`;
                const miss = p.missing > 0 ? `<span style="color:#B0AFA9;font-size:9px"> · ${p.missing} missing</span>` : '';
                return (
                  <div key={i} className="bg-gray-50 rounded-xl p-3">
                    <div className="flex justify-between items-center mb-0.5">
                      <div className="font-[700] text-[10.5px] tracking-wider uppercase text-gray-900">{p.name}</div>
                      <div className="text-[9.5px] text-gray-400">{p.type}</div>
                    </div>
                    <div className="h-[2.5px] bg-gray-200 rounded-full overflow-hidden"><div className="h-full bg-gray-900 rounded-full" style={{ width: `${fill}%` }} /></div>
                    <div className="text-[10px] text-gray-900 mt-1 leading-relaxed">{stats}{miss}</div>
                    {colChart(p)}
                  </div>
                );
              })}
            </div>
            <div className="text-[9px] text-gray-300 tracking-wider font-medium mt-3">9 COLUMNS · AUTO-PROFILED · INLINE DISTRIBUTION</div>
          </div>
        )}

        {/* Relations */}
        {activeTab === 'relations' && (
          <div>
            <div className="text-[11.5px] text-gray-400 mb-3">Cross-column relationships — how fields connect.</div>
            <div className="grid grid-cols-2 gap-3">
              <div className="bg-gray-50 rounded-xl p-3.5">
                <span className="text-[8px] font-[700] tracking-wider px-2 py-0.5 rounded-full border border-dashed border-gray-400 text-gray-400 inline-block mb-2">F8 PLUMB SCATTER</span>
                <ScatterChart data={DATA.map(r => [parseFloat(r[6]), parseFloat(r[7])] as [number, number]).filter(([a, m]) => !isNaN(a) && !isNaN(m))} />
                <div className="text-[10px] text-gray-400 mt-2">Amount vs margin · each dot = one order</div>
              </div>
              <div>
                <div className="bg-gray-50 rounded-xl p-3.5 mb-2.5">
                  <div className="text-[11px] font-[700] tracking-wider uppercase text-gray-900 mb-1">Correlation</div>
                  <div className="text-[24px] font-[800] leading-tight tracking-[-0.02em]">R² ≈ 0.18</div>
                  <div className="text-[10px] text-gray-400 mt-1 leading-relaxed">Weak positive — pricing strategy matters more than order size.</div>
                </div>
                <div className="bg-gray-50 rounded-xl p-3.5">
                  <div className="text-[11px] font-[700] tracking-wider uppercase text-gray-900 mb-1">Category × Amount</div>
                  <div className="text-[24px] font-[800] leading-tight tracking-[-0.02em]">$42–$489</div>
                  <div className="text-[10px] text-gray-400 mt-1 leading-relaxed">Software has the widest range; Pet &amp; Food cluster below $100.</div>
                </div>
              </div>
              <div className="bg-gray-50 rounded-xl p-3.5">
                <span className="text-[8px] font-[700] tracking-wider px-2 py-0.5 rounded-full border border-dashed border-gray-400 text-gray-400 inline-block mb-2">F1 RUNGS</span>
                <RungChart data={CATS.map(c => [c, DATA.filter(r => r[3] === c).length] as [string, number]).sort((a, b) => b[1] - a[1])} />
                <div className="text-[10px] text-gray-400 mt-2">Orders by category · one rung = one order</div>
              </div>
              <div className="bg-gray-50 rounded-xl p-3.5">
                <span className="text-[8px] font-[700] tracking-wider px-2 py-0.5 rounded-full border border-dashed border-gray-400 text-gray-400 inline-block mb-2">F1 RUNGS</span>
                <RungChart data={([['Completed', (sc.Completed || 0) as number], ['Pending', (sc.Pending || 0) as number], ['Refunded', (sc.Refunded || 0) as number]] as [string, number][]).filter(([, v]) => v > 0)} />
                <div className="text-[10px] text-gray-400 mt-2">Orders by status</div>
              </div>
            </div>
            <div className="text-[9px] text-gray-300 tracking-wider font-medium mt-3">CHARTS · SVG · INLINE</div>
          </div>
        )}

        {/* Report */}
        {activeTab === 'report' && (
          <div>
            <div className="text-[11.5px] text-gray-400 mb-3">5 things worth knowing about this dataset.</div>
            {[
              { n: 1, tag: 'COMPLETENESS', text: (<><b>98.75%</b> — 5 missing values. 4 in <code>margin_pct</code> (likely refunded orders without cost basis), 1 in <code>status</code>.</>), },
              { n: 2, tag: 'DISTRIBUTION', text: (<><b>Right-skewed</b> (mean $147.82 &gt; median $124). The top 10% of orders drive ~28% of revenue.</>), },
              { n: 3, tag: 'CONCENTRATION', text: (<><b>Electronics + Books + Clothing</b> = 41% of orders. Bottom 6 categories = 8 orders. Long-tail category strategy needs review.</>), },
              { n: 4, tag: 'BEHAVIOR', text: (<><b>Repeat customers spend 2.3× more</b> ($204 vs $89). 16 pending orders are mostly first-time buyers — possible onboarding friction.</>), },
              { n: 5, tag: 'MARGIN', text: (<><b>Software margins</b> (28–52%) beat Physical Goods (14–32%). The mix shift toward software is already happening — 22% of Q2 orders.</>), },
            ].map(item => (
              <div key={item.n} className="py-2.5 border-b border-gray-100 flex gap-2.5 items-start text-[11.5px] leading-relaxed text-gray-600">
                <div className="w-[18px] h-[18px] rounded-full bg-gray-900 text-white text-[8.5px] font-bold text-center leading-[18px] flex-shrink-0 mt-0.5">{item.n}</div>
                <div><span className="text-[8.5px] font-semibold text-gray-400 bg-gray-100 rounded px-1.5 py-0.5 mr-1 uppercase">{item.tag}</span> {item.text}</div>
              </div>
            ))}
            <div className="text-[9px] text-gray-300 tracking-wider font-medium mt-3">AUTO-GENERATED · 5 INSIGHTS</div>
          </div>
        )}
      </div>
    </div>
  );
};

export default CSVPreview;
