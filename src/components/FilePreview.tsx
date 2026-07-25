import { useState, useMemo, useCallback } from 'react';

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

const Ink = '#1C1C1A';
const Paper = '#F0EFEB';
const Muted = '#8F8E88';
const Grid = '#DEDDD6';
const Faint = '#C6C5BF';
const HoverBg = '#EBEAE7';

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

function BarChart({ data, mx }: { data: number[]; mx?: number }) {
  const maxV = mx ?? Math.max(...data);
  const n = data.length;
  const sw = n > 20 ? 2 : n > 10 ? 3 : 5;
  const gap = 1;
  const totalW = n * (sw + gap);
  return (
    <svg viewBox={`0 0 ${Math.max(totalW + 4, 40)} 28`} preserveAspectRatio="xMidYMid meet" style={{ width: '100%', height: 28 }}>
      {data.map((v, i) => {
        const h = Math.max(1, (v / maxV) * 22);
        const x = 2 + i * (sw + gap);
        return (
          <rect key={i} x={x} y={26 - h} width={sw} height={h} fill={Ink} opacity={0.5 + rnd(i, 3) * 0.5} rx={1} />
        );
      })}
    </svg>
  );
}

function RungChart({ data }: { data: [string, number][] }) {
  const maxV = Math.max(...data.map(d => d[1]));
  const baseY = 130;
  const rungH = 5;
  return (
    <svg viewBox="0 0 300 150" preserveAspectRatio="xMidYMid meet" style={{ width: '100%', height: 150 }}>
      {data.map(([name, v], i) => {
        const y = baseY - i * (rungH + 3);
        const w = (v / maxV) * 160;
        const x = (300 - w) / 2;
        return (
          <g key={name}>
            <rect x={x} y={y} width={w} height={rungH} fill={Ink} rx={1} opacity={0.55} />
            <text x={12} y={y + rungH / 2 + 3} fontSize={7} fill={Muted} fontWeight={600} letterSpacing="0.05em">{name.slice(0, 4).toUpperCase()}</text>
            <text x={x - 5} y={y - 1} fontSize={7} fill={Ink} fontWeight={700} textAnchor="end">{v}</text>
          </g>
        );
      })}
      <line x1={10} y1={baseY + 2} x2={290} y2={baseY + 2} stroke={Grid} strokeWidth={0.8} />
    </svg>
  );
}

function ScatterChart({ data }: { data: [number, number][] }) {
  const xs = data.map(d => d[0]);
  const ys = data.map(d => d[1]);
  const xMin = Math.min(...xs), xMax = Math.max(...xs);
  const yMin = Math.min(...ys), yMax = Math.max(...ys);
  const x0 = 40, x1 = 270, yB = 120, yT = 10;
  const mX = (v: number) => x0 + ((v - xMin) / (xMax - xMin)) * (x1 - x0);
  const mY = (v: number) => yB - ((v - yMin) / (yMax - yMin)) * (yB - yT);
  return (
    <svg viewBox="0 0 300 140" preserveAspectRatio="xMidYMid meet" style={{ width: '100%', height: 140 }}>
      {Array.from({ length: 5 }, (_, g) => {
        const x = x0 + (g / 4) * (x1 - x0);
        return <line key={g} x1={x} y1={yT} x2={x} y2={yB} stroke={Grid} strokeWidth={0.4} />;
      })}
      {data.map(([a, m], i) => {
        const cx = mX(a), cy = mY(m);
        return (
          <g key={i}>
            <line x1={cx} y1={yB} x2={cx} y2={cy} stroke={Grid} strokeWidth={0.5} opacity={0.5} />
            <circle cx={cx} cy={cy} r={1.5 + rnd(i, 3) * 0.8} fill={Ink} />
          </g>
        );
      })}
      <line x1={x0} y1={yB} x2={x1} y2={yB} stroke={Grid} strokeWidth={0.8} />
      <text x={x0} y={yB + 10} fontSize={6} fill={Muted} fontWeight={500}>$0</text>
      <text x={x1} y={yB + 10} fontSize={6} fill={Muted} fontWeight={500} textAnchor="end">$500</text>
    </svg>
  );
}

export default function FilePreview() {
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
      Object.entries(counts).sort((a, b) => b[1] - a[1]).forEach(([, v]) => dist.push(v));
    } else if (TY[i] === 'num') {
      const bins = 8;
      const minV = min ? parseFloat(min) : 0;
      const maxV = max ? parseFloat(max) : 1;
      const bw = (maxV - minV) / bins || 1;
      const cts = Array(bins).fill(0);
      nums.forEach(v => cts[Math.min(bins - 1, Math.floor((v - minV) / bw))]++);
      dist.push(...cts);
    } else {
      const counts: Record<string, number> = {};
      strs.forEach((v: string) => { counts[v] = (counts[v] || 0) + 1; });
      Object.entries(counts).sort((a, b) => b[1] - a[1]).forEach(([, v]) => dist.push(v));
    }
    return { name: h, type: TY[i], uniq, missing, min, max, mean, dist, isNum: TY[i] === 'num', isCat: TY[i] === 'cat' };
  }), []);

  const maxU = Math.max(...profs.map(p => p.uniq));

  const sorted = useMemo(() => {
    let s = [...DATA];
    if (sortCol !== null) {
      s.sort((a, b) => {
        const va = a[sortCol], vb = b[sortCol];
        if (va === '' && vb === '') return 0;
        if (va === '') return 1;
        if (vb === '') return -1;
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
      const items = [...p.dist].sort((a: number, b: number) => b - a).slice(0, 8);
      return <div className="mt-2"><BarChart data={items} mx={mx} /></div>;
    }
    return null;
  }, []);

  const tabs = [
    { key: 'summary', label: 'Summary' },
    { key: 'preview', label: 'Preview', cnt: '80' },
    { key: 'schema', label: 'Schema', cnt: '9' },
    { key: 'relations', label: 'Relations' },
    { key: 'report', label: 'Report' },
  ] as { key: string; label: string; cnt?: string }[];

  return (
    <div className="h-full flex flex-col" style={{ color: Ink }}>
      {/* Card body with mono paper */}
      <div className="flex-1 overflow-y-auto px-6 py-5" style={{ backgroundColor: Paper }}>
        {/* Title */}
        <div className="mb-3">
          <h2 className="text-lg font-[700] tracking-[-0.03em] leading-tight m-0">Customer Orders</h2>
          <div className="text-[10.5px] mt-1 leading-relaxed" style={{ color: Muted }}>
            <b style={{ color: Ink }}>80</b> rows · <b style={{ color: Ink }}>9</b> columns · 4.2 KB · 58 customers · 12 categories · Jan–Jul 2026
          </div>
        </div>

        {/* Tabs */}
        <div className="flex gap-0 mb-4" style={{ borderBottom: `1px solid ${Grid}` }}>
          {tabs.map(t => (
            <button
              key={t.key}
              onClick={() => setActiveTab(t.key)}
              className="h-[34px] px-3 rounded-t-lg border-none bg-transparent font-sans text-[11.5px] font-medium cursor-pointer transition-colors relative"
              style={{ color: activeTab === t.key ? Ink : Muted, fontWeight: activeTab === t.key ? 600 : 500, background: activeTab === t.key ? 'transparent' : 'transparent' }}
            >
              {t.label}{t.cnt ? <span className="ml-1 text-[9px]" style={{ color: Faint }}>{t.cnt}</span> : null}
              {activeTab === t.key && <span className="absolute bottom-[-1px] left-1 right-1 h-[2px] rounded-full" style={{ background: Ink }} />}
            </button>
          ))}
        </div>

        {/* ── Summary ── */}
        {activeTab === 'summary' && (
          <div>
            <div className="text-[11.5px] mb-3" style={{ color: Muted }}>Click any card to explore the relevant view.</div>
            <div className="grid grid-cols-5 gap-3 mb-4">
              {[
                { val: '80', label: 'records', hint: 'view table', bar: 100, jump: 'preview' },
                { val: '9', label: 'fields', hint: 'view schema', bar: 100, jump: 'schema' },
                { val: '58', label: 'customers', hint: '72% repeat rate', bar: 72 },
                { val: '$147.82', label: 'avg order', hint: 'median $124', bar: 82, jump: 'relations' },
                { val: '$28.2k', label: 'total revenue', hint: 'top 10% → 28%', bar: 100, jump: 'report' },
              ].map((c) => (
                <div
                  key={c.label}
                  onClick={() => c.jump && setActiveTab(c.jump)}
                  className="rounded-2xl p-3 cursor-pointer transition-colors"
                  style={{ background: '#F5F5F3', cursor: c.jump ? 'pointer' : 'default' }}
                >
                  <div className="text-[22px] font-[800] leading-tight tracking-[-0.02em]" style={{ color: Ink }}>{c.val}</div>
                  <div className="text-[10px] mt-1" style={{ color: Muted }}>{c.label}</div>
                  <div className="text-[8.5px] mt-1" style={{ color: Faint }}>{c.hint}</div>
                  <div className="h-[2.5px] rounded-full mt-2 overflow-hidden" style={{ background: Grid }}>
                    <div className="h-full rounded-full transition-[width] duration-600" style={{ width: `${c.bar}%`, background: Ink }} />
                  </div>
                </div>
              ))}
            </div>
            <div className="flex gap-2.5 flex-wrap">
              <span className="inline-flex items-center gap-1.5 h-[26px] px-3 text-[11px] font-medium rounded-full" style={{ background: Ink, color: Paper }}>
                <span className="w-[7px] h-[7px] rounded-full" style={{ background: Paper }} />{sc.Completed || 0} Completed
              </span>
              <span className="inline-flex items-center gap-1.5 h-[26px] px-3 text-[11px] font-medium rounded-full" style={{ background: HoverBg, color: Ink }}>
                <span className="w-[7px] h-[7px] rounded-full" style={{ background: Ink }} />{sc.Pending || 0} Pending
              </span>
              <span className="inline-flex items-center gap-1.5 h-[26px] px-3 text-[11px] font-medium rounded-full" style={{ background: '#E8E7E3', color: Muted }}>
                <span className="w-[7px] h-[7px] rounded-full" style={{ background: Muted }} />{sc.Refunded || 0} Refunded
              </span>
            </div>
            <div className="mt-3 text-[9px] tracking-[0.08em] font-medium" style={{ color: Faint }}>DATASET · H1 2026 · SYNTHETIC</div>
          </div>
        )}

        {/* ── Preview ── */}
        {activeTab === 'preview' && (
          <div>
            <div className="text-[11.5px] mb-3" style={{ color: Muted }}>
              All 80 records, sortable. {page * PER + 1}–{Math.min((page + 1) * PER, DATA.length)} · Page {page + 1} of {tp}
            </div>
            <div className="overflow-x-auto">
              <table className="w-full border-collapse text-[11.5px] whitespace-nowrap">
                <thead>
                  <tr>
                    {H.map((h, i) => (
                      <th
                        key={h}
                        onClick={() => { setSortDir(sortCol === i && sortDir === 1 ? -1 : 1); setSortCol(i); }}
                        className="text-left font-[600] pb-1.5 pt-2 border-b text-[10px] tracking-[0.06em] uppercase cursor-pointer select-none transition-colors"
                        style={{ color: Muted, borderColor: Grid }}
                      >
                        {h}<span className="ml-1 text-[7.5px] font-[500] normal-case tracking-normal" style={{ color: Faint }}>{TY[i]}</span>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r, ri) => (
                    <tr key={ri} className="transition-colors" style={{ background: 'transparent' }}>
                      {r.map((v, ci) => (
                        <td
                          key={ci}
                          className="py-1 px-1.5 border-b"
                          style={{ borderColor: '#E8E7E3', fontWeight: TY[ci] === 'num' ? 500 : 400, textAlign: TY[ci] === 'num' ? 'right' : 'left', fontVariantNumeric: TY[ci] === 'num' ? 'tabular-nums' : undefined, fontSize: TY[ci] === 'num' ? '11px' : undefined }}
                        >{v || '—'}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="flex items-center gap-2.5 mt-2 text-[11px]" style={{ color: Muted }}>
              <button
                disabled={page === 0}
                onClick={() => setPage(p => p - 1)}
                className="h-6 px-2 rounded-md border font-sans text-[11px] font-medium cursor-pointer transition-colors disabled:cursor-default"
                style={{ borderColor: Grid, color: Ink, background: 'transparent', opacity: page === 0 ? 0.3 : 1 }}
              >← Prev</button>
              <span>{page + 1} / {tp}</span>
              <button
                disabled={page >= tp - 1}
                onClick={() => setPage(p => p + 1)}
                className="h-6 px-2 rounded-md border font-sans text-[11px] font-medium cursor-pointer transition-colors disabled:cursor-default"
                style={{ borderColor: Grid, color: Ink, background: 'transparent', opacity: page >= tp - 1 ? 0.3 : 1 }}
              >Next →</button>
            </div>
            <div className="mt-3 text-[9px] tracking-[0.08em] font-medium" style={{ color: Faint }}>80 ROWS · SORTABLE · PAGINATED</div>
          </div>
        )}

        {/* ── Schema ── */}
        {activeTab === 'schema' && (
          <div>
            <div className="text-[11.5px] mb-3" style={{ color: Muted }}>Each column's profile plus its value distribution.</div>
            <div className="grid grid-cols-2 gap-3">
              {profs.map((p, i) => (
                <div key={i} className="rounded-2xl p-3.5" style={{ background: '#F5F5F3' }}>
                  <div className="flex justify-between items-center mb-1">
                    <div className="font-[700] text-[10.5px] tracking-[0.04em] uppercase" style={{ color: Ink }}>{p.name}</div>
                    <div className="text-[9.5px]" style={{ color: Muted }}>{p.type}</div>
                  </div>
                  <div className="h-[2px] rounded-full overflow-hidden" style={{ background: Grid }}>
                    <div className="h-full rounded-full transition-[width] duration-600" style={{ width: `${p.uniq / maxU * 100}%`, background: Ink }} />
                  </div>
                  <div className="text-[10px] mt-1 leading-relaxed" style={{ color: Ink }}>
                    <span style={{ color: Muted }}>{p.type === 'num' ? 'range' : 'unique'}</span> {p.type === 'num' ? `${p.min}–${p.max} · ` : `${p.uniq}`}{p.type === 'num' ? <><span style={{ color: Muted }}>mean</span> {p.mean}</> : null}
                    {p.missing > 0 ? <span className="text-[9px] ml-1" style={{ color: Faint }}>· {p.missing} missing</span> : null}
                  </div>
                  {colChart(p)}
                </div>
              ))}
            </div>
            <div className="mt-3 text-[9px] tracking-[0.08em] font-medium" style={{ color: Faint }}>9 COLUMNS · AUTO-PROFILED · INLINE DISTRIBUTION</div>
          </div>
        )}

        {/* ── Relations ── */}
        {activeTab === 'relations' && (
          <div>
            <div className="text-[11.5px] mb-3" style={{ color: Muted }}>Cross-column relationships — how fields connect.</div>
            <div className="grid grid-cols-2 gap-3">
              <div className="rounded-2xl p-4" style={{ background: '#F5F5F3' }}>
                <span className="text-[8px] font-[700] tracking-[0.08em] px-2 py-0.5 rounded-full border border-dashed inline-block mb-3" style={{ borderColor: Muted, color: Muted }}>F8 PLUMB SCATTER</span>
                <ScatterChart data={DATA.map(r => [parseFloat(r[6]), parseFloat(r[7])] as [number, number]).filter(([a, m]) => !isNaN(a) && !isNaN(m))} />
                <div className="text-[10px] mt-2" style={{ color: Muted }}>Amount vs margin · each dot = one order</div>
              </div>
              <div className="flex flex-col gap-3">
                <div className="rounded-2xl p-4" style={{ background: '#F5F5F3' }}>
                  <div className="text-[11px] font-[700] tracking-[0.04em] uppercase mb-1" style={{ color: Ink }}>Correlation</div>
                  <div className="text-[24px] font-[800] leading-tight tracking-[-0.02em]" style={{ color: Ink }}>R² ≈ 0.18</div>
                  <div className="text-[10px] mt-1 leading-relaxed" style={{ color: Muted }}>Weak positive — pricing strategy matters more than order size.</div>
                </div>
                <div className="rounded-2xl p-4" style={{ background: '#F5F5F3' }}>
                  <div className="text-[11px] font-[700] tracking-[0.04em] uppercase mb-1" style={{ color: Ink }}>Category × Amount</div>
                  <div className="text-[24px] font-[800] leading-tight tracking-[-0.02em]" style={{ color: Ink }}>$42–$489</div>
                  <div className="text-[10px] mt-1 leading-relaxed" style={{ color: Muted }}>Software has the widest range; Pet &amp; Food cluster below $100.</div>
                </div>
              </div>
              <div className="rounded-2xl p-4" style={{ background: '#F5F5F3' }}>
                <span className="text-[8px] font-[700] tracking-[0.08em] px-2 py-0.5 rounded-full border border-dashed inline-block mb-3" style={{ borderColor: Muted, color: Muted }}>F1 RUNGS</span>
                <RungChart data={CATS.map(c => [c, DATA.filter(r => r[3] === c).length] as [string, number]).sort((a, b) => b[1] - a[1])} />
                <div className="text-[10px] mt-2" style={{ color: Muted }}>Orders by category · one rung = one order</div>
              </div>
              <div className="rounded-2xl p-4" style={{ background: '#F5F5F3' }}>
                <span className="text-[8px] font-[700] tracking-[0.08em] px-2 py-0.5 rounded-full border border-dashed inline-block mb-3" style={{ borderColor: Muted, color: Muted }}>F1 RUNGS</span>
                <RungChart data={([['Completed', sc.Completed || 0], ['Pending', sc.Pending || 0], ['Refunded', sc.Refunded || 0]] as [string, number][]).filter(([, v]) => v > 0)} />
                <div className="text-[10px] mt-2" style={{ color: Muted }}>Orders by status</div>
              </div>
            </div>
            <div className="mt-3 text-[9px] tracking-[0.08em] font-medium" style={{ color: Faint }}>CHARTS · SVG · INLINE</div>
          </div>
        )}

        {/* ── Report ── */}
        {activeTab === 'report' && (
          <div>
            <div className="text-[11.5px] mb-3" style={{ color: Muted }}>5 things worth knowing about this dataset.</div>
            {[
              { n: 1, tag: 'COMPLETENESS', text: <><b>98.75%</b> — 5 missing values. 4 in <code>margin_pct</code> (likely refunded orders without cost basis), 1 in <code>status</code>.</> },
              { n: 2, tag: 'DISTRIBUTION', text: <><b>Right-skewed</b> (mean $147.82 &gt; median $124). The top 10% of orders drive ~28% of revenue.</> },
              { n: 3, tag: 'CONCENTRATION', text: <><b>Electronics + Books + Clothing</b> = 41% of orders. Bottom 6 categories = 8 orders. Long-tail category strategy needs review.</> },
              { n: 4, tag: 'BEHAVIOR', text: <><b>Repeat customers spend 2.3× more</b> ($204 vs $89). 16 pending orders are mostly first-time buyers — possible onboarding friction.</> },
              { n: 5, tag: 'MARGIN', text: <><b>Software margins</b> (28–52%) beat Physical Goods (14–32%). The mix shift toward software is already happening — 22% of Q2 orders.</> },
            ].map(item => (
              <div key={item.n} className="flex gap-2.5 items-start py-2.5 border-b text-[11.5px] leading-relaxed" style={{ borderColor: '#ECEBE7', color: '#3a3a37' }}>
                <div className="w-[18px] h-[18px] rounded-full text-center leading-[18px] flex-shrink-0 mt-0.5 text-[8.5px] font-[700]" style={{ background: Ink, color: Paper }}>{item.n}</div>
                <div><span className="text-[8.5px] font-[600] rounded px-1.5 py-0.5 mr-1 uppercase" style={{ color: Muted, background: '#E8E7E3' }}>{item.tag}</span> {item.text}</div>
              </div>
            ))}
            <div className="mt-3 text-[9px] tracking-[0.08em] font-medium" style={{ color: Faint }}>AUTO-GENERATED · 5 INSIGHTS</div>
          </div>
        )}
      </div>
    </div>
  );
}
