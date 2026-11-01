"""Render the implemented daily country attribution path as a standalone SVG."""
from html import escape
from pathlib import Path
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT/'docs/flows/country-parallel-end-to-end.svg'
W,H = 1900,3460
PALETTE = {
    'read':('#edf6ff','#2563eb'), 'filter':('#fff7df','#b77908'),
    'aggregate':('#f3eeff','#7951b0'), 'compute':('#eaf8f0','#17815b'),
    'write':('#fff0f3','#c13e62'), 'control':('#f2f5f9','#64748b'),
    'reference':('#fff4cd','#b77908'),
}
parts=[]

def text(x,y,lines,style='body',anchor='start',step=25):
    if isinstance(lines,str): lines=[lines]
    parts.append(f'<text x="{x}" y="{y}" class="{style}" text-anchor="{anchor}">')
    for i,line in enumerate(lines):
        parts.append(f'<tspan x="{x}" dy="{0 if i==0 else step}">{escape(line)}</tspan>')
    parts.append('</text>')


def box(x,y,w,h,title,lines,color='control',step=23):
    fill,stroke=PALETTE[color]
    parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill="{fill}" stroke="{stroke}" stroke-width="1.6"/>')
    text(x+22,y+33,title,'node-title')
    text(x+22,y+65,lines,step=step)
    assert y+65+(len(lines)-1)*step+6 <= y+h, (title,'text height')
    for line in lines:
        assert len(line)*9.0 < w-24, (title,line,'review line width')


def edge(path,color='#62748a',dashed=False):
    dash=' stroke-dasharray="7 5"' if dashed else ''
    marker='reference-arrow' if color=='#b77908' else 'arrow'
    parts.append(f'<path d="{path}" fill="none" stroke="{color}" stroke-width="2.2"{dash} marker-end="url(#{marker})"/>')


def section(y,number,title,subtitle):
    parts.append(f'<rect x="40" y="{y-24}" width="36" height="32" rx="9" fill="#17324d"/>')
    text(58,y,number,'section-number','middle')
    text(90,y,title,'section')
    if subtitle:text(90,y+29,subtitle,'caption')


def main():
    parts.clear()
    parts.extend([
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" role="img" aria-labelledby="title desc">',
        '<title id="title">Daily country attribution: parallel CSV and Parquet inputs through ordered Brinson-Fachler output</title>',
        '<desc id="desc">Both Go and Rust load an effective-dated master, partition whole dates, read one logical date at a time per worker, validate all holdings, retain US country records, normalize selected weights independently for portfolio and benchmark, aggregate weighted returns, collect results in original date order, and calculate allocation, selection, interaction and contributions. Total benchmark return is computed after US filtering and is used only as allocation reference, while selection uses benchmark group return. With one normalized US group, allocation and interaction are approximately zero. Reports are buffered, finalized, synced and published after success.</desc>',
        '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 Z" fill="#62748a"/></marker><marker id="reference-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 Z" fill="#b77908"/></marker></defs>',
        '''<style>text{font-family:Arial,Helvetica,sans-serif;fill:#17324d}.title{font-size:34px;font-weight:700}.subtitle{font-size:19px;fill:#51657b}.body{font-size:18px}.node-title{font-size:21px;font-weight:700}.section{font-size:22px;font-weight:700;letter-spacing:.5px}.section-number{font-size:18px;fill:white;font-weight:700}.caption{font-size:17px;fill:#51657b}.formula{font-family:Menlo,Consolas,monospace;font-size:18px;font-weight:600}.small{font-size:16px;fill:#51657b}.reference-label{font-size:16px;fill:#8b5b00;font-weight:700}</style>''',
        f'<rect width="{W}" height="{H}" fill="#ffffff"/>',
    ])
    text(50,55,'Country attribution: complete read → filter → group → compute → write flow','title')
    text(50,92,'Implemented daily US-only scenario • shared logic in Go and Rust • parallel CSV / Parquet • observed dataset shown','subtitle')
    for i,(key,label) in enumerate([('read','Read'),('filter','Filter'),('aggregate','Aggregate'),('compute','Compute'),('write','Write'),('control','Plan / merge')]):
        x=50+i*225;fill,stroke=PALETTE[key]
        parts.append(f'<rect x="{x}" y="115" width="18" height="18" rx="4" fill="{fill}" stroke="{stroke}"/>')
        text(x+28,130,label,'small')

    section(182,'01','LOAD ONCE AND PLAN COMPLETE DATES','Read units differ by format; the calculation universe and formulas do not.')
    box(65,232,500,230,'Effective-dated security master',[
        'Read the entire master before processing holdings.',
        'CSV records or Arrow batches (≤65,536 rows).',
        '5,760,000 versions / 160,000 instrument IDs.',
        'Index and sort versions by EffectiveDate.',
        'Reject blanks, bad dates and duplicate ID/date.',
        'Share immutable history with every worker.',
        'Currency is checked; no FX conversion here.',
    ],'read')
    box(635,232,580,190,'CSV planning: inspect date boundaries',[
        'Scan index.csv and portfolio_PF.csv.',
        'Find whole-date byte ranges; keep row order.',
        'Planner does not parse returns for attribution.',
        'Workers later seek and parse their own ranges.',
        'Multiline records → streaming fallback.',
    ],'read')
    box(1255,232,580,190,'Parquet planning: inspect the footer',[
        'Current files: 1,095 groups; one date per group.',
        'Statistics establish each group’s single date.',
        'Keep adjacent groups for the same date together.',
        'Assign disjoint complete-date group lists.',
        'Missing stats / mixed dates → streaming fallback.',
    ],'read')
    box(635,512,1200,155,'One bounded pool across both sources',[
        'Runtime CPU detection; recorded run: 12 workers and 24 tasks across index + portfolio.',
        'Each task owns a reader and loops through contiguous dates (up to 92 dates in this run).',
        'Each worker buffers ONE logical date at a time; it does not load an entire date range into RAM.',
        'Fallback: existing index-producer / portfolio-worker pipeline; downstream math is identical.',
    ])
    edge('M925 422 V512');edge('M1545 422 V512')
    text(935,470,'byte ranges','caption');text(1555,470,'row-group lists','caption')
    edge('M565 350 H598 V590 H635',dashed=True)
    text(590,495,'shared','small','end');text(590,516,'master','small','end')
    edge('M1235 667 V790 H490 V810');edge('M1235 790 H1410 V810')

    section(737,'02','WITHIN EACH RANGE WORKER: REPEAT FOR EACH DATE','Left and right are logical source lanes in the same pool, not fixed one-to-one thread pairs.')
    for x,side,label,count,keep,sym in [(90,'p','PORTFOLIO',60000,59000,'p'),(1010,'b','BENCHMARK / INDEX',100000,99000,'b')]:
        box(x,810,800,155,f'{label}: read one logical date',[
            'Required fields: Entity, EffectiveDate, InstrumentId,',
            'Weight, BaseReturn, LocalReturn.',
            'CSV: 1 MiB input buffer. Parquet: Arrow batches ≤65,536 rows.',
            f'{count:,} holdings/day; a complete date may span several batches / groups.',
        ],'read')
        box(x,1000,800,125,'Validate every row before filtering',[
            'Map instrument ID; require one entity and no duplicate instrument/date.',
            'Weights ≥0; finite numbers; returns >−1; valid increasing dates.',
            'Require original full-date weights to sum to 1 (tolerance 1e−6).',
        ],'read')
        box(x,1160,800,170,'Resolve the dated country → keep US only',[
            'For each holding date d, choose the latest master version with date ≤ d.',
            'No effective master record → fail. Retain only Country == "US".',
            f'Current data: keep {keep:,}; exclude 1,000 DE holdings per date.',
            f's_{sym} = sum of RETAINED original weights; require s_{sym} > 0.',
            'Discarded holdings were still read and validated.',
        ],'filter')
        box(x,1365,800,190,'Normalize and group by date + entity + country',[
            f'For each retained security i: normalized weight = w_i / s_{sym}.',
            f'W{sym},g = Σ(i in g) w_i / s_{sym}',
            f'C{sym},g,t = Σ(i in g) (w_i × return_i,t) / s_{sym}',
            'Repeat contributions for t = base and local; g = US in this scenario.',
            'Normalize by selected WEIGHT, not security count. Group before BF math.',
            'Day result stores W, C_base, C_local and presence per country.',
        ],'aggregate')
        box(x,1590,800,80,'Save Day aggregate → original source/date slot',[
            'Reuse the worker’s raw daily buffer for the next date in its assigned range.',
        ],'aggregate')
        cx=x+400
        for a,b in [(965,1000),(1125,1160),(1330,1365),(1555,1590)]:edge(f'M{cx} {a} V{b}')
    edge('M90 1630 H40 V887 H90',dashed=True)
    edge('M1810 1630 H1855 V887 H1810',dashed=True)
    parts.append('<text x="25" y="1325" class="small" transform="rotate(-90 25 1325)">More dates in this task</text>')
    parts.append('<text x="1875" y="1090" class="small" transform="rotate(90 1875 1090)">More dates in this task</text>')

    section(1710,'03','COLLECT AND ORDER; NO CROSS-DATE NUMERIC REDUCTION','A task finishing first does not change output order or the summation order inside a date.')
    box(90,1770,1720,135,'Barrier: all ranges must succeed → align daily aggregates',[
        'Collect 1,095 daily results per source into original positions; retain compact country aggregates, not all securities.',
        'Check range exhaustion, entity consistency across workers, and matching index/portfolio dates.',
        'Pair by DATE + COUNTRY, not matching security IDs. “Merge” means reorder/align; do not average or sum different dates.',
    ])
    edge('M490 1670 V1680 H1770 V1770');edge('M1410 1670 V1680 H1770')

    section(1950,'04','CALCULATE ONE DATE AT A TIME, IN ORDER','Run independently for base and local returns. Country uses a fresh one-day period; there is no linking across dates.')
    edge('M1790 1905 V1998 H490 V2015');edge('M1790 1998 H1410 V2015')
    box(90,2015,820,170,'Portfolio group return and total portfolio return',[
        'For each group g: rp,g = Cp,g / Wp,g when Wp,g > 0.',
        'Total portfolio return: RP = Σg Cp,g.',
        'Cp,g is the group’s weighted contribution after US renormalization.',
        'For this single US group: Wp,US ≈ 1 and RP ≈ rp,US.',
    ],'compute')
    box(1010,2015,800,170,'Benchmark group return versus TOTAL benchmark return',[
        'Group return: rb,g = Cb,g / Wb,g when Wb,g > 0.',
        'TOTAL benchmark return: RB = Σg Cb,g.',
        'RB is computed AFTER US filtering and renormalization.',
        'It is the US-only benchmark return, not the original full-index return.',
    ],'reference')
    edge('M490 2185 V2215 H355 V2270');edge('M490 2215 H950 V2270')
    edge('M1410 2185 V2215 H950');edge('M1410 2215 H1545 V2270')
    text(950,2204,'group weights and group returns','small','middle')
    edge('M1210 2185 V2243 H355 V2270','#b77908',True)
    text(665,2235,'RB → allocation reference','reference-label','middle')
    for x,title in [(90,'Allocation'),(685,'Selection'),(1280,'Interaction')]:
        fill,stroke=PALETTE['reference' if title=='Allocation' else 'compute']
        parts.append(f'<rect x="{x}" y="2270" width="530" height="185" rx="16" fill="{fill}" stroke="{stroke}" stroke-width="1.8"/>')
        text(x+22,2304,title,'node-title')
    text(112,2344,['A_g = (Wp,g − Wb,g)','      × (rb,g − RB)'],'formula')
    text(112,2410,['Uses group rb,g AND total RB.','Over/underweight × group excess return.'],'body')
    text(707,2344,'S_g = Wb,g × (rp,g − rb,g)','formula')
    text(707,2390,['Uses benchmark GROUP return rb,g.','Selection does not subtract total RB.'],'body')
    text(1302,2344,['I_g = (Wp,g − Wb,g)','      × (rp,g − rb,g)'],'formula')
    text(1302,2410,['Total effect for group g = A_g + S_g + I_g.'],'body')
    box(90,2520,1720,145,'Why the current US-only result has almost zero allocation and interaction',[
        'Only one retained country: Wp,US ≈ Wb,US ≈ 1, and RB ≈ rb,US. Thus A_US ≈ 0 and I_US ≈ 0 (floating-point rounding).',
        'S_US ≈ RP − RB; selection carries the US portfolio-versus-US-benchmark active return.',
        'Illustrative decimals, not measured data: RP = 0.008, RB = 0.005 → A = 0, S = 0.003, I = 0; Total = 0.003 (0.30 percentage points).',
        'General zero-group fallback: if Wb,g = 0 use rb,g = rp,g; if Wp,g = 0 use rp,g = rb,g. This US case has positive weight on both sides.',
    ],'compute')
    edge('M355 2455 V2505 H950 V2520');edge('M950 2455 V2520');edge('M1545 2455 V2505 H950')

    section(2710,'05','RECONCILE, APPEND OUTPUT ROWS, THEN FINALIZE','Return values and effect values are decimal returns. Allocation/selection are country-level calculations, not sums of security-level BF effects.')
    box(90,2750,1720,125,'Reconcile each date / return type before publishing',[
        'Require Σg (A_g + S_g + I_g) ≈ RP − RB (daily tolerance 1e−9). Check contribution and report-period reconciliation (1e−8).',
        'Portfolio contribution = Cp,g; benchmark contribution = Cb,g; active contribution = Cp,g − Cb,g.',
        'Any invalid input, unmatched date, failed range or failed reconciliation aborts success; no final report is published for that failure.',
    ],'compute')
    edge('M1770 2665 V2750')
    box(90,2930,820,190,'Map the daily values to the 20 output columns',[
        '8 context fields: scenario, entity, date range, days, row type, country, return type.',
        'Weights; Allocation A_g; Selection S_g; Interaction I_g; Total = A_g + S_g + I_g.',
        'PortfolioReturn = RP; BenchmarkReturn = RB; ActiveReturn = RP − RB.',
        'PortfolioContribution = Cp,g; BenchmarkContribution = Cb,g;',
        'ActiveContribution = Cp,g − Cb,g. Days = 1; Segment = US.',
    ],'write',23)
    box(1010,2930,800,190,'Append in DATE ORDER; format follows portfolio input',[
        'CSV input → CSV rows through a buffered writer (Go: 1 MiB buffer).',
        'Parquet input → typed Arrow output batches of at most 65,536 rows.',
        'Encode / flush a Zstd Parquet batch when full; keep typed numeric values.',
        'No worker writes the final report directly; the coordinator owns ordering.',
        'Current output: 2 rows/date × 1,095 dates = 2,190 rows per portfolio.',
    ],'write',23)
    edge('M950 2875 V2900 H490 V2930');edge('M910 3020 H1010')
    edge('M1810 3020 H1880 V1998 H950',dashed=True)
    parts.append('<text x="1896" y="2780" class="small" transform="rotate(-90 1896 2780)">Next date: repeat ordered compute and append</text>')
    box(340,3210,1220,140,'After the last date: finalize and publish',[
        'Flush remaining CSV / Arrow buffers; finish Parquet compression and footer; fsync and close the .partial file.',
        'After all reports succeed, rename .partial files to final output names. Record output sizes and runtime metrics.',
        'The output contains daily country results only; dates are never numerically merged into a full-history return.',
    ],'write')
    edge('M1410 3120 V3170 H950 V3210')
    text(1425,3153,'last date / EOF','caption')
    text(50,3390,'Source: go/cmd/attribution/{main.go, country_parallel.go, parquet_ranges.go, input.go, output.go}','small')
    text(50,3415,'Source: rust/src/{main.rs, country_parallel.rs, input.rs, output.rs} • Current daily country scenario; other scenarios use different grouping / linking.','small')
    parts.append('</svg>')
    svg='\n'.join(parts)+'\n'
    ET.fromstring(svg)
    DEST.parent.mkdir(parents=True,exist_ok=True)
    DEST.write_text(svg)
    print(DEST)


if __name__=='__main__':main()
