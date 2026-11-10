"""Render five structurally distinct SVGs that describe the implemented scenarios."""
from html import escape
from pathlib import Path
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT / 'docs/flows'
COLORS = {'read': ('#e0f2fe', '#0284c7'), 'filter': ('#fef3c7', '#d97706'), 'aggregate': ('#ede9fe', '#7c3aed'), 'compute': ('#dcfce7', '#16a34a'), 'write': ('#ffe4e6', '#e11d48'), 'note': ('#f1f5f9', '#64748b')}

class Canvas:
    def __init__(self, title, subtitle, height=1250):
        self.height = height
        self.parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="1440" height="{height}" viewBox="0 0 1440 {height}" role="img" aria-labelledby="title desc">', f'<title id="title">{escape(title)}</title>', f'<desc id="desc">{escape(subtitle)}</desc>', '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto"><path d="M0 0L10 5L0 10Z" fill="#475569"/></marker></defs>', f'<rect width="1440" height="{height}" fill="white"/>', '<style>text{font-family:Arial,Helvetica,sans-serif;fill:#0f172a}.title{font-size:32px;font-weight:700}.sub{font-size:18px;fill:#475569}.head{font-size:20px;font-weight:700}.body{font-size:17px}.label{font-size:16px;fill:#475569}.edge{fill:none;stroke:#475569;stroke-width:2.2;marker-end:url(#arrow)}</style>']
        self.text(65, 58, [title], 'title', 'start')
        self.text(65, 94, [subtitle], 'sub', 'start')
    def text(self, x, y, lines, style='body', anchor='middle', step=25):
        self.parts.append(f'<text x="{x}" y="{y}" class="{style}" text-anchor="{anchor}">')
        self.parts.extend(f'<tspan x="{x}" dy="{0 if i==0 else step}">{escape(str(line))}</tspan>' for i, line in enumerate(lines))
        self.parts.append('</text>')
    def box(self, x, y, w, h, color, title, lines=()):
        fill, stroke = COLORS[color]
        self.parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" fill="{fill}" stroke="{stroke}" stroke-width="2"/>')
        self.text(x+w/2, y+30, [title], 'head')
        self.text(x+w/2, y+61, lines)
    def edge(self, d, label=None, xy=None):
        self.parts.append(f'<path d="{d}" class="edge"/>')
        if label:
            self.text(*xy, [label], 'label')
    def diamond(self, cx, cy, w, h, lines):
        self.parts.append(f'<path d="M{cx} {cy-h/2} L{cx+w/2} {cy} L{cx} {cy+h/2} L{cx-w/2} {cy} Z" fill="#fff7ed" stroke="#d97706" stroke-width="2"/>')
        self.text(cx, cy-8 if len(lines)>1 else cy+6, lines)
    def finish(self, name):
        self.text(65, self.height-18, ['Every execution validates attribution and writes the final CSV or Parquet report.'], 'label', 'start')
        self.parts.append('</svg>')
        svg='\n'.join(self.parts)
        ET.fromstring(svg)
        (DEST/name).write_text(svg)


def security():
    c=Canvas('01  Security: daily calculation, full-history held-security output', 'Compute the full union • link across all dates • write held securities plus a benchmark-only bucket', 1280)
    c.box(75,145,570,110,'read','STREAM PORTFOLIO • one date',['A: weight 60%, return 2%','B: weight 40%, return −1%'])
    c.box(795,145,570,110,'read','STREAM INDEX • same date',['A: weight 50%, return 1%','C: weight 50%, return 0%'])
    c.edge('M360 255 V310 H720 V345');c.edge('M1080 255 V310 H720')
    c.box(300,345,840,105,'aggregate','ALIGN THE UNION USING DENSE ID SLOTS',['Validate dates; normalize each side’s full weights independently.','Missing-side return uses the held-side return.'])
    for x,title,lines in [(75,'A • held on both sides',['Wp = 0.60; Wb = 0.50','Rp = 0.02; Rb = 0.01']), (510,'B • portfolio only',['Wp = 0.40; Wb = 0','Rp = Rb = −0.01']), (945,'C • index only',['Wp = 0; Wb = 0.50','Rp = Rb = 0'])]:
        c.box(x,515,420,110,'compute',title,lines)
    c.edge('M720 450 V480 H285 V515');c.edge('M720 480 V515');c.edge('M720 480 H1155 V515')
    c.edge('M285 625 V675 H720 V715');c.edge('M720 625 V715');c.edge('M1155 625 V675 H720')
    c.box(250,715,940,110,'compute','DAILY EFFECTS → FULL-HISTORY LINKING',['Calculate daily allocation, selection and interaction; reconcile daily active return.','Update linked per-security components; track IDs ever held by the portfolio.'])
    c.edge('M720 825 V885','EOF',(770,860))
    c.box(250,885,940,110,'write','AT EOF: WRITE HELD IDs + BENCHMARK-ONLY RECONCILIATION',['One row per held instrument / return type over the full requested range.','Sum never-held index instruments into one explicit residual row per return type.'])
    c.edge('M250 770 H35 V125 H360 V145','Next date: reuse reader buffers',(190,1090))
    c.box(250,1125,940,95,'note','DISTINGUISHING COST',['Full union is used for calculation; only 120,000 held-detail + 8 residual rows are written.'])
    c.finish('01-security.svg')


def subset():
    c=Canvas('02  Standalone subset: select → reject or retain → renormalize', 'The filter changes the investment universe on each side before attribution', 1460)
    c.box(75,140,1290,90,'filter','LOAD SELECTED IDs ONCE',['Example selection = {A, C}; precompute membership mask and compact selected-ID slots.'])
    c.box(100,285,520,105,'read','PARSE PORTFOLIO DAY',['A: 20%, B: 30%, C: 50%','Validate all rows, including excluded IDs.'])
    c.box(820,285,520,105,'read','PARSE INDEX DAY',['A: 10%, B: 60%, C: 30%','Input still must be read in full.'])
    c.edge('M360 230 V285');c.edge('M1080 230 V285')
    c.diamond(360,480,360,110,['Instrument selected?','Mask lookup for each row'])
    c.diamond(1080,480,360,110,['Instrument selected?','Mask lookup for each row'])
    c.edge('M360 390 V425');c.edge('M1080 390 V425')
    c.box(25,595,240,90,'note','EXCLUDE B',['No aggregate/output slot.'])
    c.box(1175,595,240,90,'note','EXCLUDE B',['No aggregate/output slot.'])
    c.edge('M180 480 H70 V595','No',(110,560));c.edge('M1260 480 H1350 V595','No',(1320,560))
    c.box(305,595,340,110,'filter','RETAIN A + C',['Selected portfolio weight = 70%','Fail if selected weight = zero.'])
    c.box(795,595,340,110,'filter','RETAIN A + C',['Selected index weight = 40%','Fail if selected weight = zero.'])
    c.edge('M360 535 V565 H475 V595','Yes',(410,564));c.edge('M1080 535 V565 H965 V595','Yes',(1025,564))
    c.box(225,780,430,120,'aggregate','PORTFOLIO ÷ 0.70',['A = 28.5714%; C = 71.4286%','Normalize weights AND weighted returns.','Use compact slots for selected IDs only.'])
    c.box(785,780,430,120,'aggregate','INDEX ÷ 0.40',['A = 25%; C = 75%','Normalize weights AND weighted returns.','Use this subset’s index return.'])
    c.edge('M475 705 V780');c.edge('M965 705 V780')
    c.edge('M440 900 V945 H720 V990');c.edge('M1000 900 V945 H720')
    c.box(225,990,990,110,'compute','CALCULATE DAILY SUBSET EFFECTS; LINK ACROSS ALL DATES',['Union the selected holdings; calculate base/local effects using renormalized weights.','Reconcile daily results, then update full-history linked accumulators.'])
    c.edge('M225 1045 H10 V265 H360 V285','Next date',(95,1090))
    c.edge('M720 1100 V1160','EOF',(770,1138))
    c.box(225,1160,990,110,'write','AT EOF: WRITE SELECTED HELD IDs + ANY INDEX-ONLY BUCKET',['No B rows; one row per held selected ID / return type over the full range.','EOF: flush/fsync; coordinator publishes files after all workers succeed.'])
    c.box(225,1320,990,90,'note','EFFICIENCY',['Read/parse remains proportional to input; aggregation/compute slots scale with selected IDs.'])
    c.finish('02-subset.svg')


def country(filename='03-country.svg'):
    c = Canvas('Country: security attribution before country aggregation',
               'Daily US reports • Rust and Go • paired complete-date tasks • CSV and Parquet', 1930)
    steps = [
        ('read', 'LOAD THE MASTER ONCE', ['160,000 instruments: 36 historical versions each, or one undated record.',
                                        'Share immutable ID and effective-date lookups across workers.']),
        ('note', 'PLAN 24 PAIRED DATE RANGES / 12 WORKERS', ['CSV: scan date boundaries. Parquet: inspect row-group date statistics.',
                                                            'Each task reads matching dates from the index and every portfolio.']),
        ('read', 'READ ALL 175,200,000 HOLDINGS ROWS; CHECK INPUT FIELDS', ['1,095 dates: 100,000 index + 60,000 portfolio holdings per date.',
                                                                   'Check dates, known IDs, duplicates, finite returns, nonnegative weights and daily weight sums.']),
        ('filter', 'LOOK UP COUNTRY ON EACH DATE; RETAIN US', ['Keep 99,000 index + 59,000 portfolio securities per date.',
                                                             '173,010,000 retained rows; non-US holdings are excluded.']),
        ('aggregate', 'NORMALIZE EACH SIDE; PRESERVE SECURITY IDs', ['Divide weights and contributions by the respective total US weight.',
                                                                     'Contribution = normalized security weight × security return, separately on each side.',
                                                                     'Build aligned per-security slots; retain one date per side per worker.']),
        ('note', 'MISSING SIDE: COPY THE RETURN; KEEP ITS WEIGHT AT ZERO', [
            'Portfolio-only A: Wp = 2%, rp = 5%; Wb = 0%, set rb = 5%. Total US benchmark RB = 3%.',
            'Allocation = (0.02 − 0) × (0.05 − 0.03) = 0.0004 = 4 basis points; selection = interaction = 0.',
            'Benchmark-only: Wp = 0%, set rp = rb. The same rule applies to zero-weight sides.',
            'Copied return is a formula input only: missing-side contribution remains zero.']),
        ('compute', 'COMPUTE BF EFFECTS FOR EVERY SECURITY IN THE UNION', ['A = (Wp-Wb) × (rb-RB); S = Wb × (rp-rb); I = (Wp-Wb) × (rp-rb).',
                                                                         'RB is the total US benchmark return. Include securities absent on either side.',
                                                                         '158,000 securities/date × 1,095 dates × 2 returns × 3 effects = 1,038,060,000 values.']),
        ('aggregate', 'SUM COMPUTED SECURITY EFFECTS INTO COUNTRY REPORTS', ['Sum allocation, selection, interaction, weights and contributions by country.',
                                                                            'Reconcile effects to active return. Retain compact daily reports, discard security buffers.']),
        ('write', 'COLLECT IN DATE ORDER AND WRITE 2,190 REPORT ROWS', ['One US row per date and return type; 20 columns including contributions.',
                                                                      'No cross-date linking. Finalize CSV or typed Zstd Parquet after successful processing.'])]
    y = 145
    for i, (color, title, lines) in enumerate(steps):
        h = 95 + 25 * (len(lines) - 1)
        c.box(100, y, 1240, h, color, title, lines)
        if i < len(steps)-1:
            c.edge(f'M720 {y+h} V{y+185}')
        y += 185
    c.text(720, 1845, ['Full-data US IDs are disjoint: existing missing-side convention assigns active return to allocation.',
                       'Shared-ID regression fixtures verify nonzero selection and interaction.'], 'sub')
    c.finish(filename)


def weekly():
    c=Canvas('04  Weekly: reduce daily effects to portfolio totals, then link', 'A streaming state machine • keep one week, no table of daily effects', 1440)
    c.box(180,140,1080,95,'read','READ NEXT MATCHED PORTFOLIO / INDEX DATE',['Normalize full weights; build daily security arrays; EOF closes the active week.'])
    c.edge('M720 235 V290')
    c.box(180,290,1080,110,'aggregate','ASSIGN MONDAY–SUNDAY WEEK KEY',['Sunday 2023-01-01 → week starting 2022-12-26','Monday 2023-01-02 through Sunday 2023-01-08 → week starting 2023-01-02'])
    c.edge('M720 400 V450')
    c.diamond(720,520,520,140,['Active week exists AND','new date has a different week key?'])
    c.box(980,650,400,130,'write','CLOSE ACTIVE WEEK',['Reconcile compounded active return.','Write one portfolio row per return type.','Reset active accumulator.'])
    c.edge('M980 520 H1180 V650','Yes: Monday boundary',(1190,560))
    c.box(330,710,570,110,'compute','SUM DAILY EFFECTS TO PORTFOLIO',['Reduce to allocation, selection, interaction for each return type.','Reconcile the daily sum before linking the portfolio totals.'])
    c.edge('M720 590 V710','No: first/same week',(550,650))
    c.edge('M1180 780 V850 H935 V765 H900')
    c.edge('M615 820 V885')
    c.box(300,885,840,130,'compute','UPDATE ACTIVE WEEK IN PLACE',['E ← E × (1 + index return today) + e_today × portfolio growth before today','Update compounded returns and sums of daily weights.','Retain one portfolio accumulator; no per-security period effect table.'])
    c.edge('M300 950 H55 V115 H720 V140','Next date',(145,1070))
    c.edge('M720 1015 V1090','At EOF',(770,1060))
    c.box(230,1090,980,115,'write','FLUSH THE LAST WEEK, EVEN IF PARTIAL',['Write observed dates and portfolio effects: 1,264 rows over the supplied history.','Flush/fsync; publish per-portfolio CSVs after worker validation.'])
    c.box(230,1260,980,100,'note','ILLUSTRATIVE WEEK OUTPUT',['Jan 1: partial prior week • Jan 2–8: one week • Jan 9: begins another bucket.'])
    c.finish('04-weekly.svg')


def monthly():
    c=Canvas('05  Monthly: partition calendar dates, emit linked period summaries', 'Calendar buckets differ from weekly buckets • same engine supports quarter/year keys', 1370)
    c.box(160,140,1120,100,'read','STREAM DAILY SECURITY INPUTS IN EFFECTIVE-DATE ORDER',['Maintain current portfolio/index day; reuse buffers; use shared immutable index data.'])
    c.edge('M720 240 V295')
    c.box(160,295,1120,100,'aggregate','SELECT CALENDAR BUCKET KEY',['month: YYYY-MM   |   quarter override: YYYY-Qn   |   year override: YYYY'])
    c.edge('M720 395 V435 H435 V485')
    c.box(85,485,700,155,'compute','JANUARY • ONE ACTIVE ACCUMULATOR',['Jan 1: sum security effects → initialize portfolio linked state','Jan 2 … Jan 30: sum daily effects → update same portfolio state','Jan 31: reduce daily effects → update same state','No daily security output history is retained.'])
    c.box(885,485,420,155,'compute','FEBRUARY • NEXT ACCUMULATOR',['Feb 1 creates a new bucket.','Feb 2 … Feb 28 update it.','Starts only after January is emitted.'])
    c.edge('M435 640 V705')
    c.diamond(435,775,610,140,['Next date is in a new month,','or input has reached EOF?'])
    c.edge('M130 775 H35 V435 H435 V485','No: keep accumulating',(210,690))
    c.edge('M435 845 V915','Yes: close observed period',(590,885))
    c.box(85,915,700,135,'write','EMIT JANUARY’S LINKED CSV ROWS',['One portfolio row per return type; no per-security monthly output.','Period returns = compounded returns, not sums of daily returns.','Portfolio/index report weights are 1; 288 rows over the full supplied history.'])
    c.edge('M785 975 H1345 V435 H1095 V485','Feb 1: reset and start February',(1100,865))
    c.edge('M1095 640 V1090')
    c.box(885,1090,420,120,'write','EMIT FEBRUARY AT NEXT BOUNDARY',['Continue month by month.','EOF also emits a partial final month.'])
    c.box(85,1125,700,145,'note','PORTFOLIO-COMPONENT LINKING, BASE AND LOCAL',['E_new = E_previous × (1 + b_today) + e_today × P_before','Only one portfolio-level calendar period is held per worker.','Final EOF: flush/fsync and publish validated files.'])
    c.finish('05-monthly.svg')


def main():
    DEST.mkdir(parents=True,exist_ok=True)
    for render in [security,subset,country,weekly,monthly]: render()
    for path in sorted(DEST.glob('*.svg')): ET.parse(path)
    print('Rendered five distinct scenario-specific SVG flowcharts')

if __name__=='__main__': main()
