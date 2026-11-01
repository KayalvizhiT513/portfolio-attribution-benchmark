"""Compare whole-date parallel country execution with serial streaming, including failures."""
import csv
import math
import subprocess
import tempfile
from pathlib import Path
from test_attribution import compare_files

ROOT = Path(__file__).resolve().parents[1]
BINARIES = {'rust': ROOT/'rust/target/release/portfolio-attribution', 'go': ROOT/'benchmark-go-attribution'}
HEADER = ['Entity', 'EffectiveDate', 'InstrumentId', 'Weight', 'BaseReturn', 'LocalReturn']


def fixture(folder, quoted=False):
    folder.mkdir()
    (folder/'security_master.csv').write_text(
        'InstrumentId,EffectiveDate,Currency,Country\n'
        'A,2023-02-01,GBP,GB\nA,2023-01-01,USD,US\nB,2023-01-01,USD,US\n')
    for entity, name in [('INDEX','index'), ('PF1','portfolio_PF1'), ('PF2','portfolio_PF2')]:
        with (folder/f'{name}.csv').open('w', newline='') as f:
            w=csv.writer(f, lineterminator='\r\n', quoting=csv.QUOTE_ALL if quoted=='single' else csv.QUOTE_MINIMAL)
            w.writerow(HEADER)
            for day in ['2023-01-30','2023-01-31','2023-02-01','2023-02-02','2023-02-03']:
                for sid, ret in [('A',0.1),('B',0.02 if entity=='INDEX' else 0.04)]:
                    w.writerow([entity+(',\nquoted' if quoted is True else ',quoted' if quoted=='single' else ''),day,sid,0.5,ret,ret+0.001])
        # Last records without a newline must still be included in the final range.
        p=folder/f'{name}.csv'
        p.write_bytes(b'\n'+p.read_bytes().removesuffix(b'\r\n'))


def run(binary, data, out, threads, limit=0, valid=True):
    p=subprocess.run([str(binary),'--scenario','country','--data',str(data),'--threads',str(threads),
        '--max-days',str(limit),'--output-dir',str(out),'--metrics',str(out/'metrics.csv')],capture_output=True,text=True)
    assert (p.returncode==0)==valid, (p.args,p.stdout,p.stderr)
    if valid:
        with (out/'metrics.csv').open() as f: metrics=next(csv.DictReader(f))
        assert int(metrics['days'])==(limit or 5)
        phases=['setup_seconds','master_load_seconds','boundary_plan_wall_seconds',
                'parallel_read_filter_aggregate_wall_seconds','ordered_compute_write_wall_seconds',
                'streaming_pipeline_wall_seconds','finalize_seconds','pipeline_overhead_seconds']
        assert all(float(metrics[k]) >= 0 for k in phases)
        assert math.isclose(sum(float(metrics[k]) for k in phases),float(metrics['wall_seconds']),rel_tol=1e-9,abs_tol=1e-7)

        return metrics
    assert not list(out.glob('country-*.csv')), 'failed run published output'


def main():
    comparisons=0
    with tempfile.TemporaryDirectory(prefix='country-parallel-') as tmp:
        root=Path(tmp)
        for quoted in [False, 'single', True]:
            data=root/f'data-{quoted}'; fixture(data,quoted)
            for language,binary in BINARIES.items():
                for limit in [0,2]:
                    serial=root/f'{language}-{quoted}-{limit}-serial'
                    baseline=run(binary,data,serial,1,limit)
                    for threads in [2,4,12]:
                        out=root/f'{language}-{quoted}-{limit}-{threads}'
                        metrics=run(binary,data,out,threads,limit)
                        for field in ['days','output_rows','report_rows']:
                            assert metrics[field]==baseline[field], (field,metrics,baseline)
                        if quoted is not True:
                            assert int(metrics['rows_read'])==6*(limit or 5), metrics
                            assert int(metrics['date_range_workers'])>0, metrics
                        for path in serial.glob('country-*.csv'):
                            count,delta=compare_files(path,out/path.name)
                            assert count==2*(limit or 5) and delta==0, (count,delta)
                            comparisons+=1
            for name in ['country-portfolio_PF1.csv','country-portfolio_PF2.csv']:
                compare_files(root/f'go-{quoted}-0-12'/name,root/f'rust-{quoted}-0-12'/name)
                comparisons+=1
        for case in ['duplicate','date-mismatch','missing','extra','mixed-entity','unsorted','bad-weight','invalid-date','before-master','field-count']:
            data=root/case;fixture(data)
            p=data/'portfolio_PF1.csv';lines=p.read_text().splitlines()
            if case=='field-count': lines[5]=lines[5].rsplit(',',1)[0]
            elif case=='duplicate': lines.insert(5,lines[5])
            elif case=='date-mismatch': lines=[s.replace('2023-02-01','2023-02-04') for s in lines]
            elif case=='missing': lines=lines[:-2]
            elif case=='extra': lines += [s.replace('2023-02-03','2023-02-04') for s in lines[-2:]]
            elif case=='mixed-entity': lines[-1]=lines[-1].replace('PF1','OTHER')
            elif case=='unsorted': lines[-2:]=[s.replace('2023-02-03','2023-01-01') for s in lines[-2:]]
            elif case=='bad-weight': lines[-1]=lines[-1].replace(',0.5,',',0.4,')
            elif case=='invalid-date': lines=[s.replace('2023-02-03','2023-02-29') for s in lines]
            elif case=='before-master':
                p=data/'security_master.csv';p.write_text(p.read_text().replace('2023-01-01','2023-02-01'));p=data/'portfolio_PF1.csv'
            p.write_text('\n'.join(lines)+'\n')
            for language,binary in BINARIES.items():
                for threads in [1,4]: run(binary,data,root/f'fail-{case}-{language}-{threads}',threads,valid=False)
    print(f'PASS: {comparisons} exact output comparisons; serial/parallel validation failures agree for 10 invalid fixtures in both engines.')


if __name__=='__main__': main()
