"""Whole-date Parquet group selection, ordered output and safe layout fallbacks."""
import csv
import json
import os
from pathlib import Path
import subprocess
import tempfile

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from benchmark_country_runtime import validate_metrics
from test_attribution import compare_files
from test_country_parallel import fixture, BINARIES


def prepare(source, target, group_rows=1, stats=True, strings=False):
    target.mkdir()
    for path in source.glob('*.csv'):
        types={'EffectiveDate':pa.string() if strings else pa.date32()}
        if path.stem!='security_master':
            types.update(Weight=pa.float64(),BaseReturn=pa.float64(),LocalReturn=pa.float64())
        table=pacsv.read_csv(path,convert_options=pacsv.ConvertOptions(column_types=types))
        # Required columns need not be in physical/schema order; nested extras
        # must not shift the date column's physical leaf selection.
        table=table.append_column('Ignored',pa.array([{'nested':1}]*table.num_rows))
        table=table.select(list(reversed(table.column_names)))
        pq.write_table(table,target/path.with_suffix('.parquet').name,row_group_size=group_rows,
                       write_statistics=stats,compression='zstd')


def execute(binary,data,out,threads,limit=0,valid=True):
    command=[str(binary),'--data',str(data),'--scenario','country','--threads',str(threads),
             '--max-days',str(limit),'--output-dir',str(out),'--metrics',str(out/'metrics.csv')]
    env={k:v for k,v in os.environ.items() if not k.startswith('ATTRIBUTION_')}
    env['ATTRIBUTION_PROFILE_READ']='1'
    process=subprocess.run(command,env=env,capture_output=True,text=True,timeout=120)
    assert (process.returncode==0)==valid,(command,process.stdout,process.stderr)
    if not valid:
        assert not list(out.glob('country-*.parquet'))
        return
    with (out/'metrics.csv').open() as f: m=next(csv.DictReader(f))
    validate_metrics(m)
    assert int(m['days'])== (limit or 5)
    return m


def main():
    count=0
    with tempfile.TemporaryDirectory(prefix='parquet-date-ranges-') as tmp:
        root=Path(tmp);source=root/'source';fixture(source)
        cases=[('split-date',1,True,False,True),('aligned',2,True,False,True),
               ('string-dates',1,True,True,True),('mixed-io',2,True,False,True),('mixed-date-groups',3,True,False,False),
               ('missing-statistics',1,False,False,False)]
        for label,size,stats,strings,parallel in cases:
            data=root/label;prepare(source,data,size,stats,strings)
            if label=='mixed-io':
                (data/'portfolio_PF1.parquet').unlink()
                (data/'portfolio_PF1.csv').symlink_to(source/'portfolio_PF1.csv')
            for language,binary in BINARIES.items():
                for limit in [0,2]:
                    serial=root/f'{label}-{language}-{limit}-serial'
                    execute(binary,data,serial,1,limit)
                    for threads in [2,4,12]:
                        out=root/f'{label}-{language}-{limit}-{threads}'
                        m=execute(binary,data,out,threads,limit)
                        assert (int(m['date_range_workers'])>0)==parallel,(label,m)
                        if parallel: assert int(m['rows_read'])==6*(limit or 5)
                        for path in serial.glob('country-*'):
                            n,d=compare_files(path,out/path.name)
                            assert n==2*(limit or 5) and d==0,(label,n,d)
                            count+=1
            for path in (root/f'{label}-rust-0-12').glob('country-*'):
                n,d=compare_files(path,root/f'{label}-go-0-12'/path.name)
                assert d==0;count+=1
        # Invalid data at group and worker boundaries must still fail.
        for case in ['duplicate','entity','unsorted','null','unknown','before-master']:
            bad=root/f'bad-csv-{case}';fixture(bad)
            path=bad/'portfolio_PF1.csv'
            with path.open(newline='') as f: rows=list(csv.reader(f))
            if case=='duplicate': rows.insert(5,rows[5][:])
            elif case=='entity': rows[-2][0]=rows[-1][0]='OTHER'
            elif case=='unsorted': rows[-2][1]=rows[-1][1]='2023-01-01'
            elif case=='unknown': rows[-1][2]='UNKNOWN'
            elif case=='before-master':
                master=bad/'security_master.csv';master.write_text(master.read_text().replace('2023-01-01','2023-02-01'))
            with path.open('w',newline='') as f:csv.writer(f).writerows(rows)
            data=root/f'bad-parquet-{case}';prepare(bad,data)
            if case=='null':
                path=data/'portfolio_PF1.parquet';table=pq.read_table(path)
                column=table.schema.get_field_index('BaseReturn');values=table.column(column).to_pylist();values[-1]=None
                table=table.set_column(column,'BaseReturn',pa.array(values,type=pa.float64()))
                pq.write_table(table,path,row_group_size=1)
            for language,binary in BINARIES.items():
                for threads in [1,4]: execute(binary,data,root/f'fail-{case}-{language}-{threads}',threads,valid=False)
    print(f'PASS: {count} exact Parquet serial/parallel comparisons; split-date groups, max-days, string dates, projection, layout fallbacks and six invalid fixtures verified in both engines.')


if __name__=='__main__': main()
