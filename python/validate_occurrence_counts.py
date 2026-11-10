"""Validate expected sector/security date counts in a limited-occurrence report."""
import argparse
import json
from pathlib import Path
import pyarrow.parquet as pq


def validate(data,report):
    manifest=json.loads((data/'manifest.json').read_text());spec=manifest['limited_occurrence']
    rows=pq.read_table(report,columns=['RowType','Sector','Security','ReturnType','Count','Days']).to_pylist()
    labels={r['ReturnType'] for r in rows};assert labels
    checked={}
    for sid,expected in spec['securities'].items():
        checked[sid]={}
        for label in labels:
            selected=[r for r in rows if r['RowType']=='security' and r['Security']==sid and r['ReturnType']==label]
            keys=[r['Sector'] for r in selected];assert len(keys)==len(set(keys)),('duplicate sector/security key',sid)
            actual={r['Sector']:r['Count'] for r in selected}
            assert actual==expected['sector_counts'],(sid,label,actual,expected['sector_counts'])
            assert sum(actual.values())==spec['target_count'],(sid,label,actual)
            assert all(r['Days']==spec['dates'] for r in selected)
            checked[sid][label]={'Count':sum(actual.values()),'sector_counts':actual}
    for row in rows:assert 0<row['Count']<=row['Days']==spec['dates'],row
    result={'target_count':spec['target_count'],'days':spec['dates'],'checked':checked,'passed':True}
    report.with_name('occurrence-count-validation.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2));return result


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--data',type=Path,required=True)
    p.add_argument('--report',type=Path,required=True);args=p.parse_args();validate(args.data,args.report)
