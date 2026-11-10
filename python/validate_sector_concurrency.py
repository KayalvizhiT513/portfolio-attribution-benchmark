"""Validate every combined sector/security output against a verified reference."""
import argparse
import json
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',type=Path,required=True)
    p.add_argument('--reference',type=Path,required=True);p.add_argument('--data',type=Path,required=True);args=p.parse_args()
    summary=json.loads((args.run/'summary.json').read_text())
    old=json.loads((args.reference/'summary.json').read_text())
    reference=pq.read_table(old['batches'][0]['responses'][0]['body']['report'])
    spec=json.loads((args.data/'manifest.json').read_text())['limited_occurrence']
    results=[]
    for batch in summary['batches']:
        for response in batch['responses']:
            assert response['status']==200,response
            path=Path(response['body']['report']);table=pq.read_table(path)
            assert table.schema==reference.schema and table.num_rows==reference.num_rows
            for column in table.column_names:
                a,b=table[column],reference[column]
                if pa.types.is_floating(a.type):
                    assert np.all(np.isfinite(a.to_numpy())) and np.allclose(a.to_numpy(),b.to_numpy(),rtol=1e-9,atol=1e-10),(path,column)
                else:assert a.equals(b),(path,column)
            for sid,expected in spec['securities'].items():
                selected=table.filter(pc.and_(pc.equal(table['RowType'],'security'),pc.equal(table['Security'],sid)))
                for label in pc.unique(selected['ReturnType']).to_pylist():
                    rows=selected.filter(pc.equal(selected['ReturnType'],label)).select(['Sector','Count']).to_pylist()
                    actual={r['Sector']:r['Count'] for r in rows}
                    assert len(actual)==len(rows) and actual==expected['sector_counts']
                    assert sum(actual.values())==spec['target_count']
            assert not Path(str(path)+'.partial').exists()
            results.append({'request_id':response['body']['request_id'],'http_status':response['status'],
                'client_seconds':response['seconds'],'output_rows':table.num_rows,'all_fields_match_reference':True,
                'selected_security_counts':spec['target_count'],'report':str(path)})
            print(f'Validated request {results[-1]["request_id"]}: {table.num_rows:,} rows',flush=True)
    result={'requests_validated':len(results),'reference':str(args.reference),'tolerance':{'relative':1e-9,'absolute':1e-10},
            'all_outputs_match_reference':True,'all_selected_counts_passed':True,'requests':results}
    (args.run/'validation.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k!='requests'},indent=2))


if __name__=='__main__':main()
