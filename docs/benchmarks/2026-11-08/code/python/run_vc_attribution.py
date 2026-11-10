"""Prepare the three VC source files for one six-input API call and verify output."""
import argparse
import datetime as dt
import json
import math
from pathlib import Path
import subprocess

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from test_six_inputs import rewrite_dates
from test_attribution import FIELDS, key, records, reference


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source',type=Path,default=Path('vc_gen_attribution_data'))
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--return-unit',choices=['percent','decimal'],required=True)
    a=p.parse_args()
    root=a.output.resolve(); root.mkdir(parents=True,exist_ok=False)
    inputs=root/'inputs';inputs.mkdir()
    days=[dt.date(2026,1,n) for n in (1,2,3)]
    master=pq.read_table(a.source/'SMF.parquet')
    securities=sorted(set(master['InstrumentId'].to_pylist()))
    aliases={sid:n+1 for n,sid in enumerate(securities)}
    master=master.append_column('AnalyticsSecId',pa.array([aliases[sid] for sid in master['InstrumentId'].to_pylist()],pa.int64()))
    rewrite_dates(inputs/'security_master.parquet',master)
    data={}
    for original,target in [('PortfolioReturns','portfolio_PF'),('IndexReturns','index')]:
        table=pq.read_table(a.source/f'{original}.parquet')
        if a.return_unit=='percent':
            for name in ('BaseReturn','LocalReturn'):
                table=table.set_column(table.schema.get_field_index(name),name,pc.divide(table[name],100.0))
        # Source weights are percentages. Normalize to the old input unit;
        # the engine separately renormalizes the selected US universe each day.
        table=table.set_column(table.schema.get_field_index('Weight'),'Weight',pc.divide(table['Weight'],100.0))
        rewrite_dates(inputs/f'{target}.parquet',table)
        groups={str(day):{} for day in days}
        for row in table.to_pylist():
            groups[str(row['EffectiveDate'])][row['InstrumentId']]=(row['Weight'],row['BaseReturn'],row['LocalReturn'])
        entity=table['Entity'][0].as_py()
        data[entity if target=='portfolio_PF' else 'INDEX']=list(groups.items())
        if target=='portfolio_PF': portfolio=entity
    account_schema=pa.schema([('AccountId',pa.string()),('EffectiveDate',pa.date32()),('PortfolioEntity',pa.string()),('BaseCurrency',pa.string()),('IsActive',pa.bool_())])
    fx_schema=pa.schema([('EffectiveDate',pa.date32()),('FromCurrency',pa.string()),('ToCurrency',pa.string()),('Rate',pa.float64())])
    analytics_schema=pa.schema([('SecId',pa.int64()),('Effective Date',pa.date32())]+[(f'Float{i:02}',pa.float64()) for i in range(1,16)])
    rewrite_dates(inputs/'accounts.parquet',pa.Table.from_pylist([dict(AccountId='ACCOUNT_PORT1',EffectiveDate=d,PortfolioEntity=portfolio,BaseCurrency='USD',IsActive=True) for d in days],schema=account_schema))
    rewrite_dates(inputs/'forex.parquet',pa.Table.from_pylist([dict(EffectiveDate=d,FromCurrency='USD',ToCurrency='USD',Rate=1.) for d in days],schema=fx_schema))
    rewrite_dates(inputs/'analytics.parquet',pa.Table.from_pylist([{'SecId':alias,'Effective Date':d,**{f'Float{i:02}':0. for i in range(1,16)}} for d in days for alias in aliases.values()],schema=analytics_schema))
    description=dict(source=str(a.source.resolve()),return_unit=a.return_unit,weight_unit='percent; converted to decimals',country_filter='US as of each calculation date',supporting_inputs='Generated one active USD account, USD/USD=1, analytics aliases with zero validation-only columns. These do not rewrite supplied returns.',original_files_modified=False)
    (root/'preparation.json').write_text(json.dumps(description,indent=2)+'\n')
    subprocess.run(['.venv/bin/python','python/load_attribution_api.py','--binary','rust/target/release/portfolio-attribution','--data',str(inputs),'--output',str(root/'api'),'--start-date',str(days[0]),'--end-date',str(days[-1]),'--workers','3','--worker-limit','12','--worker-trace','--smf-trace','records','--stage-profile','--concurrency','1','--repeats','1','--settle-seconds','0.2'],check=True)
    experiment=json.loads((root/'api/summary.json').read_text());response=experiment['batches'][0]['responses'][0]
    assert response['status']==200,response
    expected={}; selected=[]
    history=sorted(master.to_pylist(),key=lambda r:r['EffectiveDate'])
    for day in days:
        countries={}
        for row in history:
            if row['EffectiveDate']<=day:countries[row['InstrumentId']]=row['Country']
        single={entity:[values[n] for n in range(len(days)) if days[n]==day] for entity,values in data.items()}
        expected.update(reference(countries,single,portfolio,'country','day'))
        selected.append(dict(date=str(day),portfolio=[s for s in single[portfolio][0][1] if countries[s]=='US'],index=[s for s in single['INDEX'][0][1] if countries[s]=='US']))
    report=Path(response['body']['report']);actual=list(records(report))
    assert len(actual)==len(expected)
    for row in actual:
        for field,value in zip(FIELDS,expected[key(row)]):
            assert math.isclose(float(row[field]),value,rel_tol=1e-8,abs_tol=1e-10),(field,row,value)
    from inspect_smf_trace import validate_trace
    for trace in report.parent.glob('smf-trace-worker-*.jsonl'): validate_trace(trace,inputs)
    import pyarrow.csv as pacsv
    pacsv.write_csv(pq.read_table(report),root/'attribution.csv')
    validation=dict(**description,report=str(report),elapsed_seconds=response['body']['elapsed_seconds'],rows=len(actual),independent_oracle_matched=True,selected_securities=selected)
    (root/'validation.json').write_text(json.dumps(validation,indent=2)+'\n')
    print(json.dumps(validation,indent=2))


if __name__=='__main__':main()
