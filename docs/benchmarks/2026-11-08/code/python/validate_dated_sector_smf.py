"""Replay sector history and verify every snapshot row against the as-of state."""
import argparse
import json
from pathlib import Path
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from prepare_sector_smf import SECTORS


def validate(history, snapshots):
    started=time.perf_counter()
    source=pq.ParquetFile(history/'security_master.parquet')
    daily=pq.ParquetFile(snapshots/'security_master.parquet')
    sector=np.full(1,-1,dtype=np.int16);effective=np.full(1,-1,dtype=np.int32)
    revision_count=np.zeros(1,dtype=np.int16)
    pending=0;changes=0;checked=0;days=set();sample=[]
    def decode(table):
        ids=pc.cast(pc.utf8_slice_codeunits(table['InstrumentId'],1),pa.int64()).to_numpy()
        codes=pc.index_in(table['Sector'],value_set=pa.array(SECTORS))
        assert codes.null_count==0,'unknown sector'
        dates=pc.cast(table['EffectiveDate'],pa.int32()).to_numpy()
        return ids,codes.to_numpy(),dates
    source_date_col=source.schema_arrow.get_field_index('EffectiveDate')
    snap_date_col=daily.schema_arrow.get_field_index('SnapshotDate')
    for group in range(daily.num_row_groups):
        stat=daily.metadata.row_group(group).column(snap_date_col).statistics
        assert stat.min==stat.max
        day=stat.min;days.add(str(day))
        while pending<source.num_row_groups:
            date_stat=source.metadata.row_group(pending).column(source_date_col).statistics
            assert date_stat.min==date_stat.max
            date=date_stat.min
            if date>day:break
            table=source.read_row_group(pending,columns=['InstrumentId','Sector','EffectiveDate'])
            ids,codes,dates=decode(table)
            size=int(ids.max())+1
            if size>len(sector):
                sector=np.pad(sector,(0,size-len(sector)),constant_values=-1)
                effective=np.pad(effective,(0,size-len(effective)),constant_values=-1)
                revision_count=np.pad(revision_count,(0,size-len(revision_count)))
            changed=(sector[ids]>=0)&(sector[ids]!=codes)
            changed_ids=ids[changed]
            assert np.all((changed_ids-1)%12+1==date.month)
            assert np.all((changed_ids-1)%28+1==date.day)
            if len(sample)<6 and len(changed_ids):
                sid=int(changed_ids[0]);new=int(codes[changed][0])
                sample.append({'security':f'S{sid:06}','effective_date':str(date),
                              'before':SECTORS[int(sector[sid])],'after':SECTORS[new]})
            revision_count[changed_ids]+=1;changes+=int(changed.sum())
            sector[ids]=codes;effective[ids]=dates;pending+=1
        table=daily.read_row_group(group,columns=['InstrumentId','Sector','EffectiveDate'])
        ids,codes,dates=decode(table)
        assert np.array_equal(codes,sector[ids]),('sector mismatch',day,group)
        assert np.array_equal(dates,effective[ids]),('effective date mismatch',day,group)
        checked+=len(table)
        if (group+1)%600==0:print(f'Validated {checked:,} snapshot rows',flush=True)
    present=sector>=0
    result={'rows_validated':checked,'snapshot_dates':len(days),'securities':int(present.sum()),
        'sector_changes':changes,'changes_per_security_min':int(revision_count[present].min()),
        'changes_per_security_max':int(revision_count[present].max()),
        'all_snapshot_sectors_match_independent_history_replay':True,
        'all_snapshot_effective_dates_match_history':True,'examples':sample,
        'validation_seconds':time.perf_counter()-started}
    (snapshots/'dated-sector-validation.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2));return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--history',type=Path,required=True)
    parser.add_argument('--snapshots',type=Path,required=True)
    args=parser.parse_args();validate(args.history,args.snapshots)
