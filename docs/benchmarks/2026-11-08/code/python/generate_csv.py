"""Generate two independently shuffled, keyed CSV files for attribution benchmarks.

No positional correspondence between portfolio.csv and benchmark.csv is guaranteed.
Rows match on (date, security_id), never on row number.
"""
import argparse
import csv
from pathlib import Path
import numpy as np

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--securities", type=int, default=15000)
    p.add_argument("--days", type=int, default=252)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--output", default="data/csv-15k")
    args = p.parse_args()
    if args.securities < 1 or args.days < 1:
        p.error("securities and days must be positive")
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(args.seed)
    with (output / "portfolio.csv").open("w", newline="") as pf, (output / "benchmark.csv").open("w", newline="") as bm:
        p_writer, b_writer = csv.writer(pf), csv.writer(bm)
        header = ["date", "security_id", "weight", "return"]
        p_writer.writerow(header)
        b_writer.writerow(header)
        count = args.securities
        for day in range(args.days):
            pw = rng.random(count)
            bw = rng.random(count)
            pw /= pw.sum()
            bw /= bw.sum()
            br = rng.uniform(-0.03, 0.03, count)
            pr = np.clip(br + rng.uniform(-0.0005, 0.0005, count), -0.03, 0.03)
            p_order = rng.permutation(count)
            b_order = rng.permutation(count)
            date = f"D{day+1:04d}"
            for i in p_order:
                p_writer.writerow((date, f"SEC{i+1:06d}", format(pw[i], ".17g"), format(pr[i], ".17g")))
            for i in b_order:
                b_writer.writerow((date, f"SEC{i+1:06d}", format(bw[i], ".17g"), format(br[i], ".17g")))
    print(f"Wrote {args.days * args.securities:,} rows per file to {output}")
    print("Join keys: (date, security_id); per-file row orders are independently shuffled.")
if __name__ == "__main__":
    main()
