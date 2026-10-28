"""Generate deterministic binary inputs using bounded per-day memory."""
import argparse
import json
from pathlib import Path

import numpy as np


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--securities", type=int, default=15000)
    p.add_argument("--days", type=int, default=252)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--output", default="data")
    args = p.parse_args()
    if args.securities < 1 or args.days < 1:
        p.error("dimensions must be positive")
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(args.seed)
    # Stream one trading day at a time. Avoid materializing all four full arrays.
    with (out / "wp.bin").open("wb") as wp_file, \
         (out / "wb.bin").open("wb") as wb_file, \
         (out / "rp.bin").open("wb") as rp_file, \
         (out / "rb.bin").open("wb") as rb_file:
        for _ in range(args.days):
            wp = rng.random(args.securities)
            wp /= wp.sum()
            wb = rng.random(args.securities)
            wb /= wb.sum()
            rb = rng.uniform(-0.03, 0.03, size=args.securities)
            rp = np.clip(
                rb + rng.uniform(-0.0005, 0.0005, size=args.securities),
                -0.03, 0.03
            )
            for f, array in ((wp_file, wp), (wb_file, wb),
                             (rp_file, rp), (rb_file, rb)):
                array.astype("<f8", copy=False).tofile(f)
    (out / "metadata.json").write_text(json.dumps({
        "days": args.days,
        "securities": args.securities,
        "seed": args.seed,
        "dtype": "little-endian float64",
        "layout": "day-major"
    }, indent=2))
    print(f"Generated {args.days * args.securities * 4:,} float64 values in {out}")


if __name__ == "__main__":
    main()
