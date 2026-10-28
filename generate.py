"""Generate a shared deterministic input fixture for both languages."""
import argparse, json
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
    shape = (args.days, args.securities)
    for key in ("wp", "wb"):
        x = rng.random(shape, dtype=np.float64)
        x /= x.sum(axis=1, keepdims=True)
        x.astype("<f8", copy=False).tofile(out / f"{key}.bin")
    rb = rng.uniform(-0.03, 0.03, size=shape)
    rp = np.clip(rb + rng.uniform(-0.0005, 0.0005, size=shape), -0.03, 0.03)
    rb.astype("<f8", copy=False).tofile(out / "rb.bin")
    rp.astype("<f8", copy=False).tofile(out / "rp.bin")
    (out / "metadata.json").write_text(json.dumps({
        "days": args.days, "securities": args.securities, "seed": args.seed,
        "dtype": "little-endian float64", "layout": "day-major"
    }, indent=2))
    print(f"Generated {args.days * args.securities * 4:,} float64 values in {out}")

if __name__ == "__main__":
    main()
