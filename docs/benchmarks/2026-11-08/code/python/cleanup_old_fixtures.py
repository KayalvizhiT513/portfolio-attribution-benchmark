"""Remove a fixed set of obsolete reproducible fixtures; journal before deleting.

Run from the repository root. Default is inventory only; --apply deletes.
Shared/current attribution inputs and benchmark evidence are never selected.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import shutil


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    recipes = {
        'data/100k': '.venv/bin/python python/generate.py --securities 100000 --days 252 --seed 42 --output data/100k',
        'data/brinson-15k': '.venv/bin/python python/generate_brinson_csv.py --securities 15000 --days 252 --segments 20 --seed 42 --output data/brinson-15k',
        'data/brinson-multi': '.venv/bin/python python/generate_multi_portfolios.py --source data/brinson-15k --output data/brinson-multi --portfolios 4',
        'rust/target/debug': 'cargo build --manifest-path rust/Cargo.toml (tests rebuild test artifacts on demand)',
    }
    selected = [root / name for name in recipes]
    # Refuse to break links outside the selected trees, including indirect links.
    for link in root.rglob('*'):
        if not link.is_symlink() or any(link.is_relative_to(p) for p in selected):
            continue
        target = link.resolve()
        if any(target.is_relative_to(p) for p in selected):
            raise RuntimeError(f'External dependency: {link} -> {target}')
    records = []
    for name, recipe in recipes.items():
        folder = root / name
        if not folder.exists():
            continue
        files = []
        for path in sorted(folder.rglob('*')):
            if path.is_symlink():
                files.append(dict(path=str(path.relative_to(root)), symlink=str(path.readlink())))
            elif path.is_file():
                entry = dict(path=str(path.relative_to(root)), bytes=path.stat().st_size,
                             allocated_bytes=path.stat().st_blocks * 512)
                if not name.startswith('rust/'):
                    with path.open('rb') as stream:
                        entry['sha256'] = hashlib.file_digest(stream, 'sha256').hexdigest()
                    if path.name == 'metadata.json':
                        entry['metadata'] = json.loads(path.read_text())
                files.append(entry)
        records.append(dict(directory=name, regenerate=recipe, files=files, status='planned'))
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    journal = root / 'docs/storage' / f'cleanup-{stamp}.json'
    journal.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(apply=args.apply, entries=records)
    def save():
        journal.write_text(json.dumps(payload, indent=2) + '\n')
    save()
    if args.apply:
        for record in records:
            shutil.rmtree(root / record['directory'])
            record['status'] = 'deleted'
            save()
    total = sum(f.get('allocated_bytes', 0) for r in records for f in r['files'])
    print(json.dumps(dict(journal=str(journal), apply=args.apply,
                          selected_allocated_bytes=total), indent=2))


if __name__ == '__main__':
    main()
