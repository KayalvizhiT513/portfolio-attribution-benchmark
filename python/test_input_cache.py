"""Cache preparation must cover every input and happen before process timing."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from input_cache import input_paths, prepare_input_cache
from benchmark_native import measured


class CacheTests(unittest.TestCase):
    def test_linux_inputs_and_symlink(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ('security_master.csv', 'index.parquet', 'holdings.parquet', 'subset.csv'):
                (root / name).write_text('test')
            (root / 'portfolio_PF.parquet').symlink_to(root / 'holdings.parquet')
            command = ['engine', '--data', str(root), '--instruments-file=' + str(root / 'subset.csv')]
            self.assertEqual(len(input_paths(command)), 4)
            with patch('input_cache.sys.platform', 'linux'), patch('input_cache.os.posix_fadvise', create=True) as advise:
                result = prepare_input_cache(command, 'evict')
            self.assertEqual(advise.call_count, 4)
            self.assertFalse(result['cache_eviction_verified'])
            self.assertEqual(len(result['cache_input_files']), 4)

    def test_normal_never_reads_inputs(self):
        with patch('input_cache.input_paths', side_effect=AssertionError):
            self.assertEqual(prepare_input_cache(['engine'], 'normal')['input_cache_mode'], 'normal')

    def test_failure_prevents_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch('benchmark_native.prepare_input_cache', side_effect=RuntimeError('failed')), patch('benchmark_native.subprocess.Popen') as process:
                with self.assertRaises(RuntimeError):
                    measured(['engine'], Path(tmp) / 'run.log')
                process.assert_not_called()

    def test_macos_purge_and_failure(self):
        with patch('input_cache.input_paths', return_value=[]), patch('input_cache.sys.platform', 'darwin'), patch('input_cache.subprocess.run') as run:
            prepare_input_cache(['engine'], 'macos-purge')
            run.assert_called_once_with(['/usr/bin/sudo', '-n', '/usr/sbin/purge'], check=True)

    def test_actual_linux_eviction(self):
        if not hasattr(os, 'posix_fadvise'):
            self.skipTest('Linux only')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ('security_master.csv', 'index.csv', 'portfolio_PF.csv'):
                (root / name).write_bytes(b'x' * 8192)
            result = prepare_input_cache(['engine', '--data', str(root)], 'evict')
            self.assertEqual(len(result['cache_input_files']), 3)


if __name__ == '__main__':
    unittest.main()
