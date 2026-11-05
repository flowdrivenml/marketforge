"""Tiny document mutations only; no engine, database, or market-data access."""
from pathlib import Path
import tempfile
import unittest
import shutil

from audit_historical_catalog import ROOT, audit, differences, export_rows


class ReconciliationTests(unittest.TestCase):
    def test_retained_exports_and_provenance(self):
        report, provenance = audit(ROOT, ROOT / 'data/.config/historical')
        self.assertEqual(report['unapproved_discrepancies'], 0)
        self.assertEqual(report['counts']['exact_raw_formats'], 36)
        self.assertEqual(report['counts']['exact_normalization_rules'], 47)
        self.assertEqual(len(provenance['raw_formats']), 36)
        self.assertEqual(len(provenance['normalization_rules']), 49)
        self.assertTrue(all(x['original']['notes'] for x in provenance['raw_formats']))
        self.assertEqual(provenance['normalization_rules'][0]['original']['id'], 1)
        self.assertEqual(audit(ROOT, ROOT / 'data/.config/historical'), (report, provenance))

    def test_structural_comparison_preserves_types_presence_and_array_order(self):
        self.assertEqual(list(differences({'a': 1, 'b': 2}, {'b': 2, 'a': 1})), [])
        for a, b in [(1, True), ({}, {'a': None}), ([1, 2], [2, 1]), ({'x': 1}, {})]:
            self.assertTrue(list(differences(a, b)))

    def test_mutations_fail_without_broad_gate_exception(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'historical'
            shutil.copytree(ROOT / 'data/.config/historical', root)
            gate = root / 'profiles/depth/gateio.spot.spot.gate-b1.v1.toml'
            original = gate.read_text()
            gate.write_text(original.replace('"2" = "bid"', '"2" = "ask"'))
            report, _ = audit(ROOT, root)
            self.assertGreater(report['unapproved_discrepancies'], 0)
            gate.write_text(original)
            raw = root / 'raw_schemas/depth/gateio.depth.gate-b1.v1.toml'
            raw.write_text(raw.read_text().replace('"ordering" = "chronological"', '"ordering" = "unknown"'))
            report, _ = audit(ROOT, root)
            self.assertTrue(report['mismatched_raw_schemas'])
            shutil.copyfile(gate, gate.with_name('duplicate.toml'))
            report, _ = audit(ROOT, root)
            self.assertTrue(report['duplicate_identities'])
            gate.unlink()
            gate.with_name('duplicate.toml').unlink()
            report, _ = audit(ROOT, root)
            self.assertTrue(report['missing_records'])
            self.assertTrue(report['unresolved_mappings'])

    def test_export_parser_does_not_depend_on_row_order(self):
        source = ROOT / 'data/.config/historical/normalization rules.txt'
        expected = export_rows(source, False)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'rules.txt'
            path.write_text('\n'.join(reversed(source.read_text().splitlines())))
            self.assertEqual(export_rows(path, False), expected)


if __name__ == '__main__':
    unittest.main()
