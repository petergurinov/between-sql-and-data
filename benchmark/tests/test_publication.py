import csv, json, re, unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
class PublicationTest(unittest.TestCase):
    def test_registry_matches_result(self):
        cells=json.loads((ROOT/'experiment/cells.json').read_text())
        with (ROOT/'results/measurements.csv').open(encoding='utf-8',newline='') as f: ids={r['cell_id'] for r in csv.DictReader(f) if r['cell_id']}
        self.assertEqual({x['cell_id'] for x in cells},ids); self.assertEqual(len(ids),912)
    def test_public_names(self):
        cells=json.loads((ROOT/'experiment/cells.json').read_text())
        self.assertEqual({x['lane'] for x in cells},{'pg','ch'})
    def test_no_forbidden_terms_or_private_ipv4(self):
        bad=re.compile('|'.join(['mana'+'ged','self[-_ ]?hos'+'ted',r'\bp'+'sh'+r'\b',r'\bc'+'sh'+r'\b']),re.I)
        private=re.compile(r'(?<![0-9])(?:10\.\d+\.\d+\.\d+|192\.168\.\d+\.\d+|172\.(?:1[6-9]|2\d|3[01])\.\d+\.\d+)(?![0-9])')
        hits=[]
        for p in ROOT.rglob('*'):
            if not p.is_file() or p.name in {'LICENSE','test_publication.py'} or p.suffix in {'.xlsx','.pyc'} or '.git' in p.parts: continue
            try: text=p.read_text(encoding='utf-8')
            except UnicodeDecodeError: continue
            if bad.search(text) or private.search(text): hits.append(str(p.relative_to(ROOT)))
        self.assertEqual(hits,[])
if __name__=='__main__': unittest.main()
