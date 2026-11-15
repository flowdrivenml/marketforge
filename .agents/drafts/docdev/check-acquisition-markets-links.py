"""Check authored docs and generated site; classify unrelated baseline changes."""
from pathlib import Path
from html.parser import HTMLParser
from urllib.parse import unquote,urlsplit
import hashlib,json,re,subprocess

class Parser(HTMLParser):
    def __init__(self):
        super().__init__(); self.links=[]; self.ids=set()
    def handle_starttag(self,tag,attrs):
        a=dict(attrs)
        if a.get('id'): self.ids.add(a['id'])
        for k in ('href','src'):
            if a.get(k):self.links.append(a[k])

def slug(text):return re.sub(r'[^\w\- ]','',text.lower()).replace(' ','-')
root=Path('docs/site').resolve();errors=[];checked=0
pages={}
for path in root.rglob('*.html'):
    p=Parser();p.feed(path.read_text());pages[path]=p
for path,p in pages.items():
    for link in p.links:
        u=urlsplit(link)
        if u.scheme or u.netloc:continue
        dest=(root/unquote(u.path.lstrip('/')) if u.path.startswith('/') else path.parent/unquote(u.path)).resolve() if u.path else path
        if dest.is_dir():dest=dest/'index.html'
        checked+=1
        if not dest.exists():errors.append(f'{path.relative_to(root)}: {link}')
        elif u.fragment and dest in pages and unquote(u.fragment) not in pages[dest].ids:errors.append(f'{path.relative_to(root)}: missing fragment {link}')

markdown=list(Path('docs/content').rglob('*.md'))+[Path('docs/README.md')]
for path in markdown:
    for link in re.findall(r'\]\(([^)]+)\)',path.read_text()):
        u=urlsplit(link)
        if u.scheme or u.netloc:continue
        dest=(path.parent/unquote(u.path)) if u.path else path
        if not dest.exists():errors.append(f'{path}: {link}')
        elif u.fragment and dest.suffix=='.md':
            ids={slug(t) for t in re.findall(r'^#+ (.+)$',dest.read_text(),re.M)}
            if u.fragment not in ids:errors.append(f'{path}: missing source anchor {link}')

baseline=json.loads(Path('.agents/drafts/docdev/acquisition-markets-baseline.json').read_text())
allowed={'docs/zensical.toml','docs/content/workflows/index.md','docs/content/configuration/index.md','docs/content/reference/index.md'}
other=[]
for name,digest in baseline['files'].items():
    if name in allowed:continue
    p=Path(name)
    if not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest()!=digest:other.append(name)
assert subprocess.check_output(['git','diff','--cached']).decode()==baseline['staged'],'Staged diff changed'
result={'markdown_files':len(markdown),'html_pages':len(pages),'local_html_links_assets':checked,'errors':errors,'baseline_differences_outside_assignment':other,'staged_diff_unchanged':True}
Path('.agents/drafts/docdev/acquisition-markets-link-results.json').write_text(json.dumps(result,indent=2))
assert not errors,'\n'.join(errors)
print(f'PASS: {len(markdown)} documentation Markdown files, {len(pages)} HTML pages, {checked} local HTML links/assets. Staged diff unchanged.')
print(f'Baseline comparison: {len(other)} differences outside authored documentation (see result file); no automatic edits/reverts.')
