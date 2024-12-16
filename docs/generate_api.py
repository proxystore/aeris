"""Generate the code reference pages and navigation."""

from __future__ import annotations

from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()

PACKAGE_DIR = 'foobar/'

for path in sorted(Path(PACKAGE_DIR).rglob('**/*.py')):
    module_path = path.with_suffix('')
    doc_path = path.relative_to(PACKAGE_DIR).with_suffix('.md')
    full_doc_path = Path('api', doc_path)

    parts = tuple(module_path.parts)
    parts = tuple('.'.join(parts[: i + 1]) for i in range(len(parts)))

    if parts[-1].endswith('__init__'):
        parts = parts[:-1]
        doc_path = doc_path.with_name('index.md')
        full_doc_path = full_doc_path.with_name('index.md')
    elif parts[-1].endswith('__main__'):
        continue

    nav[parts] = doc_path.as_posix()

    with mkdocs_gen_files.open(full_doc_path, 'w') as fd:
        fd.write(f'::: {parts[-1]}')

    mkdocs_gen_files.set_edit_path(full_doc_path, path)

with mkdocs_gen_files.open('api/SUMMARY.md', 'w') as nav_file:
    nav_file.writelines(nav.build_literate_nav())
