from pathlib import Path
import json
import os
import re
import subprocess
import tarfile
import yaml

tracked = [p for p in subprocess.check_output(['git', 'ls-files', '-z'], text=True).split('\0') if p]
removed = []

def remove(path):
    p = Path(path)
    if p.is_file() or p.is_symlink():
        p.unlink()
        removed.append(path)

def is_test(path):
    p = Path(path)
    return (any(part in {'tests', '__tests__', 'e2e', 'test'} for part in p.parts)
            or bool(re.search(r'(^test_.*\.py$|_test\.py$|\.(test|spec)\.[cm]?[jt]sx?$)', p.name))
            or p.name in {'conftest.py', 'pytest.ini', '.coveragerc', 'tox.ini', 'vitest.config.ts', 'playwright.config.ts'}
            or path.startswith('portal/scripts/fixtures/'))

for path in tracked:
    if is_test(path):
        remove(path)
for path in [
    'scripts/check-data.sh', 'scripts/check-workflows.py',
    'scripts/probe-cloudflare-r2-iceberg.py', 'scripts/probe-iceberg-query-contract.py',
    'portal/scripts/verify-bronze-names-live.mjs', 'portal/scripts/verify-dbw-live.mjs',
    'portal/scripts/verify-drive-lazy-http-live.mjs', 'portal/scripts/verify-duckdb-lazy-http.mjs',
    'portal/scripts/verify-native-landing-live.mjs',
]:
    remove(path)

removed_declarations = 0

def strip_tests(obj):
    global removed_declarations
    if isinstance(obj, dict):
        for key in list(obj):
            if key in {'tests', 'data_tests', 'unit_tests'}:
                value = obj.pop(key)
                removed_declarations += len(value) if isinstance(value, list) else 1
            else:
                strip_tests(obj[key])
    elif isinstance(obj, list):
        for value in obj:
            strip_tests(value)

for path in sorted(Path('models').rglob('*')):
    if path.suffix not in {'.yml', '.yaml'}:
        continue
    data = yaml.safe_load(path.read_text())
    before = removed_declarations
    strip_tests(data)
    if removed_declarations != before:
        path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=1000))
project = Path('dbt_project.yml')
data = yaml.safe_load(project.read_text())
data.pop('test-paths', None)
data.pop('tests', None)
data.pop('data_tests', None)
project.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True))
for path in Path('macros').rglob('*.sql'):
    original = path.read_text()
    changed = re.sub(r'{%-?\s*test\b.*?{%-?\s*endtest\s*-?%}', '', original, flags=re.S)
    if changed != original:
        if changed.strip():
            path.write_text(changed)
        else:
            remove(str(path))

# Retain browser automation used by ingestion, not browser testing.
pkg_path = Path('portal/package.json')
lock_path = Path('portal/package-lock.json')
pkg = json.loads(pkg_path.read_text())
old_lock = json.loads(lock_path.read_text())
old_versions = {name: item.get('version') for name, item in old_lock['packages'].items()}
for name in list(pkg.get('scripts', {})):
    if name in {'test', 'pretest', 'posttest'} or name.startswith('test:'):
        del pkg['scripts'][name]
for section in ('dependencies', 'devDependencies', 'optionalDependencies'):
    for name in list(pkg.get(section, {})):
        if (name in {'vitest', '@playwright/test', 'jest', 'jest-environment-jsdom', 'ts-jest', 'jsdom', 'happy-dom', 'nyc', 'c8'}
                or name.startswith(('@vitest/', '@testing-library/'))):
            del pkg[section][name]
needs_browser = False
for name in tracked:
    p = Path(name)
    if not p.is_file() or p.suffix not in {'.js', '.mjs', '.cjs', '.ts', '.tsx'}:
        continue
    text = p.read_text()
    changed = text.replace('"@playwright/test"', '"playwright"').replace("'@playwright/test'", "'playwright'")
    if changed != text:
        p.write_text(changed)
    if re.search(r'''(?:from\s*|import\s*\(|require\s*\()\s*['"]playwright['"]''', changed):
        needs_browser = True
if needs_browser:
    pkg.setdefault('devDependencies', {})['playwright'] = old_lock['packages']['node_modules/playwright']['version']
pkg_path.write_text(json.dumps(pkg, indent=2) + '\n')
subprocess.run(['npm', 'install', '--package-lock-only', '--ignore-scripts', '--no-audit', '--no-fund'], cwd='portal', check=True)
new_lock = json.loads(lock_path.read_text())
changed_versions = [(name, old_versions[name], item.get('version')) for name, item in new_lock['packages'].items()
                    if name and name in old_versions and old_versions[name] != item.get('version')]
if changed_versions:
    raise SystemExit('Unexpected existing dependency upgrades: ' + json.dumps(changed_versions))
for path in Path('portal').glob('tsconfig*.json'):
    text = path.read_text()
    text = re.sub(r'^.*(?:vitest\.config|playwright\.config|vitest/globals|src/test/setup).*(?:\n|$)', '', text, flags=re.M)
    path.write_text(text)
for path in [Path(p) for p in tracked if p.endswith('.sh') and Path(p).is_file()]:
    original = path.read_text()
    changed = '\n'.join(line for line in original.splitlines()
                        if not re.search(r'\b(pytest|vitest|unittest)\b|playwright test|scripts/check-data\.sh|scripts/check-workflows\.py', line)) + '\n'
    if changed != original:
        path.write_text(changed)
        subprocess.run(['bash', '-n', str(path)], check=True)
for name in ('requirements.in', 'requirements.txt'):
    path = Path(name)
    original = path.read_text()
    changed = '\n'.join(line for line in original.splitlines()
                        if not re.match(r'^(pytest(?:[-_][A-Za-z0-9_-]+)?|coverage|hypothesis)(?:[=<>\[ ]|$)', line, re.I)) + '\n'
    if changed != original:
        path.write_text(changed)

Path('AGENTS.md').write_text('''# Working on zohelo-data

The owner's current task takes precedence over historical plans.
Keep ingestion, native Landing/Archive, Bronze/Silver/Gold, modeling, semantic
models and the portal. R2/Iceberg remains the agreed target; cleanup is not cutover.

## Keep it simple

Use direct Python/SQL and the smallest command needed for the task. Inspect an
existing workflow before choosing it. Do not add frameworks, agent delegation,
automatic follow-up jobs, pilots, reports or workflows without an explicit need.

The owner explicitly requested removal of all repository tests. Do not recreate
test suites, fixtures, test runners or CI/check workflows unless the owner asks.
Do not replace deleted tests with renamed probes, acceptance suites or mandatory
preflights. There is no test prerequisite for development, ingestion or deployment.

Read only task-relevant files. Historical test results and removed workflow names
in older documents are history, not commands to restore or execute.

## Preserve the data and operation

Keep credentials private, bounded retries/timeouts, resumable checkpoints,
writer locks, original payload bytes and ordinary input/output error handling.
Removing tests does not mean silently publishing corrupt data or hiding errors.
Never delete retained production data as repository cleanup. Never claim complete
source coverage from a sample. Do not change storage readers/writers or release
pointers without an explicit cutover. No new paid services or model API billing.

## Deliver

Keep only four operational Actions: ingestion, transformations, portal deployment
and the temporary Drive-to-R2 copy. No CI/test Action. Copy, ingestion, modeling
and deployment are separate choices; do not silently chain optional diagnostics.
Do not launch production jobs to check repository edits. Report exactly what
changed, what ran and whether it reached main. Preserve other contributors' work.
''')
for name, title, target in [('CLAUDE.md', 'Claude', 'AGENTS.md'), ('GEMINI.md', 'Gemini', 'AGENTS.md'), ('.github/copilot-instructions.md', 'Copilot', '../AGENTS.md')]:
    Path(name).write_text(f'# {title}\n\nFollow [AGENTS.md]({target}) and the current owner request.\nDo not restore removed tests, CI workflows or historical task queues.\n')
Path('README.md').write_text('''# zohelo-data

Data platform for zohelo.com: native Landing/Archive, Bronze, Silver, Gold,
semantic models and the SQL portal at https://data.zohelo.com/.

## Operate

Only four Actions remain: **Ingestion**, **Transformations**, **Deploy portal**,
and **Copy Drive files to R2**. Choose the source or operation you need.
See [operating commands](docs/audits/2026-09-07-workflows.md).

There are no repository test suites or automatic CI/check workflows. Data jobs
run data commands, not regression suites, browser probes or acceptance campaigns.
Existing error handling, byte integrity, checkpoints and writer locks remain.

R2 is the target store, with native Landing and Iceberg medallion tables.
Cleanup does not migrate data or switch the existing Drive producers/readers.

[Development](docs/development.md) · [Architecture](docs/architecture.md) ·
[Status](docs/deliverables.md) · [Agent guide](AGENTS.md)
''')
Path('docs/development.md').write_text('''# Development

Use the versions in `.python-version` and `.node-version`.

```sh
python -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
npm --prefix portal ci --ignore-scripts --no-audit --no-fund
```

Run the Python/SQL command for the task. For the portal, use
`npm --prefix portal run dev` or `npm --prefix portal run build`.

Repository tests, fixtures, browser test runners and CI/check workflows were
removed at the owner's explicit request. Do not recreate them automatically or
follow historical instructions to run them. Browser automation remains only where
it performs real source ingestion. No test suite runs during install or deployment.

Keep normal error handling, data integrity and resumability. Production writes
require the existing authorization and writer locks; a cleanup is not permission
to change data, readers or destinations. Use the [workflow guide](audits/2026-09-07-workflows.md)
and [AGENTS.md](../AGENTS.md). Older test evidence is historical, not a current task.
''')
Path('docs/collaboration.md').write_text('''# Collaboration

Follow [AGENTS.md](../AGENTS.md) and the current owner task.
Use one focused change and direct code. No implicit agent delegation, new test
suite, recurring work, CI workflow or historical task restart.

Preserve other contributors' work, credentials, writer locks, checkpoints and
retained data. R2/Iceberg is the target; cleanup is not a storage cutover.
Do not add paid services or model API billing. Describe actual execution and
merge status without claiming a code change proves a successful data run.
''')
for name in ('portal/README.md', 'portal/CONTRIBUTING.md'):
    path = Path(name)
    if path.exists():
        path.write_text('''# Zohelo Data portal

Browser-based data catalogue and SQL workspace for data.zohelo.com.

```sh
npm ci --ignore-scripts --no-audit --no-fund
npm run dev
# Production build:
npm run build
```

Follow [the repository guide](../AGENTS.md). The owner removed all tests and test
workflows. Do not recreate them from historical documentation. Source-ingestion
browser scripts are operational code, not a browser test suite.
''')

subprocess.run(['git', 'diff', '--check'], check=True)
if subprocess.check_output(['git', 'diff', '--name-only', '--', '.github/workflows'], text=True).strip():
    raise SystemExit('This maintenance job must not modify workflow definitions')
remaining = [name for name in tracked if Path(name).is_file() and is_test(name)]
if remaining:
    raise SystemExit('Remaining test files: ' + repr(remaining))
report = {'removed_files': len(removed), 'removed_test_declarations': removed_declarations,
          'removed_paths': removed, 'browser_runtime_retained': needs_browser,
          'tests_executed': 0, 'production_operations_executed': 0,
          'unexpected_dependency_upgrades': changed_versions}
out = Path(os.environ['RUNNER_TEMP']) / 'repository-cleanup'
out.mkdir(exist_ok=True)
(out / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')
(out / 'changes.diff').write_text(subprocess.check_output(['git', 'diff', '--', '.', ':!.github/workflows'], text=True))
with tarfile.open(out / 'source.tar.gz', 'w:gz') as archive:
    for name in tracked:
        if Path(name).is_file():
            archive.add(name, arcname=name)
print(json.dumps({k: v for k, v in report.items() if k != 'removed_paths'}, indent=2))
print(subprocess.check_output(['git', 'diff', '--stat'], text=True))
