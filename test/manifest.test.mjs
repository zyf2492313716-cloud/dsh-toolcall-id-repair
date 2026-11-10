import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const pkg = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'));

const LATEST_THREE = ['0.2.2-alpha.2', '0.2.2-alpha.3', '0.2.2-rc.1'];

test('manifest declares the DSH bundle and cordis patch', () => {
	assert.equal(pkg.name, '@dsh-local/dsh-toolcall-id-repair');
	assert.equal(pkg.dsh.bundle.patch, 'toolcall-id-repair.cordis.yml');
	assert.ok(existsSync(join(root, pkg.dsh.bundle.patch)));
	assert.ok(existsSync(join(root, 'lib/index.js')));
});

test('manifest files array only contains runtime distribution files', () => {
	assert.ok(pkg.files.includes('lib'));
	assert.ok(pkg.files.includes('toolcall-id-repair.cordis.yml'));
	assert.equal(pkg.files.length, 2);
});

test('manifest declares the tested DSH releases including the current window', () => {
	assert.equal(pkg.dsh.compatibility.dshReleases['0.2.1-rc.5'], 'compatible');
	for (const rel of LATEST_THREE) {
		assert.equal(pkg.dsh.compatibility.dshReleases[rel], 'compatible');
	}
});

test('every declared release satisfies engines.dsh', () => {
	const range = pkg.engines.dsh;
	assert.ok(range.startsWith('>='));
	const lower = range.slice(2);
	for (const [release, status] of Object.entries(pkg.dsh.compatibility.dshReleases)) {
		if (status !== 'compatible') continue;
		assert.ok(release >= lower, `${release} must satisfy ${range}`);
	}
});

test('runtime lib/index.js has zero file, network, command, or credential signals', async () => {
	const code = await readFile(join(root, 'lib/index.js'), 'utf8');
	assert.ok(!code.includes('node:fs'), 'must not import node:fs');
	assert.ok(!code.includes('appendFileSync'), 'must not write files synchronously');
	assert.ok(!code.includes('fetch('), 'must not make network requests');
	assert.ok(!code.includes('child_process'), 'must not spawn commands');
	assert.ok(!code.includes('process.env'), 'must not access environment variables directly');
});
