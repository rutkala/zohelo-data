// Test the real Vite configuration with synthetic credentials, never real secrets.
import assert from 'node:assert/strict';
import { loadConfigFromFile } from 'vite';

const sentinels = {
  GOOGLE_OAUTH_CLIENT_ID: 'synthetic-public-google-client.apps.googleusercontent.com',
  GOOGLE_OAUTH_CLIENT_SECRET: 'synthetic-private-google-client-secret',
  GOOGLE_OAUTH_REFRESH_TOKEN: 'synthetic-private-google-refresh-token',
};
Object.assign(process.env, sentinels);
delete process.env.DUCK_UI_GOOGLE_CLIENT_ID;

const loaded = await loadConfigFromFile({ command: 'serve', mode: 'development' }, 'vite.config.ts');
assert(loaded, 'Vite configuration must load');
assert.equal(
  loaded.config.define['import.meta.env.DUCK_UI_GOOGLE_CLIENT_ID'],
  JSON.stringify(sentinels.GOOGLE_OAUTH_CLIENT_ID),
  'Codespaces client ID must reach the public portal configuration',
);
const serialized = JSON.stringify(loaded.config.define);
assert(!serialized.includes(sentinels.GOOGLE_OAUTH_CLIENT_SECRET), 'Client secret must stay private');
assert(!serialized.includes(sentinels.GOOGLE_OAUTH_REFRESH_TOKEN), 'Refresh token must stay private');

process.env.DUCK_UI_GOOGLE_CLIENT_ID = 'synthetic-explicit-portal-client';
const explicit = await loadConfigFromFile({ command: 'serve', mode: 'development' }, 'vite.config.ts');
assert.equal(
  explicit.config.define['import.meta.env.DUCK_UI_GOOGLE_CLIENT_ID'],
  JSON.stringify('synthetic-explicit-portal-client'),
  'Explicit portal configuration must take precedence',
);
console.log('Google client-ID mapping and secret isolation checks passed.');
