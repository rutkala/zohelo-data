// Explicit per-worker Playwright routing; never substitute a direct connection.
export function bdlBrowserOptions(env = process.env) {
  const server = env.BDL_WEB_PROXY;
  if (server === undefined) return { headless: true };
  const match = /^http:\/\/127\.0\.0\.1:([0-9]+)$/.exec(server);
  if (!match || Number(match[1]) < 1 || Number(match[1]) > 65535) {
    throw new Error('Invalid BDL worker proxy; expected an explicit localhost HTTP route');
  }
  return { headless: true, proxy: { server: `http://127.0.0.1:${Number(match[1])}` } };
}
