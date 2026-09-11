// Explicit local-only harness: the actual QRT Worker operates on an in-memory KV.
import { createInterface } from 'node:readline';
import { pathToFileURL } from 'node:url';
const RealDate = Date;
globalThis.Date = class extends RealDate {
  constructor(...args) { super(...(args.length ? args : ['2027-09-02T00:00:00Z'])); }
  static now() { return new RealDate('2027-09-02T00:00:00Z').getTime(); }
};
globalThis.fetch = async () => { throw new Error('Network forbidden in synthetic V7 integration'); };
const { default: worker, __test } = await import(pathToFileURL(process.env.QRT_WORKER_PATH));
const values = new Map();
const env = {
  SESSION_SECRET: 'synthetic-v7-local-session',
  RESEARCH_PROMOTION_SYNC_TOKEN: 'synthetic-v7-local-sync',
  ALLOWED_GITHUB_LOGINS: 'synthetic-admin', STRATEGY_SWITCH_ADMIN_LOGINS: 'synthetic-admin',
  STRATEGY_SWITCH_CONFIG: {
    async get(key) { return values.get(key) ?? null; },
    async put(key, value) { values.set(key, value); },
    async list({ prefix }) { return { keys: [...values.keys()].filter(k => k.startsWith(prefix)).map(name => ({ name })) }; },
  },
};
const cookie = await __test.makeSession('synthetic-admin', [], env);
for await (const line of createInterface({ input: process.stdin })) {
  const command = JSON.parse(line);
  const headers = { 'Content-Type': 'application/json' };
  if (command.admin) Object.assign(headers, { Cookie: `qsl_switch_session=${cookie}`, Origin: 'https://switch.example' });
  else headers.Authorization = `Bearer ${env.RESEARCH_PROMOTION_SYNC_TOKEN}`;
  const response = await worker.fetch(new Request(`https://switch.example${command.path}`, {
    method: command.method || 'GET', headers,
    ...(command.payload ? { body: JSON.stringify(command.payload) } : {}),
  }), env);
  process.stdout.write(JSON.stringify({ status: response.status, body: await response.json() }) + '\n');
}
