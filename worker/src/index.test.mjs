import assert from 'node:assert/strict';
import test from 'node:test';
import worker from './index.mjs';

const origin = 'https://kafitransporte.de';
const env = { ALLOWED_ORIGINS: `${origin},http://localhost:4200`, RESEND_API_KEY: 'test-secret' };

function makeRequest(payload, requestOrigin = origin) {
  return new Request('https://api.example.workers.dev/api/inquiries', {
    method: 'POST',
    headers: { Origin: requestOrigin, 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
}

function futureDate(days = 30) {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function validPayload(overrides = {}) {
  return {
    service: 'Privatumzug',
    origin: { zip: '12353', city: 'Berlin', address: 'Ringslebenstraße 78' },
    destination: { zip: '20095', city: 'Hamburg', address: 'Mönckebergstraße 7' },
    date: futureDate(),
    alternateDate: '',
    contact: { firstName: 'Firas', lastName: 'Test', phone: '+49123456789', email: 'kunde@example.com' },
    website: '',
    ...overrides
  };
}

test('answers the browser preflight with the allowed KAFI origin', async () => {
  const request = new Request('https://api.example.workers.dev/api/inquiries', {
    method: 'OPTIONS',
    headers: {
      Origin: origin,
      'Access-Control-Request-Method': 'POST',
      'Access-Control-Request-Headers': 'content-type'
    }
  });
  const result = await worker.fetch(request, env);

  assert.equal(result.status, 204);
  assert.equal(result.headers.get('Access-Control-Allow-Origin'), origin);
  assert.equal(result.headers.get('Access-Control-Allow-Methods'), 'POST, OPTIONS');
  assert.equal(result.headers.get('Access-Control-Allow-Headers'), 'Content-Type');
});

test('sends a validated inquiry to the configured recipient', async t => {
  let sent;
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    sent = { url, options, body: JSON.parse(options.body) };
    return Response.json({ id: 'email-id' });
  });
  const result = await worker.fetch(makeRequest(validPayload()), env);
  assert.equal(result.status, 200);
  assert.deepEqual(await result.json(), { accepted: true });
  assert.equal(sent.url, 'https://api.resend.com/emails');
  assert.equal(sent.options.headers.Authorization, 'Bearer test-secret');
  assert.equal(sent.body.to[0], 'firasajam10@gmail.com');
  assert.equal(sent.body.reply_to, 'kunde@example.com');
  assert.match(sent.body.text, /12353 Berlin/);
  assert.equal(result.headers.get('Access-Control-Allow-Origin'), origin);
});

test('accepts the German national phone number 15700000000', async t => {
  let sentBody;
  t.mock.method(globalThis, 'fetch', async (_url, options) => {
    sentBody = JSON.parse(options.body);
    return Response.json({ id: 'email-id' });
  });
  const result = await worker.fetch(makeRequest(validPayload({
    contact: { firstName: 'Firas', lastName: 'Test', phone: '15700000000', email: 'kunde@example.com' }
  })), env);
  assert.equal(result.status, 200);
  assert.match(sentBody.text, /Telefon: 15700000000/);
});

test('includes up to three selected alternative dates in the email', async t => {
  let sentBody;
  t.mock.method(globalThis, 'fetch', async (_url, options) => {
    sentBody = JSON.parse(options.body);
    return Response.json({ id: 'email-id' });
  });
  const alternatives = [futureDate(40), futureDate(50)];
  const result = await worker.fetch(makeRequest(validPayload({ alternateDates: alternatives })), env);
  assert.equal(result.status, 200);
  assert.match(sentBody.text, new RegExp(`Alternative Termine: ${alternatives.join(', ')}`));
});

test('rejects past move dates and malformed German postcodes', async t => {
  t.mock.method(globalThis, 'fetch', async () => { throw new Error('Resend must not be called'); });
  const yesterday = new Date();
  yesterday.setUTCDate(yesterday.getUTCDate() - 1);
  const pastDate = yesterday.toISOString().slice(0, 10);
  const pastResult = await worker.fetch(makeRequest(validPayload({ date: pastDate })), env);
  assert.equal(pastResult.status, 400);

  const postcodeResult = await worker.fetch(makeRequest(validPayload({
    origin: { zip: '1234', city: 'Berlin', address: 'Ringslebenstraße 78' }
  })), env);
  assert.equal(postcodeResult.status, 400);
});

test('rejects unapproved origins without calling Resend', async t => {
  t.mock.method(globalThis, 'fetch', async () => { throw new Error('Unexpected mail send'); });
  const result = await worker.fetch(makeRequest(validPayload(), 'https://attacker.example'), env);
  assert.equal(result.status, 403);
});

test('rejects invalid email addresses without calling Resend', async t => {
  t.mock.method(globalThis, 'fetch', async () => { throw new Error('Unexpected mail send'); });
  const result = await worker.fetch(makeRequest(validPayload({
    contact: { firstName: 'Firas', lastName: 'Test', phone: '+49123456789', email: 'invalid' }
  })), env);
  assert.equal(result.status, 400);
});

test('rejects services that are not offered by the form', async t => {
  t.mock.method(globalThis, 'fetch', async () => { throw new Error('Unexpected mail send'); });
  const result = await worker.fetch(makeRequest(validPayload({ service: 'Admin access' })), env);
  assert.equal(result.status, 400);
});

test('does not report success when the Resend key is missing', async t => {
  t.mock.method(globalThis, 'fetch', async () => { throw new Error('Unexpected mail send'); });
  const result = await worker.fetch(makeRequest(validPayload()), { ...env, RESEND_API_KEY: '' });
  assert.equal(result.status, 503);
});

test('honeypot submissions are accepted without sending mail', async t => {
  t.mock.method(globalThis, 'fetch', async () => { throw new Error('Unexpected mail send'); });
  const result = await worker.fetch(makeRequest(validPayload({ website: 'bot filled this' })), env);
  assert.equal(result.status, 200);
  assert.deepEqual(await result.json(), { accepted: true });
});

test('escapes user-provided values in the HTML email', async t => {
  let sentBody;
  t.mock.method(globalThis, 'fetch', async (_url, options) => {
    sentBody = JSON.parse(options.body);
    return Response.json({ id: 'email-id' });
  });
  const result = await worker.fetch(makeRequest(validPayload({
    contact: { firstName: '<img src=x>', lastName: 'Test', phone: '+49123456789', email: 'kunde@example.com' }
  })), env);
  assert.equal(result.status, 200);
  assert.match(sentBody.html, /&lt;img src=x&gt;/);
  assert.doesNotMatch(sentBody.html, /<img src=x>/);
});

test('does not report success when Resend rejects the message', async t => {
  t.mock.method(globalThis, 'fetch', async () => Response.json({ message: 'rejected' }, { status: 401 }));
  const result = await worker.fetch(makeRequest(validPayload()), env);
  assert.equal(result.status, 502);
  assert.deepEqual(await result.json(), { error: 'Email could not be sent' });
});