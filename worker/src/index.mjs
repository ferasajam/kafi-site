const MAX_BODY_BYTES = 12000;
const MAX_TEXT_LENGTH = 200;
const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const PHONE_PATTERN = /^\+?[\d\s()./-]+$/;
const POSTCODE_PATTERN = /^\d{5}$/;
const ALLOWED_SERVICES = new Set([
  'Privatumzug',
  'Firmenumzug',
  'Fernumzug',
  'Einzelner Möbeltransport',
  'Sonstiges'
]);

function response(body, status, origin) {
  const headers = new Headers({
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store'
  });
  if (origin) {
    headers.set('Access-Control-Allow-Origin', origin);
    headers.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
    headers.set('Access-Control-Allow-Headers', 'Content-Type');
    headers.set('Vary', 'Origin');
  }
  return new Response(JSON.stringify(body), { status, headers });
}

function clean(value, maxLength = MAX_TEXT_LENGTH) {
  if (typeof value !== 'string') return '';
  return value.trim().replace(/[\u0000-\u001f\u007f]/g, '').slice(0, maxLength);
}

function escapeHtml(value) {
  return value.replace(/[&<>"']/g, character => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  })[character]);
}

function formatAddress(address) {
  return [address.zip, address.city, address.address].filter(Boolean).join(' ');
}

function isFutureDate(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const date = new Date(`${value}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime()) || date.toISOString().slice(0, 10) !== value) return false;
  return value >= new Date().toISOString().slice(0, 10);
}

function optionalInteger(value, min, max) {
  if (value === null || value === undefined) return null;
  return Number.isInteger(value) && value >= min && value <= max ? value : Number.NaN;
}

function formatFloor(value) {
  if (value === null) return 'Nicht angegeben';
  if (value === 0) return 'Erdgeschoss';
  if (value < 0) return `Keller ${Math.abs(value)}`;
  return `${value}. Obergeschoss`;
}

export default {
  async fetch(request, env) {
    const origin = request.headers.get('Origin') || '';
    const allowedOrigins = (env.ALLOWED_ORIGINS || '').split(',').map(value => value.trim());
    if (!allowedOrigins.includes(origin)) return response({ error: 'Origin not allowed' }, 403);

    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: {
          'Access-Control-Allow-Origin': origin,
          'Access-Control-Allow-Methods': 'POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type',
          'Access-Control-Max-Age': '86400',
          'Vary': 'Origin'
        }
      });
    }

    if (request.method !== 'POST' || new URL(request.url).pathname !== '/api/inquiries') {
      return response({ error: 'Not found' }, 404, origin);
    }
    if (!request.headers.get('Content-Type')?.toLowerCase().startsWith('application/json')) {
      return response({ error: 'JSON required' }, 415, origin);
    }
    if (Number(request.headers.get('Content-Length') || 0) > MAX_BODY_BYTES) {
      return response({ error: 'Request too large' }, 413, origin);
    }

    let payload;
    try {
      const body = await request.text();
      if (new TextEncoder().encode(body).length > MAX_BODY_BYTES) {
        return response({ error: 'Request too large' }, 413, origin);
      }
      payload = JSON.parse(body);
    } catch {
      return response({ error: 'Invalid request' }, 400, origin);
    }
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
      return response({ error: 'Invalid request' }, 400, origin);
    }
    if (clean(payload.website, 500)) return response({ accepted: true }, 200, origin);

    const submittedAlternatives = Array.isArray(payload.alternateDates)
      ? payload.alternateDates.slice(0, 4).map(value => clean(value, 20))
      : clean(payload.alternateDate, 80).split(',').map(value => value.trim()).filter(Boolean);
    const inquiry = {
      service: clean(payload.service, 80),
      origin: {
        zip: clean(payload.origin?.zip, 12),
        city: clean(payload.origin?.city, 80),
        address: clean(payload.origin?.address, 120)
      },
      destination: {
        zip: clean(payload.destination?.zip, 12),
        city: clean(payload.destination?.city, 80),
        address: clean(payload.destination?.address, 120)
      },
      moveDetails: {
        areaSqm: optionalInteger(payload.moveDetails?.areaSqm, 1, 1000),
        roomCount: optionalInteger(payload.moveDetails?.roomCount, 1, 50),
        originFloor: optionalInteger(payload.moveDetails?.originFloor, -3, 100),
        destinationFloor: optionalInteger(payload.moveDetails?.destinationFloor, -3, 100)
      },
      date: clean(payload.date, 20),
      alternateDates: submittedAlternatives,
      contact: {
        firstName: clean(payload.contact?.firstName, 80),
        lastName: clean(payload.contact?.lastName, 80),
        phone: clean(payload.contact?.phone, 40),
        email: clean(payload.contact?.email, 254)
      }
    };
    const required = [
      inquiry.service, inquiry.origin.zip, inquiry.origin.city, inquiry.origin.address,
      inquiry.destination.zip, inquiry.destination.city, inquiry.destination.address, inquiry.date,
      inquiry.contact.firstName, inquiry.contact.lastName,
      inquiry.contact.phone, inquiry.contact.email
    ];
    const phoneDigits = inquiry.contact.phone.replace(/\D/g, '').length;
    const uniqueAlternatives = new Set(inquiry.alternateDates);
    if (
      required.some(value => !value) ||
      !POSTCODE_PATTERN.test(inquiry.origin.zip) ||
      !POSTCODE_PATTERN.test(inquiry.destination.zip) ||
      !isFutureDate(inquiry.date) ||
      Object.values(inquiry.moveDetails).some(Number.isNaN) ||
      inquiry.alternateDates.length > 3 ||
      uniqueAlternatives.size !== inquiry.alternateDates.length ||
      inquiry.alternateDates.some(value => !isFutureDate(value) || value === inquiry.date) ||
      !ALLOWED_SERVICES.has(inquiry.service) ||
      !EMAIL_PATTERN.test(inquiry.contact.email) ||
      !PHONE_PATTERN.test(inquiry.contact.phone) ||
      phoneDigits < 7 ||
      phoneDigits > 15
    ) {
      return response({ error: 'Please check the required fields' }, 400, origin);
    }
    if (!env.RESEND_API_KEY) return response({ error: 'Email service is not configured' }, 503, origin);

    const fullName = `${inquiry.contact.firstName} ${inquiry.contact.lastName}`;
    const rows = [
      ['Leistung', inquiry.service],
      ['Von', formatAddress(inquiry.origin)],
      ['Nach', formatAddress(inquiry.destination)],
      ['Wohnfläche', inquiry.moveDetails.areaSqm === null ? 'Nicht angegeben' : `${inquiry.moveDetails.areaSqm} m²`],
      ['Zimmer', inquiry.moveDetails.roomCount === null ? 'Nicht angegeben' : String(inquiry.moveDetails.roomCount)],
      ['Etage Start', formatFloor(inquiry.moveDetails.originFloor)],
      ['Etage Ziel', formatFloor(inquiry.moveDetails.destinationFloor)],
      ['Wunschtermin', inquiry.date],
      ['Alternative Termine', inquiry.alternateDates.join(', ') || 'Keine angegeben'],
      ['Name', fullName],
      ['Telefon', inquiry.contact.phone],
      ['E-Mail', inquiry.contact.email]
    ];
    const text = rows.map(([label, value]) => `${label}: ${value}`).join('\n');
    const htmlRows = rows.map(([label, value]) =>
      `<tr><th align="left" style="padding:8px;border-bottom:1px solid #eee">${escapeHtml(label)}</th><td style="padding:8px;border-bottom:1px solid #eee">${escapeHtml(value)}</td></tr>`
    ).join('');

    let resendResponse;
    try {
      resendResponse = await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${env.RESEND_API_KEY}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          from: 'KAFI Transporte <anfragen@kafitransporte.de>',
          to: ['firasajam10@gmail.com'],
          reply_to: inquiry.contact.email,
          subject: `Neue Umzugsanfrage von ${fullName}`,
          text,
          html: `<h1>Neue Umzugsanfrage</h1><table style="border-collapse:collapse;width:100%">${htmlRows}</table>`
        })
      });
    } catch {
      return response({ error: 'Email service unavailable' }, 502, origin);
    }
    if (!resendResponse.ok) return response({ error: 'Email could not be sent' }, 502, origin);
    return response({ accepted: true }, 200, origin);
  }
};