import { Component, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { TiltDirective } from './effects';

declare global {
  interface Window {
    KAFI_CONFIG?: { leadsApiUrl?: string };
  }
}

type AddressSide = 'origin' | 'destination';

interface AddressSuggestion {
  id: string;
  street: string;
  postalCity: string;
  postcode: string;
  city: string;
}

interface InquiryDraft {
  version: 1;
  step: number;
  service: string;
  originZip: string;
  originCity: string;
  originAddress: string;
  destZip: string;
  destCity: string;
  destAddress: string;
  areaSqm: number | null;
  roomCount: number | null;
  originFloor: number | null;
  destinationFloor: number | null;
  date: string;
  alternateDates: string[];
  name: string;
  surname: string;
  phone: string;
  email: string;
}

@Component({
  selector: 'app-estimate',
  standalone: true,
  imports: [FormsModule, TiltDirective],
  template: `
    <div class="wizard-progress">
      <span>Schritt {{ step() < 6 ? step() + 1 : 6 }} von 6</span>
      <div class="bar"><i [style.width.%]="(Math.min(step(), 5) + 1) * 100 / 6"></i></div>
    </div>
    @switch (step()) {
      @case (0) {
        <h2>Welche Art von Umzug planen Sie?</h2>
        <p>Wählen Sie die passende Leistung für Ihre Anfrage.</p>
        <div class="opts">
          @for (s of services; track s.l) {
            <button type="button" tilt class="opt" [class.on]="service() === s.l" [attr.aria-pressed]="service() === s.l" (click)="selectService(s.l)"><span class="ico">{{ s.i }}</span>{{ s.l }}</button>
          }
        </div>
      }
      @case (1) {
        <h2>Wo startet Ihr Umzug?</h2>
        <div class="field-grid">
          <div class="field-group">
            <label for="origin-zip">Postleitzahl</label>
            <input class="fld" id="origin-zip" inputmode="numeric" autocomplete="postal-code" placeholder="z. B. 12353" maxlength="5" pattern="[0-9]{5}" required [ngModel]="originZip" (ngModelChange)="originZip = $event; saveDraft()" #originZipField="ngModel">
            @if (originZipField.invalid && (originZipField.touched || originZipField.dirty)) { <small class="field-error">Bitte geben Sie eine fünfstellige deutsche Postleitzahl ein.</small> }
          </div>
          <div class="field-group">
            <label for="origin-city">Ort</label>
            <input class="fld" id="origin-city" autocomplete="address-level2" placeholder="Stadt oder Gemeinde" minlength="2" maxlength="80" required [ngModel]="originCity" (ngModelChange)="originCity = $event; saveDraft()" #originCityField="ngModel">
            @if (originCityField.invalid && (originCityField.touched || originCityField.dirty)) { <small class="field-error">Bitte geben Sie einen Ort ein.</small> }
          </div>
          <div class="field-group autocomplete-field">
            <label for="origin-address">Straße und Hausnummer</label>
            <input class="fld" id="origin-address" autocomplete="street-address" placeholder="Straße und Hausnummer eingeben" minlength="3" maxlength="120" required [ngModel]="originAddress" (ngModelChange)="originAddress = $event; searchAddress('origin', $event); saveDraft()" #originAddressField="ngModel" role="combobox" aria-autocomplete="list" aria-controls="origin-address-suggestions" [attr.aria-expanded]="originSuggestions().length > 0">
            @if (originAddressField.invalid && (originAddressField.touched || originAddressField.dirty)) { <small class="field-error">Bitte geben Sie Straße und Hausnummer ein.</small> }
            @if (originSuggestions().length) {
              <div class="address-suggestions" id="origin-address-suggestions" role="listbox" aria-label="Adressvorschläge">
                @for (suggestion of originSuggestions(); track suggestion.id) {
                  <button type="button" class="address-suggestion" role="option" (click)="selectAddress('origin', suggestion)"><strong>{{ suggestion.street }}</strong><span>{{ suggestion.postalCity }}</span></button>
                }
              </div>
            }
            <small class="form-hint">Adressvorschläge über Photon / <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noreferrer">© OpenStreetMap</a>. Manuelle Eingabe ist ebenfalls möglich.</small>
          </div>
        </div>
      }
      @case (2) {
        <h2>Wohin geht Ihr Umzug?</h2>
        <div class="field-grid">
          <div class="field-group">
            <label for="destination-zip">Postleitzahl</label>
            <input class="fld" id="destination-zip" inputmode="numeric" autocomplete="postal-code" placeholder="z. B. 20095" maxlength="5" pattern="[0-9]{5}" required [ngModel]="destZip" (ngModelChange)="destZip = $event; saveDraft()" #destZipField="ngModel">
            @if (destZipField.invalid && (destZipField.touched || destZipField.dirty)) { <small class="field-error">Bitte geben Sie eine fünfstellige deutsche Postleitzahl ein.</small> }
          </div>
          <div class="field-group">
            <label for="destination-city">Ort</label>
            <input class="fld" id="destination-city" autocomplete="address-level2" placeholder="Stadt oder Gemeinde" minlength="2" maxlength="80" required [ngModel]="destCity" (ngModelChange)="destCity = $event; saveDraft()" #destCityField="ngModel">
            @if (destCityField.invalid && (destCityField.touched || destCityField.dirty)) { <small class="field-error">Bitte geben Sie einen Ort ein.</small> }
          </div>
          <div class="field-group autocomplete-field">
            <label for="destination-address">Straße und Hausnummer</label>
            <input class="fld" id="destination-address" autocomplete="street-address" placeholder="Straße und Hausnummer eingeben" minlength="3" maxlength="120" required [ngModel]="destAddress" (ngModelChange)="destAddress = $event; searchAddress('destination', $event); saveDraft()" #destAddressField="ngModel" role="combobox" aria-autocomplete="list" aria-controls="destination-address-suggestions" [attr.aria-expanded]="destinationSuggestions().length > 0">
            @if (destAddressField.invalid && (destAddressField.touched || destAddressField.dirty)) { <small class="field-error">Bitte geben Sie Straße und Hausnummer ein.</small> }
            @if (destinationSuggestions().length) {
              <div class="address-suggestions" id="destination-address-suggestions" role="listbox" aria-label="Adressvorschläge">
                @for (suggestion of destinationSuggestions(); track suggestion.id) {
                  <button type="button" class="address-suggestion" role="option" (click)="selectAddress('destination', suggestion)"><strong>{{ suggestion.street }}</strong><span>{{ suggestion.postalCity }}</span></button>
                }
              </div>
            }
            <small class="form-hint">Adressvorschläge über Photon / <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noreferrer">© OpenStreetMap</a>. Manuelle Eingabe ist ebenfalls möglich.</small>
          </div>
        </div>
      }
      @case (3) {
        <h2>Wie groß ist Ihr Umzug?</h2>
        <p>Ungefähre Angaben helfen uns bei der Planung. Alle Felder sind optional.</p>
        <div class="field-grid move-details-grid">
          <div class="field-group">
            <label for="area-sqm">Wohnfläche (m²)</label>
            <input class="fld" id="area-sqm" type="number" min="1" max="1000" step="1" inputmode="numeric" placeholder="z. B. 65" [(ngModel)]="areaSqm" (ngModelChange)="saveDraft()">
            @if (areaSqm !== null && !validOptionalInteger(areaSqm, 1, 1000)) { <small class="field-error">Bitte geben Sie einen Wert zwischen 1 und 1000 m² ein.</small> }
          </div>
          <div class="field-group">
            <label for="room-count">Anzahl Zimmer</label>
            <input class="fld" id="room-count" type="number" min="1" max="50" step="1" inputmode="numeric" placeholder="z. B. 3" [(ngModel)]="roomCount" (ngModelChange)="saveDraft()">
            @if (roomCount !== null && !validOptionalInteger(roomCount, 1, 50)) { <small class="field-error">Bitte geben Sie 1 bis 50 Zimmer an.</small> }
          </div>
          <div class="field-group">
            <label for="origin-floor">Etage am Startort</label>
            <input class="fld" id="origin-floor" type="number" min="-3" max="100" step="1" inputmode="numeric" placeholder="z. B. 3" [(ngModel)]="originFloor" (ngModelChange)="saveDraft()">
            @if (originFloor !== null && !validOptionalInteger(originFloor, -3, 100)) { <small class="field-error">Bitte geben Sie eine Etage zwischen Keller -3 und 100 ein.</small> }
          </div>
          <div class="field-group">
            <label for="destination-floor">Etage am Zielort</label>
            <input class="fld" id="destination-floor" type="number" min="-3" max="100" step="1" inputmode="numeric" placeholder="z. B. 2" [(ngModel)]="destinationFloor" (ngModelChange)="saveDraft()">
            @if (destinationFloor !== null && !validOptionalInteger(destinationFloor, -3, 100)) { <small class="field-error">Bitte geben Sie eine Etage zwischen Keller -3 und 100 ein.</small> }
          </div>
        </div>
        <small class="form-hint">0 = Erdgeschoss, negative Werte = Keller.</small>
      }
      @case (4) {
        <h2>Wann möchten Sie umziehen?</h2>
        <div class="field-group">
          <label for="move-date">Wunschtermin</label>
          <input class="fld" id="move-date" type="date" [min]="today" required [(ngModel)]="date" (ngModelChange)="saveDraft()">
          @if (date && date < today) { <small class="field-error">Der Wunschtermin darf nicht in der Vergangenheit liegen.</small> }
        </div>
        <div class="alternate-dates">
          <div class="alternate-heading"><h3>Alternative Termine</h3><span>Optional, maximal 3</span></div>
          @for (alternateDate of alternateDates(); track $index; let index = $index) {
            <div class="alternate-date-row">
              <div class="field-group">
                <label [for]="'alternate-date-' + index">Alternative {{ index + 1 }}</label>
                <input class="fld" [id]="'alternate-date-' + index" type="date" [min]="today" [ngModel]="alternateDate" (ngModelChange)="updateAlternateDate(index, $event)">
              </div>
              <button type="button" class="remove-date" [attr.aria-label]="'Alternativtermin ' + (index + 1) + ' entfernen'" (click)="removeAlternateDate(index)">Entfernen</button>
            </div>
            @if (alternateDate && (alternateDate < today || alternateDate === date)) { <small class="field-error">Alternativtermine müssen in der Zukunft liegen und sich vom Wunschtermin unterscheiden.</small> }
          }
          @if (alternateDates().length < 3) {
            <button type="button" class="add-date" (click)="addAlternateDate()">＋ Alternativtermin hinzufügen</button>
          }
        </div>
      }
      @case (5) {
        <h2>Ihre Kontaktdaten</h2>
        <div class="field-stack">
          <div class="field-group"><label for="first-name">Vorname</label><input class="fld" id="first-name" autocomplete="given-name" placeholder="Vorname" minlength="2" maxlength="80" required [ngModel]="name" (ngModelChange)="name = $event; saveDraft()">@if (name && !validName(name)) { <small class="field-error">Bitte prüfen Sie Ihren Vornamen.</small> }</div>
          <div class="field-group"><label for="last-name">Nachname</label><input class="fld" id="last-name" autocomplete="family-name" placeholder="Nachname" minlength="2" maxlength="80" required [ngModel]="surname" (ngModelChange)="surname = $event; saveDraft()">@if (surname && !validName(surname)) { <small class="field-error">Bitte prüfen Sie Ihren Nachnamen.</small> }</div>
          <div class="field-group">
            <label for="contact-phone">Telefon</label>
            <input class="fld" id="contact-phone" type="tel" autocomplete="tel" placeholder="015780945403" maxlength="40" required [ngModel]="phone" (ngModelChange)="phone = $event; saveDraft()">
            <small class="form-hint">Zum Beispiel 015780945403 oder +49 157 80945403</small>
            @if (phone && !validPhone()) { <small class="field-error">Bitte geben Sie eine gültige Telefonnummer mit 7 bis 15 Ziffern ein.</small> }
          </div>
          <div class="field-group">
            <label for="contact-email">E-Mail</label>
            <input class="fld" id="contact-email" type="email" autocomplete="email" placeholder="name@beispiel.de" maxlength="254" required [ngModel]="email" (ngModelChange)="email = $event; saveDraft()">
            @if (email && !validEmail()) { <small class="field-error">Bitte geben Sie eine gültige E-Mail-Adresse ein.</small> }
          </div>
        </div>
        <div class="form-trap" aria-hidden="true">
          <label>Dieses Feld bitte leer lassen<input tabindex="-1" autocomplete="off" [(ngModel)]="website"></label>
        </div>
        <p class="form-note">Ihr Entwurf bleibt vorübergehend in diesem Browser-Tab gespeichert und wird nach erfolgreichem Versand gelöscht. Wir verwenden Ihre Angaben zur Bearbeitung Ihrer Umzugsanfrage. Details finden Sie in unserer <a href="/datenschutz/" target="_blank" rel="noreferrer">Datenschutzerklärung</a>.</p>
      }
      @case (6) {
        <h2>Vielen Dank, {{ name }}!</h2>
        <p>Ihre Umzugsanfrage wurde erfolgreich an KAFI Transporte übermittelt. Wir melden uns persönlich bei Ihnen.</p>
        <p class="request-summary"><strong>{{ service() }}</strong><br>{{ originCity }} → {{ destCity }}<br>{{ areaSqm ? areaSqm + ' m²' : 'Wohnfläche offen' }} · {{ roomCount ? roomCount + ' Zimmer' : 'Zimmer offen' }}<br>Etage: {{ originFloor ?? 'offen' }} → {{ destinationFloor ?? 'offen' }}<br>Wunschtermin: {{ date }}<br>Alternativen: {{ alternateDates().join(', ') || 'Keine' }}</p>
        <div class="row">
          <a class="btn ghost dark" href="tel:+491787410656">☎ Anrufen</a>
          <a class="btn ghost dark" href="https://wa.me/491787410656" target="_blank" rel="noreferrer">WhatsApp</a>
        </div>
      }
    }

    @if (submitError()) {
      <p class="form-error" role="alert">{{ submitError() }}</p>
    }

    @if (step() < 6) {
      <div class="row">
        <button type="button" class="btn ghost dark" [disabled]="step() === 0 || sending()" (click)="goBack()">Zurück</button>
        <button class="btn solid" [disabled]="!ok() || sending()" (click)="advance()">
          {{ sending() ? 'Wird gesendet …' : step() === 5 ? 'Kostenloses Angebot anfragen' : 'Weiter' }}
        </button>
      </div>
    }
  `
})
export class EstimateComponent {
  private readonly draftKey = 'kafi-inquiry-draft-v1';
  private originSearchTimer?: ReturnType<typeof setTimeout>;
  private destinationSearchTimer?: ReturnType<typeof setTimeout>;
  private originSearchController?: AbortController;
  private destinationSearchController?: AbortController;

  services = [
    { i: '🏠', l: 'Privatumzug' },
    { i: '🏢', l: 'Firmenumzug' },
    { i: '🗺️', l: 'Fernumzug' },
    { i: '🛋️', l: 'Einzelner Möbeltransport' },
    { i: '📦', l: 'Sonstiges' }
  ];

  step = signal(0);
  service = signal('');
  sending = signal(false);
  submitError = signal('');
  originSuggestions = signal<AddressSuggestion[]>([]);
  destinationSuggestions = signal<AddressSuggestion[]>([]);
  alternateDates = signal<string[]>([]);
  originZip = '';
  originCity = '';
  originAddress = '';
  destZip = '';
  destCity = '';
  destAddress = '';
  areaSqm: number | null = null;
  roomCount: number | null = null;
  originFloor: number | null = null;
  destinationFloor: number | null = null;
  date = '';
  name = '';
  surname = '';
  phone = '';
  email = '';
  website = '';
  Math = Math;
  readonly today = this.getToday();

  constructor() {
    this.restoreDraft();
  }

  private getToday() {
    const date = new Date();
    date.setMinutes(date.getMinutes() - date.getTimezoneOffset());
    return date.toISOString().slice(0, 10);
  }

  private restoreDraft() {
    try {
      const stored = sessionStorage.getItem(this.draftKey);
      if (!stored) return;
      const draft = JSON.parse(stored) as Partial<InquiryDraft>;
      if (draft.version !== 1) return;

      this.step.set(Number.isInteger(draft.step) ? Math.max(0, Math.min(5, draft.step!)) : 0);
      this.service.set(typeof draft.service === 'string' ? draft.service : '');
      this.originZip = typeof draft.originZip === 'string' ? draft.originZip : '';
      this.originCity = typeof draft.originCity === 'string' ? draft.originCity : '';
      this.originAddress = typeof draft.originAddress === 'string' ? draft.originAddress : '';
      this.destZip = typeof draft.destZip === 'string' ? draft.destZip : '';
      this.destCity = typeof draft.destCity === 'string' ? draft.destCity : '';
      this.destAddress = typeof draft.destAddress === 'string' ? draft.destAddress : '';
      this.areaSqm = typeof draft.areaSqm === 'number' ? draft.areaSqm : null;
      this.roomCount = typeof draft.roomCount === 'number' ? draft.roomCount : null;
      this.originFloor = typeof draft.originFloor === 'number' ? draft.originFloor : null;
      this.destinationFloor = typeof draft.destinationFloor === 'number' ? draft.destinationFloor : null;
      this.date = typeof draft.date === 'string' ? draft.date : '';
      this.alternateDates.set(Array.isArray(draft.alternateDates)
        ? draft.alternateDates.filter((value): value is string => typeof value === 'string').slice(0, 3)
        : []);
      this.name = typeof draft.name === 'string' ? draft.name : '';
      this.surname = typeof draft.surname === 'string' ? draft.surname : '';
      this.phone = typeof draft.phone === 'string' ? draft.phone : '';
      this.email = typeof draft.email === 'string' ? draft.email : '';
    } catch {
      this.clearDraft();
    }
  }

  saveDraft() {
    const draft: InquiryDraft = {
      version: 1,
      step: this.step(),
      service: this.service(),
      originZip: this.originZip,
      originCity: this.originCity,
      originAddress: this.originAddress,
      destZip: this.destZip,
      destCity: this.destCity,
      destAddress: this.destAddress,
      areaSqm: this.areaSqm,
      roomCount: this.roomCount,
      originFloor: this.originFloor,
      destinationFloor: this.destinationFloor,
      date: this.date,
      alternateDates: this.alternateDates(),
      name: this.name,
      surname: this.surname,
      phone: this.phone,
      email: this.email
    };
    try {
      sessionStorage.setItem(this.draftKey, JSON.stringify(draft));
    } catch {
      // The wizard remains usable if browser storage is unavailable.
    }
  }

  private clearDraft() {
    try {
      sessionStorage.removeItem(this.draftKey);
    } catch {
      // Browser storage can be disabled by privacy settings.
    }
  }

  selectService(service: string) {
    this.service.set(service);
    this.saveDraft();
  }

  goBack() {
    if (this.step() === 0 || this.sending()) return;
    this.submitError.set('');
    this.step.update(step => step - 1);
    this.saveDraft();
  }

  addAlternateDate() {
    if (this.alternateDates().length >= 3) return;
    this.alternateDates.update(dates => [...dates, '']);
    this.saveDraft();
  }

  updateAlternateDate(index: number, value: string) {
    this.alternateDates.update(dates => dates.map((date, currentIndex) => currentIndex === index ? value : date));
    this.saveDraft();
  }

  removeAlternateDate(index: number) {
    this.alternateDates.update(dates => dates.filter((_date, currentIndex) => currentIndex !== index));
    this.saveDraft();
  }

  searchAddress(side: AddressSide, query: string) {
    const timer = side === 'origin' ? this.originSearchTimer : this.destinationSearchTimer;
    const controller = side === 'origin' ? this.originSearchController : this.destinationSearchController;
    if (timer) clearTimeout(timer);
    controller?.abort();

    const suggestions = side === 'origin' ? this.originSuggestions : this.destinationSuggestions;
    const trimmedQuery = query.trim();
    if (trimmedQuery.length < 3) {
      suggestions.set([]);
      return;
    }

    const timeout = setTimeout(async () => {
      const requestController = new AbortController();
      if (side === 'origin') this.originSearchController = requestController;
      else this.destinationSearchController = requestController;
      const params = new URLSearchParams({
        q: trimmedQuery,
        limit: '5',
        lang: 'de',
        bbox: '5.8,47.2,15.1,55.1'
      });

      try {
        const response = await fetch(`https://photon.komoot.io/api/?${params}`, { signal: requestController.signal });
        if (!response.ok) throw new Error('Address lookup failed');
        const result = await response.json() as { features?: Array<{ properties?: Record<string, unknown>; geometry?: { coordinates?: number[] } }> };
        const matches = (result.features || []).flatMap((feature, index): AddressSuggestion[] => {
          const properties = feature.properties || {};
          const countryCode = String(properties['countrycode'] || '').toLowerCase();
          if (countryCode && countryCode !== 'de') return [];
          const street = [String(properties['street'] || properties['name'] || ''), String(properties['housenumber'] || '')].filter(Boolean).join(' ');
          const postcode = String(properties['postcode'] || '');
          const city = String(properties['city'] || properties['locality'] || properties['district'] || '');
          if (!street || !postcode || !city) return [];
          return [{
            id: `${feature.geometry?.coordinates?.join(',') || street}-${index}`,
            street,
            postcode,
            city,
            postalCity: [postcode, city].filter(Boolean).join(' ')
          }];
        });
        if (!requestController.signal.aborted) suggestions.set(matches);
      } catch {
        if (!requestController.signal.aborted) suggestions.set([]);
      }
    }, 350);

    if (side === 'origin') this.originSearchTimer = timeout;
    else this.destinationSearchTimer = timeout;
  }

  selectAddress(side: AddressSide, suggestion: AddressSuggestion) {
    if (side === 'origin') {
      this.originAddress = suggestion.street;
      this.originZip = suggestion.postcode;
      this.originCity = suggestion.city;
      this.originSuggestions.set([]);
    } else {
      this.destAddress = suggestion.street;
      this.destZip = suggestion.postcode;
      this.destCity = suggestion.city;
      this.destinationSuggestions.set([]);
    }
    this.saveDraft();
  }

  validZip(zip: string) {
    return /^\d{5}$/.test(zip.trim());
  }

  validMoveDate(date: string) {
    return /^\d{4}-\d{2}-\d{2}$/.test(date) && date >= this.today;
  }

  validDates() {
    const alternatives = this.alternateDates();
    return this.validMoveDate(this.date) &&
      alternatives.length <= 3 &&
      alternatives.every(date => this.validMoveDate(date) && date !== this.date) &&
      new Set(alternatives).size === alternatives.length;
  }

  validName(name: string) {
    return /^[\p{L}\p{M}][\p{L}\p{M}'’ .-]{1,79}$/u.test(name.trim());
  }

  validOptionalInteger(value: number | null, min: number, max: number) {
    return value === null || (Number.isInteger(value) && value >= min && value <= max);
  }

  validEmail() {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(this.email.trim());
  }

  advance() {
    if (this.sending() || !this.ok()) return;
    if (this.step() === 5) {
      void this.submit();
      return;
    }
    this.submitError.set('');
    this.step.update(step => step + 1);
    this.saveDraft();
  }

  async submit() {
    const apiUrl = window.KAFI_CONFIG?.leadsApiUrl?.trim();
    if (!apiUrl) {
      this.submitError.set('Das Anfrageformular ist noch nicht mit dem E-Mail-Dienst verbunden. Bitte kontaktieren Sie uns telefonisch oder per WhatsApp.');
      return;
    }

    this.sending.set(true);
    this.submitError.set('');
    try {
      const response = await fetch(apiUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        cache: 'no-store',
        body: JSON.stringify({
          service: this.service(),
          origin: { zip: this.originZip, city: this.originCity, address: this.originAddress },
          destination: { zip: this.destZip, city: this.destCity, address: this.destAddress },
          moveDetails: {
            areaSqm: this.areaSqm,
            roomCount: this.roomCount,
            originFloor: this.originFloor,
            destinationFloor: this.destinationFloor
          },
          date: this.date,
          alternateDate: this.alternateDates().filter(Boolean).join(', '),
          alternateDates: this.alternateDates().filter(Boolean),
          contact: { firstName: this.name, lastName: this.surname, phone: this.phone, email: this.email },
          website: this.website
        })
      });

      if (!response.ok) throw new Error('Request was rejected');
      this.clearDraft();
      this.step.set(5);
    } catch {
      this.submitError.set('Ihre Anfrage konnte gerade nicht gesendet werden. Bitte versuchen Sie es erneut oder kontaktieren Sie uns telefonisch unter +49 178 7410656.');
    } finally {
      this.sending.set(false);
    }
  }

  ok() {
    switch (this.step()) {
      case 0: return !!this.service();
      case 1: return this.validAddress(this.originZip, this.originCity, this.originAddress);
      case 2: return this.validAddress(this.destZip, this.destCity, this.destAddress);
      case 3: return this.validMoveDetails();
      case 4: return this.validDates();
      case 5: return this.validName(this.name) && this.validName(this.surname) && this.validPhone() && this.validEmail();
      default: return true;
    }
  }

  private validMoveDetails() {
    return this.validOptionalInteger(this.areaSqm, 1, 1000) &&
      this.validOptionalInteger(this.roomCount, 1, 50) &&
      this.validOptionalInteger(this.originFloor, -3, 100) &&
      this.validOptionalInteger(this.destinationFloor, -3, 100);
  }

  private validAddress(zip: string, city: string, address: string) {
    return this.validZip(zip) && city.trim().length >= 2 && address.trim().length >= 3;
  }

  validPhone() {
    const phone = this.phone.trim();
    const digits = phone.replace(/\D/g, '').length;
    return /^\+?[\d\s()./-]+$/.test(phone) && digits >= 7 && digits <= 15;
  }
}
