import { Component, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { TiltDirective } from './effects';

declare global {
  interface Window {
    KAFI_CONFIG?: { leadsApiUrl?: string };
  }
}

@Component({
  selector: 'app-estimate',
  standalone: true,
  imports: [FormsModule, TiltDirective],
  template: `
    <div class="wizard-progress">
      <span>Schritt {{ step() < 5 ? step() + 1 : 5 }} von 5</span>
      <div class="bar"><i [style.width.%]="(Math.min(step(), 4) + 1) * 20"></i></div>
    </div>
    @switch (step()) {
      @case (0) {
        <h2>Welche Art von Umzug planen Sie?</h2>
        <p>Teilen Sie uns die wichtigsten Details mit, damit wir Ihre Anfrage prüfen können.</p>
        <div class="opts">
          @for (s of services; track s.l) {
            <button tilt class="opt" [class.on]="service() === s.l" (click)="service.set(s.l)"><span class="ico">{{ s.i }}</span>{{ s.l }}</button>
          }
        </div>
      }
      @case (1) {
        <h2>Wo startet Ihr Umzug?</h2>
        <div class="field-stack">
          <input class="fld" placeholder="PLZ" [(ngModel)]="originZip">
          <input class="fld" placeholder="Ort" [(ngModel)]="originCity">
          <input class="fld" placeholder="Straße / Adresse" [(ngModel)]="originAddress">
        </div>
      }
      @case (2) {
        <h2>Wohin geht Ihr Umzug?</h2>
        <div class="field-stack">
          <input class="fld" placeholder="PLZ" [(ngModel)]="destZip">
          <input class="fld" placeholder="Ort" [(ngModel)]="destCity">
          <input class="fld" placeholder="Straße / Adresse" [(ngModel)]="destAddress">
        </div>
      }
      @case (3) {
        <h2>Wann möchten Sie umziehen?</h2>
        <div class="field-stack">
          <input class="fld" type="date" [(ngModel)]="date">
          <input class="fld" placeholder="Alternativer Termin" [(ngModel)]="altDate">
        </div>
      }
      @case (4) {
        <h2>Ihre Kontaktdaten</h2>
        <div class="field-stack">
          <input class="fld" autocomplete="given-name" placeholder="Vorname" aria-label="Vorname" [(ngModel)]="name">
          <input class="fld" autocomplete="family-name" placeholder="Nachname" aria-label="Nachname" [(ngModel)]="surname">
          <input class="fld" type="tel" autocomplete="tel" placeholder="Telefon" aria-label="Telefon" [(ngModel)]="phone">
          <small class="form-hint">Zum Beispiel 015780945403 oder +49 157 80945403</small>
          <input class="fld" type="email" autocomplete="email" placeholder="E-Mail" aria-label="E-Mail" [(ngModel)]="email">
        </div>
        <div class="form-trap" aria-hidden="true">
          <label>Dieses Feld bitte leer lassen<input tabindex="-1" autocomplete="off" [(ngModel)]="website"></label>
        </div>
        <p class="form-note">Wir verwenden Ihre Angaben ausschließlich zur Bearbeitung Ihrer Umzugsanfrage. Details finden Sie in unserer <a href="/datenschutz/" target="_blank" rel="noreferrer">Datenschutzerklärung</a>.</p>
      }
      @case (5) {
        <h2>Vielen Dank, {{ name }}!</h2>
        <p>Ihre Umzugsanfrage wurde erfolgreich an KAFI Transporte übermittelt. Wir melden uns persönlich bei Ihnen.</p>
        <p class="request-summary"><strong>{{ service() }}</strong><br>{{ originCity }} → {{ destCity }}<br>Wunschtermin: {{ date }}</p>
        <div class="row">
          <a class="btn ghost dark" href="tel:+491787410656">☎ Anrufen</a>
          <a class="btn ghost dark" href="https://wa.me/491787410656" target="_blank" rel="noreferrer">WhatsApp</a>
        </div>
      }
    }

    @if (submitError()) {
      <p class="form-error" role="alert">{{ submitError() }}</p>
    }

    @if (step() < 5) {
      <div class="row">
        <button class="btn ghost dark" [disabled]="step() === 0 || sending()" (click)="step.set(step() - 1)">Zurück</button>
        <button class="btn solid" [disabled]="!ok() || sending()" (click)="advance()">
          {{ sending() ? 'Wird gesendet …' : step() === 4 ? 'Kostenloses Angebot anfragen' : 'Weiter' }}
        </button>
      </div>
    }
  `
})
export class EstimateComponent {
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
  originZip = '';
  originCity = '';
  originAddress = '';
  destZip = '';
  destCity = '';
  destAddress = '';
  date = '';
  altDate = '';
  name = '';
  surname = '';
  phone = '';
  email = '';
  website = '';

  Math = Math;

  advance() {
    if (this.sending()) return;
    if (this.step() === 4) {
      void this.submit();
      return;
    }
    this.submitError.set('');
    this.step.update(step => step + 1);
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
          date: this.date,
          alternateDate: this.altDate,
          contact: { firstName: this.name, lastName: this.surname, phone: this.phone, email: this.email },
          website: this.website
        })
      });

      if (!response.ok) throw new Error('Request was rejected');
      this.step.set(5);
    } catch {
      this.submitError.set('Ihre Anfrage konnte gerade nicht gesendet werden. Bitte versuchen Sie es erneut oder kontaktieren Sie uns telefonisch unter +49 178 7410656.');
    } finally {
      this.sending.set(false);
    }
  }

  ok() {
    const s = this.step();
    if (s === 0) return !!this.service();
    if (s === 1) return !!(this.originZip && this.originCity);
    if (s === 2) return !!(this.destZip && this.destCity);
    if (s === 3) return !!this.date;
    if (s === 4) return !!(this.name.trim() && this.surname.trim() && this.validPhone() && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(this.email.trim()));
    return true;
  }

  validPhone() {
    const phone = this.phone.trim();
    const digits = phone.replace(/\D/g, '').length;
    return /^\+?[\d\s()./-]+$/.test(phone) && digits >= 7 && digits <= 15;
  }
}
