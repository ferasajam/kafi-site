import { Component, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { TiltDirective } from './effects';

@Component({
  selector: 'app-estimate',
  standalone: true,
  imports: [FormsModule, TiltDirective],
  template: `
    <div class="bar"><i [style.width.%]="((step() + 1) / 5) * 100"></i></div>
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
          <input class="fld" placeholder="Vorname" [(ngModel)]="name">
          <input class="fld" placeholder="Nachname" [(ngModel)]="surname">
          <input class="fld" type="tel" placeholder="Telefon" [(ngModel)]="phone">
          <input class="fld" type="email" placeholder="E-Mail" [(ngModel)]="email">
        </div>
      }
      @case (5) {
        <h2>Vielen Dank für Ihre Anfrage!</h2>
        <p>Wir haben Ihre Angaben erhalten. KAFI Transporte prüft die Anfrage und meldet sich persönlich bei Ihnen.</p>
      }
    }

    @if (step() < 5) {
      <div class="row">
        <button class="btn ghost dark" [disabled]="step() === 0" (click)="step.set(step() - 1)">Zurück</button>
        <button class="btn solid" [disabled]="!ok()" (click)="step.set(step() + 1)">{{ step() === 4 ? 'Kostenloses Angebot anfragen →' : 'Weiter' }}</button>
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

  ok() {
    const s = this.step();
    if (s === 0) return !!this.service();
    if (s === 1) return !!(this.originZip && this.originCity);
    if (s === 2) return !!(this.destZip && this.destCity);
    if (s === 3) return !!this.date;
    if (s === 4) return !!(this.name && this.surname && this.phone && this.email);
    return true;
  }
}
