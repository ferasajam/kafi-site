import { Component, HostListener, signal } from '@angular/core';
import { Hero3dComponent } from './hero-3d.component';
import { EstimateComponent } from './estimate.component';
import { ParallaxDirective, RevealDirective, TiltDirective } from './effects';

@Component({
  selector: 'app-root',
  standalone: true,
  templateUrl: './app.component.html',
  imports: [Hero3dComponent, EstimateComponent, RevealDirective, ParallaxDirective, TiltDirective]
})
export class AppComponent {
  scrolled = signal(false);
  modal = signal(false);
  open = signal(-1);

  nav = [
    { label: 'Leistungen', href: '#services' },
    { label: 'Ablauf', href: '#steps' },
    { label: 'Deutschlandweit', href: '#coverage' },
    { label: 'Bewertungen', href: '#reviews' },
    { label: 'FAQ', href: '#faqs' }
  ];

  services = [
    { i: '🏠', t: 'Privatumzug', d: 'Wohnung, Haus oder WG – Unterstützung beim Transport Ihres Hausstands.' },
    { i: '🏢', t: 'Firmenumzug', d: 'Umzüge von Büros, Praxen oder Unternehmensstandorten.' },
    { i: '🗺️', t: 'Fernumzug', d: 'Deutschlandweite Umzüge über größere Entfernungen.' },
    { i: '🛋️', t: 'Möbeltransport', d: 'Transport einzelner Möbelstücke oder größerer Ladungen.' },
    { i: '🧰', t: 'Möbelmontage', d: 'Abbau und Aufbau von Möbeln, sofern angeboten.' },
    { i: '📦', t: 'Verpackungsservice', d: 'Unterstützung bei der Vorbereitung und Verpackung, sofern angeboten.' },
    { i: '🧹', t: 'Entrümpelung', d: 'Entrümpelungen und Haushaltsauflösungen, sofern angeboten.' },
    { i: '🚧', t: 'Halteverbotszone', d: 'Unterstützung bei der Organisation einer Ladezone, sofern angeboten.' }
  ];

  trust = [
    { label: 'Deutschlandweit', value: 'für Sie unterwegs' },
    { label: 'MyHammer', value: '4,5 / 5' },
    { label: 'Google', value: '4,6 / 5' },
    { label: 'Persönlicher Kontakt', value: 'direkt mit KAFI' }
  ];

  features = [
    { i: '🌍', t: 'Deutschlandweit', d: 'Von Berlin in Städte und Regionen in ganz Deutschland.' },
    { i: '🤝', t: 'Persönlich', d: 'Direkter Kontakt und individuelle Abstimmung.' },
    { i: '🔎', t: 'Transparent', d: 'Individuelle Angebote auf Basis Ihrer Umzugsdaten.' },
    { i: '⚙️', t: 'Flexibel', d: 'Vom einzelnen Möbeltransport bis zum kompletten Umzug.' }
  ];

  faqs = [
    { q: 'Wie weit fährt KAFI Transporte?', a: 'KAFI Transporte hat seinen Standort in Berlin und führt Umzüge deutschlandweit durch.' },
    { q: 'Wie früh sollte ich meinen Umzug buchen?', a: 'Das hängt unter anderem von Route, Umfang und gewünschtem Termin ab. Eine frühzeitige Anfrage erleichtert die Planung.' },
    { q: 'Wie wird der Preis bestimmt?', a: 'Der Preis wird nicht automatisch auf der Website berechnet. Nach Ihrer Anfrage prüft KAFI Transporte die Angaben und erstellt bzw. bespricht ein individuelles Angebot.' },
    { q: 'Kann ich nur einzelne Möbel transportieren lassen?', a: 'Ja, sofern dieser Service für Ihre Anfrage geeignet ist, kann ein individueller Transport einzelner Möbel oder Ladungen organisiert werden.' },
    { q: 'Übernimmt KAFI Transporte Möbelmontagen?', a: 'Nur wenn die Leistung für Ihren Auftrag passend ist. Bitte teilen Sie uns Ihre Anforderungen in der Anfrage mit.' },
    { q: 'Wie bekomme ich ein Angebot?', a: 'Über das Anfrageformular oder telefonisch unter +49 176 62811839.' }
  ];

  phones = [{ c: 'Berlin', n: '+49 176 62811839' }];

  @HostListener('window:scroll') onScroll() {
    this.scrolled.set(scrollY > 40);
  }
}
