import { Directive, ElementRef, HostListener, Input, NgZone, OnDestroy, OnInit } from '@angular/core';

/** Fade/slide-in sobald das Element im Viewport ist (Webflow "scroll into view"). */
@Directive({ selector: '[reveal]', standalone: true })
export class RevealDirective implements OnInit, OnDestroy {
  @Input() delay = 0;
  private io?: IntersectionObserver;
  constructor(private el: ElementRef<HTMLElement>) {}
  ngOnInit() {
    const e = this.el.nativeElement;
    e.classList.add('rv'); e.style.transitionDelay = this.delay + 'ms';
    this.io = new IntersectionObserver(([x]) => { if (x.isIntersecting) { e.classList.add('in'); this.io?.disconnect(); } }, { threshold: 0.15 });
    this.io.observe(e);
  }
  ngOnDestroy() { this.io?.disconnect(); }
}

/** Scroll-Parallax: speed > 0 bewegt langsamer, < 0 gegenläufig. */
@Directive({ selector: '[parallax]', standalone: true })
export class ParallaxDirective implements OnInit, OnDestroy {
  @Input('parallax') speed = 0.1;
  private fn = () => {
    const e = this.el.nativeElement, r = e.getBoundingClientRect();
    e.style.translate = `0 ${(r.top + r.height / 2 - innerHeight / 2) * this.speed}px`;
  };
  constructor(private el: ElementRef<HTMLElement>, private zone: NgZone) {}
  ngOnInit() {
    if (matchMedia('(prefers-reduced-motion: reduce)').matches) return;
    this.zone.runOutsideAngular(() => addEventListener('scroll', this.fn, { passive: true }));
    this.fn();
  }
  ngOnDestroy() { removeEventListener('scroll', this.fn); }
}

/** 3D-Tilt (perspective + rotateX/Y) für Karten; Kinder mit .ico schweben via translateZ. */
@Directive({ selector: '[tilt]', standalone: true, host: { class: 'tilt' } })
export class TiltDirective {
  constructor(private el: ElementRef<HTMLElement>) {}
  @HostListener('mousemove', ['$event']) move(ev: MouseEvent) {
    const e = this.el.nativeElement, r = e.getBoundingClientRect();
    const x = (ev.clientX - r.left) / r.width - 0.5, y = (ev.clientY - r.top) / r.height - 0.5;
    e.style.transform = `perspective(800px) rotateX(${-y * 14}deg) rotateY(${x * 14}deg) scale(1.03)`;
  }
  @HostListener('mouseleave') leave() { this.el.nativeElement.style.transform = ''; }
}
