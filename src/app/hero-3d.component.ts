import { AfterViewInit, Component, ElementRef, NgZone, OnDestroy, ViewChild } from '@angular/core';
import * as THREE from 'three';

/** Three.js-Hero: schwebende Umzugskartons, Maus-Parallax, Scroll-Rotation. */
@Component({
  selector: 'app-hero-3d', standalone: true, template: '<canvas #c></canvas>',
  styles: [':host{position:absolute;inset:0;display:block}canvas{width:100%;height:100%;display:block}']
})
export class Hero3dComponent implements AfterViewInit, OnDestroy {
  @ViewChild('c', { static: true }) c!: ElementRef<HTMLCanvasElement>;
  private stop = () => {};
  constructor(private zone: NgZone) {}
  ngAfterViewInit() { this.zone.runOutsideAngular(() => this.init()); }
  ngOnDestroy() { this.stop(); }

  private init() {
    const canvas = this.c.nativeElement;
    const renderer = new THREE.WebGLRenderer({ canvas, alpha: true, antialias: true });
    renderer.setPixelRatio(Math.min(devicePixelRatio, 2));
    const scene = new THREE.Scene();
    const cam = new THREE.PerspectiveCamera(45, 1, 0.1, 100); cam.position.z = 14;
    scene.add(new THREE.AmbientLight(0xffffff, 1.2));
    const sun = new THREE.DirectionalLight(0xffffff, 2.4); sun.position.set(5, 8, 6); scene.add(sun);

    const group = new THREE.Group(); scene.add(group);
    const mats = [0xff7a1a, 0xffd2a1, 0x22346b].map(c => new THREE.MeshStandardMaterial({ color: c, roughness: .45, metalness: .1 }));
    const geo = new THREE.BoxGeometry(1, 1, 1), tape = new THREE.BoxGeometry(1.02, .12, 1.02);
    const tapeMat = new THREE.MeshStandardMaterial({ color: 0xf5e6c8, roughness: .6 });
    const items: { m: THREE.Group; v: number; p: number }[] = [];
    for (let i = 0; i < 18; i++) {
      const box = new THREE.Group();
      box.add(new THREE.Mesh(geo, mats[i % 3]), new THREE.Mesh(tape, tapeMat));
      box.scale.setScalar(.6 + Math.random() * 1.1);
      box.position.set((Math.random() - .5) * 18, (Math.random() - .5) * 9, (Math.random() - .5) * 6 - 1);
      box.rotation.set(Math.random() * 3, Math.random() * 3, 0);
      group.add(box); items.push({ m: box, v: .1 + Math.random() * .3, p: Math.random() * 6 });
    }

    const mouse = { x: 0, y: 0 };
    const onMove = (e: MouseEvent) => { mouse.x = e.clientX / innerWidth - .5; mouse.y = e.clientY / innerHeight - .5; };
    const resize = () => {
      const p = canvas.parentElement!; renderer.setSize(p.clientWidth, p.clientHeight, false);
      cam.aspect = p.clientWidth / p.clientHeight; cam.updateProjectionMatrix();
    };
    addEventListener('mousemove', onMove); addEventListener('resize', resize); resize();

    const still = matchMedia('(prefers-reduced-motion: reduce)').matches;
    let raf = 0; const t0 = performance.now();
    const loop = () => {
      const t = (performance.now() - t0) / 1000;
      items.forEach(i => { i.m.rotation.x += i.v * .01; i.m.rotation.y += i.v * .012; i.m.position.y += Math.sin(t + i.p) * .003; });
      group.rotation.y = scrollY * .0012; group.position.y = scrollY * .004;
      cam.position.x += (mouse.x * 3 - cam.position.x) * .05;
      cam.position.y += (-mouse.y * 2 - cam.position.y) * .05; cam.lookAt(0, 0, 0);
      renderer.render(scene, cam);
      if (!still) raf = requestAnimationFrame(loop);
    };
    loop();
    this.stop = () => { cancelAnimationFrame(raf); removeEventListener('mousemove', onMove); removeEventListener('resize', resize); renderer.dispose(); geo.dispose(); };
  }
}
