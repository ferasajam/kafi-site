var i0=Object.defineProperty,r0=Object.defineProperties;var s0=Object.getOwnPropertyDescriptors;var pp=Object.getOwnPropertySymbols;var o0=Object.prototype.hasOwnProperty,a0=Object.prototype.propertyIsEnumerable;var mp=(n,e,t)=>e in n?i0(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,ht=(n,e)=>{for(var t in e||={})o0.call(e,t)&&mp(n,t,e[t]);if(pp)for(var t of pp(e))a0.call(e,t)&&mp(n,t,e[t]);return n},yt=(n,e)=>r0(n,s0(e));var Ir=(n,e,t)=>new Promise((i,r)=>{var s=l=>{try{a(t.next(l))}catch(c){r(c)}},o=l=>{try{a(t.throw(l))}catch(c){r(c)}},a=l=>l.done?i(l.value):Promise.resolve(l.value).then(s,o);a((t=t.apply(n,e)).next())});function gp(n,e){return Object.is(n,e)}var It=null,Uo=!1,Vo=1,Jn=Symbol("SIGNAL");function Ue(n){let e=It;return It=n,e}function vp(){return It}var Gs={version:0,lastCleanEpoch:0,dirty:!1,producerNode:void 0,producerLastReadVersion:void 0,producerIndexOfThis:void 0,nextProducerIndex:0,liveConsumerNode:void 0,liveConsumerIndexOfThis:void 0,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Ic(n){if(Uo)throw new Error("");if(It===null)return;It.consumerOnSignalRead(n);let e=It.nextProducerIndex++;if(Go(It),e<It.producerNode.length&&It.producerNode[e]!==n&&zs(It)){let t=It.producerNode[e];zo(t,It.producerIndexOfThis[e])}It.producerNode[e]!==n&&(It.producerNode[e]=n,It.producerIndexOfThis[e]=zs(It)?Mp(n,It,e):0),It.producerLastReadVersion[e]=n.version}function l0(){Vo++}function yp(n){if(!(zs(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===Vo)){if(!n.producerMustRecompute(n)&&!Nc(n)){n.dirty=!1,n.lastCleanEpoch=Vo;return}n.producerRecomputeValue(n),n.dirty=!1,n.lastCleanEpoch=Vo}}function _p(n){if(n.liveConsumerNode===void 0)return;let e=Uo;Uo=!0;try{for(let t of n.liveConsumerNode)t.dirty||c0(t)}finally{Uo=e}}function xp(){return It?.consumerAllowSignalWrites!==!1}function c0(n){n.dirty=!0,_p(n),n.consumerMarkedDirty?.(n)}function Ho(n){return n&&(n.nextProducerIndex=0),Ue(n)}function Rc(n,e){if(Ue(e),!(!n||n.producerNode===void 0||n.producerIndexOfThis===void 0||n.producerLastReadVersion===void 0)){if(zs(n))for(let t=n.nextProducerIndex;t<n.producerNode.length;t++)zo(n.producerNode[t],n.producerIndexOfThis[t]);for(;n.producerNode.length>n.nextProducerIndex;)n.producerNode.pop(),n.producerLastReadVersion.pop(),n.producerIndexOfThis.pop()}}function Nc(n){Go(n);for(let e=0;e<n.producerNode.length;e++){let t=n.producerNode[e],i=n.producerLastReadVersion[e];if(i!==t.version||(yp(t),i!==t.version))return!0}return!1}function Pc(n){if(Go(n),zs(n))for(let e=0;e<n.producerNode.length;e++)zo(n.producerNode[e],n.producerIndexOfThis[e]);n.producerNode.length=n.producerLastReadVersion.length=n.producerIndexOfThis.length=0,n.liveConsumerNode&&(n.liveConsumerNode.length=n.liveConsumerIndexOfThis.length=0)}function Mp(n,e,t){if(Ep(n),n.liveConsumerNode.length===0&&bp(n))for(let i=0;i<n.producerNode.length;i++)n.producerIndexOfThis[i]=Mp(n.producerNode[i],n,i);return n.liveConsumerIndexOfThis.push(t),n.liveConsumerNode.push(e)-1}function zo(n,e){if(Ep(n),n.liveConsumerNode.length===1&&bp(n))for(let i=0;i<n.producerNode.length;i++)zo(n.producerNode[i],n.producerIndexOfThis[i]);let t=n.liveConsumerNode.length-1;if(n.liveConsumerNode[e]=n.liveConsumerNode[t],n.liveConsumerIndexOfThis[e]=n.liveConsumerIndexOfThis[t],n.liveConsumerNode.length--,n.liveConsumerIndexOfThis.length--,e<n.liveConsumerNode.length){let i=n.liveConsumerIndexOfThis[e],r=n.liveConsumerNode[e];Go(r),r.producerIndexOfThis[i]=e}}function zs(n){return n.consumerIsAlwaysLive||(n?.liveConsumerNode?.length??0)>0}function Go(n){n.producerNode??=[],n.producerIndexOfThis??=[],n.producerLastReadVersion??=[]}function Ep(n){n.liveConsumerNode??=[],n.liveConsumerIndexOfThis??=[]}function bp(n){return n.producerNode!==void 0}function wp(n){let e=Object.create(u0);e.computation=n;let t=()=>{if(yp(e),Ic(e),e.value===Bo)throw e.error;return e.value};return t[Jn]=e,t}var Tc=Symbol("UNSET"),Ac=Symbol("COMPUTING"),Bo=Symbol("ERRORED"),u0=yt(ht({},Gs),{value:Tc,dirty:!0,error:null,equal:gp,producerMustRecompute(n){return n.value===Tc||n.value===Ac},producerRecomputeValue(n){if(n.value===Ac)throw new Error("Detected cycle in computations.");let e=n.value;n.value=Ac;let t=Ho(n),i;try{i=n.computation()}catch(r){i=Bo,n.error=r}finally{Rc(n,t)}if(e!==Tc&&e!==Bo&&i!==Bo&&n.equal(e,i)){n.value=e;return}n.value=i,n.version++}});function d0(){throw new Error}var Sp=d0;function Cp(){Sp()}function Dp(n){Sp=n}var f0=null;function Tp(n){let e=Object.create(Ip);e.value=n;let t=()=>(Ic(e),e.value);return t[Jn]=e,t}function Fc(n,e){xp()||Cp(),n.equal(n.value,e)||(n.value=e,h0(n))}function Ap(n,e){xp()||Cp(),Fc(n,e(n.value))}var Ip=yt(ht({},Gs),{equal:gp,value:void 0});function h0(n){n.version++,l0(),_p(n),f0?.()}function nt(n){return typeof n=="function"}function Wo(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var jo=Wo(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Ws(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var zt=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(nt(i))try{i()}catch(s){e=s instanceof jo?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{Rp(s)}catch(o){e=e??[],o instanceof jo?e=[...e,...o.errors]:e.push(o)}}if(e)throw new jo(e)}}add(e){var t;if(e&&e!==this)if(this.closed)Rp(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&Ws(t,e)}remove(e){let{_finalizers:t}=this;t&&Ws(t,e),e instanceof n&&e._removeParent(this)}};zt.EMPTY=(()=>{let n=new zt;return n.closed=!0,n})();var Oc=zt.EMPTY;function $o(n){return n instanceof zt||n&&"closed"in n&&nt(n.remove)&&nt(n.add)&&nt(n.unsubscribe)}function Rp(n){nt(n)?n():n.unsubscribe()}var bn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Rr={setTimeout(n,e,...t){let{delegate:i}=Rr;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=Rr;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function qo(n){Rr.setTimeout(()=>{let{onUnhandledError:e}=bn;if(e)e(n);else throw n})}function Lc(){}var Np=kc("C",void 0,void 0);function Pp(n){return kc("E",void 0,n)}function Fp(n){return kc("N",n,void 0)}function kc(n,e,t){return{kind:n,value:e,error:t}}var Xi=null;function Nr(n){if(bn.useDeprecatedSynchronousErrorHandling){let e=!Xi;if(e&&(Xi={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=Xi;if(Xi=null,t)throw i}}else n()}function Op(n){bn.useDeprecatedSynchronousErrorHandling&&Xi&&(Xi.errorThrown=!0,Xi.error=n)}var Yi=class extends zt{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,$o(e)&&e.add(this)):this.destination=g0}static create(e,t,i){return new Pr(e,t,i)}next(e){this.isStopped?Vc(Fp(e),this):this._next(e)}error(e){this.isStopped?Vc(Pp(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?Vc(Np,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},p0=Function.prototype.bind;function Uc(n,e){return p0.call(n,e)}var Bc=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){Xo(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){Xo(i)}else Xo(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){Xo(t)}}},Pr=class extends Yi{constructor(e,t,i){super();let r;if(nt(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&bn.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&Uc(e.next,s),error:e.error&&Uc(e.error,s),complete:e.complete&&Uc(e.complete,s)}):r=e}this.destination=new Bc(r)}};function Xo(n){bn.useDeprecatedSynchronousErrorHandling?Op(n):qo(n)}function m0(n){throw n}function Vc(n,e){let{onStoppedNotification:t}=bn;t&&Rr.setTimeout(()=>t(n,e))}var g0={closed:!0,next:Lc,error:m0,complete:Lc};var Fr=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Lp(n){return n}function kp(n){return n.length===0?Lp:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var Pt=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=y0(t)?t:new Pr(t,i,r);return Nr(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=Up(i),new i((r,s)=>{let o=new Pr({next:a=>{try{t(a)}catch(l){s(l),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[Fr](){return this}pipe(...t){return kp(t)(this)}toPromise(t){return t=Up(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function Up(n){var e;return(e=n??bn.Promise)!==null&&e!==void 0?e:Promise}function v0(n){return n&&nt(n.next)&&nt(n.error)&&nt(n.complete)}function y0(n){return n&&n instanceof Yi||v0(n)&&$o(n)}function _0(n){return nt(n?.lift)}function Or(n){return e=>{if(_0(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function Lr(n,e,t,i,r){return new Hc(n,e,t,i,r)}var Hc=class extends Yi{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(l){e.error(l)}}:super._next,this._error=r?function(a){try{r(a)}catch(l){e.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var Vp=Wo(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var Ln=(()=>{class n extends Pt{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new Yo(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new Vp}next(t){Nr(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){Nr(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){Nr(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?Oc:(this.currentObservers=null,s.push(t),new zt(()=>{this.currentObservers=null,Ws(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new Pt;return t.source=this,t}}return n.create=(e,t)=>new Yo(e,t),n})(),Yo=class extends Ln{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:Oc}};var js=class extends Ln{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};function x0(n){return n[n.length-1]}function Bp(n){return nt(x0(n))?n.pop():void 0}function zp(n,e,t,i){function r(s){return s instanceof t?s:new t(function(o){o(s)})}return new(t||(t=Promise))(function(s,o){function a(u){try{c(i.next(u))}catch(d){o(d)}}function l(u){try{c(i.throw(u))}catch(d){o(d)}}function c(u){u.done?s(u.value):r(u.value).then(a,l)}c((i=i.apply(n,e||[])).next())})}function Hp(n){var e=typeof Symbol=="function"&&Symbol.iterator,t=e&&n[e],i=0;if(t)return t.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function Zi(n){return this instanceof Zi?(this.v=n,this):new Zi(n)}function Gp(n,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(n,e||[]),r,s=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",o),r[Symbol.asyncIterator]=function(){return this},r;function o(h){return function(g){return Promise.resolve(g).then(h,d)}}function a(h,g){i[h]&&(r[h]=function(x){return new Promise(function(m,p){s.push([h,x,m,p])>1||l(h,x)})},g&&(r[h]=g(r[h])))}function l(h,g){try{c(i[h](g))}catch(x){f(s[0][3],x)}}function c(h){h.value instanceof Zi?Promise.resolve(h.value.v).then(u,d):f(s[0][2],h)}function u(h){l("next",h)}function d(h){l("throw",h)}function f(h,g){h(g),s.shift(),s.length&&l(s[0][0],s[0][1])}}function Wp(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=n[Symbol.asyncIterator],t;return e?e.call(n):(n=typeof Hp=="function"?Hp(n):n[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(s){t[s]=n[s]&&function(o){return new Promise(function(a,l){o=n[s](o),r(a,l,o.done,o.value)})}}function r(s,o,a,l){Promise.resolve(l).then(function(c){s({value:c,done:a})},o)}}var Zo=n=>n&&typeof n.length=="number"&&typeof n!="function";function Ko(n){return nt(n?.then)}function Jo(n){return nt(n[Fr])}function Qo(n){return Symbol.asyncIterator&&nt(n?.[Symbol.asyncIterator])}function ea(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function M0(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var ta=M0();function na(n){return nt(n?.[ta])}function ia(n){return Gp(this,arguments,function*(){let t=n.getReader();try{for(;;){let{value:i,done:r}=yield Zi(t.read());if(r)return yield Zi(void 0);yield yield Zi(i)}}finally{t.releaseLock()}})}function ra(n){return nt(n?.getReader)}function Mi(n){if(n instanceof Pt)return n;if(n!=null){if(Jo(n))return E0(n);if(Zo(n))return b0(n);if(Ko(n))return w0(n);if(Qo(n))return jp(n);if(na(n))return S0(n);if(ra(n))return C0(n)}throw ea(n)}function E0(n){return new Pt(e=>{let t=n[Fr]();if(nt(t.subscribe))return t.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function b0(n){return new Pt(e=>{for(let t=0;t<n.length&&!e.closed;t++)e.next(n[t]);e.complete()})}function w0(n){return new Pt(e=>{n.then(t=>{e.closed||(e.next(t),e.complete())},t=>e.error(t)).then(null,qo)})}function S0(n){return new Pt(e=>{for(let t of n)if(e.next(t),e.closed)return;e.complete()})}function jp(n){return new Pt(e=>{D0(n,e).catch(t=>e.error(t))})}function C0(n){return jp(ia(n))}function D0(n,e){var t,i,r,s;return zp(this,void 0,void 0,function*(){try{for(t=Wp(n);i=yield t.next(),!i.done;){let o=i.value;if(e.next(o),e.closed)return}}catch(o){r={error:o}}finally{try{i&&!i.done&&(s=t.return)&&(yield s.call(t))}finally{if(r)throw r.error}}e.complete()})}function kn(n,e,t,i=0,r=!1){let s=e.schedule(function(){t(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(s),!r)return s}function sa(n,e=0){return Or((t,i)=>{t.subscribe(Lr(i,r=>kn(i,n,()=>i.next(r),e),()=>kn(i,n,()=>i.complete(),e),r=>kn(i,n,()=>i.error(r),e)))})}function oa(n,e=0){return Or((t,i)=>{i.add(n.schedule(()=>t.subscribe(i),e))})}function $p(n,e){return Mi(n).pipe(oa(e),sa(e))}function qp(n,e){return Mi(n).pipe(oa(e),sa(e))}function Xp(n,e){return new Pt(t=>{let i=0;return e.schedule(function(){i===n.length?t.complete():(t.next(n[i++]),t.closed||this.schedule())})})}function Yp(n,e){return new Pt(t=>{let i;return kn(t,e,()=>{i=n[ta](),kn(t,e,()=>{let r,s;try{({value:r,done:s}=i.next())}catch(o){t.error(o);return}s?t.complete():t.next(r)},0,!0)}),()=>nt(i?.return)&&i.return()})}function aa(n,e){if(!n)throw new Error("Iterable cannot be null");return new Pt(t=>{kn(t,e,()=>{let i=n[Symbol.asyncIterator]();kn(t,e,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function Zp(n,e){return aa(ia(n),e)}function Kp(n,e){if(n!=null){if(Jo(n))return $p(n,e);if(Zo(n))return Xp(n,e);if(Ko(n))return qp(n,e);if(Qo(n))return aa(n,e);if(na(n))return Yp(n,e);if(ra(n))return Zp(n,e)}throw ea(n)}function zc(n,e){return e?Kp(n,e):Mi(n)}function Ki(n,e){return Or((t,i)=>{let r=0;t.subscribe(Lr(i,s=>{i.next(n.call(e,s,r++))}))})}var{isArray:T0}=Array;function A0(n,e){return T0(e)?n(...e):n(e)}function Jp(n){return Ki(e=>A0(n,e))}var{isArray:I0}=Array,{getPrototypeOf:R0,prototype:N0,keys:P0}=Object;function Qp(n){if(n.length===1){let e=n[0];if(I0(e))return{args:e,keys:null};if(F0(e)){let t=P0(e);return{args:t.map(i=>e[i]),keys:t}}}return{args:n,keys:null}}function F0(n){return n&&typeof n=="object"&&R0(n)===N0}function em(n,e){return n.reduce((t,i,r)=>(t[i]=e[r],t),{})}function Gc(...n){let e=Bp(n),{args:t,keys:i}=Qp(n),r=new Pt(s=>{let{length:o}=t;if(!o){s.complete();return}let a=new Array(o),l=o,c=o;for(let u=0;u<o;u++){let d=!1;Mi(t[u]).subscribe(Lr(s,f=>{d||(d=!0,c--),a[u]=f},()=>l--,void 0,()=>{(!l||!d)&&(c||s.next(i?em(i,a):a),s.complete())}))}});return e?r.pipe(Jp(e)):r}var Um="https://g.co/ng/security#xss",Ge=class extends Error{constructor(e,t){super(Ju(e,t)),this.code=e}};function Ju(n,e){return`${`NG0${Math.abs(n)}`}${e?": "+e:""}`}function Ha(n){return{toString:n}.toString()}function pt(n){for(let e in n)if(n[e]===pt)return e;throw Error("Could not find renamed property on target object.")}function O0(n,e){for(let t in e)e.hasOwnProperty(t)&&!n.hasOwnProperty(t)&&(n[t]=e[t])}function mn(n){if(typeof n=="string")return n;if(Array.isArray(n))return"["+n.map(mn).join(", ")+"]";if(n==null)return""+n;if(n.overriddenName)return`${n.overriddenName}`;if(n.name)return`${n.name}`;let e=n.toString();if(e==null)return""+e;let t=e.indexOf(`
`);return t===-1?e:e.substring(0,t)}function tm(n,e){return n==null||n===""?e===null?"":e:e==null||e===""?n:n+" "+e}var L0=pt({__forward_ref__:pt});function oo(n){return n.__forward_ref__=oo,n.toString=function(){return mn(this())},n}function Gt(n){return Vm(n)?n():n}function Vm(n){return typeof n=="function"&&n.hasOwnProperty(L0)&&n.__forward_ref__===oo}function wt(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function za(n){return{providers:n.providers||[],imports:n.imports||[]}}function Qu(n){return nm(n,Bm)||nm(n,Hm)}function nm(n,e){return n.hasOwnProperty(e)?n[e]:null}function k0(n){let e=n&&(n[Bm]||n[Hm]);return e||null}function im(n){return n&&(n.hasOwnProperty(rm)||n.hasOwnProperty(U0))?n[rm]:null}var Bm=pt({\u0275prov:pt}),rm=pt({\u0275inj:pt}),Hm=pt({ngInjectableDef:pt}),U0=pt({ngInjectorDef:pt}),Be=class{constructor(e,t){this._desc=e,this.ngMetadataName="InjectionToken",this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=wt({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function zm(n){return n&&!!n.\u0275providers}var V0=pt({\u0275cmp:pt}),B0=pt({\u0275dir:pt}),H0=pt({\u0275pipe:pt});var va=pt({\u0275fac:pt}),Xs=pt({__NG_ELEMENT_ID__:pt}),sm=pt({__NG_ENV_ID__:pt});function Ys(n){return typeof n=="string"?n:n==null?"":String(n)}function z0(n){return typeof n=="function"?n.name||n.toString():typeof n=="object"&&n!=null&&typeof n.type=="function"?n.type.name||n.type.toString():Ys(n)}function G0(n,e){let t=e?`. Dependency path: ${e.join(" > ")} > ${n}`:"";throw new Ge(-200,n)}function ed(n,e){throw new Ge(-201,!1)}var Ve=function(n){return n[n.Default=0]="Default",n[n.Host=1]="Host",n[n.Self=2]="Self",n[n.SkipSelf=4]="SkipSelf",n[n.Optional=8]="Optional",n}(Ve||{}),ru;function Gm(){return ru}function Un(n){let e=ru;return ru=n,e}function Wm(n,e,t){let i=Qu(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&Ve.Optional)return null;if(e!==void 0)return e;ed(n,"Injector")}var W0={},Zs=W0,j0="__NG_DI_FLAG__",ya="ngTempTokenPath",$0="ngTokenPath",q0=/\n/gm,X0="\u0275",om="__source",Hr;function Y0(){return Hr}function kr(n){let e=Hr;return Hr=n,e}function Z0(n,e=Ve.Default){if(Hr===void 0)throw new Ge(-203,!1);return Hr===null?Wm(n,void 0,e):Hr.get(n,e&Ve.Optional?null:void 0,e)}function ut(n,e=Ve.Default){return(Gm()||Z0)(Gt(n),e)}function it(n,e=Ve.Default){return ut(n,Ga(e))}function Ga(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function su(n){let e=[];for(let t=0;t<n.length;t++){let i=Gt(n[t]);if(Array.isArray(i)){if(i.length===0)throw new Ge(900,!1);let r,s=Ve.Default;for(let o=0;o<i.length;o++){let a=i[o],l=K0(a);typeof l=="number"?l===-1?r=a.token:s|=l:r=a}e.push(ut(r,s))}else e.push(ut(i))}return e}function K0(n){return n[j0]}function J0(n,e,t,i){let r=n[ya];throw e[om]&&r.unshift(e[om]),n.message=Q0(`
`+n.message,r,t,i),n[$0]=r,n[ya]=null,n}function Q0(n,e,t,i=null){n=n&&n.charAt(0)===`
`&&n.charAt(1)==X0?n.slice(2):n;let r=mn(e);if(Array.isArray(e))r=e.map(mn).join(" -> ");else if(typeof e=="object"){let s=[];for(let o in e)if(e.hasOwnProperty(o)){let a=e[o];s.push(o+":"+(typeof a=="string"?JSON.stringify(a):mn(a)))}r=`{${s.join(", ")}}`}return`${t}${i?"("+i+")":""}[${r}]: ${n.replace(q0,`
  `)}`}function Gr(n,e){let t=n.hasOwnProperty(va);return t?n[va]:null}function ex(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function tx(n){return n.flat(Number.POSITIVE_INFINITY)}function td(n,e){n.forEach(t=>Array.isArray(t)?td(t,e):e(t))}function jm(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function _a(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function nx(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let s=r-2;n[r]=n[s],r--}n[e]=t,n[e+1]=i}}function ix(n,e,t){let i=ao(n,e);return i>=0?n[i|1]=t:(i=~i,nx(n,i,e,t)),i}function Wc(n,e){let t=ao(n,e);if(t>=0)return n[t|1]}function ao(n,e){return rx(n,e,1)}function rx(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let s=i+(r-i>>1),o=n[s<<t];if(e===o)return s<<t;o>e?r=s:i=s+1}return~(r<<t)}var Wr={},pn=[],Ks=new Be(""),$m=new Be("",-1),qm=new Be(""),xa=class{get(e,t=Zs){if(t===Zs){let i=new Error(`NullInjectorError: No provider for ${mn(e)}!`);throw i.name="NullInjectorError",i}return t}},Xm=function(n){return n[n.OnPush=0]="OnPush",n[n.Default=1]="Default",n}(Xm||{}),Hn=function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n}(Hn||{}),wi=function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n}(wi||{});function sx(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}function ou(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];ax(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function ox(n){return n===3||n===4||n===6}function ax(n){return n.charCodeAt(0)===64}function Js(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?am(n,t,r,null,e[++i]):am(n,t,r,null,null))}}return n}function am(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){if(i===null){r!==null&&(n[s+1]=r);return}else if(i===n[s+1]){n[s+2]=r;return}}s++,i!==null&&s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),i!==null&&n.splice(s++,0,i),r!==null&&n.splice(s++,0,r)}var Ym="ng-template";function lx(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&sx(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(nd(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function nd(n){return n.type===4&&n.value!==Ym}function cx(n,e,t){let i=n.type===4&&!t?Ym:n.value;return e===i}function ux(n,e,t){let i=4,r=n.attrs,s=r!==null?hx(r):0,o=!1;for(let a=0;a<e.length;a++){let l=e[a];if(typeof l=="number"){if(!o&&!wn(i)&&!wn(l))return!1;if(o&&wn(l))continue;o=!1,i=l|i&1;continue}if(!o)if(i&4){if(i=2|i&1,l!==""&&!cx(n,l,t)||l===""&&e.length===1){if(wn(i))return!1;o=!0}}else if(i&8){if(r===null||!lx(n,r,l,t)){if(wn(i))return!1;o=!0}}else{let c=e[++a],u=dx(l,r,nd(n),t);if(u===-1){if(wn(i))return!1;o=!0;continue}if(c!==""){let d;if(u>s?d="":d=r[u+1].toLowerCase(),i&2&&c!==d){if(wn(i))return!1;o=!0}}}}return wn(i)||o}function wn(n){return(n&1)===0}function dx(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return px(e,n)}function fx(n,e,t=!1){for(let i=0;i<e.length;i++)if(ux(n,e[i],t))return!0;return!1}function hx(n){for(let e=0;e<n.length;e++){let t=n[e];if(ox(t))return e}return n.length}function px(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function lm(n,e){return n?":not("+e.trim()+")":e}function mx(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!wn(o)&&(e+=lm(s,r),r=""),i=o,s=s||!wn(i);t++}return r!==""&&(e+=lm(s,r)),e}function gx(n){return n.map(mx).join(",")}function vx(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!wn(r))break;r=s}i++}return{attrs:e,classes:t}}function ts(n){return Ha(()=>{let e=Jm(n),t=yt(ht({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===Xm.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||Hn.Emulated,styles:n.styles||pn,_:null,schemas:n.schemas||null,tView:null,id:""});Qm(t);let i=n.dependencies;return t.directiveDefs=um(i,!1),t.pipeDefs=um(i,!0),t.id=Mx(t),t})}function yx(n){return jr(n)||Zm(n)}function _x(n){return n!==null}function Wa(n){return Ha(()=>({type:n.type,bootstrap:n.bootstrap||pn,declarations:n.declarations||pn,imports:n.imports||pn,exports:n.exports||pn,transitiveCompileScopes:null,schemas:n.schemas||null,id:n.id||null}))}function cm(n,e){if(n==null)return Wr;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a=wi.None;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s):(s=r,o=r),e?(t[s]=a!==wi.None?[i,a]:i,e[s]=o):t[s]=i}return t}function Wn(n){return Ha(()=>{let e=Jm(n);return Qm(e),e})}function jr(n){return n[V0]||null}function Zm(n){return n[B0]||null}function Km(n){return n[H0]||null}function xx(n){let e=jr(n)||Zm(n)||Km(n);return e!==null?e.standalone:!1}function Jm(n){let e={};return{type:n.type,providersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputTransforms:null,inputConfig:n.inputs||Wr,exportAs:n.exportAs||null,standalone:n.standalone===!0,signals:n.signals===!0,selectors:n.selectors||pn,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,findHostDirectiveDefs:null,hostDirectives:null,inputs:cm(n.inputs,e),outputs:cm(n.outputs),debugInfo:null}}function Qm(n){n.features?.forEach(e=>e(n))}function um(n,e){if(!n)return null;let t=e?Km:yx;return()=>(typeof n=="function"?n():n).map(i=>t(i)).filter(_x)}function Mx(n){let e=0,t=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,n.consts,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery].join("|");for(let r of t)e=Math.imul(31,e)+r.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function Ex(...n){return{\u0275providers:eg(!0,n),\u0275fromNgModule:!0}}function eg(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return td(e,o=>{let a=o;au(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&tg(r,s),t}function tg(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];id(r,s=>{e(s,i)})}}function au(n,e,t,i){if(n=Gt(n),!n)return!1;let r=null,s=im(n),o=!s&&jr(n);if(!s&&!o){let l=n.ngModule;if(s=im(l),s)r=l;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let l=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let c of l)au(c,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let c;try{td(s.imports,u=>{au(u,e,t,i)&&(c||=[],c.push(u))})}finally{}c!==void 0&&tg(c,e)}if(!a){let c=Gr(r)||(()=>new r);e({provide:r,useFactory:c,deps:pn},r),e({provide:qm,useValue:r,multi:!0},r),e({provide:Ks,useValue:()=>ut(r),multi:!0},r)}let l=s.providers;if(l!=null&&!a){let c=n;id(l,u=>{e(u,c)})}}else return!1;return r!==n&&n.providers!==void 0}function id(n,e){for(let t of n)zm(t)&&(t=t.\u0275providers),Array.isArray(t)?id(t,e):e(t)}var bx=pt({provide:String,useValue:pt});function ng(n){return n!==null&&typeof n=="object"&&bx in n}function wx(n){return!!(n&&n.useExisting)}function Sx(n){return!!(n&&n.useFactory)}function $r(n){return typeof n=="function"}function Cx(n){return!!n.useClass}var ja=new Be(""),da={},Dx={},jc;function rd(){return jc===void 0&&(jc=new xa),jc}var Si=class{},Ma=class extends Si{get destroyed(){return this._destroyed}constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,this.records=new Map,this._ngOnDestroyHooks=new Set,this._onDestroyHooks=[],this._destroyed=!1,cu(e,o=>this.processProvider(o)),this.records.set($m,Ur(void 0,this)),r.has("environment")&&this.records.set(Si,Ur(void 0,this));let s=this.records.get(ja);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(qm,pn,Ve.Self))}destroy(){this.assertNotDestroyed(),this._destroyed=!0;let e=Ue(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),Ue(e)}}onDestroy(e){return this.assertNotDestroyed(),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){this.assertNotDestroyed();let t=kr(this),i=Un(void 0),r;try{return e()}finally{kr(t),Un(i)}}get(e,t=Zs,i=Ve.Default){if(this.assertNotDestroyed(),e.hasOwnProperty(sm))return e[sm](this);i=Ga(i);let r,s=kr(this),o=Un(void 0);try{if(!(i&Ve.SkipSelf)){let l=this.records.get(e);if(l===void 0){let c=Nx(e)&&Qu(e);c&&this.injectableDefInScope(c)?l=Ur(lu(e),da):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l)}let a=i&Ve.Self?rd():this.parent;return t=i&Ve.Optional&&t===Zs?null:t,a.get(e,t)}catch(a){if(a.name==="NullInjectorError"){if((a[ya]=a[ya]||[]).unshift(mn(e)),s)throw a;return J0(a,e,"R3InjectorError",this.source)}else throw a}finally{Un(o),kr(s)}}resolveInjectorInitializers(){let e=Ue(null),t=kr(this),i=Un(void 0),r;try{let s=this.get(Ks,pn,Ve.Self);for(let o of s)o()}finally{kr(t),Un(i),Ue(e)}}toString(){let e=[],t=this.records;for(let i of t.keys())e.push(mn(i));return`R3Injector[${e.join(", ")}]`}assertNotDestroyed(){if(this._destroyed)throw new Ge(205,!1)}processProvider(e){e=Gt(e);let t=$r(e)?e:Gt(e&&e.provide),i=Ax(e);if(!$r(e)&&e.multi===!0){let r=this.records.get(t);r||(r=Ur(void 0,da,!0),r.factory=()=>su(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t){let i=Ue(null);try{return t.value===da&&(t.value=Dx,t.value=t.factory()),typeof t.value=="object"&&t.value&&Rx(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{Ue(i)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=Gt(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function lu(n){let e=Qu(n),t=e!==null?e.factory:Gr(n);if(t!==null)return t;if(n instanceof Be)throw new Ge(204,!1);if(n instanceof Function)return Tx(n);throw new Ge(204,!1)}function Tx(n){if(n.length>0)throw new Ge(204,!1);let t=k0(n);return t!==null?()=>t.factory(n):()=>new n}function Ax(n){if(ng(n))return Ur(void 0,n.useValue);{let e=ig(n);return Ur(e,da)}}function ig(n,e,t){let i;if($r(n)){let r=Gt(n);return Gr(r)||lu(r)}else if(ng(n))i=()=>Gt(n.useValue);else if(Sx(n))i=()=>n.useFactory(...su(n.deps||[]));else if(wx(n))i=()=>ut(Gt(n.useExisting));else{let r=Gt(n&&(n.useClass||n.provide));if(Ix(n))i=()=>new r(...su(n.deps));else return Gr(r)||lu(r)}return i}function Ur(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function Ix(n){return!!n.deps}function Rx(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function Nx(n){return typeof n=="function"||typeof n=="object"&&n instanceof Be}function cu(n,e){for(let t of n)Array.isArray(t)?cu(t,e):t&&zm(t)?cu(t.\u0275providers,e):e(t)}function Px(){return Gm()!==void 0||Y0()!=null}function Fx(n){return typeof n=="function"}var ni=0,Ne=1,De=2,Vt=3,Sn=4,Tn=5,Qs=6,Ea=7,kt=8,qr=9,zn=10,Ft=11,eo=12,dm=13,ns=14,Cn=15,Qi=16,Vr=17,Qn=18,$a=19,rg=20,Ei=21,$c=22,gn=23,Dn=25,sg=1;var er=7,ba=8,Xr=9,Ut=10,wa=function(n){return n[n.None=0]="None",n[n.HasTransplantedViews=2]="HasTransplantedViews",n}(wa||{});function bi(n){return Array.isArray(n)&&typeof n[sg]=="object"}function ii(n){return Array.isArray(n)&&n[sg]===!0}function og(n){return(n.flags&4)!==0}function qa(n){return n.componentOffset>-1}function sd(n){return(n.flags&1)===1}function Ci(n){return!!n.template}function uu(n){return(n[De]&512)!==0}var du=class{constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}};function ag(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}function od(){return lg}function lg(n){return n.type.prototype.ngOnChanges&&(n.setInput=Lx),Ox}od.ngInherit=!0;function Ox(){let n=ug(this),e=n?.current;if(e){let t=n.previous;if(t===Wr)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function Lx(n,e,t,i,r){let s=this.declaredInputs[i],o=ug(n)||kx(n,{previous:Wr,current:null}),a=o.current||(o.current={}),l=o.previous,c=l[s];a[s]=new du(c&&c.currentValue,t,l===Wr),ag(n,e,r,t)}var cg="__ngSimpleChanges__";function ug(n){return n[cg]||null}function kx(n,e){return n[cg]=e}var fm=null;var Vn=function(n,e,t){fm?.(n,e,t)},Ux="svg",Vx="math";function Gn(n){for(;Array.isArray(n);)n=n[ni];return n}function dg(n,e){return Gn(e[n])}function An(n,e){return Gn(e[n.index])}function ad(n,e){return n.data[e]}function Ti(n,e){let t=e[n];return bi(t)?t:t[ni]}function Bx(n){return(n[De]&4)===4}function ld(n){return(n[De]&128)===128}function Hx(n){return ii(n[Vt])}function Yr(n,e){return e==null?null:n[e]}function fg(n){n[Vr]=0}function hg(n){n[De]&1024||(n[De]|=1024,ld(n)&&Ya(n))}function zx(n,e){for(;n>0;)e=e[ns],n--;return e}function Xa(n){return!!(n[De]&9216||n[gn]?.dirty)}function fu(n){n[zn].changeDetectionScheduler?.notify(8),n[De]&64&&(n[De]|=1024),Xa(n)&&Ya(n)}function Ya(n){n[zn].changeDetectionScheduler?.notify(0);let e=tr(n);for(;e!==null&&!(e[De]&8192||(e[De]|=8192,!ld(e)));)e=tr(e)}function pg(n,e){if((n[De]&256)===256)throw new Ge(911,!1);n[Ei]===null&&(n[Ei]=[]),n[Ei].push(e)}function Gx(n,e){if(n[Ei]===null)return;let t=n[Ei].indexOf(e);t!==-1&&n[Ei].splice(t,1)}function tr(n){let e=n[Vt];return ii(e)?e[Vt]:e}var ke={lFrame:wg(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var mg=!1;function Wx(){return ke.lFrame.elementDepthCount}function jx(){ke.lFrame.elementDepthCount++}function $x(){ke.lFrame.elementDepthCount--}function gg(){return ke.bindingsEnabled}function qx(){return ke.skipHydrationRootTNode!==null}function Xx(n){return ke.skipHydrationRootTNode===n}function Yx(){ke.skipHydrationRootTNode=null}function Ze(){return ke.lFrame.lView}function Wt(){return ke.lFrame.tView}function xt(n){return ke.lFrame.contextLView=n,n[kt]}function Mt(n){return ke.lFrame.contextLView=null,n}function Qt(){let n=vg();for(;n!==null&&n.type===64;)n=n.parent;return n}function vg(){return ke.lFrame.currentTNode}function Zx(){let n=ke.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function lo(n,e){let t=ke.lFrame;t.currentTNode=n,t.isParent=e}function yg(){return ke.lFrame.isParent}function Kx(){ke.lFrame.isParent=!1}function _g(){return mg}function hm(n){mg=n}function Jx(){return ke.lFrame.bindingIndex}function Qx(n){return ke.lFrame.bindingIndex=n}function co(){return ke.lFrame.bindingIndex++}function xg(n){let e=ke.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function eM(){return ke.lFrame.inI18n}function tM(n,e){let t=ke.lFrame;t.bindingIndex=t.bindingRootIndex=n,hu(e)}function nM(){return ke.lFrame.currentDirectiveIndex}function hu(n){ke.lFrame.currentDirectiveIndex=n}function iM(n){let e=ke.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function Mg(){return ke.lFrame.currentQueryIndex}function cd(n){ke.lFrame.currentQueryIndex=n}function rM(n){let e=n[Ne];return e.type===2?e.declTNode:e.type===1?n[Tn]:null}function Eg(n,e,t){if(t&Ve.SkipSelf){let r=e,s=n;for(;r=r.parent,r===null&&!(t&Ve.Host);)if(r=rM(s),r===null||(s=s[ns],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=ke.lFrame=bg();return i.currentTNode=e,i.lView=n,!0}function ud(n){let e=bg(),t=n[Ne];ke.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function bg(){let n=ke.lFrame,e=n===null?null:n.child;return e===null?wg(n):e}function wg(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function Sg(){let n=ke.lFrame;return ke.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var Cg=Sg;function dd(){let n=Sg();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function sM(n){return(ke.lFrame.contextLView=zx(n,ke.lFrame.contextLView))[kt]}function Ai(){return ke.lFrame.selectedIndex}function nr(n){ke.lFrame.selectedIndex=n}function Dg(){let n=ke.lFrame;return ad(n.tView,n.selectedIndex)}function oM(){return ke.lFrame.currentNamespace}var Tg=!0;function fd(){return Tg}function hd(n){Tg=n}function aM(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=lg(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function pd(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),l&&(n.viewHooks??=[]).push(-t,l),c&&((n.viewHooks??=[]).push(t,c),(n.viewCheckHooks??=[]).push(t,c)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function fa(n,e,t){Ag(n,e,3,t)}function ha(n,e,t,i){(n[De]&3)===t&&Ag(n,e,t,i)}function qc(n,e){let t=n[De];(t&3)===e&&(t&=16383,t+=1,n[De]=t)}function Ag(n,e,t,i){let r=i!==void 0?n[Vr]&65535:0,s=i??-1,o=e.length-1,a=0;for(let l=r;l<o;l++)if(typeof e[l+1]=="number"){if(a=e[l],i!=null&&a>=i)break}else e[l]<0&&(n[Vr]+=65536),(a<s||s==-1)&&(lM(n,t,e,l),n[Vr]=(n[Vr]&4294901760)+l+2),l++}function pm(n,e){Vn(4,n,e);let t=Ue(null);try{e.call(n)}finally{Ue(t),Vn(5,n,e)}}function lM(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[De]>>14<n[Vr]>>16&&(n[De]&3)===e&&(n[De]+=16384,pm(a,s)):pm(a,s)}var zr=-1,ir=class{constructor(e,t,i){this.factory=e,this.resolving=!1,this.canSeeViewProviders=t,this.injectImpl=i}};function cM(n){return n instanceof ir}function uM(n){return(n.flags&8)!==0}function dM(n){return(n.flags&16)!==0}var Xc={},pu=class{constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){i=Ga(i);let r=this.injector.get(e,Xc,i);return r!==Xc||t===Xc?r:this.parentInjector.get(e,t,i)}};function Ig(n){return n!==zr}function Sa(n){return n&32767}function fM(n){return n>>16}function Ca(n,e){let t=fM(n),i=e;for(;t>0;)i=i[ns],t--;return i}var mu=!0;function mm(n){let e=mu;return mu=n,e}var hM=256,Rg=hM-1,Ng=5,pM=0,Bn={};function mM(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(Xs)&&(i=t[Xs]),i==null&&(i=t[Xs]=pM++);let r=i&Rg,s=1<<r;e.data[n+(r>>Ng)]|=s}function Da(n,e){let t=Pg(n,e);if(t!==-1)return t;let i=e[Ne];i.firstCreatePass&&(n.injectorIndex=e.length,Yc(i.data,n),Yc(e,null),Yc(i.blueprint,null));let r=md(n,e),s=n.injectorIndex;if(Ig(r)){let o=Sa(r),a=Ca(r,e),l=a[Ne].data;for(let c=0;c<8;c++)e[s+c]=a[o+c]|l[o+c]}return e[s+8]=r,s}function Yc(n,e){n.push(0,0,0,0,0,0,0,0,e)}function Pg(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function md(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=Vg(r),i===null)return zr;if(t++,r=r[ns],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return zr}function gu(n,e,t){mM(n,e,t)}function Fg(n,e,t){if(t&Ve.Optional||n!==void 0)return n;ed(e,"NodeInjector")}function Og(n,e,t,i){if(t&Ve.Optional&&i===void 0&&(i=null),!(t&(Ve.Self|Ve.Host))){let r=n[qr],s=Un(void 0);try{return r?r.get(e,i,t&Ve.Optional):Wm(e,i,t&Ve.Optional)}finally{Un(s)}}return Fg(i,e,t)}function Lg(n,e,t,i=Ve.Default,r){if(n!==null){if(e[De]&2048&&!(i&Ve.Self)){let o=_M(n,e,t,i,Bn);if(o!==Bn)return o}let s=kg(n,e,t,i,Bn);if(s!==Bn)return s}return Og(e,t,i,r)}function kg(n,e,t,i,r){let s=vM(t);if(typeof s=="function"){if(!Eg(e,n,i))return i&Ve.Host?Fg(r,t,i):Og(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&Ve.Optional))ed(t);else return o}finally{Cg()}}else if(typeof s=="number"){let o=null,a=Pg(n,e),l=zr,c=i&Ve.Host?e[Cn][Tn]:null;for((a===-1||i&Ve.SkipSelf)&&(l=a===-1?md(n,e):e[a+8],l===zr||!vm(i,!1)?a=-1:(o=e[Ne],a=Sa(l),e=Ca(l,e)));a!==-1;){let u=e[Ne];if(gm(s,a,u.data)){let d=gM(a,e,t,o,i,c);if(d!==Bn)return d}l=e[a+8],l!==zr&&vm(i,e[Ne].data[a+8]===c)&&gm(s,a,e)?(o=u,a=Sa(l),e=Ca(l,e)):a=-1}}return r}function gM(n,e,t,i,r,s){let o=e[Ne],a=o.data[n+8],l=i==null?qa(a)&&mu:i!=o&&(a.type&3)!==0,c=r&Ve.Host&&s===a,u=pa(a,o,t,l,c);return u!==null?rr(e,o,u,a):Bn}function pa(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,l=n.directiveStart,c=n.directiveEnd,u=s>>20,d=i?a:a+u,f=r?a+u:c;for(let h=d;h<f;h++){let g=o[h];if(h<l&&t===g||h>=l&&g.type===t)return h}if(r){let h=o[l];if(h&&Ci(h)&&h.type===t)return l}return null}function rr(n,e,t,i){let r=n[t],s=e.data;if(cM(r)){let o=r;o.resolving&&G0(z0(s[t]));let a=mm(o.canSeeViewProviders);o.resolving=!0;let l,c=o.injectImpl?Un(o.injectImpl):null,u=Eg(n,i,Ve.Default);try{r=n[t]=o.factory(void 0,s,n,i),e.firstCreatePass&&t>=i.directiveStart&&aM(t,s[t],e)}finally{c!==null&&Un(c),mm(a),o.resolving=!1,Cg()}}return r}function vM(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(Xs)?n[Xs]:void 0;return typeof e=="number"?e>=0?e&Rg:yM:e}function gm(n,e,t){let i=1<<n;return!!(t[e+(n>>Ng)]&i)}function vm(n,e){return!(n&Ve.Self)&&!(n&Ve.Host&&e)}var Ji=class{constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return Lg(this._tNode,this._lView,e,Ga(i),t)}};function yM(){return new Ji(Qt(),Ze())}function Ug(n){return Ha(()=>{let e=n.prototype.constructor,t=e[va]||vu(e),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let s=r[va]||vu(r);if(s&&s!==t)return s;r=Object.getPrototypeOf(r)}return s=>new s})}function vu(n){return Vm(n)?()=>{let e=vu(Gt(n));return e&&e()}:Gr(n)}function _M(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[De]&2048&&!(o[De]&512);){let a=kg(s,o,t,i|Ve.Self,Bn);if(a!==Bn)return a;let l=s.parent;if(!l){let c=o[rg];if(c){let u=c.get(t,Bn,i);if(u!==Bn)return u}l=Vg(o),o=o[ns]}s=l}return r}function Vg(n){let e=n[Ne],t=e.type;return t===2?e.declTNode:t===1?n[Tn]:null}function ym(n,e=null,t=null,i){let r=xM(n,e,t,i);return r.resolveInjectorInitializers(),r}function xM(n,e=null,t=null,i,r=new Set){let s=[t||pn,Ex(n)];return i=i||(typeof n=="object"?void 0:mn(n)),new Ma(s,e||rd(),i||null,r)}var Zr=class n{static{this.THROW_IF_NOT_FOUND=Zs}static{this.NULL=new xa}static create(e,t){if(Array.isArray(e))return ym({name:""},t,e,"");{let i=e.name??"";return ym({name:i},e.parent,e.providers,i)}}static{this.\u0275prov=wt({token:n,providedIn:"any",factory:()=>ut($m)})}static{this.__NG_ELEMENT_ID__=-1}};var MM=new Be("");MM.__NG_ELEMENT_ID__=n=>{let e=Qt();if(e===null)throw new Ge(204,!1);if(e.type&2)return e.value;if(n&Ve.Optional)return null;throw new Ge(204,!1)};var EM="ngOriginalError";function Zc(n){return n[EM]}var Bg=!0,Hg=(()=>{class n{static{this.__NG_ELEMENT_ID__=bM}static{this.__NG_ENV_ID__=t=>t}}return n})(),yu=class extends Hg{constructor(e){super(),this._lView=e}onDestroy(e){return pg(this._lView,e),()=>Gx(this._lView,e)}};function bM(){return new yu(Ze())}var Za=(()=>{class n{constructor(){this.taskId=0,this.pendingTasks=new Set,this.hasPendingTasks=new js(!1)}get _hasPendingTasks(){return this.hasPendingTasks.value}add(){this._hasPendingTasks||this.hasPendingTasks.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),t}remove(t){this.pendingTasks.delete(t),this.pendingTasks.size===0&&this._hasPendingTasks&&this.hasPendingTasks.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this._hasPendingTasks&&this.hasPendingTasks.next(!1)}static{this.\u0275prov=wt({token:n,providedIn:"root",factory:()=>new n})}}return n})();var _u=class extends Ln{constructor(e=!1){super(),this.destroyRef=void 0,this.pendingTasks=void 0,this.__isAsync=e,Px()&&(this.destroyRef=it(Hg,{optional:!0})??void 0,this.pendingTasks=it(Za,{optional:!0})??void 0)}emit(e){let t=Ue(null);try{super.next(e)}finally{Ue(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let l=e;r=l.next?.bind(l),s=l.error?.bind(l),o=l.complete?.bind(l)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof zt&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{e(t),i!==void 0&&this.pendingTasks?.remove(i)})}}},Jt=_u;function Ta(...n){}function zg(n){let e,t;function i(){n=Ta;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function _m(n){return queueMicrotask(()=>n()),()=>{n=Ta}}var gd="isAngularZone",Aa=gd+"_ID",wM=0,_t=class n{constructor(e){this.hasPendingMacrotasks=!1,this.hasPendingMicrotasks=!1,this.isStable=!0,this.onUnstable=new Jt(!1),this.onMicrotaskEmpty=new Jt(!1),this.onStable=new Jt(!1),this.onError=new Jt(!1);let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=Bg}=e;if(typeof Zone>"u")throw new Ge(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,DM(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(gd)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new Ge(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new Ge(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,SM,Ta,Ta);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},SM={};function vd(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function CM(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){zg(()=>{n.callbackScheduled=!1,xu(n),n.isCheckStableRunning=!0,vd(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),xu(n)}function DM(n){let e=()=>{CM(n)},t=wM++;n._inner=n._inner.fork({name:"angular",properties:{[gd]:!0,[Aa]:t,[Aa+t]:!0},onInvokeTask:(i,r,s,o,a,l)=>{if(TM(l))return i.invokeTask(s,o,a,l);try{return xm(n),i.invokeTask(s,o,a,l)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),Mm(n)}},onInvoke:(i,r,s,o,a,l,c)=>{try{return xm(n),i.invoke(s,o,a,l,c)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!AM(l)&&e(),Mm(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,xu(n),vd(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function xu(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function xm(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function Mm(n){n._nesting--,vd(n)}var Mu=class{constructor(){this.hasPendingMicrotasks=!1,this.hasPendingMacrotasks=!1,this.isStable=!0,this.onUnstable=new Jt,this.onMicrotaskEmpty=new Jt,this.onStable=new Jt,this.onError=new Jt}run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function TM(n){return Gg(n,"__ignore_ng_zone__")}function AM(n){return Gg(n,"__scheduler_tick__")}function Gg(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var ei=class{constructor(){this._console=console}handleError(e){let t=this._findOriginalError(e);this._console.error("ERROR",e),t&&this._console.error("ORIGINAL ERROR",t)}_findOriginalError(e){let t=e&&Zc(e);for(;t&&Zc(t);)t=Zc(t);return t||null}},IM=new Be("",{providedIn:"root",factory:()=>{let n=it(_t),e=it(ei);return t=>n.runOutsideAngular(()=>e.handleError(t))}});function RM(){return is(Qt(),Ze())}function is(n,e){return new vn(An(n,e))}var vn=(()=>{class n{constructor(t){this.nativeElement=t}static{this.__NG_ELEMENT_ID__=RM}}return n})();function NM(n){return n instanceof vn?n.nativeElement:n}function PM(){return this._results[Symbol.iterator]()}var Eu=class n{get changes(){return this._changes??=new Jt}constructor(e=!1){this._emitDistinctChangesOnly=e,this.dirty=!0,this._onDirty=void 0,this._results=[],this._changesDetected=!1,this._changes=void 0,this.length=0,this.first=void 0,this.last=void 0;let t=n.prototype;t[Symbol.iterator]||(t[Symbol.iterator]=PM)}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=tx(e);(this._changesDetected=!ex(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.emit(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}};function Wg(n){return(n.flags&128)===128}var jg=new Map,FM=0;function OM(){return FM++}function LM(n){jg.set(n[$a],n)}function bu(n){jg.delete(n[$a])}var Em="__ngContext__";function sr(n,e){bi(e)?(n[Em]=e[$a],LM(e)):n[Em]=e}function $g(n){return Xg(n[eo])}function qg(n){return Xg(n[Sn])}function Xg(n){for(;n!==null&&!ii(n);)n=n[Sn];return n}var wu;function Yg(n){wu=n}function kM(){if(wu!==void 0)return wu;if(typeof document<"u")return document;throw new Ge(210,!1)}var yd=new Be("",{providedIn:"root",factory:()=>UM}),UM="ng",_d=new Be(""),rs=new Be("",{providedIn:"platform",factory:()=>"unknown"});var xd=new Be("",{providedIn:"root",factory:()=>kM().body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var VM="h",BM="b";var HM=()=>null;function Md(n,e,t=!1){return HM(n,e,t)}var Zg=!1,zM=new Be("",{providedIn:"root",factory:()=>Zg});var Ia=class{constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Um})`}};function Ka(n){return n instanceof Ia?n.changingThisBreaksApplicationSecurity:n}function Kg(n,e){let t=GM(n);if(t!=null&&t!==e){if(t==="ResourceURL"&&e==="URL")return!0;throw new Error(`Required a safe ${e}, got a ${t} (see ${Um})`)}return t===e}function GM(n){return n instanceof Ia&&n.getTypeName()||null}var WM=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Jg(n){return n=String(n),n.match(WM)?n:"unsafe:"+n}var Ed=function(n){return n[n.NONE=0]="NONE",n[n.HTML=1]="HTML",n[n.STYLE=2]="STYLE",n[n.SCRIPT=3]="SCRIPT",n[n.URL=4]="URL",n[n.RESOURCE_URL=5]="RESOURCE_URL",n}(Ed||{});function Ja(n){let e=jM();return e?e.sanitize(Ed.URL,n)||"":Kg(n,"URL")?Ka(n):Jg(Ys(n))}function jM(){let n=Ze();return n&&n[zn].sanitizer}function Qg(n){return n.ownerDocument.defaultView}var ti=function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n}(ti||{}),$M;function bd(n,e){return $M(n,e)}function Br(n,e,t,i,r){if(i!=null){let s,o=!1;ii(i)?s=i:bi(i)&&(o=!0,i=i[ni]);let a=Gn(i);n===0&&t!==null?r==null?iv(e,t,a):Ra(e,t,a,r||null,!0):n===1&&t!==null?Ra(e,t,a,r||null,!0):n===2?aE(e,a,o):n===3&&e.destroyNode(a),s!=null&&cE(e,n,s,t,r)}}function qM(n,e){return n.createText(e)}function XM(n,e,t){n.setValue(e,t)}function ev(n,e,t){return n.createElement(e,t)}function YM(n,e){tv(n,e),e[ni]=null,e[Tn]=null}function ZM(n,e,t,i,r,s){i[ni]=r,i[Tn]=e,el(n,i,t,1,r,s)}function tv(n,e){e[zn].changeDetectionScheduler?.notify(9),el(n,e,e[Ft],2,null,null)}function KM(n){let e=n[eo];if(!e)return Kc(n[Ne],n);for(;e;){let t=null;if(bi(e))t=e[eo];else{let i=e[Ut];i&&(t=i)}if(!t){for(;e&&!e[Sn]&&e!==n;)bi(e)&&Kc(e[Ne],e),e=e[Vt];e===null&&(e=n),bi(e)&&Kc(e[Ne],e),t=e&&e[Sn]}e=t}}function JM(n,e,t,i){let r=Ut+i,s=t.length;i>0&&(t[r-1][Sn]=e),i<s-Ut?(e[Sn]=t[r],jm(t,Ut+i,e)):(t.push(e),e[Sn]=null),e[Vt]=t;let o=e[Qi];o!==null&&t!==o&&nv(o,e);let a=e[Qn];a!==null&&a.insertView(n),fu(e),e[De]|=128}function nv(n,e){let t=n[Xr],i=e[Vt];if(bi(i))n[De]|=wa.HasTransplantedViews;else{let r=i[Vt][Cn];e[Cn]!==r&&(n[De]|=wa.HasTransplantedViews)}t===null?n[Xr]=[e]:t.push(e)}function wd(n,e){let t=n[Xr],i=t.indexOf(e);t.splice(i,1)}function to(n,e){if(n.length<=Ut)return;let t=Ut+e,i=n[t];if(i){let r=i[Qi];r!==null&&r!==n&&wd(r,i),e>0&&(n[t-1][Sn]=i[Sn]);let s=_a(n,Ut+e);YM(i[Ne],i);let o=s[Qn];o!==null&&o.detachView(s[Ne]),i[Vt]=null,i[Sn]=null,i[De]&=-129}return i}function Qa(n,e){if(!(e[De]&256)){let t=e[Ft];t.destroyNode&&el(n,e,t,3,null,null),KM(e)}}function Kc(n,e){if(e[De]&256)return;let t=Ue(null);try{e[De]&=-129,e[De]|=256,e[gn]&&Pc(e[gn]),eE(n,e),QM(n,e),e[Ne].type===1&&e[Ft].destroy();let i=e[Qi];if(i!==null&&ii(e[Vt])){i!==e[Vt]&&wd(i,e);let r=e[Qn];r!==null&&r.detachView(n)}bu(e)}finally{Ue(t)}}function QM(n,e){let t=n.cleanup,i=e[Ea];if(t!==null)for(let s=0;s<t.length-1;s+=2)if(typeof t[s]=="string"){let o=t[s+3];o>=0?i[o]():i[-o].unsubscribe(),s+=2}else{let o=i[t[s+1]];t[s].call(o)}i!==null&&(e[Ea]=null);let r=e[Ei];if(r!==null){e[Ei]=null;for(let s=0;s<r.length;s++){let o=r[s];o()}}}function eE(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof ir)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],l=s[o+1];Vn(4,a,l);try{l.call(a)}finally{Vn(5,a,l)}}else{Vn(4,r,s);try{s.call(r)}finally{Vn(5,r,s)}}}}}function tE(n,e,t){return nE(n,e.parent,t)}function nE(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[ni];{let{componentOffset:r}=i;if(r>-1){let{encapsulation:s}=n.data[i.directiveStart+r];if(s===Hn.None||s===Hn.Emulated)return null}return An(i,t)}}function Ra(n,e,t,i,r){n.insertBefore(e,t,i,r)}function iv(n,e,t){n.appendChild(e,t)}function bm(n,e,t,i,r){i!==null?Ra(n,e,t,i,r):iv(n,e,t)}function rv(n,e){return n.parentNode(e)}function iE(n,e){return n.nextSibling(e)}function rE(n,e,t){return oE(n,e,t)}function sE(n,e,t){return n.type&40?An(n,t):null}var oE=sE,wm;function Sd(n,e,t,i){let r=tE(n,i,e),s=e[Ft],o=i.parent||e[Tn],a=rE(o,i,e);if(r!=null)if(Array.isArray(t))for(let l=0;l<t.length;l++)bm(s,r,t[l],a,!1);else bm(s,r,t,a,!1);wm!==void 0&&wm(s,i,e,t,r)}function $s(n,e){if(e!==null){let t=e.type;if(t&3)return An(e,n);if(t&4)return Su(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return $s(n,i);{let r=n[e.index];return ii(r)?Su(-1,r):Gn(r)}}else{if(t&128)return $s(n,e.next);if(t&32)return bd(e,n)()||Gn(n[e.index]);{let i=sv(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=tr(n[Cn]);return $s(r,i)}else return $s(n,e.next)}}}return null}function sv(n,e){if(e!==null){let i=n[Cn][Tn],r=e.projection;return i.projection[r]}return null}function Su(n,e){let t=Ut+n+1;if(t<e.length){let i=e[t],r=i[Ne].firstChild;if(r!==null)return $s(i,r)}return e[er]}function aE(n,e,t){n.removeChild(null,e,t)}function Cd(n,e,t,i,r,s,o){for(;t!=null;){if(t.type===128){t=t.next;continue}let a=i[t.index],l=t.type;if(o&&e===0&&(a&&sr(Gn(a),i),t.flags|=2),(t.flags&32)!==32)if(l&8)Cd(n,e,t.child,i,r,s,!1),Br(e,n,r,a,s);else if(l&32){let c=bd(t,i),u;for(;u=c();)Br(e,n,r,u,s);Br(e,n,r,a,s)}else l&16?lE(n,e,i,t,r,s):Br(e,n,r,a,s);t=o?t.projectionNext:t.next}}function el(n,e,t,i,r,s){Cd(t,i,n.firstChild,e,r,s,!1)}function lE(n,e,t,i,r,s){let o=t[Cn],l=o[Tn].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let u=l[c];Br(e,n,r,u,s)}else{let c=l,u=o[Vt];Wg(i)&&(c.flags|=128),Cd(n,e,c,u,r,s,!0)}}function cE(n,e,t,i,r){let s=t[er],o=Gn(t);s!==o&&Br(e,n,i,s,r);for(let a=Ut;a<t.length;a++){let l=t[a];el(l[Ne],l,n,e,i,s)}}function uE(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let s=i.indexOf("-")===-1?void 0:ti.DashCase;r==null?n.removeStyle(t,i,s):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),s|=ti.Important),n.setStyle(t,i,r,s))}}function dE(n,e,t){n.setAttribute(e,"style",t)}function ov(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function av(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&ou(n,e,i),r!==null&&ov(n,e,r),s!==null&&dE(n,e,s)}var ri={};function xe(n=1){lv(Wt(),Ze(),Ai()+n,!1)}function lv(n,e,t,i){if(!i)if((e[De]&3)===3){let s=n.preOrderCheckHooks;s!==null&&fa(e,s,t)}else{let s=n.preOrderHooks;s!==null&&ha(e,s,0,t)}nr(t)}function gt(n,e=Ve.Default){let t=Ze();if(t===null)return ut(n,e);let i=Qt();return Lg(i,t,Gt(n),e)}function cv(n,e,t,i,r,s){let o=Ue(null);try{let a=null;r&wi.SignalBased&&(a=e[i][Jn]),a!==null&&a.transformFn!==void 0&&(s=a.transformFn(s)),r&wi.HasDecoratorInputTransform&&(s=n.inputTransforms[i].call(e,s)),n.setInput!==null?n.setInput(e,a,s,t,i):ag(e,a,i,s)}finally{Ue(o)}}function fE(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)nr(~r);else{let s=r,o=t[++i],a=t[++i];tM(o,s);let l=e[s];a(2,l)}}}finally{nr(-1)}}function tl(n,e,t,i,r,s,o,a,l,c,u){let d=e.blueprint.slice();return d[ni]=r,d[De]=i|4|128|8|64,(c!==null||n&&n[De]&2048)&&(d[De]|=2048),fg(d),d[Vt]=d[ns]=n,d[kt]=t,d[zn]=o||n&&n[zn],d[Ft]=a||n&&n[Ft],d[qr]=l||n&&n[qr]||null,d[Tn]=s,d[$a]=OM(),d[Qs]=u,d[rg]=c,d[Cn]=e.type==2?n[Cn]:d,d}function nl(n,e,t,i,r){let s=n.data[e];if(s===null)s=hE(n,e,t,i,r),eM()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=Zx();s.injectorIndex=o===null?-1:o.injectorIndex}return lo(s,!0),s}function hE(n,e,t,i,r){let s=vg(),o=yg(),a=o?s:s&&s.parent,l=n.data[e]=_E(n,a,t,e,i,r);return n.firstChild===null&&(n.firstChild=l),s!==null&&(o?s.child==null&&l.parent!==null&&(s.child=l):s.next===null&&(s.next=l,l.prev=s)),l}function uv(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function dv(n,e,t,i,r){let s=Ai(),o=i&2;try{nr(-1),o&&e.length>Dn&&lv(n,e,Dn,!1),Vn(o?2:0,r),t(i,r)}finally{nr(s),Vn(o?3:1,r)}}function fv(n,e,t){if(og(e)){let i=Ue(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let l=t[o];a.contentQueries(1,l,o)}}}finally{Ue(i)}}}function hv(n,e,t){gg()&&(SE(n,e,t,An(t,e)),(t.flags&64)===64&&_v(n,e,t))}function pv(n,e,t=An){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function mv(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=Dd(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function Dd(n,e,t,i,r,s,o,a,l,c,u){let d=Dn+i,f=d+r,h=pE(d,f),g=typeof c=="function"?c():c;return h[Ne]={type:n,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:e,data:h.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:f,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:l,consts:g,incompleteFirstPass:!1,ssrId:u}}function pE(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:ri);return t}function mE(n,e,t,i){let s=i.get(zM,Zg)||t===Hn.ShadowDom,o=n.selectRootElement(e,s);return gE(o),o}function gE(n){vE(n)}var vE=()=>null;function yE(n,e,t,i){let r=Ev(e);r.push(t),n.firstCreatePass&&bv(n).push(i,r.length-1)}function _E(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return qx()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:s,mergedAttrs:null,localNames:null,initialInputs:void 0,inputs:null,outputs:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function Sm(n,e,t,i,r){for(let s in e){if(!e.hasOwnProperty(s))continue;let o=e[s];if(o===void 0)continue;i??={};let a,l=wi.None;Array.isArray(o)?(a=o[0],l=o[1]):a=o;let c=s;if(r!==null){if(!r.hasOwnProperty(s))continue;c=r[s]}n===0?Cm(i,t,c,a,l):Cm(i,t,c,a)}return i}function Cm(n,e,t,i,r){let s;n.hasOwnProperty(t)?(s=n[t]).push(e,i):s=n[t]=[e,i],r!==void 0&&s.push(r)}function xE(n,e,t){let i=e.directiveStart,r=e.directiveEnd,s=n.data,o=e.attrs,a=[],l=null,c=null;for(let u=i;u<r;u++){let d=s[u],f=t?t.get(d):null,h=f?f.inputs:null,g=f?f.outputs:null;l=Sm(0,d.inputs,u,l,h),c=Sm(1,d.outputs,u,c,g);let x=l!==null&&o!==null&&!nd(e)?FE(l,u,o):null;a.push(x)}l!==null&&(l.hasOwnProperty("class")&&(e.flags|=8),l.hasOwnProperty("style")&&(e.flags|=16)),e.initialInputs=a,e.inputs=l,e.outputs=c}function ME(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function gv(n,e,t,i,r,s,o,a){let l=An(e,t),c=e.inputs,u;!a&&c!=null&&(u=c[i])?(Td(n,t,u,i,r),qa(e)&&EE(t,e.index)):e.type&3?(i=ME(i),r=o!=null?o(r,e.value||"",i):r,s.setProperty(l,i,r)):e.type&12}function EE(n,e){let t=Ti(e,n);t[De]&16||(t[De]|=64)}function vv(n,e,t,i){if(gg()){let r=i===null?null:{"":-1},s=DE(n,t),o,a;s===null?o=a=null:[o,a]=s,o!==null&&yv(n,e,t,o,r,a),r&&TE(t,i,r)}t.mergedAttrs=Js(t.mergedAttrs,t.attrs)}function yv(n,e,t,i,r,s){for(let c=0;c<i.length;c++)gu(Da(t,e),n,i[c].type);IE(t,n.data.length,i.length);for(let c=0;c<i.length;c++){let u=i[c];u.providersResolver&&u.providersResolver(u)}let o=!1,a=!1,l=uv(n,e,i.length,null);for(let c=0;c<i.length;c++){let u=i[c];t.mergedAttrs=Js(t.mergedAttrs,u.hostAttrs),RE(n,t,e,l,u),AE(l,u,r),u.contentQueries!==null&&(t.flags|=4),(u.hostBindings!==null||u.hostAttrs!==null||u.hostVars!==0)&&(t.flags|=64);let d=u.type.prototype;!o&&(d.ngOnChanges||d.ngOnInit||d.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),o=!0),!a&&(d.ngOnChanges||d.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),a=!0),l++}xE(n,t,s)}function bE(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;wE(o)!=a&&o.push(a),o.push(t,i,s)}}function wE(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function SE(n,e,t,i){let r=t.directiveStart,s=t.directiveEnd;qa(t)&&NE(e,t,n.data[r+t.componentOffset]),n.firstCreatePass||Da(t,e),sr(i,e);let o=t.initialInputs;for(let a=r;a<s;a++){let l=n.data[a],c=rr(e,n,a,t);if(sr(c,e),o!==null&&PE(e,a-r,c,l,t,o),Ci(l)){let u=Ti(t.index,e);u[kt]=rr(e,n,a,t)}}}function _v(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=nM();try{nr(s);for(let a=i;a<r;a++){let l=n.data[a],c=e[a];hu(a),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&CE(l,c)}}finally{nr(-1),hu(o)}}function CE(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function DE(n,e){let t=n.directiveRegistry,i=null,r=null;if(t)for(let s=0;s<t.length;s++){let o=t[s];if(fx(e,o.selectors,!1))if(i||(i=[]),Ci(o))if(o.findHostDirectiveDefs!==null){let a=[];r=r||new Map,o.findHostDirectiveDefs(o,a,r),i.unshift(...a,o);let l=a.length;Cu(n,e,l)}else i.unshift(o),Cu(n,e,0);else r=r||new Map,o.findHostDirectiveDefs?.(o,i,r),i.push(o)}return i===null?null:[i,r]}function Cu(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function TE(n,e,t){if(e){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new Ge(-301,!1);i.push(e[r],s)}}}function AE(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;Ci(e)&&(t[""]=n)}}function IE(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function RE(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=Gr(r.type,!0)),o=new ir(s,Ci(r),gt);n.blueprint[i]=o,t[i]=o,bE(n,e,i,uv(n,t,r.hostVars,ri),r)}function NE(n,e,t){let i=An(e,n),r=mv(t),s=n[zn].rendererFactory,o=16;t.signals?o=4096:t.onPush&&(o=64);let a=il(n,tl(n,r,null,o,i,e,null,s.createRenderer(i,t),null,null,null));n[e.index]=a}function PE(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;){let l=o[a++],c=o[a++],u=o[a++],d=o[a++];cv(i,t,l,c,u,d)}}function FE(n,e,t){let i=null,r=0;for(;r<t.length;){let s=t[r];if(s===0){r+=4;continue}else if(s===5){r+=2;continue}if(typeof s=="number")break;if(n.hasOwnProperty(s)){i===null&&(i=[]);let o=n[s];for(let a=0;a<o.length;a+=3)if(o[a]===e){i.push(s,o[a+1],o[a+2],t[r+1]);break}}r+=2}return i}function xv(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function Mv(n,e){let t=n.contentQueries;if(t!==null){let i=Ue(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];cd(s),a.contentQueries(2,e[o],o)}}}finally{Ue(i)}}}function il(n,e){return n[eo]?n[dm][Sn]=e:n[eo]=e,n[dm]=e,e}function Du(n,e,t){cd(0);let i=Ue(null);try{e(n,t)}finally{Ue(i)}}function Ev(n){return n[Ea]??=[]}function bv(n){return n.cleanup??=[]}function wv(n,e){let t=n[qr],i=t?t.get(ei,null):null;i&&i.handleError(e)}function Td(n,e,t,i,r){for(let s=0;s<t.length;){let o=t[s++],a=t[s++],l=t[s++],c=e[o],u=n.data[o];cv(u,c,i,a,l,r)}}function Sv(n,e,t){let i=dg(e,n);XM(n[Ft],i,t)}function OE(n,e){let t=Ti(e,n),i=t[Ne];LE(i,t);let r=t[ni];r!==null&&t[Qs]===null&&(t[Qs]=Md(r,t[qr])),Ad(i,t,t[kt])}function LE(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function Ad(n,e,t){ud(e);try{let i=n.viewQuery;i!==null&&Du(1,i,t);let r=n.template;r!==null&&dv(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[Qn]?.finishViewCreation(n),n.staticContentQueries&&Mv(n,e),n.staticViewQueries&&Du(2,n.viewQuery,t);let s=n.components;s!==null&&kE(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[De]&=-5,dd()}}function kE(n,e){for(let t=0;t<e.length;t++)OE(n,e[t])}function rl(n,e,t,i){let r=Ue(null);try{let s=e.tView,a=n[De]&4096?4096:16,l=tl(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=n[e.index];l[Qi]=c;let u=n[Qn];return u!==null&&(l[Qn]=u.createEmbeddedView(s)),Ad(s,l,t),l}finally{Ue(r)}}function Cv(n,e){let t=Ut+e;if(t<n.length)return n[t]}function no(n,e){return!e||e.firstChild===null||Wg(n)}function sl(n,e,t,i=!0){let r=e[Ne];if(JM(r,e,n,t),i){let o=Su(t,n),a=e[Ft],l=rv(a,n[er]);l!==null&&ZM(r,n[Tn],a,e,l,o)}let s=e[Qs];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function Dv(n,e){let t=to(n,e);return t!==void 0&&Qa(t[Ne],t),t}function Na(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(Gn(s)),ii(s)&&UE(s,i);let o=t.type;if(o&8)Na(n,e,t.child,i);else if(o&32){let a=bd(t,e),l;for(;l=a();)i.push(l)}else if(o&16){let a=sv(e,t);if(Array.isArray(a))i.push(...a);else{let l=tr(e[Cn]);Na(l[Ne],l,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function UE(n,e){for(let t=Ut;t<n.length;t++){let i=n[t],r=i[Ne].firstChild;r!==null&&Na(i[Ne],i,r,e)}n[er]!==n[ni]&&e.push(n[er])}var Tv=[];function VE(n){return n[gn]??BE(n)}function BE(n){let e=Tv.pop()??Object.create(zE);return e.lView=n,e}function HE(n){n.lView[gn]!==n&&(n.lView=null,Tv.push(n))}var zE=yt(ht({},Gs),{consumerIsAlwaysLive:!0,consumerMarkedDirty:n=>{Ya(n.lView)},consumerOnSignalRead(){this.lView[gn]=this}});function GE(n){let e=n[gn]??Object.create(WE);return e.lView=n,e}var WE=yt(ht({},Gs),{consumerIsAlwaysLive:!0,consumerMarkedDirty:n=>{let e=tr(n.lView);for(;e&&!Av(e[Ne]);)e=tr(e);e&&hg(e)},consumerOnSignalRead(){this.lView[gn]=this}});function Av(n){return n.type!==2}var jE=100;function Iv(n,e=!0,t=0){let i=n[zn],r=i.rendererFactory,s=!1;s||r.begin?.();try{$E(n,t)}catch(o){throw e&&wv(n,o),o}finally{s||(r.end?.(),i.inlineEffectRunner?.flush())}}function $E(n,e){let t=_g();try{hm(!0),Tu(n,e);let i=0;for(;Xa(n);){if(i===jE)throw new Ge(103,!1);i++,Tu(n,1)}}finally{hm(t)}}function qE(n,e,t,i){let r=e[De];if((r&256)===256)return;let s=!1,o=!1;!s&&e[zn].inlineEffectRunner?.flush(),ud(e);let a=!0,l=null,c=null;s||(Av(n)?(c=VE(e),l=Ho(c)):vp()===null?(a=!1,c=GE(e),l=Ho(c)):e[gn]&&(Pc(e[gn]),e[gn]=null));try{fg(e),Qx(n.bindingStartIndex),t!==null&&dv(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let h=n.preOrderCheckHooks;h!==null&&fa(e,h,null)}else{let h=n.preOrderHooks;h!==null&&ha(e,h,0,null),qc(e,0)}if(o||XE(e),Rv(e,0),n.contentQueries!==null&&Mv(n,e),!s)if(u){let h=n.contentCheckHooks;h!==null&&fa(e,h)}else{let h=n.contentHooks;h!==null&&ha(e,h,1),qc(e,1)}fE(n,e);let d=n.components;d!==null&&Pv(e,d,0);let f=n.viewQuery;if(f!==null&&Du(2,f,i),!s)if(u){let h=n.viewCheckHooks;h!==null&&fa(e,h)}else{let h=n.viewHooks;h!==null&&ha(e,h,2),qc(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[$c]){for(let h of e[$c])h();e[$c]=null}s||(e[De]&=-73)}catch(u){throw s||Ya(e),u}finally{c!==null&&(Rc(c,l),a&&HE(c)),dd()}}function Rv(n,e){for(let t=$g(n);t!==null;t=qg(t))for(let i=Ut;i<t.length;i++){let r=t[i];Nv(r,e)}}function XE(n){for(let e=$g(n);e!==null;e=qg(e)){if(!(e[De]&wa.HasTransplantedViews))continue;let t=e[Xr];for(let i=0;i<t.length;i++){let r=t[i];hg(r)}}}function YE(n,e,t){let i=Ti(e,n);Nv(i,t)}function Nv(n,e){ld(n)&&Tu(n,e)}function Tu(n,e){let i=n[Ne],r=n[De],s=n[gn],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&Nc(s)),o||=!1,s&&(s.dirty=!1),n[De]&=-9217,o)qE(i,n,i.template,n[kt]);else if(r&8192){Rv(n,1);let a=i.components;a!==null&&Pv(n,a,1)}}function Pv(n,e,t){for(let i=0;i<e.length;i++)YE(n,e[i],t)}function Id(n,e){let t=_g()?64:1088;for(n[zn].changeDetectionScheduler?.notify(e);n;){n[De]|=t;let i=tr(n);if(uu(n)&&!i)return n;n=i}return null}var or=class{get rootNodes(){let e=this._lView,t=e[Ne];return Na(t,e,t.firstChild,[])}constructor(e,t,i=!0){this._lView=e,this._cdRefInjectingView=t,this.notifyErrorHandler=i,this._appRef=null,this._attachedToViewContainer=!1}get context(){return this._lView[kt]}set context(e){this._lView[kt]=e}get destroyed(){return(this._lView[De]&256)===256}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[Vt];if(ii(e)){let t=e[ba],i=t?t.indexOf(this):-1;i>-1&&(to(e,i),_a(t,i))}this._attachedToViewContainer=!1}Qa(this._lView[Ne],this._lView)}onDestroy(e){pg(this._lView,e)}markForCheck(){Id(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[De]&=-129}reattach(){fu(this._lView),this._lView[De]|=128}detectChanges(){this._lView[De]|=1024,Iv(this._lView,this.notifyErrorHandler)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new Ge(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=uu(this._lView),t=this._lView[Qi];t!==null&&!e&&wd(t,this._lView),tv(this._lView[Ne],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new Ge(902,!1);this._appRef=e;let t=uu(this._lView),i=this._lView[Qi];i!==null&&!t&&nv(i,this._lView),fu(this._lView)}},Pa=(()=>{class n{static{this.__NG_ELEMENT_ID__=JE}}return n})(),ZE=Pa,KE=class extends ZE{constructor(e,t,i){super(),this._declarationLView=e,this._declarationTContainer=t,this.elementRef=i}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,t){return this.createEmbeddedViewImpl(e,t)}createEmbeddedViewImpl(e,t,i){let r=rl(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:t,dehydratedView:i});return new or(r)}};function JE(){return Rd(Qt(),Ze())}function Rd(n,e){return n.type&4?new KE(e,n,is(n,e)):null}var iF=new RegExp(`^(\\d+)*(${BM}|${VM})*(.*)`);var QE=()=>null;function io(n,e){return QE(n,e)}var Kr=class{},Nd=new Be("",{providedIn:"root",factory:()=>!1});var Fv=new Be(""),Ov=new Be(""),Au=class{},Fa=class{};function eb(n){let e=Error(`No component factory found for ${mn(n)}.`);return e[tb]=n,e}var tb="ngComponent";var Iu=class{resolveComponentFactory(e){throw eb(e)}},ro=class{static{this.NULL=new Iu}},Jr=class{},ol=(()=>{class n{constructor(){this.destroyNode=null}static{this.__NG_ELEMENT_ID__=()=>nb()}}return n})();function nb(){let n=Ze(),e=Qt(),t=Ti(e.index,n);return(bi(t)?t:n)[Ft]}var ib=(()=>{class n{static{this.\u0275prov=wt({token:n,providedIn:"root",factory:()=>null})}}return n})();function Ru(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=tm(r,a);else if(s==2){let l=a,c=e[++o];i=tm(i,l+": "+c+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}var Nu=class extends ro{constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=jr(e);return new Oa(t,this.ngModule)}};function Dm(n,e){let t=[];for(let i in n){if(!n.hasOwnProperty(i))continue;let r=n[i];if(r===void 0)continue;let s=Array.isArray(r),o=s?r[0]:r,a=s?r[1]:wi.None;e?t.push({propName:o,templateName:i,isSignal:(a&wi.SignalBased)!==0}):t.push({propName:o,templateName:i})}return t}function rb(n){let e=n.toLowerCase();return e==="svg"?Ux:e==="math"?Vx:null}var Oa=class extends Fa{get inputs(){let e=this.componentDef,t=e.inputTransforms,i=Dm(e.inputs,!0);if(t!==null)for(let r of i)t.hasOwnProperty(r.propName)&&(r.transform=t[r.propName]);return i}get outputs(){return Dm(this.componentDef.outputs,!1)}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=gx(e.selectors),this.ngContentSelectors=e.ngContentSelectors?e.ngContentSelectors:[],this.isBoundToModule=!!t}create(e,t,i,r){let s=Ue(null);try{r=r||this.ngModule;let o=r instanceof Si?r:r?.injector;o&&this.componentDef.getStandaloneInjector!==null&&(o=this.componentDef.getStandaloneInjector(o)||o);let a=o?new pu(e,o):e,l=a.get(Jr,null);if(l===null)throw new Ge(407,!1);let c=a.get(ib,null),u=a.get(Kr,null),d={rendererFactory:l,sanitizer:c,inlineEffectRunner:null,changeDetectionScheduler:u},f=l.createRenderer(null,this.componentDef),h=this.componentDef.selectors[0][0]||"div",g=i?mE(f,i,this.componentDef.encapsulation,a):ev(f,h,rb(h)),x=512;this.componentDef.signals?x|=4096:this.componentDef.onPush||(x|=16);let m=null;g!==null&&(m=Md(g,a,!0));let p=Dd(0,null,null,1,0,null,null,null,null,null,null),S=tl(null,p,null,x,null,null,d,f,a,null,m);ud(S);let E,w,L=null;try{let T=this.componentDef,C,U=null;T.findHostDirectiveDefs?(C=[],U=new Map,T.findHostDirectiveDefs(T,C,U),C.push(T)):C=[T];let ie=sb(S,g);L=ob(ie,g,T,C,S,d,f),w=ad(p,Dn),g&&cb(f,T,g,i),t!==void 0&&ub(w,this.ngContentSelectors,t),E=lb(L,T,C,U,S,[db]),Ad(p,S,null)}catch(T){throw L!==null&&bu(L),bu(S),T}finally{dd()}return new Pu(this.componentType,E,is(w,S),S,w)}finally{Ue(s)}}},Pu=class extends Au{constructor(e,t,i,r,s){super(),this.location=i,this._rootLView=r,this._tNode=s,this.previousInputValues=null,this.instance=t,this.hostView=this.changeDetectorRef=new or(r,void 0,!1),this.componentType=e}setInput(e,t){let i=this._tNode.inputs,r;if(i!==null&&(r=i[e])){if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let s=this._rootLView;Td(s[Ne],s,r,e,t),this.previousInputValues.set(e,t);let o=Ti(this._tNode.index,s);Id(o,1)}}get injector(){return new Ji(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function sb(n,e){let t=n[Ne],i=Dn;return n[i]=e,nl(t,i,2,"#host",null)}function ob(n,e,t,i,r,s,o){let a=r[Ne];ab(i,n,e,o);let l=null;e!==null&&(l=Md(e,r[qr]));let c=s.rendererFactory.createRenderer(e,t),u=16;t.signals?u=4096:t.onPush&&(u=64);let d=tl(r,mv(t),null,u,r[n.index],n,s,c,null,null,l);return a.firstCreatePass&&Cu(a,n,i.length-1),il(r,d),r[n.index]=d}function ab(n,e,t,i){for(let r of n)e.mergedAttrs=Js(e.mergedAttrs,r.hostAttrs);e.mergedAttrs!==null&&(Ru(e,e.mergedAttrs,!0),t!==null&&av(i,t,e))}function lb(n,e,t,i,r,s){let o=Qt(),a=r[Ne],l=An(o,r);yv(a,r,o,t,null,i);for(let u=0;u<t.length;u++){let d=o.directiveStart+u,f=rr(r,a,d,o);sr(f,r)}_v(a,r,o),l&&sr(l,r);let c=rr(r,a,o.directiveStart+o.componentOffset,o);if(n[kt]=r[kt]=c,s!==null)for(let u of s)u(c,e);return fv(a,o,r),c}function cb(n,e,t,i){if(i)ou(n,t,["ng-version","18.2.14"]);else{let{attrs:r,classes:s}=vx(e.selectors[0]);r&&ou(n,t,r),s&&s.length>0&&ov(n,t,s.join(" "))}}function ub(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null?Array.from(s):null)}}function db(){let n=Qt();pd(Ze()[Ne],n)}var Pd=(()=>{class n{static{this.__NG_ELEMENT_ID__=fb}}return n})();function fb(){let n=Qt();return kv(n,Ze())}var hb=Pd,Lv=class extends hb{constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return is(this._hostTNode,this._hostLView)}get injector(){return new Ji(this._hostTNode,this._hostLView)}get parentInjector(){let e=md(this._hostTNode,this._hostLView);if(Ig(e)){let t=Ca(e,this._hostLView),i=Sa(e),r=t[Ne].data[i+8];return new Ji(r,t)}else return new Ji(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=Tm(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-Ut}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=io(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,no(this._hostTNode,o)),a}createComponent(e,t,i,r,s){let o=e&&!Fx(e),a;if(o)a=t;else{let g=t||{};a=g.index,i=g.injector,r=g.projectableNodes,s=g.environmentInjector||g.ngModuleRef}let l=o?e:new Oa(jr(e)),c=i||this.parentInjector;if(!s&&l.ngModule==null){let x=(o?c:this.parentInjector).get(Si,null);x&&(s=x)}let u=jr(l.componentType??{}),d=io(this._lContainer,u?.id??null),f=d?.firstChild??null,h=l.create(c,r,f,s);return this.insertImpl(h.hostView,a,no(this._hostTNode,d)),h}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(Hx(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let l=r[Vt],c=new Lv(l,l[Tn],l[Vt]);c.detach(c.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return sl(o,r,s,i),e.attachToViewContainerRef(),jm(Jc(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=Tm(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=to(this._lContainer,t);i&&(_a(Jc(this._lContainer),t),Qa(i[Ne],i))}detach(e){let t=this._adjustIndex(e,-1),i=to(this._lContainer,t);return i&&_a(Jc(this._lContainer),t)!=null?new or(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function Tm(n){return n[ba]}function Jc(n){return n[ba]||(n[ba]=[])}function kv(n,e){let t,i=e[n.index];return ii(i)?t=i:(t=xv(i,e,null,n),e[n.index]=t,il(e,t)),mb(t,e,n,i),new Lv(t,n,e)}function pb(n,e){let t=n[Ft],i=t.createComment(""),r=An(e,n),s=rv(t,r);return Ra(t,s,i,iE(t,r),!1),i}var mb=yb,gb=()=>!1;function vb(n,e,t){return gb(n,e,t)}function yb(n,e,t,i){if(n[er])return;let r;t.type&8?r=Gn(i):r=pb(e,t),n[er]=r}var Fu=class n{constructor(e){this.queryList=e,this.matches=null}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},Ou=class n{constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)Fd(e,t).matches!==null&&this.queries[t].setDirty()}},Lu=class{constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=Cb(e):this.predicate=e}},ku=class n{constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},Uu=class n{constructor(e,t=-1){this.metadata=e,this.matches=null,this.indexInDeclarationView=-1,this.crossesNgTemplate=!1,this._appliesToNextNode=!0,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,_b(t,s)),this.matchTNodeWithReadOption(e,t,pa(t,e,s,!1,!1))}else i===Pa?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,pa(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===vn||r===Pd||r===Pa&&t.type&4)this.addMatch(t.index,-2);else{let s=pa(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function _b(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function xb(n,e){return n.type&11?is(n,e):n.type&4?Rd(n,e):null}function Mb(n,e,t,i){return t===-1?xb(e,n):t===-2?Eb(n,e,i):rr(n,n[Ne],t,e)}function Eb(n,e,t){if(t===vn)return is(e,n);if(t===Pa)return Rd(e,n);if(t===Pd)return kv(e,n)}function Uv(n,e,t,i){let r=e[Qn].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let l=0;o!==null&&l<o.length;l+=2){let c=o[l];if(c<0)a.push(null);else{let u=s[c];a.push(Mb(e,u,o[l+1],t.metadata.read))}}r.matches=a}return r.matches}function Vu(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=Uv(n,e,r,t);for(let a=0;a<s.length;a+=2){let l=s[a];if(l>0)i.push(o[a/2]);else{let c=s[a+1],u=e[-l];for(let d=Ut;d<u.length;d++){let f=u[d];f[Qi]===f[Vt]&&Vu(f[Ne],f,c,i)}if(u[Xr]!==null){let d=u[Xr];for(let f=0;f<d.length;f++){let h=d[f];Vu(h[Ne],h,c,i)}}}}}return i}function bb(n,e){return n[Qn].queries[e].queryList}function wb(n,e,t){let i=new Eu((t&4)===4);return yE(n,e,i,i.destroy),(e[Qn]??=new Ou).queries.push(new Fu(i))-1}function Sb(n,e,t){let i=Wt();return i.firstCreatePass&&(Db(i,new Lu(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),wb(i,Ze(),e)}function Cb(n){return n.split(",").map(e=>e.trim())}function Db(n,e,t){n.queries===null&&(n.queries=new ku),n.queries.track(new Uu(e,t))}function Fd(n,e){return n.queries.getByIndex(e)}function Tb(n,e){let t=n[Ne],i=Fd(t,e);return i.crossesNgTemplate?Vu(t,n,e,[]):Uv(t,n,i,e)}var Am=new Set;function ss(n){Am.has(n)||(Am.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}function Ab(n){return typeof n=="function"&&n[Jn]!==void 0}function cn(n,e){ss("NgSignals");let t=Tp(n),i=t[Jn];return e?.equal&&(i.equal=e.equal),t.set=r=>Fc(i,r),t.update=r=>Ap(i,r),t.asReadonly=Ib.bind(t),t}function Ib(){let n=this[Jn];if(n.readonlyFn===void 0){let e=()=>this();e[Jn]=n,n.readonlyFn=e}return n.readonlyFn}function Vv(n){return Ab(n)&&typeof n.set=="function"}function Rb(n){return Object.getPrototypeOf(n.prototype).constructor}function os(n){let e=Rb(n.type),t=!0,i=[n];for(;e;){let r;if(Ci(n))r=e.\u0275cmp||e.\u0275dir;else{if(e.\u0275cmp)throw new Ge(903,!1);r=e.\u0275dir}if(r){if(t){i.push(r);let o=n;o.inputs=la(n.inputs),o.inputTransforms=la(n.inputTransforms),o.declaredInputs=la(n.declaredInputs),o.outputs=la(n.outputs);let a=r.hostBindings;a&&Lb(n,a);let l=r.viewQuery,c=r.contentQueries;if(l&&Fb(n,l),c&&Ob(n,c),Nb(n,r),O0(n.outputs,r.outputs),Ci(r)&&r.data.animation){let u=n.data;u.animation=(u.animation||[]).concat(r.data.animation)}}let s=r.features;if(s)for(let o=0;o<s.length;o++){let a=s[o];a&&a.ngInherit&&a(n),a===os&&(t=!1)}}e=Object.getPrototypeOf(e)}Pb(i)}function Nb(n,e){for(let t in e.inputs){if(!e.inputs.hasOwnProperty(t)||n.inputs.hasOwnProperty(t))continue;let i=e.inputs[t];if(i!==void 0&&(n.inputs[t]=i,n.declaredInputs[t]=e.declaredInputs[t],e.inputTransforms!==null)){let r=Array.isArray(i)?i[0]:i;if(!e.inputTransforms.hasOwnProperty(r))continue;n.inputTransforms??={},n.inputTransforms[r]=e.inputTransforms[r]}}}function Pb(n){let e=0,t=null;for(let i=n.length-1;i>=0;i--){let r=n[i];r.hostVars=e+=r.hostVars,r.hostAttrs=Js(r.hostAttrs,t=Js(t,r.hostAttrs))}}function la(n){return n===Wr?{}:n===pn?[]:n}function Fb(n,e){let t=n.viewQuery;t?n.viewQuery=(i,r)=>{e(i,r),t(i,r)}:n.viewQuery=e}function Ob(n,e){let t=n.contentQueries;t?n.contentQueries=(i,r,s)=>{e(i,r,s),t(i,r,s)}:n.contentQueries=e}function Lb(n,e){let t=n.hostBindings;t?n.hostBindings=(i,r)=>{e(i,r),t(i,r)}:n.hostBindings=e}var Qr=class{};var La=class extends Qr{constructor(e){super(),this.componentFactoryResolver=new Nu(this),this.instance=null;let t=new Ma([...e.providers,{provide:Qr,useValue:this},{provide:ro,useValue:this.componentFactoryResolver}],e.parent||rd(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function kb(n,e,t=null){return new La({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}function Di(n,e,t){let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function Ub(n,e,t,i){let r=Di(n,e,t);return Di(n,e+1,i)||r}function Vb(n){return(n.flags&32)===32}function Bb(n,e,t,i,r,s,o,a,l){let c=e.consts,u=nl(e,n,4,o||null,a||null);vv(e,t,u,Yr(c,l)),pd(e,u);let d=u.tView=Dd(2,u,i,r,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,c,null);return e.queries!==null&&(e.queries.template(e,u),d.queries=e.queries.embeddedTView(u)),u}function Bu(n,e,t,i,r,s,o,a,l,c){let u=t+Dn,d=e.firstCreatePass?Bb(u,e,n,i,r,s,o,a,l):e.data[u];lo(d,!1);let f=Hb(e,n,d,t);fd()&&Sd(e,n,f,d),sr(f,n);let h=xv(f,n,f,d);return n[u]=h,il(n,h),vb(h,d,n),sd(d)&&hv(e,n,d),l!=null&&pv(n,d,c),d}function uo(n,e,t,i,r,s,o,a){let l=Ze(),c=Wt(),u=Yr(c.consts,s);return Bu(l,c,n,e,t,i,r,u,o,a),uo}var Hb=zb;function zb(n,e,t,i){return hd(!0),e[Ft].createComment("")}var qs=function(n){return n[n.EarlyRead=0]="EarlyRead",n[n.Write=1]="Write",n[n.MixedReadWrite=2]="MixedReadWrite",n[n.Read=3]="Read",n}(qs||{}),Gb=(()=>{class n{constructor(){this.impl=null}execute(){this.impl?.execute()}static{this.\u0275prov=wt({token:n,providedIn:"root",factory:()=>new n})}}return n})(),Im=class n{constructor(){this.ngZone=it(_t),this.scheduler=it(Kr),this.errorHandler=it(ei,{optional:!0}),this.sequences=new Set,this.deferredRegistrations=new Set,this.executing=!1}static{this.PHASES=[qs.EarlyRead,qs.Write,qs.MixedReadWrite,qs.Read]}execute(){this.executing=!0;for(let e of n.PHASES)for(let t of this.sequences)if(!(t.erroredOrDestroyed||!t.hooks[e]))try{t.pipelinedValue=this.ngZone.runOutsideAngular(()=>t.hooks[e](t.pipelinedValue))}catch(i){t.erroredOrDestroyed=!0,this.errorHandler?.handleError(i)}this.executing=!1;for(let e of this.sequences)e.afterRun(),e.once&&(this.sequences.delete(e),e.destroy());for(let e of this.deferredRegistrations)this.sequences.add(e);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear()}register(e){this.executing?this.deferredRegistrations.add(e):(this.sequences.add(e),this.scheduler.notify(6))}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}static{this.\u0275prov=wt({token:n,providedIn:"root",factory:()=>new n})}};function Wb(n,e,t,i){return Di(n,co(),t)?e+Ys(t)+i:ri}function jb(n,e,t,i,r,s){let o=Jx(),a=Ub(n,o,t,r);return xg(2),a?e+Ys(t)+i+Ys(r)+s:ri}function ca(n,e){return n<<17|e<<2}function ar(n){return n>>17&32767}function $b(n){return(n&2)==2}function qb(n,e){return n&131071|e<<17}function Hu(n){return n|2}function es(n){return(n&131068)>>2}function Qc(n,e){return n&-131069|e<<2}function Xb(n){return(n&1)===1}function zu(n){return n|1}function Yb(n,e,t,i,r,s){let o=s?e.classBindings:e.styleBindings,a=ar(o),l=es(o);n[i]=t;let c=!1,u;if(Array.isArray(t)){let d=t;u=d[1],(u===null||ao(d,u)>0)&&(c=!0)}else u=t;if(r)if(l!==0){let f=ar(n[a+1]);n[i+1]=ca(f,a),f!==0&&(n[f+1]=Qc(n[f+1],i)),n[a+1]=qb(n[a+1],i)}else n[i+1]=ca(a,0),a!==0&&(n[a+1]=Qc(n[a+1],i)),a=i;else n[i+1]=ca(l,0),a===0?a=i:n[l+1]=Qc(n[l+1],i),l=i;c&&(n[i+1]=Hu(n[i+1])),Rm(n,u,i,!0),Rm(n,u,i,!1),Zb(e,u,n,i,s),o=ca(a,l),s?e.classBindings=o:e.styleBindings=o}function Zb(n,e,t,i,r){let s=r?n.residualClasses:n.residualStyles;s!=null&&typeof e=="string"&&ao(s,e)>=0&&(t[i+1]=zu(t[i+1]))}function Rm(n,e,t,i){let r=n[t+1],s=e===null,o=i?ar(r):es(r),a=!1;for(;o!==0&&(a===!1||s);){let l=n[o],c=n[o+1];Kb(l,e)&&(a=!0,n[o+1]=i?zu(c):Hu(c)),o=i?ar(c):es(c)}a&&(n[t+1]=i?Hu(r):zu(r))}function Kb(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?ao(n,e)>=0:!1}function un(n,e,t){let i=Ze(),r=co();if(Di(i,r,e)){let s=Wt(),o=Dg();gv(s,o,i,n,e,i[Ft],t,!1)}return un}function Nm(n,e,t,i,r){let s=e.inputs,o=r?"class":"style";Td(n,t,s[o],o,i)}function Od(n,e,t){return Bv(n,e,t,!1),Od}function Ii(n,e){return Bv(n,e,null,!0),Ii}function Bv(n,e,t,i){let r=Ze(),s=Wt(),o=xg(2);if(s.firstUpdatePass&&Qb(s,n,o,i),e!==ri&&Di(r,o,e)){let a=s.data[Ai()];rw(s,a,r,r[Ft],n,r[o+1]=sw(e,t),i,o)}}function Jb(n,e){return e>=n.expandoStartIndex}function Qb(n,e,t,i){let r=n.data;if(r[t+1]===null){let s=r[Ai()],o=Jb(n,t);ow(s,i)&&e===null&&!o&&(e=!1),e=ew(r,s,e,i),Yb(r,s,e,t,o,i)}}function ew(n,e,t,i){let r=iM(n),s=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=eu(null,n,e,t,i),t=so(t,e.attrs,i),s=null);else{let o=e.directiveStylingLast;if(o===-1||n[o]!==r)if(t=eu(r,n,e,t,i),s===null){let l=tw(n,e,i);l!==void 0&&Array.isArray(l)&&(l=eu(null,n,e,l[1],i),l=so(l,e.attrs,i),nw(n,e,i,l))}else s=iw(n,e,i)}return s!==void 0&&(i?e.residualClasses=s:e.residualStyles=s),t}function tw(n,e,t){let i=t?e.classBindings:e.styleBindings;if(es(i)!==0)return n[ar(i)]}function nw(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[ar(r)]=i}function iw(n,e,t){let i,r=e.directiveEnd;for(let s=1+e.directiveStylingLast;s<r;s++){let o=n[s].hostAttrs;i=so(i,o,t)}return so(i,e.attrs,t)}function eu(n,e,t,i,r){let s=null,o=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<o&&(s=e[a],i=so(i,s.hostAttrs,r),s!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function so(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let s=0;s<e.length;s++){let o=e[s];typeof o=="number"?r=o:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),ix(n,o,t?!0:e[++s]))}return n===void 0?null:n}function rw(n,e,t,i,r,s,o,a){if(!(e.type&3))return;let l=n.data,c=l[a+1],u=Xb(c)?Pm(l,e,t,r,es(c),o):void 0;if(!ka(u)){ka(s)||$b(c)&&(s=Pm(l,null,t,r,a,o));let d=dg(Ai(),t);uE(i,o,d,r,s)}}function Pm(n,e,t,i,r,s){let o=e===null,a;for(;r>0;){let l=n[r],c=Array.isArray(l),u=c?l[1]:l,d=u===null,f=t[r+1];f===ri&&(f=d?pn:void 0);let h=d?Wc(f,i):u===i?f:void 0;if(c&&!ka(h)&&(h=Wc(l,i)),ka(h)&&(a=h,o))return a;let g=n[r+1];r=o?ar(g):es(g)}if(e!==null){let l=s?e.residualClasses:e.residualStyles;l!=null&&(a=Wc(l,i))}return a}function ka(n){return n!==void 0}function sw(n,e){return n==null||n===""||(typeof e=="string"?n=n+e:typeof n=="object"&&(n=mn(Ka(n)))),n}function ow(n,e){return(n.flags&(e?8:16))!==0}var Gu=class{destroy(e){}updateValue(e,t){}swap(e,t){let i=Math.min(e,t),r=Math.max(e,t),s=this.detach(r);if(r-i>1){let o=this.detach(i);this.attach(i,s),this.attach(r,o)}else this.attach(i,s)}move(e,t){this.attach(t,this.detach(e))}};function tu(n,e,t,i,r){return n===t&&Object.is(e,i)?1:Object.is(r(n,e),r(t,i))?-1:0}function aw(n,e,t){let i,r,s=0,o=n.length-1,a=void 0;if(Array.isArray(e)){let l=e.length-1;for(;s<=o&&s<=l;){let c=n.at(s),u=e[s],d=tu(s,c,s,u,t);if(d!==0){d<0&&n.updateValue(s,u),s++;continue}let f=n.at(o),h=e[l],g=tu(o,f,l,h,t);if(g!==0){g<0&&n.updateValue(o,h),o--,l--;continue}let x=t(s,c),m=t(o,f),p=t(s,u);if(Object.is(p,m)){let S=t(l,h);Object.is(S,x)?(n.swap(s,o),n.updateValue(o,h),l--,o--):n.move(o,s),n.updateValue(s,u),s++;continue}if(i??=new Ua,r??=Om(n,s,o,t),Wu(n,i,s,p))n.updateValue(s,u),s++,o++;else if(r.has(p))i.set(x,n.detach(s)),o--;else{let S=n.create(s,e[s]);n.attach(s,S),s++,o++}}for(;s<=l;)Fm(n,i,t,s,e[s]),s++}else if(e!=null){let l=e[Symbol.iterator](),c=l.next();for(;!c.done&&s<=o;){let u=n.at(s),d=c.value,f=tu(s,u,s,d,t);if(f!==0)f<0&&n.updateValue(s,d),s++,c=l.next();else{i??=new Ua,r??=Om(n,s,o,t);let h=t(s,d);if(Wu(n,i,s,h))n.updateValue(s,d),s++,o++,c=l.next();else if(!r.has(h))n.attach(s,n.create(s,d)),s++,o++,c=l.next();else{let g=t(s,u);i.set(g,n.detach(s)),o--}}}for(;!c.done;)Fm(n,i,t,n.length,c.value),c=l.next()}for(;s<=o;)n.destroy(n.detach(o--));i?.forEach(l=>{n.destroy(l)})}function Wu(n,e,t,i){return e!==void 0&&e.has(i)?(n.attach(t,e.get(i)),e.delete(i),!0):!1}function Fm(n,e,t,i,r){if(Wu(n,e,i,t(i,r)))n.updateValue(i,r);else{let s=n.create(i,r);n.attach(i,s)}}function Om(n,e,t,i){let r=new Set;for(let s=e;s<=t;s++)r.add(i(s,n.at(s)));return r}var Ua=class{constructor(){this.kvMap=new Map,this._vMap=void 0}has(e){return this.kvMap.has(e)}delete(e){if(!this.has(e))return!1;let t=this.kvMap.get(e);return this._vMap!==void 0&&this._vMap.has(t)?(this.kvMap.set(e,this._vMap.get(t)),this._vMap.delete(t)):this.kvMap.delete(e),!0}get(e){return this.kvMap.get(e)}set(e,t){if(this.kvMap.has(e)){let i=this.kvMap.get(e);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,t)}else this.kvMap.set(e,t)}forEach(e){for(let[t,i]of this.kvMap)if(e(i,t),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),e(i,t)}}};function as(n,e){ss("NgControlFlow");let t=Ze(),i=co(),r=t[i]!==ri?t[i]:-1,s=r!==-1?Va(t,Dn+r):void 0,o=0;if(Di(t,i,n)){let a=Ue(null);try{if(s!==void 0&&Dv(s,o),n!==-1){let l=Dn+n,c=Va(t,l),u=Xu(t[Ne],l),d=io(c,u.tView.ssrId),f=rl(t,u,e,{dehydratedView:d});sl(c,f,o,no(u,d))}}finally{Ue(a)}}else if(s!==void 0){let a=Cv(s,o);a!==void 0&&(a[kt]=e)}}var ju=class{constructor(e,t,i){this.lContainer=e,this.$implicit=t,this.$index=i}get $count(){return this.lContainer.length-Ut}};var $u=class{constructor(e,t,i){this.hasEmptyBlock=e,this.trackByFn=t,this.liveCollection=i}};function jn(n,e,t,i,r,s,o,a,l,c,u,d,f){ss("NgControlFlow");let h=Ze(),g=Wt(),x=l!==void 0,m=Ze(),p=a?o.bind(m[Cn][kt]):o,S=new $u(x,p);m[Dn+n]=S,Bu(h,g,n+1,e,t,i,r,Yr(g.consts,s)),x&&Bu(h,g,n+2,l,c,u,d,Yr(g.consts,f))}var qu=class extends Gu{constructor(e,t,i){super(),this.lContainer=e,this.hostLView=t,this.templateTNode=i,this.operationsCounter=void 0,this.needsIndexUpdate=!1}get length(){return this.lContainer.length-Ut}at(e){return this.getLView(e)[kt].$implicit}attach(e,t){let i=t[Qs];this.needsIndexUpdate||=e!==this.length,sl(this.lContainer,t,e,no(this.templateTNode,i))}detach(e){return this.needsIndexUpdate||=e!==this.length-1,lw(this.lContainer,e)}create(e,t){let i=io(this.lContainer,this.templateTNode.tView.ssrId),r=rl(this.hostLView,this.templateTNode,new ju(this.lContainer,t,e),{dehydratedView:i});return this.operationsCounter?.recordCreate(),r}destroy(e){Qa(e[Ne],e),this.operationsCounter?.recordDestroy()}updateValue(e,t){this.getLView(e)[kt].$implicit=t}reset(){this.needsIndexUpdate=!1,this.operationsCounter?.reset()}updateIndexes(){if(this.needsIndexUpdate)for(let e=0;e<this.length;e++)this.getLView(e)[kt].$index=e}getLView(e){return cw(this.lContainer,e)}};function $n(n){let e=Ue(null),t=Ai();try{let i=Ze(),r=i[Ne],s=i[t],o=t+1,a=Va(i,o);if(s.liveCollection===void 0){let c=Xu(r,o);s.liveCollection=new qu(a,i,c)}else s.liveCollection.reset();let l=s.liveCollection;if(aw(l,n,s.trackByFn),l.updateIndexes(),s.hasEmptyBlock){let c=co(),u=l.length===0;if(Di(i,c,u)){let d=t+2,f=Va(i,d);if(u){let h=Xu(r,d),g=io(f,h.tView.ssrId),x=rl(i,h,void 0,{dehydratedView:g});sl(f,x,0,no(h,g))}else Dv(f,0)}}}finally{Ue(e)}}function Va(n,e){return n[e]}function lw(n,e){return to(n,e)}function cw(n,e){return Cv(n,e)}function Xu(n,e){return ad(n,e)}function uw(n,e,t,i,r,s){let o=e.consts,a=Yr(o,r),l=nl(e,n,2,i,a);return vv(e,t,l,Yr(o,s)),l.attrs!==null&&Ru(l,l.attrs,!1),l.mergedAttrs!==null&&Ru(l,l.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,l),l}function N(n,e,t,i){let r=Ze(),s=Wt(),o=Dn+n,a=r[Ft],l=s.firstCreatePass?uw(o,s,r,e,t,i):s.data[o],c=dw(s,r,l,a,e,n);r[o]=c;let u=sd(l);return lo(l,!0),av(a,c,l),!Vb(l)&&fd()&&Sd(s,r,c,l),Wx()===0&&sr(c,r),jx(),u&&(hv(s,r,l),fv(s,l,r)),i!==null&&pv(r,l),N}function P(){let n=Qt();yg()?Kx():(n=n.parent,lo(n,!1));let e=n;Xx(e)&&Yx(),$x();let t=Wt();return t.firstCreatePass&&(pd(t,n),og(n)&&t.queries.elementEnd(n)),e.classesWithoutHost!=null&&uM(e)&&Nm(t,e,Ze(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&dM(e)&&Nm(t,e,Ze(),e.stylesWithoutHost,!1),P}function yn(n,e,t,i){return N(n,e,t,i),P(),yn}var dw=(n,e,t,i,r,s)=>(hd(!0),ev(i,r,oM()));function qn(){return Ze()}var Ba="en-US";var fw=Ba;function hw(n){typeof n=="string"&&(fw=n.toLowerCase().replace(/_/g,"-"))}var pw=(n,e,t)=>{};function jt(n,e,t,i){let r=Ze(),s=Wt(),o=Qt();return Hv(s,r,r[Ft],o,n,e,i),jt}function mw(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[Ea],l=r[s+2];return a.length>l?a[l]:null}typeof o=="string"&&(s+=2)}return null}function Hv(n,e,t,i,r,s,o){let a=sd(i),c=n.firstCreatePass&&bv(n),u=e[kt],d=Ev(e),f=!0;if(i.type&3||o){let x=An(i,e),m=o?o(x):x,p=d.length,S=o?w=>o(Gn(w[i.index])):i.index,E=null;if(!o&&a&&(E=mw(n,e,r,i.index)),E!==null){let w=E.__ngLastListenerFn__||E;w.__ngNextListenerFn__=s,E.__ngLastListenerFn__=s,f=!1}else{s=km(i,e,u,s),pw(x,r,s);let w=t.listen(m,r,s);d.push(s,w),c&&c.push(r,S,p,p+1)}}else s=km(i,e,u,s);let h=i.outputs,g;if(f&&h!==null&&(g=h[r])){let x=g.length;if(x)for(let m=0;m<x;m+=2){let p=g[m],S=g[m+1],L=e[p][S].subscribe(s),T=d.length;d.push(s,L),c&&c.push(r,i.index,T,-(T+1))}}}function Lm(n,e,t,i){let r=Ue(null);try{return Vn(6,e,t),t(i)!==!1}catch(s){return wv(n,s),!1}finally{Vn(7,e,t),Ue(r)}}function km(n,e,t,i){return function r(s){if(s===Function)return i;let o=n.componentOffset>-1?Ti(n.index,e):e;Id(o,5);let a=Lm(e,t,i,s),l=r.__ngNextListenerFn__;for(;l;)a=Lm(e,t,l,s)&&a,l=l.__ngNextListenerFn__;return a}}function Ke(n=1){return sM(n)}function zv(n,e,t){Sb(n,e,t)}function Gv(n){let e=Ze(),t=Wt(),i=Mg();cd(i+1);let r=Fd(t,i);if(n.dirty&&Bx(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let s=Tb(e,i);n.reset(s,NM),n.notifyOnChanges()}return!0}return!1}function Wv(){return bb(Ze(),Mg())}function B(n,e=""){let t=Ze(),i=Wt(),r=n+Dn,s=i.firstCreatePass?nl(i,r,1,e,null):i.data[r],o=gw(i,t,s,e,n);t[r]=o,fd()&&Sd(i,t,o,s),lo(s,!1)}var gw=(n,e,t,i,r)=>(hd(!0),qM(e[Ft],i));function St(n){return lr("",n,""),St}function lr(n,e,t){let i=Ze(),r=Wb(i,n,e,t);return r!==ri&&Sv(i,Ai(),r),lr}function Ld(n,e,t,i,r){let s=Ze(),o=jb(s,n,e,t,i,r);return o!==ri&&Sv(s,Ai(),o),Ld}function $t(n,e,t){Vv(e)&&(e=e());let i=Ze(),r=co();if(Di(i,r,e)){let s=Wt(),o=Dg();gv(s,o,i,n,e,i[Ft],t,!1)}return $t}function en(n,e){let t=Vv(n);return t&&n.set(e),t}function qt(n,e){let t=Ze(),i=Wt(),r=Qt();return Hv(i,t,t[Ft],r,n,e),qt}function vw(n,e,t){let i=Wt();if(i.firstCreatePass){let r=Ci(n);Yu(t,i.data,i.blueprint,r,!0),Yu(e,i.data,i.blueprint,r,!1)}}function Yu(n,e,t,i,r){if(n=Gt(n),Array.isArray(n))for(let s=0;s<n.length;s++)Yu(n[s],e,t,i,r);else{let s=Wt(),o=Ze(),a=Qt(),l=$r(n)?n:Gt(n.provide),c=ig(n),u=a.providerIndexes&1048575,d=a.directiveStart,f=a.providerIndexes>>20;if($r(n)||!n.multi){let h=new ir(c,r,gt),g=iu(l,e,r?u:u+f,d);g===-1?(gu(Da(a,o),s,l),nu(s,n,e.length),e.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(h),o.push(h)):(t[g]=h,o[g]=h)}else{let h=iu(l,e,u+f,d),g=iu(l,e,u,u+f),x=h>=0&&t[h],m=g>=0&&t[g];if(r&&!m||!r&&!x){gu(Da(a,o),s,l);let p=xw(r?_w:yw,t.length,r,i,c);!r&&m&&(t[g].providerFactory=p),nu(s,n,e.length,0),e.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(p),o.push(p)}else{let p=jv(t[r?g:h],c,!r&&i);nu(s,n,h>-1?h:g,p)}!r&&i&&m&&t[g].componentProviders++}}}function nu(n,e,t,i){let r=$r(e),s=Cx(e);if(r||s){let l=(s?Gt(e.useClass):e).prototype.ngOnDestroy;if(l){let c=n.destroyHooks||(n.destroyHooks=[]);if(!r&&e.multi){let u=c.indexOf(t);u===-1?c.push(t,[i,l]):c[u+1].push(i,l)}else c.push(t,l)}}}function jv(n,e,t){return t&&n.componentProviders++,n.multi.push(e)-1}function iu(n,e,t,i){for(let r=t;r<i;r++)if(e[r]===n)return r;return-1}function yw(n,e,t,i){return Zu(this.multi,[])}function _w(n,e,t,i){let r=this.multi,s;if(this.providerFactory){let o=this.providerFactory.componentProviders,a=rr(t,t[Ne],this.providerFactory.index,i);s=a.slice(0,o),Zu(r,s);for(let l=o;l<a.length;l++)s.push(a[l])}else s=[],Zu(r,s);return s}function Zu(n,e){for(let t=0;t<n.length;t++){let i=n[t];e.push(i())}return e}function xw(n,e,t,i,r){let s=new ir(n,t,gt);return s.multi=[],s.index=e,s.componentProviders=0,jv(s,r,i&&!t),s}function kd(n,e=[]){return t=>{t.providersResolver=(i,r)=>vw(i,r?r(n):n,e)}}var Mw=(()=>{class n{constructor(t){this._injector=t,this.cachedInjectors=new Map}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=eg(!1,t.type),r=i.length>0?kb([i],this._injector,`Standalone[${t.type.name}]`):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static{this.\u0275prov=wt({token:n,providedIn:"environment",factory:()=>new n(ut(Si))})}}return n})();function ls(n){ss("NgStandalone"),n.getStandaloneInjector=e=>e.get(Mw).getOrCreateStandaloneInjector(n)}var $v=new Be("");function fo(n){return!!n&&typeof n.then=="function"}function qv(n){return!!n&&typeof n.subscribe=="function"}var Ew=new Be(""),Xv=(()=>{class n{constructor(){this.initialized=!1,this.done=!1,this.donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i}),this.appInits=it(Ew,{optional:!0})??[]}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=r();if(fo(s))t.push(s);else if(qv(s)){let o=new Promise((a,l)=>{s.subscribe({complete:a,error:l})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=wt({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})(),bw=new Be("");function ww(){Dp(()=>{throw new Ge(600,!1)})}function Sw(n){return n.isBoundToModule}var Cw=10;function Dw(n,e,t){try{let i=t();return fo(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n.handleError(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n.handleError(i)),i}}var ho=(()=>{class n{constructor(){this._bootstrapListeners=[],this._runningTick=!1,this._destroyed=!1,this._destroyListeners=[],this._views=[],this.internalErrorHandler=it(IM),this.afterRenderManager=it(Gb),this.zonelessEnabled=it(Nd),this.dirtyFlags=0,this.deferredDirtyFlags=0,this.externalTestViews=new Set,this.beforeRender=new Ln,this.afterTick=new Ln,this.componentTypes=[],this.components=[],this.isStable=it(Za).hasPendingTasks.pipe(Ki(t=>!t)),this._injector=it(Si)}get allViews(){return[...this.externalTestViews.keys(),...this._views]}get destroyed(){return this._destroyed}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}get injector(){return this._injector}bootstrap(t,i){let r=t instanceof Fa;if(!this._injector.get(Xv).done){let f=!r&&xx(t),h=!1;throw new Ge(405,h)}let o;r?o=t:o=this._injector.get(ro).resolveComponentFactory(t),this.componentTypes.push(o.componentType);let a=Sw(o)?void 0:this._injector.get(Qr),l=i||o.selector,c=o.create(Zr.NULL,[],l,a),u=c.location.nativeElement,d=c.injector.get($v,null);return d?.registerApplication(u),c.onDestroy(()=>{this.detachView(c.hostView),ma(this.components,c),d?.unregisterApplication(u)}),this._loadComponent(c),c}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){if(this._runningTick)throw new Ge(101,!1);let t=Ue(null);try{this._runningTick=!0,this.synchronize()}catch(i){this.internalErrorHandler(i)}finally{this._runningTick=!1,Ue(t),this.afterTick.next()}}synchronize(){let t=null;this._injector.destroyed||(t=this._injector.get(Jr,null,{optional:!0})),this.dirtyFlags|=this.deferredDirtyFlags,this.deferredDirtyFlags=0;let i=0;for(;this.dirtyFlags!==0&&i++<Cw;)this.synchronizeOnce(t)}synchronizeOnce(t){if(this.dirtyFlags|=this.deferredDirtyFlags,this.deferredDirtyFlags=0,this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8,this.beforeRender.next(i);for(let{_lView:r,notifyErrorHandler:s}of this._views)Tw(r,s,i,this.zonelessEnabled);if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&7)return}else t?.begin?.(),t?.end?.();this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>Xa(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;ma(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView),this.tick(),this.components.push(t);let i=this._injector.get(bw,[]);[...this._bootstrapListeners,...i].forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._bootstrapListeners=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>ma(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new Ge(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}warnIfDestroyed(){}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=wt({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})();function ma(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function Tw(n,e,t,i){if(!t&&!Xa(n))return;Iv(n,e,t&&!i?0:1)}var Aw=(()=>{class n{constructor(){this.zone=it(_t),this.changeDetectionScheduler=it(Kr),this.applicationRef=it(ho)}initialize(){this._onMicrotaskEmptySubscription||(this._onMicrotaskEmptySubscription=this.zone.onMicrotaskEmpty.subscribe({next:()=>{this.changeDetectionScheduler.runningTick||this.zone.run(()=>{this.applicationRef.tick()})}}))}ngOnDestroy(){this._onMicrotaskEmptySubscription?.unsubscribe()}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=wt({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})();function Iw({ngZoneFactory:n,ignoreChangesOutsideZone:e,scheduleInRootZone:t}){return n??=()=>new _t(yt(ht({},Rw()),{scheduleInRootZone:t})),[{provide:_t,useFactory:n},{provide:Ks,multi:!0,useFactory:()=>{let i=it(Aw,{optional:!0});return()=>i.initialize()}},{provide:Ks,multi:!0,useFactory:()=>{let i=it(Nw);return()=>{i.initialize()}}},e===!0?{provide:Fv,useValue:!0}:[],{provide:Ov,useValue:t??Bg}]}function Rw(n){return{enableLongStackTrace:!1,shouldCoalesceEventChangeDetection:n?.eventCoalescing??!1,shouldCoalesceRunChangeDetection:n?.runCoalescing??!1}}var Nw=(()=>{class n{constructor(){this.subscription=new zt,this.initialized=!1,this.zone=it(_t),this.pendingTasks=it(Za)}initialize(){if(this.initialized)return;this.initialized=!0;let t=null;!this.zone.isStable&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(t=this.pendingTasks.add()),this.zone.runOutsideAngular(()=>{this.subscription.add(this.zone.onStable.subscribe(()=>{_t.assertNotInAngularZone(),queueMicrotask(()=>{t!==null&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(this.pendingTasks.remove(t),t=null)})}))}),this.subscription.add(this.zone.onUnstable.subscribe(()=>{_t.assertInAngularZone(),t??=this.pendingTasks.add()}))}ngOnDestroy(){this.subscription.unsubscribe()}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=wt({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})();var Pw=(()=>{class n{constructor(){this.appRef=it(ho),this.taskService=it(Za),this.ngZone=it(_t),this.zonelessEnabled=it(Nd),this.disableScheduling=it(Fv,{optional:!0})??!1,this.zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run,this.schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}],this.subscriptions=new zt,this.angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Aa):null,this.scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(it(Ov,{optional:!0})??!1),this.cancelScheduledCallback=null,this.useMicrotaskScheduler=!1,this.runningTick=!1,this.pendingRenderTaskId=null,this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{this.runningTick||this.cleanup()})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()})),this.disableScheduling||=!this.zonelessEnabled&&(this.ngZone instanceof Mu||!this.zoneIsDefined)}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 7:{this.appRef.deferredDirtyFlags|=8;break}case 9:case 8:case 6:case 10:default:this.appRef.dirtyFlags|=8}if(!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?_m:zg;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.disableScheduling||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Aa+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){throw this.taskService.remove(t),i}finally{this.cleanup()}this.useMicrotaskScheduler=!0,_m(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=wt({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})();function Fw(){return typeof $localize<"u"&&$localize.locale||Ba}var Ud=new Be("",{providedIn:"root",factory:()=>it(Ud,Ve.Optional|Ve.SkipSelf)||Fw()});var Ku=new Be("");function ua(n){return!n.moduleRef}function Ow(n){let e=ua(n)?n.r3Injector:n.moduleRef.injector,t=e.get(_t);return t.run(()=>{ua(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(ei,null),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:s=>{i.handleError(s)}})}),ua(n)){let s=()=>e.destroy(),o=n.platformInjector.get(Ku);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(Ku);o.add(s),n.moduleRef.onDestroy(()=>{ma(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return Dw(i,t,()=>{let s=e.get(Xv);return s.runInitializers(),s.donePromise.then(()=>{let o=e.get(Ud,Ba);if(hw(o||Ba),ua(n)){let a=e.get(ho);return n.rootComponent!==void 0&&a.bootstrap(n.rootComponent),a}else return Lw(n.moduleRef,n.allPlatformModules),n.moduleRef})})})}function Lw(n,e){let t=n.injector.get(ho);if(n._bootstrapComponents.length>0)n._bootstrapComponents.forEach(i=>t.bootstrap(i));else if(n.instance.ngDoBootstrap)n.instance.ngDoBootstrap(t);else throw new Ge(-403,!1);e.push(n)}var ga=null,Yv=new Be("");function kw(n=[],e){return Zr.create({name:e,providers:[{provide:ja,useValue:"platform"},{provide:Ku,useValue:new Set([()=>ga=null])},...n]})}function Uw(n=[]){if(ga)return ga;let e=kw(n);return e.get(Yv,!1)||(ga=e),ww(),Vw(e),e}function Vw(n){n.get(_d,null)?.forEach(t=>t())}var Vd=(()=>{class n{static{this.__NG_ELEMENT_ID__=Bw}}return n})();function Bw(n){return Hw(Qt(),Ze(),(n&16)===16)}function Hw(n,e,t){if(qa(n)&&!t){let i=Ti(n.index,e);return new or(i,i)}else if(n.type&175){let i=e[Cn];return new or(i,e)}return null}function Zv(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;try{let s=r?.injector??Uw(i);if(s.get(Yv,!1)===!0&&!n.platformRef)throw new Ge(401,!1);let o=[Iw({}),{provide:Kr,useExisting:Pw},...t||[]],a=new La({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return Ow({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}}function Bd(n){return typeof n=="boolean"?n:n!=null&&n!=="false"}function al(n,e){ss("NgSignals");let t=wp(n);return e?.equal&&(t[Jn].equal=e.equal),t}function Ri(n){let e=Ue(null);try{return n()}finally{Ue(e)}}var Jv=null;function us(){return Jv}function Qv(n){Jv??=n}var ll=class{};var Ni=new Be("");function ey(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var ty="browser",zw="server";function Hd(n){return n===zw}var cl=class{};var Wd=class extends ll{constructor(){super(...arguments),this.supportsDOMEvents=!0}},jd=class n extends Wd{static makeCurrent(){Qv(new n)}onAndCancel(e,t,i){return e.addEventListener(t,i),()=>{e.removeEventListener(t,i)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=Ww();return t==null?null:jw(t)}resetBaseElement(){po=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return ey(document.cookie,e)}},po=null;function Ww(){return po=po||document.querySelector("base"),po?po.getAttribute("href"):null}function jw(n){return new URL(n,document.baseURI).pathname}var $w=(()=>{class n{build(){return new XMLHttpRequest}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=wt({token:n,factory:n.\u0275fac})}}return n})(),$d=new Be(""),sy=(()=>{class n{constructor(t,i){this._zone=i,this._eventNameToPlugin=new Map,t.forEach(r=>{r.manager=this}),this._plugins=t.slice().reverse()}addEventListener(t,i,r){return this._findPluginFor(i).addEventListener(t,i,r)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new Ge(5101,!1);return this._eventNameToPlugin.set(t,i),i}static{this.\u0275fac=function(i){return new(i||n)(ut($d),ut(_t))}}static{this.\u0275prov=wt({token:n,factory:n.\u0275fac})}}return n})(),ul=class{constructor(e){this._doc=e}},zd="ng-app-id",oy=(()=>{class n{constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,this.platformId=s,this.styleRef=new Map,this.hostNodes=new Set,this.styleNodesInDOM=this.collectServerRenderedStyles(),this.platformIsServer=Hd(s),this.resetHostNodes()}addStyles(t){for(let i of t)this.changeUsageCount(i,1)===1&&this.onStyleAdded(i)}removeStyles(t){for(let i of t)this.changeUsageCount(i,-1)<=0&&this.onStyleRemoved(i)}ngOnDestroy(){let t=this.styleNodesInDOM;t&&(t.forEach(i=>i.remove()),t.clear());for(let i of this.getAllStyles())this.onStyleRemoved(i);this.resetHostNodes()}addHost(t){this.hostNodes.add(t);for(let i of this.getAllStyles())this.addStyleToHost(t,i)}removeHost(t){this.hostNodes.delete(t)}getAllStyles(){return this.styleRef.keys()}onStyleAdded(t){for(let i of this.hostNodes)this.addStyleToHost(i,t)}onStyleRemoved(t){let i=this.styleRef;i.get(t)?.elements?.forEach(r=>r.remove()),i.delete(t)}collectServerRenderedStyles(){let t=this.doc.head?.querySelectorAll(`style[${zd}="${this.appId}"]`);if(t?.length){let i=new Map;return t.forEach(r=>{r.textContent!=null&&i.set(r.textContent,r)}),i}return null}changeUsageCount(t,i){let r=this.styleRef;if(r.has(t)){let s=r.get(t);return s.usage+=i,s.usage}return r.set(t,{usage:i,elements:[]}),i}getStyleElement(t,i){let r=this.styleNodesInDOM,s=r?.get(i);if(s?.parentNode===t)return r.delete(i),s.removeAttribute(zd),s;{let o=this.doc.createElement("style");return this.nonce&&o.setAttribute("nonce",this.nonce),o.textContent=i,this.platformIsServer&&o.setAttribute(zd,this.appId),t.appendChild(o),o}}addStyleToHost(t,i){let r=this.getStyleElement(t,i),s=this.styleRef,o=s.get(i)?.elements;o?o.push(r):s.set(i,{elements:[r],usage:1})}resetHostNodes(){let t=this.hostNodes;t.clear(),t.add(this.doc.head)}static{this.\u0275fac=function(i){return new(i||n)(ut(Ni),ut(yd),ut(xd,8),ut(rs))}}static{this.\u0275prov=wt({token:n,factory:n.\u0275fac})}}return n})(),Gd={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Xd=/%COMP%/g,ay="%COMP%",qw=`_nghost-${ay}`,Xw=`_ngcontent-${ay}`,Yw=!0,Zw=new Be("",{providedIn:"root",factory:()=>Yw});function Kw(n){return Xw.replace(Xd,n)}function Jw(n){return qw.replace(Xd,n)}function ly(n,e){return e.map(t=>t.replace(Xd,n))}var ny=(()=>{class n{constructor(t,i,r,s,o,a,l,c=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.platformId=a,this.ngZone=l,this.nonce=c,this.rendererByCompId=new Map,this.platformIsServer=Hd(a),this.defaultRenderer=new mo(t,o,l,this.platformIsServer)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;this.platformIsServer&&i.encapsulation===Hn.ShadowDom&&(i=yt(ht({},i),{encapsulation:Hn.Emulated}));let r=this.getOrCreateRenderer(t,i);return r instanceof dl?r.applyToHost(t):r instanceof go&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.platformIsServer;switch(i.encapsulation){case Hn.Emulated:s=new dl(l,c,i,this.appId,u,o,a,d);break;case Hn.ShadowDom:return new qd(l,c,t,i,o,a,this.nonce,d);default:s=new go(l,c,i,u,o,a,d);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}static{this.\u0275fac=function(i){return new(i||n)(ut(sy),ut(oy),ut(yd),ut(Zw),ut(Ni),ut(rs),ut(_t),ut(xd))}}static{this.\u0275prov=wt({token:n,factory:n.\u0275fac})}}return n})(),mo=class{constructor(e,t,i,r){this.eventManager=e,this.doc=t,this.ngZone=i,this.platformIsServer=r,this.data=Object.create(null),this.throwOnSyntheticProps=!0,this.destroyNode=null}destroy(){}createElement(e,t){return t?this.doc.createElementNS(Gd[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(iy(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(iy(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new Ge(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=Gd[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=Gd[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(ti.DashCase|ti.Important)?e.style.setProperty(t,i,r&ti.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&ti.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i){if(typeof e=="string"&&(e=us().getGlobalEventTarget(this.doc,e),!e))throw new Error(`Unsupported event target ${e} for event ${t}`);return this.eventManager.addEventListener(e,t,this.decoratePreventDefault(i))}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;(this.platformIsServer?this.ngZone.runGuarded(()=>e(t)):e(t))===!1&&t.preventDefault()}}};function iy(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var qd=class extends mo{constructor(e,t,i,r,s,o,a,l){super(e,s,o,l),this.sharedStylesHost=t,this.hostEl=i,this.shadowRoot=i.attachShadow({mode:"open"}),this.sharedStylesHost.addHost(this.shadowRoot);let c=ly(r.id,r.styles);for(let u of c){let d=document.createElement("style");a&&d.setAttribute("nonce",a),d.textContent=u,this.shadowRoot.appendChild(d)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost.removeHost(this.shadowRoot)}},go=class extends mo{constructor(e,t,i,r,s,o,a,l){super(e,s,o,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r,this.styles=l?ly(l,i.styles):i.styles}applyStyles(){this.sharedStylesHost.addStyles(this.styles)}destroy(){this.removeStylesOnCompDestroy&&this.sharedStylesHost.removeStyles(this.styles)}},dl=class extends go{constructor(e,t,i,r,s,o,a,l){let c=r+"-"+i.id;super(e,t,i,s,o,a,l,c),this.contentAttr=Kw(c),this.hostAttr=Jw(c)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}},Qw=(()=>{class n extends ul{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r){return t.addEventListener(i,r,!1),()=>this.removeEventListener(t,i,r)}removeEventListener(t,i,r){return t.removeEventListener(i,r)}static{this.\u0275fac=function(i){return new(i||n)(ut(Ni))}}static{this.\u0275prov=wt({token:n,factory:n.\u0275fac})}}return n})(),ry=["alt","control","meta","shift"],eS={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},tS={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},nS=(()=>{class n extends ul{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r){let s=n.parseEventName(i),o=n.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>us().onAndCancel(t,s.domEventName,o))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),ry.forEach(c=>{let u=i.indexOf(c);u>-1&&(i.splice(u,1),o+=c+".")}),o+=s,i.length!=0||s.length===0)return null;let l={};return l.domEventName=r,l.fullKey=o,l}static matchEventFullKeyCode(t,i){let r=eS[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),ry.forEach(o=>{if(o!==r){let a=tS[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static{this.\u0275fac=function(i){return new(i||n)(ut(Ni))}}static{this.\u0275prov=wt({token:n,factory:n.\u0275fac})}}return n})();function cy(n,e,t){return Zv(ht({rootComponent:n,platformRef:t?.platformRef},iS(e)))}function iS(n){return{appProviders:[...lS,...n?.providers??[]],platformProviders:aS}}function rS(){jd.makeCurrent()}function sS(){return new ei}function oS(){return Yg(document),document}var aS=[{provide:rs,useValue:ty},{provide:_d,useValue:rS,multi:!0},{provide:Ni,useFactory:oS,deps:[]}];var lS=[{provide:ja,useValue:"root"},{provide:ei,useFactory:sS,deps:[]},{provide:$d,useClass:Qw,multi:!0,deps:[Ni,_t,rs]},{provide:$d,useClass:nS,multi:!0,deps:[Ni]},ny,oy,sy,{provide:Jr,useExisting:ny},{provide:cl,useClass:$w,deps:[]},[]];var zh="169";var cS=0,uy=1,uS=2;var c_=1,dS=2,ui=3,zi=0,sn=1,fi=2,Bi=0,Cs=1,dy=2,fy=3,hy=4,fS=5,gr=100,hS=101,pS=102,mS=103,gS=104,vS=200,yS=201,_S=202,xS=203,Tf=204,Af=205,MS=206,ES=207,bS=208,wS=209,SS=210,CS=211,DS=212,TS=213,AS=214,If=0,Rf=1,Nf=2,Is=3,Pf=4,Ff=5,Of=6,Lf=7,u_=0,IS=1,RS=2,Hi=0,NS=1,PS=2,FS=3,OS=4,LS=5,kS=6,US=7;var py=300,Rs=301,Ns=302,kf=303,Uf=304,uc=306,Vf=1e3,_r=1001,Bf=1002,xn=1003,VS=1004;var fl=1005;var Pn=1006,Yd=1007;var xr=1008;var vi=1009,d_=1010,f_=1011,So=1012,Gh=1013,Mr=1014,hi=1015,Ao=1016,Wh=1017,jh=1018,Ps=1020,h_=35902,p_=1021,m_=1022,Fn=1023,g_=1024,v_=1025,Ds=1026,Fs=1027,y_=1028,$h=1029,__=1030,qh=1031;var Xh=1033,Fl=33776,Ol=33777,Ll=33778,kl=33779,Hf=35840,zf=35841,Gf=35842,Wf=35843,jf=36196,$f=37492,qf=37496,Xf=37808,Yf=37809,Zf=37810,Kf=37811,Jf=37812,Qf=37813,eh=37814,th=37815,nh=37816,ih=37817,rh=37818,sh=37819,oh=37820,ah=37821,Ul=36492,lh=36494,ch=36495,x_=36283,uh=36284,dh=36285,fh=36286;var Bl=2300,hh=2301,Zd=2302,my=2400,gy=2401,vy=2402;var BS=3200,HS=3201;var M_=0,zS=1,Vi="",Xn="srgb",$i="srgb-linear",Yh="display-p3",dc="display-p3-linear",Hl="linear",mt="srgb",zl="rec709",Gl="p3";var ds=7680;var yy=519,GS=512,WS=513,jS=514,E_=515,$S=516,qS=517,XS=518,YS=519,_y=35044;var xy="300 es",pi=2e3,Wl=2001,Gi=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){if(this._listeners===void 0)return!1;let i=this._listeners;return i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){if(this._listeners===void 0)return;let r=this._listeners[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){if(this._listeners===void 0)return;let i=this._listeners[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},Bt=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"];var Kd=Math.PI/180,ph=180/Math.PI;function Io(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(Bt[n&255]+Bt[n>>8&255]+Bt[n>>16&255]+Bt[n>>24&255]+"-"+Bt[e&255]+Bt[e>>8&255]+"-"+Bt[e>>16&15|64]+Bt[e>>24&255]+"-"+Bt[t&63|128]+Bt[t>>8&255]+"-"+Bt[t>>16&255]+Bt[t>>24&255]+Bt[i&255]+Bt[i>>8&255]+Bt[i>>16&255]+Bt[i>>24&255]).toLowerCase()}function nn(n,e,t){return Math.max(e,Math.min(t,n))}function ZS(n,e){return(n%e+e)%e}function Jd(n,e,t){return(1-t)*n+t*e}function vo(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function tn(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var st=class n{constructor(e=0,t=0){n.prototype.isVector2=!0,this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(nn(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},Le=class n{constructor(e,t,i,r,s,o,a,l,c){n.prototype.isMatrix3=!0,this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,l,c)}set(e,t,i,r,s,o,a,l,c){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=l,u[6]=i,u[7]=o,u[8]=c,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],l=i[6],c=i[1],u=i[4],d=i[7],f=i[2],h=i[5],g=i[8],x=r[0],m=r[3],p=r[6],S=r[1],E=r[4],w=r[7],L=r[2],T=r[5],C=r[8];return s[0]=o*x+a*S+l*L,s[3]=o*m+a*E+l*T,s[6]=o*p+a*w+l*C,s[1]=c*x+u*S+d*L,s[4]=c*m+u*E+d*T,s[7]=c*p+u*w+d*C,s[2]=f*x+h*S+g*L,s[5]=f*m+h*E+g*T,s[8]=f*p+h*w+g*C,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],l=e[6],c=e[7],u=e[8];return t*o*u-t*a*c-i*s*u+i*a*l+r*s*c-r*o*l}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],l=e[6],c=e[7],u=e[8],d=u*o-a*c,f=a*l-u*s,h=c*s-o*l,g=t*d+i*f+r*h;if(g===0)return this.set(0,0,0,0,0,0,0,0,0);let x=1/g;return e[0]=d*x,e[1]=(r*c-u*i)*x,e[2]=(a*i-r*o)*x,e[3]=f*x,e[4]=(u*t-r*l)*x,e[5]=(r*s-a*t)*x,e[6]=h*x,e[7]=(i*l-c*t)*x,e[8]=(o*t-i*s)*x,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let l=Math.cos(s),c=Math.sin(s);return this.set(i*l,i*c,-i*(l*o+c*a)+o+e,-r*c,r*l,-r*(-c*o+l*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(Qd.makeScale(e,t)),this}rotate(e){return this.premultiply(Qd.makeRotation(-e)),this}translate(e,t){return this.premultiply(Qd.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},Qd=new Le;function b_(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function jl(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function KS(){let n=jl("canvas");return n.style.display="block",n}var My={};function Vl(n){n in My||(My[n]=!0,console.warn(n))}function JS(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}function QS(n){let e=n.elements;e[2]=.5*e[2]+.5*e[3],e[6]=.5*e[6]+.5*e[7],e[10]=.5*e[10]+.5*e[11],e[14]=.5*e[14]+.5*e[15]}function eC(n){let e=n.elements;e[11]===-1?(e[10]=-e[10]-1,e[14]=-e[14]):(e[10]=-e[10],e[14]=-e[14]+1)}var Ey=new Le().set(.8224621,.177538,0,.0331941,.9668058,0,.0170827,.0723974,.9105199),by=new Le().set(1.2249401,-.2249404,0,-.0420569,1.0420571,0,-.0196376,-.0786361,1.0982735),yo={[$i]:{transfer:Hl,primaries:zl,luminanceCoefficients:[.2126,.7152,.0722],toReference:n=>n,fromReference:n=>n},[Xn]:{transfer:mt,primaries:zl,luminanceCoefficients:[.2126,.7152,.0722],toReference:n=>n.convertSRGBToLinear(),fromReference:n=>n.convertLinearToSRGB()},[dc]:{transfer:Hl,primaries:Gl,luminanceCoefficients:[.2289,.6917,.0793],toReference:n=>n.applyMatrix3(by),fromReference:n=>n.applyMatrix3(Ey)},[Yh]:{transfer:mt,primaries:Gl,luminanceCoefficients:[.2289,.6917,.0793],toReference:n=>n.convertSRGBToLinear().applyMatrix3(by),fromReference:n=>n.applyMatrix3(Ey).convertLinearToSRGB()}},tC=new Set([$i,dc]),rt={enabled:!0,_workingColorSpace:$i,get workingColorSpace(){return this._workingColorSpace},set workingColorSpace(n){if(!tC.has(n))throw new Error(`Unsupported working color space, "${n}".`);this._workingColorSpace=n},convert:function(n,e,t){if(this.enabled===!1||e===t||!e||!t)return n;let i=yo[e].toReference,r=yo[t].fromReference;return r(i(n))},fromWorkingColorSpace:function(n,e){return this.convert(n,this._workingColorSpace,e)},toWorkingColorSpace:function(n,e){return this.convert(n,e,this._workingColorSpace)},getPrimaries:function(n){return yo[n].primaries},getTransfer:function(n){return n===Vi?Hl:yo[n].transfer},getLuminanceCoefficients:function(n,e=this._workingColorSpace){return n.fromArray(yo[e].luminanceCoefficients)}};function Ts(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function ef(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var fs,mh=class{static getDataURL(e){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let t;if(e instanceof HTMLCanvasElement)t=e;else{fs===void 0&&(fs=jl("canvas")),fs.width=e.width,fs.height=e.height;let i=fs.getContext("2d");e instanceof ImageData?i.putImageData(e,0,0):i.drawImage(e,0,0,e.width,e.height),t=fs}return t.width>2048||t.height>2048?(console.warn("THREE.ImageUtils.getDataURL: Image converted to jpg for performance reasons",e),t.toDataURL("image/jpeg",.6)):t.toDataURL("image/png")}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=jl("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=Ts(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(Ts(t[i]/255)*255):t[i]=Ts(t[i]);return{data:t,width:e.width,height:e.height}}else return console.warn("THREE.ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},nC=0,$l=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:nC++}),this.uuid=Io(),this.data=e,this.dataReady=!0,this.version=0}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(tf(r[o].image)):s.push(tf(r[o]))}else s=tf(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function tf(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?mh.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(console.warn("THREE.Texture: Unable to serialize Texture."),{})}var iC=0,Tr=(()=>{class n extends Gi{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=_r,s=_r,o=Pn,a=xr,l=Fn,c=vi,u=n.DEFAULT_ANISOTROPY,d=Vi){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:iC++}),this.uuid=Io(),this.name="",this.source=new $l(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=l,this.internalFormat=null,this.type=c,this.offset=new st(0,0),this.repeat=new st(1,1),this.center=new st(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new Le,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=d,this.userData={},this.version=0,this.onUpdate=null,this.isRenderTargetTexture=!1,this.pmremVersion=0}get image(){return this.source.data}set image(t=null){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.6,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==py)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case Vf:t.x=t.x-Math.floor(t.x);break;case _r:t.x=t.x<0?0:1;break;case Bf:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case Vf:t.y=t.y-Math.floor(t.y);break;case _r:t.y=t.y<0?0:1;break;case Bf:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=py,n.DEFAULT_ANISOTROPY=1,n})(),Et=class n{constructor(e=0,t=0,i=0,r=1){n.prototype.isVector4=!0,this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,l=e.elements,c=l[0],u=l[4],d=l[8],f=l[1],h=l[5],g=l[9],x=l[2],m=l[6],p=l[10];if(Math.abs(u-f)<.01&&Math.abs(d-x)<.01&&Math.abs(g-m)<.01){if(Math.abs(u+f)<.1&&Math.abs(d+x)<.1&&Math.abs(g+m)<.1&&Math.abs(c+h+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let E=(c+1)/2,w=(h+1)/2,L=(p+1)/2,T=(u+f)/4,C=(d+x)/4,U=(g+m)/4;return E>w&&E>L?E<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(E),r=T/i,s=C/i):w>L?w<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(w),i=T/r,s=U/r):L<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(L),i=C/s,r=U/s),this.set(i,r,s,t),this}let S=Math.sqrt((m-g)*(m-g)+(d-x)*(d-x)+(f-u)*(f-u));return Math.abs(S)<.001&&(S=1),this.x=(m-g)/S,this.y=(d-x)/S,this.z=(f-u)/S,this.w=Math.acos((c+h+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this.z=Math.max(e.z,Math.min(t.z,this.z)),this.w=Math.max(e.w,Math.min(t.w,this.w)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this.z=Math.max(e,Math.min(t,this.z)),this.w=Math.max(e,Math.min(t,this.w)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},gh=class extends Gi{constructor(e=1,t=1,i={}){super(),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=1,this.scissor=new Et(0,0,e,t),this.scissorTest=!1,this.viewport=new Et(0,0,e,t);let r={width:e,height:t,depth:1};i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:Pn,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1},i);let s=new Tr(r,i.mapping,i.wrapS,i.wrapT,i.magFilter,i.minFilter,i.format,i.type,i.anisotropy,i.colorSpace);s.flipY=!1,s.generateMipmaps=i.generateMipmaps,s.internalFormat=i.internalFormat,this.textures=[];let o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0;this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this.depthTexture=i.depthTexture,this.samples=i.samples}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i;this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let i=0,r=e.textures.length;i<r;i++)this.textures[i]=e.textures[i].clone(),this.textures[i].isRenderTargetTexture=!0;let t=Object.assign({},e.texture.image);return this.texture.source=new $l(t),this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this}dispose(){this.dispatchEvent({type:"dispose"})}},yi=class extends gh{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},ql=class extends Tr{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=xn,this.minFilter=xn,this.wrapR=_r,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var vh=class extends Tr{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=xn,this.minFilter=xn,this.wrapR=_r,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Wi=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let l=i[r+0],c=i[r+1],u=i[r+2],d=i[r+3],f=s[o+0],h=s[o+1],g=s[o+2],x=s[o+3];if(a===0){e[t+0]=l,e[t+1]=c,e[t+2]=u,e[t+3]=d;return}if(a===1){e[t+0]=f,e[t+1]=h,e[t+2]=g,e[t+3]=x;return}if(d!==x||l!==f||c!==h||u!==g){let m=1-a,p=l*f+c*h+u*g+d*x,S=p>=0?1:-1,E=1-p*p;if(E>Number.EPSILON){let L=Math.sqrt(E),T=Math.atan2(L,p*S);m=Math.sin(m*T)/L,a=Math.sin(a*T)/L}let w=a*S;if(l=l*m+f*w,c=c*m+h*w,u=u*m+g*w,d=d*m+x*w,m===1-a){let L=1/Math.sqrt(l*l+c*c+u*u+d*d);l*=L,c*=L,u*=L,d*=L}}e[t]=l,e[t+1]=c,e[t+2]=u,e[t+3]=d}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],l=i[r+1],c=i[r+2],u=i[r+3],d=s[o],f=s[o+1],h=s[o+2],g=s[o+3];return e[t]=a*g+u*d+l*h-c*f,e[t+1]=l*g+u*f+c*d-a*h,e[t+2]=c*g+u*h+a*f-l*d,e[t+3]=u*g-a*d-l*f-c*h,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,l=Math.sin,c=a(i/2),u=a(r/2),d=a(s/2),f=l(i/2),h=l(r/2),g=l(s/2);switch(o){case"XYZ":this._x=f*u*d+c*h*g,this._y=c*h*d-f*u*g,this._z=c*u*g+f*h*d,this._w=c*u*d-f*h*g;break;case"YXZ":this._x=f*u*d+c*h*g,this._y=c*h*d-f*u*g,this._z=c*u*g-f*h*d,this._w=c*u*d+f*h*g;break;case"ZXY":this._x=f*u*d-c*h*g,this._y=c*h*d+f*u*g,this._z=c*u*g+f*h*d,this._w=c*u*d-f*h*g;break;case"ZYX":this._x=f*u*d-c*h*g,this._y=c*h*d+f*u*g,this._z=c*u*g-f*h*d,this._w=c*u*d+f*h*g;break;case"YZX":this._x=f*u*d+c*h*g,this._y=c*h*d+f*u*g,this._z=c*u*g-f*h*d,this._w=c*u*d-f*h*g;break;case"XZY":this._x=f*u*d-c*h*g,this._y=c*h*d-f*u*g,this._z=c*u*g+f*h*d,this._w=c*u*d+f*h*g;break;default:console.warn("THREE.Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],l=t[9],c=t[2],u=t[6],d=t[10],f=i+a+d;if(f>0){let h=.5/Math.sqrt(f+1);this._w=.25/h,this._x=(u-l)*h,this._y=(s-c)*h,this._z=(o-r)*h}else if(i>a&&i>d){let h=2*Math.sqrt(1+i-a-d);this._w=(u-l)/h,this._x=.25*h,this._y=(r+o)/h,this._z=(s+c)/h}else if(a>d){let h=2*Math.sqrt(1+a-i-d);this._w=(s-c)/h,this._x=(r+o)/h,this._y=.25*h,this._z=(l+u)/h}else{let h=2*Math.sqrt(1+d-i-a);this._w=(o-r)/h,this._x=(s+c)/h,this._y=(l+u)/h,this._z=.25*h}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<Number.EPSILON?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(nn(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,l=t._y,c=t._z,u=t._w;return this._x=i*u+o*a+r*c-s*l,this._y=r*u+o*l+s*a-i*c,this._z=s*u+o*c+i*l-r*a,this._w=o*u-i*a-r*l-s*c,this._onChangeCallback(),this}slerp(e,t){if(t===0)return this;if(t===1)return this.copy(e);let i=this._x,r=this._y,s=this._z,o=this._w,a=o*e._w+i*e._x+r*e._y+s*e._z;if(a<0?(this._w=-e._w,this._x=-e._x,this._y=-e._y,this._z=-e._z,a=-a):this.copy(e),a>=1)return this._w=o,this._x=i,this._y=r,this._z=s,this;let l=1-a*a;if(l<=Number.EPSILON){let h=1-t;return this._w=h*o+t*this._w,this._x=h*i+t*this._x,this._y=h*r+t*this._y,this._z=h*s+t*this._z,this.normalize(),this}let c=Math.sqrt(l),u=Math.atan2(c,a),d=Math.sin((1-t)*u)/c,f=Math.sin(t*u)/c;return this._w=o*d+this._w*f,this._x=i*d+this._x*f,this._y=r*d+this._y*f,this._z=s*d+this._z*f,this._onChangeCallback(),this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},V=class n{constructor(e=0,t=0,i=0){n.prototype.isVector3=!0,this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(wy.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(wy.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,l=e.w,c=2*(o*r-a*i),u=2*(a*t-s*r),d=2*(s*i-o*t);return this.x=t+l*c+o*d-a*u,this.y=i+l*u+a*c-s*d,this.z=r+l*d+s*u-o*c,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this.z=Math.max(e.z,Math.min(t.z,this.z)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this.z=Math.max(e,Math.min(t,this.z)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,l=t.z;return this.x=r*l-s*a,this.y=s*o-i*l,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return nf.copy(this).projectOnVector(e),this.sub(nf)}reflect(e){return this.sub(nf.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(nn(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},nf=new V,wy=new Wi,Er=class{constructor(e=new V(1/0,1/0,1/0),t=new V(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(In.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(In.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=In.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,In):In.fromBufferAttribute(s,o),In.applyMatrix4(e.matrixWorld),this.expandByPoint(In);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),hl.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),hl.copy(i.boundingBox)),hl.applyMatrix4(e.matrixWorld),this.union(hl)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,In),In.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(_o),pl.subVectors(this.max,_o),hs.subVectors(e.a,_o),ps.subVectors(e.b,_o),ms.subVectors(e.c,_o),Pi.subVectors(ps,hs),Fi.subVectors(ms,ps),cr.subVectors(hs,ms);let t=[0,-Pi.z,Pi.y,0,-Fi.z,Fi.y,0,-cr.z,cr.y,Pi.z,0,-Pi.x,Fi.z,0,-Fi.x,cr.z,0,-cr.x,-Pi.y,Pi.x,0,-Fi.y,Fi.x,0,-cr.y,cr.x,0];return!rf(t,hs,ps,ms,pl)||(t=[1,0,0,0,1,0,0,0,1],!rf(t,hs,ps,ms,pl))?!1:(ml.crossVectors(Pi,Fi),t=[ml.x,ml.y,ml.z],rf(t,hs,ps,ms,pl))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,In).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(In).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(si[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),si[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),si[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),si[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),si[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),si[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),si[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),si[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(si),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}},si=[new V,new V,new V,new V,new V,new V,new V,new V],In=new V,hl=new Er,hs=new V,ps=new V,ms=new V,Pi=new V,Fi=new V,cr=new V,_o=new V,pl=new V,ml=new V,ur=new V;function rf(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){ur.fromArray(n,s);let a=r.x*Math.abs(ur.x)+r.y*Math.abs(ur.y)+r.z*Math.abs(ur.z),l=e.dot(ur),c=t.dot(ur),u=i.dot(ur);if(Math.max(-Math.max(l,c,u),Math.min(l,c,u))>a)return!1}return!0}var rC=new Er,xo=new V,sf=new V,Co=class{constructor(e=new V,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):rC.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;xo.subVectors(e,this.center);let t=xo.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(xo,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(sf.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(xo.copy(e.center).add(sf)),this.expandByPoint(xo.copy(e.center).sub(sf))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}},oi=new V,of=new V,gl=new V,Oi=new V,af=new V,vl=new V,lf=new V,yh=class{constructor(e=new V,t=new V(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,oi)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=oi.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(oi.copy(this.origin).addScaledVector(this.direction,t),oi.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){of.copy(e).add(t).multiplyScalar(.5),gl.copy(t).sub(e).normalize(),Oi.copy(this.origin).sub(of);let s=e.distanceTo(t)*.5,o=-this.direction.dot(gl),a=Oi.dot(this.direction),l=-Oi.dot(gl),c=Oi.lengthSq(),u=Math.abs(1-o*o),d,f,h,g;if(u>0)if(d=o*l-a,f=o*a-l,g=s*u,d>=0)if(f>=-g)if(f<=g){let x=1/u;d*=x,f*=x,h=d*(d+o*f+2*a)+f*(o*d+f+2*l)+c}else f=s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*l)+c;else f=-s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*l)+c;else f<=-g?(d=Math.max(0,-(-o*s+a)),f=d>0?-s:Math.min(Math.max(-s,-l),s),h=-d*d+f*(f+2*l)+c):f<=g?(d=0,f=Math.min(Math.max(-s,-l),s),h=f*(f+2*l)+c):(d=Math.max(0,-(o*s+a)),f=d>0?s:Math.min(Math.max(-s,-l),s),h=-d*d+f*(f+2*l)+c);else f=o>0?-s:s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*l)+c;return i&&i.copy(this.origin).addScaledVector(this.direction,d),r&&r.copy(of).addScaledVector(gl,f),h}intersectSphere(e,t){oi.subVectors(e.center,this.origin);let i=oi.dot(this.direction),r=oi.dot(oi)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,l=i+o;return l<0?null:a<0?this.at(l,t):this.at(a,t)}intersectsSphere(e){return this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,l,c=1/this.direction.x,u=1/this.direction.y,d=1/this.direction.z,f=this.origin;return c>=0?(i=(e.min.x-f.x)*c,r=(e.max.x-f.x)*c):(i=(e.max.x-f.x)*c,r=(e.min.x-f.x)*c),u>=0?(s=(e.min.y-f.y)*u,o=(e.max.y-f.y)*u):(s=(e.max.y-f.y)*u,o=(e.min.y-f.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),d>=0?(a=(e.min.z-f.z)*d,l=(e.max.z-f.z)*d):(a=(e.max.z-f.z)*d,l=(e.min.z-f.z)*d),i>l||a>r)||((a>i||i!==i)&&(i=a),(l<r||r!==r)&&(r=l),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,oi)!==null}intersectTriangle(e,t,i,r,s){af.subVectors(t,e),vl.subVectors(i,e),lf.crossVectors(af,vl);let o=this.direction.dot(lf),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;Oi.subVectors(this.origin,e);let l=a*this.direction.dot(vl.crossVectors(Oi,vl));if(l<0)return null;let c=a*this.direction.dot(af.cross(Oi));if(c<0||l+c>o)return null;let u=-a*Oi.dot(lf);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},Dt=class n{constructor(e,t,i,r,s,o,a,l,c,u,d,f,h,g,x,m){n.prototype.isMatrix4=!0,this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,l,c,u,d,f,h,g,x,m)}set(e,t,i,r,s,o,a,l,c,u,d,f,h,g,x,m){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=l,p[2]=c,p[6]=u,p[10]=d,p[14]=f,p[3]=h,p[7]=g,p[11]=x,p[15]=m,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){let t=this.elements,i=e.elements,r=1/gs.setFromMatrixColumn(e,0).length(),s=1/gs.setFromMatrixColumn(e,1).length(),o=1/gs.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),l=Math.cos(r),c=Math.sin(r),u=Math.cos(s),d=Math.sin(s);if(e.order==="XYZ"){let f=o*u,h=o*d,g=a*u,x=a*d;t[0]=l*u,t[4]=-l*d,t[8]=c,t[1]=h+g*c,t[5]=f-x*c,t[9]=-a*l,t[2]=x-f*c,t[6]=g+h*c,t[10]=o*l}else if(e.order==="YXZ"){let f=l*u,h=l*d,g=c*u,x=c*d;t[0]=f+x*a,t[4]=g*a-h,t[8]=o*c,t[1]=o*d,t[5]=o*u,t[9]=-a,t[2]=h*a-g,t[6]=x+f*a,t[10]=o*l}else if(e.order==="ZXY"){let f=l*u,h=l*d,g=c*u,x=c*d;t[0]=f-x*a,t[4]=-o*d,t[8]=g+h*a,t[1]=h+g*a,t[5]=o*u,t[9]=x-f*a,t[2]=-o*c,t[6]=a,t[10]=o*l}else if(e.order==="ZYX"){let f=o*u,h=o*d,g=a*u,x=a*d;t[0]=l*u,t[4]=g*c-h,t[8]=f*c+x,t[1]=l*d,t[5]=x*c+f,t[9]=h*c-g,t[2]=-c,t[6]=a*l,t[10]=o*l}else if(e.order==="YZX"){let f=o*l,h=o*c,g=a*l,x=a*c;t[0]=l*u,t[4]=x-f*d,t[8]=g*d+h,t[1]=d,t[5]=o*u,t[9]=-a*u,t[2]=-c*u,t[6]=h*d+g,t[10]=f-x*d}else if(e.order==="XZY"){let f=o*l,h=o*c,g=a*l,x=a*c;t[0]=l*u,t[4]=-d,t[8]=c*u,t[1]=f*d+x,t[5]=o*u,t[9]=h*d-g,t[2]=g*d-h,t[6]=a*u,t[10]=x*d+f}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(sC,e,oC)}lookAt(e,t,i){let r=this.elements;return dn.subVectors(e,t),dn.lengthSq()===0&&(dn.z=1),dn.normalize(),Li.crossVectors(i,dn),Li.lengthSq()===0&&(Math.abs(i.z)===1?dn.x+=1e-4:dn.z+=1e-4,dn.normalize(),Li.crossVectors(i,dn)),Li.normalize(),yl.crossVectors(dn,Li),r[0]=Li.x,r[4]=yl.x,r[8]=dn.x,r[1]=Li.y,r[5]=yl.y,r[9]=dn.y,r[2]=Li.z,r[6]=yl.z,r[10]=dn.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],l=i[8],c=i[12],u=i[1],d=i[5],f=i[9],h=i[13],g=i[2],x=i[6],m=i[10],p=i[14],S=i[3],E=i[7],w=i[11],L=i[15],T=r[0],C=r[4],U=r[8],ie=r[12],v=r[1],M=r[5],j=r[9],z=r[13],Y=r[2],J=r[6],W=r[10],K=r[14],G=r[3],ue=r[7],de=r[11],_e=r[15];return s[0]=o*T+a*v+l*Y+c*G,s[4]=o*C+a*M+l*J+c*ue,s[8]=o*U+a*j+l*W+c*de,s[12]=o*ie+a*z+l*K+c*_e,s[1]=u*T+d*v+f*Y+h*G,s[5]=u*C+d*M+f*J+h*ue,s[9]=u*U+d*j+f*W+h*de,s[13]=u*ie+d*z+f*K+h*_e,s[2]=g*T+x*v+m*Y+p*G,s[6]=g*C+x*M+m*J+p*ue,s[10]=g*U+x*j+m*W+p*de,s[14]=g*ie+x*z+m*K+p*_e,s[3]=S*T+E*v+w*Y+L*G,s[7]=S*C+E*M+w*J+L*ue,s[11]=S*U+E*j+w*W+L*de,s[15]=S*ie+E*z+w*K+L*_e,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],l=e[9],c=e[13],u=e[2],d=e[6],f=e[10],h=e[14],g=e[3],x=e[7],m=e[11],p=e[15];return g*(+s*l*d-r*c*d-s*a*f+i*c*f+r*a*h-i*l*h)+x*(+t*l*h-t*c*f+s*o*f-r*o*h+r*c*u-s*l*u)+m*(+t*c*d-t*a*h-s*o*d+i*o*h+s*a*u-i*c*u)+p*(-r*a*u-t*l*d+t*a*f+r*o*d-i*o*f+i*l*u)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],l=e[6],c=e[7],u=e[8],d=e[9],f=e[10],h=e[11],g=e[12],x=e[13],m=e[14],p=e[15],S=d*m*c-x*f*c+x*l*h-a*m*h-d*l*p+a*f*p,E=g*f*c-u*m*c-g*l*h+o*m*h+u*l*p-o*f*p,w=u*x*c-g*d*c+g*a*h-o*x*h-u*a*p+o*d*p,L=g*d*l-u*x*l-g*a*f+o*x*f+u*a*m-o*d*m,T=t*S+i*E+r*w+s*L;if(T===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let C=1/T;return e[0]=S*C,e[1]=(x*f*s-d*m*s-x*r*h+i*m*h+d*r*p-i*f*p)*C,e[2]=(a*m*s-x*l*s+x*r*c-i*m*c-a*r*p+i*l*p)*C,e[3]=(d*l*s-a*f*s-d*r*c+i*f*c+a*r*h-i*l*h)*C,e[4]=E*C,e[5]=(u*m*s-g*f*s+g*r*h-t*m*h-u*r*p+t*f*p)*C,e[6]=(g*l*s-o*m*s-g*r*c+t*m*c+o*r*p-t*l*p)*C,e[7]=(o*f*s-u*l*s+u*r*c-t*f*c-o*r*h+t*l*h)*C,e[8]=w*C,e[9]=(g*d*s-u*x*s-g*i*h+t*x*h+u*i*p-t*d*p)*C,e[10]=(o*x*s-g*a*s+g*i*c-t*x*c-o*i*p+t*a*p)*C,e[11]=(u*a*s-o*d*s-u*i*c+t*d*c+o*i*h-t*a*h)*C,e[12]=L*C,e[13]=(u*x*r-g*d*r+g*i*f-t*x*f-u*i*m+t*d*m)*C,e[14]=(g*a*r-o*x*r-g*i*l+t*x*l+o*i*m-t*a*m)*C,e[15]=(o*d*r-u*a*r+u*i*l-t*d*l-o*i*f+t*a*f)*C,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,l=e.z,c=s*o,u=s*a;return this.set(c*o+i,c*a-r*l,c*l+r*a,0,c*a+r*l,u*a+i,u*l-r*o,0,c*l-r*a,u*l+r*o,s*l*l+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,l=t._w,c=s+s,u=o+o,d=a+a,f=s*c,h=s*u,g=s*d,x=o*u,m=o*d,p=a*d,S=l*c,E=l*u,w=l*d,L=i.x,T=i.y,C=i.z;return r[0]=(1-(x+p))*L,r[1]=(h+w)*L,r[2]=(g-E)*L,r[3]=0,r[4]=(h-w)*T,r[5]=(1-(f+p))*T,r[6]=(m+S)*T,r[7]=0,r[8]=(g+E)*C,r[9]=(m-S)*C,r[10]=(1-(f+x))*C,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements,s=gs.set(r[0],r[1],r[2]).length(),o=gs.set(r[4],r[5],r[6]).length(),a=gs.set(r[8],r[9],r[10]).length();this.determinant()<0&&(s=-s),e.x=r[12],e.y=r[13],e.z=r[14],Rn.copy(this);let c=1/s,u=1/o,d=1/a;return Rn.elements[0]*=c,Rn.elements[1]*=c,Rn.elements[2]*=c,Rn.elements[4]*=u,Rn.elements[5]*=u,Rn.elements[6]*=u,Rn.elements[8]*=d,Rn.elements[9]*=d,Rn.elements[10]*=d,t.setFromRotationMatrix(Rn),i.x=s,i.y=o,i.z=a,this}makePerspective(e,t,i,r,s,o,a=pi){let l=this.elements,c=2*s/(t-e),u=2*s/(i-r),d=(t+e)/(t-e),f=(i+r)/(i-r),h,g;if(a===pi)h=-(o+s)/(o-s),g=-2*o*s/(o-s);else if(a===Wl)h=-o/(o-s),g=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=c,l[4]=0,l[8]=d,l[12]=0,l[1]=0,l[5]=u,l[9]=f,l[13]=0,l[2]=0,l[6]=0,l[10]=h,l[14]=g,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=pi){let l=this.elements,c=1/(t-e),u=1/(i-r),d=1/(o-s),f=(t+e)*c,h=(i+r)*u,g,x;if(a===pi)g=(o+s)*d,x=-2*d;else if(a===Wl)g=s*d,x=-1*d;else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=2*c,l[4]=0,l[8]=0,l[12]=-f,l[1]=0,l[5]=2*u,l[9]=0,l[13]=-h,l[2]=0,l[6]=0,l[10]=x,l[14]=-g,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},gs=new V,Rn=new Dt,sC=new V(0,0,0),oC=new V(1,1,1),Li=new V,yl=new V,dn=new V,Sy=new Dt,Cy=new Wi,br=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],l=s[8],c=s[1],u=s[5],d=s[9],f=s[2],h=s[6],g=s[10];switch(i){case"XYZ":this._y=Math.asin(nn(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-d,g),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(h,u),this._z=0);break;case"YXZ":this._x=Math.asin(-nn(d,-1,1)),Math.abs(d)<.9999999?(this._y=Math.atan2(l,g),this._z=Math.atan2(c,u)):(this._y=Math.atan2(-f,o),this._z=0);break;case"ZXY":this._x=Math.asin(nn(h,-1,1)),Math.abs(h)<.9999999?(this._y=Math.atan2(-f,g),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(c,o));break;case"ZYX":this._y=Math.asin(-nn(f,-1,1)),Math.abs(f)<.9999999?(this._x=Math.atan2(h,g),this._z=Math.atan2(c,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(nn(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-d,u),this._y=Math.atan2(-f,o)):(this._x=0,this._y=Math.atan2(l,g));break;case"XZY":this._z=Math.asin(-nn(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(h,u),this._y=Math.atan2(l,o)):(this._x=Math.atan2(-d,g),this._y=0);break;default:console.warn("THREE.Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return Sy.makeRotationFromQuaternion(t),this.setFromRotationMatrix(Sy,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return Cy.setFromEuler(this),this.setFromQuaternion(Cy,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),Xl=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},aC=0,Dy=new V,vs=new Wi,ai=new Dt,_l=new V,Mo=new V,lC=new V,cC=new Wi,Ty=new V(1,0,0),Ay=new V(0,1,0),Iy=new V(0,0,1),Ry={type:"added"},uC={type:"removed"},ys={type:"childadded",child:null},cf={type:"childremoved",child:null},_i=(()=>{class n extends Gi{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:aC++}),this.uuid=Io(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new V,i=new br,r=new Wi,s=new V(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new Dt},normalMatrix:{value:new Le}}),this.matrix=new Dt,this.matrixWorld=new Dt,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new Xl,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.userData={}}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return vs.setFromAxisAngle(t,i),this.quaternion.multiply(vs),this}rotateOnWorldAxis(t,i){return vs.setFromAxisAngle(t,i),this.quaternion.premultiply(vs),this}rotateX(t){return this.rotateOnAxis(Ty,t)}rotateY(t){return this.rotateOnAxis(Ay,t)}rotateZ(t){return this.rotateOnAxis(Iy,t)}translateOnAxis(t,i){return Dy.copy(t).applyQuaternion(this.quaternion),this.position.add(Dy.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(Ty,t)}translateY(t){return this.translateOnAxis(Ay,t)}translateZ(t){return this.translateOnAxis(Iy,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(ai.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?_l.copy(t):_l.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),Mo.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?ai.lookAt(Mo,_l,this.up):ai.lookAt(_l,Mo,this.up),this.quaternion.setFromRotationMatrix(ai),s&&(ai.extractRotation(s.matrixWorld),vs.setFromRotationMatrix(ai),this.quaternion.premultiply(vs.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(console.error("THREE.Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(Ry),ys.child=t,this.dispatchEvent(ys),ys.child=null):console.error("THREE.Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(uC),cf.child=t,this.dispatchEvent(cf),cf.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),ai.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),ai.multiply(t.parent.matrixWorld)),t.applyMatrix4(ai),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(Ry),ys.child=t,this.dispatchEvent(ys),ys.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Mo,t,lC),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Mo,cC,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale),this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.6,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.visibility=this._visibility,s.active=this._active,s.bounds=this._bounds.map(l=>({boxInitialized:l.boxInitialized,boxMin:l.box.min.toArray(),boxMax:l.box.max.toArray(),sphereInitialized:l.sphereInitialized,sphereRadius:l.sphere.radius,sphereCenter:l.sphere.center.toArray()})),s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.geometryCount=this._geometryCount,s.matricesTexture=this._matricesTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere={center:s.boundingSphere.center.toArray(),radius:s.boundingSphere.radius}),this.boundingBox!==null&&(s.boundingBox={min:s.boundingBox.min.toArray(),max:s.boundingBox.max.toArray()}));function o(l,c){return l[c.uuid]===void 0&&(l[c.uuid]=c.toJSON(t)),c.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let l=this.geometry.parameters;if(l!==void 0&&l.shapes!==void 0){let c=l.shapes;if(Array.isArray(c))for(let u=0,d=c.length;u<d;u++){let f=c[u];o(t.shapes,f)}else o(t.shapes,c)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let l=[];for(let c=0,u=this.material.length;c<u;c++)l.push(o(t.materials,this.material[c]));s.material=l}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let l=0;l<this.children.length;l++)s.children.push(this.children[l].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let l=0;l<this.animations.length;l++){let c=this.animations[l];s.animations.push(o(t.animations,c))}}if(i){let l=a(t.geometries),c=a(t.materials),u=a(t.textures),d=a(t.images),f=a(t.shapes),h=a(t.skeletons),g=a(t.animations),x=a(t.nodes);l.length>0&&(r.geometries=l),c.length>0&&(r.materials=c),u.length>0&&(r.textures=u),d.length>0&&(r.images=d),f.length>0&&(r.shapes=f),h.length>0&&(r.skeletons=h),g.length>0&&(r.animations=g),x.length>0&&(r.nodes=x)}return r.object=s,r;function a(l){let c=[];for(let u in l){let d=l[u];delete d.metadata,c.push(d)}return c}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new V(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),Nn=new V,li=new V,uf=new V,ci=new V,_s=new V,xs=new V,Ny=new V,df=new V,ff=new V,hf=new V,pf=new Et,mf=new Et,gf=new Et,vr=class n{constructor(e=new V,t=new V,i=new V){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),Nn.subVectors(e,t),r.cross(Nn);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){Nn.subVectors(r,t),li.subVectors(i,t),uf.subVectors(e,t);let o=Nn.dot(Nn),a=Nn.dot(li),l=Nn.dot(uf),c=li.dot(li),u=li.dot(uf),d=o*c-a*a;if(d===0)return s.set(0,0,0),null;let f=1/d,h=(c*l-a*u)*f,g=(o*u-a*l)*f;return s.set(1-h-g,g,h)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,ci)===null?!1:ci.x>=0&&ci.y>=0&&ci.x+ci.y<=1}static getInterpolation(e,t,i,r,s,o,a,l){return this.getBarycoord(e,t,i,r,ci)===null?(l.x=0,l.y=0,"z"in l&&(l.z=0),"w"in l&&(l.w=0),null):(l.setScalar(0),l.addScaledVector(s,ci.x),l.addScaledVector(o,ci.y),l.addScaledVector(a,ci.z),l)}static getInterpolatedAttribute(e,t,i,r,s,o){return pf.setScalar(0),mf.setScalar(0),gf.setScalar(0),pf.fromBufferAttribute(e,t),mf.fromBufferAttribute(e,i),gf.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(pf,s.x),o.addScaledVector(mf,s.y),o.addScaledVector(gf,s.z),o}static isFrontFacing(e,t,i,r){return Nn.subVectors(i,t),li.subVectors(e,t),Nn.cross(li).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return Nn.subVectors(this.c,this.b),li.subVectors(this.a,this.b),Nn.cross(li).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;_s.subVectors(r,i),xs.subVectors(s,i),df.subVectors(e,i);let l=_s.dot(df),c=xs.dot(df);if(l<=0&&c<=0)return t.copy(i);ff.subVectors(e,r);let u=_s.dot(ff),d=xs.dot(ff);if(u>=0&&d<=u)return t.copy(r);let f=l*d-u*c;if(f<=0&&l>=0&&u<=0)return o=l/(l-u),t.copy(i).addScaledVector(_s,o);hf.subVectors(e,s);let h=_s.dot(hf),g=xs.dot(hf);if(g>=0&&h<=g)return t.copy(s);let x=h*c-l*g;if(x<=0&&c>=0&&g<=0)return a=c/(c-g),t.copy(i).addScaledVector(xs,a);let m=u*g-h*d;if(m<=0&&d-u>=0&&h-g>=0)return Ny.subVectors(s,r),a=(d-u)/(d-u+(h-g)),t.copy(r).addScaledVector(Ny,a);let p=1/(m+x+f);return o=x*p,a=f*p,t.copy(i).addScaledVector(_s,o).addScaledVector(xs,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},w_={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},ki={h:0,s:0,l:0},xl={h:0,s:0,l:0};function vf(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var qe=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=Xn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,rt.toWorkingColorSpace(this,t),this}setRGB(e,t,i,r=rt.workingColorSpace){return this.r=e,this.g=t,this.b=i,rt.toWorkingColorSpace(this,r),this}setHSL(e,t,i,r=rt.workingColorSpace){if(e=ZS(e,1),t=nn(t,0,1),i=nn(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=vf(o,s,e+1/3),this.g=vf(o,s,e),this.b=vf(o,s,e-1/3)}return rt.toWorkingColorSpace(this,r),this}setStyle(e,t=Xn){function i(s){s!==void 0&&parseFloat(s)<1&&console.warn("THREE.Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:console.warn("THREE.Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);console.warn("THREE.Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=Xn){let i=w_[e.toLowerCase()];return i!==void 0?this.setHex(i,t):console.warn("THREE.Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=Ts(e.r),this.g=Ts(e.g),this.b=Ts(e.b),this}copyLinearToSRGB(e){return this.r=ef(e.r),this.g=ef(e.g),this.b=ef(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=Xn){return rt.fromWorkingColorSpace(Ht.copy(this),e),Math.round(nn(Ht.r*255,0,255))*65536+Math.round(nn(Ht.g*255,0,255))*256+Math.round(nn(Ht.b*255,0,255))}getHexString(e=Xn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=rt.workingColorSpace){rt.fromWorkingColorSpace(Ht.copy(this),t);let i=Ht.r,r=Ht.g,s=Ht.b,o=Math.max(i,r,s),a=Math.min(i,r,s),l,c,u=(a+o)/2;if(a===o)l=0,c=0;else{let d=o-a;switch(c=u<=.5?d/(o+a):d/(2-o-a),o){case i:l=(r-s)/d+(r<s?6:0);break;case r:l=(s-i)/d+2;break;case s:l=(i-r)/d+4;break}l/=6}return e.h=l,e.s=c,e.l=u,e}getRGB(e,t=rt.workingColorSpace){return rt.fromWorkingColorSpace(Ht.copy(this),t),e.r=Ht.r,e.g=Ht.g,e.b=Ht.b,e}getStyle(e=Xn){rt.fromWorkingColorSpace(Ht.copy(this),e);let t=Ht.r,i=Ht.g,r=Ht.b;return e!==Xn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(ki),this.setHSL(ki.h+e,ki.s+t,ki.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(ki),e.getHSL(xl);let i=Jd(ki.h,xl.h,t),r=Jd(ki.s,xl.s,t),s=Jd(ki.l,xl.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},Ht=new qe;qe.NAMES=w_;var dC=0,wr=class extends Gi{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:dC++}),this.uuid=Io(),this.name="",this.type="Material",this.blending=Cs,this.side=zi,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=Tf,this.blendDst=Af,this.blendEquation=gr,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new qe(0,0,0),this.blendAlpha=0,this.depthFunc=Is,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=yy,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=ds,this.stencilZFail=ds,this.stencilZPass=ds,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){console.warn(`THREE.Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){console.warn(`THREE.Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.6,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==Cs&&(i.blending=this.blending),this.side!==zi&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==Tf&&(i.blendSrc=this.blendSrc),this.blendDst!==Af&&(i.blendDst=this.blendDst),this.blendEquation!==gr&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==Is&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==yy&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==ds&&(i.stencilFail=this.stencilFail),this.stencilZFail!==ds&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==ds&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let l=s[a];delete l.metadata,o.push(l)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}onBuild(){console.warn("Material: onBuild() has been removed.")}},Yl=class extends wr{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new qe(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new br,this.combine=u_,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}};var At=new V,Ml=new st,Mn=class{constructor(e,t,i=!1){if(Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=_y,this.updateRanges=[],this.gpuType=hi,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)Ml.fromBufferAttribute(this,t),Ml.applyMatrix3(e),this.setXY(t,Ml.x,Ml.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)At.fromBufferAttribute(this,t),At.applyMatrix3(e),this.setXYZ(t,At.x,At.y,At.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)At.fromBufferAttribute(this,t),At.applyMatrix4(e),this.setXYZ(t,At.x,At.y,At.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)At.fromBufferAttribute(this,t),At.applyNormalMatrix(e),this.setXYZ(t,At.x,At.y,At.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)At.fromBufferAttribute(this,t),At.transformDirection(e),this.setXYZ(t,At.x,At.y,At.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=vo(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=tn(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=vo(t,this.array)),t}setX(e,t){return this.normalized&&(t=tn(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=vo(t,this.array)),t}setY(e,t){return this.normalized&&(t=tn(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=vo(t,this.array)),t}setZ(e,t){return this.normalized&&(t=tn(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=vo(t,this.array)),t}setW(e,t){return this.normalized&&(t=tn(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=tn(t,this.array),i=tn(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=tn(t,this.array),i=tn(i,this.array),r=tn(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=tn(t,this.array),i=tn(i,this.array),r=tn(r,this.array),s=tn(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==_y&&(e.usage=this.usage),e}};var Zl=class extends Mn{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var Kl=class extends Mn{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var gi=class extends Mn{constructor(e,t,i){super(new Float32Array(e),t,i)}},fC=0,_n=new Dt,yf=new _i,Ms=new V,fn=new Er,Eo=new Er,Ot=new V,Sr=class n extends Gi{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:fC++}),this.uuid=Io(),this.name="",this.type="BufferGeometry",this.index=null,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(b_(e)?Kl:Zl)(e,1):this.index=e,this}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new Le().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return _n.makeRotationFromQuaternion(e),this.applyMatrix4(_n),this}rotateX(e){return _n.makeRotationX(e),this.applyMatrix4(_n),this}rotateY(e){return _n.makeRotationY(e),this.applyMatrix4(_n),this}rotateZ(e){return _n.makeRotationZ(e),this.applyMatrix4(_n),this}translate(e,t,i){return _n.makeTranslation(e,t,i),this.applyMatrix4(_n),this}scale(e,t,i){return _n.makeScale(e,t,i),this.applyMatrix4(_n),this}lookAt(e){return yf.lookAt(e),yf.updateMatrix(),this.applyMatrix4(yf.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(Ms).negate(),this.translate(Ms.x,Ms.y,Ms.z),this}setFromPoints(e){let t=[];for(let i=0,r=e.length;i<r;i++){let s=e[i];t.push(s.x,s.y,s.z||0)}return this.setAttribute("position",new gi(t,3)),this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new Er);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){console.error("THREE.BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new V(-1/0,-1/0,-1/0),new V(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];fn.setFromBufferAttribute(s),this.morphTargetsRelative?(Ot.addVectors(this.boundingBox.min,fn.min),this.boundingBox.expandByPoint(Ot),Ot.addVectors(this.boundingBox.max,fn.max),this.boundingBox.expandByPoint(Ot)):(this.boundingBox.expandByPoint(fn.min),this.boundingBox.expandByPoint(fn.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&console.error('THREE.BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new Co);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){console.error("THREE.BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new V,1/0);return}if(e){let i=this.boundingSphere.center;if(fn.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];Eo.setFromBufferAttribute(a),this.morphTargetsRelative?(Ot.addVectors(fn.min,Eo.min),fn.expandByPoint(Ot),Ot.addVectors(fn.max,Eo.max),fn.expandByPoint(Ot)):(fn.expandByPoint(Eo.min),fn.expandByPoint(Eo.max))}fn.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)Ot.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(Ot));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],l=this.morphTargetsRelative;for(let c=0,u=a.count;c<u;c++)Ot.fromBufferAttribute(a,c),l&&(Ms.fromBufferAttribute(e,c),Ot.add(Ms)),r=Math.max(r,i.distanceToSquared(Ot))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&console.error('THREE.BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){console.error("THREE.BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new Mn(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],l=[];for(let U=0;U<i.count;U++)a[U]=new V,l[U]=new V;let c=new V,u=new V,d=new V,f=new st,h=new st,g=new st,x=new V,m=new V;function p(U,ie,v){c.fromBufferAttribute(i,U),u.fromBufferAttribute(i,ie),d.fromBufferAttribute(i,v),f.fromBufferAttribute(s,U),h.fromBufferAttribute(s,ie),g.fromBufferAttribute(s,v),u.sub(c),d.sub(c),h.sub(f),g.sub(f);let M=1/(h.x*g.y-g.x*h.y);isFinite(M)&&(x.copy(u).multiplyScalar(g.y).addScaledVector(d,-h.y).multiplyScalar(M),m.copy(d).multiplyScalar(h.x).addScaledVector(u,-g.x).multiplyScalar(M),a[U].add(x),a[ie].add(x),a[v].add(x),l[U].add(m),l[ie].add(m),l[v].add(m))}let S=this.groups;S.length===0&&(S=[{start:0,count:e.count}]);for(let U=0,ie=S.length;U<ie;++U){let v=S[U],M=v.start,j=v.count;for(let z=M,Y=M+j;z<Y;z+=3)p(e.getX(z+0),e.getX(z+1),e.getX(z+2))}let E=new V,w=new V,L=new V,T=new V;function C(U){L.fromBufferAttribute(r,U),T.copy(L);let ie=a[U];E.copy(ie),E.sub(L.multiplyScalar(L.dot(ie))).normalize(),w.crossVectors(T,ie);let M=w.dot(l[U])<0?-1:1;o.setXYZW(U,E.x,E.y,E.z,M)}for(let U=0,ie=S.length;U<ie;++U){let v=S[U],M=v.start,j=v.count;for(let z=M,Y=M+j;z<Y;z+=3)C(e.getX(z+0)),C(e.getX(z+1)),C(e.getX(z+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new Mn(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let f=0,h=i.count;f<h;f++)i.setXYZ(f,0,0,0);let r=new V,s=new V,o=new V,a=new V,l=new V,c=new V,u=new V,d=new V;if(e)for(let f=0,h=e.count;f<h;f+=3){let g=e.getX(f+0),x=e.getX(f+1),m=e.getX(f+2);r.fromBufferAttribute(t,g),s.fromBufferAttribute(t,x),o.fromBufferAttribute(t,m),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),a.fromBufferAttribute(i,g),l.fromBufferAttribute(i,x),c.fromBufferAttribute(i,m),a.add(u),l.add(u),c.add(u),i.setXYZ(g,a.x,a.y,a.z),i.setXYZ(x,l.x,l.y,l.z),i.setXYZ(m,c.x,c.y,c.z)}else for(let f=0,h=t.count;f<h;f+=3)r.fromBufferAttribute(t,f+0),s.fromBufferAttribute(t,f+1),o.fromBufferAttribute(t,f+2),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),i.setXYZ(f+0,u.x,u.y,u.z),i.setXYZ(f+1,u.x,u.y,u.z),i.setXYZ(f+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)Ot.fromBufferAttribute(e,t),Ot.normalize(),e.setXYZ(t,Ot.x,Ot.y,Ot.z)}toNonIndexed(){function e(a,l){let c=a.array,u=a.itemSize,d=a.normalized,f=new c.constructor(l.length*u),h=0,g=0;for(let x=0,m=l.length;x<m;x++){a.isInterleavedBufferAttribute?h=l[x]*a.data.stride+a.offset:h=l[x]*u;for(let p=0;p<u;p++)f[g++]=c[h++]}return new Mn(f,u,d)}if(this.index===null)return console.warn("THREE.BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let l=r[a],c=e(l,i);t.setAttribute(a,c)}let s=this.morphAttributes;for(let a in s){let l=[],c=s[a];for(let u=0,d=c.length;u<d;u++){let f=c[u],h=e(f,i);l.push(h)}t.morphAttributes[a]=l}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,l=o.length;a<l;a++){let c=o[a];t.addGroup(c.start,c.count,c.materialIndex)}return t}toJSON(){let e={metadata:{version:4.6,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let l=this.parameters;for(let c in l)l[c]!==void 0&&(e[c]=l[c]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let l in i){let c=i[l];e.data.attributes[l]=c.toJSON(e.data)}let r={},s=!1;for(let l in this.morphAttributes){let c=this.morphAttributes[l],u=[];for(let d=0,f=c.length;d<f;d++){let h=c[d];u.push(h.toJSON(e.data))}u.length>0&&(r[l]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere={center:a.center.toArray(),radius:a.radius}),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone(t));let r=e.attributes;for(let c in r){let u=r[c];this.setAttribute(c,u.clone(t))}let s=e.morphAttributes;for(let c in s){let u=[],d=s[c];for(let f=0,h=d.length;f<h;f++)u.push(d[f].clone(t));this.morphAttributes[c]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let c=0,u=o.length;c<u;c++){let d=o[c];this.addGroup(d.start,d.count,d.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let l=e.boundingSphere;return l!==null&&(this.boundingSphere=l.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}},Py=new Dt,dr=new yh,El=new Co,Fy=new V,bl=new V,wl=new V,Sl=new V,_f=new V,Cl=new V,Oy=new V,Dl=new V,rn=class extends _i{constructor(e=new Sr,t=new Yl){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){Cl.set(0,0,0);for(let l=0,c=s.length;l<c;l++){let u=a[l],d=s[l];u!==0&&(_f.fromBufferAttribute(d,e),o?Cl.addScaledVector(_f,u):Cl.addScaledVector(_f.sub(t),u))}t.add(Cl)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),El.copy(i.boundingSphere),El.applyMatrix4(s),dr.copy(e.ray).recast(e.near),!(El.containsPoint(dr.origin)===!1&&(dr.intersectSphere(El,Fy)===null||dr.origin.distanceToSquared(Fy)>(e.far-e.near)**2))&&(Py.copy(s).invert(),dr.copy(e.ray).applyMatrix4(Py),!(i.boundingBox!==null&&dr.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,dr)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,l=s.attributes.position,c=s.attributes.uv,u=s.attributes.uv1,d=s.attributes.normal,f=s.groups,h=s.drawRange;if(a!==null)if(Array.isArray(o))for(let g=0,x=f.length;g<x;g++){let m=f[g],p=o[m.materialIndex],S=Math.max(m.start,h.start),E=Math.min(a.count,Math.min(m.start+m.count,h.start+h.count));for(let w=S,L=E;w<L;w+=3){let T=a.getX(w),C=a.getX(w+1),U=a.getX(w+2);r=Tl(this,p,e,i,c,u,d,T,C,U),r&&(r.faceIndex=Math.floor(w/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),x=Math.min(a.count,h.start+h.count);for(let m=g,p=x;m<p;m+=3){let S=a.getX(m),E=a.getX(m+1),w=a.getX(m+2);r=Tl(this,o,e,i,c,u,d,S,E,w),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}else if(l!==void 0)if(Array.isArray(o))for(let g=0,x=f.length;g<x;g++){let m=f[g],p=o[m.materialIndex],S=Math.max(m.start,h.start),E=Math.min(l.count,Math.min(m.start+m.count,h.start+h.count));for(let w=S,L=E;w<L;w+=3){let T=w,C=w+1,U=w+2;r=Tl(this,p,e,i,c,u,d,T,C,U),r&&(r.faceIndex=Math.floor(w/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),x=Math.min(l.count,h.start+h.count);for(let m=g,p=x;m<p;m+=3){let S=m,E=m+1,w=m+2;r=Tl(this,o,e,i,c,u,d,S,E,w),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}}};function hC(n,e,t,i,r,s,o,a){let l;if(e.side===sn?l=i.intersectTriangle(o,s,r,!0,a):l=i.intersectTriangle(r,s,o,e.side===zi,a),l===null)return null;Dl.copy(a),Dl.applyMatrix4(n.matrixWorld);let c=t.ray.origin.distanceTo(Dl);return c<t.near||c>t.far?null:{distance:c,point:Dl.clone(),object:n}}function Tl(n,e,t,i,r,s,o,a,l,c){n.getVertexPosition(a,bl),n.getVertexPosition(l,wl),n.getVertexPosition(c,Sl);let u=hC(n,e,t,i,bl,wl,Sl,Oy);if(u){let d=new V;vr.getBarycoord(Oy,bl,wl,Sl,d),r&&(u.uv=vr.getInterpolatedAttribute(r,a,l,c,d,new st)),s&&(u.uv1=vr.getInterpolatedAttribute(s,a,l,c,d,new st)),o&&(u.normal=vr.getInterpolatedAttribute(o,a,l,c,d,new V),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let f={a,b:l,c,normal:new V,materialIndex:0};vr.getNormal(bl,wl,Sl,f.normal),u.face=f,u.barycoord=d}return u}var ji=class n extends Sr{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let l=[],c=[],u=[],d=[],f=0,h=0;g("z","y","x",-1,-1,i,t,e,o,s,0),g("z","y","x",1,-1,i,t,-e,o,s,1),g("x","z","y",1,1,e,i,t,r,o,2),g("x","z","y",1,-1,e,i,-t,r,o,3),g("x","y","z",1,-1,e,t,i,r,s,4),g("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(l),this.setAttribute("position",new gi(c,3)),this.setAttribute("normal",new gi(u,3)),this.setAttribute("uv",new gi(d,2));function g(x,m,p,S,E,w,L,T,C,U,ie){let v=w/C,M=L/U,j=w/2,z=L/2,Y=T/2,J=C+1,W=U+1,K=0,G=0,ue=new V;for(let de=0;de<W;de++){let _e=de*M-z;for(let Je=0;Je<J;Je++){let ot=Je*v-j;ue[x]=ot*S,ue[m]=_e*E,ue[p]=Y,c.push(ue.x,ue.y,ue.z),ue[x]=0,ue[m]=0,ue[p]=T>0?1:-1,u.push(ue.x,ue.y,ue.z),d.push(Je/C),d.push(1-de/U),K+=1}}for(let de=0;de<U;de++)for(let _e=0;_e<C;_e++){let Je=f+_e+J*de,ot=f+_e+J*(de+1),$=f+(_e+1)+J*(de+1),ee=f+(_e+1)+J*de;l.push(Je,ot,ee),l.push(ot,$,ee),G+=6}a.addGroup(h,G,ie),h+=G,f+=K}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};function Os(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];r&&(r.isColor||r.isMatrix3||r.isMatrix4||r.isVector2||r.isVector3||r.isVector4||r.isTexture||r.isQuaternion)?r.isRenderTargetTexture?(console.warn("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone():Array.isArray(r)?e[t][i]=r.slice():e[t][i]=r}}return e}function Xt(n){let e={};for(let t=0;t<n.length;t++){let i=Os(n[t]);for(let r in i)e[r]=i[r]}return e}function pC(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function S_(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:rt.workingColorSpace}var mC={clone:Os,merge:Xt},gC=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,vC=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,Zn=class extends wr{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=gC,this.fragmentShader=vC,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=Os(e.uniforms),this.uniformsGroups=pC(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},Jl=class extends _i{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new Dt,this.projectionMatrix=new Dt,this.projectionMatrixInverse=new Dt,this.coordinateSystem=pi}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorldInverse.copy(this.matrixWorld).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorldInverse.copy(this.matrixWorld).invert()}clone(){return new this.constructor().copy(this)}},Ui=new V,Ly=new st,ky=new st,Yt=class extends Jl{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=ph*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(Kd*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return ph*2*Math.atan(Math.tan(Kd*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){Ui.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(Ui.x,Ui.y).multiplyScalar(-e/Ui.z),Ui.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(Ui.x,Ui.y).multiplyScalar(-e/Ui.z)}getViewSize(e,t){return this.getViewBounds(e,Ly,ky),t.subVectors(ky,Ly)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(Kd*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let l=o.fullWidth,c=o.fullHeight;s+=o.offsetX*r/l,t-=o.offsetY*i/c,r*=o.width/l,i*=o.height/c}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}},Es=-90,bs=1,_h=class extends _i{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new Yt(Es,bs,e,t);r.layers=this.layers,this.add(r);let s=new Yt(Es,bs,e,t);s.layers=this.layers,this.add(s);let o=new Yt(Es,bs,e,t);o.layers=this.layers,this.add(o);let a=new Yt(Es,bs,e,t);a.layers=this.layers,this.add(a);let l=new Yt(Es,bs,e,t);l.layers=this.layers,this.add(l);let c=new Yt(Es,bs,e,t);c.layers=this.layers,this.add(c)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,l]=t;for(let c of t)this.remove(c);if(e===pi)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),l.up.set(0,1,0),l.lookAt(0,0,-1);else if(e===Wl)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),l.up.set(0,-1,0),l.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let c of t)this.add(c),c.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,l,c,u]=this.children,d=e.getRenderTarget(),f=e.getActiveCubeFace(),h=e.getActiveMipmapLevel(),g=e.xr.enabled;e.xr.enabled=!1;let x=i.texture.generateMipmaps;i.texture.generateMipmaps=!1,e.setRenderTarget(i,0,r),e.render(t,s),e.setRenderTarget(i,1,r),e.render(t,o),e.setRenderTarget(i,2,r),e.render(t,a),e.setRenderTarget(i,3,r),e.render(t,l),e.setRenderTarget(i,4,r),e.render(t,c),i.texture.generateMipmaps=x,e.setRenderTarget(i,5,r),e.render(t,u),e.setRenderTarget(d,f,h),e.xr.enabled=g,i.texture.needsPMREMUpdate=!0}},Ql=class extends Tr{constructor(e,t,i,r,s,o,a,l,c,u){e=e!==void 0?e:[],t=t!==void 0?t:Rs,super(e,t,i,r,s,o,a,l,c,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}},xh=class extends yi{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new Ql(r,t.mapping,t.wrapS,t.wrapT,t.magFilter,t.minFilter,t.format,t.type,t.anisotropy,t.colorSpace),this.texture.isRenderTargetTexture=!0,this.texture.generateMipmaps=t.generateMipmaps!==void 0?t.generateMipmaps:!1,this.texture.minFilter=t.minFilter!==void 0?t.minFilter:Pn}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

				varying vec3 vWorldDirection;

				vec3 transformDirection( in vec3 dir, in mat4 matrix ) {

					return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );

				}

				void main() {

					vWorldDirection = transformDirection( position, modelMatrix );

					#include <begin_vertex>
					#include <project_vertex>

				}
			`,fragmentShader:`

				uniform sampler2D tEquirect;

				varying vec3 vWorldDirection;

				#include <common>

				void main() {

					vec3 direction = normalize( vWorldDirection );

					vec2 sampleUV = equirectUv( direction );

					gl_FragColor = texture2D( tEquirect, sampleUV );

				}
			`},r=new ji(5,5,5),s=new Zn({name:"CubemapFromEquirect",uniforms:Os(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:sn,blending:Bi});s.uniforms.tEquirect.value=t;let o=new rn(r,s),a=t.minFilter;return t.minFilter===xr&&(t.minFilter=Pn),new _h(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t,i,r){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}},xf=new V,yC=new V,_C=new Le,di=class{constructor(e=new V(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=xf.subVectors(i,t).cross(yC.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t){let i=e.delta(xf),r=this.normal.dot(i);if(r===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let s=-(e.start.dot(this.normal)+this.constant)/r;return s<0||s>1?null:t.copy(e.start).addScaledVector(i,s)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||_C.getNormalMatrix(e),r=this.coplanarPoint(xf).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},fr=new Co,Al=new V,Do=class{constructor(e=new di,t=new di,i=new di,r=new di,s=new di,o=new di){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=pi){let i=this.planes,r=e.elements,s=r[0],o=r[1],a=r[2],l=r[3],c=r[4],u=r[5],d=r[6],f=r[7],h=r[8],g=r[9],x=r[10],m=r[11],p=r[12],S=r[13],E=r[14],w=r[15];if(i[0].setComponents(l-s,f-c,m-h,w-p).normalize(),i[1].setComponents(l+s,f+c,m+h,w+p).normalize(),i[2].setComponents(l+o,f+u,m+g,w+S).normalize(),i[3].setComponents(l-o,f-u,m-g,w-S).normalize(),i[4].setComponents(l-a,f-d,m-x,w-E).normalize(),t===pi)i[5].setComponents(l+a,f+d,m+x,w+E).normalize();else if(t===Wl)i[5].setComponents(a,d,x,E).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),fr.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),fr.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(fr)}intersectsSprite(e){return fr.center.set(0,0,0),fr.radius=.7071067811865476,fr.applyMatrix4(e.matrixWorld),this.intersectsSphere(fr)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(Al.x=r.normal.x>0?e.max.x:e.min.x,Al.y=r.normal.y>0?e.max.y:e.min.y,Al.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(Al)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};function C_(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function xC(n){let e=new WeakMap;function t(a,l){let c=a.array,u=a.usage,d=c.byteLength,f=n.createBuffer();n.bindBuffer(l,f),n.bufferData(l,c,u),a.onUploadCallback();let h;if(c instanceof Float32Array)h=n.FLOAT;else if(c instanceof Uint16Array)a.isFloat16BufferAttribute?h=n.HALF_FLOAT:h=n.UNSIGNED_SHORT;else if(c instanceof Int16Array)h=n.SHORT;else if(c instanceof Uint32Array)h=n.UNSIGNED_INT;else if(c instanceof Int32Array)h=n.INT;else if(c instanceof Int8Array)h=n.BYTE;else if(c instanceof Uint8Array)h=n.UNSIGNED_BYTE;else if(c instanceof Uint8ClampedArray)h=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+c);return{buffer:f,type:h,bytesPerElement:c.BYTES_PER_ELEMENT,version:a.version,size:d}}function i(a,l,c){let u=l.array,d=l.updateRanges;if(n.bindBuffer(c,a),d.length===0)n.bufferSubData(c,0,u);else{d.sort((h,g)=>h.start-g.start);let f=0;for(let h=1;h<d.length;h++){let g=d[f],x=d[h];x.start<=g.start+g.count+1?g.count=Math.max(g.count,x.start+x.count-g.start):(++f,d[f]=x)}d.length=f+1;for(let h=0,g=d.length;h<g;h++){let x=d[h];n.bufferSubData(c,x.start*u.BYTES_PER_ELEMENT,u,x.start,x.count)}l.clearUpdateRanges()}l.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let l=e.get(a);l&&(n.deleteBuffer(l.buffer),e.delete(a))}function o(a,l){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let c=e.get(a);if(c===void 0)e.set(a,t(a,l));else if(c.version<a.version){if(c.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(c.buffer,a,l),c.version=a.version}}return{get:r,remove:s,update:o}}var ec=class n extends Sr{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),l=Math.floor(r),c=a+1,u=l+1,d=e/a,f=t/l,h=[],g=[],x=[],m=[];for(let p=0;p<u;p++){let S=p*f-o;for(let E=0;E<c;E++){let w=E*d-s;g.push(w,-S,0),x.push(0,0,1),m.push(E/a),m.push(1-p/l)}}for(let p=0;p<l;p++)for(let S=0;S<a;S++){let E=S+c*p,w=S+c*(p+1),L=S+1+c*(p+1),T=S+1+c*p;h.push(E,w,T),h.push(w,L,T)}this.setIndex(h),this.setAttribute("position",new gi(g,3)),this.setAttribute("normal",new gi(x,3)),this.setAttribute("uv",new gi(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}},MC=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,EC=`#ifdef USE_ALPHAHASH
	const float ALPHA_HASH_SCALE = 0.05;
	float hash2D( vec2 value ) {
		return fract( 1.0e4 * sin( 17.0 * value.x + 0.1 * value.y ) * ( 0.1 + abs( sin( 13.0 * value.y + value.x ) ) ) );
	}
	float hash3D( vec3 value ) {
		return hash2D( vec2( hash2D( value.xy ), value.z ) );
	}
	float getAlphaHashThreshold( vec3 position ) {
		float maxDeriv = max(
			length( dFdx( position.xyz ) ),
			length( dFdy( position.xyz ) )
		);
		float pixScale = 1.0 / ( ALPHA_HASH_SCALE * maxDeriv );
		vec2 pixScales = vec2(
			exp2( floor( log2( pixScale ) ) ),
			exp2( ceil( log2( pixScale ) ) )
		);
		vec2 alpha = vec2(
			hash3D( floor( pixScales.x * position.xyz ) ),
			hash3D( floor( pixScales.y * position.xyz ) )
		);
		float lerpFactor = fract( log2( pixScale ) );
		float x = ( 1.0 - lerpFactor ) * alpha.x + lerpFactor * alpha.y;
		float a = min( lerpFactor, 1.0 - lerpFactor );
		vec3 cases = vec3(
			x * x / ( 2.0 * a * ( 1.0 - a ) ),
			( x - 0.5 * a ) / ( 1.0 - a ),
			1.0 - ( ( 1.0 - x ) * ( 1.0 - x ) / ( 2.0 * a * ( 1.0 - a ) ) )
		);
		float threshold = ( x < ( 1.0 - a ) )
			? ( ( x < a ) ? cases.x : cases.y )
			: cases.z;
		return clamp( threshold , 1.0e-6, 1.0 );
	}
#endif`,bC=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,wC=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,SC=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,CC=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,DC=`#ifdef USE_AOMAP
	float ambientOcclusion = ( texture2D( aoMap, vAoMapUv ).r - 1.0 ) * aoMapIntensity + 1.0;
	reflectedLight.indirectDiffuse *= ambientOcclusion;
	#if defined( USE_CLEARCOAT ) 
		clearcoatSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_SHEEN ) 
		sheenSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD )
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		reflectedLight.indirectSpecular *= computeSpecularOcclusion( dotNV, ambientOcclusion, material.roughness );
	#endif
#endif`,TC=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,AC=`#ifdef USE_BATCHING
	#if ! defined( GL_ANGLE_multi_draw )
	#define gl_DrawID _gl_DrawID
	uniform int _gl_DrawID;
	#endif
	uniform highp sampler2D batchingTexture;
	uniform highp usampler2D batchingIdTexture;
	mat4 getBatchingMatrix( const in float i ) {
		int size = textureSize( batchingTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( batchingTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( batchingTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( batchingTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( batchingTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
	float getIndirectIndex( const in int i ) {
		int size = textureSize( batchingIdTexture, 0 ).x;
		int x = i % size;
		int y = i / size;
		return float( texelFetch( batchingIdTexture, ivec2( x, y ), 0 ).r );
	}
#endif
#ifdef USE_BATCHING_COLOR
	uniform sampler2D batchingColorTexture;
	vec3 getBatchingColor( const in float i ) {
		int size = textureSize( batchingColorTexture, 0 ).x;
		int j = int( i );
		int x = j % size;
		int y = j / size;
		return texelFetch( batchingColorTexture, ivec2( x, y ), 0 ).rgb;
	}
#endif`,IC=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,RC=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,NC=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,PC=`float G_BlinnPhong_Implicit( ) {
	return 0.25;
}
float D_BlinnPhong( const in float shininess, const in float dotNH ) {
	return RECIPROCAL_PI * ( shininess * 0.5 + 1.0 ) * pow( dotNH, shininess );
}
vec3 BRDF_BlinnPhong( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 specularColor, const in float shininess ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( specularColor, 1.0, dotVH );
	float G = G_BlinnPhong_Implicit( );
	float D = D_BlinnPhong( shininess, dotNH );
	return F * ( G * D );
} // validated`,FC=`#ifdef USE_IRIDESCENCE
	const mat3 XYZ_TO_REC709 = mat3(
		 3.2404542, -0.9692660,  0.0556434,
		-1.5371385,  1.8760108, -0.2040259,
		-0.4985314,  0.0415560,  1.0572252
	);
	vec3 Fresnel0ToIor( vec3 fresnel0 ) {
		vec3 sqrtF0 = sqrt( fresnel0 );
		return ( vec3( 1.0 ) + sqrtF0 ) / ( vec3( 1.0 ) - sqrtF0 );
	}
	vec3 IorToFresnel0( vec3 transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - vec3( incidentIor ) ) / ( transmittedIor + vec3( incidentIor ) ) );
	}
	float IorToFresnel0( float transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - incidentIor ) / ( transmittedIor + incidentIor ));
	}
	vec3 evalSensitivity( float OPD, vec3 shift ) {
		float phase = 2.0 * PI * OPD * 1.0e-9;
		vec3 val = vec3( 5.4856e-13, 4.4201e-13, 5.2481e-13 );
		vec3 pos = vec3( 1.6810e+06, 1.7953e+06, 2.2084e+06 );
		vec3 var = vec3( 4.3278e+09, 9.3046e+09, 6.6121e+09 );
		vec3 xyz = val * sqrt( 2.0 * PI * var ) * cos( pos * phase + shift ) * exp( - pow2( phase ) * var );
		xyz.x += 9.7470e-14 * sqrt( 2.0 * PI * 4.5282e+09 ) * cos( 2.2399e+06 * phase + shift[ 0 ] ) * exp( - 4.5282e+09 * pow2( phase ) );
		xyz /= 1.0685e-7;
		vec3 rgb = XYZ_TO_REC709 * xyz;
		return rgb;
	}
	vec3 evalIridescence( float outsideIOR, float eta2, float cosTheta1, float thinFilmThickness, vec3 baseF0 ) {
		vec3 I;
		float iridescenceIOR = mix( outsideIOR, eta2, smoothstep( 0.0, 0.03, thinFilmThickness ) );
		float sinTheta2Sq = pow2( outsideIOR / iridescenceIOR ) * ( 1.0 - pow2( cosTheta1 ) );
		float cosTheta2Sq = 1.0 - sinTheta2Sq;
		if ( cosTheta2Sq < 0.0 ) {
			return vec3( 1.0 );
		}
		float cosTheta2 = sqrt( cosTheta2Sq );
		float R0 = IorToFresnel0( iridescenceIOR, outsideIOR );
		float R12 = F_Schlick( R0, 1.0, cosTheta1 );
		float T121 = 1.0 - R12;
		float phi12 = 0.0;
		if ( iridescenceIOR < outsideIOR ) phi12 = PI;
		float phi21 = PI - phi12;
		vec3 baseIOR = Fresnel0ToIor( clamp( baseF0, 0.0, 0.9999 ) );		vec3 R1 = IorToFresnel0( baseIOR, iridescenceIOR );
		vec3 R23 = F_Schlick( R1, 1.0, cosTheta2 );
		vec3 phi23 = vec3( 0.0 );
		if ( baseIOR[ 0 ] < iridescenceIOR ) phi23[ 0 ] = PI;
		if ( baseIOR[ 1 ] < iridescenceIOR ) phi23[ 1 ] = PI;
		if ( baseIOR[ 2 ] < iridescenceIOR ) phi23[ 2 ] = PI;
		float OPD = 2.0 * iridescenceIOR * thinFilmThickness * cosTheta2;
		vec3 phi = vec3( phi21 ) + phi23;
		vec3 R123 = clamp( R12 * R23, 1e-5, 0.9999 );
		vec3 r123 = sqrt( R123 );
		vec3 Rs = pow2( T121 ) * R23 / ( vec3( 1.0 ) - R123 );
		vec3 C0 = R12 + Rs;
		I = C0;
		vec3 Cm = Rs - T121;
		for ( int m = 1; m <= 2; ++ m ) {
			Cm *= r123;
			vec3 Sm = 2.0 * evalSensitivity( float( m ) * OPD, float( m ) * phi );
			I += Cm * Sm;
		}
		return max( I, vec3( 0.0 ) );
	}
#endif`,OC=`#ifdef USE_BUMPMAP
	uniform sampler2D bumpMap;
	uniform float bumpScale;
	vec2 dHdxy_fwd() {
		vec2 dSTdx = dFdx( vBumpMapUv );
		vec2 dSTdy = dFdy( vBumpMapUv );
		float Hll = bumpScale * texture2D( bumpMap, vBumpMapUv ).x;
		float dBx = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdx ).x - Hll;
		float dBy = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdy ).x - Hll;
		return vec2( dBx, dBy );
	}
	vec3 perturbNormalArb( vec3 surf_pos, vec3 surf_norm, vec2 dHdxy, float faceDirection ) {
		vec3 vSigmaX = normalize( dFdx( surf_pos.xyz ) );
		vec3 vSigmaY = normalize( dFdy( surf_pos.xyz ) );
		vec3 vN = surf_norm;
		vec3 R1 = cross( vSigmaY, vN );
		vec3 R2 = cross( vN, vSigmaX );
		float fDet = dot( vSigmaX, R1 ) * faceDirection;
		vec3 vGrad = sign( fDet ) * ( dHdxy.x * R1 + dHdxy.y * R2 );
		return normalize( abs( fDet ) * surf_norm - vGrad );
	}
#endif`,LC=`#if NUM_CLIPPING_PLANES > 0
	vec4 plane;
	#ifdef ALPHA_TO_COVERAGE
		float distanceToPlane, distanceGradient;
		float clipOpacity = 1.0;
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
			distanceGradient = fwidth( distanceToPlane ) / 2.0;
			clipOpacity *= smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			if ( clipOpacity == 0.0 ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			float unionClipOpacity = 1.0;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
				distanceGradient = fwidth( distanceToPlane ) / 2.0;
				unionClipOpacity *= 1.0 - smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			}
			#pragma unroll_loop_end
			clipOpacity *= 1.0 - unionClipOpacity;
		#endif
		diffuseColor.a *= clipOpacity;
		if ( diffuseColor.a == 0.0 ) discard;
	#else
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			if ( dot( vClipPosition, plane.xyz ) > plane.w ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			bool clipped = true;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				clipped = ( dot( vClipPosition, plane.xyz ) > plane.w ) && clipped;
			}
			#pragma unroll_loop_end
			if ( clipped ) discard;
		#endif
	#endif
#endif`,kC=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,UC=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,VC=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,BC=`#if defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#elif defined( USE_COLOR )
	diffuseColor.rgb *= vColor;
#endif`,HC=`#if defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#elif defined( USE_COLOR )
	varying vec3 vColor;
#endif`,zC=`#if defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#elif defined( USE_COLOR ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec3 vColor;
#endif`,GC=`#if defined( USE_COLOR_ALPHA )
	vColor = vec4( 1.0 );
#elif defined( USE_COLOR ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	vColor = vec3( 1.0 );
#endif
#ifdef USE_COLOR
	vColor *= color;
#endif
#ifdef USE_INSTANCING_COLOR
	vColor.xyz *= instanceColor.xyz;
#endif
#ifdef USE_BATCHING_COLOR
	vec3 batchingColor = getBatchingColor( getIndirectIndex( gl_DrawID ) );
	vColor.xyz *= batchingColor.xyz;
#endif`,WC=`#define PI 3.141592653589793
#define PI2 6.283185307179586
#define PI_HALF 1.5707963267948966
#define RECIPROCAL_PI 0.3183098861837907
#define RECIPROCAL_PI2 0.15915494309189535
#define EPSILON 1e-6
#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
#define whiteComplement( a ) ( 1.0 - saturate( a ) )
float pow2( const in float x ) { return x*x; }
vec3 pow2( const in vec3 x ) { return x*x; }
float pow3( const in float x ) { return x*x*x; }
float pow4( const in float x ) { float x2 = x*x; return x2*x2; }
float max3( const in vec3 v ) { return max( max( v.x, v.y ), v.z ); }
float average( const in vec3 v ) { return dot( v, vec3( 0.3333333 ) ); }
highp float rand( const in vec2 uv ) {
	const highp float a = 12.9898, b = 78.233, c = 43758.5453;
	highp float dt = dot( uv.xy, vec2( a,b ) ), sn = mod( dt, PI );
	return fract( sin( sn ) * c );
}
#ifdef HIGH_PRECISION
	float precisionSafeLength( vec3 v ) { return length( v ); }
#else
	float precisionSafeLength( vec3 v ) {
		float maxComponent = max3( abs( v ) );
		return length( v / maxComponent ) * maxComponent;
	}
#endif
struct IncidentLight {
	vec3 color;
	vec3 direction;
	bool visible;
};
struct ReflectedLight {
	vec3 directDiffuse;
	vec3 directSpecular;
	vec3 indirectDiffuse;
	vec3 indirectSpecular;
};
#ifdef USE_ALPHAHASH
	varying vec3 vPosition;
#endif
vec3 transformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );
}
vec3 inverseTransformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( vec4( dir, 0.0 ) * matrix ).xyz );
}
mat3 transposeMat3( const in mat3 m ) {
	mat3 tmp;
	tmp[ 0 ] = vec3( m[ 0 ].x, m[ 1 ].x, m[ 2 ].x );
	tmp[ 1 ] = vec3( m[ 0 ].y, m[ 1 ].y, m[ 2 ].y );
	tmp[ 2 ] = vec3( m[ 0 ].z, m[ 1 ].z, m[ 2 ].z );
	return tmp;
}
bool isPerspectiveMatrix( mat4 m ) {
	return m[ 2 ][ 3 ] == - 1.0;
}
vec2 equirectUv( in vec3 dir ) {
	float u = atan( dir.z, dir.x ) * RECIPROCAL_PI2 + 0.5;
	float v = asin( clamp( dir.y, - 1.0, 1.0 ) ) * RECIPROCAL_PI + 0.5;
	return vec2( u, v );
}
vec3 BRDF_Lambert( const in vec3 diffuseColor ) {
	return RECIPROCAL_PI * diffuseColor;
}
vec3 F_Schlick( const in vec3 f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
}
float F_Schlick( const in float f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
} // validated`,jC=`#ifdef ENVMAP_TYPE_CUBE_UV
	#define cubeUV_minMipLevel 4.0
	#define cubeUV_minTileSize 16.0
	float getFace( vec3 direction ) {
		vec3 absDirection = abs( direction );
		float face = - 1.0;
		if ( absDirection.x > absDirection.z ) {
			if ( absDirection.x > absDirection.y )
				face = direction.x > 0.0 ? 0.0 : 3.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		} else {
			if ( absDirection.z > absDirection.y )
				face = direction.z > 0.0 ? 2.0 : 5.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		}
		return face;
	}
	vec2 getUV( vec3 direction, float face ) {
		vec2 uv;
		if ( face == 0.0 ) {
			uv = vec2( direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 1.0 ) {
			uv = vec2( - direction.x, - direction.z ) / abs( direction.y );
		} else if ( face == 2.0 ) {
			uv = vec2( - direction.x, direction.y ) / abs( direction.z );
		} else if ( face == 3.0 ) {
			uv = vec2( - direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 4.0 ) {
			uv = vec2( - direction.x, direction.z ) / abs( direction.y );
		} else {
			uv = vec2( direction.x, direction.y ) / abs( direction.z );
		}
		return 0.5 * ( uv + 1.0 );
	}
	vec3 bilinearCubeUV( sampler2D envMap, vec3 direction, float mipInt ) {
		float face = getFace( direction );
		float filterInt = max( cubeUV_minMipLevel - mipInt, 0.0 );
		mipInt = max( mipInt, cubeUV_minMipLevel );
		float faceSize = exp2( mipInt );
		highp vec2 uv = getUV( direction, face ) * ( faceSize - 2.0 ) + 1.0;
		if ( face > 2.0 ) {
			uv.y += faceSize;
			face -= 3.0;
		}
		uv.x += face * faceSize;
		uv.x += filterInt * 3.0 * cubeUV_minTileSize;
		uv.y += 4.0 * ( exp2( CUBEUV_MAX_MIP ) - faceSize );
		uv.x *= CUBEUV_TEXEL_WIDTH;
		uv.y *= CUBEUV_TEXEL_HEIGHT;
		#ifdef texture2DGradEXT
			return texture2DGradEXT( envMap, uv, vec2( 0.0 ), vec2( 0.0 ) ).rgb;
		#else
			return texture2D( envMap, uv ).rgb;
		#endif
	}
	#define cubeUV_r0 1.0
	#define cubeUV_m0 - 2.0
	#define cubeUV_r1 0.8
	#define cubeUV_m1 - 1.0
	#define cubeUV_r4 0.4
	#define cubeUV_m4 2.0
	#define cubeUV_r5 0.305
	#define cubeUV_m5 3.0
	#define cubeUV_r6 0.21
	#define cubeUV_m6 4.0
	float roughnessToMip( float roughness ) {
		float mip = 0.0;
		if ( roughness >= cubeUV_r1 ) {
			mip = ( cubeUV_r0 - roughness ) * ( cubeUV_m1 - cubeUV_m0 ) / ( cubeUV_r0 - cubeUV_r1 ) + cubeUV_m0;
		} else if ( roughness >= cubeUV_r4 ) {
			mip = ( cubeUV_r1 - roughness ) * ( cubeUV_m4 - cubeUV_m1 ) / ( cubeUV_r1 - cubeUV_r4 ) + cubeUV_m1;
		} else if ( roughness >= cubeUV_r5 ) {
			mip = ( cubeUV_r4 - roughness ) * ( cubeUV_m5 - cubeUV_m4 ) / ( cubeUV_r4 - cubeUV_r5 ) + cubeUV_m4;
		} else if ( roughness >= cubeUV_r6 ) {
			mip = ( cubeUV_r5 - roughness ) * ( cubeUV_m6 - cubeUV_m5 ) / ( cubeUV_r5 - cubeUV_r6 ) + cubeUV_m5;
		} else {
			mip = - 2.0 * log2( 1.16 * roughness );		}
		return mip;
	}
	vec4 textureCubeUV( sampler2D envMap, vec3 sampleDir, float roughness ) {
		float mip = clamp( roughnessToMip( roughness ), cubeUV_m0, CUBEUV_MAX_MIP );
		float mipF = fract( mip );
		float mipInt = floor( mip );
		vec3 color0 = bilinearCubeUV( envMap, sampleDir, mipInt );
		if ( mipF == 0.0 ) {
			return vec4( color0, 1.0 );
		} else {
			vec3 color1 = bilinearCubeUV( envMap, sampleDir, mipInt + 1.0 );
			return vec4( mix( color0, color1, mipF ), 1.0 );
		}
	}
#endif`,$C=`vec3 transformedNormal = objectNormal;
#ifdef USE_TANGENT
	vec3 transformedTangent = objectTangent;
#endif
#ifdef USE_BATCHING
	mat3 bm = mat3( batchingMatrix );
	transformedNormal /= vec3( dot( bm[ 0 ], bm[ 0 ] ), dot( bm[ 1 ], bm[ 1 ] ), dot( bm[ 2 ], bm[ 2 ] ) );
	transformedNormal = bm * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = bm * transformedTangent;
	#endif
#endif
#ifdef USE_INSTANCING
	mat3 im = mat3( instanceMatrix );
	transformedNormal /= vec3( dot( im[ 0 ], im[ 0 ] ), dot( im[ 1 ], im[ 1 ] ), dot( im[ 2 ], im[ 2 ] ) );
	transformedNormal = im * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = im * transformedTangent;
	#endif
#endif
transformedNormal = normalMatrix * transformedNormal;
#ifdef FLIP_SIDED
	transformedNormal = - transformedNormal;
#endif
#ifdef USE_TANGENT
	transformedTangent = ( modelViewMatrix * vec4( transformedTangent, 0.0 ) ).xyz;
	#ifdef FLIP_SIDED
		transformedTangent = - transformedTangent;
	#endif
#endif`,qC=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,XC=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,YC=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,ZC=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,KC="gl_FragColor = linearToOutputTexel( gl_FragColor );",JC=`
const mat3 LINEAR_SRGB_TO_LINEAR_DISPLAY_P3 = mat3(
	vec3( 0.8224621, 0.177538, 0.0 ),
	vec3( 0.0331941, 0.9668058, 0.0 ),
	vec3( 0.0170827, 0.0723974, 0.9105199 )
);
const mat3 LINEAR_DISPLAY_P3_TO_LINEAR_SRGB = mat3(
	vec3( 1.2249401, - 0.2249404, 0.0 ),
	vec3( - 0.0420569, 1.0420571, 0.0 ),
	vec3( - 0.0196376, - 0.0786361, 1.0982735 )
);
vec4 LinearSRGBToLinearDisplayP3( in vec4 value ) {
	return vec4( value.rgb * LINEAR_SRGB_TO_LINEAR_DISPLAY_P3, value.a );
}
vec4 LinearDisplayP3ToLinearSRGB( in vec4 value ) {
	return vec4( value.rgb * LINEAR_DISPLAY_P3_TO_LINEAR_SRGB, value.a );
}
vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,QC=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vec3 cameraToFrag;
		if ( isOrthographic ) {
			cameraToFrag = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToFrag = normalize( vWorldPosition - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vec3 reflectVec = reflect( cameraToFrag, worldNormal );
		#else
			vec3 reflectVec = refract( cameraToFrag, worldNormal, refractionRatio );
		#endif
	#else
		vec3 reflectVec = vReflect;
	#endif
	#ifdef ENVMAP_TYPE_CUBE
		vec4 envColor = textureCube( envMap, envMapRotation * vec3( flipEnvMap * reflectVec.x, reflectVec.yz ) );
	#else
		vec4 envColor = vec4( 0.0 );
	#endif
	#ifdef ENVMAP_BLENDING_MULTIPLY
		outgoingLight = mix( outgoingLight, outgoingLight * envColor.xyz, specularStrength * reflectivity );
	#elif defined( ENVMAP_BLENDING_MIX )
		outgoingLight = mix( outgoingLight, envColor.xyz, specularStrength * reflectivity );
	#elif defined( ENVMAP_BLENDING_ADD )
		outgoingLight += envColor.xyz * specularStrength * reflectivity;
	#endif
#endif`,eD=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform float flipEnvMap;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
	
#endif`,tD=`#ifdef USE_ENVMAP
	uniform float reflectivity;
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		varying vec3 vWorldPosition;
		uniform float refractionRatio;
	#else
		varying vec3 vReflect;
	#endif
#endif`,nD=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,iD=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vWorldPosition = worldPosition.xyz;
	#else
		vec3 cameraToVertex;
		if ( isOrthographic ) {
			cameraToVertex = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToVertex = normalize( worldPosition.xyz - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vReflect = reflect( cameraToVertex, worldNormal );
		#else
			vReflect = refract( cameraToVertex, worldNormal, refractionRatio );
		#endif
	#endif
#endif`,rD=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,sD=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,oD=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,aD=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,lD=`#ifdef USE_GRADIENTMAP
	uniform sampler2D gradientMap;
#endif
vec3 getGradientIrradiance( vec3 normal, vec3 lightDirection ) {
	float dotNL = dot( normal, lightDirection );
	vec2 coord = vec2( dotNL * 0.5 + 0.5, 0.0 );
	#ifdef USE_GRADIENTMAP
		return vec3( texture2D( gradientMap, coord ).r );
	#else
		vec2 fw = fwidth( coord ) * 0.5;
		return mix( vec3( 0.7 ), vec3( 1.0 ), smoothstep( 0.7 - fw.x, 0.7 + fw.x, coord.x ) );
	#endif
}`,cD=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,uD=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,dD=`varying vec3 vViewPosition;
struct LambertMaterial {
	vec3 diffuseColor;
	float specularStrength;
};
void RE_Direct_Lambert( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Lambert( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Lambert
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,fD=`uniform bool receiveShadow;
uniform vec3 ambientLightColor;
#if defined( USE_LIGHT_PROBES )
	uniform vec3 lightProbe[ 9 ];
#endif
vec3 shGetIrradianceAt( in vec3 normal, in vec3 shCoefficients[ 9 ] ) {
	float x = normal.x, y = normal.y, z = normal.z;
	vec3 result = shCoefficients[ 0 ] * 0.886227;
	result += shCoefficients[ 1 ] * 2.0 * 0.511664 * y;
	result += shCoefficients[ 2 ] * 2.0 * 0.511664 * z;
	result += shCoefficients[ 3 ] * 2.0 * 0.511664 * x;
	result += shCoefficients[ 4 ] * 2.0 * 0.429043 * x * y;
	result += shCoefficients[ 5 ] * 2.0 * 0.429043 * y * z;
	result += shCoefficients[ 6 ] * ( 0.743125 * z * z - 0.247708 );
	result += shCoefficients[ 7 ] * 2.0 * 0.429043 * x * z;
	result += shCoefficients[ 8 ] * 0.429043 * ( x * x - y * y );
	return result;
}
vec3 getLightProbeIrradiance( const in vec3 lightProbe[ 9 ], const in vec3 normal ) {
	vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
	vec3 irradiance = shGetIrradianceAt( worldNormal, lightProbe );
	return irradiance;
}
vec3 getAmbientLightIrradiance( const in vec3 ambientLightColor ) {
	vec3 irradiance = ambientLightColor;
	return irradiance;
}
float getDistanceAttenuation( const in float lightDistance, const in float cutoffDistance, const in float decayExponent ) {
	float distanceFalloff = 1.0 / max( pow( lightDistance, decayExponent ), 0.01 );
	if ( cutoffDistance > 0.0 ) {
		distanceFalloff *= pow2( saturate( 1.0 - pow4( lightDistance / cutoffDistance ) ) );
	}
	return distanceFalloff;
}
float getSpotAttenuation( const in float coneCosine, const in float penumbraCosine, const in float angleCosine ) {
	return smoothstep( coneCosine, penumbraCosine, angleCosine );
}
#if NUM_DIR_LIGHTS > 0
	struct DirectionalLight {
		vec3 direction;
		vec3 color;
	};
	uniform DirectionalLight directionalLights[ NUM_DIR_LIGHTS ];
	void getDirectionalLightInfo( const in DirectionalLight directionalLight, out IncidentLight light ) {
		light.color = directionalLight.color;
		light.direction = directionalLight.direction;
		light.visible = true;
	}
#endif
#if NUM_POINT_LIGHTS > 0
	struct PointLight {
		vec3 position;
		vec3 color;
		float distance;
		float decay;
	};
	uniform PointLight pointLights[ NUM_POINT_LIGHTS ];
	void getPointLightInfo( const in PointLight pointLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = pointLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float lightDistance = length( lVector );
		light.color = pointLight.color;
		light.color *= getDistanceAttenuation( lightDistance, pointLight.distance, pointLight.decay );
		light.visible = ( light.color != vec3( 0.0 ) );
	}
#endif
#if NUM_SPOT_LIGHTS > 0
	struct SpotLight {
		vec3 position;
		vec3 direction;
		vec3 color;
		float distance;
		float decay;
		float coneCos;
		float penumbraCos;
	};
	uniform SpotLight spotLights[ NUM_SPOT_LIGHTS ];
	void getSpotLightInfo( const in SpotLight spotLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = spotLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float angleCos = dot( light.direction, spotLight.direction );
		float spotAttenuation = getSpotAttenuation( spotLight.coneCos, spotLight.penumbraCos, angleCos );
		if ( spotAttenuation > 0.0 ) {
			float lightDistance = length( lVector );
			light.color = spotLight.color * spotAttenuation;
			light.color *= getDistanceAttenuation( lightDistance, spotLight.distance, spotLight.decay );
			light.visible = ( light.color != vec3( 0.0 ) );
		} else {
			light.color = vec3( 0.0 );
			light.visible = false;
		}
	}
#endif
#if NUM_RECT_AREA_LIGHTS > 0
	struct RectAreaLight {
		vec3 color;
		vec3 position;
		vec3 halfWidth;
		vec3 halfHeight;
	};
	uniform sampler2D ltc_1;	uniform sampler2D ltc_2;
	uniform RectAreaLight rectAreaLights[ NUM_RECT_AREA_LIGHTS ];
#endif
#if NUM_HEMI_LIGHTS > 0
	struct HemisphereLight {
		vec3 direction;
		vec3 skyColor;
		vec3 groundColor;
	};
	uniform HemisphereLight hemisphereLights[ NUM_HEMI_LIGHTS ];
	vec3 getHemisphereLightIrradiance( const in HemisphereLight hemiLight, const in vec3 normal ) {
		float dotNL = dot( normal, hemiLight.direction );
		float hemiDiffuseWeight = 0.5 * dotNL + 0.5;
		vec3 irradiance = mix( hemiLight.groundColor, hemiLight.skyColor, hemiDiffuseWeight );
		return irradiance;
	}
#endif`,hD=`#ifdef USE_ENVMAP
	vec3 getIBLIrradiance( const in vec3 normal ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * worldNormal, 1.0 );
			return PI * envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	vec3 getIBLRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 reflectVec = reflect( - viewDir, normal );
			reflectVec = normalize( mix( reflectVec, normal, roughness * roughness) );
			reflectVec = inverseTransformDirection( reflectVec, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * reflectVec, roughness );
			return envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	#ifdef USE_ANISOTROPY
		vec3 getIBLAnisotropyRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
			#ifdef ENVMAP_TYPE_CUBE_UV
				vec3 bentNormal = cross( bitangent, viewDir );
				bentNormal = normalize( cross( bentNormal, bitangent ) );
				bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
				return getIBLRadiance( viewDir, bentNormal, roughness );
			#else
				return vec3( 0.0 );
			#endif
		}
	#endif
#endif`,pD=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,mD=`varying vec3 vViewPosition;
struct ToonMaterial {
	vec3 diffuseColor;
};
void RE_Direct_Toon( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 irradiance = getGradientIrradiance( geometryNormal, directLight.direction ) * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Toon( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Toon
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,gD=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,vD=`varying vec3 vViewPosition;
struct BlinnPhongMaterial {
	vec3 diffuseColor;
	vec3 specularColor;
	float specularShininess;
	float specularStrength;
};
void RE_Direct_BlinnPhong( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
	reflectedLight.directSpecular += irradiance * BRDF_BlinnPhong( directLight.direction, geometryViewDir, geometryNormal, material.specularColor, material.specularShininess ) * material.specularStrength;
}
void RE_IndirectDiffuse_BlinnPhong( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_BlinnPhong
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,yD=`PhysicalMaterial material;
material.diffuseColor = diffuseColor.rgb * ( 1.0 - metalnessFactor );
vec3 dxy = max( abs( dFdx( nonPerturbedNormal ) ), abs( dFdy( nonPerturbedNormal ) ) );
float geometryRoughness = max( max( dxy.x, dxy.y ), dxy.z );
material.roughness = max( roughnessFactor, 0.0525 );material.roughness += geometryRoughness;
material.roughness = min( material.roughness, 1.0 );
#ifdef IOR
	material.ior = ior;
	#ifdef USE_SPECULAR
		float specularIntensityFactor = specularIntensity;
		vec3 specularColorFactor = specularColor;
		#ifdef USE_SPECULAR_COLORMAP
			specularColorFactor *= texture2D( specularColorMap, vSpecularColorMapUv ).rgb;
		#endif
		#ifdef USE_SPECULAR_INTENSITYMAP
			specularIntensityFactor *= texture2D( specularIntensityMap, vSpecularIntensityMapUv ).a;
		#endif
		material.specularF90 = mix( specularIntensityFactor, 1.0, metalnessFactor );
	#else
		float specularIntensityFactor = 1.0;
		vec3 specularColorFactor = vec3( 1.0 );
		material.specularF90 = 1.0;
	#endif
	material.specularColor = mix( min( pow2( ( material.ior - 1.0 ) / ( material.ior + 1.0 ) ) * specularColorFactor, vec3( 1.0 ) ) * specularIntensityFactor, diffuseColor.rgb, metalnessFactor );
#else
	material.specularColor = mix( vec3( 0.04 ), diffuseColor.rgb, metalnessFactor );
	material.specularF90 = 1.0;
#endif
#ifdef USE_CLEARCOAT
	material.clearcoat = clearcoat;
	material.clearcoatRoughness = clearcoatRoughness;
	material.clearcoatF0 = vec3( 0.04 );
	material.clearcoatF90 = 1.0;
	#ifdef USE_CLEARCOATMAP
		material.clearcoat *= texture2D( clearcoatMap, vClearcoatMapUv ).x;
	#endif
	#ifdef USE_CLEARCOAT_ROUGHNESSMAP
		material.clearcoatRoughness *= texture2D( clearcoatRoughnessMap, vClearcoatRoughnessMapUv ).y;
	#endif
	material.clearcoat = saturate( material.clearcoat );	material.clearcoatRoughness = max( material.clearcoatRoughness, 0.0525 );
	material.clearcoatRoughness += geometryRoughness;
	material.clearcoatRoughness = min( material.clearcoatRoughness, 1.0 );
#endif
#ifdef USE_DISPERSION
	material.dispersion = dispersion;
#endif
#ifdef USE_IRIDESCENCE
	material.iridescence = iridescence;
	material.iridescenceIOR = iridescenceIOR;
	#ifdef USE_IRIDESCENCEMAP
		material.iridescence *= texture2D( iridescenceMap, vIridescenceMapUv ).r;
	#endif
	#ifdef USE_IRIDESCENCE_THICKNESSMAP
		material.iridescenceThickness = (iridescenceThicknessMaximum - iridescenceThicknessMinimum) * texture2D( iridescenceThicknessMap, vIridescenceThicknessMapUv ).g + iridescenceThicknessMinimum;
	#else
		material.iridescenceThickness = iridescenceThicknessMaximum;
	#endif
#endif
#ifdef USE_SHEEN
	material.sheenColor = sheenColor;
	#ifdef USE_SHEEN_COLORMAP
		material.sheenColor *= texture2D( sheenColorMap, vSheenColorMapUv ).rgb;
	#endif
	material.sheenRoughness = clamp( sheenRoughness, 0.07, 1.0 );
	#ifdef USE_SHEEN_ROUGHNESSMAP
		material.sheenRoughness *= texture2D( sheenRoughnessMap, vSheenRoughnessMapUv ).a;
	#endif
#endif
#ifdef USE_ANISOTROPY
	#ifdef USE_ANISOTROPYMAP
		mat2 anisotropyMat = mat2( anisotropyVector.x, anisotropyVector.y, - anisotropyVector.y, anisotropyVector.x );
		vec3 anisotropyPolar = texture2D( anisotropyMap, vAnisotropyMapUv ).rgb;
		vec2 anisotropyV = anisotropyMat * normalize( 2.0 * anisotropyPolar.rg - vec2( 1.0 ) ) * anisotropyPolar.b;
	#else
		vec2 anisotropyV = anisotropyVector;
	#endif
	material.anisotropy = length( anisotropyV );
	if( material.anisotropy == 0.0 ) {
		anisotropyV = vec2( 1.0, 0.0 );
	} else {
		anisotropyV /= material.anisotropy;
		material.anisotropy = saturate( material.anisotropy );
	}
	material.alphaT = mix( pow2( material.roughness ), 1.0, pow2( material.anisotropy ) );
	material.anisotropyT = tbn[ 0 ] * anisotropyV.x + tbn[ 1 ] * anisotropyV.y;
	material.anisotropyB = tbn[ 1 ] * anisotropyV.x - tbn[ 0 ] * anisotropyV.y;
#endif`,_D=`struct PhysicalMaterial {
	vec3 diffuseColor;
	float roughness;
	vec3 specularColor;
	float specularF90;
	float dispersion;
	#ifdef USE_CLEARCOAT
		float clearcoat;
		float clearcoatRoughness;
		vec3 clearcoatF0;
		float clearcoatF90;
	#endif
	#ifdef USE_IRIDESCENCE
		float iridescence;
		float iridescenceIOR;
		float iridescenceThickness;
		vec3 iridescenceFresnel;
		vec3 iridescenceF0;
	#endif
	#ifdef USE_SHEEN
		vec3 sheenColor;
		float sheenRoughness;
	#endif
	#ifdef IOR
		float ior;
	#endif
	#ifdef USE_TRANSMISSION
		float transmission;
		float transmissionAlpha;
		float thickness;
		float attenuationDistance;
		vec3 attenuationColor;
	#endif
	#ifdef USE_ANISOTROPY
		float anisotropy;
		float alphaT;
		vec3 anisotropyT;
		vec3 anisotropyB;
	#endif
};
vec3 clearcoatSpecularDirect = vec3( 0.0 );
vec3 clearcoatSpecularIndirect = vec3( 0.0 );
vec3 sheenSpecularDirect = vec3( 0.0 );
vec3 sheenSpecularIndirect = vec3(0.0 );
vec3 Schlick_to_F0( const in vec3 f, const in float f90, const in float dotVH ) {
    float x = clamp( 1.0 - dotVH, 0.0, 1.0 );
    float x2 = x * x;
    float x5 = clamp( x * x2 * x2, 0.0, 0.9999 );
    return ( f - vec3( f90 ) * x5 ) / ( 1.0 - x5 );
}
float V_GGX_SmithCorrelated( const in float alpha, const in float dotNL, const in float dotNV ) {
	float a2 = pow2( alpha );
	float gv = dotNL * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNV ) );
	float gl = dotNV * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNL ) );
	return 0.5 / max( gv + gl, EPSILON );
}
float D_GGX( const in float alpha, const in float dotNH ) {
	float a2 = pow2( alpha );
	float denom = pow2( dotNH ) * ( a2 - 1.0 ) + 1.0;
	return RECIPROCAL_PI * a2 / pow2( denom );
}
#ifdef USE_ANISOTROPY
	float V_GGX_SmithCorrelated_Anisotropic( const in float alphaT, const in float alphaB, const in float dotTV, const in float dotBV, const in float dotTL, const in float dotBL, const in float dotNV, const in float dotNL ) {
		float gv = dotNL * length( vec3( alphaT * dotTV, alphaB * dotBV, dotNV ) );
		float gl = dotNV * length( vec3( alphaT * dotTL, alphaB * dotBL, dotNL ) );
		float v = 0.5 / ( gv + gl );
		return saturate(v);
	}
	float D_GGX_Anisotropic( const in float alphaT, const in float alphaB, const in float dotNH, const in float dotTH, const in float dotBH ) {
		float a2 = alphaT * alphaB;
		highp vec3 v = vec3( alphaB * dotTH, alphaT * dotBH, a2 * dotNH );
		highp float v2 = dot( v, v );
		float w2 = a2 / v2;
		return RECIPROCAL_PI * a2 * pow2 ( w2 );
	}
#endif
#ifdef USE_CLEARCOAT
	vec3 BRDF_GGX_Clearcoat( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material) {
		vec3 f0 = material.clearcoatF0;
		float f90 = material.clearcoatF90;
		float roughness = material.clearcoatRoughness;
		float alpha = pow2( roughness );
		vec3 halfDir = normalize( lightDir + viewDir );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float dotNH = saturate( dot( normal, halfDir ) );
		float dotVH = saturate( dot( viewDir, halfDir ) );
		vec3 F = F_Schlick( f0, f90, dotVH );
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
		return F * ( V * D );
	}
#endif
vec3 BRDF_GGX( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 f0 = material.specularColor;
	float f90 = material.specularF90;
	float roughness = material.roughness;
	float alpha = pow2( roughness );
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( f0, f90, dotVH );
	#ifdef USE_IRIDESCENCE
		F = mix( F, material.iridescenceFresnel, material.iridescence );
	#endif
	#ifdef USE_ANISOTROPY
		float dotTL = dot( material.anisotropyT, lightDir );
		float dotTV = dot( material.anisotropyT, viewDir );
		float dotTH = dot( material.anisotropyT, halfDir );
		float dotBL = dot( material.anisotropyB, lightDir );
		float dotBV = dot( material.anisotropyB, viewDir );
		float dotBH = dot( material.anisotropyB, halfDir );
		float V = V_GGX_SmithCorrelated_Anisotropic( material.alphaT, alpha, dotTV, dotBV, dotTL, dotBL, dotNV, dotNL );
		float D = D_GGX_Anisotropic( material.alphaT, alpha, dotNH, dotTH, dotBH );
	#else
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
	#endif
	return F * ( V * D );
}
vec2 LTC_Uv( const in vec3 N, const in vec3 V, const in float roughness ) {
	const float LUT_SIZE = 64.0;
	const float LUT_SCALE = ( LUT_SIZE - 1.0 ) / LUT_SIZE;
	const float LUT_BIAS = 0.5 / LUT_SIZE;
	float dotNV = saturate( dot( N, V ) );
	vec2 uv = vec2( roughness, sqrt( 1.0 - dotNV ) );
	uv = uv * LUT_SCALE + LUT_BIAS;
	return uv;
}
float LTC_ClippedSphereFormFactor( const in vec3 f ) {
	float l = length( f );
	return max( ( l * l + f.z ) / ( l + 1.0 ), 0.0 );
}
vec3 LTC_EdgeVectorFormFactor( const in vec3 v1, const in vec3 v2 ) {
	float x = dot( v1, v2 );
	float y = abs( x );
	float a = 0.8543985 + ( 0.4965155 + 0.0145206 * y ) * y;
	float b = 3.4175940 + ( 4.1616724 + y ) * y;
	float v = a / b;
	float theta_sintheta = ( x > 0.0 ) ? v : 0.5 * inversesqrt( max( 1.0 - x * x, 1e-7 ) ) - v;
	return cross( v1, v2 ) * theta_sintheta;
}
vec3 LTC_Evaluate( const in vec3 N, const in vec3 V, const in vec3 P, const in mat3 mInv, const in vec3 rectCoords[ 4 ] ) {
	vec3 v1 = rectCoords[ 1 ] - rectCoords[ 0 ];
	vec3 v2 = rectCoords[ 3 ] - rectCoords[ 0 ];
	vec3 lightNormal = cross( v1, v2 );
	if( dot( lightNormal, P - rectCoords[ 0 ] ) < 0.0 ) return vec3( 0.0 );
	vec3 T1, T2;
	T1 = normalize( V - N * dot( V, N ) );
	T2 = - cross( N, T1 );
	mat3 mat = mInv * transposeMat3( mat3( T1, T2, N ) );
	vec3 coords[ 4 ];
	coords[ 0 ] = mat * ( rectCoords[ 0 ] - P );
	coords[ 1 ] = mat * ( rectCoords[ 1 ] - P );
	coords[ 2 ] = mat * ( rectCoords[ 2 ] - P );
	coords[ 3 ] = mat * ( rectCoords[ 3 ] - P );
	coords[ 0 ] = normalize( coords[ 0 ] );
	coords[ 1 ] = normalize( coords[ 1 ] );
	coords[ 2 ] = normalize( coords[ 2 ] );
	coords[ 3 ] = normalize( coords[ 3 ] );
	vec3 vectorFormFactor = vec3( 0.0 );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 0 ], coords[ 1 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 1 ], coords[ 2 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 2 ], coords[ 3 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 3 ], coords[ 0 ] );
	float result = LTC_ClippedSphereFormFactor( vectorFormFactor );
	return vec3( result );
}
#if defined( USE_SHEEN )
float D_Charlie( float roughness, float dotNH ) {
	float alpha = pow2( roughness );
	float invAlpha = 1.0 / alpha;
	float cos2h = dotNH * dotNH;
	float sin2h = max( 1.0 - cos2h, 0.0078125 );
	return ( 2.0 + invAlpha ) * pow( sin2h, invAlpha * 0.5 ) / ( 2.0 * PI );
}
float V_Neubelt( float dotNV, float dotNL ) {
	return saturate( 1.0 / ( 4.0 * ( dotNL + dotNV - dotNL * dotNV ) ) );
}
vec3 BRDF_Sheen( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, vec3 sheenColor, const in float sheenRoughness ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float D = D_Charlie( sheenRoughness, dotNH );
	float V = V_Neubelt( dotNV, dotNL );
	return sheenColor * ( D * V );
}
#endif
float IBLSheenBRDF( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	float r2 = roughness * roughness;
	float a = roughness < 0.25 ? -339.2 * r2 + 161.4 * roughness - 25.9 : -8.48 * r2 + 14.3 * roughness - 9.95;
	float b = roughness < 0.25 ? 44.0 * r2 - 23.7 * roughness + 3.26 : 1.97 * r2 - 3.27 * roughness + 0.72;
	float DG = exp( a * dotNV + b ) + ( roughness < 0.25 ? 0.0 : 0.1 * ( roughness - 0.25 ) );
	return saturate( DG * RECIPROCAL_PI );
}
vec2 DFGApprox( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	const vec4 c0 = vec4( - 1, - 0.0275, - 0.572, 0.022 );
	const vec4 c1 = vec4( 1, 0.0425, 1.04, - 0.04 );
	vec4 r = roughness * c0 + c1;
	float a004 = min( r.x * r.x, exp2( - 9.28 * dotNV ) ) * r.x + r.y;
	vec2 fab = vec2( - 1.04, 1.04 ) * a004 + r.zw;
	return fab;
}
vec3 EnvironmentBRDF( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness ) {
	vec2 fab = DFGApprox( normal, viewDir, roughness );
	return specularColor * fab.x + specularF90 * fab.y;
}
#ifdef USE_IRIDESCENCE
void computeMultiscatteringIridescence( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float iridescence, const in vec3 iridescenceF0, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#else
void computeMultiscattering( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#endif
	vec2 fab = DFGApprox( normal, viewDir, roughness );
	#ifdef USE_IRIDESCENCE
		vec3 Fr = mix( specularColor, iridescenceF0, iridescence );
	#else
		vec3 Fr = specularColor;
	#endif
	vec3 FssEss = Fr * fab.x + specularF90 * fab.y;
	float Ess = fab.x + fab.y;
	float Ems = 1.0 - Ess;
	vec3 Favg = Fr + ( 1.0 - Fr ) * 0.047619;	vec3 Fms = FssEss * Favg / ( 1.0 - Ems * Favg );
	singleScatter += FssEss;
	multiScatter += Fms * Ems;
}
#if NUM_RECT_AREA_LIGHTS > 0
	void RE_Direct_RectArea_Physical( const in RectAreaLight rectAreaLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
		vec3 normal = geometryNormal;
		vec3 viewDir = geometryViewDir;
		vec3 position = geometryPosition;
		vec3 lightPos = rectAreaLight.position;
		vec3 halfWidth = rectAreaLight.halfWidth;
		vec3 halfHeight = rectAreaLight.halfHeight;
		vec3 lightColor = rectAreaLight.color;
		float roughness = material.roughness;
		vec3 rectCoords[ 4 ];
		rectCoords[ 0 ] = lightPos + halfWidth - halfHeight;		rectCoords[ 1 ] = lightPos - halfWidth - halfHeight;
		rectCoords[ 2 ] = lightPos - halfWidth + halfHeight;
		rectCoords[ 3 ] = lightPos + halfWidth + halfHeight;
		vec2 uv = LTC_Uv( normal, viewDir, roughness );
		vec4 t1 = texture2D( ltc_1, uv );
		vec4 t2 = texture2D( ltc_2, uv );
		mat3 mInv = mat3(
			vec3( t1.x, 0, t1.y ),
			vec3(    0, 1,    0 ),
			vec3( t1.z, 0, t1.w )
		);
		vec3 fresnel = ( material.specularColor * t2.x + ( vec3( 1.0 ) - material.specularColor ) * t2.y );
		reflectedLight.directSpecular += lightColor * fresnel * LTC_Evaluate( normal, viewDir, position, mInv, rectCoords );
		reflectedLight.directDiffuse += lightColor * material.diffuseColor * LTC_Evaluate( normal, viewDir, position, mat3( 1.0 ), rectCoords );
	}
#endif
void RE_Direct_Physical( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	#ifdef USE_CLEARCOAT
		float dotNLcc = saturate( dot( geometryClearcoatNormal, directLight.direction ) );
		vec3 ccIrradiance = dotNLcc * directLight.color;
		clearcoatSpecularDirect += ccIrradiance * BRDF_GGX_Clearcoat( directLight.direction, geometryViewDir, geometryClearcoatNormal, material );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularDirect += irradiance * BRDF_Sheen( directLight.direction, geometryViewDir, geometryNormal, material.sheenColor, material.sheenRoughness );
	#endif
	reflectedLight.directSpecular += irradiance * BRDF_GGX( directLight.direction, geometryViewDir, geometryNormal, material );
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Physical( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectSpecular_Physical( const in vec3 radiance, const in vec3 irradiance, const in vec3 clearcoatRadiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight) {
	#ifdef USE_CLEARCOAT
		clearcoatSpecularIndirect += clearcoatRadiance * EnvironmentBRDF( geometryClearcoatNormal, geometryViewDir, material.clearcoatF0, material.clearcoatF90, material.clearcoatRoughness );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularIndirect += irradiance * material.sheenColor * IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
	#endif
	vec3 singleScattering = vec3( 0.0 );
	vec3 multiScattering = vec3( 0.0 );
	vec3 cosineWeightedIrradiance = irradiance * RECIPROCAL_PI;
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.iridescence, material.iridescenceFresnel, material.roughness, singleScattering, multiScattering );
	#else
		computeMultiscattering( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.roughness, singleScattering, multiScattering );
	#endif
	vec3 totalScattering = singleScattering + multiScattering;
	vec3 diffuse = material.diffuseColor * ( 1.0 - max( max( totalScattering.r, totalScattering.g ), totalScattering.b ) );
	reflectedLight.indirectSpecular += radiance * singleScattering;
	reflectedLight.indirectSpecular += multiScattering * cosineWeightedIrradiance;
	reflectedLight.indirectDiffuse += diffuse * cosineWeightedIrradiance;
}
#define RE_Direct				RE_Direct_Physical
#define RE_Direct_RectArea		RE_Direct_RectArea_Physical
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Physical
#define RE_IndirectSpecular		RE_IndirectSpecular_Physical
float computeSpecularOcclusion( const in float dotNV, const in float ambientOcclusion, const in float roughness ) {
	return saturate( pow( dotNV + ambientOcclusion, exp2( - 16.0 * roughness - 1.0 ) ) - 1.0 + ambientOcclusion );
}`,xD=`
vec3 geometryPosition = - vViewPosition;
vec3 geometryNormal = normal;
vec3 geometryViewDir = ( isOrthographic ) ? vec3( 0, 0, 1 ) : normalize( vViewPosition );
vec3 geometryClearcoatNormal = vec3( 0.0 );
#ifdef USE_CLEARCOAT
	geometryClearcoatNormal = clearcoatNormal;
#endif
#ifdef USE_IRIDESCENCE
	float dotNVi = saturate( dot( normal, geometryViewDir ) );
	if ( material.iridescenceThickness == 0.0 ) {
		material.iridescence = 0.0;
	} else {
		material.iridescence = saturate( material.iridescence );
	}
	if ( material.iridescence > 0.0 ) {
		material.iridescenceFresnel = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.specularColor );
		material.iridescenceF0 = Schlick_to_F0( material.iridescenceFresnel, 1.0, dotNVi );
	}
#endif
IncidentLight directLight;
#if ( NUM_POINT_LIGHTS > 0 ) && defined( RE_Direct )
	PointLight pointLight;
	#if defined( USE_SHADOWMAP ) && NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHTS; i ++ ) {
		pointLight = pointLights[ i ];
		getPointLightInfo( pointLight, geometryPosition, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_POINT_LIGHT_SHADOWS )
		pointLightShadow = pointLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getPointShadow( pointShadowMap[ i ], pointLightShadow.shadowMapSize, pointLightShadow.shadowIntensity, pointLightShadow.shadowBias, pointLightShadow.shadowRadius, vPointShadowCoord[ i ], pointLightShadow.shadowCameraNear, pointLightShadow.shadowCameraFar ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SPOT_LIGHTS > 0 ) && defined( RE_Direct )
	SpotLight spotLight;
	vec4 spotColor;
	vec3 spotLightCoord;
	bool inSpotLightMap;
	#if defined( USE_SHADOWMAP ) && NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHTS; i ++ ) {
		spotLight = spotLights[ i ];
		getSpotLightInfo( spotLight, geometryPosition, directLight );
		#if ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#define SPOT_LIGHT_MAP_INDEX UNROLLED_LOOP_INDEX
		#elif ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		#define SPOT_LIGHT_MAP_INDEX NUM_SPOT_LIGHT_MAPS
		#else
		#define SPOT_LIGHT_MAP_INDEX ( UNROLLED_LOOP_INDEX - NUM_SPOT_LIGHT_SHADOWS + NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#endif
		#if ( SPOT_LIGHT_MAP_INDEX < NUM_SPOT_LIGHT_MAPS )
			spotLightCoord = vSpotLightCoord[ i ].xyz / vSpotLightCoord[ i ].w;
			inSpotLightMap = all( lessThan( abs( spotLightCoord * 2. - 1. ), vec3( 1.0 ) ) );
			spotColor = texture2D( spotLightMap[ SPOT_LIGHT_MAP_INDEX ], spotLightCoord.xy );
			directLight.color = inSpotLightMap ? directLight.color * spotColor.rgb : directLight.color;
		#endif
		#undef SPOT_LIGHT_MAP_INDEX
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		spotLightShadow = spotLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( spotShadowMap[ i ], spotLightShadow.shadowMapSize, spotLightShadow.shadowIntensity, spotLightShadow.shadowBias, spotLightShadow.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_DIR_LIGHTS > 0 ) && defined( RE_Direct )
	DirectionalLight directionalLight;
	#if defined( USE_SHADOWMAP ) && NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHTS; i ++ ) {
		directionalLight = directionalLights[ i ];
		getDirectionalLightInfo( directionalLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_DIR_LIGHT_SHADOWS )
		directionalLightShadow = directionalLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( directionalShadowMap[ i ], directionalLightShadow.shadowMapSize, directionalLightShadow.shadowIntensity, directionalLightShadow.shadowBias, directionalLightShadow.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_RECT_AREA_LIGHTS > 0 ) && defined( RE_Direct_RectArea )
	RectAreaLight rectAreaLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_RECT_AREA_LIGHTS; i ++ ) {
		rectAreaLight = rectAreaLights[ i ];
		RE_Direct_RectArea( rectAreaLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if defined( RE_IndirectDiffuse )
	vec3 iblIrradiance = vec3( 0.0 );
	vec3 irradiance = getAmbientLightIrradiance( ambientLightColor );
	#if defined( USE_LIGHT_PROBES )
		irradiance += getLightProbeIrradiance( lightProbe, geometryNormal );
	#endif
	#if ( NUM_HEMI_LIGHTS > 0 )
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_HEMI_LIGHTS; i ++ ) {
			irradiance += getHemisphereLightIrradiance( hemisphereLights[ i ], geometryNormal );
		}
		#pragma unroll_loop_end
	#endif
#endif
#if defined( RE_IndirectSpecular )
	vec3 radiance = vec3( 0.0 );
	vec3 clearcoatRadiance = vec3( 0.0 );
#endif`,MD=`#if defined( RE_IndirectDiffuse )
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		vec3 lightMapIrradiance = lightMapTexel.rgb * lightMapIntensity;
		irradiance += lightMapIrradiance;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD ) && defined( ENVMAP_TYPE_CUBE_UV )
		iblIrradiance += getIBLIrradiance( geometryNormal );
	#endif
#endif
#if defined( USE_ENVMAP ) && defined( RE_IndirectSpecular )
	#ifdef USE_ANISOTROPY
		radiance += getIBLAnisotropyRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
	#else
		radiance += getIBLRadiance( geometryViewDir, geometryNormal, material.roughness );
	#endif
	#ifdef USE_CLEARCOAT
		clearcoatRadiance += getIBLRadiance( geometryViewDir, geometryClearcoatNormal, material.clearcoatRoughness );
	#endif
#endif`,ED=`#if defined( RE_IndirectDiffuse )
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,bD=`#if defined( USE_LOGDEPTHBUF )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,wD=`#if defined( USE_LOGDEPTHBUF )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,SD=`#ifdef USE_LOGDEPTHBUF
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,CD=`#ifdef USE_LOGDEPTHBUF
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,DD=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = vec4( mix( pow( sampledDiffuseColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), sampledDiffuseColor.rgb * 0.0773993808, vec3( lessThanEqual( sampledDiffuseColor.rgb, vec3( 0.04045 ) ) ) ), sampledDiffuseColor.w );
	
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,TD=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,AD=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
	#if defined( USE_POINTS_UV )
		vec2 uv = vUv;
	#else
		vec2 uv = ( uvTransform * vec3( gl_PointCoord.x, 1.0 - gl_PointCoord.y, 1 ) ).xy;
	#endif
#endif
#ifdef USE_MAP
	diffuseColor *= texture2D( map, uv );
#endif
#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, uv ).g;
#endif`,ID=`#if defined( USE_POINTS_UV )
	varying vec2 vUv;
#else
	#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
		uniform mat3 uvTransform;
	#endif
#endif
#ifdef USE_MAP
	uniform sampler2D map;
#endif
#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,RD=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,ND=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,PD=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,FD=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,OD=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,LD=`#ifdef USE_MORPHTARGETS
	#ifndef USE_INSTANCING_MORPH
		uniform float morphTargetBaseInfluence;
		uniform float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	#endif
	uniform sampler2DArray morphTargetsTexture;
	uniform ivec2 morphTargetsTextureSize;
	vec4 getMorph( const in int vertexIndex, const in int morphTargetIndex, const in int offset ) {
		int texelIndex = vertexIndex * MORPHTARGETS_TEXTURE_STRIDE + offset;
		int y = texelIndex / morphTargetsTextureSize.x;
		int x = texelIndex - y * morphTargetsTextureSize.x;
		ivec3 morphUV = ivec3( x, y, morphTargetIndex );
		return texelFetch( morphTargetsTexture, morphUV, 0 );
	}
#endif`,kD=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,UD=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
#ifdef FLAT_SHADED
	vec3 fdx = dFdx( vViewPosition );
	vec3 fdy = dFdy( vViewPosition );
	vec3 normal = normalize( cross( fdx, fdy ) );
#else
	vec3 normal = normalize( vNormal );
	#ifdef DOUBLE_SIDED
		normal *= faceDirection;
	#endif
#endif
#if defined( USE_NORMALMAP_TANGENTSPACE ) || defined( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY )
	#ifdef USE_TANGENT
		mat3 tbn = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn = getTangentFrame( - vViewPosition, normal,
		#if defined( USE_NORMALMAP )
			vNormalMapUv
		#elif defined( USE_CLEARCOAT_NORMALMAP )
			vClearcoatNormalMapUv
		#else
			vUv
		#endif
		);
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn[0] *= faceDirection;
		tbn[1] *= faceDirection;
	#endif
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	#ifdef USE_TANGENT
		mat3 tbn2 = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn2 = getTangentFrame( - vViewPosition, normal, vClearcoatNormalMapUv );
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn2[0] *= faceDirection;
		tbn2[1] *= faceDirection;
	#endif
#endif
vec3 nonPerturbedNormal = normal;`,VD=`#ifdef USE_NORMALMAP_OBJECTSPACE
	normal = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#ifdef FLIP_SIDED
		normal = - normal;
	#endif
	#ifdef DOUBLE_SIDED
		normal = normal * faceDirection;
	#endif
	normal = normalize( normalMatrix * normal );
#elif defined( USE_NORMALMAP_TANGENTSPACE )
	vec3 mapN = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	mapN.xy *= normalScale;
	normal = normalize( tbn * mapN );
#elif defined( USE_BUMPMAP )
	normal = perturbNormalArb( - vViewPosition, normal, dHdxy_fwd(), faceDirection );
#endif`,BD=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,HD=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,zD=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,GD=`#ifdef USE_NORMALMAP
	uniform sampler2D normalMap;
	uniform vec2 normalScale;
#endif
#ifdef USE_NORMALMAP_OBJECTSPACE
	uniform mat3 normalMatrix;
#endif
#if ! defined ( USE_TANGENT ) && ( defined ( USE_NORMALMAP_TANGENTSPACE ) || defined ( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY ) )
	mat3 getTangentFrame( vec3 eye_pos, vec3 surf_norm, vec2 uv ) {
		vec3 q0 = dFdx( eye_pos.xyz );
		vec3 q1 = dFdy( eye_pos.xyz );
		vec2 st0 = dFdx( uv.st );
		vec2 st1 = dFdy( uv.st );
		vec3 N = surf_norm;
		vec3 q1perp = cross( q1, N );
		vec3 q0perp = cross( N, q0 );
		vec3 T = q1perp * st0.x + q0perp * st1.x;
		vec3 B = q1perp * st0.y + q0perp * st1.y;
		float det = max( dot( T, T ), dot( B, B ) );
		float scale = ( det == 0.0 ) ? 0.0 : inversesqrt( det );
		return mat3( T * scale, B * scale, N );
	}
#endif`,WD=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,jD=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,$D=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,qD=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,XD=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,YD=`vec3 packNormalToRGB( const in vec3 normal ) {
	return normalize( normal ) * 0.5 + 0.5;
}
vec3 unpackRGBToNormal( const in vec3 rgb ) {
	return 2.0 * rgb.xyz - 1.0;
}
const float PackUpscale = 256. / 255.;const float UnpackDownscale = 255. / 256.;const float ShiftRight8 = 1. / 256.;
const float Inv255 = 1. / 255.;
const vec4 PackFactors = vec4( 1.0, 256.0, 256.0 * 256.0, 256.0 * 256.0 * 256.0 );
const vec2 UnpackFactors2 = vec2( UnpackDownscale, 1.0 / PackFactors.g );
const vec3 UnpackFactors3 = vec3( UnpackDownscale / PackFactors.rg, 1.0 / PackFactors.b );
const vec4 UnpackFactors4 = vec4( UnpackDownscale / PackFactors.rgb, 1.0 / PackFactors.a );
vec4 packDepthToRGBA( const in float v ) {
	if( v <= 0.0 )
		return vec4( 0., 0., 0., 0. );
	if( v >= 1.0 )
		return vec4( 1., 1., 1., 1. );
	float vuf;
	float af = modf( v * PackFactors.a, vuf );
	float bf = modf( vuf * ShiftRight8, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec4( vuf * Inv255, gf * PackUpscale, bf * PackUpscale, af );
}
vec3 packDepthToRGB( const in float v ) {
	if( v <= 0.0 )
		return vec3( 0., 0., 0. );
	if( v >= 1.0 )
		return vec3( 1., 1., 1. );
	float vuf;
	float bf = modf( v * PackFactors.b, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec3( vuf * Inv255, gf * PackUpscale, bf );
}
vec2 packDepthToRG( const in float v ) {
	if( v <= 0.0 )
		return vec2( 0., 0. );
	if( v >= 1.0 )
		return vec2( 1., 1. );
	float vuf;
	float gf = modf( v * 256., vuf );
	return vec2( vuf * Inv255, gf );
}
float unpackRGBAToDepth( const in vec4 v ) {
	return dot( v, UnpackFactors4 );
}
float unpackRGBToDepth( const in vec3 v ) {
	return dot( v, UnpackFactors3 );
}
float unpackRGToDepth( const in vec2 v ) {
	return v.r * UnpackFactors2.r + v.g * UnpackFactors2.g;
}
vec4 pack2HalfToRGBA( const in vec2 v ) {
	vec4 r = vec4( v.x, fract( v.x * 255.0 ), v.y, fract( v.y * 255.0 ) );
	return vec4( r.x - r.y / 255.0, r.y, r.z - r.w / 255.0, r.w );
}
vec2 unpackRGBATo2Half( const in vec4 v ) {
	return vec2( v.x + ( v.y / 255.0 ), v.z + ( v.w / 255.0 ) );
}
float viewZToOrthographicDepth( const in float viewZ, const in float near, const in float far ) {
	return ( viewZ + near ) / ( near - far );
}
float orthographicDepthToViewZ( const in float depth, const in float near, const in float far ) {
	return depth * ( near - far ) - near;
}
float viewZToPerspectiveDepth( const in float viewZ, const in float near, const in float far ) {
	return ( ( near + viewZ ) * far ) / ( ( far - near ) * viewZ );
}
float perspectiveDepthToViewZ( const in float depth, const in float near, const in float far ) {
	return ( near * far ) / ( ( far - near ) * depth - far );
}`,ZD=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,KD=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,JD=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,QD=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,eT=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,tT=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,nT=`#if NUM_SPOT_LIGHT_COORDS > 0
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#if NUM_SPOT_LIGHT_MAPS > 0
	uniform sampler2D spotLightMap[ NUM_SPOT_LIGHT_MAPS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform sampler2D directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		uniform sampler2D spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform sampler2D pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
	float texture2DCompare( sampler2D depths, vec2 uv, float compare ) {
		return step( compare, unpackRGBAToDepth( texture2D( depths, uv ) ) );
	}
	vec2 texture2DDistribution( sampler2D shadow, vec2 uv ) {
		return unpackRGBATo2Half( texture2D( shadow, uv ) );
	}
	float VSMShadow (sampler2D shadow, vec2 uv, float compare ){
		float occlusion = 1.0;
		vec2 distribution = texture2DDistribution( shadow, uv );
		float hard_shadow = step( compare , distribution.x );
		if (hard_shadow != 1.0 ) {
			float distance = compare - distribution.x ;
			float variance = max( 0.00000, distribution.y * distribution.y );
			float softness_probability = variance / (variance + distance * distance );			softness_probability = clamp( ( softness_probability - 0.3 ) / ( 0.95 - 0.3 ), 0.0, 1.0 );			occlusion = clamp( max( hard_shadow, softness_probability ), 0.0, 1.0 );
		}
		return occlusion;
	}
	float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
		float shadow = 1.0;
		shadowCoord.xyz /= shadowCoord.w;
		shadowCoord.z += shadowBias;
		bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
		bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
		if ( frustumTest ) {
		#if defined( SHADOWMAP_TYPE_PCF )
			vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
			float dx0 = - texelSize.x * shadowRadius;
			float dy0 = - texelSize.y * shadowRadius;
			float dx1 = + texelSize.x * shadowRadius;
			float dy1 = + texelSize.y * shadowRadius;
			float dx2 = dx0 / 2.0;
			float dy2 = dy0 / 2.0;
			float dx3 = dx1 / 2.0;
			float dy3 = dy1 / 2.0;
			shadow = (
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx0, dy0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx1, dy0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx2, dy2 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy2 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx3, dy2 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx0, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx2, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy, shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx3, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx1, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx2, dy3 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy3 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx3, dy3 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx0, dy1 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy1 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx1, dy1 ), shadowCoord.z )
			) * ( 1.0 / 17.0 );
		#elif defined( SHADOWMAP_TYPE_PCF_SOFT )
			vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
			float dx = texelSize.x;
			float dy = texelSize.y;
			vec2 uv = shadowCoord.xy;
			vec2 f = fract( uv * shadowMapSize + 0.5 );
			uv -= f * texelSize;
			shadow = (
				texture2DCompare( shadowMap, uv, shadowCoord.z ) +
				texture2DCompare( shadowMap, uv + vec2( dx, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, uv + vec2( 0.0, dy ), shadowCoord.z ) +
				texture2DCompare( shadowMap, uv + texelSize, shadowCoord.z ) +
				mix( texture2DCompare( shadowMap, uv + vec2( -dx, 0.0 ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, 0.0 ), shadowCoord.z ),
					 f.x ) +
				mix( texture2DCompare( shadowMap, uv + vec2( -dx, dy ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, dy ), shadowCoord.z ),
					 f.x ) +
				mix( texture2DCompare( shadowMap, uv + vec2( 0.0, -dy ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( 0.0, 2.0 * dy ), shadowCoord.z ),
					 f.y ) +
				mix( texture2DCompare( shadowMap, uv + vec2( dx, -dy ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( dx, 2.0 * dy ), shadowCoord.z ),
					 f.y ) +
				mix( mix( texture2DCompare( shadowMap, uv + vec2( -dx, -dy ), shadowCoord.z ),
						  texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, -dy ), shadowCoord.z ),
						  f.x ),
					 mix( texture2DCompare( shadowMap, uv + vec2( -dx, 2.0 * dy ), shadowCoord.z ),
						  texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, 2.0 * dy ), shadowCoord.z ),
						  f.x ),
					 f.y )
			) * ( 1.0 / 9.0 );
		#elif defined( SHADOWMAP_TYPE_VSM )
			shadow = VSMShadow( shadowMap, shadowCoord.xy, shadowCoord.z );
		#else
			shadow = texture2DCompare( shadowMap, shadowCoord.xy, shadowCoord.z );
		#endif
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	vec2 cubeToUV( vec3 v, float texelSizeY ) {
		vec3 absV = abs( v );
		float scaleToCube = 1.0 / max( absV.x, max( absV.y, absV.z ) );
		absV *= scaleToCube;
		v *= scaleToCube * ( 1.0 - 2.0 * texelSizeY );
		vec2 planar = v.xy;
		float almostATexel = 1.5 * texelSizeY;
		float almostOne = 1.0 - almostATexel;
		if ( absV.z >= almostOne ) {
			if ( v.z > 0.0 )
				planar.x = 4.0 - v.x;
		} else if ( absV.x >= almostOne ) {
			float signX = sign( v.x );
			planar.x = v.z * signX + 2.0 * signX;
		} else if ( absV.y >= almostOne ) {
			float signY = sign( v.y );
			planar.x = v.x + 2.0 * signY + 2.0;
			planar.y = v.z * signY - 2.0;
		}
		return vec2( 0.125, 0.25 ) * planar + vec2( 0.375, 0.75 );
	}
	float getPointShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		
		float lightToPositionLength = length( lightToPosition );
		if ( lightToPositionLength - shadowCameraFar <= 0.0 && lightToPositionLength - shadowCameraNear >= 0.0 ) {
			float dp = ( lightToPositionLength - shadowCameraNear ) / ( shadowCameraFar - shadowCameraNear );			dp += shadowBias;
			vec3 bd3D = normalize( lightToPosition );
			vec2 texelSize = vec2( 1.0 ) / ( shadowMapSize * vec2( 4.0, 2.0 ) );
			#if defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_PCF_SOFT ) || defined( SHADOWMAP_TYPE_VSM )
				vec2 offset = vec2( - 1, 1 ) * shadowRadius * texelSize.y;
				shadow = (
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xyy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yyy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xyx, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yyx, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xxy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yxy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xxx, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yxx, texelSize.y ), dp )
				) * ( 1.0 / 9.0 );
			#else
				shadow = texture2DCompare( shadowMap, cubeToUV( bd3D, texelSize.y ), dp );
			#endif
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
#endif`,iT=`#if NUM_SPOT_LIGHT_COORDS > 0
	uniform mat4 spotLightMatrix[ NUM_SPOT_LIGHT_COORDS ];
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform mat4 directionalShadowMatrix[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform mat4 pointShadowMatrix[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
#endif`,rT=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
	vec3 shadowWorldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
	vec4 shadowWorldPosition;
#endif
#if defined( USE_SHADOWMAP )
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * directionalLightShadows[ i ].shadowNormalBias, 0 );
			vDirectionalShadowCoord[ i ] = directionalShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * pointLightShadows[ i ].shadowNormalBias, 0 );
			vPointShadowCoord[ i ] = pointShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
#endif
#if NUM_SPOT_LIGHT_COORDS > 0
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_COORDS; i ++ ) {
		shadowWorldPosition = worldPosition;
		#if ( defined( USE_SHADOWMAP ) && UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
			shadowWorldPosition.xyz += shadowWorldNormal * spotLightShadows[ i ].shadowNormalBias;
		#endif
		vSpotLightCoord[ i ] = spotLightMatrix[ i ] * shadowWorldPosition;
	}
	#pragma unroll_loop_end
#endif`,sT=`float getShadowMask() {
	float shadow = 1.0;
	#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
		directionalLight = directionalLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( directionalShadowMap[ i ], directionalLight.shadowMapSize, directionalLight.shadowIntensity, directionalLight.shadowBias, directionalLight.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_SHADOWS; i ++ ) {
		spotLight = spotLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( spotShadowMap[ i ], spotLight.shadowMapSize, spotLight.shadowIntensity, spotLight.shadowBias, spotLight.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
		pointLight = pointLightShadows[ i ];
		shadow *= receiveShadow ? getPointShadow( pointShadowMap[ i ], pointLight.shadowMapSize, pointLight.shadowIntensity, pointLight.shadowBias, pointLight.shadowRadius, vPointShadowCoord[ i ], pointLight.shadowCameraNear, pointLight.shadowCameraFar ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#endif
	return shadow;
}`,oT=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,aT=`#ifdef USE_SKINNING
	uniform mat4 bindMatrix;
	uniform mat4 bindMatrixInverse;
	uniform highp sampler2D boneTexture;
	mat4 getBoneMatrix( const in float i ) {
		int size = textureSize( boneTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( boneTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( boneTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( boneTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( boneTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
#endif`,lT=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,cT=`#ifdef USE_SKINNING
	mat4 skinMatrix = mat4( 0.0 );
	skinMatrix += skinWeight.x * boneMatX;
	skinMatrix += skinWeight.y * boneMatY;
	skinMatrix += skinWeight.z * boneMatZ;
	skinMatrix += skinWeight.w * boneMatW;
	skinMatrix = bindMatrixInverse * skinMatrix * bindMatrix;
	objectNormal = vec4( skinMatrix * vec4( objectNormal, 0.0 ) ).xyz;
	#ifdef USE_TANGENT
		objectTangent = vec4( skinMatrix * vec4( objectTangent, 0.0 ) ).xyz;
	#endif
#endif`,uT=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,dT=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,fT=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,hT=`#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
uniform float toneMappingExposure;
vec3 LinearToneMapping( vec3 color ) {
	return saturate( toneMappingExposure * color );
}
vec3 ReinhardToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	return saturate( color / ( vec3( 1.0 ) + color ) );
}
vec3 CineonToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	color = max( vec3( 0.0 ), color - 0.004 );
	return pow( ( color * ( 6.2 * color + 0.5 ) ) / ( color * ( 6.2 * color + 1.7 ) + 0.06 ), vec3( 2.2 ) );
}
vec3 RRTAndODTFit( vec3 v ) {
	vec3 a = v * ( v + 0.0245786 ) - 0.000090537;
	vec3 b = v * ( 0.983729 * v + 0.4329510 ) + 0.238081;
	return a / b;
}
vec3 ACESFilmicToneMapping( vec3 color ) {
	const mat3 ACESInputMat = mat3(
		vec3( 0.59719, 0.07600, 0.02840 ),		vec3( 0.35458, 0.90834, 0.13383 ),
		vec3( 0.04823, 0.01566, 0.83777 )
	);
	const mat3 ACESOutputMat = mat3(
		vec3(  1.60475, -0.10208, -0.00327 ),		vec3( -0.53108,  1.10813, -0.07276 ),
		vec3( -0.07367, -0.00605,  1.07602 )
	);
	color *= toneMappingExposure / 0.6;
	color = ACESInputMat * color;
	color = RRTAndODTFit( color );
	color = ACESOutputMat * color;
	return saturate( color );
}
const mat3 LINEAR_REC2020_TO_LINEAR_SRGB = mat3(
	vec3( 1.6605, - 0.1246, - 0.0182 ),
	vec3( - 0.5876, 1.1329, - 0.1006 ),
	vec3( - 0.0728, - 0.0083, 1.1187 )
);
const mat3 LINEAR_SRGB_TO_LINEAR_REC2020 = mat3(
	vec3( 0.6274, 0.0691, 0.0164 ),
	vec3( 0.3293, 0.9195, 0.0880 ),
	vec3( 0.0433, 0.0113, 0.8956 )
);
vec3 agxDefaultContrastApprox( vec3 x ) {
	vec3 x2 = x * x;
	vec3 x4 = x2 * x2;
	return + 15.5 * x4 * x2
		- 40.14 * x4 * x
		+ 31.96 * x4
		- 6.868 * x2 * x
		+ 0.4298 * x2
		+ 0.1191 * x
		- 0.00232;
}
vec3 AgXToneMapping( vec3 color ) {
	const mat3 AgXInsetMatrix = mat3(
		vec3( 0.856627153315983, 0.137318972929847, 0.11189821299995 ),
		vec3( 0.0951212405381588, 0.761241990602591, 0.0767994186031903 ),
		vec3( 0.0482516061458583, 0.101439036467562, 0.811302368396859 )
	);
	const mat3 AgXOutsetMatrix = mat3(
		vec3( 1.1271005818144368, - 0.1413297634984383, - 0.14132976349843826 ),
		vec3( - 0.11060664309660323, 1.157823702216272, - 0.11060664309660294 ),
		vec3( - 0.016493938717834573, - 0.016493938717834257, 1.2519364065950405 )
	);
	const float AgxMinEv = - 12.47393;	const float AgxMaxEv = 4.026069;
	color *= toneMappingExposure;
	color = LINEAR_SRGB_TO_LINEAR_REC2020 * color;
	color = AgXInsetMatrix * color;
	color = max( color, 1e-10 );	color = log2( color );
	color = ( color - AgxMinEv ) / ( AgxMaxEv - AgxMinEv );
	color = clamp( color, 0.0, 1.0 );
	color = agxDefaultContrastApprox( color );
	color = AgXOutsetMatrix * color;
	color = pow( max( vec3( 0.0 ), color ), vec3( 2.2 ) );
	color = LINEAR_REC2020_TO_LINEAR_SRGB * color;
	color = clamp( color, 0.0, 1.0 );
	return color;
}
vec3 NeutralToneMapping( vec3 color ) {
	const float StartCompression = 0.8 - 0.04;
	const float Desaturation = 0.15;
	color *= toneMappingExposure;
	float x = min( color.r, min( color.g, color.b ) );
	float offset = x < 0.08 ? x - 6.25 * x * x : 0.04;
	color -= offset;
	float peak = max( color.r, max( color.g, color.b ) );
	if ( peak < StartCompression ) return color;
	float d = 1. - StartCompression;
	float newPeak = 1. - d * d / ( peak + d - StartCompression );
	color *= newPeak / peak;
	float g = 1. - 1. / ( Desaturation * ( peak - newPeak ) + 1. );
	return mix( color, vec3( newPeak ), g );
}
vec3 CustomToneMapping( vec3 color ) { return color; }`,pT=`#ifdef USE_TRANSMISSION
	material.transmission = transmission;
	material.transmissionAlpha = 1.0;
	material.thickness = thickness;
	material.attenuationDistance = attenuationDistance;
	material.attenuationColor = attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		material.transmission *= texture2D( transmissionMap, vTransmissionMapUv ).r;
	#endif
	#ifdef USE_THICKNESSMAP
		material.thickness *= texture2D( thicknessMap, vThicknessMapUv ).g;
	#endif
	vec3 pos = vWorldPosition;
	vec3 v = normalize( cameraPosition - pos );
	vec3 n = inverseTransformDirection( normal, viewMatrix );
	vec4 transmitted = getIBLVolumeRefraction(
		n, v, material.roughness, material.diffuseColor, material.specularColor, material.specularF90,
		pos, modelMatrix, viewMatrix, projectionMatrix, material.dispersion, material.ior, material.thickness,
		material.attenuationColor, material.attenuationDistance );
	material.transmissionAlpha = mix( material.transmissionAlpha, transmitted.a, material.transmission );
	totalDiffuse = mix( totalDiffuse, transmitted.rgb, material.transmission );
#endif`,mT=`#ifdef USE_TRANSMISSION
	uniform float transmission;
	uniform float thickness;
	uniform float attenuationDistance;
	uniform vec3 attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		uniform sampler2D transmissionMap;
	#endif
	#ifdef USE_THICKNESSMAP
		uniform sampler2D thicknessMap;
	#endif
	uniform vec2 transmissionSamplerSize;
	uniform sampler2D transmissionSamplerMap;
	uniform mat4 modelMatrix;
	uniform mat4 projectionMatrix;
	varying vec3 vWorldPosition;
	float w0( float a ) {
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - a + 3.0 ) - 3.0 ) + 1.0 );
	}
	float w1( float a ) {
		return ( 1.0 / 6.0 ) * ( a *  a * ( 3.0 * a - 6.0 ) + 4.0 );
	}
	float w2( float a ){
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - 3.0 * a + 3.0 ) + 3.0 ) + 1.0 );
	}
	float w3( float a ) {
		return ( 1.0 / 6.0 ) * ( a * a * a );
	}
	float g0( float a ) {
		return w0( a ) + w1( a );
	}
	float g1( float a ) {
		return w2( a ) + w3( a );
	}
	float h0( float a ) {
		return - 1.0 + w1( a ) / ( w0( a ) + w1( a ) );
	}
	float h1( float a ) {
		return 1.0 + w3( a ) / ( w2( a ) + w3( a ) );
	}
	vec4 bicubic( sampler2D tex, vec2 uv, vec4 texelSize, float lod ) {
		uv = uv * texelSize.zw + 0.5;
		vec2 iuv = floor( uv );
		vec2 fuv = fract( uv );
		float g0x = g0( fuv.x );
		float g1x = g1( fuv.x );
		float h0x = h0( fuv.x );
		float h1x = h1( fuv.x );
		float h0y = h0( fuv.y );
		float h1y = h1( fuv.y );
		vec2 p0 = ( vec2( iuv.x + h0x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p1 = ( vec2( iuv.x + h1x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p2 = ( vec2( iuv.x + h0x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		vec2 p3 = ( vec2( iuv.x + h1x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		return g0( fuv.y ) * ( g0x * textureLod( tex, p0, lod ) + g1x * textureLod( tex, p1, lod ) ) +
			g1( fuv.y ) * ( g0x * textureLod( tex, p2, lod ) + g1x * textureLod( tex, p3, lod ) );
	}
	vec4 textureBicubic( sampler2D sampler, vec2 uv, float lod ) {
		vec2 fLodSize = vec2( textureSize( sampler, int( lod ) ) );
		vec2 cLodSize = vec2( textureSize( sampler, int( lod + 1.0 ) ) );
		vec2 fLodSizeInv = 1.0 / fLodSize;
		vec2 cLodSizeInv = 1.0 / cLodSize;
		vec4 fSample = bicubic( sampler, uv, vec4( fLodSizeInv, fLodSize ), floor( lod ) );
		vec4 cSample = bicubic( sampler, uv, vec4( cLodSizeInv, cLodSize ), ceil( lod ) );
		return mix( fSample, cSample, fract( lod ) );
	}
	vec3 getVolumeTransmissionRay( const in vec3 n, const in vec3 v, const in float thickness, const in float ior, const in mat4 modelMatrix ) {
		vec3 refractionVector = refract( - v, normalize( n ), 1.0 / ior );
		vec3 modelScale;
		modelScale.x = length( vec3( modelMatrix[ 0 ].xyz ) );
		modelScale.y = length( vec3( modelMatrix[ 1 ].xyz ) );
		modelScale.z = length( vec3( modelMatrix[ 2 ].xyz ) );
		return normalize( refractionVector ) * thickness * modelScale;
	}
	float applyIorToRoughness( const in float roughness, const in float ior ) {
		return roughness * clamp( ior * 2.0 - 2.0, 0.0, 1.0 );
	}
	vec4 getTransmissionSample( const in vec2 fragCoord, const in float roughness, const in float ior ) {
		float lod = log2( transmissionSamplerSize.x ) * applyIorToRoughness( roughness, ior );
		return textureBicubic( transmissionSamplerMap, fragCoord.xy, lod );
	}
	vec3 volumeAttenuation( const in float transmissionDistance, const in vec3 attenuationColor, const in float attenuationDistance ) {
		if ( isinf( attenuationDistance ) ) {
			return vec3( 1.0 );
		} else {
			vec3 attenuationCoefficient = -log( attenuationColor ) / attenuationDistance;
			vec3 transmittance = exp( - attenuationCoefficient * transmissionDistance );			return transmittance;
		}
	}
	vec4 getIBLVolumeRefraction( const in vec3 n, const in vec3 v, const in float roughness, const in vec3 diffuseColor,
		const in vec3 specularColor, const in float specularF90, const in vec3 position, const in mat4 modelMatrix,
		const in mat4 viewMatrix, const in mat4 projMatrix, const in float dispersion, const in float ior, const in float thickness,
		const in vec3 attenuationColor, const in float attenuationDistance ) {
		vec4 transmittedLight;
		vec3 transmittance;
		#ifdef USE_DISPERSION
			float halfSpread = ( ior - 1.0 ) * 0.025 * dispersion;
			vec3 iors = vec3( ior - halfSpread, ior, ior + halfSpread );
			for ( int i = 0; i < 3; i ++ ) {
				vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, iors[ i ], modelMatrix );
				vec3 refractedRayExit = position + transmissionRay;
		
				vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
				vec2 refractionCoords = ndcPos.xy / ndcPos.w;
				refractionCoords += 1.0;
				refractionCoords /= 2.0;
		
				vec4 transmissionSample = getTransmissionSample( refractionCoords, roughness, iors[ i ] );
				transmittedLight[ i ] = transmissionSample[ i ];
				transmittedLight.a += transmissionSample.a;
				transmittance[ i ] = diffuseColor[ i ] * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance )[ i ];
			}
			transmittedLight.a /= 3.0;
		
		#else
		
			vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, ior, modelMatrix );
			vec3 refractedRayExit = position + transmissionRay;
			vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
			vec2 refractionCoords = ndcPos.xy / ndcPos.w;
			refractionCoords += 1.0;
			refractionCoords /= 2.0;
			transmittedLight = getTransmissionSample( refractionCoords, roughness, ior );
			transmittance = diffuseColor * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance );
		
		#endif
		vec3 attenuatedColor = transmittance * transmittedLight.rgb;
		vec3 F = EnvironmentBRDF( n, v, specularColor, specularF90, roughness );
		float transmittanceFactor = ( transmittance.r + transmittance.g + transmittance.b ) / 3.0;
		return vec4( ( 1.0 - F ) * attenuatedColor, 1.0 - ( 1.0 - transmittedLight.a ) * transmittanceFactor );
	}
#endif`,gT=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_SPECULARMAP
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,vT=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	uniform mat3 mapTransform;
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	uniform mat3 alphaMapTransform;
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	uniform mat3 lightMapTransform;
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	uniform mat3 aoMapTransform;
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	uniform mat3 bumpMapTransform;
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	uniform mat3 normalMapTransform;
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_DISPLACEMENTMAP
	uniform mat3 displacementMapTransform;
	varying vec2 vDisplacementMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	uniform mat3 emissiveMapTransform;
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	uniform mat3 metalnessMapTransform;
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	uniform mat3 roughnessMapTransform;
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	uniform mat3 anisotropyMapTransform;
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	uniform mat3 clearcoatMapTransform;
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform mat3 clearcoatNormalMapTransform;
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform mat3 clearcoatRoughnessMapTransform;
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	uniform mat3 sheenColorMapTransform;
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	uniform mat3 sheenRoughnessMapTransform;
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	uniform mat3 iridescenceMapTransform;
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform mat3 iridescenceThicknessMapTransform;
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SPECULARMAP
	uniform mat3 specularMapTransform;
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	uniform mat3 specularColorMapTransform;
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	uniform mat3 specularIntensityMapTransform;
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,yT=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	vUv = vec3( uv, 1 ).xy;
#endif
#ifdef USE_MAP
	vMapUv = ( mapTransform * vec3( MAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ALPHAMAP
	vAlphaMapUv = ( alphaMapTransform * vec3( ALPHAMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_LIGHTMAP
	vLightMapUv = ( lightMapTransform * vec3( LIGHTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_AOMAP
	vAoMapUv = ( aoMapTransform * vec3( AOMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_BUMPMAP
	vBumpMapUv = ( bumpMapTransform * vec3( BUMPMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_NORMALMAP
	vNormalMapUv = ( normalMapTransform * vec3( NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DISPLACEMENTMAP
	vDisplacementMapUv = ( displacementMapTransform * vec3( DISPLACEMENTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_EMISSIVEMAP
	vEmissiveMapUv = ( emissiveMapTransform * vec3( EMISSIVEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_METALNESSMAP
	vMetalnessMapUv = ( metalnessMapTransform * vec3( METALNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ROUGHNESSMAP
	vRoughnessMapUv = ( roughnessMapTransform * vec3( ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ANISOTROPYMAP
	vAnisotropyMapUv = ( anisotropyMapTransform * vec3( ANISOTROPYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOATMAP
	vClearcoatMapUv = ( clearcoatMapTransform * vec3( CLEARCOATMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	vClearcoatNormalMapUv = ( clearcoatNormalMapTransform * vec3( CLEARCOAT_NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	vClearcoatRoughnessMapUv = ( clearcoatRoughnessMapTransform * vec3( CLEARCOAT_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCEMAP
	vIridescenceMapUv = ( iridescenceMapTransform * vec3( IRIDESCENCEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	vIridescenceThicknessMapUv = ( iridescenceThicknessMapTransform * vec3( IRIDESCENCE_THICKNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_COLORMAP
	vSheenColorMapUv = ( sheenColorMapTransform * vec3( SHEEN_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	vSheenRoughnessMapUv = ( sheenRoughnessMapTransform * vec3( SHEEN_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULARMAP
	vSpecularMapUv = ( specularMapTransform * vec3( SPECULARMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_COLORMAP
	vSpecularColorMapUv = ( specularColorMapTransform * vec3( SPECULAR_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	vSpecularIntensityMapUv = ( specularIntensityMapTransform * vec3( SPECULAR_INTENSITYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_TRANSMISSIONMAP
	vTransmissionMapUv = ( transmissionMapTransform * vec3( TRANSMISSIONMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_THICKNESSMAP
	vThicknessMapUv = ( thicknessMapTransform * vec3( THICKNESSMAP_UV, 1 ) ).xy;
#endif`,_T=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,xT=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,MT=`uniform sampler2D t2D;
uniform float backgroundIntensity;
varying vec2 vUv;
void main() {
	vec4 texColor = texture2D( t2D, vUv );
	#ifdef DECODE_VIDEO_TEXTURE
		texColor = vec4( mix( pow( texColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), texColor.rgb * 0.0773993808, vec3( lessThanEqual( texColor.rgb, vec3( 0.04045 ) ) ) ), texColor.w );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,ET=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,bT=`#ifdef ENVMAP_TYPE_CUBE
	uniform samplerCube envMap;
#elif defined( ENVMAP_TYPE_CUBE_UV )
	uniform sampler2D envMap;
#endif
uniform float flipEnvMap;
uniform float backgroundBlurriness;
uniform float backgroundIntensity;
uniform mat3 backgroundRotation;
varying vec3 vWorldDirection;
#include <cube_uv_reflection_fragment>
void main() {
	#ifdef ENVMAP_TYPE_CUBE
		vec4 texColor = textureCube( envMap, backgroundRotation * vec3( flipEnvMap * vWorldDirection.x, vWorldDirection.yz ) );
	#elif defined( ENVMAP_TYPE_CUBE_UV )
		vec4 texColor = textureCubeUV( envMap, backgroundRotation * vWorldDirection, backgroundBlurriness );
	#else
		vec4 texColor = vec4( 0.0, 0.0, 0.0, 1.0 );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,wT=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,ST=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,CT=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
varying vec2 vHighPrecisionZW;
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vHighPrecisionZW = gl_Position.zw;
}`,DT=`#if DEPTH_PACKING == 3200
	uniform float opacity;
#endif
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
varying vec2 vHighPrecisionZW;
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#if DEPTH_PACKING == 3200
		diffuseColor.a = opacity;
	#endif
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <logdepthbuf_fragment>
	float fragCoordZ = 0.5 * vHighPrecisionZW[0] / vHighPrecisionZW[1] + 0.5;
	#if DEPTH_PACKING == 3200
		gl_FragColor = vec4( vec3( 1.0 - fragCoordZ ), opacity );
	#elif DEPTH_PACKING == 3201
		gl_FragColor = packDepthToRGBA( fragCoordZ );
	#elif DEPTH_PACKING == 3202
		gl_FragColor = vec4( packDepthToRGB( fragCoordZ ), 1.0 );
	#elif DEPTH_PACKING == 3203
		gl_FragColor = vec4( packDepthToRG( fragCoordZ ), 0.0, 1.0 );
	#endif
}`,TT=`#define DISTANCE
varying vec3 vWorldPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <worldpos_vertex>
	#include <clipping_planes_vertex>
	vWorldPosition = worldPosition.xyz;
}`,AT=`#define DISTANCE
uniform vec3 referencePosition;
uniform float nearDistance;
uniform float farDistance;
varying vec3 vWorldPosition;
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <clipping_planes_pars_fragment>
void main () {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	float dist = length( vWorldPosition - referencePosition );
	dist = ( dist - nearDistance ) / ( farDistance - nearDistance );
	dist = saturate( dist );
	gl_FragColor = packDepthToRGBA( dist );
}`,IT=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,RT=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,NT=`uniform float scale;
attribute float lineDistance;
varying float vLineDistance;
#include <common>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	vLineDistance = scale * lineDistance;
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,PT=`uniform vec3 diffuse;
uniform float opacity;
uniform float dashSize;
uniform float totalSize;
varying float vLineDistance;
#include <common>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	if ( mod( vLineDistance, totalSize ) > dashSize ) {
		discard;
	}
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,FT=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#if defined ( USE_ENVMAP ) || defined ( USE_SKINNING )
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinbase_vertex>
		#include <skinnormal_vertex>
		#include <defaultnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <fog_vertex>
}`,OT=`uniform vec3 diffuse;
uniform float opacity;
#ifndef FLAT_SHADED
	varying vec3 vNormal;
#endif
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		reflectedLight.indirectDiffuse += lightMapTexel.rgb * lightMapIntensity * RECIPROCAL_PI;
	#else
		reflectedLight.indirectDiffuse += vec3( 1.0 );
	#endif
	#include <aomap_fragment>
	reflectedLight.indirectDiffuse *= diffuseColor.rgb;
	vec3 outgoingLight = reflectedLight.indirectDiffuse;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,LT=`#define LAMBERT
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,kT=`#define LAMBERT
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_lambert_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_lambert_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,UT=`#define MATCAP
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <displacementmap_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
	vViewPosition = - mvPosition.xyz;
}`,VT=`#define MATCAP
uniform vec3 diffuse;
uniform float opacity;
uniform sampler2D matcap;
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	vec3 viewDir = normalize( vViewPosition );
	vec3 x = normalize( vec3( viewDir.z, 0.0, - viewDir.x ) );
	vec3 y = cross( viewDir, x );
	vec2 uv = vec2( dot( x, normal ), dot( y, normal ) ) * 0.495 + 0.5;
	#ifdef USE_MATCAP
		vec4 matcapColor = texture2D( matcap, uv );
	#else
		vec4 matcapColor = vec4( vec3( mix( 0.2, 0.8, uv.y ) ), 1.0 );
	#endif
	vec3 outgoingLight = diffuseColor.rgb * matcapColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,BT=`#define NORMAL
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	vViewPosition = - mvPosition.xyz;
#endif
}`,HT=`#define NORMAL
uniform float opacity;
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <packing>
#include <uv_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 0.0, 0.0, 0.0, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	gl_FragColor = vec4( packNormalToRGB( normal ), diffuseColor.a );
	#ifdef OPAQUE
		gl_FragColor.a = 1.0;
	#endif
}`,zT=`#define PHONG
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,GT=`#define PHONG
uniform vec3 diffuse;
uniform vec3 emissive;
uniform vec3 specular;
uniform float shininess;
uniform float opacity;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_phong_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_phong_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + reflectedLight.directSpecular + reflectedLight.indirectSpecular + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,WT=`#define STANDARD
varying vec3 vViewPosition;
#ifdef USE_TRANSMISSION
	varying vec3 vWorldPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
#ifdef USE_TRANSMISSION
	vWorldPosition = worldPosition.xyz;
#endif
}`,jT=`#define STANDARD
#ifdef PHYSICAL
	#define IOR
	#define USE_SPECULAR
#endif
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float roughness;
uniform float metalness;
uniform float opacity;
#ifdef IOR
	uniform float ior;
#endif
#ifdef USE_SPECULAR
	uniform float specularIntensity;
	uniform vec3 specularColor;
	#ifdef USE_SPECULAR_COLORMAP
		uniform sampler2D specularColorMap;
	#endif
	#ifdef USE_SPECULAR_INTENSITYMAP
		uniform sampler2D specularIntensityMap;
	#endif
#endif
#ifdef USE_CLEARCOAT
	uniform float clearcoat;
	uniform float clearcoatRoughness;
#endif
#ifdef USE_DISPERSION
	uniform float dispersion;
#endif
#ifdef USE_IRIDESCENCE
	uniform float iridescence;
	uniform float iridescenceIOR;
	uniform float iridescenceThicknessMinimum;
	uniform float iridescenceThicknessMaximum;
#endif
#ifdef USE_SHEEN
	uniform vec3 sheenColor;
	uniform float sheenRoughness;
	#ifdef USE_SHEEN_COLORMAP
		uniform sampler2D sheenColorMap;
	#endif
	#ifdef USE_SHEEN_ROUGHNESSMAP
		uniform sampler2D sheenRoughnessMap;
	#endif
#endif
#ifdef USE_ANISOTROPY
	uniform vec2 anisotropyVector;
	#ifdef USE_ANISOTROPYMAP
		uniform sampler2D anisotropyMap;
	#endif
#endif
varying vec3 vViewPosition;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <iridescence_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_physical_pars_fragment>
#include <transmission_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <clearcoat_pars_fragment>
#include <iridescence_pars_fragment>
#include <roughnessmap_pars_fragment>
#include <metalnessmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <roughnessmap_fragment>
	#include <metalnessmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <clearcoat_normal_fragment_begin>
	#include <clearcoat_normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_physical_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 totalDiffuse = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse;
	vec3 totalSpecular = reflectedLight.directSpecular + reflectedLight.indirectSpecular;
	#include <transmission_fragment>
	vec3 outgoingLight = totalDiffuse + totalSpecular + totalEmissiveRadiance;
	#ifdef USE_SHEEN
		float sheenEnergyComp = 1.0 - 0.157 * max3( material.sheenColor );
		outgoingLight = outgoingLight * sheenEnergyComp + sheenSpecularDirect + sheenSpecularIndirect;
	#endif
	#ifdef USE_CLEARCOAT
		float dotNVcc = saturate( dot( geometryClearcoatNormal, geometryViewDir ) );
		vec3 Fcc = F_Schlick( material.clearcoatF0, material.clearcoatF90, dotNVcc );
		outgoingLight = outgoingLight * ( 1.0 - material.clearcoat * Fcc ) + ( clearcoatSpecularDirect + clearcoatSpecularIndirect ) * material.clearcoat;
	#endif
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,$T=`#define TOON
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,qT=`#define TOON
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <gradientmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_toon_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_toon_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,XT=`uniform float size;
uniform float scale;
#include <common>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
#ifdef USE_POINTS_UV
	varying vec2 vUv;
	uniform mat3 uvTransform;
#endif
void main() {
	#ifdef USE_POINTS_UV
		vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	#endif
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	gl_PointSize = size;
	#ifdef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) gl_PointSize *= ( scale / - mvPosition.z );
	#endif
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <fog_vertex>
}`,YT=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <color_pars_fragment>
#include <map_particle_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_particle_fragment>
	#include <color_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,ZT=`#include <common>
#include <batching_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <shadowmap_pars_vertex>
void main() {
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,KT=`uniform vec3 color;
uniform float opacity;
#include <common>
#include <packing>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <logdepthbuf_pars_fragment>
#include <shadowmap_pars_fragment>
#include <shadowmask_pars_fragment>
void main() {
	#include <logdepthbuf_fragment>
	gl_FragColor = vec4( color, opacity * ( 1.0 - getShadowMask() ) );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,JT=`uniform float rotation;
uniform vec2 center;
#include <common>
#include <uv_pars_vertex>
#include <fog_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	vec4 mvPosition = modelViewMatrix[ 3 ];
	vec2 scale = vec2( length( modelMatrix[ 0 ].xyz ), length( modelMatrix[ 1 ].xyz ) );
	#ifndef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) scale *= - mvPosition.z;
	#endif
	vec2 alignedPosition = ( position.xy - ( center - vec2( 0.5 ) ) ) * scale;
	vec2 rotatedPosition;
	rotatedPosition.x = cos( rotation ) * alignedPosition.x - sin( rotation ) * alignedPosition.y;
	rotatedPosition.y = sin( rotation ) * alignedPosition.x + cos( rotation ) * alignedPosition.y;
	mvPosition.xy += rotatedPosition;
	gl_Position = projectionMatrix * mvPosition;
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,QT=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,Oe={alphahash_fragment:MC,alphahash_pars_fragment:EC,alphamap_fragment:bC,alphamap_pars_fragment:wC,alphatest_fragment:SC,alphatest_pars_fragment:CC,aomap_fragment:DC,aomap_pars_fragment:TC,batching_pars_vertex:AC,batching_vertex:IC,begin_vertex:RC,beginnormal_vertex:NC,bsdfs:PC,iridescence_fragment:FC,bumpmap_pars_fragment:OC,clipping_planes_fragment:LC,clipping_planes_pars_fragment:kC,clipping_planes_pars_vertex:UC,clipping_planes_vertex:VC,color_fragment:BC,color_pars_fragment:HC,color_pars_vertex:zC,color_vertex:GC,common:WC,cube_uv_reflection_fragment:jC,defaultnormal_vertex:$C,displacementmap_pars_vertex:qC,displacementmap_vertex:XC,emissivemap_fragment:YC,emissivemap_pars_fragment:ZC,colorspace_fragment:KC,colorspace_pars_fragment:JC,envmap_fragment:QC,envmap_common_pars_fragment:eD,envmap_pars_fragment:tD,envmap_pars_vertex:nD,envmap_physical_pars_fragment:hD,envmap_vertex:iD,fog_vertex:rD,fog_pars_vertex:sD,fog_fragment:oD,fog_pars_fragment:aD,gradientmap_pars_fragment:lD,lightmap_pars_fragment:cD,lights_lambert_fragment:uD,lights_lambert_pars_fragment:dD,lights_pars_begin:fD,lights_toon_fragment:pD,lights_toon_pars_fragment:mD,lights_phong_fragment:gD,lights_phong_pars_fragment:vD,lights_physical_fragment:yD,lights_physical_pars_fragment:_D,lights_fragment_begin:xD,lights_fragment_maps:MD,lights_fragment_end:ED,logdepthbuf_fragment:bD,logdepthbuf_pars_fragment:wD,logdepthbuf_pars_vertex:SD,logdepthbuf_vertex:CD,map_fragment:DD,map_pars_fragment:TD,map_particle_fragment:AD,map_particle_pars_fragment:ID,metalnessmap_fragment:RD,metalnessmap_pars_fragment:ND,morphinstance_vertex:PD,morphcolor_vertex:FD,morphnormal_vertex:OD,morphtarget_pars_vertex:LD,morphtarget_vertex:kD,normal_fragment_begin:UD,normal_fragment_maps:VD,normal_pars_fragment:BD,normal_pars_vertex:HD,normal_vertex:zD,normalmap_pars_fragment:GD,clearcoat_normal_fragment_begin:WD,clearcoat_normal_fragment_maps:jD,clearcoat_pars_fragment:$D,iridescence_pars_fragment:qD,opaque_fragment:XD,packing:YD,premultiplied_alpha_fragment:ZD,project_vertex:KD,dithering_fragment:JD,dithering_pars_fragment:QD,roughnessmap_fragment:eT,roughnessmap_pars_fragment:tT,shadowmap_pars_fragment:nT,shadowmap_pars_vertex:iT,shadowmap_vertex:rT,shadowmask_pars_fragment:sT,skinbase_vertex:oT,skinning_pars_vertex:aT,skinning_vertex:lT,skinnormal_vertex:cT,specularmap_fragment:uT,specularmap_pars_fragment:dT,tonemapping_fragment:fT,tonemapping_pars_fragment:hT,transmission_fragment:pT,transmission_pars_fragment:mT,uv_pars_fragment:gT,uv_pars_vertex:vT,uv_vertex:yT,worldpos_vertex:_T,background_vert:xT,background_frag:MT,backgroundCube_vert:ET,backgroundCube_frag:bT,cube_vert:wT,cube_frag:ST,depth_vert:CT,depth_frag:DT,distanceRGBA_vert:TT,distanceRGBA_frag:AT,equirect_vert:IT,equirect_frag:RT,linedashed_vert:NT,linedashed_frag:PT,meshbasic_vert:FT,meshbasic_frag:OT,meshlambert_vert:LT,meshlambert_frag:kT,meshmatcap_vert:UT,meshmatcap_frag:VT,meshnormal_vert:BT,meshnormal_frag:HT,meshphong_vert:zT,meshphong_frag:GT,meshphysical_vert:WT,meshphysical_frag:jT,meshtoon_vert:$T,meshtoon_frag:qT,points_vert:XT,points_frag:YT,shadow_vert:ZT,shadow_frag:KT,sprite_vert:JT,sprite_frag:QT},re={common:{diffuse:{value:new qe(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new Le},alphaMap:{value:null},alphaMapTransform:{value:new Le},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new Le}},envmap:{envMap:{value:null},envMapRotation:{value:new Le},flipEnvMap:{value:-1},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new Le}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new Le}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new Le},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new Le},normalScale:{value:new st(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new Le},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new Le}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new Le}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new Le}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new qe(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMap:{value:[]},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotShadowMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMap:{value:[]},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null}},points:{diffuse:{value:new qe(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new Le},alphaTest:{value:0},uvTransform:{value:new Le}},sprite:{diffuse:{value:new qe(16777215)},opacity:{value:1},center:{value:new st(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new Le},alphaMap:{value:null},alphaMapTransform:{value:new Le},alphaTest:{value:0}}},Yn={basic:{uniforms:Xt([re.common,re.specularmap,re.envmap,re.aomap,re.lightmap,re.fog]),vertexShader:Oe.meshbasic_vert,fragmentShader:Oe.meshbasic_frag},lambert:{uniforms:Xt([re.common,re.specularmap,re.envmap,re.aomap,re.lightmap,re.emissivemap,re.bumpmap,re.normalmap,re.displacementmap,re.fog,re.lights,{emissive:{value:new qe(0)}}]),vertexShader:Oe.meshlambert_vert,fragmentShader:Oe.meshlambert_frag},phong:{uniforms:Xt([re.common,re.specularmap,re.envmap,re.aomap,re.lightmap,re.emissivemap,re.bumpmap,re.normalmap,re.displacementmap,re.fog,re.lights,{emissive:{value:new qe(0)},specular:{value:new qe(1118481)},shininess:{value:30}}]),vertexShader:Oe.meshphong_vert,fragmentShader:Oe.meshphong_frag},standard:{uniforms:Xt([re.common,re.envmap,re.aomap,re.lightmap,re.emissivemap,re.bumpmap,re.normalmap,re.displacementmap,re.roughnessmap,re.metalnessmap,re.fog,re.lights,{emissive:{value:new qe(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:Oe.meshphysical_vert,fragmentShader:Oe.meshphysical_frag},toon:{uniforms:Xt([re.common,re.aomap,re.lightmap,re.emissivemap,re.bumpmap,re.normalmap,re.displacementmap,re.gradientmap,re.fog,re.lights,{emissive:{value:new qe(0)}}]),vertexShader:Oe.meshtoon_vert,fragmentShader:Oe.meshtoon_frag},matcap:{uniforms:Xt([re.common,re.bumpmap,re.normalmap,re.displacementmap,re.fog,{matcap:{value:null}}]),vertexShader:Oe.meshmatcap_vert,fragmentShader:Oe.meshmatcap_frag},points:{uniforms:Xt([re.points,re.fog]),vertexShader:Oe.points_vert,fragmentShader:Oe.points_frag},dashed:{uniforms:Xt([re.common,re.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:Oe.linedashed_vert,fragmentShader:Oe.linedashed_frag},depth:{uniforms:Xt([re.common,re.displacementmap]),vertexShader:Oe.depth_vert,fragmentShader:Oe.depth_frag},normal:{uniforms:Xt([re.common,re.bumpmap,re.normalmap,re.displacementmap,{opacity:{value:1}}]),vertexShader:Oe.meshnormal_vert,fragmentShader:Oe.meshnormal_frag},sprite:{uniforms:Xt([re.sprite,re.fog]),vertexShader:Oe.sprite_vert,fragmentShader:Oe.sprite_frag},background:{uniforms:{uvTransform:{value:new Le},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:Oe.background_vert,fragmentShader:Oe.background_frag},backgroundCube:{uniforms:{envMap:{value:null},flipEnvMap:{value:-1},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new Le}},vertexShader:Oe.backgroundCube_vert,fragmentShader:Oe.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:Oe.cube_vert,fragmentShader:Oe.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:Oe.equirect_vert,fragmentShader:Oe.equirect_frag},distanceRGBA:{uniforms:Xt([re.common,re.displacementmap,{referencePosition:{value:new V},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:Oe.distanceRGBA_vert,fragmentShader:Oe.distanceRGBA_frag},shadow:{uniforms:Xt([re.lights,re.fog,{color:{value:new qe(0)},opacity:{value:1}}]),vertexShader:Oe.shadow_vert,fragmentShader:Oe.shadow_frag}};Yn.physical={uniforms:Xt([Yn.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new Le},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new Le},clearcoatNormalScale:{value:new st(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new Le},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new Le},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new Le},sheen:{value:0},sheenColor:{value:new qe(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new Le},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new Le},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new Le},transmissionSamplerSize:{value:new st},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new Le},attenuationDistance:{value:0},attenuationColor:{value:new qe(0)},specularColor:{value:new qe(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new Le},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new Le},anisotropyVector:{value:new st},anisotropyMap:{value:null},anisotropyMapTransform:{value:new Le}}]),vertexShader:Oe.meshphysical_vert,fragmentShader:Oe.meshphysical_frag};var Il={r:0,b:0,g:0},hr=new br,eA=new Dt;function tA(n,e,t,i,r,s,o){let a=new qe(0),l=s===!0?0:1,c,u,d=null,f=0,h=null;function g(S){let E=S.isScene===!0?S.background:null;return E&&E.isTexture&&(E=(S.backgroundBlurriness>0?t:e).get(E)),E}function x(S){let E=!1,w=g(S);w===null?p(a,l):w&&w.isColor&&(p(w,1),E=!0);let L=n.xr.getEnvironmentBlendMode();L==="additive"?i.buffers.color.setClear(0,0,0,1,o):L==="alpha-blend"&&i.buffers.color.setClear(0,0,0,0,o),(n.autoClear||E)&&(i.buffers.depth.setTest(!0),i.buffers.depth.setMask(!0),i.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function m(S,E){let w=g(E);w&&(w.isCubeTexture||w.mapping===uc)?(u===void 0&&(u=new rn(new ji(1,1,1),new Zn({name:"BackgroundCubeMaterial",uniforms:Os(Yn.backgroundCube.uniforms),vertexShader:Yn.backgroundCube.vertexShader,fragmentShader:Yn.backgroundCube.fragmentShader,side:sn,depthTest:!1,depthWrite:!1,fog:!1})),u.geometry.deleteAttribute("normal"),u.geometry.deleteAttribute("uv"),u.onBeforeRender=function(L,T,C){this.matrixWorld.copyPosition(C.matrixWorld)},Object.defineProperty(u.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),r.update(u)),hr.copy(E.backgroundRotation),hr.x*=-1,hr.y*=-1,hr.z*=-1,w.isCubeTexture&&w.isRenderTargetTexture===!1&&(hr.y*=-1,hr.z*=-1),u.material.uniforms.envMap.value=w,u.material.uniforms.flipEnvMap.value=w.isCubeTexture&&w.isRenderTargetTexture===!1?-1:1,u.material.uniforms.backgroundBlurriness.value=E.backgroundBlurriness,u.material.uniforms.backgroundIntensity.value=E.backgroundIntensity,u.material.uniforms.backgroundRotation.value.setFromMatrix4(eA.makeRotationFromEuler(hr)),u.material.toneMapped=rt.getTransfer(w.colorSpace)!==mt,(d!==w||f!==w.version||h!==n.toneMapping)&&(u.material.needsUpdate=!0,d=w,f=w.version,h=n.toneMapping),u.layers.enableAll(),S.unshift(u,u.geometry,u.material,0,0,null)):w&&w.isTexture&&(c===void 0&&(c=new rn(new ec(2,2),new Zn({name:"BackgroundMaterial",uniforms:Os(Yn.background.uniforms),vertexShader:Yn.background.vertexShader,fragmentShader:Yn.background.fragmentShader,side:zi,depthTest:!1,depthWrite:!1,fog:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),r.update(c)),c.material.uniforms.t2D.value=w,c.material.uniforms.backgroundIntensity.value=E.backgroundIntensity,c.material.toneMapped=rt.getTransfer(w.colorSpace)!==mt,w.matrixAutoUpdate===!0&&w.updateMatrix(),c.material.uniforms.uvTransform.value.copy(w.matrix),(d!==w||f!==w.version||h!==n.toneMapping)&&(c.material.needsUpdate=!0,d=w,f=w.version,h=n.toneMapping),c.layers.enableAll(),S.unshift(c,c.geometry,c.material,0,0,null))}function p(S,E){S.getRGB(Il,S_(n)),i.buffers.color.setClear(Il.r,Il.g,Il.b,E,o)}return{getClearColor:function(){return a},setClearColor:function(S,E=1){a.set(S),l=E,p(a,l)},getClearAlpha:function(){return l},setClearAlpha:function(S){l=S,p(a,l)},render:x,addToRenderList:m}}function nA(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=f(null),s=r,o=!1;function a(v,M,j,z,Y){let J=!1,W=d(z,j,M);s!==W&&(s=W,c(s.object)),J=h(v,z,j,Y),J&&g(v,z,j,Y),Y!==null&&e.update(Y,n.ELEMENT_ARRAY_BUFFER),(J||o)&&(o=!1,w(v,M,j,z),Y!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(Y).buffer))}function l(){return n.createVertexArray()}function c(v){return n.bindVertexArray(v)}function u(v){return n.deleteVertexArray(v)}function d(v,M,j){let z=j.wireframe===!0,Y=i[v.id];Y===void 0&&(Y={},i[v.id]=Y);let J=Y[M.id];J===void 0&&(J={},Y[M.id]=J);let W=J[z];return W===void 0&&(W=f(l()),J[z]=W),W}function f(v){let M=[],j=[],z=[];for(let Y=0;Y<t;Y++)M[Y]=0,j[Y]=0,z[Y]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:M,enabledAttributes:j,attributeDivisors:z,object:v,attributes:{},index:null}}function h(v,M,j,z){let Y=s.attributes,J=M.attributes,W=0,K=j.getAttributes();for(let G in K)if(K[G].location>=0){let de=Y[G],_e=J[G];if(_e===void 0&&(G==="instanceMatrix"&&v.instanceMatrix&&(_e=v.instanceMatrix),G==="instanceColor"&&v.instanceColor&&(_e=v.instanceColor)),de===void 0||de.attribute!==_e||_e&&de.data!==_e.data)return!0;W++}return s.attributesNum!==W||s.index!==z}function g(v,M,j,z){let Y={},J=M.attributes,W=0,K=j.getAttributes();for(let G in K)if(K[G].location>=0){let de=J[G];de===void 0&&(G==="instanceMatrix"&&v.instanceMatrix&&(de=v.instanceMatrix),G==="instanceColor"&&v.instanceColor&&(de=v.instanceColor));let _e={};_e.attribute=de,de&&de.data&&(_e.data=de.data),Y[G]=_e,W++}s.attributes=Y,s.attributesNum=W,s.index=z}function x(){let v=s.newAttributes;for(let M=0,j=v.length;M<j;M++)v[M]=0}function m(v){p(v,0)}function p(v,M){let j=s.newAttributes,z=s.enabledAttributes,Y=s.attributeDivisors;j[v]=1,z[v]===0&&(n.enableVertexAttribArray(v),z[v]=1),Y[v]!==M&&(n.vertexAttribDivisor(v,M),Y[v]=M)}function S(){let v=s.newAttributes,M=s.enabledAttributes;for(let j=0,z=M.length;j<z;j++)M[j]!==v[j]&&(n.disableVertexAttribArray(j),M[j]=0)}function E(v,M,j,z,Y,J,W){W===!0?n.vertexAttribIPointer(v,M,j,Y,J):n.vertexAttribPointer(v,M,j,z,Y,J)}function w(v,M,j,z){x();let Y=z.attributes,J=j.getAttributes(),W=M.defaultAttributeValues;for(let K in J){let G=J[K];if(G.location>=0){let ue=Y[K];if(ue===void 0&&(K==="instanceMatrix"&&v.instanceMatrix&&(ue=v.instanceMatrix),K==="instanceColor"&&v.instanceColor&&(ue=v.instanceColor)),ue!==void 0){let de=ue.normalized,_e=ue.itemSize,Je=e.get(ue);if(Je===void 0)continue;let ot=Je.buffer,$=Je.type,ee=Je.bytesPerElement,ve=$===n.INT||$===n.UNSIGNED_INT||ue.gpuType===Gh;if(ue.isInterleavedBufferAttribute){let fe=ue.data,Pe=fe.stride,Se=ue.offset;if(fe.isInstancedInterleavedBuffer){for(let We=0;We<G.locationSize;We++)p(G.location+We,fe.meshPerAttribute);v.isInstancedMesh!==!0&&z._maxInstanceCount===void 0&&(z._maxInstanceCount=fe.meshPerAttribute*fe.count)}else for(let We=0;We<G.locationSize;We++)m(G.location+We);n.bindBuffer(n.ARRAY_BUFFER,ot);for(let We=0;We<G.locationSize;We++)E(G.location+We,_e/G.locationSize,$,de,Pe*ee,(Se+_e/G.locationSize*We)*ee,ve)}else{if(ue.isInstancedBufferAttribute){for(let fe=0;fe<G.locationSize;fe++)p(G.location+fe,ue.meshPerAttribute);v.isInstancedMesh!==!0&&z._maxInstanceCount===void 0&&(z._maxInstanceCount=ue.meshPerAttribute*ue.count)}else for(let fe=0;fe<G.locationSize;fe++)m(G.location+fe);n.bindBuffer(n.ARRAY_BUFFER,ot);for(let fe=0;fe<G.locationSize;fe++)E(G.location+fe,_e/G.locationSize,$,de,_e*ee,_e/G.locationSize*fe*ee,ve)}}else if(W!==void 0){let de=W[K];if(de!==void 0)switch(de.length){case 2:n.vertexAttrib2fv(G.location,de);break;case 3:n.vertexAttrib3fv(G.location,de);break;case 4:n.vertexAttrib4fv(G.location,de);break;default:n.vertexAttrib1fv(G.location,de)}}}}S()}function L(){U();for(let v in i){let M=i[v];for(let j in M){let z=M[j];for(let Y in z)u(z[Y].object),delete z[Y];delete M[j]}delete i[v]}}function T(v){if(i[v.id]===void 0)return;let M=i[v.id];for(let j in M){let z=M[j];for(let Y in z)u(z[Y].object),delete z[Y];delete M[j]}delete i[v.id]}function C(v){for(let M in i){let j=i[M];if(j[v.id]===void 0)continue;let z=j[v.id];for(let Y in z)u(z[Y].object),delete z[Y];delete j[v.id]}}function U(){ie(),o=!0,s!==r&&(s=r,c(s.object))}function ie(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:U,resetDefaultState:ie,dispose:L,releaseStatesOfGeometry:T,releaseStatesOfProgram:C,initAttributes:x,enableAttribute:m,disableUnusedAttributes:S}}function iA(n,e,t){let i;function r(c){i=c}function s(c,u){n.drawArrays(i,c,u),t.update(u,i,1)}function o(c,u,d){d!==0&&(n.drawArraysInstanced(i,c,u,d),t.update(u,i,d))}function a(c,u,d){if(d===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,c,0,u,0,d);let h=0;for(let g=0;g<d;g++)h+=u[g];t.update(h,i,1)}function l(c,u,d,f){if(d===0)return;let h=e.get("WEBGL_multi_draw");if(h===null)for(let g=0;g<c.length;g++)o(c[g],u[g],f[g]);else{h.multiDrawArraysInstancedWEBGL(i,c,0,u,0,f,0,d);let g=0;for(let x=0;x<d;x++)g+=u[x];for(let x=0;x<f.length;x++)t.update(g,i,f[x])}}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a,this.renderMultiDrawInstances=l}function rA(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let C=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(C.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(C){return!(C!==Fn&&i.convert(C)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(C){let U=C===Ao&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(C!==vi&&i.convert(C)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&C!==hi&&!U)}function l(C){if(C==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";C="mediump"}return C==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let c=t.precision!==void 0?t.precision:"highp",u=l(c);u!==c&&(console.warn("THREE.WebGLRenderer:",c,"not supported, using",u,"instead."),c=u);let d=t.logarithmicDepthBuffer===!0,f=t.reverseDepthBuffer===!0&&e.has("EXT_clip_control");if(f===!0){let C=e.get("EXT_clip_control");C.clipControlEXT(C.LOWER_LEFT_EXT,C.ZERO_TO_ONE_EXT)}let h=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),g=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),x=n.getParameter(n.MAX_TEXTURE_SIZE),m=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),S=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),E=n.getParameter(n.MAX_VARYING_VECTORS),w=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),L=g>0,T=n.getParameter(n.MAX_SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:l,textureFormatReadable:o,textureTypeReadable:a,precision:c,logarithmicDepthBuffer:d,reverseDepthBuffer:f,maxTextures:h,maxVertexTextures:g,maxTextureSize:x,maxCubemapSize:m,maxAttributes:p,maxVertexUniforms:S,maxVaryings:E,maxFragmentUniforms:w,vertexTextures:L,maxSamples:T}}function sA(n){let e=this,t=null,i=0,r=!1,s=!1,o=new di,a=new Le,l={value:null,needsUpdate:!1};this.uniform=l,this.numPlanes=0,this.numIntersection=0,this.init=function(d,f){let h=d.length!==0||f||i!==0||r;return r=f,i=d.length,h},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(d,f){t=u(d,f,0)},this.setState=function(d,f,h){let g=d.clippingPlanes,x=d.clipIntersection,m=d.clipShadows,p=n.get(d);if(!r||g===null||g.length===0||s&&!m)s?u(null):c();else{let S=s?0:i,E=S*4,w=p.clippingState||null;l.value=w,w=u(g,f,E,h);for(let L=0;L!==E;++L)w[L]=t[L];p.clippingState=w,this.numIntersection=x?this.numPlanes:0,this.numPlanes+=S}};function c(){l.value!==t&&(l.value=t,l.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(d,f,h,g){let x=d!==null?d.length:0,m=null;if(x!==0){if(m=l.value,g!==!0||m===null){let p=h+x*4,S=f.matrixWorldInverse;a.getNormalMatrix(S),(m===null||m.length<p)&&(m=new Float32Array(p));for(let E=0,w=h;E!==x;++E,w+=4)o.copy(d[E]).applyMatrix4(S,a),o.normal.toArray(m,w),m[w+3]=o.constant}l.value=m,l.needsUpdate=!0}return e.numPlanes=x,e.numIntersection=0,m}}function oA(n){let e=new WeakMap;function t(o,a){return a===kf?o.mapping=Rs:a===Uf&&(o.mapping=Ns),o}function i(o){if(o&&o.isTexture){let a=o.mapping;if(a===kf||a===Uf)if(e.has(o)){let l=e.get(o).texture;return t(l,o.mapping)}else{let l=o.image;if(l&&l.height>0){let c=new xh(l.height);return c.fromEquirectangularTexture(n,o),e.set(o,c),o.addEventListener("dispose",r),t(c.texture,o.mapping)}else return null}}return o}function r(o){let a=o.target;a.removeEventListener("dispose",r);let l=e.get(a);l!==void 0&&(e.delete(a),l.dispose())}function s(){e=new WeakMap}return{get:i,dispose:s}}var tc=class extends Jl{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,l=r-t;if(this.view!==null&&this.view.enabled){let c=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=c*this.view.offsetX,o=s+c*this.view.width,a-=u*this.view.offsetY,l=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,l,this.near,this.far,this.coordinateSystem),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}},Ss=4,Uy=[.125,.215,.35,.446,.526,.582],yr=20,Mf=new tc,Vy=new qe,Ef=null,bf=0,wf=0,Sf=!1,mr=(1+Math.sqrt(5))/2,ws=1/mr,By=[new V(-mr,ws,0),new V(mr,ws,0),new V(-ws,0,mr),new V(ws,0,mr),new V(0,mr,-ws),new V(0,mr,ws),new V(-1,1,-1),new V(1,1,-1),new V(-1,1,1),new V(1,1,1)],nc=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._lodPlanes=[],this._sizeLods=[],this._sigmas=[],this._blurMaterial=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._compileMaterial(this._blurMaterial)}fromScene(e,t=0,i=.1,r=100){Ef=this._renderer.getRenderTarget(),bf=this._renderer.getActiveCubeFace(),wf=this._renderer.getActiveMipmapLevel(),Sf=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(256);let s=this._allocateTargets();return s.depthBuffer=!0,this._sceneToCubeUV(e,i,r,s),t>0&&this._blur(s,0,0,t),this._applyPMREM(s),this._cleanup(s),s}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=Gy(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=zy(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose()}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodPlanes.length;e++)this._lodPlanes[e].dispose()}_cleanup(e){this._renderer.setRenderTarget(Ef,bf,wf),this._renderer.xr.enabled=Sf,e.scissorTest=!1,Rl(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===Rs||e.mapping===Ns?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),Ef=this._renderer.getRenderTarget(),bf=this._renderer.getActiveCubeFace(),wf=this._renderer.getActiveMipmapLevel(),Sf=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:Pn,minFilter:Pn,generateMipmaps:!1,type:Ao,format:Fn,colorSpace:$i,depthBuffer:!1},r=Hy(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=Hy(e,t,i);let{_lodMax:s}=this;({sizeLods:this._sizeLods,lodPlanes:this._lodPlanes,sigmas:this._sigmas}=aA(s)),this._blurMaterial=lA(s,e,t)}return r}_compileMaterial(e){let t=new rn(this._lodPlanes[0],e);this._renderer.compile(t,Mf)}_sceneToCubeUV(e,t,i,r){let a=new Yt(90,1,t,i),l=[1,-1,1,1,1,1],c=[1,1,1,-1,-1,-1],u=this._renderer,d=u.autoClear,f=u.toneMapping;u.getClearColor(Vy),u.toneMapping=Hi,u.autoClear=!1;let h=new Yl({name:"PMREM.Background",side:sn,depthWrite:!1,depthTest:!1}),g=new rn(new ji,h),x=!1,m=e.background;m?m.isColor&&(h.color.copy(m),e.background=null,x=!0):(h.color.copy(Vy),x=!0);for(let p=0;p<6;p++){let S=p%3;S===0?(a.up.set(0,l[p],0),a.lookAt(c[p],0,0)):S===1?(a.up.set(0,0,l[p]),a.lookAt(0,c[p],0)):(a.up.set(0,l[p],0),a.lookAt(0,0,c[p]));let E=this._cubeSize;Rl(r,S*E,p>2?E:0,E,E),u.setRenderTarget(r),x&&u.render(g,a),u.render(e,a)}g.geometry.dispose(),g.material.dispose(),u.toneMapping=f,u.autoClear=d,e.background=m}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===Rs||e.mapping===Ns;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=Gy()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=zy());let s=r?this._cubemapMaterial:this._equirectMaterial,o=new rn(this._lodPlanes[0],s),a=s.uniforms;a.envMap.value=e;let l=this._cubeSize;Rl(t,0,0,3*l,2*l),i.setRenderTarget(t),i.render(o,Mf)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodPlanes.length;for(let s=1;s<r;s++){let o=Math.sqrt(this._sigmas[s]*this._sigmas[s]-this._sigmas[s-1]*this._sigmas[s-1]),a=By[(r-s-1)%By.length];this._blur(e,s-1,s,o,a)}t.autoClear=i}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let l=this._renderer,c=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&console.error("blur direction must be either latitudinal or longitudinal!");let u=3,d=new rn(this._lodPlanes[r],c),f=c.uniforms,h=this._sizeLods[i]-1,g=isFinite(s)?Math.PI/(2*h):2*Math.PI/(2*yr-1),x=s/g,m=isFinite(s)?1+Math.floor(u*x):yr;m>yr&&console.warn(`sigmaRadians, ${s}, is too large and will clip, as it requested ${m} samples when the maximum is set to ${yr}`);let p=[],S=0;for(let C=0;C<yr;++C){let U=C/x,ie=Math.exp(-U*U/2);p.push(ie),C===0?S+=ie:C<m&&(S+=2*ie)}for(let C=0;C<p.length;C++)p[C]=p[C]/S;f.envMap.value=e.texture,f.samples.value=m,f.weights.value=p,f.latitudinal.value=o==="latitudinal",a&&(f.poleAxis.value=a);let{_lodMax:E}=this;f.dTheta.value=g,f.mipInt.value=E-i;let w=this._sizeLods[r],L=3*w*(r>E-Ss?r-E+Ss:0),T=4*(this._cubeSize-w);Rl(t,L,T,3*w,2*w),l.setRenderTarget(t),l.render(d,Mf)}};function aA(n){let e=[],t=[],i=[],r=n,s=n-Ss+1+Uy.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);t.push(a);let l=1/a;o>n-Ss?l=Uy[o-n+Ss-1]:o===0&&(l=0),i.push(l);let c=1/(a-2),u=-c,d=1+c,f=[u,u,d,u,d,d,u,u,d,d,u,d],h=6,g=6,x=3,m=2,p=1,S=new Float32Array(x*g*h),E=new Float32Array(m*g*h),w=new Float32Array(p*g*h);for(let T=0;T<h;T++){let C=T%3*2/3-1,U=T>2?0:-1,ie=[C,U,0,C+2/3,U,0,C+2/3,U+1,0,C,U,0,C+2/3,U+1,0,C,U+1,0];S.set(ie,x*g*T),E.set(f,m*g*T);let v=[T,T,T,T,T,T];w.set(v,p*g*T)}let L=new Sr;L.setAttribute("position",new Mn(S,x)),L.setAttribute("uv",new Mn(E,m)),L.setAttribute("faceIndex",new Mn(w,p)),e.push(L),r>Ss&&r--}return{lodPlanes:e,sizeLods:t,sigmas:i}}function Hy(n,e,t){let i=new yi(n,e,t);return i.texture.mapping=uc,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function Rl(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function lA(n,e,t){let i=new Float32Array(yr),r=new V(0,1,0);return new Zn({name:"SphericalGaussianBlur",defines:{n:yr,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:Zh(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform int samples;
			uniform float weights[ n ];
			uniform bool latitudinal;
			uniform float dTheta;
			uniform float mipInt;
			uniform vec3 poleAxis;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			vec3 getSample( float theta, vec3 axis ) {

				float cosTheta = cos( theta );
				// Rodrigues' axis-angle rotation
				vec3 sampleDirection = vOutputDirection * cosTheta
					+ cross( axis, vOutputDirection ) * sin( theta )
					+ axis * dot( axis, vOutputDirection ) * ( 1.0 - cosTheta );

				return bilinearCubeUV( envMap, sampleDirection, mipInt );

			}

			void main() {

				vec3 axis = latitudinal ? poleAxis : cross( poleAxis, vOutputDirection );

				if ( all( equal( axis, vec3( 0.0 ) ) ) ) {

					axis = vec3( vOutputDirection.z, 0.0, - vOutputDirection.x );

				}

				axis = normalize( axis );

				gl_FragColor = vec4( 0.0, 0.0, 0.0, 1.0 );
				gl_FragColor.rgb += weights[ 0 ] * getSample( 0.0, axis );

				for ( int i = 1; i < n; i++ ) {

					if ( i >= samples ) {

						break;

					}

					float theta = dTheta * float( i );
					gl_FragColor.rgb += weights[ i ] * getSample( -1.0 * theta, axis );
					gl_FragColor.rgb += weights[ i ] * getSample( theta, axis );

				}

			}
		`,blending:Bi,depthTest:!1,depthWrite:!1})}function zy(){return new Zn({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:Zh(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;

			#include <common>

			void main() {

				vec3 outputDirection = normalize( vOutputDirection );
				vec2 uv = equirectUv( outputDirection );

				gl_FragColor = vec4( texture2D ( envMap, uv ).rgb, 1.0 );

			}
		`,blending:Bi,depthTest:!1,depthWrite:!1})}function Gy(){return new Zn({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:Zh(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:Bi,depthTest:!1,depthWrite:!1})}function Zh(){return`

		precision mediump float;
		precision mediump int;

		attribute float faceIndex;

		varying vec3 vOutputDirection;

		// RH coordinate system; PMREM face-indexing convention
		vec3 getDirection( vec2 uv, float face ) {

			uv = 2.0 * uv - 1.0;

			vec3 direction = vec3( uv, 1.0 );

			if ( face == 0.0 ) {

				direction = direction.zyx; // ( 1, v, u ) pos x

			} else if ( face == 1.0 ) {

				direction = direction.xzy;
				direction.xz *= -1.0; // ( -u, 1, -v ) pos y

			} else if ( face == 2.0 ) {

				direction.x *= -1.0; // ( -u, v, 1 ) pos z

			} else if ( face == 3.0 ) {

				direction = direction.zyx;
				direction.xz *= -1.0; // ( -1, v, -u ) neg x

			} else if ( face == 4.0 ) {

				direction = direction.xzy;
				direction.xy *= -1.0; // ( -u, -1, v ) neg y

			} else if ( face == 5.0 ) {

				direction.z *= -1.0; // ( u, v, -1 ) neg z

			}

			return direction;

		}

		void main() {

			vOutputDirection = getDirection( uv, faceIndex );
			gl_Position = vec4( position, 1.0 );

		}
	`}function cA(n){let e=new WeakMap,t=null;function i(a){if(a&&a.isTexture){let l=a.mapping,c=l===kf||l===Uf,u=l===Rs||l===Ns;if(c||u){let d=e.get(a),f=d!==void 0?d.texture.pmremVersion:0;if(a.isRenderTargetTexture&&a.pmremVersion!==f)return t===null&&(t=new nc(n)),d=c?t.fromEquirectangular(a,d):t.fromCubemap(a,d),d.texture.pmremVersion=a.pmremVersion,e.set(a,d),d.texture;if(d!==void 0)return d.texture;{let h=a.image;return c&&h&&h.height>0||u&&h&&r(h)?(t===null&&(t=new nc(n)),d=c?t.fromEquirectangular(a):t.fromCubemap(a),d.texture.pmremVersion=a.pmremVersion,e.set(a,d),a.addEventListener("dispose",s),d.texture):null}}}return a}function r(a){let l=0,c=6;for(let u=0;u<c;u++)a[u]!==void 0&&l++;return l===c}function s(a){let l=a.target;l.removeEventListener("dispose",s);let c=e.get(l);c!==void 0&&(e.delete(l),c.dispose())}function o(){e=new WeakMap,t!==null&&(t.dispose(),t=null)}return{get:i,dispose:o}}function uA(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r;switch(i){case"WEBGL_depth_texture":r=n.getExtension("WEBGL_depth_texture")||n.getExtension("MOZ_WEBGL_depth_texture")||n.getExtension("WEBKIT_WEBGL_depth_texture");break;case"EXT_texture_filter_anisotropic":r=n.getExtension("EXT_texture_filter_anisotropic")||n.getExtension("MOZ_EXT_texture_filter_anisotropic")||n.getExtension("WEBKIT_EXT_texture_filter_anisotropic");break;case"WEBGL_compressed_texture_s3tc":r=n.getExtension("WEBGL_compressed_texture_s3tc")||n.getExtension("MOZ_WEBGL_compressed_texture_s3tc")||n.getExtension("WEBKIT_WEBGL_compressed_texture_s3tc");break;case"WEBGL_compressed_texture_pvrtc":r=n.getExtension("WEBGL_compressed_texture_pvrtc")||n.getExtension("WEBKIT_WEBGL_compressed_texture_pvrtc");break;default:r=n.getExtension(i)}return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&Vl("THREE.WebGLRenderer: "+i+" extension not supported."),r}}}function dA(n,e,t,i){let r={},s=new WeakMap;function o(d){let f=d.target;f.index!==null&&e.remove(f.index);for(let g in f.attributes)e.remove(f.attributes[g]);for(let g in f.morphAttributes){let x=f.morphAttributes[g];for(let m=0,p=x.length;m<p;m++)e.remove(x[m])}f.removeEventListener("dispose",o),delete r[f.id];let h=s.get(f);h&&(e.remove(h),s.delete(f)),i.releaseStatesOfGeometry(f),f.isInstancedBufferGeometry===!0&&delete f._maxInstanceCount,t.memory.geometries--}function a(d,f){return r[f.id]===!0||(f.addEventListener("dispose",o),r[f.id]=!0,t.memory.geometries++),f}function l(d){let f=d.attributes;for(let g in f)e.update(f[g],n.ARRAY_BUFFER);let h=d.morphAttributes;for(let g in h){let x=h[g];for(let m=0,p=x.length;m<p;m++)e.update(x[m],n.ARRAY_BUFFER)}}function c(d){let f=[],h=d.index,g=d.attributes.position,x=0;if(h!==null){let S=h.array;x=h.version;for(let E=0,w=S.length;E<w;E+=3){let L=S[E+0],T=S[E+1],C=S[E+2];f.push(L,T,T,C,C,L)}}else if(g!==void 0){let S=g.array;x=g.version;for(let E=0,w=S.length/3-1;E<w;E+=3){let L=E+0,T=E+1,C=E+2;f.push(L,T,T,C,C,L)}}else return;let m=new(b_(f)?Kl:Zl)(f,1);m.version=x;let p=s.get(d);p&&e.remove(p),s.set(d,m)}function u(d){let f=s.get(d);if(f){let h=d.index;h!==null&&f.version<h.version&&c(d)}else c(d);return s.get(d)}return{get:a,update:l,getWireframeAttribute:u}}function fA(n,e,t){let i;function r(f){i=f}let s,o;function a(f){s=f.type,o=f.bytesPerElement}function l(f,h){n.drawElements(i,h,s,f*o),t.update(h,i,1)}function c(f,h,g){g!==0&&(n.drawElementsInstanced(i,h,s,f*o,g),t.update(h,i,g))}function u(f,h,g){if(g===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,h,0,s,f,0,g);let m=0;for(let p=0;p<g;p++)m+=h[p];t.update(m,i,1)}function d(f,h,g,x){if(g===0)return;let m=e.get("WEBGL_multi_draw");if(m===null)for(let p=0;p<f.length;p++)c(f[p]/o,h[p],x[p]);else{m.multiDrawElementsInstancedWEBGL(i,h,0,s,f,0,x,0,g);let p=0;for(let S=0;S<g;S++)p+=h[S];for(let S=0;S<x.length;S++)t.update(p,i,x[S])}}this.setMode=r,this.setIndex=a,this.render=l,this.renderInstances=c,this.renderMultiDraw=u,this.renderMultiDrawInstances=d}function hA(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:console.error("THREE.WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function pA(n,e,t){let i=new WeakMap,r=new Et;function s(o,a,l){let c=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,d=u!==void 0?u.length:0,f=i.get(a);if(f===void 0||f.count!==d){let v=function(){U.dispose(),i.delete(a),a.removeEventListener("dispose",v)};var h=v;f!==void 0&&f.texture.dispose();let g=a.morphAttributes.position!==void 0,x=a.morphAttributes.normal!==void 0,m=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],S=a.morphAttributes.normal||[],E=a.morphAttributes.color||[],w=0;g===!0&&(w=1),x===!0&&(w=2),m===!0&&(w=3);let L=a.attributes.position.count*w,T=1;L>e.maxTextureSize&&(T=Math.ceil(L/e.maxTextureSize),L=e.maxTextureSize);let C=new Float32Array(L*T*4*d),U=new ql(C,L,T,d);U.type=hi,U.needsUpdate=!0;let ie=w*4;for(let M=0;M<d;M++){let j=p[M],z=S[M],Y=E[M],J=L*T*4*M;for(let W=0;W<j.count;W++){let K=W*ie;g===!0&&(r.fromBufferAttribute(j,W),C[J+K+0]=r.x,C[J+K+1]=r.y,C[J+K+2]=r.z,C[J+K+3]=0),x===!0&&(r.fromBufferAttribute(z,W),C[J+K+4]=r.x,C[J+K+5]=r.y,C[J+K+6]=r.z,C[J+K+7]=0),m===!0&&(r.fromBufferAttribute(Y,W),C[J+K+8]=r.x,C[J+K+9]=r.y,C[J+K+10]=r.z,C[J+K+11]=Y.itemSize===4?r.w:1)}}f={count:d,texture:U,size:new st(L,T)},i.set(a,f),a.addEventListener("dispose",v)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)l.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let g=0;for(let m=0;m<c.length;m++)g+=c[m];let x=a.morphTargetsRelative?1:1-g;l.getUniforms().setValue(n,"morphTargetBaseInfluence",x),l.getUniforms().setValue(n,"morphTargetInfluences",c)}l.getUniforms().setValue(n,"morphTargetsTexture",f.texture,t),l.getUniforms().setValue(n,"morphTargetsTextureSize",f.size)}return{update:s}}function mA(n,e,t,i){let r=new WeakMap;function s(l){let c=i.render.frame,u=l.geometry,d=e.get(l,u);if(r.get(d)!==c&&(e.update(d),r.set(d,c)),l.isInstancedMesh&&(l.hasEventListener("dispose",a)===!1&&l.addEventListener("dispose",a),r.get(l)!==c&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),r.set(l,c))),l.isSkinnedMesh){let f=l.skeleton;r.get(f)!==c&&(f.update(),r.set(f,c))}return d}function o(){r=new WeakMap}function a(l){let c=l.target;c.removeEventListener("dispose",a),t.remove(c.instanceMatrix),c.instanceColor!==null&&t.remove(c.instanceColor)}return{update:s,dispose:o}}var ic=class extends Tr{constructor(e,t,i,r,s,o,a,l,c,u=Ds){if(u!==Ds&&u!==Fs)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");i===void 0&&u===Ds&&(i=Mr),i===void 0&&u===Fs&&(i=Ps),super(null,r,s,o,a,l,u,i,c),this.isDepthTexture=!0,this.image={width:e,height:t},this.magFilter=a!==void 0?a:xn,this.minFilter=l!==void 0?l:xn,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},D_=new Tr,Wy=new ic(1,1),T_=new ql,A_=new vh,I_=new Ql,jy=[],$y=[],qy=new Float32Array(16),Xy=new Float32Array(9),Yy=new Float32Array(4);function ks(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=jy[r];if(s===void 0&&(s=new Float32Array(r),jy[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function Rt(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function Nt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function fc(n,e){let t=$y[e];t===void 0&&(t=new Int32Array(e),$y[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function gA(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function vA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Rt(t,e))return;n.uniform2fv(this.addr,e),Nt(t,e)}}function yA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(Rt(t,e))return;n.uniform3fv(this.addr,e),Nt(t,e)}}function _A(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Rt(t,e))return;n.uniform4fv(this.addr,e),Nt(t,e)}}function xA(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Rt(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),Nt(t,e)}else{if(Rt(t,i))return;Yy.set(i),n.uniformMatrix2fv(this.addr,!1,Yy),Nt(t,i)}}function MA(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Rt(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),Nt(t,e)}else{if(Rt(t,i))return;Xy.set(i),n.uniformMatrix3fv(this.addr,!1,Xy),Nt(t,i)}}function EA(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Rt(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),Nt(t,e)}else{if(Rt(t,i))return;qy.set(i),n.uniformMatrix4fv(this.addr,!1,qy),Nt(t,i)}}function bA(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function wA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Rt(t,e))return;n.uniform2iv(this.addr,e),Nt(t,e)}}function SA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Rt(t,e))return;n.uniform3iv(this.addr,e),Nt(t,e)}}function CA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Rt(t,e))return;n.uniform4iv(this.addr,e),Nt(t,e)}}function DA(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function TA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Rt(t,e))return;n.uniform2uiv(this.addr,e),Nt(t,e)}}function AA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Rt(t,e))return;n.uniform3uiv(this.addr,e),Nt(t,e)}}function IA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Rt(t,e))return;n.uniform4uiv(this.addr,e),Nt(t,e)}}function RA(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(Wy.compareFunction=E_,s=Wy):s=D_,t.setTexture2D(e||s,r)}function NA(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||A_,r)}function PA(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||I_,r)}function FA(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||T_,r)}function OA(n){switch(n){case 5126:return gA;case 35664:return vA;case 35665:return yA;case 35666:return _A;case 35674:return xA;case 35675:return MA;case 35676:return EA;case 5124:case 35670:return bA;case 35667:case 35671:return wA;case 35668:case 35672:return SA;case 35669:case 35673:return CA;case 5125:return DA;case 36294:return TA;case 36295:return AA;case 36296:return IA;case 35678:case 36198:case 36298:case 36306:case 35682:return RA;case 35679:case 36299:case 36307:return NA;case 35680:case 36300:case 36308:case 36293:return PA;case 36289:case 36303:case 36311:case 36292:return FA}}function LA(n,e){n.uniform1fv(this.addr,e)}function kA(n,e){let t=ks(e,this.size,2);n.uniform2fv(this.addr,t)}function UA(n,e){let t=ks(e,this.size,3);n.uniform3fv(this.addr,t)}function VA(n,e){let t=ks(e,this.size,4);n.uniform4fv(this.addr,t)}function BA(n,e){let t=ks(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function HA(n,e){let t=ks(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function zA(n,e){let t=ks(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function GA(n,e){n.uniform1iv(this.addr,e)}function WA(n,e){n.uniform2iv(this.addr,e)}function jA(n,e){n.uniform3iv(this.addr,e)}function $A(n,e){n.uniform4iv(this.addr,e)}function qA(n,e){n.uniform1uiv(this.addr,e)}function XA(n,e){n.uniform2uiv(this.addr,e)}function YA(n,e){n.uniform3uiv(this.addr,e)}function ZA(n,e){n.uniform4uiv(this.addr,e)}function KA(n,e,t){let i=this.cache,r=e.length,s=fc(t,r);Rt(i,s)||(n.uniform1iv(this.addr,s),Nt(i,s));for(let o=0;o!==r;++o)t.setTexture2D(e[o]||D_,s[o])}function JA(n,e,t){let i=this.cache,r=e.length,s=fc(t,r);Rt(i,s)||(n.uniform1iv(this.addr,s),Nt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||A_,s[o])}function QA(n,e,t){let i=this.cache,r=e.length,s=fc(t,r);Rt(i,s)||(n.uniform1iv(this.addr,s),Nt(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||I_,s[o])}function eI(n,e,t){let i=this.cache,r=e.length,s=fc(t,r);Rt(i,s)||(n.uniform1iv(this.addr,s),Nt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||T_,s[o])}function tI(n){switch(n){case 5126:return LA;case 35664:return kA;case 35665:return UA;case 35666:return VA;case 35674:return BA;case 35675:return HA;case 35676:return zA;case 5124:case 35670:return GA;case 35667:case 35671:return WA;case 35668:case 35672:return jA;case 35669:case 35673:return $A;case 5125:return qA;case 36294:return XA;case 36295:return YA;case 36296:return ZA;case 35678:case 36198:case 36298:case 36306:case 35682:return KA;case 35679:case 36299:case 36307:return JA;case 35680:case 36300:case 36308:case 36293:return QA;case 36289:case 36303:case 36311:case 36292:return eI}}var Mh=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=OA(t.type)}},Eh=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=tI(t.type)}},bh=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},Cf=/(\w+)(\])?(\[|\.)?/g;function Zy(n,e){n.seq.push(e),n.map[e.id]=e}function nI(n,e,t){let i=n.name,r=i.length;for(Cf.lastIndex=0;;){let s=Cf.exec(i),o=Cf.lastIndex,a=s[1],l=s[2]==="]",c=s[3];if(l&&(a=a|0),c===void 0||c==="["&&o+2===r){Zy(t,c===void 0?new Mh(a,n,e):new Eh(a,n,e));break}else{let d=t.map[a];d===void 0&&(d=new bh(a),Zy(t,d)),t=d}}}var As=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let r=0;r<i;++r){let s=e.getActiveUniform(t,r),o=e.getUniformLocation(t,s.name);nI(s,o,this)}}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],l=i[a.id];l.needsUpdate!==!1&&a.setValue(e,l.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function Ky(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var iI=37297,rI=0;function sI(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}function oI(n){let e=rt.getPrimaries(rt.workingColorSpace),t=rt.getPrimaries(n),i;switch(e===t?i="":e===Gl&&t===zl?i="LinearDisplayP3ToLinearSRGB":e===zl&&t===Gl&&(i="LinearSRGBToLinearDisplayP3"),n){case $i:case dc:return[i,"LinearTransferOETF"];case Xn:case Yh:return[i,"sRGBTransferOETF"];default:return console.warn("THREE.WebGLProgram: Unsupported color space:",n),[i,"LinearTransferOETF"]}}function Jy(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),r=n.getShaderInfoLog(e).trim();if(i&&r==="")return"";let s=/ERROR: 0:(\d+)/.exec(r);if(s){let o=parseInt(s[1]);return t.toUpperCase()+`

`+r+`

`+sI(n.getShaderSource(e),o)}else return r}function aI(n,e){let t=oI(e);return`vec4 ${n}( vec4 value ) { return ${t[0]}( ${t[1]}( value ) ); }`}function lI(n,e){let t;switch(e){case NS:t="Linear";break;case PS:t="Reinhard";break;case FS:t="Cineon";break;case OS:t="ACESFilmic";break;case kS:t="AgX";break;case US:t="Neutral";break;case LS:t="Custom";break;default:console.warn("THREE.WebGLProgram: Unsupported toneMapping:",e),t="Linear"}return"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var Nl=new V;function cI(){rt.getLuminanceCoefficients(Nl);let n=Nl.x.toFixed(4),e=Nl.y.toFixed(4),t=Nl.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function uI(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(bo).join(`
`)}function dI(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function fI(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function bo(n){return n!==""}function Qy(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function e_(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var hI=/^[ \t]*#include +<([\w\d./]+)>/gm;function wh(n){return n.replace(hI,mI)}var pI=new Map;function mI(n,e){let t=Oe[e];if(t===void 0){let i=pI.get(e);if(i!==void 0)t=Oe[i],console.warn('THREE.WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return wh(t)}var gI=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function t_(n){return n.replace(gI,vI)}function vI(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function n_(n){let e=`precision ${n.precision} float;
	precision ${n.precision} int;
	precision ${n.precision} sampler2D;
	precision ${n.precision} samplerCube;
	precision ${n.precision} sampler3D;
	precision ${n.precision} sampler2DArray;
	precision ${n.precision} sampler2DShadow;
	precision ${n.precision} samplerCubeShadow;
	precision ${n.precision} sampler2DArrayShadow;
	precision ${n.precision} isampler2D;
	precision ${n.precision} isampler3D;
	precision ${n.precision} isamplerCube;
	precision ${n.precision} isampler2DArray;
	precision ${n.precision} usampler2D;
	precision ${n.precision} usampler3D;
	precision ${n.precision} usamplerCube;
	precision ${n.precision} usampler2DArray;
	`;return n.precision==="highp"?e+=`
#define HIGH_PRECISION`:n.precision==="mediump"?e+=`
#define MEDIUM_PRECISION`:n.precision==="lowp"&&(e+=`
#define LOW_PRECISION`),e}function yI(n){let e="SHADOWMAP_TYPE_BASIC";return n.shadowMapType===c_?e="SHADOWMAP_TYPE_PCF":n.shadowMapType===dS?e="SHADOWMAP_TYPE_PCF_SOFT":n.shadowMapType===ui&&(e="SHADOWMAP_TYPE_VSM"),e}function _I(n){let e="ENVMAP_TYPE_CUBE";if(n.envMap)switch(n.envMapMode){case Rs:case Ns:e="ENVMAP_TYPE_CUBE";break;case uc:e="ENVMAP_TYPE_CUBE_UV";break}return e}function xI(n){let e="ENVMAP_MODE_REFLECTION";if(n.envMap)switch(n.envMapMode){case Ns:e="ENVMAP_MODE_REFRACTION";break}return e}function MI(n){let e="ENVMAP_BLENDING_NONE";if(n.envMap)switch(n.combine){case u_:e="ENVMAP_BLENDING_MULTIPLY";break;case IS:e="ENVMAP_BLENDING_MIX";break;case RS:e="ENVMAP_BLENDING_ADD";break}return e}function EI(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),7*16)),texelHeight:i,maxMip:t}}function bI(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,l=yI(t),c=_I(t),u=xI(t),d=MI(t),f=EI(t),h=uI(t),g=dI(s),x=r.createProgram(),m,p,S=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(bo).join(`
`),m.length>0&&(m+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(bo).join(`
`),p.length>0&&(p+=`
`)):(m=[n_(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+l:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGDEPTHBUF":"",t.reverseDepthBuffer?"#define USE_REVERSEDEPTHBUF":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(bo).join(`
`),p=[n_(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+c:"",t.envMap?"#define "+u:"",t.envMap?"#define "+d:"",f?"#define CUBEUV_TEXEL_WIDTH "+f.texelWidth:"",f?"#define CUBEUV_TEXEL_HEIGHT "+f.texelHeight:"",f?"#define CUBEUV_MAX_MIP "+f.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor||t.batchingColor?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+l:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.logarithmicDepthBuffer?"#define USE_LOGDEPTHBUF":"",t.reverseDepthBuffer?"#define USE_REVERSEDEPTHBUF":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==Hi?"#define TONE_MAPPING":"",t.toneMapping!==Hi?Oe.tonemapping_pars_fragment:"",t.toneMapping!==Hi?lI("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",Oe.colorspace_pars_fragment,aI("linearToOutputTexel",t.outputColorSpace),cI(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(bo).join(`
`)),o=wh(o),o=Qy(o,t),o=e_(o,t),a=wh(a),a=Qy(a,t),a=e_(a,t),o=t_(o),a=t_(a),t.isRawShaderMaterial!==!0&&(S=`#version 300 es
`,m=[h,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+m,p=["#define varying in",t.glslVersion===xy?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===xy?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let E=S+m+o,w=S+p+a,L=Ky(r,r.VERTEX_SHADER,E),T=Ky(r,r.FRAGMENT_SHADER,w);r.attachShader(x,L),r.attachShader(x,T),t.index0AttributeName!==void 0?r.bindAttribLocation(x,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(x,0,"position"),r.linkProgram(x);function C(M){if(n.debug.checkShaderErrors){let j=r.getProgramInfoLog(x).trim(),z=r.getShaderInfoLog(L).trim(),Y=r.getShaderInfoLog(T).trim(),J=!0,W=!0;if(r.getProgramParameter(x,r.LINK_STATUS)===!1)if(J=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,x,L,T);else{let K=Jy(r,L,"vertex"),G=Jy(r,T,"fragment");console.error("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(x,r.VALIDATE_STATUS)+`

Material Name: `+M.name+`
Material Type: `+M.type+`

Program Info Log: `+j+`
`+K+`
`+G)}else j!==""?console.warn("THREE.WebGLProgram: Program Info Log:",j):(z===""||Y==="")&&(W=!1);W&&(M.diagnostics={runnable:J,programLog:j,vertexShader:{log:z,prefix:m},fragmentShader:{log:Y,prefix:p}})}r.deleteShader(L),r.deleteShader(T),U=new As(r,x),ie=fI(r,x)}let U;this.getUniforms=function(){return U===void 0&&C(this),U};let ie;this.getAttributes=function(){return ie===void 0&&C(this),ie};let v=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return v===!1&&(v=r.getProgramParameter(x,iI)),v},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(x),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=rI++,this.cacheKey=e,this.usedTimes=1,this.program=x,this.vertexShader=L,this.fragmentShader=T,this}var wI=0,Sh=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new Ch(e),t.set(e,i)),i}},Ch=class{constructor(e){this.id=wI++,this.code=e,this.usedTimes=0}};function SI(n,e,t,i,r,s,o){let a=new Xl,l=new Sh,c=new Set,u=[],d=r.logarithmicDepthBuffer,f=r.reverseDepthBuffer,h=r.vertexTextures,g=r.precision,x={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distanceRGBA",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function m(v){return c.add(v),v===0?"uv":`uv${v}`}function p(v,M,j,z,Y){let J=z.fog,W=Y.geometry,K=v.isMeshStandardMaterial?z.environment:null,G=(v.isMeshStandardMaterial?t:e).get(v.envMap||K),ue=G&&G.mapping===uc?G.image.height:null,de=x[v.type];v.precision!==null&&(g=r.getMaxPrecision(v.precision),g!==v.precision&&console.warn("THREE.WebGLProgram.getParameters:",v.precision,"not supported, using",g,"instead."));let _e=W.morphAttributes.position||W.morphAttributes.normal||W.morphAttributes.color,Je=_e!==void 0?_e.length:0,ot=0;W.morphAttributes.position!==void 0&&(ot=1),W.morphAttributes.normal!==void 0&&(ot=2),W.morphAttributes.color!==void 0&&(ot=3);let $,ee,ve,fe;if(de){let Kt=Yn[de];$=Kt.vertexShader,ee=Kt.fragmentShader}else $=v.vertexShader,ee=v.fragmentShader,l.update(v),ve=l.getVertexShaderID(v),fe=l.getFragmentShaderID(v);let Pe=n.getRenderTarget(),Se=Y.isInstancedMesh===!0,We=Y.isBatchedMesh===!0,lt=!!v.map,je=!!v.matcap,D=!!G,on=!!v.aoMap,He=!!v.lightMap,Xe=!!v.bumpMap,Te=!!v.normalMap,dt=!!v.displacementMap,Re=!!v.emissiveMap,b=!!v.metalnessMap,y=!!v.roughnessMap,F=v.anisotropy>0,X=v.clearcoat>0,Q=v.dispersion>0,q=v.iridescence>0,Me=v.sheen>0,se=v.transmission>0,he=F&&!!v.anisotropyMap,Ye=X&&!!v.clearcoatMap,te=X&&!!v.clearcoatNormalMap,pe=X&&!!v.clearcoatRoughnessMap,Ae=q&&!!v.iridescenceMap,Ie=q&&!!v.iridescenceThicknessMap,me=Me&&!!v.sheenColorMap,ze=Me&&!!v.sheenRoughnessMap,Fe=!!v.specularMap,ct=!!v.specularColorMap,A=!!v.specularIntensityMap,le=se&&!!v.transmissionMap,H=se&&!!v.thicknessMap,Z=!!v.gradientMap,oe=!!v.alphaMap,ce=v.alphaTest>0,$e=!!v.alphaHash,Tt=!!v.extensions,Zt=Hi;v.toneMapped&&(Pe===null||Pe.isXRRenderTarget===!0)&&(Zt=n.toneMapping);let Qe={shaderID:de,shaderType:v.type,shaderName:v.name,vertexShader:$,fragmentShader:ee,defines:v.defines,customVertexShaderID:ve,customFragmentShaderID:fe,isRawShaderMaterial:v.isRawShaderMaterial===!0,glslVersion:v.glslVersion,precision:g,batching:We,batchingColor:We&&Y._colorsTexture!==null,instancing:Se,instancingColor:Se&&Y.instanceColor!==null,instancingMorph:Se&&Y.morphTexture!==null,supportsVertexTextures:h,outputColorSpace:Pe===null?n.outputColorSpace:Pe.isXRRenderTarget===!0?Pe.texture.colorSpace:$i,alphaToCoverage:!!v.alphaToCoverage,map:lt,matcap:je,envMap:D,envMapMode:D&&G.mapping,envMapCubeUVHeight:ue,aoMap:on,lightMap:He,bumpMap:Xe,normalMap:Te,displacementMap:h&&dt,emissiveMap:Re,normalMapObjectSpace:Te&&v.normalMapType===zS,normalMapTangentSpace:Te&&v.normalMapType===M_,metalnessMap:b,roughnessMap:y,anisotropy:F,anisotropyMap:he,clearcoat:X,clearcoatMap:Ye,clearcoatNormalMap:te,clearcoatRoughnessMap:pe,dispersion:Q,iridescence:q,iridescenceMap:Ae,iridescenceThicknessMap:Ie,sheen:Me,sheenColorMap:me,sheenRoughnessMap:ze,specularMap:Fe,specularColorMap:ct,specularIntensityMap:A,transmission:se,transmissionMap:le,thicknessMap:H,gradientMap:Z,opaque:v.transparent===!1&&v.blending===Cs&&v.alphaToCoverage===!1,alphaMap:oe,alphaTest:ce,alphaHash:$e,combine:v.combine,mapUv:lt&&m(v.map.channel),aoMapUv:on&&m(v.aoMap.channel),lightMapUv:He&&m(v.lightMap.channel),bumpMapUv:Xe&&m(v.bumpMap.channel),normalMapUv:Te&&m(v.normalMap.channel),displacementMapUv:dt&&m(v.displacementMap.channel),emissiveMapUv:Re&&m(v.emissiveMap.channel),metalnessMapUv:b&&m(v.metalnessMap.channel),roughnessMapUv:y&&m(v.roughnessMap.channel),anisotropyMapUv:he&&m(v.anisotropyMap.channel),clearcoatMapUv:Ye&&m(v.clearcoatMap.channel),clearcoatNormalMapUv:te&&m(v.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:pe&&m(v.clearcoatRoughnessMap.channel),iridescenceMapUv:Ae&&m(v.iridescenceMap.channel),iridescenceThicknessMapUv:Ie&&m(v.iridescenceThicknessMap.channel),sheenColorMapUv:me&&m(v.sheenColorMap.channel),sheenRoughnessMapUv:ze&&m(v.sheenRoughnessMap.channel),specularMapUv:Fe&&m(v.specularMap.channel),specularColorMapUv:ct&&m(v.specularColorMap.channel),specularIntensityMapUv:A&&m(v.specularIntensityMap.channel),transmissionMapUv:le&&m(v.transmissionMap.channel),thicknessMapUv:H&&m(v.thicknessMap.channel),alphaMapUv:oe&&m(v.alphaMap.channel),vertexTangents:!!W.attributes.tangent&&(Te||F),vertexColors:v.vertexColors,vertexAlphas:v.vertexColors===!0&&!!W.attributes.color&&W.attributes.color.itemSize===4,pointsUvs:Y.isPoints===!0&&!!W.attributes.uv&&(lt||oe),fog:!!J,useFog:v.fog===!0,fogExp2:!!J&&J.isFogExp2,flatShading:v.flatShading===!0,sizeAttenuation:v.sizeAttenuation===!0,logarithmicDepthBuffer:d,reverseDepthBuffer:f,skinning:Y.isSkinnedMesh===!0,morphTargets:W.morphAttributes.position!==void 0,morphNormals:W.morphAttributes.normal!==void 0,morphColors:W.morphAttributes.color!==void 0,morphTargetsCount:Je,morphTextureStride:ot,numDirLights:M.directional.length,numPointLights:M.point.length,numSpotLights:M.spot.length,numSpotLightMaps:M.spotLightMap.length,numRectAreaLights:M.rectArea.length,numHemiLights:M.hemi.length,numDirLightShadows:M.directionalShadowMap.length,numPointLightShadows:M.pointShadowMap.length,numSpotLightShadows:M.spotShadowMap.length,numSpotLightShadowsWithMaps:M.numSpotLightShadowsWithMaps,numLightProbes:M.numLightProbes,numClippingPlanes:o.numPlanes,numClipIntersection:o.numIntersection,dithering:v.dithering,shadowMapEnabled:n.shadowMap.enabled&&j.length>0,shadowMapType:n.shadowMap.type,toneMapping:Zt,decodeVideoTexture:lt&&v.map.isVideoTexture===!0&&rt.getTransfer(v.map.colorSpace)===mt,premultipliedAlpha:v.premultipliedAlpha,doubleSided:v.side===fi,flipSided:v.side===sn,useDepthPacking:v.depthPacking>=0,depthPacking:v.depthPacking||0,index0AttributeName:v.index0AttributeName,extensionClipCullDistance:Tt&&v.extensions.clipCullDistance===!0&&i.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(Tt&&v.extensions.multiDraw===!0||We)&&i.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:i.has("KHR_parallel_shader_compile"),customProgramCacheKey:v.customProgramCacheKey()};return Qe.vertexUv1s=c.has(1),Qe.vertexUv2s=c.has(2),Qe.vertexUv3s=c.has(3),c.clear(),Qe}function S(v){let M=[];if(v.shaderID?M.push(v.shaderID):(M.push(v.customVertexShaderID),M.push(v.customFragmentShaderID)),v.defines!==void 0)for(let j in v.defines)M.push(j),M.push(v.defines[j]);return v.isRawShaderMaterial===!1&&(E(M,v),w(M,v),M.push(n.outputColorSpace)),M.push(v.customProgramCacheKey),M.join()}function E(v,M){v.push(M.precision),v.push(M.outputColorSpace),v.push(M.envMapMode),v.push(M.envMapCubeUVHeight),v.push(M.mapUv),v.push(M.alphaMapUv),v.push(M.lightMapUv),v.push(M.aoMapUv),v.push(M.bumpMapUv),v.push(M.normalMapUv),v.push(M.displacementMapUv),v.push(M.emissiveMapUv),v.push(M.metalnessMapUv),v.push(M.roughnessMapUv),v.push(M.anisotropyMapUv),v.push(M.clearcoatMapUv),v.push(M.clearcoatNormalMapUv),v.push(M.clearcoatRoughnessMapUv),v.push(M.iridescenceMapUv),v.push(M.iridescenceThicknessMapUv),v.push(M.sheenColorMapUv),v.push(M.sheenRoughnessMapUv),v.push(M.specularMapUv),v.push(M.specularColorMapUv),v.push(M.specularIntensityMapUv),v.push(M.transmissionMapUv),v.push(M.thicknessMapUv),v.push(M.combine),v.push(M.fogExp2),v.push(M.sizeAttenuation),v.push(M.morphTargetsCount),v.push(M.morphAttributeCount),v.push(M.numDirLights),v.push(M.numPointLights),v.push(M.numSpotLights),v.push(M.numSpotLightMaps),v.push(M.numHemiLights),v.push(M.numRectAreaLights),v.push(M.numDirLightShadows),v.push(M.numPointLightShadows),v.push(M.numSpotLightShadows),v.push(M.numSpotLightShadowsWithMaps),v.push(M.numLightProbes),v.push(M.shadowMapType),v.push(M.toneMapping),v.push(M.numClippingPlanes),v.push(M.numClipIntersection),v.push(M.depthPacking)}function w(v,M){a.disableAll(),M.supportsVertexTextures&&a.enable(0),M.instancing&&a.enable(1),M.instancingColor&&a.enable(2),M.instancingMorph&&a.enable(3),M.matcap&&a.enable(4),M.envMap&&a.enable(5),M.normalMapObjectSpace&&a.enable(6),M.normalMapTangentSpace&&a.enable(7),M.clearcoat&&a.enable(8),M.iridescence&&a.enable(9),M.alphaTest&&a.enable(10),M.vertexColors&&a.enable(11),M.vertexAlphas&&a.enable(12),M.vertexUv1s&&a.enable(13),M.vertexUv2s&&a.enable(14),M.vertexUv3s&&a.enable(15),M.vertexTangents&&a.enable(16),M.anisotropy&&a.enable(17),M.alphaHash&&a.enable(18),M.batching&&a.enable(19),M.dispersion&&a.enable(20),M.batchingColor&&a.enable(21),v.push(a.mask),a.disableAll(),M.fog&&a.enable(0),M.useFog&&a.enable(1),M.flatShading&&a.enable(2),M.logarithmicDepthBuffer&&a.enable(3),M.reverseDepthBuffer&&a.enable(4),M.skinning&&a.enable(5),M.morphTargets&&a.enable(6),M.morphNormals&&a.enable(7),M.morphColors&&a.enable(8),M.premultipliedAlpha&&a.enable(9),M.shadowMapEnabled&&a.enable(10),M.doubleSided&&a.enable(11),M.flipSided&&a.enable(12),M.useDepthPacking&&a.enable(13),M.dithering&&a.enable(14),M.transmission&&a.enable(15),M.sheen&&a.enable(16),M.opaque&&a.enable(17),M.pointsUvs&&a.enable(18),M.decodeVideoTexture&&a.enable(19),M.alphaToCoverage&&a.enable(20),v.push(a.mask)}function L(v){let M=x[v.type],j;if(M){let z=Yn[M];j=mC.clone(z.uniforms)}else j=v.uniforms;return j}function T(v,M){let j;for(let z=0,Y=u.length;z<Y;z++){let J=u[z];if(J.cacheKey===M){j=J,++j.usedTimes;break}}return j===void 0&&(j=new bI(n,M,v,s),u.push(j)),j}function C(v){if(--v.usedTimes===0){let M=u.indexOf(v);u[M]=u[u.length-1],u.pop(),v.destroy()}}function U(v){l.remove(v)}function ie(){l.dispose()}return{getParameters:p,getProgramCacheKey:S,getUniforms:L,acquireProgram:T,releaseProgram:C,releaseShaderCache:U,programs:u,dispose:ie}}function CI(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,l){n.get(o)[a]=l}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function DI(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.z!==e.z?n.z-e.z:n.id-e.id}function i_(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function r_(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(d,f,h,g,x,m){let p=n[e];return p===void 0?(p={id:d.id,object:d,geometry:f,material:h,groupOrder:g,renderOrder:d.renderOrder,z:x,group:m},n[e]=p):(p.id=d.id,p.object=d,p.geometry=f,p.material=h,p.groupOrder=g,p.renderOrder=d.renderOrder,p.z=x,p.group=m),e++,p}function a(d,f,h,g,x,m){let p=o(d,f,h,g,x,m);h.transmission>0?i.push(p):h.transparent===!0?r.push(p):t.push(p)}function l(d,f,h,g,x,m){let p=o(d,f,h,g,x,m);h.transmission>0?i.unshift(p):h.transparent===!0?r.unshift(p):t.unshift(p)}function c(d,f){t.length>1&&t.sort(d||DI),i.length>1&&i.sort(f||i_),r.length>1&&r.sort(f||i_)}function u(){for(let d=e,f=n.length;d<f;d++){let h=n[d];if(h.id===null)break;h.id=null,h.object=null,h.geometry=null,h.material=null,h.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:a,unshift:l,finish:u,sort:c}}function TI(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new r_,n.set(i,[o])):r>=s.length?(o=new r_,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function AI(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new V,color:new qe};break;case"SpotLight":t={position:new V,direction:new V,color:new qe,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new V,color:new qe,distance:0,decay:0};break;case"HemisphereLight":t={direction:new V,skyColor:new qe,groundColor:new qe};break;case"RectAreaLight":t={color:new qe,position:new V,halfWidth:new V,halfHeight:new V};break}return n[e.id]=t,t}}}function II(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new st};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new st};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new st,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var RI=0;function NI(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function PI(n){let e=new AI,t=II(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let c=0;c<9;c++)i.probe.push(new V);let r=new V,s=new Dt,o=new Dt;function a(c){let u=0,d=0,f=0;for(let ie=0;ie<9;ie++)i.probe[ie].set(0,0,0);let h=0,g=0,x=0,m=0,p=0,S=0,E=0,w=0,L=0,T=0,C=0;c.sort(NI);for(let ie=0,v=c.length;ie<v;ie++){let M=c[ie],j=M.color,z=M.intensity,Y=M.distance,J=M.shadow&&M.shadow.map?M.shadow.map.texture:null;if(M.isAmbientLight)u+=j.r*z,d+=j.g*z,f+=j.b*z;else if(M.isLightProbe){for(let W=0;W<9;W++)i.probe[W].addScaledVector(M.sh.coefficients[W],z);C++}else if(M.isDirectionalLight){let W=e.get(M);if(W.color.copy(M.color).multiplyScalar(M.intensity),M.castShadow){let K=M.shadow,G=t.get(M);G.shadowIntensity=K.intensity,G.shadowBias=K.bias,G.shadowNormalBias=K.normalBias,G.shadowRadius=K.radius,G.shadowMapSize=K.mapSize,i.directionalShadow[h]=G,i.directionalShadowMap[h]=J,i.directionalShadowMatrix[h]=M.shadow.matrix,S++}i.directional[h]=W,h++}else if(M.isSpotLight){let W=e.get(M);W.position.setFromMatrixPosition(M.matrixWorld),W.color.copy(j).multiplyScalar(z),W.distance=Y,W.coneCos=Math.cos(M.angle),W.penumbraCos=Math.cos(M.angle*(1-M.penumbra)),W.decay=M.decay,i.spot[x]=W;let K=M.shadow;if(M.map&&(i.spotLightMap[L]=M.map,L++,K.updateMatrices(M),M.castShadow&&T++),i.spotLightMatrix[x]=K.matrix,M.castShadow){let G=t.get(M);G.shadowIntensity=K.intensity,G.shadowBias=K.bias,G.shadowNormalBias=K.normalBias,G.shadowRadius=K.radius,G.shadowMapSize=K.mapSize,i.spotShadow[x]=G,i.spotShadowMap[x]=J,w++}x++}else if(M.isRectAreaLight){let W=e.get(M);W.color.copy(j).multiplyScalar(z),W.halfWidth.set(M.width*.5,0,0),W.halfHeight.set(0,M.height*.5,0),i.rectArea[m]=W,m++}else if(M.isPointLight){let W=e.get(M);if(W.color.copy(M.color).multiplyScalar(M.intensity),W.distance=M.distance,W.decay=M.decay,M.castShadow){let K=M.shadow,G=t.get(M);G.shadowIntensity=K.intensity,G.shadowBias=K.bias,G.shadowNormalBias=K.normalBias,G.shadowRadius=K.radius,G.shadowMapSize=K.mapSize,G.shadowCameraNear=K.camera.near,G.shadowCameraFar=K.camera.far,i.pointShadow[g]=G,i.pointShadowMap[g]=J,i.pointShadowMatrix[g]=M.shadow.matrix,E++}i.point[g]=W,g++}else if(M.isHemisphereLight){let W=e.get(M);W.skyColor.copy(M.color).multiplyScalar(z),W.groundColor.copy(M.groundColor).multiplyScalar(z),i.hemi[p]=W,p++}}m>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=re.LTC_FLOAT_1,i.rectAreaLTC2=re.LTC_FLOAT_2):(i.rectAreaLTC1=re.LTC_HALF_1,i.rectAreaLTC2=re.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=d,i.ambient[2]=f;let U=i.hash;(U.directionalLength!==h||U.pointLength!==g||U.spotLength!==x||U.rectAreaLength!==m||U.hemiLength!==p||U.numDirectionalShadows!==S||U.numPointShadows!==E||U.numSpotShadows!==w||U.numSpotMaps!==L||U.numLightProbes!==C)&&(i.directional.length=h,i.spot.length=x,i.rectArea.length=m,i.point.length=g,i.hemi.length=p,i.directionalShadow.length=S,i.directionalShadowMap.length=S,i.pointShadow.length=E,i.pointShadowMap.length=E,i.spotShadow.length=w,i.spotShadowMap.length=w,i.directionalShadowMatrix.length=S,i.pointShadowMatrix.length=E,i.spotLightMatrix.length=w+L-T,i.spotLightMap.length=L,i.numSpotLightShadowsWithMaps=T,i.numLightProbes=C,U.directionalLength=h,U.pointLength=g,U.spotLength=x,U.rectAreaLength=m,U.hemiLength=p,U.numDirectionalShadows=S,U.numPointShadows=E,U.numSpotShadows=w,U.numSpotMaps=L,U.numLightProbes=C,i.version=RI++)}function l(c,u){let d=0,f=0,h=0,g=0,x=0,m=u.matrixWorldInverse;for(let p=0,S=c.length;p<S;p++){let E=c[p];if(E.isDirectionalLight){let w=i.directional[d];w.direction.setFromMatrixPosition(E.matrixWorld),r.setFromMatrixPosition(E.target.matrixWorld),w.direction.sub(r),w.direction.transformDirection(m),d++}else if(E.isSpotLight){let w=i.spot[h];w.position.setFromMatrixPosition(E.matrixWorld),w.position.applyMatrix4(m),w.direction.setFromMatrixPosition(E.matrixWorld),r.setFromMatrixPosition(E.target.matrixWorld),w.direction.sub(r),w.direction.transformDirection(m),h++}else if(E.isRectAreaLight){let w=i.rectArea[g];w.position.setFromMatrixPosition(E.matrixWorld),w.position.applyMatrix4(m),o.identity(),s.copy(E.matrixWorld),s.premultiply(m),o.extractRotation(s),w.halfWidth.set(E.width*.5,0,0),w.halfHeight.set(0,E.height*.5,0),w.halfWidth.applyMatrix4(o),w.halfHeight.applyMatrix4(o),g++}else if(E.isPointLight){let w=i.point[f];w.position.setFromMatrixPosition(E.matrixWorld),w.position.applyMatrix4(m),f++}else if(E.isHemisphereLight){let w=i.hemi[x];w.direction.setFromMatrixPosition(E.matrixWorld),w.direction.transformDirection(m),x++}}}return{setup:a,setupView:l,state:i}}function s_(n){let e=new PI(n),t=[],i=[];function r(u){c.camera=u,t.length=0,i.length=0}function s(u){t.push(u)}function o(u){i.push(u)}function a(){e.setup(t)}function l(u){e.setupView(t,u)}let c={lightsArray:t,shadowsArray:i,camera:null,lights:e,transmissionRenderTarget:{}};return{init:r,state:c,setupLights:a,setupLightsView:l,pushLight:s,pushShadow:o}}function FI(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new s_(n),e.set(r,[a])):s>=o.length?(a=new s_(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var Dh=class extends wr{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=BS,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},Th=class extends wr{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}},OI=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,LI=`uniform sampler2D shadow_pass;
uniform vec2 resolution;
uniform float radius;
#include <packing>
void main() {
	const float samples = float( VSM_SAMPLES );
	float mean = 0.0;
	float squared_mean = 0.0;
	float uvStride = samples <= 1.0 ? 0.0 : 2.0 / ( samples - 1.0 );
	float uvStart = samples <= 1.0 ? 0.0 : - 1.0;
	for ( float i = 0.0; i < samples; i ++ ) {
		float uvOffset = uvStart + i * uvStride;
		#ifdef HORIZONTAL_PASS
			vec2 distribution = unpackRGBATo2Half( texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( uvOffset, 0.0 ) * radius ) / resolution ) );
			mean += distribution.x;
			squared_mean += distribution.y * distribution.y + distribution.x * distribution.x;
		#else
			float depth = unpackRGBAToDepth( texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( 0.0, uvOffset ) * radius ) / resolution ) );
			mean += depth;
			squared_mean += depth * depth;
		#endif
	}
	mean = mean / samples;
	squared_mean = squared_mean / samples;
	float std_dev = sqrt( squared_mean - mean * mean );
	gl_FragColor = pack2HalfToRGBA( vec2( mean, std_dev ) );
}`;function kI(n,e,t){let i=new Do,r=new st,s=new st,o=new Et,a=new Dh({depthPacking:HS}),l=new Th,c={},u=t.maxTextureSize,d={[zi]:sn,[sn]:zi,[fi]:fi},f=new Zn({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new st},radius:{value:4}},vertexShader:OI,fragmentShader:LI}),h=f.clone();h.defines.HORIZONTAL_PASS=1;let g=new Sr;g.setAttribute("position",new Mn(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let x=new rn(g,f),m=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=c_;let p=this.type;this.render=function(T,C,U){if(m.enabled===!1||m.autoUpdate===!1&&m.needsUpdate===!1||T.length===0)return;let ie=n.getRenderTarget(),v=n.getActiveCubeFace(),M=n.getActiveMipmapLevel(),j=n.state;j.setBlending(Bi),j.buffers.color.setClear(1,1,1,1),j.buffers.depth.setTest(!0),j.setScissorTest(!1);let z=p!==ui&&this.type===ui,Y=p===ui&&this.type!==ui;for(let J=0,W=T.length;J<W;J++){let K=T[J],G=K.shadow;if(G===void 0){console.warn("THREE.WebGLShadowMap:",K,"has no shadow.");continue}if(G.autoUpdate===!1&&G.needsUpdate===!1)continue;r.copy(G.mapSize);let ue=G.getFrameExtents();if(r.multiply(ue),s.copy(G.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/ue.x),r.x=s.x*ue.x,G.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/ue.y),r.y=s.y*ue.y,G.mapSize.y=s.y)),G.map===null||z===!0||Y===!0){let _e=this.type!==ui?{minFilter:xn,magFilter:xn}:{};G.map!==null&&G.map.dispose(),G.map=new yi(r.x,r.y,_e),G.map.texture.name=K.name+".shadowMap",G.camera.updateProjectionMatrix()}n.setRenderTarget(G.map),n.clear();let de=G.getViewportCount();for(let _e=0;_e<de;_e++){let Je=G.getViewport(_e);o.set(s.x*Je.x,s.y*Je.y,s.x*Je.z,s.y*Je.w),j.viewport(o),G.updateMatrices(K,_e),i=G.getFrustum(),w(C,U,G.camera,K,this.type)}G.isPointLightShadow!==!0&&this.type===ui&&S(G,U),G.needsUpdate=!1}p=this.type,m.needsUpdate=!1,n.setRenderTarget(ie,v,M)};function S(T,C){let U=e.update(x);f.defines.VSM_SAMPLES!==T.blurSamples&&(f.defines.VSM_SAMPLES=T.blurSamples,h.defines.VSM_SAMPLES=T.blurSamples,f.needsUpdate=!0,h.needsUpdate=!0),T.mapPass===null&&(T.mapPass=new yi(r.x,r.y)),f.uniforms.shadow_pass.value=T.map.texture,f.uniforms.resolution.value=T.mapSize,f.uniforms.radius.value=T.radius,n.setRenderTarget(T.mapPass),n.clear(),n.renderBufferDirect(C,null,U,f,x,null),h.uniforms.shadow_pass.value=T.mapPass.texture,h.uniforms.resolution.value=T.mapSize,h.uniforms.radius.value=T.radius,n.setRenderTarget(T.map),n.clear(),n.renderBufferDirect(C,null,U,h,x,null)}function E(T,C,U,ie){let v=null,M=U.isPointLight===!0?T.customDistanceMaterial:T.customDepthMaterial;if(M!==void 0)v=M;else if(v=U.isPointLight===!0?l:a,n.localClippingEnabled&&C.clipShadows===!0&&Array.isArray(C.clippingPlanes)&&C.clippingPlanes.length!==0||C.displacementMap&&C.displacementScale!==0||C.alphaMap&&C.alphaTest>0||C.map&&C.alphaTest>0){let j=v.uuid,z=C.uuid,Y=c[j];Y===void 0&&(Y={},c[j]=Y);let J=Y[z];J===void 0&&(J=v.clone(),Y[z]=J,C.addEventListener("dispose",L)),v=J}if(v.visible=C.visible,v.wireframe=C.wireframe,ie===ui?v.side=C.shadowSide!==null?C.shadowSide:C.side:v.side=C.shadowSide!==null?C.shadowSide:d[C.side],v.alphaMap=C.alphaMap,v.alphaTest=C.alphaTest,v.map=C.map,v.clipShadows=C.clipShadows,v.clippingPlanes=C.clippingPlanes,v.clipIntersection=C.clipIntersection,v.displacementMap=C.displacementMap,v.displacementScale=C.displacementScale,v.displacementBias=C.displacementBias,v.wireframeLinewidth=C.wireframeLinewidth,v.linewidth=C.linewidth,U.isPointLight===!0&&v.isMeshDistanceMaterial===!0){let j=n.properties.get(v);j.light=U}return v}function w(T,C,U,ie,v){if(T.visible===!1)return;if(T.layers.test(C.layers)&&(T.isMesh||T.isLine||T.isPoints)&&(T.castShadow||T.receiveShadow&&v===ui)&&(!T.frustumCulled||i.intersectsObject(T))){T.modelViewMatrix.multiplyMatrices(U.matrixWorldInverse,T.matrixWorld);let z=e.update(T),Y=T.material;if(Array.isArray(Y)){let J=z.groups;for(let W=0,K=J.length;W<K;W++){let G=J[W],ue=Y[G.materialIndex];if(ue&&ue.visible){let de=E(T,ue,ie,v);T.onBeforeShadow(n,T,C,U,z,de,G),n.renderBufferDirect(U,null,z,de,T,G),T.onAfterShadow(n,T,C,U,z,de,G)}}}else if(Y.visible){let J=E(T,Y,ie,v);T.onBeforeShadow(n,T,C,U,z,J,null),n.renderBufferDirect(U,null,z,J,T,null),T.onAfterShadow(n,T,C,U,z,J,null)}}let j=T.children;for(let z=0,Y=j.length;z<Y;z++)w(j[z],C,U,ie,v)}function L(T){T.target.removeEventListener("dispose",L);for(let U in c){let ie=c[U],v=T.target.uuid;v in ie&&(ie[v].dispose(),delete ie[v])}}}var UI={[If]:Rf,[Nf]:Of,[Pf]:Lf,[Is]:Ff,[Rf]:If,[Of]:Nf,[Lf]:Pf,[Ff]:Is};function VI(n){function e(){let A=!1,le=new Et,H=null,Z=new Et(0,0,0,0);return{setMask:function(oe){H!==oe&&!A&&(n.colorMask(oe,oe,oe,oe),H=oe)},setLocked:function(oe){A=oe},setClear:function(oe,ce,$e,Tt,Zt){Zt===!0&&(oe*=Tt,ce*=Tt,$e*=Tt),le.set(oe,ce,$e,Tt),Z.equals(le)===!1&&(n.clearColor(oe,ce,$e,Tt),Z.copy(le))},reset:function(){A=!1,H=null,Z.set(-1,0,0,0)}}}function t(){let A=!1,le=!1,H=null,Z=null,oe=null;return{setReversed:function(ce){le=ce},setTest:function(ce){ce?ve(n.DEPTH_TEST):fe(n.DEPTH_TEST)},setMask:function(ce){H!==ce&&!A&&(n.depthMask(ce),H=ce)},setFunc:function(ce){if(le&&(ce=UI[ce]),Z!==ce){switch(ce){case If:n.depthFunc(n.NEVER);break;case Rf:n.depthFunc(n.ALWAYS);break;case Nf:n.depthFunc(n.LESS);break;case Is:n.depthFunc(n.LEQUAL);break;case Pf:n.depthFunc(n.EQUAL);break;case Ff:n.depthFunc(n.GEQUAL);break;case Of:n.depthFunc(n.GREATER);break;case Lf:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}Z=ce}},setLocked:function(ce){A=ce},setClear:function(ce){oe!==ce&&(n.clearDepth(ce),oe=ce)},reset:function(){A=!1,H=null,Z=null,oe=null}}}function i(){let A=!1,le=null,H=null,Z=null,oe=null,ce=null,$e=null,Tt=null,Zt=null;return{setTest:function(Qe){A||(Qe?ve(n.STENCIL_TEST):fe(n.STENCIL_TEST))},setMask:function(Qe){le!==Qe&&!A&&(n.stencilMask(Qe),le=Qe)},setFunc:function(Qe,Kt,Kn){(H!==Qe||Z!==Kt||oe!==Kn)&&(n.stencilFunc(Qe,Kt,Kn),H=Qe,Z=Kt,oe=Kn)},setOp:function(Qe,Kt,Kn){(ce!==Qe||$e!==Kt||Tt!==Kn)&&(n.stencilOp(Qe,Kt,Kn),ce=Qe,$e=Kt,Tt=Kn)},setLocked:function(Qe){A=Qe},setClear:function(Qe){Zt!==Qe&&(n.clearStencil(Qe),Zt=Qe)},reset:function(){A=!1,le=null,H=null,Z=null,oe=null,ce=null,$e=null,Tt=null,Zt=null}}}let r=new e,s=new t,o=new i,a=new WeakMap,l=new WeakMap,c={},u={},d=new WeakMap,f=[],h=null,g=!1,x=null,m=null,p=null,S=null,E=null,w=null,L=null,T=new qe(0,0,0),C=0,U=!1,ie=null,v=null,M=null,j=null,z=null,Y=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),J=!1,W=0,K=n.getParameter(n.VERSION);K.indexOf("WebGL")!==-1?(W=parseFloat(/^WebGL (\d)/.exec(K)[1]),J=W>=1):K.indexOf("OpenGL ES")!==-1&&(W=parseFloat(/^OpenGL ES (\d)/.exec(K)[1]),J=W>=2);let G=null,ue={},de=n.getParameter(n.SCISSOR_BOX),_e=n.getParameter(n.VIEWPORT),Je=new Et().fromArray(de),ot=new Et().fromArray(_e);function $(A,le,H,Z){let oe=new Uint8Array(4),ce=n.createTexture();n.bindTexture(A,ce),n.texParameteri(A,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(A,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let $e=0;$e<H;$e++)A===n.TEXTURE_3D||A===n.TEXTURE_2D_ARRAY?n.texImage3D(le,0,n.RGBA,1,1,Z,0,n.RGBA,n.UNSIGNED_BYTE,oe):n.texImage2D(le+$e,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,oe);return ce}let ee={};ee[n.TEXTURE_2D]=$(n.TEXTURE_2D,n.TEXTURE_2D,1),ee[n.TEXTURE_CUBE_MAP]=$(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),ee[n.TEXTURE_2D_ARRAY]=$(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),ee[n.TEXTURE_3D]=$(n.TEXTURE_3D,n.TEXTURE_3D,1,1),r.setClear(0,0,0,1),s.setClear(1),o.setClear(0),ve(n.DEPTH_TEST),s.setFunc(Is),He(!1),Xe(uy),ve(n.CULL_FACE),D(Bi);function ve(A){c[A]!==!0&&(n.enable(A),c[A]=!0)}function fe(A){c[A]!==!1&&(n.disable(A),c[A]=!1)}function Pe(A,le){return u[A]!==le?(n.bindFramebuffer(A,le),u[A]=le,A===n.DRAW_FRAMEBUFFER&&(u[n.FRAMEBUFFER]=le),A===n.FRAMEBUFFER&&(u[n.DRAW_FRAMEBUFFER]=le),!0):!1}function Se(A,le){let H=f,Z=!1;if(A){H=d.get(le),H===void 0&&(H=[],d.set(le,H));let oe=A.textures;if(H.length!==oe.length||H[0]!==n.COLOR_ATTACHMENT0){for(let ce=0,$e=oe.length;ce<$e;ce++)H[ce]=n.COLOR_ATTACHMENT0+ce;H.length=oe.length,Z=!0}}else H[0]!==n.BACK&&(H[0]=n.BACK,Z=!0);Z&&n.drawBuffers(H)}function We(A){return h!==A?(n.useProgram(A),h=A,!0):!1}let lt={[gr]:n.FUNC_ADD,[hS]:n.FUNC_SUBTRACT,[pS]:n.FUNC_REVERSE_SUBTRACT};lt[mS]=n.MIN,lt[gS]=n.MAX;let je={[vS]:n.ZERO,[yS]:n.ONE,[_S]:n.SRC_COLOR,[Tf]:n.SRC_ALPHA,[SS]:n.SRC_ALPHA_SATURATE,[bS]:n.DST_COLOR,[MS]:n.DST_ALPHA,[xS]:n.ONE_MINUS_SRC_COLOR,[Af]:n.ONE_MINUS_SRC_ALPHA,[wS]:n.ONE_MINUS_DST_COLOR,[ES]:n.ONE_MINUS_DST_ALPHA,[CS]:n.CONSTANT_COLOR,[DS]:n.ONE_MINUS_CONSTANT_COLOR,[TS]:n.CONSTANT_ALPHA,[AS]:n.ONE_MINUS_CONSTANT_ALPHA};function D(A,le,H,Z,oe,ce,$e,Tt,Zt,Qe){if(A===Bi){g===!0&&(fe(n.BLEND),g=!1);return}if(g===!1&&(ve(n.BLEND),g=!0),A!==fS){if(A!==x||Qe!==U){if((m!==gr||E!==gr)&&(n.blendEquation(n.FUNC_ADD),m=gr,E=gr),Qe)switch(A){case Cs:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case dy:n.blendFunc(n.ONE,n.ONE);break;case fy:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case hy:n.blendFuncSeparate(n.ZERO,n.SRC_COLOR,n.ZERO,n.SRC_ALPHA);break;default:console.error("THREE.WebGLState: Invalid blending: ",A);break}else switch(A){case Cs:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case dy:n.blendFunc(n.SRC_ALPHA,n.ONE);break;case fy:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case hy:n.blendFunc(n.ZERO,n.SRC_COLOR);break;default:console.error("THREE.WebGLState: Invalid blending: ",A);break}p=null,S=null,w=null,L=null,T.set(0,0,0),C=0,x=A,U=Qe}return}oe=oe||le,ce=ce||H,$e=$e||Z,(le!==m||oe!==E)&&(n.blendEquationSeparate(lt[le],lt[oe]),m=le,E=oe),(H!==p||Z!==S||ce!==w||$e!==L)&&(n.blendFuncSeparate(je[H],je[Z],je[ce],je[$e]),p=H,S=Z,w=ce,L=$e),(Tt.equals(T)===!1||Zt!==C)&&(n.blendColor(Tt.r,Tt.g,Tt.b,Zt),T.copy(Tt),C=Zt),x=A,U=!1}function on(A,le){A.side===fi?fe(n.CULL_FACE):ve(n.CULL_FACE);let H=A.side===sn;le&&(H=!H),He(H),A.blending===Cs&&A.transparent===!1?D(Bi):D(A.blending,A.blendEquation,A.blendSrc,A.blendDst,A.blendEquationAlpha,A.blendSrcAlpha,A.blendDstAlpha,A.blendColor,A.blendAlpha,A.premultipliedAlpha),s.setFunc(A.depthFunc),s.setTest(A.depthTest),s.setMask(A.depthWrite),r.setMask(A.colorWrite);let Z=A.stencilWrite;o.setTest(Z),Z&&(o.setMask(A.stencilWriteMask),o.setFunc(A.stencilFunc,A.stencilRef,A.stencilFuncMask),o.setOp(A.stencilFail,A.stencilZFail,A.stencilZPass)),dt(A.polygonOffset,A.polygonOffsetFactor,A.polygonOffsetUnits),A.alphaToCoverage===!0?ve(n.SAMPLE_ALPHA_TO_COVERAGE):fe(n.SAMPLE_ALPHA_TO_COVERAGE)}function He(A){ie!==A&&(A?n.frontFace(n.CW):n.frontFace(n.CCW),ie=A)}function Xe(A){A!==cS?(ve(n.CULL_FACE),A!==v&&(A===uy?n.cullFace(n.BACK):A===uS?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):fe(n.CULL_FACE),v=A}function Te(A){A!==M&&(J&&n.lineWidth(A),M=A)}function dt(A,le,H){A?(ve(n.POLYGON_OFFSET_FILL),(j!==le||z!==H)&&(n.polygonOffset(le,H),j=le,z=H)):fe(n.POLYGON_OFFSET_FILL)}function Re(A){A?ve(n.SCISSOR_TEST):fe(n.SCISSOR_TEST)}function b(A){A===void 0&&(A=n.TEXTURE0+Y-1),G!==A&&(n.activeTexture(A),G=A)}function y(A,le,H){H===void 0&&(G===null?H=n.TEXTURE0+Y-1:H=G);let Z=ue[H];Z===void 0&&(Z={type:void 0,texture:void 0},ue[H]=Z),(Z.type!==A||Z.texture!==le)&&(G!==H&&(n.activeTexture(H),G=H),n.bindTexture(A,le||ee[A]),Z.type=A,Z.texture=le)}function F(){let A=ue[G];A!==void 0&&A.type!==void 0&&(n.bindTexture(A.type,null),A.type=void 0,A.texture=void 0)}function X(){try{n.compressedTexImage2D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function Q(){try{n.compressedTexImage3D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function q(){try{n.texSubImage2D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function Me(){try{n.texSubImage3D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function se(){try{n.compressedTexSubImage2D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function he(){try{n.compressedTexSubImage3D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function Ye(){try{n.texStorage2D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function te(){try{n.texStorage3D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function pe(){try{n.texImage2D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function Ae(){try{n.texImage3D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function Ie(A){Je.equals(A)===!1&&(n.scissor(A.x,A.y,A.z,A.w),Je.copy(A))}function me(A){ot.equals(A)===!1&&(n.viewport(A.x,A.y,A.z,A.w),ot.copy(A))}function ze(A,le){let H=l.get(le);H===void 0&&(H=new WeakMap,l.set(le,H));let Z=H.get(A);Z===void 0&&(Z=n.getUniformBlockIndex(le,A.name),H.set(A,Z))}function Fe(A,le){let Z=l.get(le).get(A);a.get(le)!==Z&&(n.uniformBlockBinding(le,Z,A.__bindingPointIndex),a.set(le,Z))}function ct(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),c={},G=null,ue={},u={},d=new WeakMap,f=[],h=null,g=!1,x=null,m=null,p=null,S=null,E=null,w=null,L=null,T=new qe(0,0,0),C=0,U=!1,ie=null,v=null,M=null,j=null,z=null,Je.set(0,0,n.canvas.width,n.canvas.height),ot.set(0,0,n.canvas.width,n.canvas.height),r.reset(),s.reset(),o.reset()}return{buffers:{color:r,depth:s,stencil:o},enable:ve,disable:fe,bindFramebuffer:Pe,drawBuffers:Se,useProgram:We,setBlending:D,setMaterial:on,setFlipSided:He,setCullFace:Xe,setLineWidth:Te,setPolygonOffset:dt,setScissorTest:Re,activeTexture:b,bindTexture:y,unbindTexture:F,compressedTexImage2D:X,compressedTexImage3D:Q,texImage2D:pe,texImage3D:Ae,updateUBOMapping:ze,uniformBlockBinding:Fe,texStorage2D:Ye,texStorage3D:te,texSubImage2D:q,texSubImage3D:Me,compressedTexSubImage2D:se,compressedTexSubImage3D:he,scissor:Ie,viewport:me,reset:ct}}function o_(n,e,t,i){let r=BI(i);switch(t){case p_:return n*e;case g_:return n*e;case v_:return n*e*2;case y_:return n*e/r.components*r.byteLength;case $h:return n*e/r.components*r.byteLength;case __:return n*e*2/r.components*r.byteLength;case qh:return n*e*2/r.components*r.byteLength;case m_:return n*e*3/r.components*r.byteLength;case Fn:return n*e*4/r.components*r.byteLength;case Xh:return n*e*4/r.components*r.byteLength;case Fl:case Ol:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Ll:case kl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case zf:case Wf:return Math.max(n,16)*Math.max(e,8)/4;case Hf:case Gf:return Math.max(n,8)*Math.max(e,8)/2;case jf:case $f:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case qf:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Xf:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Yf:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case Zf:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case Kf:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case Jf:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case Qf:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case eh:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case th:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case nh:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case ih:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case rh:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case sh:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case oh:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case ah:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case Ul:case lh:case ch:return Math.ceil(n/4)*Math.ceil(e/4)*16;case x_:case uh:return Math.ceil(n/4)*Math.ceil(e/4)*8;case dh:case fh:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function BI(n){switch(n){case vi:case d_:return{byteLength:1,components:1};case So:case f_:case Ao:return{byteLength:2,components:1};case Wh:case jh:return{byteLength:2,components:4};case Mr:case Gh:case hi:return{byteLength:4,components:1};case h_:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}function HI(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,l=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),c=new st,u=new WeakMap,d,f=new WeakMap,h=!1;try{h=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function g(b,y){return h?new OffscreenCanvas(b,y):jl("canvas")}function x(b,y,F){let X=1,Q=Re(b);if((Q.width>F||Q.height>F)&&(X=F/Math.max(Q.width,Q.height)),X<1)if(typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&b instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&b instanceof ImageBitmap||typeof VideoFrame<"u"&&b instanceof VideoFrame){let q=Math.floor(X*Q.width),Me=Math.floor(X*Q.height);d===void 0&&(d=g(q,Me));let se=y?g(q,Me):d;return se.width=q,se.height=Me,se.getContext("2d").drawImage(b,0,0,q,Me),console.warn("THREE.WebGLRenderer: Texture has been resized from ("+Q.width+"x"+Q.height+") to ("+q+"x"+Me+")."),se}else return"data"in b&&console.warn("THREE.WebGLRenderer: Image in DataTexture is too big ("+Q.width+"x"+Q.height+")."),b;return b}function m(b){return b.generateMipmaps&&b.minFilter!==xn&&b.minFilter!==Pn}function p(b){n.generateMipmap(b)}function S(b,y,F,X,Q=!1){if(b!==null){if(n[b]!==void 0)return n[b];console.warn("THREE.WebGLRenderer: Attempt to use non-existing WebGL internal format '"+b+"'")}let q=y;if(y===n.RED&&(F===n.FLOAT&&(q=n.R32F),F===n.HALF_FLOAT&&(q=n.R16F),F===n.UNSIGNED_BYTE&&(q=n.R8)),y===n.RED_INTEGER&&(F===n.UNSIGNED_BYTE&&(q=n.R8UI),F===n.UNSIGNED_SHORT&&(q=n.R16UI),F===n.UNSIGNED_INT&&(q=n.R32UI),F===n.BYTE&&(q=n.R8I),F===n.SHORT&&(q=n.R16I),F===n.INT&&(q=n.R32I)),y===n.RG&&(F===n.FLOAT&&(q=n.RG32F),F===n.HALF_FLOAT&&(q=n.RG16F),F===n.UNSIGNED_BYTE&&(q=n.RG8)),y===n.RG_INTEGER&&(F===n.UNSIGNED_BYTE&&(q=n.RG8UI),F===n.UNSIGNED_SHORT&&(q=n.RG16UI),F===n.UNSIGNED_INT&&(q=n.RG32UI),F===n.BYTE&&(q=n.RG8I),F===n.SHORT&&(q=n.RG16I),F===n.INT&&(q=n.RG32I)),y===n.RGB_INTEGER&&(F===n.UNSIGNED_BYTE&&(q=n.RGB8UI),F===n.UNSIGNED_SHORT&&(q=n.RGB16UI),F===n.UNSIGNED_INT&&(q=n.RGB32UI),F===n.BYTE&&(q=n.RGB8I),F===n.SHORT&&(q=n.RGB16I),F===n.INT&&(q=n.RGB32I)),y===n.RGBA_INTEGER&&(F===n.UNSIGNED_BYTE&&(q=n.RGBA8UI),F===n.UNSIGNED_SHORT&&(q=n.RGBA16UI),F===n.UNSIGNED_INT&&(q=n.RGBA32UI),F===n.BYTE&&(q=n.RGBA8I),F===n.SHORT&&(q=n.RGBA16I),F===n.INT&&(q=n.RGBA32I)),y===n.RGB&&F===n.UNSIGNED_INT_5_9_9_9_REV&&(q=n.RGB9_E5),y===n.RGBA){let Me=Q?Hl:rt.getTransfer(X);F===n.FLOAT&&(q=n.RGBA32F),F===n.HALF_FLOAT&&(q=n.RGBA16F),F===n.UNSIGNED_BYTE&&(q=Me===mt?n.SRGB8_ALPHA8:n.RGBA8),F===n.UNSIGNED_SHORT_4_4_4_4&&(q=n.RGBA4),F===n.UNSIGNED_SHORT_5_5_5_1&&(q=n.RGB5_A1)}return(q===n.R16F||q===n.R32F||q===n.RG16F||q===n.RG32F||q===n.RGBA16F||q===n.RGBA32F)&&e.get("EXT_color_buffer_float"),q}function E(b,y){let F;return b?y===null||y===Mr||y===Ps?F=n.DEPTH24_STENCIL8:y===hi?F=n.DEPTH32F_STENCIL8:y===So&&(F=n.DEPTH24_STENCIL8,console.warn("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):y===null||y===Mr||y===Ps?F=n.DEPTH_COMPONENT24:y===hi?F=n.DEPTH_COMPONENT32F:y===So&&(F=n.DEPTH_COMPONENT16),F}function w(b,y){return m(b)===!0||b.isFramebufferTexture&&b.minFilter!==xn&&b.minFilter!==Pn?Math.log2(Math.max(y.width,y.height))+1:b.mipmaps!==void 0&&b.mipmaps.length>0?b.mipmaps.length:b.isCompressedTexture&&Array.isArray(b.image)?y.mipmaps.length:1}function L(b){let y=b.target;y.removeEventListener("dispose",L),C(y),y.isVideoTexture&&u.delete(y)}function T(b){let y=b.target;y.removeEventListener("dispose",T),ie(y)}function C(b){let y=i.get(b);if(y.__webglInit===void 0)return;let F=b.source,X=f.get(F);if(X){let Q=X[y.__cacheKey];Q.usedTimes--,Q.usedTimes===0&&U(b),Object.keys(X).length===0&&f.delete(F)}i.remove(b)}function U(b){let y=i.get(b);n.deleteTexture(y.__webglTexture);let F=b.source,X=f.get(F);delete X[y.__cacheKey],o.memory.textures--}function ie(b){let y=i.get(b);if(b.depthTexture&&b.depthTexture.dispose(),b.isWebGLCubeRenderTarget)for(let X=0;X<6;X++){if(Array.isArray(y.__webglFramebuffer[X]))for(let Q=0;Q<y.__webglFramebuffer[X].length;Q++)n.deleteFramebuffer(y.__webglFramebuffer[X][Q]);else n.deleteFramebuffer(y.__webglFramebuffer[X]);y.__webglDepthbuffer&&n.deleteRenderbuffer(y.__webglDepthbuffer[X])}else{if(Array.isArray(y.__webglFramebuffer))for(let X=0;X<y.__webglFramebuffer.length;X++)n.deleteFramebuffer(y.__webglFramebuffer[X]);else n.deleteFramebuffer(y.__webglFramebuffer);if(y.__webglDepthbuffer&&n.deleteRenderbuffer(y.__webglDepthbuffer),y.__webglMultisampledFramebuffer&&n.deleteFramebuffer(y.__webglMultisampledFramebuffer),y.__webglColorRenderbuffer)for(let X=0;X<y.__webglColorRenderbuffer.length;X++)y.__webglColorRenderbuffer[X]&&n.deleteRenderbuffer(y.__webglColorRenderbuffer[X]);y.__webglDepthRenderbuffer&&n.deleteRenderbuffer(y.__webglDepthRenderbuffer)}let F=b.textures;for(let X=0,Q=F.length;X<Q;X++){let q=i.get(F[X]);q.__webglTexture&&(n.deleteTexture(q.__webglTexture),o.memory.textures--),i.remove(F[X])}i.remove(b)}let v=0;function M(){v=0}function j(){let b=v;return b>=r.maxTextures&&console.warn("THREE.WebGLTextures: Trying to use "+b+" texture units while this GPU supports only "+r.maxTextures),v+=1,b}function z(b){let y=[];return y.push(b.wrapS),y.push(b.wrapT),y.push(b.wrapR||0),y.push(b.magFilter),y.push(b.minFilter),y.push(b.anisotropy),y.push(b.internalFormat),y.push(b.format),y.push(b.type),y.push(b.generateMipmaps),y.push(b.premultiplyAlpha),y.push(b.flipY),y.push(b.unpackAlignment),y.push(b.colorSpace),y.join()}function Y(b,y){let F=i.get(b);if(b.isVideoTexture&&Te(b),b.isRenderTargetTexture===!1&&b.version>0&&F.__version!==b.version){let X=b.image;if(X===null)console.warn("THREE.WebGLRenderer: Texture marked for update but no image data found.");else if(X.complete===!1)console.warn("THREE.WebGLRenderer: Texture marked for update but image is incomplete");else{ot(F,b,y);return}}t.bindTexture(n.TEXTURE_2D,F.__webglTexture,n.TEXTURE0+y)}function J(b,y){let F=i.get(b);if(b.version>0&&F.__version!==b.version){ot(F,b,y);return}t.bindTexture(n.TEXTURE_2D_ARRAY,F.__webglTexture,n.TEXTURE0+y)}function W(b,y){let F=i.get(b);if(b.version>0&&F.__version!==b.version){ot(F,b,y);return}t.bindTexture(n.TEXTURE_3D,F.__webglTexture,n.TEXTURE0+y)}function K(b,y){let F=i.get(b);if(b.version>0&&F.__version!==b.version){$(F,b,y);return}t.bindTexture(n.TEXTURE_CUBE_MAP,F.__webglTexture,n.TEXTURE0+y)}let G={[Vf]:n.REPEAT,[_r]:n.CLAMP_TO_EDGE,[Bf]:n.MIRRORED_REPEAT},ue={[xn]:n.NEAREST,[VS]:n.NEAREST_MIPMAP_NEAREST,[fl]:n.NEAREST_MIPMAP_LINEAR,[Pn]:n.LINEAR,[Yd]:n.LINEAR_MIPMAP_NEAREST,[xr]:n.LINEAR_MIPMAP_LINEAR},de={[GS]:n.NEVER,[YS]:n.ALWAYS,[WS]:n.LESS,[E_]:n.LEQUAL,[jS]:n.EQUAL,[XS]:n.GEQUAL,[$S]:n.GREATER,[qS]:n.NOTEQUAL};function _e(b,y){if(y.type===hi&&e.has("OES_texture_float_linear")===!1&&(y.magFilter===Pn||y.magFilter===Yd||y.magFilter===fl||y.magFilter===xr||y.minFilter===Pn||y.minFilter===Yd||y.minFilter===fl||y.minFilter===xr)&&console.warn("THREE.WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(b,n.TEXTURE_WRAP_S,G[y.wrapS]),n.texParameteri(b,n.TEXTURE_WRAP_T,G[y.wrapT]),(b===n.TEXTURE_3D||b===n.TEXTURE_2D_ARRAY)&&n.texParameteri(b,n.TEXTURE_WRAP_R,G[y.wrapR]),n.texParameteri(b,n.TEXTURE_MAG_FILTER,ue[y.magFilter]),n.texParameteri(b,n.TEXTURE_MIN_FILTER,ue[y.minFilter]),y.compareFunction&&(n.texParameteri(b,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(b,n.TEXTURE_COMPARE_FUNC,de[y.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(y.magFilter===xn||y.minFilter!==fl&&y.minFilter!==xr||y.type===hi&&e.has("OES_texture_float_linear")===!1)return;if(y.anisotropy>1||i.get(y).__currentAnisotropy){let F=e.get("EXT_texture_filter_anisotropic");n.texParameterf(b,F.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(y.anisotropy,r.getMaxAnisotropy())),i.get(y).__currentAnisotropy=y.anisotropy}}}function Je(b,y){let F=!1;b.__webglInit===void 0&&(b.__webglInit=!0,y.addEventListener("dispose",L));let X=y.source,Q=f.get(X);Q===void 0&&(Q={},f.set(X,Q));let q=z(y);if(q!==b.__cacheKey){Q[q]===void 0&&(Q[q]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,F=!0),Q[q].usedTimes++;let Me=Q[b.__cacheKey];Me!==void 0&&(Q[b.__cacheKey].usedTimes--,Me.usedTimes===0&&U(y)),b.__cacheKey=q,b.__webglTexture=Q[q].texture}return F}function ot(b,y,F){let X=n.TEXTURE_2D;(y.isDataArrayTexture||y.isCompressedArrayTexture)&&(X=n.TEXTURE_2D_ARRAY),y.isData3DTexture&&(X=n.TEXTURE_3D);let Q=Je(b,y),q=y.source;t.bindTexture(X,b.__webglTexture,n.TEXTURE0+F);let Me=i.get(q);if(q.version!==Me.__version||Q===!0){t.activeTexture(n.TEXTURE0+F);let se=rt.getPrimaries(rt.workingColorSpace),he=y.colorSpace===Vi?null:rt.getPrimaries(y.colorSpace),Ye=y.colorSpace===Vi||se===he?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,y.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,y.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,y.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,Ye);let te=x(y.image,!1,r.maxTextureSize);te=dt(y,te);let pe=s.convert(y.format,y.colorSpace),Ae=s.convert(y.type),Ie=S(y.internalFormat,pe,Ae,y.colorSpace,y.isVideoTexture);_e(X,y);let me,ze=y.mipmaps,Fe=y.isVideoTexture!==!0,ct=Me.__version===void 0||Q===!0,A=q.dataReady,le=w(y,te);if(y.isDepthTexture)Ie=E(y.format===Fs,y.type),ct&&(Fe?t.texStorage2D(n.TEXTURE_2D,1,Ie,te.width,te.height):t.texImage2D(n.TEXTURE_2D,0,Ie,te.width,te.height,0,pe,Ae,null));else if(y.isDataTexture)if(ze.length>0){Fe&&ct&&t.texStorage2D(n.TEXTURE_2D,le,Ie,ze[0].width,ze[0].height);for(let H=0,Z=ze.length;H<Z;H++)me=ze[H],Fe?A&&t.texSubImage2D(n.TEXTURE_2D,H,0,0,me.width,me.height,pe,Ae,me.data):t.texImage2D(n.TEXTURE_2D,H,Ie,me.width,me.height,0,pe,Ae,me.data);y.generateMipmaps=!1}else Fe?(ct&&t.texStorage2D(n.TEXTURE_2D,le,Ie,te.width,te.height),A&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,te.width,te.height,pe,Ae,te.data)):t.texImage2D(n.TEXTURE_2D,0,Ie,te.width,te.height,0,pe,Ae,te.data);else if(y.isCompressedTexture)if(y.isCompressedArrayTexture){Fe&&ct&&t.texStorage3D(n.TEXTURE_2D_ARRAY,le,Ie,ze[0].width,ze[0].height,te.depth);for(let H=0,Z=ze.length;H<Z;H++)if(me=ze[H],y.format!==Fn)if(pe!==null)if(Fe){if(A)if(y.layerUpdates.size>0){let oe=o_(me.width,me.height,y.format,y.type);for(let ce of y.layerUpdates){let $e=me.data.subarray(ce*oe/me.data.BYTES_PER_ELEMENT,(ce+1)*oe/me.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,H,0,0,ce,me.width,me.height,1,pe,$e,0,0)}y.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,H,0,0,0,me.width,me.height,te.depth,pe,me.data,0,0)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,H,Ie,me.width,me.height,te.depth,0,me.data,0,0);else console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else Fe?A&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,H,0,0,0,me.width,me.height,te.depth,pe,Ae,me.data):t.texImage3D(n.TEXTURE_2D_ARRAY,H,Ie,me.width,me.height,te.depth,0,pe,Ae,me.data)}else{Fe&&ct&&t.texStorage2D(n.TEXTURE_2D,le,Ie,ze[0].width,ze[0].height);for(let H=0,Z=ze.length;H<Z;H++)me=ze[H],y.format!==Fn?pe!==null?Fe?A&&t.compressedTexSubImage2D(n.TEXTURE_2D,H,0,0,me.width,me.height,pe,me.data):t.compressedTexImage2D(n.TEXTURE_2D,H,Ie,me.width,me.height,0,me.data):console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):Fe?A&&t.texSubImage2D(n.TEXTURE_2D,H,0,0,me.width,me.height,pe,Ae,me.data):t.texImage2D(n.TEXTURE_2D,H,Ie,me.width,me.height,0,pe,Ae,me.data)}else if(y.isDataArrayTexture)if(Fe){if(ct&&t.texStorage3D(n.TEXTURE_2D_ARRAY,le,Ie,te.width,te.height,te.depth),A)if(y.layerUpdates.size>0){let H=o_(te.width,te.height,y.format,y.type);for(let Z of y.layerUpdates){let oe=te.data.subarray(Z*H/te.data.BYTES_PER_ELEMENT,(Z+1)*H/te.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,Z,te.width,te.height,1,pe,Ae,oe)}y.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,te.width,te.height,te.depth,pe,Ae,te.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,Ie,te.width,te.height,te.depth,0,pe,Ae,te.data);else if(y.isData3DTexture)Fe?(ct&&t.texStorage3D(n.TEXTURE_3D,le,Ie,te.width,te.height,te.depth),A&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,te.width,te.height,te.depth,pe,Ae,te.data)):t.texImage3D(n.TEXTURE_3D,0,Ie,te.width,te.height,te.depth,0,pe,Ae,te.data);else if(y.isFramebufferTexture){if(ct)if(Fe)t.texStorage2D(n.TEXTURE_2D,le,Ie,te.width,te.height);else{let H=te.width,Z=te.height;for(let oe=0;oe<le;oe++)t.texImage2D(n.TEXTURE_2D,oe,Ie,H,Z,0,pe,Ae,null),H>>=1,Z>>=1}}else if(ze.length>0){if(Fe&&ct){let H=Re(ze[0]);t.texStorage2D(n.TEXTURE_2D,le,Ie,H.width,H.height)}for(let H=0,Z=ze.length;H<Z;H++)me=ze[H],Fe?A&&t.texSubImage2D(n.TEXTURE_2D,H,0,0,pe,Ae,me):t.texImage2D(n.TEXTURE_2D,H,Ie,pe,Ae,me);y.generateMipmaps=!1}else if(Fe){if(ct){let H=Re(te);t.texStorage2D(n.TEXTURE_2D,le,Ie,H.width,H.height)}A&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,pe,Ae,te)}else t.texImage2D(n.TEXTURE_2D,0,Ie,pe,Ae,te);m(y)&&p(X),Me.__version=q.version,y.onUpdate&&y.onUpdate(y)}b.__version=y.version}function $(b,y,F){if(y.image.length!==6)return;let X=Je(b,y),Q=y.source;t.bindTexture(n.TEXTURE_CUBE_MAP,b.__webglTexture,n.TEXTURE0+F);let q=i.get(Q);if(Q.version!==q.__version||X===!0){t.activeTexture(n.TEXTURE0+F);let Me=rt.getPrimaries(rt.workingColorSpace),se=y.colorSpace===Vi?null:rt.getPrimaries(y.colorSpace),he=y.colorSpace===Vi||Me===se?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,y.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,y.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,y.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,he);let Ye=y.isCompressedTexture||y.image[0].isCompressedTexture,te=y.image[0]&&y.image[0].isDataTexture,pe=[];for(let Z=0;Z<6;Z++)!Ye&&!te?pe[Z]=x(y.image[Z],!0,r.maxCubemapSize):pe[Z]=te?y.image[Z].image:y.image[Z],pe[Z]=dt(y,pe[Z]);let Ae=pe[0],Ie=s.convert(y.format,y.colorSpace),me=s.convert(y.type),ze=S(y.internalFormat,Ie,me,y.colorSpace),Fe=y.isVideoTexture!==!0,ct=q.__version===void 0||X===!0,A=Q.dataReady,le=w(y,Ae);_e(n.TEXTURE_CUBE_MAP,y);let H;if(Ye){Fe&&ct&&t.texStorage2D(n.TEXTURE_CUBE_MAP,le,ze,Ae.width,Ae.height);for(let Z=0;Z<6;Z++){H=pe[Z].mipmaps;for(let oe=0;oe<H.length;oe++){let ce=H[oe];y.format!==Fn?Ie!==null?Fe?A&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,oe,0,0,ce.width,ce.height,Ie,ce.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,oe,ze,ce.width,ce.height,0,ce.data):console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):Fe?A&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,oe,0,0,ce.width,ce.height,Ie,me,ce.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,oe,ze,ce.width,ce.height,0,Ie,me,ce.data)}}}else{if(H=y.mipmaps,Fe&&ct){H.length>0&&le++;let Z=Re(pe[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,le,ze,Z.width,Z.height)}for(let Z=0;Z<6;Z++)if(te){Fe?A&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,pe[Z].width,pe[Z].height,Ie,me,pe[Z].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,ze,pe[Z].width,pe[Z].height,0,Ie,me,pe[Z].data);for(let oe=0;oe<H.length;oe++){let $e=H[oe].image[Z].image;Fe?A&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,oe+1,0,0,$e.width,$e.height,Ie,me,$e.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,oe+1,ze,$e.width,$e.height,0,Ie,me,$e.data)}}else{Fe?A&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,Ie,me,pe[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,ze,Ie,me,pe[Z]);for(let oe=0;oe<H.length;oe++){let ce=H[oe];Fe?A&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,oe+1,0,0,Ie,me,ce.image[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,oe+1,ze,Ie,me,ce.image[Z])}}}m(y)&&p(n.TEXTURE_CUBE_MAP),q.__version=Q.version,y.onUpdate&&y.onUpdate(y)}b.__version=y.version}function ee(b,y,F,X,Q,q){let Me=s.convert(F.format,F.colorSpace),se=s.convert(F.type),he=S(F.internalFormat,Me,se,F.colorSpace);if(!i.get(y).__hasExternalTextures){let te=Math.max(1,y.width>>q),pe=Math.max(1,y.height>>q);Q===n.TEXTURE_3D||Q===n.TEXTURE_2D_ARRAY?t.texImage3D(Q,q,he,te,pe,y.depth,0,Me,se,null):t.texImage2D(Q,q,he,te,pe,0,Me,se,null)}t.bindFramebuffer(n.FRAMEBUFFER,b),Xe(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,X,Q,i.get(F).__webglTexture,0,He(y)):(Q===n.TEXTURE_2D||Q>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&Q<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,X,Q,i.get(F).__webglTexture,q),t.bindFramebuffer(n.FRAMEBUFFER,null)}function ve(b,y,F){if(n.bindRenderbuffer(n.RENDERBUFFER,b),y.depthBuffer){let X=y.depthTexture,Q=X&&X.isDepthTexture?X.type:null,q=E(y.stencilBuffer,Q),Me=y.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,se=He(y);Xe(y)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,se,q,y.width,y.height):F?n.renderbufferStorageMultisample(n.RENDERBUFFER,se,q,y.width,y.height):n.renderbufferStorage(n.RENDERBUFFER,q,y.width,y.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,Me,n.RENDERBUFFER,b)}else{let X=y.textures;for(let Q=0;Q<X.length;Q++){let q=X[Q],Me=s.convert(q.format,q.colorSpace),se=s.convert(q.type),he=S(q.internalFormat,Me,se,q.colorSpace),Ye=He(y);F&&Xe(y)===!1?n.renderbufferStorageMultisample(n.RENDERBUFFER,Ye,he,y.width,y.height):Xe(y)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,Ye,he,y.width,y.height):n.renderbufferStorage(n.RENDERBUFFER,he,y.width,y.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function fe(b,y){if(y&&y.isWebGLCubeRenderTarget)throw new Error("Depth Texture with cube render targets is not supported");if(t.bindFramebuffer(n.FRAMEBUFFER,b),!(y.depthTexture&&y.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");(!i.get(y.depthTexture).__webglTexture||y.depthTexture.image.width!==y.width||y.depthTexture.image.height!==y.height)&&(y.depthTexture.image.width=y.width,y.depthTexture.image.height=y.height,y.depthTexture.needsUpdate=!0),Y(y.depthTexture,0);let X=i.get(y.depthTexture).__webglTexture,Q=He(y);if(y.depthTexture.format===Ds)Xe(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,n.DEPTH_ATTACHMENT,n.TEXTURE_2D,X,0,Q):n.framebufferTexture2D(n.FRAMEBUFFER,n.DEPTH_ATTACHMENT,n.TEXTURE_2D,X,0);else if(y.depthTexture.format===Fs)Xe(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,n.DEPTH_STENCIL_ATTACHMENT,n.TEXTURE_2D,X,0,Q):n.framebufferTexture2D(n.FRAMEBUFFER,n.DEPTH_STENCIL_ATTACHMENT,n.TEXTURE_2D,X,0);else throw new Error("Unknown depthTexture format")}function Pe(b){let y=i.get(b),F=b.isWebGLCubeRenderTarget===!0;if(y.__boundDepthTexture!==b.depthTexture){let X=b.depthTexture;if(y.__depthDisposeCallback&&y.__depthDisposeCallback(),X){let Q=()=>{delete y.__boundDepthTexture,delete y.__depthDisposeCallback,X.removeEventListener("dispose",Q)};X.addEventListener("dispose",Q),y.__depthDisposeCallback=Q}y.__boundDepthTexture=X}if(b.depthTexture&&!y.__autoAllocateDepthBuffer){if(F)throw new Error("target.depthTexture not supported in Cube render targets");fe(y.__webglFramebuffer,b)}else if(F){y.__webglDepthbuffer=[];for(let X=0;X<6;X++)if(t.bindFramebuffer(n.FRAMEBUFFER,y.__webglFramebuffer[X]),y.__webglDepthbuffer[X]===void 0)y.__webglDepthbuffer[X]=n.createRenderbuffer(),ve(y.__webglDepthbuffer[X],b,!1);else{let Q=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,q=y.__webglDepthbuffer[X];n.bindRenderbuffer(n.RENDERBUFFER,q),n.framebufferRenderbuffer(n.FRAMEBUFFER,Q,n.RENDERBUFFER,q)}}else if(t.bindFramebuffer(n.FRAMEBUFFER,y.__webglFramebuffer),y.__webglDepthbuffer===void 0)y.__webglDepthbuffer=n.createRenderbuffer(),ve(y.__webglDepthbuffer,b,!1);else{let X=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,Q=y.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,Q),n.framebufferRenderbuffer(n.FRAMEBUFFER,X,n.RENDERBUFFER,Q)}t.bindFramebuffer(n.FRAMEBUFFER,null)}function Se(b,y,F){let X=i.get(b);y!==void 0&&ee(X.__webglFramebuffer,b,b.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),F!==void 0&&Pe(b)}function We(b){let y=b.texture,F=i.get(b),X=i.get(y);b.addEventListener("dispose",T);let Q=b.textures,q=b.isWebGLCubeRenderTarget===!0,Me=Q.length>1;if(Me||(X.__webglTexture===void 0&&(X.__webglTexture=n.createTexture()),X.__version=y.version,o.memory.textures++),q){F.__webglFramebuffer=[];for(let se=0;se<6;se++)if(y.mipmaps&&y.mipmaps.length>0){F.__webglFramebuffer[se]=[];for(let he=0;he<y.mipmaps.length;he++)F.__webglFramebuffer[se][he]=n.createFramebuffer()}else F.__webglFramebuffer[se]=n.createFramebuffer()}else{if(y.mipmaps&&y.mipmaps.length>0){F.__webglFramebuffer=[];for(let se=0;se<y.mipmaps.length;se++)F.__webglFramebuffer[se]=n.createFramebuffer()}else F.__webglFramebuffer=n.createFramebuffer();if(Me)for(let se=0,he=Q.length;se<he;se++){let Ye=i.get(Q[se]);Ye.__webglTexture===void 0&&(Ye.__webglTexture=n.createTexture(),o.memory.textures++)}if(b.samples>0&&Xe(b)===!1){F.__webglMultisampledFramebuffer=n.createFramebuffer(),F.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,F.__webglMultisampledFramebuffer);for(let se=0;se<Q.length;se++){let he=Q[se];F.__webglColorRenderbuffer[se]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,F.__webglColorRenderbuffer[se]);let Ye=s.convert(he.format,he.colorSpace),te=s.convert(he.type),pe=S(he.internalFormat,Ye,te,he.colorSpace,b.isXRRenderTarget===!0),Ae=He(b);n.renderbufferStorageMultisample(n.RENDERBUFFER,Ae,pe,b.width,b.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+se,n.RENDERBUFFER,F.__webglColorRenderbuffer[se])}n.bindRenderbuffer(n.RENDERBUFFER,null),b.depthBuffer&&(F.__webglDepthRenderbuffer=n.createRenderbuffer(),ve(F.__webglDepthRenderbuffer,b,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(q){t.bindTexture(n.TEXTURE_CUBE_MAP,X.__webglTexture),_e(n.TEXTURE_CUBE_MAP,y);for(let se=0;se<6;se++)if(y.mipmaps&&y.mipmaps.length>0)for(let he=0;he<y.mipmaps.length;he++)ee(F.__webglFramebuffer[se][he],b,y,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+se,he);else ee(F.__webglFramebuffer[se],b,y,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+se,0);m(y)&&p(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(Me){for(let se=0,he=Q.length;se<he;se++){let Ye=Q[se],te=i.get(Ye);t.bindTexture(n.TEXTURE_2D,te.__webglTexture),_e(n.TEXTURE_2D,Ye),ee(F.__webglFramebuffer,b,Ye,n.COLOR_ATTACHMENT0+se,n.TEXTURE_2D,0),m(Ye)&&p(n.TEXTURE_2D)}t.unbindTexture()}else{let se=n.TEXTURE_2D;if((b.isWebGL3DRenderTarget||b.isWebGLArrayRenderTarget)&&(se=b.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(se,X.__webglTexture),_e(se,y),y.mipmaps&&y.mipmaps.length>0)for(let he=0;he<y.mipmaps.length;he++)ee(F.__webglFramebuffer[he],b,y,n.COLOR_ATTACHMENT0,se,he);else ee(F.__webglFramebuffer,b,y,n.COLOR_ATTACHMENT0,se,0);m(y)&&p(se),t.unbindTexture()}b.depthBuffer&&Pe(b)}function lt(b){let y=b.textures;for(let F=0,X=y.length;F<X;F++){let Q=y[F];if(m(Q)){let q=b.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:n.TEXTURE_2D,Me=i.get(Q).__webglTexture;t.bindTexture(q,Me),p(q),t.unbindTexture()}}}let je=[],D=[];function on(b){if(b.samples>0){if(Xe(b)===!1){let y=b.textures,F=b.width,X=b.height,Q=n.COLOR_BUFFER_BIT,q=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,Me=i.get(b),se=y.length>1;if(se)for(let he=0;he<y.length;he++)t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,Me.__webglMultisampledFramebuffer),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,Me.__webglFramebuffer);for(let he=0;he<y.length;he++){if(b.resolveDepthBuffer&&(b.depthBuffer&&(Q|=n.DEPTH_BUFFER_BIT),b.stencilBuffer&&b.resolveStencilBuffer&&(Q|=n.STENCIL_BUFFER_BIT)),se){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,Me.__webglColorRenderbuffer[he]);let Ye=i.get(y[he]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,Ye,0)}n.blitFramebuffer(0,0,F,X,0,0,F,X,Q,n.NEAREST),l===!0&&(je.length=0,D.length=0,je.push(n.COLOR_ATTACHMENT0+he),b.depthBuffer&&b.resolveDepthBuffer===!1&&(je.push(q),D.push(q),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,D)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,je))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),se)for(let he=0;he<y.length;he++){t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.RENDERBUFFER,Me.__webglColorRenderbuffer[he]);let Ye=i.get(y[he]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.TEXTURE_2D,Ye,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,Me.__webglMultisampledFramebuffer)}else if(b.depthBuffer&&b.resolveDepthBuffer===!1&&l){let y=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[y])}}}function He(b){return Math.min(r.maxSamples,b.samples)}function Xe(b){let y=i.get(b);return b.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&y.__useRenderToTexture!==!1}function Te(b){let y=o.render.frame;u.get(b)!==y&&(u.set(b,y),b.update())}function dt(b,y){let F=b.colorSpace,X=b.format,Q=b.type;return b.isCompressedTexture===!0||b.isVideoTexture===!0||F!==$i&&F!==Vi&&(rt.getTransfer(F)===mt?(X!==Fn||Q!==vi)&&console.warn("THREE.WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):console.error("THREE.WebGLTextures: Unsupported texture color space:",F)),y}function Re(b){return typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement?(c.width=b.naturalWidth||b.width,c.height=b.naturalHeight||b.height):typeof VideoFrame<"u"&&b instanceof VideoFrame?(c.width=b.displayWidth,c.height=b.displayHeight):(c.width=b.width,c.height=b.height),c}this.allocateTextureUnit=j,this.resetTextureUnits=M,this.setTexture2D=Y,this.setTexture2DArray=J,this.setTexture3D=W,this.setTextureCube=K,this.rebindTextures=Se,this.setupRenderTarget=We,this.updateRenderTargetMipmap=lt,this.updateMultisampleRenderTarget=on,this.setupDepthRenderbuffer=Pe,this.setupFrameBufferTexture=ee,this.useMultisampledRTT=Xe}function zI(n,e){function t(i,r=Vi){let s,o=rt.getTransfer(r);if(i===vi)return n.UNSIGNED_BYTE;if(i===Wh)return n.UNSIGNED_SHORT_4_4_4_4;if(i===jh)return n.UNSIGNED_SHORT_5_5_5_1;if(i===h_)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===d_)return n.BYTE;if(i===f_)return n.SHORT;if(i===So)return n.UNSIGNED_SHORT;if(i===Gh)return n.INT;if(i===Mr)return n.UNSIGNED_INT;if(i===hi)return n.FLOAT;if(i===Ao)return n.HALF_FLOAT;if(i===p_)return n.ALPHA;if(i===m_)return n.RGB;if(i===Fn)return n.RGBA;if(i===g_)return n.LUMINANCE;if(i===v_)return n.LUMINANCE_ALPHA;if(i===Ds)return n.DEPTH_COMPONENT;if(i===Fs)return n.DEPTH_STENCIL;if(i===y_)return n.RED;if(i===$h)return n.RED_INTEGER;if(i===__)return n.RG;if(i===qh)return n.RG_INTEGER;if(i===Xh)return n.RGBA_INTEGER;if(i===Fl||i===Ol||i===Ll||i===kl)if(o===mt)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===Fl)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===Ol)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===Ll)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===kl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===Fl)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===Ol)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===Ll)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===kl)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===Hf||i===zf||i===Gf||i===Wf)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===Hf)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===zf)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===Gf)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===Wf)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===jf||i===$f||i===qf)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===jf||i===$f)return o===mt?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===qf)return o===mt?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC}else return null;if(i===Xf||i===Yf||i===Zf||i===Kf||i===Jf||i===Qf||i===eh||i===th||i===nh||i===ih||i===rh||i===sh||i===oh||i===ah)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===Xf)return o===mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===Yf)return o===mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===Zf)return o===mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===Kf)return o===mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===Jf)return o===mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===Qf)return o===mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===eh)return o===mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===th)return o===mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===nh)return o===mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===ih)return o===mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===rh)return o===mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===sh)return o===mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===oh)return o===mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===ah)return o===mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===Ul||i===lh||i===ch)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===Ul)return o===mt?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===lh)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===ch)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===x_||i===uh||i===dh||i===fh)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===Ul)return s.COMPRESSED_RED_RGTC1_EXT;if(i===uh)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===dh)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===fh)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===Ps?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var Ah=class extends Yt{constructor(e=[]){super(),this.isArrayCamera=!0,this.cameras=e}},mi=class extends _i{constructor(){super(),this.isGroup=!0,this.type="Group"}},GI={type:"move"},wo=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new mi,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new mi,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new V,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new V),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new mi,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new V,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new V),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,l=this._grip,c=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(c&&e.hand){o=!0;for(let x of e.hand.values()){let m=t.getJointPose(x,i),p=this._getHandJoint(c,x);m!==null&&(p.matrix.fromArray(m.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=m.radius),p.visible=m!==null}let u=c.joints["index-finger-tip"],d=c.joints["thumb-tip"],f=u.position.distanceTo(d.position),h=.02,g=.005;c.inputState.pinching&&f>h+g?(c.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!c.inputState.pinching&&f<=h-g&&(c.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else l!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(l.matrix.fromArray(s.transform.matrix),l.matrix.decompose(l.position,l.rotation,l.scale),l.matrixWorldNeedsUpdate=!0,s.linearVelocity?(l.hasLinearVelocity=!0,l.linearVelocity.copy(s.linearVelocity)):l.hasLinearVelocity=!1,s.angularVelocity?(l.hasAngularVelocity=!0,l.angularVelocity.copy(s.angularVelocity)):l.hasAngularVelocity=!1));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(GI)))}return a!==null&&(a.visible=r!==null),l!==null&&(l.visible=s!==null),c!==null&&(c.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new mi;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},WI=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,jI=`
uniform sampler2DArray depthColor;
uniform float depthWidth;
uniform float depthHeight;

void main() {

	vec2 coord = vec2( gl_FragCoord.x / depthWidth, gl_FragCoord.y / depthHeight );

	if ( coord.x >= 1.0 ) {

		gl_FragDepth = texture( depthColor, vec3( coord.x - 1.0, coord.y, 1 ) ).r;

	} else {

		gl_FragDepth = texture( depthColor, vec3( coord.x, coord.y, 0 ) ).r;

	}

}`,Ih=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t,i){if(this.texture===null){let r=new Tr,s=e.properties.get(r);s.__webglTexture=t.texture,(t.depthNear!=i.depthNear||t.depthFar!=i.depthFar)&&(this.depthNear=t.depthNear,this.depthFar=t.depthFar),this.texture=r}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new Zn({vertexShader:WI,fragmentShader:jI,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new rn(new ec(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},Rh=class extends Gi{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",l=1,c=null,u=null,d=null,f=null,h=null,g=null,x=new Ih,m=t.getContextAttributes(),p=null,S=null,E=[],w=[],L=new st,T=null,C=new Yt;C.layers.enable(1),C.viewport=new Et;let U=new Yt;U.layers.enable(2),U.viewport=new Et;let ie=[C,U],v=new Ah;v.layers.enable(1),v.layers.enable(2);let M=null,j=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function($){let ee=E[$];return ee===void 0&&(ee=new wo,E[$]=ee),ee.getTargetRaySpace()},this.getControllerGrip=function($){let ee=E[$];return ee===void 0&&(ee=new wo,E[$]=ee),ee.getGripSpace()},this.getHand=function($){let ee=E[$];return ee===void 0&&(ee=new wo,E[$]=ee),ee.getHandSpace()};function z($){let ee=w.indexOf($.inputSource);if(ee===-1)return;let ve=E[ee];ve!==void 0&&(ve.update($.inputSource,$.frame,c||o),ve.dispatchEvent({type:$.type,data:$.inputSource}))}function Y(){r.removeEventListener("select",z),r.removeEventListener("selectstart",z),r.removeEventListener("selectend",z),r.removeEventListener("squeeze",z),r.removeEventListener("squeezestart",z),r.removeEventListener("squeezeend",z),r.removeEventListener("end",Y),r.removeEventListener("inputsourceschange",J);for(let $=0;$<E.length;$++){let ee=w[$];ee!==null&&(w[$]=null,E[$].disconnect(ee))}M=null,j=null,x.reset(),e.setRenderTarget(p),h=null,f=null,d=null,r=null,S=null,ot.stop(),i.isPresenting=!1,e.setPixelRatio(T),e.setSize(L.width,L.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function($){s=$,i.isPresenting===!0&&console.warn("THREE.WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function($){a=$,i.isPresenting===!0&&console.warn("THREE.WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return c||o},this.setReferenceSpace=function($){c=$},this.getBaseLayer=function(){return f!==null?f:h},this.getBinding=function(){return d},this.getFrame=function(){return g},this.getSession=function(){return r},this.setSession=function($){return Ir(this,null,function*(){if(r=$,r!==null){if(p=e.getRenderTarget(),r.addEventListener("select",z),r.addEventListener("selectstart",z),r.addEventListener("selectend",z),r.addEventListener("squeeze",z),r.addEventListener("squeezestart",z),r.addEventListener("squeezeend",z),r.addEventListener("end",Y),r.addEventListener("inputsourceschange",J),m.xrCompatible!==!0&&(yield t.makeXRCompatible()),T=e.getPixelRatio(),e.getSize(L),r.renderState.layers===void 0){let ee={antialias:m.antialias,alpha:!0,depth:m.depth,stencil:m.stencil,framebufferScaleFactor:s};h=new XRWebGLLayer(r,t,ee),r.updateRenderState({baseLayer:h}),e.setPixelRatio(1),e.setSize(h.framebufferWidth,h.framebufferHeight,!1),S=new yi(h.framebufferWidth,h.framebufferHeight,{format:Fn,type:vi,colorSpace:e.outputColorSpace,stencilBuffer:m.stencil})}else{let ee=null,ve=null,fe=null;m.depth&&(fe=m.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,ee=m.stencil?Fs:Ds,ve=m.stencil?Ps:Mr);let Pe={colorFormat:t.RGBA8,depthFormat:fe,scaleFactor:s};d=new XRWebGLBinding(r,t),f=d.createProjectionLayer(Pe),r.updateRenderState({layers:[f]}),e.setPixelRatio(1),e.setSize(f.textureWidth,f.textureHeight,!1),S=new yi(f.textureWidth,f.textureHeight,{format:Fn,type:vi,depthTexture:new ic(f.textureWidth,f.textureHeight,ve,void 0,void 0,void 0,void 0,void 0,void 0,ee),stencilBuffer:m.stencil,colorSpace:e.outputColorSpace,samples:m.antialias?4:0,resolveDepthBuffer:f.ignoreDepthValues===!1})}S.isXRRenderTarget=!0,this.setFoveation(l),c=null,o=yield r.requestReferenceSpace(a),ot.setContext(r),ot.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}})},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return x.getDepthTexture()};function J($){for(let ee=0;ee<$.removed.length;ee++){let ve=$.removed[ee],fe=w.indexOf(ve);fe>=0&&(w[fe]=null,E[fe].disconnect(ve))}for(let ee=0;ee<$.added.length;ee++){let ve=$.added[ee],fe=w.indexOf(ve);if(fe===-1){for(let Se=0;Se<E.length;Se++)if(Se>=w.length){w.push(ve),fe=Se;break}else if(w[Se]===null){w[Se]=ve,fe=Se;break}if(fe===-1)break}let Pe=E[fe];Pe&&Pe.connect(ve)}}let W=new V,K=new V;function G($,ee,ve){W.setFromMatrixPosition(ee.matrixWorld),K.setFromMatrixPosition(ve.matrixWorld);let fe=W.distanceTo(K),Pe=ee.projectionMatrix.elements,Se=ve.projectionMatrix.elements,We=Pe[14]/(Pe[10]-1),lt=Pe[14]/(Pe[10]+1),je=(Pe[9]+1)/Pe[5],D=(Pe[9]-1)/Pe[5],on=(Pe[8]-1)/Pe[0],He=(Se[8]+1)/Se[0],Xe=We*on,Te=We*He,dt=fe/(-on+He),Re=dt*-on;if(ee.matrixWorld.decompose($.position,$.quaternion,$.scale),$.translateX(Re),$.translateZ(dt),$.matrixWorld.compose($.position,$.quaternion,$.scale),$.matrixWorldInverse.copy($.matrixWorld).invert(),Pe[10]===-1)$.projectionMatrix.copy(ee.projectionMatrix),$.projectionMatrixInverse.copy(ee.projectionMatrixInverse);else{let b=We+dt,y=lt+dt,F=Xe-Re,X=Te+(fe-Re),Q=je*lt/y*b,q=D*lt/y*b;$.projectionMatrix.makePerspective(F,X,Q,q,b,y),$.projectionMatrixInverse.copy($.projectionMatrix).invert()}}function ue($,ee){ee===null?$.matrixWorld.copy($.matrix):$.matrixWorld.multiplyMatrices(ee.matrixWorld,$.matrix),$.matrixWorldInverse.copy($.matrixWorld).invert()}this.updateCamera=function($){if(r===null)return;let ee=$.near,ve=$.far;x.texture!==null&&(x.depthNear>0&&(ee=x.depthNear),x.depthFar>0&&(ve=x.depthFar)),v.near=U.near=C.near=ee,v.far=U.far=C.far=ve,(M!==v.near||j!==v.far)&&(r.updateRenderState({depthNear:v.near,depthFar:v.far}),M=v.near,j=v.far);let fe=$.parent,Pe=v.cameras;ue(v,fe);for(let Se=0;Se<Pe.length;Se++)ue(Pe[Se],fe);Pe.length===2?G(v,C,U):v.projectionMatrix.copy(C.projectionMatrix),de($,v,fe)};function de($,ee,ve){ve===null?$.matrix.copy(ee.matrixWorld):($.matrix.copy(ve.matrixWorld),$.matrix.invert(),$.matrix.multiply(ee.matrixWorld)),$.matrix.decompose($.position,$.quaternion,$.scale),$.updateMatrixWorld(!0),$.projectionMatrix.copy(ee.projectionMatrix),$.projectionMatrixInverse.copy(ee.projectionMatrixInverse),$.isPerspectiveCamera&&($.fov=ph*2*Math.atan(1/$.projectionMatrix.elements[5]),$.zoom=1)}this.getCamera=function(){return v},this.getFoveation=function(){if(!(f===null&&h===null))return l},this.setFoveation=function($){l=$,f!==null&&(f.fixedFoveation=$),h!==null&&h.fixedFoveation!==void 0&&(h.fixedFoveation=$)},this.hasDepthSensing=function(){return x.texture!==null},this.getDepthSensingMesh=function(){return x.getMesh(v)};let _e=null;function Je($,ee){if(u=ee.getViewerPose(c||o),g=ee,u!==null){let ve=u.views;h!==null&&(e.setRenderTargetFramebuffer(S,h.framebuffer),e.setRenderTarget(S));let fe=!1;ve.length!==v.cameras.length&&(v.cameras.length=0,fe=!0);for(let Se=0;Se<ve.length;Se++){let We=ve[Se],lt=null;if(h!==null)lt=h.getViewport(We);else{let D=d.getViewSubImage(f,We);lt=D.viewport,Se===0&&(e.setRenderTargetTextures(S,D.colorTexture,f.ignoreDepthValues?void 0:D.depthStencilTexture),e.setRenderTarget(S))}let je=ie[Se];je===void 0&&(je=new Yt,je.layers.enable(Se),je.viewport=new Et,ie[Se]=je),je.matrix.fromArray(We.transform.matrix),je.matrix.decompose(je.position,je.quaternion,je.scale),je.projectionMatrix.fromArray(We.projectionMatrix),je.projectionMatrixInverse.copy(je.projectionMatrix).invert(),je.viewport.set(lt.x,lt.y,lt.width,lt.height),Se===0&&(v.matrix.copy(je.matrix),v.matrix.decompose(v.position,v.quaternion,v.scale)),fe===!0&&v.cameras.push(je)}let Pe=r.enabledFeatures;if(Pe&&Pe.includes("depth-sensing")){let Se=d.getDepthInformation(ve[0]);Se&&Se.isValid&&Se.texture&&x.init(e,Se,r.renderState)}}for(let ve=0;ve<E.length;ve++){let fe=w[ve],Pe=E[ve];fe!==null&&Pe!==void 0&&Pe.update(fe,ee,c||o)}_e&&_e($,ee),ee.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:ee}),g=null}let ot=new C_;ot.setAnimationLoop(Je),this.setAnimationLoop=function($){_e=$},this.dispose=function(){}}},pr=new br,$I=new Dt;function qI(n,e){function t(m,p){m.matrixAutoUpdate===!0&&m.updateMatrix(),p.value.copy(m.matrix)}function i(m,p){p.color.getRGB(m.fogColor.value,S_(n)),p.isFog?(m.fogNear.value=p.near,m.fogFar.value=p.far):p.isFogExp2&&(m.fogDensity.value=p.density)}function r(m,p,S,E,w){p.isMeshBasicMaterial||p.isMeshLambertMaterial?s(m,p):p.isMeshToonMaterial?(s(m,p),d(m,p)):p.isMeshPhongMaterial?(s(m,p),u(m,p)):p.isMeshStandardMaterial?(s(m,p),f(m,p),p.isMeshPhysicalMaterial&&h(m,p,w)):p.isMeshMatcapMaterial?(s(m,p),g(m,p)):p.isMeshDepthMaterial?s(m,p):p.isMeshDistanceMaterial?(s(m,p),x(m,p)):p.isMeshNormalMaterial?s(m,p):p.isLineBasicMaterial?(o(m,p),p.isLineDashedMaterial&&a(m,p)):p.isPointsMaterial?l(m,p,S,E):p.isSpriteMaterial?c(m,p):p.isShadowMaterial?(m.color.value.copy(p.color),m.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(m,p){m.opacity.value=p.opacity,p.color&&m.diffuse.value.copy(p.color),p.emissive&&m.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.bumpMap&&(m.bumpMap.value=p.bumpMap,t(p.bumpMap,m.bumpMapTransform),m.bumpScale.value=p.bumpScale,p.side===sn&&(m.bumpScale.value*=-1)),p.normalMap&&(m.normalMap.value=p.normalMap,t(p.normalMap,m.normalMapTransform),m.normalScale.value.copy(p.normalScale),p.side===sn&&m.normalScale.value.negate()),p.displacementMap&&(m.displacementMap.value=p.displacementMap,t(p.displacementMap,m.displacementMapTransform),m.displacementScale.value=p.displacementScale,m.displacementBias.value=p.displacementBias),p.emissiveMap&&(m.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,m.emissiveMapTransform)),p.specularMap&&(m.specularMap.value=p.specularMap,t(p.specularMap,m.specularMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest);let S=e.get(p),E=S.envMap,w=S.envMapRotation;E&&(m.envMap.value=E,pr.copy(w),pr.x*=-1,pr.y*=-1,pr.z*=-1,E.isCubeTexture&&E.isRenderTargetTexture===!1&&(pr.y*=-1,pr.z*=-1),m.envMapRotation.value.setFromMatrix4($I.makeRotationFromEuler(pr)),m.flipEnvMap.value=E.isCubeTexture&&E.isRenderTargetTexture===!1?-1:1,m.reflectivity.value=p.reflectivity,m.ior.value=p.ior,m.refractionRatio.value=p.refractionRatio),p.lightMap&&(m.lightMap.value=p.lightMap,m.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,m.lightMapTransform)),p.aoMap&&(m.aoMap.value=p.aoMap,m.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,m.aoMapTransform))}function o(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform))}function a(m,p){m.dashSize.value=p.dashSize,m.totalSize.value=p.dashSize+p.gapSize,m.scale.value=p.scale}function l(m,p,S,E){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.size.value=p.size*S,m.scale.value=E*.5,p.map&&(m.map.value=p.map,t(p.map,m.uvTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function c(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.rotation.value=p.rotation,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function u(m,p){m.specular.value.copy(p.specular),m.shininess.value=Math.max(p.shininess,1e-4)}function d(m,p){p.gradientMap&&(m.gradientMap.value=p.gradientMap)}function f(m,p){m.metalness.value=p.metalness,p.metalnessMap&&(m.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,m.metalnessMapTransform)),m.roughness.value=p.roughness,p.roughnessMap&&(m.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,m.roughnessMapTransform)),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)}function h(m,p,S){m.ior.value=p.ior,p.sheen>0&&(m.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),m.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(m.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,m.sheenColorMapTransform)),p.sheenRoughnessMap&&(m.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,m.sheenRoughnessMapTransform))),p.clearcoat>0&&(m.clearcoat.value=p.clearcoat,m.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(m.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,m.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(m.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,m.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(m.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,m.clearcoatNormalMapTransform),m.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===sn&&m.clearcoatNormalScale.value.negate())),p.dispersion>0&&(m.dispersion.value=p.dispersion),p.iridescence>0&&(m.iridescence.value=p.iridescence,m.iridescenceIOR.value=p.iridescenceIOR,m.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],m.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(m.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,m.iridescenceMapTransform)),p.iridescenceThicknessMap&&(m.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,m.iridescenceThicknessMapTransform))),p.transmission>0&&(m.transmission.value=p.transmission,m.transmissionSamplerMap.value=S.texture,m.transmissionSamplerSize.value.set(S.width,S.height),p.transmissionMap&&(m.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,m.transmissionMapTransform)),m.thickness.value=p.thickness,p.thicknessMap&&(m.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,m.thicknessMapTransform)),m.attenuationDistance.value=p.attenuationDistance,m.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(m.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(m.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,m.anisotropyMapTransform))),m.specularIntensity.value=p.specularIntensity,m.specularColor.value.copy(p.specularColor),p.specularColorMap&&(m.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,m.specularColorMapTransform)),p.specularIntensityMap&&(m.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,m.specularIntensityMapTransform))}function g(m,p){p.matcap&&(m.matcap.value=p.matcap)}function x(m,p){let S=e.get(p).light;m.referencePosition.value.setFromMatrixPosition(S.matrixWorld),m.nearDistance.value=S.shadow.camera.near,m.farDistance.value=S.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function XI(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function l(S,E){let w=E.program;i.uniformBlockBinding(S,w)}function c(S,E){let w=r[S.id];w===void 0&&(g(S),w=u(S),r[S.id]=w,S.addEventListener("dispose",m));let L=E.program;i.updateUBOMapping(S,L);let T=e.render.frame;s[S.id]!==T&&(f(S),s[S.id]=T)}function u(S){let E=d();S.__bindingPointIndex=E;let w=n.createBuffer(),L=S.__size,T=S.usage;return n.bindBuffer(n.UNIFORM_BUFFER,w),n.bufferData(n.UNIFORM_BUFFER,L,T),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,E,w),w}function d(){for(let S=0;S<a;S++)if(o.indexOf(S)===-1)return o.push(S),S;return console.error("THREE.WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function f(S){let E=r[S.id],w=S.uniforms,L=S.__cache;n.bindBuffer(n.UNIFORM_BUFFER,E);for(let T=0,C=w.length;T<C;T++){let U=Array.isArray(w[T])?w[T]:[w[T]];for(let ie=0,v=U.length;ie<v;ie++){let M=U[ie];if(h(M,T,ie,L)===!0){let j=M.__offset,z=Array.isArray(M.value)?M.value:[M.value],Y=0;for(let J=0;J<z.length;J++){let W=z[J],K=x(W);typeof W=="number"||typeof W=="boolean"?(M.__data[0]=W,n.bufferSubData(n.UNIFORM_BUFFER,j+Y,M.__data)):W.isMatrix3?(M.__data[0]=W.elements[0],M.__data[1]=W.elements[1],M.__data[2]=W.elements[2],M.__data[3]=0,M.__data[4]=W.elements[3],M.__data[5]=W.elements[4],M.__data[6]=W.elements[5],M.__data[7]=0,M.__data[8]=W.elements[6],M.__data[9]=W.elements[7],M.__data[10]=W.elements[8],M.__data[11]=0):(W.toArray(M.__data,Y),Y+=K.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,j,M.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function h(S,E,w,L){let T=S.value,C=E+"_"+w;if(L[C]===void 0)return typeof T=="number"||typeof T=="boolean"?L[C]=T:L[C]=T.clone(),!0;{let U=L[C];if(typeof T=="number"||typeof T=="boolean"){if(U!==T)return L[C]=T,!0}else if(U.equals(T)===!1)return U.copy(T),!0}return!1}function g(S){let E=S.uniforms,w=0,L=16;for(let C=0,U=E.length;C<U;C++){let ie=Array.isArray(E[C])?E[C]:[E[C]];for(let v=0,M=ie.length;v<M;v++){let j=ie[v],z=Array.isArray(j.value)?j.value:[j.value];for(let Y=0,J=z.length;Y<J;Y++){let W=z[Y],K=x(W),G=w%L,ue=G%K.boundary,de=G+ue;w+=ue,de!==0&&L-de<K.storage&&(w+=L-de),j.__data=new Float32Array(K.storage/Float32Array.BYTES_PER_ELEMENT),j.__offset=w,w+=K.storage}}}let T=w%L;return T>0&&(w+=L-T),S.__size=w,S.__cache={},this}function x(S){let E={boundary:0,storage:0};return typeof S=="number"||typeof S=="boolean"?(E.boundary=4,E.storage=4):S.isVector2?(E.boundary=8,E.storage=8):S.isVector3||S.isColor?(E.boundary=16,E.storage=12):S.isVector4?(E.boundary=16,E.storage=16):S.isMatrix3?(E.boundary=48,E.storage=48):S.isMatrix4?(E.boundary=64,E.storage=64):S.isTexture?console.warn("THREE.WebGLRenderer: Texture samplers can not be part of an uniforms group."):console.warn("THREE.WebGLRenderer: Unsupported uniform value type.",S),E}function m(S){let E=S.target;E.removeEventListener("dispose",m);let w=o.indexOf(E.__bindingPointIndex);o.splice(w,1),n.deleteBuffer(r[E.id]),delete r[E.id],delete s[E.id]}function p(){for(let S in r)n.deleteBuffer(r[S]);o=[],r={},s={}}return{bind:l,update:c,dispose:p}}var rc=class{constructor(e={}){let{canvas:t=KS(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:l=!0,preserveDrawingBuffer:c=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:d=!1}=e;this.isWebGLRenderer=!0;let f;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");f=i.getContextAttributes().alpha}else f=o;let h=new Uint32Array(4),g=new Int32Array(4),x=null,m=null,p=[],S=[];this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this._outputColorSpace=Xn,this.toneMapping=Hi,this.toneMappingExposure=1;let E=this,w=!1,L=0,T=0,C=null,U=-1,ie=null,v=new Et,M=new Et,j=null,z=new qe(0),Y=0,J=t.width,W=t.height,K=1,G=null,ue=null,de=new Et(0,0,J,W),_e=new Et(0,0,J,W),Je=!1,ot=new Do,$=!1,ee=!1,ve=new Dt,fe=new Dt,Pe=new V,Se=new Et,We={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},lt=!1;function je(){return C===null?K:1}let D=i;function on(_,I){return t.getContext(_,I)}try{let _={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:l,preserveDrawingBuffer:c,powerPreference:u,failIfMajorPerformanceCaveat:d};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${zh}`),t.addEventListener("webglcontextlost",Z,!1),t.addEventListener("webglcontextrestored",oe,!1),t.addEventListener("webglcontextcreationerror",ce,!1),D===null){let I="webgl2";if(D=on(I,_),D===null)throw on(I)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(_){throw console.error("THREE.WebGLRenderer: "+_.message),_}let He,Xe,Te,dt,Re,b,y,F,X,Q,q,Me,se,he,Ye,te,pe,Ae,Ie,me,ze,Fe,ct,A;function le(){He=new uA(D),He.init(),Fe=new zI(D,He),Xe=new rA(D,He,e,Fe),Te=new VI(D),Xe.reverseDepthBuffer&&Te.buffers.depth.setReversed(!0),dt=new hA(D),Re=new CI,b=new HI(D,He,Te,Re,Xe,Fe,dt),y=new oA(E),F=new cA(E),X=new xC(D),ct=new nA(D,X),Q=new dA(D,X,dt,ct),q=new mA(D,Q,X,dt),Ie=new pA(D,Xe,b),te=new sA(Re),Me=new SI(E,y,F,He,Xe,ct,te),se=new qI(E,Re),he=new TI,Ye=new FI(He),Ae=new tA(E,y,F,Te,q,f,l),pe=new kI(E,q,Xe),A=new XI(D,dt,Xe,Te),me=new iA(D,He,dt),ze=new fA(D,He,dt),dt.programs=Me.programs,E.capabilities=Xe,E.extensions=He,E.properties=Re,E.renderLists=he,E.shadowMap=pe,E.state=Te,E.info=dt}le();let H=new Rh(E,D);this.xr=H,this.getContext=function(){return D},this.getContextAttributes=function(){return D.getContextAttributes()},this.forceContextLoss=function(){let _=He.get("WEBGL_lose_context");_&&_.loseContext()},this.forceContextRestore=function(){let _=He.get("WEBGL_lose_context");_&&_.restoreContext()},this.getPixelRatio=function(){return K},this.setPixelRatio=function(_){_!==void 0&&(K=_,this.setSize(J,W,!1))},this.getSize=function(_){return _.set(J,W)},this.setSize=function(_,I,O=!0){if(H.isPresenting){console.warn("THREE.WebGLRenderer: Can't change size while VR device is presenting.");return}J=_,W=I,t.width=Math.floor(_*K),t.height=Math.floor(I*K),O===!0&&(t.style.width=_+"px",t.style.height=I+"px"),this.setViewport(0,0,_,I)},this.getDrawingBufferSize=function(_){return _.set(J*K,W*K).floor()},this.setDrawingBufferSize=function(_,I,O){J=_,W=I,K=O,t.width=Math.floor(_*O),t.height=Math.floor(I*O),this.setViewport(0,0,_,I)},this.getCurrentViewport=function(_){return _.copy(v)},this.getViewport=function(_){return _.copy(de)},this.setViewport=function(_,I,O,k){_.isVector4?de.set(_.x,_.y,_.z,_.w):de.set(_,I,O,k),Te.viewport(v.copy(de).multiplyScalar(K).round())},this.getScissor=function(_){return _.copy(_e)},this.setScissor=function(_,I,O,k){_.isVector4?_e.set(_.x,_.y,_.z,_.w):_e.set(_,I,O,k),Te.scissor(M.copy(_e).multiplyScalar(K).round())},this.getScissorTest=function(){return Je},this.setScissorTest=function(_){Te.setScissorTest(Je=_)},this.setOpaqueSort=function(_){G=_},this.setTransparentSort=function(_){ue=_},this.getClearColor=function(_){return _.copy(Ae.getClearColor())},this.setClearColor=function(){Ae.setClearColor.apply(Ae,arguments)},this.getClearAlpha=function(){return Ae.getClearAlpha()},this.setClearAlpha=function(){Ae.setClearAlpha.apply(Ae,arguments)},this.clear=function(_=!0,I=!0,O=!0){let k=0;if(_){let R=!1;if(C!==null){let ne=C.texture.format;R=ne===Xh||ne===qh||ne===$h}if(R){let ne=C.texture.type,ae=ne===vi||ne===Mr||ne===So||ne===Ps||ne===Wh||ne===jh,ge=Ae.getClearColor(),ye=Ae.getClearAlpha(),we=ge.r,Ce=ge.g,Ee=ge.b;ae?(h[0]=we,h[1]=Ce,h[2]=Ee,h[3]=ye,D.clearBufferuiv(D.COLOR,0,h)):(g[0]=we,g[1]=Ce,g[2]=Ee,g[3]=ye,D.clearBufferiv(D.COLOR,0,g))}else k|=D.COLOR_BUFFER_BIT}I&&(k|=D.DEPTH_BUFFER_BIT,D.clearDepth(this.capabilities.reverseDepthBuffer?0:1)),O&&(k|=D.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),D.clear(k)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.dispose=function(){t.removeEventListener("webglcontextlost",Z,!1),t.removeEventListener("webglcontextrestored",oe,!1),t.removeEventListener("webglcontextcreationerror",ce,!1),he.dispose(),Ye.dispose(),Re.dispose(),y.dispose(),F.dispose(),q.dispose(),ct.dispose(),A.dispose(),Me.dispose(),H.dispose(),H.removeEventListener("sessionstart",op),H.removeEventListener("sessionend",ap),qi.stop()};function Z(_){_.preventDefault(),console.log("THREE.WebGLRenderer: Context Lost."),w=!0}function oe(){console.log("THREE.WebGLRenderer: Context Restored."),w=!1;let _=dt.autoReset,I=pe.enabled,O=pe.autoUpdate,k=pe.needsUpdate,R=pe.type;le(),dt.autoReset=_,pe.enabled=I,pe.autoUpdate=O,pe.needsUpdate=k,pe.type=R}function ce(_){console.error("THREE.WebGLRenderer: A WebGL context could not be created. Reason: ",_.statusMessage)}function $e(_){let I=_.target;I.removeEventListener("dispose",$e),Tt(I)}function Tt(_){Zt(_),Re.remove(_)}function Zt(_){let I=Re.get(_).programs;I!==void 0&&(I.forEach(function(O){Me.releaseProgram(O)}),_.isShaderMaterial&&Me.releaseShaderCache(_))}this.renderBufferDirect=function(_,I,O,k,R,ne){I===null&&(I=We);let ae=R.isMesh&&R.matrixWorld.determinant()<0,ge=Q_(_,I,O,k,R);Te.setMaterial(k,ae);let ye=O.index,we=1;if(k.wireframe===!0){if(ye=Q.getWireframeAttribute(O),ye===void 0)return;we=2}let Ce=O.drawRange,Ee=O.attributes.position,at=Ce.start*we,ft=(Ce.start+Ce.count)*we;ne!==null&&(at=Math.max(at,ne.start*we),ft=Math.min(ft,(ne.start+ne.count)*we)),ye!==null?(at=Math.max(at,0),ft=Math.min(ft,ye.count)):Ee!=null&&(at=Math.max(at,0),ft=Math.min(ft,Ee.count));let vt=ft-at;if(vt<0||vt===1/0)return;ct.setup(R,k,ge,O,ye);let an,et=me;if(ye!==null&&(an=X.get(ye),et=ze,et.setIndex(an)),R.isMesh)k.wireframe===!0?(Te.setLineWidth(k.wireframeLinewidth*je()),et.setMode(D.LINES)):et.setMode(D.TRIANGLES);else if(R.isLine){let be=k.linewidth;be===void 0&&(be=1),Te.setLineWidth(be*je()),R.isLineSegments?et.setMode(D.LINES):R.isLineLoop?et.setMode(D.LINE_LOOP):et.setMode(D.LINE_STRIP)}else R.isPoints?et.setMode(D.POINTS):R.isSprite&&et.setMode(D.TRIANGLES);if(R.isBatchedMesh)if(R._multiDrawInstances!==null)et.renderMultiDrawInstances(R._multiDrawStarts,R._multiDrawCounts,R._multiDrawCount,R._multiDrawInstances);else if(He.get("WEBGL_multi_draw"))et.renderMultiDraw(R._multiDrawStarts,R._multiDrawCounts,R._multiDrawCount);else{let be=R._multiDrawStarts,Lt=R._multiDrawCounts,tt=R._multiDrawCount,En=ye?X.get(ye).bytesPerElement:1,Ar=Re.get(k).currentProgram.getUniforms();for(let ln=0;ln<tt;ln++)Ar.setValue(D,"_gl_DrawID",ln),et.render(be[ln]/En,Lt[ln])}else if(R.isInstancedMesh)et.renderInstances(at,vt,R.count);else if(O.isInstancedBufferGeometry){let be=O._maxInstanceCount!==void 0?O._maxInstanceCount:1/0,Lt=Math.min(O.instanceCount,be);et.renderInstances(at,vt,Lt)}else et.render(at,vt)};function Qe(_,I,O){_.transparent===!0&&_.side===fi&&_.forceSinglePass===!1?(_.side=sn,_.needsUpdate=!0,ko(_,I,O),_.side=zi,_.needsUpdate=!0,ko(_,I,O),_.side=fi):ko(_,I,O)}this.compile=function(_,I,O=null){O===null&&(O=_),m=Ye.get(O),m.init(I),S.push(m),O.traverseVisible(function(R){R.isLight&&R.layers.test(I.layers)&&(m.pushLight(R),R.castShadow&&m.pushShadow(R))}),_!==O&&_.traverseVisible(function(R){R.isLight&&R.layers.test(I.layers)&&(m.pushLight(R),R.castShadow&&m.pushShadow(R))}),m.setupLights();let k=new Set;return _.traverse(function(R){if(!(R.isMesh||R.isPoints||R.isLine||R.isSprite))return;let ne=R.material;if(ne)if(Array.isArray(ne))for(let ae=0;ae<ne.length;ae++){let ge=ne[ae];Qe(ge,O,R),k.add(ge)}else Qe(ne,O,R),k.add(ne)}),S.pop(),m=null,k},this.compileAsync=function(_,I,O=null){let k=this.compile(_,I,O);return new Promise(R=>{function ne(){if(k.forEach(function(ae){Re.get(ae).currentProgram.isReady()&&k.delete(ae)}),k.size===0){R(_);return}setTimeout(ne,10)}He.get("KHR_parallel_shader_compile")!==null?ne():setTimeout(ne,10)})};let Kt=null;function Kn(_){Kt&&Kt(_)}function op(){qi.stop()}function ap(){qi.start()}let qi=new C_;qi.setAnimationLoop(Kn),typeof self<"u"&&qi.setContext(self),this.setAnimationLoop=function(_){Kt=_,H.setAnimationLoop(_),_===null?qi.stop():qi.start()},H.addEventListener("sessionstart",op),H.addEventListener("sessionend",ap),this.render=function(_,I){if(I!==void 0&&I.isCamera!==!0){console.error("THREE.WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(w===!0)return;if(_.matrixWorldAutoUpdate===!0&&_.updateMatrixWorld(),I.parent===null&&I.matrixWorldAutoUpdate===!0&&I.updateMatrixWorld(),H.enabled===!0&&H.isPresenting===!0&&(H.cameraAutoUpdate===!0&&H.updateCamera(I),I=H.getCamera()),_.isScene===!0&&_.onBeforeRender(E,_,I,C),m=Ye.get(_,S.length),m.init(I),S.push(m),fe.multiplyMatrices(I.projectionMatrix,I.matrixWorldInverse),ot.setFromProjectionMatrix(fe),ee=this.localClippingEnabled,$=te.init(this.clippingPlanes,ee),x=he.get(_,p.length),x.init(),p.push(x),H.enabled===!0&&H.isPresenting===!0){let ne=E.xr.getDepthSensingMesh();ne!==null&&wc(ne,I,-1/0,E.sortObjects)}wc(_,I,0,E.sortObjects),x.finish(),E.sortObjects===!0&&x.sort(G,ue),lt=H.enabled===!1||H.isPresenting===!1||H.hasDepthSensing()===!1,lt&&Ae.addToRenderList(x,_),this.info.render.frame++,$===!0&&te.beginShadows();let O=m.state.shadowsArray;pe.render(O,_,I),$===!0&&te.endShadows(),this.info.autoReset===!0&&this.info.reset();let k=x.opaque,R=x.transmissive;if(m.setupLights(),I.isArrayCamera){let ne=I.cameras;if(R.length>0)for(let ae=0,ge=ne.length;ae<ge;ae++){let ye=ne[ae];cp(k,R,_,ye)}lt&&Ae.render(_);for(let ae=0,ge=ne.length;ae<ge;ae++){let ye=ne[ae];lp(x,_,ye,ye.viewport)}}else R.length>0&&cp(k,R,_,I),lt&&Ae.render(_),lp(x,_,I);C!==null&&(b.updateMultisampleRenderTarget(C),b.updateRenderTargetMipmap(C)),_.isScene===!0&&_.onAfterRender(E,_,I),ct.resetDefaultState(),U=-1,ie=null,S.pop(),S.length>0?(m=S[S.length-1],$===!0&&te.setGlobalState(E.clippingPlanes,m.state.camera)):m=null,p.pop(),p.length>0?x=p[p.length-1]:x=null};function wc(_,I,O,k){if(_.visible===!1)return;if(_.layers.test(I.layers)){if(_.isGroup)O=_.renderOrder;else if(_.isLOD)_.autoUpdate===!0&&_.update(I);else if(_.isLight)m.pushLight(_),_.castShadow&&m.pushShadow(_);else if(_.isSprite){if(!_.frustumCulled||ot.intersectsSprite(_)){k&&Se.setFromMatrixPosition(_.matrixWorld).applyMatrix4(fe);let ae=q.update(_),ge=_.material;ge.visible&&x.push(_,ae,ge,O,Se.z,null)}}else if((_.isMesh||_.isLine||_.isPoints)&&(!_.frustumCulled||ot.intersectsObject(_))){let ae=q.update(_),ge=_.material;if(k&&(_.boundingSphere!==void 0?(_.boundingSphere===null&&_.computeBoundingSphere(),Se.copy(_.boundingSphere.center)):(ae.boundingSphere===null&&ae.computeBoundingSphere(),Se.copy(ae.boundingSphere.center)),Se.applyMatrix4(_.matrixWorld).applyMatrix4(fe)),Array.isArray(ge)){let ye=ae.groups;for(let we=0,Ce=ye.length;we<Ce;we++){let Ee=ye[we],at=ge[Ee.materialIndex];at&&at.visible&&x.push(_,ae,at,O,Se.z,Ee)}}else ge.visible&&x.push(_,ae,ge,O,Se.z,null)}}let ne=_.children;for(let ae=0,ge=ne.length;ae<ge;ae++)wc(ne[ae],I,O,k)}function lp(_,I,O,k){let R=_.opaque,ne=_.transmissive,ae=_.transparent;m.setupLightsView(O),$===!0&&te.setGlobalState(E.clippingPlanes,O),k&&Te.viewport(v.copy(k)),R.length>0&&Lo(R,I,O),ne.length>0&&Lo(ne,I,O),ae.length>0&&Lo(ae,I,O),Te.buffers.depth.setTest(!0),Te.buffers.depth.setMask(!0),Te.buffers.color.setMask(!0),Te.setPolygonOffset(!1)}function cp(_,I,O,k){if((O.isScene===!0?O.overrideMaterial:null)!==null)return;m.state.transmissionRenderTarget[k.id]===void 0&&(m.state.transmissionRenderTarget[k.id]=new yi(1,1,{generateMipmaps:!0,type:He.has("EXT_color_buffer_half_float")||He.has("EXT_color_buffer_float")?Ao:vi,minFilter:xr,samples:4,stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:rt.workingColorSpace}));let ne=m.state.transmissionRenderTarget[k.id],ae=k.viewport||v;ne.setSize(ae.z,ae.w);let ge=E.getRenderTarget();E.setRenderTarget(ne),E.getClearColor(z),Y=E.getClearAlpha(),Y<1&&E.setClearColor(16777215,.5),E.clear(),lt&&Ae.render(O);let ye=E.toneMapping;E.toneMapping=Hi;let we=k.viewport;if(k.viewport!==void 0&&(k.viewport=void 0),m.setupLightsView(k),$===!0&&te.setGlobalState(E.clippingPlanes,k),Lo(_,O,k),b.updateMultisampleRenderTarget(ne),b.updateRenderTargetMipmap(ne),He.has("WEBGL_multisampled_render_to_texture")===!1){let Ce=!1;for(let Ee=0,at=I.length;Ee<at;Ee++){let ft=I[Ee],vt=ft.object,an=ft.geometry,et=ft.material,be=ft.group;if(et.side===fi&&vt.layers.test(k.layers)){let Lt=et.side;et.side=sn,et.needsUpdate=!0,up(vt,O,k,an,et,be),et.side=Lt,et.needsUpdate=!0,Ce=!0}}Ce===!0&&(b.updateMultisampleRenderTarget(ne),b.updateRenderTargetMipmap(ne))}E.setRenderTarget(ge),E.setClearColor(z,Y),we!==void 0&&(k.viewport=we),E.toneMapping=ye}function Lo(_,I,O){let k=I.isScene===!0?I.overrideMaterial:null;for(let R=0,ne=_.length;R<ne;R++){let ae=_[R],ge=ae.object,ye=ae.geometry,we=k===null?ae.material:k,Ce=ae.group;ge.layers.test(O.layers)&&up(ge,I,O,ye,we,Ce)}}function up(_,I,O,k,R,ne){_.onBeforeRender(E,I,O,k,R,ne),_.modelViewMatrix.multiplyMatrices(O.matrixWorldInverse,_.matrixWorld),_.normalMatrix.getNormalMatrix(_.modelViewMatrix),R.onBeforeRender(E,I,O,k,_,ne),R.transparent===!0&&R.side===fi&&R.forceSinglePass===!1?(R.side=sn,R.needsUpdate=!0,E.renderBufferDirect(O,I,k,R,_,ne),R.side=zi,R.needsUpdate=!0,E.renderBufferDirect(O,I,k,R,_,ne),R.side=fi):E.renderBufferDirect(O,I,k,R,_,ne),_.onAfterRender(E,I,O,k,R,ne)}function ko(_,I,O){I.isScene!==!0&&(I=We);let k=Re.get(_),R=m.state.lights,ne=m.state.shadowsArray,ae=R.state.version,ge=Me.getParameters(_,R.state,ne,I,O),ye=Me.getProgramCacheKey(ge),we=k.programs;k.environment=_.isMeshStandardMaterial?I.environment:null,k.fog=I.fog,k.envMap=(_.isMeshStandardMaterial?F:y).get(_.envMap||k.environment),k.envMapRotation=k.environment!==null&&_.envMap===null?I.environmentRotation:_.envMapRotation,we===void 0&&(_.addEventListener("dispose",$e),we=new Map,k.programs=we);let Ce=we.get(ye);if(Ce!==void 0){if(k.currentProgram===Ce&&k.lightsStateVersion===ae)return fp(_,ge),Ce}else ge.uniforms=Me.getUniforms(_),_.onBeforeCompile(ge,E),Ce=Me.acquireProgram(ge,ye),we.set(ye,Ce),k.uniforms=ge.uniforms;let Ee=k.uniforms;return(!_.isShaderMaterial&&!_.isRawShaderMaterial||_.clipping===!0)&&(Ee.clippingPlanes=te.uniform),fp(_,ge),k.needsLights=t0(_),k.lightsStateVersion=ae,k.needsLights&&(Ee.ambientLightColor.value=R.state.ambient,Ee.lightProbe.value=R.state.probe,Ee.directionalLights.value=R.state.directional,Ee.directionalLightShadows.value=R.state.directionalShadow,Ee.spotLights.value=R.state.spot,Ee.spotLightShadows.value=R.state.spotShadow,Ee.rectAreaLights.value=R.state.rectArea,Ee.ltc_1.value=R.state.rectAreaLTC1,Ee.ltc_2.value=R.state.rectAreaLTC2,Ee.pointLights.value=R.state.point,Ee.pointLightShadows.value=R.state.pointShadow,Ee.hemisphereLights.value=R.state.hemi,Ee.directionalShadowMap.value=R.state.directionalShadowMap,Ee.directionalShadowMatrix.value=R.state.directionalShadowMatrix,Ee.spotShadowMap.value=R.state.spotShadowMap,Ee.spotLightMatrix.value=R.state.spotLightMatrix,Ee.spotLightMap.value=R.state.spotLightMap,Ee.pointShadowMap.value=R.state.pointShadowMap,Ee.pointShadowMatrix.value=R.state.pointShadowMatrix),k.currentProgram=Ce,k.uniformsList=null,Ce}function dp(_){if(_.uniformsList===null){let I=_.currentProgram.getUniforms();_.uniformsList=As.seqWithValue(I.seq,_.uniforms)}return _.uniformsList}function fp(_,I){let O=Re.get(_);O.outputColorSpace=I.outputColorSpace,O.batching=I.batching,O.batchingColor=I.batchingColor,O.instancing=I.instancing,O.instancingColor=I.instancingColor,O.instancingMorph=I.instancingMorph,O.skinning=I.skinning,O.morphTargets=I.morphTargets,O.morphNormals=I.morphNormals,O.morphColors=I.morphColors,O.morphTargetsCount=I.morphTargetsCount,O.numClippingPlanes=I.numClippingPlanes,O.numIntersection=I.numClipIntersection,O.vertexAlphas=I.vertexAlphas,O.vertexTangents=I.vertexTangents,O.toneMapping=I.toneMapping}function Q_(_,I,O,k,R){I.isScene!==!0&&(I=We),b.resetTextureUnits();let ne=I.fog,ae=k.isMeshStandardMaterial?I.environment:null,ge=C===null?E.outputColorSpace:C.isXRRenderTarget===!0?C.texture.colorSpace:$i,ye=(k.isMeshStandardMaterial?F:y).get(k.envMap||ae),we=k.vertexColors===!0&&!!O.attributes.color&&O.attributes.color.itemSize===4,Ce=!!O.attributes.tangent&&(!!k.normalMap||k.anisotropy>0),Ee=!!O.morphAttributes.position,at=!!O.morphAttributes.normal,ft=!!O.morphAttributes.color,vt=Hi;k.toneMapped&&(C===null||C.isXRRenderTarget===!0)&&(vt=E.toneMapping);let an=O.morphAttributes.position||O.morphAttributes.normal||O.morphAttributes.color,et=an!==void 0?an.length:0,be=Re.get(k),Lt=m.state.lights;if($===!0&&(ee===!0||_!==ie)){let hn=_===ie&&k.id===U;te.setState(k,_,hn)}let tt=!1;k.version===be.__version?(be.needsLights&&be.lightsStateVersion!==Lt.state.version||be.outputColorSpace!==ge||R.isBatchedMesh&&be.batching===!1||!R.isBatchedMesh&&be.batching===!0||R.isBatchedMesh&&be.batchingColor===!0&&R.colorTexture===null||R.isBatchedMesh&&be.batchingColor===!1&&R.colorTexture!==null||R.isInstancedMesh&&be.instancing===!1||!R.isInstancedMesh&&be.instancing===!0||R.isSkinnedMesh&&be.skinning===!1||!R.isSkinnedMesh&&be.skinning===!0||R.isInstancedMesh&&be.instancingColor===!0&&R.instanceColor===null||R.isInstancedMesh&&be.instancingColor===!1&&R.instanceColor!==null||R.isInstancedMesh&&be.instancingMorph===!0&&R.morphTexture===null||R.isInstancedMesh&&be.instancingMorph===!1&&R.morphTexture!==null||be.envMap!==ye||k.fog===!0&&be.fog!==ne||be.numClippingPlanes!==void 0&&(be.numClippingPlanes!==te.numPlanes||be.numIntersection!==te.numIntersection)||be.vertexAlphas!==we||be.vertexTangents!==Ce||be.morphTargets!==Ee||be.morphNormals!==at||be.morphColors!==ft||be.toneMapping!==vt||be.morphTargetsCount!==et)&&(tt=!0):(tt=!0,be.__version=k.version);let En=be.currentProgram;tt===!0&&(En=ko(k,I,R));let Ar=!1,ln=!1,Sc=!1,bt=En.getUniforms(),xi=be.uniforms;if(Te.useProgram(En.program)&&(Ar=!0,ln=!0,Sc=!0),k.id!==U&&(U=k.id,ln=!0),Ar||ie!==_){Xe.reverseDepthBuffer?(ve.copy(_.projectionMatrix),QS(ve),eC(ve),bt.setValue(D,"projectionMatrix",ve)):bt.setValue(D,"projectionMatrix",_.projectionMatrix),bt.setValue(D,"viewMatrix",_.matrixWorldInverse);let hn=bt.map.cameraPosition;hn!==void 0&&hn.setValue(D,Pe.setFromMatrixPosition(_.matrixWorld)),Xe.logarithmicDepthBuffer&&bt.setValue(D,"logDepthBufFC",2/(Math.log(_.far+1)/Math.LN2)),(k.isMeshPhongMaterial||k.isMeshToonMaterial||k.isMeshLambertMaterial||k.isMeshBasicMaterial||k.isMeshStandardMaterial||k.isShaderMaterial)&&bt.setValue(D,"isOrthographic",_.isOrthographicCamera===!0),ie!==_&&(ie=_,ln=!0,Sc=!0)}if(R.isSkinnedMesh){bt.setOptional(D,R,"bindMatrix"),bt.setOptional(D,R,"bindMatrixInverse");let hn=R.skeleton;hn&&(hn.boneTexture===null&&hn.computeBoneTexture(),bt.setValue(D,"boneTexture",hn.boneTexture,b))}R.isBatchedMesh&&(bt.setOptional(D,R,"batchingTexture"),bt.setValue(D,"batchingTexture",R._matricesTexture,b),bt.setOptional(D,R,"batchingIdTexture"),bt.setValue(D,"batchingIdTexture",R._indirectTexture,b),bt.setOptional(D,R,"batchingColorTexture"),R._colorsTexture!==null&&bt.setValue(D,"batchingColorTexture",R._colorsTexture,b));let Cc=O.morphAttributes;if((Cc.position!==void 0||Cc.normal!==void 0||Cc.color!==void 0)&&Ie.update(R,O,En),(ln||be.receiveShadow!==R.receiveShadow)&&(be.receiveShadow=R.receiveShadow,bt.setValue(D,"receiveShadow",R.receiveShadow)),k.isMeshGouraudMaterial&&k.envMap!==null&&(xi.envMap.value=ye,xi.flipEnvMap.value=ye.isCubeTexture&&ye.isRenderTargetTexture===!1?-1:1),k.isMeshStandardMaterial&&k.envMap===null&&I.environment!==null&&(xi.envMapIntensity.value=I.environmentIntensity),ln&&(bt.setValue(D,"toneMappingExposure",E.toneMappingExposure),be.needsLights&&e0(xi,Sc),ne&&k.fog===!0&&se.refreshFogUniforms(xi,ne),se.refreshMaterialUniforms(xi,k,K,W,m.state.transmissionRenderTarget[_.id]),As.upload(D,dp(be),xi,b)),k.isShaderMaterial&&k.uniformsNeedUpdate===!0&&(As.upload(D,dp(be),xi,b),k.uniformsNeedUpdate=!1),k.isSpriteMaterial&&bt.setValue(D,"center",R.center),bt.setValue(D,"modelViewMatrix",R.modelViewMatrix),bt.setValue(D,"normalMatrix",R.normalMatrix),bt.setValue(D,"modelMatrix",R.matrixWorld),k.isShaderMaterial||k.isRawShaderMaterial){let hn=k.uniformsGroups;for(let Dc=0,n0=hn.length;Dc<n0;Dc++){let hp=hn[Dc];A.update(hp,En),A.bind(hp,En)}}return En}function e0(_,I){_.ambientLightColor.needsUpdate=I,_.lightProbe.needsUpdate=I,_.directionalLights.needsUpdate=I,_.directionalLightShadows.needsUpdate=I,_.pointLights.needsUpdate=I,_.pointLightShadows.needsUpdate=I,_.spotLights.needsUpdate=I,_.spotLightShadows.needsUpdate=I,_.rectAreaLights.needsUpdate=I,_.hemisphereLights.needsUpdate=I}function t0(_){return _.isMeshLambertMaterial||_.isMeshToonMaterial||_.isMeshPhongMaterial||_.isMeshStandardMaterial||_.isShadowMaterial||_.isShaderMaterial&&_.lights===!0}this.getActiveCubeFace=function(){return L},this.getActiveMipmapLevel=function(){return T},this.getRenderTarget=function(){return C},this.setRenderTargetTextures=function(_,I,O){Re.get(_.texture).__webglTexture=I,Re.get(_.depthTexture).__webglTexture=O;let k=Re.get(_);k.__hasExternalTextures=!0,k.__autoAllocateDepthBuffer=O===void 0,k.__autoAllocateDepthBuffer||He.has("WEBGL_multisampled_render_to_texture")===!0&&(console.warn("THREE.WebGLRenderer: Render-to-texture extension was disabled because an external texture was provided"),k.__useRenderToTexture=!1)},this.setRenderTargetFramebuffer=function(_,I){let O=Re.get(_);O.__webglFramebuffer=I,O.__useDefaultFramebuffer=I===void 0},this.setRenderTarget=function(_,I=0,O=0){C=_,L=I,T=O;let k=!0,R=null,ne=!1,ae=!1;if(_){let ye=Re.get(_);if(ye.__useDefaultFramebuffer!==void 0)Te.bindFramebuffer(D.FRAMEBUFFER,null),k=!1;else if(ye.__webglFramebuffer===void 0)b.setupRenderTarget(_);else if(ye.__hasExternalTextures)b.rebindTextures(_,Re.get(_.texture).__webglTexture,Re.get(_.depthTexture).__webglTexture);else if(_.depthBuffer){let Ee=_.depthTexture;if(ye.__boundDepthTexture!==Ee){if(Ee!==null&&Re.has(Ee)&&(_.width!==Ee.image.width||_.height!==Ee.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");b.setupDepthRenderbuffer(_)}}let we=_.texture;(we.isData3DTexture||we.isDataArrayTexture||we.isCompressedArrayTexture)&&(ae=!0);let Ce=Re.get(_).__webglFramebuffer;_.isWebGLCubeRenderTarget?(Array.isArray(Ce[I])?R=Ce[I][O]:R=Ce[I],ne=!0):_.samples>0&&b.useMultisampledRTT(_)===!1?R=Re.get(_).__webglMultisampledFramebuffer:Array.isArray(Ce)?R=Ce[O]:R=Ce,v.copy(_.viewport),M.copy(_.scissor),j=_.scissorTest}else v.copy(de).multiplyScalar(K).floor(),M.copy(_e).multiplyScalar(K).floor(),j=Je;if(Te.bindFramebuffer(D.FRAMEBUFFER,R)&&k&&Te.drawBuffers(_,R),Te.viewport(v),Te.scissor(M),Te.setScissorTest(j),ne){let ye=Re.get(_.texture);D.framebufferTexture2D(D.FRAMEBUFFER,D.COLOR_ATTACHMENT0,D.TEXTURE_CUBE_MAP_POSITIVE_X+I,ye.__webglTexture,O)}else if(ae){let ye=Re.get(_.texture),we=I||0;D.framebufferTextureLayer(D.FRAMEBUFFER,D.COLOR_ATTACHMENT0,ye.__webglTexture,O||0,we)}U=-1},this.readRenderTargetPixels=function(_,I,O,k,R,ne,ae){if(!(_&&_.isWebGLRenderTarget)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let ge=Re.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&ae!==void 0&&(ge=ge[ae]),ge){Te.bindFramebuffer(D.FRAMEBUFFER,ge);try{let ye=_.texture,we=ye.format,Ce=ye.type;if(!Xe.textureFormatReadable(we)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!Xe.textureTypeReadable(Ce)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}I>=0&&I<=_.width-k&&O>=0&&O<=_.height-R&&D.readPixels(I,O,k,R,Fe.convert(we),Fe.convert(Ce),ne)}finally{let ye=C!==null?Re.get(C).__webglFramebuffer:null;Te.bindFramebuffer(D.FRAMEBUFFER,ye)}}},this.readRenderTargetPixelsAsync=function(_,I,O,k,R,ne,ae){return Ir(this,null,function*(){if(!(_&&_.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let ge=Re.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&ae!==void 0&&(ge=ge[ae]),ge){let ye=_.texture,we=ye.format,Ce=ye.type;if(!Xe.textureFormatReadable(we))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!Xe.textureTypeReadable(Ce))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");if(I>=0&&I<=_.width-k&&O>=0&&O<=_.height-R){Te.bindFramebuffer(D.FRAMEBUFFER,ge);let Ee=D.createBuffer();D.bindBuffer(D.PIXEL_PACK_BUFFER,Ee),D.bufferData(D.PIXEL_PACK_BUFFER,ne.byteLength,D.STREAM_READ),D.readPixels(I,O,k,R,Fe.convert(we),Fe.convert(Ce),0);let at=C!==null?Re.get(C).__webglFramebuffer:null;Te.bindFramebuffer(D.FRAMEBUFFER,at);let ft=D.fenceSync(D.SYNC_GPU_COMMANDS_COMPLETE,0);return D.flush(),yield JS(D,ft,4),D.bindBuffer(D.PIXEL_PACK_BUFFER,Ee),D.getBufferSubData(D.PIXEL_PACK_BUFFER,0,ne),D.deleteBuffer(Ee),D.deleteSync(ft),ne}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")}})},this.copyFramebufferToTexture=function(_,I=null,O=0){_.isTexture!==!0&&(Vl("WebGLRenderer: copyFramebufferToTexture function signature has changed."),I=arguments[0]||null,_=arguments[1]);let k=Math.pow(2,-O),R=Math.floor(_.image.width*k),ne=Math.floor(_.image.height*k),ae=I!==null?I.x:0,ge=I!==null?I.y:0;b.setTexture2D(_,0),D.copyTexSubImage2D(D.TEXTURE_2D,O,0,0,ae,ge,R,ne),Te.unbindTexture()},this.copyTextureToTexture=function(_,I,O=null,k=null,R=0){_.isTexture!==!0&&(Vl("WebGLRenderer: copyTextureToTexture function signature has changed."),k=arguments[0]||null,_=arguments[1],I=arguments[2],R=arguments[3]||0,O=null);let ne,ae,ge,ye,we,Ce;O!==null?(ne=O.max.x-O.min.x,ae=O.max.y-O.min.y,ge=O.min.x,ye=O.min.y):(ne=_.image.width,ae=_.image.height,ge=0,ye=0),k!==null?(we=k.x,Ce=k.y):(we=0,Ce=0);let Ee=Fe.convert(I.format),at=Fe.convert(I.type);b.setTexture2D(I,0),D.pixelStorei(D.UNPACK_FLIP_Y_WEBGL,I.flipY),D.pixelStorei(D.UNPACK_PREMULTIPLY_ALPHA_WEBGL,I.premultiplyAlpha),D.pixelStorei(D.UNPACK_ALIGNMENT,I.unpackAlignment);let ft=D.getParameter(D.UNPACK_ROW_LENGTH),vt=D.getParameter(D.UNPACK_IMAGE_HEIGHT),an=D.getParameter(D.UNPACK_SKIP_PIXELS),et=D.getParameter(D.UNPACK_SKIP_ROWS),be=D.getParameter(D.UNPACK_SKIP_IMAGES),Lt=_.isCompressedTexture?_.mipmaps[R]:_.image;D.pixelStorei(D.UNPACK_ROW_LENGTH,Lt.width),D.pixelStorei(D.UNPACK_IMAGE_HEIGHT,Lt.height),D.pixelStorei(D.UNPACK_SKIP_PIXELS,ge),D.pixelStorei(D.UNPACK_SKIP_ROWS,ye),_.isDataTexture?D.texSubImage2D(D.TEXTURE_2D,R,we,Ce,ne,ae,Ee,at,Lt.data):_.isCompressedTexture?D.compressedTexSubImage2D(D.TEXTURE_2D,R,we,Ce,Lt.width,Lt.height,Ee,Lt.data):D.texSubImage2D(D.TEXTURE_2D,R,we,Ce,ne,ae,Ee,at,Lt),D.pixelStorei(D.UNPACK_ROW_LENGTH,ft),D.pixelStorei(D.UNPACK_IMAGE_HEIGHT,vt),D.pixelStorei(D.UNPACK_SKIP_PIXELS,an),D.pixelStorei(D.UNPACK_SKIP_ROWS,et),D.pixelStorei(D.UNPACK_SKIP_IMAGES,be),R===0&&I.generateMipmaps&&D.generateMipmap(D.TEXTURE_2D),Te.unbindTexture()},this.copyTextureToTexture3D=function(_,I,O=null,k=null,R=0){_.isTexture!==!0&&(Vl("WebGLRenderer: copyTextureToTexture3D function signature has changed."),O=arguments[0]||null,k=arguments[1]||null,_=arguments[2],I=arguments[3],R=arguments[4]||0);let ne,ae,ge,ye,we,Ce,Ee,at,ft,vt=_.isCompressedTexture?_.mipmaps[R]:_.image;O!==null?(ne=O.max.x-O.min.x,ae=O.max.y-O.min.y,ge=O.max.z-O.min.z,ye=O.min.x,we=O.min.y,Ce=O.min.z):(ne=vt.width,ae=vt.height,ge=vt.depth,ye=0,we=0,Ce=0),k!==null?(Ee=k.x,at=k.y,ft=k.z):(Ee=0,at=0,ft=0);let an=Fe.convert(I.format),et=Fe.convert(I.type),be;if(I.isData3DTexture)b.setTexture3D(I,0),be=D.TEXTURE_3D;else if(I.isDataArrayTexture||I.isCompressedArrayTexture)b.setTexture2DArray(I,0),be=D.TEXTURE_2D_ARRAY;else{console.warn("THREE.WebGLRenderer.copyTextureToTexture3D: only supports THREE.DataTexture3D and THREE.DataTexture2DArray.");return}D.pixelStorei(D.UNPACK_FLIP_Y_WEBGL,I.flipY),D.pixelStorei(D.UNPACK_PREMULTIPLY_ALPHA_WEBGL,I.premultiplyAlpha),D.pixelStorei(D.UNPACK_ALIGNMENT,I.unpackAlignment);let Lt=D.getParameter(D.UNPACK_ROW_LENGTH),tt=D.getParameter(D.UNPACK_IMAGE_HEIGHT),En=D.getParameter(D.UNPACK_SKIP_PIXELS),Ar=D.getParameter(D.UNPACK_SKIP_ROWS),ln=D.getParameter(D.UNPACK_SKIP_IMAGES);D.pixelStorei(D.UNPACK_ROW_LENGTH,vt.width),D.pixelStorei(D.UNPACK_IMAGE_HEIGHT,vt.height),D.pixelStorei(D.UNPACK_SKIP_PIXELS,ye),D.pixelStorei(D.UNPACK_SKIP_ROWS,we),D.pixelStorei(D.UNPACK_SKIP_IMAGES,Ce),_.isDataTexture||_.isData3DTexture?D.texSubImage3D(be,R,Ee,at,ft,ne,ae,ge,an,et,vt.data):I.isCompressedArrayTexture?D.compressedTexSubImage3D(be,R,Ee,at,ft,ne,ae,ge,an,vt.data):D.texSubImage3D(be,R,Ee,at,ft,ne,ae,ge,an,et,vt),D.pixelStorei(D.UNPACK_ROW_LENGTH,Lt),D.pixelStorei(D.UNPACK_IMAGE_HEIGHT,tt),D.pixelStorei(D.UNPACK_SKIP_PIXELS,En),D.pixelStorei(D.UNPACK_SKIP_ROWS,Ar),D.pixelStorei(D.UNPACK_SKIP_IMAGES,ln),R===0&&I.generateMipmaps&&D.generateMipmap(be),Te.unbindTexture()},this.initRenderTarget=function(_){Re.get(_).__webglFramebuffer===void 0&&b.setupRenderTarget(_)},this.initTexture=function(_){_.isCubeTexture?b.setTextureCube(_,0):_.isData3DTexture?b.setTexture3D(_,0):_.isDataArrayTexture||_.isCompressedArrayTexture?b.setTexture2DArray(_,0):b.setTexture2D(_,0),Te.unbindTexture()},this.resetState=function(){L=0,T=0,C=null,Te.reset(),ct.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return pi}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=e===Yh?"display-p3":"srgb",t.unpackColorSpace=rt.workingColorSpace===dc?"display-p3":"srgb"}};var sc=class extends _i{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new br,this.environmentIntensity=1,this.environmentRotation=new br,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}};var To=class extends wr{constructor(e){super(),this.isMeshStandardMaterial=!0,this.defines={STANDARD:""},this.type="MeshStandardMaterial",this.color=new qe(16777215),this.roughness=1,this.metalness=0,this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.emissive=new qe(0),this.emissiveIntensity=1,this.emissiveMap=null,this.bumpMap=null,this.bumpScale=1,this.normalMap=null,this.normalMapType=M_,this.normalScale=new st(1,1),this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.roughnessMap=null,this.metalnessMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new br,this.envMapIntensity=1,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.flatShading=!1,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.defines={STANDARD:""},this.color.copy(e.color),this.roughness=e.roughness,this.metalness=e.metalness,this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.emissive.copy(e.emissive),this.emissiveMap=e.emissiveMap,this.emissiveIntensity=e.emissiveIntensity,this.bumpMap=e.bumpMap,this.bumpScale=e.bumpScale,this.normalMap=e.normalMap,this.normalMapType=e.normalMapType,this.normalScale.copy(e.normalScale),this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.roughnessMap=e.roughnessMap,this.metalnessMap=e.metalnessMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.envMapIntensity=e.envMapIntensity,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.flatShading=e.flatShading,this.fog=e.fog,this}};function Pl(n,e,t){return!n||!t&&n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}function YI(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}var Ls=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let l=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===l)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},Nh=class extends Ls{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:my,endingEnd:my}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],l=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case gy:s=e,a=2*t-i;break;case vy:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(l===void 0)switch(this.getSettings_().endingEnd){case gy:o=e,l=2*i-t;break;case vy:o=1,l=i+r[1]-r[0];break;default:o=e-1,l=t}let c=(i-t)*.5,u=this.valueSize;this._weightPrev=c/(t-a),this._weightNext=c/(l-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,l=e*a,c=l-a,u=this._offsetPrev,d=this._offsetNext,f=this._weightPrev,h=this._weightNext,g=(i-t)/(r-t),x=g*g,m=x*g,p=-f*m+2*f*x-f*g,S=(1+f)*m+(-1.5-2*f)*x+(-.5+f)*g+1,E=(-1-h)*m+(1.5+h)*x+.5*g,w=h*m-h*x;for(let L=0;L!==a;++L)s[L]=p*o[u+L]+S*o[c+L]+E*o[l+L]+w*o[d+L];return s}},Ph=class extends Ls{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,l=e*a,c=l-a,u=(i-t)/(r-t),d=1-u;for(let f=0;f!==a;++f)s[f]=o[c+f]*d+o[l+f]*u;return s}},Fh=class extends Ls{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},On=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=Pl(t,this.TimeBufferType),this.values=Pl(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:Pl(e.times,Array),values:Pl(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new Fh(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new Ph(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new Nh(this.times,this.values,this.getValueSize(),e)}setInterpolation(e){let t;switch(e){case Bl:t=this.InterpolantFactoryMethodDiscrete;break;case hh:t=this.InterpolantFactoryMethodLinear;break;case Zd:t=this.InterpolantFactoryMethodSmooth;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return console.warn("THREE.KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return Bl;case this.InterpolantFactoryMethodLinear:return hh;case this.InterpolantFactoryMethodSmooth:return Zd}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(console.error("THREE.KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(console.error("THREE.KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let l=i[a];if(typeof l=="number"&&isNaN(l)){console.error("THREE.KeyframeTrack: Time is not a valid number.",this,a,l),e=!1;break}if(o!==null&&o>l){console.error("THREE.KeyframeTrack: Out of order keys.",this,a,l,o),e=!1;break}o=l}if(r!==void 0&&YI(r))for(let a=0,l=r.length;a!==l;++a){let c=r[a];if(isNaN(c)){console.error("THREE.KeyframeTrack: Value is not a valid number.",this,a,c),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===Zd,s=e.length-1,o=1;for(let a=1;a<s;++a){let l=!1,c=e[a],u=e[a+1];if(c!==u&&(a!==1||c!==e[0]))if(r)l=!0;else{let d=a*i,f=d-i,h=d+i;for(let g=0;g!==i;++g){let x=t[d+g];if(x!==t[f+g]||x!==t[h+g]){l=!0;break}}}if(l){if(a!==o){e[o]=e[a];let d=a*i,f=o*i;for(let h=0;h!==i;++h)t[f+h]=t[d+h]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,l=o*i,c=0;c!==i;++c)t[l+c]=t[a+c];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};On.prototype.TimeBufferType=Float32Array;On.prototype.ValueBufferType=Float32Array;On.prototype.DefaultInterpolation=hh;var Cr=class extends On{constructor(e,t,i){super(e,t,i)}};Cr.prototype.ValueTypeName="bool";Cr.prototype.ValueBufferType=Array;Cr.prototype.DefaultInterpolation=Bl;Cr.prototype.InterpolantFactoryMethodLinear=void 0;Cr.prototype.InterpolantFactoryMethodSmooth=void 0;var Oh=class extends On{};Oh.prototype.ValueTypeName="color";var Lh=class extends On{};Lh.prototype.ValueTypeName="number";var kh=class extends Ls{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,l=(i-t)/(r-t),c=e*a;for(let u=c+a;c!==u;c+=4)Wi.slerpFlat(s,0,o,c-a,o,c,l);return s}},oc=class extends On{InterpolantFactoryMethodLinear(e){return new kh(this.times,this.values,this.getValueSize(),e)}};oc.prototype.ValueTypeName="quaternion";oc.prototype.InterpolantFactoryMethodSmooth=void 0;var Dr=class extends On{constructor(e,t,i){super(e,t,i)}};Dr.prototype.ValueTypeName="string";Dr.prototype.ValueBufferType=Array;Dr.prototype.DefaultInterpolation=Bl;Dr.prototype.InterpolantFactoryMethodLinear=void 0;Dr.prototype.InterpolantFactoryMethodSmooth=void 0;var Uh=class extends On{};Uh.prototype.ValueTypeName="vector";var ac=class extends _i{constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new qe(e),this.intensity=t}dispose(){}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,this.groundColor!==void 0&&(t.object.groundColor=this.groundColor.getHex()),this.distance!==void 0&&(t.object.distance=this.distance),this.angle!==void 0&&(t.object.angle=this.angle),this.decay!==void 0&&(t.object.decay=this.decay),this.penumbra!==void 0&&(t.object.penumbra=this.penumbra),this.shadow!==void 0&&(t.object.shadow=this.shadow.toJSON()),this.target!==void 0&&(t.object.target=this.target.uuid),t}};var Df=new Dt,a_=new V,l_=new V,Vh=class{constructor(e){this.camera=e,this.intensity=1,this.bias=0,this.normalBias=0,this.radius=1,this.blurSamples=8,this.mapSize=new st(512,512),this.map=null,this.mapPass=null,this.matrix=new Dt,this.autoUpdate=!0,this.needsUpdate=!1,this._frustum=new Do,this._frameExtents=new st(1,1),this._viewportCount=1,this._viewports=[new Et(0,0,1,1)]}getViewportCount(){return this._viewportCount}getFrustum(){return this._frustum}updateMatrices(e){let t=this.camera,i=this.matrix;a_.setFromMatrixPosition(e.matrixWorld),t.position.copy(a_),l_.setFromMatrixPosition(e.target.matrixWorld),t.lookAt(l_),t.updateMatrixWorld(),Df.multiplyMatrices(t.projectionMatrix,t.matrixWorldInverse),this._frustum.setFromProjectionMatrix(Df),i.set(.5,0,0,.5,0,.5,0,.5,0,0,.5,.5,0,0,0,1),i.multiply(Df)}getViewport(e){return this._viewports[e]}getFrameExtents(){return this._frameExtents}dispose(){this.map&&this.map.dispose(),this.mapPass&&this.mapPass.dispose()}copy(e){return this.camera=e.camera.clone(),this.intensity=e.intensity,this.bias=e.bias,this.radius=e.radius,this.mapSize.copy(e.mapSize),this}clone(){return new this.constructor().copy(this)}toJSON(){let e={};return this.intensity!==1&&(e.intensity=this.intensity),this.bias!==0&&(e.bias=this.bias),this.normalBias!==0&&(e.normalBias=this.normalBias),this.radius!==1&&(e.radius=this.radius),(this.mapSize.x!==512||this.mapSize.y!==512)&&(e.mapSize=this.mapSize.toArray()),e.camera=this.camera.toJSON(!1).object,delete e.camera.matrix,e}};var Bh=class extends Vh{constructor(){super(new tc(-5,5,5,-5,.5,500)),this.isDirectionalLightShadow=!0}},lc=class extends ac{constructor(e,t){super(e,t),this.isDirectionalLight=!0,this.type="DirectionalLight",this.position.copy(_i.DEFAULT_UP),this.updateMatrix(),this.target=new _i,this.shadow=new Bh}dispose(){this.shadow.dispose()}copy(e){return super.copy(e),this.target=e.target.clone(),this.shadow=e.shadow.clone(),this}},cc=class extends ac{constructor(e,t){super(e,t),this.isAmbientLight=!0,this.type="AmbientLight"}};var Kh="\\[\\]\\.:\\/",ZI=new RegExp("["+Kh+"]","g"),Jh="[^"+Kh+"]",KI="[^"+Kh.replace("\\.","")+"]",JI=/((?:WC+[\/:])*)/.source.replace("WC",Jh),QI=/(WCOD+)?/.source.replace("WCOD",KI),e1=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",Jh),t1=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",Jh),n1=new RegExp("^"+JI+QI+e1+t1+"$"),i1=["material","materials","bones","map"],Hh=class{constructor(e,t,i){let r=i||Ct.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},Ct=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(ZI,"")}static parseTrackName(t){let i=n1.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);i1.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let l=o[a];if(l.name===i||l.uuid===i)return l;let c=r(l.children);if(c)return c}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){console.warn("THREE.PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){console.error("THREE.PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){console.error("THREE.PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){console.error("THREE.PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let d=0;d<t.length;d++)if(t[d].name===u){u=d;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){console.error("THREE.PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){console.error("THREE.PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){console.error("THREE.PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){console.error("THREE.PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;console.error("THREE.PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let l=this.Versioning.None;this.targetObject=t,t.needsUpdate!==void 0?l=this.Versioning.NeedsUpdate:t.matrixWorldNeedsUpdate!==void 0&&(l=this.Versioning.MatrixWorldNeedsUpdate);let c=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){console.error("THREE.PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){console.error("THREE.PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}c=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(c=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(c=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[c],this.setValue=this.SetterByBindingTypeAndVersioning[c][l]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=Hh,n})();Ct.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};Ct.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};Ct.prototype.GetterByBindingType=[Ct.prototype._getValue_direct,Ct.prototype._getValue_array,Ct.prototype._getValue_arrayElement,Ct.prototype._getValue_toArray];Ct.prototype.SetterByBindingTypeAndVersioning=[[Ct.prototype._setValue_direct,Ct.prototype._setValue_direct_setNeedsUpdate,Ct.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[Ct.prototype._setValue_array,Ct.prototype._setValue_array_setNeedsUpdate,Ct.prototype._setValue_array_setMatrixWorldNeedsUpdate],[Ct.prototype._setValue_arrayElement,Ct.prototype._setValue_arrayElement_setNeedsUpdate,Ct.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[Ct.prototype._setValue_fromArray,Ct.prototype._setValue_fromArray_setNeedsUpdate,Ct.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var sO=new Float32Array(1);typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:zh}}));typeof window<"u"&&(window.__THREE__?console.warn("WARNING: Multiple instances of Three.js being imported."):window.__THREE__=zh);var s1=["c"],hc=class n{constructor(e){this.zone=e;this.stop=()=>{}}ngAfterViewInit(){this.zone.runOutsideAngular(()=>this.init())}ngOnDestroy(){this.stop()}init(){let e=this.c.nativeElement,t=new rc({canvas:e,alpha:!0,antialias:!0});t.setPixelRatio(Math.min(devicePixelRatio,2));let i=new sc,r=new Yt(45,1,.1,100);r.position.z=14,i.add(new cc(16777215,1.2));let s=new lc(16777215,2.4);s.position.set(5,8,6),i.add(s);let o=new mi;i.add(o);let a=[16742938,16765601,2241643].map(E=>new To({color:E,roughness:.45,metalness:.1})),l=new ji(1,1,1),c=new ji(1.02,.12,1.02),u=new To({color:16115400,roughness:.6}),d=[];for(let E=0;E<18;E++){let w=new mi;w.add(new rn(l,a[E%3]),new rn(c,u)),w.scale.setScalar(.6+Math.random()*1.1),w.position.set((Math.random()-.5)*18,(Math.random()-.5)*9,(Math.random()-.5)*6-1),w.rotation.set(Math.random()*3,Math.random()*3,0),o.add(w),d.push({m:w,v:.1+Math.random()*.3,p:Math.random()*6})}let f={x:0,y:0},h=E=>{f.x=E.clientX/innerWidth-.5,f.y=E.clientY/innerHeight-.5},g=()=>{let E=e.parentElement;t.setSize(E.clientWidth,E.clientHeight,!1),r.aspect=E.clientWidth/E.clientHeight,r.updateProjectionMatrix()};addEventListener("mousemove",h),addEventListener("resize",g),g();let x=matchMedia("(prefers-reduced-motion: reduce)").matches,m=0,p=performance.now(),S=()=>{let E=(performance.now()-p)/1e3;d.forEach(w=>{w.m.rotation.x+=w.v*.01,w.m.rotation.y+=w.v*.012,w.m.position.y+=Math.sin(E+w.p)*.003}),o.rotation.y=scrollY*.0012,o.position.y=scrollY*.004,r.position.x+=(f.x*3-r.position.x)*.05,r.position.y+=(-f.y*2-r.position.y)*.05,r.lookAt(0,0,0),t.render(i,r),x||(m=requestAnimationFrame(S))};S(),this.stop=()=>{cancelAnimationFrame(m),removeEventListener("mousemove",h),removeEventListener("resize",g),t.dispose(),l.dispose()}}static{this.\u0275fac=function(t){return new(t||n)(gt(_t))}}static{this.\u0275cmp=ts({type:n,selectors:[["app-hero-3d"]],viewQuery:function(t,i){if(t&1&&zv(s1,7),t&2){let r;Gv(r=Wv())&&(i.c=r.first)}},standalone:!0,features:[ls],decls:2,vars:0,consts:[["c",""]],template:function(t,i){t&1&&yn(0,"canvas",null,0)},styles:["[_nghost-%COMP%]{position:absolute;inset:0;display:block}canvas[_ngcontent-%COMP%]{width:100%;height:100%;display:block}"]})}};var V_=(()=>{class n{constructor(t,i){this._renderer=t,this._elementRef=i,this.onChange=r=>{},this.onTouched=()=>{}}setProperty(t,i){this._renderer.setProperty(this._elementRef.nativeElement,t,i)}registerOnTouched(t){this.onTouched=t}registerOnChange(t){this.onChange=t}setDisabledState(t){this.setProperty("disabled",t)}static{this.\u0275fac=function(i){return new(i||n)(gt(ol),gt(vn))}}static{this.\u0275dir=Wn({type:n})}}return n})(),o1=(()=>{class n extends V_{static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=Ug(n)))(r||n)}})()}static{this.\u0275dir=Wn({type:n,features:[os]})}}return n})(),B_=new Be("");var a1={provide:B_,useExisting:oo(()=>yc),multi:!0};function l1(){let n=us()?us().getUserAgent():"";return/android (\d+)/.test(n.toLowerCase())}var c1=new Be(""),yc=(()=>{class n extends V_{constructor(t,i,r){super(t,i),this._compositionMode=r,this._composing=!1,this._compositionMode==null&&(this._compositionMode=!l1())}writeValue(t){let i=t??"";this.setProperty("value",i)}_handleInput(t){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(t)}_compositionStart(){this._composing=!0}_compositionEnd(t){this._composing=!1,this._compositionMode&&this.onChange(t)}static{this.\u0275fac=function(i){return new(i||n)(gt(ol),gt(vn),gt(c1,8))}}static{this.\u0275dir=Wn({type:n,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&jt("input",function(o){return r._handleInput(o.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(o){return r._compositionEnd(o.target.value)})},features:[kd([a1]),os]})}}return n})();var u1=new Be(""),d1=new Be("");function H_(n){return n!=null}function z_(n){return fo(n)?zc(n):n}function G_(n){let e={};return n.forEach(t=>{e=t!=null?ht(ht({},e),t):e}),Object.keys(e).length===0?null:e}function W_(n,e){return e.map(t=>t(n))}function f1(n){return!n.validate}function j_(n){return n.map(e=>f1(e)?e:t=>e.validate(t))}function h1(n){if(!n)return null;let e=n.filter(H_);return e.length==0?null:function(t){return G_(W_(t,e))}}function $_(n){return n!=null?h1(j_(n)):null}function p1(n){if(!n)return null;let e=n.filter(H_);return e.length==0?null:function(t){let i=W_(t,e).map(z_);return Gc(i).pipe(Ki(G_))}}function q_(n){return n!=null?p1(j_(n)):null}function N_(n,e){return n===null?[e]:Array.isArray(n)?[...n,e]:[n,e]}function m1(n){return n._rawValidators}function g1(n){return n._rawAsyncValidators}function Qh(n){return n?Array.isArray(n)?n:[n]:[]}function mc(n,e){return Array.isArray(n)?n.includes(e):n===e}function P_(n,e){let t=Qh(e);return Qh(n).forEach(r=>{mc(t,r)||t.push(r)}),t}function F_(n,e){return Qh(e).filter(t=>!mc(n,t))}var gc=class{constructor(){this._rawValidators=[],this._rawAsyncValidators=[],this._onDestroyCallbacks=[]}get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_setValidators(e){this._rawValidators=e||[],this._composedValidatorFn=$_(this._rawValidators)}_setAsyncValidators(e){this._rawAsyncValidators=e||[],this._composedAsyncValidatorFn=q_(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_registerOnDestroy(e){this._onDestroyCallbacks.push(e)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(e=>e()),this._onDestroyCallbacks=[]}reset(e=void 0){this.control&&this.control.reset(e)}hasError(e,t){return this.control?this.control.hasError(e,t):!1}getError(e,t){return this.control?this.control.getError(e,t):null}},ep=class extends gc{get formDirective(){return null}get path(){return null}},Oo=class extends gc{constructor(){super(...arguments),this._parent=null,this.name=null,this.valueAccessor=null}},tp=class{constructor(e){this._cd=e}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}},v1={"[class.ng-untouched]":"isUntouched","[class.ng-touched]":"isTouched","[class.ng-pristine]":"isPristine","[class.ng-dirty]":"isDirty","[class.ng-valid]":"isValid","[class.ng-invalid]":"isInvalid","[class.ng-pending]":"isPending"},wO=yt(ht({},v1),{"[class.ng-submitted]":"isSubmitted"}),X_=(()=>{class n extends tp{constructor(t){super(t)}static{this.\u0275fac=function(i){return new(i||n)(gt(Oo,2))}}static{this.\u0275dir=Wn({type:n,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&Ii("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},features:[os]})}}return n})();var Ro="VALID",pc="INVALID",Us="PENDING",No="DISABLED",Bs=class{},vc=class extends Bs{constructor(e,t){super(),this.value=e,this.source=t}},Po=class extends Bs{constructor(e,t){super(),this.pristine=e,this.source=t}},Fo=class extends Bs{constructor(e,t){super(),this.touched=e,this.source=t}},Vs=class extends Bs{constructor(e,t){super(),this.status=e,this.source=t}};function y1(n){return(_c(n)?n.validators:n)||null}function _1(n){return Array.isArray(n)?$_(n):n||null}function x1(n,e){return(_c(e)?e.asyncValidators:n)||null}function M1(n){return Array.isArray(n)?q_(n):n||null}function _c(n){return n!=null&&!Array.isArray(n)&&typeof n=="object"}var np=class{constructor(e,t){this._pendingDirty=!1,this._hasOwnPendingAsyncValidator=null,this._pendingTouched=!1,this._onCollectionChange=()=>{},this._parent=null,this._status=al(()=>this.statusReactive()),this.statusReactive=cn(void 0),this._pristine=al(()=>this.pristineReactive()),this.pristineReactive=cn(!0),this._touched=al(()=>this.touchedReactive()),this.touchedReactive=cn(!1),this._events=new Ln,this.events=this._events.asObservable(),this._onDisabledChange=[],this._assignValidators(e),this._assignAsyncValidators(t)}get validator(){return this._composedValidatorFn}set validator(e){this._rawValidators=this._composedValidatorFn=e}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(e){this._rawAsyncValidators=this._composedAsyncValidatorFn=e}get parent(){return this._parent}get status(){return Ri(this.statusReactive)}set status(e){Ri(()=>this.statusReactive.set(e))}get valid(){return this.status===Ro}get invalid(){return this.status===pc}get pending(){return this.status==Us}get disabled(){return this.status===No}get enabled(){return this.status!==No}get pristine(){return Ri(this.pristineReactive)}set pristine(e){Ri(()=>this.pristineReactive.set(e))}get dirty(){return!this.pristine}get touched(){return Ri(this.touchedReactive)}set touched(e){Ri(()=>this.touchedReactive.set(e))}get untouched(){return!this.touched}get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(e){this._assignValidators(e)}setAsyncValidators(e){this._assignAsyncValidators(e)}addValidators(e){this.setValidators(P_(e,this._rawValidators))}addAsyncValidators(e){this.setAsyncValidators(P_(e,this._rawAsyncValidators))}removeValidators(e){this.setValidators(F_(e,this._rawValidators))}removeAsyncValidators(e){this.setAsyncValidators(F_(e,this._rawAsyncValidators))}hasValidator(e){return mc(this._rawValidators,e)}hasAsyncValidator(e){return mc(this._rawAsyncValidators,e)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(e={}){let t=this.touched===!1;this.touched=!0;let i=e.sourceControl??this;this._parent&&!e.onlySelf&&this._parent.markAsTouched(yt(ht({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new Fo(!0,i))}markAllAsTouched(e={}){this.markAsTouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsTouched(e))}markAsUntouched(e={}){let t=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:i})}),this._parent&&!e.onlySelf&&this._parent._updateTouched(e,i),t&&e.emitEvent!==!1&&this._events.next(new Fo(!1,i))}markAsDirty(e={}){let t=this.pristine===!0;this.pristine=!1;let i=e.sourceControl??this;this._parent&&!e.onlySelf&&this._parent.markAsDirty(yt(ht({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new Po(!1,i))}markAsPristine(e={}){let t=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:e.emitEvent})}),this._parent&&!e.onlySelf&&this._parent._updatePristine(e,i),t&&e.emitEvent!==!1&&this._events.next(new Po(!0,i))}markAsPending(e={}){this.status=Us;let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new Vs(this.status,t)),this.statusChanges.emit(this.status)),this._parent&&!e.onlySelf&&this._parent.markAsPending(yt(ht({},e),{sourceControl:t}))}disable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=No,this.errors=null,this._forEachChild(r=>{r.disable(yt(ht({},e),{onlySelf:!0}))}),this._updateValue();let i=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new vc(this.value,i)),this._events.next(new Vs(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(yt(ht({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=Ro,this._forEachChild(i=>{i.enable(yt(ht({},e),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent}),this._updateAncestors(yt(ht({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(e,t){this._parent&&!e.onlySelf&&(this._parent.updateValueAndValidity(e),e.skipPristineCheck||this._parent._updatePristine({},t),this._parent._updateTouched({},t))}setParent(e){this._parent=e}getRawValue(){return this.value}updateValueAndValidity(e={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Ro||this.status===Us)&&this._runAsyncValidator(i,e.emitEvent)}let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new vc(this.value,t)),this._events.next(new Vs(this.status,t)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._parent&&!e.onlySelf&&this._parent.updateValueAndValidity(yt(ht({},e),{sourceControl:t}))}_updateTreeValidity(e={emitEvent:!0}){this._forEachChild(t=>t._updateTreeValidity(e)),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?No:Ro}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(e,t){if(this.asyncValidator){this.status=Us,this._hasOwnPendingAsyncValidator={emitEvent:t!==!1};let i=z_(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:t,shouldHaveEmitted:e})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let e=this._hasOwnPendingAsyncValidator?.emitEvent??!1;return this._hasOwnPendingAsyncValidator=null,e}return!1}setErrors(e,t={}){this.errors=e,this._updateControlsErrors(t.emitEvent!==!1,this,t.shouldHaveEmitted)}get(e){let t=e;return t==null||(Array.isArray(t)||(t=t.split(".")),t.length===0)?null:t.reduce((i,r)=>i&&i._find(r),this)}getError(e,t){let i=t?this.get(t):this;return i&&i.errors?i.errors[e]:null}hasError(e,t){return!!this.getError(e,t)}get root(){let e=this;for(;e._parent;)e=e._parent;return e}_updateControlsErrors(e,t,i){this.status=this._calculateStatus(),e&&this.statusChanges.emit(this.status),(e||i)&&this._events.next(new Vs(this.status,t)),this._parent&&this._parent._updateControlsErrors(e,t,i)}_initObservables(){this.valueChanges=new Jt,this.statusChanges=new Jt}_calculateStatus(){return this._allControlsDisabled()?No:this.errors?pc:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Us)?Us:this._anyControlsHaveStatus(pc)?pc:Ro}_anyControlsHaveStatus(e){return this._anyControls(t=>t.status===e)}_anyControlsDirty(){return this._anyControls(e=>e.dirty)}_anyControlsTouched(){return this._anyControls(e=>e.touched)}_updatePristine(e,t){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,this._parent&&!e.onlySelf&&this._parent._updatePristine(e,t),r&&this._events.next(new Po(this.pristine,t))}_updateTouched(e={},t){this.touched=this._anyControlsTouched(),this._events.next(new Fo(this.touched,t)),this._parent&&!e.onlySelf&&this._parent._updateTouched(e,t)}_registerOnCollectionChange(e){this._onCollectionChange=e}_setUpdateStrategy(e){_c(e)&&e.updateOn!=null&&(this._updateOn=e.updateOn)}_parentMarkedDirty(e){let t=this._parent&&this._parent.dirty;return!e&&!!t&&!this._parent._anyControlsDirty()}_find(e){return null}_assignValidators(e){this._rawValidators=Array.isArray(e)?e.slice():e,this._composedValidatorFn=_1(this._rawValidators)}_assignAsyncValidators(e){this._rawAsyncValidators=Array.isArray(e)?e.slice():e,this._composedAsyncValidatorFn=M1(this._rawAsyncValidators)}};var Y_=new Be("CallSetDisabledState",{providedIn:"root",factory:()=>ip}),ip="always";function E1(n,e){return[...e.path,n]}function b1(n,e,t=ip){S1(n,e),e.valueAccessor.writeValue(n.value),(n.disabled||t==="always")&&e.valueAccessor.setDisabledState?.(n.disabled),C1(n,e),T1(n,e),D1(n,e),w1(n,e)}function O_(n,e){n.forEach(t=>{t.registerOnValidatorChange&&t.registerOnValidatorChange(e)})}function w1(n,e){if(e.valueAccessor.setDisabledState){let t=i=>{e.valueAccessor.setDisabledState(i)};n.registerOnDisabledChange(t),e._registerOnDestroy(()=>{n._unregisterOnDisabledChange(t)})}}function S1(n,e){let t=m1(n);e.validator!==null?n.setValidators(N_(t,e.validator)):typeof t=="function"&&n.setValidators([t]);let i=g1(n);e.asyncValidator!==null?n.setAsyncValidators(N_(i,e.asyncValidator)):typeof i=="function"&&n.setAsyncValidators([i]);let r=()=>n.updateValueAndValidity();O_(e._rawValidators,r),O_(e._rawAsyncValidators,r)}function C1(n,e){e.valueAccessor.registerOnChange(t=>{n._pendingValue=t,n._pendingChange=!0,n._pendingDirty=!0,n.updateOn==="change"&&Z_(n,e)})}function D1(n,e){e.valueAccessor.registerOnTouched(()=>{n._pendingTouched=!0,n.updateOn==="blur"&&n._pendingChange&&Z_(n,e),n.updateOn!=="submit"&&n.markAsTouched()})}function Z_(n,e){n._pendingDirty&&n.markAsDirty(),n.setValue(n._pendingValue,{emitModelToViewChange:!1}),e.viewToModelUpdate(n._pendingValue),n._pendingChange=!1}function T1(n,e){let t=(i,r)=>{e.valueAccessor.writeValue(i),r&&e.viewToModelUpdate(i)};n.registerOnChange(t),e._registerOnDestroy(()=>{n._unregisterOnChange(t)})}function A1(n,e){if(!n.hasOwnProperty("model"))return!1;let t=n.model;return t.isFirstChange()?!0:!Object.is(e,t.currentValue)}function I1(n){return Object.getPrototypeOf(n.constructor)===o1}function R1(n,e){if(!e)return null;Array.isArray(e);let t,i,r;return e.forEach(s=>{s.constructor===yc?t=s:I1(s)?i=s:r=s}),r||i||t||null}function L_(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function k_(n){return typeof n=="object"&&n!==null&&Object.keys(n).length===2&&"value"in n&&"disabled"in n}var N1=class extends np{constructor(e=null,t,i){super(y1(t),x1(i,t)),this.defaultValue=null,this._onChange=[],this._pendingChange=!1,this._applyFormState(e),this._setUpdateStrategy(t),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),_c(t)&&(t.nonNullable||t.initialValueIsDefault)&&(k_(e)?this.defaultValue=e.value:this.defaultValue=e)}setValue(e,t={}){this.value=this._pendingValue=e,this._onChange.length&&t.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,t.emitViewToModelChange!==!1)),this.updateValueAndValidity(t)}patchValue(e,t={}){this.setValue(e,t)}reset(e=this.defaultValue,t={}){this._applyFormState(e),this.markAsPristine(t),this.markAsUntouched(t),this.setValue(this.value,t),this._pendingChange=!1}_updateValue(){}_anyControls(e){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(e){this._onChange.push(e)}_unregisterOnChange(e){L_(this._onChange,e)}registerOnDisabledChange(e){this._onDisabledChange.push(e)}_unregisterOnDisabledChange(e){L_(this._onDisabledChange,e)}_forEachChild(e){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(e){k_(e)?(this.value=this._pendingValue=e.value,e.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=e}};var P1={provide:Oo,useExisting:oo(()=>rp)},U_=Promise.resolve(),rp=(()=>{class n extends Oo{constructor(t,i,r,s,o,a){super(),this._changeDetectorRef=o,this.callSetDisabledState=a,this.control=new N1,this._registered=!1,this.name="",this.update=new Jt,this._parent=t,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=R1(this,s)}ngOnChanges(t){if(this._checkForErrors(),!this._registered||"name"in t){if(this._registered&&(this._checkName(),this.formDirective)){let i=t.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in t&&this._updateDisabled(t),A1(t,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective&&this.formDirective.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(t){this.viewModel=t,this.update.emit(t)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){b1(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._isStandalone()||this._checkParentType(),this._checkName()}_checkParentType(){}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(t){U_.then(()=>{this.control.setValue(t,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(t){let i=t.isDisabled.currentValue,r=i!==0&&Bd(i);U_.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(t){return this._parent?E1(t,this._parent):[t]}static{this.\u0275fac=function(i){return new(i||n)(gt(ep,9),gt(u1,10),gt(d1,10),gt(B_,10),gt(Vd,8),gt(Y_,8))}}static{this.\u0275dir=Wn({type:n,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],features:[kd([P1]),os,od]})}}return n})();var F1=(()=>{class n{static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275mod=Wa({type:n})}static{this.\u0275inj=za({})}}return n})();var K_=(()=>{class n{static withConfig(t){return{ngModule:n,providers:[{provide:Y_,useValue:t.callSetDisabledState??ip}]}}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275mod=Wa({type:n})}static{this.\u0275inj=za({imports:[F1]})}}return n})();var xc=class n{constructor(e){this.el=e;this.delay=0}ngOnInit(){let e=this.el.nativeElement;e.classList.add("rv"),e.style.transitionDelay=this.delay+"ms",this.io=new IntersectionObserver(([t])=>{t.isIntersecting&&(e.classList.add("in"),this.io?.disconnect())},{threshold:.15}),this.io.observe(e)}ngOnDestroy(){this.io?.disconnect()}static{this.\u0275fac=function(t){return new(t||n)(gt(vn))}}static{this.\u0275dir=Wn({type:n,selectors:[["","reveal",""]],inputs:{delay:"delay"},standalone:!0})}},Mc=class n{constructor(e,t){this.el=e;this.zone=t;this.speed=.1;this.fn=()=>{let e=this.el.nativeElement,t=e.getBoundingClientRect();e.style.translate=`0 ${(t.top+t.height/2-innerHeight/2)*this.speed}px`}}ngOnInit(){matchMedia("(prefers-reduced-motion: reduce)").matches||(this.zone.runOutsideAngular(()=>addEventListener("scroll",this.fn,{passive:!0})),this.fn())}ngOnDestroy(){removeEventListener("scroll",this.fn)}static{this.\u0275fac=function(t){return new(t||n)(gt(vn),gt(_t))}}static{this.\u0275dir=Wn({type:n,selectors:[["","parallax",""]],inputs:{speed:[0,"parallax","speed"]},standalone:!0})}},Hs=class n{constructor(e){this.el=e}move(e){let t=this.el.nativeElement,i=t.getBoundingClientRect(),r=(e.clientX-i.left)/i.width-.5,s=(e.clientY-i.top)/i.height-.5;t.style.transform=`perspective(800px) rotateX(${-s*14}deg) rotateY(${r*14}deg) scale(1.03)`}leave(){this.el.nativeElement.style.transform=""}static{this.\u0275fac=function(t){return new(t||n)(gt(vn))}}static{this.\u0275dir=Wn({type:n,selectors:[["","tilt",""]],hostAttrs:[1,"tilt"],hostBindings:function(t,i){t&1&&jt("mousemove",function(s){return i.move(s)})("mouseleave",function(){return i.leave()})},standalone:!0})}};var L1=(n,e)=>e.l;function k1(n,e){if(n&1){let t=qn();N(0,"button",6),jt("click",function(){let r=xt(t).$implicit,s=Ke(2);return Mt(s.service.set(r.l))}),N(1,"span",7),B(2),P(),B(3),P()}if(n&2){let t=e.$implicit,i=Ke(2);Ii("on",i.service()===t.l),xe(2),St(t.i),xe(),St(t.l)}}function U1(n,e){if(n&1&&(N(0,"h2"),B(1,"Welche Art von Umzug planen Sie?"),P(),N(2,"p"),B(3,"Teilen Sie uns die wichtigsten Details mit, damit wir Ihre Anfrage pr\xFCfen k\xF6nnen."),P(),N(4,"div",4),jn(5,k1,4,4,"button",5,L1),P()),n&2){let t=Ke();xe(5),$n(t.services)}}function V1(n,e){if(n&1){let t=qn();N(0,"h2"),B(1,"Wo startet Ihr Umzug?"),P(),N(2,"div",8)(3,"input",9),qt("ngModelChange",function(r){xt(t);let s=Ke();return en(s.originZip,r)||(s.originZip=r),Mt(r)}),P(),N(4,"input",10),qt("ngModelChange",function(r){xt(t);let s=Ke();return en(s.originCity,r)||(s.originCity=r),Mt(r)}),P(),N(5,"input",11),qt("ngModelChange",function(r){xt(t);let s=Ke();return en(s.originAddress,r)||(s.originAddress=r),Mt(r)}),P()()}if(n&2){let t=Ke();xe(3),$t("ngModel",t.originZip),xe(),$t("ngModel",t.originCity),xe(),$t("ngModel",t.originAddress)}}function B1(n,e){if(n&1){let t=qn();N(0,"h2"),B(1,"Wohin geht Ihr Umzug?"),P(),N(2,"div",8)(3,"input",9),qt("ngModelChange",function(r){xt(t);let s=Ke();return en(s.destZip,r)||(s.destZip=r),Mt(r)}),P(),N(4,"input",10),qt("ngModelChange",function(r){xt(t);let s=Ke();return en(s.destCity,r)||(s.destCity=r),Mt(r)}),P(),N(5,"input",11),qt("ngModelChange",function(r){xt(t);let s=Ke();return en(s.destAddress,r)||(s.destAddress=r),Mt(r)}),P()()}if(n&2){let t=Ke();xe(3),$t("ngModel",t.destZip),xe(),$t("ngModel",t.destCity),xe(),$t("ngModel",t.destAddress)}}function H1(n,e){if(n&1){let t=qn();N(0,"h2"),B(1,"Wann m\xF6chten Sie umziehen?"),P(),N(2,"div",8)(3,"input",12),qt("ngModelChange",function(r){xt(t);let s=Ke();return en(s.date,r)||(s.date=r),Mt(r)}),P(),N(4,"input",13),qt("ngModelChange",function(r){xt(t);let s=Ke();return en(s.altDate,r)||(s.altDate=r),Mt(r)}),P()()}if(n&2){let t=Ke();xe(3),$t("ngModel",t.date),xe(),$t("ngModel",t.altDate)}}function z1(n,e){if(n&1){let t=qn();N(0,"h2"),B(1,"Ihre Kontaktdaten"),P(),N(2,"div",8)(3,"input",14),qt("ngModelChange",function(r){xt(t);let s=Ke();return en(s.name,r)||(s.name=r),Mt(r)}),P(),N(4,"input",15),qt("ngModelChange",function(r){xt(t);let s=Ke();return en(s.surname,r)||(s.surname=r),Mt(r)}),P(),N(5,"input",16),qt("ngModelChange",function(r){xt(t);let s=Ke();return en(s.phone,r)||(s.phone=r),Mt(r)}),P(),N(6,"input",17),qt("ngModelChange",function(r){xt(t);let s=Ke();return en(s.email,r)||(s.email=r),Mt(r)}),P()(),N(7,"div",18)(8,"label"),B(9,"Dieses Feld bitte leer lassen"),N(10,"input",19),qt("ngModelChange",function(r){xt(t);let s=Ke();return en(s.website,r)||(s.website=r),Mt(r)}),P()()(),N(11,"p",20),B(12,"Wir verwenden Ihre Angaben ausschlie\xDFlich zur Bearbeitung Ihrer Umzugsanfrage. Details finden Sie in unserer "),N(13,"a",21),B(14,"Datenschutzerkl\xE4rung"),P(),B(15,"."),P()}if(n&2){let t=Ke();xe(3),$t("ngModel",t.name),xe(),$t("ngModel",t.surname),xe(),$t("ngModel",t.phone),xe(),$t("ngModel",t.email),xe(4),$t("ngModel",t.website)}}function G1(n,e){if(n&1&&(N(0,"h2"),B(1),P(),N(2,"p"),B(3,"Ihre Umzugsanfrage wurde erfolgreich an KAFI Transporte \xFCbermittelt. Wir melden uns pers\xF6nlich bei Ihnen."),P(),N(4,"p",22)(5,"strong"),B(6),P(),yn(7,"br"),B(8),yn(9,"br"),B(10),P(),N(11,"div",3)(12,"a",23),B(13,"\u260E Anrufen"),P(),N(14,"a",24),B(15,"WhatsApp"),P()()),n&2){let t=Ke();xe(),lr("Vielen Dank, ",t.name,"!"),xe(5),St(t.service()),xe(2),Ld("",t.originCity," \u2192 ",t.destCity,""),xe(2),lr("Wunschtermin: ",t.date,"")}}function W1(n,e){if(n&1&&(N(0,"p",2),B(1),P()),n&2){let t=Ke();xe(),St(t.submitError())}}function j1(n,e){if(n&1){let t=qn();N(0,"div",3)(1,"button",25),jt("click",function(){xt(t);let r=Ke();return Mt(r.step.set(r.step()-1))}),B(2,"Zur\xFCck"),P(),N(3,"button",26),jt("click",function(){xt(t);let r=Ke();return Mt(r.advance())}),B(4),P()()}if(n&2){let t=Ke();xe(),un("disabled",t.step()===0||t.sending()),xe(2),un("disabled",!t.ok()||t.sending()),xe(),lr(" ",t.sending()?"Wird gesendet \u2026":t.step()===4?"Kostenloses Angebot anfragen":"Weiter"," ")}}var Ec=class n{constructor(){this.services=[{i:"\u{1F3E0}",l:"Privatumzug"},{i:"\u{1F3E2}",l:"Firmenumzug"},{i:"\u{1F5FA}\uFE0F",l:"Fernumzug"},{i:"\u{1F6CB}\uFE0F",l:"Einzelner M\xF6beltransport"},{i:"\u{1F4E6}",l:"Sonstiges"}];this.step=cn(0);this.service=cn("");this.sending=cn(!1);this.submitError=cn("");this.originZip="";this.originCity="";this.originAddress="";this.destZip="";this.destCity="";this.destAddress="";this.date="";this.altDate="";this.name="";this.surname="";this.phone="";this.email="";this.website="";this.Math=Math}advance(){if(!this.sending()){if(this.step()===4){this.submit();return}this.submitError.set(""),this.step.update(e=>e+1)}}submit(){return Ir(this,null,function*(){let e=window.KAFI_CONFIG?.leadsApiUrl?.trim();if(!e){this.submitError.set("Das Anfrageformular ist noch nicht mit dem E-Mail-Dienst verbunden. Bitte kontaktieren Sie uns telefonisch oder per WhatsApp.");return}this.sending.set(!0),this.submitError.set("");try{if(!(yield fetch(e,{method:"POST",headers:{"Content-Type":"application/json"},cache:"no-store",body:JSON.stringify({service:this.service(),origin:{zip:this.originZip,city:this.originCity,address:this.originAddress},destination:{zip:this.destZip,city:this.destCity,address:this.destAddress},date:this.date,alternateDate:this.altDate,contact:{firstName:this.name,lastName:this.surname,phone:this.phone,email:this.email},website:this.website})})).ok)throw new Error("Request was rejected");this.step.set(5)}catch{this.submitError.set("Ihre Anfrage konnte gerade nicht gesendet werden. Bitte versuchen Sie es erneut oder kontaktieren Sie uns telefonisch unter +49 178 7410656.")}finally{this.sending.set(!1)}})}ok(){let e=this.step();return e===0?!!this.service():e===1?!!(this.originZip&&this.originCity):e===2?!!(this.destZip&&this.destCity):e===3?!!this.date:e===4?!!(this.name.trim()&&this.surname.trim()&&this.phone.trim()&&/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(this.email.trim())):!0}static{this.\u0275fac=function(t){return new(t||n)}}static{this.\u0275cmp=ts({type:n,selectors:[["app-estimate"]],standalone:!0,features:[ls],decls:13,vars:6,consts:[[1,"wizard-progress"],[1,"bar"],["role","alert",1,"form-error"],[1,"row"],[1,"opts"],["tilt","",1,"opt",3,"on"],["tilt","",1,"opt",3,"click"],[1,"ico"],[1,"field-stack"],["placeholder","PLZ",1,"fld",3,"ngModelChange","ngModel"],["placeholder","Ort",1,"fld",3,"ngModelChange","ngModel"],["placeholder","Stra\xDFe / Adresse",1,"fld",3,"ngModelChange","ngModel"],["type","date",1,"fld",3,"ngModelChange","ngModel"],["placeholder","Alternativer Termin",1,"fld",3,"ngModelChange","ngModel"],["autocomplete","given-name","placeholder","Vorname","aria-label","Vorname",1,"fld",3,"ngModelChange","ngModel"],["autocomplete","family-name","placeholder","Nachname","aria-label","Nachname",1,"fld",3,"ngModelChange","ngModel"],["type","tel","autocomplete","tel","placeholder","Telefon","aria-label","Telefon",1,"fld",3,"ngModelChange","ngModel"],["type","email","autocomplete","email","placeholder","E-Mail","aria-label","E-Mail",1,"fld",3,"ngModelChange","ngModel"],["aria-hidden","true",1,"form-trap"],["tabindex","-1","autocomplete","off",3,"ngModelChange","ngModel"],[1,"form-note"],["href","/datenschutz/","target","_blank","rel","noreferrer"],[1,"request-summary"],["href","tel:+491787410656",1,"btn","ghost","dark"],["href","https://wa.me/491787410656","target","_blank","rel","noreferrer",1,"btn","ghost","dark"],[1,"btn","ghost","dark",3,"click","disabled"],[1,"btn","solid",3,"click","disabled"]],template:function(t,i){if(t&1&&(N(0,"div",0)(1,"span"),B(2),P(),N(3,"div",1),yn(4,"i"),P()(),uo(5,U1,7,0)(6,V1,6,3)(7,B1,6,3)(8,H1,5,2)(9,z1,16,5)(10,G1,16,5)(11,W1,2,1,"p",2)(12,j1,5,3,"div",3)),t&2){let r;xe(2),lr("Schritt ",i.step()<5?i.step()+1:5," von 5"),xe(2),Od("width",(i.Math.min(i.step(),4)+1)*20,"%"),xe(),as((r=i.step())===0?5:r===1?6:r===2?7:r===3?8:r===4?9:r===5?10:-1),xe(6),as(i.submitError()?11:-1),xe(),as(i.step()<5?12:-1)}},dependencies:[K_,yc,X_,rp,Hs],encapsulation:2})}};var sp=(n,e)=>e.label,J_=(n,e)=>e.t,$1=(n,e)=>e.q,q1=(n,e)=>e.c;function X1(n,e){if(n&1&&(N(0,"a",2),B(1),P()),n&2){let t=e.$implicit;un("href",t.href,Ja),xe(),St(t.label)}}function Y1(n,e){if(n&1&&(N(0,"span")(1,"strong"),B(2),P(),N(3,"small"),B(4),P()()),n&2){let t=e.$implicit;xe(2),St(t.label),xe(2),St(t.value)}}function Z1(n,e){if(n&1&&(N(0,"article",21)(1,"span",72),B(2),P(),N(3,"h3"),B(4),P(),N(5,"p"),B(6),P()()),n&2){let t=e.$implicit,i=e.$index;un("delay",i*80),xe(2),St(t.i),xe(2),St(t.t),xe(2),St(t.d)}}function K1(n,e){if(n&1&&(N(0,"article",47)(1,"span",72),B(2),P(),N(3,"h3"),B(4),P(),N(5,"p"),B(6),P()()),n&2){let t=e.$implicit,i=e.$index;un("delay",i*100),xe(2),St(t.i),xe(2),St(t.t),xe(2),St(t.d)}}function J1(n,e){if(n&1){let t=qn();N(0,"div",73)(1,"button",74),jt("click",function(){let r=xt(t).$index,s=Ke();return Mt(s.open.set(s.open()===r?-1:r))}),B(2),N(3,"i"),B(4,"+"),P()(),N(5,"div")(6,"p"),B(7),P()()()}if(n&2){let t=e.$implicit,i=e.$index,r=Ke();Ii("open",r.open()===i),xe(2),St(t.q),xe(5),St(t.a)}}function Q1(n,e){if(n&1&&(N(0,"li")(1,"a",2),B(2),P()()),n&2){let t=e.$implicit;xe(),un("href",t.href,Ja),xe(),St(t.label)}}function eR(n,e){if(n&1&&(N(0,"p"),B(1),yn(2,"br"),N(3,"a",2),B(4),P()()),n&2){let t=e.$implicit;xe(),St(t.c),xe(2),un("href","tel:"+t.n,Ja),xe(),St(t.n)}}function tR(n,e){if(n&1){let t=qn();N(0,"div",75),jt("click",function(){xt(t);let r=Ke();return Mt(r.modal.set(!1))}),N(1,"div",76),jt("click",function(r){return xt(t),Mt(r.stopPropagation())}),N(2,"button",77),jt("click",function(){xt(t);let r=Ke();return Mt(r.modal.set(!1))}),B(3,"\xD7"),P(),N(4,"h3"),B(5,"Jetzt anrufen"),P(),jn(6,eR,5,3,"p",null,q1),N(8,"a",78),B(9,"WhatsApp"),P()()()}if(n&2){let t=Ke();xe(6),$n(t.phones)}}var bc=class n{constructor(){this.scrolled=cn(!1);this.modal=cn(!1);this.open=cn(-1);this.nav=[{label:"Leistungen",href:"#services"},{label:"Ablauf",href:"#steps"},{label:"Deutschlandweit",href:"#coverage"},{label:"Bewertungen",href:"#reviews"},{label:"FAQ",href:"#faqs"}];this.services=[{i:"\u{1F3E0}",t:"Privatumzug",d:"Wohnung, Haus oder WG \u2013 Unterst\xFCtzung beim Transport Ihres Hausstands."},{i:"\u{1F3E2}",t:"Firmenumzug",d:"Umz\xFCge von B\xFCros, Praxen oder Unternehmensstandorten."},{i:"\u{1F5FA}\uFE0F",t:"Fernumzug",d:"Deutschlandweite Umz\xFCge \xFCber gr\xF6\xDFere Entfernungen."},{i:"\u{1F6CB}\uFE0F",t:"M\xF6beltransport",d:"Transport einzelner M\xF6belst\xFCcke oder gr\xF6\xDFerer Ladungen."},{i:"\u{1F9F0}",t:"M\xF6belmontage",d:"Abbau und Aufbau von M\xF6beln, sofern angeboten."},{i:"\u{1F4E6}",t:"Verpackungsservice",d:"Unterst\xFCtzung bei der Vorbereitung und Verpackung, sofern angeboten."},{i:"\u{1F9F9}",t:"Entr\xFCmpelung",d:"Entr\xFCmpelungen und Haushaltsaufl\xF6sungen, sofern angeboten."},{i:"\u{1F6A7}",t:"Halteverbotszone",d:"Unterst\xFCtzung bei der Organisation einer Ladezone, sofern angeboten."}];this.trust=[{label:"Deutschlandweit",value:"f\xFCr Sie unterwegs"},{label:"MyHammer",value:"4,5 / 5"},{label:"Google",value:"4,6 / 5"},{label:"Pers\xF6nlicher Kontakt",value:"direkt mit KAFI"}];this.features=[{i:"\u{1F30D}",t:"Deutschlandweit",d:"Von Berlin in St\xE4dte und Regionen in ganz Deutschland."},{i:"\u{1F91D}",t:"Pers\xF6nlich",d:"Direkter Kontakt und individuelle Abstimmung."},{i:"\u{1F50E}",t:"Transparent",d:"Individuelle Angebote auf Basis Ihrer Umzugsdaten."},{i:"\u2699\uFE0F",t:"Flexibel",d:"Vom einzelnen M\xF6beltransport bis zum kompletten Umzug."}];this.faqs=[{q:"Wie weit f\xE4hrt KAFI Transporte?",a:"KAFI Transporte hat seinen Standort in Berlin und f\xFChrt Umz\xFCge deutschlandweit durch."},{q:"Wie fr\xFCh sollte ich meinen Umzug buchen?",a:"Das h\xE4ngt unter anderem von Route, Umfang und gew\xFCnschtem Termin ab. Eine fr\xFChzeitige Anfrage erleichtert die Planung."},{q:"Wie wird der Preis bestimmt?",a:"Der Preis wird nicht automatisch auf der Website berechnet. Nach Ihrer Anfrage pr\xFCft KAFI Transporte die Angaben und erstellt bzw. bespricht ein individuelles Angebot."},{q:"Kann ich nur einzelne M\xF6bel transportieren lassen?",a:"Ja, sofern dieser Service f\xFCr Ihre Anfrage geeignet ist, kann ein individueller Transport einzelner M\xF6bel oder Ladungen organisiert werden."},{q:"\xDCbernimmt KAFI Transporte M\xF6belmontagen?",a:"Nur wenn die Leistung f\xFCr Ihren Auftrag passend ist. Bitte teilen Sie uns Ihre Anforderungen in der Anfrage mit."},{q:"Wie bekomme ich ein Angebot?",a:"\xDCber das Anfrageformular oder telefonisch unter +49 178 7410656."}];this.phones=[{c:"Berlin",n:"+49 178 7410656"}]}onScroll(){this.scrolled.set(scrollY>40)}static{this.\u0275fac=function(t){return new(t||n)}}static{this.\u0275cmp=ts({type:n,selectors:[["app-root"]],hostBindings:function(t,i){t&1&&jt("scroll",function(){return i.onScroll()},!1,Qg)},standalone:!0,features:[ls],decls:266,vars:6,consts:[[1,"nav"],["href","#",1,"logo"],[3,"href"],[1,"nav-actions"],["href","#estimate",1,"btn","ghost"],["href","tel:+491787410656","aria-label","KAFI Transporte anrufen: +49 178 7410656",1,"btn","call-btn"],["aria-hidden","true"],[1,"hero"],[1,"hero-in"],[1,"eyebrow"],["reveal",""],["reveal","",3,"delay"],["reveal","",1,"row",3,"delay"],["href","#estimate",1,"btn","solid"],["href","https://wa.me/491787410656?text=Hallo%20KAFI%20Transporte%2C%20ich%20m%C3%B6chte%20ein%20kostenloses%20Angebot%20anfragen.","target","_blank","rel","noreferrer",1,"btn","ghost"],[1,"trust-bar"],[1,"trust-inner"],["id","services",1,"sec","light"],["reveal","",1,"intro"],[1,"eyebrow","accent"],[1,"cards","services-grid"],["tilt","","reveal","",1,"card","service",3,"delay"],["id","coverage",1,"sec","dark","map-sec"],[1,"split","map-layout"],[1,"lede"],[1,"chip-row"],["tilt","",1,"map-art"],[1,"map-origin"],["data-name","Hamburg","aria-label","Hamburg",1,"city","c-hamburg"],["data-name","Bremen","aria-label","Bremen",1,"city","c-bremen"],["data-name","Hannover","aria-label","Hannover",1,"city","c-hannover"],["data-name","M\xFCnster","aria-label","M\xFCnster",1,"city","c-muenster"],["data-name","Essen","aria-label","Essen",1,"city","c-essen"],["data-name","Dortmund","aria-label","Dortmund",1,"city","c-dortmund"],["data-name","D\xFCsseldorf","aria-label","D\xFCsseldorf",1,"city","c-duesseldorf"],["data-name","K\xF6ln","aria-label","K\xF6ln",1,"city","c-koeln"],["data-name","Frankfurt","aria-label","Frankfurt",1,"city","c-frankfurt"],["data-name","Leipzig","aria-label","Leipzig",1,"city","c-leipzig"],["data-name","Dresden","aria-label","Dresden",1,"city","c-dresden"],["data-name","N\xFCrnberg","aria-label","N\xFCrnberg",1,"city","c-nuernberg"],["data-name","Stuttgart","aria-label","Stuttgart",1,"city","c-stuttgart"],["data-name","M\xFCnchen","aria-label","M\xFCnchen",1,"city","c-muenchen"],["id","steps",1,"sec","light"],[1,"process-grid"],["reveal","",1,"process"],["id","about",1,"sec","dark"],[1,"cards","about-grid"],["tilt","","reveal","",1,"card","about-card",3,"delay"],["id","reviews",1,"sec","light","review-sec"],[1,"lede","small"],[1,"cards","review-grid"],["reveal","",1,"card","review-card"],[1,"stars"],["href","https://www.my-hammer.de/","target","_blank","rel","noreferrer"],["href","https://www.google.com/search?q=KAFI+Transporte+Berlin","target","_blank","rel","noreferrer"],["id","estimate",1,"sec","light"],["reveal","",1,"wiz"],["id","faqs",1,"sec","light"],[1,"faq",3,"open"],["id","cta",1,"sec","dark","center","final-cta"],[1,"mega",3,"parallax"],[1,"cta-sub"],[1,"row"],[1,"footer-grid"],[1,"logo","footer-logo"],["href","tel:+491787410656"],["href","mailto:abourabiyehf@gmail.com"],[1,"footer-bottom"],["href","/impressum/"],["href","/datenschutz/"],["href","https://quality-1st.de/","target","_blank","rel","noopener noreferrer"],[1,"modal"],[1,"ico"],[1,"faq"],[3,"click"],[1,"modal",3,"click"],[1,"sheet",3,"click"],["aria-label","Close",1,"x",3,"click"],["href","https://wa.me/491787410656?text=Hallo%20KAFI%20Transporte%2C%20ich%20m%C3%B6chte%20ein%20kostenloses%20Angebot%20anfragen.","target","_blank","rel","noreferrer",1,"btn","solid"]],template:function(t,i){t&1&&(N(0,"header",0)(1,"a",1),B(2,"KAFI "),N(3,"b"),B(4,"Transporte"),P()(),N(5,"nav"),jn(6,X1,2,2,"a",2,sp),P(),N(8,"div",3)(9,"a",4),B(10,"Angebot erhalten"),P(),N(11,"a",5)(12,"span",6),B(13,"\u260E"),P(),B(14," Anrufen "),P()()(),N(15,"section",7),yn(16,"app-hero-3d"),N(17,"div",8)(18,"p",9),B(19,"Berlin \u2022 Deutschlandweit"),P(),N(20,"h1",10),B(21,"Ihr Umzug."),N(22,"span"),B(23,"Deutschlandweit."),P()(),N(24,"p",11),B(25,"Professionelle Umz\xFCge und Transporte von Berlin in ganz Deutschland \u2013 zuverl\xE4ssig, pers\xF6nlich und unkompliziert."),P(),N(26,"div",12)(27,"a",13),B(28,"Kostenloses Angebot anfragen \u2192"),P(),N(29,"a",5)(30,"span",6),B(31,"\u260E"),P(),B(32," Anrufen "),P(),N(33,"a",14),B(34,"WhatsApp"),P()()()(),N(35,"div",15)(36,"div",16),jn(37,Y1,5,2,"span",null,sp),P()(),N(39,"section",17)(40,"div",18)(41,"p",19),B(42,"Leistungen"),P(),N(43,"h2"),B(44,"Alles f\xFCr Ihren Umzug. Aus einer Hand."),P()(),N(45,"div",20),jn(46,Z1,7,4,"article",21,J_),P()(),N(48,"section",22)(49,"div",23)(50,"div",10)(51,"p",19),B(52,"Deutschlandweit"),P(),N(53,"h2"),B(54,"Von Berlin nach \xFCberall."),P(),N(55,"p",24),B(56,"Ihr Ziel ist nicht Berlin? KAFI Transporte ist deutschlandweit f\xFCr Sie unterwegs."),P(),N(57,"div",25)(58,"span"),B(59,"Hamburg"),P(),N(60,"span"),B(61,"Bremen"),P(),N(62,"span"),B(63,"Hannover"),P(),N(64,"span"),B(65,"M\xFCnster"),P(),N(66,"span"),B(67,"Essen"),P(),N(68,"span"),B(69,"Dortmund"),P(),N(70,"span"),B(71,"D\xFCsseldorf"),P(),N(72,"span"),B(73,"K\xF6ln"),P(),N(74,"span"),B(75,"Frankfurt"),P(),N(76,"span"),B(77,"Leipzig"),P(),N(78,"span"),B(79,"Dresden"),P(),N(80,"span"),B(81,"N\xFCrnberg"),P(),N(82,"span"),B(83,"Stuttgart"),P(),N(84,"span"),B(85,"M\xFCnchen"),P()()(),N(86,"div",26)(87,"div",27),B(88,"Berlin"),P(),yn(89,"span",28)(90,"span",29)(91,"span",30)(92,"span",31)(93,"span",32)(94,"span",33)(95,"span",34)(96,"span",35)(97,"span",36)(98,"span",37)(99,"span",38)(100,"span",39)(101,"span",40)(102,"span",41),P()()(),N(103,"section",42)(104,"div",18)(105,"p",19),B(106,"Ablauf"),P(),N(107,"h2"),B(108,"So einfach kann Umziehen sein."),P()(),N(109,"div",43)(110,"div",44)(111,"span"),B(112,"01"),P(),N(113,"h3"),B(114,"Anfrage"),P(),N(115,"p"),B(116,"Sie teilen uns Ihre Umzugsdaten mit."),P()(),N(117,"div",44)(118,"span"),B(119,"02"),P(),N(120,"h3"),B(121,"Pr\xFCfung"),P(),N(122,"p"),B(123,"Wir pr\xFCfen Route, Umfang, Termin und Anforderungen."),P()(),N(124,"div",44)(125,"span"),B(126,"03"),P(),N(127,"h3"),B(128,"Kontakt"),P(),N(129,"p"),B(130,"Wir melden uns bei Ihnen und kl\xE4ren offene Fragen."),P()(),N(131,"div",44)(132,"span"),B(133,"04"),P(),N(134,"h3"),B(135,"Angebot"),P(),N(136,"p"),B(137,"Sie erhalten ein individuelles Angebot."),P()(),N(138,"div",44)(139,"span"),B(140,"05"),P(),N(141,"h3"),B(142,"Planung"),P(),N(143,"p"),B(144,"Termin und weitere Details werden abgestimmt."),P()(),N(145,"div",44)(146,"span"),B(147,"06"),P(),N(148,"h3"),B(149,"Umzug"),P(),N(150,"p"),B(151,"Der Umzug wird durchgef\xFChrt."),P()()()(),N(152,"section",45)(153,"div",18)(154,"p",19),B(155,"Warum KAFI"),P(),N(156,"h2"),B(157,"Mehr als nur Transport."),P()(),N(158,"div",46),jn(159,K1,7,4,"article",47,J_),P()(),N(161,"section",48)(162,"div",18)(163,"p",19),B(164,"Bewertungen"),P(),N(165,"h2"),B(166,"Was Kunden \xFCber KAFI Transporte sagen."),P(),N(167,"p",49),B(168,"Echte Erfahrungen von Kunden auf unabh\xE4ngigen Plattformen."),P()(),N(169,"div",50)(170,"article",51)(171,"div",52),B(172,"\u2605\u2605\u2605\u2605\u2605"),P(),N(173,"h3"),B(174,"MyHammer"),P(),N(175,"strong"),B(176,"4,5 / 5"),P(),N(177,"span"),B(178,"414 Bewertungen"),P(),N(179,"a",53),B(180,"Auf MyHammer ansehen \u2192"),P()(),N(181,"article",51)(182,"div",52),B(183,"\u2605\u2605\u2605\u2605\u2605"),P(),N(184,"h3"),B(185,"Google"),P(),N(186,"strong"),B(187,"4,6 / 5"),P(),N(188,"span"),B(189,"21 Bewertungen"),P(),N(190,"a",54),B(191,"Auf Google ansehen \u2192"),P()()()(),N(192,"section",55)(193,"div",56),yn(194,"app-estimate"),P()(),N(195,"section",57)(196,"h2",10),B(197,"H\xE4ufige Fragen"),P(),jn(198,J1,8,4,"div",58,$1),P(),N(200,"section",59)(201,"h2",60),B(202,"Bereit f\xFCr Ihren Umzug?"),P(),N(203,"p",61),B(204,"Von Berlin in ganz Deutschland."),P(),N(205,"div",62)(206,"a",13),B(207,"Kostenloses Angebot anfragen \u2192"),P(),N(208,"a",5)(209,"span",6),B(210,"\u260E"),P(),B(211," Anrufen "),P()()(),N(212,"footer")(213,"div",63)(214,"div")(215,"div",64),B(216,"KAFI "),N(217,"b"),B(218,"Transporte"),P()(),N(219,"p"),B(220,"Ihr Umzug. Deutschlandweit."),P()(),N(221,"div")(222,"h4"),B(223,"Navigation"),P(),N(224,"ul"),jn(225,Q1,3,2,"li",null,sp),P()(),N(227,"div")(228,"h4"),B(229,"Kontakt"),P(),N(230,"ul")(231,"li"),B(232,"Ringslebenstra\xDFe 78"),P(),N(233,"li"),B(234,"12353 Berlin"),P(),N(235,"li"),B(236,"Deutschland"),P(),N(237,"li")(238,"a",65),B(239,"+49 178 7410656"),P()(),N(240,"li")(241,"a",66),B(242,"abourabiyehf@gmail.com"),P()()()(),N(243,"div")(244,"h4"),B(245,"Externe Plattformen"),P(),N(246,"ul")(247,"li")(248,"a",53),B(249,"MyHammer"),P()(),N(250,"li")(251,"a",54),B(252,"Google"),P()()()()(),N(253,"div",67)(254,"span"),B(255,"\xA9 2026 KAFI Transporte \xB7 "),N(256,"a",68),B(257,"Impressum"),P(),B(258," \xB7 "),N(259,"a",69),B(260,"Datenschutz"),P()(),N(261,"span"),B(262,"Professionell entwickelt von "),N(263,"a",70),B(264,"Quality1st"),P()()()(),uo(265,tR,10,0,"div",71)),t&2&&(Ii("solid",i.scrolled()),xe(6),$n(i.nav),xe(18),un("delay",120),xe(2),un("delay",240),xe(11),$n(i.trust),xe(9),$n(i.services),xe(113),$n(i.features),xe(39),$n(i.faqs),xe(3),un("parallax",.05),xe(24),$n(i.nav),xe(40),as(i.modal()?265:-1))},dependencies:[hc,Ec,xc,Mc,Hs],encapsulation:2})}};cy(bc).catch(console.error);
