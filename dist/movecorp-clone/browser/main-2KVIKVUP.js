var v_=Object.defineProperty,y_=Object.defineProperties;var __=Object.getOwnPropertyDescriptors;var Cp=Object.getOwnPropertySymbols;var x_=Object.prototype.hasOwnProperty,M_=Object.prototype.propertyIsEnumerable;var Dp=(n,e,t)=>e in n?v_(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,vt=(n,e)=>{for(var t in e||={})x_.call(e,t)&&Dp(n,t,e[t]);if(Cp)for(var t of Cp(e))M_.call(e,t)&&Dp(n,t,e[t]);return n},bt=(n,e)=>y_(n,__(e));var Ki=(n,e,t)=>new Promise((i,r)=>{var s=l=>{try{a(t.next(l))}catch(c){r(c)}},o=l=>{try{a(t.throw(l))}catch(c){r(c)}},a=l=>l.done?i(l.value):Promise.resolve(l.value).then(s,o);a((t=t.apply(n,e)).next())});function Ap(n,e){return Object.is(n,e)}var Pt=null,Bo=!1,Ho=1,Qn=Symbol("SIGNAL");function Ve(n){let e=Pt;return Pt=n,e}function Tp(){return Pt}var Xs={version:0,lastCleanEpoch:0,dirty:!1,producerNode:void 0,producerLastReadVersion:void 0,producerIndexOfThis:void 0,nextProducerIndex:0,liveConsumerNode:void 0,liveConsumerIndexOfThis:void 0,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function kc(n){if(Bo)throw new Error("");if(Pt===null)return;Pt.consumerOnSignalRead(n);let e=Pt.nextProducerIndex++;if(jo(Pt),e<Pt.producerNode.length&&Pt.producerNode[e]!==n&&qs(Pt)){let t=Pt.producerNode[e];Wo(t,Pt.producerIndexOfThis[e])}Pt.producerNode[e]!==n&&(Pt.producerNode[e]=n,Pt.producerIndexOfThis[e]=qs(Pt)?Pp(n,Pt,e):0),Pt.producerLastReadVersion[e]=n.version}function E_(){Ho++}function Ip(n){if(!(qs(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===Ho)){if(!n.producerMustRecompute(n)&&!Vc(n)){n.dirty=!1,n.lastCleanEpoch=Ho;return}n.producerRecomputeValue(n),n.dirty=!1,n.lastCleanEpoch=Ho}}function Rp(n){if(n.liveConsumerNode===void 0)return;let e=Bo;Bo=!0;try{for(let t of n.liveConsumerNode)t.dirty||b_(t)}finally{Bo=e}}function Np(){return Pt?.consumerAllowSignalWrites!==!1}function b_(n){n.dirty=!0,Rp(n),n.consumerMarkedDirty?.(n)}function Go(n){return n&&(n.nextProducerIndex=0),Ve(n)}function Uc(n,e){if(Ve(e),!(!n||n.producerNode===void 0||n.producerIndexOfThis===void 0||n.producerLastReadVersion===void 0)){if(qs(n))for(let t=n.nextProducerIndex;t<n.producerNode.length;t++)Wo(n.producerNode[t],n.producerIndexOfThis[t]);for(;n.producerNode.length>n.nextProducerIndex;)n.producerNode.pop(),n.producerLastReadVersion.pop(),n.producerIndexOfThis.pop()}}function Vc(n){jo(n);for(let e=0;e<n.producerNode.length;e++){let t=n.producerNode[e],i=n.producerLastReadVersion[e];if(i!==t.version||(Ip(t),i!==t.version))return!0}return!1}function Bc(n){if(jo(n),qs(n))for(let e=0;e<n.producerNode.length;e++)Wo(n.producerNode[e],n.producerIndexOfThis[e]);n.producerNode.length=n.producerLastReadVersion.length=n.producerIndexOfThis.length=0,n.liveConsumerNode&&(n.liveConsumerNode.length=n.liveConsumerIndexOfThis.length=0)}function Pp(n,e,t){if(Fp(n),n.liveConsumerNode.length===0&&Op(n))for(let i=0;i<n.producerNode.length;i++)n.producerIndexOfThis[i]=Pp(n.producerNode[i],n,i);return n.liveConsumerIndexOfThis.push(t),n.liveConsumerNode.push(e)-1}function Wo(n,e){if(Fp(n),n.liveConsumerNode.length===1&&Op(n))for(let i=0;i<n.producerNode.length;i++)Wo(n.producerNode[i],n.producerIndexOfThis[i]);let t=n.liveConsumerNode.length-1;if(n.liveConsumerNode[e]=n.liveConsumerNode[t],n.liveConsumerIndexOfThis[e]=n.liveConsumerIndexOfThis[t],n.liveConsumerNode.length--,n.liveConsumerIndexOfThis.length--,e<n.liveConsumerNode.length){let i=n.liveConsumerIndexOfThis[e],r=n.liveConsumerNode[e];jo(r),r.producerIndexOfThis[i]=e}}function qs(n){return n.consumerIsAlwaysLive||(n?.liveConsumerNode?.length??0)>0}function jo(n){n.producerNode??=[],n.producerIndexOfThis??=[],n.producerLastReadVersion??=[]}function Fp(n){n.liveConsumerNode??=[],n.liveConsumerIndexOfThis??=[]}function Op(n){return n.producerNode!==void 0}function Lp(n){let e=Object.create(S_);e.computation=n;let t=()=>{if(Ip(e),kc(e),e.value===zo)throw e.error;return e.value};return t[Qn]=e,t}var Oc=Symbol("UNSET"),Lc=Symbol("COMPUTING"),zo=Symbol("ERRORED"),S_=bt(vt({},Xs),{value:Oc,dirty:!0,error:null,equal:Ap,producerMustRecompute(n){return n.value===Oc||n.value===Lc},producerRecomputeValue(n){if(n.value===Lc)throw new Error("Detected cycle in computations.");let e=n.value;n.value=Lc;let t=Go(n),i;try{i=n.computation()}catch(r){i=zo,n.error=r}finally{Uc(n,t)}if(e!==Oc&&e!==zo&&i!==zo&&n.equal(e,i)){n.value=e;return}n.value=i,n.version++}});function w_(){throw new Error}var kp=w_;function Up(){kp()}function Vp(n){kp=n}var C_=null;function Bp(n){let e=Object.create(zp);e.value=n;let t=()=>(kc(e),e.value);return t[Qn]=e,t}function Hc(n,e){Np()||Up(),n.equal(n.value,e)||(n.value=e,D_(n))}function Hp(n,e){Np()||Up(),Hc(n,e(n.value))}var zp=bt(vt({},Xs),{equal:Ap,value:void 0});function D_(n){n.version++,E_(),Rp(n),C_?.()}function it(n){return typeof n=="function"}function $o(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var qo=$o(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Ys(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var Xt=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(it(i))try{i()}catch(s){e=s instanceof qo?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{Gp(s)}catch(o){e=e??[],o instanceof qo?e=[...e,...o.errors]:e.push(o)}}if(e)throw new qo(e)}}add(e){var t;if(e&&e!==this)if(this.closed)Gp(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&Ys(t,e)}remove(e){let{_finalizers:t}=this;t&&Ys(t,e),e instanceof n&&e._removeParent(this)}};Xt.EMPTY=(()=>{let n=new Xt;return n.closed=!0,n})();var zc=Xt.EMPTY;function Xo(n){return n instanceof Xt||n&&"closed"in n&&it(n.remove)&&it(n.add)&&it(n.unsubscribe)}function Gp(n){it(n)?n():n.unsubscribe()}var An={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Or={setTimeout(n,e,...t){let{delegate:i}=Or;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=Or;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function Yo(n){Or.setTimeout(()=>{let{onUnhandledError:e}=An;if(e)e(n);else throw n})}function Gc(){}var Wp=Wc("C",void 0,void 0);function jp(n){return Wc("E",void 0,n)}function $p(n){return Wc("N",n,void 0)}function Wc(n,e,t){return{kind:n,value:e,error:t}}var Ji=null;function Lr(n){if(An.useDeprecatedSynchronousErrorHandling){let e=!Ji;if(e&&(Ji={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=Ji;if(Ji=null,t)throw i}}else n()}function qp(n){An.useDeprecatedSynchronousErrorHandling&&Ji&&(Ji.errorThrown=!0,Ji.error=n)}var Qi=class extends Xt{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,Xo(e)&&e.add(this)):this.destination=I_}static create(e,t,i){return new kr(e,t,i)}next(e){this.isStopped?$c($p(e),this):this._next(e)}error(e){this.isStopped?$c(jp(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?$c(Wp,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},A_=Function.prototype.bind;function jc(n,e){return A_.call(n,e)}var qc=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){Zo(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){Zo(i)}else Zo(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){Zo(t)}}},kr=class extends Qi{constructor(e,t,i){super();let r;if(it(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&An.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&jc(e.next,s),error:e.error&&jc(e.error,s),complete:e.complete&&jc(e.complete,s)}):r=e}this.destination=new qc(r)}};function Zo(n){An.useDeprecatedSynchronousErrorHandling?qp(n):Yo(n)}function T_(n){throw n}function $c(n,e){let{onStoppedNotification:t}=An;t&&Or.setTimeout(()=>t(n,e))}var I_={closed:!0,next:Gc,error:T_,complete:Gc};var Ur=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Xp(n){return n}function Yp(n){return n.length===0?Xp:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var kt=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=N_(t)?t:new kr(t,i,r);return Lr(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=Zp(i),new i((r,s)=>{let o=new kr({next:a=>{try{t(a)}catch(l){s(l),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[Ur](){return this}pipe(...t){return Yp(t)(this)}toPromise(t){return t=Zp(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function Zp(n){var e;return(e=n??An.Promise)!==null&&e!==void 0?e:Promise}function R_(n){return n&&it(n.next)&&it(n.error)&&it(n.complete)}function N_(n){return n&&n instanceof Qi||R_(n)&&Xo(n)}function P_(n){return it(n?.lift)}function Vr(n){return e=>{if(P_(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function Br(n,e,t,i,r){return new Xc(n,e,t,i,r)}var Xc=class extends Qi{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(l){e.error(l)}}:super._next,this._error=r?function(a){try{r(a)}catch(l){e.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var Kp=$o(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var Bn=(()=>{class n extends kt{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new Ko(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new Kp}next(t){Lr(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){Lr(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){Lr(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?zc:(this.currentObservers=null,s.push(t),new Xt(()=>{this.currentObservers=null,Ys(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new kt;return t.source=this,t}}return n.create=(e,t)=>new Ko(e,t),n})(),Ko=class extends Bn{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:zc}};var Zs=class extends Bn{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};function F_(n){return n[n.length-1]}function Jp(n){return it(F_(n))?n.pop():void 0}function em(n,e,t,i){function r(s){return s instanceof t?s:new t(function(o){o(s)})}return new(t||(t=Promise))(function(s,o){function a(u){try{c(i.next(u))}catch(d){o(d)}}function l(u){try{c(i.throw(u))}catch(d){o(d)}}function c(u){u.done?s(u.value):r(u.value).then(a,l)}c((i=i.apply(n,e||[])).next())})}function Qp(n){var e=typeof Symbol=="function"&&Symbol.iterator,t=e&&n[e],i=0;if(t)return t.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function er(n){return this instanceof er?(this.v=n,this):new er(n)}function tm(n,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(n,e||[]),r,s=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",o),r[Symbol.asyncIterator]=function(){return this},r;function o(f){return function(g){return Promise.resolve(g).then(f,d)}}function a(f,g){i[f]&&(r[f]=function(_){return new Promise(function(m,p){s.push([f,_,m,p])>1||l(f,_)})},g&&(r[f]=g(r[f])))}function l(f,g){try{c(i[f](g))}catch(_){h(s[0][3],_)}}function c(f){f.value instanceof er?Promise.resolve(f.value.v).then(u,d):h(s[0][2],f)}function u(f){l("next",f)}function d(f){l("throw",f)}function h(f,g){f(g),s.shift(),s.length&&l(s[0][0],s[0][1])}}function nm(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=n[Symbol.asyncIterator],t;return e?e.call(n):(n=typeof Qp=="function"?Qp(n):n[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(s){t[s]=n[s]&&function(o){return new Promise(function(a,l){o=n[s](o),r(a,l,o.done,o.value)})}}function r(s,o,a,l){Promise.resolve(l).then(function(c){s({value:c,done:a})},o)}}var Jo=n=>n&&typeof n.length=="number"&&typeof n!="function";function Qo(n){return it(n?.then)}function ea(n){return it(n[Ur])}function ta(n){return Symbol.asyncIterator&&it(n?.[Symbol.asyncIterator])}function na(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function O_(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var ia=O_();function ra(n){return it(n?.[ia])}function sa(n){return tm(this,arguments,function*(){let t=n.getReader();try{for(;;){let{value:i,done:r}=yield er(t.read());if(r)return yield er(void 0);yield yield er(i)}}finally{t.releaseLock()}})}function oa(n){return it(n?.getReader)}function wi(n){if(n instanceof kt)return n;if(n!=null){if(ea(n))return L_(n);if(Jo(n))return k_(n);if(Qo(n))return U_(n);if(ta(n))return im(n);if(ra(n))return V_(n);if(oa(n))return B_(n)}throw na(n)}function L_(n){return new kt(e=>{let t=n[Ur]();if(it(t.subscribe))return t.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function k_(n){return new kt(e=>{for(let t=0;t<n.length&&!e.closed;t++)e.next(n[t]);e.complete()})}function U_(n){return new kt(e=>{n.then(t=>{e.closed||(e.next(t),e.complete())},t=>e.error(t)).then(null,Yo)})}function V_(n){return new kt(e=>{for(let t of n)if(e.next(t),e.closed)return;e.complete()})}function im(n){return new kt(e=>{H_(n,e).catch(t=>e.error(t))})}function B_(n){return im(sa(n))}function H_(n,e){var t,i,r,s;return em(this,void 0,void 0,function*(){try{for(t=nm(n);i=yield t.next(),!i.done;){let o=i.value;if(e.next(o),e.closed)return}}catch(o){r={error:o}}finally{try{i&&!i.done&&(s=t.return)&&(yield s.call(t))}finally{if(r)throw r.error}}e.complete()})}function Hn(n,e,t,i=0,r=!1){let s=e.schedule(function(){t(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(s),!r)return s}function aa(n,e=0){return Vr((t,i)=>{t.subscribe(Br(i,r=>Hn(i,n,()=>i.next(r),e),()=>Hn(i,n,()=>i.complete(),e),r=>Hn(i,n,()=>i.error(r),e)))})}function la(n,e=0){return Vr((t,i)=>{i.add(n.schedule(()=>t.subscribe(i),e))})}function rm(n,e){return wi(n).pipe(la(e),aa(e))}function sm(n,e){return wi(n).pipe(la(e),aa(e))}function om(n,e){return new kt(t=>{let i=0;return e.schedule(function(){i===n.length?t.complete():(t.next(n[i++]),t.closed||this.schedule())})})}function am(n,e){return new kt(t=>{let i;return Hn(t,e,()=>{i=n[ia](),Hn(t,e,()=>{let r,s;try{({value:r,done:s}=i.next())}catch(o){t.error(o);return}s?t.complete():t.next(r)},0,!0)}),()=>it(i?.return)&&i.return()})}function ca(n,e){if(!n)throw new Error("Iterable cannot be null");return new kt(t=>{Hn(t,e,()=>{let i=n[Symbol.asyncIterator]();Hn(t,e,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function lm(n,e){return ca(sa(n),e)}function cm(n,e){if(n!=null){if(ea(n))return rm(n,e);if(Jo(n))return om(n,e);if(Qo(n))return sm(n,e);if(ta(n))return ca(n,e);if(ra(n))return am(n,e);if(oa(n))return lm(n,e)}throw na(n)}function Yc(n,e){return e?cm(n,e):wi(n)}function tr(n,e){return Vr((t,i)=>{let r=0;t.subscribe(Br(i,s=>{i.next(n.call(e,s,r++))}))})}var{isArray:z_}=Array;function G_(n,e){return z_(e)?n(...e):n(e)}function um(n){return tr(e=>G_(n,e))}var{isArray:W_}=Array,{getPrototypeOf:j_,prototype:$_,keys:q_}=Object;function dm(n){if(n.length===1){let e=n[0];if(W_(e))return{args:e,keys:null};if(X_(e)){let t=q_(e);return{args:t.map(i=>e[i]),keys:t}}}return{args:n,keys:null}}function X_(n){return n&&typeof n=="object"&&j_(n)===$_}function hm(n,e){return n.reduce((t,i,r)=>(t[i]=e[r],t),{})}function Zc(...n){let e=Jp(n),{args:t,keys:i}=dm(n),r=new kt(s=>{let{length:o}=t;if(!o){s.complete();return}let a=new Array(o),l=o,c=o;for(let u=0;u<o;u++){let d=!1;wi(t[u]).subscribe(Br(s,h=>{d||(d=!0,c--),a[u]=h},()=>l--,void 0,()=>{(!l||!d)&&(c||s.next(i?hm(i,a):a),s.complete())}))}});return e?r.pipe(um(e)):r}var Zm="https://g.co/ng/security#xss",We=class extends Error{constructor(e,t){super(sd(e,t)),this.code=e}};function sd(n,e){return`${`NG0${Math.abs(n)}`}${e?": "+e:""}`}function Ga(n){return{toString:n}.toString()}function yt(n){for(let e in n)if(n[e]===yt)return e;throw Error("Could not find renamed property on target object.")}function Y_(n,e){for(let t in e)e.hasOwnProperty(t)&&!n.hasOwnProperty(t)&&(n[t]=e[t])}function _n(n){if(typeof n=="string")return n;if(Array.isArray(n))return"["+n.map(_n).join(", ")+"]";if(n==null)return""+n;if(n.overriddenName)return`${n.overriddenName}`;if(n.name)return`${n.name}`;let e=n.toString();if(e==null)return""+e;let t=e.indexOf(`
`);return t===-1?e:e.substring(0,t)}function fm(n,e){return n==null||n===""?e===null?"":e:e==null||e===""?n:n+" "+e}var Z_=yt({__forward_ref__:yt});function ri(n){return n.__forward_ref__=ri,n.toString=function(){return _n(this())},n}function Yt(n){return Km(n)?n():n}function Km(n){return typeof n=="function"&&n.hasOwnProperty(Z_)&&n.__forward_ref__===ri}function Dt(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function Wa(n){return{providers:n.providers||[],imports:n.imports||[]}}function od(n){return pm(n,Jm)||pm(n,Qm)}function pm(n,e){return n.hasOwnProperty(e)?n[e]:null}function K_(n){let e=n&&(n[Jm]||n[Qm]);return e||null}function mm(n){return n&&(n.hasOwnProperty(gm)||n.hasOwnProperty(J_))?n[gm]:null}var Jm=yt({\u0275prov:yt}),gm=yt({\u0275inj:yt}),Qm=yt({ngInjectableDef:yt}),J_=yt({ngInjectorDef:yt}),He=class{constructor(e,t){this._desc=e,this.ngMetadataName="InjectionToken",this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=Dt({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function eg(n){return n&&!!n.\u0275providers}var Q_=yt({\u0275cmp:yt}),ex=yt({\u0275dir:yt}),tx=yt({\u0275pipe:yt});var _a=yt({\u0275fac:yt}),Qs=yt({__NG_ELEMENT_ID__:yt}),vm=yt({__NG_ENV_ID__:yt});function qr(n){return typeof n=="string"?n:n==null?"":String(n)}function nx(n){return typeof n=="function"?n.name||n.toString():typeof n=="object"&&n!=null&&typeof n.type=="function"?n.type.name||n.type.toString():qr(n)}function ix(n,e){let t=e?`. Dependency path: ${e.join(" > ")} > ${n}`:"";throw new We(-200,n)}function ad(n,e){throw new We(-201,!1)}var Be=function(n){return n[n.Default=0]="Default",n[n.Host=1]="Host",n[n.Self=2]="Self",n[n.SkipSelf=4]="SkipSelf",n[n.Optional=8]="Optional",n}(Be||{}),du;function tg(){return du}function zn(n){let e=du;return du=n,e}function ng(n,e,t){let i=od(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&Be.Optional)return null;if(e!==void 0)return e;ad(n,"Injector")}var rx={},eo=rx,sx="__NG_DI_FLAG__",xa="ngTempTokenPath",ox="ngTokenPath",ax=/\n/gm,lx="\u0275",ym="__source",jr;function cx(){return jr}function Hr(n){let e=jr;return jr=n,e}function ux(n,e=Be.Default){if(jr===void 0)throw new We(-203,!1);return jr===null?ng(n,void 0,e):jr.get(n,e&Be.Optional?null:void 0,e)}function ft(n,e=Be.Default){return(tg()||ux)(Yt(n),e)}function rt(n,e=Be.Default){return ft(n,ja(e))}function ja(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function hu(n){let e=[];for(let t=0;t<n.length;t++){let i=Yt(n[t]);if(Array.isArray(i)){if(i.length===0)throw new We(900,!1);let r,s=Be.Default;for(let o=0;o<i.length;o++){let a=i[o],l=dx(a);typeof l=="number"?l===-1?r=a.token:s|=l:r=a}e.push(ft(r,s))}else e.push(ft(i))}return e}function dx(n){return n[sx]}function hx(n,e,t,i){let r=n[xa];throw e[ym]&&r.unshift(e[ym]),n.message=fx(`
`+n.message,r,t,i),n[ox]=r,n[xa]=null,n}function fx(n,e,t,i=null){n=n&&n.charAt(0)===`
`&&n.charAt(1)==lx?n.slice(2):n;let r=_n(e);if(Array.isArray(e))r=e.map(_n).join(" -> ");else if(typeof e=="object"){let s=[];for(let o in e)if(e.hasOwnProperty(o)){let a=e[o];s.push(o+":"+(typeof a=="string"?JSON.stringify(a):_n(a)))}r=`{${s.join(", ")}}`}return`${t}${i?"("+i+")":""}[${r}]: ${n.replace(ax,`
  `)}`}function Xr(n,e){let t=n.hasOwnProperty(_a);return t?n[_a]:null}function px(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function mx(n){return n.flat(Number.POSITIVE_INFINITY)}function ld(n,e){n.forEach(t=>Array.isArray(t)?ld(t,e):e(t))}function ig(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function Ma(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function gx(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let s=r-2;n[r]=n[s],r--}n[e]=t,n[e+1]=i}}function vx(n,e,t){let i=uo(n,e);return i>=0?n[i|1]=t:(i=~i,gx(n,i,e,t)),i}function Kc(n,e){let t=uo(n,e);if(t>=0)return n[t|1]}function uo(n,e){return yx(n,e,1)}function yx(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let s=i+(r-i>>1),o=n[s<<t];if(e===o)return s<<t;o>e?r=s:i=s+1}return~(r<<t)}var Yr={},yn=[],to=new He(""),rg=new He("",-1),sg=new He(""),Ea=class{get(e,t=eo){if(t===eo){let i=new Error(`NullInjectorError: No provider for ${_n(e)}!`);throw i.name="NullInjectorError",i}return t}},og=function(n){return n[n.OnPush=0]="OnPush",n[n.Default=1]="Default",n}(og||{}),jn=function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n}(jn||{}),Ai=function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n}(Ai||{});function _x(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}function fu(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];Mx(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function xx(n){return n===3||n===4||n===6}function Mx(n){return n.charCodeAt(0)===64}function no(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?_m(n,t,r,null,e[++i]):_m(n,t,r,null,null))}}return n}function _m(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){if(i===null){r!==null&&(n[s+1]=r);return}else if(i===n[s+1]){n[s+2]=r;return}}s++,i!==null&&s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),i!==null&&n.splice(s++,0,i),r!==null&&n.splice(s++,0,r)}var ag="ng-template";function Ex(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&_x(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(cd(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function cd(n){return n.type===4&&n.value!==ag}function bx(n,e,t){let i=n.type===4&&!t?ag:n.value;return e===i}function Sx(n,e,t){let i=4,r=n.attrs,s=r!==null?Dx(r):0,o=!1;for(let a=0;a<e.length;a++){let l=e[a];if(typeof l=="number"){if(!o&&!Tn(i)&&!Tn(l))return!1;if(o&&Tn(l))continue;o=!1,i=l|i&1;continue}if(!o)if(i&4){if(i=2|i&1,l!==""&&!bx(n,l,t)||l===""&&e.length===1){if(Tn(i))return!1;o=!0}}else if(i&8){if(r===null||!Ex(n,r,l,t)){if(Tn(i))return!1;o=!0}}else{let c=e[++a],u=wx(l,r,cd(n),t);if(u===-1){if(Tn(i))return!1;o=!0;continue}if(c!==""){let d;if(u>s?d="":d=r[u+1].toLowerCase(),i&2&&c!==d){if(Tn(i))return!1;o=!0}}}}return Tn(i)||o}function Tn(n){return(n&1)===0}function wx(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return Ax(e,n)}function Cx(n,e,t=!1){for(let i=0;i<e.length;i++)if(Sx(n,e[i],t))return!0;return!1}function Dx(n){for(let e=0;e<n.length;e++){let t=n[e];if(xx(t))return e}return n.length}function Ax(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function xm(n,e){return n?":not("+e.trim()+")":e}function Tx(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!Tn(o)&&(e+=xm(s,r),r=""),i=o,s=s||!Tn(i);t++}return r!==""&&(e+=xm(s,r)),e}function Ix(n){return n.map(Tx).join(",")}function Rx(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!Tn(r))break;r=s}i++}return{attrs:e,classes:t}}function os(n){return Ga(()=>{let e=ug(n),t=bt(vt({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===og.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||jn.Emulated,styles:n.styles||yn,_:null,schemas:n.schemas||null,tView:null,id:""});dg(t);let i=n.dependencies;return t.directiveDefs=Em(i,!1),t.pipeDefs=Em(i,!0),t.id=Ox(t),t})}function Nx(n){return Zr(n)||lg(n)}function Px(n){return n!==null}function $a(n){return Ga(()=>({type:n.type,bootstrap:n.bootstrap||yn,declarations:n.declarations||yn,imports:n.imports||yn,exports:n.exports||yn,transitiveCompileScopes:null,schemas:n.schemas||null,id:n.id||null}))}function Mm(n,e){if(n==null)return Yr;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a=Ai.None;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s):(s=r,o=r),e?(t[s]=a!==Ai.None?[i,a]:i,e[s]=o):t[s]=i}return t}function Zt(n){return Ga(()=>{let e=ug(n);return dg(e),e})}function Zr(n){return n[Q_]||null}function lg(n){return n[ex]||null}function cg(n){return n[tx]||null}function Fx(n){let e=Zr(n)||lg(n)||cg(n);return e!==null?e.standalone:!1}function ug(n){let e={};return{type:n.type,providersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputTransforms:null,inputConfig:n.inputs||Yr,exportAs:n.exportAs||null,standalone:n.standalone===!0,signals:n.signals===!0,selectors:n.selectors||yn,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,findHostDirectiveDefs:null,hostDirectives:null,inputs:Mm(n.inputs,e),outputs:Mm(n.outputs),debugInfo:null}}function dg(n){n.features?.forEach(e=>e(n))}function Em(n,e){if(!n)return null;let t=e?cg:Nx;return()=>(typeof n=="function"?n():n).map(i=>t(i)).filter(Px)}function Ox(n){let e=0,t=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,n.consts,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery].join("|");for(let r of t)e=Math.imul(31,e)+r.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function Lx(...n){return{\u0275providers:hg(!0,n),\u0275fromNgModule:!0}}function hg(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return ld(e,o=>{let a=o;pu(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&fg(r,s),t}function fg(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];ud(r,s=>{e(s,i)})}}function pu(n,e,t,i){if(n=Yt(n),!n)return!1;let r=null,s=mm(n),o=!s&&Zr(n);if(!s&&!o){let l=n.ngModule;if(s=mm(l),s)r=l;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let l=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let c of l)pu(c,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let c;try{ld(s.imports,u=>{pu(u,e,t,i)&&(c||=[],c.push(u))})}finally{}c!==void 0&&fg(c,e)}if(!a){let c=Xr(r)||(()=>new r);e({provide:r,useFactory:c,deps:yn},r),e({provide:sg,useValue:r,multi:!0},r),e({provide:to,useValue:()=>ft(r),multi:!0},r)}let l=s.providers;if(l!=null&&!a){let c=n;ud(l,u=>{e(u,c)})}}else return!1;return r!==n&&n.providers!==void 0}function ud(n,e){for(let t of n)eg(t)&&(t=t.\u0275providers),Array.isArray(t)?ud(t,e):e(t)}var kx=yt({provide:String,useValue:yt});function pg(n){return n!==null&&typeof n=="object"&&kx in n}function Ux(n){return!!(n&&n.useExisting)}function Vx(n){return!!(n&&n.useFactory)}function Kr(n){return typeof n=="function"}function Bx(n){return!!n.useClass}var qa=new He(""),fa={},Hx={},Jc;function dd(){return Jc===void 0&&(Jc=new Ea),Jc}var Ti=class{},ba=class extends Ti{get destroyed(){return this._destroyed}constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,this.records=new Map,this._ngOnDestroyHooks=new Set,this._onDestroyHooks=[],this._destroyed=!1,gu(e,o=>this.processProvider(o)),this.records.set(rg,zr(void 0,this)),r.has("environment")&&this.records.set(Ti,zr(void 0,this));let s=this.records.get(qa);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(sg,yn,Be.Self))}destroy(){this.assertNotDestroyed(),this._destroyed=!0;let e=Ve(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),Ve(e)}}onDestroy(e){return this.assertNotDestroyed(),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){this.assertNotDestroyed();let t=Hr(this),i=zn(void 0),r;try{return e()}finally{Hr(t),zn(i)}}get(e,t=eo,i=Be.Default){if(this.assertNotDestroyed(),e.hasOwnProperty(vm))return e[vm](this);i=ja(i);let r,s=Hr(this),o=zn(void 0);try{if(!(i&Be.SkipSelf)){let l=this.records.get(e);if(l===void 0){let c=$x(e)&&od(e);c&&this.injectableDefInScope(c)?l=zr(mu(e),fa):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l)}let a=i&Be.Self?dd():this.parent;return t=i&Be.Optional&&t===eo?null:t,a.get(e,t)}catch(a){if(a.name==="NullInjectorError"){if((a[xa]=a[xa]||[]).unshift(_n(e)),s)throw a;return hx(a,e,"R3InjectorError",this.source)}else throw a}finally{zn(o),Hr(s)}}resolveInjectorInitializers(){let e=Ve(null),t=Hr(this),i=zn(void 0),r;try{let s=this.get(to,yn,Be.Self);for(let o of s)o()}finally{Hr(t),zn(i),Ve(e)}}toString(){let e=[],t=this.records;for(let i of t.keys())e.push(_n(i));return`R3Injector[${e.join(", ")}]`}assertNotDestroyed(){if(this._destroyed)throw new We(205,!1)}processProvider(e){e=Yt(e);let t=Kr(e)?e:Yt(e&&e.provide),i=Gx(e);if(!Kr(e)&&e.multi===!0){let r=this.records.get(t);r||(r=zr(void 0,fa,!0),r.factory=()=>hu(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t){let i=Ve(null);try{return t.value===fa&&(t.value=Hx,t.value=t.factory()),typeof t.value=="object"&&t.value&&jx(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{Ve(i)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=Yt(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function mu(n){let e=od(n),t=e!==null?e.factory:Xr(n);if(t!==null)return t;if(n instanceof He)throw new We(204,!1);if(n instanceof Function)return zx(n);throw new We(204,!1)}function zx(n){if(n.length>0)throw new We(204,!1);let t=K_(n);return t!==null?()=>t.factory(n):()=>new n}function Gx(n){if(pg(n))return zr(void 0,n.useValue);{let e=mg(n);return zr(e,fa)}}function mg(n,e,t){let i;if(Kr(n)){let r=Yt(n);return Xr(r)||mu(r)}else if(pg(n))i=()=>Yt(n.useValue);else if(Vx(n))i=()=>n.useFactory(...hu(n.deps||[]));else if(Ux(n))i=()=>ft(Yt(n.useExisting));else{let r=Yt(n&&(n.useClass||n.provide));if(Wx(n))i=()=>new r(...hu(n.deps));else return Xr(r)||mu(r)}return i}function zr(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function Wx(n){return!!n.deps}function jx(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function $x(n){return typeof n=="function"||typeof n=="object"&&n instanceof He}function gu(n,e){for(let t of n)Array.isArray(t)?gu(t,e):t&&eg(t)?gu(t.\u0275providers,e):e(t)}function qx(){return tg()!==void 0||cx()!=null}function Xx(n){return typeof n=="function"}var si=0,Ne=1,De=2,Gt=3,In=4,Nn=5,io=6,Sa=7,Ht=8,Jr=9,$n=10,Ft=11,ro=12,bm=13,as=14,Rn=15,ir=16,Gr=17,ei=18,Xa=19,gg=20,Ci=21,Qc=22,xn=23,Mn=25,vg=1;var rr=7,wa=8,Qr=9,zt=10,Ca=function(n){return n[n.None=0]="None",n[n.HasTransplantedViews=2]="HasTransplantedViews",n}(Ca||{});function Di(n){return Array.isArray(n)&&typeof n[vg]=="object"}function oi(n){return Array.isArray(n)&&n[vg]===!0}function yg(n){return(n.flags&4)!==0}function Ya(n){return n.componentOffset>-1}function hd(n){return(n.flags&1)===1}function Ii(n){return!!n.template}function vu(n){return(n[De]&512)!==0}var yu=class{constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}};function _g(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}function Za(){return xg}function xg(n){return n.type.prototype.ngOnChanges&&(n.setInput=Zx),Yx}Za.ngInherit=!0;function Yx(){let n=Eg(this),e=n?.current;if(e){let t=n.previous;if(t===Yr)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function Zx(n,e,t,i,r){let s=this.declaredInputs[i],o=Eg(n)||Kx(n,{previous:Yr,current:null}),a=o.current||(o.current={}),l=o.previous,c=l[s];a[s]=new yu(c&&c.currentValue,t,l===Yr),_g(n,e,r,t)}var Mg="__ngSimpleChanges__";function Eg(n){return n[Mg]||null}function Kx(n,e){return n[Mg]=e}var Sm=null;var Gn=function(n,e,t){Sm?.(n,e,t)},Jx="svg",Qx="math";function qn(n){for(;Array.isArray(n);)n=n[si];return n}function bg(n,e){return qn(e[n])}function En(n,e){return qn(e[n.index])}function fd(n,e){return n.data[e]}function eM(n,e){return n[e]}function Ri(n,e){let t=e[n];return Di(t)?t:t[si]}function tM(n){return(n[De]&4)===4}function pd(n){return(n[De]&128)===128}function nM(n){return oi(n[Gt])}function es(n,e){return e==null?null:n[e]}function Sg(n){n[Gr]=0}function wg(n){n[De]&1024||(n[De]|=1024,pd(n)&&Ja(n))}function iM(n,e){for(;n>0;)e=e[as],n--;return e}function Ka(n){return!!(n[De]&9216||n[xn]?.dirty)}function _u(n){n[$n].changeDetectionScheduler?.notify(8),n[De]&64&&(n[De]|=1024),Ka(n)&&Ja(n)}function Ja(n){n[$n].changeDetectionScheduler?.notify(0);let e=sr(n);for(;e!==null&&!(e[De]&8192||(e[De]|=8192,!pd(e)));)e=sr(e)}function Cg(n,e){if((n[De]&256)===256)throw new We(911,!1);n[Ci]===null&&(n[Ci]=[]),n[Ci].push(e)}function rM(n,e){if(n[Ci]===null)return;let t=n[Ci].indexOf(e);t!==-1&&n[Ci].splice(t,1)}function sr(n){let e=n[Gt];return oi(e)?e[Gt]:e}var Ue={lFrame:Lg(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Dg=!1;function sM(){return Ue.lFrame.elementDepthCount}function oM(){Ue.lFrame.elementDepthCount++}function aM(){Ue.lFrame.elementDepthCount--}function Ag(){return Ue.bindingsEnabled}function lM(){return Ue.skipHydrationRootTNode!==null}function cM(n){return Ue.skipHydrationRootTNode===n}function uM(){Ue.skipHydrationRootTNode=null}function Xe(){return Ue.lFrame.lView}function Wt(){return Ue.lFrame.tView}function ut(n){return Ue.lFrame.contextLView=n,n[Ht]}function dt(n){return Ue.lFrame.contextLView=null,n}function nn(){let n=Tg();for(;n!==null&&n.type===64;)n=n.parent;return n}function Tg(){return Ue.lFrame.currentTNode}function dM(){let n=Ue.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function ho(n,e){let t=Ue.lFrame;t.currentTNode=n,t.isParent=e}function Ig(){return Ue.lFrame.isParent}function hM(){Ue.lFrame.isParent=!1}function fM(){return Ue.lFrame.contextLView}function Rg(){return Dg}function wm(n){Dg=n}function pM(){return Ue.lFrame.bindingIndex}function mM(n){return Ue.lFrame.bindingIndex=n}function ls(){return Ue.lFrame.bindingIndex++}function Ng(n){let e=Ue.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function gM(){return Ue.lFrame.inI18n}function vM(n,e){let t=Ue.lFrame;t.bindingIndex=t.bindingRootIndex=n,xu(e)}function yM(){return Ue.lFrame.currentDirectiveIndex}function xu(n){Ue.lFrame.currentDirectiveIndex=n}function _M(n){let e=Ue.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function Pg(){return Ue.lFrame.currentQueryIndex}function md(n){Ue.lFrame.currentQueryIndex=n}function xM(n){let e=n[Ne];return e.type===2?e.declTNode:e.type===1?n[Nn]:null}function Fg(n,e,t){if(t&Be.SkipSelf){let r=e,s=n;for(;r=r.parent,r===null&&!(t&Be.Host);)if(r=xM(s),r===null||(s=s[as],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=Ue.lFrame=Og();return i.currentTNode=e,i.lView=n,!0}function gd(n){let e=Og(),t=n[Ne];Ue.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function Og(){let n=Ue.lFrame,e=n===null?null:n.child;return e===null?Lg(n):e}function Lg(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function kg(){let n=Ue.lFrame;return Ue.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var Ug=kg;function vd(){let n=kg();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function MM(n){return(Ue.lFrame.contextLView=iM(n,Ue.lFrame.contextLView))[Ht]}function Ni(){return Ue.lFrame.selectedIndex}function or(n){Ue.lFrame.selectedIndex=n}function yd(){let n=Ue.lFrame;return fd(n.tView,n.selectedIndex)}function EM(){return Ue.lFrame.currentNamespace}var Vg=!0;function _d(){return Vg}function xd(n){Vg=n}function bM(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=xg(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function Md(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),l&&(n.viewHooks??=[]).push(-t,l),c&&((n.viewHooks??=[]).push(t,c),(n.viewCheckHooks??=[]).push(t,c)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function pa(n,e,t){Bg(n,e,3,t)}function ma(n,e,t,i){(n[De]&3)===t&&Bg(n,e,t,i)}function eu(n,e){let t=n[De];(t&3)===e&&(t&=16383,t+=1,n[De]=t)}function Bg(n,e,t,i){let r=i!==void 0?n[Gr]&65535:0,s=i??-1,o=e.length-1,a=0;for(let l=r;l<o;l++)if(typeof e[l+1]=="number"){if(a=e[l],i!=null&&a>=i)break}else e[l]<0&&(n[Gr]+=65536),(a<s||s==-1)&&(SM(n,t,e,l),n[Gr]=(n[Gr]&4294901760)+l+2),l++}function Cm(n,e){Gn(4,n,e);let t=Ve(null);try{e.call(n)}finally{Ve(t),Gn(5,n,e)}}function SM(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[De]>>14<n[Gr]>>16&&(n[De]&3)===e&&(n[De]+=16384,Cm(a,s)):Cm(a,s)}var $r=-1,ar=class{constructor(e,t,i){this.factory=e,this.resolving=!1,this.canSeeViewProviders=t,this.injectImpl=i}};function wM(n){return n instanceof ar}function CM(n){return(n.flags&8)!==0}function DM(n){return(n.flags&16)!==0}var tu={},Mu=class{constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){i=ja(i);let r=this.injector.get(e,tu,i);return r!==tu||t===tu?r:this.parentInjector.get(e,t,i)}};function Hg(n){return n!==$r}function Da(n){return n&32767}function AM(n){return n>>16}function Aa(n,e){let t=AM(n),i=e;for(;t>0;)i=i[as],t--;return i}var Eu=!0;function Dm(n){let e=Eu;return Eu=n,e}var TM=256,zg=TM-1,Gg=5,IM=0,Wn={};function RM(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(Qs)&&(i=t[Qs]),i==null&&(i=t[Qs]=IM++);let r=i&zg,s=1<<r;e.data[n+(r>>Gg)]|=s}function Ta(n,e){let t=Wg(n,e);if(t!==-1)return t;let i=e[Ne];i.firstCreatePass&&(n.injectorIndex=e.length,nu(i.data,n),nu(e,null),nu(i.blueprint,null));let r=Ed(n,e),s=n.injectorIndex;if(Hg(r)){let o=Da(r),a=Aa(r,e),l=a[Ne].data;for(let c=0;c<8;c++)e[s+c]=a[o+c]|l[o+c]}return e[s+8]=r,s}function nu(n,e){n.push(0,0,0,0,0,0,0,0,e)}function Wg(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function Ed(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=Yg(r),i===null)return $r;if(t++,r=r[as],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return $r}function bu(n,e,t){RM(n,e,t)}function jg(n,e,t){if(t&Be.Optional||n!==void 0)return n;ad(e,"NodeInjector")}function $g(n,e,t,i){if(t&Be.Optional&&i===void 0&&(i=null),!(t&(Be.Self|Be.Host))){let r=n[Jr],s=zn(void 0);try{return r?r.get(e,i,t&Be.Optional):ng(e,i,t&Be.Optional)}finally{zn(s)}}return jg(i,e,t)}function qg(n,e,t,i=Be.Default,r){if(n!==null){if(e[De]&2048&&!(i&Be.Self)){let o=OM(n,e,t,i,Wn);if(o!==Wn)return o}let s=Xg(n,e,t,i,Wn);if(s!==Wn)return s}return $g(e,t,i,r)}function Xg(n,e,t,i,r){let s=PM(t);if(typeof s=="function"){if(!Fg(e,n,i))return i&Be.Host?jg(r,t,i):$g(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&Be.Optional))ad(t);else return o}finally{Ug()}}else if(typeof s=="number"){let o=null,a=Wg(n,e),l=$r,c=i&Be.Host?e[Rn][Nn]:null;for((a===-1||i&Be.SkipSelf)&&(l=a===-1?Ed(n,e):e[a+8],l===$r||!Tm(i,!1)?a=-1:(o=e[Ne],a=Da(l),e=Aa(l,e)));a!==-1;){let u=e[Ne];if(Am(s,a,u.data)){let d=NM(a,e,t,o,i,c);if(d!==Wn)return d}l=e[a+8],l!==$r&&Tm(i,e[Ne].data[a+8]===c)&&Am(s,a,e)?(o=u,a=Da(l),e=Aa(l,e)):a=-1}}return r}function NM(n,e,t,i,r,s){let o=e[Ne],a=o.data[n+8],l=i==null?Ya(a)&&Eu:i!=o&&(a.type&3)!==0,c=r&Be.Host&&s===a,u=ga(a,o,t,l,c);return u!==null?lr(e,o,u,a):Wn}function ga(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,l=n.directiveStart,c=n.directiveEnd,u=s>>20,d=i?a:a+u,h=r?a+u:c;for(let f=d;f<h;f++){let g=o[f];if(f<l&&t===g||f>=l&&g.type===t)return f}if(r){let f=o[l];if(f&&Ii(f)&&f.type===t)return l}return null}function lr(n,e,t,i){let r=n[t],s=e.data;if(wM(r)){let o=r;o.resolving&&ix(nx(s[t]));let a=Dm(o.canSeeViewProviders);o.resolving=!0;let l,c=o.injectImpl?zn(o.injectImpl):null,u=Fg(n,i,Be.Default);try{r=n[t]=o.factory(void 0,s,n,i),e.firstCreatePass&&t>=i.directiveStart&&bM(t,s[t],e)}finally{c!==null&&zn(c),Dm(a),o.resolving=!1,Ug()}}return r}function PM(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(Qs)?n[Qs]:void 0;return typeof e=="number"?e>=0?e&zg:FM:e}function Am(n,e,t){let i=1<<n;return!!(t[e+(n>>Gg)]&i)}function Tm(n,e){return!(n&Be.Self)&&!(n&Be.Host&&e)}var nr=class{constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return qg(this._tNode,this._lView,e,ja(i),t)}};function FM(){return new nr(nn(),Xe())}function cs(n){return Ga(()=>{let e=n.prototype.constructor,t=e[_a]||Su(e),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let s=r[_a]||Su(r);if(s&&s!==t)return s;r=Object.getPrototypeOf(r)}return s=>new s})}function Su(n){return Km(n)?()=>{let e=Su(Yt(n));return e&&e()}:Xr(n)}function OM(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[De]&2048&&!(o[De]&512);){let a=Xg(s,o,t,i|Be.Self,Wn);if(a!==Wn)return a;let l=s.parent;if(!l){let c=o[gg];if(c){let u=c.get(t,Wn,i);if(u!==Wn)return u}l=Yg(o),o=o[as]}s=l}return r}function Yg(n){let e=n[Ne],t=e.type;return t===2?e.declTNode:t===1?n[Nn]:null}function Im(n,e=null,t=null,i){let r=LM(n,e,t,i);return r.resolveInjectorInitializers(),r}function LM(n,e=null,t=null,i,r=new Set){let s=[t||yn,Lx(n)];return i=i||(typeof n=="object"?void 0:_n(n)),new ba(s,e||dd(),i||null,r)}var ts=class n{static{this.THROW_IF_NOT_FOUND=eo}static{this.NULL=new Ea}static create(e,t){if(Array.isArray(e))return Im({name:""},t,e,"");{let i=e.name??"";return Im({name:i},e.parent,e.providers,i)}}static{this.\u0275prov=Dt({token:n,providedIn:"any",factory:()=>ft(rg)})}static{this.__NG_ELEMENT_ID__=-1}};var kM=new He("");kM.__NG_ELEMENT_ID__=n=>{let e=nn();if(e===null)throw new We(204,!1);if(e.type&2)return e.value;if(n&Be.Optional)return null;throw new We(204,!1)};var UM="ngOriginalError";function iu(n){return n[UM]}var Zg=!0,Kg=(()=>{class n{static{this.__NG_ELEMENT_ID__=VM}static{this.__NG_ENV_ID__=t=>t}}return n})(),wu=class extends Kg{constructor(e){super(),this._lView=e}onDestroy(e){return Cg(this._lView,e),()=>rM(this._lView,e)}};function VM(){return new wu(Xe())}var Qa=(()=>{class n{constructor(){this.taskId=0,this.pendingTasks=new Set,this.hasPendingTasks=new Zs(!1)}get _hasPendingTasks(){return this.hasPendingTasks.value}add(){this._hasPendingTasks||this.hasPendingTasks.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),t}remove(t){this.pendingTasks.delete(t),this.pendingTasks.size===0&&this._hasPendingTasks&&this.hasPendingTasks.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this._hasPendingTasks&&this.hasPendingTasks.next(!1)}static{this.\u0275prov=Dt({token:n,providedIn:"root",factory:()=>new n})}}return n})();var Cu=class extends Bn{constructor(e=!1){super(),this.destroyRef=void 0,this.pendingTasks=void 0,this.__isAsync=e,qx()&&(this.destroyRef=rt(Kg,{optional:!0})??void 0,this.pendingTasks=rt(Qa,{optional:!0})??void 0)}emit(e){let t=Ve(null);try{super.next(e)}finally{Ve(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let l=e;r=l.next?.bind(l),s=l.error?.bind(l),o=l.complete?.bind(l)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof Xt&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{e(t),i!==void 0&&this.pendingTasks?.remove(i)})}}},tn=Cu;function Ia(...n){}function Jg(n){let e,t;function i(){n=Ia;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function Rm(n){return queueMicrotask(()=>n()),()=>{n=Ia}}var bd="isAngularZone",Ra=bd+"_ID",BM=0,St=class n{constructor(e){this.hasPendingMacrotasks=!1,this.hasPendingMicrotasks=!1,this.isStable=!0,this.onUnstable=new tn(!1),this.onMicrotaskEmpty=new tn(!1),this.onStable=new tn(!1),this.onError=new tn(!1);let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=Zg}=e;if(typeof Zone>"u")throw new We(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,GM(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(bd)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new We(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new We(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,HM,Ia,Ia);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},HM={};function Sd(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function zM(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){Jg(()=>{n.callbackScheduled=!1,Du(n),n.isCheckStableRunning=!0,Sd(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),Du(n)}function GM(n){let e=()=>{zM(n)},t=BM++;n._inner=n._inner.fork({name:"angular",properties:{[bd]:!0,[Ra]:t,[Ra+t]:!0},onInvokeTask:(i,r,s,o,a,l)=>{if(WM(l))return i.invokeTask(s,o,a,l);try{return Nm(n),i.invokeTask(s,o,a,l)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),Pm(n)}},onInvoke:(i,r,s,o,a,l,c)=>{try{return Nm(n),i.invoke(s,o,a,l,c)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!jM(l)&&e(),Pm(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,Du(n),Sd(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function Du(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function Nm(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function Pm(n){n._nesting--,Sd(n)}var Au=class{constructor(){this.hasPendingMicrotasks=!1,this.hasPendingMacrotasks=!1,this.isStable=!0,this.onUnstable=new tn,this.onMicrotaskEmpty=new tn,this.onStable=new tn,this.onError=new tn}run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function WM(n){return Qg(n,"__ignore_ng_zone__")}function jM(n){return Qg(n,"__scheduler_tick__")}function Qg(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var ti=class{constructor(){this._console=console}handleError(e){let t=this._findOriginalError(e);this._console.error("ERROR",e),t&&this._console.error("ORIGINAL ERROR",t)}_findOriginalError(e){let t=e&&iu(e);for(;t&&iu(t);)t=iu(t);return t||null}},$M=new He("",{providedIn:"root",factory:()=>{let n=rt(St),e=rt(ti);return t=>n.runOutsideAngular(()=>e.handleError(t))}});function qM(){return us(nn(),Xe())}function us(n,e){return new bn(En(n,e))}var bn=(()=>{class n{constructor(t){this.nativeElement=t}static{this.__NG_ELEMENT_ID__=qM}}return n})();function XM(n){return n instanceof bn?n.nativeElement:n}function YM(){return this._results[Symbol.iterator]()}var Tu=class n{get changes(){return this._changes??=new tn}constructor(e=!1){this._emitDistinctChangesOnly=e,this.dirty=!0,this._onDirty=void 0,this._results=[],this._changesDetected=!1,this._changes=void 0,this.length=0,this.first=void 0,this.last=void 0;let t=n.prototype;t[Symbol.iterator]||(t[Symbol.iterator]=YM)}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=mx(e);(this._changesDetected=!px(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.emit(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}};function ev(n){return(n.flags&128)===128}var tv=new Map,ZM=0;function KM(){return ZM++}function JM(n){tv.set(n[Xa],n)}function Iu(n){tv.delete(n[Xa])}var Fm="__ngContext__";function cr(n,e){Di(e)?(n[Fm]=e[Xa],JM(e)):n[Fm]=e}function nv(n){return rv(n[ro])}function iv(n){return rv(n[In])}function rv(n){for(;n!==null&&!oi(n);)n=n[In];return n}var Ru;function sv(n){Ru=n}function QM(){if(Ru!==void 0)return Ru;if(typeof document<"u")return document;throw new We(210,!1)}var wd=new He("",{providedIn:"root",factory:()=>eE}),eE="ng",Cd=new He(""),ds=new He("",{providedIn:"platform",factory:()=>"unknown"});var Dd=new He("",{providedIn:"root",factory:()=>QM().body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var tE="h",nE="b";var iE=()=>null;function Ad(n,e,t=!1){return iE(n,e,t)}var ov=!1,rE=new He("",{providedIn:"root",factory:()=>ov});var Na=class{constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Zm})`}};function el(n){return n instanceof Na?n.changingThisBreaksApplicationSecurity:n}function av(n,e){let t=sE(n);if(t!=null&&t!==e){if(t==="ResourceURL"&&e==="URL")return!0;throw new Error(`Required a safe ${e}, got a ${t} (see ${Zm})`)}return t===e}function sE(n){return n instanceof Na&&n.getTypeName()||null}var oE=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function lv(n){return n=String(n),n.match(oE)?n:"unsafe:"+n}var Td=function(n){return n[n.NONE=0]="NONE",n[n.HTML=1]="HTML",n[n.STYLE=2]="STYLE",n[n.SCRIPT=3]="SCRIPT",n[n.URL=4]="URL",n[n.RESOURCE_URL=5]="RESOURCE_URL",n}(Td||{});function tl(n){let e=aE();return e?e.sanitize(Td.URL,n)||"":av(n,"URL")?el(n):lv(qr(n))}function aE(){let n=Xe();return n&&n[$n].sanitizer}function cv(n){return n.ownerDocument.defaultView}var ni=function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n}(ni||{}),lE;function Id(n,e){return lE(n,e)}function Wr(n,e,t,i,r){if(i!=null){let s,o=!1;oi(i)?s=i:Di(i)&&(o=!0,i=i[si]);let a=qn(i);n===0&&t!==null?r==null?fv(e,t,a):Pa(e,t,a,r||null,!0):n===1&&t!==null?Pa(e,t,a,r||null,!0):n===2?bE(e,a,o):n===3&&e.destroyNode(a),s!=null&&wE(e,n,s,t,r)}}function cE(n,e){return n.createText(e)}function uE(n,e,t){n.setValue(e,t)}function uv(n,e,t){return n.createElement(e,t)}function dE(n,e){dv(n,e),e[si]=null,e[Nn]=null}function hE(n,e,t,i,r,s){i[si]=r,i[Nn]=e,il(n,i,t,1,r,s)}function dv(n,e){e[$n].changeDetectionScheduler?.notify(9),il(n,e,e[Ft],2,null,null)}function fE(n){let e=n[ro];if(!e)return ru(n[Ne],n);for(;e;){let t=null;if(Di(e))t=e[ro];else{let i=e[zt];i&&(t=i)}if(!t){for(;e&&!e[In]&&e!==n;)Di(e)&&ru(e[Ne],e),e=e[Gt];e===null&&(e=n),Di(e)&&ru(e[Ne],e),t=e&&e[In]}e=t}}function pE(n,e,t,i){let r=zt+i,s=t.length;i>0&&(t[r-1][In]=e),i<s-zt?(e[In]=t[r],ig(t,zt+i,e)):(t.push(e),e[In]=null),e[Gt]=t;let o=e[ir];o!==null&&t!==o&&hv(o,e);let a=e[ei];a!==null&&a.insertView(n),_u(e),e[De]|=128}function hv(n,e){let t=n[Qr],i=e[Gt];if(Di(i))n[De]|=Ca.HasTransplantedViews;else{let r=i[Gt][Rn];e[Rn]!==r&&(n[De]|=Ca.HasTransplantedViews)}t===null?n[Qr]=[e]:t.push(e)}function Rd(n,e){let t=n[Qr],i=t.indexOf(e);t.splice(i,1)}function so(n,e){if(n.length<=zt)return;let t=zt+e,i=n[t];if(i){let r=i[ir];r!==null&&r!==n&&Rd(r,i),e>0&&(n[t-1][In]=i[In]);let s=Ma(n,zt+e);dE(i[Ne],i);let o=s[ei];o!==null&&o.detachView(s[Ne]),i[Gt]=null,i[In]=null,i[De]&=-129}return i}function nl(n,e){if(!(e[De]&256)){let t=e[Ft];t.destroyNode&&il(n,e,t,3,null,null),fE(e)}}function ru(n,e){if(e[De]&256)return;let t=Ve(null);try{e[De]&=-129,e[De]|=256,e[xn]&&Bc(e[xn]),gE(n,e),mE(n,e),e[Ne].type===1&&e[Ft].destroy();let i=e[ir];if(i!==null&&oi(e[Gt])){i!==e[Gt]&&Rd(i,e);let r=e[ei];r!==null&&r.detachView(n)}Iu(e)}finally{Ve(t)}}function mE(n,e){let t=n.cleanup,i=e[Sa];if(t!==null)for(let s=0;s<t.length-1;s+=2)if(typeof t[s]=="string"){let o=t[s+3];o>=0?i[o]():i[-o].unsubscribe(),s+=2}else{let o=i[t[s+1]];t[s].call(o)}i!==null&&(e[Sa]=null);let r=e[Ci];if(r!==null){e[Ci]=null;for(let s=0;s<r.length;s++){let o=r[s];o()}}}function gE(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof ar)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],l=s[o+1];Gn(4,a,l);try{l.call(a)}finally{Gn(5,a,l)}}else{Gn(4,r,s);try{s.call(r)}finally{Gn(5,r,s)}}}}}function vE(n,e,t){return yE(n,e.parent,t)}function yE(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[si];{let{componentOffset:r}=i;if(r>-1){let{encapsulation:s}=n.data[i.directiveStart+r];if(s===jn.None||s===jn.Emulated)return null}return En(i,t)}}function Pa(n,e,t,i,r){n.insertBefore(e,t,i,r)}function fv(n,e,t){n.appendChild(e,t)}function Om(n,e,t,i,r){i!==null?Pa(n,e,t,i,r):fv(n,e,t)}function pv(n,e){return n.parentNode(e)}function _E(n,e){return n.nextSibling(e)}function xE(n,e,t){return EE(n,e,t)}function ME(n,e,t){return n.type&40?En(n,t):null}var EE=ME,Lm;function Nd(n,e,t,i){let r=vE(n,i,e),s=e[Ft],o=i.parent||e[Nn],a=xE(o,i,e);if(r!=null)if(Array.isArray(t))for(let l=0;l<t.length;l++)Om(s,r,t[l],a,!1);else Om(s,r,t,a,!1);Lm!==void 0&&Lm(s,i,e,t,r)}function Ks(n,e){if(e!==null){let t=e.type;if(t&3)return En(e,n);if(t&4)return Nu(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return Ks(n,i);{let r=n[e.index];return oi(r)?Nu(-1,r):qn(r)}}else{if(t&128)return Ks(n,e.next);if(t&32)return Id(e,n)()||qn(n[e.index]);{let i=mv(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=sr(n[Rn]);return Ks(r,i)}else return Ks(n,e.next)}}}return null}function mv(n,e){if(e!==null){let i=n[Rn][Nn],r=e.projection;return i.projection[r]}return null}function Nu(n,e){let t=zt+n+1;if(t<e.length){let i=e[t],r=i[Ne].firstChild;if(r!==null)return Ks(i,r)}return e[rr]}function bE(n,e,t){n.removeChild(null,e,t)}function Pd(n,e,t,i,r,s,o){for(;t!=null;){if(t.type===128){t=t.next;continue}let a=i[t.index],l=t.type;if(o&&e===0&&(a&&cr(qn(a),i),t.flags|=2),(t.flags&32)!==32)if(l&8)Pd(n,e,t.child,i,r,s,!1),Wr(e,n,r,a,s);else if(l&32){let c=Id(t,i),u;for(;u=c();)Wr(e,n,r,u,s);Wr(e,n,r,a,s)}else l&16?SE(n,e,i,t,r,s):Wr(e,n,r,a,s);t=o?t.projectionNext:t.next}}function il(n,e,t,i,r,s){Pd(t,i,n.firstChild,e,r,s,!1)}function SE(n,e,t,i,r,s){let o=t[Rn],l=o[Nn].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let u=l[c];Wr(e,n,r,u,s)}else{let c=l,u=o[Gt];ev(i)&&(c.flags|=128),Pd(n,e,c,u,r,s,!0)}}function wE(n,e,t,i,r){let s=t[rr],o=qn(t);s!==o&&Wr(e,n,i,s,r);for(let a=zt;a<t.length;a++){let l=t[a];il(l[Ne],l,n,e,i,s)}}function CE(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let s=i.indexOf("-")===-1?void 0:ni.DashCase;r==null?n.removeStyle(t,i,s):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),s|=ni.Important),n.setStyle(t,i,r,s))}}function DE(n,e,t){n.setAttribute(e,"style",t)}function gv(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function vv(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&fu(n,e,i),r!==null&&gv(n,e,r),s!==null&&DE(n,e,s)}var ai={};function ie(n=1){yv(Wt(),Xe(),Ni()+n,!1)}function yv(n,e,t,i){if(!i)if((e[De]&3)===3){let s=n.preOrderCheckHooks;s!==null&&pa(e,s,t)}else{let s=n.preOrderHooks;s!==null&&ma(e,s,0,t)}or(t)}function Mt(n,e=Be.Default){let t=Xe();if(t===null)return ft(n,e);let i=nn();return qg(i,t,Yt(n),e)}function _v(n,e,t,i,r,s){let o=Ve(null);try{let a=null;r&Ai.SignalBased&&(a=e[i][Qn]),a!==null&&a.transformFn!==void 0&&(s=a.transformFn(s)),r&Ai.HasDecoratorInputTransform&&(s=n.inputTransforms[i].call(e,s)),n.setInput!==null?n.setInput(e,a,s,t,i):_g(e,a,i,s)}finally{Ve(o)}}function AE(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)or(~r);else{let s=r,o=t[++i],a=t[++i];vM(o,s);let l=e[s];a(2,l)}}}finally{or(-1)}}function rl(n,e,t,i,r,s,o,a,l,c,u){let d=e.blueprint.slice();return d[si]=r,d[De]=i|4|128|8|64,(c!==null||n&&n[De]&2048)&&(d[De]|=2048),Sg(d),d[Gt]=d[as]=n,d[Ht]=t,d[$n]=o||n&&n[$n],d[Ft]=a||n&&n[Ft],d[Jr]=l||n&&n[Jr]||null,d[Nn]=s,d[Xa]=KM(),d[io]=u,d[gg]=c,d[Rn]=e.type==2?n[Rn]:d,d}function sl(n,e,t,i,r){let s=n.data[e];if(s===null)s=TE(n,e,t,i,r),gM()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=dM();s.injectorIndex=o===null?-1:o.injectorIndex}return ho(s,!0),s}function TE(n,e,t,i,r){let s=Tg(),o=Ig(),a=o?s:s&&s.parent,l=n.data[e]=OE(n,a,t,e,i,r);return n.firstChild===null&&(n.firstChild=l),s!==null&&(o?s.child==null&&l.parent!==null&&(s.child=l):s.next===null&&(s.next=l,l.prev=s)),l}function xv(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function Mv(n,e,t,i,r){let s=Ni(),o=i&2;try{or(-1),o&&e.length>Mn&&yv(n,e,Mn,!1),Gn(o?2:0,r),t(i,r)}finally{or(s),Gn(o?3:1,r)}}function Ev(n,e,t){if(yg(e)){let i=Ve(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let l=t[o];a.contentQueries(1,l,o)}}}finally{Ve(i)}}}function bv(n,e,t){Ag()&&(HE(n,e,t,En(t,e)),(t.flags&64)===64&&Tv(n,e,t))}function Sv(n,e,t=En){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function wv(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=Fd(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function Fd(n,e,t,i,r,s,o,a,l,c,u){let d=Mn+i,h=d+r,f=IE(d,h),g=typeof c=="function"?c():c;return f[Ne]={type:n,blueprint:f,template:t,queries:null,viewQuery:a,declTNode:e,data:f.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:l,consts:g,incompleteFirstPass:!1,ssrId:u}}function IE(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:ai);return t}function RE(n,e,t,i){let s=i.get(rE,ov)||t===jn.ShadowDom,o=n.selectRootElement(e,s);return NE(o),o}function NE(n){PE(n)}var PE=()=>null;function FE(n,e,t,i){let r=Nv(e);r.push(t),n.firstCreatePass&&Pv(n).push(i,r.length-1)}function OE(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return lM()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:s,mergedAttrs:null,localNames:null,initialInputs:void 0,inputs:null,outputs:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function km(n,e,t,i,r){for(let s in e){if(!e.hasOwnProperty(s))continue;let o=e[s];if(o===void 0)continue;i??={};let a,l=Ai.None;Array.isArray(o)?(a=o[0],l=o[1]):a=o;let c=s;if(r!==null){if(!r.hasOwnProperty(s))continue;c=r[s]}n===0?Um(i,t,c,a,l):Um(i,t,c,a)}return i}function Um(n,e,t,i,r){let s;n.hasOwnProperty(t)?(s=n[t]).push(e,i):s=n[t]=[e,i],r!==void 0&&s.push(r)}function LE(n,e,t){let i=e.directiveStart,r=e.directiveEnd,s=n.data,o=e.attrs,a=[],l=null,c=null;for(let u=i;u<r;u++){let d=s[u],h=t?t.get(d):null,f=h?h.inputs:null,g=h?h.outputs:null;l=km(0,d.inputs,u,l,f),c=km(1,d.outputs,u,c,g);let _=l!==null&&o!==null&&!cd(e)?JE(l,u,o):null;a.push(_)}l!==null&&(l.hasOwnProperty("class")&&(e.flags|=8),l.hasOwnProperty("style")&&(e.flags|=16)),e.initialInputs=a,e.inputs=l,e.outputs=c}function kE(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function Cv(n,e,t,i,r,s,o,a){let l=En(e,t),c=e.inputs,u;!a&&c!=null&&(u=c[i])?(Od(n,t,u,i,r),Ya(e)&&UE(t,e.index)):e.type&3?(i=kE(i),r=o!=null?o(r,e.value||"",i):r,s.setProperty(l,i,r)):e.type&12}function UE(n,e){let t=Ri(e,n);t[De]&16||(t[De]|=64)}function Dv(n,e,t,i){if(Ag()){let r=i===null?null:{"":-1},s=GE(n,t),o,a;s===null?o=a=null:[o,a]=s,o!==null&&Av(n,e,t,o,r,a),r&&WE(t,i,r)}t.mergedAttrs=no(t.mergedAttrs,t.attrs)}function Av(n,e,t,i,r,s){for(let c=0;c<i.length;c++)bu(Ta(t,e),n,i[c].type);$E(t,n.data.length,i.length);for(let c=0;c<i.length;c++){let u=i[c];u.providersResolver&&u.providersResolver(u)}let o=!1,a=!1,l=xv(n,e,i.length,null);for(let c=0;c<i.length;c++){let u=i[c];t.mergedAttrs=no(t.mergedAttrs,u.hostAttrs),qE(n,t,e,l,u),jE(l,u,r),u.contentQueries!==null&&(t.flags|=4),(u.hostBindings!==null||u.hostAttrs!==null||u.hostVars!==0)&&(t.flags|=64);let d=u.type.prototype;!o&&(d.ngOnChanges||d.ngOnInit||d.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),o=!0),!a&&(d.ngOnChanges||d.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),a=!0),l++}LE(n,t,s)}function VE(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;BE(o)!=a&&o.push(a),o.push(t,i,s)}}function BE(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function HE(n,e,t,i){let r=t.directiveStart,s=t.directiveEnd;Ya(t)&&XE(e,t,n.data[r+t.componentOffset]),n.firstCreatePass||Ta(t,e),cr(i,e);let o=t.initialInputs;for(let a=r;a<s;a++){let l=n.data[a],c=lr(e,n,a,t);if(cr(c,e),o!==null&&KE(e,a-r,c,l,t,o),Ii(l)){let u=Ri(t.index,e);u[Ht]=lr(e,n,a,t)}}}function Tv(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=yM();try{or(s);for(let a=i;a<r;a++){let l=n.data[a],c=e[a];xu(a),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&zE(l,c)}}finally{or(-1),xu(o)}}function zE(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function GE(n,e){let t=n.directiveRegistry,i=null,r=null;if(t)for(let s=0;s<t.length;s++){let o=t[s];if(Cx(e,o.selectors,!1))if(i||(i=[]),Ii(o))if(o.findHostDirectiveDefs!==null){let a=[];r=r||new Map,o.findHostDirectiveDefs(o,a,r),i.unshift(...a,o);let l=a.length;Pu(n,e,l)}else i.unshift(o),Pu(n,e,0);else r=r||new Map,o.findHostDirectiveDefs?.(o,i,r),i.push(o)}return i===null?null:[i,r]}function Pu(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function WE(n,e,t){if(e){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new We(-301,!1);i.push(e[r],s)}}}function jE(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;Ii(e)&&(t[""]=n)}}function $E(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function qE(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=Xr(r.type,!0)),o=new ar(s,Ii(r),Mt);n.blueprint[i]=o,t[i]=o,VE(n,e,i,xv(n,t,r.hostVars,ai),r)}function XE(n,e,t){let i=En(e,n),r=wv(t),s=n[$n].rendererFactory,o=16;t.signals?o=4096:t.onPush&&(o=64);let a=ol(n,rl(n,r,null,o,i,e,null,s.createRenderer(i,t),null,null,null));n[e.index]=a}function YE(n,e,t,i,r,s){let o=En(n,e);ZE(e[Ft],o,s,n.value,t,i,r)}function ZE(n,e,t,i,r,s,o){if(s==null)n.removeAttribute(e,r,t);else{let a=o==null?qr(s):o(s,i||"",r);n.setAttribute(e,r,a,t)}}function KE(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;){let l=o[a++],c=o[a++],u=o[a++],d=o[a++];_v(i,t,l,c,u,d)}}function JE(n,e,t){let i=null,r=0;for(;r<t.length;){let s=t[r];if(s===0){r+=4;continue}else if(s===5){r+=2;continue}if(typeof s=="number")break;if(n.hasOwnProperty(s)){i===null&&(i=[]);let o=n[s];for(let a=0;a<o.length;a+=3)if(o[a]===e){i.push(s,o[a+1],o[a+2],t[r+1]);break}}r+=2}return i}function Iv(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function Rv(n,e){let t=n.contentQueries;if(t!==null){let i=Ve(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];md(s),a.contentQueries(2,e[o],o)}}}finally{Ve(i)}}}function ol(n,e){return n[ro]?n[bm][In]=e:n[ro]=e,n[bm]=e,e}function Fu(n,e,t){md(0);let i=Ve(null);try{e(n,t)}finally{Ve(i)}}function Nv(n){return n[Sa]??=[]}function Pv(n){return n.cleanup??=[]}function Fv(n,e){let t=n[Jr],i=t?t.get(ti,null):null;i&&i.handleError(e)}function Od(n,e,t,i,r){for(let s=0;s<t.length;){let o=t[s++],a=t[s++],l=t[s++],c=e[o],u=n.data[o];_v(u,c,i,a,l,r)}}function Ov(n,e,t){let i=bg(e,n);uE(n[Ft],i,t)}function QE(n,e){let t=Ri(e,n),i=t[Ne];eb(i,t);let r=t[si];r!==null&&t[io]===null&&(t[io]=Ad(r,t[Jr])),Ld(i,t,t[Ht])}function eb(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function Ld(n,e,t){gd(e);try{let i=n.viewQuery;i!==null&&Fu(1,i,t);let r=n.template;r!==null&&Mv(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[ei]?.finishViewCreation(n),n.staticContentQueries&&Rv(n,e),n.staticViewQueries&&Fu(2,n.viewQuery,t);let s=n.components;s!==null&&tb(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[De]&=-5,vd()}}function tb(n,e){for(let t=0;t<e.length;t++)QE(n,e[t])}function al(n,e,t,i){let r=Ve(null);try{let s=e.tView,a=n[De]&4096?4096:16,l=rl(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=n[e.index];l[ir]=c;let u=n[ei];return u!==null&&(l[ei]=u.createEmbeddedView(s)),Ld(s,l,t),l}finally{Ve(r)}}function Lv(n,e){let t=zt+e;if(t<n.length)return n[t]}function oo(n,e){return!e||e.firstChild===null||ev(n)}function ll(n,e,t,i=!0){let r=e[Ne];if(pE(r,e,n,t),i){let o=Nu(t,n),a=e[Ft],l=pv(a,n[rr]);l!==null&&hE(r,n[Nn],a,e,l,o)}let s=e[io];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function kv(n,e){let t=so(n,e);return t!==void 0&&nl(t[Ne],t),t}function Fa(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(qn(s)),oi(s)&&nb(s,i);let o=t.type;if(o&8)Fa(n,e,t.child,i);else if(o&32){let a=Id(t,e),l;for(;l=a();)i.push(l)}else if(o&16){let a=mv(e,t);if(Array.isArray(a))i.push(...a);else{let l=sr(e[Rn]);Fa(l[Ne],l,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function nb(n,e){for(let t=zt;t<n.length;t++){let i=n[t],r=i[Ne].firstChild;r!==null&&Fa(i[Ne],i,r,e)}n[rr]!==n[si]&&e.push(n[rr])}var Uv=[];function ib(n){return n[xn]??rb(n)}function rb(n){let e=Uv.pop()??Object.create(ob);return e.lView=n,e}function sb(n){n.lView[xn]!==n&&(n.lView=null,Uv.push(n))}var ob=bt(vt({},Xs),{consumerIsAlwaysLive:!0,consumerMarkedDirty:n=>{Ja(n.lView)},consumerOnSignalRead(){this.lView[xn]=this}});function ab(n){let e=n[xn]??Object.create(lb);return e.lView=n,e}var lb=bt(vt({},Xs),{consumerIsAlwaysLive:!0,consumerMarkedDirty:n=>{let e=sr(n.lView);for(;e&&!Vv(e[Ne]);)e=sr(e);e&&wg(e)},consumerOnSignalRead(){this.lView[xn]=this}});function Vv(n){return n.type!==2}var cb=100;function Bv(n,e=!0,t=0){let i=n[$n],r=i.rendererFactory,s=!1;s||r.begin?.();try{ub(n,t)}catch(o){throw e&&Fv(n,o),o}finally{s||(r.end?.(),i.inlineEffectRunner?.flush())}}function ub(n,e){let t=Rg();try{wm(!0),Ou(n,e);let i=0;for(;Ka(n);){if(i===cb)throw new We(103,!1);i++,Ou(n,1)}}finally{wm(t)}}function db(n,e,t,i){let r=e[De];if((r&256)===256)return;let s=!1,o=!1;!s&&e[$n].inlineEffectRunner?.flush(),gd(e);let a=!0,l=null,c=null;s||(Vv(n)?(c=ib(e),l=Go(c)):Tp()===null?(a=!1,c=ab(e),l=Go(c)):e[xn]&&(Bc(e[xn]),e[xn]=null));try{Sg(e),mM(n.bindingStartIndex),t!==null&&Mv(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let f=n.preOrderCheckHooks;f!==null&&pa(e,f,null)}else{let f=n.preOrderHooks;f!==null&&ma(e,f,0,null),eu(e,0)}if(o||hb(e),Hv(e,0),n.contentQueries!==null&&Rv(n,e),!s)if(u){let f=n.contentCheckHooks;f!==null&&pa(e,f)}else{let f=n.contentHooks;f!==null&&ma(e,f,1),eu(e,1)}AE(n,e);let d=n.components;d!==null&&Gv(e,d,0);let h=n.viewQuery;if(h!==null&&Fu(2,h,i),!s)if(u){let f=n.viewCheckHooks;f!==null&&pa(e,f)}else{let f=n.viewHooks;f!==null&&ma(e,f,2),eu(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[Qc]){for(let f of e[Qc])f();e[Qc]=null}s||(e[De]&=-73)}catch(u){throw s||Ja(e),u}finally{c!==null&&(Uc(c,l),a&&sb(c)),vd()}}function Hv(n,e){for(let t=nv(n);t!==null;t=iv(t))for(let i=zt;i<t.length;i++){let r=t[i];zv(r,e)}}function hb(n){for(let e=nv(n);e!==null;e=iv(e)){if(!(e[De]&Ca.HasTransplantedViews))continue;let t=e[Qr];for(let i=0;i<t.length;i++){let r=t[i];wg(r)}}}function fb(n,e,t){let i=Ri(e,n);zv(i,t)}function zv(n,e){pd(n)&&Ou(n,e)}function Ou(n,e){let i=n[Ne],r=n[De],s=n[xn],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&Vc(s)),o||=!1,s&&(s.dirty=!1),n[De]&=-9217,o)db(i,n,i.template,n[Ht]);else if(r&8192){Hv(n,1);let a=i.components;a!==null&&Gv(n,a,1)}}function Gv(n,e,t){for(let i=0;i<e.length;i++)fb(n,e[i],t)}function kd(n,e){let t=Rg()?64:1088;for(n[$n].changeDetectionScheduler?.notify(e);n;){n[De]|=t;let i=sr(n);if(vu(n)&&!i)return n;n=i}return null}var ur=class{get rootNodes(){let e=this._lView,t=e[Ne];return Fa(t,e,t.firstChild,[])}constructor(e,t,i=!0){this._lView=e,this._cdRefInjectingView=t,this.notifyErrorHandler=i,this._appRef=null,this._attachedToViewContainer=!1}get context(){return this._lView[Ht]}set context(e){this._lView[Ht]=e}get destroyed(){return(this._lView[De]&256)===256}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[Gt];if(oi(e)){let t=e[wa],i=t?t.indexOf(this):-1;i>-1&&(so(e,i),Ma(t,i))}this._attachedToViewContainer=!1}nl(this._lView[Ne],this._lView)}onDestroy(e){Cg(this._lView,e)}markForCheck(){kd(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[De]&=-129}reattach(){_u(this._lView),this._lView[De]|=128}detectChanges(){this._lView[De]|=1024,Bv(this._lView,this.notifyErrorHandler)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new We(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=vu(this._lView),t=this._lView[ir];t!==null&&!e&&Rd(t,this._lView),dv(this._lView[Ne],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new We(902,!1);this._appRef=e;let t=vu(this._lView),i=this._lView[ir];i!==null&&!t&&hv(i,this._lView),_u(this._lView)}},Oa=(()=>{class n{static{this.__NG_ELEMENT_ID__=gb}}return n})(),pb=Oa,mb=class extends pb{constructor(e,t,i){super(),this._declarationLView=e,this._declarationTContainer=t,this.elementRef=i}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,t){return this.createEmbeddedViewImpl(e,t)}createEmbeddedViewImpl(e,t,i){let r=al(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:t,dehydratedView:i});return new ur(r)}};function gb(){return Ud(nn(),Xe())}function Ud(n,e){return n.type&4?new mb(e,n,us(n,e)):null}var $F=new RegExp(`^(\\d+)*(${nE}|${tE})*(.*)`);var vb=()=>null;function ao(n,e){return vb(n,e)}var ns=class{},Vd=new He("",{providedIn:"root",factory:()=>!1});var Wv=new He(""),jv=new He(""),Lu=class{},La=class{};function yb(n){let e=Error(`No component factory found for ${_n(n)}.`);return e[_b]=n,e}var _b="ngComponent";var ku=class{resolveComponentFactory(e){throw yb(e)}},lo=class{static{this.NULL=new ku}},is=class{},cl=(()=>{class n{constructor(){this.destroyNode=null}static{this.__NG_ELEMENT_ID__=()=>xb()}}return n})();function xb(){let n=Xe(),e=nn(),t=Ri(e.index,n);return(Di(t)?t:n)[Ft]}var Mb=(()=>{class n{static{this.\u0275prov=Dt({token:n,providedIn:"root",factory:()=>null})}}return n})();function Uu(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=fm(r,a);else if(s==2){let l=a,c=e[++o];i=fm(i,l+": "+c+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}var Vu=class extends lo{constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=Zr(e);return new ka(t,this.ngModule)}};function Vm(n,e){let t=[];for(let i in n){if(!n.hasOwnProperty(i))continue;let r=n[i];if(r===void 0)continue;let s=Array.isArray(r),o=s?r[0]:r,a=s?r[1]:Ai.None;e?t.push({propName:o,templateName:i,isSignal:(a&Ai.SignalBased)!==0}):t.push({propName:o,templateName:i})}return t}function Eb(n){let e=n.toLowerCase();return e==="svg"?Jx:e==="math"?Qx:null}var ka=class extends La{get inputs(){let e=this.componentDef,t=e.inputTransforms,i=Vm(e.inputs,!0);if(t!==null)for(let r of i)t.hasOwnProperty(r.propName)&&(r.transform=t[r.propName]);return i}get outputs(){return Vm(this.componentDef.outputs,!1)}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=Ix(e.selectors),this.ngContentSelectors=e.ngContentSelectors?e.ngContentSelectors:[],this.isBoundToModule=!!t}create(e,t,i,r){let s=Ve(null);try{r=r||this.ngModule;let o=r instanceof Ti?r:r?.injector;o&&this.componentDef.getStandaloneInjector!==null&&(o=this.componentDef.getStandaloneInjector(o)||o);let a=o?new Mu(e,o):e,l=a.get(is,null);if(l===null)throw new We(407,!1);let c=a.get(Mb,null),u=a.get(ns,null),d={rendererFactory:l,sanitizer:c,inlineEffectRunner:null,changeDetectionScheduler:u},h=l.createRenderer(null,this.componentDef),f=this.componentDef.selectors[0][0]||"div",g=i?RE(h,i,this.componentDef.encapsulation,a):uv(h,f,Eb(f)),_=512;this.componentDef.signals?_|=4096:this.componentDef.onPush||(_|=16);let m=null;g!==null&&(m=Ad(g,a,!0));let p=Fd(0,null,null,1,0,null,null,null,null,null,null),w=rl(null,p,null,_,null,null,d,h,a,null,m);gd(w);let E,S,k=null;try{let I=this.componentDef,C,V=null;I.findHostDirectiveDefs?(C=[],V=new Map,I.findHostDirectiveDefs(I,C,V),C.push(I)):C=[I];let re=bb(w,g);k=Sb(re,g,I,C,w,d,h),S=fd(p,Mn),g&&Db(h,I,g,i),t!==void 0&&Ab(S,this.ngContentSelectors,t),E=Cb(k,I,C,V,w,[Tb]),Ld(p,w,null)}catch(I){throw k!==null&&Iu(k),Iu(w),I}finally{vd()}return new Bu(this.componentType,E,us(S,w),w,S)}finally{Ve(s)}}},Bu=class extends Lu{constructor(e,t,i,r,s){super(),this.location=i,this._rootLView=r,this._tNode=s,this.previousInputValues=null,this.instance=t,this.hostView=this.changeDetectorRef=new ur(r,void 0,!1),this.componentType=e}setInput(e,t){let i=this._tNode.inputs,r;if(i!==null&&(r=i[e])){if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let s=this._rootLView;Od(s[Ne],s,r,e,t),this.previousInputValues.set(e,t);let o=Ri(this._tNode.index,s);kd(o,1)}}get injector(){return new nr(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function bb(n,e){let t=n[Ne],i=Mn;return n[i]=e,sl(t,i,2,"#host",null)}function Sb(n,e,t,i,r,s,o){let a=r[Ne];wb(i,n,e,o);let l=null;e!==null&&(l=Ad(e,r[Jr]));let c=s.rendererFactory.createRenderer(e,t),u=16;t.signals?u=4096:t.onPush&&(u=64);let d=rl(r,wv(t),null,u,r[n.index],n,s,c,null,null,l);return a.firstCreatePass&&Pu(a,n,i.length-1),ol(r,d),r[n.index]=d}function wb(n,e,t,i){for(let r of n)e.mergedAttrs=no(e.mergedAttrs,r.hostAttrs);e.mergedAttrs!==null&&(Uu(e,e.mergedAttrs,!0),t!==null&&vv(i,t,e))}function Cb(n,e,t,i,r,s){let o=nn(),a=r[Ne],l=En(o,r);Av(a,r,o,t,null,i);for(let u=0;u<t.length;u++){let d=o.directiveStart+u,h=lr(r,a,d,o);cr(h,r)}Tv(a,r,o),l&&cr(l,r);let c=lr(r,a,o.directiveStart+o.componentOffset,o);if(n[Ht]=r[Ht]=c,s!==null)for(let u of s)u(c,e);return Ev(a,o,r),c}function Db(n,e,t,i){if(i)fu(n,t,["ng-version","18.2.14"]);else{let{attrs:r,classes:s}=Rx(e.selectors[0]);r&&fu(n,t,r),s&&s.length>0&&gv(n,t,s.join(" "))}}function Ab(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null?Array.from(s):null)}}function Tb(){let n=nn();Md(Xe()[Ne],n)}var Bd=(()=>{class n{static{this.__NG_ELEMENT_ID__=Ib}}return n})();function Ib(){let n=nn();return qv(n,Xe())}var Rb=Bd,$v=class extends Rb{constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return us(this._hostTNode,this._hostLView)}get injector(){return new nr(this._hostTNode,this._hostLView)}get parentInjector(){let e=Ed(this._hostTNode,this._hostLView);if(Hg(e)){let t=Aa(e,this._hostLView),i=Da(e),r=t[Ne].data[i+8];return new nr(r,t)}else return new nr(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=Bm(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-zt}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=ao(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,oo(this._hostTNode,o)),a}createComponent(e,t,i,r,s){let o=e&&!Xx(e),a;if(o)a=t;else{let g=t||{};a=g.index,i=g.injector,r=g.projectableNodes,s=g.environmentInjector||g.ngModuleRef}let l=o?e:new ka(Zr(e)),c=i||this.parentInjector;if(!s&&l.ngModule==null){let _=(o?c:this.parentInjector).get(Ti,null);_&&(s=_)}let u=Zr(l.componentType??{}),d=ao(this._lContainer,u?.id??null),h=d?.firstChild??null,f=l.create(c,r,h,s);return this.insertImpl(f.hostView,a,oo(this._hostTNode,d)),f}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(nM(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let l=r[Gt],c=new $v(l,l[Nn],l[Gt]);c.detach(c.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return ll(o,r,s,i),e.attachToViewContainerRef(),ig(su(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=Bm(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=so(this._lContainer,t);i&&(Ma(su(this._lContainer),t),nl(i[Ne],i))}detach(e){let t=this._adjustIndex(e,-1),i=so(this._lContainer,t);return i&&Ma(su(this._lContainer),t)!=null?new ur(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function Bm(n){return n[wa]}function su(n){return n[wa]||(n[wa]=[])}function qv(n,e){let t,i=e[n.index];return oi(i)?t=i:(t=Iv(i,e,null,n),e[n.index]=t,ol(e,t)),Pb(t,e,n,i),new $v(t,n,e)}function Nb(n,e){let t=n[Ft],i=t.createComment(""),r=En(e,n),s=pv(t,r);return Pa(t,s,i,_E(t,r),!1),i}var Pb=Lb,Fb=()=>!1;function Ob(n,e,t){return Fb(n,e,t)}function Lb(n,e,t,i){if(n[rr])return;let r;t.type&8?r=qn(i):r=Nb(e,t),n[rr]=r}var Hu=class n{constructor(e){this.queryList=e,this.matches=null}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},zu=class n{constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)Hd(e,t).matches!==null&&this.queries[t].setDirty()}},Gu=class{constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=Wb(e):this.predicate=e}},Wu=class n{constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},ju=class n{constructor(e,t=-1){this.metadata=e,this.matches=null,this.indexInDeclarationView=-1,this.crossesNgTemplate=!1,this._appliesToNextNode=!0,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,kb(t,s)),this.matchTNodeWithReadOption(e,t,ga(t,e,s,!1,!1))}else i===Oa?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,ga(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===bn||r===Bd||r===Oa&&t.type&4)this.addMatch(t.index,-2);else{let s=ga(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function kb(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function Ub(n,e){return n.type&11?us(n,e):n.type&4?Ud(n,e):null}function Vb(n,e,t,i){return t===-1?Ub(e,n):t===-2?Bb(n,e,i):lr(n,n[Ne],t,e)}function Bb(n,e,t){if(t===bn)return us(e,n);if(t===Oa)return Ud(e,n);if(t===Bd)return qv(e,n)}function Xv(n,e,t,i){let r=e[ei].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let l=0;o!==null&&l<o.length;l+=2){let c=o[l];if(c<0)a.push(null);else{let u=s[c];a.push(Vb(e,u,o[l+1],t.metadata.read))}}r.matches=a}return r.matches}function $u(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=Xv(n,e,r,t);for(let a=0;a<s.length;a+=2){let l=s[a];if(l>0)i.push(o[a/2]);else{let c=s[a+1],u=e[-l];for(let d=zt;d<u.length;d++){let h=u[d];h[ir]===h[Gt]&&$u(h[Ne],h,c,i)}if(u[Qr]!==null){let d=u[Qr];for(let h=0;h<d.length;h++){let f=d[h];$u(f[Ne],f,c,i)}}}}}return i}function Hb(n,e){return n[ei].queries[e].queryList}function zb(n,e,t){let i=new Tu((t&4)===4);return FE(n,e,i,i.destroy),(e[ei]??=new zu).queries.push(new Hu(i))-1}function Gb(n,e,t){let i=Wt();return i.firstCreatePass&&(jb(i,new Gu(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),zb(i,Xe(),e)}function Wb(n){return n.split(",").map(e=>e.trim())}function jb(n,e,t){n.queries===null&&(n.queries=new Wu),n.queries.track(new ju(e,t))}function Hd(n,e){return n.queries.getByIndex(e)}function $b(n,e){let t=n[Ne],i=Hd(t,e);return i.crossesNgTemplate?$u(t,n,e,[]):Xv(t,n,i,e)}var Hm=new Set;function hs(n){Hm.has(n)||(Hm.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}function qb(n){return typeof n=="function"&&n[Qn]!==void 0}function jt(n,e){hs("NgSignals");let t=Bp(n),i=t[Qn];return e?.equal&&(i.equal=e.equal),t.set=r=>Hc(i,r),t.update=r=>Hp(i,r),t.asReadonly=Xb.bind(t),t}function Xb(){let n=this[Qn];if(n.readonlyFn===void 0){let e=()=>this();e[Qn]=n,n.readonlyFn=e}return n.readonlyFn}function Yv(n){return qb(n)&&typeof n.set=="function"}function Yb(n){return Object.getPrototypeOf(n.prototype).constructor}function Xn(n){let e=Yb(n.type),t=!0,i=[n];for(;e;){let r;if(Ii(n))r=e.\u0275cmp||e.\u0275dir;else{if(e.\u0275cmp)throw new We(903,!1);r=e.\u0275dir}if(r){if(t){i.push(r);let o=n;o.inputs=ua(n.inputs),o.inputTransforms=ua(n.inputTransforms),o.declaredInputs=ua(n.declaredInputs),o.outputs=ua(n.outputs);let a=r.hostBindings;a&&eS(n,a);let l=r.viewQuery,c=r.contentQueries;if(l&&Jb(n,l),c&&Qb(n,c),Zb(n,r),Y_(n.outputs,r.outputs),Ii(r)&&r.data.animation){let u=n.data;u.animation=(u.animation||[]).concat(r.data.animation)}}let s=r.features;if(s)for(let o=0;o<s.length;o++){let a=s[o];a&&a.ngInherit&&a(n),a===Xn&&(t=!1)}}e=Object.getPrototypeOf(e)}Kb(i)}function Zb(n,e){for(let t in e.inputs){if(!e.inputs.hasOwnProperty(t)||n.inputs.hasOwnProperty(t))continue;let i=e.inputs[t];if(i!==void 0&&(n.inputs[t]=i,n.declaredInputs[t]=e.declaredInputs[t],e.inputTransforms!==null)){let r=Array.isArray(i)?i[0]:i;if(!e.inputTransforms.hasOwnProperty(r))continue;n.inputTransforms??={},n.inputTransforms[r]=e.inputTransforms[r]}}}function Kb(n){let e=0,t=null;for(let i=n.length-1;i>=0;i--){let r=n[i];r.hostVars=e+=r.hostVars,r.hostAttrs=no(r.hostAttrs,t=no(t,r.hostAttrs))}}function ua(n){return n===Yr?{}:n===yn?[]:n}function Jb(n,e){let t=n.viewQuery;t?n.viewQuery=(i,r)=>{e(i,r),t(i,r)}:n.viewQuery=e}function Qb(n,e){let t=n.contentQueries;t?n.contentQueries=(i,r,s)=>{e(i,r,s),t(i,r,s)}:n.contentQueries=e}function eS(n,e){let t=n.hostBindings;t?n.hostBindings=(i,r)=>{e(i,r),t(i,r)}:n.hostBindings=e}var rs=class{};var Ua=class extends rs{constructor(e){super(),this.componentFactoryResolver=new Vu(this),this.instance=null;let t=new ba([...e.providers,{provide:rs,useValue:this},{provide:lo,useValue:this.componentFactoryResolver}],e.parent||dd(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function tS(n,e,t=null){return new Ua({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}function ii(n,e,t){let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function nS(n,e,t,i){let r=ii(n,e,t);return ii(n,e+1,i)||r}function iS(n){return(n.flags&32)===32}function rS(n,e,t,i,r,s,o,a,l){let c=e.consts,u=sl(e,n,4,o||null,a||null);Dv(e,t,u,es(c,l)),Md(e,u);let d=u.tView=Fd(2,u,i,r,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,c,null);return e.queries!==null&&(e.queries.template(e,u),d.queries=e.queries.embeddedTView(u)),u}function qu(n,e,t,i,r,s,o,a,l,c){let u=t+Mn,d=e.firstCreatePass?rS(u,e,n,i,r,s,o,a,l):e.data[u];ho(d,!1);let h=sS(e,n,d,t);_d()&&Nd(e,n,h,d),cr(h,n);let f=Iv(h,n,h,d);return n[u]=f,ol(n,f),Ob(f,d,n),hd(d)&&bv(e,n,d),l!=null&&Sv(n,d,c),d}function Ut(n,e,t,i,r,s,o,a){let l=Xe(),c=Wt(),u=es(c.consts,s);return qu(l,c,n,e,t,i,r,u,o,a),Ut}var sS=oS;function oS(n,e,t,i){return xd(!0),e[Ft].createComment("")}var Js=function(n){return n[n.EarlyRead=0]="EarlyRead",n[n.Write=1]="Write",n[n.MixedReadWrite=2]="MixedReadWrite",n[n.Read=3]="Read",n}(Js||{}),aS=(()=>{class n{constructor(){this.impl=null}execute(){this.impl?.execute()}static{this.\u0275prov=Dt({token:n,providedIn:"root",factory:()=>new n})}}return n})(),zm=class n{constructor(){this.ngZone=rt(St),this.scheduler=rt(ns),this.errorHandler=rt(ti,{optional:!0}),this.sequences=new Set,this.deferredRegistrations=new Set,this.executing=!1}static{this.PHASES=[Js.EarlyRead,Js.Write,Js.MixedReadWrite,Js.Read]}execute(){this.executing=!0;for(let e of n.PHASES)for(let t of this.sequences)if(!(t.erroredOrDestroyed||!t.hooks[e]))try{t.pipelinedValue=this.ngZone.runOutsideAngular(()=>t.hooks[e](t.pipelinedValue))}catch(i){t.erroredOrDestroyed=!0,this.errorHandler?.handleError(i)}this.executing=!1;for(let e of this.sequences)e.afterRun(),e.once&&(this.sequences.delete(e),e.destroy());for(let e of this.deferredRegistrations)this.sequences.add(e);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear()}register(e){this.executing?this.deferredRegistrations.add(e):(this.sequences.add(e),this.scheduler.notify(6))}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}static{this.\u0275prov=Dt({token:n,providedIn:"root",factory:()=>new n})}};function Pn(n,e,t,i){let r=Xe(),s=ls();if(ii(r,s,e)){let o=Wt(),a=yd();YE(a,r,n,e,t,i)}return Pn}function lS(n,e,t,i){return ii(n,ls(),t)?e+qr(t)+i:ai}function cS(n,e,t,i,r,s){let o=pM(),a=nS(n,o,t,r);return Ng(2),a?e+qr(t)+i+qr(r)+s:ai}function da(n,e){return n<<17|e<<2}function dr(n){return n>>17&32767}function uS(n){return(n&2)==2}function dS(n,e){return n&131071|e<<17}function Xu(n){return n|2}function ss(n){return(n&131068)>>2}function ou(n,e){return n&-131069|e<<2}function hS(n){return(n&1)===1}function Yu(n){return n|1}function fS(n,e,t,i,r,s){let o=s?e.classBindings:e.styleBindings,a=dr(o),l=ss(o);n[i]=t;let c=!1,u;if(Array.isArray(t)){let d=t;u=d[1],(u===null||uo(d,u)>0)&&(c=!0)}else u=t;if(r)if(l!==0){let h=dr(n[a+1]);n[i+1]=da(h,a),h!==0&&(n[h+1]=ou(n[h+1],i)),n[a+1]=dS(n[a+1],i)}else n[i+1]=da(a,0),a!==0&&(n[a+1]=ou(n[a+1],i)),a=i;else n[i+1]=da(l,0),a===0?a=i:n[l+1]=ou(n[l+1],i),l=i;c&&(n[i+1]=Xu(n[i+1])),Gm(n,u,i,!0),Gm(n,u,i,!1),pS(e,u,n,i,s),o=da(a,l),s?e.classBindings=o:e.styleBindings=o}function pS(n,e,t,i,r){let s=r?n.residualClasses:n.residualStyles;s!=null&&typeof e=="string"&&uo(s,e)>=0&&(t[i+1]=Yu(t[i+1]))}function Gm(n,e,t,i){let r=n[t+1],s=e===null,o=i?dr(r):ss(r),a=!1;for(;o!==0&&(a===!1||s);){let l=n[o],c=n[o+1];mS(l,e)&&(a=!0,n[o+1]=i?Yu(c):Xu(c)),o=i?dr(c):ss(c)}a&&(n[t+1]=i?Xu(r):Yu(r))}function mS(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?uo(n,e)>=0:!1}function pt(n,e,t){let i=Xe(),r=ls();if(ii(i,r,e)){let s=Wt(),o=yd();Cv(s,o,i,n,e,i[Ft],t,!1)}return pt}function Wm(n,e,t,i,r){let s=e.inputs,o=r?"class":"style";Od(n,t,s[o],o,i)}function zd(n,e,t){return Zv(n,e,t,!1),zd}function Pi(n,e){return Zv(n,e,null,!0),Pi}function Zv(n,e,t,i){let r=Xe(),s=Wt(),o=Ng(2);if(s.firstUpdatePass&&vS(s,n,o,i),e!==ai&&ii(r,o,e)){let a=s.data[Ni()];ES(s,a,r,r[Ft],n,r[o+1]=bS(e,t),i,o)}}function gS(n,e){return e>=n.expandoStartIndex}function vS(n,e,t,i){let r=n.data;if(r[t+1]===null){let s=r[Ni()],o=gS(n,t);SS(s,i)&&e===null&&!o&&(e=!1),e=yS(r,s,e,i),fS(r,s,e,t,o,i)}}function yS(n,e,t,i){let r=_M(n),s=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=au(null,n,e,t,i),t=co(t,e.attrs,i),s=null);else{let o=e.directiveStylingLast;if(o===-1||n[o]!==r)if(t=au(r,n,e,t,i),s===null){let l=_S(n,e,i);l!==void 0&&Array.isArray(l)&&(l=au(null,n,e,l[1],i),l=co(l,e.attrs,i),xS(n,e,i,l))}else s=MS(n,e,i)}return s!==void 0&&(i?e.residualClasses=s:e.residualStyles=s),t}function _S(n,e,t){let i=t?e.classBindings:e.styleBindings;if(ss(i)!==0)return n[dr(i)]}function xS(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[dr(r)]=i}function MS(n,e,t){let i,r=e.directiveEnd;for(let s=1+e.directiveStylingLast;s<r;s++){let o=n[s].hostAttrs;i=co(i,o,t)}return co(i,e.attrs,t)}function au(n,e,t,i,r){let s=null,o=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<o&&(s=e[a],i=co(i,s.hostAttrs,r),s!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function co(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let s=0;s<e.length;s++){let o=e[s];typeof o=="number"?r=o:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),vx(n,o,t?!0:e[++s]))}return n===void 0?null:n}function ES(n,e,t,i,r,s,o,a){if(!(e.type&3))return;let l=n.data,c=l[a+1],u=hS(c)?jm(l,e,t,r,ss(c),o):void 0;if(!Va(u)){Va(s)||uS(c)&&(s=jm(l,null,t,r,a,o));let d=bg(Ni(),t);CE(i,o,d,r,s)}}function jm(n,e,t,i,r,s){let o=e===null,a;for(;r>0;){let l=n[r],c=Array.isArray(l),u=c?l[1]:l,d=u===null,h=t[r+1];h===ai&&(h=d?yn:void 0);let f=d?Kc(h,i):u===i?h:void 0;if(c&&!Va(f)&&(f=Kc(l,i)),Va(f)&&(a=f,o))return a;let g=n[r+1];r=o?dr(g):ss(g)}if(e!==null){let l=s?e.residualClasses:e.residualStyles;l!=null&&(a=Kc(l,i))}return a}function Va(n){return n!==void 0}function bS(n,e){return n==null||n===""||(typeof e=="string"?n=n+e:typeof n=="object"&&(n=_n(el(n)))),n}function SS(n,e){return(n.flags&(e?8:16))!==0}var Zu=class{destroy(e){}updateValue(e,t){}swap(e,t){let i=Math.min(e,t),r=Math.max(e,t),s=this.detach(r);if(r-i>1){let o=this.detach(i);this.attach(i,s),this.attach(r,o)}else this.attach(i,s)}move(e,t){this.attach(t,this.detach(e))}};function lu(n,e,t,i,r){return n===t&&Object.is(e,i)?1:Object.is(r(n,e),r(t,i))?-1:0}function wS(n,e,t){let i,r,s=0,o=n.length-1,a=void 0;if(Array.isArray(e)){let l=e.length-1;for(;s<=o&&s<=l;){let c=n.at(s),u=e[s],d=lu(s,c,s,u,t);if(d!==0){d<0&&n.updateValue(s,u),s++;continue}let h=n.at(o),f=e[l],g=lu(o,h,l,f,t);if(g!==0){g<0&&n.updateValue(o,f),o--,l--;continue}let _=t(s,c),m=t(o,h),p=t(s,u);if(Object.is(p,m)){let w=t(l,f);Object.is(w,_)?(n.swap(s,o),n.updateValue(o,f),l--,o--):n.move(o,s),n.updateValue(s,u),s++;continue}if(i??=new Ba,r??=qm(n,s,o,t),Ku(n,i,s,p))n.updateValue(s,u),s++,o++;else if(r.has(p))i.set(_,n.detach(s)),o--;else{let w=n.create(s,e[s]);n.attach(s,w),s++,o++}}for(;s<=l;)$m(n,i,t,s,e[s]),s++}else if(e!=null){let l=e[Symbol.iterator](),c=l.next();for(;!c.done&&s<=o;){let u=n.at(s),d=c.value,h=lu(s,u,s,d,t);if(h!==0)h<0&&n.updateValue(s,d),s++,c=l.next();else{i??=new Ba,r??=qm(n,s,o,t);let f=t(s,d);if(Ku(n,i,s,f))n.updateValue(s,d),s++,o++,c=l.next();else if(!r.has(f))n.attach(s,n.create(s,d)),s++,o++,c=l.next();else{let g=t(s,u);i.set(g,n.detach(s)),o--}}}for(;!c.done;)$m(n,i,t,n.length,c.value),c=l.next()}for(;s<=o;)n.destroy(n.detach(o--));i?.forEach(l=>{n.destroy(l)})}function Ku(n,e,t,i){return e!==void 0&&e.has(i)?(n.attach(t,e.get(i)),e.delete(i),!0):!1}function $m(n,e,t,i,r){if(Ku(n,e,i,t(i,r)))n.updateValue(i,r);else{let s=n.create(i,r);n.attach(i,s)}}function qm(n,e,t,i){let r=new Set;for(let s=e;s<=t;s++)r.add(i(s,n.at(s)));return r}var Ba=class{constructor(){this.kvMap=new Map,this._vMap=void 0}has(e){return this.kvMap.has(e)}delete(e){if(!this.has(e))return!1;let t=this.kvMap.get(e);return this._vMap!==void 0&&this._vMap.has(t)?(this.kvMap.set(e,this._vMap.get(t)),this._vMap.delete(t)):this.kvMap.delete(e),!0}get(e){return this.kvMap.get(e)}set(e,t){if(this.kvMap.has(e)){let i=this.kvMap.get(e);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,t)}else this.kvMap.set(e,t)}forEach(e){for(let[t,i]of this.kvMap)if(e(i,t),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),e(i,t)}}};function At(n,e){hs("NgControlFlow");let t=Xe(),i=ls(),r=t[i]!==ai?t[i]:-1,s=r!==-1?Ha(t,Mn+r):void 0,o=0;if(ii(t,i,n)){let a=Ve(null);try{if(s!==void 0&&kv(s,o),n!==-1){let l=Mn+n,c=Ha(t,l),u=td(t[Ne],l),d=ao(c,u.tView.ssrId),h=al(t,u,e,{dehydratedView:d});ll(c,h,o,oo(u,d))}}finally{Ve(a)}}else if(s!==void 0){let a=Lv(s,o);a!==void 0&&(a[Ht]=e)}}var Ju=class{constructor(e,t,i){this.lContainer=e,this.$implicit=t,this.$index=i}get $count(){return this.lContainer.length-zt}};function Kv(n){return n}var Qu=class{constructor(e,t,i){this.hasEmptyBlock=e,this.trackByFn=t,this.liveCollection=i}};function hn(n,e,t,i,r,s,o,a,l,c,u,d,h){hs("NgControlFlow");let f=Xe(),g=Wt(),_=l!==void 0,m=Xe(),p=a?o.bind(m[Rn][Ht]):o,w=new Qu(_,p);m[Mn+n]=w,qu(f,g,n+1,e,t,i,r,es(g.consts,s)),_&&qu(f,g,n+2,l,c,u,d,es(g.consts,h))}var ed=class extends Zu{constructor(e,t,i){super(),this.lContainer=e,this.hostLView=t,this.templateTNode=i,this.operationsCounter=void 0,this.needsIndexUpdate=!1}get length(){return this.lContainer.length-zt}at(e){return this.getLView(e)[Ht].$implicit}attach(e,t){let i=t[io];this.needsIndexUpdate||=e!==this.length,ll(this.lContainer,t,e,oo(this.templateTNode,i))}detach(e){return this.needsIndexUpdate||=e!==this.length-1,CS(this.lContainer,e)}create(e,t){let i=ao(this.lContainer,this.templateTNode.tView.ssrId),r=al(this.hostLView,this.templateTNode,new Ju(this.lContainer,t,e),{dehydratedView:i});return this.operationsCounter?.recordCreate(),r}destroy(e){nl(e[Ne],e),this.operationsCounter?.recordDestroy()}updateValue(e,t){this.getLView(e)[Ht].$implicit=t}reset(){this.needsIndexUpdate=!1,this.operationsCounter?.reset()}updateIndexes(){if(this.needsIndexUpdate)for(let e=0;e<this.length;e++)this.getLView(e)[Ht].$index=e}getLView(e){return DS(this.lContainer,e)}};function fn(n){let e=Ve(null),t=Ni();try{let i=Xe(),r=i[Ne],s=i[t],o=t+1,a=Ha(i,o);if(s.liveCollection===void 0){let c=td(r,o);s.liveCollection=new ed(a,i,c)}else s.liveCollection.reset();let l=s.liveCollection;if(wS(l,n,s.trackByFn),l.updateIndexes(),s.hasEmptyBlock){let c=ls(),u=l.length===0;if(ii(i,c,u)){let d=t+2,h=Ha(i,d);if(u){let f=td(r,d),g=ao(h,f.tView.ssrId),_=al(i,f,void 0,{dehydratedView:g});ll(h,_,0,oo(f,g))}else kv(h,0)}}}finally{Ve(e)}}function Ha(n,e){return n[e]}function CS(n,e){return so(n,e)}function DS(n,e){return Lv(n,e)}function td(n,e){return fd(n,e)}function AS(n,e,t,i,r,s){let o=e.consts,a=es(o,r),l=sl(e,n,2,i,a);return Dv(e,t,l,es(o,s)),l.attrs!==null&&Uu(l,l.attrs,!1),l.mergedAttrs!==null&&Uu(l,l.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,l),l}function A(n,e,t,i){let r=Xe(),s=Wt(),o=Mn+n,a=r[Ft],l=s.firstCreatePass?AS(o,s,r,e,t,i):s.data[o],c=TS(s,r,l,a,e,n);r[o]=c;let u=hd(l);return ho(l,!0),vv(a,c,l),!iS(l)&&_d()&&Nd(s,r,c,l),sM()===0&&cr(c,r),oM(),u&&(bv(s,r,l),Ev(s,l,r)),i!==null&&Sv(r,l),A}function D(){let n=nn();Ig()?hM():(n=n.parent,ho(n,!1));let e=n;cM(e)&&uM(),aM();let t=Wt();return t.firstCreatePass&&(Md(t,n),yg(n)&&t.queries.elementEnd(n)),e.classesWithoutHost!=null&&CM(e)&&Wm(t,e,Xe(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&DM(e)&&Wm(t,e,Xe(),e.stylesWithoutHost,!1),D}function pn(n,e,t,i){return A(n,e,t,i),D(),pn}var TS=(n,e,t,i,r,s)=>(xd(!0),uv(i,r,EM()));function rn(){return Xe()}var za="en-US";var IS=za;function RS(n){typeof n=="string"&&(IS=n.toLowerCase().replace(/_/g,"-"))}var NS=(n,e,t)=>{};function et(n,e,t,i){let r=Xe(),s=Wt(),o=nn();return Jv(s,r,r[Ft],o,n,e,i),et}function PS(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[Sa],l=r[s+2];return a.length>l?a[l]:null}typeof o=="string"&&(s+=2)}return null}function Jv(n,e,t,i,r,s,o){let a=hd(i),c=n.firstCreatePass&&Pv(n),u=e[Ht],d=Nv(e),h=!0;if(i.type&3||o){let _=En(i,e),m=o?o(_):_,p=d.length,w=o?S=>o(qn(S[i.index])):i.index,E=null;if(!o&&a&&(E=PS(n,e,r,i.index)),E!==null){let S=E.__ngLastListenerFn__||E;S.__ngNextListenerFn__=s,E.__ngLastListenerFn__=s,h=!1}else{s=Ym(i,e,u,s),NS(_,r,s);let S=t.listen(m,r,s);d.push(s,S),c&&c.push(r,w,p,p+1)}}else s=Ym(i,e,u,s);let f=i.outputs,g;if(h&&f!==null&&(g=f[r])){let _=g.length;if(_)for(let m=0;m<_;m+=2){let p=g[m],w=g[m+1],k=e[p][w].subscribe(s),I=d.length;d.push(s,k),c&&c.push(r,i.index,I,-(I+1))}}}function Xm(n,e,t,i){let r=Ve(null);try{return Gn(6,e,t),t(i)!==!1}catch(s){return Fv(n,s),!1}finally{Gn(7,e,t),Ve(r)}}function Ym(n,e,t,i){return function r(s){if(s===Function)return i;let o=n.componentOffset>-1?Ri(n.index,e):e;kd(o,5);let a=Xm(e,t,i,s),l=r.__ngNextListenerFn__;for(;l;)a=Xm(e,t,l,s)&&a,l=l.__ngNextListenerFn__;return a}}function Oe(n=1){return MM(n)}function Qv(n,e,t){Gb(n,e,t)}function ey(n){let e=Xe(),t=Wt(),i=Pg();md(i+1);let r=Hd(t,i);if(n.dirty&&tM(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let s=$b(e,i);n.reset(s,XM),n.notifyOnChanges()}return!0}return!1}function ty(){return Hb(Xe(),Pg())}function hr(n){let e=fM();return eM(e,Mn+n)}function P(n,e=""){let t=Xe(),i=Wt(),r=n+Mn,s=i.firstCreatePass?sl(i,r,1,e,null):i.data[r],o=FS(i,t,s,e,n);t[r]=o,_d()&&Nd(i,t,o,s),ho(s,!1)}var FS=(n,e,t,i,r)=>(xd(!0),cE(e[Ft],i));function _t(n){return li("",n,""),_t}function li(n,e,t){let i=Xe(),r=lS(i,n,e,t);return r!==ai&&Ov(i,Ni(),r),li}function Gd(n,e,t,i,r){let s=Xe(),o=cS(s,n,e,t,i,r);return o!==ai&&Ov(s,Ni(),o),Gd}function ul(n,e,t){Yv(e)&&(e=e());let i=Xe(),r=ls();if(ii(i,r,e)){let s=Wt(),o=yd();Cv(s,o,i,n,e,i[Ft],t,!1)}return ul}function Wd(n,e){let t=Yv(n);return t&&n.set(e),t}function dl(n,e){let t=Xe(),i=Wt(),r=nn();return Jv(i,t,t[Ft],r,n,e),dl}function OS(n,e,t){let i=Wt();if(i.firstCreatePass){let r=Ii(n);nd(t,i.data,i.blueprint,r,!0),nd(e,i.data,i.blueprint,r,!1)}}function nd(n,e,t,i,r){if(n=Yt(n),Array.isArray(n))for(let s=0;s<n.length;s++)nd(n[s],e,t,i,r);else{let s=Wt(),o=Xe(),a=nn(),l=Kr(n)?n:Yt(n.provide),c=mg(n),u=a.providerIndexes&1048575,d=a.directiveStart,h=a.providerIndexes>>20;if(Kr(n)||!n.multi){let f=new ar(c,r,Mt),g=uu(l,e,r?u:u+h,d);g===-1?(bu(Ta(a,o),s,l),cu(s,n,e.length),e.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(f),o.push(f)):(t[g]=f,o[g]=f)}else{let f=uu(l,e,u+h,d),g=uu(l,e,u,u+h),_=f>=0&&t[f],m=g>=0&&t[g];if(r&&!m||!r&&!_){bu(Ta(a,o),s,l);let p=US(r?kS:LS,t.length,r,i,c);!r&&m&&(t[g].providerFactory=p),cu(s,n,e.length,0),e.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(p),o.push(p)}else{let p=ny(t[r?g:f],c,!r&&i);cu(s,n,f>-1?f:g,p)}!r&&i&&m&&t[g].componentProviders++}}}function cu(n,e,t,i){let r=Kr(e),s=Bx(e);if(r||s){let l=(s?Yt(e.useClass):e).prototype.ngOnDestroy;if(l){let c=n.destroyHooks||(n.destroyHooks=[]);if(!r&&e.multi){let u=c.indexOf(t);u===-1?c.push(t,[i,l]):c[u+1].push(i,l)}else c.push(t,l)}}}function ny(n,e,t){return t&&n.componentProviders++,n.multi.push(e)-1}function uu(n,e,t,i){for(let r=t;r<i;r++)if(e[r]===n)return r;return-1}function LS(n,e,t,i){return id(this.multi,[])}function kS(n,e,t,i){let r=this.multi,s;if(this.providerFactory){let o=this.providerFactory.componentProviders,a=lr(t,t[Ne],this.providerFactory.index,i);s=a.slice(0,o),id(r,s);for(let l=o;l<a.length;l++)s.push(a[l])}else s=[],id(r,s);return s}function id(n,e){for(let t=0;t<n.length;t++){let i=n[t];e.push(i())}return e}function US(n,e,t,i,r){let s=new ar(n,t,Mt);return s.multi=[],s.index=e,s.componentProviders=0,ny(s,r,i&&!t),s}function fr(n,e=[]){return t=>{t.providersResolver=(i,r)=>OS(i,r?r(n):n,e)}}var VS=(()=>{class n{constructor(t){this._injector=t,this.cachedInjectors=new Map}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=hg(!1,t.type),r=i.length>0?tS([i],this._injector,`Standalone[${t.type.name}]`):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static{this.\u0275prov=Dt({token:n,providedIn:"environment",factory:()=>new n(ft(Ti))})}}return n})();function fs(n){hs("NgStandalone"),n.getStandaloneInjector=e=>e.get(VS).getOrCreateStandaloneInjector(n)}var iy=new He("");function fo(n){return!!n&&typeof n.then=="function"}function ry(n){return!!n&&typeof n.subscribe=="function"}var BS=new He(""),sy=(()=>{class n{constructor(){this.initialized=!1,this.done=!1,this.donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i}),this.appInits=rt(BS,{optional:!0})??[]}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=r();if(fo(s))t.push(s);else if(ry(s)){let o=new Promise((a,l)=>{s.subscribe({complete:a,error:l})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=Dt({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})(),HS=new He("");function zS(){Vp(()=>{throw new We(600,!1)})}function GS(n){return n.isBoundToModule}var WS=10;function jS(n,e,t){try{let i=t();return fo(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n.handleError(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n.handleError(i)),i}}var po=(()=>{class n{constructor(){this._bootstrapListeners=[],this._runningTick=!1,this._destroyed=!1,this._destroyListeners=[],this._views=[],this.internalErrorHandler=rt($M),this.afterRenderManager=rt(aS),this.zonelessEnabled=rt(Vd),this.dirtyFlags=0,this.deferredDirtyFlags=0,this.externalTestViews=new Set,this.beforeRender=new Bn,this.afterTick=new Bn,this.componentTypes=[],this.components=[],this.isStable=rt(Qa).hasPendingTasks.pipe(tr(t=>!t)),this._injector=rt(Ti)}get allViews(){return[...this.externalTestViews.keys(),...this._views]}get destroyed(){return this._destroyed}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}get injector(){return this._injector}bootstrap(t,i){let r=t instanceof La;if(!this._injector.get(sy).done){let h=!r&&Fx(t),f=!1;throw new We(405,f)}let o;r?o=t:o=this._injector.get(lo).resolveComponentFactory(t),this.componentTypes.push(o.componentType);let a=GS(o)?void 0:this._injector.get(rs),l=i||o.selector,c=o.create(ts.NULL,[],l,a),u=c.location.nativeElement,d=c.injector.get(iy,null);return d?.registerApplication(u),c.onDestroy(()=>{this.detachView(c.hostView),va(this.components,c),d?.unregisterApplication(u)}),this._loadComponent(c),c}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){if(this._runningTick)throw new We(101,!1);let t=Ve(null);try{this._runningTick=!0,this.synchronize()}catch(i){this.internalErrorHandler(i)}finally{this._runningTick=!1,Ve(t),this.afterTick.next()}}synchronize(){let t=null;this._injector.destroyed||(t=this._injector.get(is,null,{optional:!0})),this.dirtyFlags|=this.deferredDirtyFlags,this.deferredDirtyFlags=0;let i=0;for(;this.dirtyFlags!==0&&i++<WS;)this.synchronizeOnce(t)}synchronizeOnce(t){if(this.dirtyFlags|=this.deferredDirtyFlags,this.deferredDirtyFlags=0,this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8,this.beforeRender.next(i);for(let{_lView:r,notifyErrorHandler:s}of this._views)$S(r,s,i,this.zonelessEnabled);if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&7)return}else t?.begin?.(),t?.end?.();this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>Ka(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;va(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView),this.tick(),this.components.push(t);let i=this._injector.get(HS,[]);[...this._bootstrapListeners,...i].forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._bootstrapListeners=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>va(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new We(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}warnIfDestroyed(){}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=Dt({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})();function va(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function $S(n,e,t,i){if(!t&&!Ka(n))return;Bv(n,e,t&&!i?0:1)}var qS=(()=>{class n{constructor(){this.zone=rt(St),this.changeDetectionScheduler=rt(ns),this.applicationRef=rt(po)}initialize(){this._onMicrotaskEmptySubscription||(this._onMicrotaskEmptySubscription=this.zone.onMicrotaskEmpty.subscribe({next:()=>{this.changeDetectionScheduler.runningTick||this.zone.run(()=>{this.applicationRef.tick()})}}))}ngOnDestroy(){this._onMicrotaskEmptySubscription?.unsubscribe()}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=Dt({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})();function XS({ngZoneFactory:n,ignoreChangesOutsideZone:e,scheduleInRootZone:t}){return n??=()=>new St(bt(vt({},YS()),{scheduleInRootZone:t})),[{provide:St,useFactory:n},{provide:to,multi:!0,useFactory:()=>{let i=rt(qS,{optional:!0});return()=>i.initialize()}},{provide:to,multi:!0,useFactory:()=>{let i=rt(ZS);return()=>{i.initialize()}}},e===!0?{provide:Wv,useValue:!0}:[],{provide:jv,useValue:t??Zg}]}function YS(n){return{enableLongStackTrace:!1,shouldCoalesceEventChangeDetection:n?.eventCoalescing??!1,shouldCoalesceRunChangeDetection:n?.runCoalescing??!1}}var ZS=(()=>{class n{constructor(){this.subscription=new Xt,this.initialized=!1,this.zone=rt(St),this.pendingTasks=rt(Qa)}initialize(){if(this.initialized)return;this.initialized=!0;let t=null;!this.zone.isStable&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(t=this.pendingTasks.add()),this.zone.runOutsideAngular(()=>{this.subscription.add(this.zone.onStable.subscribe(()=>{St.assertNotInAngularZone(),queueMicrotask(()=>{t!==null&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(this.pendingTasks.remove(t),t=null)})}))}),this.subscription.add(this.zone.onUnstable.subscribe(()=>{St.assertInAngularZone(),t??=this.pendingTasks.add()}))}ngOnDestroy(){this.subscription.unsubscribe()}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=Dt({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})();var KS=(()=>{class n{constructor(){this.appRef=rt(po),this.taskService=rt(Qa),this.ngZone=rt(St),this.zonelessEnabled=rt(Vd),this.disableScheduling=rt(Wv,{optional:!0})??!1,this.zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run,this.schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}],this.subscriptions=new Xt,this.angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Ra):null,this.scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(rt(jv,{optional:!0})??!1),this.cancelScheduledCallback=null,this.useMicrotaskScheduler=!1,this.runningTick=!1,this.pendingRenderTaskId=null,this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{this.runningTick||this.cleanup()})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()})),this.disableScheduling||=!this.zonelessEnabled&&(this.ngZone instanceof Au||!this.zoneIsDefined)}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 7:{this.appRef.deferredDirtyFlags|=8;break}case 9:case 8:case 6:case 10:default:this.appRef.dirtyFlags|=8}if(!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?Rm:Jg;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.disableScheduling||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Ra+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){throw this.taskService.remove(t),i}finally{this.cleanup()}this.useMicrotaskScheduler=!0,Rm(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=Dt({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})();function JS(){return typeof $localize<"u"&&$localize.locale||za}var jd=new He("",{providedIn:"root",factory:()=>rt(jd,Be.Optional|Be.SkipSelf)||JS()});var rd=new He("");function ha(n){return!n.moduleRef}function QS(n){let e=ha(n)?n.r3Injector:n.moduleRef.injector,t=e.get(St);return t.run(()=>{ha(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(ti,null),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:s=>{i.handleError(s)}})}),ha(n)){let s=()=>e.destroy(),o=n.platformInjector.get(rd);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(rd);o.add(s),n.moduleRef.onDestroy(()=>{va(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return jS(i,t,()=>{let s=e.get(sy);return s.runInitializers(),s.donePromise.then(()=>{let o=e.get(jd,za);if(RS(o||za),ha(n)){let a=e.get(po);return n.rootComponent!==void 0&&a.bootstrap(n.rootComponent),a}else return ew(n.moduleRef,n.allPlatformModules),n.moduleRef})})})}function ew(n,e){let t=n.injector.get(po);if(n._bootstrapComponents.length>0)n._bootstrapComponents.forEach(i=>t.bootstrap(i));else if(n.instance.ngDoBootstrap)n.instance.ngDoBootstrap(t);else throw new We(-403,!1);e.push(n)}var ya=null,oy=new He("");function tw(n=[],e){return ts.create({name:e,providers:[{provide:qa,useValue:"platform"},{provide:rd,useValue:new Set([()=>ya=null])},...n]})}function nw(n=[]){if(ya)return ya;let e=tw(n);return e.get(oy,!1)||(ya=e),zS(),iw(e),e}function iw(n){n.get(Cd,null)?.forEach(t=>t())}var $d=(()=>{class n{static{this.__NG_ELEMENT_ID__=rw}}return n})();function rw(n){return sw(nn(),Xe(),(n&16)===16)}function sw(n,e,t){if(Ya(n)&&!t){let i=Ri(n.index,e);return new ur(i,i)}else if(n.type&175){let i=e[Rn];return new ur(i,e)}return null}function ay(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;try{let s=r?.injector??nw(i);if(s.get(oy,!1)===!0&&!n.platformRef)throw new We(401,!1);let o=[XS({}),{provide:ns,useExisting:KS},...t||[]],a=new Ua({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return QS({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}}function hl(n){return typeof n=="boolean"?n:n!=null&&n!=="false"}function fl(n,e){hs("NgSignals");let t=Lp(n);return e?.equal&&(t[Qn].equal=e.equal),t}function Fi(n){let e=Ve(null);try{return n()}finally{Ve(e)}}var cy=null;function ms(){return cy}function uy(n){cy??=n}var pl=class{};var Oi=new He("");function dy(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var hy="browser",ow="server";function qd(n){return n===ow}var ml=class{};var Zd=class extends pl{constructor(){super(...arguments),this.supportsDOMEvents=!0}},Kd=class n extends Zd{static makeCurrent(){uy(new n)}onAndCancel(e,t,i){return e.addEventListener(t,i),()=>{e.removeEventListener(t,i)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=lw();return t==null?null:cw(t)}resetBaseElement(){mo=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return dy(document.cookie,e)}},mo=null;function lw(){return mo=mo||document.querySelector("base"),mo?mo.getAttribute("href"):null}function cw(n){return new URL(n,document.baseURI).pathname}var uw=(()=>{class n{build(){return new XMLHttpRequest}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=Dt({token:n,factory:n.\u0275fac})}}return n})(),Jd=new He(""),gy=(()=>{class n{constructor(t,i){this._zone=i,this._eventNameToPlugin=new Map,t.forEach(r=>{r.manager=this}),this._plugins=t.slice().reverse()}addEventListener(t,i,r){return this._findPluginFor(i).addEventListener(t,i,r)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new We(5101,!1);return this._eventNameToPlugin.set(t,i),i}static{this.\u0275fac=function(i){return new(i||n)(ft(Jd),ft(St))}}static{this.\u0275prov=Dt({token:n,factory:n.\u0275fac})}}return n})(),gl=class{constructor(e){this._doc=e}},Xd="ng-app-id",vy=(()=>{class n{constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,this.platformId=s,this.styleRef=new Map,this.hostNodes=new Set,this.styleNodesInDOM=this.collectServerRenderedStyles(),this.platformIsServer=qd(s),this.resetHostNodes()}addStyles(t){for(let i of t)this.changeUsageCount(i,1)===1&&this.onStyleAdded(i)}removeStyles(t){for(let i of t)this.changeUsageCount(i,-1)<=0&&this.onStyleRemoved(i)}ngOnDestroy(){let t=this.styleNodesInDOM;t&&(t.forEach(i=>i.remove()),t.clear());for(let i of this.getAllStyles())this.onStyleRemoved(i);this.resetHostNodes()}addHost(t){this.hostNodes.add(t);for(let i of this.getAllStyles())this.addStyleToHost(t,i)}removeHost(t){this.hostNodes.delete(t)}getAllStyles(){return this.styleRef.keys()}onStyleAdded(t){for(let i of this.hostNodes)this.addStyleToHost(i,t)}onStyleRemoved(t){let i=this.styleRef;i.get(t)?.elements?.forEach(r=>r.remove()),i.delete(t)}collectServerRenderedStyles(){let t=this.doc.head?.querySelectorAll(`style[${Xd}="${this.appId}"]`);if(t?.length){let i=new Map;return t.forEach(r=>{r.textContent!=null&&i.set(r.textContent,r)}),i}return null}changeUsageCount(t,i){let r=this.styleRef;if(r.has(t)){let s=r.get(t);return s.usage+=i,s.usage}return r.set(t,{usage:i,elements:[]}),i}getStyleElement(t,i){let r=this.styleNodesInDOM,s=r?.get(i);if(s?.parentNode===t)return r.delete(i),s.removeAttribute(Xd),s;{let o=this.doc.createElement("style");return this.nonce&&o.setAttribute("nonce",this.nonce),o.textContent=i,this.platformIsServer&&o.setAttribute(Xd,this.appId),t.appendChild(o),o}}addStyleToHost(t,i){let r=this.getStyleElement(t,i),s=this.styleRef,o=s.get(i)?.elements;o?o.push(r):s.set(i,{elements:[r],usage:1})}resetHostNodes(){let t=this.hostNodes;t.clear(),t.add(this.doc.head)}static{this.\u0275fac=function(i){return new(i||n)(ft(Oi),ft(wd),ft(Dd,8),ft(ds))}}static{this.\u0275prov=Dt({token:n,factory:n.\u0275fac})}}return n})(),Yd={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},eh=/%COMP%/g,yy="%COMP%",dw=`_nghost-${yy}`,hw=`_ngcontent-${yy}`,fw=!0,pw=new He("",{providedIn:"root",factory:()=>fw});function mw(n){return hw.replace(eh,n)}function gw(n){return dw.replace(eh,n)}function _y(n,e){return e.map(t=>t.replace(eh,n))}var fy=(()=>{class n{constructor(t,i,r,s,o,a,l,c=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.platformId=a,this.ngZone=l,this.nonce=c,this.rendererByCompId=new Map,this.platformIsServer=qd(a),this.defaultRenderer=new go(t,o,l,this.platformIsServer)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;this.platformIsServer&&i.encapsulation===jn.ShadowDom&&(i=bt(vt({},i),{encapsulation:jn.Emulated}));let r=this.getOrCreateRenderer(t,i);return r instanceof vl?r.applyToHost(t):r instanceof vo&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.platformIsServer;switch(i.encapsulation){case jn.Emulated:s=new vl(l,c,i,this.appId,u,o,a,d);break;case jn.ShadowDom:return new Qd(l,c,t,i,o,a,this.nonce,d);default:s=new vo(l,c,i,u,o,a,d);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}static{this.\u0275fac=function(i){return new(i||n)(ft(gy),ft(vy),ft(wd),ft(pw),ft(Oi),ft(ds),ft(St),ft(Dd))}}static{this.\u0275prov=Dt({token:n,factory:n.\u0275fac})}}return n})(),go=class{constructor(e,t,i,r){this.eventManager=e,this.doc=t,this.ngZone=i,this.platformIsServer=r,this.data=Object.create(null),this.throwOnSyntheticProps=!0,this.destroyNode=null}destroy(){}createElement(e,t){return t?this.doc.createElementNS(Yd[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(py(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(py(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new We(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=Yd[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=Yd[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(ni.DashCase|ni.Important)?e.style.setProperty(t,i,r&ni.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&ni.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i){if(typeof e=="string"&&(e=ms().getGlobalEventTarget(this.doc,e),!e))throw new Error(`Unsupported event target ${e} for event ${t}`);return this.eventManager.addEventListener(e,t,this.decoratePreventDefault(i))}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;(this.platformIsServer?this.ngZone.runGuarded(()=>e(t)):e(t))===!1&&t.preventDefault()}}};function py(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var Qd=class extends go{constructor(e,t,i,r,s,o,a,l){super(e,s,o,l),this.sharedStylesHost=t,this.hostEl=i,this.shadowRoot=i.attachShadow({mode:"open"}),this.sharedStylesHost.addHost(this.shadowRoot);let c=_y(r.id,r.styles);for(let u of c){let d=document.createElement("style");a&&d.setAttribute("nonce",a),d.textContent=u,this.shadowRoot.appendChild(d)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost.removeHost(this.shadowRoot)}},vo=class extends go{constructor(e,t,i,r,s,o,a,l){super(e,s,o,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r,this.styles=l?_y(l,i.styles):i.styles}applyStyles(){this.sharedStylesHost.addStyles(this.styles)}destroy(){this.removeStylesOnCompDestroy&&this.sharedStylesHost.removeStyles(this.styles)}},vl=class extends vo{constructor(e,t,i,r,s,o,a,l){let c=r+"-"+i.id;super(e,t,i,s,o,a,l,c),this.contentAttr=mw(c),this.hostAttr=gw(c)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}},vw=(()=>{class n extends gl{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r){return t.addEventListener(i,r,!1),()=>this.removeEventListener(t,i,r)}removeEventListener(t,i,r){return t.removeEventListener(i,r)}static{this.\u0275fac=function(i){return new(i||n)(ft(Oi))}}static{this.\u0275prov=Dt({token:n,factory:n.\u0275fac})}}return n})(),my=["alt","control","meta","shift"],yw={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},_w={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},xw=(()=>{class n extends gl{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r){let s=n.parseEventName(i),o=n.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>ms().onAndCancel(t,s.domEventName,o))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),my.forEach(c=>{let u=i.indexOf(c);u>-1&&(i.splice(u,1),o+=c+".")}),o+=s,i.length!=0||s.length===0)return null;let l={};return l.domEventName=r,l.fullKey=o,l}static matchEventFullKeyCode(t,i){let r=yw[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),my.forEach(o=>{if(o!==r){let a=_w[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static{this.\u0275fac=function(i){return new(i||n)(ft(Oi))}}static{this.\u0275prov=Dt({token:n,factory:n.\u0275fac})}}return n})();function xy(n,e,t){return ay(vt({rootComponent:n,platformRef:t?.platformRef},Mw(e)))}function Mw(n){return{appProviders:[...Cw,...n?.providers??[]],platformProviders:ww}}function Ew(){Kd.makeCurrent()}function bw(){return new ti}function Sw(){return sv(document),document}var ww=[{provide:ds,useValue:hy},{provide:Cd,useValue:Ew,multi:!0},{provide:Oi,useFactory:Sw,deps:[]}];var Cw=[{provide:qa,useValue:"root"},{provide:ti,useFactory:bw,deps:[]},{provide:Jd,useClass:vw,multi:!0,deps:[Oi,St,ds]},{provide:Jd,useClass:xw,multi:!0,deps:[Oi]},fy,vy,gy,{provide:is,useExisting:fy},{provide:ml,useClass:uw,deps:[]},[]];var Xf="169";var Dw=0,My=1,Aw=2;var x0=1,Tw=2,pi=3,ji=0,ln=1,gi=2,Gi=0,Rs=1,Ey=2,by=3,Sy=4,Iw=5,Mr=100,Rw=101,Nw=102,Pw=103,Fw=104,Ow=200,Lw=201,kw=202,Uw=203,Ph=204,Fh=205,Vw=206,Bw=207,Hw=208,zw=209,Gw=210,Ww=211,jw=212,$w=213,qw=214,Oh=0,Lh=1,kh=2,Os=3,Uh=4,Vh=5,Bh=6,Hh=7,M0=0,Xw=1,Yw=2,Wi=0,Zw=1,Kw=2,Jw=3,Qw=4,eC=5,tC=6,nC=7;var wy=300,Ls=301,ks=302,zh=303,Gh=304,gc=306,Wh=1e3,Sr=1001,jh=1002,wn=1003,iC=1004;var yl=1005;var kn=1006,th=1007;var wr=1008;var Mi=1009,E0=1010,b0=1011,Co=1012,Yf=1013,Cr=1014,vi=1015,Io=1016,Zf=1017,Kf=1018,Us=1020,S0=35902,w0=1021,C0=1022,Un=1023,D0=1024,A0=1025,Ns=1026,Vs=1027,T0=1028,Jf=1029,I0=1030,Qf=1031;var ep=1033,Bl=33776,Hl=33777,zl=33778,Gl=33779,$h=35840,qh=35841,Xh=35842,Yh=35843,Zh=36196,Kh=37492,Jh=37496,Qh=37808,ef=37809,tf=37810,nf=37811,rf=37812,sf=37813,of=37814,af=37815,lf=37816,cf=37817,uf=37818,df=37819,hf=37820,ff=37821,Wl=36492,pf=36494,mf=36495,R0=36283,gf=36284,vf=36285,yf=36286;var $l=2300,_f=2301,nh=2302,Cy=2400,Dy=2401,Ay=2402;var rC=3200,sC=3201;var N0=0,oC=1,zi="",Yn="srgb",Yi="srgb-linear",tp="display-p3",vc="display-p3-linear",ql="linear",xt="srgb",Xl="rec709",Yl="p3";var gs=7680;var Ty=519,aC=512,lC=513,cC=514,P0=515,uC=516,dC=517,hC=518,fC=519,Iy=35044;var Ry="300 es",yi=2e3,Zl=2001,$i=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){if(this._listeners===void 0)return!1;let i=this._listeners;return i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){if(this._listeners===void 0)return;let r=this._listeners[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){if(this._listeners===void 0)return;let i=this._listeners[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},$t=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"];var ih=Math.PI/180,xf=180/Math.PI;function Ro(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return($t[n&255]+$t[n>>8&255]+$t[n>>16&255]+$t[n>>24&255]+"-"+$t[e&255]+$t[e>>8&255]+"-"+$t[e>>16&15|64]+$t[e>>24&255]+"-"+$t[t&63|128]+$t[t>>8&255]+"-"+$t[t>>16&255]+$t[t>>24&255]+$t[i&255]+$t[i>>8&255]+$t[i>>16&255]+$t[i>>24&255]).toLowerCase()}function on(n,e,t){return Math.max(e,Math.min(t,n))}function pC(n,e){return(n%e+e)%e}function rh(n,e,t){return(1-t)*n+t*e}function yo(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function sn(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var ot=class n{constructor(e=0,t=0){n.prototype.isVector2=!0,this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(on(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},ke=class n{constructor(e,t,i,r,s,o,a,l,c){n.prototype.isMatrix3=!0,this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,l,c)}set(e,t,i,r,s,o,a,l,c){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=l,u[6]=i,u[7]=o,u[8]=c,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],l=i[6],c=i[1],u=i[4],d=i[7],h=i[2],f=i[5],g=i[8],_=r[0],m=r[3],p=r[6],w=r[1],E=r[4],S=r[7],k=r[2],I=r[5],C=r[8];return s[0]=o*_+a*w+l*k,s[3]=o*m+a*E+l*I,s[6]=o*p+a*S+l*C,s[1]=c*_+u*w+d*k,s[4]=c*m+u*E+d*I,s[7]=c*p+u*S+d*C,s[2]=h*_+f*w+g*k,s[5]=h*m+f*E+g*I,s[8]=h*p+f*S+g*C,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],l=e[6],c=e[7],u=e[8];return t*o*u-t*a*c-i*s*u+i*a*l+r*s*c-r*o*l}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],l=e[6],c=e[7],u=e[8],d=u*o-a*c,h=a*l-u*s,f=c*s-o*l,g=t*d+i*h+r*f;if(g===0)return this.set(0,0,0,0,0,0,0,0,0);let _=1/g;return e[0]=d*_,e[1]=(r*c-u*i)*_,e[2]=(a*i-r*o)*_,e[3]=h*_,e[4]=(u*t-r*l)*_,e[5]=(r*s-a*t)*_,e[6]=f*_,e[7]=(i*l-c*t)*_,e[8]=(o*t-i*s)*_,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let l=Math.cos(s),c=Math.sin(s);return this.set(i*l,i*c,-i*(l*o+c*a)+o+e,-r*c,r*l,-r*(-c*o+l*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(sh.makeScale(e,t)),this}rotate(e){return this.premultiply(sh.makeRotation(-e)),this}translate(e,t){return this.premultiply(sh.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},sh=new ke;function F0(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function Kl(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function mC(){let n=Kl("canvas");return n.style.display="block",n}var Ny={};function jl(n){n in Ny||(Ny[n]=!0,console.warn(n))}function gC(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}function vC(n){let e=n.elements;e[2]=.5*e[2]+.5*e[3],e[6]=.5*e[6]+.5*e[7],e[10]=.5*e[10]+.5*e[11],e[14]=.5*e[14]+.5*e[15]}function yC(n){let e=n.elements;e[11]===-1?(e[10]=-e[10]-1,e[14]=-e[14]):(e[10]=-e[10],e[14]=-e[14]+1)}var Py=new ke().set(.8224621,.177538,0,.0331941,.9668058,0,.0170827,.0723974,.9105199),Fy=new ke().set(1.2249401,-.2249404,0,-.0420569,1.0420571,0,-.0196376,-.0786361,1.0982735),_o={[Yi]:{transfer:ql,primaries:Xl,luminanceCoefficients:[.2126,.7152,.0722],toReference:n=>n,fromReference:n=>n},[Yn]:{transfer:xt,primaries:Xl,luminanceCoefficients:[.2126,.7152,.0722],toReference:n=>n.convertSRGBToLinear(),fromReference:n=>n.convertLinearToSRGB()},[vc]:{transfer:ql,primaries:Yl,luminanceCoefficients:[.2289,.6917,.0793],toReference:n=>n.applyMatrix3(Fy),fromReference:n=>n.applyMatrix3(Py)},[tp]:{transfer:xt,primaries:Yl,luminanceCoefficients:[.2289,.6917,.0793],toReference:n=>n.convertSRGBToLinear().applyMatrix3(Fy),fromReference:n=>n.applyMatrix3(Py).convertLinearToSRGB()}},_C=new Set([Yi,vc]),st={enabled:!0,_workingColorSpace:Yi,get workingColorSpace(){return this._workingColorSpace},set workingColorSpace(n){if(!_C.has(n))throw new Error(`Unsupported working color space, "${n}".`);this._workingColorSpace=n},convert:function(n,e,t){if(this.enabled===!1||e===t||!e||!t)return n;let i=_o[e].toReference,r=_o[t].fromReference;return r(i(n))},fromWorkingColorSpace:function(n,e){return this.convert(n,this._workingColorSpace,e)},toWorkingColorSpace:function(n,e){return this.convert(n,e,this._workingColorSpace)},getPrimaries:function(n){return _o[n].primaries},getTransfer:function(n){return n===zi?ql:_o[n].transfer},getLuminanceCoefficients:function(n,e=this._workingColorSpace){return n.fromArray(_o[e].luminanceCoefficients)}};function Ps(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function oh(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var vs,Mf=class{static getDataURL(e){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let t;if(e instanceof HTMLCanvasElement)t=e;else{vs===void 0&&(vs=Kl("canvas")),vs.width=e.width,vs.height=e.height;let i=vs.getContext("2d");e instanceof ImageData?i.putImageData(e,0,0):i.drawImage(e,0,0,e.width,e.height),t=vs}return t.width>2048||t.height>2048?(console.warn("THREE.ImageUtils.getDataURL: Image converted to jpg for performance reasons",e),t.toDataURL("image/jpeg",.6)):t.toDataURL("image/png")}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=Kl("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=Ps(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(Ps(t[i]/255)*255):t[i]=Ps(t[i]);return{data:t,width:e.width,height:e.height}}else return console.warn("THREE.ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},xC=0,Jl=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:xC++}),this.uuid=Ro(),this.data=e,this.dataReady=!0,this.version=0}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(ah(r[o].image)):s.push(ah(r[o]))}else s=ah(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function ah(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?Mf.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(console.warn("THREE.Texture: Unable to serialize Texture."),{})}var MC=0,Pr=(()=>{class n extends $i{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=Sr,s=Sr,o=kn,a=wr,l=Un,c=Mi,u=n.DEFAULT_ANISOTROPY,d=zi){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:MC++}),this.uuid=Ro(),this.name="",this.source=new Jl(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=l,this.internalFormat=null,this.type=c,this.offset=new ot(0,0),this.repeat=new ot(1,1),this.center=new ot(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new ke,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=d,this.userData={},this.version=0,this.onUpdate=null,this.isRenderTargetTexture=!1,this.pmremVersion=0}get image(){return this.source.data}set image(t=null){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.6,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==wy)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case Wh:t.x=t.x-Math.floor(t.x);break;case Sr:t.x=t.x<0?0:1;break;case jh:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case Wh:t.y=t.y-Math.floor(t.y);break;case Sr:t.y=t.y<0?0:1;break;case jh:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=wy,n.DEFAULT_ANISOTROPY=1,n})(),wt=class n{constructor(e=0,t=0,i=0,r=1){n.prototype.isVector4=!0,this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,l=e.elements,c=l[0],u=l[4],d=l[8],h=l[1],f=l[5],g=l[9],_=l[2],m=l[6],p=l[10];if(Math.abs(u-h)<.01&&Math.abs(d-_)<.01&&Math.abs(g-m)<.01){if(Math.abs(u+h)<.1&&Math.abs(d+_)<.1&&Math.abs(g+m)<.1&&Math.abs(c+f+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let E=(c+1)/2,S=(f+1)/2,k=(p+1)/2,I=(u+h)/4,C=(d+_)/4,V=(g+m)/4;return E>S&&E>k?E<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(E),r=I/i,s=C/i):S>k?S<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(S),i=I/r,s=V/r):k<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(k),i=C/s,r=V/s),this.set(i,r,s,t),this}let w=Math.sqrt((m-g)*(m-g)+(d-_)*(d-_)+(h-u)*(h-u));return Math.abs(w)<.001&&(w=1),this.x=(m-g)/w,this.y=(d-_)/w,this.z=(h-u)/w,this.w=Math.acos((c+f+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this.z=Math.max(e.z,Math.min(t.z,this.z)),this.w=Math.max(e.w,Math.min(t.w,this.w)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this.z=Math.max(e,Math.min(t,this.z)),this.w=Math.max(e,Math.min(t,this.w)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},Ef=class extends $i{constructor(e=1,t=1,i={}){super(),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=1,this.scissor=new wt(0,0,e,t),this.scissorTest=!1,this.viewport=new wt(0,0,e,t);let r={width:e,height:t,depth:1};i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:kn,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1},i);let s=new Pr(r,i.mapping,i.wrapS,i.wrapT,i.magFilter,i.minFilter,i.format,i.type,i.anisotropy,i.colorSpace);s.flipY=!1,s.generateMipmaps=i.generateMipmaps,s.internalFormat=i.internalFormat,this.textures=[];let o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0;this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this.depthTexture=i.depthTexture,this.samples=i.samples}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i;this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let i=0,r=e.textures.length;i<r;i++)this.textures[i]=e.textures[i].clone(),this.textures[i].isRenderTargetTexture=!0;let t=Object.assign({},e.texture.image);return this.texture.source=new Jl(t),this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this}dispose(){this.dispatchEvent({type:"dispose"})}},Ei=class extends Ef{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},Ql=class extends Pr{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=wn,this.minFilter=wn,this.wrapR=Sr,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var bf=class extends Pr{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=wn,this.minFilter=wn,this.wrapR=Sr,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var qi=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let l=i[r+0],c=i[r+1],u=i[r+2],d=i[r+3],h=s[o+0],f=s[o+1],g=s[o+2],_=s[o+3];if(a===0){e[t+0]=l,e[t+1]=c,e[t+2]=u,e[t+3]=d;return}if(a===1){e[t+0]=h,e[t+1]=f,e[t+2]=g,e[t+3]=_;return}if(d!==_||l!==h||c!==f||u!==g){let m=1-a,p=l*h+c*f+u*g+d*_,w=p>=0?1:-1,E=1-p*p;if(E>Number.EPSILON){let k=Math.sqrt(E),I=Math.atan2(k,p*w);m=Math.sin(m*I)/k,a=Math.sin(a*I)/k}let S=a*w;if(l=l*m+h*S,c=c*m+f*S,u=u*m+g*S,d=d*m+_*S,m===1-a){let k=1/Math.sqrt(l*l+c*c+u*u+d*d);l*=k,c*=k,u*=k,d*=k}}e[t]=l,e[t+1]=c,e[t+2]=u,e[t+3]=d}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],l=i[r+1],c=i[r+2],u=i[r+3],d=s[o],h=s[o+1],f=s[o+2],g=s[o+3];return e[t]=a*g+u*d+l*f-c*h,e[t+1]=l*g+u*h+c*d-a*f,e[t+2]=c*g+u*f+a*h-l*d,e[t+3]=u*g-a*d-l*h-c*f,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,l=Math.sin,c=a(i/2),u=a(r/2),d=a(s/2),h=l(i/2),f=l(r/2),g=l(s/2);switch(o){case"XYZ":this._x=h*u*d+c*f*g,this._y=c*f*d-h*u*g,this._z=c*u*g+h*f*d,this._w=c*u*d-h*f*g;break;case"YXZ":this._x=h*u*d+c*f*g,this._y=c*f*d-h*u*g,this._z=c*u*g-h*f*d,this._w=c*u*d+h*f*g;break;case"ZXY":this._x=h*u*d-c*f*g,this._y=c*f*d+h*u*g,this._z=c*u*g+h*f*d,this._w=c*u*d-h*f*g;break;case"ZYX":this._x=h*u*d-c*f*g,this._y=c*f*d+h*u*g,this._z=c*u*g-h*f*d,this._w=c*u*d+h*f*g;break;case"YZX":this._x=h*u*d+c*f*g,this._y=c*f*d+h*u*g,this._z=c*u*g-h*f*d,this._w=c*u*d-h*f*g;break;case"XZY":this._x=h*u*d-c*f*g,this._y=c*f*d-h*u*g,this._z=c*u*g+h*f*d,this._w=c*u*d+h*f*g;break;default:console.warn("THREE.Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],l=t[9],c=t[2],u=t[6],d=t[10],h=i+a+d;if(h>0){let f=.5/Math.sqrt(h+1);this._w=.25/f,this._x=(u-l)*f,this._y=(s-c)*f,this._z=(o-r)*f}else if(i>a&&i>d){let f=2*Math.sqrt(1+i-a-d);this._w=(u-l)/f,this._x=.25*f,this._y=(r+o)/f,this._z=(s+c)/f}else if(a>d){let f=2*Math.sqrt(1+a-i-d);this._w=(s-c)/f,this._x=(r+o)/f,this._y=.25*f,this._z=(l+u)/f}else{let f=2*Math.sqrt(1+d-i-a);this._w=(o-r)/f,this._x=(s+c)/f,this._y=(l+u)/f,this._z=.25*f}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<Number.EPSILON?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(on(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,l=t._y,c=t._z,u=t._w;return this._x=i*u+o*a+r*c-s*l,this._y=r*u+o*l+s*a-i*c,this._z=s*u+o*c+i*l-r*a,this._w=o*u-i*a-r*l-s*c,this._onChangeCallback(),this}slerp(e,t){if(t===0)return this;if(t===1)return this.copy(e);let i=this._x,r=this._y,s=this._z,o=this._w,a=o*e._w+i*e._x+r*e._y+s*e._z;if(a<0?(this._w=-e._w,this._x=-e._x,this._y=-e._y,this._z=-e._z,a=-a):this.copy(e),a>=1)return this._w=o,this._x=i,this._y=r,this._z=s,this;let l=1-a*a;if(l<=Number.EPSILON){let f=1-t;return this._w=f*o+t*this._w,this._x=f*i+t*this._x,this._y=f*r+t*this._y,this._z=f*s+t*this._z,this.normalize(),this}let c=Math.sqrt(l),u=Math.atan2(c,a),d=Math.sin((1-t)*u)/c,h=Math.sin(t*u)/c;return this._w=o*d+this._w*h,this._x=i*d+this._x*h,this._y=r*d+this._y*h,this._z=s*d+this._z*h,this._onChangeCallback(),this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},B=class n{constructor(e=0,t=0,i=0){n.prototype.isVector3=!0,this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(Oy.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(Oy.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,l=e.w,c=2*(o*r-a*i),u=2*(a*t-s*r),d=2*(s*i-o*t);return this.x=t+l*c+o*d-a*u,this.y=i+l*u+a*c-s*d,this.z=r+l*d+s*u-o*c,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this.z=Math.max(e.z,Math.min(t.z,this.z)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this.z=Math.max(e,Math.min(t,this.z)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,l=t.z;return this.x=r*l-s*a,this.y=s*o-i*l,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return lh.copy(this).projectOnVector(e),this.sub(lh)}reflect(e){return this.sub(lh.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(on(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},lh=new B,Oy=new qi,Dr=class{constructor(e=new B(1/0,1/0,1/0),t=new B(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(Fn.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(Fn.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=Fn.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,Fn):Fn.fromBufferAttribute(s,o),Fn.applyMatrix4(e.matrixWorld),this.expandByPoint(Fn);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),_l.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),_l.copy(i.boundingBox)),_l.applyMatrix4(e.matrixWorld),this.union(_l)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,Fn),Fn.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(xo),xl.subVectors(this.max,xo),ys.subVectors(e.a,xo),_s.subVectors(e.b,xo),xs.subVectors(e.c,xo),Li.subVectors(_s,ys),ki.subVectors(xs,_s),pr.subVectors(ys,xs);let t=[0,-Li.z,Li.y,0,-ki.z,ki.y,0,-pr.z,pr.y,Li.z,0,-Li.x,ki.z,0,-ki.x,pr.z,0,-pr.x,-Li.y,Li.x,0,-ki.y,ki.x,0,-pr.y,pr.x,0];return!ch(t,ys,_s,xs,xl)||(t=[1,0,0,0,1,0,0,0,1],!ch(t,ys,_s,xs,xl))?!1:(Ml.crossVectors(Li,ki),t=[Ml.x,Ml.y,Ml.z],ch(t,ys,_s,xs,xl))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,Fn).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(Fn).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(ci[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),ci[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),ci[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),ci[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),ci[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),ci[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),ci[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),ci[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(ci),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}},ci=[new B,new B,new B,new B,new B,new B,new B,new B],Fn=new B,_l=new Dr,ys=new B,_s=new B,xs=new B,Li=new B,ki=new B,pr=new B,xo=new B,xl=new B,Ml=new B,mr=new B;function ch(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){mr.fromArray(n,s);let a=r.x*Math.abs(mr.x)+r.y*Math.abs(mr.y)+r.z*Math.abs(mr.z),l=e.dot(mr),c=t.dot(mr),u=i.dot(mr);if(Math.max(-Math.max(l,c,u),Math.min(l,c,u))>a)return!1}return!0}var EC=new Dr,Mo=new B,uh=new B,Do=class{constructor(e=new B,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):EC.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;Mo.subVectors(e,this.center);let t=Mo.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(Mo,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(uh.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(Mo.copy(e.center).add(uh)),this.expandByPoint(Mo.copy(e.center).sub(uh))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}},ui=new B,dh=new B,El=new B,Ui=new B,hh=new B,bl=new B,fh=new B,Sf=class{constructor(e=new B,t=new B(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,ui)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=ui.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(ui.copy(this.origin).addScaledVector(this.direction,t),ui.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){dh.copy(e).add(t).multiplyScalar(.5),El.copy(t).sub(e).normalize(),Ui.copy(this.origin).sub(dh);let s=e.distanceTo(t)*.5,o=-this.direction.dot(El),a=Ui.dot(this.direction),l=-Ui.dot(El),c=Ui.lengthSq(),u=Math.abs(1-o*o),d,h,f,g;if(u>0)if(d=o*l-a,h=o*a-l,g=s*u,d>=0)if(h>=-g)if(h<=g){let _=1/u;d*=_,h*=_,f=d*(d+o*h+2*a)+h*(o*d+h+2*l)+c}else h=s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*l)+c;else h=-s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*l)+c;else h<=-g?(d=Math.max(0,-(-o*s+a)),h=d>0?-s:Math.min(Math.max(-s,-l),s),f=-d*d+h*(h+2*l)+c):h<=g?(d=0,h=Math.min(Math.max(-s,-l),s),f=h*(h+2*l)+c):(d=Math.max(0,-(o*s+a)),h=d>0?s:Math.min(Math.max(-s,-l),s),f=-d*d+h*(h+2*l)+c);else h=o>0?-s:s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*l)+c;return i&&i.copy(this.origin).addScaledVector(this.direction,d),r&&r.copy(dh).addScaledVector(El,h),f}intersectSphere(e,t){ui.subVectors(e.center,this.origin);let i=ui.dot(this.direction),r=ui.dot(ui)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,l=i+o;return l<0?null:a<0?this.at(l,t):this.at(a,t)}intersectsSphere(e){return this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,l,c=1/this.direction.x,u=1/this.direction.y,d=1/this.direction.z,h=this.origin;return c>=0?(i=(e.min.x-h.x)*c,r=(e.max.x-h.x)*c):(i=(e.max.x-h.x)*c,r=(e.min.x-h.x)*c),u>=0?(s=(e.min.y-h.y)*u,o=(e.max.y-h.y)*u):(s=(e.max.y-h.y)*u,o=(e.min.y-h.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),d>=0?(a=(e.min.z-h.z)*d,l=(e.max.z-h.z)*d):(a=(e.max.z-h.z)*d,l=(e.min.z-h.z)*d),i>l||a>r)||((a>i||i!==i)&&(i=a),(l<r||r!==r)&&(r=l),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,ui)!==null}intersectTriangle(e,t,i,r,s){hh.subVectors(t,e),bl.subVectors(i,e),fh.crossVectors(hh,bl);let o=this.direction.dot(fh),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;Ui.subVectors(this.origin,e);let l=a*this.direction.dot(bl.crossVectors(Ui,bl));if(l<0)return null;let c=a*this.direction.dot(hh.cross(Ui));if(c<0||l+c>o)return null;let u=-a*Ui.dot(fh);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},It=class n{constructor(e,t,i,r,s,o,a,l,c,u,d,h,f,g,_,m){n.prototype.isMatrix4=!0,this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,l,c,u,d,h,f,g,_,m)}set(e,t,i,r,s,o,a,l,c,u,d,h,f,g,_,m){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=l,p[2]=c,p[6]=u,p[10]=d,p[14]=h,p[3]=f,p[7]=g,p[11]=_,p[15]=m,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){let t=this.elements,i=e.elements,r=1/Ms.setFromMatrixColumn(e,0).length(),s=1/Ms.setFromMatrixColumn(e,1).length(),o=1/Ms.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),l=Math.cos(r),c=Math.sin(r),u=Math.cos(s),d=Math.sin(s);if(e.order==="XYZ"){let h=o*u,f=o*d,g=a*u,_=a*d;t[0]=l*u,t[4]=-l*d,t[8]=c,t[1]=f+g*c,t[5]=h-_*c,t[9]=-a*l,t[2]=_-h*c,t[6]=g+f*c,t[10]=o*l}else if(e.order==="YXZ"){let h=l*u,f=l*d,g=c*u,_=c*d;t[0]=h+_*a,t[4]=g*a-f,t[8]=o*c,t[1]=o*d,t[5]=o*u,t[9]=-a,t[2]=f*a-g,t[6]=_+h*a,t[10]=o*l}else if(e.order==="ZXY"){let h=l*u,f=l*d,g=c*u,_=c*d;t[0]=h-_*a,t[4]=-o*d,t[8]=g+f*a,t[1]=f+g*a,t[5]=o*u,t[9]=_-h*a,t[2]=-o*c,t[6]=a,t[10]=o*l}else if(e.order==="ZYX"){let h=o*u,f=o*d,g=a*u,_=a*d;t[0]=l*u,t[4]=g*c-f,t[8]=h*c+_,t[1]=l*d,t[5]=_*c+h,t[9]=f*c-g,t[2]=-c,t[6]=a*l,t[10]=o*l}else if(e.order==="YZX"){let h=o*l,f=o*c,g=a*l,_=a*c;t[0]=l*u,t[4]=_-h*d,t[8]=g*d+f,t[1]=d,t[5]=o*u,t[9]=-a*u,t[2]=-c*u,t[6]=f*d+g,t[10]=h-_*d}else if(e.order==="XZY"){let h=o*l,f=o*c,g=a*l,_=a*c;t[0]=l*u,t[4]=-d,t[8]=c*u,t[1]=h*d+_,t[5]=o*u,t[9]=f*d-g,t[2]=g*d-f,t[6]=a*u,t[10]=_*d+h}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(bC,e,SC)}lookAt(e,t,i){let r=this.elements;return mn.subVectors(e,t),mn.lengthSq()===0&&(mn.z=1),mn.normalize(),Vi.crossVectors(i,mn),Vi.lengthSq()===0&&(Math.abs(i.z)===1?mn.x+=1e-4:mn.z+=1e-4,mn.normalize(),Vi.crossVectors(i,mn)),Vi.normalize(),Sl.crossVectors(mn,Vi),r[0]=Vi.x,r[4]=Sl.x,r[8]=mn.x,r[1]=Vi.y,r[5]=Sl.y,r[9]=mn.y,r[2]=Vi.z,r[6]=Sl.z,r[10]=mn.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],l=i[8],c=i[12],u=i[1],d=i[5],h=i[9],f=i[13],g=i[2],_=i[6],m=i[10],p=i[14],w=i[3],E=i[7],S=i[11],k=i[15],I=r[0],C=r[4],V=r[8],re=r[12],v=r[1],M=r[5],j=r[9],z=r[13],Y=r[2],J=r[6],W=r[10],K=r[14],G=r[3],de=r[7],he=r[11],xe=r[15];return s[0]=o*I+a*v+l*Y+c*G,s[4]=o*C+a*M+l*J+c*de,s[8]=o*V+a*j+l*W+c*he,s[12]=o*re+a*z+l*K+c*xe,s[1]=u*I+d*v+h*Y+f*G,s[5]=u*C+d*M+h*J+f*de,s[9]=u*V+d*j+h*W+f*he,s[13]=u*re+d*z+h*K+f*xe,s[2]=g*I+_*v+m*Y+p*G,s[6]=g*C+_*M+m*J+p*de,s[10]=g*V+_*j+m*W+p*he,s[14]=g*re+_*z+m*K+p*xe,s[3]=w*I+E*v+S*Y+k*G,s[7]=w*C+E*M+S*J+k*de,s[11]=w*V+E*j+S*W+k*he,s[15]=w*re+E*z+S*K+k*xe,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],l=e[9],c=e[13],u=e[2],d=e[6],h=e[10],f=e[14],g=e[3],_=e[7],m=e[11],p=e[15];return g*(+s*l*d-r*c*d-s*a*h+i*c*h+r*a*f-i*l*f)+_*(+t*l*f-t*c*h+s*o*h-r*o*f+r*c*u-s*l*u)+m*(+t*c*d-t*a*f-s*o*d+i*o*f+s*a*u-i*c*u)+p*(-r*a*u-t*l*d+t*a*h+r*o*d-i*o*h+i*l*u)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],l=e[6],c=e[7],u=e[8],d=e[9],h=e[10],f=e[11],g=e[12],_=e[13],m=e[14],p=e[15],w=d*m*c-_*h*c+_*l*f-a*m*f-d*l*p+a*h*p,E=g*h*c-u*m*c-g*l*f+o*m*f+u*l*p-o*h*p,S=u*_*c-g*d*c+g*a*f-o*_*f-u*a*p+o*d*p,k=g*d*l-u*_*l-g*a*h+o*_*h+u*a*m-o*d*m,I=t*w+i*E+r*S+s*k;if(I===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let C=1/I;return e[0]=w*C,e[1]=(_*h*s-d*m*s-_*r*f+i*m*f+d*r*p-i*h*p)*C,e[2]=(a*m*s-_*l*s+_*r*c-i*m*c-a*r*p+i*l*p)*C,e[3]=(d*l*s-a*h*s-d*r*c+i*h*c+a*r*f-i*l*f)*C,e[4]=E*C,e[5]=(u*m*s-g*h*s+g*r*f-t*m*f-u*r*p+t*h*p)*C,e[6]=(g*l*s-o*m*s-g*r*c+t*m*c+o*r*p-t*l*p)*C,e[7]=(o*h*s-u*l*s+u*r*c-t*h*c-o*r*f+t*l*f)*C,e[8]=S*C,e[9]=(g*d*s-u*_*s-g*i*f+t*_*f+u*i*p-t*d*p)*C,e[10]=(o*_*s-g*a*s+g*i*c-t*_*c-o*i*p+t*a*p)*C,e[11]=(u*a*s-o*d*s-u*i*c+t*d*c+o*i*f-t*a*f)*C,e[12]=k*C,e[13]=(u*_*r-g*d*r+g*i*h-t*_*h-u*i*m+t*d*m)*C,e[14]=(g*a*r-o*_*r-g*i*l+t*_*l+o*i*m-t*a*m)*C,e[15]=(o*d*r-u*a*r+u*i*l-t*d*l-o*i*h+t*a*h)*C,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,l=e.z,c=s*o,u=s*a;return this.set(c*o+i,c*a-r*l,c*l+r*a,0,c*a+r*l,u*a+i,u*l-r*o,0,c*l-r*a,u*l+r*o,s*l*l+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,l=t._w,c=s+s,u=o+o,d=a+a,h=s*c,f=s*u,g=s*d,_=o*u,m=o*d,p=a*d,w=l*c,E=l*u,S=l*d,k=i.x,I=i.y,C=i.z;return r[0]=(1-(_+p))*k,r[1]=(f+S)*k,r[2]=(g-E)*k,r[3]=0,r[4]=(f-S)*I,r[5]=(1-(h+p))*I,r[6]=(m+w)*I,r[7]=0,r[8]=(g+E)*C,r[9]=(m-w)*C,r[10]=(1-(h+_))*C,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements,s=Ms.set(r[0],r[1],r[2]).length(),o=Ms.set(r[4],r[5],r[6]).length(),a=Ms.set(r[8],r[9],r[10]).length();this.determinant()<0&&(s=-s),e.x=r[12],e.y=r[13],e.z=r[14],On.copy(this);let c=1/s,u=1/o,d=1/a;return On.elements[0]*=c,On.elements[1]*=c,On.elements[2]*=c,On.elements[4]*=u,On.elements[5]*=u,On.elements[6]*=u,On.elements[8]*=d,On.elements[9]*=d,On.elements[10]*=d,t.setFromRotationMatrix(On),i.x=s,i.y=o,i.z=a,this}makePerspective(e,t,i,r,s,o,a=yi){let l=this.elements,c=2*s/(t-e),u=2*s/(i-r),d=(t+e)/(t-e),h=(i+r)/(i-r),f,g;if(a===yi)f=-(o+s)/(o-s),g=-2*o*s/(o-s);else if(a===Zl)f=-o/(o-s),g=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=c,l[4]=0,l[8]=d,l[12]=0,l[1]=0,l[5]=u,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=f,l[14]=g,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=yi){let l=this.elements,c=1/(t-e),u=1/(i-r),d=1/(o-s),h=(t+e)*c,f=(i+r)*u,g,_;if(a===yi)g=(o+s)*d,_=-2*d;else if(a===Zl)g=s*d,_=-1*d;else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=2*c,l[4]=0,l[8]=0,l[12]=-h,l[1]=0,l[5]=2*u,l[9]=0,l[13]=-f,l[2]=0,l[6]=0,l[10]=_,l[14]=-g,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},Ms=new B,On=new It,bC=new B(0,0,0),SC=new B(1,1,1),Vi=new B,Sl=new B,mn=new B,Ly=new It,ky=new qi,Ar=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],l=s[8],c=s[1],u=s[5],d=s[9],h=s[2],f=s[6],g=s[10];switch(i){case"XYZ":this._y=Math.asin(on(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-d,g),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(f,u),this._z=0);break;case"YXZ":this._x=Math.asin(-on(d,-1,1)),Math.abs(d)<.9999999?(this._y=Math.atan2(l,g),this._z=Math.atan2(c,u)):(this._y=Math.atan2(-h,o),this._z=0);break;case"ZXY":this._x=Math.asin(on(f,-1,1)),Math.abs(f)<.9999999?(this._y=Math.atan2(-h,g),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(c,o));break;case"ZYX":this._y=Math.asin(-on(h,-1,1)),Math.abs(h)<.9999999?(this._x=Math.atan2(f,g),this._z=Math.atan2(c,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(on(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-d,u),this._y=Math.atan2(-h,o)):(this._x=0,this._y=Math.atan2(l,g));break;case"XZY":this._z=Math.asin(-on(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(f,u),this._y=Math.atan2(l,o)):(this._x=Math.atan2(-d,g),this._y=0);break;default:console.warn("THREE.Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return Ly.makeRotationFromQuaternion(t),this.setFromRotationMatrix(Ly,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return ky.setFromEuler(this),this.setFromQuaternion(ky,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),ec=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},wC=0,Uy=new B,Es=new qi,di=new It,wl=new B,Eo=new B,CC=new B,DC=new qi,Vy=new B(1,0,0),By=new B(0,1,0),Hy=new B(0,0,1),zy={type:"added"},AC={type:"removed"},bs={type:"childadded",child:null},ph={type:"childremoved",child:null},bi=(()=>{class n extends $i{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:wC++}),this.uuid=Ro(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new B,i=new Ar,r=new qi,s=new B(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new It},normalMatrix:{value:new ke}}),this.matrix=new It,this.matrixWorld=new It,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new ec,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.userData={}}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return Es.setFromAxisAngle(t,i),this.quaternion.multiply(Es),this}rotateOnWorldAxis(t,i){return Es.setFromAxisAngle(t,i),this.quaternion.premultiply(Es),this}rotateX(t){return this.rotateOnAxis(Vy,t)}rotateY(t){return this.rotateOnAxis(By,t)}rotateZ(t){return this.rotateOnAxis(Hy,t)}translateOnAxis(t,i){return Uy.copy(t).applyQuaternion(this.quaternion),this.position.add(Uy.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(Vy,t)}translateY(t){return this.translateOnAxis(By,t)}translateZ(t){return this.translateOnAxis(Hy,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(di.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?wl.copy(t):wl.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),Eo.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?di.lookAt(Eo,wl,this.up):di.lookAt(wl,Eo,this.up),this.quaternion.setFromRotationMatrix(di),s&&(di.extractRotation(s.matrixWorld),Es.setFromRotationMatrix(di),this.quaternion.premultiply(Es.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(console.error("THREE.Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(zy),bs.child=t,this.dispatchEvent(bs),bs.child=null):console.error("THREE.Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(AC),ph.child=t,this.dispatchEvent(ph),ph.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),di.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),di.multiply(t.parent.matrixWorld)),t.applyMatrix4(di),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(zy),bs.child=t,this.dispatchEvent(bs),bs.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Eo,t,CC),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Eo,DC,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale),this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.6,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.visibility=this._visibility,s.active=this._active,s.bounds=this._bounds.map(l=>({boxInitialized:l.boxInitialized,boxMin:l.box.min.toArray(),boxMax:l.box.max.toArray(),sphereInitialized:l.sphereInitialized,sphereRadius:l.sphere.radius,sphereCenter:l.sphere.center.toArray()})),s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.geometryCount=this._geometryCount,s.matricesTexture=this._matricesTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere={center:s.boundingSphere.center.toArray(),radius:s.boundingSphere.radius}),this.boundingBox!==null&&(s.boundingBox={min:s.boundingBox.min.toArray(),max:s.boundingBox.max.toArray()}));function o(l,c){return l[c.uuid]===void 0&&(l[c.uuid]=c.toJSON(t)),c.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let l=this.geometry.parameters;if(l!==void 0&&l.shapes!==void 0){let c=l.shapes;if(Array.isArray(c))for(let u=0,d=c.length;u<d;u++){let h=c[u];o(t.shapes,h)}else o(t.shapes,c)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let l=[];for(let c=0,u=this.material.length;c<u;c++)l.push(o(t.materials,this.material[c]));s.material=l}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let l=0;l<this.children.length;l++)s.children.push(this.children[l].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let l=0;l<this.animations.length;l++){let c=this.animations[l];s.animations.push(o(t.animations,c))}}if(i){let l=a(t.geometries),c=a(t.materials),u=a(t.textures),d=a(t.images),h=a(t.shapes),f=a(t.skeletons),g=a(t.animations),_=a(t.nodes);l.length>0&&(r.geometries=l),c.length>0&&(r.materials=c),u.length>0&&(r.textures=u),d.length>0&&(r.images=d),h.length>0&&(r.shapes=h),f.length>0&&(r.skeletons=f),g.length>0&&(r.animations=g),_.length>0&&(r.nodes=_)}return r.object=s,r;function a(l){let c=[];for(let u in l){let d=l[u];delete d.metadata,c.push(d)}return c}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new B(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),Ln=new B,hi=new B,mh=new B,fi=new B,Ss=new B,ws=new B,Gy=new B,gh=new B,vh=new B,yh=new B,_h=new wt,xh=new wt,Mh=new wt,Er=class n{constructor(e=new B,t=new B,i=new B){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),Ln.subVectors(e,t),r.cross(Ln);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){Ln.subVectors(r,t),hi.subVectors(i,t),mh.subVectors(e,t);let o=Ln.dot(Ln),a=Ln.dot(hi),l=Ln.dot(mh),c=hi.dot(hi),u=hi.dot(mh),d=o*c-a*a;if(d===0)return s.set(0,0,0),null;let h=1/d,f=(c*l-a*u)*h,g=(o*u-a*l)*h;return s.set(1-f-g,g,f)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,fi)===null?!1:fi.x>=0&&fi.y>=0&&fi.x+fi.y<=1}static getInterpolation(e,t,i,r,s,o,a,l){return this.getBarycoord(e,t,i,r,fi)===null?(l.x=0,l.y=0,"z"in l&&(l.z=0),"w"in l&&(l.w=0),null):(l.setScalar(0),l.addScaledVector(s,fi.x),l.addScaledVector(o,fi.y),l.addScaledVector(a,fi.z),l)}static getInterpolatedAttribute(e,t,i,r,s,o){return _h.setScalar(0),xh.setScalar(0),Mh.setScalar(0),_h.fromBufferAttribute(e,t),xh.fromBufferAttribute(e,i),Mh.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(_h,s.x),o.addScaledVector(xh,s.y),o.addScaledVector(Mh,s.z),o}static isFrontFacing(e,t,i,r){return Ln.subVectors(i,t),hi.subVectors(e,t),Ln.cross(hi).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return Ln.subVectors(this.c,this.b),hi.subVectors(this.a,this.b),Ln.cross(hi).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;Ss.subVectors(r,i),ws.subVectors(s,i),gh.subVectors(e,i);let l=Ss.dot(gh),c=ws.dot(gh);if(l<=0&&c<=0)return t.copy(i);vh.subVectors(e,r);let u=Ss.dot(vh),d=ws.dot(vh);if(u>=0&&d<=u)return t.copy(r);let h=l*d-u*c;if(h<=0&&l>=0&&u<=0)return o=l/(l-u),t.copy(i).addScaledVector(Ss,o);yh.subVectors(e,s);let f=Ss.dot(yh),g=ws.dot(yh);if(g>=0&&f<=g)return t.copy(s);let _=f*c-l*g;if(_<=0&&c>=0&&g<=0)return a=c/(c-g),t.copy(i).addScaledVector(ws,a);let m=u*g-f*d;if(m<=0&&d-u>=0&&f-g>=0)return Gy.subVectors(s,r),a=(d-u)/(d-u+(f-g)),t.copy(r).addScaledVector(Gy,a);let p=1/(m+_+h);return o=_*p,a=h*p,t.copy(i).addScaledVector(Ss,o).addScaledVector(ws,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},O0={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},Bi={h:0,s:0,l:0},Cl={h:0,s:0,l:0};function Eh(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var Ye=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=Yn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,st.toWorkingColorSpace(this,t),this}setRGB(e,t,i,r=st.workingColorSpace){return this.r=e,this.g=t,this.b=i,st.toWorkingColorSpace(this,r),this}setHSL(e,t,i,r=st.workingColorSpace){if(e=pC(e,1),t=on(t,0,1),i=on(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=Eh(o,s,e+1/3),this.g=Eh(o,s,e),this.b=Eh(o,s,e-1/3)}return st.toWorkingColorSpace(this,r),this}setStyle(e,t=Yn){function i(s){s!==void 0&&parseFloat(s)<1&&console.warn("THREE.Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:console.warn("THREE.Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);console.warn("THREE.Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=Yn){let i=O0[e.toLowerCase()];return i!==void 0?this.setHex(i,t):console.warn("THREE.Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=Ps(e.r),this.g=Ps(e.g),this.b=Ps(e.b),this}copyLinearToSRGB(e){return this.r=oh(e.r),this.g=oh(e.g),this.b=oh(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=Yn){return st.fromWorkingColorSpace(qt.copy(this),e),Math.round(on(qt.r*255,0,255))*65536+Math.round(on(qt.g*255,0,255))*256+Math.round(on(qt.b*255,0,255))}getHexString(e=Yn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=st.workingColorSpace){st.fromWorkingColorSpace(qt.copy(this),t);let i=qt.r,r=qt.g,s=qt.b,o=Math.max(i,r,s),a=Math.min(i,r,s),l,c,u=(a+o)/2;if(a===o)l=0,c=0;else{let d=o-a;switch(c=u<=.5?d/(o+a):d/(2-o-a),o){case i:l=(r-s)/d+(r<s?6:0);break;case r:l=(s-i)/d+2;break;case s:l=(i-r)/d+4;break}l/=6}return e.h=l,e.s=c,e.l=u,e}getRGB(e,t=st.workingColorSpace){return st.fromWorkingColorSpace(qt.copy(this),t),e.r=qt.r,e.g=qt.g,e.b=qt.b,e}getStyle(e=Yn){st.fromWorkingColorSpace(qt.copy(this),e);let t=qt.r,i=qt.g,r=qt.b;return e!==Yn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(Bi),this.setHSL(Bi.h+e,Bi.s+t,Bi.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(Bi),e.getHSL(Cl);let i=rh(Bi.h,Cl.h,t),r=rh(Bi.s,Cl.s,t),s=rh(Bi.l,Cl.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},qt=new Ye;Ye.NAMES=O0;var TC=0,Tr=class extends $i{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:TC++}),this.uuid=Ro(),this.name="",this.type="Material",this.blending=Rs,this.side=ji,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=Ph,this.blendDst=Fh,this.blendEquation=Mr,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new Ye(0,0,0),this.blendAlpha=0,this.depthFunc=Os,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=Ty,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=gs,this.stencilZFail=gs,this.stencilZPass=gs,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){console.warn(`THREE.Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){console.warn(`THREE.Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.6,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==Rs&&(i.blending=this.blending),this.side!==ji&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==Ph&&(i.blendSrc=this.blendSrc),this.blendDst!==Fh&&(i.blendDst=this.blendDst),this.blendEquation!==Mr&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==Os&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==Ty&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==gs&&(i.stencilFail=this.stencilFail),this.stencilZFail!==gs&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==gs&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let l=s[a];delete l.metadata,o.push(l)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}onBuild(){console.warn("Material: onBuild() has been removed.")}},tc=class extends Tr{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new Ye(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new Ar,this.combine=M0,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}};var Nt=new B,Dl=new ot,Cn=class{constructor(e,t,i=!1){if(Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=Iy,this.updateRanges=[],this.gpuType=vi,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)Dl.fromBufferAttribute(this,t),Dl.applyMatrix3(e),this.setXY(t,Dl.x,Dl.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)Nt.fromBufferAttribute(this,t),Nt.applyMatrix3(e),this.setXYZ(t,Nt.x,Nt.y,Nt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)Nt.fromBufferAttribute(this,t),Nt.applyMatrix4(e),this.setXYZ(t,Nt.x,Nt.y,Nt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Nt.fromBufferAttribute(this,t),Nt.applyNormalMatrix(e),this.setXYZ(t,Nt.x,Nt.y,Nt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Nt.fromBufferAttribute(this,t),Nt.transformDirection(e),this.setXYZ(t,Nt.x,Nt.y,Nt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=yo(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=sn(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=yo(t,this.array)),t}setX(e,t){return this.normalized&&(t=sn(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=yo(t,this.array)),t}setY(e,t){return this.normalized&&(t=sn(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=yo(t,this.array)),t}setZ(e,t){return this.normalized&&(t=sn(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=yo(t,this.array)),t}setW(e,t){return this.normalized&&(t=sn(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=sn(t,this.array),i=sn(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=sn(t,this.array),i=sn(i,this.array),r=sn(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=sn(t,this.array),i=sn(i,this.array),r=sn(r,this.array),s=sn(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==Iy&&(e.usage=this.usage),e}};var nc=class extends Cn{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var ic=class extends Cn{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var xi=class extends Cn{constructor(e,t,i){super(new Float32Array(e),t,i)}},IC=0,Sn=new It,bh=new bi,Cs=new B,gn=new Dr,bo=new Dr,Vt=new B,Ir=class n extends $i{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:IC++}),this.uuid=Ro(),this.name="",this.type="BufferGeometry",this.index=null,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(F0(e)?ic:nc)(e,1):this.index=e,this}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new ke().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return Sn.makeRotationFromQuaternion(e),this.applyMatrix4(Sn),this}rotateX(e){return Sn.makeRotationX(e),this.applyMatrix4(Sn),this}rotateY(e){return Sn.makeRotationY(e),this.applyMatrix4(Sn),this}rotateZ(e){return Sn.makeRotationZ(e),this.applyMatrix4(Sn),this}translate(e,t,i){return Sn.makeTranslation(e,t,i),this.applyMatrix4(Sn),this}scale(e,t,i){return Sn.makeScale(e,t,i),this.applyMatrix4(Sn),this}lookAt(e){return bh.lookAt(e),bh.updateMatrix(),this.applyMatrix4(bh.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(Cs).negate(),this.translate(Cs.x,Cs.y,Cs.z),this}setFromPoints(e){let t=[];for(let i=0,r=e.length;i<r;i++){let s=e[i];t.push(s.x,s.y,s.z||0)}return this.setAttribute("position",new xi(t,3)),this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new Dr);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){console.error("THREE.BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new B(-1/0,-1/0,-1/0),new B(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];gn.setFromBufferAttribute(s),this.morphTargetsRelative?(Vt.addVectors(this.boundingBox.min,gn.min),this.boundingBox.expandByPoint(Vt),Vt.addVectors(this.boundingBox.max,gn.max),this.boundingBox.expandByPoint(Vt)):(this.boundingBox.expandByPoint(gn.min),this.boundingBox.expandByPoint(gn.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&console.error('THREE.BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new Do);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){console.error("THREE.BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new B,1/0);return}if(e){let i=this.boundingSphere.center;if(gn.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];bo.setFromBufferAttribute(a),this.morphTargetsRelative?(Vt.addVectors(gn.min,bo.min),gn.expandByPoint(Vt),Vt.addVectors(gn.max,bo.max),gn.expandByPoint(Vt)):(gn.expandByPoint(bo.min),gn.expandByPoint(bo.max))}gn.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)Vt.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(Vt));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],l=this.morphTargetsRelative;for(let c=0,u=a.count;c<u;c++)Vt.fromBufferAttribute(a,c),l&&(Cs.fromBufferAttribute(e,c),Vt.add(Cs)),r=Math.max(r,i.distanceToSquared(Vt))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&console.error('THREE.BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){console.error("THREE.BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new Cn(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],l=[];for(let V=0;V<i.count;V++)a[V]=new B,l[V]=new B;let c=new B,u=new B,d=new B,h=new ot,f=new ot,g=new ot,_=new B,m=new B;function p(V,re,v){c.fromBufferAttribute(i,V),u.fromBufferAttribute(i,re),d.fromBufferAttribute(i,v),h.fromBufferAttribute(s,V),f.fromBufferAttribute(s,re),g.fromBufferAttribute(s,v),u.sub(c),d.sub(c),f.sub(h),g.sub(h);let M=1/(f.x*g.y-g.x*f.y);isFinite(M)&&(_.copy(u).multiplyScalar(g.y).addScaledVector(d,-f.y).multiplyScalar(M),m.copy(d).multiplyScalar(f.x).addScaledVector(u,-g.x).multiplyScalar(M),a[V].add(_),a[re].add(_),a[v].add(_),l[V].add(m),l[re].add(m),l[v].add(m))}let w=this.groups;w.length===0&&(w=[{start:0,count:e.count}]);for(let V=0,re=w.length;V<re;++V){let v=w[V],M=v.start,j=v.count;for(let z=M,Y=M+j;z<Y;z+=3)p(e.getX(z+0),e.getX(z+1),e.getX(z+2))}let E=new B,S=new B,k=new B,I=new B;function C(V){k.fromBufferAttribute(r,V),I.copy(k);let re=a[V];E.copy(re),E.sub(k.multiplyScalar(k.dot(re))).normalize(),S.crossVectors(I,re);let M=S.dot(l[V])<0?-1:1;o.setXYZW(V,E.x,E.y,E.z,M)}for(let V=0,re=w.length;V<re;++V){let v=w[V],M=v.start,j=v.count;for(let z=M,Y=M+j;z<Y;z+=3)C(e.getX(z+0)),C(e.getX(z+1)),C(e.getX(z+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new Cn(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let h=0,f=i.count;h<f;h++)i.setXYZ(h,0,0,0);let r=new B,s=new B,o=new B,a=new B,l=new B,c=new B,u=new B,d=new B;if(e)for(let h=0,f=e.count;h<f;h+=3){let g=e.getX(h+0),_=e.getX(h+1),m=e.getX(h+2);r.fromBufferAttribute(t,g),s.fromBufferAttribute(t,_),o.fromBufferAttribute(t,m),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),a.fromBufferAttribute(i,g),l.fromBufferAttribute(i,_),c.fromBufferAttribute(i,m),a.add(u),l.add(u),c.add(u),i.setXYZ(g,a.x,a.y,a.z),i.setXYZ(_,l.x,l.y,l.z),i.setXYZ(m,c.x,c.y,c.z)}else for(let h=0,f=t.count;h<f;h+=3)r.fromBufferAttribute(t,h+0),s.fromBufferAttribute(t,h+1),o.fromBufferAttribute(t,h+2),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),i.setXYZ(h+0,u.x,u.y,u.z),i.setXYZ(h+1,u.x,u.y,u.z),i.setXYZ(h+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)Vt.fromBufferAttribute(e,t),Vt.normalize(),e.setXYZ(t,Vt.x,Vt.y,Vt.z)}toNonIndexed(){function e(a,l){let c=a.array,u=a.itemSize,d=a.normalized,h=new c.constructor(l.length*u),f=0,g=0;for(let _=0,m=l.length;_<m;_++){a.isInterleavedBufferAttribute?f=l[_]*a.data.stride+a.offset:f=l[_]*u;for(let p=0;p<u;p++)h[g++]=c[f++]}return new Cn(h,u,d)}if(this.index===null)return console.warn("THREE.BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let l=r[a],c=e(l,i);t.setAttribute(a,c)}let s=this.morphAttributes;for(let a in s){let l=[],c=s[a];for(let u=0,d=c.length;u<d;u++){let h=c[u],f=e(h,i);l.push(f)}t.morphAttributes[a]=l}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,l=o.length;a<l;a++){let c=o[a];t.addGroup(c.start,c.count,c.materialIndex)}return t}toJSON(){let e={metadata:{version:4.6,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let l=this.parameters;for(let c in l)l[c]!==void 0&&(e[c]=l[c]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let l in i){let c=i[l];e.data.attributes[l]=c.toJSON(e.data)}let r={},s=!1;for(let l in this.morphAttributes){let c=this.morphAttributes[l],u=[];for(let d=0,h=c.length;d<h;d++){let f=c[d];u.push(f.toJSON(e.data))}u.length>0&&(r[l]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere={center:a.center.toArray(),radius:a.radius}),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone(t));let r=e.attributes;for(let c in r){let u=r[c];this.setAttribute(c,u.clone(t))}let s=e.morphAttributes;for(let c in s){let u=[],d=s[c];for(let h=0,f=d.length;h<f;h++)u.push(d[h].clone(t));this.morphAttributes[c]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let c=0,u=o.length;c<u;c++){let d=o[c];this.addGroup(d.start,d.count,d.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let l=e.boundingSphere;return l!==null&&(this.boundingSphere=l.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}},Wy=new It,gr=new Sf,Al=new Do,jy=new B,Tl=new B,Il=new B,Rl=new B,Sh=new B,Nl=new B,$y=new B,Pl=new B,an=class extends bi{constructor(e=new Ir,t=new tc){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){Nl.set(0,0,0);for(let l=0,c=s.length;l<c;l++){let u=a[l],d=s[l];u!==0&&(Sh.fromBufferAttribute(d,e),o?Nl.addScaledVector(Sh,u):Nl.addScaledVector(Sh.sub(t),u))}t.add(Nl)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),Al.copy(i.boundingSphere),Al.applyMatrix4(s),gr.copy(e.ray).recast(e.near),!(Al.containsPoint(gr.origin)===!1&&(gr.intersectSphere(Al,jy)===null||gr.origin.distanceToSquared(jy)>(e.far-e.near)**2))&&(Wy.copy(s).invert(),gr.copy(e.ray).applyMatrix4(Wy),!(i.boundingBox!==null&&gr.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,gr)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,l=s.attributes.position,c=s.attributes.uv,u=s.attributes.uv1,d=s.attributes.normal,h=s.groups,f=s.drawRange;if(a!==null)if(Array.isArray(o))for(let g=0,_=h.length;g<_;g++){let m=h[g],p=o[m.materialIndex],w=Math.max(m.start,f.start),E=Math.min(a.count,Math.min(m.start+m.count,f.start+f.count));for(let S=w,k=E;S<k;S+=3){let I=a.getX(S),C=a.getX(S+1),V=a.getX(S+2);r=Fl(this,p,e,i,c,u,d,I,C,V),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,f.start),_=Math.min(a.count,f.start+f.count);for(let m=g,p=_;m<p;m+=3){let w=a.getX(m),E=a.getX(m+1),S=a.getX(m+2);r=Fl(this,o,e,i,c,u,d,w,E,S),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}else if(l!==void 0)if(Array.isArray(o))for(let g=0,_=h.length;g<_;g++){let m=h[g],p=o[m.materialIndex],w=Math.max(m.start,f.start),E=Math.min(l.count,Math.min(m.start+m.count,f.start+f.count));for(let S=w,k=E;S<k;S+=3){let I=S,C=S+1,V=S+2;r=Fl(this,p,e,i,c,u,d,I,C,V),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,f.start),_=Math.min(l.count,f.start+f.count);for(let m=g,p=_;m<p;m+=3){let w=m,E=m+1,S=m+2;r=Fl(this,o,e,i,c,u,d,w,E,S),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}}};function RC(n,e,t,i,r,s,o,a){let l;if(e.side===ln?l=i.intersectTriangle(o,s,r,!0,a):l=i.intersectTriangle(r,s,o,e.side===ji,a),l===null)return null;Pl.copy(a),Pl.applyMatrix4(n.matrixWorld);let c=t.ray.origin.distanceTo(Pl);return c<t.near||c>t.far?null:{distance:c,point:Pl.clone(),object:n}}function Fl(n,e,t,i,r,s,o,a,l,c){n.getVertexPosition(a,Tl),n.getVertexPosition(l,Il),n.getVertexPosition(c,Rl);let u=RC(n,e,t,i,Tl,Il,Rl,$y);if(u){let d=new B;Er.getBarycoord($y,Tl,Il,Rl,d),r&&(u.uv=Er.getInterpolatedAttribute(r,a,l,c,d,new ot)),s&&(u.uv1=Er.getInterpolatedAttribute(s,a,l,c,d,new ot)),o&&(u.normal=Er.getInterpolatedAttribute(o,a,l,c,d,new B),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let h={a,b:l,c,normal:new B,materialIndex:0};Er.getNormal(Tl,Il,Rl,h.normal),u.face=h,u.barycoord=d}return u}var Xi=class n extends Ir{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let l=[],c=[],u=[],d=[],h=0,f=0;g("z","y","x",-1,-1,i,t,e,o,s,0),g("z","y","x",1,-1,i,t,-e,o,s,1),g("x","z","y",1,1,e,i,t,r,o,2),g("x","z","y",1,-1,e,i,-t,r,o,3),g("x","y","z",1,-1,e,t,i,r,s,4),g("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(l),this.setAttribute("position",new xi(c,3)),this.setAttribute("normal",new xi(u,3)),this.setAttribute("uv",new xi(d,2));function g(_,m,p,w,E,S,k,I,C,V,re){let v=S/C,M=k/V,j=S/2,z=k/2,Y=I/2,J=C+1,W=V+1,K=0,G=0,de=new B;for(let he=0;he<W;he++){let xe=he*M-z;for(let Je=0;Je<J;Je++){let at=Je*v-j;de[_]=at*w,de[m]=xe*E,de[p]=Y,c.push(de.x,de.y,de.z),de[_]=0,de[m]=0,de[p]=I>0?1:-1,u.push(de.x,de.y,de.z),d.push(Je/C),d.push(1-he/V),K+=1}}for(let he=0;he<V;he++)for(let xe=0;xe<C;xe++){let Je=h+xe+J*he,at=h+xe+J*(he+1),$=h+(xe+1)+J*(he+1),ee=h+(xe+1)+J*he;l.push(Je,at,ee),l.push(at,$,ee),G+=6}a.addGroup(f,G,re),f+=G,h+=K}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};function Bs(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];r&&(r.isColor||r.isMatrix3||r.isMatrix4||r.isVector2||r.isVector3||r.isVector4||r.isTexture||r.isQuaternion)?r.isRenderTargetTexture?(console.warn("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone():Array.isArray(r)?e[t][i]=r.slice():e[t][i]=r}}return e}function Kt(n){let e={};for(let t=0;t<n.length;t++){let i=Bs(n[t]);for(let r in i)e[r]=i[r]}return e}function NC(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function L0(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:st.workingColorSpace}var PC={clone:Bs,merge:Kt},FC=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,OC=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,Kn=class extends Tr{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=FC,this.fragmentShader=OC,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=Bs(e.uniforms),this.uniformsGroups=NC(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},rc=class extends bi{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new It,this.projectionMatrix=new It,this.projectionMatrixInverse=new It,this.coordinateSystem=yi}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorldInverse.copy(this.matrixWorld).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorldInverse.copy(this.matrixWorld).invert()}clone(){return new this.constructor().copy(this)}},Hi=new B,qy=new ot,Xy=new ot,Jt=class extends rc{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=xf*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(ih*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return xf*2*Math.atan(Math.tan(ih*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){Hi.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(Hi.x,Hi.y).multiplyScalar(-e/Hi.z),Hi.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(Hi.x,Hi.y).multiplyScalar(-e/Hi.z)}getViewSize(e,t){return this.getViewBounds(e,qy,Xy),t.subVectors(Xy,qy)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(ih*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let l=o.fullWidth,c=o.fullHeight;s+=o.offsetX*r/l,t-=o.offsetY*i/c,r*=o.width/l,i*=o.height/c}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}},Ds=-90,As=1,wf=class extends bi{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new Jt(Ds,As,e,t);r.layers=this.layers,this.add(r);let s=new Jt(Ds,As,e,t);s.layers=this.layers,this.add(s);let o=new Jt(Ds,As,e,t);o.layers=this.layers,this.add(o);let a=new Jt(Ds,As,e,t);a.layers=this.layers,this.add(a);let l=new Jt(Ds,As,e,t);l.layers=this.layers,this.add(l);let c=new Jt(Ds,As,e,t);c.layers=this.layers,this.add(c)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,l]=t;for(let c of t)this.remove(c);if(e===yi)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),l.up.set(0,1,0),l.lookAt(0,0,-1);else if(e===Zl)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),l.up.set(0,-1,0),l.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let c of t)this.add(c),c.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,l,c,u]=this.children,d=e.getRenderTarget(),h=e.getActiveCubeFace(),f=e.getActiveMipmapLevel(),g=e.xr.enabled;e.xr.enabled=!1;let _=i.texture.generateMipmaps;i.texture.generateMipmaps=!1,e.setRenderTarget(i,0,r),e.render(t,s),e.setRenderTarget(i,1,r),e.render(t,o),e.setRenderTarget(i,2,r),e.render(t,a),e.setRenderTarget(i,3,r),e.render(t,l),e.setRenderTarget(i,4,r),e.render(t,c),i.texture.generateMipmaps=_,e.setRenderTarget(i,5,r),e.render(t,u),e.setRenderTarget(d,h,f),e.xr.enabled=g,i.texture.needsPMREMUpdate=!0}},sc=class extends Pr{constructor(e,t,i,r,s,o,a,l,c,u){e=e!==void 0?e:[],t=t!==void 0?t:Ls,super(e,t,i,r,s,o,a,l,c,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}},Cf=class extends Ei{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new sc(r,t.mapping,t.wrapS,t.wrapT,t.magFilter,t.minFilter,t.format,t.type,t.anisotropy,t.colorSpace),this.texture.isRenderTargetTexture=!0,this.texture.generateMipmaps=t.generateMipmaps!==void 0?t.generateMipmaps:!1,this.texture.minFilter=t.minFilter!==void 0?t.minFilter:kn}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

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
			`},r=new Xi(5,5,5),s=new Kn({name:"CubemapFromEquirect",uniforms:Bs(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:ln,blending:Gi});s.uniforms.tEquirect.value=t;let o=new an(r,s),a=t.minFilter;return t.minFilter===wr&&(t.minFilter=kn),new wf(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t,i,r){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}},wh=new B,LC=new B,kC=new ke,mi=class{constructor(e=new B(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=wh.subVectors(i,t).cross(LC.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t){let i=e.delta(wh),r=this.normal.dot(i);if(r===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let s=-(e.start.dot(this.normal)+this.constant)/r;return s<0||s>1?null:t.copy(e.start).addScaledVector(i,s)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||kC.getNormalMatrix(e),r=this.coplanarPoint(wh).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},vr=new Do,Ol=new B,Ao=class{constructor(e=new mi,t=new mi,i=new mi,r=new mi,s=new mi,o=new mi){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=yi){let i=this.planes,r=e.elements,s=r[0],o=r[1],a=r[2],l=r[3],c=r[4],u=r[5],d=r[6],h=r[7],f=r[8],g=r[9],_=r[10],m=r[11],p=r[12],w=r[13],E=r[14],S=r[15];if(i[0].setComponents(l-s,h-c,m-f,S-p).normalize(),i[1].setComponents(l+s,h+c,m+f,S+p).normalize(),i[2].setComponents(l+o,h+u,m+g,S+w).normalize(),i[3].setComponents(l-o,h-u,m-g,S-w).normalize(),i[4].setComponents(l-a,h-d,m-_,S-E).normalize(),t===yi)i[5].setComponents(l+a,h+d,m+_,S+E).normalize();else if(t===Zl)i[5].setComponents(a,d,_,E).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),vr.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),vr.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(vr)}intersectsSprite(e){return vr.center.set(0,0,0),vr.radius=.7071067811865476,vr.applyMatrix4(e.matrixWorld),this.intersectsSphere(vr)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(Ol.x=r.normal.x>0?e.max.x:e.min.x,Ol.y=r.normal.y>0?e.max.y:e.min.y,Ol.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(Ol)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};function k0(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function UC(n){let e=new WeakMap;function t(a,l){let c=a.array,u=a.usage,d=c.byteLength,h=n.createBuffer();n.bindBuffer(l,h),n.bufferData(l,c,u),a.onUploadCallback();let f;if(c instanceof Float32Array)f=n.FLOAT;else if(c instanceof Uint16Array)a.isFloat16BufferAttribute?f=n.HALF_FLOAT:f=n.UNSIGNED_SHORT;else if(c instanceof Int16Array)f=n.SHORT;else if(c instanceof Uint32Array)f=n.UNSIGNED_INT;else if(c instanceof Int32Array)f=n.INT;else if(c instanceof Int8Array)f=n.BYTE;else if(c instanceof Uint8Array)f=n.UNSIGNED_BYTE;else if(c instanceof Uint8ClampedArray)f=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+c);return{buffer:h,type:f,bytesPerElement:c.BYTES_PER_ELEMENT,version:a.version,size:d}}function i(a,l,c){let u=l.array,d=l.updateRanges;if(n.bindBuffer(c,a),d.length===0)n.bufferSubData(c,0,u);else{d.sort((f,g)=>f.start-g.start);let h=0;for(let f=1;f<d.length;f++){let g=d[h],_=d[f];_.start<=g.start+g.count+1?g.count=Math.max(g.count,_.start+_.count-g.start):(++h,d[h]=_)}d.length=h+1;for(let f=0,g=d.length;f<g;f++){let _=d[f];n.bufferSubData(c,_.start*u.BYTES_PER_ELEMENT,u,_.start,_.count)}l.clearUpdateRanges()}l.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let l=e.get(a);l&&(n.deleteBuffer(l.buffer),e.delete(a))}function o(a,l){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let c=e.get(a);if(c===void 0)e.set(a,t(a,l));else if(c.version<a.version){if(c.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(c.buffer,a,l),c.version=a.version}}return{get:r,remove:s,update:o}}var oc=class n extends Ir{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),l=Math.floor(r),c=a+1,u=l+1,d=e/a,h=t/l,f=[],g=[],_=[],m=[];for(let p=0;p<u;p++){let w=p*h-o;for(let E=0;E<c;E++){let S=E*d-s;g.push(S,-w,0),_.push(0,0,1),m.push(E/a),m.push(1-p/l)}}for(let p=0;p<l;p++)for(let w=0;w<a;w++){let E=w+c*p,S=w+c*(p+1),k=w+1+c*(p+1),I=w+1+c*p;f.push(E,S,I),f.push(S,k,I)}this.setIndex(f),this.setAttribute("position",new xi(g,3)),this.setAttribute("normal",new xi(_,3)),this.setAttribute("uv",new xi(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}},VC=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,BC=`#ifdef USE_ALPHAHASH
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
#endif`,HC=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,zC=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,GC=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,WC=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,jC=`#ifdef USE_AOMAP
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
#endif`,$C=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,qC=`#ifdef USE_BATCHING
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
#endif`,XC=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,YC=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,ZC=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,KC=`float G_BlinnPhong_Implicit( ) {
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
} // validated`,JC=`#ifdef USE_IRIDESCENCE
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
#endif`,QC=`#ifdef USE_BUMPMAP
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
#endif`,eD=`#if NUM_CLIPPING_PLANES > 0
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
#endif`,tD=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,nD=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,iD=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,rD=`#if defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#elif defined( USE_COLOR )
	diffuseColor.rgb *= vColor;
#endif`,sD=`#if defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#elif defined( USE_COLOR )
	varying vec3 vColor;
#endif`,oD=`#if defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#elif defined( USE_COLOR ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec3 vColor;
#endif`,aD=`#if defined( USE_COLOR_ALPHA )
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
#endif`,lD=`#define PI 3.141592653589793
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
} // validated`,cD=`#ifdef ENVMAP_TYPE_CUBE_UV
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
#endif`,uD=`vec3 transformedNormal = objectNormal;
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
#endif`,dD=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,hD=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,fD=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,pD=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,mD="gl_FragColor = linearToOutputTexel( gl_FragColor );",gD=`
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
}`,vD=`#ifdef USE_ENVMAP
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
#endif`,yD=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform float flipEnvMap;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
	
#endif`,_D=`#ifdef USE_ENVMAP
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
#endif`,xD=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,MD=`#ifdef USE_ENVMAP
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
#endif`,ED=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,bD=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,SD=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,wD=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,CD=`#ifdef USE_GRADIENTMAP
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
}`,DD=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,AD=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,TD=`varying vec3 vViewPosition;
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
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,ID=`uniform bool receiveShadow;
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
#endif`,RD=`#ifdef USE_ENVMAP
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
#endif`,ND=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,PD=`varying vec3 vViewPosition;
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
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,FD=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,OD=`varying vec3 vViewPosition;
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
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,LD=`PhysicalMaterial material;
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
#endif`,kD=`struct PhysicalMaterial {
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
}`,UD=`
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
#endif`,VD=`#if defined( RE_IndirectDiffuse )
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
#endif`,BD=`#if defined( RE_IndirectDiffuse )
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,HD=`#if defined( USE_LOGDEPTHBUF )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,zD=`#if defined( USE_LOGDEPTHBUF )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,GD=`#ifdef USE_LOGDEPTHBUF
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,WD=`#ifdef USE_LOGDEPTHBUF
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,jD=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = vec4( mix( pow( sampledDiffuseColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), sampledDiffuseColor.rgb * 0.0773993808, vec3( lessThanEqual( sampledDiffuseColor.rgb, vec3( 0.04045 ) ) ) ), sampledDiffuseColor.w );
	
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,$D=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,qD=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
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
#endif`,XD=`#if defined( USE_POINTS_UV )
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
#endif`,YD=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,ZD=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,KD=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,JD=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,QD=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,eA=`#ifdef USE_MORPHTARGETS
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
#endif`,tA=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,nA=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
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
vec3 nonPerturbedNormal = normal;`,iA=`#ifdef USE_NORMALMAP_OBJECTSPACE
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
#endif`,rA=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,sA=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,oA=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,aA=`#ifdef USE_NORMALMAP
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
#endif`,lA=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,cA=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,uA=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,dA=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,hA=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,fA=`vec3 packNormalToRGB( const in vec3 normal ) {
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
}`,pA=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,mA=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,gA=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,vA=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,yA=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,_A=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,xA=`#if NUM_SPOT_LIGHT_COORDS > 0
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
#endif`,MA=`#if NUM_SPOT_LIGHT_COORDS > 0
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
#endif`,EA=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
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
#endif`,bA=`float getShadowMask() {
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
}`,SA=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,wA=`#ifdef USE_SKINNING
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
#endif`,CA=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,DA=`#ifdef USE_SKINNING
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
#endif`,AA=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,TA=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,IA=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,RA=`#ifndef saturate
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
vec3 CustomToneMapping( vec3 color ) { return color; }`,NA=`#ifdef USE_TRANSMISSION
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
#endif`,PA=`#ifdef USE_TRANSMISSION
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
#endif`,FA=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
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
#endif`,OA=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
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
#endif`,LA=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
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
#endif`,kA=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,UA=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,VA=`uniform sampler2D t2D;
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
}`,BA=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,HA=`#ifdef ENVMAP_TYPE_CUBE
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
}`,zA=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,GA=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,WA=`#include <common>
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
}`,jA=`#if DEPTH_PACKING == 3200
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
}`,$A=`#define DISTANCE
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
}`,qA=`#define DISTANCE
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
}`,XA=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,YA=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,ZA=`uniform float scale;
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
}`,KA=`uniform vec3 diffuse;
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
}`,JA=`#include <common>
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
}`,QA=`uniform vec3 diffuse;
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
}`,eT=`#define LAMBERT
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
}`,tT=`#define LAMBERT
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
}`,nT=`#define MATCAP
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
}`,iT=`#define MATCAP
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
}`,rT=`#define NORMAL
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
}`,sT=`#define NORMAL
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
}`,oT=`#define PHONG
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
}`,aT=`#define PHONG
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
}`,lT=`#define STANDARD
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
}`,cT=`#define STANDARD
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
}`,uT=`#define TOON
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
}`,dT=`#define TOON
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
}`,hT=`uniform float size;
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
}`,fT=`uniform vec3 diffuse;
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
}`,pT=`#include <common>
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
}`,mT=`uniform vec3 color;
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
}`,gT=`uniform float rotation;
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
}`,vT=`uniform vec3 diffuse;
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
}`,Le={alphahash_fragment:VC,alphahash_pars_fragment:BC,alphamap_fragment:HC,alphamap_pars_fragment:zC,alphatest_fragment:GC,alphatest_pars_fragment:WC,aomap_fragment:jC,aomap_pars_fragment:$C,batching_pars_vertex:qC,batching_vertex:XC,begin_vertex:YC,beginnormal_vertex:ZC,bsdfs:KC,iridescence_fragment:JC,bumpmap_pars_fragment:QC,clipping_planes_fragment:eD,clipping_planes_pars_fragment:tD,clipping_planes_pars_vertex:nD,clipping_planes_vertex:iD,color_fragment:rD,color_pars_fragment:sD,color_pars_vertex:oD,color_vertex:aD,common:lD,cube_uv_reflection_fragment:cD,defaultnormal_vertex:uD,displacementmap_pars_vertex:dD,displacementmap_vertex:hD,emissivemap_fragment:fD,emissivemap_pars_fragment:pD,colorspace_fragment:mD,colorspace_pars_fragment:gD,envmap_fragment:vD,envmap_common_pars_fragment:yD,envmap_pars_fragment:_D,envmap_pars_vertex:xD,envmap_physical_pars_fragment:RD,envmap_vertex:MD,fog_vertex:ED,fog_pars_vertex:bD,fog_fragment:SD,fog_pars_fragment:wD,gradientmap_pars_fragment:CD,lightmap_pars_fragment:DD,lights_lambert_fragment:AD,lights_lambert_pars_fragment:TD,lights_pars_begin:ID,lights_toon_fragment:ND,lights_toon_pars_fragment:PD,lights_phong_fragment:FD,lights_phong_pars_fragment:OD,lights_physical_fragment:LD,lights_physical_pars_fragment:kD,lights_fragment_begin:UD,lights_fragment_maps:VD,lights_fragment_end:BD,logdepthbuf_fragment:HD,logdepthbuf_pars_fragment:zD,logdepthbuf_pars_vertex:GD,logdepthbuf_vertex:WD,map_fragment:jD,map_pars_fragment:$D,map_particle_fragment:qD,map_particle_pars_fragment:XD,metalnessmap_fragment:YD,metalnessmap_pars_fragment:ZD,morphinstance_vertex:KD,morphcolor_vertex:JD,morphnormal_vertex:QD,morphtarget_pars_vertex:eA,morphtarget_vertex:tA,normal_fragment_begin:nA,normal_fragment_maps:iA,normal_pars_fragment:rA,normal_pars_vertex:sA,normal_vertex:oA,normalmap_pars_fragment:aA,clearcoat_normal_fragment_begin:lA,clearcoat_normal_fragment_maps:cA,clearcoat_pars_fragment:uA,iridescence_pars_fragment:dA,opaque_fragment:hA,packing:fA,premultiplied_alpha_fragment:pA,project_vertex:mA,dithering_fragment:gA,dithering_pars_fragment:vA,roughnessmap_fragment:yA,roughnessmap_pars_fragment:_A,shadowmap_pars_fragment:xA,shadowmap_pars_vertex:MA,shadowmap_vertex:EA,shadowmask_pars_fragment:bA,skinbase_vertex:SA,skinning_pars_vertex:wA,skinning_vertex:CA,skinnormal_vertex:DA,specularmap_fragment:AA,specularmap_pars_fragment:TA,tonemapping_fragment:IA,tonemapping_pars_fragment:RA,transmission_fragment:NA,transmission_pars_fragment:PA,uv_pars_fragment:FA,uv_pars_vertex:OA,uv_vertex:LA,worldpos_vertex:kA,background_vert:UA,background_frag:VA,backgroundCube_vert:BA,backgroundCube_frag:HA,cube_vert:zA,cube_frag:GA,depth_vert:WA,depth_frag:jA,distanceRGBA_vert:$A,distanceRGBA_frag:qA,equirect_vert:XA,equirect_frag:YA,linedashed_vert:ZA,linedashed_frag:KA,meshbasic_vert:JA,meshbasic_frag:QA,meshlambert_vert:eT,meshlambert_frag:tT,meshmatcap_vert:nT,meshmatcap_frag:iT,meshnormal_vert:rT,meshnormal_frag:sT,meshphong_vert:oT,meshphong_frag:aT,meshphysical_vert:lT,meshphysical_frag:cT,meshtoon_vert:uT,meshtoon_frag:dT,points_vert:hT,points_frag:fT,shadow_vert:pT,shadow_frag:mT,sprite_vert:gT,sprite_frag:vT},se={common:{diffuse:{value:new Ye(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new ke},alphaMap:{value:null},alphaMapTransform:{value:new ke},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new ke}},envmap:{envMap:{value:null},envMapRotation:{value:new ke},flipEnvMap:{value:-1},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new ke}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new ke}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new ke},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new ke},normalScale:{value:new ot(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new ke},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new ke}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new ke}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new ke}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new Ye(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMap:{value:[]},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotShadowMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMap:{value:[]},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null}},points:{diffuse:{value:new Ye(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new ke},alphaTest:{value:0},uvTransform:{value:new ke}},sprite:{diffuse:{value:new Ye(16777215)},opacity:{value:1},center:{value:new ot(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new ke},alphaMap:{value:null},alphaMapTransform:{value:new ke},alphaTest:{value:0}}},Zn={basic:{uniforms:Kt([se.common,se.specularmap,se.envmap,se.aomap,se.lightmap,se.fog]),vertexShader:Le.meshbasic_vert,fragmentShader:Le.meshbasic_frag},lambert:{uniforms:Kt([se.common,se.specularmap,se.envmap,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.fog,se.lights,{emissive:{value:new Ye(0)}}]),vertexShader:Le.meshlambert_vert,fragmentShader:Le.meshlambert_frag},phong:{uniforms:Kt([se.common,se.specularmap,se.envmap,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.fog,se.lights,{emissive:{value:new Ye(0)},specular:{value:new Ye(1118481)},shininess:{value:30}}]),vertexShader:Le.meshphong_vert,fragmentShader:Le.meshphong_frag},standard:{uniforms:Kt([se.common,se.envmap,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.roughnessmap,se.metalnessmap,se.fog,se.lights,{emissive:{value:new Ye(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:Le.meshphysical_vert,fragmentShader:Le.meshphysical_frag},toon:{uniforms:Kt([se.common,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.gradientmap,se.fog,se.lights,{emissive:{value:new Ye(0)}}]),vertexShader:Le.meshtoon_vert,fragmentShader:Le.meshtoon_frag},matcap:{uniforms:Kt([se.common,se.bumpmap,se.normalmap,se.displacementmap,se.fog,{matcap:{value:null}}]),vertexShader:Le.meshmatcap_vert,fragmentShader:Le.meshmatcap_frag},points:{uniforms:Kt([se.points,se.fog]),vertexShader:Le.points_vert,fragmentShader:Le.points_frag},dashed:{uniforms:Kt([se.common,se.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:Le.linedashed_vert,fragmentShader:Le.linedashed_frag},depth:{uniforms:Kt([se.common,se.displacementmap]),vertexShader:Le.depth_vert,fragmentShader:Le.depth_frag},normal:{uniforms:Kt([se.common,se.bumpmap,se.normalmap,se.displacementmap,{opacity:{value:1}}]),vertexShader:Le.meshnormal_vert,fragmentShader:Le.meshnormal_frag},sprite:{uniforms:Kt([se.sprite,se.fog]),vertexShader:Le.sprite_vert,fragmentShader:Le.sprite_frag},background:{uniforms:{uvTransform:{value:new ke},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:Le.background_vert,fragmentShader:Le.background_frag},backgroundCube:{uniforms:{envMap:{value:null},flipEnvMap:{value:-1},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new ke}},vertexShader:Le.backgroundCube_vert,fragmentShader:Le.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:Le.cube_vert,fragmentShader:Le.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:Le.equirect_vert,fragmentShader:Le.equirect_frag},distanceRGBA:{uniforms:Kt([se.common,se.displacementmap,{referencePosition:{value:new B},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:Le.distanceRGBA_vert,fragmentShader:Le.distanceRGBA_frag},shadow:{uniforms:Kt([se.lights,se.fog,{color:{value:new Ye(0)},opacity:{value:1}}]),vertexShader:Le.shadow_vert,fragmentShader:Le.shadow_frag}};Zn.physical={uniforms:Kt([Zn.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new ke},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new ke},clearcoatNormalScale:{value:new ot(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new ke},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new ke},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new ke},sheen:{value:0},sheenColor:{value:new Ye(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new ke},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new ke},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new ke},transmissionSamplerSize:{value:new ot},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new ke},attenuationDistance:{value:0},attenuationColor:{value:new Ye(0)},specularColor:{value:new Ye(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new ke},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new ke},anisotropyVector:{value:new ot},anisotropyMap:{value:null},anisotropyMapTransform:{value:new ke}}]),vertexShader:Le.meshphysical_vert,fragmentShader:Le.meshphysical_frag};var Ll={r:0,b:0,g:0},yr=new Ar,yT=new It;function _T(n,e,t,i,r,s,o){let a=new Ye(0),l=s===!0?0:1,c,u,d=null,h=0,f=null;function g(w){let E=w.isScene===!0?w.background:null;return E&&E.isTexture&&(E=(w.backgroundBlurriness>0?t:e).get(E)),E}function _(w){let E=!1,S=g(w);S===null?p(a,l):S&&S.isColor&&(p(S,1),E=!0);let k=n.xr.getEnvironmentBlendMode();k==="additive"?i.buffers.color.setClear(0,0,0,1,o):k==="alpha-blend"&&i.buffers.color.setClear(0,0,0,0,o),(n.autoClear||E)&&(i.buffers.depth.setTest(!0),i.buffers.depth.setMask(!0),i.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function m(w,E){let S=g(E);S&&(S.isCubeTexture||S.mapping===gc)?(u===void 0&&(u=new an(new Xi(1,1,1),new Kn({name:"BackgroundCubeMaterial",uniforms:Bs(Zn.backgroundCube.uniforms),vertexShader:Zn.backgroundCube.vertexShader,fragmentShader:Zn.backgroundCube.fragmentShader,side:ln,depthTest:!1,depthWrite:!1,fog:!1})),u.geometry.deleteAttribute("normal"),u.geometry.deleteAttribute("uv"),u.onBeforeRender=function(k,I,C){this.matrixWorld.copyPosition(C.matrixWorld)},Object.defineProperty(u.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),r.update(u)),yr.copy(E.backgroundRotation),yr.x*=-1,yr.y*=-1,yr.z*=-1,S.isCubeTexture&&S.isRenderTargetTexture===!1&&(yr.y*=-1,yr.z*=-1),u.material.uniforms.envMap.value=S,u.material.uniforms.flipEnvMap.value=S.isCubeTexture&&S.isRenderTargetTexture===!1?-1:1,u.material.uniforms.backgroundBlurriness.value=E.backgroundBlurriness,u.material.uniforms.backgroundIntensity.value=E.backgroundIntensity,u.material.uniforms.backgroundRotation.value.setFromMatrix4(yT.makeRotationFromEuler(yr)),u.material.toneMapped=st.getTransfer(S.colorSpace)!==xt,(d!==S||h!==S.version||f!==n.toneMapping)&&(u.material.needsUpdate=!0,d=S,h=S.version,f=n.toneMapping),u.layers.enableAll(),w.unshift(u,u.geometry,u.material,0,0,null)):S&&S.isTexture&&(c===void 0&&(c=new an(new oc(2,2),new Kn({name:"BackgroundMaterial",uniforms:Bs(Zn.background.uniforms),vertexShader:Zn.background.vertexShader,fragmentShader:Zn.background.fragmentShader,side:ji,depthTest:!1,depthWrite:!1,fog:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),r.update(c)),c.material.uniforms.t2D.value=S,c.material.uniforms.backgroundIntensity.value=E.backgroundIntensity,c.material.toneMapped=st.getTransfer(S.colorSpace)!==xt,S.matrixAutoUpdate===!0&&S.updateMatrix(),c.material.uniforms.uvTransform.value.copy(S.matrix),(d!==S||h!==S.version||f!==n.toneMapping)&&(c.material.needsUpdate=!0,d=S,h=S.version,f=n.toneMapping),c.layers.enableAll(),w.unshift(c,c.geometry,c.material,0,0,null))}function p(w,E){w.getRGB(Ll,L0(n)),i.buffers.color.setClear(Ll.r,Ll.g,Ll.b,E,o)}return{getClearColor:function(){return a},setClearColor:function(w,E=1){a.set(w),l=E,p(a,l)},getClearAlpha:function(){return l},setClearAlpha:function(w){l=w,p(a,l)},render:_,addToRenderList:m}}function xT(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=h(null),s=r,o=!1;function a(v,M,j,z,Y){let J=!1,W=d(z,j,M);s!==W&&(s=W,c(s.object)),J=f(v,z,j,Y),J&&g(v,z,j,Y),Y!==null&&e.update(Y,n.ELEMENT_ARRAY_BUFFER),(J||o)&&(o=!1,S(v,M,j,z),Y!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(Y).buffer))}function l(){return n.createVertexArray()}function c(v){return n.bindVertexArray(v)}function u(v){return n.deleteVertexArray(v)}function d(v,M,j){let z=j.wireframe===!0,Y=i[v.id];Y===void 0&&(Y={},i[v.id]=Y);let J=Y[M.id];J===void 0&&(J={},Y[M.id]=J);let W=J[z];return W===void 0&&(W=h(l()),J[z]=W),W}function h(v){let M=[],j=[],z=[];for(let Y=0;Y<t;Y++)M[Y]=0,j[Y]=0,z[Y]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:M,enabledAttributes:j,attributeDivisors:z,object:v,attributes:{},index:null}}function f(v,M,j,z){let Y=s.attributes,J=M.attributes,W=0,K=j.getAttributes();for(let G in K)if(K[G].location>=0){let he=Y[G],xe=J[G];if(xe===void 0&&(G==="instanceMatrix"&&v.instanceMatrix&&(xe=v.instanceMatrix),G==="instanceColor"&&v.instanceColor&&(xe=v.instanceColor)),he===void 0||he.attribute!==xe||xe&&he.data!==xe.data)return!0;W++}return s.attributesNum!==W||s.index!==z}function g(v,M,j,z){let Y={},J=M.attributes,W=0,K=j.getAttributes();for(let G in K)if(K[G].location>=0){let he=J[G];he===void 0&&(G==="instanceMatrix"&&v.instanceMatrix&&(he=v.instanceMatrix),G==="instanceColor"&&v.instanceColor&&(he=v.instanceColor));let xe={};xe.attribute=he,he&&he.data&&(xe.data=he.data),Y[G]=xe,W++}s.attributes=Y,s.attributesNum=W,s.index=z}function _(){let v=s.newAttributes;for(let M=0,j=v.length;M<j;M++)v[M]=0}function m(v){p(v,0)}function p(v,M){let j=s.newAttributes,z=s.enabledAttributes,Y=s.attributeDivisors;j[v]=1,z[v]===0&&(n.enableVertexAttribArray(v),z[v]=1),Y[v]!==M&&(n.vertexAttribDivisor(v,M),Y[v]=M)}function w(){let v=s.newAttributes,M=s.enabledAttributes;for(let j=0,z=M.length;j<z;j++)M[j]!==v[j]&&(n.disableVertexAttribArray(j),M[j]=0)}function E(v,M,j,z,Y,J,W){W===!0?n.vertexAttribIPointer(v,M,j,Y,J):n.vertexAttribPointer(v,M,j,z,Y,J)}function S(v,M,j,z){_();let Y=z.attributes,J=j.getAttributes(),W=M.defaultAttributeValues;for(let K in J){let G=J[K];if(G.location>=0){let de=Y[K];if(de===void 0&&(K==="instanceMatrix"&&v.instanceMatrix&&(de=v.instanceMatrix),K==="instanceColor"&&v.instanceColor&&(de=v.instanceColor)),de!==void 0){let he=de.normalized,xe=de.itemSize,Je=e.get(de);if(Je===void 0)continue;let at=Je.buffer,$=Je.type,ee=Je.bytesPerElement,ye=$===n.INT||$===n.UNSIGNED_INT||de.gpuType===Yf;if(de.isInterleavedBufferAttribute){let fe=de.data,Pe=fe.stride,we=de.offset;if(fe.isInstancedInterleavedBuffer){for(let je=0;je<G.locationSize;je++)p(G.location+je,fe.meshPerAttribute);v.isInstancedMesh!==!0&&z._maxInstanceCount===void 0&&(z._maxInstanceCount=fe.meshPerAttribute*fe.count)}else for(let je=0;je<G.locationSize;je++)m(G.location+je);n.bindBuffer(n.ARRAY_BUFFER,at);for(let je=0;je<G.locationSize;je++)E(G.location+je,xe/G.locationSize,$,he,Pe*ee,(we+xe/G.locationSize*je)*ee,ye)}else{if(de.isInstancedBufferAttribute){for(let fe=0;fe<G.locationSize;fe++)p(G.location+fe,de.meshPerAttribute);v.isInstancedMesh!==!0&&z._maxInstanceCount===void 0&&(z._maxInstanceCount=de.meshPerAttribute*de.count)}else for(let fe=0;fe<G.locationSize;fe++)m(G.location+fe);n.bindBuffer(n.ARRAY_BUFFER,at);for(let fe=0;fe<G.locationSize;fe++)E(G.location+fe,xe/G.locationSize,$,he,xe*ee,xe/G.locationSize*fe*ee,ye)}}else if(W!==void 0){let he=W[K];if(he!==void 0)switch(he.length){case 2:n.vertexAttrib2fv(G.location,he);break;case 3:n.vertexAttrib3fv(G.location,he);break;case 4:n.vertexAttrib4fv(G.location,he);break;default:n.vertexAttrib1fv(G.location,he)}}}}w()}function k(){V();for(let v in i){let M=i[v];for(let j in M){let z=M[j];for(let Y in z)u(z[Y].object),delete z[Y];delete M[j]}delete i[v]}}function I(v){if(i[v.id]===void 0)return;let M=i[v.id];for(let j in M){let z=M[j];for(let Y in z)u(z[Y].object),delete z[Y];delete M[j]}delete i[v.id]}function C(v){for(let M in i){let j=i[M];if(j[v.id]===void 0)continue;let z=j[v.id];for(let Y in z)u(z[Y].object),delete z[Y];delete j[v.id]}}function V(){re(),o=!0,s!==r&&(s=r,c(s.object))}function re(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:V,resetDefaultState:re,dispose:k,releaseStatesOfGeometry:I,releaseStatesOfProgram:C,initAttributes:_,enableAttribute:m,disableUnusedAttributes:w}}function MT(n,e,t){let i;function r(c){i=c}function s(c,u){n.drawArrays(i,c,u),t.update(u,i,1)}function o(c,u,d){d!==0&&(n.drawArraysInstanced(i,c,u,d),t.update(u,i,d))}function a(c,u,d){if(d===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,c,0,u,0,d);let f=0;for(let g=0;g<d;g++)f+=u[g];t.update(f,i,1)}function l(c,u,d,h){if(d===0)return;let f=e.get("WEBGL_multi_draw");if(f===null)for(let g=0;g<c.length;g++)o(c[g],u[g],h[g]);else{f.multiDrawArraysInstancedWEBGL(i,c,0,u,0,h,0,d);let g=0;for(let _=0;_<d;_++)g+=u[_];for(let _=0;_<h.length;_++)t.update(g,i,h[_])}}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a,this.renderMultiDrawInstances=l}function ET(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let C=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(C.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(C){return!(C!==Un&&i.convert(C)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(C){let V=C===Io&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(C!==Mi&&i.convert(C)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&C!==vi&&!V)}function l(C){if(C==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";C="mediump"}return C==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let c=t.precision!==void 0?t.precision:"highp",u=l(c);u!==c&&(console.warn("THREE.WebGLRenderer:",c,"not supported, using",u,"instead."),c=u);let d=t.logarithmicDepthBuffer===!0,h=t.reverseDepthBuffer===!0&&e.has("EXT_clip_control");if(h===!0){let C=e.get("EXT_clip_control");C.clipControlEXT(C.LOWER_LEFT_EXT,C.ZERO_TO_ONE_EXT)}let f=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),g=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),_=n.getParameter(n.MAX_TEXTURE_SIZE),m=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),w=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),E=n.getParameter(n.MAX_VARYING_VECTORS),S=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),k=g>0,I=n.getParameter(n.MAX_SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:l,textureFormatReadable:o,textureTypeReadable:a,precision:c,logarithmicDepthBuffer:d,reverseDepthBuffer:h,maxTextures:f,maxVertexTextures:g,maxTextureSize:_,maxCubemapSize:m,maxAttributes:p,maxVertexUniforms:w,maxVaryings:E,maxFragmentUniforms:S,vertexTextures:k,maxSamples:I}}function bT(n){let e=this,t=null,i=0,r=!1,s=!1,o=new mi,a=new ke,l={value:null,needsUpdate:!1};this.uniform=l,this.numPlanes=0,this.numIntersection=0,this.init=function(d,h){let f=d.length!==0||h||i!==0||r;return r=h,i=d.length,f},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(d,h){t=u(d,h,0)},this.setState=function(d,h,f){let g=d.clippingPlanes,_=d.clipIntersection,m=d.clipShadows,p=n.get(d);if(!r||g===null||g.length===0||s&&!m)s?u(null):c();else{let w=s?0:i,E=w*4,S=p.clippingState||null;l.value=S,S=u(g,h,E,f);for(let k=0;k!==E;++k)S[k]=t[k];p.clippingState=S,this.numIntersection=_?this.numPlanes:0,this.numPlanes+=w}};function c(){l.value!==t&&(l.value=t,l.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(d,h,f,g){let _=d!==null?d.length:0,m=null;if(_!==0){if(m=l.value,g!==!0||m===null){let p=f+_*4,w=h.matrixWorldInverse;a.getNormalMatrix(w),(m===null||m.length<p)&&(m=new Float32Array(p));for(let E=0,S=f;E!==_;++E,S+=4)o.copy(d[E]).applyMatrix4(w,a),o.normal.toArray(m,S),m[S+3]=o.constant}l.value=m,l.needsUpdate=!0}return e.numPlanes=_,e.numIntersection=0,m}}function ST(n){let e=new WeakMap;function t(o,a){return a===zh?o.mapping=Ls:a===Gh&&(o.mapping=ks),o}function i(o){if(o&&o.isTexture){let a=o.mapping;if(a===zh||a===Gh)if(e.has(o)){let l=e.get(o).texture;return t(l,o.mapping)}else{let l=o.image;if(l&&l.height>0){let c=new Cf(l.height);return c.fromEquirectangularTexture(n,o),e.set(o,c),o.addEventListener("dispose",r),t(c.texture,o.mapping)}else return null}}return o}function r(o){let a=o.target;a.removeEventListener("dispose",r);let l=e.get(a);l!==void 0&&(e.delete(a),l.dispose())}function s(){e=new WeakMap}return{get:i,dispose:s}}var ac=class extends rc{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,l=r-t;if(this.view!==null&&this.view.enabled){let c=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=c*this.view.offsetX,o=s+c*this.view.width,a-=u*this.view.offsetY,l=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,l,this.near,this.far,this.coordinateSystem),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}},Is=4,Yy=[.125,.215,.35,.446,.526,.582],br=20,Ch=new ac,Zy=new Ye,Dh=null,Ah=0,Th=0,Ih=!1,xr=(1+Math.sqrt(5))/2,Ts=1/xr,Ky=[new B(-xr,Ts,0),new B(xr,Ts,0),new B(-Ts,0,xr),new B(Ts,0,xr),new B(0,xr,-Ts),new B(0,xr,Ts),new B(-1,1,-1),new B(1,1,-1),new B(-1,1,1),new B(1,1,1)],lc=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._lodPlanes=[],this._sizeLods=[],this._sigmas=[],this._blurMaterial=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._compileMaterial(this._blurMaterial)}fromScene(e,t=0,i=.1,r=100){Dh=this._renderer.getRenderTarget(),Ah=this._renderer.getActiveCubeFace(),Th=this._renderer.getActiveMipmapLevel(),Ih=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(256);let s=this._allocateTargets();return s.depthBuffer=!0,this._sceneToCubeUV(e,i,r,s),t>0&&this._blur(s,0,0,t),this._applyPMREM(s),this._cleanup(s),s}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=e0(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=Qy(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose()}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodPlanes.length;e++)this._lodPlanes[e].dispose()}_cleanup(e){this._renderer.setRenderTarget(Dh,Ah,Th),this._renderer.xr.enabled=Ih,e.scissorTest=!1,kl(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===Ls||e.mapping===ks?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),Dh=this._renderer.getRenderTarget(),Ah=this._renderer.getActiveCubeFace(),Th=this._renderer.getActiveMipmapLevel(),Ih=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:kn,minFilter:kn,generateMipmaps:!1,type:Io,format:Un,colorSpace:Yi,depthBuffer:!1},r=Jy(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=Jy(e,t,i);let{_lodMax:s}=this;({sizeLods:this._sizeLods,lodPlanes:this._lodPlanes,sigmas:this._sigmas}=wT(s)),this._blurMaterial=CT(s,e,t)}return r}_compileMaterial(e){let t=new an(this._lodPlanes[0],e);this._renderer.compile(t,Ch)}_sceneToCubeUV(e,t,i,r){let a=new Jt(90,1,t,i),l=[1,-1,1,1,1,1],c=[1,1,1,-1,-1,-1],u=this._renderer,d=u.autoClear,h=u.toneMapping;u.getClearColor(Zy),u.toneMapping=Wi,u.autoClear=!1;let f=new tc({name:"PMREM.Background",side:ln,depthWrite:!1,depthTest:!1}),g=new an(new Xi,f),_=!1,m=e.background;m?m.isColor&&(f.color.copy(m),e.background=null,_=!0):(f.color.copy(Zy),_=!0);for(let p=0;p<6;p++){let w=p%3;w===0?(a.up.set(0,l[p],0),a.lookAt(c[p],0,0)):w===1?(a.up.set(0,0,l[p]),a.lookAt(0,c[p],0)):(a.up.set(0,l[p],0),a.lookAt(0,0,c[p]));let E=this._cubeSize;kl(r,w*E,p>2?E:0,E,E),u.setRenderTarget(r),_&&u.render(g,a),u.render(e,a)}g.geometry.dispose(),g.material.dispose(),u.toneMapping=h,u.autoClear=d,e.background=m}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===Ls||e.mapping===ks;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=e0()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=Qy());let s=r?this._cubemapMaterial:this._equirectMaterial,o=new an(this._lodPlanes[0],s),a=s.uniforms;a.envMap.value=e;let l=this._cubeSize;kl(t,0,0,3*l,2*l),i.setRenderTarget(t),i.render(o,Ch)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodPlanes.length;for(let s=1;s<r;s++){let o=Math.sqrt(this._sigmas[s]*this._sigmas[s]-this._sigmas[s-1]*this._sigmas[s-1]),a=Ky[(r-s-1)%Ky.length];this._blur(e,s-1,s,o,a)}t.autoClear=i}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let l=this._renderer,c=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&console.error("blur direction must be either latitudinal or longitudinal!");let u=3,d=new an(this._lodPlanes[r],c),h=c.uniforms,f=this._sizeLods[i]-1,g=isFinite(s)?Math.PI/(2*f):2*Math.PI/(2*br-1),_=s/g,m=isFinite(s)?1+Math.floor(u*_):br;m>br&&console.warn(`sigmaRadians, ${s}, is too large and will clip, as it requested ${m} samples when the maximum is set to ${br}`);let p=[],w=0;for(let C=0;C<br;++C){let V=C/_,re=Math.exp(-V*V/2);p.push(re),C===0?w+=re:C<m&&(w+=2*re)}for(let C=0;C<p.length;C++)p[C]=p[C]/w;h.envMap.value=e.texture,h.samples.value=m,h.weights.value=p,h.latitudinal.value=o==="latitudinal",a&&(h.poleAxis.value=a);let{_lodMax:E}=this;h.dTheta.value=g,h.mipInt.value=E-i;let S=this._sizeLods[r],k=3*S*(r>E-Is?r-E+Is:0),I=4*(this._cubeSize-S);kl(t,k,I,3*S,2*S),l.setRenderTarget(t),l.render(d,Ch)}};function wT(n){let e=[],t=[],i=[],r=n,s=n-Is+1+Yy.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);t.push(a);let l=1/a;o>n-Is?l=Yy[o-n+Is-1]:o===0&&(l=0),i.push(l);let c=1/(a-2),u=-c,d=1+c,h=[u,u,d,u,d,d,u,u,d,d,u,d],f=6,g=6,_=3,m=2,p=1,w=new Float32Array(_*g*f),E=new Float32Array(m*g*f),S=new Float32Array(p*g*f);for(let I=0;I<f;I++){let C=I%3*2/3-1,V=I>2?0:-1,re=[C,V,0,C+2/3,V,0,C+2/3,V+1,0,C,V,0,C+2/3,V+1,0,C,V+1,0];w.set(re,_*g*I),E.set(h,m*g*I);let v=[I,I,I,I,I,I];S.set(v,p*g*I)}let k=new Ir;k.setAttribute("position",new Cn(w,_)),k.setAttribute("uv",new Cn(E,m)),k.setAttribute("faceIndex",new Cn(S,p)),e.push(k),r>Is&&r--}return{lodPlanes:e,sizeLods:t,sigmas:i}}function Jy(n,e,t){let i=new Ei(n,e,t);return i.texture.mapping=gc,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function kl(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function CT(n,e,t){let i=new Float32Array(br),r=new B(0,1,0);return new Kn({name:"SphericalGaussianBlur",defines:{n:br,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:np(),fragmentShader:`

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
		`,blending:Gi,depthTest:!1,depthWrite:!1})}function Qy(){return new Kn({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:np(),fragmentShader:`

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
		`,blending:Gi,depthTest:!1,depthWrite:!1})}function e0(){return new Kn({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:np(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:Gi,depthTest:!1,depthWrite:!1})}function np(){return`

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
	`}function DT(n){let e=new WeakMap,t=null;function i(a){if(a&&a.isTexture){let l=a.mapping,c=l===zh||l===Gh,u=l===Ls||l===ks;if(c||u){let d=e.get(a),h=d!==void 0?d.texture.pmremVersion:0;if(a.isRenderTargetTexture&&a.pmremVersion!==h)return t===null&&(t=new lc(n)),d=c?t.fromEquirectangular(a,d):t.fromCubemap(a,d),d.texture.pmremVersion=a.pmremVersion,e.set(a,d),d.texture;if(d!==void 0)return d.texture;{let f=a.image;return c&&f&&f.height>0||u&&f&&r(f)?(t===null&&(t=new lc(n)),d=c?t.fromEquirectangular(a):t.fromCubemap(a),d.texture.pmremVersion=a.pmremVersion,e.set(a,d),a.addEventListener("dispose",s),d.texture):null}}}return a}function r(a){let l=0,c=6;for(let u=0;u<c;u++)a[u]!==void 0&&l++;return l===c}function s(a){let l=a.target;l.removeEventListener("dispose",s);let c=e.get(l);c!==void 0&&(e.delete(l),c.dispose())}function o(){e=new WeakMap,t!==null&&(t.dispose(),t=null)}return{get:i,dispose:o}}function AT(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r;switch(i){case"WEBGL_depth_texture":r=n.getExtension("WEBGL_depth_texture")||n.getExtension("MOZ_WEBGL_depth_texture")||n.getExtension("WEBKIT_WEBGL_depth_texture");break;case"EXT_texture_filter_anisotropic":r=n.getExtension("EXT_texture_filter_anisotropic")||n.getExtension("MOZ_EXT_texture_filter_anisotropic")||n.getExtension("WEBKIT_EXT_texture_filter_anisotropic");break;case"WEBGL_compressed_texture_s3tc":r=n.getExtension("WEBGL_compressed_texture_s3tc")||n.getExtension("MOZ_WEBGL_compressed_texture_s3tc")||n.getExtension("WEBKIT_WEBGL_compressed_texture_s3tc");break;case"WEBGL_compressed_texture_pvrtc":r=n.getExtension("WEBGL_compressed_texture_pvrtc")||n.getExtension("WEBKIT_WEBGL_compressed_texture_pvrtc");break;default:r=n.getExtension(i)}return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&jl("THREE.WebGLRenderer: "+i+" extension not supported."),r}}}function TT(n,e,t,i){let r={},s=new WeakMap;function o(d){let h=d.target;h.index!==null&&e.remove(h.index);for(let g in h.attributes)e.remove(h.attributes[g]);for(let g in h.morphAttributes){let _=h.morphAttributes[g];for(let m=0,p=_.length;m<p;m++)e.remove(_[m])}h.removeEventListener("dispose",o),delete r[h.id];let f=s.get(h);f&&(e.remove(f),s.delete(h)),i.releaseStatesOfGeometry(h),h.isInstancedBufferGeometry===!0&&delete h._maxInstanceCount,t.memory.geometries--}function a(d,h){return r[h.id]===!0||(h.addEventListener("dispose",o),r[h.id]=!0,t.memory.geometries++),h}function l(d){let h=d.attributes;for(let g in h)e.update(h[g],n.ARRAY_BUFFER);let f=d.morphAttributes;for(let g in f){let _=f[g];for(let m=0,p=_.length;m<p;m++)e.update(_[m],n.ARRAY_BUFFER)}}function c(d){let h=[],f=d.index,g=d.attributes.position,_=0;if(f!==null){let w=f.array;_=f.version;for(let E=0,S=w.length;E<S;E+=3){let k=w[E+0],I=w[E+1],C=w[E+2];h.push(k,I,I,C,C,k)}}else if(g!==void 0){let w=g.array;_=g.version;for(let E=0,S=w.length/3-1;E<S;E+=3){let k=E+0,I=E+1,C=E+2;h.push(k,I,I,C,C,k)}}else return;let m=new(F0(h)?ic:nc)(h,1);m.version=_;let p=s.get(d);p&&e.remove(p),s.set(d,m)}function u(d){let h=s.get(d);if(h){let f=d.index;f!==null&&h.version<f.version&&c(d)}else c(d);return s.get(d)}return{get:a,update:l,getWireframeAttribute:u}}function IT(n,e,t){let i;function r(h){i=h}let s,o;function a(h){s=h.type,o=h.bytesPerElement}function l(h,f){n.drawElements(i,f,s,h*o),t.update(f,i,1)}function c(h,f,g){g!==0&&(n.drawElementsInstanced(i,f,s,h*o,g),t.update(f,i,g))}function u(h,f,g){if(g===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,f,0,s,h,0,g);let m=0;for(let p=0;p<g;p++)m+=f[p];t.update(m,i,1)}function d(h,f,g,_){if(g===0)return;let m=e.get("WEBGL_multi_draw");if(m===null)for(let p=0;p<h.length;p++)c(h[p]/o,f[p],_[p]);else{m.multiDrawElementsInstancedWEBGL(i,f,0,s,h,0,_,0,g);let p=0;for(let w=0;w<g;w++)p+=f[w];for(let w=0;w<_.length;w++)t.update(p,i,_[w])}}this.setMode=r,this.setIndex=a,this.render=l,this.renderInstances=c,this.renderMultiDraw=u,this.renderMultiDrawInstances=d}function RT(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:console.error("THREE.WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function NT(n,e,t){let i=new WeakMap,r=new wt;function s(o,a,l){let c=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,d=u!==void 0?u.length:0,h=i.get(a);if(h===void 0||h.count!==d){let v=function(){V.dispose(),i.delete(a),a.removeEventListener("dispose",v)};var f=v;h!==void 0&&h.texture.dispose();let g=a.morphAttributes.position!==void 0,_=a.morphAttributes.normal!==void 0,m=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],w=a.morphAttributes.normal||[],E=a.morphAttributes.color||[],S=0;g===!0&&(S=1),_===!0&&(S=2),m===!0&&(S=3);let k=a.attributes.position.count*S,I=1;k>e.maxTextureSize&&(I=Math.ceil(k/e.maxTextureSize),k=e.maxTextureSize);let C=new Float32Array(k*I*4*d),V=new Ql(C,k,I,d);V.type=vi,V.needsUpdate=!0;let re=S*4;for(let M=0;M<d;M++){let j=p[M],z=w[M],Y=E[M],J=k*I*4*M;for(let W=0;W<j.count;W++){let K=W*re;g===!0&&(r.fromBufferAttribute(j,W),C[J+K+0]=r.x,C[J+K+1]=r.y,C[J+K+2]=r.z,C[J+K+3]=0),_===!0&&(r.fromBufferAttribute(z,W),C[J+K+4]=r.x,C[J+K+5]=r.y,C[J+K+6]=r.z,C[J+K+7]=0),m===!0&&(r.fromBufferAttribute(Y,W),C[J+K+8]=r.x,C[J+K+9]=r.y,C[J+K+10]=r.z,C[J+K+11]=Y.itemSize===4?r.w:1)}}h={count:d,texture:V,size:new ot(k,I)},i.set(a,h),a.addEventListener("dispose",v)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)l.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let g=0;for(let m=0;m<c.length;m++)g+=c[m];let _=a.morphTargetsRelative?1:1-g;l.getUniforms().setValue(n,"morphTargetBaseInfluence",_),l.getUniforms().setValue(n,"morphTargetInfluences",c)}l.getUniforms().setValue(n,"morphTargetsTexture",h.texture,t),l.getUniforms().setValue(n,"morphTargetsTextureSize",h.size)}return{update:s}}function PT(n,e,t,i){let r=new WeakMap;function s(l){let c=i.render.frame,u=l.geometry,d=e.get(l,u);if(r.get(d)!==c&&(e.update(d),r.set(d,c)),l.isInstancedMesh&&(l.hasEventListener("dispose",a)===!1&&l.addEventListener("dispose",a),r.get(l)!==c&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),r.set(l,c))),l.isSkinnedMesh){let h=l.skeleton;r.get(h)!==c&&(h.update(),r.set(h,c))}return d}function o(){r=new WeakMap}function a(l){let c=l.target;c.removeEventListener("dispose",a),t.remove(c.instanceMatrix),c.instanceColor!==null&&t.remove(c.instanceColor)}return{update:s,dispose:o}}var cc=class extends Pr{constructor(e,t,i,r,s,o,a,l,c,u=Ns){if(u!==Ns&&u!==Vs)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");i===void 0&&u===Ns&&(i=Cr),i===void 0&&u===Vs&&(i=Us),super(null,r,s,o,a,l,u,i,c),this.isDepthTexture=!0,this.image={width:e,height:t},this.magFilter=a!==void 0?a:wn,this.minFilter=l!==void 0?l:wn,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},U0=new Pr,t0=new cc(1,1),V0=new Ql,B0=new bf,H0=new sc,n0=[],i0=[],r0=new Float32Array(16),s0=new Float32Array(9),o0=new Float32Array(4);function zs(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=n0[r];if(s===void 0&&(s=new Float32Array(r),n0[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function Ot(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function Lt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function yc(n,e){let t=i0[e];t===void 0&&(t=new Int32Array(e),i0[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function FT(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function OT(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Ot(t,e))return;n.uniform2fv(this.addr,e),Lt(t,e)}}function LT(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(Ot(t,e))return;n.uniform3fv(this.addr,e),Lt(t,e)}}function kT(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Ot(t,e))return;n.uniform4fv(this.addr,e),Lt(t,e)}}function UT(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Ot(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),Lt(t,e)}else{if(Ot(t,i))return;o0.set(i),n.uniformMatrix2fv(this.addr,!1,o0),Lt(t,i)}}function VT(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Ot(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),Lt(t,e)}else{if(Ot(t,i))return;s0.set(i),n.uniformMatrix3fv(this.addr,!1,s0),Lt(t,i)}}function BT(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Ot(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),Lt(t,e)}else{if(Ot(t,i))return;r0.set(i),n.uniformMatrix4fv(this.addr,!1,r0),Lt(t,i)}}function HT(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function zT(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Ot(t,e))return;n.uniform2iv(this.addr,e),Lt(t,e)}}function GT(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Ot(t,e))return;n.uniform3iv(this.addr,e),Lt(t,e)}}function WT(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Ot(t,e))return;n.uniform4iv(this.addr,e),Lt(t,e)}}function jT(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function $T(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Ot(t,e))return;n.uniform2uiv(this.addr,e),Lt(t,e)}}function qT(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Ot(t,e))return;n.uniform3uiv(this.addr,e),Lt(t,e)}}function XT(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Ot(t,e))return;n.uniform4uiv(this.addr,e),Lt(t,e)}}function YT(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(t0.compareFunction=P0,s=t0):s=U0,t.setTexture2D(e||s,r)}function ZT(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||B0,r)}function KT(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||H0,r)}function JT(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||V0,r)}function QT(n){switch(n){case 5126:return FT;case 35664:return OT;case 35665:return LT;case 35666:return kT;case 35674:return UT;case 35675:return VT;case 35676:return BT;case 5124:case 35670:return HT;case 35667:case 35671:return zT;case 35668:case 35672:return GT;case 35669:case 35673:return WT;case 5125:return jT;case 36294:return $T;case 36295:return qT;case 36296:return XT;case 35678:case 36198:case 36298:case 36306:case 35682:return YT;case 35679:case 36299:case 36307:return ZT;case 35680:case 36300:case 36308:case 36293:return KT;case 36289:case 36303:case 36311:case 36292:return JT}}function eI(n,e){n.uniform1fv(this.addr,e)}function tI(n,e){let t=zs(e,this.size,2);n.uniform2fv(this.addr,t)}function nI(n,e){let t=zs(e,this.size,3);n.uniform3fv(this.addr,t)}function iI(n,e){let t=zs(e,this.size,4);n.uniform4fv(this.addr,t)}function rI(n,e){let t=zs(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function sI(n,e){let t=zs(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function oI(n,e){let t=zs(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function aI(n,e){n.uniform1iv(this.addr,e)}function lI(n,e){n.uniform2iv(this.addr,e)}function cI(n,e){n.uniform3iv(this.addr,e)}function uI(n,e){n.uniform4iv(this.addr,e)}function dI(n,e){n.uniform1uiv(this.addr,e)}function hI(n,e){n.uniform2uiv(this.addr,e)}function fI(n,e){n.uniform3uiv(this.addr,e)}function pI(n,e){n.uniform4uiv(this.addr,e)}function mI(n,e,t){let i=this.cache,r=e.length,s=yc(t,r);Ot(i,s)||(n.uniform1iv(this.addr,s),Lt(i,s));for(let o=0;o!==r;++o)t.setTexture2D(e[o]||U0,s[o])}function gI(n,e,t){let i=this.cache,r=e.length,s=yc(t,r);Ot(i,s)||(n.uniform1iv(this.addr,s),Lt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||B0,s[o])}function vI(n,e,t){let i=this.cache,r=e.length,s=yc(t,r);Ot(i,s)||(n.uniform1iv(this.addr,s),Lt(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||H0,s[o])}function yI(n,e,t){let i=this.cache,r=e.length,s=yc(t,r);Ot(i,s)||(n.uniform1iv(this.addr,s),Lt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||V0,s[o])}function _I(n){switch(n){case 5126:return eI;case 35664:return tI;case 35665:return nI;case 35666:return iI;case 35674:return rI;case 35675:return sI;case 35676:return oI;case 5124:case 35670:return aI;case 35667:case 35671:return lI;case 35668:case 35672:return cI;case 35669:case 35673:return uI;case 5125:return dI;case 36294:return hI;case 36295:return fI;case 36296:return pI;case 35678:case 36198:case 36298:case 36306:case 35682:return mI;case 35679:case 36299:case 36307:return gI;case 35680:case 36300:case 36308:case 36293:return vI;case 36289:case 36303:case 36311:case 36292:return yI}}var Df=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=QT(t.type)}},Af=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=_I(t.type)}},Tf=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},Rh=/(\w+)(\])?(\[|\.)?/g;function a0(n,e){n.seq.push(e),n.map[e.id]=e}function xI(n,e,t){let i=n.name,r=i.length;for(Rh.lastIndex=0;;){let s=Rh.exec(i),o=Rh.lastIndex,a=s[1],l=s[2]==="]",c=s[3];if(l&&(a=a|0),c===void 0||c==="["&&o+2===r){a0(t,c===void 0?new Df(a,n,e):new Af(a,n,e));break}else{let d=t.map[a];d===void 0&&(d=new Tf(a),a0(t,d)),t=d}}}var Fs=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let r=0;r<i;++r){let s=e.getActiveUniform(t,r),o=e.getUniformLocation(t,s.name);xI(s,o,this)}}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],l=i[a.id];l.needsUpdate!==!1&&a.setValue(e,l.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function l0(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var MI=37297,EI=0;function bI(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}function SI(n){let e=st.getPrimaries(st.workingColorSpace),t=st.getPrimaries(n),i;switch(e===t?i="":e===Yl&&t===Xl?i="LinearDisplayP3ToLinearSRGB":e===Xl&&t===Yl&&(i="LinearSRGBToLinearDisplayP3"),n){case Yi:case vc:return[i,"LinearTransferOETF"];case Yn:case tp:return[i,"sRGBTransferOETF"];default:return console.warn("THREE.WebGLProgram: Unsupported color space:",n),[i,"LinearTransferOETF"]}}function c0(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),r=n.getShaderInfoLog(e).trim();if(i&&r==="")return"";let s=/ERROR: 0:(\d+)/.exec(r);if(s){let o=parseInt(s[1]);return t.toUpperCase()+`

`+r+`

`+bI(n.getShaderSource(e),o)}else return r}function wI(n,e){let t=SI(e);return`vec4 ${n}( vec4 value ) { return ${t[0]}( ${t[1]}( value ) ); }`}function CI(n,e){let t;switch(e){case Zw:t="Linear";break;case Kw:t="Reinhard";break;case Jw:t="Cineon";break;case Qw:t="ACESFilmic";break;case tC:t="AgX";break;case nC:t="Neutral";break;case eC:t="Custom";break;default:console.warn("THREE.WebGLProgram: Unsupported toneMapping:",e),t="Linear"}return"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var Ul=new B;function DI(){st.getLuminanceCoefficients(Ul);let n=Ul.x.toFixed(4),e=Ul.y.toFixed(4),t=Ul.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function AI(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(So).join(`
`)}function TI(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function II(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function So(n){return n!==""}function u0(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function d0(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var RI=/^[ \t]*#include +<([\w\d./]+)>/gm;function If(n){return n.replace(RI,PI)}var NI=new Map;function PI(n,e){let t=Le[e];if(t===void 0){let i=NI.get(e);if(i!==void 0)t=Le[i],console.warn('THREE.WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return If(t)}var FI=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function h0(n){return n.replace(FI,OI)}function OI(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function f0(n){let e=`precision ${n.precision} float;
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
#define LOW_PRECISION`),e}function LI(n){let e="SHADOWMAP_TYPE_BASIC";return n.shadowMapType===x0?e="SHADOWMAP_TYPE_PCF":n.shadowMapType===Tw?e="SHADOWMAP_TYPE_PCF_SOFT":n.shadowMapType===pi&&(e="SHADOWMAP_TYPE_VSM"),e}function kI(n){let e="ENVMAP_TYPE_CUBE";if(n.envMap)switch(n.envMapMode){case Ls:case ks:e="ENVMAP_TYPE_CUBE";break;case gc:e="ENVMAP_TYPE_CUBE_UV";break}return e}function UI(n){let e="ENVMAP_MODE_REFLECTION";if(n.envMap)switch(n.envMapMode){case ks:e="ENVMAP_MODE_REFRACTION";break}return e}function VI(n){let e="ENVMAP_BLENDING_NONE";if(n.envMap)switch(n.combine){case M0:e="ENVMAP_BLENDING_MULTIPLY";break;case Xw:e="ENVMAP_BLENDING_MIX";break;case Yw:e="ENVMAP_BLENDING_ADD";break}return e}function BI(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),7*16)),texelHeight:i,maxMip:t}}function HI(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,l=LI(t),c=kI(t),u=UI(t),d=VI(t),h=BI(t),f=AI(t),g=TI(s),_=r.createProgram(),m,p,w=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(So).join(`
`),m.length>0&&(m+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(So).join(`
`),p.length>0&&(p+=`
`)):(m=[f0(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+l:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGDEPTHBUF":"",t.reverseDepthBuffer?"#define USE_REVERSEDEPTHBUF":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(So).join(`
`),p=[f0(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+c:"",t.envMap?"#define "+u:"",t.envMap?"#define "+d:"",h?"#define CUBEUV_TEXEL_WIDTH "+h.texelWidth:"",h?"#define CUBEUV_TEXEL_HEIGHT "+h.texelHeight:"",h?"#define CUBEUV_MAX_MIP "+h.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor||t.batchingColor?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+l:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.logarithmicDepthBuffer?"#define USE_LOGDEPTHBUF":"",t.reverseDepthBuffer?"#define USE_REVERSEDEPTHBUF":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==Wi?"#define TONE_MAPPING":"",t.toneMapping!==Wi?Le.tonemapping_pars_fragment:"",t.toneMapping!==Wi?CI("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",Le.colorspace_pars_fragment,wI("linearToOutputTexel",t.outputColorSpace),DI(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(So).join(`
`)),o=If(o),o=u0(o,t),o=d0(o,t),a=If(a),a=u0(a,t),a=d0(a,t),o=h0(o),a=h0(a),t.isRawShaderMaterial!==!0&&(w=`#version 300 es
`,m=[f,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+m,p=["#define varying in",t.glslVersion===Ry?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===Ry?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let E=w+m+o,S=w+p+a,k=l0(r,r.VERTEX_SHADER,E),I=l0(r,r.FRAGMENT_SHADER,S);r.attachShader(_,k),r.attachShader(_,I),t.index0AttributeName!==void 0?r.bindAttribLocation(_,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(_,0,"position"),r.linkProgram(_);function C(M){if(n.debug.checkShaderErrors){let j=r.getProgramInfoLog(_).trim(),z=r.getShaderInfoLog(k).trim(),Y=r.getShaderInfoLog(I).trim(),J=!0,W=!0;if(r.getProgramParameter(_,r.LINK_STATUS)===!1)if(J=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,_,k,I);else{let K=c0(r,k,"vertex"),G=c0(r,I,"fragment");console.error("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(_,r.VALIDATE_STATUS)+`

Material Name: `+M.name+`
Material Type: `+M.type+`

Program Info Log: `+j+`
`+K+`
`+G)}else j!==""?console.warn("THREE.WebGLProgram: Program Info Log:",j):(z===""||Y==="")&&(W=!1);W&&(M.diagnostics={runnable:J,programLog:j,vertexShader:{log:z,prefix:m},fragmentShader:{log:Y,prefix:p}})}r.deleteShader(k),r.deleteShader(I),V=new Fs(r,_),re=II(r,_)}let V;this.getUniforms=function(){return V===void 0&&C(this),V};let re;this.getAttributes=function(){return re===void 0&&C(this),re};let v=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return v===!1&&(v=r.getProgramParameter(_,MI)),v},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(_),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=EI++,this.cacheKey=e,this.usedTimes=1,this.program=_,this.vertexShader=k,this.fragmentShader=I,this}var zI=0,Rf=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new Nf(e),t.set(e,i)),i}},Nf=class{constructor(e){this.id=zI++,this.code=e,this.usedTimes=0}};function GI(n,e,t,i,r,s,o){let a=new ec,l=new Rf,c=new Set,u=[],d=r.logarithmicDepthBuffer,h=r.reverseDepthBuffer,f=r.vertexTextures,g=r.precision,_={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distanceRGBA",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function m(v){return c.add(v),v===0?"uv":`uv${v}`}function p(v,M,j,z,Y){let J=z.fog,W=Y.geometry,K=v.isMeshStandardMaterial?z.environment:null,G=(v.isMeshStandardMaterial?t:e).get(v.envMap||K),de=G&&G.mapping===gc?G.image.height:null,he=_[v.type];v.precision!==null&&(g=r.getMaxPrecision(v.precision),g!==v.precision&&console.warn("THREE.WebGLProgram.getParameters:",v.precision,"not supported, using",g,"instead."));let xe=W.morphAttributes.position||W.morphAttributes.normal||W.morphAttributes.color,Je=xe!==void 0?xe.length:0,at=0;W.morphAttributes.position!==void 0&&(at=1),W.morphAttributes.normal!==void 0&&(at=2),W.morphAttributes.color!==void 0&&(at=3);let $,ee,ye,fe;if(he){let en=Zn[he];$=en.vertexShader,ee=en.fragmentShader}else $=v.vertexShader,ee=v.fragmentShader,l.update(v),ye=l.getVertexShaderID(v),fe=l.getFragmentShaderID(v);let Pe=n.getRenderTarget(),we=Y.isInstancedMesh===!0,je=Y.isBatchedMesh===!0,ct=!!v.map,$e=!!v.matcap,T=!!G,cn=!!v.aoMap,ze=!!v.lightMap,Ze=!!v.bumpMap,Ae=!!v.normalMap,mt=!!v.displacementMap,Re=!!v.emissiveMap,b=!!v.metalnessMap,y=!!v.roughnessMap,O=v.anisotropy>0,X=v.clearcoat>0,Q=v.dispersion>0,q=v.iridescence>0,Me=v.sheen>0,oe=v.transmission>0,pe=O&&!!v.anisotropyMap,Ke=X&&!!v.clearcoatMap,te=X&&!!v.clearcoatNormalMap,me=X&&!!v.clearcoatRoughnessMap,Te=q&&!!v.iridescenceMap,Ie=q&&!!v.iridescenceThicknessMap,ge=Me&&!!v.sheenColorMap,Ge=Me&&!!v.sheenRoughnessMap,Fe=!!v.specularMap,ht=!!v.specularColorMap,R=!!v.specularIntensityMap,ce=oe&&!!v.transmissionMap,H=oe&&!!v.thicknessMap,Z=!!v.gradientMap,ae=!!v.alphaMap,ue=v.alphaTest>0,qe=!!v.alphaHash,Rt=!!v.extensions,Qt=Wi;v.toneMapped&&(Pe===null||Pe.isXRRenderTarget===!0)&&(Qt=n.toneMapping);let Qe={shaderID:he,shaderType:v.type,shaderName:v.name,vertexShader:$,fragmentShader:ee,defines:v.defines,customVertexShaderID:ye,customFragmentShaderID:fe,isRawShaderMaterial:v.isRawShaderMaterial===!0,glslVersion:v.glslVersion,precision:g,batching:je,batchingColor:je&&Y._colorsTexture!==null,instancing:we,instancingColor:we&&Y.instanceColor!==null,instancingMorph:we&&Y.morphTexture!==null,supportsVertexTextures:f,outputColorSpace:Pe===null?n.outputColorSpace:Pe.isXRRenderTarget===!0?Pe.texture.colorSpace:Yi,alphaToCoverage:!!v.alphaToCoverage,map:ct,matcap:$e,envMap:T,envMapMode:T&&G.mapping,envMapCubeUVHeight:de,aoMap:cn,lightMap:ze,bumpMap:Ze,normalMap:Ae,displacementMap:f&&mt,emissiveMap:Re,normalMapObjectSpace:Ae&&v.normalMapType===oC,normalMapTangentSpace:Ae&&v.normalMapType===N0,metalnessMap:b,roughnessMap:y,anisotropy:O,anisotropyMap:pe,clearcoat:X,clearcoatMap:Ke,clearcoatNormalMap:te,clearcoatRoughnessMap:me,dispersion:Q,iridescence:q,iridescenceMap:Te,iridescenceThicknessMap:Ie,sheen:Me,sheenColorMap:ge,sheenRoughnessMap:Ge,specularMap:Fe,specularColorMap:ht,specularIntensityMap:R,transmission:oe,transmissionMap:ce,thicknessMap:H,gradientMap:Z,opaque:v.transparent===!1&&v.blending===Rs&&v.alphaToCoverage===!1,alphaMap:ae,alphaTest:ue,alphaHash:qe,combine:v.combine,mapUv:ct&&m(v.map.channel),aoMapUv:cn&&m(v.aoMap.channel),lightMapUv:ze&&m(v.lightMap.channel),bumpMapUv:Ze&&m(v.bumpMap.channel),normalMapUv:Ae&&m(v.normalMap.channel),displacementMapUv:mt&&m(v.displacementMap.channel),emissiveMapUv:Re&&m(v.emissiveMap.channel),metalnessMapUv:b&&m(v.metalnessMap.channel),roughnessMapUv:y&&m(v.roughnessMap.channel),anisotropyMapUv:pe&&m(v.anisotropyMap.channel),clearcoatMapUv:Ke&&m(v.clearcoatMap.channel),clearcoatNormalMapUv:te&&m(v.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:me&&m(v.clearcoatRoughnessMap.channel),iridescenceMapUv:Te&&m(v.iridescenceMap.channel),iridescenceThicknessMapUv:Ie&&m(v.iridescenceThicknessMap.channel),sheenColorMapUv:ge&&m(v.sheenColorMap.channel),sheenRoughnessMapUv:Ge&&m(v.sheenRoughnessMap.channel),specularMapUv:Fe&&m(v.specularMap.channel),specularColorMapUv:ht&&m(v.specularColorMap.channel),specularIntensityMapUv:R&&m(v.specularIntensityMap.channel),transmissionMapUv:ce&&m(v.transmissionMap.channel),thicknessMapUv:H&&m(v.thicknessMap.channel),alphaMapUv:ae&&m(v.alphaMap.channel),vertexTangents:!!W.attributes.tangent&&(Ae||O),vertexColors:v.vertexColors,vertexAlphas:v.vertexColors===!0&&!!W.attributes.color&&W.attributes.color.itemSize===4,pointsUvs:Y.isPoints===!0&&!!W.attributes.uv&&(ct||ae),fog:!!J,useFog:v.fog===!0,fogExp2:!!J&&J.isFogExp2,flatShading:v.flatShading===!0,sizeAttenuation:v.sizeAttenuation===!0,logarithmicDepthBuffer:d,reverseDepthBuffer:h,skinning:Y.isSkinnedMesh===!0,morphTargets:W.morphAttributes.position!==void 0,morphNormals:W.morphAttributes.normal!==void 0,morphColors:W.morphAttributes.color!==void 0,morphTargetsCount:Je,morphTextureStride:at,numDirLights:M.directional.length,numPointLights:M.point.length,numSpotLights:M.spot.length,numSpotLightMaps:M.spotLightMap.length,numRectAreaLights:M.rectArea.length,numHemiLights:M.hemi.length,numDirLightShadows:M.directionalShadowMap.length,numPointLightShadows:M.pointShadowMap.length,numSpotLightShadows:M.spotShadowMap.length,numSpotLightShadowsWithMaps:M.numSpotLightShadowsWithMaps,numLightProbes:M.numLightProbes,numClippingPlanes:o.numPlanes,numClipIntersection:o.numIntersection,dithering:v.dithering,shadowMapEnabled:n.shadowMap.enabled&&j.length>0,shadowMapType:n.shadowMap.type,toneMapping:Qt,decodeVideoTexture:ct&&v.map.isVideoTexture===!0&&st.getTransfer(v.map.colorSpace)===xt,premultipliedAlpha:v.premultipliedAlpha,doubleSided:v.side===gi,flipSided:v.side===ln,useDepthPacking:v.depthPacking>=0,depthPacking:v.depthPacking||0,index0AttributeName:v.index0AttributeName,extensionClipCullDistance:Rt&&v.extensions.clipCullDistance===!0&&i.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(Rt&&v.extensions.multiDraw===!0||je)&&i.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:i.has("KHR_parallel_shader_compile"),customProgramCacheKey:v.customProgramCacheKey()};return Qe.vertexUv1s=c.has(1),Qe.vertexUv2s=c.has(2),Qe.vertexUv3s=c.has(3),c.clear(),Qe}function w(v){let M=[];if(v.shaderID?M.push(v.shaderID):(M.push(v.customVertexShaderID),M.push(v.customFragmentShaderID)),v.defines!==void 0)for(let j in v.defines)M.push(j),M.push(v.defines[j]);return v.isRawShaderMaterial===!1&&(E(M,v),S(M,v),M.push(n.outputColorSpace)),M.push(v.customProgramCacheKey),M.join()}function E(v,M){v.push(M.precision),v.push(M.outputColorSpace),v.push(M.envMapMode),v.push(M.envMapCubeUVHeight),v.push(M.mapUv),v.push(M.alphaMapUv),v.push(M.lightMapUv),v.push(M.aoMapUv),v.push(M.bumpMapUv),v.push(M.normalMapUv),v.push(M.displacementMapUv),v.push(M.emissiveMapUv),v.push(M.metalnessMapUv),v.push(M.roughnessMapUv),v.push(M.anisotropyMapUv),v.push(M.clearcoatMapUv),v.push(M.clearcoatNormalMapUv),v.push(M.clearcoatRoughnessMapUv),v.push(M.iridescenceMapUv),v.push(M.iridescenceThicknessMapUv),v.push(M.sheenColorMapUv),v.push(M.sheenRoughnessMapUv),v.push(M.specularMapUv),v.push(M.specularColorMapUv),v.push(M.specularIntensityMapUv),v.push(M.transmissionMapUv),v.push(M.thicknessMapUv),v.push(M.combine),v.push(M.fogExp2),v.push(M.sizeAttenuation),v.push(M.morphTargetsCount),v.push(M.morphAttributeCount),v.push(M.numDirLights),v.push(M.numPointLights),v.push(M.numSpotLights),v.push(M.numSpotLightMaps),v.push(M.numHemiLights),v.push(M.numRectAreaLights),v.push(M.numDirLightShadows),v.push(M.numPointLightShadows),v.push(M.numSpotLightShadows),v.push(M.numSpotLightShadowsWithMaps),v.push(M.numLightProbes),v.push(M.shadowMapType),v.push(M.toneMapping),v.push(M.numClippingPlanes),v.push(M.numClipIntersection),v.push(M.depthPacking)}function S(v,M){a.disableAll(),M.supportsVertexTextures&&a.enable(0),M.instancing&&a.enable(1),M.instancingColor&&a.enable(2),M.instancingMorph&&a.enable(3),M.matcap&&a.enable(4),M.envMap&&a.enable(5),M.normalMapObjectSpace&&a.enable(6),M.normalMapTangentSpace&&a.enable(7),M.clearcoat&&a.enable(8),M.iridescence&&a.enable(9),M.alphaTest&&a.enable(10),M.vertexColors&&a.enable(11),M.vertexAlphas&&a.enable(12),M.vertexUv1s&&a.enable(13),M.vertexUv2s&&a.enable(14),M.vertexUv3s&&a.enable(15),M.vertexTangents&&a.enable(16),M.anisotropy&&a.enable(17),M.alphaHash&&a.enable(18),M.batching&&a.enable(19),M.dispersion&&a.enable(20),M.batchingColor&&a.enable(21),v.push(a.mask),a.disableAll(),M.fog&&a.enable(0),M.useFog&&a.enable(1),M.flatShading&&a.enable(2),M.logarithmicDepthBuffer&&a.enable(3),M.reverseDepthBuffer&&a.enable(4),M.skinning&&a.enable(5),M.morphTargets&&a.enable(6),M.morphNormals&&a.enable(7),M.morphColors&&a.enable(8),M.premultipliedAlpha&&a.enable(9),M.shadowMapEnabled&&a.enable(10),M.doubleSided&&a.enable(11),M.flipSided&&a.enable(12),M.useDepthPacking&&a.enable(13),M.dithering&&a.enable(14),M.transmission&&a.enable(15),M.sheen&&a.enable(16),M.opaque&&a.enable(17),M.pointsUvs&&a.enable(18),M.decodeVideoTexture&&a.enable(19),M.alphaToCoverage&&a.enable(20),v.push(a.mask)}function k(v){let M=_[v.type],j;if(M){let z=Zn[M];j=PC.clone(z.uniforms)}else j=v.uniforms;return j}function I(v,M){let j;for(let z=0,Y=u.length;z<Y;z++){let J=u[z];if(J.cacheKey===M){j=J,++j.usedTimes;break}}return j===void 0&&(j=new HI(n,M,v,s),u.push(j)),j}function C(v){if(--v.usedTimes===0){let M=u.indexOf(v);u[M]=u[u.length-1],u.pop(),v.destroy()}}function V(v){l.remove(v)}function re(){l.dispose()}return{getParameters:p,getProgramCacheKey:w,getUniforms:k,acquireProgram:I,releaseProgram:C,releaseShaderCache:V,programs:u,dispose:re}}function WI(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,l){n.get(o)[a]=l}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function jI(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.z!==e.z?n.z-e.z:n.id-e.id}function p0(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function m0(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(d,h,f,g,_,m){let p=n[e];return p===void 0?(p={id:d.id,object:d,geometry:h,material:f,groupOrder:g,renderOrder:d.renderOrder,z:_,group:m},n[e]=p):(p.id=d.id,p.object=d,p.geometry=h,p.material=f,p.groupOrder=g,p.renderOrder=d.renderOrder,p.z=_,p.group=m),e++,p}function a(d,h,f,g,_,m){let p=o(d,h,f,g,_,m);f.transmission>0?i.push(p):f.transparent===!0?r.push(p):t.push(p)}function l(d,h,f,g,_,m){let p=o(d,h,f,g,_,m);f.transmission>0?i.unshift(p):f.transparent===!0?r.unshift(p):t.unshift(p)}function c(d,h){t.length>1&&t.sort(d||jI),i.length>1&&i.sort(h||p0),r.length>1&&r.sort(h||p0)}function u(){for(let d=e,h=n.length;d<h;d++){let f=n[d];if(f.id===null)break;f.id=null,f.object=null,f.geometry=null,f.material=null,f.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:a,unshift:l,finish:u,sort:c}}function $I(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new m0,n.set(i,[o])):r>=s.length?(o=new m0,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function qI(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new B,color:new Ye};break;case"SpotLight":t={position:new B,direction:new B,color:new Ye,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new B,color:new Ye,distance:0,decay:0};break;case"HemisphereLight":t={direction:new B,skyColor:new Ye,groundColor:new Ye};break;case"RectAreaLight":t={color:new Ye,position:new B,halfWidth:new B,halfHeight:new B};break}return n[e.id]=t,t}}}function XI(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new ot};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new ot};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new ot,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var YI=0;function ZI(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function KI(n){let e=new qI,t=XI(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let c=0;c<9;c++)i.probe.push(new B);let r=new B,s=new It,o=new It;function a(c){let u=0,d=0,h=0;for(let re=0;re<9;re++)i.probe[re].set(0,0,0);let f=0,g=0,_=0,m=0,p=0,w=0,E=0,S=0,k=0,I=0,C=0;c.sort(ZI);for(let re=0,v=c.length;re<v;re++){let M=c[re],j=M.color,z=M.intensity,Y=M.distance,J=M.shadow&&M.shadow.map?M.shadow.map.texture:null;if(M.isAmbientLight)u+=j.r*z,d+=j.g*z,h+=j.b*z;else if(M.isLightProbe){for(let W=0;W<9;W++)i.probe[W].addScaledVector(M.sh.coefficients[W],z);C++}else if(M.isDirectionalLight){let W=e.get(M);if(W.color.copy(M.color).multiplyScalar(M.intensity),M.castShadow){let K=M.shadow,G=t.get(M);G.shadowIntensity=K.intensity,G.shadowBias=K.bias,G.shadowNormalBias=K.normalBias,G.shadowRadius=K.radius,G.shadowMapSize=K.mapSize,i.directionalShadow[f]=G,i.directionalShadowMap[f]=J,i.directionalShadowMatrix[f]=M.shadow.matrix,w++}i.directional[f]=W,f++}else if(M.isSpotLight){let W=e.get(M);W.position.setFromMatrixPosition(M.matrixWorld),W.color.copy(j).multiplyScalar(z),W.distance=Y,W.coneCos=Math.cos(M.angle),W.penumbraCos=Math.cos(M.angle*(1-M.penumbra)),W.decay=M.decay,i.spot[_]=W;let K=M.shadow;if(M.map&&(i.spotLightMap[k]=M.map,k++,K.updateMatrices(M),M.castShadow&&I++),i.spotLightMatrix[_]=K.matrix,M.castShadow){let G=t.get(M);G.shadowIntensity=K.intensity,G.shadowBias=K.bias,G.shadowNormalBias=K.normalBias,G.shadowRadius=K.radius,G.shadowMapSize=K.mapSize,i.spotShadow[_]=G,i.spotShadowMap[_]=J,S++}_++}else if(M.isRectAreaLight){let W=e.get(M);W.color.copy(j).multiplyScalar(z),W.halfWidth.set(M.width*.5,0,0),W.halfHeight.set(0,M.height*.5,0),i.rectArea[m]=W,m++}else if(M.isPointLight){let W=e.get(M);if(W.color.copy(M.color).multiplyScalar(M.intensity),W.distance=M.distance,W.decay=M.decay,M.castShadow){let K=M.shadow,G=t.get(M);G.shadowIntensity=K.intensity,G.shadowBias=K.bias,G.shadowNormalBias=K.normalBias,G.shadowRadius=K.radius,G.shadowMapSize=K.mapSize,G.shadowCameraNear=K.camera.near,G.shadowCameraFar=K.camera.far,i.pointShadow[g]=G,i.pointShadowMap[g]=J,i.pointShadowMatrix[g]=M.shadow.matrix,E++}i.point[g]=W,g++}else if(M.isHemisphereLight){let W=e.get(M);W.skyColor.copy(M.color).multiplyScalar(z),W.groundColor.copy(M.groundColor).multiplyScalar(z),i.hemi[p]=W,p++}}m>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=se.LTC_FLOAT_1,i.rectAreaLTC2=se.LTC_FLOAT_2):(i.rectAreaLTC1=se.LTC_HALF_1,i.rectAreaLTC2=se.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=d,i.ambient[2]=h;let V=i.hash;(V.directionalLength!==f||V.pointLength!==g||V.spotLength!==_||V.rectAreaLength!==m||V.hemiLength!==p||V.numDirectionalShadows!==w||V.numPointShadows!==E||V.numSpotShadows!==S||V.numSpotMaps!==k||V.numLightProbes!==C)&&(i.directional.length=f,i.spot.length=_,i.rectArea.length=m,i.point.length=g,i.hemi.length=p,i.directionalShadow.length=w,i.directionalShadowMap.length=w,i.pointShadow.length=E,i.pointShadowMap.length=E,i.spotShadow.length=S,i.spotShadowMap.length=S,i.directionalShadowMatrix.length=w,i.pointShadowMatrix.length=E,i.spotLightMatrix.length=S+k-I,i.spotLightMap.length=k,i.numSpotLightShadowsWithMaps=I,i.numLightProbes=C,V.directionalLength=f,V.pointLength=g,V.spotLength=_,V.rectAreaLength=m,V.hemiLength=p,V.numDirectionalShadows=w,V.numPointShadows=E,V.numSpotShadows=S,V.numSpotMaps=k,V.numLightProbes=C,i.version=YI++)}function l(c,u){let d=0,h=0,f=0,g=0,_=0,m=u.matrixWorldInverse;for(let p=0,w=c.length;p<w;p++){let E=c[p];if(E.isDirectionalLight){let S=i.directional[d];S.direction.setFromMatrixPosition(E.matrixWorld),r.setFromMatrixPosition(E.target.matrixWorld),S.direction.sub(r),S.direction.transformDirection(m),d++}else if(E.isSpotLight){let S=i.spot[f];S.position.setFromMatrixPosition(E.matrixWorld),S.position.applyMatrix4(m),S.direction.setFromMatrixPosition(E.matrixWorld),r.setFromMatrixPosition(E.target.matrixWorld),S.direction.sub(r),S.direction.transformDirection(m),f++}else if(E.isRectAreaLight){let S=i.rectArea[g];S.position.setFromMatrixPosition(E.matrixWorld),S.position.applyMatrix4(m),o.identity(),s.copy(E.matrixWorld),s.premultiply(m),o.extractRotation(s),S.halfWidth.set(E.width*.5,0,0),S.halfHeight.set(0,E.height*.5,0),S.halfWidth.applyMatrix4(o),S.halfHeight.applyMatrix4(o),g++}else if(E.isPointLight){let S=i.point[h];S.position.setFromMatrixPosition(E.matrixWorld),S.position.applyMatrix4(m),h++}else if(E.isHemisphereLight){let S=i.hemi[_];S.direction.setFromMatrixPosition(E.matrixWorld),S.direction.transformDirection(m),_++}}}return{setup:a,setupView:l,state:i}}function g0(n){let e=new KI(n),t=[],i=[];function r(u){c.camera=u,t.length=0,i.length=0}function s(u){t.push(u)}function o(u){i.push(u)}function a(){e.setup(t)}function l(u){e.setupView(t,u)}let c={lightsArray:t,shadowsArray:i,camera:null,lights:e,transmissionRenderTarget:{}};return{init:r,state:c,setupLights:a,setupLightsView:l,pushLight:s,pushShadow:o}}function JI(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new g0(n),e.set(r,[a])):s>=o.length?(a=new g0(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var Pf=class extends Tr{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=rC,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},Ff=class extends Tr{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}},QI=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,e1=`uniform sampler2D shadow_pass;
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
}`;function t1(n,e,t){let i=new Ao,r=new ot,s=new ot,o=new wt,a=new Pf({depthPacking:sC}),l=new Ff,c={},u=t.maxTextureSize,d={[ji]:ln,[ln]:ji,[gi]:gi},h=new Kn({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new ot},radius:{value:4}},vertexShader:QI,fragmentShader:e1}),f=h.clone();f.defines.HORIZONTAL_PASS=1;let g=new Ir;g.setAttribute("position",new Cn(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let _=new an(g,h),m=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=x0;let p=this.type;this.render=function(I,C,V){if(m.enabled===!1||m.autoUpdate===!1&&m.needsUpdate===!1||I.length===0)return;let re=n.getRenderTarget(),v=n.getActiveCubeFace(),M=n.getActiveMipmapLevel(),j=n.state;j.setBlending(Gi),j.buffers.color.setClear(1,1,1,1),j.buffers.depth.setTest(!0),j.setScissorTest(!1);let z=p!==pi&&this.type===pi,Y=p===pi&&this.type!==pi;for(let J=0,W=I.length;J<W;J++){let K=I[J],G=K.shadow;if(G===void 0){console.warn("THREE.WebGLShadowMap:",K,"has no shadow.");continue}if(G.autoUpdate===!1&&G.needsUpdate===!1)continue;r.copy(G.mapSize);let de=G.getFrameExtents();if(r.multiply(de),s.copy(G.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/de.x),r.x=s.x*de.x,G.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/de.y),r.y=s.y*de.y,G.mapSize.y=s.y)),G.map===null||z===!0||Y===!0){let xe=this.type!==pi?{minFilter:wn,magFilter:wn}:{};G.map!==null&&G.map.dispose(),G.map=new Ei(r.x,r.y,xe),G.map.texture.name=K.name+".shadowMap",G.camera.updateProjectionMatrix()}n.setRenderTarget(G.map),n.clear();let he=G.getViewportCount();for(let xe=0;xe<he;xe++){let Je=G.getViewport(xe);o.set(s.x*Je.x,s.y*Je.y,s.x*Je.z,s.y*Je.w),j.viewport(o),G.updateMatrices(K,xe),i=G.getFrustum(),S(C,V,G.camera,K,this.type)}G.isPointLightShadow!==!0&&this.type===pi&&w(G,V),G.needsUpdate=!1}p=this.type,m.needsUpdate=!1,n.setRenderTarget(re,v,M)};function w(I,C){let V=e.update(_);h.defines.VSM_SAMPLES!==I.blurSamples&&(h.defines.VSM_SAMPLES=I.blurSamples,f.defines.VSM_SAMPLES=I.blurSamples,h.needsUpdate=!0,f.needsUpdate=!0),I.mapPass===null&&(I.mapPass=new Ei(r.x,r.y)),h.uniforms.shadow_pass.value=I.map.texture,h.uniforms.resolution.value=I.mapSize,h.uniforms.radius.value=I.radius,n.setRenderTarget(I.mapPass),n.clear(),n.renderBufferDirect(C,null,V,h,_,null),f.uniforms.shadow_pass.value=I.mapPass.texture,f.uniforms.resolution.value=I.mapSize,f.uniforms.radius.value=I.radius,n.setRenderTarget(I.map),n.clear(),n.renderBufferDirect(C,null,V,f,_,null)}function E(I,C,V,re){let v=null,M=V.isPointLight===!0?I.customDistanceMaterial:I.customDepthMaterial;if(M!==void 0)v=M;else if(v=V.isPointLight===!0?l:a,n.localClippingEnabled&&C.clipShadows===!0&&Array.isArray(C.clippingPlanes)&&C.clippingPlanes.length!==0||C.displacementMap&&C.displacementScale!==0||C.alphaMap&&C.alphaTest>0||C.map&&C.alphaTest>0){let j=v.uuid,z=C.uuid,Y=c[j];Y===void 0&&(Y={},c[j]=Y);let J=Y[z];J===void 0&&(J=v.clone(),Y[z]=J,C.addEventListener("dispose",k)),v=J}if(v.visible=C.visible,v.wireframe=C.wireframe,re===pi?v.side=C.shadowSide!==null?C.shadowSide:C.side:v.side=C.shadowSide!==null?C.shadowSide:d[C.side],v.alphaMap=C.alphaMap,v.alphaTest=C.alphaTest,v.map=C.map,v.clipShadows=C.clipShadows,v.clippingPlanes=C.clippingPlanes,v.clipIntersection=C.clipIntersection,v.displacementMap=C.displacementMap,v.displacementScale=C.displacementScale,v.displacementBias=C.displacementBias,v.wireframeLinewidth=C.wireframeLinewidth,v.linewidth=C.linewidth,V.isPointLight===!0&&v.isMeshDistanceMaterial===!0){let j=n.properties.get(v);j.light=V}return v}function S(I,C,V,re,v){if(I.visible===!1)return;if(I.layers.test(C.layers)&&(I.isMesh||I.isLine||I.isPoints)&&(I.castShadow||I.receiveShadow&&v===pi)&&(!I.frustumCulled||i.intersectsObject(I))){I.modelViewMatrix.multiplyMatrices(V.matrixWorldInverse,I.matrixWorld);let z=e.update(I),Y=I.material;if(Array.isArray(Y)){let J=z.groups;for(let W=0,K=J.length;W<K;W++){let G=J[W],de=Y[G.materialIndex];if(de&&de.visible){let he=E(I,de,re,v);I.onBeforeShadow(n,I,C,V,z,he,G),n.renderBufferDirect(V,null,z,he,I,G),I.onAfterShadow(n,I,C,V,z,he,G)}}}else if(Y.visible){let J=E(I,Y,re,v);I.onBeforeShadow(n,I,C,V,z,J,null),n.renderBufferDirect(V,null,z,J,I,null),I.onAfterShadow(n,I,C,V,z,J,null)}}let j=I.children;for(let z=0,Y=j.length;z<Y;z++)S(j[z],C,V,re,v)}function k(I){I.target.removeEventListener("dispose",k);for(let V in c){let re=c[V],v=I.target.uuid;v in re&&(re[v].dispose(),delete re[v])}}}var n1={[Oh]:Lh,[kh]:Bh,[Uh]:Hh,[Os]:Vh,[Lh]:Oh,[Bh]:kh,[Hh]:Uh,[Vh]:Os};function i1(n){function e(){let R=!1,ce=new wt,H=null,Z=new wt(0,0,0,0);return{setMask:function(ae){H!==ae&&!R&&(n.colorMask(ae,ae,ae,ae),H=ae)},setLocked:function(ae){R=ae},setClear:function(ae,ue,qe,Rt,Qt){Qt===!0&&(ae*=Rt,ue*=Rt,qe*=Rt),ce.set(ae,ue,qe,Rt),Z.equals(ce)===!1&&(n.clearColor(ae,ue,qe,Rt),Z.copy(ce))},reset:function(){R=!1,H=null,Z.set(-1,0,0,0)}}}function t(){let R=!1,ce=!1,H=null,Z=null,ae=null;return{setReversed:function(ue){ce=ue},setTest:function(ue){ue?ye(n.DEPTH_TEST):fe(n.DEPTH_TEST)},setMask:function(ue){H!==ue&&!R&&(n.depthMask(ue),H=ue)},setFunc:function(ue){if(ce&&(ue=n1[ue]),Z!==ue){switch(ue){case Oh:n.depthFunc(n.NEVER);break;case Lh:n.depthFunc(n.ALWAYS);break;case kh:n.depthFunc(n.LESS);break;case Os:n.depthFunc(n.LEQUAL);break;case Uh:n.depthFunc(n.EQUAL);break;case Vh:n.depthFunc(n.GEQUAL);break;case Bh:n.depthFunc(n.GREATER);break;case Hh:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}Z=ue}},setLocked:function(ue){R=ue},setClear:function(ue){ae!==ue&&(n.clearDepth(ue),ae=ue)},reset:function(){R=!1,H=null,Z=null,ae=null}}}function i(){let R=!1,ce=null,H=null,Z=null,ae=null,ue=null,qe=null,Rt=null,Qt=null;return{setTest:function(Qe){R||(Qe?ye(n.STENCIL_TEST):fe(n.STENCIL_TEST))},setMask:function(Qe){ce!==Qe&&!R&&(n.stencilMask(Qe),ce=Qe)},setFunc:function(Qe,en,Jn){(H!==Qe||Z!==en||ae!==Jn)&&(n.stencilFunc(Qe,en,Jn),H=Qe,Z=en,ae=Jn)},setOp:function(Qe,en,Jn){(ue!==Qe||qe!==en||Rt!==Jn)&&(n.stencilOp(Qe,en,Jn),ue=Qe,qe=en,Rt=Jn)},setLocked:function(Qe){R=Qe},setClear:function(Qe){Qt!==Qe&&(n.clearStencil(Qe),Qt=Qe)},reset:function(){R=!1,ce=null,H=null,Z=null,ae=null,ue=null,qe=null,Rt=null,Qt=null}}}let r=new e,s=new t,o=new i,a=new WeakMap,l=new WeakMap,c={},u={},d=new WeakMap,h=[],f=null,g=!1,_=null,m=null,p=null,w=null,E=null,S=null,k=null,I=new Ye(0,0,0),C=0,V=!1,re=null,v=null,M=null,j=null,z=null,Y=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),J=!1,W=0,K=n.getParameter(n.VERSION);K.indexOf("WebGL")!==-1?(W=parseFloat(/^WebGL (\d)/.exec(K)[1]),J=W>=1):K.indexOf("OpenGL ES")!==-1&&(W=parseFloat(/^OpenGL ES (\d)/.exec(K)[1]),J=W>=2);let G=null,de={},he=n.getParameter(n.SCISSOR_BOX),xe=n.getParameter(n.VIEWPORT),Je=new wt().fromArray(he),at=new wt().fromArray(xe);function $(R,ce,H,Z){let ae=new Uint8Array(4),ue=n.createTexture();n.bindTexture(R,ue),n.texParameteri(R,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(R,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let qe=0;qe<H;qe++)R===n.TEXTURE_3D||R===n.TEXTURE_2D_ARRAY?n.texImage3D(ce,0,n.RGBA,1,1,Z,0,n.RGBA,n.UNSIGNED_BYTE,ae):n.texImage2D(ce+qe,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,ae);return ue}let ee={};ee[n.TEXTURE_2D]=$(n.TEXTURE_2D,n.TEXTURE_2D,1),ee[n.TEXTURE_CUBE_MAP]=$(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),ee[n.TEXTURE_2D_ARRAY]=$(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),ee[n.TEXTURE_3D]=$(n.TEXTURE_3D,n.TEXTURE_3D,1,1),r.setClear(0,0,0,1),s.setClear(1),o.setClear(0),ye(n.DEPTH_TEST),s.setFunc(Os),ze(!1),Ze(My),ye(n.CULL_FACE),T(Gi);function ye(R){c[R]!==!0&&(n.enable(R),c[R]=!0)}function fe(R){c[R]!==!1&&(n.disable(R),c[R]=!1)}function Pe(R,ce){return u[R]!==ce?(n.bindFramebuffer(R,ce),u[R]=ce,R===n.DRAW_FRAMEBUFFER&&(u[n.FRAMEBUFFER]=ce),R===n.FRAMEBUFFER&&(u[n.DRAW_FRAMEBUFFER]=ce),!0):!1}function we(R,ce){let H=h,Z=!1;if(R){H=d.get(ce),H===void 0&&(H=[],d.set(ce,H));let ae=R.textures;if(H.length!==ae.length||H[0]!==n.COLOR_ATTACHMENT0){for(let ue=0,qe=ae.length;ue<qe;ue++)H[ue]=n.COLOR_ATTACHMENT0+ue;H.length=ae.length,Z=!0}}else H[0]!==n.BACK&&(H[0]=n.BACK,Z=!0);Z&&n.drawBuffers(H)}function je(R){return f!==R?(n.useProgram(R),f=R,!0):!1}let ct={[Mr]:n.FUNC_ADD,[Rw]:n.FUNC_SUBTRACT,[Nw]:n.FUNC_REVERSE_SUBTRACT};ct[Pw]=n.MIN,ct[Fw]=n.MAX;let $e={[Ow]:n.ZERO,[Lw]:n.ONE,[kw]:n.SRC_COLOR,[Ph]:n.SRC_ALPHA,[Gw]:n.SRC_ALPHA_SATURATE,[Hw]:n.DST_COLOR,[Vw]:n.DST_ALPHA,[Uw]:n.ONE_MINUS_SRC_COLOR,[Fh]:n.ONE_MINUS_SRC_ALPHA,[zw]:n.ONE_MINUS_DST_COLOR,[Bw]:n.ONE_MINUS_DST_ALPHA,[Ww]:n.CONSTANT_COLOR,[jw]:n.ONE_MINUS_CONSTANT_COLOR,[$w]:n.CONSTANT_ALPHA,[qw]:n.ONE_MINUS_CONSTANT_ALPHA};function T(R,ce,H,Z,ae,ue,qe,Rt,Qt,Qe){if(R===Gi){g===!0&&(fe(n.BLEND),g=!1);return}if(g===!1&&(ye(n.BLEND),g=!0),R!==Iw){if(R!==_||Qe!==V){if((m!==Mr||E!==Mr)&&(n.blendEquation(n.FUNC_ADD),m=Mr,E=Mr),Qe)switch(R){case Rs:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Ey:n.blendFunc(n.ONE,n.ONE);break;case by:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Sy:n.blendFuncSeparate(n.ZERO,n.SRC_COLOR,n.ZERO,n.SRC_ALPHA);break;default:console.error("THREE.WebGLState: Invalid blending: ",R);break}else switch(R){case Rs:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Ey:n.blendFunc(n.SRC_ALPHA,n.ONE);break;case by:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Sy:n.blendFunc(n.ZERO,n.SRC_COLOR);break;default:console.error("THREE.WebGLState: Invalid blending: ",R);break}p=null,w=null,S=null,k=null,I.set(0,0,0),C=0,_=R,V=Qe}return}ae=ae||ce,ue=ue||H,qe=qe||Z,(ce!==m||ae!==E)&&(n.blendEquationSeparate(ct[ce],ct[ae]),m=ce,E=ae),(H!==p||Z!==w||ue!==S||qe!==k)&&(n.blendFuncSeparate($e[H],$e[Z],$e[ue],$e[qe]),p=H,w=Z,S=ue,k=qe),(Rt.equals(I)===!1||Qt!==C)&&(n.blendColor(Rt.r,Rt.g,Rt.b,Qt),I.copy(Rt),C=Qt),_=R,V=!1}function cn(R,ce){R.side===gi?fe(n.CULL_FACE):ye(n.CULL_FACE);let H=R.side===ln;ce&&(H=!H),ze(H),R.blending===Rs&&R.transparent===!1?T(Gi):T(R.blending,R.blendEquation,R.blendSrc,R.blendDst,R.blendEquationAlpha,R.blendSrcAlpha,R.blendDstAlpha,R.blendColor,R.blendAlpha,R.premultipliedAlpha),s.setFunc(R.depthFunc),s.setTest(R.depthTest),s.setMask(R.depthWrite),r.setMask(R.colorWrite);let Z=R.stencilWrite;o.setTest(Z),Z&&(o.setMask(R.stencilWriteMask),o.setFunc(R.stencilFunc,R.stencilRef,R.stencilFuncMask),o.setOp(R.stencilFail,R.stencilZFail,R.stencilZPass)),mt(R.polygonOffset,R.polygonOffsetFactor,R.polygonOffsetUnits),R.alphaToCoverage===!0?ye(n.SAMPLE_ALPHA_TO_COVERAGE):fe(n.SAMPLE_ALPHA_TO_COVERAGE)}function ze(R){re!==R&&(R?n.frontFace(n.CW):n.frontFace(n.CCW),re=R)}function Ze(R){R!==Dw?(ye(n.CULL_FACE),R!==v&&(R===My?n.cullFace(n.BACK):R===Aw?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):fe(n.CULL_FACE),v=R}function Ae(R){R!==M&&(J&&n.lineWidth(R),M=R)}function mt(R,ce,H){R?(ye(n.POLYGON_OFFSET_FILL),(j!==ce||z!==H)&&(n.polygonOffset(ce,H),j=ce,z=H)):fe(n.POLYGON_OFFSET_FILL)}function Re(R){R?ye(n.SCISSOR_TEST):fe(n.SCISSOR_TEST)}function b(R){R===void 0&&(R=n.TEXTURE0+Y-1),G!==R&&(n.activeTexture(R),G=R)}function y(R,ce,H){H===void 0&&(G===null?H=n.TEXTURE0+Y-1:H=G);let Z=de[H];Z===void 0&&(Z={type:void 0,texture:void 0},de[H]=Z),(Z.type!==R||Z.texture!==ce)&&(G!==H&&(n.activeTexture(H),G=H),n.bindTexture(R,ce||ee[R]),Z.type=R,Z.texture=ce)}function O(){let R=de[G];R!==void 0&&R.type!==void 0&&(n.bindTexture(R.type,null),R.type=void 0,R.texture=void 0)}function X(){try{n.compressedTexImage2D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function Q(){try{n.compressedTexImage3D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function q(){try{n.texSubImage2D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function Me(){try{n.texSubImage3D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function oe(){try{n.compressedTexSubImage2D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function pe(){try{n.compressedTexSubImage3D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function Ke(){try{n.texStorage2D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function te(){try{n.texStorage3D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function me(){try{n.texImage2D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function Te(){try{n.texImage3D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function Ie(R){Je.equals(R)===!1&&(n.scissor(R.x,R.y,R.z,R.w),Je.copy(R))}function ge(R){at.equals(R)===!1&&(n.viewport(R.x,R.y,R.z,R.w),at.copy(R))}function Ge(R,ce){let H=l.get(ce);H===void 0&&(H=new WeakMap,l.set(ce,H));let Z=H.get(R);Z===void 0&&(Z=n.getUniformBlockIndex(ce,R.name),H.set(R,Z))}function Fe(R,ce){let Z=l.get(ce).get(R);a.get(ce)!==Z&&(n.uniformBlockBinding(ce,Z,R.__bindingPointIndex),a.set(ce,Z))}function ht(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),c={},G=null,de={},u={},d=new WeakMap,h=[],f=null,g=!1,_=null,m=null,p=null,w=null,E=null,S=null,k=null,I=new Ye(0,0,0),C=0,V=!1,re=null,v=null,M=null,j=null,z=null,Je.set(0,0,n.canvas.width,n.canvas.height),at.set(0,0,n.canvas.width,n.canvas.height),r.reset(),s.reset(),o.reset()}return{buffers:{color:r,depth:s,stencil:o},enable:ye,disable:fe,bindFramebuffer:Pe,drawBuffers:we,useProgram:je,setBlending:T,setMaterial:cn,setFlipSided:ze,setCullFace:Ze,setLineWidth:Ae,setPolygonOffset:mt,setScissorTest:Re,activeTexture:b,bindTexture:y,unbindTexture:O,compressedTexImage2D:X,compressedTexImage3D:Q,texImage2D:me,texImage3D:Te,updateUBOMapping:Ge,uniformBlockBinding:Fe,texStorage2D:Ke,texStorage3D:te,texSubImage2D:q,texSubImage3D:Me,compressedTexSubImage2D:oe,compressedTexSubImage3D:pe,scissor:Ie,viewport:ge,reset:ht}}function v0(n,e,t,i){let r=r1(i);switch(t){case w0:return n*e;case D0:return n*e;case A0:return n*e*2;case T0:return n*e/r.components*r.byteLength;case Jf:return n*e/r.components*r.byteLength;case I0:return n*e*2/r.components*r.byteLength;case Qf:return n*e*2/r.components*r.byteLength;case C0:return n*e*3/r.components*r.byteLength;case Un:return n*e*4/r.components*r.byteLength;case ep:return n*e*4/r.components*r.byteLength;case Bl:case Hl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case zl:case Gl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case qh:case Yh:return Math.max(n,16)*Math.max(e,8)/4;case $h:case Xh:return Math.max(n,8)*Math.max(e,8)/2;case Zh:case Kh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Jh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Qh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case ef:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case tf:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case nf:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case rf:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case sf:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case of:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case af:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case lf:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case cf:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case uf:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case df:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case hf:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case ff:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case Wl:case pf:case mf:return Math.ceil(n/4)*Math.ceil(e/4)*16;case R0:case gf:return Math.ceil(n/4)*Math.ceil(e/4)*8;case vf:case yf:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function r1(n){switch(n){case Mi:case E0:return{byteLength:1,components:1};case Co:case b0:case Io:return{byteLength:2,components:1};case Zf:case Kf:return{byteLength:2,components:4};case Cr:case Yf:case vi:return{byteLength:4,components:1};case S0:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}function s1(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,l=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),c=new ot,u=new WeakMap,d,h=new WeakMap,f=!1;try{f=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function g(b,y){return f?new OffscreenCanvas(b,y):Kl("canvas")}function _(b,y,O){let X=1,Q=Re(b);if((Q.width>O||Q.height>O)&&(X=O/Math.max(Q.width,Q.height)),X<1)if(typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&b instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&b instanceof ImageBitmap||typeof VideoFrame<"u"&&b instanceof VideoFrame){let q=Math.floor(X*Q.width),Me=Math.floor(X*Q.height);d===void 0&&(d=g(q,Me));let oe=y?g(q,Me):d;return oe.width=q,oe.height=Me,oe.getContext("2d").drawImage(b,0,0,q,Me),console.warn("THREE.WebGLRenderer: Texture has been resized from ("+Q.width+"x"+Q.height+") to ("+q+"x"+Me+")."),oe}else return"data"in b&&console.warn("THREE.WebGLRenderer: Image in DataTexture is too big ("+Q.width+"x"+Q.height+")."),b;return b}function m(b){return b.generateMipmaps&&b.minFilter!==wn&&b.minFilter!==kn}function p(b){n.generateMipmap(b)}function w(b,y,O,X,Q=!1){if(b!==null){if(n[b]!==void 0)return n[b];console.warn("THREE.WebGLRenderer: Attempt to use non-existing WebGL internal format '"+b+"'")}let q=y;if(y===n.RED&&(O===n.FLOAT&&(q=n.R32F),O===n.HALF_FLOAT&&(q=n.R16F),O===n.UNSIGNED_BYTE&&(q=n.R8)),y===n.RED_INTEGER&&(O===n.UNSIGNED_BYTE&&(q=n.R8UI),O===n.UNSIGNED_SHORT&&(q=n.R16UI),O===n.UNSIGNED_INT&&(q=n.R32UI),O===n.BYTE&&(q=n.R8I),O===n.SHORT&&(q=n.R16I),O===n.INT&&(q=n.R32I)),y===n.RG&&(O===n.FLOAT&&(q=n.RG32F),O===n.HALF_FLOAT&&(q=n.RG16F),O===n.UNSIGNED_BYTE&&(q=n.RG8)),y===n.RG_INTEGER&&(O===n.UNSIGNED_BYTE&&(q=n.RG8UI),O===n.UNSIGNED_SHORT&&(q=n.RG16UI),O===n.UNSIGNED_INT&&(q=n.RG32UI),O===n.BYTE&&(q=n.RG8I),O===n.SHORT&&(q=n.RG16I),O===n.INT&&(q=n.RG32I)),y===n.RGB_INTEGER&&(O===n.UNSIGNED_BYTE&&(q=n.RGB8UI),O===n.UNSIGNED_SHORT&&(q=n.RGB16UI),O===n.UNSIGNED_INT&&(q=n.RGB32UI),O===n.BYTE&&(q=n.RGB8I),O===n.SHORT&&(q=n.RGB16I),O===n.INT&&(q=n.RGB32I)),y===n.RGBA_INTEGER&&(O===n.UNSIGNED_BYTE&&(q=n.RGBA8UI),O===n.UNSIGNED_SHORT&&(q=n.RGBA16UI),O===n.UNSIGNED_INT&&(q=n.RGBA32UI),O===n.BYTE&&(q=n.RGBA8I),O===n.SHORT&&(q=n.RGBA16I),O===n.INT&&(q=n.RGBA32I)),y===n.RGB&&O===n.UNSIGNED_INT_5_9_9_9_REV&&(q=n.RGB9_E5),y===n.RGBA){let Me=Q?ql:st.getTransfer(X);O===n.FLOAT&&(q=n.RGBA32F),O===n.HALF_FLOAT&&(q=n.RGBA16F),O===n.UNSIGNED_BYTE&&(q=Me===xt?n.SRGB8_ALPHA8:n.RGBA8),O===n.UNSIGNED_SHORT_4_4_4_4&&(q=n.RGBA4),O===n.UNSIGNED_SHORT_5_5_5_1&&(q=n.RGB5_A1)}return(q===n.R16F||q===n.R32F||q===n.RG16F||q===n.RG32F||q===n.RGBA16F||q===n.RGBA32F)&&e.get("EXT_color_buffer_float"),q}function E(b,y){let O;return b?y===null||y===Cr||y===Us?O=n.DEPTH24_STENCIL8:y===vi?O=n.DEPTH32F_STENCIL8:y===Co&&(O=n.DEPTH24_STENCIL8,console.warn("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):y===null||y===Cr||y===Us?O=n.DEPTH_COMPONENT24:y===vi?O=n.DEPTH_COMPONENT32F:y===Co&&(O=n.DEPTH_COMPONENT16),O}function S(b,y){return m(b)===!0||b.isFramebufferTexture&&b.minFilter!==wn&&b.minFilter!==kn?Math.log2(Math.max(y.width,y.height))+1:b.mipmaps!==void 0&&b.mipmaps.length>0?b.mipmaps.length:b.isCompressedTexture&&Array.isArray(b.image)?y.mipmaps.length:1}function k(b){let y=b.target;y.removeEventListener("dispose",k),C(y),y.isVideoTexture&&u.delete(y)}function I(b){let y=b.target;y.removeEventListener("dispose",I),re(y)}function C(b){let y=i.get(b);if(y.__webglInit===void 0)return;let O=b.source,X=h.get(O);if(X){let Q=X[y.__cacheKey];Q.usedTimes--,Q.usedTimes===0&&V(b),Object.keys(X).length===0&&h.delete(O)}i.remove(b)}function V(b){let y=i.get(b);n.deleteTexture(y.__webglTexture);let O=b.source,X=h.get(O);delete X[y.__cacheKey],o.memory.textures--}function re(b){let y=i.get(b);if(b.depthTexture&&b.depthTexture.dispose(),b.isWebGLCubeRenderTarget)for(let X=0;X<6;X++){if(Array.isArray(y.__webglFramebuffer[X]))for(let Q=0;Q<y.__webglFramebuffer[X].length;Q++)n.deleteFramebuffer(y.__webglFramebuffer[X][Q]);else n.deleteFramebuffer(y.__webglFramebuffer[X]);y.__webglDepthbuffer&&n.deleteRenderbuffer(y.__webglDepthbuffer[X])}else{if(Array.isArray(y.__webglFramebuffer))for(let X=0;X<y.__webglFramebuffer.length;X++)n.deleteFramebuffer(y.__webglFramebuffer[X]);else n.deleteFramebuffer(y.__webglFramebuffer);if(y.__webglDepthbuffer&&n.deleteRenderbuffer(y.__webglDepthbuffer),y.__webglMultisampledFramebuffer&&n.deleteFramebuffer(y.__webglMultisampledFramebuffer),y.__webglColorRenderbuffer)for(let X=0;X<y.__webglColorRenderbuffer.length;X++)y.__webglColorRenderbuffer[X]&&n.deleteRenderbuffer(y.__webglColorRenderbuffer[X]);y.__webglDepthRenderbuffer&&n.deleteRenderbuffer(y.__webglDepthRenderbuffer)}let O=b.textures;for(let X=0,Q=O.length;X<Q;X++){let q=i.get(O[X]);q.__webglTexture&&(n.deleteTexture(q.__webglTexture),o.memory.textures--),i.remove(O[X])}i.remove(b)}let v=0;function M(){v=0}function j(){let b=v;return b>=r.maxTextures&&console.warn("THREE.WebGLTextures: Trying to use "+b+" texture units while this GPU supports only "+r.maxTextures),v+=1,b}function z(b){let y=[];return y.push(b.wrapS),y.push(b.wrapT),y.push(b.wrapR||0),y.push(b.magFilter),y.push(b.minFilter),y.push(b.anisotropy),y.push(b.internalFormat),y.push(b.format),y.push(b.type),y.push(b.generateMipmaps),y.push(b.premultiplyAlpha),y.push(b.flipY),y.push(b.unpackAlignment),y.push(b.colorSpace),y.join()}function Y(b,y){let O=i.get(b);if(b.isVideoTexture&&Ae(b),b.isRenderTargetTexture===!1&&b.version>0&&O.__version!==b.version){let X=b.image;if(X===null)console.warn("THREE.WebGLRenderer: Texture marked for update but no image data found.");else if(X.complete===!1)console.warn("THREE.WebGLRenderer: Texture marked for update but image is incomplete");else{at(O,b,y);return}}t.bindTexture(n.TEXTURE_2D,O.__webglTexture,n.TEXTURE0+y)}function J(b,y){let O=i.get(b);if(b.version>0&&O.__version!==b.version){at(O,b,y);return}t.bindTexture(n.TEXTURE_2D_ARRAY,O.__webglTexture,n.TEXTURE0+y)}function W(b,y){let O=i.get(b);if(b.version>0&&O.__version!==b.version){at(O,b,y);return}t.bindTexture(n.TEXTURE_3D,O.__webglTexture,n.TEXTURE0+y)}function K(b,y){let O=i.get(b);if(b.version>0&&O.__version!==b.version){$(O,b,y);return}t.bindTexture(n.TEXTURE_CUBE_MAP,O.__webglTexture,n.TEXTURE0+y)}let G={[Wh]:n.REPEAT,[Sr]:n.CLAMP_TO_EDGE,[jh]:n.MIRRORED_REPEAT},de={[wn]:n.NEAREST,[iC]:n.NEAREST_MIPMAP_NEAREST,[yl]:n.NEAREST_MIPMAP_LINEAR,[kn]:n.LINEAR,[th]:n.LINEAR_MIPMAP_NEAREST,[wr]:n.LINEAR_MIPMAP_LINEAR},he={[aC]:n.NEVER,[fC]:n.ALWAYS,[lC]:n.LESS,[P0]:n.LEQUAL,[cC]:n.EQUAL,[hC]:n.GEQUAL,[uC]:n.GREATER,[dC]:n.NOTEQUAL};function xe(b,y){if(y.type===vi&&e.has("OES_texture_float_linear")===!1&&(y.magFilter===kn||y.magFilter===th||y.magFilter===yl||y.magFilter===wr||y.minFilter===kn||y.minFilter===th||y.minFilter===yl||y.minFilter===wr)&&console.warn("THREE.WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(b,n.TEXTURE_WRAP_S,G[y.wrapS]),n.texParameteri(b,n.TEXTURE_WRAP_T,G[y.wrapT]),(b===n.TEXTURE_3D||b===n.TEXTURE_2D_ARRAY)&&n.texParameteri(b,n.TEXTURE_WRAP_R,G[y.wrapR]),n.texParameteri(b,n.TEXTURE_MAG_FILTER,de[y.magFilter]),n.texParameteri(b,n.TEXTURE_MIN_FILTER,de[y.minFilter]),y.compareFunction&&(n.texParameteri(b,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(b,n.TEXTURE_COMPARE_FUNC,he[y.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(y.magFilter===wn||y.minFilter!==yl&&y.minFilter!==wr||y.type===vi&&e.has("OES_texture_float_linear")===!1)return;if(y.anisotropy>1||i.get(y).__currentAnisotropy){let O=e.get("EXT_texture_filter_anisotropic");n.texParameterf(b,O.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(y.anisotropy,r.getMaxAnisotropy())),i.get(y).__currentAnisotropy=y.anisotropy}}}function Je(b,y){let O=!1;b.__webglInit===void 0&&(b.__webglInit=!0,y.addEventListener("dispose",k));let X=y.source,Q=h.get(X);Q===void 0&&(Q={},h.set(X,Q));let q=z(y);if(q!==b.__cacheKey){Q[q]===void 0&&(Q[q]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,O=!0),Q[q].usedTimes++;let Me=Q[b.__cacheKey];Me!==void 0&&(Q[b.__cacheKey].usedTimes--,Me.usedTimes===0&&V(y)),b.__cacheKey=q,b.__webglTexture=Q[q].texture}return O}function at(b,y,O){let X=n.TEXTURE_2D;(y.isDataArrayTexture||y.isCompressedArrayTexture)&&(X=n.TEXTURE_2D_ARRAY),y.isData3DTexture&&(X=n.TEXTURE_3D);let Q=Je(b,y),q=y.source;t.bindTexture(X,b.__webglTexture,n.TEXTURE0+O);let Me=i.get(q);if(q.version!==Me.__version||Q===!0){t.activeTexture(n.TEXTURE0+O);let oe=st.getPrimaries(st.workingColorSpace),pe=y.colorSpace===zi?null:st.getPrimaries(y.colorSpace),Ke=y.colorSpace===zi||oe===pe?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,y.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,y.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,y.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,Ke);let te=_(y.image,!1,r.maxTextureSize);te=mt(y,te);let me=s.convert(y.format,y.colorSpace),Te=s.convert(y.type),Ie=w(y.internalFormat,me,Te,y.colorSpace,y.isVideoTexture);xe(X,y);let ge,Ge=y.mipmaps,Fe=y.isVideoTexture!==!0,ht=Me.__version===void 0||Q===!0,R=q.dataReady,ce=S(y,te);if(y.isDepthTexture)Ie=E(y.format===Vs,y.type),ht&&(Fe?t.texStorage2D(n.TEXTURE_2D,1,Ie,te.width,te.height):t.texImage2D(n.TEXTURE_2D,0,Ie,te.width,te.height,0,me,Te,null));else if(y.isDataTexture)if(Ge.length>0){Fe&&ht&&t.texStorage2D(n.TEXTURE_2D,ce,Ie,Ge[0].width,Ge[0].height);for(let H=0,Z=Ge.length;H<Z;H++)ge=Ge[H],Fe?R&&t.texSubImage2D(n.TEXTURE_2D,H,0,0,ge.width,ge.height,me,Te,ge.data):t.texImage2D(n.TEXTURE_2D,H,Ie,ge.width,ge.height,0,me,Te,ge.data);y.generateMipmaps=!1}else Fe?(ht&&t.texStorage2D(n.TEXTURE_2D,ce,Ie,te.width,te.height),R&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,te.width,te.height,me,Te,te.data)):t.texImage2D(n.TEXTURE_2D,0,Ie,te.width,te.height,0,me,Te,te.data);else if(y.isCompressedTexture)if(y.isCompressedArrayTexture){Fe&&ht&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ce,Ie,Ge[0].width,Ge[0].height,te.depth);for(let H=0,Z=Ge.length;H<Z;H++)if(ge=Ge[H],y.format!==Un)if(me!==null)if(Fe){if(R)if(y.layerUpdates.size>0){let ae=v0(ge.width,ge.height,y.format,y.type);for(let ue of y.layerUpdates){let qe=ge.data.subarray(ue*ae/ge.data.BYTES_PER_ELEMENT,(ue+1)*ae/ge.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,H,0,0,ue,ge.width,ge.height,1,me,qe,0,0)}y.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,H,0,0,0,ge.width,ge.height,te.depth,me,ge.data,0,0)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,H,Ie,ge.width,ge.height,te.depth,0,ge.data,0,0);else console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else Fe?R&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,H,0,0,0,ge.width,ge.height,te.depth,me,Te,ge.data):t.texImage3D(n.TEXTURE_2D_ARRAY,H,Ie,ge.width,ge.height,te.depth,0,me,Te,ge.data)}else{Fe&&ht&&t.texStorage2D(n.TEXTURE_2D,ce,Ie,Ge[0].width,Ge[0].height);for(let H=0,Z=Ge.length;H<Z;H++)ge=Ge[H],y.format!==Un?me!==null?Fe?R&&t.compressedTexSubImage2D(n.TEXTURE_2D,H,0,0,ge.width,ge.height,me,ge.data):t.compressedTexImage2D(n.TEXTURE_2D,H,Ie,ge.width,ge.height,0,ge.data):console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):Fe?R&&t.texSubImage2D(n.TEXTURE_2D,H,0,0,ge.width,ge.height,me,Te,ge.data):t.texImage2D(n.TEXTURE_2D,H,Ie,ge.width,ge.height,0,me,Te,ge.data)}else if(y.isDataArrayTexture)if(Fe){if(ht&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ce,Ie,te.width,te.height,te.depth),R)if(y.layerUpdates.size>0){let H=v0(te.width,te.height,y.format,y.type);for(let Z of y.layerUpdates){let ae=te.data.subarray(Z*H/te.data.BYTES_PER_ELEMENT,(Z+1)*H/te.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,Z,te.width,te.height,1,me,Te,ae)}y.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,te.width,te.height,te.depth,me,Te,te.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,Ie,te.width,te.height,te.depth,0,me,Te,te.data);else if(y.isData3DTexture)Fe?(ht&&t.texStorage3D(n.TEXTURE_3D,ce,Ie,te.width,te.height,te.depth),R&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,te.width,te.height,te.depth,me,Te,te.data)):t.texImage3D(n.TEXTURE_3D,0,Ie,te.width,te.height,te.depth,0,me,Te,te.data);else if(y.isFramebufferTexture){if(ht)if(Fe)t.texStorage2D(n.TEXTURE_2D,ce,Ie,te.width,te.height);else{let H=te.width,Z=te.height;for(let ae=0;ae<ce;ae++)t.texImage2D(n.TEXTURE_2D,ae,Ie,H,Z,0,me,Te,null),H>>=1,Z>>=1}}else if(Ge.length>0){if(Fe&&ht){let H=Re(Ge[0]);t.texStorage2D(n.TEXTURE_2D,ce,Ie,H.width,H.height)}for(let H=0,Z=Ge.length;H<Z;H++)ge=Ge[H],Fe?R&&t.texSubImage2D(n.TEXTURE_2D,H,0,0,me,Te,ge):t.texImage2D(n.TEXTURE_2D,H,Ie,me,Te,ge);y.generateMipmaps=!1}else if(Fe){if(ht){let H=Re(te);t.texStorage2D(n.TEXTURE_2D,ce,Ie,H.width,H.height)}R&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,me,Te,te)}else t.texImage2D(n.TEXTURE_2D,0,Ie,me,Te,te);m(y)&&p(X),Me.__version=q.version,y.onUpdate&&y.onUpdate(y)}b.__version=y.version}function $(b,y,O){if(y.image.length!==6)return;let X=Je(b,y),Q=y.source;t.bindTexture(n.TEXTURE_CUBE_MAP,b.__webglTexture,n.TEXTURE0+O);let q=i.get(Q);if(Q.version!==q.__version||X===!0){t.activeTexture(n.TEXTURE0+O);let Me=st.getPrimaries(st.workingColorSpace),oe=y.colorSpace===zi?null:st.getPrimaries(y.colorSpace),pe=y.colorSpace===zi||Me===oe?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,y.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,y.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,y.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,pe);let Ke=y.isCompressedTexture||y.image[0].isCompressedTexture,te=y.image[0]&&y.image[0].isDataTexture,me=[];for(let Z=0;Z<6;Z++)!Ke&&!te?me[Z]=_(y.image[Z],!0,r.maxCubemapSize):me[Z]=te?y.image[Z].image:y.image[Z],me[Z]=mt(y,me[Z]);let Te=me[0],Ie=s.convert(y.format,y.colorSpace),ge=s.convert(y.type),Ge=w(y.internalFormat,Ie,ge,y.colorSpace),Fe=y.isVideoTexture!==!0,ht=q.__version===void 0||X===!0,R=Q.dataReady,ce=S(y,Te);xe(n.TEXTURE_CUBE_MAP,y);let H;if(Ke){Fe&&ht&&t.texStorage2D(n.TEXTURE_CUBE_MAP,ce,Ge,Te.width,Te.height);for(let Z=0;Z<6;Z++){H=me[Z].mipmaps;for(let ae=0;ae<H.length;ae++){let ue=H[ae];y.format!==Un?Ie!==null?Fe?R&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae,0,0,ue.width,ue.height,Ie,ue.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae,Ge,ue.width,ue.height,0,ue.data):console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):Fe?R&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae,0,0,ue.width,ue.height,Ie,ge,ue.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae,Ge,ue.width,ue.height,0,Ie,ge,ue.data)}}}else{if(H=y.mipmaps,Fe&&ht){H.length>0&&ce++;let Z=Re(me[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,ce,Ge,Z.width,Z.height)}for(let Z=0;Z<6;Z++)if(te){Fe?R&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,me[Z].width,me[Z].height,Ie,ge,me[Z].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,Ge,me[Z].width,me[Z].height,0,Ie,ge,me[Z].data);for(let ae=0;ae<H.length;ae++){let qe=H[ae].image[Z].image;Fe?R&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae+1,0,0,qe.width,qe.height,Ie,ge,qe.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae+1,Ge,qe.width,qe.height,0,Ie,ge,qe.data)}}else{Fe?R&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,Ie,ge,me[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,Ge,Ie,ge,me[Z]);for(let ae=0;ae<H.length;ae++){let ue=H[ae];Fe?R&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae+1,0,0,Ie,ge,ue.image[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae+1,Ge,Ie,ge,ue.image[Z])}}}m(y)&&p(n.TEXTURE_CUBE_MAP),q.__version=Q.version,y.onUpdate&&y.onUpdate(y)}b.__version=y.version}function ee(b,y,O,X,Q,q){let Me=s.convert(O.format,O.colorSpace),oe=s.convert(O.type),pe=w(O.internalFormat,Me,oe,O.colorSpace);if(!i.get(y).__hasExternalTextures){let te=Math.max(1,y.width>>q),me=Math.max(1,y.height>>q);Q===n.TEXTURE_3D||Q===n.TEXTURE_2D_ARRAY?t.texImage3D(Q,q,pe,te,me,y.depth,0,Me,oe,null):t.texImage2D(Q,q,pe,te,me,0,Me,oe,null)}t.bindFramebuffer(n.FRAMEBUFFER,b),Ze(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,X,Q,i.get(O).__webglTexture,0,ze(y)):(Q===n.TEXTURE_2D||Q>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&Q<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,X,Q,i.get(O).__webglTexture,q),t.bindFramebuffer(n.FRAMEBUFFER,null)}function ye(b,y,O){if(n.bindRenderbuffer(n.RENDERBUFFER,b),y.depthBuffer){let X=y.depthTexture,Q=X&&X.isDepthTexture?X.type:null,q=E(y.stencilBuffer,Q),Me=y.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,oe=ze(y);Ze(y)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,oe,q,y.width,y.height):O?n.renderbufferStorageMultisample(n.RENDERBUFFER,oe,q,y.width,y.height):n.renderbufferStorage(n.RENDERBUFFER,q,y.width,y.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,Me,n.RENDERBUFFER,b)}else{let X=y.textures;for(let Q=0;Q<X.length;Q++){let q=X[Q],Me=s.convert(q.format,q.colorSpace),oe=s.convert(q.type),pe=w(q.internalFormat,Me,oe,q.colorSpace),Ke=ze(y);O&&Ze(y)===!1?n.renderbufferStorageMultisample(n.RENDERBUFFER,Ke,pe,y.width,y.height):Ze(y)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,Ke,pe,y.width,y.height):n.renderbufferStorage(n.RENDERBUFFER,pe,y.width,y.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function fe(b,y){if(y&&y.isWebGLCubeRenderTarget)throw new Error("Depth Texture with cube render targets is not supported");if(t.bindFramebuffer(n.FRAMEBUFFER,b),!(y.depthTexture&&y.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");(!i.get(y.depthTexture).__webglTexture||y.depthTexture.image.width!==y.width||y.depthTexture.image.height!==y.height)&&(y.depthTexture.image.width=y.width,y.depthTexture.image.height=y.height,y.depthTexture.needsUpdate=!0),Y(y.depthTexture,0);let X=i.get(y.depthTexture).__webglTexture,Q=ze(y);if(y.depthTexture.format===Ns)Ze(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,n.DEPTH_ATTACHMENT,n.TEXTURE_2D,X,0,Q):n.framebufferTexture2D(n.FRAMEBUFFER,n.DEPTH_ATTACHMENT,n.TEXTURE_2D,X,0);else if(y.depthTexture.format===Vs)Ze(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,n.DEPTH_STENCIL_ATTACHMENT,n.TEXTURE_2D,X,0,Q):n.framebufferTexture2D(n.FRAMEBUFFER,n.DEPTH_STENCIL_ATTACHMENT,n.TEXTURE_2D,X,0);else throw new Error("Unknown depthTexture format")}function Pe(b){let y=i.get(b),O=b.isWebGLCubeRenderTarget===!0;if(y.__boundDepthTexture!==b.depthTexture){let X=b.depthTexture;if(y.__depthDisposeCallback&&y.__depthDisposeCallback(),X){let Q=()=>{delete y.__boundDepthTexture,delete y.__depthDisposeCallback,X.removeEventListener("dispose",Q)};X.addEventListener("dispose",Q),y.__depthDisposeCallback=Q}y.__boundDepthTexture=X}if(b.depthTexture&&!y.__autoAllocateDepthBuffer){if(O)throw new Error("target.depthTexture not supported in Cube render targets");fe(y.__webglFramebuffer,b)}else if(O){y.__webglDepthbuffer=[];for(let X=0;X<6;X++)if(t.bindFramebuffer(n.FRAMEBUFFER,y.__webglFramebuffer[X]),y.__webglDepthbuffer[X]===void 0)y.__webglDepthbuffer[X]=n.createRenderbuffer(),ye(y.__webglDepthbuffer[X],b,!1);else{let Q=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,q=y.__webglDepthbuffer[X];n.bindRenderbuffer(n.RENDERBUFFER,q),n.framebufferRenderbuffer(n.FRAMEBUFFER,Q,n.RENDERBUFFER,q)}}else if(t.bindFramebuffer(n.FRAMEBUFFER,y.__webglFramebuffer),y.__webglDepthbuffer===void 0)y.__webglDepthbuffer=n.createRenderbuffer(),ye(y.__webglDepthbuffer,b,!1);else{let X=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,Q=y.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,Q),n.framebufferRenderbuffer(n.FRAMEBUFFER,X,n.RENDERBUFFER,Q)}t.bindFramebuffer(n.FRAMEBUFFER,null)}function we(b,y,O){let X=i.get(b);y!==void 0&&ee(X.__webglFramebuffer,b,b.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),O!==void 0&&Pe(b)}function je(b){let y=b.texture,O=i.get(b),X=i.get(y);b.addEventListener("dispose",I);let Q=b.textures,q=b.isWebGLCubeRenderTarget===!0,Me=Q.length>1;if(Me||(X.__webglTexture===void 0&&(X.__webglTexture=n.createTexture()),X.__version=y.version,o.memory.textures++),q){O.__webglFramebuffer=[];for(let oe=0;oe<6;oe++)if(y.mipmaps&&y.mipmaps.length>0){O.__webglFramebuffer[oe]=[];for(let pe=0;pe<y.mipmaps.length;pe++)O.__webglFramebuffer[oe][pe]=n.createFramebuffer()}else O.__webglFramebuffer[oe]=n.createFramebuffer()}else{if(y.mipmaps&&y.mipmaps.length>0){O.__webglFramebuffer=[];for(let oe=0;oe<y.mipmaps.length;oe++)O.__webglFramebuffer[oe]=n.createFramebuffer()}else O.__webglFramebuffer=n.createFramebuffer();if(Me)for(let oe=0,pe=Q.length;oe<pe;oe++){let Ke=i.get(Q[oe]);Ke.__webglTexture===void 0&&(Ke.__webglTexture=n.createTexture(),o.memory.textures++)}if(b.samples>0&&Ze(b)===!1){O.__webglMultisampledFramebuffer=n.createFramebuffer(),O.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,O.__webglMultisampledFramebuffer);for(let oe=0;oe<Q.length;oe++){let pe=Q[oe];O.__webglColorRenderbuffer[oe]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,O.__webglColorRenderbuffer[oe]);let Ke=s.convert(pe.format,pe.colorSpace),te=s.convert(pe.type),me=w(pe.internalFormat,Ke,te,pe.colorSpace,b.isXRRenderTarget===!0),Te=ze(b);n.renderbufferStorageMultisample(n.RENDERBUFFER,Te,me,b.width,b.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+oe,n.RENDERBUFFER,O.__webglColorRenderbuffer[oe])}n.bindRenderbuffer(n.RENDERBUFFER,null),b.depthBuffer&&(O.__webglDepthRenderbuffer=n.createRenderbuffer(),ye(O.__webglDepthRenderbuffer,b,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(q){t.bindTexture(n.TEXTURE_CUBE_MAP,X.__webglTexture),xe(n.TEXTURE_CUBE_MAP,y);for(let oe=0;oe<6;oe++)if(y.mipmaps&&y.mipmaps.length>0)for(let pe=0;pe<y.mipmaps.length;pe++)ee(O.__webglFramebuffer[oe][pe],b,y,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+oe,pe);else ee(O.__webglFramebuffer[oe],b,y,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+oe,0);m(y)&&p(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(Me){for(let oe=0,pe=Q.length;oe<pe;oe++){let Ke=Q[oe],te=i.get(Ke);t.bindTexture(n.TEXTURE_2D,te.__webglTexture),xe(n.TEXTURE_2D,Ke),ee(O.__webglFramebuffer,b,Ke,n.COLOR_ATTACHMENT0+oe,n.TEXTURE_2D,0),m(Ke)&&p(n.TEXTURE_2D)}t.unbindTexture()}else{let oe=n.TEXTURE_2D;if((b.isWebGL3DRenderTarget||b.isWebGLArrayRenderTarget)&&(oe=b.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(oe,X.__webglTexture),xe(oe,y),y.mipmaps&&y.mipmaps.length>0)for(let pe=0;pe<y.mipmaps.length;pe++)ee(O.__webglFramebuffer[pe],b,y,n.COLOR_ATTACHMENT0,oe,pe);else ee(O.__webglFramebuffer,b,y,n.COLOR_ATTACHMENT0,oe,0);m(y)&&p(oe),t.unbindTexture()}b.depthBuffer&&Pe(b)}function ct(b){let y=b.textures;for(let O=0,X=y.length;O<X;O++){let Q=y[O];if(m(Q)){let q=b.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:n.TEXTURE_2D,Me=i.get(Q).__webglTexture;t.bindTexture(q,Me),p(q),t.unbindTexture()}}}let $e=[],T=[];function cn(b){if(b.samples>0){if(Ze(b)===!1){let y=b.textures,O=b.width,X=b.height,Q=n.COLOR_BUFFER_BIT,q=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,Me=i.get(b),oe=y.length>1;if(oe)for(let pe=0;pe<y.length;pe++)t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+pe,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+pe,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,Me.__webglMultisampledFramebuffer),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,Me.__webglFramebuffer);for(let pe=0;pe<y.length;pe++){if(b.resolveDepthBuffer&&(b.depthBuffer&&(Q|=n.DEPTH_BUFFER_BIT),b.stencilBuffer&&b.resolveStencilBuffer&&(Q|=n.STENCIL_BUFFER_BIT)),oe){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,Me.__webglColorRenderbuffer[pe]);let Ke=i.get(y[pe]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,Ke,0)}n.blitFramebuffer(0,0,O,X,0,0,O,X,Q,n.NEAREST),l===!0&&($e.length=0,T.length=0,$e.push(n.COLOR_ATTACHMENT0+pe),b.depthBuffer&&b.resolveDepthBuffer===!1&&($e.push(q),T.push(q),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,T)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,$e))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),oe)for(let pe=0;pe<y.length;pe++){t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+pe,n.RENDERBUFFER,Me.__webglColorRenderbuffer[pe]);let Ke=i.get(y[pe]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+pe,n.TEXTURE_2D,Ke,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,Me.__webglMultisampledFramebuffer)}else if(b.depthBuffer&&b.resolveDepthBuffer===!1&&l){let y=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[y])}}}function ze(b){return Math.min(r.maxSamples,b.samples)}function Ze(b){let y=i.get(b);return b.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&y.__useRenderToTexture!==!1}function Ae(b){let y=o.render.frame;u.get(b)!==y&&(u.set(b,y),b.update())}function mt(b,y){let O=b.colorSpace,X=b.format,Q=b.type;return b.isCompressedTexture===!0||b.isVideoTexture===!0||O!==Yi&&O!==zi&&(st.getTransfer(O)===xt?(X!==Un||Q!==Mi)&&console.warn("THREE.WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):console.error("THREE.WebGLTextures: Unsupported texture color space:",O)),y}function Re(b){return typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement?(c.width=b.naturalWidth||b.width,c.height=b.naturalHeight||b.height):typeof VideoFrame<"u"&&b instanceof VideoFrame?(c.width=b.displayWidth,c.height=b.displayHeight):(c.width=b.width,c.height=b.height),c}this.allocateTextureUnit=j,this.resetTextureUnits=M,this.setTexture2D=Y,this.setTexture2DArray=J,this.setTexture3D=W,this.setTextureCube=K,this.rebindTextures=we,this.setupRenderTarget=je,this.updateRenderTargetMipmap=ct,this.updateMultisampleRenderTarget=cn,this.setupDepthRenderbuffer=Pe,this.setupFrameBufferTexture=ee,this.useMultisampledRTT=Ze}function o1(n,e){function t(i,r=zi){let s,o=st.getTransfer(r);if(i===Mi)return n.UNSIGNED_BYTE;if(i===Zf)return n.UNSIGNED_SHORT_4_4_4_4;if(i===Kf)return n.UNSIGNED_SHORT_5_5_5_1;if(i===S0)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===E0)return n.BYTE;if(i===b0)return n.SHORT;if(i===Co)return n.UNSIGNED_SHORT;if(i===Yf)return n.INT;if(i===Cr)return n.UNSIGNED_INT;if(i===vi)return n.FLOAT;if(i===Io)return n.HALF_FLOAT;if(i===w0)return n.ALPHA;if(i===C0)return n.RGB;if(i===Un)return n.RGBA;if(i===D0)return n.LUMINANCE;if(i===A0)return n.LUMINANCE_ALPHA;if(i===Ns)return n.DEPTH_COMPONENT;if(i===Vs)return n.DEPTH_STENCIL;if(i===T0)return n.RED;if(i===Jf)return n.RED_INTEGER;if(i===I0)return n.RG;if(i===Qf)return n.RG_INTEGER;if(i===ep)return n.RGBA_INTEGER;if(i===Bl||i===Hl||i===zl||i===Gl)if(o===xt)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===Bl)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===Hl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===zl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===Gl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===Bl)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===Hl)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===zl)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===Gl)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===$h||i===qh||i===Xh||i===Yh)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===$h)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===qh)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===Xh)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===Yh)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===Zh||i===Kh||i===Jh)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===Zh||i===Kh)return o===xt?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===Jh)return o===xt?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC}else return null;if(i===Qh||i===ef||i===tf||i===nf||i===rf||i===sf||i===of||i===af||i===lf||i===cf||i===uf||i===df||i===hf||i===ff)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===Qh)return o===xt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===ef)return o===xt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===tf)return o===xt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===nf)return o===xt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===rf)return o===xt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===sf)return o===xt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===of)return o===xt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===af)return o===xt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===lf)return o===xt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===cf)return o===xt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===uf)return o===xt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===df)return o===xt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===hf)return o===xt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===ff)return o===xt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===Wl||i===pf||i===mf)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===Wl)return o===xt?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===pf)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===mf)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===R0||i===gf||i===vf||i===yf)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===Wl)return s.COMPRESSED_RED_RGTC1_EXT;if(i===gf)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===vf)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===yf)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===Us?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var Of=class extends Jt{constructor(e=[]){super(),this.isArrayCamera=!0,this.cameras=e}},_i=class extends bi{constructor(){super(),this.isGroup=!0,this.type="Group"}},a1={type:"move"},wo=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new _i,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new _i,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new B,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new B),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new _i,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new B,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new B),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,l=this._grip,c=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(c&&e.hand){o=!0;for(let _ of e.hand.values()){let m=t.getJointPose(_,i),p=this._getHandJoint(c,_);m!==null&&(p.matrix.fromArray(m.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=m.radius),p.visible=m!==null}let u=c.joints["index-finger-tip"],d=c.joints["thumb-tip"],h=u.position.distanceTo(d.position),f=.02,g=.005;c.inputState.pinching&&h>f+g?(c.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!c.inputState.pinching&&h<=f-g&&(c.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else l!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(l.matrix.fromArray(s.transform.matrix),l.matrix.decompose(l.position,l.rotation,l.scale),l.matrixWorldNeedsUpdate=!0,s.linearVelocity?(l.hasLinearVelocity=!0,l.linearVelocity.copy(s.linearVelocity)):l.hasLinearVelocity=!1,s.angularVelocity?(l.hasAngularVelocity=!0,l.angularVelocity.copy(s.angularVelocity)):l.hasAngularVelocity=!1));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(a1)))}return a!==null&&(a.visible=r!==null),l!==null&&(l.visible=s!==null),c!==null&&(c.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new _i;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},l1=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,c1=`
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

}`,Lf=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t,i){if(this.texture===null){let r=new Pr,s=e.properties.get(r);s.__webglTexture=t.texture,(t.depthNear!=i.depthNear||t.depthFar!=i.depthFar)&&(this.depthNear=t.depthNear,this.depthFar=t.depthFar),this.texture=r}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new Kn({vertexShader:l1,fragmentShader:c1,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new an(new oc(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},kf=class extends $i{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",l=1,c=null,u=null,d=null,h=null,f=null,g=null,_=new Lf,m=t.getContextAttributes(),p=null,w=null,E=[],S=[],k=new ot,I=null,C=new Jt;C.layers.enable(1),C.viewport=new wt;let V=new Jt;V.layers.enable(2),V.viewport=new wt;let re=[C,V],v=new Of;v.layers.enable(1),v.layers.enable(2);let M=null,j=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function($){let ee=E[$];return ee===void 0&&(ee=new wo,E[$]=ee),ee.getTargetRaySpace()},this.getControllerGrip=function($){let ee=E[$];return ee===void 0&&(ee=new wo,E[$]=ee),ee.getGripSpace()},this.getHand=function($){let ee=E[$];return ee===void 0&&(ee=new wo,E[$]=ee),ee.getHandSpace()};function z($){let ee=S.indexOf($.inputSource);if(ee===-1)return;let ye=E[ee];ye!==void 0&&(ye.update($.inputSource,$.frame,c||o),ye.dispatchEvent({type:$.type,data:$.inputSource}))}function Y(){r.removeEventListener("select",z),r.removeEventListener("selectstart",z),r.removeEventListener("selectend",z),r.removeEventListener("squeeze",z),r.removeEventListener("squeezestart",z),r.removeEventListener("squeezeend",z),r.removeEventListener("end",Y),r.removeEventListener("inputsourceschange",J);for(let $=0;$<E.length;$++){let ee=S[$];ee!==null&&(S[$]=null,E[$].disconnect(ee))}M=null,j=null,_.reset(),e.setRenderTarget(p),f=null,h=null,d=null,r=null,w=null,at.stop(),i.isPresenting=!1,e.setPixelRatio(I),e.setSize(k.width,k.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function($){s=$,i.isPresenting===!0&&console.warn("THREE.WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function($){a=$,i.isPresenting===!0&&console.warn("THREE.WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return c||o},this.setReferenceSpace=function($){c=$},this.getBaseLayer=function(){return h!==null?h:f},this.getBinding=function(){return d},this.getFrame=function(){return g},this.getSession=function(){return r},this.setSession=function($){return Ki(this,null,function*(){if(r=$,r!==null){if(p=e.getRenderTarget(),r.addEventListener("select",z),r.addEventListener("selectstart",z),r.addEventListener("selectend",z),r.addEventListener("squeeze",z),r.addEventListener("squeezestart",z),r.addEventListener("squeezeend",z),r.addEventListener("end",Y),r.addEventListener("inputsourceschange",J),m.xrCompatible!==!0&&(yield t.makeXRCompatible()),I=e.getPixelRatio(),e.getSize(k),r.renderState.layers===void 0){let ee={antialias:m.antialias,alpha:!0,depth:m.depth,stencil:m.stencil,framebufferScaleFactor:s};f=new XRWebGLLayer(r,t,ee),r.updateRenderState({baseLayer:f}),e.setPixelRatio(1),e.setSize(f.framebufferWidth,f.framebufferHeight,!1),w=new Ei(f.framebufferWidth,f.framebufferHeight,{format:Un,type:Mi,colorSpace:e.outputColorSpace,stencilBuffer:m.stencil})}else{let ee=null,ye=null,fe=null;m.depth&&(fe=m.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,ee=m.stencil?Vs:Ns,ye=m.stencil?Us:Cr);let Pe={colorFormat:t.RGBA8,depthFormat:fe,scaleFactor:s};d=new XRWebGLBinding(r,t),h=d.createProjectionLayer(Pe),r.updateRenderState({layers:[h]}),e.setPixelRatio(1),e.setSize(h.textureWidth,h.textureHeight,!1),w=new Ei(h.textureWidth,h.textureHeight,{format:Un,type:Mi,depthTexture:new cc(h.textureWidth,h.textureHeight,ye,void 0,void 0,void 0,void 0,void 0,void 0,ee),stencilBuffer:m.stencil,colorSpace:e.outputColorSpace,samples:m.antialias?4:0,resolveDepthBuffer:h.ignoreDepthValues===!1})}w.isXRRenderTarget=!0,this.setFoveation(l),c=null,o=yield r.requestReferenceSpace(a),at.setContext(r),at.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}})},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return _.getDepthTexture()};function J($){for(let ee=0;ee<$.removed.length;ee++){let ye=$.removed[ee],fe=S.indexOf(ye);fe>=0&&(S[fe]=null,E[fe].disconnect(ye))}for(let ee=0;ee<$.added.length;ee++){let ye=$.added[ee],fe=S.indexOf(ye);if(fe===-1){for(let we=0;we<E.length;we++)if(we>=S.length){S.push(ye),fe=we;break}else if(S[we]===null){S[we]=ye,fe=we;break}if(fe===-1)break}let Pe=E[fe];Pe&&Pe.connect(ye)}}let W=new B,K=new B;function G($,ee,ye){W.setFromMatrixPosition(ee.matrixWorld),K.setFromMatrixPosition(ye.matrixWorld);let fe=W.distanceTo(K),Pe=ee.projectionMatrix.elements,we=ye.projectionMatrix.elements,je=Pe[14]/(Pe[10]-1),ct=Pe[14]/(Pe[10]+1),$e=(Pe[9]+1)/Pe[5],T=(Pe[9]-1)/Pe[5],cn=(Pe[8]-1)/Pe[0],ze=(we[8]+1)/we[0],Ze=je*cn,Ae=je*ze,mt=fe/(-cn+ze),Re=mt*-cn;if(ee.matrixWorld.decompose($.position,$.quaternion,$.scale),$.translateX(Re),$.translateZ(mt),$.matrixWorld.compose($.position,$.quaternion,$.scale),$.matrixWorldInverse.copy($.matrixWorld).invert(),Pe[10]===-1)$.projectionMatrix.copy(ee.projectionMatrix),$.projectionMatrixInverse.copy(ee.projectionMatrixInverse);else{let b=je+mt,y=ct+mt,O=Ze-Re,X=Ae+(fe-Re),Q=$e*ct/y*b,q=T*ct/y*b;$.projectionMatrix.makePerspective(O,X,Q,q,b,y),$.projectionMatrixInverse.copy($.projectionMatrix).invert()}}function de($,ee){ee===null?$.matrixWorld.copy($.matrix):$.matrixWorld.multiplyMatrices(ee.matrixWorld,$.matrix),$.matrixWorldInverse.copy($.matrixWorld).invert()}this.updateCamera=function($){if(r===null)return;let ee=$.near,ye=$.far;_.texture!==null&&(_.depthNear>0&&(ee=_.depthNear),_.depthFar>0&&(ye=_.depthFar)),v.near=V.near=C.near=ee,v.far=V.far=C.far=ye,(M!==v.near||j!==v.far)&&(r.updateRenderState({depthNear:v.near,depthFar:v.far}),M=v.near,j=v.far);let fe=$.parent,Pe=v.cameras;de(v,fe);for(let we=0;we<Pe.length;we++)de(Pe[we],fe);Pe.length===2?G(v,C,V):v.projectionMatrix.copy(C.projectionMatrix),he($,v,fe)};function he($,ee,ye){ye===null?$.matrix.copy(ee.matrixWorld):($.matrix.copy(ye.matrixWorld),$.matrix.invert(),$.matrix.multiply(ee.matrixWorld)),$.matrix.decompose($.position,$.quaternion,$.scale),$.updateMatrixWorld(!0),$.projectionMatrix.copy(ee.projectionMatrix),$.projectionMatrixInverse.copy(ee.projectionMatrixInverse),$.isPerspectiveCamera&&($.fov=xf*2*Math.atan(1/$.projectionMatrix.elements[5]),$.zoom=1)}this.getCamera=function(){return v},this.getFoveation=function(){if(!(h===null&&f===null))return l},this.setFoveation=function($){l=$,h!==null&&(h.fixedFoveation=$),f!==null&&f.fixedFoveation!==void 0&&(f.fixedFoveation=$)},this.hasDepthSensing=function(){return _.texture!==null},this.getDepthSensingMesh=function(){return _.getMesh(v)};let xe=null;function Je($,ee){if(u=ee.getViewerPose(c||o),g=ee,u!==null){let ye=u.views;f!==null&&(e.setRenderTargetFramebuffer(w,f.framebuffer),e.setRenderTarget(w));let fe=!1;ye.length!==v.cameras.length&&(v.cameras.length=0,fe=!0);for(let we=0;we<ye.length;we++){let je=ye[we],ct=null;if(f!==null)ct=f.getViewport(je);else{let T=d.getViewSubImage(h,je);ct=T.viewport,we===0&&(e.setRenderTargetTextures(w,T.colorTexture,h.ignoreDepthValues?void 0:T.depthStencilTexture),e.setRenderTarget(w))}let $e=re[we];$e===void 0&&($e=new Jt,$e.layers.enable(we),$e.viewport=new wt,re[we]=$e),$e.matrix.fromArray(je.transform.matrix),$e.matrix.decompose($e.position,$e.quaternion,$e.scale),$e.projectionMatrix.fromArray(je.projectionMatrix),$e.projectionMatrixInverse.copy($e.projectionMatrix).invert(),$e.viewport.set(ct.x,ct.y,ct.width,ct.height),we===0&&(v.matrix.copy($e.matrix),v.matrix.decompose(v.position,v.quaternion,v.scale)),fe===!0&&v.cameras.push($e)}let Pe=r.enabledFeatures;if(Pe&&Pe.includes("depth-sensing")){let we=d.getDepthInformation(ye[0]);we&&we.isValid&&we.texture&&_.init(e,we,r.renderState)}}for(let ye=0;ye<E.length;ye++){let fe=S[ye],Pe=E[ye];fe!==null&&Pe!==void 0&&Pe.update(fe,ee,c||o)}xe&&xe($,ee),ee.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:ee}),g=null}let at=new k0;at.setAnimationLoop(Je),this.setAnimationLoop=function($){xe=$},this.dispose=function(){}}},_r=new Ar,u1=new It;function d1(n,e){function t(m,p){m.matrixAutoUpdate===!0&&m.updateMatrix(),p.value.copy(m.matrix)}function i(m,p){p.color.getRGB(m.fogColor.value,L0(n)),p.isFog?(m.fogNear.value=p.near,m.fogFar.value=p.far):p.isFogExp2&&(m.fogDensity.value=p.density)}function r(m,p,w,E,S){p.isMeshBasicMaterial||p.isMeshLambertMaterial?s(m,p):p.isMeshToonMaterial?(s(m,p),d(m,p)):p.isMeshPhongMaterial?(s(m,p),u(m,p)):p.isMeshStandardMaterial?(s(m,p),h(m,p),p.isMeshPhysicalMaterial&&f(m,p,S)):p.isMeshMatcapMaterial?(s(m,p),g(m,p)):p.isMeshDepthMaterial?s(m,p):p.isMeshDistanceMaterial?(s(m,p),_(m,p)):p.isMeshNormalMaterial?s(m,p):p.isLineBasicMaterial?(o(m,p),p.isLineDashedMaterial&&a(m,p)):p.isPointsMaterial?l(m,p,w,E):p.isSpriteMaterial?c(m,p):p.isShadowMaterial?(m.color.value.copy(p.color),m.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(m,p){m.opacity.value=p.opacity,p.color&&m.diffuse.value.copy(p.color),p.emissive&&m.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.bumpMap&&(m.bumpMap.value=p.bumpMap,t(p.bumpMap,m.bumpMapTransform),m.bumpScale.value=p.bumpScale,p.side===ln&&(m.bumpScale.value*=-1)),p.normalMap&&(m.normalMap.value=p.normalMap,t(p.normalMap,m.normalMapTransform),m.normalScale.value.copy(p.normalScale),p.side===ln&&m.normalScale.value.negate()),p.displacementMap&&(m.displacementMap.value=p.displacementMap,t(p.displacementMap,m.displacementMapTransform),m.displacementScale.value=p.displacementScale,m.displacementBias.value=p.displacementBias),p.emissiveMap&&(m.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,m.emissiveMapTransform)),p.specularMap&&(m.specularMap.value=p.specularMap,t(p.specularMap,m.specularMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest);let w=e.get(p),E=w.envMap,S=w.envMapRotation;E&&(m.envMap.value=E,_r.copy(S),_r.x*=-1,_r.y*=-1,_r.z*=-1,E.isCubeTexture&&E.isRenderTargetTexture===!1&&(_r.y*=-1,_r.z*=-1),m.envMapRotation.value.setFromMatrix4(u1.makeRotationFromEuler(_r)),m.flipEnvMap.value=E.isCubeTexture&&E.isRenderTargetTexture===!1?-1:1,m.reflectivity.value=p.reflectivity,m.ior.value=p.ior,m.refractionRatio.value=p.refractionRatio),p.lightMap&&(m.lightMap.value=p.lightMap,m.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,m.lightMapTransform)),p.aoMap&&(m.aoMap.value=p.aoMap,m.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,m.aoMapTransform))}function o(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform))}function a(m,p){m.dashSize.value=p.dashSize,m.totalSize.value=p.dashSize+p.gapSize,m.scale.value=p.scale}function l(m,p,w,E){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.size.value=p.size*w,m.scale.value=E*.5,p.map&&(m.map.value=p.map,t(p.map,m.uvTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function c(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.rotation.value=p.rotation,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function u(m,p){m.specular.value.copy(p.specular),m.shininess.value=Math.max(p.shininess,1e-4)}function d(m,p){p.gradientMap&&(m.gradientMap.value=p.gradientMap)}function h(m,p){m.metalness.value=p.metalness,p.metalnessMap&&(m.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,m.metalnessMapTransform)),m.roughness.value=p.roughness,p.roughnessMap&&(m.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,m.roughnessMapTransform)),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)}function f(m,p,w){m.ior.value=p.ior,p.sheen>0&&(m.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),m.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(m.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,m.sheenColorMapTransform)),p.sheenRoughnessMap&&(m.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,m.sheenRoughnessMapTransform))),p.clearcoat>0&&(m.clearcoat.value=p.clearcoat,m.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(m.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,m.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(m.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,m.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(m.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,m.clearcoatNormalMapTransform),m.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===ln&&m.clearcoatNormalScale.value.negate())),p.dispersion>0&&(m.dispersion.value=p.dispersion),p.iridescence>0&&(m.iridescence.value=p.iridescence,m.iridescenceIOR.value=p.iridescenceIOR,m.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],m.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(m.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,m.iridescenceMapTransform)),p.iridescenceThicknessMap&&(m.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,m.iridescenceThicknessMapTransform))),p.transmission>0&&(m.transmission.value=p.transmission,m.transmissionSamplerMap.value=w.texture,m.transmissionSamplerSize.value.set(w.width,w.height),p.transmissionMap&&(m.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,m.transmissionMapTransform)),m.thickness.value=p.thickness,p.thicknessMap&&(m.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,m.thicknessMapTransform)),m.attenuationDistance.value=p.attenuationDistance,m.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(m.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(m.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,m.anisotropyMapTransform))),m.specularIntensity.value=p.specularIntensity,m.specularColor.value.copy(p.specularColor),p.specularColorMap&&(m.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,m.specularColorMapTransform)),p.specularIntensityMap&&(m.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,m.specularIntensityMapTransform))}function g(m,p){p.matcap&&(m.matcap.value=p.matcap)}function _(m,p){let w=e.get(p).light;m.referencePosition.value.setFromMatrixPosition(w.matrixWorld),m.nearDistance.value=w.shadow.camera.near,m.farDistance.value=w.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function h1(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function l(w,E){let S=E.program;i.uniformBlockBinding(w,S)}function c(w,E){let S=r[w.id];S===void 0&&(g(w),S=u(w),r[w.id]=S,w.addEventListener("dispose",m));let k=E.program;i.updateUBOMapping(w,k);let I=e.render.frame;s[w.id]!==I&&(h(w),s[w.id]=I)}function u(w){let E=d();w.__bindingPointIndex=E;let S=n.createBuffer(),k=w.__size,I=w.usage;return n.bindBuffer(n.UNIFORM_BUFFER,S),n.bufferData(n.UNIFORM_BUFFER,k,I),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,E,S),S}function d(){for(let w=0;w<a;w++)if(o.indexOf(w)===-1)return o.push(w),w;return console.error("THREE.WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function h(w){let E=r[w.id],S=w.uniforms,k=w.__cache;n.bindBuffer(n.UNIFORM_BUFFER,E);for(let I=0,C=S.length;I<C;I++){let V=Array.isArray(S[I])?S[I]:[S[I]];for(let re=0,v=V.length;re<v;re++){let M=V[re];if(f(M,I,re,k)===!0){let j=M.__offset,z=Array.isArray(M.value)?M.value:[M.value],Y=0;for(let J=0;J<z.length;J++){let W=z[J],K=_(W);typeof W=="number"||typeof W=="boolean"?(M.__data[0]=W,n.bufferSubData(n.UNIFORM_BUFFER,j+Y,M.__data)):W.isMatrix3?(M.__data[0]=W.elements[0],M.__data[1]=W.elements[1],M.__data[2]=W.elements[2],M.__data[3]=0,M.__data[4]=W.elements[3],M.__data[5]=W.elements[4],M.__data[6]=W.elements[5],M.__data[7]=0,M.__data[8]=W.elements[6],M.__data[9]=W.elements[7],M.__data[10]=W.elements[8],M.__data[11]=0):(W.toArray(M.__data,Y),Y+=K.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,j,M.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function f(w,E,S,k){let I=w.value,C=E+"_"+S;if(k[C]===void 0)return typeof I=="number"||typeof I=="boolean"?k[C]=I:k[C]=I.clone(),!0;{let V=k[C];if(typeof I=="number"||typeof I=="boolean"){if(V!==I)return k[C]=I,!0}else if(V.equals(I)===!1)return V.copy(I),!0}return!1}function g(w){let E=w.uniforms,S=0,k=16;for(let C=0,V=E.length;C<V;C++){let re=Array.isArray(E[C])?E[C]:[E[C]];for(let v=0,M=re.length;v<M;v++){let j=re[v],z=Array.isArray(j.value)?j.value:[j.value];for(let Y=0,J=z.length;Y<J;Y++){let W=z[Y],K=_(W),G=S%k,de=G%K.boundary,he=G+de;S+=de,he!==0&&k-he<K.storage&&(S+=k-he),j.__data=new Float32Array(K.storage/Float32Array.BYTES_PER_ELEMENT),j.__offset=S,S+=K.storage}}}let I=S%k;return I>0&&(S+=k-I),w.__size=S,w.__cache={},this}function _(w){let E={boundary:0,storage:0};return typeof w=="number"||typeof w=="boolean"?(E.boundary=4,E.storage=4):w.isVector2?(E.boundary=8,E.storage=8):w.isVector3||w.isColor?(E.boundary=16,E.storage=12):w.isVector4?(E.boundary=16,E.storage=16):w.isMatrix3?(E.boundary=48,E.storage=48):w.isMatrix4?(E.boundary=64,E.storage=64):w.isTexture?console.warn("THREE.WebGLRenderer: Texture samplers can not be part of an uniforms group."):console.warn("THREE.WebGLRenderer: Unsupported uniform value type.",w),E}function m(w){let E=w.target;E.removeEventListener("dispose",m);let S=o.indexOf(E.__bindingPointIndex);o.splice(S,1),n.deleteBuffer(r[E.id]),delete r[E.id],delete s[E.id]}function p(){for(let w in r)n.deleteBuffer(r[w]);o=[],r={},s={}}return{bind:l,update:c,dispose:p}}var uc=class{constructor(e={}){let{canvas:t=mC(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:l=!0,preserveDrawingBuffer:c=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:d=!1}=e;this.isWebGLRenderer=!0;let h;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");h=i.getContextAttributes().alpha}else h=o;let f=new Uint32Array(4),g=new Int32Array(4),_=null,m=null,p=[],w=[];this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this._outputColorSpace=Yn,this.toneMapping=Wi,this.toneMappingExposure=1;let E=this,S=!1,k=0,I=0,C=null,V=-1,re=null,v=new wt,M=new wt,j=null,z=new Ye(0),Y=0,J=t.width,W=t.height,K=1,G=null,de=null,he=new wt(0,0,J,W),xe=new wt(0,0,J,W),Je=!1,at=new Ao,$=!1,ee=!1,ye=new It,fe=new It,Pe=new B,we=new wt,je={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},ct=!1;function $e(){return C===null?K:1}let T=i;function cn(x,N){return t.getContext(x,N)}try{let x={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:l,preserveDrawingBuffer:c,powerPreference:u,failIfMajorPerformanceCaveat:d};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${Xf}`),t.addEventListener("webglcontextlost",Z,!1),t.addEventListener("webglcontextrestored",ae,!1),t.addEventListener("webglcontextcreationerror",ue,!1),T===null){let N="webgl2";if(T=cn(N,x),T===null)throw cn(N)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(x){throw console.error("THREE.WebGLRenderer: "+x.message),x}let ze,Ze,Ae,mt,Re,b,y,O,X,Q,q,Me,oe,pe,Ke,te,me,Te,Ie,ge,Ge,Fe,ht,R;function ce(){ze=new AT(T),ze.init(),Fe=new o1(T,ze),Ze=new ET(T,ze,e,Fe),Ae=new i1(T),Ze.reverseDepthBuffer&&Ae.buffers.depth.setReversed(!0),mt=new RT(T),Re=new WI,b=new s1(T,ze,Ae,Re,Ze,Fe,mt),y=new ST(E),O=new DT(E),X=new UC(T),ht=new xT(T,X),Q=new TT(T,X,mt,ht),q=new PT(T,Q,X,mt),Ie=new NT(T,Ze,b),te=new bT(Re),Me=new GI(E,y,O,ze,Ze,ht,te),oe=new d1(E,Re),pe=new $I,Ke=new JI(ze),Te=new _T(E,y,O,Ae,q,h,l),me=new t1(E,q,Ze),R=new h1(T,mt,Ze,Ae),ge=new MT(T,ze,mt),Ge=new IT(T,ze,mt),mt.programs=Me.programs,E.capabilities=Ze,E.extensions=ze,E.properties=Re,E.renderLists=pe,E.shadowMap=me,E.state=Ae,E.info=mt}ce();let H=new kf(E,T);this.xr=H,this.getContext=function(){return T},this.getContextAttributes=function(){return T.getContextAttributes()},this.forceContextLoss=function(){let x=ze.get("WEBGL_lose_context");x&&x.loseContext()},this.forceContextRestore=function(){let x=ze.get("WEBGL_lose_context");x&&x.restoreContext()},this.getPixelRatio=function(){return K},this.setPixelRatio=function(x){x!==void 0&&(K=x,this.setSize(J,W,!1))},this.getSize=function(x){return x.set(J,W)},this.setSize=function(x,N,L=!0){if(H.isPresenting){console.warn("THREE.WebGLRenderer: Can't change size while VR device is presenting.");return}J=x,W=N,t.width=Math.floor(x*K),t.height=Math.floor(N*K),L===!0&&(t.style.width=x+"px",t.style.height=N+"px"),this.setViewport(0,0,x,N)},this.getDrawingBufferSize=function(x){return x.set(J*K,W*K).floor()},this.setDrawingBufferSize=function(x,N,L){J=x,W=N,K=L,t.width=Math.floor(x*L),t.height=Math.floor(N*L),this.setViewport(0,0,x,N)},this.getCurrentViewport=function(x){return x.copy(v)},this.getViewport=function(x){return x.copy(he)},this.setViewport=function(x,N,L,U){x.isVector4?he.set(x.x,x.y,x.z,x.w):he.set(x,N,L,U),Ae.viewport(v.copy(he).multiplyScalar(K).round())},this.getScissor=function(x){return x.copy(xe)},this.setScissor=function(x,N,L,U){x.isVector4?xe.set(x.x,x.y,x.z,x.w):xe.set(x,N,L,U),Ae.scissor(M.copy(xe).multiplyScalar(K).round())},this.getScissorTest=function(){return Je},this.setScissorTest=function(x){Ae.setScissorTest(Je=x)},this.setOpaqueSort=function(x){G=x},this.setTransparentSort=function(x){de=x},this.getClearColor=function(x){return x.copy(Te.getClearColor())},this.setClearColor=function(){Te.setClearColor.apply(Te,arguments)},this.getClearAlpha=function(){return Te.getClearAlpha()},this.setClearAlpha=function(){Te.setClearAlpha.apply(Te,arguments)},this.clear=function(x=!0,N=!0,L=!0){let U=0;if(x){let F=!1;if(C!==null){let ne=C.texture.format;F=ne===ep||ne===Qf||ne===Jf}if(F){let ne=C.texture.type,le=ne===Mi||ne===Cr||ne===Co||ne===Us||ne===Zf||ne===Kf,ve=Te.getClearColor(),_e=Te.getClearAlpha(),Se=ve.r,Ce=ve.g,Ee=ve.b;le?(f[0]=Se,f[1]=Ce,f[2]=Ee,f[3]=_e,T.clearBufferuiv(T.COLOR,0,f)):(g[0]=Se,g[1]=Ce,g[2]=Ee,g[3]=_e,T.clearBufferiv(T.COLOR,0,g))}else U|=T.COLOR_BUFFER_BIT}N&&(U|=T.DEPTH_BUFFER_BIT,T.clearDepth(this.capabilities.reverseDepthBuffer?0:1)),L&&(U|=T.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),T.clear(U)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.dispose=function(){t.removeEventListener("webglcontextlost",Z,!1),t.removeEventListener("webglcontextrestored",ae,!1),t.removeEventListener("webglcontextcreationerror",ue,!1),pe.dispose(),Ke.dispose(),Re.dispose(),y.dispose(),O.dispose(),q.dispose(),ht.dispose(),R.dispose(),Me.dispose(),H.dispose(),H.removeEventListener("sessionstart",yp),H.removeEventListener("sessionend",_p),Zi.stop()};function Z(x){x.preventDefault(),console.log("THREE.WebGLRenderer: Context Lost."),S=!0}function ae(){console.log("THREE.WebGLRenderer: Context Restored."),S=!1;let x=mt.autoReset,N=me.enabled,L=me.autoUpdate,U=me.needsUpdate,F=me.type;ce(),mt.autoReset=x,me.enabled=N,me.autoUpdate=L,me.needsUpdate=U,me.type=F}function ue(x){console.error("THREE.WebGLRenderer: A WebGL context could not be created. Reason: ",x.statusMessage)}function qe(x){let N=x.target;N.removeEventListener("dispose",qe),Rt(N)}function Rt(x){Qt(x),Re.remove(x)}function Qt(x){let N=Re.get(x).programs;N!==void 0&&(N.forEach(function(L){Me.releaseProgram(L)}),x.isShaderMaterial&&Me.releaseShaderCache(x))}this.renderBufferDirect=function(x,N,L,U,F,ne){N===null&&(N=je);let le=F.isMesh&&F.matrixWorld.determinant()<0,ve=f_(x,N,L,U,F);Ae.setMaterial(U,le);let _e=L.index,Se=1;if(U.wireframe===!0){if(_e=Q.getWireframeAttribute(L),_e===void 0)return;Se=2}let Ce=L.drawRange,Ee=L.attributes.position,lt=Ce.start*Se,gt=(Ce.start+Ce.count)*Se;ne!==null&&(lt=Math.max(lt,ne.start*Se),gt=Math.min(gt,(ne.start+ne.count)*Se)),_e!==null?(lt=Math.max(lt,0),gt=Math.min(gt,_e.count)):Ee!=null&&(lt=Math.max(lt,0),gt=Math.min(gt,Ee.count));let Et=gt-lt;if(Et<0||Et===1/0)return;ht.setup(F,U,ve,L,_e);let un,tt=ge;if(_e!==null&&(un=X.get(_e),tt=Ge,tt.setIndex(un)),F.isMesh)U.wireframe===!0?(Ae.setLineWidth(U.wireframeLinewidth*$e()),tt.setMode(T.LINES)):tt.setMode(T.TRIANGLES);else if(F.isLine){let be=U.linewidth;be===void 0&&(be=1),Ae.setLineWidth(be*$e()),F.isLineSegments?tt.setMode(T.LINES):F.isLineLoop?tt.setMode(T.LINE_LOOP):tt.setMode(T.LINE_STRIP)}else F.isPoints?tt.setMode(T.POINTS):F.isSprite&&tt.setMode(T.TRIANGLES);if(F.isBatchedMesh)if(F._multiDrawInstances!==null)tt.renderMultiDrawInstances(F._multiDrawStarts,F._multiDrawCounts,F._multiDrawCount,F._multiDrawInstances);else if(ze.get("WEBGL_multi_draw"))tt.renderMultiDraw(F._multiDrawStarts,F._multiDrawCounts,F._multiDrawCount);else{let be=F._multiDrawStarts,Bt=F._multiDrawCounts,nt=F._multiDrawCount,Dn=_e?X.get(_e).bytesPerElement:1,Fr=Re.get(U).currentProgram.getUniforms();for(let dn=0;dn<nt;dn++)Fr.setValue(T,"_gl_DrawID",dn),tt.render(be[dn]/Dn,Bt[dn])}else if(F.isInstancedMesh)tt.renderInstances(lt,Et,F.count);else if(L.isInstancedBufferGeometry){let be=L._maxInstanceCount!==void 0?L._maxInstanceCount:1/0,Bt=Math.min(L.instanceCount,be);tt.renderInstances(lt,Et,Bt)}else tt.render(lt,Et)};function Qe(x,N,L){x.transparent===!0&&x.side===gi&&x.forceSinglePass===!1?(x.side=ln,x.needsUpdate=!0,Vo(x,N,L),x.side=ji,x.needsUpdate=!0,Vo(x,N,L),x.side=gi):Vo(x,N,L)}this.compile=function(x,N,L=null){L===null&&(L=x),m=Ke.get(L),m.init(N),w.push(m),L.traverseVisible(function(F){F.isLight&&F.layers.test(N.layers)&&(m.pushLight(F),F.castShadow&&m.pushShadow(F))}),x!==L&&x.traverseVisible(function(F){F.isLight&&F.layers.test(N.layers)&&(m.pushLight(F),F.castShadow&&m.pushShadow(F))}),m.setupLights();let U=new Set;return x.traverse(function(F){if(!(F.isMesh||F.isPoints||F.isLine||F.isSprite))return;let ne=F.material;if(ne)if(Array.isArray(ne))for(let le=0;le<ne.length;le++){let ve=ne[le];Qe(ve,L,F),U.add(ve)}else Qe(ne,L,F),U.add(ne)}),w.pop(),m=null,U},this.compileAsync=function(x,N,L=null){let U=this.compile(x,N,L);return new Promise(F=>{function ne(){if(U.forEach(function(le){Re.get(le).currentProgram.isReady()&&U.delete(le)}),U.size===0){F(x);return}setTimeout(ne,10)}ze.get("KHR_parallel_shader_compile")!==null?ne():setTimeout(ne,10)})};let en=null;function Jn(x){en&&en(x)}function yp(){Zi.stop()}function _p(){Zi.start()}let Zi=new k0;Zi.setAnimationLoop(Jn),typeof self<"u"&&Zi.setContext(self),this.setAnimationLoop=function(x){en=x,H.setAnimationLoop(x),x===null?Zi.stop():Zi.start()},H.addEventListener("sessionstart",yp),H.addEventListener("sessionend",_p),this.render=function(x,N){if(N!==void 0&&N.isCamera!==!0){console.error("THREE.WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(S===!0)return;if(x.matrixWorldAutoUpdate===!0&&x.updateMatrixWorld(),N.parent===null&&N.matrixWorldAutoUpdate===!0&&N.updateMatrixWorld(),H.enabled===!0&&H.isPresenting===!0&&(H.cameraAutoUpdate===!0&&H.updateCamera(N),N=H.getCamera()),x.isScene===!0&&x.onBeforeRender(E,x,N,C),m=Ke.get(x,w.length),m.init(N),w.push(m),fe.multiplyMatrices(N.projectionMatrix,N.matrixWorldInverse),at.setFromProjectionMatrix(fe),ee=this.localClippingEnabled,$=te.init(this.clippingPlanes,ee),_=pe.get(x,p.length),_.init(),p.push(_),H.enabled===!0&&H.isPresenting===!0){let ne=E.xr.getDepthSensingMesh();ne!==null&&Rc(ne,N,-1/0,E.sortObjects)}Rc(x,N,0,E.sortObjects),_.finish(),E.sortObjects===!0&&_.sort(G,de),ct=H.enabled===!1||H.isPresenting===!1||H.hasDepthSensing()===!1,ct&&Te.addToRenderList(_,x),this.info.render.frame++,$===!0&&te.beginShadows();let L=m.state.shadowsArray;me.render(L,x,N),$===!0&&te.endShadows(),this.info.autoReset===!0&&this.info.reset();let U=_.opaque,F=_.transmissive;if(m.setupLights(),N.isArrayCamera){let ne=N.cameras;if(F.length>0)for(let le=0,ve=ne.length;le<ve;le++){let _e=ne[le];Mp(U,F,x,_e)}ct&&Te.render(x);for(let le=0,ve=ne.length;le<ve;le++){let _e=ne[le];xp(_,x,_e,_e.viewport)}}else F.length>0&&Mp(U,F,x,N),ct&&Te.render(x),xp(_,x,N);C!==null&&(b.updateMultisampleRenderTarget(C),b.updateRenderTargetMipmap(C)),x.isScene===!0&&x.onAfterRender(E,x,N),ht.resetDefaultState(),V=-1,re=null,w.pop(),w.length>0?(m=w[w.length-1],$===!0&&te.setGlobalState(E.clippingPlanes,m.state.camera)):m=null,p.pop(),p.length>0?_=p[p.length-1]:_=null};function Rc(x,N,L,U){if(x.visible===!1)return;if(x.layers.test(N.layers)){if(x.isGroup)L=x.renderOrder;else if(x.isLOD)x.autoUpdate===!0&&x.update(N);else if(x.isLight)m.pushLight(x),x.castShadow&&m.pushShadow(x);else if(x.isSprite){if(!x.frustumCulled||at.intersectsSprite(x)){U&&we.setFromMatrixPosition(x.matrixWorld).applyMatrix4(fe);let le=q.update(x),ve=x.material;ve.visible&&_.push(x,le,ve,L,we.z,null)}}else if((x.isMesh||x.isLine||x.isPoints)&&(!x.frustumCulled||at.intersectsObject(x))){let le=q.update(x),ve=x.material;if(U&&(x.boundingSphere!==void 0?(x.boundingSphere===null&&x.computeBoundingSphere(),we.copy(x.boundingSphere.center)):(le.boundingSphere===null&&le.computeBoundingSphere(),we.copy(le.boundingSphere.center)),we.applyMatrix4(x.matrixWorld).applyMatrix4(fe)),Array.isArray(ve)){let _e=le.groups;for(let Se=0,Ce=_e.length;Se<Ce;Se++){let Ee=_e[Se],lt=ve[Ee.materialIndex];lt&&lt.visible&&_.push(x,le,lt,L,we.z,Ee)}}else ve.visible&&_.push(x,le,ve,L,we.z,null)}}let ne=x.children;for(let le=0,ve=ne.length;le<ve;le++)Rc(ne[le],N,L,U)}function xp(x,N,L,U){let F=x.opaque,ne=x.transmissive,le=x.transparent;m.setupLightsView(L),$===!0&&te.setGlobalState(E.clippingPlanes,L),U&&Ae.viewport(v.copy(U)),F.length>0&&Uo(F,N,L),ne.length>0&&Uo(ne,N,L),le.length>0&&Uo(le,N,L),Ae.buffers.depth.setTest(!0),Ae.buffers.depth.setMask(!0),Ae.buffers.color.setMask(!0),Ae.setPolygonOffset(!1)}function Mp(x,N,L,U){if((L.isScene===!0?L.overrideMaterial:null)!==null)return;m.state.transmissionRenderTarget[U.id]===void 0&&(m.state.transmissionRenderTarget[U.id]=new Ei(1,1,{generateMipmaps:!0,type:ze.has("EXT_color_buffer_half_float")||ze.has("EXT_color_buffer_float")?Io:Mi,minFilter:wr,samples:4,stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:st.workingColorSpace}));let ne=m.state.transmissionRenderTarget[U.id],le=U.viewport||v;ne.setSize(le.z,le.w);let ve=E.getRenderTarget();E.setRenderTarget(ne),E.getClearColor(z),Y=E.getClearAlpha(),Y<1&&E.setClearColor(16777215,.5),E.clear(),ct&&Te.render(L);let _e=E.toneMapping;E.toneMapping=Wi;let Se=U.viewport;if(U.viewport!==void 0&&(U.viewport=void 0),m.setupLightsView(U),$===!0&&te.setGlobalState(E.clippingPlanes,U),Uo(x,L,U),b.updateMultisampleRenderTarget(ne),b.updateRenderTargetMipmap(ne),ze.has("WEBGL_multisampled_render_to_texture")===!1){let Ce=!1;for(let Ee=0,lt=N.length;Ee<lt;Ee++){let gt=N[Ee],Et=gt.object,un=gt.geometry,tt=gt.material,be=gt.group;if(tt.side===gi&&Et.layers.test(U.layers)){let Bt=tt.side;tt.side=ln,tt.needsUpdate=!0,Ep(Et,L,U,un,tt,be),tt.side=Bt,tt.needsUpdate=!0,Ce=!0}}Ce===!0&&(b.updateMultisampleRenderTarget(ne),b.updateRenderTargetMipmap(ne))}E.setRenderTarget(ve),E.setClearColor(z,Y),Se!==void 0&&(U.viewport=Se),E.toneMapping=_e}function Uo(x,N,L){let U=N.isScene===!0?N.overrideMaterial:null;for(let F=0,ne=x.length;F<ne;F++){let le=x[F],ve=le.object,_e=le.geometry,Se=U===null?le.material:U,Ce=le.group;ve.layers.test(L.layers)&&Ep(ve,N,L,_e,Se,Ce)}}function Ep(x,N,L,U,F,ne){x.onBeforeRender(E,N,L,U,F,ne),x.modelViewMatrix.multiplyMatrices(L.matrixWorldInverse,x.matrixWorld),x.normalMatrix.getNormalMatrix(x.modelViewMatrix),F.onBeforeRender(E,N,L,U,x,ne),F.transparent===!0&&F.side===gi&&F.forceSinglePass===!1?(F.side=ln,F.needsUpdate=!0,E.renderBufferDirect(L,N,U,F,x,ne),F.side=ji,F.needsUpdate=!0,E.renderBufferDirect(L,N,U,F,x,ne),F.side=gi):E.renderBufferDirect(L,N,U,F,x,ne),x.onAfterRender(E,N,L,U,F,ne)}function Vo(x,N,L){N.isScene!==!0&&(N=je);let U=Re.get(x),F=m.state.lights,ne=m.state.shadowsArray,le=F.state.version,ve=Me.getParameters(x,F.state,ne,N,L),_e=Me.getProgramCacheKey(ve),Se=U.programs;U.environment=x.isMeshStandardMaterial?N.environment:null,U.fog=N.fog,U.envMap=(x.isMeshStandardMaterial?O:y).get(x.envMap||U.environment),U.envMapRotation=U.environment!==null&&x.envMap===null?N.environmentRotation:x.envMapRotation,Se===void 0&&(x.addEventListener("dispose",qe),Se=new Map,U.programs=Se);let Ce=Se.get(_e);if(Ce!==void 0){if(U.currentProgram===Ce&&U.lightsStateVersion===le)return Sp(x,ve),Ce}else ve.uniforms=Me.getUniforms(x),x.onBeforeCompile(ve,E),Ce=Me.acquireProgram(ve,_e),Se.set(_e,Ce),U.uniforms=ve.uniforms;let Ee=U.uniforms;return(!x.isShaderMaterial&&!x.isRawShaderMaterial||x.clipping===!0)&&(Ee.clippingPlanes=te.uniform),Sp(x,ve),U.needsLights=m_(x),U.lightsStateVersion=le,U.needsLights&&(Ee.ambientLightColor.value=F.state.ambient,Ee.lightProbe.value=F.state.probe,Ee.directionalLights.value=F.state.directional,Ee.directionalLightShadows.value=F.state.directionalShadow,Ee.spotLights.value=F.state.spot,Ee.spotLightShadows.value=F.state.spotShadow,Ee.rectAreaLights.value=F.state.rectArea,Ee.ltc_1.value=F.state.rectAreaLTC1,Ee.ltc_2.value=F.state.rectAreaLTC2,Ee.pointLights.value=F.state.point,Ee.pointLightShadows.value=F.state.pointShadow,Ee.hemisphereLights.value=F.state.hemi,Ee.directionalShadowMap.value=F.state.directionalShadowMap,Ee.directionalShadowMatrix.value=F.state.directionalShadowMatrix,Ee.spotShadowMap.value=F.state.spotShadowMap,Ee.spotLightMatrix.value=F.state.spotLightMatrix,Ee.spotLightMap.value=F.state.spotLightMap,Ee.pointShadowMap.value=F.state.pointShadowMap,Ee.pointShadowMatrix.value=F.state.pointShadowMatrix),U.currentProgram=Ce,U.uniformsList=null,Ce}function bp(x){if(x.uniformsList===null){let N=x.currentProgram.getUniforms();x.uniformsList=Fs.seqWithValue(N.seq,x.uniforms)}return x.uniformsList}function Sp(x,N){let L=Re.get(x);L.outputColorSpace=N.outputColorSpace,L.batching=N.batching,L.batchingColor=N.batchingColor,L.instancing=N.instancing,L.instancingColor=N.instancingColor,L.instancingMorph=N.instancingMorph,L.skinning=N.skinning,L.morphTargets=N.morphTargets,L.morphNormals=N.morphNormals,L.morphColors=N.morphColors,L.morphTargetsCount=N.morphTargetsCount,L.numClippingPlanes=N.numClippingPlanes,L.numIntersection=N.numClipIntersection,L.vertexAlphas=N.vertexAlphas,L.vertexTangents=N.vertexTangents,L.toneMapping=N.toneMapping}function f_(x,N,L,U,F){N.isScene!==!0&&(N=je),b.resetTextureUnits();let ne=N.fog,le=U.isMeshStandardMaterial?N.environment:null,ve=C===null?E.outputColorSpace:C.isXRRenderTarget===!0?C.texture.colorSpace:Yi,_e=(U.isMeshStandardMaterial?O:y).get(U.envMap||le),Se=U.vertexColors===!0&&!!L.attributes.color&&L.attributes.color.itemSize===4,Ce=!!L.attributes.tangent&&(!!U.normalMap||U.anisotropy>0),Ee=!!L.morphAttributes.position,lt=!!L.morphAttributes.normal,gt=!!L.morphAttributes.color,Et=Wi;U.toneMapped&&(C===null||C.isXRRenderTarget===!0)&&(Et=E.toneMapping);let un=L.morphAttributes.position||L.morphAttributes.normal||L.morphAttributes.color,tt=un!==void 0?un.length:0,be=Re.get(U),Bt=m.state.lights;if($===!0&&(ee===!0||x!==re)){let vn=x===re&&U.id===V;te.setState(U,x,vn)}let nt=!1;U.version===be.__version?(be.needsLights&&be.lightsStateVersion!==Bt.state.version||be.outputColorSpace!==ve||F.isBatchedMesh&&be.batching===!1||!F.isBatchedMesh&&be.batching===!0||F.isBatchedMesh&&be.batchingColor===!0&&F.colorTexture===null||F.isBatchedMesh&&be.batchingColor===!1&&F.colorTexture!==null||F.isInstancedMesh&&be.instancing===!1||!F.isInstancedMesh&&be.instancing===!0||F.isSkinnedMesh&&be.skinning===!1||!F.isSkinnedMesh&&be.skinning===!0||F.isInstancedMesh&&be.instancingColor===!0&&F.instanceColor===null||F.isInstancedMesh&&be.instancingColor===!1&&F.instanceColor!==null||F.isInstancedMesh&&be.instancingMorph===!0&&F.morphTexture===null||F.isInstancedMesh&&be.instancingMorph===!1&&F.morphTexture!==null||be.envMap!==_e||U.fog===!0&&be.fog!==ne||be.numClippingPlanes!==void 0&&(be.numClippingPlanes!==te.numPlanes||be.numIntersection!==te.numIntersection)||be.vertexAlphas!==Se||be.vertexTangents!==Ce||be.morphTargets!==Ee||be.morphNormals!==lt||be.morphColors!==gt||be.toneMapping!==Et||be.morphTargetsCount!==tt)&&(nt=!0):(nt=!0,be.__version=U.version);let Dn=be.currentProgram;nt===!0&&(Dn=Vo(U,N,F));let Fr=!1,dn=!1,Nc=!1,Ct=Dn.getUniforms(),Si=be.uniforms;if(Ae.useProgram(Dn.program)&&(Fr=!0,dn=!0,Nc=!0),U.id!==V&&(V=U.id,dn=!0),Fr||re!==x){Ze.reverseDepthBuffer?(ye.copy(x.projectionMatrix),vC(ye),yC(ye),Ct.setValue(T,"projectionMatrix",ye)):Ct.setValue(T,"projectionMatrix",x.projectionMatrix),Ct.setValue(T,"viewMatrix",x.matrixWorldInverse);let vn=Ct.map.cameraPosition;vn!==void 0&&vn.setValue(T,Pe.setFromMatrixPosition(x.matrixWorld)),Ze.logarithmicDepthBuffer&&Ct.setValue(T,"logDepthBufFC",2/(Math.log(x.far+1)/Math.LN2)),(U.isMeshPhongMaterial||U.isMeshToonMaterial||U.isMeshLambertMaterial||U.isMeshBasicMaterial||U.isMeshStandardMaterial||U.isShaderMaterial)&&Ct.setValue(T,"isOrthographic",x.isOrthographicCamera===!0),re!==x&&(re=x,dn=!0,Nc=!0)}if(F.isSkinnedMesh){Ct.setOptional(T,F,"bindMatrix"),Ct.setOptional(T,F,"bindMatrixInverse");let vn=F.skeleton;vn&&(vn.boneTexture===null&&vn.computeBoneTexture(),Ct.setValue(T,"boneTexture",vn.boneTexture,b))}F.isBatchedMesh&&(Ct.setOptional(T,F,"batchingTexture"),Ct.setValue(T,"batchingTexture",F._matricesTexture,b),Ct.setOptional(T,F,"batchingIdTexture"),Ct.setValue(T,"batchingIdTexture",F._indirectTexture,b),Ct.setOptional(T,F,"batchingColorTexture"),F._colorsTexture!==null&&Ct.setValue(T,"batchingColorTexture",F._colorsTexture,b));let Pc=L.morphAttributes;if((Pc.position!==void 0||Pc.normal!==void 0||Pc.color!==void 0)&&Ie.update(F,L,Dn),(dn||be.receiveShadow!==F.receiveShadow)&&(be.receiveShadow=F.receiveShadow,Ct.setValue(T,"receiveShadow",F.receiveShadow)),U.isMeshGouraudMaterial&&U.envMap!==null&&(Si.envMap.value=_e,Si.flipEnvMap.value=_e.isCubeTexture&&_e.isRenderTargetTexture===!1?-1:1),U.isMeshStandardMaterial&&U.envMap===null&&N.environment!==null&&(Si.envMapIntensity.value=N.environmentIntensity),dn&&(Ct.setValue(T,"toneMappingExposure",E.toneMappingExposure),be.needsLights&&p_(Si,Nc),ne&&U.fog===!0&&oe.refreshFogUniforms(Si,ne),oe.refreshMaterialUniforms(Si,U,K,W,m.state.transmissionRenderTarget[x.id]),Fs.upload(T,bp(be),Si,b)),U.isShaderMaterial&&U.uniformsNeedUpdate===!0&&(Fs.upload(T,bp(be),Si,b),U.uniformsNeedUpdate=!1),U.isSpriteMaterial&&Ct.setValue(T,"center",F.center),Ct.setValue(T,"modelViewMatrix",F.modelViewMatrix),Ct.setValue(T,"normalMatrix",F.normalMatrix),Ct.setValue(T,"modelMatrix",F.matrixWorld),U.isShaderMaterial||U.isRawShaderMaterial){let vn=U.uniformsGroups;for(let Fc=0,g_=vn.length;Fc<g_;Fc++){let wp=vn[Fc];R.update(wp,Dn),R.bind(wp,Dn)}}return Dn}function p_(x,N){x.ambientLightColor.needsUpdate=N,x.lightProbe.needsUpdate=N,x.directionalLights.needsUpdate=N,x.directionalLightShadows.needsUpdate=N,x.pointLights.needsUpdate=N,x.pointLightShadows.needsUpdate=N,x.spotLights.needsUpdate=N,x.spotLightShadows.needsUpdate=N,x.rectAreaLights.needsUpdate=N,x.hemisphereLights.needsUpdate=N}function m_(x){return x.isMeshLambertMaterial||x.isMeshToonMaterial||x.isMeshPhongMaterial||x.isMeshStandardMaterial||x.isShadowMaterial||x.isShaderMaterial&&x.lights===!0}this.getActiveCubeFace=function(){return k},this.getActiveMipmapLevel=function(){return I},this.getRenderTarget=function(){return C},this.setRenderTargetTextures=function(x,N,L){Re.get(x.texture).__webglTexture=N,Re.get(x.depthTexture).__webglTexture=L;let U=Re.get(x);U.__hasExternalTextures=!0,U.__autoAllocateDepthBuffer=L===void 0,U.__autoAllocateDepthBuffer||ze.has("WEBGL_multisampled_render_to_texture")===!0&&(console.warn("THREE.WebGLRenderer: Render-to-texture extension was disabled because an external texture was provided"),U.__useRenderToTexture=!1)},this.setRenderTargetFramebuffer=function(x,N){let L=Re.get(x);L.__webglFramebuffer=N,L.__useDefaultFramebuffer=N===void 0},this.setRenderTarget=function(x,N=0,L=0){C=x,k=N,I=L;let U=!0,F=null,ne=!1,le=!1;if(x){let _e=Re.get(x);if(_e.__useDefaultFramebuffer!==void 0)Ae.bindFramebuffer(T.FRAMEBUFFER,null),U=!1;else if(_e.__webglFramebuffer===void 0)b.setupRenderTarget(x);else if(_e.__hasExternalTextures)b.rebindTextures(x,Re.get(x.texture).__webglTexture,Re.get(x.depthTexture).__webglTexture);else if(x.depthBuffer){let Ee=x.depthTexture;if(_e.__boundDepthTexture!==Ee){if(Ee!==null&&Re.has(Ee)&&(x.width!==Ee.image.width||x.height!==Ee.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");b.setupDepthRenderbuffer(x)}}let Se=x.texture;(Se.isData3DTexture||Se.isDataArrayTexture||Se.isCompressedArrayTexture)&&(le=!0);let Ce=Re.get(x).__webglFramebuffer;x.isWebGLCubeRenderTarget?(Array.isArray(Ce[N])?F=Ce[N][L]:F=Ce[N],ne=!0):x.samples>0&&b.useMultisampledRTT(x)===!1?F=Re.get(x).__webglMultisampledFramebuffer:Array.isArray(Ce)?F=Ce[L]:F=Ce,v.copy(x.viewport),M.copy(x.scissor),j=x.scissorTest}else v.copy(he).multiplyScalar(K).floor(),M.copy(xe).multiplyScalar(K).floor(),j=Je;if(Ae.bindFramebuffer(T.FRAMEBUFFER,F)&&U&&Ae.drawBuffers(x,F),Ae.viewport(v),Ae.scissor(M),Ae.setScissorTest(j),ne){let _e=Re.get(x.texture);T.framebufferTexture2D(T.FRAMEBUFFER,T.COLOR_ATTACHMENT0,T.TEXTURE_CUBE_MAP_POSITIVE_X+N,_e.__webglTexture,L)}else if(le){let _e=Re.get(x.texture),Se=N||0;T.framebufferTextureLayer(T.FRAMEBUFFER,T.COLOR_ATTACHMENT0,_e.__webglTexture,L||0,Se)}V=-1},this.readRenderTargetPixels=function(x,N,L,U,F,ne,le){if(!(x&&x.isWebGLRenderTarget)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let ve=Re.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&le!==void 0&&(ve=ve[le]),ve){Ae.bindFramebuffer(T.FRAMEBUFFER,ve);try{let _e=x.texture,Se=_e.format,Ce=_e.type;if(!Ze.textureFormatReadable(Se)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!Ze.textureTypeReadable(Ce)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}N>=0&&N<=x.width-U&&L>=0&&L<=x.height-F&&T.readPixels(N,L,U,F,Fe.convert(Se),Fe.convert(Ce),ne)}finally{let _e=C!==null?Re.get(C).__webglFramebuffer:null;Ae.bindFramebuffer(T.FRAMEBUFFER,_e)}}},this.readRenderTargetPixelsAsync=function(x,N,L,U,F,ne,le){return Ki(this,null,function*(){if(!(x&&x.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let ve=Re.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&le!==void 0&&(ve=ve[le]),ve){let _e=x.texture,Se=_e.format,Ce=_e.type;if(!Ze.textureFormatReadable(Se))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!Ze.textureTypeReadable(Ce))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");if(N>=0&&N<=x.width-U&&L>=0&&L<=x.height-F){Ae.bindFramebuffer(T.FRAMEBUFFER,ve);let Ee=T.createBuffer();T.bindBuffer(T.PIXEL_PACK_BUFFER,Ee),T.bufferData(T.PIXEL_PACK_BUFFER,ne.byteLength,T.STREAM_READ),T.readPixels(N,L,U,F,Fe.convert(Se),Fe.convert(Ce),0);let lt=C!==null?Re.get(C).__webglFramebuffer:null;Ae.bindFramebuffer(T.FRAMEBUFFER,lt);let gt=T.fenceSync(T.SYNC_GPU_COMMANDS_COMPLETE,0);return T.flush(),yield gC(T,gt,4),T.bindBuffer(T.PIXEL_PACK_BUFFER,Ee),T.getBufferSubData(T.PIXEL_PACK_BUFFER,0,ne),T.deleteBuffer(Ee),T.deleteSync(gt),ne}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")}})},this.copyFramebufferToTexture=function(x,N=null,L=0){x.isTexture!==!0&&(jl("WebGLRenderer: copyFramebufferToTexture function signature has changed."),N=arguments[0]||null,x=arguments[1]);let U=Math.pow(2,-L),F=Math.floor(x.image.width*U),ne=Math.floor(x.image.height*U),le=N!==null?N.x:0,ve=N!==null?N.y:0;b.setTexture2D(x,0),T.copyTexSubImage2D(T.TEXTURE_2D,L,0,0,le,ve,F,ne),Ae.unbindTexture()},this.copyTextureToTexture=function(x,N,L=null,U=null,F=0){x.isTexture!==!0&&(jl("WebGLRenderer: copyTextureToTexture function signature has changed."),U=arguments[0]||null,x=arguments[1],N=arguments[2],F=arguments[3]||0,L=null);let ne,le,ve,_e,Se,Ce;L!==null?(ne=L.max.x-L.min.x,le=L.max.y-L.min.y,ve=L.min.x,_e=L.min.y):(ne=x.image.width,le=x.image.height,ve=0,_e=0),U!==null?(Se=U.x,Ce=U.y):(Se=0,Ce=0);let Ee=Fe.convert(N.format),lt=Fe.convert(N.type);b.setTexture2D(N,0),T.pixelStorei(T.UNPACK_FLIP_Y_WEBGL,N.flipY),T.pixelStorei(T.UNPACK_PREMULTIPLY_ALPHA_WEBGL,N.premultiplyAlpha),T.pixelStorei(T.UNPACK_ALIGNMENT,N.unpackAlignment);let gt=T.getParameter(T.UNPACK_ROW_LENGTH),Et=T.getParameter(T.UNPACK_IMAGE_HEIGHT),un=T.getParameter(T.UNPACK_SKIP_PIXELS),tt=T.getParameter(T.UNPACK_SKIP_ROWS),be=T.getParameter(T.UNPACK_SKIP_IMAGES),Bt=x.isCompressedTexture?x.mipmaps[F]:x.image;T.pixelStorei(T.UNPACK_ROW_LENGTH,Bt.width),T.pixelStorei(T.UNPACK_IMAGE_HEIGHT,Bt.height),T.pixelStorei(T.UNPACK_SKIP_PIXELS,ve),T.pixelStorei(T.UNPACK_SKIP_ROWS,_e),x.isDataTexture?T.texSubImage2D(T.TEXTURE_2D,F,Se,Ce,ne,le,Ee,lt,Bt.data):x.isCompressedTexture?T.compressedTexSubImage2D(T.TEXTURE_2D,F,Se,Ce,Bt.width,Bt.height,Ee,Bt.data):T.texSubImage2D(T.TEXTURE_2D,F,Se,Ce,ne,le,Ee,lt,Bt),T.pixelStorei(T.UNPACK_ROW_LENGTH,gt),T.pixelStorei(T.UNPACK_IMAGE_HEIGHT,Et),T.pixelStorei(T.UNPACK_SKIP_PIXELS,un),T.pixelStorei(T.UNPACK_SKIP_ROWS,tt),T.pixelStorei(T.UNPACK_SKIP_IMAGES,be),F===0&&N.generateMipmaps&&T.generateMipmap(T.TEXTURE_2D),Ae.unbindTexture()},this.copyTextureToTexture3D=function(x,N,L=null,U=null,F=0){x.isTexture!==!0&&(jl("WebGLRenderer: copyTextureToTexture3D function signature has changed."),L=arguments[0]||null,U=arguments[1]||null,x=arguments[2],N=arguments[3],F=arguments[4]||0);let ne,le,ve,_e,Se,Ce,Ee,lt,gt,Et=x.isCompressedTexture?x.mipmaps[F]:x.image;L!==null?(ne=L.max.x-L.min.x,le=L.max.y-L.min.y,ve=L.max.z-L.min.z,_e=L.min.x,Se=L.min.y,Ce=L.min.z):(ne=Et.width,le=Et.height,ve=Et.depth,_e=0,Se=0,Ce=0),U!==null?(Ee=U.x,lt=U.y,gt=U.z):(Ee=0,lt=0,gt=0);let un=Fe.convert(N.format),tt=Fe.convert(N.type),be;if(N.isData3DTexture)b.setTexture3D(N,0),be=T.TEXTURE_3D;else if(N.isDataArrayTexture||N.isCompressedArrayTexture)b.setTexture2DArray(N,0),be=T.TEXTURE_2D_ARRAY;else{console.warn("THREE.WebGLRenderer.copyTextureToTexture3D: only supports THREE.DataTexture3D and THREE.DataTexture2DArray.");return}T.pixelStorei(T.UNPACK_FLIP_Y_WEBGL,N.flipY),T.pixelStorei(T.UNPACK_PREMULTIPLY_ALPHA_WEBGL,N.premultiplyAlpha),T.pixelStorei(T.UNPACK_ALIGNMENT,N.unpackAlignment);let Bt=T.getParameter(T.UNPACK_ROW_LENGTH),nt=T.getParameter(T.UNPACK_IMAGE_HEIGHT),Dn=T.getParameter(T.UNPACK_SKIP_PIXELS),Fr=T.getParameter(T.UNPACK_SKIP_ROWS),dn=T.getParameter(T.UNPACK_SKIP_IMAGES);T.pixelStorei(T.UNPACK_ROW_LENGTH,Et.width),T.pixelStorei(T.UNPACK_IMAGE_HEIGHT,Et.height),T.pixelStorei(T.UNPACK_SKIP_PIXELS,_e),T.pixelStorei(T.UNPACK_SKIP_ROWS,Se),T.pixelStorei(T.UNPACK_SKIP_IMAGES,Ce),x.isDataTexture||x.isData3DTexture?T.texSubImage3D(be,F,Ee,lt,gt,ne,le,ve,un,tt,Et.data):N.isCompressedArrayTexture?T.compressedTexSubImage3D(be,F,Ee,lt,gt,ne,le,ve,un,Et.data):T.texSubImage3D(be,F,Ee,lt,gt,ne,le,ve,un,tt,Et),T.pixelStorei(T.UNPACK_ROW_LENGTH,Bt),T.pixelStorei(T.UNPACK_IMAGE_HEIGHT,nt),T.pixelStorei(T.UNPACK_SKIP_PIXELS,Dn),T.pixelStorei(T.UNPACK_SKIP_ROWS,Fr),T.pixelStorei(T.UNPACK_SKIP_IMAGES,dn),F===0&&N.generateMipmaps&&T.generateMipmap(be),Ae.unbindTexture()},this.initRenderTarget=function(x){Re.get(x).__webglFramebuffer===void 0&&b.setupRenderTarget(x)},this.initTexture=function(x){x.isCubeTexture?b.setTextureCube(x,0):x.isData3DTexture?b.setTexture3D(x,0):x.isDataArrayTexture||x.isCompressedArrayTexture?b.setTexture2DArray(x,0):b.setTexture2D(x,0),Ae.unbindTexture()},this.resetState=function(){k=0,I=0,C=null,Ae.reset(),ht.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return yi}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=e===tp?"display-p3":"srgb",t.unpackColorSpace=st.workingColorSpace===vc?"display-p3":"srgb"}};var dc=class extends bi{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new Ar,this.environmentIntensity=1,this.environmentRotation=new Ar,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}};var To=class extends Tr{constructor(e){super(),this.isMeshStandardMaterial=!0,this.defines={STANDARD:""},this.type="MeshStandardMaterial",this.color=new Ye(16777215),this.roughness=1,this.metalness=0,this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.emissive=new Ye(0),this.emissiveIntensity=1,this.emissiveMap=null,this.bumpMap=null,this.bumpScale=1,this.normalMap=null,this.normalMapType=N0,this.normalScale=new ot(1,1),this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.roughnessMap=null,this.metalnessMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new Ar,this.envMapIntensity=1,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.flatShading=!1,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.defines={STANDARD:""},this.color.copy(e.color),this.roughness=e.roughness,this.metalness=e.metalness,this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.emissive.copy(e.emissive),this.emissiveMap=e.emissiveMap,this.emissiveIntensity=e.emissiveIntensity,this.bumpMap=e.bumpMap,this.bumpScale=e.bumpScale,this.normalMap=e.normalMap,this.normalMapType=e.normalMapType,this.normalScale.copy(e.normalScale),this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.roughnessMap=e.roughnessMap,this.metalnessMap=e.metalnessMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.envMapIntensity=e.envMapIntensity,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.flatShading=e.flatShading,this.fog=e.fog,this}};function Vl(n,e,t){return!n||!t&&n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}function f1(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}var Hs=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let l=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===l)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},Uf=class extends Hs{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:Cy,endingEnd:Cy}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],l=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case Dy:s=e,a=2*t-i;break;case Ay:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(l===void 0)switch(this.getSettings_().endingEnd){case Dy:o=e,l=2*i-t;break;case Ay:o=1,l=i+r[1]-r[0];break;default:o=e-1,l=t}let c=(i-t)*.5,u=this.valueSize;this._weightPrev=c/(t-a),this._weightNext=c/(l-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,l=e*a,c=l-a,u=this._offsetPrev,d=this._offsetNext,h=this._weightPrev,f=this._weightNext,g=(i-t)/(r-t),_=g*g,m=_*g,p=-h*m+2*h*_-h*g,w=(1+h)*m+(-1.5-2*h)*_+(-.5+h)*g+1,E=(-1-f)*m+(1.5+f)*_+.5*g,S=f*m-f*_;for(let k=0;k!==a;++k)s[k]=p*o[u+k]+w*o[c+k]+E*o[l+k]+S*o[d+k];return s}},Vf=class extends Hs{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,l=e*a,c=l-a,u=(i-t)/(r-t),d=1-u;for(let h=0;h!==a;++h)s[h]=o[c+h]*d+o[l+h]*u;return s}},Bf=class extends Hs{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},Vn=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=Vl(t,this.TimeBufferType),this.values=Vl(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:Vl(e.times,Array),values:Vl(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new Bf(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new Vf(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new Uf(this.times,this.values,this.getValueSize(),e)}setInterpolation(e){let t;switch(e){case $l:t=this.InterpolantFactoryMethodDiscrete;break;case _f:t=this.InterpolantFactoryMethodLinear;break;case nh:t=this.InterpolantFactoryMethodSmooth;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return console.warn("THREE.KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return $l;case this.InterpolantFactoryMethodLinear:return _f;case this.InterpolantFactoryMethodSmooth:return nh}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(console.error("THREE.KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(console.error("THREE.KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let l=i[a];if(typeof l=="number"&&isNaN(l)){console.error("THREE.KeyframeTrack: Time is not a valid number.",this,a,l),e=!1;break}if(o!==null&&o>l){console.error("THREE.KeyframeTrack: Out of order keys.",this,a,l,o),e=!1;break}o=l}if(r!==void 0&&f1(r))for(let a=0,l=r.length;a!==l;++a){let c=r[a];if(isNaN(c)){console.error("THREE.KeyframeTrack: Value is not a valid number.",this,a,c),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===nh,s=e.length-1,o=1;for(let a=1;a<s;++a){let l=!1,c=e[a],u=e[a+1];if(c!==u&&(a!==1||c!==e[0]))if(r)l=!0;else{let d=a*i,h=d-i,f=d+i;for(let g=0;g!==i;++g){let _=t[d+g];if(_!==t[h+g]||_!==t[f+g]){l=!0;break}}}if(l){if(a!==o){e[o]=e[a];let d=a*i,h=o*i;for(let f=0;f!==i;++f)t[h+f]=t[d+f]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,l=o*i,c=0;c!==i;++c)t[l+c]=t[a+c];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};Vn.prototype.TimeBufferType=Float32Array;Vn.prototype.ValueBufferType=Float32Array;Vn.prototype.DefaultInterpolation=_f;var Rr=class extends Vn{constructor(e,t,i){super(e,t,i)}};Rr.prototype.ValueTypeName="bool";Rr.prototype.ValueBufferType=Array;Rr.prototype.DefaultInterpolation=$l;Rr.prototype.InterpolantFactoryMethodLinear=void 0;Rr.prototype.InterpolantFactoryMethodSmooth=void 0;var Hf=class extends Vn{};Hf.prototype.ValueTypeName="color";var zf=class extends Vn{};zf.prototype.ValueTypeName="number";var Gf=class extends Hs{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,l=(i-t)/(r-t),c=e*a;for(let u=c+a;c!==u;c+=4)qi.slerpFlat(s,0,o,c-a,o,c,l);return s}},hc=class extends Vn{InterpolantFactoryMethodLinear(e){return new Gf(this.times,this.values,this.getValueSize(),e)}};hc.prototype.ValueTypeName="quaternion";hc.prototype.InterpolantFactoryMethodSmooth=void 0;var Nr=class extends Vn{constructor(e,t,i){super(e,t,i)}};Nr.prototype.ValueTypeName="string";Nr.prototype.ValueBufferType=Array;Nr.prototype.DefaultInterpolation=$l;Nr.prototype.InterpolantFactoryMethodLinear=void 0;Nr.prototype.InterpolantFactoryMethodSmooth=void 0;var Wf=class extends Vn{};Wf.prototype.ValueTypeName="vector";var fc=class extends bi{constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new Ye(e),this.intensity=t}dispose(){}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,this.groundColor!==void 0&&(t.object.groundColor=this.groundColor.getHex()),this.distance!==void 0&&(t.object.distance=this.distance),this.angle!==void 0&&(t.object.angle=this.angle),this.decay!==void 0&&(t.object.decay=this.decay),this.penumbra!==void 0&&(t.object.penumbra=this.penumbra),this.shadow!==void 0&&(t.object.shadow=this.shadow.toJSON()),this.target!==void 0&&(t.object.target=this.target.uuid),t}};var Nh=new It,y0=new B,_0=new B,jf=class{constructor(e){this.camera=e,this.intensity=1,this.bias=0,this.normalBias=0,this.radius=1,this.blurSamples=8,this.mapSize=new ot(512,512),this.map=null,this.mapPass=null,this.matrix=new It,this.autoUpdate=!0,this.needsUpdate=!1,this._frustum=new Ao,this._frameExtents=new ot(1,1),this._viewportCount=1,this._viewports=[new wt(0,0,1,1)]}getViewportCount(){return this._viewportCount}getFrustum(){return this._frustum}updateMatrices(e){let t=this.camera,i=this.matrix;y0.setFromMatrixPosition(e.matrixWorld),t.position.copy(y0),_0.setFromMatrixPosition(e.target.matrixWorld),t.lookAt(_0),t.updateMatrixWorld(),Nh.multiplyMatrices(t.projectionMatrix,t.matrixWorldInverse),this._frustum.setFromProjectionMatrix(Nh),i.set(.5,0,0,.5,0,.5,0,.5,0,0,.5,.5,0,0,0,1),i.multiply(Nh)}getViewport(e){return this._viewports[e]}getFrameExtents(){return this._frameExtents}dispose(){this.map&&this.map.dispose(),this.mapPass&&this.mapPass.dispose()}copy(e){return this.camera=e.camera.clone(),this.intensity=e.intensity,this.bias=e.bias,this.radius=e.radius,this.mapSize.copy(e.mapSize),this}clone(){return new this.constructor().copy(this)}toJSON(){let e={};return this.intensity!==1&&(e.intensity=this.intensity),this.bias!==0&&(e.bias=this.bias),this.normalBias!==0&&(e.normalBias=this.normalBias),this.radius!==1&&(e.radius=this.radius),(this.mapSize.x!==512||this.mapSize.y!==512)&&(e.mapSize=this.mapSize.toArray()),e.camera=this.camera.toJSON(!1).object,delete e.camera.matrix,e}};var $f=class extends jf{constructor(){super(new ac(-5,5,5,-5,.5,500)),this.isDirectionalLightShadow=!0}},pc=class extends fc{constructor(e,t){super(e,t),this.isDirectionalLight=!0,this.type="DirectionalLight",this.position.copy(bi.DEFAULT_UP),this.updateMatrix(),this.target=new bi,this.shadow=new $f}dispose(){this.shadow.dispose()}copy(e){return super.copy(e),this.target=e.target.clone(),this.shadow=e.shadow.clone(),this}},mc=class extends fc{constructor(e,t){super(e,t),this.isAmbientLight=!0,this.type="AmbientLight"}};var ip="\\[\\]\\.:\\/",p1=new RegExp("["+ip+"]","g"),rp="[^"+ip+"]",m1="[^"+ip.replace("\\.","")+"]",g1=/((?:WC+[\/:])*)/.source.replace("WC",rp),v1=/(WCOD+)?/.source.replace("WCOD",m1),y1=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",rp),_1=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",rp),x1=new RegExp("^"+g1+v1+y1+_1+"$"),M1=["material","materials","bones","map"],qf=class{constructor(e,t,i){let r=i||Tt.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},Tt=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(p1,"")}static parseTrackName(t){let i=x1.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);M1.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let l=o[a];if(l.name===i||l.uuid===i)return l;let c=r(l.children);if(c)return c}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){console.warn("THREE.PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){console.error("THREE.PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){console.error("THREE.PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){console.error("THREE.PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let d=0;d<t.length;d++)if(t[d].name===u){u=d;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){console.error("THREE.PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){console.error("THREE.PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){console.error("THREE.PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){console.error("THREE.PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;console.error("THREE.PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let l=this.Versioning.None;this.targetObject=t,t.needsUpdate!==void 0?l=this.Versioning.NeedsUpdate:t.matrixWorldNeedsUpdate!==void 0&&(l=this.Versioning.MatrixWorldNeedsUpdate);let c=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){console.error("THREE.PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){console.error("THREE.PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}c=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(c=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(c=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[c],this.setValue=this.SetterByBindingTypeAndVersioning[c][l]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=qf,n})();Tt.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};Tt.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};Tt.prototype.GetterByBindingType=[Tt.prototype._getValue_direct,Tt.prototype._getValue_array,Tt.prototype._getValue_arrayElement,Tt.prototype._getValue_toArray];Tt.prototype.SetterByBindingTypeAndVersioning=[[Tt.prototype._setValue_direct,Tt.prototype._setValue_direct_setNeedsUpdate,Tt.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[Tt.prototype._setValue_array,Tt.prototype._setValue_array_setNeedsUpdate,Tt.prototype._setValue_array_setMatrixWorldNeedsUpdate],[Tt.prototype._setValue_arrayElement,Tt.prototype._setValue_arrayElement_setNeedsUpdate,Tt.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[Tt.prototype._setValue_fromArray,Tt.prototype._setValue_fromArray_setNeedsUpdate,Tt.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var XO=new Float32Array(1);typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:Xf}}));typeof window<"u"&&(window.__THREE__?console.warn("WARNING: Multiple instances of Three.js being imported."):window.__THREE__=Xf);var b1=["c"],_c=class n{constructor(e){this.zone=e;this.stop=()=>{}}ngAfterViewInit(){this.zone.runOutsideAngular(()=>this.init())}ngOnDestroy(){this.stop()}init(){let e=this.c.nativeElement,t=new uc({canvas:e,alpha:!0,antialias:!0});t.setPixelRatio(Math.min(devicePixelRatio,2));let i=new dc,r=new Jt(45,1,.1,100);r.position.z=14,i.add(new mc(16777215,1.2));let s=new pc(16777215,2.4);s.position.set(5,8,6),i.add(s);let o=new _i;i.add(o);let a=[16742938,16765601,2241643].map(E=>new To({color:E,roughness:.45,metalness:.1})),l=new Xi(1,1,1),c=new Xi(1.02,.12,1.02),u=new To({color:16115400,roughness:.6}),d=[];for(let E=0;E<18;E++){let S=new _i;S.add(new an(l,a[E%3]),new an(c,u)),S.scale.setScalar(.6+Math.random()*1.1),S.position.set((Math.random()-.5)*18,(Math.random()-.5)*9,(Math.random()-.5)*6-1),S.rotation.set(Math.random()*3,Math.random()*3,0),o.add(S),d.push({m:S,v:.1+Math.random()*.3,p:Math.random()*6})}let h={x:0,y:0},f=E=>{h.x=E.clientX/innerWidth-.5,h.y=E.clientY/innerHeight-.5},g=()=>{let E=e.parentElement;t.setSize(E.clientWidth,E.clientHeight,!1),r.aspect=E.clientWidth/E.clientHeight,r.updateProjectionMatrix()};addEventListener("mousemove",f),addEventListener("resize",g),g();let _=matchMedia("(prefers-reduced-motion: reduce)").matches,m=0,p=performance.now(),w=()=>{let E=(performance.now()-p)/1e3;d.forEach(S=>{S.m.rotation.x+=S.v*.01,S.m.rotation.y+=S.v*.012,S.m.position.y+=Math.sin(E+S.p)*.003}),o.rotation.y=scrollY*.0012,o.position.y=scrollY*.004,r.position.x+=(h.x*3-r.position.x)*.05,r.position.y+=(-h.y*2-r.position.y)*.05,r.lookAt(0,0,0),t.render(i,r),_||(m=requestAnimationFrame(w))};w(),this.stop=()=>{cancelAnimationFrame(m),removeEventListener("mousemove",f),removeEventListener("resize",g),t.dispose(),l.dispose()}}static{this.\u0275fac=function(t){return new(t||n)(Mt(St))}}static{this.\u0275cmp=os({type:n,selectors:[["app-hero-3d"]],viewQuery:function(t,i){if(t&1&&Qv(b1,7),t&2){let r;ey(r=ty())&&(i.c=r.first)}},standalone:!0,features:[fs],decls:2,vars:0,consts:[["c",""]],template:function(t,i){t&1&&pn(0,"canvas",null,0)},styles:["[_nghost-%COMP%]{position:absolute;inset:0;display:block}canvas[_ngcontent-%COMP%]{width:100%;height:100%;display:block}"]})}};var Z0=(()=>{class n{constructor(t,i){this._renderer=t,this._elementRef=i,this.onChange=r=>{},this.onTouched=()=>{}}setProperty(t,i){this._renderer.setProperty(this._elementRef.nativeElement,t,i)}registerOnTouched(t){this.onTouched=t}registerOnChange(t){this.onChange=t}setDisabledState(t){this.setProperty("disabled",t)}static{this.\u0275fac=function(i){return new(i||n)(Mt(cl),Mt(bn))}}static{this.\u0275dir=Zt({type:n})}}return n})(),S1=(()=>{class n extends Z0{static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=cs(n)))(r||n)}})()}static{this.\u0275dir=Zt({type:n,features:[Xn]})}}return n})(),K0=new He("");var w1={provide:K0,useExisting:ri(()=>Sc),multi:!0};function C1(){let n=ms()?ms().getUserAgent():"";return/android (\d+)/.test(n.toLowerCase())}var D1=new He(""),Sc=(()=>{class n extends Z0{constructor(t,i,r){super(t,i),this._compositionMode=r,this._composing=!1,this._compositionMode==null&&(this._compositionMode=!C1())}writeValue(t){let i=t??"";this.setProperty("value",i)}_handleInput(t){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(t)}_compositionStart(){this._composing=!0}_compositionEnd(t){this._composing=!1,this._compositionMode&&this.onChange(t)}static{this.\u0275fac=function(i){return new(i||n)(Mt(cl),Mt(bn),Mt(D1,8))}}static{this.\u0275dir=Zt({type:n,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&et("input",function(o){return r._handleInput(o.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(o){return r._compositionEnd(o.target.value)})},features:[fr([w1]),Xn]})}}return n})();function up(n){return n==null||(typeof n=="string"||Array.isArray(n))&&n.length===0}function J0(n){return n!=null&&typeof n.length=="number"}var ko=new He(""),A1=new He("");function T1(n){return up(n.value)?{required:!0}:null}function I1(n){return e=>up(e.value)||!J0(e.value)?null:e.value.length<n?{minlength:{requiredLength:n,actualLength:e.value.length}}:null}function R1(n){return e=>J0(e.value)&&e.value.length>n?{maxlength:{requiredLength:n,actualLength:e.value.length}}:null}function N1(n){if(!n)return sp;let e,t;return typeof n=="string"?(t="",n.charAt(0)!=="^"&&(t+="^"),t+=n,n.charAt(n.length-1)!=="$"&&(t+="$"),e=new RegExp(t)):(t=n.toString(),e=n),i=>{if(up(i.value))return null;let r=i.value;return e.test(r)?null:{pattern:{requiredPattern:t,actualValue:r}}}}function sp(n){return null}function Q0(n){return n!=null}function e_(n){return fo(n)?Yc(n):n}function t_(n){let e={};return n.forEach(t=>{e=t!=null?vt(vt({},e),t):e}),Object.keys(e).length===0?null:e}function n_(n,e){return e.map(t=>t(n))}function P1(n){return!n.validate}function i_(n){return n.map(e=>P1(e)?e:t=>e.validate(t))}function F1(n){if(!n)return null;let e=n.filter(Q0);return e.length==0?null:function(t){return t_(n_(t,e))}}function r_(n){return n!=null?F1(i_(n)):null}function O1(n){if(!n)return null;let e=n.filter(Q0);return e.length==0?null:function(t){let i=n_(t,e).map(e_);return Zc(i).pipe(tr(t_))}}function s_(n){return n!=null?O1(i_(n)):null}function G0(n,e){return n===null?[e]:Array.isArray(n)?[...n,e]:[n,e]}function L1(n){return n._rawValidators}function k1(n){return n._rawAsyncValidators}function op(n){return n?Array.isArray(n)?n:[n]:[]}function Mc(n,e){return Array.isArray(n)?n.includes(e):n===e}function W0(n,e){let t=op(e);return op(n).forEach(r=>{Mc(t,r)||t.push(r)}),t}function j0(n,e){return op(e).filter(t=>!Mc(n,t))}var Ec=class{constructor(){this._rawValidators=[],this._rawAsyncValidators=[],this._onDestroyCallbacks=[]}get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_setValidators(e){this._rawValidators=e||[],this._composedValidatorFn=r_(this._rawValidators)}_setAsyncValidators(e){this._rawAsyncValidators=e||[],this._composedAsyncValidatorFn=s_(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_registerOnDestroy(e){this._onDestroyCallbacks.push(e)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(e=>e()),this._onDestroyCallbacks=[]}reset(e=void 0){this.control&&this.control.reset(e)}hasError(e,t){return this.control?this.control.hasError(e,t):!1}getError(e,t){return this.control?this.control.getError(e,t):null}},ap=class extends Ec{get formDirective(){return null}get path(){return null}},Lo=class extends Ec{constructor(){super(...arguments),this._parent=null,this.name=null,this.valueAccessor=null}},lp=class{constructor(e){this._cd=e}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}},U1={"[class.ng-untouched]":"isUntouched","[class.ng-touched]":"isTouched","[class.ng-pristine]":"isPristine","[class.ng-dirty]":"isDirty","[class.ng-valid]":"isValid","[class.ng-invalid]":"isInvalid","[class.ng-pending]":"isPending"},h2=bt(vt({},U1),{"[class.ng-submitted]":"isSubmitted"}),o_=(()=>{class n extends lp{constructor(t){super(t)}static{this.\u0275fac=function(i){return new(i||n)(Mt(Lo,2))}}static{this.\u0275dir=Zt({type:n,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&Pi("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},features:[Xn]})}}return n})();var No="VALID",xc="INVALID",Gs="PENDING",Po="DISABLED",js=class{},bc=class extends js{constructor(e,t){super(),this.value=e,this.source=t}},Fo=class extends js{constructor(e,t){super(),this.pristine=e,this.source=t}},Oo=class extends js{constructor(e,t){super(),this.touched=e,this.source=t}},Ws=class extends js{constructor(e,t){super(),this.status=e,this.source=t}};function V1(n){return(wc(n)?n.validators:n)||null}function B1(n){return Array.isArray(n)?r_(n):n||null}function H1(n,e){return(wc(e)?e.asyncValidators:n)||null}function z1(n){return Array.isArray(n)?s_(n):n||null}function wc(n){return n!=null&&!Array.isArray(n)&&typeof n=="object"}var cp=class{constructor(e,t){this._pendingDirty=!1,this._hasOwnPendingAsyncValidator=null,this._pendingTouched=!1,this._onCollectionChange=()=>{},this._parent=null,this._status=fl(()=>this.statusReactive()),this.statusReactive=jt(void 0),this._pristine=fl(()=>this.pristineReactive()),this.pristineReactive=jt(!0),this._touched=fl(()=>this.touchedReactive()),this.touchedReactive=jt(!1),this._events=new Bn,this.events=this._events.asObservable(),this._onDisabledChange=[],this._assignValidators(e),this._assignAsyncValidators(t)}get validator(){return this._composedValidatorFn}set validator(e){this._rawValidators=this._composedValidatorFn=e}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(e){this._rawAsyncValidators=this._composedAsyncValidatorFn=e}get parent(){return this._parent}get status(){return Fi(this.statusReactive)}set status(e){Fi(()=>this.statusReactive.set(e))}get valid(){return this.status===No}get invalid(){return this.status===xc}get pending(){return this.status==Gs}get disabled(){return this.status===Po}get enabled(){return this.status!==Po}get pristine(){return Fi(this.pristineReactive)}set pristine(e){Fi(()=>this.pristineReactive.set(e))}get dirty(){return!this.pristine}get touched(){return Fi(this.touchedReactive)}set touched(e){Fi(()=>this.touchedReactive.set(e))}get untouched(){return!this.touched}get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(e){this._assignValidators(e)}setAsyncValidators(e){this._assignAsyncValidators(e)}addValidators(e){this.setValidators(W0(e,this._rawValidators))}addAsyncValidators(e){this.setAsyncValidators(W0(e,this._rawAsyncValidators))}removeValidators(e){this.setValidators(j0(e,this._rawValidators))}removeAsyncValidators(e){this.setAsyncValidators(j0(e,this._rawAsyncValidators))}hasValidator(e){return Mc(this._rawValidators,e)}hasAsyncValidator(e){return Mc(this._rawAsyncValidators,e)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(e={}){let t=this.touched===!1;this.touched=!0;let i=e.sourceControl??this;this._parent&&!e.onlySelf&&this._parent.markAsTouched(bt(vt({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new Oo(!0,i))}markAllAsTouched(e={}){this.markAsTouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsTouched(e))}markAsUntouched(e={}){let t=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:i})}),this._parent&&!e.onlySelf&&this._parent._updateTouched(e,i),t&&e.emitEvent!==!1&&this._events.next(new Oo(!1,i))}markAsDirty(e={}){let t=this.pristine===!0;this.pristine=!1;let i=e.sourceControl??this;this._parent&&!e.onlySelf&&this._parent.markAsDirty(bt(vt({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new Fo(!1,i))}markAsPristine(e={}){let t=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:e.emitEvent})}),this._parent&&!e.onlySelf&&this._parent._updatePristine(e,i),t&&e.emitEvent!==!1&&this._events.next(new Fo(!0,i))}markAsPending(e={}){this.status=Gs;let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new Ws(this.status,t)),this.statusChanges.emit(this.status)),this._parent&&!e.onlySelf&&this._parent.markAsPending(bt(vt({},e),{sourceControl:t}))}disable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=Po,this.errors=null,this._forEachChild(r=>{r.disable(bt(vt({},e),{onlySelf:!0}))}),this._updateValue();let i=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new bc(this.value,i)),this._events.next(new Ws(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(bt(vt({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=No,this._forEachChild(i=>{i.enable(bt(vt({},e),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent}),this._updateAncestors(bt(vt({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(e,t){this._parent&&!e.onlySelf&&(this._parent.updateValueAndValidity(e),e.skipPristineCheck||this._parent._updatePristine({},t),this._parent._updateTouched({},t))}setParent(e){this._parent=e}getRawValue(){return this.value}updateValueAndValidity(e={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===No||this.status===Gs)&&this._runAsyncValidator(i,e.emitEvent)}let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new bc(this.value,t)),this._events.next(new Ws(this.status,t)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._parent&&!e.onlySelf&&this._parent.updateValueAndValidity(bt(vt({},e),{sourceControl:t}))}_updateTreeValidity(e={emitEvent:!0}){this._forEachChild(t=>t._updateTreeValidity(e)),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Po:No}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(e,t){if(this.asyncValidator){this.status=Gs,this._hasOwnPendingAsyncValidator={emitEvent:t!==!1};let i=e_(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:t,shouldHaveEmitted:e})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let e=this._hasOwnPendingAsyncValidator?.emitEvent??!1;return this._hasOwnPendingAsyncValidator=null,e}return!1}setErrors(e,t={}){this.errors=e,this._updateControlsErrors(t.emitEvent!==!1,this,t.shouldHaveEmitted)}get(e){let t=e;return t==null||(Array.isArray(t)||(t=t.split(".")),t.length===0)?null:t.reduce((i,r)=>i&&i._find(r),this)}getError(e,t){let i=t?this.get(t):this;return i&&i.errors?i.errors[e]:null}hasError(e,t){return!!this.getError(e,t)}get root(){let e=this;for(;e._parent;)e=e._parent;return e}_updateControlsErrors(e,t,i){this.status=this._calculateStatus(),e&&this.statusChanges.emit(this.status),(e||i)&&this._events.next(new Ws(this.status,t)),this._parent&&this._parent._updateControlsErrors(e,t,i)}_initObservables(){this.valueChanges=new tn,this.statusChanges=new tn}_calculateStatus(){return this._allControlsDisabled()?Po:this.errors?xc:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Gs)?Gs:this._anyControlsHaveStatus(xc)?xc:No}_anyControlsHaveStatus(e){return this._anyControls(t=>t.status===e)}_anyControlsDirty(){return this._anyControls(e=>e.dirty)}_anyControlsTouched(){return this._anyControls(e=>e.touched)}_updatePristine(e,t){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,this._parent&&!e.onlySelf&&this._parent._updatePristine(e,t),r&&this._events.next(new Fo(this.pristine,t))}_updateTouched(e={},t){this.touched=this._anyControlsTouched(),this._events.next(new Oo(this.touched,t)),this._parent&&!e.onlySelf&&this._parent._updateTouched(e,t)}_registerOnCollectionChange(e){this._onCollectionChange=e}_setUpdateStrategy(e){wc(e)&&e.updateOn!=null&&(this._updateOn=e.updateOn)}_parentMarkedDirty(e){let t=this._parent&&this._parent.dirty;return!e&&!!t&&!this._parent._anyControlsDirty()}_find(e){return null}_assignValidators(e){this._rawValidators=Array.isArray(e)?e.slice():e,this._composedValidatorFn=B1(this._rawValidators)}_assignAsyncValidators(e){this._rawAsyncValidators=Array.isArray(e)?e.slice():e,this._composedAsyncValidatorFn=z1(this._rawAsyncValidators)}};var a_=new He("CallSetDisabledState",{providedIn:"root",factory:()=>dp}),dp="always";function G1(n,e){return[...e.path,n]}function W1(n,e,t=dp){$1(n,e),e.valueAccessor.writeValue(n.value),(n.disabled||t==="always")&&e.valueAccessor.setDisabledState?.(n.disabled),q1(n,e),Y1(n,e),X1(n,e),j1(n,e)}function $0(n,e){n.forEach(t=>{t.registerOnValidatorChange&&t.registerOnValidatorChange(e)})}function j1(n,e){if(e.valueAccessor.setDisabledState){let t=i=>{e.valueAccessor.setDisabledState(i)};n.registerOnDisabledChange(t),e._registerOnDestroy(()=>{n._unregisterOnDisabledChange(t)})}}function $1(n,e){let t=L1(n);e.validator!==null?n.setValidators(G0(t,e.validator)):typeof t=="function"&&n.setValidators([t]);let i=k1(n);e.asyncValidator!==null?n.setAsyncValidators(G0(i,e.asyncValidator)):typeof i=="function"&&n.setAsyncValidators([i]);let r=()=>n.updateValueAndValidity();$0(e._rawValidators,r),$0(e._rawAsyncValidators,r)}function q1(n,e){e.valueAccessor.registerOnChange(t=>{n._pendingValue=t,n._pendingChange=!0,n._pendingDirty=!0,n.updateOn==="change"&&l_(n,e)})}function X1(n,e){e.valueAccessor.registerOnTouched(()=>{n._pendingTouched=!0,n.updateOn==="blur"&&n._pendingChange&&l_(n,e),n.updateOn!=="submit"&&n.markAsTouched()})}function l_(n,e){n._pendingDirty&&n.markAsDirty(),n.setValue(n._pendingValue,{emitModelToViewChange:!1}),e.viewToModelUpdate(n._pendingValue),n._pendingChange=!1}function Y1(n,e){let t=(i,r)=>{e.valueAccessor.writeValue(i),r&&e.viewToModelUpdate(i)};n.registerOnChange(t),e._registerOnDestroy(()=>{n._unregisterOnChange(t)})}function Z1(n,e){if(!n.hasOwnProperty("model"))return!1;let t=n.model;return t.isFirstChange()?!0:!Object.is(e,t.currentValue)}function K1(n){return Object.getPrototypeOf(n.constructor)===S1}function J1(n,e){if(!e)return null;Array.isArray(e);let t,i,r;return e.forEach(s=>{s.constructor===Sc?t=s:K1(s)?i=s:r=s}),r||i||t||null}function q0(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function X0(n){return typeof n=="object"&&n!==null&&Object.keys(n).length===2&&"value"in n&&"disabled"in n}var Q1=class extends cp{constructor(e=null,t,i){super(V1(t),H1(i,t)),this.defaultValue=null,this._onChange=[],this._pendingChange=!1,this._applyFormState(e),this._setUpdateStrategy(t),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),wc(t)&&(t.nonNullable||t.initialValueIsDefault)&&(X0(e)?this.defaultValue=e.value:this.defaultValue=e)}setValue(e,t={}){this.value=this._pendingValue=e,this._onChange.length&&t.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,t.emitViewToModelChange!==!1)),this.updateValueAndValidity(t)}patchValue(e,t={}){this.setValue(e,t)}reset(e=this.defaultValue,t={}){this._applyFormState(e),this.markAsPristine(t),this.markAsUntouched(t),this.setValue(this.value,t),this._pendingChange=!1}_updateValue(){}_anyControls(e){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(e){this._onChange.push(e)}_unregisterOnChange(e){q0(this._onChange,e)}registerOnDisabledChange(e){this._onDisabledChange.push(e)}_unregisterOnDisabledChange(e){q0(this._onDisabledChange,e)}_forEachChild(e){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(e){X0(e)?(this.value=this._pendingValue=e.value,e.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=e}};var eR={provide:Lo,useExisting:ri(()=>hp)},Y0=Promise.resolve(),hp=(()=>{class n extends Lo{constructor(t,i,r,s,o,a){super(),this._changeDetectorRef=o,this.callSetDisabledState=a,this.control=new Q1,this._registered=!1,this.name="",this.update=new tn,this._parent=t,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=J1(this,s)}ngOnChanges(t){if(this._checkForErrors(),!this._registered||"name"in t){if(this._registered&&(this._checkName(),this.formDirective)){let i=t.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in t&&this._updateDisabled(t),Z1(t,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective&&this.formDirective.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(t){this.viewModel=t,this.update.emit(t)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){W1(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._isStandalone()||this._checkParentType(),this._checkName()}_checkParentType(){}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(t){Y0.then(()=>{this.control.setValue(t,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(t){let i=t.isDisabled.currentValue,r=i!==0&&hl(i);Y0.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(t){return this._parent?G1(t,this._parent):[t]}static{this.\u0275fac=function(i){return new(i||n)(Mt(ap,9),Mt(ko,10),Mt(A1,10),Mt(K0,10),Mt($d,8),Mt(a_,8))}}static{this.\u0275dir=Zt({type:n,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],features:[fr([eR]),Xn,Za]})}}return n})();function c_(n){return typeof n=="number"?n:parseInt(n,10)}var Cc=(()=>{class n{constructor(){this._validator=sp}ngOnChanges(t){if(this.inputName in t){let i=this.normalizeInput(t[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):sp,this._onChange&&this._onChange()}}validate(t){return this._validator(t)}registerOnValidatorChange(t){this._onChange=t}enabled(t){return t!=null}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275dir=Zt({type:n,features:[Za]})}}return n})();var tR={provide:ko,useExisting:ri(()=>fp),multi:!0};var fp=(()=>{class n extends Cc{constructor(){super(...arguments),this.inputName="required",this.normalizeInput=hl,this.createValidator=t=>T1}enabled(t){return t}static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=cs(n)))(r||n)}})()}static{this.\u0275dir=Zt({type:n,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&Pn("required",r._enabled?"":null)},inputs:{required:"required"},features:[fr([tR]),Xn]})}}return n})();var nR={provide:ko,useExisting:ri(()=>pp),multi:!0},pp=(()=>{class n extends Cc{constructor(){super(...arguments),this.inputName="minlength",this.normalizeInput=t=>c_(t),this.createValidator=t=>I1(t)}static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=cs(n)))(r||n)}})()}static{this.\u0275dir=Zt({type:n,selectors:[["","minlength","","formControlName",""],["","minlength","","formControl",""],["","minlength","","ngModel",""]],hostVars:1,hostBindings:function(i,r){i&2&&Pn("minlength",r._enabled?r.minlength:null)},inputs:{minlength:"minlength"},features:[fr([nR]),Xn]})}}return n})(),iR={provide:ko,useExisting:ri(()=>mp),multi:!0},mp=(()=>{class n extends Cc{constructor(){super(...arguments),this.inputName="maxlength",this.normalizeInput=t=>c_(t),this.createValidator=t=>R1(t)}static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=cs(n)))(r||n)}})()}static{this.\u0275dir=Zt({type:n,selectors:[["","maxlength","","formControlName",""],["","maxlength","","formControl",""],["","maxlength","","ngModel",""]],hostVars:1,hostBindings:function(i,r){i&2&&Pn("maxlength",r._enabled?r.maxlength:null)},inputs:{maxlength:"maxlength"},features:[fr([iR]),Xn]})}}return n})(),rR={provide:ko,useExisting:ri(()=>gp),multi:!0},gp=(()=>{class n extends Cc{constructor(){super(...arguments),this.inputName="pattern",this.normalizeInput=t=>t,this.createValidator=t=>N1(t)}static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=cs(n)))(r||n)}})()}static{this.\u0275dir=Zt({type:n,selectors:[["","pattern","","formControlName",""],["","pattern","","formControl",""],["","pattern","","ngModel",""]],hostVars:1,hostBindings:function(i,r){i&2&&Pn("pattern",r._enabled?r.pattern:null)},inputs:{pattern:"pattern"},features:[fr([rR]),Xn]})}}return n})();var sR=(()=>{class n{static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275mod=$a({type:n})}static{this.\u0275inj=Wa({})}}return n})();var u_=(()=>{class n{static withConfig(t){return{ngModule:n,providers:[{provide:a_,useValue:t.callSetDisabledState??dp}]}}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275mod=$a({type:n})}static{this.\u0275inj=Wa({imports:[sR]})}}return n})();var Dc=class n{constructor(e){this.el=e;this.delay=0}ngOnInit(){let e=this.el.nativeElement;e.classList.add("rv"),e.style.transitionDelay=this.delay+"ms",this.io=new IntersectionObserver(([t])=>{t.isIntersecting&&(e.classList.add("in"),this.io?.disconnect())},{threshold:.15}),this.io.observe(e)}ngOnDestroy(){this.io?.disconnect()}static{this.\u0275fac=function(t){return new(t||n)(Mt(bn))}}static{this.\u0275dir=Zt({type:n,selectors:[["","reveal",""]],inputs:{delay:"delay"},standalone:!0})}},Ac=class n{constructor(e,t){this.el=e;this.zone=t;this.speed=.1;this.fn=()=>{let e=this.el.nativeElement,t=e.getBoundingClientRect();e.style.translate=`0 ${(t.top+t.height/2-innerHeight/2)*this.speed}px`}}ngOnInit(){matchMedia("(prefers-reduced-motion: reduce)").matches||(this.zone.runOutsideAngular(()=>addEventListener("scroll",this.fn,{passive:!0})),this.fn())}ngOnDestroy(){removeEventListener("scroll",this.fn)}static{this.\u0275fac=function(t){return new(t||n)(Mt(bn),Mt(St))}}static{this.\u0275dir=Zt({type:n,selectors:[["","parallax",""]],inputs:{speed:[0,"parallax","speed"]},standalone:!0})}},$s=class n{constructor(e){this.el=e}move(e){let t=this.el.nativeElement,i=t.getBoundingClientRect(),r=(e.clientX-i.left)/i.width-.5,s=(e.clientY-i.top)/i.height-.5;t.style.transform=`perspective(800px) rotateX(${-s*14}deg) rotateY(${r*14}deg) scale(1.03)`}leave(){this.el.nativeElement.style.transform=""}static{this.\u0275fac=function(t){return new(t||n)(Mt(bn))}}static{this.\u0275dir=Zt({type:n,selectors:[["","tilt",""]],hostAttrs:[1,"tilt"],hostBindings:function(t,i){t&1&&et("mousemove",function(s){return i.move(s)})("mouseleave",function(){return i.leave()})},standalone:!0})}};var aR=(n,e)=>e.l,d_=(n,e)=>e.id;function lR(n,e){if(n&1){let t=rn();A(0,"button",12),et("click",function(){let r=ut(t).$implicit,s=Oe(2);return dt(s.selectService(r.l))}),A(1,"span",13),P(2),D(),P(3),D()}if(n&2){let t=e.$implicit,i=Oe(2);Pi("on",i.service()===t.l),Pn("aria-pressed",i.service()===t.l),ie(2),_t(t.i),ie(),_t(t.l)}}function cR(n,e){if(n&1&&(A(0,"h2"),P(1,"Welche Art von Umzug planen Sie?"),D(),A(2,"p"),P(3,"W\xE4hlen Sie die passende Leistung f\xFCr Ihre Anfrage."),D(),A(4,"div",10),hn(5,lR,4,5,"button",11,aR),D()),n&2){let t=Oe();ie(5),fn(t.services)}}function uR(n,e){n&1&&(A(0,"small",18),P(1,"Bitte geben Sie eine f\xFCnfstellige deutsche Postleitzahl ein."),D())}function dR(n,e){n&1&&(A(0,"small",18),P(1,"Bitte geben Sie einen Ort ein."),D())}function hR(n,e){n&1&&(A(0,"small",18),P(1,"Bitte geben Sie Stra\xDFe und Hausnummer ein."),D())}function fR(n,e){if(n&1){let t=rn();A(0,"button",28),et("click",function(){let r=ut(t).$implicit,s=Oe(3);return dt(s.selectAddress("origin",r))}),A(1,"strong"),P(2),D(),A(3,"span"),P(4),D()()}if(n&2){let t=e.$implicit;ie(2),_t(t.street),ie(2),_t(t.postalCity)}}function pR(n,e){if(n&1&&(A(0,"div",24),hn(1,fR,5,2,"button",27,d_),D()),n&2){let t=Oe(2);ie(),fn(t.originSuggestions())}}function mR(n,e){if(n&1){let t=rn();A(0,"h2"),P(1,"Wo startet Ihr Umzug?"),D(),A(2,"div",14)(3,"div",15)(4,"label",16),P(5,"Postleitzahl"),D(),A(6,"input",17,0),et("ngModelChange",function(r){ut(t);let s=Oe();return s.originZip=r,dt(s.saveDraft())}),D(),Ut(8,uR,2,0,"small",18),D(),A(9,"div",15)(10,"label",19),P(11,"Ort"),D(),A(12,"input",20,1),et("ngModelChange",function(r){ut(t);let s=Oe();return s.originCity=r,dt(s.saveDraft())}),D(),Ut(14,dR,2,0,"small",18),D(),A(15,"div",21)(16,"label",22),P(17,"Stra\xDFe und Hausnummer"),D(),A(18,"input",23,2),et("ngModelChange",function(r){ut(t);let s=Oe();return s.originAddress=r,s.searchAddress("origin",r),dt(s.saveDraft())}),D(),Ut(20,hR,2,0,"small",18)(21,pR,3,0,"div",24),A(22,"small",25),P(23,"Adressvorschl\xE4ge \xFCber Photon / "),A(24,"a",26),P(25,"\xA9 OpenStreetMap"),D(),P(26,". Manuelle Eingabe ist ebenfalls m\xF6glich."),D()()()}if(n&2){let t=hr(7),i=hr(13),r=hr(19),s=Oe();ie(6),pt("ngModel",s.originZip),ie(2),At(t.invalid&&(t.touched||t.dirty)?8:-1),ie(4),pt("ngModel",s.originCity),ie(2),At(i.invalid&&(i.touched||i.dirty)?14:-1),ie(4),pt("ngModel",s.originAddress),Pn("aria-expanded",s.originSuggestions().length>0),ie(2),At(r.invalid&&(r.touched||r.dirty)?20:-1),ie(),At(s.originSuggestions().length?21:-1)}}function gR(n,e){n&1&&(A(0,"small",18),P(1,"Bitte geben Sie eine f\xFCnfstellige deutsche Postleitzahl ein."),D())}function vR(n,e){n&1&&(A(0,"small",18),P(1,"Bitte geben Sie einen Ort ein."),D())}function yR(n,e){n&1&&(A(0,"small",18),P(1,"Bitte geben Sie Stra\xDFe und Hausnummer ein."),D())}function _R(n,e){if(n&1){let t=rn();A(0,"button",28),et("click",function(){let r=ut(t).$implicit,s=Oe(3);return dt(s.selectAddress("destination",r))}),A(1,"strong"),P(2),D(),A(3,"span"),P(4),D()()}if(n&2){let t=e.$implicit;ie(2),_t(t.street),ie(2),_t(t.postalCity)}}function xR(n,e){if(n&1&&(A(0,"div",35),hn(1,_R,5,2,"button",27,d_),D()),n&2){let t=Oe(2);ie(),fn(t.destinationSuggestions())}}function MR(n,e){if(n&1){let t=rn();A(0,"h2"),P(1,"Wohin geht Ihr Umzug?"),D(),A(2,"div",14)(3,"div",15)(4,"label",29),P(5,"Postleitzahl"),D(),A(6,"input",30,3),et("ngModelChange",function(r){ut(t);let s=Oe();return s.destZip=r,dt(s.saveDraft())}),D(),Ut(8,gR,2,0,"small",18),D(),A(9,"div",15)(10,"label",31),P(11,"Ort"),D(),A(12,"input",32,4),et("ngModelChange",function(r){ut(t);let s=Oe();return s.destCity=r,dt(s.saveDraft())}),D(),Ut(14,vR,2,0,"small",18),D(),A(15,"div",21)(16,"label",33),P(17,"Stra\xDFe und Hausnummer"),D(),A(18,"input",34,5),et("ngModelChange",function(r){ut(t);let s=Oe();return s.destAddress=r,s.searchAddress("destination",r),dt(s.saveDraft())}),D(),Ut(20,yR,2,0,"small",18)(21,xR,3,0,"div",35),A(22,"small",25),P(23,"Adressvorschl\xE4ge \xFCber Photon / "),A(24,"a",26),P(25,"\xA9 OpenStreetMap"),D(),P(26,". Manuelle Eingabe ist ebenfalls m\xF6glich."),D()()()}if(n&2){let t=hr(7),i=hr(13),r=hr(19),s=Oe();ie(6),pt("ngModel",s.destZip),ie(2),At(t.invalid&&(t.touched||t.dirty)?8:-1),ie(4),pt("ngModel",s.destCity),ie(2),At(i.invalid&&(i.touched||i.dirty)?14:-1),ie(4),pt("ngModel",s.destAddress),Pn("aria-expanded",s.destinationSuggestions().length>0),ie(2),At(r.invalid&&(r.touched||r.dirty)?20:-1),ie(),At(s.destinationSuggestions().length?21:-1)}}function ER(n,e){n&1&&(A(0,"small",18),P(1,"Der Wunschtermin darf nicht in der Vergangenheit liegen."),D())}function bR(n,e){n&1&&(A(0,"small",18),P(1,"Alternativtermine m\xFCssen in der Zukunft liegen und sich vom Wunschtermin unterscheiden."),D())}function SR(n,e){if(n&1){let t=rn();A(0,"div",41)(1,"div",15)(2,"label",42),P(3),D(),A(4,"input",43),et("ngModelChange",function(r){let s=ut(t).$index,o=Oe(2);return dt(o.updateAlternateDate(s,r))}),D()(),A(5,"button",44),et("click",function(){let r=ut(t).$index,s=Oe(2);return dt(s.removeAlternateDate(r))}),P(6,"Entfernen"),D()(),Ut(7,bR,2,0,"small",18)}if(n&2){let t=e.$implicit,i=e.$index,r=Oe(2);ie(2),pt("for","alternate-date-"+i),ie(),li("Alternative ",i+1,""),ie(),pt("id","alternate-date-"+i)("min",r.today)("ngModel",t),ie(),Pn("aria-label","Alternativtermin "+(i+1)+" entfernen"),ie(2),At(t&&(t<r.today||t===r.date)?7:-1)}}function wR(n,e){if(n&1){let t=rn();A(0,"button",45),et("click",function(){ut(t);let r=Oe(2);return dt(r.addAlternateDate())}),P(1,"\uFF0B Alternativtermin hinzuf\xFCgen"),D()}}function CR(n,e){if(n&1){let t=rn();A(0,"h2"),P(1,"Wann m\xF6chten Sie umziehen?"),D(),A(2,"div",15)(3,"label",36),P(4,"Wunschtermin"),D(),A(5,"input",37),dl("ngModelChange",function(r){ut(t);let s=Oe();return Wd(s.date,r)||(s.date=r),dt(r)}),et("ngModelChange",function(){ut(t);let r=Oe();return dt(r.saveDraft())}),D(),Ut(6,ER,2,0,"small",18),D(),A(7,"div",38)(8,"div",39)(9,"h3"),P(10,"Alternative Termine"),D(),A(11,"span"),P(12,"Optional, maximal 3"),D()(),hn(13,SR,8,7,null,null,Kv),Ut(15,wR,2,0,"button",40),D()}if(n&2){let t=Oe();ie(5),pt("min",t.today),ul("ngModel",t.date),ie(),At(t.date&&t.date<t.today?6:-1),ie(7),fn(t.alternateDates()),ie(2),At(t.alternateDates().length<3?15:-1)}}function DR(n,e){n&1&&(A(0,"small",18),P(1,"Bitte pr\xFCfen Sie Ihren Vornamen."),D())}function AR(n,e){n&1&&(A(0,"small",18),P(1,"Bitte pr\xFCfen Sie Ihren Nachnamen."),D())}function TR(n,e){n&1&&(A(0,"small",18),P(1,"Bitte geben Sie eine g\xFCltige Telefonnummer mit 7 bis 15 Ziffern ein."),D())}function IR(n,e){n&1&&(A(0,"small",18),P(1,"Bitte geben Sie eine g\xFCltige E-Mail-Adresse ein."),D())}function RR(n,e){if(n&1){let t=rn();A(0,"h2"),P(1,"Ihre Kontaktdaten"),D(),A(2,"div",46)(3,"div",15)(4,"label",47),P(5,"Vorname"),D(),A(6,"input",48),et("ngModelChange",function(r){ut(t);let s=Oe();return s.name=r,dt(s.saveDraft())}),D(),Ut(7,DR,2,0,"small",18),D(),A(8,"div",15)(9,"label",49),P(10,"Nachname"),D(),A(11,"input",50),et("ngModelChange",function(r){ut(t);let s=Oe();return s.surname=r,dt(s.saveDraft())}),D(),Ut(12,AR,2,0,"small",18),D(),A(13,"div",15)(14,"label",51),P(15,"Telefon"),D(),A(16,"input",52),et("ngModelChange",function(r){ut(t);let s=Oe();return s.phone=r,dt(s.saveDraft())}),D(),A(17,"small",25),P(18,"Zum Beispiel 015780945403 oder +49 157 80945403"),D(),Ut(19,TR,2,0,"small",18),D(),A(20,"div",15)(21,"label",53),P(22,"E-Mail"),D(),A(23,"input",54),et("ngModelChange",function(r){ut(t);let s=Oe();return s.email=r,dt(s.saveDraft())}),D(),Ut(24,IR,2,0,"small",18),D()(),A(25,"div",55)(26,"label"),P(27,"Dieses Feld bitte leer lassen"),A(28,"input",56),dl("ngModelChange",function(r){ut(t);let s=Oe();return Wd(s.website,r)||(s.website=r),dt(r)}),D()()(),A(29,"p",57),P(30,"Ihr Entwurf bleibt vor\xFCbergehend in diesem Browser-Tab gespeichert und wird nach erfolgreichem Versand gel\xF6scht. Wir verwenden Ihre Angaben zur Bearbeitung Ihrer Umzugsanfrage. Details finden Sie in unserer "),A(31,"a",58),P(32,"Datenschutzerkl\xE4rung"),D(),P(33,"."),D()}if(n&2){let t=Oe();ie(6),pt("ngModel",t.name),ie(),At(t.name&&!t.validName(t.name)?7:-1),ie(4),pt("ngModel",t.surname),ie(),At(t.surname&&!t.validName(t.surname)?12:-1),ie(4),pt("ngModel",t.phone),ie(3),At(t.phone&&!t.validPhone()?19:-1),ie(4),pt("ngModel",t.email),ie(),At(t.email&&!t.validEmail()?24:-1),ie(4),ul("ngModel",t.website)}}function NR(n,e){if(n&1&&(A(0,"h2"),P(1),D(),A(2,"p"),P(3,"Ihre Umzugsanfrage wurde erfolgreich an KAFI Transporte \xFCbermittelt. Wir melden uns pers\xF6nlich bei Ihnen."),D(),A(4,"p",59)(5,"strong"),P(6),D(),pn(7,"br"),P(8),pn(9,"br"),P(10),pn(11,"br"),P(12),D(),A(13,"div",9)(14,"a",60),P(15,"\u260E Anrufen"),D(),A(16,"a",61),P(17,"WhatsApp"),D()()),n&2){let t=Oe();ie(),li("Vielen Dank, ",t.name,"!"),ie(5),_t(t.service()),ie(2),Gd("",t.originCity," \u2192 ",t.destCity,""),ie(2),li("Wunschtermin: ",t.date,""),ie(2),li("Alternativen: ",t.alternateDates().join(", ")||"Keine","")}}function PR(n,e){if(n&1&&(A(0,"p",8),P(1),D()),n&2){let t=Oe();ie(),_t(t.submitError())}}function FR(n,e){if(n&1){let t=rn();A(0,"div",9)(1,"button",62),et("click",function(){ut(t);let r=Oe();return dt(r.goBack())}),P(2,"Zur\xFCck"),D(),A(3,"button",63),et("click",function(){ut(t);let r=Oe();return dt(r.advance())}),P(4),D()()}if(n&2){let t=Oe();ie(),pt("disabled",t.step()===0||t.sending()),ie(2),pt("disabled",!t.ok()||t.sending()),ie(),li(" ",t.sending()?"Wird gesendet \u2026":t.step()===4?"Kostenloses Angebot anfragen":"Weiter"," ")}}var Tc=class n{constructor(){this.draftKey="kafi-inquiry-draft-v1";this.services=[{i:"\u{1F3E0}",l:"Privatumzug"},{i:"\u{1F3E2}",l:"Firmenumzug"},{i:"\u{1F5FA}\uFE0F",l:"Fernumzug"},{i:"\u{1F6CB}\uFE0F",l:"Einzelner M\xF6beltransport"},{i:"\u{1F4E6}",l:"Sonstiges"}];this.step=jt(0);this.service=jt("");this.sending=jt(!1);this.submitError=jt("");this.originSuggestions=jt([]);this.destinationSuggestions=jt([]);this.alternateDates=jt([]);this.originZip="";this.originCity="";this.originAddress="";this.destZip="";this.destCity="";this.destAddress="";this.date="";this.altDate="";this.name="";this.surname="";this.phone="";this.email="";this.website="";this.Math=Math;this.today=this.getToday();this.restoreDraft()}getToday(){let e=new Date;return e.setMinutes(e.getMinutes()-e.getTimezoneOffset()),e.toISOString().slice(0,10)}restoreDraft(){try{let e=sessionStorage.getItem(this.draftKey);if(!e)return;let t=JSON.parse(e);if(t.version!==1)return;this.step.set(Number.isInteger(t.step)?Math.max(0,Math.min(4,t.step)):0),this.service.set(typeof t.service=="string"?t.service:""),this.originZip=typeof t.originZip=="string"?t.originZip:"",this.originCity=typeof t.originCity=="string"?t.originCity:"",this.originAddress=typeof t.originAddress=="string"?t.originAddress:"",this.destZip=typeof t.destZip=="string"?t.destZip:"",this.destCity=typeof t.destCity=="string"?t.destCity:"",this.destAddress=typeof t.destAddress=="string"?t.destAddress:"",this.date=typeof t.date=="string"?t.date:"",this.alternateDates.set(Array.isArray(t.alternateDates)?t.alternateDates.filter(i=>typeof i=="string").slice(0,3):[]),this.name=typeof t.name=="string"?t.name:"",this.surname=typeof t.surname=="string"?t.surname:"",this.phone=typeof t.phone=="string"?t.phone:"",this.email=typeof t.email=="string"?t.email:""}catch{this.clearDraft()}}saveDraft(){let e={version:1,step:this.step(),service:this.service(),originZip:this.originZip,originCity:this.originCity,originAddress:this.originAddress,destZip:this.destZip,destCity:this.destCity,destAddress:this.destAddress,date:this.date,alternateDates:this.alternateDates(),name:this.name,surname:this.surname,phone:this.phone,email:this.email};try{sessionStorage.setItem(this.draftKey,JSON.stringify(e))}catch{}}clearDraft(){try{sessionStorage.removeItem(this.draftKey)}catch{}}selectService(e){this.service.set(e),this.saveDraft()}goBack(){this.step()===0||this.sending()||(this.submitError.set(""),this.step.update(e=>e-1),this.saveDraft())}addAlternateDate(){this.alternateDates().length>=3||(this.alternateDates.update(e=>[...e,""]),this.saveDraft())}updateAlternateDate(e,t){this.alternateDates.update(i=>i.map((r,s)=>s===e?t:r)),this.saveDraft()}removeAlternateDate(e){this.alternateDates.update(t=>t.filter((i,r)=>r!==e)),this.saveDraft()}searchAddress(e,t){let i=e==="origin"?this.originSearchTimer:this.destinationSearchTimer,r=e==="origin"?this.originSearchController:this.destinationSearchController;i&&clearTimeout(i),r?.abort();let s=e==="origin"?this.originSuggestions:this.destinationSuggestions,o=t.trim();if(o.length<3){s.set([]);return}let a=setTimeout(()=>Ki(this,null,function*(){let l=new AbortController;e==="origin"?this.originSearchController=l:this.destinationSearchController=l;let c=new URLSearchParams({q:o,limit:"5",lang:"de",bbox:"5.8,47.2,15.1,55.1"});try{let u=yield fetch(`https://photon.komoot.io/api/?${c}`,{signal:l.signal});if(!u.ok)throw new Error("Address lookup failed");let h=((yield u.json()).features||[]).flatMap((f,g)=>{let _=f.properties||{},m=String(_.countrycode||"").toLowerCase();if(m&&m!=="de")return[];let p=[String(_.street||_.name||""),String(_.housenumber||"")].filter(Boolean).join(" "),w=String(_.postcode||""),E=String(_.city||_.locality||_.district||"");return!p||!w||!E?[]:[{id:`${f.geometry?.coordinates?.join(",")||p}-${g}`,street:p,postcode:w,city:E,postalCity:[w,E].filter(Boolean).join(" ")}]});l.signal.aborted||s.set(h)}catch{l.signal.aborted||s.set([])}}),350);e==="origin"?this.originSearchTimer=a:this.destinationSearchTimer=a}selectAddress(e,t){e==="origin"?(this.originAddress=t.street,this.originZip=t.postcode,this.originCity=t.city,this.originSuggestions.set([])):(this.destAddress=t.street,this.destZip=t.postcode,this.destCity=t.city,this.destinationSuggestions.set([])),this.saveDraft()}validZip(e){return/^\d{5}$/.test(e.trim())}validMoveDate(e){return/^\d{4}-\d{2}-\d{2}$/.test(e)&&e>=this.today}validDates(){let e=this.alternateDates();return this.validMoveDate(this.date)&&e.length<=3&&e.every(t=>this.validMoveDate(t)&&t!==this.date)&&new Set(e).size===e.length}validName(e){return/^[\p{L}\p{M}][\p{L}\p{M}'’ .-]{1,79}$/u.test(e.trim())}validEmail(){return/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(this.email.trim())}advance(){if(!(this.sending()||!this.ok())){if(this.step()===4){this.submit();return}this.submitError.set(""),this.step.update(e=>e+1),this.saveDraft()}}submit(){return Ki(this,null,function*(){let e=window.KAFI_CONFIG?.leadsApiUrl?.trim();if(!e){this.submitError.set("Das Anfrageformular ist noch nicht mit dem E-Mail-Dienst verbunden. Bitte kontaktieren Sie uns telefonisch oder per WhatsApp.");return}this.sending.set(!0),this.submitError.set("");try{if(!(yield fetch(e,{method:"POST",headers:{"Content-Type":"application/json"},cache:"no-store",body:JSON.stringify({service:this.service(),origin:{zip:this.originZip,city:this.originCity,address:this.originAddress},destination:{zip:this.destZip,city:this.destCity,address:this.destAddress},date:this.date,alternateDate:this.alternateDates().filter(Boolean).join(", "),alternateDates:this.alternateDates().filter(Boolean),contact:{firstName:this.name,lastName:this.surname,phone:this.phone,email:this.email},website:this.website})})).ok)throw new Error("Request was rejected");this.clearDraft(),this.step.set(5)}catch{this.submitError.set("Ihre Anfrage konnte gerade nicht gesendet werden. Bitte versuchen Sie es erneut oder kontaktieren Sie uns telefonisch unter +49 178 7410656.")}finally{this.sending.set(!1)}})}ok(){switch(this.step()){case 0:return!!this.service();case 1:return this.validAddress(this.originZip,this.originCity,this.originAddress);case 2:return this.validAddress(this.destZip,this.destCity,this.destAddress);case 3:return this.validDates();case 4:return this.validName(this.name)&&this.validName(this.surname)&&this.validPhone()&&this.validEmail();default:return!0}}validAddress(e,t,i){return this.validZip(e)&&t.trim().length>=2&&i.trim().length>=3}validPhone(){let e=this.phone.trim(),t=e.replace(/\D/g,"").length;return/^\+?[\d\s()./-]+$/.test(e)&&t>=7&&t<=15}static{this.\u0275fac=function(t){return new(t||n)}}static{this.\u0275cmp=os({type:n,selectors:[["app-estimate"]],standalone:!0,features:[fs],decls:13,vars:6,consts:[["originZipField","ngModel"],["originCityField","ngModel"],["originAddressField","ngModel"],["destZipField","ngModel"],["destCityField","ngModel"],["destAddressField","ngModel"],[1,"wizard-progress"],[1,"bar"],["role","alert",1,"form-error"],[1,"row"],[1,"opts"],["type","button","tilt","",1,"opt",3,"on"],["type","button","tilt","",1,"opt",3,"click"],[1,"ico"],[1,"field-grid"],[1,"field-group"],["for","origin-zip"],["id","origin-zip","inputmode","numeric","autocomplete","postal-code","placeholder","z. B. 12353","maxlength","5","pattern","[0-9]{5}","required","",1,"fld",3,"ngModelChange","ngModel"],[1,"field-error"],["for","origin-city"],["id","origin-city","autocomplete","address-level2","placeholder","Stadt oder Gemeinde","minlength","2","maxlength","80","required","",1,"fld",3,"ngModelChange","ngModel"],[1,"field-group","autocomplete-field"],["for","origin-address"],["id","origin-address","autocomplete","street-address","placeholder","Stra\xDFe und Hausnummer eingeben","minlength","3","maxlength","120","required","","role","combobox","aria-autocomplete","list","aria-controls","origin-address-suggestions",1,"fld",3,"ngModelChange","ngModel"],["id","origin-address-suggestions","role","listbox","aria-label","Adressvorschl\xE4ge",1,"address-suggestions"],[1,"form-hint"],["href","https://www.openstreetmap.org/copyright","target","_blank","rel","noreferrer"],["type","button","role","option",1,"address-suggestion"],["type","button","role","option",1,"address-suggestion",3,"click"],["for","destination-zip"],["id","destination-zip","inputmode","numeric","autocomplete","postal-code","placeholder","z. B. 20095","maxlength","5","pattern","[0-9]{5}","required","",1,"fld",3,"ngModelChange","ngModel"],["for","destination-city"],["id","destination-city","autocomplete","address-level2","placeholder","Stadt oder Gemeinde","minlength","2","maxlength","80","required","",1,"fld",3,"ngModelChange","ngModel"],["for","destination-address"],["id","destination-address","autocomplete","street-address","placeholder","Stra\xDFe und Hausnummer eingeben","minlength","3","maxlength","120","required","","role","combobox","aria-autocomplete","list","aria-controls","destination-address-suggestions",1,"fld",3,"ngModelChange","ngModel"],["id","destination-address-suggestions","role","listbox","aria-label","Adressvorschl\xE4ge",1,"address-suggestions"],["for","move-date"],["id","move-date","type","date","required","",1,"fld",3,"ngModelChange","min","ngModel"],[1,"alternate-dates"],[1,"alternate-heading"],["type","button",1,"add-date"],[1,"alternate-date-row"],[3,"for"],["type","date",1,"fld",3,"ngModelChange","id","min","ngModel"],["type","button",1,"remove-date",3,"click"],["type","button",1,"add-date",3,"click"],[1,"field-stack"],["for","first-name"],["id","first-name","autocomplete","given-name","placeholder","Vorname","minlength","2","maxlength","80","required","",1,"fld",3,"ngModelChange","ngModel"],["for","last-name"],["id","last-name","autocomplete","family-name","placeholder","Nachname","minlength","2","maxlength","80","required","",1,"fld",3,"ngModelChange","ngModel"],["for","contact-phone"],["id","contact-phone","type","tel","autocomplete","tel","placeholder","015780945403","maxlength","40","required","",1,"fld",3,"ngModelChange","ngModel"],["for","contact-email"],["id","contact-email","type","email","autocomplete","email","placeholder","name@beispiel.de","maxlength","254","required","",1,"fld",3,"ngModelChange","ngModel"],["aria-hidden","true",1,"form-trap"],["tabindex","-1","autocomplete","off",3,"ngModelChange","ngModel"],[1,"form-note"],["href","/datenschutz/","target","_blank","rel","noreferrer"],[1,"request-summary"],["href","tel:+491787410656",1,"btn","ghost","dark"],["href","https://wa.me/491787410656","target","_blank","rel","noreferrer",1,"btn","ghost","dark"],["type","button",1,"btn","ghost","dark",3,"click","disabled"],[1,"btn","solid",3,"click","disabled"]],template:function(t,i){if(t&1&&(A(0,"div",6)(1,"span"),P(2),D(),A(3,"div",7),pn(4,"i"),D()(),Ut(5,cR,7,0)(6,mR,27,8)(7,MR,27,8)(8,CR,16,4)(9,RR,34,9)(10,NR,18,6)(11,PR,2,1,"p",8)(12,FR,5,3,"div",9)),t&2){let r;ie(2),li("Schritt ",i.step()<5?i.step()+1:5," von 5"),ie(2),zd("width",(i.Math.min(i.step(),4)+1)*20,"%"),ie(),At((r=i.step())===0?5:r===1?6:r===2?7:r===3?8:r===4?9:r===5?10:-1),ie(6),At(i.submitError()?11:-1),ie(),At(i.step()<5?12:-1)}},dependencies:[u_,Sc,o_,fp,pp,mp,gp,hp,$s],encapsulation:2})}};var vp=(n,e)=>e.label,h_=(n,e)=>e.t,OR=(n,e)=>e.q,LR=(n,e)=>e.c;function kR(n,e){if(n&1&&(A(0,"a",2),P(1),D()),n&2){let t=e.$implicit;pt("href",t.href,tl),ie(),_t(t.label)}}function UR(n,e){if(n&1&&(A(0,"span")(1,"strong"),P(2),D(),A(3,"small"),P(4),D()()),n&2){let t=e.$implicit;ie(2),_t(t.label),ie(2),_t(t.value)}}function VR(n,e){if(n&1&&(A(0,"article",21)(1,"span",72),P(2),D(),A(3,"h3"),P(4),D(),A(5,"p"),P(6),D()()),n&2){let t=e.$implicit,i=e.$index;pt("delay",i*80),ie(2),_t(t.i),ie(2),_t(t.t),ie(2),_t(t.d)}}function BR(n,e){if(n&1&&(A(0,"article",47)(1,"span",72),P(2),D(),A(3,"h3"),P(4),D(),A(5,"p"),P(6),D()()),n&2){let t=e.$implicit,i=e.$index;pt("delay",i*100),ie(2),_t(t.i),ie(2),_t(t.t),ie(2),_t(t.d)}}function HR(n,e){if(n&1){let t=rn();A(0,"div",73)(1,"button",74),et("click",function(){let r=ut(t).$index,s=Oe();return dt(s.open.set(s.open()===r?-1:r))}),P(2),A(3,"i"),P(4,"+"),D()(),A(5,"div")(6,"p"),P(7),D()()()}if(n&2){let t=e.$implicit,i=e.$index,r=Oe();Pi("open",r.open()===i),ie(2),_t(t.q),ie(5),_t(t.a)}}function zR(n,e){if(n&1&&(A(0,"li")(1,"a",2),P(2),D()()),n&2){let t=e.$implicit;ie(),pt("href",t.href,tl),ie(),_t(t.label)}}function GR(n,e){if(n&1&&(A(0,"p"),P(1),pn(2,"br"),A(3,"a",2),P(4),D()()),n&2){let t=e.$implicit;ie(),_t(t.c),ie(2),pt("href","tel:"+t.n,tl),ie(),_t(t.n)}}function WR(n,e){if(n&1){let t=rn();A(0,"div",75),et("click",function(){ut(t);let r=Oe();return dt(r.modal.set(!1))}),A(1,"div",76),et("click",function(r){return ut(t),dt(r.stopPropagation())}),A(2,"button",77),et("click",function(){ut(t);let r=Oe();return dt(r.modal.set(!1))}),P(3,"\xD7"),D(),A(4,"h3"),P(5,"Jetzt anrufen"),D(),hn(6,GR,5,3,"p",null,LR),A(8,"a",78),P(9,"WhatsApp"),D()()()}if(n&2){let t=Oe();ie(6),fn(t.phones)}}var Ic=class n{constructor(){this.scrolled=jt(!1);this.modal=jt(!1);this.open=jt(-1);this.nav=[{label:"Leistungen",href:"#services"},{label:"Ablauf",href:"#steps"},{label:"Deutschlandweit",href:"#coverage"},{label:"Bewertungen",href:"#reviews"},{label:"FAQ",href:"#faqs"}];this.services=[{i:"\u{1F3E0}",t:"Privatumzug",d:"Wohnung, Haus oder WG \u2013 Unterst\xFCtzung beim Transport Ihres Hausstands."},{i:"\u{1F3E2}",t:"Firmenumzug",d:"Umz\xFCge von B\xFCros, Praxen oder Unternehmensstandorten."},{i:"\u{1F5FA}\uFE0F",t:"Fernumzug",d:"Deutschlandweite Umz\xFCge \xFCber gr\xF6\xDFere Entfernungen."},{i:"\u{1F6CB}\uFE0F",t:"M\xF6beltransport",d:"Transport einzelner M\xF6belst\xFCcke oder gr\xF6\xDFerer Ladungen."},{i:"\u{1F9F0}",t:"M\xF6belmontage",d:"Abbau und Aufbau von M\xF6beln, sofern angeboten."},{i:"\u{1F4E6}",t:"Verpackungsservice",d:"Unterst\xFCtzung bei der Vorbereitung und Verpackung, sofern angeboten."},{i:"\u{1F9F9}",t:"Entr\xFCmpelung",d:"Entr\xFCmpelungen und Haushaltsaufl\xF6sungen, sofern angeboten."},{i:"\u{1F6A7}",t:"Halteverbotszone",d:"Unterst\xFCtzung bei der Organisation einer Ladezone, sofern angeboten."}];this.trust=[{label:"Deutschlandweit",value:"f\xFCr Sie unterwegs"},{label:"MyHammer",value:"4,5 / 5"},{label:"Google",value:"4,6 / 5"},{label:"Pers\xF6nlicher Kontakt",value:"direkt mit KAFI"}];this.features=[{i:"\u{1F30D}",t:"Deutschlandweit",d:"Von Berlin in St\xE4dte und Regionen in ganz Deutschland."},{i:"\u{1F91D}",t:"Pers\xF6nlich",d:"Direkter Kontakt und individuelle Abstimmung."},{i:"\u{1F50E}",t:"Transparent",d:"Individuelle Angebote auf Basis Ihrer Umzugsdaten."},{i:"\u2699\uFE0F",t:"Flexibel",d:"Vom einzelnen M\xF6beltransport bis zum kompletten Umzug."}];this.faqs=[{q:"Wie weit f\xE4hrt KAFI Transporte?",a:"KAFI Transporte hat seinen Standort in Berlin und f\xFChrt Umz\xFCge deutschlandweit durch."},{q:"Wie fr\xFCh sollte ich meinen Umzug buchen?",a:"Das h\xE4ngt unter anderem von Route, Umfang und gew\xFCnschtem Termin ab. Eine fr\xFChzeitige Anfrage erleichtert die Planung."},{q:"Wie wird der Preis bestimmt?",a:"Der Preis wird nicht automatisch auf der Website berechnet. Nach Ihrer Anfrage pr\xFCft KAFI Transporte die Angaben und erstellt bzw. bespricht ein individuelles Angebot."},{q:"Kann ich nur einzelne M\xF6bel transportieren lassen?",a:"Ja, sofern dieser Service f\xFCr Ihre Anfrage geeignet ist, kann ein individueller Transport einzelner M\xF6bel oder Ladungen organisiert werden."},{q:"\xDCbernimmt KAFI Transporte M\xF6belmontagen?",a:"Nur wenn die Leistung f\xFCr Ihren Auftrag passend ist. Bitte teilen Sie uns Ihre Anforderungen in der Anfrage mit."},{q:"Wie bekomme ich ein Angebot?",a:"\xDCber das Anfrageformular oder telefonisch unter +49 178 7410656."}];this.phones=[{c:"Berlin",n:"+49 178 7410656"}]}onScroll(){this.scrolled.set(scrollY>40)}static{this.\u0275fac=function(t){return new(t||n)}}static{this.\u0275cmp=os({type:n,selectors:[["app-root"]],hostBindings:function(t,i){t&1&&et("scroll",function(){return i.onScroll()},!1,cv)},standalone:!0,features:[fs],decls:266,vars:6,consts:[[1,"nav"],["href","#",1,"logo"],[3,"href"],[1,"nav-actions"],["href","#estimate",1,"btn","ghost"],["href","tel:+491787410656","aria-label","KAFI Transporte anrufen: +49 178 7410656",1,"btn","call-btn"],["aria-hidden","true"],[1,"hero"],[1,"hero-in"],[1,"eyebrow"],["reveal",""],["reveal","",3,"delay"],["reveal","",1,"row",3,"delay"],["href","#estimate",1,"btn","solid"],["href","https://wa.me/491787410656?text=Hallo%20KAFI%20Transporte%2C%20ich%20m%C3%B6chte%20ein%20kostenloses%20Angebot%20anfragen.","target","_blank","rel","noreferrer",1,"btn","ghost"],[1,"trust-bar"],[1,"trust-inner"],["id","services",1,"sec","light"],["reveal","",1,"intro"],[1,"eyebrow","accent"],[1,"cards","services-grid"],["tilt","","reveal","",1,"card","service",3,"delay"],["id","coverage",1,"sec","dark","map-sec"],[1,"split","map-layout"],[1,"lede"],[1,"chip-row"],["tilt","",1,"map-art"],[1,"map-origin"],["data-name","Hamburg","aria-label","Hamburg",1,"city","c-hamburg"],["data-name","Bremen","aria-label","Bremen",1,"city","c-bremen"],["data-name","Hannover","aria-label","Hannover",1,"city","c-hannover"],["data-name","M\xFCnster","aria-label","M\xFCnster",1,"city","c-muenster"],["data-name","Essen","aria-label","Essen",1,"city","c-essen"],["data-name","Dortmund","aria-label","Dortmund",1,"city","c-dortmund"],["data-name","D\xFCsseldorf","aria-label","D\xFCsseldorf",1,"city","c-duesseldorf"],["data-name","K\xF6ln","aria-label","K\xF6ln",1,"city","c-koeln"],["data-name","Frankfurt","aria-label","Frankfurt",1,"city","c-frankfurt"],["data-name","Leipzig","aria-label","Leipzig",1,"city","c-leipzig"],["data-name","Dresden","aria-label","Dresden",1,"city","c-dresden"],["data-name","N\xFCrnberg","aria-label","N\xFCrnberg",1,"city","c-nuernberg"],["data-name","Stuttgart","aria-label","Stuttgart",1,"city","c-stuttgart"],["data-name","M\xFCnchen","aria-label","M\xFCnchen",1,"city","c-muenchen"],["id","steps",1,"sec","light"],[1,"process-grid"],["reveal","",1,"process"],["id","about",1,"sec","dark"],[1,"cards","about-grid"],["tilt","","reveal","",1,"card","about-card",3,"delay"],["id","reviews",1,"sec","light","review-sec"],[1,"lede","small"],[1,"cards","review-grid"],["reveal","",1,"card","review-card"],[1,"stars"],["href","https://www.my-hammer.de/","target","_blank","rel","noreferrer"],["href","https://www.google.com/search?q=KAFI+Transporte+Berlin","target","_blank","rel","noreferrer"],["id","estimate",1,"sec","light"],["reveal","",1,"wiz"],["id","faqs",1,"sec","light"],[1,"faq",3,"open"],["id","cta",1,"sec","dark","center","final-cta"],[1,"mega",3,"parallax"],[1,"cta-sub"],[1,"row"],[1,"footer-grid"],[1,"logo","footer-logo"],["href","tel:+491787410656"],["href","mailto:abourabiyehf@gmail.com"],[1,"footer-bottom"],["href","/impressum/"],["href","/datenschutz/"],["href","https://quality-1st.de/","target","_blank","rel","noopener noreferrer"],[1,"modal"],[1,"ico"],[1,"faq"],[3,"click"],[1,"modal",3,"click"],[1,"sheet",3,"click"],["aria-label","Close",1,"x",3,"click"],["href","https://wa.me/491787410656?text=Hallo%20KAFI%20Transporte%2C%20ich%20m%C3%B6chte%20ein%20kostenloses%20Angebot%20anfragen.","target","_blank","rel","noreferrer",1,"btn","solid"]],template:function(t,i){t&1&&(A(0,"header",0)(1,"a",1),P(2,"KAFI "),A(3,"b"),P(4,"Transporte"),D()(),A(5,"nav"),hn(6,kR,2,2,"a",2,vp),D(),A(8,"div",3)(9,"a",4),P(10,"Angebot erhalten"),D(),A(11,"a",5)(12,"span",6),P(13,"\u260E"),D(),P(14," Anrufen "),D()()(),A(15,"section",7),pn(16,"app-hero-3d"),A(17,"div",8)(18,"p",9),P(19,"Berlin \u2022 Deutschlandweit"),D(),A(20,"h1",10),P(21,"Ihr Umzug."),A(22,"span"),P(23,"Deutschlandweit."),D()(),A(24,"p",11),P(25,"Professionelle Umz\xFCge und Transporte von Berlin in ganz Deutschland \u2013 zuverl\xE4ssig, pers\xF6nlich und unkompliziert."),D(),A(26,"div",12)(27,"a",13),P(28,"Kostenloses Angebot anfragen \u2192"),D(),A(29,"a",5)(30,"span",6),P(31,"\u260E"),D(),P(32," Anrufen "),D(),A(33,"a",14),P(34,"WhatsApp"),D()()()(),A(35,"div",15)(36,"div",16),hn(37,UR,5,2,"span",null,vp),D()(),A(39,"section",17)(40,"div",18)(41,"p",19),P(42,"Leistungen"),D(),A(43,"h2"),P(44,"Alles f\xFCr Ihren Umzug. Aus einer Hand."),D()(),A(45,"div",20),hn(46,VR,7,4,"article",21,h_),D()(),A(48,"section",22)(49,"div",23)(50,"div",10)(51,"p",19),P(52,"Deutschlandweit"),D(),A(53,"h2"),P(54,"Von Berlin nach \xFCberall."),D(),A(55,"p",24),P(56,"Ihr Ziel ist nicht Berlin? KAFI Transporte ist deutschlandweit f\xFCr Sie unterwegs."),D(),A(57,"div",25)(58,"span"),P(59,"Hamburg"),D(),A(60,"span"),P(61,"Bremen"),D(),A(62,"span"),P(63,"Hannover"),D(),A(64,"span"),P(65,"M\xFCnster"),D(),A(66,"span"),P(67,"Essen"),D(),A(68,"span"),P(69,"Dortmund"),D(),A(70,"span"),P(71,"D\xFCsseldorf"),D(),A(72,"span"),P(73,"K\xF6ln"),D(),A(74,"span"),P(75,"Frankfurt"),D(),A(76,"span"),P(77,"Leipzig"),D(),A(78,"span"),P(79,"Dresden"),D(),A(80,"span"),P(81,"N\xFCrnberg"),D(),A(82,"span"),P(83,"Stuttgart"),D(),A(84,"span"),P(85,"M\xFCnchen"),D()()(),A(86,"div",26)(87,"div",27),P(88,"Berlin"),D(),pn(89,"span",28)(90,"span",29)(91,"span",30)(92,"span",31)(93,"span",32)(94,"span",33)(95,"span",34)(96,"span",35)(97,"span",36)(98,"span",37)(99,"span",38)(100,"span",39)(101,"span",40)(102,"span",41),D()()(),A(103,"section",42)(104,"div",18)(105,"p",19),P(106,"Ablauf"),D(),A(107,"h2"),P(108,"So einfach kann Umziehen sein."),D()(),A(109,"div",43)(110,"div",44)(111,"span"),P(112,"01"),D(),A(113,"h3"),P(114,"Anfrage"),D(),A(115,"p"),P(116,"Sie teilen uns Ihre Umzugsdaten mit."),D()(),A(117,"div",44)(118,"span"),P(119,"02"),D(),A(120,"h3"),P(121,"Pr\xFCfung"),D(),A(122,"p"),P(123,"Wir pr\xFCfen Route, Umfang, Termin und Anforderungen."),D()(),A(124,"div",44)(125,"span"),P(126,"03"),D(),A(127,"h3"),P(128,"Kontakt"),D(),A(129,"p"),P(130,"Wir melden uns bei Ihnen und kl\xE4ren offene Fragen."),D()(),A(131,"div",44)(132,"span"),P(133,"04"),D(),A(134,"h3"),P(135,"Angebot"),D(),A(136,"p"),P(137,"Sie erhalten ein individuelles Angebot."),D()(),A(138,"div",44)(139,"span"),P(140,"05"),D(),A(141,"h3"),P(142,"Planung"),D(),A(143,"p"),P(144,"Termin und weitere Details werden abgestimmt."),D()(),A(145,"div",44)(146,"span"),P(147,"06"),D(),A(148,"h3"),P(149,"Umzug"),D(),A(150,"p"),P(151,"Der Umzug wird durchgef\xFChrt."),D()()()(),A(152,"section",45)(153,"div",18)(154,"p",19),P(155,"Warum KAFI"),D(),A(156,"h2"),P(157,"Mehr als nur Transport."),D()(),A(158,"div",46),hn(159,BR,7,4,"article",47,h_),D()(),A(161,"section",48)(162,"div",18)(163,"p",19),P(164,"Bewertungen"),D(),A(165,"h2"),P(166,"Was Kunden \xFCber KAFI Transporte sagen."),D(),A(167,"p",49),P(168,"Echte Erfahrungen von Kunden auf unabh\xE4ngigen Plattformen."),D()(),A(169,"div",50)(170,"article",51)(171,"div",52),P(172,"\u2605\u2605\u2605\u2605\u2605"),D(),A(173,"h3"),P(174,"MyHammer"),D(),A(175,"strong"),P(176,"4,5 / 5"),D(),A(177,"span"),P(178,"414 Bewertungen"),D(),A(179,"a",53),P(180,"Auf MyHammer ansehen \u2192"),D()(),A(181,"article",51)(182,"div",52),P(183,"\u2605\u2605\u2605\u2605\u2605"),D(),A(184,"h3"),P(185,"Google"),D(),A(186,"strong"),P(187,"4,6 / 5"),D(),A(188,"span"),P(189,"21 Bewertungen"),D(),A(190,"a",54),P(191,"Auf Google ansehen \u2192"),D()()()(),A(192,"section",55)(193,"div",56),pn(194,"app-estimate"),D()(),A(195,"section",57)(196,"h2",10),P(197,"H\xE4ufige Fragen"),D(),hn(198,HR,8,4,"div",58,OR),D(),A(200,"section",59)(201,"h2",60),P(202,"Bereit f\xFCr Ihren Umzug?"),D(),A(203,"p",61),P(204,"Von Berlin in ganz Deutschland."),D(),A(205,"div",62)(206,"a",13),P(207,"Kostenloses Angebot anfragen \u2192"),D(),A(208,"a",5)(209,"span",6),P(210,"\u260E"),D(),P(211," Anrufen "),D()()(),A(212,"footer")(213,"div",63)(214,"div")(215,"div",64),P(216,"KAFI "),A(217,"b"),P(218,"Transporte"),D()(),A(219,"p"),P(220,"Ihr Umzug. Deutschlandweit."),D()(),A(221,"div")(222,"h4"),P(223,"Navigation"),D(),A(224,"ul"),hn(225,zR,3,2,"li",null,vp),D()(),A(227,"div")(228,"h4"),P(229,"Kontakt"),D(),A(230,"ul")(231,"li"),P(232,"Ringslebenstra\xDFe 78"),D(),A(233,"li"),P(234,"12353 Berlin"),D(),A(235,"li"),P(236,"Deutschland"),D(),A(237,"li")(238,"a",65),P(239,"+49 178 7410656"),D()(),A(240,"li")(241,"a",66),P(242,"abourabiyehf@gmail.com"),D()()()(),A(243,"div")(244,"h4"),P(245,"Externe Plattformen"),D(),A(246,"ul")(247,"li")(248,"a",53),P(249,"MyHammer"),D()(),A(250,"li")(251,"a",54),P(252,"Google"),D()()()()(),A(253,"div",67)(254,"span"),P(255,"\xA9 2026 KAFI Transporte \xB7 "),A(256,"a",68),P(257,"Impressum"),D(),P(258," \xB7 "),A(259,"a",69),P(260,"Datenschutz"),D()(),A(261,"span"),P(262,"Professionell entwickelt von "),A(263,"a",70),P(264,"Quality1st"),D()()()(),Ut(265,WR,10,0,"div",71)),t&2&&(Pi("solid",i.scrolled()),ie(6),fn(i.nav),ie(18),pt("delay",120),ie(2),pt("delay",240),ie(11),fn(i.trust),ie(9),fn(i.services),ie(113),fn(i.features),ie(39),fn(i.faqs),ie(3),pt("parallax",.05),ie(24),fn(i.nav),ie(40),At(i.modal()?265:-1))},dependencies:[_c,Tc,Dc,Ac,$s],encapsulation:2})}};xy(Ic).catch(console.error);
