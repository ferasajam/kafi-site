var E_=Object.defineProperty,b_=Object.defineProperties;var S_=Object.getOwnPropertyDescriptors;var Ip=Object.getOwnPropertySymbols;var w_=Object.prototype.hasOwnProperty,C_=Object.prototype.propertyIsEnumerable;var Rp=(n,e,t)=>e in n?E_(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,vt=(n,e)=>{for(var t in e||={})w_.call(e,t)&&Rp(n,t,e[t]);if(Ip)for(var t of Ip(e))C_.call(e,t)&&Rp(n,t,e[t]);return n},St=(n,e)=>b_(n,S_(e));var tr=(n,e,t)=>new Promise((i,r)=>{var s=l=>{try{a(t.next(l))}catch(c){r(c)}},o=l=>{try{a(t.throw(l))}catch(c){r(c)}},a=l=>l.done?i(l.value):Promise.resolve(l.value).then(s,o);a((t=t.apply(n,e)).next())});function Np(n,e){return Object.is(n,e)}var Ft=null,$o=!1,qo=1,ti=Symbol("SIGNAL");function Ve(n){let e=Ft;return Ft=n,e}function Pp(){return Ft}var eo={version:0,lastCleanEpoch:0,dirty:!1,producerNode:void 0,producerLastReadVersion:void 0,producerIndexOfThis:void 0,nextProducerIndex:0,liveConsumerNode:void 0,liveConsumerIndexOfThis:void 0,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Bc(n){if($o)throw new Error("");if(Ft===null)return;Ft.consumerOnSignalRead(n);let e=Ft.nextProducerIndex++;if(Ko(Ft),e<Ft.producerNode.length&&Ft.producerNode[e]!==n&&Qs(Ft)){let t=Ft.producerNode[e];Zo(t,Ft.producerIndexOfThis[e])}Ft.producerNode[e]!==n&&(Ft.producerNode[e]=n,Ft.producerIndexOfThis[e]=Qs(Ft)?kp(n,Ft,e):0),Ft.producerLastReadVersion[e]=n.version}function D_(){qo++}function Fp(n){if(!(Qs(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===qo)){if(!n.producerMustRecompute(n)&&!zc(n)){n.dirty=!1,n.lastCleanEpoch=qo;return}n.producerRecomputeValue(n),n.dirty=!1,n.lastCleanEpoch=qo}}function Op(n){if(n.liveConsumerNode===void 0)return;let e=$o;$o=!0;try{for(let t of n.liveConsumerNode)t.dirty||T_(t)}finally{$o=e}}function Lp(){return Ft?.consumerAllowSignalWrites!==!1}function T_(n){n.dirty=!0,Op(n),n.consumerMarkedDirty?.(n)}function Yo(n){return n&&(n.nextProducerIndex=0),Ve(n)}function Hc(n,e){if(Ve(e),!(!n||n.producerNode===void 0||n.producerIndexOfThis===void 0||n.producerLastReadVersion===void 0)){if(Qs(n))for(let t=n.nextProducerIndex;t<n.producerNode.length;t++)Zo(n.producerNode[t],n.producerIndexOfThis[t]);for(;n.producerNode.length>n.nextProducerIndex;)n.producerNode.pop(),n.producerLastReadVersion.pop(),n.producerIndexOfThis.pop()}}function zc(n){Ko(n);for(let e=0;e<n.producerNode.length;e++){let t=n.producerNode[e],i=n.producerLastReadVersion[e];if(i!==t.version||(Fp(t),i!==t.version))return!0}return!1}function Gc(n){if(Ko(n),Qs(n))for(let e=0;e<n.producerNode.length;e++)Zo(n.producerNode[e],n.producerIndexOfThis[e]);n.producerNode.length=n.producerLastReadVersion.length=n.producerIndexOfThis.length=0,n.liveConsumerNode&&(n.liveConsumerNode.length=n.liveConsumerIndexOfThis.length=0)}function kp(n,e,t){if(Up(n),n.liveConsumerNode.length===0&&Vp(n))for(let i=0;i<n.producerNode.length;i++)n.producerIndexOfThis[i]=kp(n.producerNode[i],n,i);return n.liveConsumerIndexOfThis.push(t),n.liveConsumerNode.push(e)-1}function Zo(n,e){if(Up(n),n.liveConsumerNode.length===1&&Vp(n))for(let i=0;i<n.producerNode.length;i++)Zo(n.producerNode[i],n.producerIndexOfThis[i]);let t=n.liveConsumerNode.length-1;if(n.liveConsumerNode[e]=n.liveConsumerNode[t],n.liveConsumerIndexOfThis[e]=n.liveConsumerIndexOfThis[t],n.liveConsumerNode.length--,n.liveConsumerIndexOfThis.length--,e<n.liveConsumerNode.length){let i=n.liveConsumerIndexOfThis[e],r=n.liveConsumerNode[e];Ko(r),r.producerIndexOfThis[i]=e}}function Qs(n){return n.consumerIsAlwaysLive||(n?.liveConsumerNode?.length??0)>0}function Ko(n){n.producerNode??=[],n.producerIndexOfThis??=[],n.producerLastReadVersion??=[]}function Up(n){n.liveConsumerNode??=[],n.liveConsumerIndexOfThis??=[]}function Vp(n){return n.producerNode!==void 0}function Bp(n){let e=Object.create(A_);e.computation=n;let t=()=>{if(Fp(e),Bc(e),e.value===Xo)throw e.error;return e.value};return t[ti]=e,t}var Uc=Symbol("UNSET"),Vc=Symbol("COMPUTING"),Xo=Symbol("ERRORED"),A_=St(vt({},eo),{value:Uc,dirty:!0,error:null,equal:Np,producerMustRecompute(n){return n.value===Uc||n.value===Vc},producerRecomputeValue(n){if(n.value===Vc)throw new Error("Detected cycle in computations.");let e=n.value;n.value=Vc;let t=Yo(n),i;try{i=n.computation()}catch(r){i=Xo,n.error=r}finally{Hc(n,t)}if(e!==Uc&&e!==Xo&&i!==Xo&&n.equal(e,i)){n.value=e;return}n.value=i,n.version++}});function I_(){throw new Error}var Hp=I_;function zp(){Hp()}function Gp(n){Hp=n}var R_=null;function Wp(n){let e=Object.create($p);e.value=n;let t=()=>(Bc(e),e.value);return t[ti]=e,t}function Wc(n,e){Lp()||zp(),n.equal(n.value,e)||(n.value=e,N_(n))}function jp(n,e){Lp()||zp(),Wc(n,e(n.value))}var $p=St(vt({},eo),{equal:Np,value:void 0});function N_(n){n.version++,D_(),Op(n),R_?.()}function st(n){return typeof n=="function"}function Jo(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var Qo=Jo(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function to(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var Yt=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(st(i))try{i()}catch(s){e=s instanceof Qo?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{qp(s)}catch(o){e=e??[],o instanceof Qo?e=[...e,...o.errors]:e.push(o)}}if(e)throw new Qo(e)}}add(e){var t;if(e&&e!==this)if(this.closed)qp(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&to(t,e)}remove(e){let{_finalizers:t}=this;t&&to(t,e),e instanceof n&&e._removeParent(this)}};Yt.EMPTY=(()=>{let n=new Yt;return n.closed=!0,n})();var jc=Yt.EMPTY;function ea(n){return n instanceof Yt||n&&"closed"in n&&st(n.remove)&&st(n.add)&&st(n.unsubscribe)}function qp(n){st(n)?n():n.unsubscribe()}var Rn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Hr={setTimeout(n,e,...t){let{delegate:i}=Hr;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=Hr;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function ta(n){Hr.setTimeout(()=>{let{onUnhandledError:e}=Rn;if(e)e(n);else throw n})}function $c(){}var Xp=qc("C",void 0,void 0);function Yp(n){return qc("E",void 0,n)}function Zp(n){return qc("N",n,void 0)}function qc(n,e,t){return{kind:n,value:e,error:t}}var nr=null;function zr(n){if(Rn.useDeprecatedSynchronousErrorHandling){let e=!nr;if(e&&(nr={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=nr;if(nr=null,t)throw i}}else n()}function Kp(n){Rn.useDeprecatedSynchronousErrorHandling&&nr&&(nr.errorThrown=!0,nr.error=n)}var ir=class extends Yt{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,ea(e)&&e.add(this)):this.destination=O_}static create(e,t,i){return new Gr(e,t,i)}next(e){this.isStopped?Yc(Zp(e),this):this._next(e)}error(e){this.isStopped?Yc(Yp(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?Yc(Xp,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},P_=Function.prototype.bind;function Xc(n,e){return P_.call(n,e)}var Zc=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){na(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){na(i)}else na(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){na(t)}}},Gr=class extends ir{constructor(e,t,i){super();let r;if(st(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&Rn.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&Xc(e.next,s),error:e.error&&Xc(e.error,s),complete:e.complete&&Xc(e.complete,s)}):r=e}this.destination=new Zc(r)}};function na(n){Rn.useDeprecatedSynchronousErrorHandling?Kp(n):ta(n)}function F_(n){throw n}function Yc(n,e){let{onStoppedNotification:t}=Rn;t&&Hr.setTimeout(()=>t(n,e))}var O_={closed:!0,next:$c,error:F_,complete:$c};var Wr=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Jp(n){return n}function Qp(n){return n.length===0?Jp:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var Ut=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=k_(t)?t:new Gr(t,i,r);return zr(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=em(i),new i((r,s)=>{let o=new Gr({next:a=>{try{t(a)}catch(l){s(l),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[Wr](){return this}pipe(...t){return Qp(t)(this)}toPromise(t){return t=em(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function em(n){var e;return(e=n??Rn.Promise)!==null&&e!==void 0?e:Promise}function L_(n){return n&&st(n.next)&&st(n.error)&&st(n.complete)}function k_(n){return n&&n instanceof ir||L_(n)&&ea(n)}function U_(n){return st(n?.lift)}function jr(n){return e=>{if(U_(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function $r(n,e,t,i,r){return new Kc(n,e,t,i,r)}var Kc=class extends ir{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(l){e.error(l)}}:super._next,this._error=r?function(a){try{r(a)}catch(l){e.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var tm=Jo(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var zn=(()=>{class n extends Ut{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new ia(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new tm}next(t){zr(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){zr(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){zr(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?jc:(this.currentObservers=null,s.push(t),new Yt(()=>{this.currentObservers=null,to(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new Ut;return t.source=this,t}}return n.create=(e,t)=>new ia(e,t),n})(),ia=class extends zn{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:jc}};var no=class extends zn{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};function V_(n){return n[n.length-1]}function nm(n){return st(V_(n))?n.pop():void 0}function rm(n,e,t,i){function r(s){return s instanceof t?s:new t(function(o){o(s)})}return new(t||(t=Promise))(function(s,o){function a(u){try{c(i.next(u))}catch(d){o(d)}}function l(u){try{c(i.throw(u))}catch(d){o(d)}}function c(u){u.done?s(u.value):r(u.value).then(a,l)}c((i=i.apply(n,e||[])).next())})}function im(n){var e=typeof Symbol=="function"&&Symbol.iterator,t=e&&n[e],i=0;if(t)return t.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function rr(n){return this instanceof rr?(this.v=n,this):new rr(n)}function sm(n,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(n,e||[]),r,s=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",o),r[Symbol.asyncIterator]=function(){return this},r;function o(f){return function(g){return Promise.resolve(g).then(f,d)}}function a(f,g){i[f]&&(r[f]=function(_){return new Promise(function(m,p){s.push([f,_,m,p])>1||l(f,_)})},g&&(r[f]=g(r[f])))}function l(f,g){try{c(i[f](g))}catch(_){h(s[0][3],_)}}function c(f){f.value instanceof rr?Promise.resolve(f.value.v).then(u,d):h(s[0][2],f)}function u(f){l("next",f)}function d(f){l("throw",f)}function h(f,g){f(g),s.shift(),s.length&&l(s[0][0],s[0][1])}}function om(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=n[Symbol.asyncIterator],t;return e?e.call(n):(n=typeof im=="function"?im(n):n[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(s){t[s]=n[s]&&function(o){return new Promise(function(a,l){o=n[s](o),r(a,l,o.done,o.value)})}}function r(s,o,a,l){Promise.resolve(l).then(function(c){s({value:c,done:a})},o)}}var ra=n=>n&&typeof n.length=="number"&&typeof n!="function";function sa(n){return st(n?.then)}function oa(n){return st(n[Wr])}function aa(n){return Symbol.asyncIterator&&st(n?.[Symbol.asyncIterator])}function la(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function B_(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var ca=B_();function ua(n){return st(n?.[ca])}function da(n){return sm(this,arguments,function*(){let t=n.getReader();try{for(;;){let{value:i,done:r}=yield rr(t.read());if(r)return yield rr(void 0);yield yield rr(i)}}finally{t.releaseLock()}})}function ha(n){return st(n?.getReader)}function Di(n){if(n instanceof Ut)return n;if(n!=null){if(oa(n))return H_(n);if(ra(n))return z_(n);if(sa(n))return G_(n);if(aa(n))return am(n);if(ua(n))return W_(n);if(ha(n))return j_(n)}throw la(n)}function H_(n){return new Ut(e=>{let t=n[Wr]();if(st(t.subscribe))return t.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function z_(n){return new Ut(e=>{for(let t=0;t<n.length&&!e.closed;t++)e.next(n[t]);e.complete()})}function G_(n){return new Ut(e=>{n.then(t=>{e.closed||(e.next(t),e.complete())},t=>e.error(t)).then(null,ta)})}function W_(n){return new Ut(e=>{for(let t of n)if(e.next(t),e.closed)return;e.complete()})}function am(n){return new Ut(e=>{$_(n,e).catch(t=>e.error(t))})}function j_(n){return am(da(n))}function $_(n,e){var t,i,r,s;return rm(this,void 0,void 0,function*(){try{for(t=om(n);i=yield t.next(),!i.done;){let o=i.value;if(e.next(o),e.closed)return}}catch(o){r={error:o}}finally{try{i&&!i.done&&(s=t.return)&&(yield s.call(t))}finally{if(r)throw r.error}}e.complete()})}function Gn(n,e,t,i=0,r=!1){let s=e.schedule(function(){t(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(s),!r)return s}function fa(n,e=0){return jr((t,i)=>{t.subscribe($r(i,r=>Gn(i,n,()=>i.next(r),e),()=>Gn(i,n,()=>i.complete(),e),r=>Gn(i,n,()=>i.error(r),e)))})}function pa(n,e=0){return jr((t,i)=>{i.add(n.schedule(()=>t.subscribe(i),e))})}function lm(n,e){return Di(n).pipe(pa(e),fa(e))}function cm(n,e){return Di(n).pipe(pa(e),fa(e))}function um(n,e){return new Ut(t=>{let i=0;return e.schedule(function(){i===n.length?t.complete():(t.next(n[i++]),t.closed||this.schedule())})})}function dm(n,e){return new Ut(t=>{let i;return Gn(t,e,()=>{i=n[ca](),Gn(t,e,()=>{let r,s;try{({value:r,done:s}=i.next())}catch(o){t.error(o);return}s?t.complete():t.next(r)},0,!0)}),()=>st(i?.return)&&i.return()})}function ma(n,e){if(!n)throw new Error("Iterable cannot be null");return new Ut(t=>{Gn(t,e,()=>{let i=n[Symbol.asyncIterator]();Gn(t,e,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function hm(n,e){return ma(da(n),e)}function fm(n,e){if(n!=null){if(oa(n))return lm(n,e);if(ra(n))return um(n,e);if(sa(n))return cm(n,e);if(aa(n))return ma(n,e);if(ua(n))return dm(n,e);if(ha(n))return hm(n,e)}throw la(n)}function Jc(n,e){return e?fm(n,e):Di(n)}function sr(n,e){return jr((t,i)=>{let r=0;t.subscribe($r(i,s=>{i.next(n.call(e,s,r++))}))})}var{isArray:q_}=Array;function X_(n,e){return q_(e)?n(...e):n(e)}function pm(n){return sr(e=>X_(n,e))}var{isArray:Y_}=Array,{getPrototypeOf:Z_,prototype:K_,keys:J_}=Object;function mm(n){if(n.length===1){let e=n[0];if(Y_(e))return{args:e,keys:null};if(Q_(e)){let t=J_(e);return{args:t.map(i=>e[i]),keys:t}}}return{args:n,keys:null}}function Q_(n){return n&&typeof n=="object"&&Z_(n)===K_}function gm(n,e){return n.reduce((t,i,r)=>(t[i]=e[r],t),{})}function Qc(...n){let e=nm(n),{args:t,keys:i}=mm(n),r=new Ut(s=>{let{length:o}=t;if(!o){s.complete();return}let a=new Array(o),l=o,c=o;for(let u=0;u<o;u++){let d=!1;Di(t[u]).subscribe($r(s,h=>{d||(d=!0,c--),a[u]=h},()=>l--,void 0,()=>{(!l||!d)&&(c||s.next(i?gm(i,a):a),s.complete())}))}});return e?r.pipe(pm(e)):r}var eg="https://g.co/ng/security#xss",qe=class extends Error{constructor(e,t){super(ld(e,t)),this.code=e}};function ld(n,e){return`${`NG0${Math.abs(n)}`}${e?": "+e:""}`}function Ya(n){return{toString:n}.toString()}function yt(n){for(let e in n)if(n[e]===yt)return e;throw Error("Could not find renamed property on target object.")}function ex(n,e){for(let t in e)e.hasOwnProperty(t)&&!n.hasOwnProperty(t)&&(n[t]=e[t])}function Mn(n){if(typeof n=="string")return n;if(Array.isArray(n))return"["+n.map(Mn).join(", ")+"]";if(n==null)return""+n;if(n.overriddenName)return`${n.overriddenName}`;if(n.name)return`${n.name}`;let e=n.toString();if(e==null)return""+e;let t=e.indexOf(`
`);return t===-1?e:e.substring(0,t)}function vm(n,e){return n==null||n===""?e===null?"":e:e==null||e===""?n:n+" "+e}var tx=yt({__forward_ref__:yt});function Sn(n){return n.__forward_ref__=Sn,n.toString=function(){return Mn(this())},n}function Zt(n){return tg(n)?n():n}function tg(n){return typeof n=="function"&&n.hasOwnProperty(tx)&&n.__forward_ref__===Sn}function At(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function Za(n){return{providers:n.providers||[],imports:n.imports||[]}}function cd(n){return ym(n,ng)||ym(n,ig)}function ym(n,e){return n.hasOwnProperty(e)?n[e]:null}function nx(n){let e=n&&(n[ng]||n[ig]);return e||null}function _m(n){return n&&(n.hasOwnProperty(xm)||n.hasOwnProperty(ix))?n[xm]:null}var ng=yt({\u0275prov:yt}),xm=yt({\u0275inj:yt}),ig=yt({ngInjectableDef:yt}),ix=yt({ngInjectorDef:yt}),ze=class{constructor(e,t){this._desc=e,this.ngMetadataName="InjectionToken",this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=At({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function rg(n){return n&&!!n.\u0275providers}var rx=yt({\u0275cmp:yt}),sx=yt({\u0275dir:yt}),ox=yt({\u0275pipe:yt});var wa=yt({\u0275fac:yt}),so=yt({__NG_ELEMENT_ID__:yt}),Mm=yt({__NG_ENV_ID__:yt});function Qr(n){return typeof n=="string"?n:n==null?"":String(n)}function ax(n){return typeof n=="function"?n.name||n.toString():typeof n=="object"&&n!=null&&typeof n.type=="function"?n.type.name||n.type.toString():Qr(n)}function lx(n,e){let t=e?`. Dependency path: ${e.join(" > ")} > ${n}`:"";throw new qe(-200,n)}function ud(n,e){throw new qe(-201,!1)}var Be=function(n){return n[n.Default=0]="Default",n[n.Host=1]="Host",n[n.Self=2]="Self",n[n.SkipSelf=4]="SkipSelf",n[n.Optional=8]="Optional",n}(Be||{}),pu;function sg(){return pu}function Wn(n){let e=pu;return pu=n,e}function og(n,e,t){let i=cd(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&Be.Optional)return null;if(e!==void 0)return e;ud(n,"Injector")}var cx={},oo=cx,ux="__NG_DI_FLAG__",Ca="ngTempTokenPath",dx="ngTokenPath",hx=/\n/gm,fx="\u0275",Em="__source",Kr;function px(){return Kr}function qr(n){let e=Kr;return Kr=n,e}function mx(n,e=Be.Default){if(Kr===void 0)throw new qe(-203,!1);return Kr===null?og(n,void 0,e):Kr.get(n,e&Be.Optional?null:void 0,e)}function ft(n,e=Be.Default){return(sg()||mx)(Zt(n),e)}function ot(n,e=Be.Default){return ft(n,Ka(e))}function Ka(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function mu(n){let e=[];for(let t=0;t<n.length;t++){let i=Zt(n[t]);if(Array.isArray(i)){if(i.length===0)throw new qe(900,!1);let r,s=Be.Default;for(let o=0;o<i.length;o++){let a=i[o],l=gx(a);typeof l=="number"?l===-1?r=a.token:s|=l:r=a}e.push(ft(r,s))}else e.push(ft(i))}return e}function gx(n){return n[ux]}function vx(n,e,t,i){let r=n[Ca];throw e[Em]&&r.unshift(e[Em]),n.message=yx(`
`+n.message,r,t,i),n[dx]=r,n[Ca]=null,n}function yx(n,e,t,i=null){n=n&&n.charAt(0)===`
`&&n.charAt(1)==fx?n.slice(2):n;let r=Mn(e);if(Array.isArray(e))r=e.map(Mn).join(" -> ");else if(typeof e=="object"){let s=[];for(let o in e)if(e.hasOwnProperty(o)){let a=e[o];s.push(o+":"+(typeof a=="string"?JSON.stringify(a):Mn(a)))}r=`{${s.join(", ")}}`}return`${t}${i?"("+i+")":""}[${r}]: ${n.replace(hx,`
  `)}`}function es(n,e){let t=n.hasOwnProperty(wa);return t?n[wa]:null}function _x(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function xx(n){return n.flat(Number.POSITIVE_INFINITY)}function dd(n,e){n.forEach(t=>Array.isArray(t)?dd(t,e):e(t))}function ag(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function Da(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function Mx(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let s=r-2;n[r]=n[s],r--}n[e]=t,n[e+1]=i}}function Ex(n,e,t){let i=vo(n,e);return i>=0?n[i|1]=t:(i=~i,Mx(n,i,e,t)),i}function eu(n,e){let t=vo(n,e);if(t>=0)return n[t|1]}function vo(n,e){return bx(n,e,1)}function bx(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let s=i+(r-i>>1),o=n[s<<t];if(e===o)return s<<t;o>e?r=s:i=s+1}return~(r<<t)}var ts={},xn=[],ao=new ze(""),lg=new ze("",-1),cg=new ze(""),Ta=class{get(e,t=oo){if(t===oo){let i=new Error(`NullInjectorError: No provider for ${Mn(e)}!`);throw i.name="NullInjectorError",i}return t}},ug=function(n){return n[n.OnPush=0]="OnPush",n[n.Default=1]="Default",n}(ug||{}),qn=function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n}(qn||{}),Ii=function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n}(Ii||{});function Sx(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}function gu(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];Cx(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function wx(n){return n===3||n===4||n===6}function Cx(n){return n.charCodeAt(0)===64}function lo(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?bm(n,t,r,null,e[++i]):bm(n,t,r,null,null))}}return n}function bm(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){if(i===null){r!==null&&(n[s+1]=r);return}else if(i===n[s+1]){n[s+2]=r;return}}s++,i!==null&&s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),i!==null&&n.splice(s++,0,i),r!==null&&n.splice(s++,0,r)}var dg="ng-template";function Dx(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&Sx(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(hd(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function hd(n){return n.type===4&&n.value!==dg}function Tx(n,e,t){let i=n.type===4&&!t?dg:n.value;return e===i}function Ax(n,e,t){let i=4,r=n.attrs,s=r!==null?Nx(r):0,o=!1;for(let a=0;a<e.length;a++){let l=e[a];if(typeof l=="number"){if(!o&&!Nn(i)&&!Nn(l))return!1;if(o&&Nn(l))continue;o=!1,i=l|i&1;continue}if(!o)if(i&4){if(i=2|i&1,l!==""&&!Tx(n,l,t)||l===""&&e.length===1){if(Nn(i))return!1;o=!0}}else if(i&8){if(r===null||!Dx(n,r,l,t)){if(Nn(i))return!1;o=!0}}else{let c=e[++a],u=Ix(l,r,hd(n),t);if(u===-1){if(Nn(i))return!1;o=!0;continue}if(c!==""){let d;if(u>s?d="":d=r[u+1].toLowerCase(),i&2&&c!==d){if(Nn(i))return!1;o=!0}}}}return Nn(i)||o}function Nn(n){return(n&1)===0}function Ix(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return Px(e,n)}function Rx(n,e,t=!1){for(let i=0;i<e.length;i++)if(Ax(n,e[i],t))return!0;return!1}function Nx(n){for(let e=0;e<n.length;e++){let t=n[e];if(wx(t))return e}return n.length}function Px(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function Sm(n,e){return n?":not("+e.trim()+")":e}function Fx(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!Nn(o)&&(e+=Sm(s,r),r=""),i=o,s=s||!Nn(i);t++}return r!==""&&(e+=Sm(s,r)),e}function Ox(n){return n.map(Fx).join(",")}function Lx(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!Nn(r))break;r=s}i++}return{attrs:e,classes:t}}function hs(n){return Ya(()=>{let e=pg(n),t=St(vt({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===ug.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||qn.Emulated,styles:n.styles||xn,_:null,schemas:n.schemas||null,tView:null,id:""});mg(t);let i=n.dependencies;return t.directiveDefs=Cm(i,!1),t.pipeDefs=Cm(i,!0),t.id=Bx(t),t})}function kx(n){return ns(n)||hg(n)}function Ux(n){return n!==null}function Ja(n){return Ya(()=>({type:n.type,bootstrap:n.bootstrap||xn,declarations:n.declarations||xn,imports:n.imports||xn,exports:n.exports||xn,transitiveCompileScopes:null,schemas:n.schemas||null,id:n.id||null}))}function wm(n,e){if(n==null)return ts;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a=Ii.None;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s):(s=r,o=r),e?(t[s]=a!==Ii.None?[i,a]:i,e[s]=o):t[s]=i}return t}function Vt(n){return Ya(()=>{let e=pg(n);return mg(e),e})}function ns(n){return n[rx]||null}function hg(n){return n[sx]||null}function fg(n){return n[ox]||null}function Vx(n){let e=ns(n)||hg(n)||fg(n);return e!==null?e.standalone:!1}function pg(n){let e={};return{type:n.type,providersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputTransforms:null,inputConfig:n.inputs||ts,exportAs:n.exportAs||null,standalone:n.standalone===!0,signals:n.signals===!0,selectors:n.selectors||xn,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,findHostDirectiveDefs:null,hostDirectives:null,inputs:wm(n.inputs,e),outputs:wm(n.outputs),debugInfo:null}}function mg(n){n.features?.forEach(e=>e(n))}function Cm(n,e){if(!n)return null;let t=e?fg:kx;return()=>(typeof n=="function"?n():n).map(i=>t(i)).filter(Ux)}function Bx(n){let e=0,t=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,n.consts,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery].join("|");for(let r of t)e=Math.imul(31,e)+r.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function Hx(...n){return{\u0275providers:gg(!0,n),\u0275fromNgModule:!0}}function gg(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return dd(e,o=>{let a=o;vu(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&vg(r,s),t}function vg(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];fd(r,s=>{e(s,i)})}}function vu(n,e,t,i){if(n=Zt(n),!n)return!1;let r=null,s=_m(n),o=!s&&ns(n);if(!s&&!o){let l=n.ngModule;if(s=_m(l),s)r=l;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let l=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let c of l)vu(c,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let c;try{dd(s.imports,u=>{vu(u,e,t,i)&&(c||=[],c.push(u))})}finally{}c!==void 0&&vg(c,e)}if(!a){let c=es(r)||(()=>new r);e({provide:r,useFactory:c,deps:xn},r),e({provide:cg,useValue:r,multi:!0},r),e({provide:ao,useValue:()=>ft(r),multi:!0},r)}let l=s.providers;if(l!=null&&!a){let c=n;fd(l,u=>{e(u,c)})}}else return!1;return r!==n&&n.providers!==void 0}function fd(n,e){for(let t of n)rg(t)&&(t=t.\u0275providers),Array.isArray(t)?fd(t,e):e(t)}var zx=yt({provide:String,useValue:yt});function yg(n){return n!==null&&typeof n=="object"&&zx in n}function Gx(n){return!!(n&&n.useExisting)}function Wx(n){return!!(n&&n.useFactory)}function is(n){return typeof n=="function"}function jx(n){return!!n.useClass}var Qa=new ze(""),_a={},$x={},tu;function pd(){return tu===void 0&&(tu=new Ta),tu}var Ri=class{},Aa=class extends Ri{get destroyed(){return this._destroyed}constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,this.records=new Map,this._ngOnDestroyHooks=new Set,this._onDestroyHooks=[],this._destroyed=!1,_u(e,o=>this.processProvider(o)),this.records.set(lg,Xr(void 0,this)),r.has("environment")&&this.records.set(Ri,Xr(void 0,this));let s=this.records.get(Qa);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(cg,xn,Be.Self))}destroy(){this.assertNotDestroyed(),this._destroyed=!0;let e=Ve(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),Ve(e)}}onDestroy(e){return this.assertNotDestroyed(),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){this.assertNotDestroyed();let t=qr(this),i=Wn(void 0),r;try{return e()}finally{qr(t),Wn(i)}}get(e,t=oo,i=Be.Default){if(this.assertNotDestroyed(),e.hasOwnProperty(Mm))return e[Mm](this);i=Ka(i);let r,s=qr(this),o=Wn(void 0);try{if(!(i&Be.SkipSelf)){let l=this.records.get(e);if(l===void 0){let c=Kx(e)&&cd(e);c&&this.injectableDefInScope(c)?l=Xr(yu(e),_a):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l)}let a=i&Be.Self?pd():this.parent;return t=i&Be.Optional&&t===oo?null:t,a.get(e,t)}catch(a){if(a.name==="NullInjectorError"){if((a[Ca]=a[Ca]||[]).unshift(Mn(e)),s)throw a;return vx(a,e,"R3InjectorError",this.source)}else throw a}finally{Wn(o),qr(s)}}resolveInjectorInitializers(){let e=Ve(null),t=qr(this),i=Wn(void 0),r;try{let s=this.get(ao,xn,Be.Self);for(let o of s)o()}finally{qr(t),Wn(i),Ve(e)}}toString(){let e=[],t=this.records;for(let i of t.keys())e.push(Mn(i));return`R3Injector[${e.join(", ")}]`}assertNotDestroyed(){if(this._destroyed)throw new qe(205,!1)}processProvider(e){e=Zt(e);let t=is(e)?e:Zt(e&&e.provide),i=Xx(e);if(!is(e)&&e.multi===!0){let r=this.records.get(t);r||(r=Xr(void 0,_a,!0),r.factory=()=>mu(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t){let i=Ve(null);try{return t.value===_a&&(t.value=$x,t.value=t.factory()),typeof t.value=="object"&&t.value&&Zx(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{Ve(i)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=Zt(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function yu(n){let e=cd(n),t=e!==null?e.factory:es(n);if(t!==null)return t;if(n instanceof ze)throw new qe(204,!1);if(n instanceof Function)return qx(n);throw new qe(204,!1)}function qx(n){if(n.length>0)throw new qe(204,!1);let t=nx(n);return t!==null?()=>t.factory(n):()=>new n}function Xx(n){if(yg(n))return Xr(void 0,n.useValue);{let e=_g(n);return Xr(e,_a)}}function _g(n,e,t){let i;if(is(n)){let r=Zt(n);return es(r)||yu(r)}else if(yg(n))i=()=>Zt(n.useValue);else if(Wx(n))i=()=>n.useFactory(...mu(n.deps||[]));else if(Gx(n))i=()=>ft(Zt(n.useExisting));else{let r=Zt(n&&(n.useClass||n.provide));if(Yx(n))i=()=>new r(...mu(n.deps));else return es(r)||yu(r)}return i}function Xr(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function Yx(n){return!!n.deps}function Zx(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function Kx(n){return typeof n=="function"||typeof n=="object"&&n instanceof ze}function _u(n,e){for(let t of n)Array.isArray(t)?_u(t,e):t&&rg(t)?_u(t.\u0275providers,e):e(t)}function Jx(){return sg()!==void 0||px()!=null}function Qx(n){return typeof n=="function"}var oi=0,Pe=1,Te=2,Wt=3,Pn=4,On=5,co=6,Ia=7,zt=8,rs=9,Xn=10,Ot=11,uo=12,Dm=13,fs=14,Fn=15,ar=16,Yr=17,ni=18,el=19,xg=20,Ti=21,nu=22,En=23,bn=25,Mg=1;var lr=7,Ra=8,ss=9,Gt=10,Na=function(n){return n[n.None=0]="None",n[n.HasTransplantedViews=2]="HasTransplantedViews",n}(Na||{});function Ai(n){return Array.isArray(n)&&typeof n[Mg]=="object"}function ai(n){return Array.isArray(n)&&n[Mg]===!0}function Eg(n){return(n.flags&4)!==0}function tl(n){return n.componentOffset>-1}function md(n){return(n.flags&1)===1}function Ni(n){return!!n.template}function xu(n){return(n[Te]&512)!==0}var Mu=class{constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}};function bg(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}function nl(){return Sg}function Sg(n){return n.type.prototype.ngOnChanges&&(n.setInput=tM),eM}nl.ngInherit=!0;function eM(){let n=Cg(this),e=n?.current;if(e){let t=n.previous;if(t===ts)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function tM(n,e,t,i,r){let s=this.declaredInputs[i],o=Cg(n)||nM(n,{previous:ts,current:null}),a=o.current||(o.current={}),l=o.previous,c=l[s];a[s]=new Mu(c&&c.currentValue,t,l===ts),bg(n,e,r,t)}var wg="__ngSimpleChanges__";function Cg(n){return n[wg]||null}function nM(n,e){return n[wg]=e}var Tm=null;var jn=function(n,e,t){Tm?.(n,e,t)},iM="svg",rM="math";function Yn(n){for(;Array.isArray(n);)n=n[oi];return n}function Dg(n,e){return Yn(e[n])}function wn(n,e){return Yn(e[n.index])}function gd(n,e){return n.data[e]}function sM(n,e){return n[e]}function Pi(n,e){let t=e[n];return Ai(t)?t:t[oi]}function oM(n){return(n[Te]&4)===4}function vd(n){return(n[Te]&128)===128}function aM(n){return ai(n[Wt])}function os(n,e){return e==null?null:n[e]}function Tg(n){n[Yr]=0}function Ag(n){n[Te]&1024||(n[Te]|=1024,vd(n)&&rl(n))}function lM(n,e){for(;n>0;)e=e[fs],n--;return e}function il(n){return!!(n[Te]&9216||n[En]?.dirty)}function Eu(n){n[Xn].changeDetectionScheduler?.notify(8),n[Te]&64&&(n[Te]|=1024),il(n)&&rl(n)}function rl(n){n[Xn].changeDetectionScheduler?.notify(0);let e=cr(n);for(;e!==null&&!(e[Te]&8192||(e[Te]|=8192,!vd(e)));)e=cr(e)}function Ig(n,e){if((n[Te]&256)===256)throw new qe(911,!1);n[Ti]===null&&(n[Ti]=[]),n[Ti].push(e)}function cM(n,e){if(n[Ti]===null)return;let t=n[Ti].indexOf(e);t!==-1&&n[Ti].splice(t,1)}function cr(n){let e=n[Wt];return ai(e)?e[Wt]:e}var Ue={lFrame:Bg(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Rg=!1;function uM(){return Ue.lFrame.elementDepthCount}function dM(){Ue.lFrame.elementDepthCount++}function hM(){Ue.lFrame.elementDepthCount--}function Ng(){return Ue.bindingsEnabled}function fM(){return Ue.skipHydrationRootTNode!==null}function pM(n){return Ue.skipHydrationRootTNode===n}function mM(){Ue.skipHydrationRootTNode=null}function Ke(){return Ue.lFrame.lView}function jt(){return Ue.lFrame.tView}function Ge(n){return Ue.lFrame.contextLView=n,n[zt]}function We(n){return Ue.lFrame.contextLView=null,n}function sn(){let n=Pg();for(;n!==null&&n.type===64;)n=n.parent;return n}function Pg(){return Ue.lFrame.currentTNode}function gM(){let n=Ue.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function yo(n,e){let t=Ue.lFrame;t.currentTNode=n,t.isParent=e}function Fg(){return Ue.lFrame.isParent}function vM(){Ue.lFrame.isParent=!1}function yM(){return Ue.lFrame.contextLView}function Og(){return Rg}function Am(n){Rg=n}function _M(){return Ue.lFrame.bindingIndex}function xM(n){return Ue.lFrame.bindingIndex=n}function ps(){return Ue.lFrame.bindingIndex++}function Lg(n){let e=Ue.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function MM(){return Ue.lFrame.inI18n}function EM(n,e){let t=Ue.lFrame;t.bindingIndex=t.bindingRootIndex=n,bu(e)}function bM(){return Ue.lFrame.currentDirectiveIndex}function bu(n){Ue.lFrame.currentDirectiveIndex=n}function SM(n){let e=Ue.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function kg(){return Ue.lFrame.currentQueryIndex}function yd(n){Ue.lFrame.currentQueryIndex=n}function wM(n){let e=n[Pe];return e.type===2?e.declTNode:e.type===1?n[On]:null}function Ug(n,e,t){if(t&Be.SkipSelf){let r=e,s=n;for(;r=r.parent,r===null&&!(t&Be.Host);)if(r=wM(s),r===null||(s=s[fs],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=Ue.lFrame=Vg();return i.currentTNode=e,i.lView=n,!0}function _d(n){let e=Vg(),t=n[Pe];Ue.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function Vg(){let n=Ue.lFrame,e=n===null?null:n.child;return e===null?Bg(n):e}function Bg(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function Hg(){let n=Ue.lFrame;return Ue.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var zg=Hg;function xd(){let n=Hg();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function CM(n){return(Ue.lFrame.contextLView=lM(n,Ue.lFrame.contextLView))[zt]}function Fi(){return Ue.lFrame.selectedIndex}function ur(n){Ue.lFrame.selectedIndex=n}function Md(){let n=Ue.lFrame;return gd(n.tView,n.selectedIndex)}function DM(){return Ue.lFrame.currentNamespace}var Gg=!0;function Ed(){return Gg}function bd(n){Gg=n}function TM(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=Sg(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function Sd(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),l&&(n.viewHooks??=[]).push(-t,l),c&&((n.viewHooks??=[]).push(t,c),(n.viewCheckHooks??=[]).push(t,c)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function xa(n,e,t){Wg(n,e,3,t)}function Ma(n,e,t,i){(n[Te]&3)===t&&Wg(n,e,t,i)}function iu(n,e){let t=n[Te];(t&3)===e&&(t&=16383,t+=1,n[Te]=t)}function Wg(n,e,t,i){let r=i!==void 0?n[Yr]&65535:0,s=i??-1,o=e.length-1,a=0;for(let l=r;l<o;l++)if(typeof e[l+1]=="number"){if(a=e[l],i!=null&&a>=i)break}else e[l]<0&&(n[Yr]+=65536),(a<s||s==-1)&&(AM(n,t,e,l),n[Yr]=(n[Yr]&4294901760)+l+2),l++}function Im(n,e){jn(4,n,e);let t=Ve(null);try{e.call(n)}finally{Ve(t),jn(5,n,e)}}function AM(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[Te]>>14<n[Yr]>>16&&(n[Te]&3)===e&&(n[Te]+=16384,Im(a,s)):Im(a,s)}var Jr=-1,dr=class{constructor(e,t,i){this.factory=e,this.resolving=!1,this.canSeeViewProviders=t,this.injectImpl=i}};function IM(n){return n instanceof dr}function RM(n){return(n.flags&8)!==0}function NM(n){return(n.flags&16)!==0}var ru={},Su=class{constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){i=Ka(i);let r=this.injector.get(e,ru,i);return r!==ru||t===ru?r:this.parentInjector.get(e,t,i)}};function jg(n){return n!==Jr}function Pa(n){return n&32767}function PM(n){return n>>16}function Fa(n,e){let t=PM(n),i=e;for(;t>0;)i=i[fs],t--;return i}var wu=!0;function Rm(n){let e=wu;return wu=n,e}var FM=256,$g=FM-1,qg=5,OM=0,$n={};function LM(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(so)&&(i=t[so]),i==null&&(i=t[so]=OM++);let r=i&$g,s=1<<r;e.data[n+(r>>qg)]|=s}function Oa(n,e){let t=Xg(n,e);if(t!==-1)return t;let i=e[Pe];i.firstCreatePass&&(n.injectorIndex=e.length,su(i.data,n),su(e,null),su(i.blueprint,null));let r=wd(n,e),s=n.injectorIndex;if(jg(r)){let o=Pa(r),a=Fa(r,e),l=a[Pe].data;for(let c=0;c<8;c++)e[s+c]=a[o+c]|l[o+c]}return e[s+8]=r,s}function su(n,e){n.push(0,0,0,0,0,0,0,0,e)}function Xg(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function wd(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=Qg(r),i===null)return Jr;if(t++,r=r[fs],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return Jr}function Cu(n,e,t){LM(n,e,t)}function Yg(n,e,t){if(t&Be.Optional||n!==void 0)return n;ud(e,"NodeInjector")}function Zg(n,e,t,i){if(t&Be.Optional&&i===void 0&&(i=null),!(t&(Be.Self|Be.Host))){let r=n[rs],s=Wn(void 0);try{return r?r.get(e,i,t&Be.Optional):og(e,i,t&Be.Optional)}finally{Wn(s)}}return Yg(i,e,t)}function Kg(n,e,t,i=Be.Default,r){if(n!==null){if(e[Te]&2048&&!(i&Be.Self)){let o=BM(n,e,t,i,$n);if(o!==$n)return o}let s=Jg(n,e,t,i,$n);if(s!==$n)return s}return Zg(e,t,i,r)}function Jg(n,e,t,i,r){let s=UM(t);if(typeof s=="function"){if(!Ug(e,n,i))return i&Be.Host?Yg(r,t,i):Zg(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&Be.Optional))ud(t);else return o}finally{zg()}}else if(typeof s=="number"){let o=null,a=Xg(n,e),l=Jr,c=i&Be.Host?e[Fn][On]:null;for((a===-1||i&Be.SkipSelf)&&(l=a===-1?wd(n,e):e[a+8],l===Jr||!Pm(i,!1)?a=-1:(o=e[Pe],a=Pa(l),e=Fa(l,e)));a!==-1;){let u=e[Pe];if(Nm(s,a,u.data)){let d=kM(a,e,t,o,i,c);if(d!==$n)return d}l=e[a+8],l!==Jr&&Pm(i,e[Pe].data[a+8]===c)&&Nm(s,a,e)?(o=u,a=Pa(l),e=Fa(l,e)):a=-1}}return r}function kM(n,e,t,i,r,s){let o=e[Pe],a=o.data[n+8],l=i==null?tl(a)&&wu:i!=o&&(a.type&3)!==0,c=r&Be.Host&&s===a,u=Ea(a,o,t,l,c);return u!==null?hr(e,o,u,a):$n}function Ea(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,l=n.directiveStart,c=n.directiveEnd,u=s>>20,d=i?a:a+u,h=r?a+u:c;for(let f=d;f<h;f++){let g=o[f];if(f<l&&t===g||f>=l&&g.type===t)return f}if(r){let f=o[l];if(f&&Ni(f)&&f.type===t)return l}return null}function hr(n,e,t,i){let r=n[t],s=e.data;if(IM(r)){let o=r;o.resolving&&lx(ax(s[t]));let a=Rm(o.canSeeViewProviders);o.resolving=!0;let l,c=o.injectImpl?Wn(o.injectImpl):null,u=Ug(n,i,Be.Default);try{r=n[t]=o.factory(void 0,s,n,i),e.firstCreatePass&&t>=i.directiveStart&&TM(t,s[t],e)}finally{c!==null&&Wn(c),Rm(a),o.resolving=!1,zg()}}return r}function UM(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(so)?n[so]:void 0;return typeof e=="number"?e>=0?e&$g:VM:e}function Nm(n,e,t){let i=1<<n;return!!(t[e+(n>>qg)]&i)}function Pm(n,e){return!(n&Be.Self)&&!(n&Be.Host&&e)}var or=class{constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return Kg(this._tNode,this._lView,e,Ka(i),t)}};function VM(){return new or(sn(),Ke())}function li(n){return Ya(()=>{let e=n.prototype.constructor,t=e[wa]||Du(e),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let s=r[wa]||Du(r);if(s&&s!==t)return s;r=Object.getPrototypeOf(r)}return s=>new s})}function Du(n){return tg(n)?()=>{let e=Du(Zt(n));return e&&e()}:es(n)}function BM(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[Te]&2048&&!(o[Te]&512);){let a=Jg(s,o,t,i|Be.Self,$n);if(a!==$n)return a;let l=s.parent;if(!l){let c=o[xg];if(c){let u=c.get(t,$n,i);if(u!==$n)return u}l=Qg(o),o=o[fs]}s=l}return r}function Qg(n){let e=n[Pe],t=e.type;return t===2?e.declTNode:t===1?n[On]:null}function Fm(n,e=null,t=null,i){let r=HM(n,e,t,i);return r.resolveInjectorInitializers(),r}function HM(n,e=null,t=null,i,r=new Set){let s=[t||xn,Hx(n)];return i=i||(typeof n=="object"?void 0:Mn(n)),new Aa(s,e||pd(),i||null,r)}var as=class n{static{this.THROW_IF_NOT_FOUND=oo}static{this.NULL=new Ta}static create(e,t){if(Array.isArray(e))return Fm({name:""},t,e,"");{let i=e.name??"";return Fm({name:i},e.parent,e.providers,i)}}static{this.\u0275prov=At({token:n,providedIn:"any",factory:()=>ft(lg)})}static{this.__NG_ELEMENT_ID__=-1}};var zM=new ze("");zM.__NG_ELEMENT_ID__=n=>{let e=sn();if(e===null)throw new qe(204,!1);if(e.type&2)return e.value;if(n&Be.Optional)return null;throw new qe(204,!1)};var GM="ngOriginalError";function ou(n){return n[GM]}var ev=!0,tv=(()=>{class n{static{this.__NG_ELEMENT_ID__=WM}static{this.__NG_ENV_ID__=t=>t}}return n})(),Tu=class extends tv{constructor(e){super(),this._lView=e}onDestroy(e){return Ig(this._lView,e),()=>cM(this._lView,e)}};function WM(){return new Tu(Ke())}var sl=(()=>{class n{constructor(){this.taskId=0,this.pendingTasks=new Set,this.hasPendingTasks=new no(!1)}get _hasPendingTasks(){return this.hasPendingTasks.value}add(){this._hasPendingTasks||this.hasPendingTasks.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),t}remove(t){this.pendingTasks.delete(t),this.pendingTasks.size===0&&this._hasPendingTasks&&this.hasPendingTasks.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this._hasPendingTasks&&this.hasPendingTasks.next(!1)}static{this.\u0275prov=At({token:n,providedIn:"root",factory:()=>new n})}}return n})();var Au=class extends zn{constructor(e=!1){super(),this.destroyRef=void 0,this.pendingTasks=void 0,this.__isAsync=e,Jx()&&(this.destroyRef=ot(tv,{optional:!0})??void 0,this.pendingTasks=ot(sl,{optional:!0})??void 0)}emit(e){let t=Ve(null);try{super.next(e)}finally{Ve(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let l=e;r=l.next?.bind(l),s=l.error?.bind(l),o=l.complete?.bind(l)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof Yt&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{e(t),i!==void 0&&this.pendingTasks?.remove(i)})}}},rn=Au;function La(...n){}function nv(n){let e,t;function i(){n=La;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function Om(n){return queueMicrotask(()=>n()),()=>{n=La}}var Cd="isAngularZone",ka=Cd+"_ID",jM=0,wt=class n{constructor(e){this.hasPendingMacrotasks=!1,this.hasPendingMicrotasks=!1,this.isStable=!0,this.onUnstable=new rn(!1),this.onMicrotaskEmpty=new rn(!1),this.onStable=new rn(!1),this.onError=new rn(!1);let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=ev}=e;if(typeof Zone>"u")throw new qe(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,XM(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Cd)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new qe(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new qe(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,$M,La,La);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},$M={};function Dd(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function qM(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){nv(()=>{n.callbackScheduled=!1,Iu(n),n.isCheckStableRunning=!0,Dd(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),Iu(n)}function XM(n){let e=()=>{qM(n)},t=jM++;n._inner=n._inner.fork({name:"angular",properties:{[Cd]:!0,[ka]:t,[ka+t]:!0},onInvokeTask:(i,r,s,o,a,l)=>{if(YM(l))return i.invokeTask(s,o,a,l);try{return Lm(n),i.invokeTask(s,o,a,l)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),km(n)}},onInvoke:(i,r,s,o,a,l,c)=>{try{return Lm(n),i.invoke(s,o,a,l,c)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!ZM(l)&&e(),km(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,Iu(n),Dd(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function Iu(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function Lm(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function km(n){n._nesting--,Dd(n)}var Ru=class{constructor(){this.hasPendingMicrotasks=!1,this.hasPendingMacrotasks=!1,this.isStable=!0,this.onUnstable=new rn,this.onMicrotaskEmpty=new rn,this.onStable=new rn,this.onError=new rn}run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function YM(n){return iv(n,"__ignore_ng_zone__")}function ZM(n){return iv(n,"__scheduler_tick__")}function iv(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var ii=class{constructor(){this._console=console}handleError(e){let t=this._findOriginalError(e);this._console.error("ERROR",e),t&&this._console.error("ORIGINAL ERROR",t)}_findOriginalError(e){let t=e&&ou(e);for(;t&&ou(t);)t=ou(t);return t||null}},KM=new ze("",{providedIn:"root",factory:()=>{let n=ot(wt),e=ot(ii);return t=>n.runOutsideAngular(()=>e.handleError(t))}});function JM(){return ms(sn(),Ke())}function ms(n,e){return new Cn(wn(n,e))}var Cn=(()=>{class n{constructor(t){this.nativeElement=t}static{this.__NG_ELEMENT_ID__=JM}}return n})();function QM(n){return n instanceof Cn?n.nativeElement:n}function eE(){return this._results[Symbol.iterator]()}var Nu=class n{get changes(){return this._changes??=new rn}constructor(e=!1){this._emitDistinctChangesOnly=e,this.dirty=!0,this._onDirty=void 0,this._results=[],this._changesDetected=!1,this._changes=void 0,this.length=0,this.first=void 0,this.last=void 0;let t=n.prototype;t[Symbol.iterator]||(t[Symbol.iterator]=eE)}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=xx(e);(this._changesDetected=!_x(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.emit(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}};function rv(n){return(n.flags&128)===128}var sv=new Map,tE=0;function nE(){return tE++}function iE(n){sv.set(n[el],n)}function Pu(n){sv.delete(n[el])}var Um="__ngContext__";function fr(n,e){Ai(e)?(n[Um]=e[el],iE(e)):n[Um]=e}function ov(n){return lv(n[uo])}function av(n){return lv(n[Pn])}function lv(n){for(;n!==null&&!ai(n);)n=n[Pn];return n}var Fu;function cv(n){Fu=n}function rE(){if(Fu!==void 0)return Fu;if(typeof document<"u")return document;throw new qe(210,!1)}var Td=new ze("",{providedIn:"root",factory:()=>sE}),sE="ng",Ad=new ze(""),gs=new ze("",{providedIn:"platform",factory:()=>"unknown"});var Id=new ze("",{providedIn:"root",factory:()=>rE().body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var oE="h",aE="b";var lE=()=>null;function Rd(n,e,t=!1){return lE(n,e,t)}var uv=!1,cE=new ze("",{providedIn:"root",factory:()=>uv});var Ua=class{constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${eg})`}};function ol(n){return n instanceof Ua?n.changingThisBreaksApplicationSecurity:n}function dv(n,e){let t=uE(n);if(t!=null&&t!==e){if(t==="ResourceURL"&&e==="URL")return!0;throw new Error(`Required a safe ${e}, got a ${t} (see ${eg})`)}return t===e}function uE(n){return n instanceof Ua&&n.getTypeName()||null}var dE=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function hv(n){return n=String(n),n.match(dE)?n:"unsafe:"+n}var Nd=function(n){return n[n.NONE=0]="NONE",n[n.HTML=1]="HTML",n[n.STYLE=2]="STYLE",n[n.SCRIPT=3]="SCRIPT",n[n.URL=4]="URL",n[n.RESOURCE_URL=5]="RESOURCE_URL",n}(Nd||{});function al(n){let e=hE();return e?e.sanitize(Nd.URL,n)||"":dv(n,"URL")?ol(n):hv(Qr(n))}function hE(){let n=Ke();return n&&n[Xn].sanitizer}function fv(n){return n.ownerDocument.defaultView}var ri=function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n}(ri||{}),fE;function Pd(n,e){return fE(n,e)}function Zr(n,e,t,i,r){if(i!=null){let s,o=!1;ai(i)?s=i:Ai(i)&&(o=!0,i=i[oi]);let a=Yn(i);n===0&&t!==null?r==null?vv(e,t,a):Va(e,t,a,r||null,!0):n===1&&t!==null?Va(e,t,a,r||null,!0):n===2?TE(e,a,o):n===3&&e.destroyNode(a),s!=null&&IE(e,n,s,t,r)}}function pE(n,e){return n.createText(e)}function mE(n,e,t){n.setValue(e,t)}function pv(n,e,t){return n.createElement(e,t)}function gE(n,e){mv(n,e),e[oi]=null,e[On]=null}function vE(n,e,t,i,r,s){i[oi]=r,i[On]=e,cl(n,i,t,1,r,s)}function mv(n,e){e[Xn].changeDetectionScheduler?.notify(9),cl(n,e,e[Ot],2,null,null)}function yE(n){let e=n[uo];if(!e)return au(n[Pe],n);for(;e;){let t=null;if(Ai(e))t=e[uo];else{let i=e[Gt];i&&(t=i)}if(!t){for(;e&&!e[Pn]&&e!==n;)Ai(e)&&au(e[Pe],e),e=e[Wt];e===null&&(e=n),Ai(e)&&au(e[Pe],e),t=e&&e[Pn]}e=t}}function _E(n,e,t,i){let r=Gt+i,s=t.length;i>0&&(t[r-1][Pn]=e),i<s-Gt?(e[Pn]=t[r],ag(t,Gt+i,e)):(t.push(e),e[Pn]=null),e[Wt]=t;let o=e[ar];o!==null&&t!==o&&gv(o,e);let a=e[ni];a!==null&&a.insertView(n),Eu(e),e[Te]|=128}function gv(n,e){let t=n[ss],i=e[Wt];if(Ai(i))n[Te]|=Na.HasTransplantedViews;else{let r=i[Wt][Fn];e[Fn]!==r&&(n[Te]|=Na.HasTransplantedViews)}t===null?n[ss]=[e]:t.push(e)}function Fd(n,e){let t=n[ss],i=t.indexOf(e);t.splice(i,1)}function ho(n,e){if(n.length<=Gt)return;let t=Gt+e,i=n[t];if(i){let r=i[ar];r!==null&&r!==n&&Fd(r,i),e>0&&(n[t-1][Pn]=i[Pn]);let s=Da(n,Gt+e);gE(i[Pe],i);let o=s[ni];o!==null&&o.detachView(s[Pe]),i[Wt]=null,i[Pn]=null,i[Te]&=-129}return i}function ll(n,e){if(!(e[Te]&256)){let t=e[Ot];t.destroyNode&&cl(n,e,t,3,null,null),yE(e)}}function au(n,e){if(e[Te]&256)return;let t=Ve(null);try{e[Te]&=-129,e[Te]|=256,e[En]&&Gc(e[En]),ME(n,e),xE(n,e),e[Pe].type===1&&e[Ot].destroy();let i=e[ar];if(i!==null&&ai(e[Wt])){i!==e[Wt]&&Fd(i,e);let r=e[ni];r!==null&&r.detachView(n)}Pu(e)}finally{Ve(t)}}function xE(n,e){let t=n.cleanup,i=e[Ia];if(t!==null)for(let s=0;s<t.length-1;s+=2)if(typeof t[s]=="string"){let o=t[s+3];o>=0?i[o]():i[-o].unsubscribe(),s+=2}else{let o=i[t[s+1]];t[s].call(o)}i!==null&&(e[Ia]=null);let r=e[Ti];if(r!==null){e[Ti]=null;for(let s=0;s<r.length;s++){let o=r[s];o()}}}function ME(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof dr)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],l=s[o+1];jn(4,a,l);try{l.call(a)}finally{jn(5,a,l)}}else{jn(4,r,s);try{s.call(r)}finally{jn(5,r,s)}}}}}function EE(n,e,t){return bE(n,e.parent,t)}function bE(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[oi];{let{componentOffset:r}=i;if(r>-1){let{encapsulation:s}=n.data[i.directiveStart+r];if(s===qn.None||s===qn.Emulated)return null}return wn(i,t)}}function Va(n,e,t,i,r){n.insertBefore(e,t,i,r)}function vv(n,e,t){n.appendChild(e,t)}function Vm(n,e,t,i,r){i!==null?Va(n,e,t,i,r):vv(n,e,t)}function yv(n,e){return n.parentNode(e)}function SE(n,e){return n.nextSibling(e)}function wE(n,e,t){return DE(n,e,t)}function CE(n,e,t){return n.type&40?wn(n,t):null}var DE=CE,Bm;function Od(n,e,t,i){let r=EE(n,i,e),s=e[Ot],o=i.parent||e[On],a=wE(o,i,e);if(r!=null)if(Array.isArray(t))for(let l=0;l<t.length;l++)Vm(s,r,t[l],a,!1);else Vm(s,r,t,a,!1);Bm!==void 0&&Bm(s,i,e,t,r)}function io(n,e){if(e!==null){let t=e.type;if(t&3)return wn(e,n);if(t&4)return Ou(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return io(n,i);{let r=n[e.index];return ai(r)?Ou(-1,r):Yn(r)}}else{if(t&128)return io(n,e.next);if(t&32)return Pd(e,n)()||Yn(n[e.index]);{let i=_v(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=cr(n[Fn]);return io(r,i)}else return io(n,e.next)}}}return null}function _v(n,e){if(e!==null){let i=n[Fn][On],r=e.projection;return i.projection[r]}return null}function Ou(n,e){let t=Gt+n+1;if(t<e.length){let i=e[t],r=i[Pe].firstChild;if(r!==null)return io(i,r)}return e[lr]}function TE(n,e,t){n.removeChild(null,e,t)}function Ld(n,e,t,i,r,s,o){for(;t!=null;){if(t.type===128){t=t.next;continue}let a=i[t.index],l=t.type;if(o&&e===0&&(a&&fr(Yn(a),i),t.flags|=2),(t.flags&32)!==32)if(l&8)Ld(n,e,t.child,i,r,s,!1),Zr(e,n,r,a,s);else if(l&32){let c=Pd(t,i),u;for(;u=c();)Zr(e,n,r,u,s);Zr(e,n,r,a,s)}else l&16?AE(n,e,i,t,r,s):Zr(e,n,r,a,s);t=o?t.projectionNext:t.next}}function cl(n,e,t,i,r,s){Ld(t,i,n.firstChild,e,r,s,!1)}function AE(n,e,t,i,r,s){let o=t[Fn],l=o[On].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let u=l[c];Zr(e,n,r,u,s)}else{let c=l,u=o[Wt];rv(i)&&(c.flags|=128),Ld(n,e,c,u,r,s,!0)}}function IE(n,e,t,i,r){let s=t[lr],o=Yn(t);s!==o&&Zr(e,n,i,s,r);for(let a=Gt;a<t.length;a++){let l=t[a];cl(l[Pe],l,n,e,i,s)}}function RE(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let s=i.indexOf("-")===-1?void 0:ri.DashCase;r==null?n.removeStyle(t,i,s):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),s|=ri.Important),n.setStyle(t,i,r,s))}}function NE(n,e,t){n.setAttribute(e,"style",t)}function xv(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function Mv(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&gu(n,e,i),r!==null&&xv(n,e,r),s!==null&&NE(n,e,s)}var ci={};function ee(n=1){Ev(jt(),Ke(),Fi()+n,!1)}function Ev(n,e,t,i){if(!i)if((e[Te]&3)===3){let s=n.preOrderCheckHooks;s!==null&&xa(e,s,t)}else{let s=n.preOrderHooks;s!==null&&Ma(e,s,0,t)}ur(t)}function Et(n,e=Be.Default){let t=Ke();if(t===null)return ft(n,e);let i=sn();return Kg(i,t,Zt(n),e)}function bv(n,e,t,i,r,s){let o=Ve(null);try{let a=null;r&Ii.SignalBased&&(a=e[i][ti]),a!==null&&a.transformFn!==void 0&&(s=a.transformFn(s)),r&Ii.HasDecoratorInputTransform&&(s=n.inputTransforms[i].call(e,s)),n.setInput!==null?n.setInput(e,a,s,t,i):bg(e,a,i,s)}finally{Ve(o)}}function PE(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)ur(~r);else{let s=r,o=t[++i],a=t[++i];EM(o,s);let l=e[s];a(2,l)}}}finally{ur(-1)}}function ul(n,e,t,i,r,s,o,a,l,c,u){let d=e.blueprint.slice();return d[oi]=r,d[Te]=i|4|128|8|64,(c!==null||n&&n[Te]&2048)&&(d[Te]|=2048),Tg(d),d[Wt]=d[fs]=n,d[zt]=t,d[Xn]=o||n&&n[Xn],d[Ot]=a||n&&n[Ot],d[rs]=l||n&&n[rs]||null,d[On]=s,d[el]=nE(),d[co]=u,d[xg]=c,d[Fn]=e.type==2?n[Fn]:d,d}function dl(n,e,t,i,r){let s=n.data[e];if(s===null)s=FE(n,e,t,i,r),MM()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=gM();s.injectorIndex=o===null?-1:o.injectorIndex}return yo(s,!0),s}function FE(n,e,t,i,r){let s=Pg(),o=Fg(),a=o?s:s&&s.parent,l=n.data[e]=BE(n,a,t,e,i,r);return n.firstChild===null&&(n.firstChild=l),s!==null&&(o?s.child==null&&l.parent!==null&&(s.child=l):s.next===null&&(s.next=l,l.prev=s)),l}function Sv(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function wv(n,e,t,i,r){let s=Fi(),o=i&2;try{ur(-1),o&&e.length>bn&&Ev(n,e,bn,!1),jn(o?2:0,r),t(i,r)}finally{ur(s),jn(o?3:1,r)}}function Cv(n,e,t){if(Eg(e)){let i=Ve(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let l=t[o];a.contentQueries(1,l,o)}}}finally{Ve(i)}}}function Dv(n,e,t){Ng()&&($E(n,e,t,wn(t,e)),(t.flags&64)===64&&Pv(n,e,t))}function Tv(n,e,t=wn){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function Av(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=kd(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function kd(n,e,t,i,r,s,o,a,l,c,u){let d=bn+i,h=d+r,f=OE(d,h),g=typeof c=="function"?c():c;return f[Pe]={type:n,blueprint:f,template:t,queries:null,viewQuery:a,declTNode:e,data:f.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:l,consts:g,incompleteFirstPass:!1,ssrId:u}}function OE(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:ci);return t}function LE(n,e,t,i){let s=i.get(cE,uv)||t===qn.ShadowDom,o=n.selectRootElement(e,s);return kE(o),o}function kE(n){UE(n)}var UE=()=>null;function VE(n,e,t,i){let r=Lv(e);r.push(t),n.firstCreatePass&&kv(n).push(i,r.length-1)}function BE(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return fM()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:s,mergedAttrs:null,localNames:null,initialInputs:void 0,inputs:null,outputs:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function Hm(n,e,t,i,r){for(let s in e){if(!e.hasOwnProperty(s))continue;let o=e[s];if(o===void 0)continue;i??={};let a,l=Ii.None;Array.isArray(o)?(a=o[0],l=o[1]):a=o;let c=s;if(r!==null){if(!r.hasOwnProperty(s))continue;c=r[s]}n===0?zm(i,t,c,a,l):zm(i,t,c,a)}return i}function zm(n,e,t,i,r){let s;n.hasOwnProperty(t)?(s=n[t]).push(e,i):s=n[t]=[e,i],r!==void 0&&s.push(r)}function HE(n,e,t){let i=e.directiveStart,r=e.directiveEnd,s=n.data,o=e.attrs,a=[],l=null,c=null;for(let u=i;u<r;u++){let d=s[u],h=t?t.get(d):null,f=h?h.inputs:null,g=h?h.outputs:null;l=Hm(0,d.inputs,u,l,f),c=Hm(1,d.outputs,u,c,g);let _=l!==null&&o!==null&&!hd(e)?ib(l,u,o):null;a.push(_)}l!==null&&(l.hasOwnProperty("class")&&(e.flags|=8),l.hasOwnProperty("style")&&(e.flags|=16)),e.initialInputs=a,e.inputs=l,e.outputs=c}function zE(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function Iv(n,e,t,i,r,s,o,a){let l=wn(e,t),c=e.inputs,u;!a&&c!=null&&(u=c[i])?(Ud(n,t,u,i,r),tl(e)&&GE(t,e.index)):e.type&3?(i=zE(i),r=o!=null?o(r,e.value||"",i):r,s.setProperty(l,i,r)):e.type&12}function GE(n,e){let t=Pi(e,n);t[Te]&16||(t[Te]|=64)}function Rv(n,e,t,i){if(Ng()){let r=i===null?null:{"":-1},s=XE(n,t),o,a;s===null?o=a=null:[o,a]=s,o!==null&&Nv(n,e,t,o,r,a),r&&YE(t,i,r)}t.mergedAttrs=lo(t.mergedAttrs,t.attrs)}function Nv(n,e,t,i,r,s){for(let c=0;c<i.length;c++)Cu(Oa(t,e),n,i[c].type);KE(t,n.data.length,i.length);for(let c=0;c<i.length;c++){let u=i[c];u.providersResolver&&u.providersResolver(u)}let o=!1,a=!1,l=Sv(n,e,i.length,null);for(let c=0;c<i.length;c++){let u=i[c];t.mergedAttrs=lo(t.mergedAttrs,u.hostAttrs),JE(n,t,e,l,u),ZE(l,u,r),u.contentQueries!==null&&(t.flags|=4),(u.hostBindings!==null||u.hostAttrs!==null||u.hostVars!==0)&&(t.flags|=64);let d=u.type.prototype;!o&&(d.ngOnChanges||d.ngOnInit||d.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),o=!0),!a&&(d.ngOnChanges||d.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),a=!0),l++}HE(n,t,s)}function WE(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;jE(o)!=a&&o.push(a),o.push(t,i,s)}}function jE(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function $E(n,e,t,i){let r=t.directiveStart,s=t.directiveEnd;tl(t)&&QE(e,t,n.data[r+t.componentOffset]),n.firstCreatePass||Oa(t,e),fr(i,e);let o=t.initialInputs;for(let a=r;a<s;a++){let l=n.data[a],c=hr(e,n,a,t);if(fr(c,e),o!==null&&nb(e,a-r,c,l,t,o),Ni(l)){let u=Pi(t.index,e);u[zt]=hr(e,n,a,t)}}}function Pv(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=bM();try{ur(s);for(let a=i;a<r;a++){let l=n.data[a],c=e[a];bu(a),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&qE(l,c)}}finally{ur(-1),bu(o)}}function qE(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function XE(n,e){let t=n.directiveRegistry,i=null,r=null;if(t)for(let s=0;s<t.length;s++){let o=t[s];if(Rx(e,o.selectors,!1))if(i||(i=[]),Ni(o))if(o.findHostDirectiveDefs!==null){let a=[];r=r||new Map,o.findHostDirectiveDefs(o,a,r),i.unshift(...a,o);let l=a.length;Lu(n,e,l)}else i.unshift(o),Lu(n,e,0);else r=r||new Map,o.findHostDirectiveDefs?.(o,i,r),i.push(o)}return i===null?null:[i,r]}function Lu(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function YE(n,e,t){if(e){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new qe(-301,!1);i.push(e[r],s)}}}function ZE(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;Ni(e)&&(t[""]=n)}}function KE(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function JE(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=es(r.type,!0)),o=new dr(s,Ni(r),Et);n.blueprint[i]=o,t[i]=o,WE(n,e,i,Sv(n,t,r.hostVars,ci),r)}function QE(n,e,t){let i=wn(e,n),r=Av(t),s=n[Xn].rendererFactory,o=16;t.signals?o=4096:t.onPush&&(o=64);let a=hl(n,ul(n,r,null,o,i,e,null,s.createRenderer(i,t),null,null,null));n[e.index]=a}function eb(n,e,t,i,r,s){let o=wn(n,e);tb(e[Ot],o,s,n.value,t,i,r)}function tb(n,e,t,i,r,s,o){if(s==null)n.removeAttribute(e,r,t);else{let a=o==null?Qr(s):o(s,i||"",r);n.setAttribute(e,r,a,t)}}function nb(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;){let l=o[a++],c=o[a++],u=o[a++],d=o[a++];bv(i,t,l,c,u,d)}}function ib(n,e,t){let i=null,r=0;for(;r<t.length;){let s=t[r];if(s===0){r+=4;continue}else if(s===5){r+=2;continue}if(typeof s=="number")break;if(n.hasOwnProperty(s)){i===null&&(i=[]);let o=n[s];for(let a=0;a<o.length;a+=3)if(o[a]===e){i.push(s,o[a+1],o[a+2],t[r+1]);break}}r+=2}return i}function Fv(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function Ov(n,e){let t=n.contentQueries;if(t!==null){let i=Ve(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];yd(s),a.contentQueries(2,e[o],o)}}}finally{Ve(i)}}}function hl(n,e){return n[uo]?n[Dm][Pn]=e:n[uo]=e,n[Dm]=e,e}function ku(n,e,t){yd(0);let i=Ve(null);try{e(n,t)}finally{Ve(i)}}function Lv(n){return n[Ia]??=[]}function kv(n){return n.cleanup??=[]}function Uv(n,e){let t=n[rs],i=t?t.get(ii,null):null;i&&i.handleError(e)}function Ud(n,e,t,i,r){for(let s=0;s<t.length;){let o=t[s++],a=t[s++],l=t[s++],c=e[o],u=n.data[o];bv(u,c,i,a,l,r)}}function Vv(n,e,t){let i=Dg(e,n);mE(n[Ot],i,t)}function rb(n,e){let t=Pi(e,n),i=t[Pe];sb(i,t);let r=t[oi];r!==null&&t[co]===null&&(t[co]=Rd(r,t[rs])),Vd(i,t,t[zt])}function sb(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function Vd(n,e,t){_d(e);try{let i=n.viewQuery;i!==null&&ku(1,i,t);let r=n.template;r!==null&&wv(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[ni]?.finishViewCreation(n),n.staticContentQueries&&Ov(n,e),n.staticViewQueries&&ku(2,n.viewQuery,t);let s=n.components;s!==null&&ob(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[Te]&=-5,xd()}}function ob(n,e){for(let t=0;t<e.length;t++)rb(n,e[t])}function fl(n,e,t,i){let r=Ve(null);try{let s=e.tView,a=n[Te]&4096?4096:16,l=ul(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=n[e.index];l[ar]=c;let u=n[ni];return u!==null&&(l[ni]=u.createEmbeddedView(s)),Vd(s,l,t),l}finally{Ve(r)}}function Bv(n,e){let t=Gt+e;if(t<n.length)return n[t]}function fo(n,e){return!e||e.firstChild===null||rv(n)}function pl(n,e,t,i=!0){let r=e[Pe];if(_E(r,e,n,t),i){let o=Ou(t,n),a=e[Ot],l=yv(a,n[lr]);l!==null&&vE(r,n[On],a,e,l,o)}let s=e[co];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function Hv(n,e){let t=ho(n,e);return t!==void 0&&ll(t[Pe],t),t}function Ba(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(Yn(s)),ai(s)&&ab(s,i);let o=t.type;if(o&8)Ba(n,e,t.child,i);else if(o&32){let a=Pd(t,e),l;for(;l=a();)i.push(l)}else if(o&16){let a=_v(e,t);if(Array.isArray(a))i.push(...a);else{let l=cr(e[Fn]);Ba(l[Pe],l,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function ab(n,e){for(let t=Gt;t<n.length;t++){let i=n[t],r=i[Pe].firstChild;r!==null&&Ba(i[Pe],i,r,e)}n[lr]!==n[oi]&&e.push(n[lr])}var zv=[];function lb(n){return n[En]??cb(n)}function cb(n){let e=zv.pop()??Object.create(db);return e.lView=n,e}function ub(n){n.lView[En]!==n&&(n.lView=null,zv.push(n))}var db=St(vt({},eo),{consumerIsAlwaysLive:!0,consumerMarkedDirty:n=>{rl(n.lView)},consumerOnSignalRead(){this.lView[En]=this}});function hb(n){let e=n[En]??Object.create(fb);return e.lView=n,e}var fb=St(vt({},eo),{consumerIsAlwaysLive:!0,consumerMarkedDirty:n=>{let e=cr(n.lView);for(;e&&!Gv(e[Pe]);)e=cr(e);e&&Ag(e)},consumerOnSignalRead(){this.lView[En]=this}});function Gv(n){return n.type!==2}var pb=100;function Wv(n,e=!0,t=0){let i=n[Xn],r=i.rendererFactory,s=!1;s||r.begin?.();try{mb(n,t)}catch(o){throw e&&Uv(n,o),o}finally{s||(r.end?.(),i.inlineEffectRunner?.flush())}}function mb(n,e){let t=Og();try{Am(!0),Uu(n,e);let i=0;for(;il(n);){if(i===pb)throw new qe(103,!1);i++,Uu(n,1)}}finally{Am(t)}}function gb(n,e,t,i){let r=e[Te];if((r&256)===256)return;let s=!1,o=!1;!s&&e[Xn].inlineEffectRunner?.flush(),_d(e);let a=!0,l=null,c=null;s||(Gv(n)?(c=lb(e),l=Yo(c)):Pp()===null?(a=!1,c=hb(e),l=Yo(c)):e[En]&&(Gc(e[En]),e[En]=null));try{Tg(e),xM(n.bindingStartIndex),t!==null&&wv(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let f=n.preOrderCheckHooks;f!==null&&xa(e,f,null)}else{let f=n.preOrderHooks;f!==null&&Ma(e,f,0,null),iu(e,0)}if(o||vb(e),jv(e,0),n.contentQueries!==null&&Ov(n,e),!s)if(u){let f=n.contentCheckHooks;f!==null&&xa(e,f)}else{let f=n.contentHooks;f!==null&&Ma(e,f,1),iu(e,1)}PE(n,e);let d=n.components;d!==null&&qv(e,d,0);let h=n.viewQuery;if(h!==null&&ku(2,h,i),!s)if(u){let f=n.viewCheckHooks;f!==null&&xa(e,f)}else{let f=n.viewHooks;f!==null&&Ma(e,f,2),iu(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[nu]){for(let f of e[nu])f();e[nu]=null}s||(e[Te]&=-73)}catch(u){throw s||rl(e),u}finally{c!==null&&(Hc(c,l),a&&ub(c)),xd()}}function jv(n,e){for(let t=ov(n);t!==null;t=av(t))for(let i=Gt;i<t.length;i++){let r=t[i];$v(r,e)}}function vb(n){for(let e=ov(n);e!==null;e=av(e)){if(!(e[Te]&Na.HasTransplantedViews))continue;let t=e[ss];for(let i=0;i<t.length;i++){let r=t[i];Ag(r)}}}function yb(n,e,t){let i=Pi(e,n);$v(i,t)}function $v(n,e){vd(n)&&Uu(n,e)}function Uu(n,e){let i=n[Pe],r=n[Te],s=n[En],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&zc(s)),o||=!1,s&&(s.dirty=!1),n[Te]&=-9217,o)gb(i,n,i.template,n[zt]);else if(r&8192){jv(n,1);let a=i.components;a!==null&&qv(n,a,1)}}function qv(n,e,t){for(let i=0;i<e.length;i++)yb(n,e[i],t)}function Bd(n,e){let t=Og()?64:1088;for(n[Xn].changeDetectionScheduler?.notify(e);n;){n[Te]|=t;let i=cr(n);if(xu(n)&&!i)return n;n=i}return null}var pr=class{get rootNodes(){let e=this._lView,t=e[Pe];return Ba(t,e,t.firstChild,[])}constructor(e,t,i=!0){this._lView=e,this._cdRefInjectingView=t,this.notifyErrorHandler=i,this._appRef=null,this._attachedToViewContainer=!1}get context(){return this._lView[zt]}set context(e){this._lView[zt]=e}get destroyed(){return(this._lView[Te]&256)===256}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[Wt];if(ai(e)){let t=e[Ra],i=t?t.indexOf(this):-1;i>-1&&(ho(e,i),Da(t,i))}this._attachedToViewContainer=!1}ll(this._lView[Pe],this._lView)}onDestroy(e){Ig(this._lView,e)}markForCheck(){Bd(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[Te]&=-129}reattach(){Eu(this._lView),this._lView[Te]|=128}detectChanges(){this._lView[Te]|=1024,Wv(this._lView,this.notifyErrorHandler)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new qe(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=xu(this._lView),t=this._lView[ar];t!==null&&!e&&Fd(t,this._lView),mv(this._lView[Pe],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new qe(902,!1);this._appRef=e;let t=xu(this._lView),i=this._lView[ar];i!==null&&!t&&gv(i,this._lView),Eu(this._lView)}},Ha=(()=>{class n{static{this.__NG_ELEMENT_ID__=Mb}}return n})(),_b=Ha,xb=class extends _b{constructor(e,t,i){super(),this._declarationLView=e,this._declarationTContainer=t,this.elementRef=i}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,t){return this.createEmbeddedViewImpl(e,t)}createEmbeddedViewImpl(e,t,i){let r=fl(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:t,dehydratedView:i});return new pr(r)}};function Mb(){return Hd(sn(),Ke())}function Hd(n,e){return n.type&4?new xb(e,n,ms(n,e)):null}var o2=new RegExp(`^(\\d+)*(${aE}|${oE})*(.*)`);var Eb=()=>null;function po(n,e){return Eb(n,e)}var ls=class{},zd=new ze("",{providedIn:"root",factory:()=>!1});var Xv=new ze(""),Yv=new ze(""),Vu=class{},za=class{};function bb(n){let e=Error(`No component factory found for ${Mn(n)}.`);return e[Sb]=n,e}var Sb="ngComponent";var Bu=class{resolveComponentFactory(e){throw bb(e)}},mo=class{static{this.NULL=new Bu}},cs=class{},ml=(()=>{class n{constructor(){this.destroyNode=null}static{this.__NG_ELEMENT_ID__=()=>wb()}}return n})();function wb(){let n=Ke(),e=sn(),t=Pi(e.index,n);return(Ai(t)?t:n)[Ot]}var Cb=(()=>{class n{static{this.\u0275prov=At({token:n,providedIn:"root",factory:()=>null})}}return n})();function Hu(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=vm(r,a);else if(s==2){let l=a,c=e[++o];i=vm(i,l+": "+c+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}var zu=class extends mo{constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=ns(e);return new Ga(t,this.ngModule)}};function Gm(n,e){let t=[];for(let i in n){if(!n.hasOwnProperty(i))continue;let r=n[i];if(r===void 0)continue;let s=Array.isArray(r),o=s?r[0]:r,a=s?r[1]:Ii.None;e?t.push({propName:o,templateName:i,isSignal:(a&Ii.SignalBased)!==0}):t.push({propName:o,templateName:i})}return t}function Db(n){let e=n.toLowerCase();return e==="svg"?iM:e==="math"?rM:null}var Ga=class extends za{get inputs(){let e=this.componentDef,t=e.inputTransforms,i=Gm(e.inputs,!0);if(t!==null)for(let r of i)t.hasOwnProperty(r.propName)&&(r.transform=t[r.propName]);return i}get outputs(){return Gm(this.componentDef.outputs,!1)}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=Ox(e.selectors),this.ngContentSelectors=e.ngContentSelectors?e.ngContentSelectors:[],this.isBoundToModule=!!t}create(e,t,i,r){let s=Ve(null);try{r=r||this.ngModule;let o=r instanceof Ri?r:r?.injector;o&&this.componentDef.getStandaloneInjector!==null&&(o=this.componentDef.getStandaloneInjector(o)||o);let a=o?new Su(e,o):e,l=a.get(cs,null);if(l===null)throw new qe(407,!1);let c=a.get(Cb,null),u=a.get(ls,null),d={rendererFactory:l,sanitizer:c,inlineEffectRunner:null,changeDetectionScheduler:u},h=l.createRenderer(null,this.componentDef),f=this.componentDef.selectors[0][0]||"div",g=i?LE(h,i,this.componentDef.encapsulation,a):pv(h,f,Db(f)),_=512;this.componentDef.signals?_|=4096:this.componentDef.onPush||(_|=16);let m=null;g!==null&&(m=Rd(g,a,!0));let p=kd(0,null,null,1,0,null,null,null,null,null,null),w=ul(null,p,null,_,null,null,d,h,a,null,m);_d(w);let E,S,k=null;try{let R=this.componentDef,D,V=null;R.findHostDirectiveDefs?(D=[],V=new Map,R.findHostDirectiveDefs(R,D,V),D.push(R)):D=[R];let re=Tb(w,g);k=Ab(re,g,R,D,w,d,h),S=gd(p,bn),g&&Nb(h,R,g,i),t!==void 0&&Pb(S,this.ngContentSelectors,t),E=Rb(k,R,D,V,w,[Fb]),Vd(p,w,null)}catch(R){throw k!==null&&Pu(k),Pu(w),R}finally{xd()}return new Gu(this.componentType,E,ms(S,w),w,S)}finally{Ve(s)}}},Gu=class extends Vu{constructor(e,t,i,r,s){super(),this.location=i,this._rootLView=r,this._tNode=s,this.previousInputValues=null,this.instance=t,this.hostView=this.changeDetectorRef=new pr(r,void 0,!1),this.componentType=e}setInput(e,t){let i=this._tNode.inputs,r;if(i!==null&&(r=i[e])){if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let s=this._rootLView;Ud(s[Pe],s,r,e,t),this.previousInputValues.set(e,t);let o=Pi(this._tNode.index,s);Bd(o,1)}}get injector(){return new or(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function Tb(n,e){let t=n[Pe],i=bn;return n[i]=e,dl(t,i,2,"#host",null)}function Ab(n,e,t,i,r,s,o){let a=r[Pe];Ib(i,n,e,o);let l=null;e!==null&&(l=Rd(e,r[rs]));let c=s.rendererFactory.createRenderer(e,t),u=16;t.signals?u=4096:t.onPush&&(u=64);let d=ul(r,Av(t),null,u,r[n.index],n,s,c,null,null,l);return a.firstCreatePass&&Lu(a,n,i.length-1),hl(r,d),r[n.index]=d}function Ib(n,e,t,i){for(let r of n)e.mergedAttrs=lo(e.mergedAttrs,r.hostAttrs);e.mergedAttrs!==null&&(Hu(e,e.mergedAttrs,!0),t!==null&&Mv(i,t,e))}function Rb(n,e,t,i,r,s){let o=sn(),a=r[Pe],l=wn(o,r);Nv(a,r,o,t,null,i);for(let u=0;u<t.length;u++){let d=o.directiveStart+u,h=hr(r,a,d,o);fr(h,r)}Pv(a,r,o),l&&fr(l,r);let c=hr(r,a,o.directiveStart+o.componentOffset,o);if(n[zt]=r[zt]=c,s!==null)for(let u of s)u(c,e);return Cv(a,o,r),c}function Nb(n,e,t,i){if(i)gu(n,t,["ng-version","18.2.14"]);else{let{attrs:r,classes:s}=Lx(e.selectors[0]);r&&gu(n,t,r),s&&s.length>0&&xv(n,t,s.join(" "))}}function Pb(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null?Array.from(s):null)}}function Fb(){let n=sn();Sd(Ke()[Pe],n)}var Gd=(()=>{class n{static{this.__NG_ELEMENT_ID__=Ob}}return n})();function Ob(){let n=sn();return Kv(n,Ke())}var Lb=Gd,Zv=class extends Lb{constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return ms(this._hostTNode,this._hostLView)}get injector(){return new or(this._hostTNode,this._hostLView)}get parentInjector(){let e=wd(this._hostTNode,this._hostLView);if(jg(e)){let t=Fa(e,this._hostLView),i=Pa(e),r=t[Pe].data[i+8];return new or(r,t)}else return new or(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=Wm(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-Gt}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=po(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,fo(this._hostTNode,o)),a}createComponent(e,t,i,r,s){let o=e&&!Qx(e),a;if(o)a=t;else{let g=t||{};a=g.index,i=g.injector,r=g.projectableNodes,s=g.environmentInjector||g.ngModuleRef}let l=o?e:new Ga(ns(e)),c=i||this.parentInjector;if(!s&&l.ngModule==null){let _=(o?c:this.parentInjector).get(Ri,null);_&&(s=_)}let u=ns(l.componentType??{}),d=po(this._lContainer,u?.id??null),h=d?.firstChild??null,f=l.create(c,r,h,s);return this.insertImpl(f.hostView,a,fo(this._hostTNode,d)),f}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(aM(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let l=r[Wt],c=new Zv(l,l[On],l[Wt]);c.detach(c.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return pl(o,r,s,i),e.attachToViewContainerRef(),ag(lu(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=Wm(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=ho(this._lContainer,t);i&&(Da(lu(this._lContainer),t),ll(i[Pe],i))}detach(e){let t=this._adjustIndex(e,-1),i=ho(this._lContainer,t);return i&&Da(lu(this._lContainer),t)!=null?new pr(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function Wm(n){return n[Ra]}function lu(n){return n[Ra]||(n[Ra]=[])}function Kv(n,e){let t,i=e[n.index];return ai(i)?t=i:(t=Fv(i,e,null,n),e[n.index]=t,hl(e,t)),Ub(t,e,n,i),new Zv(t,n,e)}function kb(n,e){let t=n[Ot],i=t.createComment(""),r=wn(e,n),s=yv(t,r);return Va(t,s,i,SE(t,r),!1),i}var Ub=Hb,Vb=()=>!1;function Bb(n,e,t){return Vb(n,e,t)}function Hb(n,e,t,i){if(n[lr])return;let r;t.type&8?r=Yn(i):r=kb(e,t),n[lr]=r}var Wu=class n{constructor(e){this.queryList=e,this.matches=null}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},ju=class n{constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)Wd(e,t).matches!==null&&this.queries[t].setDirty()}},$u=class{constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=Yb(e):this.predicate=e}},qu=class n{constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},Xu=class n{constructor(e,t=-1){this.metadata=e,this.matches=null,this.indexInDeclarationView=-1,this.crossesNgTemplate=!1,this._appliesToNextNode=!0,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,zb(t,s)),this.matchTNodeWithReadOption(e,t,Ea(t,e,s,!1,!1))}else i===Ha?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,Ea(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===Cn||r===Gd||r===Ha&&t.type&4)this.addMatch(t.index,-2);else{let s=Ea(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function zb(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function Gb(n,e){return n.type&11?ms(n,e):n.type&4?Hd(n,e):null}function Wb(n,e,t,i){return t===-1?Gb(e,n):t===-2?jb(n,e,i):hr(n,n[Pe],t,e)}function jb(n,e,t){if(t===Cn)return ms(e,n);if(t===Ha)return Hd(e,n);if(t===Gd)return Kv(e,n)}function Jv(n,e,t,i){let r=e[ni].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let l=0;o!==null&&l<o.length;l+=2){let c=o[l];if(c<0)a.push(null);else{let u=s[c];a.push(Wb(e,u,o[l+1],t.metadata.read))}}r.matches=a}return r.matches}function Yu(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=Jv(n,e,r,t);for(let a=0;a<s.length;a+=2){let l=s[a];if(l>0)i.push(o[a/2]);else{let c=s[a+1],u=e[-l];for(let d=Gt;d<u.length;d++){let h=u[d];h[ar]===h[Wt]&&Yu(h[Pe],h,c,i)}if(u[ss]!==null){let d=u[ss];for(let h=0;h<d.length;h++){let f=d[h];Yu(f[Pe],f,c,i)}}}}}return i}function $b(n,e){return n[ni].queries[e].queryList}function qb(n,e,t){let i=new Nu((t&4)===4);return VE(n,e,i,i.destroy),(e[ni]??=new ju).queries.push(new Wu(i))-1}function Xb(n,e,t){let i=jt();return i.firstCreatePass&&(Zb(i,new $u(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),qb(i,Ke(),e)}function Yb(n){return n.split(",").map(e=>e.trim())}function Zb(n,e,t){n.queries===null&&(n.queries=new qu),n.queries.track(new Xu(e,t))}function Wd(n,e){return n.queries.getByIndex(e)}function Kb(n,e){let t=n[Pe],i=Wd(t,e);return i.crossesNgTemplate?Yu(t,n,e,[]):Jv(t,n,i,e)}var jm=new Set;function vs(n){jm.has(n)||(jm.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}function Jb(n){return typeof n=="function"&&n[ti]!==void 0}function $t(n,e){vs("NgSignals");let t=Wp(n),i=t[ti];return e?.equal&&(i.equal=e.equal),t.set=r=>Wc(i,r),t.update=r=>jp(i,r),t.asReadonly=Qb.bind(t),t}function Qb(){let n=this[ti];if(n.readonlyFn===void 0){let e=()=>this();e[ti]=n,n.readonlyFn=e}return n.readonlyFn}function Qv(n){return Jb(n)&&typeof n.set=="function"}function eS(n){return Object.getPrototypeOf(n.prototype).constructor}function fn(n){let e=eS(n.type),t=!0,i=[n];for(;e;){let r;if(Ni(n))r=e.\u0275cmp||e.\u0275dir;else{if(e.\u0275cmp)throw new qe(903,!1);r=e.\u0275dir}if(r){if(t){i.push(r);let o=n;o.inputs=ga(n.inputs),o.inputTransforms=ga(n.inputTransforms),o.declaredInputs=ga(n.declaredInputs),o.outputs=ga(n.outputs);let a=r.hostBindings;a&&sS(n,a);let l=r.viewQuery,c=r.contentQueries;if(l&&iS(n,l),c&&rS(n,c),tS(n,r),ex(n.outputs,r.outputs),Ni(r)&&r.data.animation){let u=n.data;u.animation=(u.animation||[]).concat(r.data.animation)}}let s=r.features;if(s)for(let o=0;o<s.length;o++){let a=s[o];a&&a.ngInherit&&a(n),a===fn&&(t=!1)}}e=Object.getPrototypeOf(e)}nS(i)}function tS(n,e){for(let t in e.inputs){if(!e.inputs.hasOwnProperty(t)||n.inputs.hasOwnProperty(t))continue;let i=e.inputs[t];if(i!==void 0&&(n.inputs[t]=i,n.declaredInputs[t]=e.declaredInputs[t],e.inputTransforms!==null)){let r=Array.isArray(i)?i[0]:i;if(!e.inputTransforms.hasOwnProperty(r))continue;n.inputTransforms??={},n.inputTransforms[r]=e.inputTransforms[r]}}}function nS(n){let e=0,t=null;for(let i=n.length-1;i>=0;i--){let r=n[i];r.hostVars=e+=r.hostVars,r.hostAttrs=lo(r.hostAttrs,t=lo(t,r.hostAttrs))}}function ga(n){return n===ts?{}:n===xn?[]:n}function iS(n,e){let t=n.viewQuery;t?n.viewQuery=(i,r)=>{e(i,r),t(i,r)}:n.viewQuery=e}function rS(n,e){let t=n.contentQueries;t?n.contentQueries=(i,r,s)=>{e(i,r,s),t(i,r,s)}:n.contentQueries=e}function sS(n,e){let t=n.hostBindings;t?n.hostBindings=(i,r)=>{e(i,r),t(i,r)}:n.hostBindings=e}var us=class{};var Wa=class extends us{constructor(e){super(),this.componentFactoryResolver=new zu(this),this.instance=null;let t=new Aa([...e.providers,{provide:us,useValue:this},{provide:mo,useValue:this.componentFactoryResolver}],e.parent||pd(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function oS(n,e,t=null){return new Wa({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}function si(n,e,t){let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function aS(n,e,t,i){let r=si(n,e,t);return si(n,e+1,i)||r}function lS(n){return(n.flags&32)===32}function cS(n,e,t,i,r,s,o,a,l){let c=e.consts,u=dl(e,n,4,o||null,a||null);Rv(e,t,u,os(c,l)),Sd(e,u);let d=u.tView=kd(2,u,i,r,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,c,null);return e.queries!==null&&(e.queries.template(e,u),d.queries=e.queries.embeddedTView(u)),u}function Zu(n,e,t,i,r,s,o,a,l,c){let u=t+bn,d=e.firstCreatePass?cS(u,e,n,i,r,s,o,a,l):e.data[u];yo(d,!1);let h=uS(e,n,d,t);Ed()&&Od(e,n,h,d),fr(h,n);let f=Fv(h,n,h,d);return n[u]=f,hl(n,f),Bb(f,d,n),md(d)&&Dv(e,n,d),l!=null&&Tv(n,d,c),d}function Ct(n,e,t,i,r,s,o,a){let l=Ke(),c=jt(),u=os(c.consts,s);return Zu(l,c,n,e,t,i,r,u,o,a),Ct}var uS=dS;function dS(n,e,t,i){return bd(!0),e[Ot].createComment("")}var ro=function(n){return n[n.EarlyRead=0]="EarlyRead",n[n.Write=1]="Write",n[n.MixedReadWrite=2]="MixedReadWrite",n[n.Read=3]="Read",n}(ro||{}),hS=(()=>{class n{constructor(){this.impl=null}execute(){this.impl?.execute()}static{this.\u0275prov=At({token:n,providedIn:"root",factory:()=>new n})}}return n})(),$m=class n{constructor(){this.ngZone=ot(wt),this.scheduler=ot(ls),this.errorHandler=ot(ii,{optional:!0}),this.sequences=new Set,this.deferredRegistrations=new Set,this.executing=!1}static{this.PHASES=[ro.EarlyRead,ro.Write,ro.MixedReadWrite,ro.Read]}execute(){this.executing=!0;for(let e of n.PHASES)for(let t of this.sequences)if(!(t.erroredOrDestroyed||!t.hooks[e]))try{t.pipelinedValue=this.ngZone.runOutsideAngular(()=>t.hooks[e](t.pipelinedValue))}catch(i){t.erroredOrDestroyed=!0,this.errorHandler?.handleError(i)}this.executing=!1;for(let e of this.sequences)e.afterRun(),e.once&&(this.sequences.delete(e),e.destroy());for(let e of this.deferredRegistrations)this.sequences.add(e);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear()}register(e){this.executing?this.deferredRegistrations.add(e):(this.sequences.add(e),this.scheduler.notify(6))}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}static{this.\u0275prov=At({token:n,providedIn:"root",factory:()=>new n})}};function pn(n,e,t,i){let r=Ke(),s=ps();if(si(r,s,e)){let o=jt(),a=Md();eb(a,r,n,e,t,i)}return pn}function fS(n,e,t,i){return si(n,ps(),t)?e+Qr(t)+i:ci}function pS(n,e,t,i,r,s){let o=_M(),a=aS(n,o,t,r);return Lg(2),a?e+Qr(t)+i+Qr(r)+s:ci}function va(n,e){return n<<17|e<<2}function mr(n){return n>>17&32767}function mS(n){return(n&2)==2}function gS(n,e){return n&131071|e<<17}function Ku(n){return n|2}function ds(n){return(n&131068)>>2}function cu(n,e){return n&-131069|e<<2}function vS(n){return(n&1)===1}function Ju(n){return n|1}function yS(n,e,t,i,r,s){let o=s?e.classBindings:e.styleBindings,a=mr(o),l=ds(o);n[i]=t;let c=!1,u;if(Array.isArray(t)){let d=t;u=d[1],(u===null||vo(d,u)>0)&&(c=!0)}else u=t;if(r)if(l!==0){let h=mr(n[a+1]);n[i+1]=va(h,a),h!==0&&(n[h+1]=cu(n[h+1],i)),n[a+1]=gS(n[a+1],i)}else n[i+1]=va(a,0),a!==0&&(n[a+1]=cu(n[a+1],i)),a=i;else n[i+1]=va(l,0),a===0?a=i:n[l+1]=cu(n[l+1],i),l=i;c&&(n[i+1]=Ku(n[i+1])),qm(n,u,i,!0),qm(n,u,i,!1),_S(e,u,n,i,s),o=va(a,l),s?e.classBindings=o:e.styleBindings=o}function _S(n,e,t,i,r){let s=r?n.residualClasses:n.residualStyles;s!=null&&typeof e=="string"&&vo(s,e)>=0&&(t[i+1]=Ju(t[i+1]))}function qm(n,e,t,i){let r=n[t+1],s=e===null,o=i?mr(r):ds(r),a=!1;for(;o!==0&&(a===!1||s);){let l=n[o],c=n[o+1];xS(l,e)&&(a=!0,n[o+1]=i?Ju(c):Ku(c)),o=i?mr(c):ds(c)}a&&(n[t+1]=i?Ku(r):Ju(r))}function xS(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?vo(n,e)>=0:!1}function pt(n,e,t){let i=Ke(),r=ps();if(si(i,r,e)){let s=jt(),o=Md();Iv(s,o,i,n,e,i[Ot],t,!1)}return pt}function Xm(n,e,t,i,r){let s=e.inputs,o=r?"class":"style";Ud(n,t,s[o],o,i)}function jd(n,e,t){return e0(n,e,t,!1),jd}function Oi(n,e){return e0(n,e,null,!0),Oi}function e0(n,e,t,i){let r=Ke(),s=jt(),o=Lg(2);if(s.firstUpdatePass&&ES(s,n,o,i),e!==ci&&si(r,o,e)){let a=s.data[Fi()];DS(s,a,r,r[Ot],n,r[o+1]=TS(e,t),i,o)}}function MS(n,e){return e>=n.expandoStartIndex}function ES(n,e,t,i){let r=n.data;if(r[t+1]===null){let s=r[Fi()],o=MS(n,t);AS(s,i)&&e===null&&!o&&(e=!1),e=bS(r,s,e,i),yS(r,s,e,t,o,i)}}function bS(n,e,t,i){let r=SM(n),s=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=uu(null,n,e,t,i),t=go(t,e.attrs,i),s=null);else{let o=e.directiveStylingLast;if(o===-1||n[o]!==r)if(t=uu(r,n,e,t,i),s===null){let l=SS(n,e,i);l!==void 0&&Array.isArray(l)&&(l=uu(null,n,e,l[1],i),l=go(l,e.attrs,i),wS(n,e,i,l))}else s=CS(n,e,i)}return s!==void 0&&(i?e.residualClasses=s:e.residualStyles=s),t}function SS(n,e,t){let i=t?e.classBindings:e.styleBindings;if(ds(i)!==0)return n[mr(i)]}function wS(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[mr(r)]=i}function CS(n,e,t){let i,r=e.directiveEnd;for(let s=1+e.directiveStylingLast;s<r;s++){let o=n[s].hostAttrs;i=go(i,o,t)}return go(i,e.attrs,t)}function uu(n,e,t,i,r){let s=null,o=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<o&&(s=e[a],i=go(i,s.hostAttrs,r),s!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function go(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let s=0;s<e.length;s++){let o=e[s];typeof o=="number"?r=o:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),Ex(n,o,t?!0:e[++s]))}return n===void 0?null:n}function DS(n,e,t,i,r,s,o,a){if(!(e.type&3))return;let l=n.data,c=l[a+1],u=vS(c)?Ym(l,e,t,r,ds(c),o):void 0;if(!ja(u)){ja(s)||mS(c)&&(s=Ym(l,null,t,r,a,o));let d=Dg(Fi(),t);RE(i,o,d,r,s)}}function Ym(n,e,t,i,r,s){let o=e===null,a;for(;r>0;){let l=n[r],c=Array.isArray(l),u=c?l[1]:l,d=u===null,h=t[r+1];h===ci&&(h=d?xn:void 0);let f=d?eu(h,i):u===i?h:void 0;if(c&&!ja(f)&&(f=eu(l,i)),ja(f)&&(a=f,o))return a;let g=n[r+1];r=o?mr(g):ds(g)}if(e!==null){let l=s?e.residualClasses:e.residualStyles;l!=null&&(a=eu(l,i))}return a}function ja(n){return n!==void 0}function TS(n,e){return n==null||n===""||(typeof e=="string"?n=n+e:typeof n=="object"&&(n=Mn(ol(n)))),n}function AS(n,e){return(n.flags&(e?8:16))!==0}var Qu=class{destroy(e){}updateValue(e,t){}swap(e,t){let i=Math.min(e,t),r=Math.max(e,t),s=this.detach(r);if(r-i>1){let o=this.detach(i);this.attach(i,s),this.attach(r,o)}else this.attach(i,s)}move(e,t){this.attach(t,this.detach(e))}};function du(n,e,t,i,r){return n===t&&Object.is(e,i)?1:Object.is(r(n,e),r(t,i))?-1:0}function IS(n,e,t){let i,r,s=0,o=n.length-1,a=void 0;if(Array.isArray(e)){let l=e.length-1;for(;s<=o&&s<=l;){let c=n.at(s),u=e[s],d=du(s,c,s,u,t);if(d!==0){d<0&&n.updateValue(s,u),s++;continue}let h=n.at(o),f=e[l],g=du(o,h,l,f,t);if(g!==0){g<0&&n.updateValue(o,f),o--,l--;continue}let _=t(s,c),m=t(o,h),p=t(s,u);if(Object.is(p,m)){let w=t(l,f);Object.is(w,_)?(n.swap(s,o),n.updateValue(o,f),l--,o--):n.move(o,s),n.updateValue(s,u),s++;continue}if(i??=new $a,r??=Km(n,s,o,t),ed(n,i,s,p))n.updateValue(s,u),s++,o++;else if(r.has(p))i.set(_,n.detach(s)),o--;else{let w=n.create(s,e[s]);n.attach(s,w),s++,o++}}for(;s<=l;)Zm(n,i,t,s,e[s]),s++}else if(e!=null){let l=e[Symbol.iterator](),c=l.next();for(;!c.done&&s<=o;){let u=n.at(s),d=c.value,h=du(s,u,s,d,t);if(h!==0)h<0&&n.updateValue(s,d),s++,c=l.next();else{i??=new $a,r??=Km(n,s,o,t);let f=t(s,d);if(ed(n,i,s,f))n.updateValue(s,d),s++,o++,c=l.next();else if(!r.has(f))n.attach(s,n.create(s,d)),s++,o++,c=l.next();else{let g=t(s,u);i.set(g,n.detach(s)),o--}}}for(;!c.done;)Zm(n,i,t,n.length,c.value),c=l.next()}for(;s<=o;)n.destroy(n.detach(o--));i?.forEach(l=>{n.destroy(l)})}function ed(n,e,t,i){return e!==void 0&&e.has(i)?(n.attach(t,e.get(i)),e.delete(i),!0):!1}function Zm(n,e,t,i,r){if(ed(n,e,i,t(i,r)))n.updateValue(i,r);else{let s=n.create(i,r);n.attach(i,s)}}function Km(n,e,t,i){let r=new Set;for(let s=e;s<=t;s++)r.add(i(s,n.at(s)));return r}var $a=class{constructor(){this.kvMap=new Map,this._vMap=void 0}has(e){return this.kvMap.has(e)}delete(e){if(!this.has(e))return!1;let t=this.kvMap.get(e);return this._vMap!==void 0&&this._vMap.has(t)?(this.kvMap.set(e,this._vMap.get(t)),this._vMap.delete(t)):this.kvMap.delete(e),!0}get(e){return this.kvMap.get(e)}set(e,t){if(this.kvMap.has(e)){let i=this.kvMap.get(e);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,t)}else this.kvMap.set(e,t)}forEach(e){for(let[t,i]of this.kvMap)if(e(i,t),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),e(i,t)}}};function _t(n,e){vs("NgControlFlow");let t=Ke(),i=ps(),r=t[i]!==ci?t[i]:-1,s=r!==-1?qa(t,bn+r):void 0,o=0;if(si(t,i,n)){let a=Ve(null);try{if(s!==void 0&&Hv(s,o),n!==-1){let l=bn+n,c=qa(t,l),u=rd(t[Pe],l),d=po(c,u.tView.ssrId),h=fl(t,u,e,{dehydratedView:d});pl(c,h,o,fo(u,d))}}finally{Ve(a)}}else if(s!==void 0){let a=Bv(s,o);a!==void 0&&(a[zt]=e)}}var td=class{constructor(e,t,i){this.lContainer=e,this.$implicit=t,this.$index=i}get $count(){return this.lContainer.length-Gt}};function t0(n){return n}var nd=class{constructor(e,t,i){this.hasEmptyBlock=e,this.trackByFn=t,this.liveCollection=i}};function mn(n,e,t,i,r,s,o,a,l,c,u,d,h){vs("NgControlFlow");let f=Ke(),g=jt(),_=l!==void 0,m=Ke(),p=a?o.bind(m[Fn][zt]):o,w=new nd(_,p);m[bn+n]=w,Zu(f,g,n+1,e,t,i,r,os(g.consts,s)),_&&Zu(f,g,n+2,l,c,u,d,os(g.consts,h))}var id=class extends Qu{constructor(e,t,i){super(),this.lContainer=e,this.hostLView=t,this.templateTNode=i,this.operationsCounter=void 0,this.needsIndexUpdate=!1}get length(){return this.lContainer.length-Gt}at(e){return this.getLView(e)[zt].$implicit}attach(e,t){let i=t[co];this.needsIndexUpdate||=e!==this.length,pl(this.lContainer,t,e,fo(this.templateTNode,i))}detach(e){return this.needsIndexUpdate||=e!==this.length-1,RS(this.lContainer,e)}create(e,t){let i=po(this.lContainer,this.templateTNode.tView.ssrId),r=fl(this.hostLView,this.templateTNode,new td(this.lContainer,t,e),{dehydratedView:i});return this.operationsCounter?.recordCreate(),r}destroy(e){ll(e[Pe],e),this.operationsCounter?.recordDestroy()}updateValue(e,t){this.getLView(e)[zt].$implicit=t}reset(){this.needsIndexUpdate=!1,this.operationsCounter?.reset()}updateIndexes(){if(this.needsIndexUpdate)for(let e=0;e<this.length;e++)this.getLView(e)[zt].$index=e}getLView(e){return NS(this.lContainer,e)}};function gn(n){let e=Ve(null),t=Fi();try{let i=Ke(),r=i[Pe],s=i[t],o=t+1,a=qa(i,o);if(s.liveCollection===void 0){let c=rd(r,o);s.liveCollection=new id(a,i,c)}else s.liveCollection.reset();let l=s.liveCollection;if(IS(l,n,s.trackByFn),l.updateIndexes(),s.hasEmptyBlock){let c=ps(),u=l.length===0;if(si(i,c,u)){let d=t+2,h=qa(i,d);if(u){let f=rd(r,d),g=po(h,f.tView.ssrId),_=fl(i,f,void 0,{dehydratedView:g});pl(h,_,0,fo(f,g))}else Hv(h,0)}}}finally{Ve(e)}}function qa(n,e){return n[e]}function RS(n,e){return ho(n,e)}function NS(n,e){return Bv(n,e)}function rd(n,e){return gd(n,e)}function PS(n,e,t,i,r,s){let o=e.consts,a=os(o,r),l=dl(e,n,2,i,a);return Rv(e,t,l,os(o,s)),l.attrs!==null&&Hu(l,l.attrs,!1),l.mergedAttrs!==null&&Hu(l,l.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,l),l}function T(n,e,t,i){let r=Ke(),s=jt(),o=bn+n,a=r[Ot],l=s.firstCreatePass?PS(o,s,r,e,t,i):s.data[o],c=FS(s,r,l,a,e,n);r[o]=c;let u=md(l);return yo(l,!0),Mv(a,c,l),!lS(l)&&Ed()&&Od(s,r,c,l),uM()===0&&fr(c,r),dM(),u&&(Dv(s,r,l),Cv(s,l,r)),i!==null&&Tv(r,l),T}function C(){let n=sn();Fg()?vM():(n=n.parent,yo(n,!1));let e=n;pM(e)&&mM(),hM();let t=jt();return t.firstCreatePass&&(Sd(t,n),Eg(n)&&t.queries.elementEnd(n)),e.classesWithoutHost!=null&&RM(e)&&Xm(t,e,Ke(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&NM(e)&&Xm(t,e,Ke(),e.stylesWithoutHost,!1),C}function Kt(n,e,t,i){return T(n,e,t,i),C(),Kt}var FS=(n,e,t,i,r,s)=>(bd(!0),pv(i,r,DM()));function Jt(){return Ke()}var Xa="en-US";var OS=Xa;function LS(n){typeof n=="string"&&(OS=n.toLowerCase().replace(/_/g,"-"))}var kS=(n,e,t)=>{};function He(n,e,t,i){let r=Ke(),s=jt(),o=sn();return n0(s,r,r[Ot],o,n,e,i),He}function US(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[Ia],l=r[s+2];return a.length>l?a[l]:null}typeof o=="string"&&(s+=2)}return null}function n0(n,e,t,i,r,s,o){let a=md(i),c=n.firstCreatePass&&kv(n),u=e[zt],d=Lv(e),h=!0;if(i.type&3||o){let _=wn(i,e),m=o?o(_):_,p=d.length,w=o?S=>o(Yn(S[i.index])):i.index,E=null;if(!o&&a&&(E=US(n,e,r,i.index)),E!==null){let S=E.__ngLastListenerFn__||E;S.__ngNextListenerFn__=s,E.__ngLastListenerFn__=s,h=!1}else{s=Qm(i,e,u,s),kS(_,r,s);let S=t.listen(m,r,s);d.push(s,S),c&&c.push(r,w,p,p+1)}}else s=Qm(i,e,u,s);let f=i.outputs,g;if(h&&f!==null&&(g=f[r])){let _=g.length;if(_)for(let m=0;m<_;m+=2){let p=g[m],w=g[m+1],k=e[p][w].subscribe(s),R=d.length;d.push(s,k),c&&c.push(r,i.index,R,-(R+1))}}}function Jm(n,e,t,i){let r=Ve(null);try{return jn(6,e,t),t(i)!==!1}catch(s){return Uv(n,s),!1}finally{jn(7,e,t),Ve(r)}}function Qm(n,e,t,i){return function r(s){if(s===Function)return i;let o=n.componentOffset>-1?Pi(n.index,e):e;Bd(o,5);let a=Jm(e,t,i,s),l=r.__ngNextListenerFn__;for(;l;)a=Jm(e,t,l,s)&&a,l=l.__ngNextListenerFn__;return a}}function Se(n=1){return CM(n)}function i0(n,e,t){Xb(n,e,t)}function r0(n){let e=Ke(),t=jt(),i=kg();yd(i+1);let r=Wd(t,i);if(n.dirty&&oM(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let s=Kb(e,i);n.reset(s,QM),n.notifyOnChanges()}return!0}return!1}function s0(){return $b(Ke(),kg())}function gr(n){let e=yM();return sM(e,bn+n)}function I(n,e=""){let t=Ke(),i=jt(),r=n+bn,s=i.firstCreatePass?dl(i,r,1,e,null):i.data[r],o=VS(i,t,s,e,n);t[r]=o,Ed()&&Od(i,t,o,s),yo(s,!1)}var VS=(n,e,t,i,r)=>(bd(!0),pE(e[Ot],i));function xt(n){return ui("",n,""),xt}function ui(n,e,t){let i=Ke(),r=fS(i,n,e,t);return r!==ci&&Vv(i,Fi(),r),ui}function _o(n,e,t,i,r){let s=Ke(),o=pS(s,n,e,t,i,r);return o!==ci&&Vv(s,Fi(),o),_o}function Li(n,e,t){Qv(e)&&(e=e());let i=Ke(),r=ps();if(si(i,r,e)){let s=jt(),o=Md();Iv(s,o,i,n,e,i[Ot],t,!1)}return Li}function vr(n,e){let t=Qv(n);return t&&n.set(e),t}function ki(n,e){let t=Ke(),i=jt(),r=sn();return n0(i,t,t[Ot],r,n,e),ki}function BS(n,e,t){let i=jt();if(i.firstCreatePass){let r=Ni(n);sd(t,i.data,i.blueprint,r,!0),sd(e,i.data,i.blueprint,r,!1)}}function sd(n,e,t,i,r){if(n=Zt(n),Array.isArray(n))for(let s=0;s<n.length;s++)sd(n[s],e,t,i,r);else{let s=jt(),o=Ke(),a=sn(),l=is(n)?n:Zt(n.provide),c=_g(n),u=a.providerIndexes&1048575,d=a.directiveStart,h=a.providerIndexes>>20;if(is(n)||!n.multi){let f=new dr(c,r,Et),g=fu(l,e,r?u:u+h,d);g===-1?(Cu(Oa(a,o),s,l),hu(s,n,e.length),e.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(f),o.push(f)):(t[g]=f,o[g]=f)}else{let f=fu(l,e,u+h,d),g=fu(l,e,u,u+h),_=f>=0&&t[f],m=g>=0&&t[g];if(r&&!m||!r&&!_){Cu(Oa(a,o),s,l);let p=GS(r?zS:HS,t.length,r,i,c);!r&&m&&(t[g].providerFactory=p),hu(s,n,e.length,0),e.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(p),o.push(p)}else{let p=o0(t[r?g:f],c,!r&&i);hu(s,n,f>-1?f:g,p)}!r&&i&&m&&t[g].componentProviders++}}}function hu(n,e,t,i){let r=is(e),s=jx(e);if(r||s){let l=(s?Zt(e.useClass):e).prototype.ngOnDestroy;if(l){let c=n.destroyHooks||(n.destroyHooks=[]);if(!r&&e.multi){let u=c.indexOf(t);u===-1?c.push(t,[i,l]):c[u+1].push(i,l)}else c.push(t,l)}}}function o0(n,e,t){return t&&n.componentProviders++,n.multi.push(e)-1}function fu(n,e,t,i){for(let r=t;r<i;r++)if(e[r]===n)return r;return-1}function HS(n,e,t,i){return od(this.multi,[])}function zS(n,e,t,i){let r=this.multi,s;if(this.providerFactory){let o=this.providerFactory.componentProviders,a=hr(t,t[Pe],this.providerFactory.index,i);s=a.slice(0,o),od(r,s);for(let l=o;l<a.length;l++)s.push(a[l])}else s=[],od(r,s);return s}function od(n,e){for(let t=0;t<n.length;t++){let i=n[t];e.push(i())}return e}function GS(n,e,t,i,r){let s=new dr(n,t,Et);return s.multi=[],s.index=e,s.componentProviders=0,o0(s,r,i&&!t),s}function Zn(n,e=[]){return t=>{t.providersResolver=(i,r)=>BS(i,r?r(n):n,e)}}var WS=(()=>{class n{constructor(t){this._injector=t,this.cachedInjectors=new Map}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=gg(!1,t.type),r=i.length>0?oS([i],this._injector,`Standalone[${t.type.name}]`):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static{this.\u0275prov=At({token:n,providedIn:"environment",factory:()=>new n(ft(Ri))})}}return n})();function ys(n){vs("NgStandalone"),n.getStandaloneInjector=e=>e.get(WS).getOrCreateStandaloneInjector(n)}var a0=new ze("");function xo(n){return!!n&&typeof n.then=="function"}function l0(n){return!!n&&typeof n.subscribe=="function"}var jS=new ze(""),c0=(()=>{class n{constructor(){this.initialized=!1,this.done=!1,this.donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i}),this.appInits=ot(jS,{optional:!0})??[]}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=r();if(xo(s))t.push(s);else if(l0(s)){let o=new Promise((a,l)=>{s.subscribe({complete:a,error:l})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=At({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})(),$S=new ze("");function qS(){Gp(()=>{throw new qe(600,!1)})}function XS(n){return n.isBoundToModule}var YS=10;function ZS(n,e,t){try{let i=t();return xo(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n.handleError(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n.handleError(i)),i}}var Mo=(()=>{class n{constructor(){this._bootstrapListeners=[],this._runningTick=!1,this._destroyed=!1,this._destroyListeners=[],this._views=[],this.internalErrorHandler=ot(KM),this.afterRenderManager=ot(hS),this.zonelessEnabled=ot(zd),this.dirtyFlags=0,this.deferredDirtyFlags=0,this.externalTestViews=new Set,this.beforeRender=new zn,this.afterTick=new zn,this.componentTypes=[],this.components=[],this.isStable=ot(sl).hasPendingTasks.pipe(sr(t=>!t)),this._injector=ot(Ri)}get allViews(){return[...this.externalTestViews.keys(),...this._views]}get destroyed(){return this._destroyed}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}get injector(){return this._injector}bootstrap(t,i){let r=t instanceof za;if(!this._injector.get(c0).done){let h=!r&&Vx(t),f=!1;throw new qe(405,f)}let o;r?o=t:o=this._injector.get(mo).resolveComponentFactory(t),this.componentTypes.push(o.componentType);let a=XS(o)?void 0:this._injector.get(us),l=i||o.selector,c=o.create(as.NULL,[],l,a),u=c.location.nativeElement,d=c.injector.get(a0,null);return d?.registerApplication(u),c.onDestroy(()=>{this.detachView(c.hostView),ba(this.components,c),d?.unregisterApplication(u)}),this._loadComponent(c),c}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){if(this._runningTick)throw new qe(101,!1);let t=Ve(null);try{this._runningTick=!0,this.synchronize()}catch(i){this.internalErrorHandler(i)}finally{this._runningTick=!1,Ve(t),this.afterTick.next()}}synchronize(){let t=null;this._injector.destroyed||(t=this._injector.get(cs,null,{optional:!0})),this.dirtyFlags|=this.deferredDirtyFlags,this.deferredDirtyFlags=0;let i=0;for(;this.dirtyFlags!==0&&i++<YS;)this.synchronizeOnce(t)}synchronizeOnce(t){if(this.dirtyFlags|=this.deferredDirtyFlags,this.deferredDirtyFlags=0,this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8,this.beforeRender.next(i);for(let{_lView:r,notifyErrorHandler:s}of this._views)KS(r,s,i,this.zonelessEnabled);if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&7)return}else t?.begin?.(),t?.end?.();this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>il(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;ba(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView),this.tick(),this.components.push(t);let i=this._injector.get($S,[]);[...this._bootstrapListeners,...i].forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._bootstrapListeners=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>ba(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new qe(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}warnIfDestroyed(){}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=At({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})();function ba(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function KS(n,e,t,i){if(!t&&!il(n))return;Wv(n,e,t&&!i?0:1)}var JS=(()=>{class n{constructor(){this.zone=ot(wt),this.changeDetectionScheduler=ot(ls),this.applicationRef=ot(Mo)}initialize(){this._onMicrotaskEmptySubscription||(this._onMicrotaskEmptySubscription=this.zone.onMicrotaskEmpty.subscribe({next:()=>{this.changeDetectionScheduler.runningTick||this.zone.run(()=>{this.applicationRef.tick()})}}))}ngOnDestroy(){this._onMicrotaskEmptySubscription?.unsubscribe()}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=At({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})();function QS({ngZoneFactory:n,ignoreChangesOutsideZone:e,scheduleInRootZone:t}){return n??=()=>new wt(St(vt({},ew()),{scheduleInRootZone:t})),[{provide:wt,useFactory:n},{provide:ao,multi:!0,useFactory:()=>{let i=ot(JS,{optional:!0});return()=>i.initialize()}},{provide:ao,multi:!0,useFactory:()=>{let i=ot(tw);return()=>{i.initialize()}}},e===!0?{provide:Xv,useValue:!0}:[],{provide:Yv,useValue:t??ev}]}function ew(n){return{enableLongStackTrace:!1,shouldCoalesceEventChangeDetection:n?.eventCoalescing??!1,shouldCoalesceRunChangeDetection:n?.runCoalescing??!1}}var tw=(()=>{class n{constructor(){this.subscription=new Yt,this.initialized=!1,this.zone=ot(wt),this.pendingTasks=ot(sl)}initialize(){if(this.initialized)return;this.initialized=!0;let t=null;!this.zone.isStable&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(t=this.pendingTasks.add()),this.zone.runOutsideAngular(()=>{this.subscription.add(this.zone.onStable.subscribe(()=>{wt.assertNotInAngularZone(),queueMicrotask(()=>{t!==null&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(this.pendingTasks.remove(t),t=null)})}))}),this.subscription.add(this.zone.onUnstable.subscribe(()=>{wt.assertInAngularZone(),t??=this.pendingTasks.add()}))}ngOnDestroy(){this.subscription.unsubscribe()}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=At({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})();var nw=(()=>{class n{constructor(){this.appRef=ot(Mo),this.taskService=ot(sl),this.ngZone=ot(wt),this.zonelessEnabled=ot(zd),this.disableScheduling=ot(Xv,{optional:!0})??!1,this.zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run,this.schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}],this.subscriptions=new Yt,this.angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(ka):null,this.scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(ot(Yv,{optional:!0})??!1),this.cancelScheduledCallback=null,this.useMicrotaskScheduler=!1,this.runningTick=!1,this.pendingRenderTaskId=null,this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{this.runningTick||this.cleanup()})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()})),this.disableScheduling||=!this.zonelessEnabled&&(this.ngZone instanceof Ru||!this.zoneIsDefined)}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 7:{this.appRef.deferredDirtyFlags|=8;break}case 9:case 8:case 6:case 10:default:this.appRef.dirtyFlags|=8}if(!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?Om:nv;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.disableScheduling||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(ka+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){throw this.taskService.remove(t),i}finally{this.cleanup()}this.useMicrotaskScheduler=!0,Om(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=At({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})();function iw(){return typeof $localize<"u"&&$localize.locale||Xa}var $d=new ze("",{providedIn:"root",factory:()=>ot($d,Be.Optional|Be.SkipSelf)||iw()});var ad=new ze("");function ya(n){return!n.moduleRef}function rw(n){let e=ya(n)?n.r3Injector:n.moduleRef.injector,t=e.get(wt);return t.run(()=>{ya(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(ii,null),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:s=>{i.handleError(s)}})}),ya(n)){let s=()=>e.destroy(),o=n.platformInjector.get(ad);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(ad);o.add(s),n.moduleRef.onDestroy(()=>{ba(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return ZS(i,t,()=>{let s=e.get(c0);return s.runInitializers(),s.donePromise.then(()=>{let o=e.get($d,Xa);if(LS(o||Xa),ya(n)){let a=e.get(Mo);return n.rootComponent!==void 0&&a.bootstrap(n.rootComponent),a}else return sw(n.moduleRef,n.allPlatformModules),n.moduleRef})})})}function sw(n,e){let t=n.injector.get(Mo);if(n._bootstrapComponents.length>0)n._bootstrapComponents.forEach(i=>t.bootstrap(i));else if(n.instance.ngDoBootstrap)n.instance.ngDoBootstrap(t);else throw new qe(-403,!1);e.push(n)}var Sa=null,u0=new ze("");function ow(n=[],e){return as.create({name:e,providers:[{provide:Qa,useValue:"platform"},{provide:ad,useValue:new Set([()=>Sa=null])},...n]})}function aw(n=[]){if(Sa)return Sa;let e=ow(n);return e.get(u0,!1)||(Sa=e),qS(),lw(e),e}function lw(n){n.get(Ad,null)?.forEach(t=>t())}var qd=(()=>{class n{static{this.__NG_ELEMENT_ID__=cw}}return n})();function cw(n){return uw(sn(),Ke(),(n&16)===16)}function uw(n,e,t){if(tl(n)&&!t){let i=Pi(n.index,e);return new pr(i,i)}else if(n.type&175){let i=e[Fn];return new pr(i,e)}return null}function d0(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;try{let s=r?.injector??aw(i);if(s.get(u0,!1)===!0&&!n.platformRef)throw new qe(401,!1);let o=[QS({}),{provide:ls,useExisting:nw},...t||[]],a=new Wa({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return rw({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}}function gl(n){return typeof n=="boolean"?n:n!=null&&n!=="false"}function vl(n,e){vs("NgSignals");let t=Bp(n);return e?.equal&&(t[ti].equal=e.equal),t}function Ui(n){let e=Ve(null);try{return n()}finally{Ve(e)}}var f0=null;function xs(){return f0}function p0(n){f0??=n}var yl=class{};var Vi=new ze("");function m0(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var g0="browser",dw="server";function Xd(n){return n===dw}var _l=class{};var Kd=class extends yl{constructor(){super(...arguments),this.supportsDOMEvents=!0}},Jd=class n extends Kd{static makeCurrent(){p0(new n)}onAndCancel(e,t,i){return e.addEventListener(t,i),()=>{e.removeEventListener(t,i)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=fw();return t==null?null:pw(t)}resetBaseElement(){Eo=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return m0(document.cookie,e)}},Eo=null;function fw(){return Eo=Eo||document.querySelector("base"),Eo?Eo.getAttribute("href"):null}function pw(n){return new URL(n,document.baseURI).pathname}var mw=(()=>{class n{build(){return new XMLHttpRequest}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=At({token:n,factory:n.\u0275fac})}}return n})(),Qd=new ze(""),x0=(()=>{class n{constructor(t,i){this._zone=i,this._eventNameToPlugin=new Map,t.forEach(r=>{r.manager=this}),this._plugins=t.slice().reverse()}addEventListener(t,i,r){return this._findPluginFor(i).addEventListener(t,i,r)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new qe(5101,!1);return this._eventNameToPlugin.set(t,i),i}static{this.\u0275fac=function(i){return new(i||n)(ft(Qd),ft(wt))}}static{this.\u0275prov=At({token:n,factory:n.\u0275fac})}}return n})(),xl=class{constructor(e){this._doc=e}},Yd="ng-app-id",M0=(()=>{class n{constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,this.platformId=s,this.styleRef=new Map,this.hostNodes=new Set,this.styleNodesInDOM=this.collectServerRenderedStyles(),this.platformIsServer=Xd(s),this.resetHostNodes()}addStyles(t){for(let i of t)this.changeUsageCount(i,1)===1&&this.onStyleAdded(i)}removeStyles(t){for(let i of t)this.changeUsageCount(i,-1)<=0&&this.onStyleRemoved(i)}ngOnDestroy(){let t=this.styleNodesInDOM;t&&(t.forEach(i=>i.remove()),t.clear());for(let i of this.getAllStyles())this.onStyleRemoved(i);this.resetHostNodes()}addHost(t){this.hostNodes.add(t);for(let i of this.getAllStyles())this.addStyleToHost(t,i)}removeHost(t){this.hostNodes.delete(t)}getAllStyles(){return this.styleRef.keys()}onStyleAdded(t){for(let i of this.hostNodes)this.addStyleToHost(i,t)}onStyleRemoved(t){let i=this.styleRef;i.get(t)?.elements?.forEach(r=>r.remove()),i.delete(t)}collectServerRenderedStyles(){let t=this.doc.head?.querySelectorAll(`style[${Yd}="${this.appId}"]`);if(t?.length){let i=new Map;return t.forEach(r=>{r.textContent!=null&&i.set(r.textContent,r)}),i}return null}changeUsageCount(t,i){let r=this.styleRef;if(r.has(t)){let s=r.get(t);return s.usage+=i,s.usage}return r.set(t,{usage:i,elements:[]}),i}getStyleElement(t,i){let r=this.styleNodesInDOM,s=r?.get(i);if(s?.parentNode===t)return r.delete(i),s.removeAttribute(Yd),s;{let o=this.doc.createElement("style");return this.nonce&&o.setAttribute("nonce",this.nonce),o.textContent=i,this.platformIsServer&&o.setAttribute(Yd,this.appId),t.appendChild(o),o}}addStyleToHost(t,i){let r=this.getStyleElement(t,i),s=this.styleRef,o=s.get(i)?.elements;o?o.push(r):s.set(i,{elements:[r],usage:1})}resetHostNodes(){let t=this.hostNodes;t.clear(),t.add(this.doc.head)}static{this.\u0275fac=function(i){return new(i||n)(ft(Vi),ft(Td),ft(Id,8),ft(gs))}}static{this.\u0275prov=At({token:n,factory:n.\u0275fac})}}return n})(),Zd={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},th=/%COMP%/g,E0="%COMP%",gw=`_nghost-${E0}`,vw=`_ngcontent-${E0}`,yw=!0,_w=new ze("",{providedIn:"root",factory:()=>yw});function xw(n){return vw.replace(th,n)}function Mw(n){return gw.replace(th,n)}function b0(n,e){return e.map(t=>t.replace(th,n))}var v0=(()=>{class n{constructor(t,i,r,s,o,a,l,c=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.platformId=a,this.ngZone=l,this.nonce=c,this.rendererByCompId=new Map,this.platformIsServer=Xd(a),this.defaultRenderer=new bo(t,o,l,this.platformIsServer)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;this.platformIsServer&&i.encapsulation===qn.ShadowDom&&(i=St(vt({},i),{encapsulation:qn.Emulated}));let r=this.getOrCreateRenderer(t,i);return r instanceof Ml?r.applyToHost(t):r instanceof So&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.platformIsServer;switch(i.encapsulation){case qn.Emulated:s=new Ml(l,c,i,this.appId,u,o,a,d);break;case qn.ShadowDom:return new eh(l,c,t,i,o,a,this.nonce,d);default:s=new So(l,c,i,u,o,a,d);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}static{this.\u0275fac=function(i){return new(i||n)(ft(x0),ft(M0),ft(Td),ft(_w),ft(Vi),ft(gs),ft(wt),ft(Id))}}static{this.\u0275prov=At({token:n,factory:n.\u0275fac})}}return n})(),bo=class{constructor(e,t,i,r){this.eventManager=e,this.doc=t,this.ngZone=i,this.platformIsServer=r,this.data=Object.create(null),this.throwOnSyntheticProps=!0,this.destroyNode=null}destroy(){}createElement(e,t){return t?this.doc.createElementNS(Zd[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(y0(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(y0(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new qe(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=Zd[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=Zd[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(ri.DashCase|ri.Important)?e.style.setProperty(t,i,r&ri.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&ri.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i){if(typeof e=="string"&&(e=xs().getGlobalEventTarget(this.doc,e),!e))throw new Error(`Unsupported event target ${e} for event ${t}`);return this.eventManager.addEventListener(e,t,this.decoratePreventDefault(i))}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;(this.platformIsServer?this.ngZone.runGuarded(()=>e(t)):e(t))===!1&&t.preventDefault()}}};function y0(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var eh=class extends bo{constructor(e,t,i,r,s,o,a,l){super(e,s,o,l),this.sharedStylesHost=t,this.hostEl=i,this.shadowRoot=i.attachShadow({mode:"open"}),this.sharedStylesHost.addHost(this.shadowRoot);let c=b0(r.id,r.styles);for(let u of c){let d=document.createElement("style");a&&d.setAttribute("nonce",a),d.textContent=u,this.shadowRoot.appendChild(d)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost.removeHost(this.shadowRoot)}},So=class extends bo{constructor(e,t,i,r,s,o,a,l){super(e,s,o,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r,this.styles=l?b0(l,i.styles):i.styles}applyStyles(){this.sharedStylesHost.addStyles(this.styles)}destroy(){this.removeStylesOnCompDestroy&&this.sharedStylesHost.removeStyles(this.styles)}},Ml=class extends So{constructor(e,t,i,r,s,o,a,l){let c=r+"-"+i.id;super(e,t,i,s,o,a,l,c),this.contentAttr=xw(c),this.hostAttr=Mw(c)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}},Ew=(()=>{class n extends xl{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r){return t.addEventListener(i,r,!1),()=>this.removeEventListener(t,i,r)}removeEventListener(t,i,r){return t.removeEventListener(i,r)}static{this.\u0275fac=function(i){return new(i||n)(ft(Vi))}}static{this.\u0275prov=At({token:n,factory:n.\u0275fac})}}return n})(),_0=["alt","control","meta","shift"],bw={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},Sw={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},ww=(()=>{class n extends xl{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r){let s=n.parseEventName(i),o=n.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>xs().onAndCancel(t,s.domEventName,o))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),_0.forEach(c=>{let u=i.indexOf(c);u>-1&&(i.splice(u,1),o+=c+".")}),o+=s,i.length!=0||s.length===0)return null;let l={};return l.domEventName=r,l.fullKey=o,l}static matchEventFullKeyCode(t,i){let r=bw[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),_0.forEach(o=>{if(o!==r){let a=Sw[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static{this.\u0275fac=function(i){return new(i||n)(ft(Vi))}}static{this.\u0275prov=At({token:n,factory:n.\u0275fac})}}return n})();function S0(n,e,t){return d0(vt({rootComponent:n,platformRef:t?.platformRef},Cw(e)))}function Cw(n){return{appProviders:[...Rw,...n?.providers??[]],platformProviders:Iw}}function Dw(){Jd.makeCurrent()}function Tw(){return new ii}function Aw(){return cv(document),document}var Iw=[{provide:gs,useValue:g0},{provide:Ad,useValue:Dw,multi:!0},{provide:Vi,useFactory:Aw,deps:[]}];var Rw=[{provide:Qa,useValue:"root"},{provide:ii,useFactory:Tw,deps:[]},{provide:Qd,useClass:Ew,multi:!0,deps:[Vi,wt,gs]},{provide:Qd,useClass:ww,multi:!0,deps:[Vi]},v0,M0,x0,{provide:cs,useExisting:v0},{provide:_l,useClass:mw,deps:[]},[]];var Yf="169";var Nw=0,w0=1,Pw=2;var Sy=1,Fw=2,gi=3,Yi=0,cn=1,yi=2,qi=0,Ls=1,C0=2,D0=3,T0=4,Ow=5,wr=100,Lw=101,kw=102,Uw=103,Vw=104,Bw=200,Hw=201,zw=202,Gw=203,Fh=204,Oh=205,Ww=206,jw=207,$w=208,qw=209,Xw=210,Yw=211,Zw=212,Kw=213,Jw=214,Lh=0,kh=1,Uh=2,Bs=3,Vh=4,Bh=5,Hh=6,zh=7,wy=0,Qw=1,eC=2,Xi=0,tC=1,nC=2,iC=3,rC=4,sC=5,oC=6,aC=7;var A0=300,Hs=301,zs=302,Gh=303,Wh=304,xc=306,jh=1e3,Tr=1001,$h=1002,Tn=1003,lC=1004;var El=1005;var Vn=1006,nh=1007;var Ar=1008;var bi=1009,Cy=1010,Dy=1011,Po=1012,Zf=1013,Ir=1014,_i=1015,ko=1016,Kf=1017,Jf=1018,Gs=1020,Ty=35902,Ay=1021,Iy=1022,Bn=1023,Ry=1024,Ny=1025,ks=1026,Ws=1027,Py=1028,Qf=1029,Fy=1030,ep=1031;var tp=1033,Wl=33776,jl=33777,$l=33778,ql=33779,qh=35840,Xh=35841,Yh=35842,Zh=35843,Kh=36196,Jh=37492,Qh=37496,ef=37808,tf=37809,nf=37810,rf=37811,sf=37812,of=37813,af=37814,lf=37815,cf=37816,uf=37817,df=37818,hf=37819,ff=37820,pf=37821,Xl=36492,mf=36494,gf=36495,Oy=36283,vf=36284,yf=36285,_f=36286;var Zl=2300,xf=2301,ih=2302,I0=2400,R0=2401,N0=2402;var cC=3200,uC=3201;var Ly=0,dC=1,$i="",Kn="srgb",Qi="srgb-linear",np="display-p3",Mc="display-p3-linear",Kl="linear",Mt="srgb",Jl="rec709",Ql="p3";var Ms=7680;var P0=519,hC=512,fC=513,pC=514,ky=515,mC=516,gC=517,vC=518,yC=519,F0=35044;var O0="300 es",xi=2e3,ec=2001,Zi=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){if(this._listeners===void 0)return!1;let i=this._listeners;return i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){if(this._listeners===void 0)return;let r=this._listeners[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){if(this._listeners===void 0)return;let i=this._listeners[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},qt=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"];var rh=Math.PI/180,Mf=180/Math.PI;function Uo(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(qt[n&255]+qt[n>>8&255]+qt[n>>16&255]+qt[n>>24&255]+"-"+qt[e&255]+qt[e>>8&255]+"-"+qt[e>>16&15|64]+qt[e>>24&255]+"-"+qt[t&63|128]+qt[t>>8&255]+"-"+qt[t>>16&255]+qt[t>>24&255]+qt[i&255]+qt[i>>8&255]+qt[i>>16&255]+qt[i>>24&255]).toLowerCase()}function an(n,e,t){return Math.max(e,Math.min(t,n))}function _C(n,e){return(n%e+e)%e}function sh(n,e,t){return(1-t)*n+t*e}function wo(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function on(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var lt=class n{constructor(e=0,t=0){n.prototype.isVector2=!0,this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(an(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},ke=class n{constructor(e,t,i,r,s,o,a,l,c){n.prototype.isMatrix3=!0,this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,l,c)}set(e,t,i,r,s,o,a,l,c){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=l,u[6]=i,u[7]=o,u[8]=c,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],l=i[6],c=i[1],u=i[4],d=i[7],h=i[2],f=i[5],g=i[8],_=r[0],m=r[3],p=r[6],w=r[1],E=r[4],S=r[7],k=r[2],R=r[5],D=r[8];return s[0]=o*_+a*w+l*k,s[3]=o*m+a*E+l*R,s[6]=o*p+a*S+l*D,s[1]=c*_+u*w+d*k,s[4]=c*m+u*E+d*R,s[7]=c*p+u*S+d*D,s[2]=h*_+f*w+g*k,s[5]=h*m+f*E+g*R,s[8]=h*p+f*S+g*D,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],l=e[6],c=e[7],u=e[8];return t*o*u-t*a*c-i*s*u+i*a*l+r*s*c-r*o*l}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],l=e[6],c=e[7],u=e[8],d=u*o-a*c,h=a*l-u*s,f=c*s-o*l,g=t*d+i*h+r*f;if(g===0)return this.set(0,0,0,0,0,0,0,0,0);let _=1/g;return e[0]=d*_,e[1]=(r*c-u*i)*_,e[2]=(a*i-r*o)*_,e[3]=h*_,e[4]=(u*t-r*l)*_,e[5]=(r*s-a*t)*_,e[6]=f*_,e[7]=(i*l-c*t)*_,e[8]=(o*t-i*s)*_,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let l=Math.cos(s),c=Math.sin(s);return this.set(i*l,i*c,-i*(l*o+c*a)+o+e,-r*c,r*l,-r*(-c*o+l*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(oh.makeScale(e,t)),this}rotate(e){return this.premultiply(oh.makeRotation(-e)),this}translate(e,t){return this.premultiply(oh.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},oh=new ke;function Uy(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function tc(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function xC(){let n=tc("canvas");return n.style.display="block",n}var L0={};function Yl(n){n in L0||(L0[n]=!0,console.warn(n))}function MC(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}function EC(n){let e=n.elements;e[2]=.5*e[2]+.5*e[3],e[6]=.5*e[6]+.5*e[7],e[10]=.5*e[10]+.5*e[11],e[14]=.5*e[14]+.5*e[15]}function bC(n){let e=n.elements;e[11]===-1?(e[10]=-e[10]-1,e[14]=-e[14]):(e[10]=-e[10],e[14]=-e[14]+1)}var k0=new ke().set(.8224621,.177538,0,.0331941,.9668058,0,.0170827,.0723974,.9105199),U0=new ke().set(1.2249401,-.2249404,0,-.0420569,1.0420571,0,-.0196376,-.0786361,1.0982735),Co={[Qi]:{transfer:Kl,primaries:Jl,luminanceCoefficients:[.2126,.7152,.0722],toReference:n=>n,fromReference:n=>n},[Kn]:{transfer:Mt,primaries:Jl,luminanceCoefficients:[.2126,.7152,.0722],toReference:n=>n.convertSRGBToLinear(),fromReference:n=>n.convertLinearToSRGB()},[Mc]:{transfer:Kl,primaries:Ql,luminanceCoefficients:[.2289,.6917,.0793],toReference:n=>n.applyMatrix3(U0),fromReference:n=>n.applyMatrix3(k0)},[np]:{transfer:Mt,primaries:Ql,luminanceCoefficients:[.2289,.6917,.0793],toReference:n=>n.convertSRGBToLinear().applyMatrix3(U0),fromReference:n=>n.applyMatrix3(k0).convertLinearToSRGB()}},SC=new Set([Qi,Mc]),at={enabled:!0,_workingColorSpace:Qi,get workingColorSpace(){return this._workingColorSpace},set workingColorSpace(n){if(!SC.has(n))throw new Error(`Unsupported working color space, "${n}".`);this._workingColorSpace=n},convert:function(n,e,t){if(this.enabled===!1||e===t||!e||!t)return n;let i=Co[e].toReference,r=Co[t].fromReference;return r(i(n))},fromWorkingColorSpace:function(n,e){return this.convert(n,this._workingColorSpace,e)},toWorkingColorSpace:function(n,e){return this.convert(n,e,this._workingColorSpace)},getPrimaries:function(n){return Co[n].primaries},getTransfer:function(n){return n===$i?Kl:Co[n].transfer},getLuminanceCoefficients:function(n,e=this._workingColorSpace){return n.fromArray(Co[e].luminanceCoefficients)}};function Us(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function ah(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var Es,Ef=class{static getDataURL(e){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let t;if(e instanceof HTMLCanvasElement)t=e;else{Es===void 0&&(Es=tc("canvas")),Es.width=e.width,Es.height=e.height;let i=Es.getContext("2d");e instanceof ImageData?i.putImageData(e,0,0):i.drawImage(e,0,0,e.width,e.height),t=Es}return t.width>2048||t.height>2048?(console.warn("THREE.ImageUtils.getDataURL: Image converted to jpg for performance reasons",e),t.toDataURL("image/jpeg",.6)):t.toDataURL("image/png")}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=tc("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=Us(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(Us(t[i]/255)*255):t[i]=Us(t[i]);return{data:t,width:e.width,height:e.height}}else return console.warn("THREE.ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},wC=0,nc=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:wC++}),this.uuid=Uo(),this.data=e,this.dataReady=!0,this.version=0}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(lh(r[o].image)):s.push(lh(r[o]))}else s=lh(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function lh(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?Ef.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(console.warn("THREE.Texture: Unable to serialize Texture."),{})}var CC=0,kr=(()=>{class n extends Zi{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=Tr,s=Tr,o=Vn,a=Ar,l=Bn,c=bi,u=n.DEFAULT_ANISOTROPY,d=$i){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:CC++}),this.uuid=Uo(),this.name="",this.source=new nc(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=l,this.internalFormat=null,this.type=c,this.offset=new lt(0,0),this.repeat=new lt(1,1),this.center=new lt(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new ke,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=d,this.userData={},this.version=0,this.onUpdate=null,this.isRenderTargetTexture=!1,this.pmremVersion=0}get image(){return this.source.data}set image(t=null){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.6,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==A0)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case jh:t.x=t.x-Math.floor(t.x);break;case Tr:t.x=t.x<0?0:1;break;case $h:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case jh:t.y=t.y-Math.floor(t.y);break;case Tr:t.y=t.y<0?0:1;break;case $h:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=A0,n.DEFAULT_ANISOTROPY=1,n})(),Dt=class n{constructor(e=0,t=0,i=0,r=1){n.prototype.isVector4=!0,this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,l=e.elements,c=l[0],u=l[4],d=l[8],h=l[1],f=l[5],g=l[9],_=l[2],m=l[6],p=l[10];if(Math.abs(u-h)<.01&&Math.abs(d-_)<.01&&Math.abs(g-m)<.01){if(Math.abs(u+h)<.1&&Math.abs(d+_)<.1&&Math.abs(g+m)<.1&&Math.abs(c+f+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let E=(c+1)/2,S=(f+1)/2,k=(p+1)/2,R=(u+h)/4,D=(d+_)/4,V=(g+m)/4;return E>S&&E>k?E<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(E),r=R/i,s=D/i):S>k?S<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(S),i=R/r,s=V/r):k<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(k),i=D/s,r=V/s),this.set(i,r,s,t),this}let w=Math.sqrt((m-g)*(m-g)+(d-_)*(d-_)+(h-u)*(h-u));return Math.abs(w)<.001&&(w=1),this.x=(m-g)/w,this.y=(d-_)/w,this.z=(h-u)/w,this.w=Math.acos((c+f+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this.z=Math.max(e.z,Math.min(t.z,this.z)),this.w=Math.max(e.w,Math.min(t.w,this.w)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this.z=Math.max(e,Math.min(t,this.z)),this.w=Math.max(e,Math.min(t,this.w)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},bf=class extends Zi{constructor(e=1,t=1,i={}){super(),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=1,this.scissor=new Dt(0,0,e,t),this.scissorTest=!1,this.viewport=new Dt(0,0,e,t);let r={width:e,height:t,depth:1};i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:Vn,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1},i);let s=new kr(r,i.mapping,i.wrapS,i.wrapT,i.magFilter,i.minFilter,i.format,i.type,i.anisotropy,i.colorSpace);s.flipY=!1,s.generateMipmaps=i.generateMipmaps,s.internalFormat=i.internalFormat,this.textures=[];let o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0;this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this.depthTexture=i.depthTexture,this.samples=i.samples}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i;this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let i=0,r=e.textures.length;i<r;i++)this.textures[i]=e.textures[i].clone(),this.textures[i].isRenderTargetTexture=!0;let t=Object.assign({},e.texture.image);return this.texture.source=new nc(t),this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this}dispose(){this.dispatchEvent({type:"dispose"})}},Si=class extends bf{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},ic=class extends kr{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Tn,this.minFilter=Tn,this.wrapR=Tr,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var Sf=class extends kr{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Tn,this.minFilter=Tn,this.wrapR=Tr,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Ki=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let l=i[r+0],c=i[r+1],u=i[r+2],d=i[r+3],h=s[o+0],f=s[o+1],g=s[o+2],_=s[o+3];if(a===0){e[t+0]=l,e[t+1]=c,e[t+2]=u,e[t+3]=d;return}if(a===1){e[t+0]=h,e[t+1]=f,e[t+2]=g,e[t+3]=_;return}if(d!==_||l!==h||c!==f||u!==g){let m=1-a,p=l*h+c*f+u*g+d*_,w=p>=0?1:-1,E=1-p*p;if(E>Number.EPSILON){let k=Math.sqrt(E),R=Math.atan2(k,p*w);m=Math.sin(m*R)/k,a=Math.sin(a*R)/k}let S=a*w;if(l=l*m+h*S,c=c*m+f*S,u=u*m+g*S,d=d*m+_*S,m===1-a){let k=1/Math.sqrt(l*l+c*c+u*u+d*d);l*=k,c*=k,u*=k,d*=k}}e[t]=l,e[t+1]=c,e[t+2]=u,e[t+3]=d}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],l=i[r+1],c=i[r+2],u=i[r+3],d=s[o],h=s[o+1],f=s[o+2],g=s[o+3];return e[t]=a*g+u*d+l*f-c*h,e[t+1]=l*g+u*h+c*d-a*f,e[t+2]=c*g+u*f+a*h-l*d,e[t+3]=u*g-a*d-l*h-c*f,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,l=Math.sin,c=a(i/2),u=a(r/2),d=a(s/2),h=l(i/2),f=l(r/2),g=l(s/2);switch(o){case"XYZ":this._x=h*u*d+c*f*g,this._y=c*f*d-h*u*g,this._z=c*u*g+h*f*d,this._w=c*u*d-h*f*g;break;case"YXZ":this._x=h*u*d+c*f*g,this._y=c*f*d-h*u*g,this._z=c*u*g-h*f*d,this._w=c*u*d+h*f*g;break;case"ZXY":this._x=h*u*d-c*f*g,this._y=c*f*d+h*u*g,this._z=c*u*g+h*f*d,this._w=c*u*d-h*f*g;break;case"ZYX":this._x=h*u*d-c*f*g,this._y=c*f*d+h*u*g,this._z=c*u*g-h*f*d,this._w=c*u*d+h*f*g;break;case"YZX":this._x=h*u*d+c*f*g,this._y=c*f*d+h*u*g,this._z=c*u*g-h*f*d,this._w=c*u*d-h*f*g;break;case"XZY":this._x=h*u*d-c*f*g,this._y=c*f*d-h*u*g,this._z=c*u*g+h*f*d,this._w=c*u*d+h*f*g;break;default:console.warn("THREE.Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],l=t[9],c=t[2],u=t[6],d=t[10],h=i+a+d;if(h>0){let f=.5/Math.sqrt(h+1);this._w=.25/f,this._x=(u-l)*f,this._y=(s-c)*f,this._z=(o-r)*f}else if(i>a&&i>d){let f=2*Math.sqrt(1+i-a-d);this._w=(u-l)/f,this._x=.25*f,this._y=(r+o)/f,this._z=(s+c)/f}else if(a>d){let f=2*Math.sqrt(1+a-i-d);this._w=(s-c)/f,this._x=(r+o)/f,this._y=.25*f,this._z=(l+u)/f}else{let f=2*Math.sqrt(1+d-i-a);this._w=(o-r)/f,this._x=(s+c)/f,this._y=(l+u)/f,this._z=.25*f}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<Number.EPSILON?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(an(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,l=t._y,c=t._z,u=t._w;return this._x=i*u+o*a+r*c-s*l,this._y=r*u+o*l+s*a-i*c,this._z=s*u+o*c+i*l-r*a,this._w=o*u-i*a-r*l-s*c,this._onChangeCallback(),this}slerp(e,t){if(t===0)return this;if(t===1)return this.copy(e);let i=this._x,r=this._y,s=this._z,o=this._w,a=o*e._w+i*e._x+r*e._y+s*e._z;if(a<0?(this._w=-e._w,this._x=-e._x,this._y=-e._y,this._z=-e._z,a=-a):this.copy(e),a>=1)return this._w=o,this._x=i,this._y=r,this._z=s,this;let l=1-a*a;if(l<=Number.EPSILON){let f=1-t;return this._w=f*o+t*this._w,this._x=f*i+t*this._x,this._y=f*r+t*this._y,this._z=f*s+t*this._z,this.normalize(),this}let c=Math.sqrt(l),u=Math.atan2(c,a),d=Math.sin((1-t)*u)/c,h=Math.sin(t*u)/c;return this._w=o*d+this._w*h,this._x=i*d+this._x*h,this._y=r*d+this._y*h,this._z=s*d+this._z*h,this._onChangeCallback(),this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},B=class n{constructor(e=0,t=0,i=0){n.prototype.isVector3=!0,this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(V0.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(V0.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,l=e.w,c=2*(o*r-a*i),u=2*(a*t-s*r),d=2*(s*i-o*t);return this.x=t+l*c+o*d-a*u,this.y=i+l*u+a*c-s*d,this.z=r+l*d+s*u-o*c,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this.z=Math.max(e.z,Math.min(t.z,this.z)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this.z=Math.max(e,Math.min(t,this.z)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,l=t.z;return this.x=r*l-s*a,this.y=s*o-i*l,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return ch.copy(this).projectOnVector(e),this.sub(ch)}reflect(e){return this.sub(ch.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(an(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},ch=new B,V0=new Ki,Rr=class{constructor(e=new B(1/0,1/0,1/0),t=new B(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(Ln.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(Ln.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=Ln.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,Ln):Ln.fromBufferAttribute(s,o),Ln.applyMatrix4(e.matrixWorld),this.expandByPoint(Ln);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),bl.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),bl.copy(i.boundingBox)),bl.applyMatrix4(e.matrixWorld),this.union(bl)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,Ln),Ln.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(Do),Sl.subVectors(this.max,Do),bs.subVectors(e.a,Do),Ss.subVectors(e.b,Do),ws.subVectors(e.c,Do),Bi.subVectors(Ss,bs),Hi.subVectors(ws,Ss),yr.subVectors(bs,ws);let t=[0,-Bi.z,Bi.y,0,-Hi.z,Hi.y,0,-yr.z,yr.y,Bi.z,0,-Bi.x,Hi.z,0,-Hi.x,yr.z,0,-yr.x,-Bi.y,Bi.x,0,-Hi.y,Hi.x,0,-yr.y,yr.x,0];return!uh(t,bs,Ss,ws,Sl)||(t=[1,0,0,0,1,0,0,0,1],!uh(t,bs,Ss,ws,Sl))?!1:(wl.crossVectors(Bi,Hi),t=[wl.x,wl.y,wl.z],uh(t,bs,Ss,ws,Sl))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,Ln).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(Ln).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(di[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),di[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),di[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),di[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),di[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),di[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),di[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),di[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(di),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}},di=[new B,new B,new B,new B,new B,new B,new B,new B],Ln=new B,bl=new Rr,bs=new B,Ss=new B,ws=new B,Bi=new B,Hi=new B,yr=new B,Do=new B,Sl=new B,wl=new B,_r=new B;function uh(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){_r.fromArray(n,s);let a=r.x*Math.abs(_r.x)+r.y*Math.abs(_r.y)+r.z*Math.abs(_r.z),l=e.dot(_r),c=t.dot(_r),u=i.dot(_r);if(Math.max(-Math.max(l,c,u),Math.min(l,c,u))>a)return!1}return!0}var DC=new Rr,To=new B,dh=new B,Fo=class{constructor(e=new B,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):DC.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;To.subVectors(e,this.center);let t=To.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(To,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(dh.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(To.copy(e.center).add(dh)),this.expandByPoint(To.copy(e.center).sub(dh))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}},hi=new B,hh=new B,Cl=new B,zi=new B,fh=new B,Dl=new B,ph=new B,wf=class{constructor(e=new B,t=new B(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,hi)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=hi.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(hi.copy(this.origin).addScaledVector(this.direction,t),hi.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){hh.copy(e).add(t).multiplyScalar(.5),Cl.copy(t).sub(e).normalize(),zi.copy(this.origin).sub(hh);let s=e.distanceTo(t)*.5,o=-this.direction.dot(Cl),a=zi.dot(this.direction),l=-zi.dot(Cl),c=zi.lengthSq(),u=Math.abs(1-o*o),d,h,f,g;if(u>0)if(d=o*l-a,h=o*a-l,g=s*u,d>=0)if(h>=-g)if(h<=g){let _=1/u;d*=_,h*=_,f=d*(d+o*h+2*a)+h*(o*d+h+2*l)+c}else h=s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*l)+c;else h=-s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*l)+c;else h<=-g?(d=Math.max(0,-(-o*s+a)),h=d>0?-s:Math.min(Math.max(-s,-l),s),f=-d*d+h*(h+2*l)+c):h<=g?(d=0,h=Math.min(Math.max(-s,-l),s),f=h*(h+2*l)+c):(d=Math.max(0,-(o*s+a)),h=d>0?s:Math.min(Math.max(-s,-l),s),f=-d*d+h*(h+2*l)+c);else h=o>0?-s:s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*l)+c;return i&&i.copy(this.origin).addScaledVector(this.direction,d),r&&r.copy(hh).addScaledVector(Cl,h),f}intersectSphere(e,t){hi.subVectors(e.center,this.origin);let i=hi.dot(this.direction),r=hi.dot(hi)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,l=i+o;return l<0?null:a<0?this.at(l,t):this.at(a,t)}intersectsSphere(e){return this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,l,c=1/this.direction.x,u=1/this.direction.y,d=1/this.direction.z,h=this.origin;return c>=0?(i=(e.min.x-h.x)*c,r=(e.max.x-h.x)*c):(i=(e.max.x-h.x)*c,r=(e.min.x-h.x)*c),u>=0?(s=(e.min.y-h.y)*u,o=(e.max.y-h.y)*u):(s=(e.max.y-h.y)*u,o=(e.min.y-h.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),d>=0?(a=(e.min.z-h.z)*d,l=(e.max.z-h.z)*d):(a=(e.max.z-h.z)*d,l=(e.min.z-h.z)*d),i>l||a>r)||((a>i||i!==i)&&(i=a),(l<r||r!==r)&&(r=l),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,hi)!==null}intersectTriangle(e,t,i,r,s){fh.subVectors(t,e),Dl.subVectors(i,e),ph.crossVectors(fh,Dl);let o=this.direction.dot(ph),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;zi.subVectors(this.origin,e);let l=a*this.direction.dot(Dl.crossVectors(zi,Dl));if(l<0)return null;let c=a*this.direction.dot(fh.cross(zi));if(c<0||l+c>o)return null;let u=-a*zi.dot(ph);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},Rt=class n{constructor(e,t,i,r,s,o,a,l,c,u,d,h,f,g,_,m){n.prototype.isMatrix4=!0,this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,l,c,u,d,h,f,g,_,m)}set(e,t,i,r,s,o,a,l,c,u,d,h,f,g,_,m){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=l,p[2]=c,p[6]=u,p[10]=d,p[14]=h,p[3]=f,p[7]=g,p[11]=_,p[15]=m,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){let t=this.elements,i=e.elements,r=1/Cs.setFromMatrixColumn(e,0).length(),s=1/Cs.setFromMatrixColumn(e,1).length(),o=1/Cs.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),l=Math.cos(r),c=Math.sin(r),u=Math.cos(s),d=Math.sin(s);if(e.order==="XYZ"){let h=o*u,f=o*d,g=a*u,_=a*d;t[0]=l*u,t[4]=-l*d,t[8]=c,t[1]=f+g*c,t[5]=h-_*c,t[9]=-a*l,t[2]=_-h*c,t[6]=g+f*c,t[10]=o*l}else if(e.order==="YXZ"){let h=l*u,f=l*d,g=c*u,_=c*d;t[0]=h+_*a,t[4]=g*a-f,t[8]=o*c,t[1]=o*d,t[5]=o*u,t[9]=-a,t[2]=f*a-g,t[6]=_+h*a,t[10]=o*l}else if(e.order==="ZXY"){let h=l*u,f=l*d,g=c*u,_=c*d;t[0]=h-_*a,t[4]=-o*d,t[8]=g+f*a,t[1]=f+g*a,t[5]=o*u,t[9]=_-h*a,t[2]=-o*c,t[6]=a,t[10]=o*l}else if(e.order==="ZYX"){let h=o*u,f=o*d,g=a*u,_=a*d;t[0]=l*u,t[4]=g*c-f,t[8]=h*c+_,t[1]=l*d,t[5]=_*c+h,t[9]=f*c-g,t[2]=-c,t[6]=a*l,t[10]=o*l}else if(e.order==="YZX"){let h=o*l,f=o*c,g=a*l,_=a*c;t[0]=l*u,t[4]=_-h*d,t[8]=g*d+f,t[1]=d,t[5]=o*u,t[9]=-a*u,t[2]=-c*u,t[6]=f*d+g,t[10]=h-_*d}else if(e.order==="XZY"){let h=o*l,f=o*c,g=a*l,_=a*c;t[0]=l*u,t[4]=-d,t[8]=c*u,t[1]=h*d+_,t[5]=o*u,t[9]=f*d-g,t[2]=g*d-f,t[6]=a*u,t[10]=_*d+h}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(TC,e,AC)}lookAt(e,t,i){let r=this.elements;return vn.subVectors(e,t),vn.lengthSq()===0&&(vn.z=1),vn.normalize(),Gi.crossVectors(i,vn),Gi.lengthSq()===0&&(Math.abs(i.z)===1?vn.x+=1e-4:vn.z+=1e-4,vn.normalize(),Gi.crossVectors(i,vn)),Gi.normalize(),Tl.crossVectors(vn,Gi),r[0]=Gi.x,r[4]=Tl.x,r[8]=vn.x,r[1]=Gi.y,r[5]=Tl.y,r[9]=vn.y,r[2]=Gi.z,r[6]=Tl.z,r[10]=vn.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],l=i[8],c=i[12],u=i[1],d=i[5],h=i[9],f=i[13],g=i[2],_=i[6],m=i[10],p=i[14],w=i[3],E=i[7],S=i[11],k=i[15],R=r[0],D=r[4],V=r[8],re=r[12],v=r[1],M=r[5],j=r[9],z=r[13],Y=r[2],J=r[6],W=r[10],K=r[14],G=r[3],de=r[7],he=r[11],xe=r[15];return s[0]=o*R+a*v+l*Y+c*G,s[4]=o*D+a*M+l*J+c*de,s[8]=o*V+a*j+l*W+c*he,s[12]=o*re+a*z+l*K+c*xe,s[1]=u*R+d*v+h*Y+f*G,s[5]=u*D+d*M+h*J+f*de,s[9]=u*V+d*j+h*W+f*he,s[13]=u*re+d*z+h*K+f*xe,s[2]=g*R+_*v+m*Y+p*G,s[6]=g*D+_*M+m*J+p*de,s[10]=g*V+_*j+m*W+p*he,s[14]=g*re+_*z+m*K+p*xe,s[3]=w*R+E*v+S*Y+k*G,s[7]=w*D+E*M+S*J+k*de,s[11]=w*V+E*j+S*W+k*he,s[15]=w*re+E*z+S*K+k*xe,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],l=e[9],c=e[13],u=e[2],d=e[6],h=e[10],f=e[14],g=e[3],_=e[7],m=e[11],p=e[15];return g*(+s*l*d-r*c*d-s*a*h+i*c*h+r*a*f-i*l*f)+_*(+t*l*f-t*c*h+s*o*h-r*o*f+r*c*u-s*l*u)+m*(+t*c*d-t*a*f-s*o*d+i*o*f+s*a*u-i*c*u)+p*(-r*a*u-t*l*d+t*a*h+r*o*d-i*o*h+i*l*u)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],l=e[6],c=e[7],u=e[8],d=e[9],h=e[10],f=e[11],g=e[12],_=e[13],m=e[14],p=e[15],w=d*m*c-_*h*c+_*l*f-a*m*f-d*l*p+a*h*p,E=g*h*c-u*m*c-g*l*f+o*m*f+u*l*p-o*h*p,S=u*_*c-g*d*c+g*a*f-o*_*f-u*a*p+o*d*p,k=g*d*l-u*_*l-g*a*h+o*_*h+u*a*m-o*d*m,R=t*w+i*E+r*S+s*k;if(R===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let D=1/R;return e[0]=w*D,e[1]=(_*h*s-d*m*s-_*r*f+i*m*f+d*r*p-i*h*p)*D,e[2]=(a*m*s-_*l*s+_*r*c-i*m*c-a*r*p+i*l*p)*D,e[3]=(d*l*s-a*h*s-d*r*c+i*h*c+a*r*f-i*l*f)*D,e[4]=E*D,e[5]=(u*m*s-g*h*s+g*r*f-t*m*f-u*r*p+t*h*p)*D,e[6]=(g*l*s-o*m*s-g*r*c+t*m*c+o*r*p-t*l*p)*D,e[7]=(o*h*s-u*l*s+u*r*c-t*h*c-o*r*f+t*l*f)*D,e[8]=S*D,e[9]=(g*d*s-u*_*s-g*i*f+t*_*f+u*i*p-t*d*p)*D,e[10]=(o*_*s-g*a*s+g*i*c-t*_*c-o*i*p+t*a*p)*D,e[11]=(u*a*s-o*d*s-u*i*c+t*d*c+o*i*f-t*a*f)*D,e[12]=k*D,e[13]=(u*_*r-g*d*r+g*i*h-t*_*h-u*i*m+t*d*m)*D,e[14]=(g*a*r-o*_*r-g*i*l+t*_*l+o*i*m-t*a*m)*D,e[15]=(o*d*r-u*a*r+u*i*l-t*d*l-o*i*h+t*a*h)*D,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,l=e.z,c=s*o,u=s*a;return this.set(c*o+i,c*a-r*l,c*l+r*a,0,c*a+r*l,u*a+i,u*l-r*o,0,c*l-r*a,u*l+r*o,s*l*l+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,l=t._w,c=s+s,u=o+o,d=a+a,h=s*c,f=s*u,g=s*d,_=o*u,m=o*d,p=a*d,w=l*c,E=l*u,S=l*d,k=i.x,R=i.y,D=i.z;return r[0]=(1-(_+p))*k,r[1]=(f+S)*k,r[2]=(g-E)*k,r[3]=0,r[4]=(f-S)*R,r[5]=(1-(h+p))*R,r[6]=(m+w)*R,r[7]=0,r[8]=(g+E)*D,r[9]=(m-w)*D,r[10]=(1-(h+_))*D,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements,s=Cs.set(r[0],r[1],r[2]).length(),o=Cs.set(r[4],r[5],r[6]).length(),a=Cs.set(r[8],r[9],r[10]).length();this.determinant()<0&&(s=-s),e.x=r[12],e.y=r[13],e.z=r[14],kn.copy(this);let c=1/s,u=1/o,d=1/a;return kn.elements[0]*=c,kn.elements[1]*=c,kn.elements[2]*=c,kn.elements[4]*=u,kn.elements[5]*=u,kn.elements[6]*=u,kn.elements[8]*=d,kn.elements[9]*=d,kn.elements[10]*=d,t.setFromRotationMatrix(kn),i.x=s,i.y=o,i.z=a,this}makePerspective(e,t,i,r,s,o,a=xi){let l=this.elements,c=2*s/(t-e),u=2*s/(i-r),d=(t+e)/(t-e),h=(i+r)/(i-r),f,g;if(a===xi)f=-(o+s)/(o-s),g=-2*o*s/(o-s);else if(a===ec)f=-o/(o-s),g=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=c,l[4]=0,l[8]=d,l[12]=0,l[1]=0,l[5]=u,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=f,l[14]=g,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=xi){let l=this.elements,c=1/(t-e),u=1/(i-r),d=1/(o-s),h=(t+e)*c,f=(i+r)*u,g,_;if(a===xi)g=(o+s)*d,_=-2*d;else if(a===ec)g=s*d,_=-1*d;else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=2*c,l[4]=0,l[8]=0,l[12]=-h,l[1]=0,l[5]=2*u,l[9]=0,l[13]=-f,l[2]=0,l[6]=0,l[10]=_,l[14]=-g,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},Cs=new B,kn=new Rt,TC=new B(0,0,0),AC=new B(1,1,1),Gi=new B,Tl=new B,vn=new B,B0=new Rt,H0=new Ki,Nr=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],l=s[8],c=s[1],u=s[5],d=s[9],h=s[2],f=s[6],g=s[10];switch(i){case"XYZ":this._y=Math.asin(an(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-d,g),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(f,u),this._z=0);break;case"YXZ":this._x=Math.asin(-an(d,-1,1)),Math.abs(d)<.9999999?(this._y=Math.atan2(l,g),this._z=Math.atan2(c,u)):(this._y=Math.atan2(-h,o),this._z=0);break;case"ZXY":this._x=Math.asin(an(f,-1,1)),Math.abs(f)<.9999999?(this._y=Math.atan2(-h,g),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(c,o));break;case"ZYX":this._y=Math.asin(-an(h,-1,1)),Math.abs(h)<.9999999?(this._x=Math.atan2(f,g),this._z=Math.atan2(c,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(an(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-d,u),this._y=Math.atan2(-h,o)):(this._x=0,this._y=Math.atan2(l,g));break;case"XZY":this._z=Math.asin(-an(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(f,u),this._y=Math.atan2(l,o)):(this._x=Math.atan2(-d,g),this._y=0);break;default:console.warn("THREE.Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return B0.makeRotationFromQuaternion(t),this.setFromRotationMatrix(B0,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return H0.setFromEuler(this),this.setFromQuaternion(H0,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),rc=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},IC=0,z0=new B,Ds=new Ki,fi=new Rt,Al=new B,Ao=new B,RC=new B,NC=new Ki,G0=new B(1,0,0),W0=new B(0,1,0),j0=new B(0,0,1),$0={type:"added"},PC={type:"removed"},Ts={type:"childadded",child:null},mh={type:"childremoved",child:null},wi=(()=>{class n extends Zi{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:IC++}),this.uuid=Uo(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new B,i=new Nr,r=new Ki,s=new B(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new Rt},normalMatrix:{value:new ke}}),this.matrix=new Rt,this.matrixWorld=new Rt,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new rc,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.userData={}}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return Ds.setFromAxisAngle(t,i),this.quaternion.multiply(Ds),this}rotateOnWorldAxis(t,i){return Ds.setFromAxisAngle(t,i),this.quaternion.premultiply(Ds),this}rotateX(t){return this.rotateOnAxis(G0,t)}rotateY(t){return this.rotateOnAxis(W0,t)}rotateZ(t){return this.rotateOnAxis(j0,t)}translateOnAxis(t,i){return z0.copy(t).applyQuaternion(this.quaternion),this.position.add(z0.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(G0,t)}translateY(t){return this.translateOnAxis(W0,t)}translateZ(t){return this.translateOnAxis(j0,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(fi.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?Al.copy(t):Al.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),Ao.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?fi.lookAt(Ao,Al,this.up):fi.lookAt(Al,Ao,this.up),this.quaternion.setFromRotationMatrix(fi),s&&(fi.extractRotation(s.matrixWorld),Ds.setFromRotationMatrix(fi),this.quaternion.premultiply(Ds.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(console.error("THREE.Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent($0),Ts.child=t,this.dispatchEvent(Ts),Ts.child=null):console.error("THREE.Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(PC),mh.child=t,this.dispatchEvent(mh),mh.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),fi.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),fi.multiply(t.parent.matrixWorld)),t.applyMatrix4(fi),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent($0),Ts.child=t,this.dispatchEvent(Ts),Ts.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Ao,t,RC),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Ao,NC,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale),this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.6,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.visibility=this._visibility,s.active=this._active,s.bounds=this._bounds.map(l=>({boxInitialized:l.boxInitialized,boxMin:l.box.min.toArray(),boxMax:l.box.max.toArray(),sphereInitialized:l.sphereInitialized,sphereRadius:l.sphere.radius,sphereCenter:l.sphere.center.toArray()})),s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.geometryCount=this._geometryCount,s.matricesTexture=this._matricesTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere={center:s.boundingSphere.center.toArray(),radius:s.boundingSphere.radius}),this.boundingBox!==null&&(s.boundingBox={min:s.boundingBox.min.toArray(),max:s.boundingBox.max.toArray()}));function o(l,c){return l[c.uuid]===void 0&&(l[c.uuid]=c.toJSON(t)),c.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let l=this.geometry.parameters;if(l!==void 0&&l.shapes!==void 0){let c=l.shapes;if(Array.isArray(c))for(let u=0,d=c.length;u<d;u++){let h=c[u];o(t.shapes,h)}else o(t.shapes,c)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let l=[];for(let c=0,u=this.material.length;c<u;c++)l.push(o(t.materials,this.material[c]));s.material=l}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let l=0;l<this.children.length;l++)s.children.push(this.children[l].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let l=0;l<this.animations.length;l++){let c=this.animations[l];s.animations.push(o(t.animations,c))}}if(i){let l=a(t.geometries),c=a(t.materials),u=a(t.textures),d=a(t.images),h=a(t.shapes),f=a(t.skeletons),g=a(t.animations),_=a(t.nodes);l.length>0&&(r.geometries=l),c.length>0&&(r.materials=c),u.length>0&&(r.textures=u),d.length>0&&(r.images=d),h.length>0&&(r.shapes=h),f.length>0&&(r.skeletons=f),g.length>0&&(r.animations=g),_.length>0&&(r.nodes=_)}return r.object=s,r;function a(l){let c=[];for(let u in l){let d=l[u];delete d.metadata,c.push(d)}return c}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new B(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),Un=new B,pi=new B,gh=new B,mi=new B,As=new B,Is=new B,q0=new B,vh=new B,yh=new B,_h=new B,xh=new Dt,Mh=new Dt,Eh=new Dt,Cr=class n{constructor(e=new B,t=new B,i=new B){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),Un.subVectors(e,t),r.cross(Un);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){Un.subVectors(r,t),pi.subVectors(i,t),gh.subVectors(e,t);let o=Un.dot(Un),a=Un.dot(pi),l=Un.dot(gh),c=pi.dot(pi),u=pi.dot(gh),d=o*c-a*a;if(d===0)return s.set(0,0,0),null;let h=1/d,f=(c*l-a*u)*h,g=(o*u-a*l)*h;return s.set(1-f-g,g,f)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,mi)===null?!1:mi.x>=0&&mi.y>=0&&mi.x+mi.y<=1}static getInterpolation(e,t,i,r,s,o,a,l){return this.getBarycoord(e,t,i,r,mi)===null?(l.x=0,l.y=0,"z"in l&&(l.z=0),"w"in l&&(l.w=0),null):(l.setScalar(0),l.addScaledVector(s,mi.x),l.addScaledVector(o,mi.y),l.addScaledVector(a,mi.z),l)}static getInterpolatedAttribute(e,t,i,r,s,o){return xh.setScalar(0),Mh.setScalar(0),Eh.setScalar(0),xh.fromBufferAttribute(e,t),Mh.fromBufferAttribute(e,i),Eh.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(xh,s.x),o.addScaledVector(Mh,s.y),o.addScaledVector(Eh,s.z),o}static isFrontFacing(e,t,i,r){return Un.subVectors(i,t),pi.subVectors(e,t),Un.cross(pi).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return Un.subVectors(this.c,this.b),pi.subVectors(this.a,this.b),Un.cross(pi).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;As.subVectors(r,i),Is.subVectors(s,i),vh.subVectors(e,i);let l=As.dot(vh),c=Is.dot(vh);if(l<=0&&c<=0)return t.copy(i);yh.subVectors(e,r);let u=As.dot(yh),d=Is.dot(yh);if(u>=0&&d<=u)return t.copy(r);let h=l*d-u*c;if(h<=0&&l>=0&&u<=0)return o=l/(l-u),t.copy(i).addScaledVector(As,o);_h.subVectors(e,s);let f=As.dot(_h),g=Is.dot(_h);if(g>=0&&f<=g)return t.copy(s);let _=f*c-l*g;if(_<=0&&c>=0&&g<=0)return a=c/(c-g),t.copy(i).addScaledVector(Is,a);let m=u*g-f*d;if(m<=0&&d-u>=0&&f-g>=0)return q0.subVectors(s,r),a=(d-u)/(d-u+(f-g)),t.copy(r).addScaledVector(q0,a);let p=1/(m+_+h);return o=_*p,a=h*p,t.copy(i).addScaledVector(As,o).addScaledVector(Is,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},Vy={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},Wi={h:0,s:0,l:0},Il={h:0,s:0,l:0};function bh(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var Je=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=Kn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,at.toWorkingColorSpace(this,t),this}setRGB(e,t,i,r=at.workingColorSpace){return this.r=e,this.g=t,this.b=i,at.toWorkingColorSpace(this,r),this}setHSL(e,t,i,r=at.workingColorSpace){if(e=_C(e,1),t=an(t,0,1),i=an(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=bh(o,s,e+1/3),this.g=bh(o,s,e),this.b=bh(o,s,e-1/3)}return at.toWorkingColorSpace(this,r),this}setStyle(e,t=Kn){function i(s){s!==void 0&&parseFloat(s)<1&&console.warn("THREE.Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:console.warn("THREE.Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);console.warn("THREE.Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=Kn){let i=Vy[e.toLowerCase()];return i!==void 0?this.setHex(i,t):console.warn("THREE.Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=Us(e.r),this.g=Us(e.g),this.b=Us(e.b),this}copyLinearToSRGB(e){return this.r=ah(e.r),this.g=ah(e.g),this.b=ah(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=Kn){return at.fromWorkingColorSpace(Xt.copy(this),e),Math.round(an(Xt.r*255,0,255))*65536+Math.round(an(Xt.g*255,0,255))*256+Math.round(an(Xt.b*255,0,255))}getHexString(e=Kn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=at.workingColorSpace){at.fromWorkingColorSpace(Xt.copy(this),t);let i=Xt.r,r=Xt.g,s=Xt.b,o=Math.max(i,r,s),a=Math.min(i,r,s),l,c,u=(a+o)/2;if(a===o)l=0,c=0;else{let d=o-a;switch(c=u<=.5?d/(o+a):d/(2-o-a),o){case i:l=(r-s)/d+(r<s?6:0);break;case r:l=(s-i)/d+2;break;case s:l=(i-r)/d+4;break}l/=6}return e.h=l,e.s=c,e.l=u,e}getRGB(e,t=at.workingColorSpace){return at.fromWorkingColorSpace(Xt.copy(this),t),e.r=Xt.r,e.g=Xt.g,e.b=Xt.b,e}getStyle(e=Kn){at.fromWorkingColorSpace(Xt.copy(this),e);let t=Xt.r,i=Xt.g,r=Xt.b;return e!==Kn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(Wi),this.setHSL(Wi.h+e,Wi.s+t,Wi.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(Wi),e.getHSL(Il);let i=sh(Wi.h,Il.h,t),r=sh(Wi.s,Il.s,t),s=sh(Wi.l,Il.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},Xt=new Je;Je.NAMES=Vy;var FC=0,Pr=class extends Zi{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:FC++}),this.uuid=Uo(),this.name="",this.type="Material",this.blending=Ls,this.side=Yi,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=Fh,this.blendDst=Oh,this.blendEquation=wr,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new Je(0,0,0),this.blendAlpha=0,this.depthFunc=Bs,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=P0,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=Ms,this.stencilZFail=Ms,this.stencilZPass=Ms,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){console.warn(`THREE.Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){console.warn(`THREE.Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.6,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==Ls&&(i.blending=this.blending),this.side!==Yi&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==Fh&&(i.blendSrc=this.blendSrc),this.blendDst!==Oh&&(i.blendDst=this.blendDst),this.blendEquation!==wr&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==Bs&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==P0&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==Ms&&(i.stencilFail=this.stencilFail),this.stencilZFail!==Ms&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==Ms&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let l=s[a];delete l.metadata,o.push(l)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}onBuild(){console.warn("Material: onBuild() has been removed.")}},sc=class extends Pr{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new Je(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new Nr,this.combine=wy,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}};var Pt=new B,Rl=new lt,An=class{constructor(e,t,i=!1){if(Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=F0,this.updateRanges=[],this.gpuType=_i,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)Rl.fromBufferAttribute(this,t),Rl.applyMatrix3(e),this.setXY(t,Rl.x,Rl.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)Pt.fromBufferAttribute(this,t),Pt.applyMatrix3(e),this.setXYZ(t,Pt.x,Pt.y,Pt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)Pt.fromBufferAttribute(this,t),Pt.applyMatrix4(e),this.setXYZ(t,Pt.x,Pt.y,Pt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Pt.fromBufferAttribute(this,t),Pt.applyNormalMatrix(e),this.setXYZ(t,Pt.x,Pt.y,Pt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Pt.fromBufferAttribute(this,t),Pt.transformDirection(e),this.setXYZ(t,Pt.x,Pt.y,Pt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=wo(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=on(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=wo(t,this.array)),t}setX(e,t){return this.normalized&&(t=on(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=wo(t,this.array)),t}setY(e,t){return this.normalized&&(t=on(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=wo(t,this.array)),t}setZ(e,t){return this.normalized&&(t=on(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=wo(t,this.array)),t}setW(e,t){return this.normalized&&(t=on(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=on(t,this.array),i=on(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=on(t,this.array),i=on(i,this.array),r=on(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=on(t,this.array),i=on(i,this.array),r=on(r,this.array),s=on(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==F0&&(e.usage=this.usage),e}};var oc=class extends An{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var ac=class extends An{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var Ei=class extends An{constructor(e,t,i){super(new Float32Array(e),t,i)}},OC=0,Dn=new Rt,Sh=new wi,Rs=new B,yn=new Rr,Io=new Rr,Bt=new B,Fr=class n extends Zi{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:OC++}),this.uuid=Uo(),this.name="",this.type="BufferGeometry",this.index=null,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(Uy(e)?ac:oc)(e,1):this.index=e,this}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new ke().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return Dn.makeRotationFromQuaternion(e),this.applyMatrix4(Dn),this}rotateX(e){return Dn.makeRotationX(e),this.applyMatrix4(Dn),this}rotateY(e){return Dn.makeRotationY(e),this.applyMatrix4(Dn),this}rotateZ(e){return Dn.makeRotationZ(e),this.applyMatrix4(Dn),this}translate(e,t,i){return Dn.makeTranslation(e,t,i),this.applyMatrix4(Dn),this}scale(e,t,i){return Dn.makeScale(e,t,i),this.applyMatrix4(Dn),this}lookAt(e){return Sh.lookAt(e),Sh.updateMatrix(),this.applyMatrix4(Sh.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(Rs).negate(),this.translate(Rs.x,Rs.y,Rs.z),this}setFromPoints(e){let t=[];for(let i=0,r=e.length;i<r;i++){let s=e[i];t.push(s.x,s.y,s.z||0)}return this.setAttribute("position",new Ei(t,3)),this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new Rr);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){console.error("THREE.BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new B(-1/0,-1/0,-1/0),new B(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];yn.setFromBufferAttribute(s),this.morphTargetsRelative?(Bt.addVectors(this.boundingBox.min,yn.min),this.boundingBox.expandByPoint(Bt),Bt.addVectors(this.boundingBox.max,yn.max),this.boundingBox.expandByPoint(Bt)):(this.boundingBox.expandByPoint(yn.min),this.boundingBox.expandByPoint(yn.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&console.error('THREE.BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new Fo);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){console.error("THREE.BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new B,1/0);return}if(e){let i=this.boundingSphere.center;if(yn.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];Io.setFromBufferAttribute(a),this.morphTargetsRelative?(Bt.addVectors(yn.min,Io.min),yn.expandByPoint(Bt),Bt.addVectors(yn.max,Io.max),yn.expandByPoint(Bt)):(yn.expandByPoint(Io.min),yn.expandByPoint(Io.max))}yn.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)Bt.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(Bt));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],l=this.morphTargetsRelative;for(let c=0,u=a.count;c<u;c++)Bt.fromBufferAttribute(a,c),l&&(Rs.fromBufferAttribute(e,c),Bt.add(Rs)),r=Math.max(r,i.distanceToSquared(Bt))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&console.error('THREE.BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){console.error("THREE.BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new An(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],l=[];for(let V=0;V<i.count;V++)a[V]=new B,l[V]=new B;let c=new B,u=new B,d=new B,h=new lt,f=new lt,g=new lt,_=new B,m=new B;function p(V,re,v){c.fromBufferAttribute(i,V),u.fromBufferAttribute(i,re),d.fromBufferAttribute(i,v),h.fromBufferAttribute(s,V),f.fromBufferAttribute(s,re),g.fromBufferAttribute(s,v),u.sub(c),d.sub(c),f.sub(h),g.sub(h);let M=1/(f.x*g.y-g.x*f.y);isFinite(M)&&(_.copy(u).multiplyScalar(g.y).addScaledVector(d,-f.y).multiplyScalar(M),m.copy(d).multiplyScalar(f.x).addScaledVector(u,-g.x).multiplyScalar(M),a[V].add(_),a[re].add(_),a[v].add(_),l[V].add(m),l[re].add(m),l[v].add(m))}let w=this.groups;w.length===0&&(w=[{start:0,count:e.count}]);for(let V=0,re=w.length;V<re;++V){let v=w[V],M=v.start,j=v.count;for(let z=M,Y=M+j;z<Y;z+=3)p(e.getX(z+0),e.getX(z+1),e.getX(z+2))}let E=new B,S=new B,k=new B,R=new B;function D(V){k.fromBufferAttribute(r,V),R.copy(k);let re=a[V];E.copy(re),E.sub(k.multiplyScalar(k.dot(re))).normalize(),S.crossVectors(R,re);let M=S.dot(l[V])<0?-1:1;o.setXYZW(V,E.x,E.y,E.z,M)}for(let V=0,re=w.length;V<re;++V){let v=w[V],M=v.start,j=v.count;for(let z=M,Y=M+j;z<Y;z+=3)D(e.getX(z+0)),D(e.getX(z+1)),D(e.getX(z+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new An(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let h=0,f=i.count;h<f;h++)i.setXYZ(h,0,0,0);let r=new B,s=new B,o=new B,a=new B,l=new B,c=new B,u=new B,d=new B;if(e)for(let h=0,f=e.count;h<f;h+=3){let g=e.getX(h+0),_=e.getX(h+1),m=e.getX(h+2);r.fromBufferAttribute(t,g),s.fromBufferAttribute(t,_),o.fromBufferAttribute(t,m),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),a.fromBufferAttribute(i,g),l.fromBufferAttribute(i,_),c.fromBufferAttribute(i,m),a.add(u),l.add(u),c.add(u),i.setXYZ(g,a.x,a.y,a.z),i.setXYZ(_,l.x,l.y,l.z),i.setXYZ(m,c.x,c.y,c.z)}else for(let h=0,f=t.count;h<f;h+=3)r.fromBufferAttribute(t,h+0),s.fromBufferAttribute(t,h+1),o.fromBufferAttribute(t,h+2),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),i.setXYZ(h+0,u.x,u.y,u.z),i.setXYZ(h+1,u.x,u.y,u.z),i.setXYZ(h+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)Bt.fromBufferAttribute(e,t),Bt.normalize(),e.setXYZ(t,Bt.x,Bt.y,Bt.z)}toNonIndexed(){function e(a,l){let c=a.array,u=a.itemSize,d=a.normalized,h=new c.constructor(l.length*u),f=0,g=0;for(let _=0,m=l.length;_<m;_++){a.isInterleavedBufferAttribute?f=l[_]*a.data.stride+a.offset:f=l[_]*u;for(let p=0;p<u;p++)h[g++]=c[f++]}return new An(h,u,d)}if(this.index===null)return console.warn("THREE.BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let l=r[a],c=e(l,i);t.setAttribute(a,c)}let s=this.morphAttributes;for(let a in s){let l=[],c=s[a];for(let u=0,d=c.length;u<d;u++){let h=c[u],f=e(h,i);l.push(f)}t.morphAttributes[a]=l}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,l=o.length;a<l;a++){let c=o[a];t.addGroup(c.start,c.count,c.materialIndex)}return t}toJSON(){let e={metadata:{version:4.6,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let l=this.parameters;for(let c in l)l[c]!==void 0&&(e[c]=l[c]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let l in i){let c=i[l];e.data.attributes[l]=c.toJSON(e.data)}let r={},s=!1;for(let l in this.morphAttributes){let c=this.morphAttributes[l],u=[];for(let d=0,h=c.length;d<h;d++){let f=c[d];u.push(f.toJSON(e.data))}u.length>0&&(r[l]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere={center:a.center.toArray(),radius:a.radius}),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone(t));let r=e.attributes;for(let c in r){let u=r[c];this.setAttribute(c,u.clone(t))}let s=e.morphAttributes;for(let c in s){let u=[],d=s[c];for(let h=0,f=d.length;h<f;h++)u.push(d[h].clone(t));this.morphAttributes[c]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let c=0,u=o.length;c<u;c++){let d=o[c];this.addGroup(d.start,d.count,d.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let l=e.boundingSphere;return l!==null&&(this.boundingSphere=l.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}},X0=new Rt,xr=new wf,Nl=new Fo,Y0=new B,Pl=new B,Fl=new B,Ol=new B,wh=new B,Ll=new B,Z0=new B,kl=new B,ln=class extends wi{constructor(e=new Fr,t=new sc){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){Ll.set(0,0,0);for(let l=0,c=s.length;l<c;l++){let u=a[l],d=s[l];u!==0&&(wh.fromBufferAttribute(d,e),o?Ll.addScaledVector(wh,u):Ll.addScaledVector(wh.sub(t),u))}t.add(Ll)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),Nl.copy(i.boundingSphere),Nl.applyMatrix4(s),xr.copy(e.ray).recast(e.near),!(Nl.containsPoint(xr.origin)===!1&&(xr.intersectSphere(Nl,Y0)===null||xr.origin.distanceToSquared(Y0)>(e.far-e.near)**2))&&(X0.copy(s).invert(),xr.copy(e.ray).applyMatrix4(X0),!(i.boundingBox!==null&&xr.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,xr)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,l=s.attributes.position,c=s.attributes.uv,u=s.attributes.uv1,d=s.attributes.normal,h=s.groups,f=s.drawRange;if(a!==null)if(Array.isArray(o))for(let g=0,_=h.length;g<_;g++){let m=h[g],p=o[m.materialIndex],w=Math.max(m.start,f.start),E=Math.min(a.count,Math.min(m.start+m.count,f.start+f.count));for(let S=w,k=E;S<k;S+=3){let R=a.getX(S),D=a.getX(S+1),V=a.getX(S+2);r=Ul(this,p,e,i,c,u,d,R,D,V),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,f.start),_=Math.min(a.count,f.start+f.count);for(let m=g,p=_;m<p;m+=3){let w=a.getX(m),E=a.getX(m+1),S=a.getX(m+2);r=Ul(this,o,e,i,c,u,d,w,E,S),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}else if(l!==void 0)if(Array.isArray(o))for(let g=0,_=h.length;g<_;g++){let m=h[g],p=o[m.materialIndex],w=Math.max(m.start,f.start),E=Math.min(l.count,Math.min(m.start+m.count,f.start+f.count));for(let S=w,k=E;S<k;S+=3){let R=S,D=S+1,V=S+2;r=Ul(this,p,e,i,c,u,d,R,D,V),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,f.start),_=Math.min(l.count,f.start+f.count);for(let m=g,p=_;m<p;m+=3){let w=m,E=m+1,S=m+2;r=Ul(this,o,e,i,c,u,d,w,E,S),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}}};function LC(n,e,t,i,r,s,o,a){let l;if(e.side===cn?l=i.intersectTriangle(o,s,r,!0,a):l=i.intersectTriangle(r,s,o,e.side===Yi,a),l===null)return null;kl.copy(a),kl.applyMatrix4(n.matrixWorld);let c=t.ray.origin.distanceTo(kl);return c<t.near||c>t.far?null:{distance:c,point:kl.clone(),object:n}}function Ul(n,e,t,i,r,s,o,a,l,c){n.getVertexPosition(a,Pl),n.getVertexPosition(l,Fl),n.getVertexPosition(c,Ol);let u=LC(n,e,t,i,Pl,Fl,Ol,Z0);if(u){let d=new B;Cr.getBarycoord(Z0,Pl,Fl,Ol,d),r&&(u.uv=Cr.getInterpolatedAttribute(r,a,l,c,d,new lt)),s&&(u.uv1=Cr.getInterpolatedAttribute(s,a,l,c,d,new lt)),o&&(u.normal=Cr.getInterpolatedAttribute(o,a,l,c,d,new B),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let h={a,b:l,c,normal:new B,materialIndex:0};Cr.getNormal(Pl,Fl,Ol,h.normal),u.face=h,u.barycoord=d}return u}var Ji=class n extends Fr{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let l=[],c=[],u=[],d=[],h=0,f=0;g("z","y","x",-1,-1,i,t,e,o,s,0),g("z","y","x",1,-1,i,t,-e,o,s,1),g("x","z","y",1,1,e,i,t,r,o,2),g("x","z","y",1,-1,e,i,-t,r,o,3),g("x","y","z",1,-1,e,t,i,r,s,4),g("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(l),this.setAttribute("position",new Ei(c,3)),this.setAttribute("normal",new Ei(u,3)),this.setAttribute("uv",new Ei(d,2));function g(_,m,p,w,E,S,k,R,D,V,re){let v=S/D,M=k/V,j=S/2,z=k/2,Y=R/2,J=D+1,W=V+1,K=0,G=0,de=new B;for(let he=0;he<W;he++){let xe=he*M-z;for(let tt=0;tt<J;tt++){let ct=tt*v-j;de[_]=ct*w,de[m]=xe*E,de[p]=Y,c.push(de.x,de.y,de.z),de[_]=0,de[m]=0,de[p]=R>0?1:-1,u.push(de.x,de.y,de.z),d.push(tt/D),d.push(1-he/V),K+=1}}for(let he=0;he<V;he++)for(let xe=0;xe<D;xe++){let tt=h+xe+J*he,ct=h+xe+J*(he+1),$=h+(xe+1)+J*(he+1),te=h+(xe+1)+J*he;l.push(tt,ct,te),l.push(ct,$,te),G+=6}a.addGroup(f,G,re),f+=G,h+=K}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};function js(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];r&&(r.isColor||r.isMatrix3||r.isMatrix4||r.isVector2||r.isVector3||r.isVector4||r.isTexture||r.isQuaternion)?r.isRenderTargetTexture?(console.warn("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone():Array.isArray(r)?e[t][i]=r.slice():e[t][i]=r}}return e}function Qt(n){let e={};for(let t=0;t<n.length;t++){let i=js(n[t]);for(let r in i)e[r]=i[r]}return e}function kC(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function By(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:at.workingColorSpace}var UC={clone:js,merge:Qt},VC=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,BC=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,Qn=class extends Pr{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=VC,this.fragmentShader=BC,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=js(e.uniforms),this.uniformsGroups=kC(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},lc=class extends wi{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new Rt,this.projectionMatrix=new Rt,this.projectionMatrixInverse=new Rt,this.coordinateSystem=xi}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorldInverse.copy(this.matrixWorld).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorldInverse.copy(this.matrixWorld).invert()}clone(){return new this.constructor().copy(this)}},ji=new B,K0=new lt,J0=new lt,en=class extends lc{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=Mf*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(rh*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return Mf*2*Math.atan(Math.tan(rh*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){ji.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(ji.x,ji.y).multiplyScalar(-e/ji.z),ji.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(ji.x,ji.y).multiplyScalar(-e/ji.z)}getViewSize(e,t){return this.getViewBounds(e,K0,J0),t.subVectors(J0,K0)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(rh*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let l=o.fullWidth,c=o.fullHeight;s+=o.offsetX*r/l,t-=o.offsetY*i/c,r*=o.width/l,i*=o.height/c}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}},Ns=-90,Ps=1,Cf=class extends wi{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new en(Ns,Ps,e,t);r.layers=this.layers,this.add(r);let s=new en(Ns,Ps,e,t);s.layers=this.layers,this.add(s);let o=new en(Ns,Ps,e,t);o.layers=this.layers,this.add(o);let a=new en(Ns,Ps,e,t);a.layers=this.layers,this.add(a);let l=new en(Ns,Ps,e,t);l.layers=this.layers,this.add(l);let c=new en(Ns,Ps,e,t);c.layers=this.layers,this.add(c)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,l]=t;for(let c of t)this.remove(c);if(e===xi)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),l.up.set(0,1,0),l.lookAt(0,0,-1);else if(e===ec)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),l.up.set(0,-1,0),l.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let c of t)this.add(c),c.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,l,c,u]=this.children,d=e.getRenderTarget(),h=e.getActiveCubeFace(),f=e.getActiveMipmapLevel(),g=e.xr.enabled;e.xr.enabled=!1;let _=i.texture.generateMipmaps;i.texture.generateMipmaps=!1,e.setRenderTarget(i,0,r),e.render(t,s),e.setRenderTarget(i,1,r),e.render(t,o),e.setRenderTarget(i,2,r),e.render(t,a),e.setRenderTarget(i,3,r),e.render(t,l),e.setRenderTarget(i,4,r),e.render(t,c),i.texture.generateMipmaps=_,e.setRenderTarget(i,5,r),e.render(t,u),e.setRenderTarget(d,h,f),e.xr.enabled=g,i.texture.needsPMREMUpdate=!0}},cc=class extends kr{constructor(e,t,i,r,s,o,a,l,c,u){e=e!==void 0?e:[],t=t!==void 0?t:Hs,super(e,t,i,r,s,o,a,l,c,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}},Df=class extends Si{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new cc(r,t.mapping,t.wrapS,t.wrapT,t.magFilter,t.minFilter,t.format,t.type,t.anisotropy,t.colorSpace),this.texture.isRenderTargetTexture=!0,this.texture.generateMipmaps=t.generateMipmaps!==void 0?t.generateMipmaps:!1,this.texture.minFilter=t.minFilter!==void 0?t.minFilter:Vn}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

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
			`},r=new Ji(5,5,5),s=new Qn({name:"CubemapFromEquirect",uniforms:js(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:cn,blending:qi});s.uniforms.tEquirect.value=t;let o=new ln(r,s),a=t.minFilter;return t.minFilter===Ar&&(t.minFilter=Vn),new Cf(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t,i,r){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}},Ch=new B,HC=new B,zC=new ke,vi=class{constructor(e=new B(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=Ch.subVectors(i,t).cross(HC.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t){let i=e.delta(Ch),r=this.normal.dot(i);if(r===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let s=-(e.start.dot(this.normal)+this.constant)/r;return s<0||s>1?null:t.copy(e.start).addScaledVector(i,s)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||zC.getNormalMatrix(e),r=this.coplanarPoint(Ch).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},Mr=new Fo,Vl=new B,Oo=class{constructor(e=new vi,t=new vi,i=new vi,r=new vi,s=new vi,o=new vi){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=xi){let i=this.planes,r=e.elements,s=r[0],o=r[1],a=r[2],l=r[3],c=r[4],u=r[5],d=r[6],h=r[7],f=r[8],g=r[9],_=r[10],m=r[11],p=r[12],w=r[13],E=r[14],S=r[15];if(i[0].setComponents(l-s,h-c,m-f,S-p).normalize(),i[1].setComponents(l+s,h+c,m+f,S+p).normalize(),i[2].setComponents(l+o,h+u,m+g,S+w).normalize(),i[3].setComponents(l-o,h-u,m-g,S-w).normalize(),i[4].setComponents(l-a,h-d,m-_,S-E).normalize(),t===xi)i[5].setComponents(l+a,h+d,m+_,S+E).normalize();else if(t===ec)i[5].setComponents(a,d,_,E).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),Mr.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),Mr.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(Mr)}intersectsSprite(e){return Mr.center.set(0,0,0),Mr.radius=.7071067811865476,Mr.applyMatrix4(e.matrixWorld),this.intersectsSphere(Mr)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(Vl.x=r.normal.x>0?e.max.x:e.min.x,Vl.y=r.normal.y>0?e.max.y:e.min.y,Vl.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(Vl)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};function Hy(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function GC(n){let e=new WeakMap;function t(a,l){let c=a.array,u=a.usage,d=c.byteLength,h=n.createBuffer();n.bindBuffer(l,h),n.bufferData(l,c,u),a.onUploadCallback();let f;if(c instanceof Float32Array)f=n.FLOAT;else if(c instanceof Uint16Array)a.isFloat16BufferAttribute?f=n.HALF_FLOAT:f=n.UNSIGNED_SHORT;else if(c instanceof Int16Array)f=n.SHORT;else if(c instanceof Uint32Array)f=n.UNSIGNED_INT;else if(c instanceof Int32Array)f=n.INT;else if(c instanceof Int8Array)f=n.BYTE;else if(c instanceof Uint8Array)f=n.UNSIGNED_BYTE;else if(c instanceof Uint8ClampedArray)f=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+c);return{buffer:h,type:f,bytesPerElement:c.BYTES_PER_ELEMENT,version:a.version,size:d}}function i(a,l,c){let u=l.array,d=l.updateRanges;if(n.bindBuffer(c,a),d.length===0)n.bufferSubData(c,0,u);else{d.sort((f,g)=>f.start-g.start);let h=0;for(let f=1;f<d.length;f++){let g=d[h],_=d[f];_.start<=g.start+g.count+1?g.count=Math.max(g.count,_.start+_.count-g.start):(++h,d[h]=_)}d.length=h+1;for(let f=0,g=d.length;f<g;f++){let _=d[f];n.bufferSubData(c,_.start*u.BYTES_PER_ELEMENT,u,_.start,_.count)}l.clearUpdateRanges()}l.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let l=e.get(a);l&&(n.deleteBuffer(l.buffer),e.delete(a))}function o(a,l){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let c=e.get(a);if(c===void 0)e.set(a,t(a,l));else if(c.version<a.version){if(c.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(c.buffer,a,l),c.version=a.version}}return{get:r,remove:s,update:o}}var uc=class n extends Fr{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),l=Math.floor(r),c=a+1,u=l+1,d=e/a,h=t/l,f=[],g=[],_=[],m=[];for(let p=0;p<u;p++){let w=p*h-o;for(let E=0;E<c;E++){let S=E*d-s;g.push(S,-w,0),_.push(0,0,1),m.push(E/a),m.push(1-p/l)}}for(let p=0;p<l;p++)for(let w=0;w<a;w++){let E=w+c*p,S=w+c*(p+1),k=w+1+c*(p+1),R=w+1+c*p;f.push(E,S,R),f.push(S,k,R)}this.setIndex(f),this.setAttribute("position",new Ei(g,3)),this.setAttribute("normal",new Ei(_,3)),this.setAttribute("uv",new Ei(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}},WC=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,jC=`#ifdef USE_ALPHAHASH
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
#endif`,$C=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,qC=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,XC=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,YC=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,ZC=`#ifdef USE_AOMAP
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
#endif`,KC=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,JC=`#ifdef USE_BATCHING
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
#endif`,QC=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,eD=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,tD=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,nD=`float G_BlinnPhong_Implicit( ) {
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
} // validated`,iD=`#ifdef USE_IRIDESCENCE
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
#endif`,rD=`#ifdef USE_BUMPMAP
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
#endif`,sD=`#if NUM_CLIPPING_PLANES > 0
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
#endif`,oD=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,aD=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,lD=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,cD=`#if defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#elif defined( USE_COLOR )
	diffuseColor.rgb *= vColor;
#endif`,uD=`#if defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#elif defined( USE_COLOR )
	varying vec3 vColor;
#endif`,dD=`#if defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#elif defined( USE_COLOR ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec3 vColor;
#endif`,hD=`#if defined( USE_COLOR_ALPHA )
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
#endif`,fD=`#define PI 3.141592653589793
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
} // validated`,pD=`#ifdef ENVMAP_TYPE_CUBE_UV
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
#endif`,mD=`vec3 transformedNormal = objectNormal;
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
#endif`,gD=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,vD=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,yD=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,_D=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,xD="gl_FragColor = linearToOutputTexel( gl_FragColor );",MD=`
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
}`,ED=`#ifdef USE_ENVMAP
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
#endif`,bD=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform float flipEnvMap;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
	
#endif`,SD=`#ifdef USE_ENVMAP
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
#endif`,wD=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,CD=`#ifdef USE_ENVMAP
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
#endif`,DD=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,TD=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,AD=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,ID=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,RD=`#ifdef USE_GRADIENTMAP
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
}`,ND=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,PD=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,FD=`varying vec3 vViewPosition;
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
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,OD=`uniform bool receiveShadow;
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
#endif`,LD=`#ifdef USE_ENVMAP
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
#endif`,kD=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,UD=`varying vec3 vViewPosition;
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
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,VD=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,BD=`varying vec3 vViewPosition;
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
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,HD=`PhysicalMaterial material;
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
#endif`,zD=`struct PhysicalMaterial {
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
}`,GD=`
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
#endif`,WD=`#if defined( RE_IndirectDiffuse )
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
#endif`,jD=`#if defined( RE_IndirectDiffuse )
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,$D=`#if defined( USE_LOGDEPTHBUF )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,qD=`#if defined( USE_LOGDEPTHBUF )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,XD=`#ifdef USE_LOGDEPTHBUF
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,YD=`#ifdef USE_LOGDEPTHBUF
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,ZD=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = vec4( mix( pow( sampledDiffuseColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), sampledDiffuseColor.rgb * 0.0773993808, vec3( lessThanEqual( sampledDiffuseColor.rgb, vec3( 0.04045 ) ) ) ), sampledDiffuseColor.w );
	
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,KD=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,JD=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
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
#endif`,QD=`#if defined( USE_POINTS_UV )
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
#endif`,eT=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,tT=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,nT=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,iT=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,rT=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,sT=`#ifdef USE_MORPHTARGETS
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
#endif`,oT=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,aT=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
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
vec3 nonPerturbedNormal = normal;`,lT=`#ifdef USE_NORMALMAP_OBJECTSPACE
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
#endif`,cT=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,uT=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,dT=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,hT=`#ifdef USE_NORMALMAP
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
#endif`,fT=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,pT=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,mT=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,gT=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,vT=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,yT=`vec3 packNormalToRGB( const in vec3 normal ) {
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
}`,_T=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,xT=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,MT=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,ET=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,bT=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,ST=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,wT=`#if NUM_SPOT_LIGHT_COORDS > 0
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
#endif`,CT=`#if NUM_SPOT_LIGHT_COORDS > 0
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
#endif`,DT=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
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
#endif`,TT=`float getShadowMask() {
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
}`,AT=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,IT=`#ifdef USE_SKINNING
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
#endif`,RT=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,NT=`#ifdef USE_SKINNING
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
#endif`,PT=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,FT=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,OT=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,LT=`#ifndef saturate
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
vec3 CustomToneMapping( vec3 color ) { return color; }`,kT=`#ifdef USE_TRANSMISSION
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
#endif`,UT=`#ifdef USE_TRANSMISSION
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
#endif`,VT=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
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
#endif`,BT=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
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
#endif`,HT=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
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
#endif`,zT=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,GT=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,WT=`uniform sampler2D t2D;
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
}`,jT=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,$T=`#ifdef ENVMAP_TYPE_CUBE
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
}`,qT=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,XT=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,YT=`#include <common>
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
}`,ZT=`#if DEPTH_PACKING == 3200
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
}`,KT=`#define DISTANCE
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
}`,JT=`#define DISTANCE
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
}`,QT=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,eA=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,tA=`uniform float scale;
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
}`,nA=`uniform vec3 diffuse;
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
}`,iA=`#include <common>
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
}`,rA=`uniform vec3 diffuse;
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
}`,sA=`#define LAMBERT
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
}`,oA=`#define LAMBERT
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
}`,aA=`#define MATCAP
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
}`,lA=`#define MATCAP
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
}`,cA=`#define NORMAL
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
}`,uA=`#define NORMAL
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
}`,dA=`#define PHONG
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
}`,hA=`#define PHONG
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
}`,fA=`#define STANDARD
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
}`,pA=`#define STANDARD
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
}`,mA=`#define TOON
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
}`,gA=`#define TOON
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
}`,vA=`uniform float size;
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
}`,yA=`uniform vec3 diffuse;
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
}`,_A=`#include <common>
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
}`,xA=`uniform vec3 color;
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
}`,MA=`uniform float rotation;
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
}`,EA=`uniform vec3 diffuse;
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
}`,Le={alphahash_fragment:WC,alphahash_pars_fragment:jC,alphamap_fragment:$C,alphamap_pars_fragment:qC,alphatest_fragment:XC,alphatest_pars_fragment:YC,aomap_fragment:ZC,aomap_pars_fragment:KC,batching_pars_vertex:JC,batching_vertex:QC,begin_vertex:eD,beginnormal_vertex:tD,bsdfs:nD,iridescence_fragment:iD,bumpmap_pars_fragment:rD,clipping_planes_fragment:sD,clipping_planes_pars_fragment:oD,clipping_planes_pars_vertex:aD,clipping_planes_vertex:lD,color_fragment:cD,color_pars_fragment:uD,color_pars_vertex:dD,color_vertex:hD,common:fD,cube_uv_reflection_fragment:pD,defaultnormal_vertex:mD,displacementmap_pars_vertex:gD,displacementmap_vertex:vD,emissivemap_fragment:yD,emissivemap_pars_fragment:_D,colorspace_fragment:xD,colorspace_pars_fragment:MD,envmap_fragment:ED,envmap_common_pars_fragment:bD,envmap_pars_fragment:SD,envmap_pars_vertex:wD,envmap_physical_pars_fragment:LD,envmap_vertex:CD,fog_vertex:DD,fog_pars_vertex:TD,fog_fragment:AD,fog_pars_fragment:ID,gradientmap_pars_fragment:RD,lightmap_pars_fragment:ND,lights_lambert_fragment:PD,lights_lambert_pars_fragment:FD,lights_pars_begin:OD,lights_toon_fragment:kD,lights_toon_pars_fragment:UD,lights_phong_fragment:VD,lights_phong_pars_fragment:BD,lights_physical_fragment:HD,lights_physical_pars_fragment:zD,lights_fragment_begin:GD,lights_fragment_maps:WD,lights_fragment_end:jD,logdepthbuf_fragment:$D,logdepthbuf_pars_fragment:qD,logdepthbuf_pars_vertex:XD,logdepthbuf_vertex:YD,map_fragment:ZD,map_pars_fragment:KD,map_particle_fragment:JD,map_particle_pars_fragment:QD,metalnessmap_fragment:eT,metalnessmap_pars_fragment:tT,morphinstance_vertex:nT,morphcolor_vertex:iT,morphnormal_vertex:rT,morphtarget_pars_vertex:sT,morphtarget_vertex:oT,normal_fragment_begin:aT,normal_fragment_maps:lT,normal_pars_fragment:cT,normal_pars_vertex:uT,normal_vertex:dT,normalmap_pars_fragment:hT,clearcoat_normal_fragment_begin:fT,clearcoat_normal_fragment_maps:pT,clearcoat_pars_fragment:mT,iridescence_pars_fragment:gT,opaque_fragment:vT,packing:yT,premultiplied_alpha_fragment:_T,project_vertex:xT,dithering_fragment:MT,dithering_pars_fragment:ET,roughnessmap_fragment:bT,roughnessmap_pars_fragment:ST,shadowmap_pars_fragment:wT,shadowmap_pars_vertex:CT,shadowmap_vertex:DT,shadowmask_pars_fragment:TT,skinbase_vertex:AT,skinning_pars_vertex:IT,skinning_vertex:RT,skinnormal_vertex:NT,specularmap_fragment:PT,specularmap_pars_fragment:FT,tonemapping_fragment:OT,tonemapping_pars_fragment:LT,transmission_fragment:kT,transmission_pars_fragment:UT,uv_pars_fragment:VT,uv_pars_vertex:BT,uv_vertex:HT,worldpos_vertex:zT,background_vert:GT,background_frag:WT,backgroundCube_vert:jT,backgroundCube_frag:$T,cube_vert:qT,cube_frag:XT,depth_vert:YT,depth_frag:ZT,distanceRGBA_vert:KT,distanceRGBA_frag:JT,equirect_vert:QT,equirect_frag:eA,linedashed_vert:tA,linedashed_frag:nA,meshbasic_vert:iA,meshbasic_frag:rA,meshlambert_vert:sA,meshlambert_frag:oA,meshmatcap_vert:aA,meshmatcap_frag:lA,meshnormal_vert:cA,meshnormal_frag:uA,meshphong_vert:dA,meshphong_frag:hA,meshphysical_vert:fA,meshphysical_frag:pA,meshtoon_vert:mA,meshtoon_frag:gA,points_vert:vA,points_frag:yA,shadow_vert:_A,shadow_frag:xA,sprite_vert:MA,sprite_frag:EA},se={common:{diffuse:{value:new Je(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new ke},alphaMap:{value:null},alphaMapTransform:{value:new ke},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new ke}},envmap:{envMap:{value:null},envMapRotation:{value:new ke},flipEnvMap:{value:-1},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new ke}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new ke}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new ke},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new ke},normalScale:{value:new lt(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new ke},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new ke}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new ke}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new ke}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new Je(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMap:{value:[]},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotShadowMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMap:{value:[]},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null}},points:{diffuse:{value:new Je(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new ke},alphaTest:{value:0},uvTransform:{value:new ke}},sprite:{diffuse:{value:new Je(16777215)},opacity:{value:1},center:{value:new lt(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new ke},alphaMap:{value:null},alphaMapTransform:{value:new ke},alphaTest:{value:0}}},Jn={basic:{uniforms:Qt([se.common,se.specularmap,se.envmap,se.aomap,se.lightmap,se.fog]),vertexShader:Le.meshbasic_vert,fragmentShader:Le.meshbasic_frag},lambert:{uniforms:Qt([se.common,se.specularmap,se.envmap,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.fog,se.lights,{emissive:{value:new Je(0)}}]),vertexShader:Le.meshlambert_vert,fragmentShader:Le.meshlambert_frag},phong:{uniforms:Qt([se.common,se.specularmap,se.envmap,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.fog,se.lights,{emissive:{value:new Je(0)},specular:{value:new Je(1118481)},shininess:{value:30}}]),vertexShader:Le.meshphong_vert,fragmentShader:Le.meshphong_frag},standard:{uniforms:Qt([se.common,se.envmap,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.roughnessmap,se.metalnessmap,se.fog,se.lights,{emissive:{value:new Je(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:Le.meshphysical_vert,fragmentShader:Le.meshphysical_frag},toon:{uniforms:Qt([se.common,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.gradientmap,se.fog,se.lights,{emissive:{value:new Je(0)}}]),vertexShader:Le.meshtoon_vert,fragmentShader:Le.meshtoon_frag},matcap:{uniforms:Qt([se.common,se.bumpmap,se.normalmap,se.displacementmap,se.fog,{matcap:{value:null}}]),vertexShader:Le.meshmatcap_vert,fragmentShader:Le.meshmatcap_frag},points:{uniforms:Qt([se.points,se.fog]),vertexShader:Le.points_vert,fragmentShader:Le.points_frag},dashed:{uniforms:Qt([se.common,se.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:Le.linedashed_vert,fragmentShader:Le.linedashed_frag},depth:{uniforms:Qt([se.common,se.displacementmap]),vertexShader:Le.depth_vert,fragmentShader:Le.depth_frag},normal:{uniforms:Qt([se.common,se.bumpmap,se.normalmap,se.displacementmap,{opacity:{value:1}}]),vertexShader:Le.meshnormal_vert,fragmentShader:Le.meshnormal_frag},sprite:{uniforms:Qt([se.sprite,se.fog]),vertexShader:Le.sprite_vert,fragmentShader:Le.sprite_frag},background:{uniforms:{uvTransform:{value:new ke},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:Le.background_vert,fragmentShader:Le.background_frag},backgroundCube:{uniforms:{envMap:{value:null},flipEnvMap:{value:-1},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new ke}},vertexShader:Le.backgroundCube_vert,fragmentShader:Le.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:Le.cube_vert,fragmentShader:Le.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:Le.equirect_vert,fragmentShader:Le.equirect_frag},distanceRGBA:{uniforms:Qt([se.common,se.displacementmap,{referencePosition:{value:new B},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:Le.distanceRGBA_vert,fragmentShader:Le.distanceRGBA_frag},shadow:{uniforms:Qt([se.lights,se.fog,{color:{value:new Je(0)},opacity:{value:1}}]),vertexShader:Le.shadow_vert,fragmentShader:Le.shadow_frag}};Jn.physical={uniforms:Qt([Jn.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new ke},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new ke},clearcoatNormalScale:{value:new lt(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new ke},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new ke},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new ke},sheen:{value:0},sheenColor:{value:new Je(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new ke},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new ke},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new ke},transmissionSamplerSize:{value:new lt},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new ke},attenuationDistance:{value:0},attenuationColor:{value:new Je(0)},specularColor:{value:new Je(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new ke},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new ke},anisotropyVector:{value:new lt},anisotropyMap:{value:null},anisotropyMapTransform:{value:new ke}}]),vertexShader:Le.meshphysical_vert,fragmentShader:Le.meshphysical_frag};var Bl={r:0,b:0,g:0},Er=new Nr,bA=new Rt;function SA(n,e,t,i,r,s,o){let a=new Je(0),l=s===!0?0:1,c,u,d=null,h=0,f=null;function g(w){let E=w.isScene===!0?w.background:null;return E&&E.isTexture&&(E=(w.backgroundBlurriness>0?t:e).get(E)),E}function _(w){let E=!1,S=g(w);S===null?p(a,l):S&&S.isColor&&(p(S,1),E=!0);let k=n.xr.getEnvironmentBlendMode();k==="additive"?i.buffers.color.setClear(0,0,0,1,o):k==="alpha-blend"&&i.buffers.color.setClear(0,0,0,0,o),(n.autoClear||E)&&(i.buffers.depth.setTest(!0),i.buffers.depth.setMask(!0),i.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function m(w,E){let S=g(E);S&&(S.isCubeTexture||S.mapping===xc)?(u===void 0&&(u=new ln(new Ji(1,1,1),new Qn({name:"BackgroundCubeMaterial",uniforms:js(Jn.backgroundCube.uniforms),vertexShader:Jn.backgroundCube.vertexShader,fragmentShader:Jn.backgroundCube.fragmentShader,side:cn,depthTest:!1,depthWrite:!1,fog:!1})),u.geometry.deleteAttribute("normal"),u.geometry.deleteAttribute("uv"),u.onBeforeRender=function(k,R,D){this.matrixWorld.copyPosition(D.matrixWorld)},Object.defineProperty(u.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),r.update(u)),Er.copy(E.backgroundRotation),Er.x*=-1,Er.y*=-1,Er.z*=-1,S.isCubeTexture&&S.isRenderTargetTexture===!1&&(Er.y*=-1,Er.z*=-1),u.material.uniforms.envMap.value=S,u.material.uniforms.flipEnvMap.value=S.isCubeTexture&&S.isRenderTargetTexture===!1?-1:1,u.material.uniforms.backgroundBlurriness.value=E.backgroundBlurriness,u.material.uniforms.backgroundIntensity.value=E.backgroundIntensity,u.material.uniforms.backgroundRotation.value.setFromMatrix4(bA.makeRotationFromEuler(Er)),u.material.toneMapped=at.getTransfer(S.colorSpace)!==Mt,(d!==S||h!==S.version||f!==n.toneMapping)&&(u.material.needsUpdate=!0,d=S,h=S.version,f=n.toneMapping),u.layers.enableAll(),w.unshift(u,u.geometry,u.material,0,0,null)):S&&S.isTexture&&(c===void 0&&(c=new ln(new uc(2,2),new Qn({name:"BackgroundMaterial",uniforms:js(Jn.background.uniforms),vertexShader:Jn.background.vertexShader,fragmentShader:Jn.background.fragmentShader,side:Yi,depthTest:!1,depthWrite:!1,fog:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),r.update(c)),c.material.uniforms.t2D.value=S,c.material.uniforms.backgroundIntensity.value=E.backgroundIntensity,c.material.toneMapped=at.getTransfer(S.colorSpace)!==Mt,S.matrixAutoUpdate===!0&&S.updateMatrix(),c.material.uniforms.uvTransform.value.copy(S.matrix),(d!==S||h!==S.version||f!==n.toneMapping)&&(c.material.needsUpdate=!0,d=S,h=S.version,f=n.toneMapping),c.layers.enableAll(),w.unshift(c,c.geometry,c.material,0,0,null))}function p(w,E){w.getRGB(Bl,By(n)),i.buffers.color.setClear(Bl.r,Bl.g,Bl.b,E,o)}return{getClearColor:function(){return a},setClearColor:function(w,E=1){a.set(w),l=E,p(a,l)},getClearAlpha:function(){return l},setClearAlpha:function(w){l=w,p(a,l)},render:_,addToRenderList:m}}function wA(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=h(null),s=r,o=!1;function a(v,M,j,z,Y){let J=!1,W=d(z,j,M);s!==W&&(s=W,c(s.object)),J=f(v,z,j,Y),J&&g(v,z,j,Y),Y!==null&&e.update(Y,n.ELEMENT_ARRAY_BUFFER),(J||o)&&(o=!1,S(v,M,j,z),Y!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(Y).buffer))}function l(){return n.createVertexArray()}function c(v){return n.bindVertexArray(v)}function u(v){return n.deleteVertexArray(v)}function d(v,M,j){let z=j.wireframe===!0,Y=i[v.id];Y===void 0&&(Y={},i[v.id]=Y);let J=Y[M.id];J===void 0&&(J={},Y[M.id]=J);let W=J[z];return W===void 0&&(W=h(l()),J[z]=W),W}function h(v){let M=[],j=[],z=[];for(let Y=0;Y<t;Y++)M[Y]=0,j[Y]=0,z[Y]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:M,enabledAttributes:j,attributeDivisors:z,object:v,attributes:{},index:null}}function f(v,M,j,z){let Y=s.attributes,J=M.attributes,W=0,K=j.getAttributes();for(let G in K)if(K[G].location>=0){let he=Y[G],xe=J[G];if(xe===void 0&&(G==="instanceMatrix"&&v.instanceMatrix&&(xe=v.instanceMatrix),G==="instanceColor"&&v.instanceColor&&(xe=v.instanceColor)),he===void 0||he.attribute!==xe||xe&&he.data!==xe.data)return!0;W++}return s.attributesNum!==W||s.index!==z}function g(v,M,j,z){let Y={},J=M.attributes,W=0,K=j.getAttributes();for(let G in K)if(K[G].location>=0){let he=J[G];he===void 0&&(G==="instanceMatrix"&&v.instanceMatrix&&(he=v.instanceMatrix),G==="instanceColor"&&v.instanceColor&&(he=v.instanceColor));let xe={};xe.attribute=he,he&&he.data&&(xe.data=he.data),Y[G]=xe,W++}s.attributes=Y,s.attributesNum=W,s.index=z}function _(){let v=s.newAttributes;for(let M=0,j=v.length;M<j;M++)v[M]=0}function m(v){p(v,0)}function p(v,M){let j=s.newAttributes,z=s.enabledAttributes,Y=s.attributeDivisors;j[v]=1,z[v]===0&&(n.enableVertexAttribArray(v),z[v]=1),Y[v]!==M&&(n.vertexAttribDivisor(v,M),Y[v]=M)}function w(){let v=s.newAttributes,M=s.enabledAttributes;for(let j=0,z=M.length;j<z;j++)M[j]!==v[j]&&(n.disableVertexAttribArray(j),M[j]=0)}function E(v,M,j,z,Y,J,W){W===!0?n.vertexAttribIPointer(v,M,j,Y,J):n.vertexAttribPointer(v,M,j,z,Y,J)}function S(v,M,j,z){_();let Y=z.attributes,J=j.getAttributes(),W=M.defaultAttributeValues;for(let K in J){let G=J[K];if(G.location>=0){let de=Y[K];if(de===void 0&&(K==="instanceMatrix"&&v.instanceMatrix&&(de=v.instanceMatrix),K==="instanceColor"&&v.instanceColor&&(de=v.instanceColor)),de!==void 0){let he=de.normalized,xe=de.itemSize,tt=e.get(de);if(tt===void 0)continue;let ct=tt.buffer,$=tt.type,te=tt.bytesPerElement,ye=$===n.INT||$===n.UNSIGNED_INT||de.gpuType===Zf;if(de.isInterleavedBufferAttribute){let fe=de.data,Fe=fe.stride,Ce=de.offset;if(fe.isInstancedInterleavedBuffer){for(let Xe=0;Xe<G.locationSize;Xe++)p(G.location+Xe,fe.meshPerAttribute);v.isInstancedMesh!==!0&&z._maxInstanceCount===void 0&&(z._maxInstanceCount=fe.meshPerAttribute*fe.count)}else for(let Xe=0;Xe<G.locationSize;Xe++)m(G.location+Xe);n.bindBuffer(n.ARRAY_BUFFER,ct);for(let Xe=0;Xe<G.locationSize;Xe++)E(G.location+Xe,xe/G.locationSize,$,he,Fe*te,(Ce+xe/G.locationSize*Xe)*te,ye)}else{if(de.isInstancedBufferAttribute){for(let fe=0;fe<G.locationSize;fe++)p(G.location+fe,de.meshPerAttribute);v.isInstancedMesh!==!0&&z._maxInstanceCount===void 0&&(z._maxInstanceCount=de.meshPerAttribute*de.count)}else for(let fe=0;fe<G.locationSize;fe++)m(G.location+fe);n.bindBuffer(n.ARRAY_BUFFER,ct);for(let fe=0;fe<G.locationSize;fe++)E(G.location+fe,xe/G.locationSize,$,he,xe*te,xe/G.locationSize*fe*te,ye)}}else if(W!==void 0){let he=W[K];if(he!==void 0)switch(he.length){case 2:n.vertexAttrib2fv(G.location,he);break;case 3:n.vertexAttrib3fv(G.location,he);break;case 4:n.vertexAttrib4fv(G.location,he);break;default:n.vertexAttrib1fv(G.location,he)}}}}w()}function k(){V();for(let v in i){let M=i[v];for(let j in M){let z=M[j];for(let Y in z)u(z[Y].object),delete z[Y];delete M[j]}delete i[v]}}function R(v){if(i[v.id]===void 0)return;let M=i[v.id];for(let j in M){let z=M[j];for(let Y in z)u(z[Y].object),delete z[Y];delete M[j]}delete i[v.id]}function D(v){for(let M in i){let j=i[M];if(j[v.id]===void 0)continue;let z=j[v.id];for(let Y in z)u(z[Y].object),delete z[Y];delete j[v.id]}}function V(){re(),o=!0,s!==r&&(s=r,c(s.object))}function re(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:V,resetDefaultState:re,dispose:k,releaseStatesOfGeometry:R,releaseStatesOfProgram:D,initAttributes:_,enableAttribute:m,disableUnusedAttributes:w}}function CA(n,e,t){let i;function r(c){i=c}function s(c,u){n.drawArrays(i,c,u),t.update(u,i,1)}function o(c,u,d){d!==0&&(n.drawArraysInstanced(i,c,u,d),t.update(u,i,d))}function a(c,u,d){if(d===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,c,0,u,0,d);let f=0;for(let g=0;g<d;g++)f+=u[g];t.update(f,i,1)}function l(c,u,d,h){if(d===0)return;let f=e.get("WEBGL_multi_draw");if(f===null)for(let g=0;g<c.length;g++)o(c[g],u[g],h[g]);else{f.multiDrawArraysInstancedWEBGL(i,c,0,u,0,h,0,d);let g=0;for(let _=0;_<d;_++)g+=u[_];for(let _=0;_<h.length;_++)t.update(g,i,h[_])}}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a,this.renderMultiDrawInstances=l}function DA(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let D=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(D.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(D){return!(D!==Bn&&i.convert(D)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(D){let V=D===ko&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(D!==bi&&i.convert(D)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&D!==_i&&!V)}function l(D){if(D==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";D="mediump"}return D==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let c=t.precision!==void 0?t.precision:"highp",u=l(c);u!==c&&(console.warn("THREE.WebGLRenderer:",c,"not supported, using",u,"instead."),c=u);let d=t.logarithmicDepthBuffer===!0,h=t.reverseDepthBuffer===!0&&e.has("EXT_clip_control");if(h===!0){let D=e.get("EXT_clip_control");D.clipControlEXT(D.LOWER_LEFT_EXT,D.ZERO_TO_ONE_EXT)}let f=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),g=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),_=n.getParameter(n.MAX_TEXTURE_SIZE),m=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),w=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),E=n.getParameter(n.MAX_VARYING_VECTORS),S=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),k=g>0,R=n.getParameter(n.MAX_SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:l,textureFormatReadable:o,textureTypeReadable:a,precision:c,logarithmicDepthBuffer:d,reverseDepthBuffer:h,maxTextures:f,maxVertexTextures:g,maxTextureSize:_,maxCubemapSize:m,maxAttributes:p,maxVertexUniforms:w,maxVaryings:E,maxFragmentUniforms:S,vertexTextures:k,maxSamples:R}}function TA(n){let e=this,t=null,i=0,r=!1,s=!1,o=new vi,a=new ke,l={value:null,needsUpdate:!1};this.uniform=l,this.numPlanes=0,this.numIntersection=0,this.init=function(d,h){let f=d.length!==0||h||i!==0||r;return r=h,i=d.length,f},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(d,h){t=u(d,h,0)},this.setState=function(d,h,f){let g=d.clippingPlanes,_=d.clipIntersection,m=d.clipShadows,p=n.get(d);if(!r||g===null||g.length===0||s&&!m)s?u(null):c();else{let w=s?0:i,E=w*4,S=p.clippingState||null;l.value=S,S=u(g,h,E,f);for(let k=0;k!==E;++k)S[k]=t[k];p.clippingState=S,this.numIntersection=_?this.numPlanes:0,this.numPlanes+=w}};function c(){l.value!==t&&(l.value=t,l.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(d,h,f,g){let _=d!==null?d.length:0,m=null;if(_!==0){if(m=l.value,g!==!0||m===null){let p=f+_*4,w=h.matrixWorldInverse;a.getNormalMatrix(w),(m===null||m.length<p)&&(m=new Float32Array(p));for(let E=0,S=f;E!==_;++E,S+=4)o.copy(d[E]).applyMatrix4(w,a),o.normal.toArray(m,S),m[S+3]=o.constant}l.value=m,l.needsUpdate=!0}return e.numPlanes=_,e.numIntersection=0,m}}function AA(n){let e=new WeakMap;function t(o,a){return a===Gh?o.mapping=Hs:a===Wh&&(o.mapping=zs),o}function i(o){if(o&&o.isTexture){let a=o.mapping;if(a===Gh||a===Wh)if(e.has(o)){let l=e.get(o).texture;return t(l,o.mapping)}else{let l=o.image;if(l&&l.height>0){let c=new Df(l.height);return c.fromEquirectangularTexture(n,o),e.set(o,c),o.addEventListener("dispose",r),t(c.texture,o.mapping)}else return null}}return o}function r(o){let a=o.target;a.removeEventListener("dispose",r);let l=e.get(a);l!==void 0&&(e.delete(a),l.dispose())}function s(){e=new WeakMap}return{get:i,dispose:s}}var dc=class extends lc{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,l=r-t;if(this.view!==null&&this.view.enabled){let c=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=c*this.view.offsetX,o=s+c*this.view.width,a-=u*this.view.offsetY,l=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,l,this.near,this.far,this.coordinateSystem),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}},Os=4,Q0=[.125,.215,.35,.446,.526,.582],Dr=20,Dh=new dc,ey=new Je,Th=null,Ah=0,Ih=0,Rh=!1,Sr=(1+Math.sqrt(5))/2,Fs=1/Sr,ty=[new B(-Sr,Fs,0),new B(Sr,Fs,0),new B(-Fs,0,Sr),new B(Fs,0,Sr),new B(0,Sr,-Fs),new B(0,Sr,Fs),new B(-1,1,-1),new B(1,1,-1),new B(-1,1,1),new B(1,1,1)],hc=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._lodPlanes=[],this._sizeLods=[],this._sigmas=[],this._blurMaterial=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._compileMaterial(this._blurMaterial)}fromScene(e,t=0,i=.1,r=100){Th=this._renderer.getRenderTarget(),Ah=this._renderer.getActiveCubeFace(),Ih=this._renderer.getActiveMipmapLevel(),Rh=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(256);let s=this._allocateTargets();return s.depthBuffer=!0,this._sceneToCubeUV(e,i,r,s),t>0&&this._blur(s,0,0,t),this._applyPMREM(s),this._cleanup(s),s}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=ry(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=iy(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose()}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodPlanes.length;e++)this._lodPlanes[e].dispose()}_cleanup(e){this._renderer.setRenderTarget(Th,Ah,Ih),this._renderer.xr.enabled=Rh,e.scissorTest=!1,Hl(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===Hs||e.mapping===zs?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),Th=this._renderer.getRenderTarget(),Ah=this._renderer.getActiveCubeFace(),Ih=this._renderer.getActiveMipmapLevel(),Rh=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:Vn,minFilter:Vn,generateMipmaps:!1,type:ko,format:Bn,colorSpace:Qi,depthBuffer:!1},r=ny(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=ny(e,t,i);let{_lodMax:s}=this;({sizeLods:this._sizeLods,lodPlanes:this._lodPlanes,sigmas:this._sigmas}=IA(s)),this._blurMaterial=RA(s,e,t)}return r}_compileMaterial(e){let t=new ln(this._lodPlanes[0],e);this._renderer.compile(t,Dh)}_sceneToCubeUV(e,t,i,r){let a=new en(90,1,t,i),l=[1,-1,1,1,1,1],c=[1,1,1,-1,-1,-1],u=this._renderer,d=u.autoClear,h=u.toneMapping;u.getClearColor(ey),u.toneMapping=Xi,u.autoClear=!1;let f=new sc({name:"PMREM.Background",side:cn,depthWrite:!1,depthTest:!1}),g=new ln(new Ji,f),_=!1,m=e.background;m?m.isColor&&(f.color.copy(m),e.background=null,_=!0):(f.color.copy(ey),_=!0);for(let p=0;p<6;p++){let w=p%3;w===0?(a.up.set(0,l[p],0),a.lookAt(c[p],0,0)):w===1?(a.up.set(0,0,l[p]),a.lookAt(0,c[p],0)):(a.up.set(0,l[p],0),a.lookAt(0,0,c[p]));let E=this._cubeSize;Hl(r,w*E,p>2?E:0,E,E),u.setRenderTarget(r),_&&u.render(g,a),u.render(e,a)}g.geometry.dispose(),g.material.dispose(),u.toneMapping=h,u.autoClear=d,e.background=m}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===Hs||e.mapping===zs;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=ry()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=iy());let s=r?this._cubemapMaterial:this._equirectMaterial,o=new ln(this._lodPlanes[0],s),a=s.uniforms;a.envMap.value=e;let l=this._cubeSize;Hl(t,0,0,3*l,2*l),i.setRenderTarget(t),i.render(o,Dh)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodPlanes.length;for(let s=1;s<r;s++){let o=Math.sqrt(this._sigmas[s]*this._sigmas[s]-this._sigmas[s-1]*this._sigmas[s-1]),a=ty[(r-s-1)%ty.length];this._blur(e,s-1,s,o,a)}t.autoClear=i}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let l=this._renderer,c=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&console.error("blur direction must be either latitudinal or longitudinal!");let u=3,d=new ln(this._lodPlanes[r],c),h=c.uniforms,f=this._sizeLods[i]-1,g=isFinite(s)?Math.PI/(2*f):2*Math.PI/(2*Dr-1),_=s/g,m=isFinite(s)?1+Math.floor(u*_):Dr;m>Dr&&console.warn(`sigmaRadians, ${s}, is too large and will clip, as it requested ${m} samples when the maximum is set to ${Dr}`);let p=[],w=0;for(let D=0;D<Dr;++D){let V=D/_,re=Math.exp(-V*V/2);p.push(re),D===0?w+=re:D<m&&(w+=2*re)}for(let D=0;D<p.length;D++)p[D]=p[D]/w;h.envMap.value=e.texture,h.samples.value=m,h.weights.value=p,h.latitudinal.value=o==="latitudinal",a&&(h.poleAxis.value=a);let{_lodMax:E}=this;h.dTheta.value=g,h.mipInt.value=E-i;let S=this._sizeLods[r],k=3*S*(r>E-Os?r-E+Os:0),R=4*(this._cubeSize-S);Hl(t,k,R,3*S,2*S),l.setRenderTarget(t),l.render(d,Dh)}};function IA(n){let e=[],t=[],i=[],r=n,s=n-Os+1+Q0.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);t.push(a);let l=1/a;o>n-Os?l=Q0[o-n+Os-1]:o===0&&(l=0),i.push(l);let c=1/(a-2),u=-c,d=1+c,h=[u,u,d,u,d,d,u,u,d,d,u,d],f=6,g=6,_=3,m=2,p=1,w=new Float32Array(_*g*f),E=new Float32Array(m*g*f),S=new Float32Array(p*g*f);for(let R=0;R<f;R++){let D=R%3*2/3-1,V=R>2?0:-1,re=[D,V,0,D+2/3,V,0,D+2/3,V+1,0,D,V,0,D+2/3,V+1,0,D,V+1,0];w.set(re,_*g*R),E.set(h,m*g*R);let v=[R,R,R,R,R,R];S.set(v,p*g*R)}let k=new Fr;k.setAttribute("position",new An(w,_)),k.setAttribute("uv",new An(E,m)),k.setAttribute("faceIndex",new An(S,p)),e.push(k),r>Os&&r--}return{lodPlanes:e,sizeLods:t,sigmas:i}}function ny(n,e,t){let i=new Si(n,e,t);return i.texture.mapping=xc,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function Hl(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function RA(n,e,t){let i=new Float32Array(Dr),r=new B(0,1,0);return new Qn({name:"SphericalGaussianBlur",defines:{n:Dr,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:ip(),fragmentShader:`

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
		`,blending:qi,depthTest:!1,depthWrite:!1})}function iy(){return new Qn({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:ip(),fragmentShader:`

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
		`,blending:qi,depthTest:!1,depthWrite:!1})}function ry(){return new Qn({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:ip(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:qi,depthTest:!1,depthWrite:!1})}function ip(){return`

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
	`}function NA(n){let e=new WeakMap,t=null;function i(a){if(a&&a.isTexture){let l=a.mapping,c=l===Gh||l===Wh,u=l===Hs||l===zs;if(c||u){let d=e.get(a),h=d!==void 0?d.texture.pmremVersion:0;if(a.isRenderTargetTexture&&a.pmremVersion!==h)return t===null&&(t=new hc(n)),d=c?t.fromEquirectangular(a,d):t.fromCubemap(a,d),d.texture.pmremVersion=a.pmremVersion,e.set(a,d),d.texture;if(d!==void 0)return d.texture;{let f=a.image;return c&&f&&f.height>0||u&&f&&r(f)?(t===null&&(t=new hc(n)),d=c?t.fromEquirectangular(a):t.fromCubemap(a),d.texture.pmremVersion=a.pmremVersion,e.set(a,d),a.addEventListener("dispose",s),d.texture):null}}}return a}function r(a){let l=0,c=6;for(let u=0;u<c;u++)a[u]!==void 0&&l++;return l===c}function s(a){let l=a.target;l.removeEventListener("dispose",s);let c=e.get(l);c!==void 0&&(e.delete(l),c.dispose())}function o(){e=new WeakMap,t!==null&&(t.dispose(),t=null)}return{get:i,dispose:o}}function PA(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r;switch(i){case"WEBGL_depth_texture":r=n.getExtension("WEBGL_depth_texture")||n.getExtension("MOZ_WEBGL_depth_texture")||n.getExtension("WEBKIT_WEBGL_depth_texture");break;case"EXT_texture_filter_anisotropic":r=n.getExtension("EXT_texture_filter_anisotropic")||n.getExtension("MOZ_EXT_texture_filter_anisotropic")||n.getExtension("WEBKIT_EXT_texture_filter_anisotropic");break;case"WEBGL_compressed_texture_s3tc":r=n.getExtension("WEBGL_compressed_texture_s3tc")||n.getExtension("MOZ_WEBGL_compressed_texture_s3tc")||n.getExtension("WEBKIT_WEBGL_compressed_texture_s3tc");break;case"WEBGL_compressed_texture_pvrtc":r=n.getExtension("WEBGL_compressed_texture_pvrtc")||n.getExtension("WEBKIT_WEBGL_compressed_texture_pvrtc");break;default:r=n.getExtension(i)}return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&Yl("THREE.WebGLRenderer: "+i+" extension not supported."),r}}}function FA(n,e,t,i){let r={},s=new WeakMap;function o(d){let h=d.target;h.index!==null&&e.remove(h.index);for(let g in h.attributes)e.remove(h.attributes[g]);for(let g in h.morphAttributes){let _=h.morphAttributes[g];for(let m=0,p=_.length;m<p;m++)e.remove(_[m])}h.removeEventListener("dispose",o),delete r[h.id];let f=s.get(h);f&&(e.remove(f),s.delete(h)),i.releaseStatesOfGeometry(h),h.isInstancedBufferGeometry===!0&&delete h._maxInstanceCount,t.memory.geometries--}function a(d,h){return r[h.id]===!0||(h.addEventListener("dispose",o),r[h.id]=!0,t.memory.geometries++),h}function l(d){let h=d.attributes;for(let g in h)e.update(h[g],n.ARRAY_BUFFER);let f=d.morphAttributes;for(let g in f){let _=f[g];for(let m=0,p=_.length;m<p;m++)e.update(_[m],n.ARRAY_BUFFER)}}function c(d){let h=[],f=d.index,g=d.attributes.position,_=0;if(f!==null){let w=f.array;_=f.version;for(let E=0,S=w.length;E<S;E+=3){let k=w[E+0],R=w[E+1],D=w[E+2];h.push(k,R,R,D,D,k)}}else if(g!==void 0){let w=g.array;_=g.version;for(let E=0,S=w.length/3-1;E<S;E+=3){let k=E+0,R=E+1,D=E+2;h.push(k,R,R,D,D,k)}}else return;let m=new(Uy(h)?ac:oc)(h,1);m.version=_;let p=s.get(d);p&&e.remove(p),s.set(d,m)}function u(d){let h=s.get(d);if(h){let f=d.index;f!==null&&h.version<f.version&&c(d)}else c(d);return s.get(d)}return{get:a,update:l,getWireframeAttribute:u}}function OA(n,e,t){let i;function r(h){i=h}let s,o;function a(h){s=h.type,o=h.bytesPerElement}function l(h,f){n.drawElements(i,f,s,h*o),t.update(f,i,1)}function c(h,f,g){g!==0&&(n.drawElementsInstanced(i,f,s,h*o,g),t.update(f,i,g))}function u(h,f,g){if(g===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,f,0,s,h,0,g);let m=0;for(let p=0;p<g;p++)m+=f[p];t.update(m,i,1)}function d(h,f,g,_){if(g===0)return;let m=e.get("WEBGL_multi_draw");if(m===null)for(let p=0;p<h.length;p++)c(h[p]/o,f[p],_[p]);else{m.multiDrawElementsInstancedWEBGL(i,f,0,s,h,0,_,0,g);let p=0;for(let w=0;w<g;w++)p+=f[w];for(let w=0;w<_.length;w++)t.update(p,i,_[w])}}this.setMode=r,this.setIndex=a,this.render=l,this.renderInstances=c,this.renderMultiDraw=u,this.renderMultiDrawInstances=d}function LA(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:console.error("THREE.WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function kA(n,e,t){let i=new WeakMap,r=new Dt;function s(o,a,l){let c=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,d=u!==void 0?u.length:0,h=i.get(a);if(h===void 0||h.count!==d){let v=function(){V.dispose(),i.delete(a),a.removeEventListener("dispose",v)};var f=v;h!==void 0&&h.texture.dispose();let g=a.morphAttributes.position!==void 0,_=a.morphAttributes.normal!==void 0,m=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],w=a.morphAttributes.normal||[],E=a.morphAttributes.color||[],S=0;g===!0&&(S=1),_===!0&&(S=2),m===!0&&(S=3);let k=a.attributes.position.count*S,R=1;k>e.maxTextureSize&&(R=Math.ceil(k/e.maxTextureSize),k=e.maxTextureSize);let D=new Float32Array(k*R*4*d),V=new ic(D,k,R,d);V.type=_i,V.needsUpdate=!0;let re=S*4;for(let M=0;M<d;M++){let j=p[M],z=w[M],Y=E[M],J=k*R*4*M;for(let W=0;W<j.count;W++){let K=W*re;g===!0&&(r.fromBufferAttribute(j,W),D[J+K+0]=r.x,D[J+K+1]=r.y,D[J+K+2]=r.z,D[J+K+3]=0),_===!0&&(r.fromBufferAttribute(z,W),D[J+K+4]=r.x,D[J+K+5]=r.y,D[J+K+6]=r.z,D[J+K+7]=0),m===!0&&(r.fromBufferAttribute(Y,W),D[J+K+8]=r.x,D[J+K+9]=r.y,D[J+K+10]=r.z,D[J+K+11]=Y.itemSize===4?r.w:1)}}h={count:d,texture:V,size:new lt(k,R)},i.set(a,h),a.addEventListener("dispose",v)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)l.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let g=0;for(let m=0;m<c.length;m++)g+=c[m];let _=a.morphTargetsRelative?1:1-g;l.getUniforms().setValue(n,"morphTargetBaseInfluence",_),l.getUniforms().setValue(n,"morphTargetInfluences",c)}l.getUniforms().setValue(n,"morphTargetsTexture",h.texture,t),l.getUniforms().setValue(n,"morphTargetsTextureSize",h.size)}return{update:s}}function UA(n,e,t,i){let r=new WeakMap;function s(l){let c=i.render.frame,u=l.geometry,d=e.get(l,u);if(r.get(d)!==c&&(e.update(d),r.set(d,c)),l.isInstancedMesh&&(l.hasEventListener("dispose",a)===!1&&l.addEventListener("dispose",a),r.get(l)!==c&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),r.set(l,c))),l.isSkinnedMesh){let h=l.skeleton;r.get(h)!==c&&(h.update(),r.set(h,c))}return d}function o(){r=new WeakMap}function a(l){let c=l.target;c.removeEventListener("dispose",a),t.remove(c.instanceMatrix),c.instanceColor!==null&&t.remove(c.instanceColor)}return{update:s,dispose:o}}var fc=class extends kr{constructor(e,t,i,r,s,o,a,l,c,u=ks){if(u!==ks&&u!==Ws)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");i===void 0&&u===ks&&(i=Ir),i===void 0&&u===Ws&&(i=Gs),super(null,r,s,o,a,l,u,i,c),this.isDepthTexture=!0,this.image={width:e,height:t},this.magFilter=a!==void 0?a:Tn,this.minFilter=l!==void 0?l:Tn,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},zy=new kr,sy=new fc(1,1),Gy=new ic,Wy=new Sf,jy=new cc,oy=[],ay=[],ly=new Float32Array(16),cy=new Float32Array(9),uy=new Float32Array(4);function qs(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=oy[r];if(s===void 0&&(s=new Float32Array(r),oy[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function Lt(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function kt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function Ec(n,e){let t=ay[e];t===void 0&&(t=new Int32Array(e),ay[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function VA(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function BA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Lt(t,e))return;n.uniform2fv(this.addr,e),kt(t,e)}}function HA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(Lt(t,e))return;n.uniform3fv(this.addr,e),kt(t,e)}}function zA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Lt(t,e))return;n.uniform4fv(this.addr,e),kt(t,e)}}function GA(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Lt(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),kt(t,e)}else{if(Lt(t,i))return;uy.set(i),n.uniformMatrix2fv(this.addr,!1,uy),kt(t,i)}}function WA(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Lt(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),kt(t,e)}else{if(Lt(t,i))return;cy.set(i),n.uniformMatrix3fv(this.addr,!1,cy),kt(t,i)}}function jA(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Lt(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),kt(t,e)}else{if(Lt(t,i))return;ly.set(i),n.uniformMatrix4fv(this.addr,!1,ly),kt(t,i)}}function $A(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function qA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Lt(t,e))return;n.uniform2iv(this.addr,e),kt(t,e)}}function XA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Lt(t,e))return;n.uniform3iv(this.addr,e),kt(t,e)}}function YA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Lt(t,e))return;n.uniform4iv(this.addr,e),kt(t,e)}}function ZA(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function KA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Lt(t,e))return;n.uniform2uiv(this.addr,e),kt(t,e)}}function JA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Lt(t,e))return;n.uniform3uiv(this.addr,e),kt(t,e)}}function QA(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Lt(t,e))return;n.uniform4uiv(this.addr,e),kt(t,e)}}function e1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(sy.compareFunction=ky,s=sy):s=zy,t.setTexture2D(e||s,r)}function t1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||Wy,r)}function n1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||jy,r)}function i1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||Gy,r)}function r1(n){switch(n){case 5126:return VA;case 35664:return BA;case 35665:return HA;case 35666:return zA;case 35674:return GA;case 35675:return WA;case 35676:return jA;case 5124:case 35670:return $A;case 35667:case 35671:return qA;case 35668:case 35672:return XA;case 35669:case 35673:return YA;case 5125:return ZA;case 36294:return KA;case 36295:return JA;case 36296:return QA;case 35678:case 36198:case 36298:case 36306:case 35682:return e1;case 35679:case 36299:case 36307:return t1;case 35680:case 36300:case 36308:case 36293:return n1;case 36289:case 36303:case 36311:case 36292:return i1}}function s1(n,e){n.uniform1fv(this.addr,e)}function o1(n,e){let t=qs(e,this.size,2);n.uniform2fv(this.addr,t)}function a1(n,e){let t=qs(e,this.size,3);n.uniform3fv(this.addr,t)}function l1(n,e){let t=qs(e,this.size,4);n.uniform4fv(this.addr,t)}function c1(n,e){let t=qs(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function u1(n,e){let t=qs(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function d1(n,e){let t=qs(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function h1(n,e){n.uniform1iv(this.addr,e)}function f1(n,e){n.uniform2iv(this.addr,e)}function p1(n,e){n.uniform3iv(this.addr,e)}function m1(n,e){n.uniform4iv(this.addr,e)}function g1(n,e){n.uniform1uiv(this.addr,e)}function v1(n,e){n.uniform2uiv(this.addr,e)}function y1(n,e){n.uniform3uiv(this.addr,e)}function _1(n,e){n.uniform4uiv(this.addr,e)}function x1(n,e,t){let i=this.cache,r=e.length,s=Ec(t,r);Lt(i,s)||(n.uniform1iv(this.addr,s),kt(i,s));for(let o=0;o!==r;++o)t.setTexture2D(e[o]||zy,s[o])}function M1(n,e,t){let i=this.cache,r=e.length,s=Ec(t,r);Lt(i,s)||(n.uniform1iv(this.addr,s),kt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||Wy,s[o])}function E1(n,e,t){let i=this.cache,r=e.length,s=Ec(t,r);Lt(i,s)||(n.uniform1iv(this.addr,s),kt(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||jy,s[o])}function b1(n,e,t){let i=this.cache,r=e.length,s=Ec(t,r);Lt(i,s)||(n.uniform1iv(this.addr,s),kt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||Gy,s[o])}function S1(n){switch(n){case 5126:return s1;case 35664:return o1;case 35665:return a1;case 35666:return l1;case 35674:return c1;case 35675:return u1;case 35676:return d1;case 5124:case 35670:return h1;case 35667:case 35671:return f1;case 35668:case 35672:return p1;case 35669:case 35673:return m1;case 5125:return g1;case 36294:return v1;case 36295:return y1;case 36296:return _1;case 35678:case 36198:case 36298:case 36306:case 35682:return x1;case 35679:case 36299:case 36307:return M1;case 35680:case 36300:case 36308:case 36293:return E1;case 36289:case 36303:case 36311:case 36292:return b1}}var Tf=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=r1(t.type)}},Af=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=S1(t.type)}},If=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},Nh=/(\w+)(\])?(\[|\.)?/g;function dy(n,e){n.seq.push(e),n.map[e.id]=e}function w1(n,e,t){let i=n.name,r=i.length;for(Nh.lastIndex=0;;){let s=Nh.exec(i),o=Nh.lastIndex,a=s[1],l=s[2]==="]",c=s[3];if(l&&(a=a|0),c===void 0||c==="["&&o+2===r){dy(t,c===void 0?new Tf(a,n,e):new Af(a,n,e));break}else{let d=t.map[a];d===void 0&&(d=new If(a),dy(t,d)),t=d}}}var Vs=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let r=0;r<i;++r){let s=e.getActiveUniform(t,r),o=e.getUniformLocation(t,s.name);w1(s,o,this)}}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],l=i[a.id];l.needsUpdate!==!1&&a.setValue(e,l.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function hy(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var C1=37297,D1=0;function T1(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}function A1(n){let e=at.getPrimaries(at.workingColorSpace),t=at.getPrimaries(n),i;switch(e===t?i="":e===Ql&&t===Jl?i="LinearDisplayP3ToLinearSRGB":e===Jl&&t===Ql&&(i="LinearSRGBToLinearDisplayP3"),n){case Qi:case Mc:return[i,"LinearTransferOETF"];case Kn:case np:return[i,"sRGBTransferOETF"];default:return console.warn("THREE.WebGLProgram: Unsupported color space:",n),[i,"LinearTransferOETF"]}}function fy(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),r=n.getShaderInfoLog(e).trim();if(i&&r==="")return"";let s=/ERROR: 0:(\d+)/.exec(r);if(s){let o=parseInt(s[1]);return t.toUpperCase()+`

`+r+`

`+T1(n.getShaderSource(e),o)}else return r}function I1(n,e){let t=A1(e);return`vec4 ${n}( vec4 value ) { return ${t[0]}( ${t[1]}( value ) ); }`}function R1(n,e){let t;switch(e){case tC:t="Linear";break;case nC:t="Reinhard";break;case iC:t="Cineon";break;case rC:t="ACESFilmic";break;case oC:t="AgX";break;case aC:t="Neutral";break;case sC:t="Custom";break;default:console.warn("THREE.WebGLProgram: Unsupported toneMapping:",e),t="Linear"}return"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var zl=new B;function N1(){at.getLuminanceCoefficients(zl);let n=zl.x.toFixed(4),e=zl.y.toFixed(4),t=zl.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function P1(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(Ro).join(`
`)}function F1(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function O1(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function Ro(n){return n!==""}function py(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function my(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var L1=/^[ \t]*#include +<([\w\d./]+)>/gm;function Rf(n){return n.replace(L1,U1)}var k1=new Map;function U1(n,e){let t=Le[e];if(t===void 0){let i=k1.get(e);if(i!==void 0)t=Le[i],console.warn('THREE.WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return Rf(t)}var V1=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function gy(n){return n.replace(V1,B1)}function B1(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function vy(n){let e=`precision ${n.precision} float;
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
#define LOW_PRECISION`),e}function H1(n){let e="SHADOWMAP_TYPE_BASIC";return n.shadowMapType===Sy?e="SHADOWMAP_TYPE_PCF":n.shadowMapType===Fw?e="SHADOWMAP_TYPE_PCF_SOFT":n.shadowMapType===gi&&(e="SHADOWMAP_TYPE_VSM"),e}function z1(n){let e="ENVMAP_TYPE_CUBE";if(n.envMap)switch(n.envMapMode){case Hs:case zs:e="ENVMAP_TYPE_CUBE";break;case xc:e="ENVMAP_TYPE_CUBE_UV";break}return e}function G1(n){let e="ENVMAP_MODE_REFLECTION";if(n.envMap)switch(n.envMapMode){case zs:e="ENVMAP_MODE_REFRACTION";break}return e}function W1(n){let e="ENVMAP_BLENDING_NONE";if(n.envMap)switch(n.combine){case wy:e="ENVMAP_BLENDING_MULTIPLY";break;case Qw:e="ENVMAP_BLENDING_MIX";break;case eC:e="ENVMAP_BLENDING_ADD";break}return e}function j1(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),7*16)),texelHeight:i,maxMip:t}}function $1(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,l=H1(t),c=z1(t),u=G1(t),d=W1(t),h=j1(t),f=P1(t),g=F1(s),_=r.createProgram(),m,p,w=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(Ro).join(`
`),m.length>0&&(m+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(Ro).join(`
`),p.length>0&&(p+=`
`)):(m=[vy(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+l:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGDEPTHBUF":"",t.reverseDepthBuffer?"#define USE_REVERSEDEPTHBUF":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(Ro).join(`
`),p=[vy(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+c:"",t.envMap?"#define "+u:"",t.envMap?"#define "+d:"",h?"#define CUBEUV_TEXEL_WIDTH "+h.texelWidth:"",h?"#define CUBEUV_TEXEL_HEIGHT "+h.texelHeight:"",h?"#define CUBEUV_MAX_MIP "+h.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor||t.batchingColor?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+l:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.logarithmicDepthBuffer?"#define USE_LOGDEPTHBUF":"",t.reverseDepthBuffer?"#define USE_REVERSEDEPTHBUF":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==Xi?"#define TONE_MAPPING":"",t.toneMapping!==Xi?Le.tonemapping_pars_fragment:"",t.toneMapping!==Xi?R1("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",Le.colorspace_pars_fragment,I1("linearToOutputTexel",t.outputColorSpace),N1(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(Ro).join(`
`)),o=Rf(o),o=py(o,t),o=my(o,t),a=Rf(a),a=py(a,t),a=my(a,t),o=gy(o),a=gy(a),t.isRawShaderMaterial!==!0&&(w=`#version 300 es
`,m=[f,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+m,p=["#define varying in",t.glslVersion===O0?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===O0?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let E=w+m+o,S=w+p+a,k=hy(r,r.VERTEX_SHADER,E),R=hy(r,r.FRAGMENT_SHADER,S);r.attachShader(_,k),r.attachShader(_,R),t.index0AttributeName!==void 0?r.bindAttribLocation(_,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(_,0,"position"),r.linkProgram(_);function D(M){if(n.debug.checkShaderErrors){let j=r.getProgramInfoLog(_).trim(),z=r.getShaderInfoLog(k).trim(),Y=r.getShaderInfoLog(R).trim(),J=!0,W=!0;if(r.getProgramParameter(_,r.LINK_STATUS)===!1)if(J=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,_,k,R);else{let K=fy(r,k,"vertex"),G=fy(r,R,"fragment");console.error("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(_,r.VALIDATE_STATUS)+`

Material Name: `+M.name+`
Material Type: `+M.type+`

Program Info Log: `+j+`
`+K+`
`+G)}else j!==""?console.warn("THREE.WebGLProgram: Program Info Log:",j):(z===""||Y==="")&&(W=!1);W&&(M.diagnostics={runnable:J,programLog:j,vertexShader:{log:z,prefix:m},fragmentShader:{log:Y,prefix:p}})}r.deleteShader(k),r.deleteShader(R),V=new Vs(r,_),re=O1(r,_)}let V;this.getUniforms=function(){return V===void 0&&D(this),V};let re;this.getAttributes=function(){return re===void 0&&D(this),re};let v=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return v===!1&&(v=r.getProgramParameter(_,C1)),v},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(_),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=D1++,this.cacheKey=e,this.usedTimes=1,this.program=_,this.vertexShader=k,this.fragmentShader=R,this}var q1=0,Nf=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new Pf(e),t.set(e,i)),i}},Pf=class{constructor(e){this.id=q1++,this.code=e,this.usedTimes=0}};function X1(n,e,t,i,r,s,o){let a=new rc,l=new Nf,c=new Set,u=[],d=r.logarithmicDepthBuffer,h=r.reverseDepthBuffer,f=r.vertexTextures,g=r.precision,_={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distanceRGBA",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function m(v){return c.add(v),v===0?"uv":`uv${v}`}function p(v,M,j,z,Y){let J=z.fog,W=Y.geometry,K=v.isMeshStandardMaterial?z.environment:null,G=(v.isMeshStandardMaterial?t:e).get(v.envMap||K),de=G&&G.mapping===xc?G.image.height:null,he=_[v.type];v.precision!==null&&(g=r.getMaxPrecision(v.precision),g!==v.precision&&console.warn("THREE.WebGLProgram.getParameters:",v.precision,"not supported, using",g,"instead."));let xe=W.morphAttributes.position||W.morphAttributes.normal||W.morphAttributes.color,tt=xe!==void 0?xe.length:0,ct=0;W.morphAttributes.position!==void 0&&(ct=1),W.morphAttributes.normal!==void 0&&(ct=2),W.morphAttributes.color!==void 0&&(ct=3);let $,te,ye,fe;if(he){let nn=Jn[he];$=nn.vertexShader,te=nn.fragmentShader}else $=v.vertexShader,te=v.fragmentShader,l.update(v),ye=l.getVertexShaderID(v),fe=l.getFragmentShaderID(v);let Fe=n.getRenderTarget(),Ce=Y.isInstancedMesh===!0,Xe=Y.isBatchedMesh===!0,dt=!!v.map,Ye=!!v.matcap,A=!!G,un=!!v.aoMap,je=!!v.lightMap,Qe=!!v.bumpMap,Ae=!!v.normalMap,mt=!!v.displacementMap,Ne=!!v.emissiveMap,b=!!v.metalnessMap,y=!!v.roughnessMap,O=v.anisotropy>0,X=v.clearcoat>0,Q=v.dispersion>0,q=v.iridescence>0,Me=v.sheen>0,oe=v.transmission>0,pe=O&&!!v.anisotropyMap,et=X&&!!v.clearcoatMap,ne=X&&!!v.clearcoatNormalMap,me=X&&!!v.clearcoatRoughnessMap,Ie=q&&!!v.iridescenceMap,Re=q&&!!v.iridescenceThicknessMap,ge=Me&&!!v.sheenColorMap,$e=Me&&!!v.sheenRoughnessMap,Oe=!!v.specularMap,ht=!!v.specularColorMap,N=!!v.specularIntensityMap,ce=oe&&!!v.transmissionMap,H=oe&&!!v.thicknessMap,Z=!!v.gradientMap,ae=!!v.alphaMap,ue=v.alphaTest>0,Ze=!!v.alphaHash,Nt=!!v.extensions,tn=Xi;v.toneMapped&&(Fe===null||Fe.isXRRenderTarget===!0)&&(tn=n.toneMapping);let nt={shaderID:he,shaderType:v.type,shaderName:v.name,vertexShader:$,fragmentShader:te,defines:v.defines,customVertexShaderID:ye,customFragmentShaderID:fe,isRawShaderMaterial:v.isRawShaderMaterial===!0,glslVersion:v.glslVersion,precision:g,batching:Xe,batchingColor:Xe&&Y._colorsTexture!==null,instancing:Ce,instancingColor:Ce&&Y.instanceColor!==null,instancingMorph:Ce&&Y.morphTexture!==null,supportsVertexTextures:f,outputColorSpace:Fe===null?n.outputColorSpace:Fe.isXRRenderTarget===!0?Fe.texture.colorSpace:Qi,alphaToCoverage:!!v.alphaToCoverage,map:dt,matcap:Ye,envMap:A,envMapMode:A&&G.mapping,envMapCubeUVHeight:de,aoMap:un,lightMap:je,bumpMap:Qe,normalMap:Ae,displacementMap:f&&mt,emissiveMap:Ne,normalMapObjectSpace:Ae&&v.normalMapType===dC,normalMapTangentSpace:Ae&&v.normalMapType===Ly,metalnessMap:b,roughnessMap:y,anisotropy:O,anisotropyMap:pe,clearcoat:X,clearcoatMap:et,clearcoatNormalMap:ne,clearcoatRoughnessMap:me,dispersion:Q,iridescence:q,iridescenceMap:Ie,iridescenceThicknessMap:Re,sheen:Me,sheenColorMap:ge,sheenRoughnessMap:$e,specularMap:Oe,specularColorMap:ht,specularIntensityMap:N,transmission:oe,transmissionMap:ce,thicknessMap:H,gradientMap:Z,opaque:v.transparent===!1&&v.blending===Ls&&v.alphaToCoverage===!1,alphaMap:ae,alphaTest:ue,alphaHash:Ze,combine:v.combine,mapUv:dt&&m(v.map.channel),aoMapUv:un&&m(v.aoMap.channel),lightMapUv:je&&m(v.lightMap.channel),bumpMapUv:Qe&&m(v.bumpMap.channel),normalMapUv:Ae&&m(v.normalMap.channel),displacementMapUv:mt&&m(v.displacementMap.channel),emissiveMapUv:Ne&&m(v.emissiveMap.channel),metalnessMapUv:b&&m(v.metalnessMap.channel),roughnessMapUv:y&&m(v.roughnessMap.channel),anisotropyMapUv:pe&&m(v.anisotropyMap.channel),clearcoatMapUv:et&&m(v.clearcoatMap.channel),clearcoatNormalMapUv:ne&&m(v.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:me&&m(v.clearcoatRoughnessMap.channel),iridescenceMapUv:Ie&&m(v.iridescenceMap.channel),iridescenceThicknessMapUv:Re&&m(v.iridescenceThicknessMap.channel),sheenColorMapUv:ge&&m(v.sheenColorMap.channel),sheenRoughnessMapUv:$e&&m(v.sheenRoughnessMap.channel),specularMapUv:Oe&&m(v.specularMap.channel),specularColorMapUv:ht&&m(v.specularColorMap.channel),specularIntensityMapUv:N&&m(v.specularIntensityMap.channel),transmissionMapUv:ce&&m(v.transmissionMap.channel),thicknessMapUv:H&&m(v.thicknessMap.channel),alphaMapUv:ae&&m(v.alphaMap.channel),vertexTangents:!!W.attributes.tangent&&(Ae||O),vertexColors:v.vertexColors,vertexAlphas:v.vertexColors===!0&&!!W.attributes.color&&W.attributes.color.itemSize===4,pointsUvs:Y.isPoints===!0&&!!W.attributes.uv&&(dt||ae),fog:!!J,useFog:v.fog===!0,fogExp2:!!J&&J.isFogExp2,flatShading:v.flatShading===!0,sizeAttenuation:v.sizeAttenuation===!0,logarithmicDepthBuffer:d,reverseDepthBuffer:h,skinning:Y.isSkinnedMesh===!0,morphTargets:W.morphAttributes.position!==void 0,morphNormals:W.morphAttributes.normal!==void 0,morphColors:W.morphAttributes.color!==void 0,morphTargetsCount:tt,morphTextureStride:ct,numDirLights:M.directional.length,numPointLights:M.point.length,numSpotLights:M.spot.length,numSpotLightMaps:M.spotLightMap.length,numRectAreaLights:M.rectArea.length,numHemiLights:M.hemi.length,numDirLightShadows:M.directionalShadowMap.length,numPointLightShadows:M.pointShadowMap.length,numSpotLightShadows:M.spotShadowMap.length,numSpotLightShadowsWithMaps:M.numSpotLightShadowsWithMaps,numLightProbes:M.numLightProbes,numClippingPlanes:o.numPlanes,numClipIntersection:o.numIntersection,dithering:v.dithering,shadowMapEnabled:n.shadowMap.enabled&&j.length>0,shadowMapType:n.shadowMap.type,toneMapping:tn,decodeVideoTexture:dt&&v.map.isVideoTexture===!0&&at.getTransfer(v.map.colorSpace)===Mt,premultipliedAlpha:v.premultipliedAlpha,doubleSided:v.side===yi,flipSided:v.side===cn,useDepthPacking:v.depthPacking>=0,depthPacking:v.depthPacking||0,index0AttributeName:v.index0AttributeName,extensionClipCullDistance:Nt&&v.extensions.clipCullDistance===!0&&i.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(Nt&&v.extensions.multiDraw===!0||Xe)&&i.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:i.has("KHR_parallel_shader_compile"),customProgramCacheKey:v.customProgramCacheKey()};return nt.vertexUv1s=c.has(1),nt.vertexUv2s=c.has(2),nt.vertexUv3s=c.has(3),c.clear(),nt}function w(v){let M=[];if(v.shaderID?M.push(v.shaderID):(M.push(v.customVertexShaderID),M.push(v.customFragmentShaderID)),v.defines!==void 0)for(let j in v.defines)M.push(j),M.push(v.defines[j]);return v.isRawShaderMaterial===!1&&(E(M,v),S(M,v),M.push(n.outputColorSpace)),M.push(v.customProgramCacheKey),M.join()}function E(v,M){v.push(M.precision),v.push(M.outputColorSpace),v.push(M.envMapMode),v.push(M.envMapCubeUVHeight),v.push(M.mapUv),v.push(M.alphaMapUv),v.push(M.lightMapUv),v.push(M.aoMapUv),v.push(M.bumpMapUv),v.push(M.normalMapUv),v.push(M.displacementMapUv),v.push(M.emissiveMapUv),v.push(M.metalnessMapUv),v.push(M.roughnessMapUv),v.push(M.anisotropyMapUv),v.push(M.clearcoatMapUv),v.push(M.clearcoatNormalMapUv),v.push(M.clearcoatRoughnessMapUv),v.push(M.iridescenceMapUv),v.push(M.iridescenceThicknessMapUv),v.push(M.sheenColorMapUv),v.push(M.sheenRoughnessMapUv),v.push(M.specularMapUv),v.push(M.specularColorMapUv),v.push(M.specularIntensityMapUv),v.push(M.transmissionMapUv),v.push(M.thicknessMapUv),v.push(M.combine),v.push(M.fogExp2),v.push(M.sizeAttenuation),v.push(M.morphTargetsCount),v.push(M.morphAttributeCount),v.push(M.numDirLights),v.push(M.numPointLights),v.push(M.numSpotLights),v.push(M.numSpotLightMaps),v.push(M.numHemiLights),v.push(M.numRectAreaLights),v.push(M.numDirLightShadows),v.push(M.numPointLightShadows),v.push(M.numSpotLightShadows),v.push(M.numSpotLightShadowsWithMaps),v.push(M.numLightProbes),v.push(M.shadowMapType),v.push(M.toneMapping),v.push(M.numClippingPlanes),v.push(M.numClipIntersection),v.push(M.depthPacking)}function S(v,M){a.disableAll(),M.supportsVertexTextures&&a.enable(0),M.instancing&&a.enable(1),M.instancingColor&&a.enable(2),M.instancingMorph&&a.enable(3),M.matcap&&a.enable(4),M.envMap&&a.enable(5),M.normalMapObjectSpace&&a.enable(6),M.normalMapTangentSpace&&a.enable(7),M.clearcoat&&a.enable(8),M.iridescence&&a.enable(9),M.alphaTest&&a.enable(10),M.vertexColors&&a.enable(11),M.vertexAlphas&&a.enable(12),M.vertexUv1s&&a.enable(13),M.vertexUv2s&&a.enable(14),M.vertexUv3s&&a.enable(15),M.vertexTangents&&a.enable(16),M.anisotropy&&a.enable(17),M.alphaHash&&a.enable(18),M.batching&&a.enable(19),M.dispersion&&a.enable(20),M.batchingColor&&a.enable(21),v.push(a.mask),a.disableAll(),M.fog&&a.enable(0),M.useFog&&a.enable(1),M.flatShading&&a.enable(2),M.logarithmicDepthBuffer&&a.enable(3),M.reverseDepthBuffer&&a.enable(4),M.skinning&&a.enable(5),M.morphTargets&&a.enable(6),M.morphNormals&&a.enable(7),M.morphColors&&a.enable(8),M.premultipliedAlpha&&a.enable(9),M.shadowMapEnabled&&a.enable(10),M.doubleSided&&a.enable(11),M.flipSided&&a.enable(12),M.useDepthPacking&&a.enable(13),M.dithering&&a.enable(14),M.transmission&&a.enable(15),M.sheen&&a.enable(16),M.opaque&&a.enable(17),M.pointsUvs&&a.enable(18),M.decodeVideoTexture&&a.enable(19),M.alphaToCoverage&&a.enable(20),v.push(a.mask)}function k(v){let M=_[v.type],j;if(M){let z=Jn[M];j=UC.clone(z.uniforms)}else j=v.uniforms;return j}function R(v,M){let j;for(let z=0,Y=u.length;z<Y;z++){let J=u[z];if(J.cacheKey===M){j=J,++j.usedTimes;break}}return j===void 0&&(j=new $1(n,M,v,s),u.push(j)),j}function D(v){if(--v.usedTimes===0){let M=u.indexOf(v);u[M]=u[u.length-1],u.pop(),v.destroy()}}function V(v){l.remove(v)}function re(){l.dispose()}return{getParameters:p,getProgramCacheKey:w,getUniforms:k,acquireProgram:R,releaseProgram:D,releaseShaderCache:V,programs:u,dispose:re}}function Y1(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,l){n.get(o)[a]=l}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function Z1(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.z!==e.z?n.z-e.z:n.id-e.id}function yy(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function _y(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(d,h,f,g,_,m){let p=n[e];return p===void 0?(p={id:d.id,object:d,geometry:h,material:f,groupOrder:g,renderOrder:d.renderOrder,z:_,group:m},n[e]=p):(p.id=d.id,p.object=d,p.geometry=h,p.material=f,p.groupOrder=g,p.renderOrder=d.renderOrder,p.z=_,p.group=m),e++,p}function a(d,h,f,g,_,m){let p=o(d,h,f,g,_,m);f.transmission>0?i.push(p):f.transparent===!0?r.push(p):t.push(p)}function l(d,h,f,g,_,m){let p=o(d,h,f,g,_,m);f.transmission>0?i.unshift(p):f.transparent===!0?r.unshift(p):t.unshift(p)}function c(d,h){t.length>1&&t.sort(d||Z1),i.length>1&&i.sort(h||yy),r.length>1&&r.sort(h||yy)}function u(){for(let d=e,h=n.length;d<h;d++){let f=n[d];if(f.id===null)break;f.id=null,f.object=null,f.geometry=null,f.material=null,f.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:a,unshift:l,finish:u,sort:c}}function K1(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new _y,n.set(i,[o])):r>=s.length?(o=new _y,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function J1(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new B,color:new Je};break;case"SpotLight":t={position:new B,direction:new B,color:new Je,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new B,color:new Je,distance:0,decay:0};break;case"HemisphereLight":t={direction:new B,skyColor:new Je,groundColor:new Je};break;case"RectAreaLight":t={color:new Je,position:new B,halfWidth:new B,halfHeight:new B};break}return n[e.id]=t,t}}}function Q1(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new lt};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new lt};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new lt,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var eI=0;function tI(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function nI(n){let e=new J1,t=Q1(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let c=0;c<9;c++)i.probe.push(new B);let r=new B,s=new Rt,o=new Rt;function a(c){let u=0,d=0,h=0;for(let re=0;re<9;re++)i.probe[re].set(0,0,0);let f=0,g=0,_=0,m=0,p=0,w=0,E=0,S=0,k=0,R=0,D=0;c.sort(tI);for(let re=0,v=c.length;re<v;re++){let M=c[re],j=M.color,z=M.intensity,Y=M.distance,J=M.shadow&&M.shadow.map?M.shadow.map.texture:null;if(M.isAmbientLight)u+=j.r*z,d+=j.g*z,h+=j.b*z;else if(M.isLightProbe){for(let W=0;W<9;W++)i.probe[W].addScaledVector(M.sh.coefficients[W],z);D++}else if(M.isDirectionalLight){let W=e.get(M);if(W.color.copy(M.color).multiplyScalar(M.intensity),M.castShadow){let K=M.shadow,G=t.get(M);G.shadowIntensity=K.intensity,G.shadowBias=K.bias,G.shadowNormalBias=K.normalBias,G.shadowRadius=K.radius,G.shadowMapSize=K.mapSize,i.directionalShadow[f]=G,i.directionalShadowMap[f]=J,i.directionalShadowMatrix[f]=M.shadow.matrix,w++}i.directional[f]=W,f++}else if(M.isSpotLight){let W=e.get(M);W.position.setFromMatrixPosition(M.matrixWorld),W.color.copy(j).multiplyScalar(z),W.distance=Y,W.coneCos=Math.cos(M.angle),W.penumbraCos=Math.cos(M.angle*(1-M.penumbra)),W.decay=M.decay,i.spot[_]=W;let K=M.shadow;if(M.map&&(i.spotLightMap[k]=M.map,k++,K.updateMatrices(M),M.castShadow&&R++),i.spotLightMatrix[_]=K.matrix,M.castShadow){let G=t.get(M);G.shadowIntensity=K.intensity,G.shadowBias=K.bias,G.shadowNormalBias=K.normalBias,G.shadowRadius=K.radius,G.shadowMapSize=K.mapSize,i.spotShadow[_]=G,i.spotShadowMap[_]=J,S++}_++}else if(M.isRectAreaLight){let W=e.get(M);W.color.copy(j).multiplyScalar(z),W.halfWidth.set(M.width*.5,0,0),W.halfHeight.set(0,M.height*.5,0),i.rectArea[m]=W,m++}else if(M.isPointLight){let W=e.get(M);if(W.color.copy(M.color).multiplyScalar(M.intensity),W.distance=M.distance,W.decay=M.decay,M.castShadow){let K=M.shadow,G=t.get(M);G.shadowIntensity=K.intensity,G.shadowBias=K.bias,G.shadowNormalBias=K.normalBias,G.shadowRadius=K.radius,G.shadowMapSize=K.mapSize,G.shadowCameraNear=K.camera.near,G.shadowCameraFar=K.camera.far,i.pointShadow[g]=G,i.pointShadowMap[g]=J,i.pointShadowMatrix[g]=M.shadow.matrix,E++}i.point[g]=W,g++}else if(M.isHemisphereLight){let W=e.get(M);W.skyColor.copy(M.color).multiplyScalar(z),W.groundColor.copy(M.groundColor).multiplyScalar(z),i.hemi[p]=W,p++}}m>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=se.LTC_FLOAT_1,i.rectAreaLTC2=se.LTC_FLOAT_2):(i.rectAreaLTC1=se.LTC_HALF_1,i.rectAreaLTC2=se.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=d,i.ambient[2]=h;let V=i.hash;(V.directionalLength!==f||V.pointLength!==g||V.spotLength!==_||V.rectAreaLength!==m||V.hemiLength!==p||V.numDirectionalShadows!==w||V.numPointShadows!==E||V.numSpotShadows!==S||V.numSpotMaps!==k||V.numLightProbes!==D)&&(i.directional.length=f,i.spot.length=_,i.rectArea.length=m,i.point.length=g,i.hemi.length=p,i.directionalShadow.length=w,i.directionalShadowMap.length=w,i.pointShadow.length=E,i.pointShadowMap.length=E,i.spotShadow.length=S,i.spotShadowMap.length=S,i.directionalShadowMatrix.length=w,i.pointShadowMatrix.length=E,i.spotLightMatrix.length=S+k-R,i.spotLightMap.length=k,i.numSpotLightShadowsWithMaps=R,i.numLightProbes=D,V.directionalLength=f,V.pointLength=g,V.spotLength=_,V.rectAreaLength=m,V.hemiLength=p,V.numDirectionalShadows=w,V.numPointShadows=E,V.numSpotShadows=S,V.numSpotMaps=k,V.numLightProbes=D,i.version=eI++)}function l(c,u){let d=0,h=0,f=0,g=0,_=0,m=u.matrixWorldInverse;for(let p=0,w=c.length;p<w;p++){let E=c[p];if(E.isDirectionalLight){let S=i.directional[d];S.direction.setFromMatrixPosition(E.matrixWorld),r.setFromMatrixPosition(E.target.matrixWorld),S.direction.sub(r),S.direction.transformDirection(m),d++}else if(E.isSpotLight){let S=i.spot[f];S.position.setFromMatrixPosition(E.matrixWorld),S.position.applyMatrix4(m),S.direction.setFromMatrixPosition(E.matrixWorld),r.setFromMatrixPosition(E.target.matrixWorld),S.direction.sub(r),S.direction.transformDirection(m),f++}else if(E.isRectAreaLight){let S=i.rectArea[g];S.position.setFromMatrixPosition(E.matrixWorld),S.position.applyMatrix4(m),o.identity(),s.copy(E.matrixWorld),s.premultiply(m),o.extractRotation(s),S.halfWidth.set(E.width*.5,0,0),S.halfHeight.set(0,E.height*.5,0),S.halfWidth.applyMatrix4(o),S.halfHeight.applyMatrix4(o),g++}else if(E.isPointLight){let S=i.point[h];S.position.setFromMatrixPosition(E.matrixWorld),S.position.applyMatrix4(m),h++}else if(E.isHemisphereLight){let S=i.hemi[_];S.direction.setFromMatrixPosition(E.matrixWorld),S.direction.transformDirection(m),_++}}}return{setup:a,setupView:l,state:i}}function xy(n){let e=new nI(n),t=[],i=[];function r(u){c.camera=u,t.length=0,i.length=0}function s(u){t.push(u)}function o(u){i.push(u)}function a(){e.setup(t)}function l(u){e.setupView(t,u)}let c={lightsArray:t,shadowsArray:i,camera:null,lights:e,transmissionRenderTarget:{}};return{init:r,state:c,setupLights:a,setupLightsView:l,pushLight:s,pushShadow:o}}function iI(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new xy(n),e.set(r,[a])):s>=o.length?(a=new xy(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var Ff=class extends Pr{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=cC,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},Of=class extends Pr{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}},rI=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,sI=`uniform sampler2D shadow_pass;
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
}`;function oI(n,e,t){let i=new Oo,r=new lt,s=new lt,o=new Dt,a=new Ff({depthPacking:uC}),l=new Of,c={},u=t.maxTextureSize,d={[Yi]:cn,[cn]:Yi,[yi]:yi},h=new Qn({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new lt},radius:{value:4}},vertexShader:rI,fragmentShader:sI}),f=h.clone();f.defines.HORIZONTAL_PASS=1;let g=new Fr;g.setAttribute("position",new An(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let _=new ln(g,h),m=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=Sy;let p=this.type;this.render=function(R,D,V){if(m.enabled===!1||m.autoUpdate===!1&&m.needsUpdate===!1||R.length===0)return;let re=n.getRenderTarget(),v=n.getActiveCubeFace(),M=n.getActiveMipmapLevel(),j=n.state;j.setBlending(qi),j.buffers.color.setClear(1,1,1,1),j.buffers.depth.setTest(!0),j.setScissorTest(!1);let z=p!==gi&&this.type===gi,Y=p===gi&&this.type!==gi;for(let J=0,W=R.length;J<W;J++){let K=R[J],G=K.shadow;if(G===void 0){console.warn("THREE.WebGLShadowMap:",K,"has no shadow.");continue}if(G.autoUpdate===!1&&G.needsUpdate===!1)continue;r.copy(G.mapSize);let de=G.getFrameExtents();if(r.multiply(de),s.copy(G.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/de.x),r.x=s.x*de.x,G.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/de.y),r.y=s.y*de.y,G.mapSize.y=s.y)),G.map===null||z===!0||Y===!0){let xe=this.type!==gi?{minFilter:Tn,magFilter:Tn}:{};G.map!==null&&G.map.dispose(),G.map=new Si(r.x,r.y,xe),G.map.texture.name=K.name+".shadowMap",G.camera.updateProjectionMatrix()}n.setRenderTarget(G.map),n.clear();let he=G.getViewportCount();for(let xe=0;xe<he;xe++){let tt=G.getViewport(xe);o.set(s.x*tt.x,s.y*tt.y,s.x*tt.z,s.y*tt.w),j.viewport(o),G.updateMatrices(K,xe),i=G.getFrustum(),S(D,V,G.camera,K,this.type)}G.isPointLightShadow!==!0&&this.type===gi&&w(G,V),G.needsUpdate=!1}p=this.type,m.needsUpdate=!1,n.setRenderTarget(re,v,M)};function w(R,D){let V=e.update(_);h.defines.VSM_SAMPLES!==R.blurSamples&&(h.defines.VSM_SAMPLES=R.blurSamples,f.defines.VSM_SAMPLES=R.blurSamples,h.needsUpdate=!0,f.needsUpdate=!0),R.mapPass===null&&(R.mapPass=new Si(r.x,r.y)),h.uniforms.shadow_pass.value=R.map.texture,h.uniforms.resolution.value=R.mapSize,h.uniforms.radius.value=R.radius,n.setRenderTarget(R.mapPass),n.clear(),n.renderBufferDirect(D,null,V,h,_,null),f.uniforms.shadow_pass.value=R.mapPass.texture,f.uniforms.resolution.value=R.mapSize,f.uniforms.radius.value=R.radius,n.setRenderTarget(R.map),n.clear(),n.renderBufferDirect(D,null,V,f,_,null)}function E(R,D,V,re){let v=null,M=V.isPointLight===!0?R.customDistanceMaterial:R.customDepthMaterial;if(M!==void 0)v=M;else if(v=V.isPointLight===!0?l:a,n.localClippingEnabled&&D.clipShadows===!0&&Array.isArray(D.clippingPlanes)&&D.clippingPlanes.length!==0||D.displacementMap&&D.displacementScale!==0||D.alphaMap&&D.alphaTest>0||D.map&&D.alphaTest>0){let j=v.uuid,z=D.uuid,Y=c[j];Y===void 0&&(Y={},c[j]=Y);let J=Y[z];J===void 0&&(J=v.clone(),Y[z]=J,D.addEventListener("dispose",k)),v=J}if(v.visible=D.visible,v.wireframe=D.wireframe,re===gi?v.side=D.shadowSide!==null?D.shadowSide:D.side:v.side=D.shadowSide!==null?D.shadowSide:d[D.side],v.alphaMap=D.alphaMap,v.alphaTest=D.alphaTest,v.map=D.map,v.clipShadows=D.clipShadows,v.clippingPlanes=D.clippingPlanes,v.clipIntersection=D.clipIntersection,v.displacementMap=D.displacementMap,v.displacementScale=D.displacementScale,v.displacementBias=D.displacementBias,v.wireframeLinewidth=D.wireframeLinewidth,v.linewidth=D.linewidth,V.isPointLight===!0&&v.isMeshDistanceMaterial===!0){let j=n.properties.get(v);j.light=V}return v}function S(R,D,V,re,v){if(R.visible===!1)return;if(R.layers.test(D.layers)&&(R.isMesh||R.isLine||R.isPoints)&&(R.castShadow||R.receiveShadow&&v===gi)&&(!R.frustumCulled||i.intersectsObject(R))){R.modelViewMatrix.multiplyMatrices(V.matrixWorldInverse,R.matrixWorld);let z=e.update(R),Y=R.material;if(Array.isArray(Y)){let J=z.groups;for(let W=0,K=J.length;W<K;W++){let G=J[W],de=Y[G.materialIndex];if(de&&de.visible){let he=E(R,de,re,v);R.onBeforeShadow(n,R,D,V,z,he,G),n.renderBufferDirect(V,null,z,he,R,G),R.onAfterShadow(n,R,D,V,z,he,G)}}}else if(Y.visible){let J=E(R,Y,re,v);R.onBeforeShadow(n,R,D,V,z,J,null),n.renderBufferDirect(V,null,z,J,R,null),R.onAfterShadow(n,R,D,V,z,J,null)}}let j=R.children;for(let z=0,Y=j.length;z<Y;z++)S(j[z],D,V,re,v)}function k(R){R.target.removeEventListener("dispose",k);for(let V in c){let re=c[V],v=R.target.uuid;v in re&&(re[v].dispose(),delete re[v])}}}var aI={[Lh]:kh,[Uh]:Hh,[Vh]:zh,[Bs]:Bh,[kh]:Lh,[Hh]:Uh,[zh]:Vh,[Bh]:Bs};function lI(n){function e(){let N=!1,ce=new Dt,H=null,Z=new Dt(0,0,0,0);return{setMask:function(ae){H!==ae&&!N&&(n.colorMask(ae,ae,ae,ae),H=ae)},setLocked:function(ae){N=ae},setClear:function(ae,ue,Ze,Nt,tn){tn===!0&&(ae*=Nt,ue*=Nt,Ze*=Nt),ce.set(ae,ue,Ze,Nt),Z.equals(ce)===!1&&(n.clearColor(ae,ue,Ze,Nt),Z.copy(ce))},reset:function(){N=!1,H=null,Z.set(-1,0,0,0)}}}function t(){let N=!1,ce=!1,H=null,Z=null,ae=null;return{setReversed:function(ue){ce=ue},setTest:function(ue){ue?ye(n.DEPTH_TEST):fe(n.DEPTH_TEST)},setMask:function(ue){H!==ue&&!N&&(n.depthMask(ue),H=ue)},setFunc:function(ue){if(ce&&(ue=aI[ue]),Z!==ue){switch(ue){case Lh:n.depthFunc(n.NEVER);break;case kh:n.depthFunc(n.ALWAYS);break;case Uh:n.depthFunc(n.LESS);break;case Bs:n.depthFunc(n.LEQUAL);break;case Vh:n.depthFunc(n.EQUAL);break;case Bh:n.depthFunc(n.GEQUAL);break;case Hh:n.depthFunc(n.GREATER);break;case zh:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}Z=ue}},setLocked:function(ue){N=ue},setClear:function(ue){ae!==ue&&(n.clearDepth(ue),ae=ue)},reset:function(){N=!1,H=null,Z=null,ae=null}}}function i(){let N=!1,ce=null,H=null,Z=null,ae=null,ue=null,Ze=null,Nt=null,tn=null;return{setTest:function(nt){N||(nt?ye(n.STENCIL_TEST):fe(n.STENCIL_TEST))},setMask:function(nt){ce!==nt&&!N&&(n.stencilMask(nt),ce=nt)},setFunc:function(nt,nn,ei){(H!==nt||Z!==nn||ae!==ei)&&(n.stencilFunc(nt,nn,ei),H=nt,Z=nn,ae=ei)},setOp:function(nt,nn,ei){(ue!==nt||Ze!==nn||Nt!==ei)&&(n.stencilOp(nt,nn,ei),ue=nt,Ze=nn,Nt=ei)},setLocked:function(nt){N=nt},setClear:function(nt){tn!==nt&&(n.clearStencil(nt),tn=nt)},reset:function(){N=!1,ce=null,H=null,Z=null,ae=null,ue=null,Ze=null,Nt=null,tn=null}}}let r=new e,s=new t,o=new i,a=new WeakMap,l=new WeakMap,c={},u={},d=new WeakMap,h=[],f=null,g=!1,_=null,m=null,p=null,w=null,E=null,S=null,k=null,R=new Je(0,0,0),D=0,V=!1,re=null,v=null,M=null,j=null,z=null,Y=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),J=!1,W=0,K=n.getParameter(n.VERSION);K.indexOf("WebGL")!==-1?(W=parseFloat(/^WebGL (\d)/.exec(K)[1]),J=W>=1):K.indexOf("OpenGL ES")!==-1&&(W=parseFloat(/^OpenGL ES (\d)/.exec(K)[1]),J=W>=2);let G=null,de={},he=n.getParameter(n.SCISSOR_BOX),xe=n.getParameter(n.VIEWPORT),tt=new Dt().fromArray(he),ct=new Dt().fromArray(xe);function $(N,ce,H,Z){let ae=new Uint8Array(4),ue=n.createTexture();n.bindTexture(N,ue),n.texParameteri(N,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(N,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let Ze=0;Ze<H;Ze++)N===n.TEXTURE_3D||N===n.TEXTURE_2D_ARRAY?n.texImage3D(ce,0,n.RGBA,1,1,Z,0,n.RGBA,n.UNSIGNED_BYTE,ae):n.texImage2D(ce+Ze,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,ae);return ue}let te={};te[n.TEXTURE_2D]=$(n.TEXTURE_2D,n.TEXTURE_2D,1),te[n.TEXTURE_CUBE_MAP]=$(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),te[n.TEXTURE_2D_ARRAY]=$(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),te[n.TEXTURE_3D]=$(n.TEXTURE_3D,n.TEXTURE_3D,1,1),r.setClear(0,0,0,1),s.setClear(1),o.setClear(0),ye(n.DEPTH_TEST),s.setFunc(Bs),je(!1),Qe(w0),ye(n.CULL_FACE),A(qi);function ye(N){c[N]!==!0&&(n.enable(N),c[N]=!0)}function fe(N){c[N]!==!1&&(n.disable(N),c[N]=!1)}function Fe(N,ce){return u[N]!==ce?(n.bindFramebuffer(N,ce),u[N]=ce,N===n.DRAW_FRAMEBUFFER&&(u[n.FRAMEBUFFER]=ce),N===n.FRAMEBUFFER&&(u[n.DRAW_FRAMEBUFFER]=ce),!0):!1}function Ce(N,ce){let H=h,Z=!1;if(N){H=d.get(ce),H===void 0&&(H=[],d.set(ce,H));let ae=N.textures;if(H.length!==ae.length||H[0]!==n.COLOR_ATTACHMENT0){for(let ue=0,Ze=ae.length;ue<Ze;ue++)H[ue]=n.COLOR_ATTACHMENT0+ue;H.length=ae.length,Z=!0}}else H[0]!==n.BACK&&(H[0]=n.BACK,Z=!0);Z&&n.drawBuffers(H)}function Xe(N){return f!==N?(n.useProgram(N),f=N,!0):!1}let dt={[wr]:n.FUNC_ADD,[Lw]:n.FUNC_SUBTRACT,[kw]:n.FUNC_REVERSE_SUBTRACT};dt[Uw]=n.MIN,dt[Vw]=n.MAX;let Ye={[Bw]:n.ZERO,[Hw]:n.ONE,[zw]:n.SRC_COLOR,[Fh]:n.SRC_ALPHA,[Xw]:n.SRC_ALPHA_SATURATE,[$w]:n.DST_COLOR,[Ww]:n.DST_ALPHA,[Gw]:n.ONE_MINUS_SRC_COLOR,[Oh]:n.ONE_MINUS_SRC_ALPHA,[qw]:n.ONE_MINUS_DST_COLOR,[jw]:n.ONE_MINUS_DST_ALPHA,[Yw]:n.CONSTANT_COLOR,[Zw]:n.ONE_MINUS_CONSTANT_COLOR,[Kw]:n.CONSTANT_ALPHA,[Jw]:n.ONE_MINUS_CONSTANT_ALPHA};function A(N,ce,H,Z,ae,ue,Ze,Nt,tn,nt){if(N===qi){g===!0&&(fe(n.BLEND),g=!1);return}if(g===!1&&(ye(n.BLEND),g=!0),N!==Ow){if(N!==_||nt!==V){if((m!==wr||E!==wr)&&(n.blendEquation(n.FUNC_ADD),m=wr,E=wr),nt)switch(N){case Ls:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case C0:n.blendFunc(n.ONE,n.ONE);break;case D0:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case T0:n.blendFuncSeparate(n.ZERO,n.SRC_COLOR,n.ZERO,n.SRC_ALPHA);break;default:console.error("THREE.WebGLState: Invalid blending: ",N);break}else switch(N){case Ls:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case C0:n.blendFunc(n.SRC_ALPHA,n.ONE);break;case D0:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case T0:n.blendFunc(n.ZERO,n.SRC_COLOR);break;default:console.error("THREE.WebGLState: Invalid blending: ",N);break}p=null,w=null,S=null,k=null,R.set(0,0,0),D=0,_=N,V=nt}return}ae=ae||ce,ue=ue||H,Ze=Ze||Z,(ce!==m||ae!==E)&&(n.blendEquationSeparate(dt[ce],dt[ae]),m=ce,E=ae),(H!==p||Z!==w||ue!==S||Ze!==k)&&(n.blendFuncSeparate(Ye[H],Ye[Z],Ye[ue],Ye[Ze]),p=H,w=Z,S=ue,k=Ze),(Nt.equals(R)===!1||tn!==D)&&(n.blendColor(Nt.r,Nt.g,Nt.b,tn),R.copy(Nt),D=tn),_=N,V=!1}function un(N,ce){N.side===yi?fe(n.CULL_FACE):ye(n.CULL_FACE);let H=N.side===cn;ce&&(H=!H),je(H),N.blending===Ls&&N.transparent===!1?A(qi):A(N.blending,N.blendEquation,N.blendSrc,N.blendDst,N.blendEquationAlpha,N.blendSrcAlpha,N.blendDstAlpha,N.blendColor,N.blendAlpha,N.premultipliedAlpha),s.setFunc(N.depthFunc),s.setTest(N.depthTest),s.setMask(N.depthWrite),r.setMask(N.colorWrite);let Z=N.stencilWrite;o.setTest(Z),Z&&(o.setMask(N.stencilWriteMask),o.setFunc(N.stencilFunc,N.stencilRef,N.stencilFuncMask),o.setOp(N.stencilFail,N.stencilZFail,N.stencilZPass)),mt(N.polygonOffset,N.polygonOffsetFactor,N.polygonOffsetUnits),N.alphaToCoverage===!0?ye(n.SAMPLE_ALPHA_TO_COVERAGE):fe(n.SAMPLE_ALPHA_TO_COVERAGE)}function je(N){re!==N&&(N?n.frontFace(n.CW):n.frontFace(n.CCW),re=N)}function Qe(N){N!==Nw?(ye(n.CULL_FACE),N!==v&&(N===w0?n.cullFace(n.BACK):N===Pw?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):fe(n.CULL_FACE),v=N}function Ae(N){N!==M&&(J&&n.lineWidth(N),M=N)}function mt(N,ce,H){N?(ye(n.POLYGON_OFFSET_FILL),(j!==ce||z!==H)&&(n.polygonOffset(ce,H),j=ce,z=H)):fe(n.POLYGON_OFFSET_FILL)}function Ne(N){N?ye(n.SCISSOR_TEST):fe(n.SCISSOR_TEST)}function b(N){N===void 0&&(N=n.TEXTURE0+Y-1),G!==N&&(n.activeTexture(N),G=N)}function y(N,ce,H){H===void 0&&(G===null?H=n.TEXTURE0+Y-1:H=G);let Z=de[H];Z===void 0&&(Z={type:void 0,texture:void 0},de[H]=Z),(Z.type!==N||Z.texture!==ce)&&(G!==H&&(n.activeTexture(H),G=H),n.bindTexture(N,ce||te[N]),Z.type=N,Z.texture=ce)}function O(){let N=de[G];N!==void 0&&N.type!==void 0&&(n.bindTexture(N.type,null),N.type=void 0,N.texture=void 0)}function X(){try{n.compressedTexImage2D.apply(n,arguments)}catch(N){console.error("THREE.WebGLState:",N)}}function Q(){try{n.compressedTexImage3D.apply(n,arguments)}catch(N){console.error("THREE.WebGLState:",N)}}function q(){try{n.texSubImage2D.apply(n,arguments)}catch(N){console.error("THREE.WebGLState:",N)}}function Me(){try{n.texSubImage3D.apply(n,arguments)}catch(N){console.error("THREE.WebGLState:",N)}}function oe(){try{n.compressedTexSubImage2D.apply(n,arguments)}catch(N){console.error("THREE.WebGLState:",N)}}function pe(){try{n.compressedTexSubImage3D.apply(n,arguments)}catch(N){console.error("THREE.WebGLState:",N)}}function et(){try{n.texStorage2D.apply(n,arguments)}catch(N){console.error("THREE.WebGLState:",N)}}function ne(){try{n.texStorage3D.apply(n,arguments)}catch(N){console.error("THREE.WebGLState:",N)}}function me(){try{n.texImage2D.apply(n,arguments)}catch(N){console.error("THREE.WebGLState:",N)}}function Ie(){try{n.texImage3D.apply(n,arguments)}catch(N){console.error("THREE.WebGLState:",N)}}function Re(N){tt.equals(N)===!1&&(n.scissor(N.x,N.y,N.z,N.w),tt.copy(N))}function ge(N){ct.equals(N)===!1&&(n.viewport(N.x,N.y,N.z,N.w),ct.copy(N))}function $e(N,ce){let H=l.get(ce);H===void 0&&(H=new WeakMap,l.set(ce,H));let Z=H.get(N);Z===void 0&&(Z=n.getUniformBlockIndex(ce,N.name),H.set(N,Z))}function Oe(N,ce){let Z=l.get(ce).get(N);a.get(ce)!==Z&&(n.uniformBlockBinding(ce,Z,N.__bindingPointIndex),a.set(ce,Z))}function ht(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),c={},G=null,de={},u={},d=new WeakMap,h=[],f=null,g=!1,_=null,m=null,p=null,w=null,E=null,S=null,k=null,R=new Je(0,0,0),D=0,V=!1,re=null,v=null,M=null,j=null,z=null,tt.set(0,0,n.canvas.width,n.canvas.height),ct.set(0,0,n.canvas.width,n.canvas.height),r.reset(),s.reset(),o.reset()}return{buffers:{color:r,depth:s,stencil:o},enable:ye,disable:fe,bindFramebuffer:Fe,drawBuffers:Ce,useProgram:Xe,setBlending:A,setMaterial:un,setFlipSided:je,setCullFace:Qe,setLineWidth:Ae,setPolygonOffset:mt,setScissorTest:Ne,activeTexture:b,bindTexture:y,unbindTexture:O,compressedTexImage2D:X,compressedTexImage3D:Q,texImage2D:me,texImage3D:Ie,updateUBOMapping:$e,uniformBlockBinding:Oe,texStorage2D:et,texStorage3D:ne,texSubImage2D:q,texSubImage3D:Me,compressedTexSubImage2D:oe,compressedTexSubImage3D:pe,scissor:Re,viewport:ge,reset:ht}}function My(n,e,t,i){let r=cI(i);switch(t){case Ay:return n*e;case Ry:return n*e;case Ny:return n*e*2;case Py:return n*e/r.components*r.byteLength;case Qf:return n*e/r.components*r.byteLength;case Fy:return n*e*2/r.components*r.byteLength;case ep:return n*e*2/r.components*r.byteLength;case Iy:return n*e*3/r.components*r.byteLength;case Bn:return n*e*4/r.components*r.byteLength;case tp:return n*e*4/r.components*r.byteLength;case Wl:case jl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case $l:case ql:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Xh:case Zh:return Math.max(n,16)*Math.max(e,8)/4;case qh:case Yh:return Math.max(n,8)*Math.max(e,8)/2;case Kh:case Jh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Qh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case ef:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case tf:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case nf:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case rf:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case sf:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case of:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case af:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case lf:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case cf:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case uf:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case df:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case hf:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case ff:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case pf:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case Xl:case mf:case gf:return Math.ceil(n/4)*Math.ceil(e/4)*16;case Oy:case vf:return Math.ceil(n/4)*Math.ceil(e/4)*8;case yf:case _f:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function cI(n){switch(n){case bi:case Cy:return{byteLength:1,components:1};case Po:case Dy:case ko:return{byteLength:2,components:1};case Kf:case Jf:return{byteLength:2,components:4};case Ir:case Zf:case _i:return{byteLength:4,components:1};case Ty:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}function uI(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,l=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),c=new lt,u=new WeakMap,d,h=new WeakMap,f=!1;try{f=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function g(b,y){return f?new OffscreenCanvas(b,y):tc("canvas")}function _(b,y,O){let X=1,Q=Ne(b);if((Q.width>O||Q.height>O)&&(X=O/Math.max(Q.width,Q.height)),X<1)if(typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&b instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&b instanceof ImageBitmap||typeof VideoFrame<"u"&&b instanceof VideoFrame){let q=Math.floor(X*Q.width),Me=Math.floor(X*Q.height);d===void 0&&(d=g(q,Me));let oe=y?g(q,Me):d;return oe.width=q,oe.height=Me,oe.getContext("2d").drawImage(b,0,0,q,Me),console.warn("THREE.WebGLRenderer: Texture has been resized from ("+Q.width+"x"+Q.height+") to ("+q+"x"+Me+")."),oe}else return"data"in b&&console.warn("THREE.WebGLRenderer: Image in DataTexture is too big ("+Q.width+"x"+Q.height+")."),b;return b}function m(b){return b.generateMipmaps&&b.minFilter!==Tn&&b.minFilter!==Vn}function p(b){n.generateMipmap(b)}function w(b,y,O,X,Q=!1){if(b!==null){if(n[b]!==void 0)return n[b];console.warn("THREE.WebGLRenderer: Attempt to use non-existing WebGL internal format '"+b+"'")}let q=y;if(y===n.RED&&(O===n.FLOAT&&(q=n.R32F),O===n.HALF_FLOAT&&(q=n.R16F),O===n.UNSIGNED_BYTE&&(q=n.R8)),y===n.RED_INTEGER&&(O===n.UNSIGNED_BYTE&&(q=n.R8UI),O===n.UNSIGNED_SHORT&&(q=n.R16UI),O===n.UNSIGNED_INT&&(q=n.R32UI),O===n.BYTE&&(q=n.R8I),O===n.SHORT&&(q=n.R16I),O===n.INT&&(q=n.R32I)),y===n.RG&&(O===n.FLOAT&&(q=n.RG32F),O===n.HALF_FLOAT&&(q=n.RG16F),O===n.UNSIGNED_BYTE&&(q=n.RG8)),y===n.RG_INTEGER&&(O===n.UNSIGNED_BYTE&&(q=n.RG8UI),O===n.UNSIGNED_SHORT&&(q=n.RG16UI),O===n.UNSIGNED_INT&&(q=n.RG32UI),O===n.BYTE&&(q=n.RG8I),O===n.SHORT&&(q=n.RG16I),O===n.INT&&(q=n.RG32I)),y===n.RGB_INTEGER&&(O===n.UNSIGNED_BYTE&&(q=n.RGB8UI),O===n.UNSIGNED_SHORT&&(q=n.RGB16UI),O===n.UNSIGNED_INT&&(q=n.RGB32UI),O===n.BYTE&&(q=n.RGB8I),O===n.SHORT&&(q=n.RGB16I),O===n.INT&&(q=n.RGB32I)),y===n.RGBA_INTEGER&&(O===n.UNSIGNED_BYTE&&(q=n.RGBA8UI),O===n.UNSIGNED_SHORT&&(q=n.RGBA16UI),O===n.UNSIGNED_INT&&(q=n.RGBA32UI),O===n.BYTE&&(q=n.RGBA8I),O===n.SHORT&&(q=n.RGBA16I),O===n.INT&&(q=n.RGBA32I)),y===n.RGB&&O===n.UNSIGNED_INT_5_9_9_9_REV&&(q=n.RGB9_E5),y===n.RGBA){let Me=Q?Kl:at.getTransfer(X);O===n.FLOAT&&(q=n.RGBA32F),O===n.HALF_FLOAT&&(q=n.RGBA16F),O===n.UNSIGNED_BYTE&&(q=Me===Mt?n.SRGB8_ALPHA8:n.RGBA8),O===n.UNSIGNED_SHORT_4_4_4_4&&(q=n.RGBA4),O===n.UNSIGNED_SHORT_5_5_5_1&&(q=n.RGB5_A1)}return(q===n.R16F||q===n.R32F||q===n.RG16F||q===n.RG32F||q===n.RGBA16F||q===n.RGBA32F)&&e.get("EXT_color_buffer_float"),q}function E(b,y){let O;return b?y===null||y===Ir||y===Gs?O=n.DEPTH24_STENCIL8:y===_i?O=n.DEPTH32F_STENCIL8:y===Po&&(O=n.DEPTH24_STENCIL8,console.warn("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):y===null||y===Ir||y===Gs?O=n.DEPTH_COMPONENT24:y===_i?O=n.DEPTH_COMPONENT32F:y===Po&&(O=n.DEPTH_COMPONENT16),O}function S(b,y){return m(b)===!0||b.isFramebufferTexture&&b.minFilter!==Tn&&b.minFilter!==Vn?Math.log2(Math.max(y.width,y.height))+1:b.mipmaps!==void 0&&b.mipmaps.length>0?b.mipmaps.length:b.isCompressedTexture&&Array.isArray(b.image)?y.mipmaps.length:1}function k(b){let y=b.target;y.removeEventListener("dispose",k),D(y),y.isVideoTexture&&u.delete(y)}function R(b){let y=b.target;y.removeEventListener("dispose",R),re(y)}function D(b){let y=i.get(b);if(y.__webglInit===void 0)return;let O=b.source,X=h.get(O);if(X){let Q=X[y.__cacheKey];Q.usedTimes--,Q.usedTimes===0&&V(b),Object.keys(X).length===0&&h.delete(O)}i.remove(b)}function V(b){let y=i.get(b);n.deleteTexture(y.__webglTexture);let O=b.source,X=h.get(O);delete X[y.__cacheKey],o.memory.textures--}function re(b){let y=i.get(b);if(b.depthTexture&&b.depthTexture.dispose(),b.isWebGLCubeRenderTarget)for(let X=0;X<6;X++){if(Array.isArray(y.__webglFramebuffer[X]))for(let Q=0;Q<y.__webglFramebuffer[X].length;Q++)n.deleteFramebuffer(y.__webglFramebuffer[X][Q]);else n.deleteFramebuffer(y.__webglFramebuffer[X]);y.__webglDepthbuffer&&n.deleteRenderbuffer(y.__webglDepthbuffer[X])}else{if(Array.isArray(y.__webglFramebuffer))for(let X=0;X<y.__webglFramebuffer.length;X++)n.deleteFramebuffer(y.__webglFramebuffer[X]);else n.deleteFramebuffer(y.__webglFramebuffer);if(y.__webglDepthbuffer&&n.deleteRenderbuffer(y.__webglDepthbuffer),y.__webglMultisampledFramebuffer&&n.deleteFramebuffer(y.__webglMultisampledFramebuffer),y.__webglColorRenderbuffer)for(let X=0;X<y.__webglColorRenderbuffer.length;X++)y.__webglColorRenderbuffer[X]&&n.deleteRenderbuffer(y.__webglColorRenderbuffer[X]);y.__webglDepthRenderbuffer&&n.deleteRenderbuffer(y.__webglDepthRenderbuffer)}let O=b.textures;for(let X=0,Q=O.length;X<Q;X++){let q=i.get(O[X]);q.__webglTexture&&(n.deleteTexture(q.__webglTexture),o.memory.textures--),i.remove(O[X])}i.remove(b)}let v=0;function M(){v=0}function j(){let b=v;return b>=r.maxTextures&&console.warn("THREE.WebGLTextures: Trying to use "+b+" texture units while this GPU supports only "+r.maxTextures),v+=1,b}function z(b){let y=[];return y.push(b.wrapS),y.push(b.wrapT),y.push(b.wrapR||0),y.push(b.magFilter),y.push(b.minFilter),y.push(b.anisotropy),y.push(b.internalFormat),y.push(b.format),y.push(b.type),y.push(b.generateMipmaps),y.push(b.premultiplyAlpha),y.push(b.flipY),y.push(b.unpackAlignment),y.push(b.colorSpace),y.join()}function Y(b,y){let O=i.get(b);if(b.isVideoTexture&&Ae(b),b.isRenderTargetTexture===!1&&b.version>0&&O.__version!==b.version){let X=b.image;if(X===null)console.warn("THREE.WebGLRenderer: Texture marked for update but no image data found.");else if(X.complete===!1)console.warn("THREE.WebGLRenderer: Texture marked for update but image is incomplete");else{ct(O,b,y);return}}t.bindTexture(n.TEXTURE_2D,O.__webglTexture,n.TEXTURE0+y)}function J(b,y){let O=i.get(b);if(b.version>0&&O.__version!==b.version){ct(O,b,y);return}t.bindTexture(n.TEXTURE_2D_ARRAY,O.__webglTexture,n.TEXTURE0+y)}function W(b,y){let O=i.get(b);if(b.version>0&&O.__version!==b.version){ct(O,b,y);return}t.bindTexture(n.TEXTURE_3D,O.__webglTexture,n.TEXTURE0+y)}function K(b,y){let O=i.get(b);if(b.version>0&&O.__version!==b.version){$(O,b,y);return}t.bindTexture(n.TEXTURE_CUBE_MAP,O.__webglTexture,n.TEXTURE0+y)}let G={[jh]:n.REPEAT,[Tr]:n.CLAMP_TO_EDGE,[$h]:n.MIRRORED_REPEAT},de={[Tn]:n.NEAREST,[lC]:n.NEAREST_MIPMAP_NEAREST,[El]:n.NEAREST_MIPMAP_LINEAR,[Vn]:n.LINEAR,[nh]:n.LINEAR_MIPMAP_NEAREST,[Ar]:n.LINEAR_MIPMAP_LINEAR},he={[hC]:n.NEVER,[yC]:n.ALWAYS,[fC]:n.LESS,[ky]:n.LEQUAL,[pC]:n.EQUAL,[vC]:n.GEQUAL,[mC]:n.GREATER,[gC]:n.NOTEQUAL};function xe(b,y){if(y.type===_i&&e.has("OES_texture_float_linear")===!1&&(y.magFilter===Vn||y.magFilter===nh||y.magFilter===El||y.magFilter===Ar||y.minFilter===Vn||y.minFilter===nh||y.minFilter===El||y.minFilter===Ar)&&console.warn("THREE.WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(b,n.TEXTURE_WRAP_S,G[y.wrapS]),n.texParameteri(b,n.TEXTURE_WRAP_T,G[y.wrapT]),(b===n.TEXTURE_3D||b===n.TEXTURE_2D_ARRAY)&&n.texParameteri(b,n.TEXTURE_WRAP_R,G[y.wrapR]),n.texParameteri(b,n.TEXTURE_MAG_FILTER,de[y.magFilter]),n.texParameteri(b,n.TEXTURE_MIN_FILTER,de[y.minFilter]),y.compareFunction&&(n.texParameteri(b,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(b,n.TEXTURE_COMPARE_FUNC,he[y.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(y.magFilter===Tn||y.minFilter!==El&&y.minFilter!==Ar||y.type===_i&&e.has("OES_texture_float_linear")===!1)return;if(y.anisotropy>1||i.get(y).__currentAnisotropy){let O=e.get("EXT_texture_filter_anisotropic");n.texParameterf(b,O.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(y.anisotropy,r.getMaxAnisotropy())),i.get(y).__currentAnisotropy=y.anisotropy}}}function tt(b,y){let O=!1;b.__webglInit===void 0&&(b.__webglInit=!0,y.addEventListener("dispose",k));let X=y.source,Q=h.get(X);Q===void 0&&(Q={},h.set(X,Q));let q=z(y);if(q!==b.__cacheKey){Q[q]===void 0&&(Q[q]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,O=!0),Q[q].usedTimes++;let Me=Q[b.__cacheKey];Me!==void 0&&(Q[b.__cacheKey].usedTimes--,Me.usedTimes===0&&V(y)),b.__cacheKey=q,b.__webglTexture=Q[q].texture}return O}function ct(b,y,O){let X=n.TEXTURE_2D;(y.isDataArrayTexture||y.isCompressedArrayTexture)&&(X=n.TEXTURE_2D_ARRAY),y.isData3DTexture&&(X=n.TEXTURE_3D);let Q=tt(b,y),q=y.source;t.bindTexture(X,b.__webglTexture,n.TEXTURE0+O);let Me=i.get(q);if(q.version!==Me.__version||Q===!0){t.activeTexture(n.TEXTURE0+O);let oe=at.getPrimaries(at.workingColorSpace),pe=y.colorSpace===$i?null:at.getPrimaries(y.colorSpace),et=y.colorSpace===$i||oe===pe?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,y.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,y.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,y.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,et);let ne=_(y.image,!1,r.maxTextureSize);ne=mt(y,ne);let me=s.convert(y.format,y.colorSpace),Ie=s.convert(y.type),Re=w(y.internalFormat,me,Ie,y.colorSpace,y.isVideoTexture);xe(X,y);let ge,$e=y.mipmaps,Oe=y.isVideoTexture!==!0,ht=Me.__version===void 0||Q===!0,N=q.dataReady,ce=S(y,ne);if(y.isDepthTexture)Re=E(y.format===Ws,y.type),ht&&(Oe?t.texStorage2D(n.TEXTURE_2D,1,Re,ne.width,ne.height):t.texImage2D(n.TEXTURE_2D,0,Re,ne.width,ne.height,0,me,Ie,null));else if(y.isDataTexture)if($e.length>0){Oe&&ht&&t.texStorage2D(n.TEXTURE_2D,ce,Re,$e[0].width,$e[0].height);for(let H=0,Z=$e.length;H<Z;H++)ge=$e[H],Oe?N&&t.texSubImage2D(n.TEXTURE_2D,H,0,0,ge.width,ge.height,me,Ie,ge.data):t.texImage2D(n.TEXTURE_2D,H,Re,ge.width,ge.height,0,me,Ie,ge.data);y.generateMipmaps=!1}else Oe?(ht&&t.texStorage2D(n.TEXTURE_2D,ce,Re,ne.width,ne.height),N&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,ne.width,ne.height,me,Ie,ne.data)):t.texImage2D(n.TEXTURE_2D,0,Re,ne.width,ne.height,0,me,Ie,ne.data);else if(y.isCompressedTexture)if(y.isCompressedArrayTexture){Oe&&ht&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ce,Re,$e[0].width,$e[0].height,ne.depth);for(let H=0,Z=$e.length;H<Z;H++)if(ge=$e[H],y.format!==Bn)if(me!==null)if(Oe){if(N)if(y.layerUpdates.size>0){let ae=My(ge.width,ge.height,y.format,y.type);for(let ue of y.layerUpdates){let Ze=ge.data.subarray(ue*ae/ge.data.BYTES_PER_ELEMENT,(ue+1)*ae/ge.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,H,0,0,ue,ge.width,ge.height,1,me,Ze,0,0)}y.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,H,0,0,0,ge.width,ge.height,ne.depth,me,ge.data,0,0)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,H,Re,ge.width,ge.height,ne.depth,0,ge.data,0,0);else console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else Oe?N&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,H,0,0,0,ge.width,ge.height,ne.depth,me,Ie,ge.data):t.texImage3D(n.TEXTURE_2D_ARRAY,H,Re,ge.width,ge.height,ne.depth,0,me,Ie,ge.data)}else{Oe&&ht&&t.texStorage2D(n.TEXTURE_2D,ce,Re,$e[0].width,$e[0].height);for(let H=0,Z=$e.length;H<Z;H++)ge=$e[H],y.format!==Bn?me!==null?Oe?N&&t.compressedTexSubImage2D(n.TEXTURE_2D,H,0,0,ge.width,ge.height,me,ge.data):t.compressedTexImage2D(n.TEXTURE_2D,H,Re,ge.width,ge.height,0,ge.data):console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):Oe?N&&t.texSubImage2D(n.TEXTURE_2D,H,0,0,ge.width,ge.height,me,Ie,ge.data):t.texImage2D(n.TEXTURE_2D,H,Re,ge.width,ge.height,0,me,Ie,ge.data)}else if(y.isDataArrayTexture)if(Oe){if(ht&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ce,Re,ne.width,ne.height,ne.depth),N)if(y.layerUpdates.size>0){let H=My(ne.width,ne.height,y.format,y.type);for(let Z of y.layerUpdates){let ae=ne.data.subarray(Z*H/ne.data.BYTES_PER_ELEMENT,(Z+1)*H/ne.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,Z,ne.width,ne.height,1,me,Ie,ae)}y.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,ne.width,ne.height,ne.depth,me,Ie,ne.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,Re,ne.width,ne.height,ne.depth,0,me,Ie,ne.data);else if(y.isData3DTexture)Oe?(ht&&t.texStorage3D(n.TEXTURE_3D,ce,Re,ne.width,ne.height,ne.depth),N&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,ne.width,ne.height,ne.depth,me,Ie,ne.data)):t.texImage3D(n.TEXTURE_3D,0,Re,ne.width,ne.height,ne.depth,0,me,Ie,ne.data);else if(y.isFramebufferTexture){if(ht)if(Oe)t.texStorage2D(n.TEXTURE_2D,ce,Re,ne.width,ne.height);else{let H=ne.width,Z=ne.height;for(let ae=0;ae<ce;ae++)t.texImage2D(n.TEXTURE_2D,ae,Re,H,Z,0,me,Ie,null),H>>=1,Z>>=1}}else if($e.length>0){if(Oe&&ht){let H=Ne($e[0]);t.texStorage2D(n.TEXTURE_2D,ce,Re,H.width,H.height)}for(let H=0,Z=$e.length;H<Z;H++)ge=$e[H],Oe?N&&t.texSubImage2D(n.TEXTURE_2D,H,0,0,me,Ie,ge):t.texImage2D(n.TEXTURE_2D,H,Re,me,Ie,ge);y.generateMipmaps=!1}else if(Oe){if(ht){let H=Ne(ne);t.texStorage2D(n.TEXTURE_2D,ce,Re,H.width,H.height)}N&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,me,Ie,ne)}else t.texImage2D(n.TEXTURE_2D,0,Re,me,Ie,ne);m(y)&&p(X),Me.__version=q.version,y.onUpdate&&y.onUpdate(y)}b.__version=y.version}function $(b,y,O){if(y.image.length!==6)return;let X=tt(b,y),Q=y.source;t.bindTexture(n.TEXTURE_CUBE_MAP,b.__webglTexture,n.TEXTURE0+O);let q=i.get(Q);if(Q.version!==q.__version||X===!0){t.activeTexture(n.TEXTURE0+O);let Me=at.getPrimaries(at.workingColorSpace),oe=y.colorSpace===$i?null:at.getPrimaries(y.colorSpace),pe=y.colorSpace===$i||Me===oe?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,y.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,y.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,y.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,pe);let et=y.isCompressedTexture||y.image[0].isCompressedTexture,ne=y.image[0]&&y.image[0].isDataTexture,me=[];for(let Z=0;Z<6;Z++)!et&&!ne?me[Z]=_(y.image[Z],!0,r.maxCubemapSize):me[Z]=ne?y.image[Z].image:y.image[Z],me[Z]=mt(y,me[Z]);let Ie=me[0],Re=s.convert(y.format,y.colorSpace),ge=s.convert(y.type),$e=w(y.internalFormat,Re,ge,y.colorSpace),Oe=y.isVideoTexture!==!0,ht=q.__version===void 0||X===!0,N=Q.dataReady,ce=S(y,Ie);xe(n.TEXTURE_CUBE_MAP,y);let H;if(et){Oe&&ht&&t.texStorage2D(n.TEXTURE_CUBE_MAP,ce,$e,Ie.width,Ie.height);for(let Z=0;Z<6;Z++){H=me[Z].mipmaps;for(let ae=0;ae<H.length;ae++){let ue=H[ae];y.format!==Bn?Re!==null?Oe?N&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae,0,0,ue.width,ue.height,Re,ue.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae,$e,ue.width,ue.height,0,ue.data):console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):Oe?N&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae,0,0,ue.width,ue.height,Re,ge,ue.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae,$e,ue.width,ue.height,0,Re,ge,ue.data)}}}else{if(H=y.mipmaps,Oe&&ht){H.length>0&&ce++;let Z=Ne(me[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,ce,$e,Z.width,Z.height)}for(let Z=0;Z<6;Z++)if(ne){Oe?N&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,me[Z].width,me[Z].height,Re,ge,me[Z].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,$e,me[Z].width,me[Z].height,0,Re,ge,me[Z].data);for(let ae=0;ae<H.length;ae++){let Ze=H[ae].image[Z].image;Oe?N&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae+1,0,0,Ze.width,Ze.height,Re,ge,Ze.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae+1,$e,Ze.width,Ze.height,0,Re,ge,Ze.data)}}else{Oe?N&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,Re,ge,me[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,$e,Re,ge,me[Z]);for(let ae=0;ae<H.length;ae++){let ue=H[ae];Oe?N&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae+1,0,0,Re,ge,ue.image[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,ae+1,$e,Re,ge,ue.image[Z])}}}m(y)&&p(n.TEXTURE_CUBE_MAP),q.__version=Q.version,y.onUpdate&&y.onUpdate(y)}b.__version=y.version}function te(b,y,O,X,Q,q){let Me=s.convert(O.format,O.colorSpace),oe=s.convert(O.type),pe=w(O.internalFormat,Me,oe,O.colorSpace);if(!i.get(y).__hasExternalTextures){let ne=Math.max(1,y.width>>q),me=Math.max(1,y.height>>q);Q===n.TEXTURE_3D||Q===n.TEXTURE_2D_ARRAY?t.texImage3D(Q,q,pe,ne,me,y.depth,0,Me,oe,null):t.texImage2D(Q,q,pe,ne,me,0,Me,oe,null)}t.bindFramebuffer(n.FRAMEBUFFER,b),Qe(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,X,Q,i.get(O).__webglTexture,0,je(y)):(Q===n.TEXTURE_2D||Q>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&Q<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,X,Q,i.get(O).__webglTexture,q),t.bindFramebuffer(n.FRAMEBUFFER,null)}function ye(b,y,O){if(n.bindRenderbuffer(n.RENDERBUFFER,b),y.depthBuffer){let X=y.depthTexture,Q=X&&X.isDepthTexture?X.type:null,q=E(y.stencilBuffer,Q),Me=y.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,oe=je(y);Qe(y)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,oe,q,y.width,y.height):O?n.renderbufferStorageMultisample(n.RENDERBUFFER,oe,q,y.width,y.height):n.renderbufferStorage(n.RENDERBUFFER,q,y.width,y.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,Me,n.RENDERBUFFER,b)}else{let X=y.textures;for(let Q=0;Q<X.length;Q++){let q=X[Q],Me=s.convert(q.format,q.colorSpace),oe=s.convert(q.type),pe=w(q.internalFormat,Me,oe,q.colorSpace),et=je(y);O&&Qe(y)===!1?n.renderbufferStorageMultisample(n.RENDERBUFFER,et,pe,y.width,y.height):Qe(y)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,et,pe,y.width,y.height):n.renderbufferStorage(n.RENDERBUFFER,pe,y.width,y.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function fe(b,y){if(y&&y.isWebGLCubeRenderTarget)throw new Error("Depth Texture with cube render targets is not supported");if(t.bindFramebuffer(n.FRAMEBUFFER,b),!(y.depthTexture&&y.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");(!i.get(y.depthTexture).__webglTexture||y.depthTexture.image.width!==y.width||y.depthTexture.image.height!==y.height)&&(y.depthTexture.image.width=y.width,y.depthTexture.image.height=y.height,y.depthTexture.needsUpdate=!0),Y(y.depthTexture,0);let X=i.get(y.depthTexture).__webglTexture,Q=je(y);if(y.depthTexture.format===ks)Qe(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,n.DEPTH_ATTACHMENT,n.TEXTURE_2D,X,0,Q):n.framebufferTexture2D(n.FRAMEBUFFER,n.DEPTH_ATTACHMENT,n.TEXTURE_2D,X,0);else if(y.depthTexture.format===Ws)Qe(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,n.DEPTH_STENCIL_ATTACHMENT,n.TEXTURE_2D,X,0,Q):n.framebufferTexture2D(n.FRAMEBUFFER,n.DEPTH_STENCIL_ATTACHMENT,n.TEXTURE_2D,X,0);else throw new Error("Unknown depthTexture format")}function Fe(b){let y=i.get(b),O=b.isWebGLCubeRenderTarget===!0;if(y.__boundDepthTexture!==b.depthTexture){let X=b.depthTexture;if(y.__depthDisposeCallback&&y.__depthDisposeCallback(),X){let Q=()=>{delete y.__boundDepthTexture,delete y.__depthDisposeCallback,X.removeEventListener("dispose",Q)};X.addEventListener("dispose",Q),y.__depthDisposeCallback=Q}y.__boundDepthTexture=X}if(b.depthTexture&&!y.__autoAllocateDepthBuffer){if(O)throw new Error("target.depthTexture not supported in Cube render targets");fe(y.__webglFramebuffer,b)}else if(O){y.__webglDepthbuffer=[];for(let X=0;X<6;X++)if(t.bindFramebuffer(n.FRAMEBUFFER,y.__webglFramebuffer[X]),y.__webglDepthbuffer[X]===void 0)y.__webglDepthbuffer[X]=n.createRenderbuffer(),ye(y.__webglDepthbuffer[X],b,!1);else{let Q=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,q=y.__webglDepthbuffer[X];n.bindRenderbuffer(n.RENDERBUFFER,q),n.framebufferRenderbuffer(n.FRAMEBUFFER,Q,n.RENDERBUFFER,q)}}else if(t.bindFramebuffer(n.FRAMEBUFFER,y.__webglFramebuffer),y.__webglDepthbuffer===void 0)y.__webglDepthbuffer=n.createRenderbuffer(),ye(y.__webglDepthbuffer,b,!1);else{let X=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,Q=y.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,Q),n.framebufferRenderbuffer(n.FRAMEBUFFER,X,n.RENDERBUFFER,Q)}t.bindFramebuffer(n.FRAMEBUFFER,null)}function Ce(b,y,O){let X=i.get(b);y!==void 0&&te(X.__webglFramebuffer,b,b.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),O!==void 0&&Fe(b)}function Xe(b){let y=b.texture,O=i.get(b),X=i.get(y);b.addEventListener("dispose",R);let Q=b.textures,q=b.isWebGLCubeRenderTarget===!0,Me=Q.length>1;if(Me||(X.__webglTexture===void 0&&(X.__webglTexture=n.createTexture()),X.__version=y.version,o.memory.textures++),q){O.__webglFramebuffer=[];for(let oe=0;oe<6;oe++)if(y.mipmaps&&y.mipmaps.length>0){O.__webglFramebuffer[oe]=[];for(let pe=0;pe<y.mipmaps.length;pe++)O.__webglFramebuffer[oe][pe]=n.createFramebuffer()}else O.__webglFramebuffer[oe]=n.createFramebuffer()}else{if(y.mipmaps&&y.mipmaps.length>0){O.__webglFramebuffer=[];for(let oe=0;oe<y.mipmaps.length;oe++)O.__webglFramebuffer[oe]=n.createFramebuffer()}else O.__webglFramebuffer=n.createFramebuffer();if(Me)for(let oe=0,pe=Q.length;oe<pe;oe++){let et=i.get(Q[oe]);et.__webglTexture===void 0&&(et.__webglTexture=n.createTexture(),o.memory.textures++)}if(b.samples>0&&Qe(b)===!1){O.__webglMultisampledFramebuffer=n.createFramebuffer(),O.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,O.__webglMultisampledFramebuffer);for(let oe=0;oe<Q.length;oe++){let pe=Q[oe];O.__webglColorRenderbuffer[oe]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,O.__webglColorRenderbuffer[oe]);let et=s.convert(pe.format,pe.colorSpace),ne=s.convert(pe.type),me=w(pe.internalFormat,et,ne,pe.colorSpace,b.isXRRenderTarget===!0),Ie=je(b);n.renderbufferStorageMultisample(n.RENDERBUFFER,Ie,me,b.width,b.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+oe,n.RENDERBUFFER,O.__webglColorRenderbuffer[oe])}n.bindRenderbuffer(n.RENDERBUFFER,null),b.depthBuffer&&(O.__webglDepthRenderbuffer=n.createRenderbuffer(),ye(O.__webglDepthRenderbuffer,b,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(q){t.bindTexture(n.TEXTURE_CUBE_MAP,X.__webglTexture),xe(n.TEXTURE_CUBE_MAP,y);for(let oe=0;oe<6;oe++)if(y.mipmaps&&y.mipmaps.length>0)for(let pe=0;pe<y.mipmaps.length;pe++)te(O.__webglFramebuffer[oe][pe],b,y,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+oe,pe);else te(O.__webglFramebuffer[oe],b,y,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+oe,0);m(y)&&p(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(Me){for(let oe=0,pe=Q.length;oe<pe;oe++){let et=Q[oe],ne=i.get(et);t.bindTexture(n.TEXTURE_2D,ne.__webglTexture),xe(n.TEXTURE_2D,et),te(O.__webglFramebuffer,b,et,n.COLOR_ATTACHMENT0+oe,n.TEXTURE_2D,0),m(et)&&p(n.TEXTURE_2D)}t.unbindTexture()}else{let oe=n.TEXTURE_2D;if((b.isWebGL3DRenderTarget||b.isWebGLArrayRenderTarget)&&(oe=b.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(oe,X.__webglTexture),xe(oe,y),y.mipmaps&&y.mipmaps.length>0)for(let pe=0;pe<y.mipmaps.length;pe++)te(O.__webglFramebuffer[pe],b,y,n.COLOR_ATTACHMENT0,oe,pe);else te(O.__webglFramebuffer,b,y,n.COLOR_ATTACHMENT0,oe,0);m(y)&&p(oe),t.unbindTexture()}b.depthBuffer&&Fe(b)}function dt(b){let y=b.textures;for(let O=0,X=y.length;O<X;O++){let Q=y[O];if(m(Q)){let q=b.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:n.TEXTURE_2D,Me=i.get(Q).__webglTexture;t.bindTexture(q,Me),p(q),t.unbindTexture()}}}let Ye=[],A=[];function un(b){if(b.samples>0){if(Qe(b)===!1){let y=b.textures,O=b.width,X=b.height,Q=n.COLOR_BUFFER_BIT,q=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,Me=i.get(b),oe=y.length>1;if(oe)for(let pe=0;pe<y.length;pe++)t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+pe,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+pe,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,Me.__webglMultisampledFramebuffer),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,Me.__webglFramebuffer);for(let pe=0;pe<y.length;pe++){if(b.resolveDepthBuffer&&(b.depthBuffer&&(Q|=n.DEPTH_BUFFER_BIT),b.stencilBuffer&&b.resolveStencilBuffer&&(Q|=n.STENCIL_BUFFER_BIT)),oe){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,Me.__webglColorRenderbuffer[pe]);let et=i.get(y[pe]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,et,0)}n.blitFramebuffer(0,0,O,X,0,0,O,X,Q,n.NEAREST),l===!0&&(Ye.length=0,A.length=0,Ye.push(n.COLOR_ATTACHMENT0+pe),b.depthBuffer&&b.resolveDepthBuffer===!1&&(Ye.push(q),A.push(q),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,A)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,Ye))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),oe)for(let pe=0;pe<y.length;pe++){t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+pe,n.RENDERBUFFER,Me.__webglColorRenderbuffer[pe]);let et=i.get(y[pe]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+pe,n.TEXTURE_2D,et,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,Me.__webglMultisampledFramebuffer)}else if(b.depthBuffer&&b.resolveDepthBuffer===!1&&l){let y=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[y])}}}function je(b){return Math.min(r.maxSamples,b.samples)}function Qe(b){let y=i.get(b);return b.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&y.__useRenderToTexture!==!1}function Ae(b){let y=o.render.frame;u.get(b)!==y&&(u.set(b,y),b.update())}function mt(b,y){let O=b.colorSpace,X=b.format,Q=b.type;return b.isCompressedTexture===!0||b.isVideoTexture===!0||O!==Qi&&O!==$i&&(at.getTransfer(O)===Mt?(X!==Bn||Q!==bi)&&console.warn("THREE.WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):console.error("THREE.WebGLTextures: Unsupported texture color space:",O)),y}function Ne(b){return typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement?(c.width=b.naturalWidth||b.width,c.height=b.naturalHeight||b.height):typeof VideoFrame<"u"&&b instanceof VideoFrame?(c.width=b.displayWidth,c.height=b.displayHeight):(c.width=b.width,c.height=b.height),c}this.allocateTextureUnit=j,this.resetTextureUnits=M,this.setTexture2D=Y,this.setTexture2DArray=J,this.setTexture3D=W,this.setTextureCube=K,this.rebindTextures=Ce,this.setupRenderTarget=Xe,this.updateRenderTargetMipmap=dt,this.updateMultisampleRenderTarget=un,this.setupDepthRenderbuffer=Fe,this.setupFrameBufferTexture=te,this.useMultisampledRTT=Qe}function dI(n,e){function t(i,r=$i){let s,o=at.getTransfer(r);if(i===bi)return n.UNSIGNED_BYTE;if(i===Kf)return n.UNSIGNED_SHORT_4_4_4_4;if(i===Jf)return n.UNSIGNED_SHORT_5_5_5_1;if(i===Ty)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===Cy)return n.BYTE;if(i===Dy)return n.SHORT;if(i===Po)return n.UNSIGNED_SHORT;if(i===Zf)return n.INT;if(i===Ir)return n.UNSIGNED_INT;if(i===_i)return n.FLOAT;if(i===ko)return n.HALF_FLOAT;if(i===Ay)return n.ALPHA;if(i===Iy)return n.RGB;if(i===Bn)return n.RGBA;if(i===Ry)return n.LUMINANCE;if(i===Ny)return n.LUMINANCE_ALPHA;if(i===ks)return n.DEPTH_COMPONENT;if(i===Ws)return n.DEPTH_STENCIL;if(i===Py)return n.RED;if(i===Qf)return n.RED_INTEGER;if(i===Fy)return n.RG;if(i===ep)return n.RG_INTEGER;if(i===tp)return n.RGBA_INTEGER;if(i===Wl||i===jl||i===$l||i===ql)if(o===Mt)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===Wl)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===jl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===$l)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===ql)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===Wl)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===jl)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===$l)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===ql)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===qh||i===Xh||i===Yh||i===Zh)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===qh)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===Xh)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===Yh)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===Zh)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===Kh||i===Jh||i===Qh)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===Kh||i===Jh)return o===Mt?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===Qh)return o===Mt?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC}else return null;if(i===ef||i===tf||i===nf||i===rf||i===sf||i===of||i===af||i===lf||i===cf||i===uf||i===df||i===hf||i===ff||i===pf)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===ef)return o===Mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===tf)return o===Mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===nf)return o===Mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===rf)return o===Mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===sf)return o===Mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===of)return o===Mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===af)return o===Mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===lf)return o===Mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===cf)return o===Mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===uf)return o===Mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===df)return o===Mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===hf)return o===Mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===ff)return o===Mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===pf)return o===Mt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===Xl||i===mf||i===gf)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===Xl)return o===Mt?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===mf)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===gf)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===Oy||i===vf||i===yf||i===_f)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===Xl)return s.COMPRESSED_RED_RGTC1_EXT;if(i===vf)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===yf)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===_f)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===Gs?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var Lf=class extends en{constructor(e=[]){super(),this.isArrayCamera=!0,this.cameras=e}},Mi=class extends wi{constructor(){super(),this.isGroup=!0,this.type="Group"}},hI={type:"move"},No=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new Mi,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new Mi,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new B,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new B),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new Mi,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new B,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new B),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,l=this._grip,c=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(c&&e.hand){o=!0;for(let _ of e.hand.values()){let m=t.getJointPose(_,i),p=this._getHandJoint(c,_);m!==null&&(p.matrix.fromArray(m.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=m.radius),p.visible=m!==null}let u=c.joints["index-finger-tip"],d=c.joints["thumb-tip"],h=u.position.distanceTo(d.position),f=.02,g=.005;c.inputState.pinching&&h>f+g?(c.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!c.inputState.pinching&&h<=f-g&&(c.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else l!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(l.matrix.fromArray(s.transform.matrix),l.matrix.decompose(l.position,l.rotation,l.scale),l.matrixWorldNeedsUpdate=!0,s.linearVelocity?(l.hasLinearVelocity=!0,l.linearVelocity.copy(s.linearVelocity)):l.hasLinearVelocity=!1,s.angularVelocity?(l.hasAngularVelocity=!0,l.angularVelocity.copy(s.angularVelocity)):l.hasAngularVelocity=!1));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(hI)))}return a!==null&&(a.visible=r!==null),l!==null&&(l.visible=s!==null),c!==null&&(c.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new Mi;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},fI=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,pI=`
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

}`,kf=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t,i){if(this.texture===null){let r=new kr,s=e.properties.get(r);s.__webglTexture=t.texture,(t.depthNear!=i.depthNear||t.depthFar!=i.depthFar)&&(this.depthNear=t.depthNear,this.depthFar=t.depthFar),this.texture=r}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new Qn({vertexShader:fI,fragmentShader:pI,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new ln(new uc(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},Uf=class extends Zi{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",l=1,c=null,u=null,d=null,h=null,f=null,g=null,_=new kf,m=t.getContextAttributes(),p=null,w=null,E=[],S=[],k=new lt,R=null,D=new en;D.layers.enable(1),D.viewport=new Dt;let V=new en;V.layers.enable(2),V.viewport=new Dt;let re=[D,V],v=new Lf;v.layers.enable(1),v.layers.enable(2);let M=null,j=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function($){let te=E[$];return te===void 0&&(te=new No,E[$]=te),te.getTargetRaySpace()},this.getControllerGrip=function($){let te=E[$];return te===void 0&&(te=new No,E[$]=te),te.getGripSpace()},this.getHand=function($){let te=E[$];return te===void 0&&(te=new No,E[$]=te),te.getHandSpace()};function z($){let te=S.indexOf($.inputSource);if(te===-1)return;let ye=E[te];ye!==void 0&&(ye.update($.inputSource,$.frame,c||o),ye.dispatchEvent({type:$.type,data:$.inputSource}))}function Y(){r.removeEventListener("select",z),r.removeEventListener("selectstart",z),r.removeEventListener("selectend",z),r.removeEventListener("squeeze",z),r.removeEventListener("squeezestart",z),r.removeEventListener("squeezeend",z),r.removeEventListener("end",Y),r.removeEventListener("inputsourceschange",J);for(let $=0;$<E.length;$++){let te=S[$];te!==null&&(S[$]=null,E[$].disconnect(te))}M=null,j=null,_.reset(),e.setRenderTarget(p),f=null,h=null,d=null,r=null,w=null,ct.stop(),i.isPresenting=!1,e.setPixelRatio(R),e.setSize(k.width,k.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function($){s=$,i.isPresenting===!0&&console.warn("THREE.WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function($){a=$,i.isPresenting===!0&&console.warn("THREE.WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return c||o},this.setReferenceSpace=function($){c=$},this.getBaseLayer=function(){return h!==null?h:f},this.getBinding=function(){return d},this.getFrame=function(){return g},this.getSession=function(){return r},this.setSession=function($){return tr(this,null,function*(){if(r=$,r!==null){if(p=e.getRenderTarget(),r.addEventListener("select",z),r.addEventListener("selectstart",z),r.addEventListener("selectend",z),r.addEventListener("squeeze",z),r.addEventListener("squeezestart",z),r.addEventListener("squeezeend",z),r.addEventListener("end",Y),r.addEventListener("inputsourceschange",J),m.xrCompatible!==!0&&(yield t.makeXRCompatible()),R=e.getPixelRatio(),e.getSize(k),r.renderState.layers===void 0){let te={antialias:m.antialias,alpha:!0,depth:m.depth,stencil:m.stencil,framebufferScaleFactor:s};f=new XRWebGLLayer(r,t,te),r.updateRenderState({baseLayer:f}),e.setPixelRatio(1),e.setSize(f.framebufferWidth,f.framebufferHeight,!1),w=new Si(f.framebufferWidth,f.framebufferHeight,{format:Bn,type:bi,colorSpace:e.outputColorSpace,stencilBuffer:m.stencil})}else{let te=null,ye=null,fe=null;m.depth&&(fe=m.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,te=m.stencil?Ws:ks,ye=m.stencil?Gs:Ir);let Fe={colorFormat:t.RGBA8,depthFormat:fe,scaleFactor:s};d=new XRWebGLBinding(r,t),h=d.createProjectionLayer(Fe),r.updateRenderState({layers:[h]}),e.setPixelRatio(1),e.setSize(h.textureWidth,h.textureHeight,!1),w=new Si(h.textureWidth,h.textureHeight,{format:Bn,type:bi,depthTexture:new fc(h.textureWidth,h.textureHeight,ye,void 0,void 0,void 0,void 0,void 0,void 0,te),stencilBuffer:m.stencil,colorSpace:e.outputColorSpace,samples:m.antialias?4:0,resolveDepthBuffer:h.ignoreDepthValues===!1})}w.isXRRenderTarget=!0,this.setFoveation(l),c=null,o=yield r.requestReferenceSpace(a),ct.setContext(r),ct.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}})},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return _.getDepthTexture()};function J($){for(let te=0;te<$.removed.length;te++){let ye=$.removed[te],fe=S.indexOf(ye);fe>=0&&(S[fe]=null,E[fe].disconnect(ye))}for(let te=0;te<$.added.length;te++){let ye=$.added[te],fe=S.indexOf(ye);if(fe===-1){for(let Ce=0;Ce<E.length;Ce++)if(Ce>=S.length){S.push(ye),fe=Ce;break}else if(S[Ce]===null){S[Ce]=ye,fe=Ce;break}if(fe===-1)break}let Fe=E[fe];Fe&&Fe.connect(ye)}}let W=new B,K=new B;function G($,te,ye){W.setFromMatrixPosition(te.matrixWorld),K.setFromMatrixPosition(ye.matrixWorld);let fe=W.distanceTo(K),Fe=te.projectionMatrix.elements,Ce=ye.projectionMatrix.elements,Xe=Fe[14]/(Fe[10]-1),dt=Fe[14]/(Fe[10]+1),Ye=(Fe[9]+1)/Fe[5],A=(Fe[9]-1)/Fe[5],un=(Fe[8]-1)/Fe[0],je=(Ce[8]+1)/Ce[0],Qe=Xe*un,Ae=Xe*je,mt=fe/(-un+je),Ne=mt*-un;if(te.matrixWorld.decompose($.position,$.quaternion,$.scale),$.translateX(Ne),$.translateZ(mt),$.matrixWorld.compose($.position,$.quaternion,$.scale),$.matrixWorldInverse.copy($.matrixWorld).invert(),Fe[10]===-1)$.projectionMatrix.copy(te.projectionMatrix),$.projectionMatrixInverse.copy(te.projectionMatrixInverse);else{let b=Xe+mt,y=dt+mt,O=Qe-Ne,X=Ae+(fe-Ne),Q=Ye*dt/y*b,q=A*dt/y*b;$.projectionMatrix.makePerspective(O,X,Q,q,b,y),$.projectionMatrixInverse.copy($.projectionMatrix).invert()}}function de($,te){te===null?$.matrixWorld.copy($.matrix):$.matrixWorld.multiplyMatrices(te.matrixWorld,$.matrix),$.matrixWorldInverse.copy($.matrixWorld).invert()}this.updateCamera=function($){if(r===null)return;let te=$.near,ye=$.far;_.texture!==null&&(_.depthNear>0&&(te=_.depthNear),_.depthFar>0&&(ye=_.depthFar)),v.near=V.near=D.near=te,v.far=V.far=D.far=ye,(M!==v.near||j!==v.far)&&(r.updateRenderState({depthNear:v.near,depthFar:v.far}),M=v.near,j=v.far);let fe=$.parent,Fe=v.cameras;de(v,fe);for(let Ce=0;Ce<Fe.length;Ce++)de(Fe[Ce],fe);Fe.length===2?G(v,D,V):v.projectionMatrix.copy(D.projectionMatrix),he($,v,fe)};function he($,te,ye){ye===null?$.matrix.copy(te.matrixWorld):($.matrix.copy(ye.matrixWorld),$.matrix.invert(),$.matrix.multiply(te.matrixWorld)),$.matrix.decompose($.position,$.quaternion,$.scale),$.updateMatrixWorld(!0),$.projectionMatrix.copy(te.projectionMatrix),$.projectionMatrixInverse.copy(te.projectionMatrixInverse),$.isPerspectiveCamera&&($.fov=Mf*2*Math.atan(1/$.projectionMatrix.elements[5]),$.zoom=1)}this.getCamera=function(){return v},this.getFoveation=function(){if(!(h===null&&f===null))return l},this.setFoveation=function($){l=$,h!==null&&(h.fixedFoveation=$),f!==null&&f.fixedFoveation!==void 0&&(f.fixedFoveation=$)},this.hasDepthSensing=function(){return _.texture!==null},this.getDepthSensingMesh=function(){return _.getMesh(v)};let xe=null;function tt($,te){if(u=te.getViewerPose(c||o),g=te,u!==null){let ye=u.views;f!==null&&(e.setRenderTargetFramebuffer(w,f.framebuffer),e.setRenderTarget(w));let fe=!1;ye.length!==v.cameras.length&&(v.cameras.length=0,fe=!0);for(let Ce=0;Ce<ye.length;Ce++){let Xe=ye[Ce],dt=null;if(f!==null)dt=f.getViewport(Xe);else{let A=d.getViewSubImage(h,Xe);dt=A.viewport,Ce===0&&(e.setRenderTargetTextures(w,A.colorTexture,h.ignoreDepthValues?void 0:A.depthStencilTexture),e.setRenderTarget(w))}let Ye=re[Ce];Ye===void 0&&(Ye=new en,Ye.layers.enable(Ce),Ye.viewport=new Dt,re[Ce]=Ye),Ye.matrix.fromArray(Xe.transform.matrix),Ye.matrix.decompose(Ye.position,Ye.quaternion,Ye.scale),Ye.projectionMatrix.fromArray(Xe.projectionMatrix),Ye.projectionMatrixInverse.copy(Ye.projectionMatrix).invert(),Ye.viewport.set(dt.x,dt.y,dt.width,dt.height),Ce===0&&(v.matrix.copy(Ye.matrix),v.matrix.decompose(v.position,v.quaternion,v.scale)),fe===!0&&v.cameras.push(Ye)}let Fe=r.enabledFeatures;if(Fe&&Fe.includes("depth-sensing")){let Ce=d.getDepthInformation(ye[0]);Ce&&Ce.isValid&&Ce.texture&&_.init(e,Ce,r.renderState)}}for(let ye=0;ye<E.length;ye++){let fe=S[ye],Fe=E[ye];fe!==null&&Fe!==void 0&&Fe.update(fe,te,c||o)}xe&&xe($,te),te.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:te}),g=null}let ct=new Hy;ct.setAnimationLoop(tt),this.setAnimationLoop=function($){xe=$},this.dispose=function(){}}},br=new Nr,mI=new Rt;function gI(n,e){function t(m,p){m.matrixAutoUpdate===!0&&m.updateMatrix(),p.value.copy(m.matrix)}function i(m,p){p.color.getRGB(m.fogColor.value,By(n)),p.isFog?(m.fogNear.value=p.near,m.fogFar.value=p.far):p.isFogExp2&&(m.fogDensity.value=p.density)}function r(m,p,w,E,S){p.isMeshBasicMaterial||p.isMeshLambertMaterial?s(m,p):p.isMeshToonMaterial?(s(m,p),d(m,p)):p.isMeshPhongMaterial?(s(m,p),u(m,p)):p.isMeshStandardMaterial?(s(m,p),h(m,p),p.isMeshPhysicalMaterial&&f(m,p,S)):p.isMeshMatcapMaterial?(s(m,p),g(m,p)):p.isMeshDepthMaterial?s(m,p):p.isMeshDistanceMaterial?(s(m,p),_(m,p)):p.isMeshNormalMaterial?s(m,p):p.isLineBasicMaterial?(o(m,p),p.isLineDashedMaterial&&a(m,p)):p.isPointsMaterial?l(m,p,w,E):p.isSpriteMaterial?c(m,p):p.isShadowMaterial?(m.color.value.copy(p.color),m.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(m,p){m.opacity.value=p.opacity,p.color&&m.diffuse.value.copy(p.color),p.emissive&&m.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.bumpMap&&(m.bumpMap.value=p.bumpMap,t(p.bumpMap,m.bumpMapTransform),m.bumpScale.value=p.bumpScale,p.side===cn&&(m.bumpScale.value*=-1)),p.normalMap&&(m.normalMap.value=p.normalMap,t(p.normalMap,m.normalMapTransform),m.normalScale.value.copy(p.normalScale),p.side===cn&&m.normalScale.value.negate()),p.displacementMap&&(m.displacementMap.value=p.displacementMap,t(p.displacementMap,m.displacementMapTransform),m.displacementScale.value=p.displacementScale,m.displacementBias.value=p.displacementBias),p.emissiveMap&&(m.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,m.emissiveMapTransform)),p.specularMap&&(m.specularMap.value=p.specularMap,t(p.specularMap,m.specularMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest);let w=e.get(p),E=w.envMap,S=w.envMapRotation;E&&(m.envMap.value=E,br.copy(S),br.x*=-1,br.y*=-1,br.z*=-1,E.isCubeTexture&&E.isRenderTargetTexture===!1&&(br.y*=-1,br.z*=-1),m.envMapRotation.value.setFromMatrix4(mI.makeRotationFromEuler(br)),m.flipEnvMap.value=E.isCubeTexture&&E.isRenderTargetTexture===!1?-1:1,m.reflectivity.value=p.reflectivity,m.ior.value=p.ior,m.refractionRatio.value=p.refractionRatio),p.lightMap&&(m.lightMap.value=p.lightMap,m.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,m.lightMapTransform)),p.aoMap&&(m.aoMap.value=p.aoMap,m.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,m.aoMapTransform))}function o(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform))}function a(m,p){m.dashSize.value=p.dashSize,m.totalSize.value=p.dashSize+p.gapSize,m.scale.value=p.scale}function l(m,p,w,E){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.size.value=p.size*w,m.scale.value=E*.5,p.map&&(m.map.value=p.map,t(p.map,m.uvTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function c(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.rotation.value=p.rotation,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function u(m,p){m.specular.value.copy(p.specular),m.shininess.value=Math.max(p.shininess,1e-4)}function d(m,p){p.gradientMap&&(m.gradientMap.value=p.gradientMap)}function h(m,p){m.metalness.value=p.metalness,p.metalnessMap&&(m.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,m.metalnessMapTransform)),m.roughness.value=p.roughness,p.roughnessMap&&(m.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,m.roughnessMapTransform)),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)}function f(m,p,w){m.ior.value=p.ior,p.sheen>0&&(m.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),m.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(m.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,m.sheenColorMapTransform)),p.sheenRoughnessMap&&(m.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,m.sheenRoughnessMapTransform))),p.clearcoat>0&&(m.clearcoat.value=p.clearcoat,m.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(m.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,m.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(m.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,m.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(m.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,m.clearcoatNormalMapTransform),m.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===cn&&m.clearcoatNormalScale.value.negate())),p.dispersion>0&&(m.dispersion.value=p.dispersion),p.iridescence>0&&(m.iridescence.value=p.iridescence,m.iridescenceIOR.value=p.iridescenceIOR,m.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],m.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(m.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,m.iridescenceMapTransform)),p.iridescenceThicknessMap&&(m.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,m.iridescenceThicknessMapTransform))),p.transmission>0&&(m.transmission.value=p.transmission,m.transmissionSamplerMap.value=w.texture,m.transmissionSamplerSize.value.set(w.width,w.height),p.transmissionMap&&(m.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,m.transmissionMapTransform)),m.thickness.value=p.thickness,p.thicknessMap&&(m.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,m.thicknessMapTransform)),m.attenuationDistance.value=p.attenuationDistance,m.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(m.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(m.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,m.anisotropyMapTransform))),m.specularIntensity.value=p.specularIntensity,m.specularColor.value.copy(p.specularColor),p.specularColorMap&&(m.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,m.specularColorMapTransform)),p.specularIntensityMap&&(m.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,m.specularIntensityMapTransform))}function g(m,p){p.matcap&&(m.matcap.value=p.matcap)}function _(m,p){let w=e.get(p).light;m.referencePosition.value.setFromMatrixPosition(w.matrixWorld),m.nearDistance.value=w.shadow.camera.near,m.farDistance.value=w.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function vI(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function l(w,E){let S=E.program;i.uniformBlockBinding(w,S)}function c(w,E){let S=r[w.id];S===void 0&&(g(w),S=u(w),r[w.id]=S,w.addEventListener("dispose",m));let k=E.program;i.updateUBOMapping(w,k);let R=e.render.frame;s[w.id]!==R&&(h(w),s[w.id]=R)}function u(w){let E=d();w.__bindingPointIndex=E;let S=n.createBuffer(),k=w.__size,R=w.usage;return n.bindBuffer(n.UNIFORM_BUFFER,S),n.bufferData(n.UNIFORM_BUFFER,k,R),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,E,S),S}function d(){for(let w=0;w<a;w++)if(o.indexOf(w)===-1)return o.push(w),w;return console.error("THREE.WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function h(w){let E=r[w.id],S=w.uniforms,k=w.__cache;n.bindBuffer(n.UNIFORM_BUFFER,E);for(let R=0,D=S.length;R<D;R++){let V=Array.isArray(S[R])?S[R]:[S[R]];for(let re=0,v=V.length;re<v;re++){let M=V[re];if(f(M,R,re,k)===!0){let j=M.__offset,z=Array.isArray(M.value)?M.value:[M.value],Y=0;for(let J=0;J<z.length;J++){let W=z[J],K=_(W);typeof W=="number"||typeof W=="boolean"?(M.__data[0]=W,n.bufferSubData(n.UNIFORM_BUFFER,j+Y,M.__data)):W.isMatrix3?(M.__data[0]=W.elements[0],M.__data[1]=W.elements[1],M.__data[2]=W.elements[2],M.__data[3]=0,M.__data[4]=W.elements[3],M.__data[5]=W.elements[4],M.__data[6]=W.elements[5],M.__data[7]=0,M.__data[8]=W.elements[6],M.__data[9]=W.elements[7],M.__data[10]=W.elements[8],M.__data[11]=0):(W.toArray(M.__data,Y),Y+=K.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,j,M.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function f(w,E,S,k){let R=w.value,D=E+"_"+S;if(k[D]===void 0)return typeof R=="number"||typeof R=="boolean"?k[D]=R:k[D]=R.clone(),!0;{let V=k[D];if(typeof R=="number"||typeof R=="boolean"){if(V!==R)return k[D]=R,!0}else if(V.equals(R)===!1)return V.copy(R),!0}return!1}function g(w){let E=w.uniforms,S=0,k=16;for(let D=0,V=E.length;D<V;D++){let re=Array.isArray(E[D])?E[D]:[E[D]];for(let v=0,M=re.length;v<M;v++){let j=re[v],z=Array.isArray(j.value)?j.value:[j.value];for(let Y=0,J=z.length;Y<J;Y++){let W=z[Y],K=_(W),G=S%k,de=G%K.boundary,he=G+de;S+=de,he!==0&&k-he<K.storage&&(S+=k-he),j.__data=new Float32Array(K.storage/Float32Array.BYTES_PER_ELEMENT),j.__offset=S,S+=K.storage}}}let R=S%k;return R>0&&(S+=k-R),w.__size=S,w.__cache={},this}function _(w){let E={boundary:0,storage:0};return typeof w=="number"||typeof w=="boolean"?(E.boundary=4,E.storage=4):w.isVector2?(E.boundary=8,E.storage=8):w.isVector3||w.isColor?(E.boundary=16,E.storage=12):w.isVector4?(E.boundary=16,E.storage=16):w.isMatrix3?(E.boundary=48,E.storage=48):w.isMatrix4?(E.boundary=64,E.storage=64):w.isTexture?console.warn("THREE.WebGLRenderer: Texture samplers can not be part of an uniforms group."):console.warn("THREE.WebGLRenderer: Unsupported uniform value type.",w),E}function m(w){let E=w.target;E.removeEventListener("dispose",m);let S=o.indexOf(E.__bindingPointIndex);o.splice(S,1),n.deleteBuffer(r[E.id]),delete r[E.id],delete s[E.id]}function p(){for(let w in r)n.deleteBuffer(r[w]);o=[],r={},s={}}return{bind:l,update:c,dispose:p}}var pc=class{constructor(e={}){let{canvas:t=xC(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:l=!0,preserveDrawingBuffer:c=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:d=!1}=e;this.isWebGLRenderer=!0;let h;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");h=i.getContextAttributes().alpha}else h=o;let f=new Uint32Array(4),g=new Int32Array(4),_=null,m=null,p=[],w=[];this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this._outputColorSpace=Kn,this.toneMapping=Xi,this.toneMappingExposure=1;let E=this,S=!1,k=0,R=0,D=null,V=-1,re=null,v=new Dt,M=new Dt,j=null,z=new Je(0),Y=0,J=t.width,W=t.height,K=1,G=null,de=null,he=new Dt(0,0,J,W),xe=new Dt(0,0,J,W),tt=!1,ct=new Oo,$=!1,te=!1,ye=new Rt,fe=new Rt,Fe=new B,Ce=new Dt,Xe={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},dt=!1;function Ye(){return D===null?K:1}let A=i;function un(x,P){return t.getContext(x,P)}try{let x={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:l,preserveDrawingBuffer:c,powerPreference:u,failIfMajorPerformanceCaveat:d};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${Yf}`),t.addEventListener("webglcontextlost",Z,!1),t.addEventListener("webglcontextrestored",ae,!1),t.addEventListener("webglcontextcreationerror",ue,!1),A===null){let P="webgl2";if(A=un(P,x),A===null)throw un(P)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(x){throw console.error("THREE.WebGLRenderer: "+x.message),x}let je,Qe,Ae,mt,Ne,b,y,O,X,Q,q,Me,oe,pe,et,ne,me,Ie,Re,ge,$e,Oe,ht,N;function ce(){je=new PA(A),je.init(),Oe=new dI(A,je),Qe=new DA(A,je,e,Oe),Ae=new lI(A),Qe.reverseDepthBuffer&&Ae.buffers.depth.setReversed(!0),mt=new LA(A),Ne=new Y1,b=new uI(A,je,Ae,Ne,Qe,Oe,mt),y=new AA(E),O=new NA(E),X=new GC(A),ht=new wA(A,X),Q=new FA(A,X,mt,ht),q=new UA(A,Q,X,mt),Re=new kA(A,Qe,b),ne=new TA(Ne),Me=new X1(E,y,O,je,Qe,ht,ne),oe=new gI(E,Ne),pe=new K1,et=new iI(je),Ie=new SA(E,y,O,Ae,q,h,l),me=new oI(E,q,Qe),N=new vI(A,mt,Qe,Ae),ge=new CA(A,je,mt),$e=new OA(A,je,mt),mt.programs=Me.programs,E.capabilities=Qe,E.extensions=je,E.properties=Ne,E.renderLists=pe,E.shadowMap=me,E.state=Ae,E.info=mt}ce();let H=new Uf(E,A);this.xr=H,this.getContext=function(){return A},this.getContextAttributes=function(){return A.getContextAttributes()},this.forceContextLoss=function(){let x=je.get("WEBGL_lose_context");x&&x.loseContext()},this.forceContextRestore=function(){let x=je.get("WEBGL_lose_context");x&&x.restoreContext()},this.getPixelRatio=function(){return K},this.setPixelRatio=function(x){x!==void 0&&(K=x,this.setSize(J,W,!1))},this.getSize=function(x){return x.set(J,W)},this.setSize=function(x,P,L=!0){if(H.isPresenting){console.warn("THREE.WebGLRenderer: Can't change size while VR device is presenting.");return}J=x,W=P,t.width=Math.floor(x*K),t.height=Math.floor(P*K),L===!0&&(t.style.width=x+"px",t.style.height=P+"px"),this.setViewport(0,0,x,P)},this.getDrawingBufferSize=function(x){return x.set(J*K,W*K).floor()},this.setDrawingBufferSize=function(x,P,L){J=x,W=P,K=L,t.width=Math.floor(x*L),t.height=Math.floor(P*L),this.setViewport(0,0,x,P)},this.getCurrentViewport=function(x){return x.copy(v)},this.getViewport=function(x){return x.copy(he)},this.setViewport=function(x,P,L,U){x.isVector4?he.set(x.x,x.y,x.z,x.w):he.set(x,P,L,U),Ae.viewport(v.copy(he).multiplyScalar(K).round())},this.getScissor=function(x){return x.copy(xe)},this.setScissor=function(x,P,L,U){x.isVector4?xe.set(x.x,x.y,x.z,x.w):xe.set(x,P,L,U),Ae.scissor(M.copy(xe).multiplyScalar(K).round())},this.getScissorTest=function(){return tt},this.setScissorTest=function(x){Ae.setScissorTest(tt=x)},this.setOpaqueSort=function(x){G=x},this.setTransparentSort=function(x){de=x},this.getClearColor=function(x){return x.copy(Ie.getClearColor())},this.setClearColor=function(){Ie.setClearColor.apply(Ie,arguments)},this.getClearAlpha=function(){return Ie.getClearAlpha()},this.setClearAlpha=function(){Ie.setClearAlpha.apply(Ie,arguments)},this.clear=function(x=!0,P=!0,L=!0){let U=0;if(x){let F=!1;if(D!==null){let ie=D.texture.format;F=ie===tp||ie===ep||ie===Qf}if(F){let ie=D.texture.type,le=ie===bi||ie===Ir||ie===Po||ie===Gs||ie===Kf||ie===Jf,ve=Ie.getClearColor(),_e=Ie.getClearAlpha(),we=ve.r,De=ve.g,Ee=ve.b;le?(f[0]=we,f[1]=De,f[2]=Ee,f[3]=_e,A.clearBufferuiv(A.COLOR,0,f)):(g[0]=we,g[1]=De,g[2]=Ee,g[3]=_e,A.clearBufferiv(A.COLOR,0,g))}else U|=A.COLOR_BUFFER_BIT}P&&(U|=A.DEPTH_BUFFER_BIT,A.clearDepth(this.capabilities.reverseDepthBuffer?0:1)),L&&(U|=A.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),A.clear(U)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.dispose=function(){t.removeEventListener("webglcontextlost",Z,!1),t.removeEventListener("webglcontextrestored",ae,!1),t.removeEventListener("webglcontextcreationerror",ue,!1),pe.dispose(),et.dispose(),Ne.dispose(),y.dispose(),O.dispose(),q.dispose(),ht.dispose(),N.dispose(),Me.dispose(),H.dispose(),H.removeEventListener("sessionstart",Ep),H.removeEventListener("sessionend",bp),er.stop()};function Z(x){x.preventDefault(),console.log("THREE.WebGLRenderer: Context Lost."),S=!0}function ae(){console.log("THREE.WebGLRenderer: Context Restored."),S=!1;let x=mt.autoReset,P=me.enabled,L=me.autoUpdate,U=me.needsUpdate,F=me.type;ce(),mt.autoReset=x,me.enabled=P,me.autoUpdate=L,me.needsUpdate=U,me.type=F}function ue(x){console.error("THREE.WebGLRenderer: A WebGL context could not be created. Reason: ",x.statusMessage)}function Ze(x){let P=x.target;P.removeEventListener("dispose",Ze),Nt(P)}function Nt(x){tn(x),Ne.remove(x)}function tn(x){let P=Ne.get(x).programs;P!==void 0&&(P.forEach(function(L){Me.releaseProgram(L)}),x.isShaderMaterial&&Me.releaseShaderCache(x))}this.renderBufferDirect=function(x,P,L,U,F,ie){P===null&&(P=Xe);let le=F.isMesh&&F.matrixWorld.determinant()<0,ve=y_(x,P,L,U,F);Ae.setMaterial(U,le);let _e=L.index,we=1;if(U.wireframe===!0){if(_e=Q.getWireframeAttribute(L),_e===void 0)return;we=2}let De=L.drawRange,Ee=L.attributes.position,ut=De.start*we,gt=(De.start+De.count)*we;ie!==null&&(ut=Math.max(ut,ie.start*we),gt=Math.min(gt,(ie.start+ie.count)*we)),_e!==null?(ut=Math.max(ut,0),gt=Math.min(gt,_e.count)):Ee!=null&&(ut=Math.max(ut,0),gt=Math.min(gt,Ee.count));let bt=gt-ut;if(bt<0||bt===1/0)return;ht.setup(F,U,ve,L,_e);let dn,it=ge;if(_e!==null&&(dn=X.get(_e),it=$e,it.setIndex(dn)),F.isMesh)U.wireframe===!0?(Ae.setLineWidth(U.wireframeLinewidth*Ye()),it.setMode(A.LINES)):it.setMode(A.TRIANGLES);else if(F.isLine){let be=U.linewidth;be===void 0&&(be=1),Ae.setLineWidth(be*Ye()),F.isLineSegments?it.setMode(A.LINES):F.isLineLoop?it.setMode(A.LINE_LOOP):it.setMode(A.LINE_STRIP)}else F.isPoints?it.setMode(A.POINTS):F.isSprite&&it.setMode(A.TRIANGLES);if(F.isBatchedMesh)if(F._multiDrawInstances!==null)it.renderMultiDrawInstances(F._multiDrawStarts,F._multiDrawCounts,F._multiDrawCount,F._multiDrawInstances);else if(je.get("WEBGL_multi_draw"))it.renderMultiDraw(F._multiDrawStarts,F._multiDrawCounts,F._multiDrawCount);else{let be=F._multiDrawStarts,Ht=F._multiDrawCounts,rt=F._multiDrawCount,In=_e?X.get(_e).bytesPerElement:1,Br=Ne.get(U).currentProgram.getUniforms();for(let hn=0;hn<rt;hn++)Br.setValue(A,"_gl_DrawID",hn),it.render(be[hn]/In,Ht[hn])}else if(F.isInstancedMesh)it.renderInstances(ut,bt,F.count);else if(L.isInstancedBufferGeometry){let be=L._maxInstanceCount!==void 0?L._maxInstanceCount:1/0,Ht=Math.min(L.instanceCount,be);it.renderInstances(ut,bt,Ht)}else it.render(ut,bt)};function nt(x,P,L){x.transparent===!0&&x.side===yi&&x.forceSinglePass===!1?(x.side=cn,x.needsUpdate=!0,jo(x,P,L),x.side=Yi,x.needsUpdate=!0,jo(x,P,L),x.side=yi):jo(x,P,L)}this.compile=function(x,P,L=null){L===null&&(L=x),m=et.get(L),m.init(P),w.push(m),L.traverseVisible(function(F){F.isLight&&F.layers.test(P.layers)&&(m.pushLight(F),F.castShadow&&m.pushShadow(F))}),x!==L&&x.traverseVisible(function(F){F.isLight&&F.layers.test(P.layers)&&(m.pushLight(F),F.castShadow&&m.pushShadow(F))}),m.setupLights();let U=new Set;return x.traverse(function(F){if(!(F.isMesh||F.isPoints||F.isLine||F.isSprite))return;let ie=F.material;if(ie)if(Array.isArray(ie))for(let le=0;le<ie.length;le++){let ve=ie[le];nt(ve,L,F),U.add(ve)}else nt(ie,L,F),U.add(ie)}),w.pop(),m=null,U},this.compileAsync=function(x,P,L=null){let U=this.compile(x,P,L);return new Promise(F=>{function ie(){if(U.forEach(function(le){Ne.get(le).currentProgram.isReady()&&U.delete(le)}),U.size===0){F(x);return}setTimeout(ie,10)}je.get("KHR_parallel_shader_compile")!==null?ie():setTimeout(ie,10)})};let nn=null;function ei(x){nn&&nn(x)}function Ep(){er.stop()}function bp(){er.start()}let er=new Hy;er.setAnimationLoop(ei),typeof self<"u"&&er.setContext(self),this.setAnimationLoop=function(x){nn=x,H.setAnimationLoop(x),x===null?er.stop():er.start()},H.addEventListener("sessionstart",Ep),H.addEventListener("sessionend",bp),this.render=function(x,P){if(P!==void 0&&P.isCamera!==!0){console.error("THREE.WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(S===!0)return;if(x.matrixWorldAutoUpdate===!0&&x.updateMatrixWorld(),P.parent===null&&P.matrixWorldAutoUpdate===!0&&P.updateMatrixWorld(),H.enabled===!0&&H.isPresenting===!0&&(H.cameraAutoUpdate===!0&&H.updateCamera(P),P=H.getCamera()),x.isScene===!0&&x.onBeforeRender(E,x,P,D),m=et.get(x,w.length),m.init(P),w.push(m),fe.multiplyMatrices(P.projectionMatrix,P.matrixWorldInverse),ct.setFromProjectionMatrix(fe),te=this.localClippingEnabled,$=ne.init(this.clippingPlanes,te),_=pe.get(x,p.length),_.init(),p.push(_),H.enabled===!0&&H.isPresenting===!0){let ie=E.xr.getDepthSensingMesh();ie!==null&&Fc(ie,P,-1/0,E.sortObjects)}Fc(x,P,0,E.sortObjects),_.finish(),E.sortObjects===!0&&_.sort(G,de),dt=H.enabled===!1||H.isPresenting===!1||H.hasDepthSensing()===!1,dt&&Ie.addToRenderList(_,x),this.info.render.frame++,$===!0&&ne.beginShadows();let L=m.state.shadowsArray;me.render(L,x,P),$===!0&&ne.endShadows(),this.info.autoReset===!0&&this.info.reset();let U=_.opaque,F=_.transmissive;if(m.setupLights(),P.isArrayCamera){let ie=P.cameras;if(F.length>0)for(let le=0,ve=ie.length;le<ve;le++){let _e=ie[le];wp(U,F,x,_e)}dt&&Ie.render(x);for(let le=0,ve=ie.length;le<ve;le++){let _e=ie[le];Sp(_,x,_e,_e.viewport)}}else F.length>0&&wp(U,F,x,P),dt&&Ie.render(x),Sp(_,x,P);D!==null&&(b.updateMultisampleRenderTarget(D),b.updateRenderTargetMipmap(D)),x.isScene===!0&&x.onAfterRender(E,x,P),ht.resetDefaultState(),V=-1,re=null,w.pop(),w.length>0?(m=w[w.length-1],$===!0&&ne.setGlobalState(E.clippingPlanes,m.state.camera)):m=null,p.pop(),p.length>0?_=p[p.length-1]:_=null};function Fc(x,P,L,U){if(x.visible===!1)return;if(x.layers.test(P.layers)){if(x.isGroup)L=x.renderOrder;else if(x.isLOD)x.autoUpdate===!0&&x.update(P);else if(x.isLight)m.pushLight(x),x.castShadow&&m.pushShadow(x);else if(x.isSprite){if(!x.frustumCulled||ct.intersectsSprite(x)){U&&Ce.setFromMatrixPosition(x.matrixWorld).applyMatrix4(fe);let le=q.update(x),ve=x.material;ve.visible&&_.push(x,le,ve,L,Ce.z,null)}}else if((x.isMesh||x.isLine||x.isPoints)&&(!x.frustumCulled||ct.intersectsObject(x))){let le=q.update(x),ve=x.material;if(U&&(x.boundingSphere!==void 0?(x.boundingSphere===null&&x.computeBoundingSphere(),Ce.copy(x.boundingSphere.center)):(le.boundingSphere===null&&le.computeBoundingSphere(),Ce.copy(le.boundingSphere.center)),Ce.applyMatrix4(x.matrixWorld).applyMatrix4(fe)),Array.isArray(ve)){let _e=le.groups;for(let we=0,De=_e.length;we<De;we++){let Ee=_e[we],ut=ve[Ee.materialIndex];ut&&ut.visible&&_.push(x,le,ut,L,Ce.z,Ee)}}else ve.visible&&_.push(x,le,ve,L,Ce.z,null)}}let ie=x.children;for(let le=0,ve=ie.length;le<ve;le++)Fc(ie[le],P,L,U)}function Sp(x,P,L,U){let F=x.opaque,ie=x.transmissive,le=x.transparent;m.setupLightsView(L),$===!0&&ne.setGlobalState(E.clippingPlanes,L),U&&Ae.viewport(v.copy(U)),F.length>0&&Wo(F,P,L),ie.length>0&&Wo(ie,P,L),le.length>0&&Wo(le,P,L),Ae.buffers.depth.setTest(!0),Ae.buffers.depth.setMask(!0),Ae.buffers.color.setMask(!0),Ae.setPolygonOffset(!1)}function wp(x,P,L,U){if((L.isScene===!0?L.overrideMaterial:null)!==null)return;m.state.transmissionRenderTarget[U.id]===void 0&&(m.state.transmissionRenderTarget[U.id]=new Si(1,1,{generateMipmaps:!0,type:je.has("EXT_color_buffer_half_float")||je.has("EXT_color_buffer_float")?ko:bi,minFilter:Ar,samples:4,stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:at.workingColorSpace}));let ie=m.state.transmissionRenderTarget[U.id],le=U.viewport||v;ie.setSize(le.z,le.w);let ve=E.getRenderTarget();E.setRenderTarget(ie),E.getClearColor(z),Y=E.getClearAlpha(),Y<1&&E.setClearColor(16777215,.5),E.clear(),dt&&Ie.render(L);let _e=E.toneMapping;E.toneMapping=Xi;let we=U.viewport;if(U.viewport!==void 0&&(U.viewport=void 0),m.setupLightsView(U),$===!0&&ne.setGlobalState(E.clippingPlanes,U),Wo(x,L,U),b.updateMultisampleRenderTarget(ie),b.updateRenderTargetMipmap(ie),je.has("WEBGL_multisampled_render_to_texture")===!1){let De=!1;for(let Ee=0,ut=P.length;Ee<ut;Ee++){let gt=P[Ee],bt=gt.object,dn=gt.geometry,it=gt.material,be=gt.group;if(it.side===yi&&bt.layers.test(U.layers)){let Ht=it.side;it.side=cn,it.needsUpdate=!0,Cp(bt,L,U,dn,it,be),it.side=Ht,it.needsUpdate=!0,De=!0}}De===!0&&(b.updateMultisampleRenderTarget(ie),b.updateRenderTargetMipmap(ie))}E.setRenderTarget(ve),E.setClearColor(z,Y),we!==void 0&&(U.viewport=we),E.toneMapping=_e}function Wo(x,P,L){let U=P.isScene===!0?P.overrideMaterial:null;for(let F=0,ie=x.length;F<ie;F++){let le=x[F],ve=le.object,_e=le.geometry,we=U===null?le.material:U,De=le.group;ve.layers.test(L.layers)&&Cp(ve,P,L,_e,we,De)}}function Cp(x,P,L,U,F,ie){x.onBeforeRender(E,P,L,U,F,ie),x.modelViewMatrix.multiplyMatrices(L.matrixWorldInverse,x.matrixWorld),x.normalMatrix.getNormalMatrix(x.modelViewMatrix),F.onBeforeRender(E,P,L,U,x,ie),F.transparent===!0&&F.side===yi&&F.forceSinglePass===!1?(F.side=cn,F.needsUpdate=!0,E.renderBufferDirect(L,P,U,F,x,ie),F.side=Yi,F.needsUpdate=!0,E.renderBufferDirect(L,P,U,F,x,ie),F.side=yi):E.renderBufferDirect(L,P,U,F,x,ie),x.onAfterRender(E,P,L,U,F,ie)}function jo(x,P,L){P.isScene!==!0&&(P=Xe);let U=Ne.get(x),F=m.state.lights,ie=m.state.shadowsArray,le=F.state.version,ve=Me.getParameters(x,F.state,ie,P,L),_e=Me.getProgramCacheKey(ve),we=U.programs;U.environment=x.isMeshStandardMaterial?P.environment:null,U.fog=P.fog,U.envMap=(x.isMeshStandardMaterial?O:y).get(x.envMap||U.environment),U.envMapRotation=U.environment!==null&&x.envMap===null?P.environmentRotation:x.envMapRotation,we===void 0&&(x.addEventListener("dispose",Ze),we=new Map,U.programs=we);let De=we.get(_e);if(De!==void 0){if(U.currentProgram===De&&U.lightsStateVersion===le)return Tp(x,ve),De}else ve.uniforms=Me.getUniforms(x),x.onBeforeCompile(ve,E),De=Me.acquireProgram(ve,_e),we.set(_e,De),U.uniforms=ve.uniforms;let Ee=U.uniforms;return(!x.isShaderMaterial&&!x.isRawShaderMaterial||x.clipping===!0)&&(Ee.clippingPlanes=ne.uniform),Tp(x,ve),U.needsLights=x_(x),U.lightsStateVersion=le,U.needsLights&&(Ee.ambientLightColor.value=F.state.ambient,Ee.lightProbe.value=F.state.probe,Ee.directionalLights.value=F.state.directional,Ee.directionalLightShadows.value=F.state.directionalShadow,Ee.spotLights.value=F.state.spot,Ee.spotLightShadows.value=F.state.spotShadow,Ee.rectAreaLights.value=F.state.rectArea,Ee.ltc_1.value=F.state.rectAreaLTC1,Ee.ltc_2.value=F.state.rectAreaLTC2,Ee.pointLights.value=F.state.point,Ee.pointLightShadows.value=F.state.pointShadow,Ee.hemisphereLights.value=F.state.hemi,Ee.directionalShadowMap.value=F.state.directionalShadowMap,Ee.directionalShadowMatrix.value=F.state.directionalShadowMatrix,Ee.spotShadowMap.value=F.state.spotShadowMap,Ee.spotLightMatrix.value=F.state.spotLightMatrix,Ee.spotLightMap.value=F.state.spotLightMap,Ee.pointShadowMap.value=F.state.pointShadowMap,Ee.pointShadowMatrix.value=F.state.pointShadowMatrix),U.currentProgram=De,U.uniformsList=null,De}function Dp(x){if(x.uniformsList===null){let P=x.currentProgram.getUniforms();x.uniformsList=Vs.seqWithValue(P.seq,x.uniforms)}return x.uniformsList}function Tp(x,P){let L=Ne.get(x);L.outputColorSpace=P.outputColorSpace,L.batching=P.batching,L.batchingColor=P.batchingColor,L.instancing=P.instancing,L.instancingColor=P.instancingColor,L.instancingMorph=P.instancingMorph,L.skinning=P.skinning,L.morphTargets=P.morphTargets,L.morphNormals=P.morphNormals,L.morphColors=P.morphColors,L.morphTargetsCount=P.morphTargetsCount,L.numClippingPlanes=P.numClippingPlanes,L.numIntersection=P.numClipIntersection,L.vertexAlphas=P.vertexAlphas,L.vertexTangents=P.vertexTangents,L.toneMapping=P.toneMapping}function y_(x,P,L,U,F){P.isScene!==!0&&(P=Xe),b.resetTextureUnits();let ie=P.fog,le=U.isMeshStandardMaterial?P.environment:null,ve=D===null?E.outputColorSpace:D.isXRRenderTarget===!0?D.texture.colorSpace:Qi,_e=(U.isMeshStandardMaterial?O:y).get(U.envMap||le),we=U.vertexColors===!0&&!!L.attributes.color&&L.attributes.color.itemSize===4,De=!!L.attributes.tangent&&(!!U.normalMap||U.anisotropy>0),Ee=!!L.morphAttributes.position,ut=!!L.morphAttributes.normal,gt=!!L.morphAttributes.color,bt=Xi;U.toneMapped&&(D===null||D.isXRRenderTarget===!0)&&(bt=E.toneMapping);let dn=L.morphAttributes.position||L.morphAttributes.normal||L.morphAttributes.color,it=dn!==void 0?dn.length:0,be=Ne.get(U),Ht=m.state.lights;if($===!0&&(te===!0||x!==re)){let _n=x===re&&U.id===V;ne.setState(U,x,_n)}let rt=!1;U.version===be.__version?(be.needsLights&&be.lightsStateVersion!==Ht.state.version||be.outputColorSpace!==ve||F.isBatchedMesh&&be.batching===!1||!F.isBatchedMesh&&be.batching===!0||F.isBatchedMesh&&be.batchingColor===!0&&F.colorTexture===null||F.isBatchedMesh&&be.batchingColor===!1&&F.colorTexture!==null||F.isInstancedMesh&&be.instancing===!1||!F.isInstancedMesh&&be.instancing===!0||F.isSkinnedMesh&&be.skinning===!1||!F.isSkinnedMesh&&be.skinning===!0||F.isInstancedMesh&&be.instancingColor===!0&&F.instanceColor===null||F.isInstancedMesh&&be.instancingColor===!1&&F.instanceColor!==null||F.isInstancedMesh&&be.instancingMorph===!0&&F.morphTexture===null||F.isInstancedMesh&&be.instancingMorph===!1&&F.morphTexture!==null||be.envMap!==_e||U.fog===!0&&be.fog!==ie||be.numClippingPlanes!==void 0&&(be.numClippingPlanes!==ne.numPlanes||be.numIntersection!==ne.numIntersection)||be.vertexAlphas!==we||be.vertexTangents!==De||be.morphTargets!==Ee||be.morphNormals!==ut||be.morphColors!==gt||be.toneMapping!==bt||be.morphTargetsCount!==it)&&(rt=!0):(rt=!0,be.__version=U.version);let In=be.currentProgram;rt===!0&&(In=jo(U,P,F));let Br=!1,hn=!1,Oc=!1,Tt=In.getUniforms(),Ci=be.uniforms;if(Ae.useProgram(In.program)&&(Br=!0,hn=!0,Oc=!0),U.id!==V&&(V=U.id,hn=!0),Br||re!==x){Qe.reverseDepthBuffer?(ye.copy(x.projectionMatrix),EC(ye),bC(ye),Tt.setValue(A,"projectionMatrix",ye)):Tt.setValue(A,"projectionMatrix",x.projectionMatrix),Tt.setValue(A,"viewMatrix",x.matrixWorldInverse);let _n=Tt.map.cameraPosition;_n!==void 0&&_n.setValue(A,Fe.setFromMatrixPosition(x.matrixWorld)),Qe.logarithmicDepthBuffer&&Tt.setValue(A,"logDepthBufFC",2/(Math.log(x.far+1)/Math.LN2)),(U.isMeshPhongMaterial||U.isMeshToonMaterial||U.isMeshLambertMaterial||U.isMeshBasicMaterial||U.isMeshStandardMaterial||U.isShaderMaterial)&&Tt.setValue(A,"isOrthographic",x.isOrthographicCamera===!0),re!==x&&(re=x,hn=!0,Oc=!0)}if(F.isSkinnedMesh){Tt.setOptional(A,F,"bindMatrix"),Tt.setOptional(A,F,"bindMatrixInverse");let _n=F.skeleton;_n&&(_n.boneTexture===null&&_n.computeBoneTexture(),Tt.setValue(A,"boneTexture",_n.boneTexture,b))}F.isBatchedMesh&&(Tt.setOptional(A,F,"batchingTexture"),Tt.setValue(A,"batchingTexture",F._matricesTexture,b),Tt.setOptional(A,F,"batchingIdTexture"),Tt.setValue(A,"batchingIdTexture",F._indirectTexture,b),Tt.setOptional(A,F,"batchingColorTexture"),F._colorsTexture!==null&&Tt.setValue(A,"batchingColorTexture",F._colorsTexture,b));let Lc=L.morphAttributes;if((Lc.position!==void 0||Lc.normal!==void 0||Lc.color!==void 0)&&Re.update(F,L,In),(hn||be.receiveShadow!==F.receiveShadow)&&(be.receiveShadow=F.receiveShadow,Tt.setValue(A,"receiveShadow",F.receiveShadow)),U.isMeshGouraudMaterial&&U.envMap!==null&&(Ci.envMap.value=_e,Ci.flipEnvMap.value=_e.isCubeTexture&&_e.isRenderTargetTexture===!1?-1:1),U.isMeshStandardMaterial&&U.envMap===null&&P.environment!==null&&(Ci.envMapIntensity.value=P.environmentIntensity),hn&&(Tt.setValue(A,"toneMappingExposure",E.toneMappingExposure),be.needsLights&&__(Ci,Oc),ie&&U.fog===!0&&oe.refreshFogUniforms(Ci,ie),oe.refreshMaterialUniforms(Ci,U,K,W,m.state.transmissionRenderTarget[x.id]),Vs.upload(A,Dp(be),Ci,b)),U.isShaderMaterial&&U.uniformsNeedUpdate===!0&&(Vs.upload(A,Dp(be),Ci,b),U.uniformsNeedUpdate=!1),U.isSpriteMaterial&&Tt.setValue(A,"center",F.center),Tt.setValue(A,"modelViewMatrix",F.modelViewMatrix),Tt.setValue(A,"normalMatrix",F.normalMatrix),Tt.setValue(A,"modelMatrix",F.matrixWorld),U.isShaderMaterial||U.isRawShaderMaterial){let _n=U.uniformsGroups;for(let kc=0,M_=_n.length;kc<M_;kc++){let Ap=_n[kc];N.update(Ap,In),N.bind(Ap,In)}}return In}function __(x,P){x.ambientLightColor.needsUpdate=P,x.lightProbe.needsUpdate=P,x.directionalLights.needsUpdate=P,x.directionalLightShadows.needsUpdate=P,x.pointLights.needsUpdate=P,x.pointLightShadows.needsUpdate=P,x.spotLights.needsUpdate=P,x.spotLightShadows.needsUpdate=P,x.rectAreaLights.needsUpdate=P,x.hemisphereLights.needsUpdate=P}function x_(x){return x.isMeshLambertMaterial||x.isMeshToonMaterial||x.isMeshPhongMaterial||x.isMeshStandardMaterial||x.isShadowMaterial||x.isShaderMaterial&&x.lights===!0}this.getActiveCubeFace=function(){return k},this.getActiveMipmapLevel=function(){return R},this.getRenderTarget=function(){return D},this.setRenderTargetTextures=function(x,P,L){Ne.get(x.texture).__webglTexture=P,Ne.get(x.depthTexture).__webglTexture=L;let U=Ne.get(x);U.__hasExternalTextures=!0,U.__autoAllocateDepthBuffer=L===void 0,U.__autoAllocateDepthBuffer||je.has("WEBGL_multisampled_render_to_texture")===!0&&(console.warn("THREE.WebGLRenderer: Render-to-texture extension was disabled because an external texture was provided"),U.__useRenderToTexture=!1)},this.setRenderTargetFramebuffer=function(x,P){let L=Ne.get(x);L.__webglFramebuffer=P,L.__useDefaultFramebuffer=P===void 0},this.setRenderTarget=function(x,P=0,L=0){D=x,k=P,R=L;let U=!0,F=null,ie=!1,le=!1;if(x){let _e=Ne.get(x);if(_e.__useDefaultFramebuffer!==void 0)Ae.bindFramebuffer(A.FRAMEBUFFER,null),U=!1;else if(_e.__webglFramebuffer===void 0)b.setupRenderTarget(x);else if(_e.__hasExternalTextures)b.rebindTextures(x,Ne.get(x.texture).__webglTexture,Ne.get(x.depthTexture).__webglTexture);else if(x.depthBuffer){let Ee=x.depthTexture;if(_e.__boundDepthTexture!==Ee){if(Ee!==null&&Ne.has(Ee)&&(x.width!==Ee.image.width||x.height!==Ee.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");b.setupDepthRenderbuffer(x)}}let we=x.texture;(we.isData3DTexture||we.isDataArrayTexture||we.isCompressedArrayTexture)&&(le=!0);let De=Ne.get(x).__webglFramebuffer;x.isWebGLCubeRenderTarget?(Array.isArray(De[P])?F=De[P][L]:F=De[P],ie=!0):x.samples>0&&b.useMultisampledRTT(x)===!1?F=Ne.get(x).__webglMultisampledFramebuffer:Array.isArray(De)?F=De[L]:F=De,v.copy(x.viewport),M.copy(x.scissor),j=x.scissorTest}else v.copy(he).multiplyScalar(K).floor(),M.copy(xe).multiplyScalar(K).floor(),j=tt;if(Ae.bindFramebuffer(A.FRAMEBUFFER,F)&&U&&Ae.drawBuffers(x,F),Ae.viewport(v),Ae.scissor(M),Ae.setScissorTest(j),ie){let _e=Ne.get(x.texture);A.framebufferTexture2D(A.FRAMEBUFFER,A.COLOR_ATTACHMENT0,A.TEXTURE_CUBE_MAP_POSITIVE_X+P,_e.__webglTexture,L)}else if(le){let _e=Ne.get(x.texture),we=P||0;A.framebufferTextureLayer(A.FRAMEBUFFER,A.COLOR_ATTACHMENT0,_e.__webglTexture,L||0,we)}V=-1},this.readRenderTargetPixels=function(x,P,L,U,F,ie,le){if(!(x&&x.isWebGLRenderTarget)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let ve=Ne.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&le!==void 0&&(ve=ve[le]),ve){Ae.bindFramebuffer(A.FRAMEBUFFER,ve);try{let _e=x.texture,we=_e.format,De=_e.type;if(!Qe.textureFormatReadable(we)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!Qe.textureTypeReadable(De)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}P>=0&&P<=x.width-U&&L>=0&&L<=x.height-F&&A.readPixels(P,L,U,F,Oe.convert(we),Oe.convert(De),ie)}finally{let _e=D!==null?Ne.get(D).__webglFramebuffer:null;Ae.bindFramebuffer(A.FRAMEBUFFER,_e)}}},this.readRenderTargetPixelsAsync=function(x,P,L,U,F,ie,le){return tr(this,null,function*(){if(!(x&&x.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let ve=Ne.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&le!==void 0&&(ve=ve[le]),ve){let _e=x.texture,we=_e.format,De=_e.type;if(!Qe.textureFormatReadable(we))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!Qe.textureTypeReadable(De))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");if(P>=0&&P<=x.width-U&&L>=0&&L<=x.height-F){Ae.bindFramebuffer(A.FRAMEBUFFER,ve);let Ee=A.createBuffer();A.bindBuffer(A.PIXEL_PACK_BUFFER,Ee),A.bufferData(A.PIXEL_PACK_BUFFER,ie.byteLength,A.STREAM_READ),A.readPixels(P,L,U,F,Oe.convert(we),Oe.convert(De),0);let ut=D!==null?Ne.get(D).__webglFramebuffer:null;Ae.bindFramebuffer(A.FRAMEBUFFER,ut);let gt=A.fenceSync(A.SYNC_GPU_COMMANDS_COMPLETE,0);return A.flush(),yield MC(A,gt,4),A.bindBuffer(A.PIXEL_PACK_BUFFER,Ee),A.getBufferSubData(A.PIXEL_PACK_BUFFER,0,ie),A.deleteBuffer(Ee),A.deleteSync(gt),ie}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")}})},this.copyFramebufferToTexture=function(x,P=null,L=0){x.isTexture!==!0&&(Yl("WebGLRenderer: copyFramebufferToTexture function signature has changed."),P=arguments[0]||null,x=arguments[1]);let U=Math.pow(2,-L),F=Math.floor(x.image.width*U),ie=Math.floor(x.image.height*U),le=P!==null?P.x:0,ve=P!==null?P.y:0;b.setTexture2D(x,0),A.copyTexSubImage2D(A.TEXTURE_2D,L,0,0,le,ve,F,ie),Ae.unbindTexture()},this.copyTextureToTexture=function(x,P,L=null,U=null,F=0){x.isTexture!==!0&&(Yl("WebGLRenderer: copyTextureToTexture function signature has changed."),U=arguments[0]||null,x=arguments[1],P=arguments[2],F=arguments[3]||0,L=null);let ie,le,ve,_e,we,De;L!==null?(ie=L.max.x-L.min.x,le=L.max.y-L.min.y,ve=L.min.x,_e=L.min.y):(ie=x.image.width,le=x.image.height,ve=0,_e=0),U!==null?(we=U.x,De=U.y):(we=0,De=0);let Ee=Oe.convert(P.format),ut=Oe.convert(P.type);b.setTexture2D(P,0),A.pixelStorei(A.UNPACK_FLIP_Y_WEBGL,P.flipY),A.pixelStorei(A.UNPACK_PREMULTIPLY_ALPHA_WEBGL,P.premultiplyAlpha),A.pixelStorei(A.UNPACK_ALIGNMENT,P.unpackAlignment);let gt=A.getParameter(A.UNPACK_ROW_LENGTH),bt=A.getParameter(A.UNPACK_IMAGE_HEIGHT),dn=A.getParameter(A.UNPACK_SKIP_PIXELS),it=A.getParameter(A.UNPACK_SKIP_ROWS),be=A.getParameter(A.UNPACK_SKIP_IMAGES),Ht=x.isCompressedTexture?x.mipmaps[F]:x.image;A.pixelStorei(A.UNPACK_ROW_LENGTH,Ht.width),A.pixelStorei(A.UNPACK_IMAGE_HEIGHT,Ht.height),A.pixelStorei(A.UNPACK_SKIP_PIXELS,ve),A.pixelStorei(A.UNPACK_SKIP_ROWS,_e),x.isDataTexture?A.texSubImage2D(A.TEXTURE_2D,F,we,De,ie,le,Ee,ut,Ht.data):x.isCompressedTexture?A.compressedTexSubImage2D(A.TEXTURE_2D,F,we,De,Ht.width,Ht.height,Ee,Ht.data):A.texSubImage2D(A.TEXTURE_2D,F,we,De,ie,le,Ee,ut,Ht),A.pixelStorei(A.UNPACK_ROW_LENGTH,gt),A.pixelStorei(A.UNPACK_IMAGE_HEIGHT,bt),A.pixelStorei(A.UNPACK_SKIP_PIXELS,dn),A.pixelStorei(A.UNPACK_SKIP_ROWS,it),A.pixelStorei(A.UNPACK_SKIP_IMAGES,be),F===0&&P.generateMipmaps&&A.generateMipmap(A.TEXTURE_2D),Ae.unbindTexture()},this.copyTextureToTexture3D=function(x,P,L=null,U=null,F=0){x.isTexture!==!0&&(Yl("WebGLRenderer: copyTextureToTexture3D function signature has changed."),L=arguments[0]||null,U=arguments[1]||null,x=arguments[2],P=arguments[3],F=arguments[4]||0);let ie,le,ve,_e,we,De,Ee,ut,gt,bt=x.isCompressedTexture?x.mipmaps[F]:x.image;L!==null?(ie=L.max.x-L.min.x,le=L.max.y-L.min.y,ve=L.max.z-L.min.z,_e=L.min.x,we=L.min.y,De=L.min.z):(ie=bt.width,le=bt.height,ve=bt.depth,_e=0,we=0,De=0),U!==null?(Ee=U.x,ut=U.y,gt=U.z):(Ee=0,ut=0,gt=0);let dn=Oe.convert(P.format),it=Oe.convert(P.type),be;if(P.isData3DTexture)b.setTexture3D(P,0),be=A.TEXTURE_3D;else if(P.isDataArrayTexture||P.isCompressedArrayTexture)b.setTexture2DArray(P,0),be=A.TEXTURE_2D_ARRAY;else{console.warn("THREE.WebGLRenderer.copyTextureToTexture3D: only supports THREE.DataTexture3D and THREE.DataTexture2DArray.");return}A.pixelStorei(A.UNPACK_FLIP_Y_WEBGL,P.flipY),A.pixelStorei(A.UNPACK_PREMULTIPLY_ALPHA_WEBGL,P.premultiplyAlpha),A.pixelStorei(A.UNPACK_ALIGNMENT,P.unpackAlignment);let Ht=A.getParameter(A.UNPACK_ROW_LENGTH),rt=A.getParameter(A.UNPACK_IMAGE_HEIGHT),In=A.getParameter(A.UNPACK_SKIP_PIXELS),Br=A.getParameter(A.UNPACK_SKIP_ROWS),hn=A.getParameter(A.UNPACK_SKIP_IMAGES);A.pixelStorei(A.UNPACK_ROW_LENGTH,bt.width),A.pixelStorei(A.UNPACK_IMAGE_HEIGHT,bt.height),A.pixelStorei(A.UNPACK_SKIP_PIXELS,_e),A.pixelStorei(A.UNPACK_SKIP_ROWS,we),A.pixelStorei(A.UNPACK_SKIP_IMAGES,De),x.isDataTexture||x.isData3DTexture?A.texSubImage3D(be,F,Ee,ut,gt,ie,le,ve,dn,it,bt.data):P.isCompressedArrayTexture?A.compressedTexSubImage3D(be,F,Ee,ut,gt,ie,le,ve,dn,bt.data):A.texSubImage3D(be,F,Ee,ut,gt,ie,le,ve,dn,it,bt),A.pixelStorei(A.UNPACK_ROW_LENGTH,Ht),A.pixelStorei(A.UNPACK_IMAGE_HEIGHT,rt),A.pixelStorei(A.UNPACK_SKIP_PIXELS,In),A.pixelStorei(A.UNPACK_SKIP_ROWS,Br),A.pixelStorei(A.UNPACK_SKIP_IMAGES,hn),F===0&&P.generateMipmaps&&A.generateMipmap(be),Ae.unbindTexture()},this.initRenderTarget=function(x){Ne.get(x).__webglFramebuffer===void 0&&b.setupRenderTarget(x)},this.initTexture=function(x){x.isCubeTexture?b.setTextureCube(x,0):x.isData3DTexture?b.setTexture3D(x,0):x.isDataArrayTexture||x.isCompressedArrayTexture?b.setTexture2DArray(x,0):b.setTexture2D(x,0),Ae.unbindTexture()},this.resetState=function(){k=0,R=0,D=null,Ae.reset(),ht.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return xi}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=e===np?"display-p3":"srgb",t.unpackColorSpace=at.workingColorSpace===Mc?"display-p3":"srgb"}};var mc=class extends wi{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new Nr,this.environmentIntensity=1,this.environmentRotation=new Nr,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}};var Lo=class extends Pr{constructor(e){super(),this.isMeshStandardMaterial=!0,this.defines={STANDARD:""},this.type="MeshStandardMaterial",this.color=new Je(16777215),this.roughness=1,this.metalness=0,this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.emissive=new Je(0),this.emissiveIntensity=1,this.emissiveMap=null,this.bumpMap=null,this.bumpScale=1,this.normalMap=null,this.normalMapType=Ly,this.normalScale=new lt(1,1),this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.roughnessMap=null,this.metalnessMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new Nr,this.envMapIntensity=1,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.flatShading=!1,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.defines={STANDARD:""},this.color.copy(e.color),this.roughness=e.roughness,this.metalness=e.metalness,this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.emissive.copy(e.emissive),this.emissiveMap=e.emissiveMap,this.emissiveIntensity=e.emissiveIntensity,this.bumpMap=e.bumpMap,this.bumpScale=e.bumpScale,this.normalMap=e.normalMap,this.normalMapType=e.normalMapType,this.normalScale.copy(e.normalScale),this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.roughnessMap=e.roughnessMap,this.metalnessMap=e.metalnessMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.envMapIntensity=e.envMapIntensity,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.flatShading=e.flatShading,this.fog=e.fog,this}};function Gl(n,e,t){return!n||!t&&n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}function yI(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}var $s=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let l=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===l)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},Vf=class extends $s{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:I0,endingEnd:I0}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],l=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case R0:s=e,a=2*t-i;break;case N0:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(l===void 0)switch(this.getSettings_().endingEnd){case R0:o=e,l=2*i-t;break;case N0:o=1,l=i+r[1]-r[0];break;default:o=e-1,l=t}let c=(i-t)*.5,u=this.valueSize;this._weightPrev=c/(t-a),this._weightNext=c/(l-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,l=e*a,c=l-a,u=this._offsetPrev,d=this._offsetNext,h=this._weightPrev,f=this._weightNext,g=(i-t)/(r-t),_=g*g,m=_*g,p=-h*m+2*h*_-h*g,w=(1+h)*m+(-1.5-2*h)*_+(-.5+h)*g+1,E=(-1-f)*m+(1.5+f)*_+.5*g,S=f*m-f*_;for(let k=0;k!==a;++k)s[k]=p*o[u+k]+w*o[c+k]+E*o[l+k]+S*o[d+k];return s}},Bf=class extends $s{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,l=e*a,c=l-a,u=(i-t)/(r-t),d=1-u;for(let h=0;h!==a;++h)s[h]=o[c+h]*d+o[l+h]*u;return s}},Hf=class extends $s{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},Hn=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=Gl(t,this.TimeBufferType),this.values=Gl(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:Gl(e.times,Array),values:Gl(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new Hf(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new Bf(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new Vf(this.times,this.values,this.getValueSize(),e)}setInterpolation(e){let t;switch(e){case Zl:t=this.InterpolantFactoryMethodDiscrete;break;case xf:t=this.InterpolantFactoryMethodLinear;break;case ih:t=this.InterpolantFactoryMethodSmooth;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return console.warn("THREE.KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return Zl;case this.InterpolantFactoryMethodLinear:return xf;case this.InterpolantFactoryMethodSmooth:return ih}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(console.error("THREE.KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(console.error("THREE.KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let l=i[a];if(typeof l=="number"&&isNaN(l)){console.error("THREE.KeyframeTrack: Time is not a valid number.",this,a,l),e=!1;break}if(o!==null&&o>l){console.error("THREE.KeyframeTrack: Out of order keys.",this,a,l,o),e=!1;break}o=l}if(r!==void 0&&yI(r))for(let a=0,l=r.length;a!==l;++a){let c=r[a];if(isNaN(c)){console.error("THREE.KeyframeTrack: Value is not a valid number.",this,a,c),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===ih,s=e.length-1,o=1;for(let a=1;a<s;++a){let l=!1,c=e[a],u=e[a+1];if(c!==u&&(a!==1||c!==e[0]))if(r)l=!0;else{let d=a*i,h=d-i,f=d+i;for(let g=0;g!==i;++g){let _=t[d+g];if(_!==t[h+g]||_!==t[f+g]){l=!0;break}}}if(l){if(a!==o){e[o]=e[a];let d=a*i,h=o*i;for(let f=0;f!==i;++f)t[h+f]=t[d+f]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,l=o*i,c=0;c!==i;++c)t[l+c]=t[a+c];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};Hn.prototype.TimeBufferType=Float32Array;Hn.prototype.ValueBufferType=Float32Array;Hn.prototype.DefaultInterpolation=xf;var Or=class extends Hn{constructor(e,t,i){super(e,t,i)}};Or.prototype.ValueTypeName="bool";Or.prototype.ValueBufferType=Array;Or.prototype.DefaultInterpolation=Zl;Or.prototype.InterpolantFactoryMethodLinear=void 0;Or.prototype.InterpolantFactoryMethodSmooth=void 0;var zf=class extends Hn{};zf.prototype.ValueTypeName="color";var Gf=class extends Hn{};Gf.prototype.ValueTypeName="number";var Wf=class extends $s{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,l=(i-t)/(r-t),c=e*a;for(let u=c+a;c!==u;c+=4)Ki.slerpFlat(s,0,o,c-a,o,c,l);return s}},gc=class extends Hn{InterpolantFactoryMethodLinear(e){return new Wf(this.times,this.values,this.getValueSize(),e)}};gc.prototype.ValueTypeName="quaternion";gc.prototype.InterpolantFactoryMethodSmooth=void 0;var Lr=class extends Hn{constructor(e,t,i){super(e,t,i)}};Lr.prototype.ValueTypeName="string";Lr.prototype.ValueBufferType=Array;Lr.prototype.DefaultInterpolation=Zl;Lr.prototype.InterpolantFactoryMethodLinear=void 0;Lr.prototype.InterpolantFactoryMethodSmooth=void 0;var jf=class extends Hn{};jf.prototype.ValueTypeName="vector";var vc=class extends wi{constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new Je(e),this.intensity=t}dispose(){}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,this.groundColor!==void 0&&(t.object.groundColor=this.groundColor.getHex()),this.distance!==void 0&&(t.object.distance=this.distance),this.angle!==void 0&&(t.object.angle=this.angle),this.decay!==void 0&&(t.object.decay=this.decay),this.penumbra!==void 0&&(t.object.penumbra=this.penumbra),this.shadow!==void 0&&(t.object.shadow=this.shadow.toJSON()),this.target!==void 0&&(t.object.target=this.target.uuid),t}};var Ph=new Rt,Ey=new B,by=new B,$f=class{constructor(e){this.camera=e,this.intensity=1,this.bias=0,this.normalBias=0,this.radius=1,this.blurSamples=8,this.mapSize=new lt(512,512),this.map=null,this.mapPass=null,this.matrix=new Rt,this.autoUpdate=!0,this.needsUpdate=!1,this._frustum=new Oo,this._frameExtents=new lt(1,1),this._viewportCount=1,this._viewports=[new Dt(0,0,1,1)]}getViewportCount(){return this._viewportCount}getFrustum(){return this._frustum}updateMatrices(e){let t=this.camera,i=this.matrix;Ey.setFromMatrixPosition(e.matrixWorld),t.position.copy(Ey),by.setFromMatrixPosition(e.target.matrixWorld),t.lookAt(by),t.updateMatrixWorld(),Ph.multiplyMatrices(t.projectionMatrix,t.matrixWorldInverse),this._frustum.setFromProjectionMatrix(Ph),i.set(.5,0,0,.5,0,.5,0,.5,0,0,.5,.5,0,0,0,1),i.multiply(Ph)}getViewport(e){return this._viewports[e]}getFrameExtents(){return this._frameExtents}dispose(){this.map&&this.map.dispose(),this.mapPass&&this.mapPass.dispose()}copy(e){return this.camera=e.camera.clone(),this.intensity=e.intensity,this.bias=e.bias,this.radius=e.radius,this.mapSize.copy(e.mapSize),this}clone(){return new this.constructor().copy(this)}toJSON(){let e={};return this.intensity!==1&&(e.intensity=this.intensity),this.bias!==0&&(e.bias=this.bias),this.normalBias!==0&&(e.normalBias=this.normalBias),this.radius!==1&&(e.radius=this.radius),(this.mapSize.x!==512||this.mapSize.y!==512)&&(e.mapSize=this.mapSize.toArray()),e.camera=this.camera.toJSON(!1).object,delete e.camera.matrix,e}};var qf=class extends $f{constructor(){super(new dc(-5,5,5,-5,.5,500)),this.isDirectionalLightShadow=!0}},yc=class extends vc{constructor(e,t){super(e,t),this.isDirectionalLight=!0,this.type="DirectionalLight",this.position.copy(wi.DEFAULT_UP),this.updateMatrix(),this.target=new wi,this.shadow=new qf}dispose(){this.shadow.dispose()}copy(e){return super.copy(e),this.target=e.target.clone(),this.shadow=e.shadow.clone(),this}},_c=class extends vc{constructor(e,t){super(e,t),this.isAmbientLight=!0,this.type="AmbientLight"}};var rp="\\[\\]\\.:\\/",_I=new RegExp("["+rp+"]","g"),sp="[^"+rp+"]",xI="[^"+rp.replace("\\.","")+"]",MI=/((?:WC+[\/:])*)/.source.replace("WC",sp),EI=/(WCOD+)?/.source.replace("WCOD",xI),bI=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",sp),SI=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",sp),wI=new RegExp("^"+MI+EI+bI+SI+"$"),CI=["material","materials","bones","map"],Xf=class{constructor(e,t,i){let r=i||It.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},It=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(_I,"")}static parseTrackName(t){let i=wI.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);CI.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let l=o[a];if(l.name===i||l.uuid===i)return l;let c=r(l.children);if(c)return c}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){console.warn("THREE.PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){console.error("THREE.PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){console.error("THREE.PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){console.error("THREE.PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let d=0;d<t.length;d++)if(t[d].name===u){u=d;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){console.error("THREE.PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){console.error("THREE.PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){console.error("THREE.PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){console.error("THREE.PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;console.error("THREE.PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let l=this.Versioning.None;this.targetObject=t,t.needsUpdate!==void 0?l=this.Versioning.NeedsUpdate:t.matrixWorldNeedsUpdate!==void 0&&(l=this.Versioning.MatrixWorldNeedsUpdate);let c=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){console.error("THREE.PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){console.error("THREE.PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}c=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(c=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(c=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[c],this.setValue=this.SetterByBindingTypeAndVersioning[c][l]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=Xf,n})();It.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};It.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};It.prototype.GetterByBindingType=[It.prototype._getValue_direct,It.prototype._getValue_array,It.prototype._getValue_arrayElement,It.prototype._getValue_toArray];It.prototype.SetterByBindingTypeAndVersioning=[[It.prototype._setValue_direct,It.prototype._setValue_direct_setNeedsUpdate,It.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[It.prototype._setValue_array,It.prototype._setValue_array_setNeedsUpdate,It.prototype._setValue_array_setMatrixWorldNeedsUpdate],[It.prototype._setValue_arrayElement,It.prototype._setValue_arrayElement_setNeedsUpdate,It.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[It.prototype._setValue_fromArray,It.prototype._setValue_fromArray_setNeedsUpdate,It.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var lO=new Float32Array(1);typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:Yf}}));typeof window<"u"&&(window.__THREE__?console.warn("WARNING: Multiple instances of Three.js being imported."):window.__THREE__=Yf);var TI=["c"],bc=class n{constructor(e){this.zone=e;this.stop=()=>{}}ngAfterViewInit(){this.zone.runOutsideAngular(()=>this.init())}ngOnDestroy(){this.stop()}init(){let e=this.c.nativeElement,t=new pc({canvas:e,alpha:!0,antialias:!0});t.setPixelRatio(Math.min(devicePixelRatio,2));let i=new mc,r=new en(45,1,.1,100);r.position.z=14,i.add(new _c(16777215,1.2));let s=new yc(16777215,2.4);s.position.set(5,8,6),i.add(s);let o=new Mi;i.add(o);let a=[16742938,16765601,2241643].map(E=>new Lo({color:E,roughness:.45,metalness:.1})),l=new Ji(1,1,1),c=new Ji(1.02,.12,1.02),u=new Lo({color:16115400,roughness:.6}),d=[];for(let E=0;E<18;E++){let S=new Mi;S.add(new ln(l,a[E%3]),new ln(c,u)),S.scale.setScalar(.6+Math.random()*1.1),S.position.set((Math.random()-.5)*18,(Math.random()-.5)*9,(Math.random()-.5)*6-1),S.rotation.set(Math.random()*3,Math.random()*3,0),o.add(S),d.push({m:S,v:.1+Math.random()*.3,p:Math.random()*6})}let h={x:0,y:0},f=E=>{h.x=E.clientX/innerWidth-.5,h.y=E.clientY/innerHeight-.5},g=()=>{let E=e.parentElement;t.setSize(E.clientWidth,E.clientHeight,!1),r.aspect=E.clientWidth/E.clientHeight,r.updateProjectionMatrix()};addEventListener("mousemove",f),addEventListener("resize",g),g();let _=matchMedia("(prefers-reduced-motion: reduce)").matches,m=0,p=performance.now(),w=()=>{let E=(performance.now()-p)/1e3;d.forEach(S=>{S.m.rotation.x+=S.v*.01,S.m.rotation.y+=S.v*.012,S.m.position.y+=Math.sin(E+S.p)*.003}),o.rotation.y=scrollY*.0012,o.position.y=scrollY*.004,r.position.x+=(h.x*3-r.position.x)*.05,r.position.y+=(-h.y*2-r.position.y)*.05,r.lookAt(0,0,0),t.render(i,r),_||(m=requestAnimationFrame(w))};w(),this.stop=()=>{cancelAnimationFrame(m),removeEventListener("mousemove",f),removeEventListener("resize",g),t.dispose(),l.dispose()}}static{this.\u0275fac=function(t){return new(t||n)(Et(wt))}}static{this.\u0275cmp=hs({type:n,selectors:[["app-hero-3d"]],viewQuery:function(t,i){if(t&1&&i0(TI,7),t&2){let r;r0(r=s0())&&(i.c=r.first)}},standalone:!0,features:[ys],decls:2,vars:0,consts:[["c",""]],template:function(t,i){t&1&&Kt(0,"canvas",null,0)},styles:["[_nghost-%COMP%]{position:absolute;inset:0;display:block}canvas[_ngcontent-%COMP%]{width:100%;height:100%;display:block}"]})}};var e_=(()=>{class n{constructor(t,i){this._renderer=t,this._elementRef=i,this.onChange=r=>{},this.onTouched=()=>{}}setProperty(t,i){this._renderer.setProperty(this._elementRef.nativeElement,t,i)}registerOnTouched(t){this.onTouched=t}registerOnChange(t){this.onChange=t}setDisabledState(t){this.setProperty("disabled",t)}static{this.\u0275fac=function(i){return new(i||n)(Et(ml),Et(Cn))}}static{this.\u0275dir=Vt({type:n})}}return n})(),t_=(()=>{class n extends e_{static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=li(n)))(r||n)}})()}static{this.\u0275dir=Vt({type:n,features:[fn]})}}return n})(),dp=new ze("");var AI={provide:dp,useExisting:Sn(()=>Tc),multi:!0};function II(){let n=xs()?xs().getUserAgent():"";return/android (\d+)/.test(n.toLowerCase())}var RI=new ze(""),Tc=(()=>{class n extends e_{constructor(t,i,r){super(t,i),this._compositionMode=r,this._composing=!1,this._compositionMode==null&&(this._compositionMode=!II())}writeValue(t){let i=t??"";this.setProperty("value",i)}_handleInput(t){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(t)}_compositionStart(){this._composing=!0}_compositionEnd(t){this._composing=!1,this._compositionMode&&this.onChange(t)}static{this.\u0275fac=function(i){return new(i||n)(Et(ml),Et(Cn),Et(RI,8))}}static{this.\u0275dir=Vt({type:n,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&He("input",function(o){return r._handleInput(o.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(o){return r._compositionEnd(o.target.value)})},features:[Zn([AI]),fn]})}}return n})();function Ur(n){return n==null||(typeof n=="string"||Array.isArray(n))&&n.length===0}function n_(n){return n!=null&&typeof n.length=="number"}var Vr=new ze(""),NI=new ze("");function PI(n){return e=>{if(Ur(e.value)||Ur(n))return null;let t=parseFloat(e.value);return!isNaN(t)&&t<n?{min:{min:n,actual:e.value}}:null}}function FI(n){return e=>{if(Ur(e.value)||Ur(n))return null;let t=parseFloat(e.value);return!isNaN(t)&&t>n?{max:{max:n,actual:e.value}}:null}}function OI(n){return Ur(n.value)?{required:!0}:null}function LI(n){return e=>Ur(e.value)||!n_(e.value)?null:e.value.length<n?{minlength:{requiredLength:n,actualLength:e.value.length}}:null}function kI(n){return e=>n_(e.value)&&e.value.length>n?{maxlength:{requiredLength:n,actualLength:e.value.length}}:null}function UI(n){if(!n)return op;let e,t;return typeof n=="string"?(t="",n.charAt(0)!=="^"&&(t+="^"),t+=n,n.charAt(n.length-1)!=="$"&&(t+="$"),e=new RegExp(t)):(t=n.toString(),e=n),i=>{if(Ur(i.value))return null;let r=i.value;return e.test(r)?null:{pattern:{requiredPattern:t,actualValue:r}}}}function op(n){return null}function i_(n){return n!=null}function r_(n){return xo(n)?Jc(n):n}function s_(n){let e={};return n.forEach(t=>{e=t!=null?vt(vt({},e),t):e}),Object.keys(e).length===0?null:e}function o_(n,e){return e.map(t=>t(n))}function VI(n){return!n.validate}function a_(n){return n.map(e=>VI(e)?e:t=>e.validate(t))}function BI(n){if(!n)return null;let e=n.filter(i_);return e.length==0?null:function(t){return s_(o_(t,e))}}function l_(n){return n!=null?BI(a_(n)):null}function HI(n){if(!n)return null;let e=n.filter(i_);return e.length==0?null:function(t){let i=o_(t,e).map(r_);return Qc(i).pipe(sr(s_))}}function c_(n){return n!=null?HI(a_(n)):null}function qy(n,e){return n===null?[e]:Array.isArray(n)?[...n,e]:[n,e]}function zI(n){return n._rawValidators}function GI(n){return n._rawAsyncValidators}function ap(n){return n?Array.isArray(n)?n:[n]:[]}function wc(n,e){return Array.isArray(n)?n.includes(e):n===e}function Xy(n,e){let t=ap(e);return ap(n).forEach(r=>{wc(t,r)||t.push(r)}),t}function Yy(n,e){return ap(e).filter(t=>!wc(n,t))}var Cc=class{constructor(){this._rawValidators=[],this._rawAsyncValidators=[],this._onDestroyCallbacks=[]}get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_setValidators(e){this._rawValidators=e||[],this._composedValidatorFn=l_(this._rawValidators)}_setAsyncValidators(e){this._rawAsyncValidators=e||[],this._composedAsyncValidatorFn=c_(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_registerOnDestroy(e){this._onDestroyCallbacks.push(e)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(e=>e()),this._onDestroyCallbacks=[]}reset(e=void 0){this.control&&this.control.reset(e)}hasError(e,t){return this.control?this.control.hasError(e,t):!1}getError(e,t){return this.control?this.control.getError(e,t):null}},lp=class extends Cc{get formDirective(){return null}get path(){return null}},Go=class extends Cc{constructor(){super(...arguments),this._parent=null,this.name=null,this.valueAccessor=null}},cp=class{constructor(e){this._cd=e}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}},WI={"[class.ng-untouched]":"isUntouched","[class.ng-touched]":"isTouched","[class.ng-pristine]":"isPristine","[class.ng-dirty]":"isDirty","[class.ng-valid]":"isValid","[class.ng-invalid]":"isInvalid","[class.ng-pending]":"isPending"},CO=St(vt({},WI),{"[class.ng-submitted]":"isSubmitted"}),u_=(()=>{class n extends cp{constructor(t){super(t)}static{this.\u0275fac=function(i){return new(i||n)(Et(Go,2))}}static{this.\u0275dir=Vt({type:n,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&Oi("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},features:[fn]})}}return n})();var Vo="VALID",Sc="INVALID",Xs="PENDING",Bo="DISABLED",Zs=class{},Dc=class extends Zs{constructor(e,t){super(),this.value=e,this.source=t}},Ho=class extends Zs{constructor(e,t){super(),this.pristine=e,this.source=t}},zo=class extends Zs{constructor(e,t){super(),this.touched=e,this.source=t}},Ys=class extends Zs{constructor(e,t){super(),this.status=e,this.source=t}};function jI(n){return(Ac(n)?n.validators:n)||null}function $I(n){return Array.isArray(n)?l_(n):n||null}function qI(n,e){return(Ac(e)?e.asyncValidators:n)||null}function XI(n){return Array.isArray(n)?c_(n):n||null}function Ac(n){return n!=null&&!Array.isArray(n)&&typeof n=="object"}var up=class{constructor(e,t){this._pendingDirty=!1,this._hasOwnPendingAsyncValidator=null,this._pendingTouched=!1,this._onCollectionChange=()=>{},this._parent=null,this._status=vl(()=>this.statusReactive()),this.statusReactive=$t(void 0),this._pristine=vl(()=>this.pristineReactive()),this.pristineReactive=$t(!0),this._touched=vl(()=>this.touchedReactive()),this.touchedReactive=$t(!1),this._events=new zn,this.events=this._events.asObservable(),this._onDisabledChange=[],this._assignValidators(e),this._assignAsyncValidators(t)}get validator(){return this._composedValidatorFn}set validator(e){this._rawValidators=this._composedValidatorFn=e}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(e){this._rawAsyncValidators=this._composedAsyncValidatorFn=e}get parent(){return this._parent}get status(){return Ui(this.statusReactive)}set status(e){Ui(()=>this.statusReactive.set(e))}get valid(){return this.status===Vo}get invalid(){return this.status===Sc}get pending(){return this.status==Xs}get disabled(){return this.status===Bo}get enabled(){return this.status!==Bo}get pristine(){return Ui(this.pristineReactive)}set pristine(e){Ui(()=>this.pristineReactive.set(e))}get dirty(){return!this.pristine}get touched(){return Ui(this.touchedReactive)}set touched(e){Ui(()=>this.touchedReactive.set(e))}get untouched(){return!this.touched}get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(e){this._assignValidators(e)}setAsyncValidators(e){this._assignAsyncValidators(e)}addValidators(e){this.setValidators(Xy(e,this._rawValidators))}addAsyncValidators(e){this.setAsyncValidators(Xy(e,this._rawAsyncValidators))}removeValidators(e){this.setValidators(Yy(e,this._rawValidators))}removeAsyncValidators(e){this.setAsyncValidators(Yy(e,this._rawAsyncValidators))}hasValidator(e){return wc(this._rawValidators,e)}hasAsyncValidator(e){return wc(this._rawAsyncValidators,e)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(e={}){let t=this.touched===!1;this.touched=!0;let i=e.sourceControl??this;this._parent&&!e.onlySelf&&this._parent.markAsTouched(St(vt({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new zo(!0,i))}markAllAsTouched(e={}){this.markAsTouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsTouched(e))}markAsUntouched(e={}){let t=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:i})}),this._parent&&!e.onlySelf&&this._parent._updateTouched(e,i),t&&e.emitEvent!==!1&&this._events.next(new zo(!1,i))}markAsDirty(e={}){let t=this.pristine===!0;this.pristine=!1;let i=e.sourceControl??this;this._parent&&!e.onlySelf&&this._parent.markAsDirty(St(vt({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new Ho(!1,i))}markAsPristine(e={}){let t=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:e.emitEvent})}),this._parent&&!e.onlySelf&&this._parent._updatePristine(e,i),t&&e.emitEvent!==!1&&this._events.next(new Ho(!0,i))}markAsPending(e={}){this.status=Xs;let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new Ys(this.status,t)),this.statusChanges.emit(this.status)),this._parent&&!e.onlySelf&&this._parent.markAsPending(St(vt({},e),{sourceControl:t}))}disable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=Bo,this.errors=null,this._forEachChild(r=>{r.disable(St(vt({},e),{onlySelf:!0}))}),this._updateValue();let i=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new Dc(this.value,i)),this._events.next(new Ys(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(St(vt({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=Vo,this._forEachChild(i=>{i.enable(St(vt({},e),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent}),this._updateAncestors(St(vt({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(e,t){this._parent&&!e.onlySelf&&(this._parent.updateValueAndValidity(e),e.skipPristineCheck||this._parent._updatePristine({},t),this._parent._updateTouched({},t))}setParent(e){this._parent=e}getRawValue(){return this.value}updateValueAndValidity(e={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Vo||this.status===Xs)&&this._runAsyncValidator(i,e.emitEvent)}let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new Dc(this.value,t)),this._events.next(new Ys(this.status,t)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._parent&&!e.onlySelf&&this._parent.updateValueAndValidity(St(vt({},e),{sourceControl:t}))}_updateTreeValidity(e={emitEvent:!0}){this._forEachChild(t=>t._updateTreeValidity(e)),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Bo:Vo}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(e,t){if(this.asyncValidator){this.status=Xs,this._hasOwnPendingAsyncValidator={emitEvent:t!==!1};let i=r_(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:t,shouldHaveEmitted:e})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let e=this._hasOwnPendingAsyncValidator?.emitEvent??!1;return this._hasOwnPendingAsyncValidator=null,e}return!1}setErrors(e,t={}){this.errors=e,this._updateControlsErrors(t.emitEvent!==!1,this,t.shouldHaveEmitted)}get(e){let t=e;return t==null||(Array.isArray(t)||(t=t.split(".")),t.length===0)?null:t.reduce((i,r)=>i&&i._find(r),this)}getError(e,t){let i=t?this.get(t):this;return i&&i.errors?i.errors[e]:null}hasError(e,t){return!!this.getError(e,t)}get root(){let e=this;for(;e._parent;)e=e._parent;return e}_updateControlsErrors(e,t,i){this.status=this._calculateStatus(),e&&this.statusChanges.emit(this.status),(e||i)&&this._events.next(new Ys(this.status,t)),this._parent&&this._parent._updateControlsErrors(e,t,i)}_initObservables(){this.valueChanges=new rn,this.statusChanges=new rn}_calculateStatus(){return this._allControlsDisabled()?Bo:this.errors?Sc:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Xs)?Xs:this._anyControlsHaveStatus(Sc)?Sc:Vo}_anyControlsHaveStatus(e){return this._anyControls(t=>t.status===e)}_anyControlsDirty(){return this._anyControls(e=>e.dirty)}_anyControlsTouched(){return this._anyControls(e=>e.touched)}_updatePristine(e,t){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,this._parent&&!e.onlySelf&&this._parent._updatePristine(e,t),r&&this._events.next(new Ho(this.pristine,t))}_updateTouched(e={},t){this.touched=this._anyControlsTouched(),this._events.next(new zo(this.touched,t)),this._parent&&!e.onlySelf&&this._parent._updateTouched(e,t)}_registerOnCollectionChange(e){this._onCollectionChange=e}_setUpdateStrategy(e){Ac(e)&&e.updateOn!=null&&(this._updateOn=e.updateOn)}_parentMarkedDirty(e){let t=this._parent&&this._parent.dirty;return!e&&!!t&&!this._parent._anyControlsDirty()}_find(e){return null}_assignValidators(e){this._rawValidators=Array.isArray(e)?e.slice():e,this._composedValidatorFn=$I(this._rawValidators)}_assignAsyncValidators(e){this._rawAsyncValidators=Array.isArray(e)?e.slice():e,this._composedAsyncValidatorFn=XI(this._rawAsyncValidators)}};var d_=new ze("CallSetDisabledState",{providedIn:"root",factory:()=>hp}),hp="always";function YI(n,e){return[...e.path,n]}function ZI(n,e,t=hp){JI(n,e),e.valueAccessor.writeValue(n.value),(n.disabled||t==="always")&&e.valueAccessor.setDisabledState?.(n.disabled),QI(n,e),tR(n,e),eR(n,e),KI(n,e)}function Zy(n,e){n.forEach(t=>{t.registerOnValidatorChange&&t.registerOnValidatorChange(e)})}function KI(n,e){if(e.valueAccessor.setDisabledState){let t=i=>{e.valueAccessor.setDisabledState(i)};n.registerOnDisabledChange(t),e._registerOnDestroy(()=>{n._unregisterOnDisabledChange(t)})}}function JI(n,e){let t=zI(n);e.validator!==null?n.setValidators(qy(t,e.validator)):typeof t=="function"&&n.setValidators([t]);let i=GI(n);e.asyncValidator!==null?n.setAsyncValidators(qy(i,e.asyncValidator)):typeof i=="function"&&n.setAsyncValidators([i]);let r=()=>n.updateValueAndValidity();Zy(e._rawValidators,r),Zy(e._rawAsyncValidators,r)}function QI(n,e){e.valueAccessor.registerOnChange(t=>{n._pendingValue=t,n._pendingChange=!0,n._pendingDirty=!0,n.updateOn==="change"&&h_(n,e)})}function eR(n,e){e.valueAccessor.registerOnTouched(()=>{n._pendingTouched=!0,n.updateOn==="blur"&&n._pendingChange&&h_(n,e),n.updateOn!=="submit"&&n.markAsTouched()})}function h_(n,e){n._pendingDirty&&n.markAsDirty(),n.setValue(n._pendingValue,{emitModelToViewChange:!1}),e.viewToModelUpdate(n._pendingValue),n._pendingChange=!1}function tR(n,e){let t=(i,r)=>{e.valueAccessor.writeValue(i),r&&e.viewToModelUpdate(i)};n.registerOnChange(t),e._registerOnDestroy(()=>{n._unregisterOnChange(t)})}function nR(n,e){if(!n.hasOwnProperty("model"))return!1;let t=n.model;return t.isFirstChange()?!0:!Object.is(e,t.currentValue)}function iR(n){return Object.getPrototypeOf(n.constructor)===t_}function rR(n,e){if(!e)return null;Array.isArray(e);let t,i,r;return e.forEach(s=>{s.constructor===Tc?t=s:iR(s)?i=s:r=s}),r||i||t||null}function Ky(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function Jy(n){return typeof n=="object"&&n!==null&&Object.keys(n).length===2&&"value"in n&&"disabled"in n}var sR=class extends up{constructor(e=null,t,i){super(jI(t),qI(i,t)),this.defaultValue=null,this._onChange=[],this._pendingChange=!1,this._applyFormState(e),this._setUpdateStrategy(t),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Ac(t)&&(t.nonNullable||t.initialValueIsDefault)&&(Jy(e)?this.defaultValue=e.value:this.defaultValue=e)}setValue(e,t={}){this.value=this._pendingValue=e,this._onChange.length&&t.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,t.emitViewToModelChange!==!1)),this.updateValueAndValidity(t)}patchValue(e,t={}){this.setValue(e,t)}reset(e=this.defaultValue,t={}){this._applyFormState(e),this.markAsPristine(t),this.markAsUntouched(t),this.setValue(this.value,t),this._pendingChange=!1}_updateValue(){}_anyControls(e){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(e){this._onChange.push(e)}_unregisterOnChange(e){Ky(this._onChange,e)}registerOnDisabledChange(e){this._onDisabledChange.push(e)}_unregisterOnDisabledChange(e){Ky(this._onDisabledChange,e)}_forEachChild(e){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(e){Jy(e)?(this.value=this._pendingValue=e.value,e.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=e}};var oR={provide:Go,useExisting:Sn(()=>fp)},Qy=Promise.resolve(),fp=(()=>{class n extends Go{constructor(t,i,r,s,o,a){super(),this._changeDetectorRef=o,this.callSetDisabledState=a,this.control=new sR,this._registered=!1,this.name="",this.update=new rn,this._parent=t,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=rR(this,s)}ngOnChanges(t){if(this._checkForErrors(),!this._registered||"name"in t){if(this._registered&&(this._checkName(),this.formDirective)){let i=t.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in t&&this._updateDisabled(t),nR(t,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective&&this.formDirective.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(t){this.viewModel=t,this.update.emit(t)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){ZI(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._isStandalone()||this._checkParentType(),this._checkName()}_checkParentType(){}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(t){Qy.then(()=>{this.control.setValue(t,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(t){let i=t.isDisabled.currentValue,r=i!==0&&gl(i);Qy.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(t){return this._parent?YI(t,this._parent):[t]}static{this.\u0275fac=function(i){return new(i||n)(Et(lp,9),Et(Vr,10),Et(NI,10),Et(dp,10),Et(qd,8),Et(d_,8))}}static{this.\u0275dir=Vt({type:n,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],features:[Zn([oR]),fn,nl]})}}return n})();var aR={provide:dp,useExisting:Sn(()=>pp),multi:!0},pp=(()=>{class n extends t_{writeValue(t){let i=t??"";this.setProperty("value",i)}registerOnChange(t){this.onChange=i=>{t(i==""?null:parseFloat(i))}}static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=li(n)))(r||n)}})()}static{this.\u0275dir=Vt({type:n,selectors:[["input","type","number","formControlName",""],["input","type","number","formControl",""],["input","type","number","ngModel",""]],hostBindings:function(i,r){i&1&&He("input",function(o){return r.onChange(o.target.value)})("blur",function(){return r.onTouched()})},features:[Zn([aR]),fn]})}}return n})();function f_(n){return typeof n=="number"?n:parseInt(n,10)}function p_(n){return typeof n=="number"?n:parseFloat(n)}var Ks=(()=>{class n{constructor(){this._validator=op}ngOnChanges(t){if(this.inputName in t){let i=this.normalizeInput(t[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):op,this._onChange&&this._onChange()}}validate(t){return this._validator(t)}registerOnValidatorChange(t){this._onChange=t}enabled(t){return t!=null}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275dir=Vt({type:n,features:[nl]})}}return n})(),lR={provide:Vr,useExisting:Sn(()=>mp),multi:!0},mp=(()=>{class n extends Ks{constructor(){super(...arguments),this.inputName="max",this.normalizeInput=t=>p_(t),this.createValidator=t=>FI(t)}static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=li(n)))(r||n)}})()}static{this.\u0275dir=Vt({type:n,selectors:[["input","type","number","max","","formControlName",""],["input","type","number","max","","formControl",""],["input","type","number","max","","ngModel",""]],hostVars:1,hostBindings:function(i,r){i&2&&pn("max",r._enabled?r.max:null)},inputs:{max:"max"},features:[Zn([lR]),fn]})}}return n})(),cR={provide:Vr,useExisting:Sn(()=>gp),multi:!0},gp=(()=>{class n extends Ks{constructor(){super(...arguments),this.inputName="min",this.normalizeInput=t=>p_(t),this.createValidator=t=>PI(t)}static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=li(n)))(r||n)}})()}static{this.\u0275dir=Vt({type:n,selectors:[["input","type","number","min","","formControlName",""],["input","type","number","min","","formControl",""],["input","type","number","min","","ngModel",""]],hostVars:1,hostBindings:function(i,r){i&2&&pn("min",r._enabled?r.min:null)},inputs:{min:"min"},features:[Zn([cR]),fn]})}}return n})(),uR={provide:Vr,useExisting:Sn(()=>vp),multi:!0};var vp=(()=>{class n extends Ks{constructor(){super(...arguments),this.inputName="required",this.normalizeInput=gl,this.createValidator=t=>OI}enabled(t){return t}static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=li(n)))(r||n)}})()}static{this.\u0275dir=Vt({type:n,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&pn("required",r._enabled?"":null)},inputs:{required:"required"},features:[Zn([uR]),fn]})}}return n})();var dR={provide:Vr,useExisting:Sn(()=>yp),multi:!0},yp=(()=>{class n extends Ks{constructor(){super(...arguments),this.inputName="minlength",this.normalizeInput=t=>f_(t),this.createValidator=t=>LI(t)}static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=li(n)))(r||n)}})()}static{this.\u0275dir=Vt({type:n,selectors:[["","minlength","","formControlName",""],["","minlength","","formControl",""],["","minlength","","ngModel",""]],hostVars:1,hostBindings:function(i,r){i&2&&pn("minlength",r._enabled?r.minlength:null)},inputs:{minlength:"minlength"},features:[Zn([dR]),fn]})}}return n})(),hR={provide:Vr,useExisting:Sn(()=>_p),multi:!0},_p=(()=>{class n extends Ks{constructor(){super(...arguments),this.inputName="maxlength",this.normalizeInput=t=>f_(t),this.createValidator=t=>kI(t)}static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=li(n)))(r||n)}})()}static{this.\u0275dir=Vt({type:n,selectors:[["","maxlength","","formControlName",""],["","maxlength","","formControl",""],["","maxlength","","ngModel",""]],hostVars:1,hostBindings:function(i,r){i&2&&pn("maxlength",r._enabled?r.maxlength:null)},inputs:{maxlength:"maxlength"},features:[Zn([hR]),fn]})}}return n})(),fR={provide:Vr,useExisting:Sn(()=>xp),multi:!0},xp=(()=>{class n extends Ks{constructor(){super(...arguments),this.inputName="pattern",this.normalizeInput=t=>t,this.createValidator=t=>UI(t)}static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=li(n)))(r||n)}})()}static{this.\u0275dir=Vt({type:n,selectors:[["","pattern","","formControlName",""],["","pattern","","formControl",""],["","pattern","","ngModel",""]],hostVars:1,hostBindings:function(i,r){i&2&&pn("pattern",r._enabled?r.pattern:null)},inputs:{pattern:"pattern"},features:[Zn([fR]),fn]})}}return n})();var pR=(()=>{class n{static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275mod=Ja({type:n})}static{this.\u0275inj=Za({})}}return n})();var m_=(()=>{class n{static withConfig(t){return{ngModule:n,providers:[{provide:d_,useValue:t.callSetDisabledState??hp}]}}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275mod=Ja({type:n})}static{this.\u0275inj=Za({imports:[pR]})}}return n})();var Ic=class n{constructor(e){this.el=e;this.delay=0}ngOnInit(){let e=this.el.nativeElement;e.classList.add("rv"),e.style.transitionDelay=this.delay+"ms",this.io=new IntersectionObserver(([t])=>{t.isIntersecting&&(e.classList.add("in"),this.io?.disconnect())},{threshold:.15}),this.io.observe(e)}ngOnDestroy(){this.io?.disconnect()}static{this.\u0275fac=function(t){return new(t||n)(Et(Cn))}}static{this.\u0275dir=Vt({type:n,selectors:[["","reveal",""]],inputs:{delay:"delay"},standalone:!0})}},Rc=class n{constructor(e,t){this.el=e;this.zone=t;this.speed=.1;this.fn=()=>{let e=this.el.nativeElement,t=e.getBoundingClientRect();e.style.translate=`0 ${(t.top+t.height/2-innerHeight/2)*this.speed}px`}}ngOnInit(){matchMedia("(prefers-reduced-motion: reduce)").matches||(this.zone.runOutsideAngular(()=>addEventListener("scroll",this.fn,{passive:!0})),this.fn())}ngOnDestroy(){removeEventListener("scroll",this.fn)}static{this.\u0275fac=function(t){return new(t||n)(Et(Cn),Et(wt))}}static{this.\u0275dir=Vt({type:n,selectors:[["","parallax",""]],inputs:{speed:[0,"parallax","speed"]},standalone:!0})}},Js=class n{constructor(e){this.el=e}move(e){let t=this.el.nativeElement,i=t.getBoundingClientRect(),r=(e.clientX-i.left)/i.width-.5,s=(e.clientY-i.top)/i.height-.5;t.style.transform=`perspective(800px) rotateX(${-s*14}deg) rotateY(${r*14}deg) scale(1.03)`}leave(){this.el.nativeElement.style.transform=""}static{this.\u0275fac=function(t){return new(t||n)(Et(Cn))}}static{this.\u0275dir=Vt({type:n,selectors:[["","tilt",""]],hostAttrs:[1,"tilt"],hostBindings:function(t,i){t&1&&He("mousemove",function(s){return i.move(s)})("mouseleave",function(){return i.leave()})},standalone:!0})}};var gR=(n,e)=>e.l,g_=(n,e)=>e.id;function vR(n,e){if(n&1){let t=Jt();T(0,"button",12),He("click",function(){let r=Ge(t).$implicit,s=Se(2);return We(s.selectService(r.l))}),T(1,"span",13),I(2),C(),I(3),C()}if(n&2){let t=e.$implicit,i=Se(2);Oi("on",i.service()===t.l),pn("aria-pressed",i.service()===t.l),ee(2),xt(t.i),ee(),xt(t.l)}}function yR(n,e){if(n&1&&(T(0,"h2"),I(1,"Welche Art von Umzug planen Sie?"),C(),T(2,"p"),I(3,"W\xE4hlen Sie die passende Leistung f\xFCr Ihre Anfrage."),C(),T(4,"div",10),mn(5,vR,4,5,"button",11,gR),C()),n&2){let t=Se();ee(5),gn(t.services)}}function _R(n,e){n&1&&(T(0,"small",18),I(1,"Bitte geben Sie eine f\xFCnfstellige deutsche Postleitzahl ein."),C())}function xR(n,e){n&1&&(T(0,"small",18),I(1,"Bitte geben Sie einen Ort ein."),C())}function MR(n,e){n&1&&(T(0,"small",18),I(1,"Bitte geben Sie Stra\xDFe und Hausnummer ein."),C())}function ER(n,e){if(n&1){let t=Jt();T(0,"button",28),He("click",function(){let r=Ge(t).$implicit,s=Se(3);return We(s.selectAddress("origin",r))}),T(1,"strong"),I(2),C(),T(3,"span"),I(4),C()()}if(n&2){let t=e.$implicit;ee(2),xt(t.street),ee(2),xt(t.postalCity)}}function bR(n,e){if(n&1&&(T(0,"div",24),mn(1,ER,5,2,"button",27,g_),C()),n&2){let t=Se(2);ee(),gn(t.originSuggestions())}}function SR(n,e){if(n&1){let t=Jt();T(0,"h2"),I(1,"Wo startet Ihr Umzug?"),C(),T(2,"div",14)(3,"div",15)(4,"label",16),I(5,"Postleitzahl"),C(),T(6,"input",17,0),He("ngModelChange",function(r){Ge(t);let s=Se();return s.originZip=r,We(s.saveDraft())}),C(),Ct(8,_R,2,0,"small",18),C(),T(9,"div",15)(10,"label",19),I(11,"Ort"),C(),T(12,"input",20,1),He("ngModelChange",function(r){Ge(t);let s=Se();return s.originCity=r,We(s.saveDraft())}),C(),Ct(14,xR,2,0,"small",18),C(),T(15,"div",21)(16,"label",22),I(17,"Stra\xDFe und Hausnummer"),C(),T(18,"input",23,2),He("ngModelChange",function(r){Ge(t);let s=Se();return s.originAddress=r,s.searchAddress("origin",r),We(s.saveDraft())}),C(),Ct(20,MR,2,0,"small",18)(21,bR,3,0,"div",24),T(22,"small",25),I(23,"Adressvorschl\xE4ge \xFCber Photon / "),T(24,"a",26),I(25,"\xA9 OpenStreetMap"),C(),I(26,". Manuelle Eingabe ist ebenfalls m\xF6glich."),C()()()}if(n&2){let t=gr(7),i=gr(13),r=gr(19),s=Se();ee(6),pt("ngModel",s.originZip),ee(2),_t(t.invalid&&(t.touched||t.dirty)?8:-1),ee(4),pt("ngModel",s.originCity),ee(2),_t(i.invalid&&(i.touched||i.dirty)?14:-1),ee(4),pt("ngModel",s.originAddress),pn("aria-expanded",s.originSuggestions().length>0),ee(2),_t(r.invalid&&(r.touched||r.dirty)?20:-1),ee(),_t(s.originSuggestions().length?21:-1)}}function wR(n,e){n&1&&(T(0,"small",18),I(1,"Bitte geben Sie eine f\xFCnfstellige deutsche Postleitzahl ein."),C())}function CR(n,e){n&1&&(T(0,"small",18),I(1,"Bitte geben Sie einen Ort ein."),C())}function DR(n,e){n&1&&(T(0,"small",18),I(1,"Bitte geben Sie Stra\xDFe und Hausnummer ein."),C())}function TR(n,e){if(n&1){let t=Jt();T(0,"button",28),He("click",function(){let r=Ge(t).$implicit,s=Se(3);return We(s.selectAddress("destination",r))}),T(1,"strong"),I(2),C(),T(3,"span"),I(4),C()()}if(n&2){let t=e.$implicit;ee(2),xt(t.street),ee(2),xt(t.postalCity)}}function AR(n,e){if(n&1&&(T(0,"div",35),mn(1,TR,5,2,"button",27,g_),C()),n&2){let t=Se(2);ee(),gn(t.destinationSuggestions())}}function IR(n,e){if(n&1){let t=Jt();T(0,"h2"),I(1,"Wohin geht Ihr Umzug?"),C(),T(2,"div",14)(3,"div",15)(4,"label",29),I(5,"Postleitzahl"),C(),T(6,"input",30,3),He("ngModelChange",function(r){Ge(t);let s=Se();return s.destZip=r,We(s.saveDraft())}),C(),Ct(8,wR,2,0,"small",18),C(),T(9,"div",15)(10,"label",31),I(11,"Ort"),C(),T(12,"input",32,4),He("ngModelChange",function(r){Ge(t);let s=Se();return s.destCity=r,We(s.saveDraft())}),C(),Ct(14,CR,2,0,"small",18),C(),T(15,"div",21)(16,"label",33),I(17,"Stra\xDFe und Hausnummer"),C(),T(18,"input",34,5),He("ngModelChange",function(r){Ge(t);let s=Se();return s.destAddress=r,s.searchAddress("destination",r),We(s.saveDraft())}),C(),Ct(20,DR,2,0,"small",18)(21,AR,3,0,"div",35),T(22,"small",25),I(23,"Adressvorschl\xE4ge \xFCber Photon / "),T(24,"a",26),I(25,"\xA9 OpenStreetMap"),C(),I(26,". Manuelle Eingabe ist ebenfalls m\xF6glich."),C()()()}if(n&2){let t=gr(7),i=gr(13),r=gr(19),s=Se();ee(6),pt("ngModel",s.destZip),ee(2),_t(t.invalid&&(t.touched||t.dirty)?8:-1),ee(4),pt("ngModel",s.destCity),ee(2),_t(i.invalid&&(i.touched||i.dirty)?14:-1),ee(4),pt("ngModel",s.destAddress),pn("aria-expanded",s.destinationSuggestions().length>0),ee(2),_t(r.invalid&&(r.touched||r.dirty)?20:-1),ee(),_t(s.destinationSuggestions().length?21:-1)}}function RR(n,e){n&1&&(T(0,"small",18),I(1,"Bitte geben Sie einen Wert zwischen 1 und 1000 m\xB2 ein."),C())}function NR(n,e){n&1&&(T(0,"small",18),I(1,"Bitte geben Sie 1 bis 50 Zimmer an."),C())}function PR(n,e){n&1&&(T(0,"small",18),I(1,"Bitte geben Sie eine Etage zwischen Keller -3 und 100 ein."),C())}function FR(n,e){n&1&&(T(0,"small",18),I(1,"Bitte geben Sie eine Etage zwischen Keller -3 und 100 ein."),C())}function OR(n,e){if(n&1){let t=Jt();T(0,"h2"),I(1,"Wie gro\xDF ist Ihr Umzug?"),C(),T(2,"p"),I(3,"Ungef\xE4hre Angaben helfen uns bei der Planung. Alle Felder sind optional."),C(),T(4,"div",36)(5,"div",15)(6,"label",37),I(7,"Wohnfl\xE4che (m\xB2)"),C(),T(8,"input",38),ki("ngModelChange",function(r){Ge(t);let s=Se();return vr(s.areaSqm,r)||(s.areaSqm=r),We(r)}),He("ngModelChange",function(){Ge(t);let r=Se();return We(r.saveDraft())}),C(),Ct(9,RR,2,0,"small",18),C(),T(10,"div",15)(11,"label",39),I(12,"Anzahl Zimmer"),C(),T(13,"input",40),ki("ngModelChange",function(r){Ge(t);let s=Se();return vr(s.roomCount,r)||(s.roomCount=r),We(r)}),He("ngModelChange",function(){Ge(t);let r=Se();return We(r.saveDraft())}),C(),Ct(14,NR,2,0,"small",18),C(),T(15,"div",15)(16,"label",41),I(17,"Etage am Startort"),C(),T(18,"input",42),ki("ngModelChange",function(r){Ge(t);let s=Se();return vr(s.originFloor,r)||(s.originFloor=r),We(r)}),He("ngModelChange",function(){Ge(t);let r=Se();return We(r.saveDraft())}),C(),Ct(19,PR,2,0,"small",18),C(),T(20,"div",15)(21,"label",43),I(22,"Etage am Zielort"),C(),T(23,"input",44),ki("ngModelChange",function(r){Ge(t);let s=Se();return vr(s.destinationFloor,r)||(s.destinationFloor=r),We(r)}),He("ngModelChange",function(){Ge(t);let r=Se();return We(r.saveDraft())}),C(),Ct(24,FR,2,0,"small",18),C()(),T(25,"small",25),I(26,"0 = Erdgeschoss, negative Werte = Keller."),C()}if(n&2){let t=Se();ee(8),Li("ngModel",t.areaSqm),ee(),_t(t.areaSqm!==null&&!t.validOptionalInteger(t.areaSqm,1,1e3)?9:-1),ee(4),Li("ngModel",t.roomCount),ee(),_t(t.roomCount!==null&&!t.validOptionalInteger(t.roomCount,1,50)?14:-1),ee(4),Li("ngModel",t.originFloor),ee(),_t(t.originFloor!==null&&!t.validOptionalInteger(t.originFloor,-3,100)?19:-1),ee(4),Li("ngModel",t.destinationFloor),ee(),_t(t.destinationFloor!==null&&!t.validOptionalInteger(t.destinationFloor,-3,100)?24:-1)}}function LR(n,e){n&1&&(T(0,"small",18),I(1,"Der Wunschtermin darf nicht in der Vergangenheit liegen."),C())}function kR(n,e){n&1&&(T(0,"small",18),I(1,"Alternativtermine m\xFCssen in der Zukunft liegen und sich vom Wunschtermin unterscheiden."),C())}function UR(n,e){if(n&1){let t=Jt();T(0,"div",50)(1,"div",15)(2,"label",51),I(3),C(),T(4,"input",52),He("ngModelChange",function(r){let s=Ge(t).$index,o=Se(2);return We(o.updateAlternateDate(s,r))}),C()(),T(5,"button",53),He("click",function(){let r=Ge(t).$index,s=Se(2);return We(s.removeAlternateDate(r))}),I(6,"Entfernen"),C()(),Ct(7,kR,2,0,"small",18)}if(n&2){let t=e.$implicit,i=e.$index,r=Se(2);ee(2),pt("for","alternate-date-"+i),ee(),ui("Alternative ",i+1,""),ee(),pt("id","alternate-date-"+i)("min",r.today)("ngModel",t),ee(),pn("aria-label","Alternativtermin "+(i+1)+" entfernen"),ee(2),_t(t&&(t<r.today||t===r.date)?7:-1)}}function VR(n,e){if(n&1){let t=Jt();T(0,"button",54),He("click",function(){Ge(t);let r=Se(2);return We(r.addAlternateDate())}),I(1,"\uFF0B Alternativtermin hinzuf\xFCgen"),C()}}function BR(n,e){if(n&1){let t=Jt();T(0,"h2"),I(1,"Wann m\xF6chten Sie umziehen?"),C(),T(2,"div",15)(3,"label",45),I(4,"Wunschtermin"),C(),T(5,"input",46),ki("ngModelChange",function(r){Ge(t);let s=Se();return vr(s.date,r)||(s.date=r),We(r)}),He("ngModelChange",function(){Ge(t);let r=Se();return We(r.saveDraft())}),C(),Ct(6,LR,2,0,"small",18),C(),T(7,"div",47)(8,"div",48)(9,"h3"),I(10,"Alternative Termine"),C(),T(11,"span"),I(12,"Optional, maximal 3"),C()(),mn(13,UR,8,7,null,null,t0),Ct(15,VR,2,0,"button",49),C()}if(n&2){let t=Se();ee(5),pt("min",t.today),Li("ngModel",t.date),ee(),_t(t.date&&t.date<t.today?6:-1),ee(7),gn(t.alternateDates()),ee(2),_t(t.alternateDates().length<3?15:-1)}}function HR(n,e){n&1&&(T(0,"small",18),I(1,"Bitte pr\xFCfen Sie Ihren Vornamen."),C())}function zR(n,e){n&1&&(T(0,"small",18),I(1,"Bitte pr\xFCfen Sie Ihren Nachnamen."),C())}function GR(n,e){n&1&&(T(0,"small",18),I(1,"Bitte geben Sie eine g\xFCltige Telefonnummer mit 7 bis 15 Ziffern ein."),C())}function WR(n,e){n&1&&(T(0,"small",18),I(1,"Bitte geben Sie eine g\xFCltige E-Mail-Adresse ein."),C())}function jR(n,e){if(n&1){let t=Jt();T(0,"h2"),I(1,"Ihre Kontaktdaten"),C(),T(2,"div",55)(3,"div",15)(4,"label",56),I(5,"Vorname"),C(),T(6,"input",57),He("ngModelChange",function(r){Ge(t);let s=Se();return s.name=r,We(s.saveDraft())}),C(),Ct(7,HR,2,0,"small",18),C(),T(8,"div",15)(9,"label",58),I(10,"Nachname"),C(),T(11,"input",59),He("ngModelChange",function(r){Ge(t);let s=Se();return s.surname=r,We(s.saveDraft())}),C(),Ct(12,zR,2,0,"small",18),C(),T(13,"div",15)(14,"label",60),I(15,"Telefon"),C(),T(16,"input",61),He("ngModelChange",function(r){Ge(t);let s=Se();return s.phone=r,We(s.saveDraft())}),C(),T(17,"small",25),I(18,"Zum Beispiel 015780945403 oder +49 157 80945403"),C(),Ct(19,GR,2,0,"small",18),C(),T(20,"div",15)(21,"label",62),I(22,"E-Mail"),C(),T(23,"input",63),He("ngModelChange",function(r){Ge(t);let s=Se();return s.email=r,We(s.saveDraft())}),C(),Ct(24,WR,2,0,"small",18),C()(),T(25,"div",64)(26,"label"),I(27,"Dieses Feld bitte leer lassen"),T(28,"input",65),ki("ngModelChange",function(r){Ge(t);let s=Se();return vr(s.website,r)||(s.website=r),We(r)}),C()()(),T(29,"p",66),I(30,"Ihr Entwurf bleibt vor\xFCbergehend in diesem Browser-Tab gespeichert und wird nach erfolgreichem Versand gel\xF6scht. Wir verwenden Ihre Angaben zur Bearbeitung Ihrer Umzugsanfrage. Details finden Sie in unserer "),T(31,"a",67),I(32,"Datenschutzerkl\xE4rung"),C(),I(33,"."),C()}if(n&2){let t=Se();ee(6),pt("ngModel",t.name),ee(),_t(t.name&&!t.validName(t.name)?7:-1),ee(4),pt("ngModel",t.surname),ee(),_t(t.surname&&!t.validName(t.surname)?12:-1),ee(4),pt("ngModel",t.phone),ee(3),_t(t.phone&&!t.validPhone()?19:-1),ee(4),pt("ngModel",t.email),ee(),_t(t.email&&!t.validEmail()?24:-1),ee(4),Li("ngModel",t.website)}}function $R(n,e){if(n&1&&(T(0,"h2"),I(1),C(),T(2,"p"),I(3,"Ihre Umzugsanfrage wurde erfolgreich an KAFI Transporte \xFCbermittelt. Wir melden uns pers\xF6nlich bei Ihnen."),C(),T(4,"p",68)(5,"strong"),I(6),C(),Kt(7,"br"),I(8),Kt(9,"br"),I(10),Kt(11,"br"),I(12),Kt(13,"br"),I(14),Kt(15,"br"),I(16),C(),T(17,"div",9)(18,"a",69),I(19,"\u260E Anrufen"),C(),T(20,"a",70),I(21,"WhatsApp"),C()()),n&2){let t,i=Se();ee(),ui("Vielen Dank, ",i.name,"!"),ee(5),xt(i.service()),ee(2),_o("",i.originCity," \u2192 ",i.destCity,""),ee(2),_o("",i.areaSqm?i.areaSqm+" m\xB2":"Wohnfl\xE4che offen"," \xB7 ",i.roomCount?i.roomCount+" Zimmer":"Zimmer offen",""),ee(2),_o("Etage: ",(t=i.originFloor)!==null&&t!==void 0?t:"offen"," \u2192 ",(t=i.destinationFloor)!==null&&t!==void 0?t:"offen",""),ee(2),ui("Wunschtermin: ",i.date,""),ee(2),ui("Alternativen: ",i.alternateDates().join(", ")||"Keine","")}}function qR(n,e){if(n&1&&(T(0,"p",8),I(1),C()),n&2){let t=Se();ee(),xt(t.submitError())}}function XR(n,e){if(n&1){let t=Jt();T(0,"div",9)(1,"button",71),He("click",function(){Ge(t);let r=Se();return We(r.goBack())}),I(2,"Zur\xFCck"),C(),T(3,"button",72),He("click",function(){Ge(t);let r=Se();return We(r.advance())}),I(4),C()()}if(n&2){let t=Se();ee(),pt("disabled",t.step()===0||t.sending()),ee(2),pt("disabled",!t.ok()||t.sending()),ee(),ui(" ",t.sending()?"Wird gesendet \u2026":t.step()===5?"Kostenloses Angebot anfragen":"Weiter"," ")}}var Nc=class n{constructor(){this.draftKey="kafi-inquiry-draft-v1";this.services=[{i:"\u{1F3E0}",l:"Privatumzug"},{i:"\u{1F3E2}",l:"Firmenumzug"},{i:"\u{1F5FA}\uFE0F",l:"Fernumzug"},{i:"\u{1F6CB}\uFE0F",l:"Einzelner M\xF6beltransport"},{i:"\u{1F4E6}",l:"Sonstiges"}];this.step=$t(0);this.service=$t("");this.sending=$t(!1);this.submitError=$t("");this.originSuggestions=$t([]);this.destinationSuggestions=$t([]);this.alternateDates=$t([]);this.originZip="";this.originCity="";this.originAddress="";this.destZip="";this.destCity="";this.destAddress="";this.areaSqm=null;this.roomCount=null;this.originFloor=null;this.destinationFloor=null;this.date="";this.name="";this.surname="";this.phone="";this.email="";this.website="";this.Math=Math;this.today=this.getToday();this.restoreDraft()}getToday(){let e=new Date;return e.setMinutes(e.getMinutes()-e.getTimezoneOffset()),e.toISOString().slice(0,10)}restoreDraft(){try{let e=sessionStorage.getItem(this.draftKey);if(!e)return;let t=JSON.parse(e);if(t.version!==1)return;this.step.set(Number.isInteger(t.step)?Math.max(0,Math.min(5,t.step)):0),this.service.set(typeof t.service=="string"?t.service:""),this.originZip=typeof t.originZip=="string"?t.originZip:"",this.originCity=typeof t.originCity=="string"?t.originCity:"",this.originAddress=typeof t.originAddress=="string"?t.originAddress:"",this.destZip=typeof t.destZip=="string"?t.destZip:"",this.destCity=typeof t.destCity=="string"?t.destCity:"",this.destAddress=typeof t.destAddress=="string"?t.destAddress:"",this.areaSqm=typeof t.areaSqm=="number"?t.areaSqm:null,this.roomCount=typeof t.roomCount=="number"?t.roomCount:null,this.originFloor=typeof t.originFloor=="number"?t.originFloor:null,this.destinationFloor=typeof t.destinationFloor=="number"?t.destinationFloor:null,this.date=typeof t.date=="string"?t.date:"",this.alternateDates.set(Array.isArray(t.alternateDates)?t.alternateDates.filter(i=>typeof i=="string").slice(0,3):[]),this.name=typeof t.name=="string"?t.name:"",this.surname=typeof t.surname=="string"?t.surname:"",this.phone=typeof t.phone=="string"?t.phone:"",this.email=typeof t.email=="string"?t.email:""}catch{this.clearDraft()}}saveDraft(){let e={version:1,step:this.step(),service:this.service(),originZip:this.originZip,originCity:this.originCity,originAddress:this.originAddress,destZip:this.destZip,destCity:this.destCity,destAddress:this.destAddress,areaSqm:this.areaSqm,roomCount:this.roomCount,originFloor:this.originFloor,destinationFloor:this.destinationFloor,date:this.date,alternateDates:this.alternateDates(),name:this.name,surname:this.surname,phone:this.phone,email:this.email};try{sessionStorage.setItem(this.draftKey,JSON.stringify(e))}catch{}}clearDraft(){try{sessionStorage.removeItem(this.draftKey)}catch{}}selectService(e){this.service.set(e),this.saveDraft()}goBack(){this.step()===0||this.sending()||(this.submitError.set(""),this.step.update(e=>e-1),this.saveDraft())}addAlternateDate(){this.alternateDates().length>=3||(this.alternateDates.update(e=>[...e,""]),this.saveDraft())}updateAlternateDate(e,t){this.alternateDates.update(i=>i.map((r,s)=>s===e?t:r)),this.saveDraft()}removeAlternateDate(e){this.alternateDates.update(t=>t.filter((i,r)=>r!==e)),this.saveDraft()}searchAddress(e,t){let i=e==="origin"?this.originSearchTimer:this.destinationSearchTimer,r=e==="origin"?this.originSearchController:this.destinationSearchController;i&&clearTimeout(i),r?.abort();let s=e==="origin"?this.originSuggestions:this.destinationSuggestions,o=t.trim();if(o.length<3){s.set([]);return}let a=setTimeout(()=>tr(this,null,function*(){let l=new AbortController;e==="origin"?this.originSearchController=l:this.destinationSearchController=l;let c=new URLSearchParams({q:o,limit:"5",lang:"de",bbox:"5.8,47.2,15.1,55.1"});try{let u=yield fetch(`https://photon.komoot.io/api/?${c}`,{signal:l.signal});if(!u.ok)throw new Error("Address lookup failed");let h=((yield u.json()).features||[]).flatMap((f,g)=>{let _=f.properties||{},m=String(_.countrycode||"").toLowerCase();if(m&&m!=="de")return[];let p=[String(_.street||_.name||""),String(_.housenumber||"")].filter(Boolean).join(" "),w=String(_.postcode||""),E=String(_.city||_.locality||_.district||"");return!p||!w||!E?[]:[{id:`${f.geometry?.coordinates?.join(",")||p}-${g}`,street:p,postcode:w,city:E,postalCity:[w,E].filter(Boolean).join(" ")}]});l.signal.aborted||s.set(h)}catch{l.signal.aborted||s.set([])}}),350);e==="origin"?this.originSearchTimer=a:this.destinationSearchTimer=a}selectAddress(e,t){e==="origin"?(this.originAddress=t.street,this.originZip=t.postcode,this.originCity=t.city,this.originSuggestions.set([])):(this.destAddress=t.street,this.destZip=t.postcode,this.destCity=t.city,this.destinationSuggestions.set([])),this.saveDraft()}validZip(e){return/^\d{5}$/.test(e.trim())}validMoveDate(e){return/^\d{4}-\d{2}-\d{2}$/.test(e)&&e>=this.today}validDates(){let e=this.alternateDates();return this.validMoveDate(this.date)&&e.length<=3&&e.every(t=>this.validMoveDate(t)&&t!==this.date)&&new Set(e).size===e.length}validName(e){return/^[\p{L}\p{M}][\p{L}\p{M}'’ .-]{1,79}$/u.test(e.trim())}validOptionalInteger(e,t,i){return e===null||Number.isInteger(e)&&e>=t&&e<=i}validEmail(){return/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(this.email.trim())}advance(){if(!(this.sending()||!this.ok())){if(this.step()===5){this.submit();return}this.submitError.set(""),this.step.update(e=>e+1),this.saveDraft()}}submit(){return tr(this,null,function*(){let e=window.KAFI_CONFIG?.leadsApiUrl?.trim();if(!e){this.submitError.set("Das Anfrageformular ist noch nicht mit dem E-Mail-Dienst verbunden. Bitte kontaktieren Sie uns telefonisch oder per WhatsApp.");return}this.sending.set(!0),this.submitError.set("");try{if(!(yield fetch(e,{method:"POST",headers:{"Content-Type":"application/json"},cache:"no-store",body:JSON.stringify({service:this.service(),origin:{zip:this.originZip,city:this.originCity,address:this.originAddress},destination:{zip:this.destZip,city:this.destCity,address:this.destAddress},moveDetails:{areaSqm:this.areaSqm,roomCount:this.roomCount,originFloor:this.originFloor,destinationFloor:this.destinationFloor},date:this.date,alternateDate:this.alternateDates().filter(Boolean).join(", "),alternateDates:this.alternateDates().filter(Boolean),contact:{firstName:this.name,lastName:this.surname,phone:this.phone,email:this.email},website:this.website})})).ok)throw new Error("Request was rejected");this.clearDraft(),this.step.set(5)}catch{this.submitError.set("Ihre Anfrage konnte gerade nicht gesendet werden. Bitte versuchen Sie es erneut oder kontaktieren Sie uns telefonisch unter +49 178 7410656.")}finally{this.sending.set(!1)}})}ok(){switch(this.step()){case 0:return!!this.service();case 1:return this.validAddress(this.originZip,this.originCity,this.originAddress);case 2:return this.validAddress(this.destZip,this.destCity,this.destAddress);case 3:return this.validMoveDetails();case 4:return this.validDates();case 5:return this.validName(this.name)&&this.validName(this.surname)&&this.validPhone()&&this.validEmail();default:return!0}}validMoveDetails(){return this.validOptionalInteger(this.areaSqm,1,1e3)&&this.validOptionalInteger(this.roomCount,1,50)&&this.validOptionalInteger(this.originFloor,-3,100)&&this.validOptionalInteger(this.destinationFloor,-3,100)}validAddress(e,t,i){return this.validZip(e)&&t.trim().length>=2&&i.trim().length>=3}validPhone(){let e=this.phone.trim(),t=e.replace(/\D/g,"").length;return/^\+?[\d\s()./-]+$/.test(e)&&t>=7&&t<=15}static{this.\u0275fac=function(t){return new(t||n)}}static{this.\u0275cmp=hs({type:n,selectors:[["app-estimate"]],standalone:!0,features:[ys],decls:14,vars:6,consts:[["originZipField","ngModel"],["originCityField","ngModel"],["originAddressField","ngModel"],["destZipField","ngModel"],["destCityField","ngModel"],["destAddressField","ngModel"],[1,"wizard-progress"],[1,"bar"],["role","alert",1,"form-error"],[1,"row"],[1,"opts"],["type","button","tilt","",1,"opt",3,"on"],["type","button","tilt","",1,"opt",3,"click"],[1,"ico"],[1,"field-grid"],[1,"field-group"],["for","origin-zip"],["id","origin-zip","inputmode","numeric","autocomplete","postal-code","placeholder","z. B. 12353","maxlength","5","pattern","[0-9]{5}","required","",1,"fld",3,"ngModelChange","ngModel"],[1,"field-error"],["for","origin-city"],["id","origin-city","autocomplete","address-level2","placeholder","Stadt oder Gemeinde","minlength","2","maxlength","80","required","",1,"fld",3,"ngModelChange","ngModel"],[1,"field-group","autocomplete-field"],["for","origin-address"],["id","origin-address","autocomplete","street-address","placeholder","Stra\xDFe und Hausnummer eingeben","minlength","3","maxlength","120","required","","role","combobox","aria-autocomplete","list","aria-controls","origin-address-suggestions",1,"fld",3,"ngModelChange","ngModel"],["id","origin-address-suggestions","role","listbox","aria-label","Adressvorschl\xE4ge",1,"address-suggestions"],[1,"form-hint"],["href","https://www.openstreetmap.org/copyright","target","_blank","rel","noreferrer"],["type","button","role","option",1,"address-suggestion"],["type","button","role","option",1,"address-suggestion",3,"click"],["for","destination-zip"],["id","destination-zip","inputmode","numeric","autocomplete","postal-code","placeholder","z. B. 20095","maxlength","5","pattern","[0-9]{5}","required","",1,"fld",3,"ngModelChange","ngModel"],["for","destination-city"],["id","destination-city","autocomplete","address-level2","placeholder","Stadt oder Gemeinde","minlength","2","maxlength","80","required","",1,"fld",3,"ngModelChange","ngModel"],["for","destination-address"],["id","destination-address","autocomplete","street-address","placeholder","Stra\xDFe und Hausnummer eingeben","minlength","3","maxlength","120","required","","role","combobox","aria-autocomplete","list","aria-controls","destination-address-suggestions",1,"fld",3,"ngModelChange","ngModel"],["id","destination-address-suggestions","role","listbox","aria-label","Adressvorschl\xE4ge",1,"address-suggestions"],[1,"field-grid","move-details-grid"],["for","area-sqm"],["id","area-sqm","type","number","min","1","max","1000","step","1","inputmode","numeric","placeholder","z. B. 65",1,"fld",3,"ngModelChange","ngModel"],["for","room-count"],["id","room-count","type","number","min","1","max","50","step","1","inputmode","numeric","placeholder","z. B. 3",1,"fld",3,"ngModelChange","ngModel"],["for","origin-floor"],["id","origin-floor","type","number","min","-3","max","100","step","1","inputmode","numeric","placeholder","z. B. 3",1,"fld",3,"ngModelChange","ngModel"],["for","destination-floor"],["id","destination-floor","type","number","min","-3","max","100","step","1","inputmode","numeric","placeholder","z. B. 2",1,"fld",3,"ngModelChange","ngModel"],["for","move-date"],["id","move-date","type","date","required","",1,"fld",3,"ngModelChange","min","ngModel"],[1,"alternate-dates"],[1,"alternate-heading"],["type","button",1,"add-date"],[1,"alternate-date-row"],[3,"for"],["type","date",1,"fld",3,"ngModelChange","id","min","ngModel"],["type","button",1,"remove-date",3,"click"],["type","button",1,"add-date",3,"click"],[1,"field-stack"],["for","first-name"],["id","first-name","autocomplete","given-name","placeholder","Vorname","minlength","2","maxlength","80","required","",1,"fld",3,"ngModelChange","ngModel"],["for","last-name"],["id","last-name","autocomplete","family-name","placeholder","Nachname","minlength","2","maxlength","80","required","",1,"fld",3,"ngModelChange","ngModel"],["for","contact-phone"],["id","contact-phone","type","tel","autocomplete","tel","placeholder","015780945403","maxlength","40","required","",1,"fld",3,"ngModelChange","ngModel"],["for","contact-email"],["id","contact-email","type","email","autocomplete","email","placeholder","name@beispiel.de","maxlength","254","required","",1,"fld",3,"ngModelChange","ngModel"],["aria-hidden","true",1,"form-trap"],["tabindex","-1","autocomplete","off",3,"ngModelChange","ngModel"],[1,"form-note"],["href","/datenschutz/","target","_blank","rel","noreferrer"],[1,"request-summary"],["href","tel:+491787410656",1,"btn","ghost","dark"],["href","https://wa.me/491787410656","target","_blank","rel","noreferrer",1,"btn","ghost","dark"],["type","button",1,"btn","ghost","dark",3,"click","disabled"],[1,"btn","solid",3,"click","disabled"]],template:function(t,i){if(t&1&&(T(0,"div",6)(1,"span"),I(2),C(),T(3,"div",7),Kt(4,"i"),C()(),Ct(5,yR,7,0)(6,SR,27,8)(7,IR,27,8)(8,OR,27,8)(9,BR,16,4)(10,jR,34,9)(11,$R,22,10)(12,qR,2,1,"p",8)(13,XR,5,3,"div",9)),t&2){let r;ee(2),ui("Schritt ",i.step()<6?i.step()+1:6," von 6"),ee(2),jd("width",(i.Math.min(i.step(),5)+1)*100/6,"%"),ee(),_t((r=i.step())===0?5:r===1?6:r===2?7:r===3?8:r===4?9:r===5?10:r===6?11:-1),ee(7),_t(i.submitError()?12:-1),ee(),_t(i.step()<6?13:-1)}},dependencies:[m_,Tc,pp,u_,vp,yp,_p,xp,gp,mp,fp,Js],encapsulation:2})}};var Mp=(n,e)=>e.label,v_=(n,e)=>e.t,YR=(n,e)=>e.q,ZR=(n,e)=>e.c;function KR(n,e){if(n&1&&(T(0,"a",2),I(1),C()),n&2){let t=e.$implicit;pt("href",t.href,al),ee(),xt(t.label)}}function JR(n,e){if(n&1&&(T(0,"span")(1,"strong"),I(2),C(),T(3,"small"),I(4),C()()),n&2){let t=e.$implicit;ee(2),xt(t.label),ee(2),xt(t.value)}}function QR(n,e){if(n&1&&(T(0,"article",21)(1,"span",72),I(2),C(),T(3,"h3"),I(4),C(),T(5,"p"),I(6),C()()),n&2){let t=e.$implicit,i=e.$index;pt("delay",i*80),ee(2),xt(t.i),ee(2),xt(t.t),ee(2),xt(t.d)}}function eN(n,e){if(n&1&&(T(0,"article",47)(1,"span",72),I(2),C(),T(3,"h3"),I(4),C(),T(5,"p"),I(6),C()()),n&2){let t=e.$implicit,i=e.$index;pt("delay",i*100),ee(2),xt(t.i),ee(2),xt(t.t),ee(2),xt(t.d)}}function tN(n,e){if(n&1){let t=Jt();T(0,"div",73)(1,"button",74),He("click",function(){let r=Ge(t).$index,s=Se();return We(s.open.set(s.open()===r?-1:r))}),I(2),T(3,"i"),I(4,"+"),C()(),T(5,"div")(6,"p"),I(7),C()()()}if(n&2){let t=e.$implicit,i=e.$index,r=Se();Oi("open",r.open()===i),ee(2),xt(t.q),ee(5),xt(t.a)}}function nN(n,e){if(n&1&&(T(0,"li")(1,"a",2),I(2),C()()),n&2){let t=e.$implicit;ee(),pt("href",t.href,al),ee(),xt(t.label)}}function iN(n,e){if(n&1&&(T(0,"p"),I(1),Kt(2,"br"),T(3,"a",2),I(4),C()()),n&2){let t=e.$implicit;ee(),xt(t.c),ee(2),pt("href","tel:"+t.n,al),ee(),xt(t.n)}}function rN(n,e){if(n&1){let t=Jt();T(0,"div",75),He("click",function(){Ge(t);let r=Se();return We(r.modal.set(!1))}),T(1,"div",76),He("click",function(r){return Ge(t),We(r.stopPropagation())}),T(2,"button",77),He("click",function(){Ge(t);let r=Se();return We(r.modal.set(!1))}),I(3,"\xD7"),C(),T(4,"h3"),I(5,"Jetzt anrufen"),C(),mn(6,iN,5,3,"p",null,ZR),T(8,"a",78),I(9,"WhatsApp"),C()()()}if(n&2){let t=Se();ee(6),gn(t.phones)}}var Pc=class n{constructor(){this.scrolled=$t(!1);this.modal=$t(!1);this.open=$t(-1);this.nav=[{label:"Leistungen",href:"#services"},{label:"Ablauf",href:"#steps"},{label:"Deutschlandweit",href:"#coverage"},{label:"Bewertungen",href:"#reviews"},{label:"FAQ",href:"#faqs"}];this.services=[{i:"\u{1F3E0}",t:"Privatumzug",d:"Wohnung, Haus oder WG \u2013 Unterst\xFCtzung beim Transport Ihres Hausstands."},{i:"\u{1F3E2}",t:"Firmenumzug",d:"Umz\xFCge von B\xFCros, Praxen oder Unternehmensstandorten."},{i:"\u{1F5FA}\uFE0F",t:"Fernumzug",d:"Deutschlandweite Umz\xFCge \xFCber gr\xF6\xDFere Entfernungen."},{i:"\u{1F6CB}\uFE0F",t:"M\xF6beltransport",d:"Transport einzelner M\xF6belst\xFCcke oder gr\xF6\xDFerer Ladungen."},{i:"\u{1F9F0}",t:"M\xF6belmontage",d:"Abbau und Aufbau von M\xF6beln, sofern angeboten."},{i:"\u{1F4E6}",t:"Verpackungsservice",d:"Unterst\xFCtzung bei der Vorbereitung und Verpackung, sofern angeboten."},{i:"\u{1F9F9}",t:"Entr\xFCmpelung",d:"Entr\xFCmpelungen und Haushaltsaufl\xF6sungen, sofern angeboten."},{i:"\u{1F6A7}",t:"Halteverbotszone",d:"Unterst\xFCtzung bei der Organisation einer Ladezone, sofern angeboten."}];this.trust=[{label:"Deutschlandweit",value:"f\xFCr Sie unterwegs"},{label:"MyHammer",value:"4,5 / 5"},{label:"Google",value:"4,6 / 5"},{label:"Pers\xF6nlicher Kontakt",value:"direkt mit KAFI"}];this.features=[{i:"\u{1F30D}",t:"Deutschlandweit",d:"Von Berlin in St\xE4dte und Regionen in ganz Deutschland."},{i:"\u{1F91D}",t:"Pers\xF6nlich",d:"Direkter Kontakt und individuelle Abstimmung."},{i:"\u{1F50E}",t:"Transparent",d:"Individuelle Angebote auf Basis Ihrer Umzugsdaten."},{i:"\u2699\uFE0F",t:"Flexibel",d:"Vom einzelnen M\xF6beltransport bis zum kompletten Umzug."}];this.faqs=[{q:"Wie weit f\xE4hrt KAFI Transporte?",a:"KAFI Transporte hat seinen Standort in Berlin und f\xFChrt Umz\xFCge deutschlandweit durch."},{q:"Wie fr\xFCh sollte ich meinen Umzug buchen?",a:"Das h\xE4ngt unter anderem von Route, Umfang und gew\xFCnschtem Termin ab. Eine fr\xFChzeitige Anfrage erleichtert die Planung."},{q:"Wie wird der Preis bestimmt?",a:"Der Preis wird nicht automatisch auf der Website berechnet. Nach Ihrer Anfrage pr\xFCft KAFI Transporte die Angaben und erstellt bzw. bespricht ein individuelles Angebot."},{q:"Kann ich nur einzelne M\xF6bel transportieren lassen?",a:"Ja, sofern dieser Service f\xFCr Ihre Anfrage geeignet ist, kann ein individueller Transport einzelner M\xF6bel oder Ladungen organisiert werden."},{q:"\xDCbernimmt KAFI Transporte M\xF6belmontagen?",a:"Nur wenn die Leistung f\xFCr Ihren Auftrag passend ist. Bitte teilen Sie uns Ihre Anforderungen in der Anfrage mit."},{q:"Wie bekomme ich ein Angebot?",a:"\xDCber das Anfrageformular oder telefonisch unter +49 178 7410656."}];this.phones=[{c:"Berlin",n:"+49 178 7410656"}]}onScroll(){this.scrolled.set(scrollY>40)}static{this.\u0275fac=function(t){return new(t||n)}}static{this.\u0275cmp=hs({type:n,selectors:[["app-root"]],hostBindings:function(t,i){t&1&&He("scroll",function(){return i.onScroll()},!1,fv)},standalone:!0,features:[ys],decls:266,vars:6,consts:[[1,"nav"],["href","#",1,"logo"],[3,"href"],[1,"nav-actions"],["href","#estimate",1,"btn","ghost"],["href","tel:+491787410656","aria-label","KAFI Transporte anrufen: +49 178 7410656",1,"btn","call-btn"],["aria-hidden","true"],[1,"hero"],[1,"hero-in"],[1,"eyebrow"],["reveal",""],["reveal","",3,"delay"],["reveal","",1,"row",3,"delay"],["href","#estimate",1,"btn","solid"],["href","https://wa.me/491787410656?text=Hallo%20KAFI%20Transporte%2C%20ich%20m%C3%B6chte%20ein%20kostenloses%20Angebot%20anfragen.","target","_blank","rel","noreferrer",1,"btn","ghost"],[1,"trust-bar"],[1,"trust-inner"],["id","services",1,"sec","light"],["reveal","",1,"intro"],[1,"eyebrow","accent"],[1,"cards","services-grid"],["tilt","","reveal","",1,"card","service",3,"delay"],["id","coverage",1,"sec","dark","map-sec"],[1,"split","map-layout"],[1,"lede"],[1,"chip-row"],["tilt","",1,"map-art"],[1,"map-origin"],["data-name","Hamburg","aria-label","Hamburg",1,"city","c-hamburg"],["data-name","Bremen","aria-label","Bremen",1,"city","c-bremen"],["data-name","Hannover","aria-label","Hannover",1,"city","c-hannover"],["data-name","M\xFCnster","aria-label","M\xFCnster",1,"city","c-muenster"],["data-name","Essen","aria-label","Essen",1,"city","c-essen"],["data-name","Dortmund","aria-label","Dortmund",1,"city","c-dortmund"],["data-name","D\xFCsseldorf","aria-label","D\xFCsseldorf",1,"city","c-duesseldorf"],["data-name","K\xF6ln","aria-label","K\xF6ln",1,"city","c-koeln"],["data-name","Frankfurt","aria-label","Frankfurt",1,"city","c-frankfurt"],["data-name","Leipzig","aria-label","Leipzig",1,"city","c-leipzig"],["data-name","Dresden","aria-label","Dresden",1,"city","c-dresden"],["data-name","N\xFCrnberg","aria-label","N\xFCrnberg",1,"city","c-nuernberg"],["data-name","Stuttgart","aria-label","Stuttgart",1,"city","c-stuttgart"],["data-name","M\xFCnchen","aria-label","M\xFCnchen",1,"city","c-muenchen"],["id","steps",1,"sec","light"],[1,"process-grid"],["reveal","",1,"process"],["id","about",1,"sec","dark"],[1,"cards","about-grid"],["tilt","","reveal","",1,"card","about-card",3,"delay"],["id","reviews",1,"sec","light","review-sec"],[1,"lede","small"],[1,"cards","review-grid"],["reveal","",1,"card","review-card"],[1,"stars"],["href","https://www.my-hammer.de/","target","_blank","rel","noreferrer"],["href","https://www.google.com/search?q=KAFI+Transporte+Berlin","target","_blank","rel","noreferrer"],["id","estimate",1,"sec","light"],["reveal","",1,"wiz"],["id","faqs",1,"sec","light"],[1,"faq",3,"open"],["id","cta",1,"sec","dark","center","final-cta"],[1,"mega",3,"parallax"],[1,"cta-sub"],[1,"row"],[1,"footer-grid"],[1,"logo","footer-logo"],["href","tel:+491787410656"],["href","mailto:abourabiyehf@gmail.com"],[1,"footer-bottom"],["href","/impressum/"],["href","/datenschutz/"],["href","https://quality-1st.de/","target","_blank","rel","noopener noreferrer"],[1,"modal"],[1,"ico"],[1,"faq"],[3,"click"],[1,"modal",3,"click"],[1,"sheet",3,"click"],["aria-label","Close",1,"x",3,"click"],["href","https://wa.me/491787410656?text=Hallo%20KAFI%20Transporte%2C%20ich%20m%C3%B6chte%20ein%20kostenloses%20Angebot%20anfragen.","target","_blank","rel","noreferrer",1,"btn","solid"]],template:function(t,i){t&1&&(T(0,"header",0)(1,"a",1),I(2,"KAFI "),T(3,"b"),I(4,"Transporte"),C()(),T(5,"nav"),mn(6,KR,2,2,"a",2,Mp),C(),T(8,"div",3)(9,"a",4),I(10,"Angebot erhalten"),C(),T(11,"a",5)(12,"span",6),I(13,"\u260E"),C(),I(14," Anrufen "),C()()(),T(15,"section",7),Kt(16,"app-hero-3d"),T(17,"div",8)(18,"p",9),I(19,"Berlin \u2022 Deutschlandweit"),C(),T(20,"h1",10),I(21,"Ihr Umzug."),T(22,"span"),I(23,"Deutschlandweit."),C()(),T(24,"p",11),I(25,"Professionelle Umz\xFCge und Transporte von Berlin in ganz Deutschland \u2013 zuverl\xE4ssig, pers\xF6nlich und unkompliziert."),C(),T(26,"div",12)(27,"a",13),I(28,"Kostenloses Angebot anfragen \u2192"),C(),T(29,"a",5)(30,"span",6),I(31,"\u260E"),C(),I(32," Anrufen "),C(),T(33,"a",14),I(34,"WhatsApp"),C()()()(),T(35,"div",15)(36,"div",16),mn(37,JR,5,2,"span",null,Mp),C()(),T(39,"section",17)(40,"div",18)(41,"p",19),I(42,"Leistungen"),C(),T(43,"h2"),I(44,"Alles f\xFCr Ihren Umzug. Aus einer Hand."),C()(),T(45,"div",20),mn(46,QR,7,4,"article",21,v_),C()(),T(48,"section",22)(49,"div",23)(50,"div",10)(51,"p",19),I(52,"Deutschlandweit"),C(),T(53,"h2"),I(54,"Von Berlin nach \xFCberall."),C(),T(55,"p",24),I(56,"Ihr Ziel ist nicht Berlin? KAFI Transporte ist deutschlandweit f\xFCr Sie unterwegs."),C(),T(57,"div",25)(58,"span"),I(59,"Hamburg"),C(),T(60,"span"),I(61,"Bremen"),C(),T(62,"span"),I(63,"Hannover"),C(),T(64,"span"),I(65,"M\xFCnster"),C(),T(66,"span"),I(67,"Essen"),C(),T(68,"span"),I(69,"Dortmund"),C(),T(70,"span"),I(71,"D\xFCsseldorf"),C(),T(72,"span"),I(73,"K\xF6ln"),C(),T(74,"span"),I(75,"Frankfurt"),C(),T(76,"span"),I(77,"Leipzig"),C(),T(78,"span"),I(79,"Dresden"),C(),T(80,"span"),I(81,"N\xFCrnberg"),C(),T(82,"span"),I(83,"Stuttgart"),C(),T(84,"span"),I(85,"M\xFCnchen"),C()()(),T(86,"div",26)(87,"div",27),I(88,"Berlin"),C(),Kt(89,"span",28)(90,"span",29)(91,"span",30)(92,"span",31)(93,"span",32)(94,"span",33)(95,"span",34)(96,"span",35)(97,"span",36)(98,"span",37)(99,"span",38)(100,"span",39)(101,"span",40)(102,"span",41),C()()(),T(103,"section",42)(104,"div",18)(105,"p",19),I(106,"Ablauf"),C(),T(107,"h2"),I(108,"So einfach kann Umziehen sein."),C()(),T(109,"div",43)(110,"div",44)(111,"span"),I(112,"01"),C(),T(113,"h3"),I(114,"Anfrage"),C(),T(115,"p"),I(116,"Sie teilen uns Ihre Umzugsdaten mit."),C()(),T(117,"div",44)(118,"span"),I(119,"02"),C(),T(120,"h3"),I(121,"Pr\xFCfung"),C(),T(122,"p"),I(123,"Wir pr\xFCfen Route, Umfang, Termin und Anforderungen."),C()(),T(124,"div",44)(125,"span"),I(126,"03"),C(),T(127,"h3"),I(128,"Kontakt"),C(),T(129,"p"),I(130,"Wir melden uns bei Ihnen und kl\xE4ren offene Fragen."),C()(),T(131,"div",44)(132,"span"),I(133,"04"),C(),T(134,"h3"),I(135,"Angebot"),C(),T(136,"p"),I(137,"Sie erhalten ein individuelles Angebot."),C()(),T(138,"div",44)(139,"span"),I(140,"05"),C(),T(141,"h3"),I(142,"Planung"),C(),T(143,"p"),I(144,"Termin und weitere Details werden abgestimmt."),C()(),T(145,"div",44)(146,"span"),I(147,"06"),C(),T(148,"h3"),I(149,"Umzug"),C(),T(150,"p"),I(151,"Der Umzug wird durchgef\xFChrt."),C()()()(),T(152,"section",45)(153,"div",18)(154,"p",19),I(155,"Warum KAFI"),C(),T(156,"h2"),I(157,"Mehr als nur Transport."),C()(),T(158,"div",46),mn(159,eN,7,4,"article",47,v_),C()(),T(161,"section",48)(162,"div",18)(163,"p",19),I(164,"Bewertungen"),C(),T(165,"h2"),I(166,"Was Kunden \xFCber KAFI Transporte sagen."),C(),T(167,"p",49),I(168,"Echte Erfahrungen von Kunden auf unabh\xE4ngigen Plattformen."),C()(),T(169,"div",50)(170,"article",51)(171,"div",52),I(172,"\u2605\u2605\u2605\u2605\u2605"),C(),T(173,"h3"),I(174,"MyHammer"),C(),T(175,"strong"),I(176,"4,5 / 5"),C(),T(177,"span"),I(178,"414 Bewertungen"),C(),T(179,"a",53),I(180,"Auf MyHammer ansehen \u2192"),C()(),T(181,"article",51)(182,"div",52),I(183,"\u2605\u2605\u2605\u2605\u2605"),C(),T(184,"h3"),I(185,"Google"),C(),T(186,"strong"),I(187,"4,6 / 5"),C(),T(188,"span"),I(189,"21 Bewertungen"),C(),T(190,"a",54),I(191,"Auf Google ansehen \u2192"),C()()()(),T(192,"section",55)(193,"div",56),Kt(194,"app-estimate"),C()(),T(195,"section",57)(196,"h2",10),I(197,"H\xE4ufige Fragen"),C(),mn(198,tN,8,4,"div",58,YR),C(),T(200,"section",59)(201,"h2",60),I(202,"Bereit f\xFCr Ihren Umzug?"),C(),T(203,"p",61),I(204,"Von Berlin in ganz Deutschland."),C(),T(205,"div",62)(206,"a",13),I(207,"Kostenloses Angebot anfragen \u2192"),C(),T(208,"a",5)(209,"span",6),I(210,"\u260E"),C(),I(211," Anrufen "),C()()(),T(212,"footer")(213,"div",63)(214,"div")(215,"div",64),I(216,"KAFI "),T(217,"b"),I(218,"Transporte"),C()(),T(219,"p"),I(220,"Ihr Umzug. Deutschlandweit."),C()(),T(221,"div")(222,"h4"),I(223,"Navigation"),C(),T(224,"ul"),mn(225,nN,3,2,"li",null,Mp),C()(),T(227,"div")(228,"h4"),I(229,"Kontakt"),C(),T(230,"ul")(231,"li"),I(232,"Ringslebenstra\xDFe 78"),C(),T(233,"li"),I(234,"12353 Berlin"),C(),T(235,"li"),I(236,"Deutschland"),C(),T(237,"li")(238,"a",65),I(239,"+49 178 7410656"),C()(),T(240,"li")(241,"a",66),I(242,"abourabiyehf@gmail.com"),C()()()(),T(243,"div")(244,"h4"),I(245,"Externe Plattformen"),C(),T(246,"ul")(247,"li")(248,"a",53),I(249,"MyHammer"),C()(),T(250,"li")(251,"a",54),I(252,"Google"),C()()()()(),T(253,"div",67)(254,"span"),I(255,"\xA9 2026 KAFI Transporte \xB7 "),T(256,"a",68),I(257,"Impressum"),C(),I(258," \xB7 "),T(259,"a",69),I(260,"Datenschutz"),C()(),T(261,"span"),I(262,"Professionell entwickelt von "),T(263,"a",70),I(264,"Quality1st"),C()()()(),Ct(265,rN,10,0,"div",71)),t&2&&(Oi("solid",i.scrolled()),ee(6),gn(i.nav),ee(18),pt("delay",120),ee(2),pt("delay",240),ee(11),gn(i.trust),ee(9),gn(i.services),ee(113),gn(i.features),ee(39),gn(i.faqs),ee(3),pt("parallax",.05),ee(24),gn(i.nav),ee(40),_t(i.modal()?265:-1))},dependencies:[bc,Nc,Ic,Rc,Js],encapsulation:2})}};S0(Pc).catch(console.error);
