#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const vm = require('vm');

function encodeBase64(s){
  return Buffer.from(String(s), 'utf8').toString('base64');
}

function buildContext(opts = {}) {
  const store = new Map(Object.entries(opts.localStorage || {}));
  const width = opts.innerWidth ?? 1280;
  const height = opts.innerHeight ?? 720;
  const screenX = opts.screenX ?? 10;
  const screenY = opts.screenY ?? -10;
  const webdriver = opts.webdriver ?? true;
  const bodyClientWidth = opts.bodyClientWidth ?? 1265;
  const bodyClientHeight = opts.bodyClientHeight ?? 1093;
  const docClientWidth = opts.docClientWidth ?? 1265;
  const docClientHeight = opts.docClientHeight ?? 720;

  function makeProto(tag, next = null) {
    const proto = {
      toString() { return `[object ${tag}]`; },
    };
    Object.defineProperty(proto, Symbol.toStringTag, { value: tag });
    Object.setPrototypeOf(proto, next);
    return proto;
  }

  const windowProto4 = makeProto('Object', null);
  const windowProto3 = makeProto('EventTarget', windowProto4);
  const windowProto2 = makeProto('WindowProperties', windowProto3);
  const windowProto1 = makeProto('Window', windowProto2);

  const documentProto5 = makeProto('Object', null);
  const documentProto4 = makeProto('EventTarget', documentProto5);
  const documentProto3 = makeProto('Node', documentProto4);
  const documentProto2 = makeProto('Document', documentProto3);
  const documentProto1 = makeProto('HTMLDocument', documentProto2);

  const windowObj = {
    navigator: {
      webdriver,
      appName: opts.appName || 'Netscape',
      userAgent: opts.userAgent || 'Mozilla/5.0',
    },
    innerWidth: width,
    innerHeight: height,
    screenX,
    screenY,
    screen: { width: opts.screenWidth ?? 1280, height: opts.screenHeight ?? 720 },
    toString() { return '[object Window]'; },
  };
  Object.setPrototypeOf(windowObj, windowProto1);

  const document = {
    body: { clientWidth: bodyClientWidth, clientHeight: bodyClientHeight },
    documentElement: { clientWidth: docClientWidth, clientHeight: docClientHeight },
    createElement(){ return {}; },
    all: opts.documentAll,
    dda: opts.documentDda,
    toString() { return '[object HTMLDocument]'; },
  };
  Object.setPrototypeOf(document, documentProto1);

  const $ = function(){};
  $.extend = function(obj){ Object.assign($, obj); return $; };
  $.ajaxSetup = function(){};
  $.cs = { encode: encodeBase64 };
  if (opts.jqueryState) {
    Object.assign($, opts.jqueryState);
  }

  function JSEncrypt(){}
  JSEncrypt.prototype.setPublicKey = function(){};
  JSEncrypt.prototype.encrypt = function(x){ return x; };

  const fakeConsole = {
    log() {},
    info() {},
    warn() {},
    error() {},
    debug() {},
  };

  windowObj.document = document;
  windowObj.window = windowObj;
  windowObj.self = windowObj;
  windowObj.top = windowObj;
  windowObj.parent = windowObj;
  document.defaultView = windowObj;

  const context = {
    console: fakeConsole,
    window: windowObj,
    document,
    navigator: windowObj.navigator,
    top: windowObj,
    self: windowObj,
    parent: windowObj,
    innerWidth: width,
    innerHeight: height,
    screenX,
    screenY,
    screen: windowObj.screen,
    invInt: opts.invInt,
    wzwschallenge: opts.wzwschallenge,
    wzwschallengex: opts.wzwschallengex,
    localStorage: {
      getItem(k){ return store.has(k) ? store.get(k) : null; },
      setItem(k,v){ store.set(k, String(v)); },
      removeItem(k){ store.delete(k); },
    },
    HTMLAllCollection: function HTMLAllCollection(){},
    setInterval(){ return 1; },
    clearInterval(){},
    setTimeout(){ return 1; },
    clearTimeout(){},
    unescape,
    encodeURIComponent,
    decodeURIComponent,
    escape,
    Uint8Array,
    Uint32Array,
    ArrayBuffer,
    Math,
    Date,
    parseInt,
    JSEncrypt,
    $, jQuery: $,
  };
  context.global = context;
  context.globalThis = context;
  return context;
}

function loadNnyd(context) {
  const code = fs.readFileSync(path.resolve(__dirname, 'upstream_js', 'wlop.js'), 'utf8');
  vm.createContext(context);
  vm.runInContext(code, context, { timeout: 20000 });
  if (!context.$ || !context.$.nnyd) throw new Error('$.nnyd not initialized');
  return context.$.nnyd;
}

function main(){
  let input = {};
  if (process.argv[2]) {
    input = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
  } else {
    const stdin = fs.readFileSync(0, 'utf8').trim();
    if (stdin) input = JSON.parse(stdin);
  }
  try {
    const context = buildContext(input.env || {});
    const n = loadNnyd(context);
    const yzm = n.yzm(input.fpdm, input.fphm, input.yzmPublicKey);
    const cyArg = input.cyArg ?? input.kprq ?? '';
    const cy = n.cy(input.fpdm, input.fphm, cyArg);
    const out = {
      ok: true,
      yzm,
      cy,
      encoderchars: context.window && context.window.encoderchars,
      invInt: context.window && context.window.invInt,
      wzwschallenge: context.window && context.window.wzwschallenge,
      wzwschallengex: context.window && context.window.wzwschallengex,
      m1Count: context.$ && context.$.m1Count,
      m2Count: context.$ && context.$.m2Count,
    };
    process.stdout.write(JSON.stringify(out));
  } catch (e) {
    process.stdout.write(JSON.stringify({ ok: false, error: String(e), stack: e && e.stack || '' }));
    process.exitCode = 1;
  }
}

if (require.main === module) main();
