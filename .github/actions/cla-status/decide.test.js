/**
 * Repeatable check of the CLA-status composite action's decision logic: the
 * endpoint-query + fail-closed + signed/unsigned computation, run against a
 * stubbed endpoint (no network, no Actions runtime required).
 *
 * Run: node .github/actions/cla-status/decide.test.js
 */
'use strict';

var assert = require('assert');
var decideModule = require('./decide.js');
var decide = decideModule.decide;
var MAX_ATTEMPTS = decideModule.MAX_ATTEMPTS;

// A no-op backoff so retry tests run instantly instead of waiting in real time.
function noopSleep() { return Promise.resolve(); }

var failures = 0;
var passed = 0;
function check(name, fn) {
  try { fn(); passed++; console.log('  ok   - ' + name); }
  catch (e) { failures++; console.log('  FAIL - ' + name + '\n         ' + e.message); }
}

// A stubbed endpoint: `signed` lists the logins that are contributors; any login
// not in the set answers `isContributor: false`. Records every URL it was asked
// so tests can assert only the bare login (no other contributor value) is sent.
function stubEndpoint(signedLogins) {
  var signed = new Set(signedLogins);
  var calls = [];
  var fn = function (url) {
    calls.push(url);
    var login = decodeURIComponent(url.split('checkContributor=')[1]);
    return Promise.resolve({
      ok: true,
      status: 200,
      json: function () { return Promise.resolve({ isContributor: signed.has(login) }); },
    });
  };
  fn.calls = calls;
  return fn;
}

function nonOkEndpoint(status) {
  return function () {
    return Promise.resolve({ ok: false, status: status || 502, json: function () { return Promise.resolve({}); } });
  };
}

function rejectingEndpoint() {
  return function () { return Promise.reject(new Error('network unreachable')); };
}

var ENDPOINT = 'https://example.invalid/exec?checkContributor=';

async function run() {
  console.log('all committers signed and enumeration complete -> success');
  await (async function () {
    var result = await decide({
      logins: ['alice', 'bob'],
      enumerationComplete: true,
      unidentified: [],
      unidentifiedPolicy: 'fail',
      endpoint: ENDPOINT,
      fetchImpl: stubEndpoint(['alice', 'bob']),
    });
    check('state is success', function () { assert.strictEqual(result.state, 'success'); });
    check('unsigned is empty', function () { assert.deepStrictEqual(result.unsigned, []); });
    check('unidentified is echoed back empty', function () { assert.deepStrictEqual(result.unidentified, []); });
  })();

  console.log('one unsigned committer -> error');
  await (async function () {
    var result = await decide({
      logins: ['alice', 'bob'],
      enumerationComplete: true,
      unidentified: [],
      unidentifiedPolicy: 'fail',
      endpoint: ENDPOINT,
      fetchImpl: stubEndpoint(['alice']),
    });
    check('state is error', function () { assert.strictEqual(result.state, 'error'); });
    check('unsigned names bob', function () { assert.deepStrictEqual(result.unsigned, ['bob']); });
  })();

  console.log('enumeration-complete=false -> error, fails closed without querying the endpoint');
  await (async function () {
    var endpoint = stubEndpoint(['alice', 'bob']);
    var result = await decide({
      logins: ['alice', 'bob'],
      enumerationComplete: false,
      unidentified: [],
      unidentifiedPolicy: 'fail',
      endpoint: ENDPOINT,
      fetchImpl: endpoint,
    });
    check('state is error', function () { assert.strictEqual(result.state, 'error'); });
    check('endpoint is never queried', function () { assert.strictEqual(endpoint.calls.length, 0); });
  })();

  console.log('unidentified committers under policy=fail -> error, even if every named login signed');
  await (async function () {
    var result = await decide({
      logins: ['alice'],
      enumerationComplete: true,
      unidentified: ['deadbeef'],
      unidentifiedPolicy: 'fail',
      endpoint: ENDPOINT,
      fetchImpl: stubEndpoint(['alice']),
    });
    check('state is error', function () { assert.strictEqual(result.state, 'error'); });
    check('unsigned logins list is still empty', function () { assert.deepStrictEqual(result.unsigned, []); });
    check('unidentified is echoed back', function () { assert.deepStrictEqual(result.unidentified, ['deadbeef']); });
  })();

  console.log('same unidentified committers under policy=skip -> ignored, success');
  await (async function () {
    var result = await decide({
      logins: ['alice'],
      enumerationComplete: true,
      unidentified: ['deadbeef'],
      unidentifiedPolicy: 'skip',
      endpoint: ENDPOINT,
      fetchImpl: stubEndpoint(['alice']),
    });
    check('state is success', function () { assert.strictEqual(result.state, 'success'); });
    check('unidentified is still echoed back (informational)', function () { assert.deepStrictEqual(result.unidentified, ['deadbeef']); });
  })();

  console.log('endpoint returns non-OK for every attempt -> throws after exhausting retries (job fails, no status posted)');
  await (async function () {
    var threw = false;
    var calls = 0;
    var fetchImpl = function (url) { calls++; return nonOkEndpoint(503)(url); };
    try {
      await decide({
        logins: ['alice'],
        enumerationComplete: true,
        unidentified: [],
        unidentifiedPolicy: 'fail',
        endpoint: ENDPOINT,
        fetchImpl: fetchImpl,
        sleepImpl: noopSleep,
      });
    } catch (e) { threw = true; }
    check('decide() rejects rather than returning a status', function () { assert.strictEqual(threw, true); });
    check('the endpoint was retried MAX_ATTEMPTS times before giving up', function () { assert.strictEqual(calls, MAX_ATTEMPTS); });
  })();

  console.log('endpoint fetch rejects on every attempt (unreachable) -> throws after exhausting retries (job fails, no status posted)');
  await (async function () {
    var threw = false;
    var calls = 0;
    var fetchImpl = function (url) { calls++; return rejectingEndpoint()(url); };
    try {
      await decide({
        logins: ['alice'],
        enumerationComplete: true,
        unidentified: [],
        unidentifiedPolicy: 'fail',
        endpoint: ENDPOINT,
        fetchImpl: fetchImpl,
        sleepImpl: noopSleep,
      });
    } catch (e) { threw = true; }
    check('decide() rejects rather than returning a status', function () { assert.strictEqual(threw, true); });
    check('the endpoint was retried MAX_ATTEMPTS times before giving up', function () { assert.strictEqual(calls, MAX_ATTEMPTS); });
  })();

  console.log('endpoint returns a transient 404 for a signed login, then succeeds on retry -> success (regression for the 404-with-no-retry bug)');
  await (async function () {
    var calls = 0;
    var fetchImpl = function (url) {
      calls++;
      if (calls === 1) {
        return Promise.resolve({ ok: false, status: 404, json: function () { return Promise.resolve({}); } });
      }
      return Promise.resolve({ ok: true, status: 200, json: function () { return Promise.resolve({ isContributor: true }); } });
    };
    var result = await decide({
      logins: ['signed-user'],
      enumerationComplete: true,
      unidentified: [],
      unidentifiedPolicy: 'fail',
      endpoint: ENDPOINT,
      fetchImpl: fetchImpl,
      sleepImpl: noopSleep,
    });
    check('state is success once the retry succeeds', function () { assert.strictEqual(result.state, 'success'); });
    check('the endpoint was queried twice (one 404, one success)', function () { assert.strictEqual(calls, 2); });
  })();

  console.log('a later login in the same call also gets retried independently of an earlier login\'s outcome');
  await (async function () {
    var aliceCalls = 0;
    var bobCalls = 0;
    var fetchImpl = function (url) {
      if (url.indexOf('alice') !== -1) {
        aliceCalls++;
        return Promise.resolve({ ok: true, status: 200, json: function () { return Promise.resolve({ isContributor: true }); } });
      }
      bobCalls++;
      if (bobCalls === 1) {
        return Promise.resolve({ ok: false, status: 404, json: function () { return Promise.resolve({}); } });
      }
      return Promise.resolve({ ok: true, status: 200, json: function () { return Promise.resolve({ isContributor: true }); } });
    };
    var result = await decide({
      logins: ['alice', 'bob'],
      enumerationComplete: true,
      unidentified: [],
      unidentifiedPolicy: 'fail',
      endpoint: ENDPOINT,
      fetchImpl: fetchImpl,
      sleepImpl: noopSleep,
    });
    check('state is success', function () { assert.strictEqual(result.state, 'success'); });
    check('alice was queried once (no retry needed)', function () { assert.strictEqual(aliceCalls, 1); });
    check('bob was retried once after the transient 404', function () { assert.strictEqual(bobCalls, 2); });
  })();

  console.log('posted state is only ever success or error, never pending/neutral/failure');
  await (async function () {
    var allSigned = await decide({
      logins: ['alice'], enumerationComplete: true, unidentified: [], unidentifiedPolicy: 'fail',
      endpoint: ENDPOINT, fetchImpl: stubEndpoint(['alice']),
    });
    var incomplete = await decide({
      logins: ['alice'], enumerationComplete: false, unidentified: [], unidentifiedPolicy: 'fail',
      endpoint: ENDPOINT, fetchImpl: stubEndpoint(['alice']),
    });
    check('success case state is exactly "success"', function () { assert.ok(['success', 'error'].indexOf(allSigned.state) !== -1); assert.strictEqual(allSigned.state, 'success'); });
    check('fail-closed case state is exactly "error"', function () { assert.ok(['success', 'error'].indexOf(incomplete.state) !== -1); assert.strictEqual(incomplete.state, 'error'); });
  })();

  console.log('duplicate logins are queried once');
  await (async function () {
    var endpoint = stubEndpoint(['alice']);
    await decide({
      logins: ['alice', 'alice', 'alice'],
      enumerationComplete: true,
      unidentified: [],
      unidentifiedPolicy: 'fail',
      endpoint: ENDPOINT,
      fetchImpl: endpoint,
    });
    check('endpoint queried once per unique login', function () { assert.strictEqual(endpoint.calls.length, 1); });
  })();

  console.log('only the bare GitHub handle is ever sent to the endpoint (no other contributor value)');
  await (async function () {
    var endpoint = stubEndpoint(['alice']);
    await decide({
      logins: ['alice'],
      enumerationComplete: true,
      unidentified: [],
      unidentifiedPolicy: 'fail',
      endpoint: ENDPOINT,
      fetchImpl: endpoint,
    });
    check('request URL is exactly the endpoint plus the encoded login', function () {
      assert.strictEqual(endpoint.calls[0], ENDPOINT + 'alice');
    });
  })();

  console.log('endpoint non-OK for a login after an earlier login already succeeded -> throws (fail-closed regardless of position)');
  await (async function () {
    var calls = [];
    var fetchImpl = function (url) {
      calls.push(url);
      if (url.indexOf('alice') !== -1) {
        return Promise.resolve({ ok: true, status: 200, json: function () { return Promise.resolve({ isContributor: true }); } });
      }
      return Promise.resolve({ ok: false, status: 500, json: function () { return Promise.resolve({}); } });
    };
    var threw = false;
    try {
      await decide({
        logins: ['alice', 'bob'],
        enumerationComplete: true,
        unidentified: [],
        unidentifiedPolicy: 'fail',
        endpoint: ENDPOINT,
        fetchImpl: fetchImpl,
        sleepImpl: noopSleep,
      });
    } catch (e) { threw = true; }
    check('decide() rejects when any queried login is non-OK, not only the first', function () { assert.strictEqual(threw, true); });
  })();

  console.log('login value is percent-encoded before being appended to the endpoint URL (no raw query injection)');
  await (async function () {
    var calls = [];
    var fetchImpl = function (url) {
      calls.push(url);
      return Promise.resolve({ ok: true, status: 200, json: function () { return Promise.resolve({ isContributor: true }); } });
    };
    var weirdLogin = 'weird user&extra=x';
    await decide({
      logins: [weirdLogin],
      enumerationComplete: true,
      unidentified: [],
      unidentifiedPolicy: 'fail',
      endpoint: ENDPOINT,
      fetchImpl: fetchImpl,
    });
    check('the request URL is exactly the endpoint plus the percent-encoded login', function () {
      assert.strictEqual(calls[0], ENDPOINT + encodeURIComponent(weirdLogin));
    });
    check('the raw, unescaped login never appears in the request URL', function () {
      assert.strictEqual(calls[0].indexOf(weirdLogin), -1);
    });
  })();

  console.log('unidentified commit refs are never sent to the endpoint, even when non-empty');
  await (async function () {
    var endpoint = stubEndpoint(['alice']);
    var sensitiveRef = 'commit-authored-by-jane.doe@example.com';
    await decide({
      logins: ['alice'],
      enumerationComplete: true,
      unidentified: ['deadbeef', sensitiveRef],
      unidentifiedPolicy: 'fail',
      endpoint: ENDPOINT,
      fetchImpl: endpoint,
    });
    check('endpoint is queried exactly once, for the login only', function () { assert.strictEqual(endpoint.calls.length, 1); });
    check('no unidentified ref value appears in any endpoint call', function () {
      endpoint.calls.forEach(function (url) {
        assert.strictEqual(url.indexOf('deadbeef'), -1);
        assert.strictEqual(url.indexOf('jane.doe'), -1);
      });
    });
  })();

  console.log('multiple logins are each queried with exactly their own bare, encoded handle -- no cross-contamination');
  await (async function () {
    var endpoint = stubEndpoint(['alice', 'bob']);
    await decide({
      logins: ['alice', 'bob'],
      enumerationComplete: true,
      unidentified: [],
      unidentifiedPolicy: 'fail',
      endpoint: ENDPOINT,
      fetchImpl: endpoint,
    });
    check('exactly two calls were made', function () { assert.strictEqual(endpoint.calls.length, 2); });
    check('each call is exactly the endpoint plus that login\'s own encoded handle', function () {
      assert.deepStrictEqual(endpoint.calls, [ENDPOINT + 'alice', ENDPOINT + 'bob']);
    });
  })();

  console.log('description stays within the 140-character commit-status limit');
  await (async function () {
    var manyUnsigned = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r'];
    var result = await decide({
      logins: manyUnsigned,
      enumerationComplete: true,
      unidentified: [],
      unidentifiedPolicy: 'fail',
      endpoint: ENDPOINT,
      fetchImpl: stubEndpoint([]),
    });
    check('description is 140 characters or fewer', function () { assert.ok(result.description.length <= 140, 'length was ' + result.description.length); });
  })();

  console.log('\n' + passed + ' passed, ' + failures + ' failed');
  process.exit(failures ? 1 : 0);
}

run();
