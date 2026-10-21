/**
 * Decision logic for the CLA-status composite action: given a resolved committer
 * set, decide whether the pull request or merge-queue candidate has satisfied the
 * CLA gate, and produce the `verification/cla-signed` state and description.
 *
 * Extracted into a plain module (rather than living inline in the wrapped
 * `github-script` step) so it can run under a stubbed endpoint outside the
 * Actions runtime -- no live endpoint, no GitHub API, no network required.
 *
 * Fails closed by construction:
 *   - Incomplete committer enumeration is treated as not-signed immediately,
 *     without ever querying the endpoint for a partially-seen committer set.
 *   - An unresolvable committer handle counts as not-signed under
 *     `unidentifiedPolicy: 'fail'`; it is ignored only under `'skip'`.
 *   - An unreachable, timed-out, or non-OK endpoint response for any login is
 *     retried (with backoff) a few times before this function rejects outright
 *     rather than return a status -- the caller must let that rejection fail
 *     the job so no `success` (or any status at all) is ever posted on an
 *     outage that outlasts the retries.
 *
 * The only value ever sent to the endpoint is the bare GitHub login being
 * checked; no other committer- or contributor-derived data is transmitted.
 */
'use strict';

var CLA_STATUS_CONTEXT = 'verification/cla-signed';
var DESCRIPTION_MAX_LENGTH = 140;

// The endpoint (a Google Apps Script web app) intermittently answers a non-OK
// status, or a request to it errors, for a login that is in fact signed --
// observed as a load/quota blip, not a per-login failure. Retry a few times
// with backoff before giving up and failing the job closed.
var MAX_ATTEMPTS = 5;
var RETRY_BASE_DELAY_MS = 500;

function defaultSleep(ms) {
  return new Promise(function (resolve) { setTimeout(resolve, ms); });
}

/**
 * Build the commit-status description, matching the established
 * "All committers have signed the CLA" / "CLA not signed: <list>" wording and
 * staying within the Statuses API's description length limit.
 */
function buildDescription(signed, unsigned, unidentified, policy) {
  if (signed) return 'All committers have signed the CLA';

  var notSigned = unsigned.slice();
  if (policy === 'fail' && unidentified.length > 0) {
    notSigned = notSigned.concat(unidentified.map(function (ref) { return 'unidentified:' + ref; }));
  }

  var text = notSigned.length > 0
    ? 'CLA not signed: ' + notSigned.join(', ')
    : 'CLA verification incomplete: committer enumeration was not confirmed complete';
  return text.slice(0, DESCRIPTION_MAX_LENGTH);
}

/**
 * Query the endpoint for a single login, retrying a non-OK response or a fetch
 * rejection up to `MAX_ATTEMPTS` times with linear backoff. Still fails closed:
 * the last attempt's failure is what gets thrown once retries are exhausted.
 */
async function queryLoginWithRetry(login, endpoint, doFetch, sleepImpl) {
  var lastError;
  for (var attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    var res;
    try {
      res = await doFetch(endpoint + encodeURIComponent(login));
    } catch (err) {
      lastError = new Error('CLA endpoint request failed for ' + login + ': ' + err.message);
    }
    if (res) {
      if (res.ok) return res.json();
      lastError = new Error('CLA endpoint returned ' + res.status + ' for ' + login);
    }
    if (attempt < MAX_ATTEMPTS) {
      await sleepImpl(RETRY_BASE_DELAY_MS * attempt);
    }
  }
  // Fail the job outright: no status is posted at all rather than a silent
  // success on an endpoint outage that outlasted every retry.
  throw lastError;
}

/**
 * @param {object} args
 * @param {string[]} args.logins - Committer GitHub logins to check (deduplicated internally).
 * @param {boolean} args.enumerationComplete - Whether every committer was enumerated.
 * @param {string[]} [args.unidentified] - Commit refs with no resolvable author login.
 * @param {'fail'|'skip'} [args.unidentifiedPolicy] - How to treat `unidentified` (default 'fail').
 * @param {string} args.endpoint - Endpoint base URL; the encoded login is appended per query.
 * @param {(url: string) => Promise<{ok: boolean, status?: number, json: () => Promise<any>}>} [args.fetchImpl]
 *   Injected fetch (defaults to the global `fetch`), so a test can stub the endpoint with no network.
 * @param {(ms: number) => Promise<void>} [args.sleepImpl]
 *   Injected backoff delay (defaults to a real `setTimeout`-based sleep), so a test can
 *   exercise retries without waiting in real time.
 * @returns {Promise<{state: 'success'|'error', unsigned: string[], unidentified: string[], description: string}>}
 *   Resolves with the decision. Rejects (throws) if the endpoint is still unreachable or
 *   still returns non-OK for any login after retries are exhausted -- callers must
 *   propagate that rejection to fail the job, never catch it into a posted status.
 */
async function decide(args) {
  var logins = args.logins;
  var enumerationComplete = args.enumerationComplete;
  var unidentified = args.unidentified;
  var unidentifiedPolicy = args.unidentifiedPolicy;
  var endpoint = args.endpoint;
  var fetchImpl = args.fetchImpl;
  var sleepImpl = args.sleepImpl || defaultSleep;

  var uniqueLogins = Array.from(new Set(logins || []));
  var unidentifiedList = Array.isArray(unidentified) ? unidentified : [];
  var policy = unidentifiedPolicy === 'skip' ? 'skip' : 'fail';
  var doFetch = fetchImpl || fetch;

  // Incomplete enumeration fails closed immediately: an endpoint that is never
  // even queried can never wave a partially-seen committer set through.
  if (!enumerationComplete) {
    return {
      state: 'error',
      unsigned: [],
      unidentified: unidentifiedList,
      description: buildDescription(false, [], unidentifiedList, policy),
    };
  }

  var unsigned = [];
  for (var i = 0; i < uniqueLogins.length; i++) {
    var login = uniqueLogins[i];
    var body = await queryLoginWithRetry(login, endpoint, doFetch, sleepImpl);
    if (!body.isContributor) unsigned.push(login);
  }

  var unidentifiedCountsAsUnsigned = policy === 'fail' && unidentifiedList.length > 0;
  var signed = unsigned.length === 0 && !unidentifiedCountsAsUnsigned;

  return {
    state: signed ? 'success' : 'error',
    unsigned: unsigned,
    unidentified: unidentifiedList,
    description: buildDescription(signed, unsigned, unidentifiedList, policy),
  };
}

// Guarded export matching the established pattern for the pure-logic modules
// behind this gate, so this file loads the same way whether it's `require()`d by
// the wrapped github-script step or by a plain-Node test.
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    decide: decide,
    buildDescription: buildDescription,
    CLA_STATUS_CONTEXT: CLA_STATUS_CONTEXT,
    MAX_ATTEMPTS: MAX_ATTEMPTS,
  };
}
