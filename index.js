var assert       = require('assert');
var inherits     = require('util').inherits;
var EventEmitter = require('events').EventEmitter;
var Sublevel     = require('level-sublevel');
var stringify    = require('json-stringify-safe');
var peek         = require('./peek');
var timestamp    = require('./timestamp');

exports = module.exports = Jobs;

function Jobs(db, worker, maxConcurrency) {
  assert.equal(typeof db, 'object', 'need db');
  assert.equal(typeof worker, 'function', 'need worker function');

  if (! maxConcurrency) maxConcurrency = Infinity;

  return new Queue(db, worker, maxConcurrency);
}

function Queue(db, worker, maxConcurrency) {
  EventEmitter.call(this);

  this._db             = db = Sublevel(db);
  this._work           = db.sublevel('work');
  this._pending        = db.sublevel('pending');
  this._worker         = worker;
  this._maxConcurrency = maxConcurrency;
  this._concurrency    = 0;

  // flags
  this._starting   = true;
  this._flushing   = false;
  this._peeking    = false;
  this._needsFlush = false;
  this._needsDrain = true;

  start(this);
}

inherits(Queue, EventEmitter);

var Q = Queue.prototype;


/// push

Q.push = function push(payload, cb) {
  var q = this;
  q._needsDrain = true;
  this._work.put(timestamp(), stringify(payload), put);

  function put(err) {
    if (err) {
      if (cb) cb(err);
      else q.emit('error', err);
    }
    maybeFlush(q);
  }
};


/// start

function start(q) {
  var ws = q._work.createWriteStream();
  q._pending.createReadStream().pipe(ws);
  ws.once('close', done);

  function done() {
    q._starting = false;
    flush(q);
  }
}


/// maybeFlush

function maybeFlush(q) {
  if (! q._starting && ! q._flushing) flush(q);
  else q._needsFlush = true;
}

/// flush

function flush(q) {
  if (q._concurrency < q._maxConcurrency && ! q._peeking) {
    q._peeking  = true;
    q._flushing = true;
    peek(q._work, poke);
  }

  function poke(err, key, work) {
    q._peeking = false;
    var done = false;

    if (key) {
      q._concurrency ++;
      q._db.batch([
        { type: 'del', key: key, prefix: q._work },
        { type: 'put', key: key, value: work, prefix: q._pending }
      ], transfered);
    } else {
      q._flushing = false;
      if (q._needsFlush) {
        q._needsFlush = false;
        maybeFlush(q);
      } else if (q._needsDrain) {
        q._needsDrain = false;
        q.emit('drain');
      }
    }

    function transfered(err) {
      if (err) {
        q._concurrency --;
        q.emit('error', err);
      } else {
        run(q, JSON.parse(work), ran);
      }
      flush(q);
    }

    function ran(err) {
      if (! done) {
        done = true;
        q._concurrency --;

        if (err) handleRunError();
        else {
          q._pending.del(key, function(_err) {
            if (err) q.emit('error', _err);
            flush(q);
          });
        }
      }
    }

    function handleRunError() {
      // Error handling
      q._needsDrain = true;
      q._db.batch([
        { type: 'del', key: key, prefix: q._pending },
        { type: 'put', key: key, value: work, prefix: q._work }
      ], backToWorkAfterError);
    }

    function backToWorkAfterError(err) {
      if (err) q.emit('error', err);
      flush(q);
    }
  }
}


/// run

function run(q, work, cb) {
  q._worker(work, cb);
}