var assert       = require('assert');
var inherits     = require('util').inherits;
var EventEmitter = require('events').EventEmitter;
var Sublevel     = require('level-sublevel');
var stringify    = require('json-stringify-safe');
var xtend        = require('xtend');
var timestamp    = require('./timestamp');

exports = module.exports = ClientQueue;

function ClientQueue(db, worker, options) {
  assert.equal(typeof db, 'object', 'need db');
  assert.equal(arguments.length, 1, 'cannot define worker on client');

  return new Queue(db);
}

ClientQueue.Queue = Queue

function Queue(db) {
  EventEmitter.call(this);

  this._db = db = Sublevel(db);
  this._pending = db.sublevel('pending');
  this._work = db.sublevel('work');
}

inherits(Queue, EventEmitter);

var Q = Queue.prototype;

/// push

Q.push = function push(payload, cb) {
  var q = this;
  var id = timestamp();
  this._work.put(id, stringify(payload), put);

  return id;

  function put(err) {
    if (err) {
      if (cb) cb(err);
      else q.emit('error', err);
    } else if (cb) cb();
  };
}

/// pushBatch

Q.pushBatch = function push(payloads, cb) {
  var q = this;
  var ids = [];

  var ops = payloads.map(function(payload) {
    var id = timestamp();
    ids.push(id)
    return {
      type: 'put',
      key: id,
      value: stringify(payload)
    }
  })

  this._work.batch(ops, batch);

  return ids;

  function batch(err) {
    if (err) {
      if (cb) cb(err);
      else q.emit('error', err);
    } else if (cb) cb();
  };
}

/// del

Q.del = function del(id, cb) {
  this._work.del(id, cb);
}

/// delBatch

Q.delBatch = function del(ids, cb) {
  var ops = ids.map(function(id) {
    return { type: 'del', key: id }
  })
  this._work.batch(ops, cb);
}

Q.pendingStream = function pendingStream(options) {
  if (!options) options = {};
  else options = xtend({}, options);
  options.valueEncoding = 'json';
  return this._pending.createReadStream(options);
};

Q.runningStream = function runningStream(options) {
  if (!options) options = {};
  else options = xtend({}, options);
  options.valueEncoding = 'json';
  return this._work.createReadStream(options);
};
