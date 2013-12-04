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

/// del

Q.del = function del(id, cb) {
  this._work.del(id, cb);
}

/// readStream

Q.readStream = function readStream(options) {
  if (! options) options = {};
  options = xtend({}, options);
  options.valueEncoding = 'json';
  return this._work.createReadStream(options);
};
