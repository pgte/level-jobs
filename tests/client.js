var test       = require('tap').test;
var rimraf     = require('rimraf');
var level      = require('level');
var async      = require('async');
var Jobs       = require('../');
var ClientJobs = require('../client');

var dbPath = __dirname + '/db';

test('client', {timeout: 2000}, function(t) {
  rimraf.sync(dbPath);
  var db = level(dbPath);

  var max = 10;
  var queue = Jobs(db, worker);

  // Ensure client runs later
  queue.once('drain', function() {
    process.nextTick(function() {
      var clientQueue = ClientJobs(db);
      for (var i = 1 ; i <= max ; i ++) {
        clientQueue.push({n:i}, pushed);
      }
    });
  });

  function pushed(err) {
    if (err) throw err;
  }

  var count = 0;
  var cbs = [];
  function worker(work, cb) {
    count ++;
    t.equal(work.n, count);
    cbs.push(cb);
    if (count == max) callback();
  };

  function callback() {
    while(cbs.length) cbs.shift()();
  }

  queue.on('drain', function() {
    if (count == max) {
      t.equal(cbs.length, 0);
      t.equal(queue._concurrency, 0);
      db.once('closed', t.end.bind(t));
      db.close();
    }
  });
});


test('can delete job', function(t) {
  rimraf.sync(dbPath);
  var db = level(dbPath);
  var jobs = Jobs(db, worker);

  var clientQueue = ClientJobs(db);
  var processed = 0;

  function worker (payload, done) {
    processed += 1;
    t.ok(processed <= 1, 'worker is not called 2 times');

    clientQueue.del(job2Id, function(err) {
      if (err) throw err;
      done();
    });

    setTimeout(function() {
      db.once('closed', t.end.bind(t));
      db.close();
    }, 500);
  };

  var job1Id = clientQueue.push({ foo: 'bar', seq: 1 });
  t.type(job1Id, 'number');

  var job2Id = clientQueue.push({ foo: 'bar', seq: 2 });
  t.type(job2Id, 'number');
});
