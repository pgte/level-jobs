var test       = require('tap').test;
var rimraf     = require('rimraf');
var level      = require('level');
var async      = require('async');
var Jobs       = require('../');
var ClientJobs = require('../client');

var dbPath = __dirname + '/db';

test('can insert and delete job', function(t) {
  rimraf.sync(dbPath);
  var db = level(dbPath);
  var jobs = Jobs(db, worker);

  var clientQueue = ClientJobs(db);
  var processed = 0;

  function worker (id, payload, done) {
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
