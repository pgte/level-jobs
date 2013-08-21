var test   = require('tap').test;
var rimraf = require('rimraf');
var level  = require('level');
var Jobs   = require('../');

var dbPath = __dirname + '/db';

rimraf.sync(dbPath);

var db = level(dbPath);

test('infinity concurrency', function(t) {
  var max = 1;
  var queue = Jobs(db, worker);

  for (var i = 0 ; i < max ; i ++) {
    queue.push({n:1}, pushed);
  }

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
      t.end();
    }
  });
});

test('closes the db', function(t) {
  db.close();
  t.end();
});