var test   = require('tap').test;
var rimraf = require('rimraf');
var level  = require('level');
var Jobs   = require('../');

var dbPath = __dirname + '/db';

test('infinity concurrency', function(t) {

  rimraf.sync(dbPath);

  var db = level(dbPath);

  var max = 10;
  var queue = Jobs(db, worker);

  for (var i = 1 ; i <= max ; i ++) {
    queue.push({n:i}, pushed);
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
      db.close();
    }
  });
});