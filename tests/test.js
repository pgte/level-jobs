var test   = require('tap').test;
var rimraf = require('rimraf');
var level  = require('level');
var Jobs   = require('../');

var dbPath = __dirname + '/db';

test('infinite concurrency', function(t) {

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
      t.equal(queue._concurrency, 0);
      db.once('closed', t.end.bind(t));
      db.close();
    }
  });
});

test('concurrency of 1', function(t) {

  rimraf.sync(dbPath);
  var db = level(dbPath);

  var max = 10;
  var concurrency = 1;
  var queue = Jobs(db, worker, concurrency);

  for (var i = 1 ; i <= max ; i ++) {
    queue.push({n:i}, pushed);
  }

  function pushed(err) {
    if (err) throw err;
  }

  var count = 0;
  var working = false;
  function worker(work, cb) {
    t.notOk(working, 'should not be concurrent');
    count ++;
    working = true;
    t.equal(work.n, count);
    setTimeout(function() {
      working = false;
      cb();
    }, 100);
  };

  queue.on('drain', function() {
    if (count == max) {
      t.equal(queue._concurrency, 0);
      db.once('closed', t.end.bind(t));
      db.close();
    }
  });
});

test('retries on error', function(t) {
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

  var erroredOn = {};
  var count = 0;
  function worker(work, cb) {
    count ++;
    if (!erroredOn[work.n]) {
      erroredOn[work.n] = true;
      cb(new Error('oops!'));
    } else {
      working = true;
      cb();
    }
  };

  queue.on('drain', function() {
    if (count == max * 2) {
      t.equal(queue._concurrency, 0);
      db.once('closed', t.end.bind(t));
      db.close();
    }
  });
});

test('works with no push callback', function(t) {
  rimraf.sync(dbPath);
  var db = level(dbPath);
  var jobs = Jobs(db, worker);

  function worker (payload, done) {
    done();
    process.nextTick(function() {
      db.once('closed', t.end.bind(t));
      db.close();
    });
  };                                                                                                                                            

  jobs.push({ foo: 'bar' });
});