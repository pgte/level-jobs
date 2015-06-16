var test   = require('tap').test;
var rimraf = require('rimraf');
var level  = require('level');
var async  = require('async');
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

test('has exponential backoff in case of error', function(t) {
  rimraf.sync(dbPath);
  var db = level(dbPath);
  var jobs = Jobs(db, worker);

  function worker (payload, done) {
    done(new Error('Oh no!'));
  };

  jobs.once('error', function(err) {
    t.equal(err.message, 'max retries reached');
    db.once('closed', t.end.bind(t));
    db.close();
  });

  jobs.push({ foo: 'bar' });
});

test('can delete job', function(t) {
  rimraf.sync(dbPath);
  var db = level(dbPath);
  var jobs = Jobs(db, worker);

  var processed = 0;

  function worker (payload, done) {
    processed += 1;
    t.ok(processed <= 1, 'worker is not called 2 times');

    jobs.del(job2Id, function(err) {
      if (err) throw err;
      done();
    });

    setTimeout(function() {
      db.once('closed', t.end.bind(t));
      db.close();
    }, 500);
  };

  var job1Id = jobs.push({ foo: 'bar', seq: 1 });
  t.type(job1Id, 'number');
  var job2Id = jobs.push({ foo: 'bar', seq: 2 });
  t.type(job2Id, 'number');
});

test('can get read stream', function(t) {
  rimraf.sync(dbPath);
  var db = level(dbPath);
  var jobs = Jobs(db, worker);

  var works = [
    { foo: 'bar', seq: 1 },
    { foo: 'bar', seq: 2 },
    { foo: 'bar', seq: 3 }
  ];

  var workIds = [];

  async.each(works, insert, doneInserting);

  function insert(work, done) {
    workIds.push(jobs.push(work, done).toString());
  }

  function doneInserting(err) {
    if (err) throw err;

    var rs = jobs.readStream();
    rs.on('data', onData);

    var seq = -1;
    function onData(d) {
      seq += 1;

      var id = d.key;
      var work = d.value;
      t.equal(id, workIds[seq]);
      var expected = works[seq];

      t.deepEqual(work, expected);
      if (seq == works.length - 1) {
        process.nextTick(function() {
          db.once('closed', t.end.bind(t));
          db.close();
        });
      }
    }
  }


  function worker (payload, done) {
    // do nothing
  };
});

test('doesn\'t skip past failed tasks', function(t) {
  rimraf.sync(dbPath);
  var db = level(dbPath);

  var max = 10;
  var queue = Jobs(db, worker, 1);

  for (var i = 1; i <= max; i++) {
    queue.push({ n: i }, pushed);
  }

  function pushed(err) {
    if (err) throw err;
  }

  var erroredOn = {};
  var count = 0;
  var next = 1;
  function worker(work, cb) {
    // fail every other one
    if (work.n % 2 && !erroredOn[work.n]) {
      erroredOn[work.n] = true;
      cb(new Error('oops!'));
    } else {
      count++;
      working = true;
      t.equal(next++, work.n)
      cb();
    }
  };

  queue.on('drain', function() {
    if (count === max) {
      t.equal(queue._concurrency, 0);
      db.once('closed', t.end.bind(t));
      db.close();
    }
  });
});

test('continues after close and reopen', function(t) {
  rimraf.sync(dbPath);
  var db = level(dbPath);

  var max = 10;
  var restartAfter = max / 2 | 0
  var queue = Jobs(db, worker, 1)

  for (var i = 1; i <= max; i++) {
    queue.push({ n: i }, pushed);
  }

  function pushed(err) {
    if (err) throw err;
  }

  var count = 0;
  function worker(work, cb) {
    count++;
    t.equal(work.n, count);
    cb();

    if (count === restartAfter) {
      db.close(function () {
        db = level(dbPath)
        queue = Jobs(db, worker, 1)
        queue.on('drain', function() {
          if (count === max) {
            t.equal(queue._concurrency, 0);
            db.once('closed', t.end.bind(t));
            db.close();
          }
        });
      })
    }
  };
});
