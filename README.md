# level-jobs

> Job Queue in LevelDB for Node.js

[![Build Status](https://travis-ci.org/pgte/level-jobs.png?branch=master)](https://travis-ci.org/pgte/level-jobs)

* Define worker functions
* Persist work units
* Work units are retried when failed
* Define maximum concurrency

## Install

```bash
$ npm install level-jobs --save
```

## Use

### Create a levelup database

```javascript
var levelup = require('levelup');
var db = levelup('./db')
```

### Require level-jobs

```javascript
var Jobs = require('level-jobs');
```

### Define a worker function

This function will take care of a work unit.

```javascript
function worker(payload, cb) {
  doSomething(cb);
}
```

This function gets 2 arguments: one is the payload of the work unit and the other is the callback function that must be called when the work is done.

This callback function accepts an error as the first argument. If an error is provided, the work unit is retried.


### Wrap the database

```javascript
var queue = Jobs(db, worker);
```

This database will be at the mercy and control of level-jobs, don't use it for anything else!

(this database can be a root levelup database or a sublevel)

You can define a maximum concurrency (the default is `Infinity`):

```javascript
var maxConcurrency = 2;
var queue = Jobs(db, worker, maxConcurrency);
```

### More Options

As an alternative the third argument can be an options object with these defaults:

```javascript
var options = {
  maxConcurrency: Infinity,
  maxRetries:     10,
  backoff: {
    randomisationFactor: 0,
    initialDelay: 10,
    maxDelay: 300
  }
};

var queue = Jobs(db, worker, options);
```

### Push work to the queue

```javascript
var payload = {what: 'ever'};

var jobId = queue.push(payload, function(err) {
  if (err) console.error('Error pushing work into the queue', err.stack);
});
```

### Delete pending job

(Only works for jobs that haven't started yet!)

```javascript
queue.del(jobId, function(err) {
  if (err) console.error('Error deleting job', err.stack);
});
```

### Traverse pending jobs

(Only works for jobs that haven't started yet!)

```javascript
var stream = queue.readStream();
stream.on('data', function(d) {
  var jobId = d.key;
  var work = d.value;
  console.log('pending job id: %s, work: %j', jobId, work);
});
```

### Events

A queue object emits the following event:

* `drain` — when there are no more jobs pending. Also happens on startup after consuming the backlog work units.
* `error` - when something goes wrong.


## Client isolated API

If you simply want a pure queue client that is only able to push jobs into the queue, you can use `level-jobs/client` like this:

```javascript
var QueueClient = require('level-jobs/client');

var client = QueueClient(db);

client.push(work, function(err) {
  if (err) throw err;
  console.log('pushed');
});
```

## License

MIT
