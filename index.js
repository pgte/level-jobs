var Server = require('./server');
var Client = require('./client');

// Combine Server and Client
exports = module.exports = function Jobs(db, worker, options) {
  return mixin(Server(db, worker, options), Client.Queue.prototype);
}

function mixin(a, b) {
  var target = Object.create(a); // avoids modifying original
  Object.keys(b).forEach(function(key) {
    target[key] = b[key];
  });
  return target;
}
