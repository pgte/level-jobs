module.exports = peek;

function peek(db, cb) {

  var calledback = false;
  function callback() {
    if (! calledback) {
      calledback = true;
      cb.apply(null, arguments);
    }
  }

  var s = db.createReadStream({ limit: 1});

  s.on('error', callback);
  s.once('end', callback);

  s.once('data', function(d) {
    if (d) callback(null, d.key, d.value);
    else callback();
  });

}