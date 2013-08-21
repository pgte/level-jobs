module.exports = timestamp;

var lastTime;

function timestamp() {
  var t = Date.now() * 1024 + Math.floor(Math.random() * 1024);
  if (lastTime) while (t <= lastTime) t ++;
  lastTime = t;
  return t;
}