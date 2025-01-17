var m_async = require("async")

module.exports = function () {
  return new Filter()
}

var Filter = function () {}

Filter.prototype.before = function (msg, session, next) {
  session.set("timestamp", new Date().getTime())
  session.pushAll()
  next()
}

Filter.prototype.after = function (err, msg, session, resp, next) {
  next(err)
}
