Date.prototype.getUnixTime = function () {
  return (this.getTime() / 1000) | 0
}

Date.now = function () {
  return new Date()
}

Date.time = function () {
  return Date.now().getUnixTime()
}

module.exports = Date
