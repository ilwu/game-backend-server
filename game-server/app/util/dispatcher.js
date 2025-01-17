var crc = require("crc")

module.exports.dispatch = function (uid, connectors) {
  var index = Math.abs(crc.crc32(uid.toString())) % connectors.length
  return connectors[index]
}
