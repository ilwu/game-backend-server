var code = require("../../../util/code")
var m_async = require("async")
var bettingDao = require("../../../DataBase/bettingDao")
var timezone = require("../../../util/timezone")

module.exports = function (app) {
  return new BetRemote(app)
}

var BetRemote = function (app) {
  this.app = app
}

// revenue 統計完成
BetRemote.prototype.onRevenue = function (cb) {
  this.app.localCache.clearRevenueResults()

  cb()
}

var bet_remote = BetRemote.prototype
