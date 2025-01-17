const logger = require("pomelo-logger").getLogger("localCache_bet", __filename)
const { inspect } = require("util")

var LocalCache = function (app) {
  this.app = app

  // 儀表板數據的快取資料
  this.revenueResults = {} // {requestKey -> {cid -> revenue}}
  // admin 的 cid 記為 ''
}

var proto = LocalCache.prototype

proto.name = "localCache"

proto.start = function (cb) {
  cb()
}

proto.stop = function (force, cb) {
  cb()
}

// 清除 revenue 資料
proto.clearRevenueResults = function () {
  this.revenueResults = {}
}
// 加入 revenue 資料
proto.addRevenueResult = function (requestKey, cid, result, hourDiff) {
  try {
    const cacheKey = `${cid}_${hourDiff}`

    this.revenueResults[requestKey] = this.revenueResults[requestKey] || {}
    this.revenueResults[requestKey][cacheKey] = result
  } catch (err) {
    logger.error("[localCache_bet][addRevenueResult] catch err", inspect(err))
  }
}
// 取得 revenue 資料
proto.getRevenueResult = function (requestKey, cid, hourDiff) {
  try {
    if (!this.revenueResults[requestKey]) {
      return
    }

    const cacheKey = `${cid}_${hourDiff}`

    return this.revenueResults[requestKey][cacheKey]
  } catch (err) {
    logger.error("[localCache_bet][getRevenueResult] catch err", inspect(err))
  }
}

module.exports = function (app, opts) {
  var localCache = new LocalCache(app, opts)
  app.set(localCache.name, localCache, true)
  return localCache
}
