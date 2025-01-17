"use strict"

var logger = require("pomelo-logger").getLogger("localCache", __filename)
var code = require("../util/code")
var async = require("async")
var userDao = require("../DataBase/userDao")

var LocalCache = function (app) {
  this.app = app

  this.hallList = {} // (hallId -> Upid)
  this.hallDCMap = {} // (DC -> hallId)
  this.OnlinePlayerNums = {} // (cid -> {upid, (gameId -> {ggId, [playerId]})})
}

var proto = LocalCache.prototype

proto.name = "localCache"

proto.start = function (cb) {
  this.updateData(function () {
    cb()
  })
}

proto.stop = function (force, cb) {
  cb()
}

// 更新資料
proto.updateData = function (cb) {
  this.getHallList(cb)
}

// 取得 Hall 階層關係列表
proto.getHallList = function (cb) {
  let self = this

  try {
    async.waterfall(
      [
        function (done) {
          userDao.getHallList(done)
        },
        function (r_code, r_data, done) {
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          self.hallList = {}
          self.hallDCMap = {}
          for (let row of r_data) {
            self.hallList[row.Cid] = row.Upid
            self.hallDCMap[row.DC] = row.Cid
          }
          done(null)
        },
      ],
      function (err) {
        if (err) {
          if (err instanceof Error) {
            logger.error("[localCache][getHallList] err: ", err)
          } else {
            logger.error("[localCache][getHallList] err: ", JSON.stringify(err))
          }
        }
      }
    )
  } catch (err) {
    logger.error("[localCache][getHallList] err: ", err)
  }

  cb(null)
}

module.exports = function (app, opts) {
  var localCache = new LocalCache(app, opts)
  app.set(localCache.name, localCache, true)
  return localCache
}
