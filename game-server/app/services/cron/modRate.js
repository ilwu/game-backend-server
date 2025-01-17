var schedule = require("pomelo-schedule")
var conf = require("../../../config/js/conf")
////////////////////////////////////////////////////////////////////

var m_async = require("async")
var logDao = require("../../DataBase/logDao")
var configDao = require("../../DataBase/configDao")

var modRate = function (app) {
  this.app = app
  self = this

  m_async.waterfall(
    [
      function (cb) {
        configDao.getExChangeRate(cb)
      },
      function (r_code, r_data, cb) {
        logDao.renewExChangeRate(r_data, cb)
      },
    ],
    function (none, r_code, r_data) {
      console.log("getExChangeRate end")
    }
  )
}

module.exports = function (app) {
  return new modRate(app)
}
if (conf.CRON == 1) {
  schedule.scheduleJob("0 0/30 * * * *", modRate, {})
}
