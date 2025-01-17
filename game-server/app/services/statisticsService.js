var logger = require("pomelo-logger").getLogger("statisticsService", __filename)
var m_async = require("async")
var conf = require("../../config/js/conf")
var timezone = require("../util/timezone.js")
var bettingDao = require("../DataBase/bettingDao")
var gameDao = require("../DataBase/gameDao")

module.exports = function (app) {
  return new statisticsService(app)
}

var statisticsService = function (app) {
  this.app = app
  this.statistics()
}

statisticsService.prototype.statistics = function () {
  try {
    self = this

    // 取往前半年日期 YYYY-MM-DD 00:00:00
    var begin_datetime = timezone.transYesterdayTime("", conf.Statistics_Time)
    begin_datetime = begin_datetime.substr(0, 10) + " 00:00:00"

    // 取當時開機 server 時間 YYYY-MM-DD HH:mm:ss
    var end_datetime = timezone.serverTime()
    var nowMinute = Number(end_datetime.substr(14, 2))
    var dateHour = end_datetime.substr(0, 14)

    if (nowMinute >= 51) end_datetime = dateHour + "50:00"
    else if (nowMinute >= 41) end_datetime = dateHour + "40:00"
    else if (nowMinute >= 31) end_datetime = dateHour + "30:00"
    else if (nowMinute >= 21) end_datetime = dateHour + "20:00"
    else if (nowMinute >= 11) end_datetime = dateHour + "10:00"
    else if (nowMinute >= 1) end_datetime = dateHour + "00:00"
    else {
      //(nowMinute ==  0)
      dateHour = timezone.transTime(end_datetime, -1)
      end_datetime = dateHour.substr(0, 14) + "50:00"
    }

    console.log("補單統計資料 ----updateTime:", begin_datetime, " , end: ", end_datetime)

    m_async.waterfall(
      [
        function (cb) {
          gameDao.getGameGroup("GGId", cb) // 遊戲種類
        },
        function (r_code, r_data, cb) {
          let ggId = r_data.map((item) => {
            return item.GGId
          })
          bettingDao.updateRevenueReport(
            {
              begin_datetime: begin_datetime,
              end_datetime: end_datetime,
              ggId: ggId.join(","),
            },
            cb
          )
        },
      ],
      function (none, r_code, r_data) {
        console.log("-updateRevenueReport End-")
      }
    )
  } catch (err) {
    logger.error("[statisticsService][statistics] catch err", err)
  }
}
