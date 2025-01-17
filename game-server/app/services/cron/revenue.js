const logger = require("pomelo-logger").getLogger("revenue", __filename)
const schedule = require("pomelo-schedule")
const { inspect } = require("util")
const conf = require("../../../config/js/conf")
const pomelo = require("pomelo")
const gameDao = require("../../DataBase/gameDao")

const m_async = require("async")
const timezone = require("../../util/timezone.js")
const bettingDao = require("../../DataBase/bettingDao")
const userDao = require("../../DataBase/userDao")
const code = require("../../util/code")

const isActiveRevenueSchedule = Number(conf.CRON) === 1

// 10分鐘為一個區間
const countIntervel = 10 * 60 * 1000

const getIntervalTable = (utcString) => {
  const base = new Date(`${utcString.slice(0, 13)}:00:00.000Z`)
  const previousBase = new Date(base.getTime() - 60 * 60 * 1000)

  const list = []

  logger.info(
    `[cron][getIntervalTable] countIntervel ${countIntervel}, interval base time ${base.toISOString()}, interval previous base time ${previousBase.toISOString()}`
  )

  for (let i = 0; i < 6; i++) {
    const intervelStartDiff = countIntervel * i
    const intervelEndDiff = countIntervel * (i + 1)

    const baseStart = base.getTime() + intervelStartDiff
    const baseEnd = base.getTime() + intervelEndDiff
    const previousStart = previousBase.getTime() + intervelStartDiff
    const previousEnd = previousBase.getTime() + intervelEndDiff

    list.push(
      { start: new Date(baseStart), end: new Date(baseEnd) },
      { start: new Date(previousStart), end: new Date(previousEnd) }
    )
  }

  return list
}

const revenue = () => {
  try {
    const serverTime = timezone.getServerTime()
    const currentUTC = serverTime.toISOString()

    // 現在時間往前推countIntervel的區間
    const indexTime = serverTime.clone().subtract(countIntervel)

    const indexTimestamp = indexTime.toDate().getTime()

    logger.info(
      `[cron][revenue] current serverTime ${serverTime.toISOString()}, target index time ${indexTime.toISOString()}`
    )

    const list = getIntervalTable(currentUTC)

    const [targetInterval] = list.filter((x) => indexTimestamp >= x.start && indexTimestamp < x.end)

    logger.info(`[cron][revenue] target interval ${inspect(targetInterval)}`)

    const startRange = timezone.formatTime(targetInterval.start)
    const endRange = timezone.formatTime(targetInterval.end)

    logger.warn(`[cron][revenue] start range: ${startRange}, end range: ${endRange}`)

    m_async.waterfall([
      function (cb) {
        bettingDao.deleteRevenue(
          {
            begin_datetime: startRange,
            end_datetime: endRange,
          },
          cb
        )
      },
      function (r_code, r_data, cb) {
        // 遊戲種類
        gameDao.getGameGroup("GGId", cb)
      },
      function (r_code, r_data, cb) {
        let ggId = r_data.map((item) => {
          return item.GGId
        })
        bettingDao.updateRevenueReport(
          {
            begin_datetime: startRange,
            end_datetime: endRange,
            ggId: ggId.join(","),
          },
          cb
        )
      },
      function (r_code, r_data, cb) {
        const serverTime = timezone.getServerTime()

        logger.warn(`[cron][revenue] update finished: ${serverTime.toISOString()}`)

        // 通知可以清除儀錶板資料查詢快取
        pomelo.app.rpc.bet.betRemote.onRevenue.toServer("*", function () {
          cb(null)
        })
      },
      function (cb) {
        // 通知前端可以取儀錶板資料
        pomelo.app.rpc.connector.backendRemote.onRevenue.toServer("*", function () {
          cb(null)
        })
      },
      function (cb) {
        // 用dc找hallId
        userDao.getUserByDC({ dc: "FUNKY", userLevel: 2 }, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK || !r_data || r_data.length !== 1) {
          logger.warn(`[cron][revenue][updateFunkyRevenueReport] DC FUNKY not found`)
          cb(null)
          return
        }

        const funkyHallId = r_data[0].Cid

        logger.warn(
          `[cron][revenue][updateFunkyRevenueReport] start range: ${startRange} , end range: ${endRange} , hallId: ${funkyHallId}`
        )

        bettingDao.updateFunkyRevenueReport(
          {
            startTime: startRange,
            endTime: endRange,
            hallId: funkyHallId,
          },
          cb
        )
      },
    ])
  } catch (err) {
    logger.error(`[cron][revenue] ${inspect(err)}`)
  }
}

module.exports = function (app) {
  return new revenue(app)
}

if (isActiveRevenueSchedule === true) {
  schedule.scheduleJob(
    "0 1,11,21,31,41,51 * * * *", // 若更動秒數, 請一併修改 bet.betHandler.getCronRevenueRemainSeconds
    revenue,
    {}
  )
}
