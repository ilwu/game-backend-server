const logger = require("pomelo-logger").getLogger("funkyHandler", __filename)
const m_async = require("async")
const short = require("short-uuid")
const code = require("../../../util/code")
const bettingDao = require("../../../DataBase/bettingDao")
const userDao = require("../../../DataBase/userDao")
const gameDao = require("../../../DataBase/gameDao")

const { getErrorMessage } = require("../../../util/log")

module.exports = function (app) {
  return new Handler(app)
}

const Handler = function (app) {
  this.app = app
}

const handler = Handler.prototype

const sortableKeyList = [
  "wagerCount",
  "currency",
  "gameId",
  "betGold",
  "realBetGold",
  "winGold",
  "jpGold",
  "payout",
  "netWin",
  "rtp",
]

handler.getFunkyReportSearchOptions = function (msg, session, next) {
  const requestId = short.generate()
  const logTag = "[funkyHandler][getFunkyReportSearchOptions]"

  const sourceData = msg.data || {}

  const sourceDataString = JSON.stringify(sourceData)
  const errorPayload = { requestId, logTag, sourceDataString }

  logger.info(`${requestId} ${logTag} source data => ${sourceDataString}`)

  const { displayConfigs } = sourceData

  const dc = "FUNKY"

  let hallId = ""

  const responseData = {
    currencyList: [],
    gameList: [],
    funkyPlatformIdList: [],
    sortableKeyList,
  }

  m_async.waterfall(
    [
      function (cb) {
        userDao.getUserByDC({ dc, userLevel: 2 }, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code !== code.OK || r_data.length === 0) {
          const message = `DC '${dc}' not found`
          const errorMessage = getErrorMessage({ ...errorPayload, message })

          logger.error(errorMessage)

          next(null, { code: code.USR.USER_NOT_EXIST, data: { requestId, logTag, message } })
          return
        }

        hallId = r_data[0].Cid

        // 是否開放幣別搜尋列
        if (!displayConfigs.currency) {
          cb(null, { code: code.OK }, [])
        } else {
          userDao.getWallets(hallId, cb)
        }
      },
      function (r_code, r_data, cb) {
        if (r_code.code !== code.OK || (displayConfigs.currency && r_data.length === 0)) {
          const message = `${dc} , hallId '${hallId}' not found any wallets`
          const errorMessage = getErrorMessage({ ...errorPayload, message })

          logger.error(errorMessage)

          next(null, { code: code.USR.CURRENCY_LOAD_FAIL, data: { requestId, logTag, message } })
          return
        }

        responseData.currencyList = r_data.map((x) => {
          const { Currency } = x

          return { value: Currency, label: Currency }
        })

        // 是否開放遊戲搜尋列
        if (!displayConfigs.gameId) {
          cb(null, { code: code.OK }, [])
        } else {
          gameDao.getUserEnabledGames({ cid: hallId }, cb)
        }
      },
      function (r_code, r_data, cb) {
        if (r_code.code !== code.OK || (displayConfigs.gameId && r_data.length === 0)) {
          const message = `${dc} , hallId '${hallId}' not found any enabled games`
          const errorMessage = getErrorMessage({ ...errorPayload, message })

          logger.error(errorMessage)

          next(null, { code: code.GAME.GAME_LOAD_FAIL, data: { requestId, logTag, message } })
          return
        }

        responseData.gameList = r_data.map((x) => {
          const { GGId, GameId, NameC, NameG, NameE } = x

          return {
            gameId: GameId,
            ggId: GGId,
            name: {
              tw: NameG,
              cn: NameC,
              en: NameE,
            },
          }
        })

        // 是否開放遊戲搜尋列
        if (!displayConfigs.platformId) {
          cb(null, { code: code.OK }, [])
        } else {
          bettingDao.getFunkyReportPlatformIdList({}, cb)
        }
      },
      function (r_code, r_data, cb) {
        if (r_code.code !== code.OK) {
          const message = `${dc} , hallId '${hallId}' not found any fpid on funky_revenue`
          const errorMessage = getErrorMessage({ ...errorPayload, message })

          logger.error(errorMessage)

          next(null, { code: code.DB.LOAD_DATA_FAIL, data: { requestId, logTag, message } })
          return
        }

        responseData.funkyPlatformIdList = r_data.map((x) => {
          const { platformId } = x

          return { value: platformId, label: platformId }
        })

        cb(null, { code: code.OK })
      },
    ],
    function (none, r_code) {
      if (r_code.code !== code.OK) {
        const message = "Get funky search option failed"
        const errorMessage = getErrorMessage({ ...errorPayload, message })

        logger.error(errorMessage)

        next(null, { code: code.FAIL, data: { requestId, logTag, message } })
        return
      } else {
        next(null, { code: code.OK, data: responseData })
        return
      }
    }
  )
}

handler.getFunkyReportTransactionDate = function (msg, session, next) {
  const requestId = short.generate()
  const logTag = "[funkyHandler][getFunkyReportTransactionDate]"

  const sourceData = msg.data || {}

  const sourceDataString = JSON.stringify(sourceData)
  const errorPayload = { requestId, logTag, sourceDataString }

  logger.info(`${requestId} ${logTag} source data => ${sourceDataString}`)

  // funky日期為UTC+8
  const {
    startDate,
    endDate,
    platformId,
    gameId,
    currency,
    playerAccount,
    pagingData = {},
    sort = {},
    accountType = false,
  } = sourceData

  const { currentPage = 1, listPerPage = 50 } = pagingData
  const { key: sortKey = "id", order: sortOrder } = sort

  const dc = "FUNKY"

  let hallId = ""

  const responseData = {
    list: [],
    sum: [],
    totalCounts: 0,
    currentPage,
    listPerPage,
  }

  m_async.waterfall(
    [
      function (cb) {
        userDao.getUserByDC({ dc, userLevel: 2 }, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code !== code.OK || r_data.length === 0) {
          const message = `DC '${dc}' not found`
          const errorMessage = getErrorMessage({ ...errorPayload, message })

          logger.error(errorMessage)

          next(null, { code: code.USR.USER_NOT_EXIST, data: { requestId, logTag, message } })
          return
        }

        hallId = r_data[0].Cid

        bettingDao.getFunkyTransactionDateDataCount(
          { startDate, endDate, platformId, gameId, currency, playerAccount, accountType },
          cb
        )
      },
      function (r_code, r_data, cb) {
        if (r_code.code !== code.OK) {
          const message = `${dc} , hallId '${hallId}' get row count error`
          const errorMessage = getErrorMessage({ ...errorPayload, message })

          logger.error(errorMessage)

          next(null, { code: code.BET.LOAD_WAGER_FAIL, data: { requestId, logTag, message } })
          return
        }

        if (r_data[0].rows === 0) {
          next(null, { code: code.OK, data: responseData })
          return
        }

        responseData.totalCounts = r_data[0].rows

        bettingDao.getFunkyTransactionDateData(
          {
            startDate,
            endDate,
            platformId,
            gameId,
            currency,
            playerAccount,
            currentPage,
            listPerPage,
            sortKey,
            sortOrder,
            accountType,
          },
          cb
        )
      },
      function (r_code, r_data, cb) {
        if (r_code.code !== code.OK) {
          const message = `${dc} , hallId '${hallId}' get report error`
          const errorMessage = getErrorMessage({ ...errorPayload, message })

          logger.error(errorMessage)

          next(null, { code: code.BET.LOAD_WAGER_FAIL, data: { requestId, logTag, message } })
          return
        }

        responseData.list = r_data.map((x) => {
          const {
            statementDate,
            fpId,
            gameId,
            currency,
            wagerCount,
            cId,
            username,
            upId,
            betGold,
            realBetGold,
            winGold,
            jpGold,
            netWin,
          } = x

          return {
            statementDate,
            platformId: fpId,
            gameId,
            currency,
            wagerCount,
            playerId: cId,
            username,
            agentId: upId,
            betGold,
            realBetGold,
            winGold,
            jpGold,
            netWin,
          }
        })

        bettingDao.getFunkyTransactionDateSumData(
          {
            startDate,
            endDate,
            platformId,
            gameId,
            currency,
            playerAccount,
            accountType,
          },
          cb
        )
      },
      function (r_code, r_data, cb) {
        if (r_code.code !== code.OK) {
          const message = `${dc} , hallId '${hallId}' get report sum error`
          const errorMessage = getErrorMessage({ ...errorPayload, message })

          logger.error(errorMessage)

          next(null, { code: code.BET.LOAD_WAGER_FAIL, data: { requestId, logTag, message } })
          return
        }

        responseData.sum = r_data.map((x) => {
          const {
            wagerCount,
            betGold,
            realBetGold,
            winGold,
            jpGold,
            netWin,
            cnyBetGold,
            cnyRealBetGold,
            cnyWinGold,
            cnyJPGold,
            cnyPayout,
            cnyNetWin,
          } = x

          return {
            wagerCount,
            betGold,
            realBetGold,
            winGold,
            jpGold,
            netWin,
            cnyBetGold,
            cnyRealBetGold,
            cnyWinGold,
            cnyJPGold,
            cnyPayout,
            cnyNetWin,
          }
        })

        cb(null, { code: code.OK })
      },
    ],
    function (none, r_code) {
      if (r_code.code !== code.OK) {
        const message = "Get funky report failed"
        const errorMessage = getErrorMessage({ ...errorPayload, message })

        logger.error(errorMessage)

        next(null, { code: code.FAIL, data: { requestId, logTag, message } })
        return
      } else {
        next(null, { code: code.OK, data: responseData })
        return
      }
    }
  )
}
