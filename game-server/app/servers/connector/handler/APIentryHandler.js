const logger = require("pomelo-logger").getLogger("APIentryHandler", __filename)
const async = require("async")
const code = require("../../../util/code")
const userDao = require("../../../DataBase/userDao")
const gameDao = require("../../../DataBase/gameDao")
const bettingDao = require("../../../DataBase/bettingDao")
const conf = require("../../../../config/js/conf")

const requestService = require("../../../services/requestService")
const { mappingCurrencyCode } = require("../../../services/currencyService")
const { isEmpty } = require("../../../util/utils")
const { BN } = require("../../../util/number")

const short = require("short-uuid")

module.exports = function (app) {
  return new Handler(app)
}

const Handler = function (app) {
  this.app = app
}

const handler = Handler.prototype

handler.getUrlParserForVa = function (msg, session, next) {
  const requestId = short.generate()
  const logTag = "[apiHandler][getUrlParserForVa]"

  const payload = msg.data
  const clientIp = msg.remoteIP

  if (!payload || !payload.token) {
    const message = `Parameter validation error`
    next(null, { code: code.BET.PARA_DATA_FAIL, data: { requestId, logTag, message } })
    return
  }

  const { token, lang } = payload

  const endpointApiPageJumperVA = `${conf.API_SERVER_PARSER_URL}/pageJumper/VA/detail`

  async.waterfall(
    [
      function (cb) {
        const payload = { token, lang, clientIp, requestId }

        requestService.post(endpointApiPageJumperVA, payload, { callback: cb, requestId })
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code !== code.OK) {
        const message = `API server request error`
        next(null, { code: code.FAIL, data: { requestId, logTag, message } })
        return
      }

      const { url } = r_data

      next(null, { code: code.OK, data: { url } })
      return
    }
  )
}

/**
 *  拿到玩家母單頁面初始化所需資料
 *
 * @param { agentId, playerId } payload
 * @param {*} session
 * @param {*} next
 *
 * @returns { games, game_group } data
 */
handler.getPlayerWagersInit = function (payload, session, next) {
  try {
    if (!payload || !payload.data || isEmpty(payload.data.agentId) || isEmpty(payload.data.playerId)) {
      next(null, { code: code.BET.PARA_DATA_FAIL })
      return
    }

    const self = this

    const {
      data: { agentId, playerId, playerName },
    } = payload

    let hallId = null
    const result = { games: [], game_group: [] }

    async.waterfall(
      [
        function (cb) {
          // 搜尋上級資料
          userDao.findUpId(agentId, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST })
            return
          }
          const [target] = r_data
          hallId = target.HallId

          const data = {
            agentId,
            playerId,
            playerName,
            hallId,
          }

          // 取得玩家資料
          userDao.getPlayerInfo(data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_FAIL })
            return
          }

          const payload = {
            level: "PR",
            hallId,
          }

          self.app.rpc.game.gameRemote.getUserJoinGames(session, payload, cb) //遊戲名稱
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.HISTORY_LOAD_FAIL, data: null })
            return
          }

          result.games = [...r_data]

          self.app.rpc.config.configRemote.getGameGroup(session, cb) //遊戲種類
        },
      ],
      function (none, r_code, r_data) {
        if (!r_code || r_code.code != code.OK) {
          next(null, { code: code.BET.HISTORY_LOAD_FAIL, data: null })
          return
        }

        result.game_group = [...r_data]

        next(null, { code: code.OK, data: result })
      }
    )
  } catch (err) {
    logger.error("[APIentryHandler][getPlayerWagersInit] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getPlayerWagers = function (payload, session, next) {
  try {
    const self = this
    var logs = []
    var gameType = []
    var game_group = []
    var sum = [] //總計
    var user_param = {}
    var ttlCount_bets = 0

    if (
      !payload ||
      !payload.data ||
      isEmpty(payload.data.agentId) ||
      isEmpty(payload.data.playerId) ||
      isEmpty(payload.data.start_date) ||
      isEmpty(payload.data.end_date)
    ) {
      next(null, {
        code: code.BET.PARA_DATA_FAIL,
      })
      return
    }

    const {
      data: {
        agentId,
        playerName,
        start_date,
        end_date,
        gameId,
        wId,
        ggId,
        roundId,
        page,
        pageCount,
        sortKey,
        sortType,
      },
    } = payload

    let playerId = payload.data.playerId
    let hallId = null
    let userCurrency = null

    //排序功能
    const finalSortKey = isEmpty(sortKey) ? "betDate" : sortKey
    const finalSortType =
      typeof sortType !== "undefined" && sortType != "" && [0, 1].indexOf(sortType) > -1 ? sortType : 0

    async.waterfall(
      [
        function (cb) {
          // 搜尋上級資料
          userDao.findUpId(agentId, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST })
            return
          }
          const [target] = r_data
          hallId = target.HallId

          const data = {
            agentId,
            playerId,
            playerName,
            hallId,
          }

          // 取得玩家資料
          userDao.getPlayerInfo(data, cb)
        },
        function (r_err, r_data, cb) {
          if (r_err.code != code.OK) {
            next(null, {
              code: code.USR.USER_FAIL,
            })
            return
          }

          const [targetPlayer] = r_data

          playerId = targetPlayer.Cid
          userCurrency = targetPlayer.Currency

          self.app.rpc.config.configRemote.getGameGroup(session, cb) //遊戲種類
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.HISTORY_LOAD_FAIL,
              data: null,
            })
            return
          }

          game_group = r_data

          const isSelectByWagerId = isEmpty(wId) ? false : true

          user_param = {
            level: "PR",
            playerId,
            currency: [userCurrency],
            start_date,
            end_date,
            wid: wId,
            select_wager: isSelectByWagerId,
            gameId,
            isPage: true,
            page: page || 1,
            pageCount: pageCount || 10,
            sortKey: finalSortKey,
            sortType: finalSortType,
            opCurrency: userCurrency,
            mainCurrency: userCurrency,
            ggId,
            roundid: roundId,
            isValid: ["1"],
          }

          bettingDao.getList_BetHistory_sum(user_param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.HISTORY_LOAD_FAIL,
              data: null,
            })
            return
          }
          sum = r_data
          bettingDao.getList_BetHistory_v3(user_param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.HISTORY_LOAD_FAIL,
              data: null,
            })
            return
          }

          if (r_data["info"].length === 0) {
            next(null, {
              code: code.DB.DATA_EMPTY,
              data: null,
            })
            return
          }

          ttlCount_bets = r_data["count"]
          var info = r_data["info"]

          for (var i in info) {
            var gameTypeInfo = gameType.filter((item) => item.Id == info[i]["gameTypeId"])
            var type = gameTypeInfo.length > 0 ? gameTypeInfo["Value"] : ""
            var group = game_group.filter((item) => item.GGId == info[i]["GGId"])

            var tmp = {
              wid: info[i]["wid"],
              userId: info[i]["userId"],
              memberName: info[i]["memberName"],
              gameId: info[i]["gameId"],
              gameTypeId: info[i]["gameTypeId"],
              gameType: type,
              betGold: info[i]["betGold"],
              winGold: info[i]["winGold"],
              payoutAmount: info[i]["payoutAmount"],
              playerPayout: BN(info[i].winGold).minus(info[i].realBetGold).dp(4).toNumber(),
              realBetGold: info[i]["realBetGold"],
              currency: info[i]["exCurrency"],
              cryDef: info[i]["cryDef"],
              isDemo: info[i]["isDemo"],
              isFree: info[i]["isFree"],
              isBonus: info[i]["isBonus"],
              isJP: info[i]["isJP"],
              jpType: info[i]["jpType"],
              betDate: typeof info[i]["betDate"] === "undefined" ? "" : info[i]["betDate"],
              gameState: info[i]["gameState"],
              ggId: info[i]["GGId"],
              groupNameE: group.length > 0 ? group[0]["NameE"] : "",
              groupNameG: group.length > 0 ? group[0]["NameG"] : "",
              groupNameC: group.length > 0 ? group[0]["NameC"] : "",
              groupNameVN: group.length > 0 ? group[0]["NameVN"] : "",
              groupNameTH: group.length > 0 ? group[0]["NameTH"] : "",
              groupNameID: group.length > 0 ? group[0]["NameID"] : "",
              groupNameMY: group.length > 0 ? group[0]["NameMY"] : "",
              groupNameJP: group.length > 0 ? group[0]["NameJP"] : "",
              groupNameKR: group.length > 0 ? group[0]["NameKR"] : "",
              jpGold: info[i]["JPGold"],
              roundid: info[i]["roundid"],
            }
            logs.push(tmp)
          }

          const gameIdList = [...new Set(logs.map((x) => x.gameId))]

          // 取遊戲名稱
          bettingDao.getUserAndGameName({ gameList: gameIdList, userList: [] }, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.GAME_NOT_EXIST,
            data: null,
          })
          return
        }

        const [gameNameList] = r_data

        const result = logs.map((x) => {
          const [target] = gameNameList.filter((g) => g.GameId === x.gameId)
          const { NameE, NameG, NameC, NameVN, NameTH, NameID, NameMY, NameJP, NameKR } = target
          return {
            ...x,
            nameE: NameE,
            nameG: NameG,
            nameC: NameC,
            nameVN: NameVN,
            nameTH: NameTH,
            nameID: NameID,
            nameMY: NameMY,
            nameJP: NameJP,
            nameKR: NameKR,
          }
        })

        //轉換成交收者加總
        const sumInfo = sum.map((x) => {
          const { converBetGold, converRealBetGold, converWinGold, converJPGold, rtp } = x

          return {
            betGold: converBetGold,
            realBetGold: converRealBetGold,
            winGold: converWinGold,
            jpGold: converJPGold,
            rtp: rtp,
            payoutAmount: BN(converWinGold).minus(converJPGold).minus(converRealBetGold).dp(4).toNumber(),
            playerPayout: BN(converWinGold).minus(converRealBetGold).dp(4).toNumber(),
          }
        })

        var data = {}

        //有分頁
        var ttlCount = ttlCount_bets
        var pageCount = pageCount
        var pageCur = page
        data = {
          counts: ttlCount,
          pages: Math.ceil(ttlCount / pageCount),
          page_cur: pageCur,
          page_count: pageCount,
          logs: result,
          sum: sumInfo,
          sortKey: sortKey,
          sortType: sortType,
        }

        next(null, {
          code: r_code.code,
          data: data,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[APIentryHandler][getPlayerWagers] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getPlayerWagersByPlatformWid = function (payload, session, next) {
  if (!payload || !payload.data || !payload.data.platformWid) {
    next(null, {
      code: code.BET.PARA_DATA_FAIL,
    })
    return
  }

  // 特定廠商要用nickname取代username
  const dcUseNicknameList = ["VA"]

  const platformWid = payload.data.platformWid
  const listPerPage = payload.data.listPerPage || 10
  const currentPage = payload.data.currentPage || 1

  let platformWidList = []
  let listCounts = -1
  let ggId = -1
  let gameId = -1
  let gameGroupNames = []
  let gameNames = []
  let dc = ""
  let isUseNickname = false
  let nickname = ""
  let playerId = ""

  async.waterfall(
    [
      function (cb) {
        bettingDao.getPlatformWidList({ platformWid, currentPage, listPerPage }, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.BET.HISTORY_LOAD_FAIL,
            data: null,
          })
          return
        }

        platformWidList = r_data

        if (!platformWidList || platformWidList.length === 0) {
          next(null, {
            code: code.DB.DATA_EMPTY,
            data: null,
          })
          return
        }

        const { HallId } = platformWidList[0]
        playerId = platformWidList[0].Cid

        userDao.getUser_byId({ userId: HallId, level: 2 }, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.USER_LOAD_FAIL,
            data: null,
          })
          return
        }

        dc = r_data[0].DC

        isUseNickname = dcUseNicknameList.includes(dc)

        if (isUseNickname) {
          userDao.getUser_byId({ userId: playerId, level: 4 }, cb)
        } else {
          cb(null, null, null)
        }
      },
      function (r_code, r_data, cb) {
        if (isUseNickname && r_code.code != code.OK) {
          next(null, {
            code: code.USR.USER_LOAD_FAIL,
            data: null,
          })
          return
        }

        if (isUseNickname) {
          nickname = r_data[0].NickName
        }

        bettingDao.getPlatformWidListCounts({ platformWid }, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.BET.HISTORY_LOAD_FAIL,
            data: null,
          })
          return
        }

        listCounts = r_data[0].counts
        ggId = platformWidList[0].GGId
        gameId = platformWidList[0].GameId

        bettingDao.getGameGroupNames(ggId, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK || r_data.length === 0) {
          next(null, {
            code: code.DB.DATA_EMPTY,
            data: null,
          })
          return
        }

        gameGroupNames = r_data.map((x) => {
          const { NameE, NameG, NameC, NameVN, NameTH, NameID, NameMY, NameJP, NameKR } = x

          return {
            groupNameE: NameE,
            groupNameG: NameG,
            groupNameC: NameC,
            groupNameVN: NameVN,
            groupNameTH: NameTH,
            groupNameID: NameID,
            groupNameMY: NameMY,
            groupNameJP: NameJP,
            groupNameKR: NameKR,
          }
        })[0]

        bettingDao.getGameNames(gameId, cb)
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code != code.OK || r_data.length === 0) {
        next(null, {
          code: code.GAME.GAME_NOT_EXIST,
          data: null,
        })
        return
      }

      gameNames = r_data.map((x) => {
        const { NameE, NameG, NameC, NameVN, NameTH, NameID, NameMY, NameJP, NameKR } = x

        return {
          nameE: NameE,
          nameG: NameG,
          nameC: NameC,
          nameVN: NameVN,
          nameTH: NameTH,
          nameID: NameID,
          nameMY: NameMY,
          nameJP: NameJP,
          nameKR: NameKR,
        }
      })[0]

      const initSum = {
        rtp: 0,
        betGold: 0,
        realBetGold: 0,
        winGold: 0,
        jpGold: 0,
        playerPayout: 0,
        payoutAmount: 0,
      }

      const result = platformWidList.reduce(
        (acc, cur) => {
          const {
            Wid,
            Cid,
            UserName,
            GameId,
            ExCurrency,
            CryDef,
            IsDemo,
            IsFreeGame,
            IsBonusGame,
            IsJP,
            JPType,
            AddDate,
            GGId,
            JPGold,
            BetGold,
            RealBetGold,
            WinGold,
            roundID,
          } = cur

          const betGold = BN(BetGold).div(1000).dp(2).toNumber()
          const realBetGold = BN(RealBetGold).div(1000).dp(2).toNumber()
          const jpGold = BN(JPGold).div(1000).dp(2).toNumber()
          const winGold = BN(WinGold).div(1000).dp(2).toNumber()
          const payoutAmount = BN(winGold).minus(jpGold).toNumber()
          const playerPayout = BN(winGold).minus(realBetGold).toNumber()

          let gameState = "IsBase"

          if (Number(IsFreeGame) === 1) {
            gameState = "IsFree"
          } else if (Number(IsBonusGame) === 1) {
            gameState = "IsBonus"
          }

          const wager = {
            wid: Wid,
            userId: Cid,
            memberName: isUseNickname ? nickname : UserName,
            gameId: GameId,
            betGold: betGold,
            winGold: winGold,
            payoutAmount: payoutAmount,
            playerPayout: playerPayout,
            realBetGold: realBetGold,
            currency: ExCurrency,
            cryDef: CryDef,
            isDemo: IsDemo,
            isFree: IsFreeGame,
            isBonus: IsBonusGame,
            isJP: IsJP,
            jpType: JPType,
            betDate: AddDate,
            gameState: gameState,
            ggId: GGId,
            ...gameGroupNames,
            ...gameNames,
            jpGold: jpGold,
            roundid: roundID,
          }

          acc.list.push(wager)

          // sum
          acc.sum.betGold = BN(acc.sum.betGold).plus(betGold).toNumber()
          acc.sum.realBetGold = BN(acc.sum.realBetGold).plus(realBetGold).toNumber()
          acc.sum.winGold = BN(acc.sum.winGold).plus(winGold).toNumber()
          acc.sum.jpGold = BN(acc.sum.jpGold).plus(jpGold).toNumber()
          acc.sum.payoutAmount = BN(acc.sum.payoutAmount).plus(payoutAmount).toNumber()
          acc.sum.playerPayout = BN(acc.sum.playerPayout).plus(winGold).minus(realBetGold).toNumber()
          acc.sum.rtp = BN(acc.sum.winGold).div(acc.sum.realBetGold).times(100).toNumber()

          return acc
        },
        {
          list: [],
          sum: initSum,
        }
      )

      const data = {
        counts: listCounts,
        pages: Math.ceil(listCounts / listPerPage),
        page_cur: currentPage,
        page_count: listPerPage,
        logs: result.list,
        sum: [result.sum],
      }

      next(null, {
        code: r_code.code,
        data: data,
      })
      return
    }
  )
}

handler.getPlayerWagersById = function (payload, session, next) {
  if (!payload || !payload.data || (!payload.data.roundId && !payload.data.cycleId)) {
    next(null, {
      code: code.BET.PARA_DATA_FAIL,
    })
    return
  }

  const roundId = payload.data.roundId
  const cycleId = payload.data.cycleId
  const listPerPage = payload.data.listPerPage || 10
  const currentPage = payload.data.currentPage || 1

  let listCounts = -1
  let ggId = -1
  let gameId = -1
  let gameGroupNames = []
  let gameNames = []
  let wagerList = []
  let DC = ""

  async.waterfall(
    [
      function (cb) {
        if (roundId) {
          bettingDao.getWagersByRoundId(roundId, cb)
        } else {
          bettingDao.getWagersByCycleId(cycleId, cb)
        }
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.BET.HISTORY_LOAD_FAIL,
            data: null,
          })
          return
        }

        wagerList = r_data

        if (!wagerList || wagerList.length === 0) {
          next(null, {
            code: code.DB.DATA_EMPTY,
            data: null,
          })
          return
        }

        listCounts = r_data.length
        ggId = wagerList[0].GGId
        gameId = wagerList[0].GameId

        userDao.getUser_byId({ userId: wagerList[0].HallId, level: 2 }, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK || r_data.length === 0) {
          next(null, {
            code: code.DB.DATA_EMPTY,
            data: null,
          })
          return
        }

        DC = r_data[0].DC

        bettingDao.getGameGroupNames(ggId, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK || r_data.length === 0) {
          next(null, {
            code: code.DB.DATA_EMPTY,
            data: null,
          })
          return
        }

        gameGroupNames = r_data.map((x) => {
          const { NameE, NameG, NameC, NameVN, NameTH, NameID, NameMY, NameJP, NameKR } = x

          return {
            groupNameE: NameE,
            groupNameG: NameG,
            groupNameC: NameC,
            groupNameVN: NameVN,
            groupNameTH: NameTH,
            groupNameID: NameID,
            groupNameMY: NameMY,
            groupNameJP: NameJP,
            groupNameKR: NameKR,
          }
        })[0]

        bettingDao.getGameNames(gameId, cb)
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code != code.OK || r_data.length === 0) {
        next(null, {
          code: code.GAME.GAME_NOT_EXIST,
          data: null,
        })
        return
      }

      gameNames = r_data.map((x) => {
        const { NameE, NameG, NameC, NameVN, NameTH, NameID, NameMY, NameJP, NameKR } = x

        return {
          nameE: NameE,
          nameG: NameG,
          nameC: NameC,
          nameVN: NameVN,
          nameTH: NameTH,
          nameID: NameID,
          nameMY: NameMY,
          nameJP: NameJP,
          nameKR: NameKR,
        }
      })[0]

      const initSum = {
        rtp: 0,
        betGold: 0,
        realBetGold: 0,
        winGold: 0,
        jpGold: 0,
        playerPayout: 0,
        payoutAmount: 0,
      }

      const result = wagerList.reduce(
        (acc, cur) => {
          const {
            Wid,
            Cid,
            UserName,
            GameId,
            ExCurrency,
            CryDef,
            IsDemo,
            IsFreeGame,
            IsBonusGame,
            IsJP,
            JPType,
            AddDate,
            GGId,
            JPGold,
            BetGold,
            RealBetGold,
            WinGold,
            roundID,
          } = cur

          const betGold = BN(BetGold).div(1000).dp(2).toNumber()
          const realBetGold = BN(RealBetGold).div(1000).dp(2).toNumber()
          const jpGold = BN(JPGold).div(1000).dp(2).toNumber()
          const winGold = BN(WinGold).div(1000).dp(2).toNumber()
          const payoutAmount = BN(winGold).minus(jpGold).toNumber()
          const playerPayout = BN(winGold).minus(realBetGold).toNumber()

          let gameState = "IsBase"

          if (Number(IsFreeGame) === 1) {
            gameState = "IsFree"
          } else if (Number(IsBonusGame) === 1) {
            gameState = "IsBonus"
          }

          const currencyCode = mappingCurrencyCode(DC, ExCurrency, true)

          const wager = {
            wid: Wid,
            userId: Cid,
            memberName: UserName,
            gameId: GameId,
            betGold: betGold,
            winGold: winGold,
            payoutAmount: payoutAmount,
            playerPayout: playerPayout,
            realBetGold: realBetGold,
            currency: currencyCode,
            cryDef: CryDef,
            isDemo: IsDemo,
            isFree: IsFreeGame,
            isBonus: IsBonusGame,
            isJP: IsJP,
            jpType: JPType,
            betDate: AddDate,
            gameState: gameState,
            ggId: GGId,
            ...gameGroupNames,
            ...gameNames,
            jpGold: jpGold,
            roundid: roundID,
          }

          acc.list.push(wager)

          // sum
          acc.sum.betGold = BN(acc.sum.betGold).plus(betGold).toNumber()
          acc.sum.realBetGold = BN(acc.sum.realBetGold).plus(realBetGold).toNumber()
          acc.sum.winGold = BN(acc.sum.winGold).plus(winGold).toNumber()
          acc.sum.jpGold = BN(acc.sum.jpGold).plus(jpGold).toNumber()
          acc.sum.payoutAmount = BN(acc.sum.payoutAmount).plus(payoutAmount).toNumber()
          acc.sum.playerPayout = BN(acc.sum.playerPayout).plus(winGold).minus(realBetGold).toNumber()
          acc.sum.rtp = BN(acc.sum.winGold).div(acc.sum.realBetGold).times(100).toNumber()

          return acc
        },
        {
          list: [],
          sum: initSum,
        }
      )

      const data = {
        counts: listCounts,
        pages: Math.ceil(listCounts / listPerPage),
        page_cur: currentPage,
        page_count: listPerPage,
        logs: result.list,
        sum: [result.sum],
      }

      next(null, {
        code: r_code.code,
        data: data,
      })
      return
    }
  )
}

handler.getWagerMetaData = function (payload, session, next) {
  if (!payload.data || !payload.data.wid || isEmpty(payload.data) || isEmpty(payload.data.wid)) {
    next(null, {
      code: code.BET.PARA_DATA_FAIL,
    })
    return
  }

  async.waterfall(
    [
      function (cb) {
        const data = {
          wid: payload.data.wid,
        }

        bettingDao.getWagerMetaData(data, cb)
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code != code.OK) {
        next(null, {
          code: code.BET.WAGER_NOT_EXIST,
        })
        return
      }

      next(null, {
        code: code.OK,
        data: r_data,
      })
      return
    }
  )
}

handler.getPlayerWagerDetail = function (msg, session, next) {
  if (!msg.data || !msg.data.wid || isEmpty(msg.data) || isEmpty(msg.data.wid)) {
    next(null, {
      code: code.BET.PARA_DATA_FAIL,
    })
    return
  }

  const self = this
  const redisCache = self.app.redisCache

  let mongoWagerData = null

  async.waterfall(
    [
      function (cb) {
        const { wid, isValidOnly = true } = msg.data

        const data = { wid, isValidOnly }

        bettingDao.getDetailSlot_BetHistory(data, cb) //明細
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.BET.HISTORY_LOAD_FAIL,
          })
          return
        }

        mongoWagerData = r_data
        const gameId = mongoWagerData.gameId

        redisCache.getGameData({ gameId }).then((x) => {
          if (x) {
            cb(null, { code: code.OK }, [x])
          } else {
            gameDao.getGameData({ gameId }, cb)
          }
        })
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code != code.OK || !r_data || r_data.length === 0) {
        next(null, {
          code: code.BET.HISTORY_LOAD_FAIL,
        })
        return
      }

      const [gameData] = r_data

      if (!gameData.isFromCache) {
        redisCache.setGameData(gameData)
      }

      const { Reel_Y, Reel_X } = gameData

      const result = { ...mongoWagerData, reelConfigSize: { rowCounts: Reel_Y, columnCounts: Reel_X } }

      next(null, {
        code: code.OK,
        data: result,
      })
      return
    }
  )
}

handler.getPlayerFishWagerDetail = function (msg, session, next) {
  try {
    if (typeof msg.data == "undefined" || typeof msg.data.wid === "undefined") {
      next(null, {
        code: code.BET.PARA_DATA_FAIL,
      })
      return
    }

    let data = {}
    async.waterfall(
      [
        function (cb) {
          bettingDao.getDetailFish_BetHistory(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            switch (r_code.code) {
              case code.DB.NO_DATA:
                next(null, { code: code.BET.HISTORY_WAGER_NOT_EXIST, data: [] })
                break
              case code.USR.USER_NOT_EXIST:
                next(null, { code: code.USR.USER_NOT_EXIST, data: [] })
                break
              default:
                next(null, { code: code.BET.HISTORY_LOAD_FAIL, data: [] })
                break
            }
            return
          }

          data = r_data
          userDao.get_user_byId({ Cid: r_data.playerId }, cb) // 找玩家名稱
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.USER_NOT_EXIST })
          return
        } else {
          data["UserName"] = r_data[0].customer[0].UserName

          // 刪除前端不需使用的參數
          delete data.playerId

          next(null, { code: code.OK, data: data })
          return
        }
      }
    )
  } catch (err) {
    logger.error("[APIentryHandler][getPlayerFishWagerDetail] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getPlayerArcadeWagerDetail = function (msg, session, next) {
  if (!msg.data || !msg.data.wid || isEmpty(msg.data) || isEmpty(msg.data.wid)) {
    next(null, {
      code: code.BET.PARA_DATA_FAIL,
    })
    return
  }

  let data = null

  async.waterfall(
    [
      function (cb) {
        data = {
          wid: msg.data.wid,
        }
        bettingDao.getDetailArcade_BetHistory(data, cb) //明細
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code != code.OK) {
        next(null, {
          code: code.BET.HISTORY_LOAD_FAIL,
        })
        return
      }

      next(null, {
        code: code.OK,
        data: r_data,
      })
      return
    }
  )
}

var API_actionNum = {}

handler.ip_state = function (msg, session, next) {
  var self = this
  var sys_config = self.app.get("sys_config")
  var uid = msg.remoteIP
  var api_state = "on"
  var timestamp = new Date().getTime()
  var request_num = 1
  if (typeof API_actionNum[uid] == "undefined") {
    //console.log('bind ip --------------', uid);
    API_actionNum[uid] = {
      api_timestamp: timestamp,
      request_num: request_num,
      state: api_state,
    }

    next(null, {
      code: code.OK,
      data: null,
    })
    return
  } else {
    timestamp = API_actionNum[uid]["api_timestamp"] //毫秒
    request_num = API_actionNum[uid]["request_num"]
    api_state = API_actionNum[uid]["state"]

    //判斷時間
    var last_time_sec = Math.ceil(timestamp / 1000)
    var now_sec = Math.ceil(new Date().getTime() / 1000)

    if (api_state == "close") {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    } else {
      request_num = last_time_sec == now_sec ? request_num + 1 : 1

      if (request_num >= sys_config.request_max_num) {
        //錯誤
        // 5分鐘後 清空
        setTimeout(() => {
          delete API_actionNum[uid]
          console.log("clean IP:", uid)
        }, sys_config.clean_disable_ip_sec * 1000)

        API_actionNum[uid]["api_timestamp"] = timestamp //毫秒
        API_actionNum[uid]["request_num"] = request_num
        API_actionNum[uid]["state"] = "close"

        //console.log('1------------session:', API_actionNum);
        next(null, {
          code: code.FAIL,
          data: null,
        })
        return
      } else {
        API_actionNum[uid]["api_timestamp"] = new Date().getTime() //毫秒
        API_actionNum[uid]["request_num"] = request_num

        //console.log('2------------------session:', request_num, last_time_sec, now_sec, API_actionNum[uid]['api_timestamp'], API_actionNum[uid]['request_num'], API_actionNum[uid]['state']);

        next(null, {
          code: code.OK,
          data: null,
        })
        return
      }
    }
  }
}
