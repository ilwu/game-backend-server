var logger = require("pomelo-logger").getLogger("bettingDao", __filename)
var code = require("../util/code")
var conf = require("../../config/js/conf")
var pomelo = require("pomelo")
var m_async = require("async")
var db = require("../util/DB")
var sprintf = require("sprintf-js").sprintf
var timezone = require("../util/timezone")
var bettingDao = module.exports
const utils = require("../util/utils")
var consts = require("../share/consts")
const { inspect } = require("util")
const { getMongoConnection } = require("../util/mongoDB.js")
const { isEmpty, SQLBuilder, mergeKeyValuePairs } = utils

bettingDao.getAreaPlayers = async function (data, cb) {
  try {
    const { wid, isValidOnly } = data

    const db = getMongoConnection("fishHunter")

    //找母單
    const collection = db.collection("fish_hunter_area_players_history")

    const searchCondition = isValidOnly ? { $or: [{ isDelete: false }, { isDelete: { $exists: false } }] } : {}

    const res = await collection
      .aggregate([
        {
          $match: { _id: wid, ...searchCondition },
        },
        {
          $project: {
            _id: 1,
            areaId: 1,
            playerId: 1,
            gameId: 1,
          },
        },
      ])
      .toArray()

    return res
  } catch (err) {
    logger.error("[bettingDao][getAreaPlayers] catch err", inspect(err))
    cb(null, code.FAIL, null)
  }
}

bettingDao.SumDetailFish_BetHistory = function (SumwhereStr, cb) {
  try {
    const db = getMongoConnection("fishHunter")

    //找出子彈依押注、贏得 為群組做統計
    const collection = db.collection("fish_hunter_bullets_history")
    const res = collection
      .aggregate([
        {
          $match: SumwhereStr,
        },
        {
          $group: {
            _id: null,
            count: { $sum: 1 },
            betSum: { $sum: "$cost" },
            gainSum: { $sum: "$gain" },
          },
        },
      ])
      .toArray()
    return res
  } catch (err) {
    logger.error("[bettingDao][SumDetailFish_BetHistory] catch err", err)
    cb(null, code.FAIL, null)
  }
}

bettingDao.getDetailFish_BetHistory = async function (data, cb) {
  try {
    //找母單
    const entries = await this.getAreaPlayers(data, null)

    if (entries.length > 0) {
      const db = getMongoConnection("fishHunter")

      // 取統計:需過濾免費武器的表演子彈及未碰撞的紀錄
      // wId: {$in:['', data.wid]}只有延遲型的子彈會有wId且五秒一張單，其餘都沒有wId且進出遊戲房才有單
      // var SumwhereStr = {areaId: entries[0].areaId, playerId: entries[0].playerId, wId: {$in:['', data.wid]}, 'endReason': {$nin:['WeaponFireComplete', 'FireComplete', 'CancelFire']}};
      var SumwhereStr = {
        areaId: entries[0].areaId,
        playerId: entries[0].playerId,
        wId: { $in: ["", data.wid] },
        endReason: { $nin: ["CancelFire"] },
      }
      let Sumbullet = await this.SumDetailFish_BetHistory(SumwhereStr, null) //找出子彈依押注、贏得 為群組做統計

      if (Sumbullet.length > 0) {
        Sumbullet[0].betSum = utils.number.oneThousand(Sumbullet[0].betSum, consts.Math.DIVIDE)
        Sumbullet[0].gainSum = utils.number.oneThousand(Sumbullet[0].gainSum, consts.Math.DIVIDE)
      }

      // 一頁有幾筆
      var pageSize = data.pageCount
      // 當前第幾頁
      var currentPage = data.curPage
      // 跳過數
      var skipnum = (currentPage - 1) * pageSize
      var res_count = 0
      var sortKey = data.sortKey
      // 排序（按endTime -1倒序,  1正序）
      var sortType = data.sortType == 0 ? -1 : 1
      var sort = {} //{ 'endTime': -1 };
      sort[sortKey] = sortType

      // 取子彈清單
      var whereStr = {}
      if (data.onKill == true) {
        // 有得分的
        // whereStr = {areaId: entries[0].areaId, playerId: entries[0].playerId,'$or':[{'gain':{$gt:0}},{'shootType':'return'}]};
        whereStr = {
          areaId: entries[0].areaId,
          playerId: entries[0].playerId,
          wId: { $in: ["", data.wid] },
          gain: { $gt: 0 },
        }
      } else {
        // 未得分
        // whereStr = {areaId: entries[0].areaId, playerId: entries[0].playerId, wId: {$in:['', data.wid]}, gain: 0, 'shootType': {$ne: 'return'}, 'endReason':{$in:['FireCollider', 'WeaponFireComplete']}};
        whereStr = {
          areaId: entries[0].areaId,
          playerId: entries[0].playerId,
          wId: { $in: ["", data.wid] },
          $or: [{ gain: 0 }, { extraBetNoDie: true }],
          endReason: { $in: ["FireCollider", "CollidReward"] },
        }
      }

      if (data.onKill == true) {
        //找出有得分的
        //計算真正筆數
        db.collection("fish_hunter_bullets_history").countDocuments(whereStr, function (error, resC) {
          if (error) {
            console.log("Error_count" + error)
          } else {
            res_count = resC
            db.collection("fish_hunter_bullets_history")
              .aggregate([
                {
                  $match: whereStr,
                },
                {
                  $project: {
                    endReason: 1,
                    alive: 1,
                    shootType: 1,
                    finishTime: 1,
                    hitFishes: 1,
                    afterBalance: 1,
                    beforeBalance: 1,
                    afterFireBalance: 1,
                    beforeFireBalance: 1,
                    denom: 1,
                    gain: 1,
                    cost: 1,
                    getInfo: 1,
                    returnInfo: 1,
                    odds: 1,
                  },
                },
                {
                  $sort: sort,
                },
                {
                  $skip: skipnum,
                },
                {
                  $limit: pageSize,
                },
              ])
              .toArray(function (err, res) {
                if (err) {
                  console.log("Error2:" + err)
                  cb(null, { code: code.DB.QUERY_FAIL }, null)
                } else {
                  for (let i in res) {
                    res[i].betScore = utils.number.oneThousand(res[i].cost, consts.Math.DIVIDE)
                    res[i].winScore = utils.number.oneThousand(res[i].gain, consts.Math.DIVIDE)
                    res[i].betAmount = utils.number.oneThousand(res[i].cost, consts.Math.DIVIDE)
                    res[i].winAmount = utils.number.oneThousand(res[i].gain, consts.Math.DIVIDE)
                  }
                  var data = {
                    count: res_count,
                    info: res,
                    sumbullet: Sumbullet[0],
                    gameId: entries[0].gameId,
                    playerId: entries[0].playerId,
                  }
                  cb(null, { code: code.OK }, data)
                }
              })
          }
        })
      } else {
        //找出有未得分的
        db.collection("fish_hunter_bullets_history")
          .aggregate([
            {
              $match: whereStr,
            },
            {
              $project: {
                hitFishes: 1,
                getInfo: 1,
                cost: 1,
                beforeFireBalance: 1,
                afterFireBalance: 1,
                shootType: 1,
                originalCost: 1,
                sum: 1,
                endReason: 1,
                oods: 1,
              },
            },
            {
              $group: {
                _id: {
                  endReason: "$FireCollider",
                  hitFishes: "$hitFishes",
                  cost: "$cost",
                  beforeFireBalance: "$beforeFireBalance",
                  afterFireBalance: "$afterFireBalance",
                  shootType: "$shootType",
                  getInfo: "$getInfo",
                  originalCost: "$getInfo.originalCost",
                },
                count: { $sum: 1 },
              },
            },
            {
              $sort: sort,
            },
          ])
          .toArray(function (err, res) {
            if (err) {
              console.log("Error3:" + err)
              cb(null, { code: code.DB.QUERY_FAIL }, null)
            } else {
              if (res.length === 0) {
                var data = {
                  count: res.length,
                  info: res,
                  sumbullet: Sumbullet[0],
                }
                cb(null, { code: code.OK }, data)
              }

              for (let i in res) {
                res[i]["_id"].betScore = utils.number.oneThousand(res[i]["_id"].cost, consts.Math.DIVIDE)
                res[i]["_id"].betAmount = utils.number.oneThousand(res[i]["_id"].cost, consts.Math.DIVIDE)
              }

              var count = pageSize * currentPage
              var end = count > res.length ? res.length : count

              var fishData = []
              for (let i = skipnum; i < end; i++) {
                fishData.push(res[i])
              }
              data = {
                count: res.length,
                info: fishData,
                sumbullet: Sumbullet[0],
                gameId: entries[0].gameId,
                playerId: entries[0].playerId,
              }
              cb(null, { code: code.OK }, data)
            }
          })
      }
    } else {
      cb(null, { code: code.DB.NO_DATA }, null)
    }
  } catch (err) {
    logger.error("[bettingDao][getDetailFish_BetHistory] catch err", err)
    cb(null, code.FAIL, null)
  }
}

bettingDao.getDetailSlot_BetHistory = async function (data, cb) {
  try {
    const { wid, isValidOnly } = data

    const db = getMongoConnection("slot")
    const collection = db.collection("wager")

    const searchCondition = isValidOnly ? { $or: [{ isDelete: false }, { isDelete: { $exists: false } }] } : {}

    const query = { $and: [{ wagerId: wid }, searchCondition] }

    const queryResult = await collection.findOne(query)

    logger.info("[bettingDao][getDetailSlot_BetHistory] mongo query ", JSON.stringify(query))

    if (queryResult) {
      const extra = mergeKeyValuePairs(queryResult.extra)

      const {
        gameId,
        score: totalWinAmount,
        bet: totalBetAmount,
        creditWin: totalWinScore,
        creditBet: totalBetScore,
        denom,
        status,
        isPremadeBetGoldList,
        extraTriggerType,
      } = queryResult

      const { json, jackpotTrigger, bonusGameResult, originalBet: originalBetAmount } = extra

      let isJackpot = false
      let jpPoolId = null
      let extraReelResult = null

      // 判斷有無彩金
      if (jackpotTrigger) {
        isJackpot = true
        jpPoolId = extra.jpType
        delete extra.jpType
      }

      if (bonusGameResult) {
        extraReelResult = bonusGameResult
        delete extra.bonusGameResult
      }

      // 判斷是否消除類
      const isCascadingGame = json ? true : false

      let detailResults = null

      if (isCascadingGame) {
        const temp = JSON.parse(json)
        detailResults = temp.map((x) => mergeKeyValuePairs(x))
        delete extra.json
      } else {
        const { score, reelResult, winLines } = extra
        detailResults = [{ score, reelResult, winLines }]
        delete extra.score
        delete extra.reelResult
        delete extra.winLines
      }

      // 取得遊戲狀態
      const gameStatus = utils.getGameStatus(status)

      bettingDao.getGameType({ gameId }, (_, r_code, data) => {
        if (r_code.code !== code.OK || data.length === 0) {
          logger.error("[bettingDao][getDetailSlot_BetHistory] getGameType err", r_code)
          cb(null, { code: code.FAIL }, null)
        } else {
          const [{ Value: gameTypeValue }] = data

          const result = {
            ...extra,
            isPremadeBetGoldList,
            extraTriggerType,
            detailResults,
            gameId,
            totalWinScore,
            totalBetScore,
            totalWinAmount,
            totalBetAmount,
            originalBetAmount,
            denom,
            isCascadingGame,
            jackpotTrigger: isJackpot,
            jpPoolId,
            extraReelResult,
            gameTypeValue,
            ...gameStatus,
          }

          cb(null, { code: code.OK }, result)
        }
      })
    } else {
      cb(null, { code: code.DB.NO_DATA }, null)
    }
  } catch (err) {
    logger.error("[bettingDao][getDetailSlot_BetHistory] catch err", err)
    cb(null, code.FAIL, null)
  }
}

bettingDao.getGameType = async function (data, cb) {
  const sql = `SELECT gt.Value,gt.Id \
                FROM games AS g \
                LEFT JOIN game_type AS gt ON g.TypeId = gt.Id \
                WHERE g.gameId = ?`

  const { gameId } = data
  const args = [gameId]

  logger.info("[bettingDao][getgameType] sql %s , args %s", sql, args)

  db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
    if (r_code.code !== code.OK) {
      cb(null, r_code, null)
    } else {
      cb(null, r_code, r_data)
    }
  })
}

bettingDao.getWagerMetaData = async function (data, cb) {
  const sql = `SELECT Wid,Cid,UserName,UpId,HallId,GGId,GameId,AddDate, \
                DATE_FORMAT(AddDate,'%Y-%m-%d %H:%i:%s') AS AddDateFormat,IsBonusGame,IsFreeGame,IsJP \
                FROM wagers_bet where Wid = ?`
  const wid = data.wid
  const args = [wid]

  logger.info("[bettingDao][getWagerMetaData] sql %s , args %s", sql, args)

  db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
    if (r_code.code !== code.OK) {
      cb(null, r_code, null)
    } else {
      cb(null, r_code, r_data[0])
    }
  })
}

bettingDao.getDetailArcade_BetHistory = async function (data, cb) {
  try {
    const db = getMongoConnection("slot")
    const collection = db.collection("wager")
    let query = { wagerId: data.wid }

    const queryResult = await collection.findOne(query)

    logger.info("[bettingDao][getDetailArcade_BetHistory] query & queryResult ", query, queryResult)

    if (queryResult) {
      const extra = mergeKeyValuePairs(queryResult.extra)

      logger.info("[bettingDao][getDetailArcade_BetHistory] extra ", extra)

      const {
        gameId,
        score: totalWinAmount,
        bet: totalBetAmount,
        creditWin: totalWinScore,
        creditBet: totalBetScore,
        denom,
        status,
      } = queryResult

      const { jackpotTrigger, reelResult, betInfo, winBetInfo } = extra

      let isJackpot = false
      let jpPoolId = null

      // 判斷有無彩金
      if (jackpotTrigger) {
        isJackpot = true
        jpPoolId = extra.jpType
        delete extra.jpType
      }

      // 取得遊戲狀態
      const gameStatus = utils.getGameStatus(status)

      let detailResults = null

      detailResults = [{ reelResult }]

      delete extra.reelResult
      delete extra.betInfo
      delete extra.winBetInfo

      const result = {
        ...extra,
        detailResults,
        betInfo,
        winBetInfo,
        gameId,
        totalWinScore,
        totalBetScore,
        totalWinAmount,
        totalBetAmount,
        denom,
        jackpotTrigger: isJackpot,
        jpPoolId,
        ...gameStatus,
      }

      cb(null, { code: code.OK }, result)
    } else {
      cb(null, { code: code.DB.NO_DATA }, null)
    }
  } catch (err) {
    logger.error("[bettingDao][getDetailArcade_BetHistory] catch err", err)
    cb(null, code.FAIL, null)
  }
}
// bettingDao.getDetail_BetHistory = function (data, cb) {

//     var sql = 'SELECT Wid,GameId,PayTotal,BetTotal,Denom,BetLevel,Result,LinesJSON,DATE_FORMAT(AddDate,"%Y-%m-%d %H:%i:%s" ) AS betDate,TypeId,GGId,FeatureId,IsJP, ' +
//         " (CASE " +
//         "  WHEN IsFreeGame = true THEN 'IsFree' " +
//         "  WHEN IsBonusGame = true THEN 'IsBonus' " +
//         "  ELSE 'IsBase' " +
//         "  END) AS status " +
//         " FROM wagers_detail_egame WHERE Wid = ? " +
//         " ORDER BY betDate ASC ";
//     var args = [data.wid];

//     db.act_query('dbclient_w_r', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// };

bettingDao.getUserRevenue_v2 = function (data, cb) {
  try {
    if (
      typeof data.start_date === "undefined" ||
      typeof data.end_date === "undefined" ||
      typeof data.sel_table === "undefined"
    ) {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }

    if (
      ["user_revenue_agent", "user_revenue_player"].indexOf(data.sel_table) > -1 &&
      typeof data.upid === "undefined"
    ) {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }

    var sql_where = []
    let args = [data["currency"]]

    if (typeof data.upid !== "undefined") {
      var sql_where_upid = ""
      if (typeof data.userName == "undefined" || data.userName == "") {
        switch (data.sel_table) {
          case "user_revenue_agent":
            sql_where_upid = " w.HallId = ? "
            break
          case "user_revenue_player":
            sql_where_upid = " w.UpId = ? "
            break
          default:
            if (data.search == "list" && data.upid != -1) {
              sql_where_upid = " w.Cid = ? " //用Hall帳號登入藥能搜尋到自己排除admin
            } else {
              sql_where_upid = " w.UpId = ? "
            }
            break
        }
        args.push(data.upid)
        sql_where.push(sql_where_upid)
      }
    }

    if (typeof data.rate != "undefined") {
      sql_where.push(" w.CryDef = ? ")
      args.push(data.rate)
    }

    //***時間***
    if (typeof data.start_date !== "undefined" && typeof data.end_date !== "undefined") {
      sql_where.push(" (w.AccountDate >= ? AND w.AccountDate <= ? ) ")
      args.push(data.start_date)
      args.push(data.end_date)
    }

    //查詢帳號結果
    if (data.user_cid != undefined && data.userName != "") {
      var sql_user_cid = data.user_cid.length > 0 ? " w.Cid IN (?) " : " w.Cid IN ('') "
      sql_where.push(sql_user_cid)
      if (data.user_cid.length > 0) args.push(data.user_cid)
    }

    //玩家用的幣別
    if (typeof data.downCurrency != "undefined" && data.downCurrency !== "" && data.downCurrency != "Consolidated") {
      sql_where.push(" w.Currency = ? ") //兌換幣別
      args.push(data.downCurrency)
    }

    //開放幣別
    if (typeof data.betCurrency != "undefined" && data.betCurrency) {
      const betCurrency = [...data.betCurrency]
      //Hall add currency setting
      if (betCurrency.length > 1) {
        sql_where.push("w.Currency IN (?)")
        args.push(betCurrency)
      } else {
        sql_where.push("w.Currency = ?")
        args.push(betCurrency)
      }
    }

    if (sql_where.length == 0) {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }

    var sql_sel_field = ""
    var out_sql_sel_field = ""
    switch (data.sel_table) {
      case "user_revenue_hall":
        sql_sel_field += "'' AS HallId, '' AS UpId, '' AS IsDemo "
        out_sql_sel_field += "'' AS HallId, t.UpId AS UpId, '' AS IsDemo "
        break
      case "user_revenue_agent":
        sql_sel_field += "w.HallId,'' AS UpId,'' AS IsDemo "
        out_sql_sel_field += "t.HallId,t.UpId,t.IsDemo "
        break
      case "user_revenue_player":
        sql_sel_field += "w.HallId,w.UpId,w.IsDemo "
        out_sql_sel_field += "t.HallId,t.UpId,t.IsDemo "
        break
    }

    var sortKey = "id"
    let outGroupByText = data.downCurrency == "Consolidated" ? "t.id" : "t.id, t.Currency, t.ExCurrency"
    if (typeof data.tabName !== "undefined" && data.tabName == "everyday") {
      outGroupByText = "DATE_FORMAT(t.AccountDate, '%Y-%m-%d')"
      sortKey = "accountDate"
    }
    //幣別匯率
    let cryDefText = `ORDER BY cusRate.EnableTime DESC LIMIT 0,1`
    let andCryDefEnableTime = `AND cusRate.EnableTime <= w.AccountDate`
    let defaultCurrency = pomelo.app.get("sys_config").main_currency
    let CryDef = data.downCurrency == "Consolidated" ? "t.loginCryDef/t.cusCryDef" : "1"

    var sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        date: "accountDate",
        id: "id",
        username: "UserName",
        currency: "Currency",
        rounds: "SUM(rounds)",
        BaseRounds: "SUM(BaseRounds)",
        betgold: `SUM( t.BetGold     *${CryDef} )`,
        realbetgold: `SUM( t.RealBetGold *${CryDef} )`,
        jprealbetgold: `SUM( t.JPRealBetGold      *${CryDef} )`,
        wingold: `SUM( t.WinGold     *${CryDef} )`,
        jpgold: `SUM( t.JPGold      *${CryDef} )`,
        jpcongoldoriginal: `SUM( t.JPConGoldOriginal      *${CryDef} )`,
        payoutamount: `SUM( (t.WinGold - t.JPGold) *${CryDef} )`,
        netwin: `(SUM(t.RealBetGold) - SUM( (t.WinGold - t.JPGold) )*${CryDef}`,
        rtp: `(SUM(t.WinGold ) / SUM(t.RealBetGold)) *100`,
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)
    let sql_limit = data.tabName != "everyday" ? " LIMIT ? , ? " : "" // 限制筆數
    if (data.tabName != "everyday") {
      if (data.isPage === true) {
        args.push((data.page - 1) * data.pageCount)
        args.push(data.pageCount)
      } else {
        args.push(data.index)
        args.push(data.pageCount)
      }
    }

    let sql_group = "GROUP BY DATE_FORMAT(w.AccountDate, '%Y-%m-%d'), w.Cid, w.Currency, w.ExCurrency"

    //取所有資料和匯率
    let baseSql = `
            SELECT
            ( SELECT cusRate.CryDef FROM game.currency_exchange_rate cusRate WHERE (cusRate.Currency = '${defaultCurrency}' AND cusRate.ExCurrency = ? ${andCryDefEnableTime}) ${cryDefText}) AS loginCryDef,
            ( SELECT cusRate.CryDef FROM game.currency_exchange_rate cusRate WHERE (cusRate.Currency = '${defaultCurrency}' AND cusRate.ExCurrency = w.ExCurrency ${andCryDefEnableTime}) ${cryDefText}) AS cusCryDef,
            DATE_FORMAT(w.AccountDate, '%Y-%m-%d') AS accountDate,
            user.UserName AS UserName, user.IsAg AS userLevel,
            w.Cid AS id, w.Currency, w.ExCurrency,
            SUM( w.Rounds ) AS rounds,
            SUM( w.BaseRounds ) AS BaseRounds,
            SUM( TRUNCATE( w.BetGold /1000, 2 ) ) AS BetGold,
            SUM( TRUNCATE( w.WinGold /1000, 2) ) AS WinGold,
            SUM( TRUNCATE( w.WinGold /1000 , 2 ) ) - SUM( TRUNCATE( w.JPGold /1000 , 2 ) ) AS payoutAmount,
            SUM( TRUNCATE( w.JPGold  /1000 , 2 ) ) AS JPGold,
            SUM( TRUNCATE( w.RealBetGold / 1000 , 2 ) ) AS RealBetGold,
            SUM( TRUNCATE(w.JPRealBetGold / 1000 , 2 )) AS JPRealBetGold,
            SUM( TRUNCATE(w.JPConGoldOriginal / 1000 , 4 )) AS JPConGoldOriginal,
            SUM( TRUNCATE( w.RealBetGold /1000 , 2 ) ) - SUM( TRUNCATE( w.WinGold /1000, 2 ) - TRUNCATE( w.JPGold/1000, 2 ) ) AS NetWin,
            '' AS ROWS,
            ${sql_sel_field}
            FROM ${data.sel_table} w LEFT JOIN game.customer user ON(user.Cid = w.Cid)
            WHERE ${sql_where.join(" AND ")}
            ${sql_group}
        `

    //取筆數和 ID 排序
    var sqlOrder = `
            SELECT SQL_CALC_FOUND_ROWS
                t.accountDate, t.id, t.Currency, t.ExCurrency,
                ${out_sql_sel_field},
                FOUND_ROWS() AS ROWS
            FROM ( ${baseSql} ) t
            GROUP BY ${outGroupByText}
            ${order_by_text}
            ${sql_limit};
        `
    let argsOrder = []
    args.forEach((item) => {
      argsOrder.push(item)
    })

    let sqlROWS = "SELECT FOUND_ROWS() AS ROWS;"
    let argsROWS = []
    db.act_query_multi(
      "dbclient_w_r",
      [baseSql, sqlOrder, sqlROWS],
      [args, argsOrder, argsROWS],
      function (r_code, r_data) {
        if (r_code.code !== code.OK) {
          cb(null, r_code, null)
        } else {
          var data = {
            count: r_data[2][0]["ROWS"],
            info: r_data[0],
            order: r_data[1],
          }
          cb(null, r_code, data)
        }
      }
    )
  } catch (err) {
    logger.error("[bettingDao][getUserRevenue_v2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/**
 * 母單列表-設分頁
 *
 * @param {object} data
 * @param {callBack} cb
 * @returns
 */
bettingDao.getList_BetHistory_v2 = function (data, cb) {
  try {
    if (typeof data.id === "undefined") {
      cb(null, { code: code.FAIL, msg: null })
      return
    }
    const parameter = Object.assign({}, data)

    let sql_where = []
    let argsWhere = []
    if (typeof parameter.id !== "undefined" && parameter.id !== "") {
      sql_where.push(" w.Cid = ? ")
      argsWhere.push(parameter.id)
    }

    if (typeof parameter.gameId !== "undefined") {
      sql_where.push(" w.GameId = ? ")
      argsWhere.push(parameter.gameId)
    }

    if (typeof parameter.start_date !== "undefined" && typeof parameter.end_date !== "undefined") {
      sql_where.push(" (w.AddDate >= ? AND w.AddDate <= ? ) ")
      argsWhere.push(parameter.start_date)
      argsWhere.push(parameter.end_date)
    }

    //只撈取已成單的單
    sql_where.push(" w.IsValid = 1 ")

    if (sql_where.length == 0) {
      cb(null, { code: code.FAIL, msg: null })
      return
    }
    let sortKey = "t.betDate"
    let sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      let sort_info = {
        wid: "w.wid",
        cycleid: "w.CycleId",
        userid: "w.userId",
        membername: "w.memberName",
        gameid: "w.gameId",
        bet: "w.bet",
        realbetgold: "w.realBetGold",
        win: "w.win",
        netwin: "w.netWin",
        currency: "w.currency", //兌換幣別
        crydef: "w.cryDef",
        isdemo: "w.isDemo",
        betdate: "w.AddDate",
        ggid: "w.GGId",
        gamestate: "w.gameState",
        jpgold: "w.jpGold",
        jpcongoldoriginal: "w.jpConGoldOriginal",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    //處理當投注時間會多新增注單排序
    let order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)
    if (sortKey == "t.betDate") {
      if (sortType == "DESC") {
        order_by_text = " ORDER BY w.AddDate DESC,w.wid DESC"
      } else {
        order_by_text = " ORDER BY w.AddDate ASC,w.wid ASC"
      }
    }

    const page = Number(data.page)
    const rowsPerPage = Number(data.pageCount)
    const index = (page - 1) * rowsPerPage

    //限制筆數
    var sql_limit =
      data.isPage === true
        ? " LIMIT " + (data.page - 1) * data.pageCount + "," + data.pageCount
        : " LIMIT  " + index + "," + data.pageCount

    let defaultCurrency = pomelo.app.get("sys_config").main_currency

    let sql_loginCryDef_Where =
      " WHERE (cusRate.Currency = ? AND cusRate.ExCurrency = ? AND cusRate.EnableTime <= w.AddDate) ORDER BY cusRate.EnableTime DESC LIMIT 0,1)  AS loginCryDef, "
    let args_loginCryDef_Where = [defaultCurrency, data["currency"]]

    let sql_custCryDef_Where =
      " WHERE (cusRate.Currency = ? AND cusRate.ExCurrency = w.ExCurrency AND cusRate.EnableTime <= w.AddDate) ORDER BY cusRate.EnableTime DESC LIMIT 0,1)  AS cusCryDef, "
    let args_custCryDef_Where = [defaultCurrency]

    let sql = `SELECT SQL_CALC_FOUND_ROWS   
                        ( SELECT cusRate.CryDef FROM game.currency_exchange_rate cusRate ${sql_loginCryDef_Where}
                        ( SELECT cusRate.CryDef FROM game.currency_exchange_rate cusRate ${sql_custCryDef_Where}
                        w.UpId as agentId, w.HallId as hallId, w.Wid AS wid, w.roundID AS roundId, w.Cid AS userId,w.UserName AS memberName,w.GameId AS gameId,
                        TRUNCATE(w.BetGold /1000,2) AS bet,
                        TRUNCATE(w.WinGold /1000,2) AS win,
                        TRUNCATE( (w.WinGold /1000 - w.JPGold/1000),2 ) AS payoutAmount,                 
                        TRUNCATE( w.RealBetGold /1000,2 ) - TRUNCATE( (w.WinGold /1000 - w.JPGold/1000),2 ) AS netWin,
                        w.IsFreeGame AS isFree, w.IsBonusGame AS isBonus, w.CycleId AS cycleId,w.IsValid AS isValid,
                        TRUNCATE(w.JPGold /1000,2) AS jpGold,
                        TRUNCATE(w.JPConGoldOriginal / 1000 , 4) AS jpConGoldOriginal,
                        TRUNCATE(w.RealBetGold /1000,2) AS realBetGold,
                        IF(w.JPConGoldOriginal > 0, TRUNCATE(w.RealBetGold /1000,2), 0) AS jpRealBetGold,
                        w.Currency AS currency,w.Currency AS exCurrency, w.CryDef AS cryDef, 
                        w.IsDemo AS isDemo,w.ExtraTriggerType AS extraTriggerType, DATE_FORMAT(w.AddDate,'%Y-%m-%d %H:%i:%s' ) AS betDate,w.GGId AS GGId, 
                        CASE  WHEN  w.IsFreeGame =1 THEN 'IsFree'  WHEN  w.IsBonusGame =1 THEN 'IsBonus'  ELSE 'IsBase' END AS gameState
                        FROM wagers_1.wagers_bet w
                        WHERE ${sql_where.join(" AND ")}
                    ${order_by_text} ${sql_limit}`
    let args = [...args_loginCryDef_Where, ...args_custCryDef_Where, ...argsWhere]

    let sql2 = "SELECT FOUND_ROWS() AS ROWS;"
    let args2 = []

    db.act_query_multi("dbclient_w_r", [sql, sql2], [args, args2], function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        let data = {
          count: r_data[1][0]["ROWS"],
          info: r_data[0],
        }
        cb(null, r_code, data)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][getList_BetHistory_v2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/**
 * 匯率轉換報表
 * @param {*} data
 * @param {*} cb
 */
bettingDao.get_Currency_Convert_Revenue = function (data, cb) {
  try {
    logger.info("get_Currency_Convert_Revenue data", data)

    if (typeof data.start_date === "undefined" || typeof data.end_date === "undefined") {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }

    // 特例的報表
    const isPlayerTable = data.sel_table.toLowerCase().indexOf("player") > -1
    const isGameRevenueHall = data.sel_table.toLowerCase().indexOf("game_revenue_hall") > -1

    // 當沒傳資料時預設開啟
    const isActiveAdminPlayerDisplay = data.isActiveAdminPlayerDisplay === false ? false : true

    // 選擇玩家層級報表且為Admin登入時
    const adminPlayerTable = isActiveAdminPlayerDisplay === true && isPlayerTable && data.user_level == 1

    const isConsolidated = data.downCurrency == "Consolidated" ? true : false
    let sql_where = []
    let args = []
    let args_order_ROWS = []
    let args_order = []

    //#region select data
    let select_data = {
      cid: "w.Cid AS id",
      username: isPlayerTable ? "w.UserName AS UserName" : "user.UserName AS UserName",
      userlevel: "user.IsAg AS userLevel",
      gameid: "w.GameId AS gameId",
      gamenamec: "g.NameC",
      gamenamee: "g.NameE",
      gamenameg: "g.NameG",
      ggid: "w.GGId",
      currency: "w.Currency AS Currency",
      isdemo: "w.IsDemo AS IsDemo",
      rounds: "SUM( w.Rounds ) AS rounds",
      baserounds: "SUM( w.BaseRounds ) AS BaseRounds",
    } //SQL 語法 key 值一定要小寫

    let select = "*,"
    let selectArray = []
    let leftJoinGames = "" //取遊戲名
    let leftJoinCustomer = "" //取帳號資料
    if (data["select"] != "" && data["select"] !== undefined) {
      data["select"].forEach((item) => {
        let selectKey = item.toLowerCase() //SQL 語法 key 值強轉小寫
        if (selectKey != "uniplayers") selectArray.push(select_data[selectKey])
        if ((selectKey == "username" && !isPlayerTable) || selectKey == "userlevel")
          leftJoinCustomer = `LEFT JOIN game.customer user ON (w.Cid = user.Cid)`
        if (selectKey.indexOf("gamename") > -1) leftJoinGames = `LEFT JOIN game.games g ON (w.GameId = g.gameId)`
      })
      select = selectArray.join(",") + ","
    }

    //#region 玩家人數
    let sql_where_unitPlayer = []
    let isUnitPlayer = data["select"].some((item) => item.toLowerCase() == "uniplayers")
    let selectUniPlayers = ""
    if (isUnitPlayer) {
      if (
        typeof data.start_date !== "undefined" &&
        data.start_date.length > 0 &&
        typeof data.end_date !== "undefined" &&
        data.end_date.length > 0
      ) {
        sql_where_unitPlayer.push("( u.AccountDate >= ? AND u.AccountDate <= ? )")
        args.push(data.start_date)
        args.push(data.end_date)
      }
      if (data.tableUpIdName != undefined && data.upId != undefined) {
        if (!adminPlayerTable && !isGameRevenueHall)
          sql_where_unitPlayer.push(` u.${data.tableUpIdName} IN ( '${data.upId.join("','")}' ) `)
      }
      if (sql_where_unitPlayer.length === 0) {
        cb(null, { code: code.FAIL, msg: data }, null)
        return
      }
      let gameId = data.uniPlayersGameId ? `w.GameId = u.GameId AND ` : ""
      selectUniPlayers = `(SELECT count(DISTINCT u.Cid) FROM wagers_1.game_revenue_player u
            WHERE (${gameId} w.Currency = u.Currency AND ${sql_where_unitPlayer.join(" AND ")})) AS uniPlayers,`
    }
    //#endregion 玩家人數

    //#region 合併幣別匯率
    let cryDefText = `ORDER BY cusRate.EnableTime DESC LIMIT 0,1`
    let andCryDefEnableTime = `AND cusRate.EnableTime <= w.AccountDate`
    let defaultCurrency = pomelo.app.get("sys_config").main_currency
    var setBetCurrency = []
    let CryDef = isConsolidated ? "TRUNCATE(t.loginCryDef/t.cusCryDef,6)" : "1"

    //帳號投注幣別
    if (typeof data.betCurrency != "undefined" && data.betCurrency && isConsolidated) {
      const betCurrency = [...data.betCurrency]
      //Hall add currency setting
      if (betCurrency.length > 1) {
        setBetCurrency.push(` AND w.Currency IN ( '${betCurrency.join("','")}' ) `)
      } else {
        setBetCurrency.push(` AND w.Currency = ('${betCurrency}')`)
      }
    }

    let selectCryDef = ""
    if (isConsolidated) {
      selectCryDef = `( SELECT cusRate.CryDef FROM game.currency_exchange_rate cusRate WHERE (cusRate.Currency = '${defaultCurrency}' AND cusRate.ExCurrency = ? ${andCryDefEnableTime}) ${cryDefText}) AS loginCryDef, 
            ( SELECT cusRate.CryDef FROM game.currency_exchange_rate cusRate WHERE (cusRate.Currency = '${defaultCurrency}' AND cusRate.ExCurrency = w.ExCurrency ${andCryDefEnableTime}) ${cryDefText}) AS cusCryDef,`
      args.push(data["currency"])
    }
    //#endregion 合併幣別匯率
    //#endregion select data

    //#region sql_where
    //AccountDate
    if (typeof data.start_date !== "undefined" && typeof data.end_date !== "undefined") {
      sql_where.push("( w.AccountDate >= ? AND w.AccountDate < ? )")
      args.push(data.start_date)
      args.push(data.end_date)
    }

    //haId, agId
    if (data.tableUpIdName != undefined && data.upId != undefined) {
      let sql_where_upId = ` w.${data.tableUpIdName} IN ( '${data.upId.join("','")}' ) `
      if (!adminPlayerTable) sql_where.push(sql_where_upId)
    }

    //帳號搜尋
    if (typeof data.userId != "undefined") {
      sql_where.push(` w.Cid IN( '${data.userId.join("','")}' ) `)
    }
    //帳號名稱搜尋
    if (typeof data.userName != "undefined" && isPlayerTable) {
      sql_where.push(` w.userName like ? `)
      args.push("%" + data.userName + "%")
    }
    //遊戲名稱搜尋
    if (data.search_game == true) {
      sql_where.push(` w.GameId IN ( '${data.search_gameId.join("','")}' ) `)
    }
    //遊戲編號
    if (typeof data.gameId !== "undefined" && data.gameId != "") {
      sql_where.push(" w.GameId = ? ")
      args.push(data.gameId)
    }
    //遊戲種類
    if (typeof data.ggId != "undefined" && data.ggId !== "") {
      sql_where.push(" w.GGId = ? ")
      args.push(data.ggId)
    }
    //開放幣別
    if (typeof data.betCurrency != "undefined" && data.betCurrency && !isConsolidated) {
      const betCurrency = [...data.betCurrency]
      //Hall add currency setting
      if (betCurrency.length > 1) {
        sql_where.push(` w.Currency IN ( '${betCurrency.join("','")}' ) `)
      } else {
        sql_where.push(`w.Currency = ('${betCurrency}')`)
      }
    }
    //點選投注幣別
    if (typeof data.downCurrency != "undefined" && data.downCurrency !== "" && !isConsolidated) {
      sql_where.push(" w.Currency = ? ")
      args.push(data.downCurrency)
    }
    if (sql_where.length == 0) {
      cb(null, { code: code.FAIL, msg: err }, null)
      return
    }
    if (typeof data.start_date === "undefined" || typeof data.end_date === "undefined") {
      cb(null, { code: code.DB.LOAD_DATA_FAIL }, null)
      return
    }
    //#endregion sql_where

    //#region order by
    let table = isConsolidated ? "t" : "w"
    let sortKey = `(SUM(${table}.BetGold)-SUM(${table}.WinGold-${table}.JPGold))`
    let sortType = conf.SORT_TYPE[1]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      let sort_info = {
        id: `${table}.Cid`, //修正玩家報表編號排序
        username: `${table}.UserName`, //修正玩家報表帳號排序
        gameid: `${table}.GameId`,
        groupname: `${table}.GGId  `,
        namec: `NameC`,
        nameg: `NameG`,
        namee: `NameE`,
        uniplayers: isConsolidated ? `SUM(${table}.uniPlayers )` : `uniPlayers`,
        currency: `${table}.Currency`,
        rounds: `SUM(${table}.Rounds )`,
        BaseRounds: `SUM(${table}.BaseRounds )`,
        betgold: `SUM( TRUNCATE( TRUNCATE(${table}.BetGold /1000, 2 ) * ${CryDef}, 2))`,
        jpgold: `SUM( TRUNCATE( TRUNCATE(${table}.JPGold  /1000, 2 ) * ${CryDef}, 2))`,
        realbetgold: `SUM( TRUNCATE( TRUNCATE(${table}.RealBetGold /1000, 2 ) * ${CryDef}, 2))`,
        jprealbetgold: `SUM( TRUNCATE( TRUNCATE(${table}.JPRealBetGold /1000, 2 ) * ${CryDef}, 2))`,
        jpcongoldoriginal: `SUM((${table}.JPConGoldOriginal /1000) * ${CryDef})`,
        wingold: `SUM( TRUNCATE( TRUNCATE(${table}.WinGold /1000, 2 ) * ${CryDef}, 2))`,
        payoutamount: `SUM( TRUNCATE( TRUNCATE( TRUNCATE(${table}.WinGold /1000, 2 ) * ${CryDef}, 2) - TRUNCATE( TRUNCATE(${table}.JPGold /1000, 2 ) * ${CryDef}, 2 ), 2) )`,
        netwin: `SUM( TRUNCATE( TRUNCATE(${table}.BetGold /1000, 2 ) * ${CryDef}, 2)) - SUM( TRUNCATE( TRUNCATE( TRUNCATE(${table}.WinGold /1000, 2 ) * ${CryDef}, 2) - TRUNCATE( TRUNCATE(${table}.JPGold /1000, 2 ) * ${CryDef}, 2 ), 2) )`,
        rtp: `(SUM(${table}.WinGold /1000 *${CryDef}) / SUM(${table}.BetGold /1000 *${CryDef}))*100`,
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }

    let order_by_text = ` ORDER BY ${sortKey} ${sortType} `
    //#endregion

    //#region 頁數限制
    let sql_limit = data.page && (data.index || data.pageCount) ? " LIMIT ? , ? " : ""
    if (isConsolidated) {
      args.forEach((item) => {
        args_order.push(item)
      })
      if (data.isPage === true) {
        args_order.push((data.page - 1) * data.pageCount)
      } else {
        args_order.push(data.index)
      }
      args_order.push(data.pageCount)
    } else {
      if (data.isPage === true) {
        args.push((data.page - 1) * data.pageCount)
      } else {
        args.push(data.index)
      }
      args.push(data.pageCount)
    }
    //#endregion

    //取原資料
    let SQL_CALC_FOUND_ROWS = !isConsolidated ? "SQL_CALC_FOUND_ROWS" : ""
    let innerGroupBy = data["innerGroupBy"].join(",")
    let inner_group_By_Text = data["innerGroupBy"].length > 0 ? "GROUP BY " + innerGroupBy : ""
    let inner_order_by_text = !isConsolidated ? order_by_text : ""
    let inner_sql_limit = !isConsolidated ? sql_limit : ""
    let sql = `SELECT ${SQL_CALC_FOUND_ROWS} ${select} ${selectUniPlayers} ${selectCryDef}
        SUM( TRUNCATE( w.BetGold / 1000, 2 ) ) AS BetGold,
        SUM( TRUNCATE( w.WinGold / 1000, 2 ) ) AS WinGold,
        SUM( TRUNCATE( w.JPGold  / 1000, 2 ) ) AS JPGold,
        SUM( TRUNCATE( w.RealBetGold  / 1000, 2 ) ) AS RealBetGold,
        SUM( TRUNCATE( w.JPRealBetGold  / 1000, 2 ) ) AS JPRealBetGold,
        SUM( TRUNCATE( w.JPConGoldOriginal  / 1000  , 4 )) AS JPConGoldOriginal
        FROM ${data.sel_table} w
        ${leftJoinCustomer}
        ${leftJoinGames}
        WHERE 1 AND ${sql_where.join(" AND ")}
        ${setBetCurrency}
        ${inner_group_By_Text}
        ${inner_order_by_text}
        ${inner_sql_limit}
        `

    //合併幣別時取筆數和 ID 排序
    let outSelect = data["outGroupBy"].join(",")
    let outGroupByText = data["outGroupBy"].length > 0 ? "GROUP BY " + outSelect : ""
    let sql_order = `
            SELECT SQL_CALC_FOUND_ROWS
            ${outSelect}
            FROM ( ${sql} ) t
            ${outGroupByText}
            ${order_by_text}
            ${sql_limit};
        `
    let sqlROWS = "Select FOUND_ROWS() AS ROWS;"

    //合併幣別 : 一般
    let sqlArray = isConsolidated ? [sql, sql_order, sqlROWS] : [sql, sqlROWS]
    let argsArray = isConsolidated ? [args, args_order, args_order_ROWS] : [args, args_order_ROWS]

    db.act_query_multi("dbclient_w_r", sqlArray, argsArray, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        let data = {}
        if (r_data.length > 2) {
          //合併幣別
          data = {
            count: r_data[2][0]["ROWS"],
            info: r_data[0],
            order: r_data[1],
          }
        } else {
          data = {
            count: r_data[1][0]["ROWS"],
            info: r_data[0],
            order: r_data[0],
          }
        }
        cb(null, r_code, data)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][get_Currency_Convert_Revenue] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//遊戲營收 - 遊戲list
bettingDao.getGameRevenue_games_v2 = function (data, cb) {
  try {
    console.log("getGameRevenue_games_v2", JSON.stringify(data))

    if (typeof data.start_date === "undefined" || typeof data.end_date === "undefined") {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }

    var sql_where = []
    var args = [data["currency"]]
    var args_order_ROWS = []
    var args_order = []

    var sql_where_unitPlayer = []
    if (
      typeof data.users.start_date !== "undefined" &&
      data.users.start_date.length > 0 &&
      typeof data.users.end_date !== "undefined" &&
      data.users.end_date.length > 0
    ) {
      sql_where_unitPlayer.push("( u.AccountDate >= ? AND u.AccountDate <= ? )")
      args.push(data.users.start_date)
      args.push(data.users.end_date)
    }
    if (typeof data.users.gamesId !== "undefined" && data.users.gamesId.length > 0) {
      sql_where_unitPlayer.push(" u.GameId IN ( ? ) ")
      args.push(data.gamesId.join("','"))
    }
    switch (data.users.level) {
      case 1: //AD (hall層)
        break
      case 2: //HA (agent層)
        sql_where_unitPlayer.push(" u.HallId = ? ")
        args.push(data.users.hallId)
        break
      case 3: //AG (player層)
        sql_where_unitPlayer.push(" u.UpId = ? ")
        args.push(data.users.agentId)
        break
    }
    if (sql_where_unitPlayer.length === 0) {
      cb(null, { code: code.FAIL, msg: data }, null)
      return
    }

    if (typeof data.start_date !== "undefined" && typeof data.end_date !== "undefined") {
      sql_where.push("( w.AccountDate >= ? AND w.AccountDate < ? )")
      args.push(data.start_date)
      args.push(data.end_date)
    }

    //haId, agId
    if (typeof data.upid != "undefined") {
      var sql_where_upId = ""
      if (data.user_level === 2) {
        sql_where_upId = " w.HallId = ? "
      }
      if (data.user_level === 3) {
        sql_where_upId = " w.UpId = ? "
      }
      sql_where.push(sql_where_upId)
      args.push(data.upid)
    }

    if (typeof data.search_user != "undefined" && data.search_user == true && typeof data.userId != "undefined") {
      //有使用帳號搜尋
      sql_where.push(" w.Cid IN( ? ) ")
      args.push(data.userId.join("','"))
    }

    //玩家遊戲幣別(兌換幣別)
    if (typeof data.downCurrency != "undefined" && data.downCurrency !== "" && data.downCurrency != "Consolidated") {
      sql_where.push(" w.Currency = ? ")
      args.push(data.downCurrency)
    }

    if (typeof data.ggId != "undefined" && data.ggId !== "") {
      sql_where.push(" w.GGId = ? ")
      args.push(data.ggId)
    }

    if (data.search_game == true) {
      sql_where.push(" w.GameId IN ( ? )")
      args.push(data.search_gameId.join("','"))
    }
    if (sql_where.length == 0) {
      cb(null, { code: code.FAIL, msg: err }, null)
      return
    }

    if (
      typeof data.users.start_date === "undefined" ||
      typeof data.users.end_date === "undefined" ||
      typeof data.users.gamesId === "undefined"
    ) {
      cb(null, { code: code.DB.LOAD_DATA_FAIL }, null)
      return
    }

    //幣別匯率
    let cryDefText = `ORDER BY cusRate.EnableTime DESC LIMIT 0,1`
    let andCryDefEnableTime = `AND cusRate.EnableTime <= w.AccountDate`
    let defaultCurrency = pomelo.app.get("sys_config").main_currency
    let CryDef = data.downCurrency == "Consolidated" ? "t.loginCryDef/t.cusCryDef" : "1"

    var sortKey = "(SUM(t.BetGold)-SUM(t.WinGold-t.JPGold))"
    var sortType = conf.SORT_TYPE[1]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        gameid: "t.GameId",
        groupname: "t.GGId",
        namec: "t.NameC",
        nameg: "t.NameG",
        namee: "t.NameE",
        rounds: "Rounds",
        BaseRounds: "BaseRounds",
        currency: "Currency",
        betgold: `SUM( TRUNCATE(t.BetGold     *${CryDef},2) )`,
        effectivebet: `SUM( TRUNCATE(t.effectiveBet*${CryDef},2) )`,
        wingold: `SUM( TRUNCATE(t.WinGold     *${CryDef},2) )`,
        netwin: `SUM( TRUNCATE(t.BetGold     *${CryDef},2) ) -
                                SUM( TRUNCATE(t.WinGold     *${CryDef},2)-
                                TRUNCATE(t.JPGold      *${CryDef},2) )
                              `,
        jpgold: `SUM( TRUNCATE(t.JPGold      *${CryDef},2) )`,
        rtp: `(SUM(TRUNCATE(t.WinGold     *${CryDef},2))/
                               SUM(TRUNCATE(t.BetGold     *${CryDef},2)) )*100`,
        uniplayers: "uniPlayers",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }

    var order_by_text = ` ORDER BY ${sortKey} ${sortType} `

    var sql_limit = " LIMIT ? , ? "

    args.forEach((item) => {
      args_order.push(item)
    })
    if (data.isPage === true) {
      args_order.push((data.page - 1) * data.pageCount)
      args_order.push(data.pageCount)
    } else {
      args_order.push(data.index)
      args_order.push(data.pageCount)
    }

    let groupByText = "GROUP BY w.GameId, w.Currency"

    var sql = `SELECT
        w.GGId, w.Id, w.GameId AS gameId, g.NameC, g.NameG, g.NameE, w.Cid, w.Currency,
        ( SELECT cusRate.CryDef FROM game.currency_exchange_rate cusRate WHERE (cusRate.Currency = '${defaultCurrency}' AND cusRate.ExCurrency = ? ${andCryDefEnableTime}) ${cryDefText}) AS loginCryDef,
        ( SELECT cusRate.CryDef FROM game.currency_exchange_rate cusRate WHERE (cusRate.Currency = '${defaultCurrency}' AND cusRate.ExCurrency = w.ExCurrency ${andCryDefEnableTime}) ${cryDefText}) AS cusCryDef,
        (SELECT count(DISTINCT u.Cid)
        FROM wagers_1.game_revenue_player u
        WHERE (w.GameId = u.GameId AND w.Currency = u.Currency AND ${sql_where_unitPlayer.join(" AND ")} ) 
        ) AS uniPlayers,
        SUM( w.Rounds ) AS rounds,
        SUM( w.BaseRounds ) AS BaseRounds,
        SUM( w.BetGold /1000 )                        AS BetGold,
        SUM( w.BetGold/1000 )                         AS effectiveBet,
        SUM( w.BetPoint/1000 )                        AS BetPoint,
        SUM(w.WinGold /1000 )                         AS WinGold,
        SUM( w.JPPoint /1000 )                        AS JPPoint,
        SUM( w.JPGold  /1000 )                        AS JPGold,
        SUM(w.BetGold/1000) - SUM( (w.WinGold /1000 ) - (w.JPGold    /1000) )  AS NetWin,
        (SUM(w.WinGold /1000) / SUM(w.BetGold/1000) ))*100 AS RTP
        ((SUM(w.WinGold /1000) - SUM( w.JPGold  /1000 )  ) / SUM(w.BetGold/1000) ))*100 AS netRTP
        FROM ${data.sel_table} w
        LEFT JOIN game.games g ON (w.GameId = g.gameId)
        WHERE 1 AND ${sql_where.join(" AND ")}
        ${groupByText}
        `

    //取筆數和 ID 排序
    let outGroupByText = data.downCurrency == "Consolidated" ? "GROUP BY gameId" : "GROUP BY gameId, Currency"
    var sql_order = `
            SELECT SQL_CALC_FOUND_ROWS
                t.loginCryDef, t.cusCryDef, t.GGId, t.gameId, t.Currency, t.NameC, t.NameG, t.NameE,
                SUM(t.uniPlayers) AS uniPlayers,
                SUM( TRUNCATE(t.BetGold     *${CryDef},2) ) AS convertBetGold,
                SUM( TRUNCATE(t.effectiveBet*${CryDef},2) ) AS convertEffectiveBet,
                SUM( TRUNCATE(t.WinGold     *${CryDef},2) -
                     TRUNCATE(t.JPGold      *${CryDef},2) ) AS convertPayoutAmount,

                SUM( TRUNCATE(t.WinGold      *${CryDef},2) ) AS convertWinGold,
                SUM( TRUNCATE(t.JPGold      *${CryDef},2) ) AS convertJPGold,

                SUM( TRUNCATE(t.NetWin      *${CryDef},2) ) AS convertNetWin,

              ( SUM( TRUNCATE(t.WinGold     *${CryDef},2) )/
                SUM( TRUNCATE(t.BetGold     *${CryDef},2) ) )*100 AS RTP

                ( SUM( TRUNCATE(t.WinGold     *${CryDef},2) - SUM( TRUNCATE(t.JPGold     *${CryDef},2) )/
                SUM( TRUNCATE(t.BetGold     *${CryDef},2) ) )*100 AS netRTP                
            FROM ( ${sql} ) t
            ${outGroupByText}
            ${order_by_text}
            ${sql_limit};
        `

    var sqlROWS = "Select FOUND_ROWS() AS ROWS;"
    db.act_query_multi(
      "dbclient_w_r",
      [sql, sql_order, sqlROWS],
      [args, args_order, args_order_ROWS],
      function (r_code, r_data) {
        if (r_code.code !== code.OK) {
          cb(null, r_code, null)
        } else {
          var data = {
            count: r_data[2][0]["ROWS"],
            info: r_data[0],
            order: r_data[1],
          }
          cb(null, r_code, data)
        }
      }
    )
  } catch (err) {
    logger.error("[bettingDao][getGameRevenue_games_v2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//遊戲營收 - 遊戲的玩家人數
bettingDao.getUniquePlayerNums = function (data, cb) {
  try {
    //console.log('-getUniquePlayerNums data -', data);
    if (
      typeof data.start_date === "undefined" ||
      typeof data.end_date === "undefined" ||
      typeof data.gamesId === "undefined"
    ) {
      cb(null, { code: code.DB.LOAD_DATA_FAIL }, null)
      return
    }
    var sql_where = []
    if (
      typeof data.start_date !== "undefined" &&
      data.start_date.length > 0 &&
      typeof data.end_date !== "undefined" &&
      data.end_date.length > 0
    ) {
      sql_where.push("( AccountDate >='" + data.start_date + "' AND AccountDate <='" + data.end_date + "')")
    }
    if (typeof data.gamesId !== "undefined" && data.gamesId.length > 0) {
      sql_where.push(" GameId IN ('" + data.gamesId.join("','") + "') ")
    }
    switch (data.level) {
      case 1: //AD (hall層)
        break
      case 2: //HA (agent層)
        sql_where.push(" HallId ='" + data.hallId + "' ")
        break
      case 3: //AG (player層)
        sql_where.push(" UpId ='" + data.agentId + "' ")
        break
    }
    if (sql_where.length === 0) {
      cb(null, { code: code.FAIL, msg: data }, null)
      return
    }

    var sql =
      " SELECT GameId, Currency, count(DISTINCT Cid) AS num FROM game_revenue_player WHERE " +
      sql_where.join(" AND ") +
      " GROUP BY GameId, Currency"
    var args = []

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][getUniquePlayerNums] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//遊戲營收  (ha ag pr)
bettingDao.getGameRevenue_users_v2 = function (data, cb) {
  try {
    if (
      typeof data.start_date === "undefined" ||
      typeof data.end_date === "undefined" ||
      typeof data.sel_table === "undefined" ||
      typeof data.gameId === "undefined"
    ) {
      cb(null, { code: code.FAIL, msg: err }, null)
      return
    }

    if (
      ["game_revenue_agent", "game_revenue_player"].indexOf(data.sel_table) > -1 &&
      typeof data.upid === "undefined"
    ) {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }

    var sql_where = []
    var args = [data["currency"]]
    var args_order_ROWS = []
    var args_order = []

    if (typeof data.upid !== "undefined") {
      var sql_where_upid = ""
      if (data.sel_table === "game_revenue_agent") {
        sql_where_upid = " w.HallId = ? "
        args.push(data.upid)
      }
      if (data.sel_table === "game_revenue_player") {
        sql_where_upid = " w.UpId = ? "
        args.push(data.upid)
      }
      if (sql_where_upid != "") sql_where.push(sql_where_upid)
    }

    if (typeof data.gameId !== "undefined") {
      sql_where.push(" w.GameId = ? ")
      args.push(data.gameId)
    }

    if (typeof data.start_date !== "undefined" && typeof data.end_date !== "undefined") {
      sql_where.push(" ( w.AccountDate >= ? AND w.AccountDate <= ? ) ")
      args.push(data.start_date)
      args.push(data.end_date)
    }

    //幣別匯率
    let cryDefText = `ORDER BY cusRate.EnableTime DESC LIMIT 0,1`
    let andCryDefEnableTime = `AND cusRate.EnableTime <= w.AccountDate`
    let defaultCurrency = pomelo.app.get("sys_config").main_currency
    let CryDef = data.downCurrency == "Consolidated" ? "t.loginCryDef/t.cusCryDef" : "1"

    //玩家幣別 (兌換幣別欄位)
    if (typeof data.downCurrency != "undefined" && data.downCurrency !== "" && data.downCurrency != "Consolidated") {
      sql_where.push(" w.Currency = ? ")
      args.push(data.downCurrency)
    }

    if (typeof data.cid !== "undefined" && data.cid != "") {
      sql_where.push(" w.Cid IN (?) ")
      args.push(data.cid)
    }

    if (sql_where.length == 0) {
      cb(null, { code: code.FAIL, msg: err }, null)
      return
    }

    var sql_sel_field = ""

    switch (data.sel_table) {
      case "game_revenue_hall":
        sql_sel_field += ",'' AS HallId, '' AS UpId, '' AS IsDemo "
        break
      case "game_revenue_agent":
        sql_sel_field += ",w.HallId,'' AS UpId,'' AS IsDemo "
        break
      case "game_revenue_player":
        sql_sel_field += ",w.HallId,w.UpId,w.IsDemo,w.UserName "
        break
    }

    var sortKey = `SUM( TRUNCATE(t.BetGold *${CryDef},2) )`
    var sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        id: "Cid",
        gameid: "GameId",
        gamename: "GameId",
        username: "UserName",
        currency: "Currency",
        rounds: "SUM( t.rounds )",
        BaseRounds: "SUM( t.BaseRounds )",
        betgold: `SUM( TRUNCATE(t.BetGold     *${CryDef},2) )`,
        effectivebet: `SUM( TRUNCATE(t.effectiveBet*${CryDef},2) )`,

        wingold: `SUM( TRUNCATE(t.WinGold     *${CryDef},2) )`,

        payoutamount: `SUM( TRUNCATE(t.WinGold     *${CryDef},2)-
                TRUNCATE(t.JPGold      *${CryDef},2) )`,

        netwin: `SUM( TRUNCATE(t.NetWin      *${CryDef},2) )`,
        jpgold: `SUM( TRUNCATE(t.JPGold      *${CryDef},2) )`,

        rtp: `(SUM(TRUNCATE(t.WinGold     *${CryDef},2))/
                                SUM(TRUNCATE(t.BetGold     *${CryDef},2)) )*100`,

        netrtp: `(SUM(TRUNCATE(t.WinGold *${CryDef},2) ) - SUM(TRUNCATE(t.JPGold *${CryDef},2) )/
                    SUM(TRUNCATE(t.BetGold     *${CryDef},2)))*100`,
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    var order_by_text = ` ORDER BY ${sortKey} ${sortType} `

    var sql_limit = " LIMIT ? , ? "

    args.forEach((item) => {
      args_order.push(item)
    })
    if (data.isPage === true) {
      args_order.push((data.page - 1) * data.pageCount)
      args_order.push(data.pageCount)
    } else {
      args_order.push(data.index)
      args_order.push(data.pageCount)
    }

    let groupByText = "GROUP BY w.id, w.Currency"

    var sql = `SELECT
        w.GGId, user.UserName, user.Cid AS id, w.GameId AS gameId, g.NameC, g.NameG, g.NameE, w.Cid, w.Currency,
        ( SELECT cusRate.CryDef FROM game.currency_exchange_rate cusRate WHERE (cusRate.Currency = '${defaultCurrency}' AND cusRate.ExCurrency = ? ${andCryDefEnableTime}) ${cryDefText}) AS loginCryDef,
        ( SELECT cusRate.CryDef FROM game.currency_exchange_rate cusRate WHERE (cusRate.Currency = '${defaultCurrency}' AND cusRate.ExCurrency = w.ExCurrency ${andCryDefEnableTime}) ${cryDefText}) AS cusCryDef,
        SUM( w.Rounds ) AS rounds,
        SUM( w.BetGold /1000 )                        AS BetGold,
        SUM( w.BetGold/1000 )                         AS effectiveBet,
        SUM( w.BetPoint/1000 )                        AS BetPoint,
        SUM((w.WinGold /1000 ) - (w.JPGold    /1000)) AS payoutAmount,
        SUM(w.WinGold /1000 )                         AS WinGold,
        SUM( w.JPPoint /1000 )                        AS JPPoint,
        SUM( w.JPGold  /1000 )                        AS JPGold,
        SUM(w.BetGold/1000) - SUM( w.WinGold /1000 )  AS NetWin,
        (SUM(w.WinGold /1000) / SUM(w.BetGold/1000) )*100 AS RTP
        (SUM(w.WinGold /1000)- SUM(w.JPGold /1000) / SUM(w.BetGold/1000) )*100 AS netRTP
        FROM ${data.sel_table} w
        LEFT JOIN game.games g ON (w.GameId = g.gameId)
        LEFT JOIN game.customer user ON(w.Cid = user.Cid)
        WHERE 1 AND ${sql_where.join(" AND ")}
        ${groupByText}
        `

    //取筆數和 ID 排序
    let outGroupByText = data.downCurrency == "Consolidated" ? "GROUP BY id" : "GROUP BY id, Currency"
    var sql_order = `
            SELECT SQL_CALC_FOUND_ROWS
                t.id, t.UserName, t.loginCryDef, t.cusCryDef, t.GGId, t.gameId, t.Currency, t.rounds, t.NameC, t.NameG, t.NameE,
                SUM( t.rounds ) AS rounds,

                SUM( TRUNCATE(t.BetGold     *${CryDef},2) ) AS convertBetGold,
                SUM( TRUNCATE(t.effectiveBet*${CryDef},2) ) AS convertEffectiveBet,
                SUM( TRUNCATE(t.WinGold     *${CryDef},2) -
                     TRUNCATE(t.JPGold      *${CryDef},2) ) AS convertPayoutAmount,

                SUM( TRUNCATE(t.WinGold      *${CryDef},2) ) AS convertWinGold,
                SUM( TRUNCATE(t.JPGold      *${CryDef},2) ) AS convertJPGold,

                SUM( TRUNCATE(t.NetWin      *${CryDef},2) ) AS convertNetWin,

              ( SUM( TRUNCATE(t.WinGold     *${CryDef},2) )/
                SUM( TRUNCATE(t.BetGold     *${CryDef},2) ) )*100 AS RTP

                ( SUM( TRUNCATE(t.WinGold     *${CryDef},2) - SUM( TRUNCATE(t.JPGold     *${CryDef},2) )/
                SUM( TRUNCATE(t.BetGold     *${CryDef},2) ) )*100 AS netRTP     
            FROM ( ${sql} ) t
            ${outGroupByText}
            ${order_by_text}
            ${sql_limit};
        `
    var sqlROWS = "Select FOUND_ROWS() AS ROWS;"

    db.act_query_multi(
      "dbclient_w_r",
      [sql, sql_order, sqlROWS],
      [args, args_order, args_order_ROWS],
      function (r_code, r_data) {
        if (r_code.code !== code.OK) {
          cb(null, r_code, null)
        } else {
          var data = {
            count: r_data[2][0]["ROWS"],
            info: r_data[0],
            order: r_data[1],
          }
          cb(null, r_code, data)
        }
      }
    )
  } catch (err) {
    logger.error("[bettingDao][getGameRevenue_users_v2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/**
 * 押注紀錄-母單列表-設分頁
 * 【遊戲投注紀錄】
 *
 * @param {object} data
 * @param {callBack} cb
 * @returns
 */
bettingDao.getList_BetHistory_v3 = function (data, cb) {
  try {
    let parameter = Object.assign({}, data)
    let sql_where = []
    let argsWhere = []

    let searchId = "t.haId"

    switch (parameter.level) {
      case "HA":
        if (typeof parameter.subResellers != "undefined") {
          let sql_Hall_id = `(w.HallId IN('${parameter.subResellers.join("','")}'))`
          sql_where.push(sql_Hall_id)
        }
        searchId = "t.agId"
        break
      case "AG":
        sql_where.push(" w.UpId = ? ")
        argsWhere.push(parameter.upid)
        break
      case "PR":
        sql_where.push(" w.Cid = ? ")
        argsWhere.push(parameter.playerId)
        break
    }
    //admin搜尋hall
    if (typeof parameter.hall_Id != "undefined") {
      let sql_user_id = ""
      if (parameter.hall_Id.length == 0) {
        sql_user_id = " w.HallId = ? "
        argsWhere.push("")
      } else {
        sql_user_id = " w.HallId IN(?) "
        argsWhere.push(parameter.hall_Id)
      }
      sql_where.push(sql_user_id)
    }
    //hall搜尋agent
    if (typeof parameter.agent_Id != "undefined") {
      let sql_user_id = ""
      if (parameter.agent_Id.length == 0) {
        sql_user_id = " w.UpId = ? "
        argsWhere.push("")
      } else {
        sql_user_id = " w.UpId IN(?) "
        argsWhere.push(parameter.agent_Id)
      }
      sql_where.push(sql_user_id)
    }
    //hall搜尋agent
    if (typeof parameter.upIds != "undefined") {
      let sql_user_id = ""
      if (parameter.upIds.length == 0) {
        sql_user_id = " w.UpId = ? "
        argsWhere.push("")
      } else {
        sql_user_id = ` (w.UpId IN('${parameter.upIds.join("','")}') OR w.HallId IN('${parameter.upIds.join("','")}')) `
      }
      sql_where.push(sql_user_id)
    }

    //會員名稱
    if (typeof parameter.playerName != "undefined" && parameter.playerName != "") {
      sql_where.push(" w.UserName  LIKE ?")
      argsWhere.push("%" + parameter.playerName + "%")
    }

    //遊戲編號
    if (typeof parameter.gameIdAry != "undefined" && parameter.gameIdAry.length > 0) {
      sql_where.push(" w.GameId IN(?) ")
      argsWhere.push(parameter.gameIdAry)
    }

    if (typeof parameter.gameId != "undefined" && parameter.gameId != "") {
      sql_where.push(" w.GameId = ? ")
      argsWhere.push(parameter.gameId)
    }

    //遊戲名稱
    if (isEmpty(parameter.search_gameId) === false && parameter.search_gameId.length > 0) {
      if (parameter.search_gameId.length > 1) {
        sql_where.push(`w.GameId IN ('${parameter.search_gameId.join("','")}')`)
      } else {
        sql_where.push(`w.GameId = ('${parameter.search_gameId}')`)
      }
    }

    //幣別
    if (typeof parameter.currency != "undefined" && parameter.currency.length > 0) {
      sql_where.push(" w.Currency IN (?) ")
      argsWhere.push(parameter.currency)
    }

    //測試 OR 線上
    if (typeof parameter.isDemo != "undefined" && parameter.isDemo.length > 0) {
      sql_where.push(" w.IsDemo IN (?) ")
      argsWhere.push(parameter.isDemo)
    }

    // 注單狀態
    if (parameter.isValid && parameter.isValid[0] == "1") {
      sql_where.push(" w.IsValid = ? ")
      argsWhere.push("1")
    } else if (parameter.isValid && parameter.isValid[0] == "0") {
      sql_where.push(" w.IsValid = ? ")
      argsWhere.push("0")
    }

    if (parameter.select_wager == false) {
      if (typeof parameter.wid != "undefined" && parameter.wid != "") {
        sql_where.push(" w.Wid = ? ")
        argsWhere.push(parameter.wid)
      } else if (typeof parameter.start_date !== "undefined" && typeof parameter.end_date !== "undefined") {
        sql_where.push(" (w.AddDate >= ? AND w.AddDate <= ? ) ")
        argsWhere.push(parameter.start_date)
        argsWhere.push(parameter.end_date)
      }
    } else {
      //注單編號
      sql_where.push(" w.Wid = ?")
      argsWhere.push(parameter.wid)
    }

    // 場次編號
    if (typeof parameter.roundid != "undefined" && parameter.roundid != "") {
      sql_where.push(" w.roundId = ? ")
      argsWhere.push(parameter.roundid)
    }

    if (typeof parameter.ggId != "undefined" && parameter.ggId != "") {
      sql_where.push(" w.GGId = ? ")
      argsWhere.push(parameter.ggId)
    }

    //state:[0:一般遊戲,1:免費遊戲,2:獎勵遊戲]
    if (typeof parameter.state != "undefined" && parameter.state.length > 0) {
      let state_query = []
      parameter.state.forEach((item) => {
        switch (item) {
          case 0:
            state_query.push(" (w.IsFreeGame!=1 AND w.IsBonusGame!=1) ")
            break
          case 1:
            state_query.push(" w.IsFreeGame = 1 ")
            break
          case 2:
            state_query.push(" w.IsBonusGame = 1")
            break
        }
      })

      if (state_query.length > 0) {
        sql_where.push(" ( " + state_query.join(" OR ") + " ) ")
      }
    }
    //result:[lost,win]
    if (typeof parameter.result != "undefined" && parameter.result.length > 0) {
      let result_query = []
      parameter.result.forEach((item) => {
        switch (item) {
          case "win":
            result_query.push(" w.WinGold > 0 ")
            break
          case "lost":
            result_query.push(" w.WinGold <= 0 ")
            break
        }
      })

      if (result_query.length > 0) {
        sql_where.push(" ( " + result_query.join(" OR ") + " ) ")
      }
    }

    if (sql_where.length == 0) {
      cb(null, { code: code.FAIL, msg: null })
      return
    }

    let sortKey = " t.betDate"
    let sortType = conf.SORT_TYPE[0]

    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()

      let sort_info = {
        wid: "wid",
        cycleid: "CycleId",
        membername: "memberName",
        gameid: "gameId",
        betgold: "BetGold",
        realbetgold: "realbetgold",
        jprealbetgold: "jpRealBetGold",
        jpcongoldoriginal: "jpConGoldOriginal",
        win: "win",
        netwin: "netWin",
        payoutamount: "payoutAmount",
        currency: "Currency",
        crydef: "cryDef",
        gamestate: "gameState",
        isdemo: "isDemo",
        ggid: "GGId",
        jpgold: "JPGold",
        roundid: "roundId",
        haname: "user.UserName",
        agname: "user.UserName",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    let order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)

    let sqlLimit = " LIMIT ?,?"
    let argsLimit = []
    if (data.isPage === true) {
      argsLimit.push((data.page - 1) * data.pageCount)
      argsLimit.push(data.pageCount)
    } else {
      argsLimit.push(data.index)
      argsLimit.push(data.pageCount)
    }

    let sql =
      "SELECT SQL_CALC_FOUND_ROWS t.*, user.UserName AS upName FROM ( \
            SELECT w.GGId,w.Wid AS wid, w.roundID as roundId, w.HallId AS haId,w.UpId AS agId, w.Cid AS userId,w.UserName AS memberName,w.GameId AS gameId,\
            (w.BetGold / 1000) AS betGold,\
            (w.WinGold / 1000) AS winGold,(TRUNCATE( RealBetGold /1000, 2 ) - (TRUNCATE( WinGold /1000, 2 ) - TRUNCATE( JPGold /1000, 2 ))) AS netWin,\
            (w.RealBetGold / 1000) AS realBetGold, \
            CASE  WHEN  w.JPConGoldOriginal > 0 THEN TRUNCATE( w.RealBetGold /1000, 2 ) ELSE 0 END AS jpRealBetGold, \
            (w.JPConGoldOriginal / 1000) AS jpConGoldOriginal, \
            (w.WinGold / 1000 - w.JPGold / 1000) AS payoutAmount, \
            (w.JPGold / 1000) AS JPGold,w.IsJP AS isJP, LOWER(w.JPType) AS jpType,\
            w.Currency AS currency,w.ExCurrency AS exCurrency, w.CryDef AS cryDef,w.IsDemo AS isDemo,w.IsFreeGame AS isFree,w.ExtraTriggerType AS extraTriggerType,w.IsBonusGame AS isBonus,w.CycleId AS cycleId,w.IsValid AS isValid,\
            DATE_FORMAT(w.AddDate,'%Y-%m-%d %H:%i:%s' ) AS betDate, w.GameTypeId AS gameTypeId,\
            CASE  WHEN  w.IsFreeGame =1 THEN 'IsFree'  WHEN  w.IsBonusGame =1 THEN 'IsBonus'  ELSE 'IsBase' END AS gameState \
            FROM wagers_bet w " +
      " WHERE " +
      sql_where.join(" AND ") +
      ") t  LEFT JOIN game.customer user ON (user.Cid = ?)" +
      order_by_text +
      sqlLimit
    let args = [...argsWhere, searchId, ...argsLimit]

    let sql2 = "SELECT FOUND_ROWS() AS ROWS;"
    let args2 = []

    db.act_query_multi("dbclient_w_r", [sql, sql2], [args, args2], function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        let data = {
          count: r_data[1][0]["ROWS"],
          info: r_data[0],
        }
        cb(null, r_code, data)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][getList_BetHistory_v3] catch err", err)
    cb(null, code.FAIL, null)
  }
}

bettingDao.getUsersByUsername = function (data, cb) {
  const param = { ...data }
  const sql_where = []

  if (param.userName) {
    const whereSql = ` UserName like '%${param.userName}%'`
    sql_where.push(whereSql)
  } else {
    cb(null, { code: code.DB.PARA_FAIL, msg: "userName - is not found." })
    return
  }

  // user層級
  switch (param.level) {
    case "HA":
      sql_where.push(`IsAg = 2`)
      break
    case "AG":
      sql_where.push(`IsAg = 3`)
      break
    case "PR":
      sql_where.push(`IsAg = 4`)
      break
  }

  /**
   *    SQL
   */
  const sql = sprintf(
    `
      SELECT Cid, Username, Upid, HallId , IsAg 
      FROM customer
      WHERE %s
      `,
    sql_where.join(" AND ")
  )
    .split("\n")
    .join("")

  db.act_transaction("dbclient_g_r", [sql], function (r_code, r_data) {
    if (r_code.code !== code.OK) {
      cb(null, r_code, null)
    } else {
      // convert to native array
      const result = JSON.parse(JSON.stringify(r_data[0]))
      cb(null, r_code, result)
    }
  })
}

//總和
bettingDao.getList_BetHistory_sum = function (data, cb) {
  try {
    var parameter = Object.assign({}, data)
    var sql_where = []
    let argsWhere = []

    switch (parameter.level) {
      case "HA":
        if (typeof parameter.subResellers != "undefined") {
          var sql_Hall_id = `(w.HallId IN('${parameter.subResellers.join("','")}'))`
          sql_where.push(sql_Hall_id)
        }
        break
      case "AG":
        sql_where.push(" w.UpId = ? ")
        argsWhere.push(parameter.upid)
        break
      case "PR":
        sql_where.push(" w.Cid = ? ")
        argsWhere.push(parameter.playerId)
        break
    }

    if (typeof parameter.hall_Id != "undefined") {
      var sql_user_id = ""
      if (parameter.hall_Id.length == 0) {
        sql_user_id = " w.HallId = ? "
        argsWhere.push("")
      } else {
        sql_user_id = " w.HallId IN(?) "
        argsWhere.push(parameter.hall_Id)
      }
      sql_where.push(sql_user_id)
    }

    if (typeof parameter.agent_Id != "undefined") {
      var sql_user_id = ""
      if (parameter.agent_Id.length == 0) {
        sql_user_id = " w.UpId = ? "
        argsWhere.push("")
      } else {
        sql_user_id = " w.UpId IN(?) "
        argsWhere.push(parameter.agent_Id)
      }
      sql_where.push(sql_user_id)
    }

    if (typeof parameter.upIds != "undefined") {
      var sql_user_id = ""
      if (parameter.upIds.length == 0) {
        sql_user_id = "( w.UpId = '' or w.HallId = '' )"
      } else {
        sql_user_id = ` (w.UpId IN('${parameter.upIds.join("','")}') OR w.HallId IN('${parameter.upIds.join("','")}')) `
      }
      sql_where.push(sql_user_id)
    }

    //注單編號
    if (parameter.select_wager == false) {
      if (typeof parameter.wid != "undefined" && parameter.wid != "") {
        sql_where.push(" w.Wid = ? ")
        argsWhere.push("parameter.wid")
      } else if (typeof parameter.start_date !== "undefined" && typeof parameter.end_date !== "undefined") {
        sql_where.push(" (w.AddDate >= ? AND w.AddDate <= ? ) ")
        argsWhere.push(parameter.start_date)
        argsWhere.push(parameter.end_date)
      }
    } else {
      sql_where.push(" w.Wid = ? ")
      argsWhere.push(parameter.wid)
    }

    // 場次編號
    if (typeof parameter.roundid != "undefined" && parameter.roundid != "") {
      sql_where.push(" w.roundid = ? ")
      argsWhere.push(parameter.roundid)
    }

    if (typeof parameter.ggId != "undefined" && parameter.ggId != "") {
      sql_where.push(" w.GGId = ? ")
      argsWhere.push(parameter.ggId)
    }

    //會員名稱
    if (typeof parameter.playerName != "undefined" && parameter.playerName != "") {
      sql_where.push(" w.UserName  LIKE ? ")
      argsWhere.push("%" + parameter.playerName + "%")
    }

    //遊戲編號
    if (typeof parameter.gameIdAry != "undefined" && parameter.gameIdAry.length > 0) {
      sql_where.push(" w.GameId IN(?) ")
      argsWhere.push("%" + parameter.gameIdAry + "%")
    }

    if (typeof parameter.gameId != "undefined" && parameter.gameId != "") {
      sql_where.push(" w.GameId = ? ")
      argsWhere.push(parameter.gameId)
    }

    if (typeof parameter.currency != "undefined" && parameter.currency.length > 0) {
      sql_where.push(" w.Currency IN (?) ")
      argsWhere.push(parameter.currency)
    }

    // 注單狀態 當搜尋條件為不限時，排除不成單的統計
    if (!parameter.isValid || parameter.isValid[0] == "1") {
      sql_where.push(" w.IsValid = ? ")
      argsWhere.push("1")
    } else if (parameter.isValid[0] == "0") {
      sql_where.push(" w.IsValid = ? ")
      argsWhere.push("0")
    }

    //測試 OR 線上
    if (typeof parameter.isDemo != "undefined" && parameter.isDemo.length > 0) {
      sql_where.push(" w.IsDemo IN (?) ")
      argsWhere.push(parameter.isDemo)
    }

    if (typeof parameter.ggId != "undefined" && parameter.ggId != "") {
      sql_where.push(" w.GGId = ? ")
      argsWhere.push(parameter.ggId)
    }

    //state:[0:一般遊戲,1:免費遊戲,2:獎勵遊戲]
    if (typeof parameter.state != "undefined" && parameter.state.length > 0) {
      var state_query = []
      parameter.state.forEach((item) => {
        switch (item) {
          case 0:
            state_query.push(" ( w.IsFreeGame!=1 AND w.IsBonusGame!=1) ")
            break
          case 1:
            state_query.push(" w.IsFreeGame = 1 ")
            break
          case 2:
            state_query.push(" w.IsBonusGame = 1 ")
            break
        }
      })

      if (state_query.length > 0) {
        sql_where.push(" ( " + state_query.join(" OR ") + " ) ")
      }
    }
    //result:[lost,win]
    if (typeof parameter.result != "undefined" && parameter.result.length > 0) {
      var result_query = []
      parameter.result.forEach((item) => {
        switch (item) {
          case "win":
            result_query.push(" w.WinGold > 0 ")
            break
          case "lost":
            result_query.push(" w.WinGold <= 0 ")
            break
        }
      })

      if (result_query.length > 0) {
        sql_where.push(" ( " + result_query.join(" OR ") + " ) ")
      }
    }
    if (sql_where.length == 0) {
      cb(null, { code: code.FAIL, msg: err })
      return
    }

    var sql =
      "SELECT t.*, (SUM(result)/count(Wid))*100 AS winRate, " +
      " IFNULL(SUM(t.BetGold/t.cusCryDef),0) AS converBetGold, " +
      " IFNULL(SUM(t.WinGold/t.cusCryDef),0) AS converWinGold, " +
      " IFNULL(SUM(t.JPGold/t.cusCryDef),0) AS converJPGold, " +
      " IFNULL(SUM(t.RealBetGold/t.cusCryDef),0) AS converRealBetGold, " +
      " IFNULL(SUM(t.JpRealBetGold/t.cusCryDef),0) AS converJpRealBetGold, " +
      " IFNULL(SUM(t.JPConGoldOriginal/t.cusCryDef),0) AS converJPConGoldOriginal, " +
      " IFNULL(SUM(t.WinGold/t.cusCryDef) - SUM(t.JPGold/t.cusCryDef),0) AS converPayoutAmount, " +
      " IFNULL(SUM(t.RealBetGold/t.cusCryDef)-SUM(t.payoutAmount/t.cusCryDef),0) AS converNetWin " +
      " FROM( " +
      " SELECT CASE WHEN (w.WinGold - w.BetGold) > 0 THEN 1 ELSE 0 END AS result, w.Wid, w.roundID, w.CryDef AS betCryDef, " +
      " w.Currency AS betCurrency,w.ExCurrency AS betExCurrency, TRUNCATE((w.BetGold / 1000),2) AS BetGold,TRUNCATE((w.BetPoint / 1000),2) AS BetPoint," +
      " TRUNCATE(w.WinGold / 1000,2) AS WinGold, TRUNCATE(w.JPPoint / 1000,2) AS JPPoint, TRUNCATE(w.JPGold / 1000,2) As JPGold, " +
      " TRUNCATE(w.RealBetGold / 1000,2) AS RealBetGold," +
      " IF(w.JPConGoldOriginal > 0, TRUNCATE(w.RealBetGold / 1000,2), 0) AS JpRealBetGold," +
      " TRUNCATE((w.JPConGoldOriginal / 1000),4) AS JPConGoldOriginal," +
      " TRUNCATE(w.WinGold /1000,2) - TRUNCATE(w.JPGold /1000,2) AS payoutAmount," +
      " CASE WHEN w.ExCurrency = ? THEN 1 ELSE ( " +
      " SELECT CryDef " +
      " FROM  game.currency_exchange_rate cusRate " +
      " WHERE (cusRate.Currency = ? AND cusRate.ExCurrency = w.ExCurrency AND cusRate.EnableTime <= w.AddDate) " +
      " ORDER BY cusRate.EnableTime DESC " +
      " LIMIT 0,1) END AS cusCryDef " +
      " FROM wagers_1.wagers_bet w " +
      " WHERE " +
      sql_where.join("AND") +
      ") t "
    let args = [data["mainCurrency"], data["mainCurrency"], ...argsWhere]

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][getList_BetHistory_sum] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//總和
// bettingDao.getList_BetHistory_sum_v2 = function (data, cb) {
//     if (typeof data.id === 'undefined') {
//         cb(null, { code: code.FAIL, msg: null });
//         return;
//     }
//     var parameter = Object.assign({}, data);

//     var sql_where = [];
//     if (typeof parameter.id !== 'undefined') {
//         if (parameter.id > 0) sql_where.push(" w.Cid = '" + parameter.id + "' ");
//     }

//     if (typeof parameter.gameId !== 'undefined') {
//         sql_where.push(" w.GameId = " + parameter.gameId + " ");
//     }

//     if (typeof parameter.start_date !== 'undefined' && typeof parameter.end_date !== 'undefined') {
//         sql_where.push(" (w.AddDate >= '" + parameter.start_date + "' AND w.AddDate <= '" + parameter.end_date + "' ) ");
//     }

//     if (sql_where.length == 0) {
//         cb(null, { code: code.FAIL, msg: err });
//         return;
//     }

//     var sql = sprintf("SELECT t.*,FORMAT((SUM(result)/ COUNT(Wid))*100,2) AS winRate, \
//     IFNULL(FORMAT(SUM(t.betCryDef*t.BetGold/t.cusCryDef),2),0) AS converBetGold,IFNULL(FORMAT(SUM(t.betCryDef*t.WinGold/t.cusCryDef),2),0) AS converWinGold, \
//     IFNULL(FORMAT(SUM(t.betCryDef*t.BetGold/t.cusCryDef) - SUM(t.betCryDef*t.WinGold/t.cusCryDef),2),0) AS converNetWin,                    \
//     IFNULL(FORMAT((SUM(t.betCryDef*(t.WinGold+t.JPGold)/t.cusCryDef) / SUM(t.betCryDef*t.BetGold/t.cusCryDef))*100,2), 0) AS RTP                      \
//     FROM( \
//     SELECT w.Wid, (CASE WHEN (w.WinGold-w.JPGold)>0 THEN 1 ELSE 0 END) AS result, w.CryDef AS betCryDef,w.Currency AS betCurrency,w.ExCurrency AS betExCurrency, w.BetGold,w.BetPoint,(w.WinGold-w.JPGold) AS WinGold,w.JPPoint,w.JPGold,         \
//     CASE WHEN   w.ExCurrency = '" + data['opCurrency'] + "' THEN 1 ELSE (                      \
//      SELECT CryDef                                                         \
//      FROM  game.currency_exchange_rate cusRate                              \
//      WHERE (cusRate.Currency = '%s' AND cusRate.ExCurrency = w.ExCurrency AND cusRate.EnableTime <= w.AddDate)       \
//      ORDER BY cusRate.EnableTime DESC                           \
//      LIMIT 0,1) END AS cusCryDef                                \
//     FROM wagers_1.wagers_bet w                                          \
//     WHERE  %s ) t ", data['mainCurrency'], sql_where.join(' AND '));
//     var args = [];

//     db.act_query('dbclient_w_r', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

bettingDao.getList_TransferRecord = function (data, cb) {
  try {
    const parameter = Object.assign({}, data)
    const sql_where = []
    const argsWhere = []

    if (parameter.level == "HA") {
      sql_where.push(" log.Cid = ? ")
      argsWhere.push(parameter.upid)
    } else if (parameter.level == "AG") {
      sql_where.push("(log.Trans_Cid = ? OR log.PlayerId = ?)")
      argsWhere.push(parameter.upid)
      argsWhere.push(parameter.upid)
    }

    //交易編號
    if (typeof parameter.uId != "undefined" && parameter.uId != "") {
      sql_where.push(" log.Uid like ? ")
      argsWhere.push("%" + parameter.uId + "%")
    }

    //代理(hall/agent)名稱
    if (typeof parameter.userName != "undefined" && parameter.userName != "") {
      sql_where.push(" agent.UserName  LIKE ? ")
      argsWhere.push("%" + parameter.userName + "%")
    }

    //玩家名稱
    if (typeof parameter.playerName != "undefined" && parameter.playerName != "") {
      sql_where.push(" player.UserName LIKE ? ")
      argsWhere.push("%" + parameter.playerName + "%")
    }

    //測試 OR 正式
    if (typeof parameter.isDemo != "undefined" && parameter.isDemo.length > 0) {
      sql_where.push(" player.isDemo IN(?) ")
      argsWhere.push(parameter.isDemo)
    }

    //幣別
    if (typeof parameter.currency != "undefined" && parameter.currency.length > 0) {
      sql_where.push(" log.ExCurrency IN (?) ")
      argsWhere.push(parameter.currency)
    }

    //狀態(成功/失敗)
    if (typeof parameter.state != "undefined" && parameter.state.length > 0) {
      sql_where.push(" log.State IN (?) ")
      argsWhere.push(parameter.state)
    }

    //轉出/轉入
    if (typeof parameter.txType != "undefined" && parameter.txType.length > 0) {
      var sql_TxType = []
      parameter.txType.forEach((item) => {
        if (item == "withdraw") {
          sql_TxType.push(" log.TxType LIKE ? ")
          argsWhere.push("%" + item + "%")
        } else {
          sql_TxType.push(" log.TxType = ? ")
          argsWhere.push(item)
        }
      })
      if (sql_TxType.length > 0) {
        sql_where.push("(" + sql_TxType.join(" OR ") + ")")
      }
    }

    if (typeof parameter.start_date !== "undefined" && typeof parameter.end_date !== "undefined") {
      sql_where.push(" (log.TxDate BETWEEN ? AND ? ) ")
      argsWhere.push(parameter.start_date)
      argsWhere.push(parameter.end_date)
    }

    if (sql_where.length == 0) {
      cb(null, { code: code.FAIL, msg: err }, null)
      return
    }

    var sortKey = "DATE_FORMAT(log.TxDate,'%Y-%m-%d %H:%i:%s')"
    var sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()

      // var key_username = (parameter.level == 'AD') ? "hall.UserName" : "agent.UserName";
      var key_username = "agent.UserName"

      var sort_info = {
        uid: "log.Uid",
        username: key_username,
        isdemo: " player.isDemo",
        crydef: "log.CryDef",
        amount: "log.Amount",
        currency: "log.ExCurrency",
        txdate: "DATE_FORMAT(log.TxDate,'%Y-%m-%d %H:%i:%s')",
        quota_before: "log.Quota_Before",
        quota_after: "log.Quota_After",
        state: "log.State",
        txtype: "log.TxType",
        targetname: "player.UserName",
        op_name: "OP_Name",
        trans_name: "Trans_Name",
        trans_amount_before: "log.Trans_Amount_Before",
        trans_amount_after: "log.Trans_Amount_After",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }

    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)

    let sqlLimit = " LIMIT ?,?"
    let argsLimit = []
    if (data.isPage == true) {
      argsLimit.push((data.page - 1) * data.pageCount)
      argsLimit.push(data.pageCount)
    } else {
      argsLimit.push(data.index)
      argsLimit.push(data.pageCount)
    }
    let sql =
      "SELECT SQL_CALC_FOUND_ROWS log.Cid, log.PlayerId, player.Upid AS Upid, log.Uid AS Uid, agent.UserName AS AgentName, player.UserName AS PlayerName, " +
      " CASE WHEN log.TxType='withdraw_all' THEN 'withdraw' ELSE log.TxType END AS TxType, " +
      " CASE WHEN log.Cid = -1 THEN 0 ELSE player.IsDemo END AS IsDemo, " +
      " log.CryDef,log.Amount,log.Currency,log.ExCurrency,DATE_FORMAT(log.TxDate,'%Y-%m-%d %H:%i:%s') AS TxDate ,log.Quota_Before,log.Quota_After,log.State, " +
      " CASE WHEN cu2.UserName IS NOT NULL THEN cu2.UserName ELSE ad.UserName END AS Trans_Name, " +
      " CASE WHEN cu2.UserName IS NOT NULL THEN log.Trans_Amount_Before ELSE '-' END AS Trans_Amount_Before, " +
      " CASE WHEN cu2.UserName IS NOT NULL THEN log.Trans_Amount_After ELSE '-' END AS Trans_Amount_After, " +
      " CASE WHEN ad.userName IS NOT NULL THEN ad.userName WHEN cu.userName IS NOT NULL THEN cu.userName ELSE 'API' END AS OP_Name " +
      " FROM transfer_record log " +
      " LEFT JOIN customer player ON(player.Cid = log.PlayerId) " +
      " LEFT JOIN customer agent ON(agent.Cid = player.Upid) " +
      " LEFT JOIN admin ad ON(ad.AdminId = log.OP_Cid) " +
      " LEFT JOIN customer cu ON(cu.Cid = log.OP_Cid) " +
      " LEFT JOIN customer cu2 ON(cu2.Cid = log.Trans_Cid)" +
      " WHERE " +
      sql_where.join(" AND ") +
      order_by_text +
      sqlLimit
    let args = [...argsWhere, ...argsLimit]

    var sql2 = "SELECT FOUND_ROWS() AS ROWS;"
    let args2 = []

    db.act_query_multi("dbclient_g_r", [sql, sql2], [args, args2], function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var data = {
          count: r_data[1][0]["ROWS"],
          info: r_data[0],
        }
        cb(null, r_code, data)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][getList_TransferRecord] catch err", err)
    cb(null, code.FAIL, null)
  }
}

bettingDao.getList_TransactionRecord = function (data, cb) {
  try {
    var parameter = Object.assign({}, data)
    var sql_where = []
    let argsWhere = []

    if (parameter.level != "AD") {
      sql_where.push(" log.Cid = ? ")
      argsWhere.push(parameter.upid)
    }

    //注單編號
    if (typeof parameter.wId != "undefined" && parameter.wId != "") {
      sql_where.push(" log.Wid like ? ")
      argsWhere.push("%" + parameter.wId + "%")
    }

    //交易編號
    if (typeof parameter.uId != "undefined" && parameter.uId != "") {
      sql_where.push(" log.Uid Like ? ")
      argsWhere.push("%" + parameter.uId + "%")
    }

    //代理(hall/agent)名稱
    if (typeof parameter.userName != "undefined" && parameter.userName != "") {
      var sql_user_text = ""
      if (parameter.level == "AD") {
        sql_user_text = " hall.UserName  LIKE ? "
      } else if (parameter.level == "HA") {
        sql_user_text = " agent.UserName  LIKE ? "
      }
      sql_where.push(sql_user_text)
      argsWhere.push("%" + parameter.userName + "%")
    }

    //玩家名稱
    if (typeof parameter.playerName != "undefined" && parameter.playerName != "") {
      sql_where.push(" player.UserName LIKE ? ")
      argsWhere.push("%" + parameter.playerName + "%")
    }

    //幣別
    if (typeof parameter.currency != "undefined" && parameter.currency.length > 0) {
      sql_where.push(" log.ExCurrency IN (?) ")
      argsWhere.push(parameter.currency)
    }

    //測試 OR 正式
    if (typeof parameter.isDemo != "undefined" && parameter.isDemo.length > 0) {
      sql_where.push(" player.isDemo IN(?) ")
      argsWhere.push(parameter.isDemo)
    }

    //狀態(成功/失敗)
    if (typeof parameter.state != "undefined" && parameter.state.length > 0) {
      sql_where.push(" log.State IN (?) ")
      argsWhere.push(parameter.state)
    }

    if (typeof parameter.start_date !== "undefined" && typeof parameter.end_date !== "undefined") {
      sql_where.push(" (log.TxDate BETWEEN ? AND ?) ")
      argsWhere.push(parameter.start_date)
      argsWhere.push(parameter.end_date)
    }

    //遊戲編號
    if (typeof parameter.gameId != "undefined" && parameter.gameId != "") {
      sql_where.push(" log.GameId = ? ")
      argsWhere.push(parameter.gameId)
    }

    if (sql_where.length == 0) {
      cb(null, { code: code.FAIL, msg: err }, null)
      return
    }
    var sortKey = "DATE_FORMAT(log.TxDate,'%Y-%m-%d %H:%i:%s')"
    var sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var key_username = parameter.level == "AD" ? "hall.UserName" : "agent.UserName"
      var gameName = ""
      if (typeof data.lang == "undefined" || data.lang == "") {
        data["lang"] = "en"
      }
      if (data["lang"] == "en") {
        gameName = "g.NameE"
      }
      if (data["lang"] == "cn") {
        gameName = "g.NameC"
      }
      if (data["lang"] == "tw") {
        gameName = "g.NameG"
      }
      var sort_info = {
        gamename: gameName,
        wid: "log.Wid",
        uid: "log.Uid",
        username: key_username,
        isdemo: " player.IsDemo",
        crydef: "log.CryDef",
        amount: "log.Amount",
        currency: "log.ExCurrency",
        txdate: "DATE_FORMAT(log.TxDate,'%Y-%m-%d %H:%i:%s')",
        quota_before: "log.Quota_Before",
        quota_after: "log.Quota_After",
        state: "log.State",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)

    let sqlLimit = " LIMIT ?,?"
    let argsLimit = [(parameter.page - 1) * parameter.pageCount, parameter.pageCount]

    var sql =
      "SELECT SQL_CALC_FOUND_ROWS log.Uid,log.Wid,hall.UserName AS HallName,agent.UserName AS AgentName,player.UserName AS PlayerName,log.CryDef,log.Amount,log.Currency,log.ExCurrency,DATE_FORMAT(log.TxDate,'%%Y-%%m-%%d %%H:%%i:%%s' ) AS TxDate, log.State, player.IsDemo," +
      " log.Quota_Before,log.Quota_After,g.GameId,g.NameC AS gameC, g.NameG AS gameG, g.NameE AS gameE" +
      " FROM transaction_record log" +
      " INNER JOIN customer hall ON(hall.Cid = log.Cid AND hall.IsAg=2 ) " +
      " INNER JOIN customer player ON(player.Cid = log.PlayerId AND player.IsAg=4 AND player.HallId = hall.Cid ) " +
      " INNER JOIN customer agent ON(agent.Cid = player.Upid AND agent.IsAg=3) " +
      " LEFT JOIN games g ON(g.GameId = log.GameId ) " +
      " WHERE " +
      sql_where.join(" AND ") +
      order_by_text +
      sqlLimit
    let args = [...argsWhere, ...argsLimit]

    var sql2 = "SELECT FOUND_ROWS() AS ROWS;"
    let args2 = []

    db.act_query_multi("dbclient_g_r", [sql, sql2], [args, args2], function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var data = {
          count: r_data[1][0]["ROWS"],
          info: r_data[0],
        }
        cb(null, r_code, data)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][getList_TransactionRecord] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// bettingDao.get_Jackpot_byWid = function (wid, cb) {
//   if (isEmpty(wid)) {
//     cb(null, { code: code.DB.PARA_FAIL, msg: err })
//     return
//   }

//   const sql = "SELECT JPId, JPType, Cid, GameId, FORMAT(Amount,2) AS Amount FROM jackpot_record WHERE Wid = ? "
//   const args = [wid]

//   db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
//     cb(null, r_code, r_data)
//   })
// }

/**
 * @param {object} data 參數物件
 * @param {Array<string>} data.userList 要查詢使用者的cid陣列
 * @param {Array<string>} data.gameList 要查詢的遊戲id陣列
 * @param {function} cb callback
 */
bettingDao.getUserAndGameName = function (data, cb) {
  const { userList, gameList } = data

  const sqlQuery = []

  if (userList && userList.length > 0) {
    const sqlUser = sprintf(
      `
          SELECT Cid, Username, IsAg 
          FROM customer
          WHERE  Cid in ('%s')
          `,
      userList.join("','")
    )
      .split("\n")
      .join("")

    sqlQuery.push(sqlUser)
  }

  if (gameList && gameList.length > 0) {
    const sqlGame = sprintf(
      `
          SELECT GameId, NameC, NameG, NameE, NameTH, NameVN, NameID, NameMY, NameJP, NameKR, GGId
          FROM games 
          WHERE GameId in ('%s')
          `,
      gameList.join("','")
    )
      .split("\n")
      .join("")

    sqlQuery.push(sqlGame)
  }

  if (sqlQuery.length === 0) {
    cb(null, { code: 500 }, null)
    return
  }

  logger.info("[bettingDao][getUserAndGameName]", JSON.stringify(sqlQuery))

  db.act_query_multi("dbclient_g_r", sqlQuery, [], function (r_code, r_data) {
    if (r_code.code !== code.OK) {
      cb(null, r_code, null)
    } else {
      // convert to native array
      const result = JSON.parse(JSON.stringify(r_data))
      cb(null, r_code, result)
    }
  })
}

/**
 * jackpot_record as A
 * jackpot_type as B
 * jackpot_pool as C
 * sql_where
 * search_gameId(遊戲名稱找遊戲ID, array)
 * currency => 必填，登入者交收幣別
 * betCurrency => 開放幣別
 *
 * @param {object} data 參數物件
 * @param {function} cb callback
 */
bettingDao.getList_JackpotSum = function (data, cb) {
  logger.info("[getList_JackpotSum][data]", data)
  const param = { ...data }
  const currencies = [...data.currency]
  const sqlWhere = []
  const groupBy = []
  const orderBy = []
  const sqlB = new SQLBuilder()
  /**
   *    Where
   */

  // 時間
  if (param.start_date && param.end_date) {
    sqlWhere.push(`( A.AddDate BETWEEN '${param.start_date}' AND '${param.end_date}' )`)
  }

  // 遊戲編號
  if (isEmpty(param.gameId) === false && parseInt(param.gameId) !== -1) {
    sqlWhere.push(`A.GameId = '${param.gameId}'`)
  }
  // 遊戲名稱
  if (isEmpty(param.search_gameId) === false && param.search_gameId.length > 0) {
    if (param.search_gameId.length > 1) {
      sqlWhere.push(`A.GameId IN ('${param.search_gameId.join("','")}')`)
    } else {
      sqlWhere.push(`A.GameId = ('${param.search_gameId}')`)
    }
  }
  // 彩金ID
  if (isEmpty(param.JPTypeId) === false) {
    sqlWhere.push(`A.JPTypeId = '${param.JPTypeId}'`)
  }
  // user層級
  switch (param.level) {
    case "HA":
      if (param.isFilterUsername) {
        sqlWhere.push(`A.HallId IN ('${param.validUserList.join("','")}')`)
      }
      if (param.userLevel === "HA") {
        sqlWhere.push(" A.HallId IN ('" + param.hallList.join("','") + "') ")
      } else {
        //For admin search Hall
      }
      //Hall add currency setting
      if (currencies.length > 1) {
        sqlWhere.push(`A.Currency IN ('${currencies.join("','")}')`)
      } else {
        sqlWhere.push(`A.Currency = ('${currencies}')`)
      }
      groupBy.push("A.HallId,A.Currency,A.JPTypeId")

      break
    case "AG":
      if (param.searchType == "list") {
        if (param.isFilterUsername) {
          sqlWhere.push(`A.UpId IN ('${param.validUserList.join("','")}')`)
        }
        //Agent Level search
        if (param.userLevel === "AD") {
          //for admin use Agent Level search
        } else if (param.userLevel === "HA") {
          //Only for Hall search agent
          sqlWhere.push(" A.HallId IN ('" + param.hallList.join("','") + "') ")
        } else if (param.userLevel === "AG" && param.isSub == 0) {
          sqlWhere.push(`A.UpId = ('${param.cid}')`)
        } else if (param.userLevel === "AG" && param.isSub == 1) {
          //For agent manager
          sqlWhere.push(`A.UpId = ('${param.upId}')`)
        }
      } else {
        if (param.userLevel === "AD") {
          sqlWhere.push(`A.HallId = ('${param.upId}')`)
        } else if (param.userLevel === "HA") {
          //Only for Hall click agent
          sqlWhere.push(`A.HallId = ('${param.upId}')`)
        }
      }
      //Agent add currency setting
      if (currencies.length > 1) {
        sqlWhere.push(`A.Currency IN ('${currencies.join("','")}')`)
      } else {
        sqlWhere.push(`A.Currency = ('${currencies}')`)
      }

      groupBy.push("A.UpId,A.Currency,A.JPTypeId")

      break
    case "PR":
      if (param.searchType == "list") {
        if (param.isFilterUsername) {
          sqlWhere.push(`A.Cid IN ('${param.validUserList.join("','")}')`)
        }
        //User Level search
        if (param.userLevel === "AD") {
          //for admin use User Level search
        } else if (param.userLevel === "HA") {
          //Only for Hall search User
          sqlWhere.push(" A.HallId IN ('" + param.hallList.join("','") + "') ")
        } else if (param.userLevel === "AG" && param.isSub == 0) {
          sqlWhere.push(`A.UpId = ('${param.cid}')`)
        } else if (param.userLevel === "AG" && param.isSub == 1) {
          //For agent manager
          sqlWhere.push(`A.UpId = ('${param.upId}')`)
        }
      } else {
        if (param.userLevel === "AD") {
          sqlWhere.push(`A.UpId = ('${param.upId}')`)
        } else if (param.userLevel === "HA") {
          //Only for agent click player
          sqlWhere.push(`A.UpId = ('${param.upId}')`)
        } else if (param.userLevel === "AG" && param.isSub == 0) {
          sqlWhere.push(`A.UpId = ('${param.cid}')`)
        } else if (param.userLevel === "AG" && param.isSub == 1) {
          //For agent manager
          sqlWhere.push(`A.UpId = ('${param.upId}')`)
        }
      }
      //Player add currency setting
      if (currencies.length > 1) {
        sqlWhere.push(`A.Currency IN ('${currencies.join("','")}')`)
      } else {
        sqlWhere.push(`A.Currency = ('${currencies}')`)
      }

      groupBy.push("A.Cid,A.Currency,A.JPTypeId")

      break
  }

  /**
   *    Order by
   */
  orderBy.push(" JPGoldAmount DESC ")

  /**
   *    Limit
   */
  const sqlLimit = sqlB.limit(param)

  /**
   *    SQL
   */
  const sql = sprintf(
    `
    SELECT SQL_CALC_FOUND_ROWS
        '%s' AS userLevel,
        A.HallId,
        A.UpId,
        A.Cid,
        A.Currency,
        A.JPTypeId,
        SUM(A.JPGold) AS JPGoldAmount,   
        SUM( if( A.JPPoolId = 0, A.JPGold, 0 ) ) grand, 
        SUM( if( A.JPPoolId = 1, A.JPGold, 0 ) ) major, 
        SUM( if( A.JPPoolId = 2, A.JPGold, 0 ) ) minor,
        SUM( if( A.JPPoolId = 3, A.JPGold, 0 ) ) mini   
    FROM jackpot_record as A
    INNER JOIN jackpot_type as B ON B.Id = A.JPTypeId
    INNER JOIN Jackpot_pool as C ON C.Id = A.JPPoolId
    WHERE %s  
    GROUP BY %s
    ORDER BY %s 
    %s   
      `,
    param.level,
    sqlWhere.join(" AND "),
    groupBy.join(","),
    orderBy.join(","),
    sqlLimit
  )
    .split("\n")
    .join("")

  const sql2 = "SELECT FOUND_ROWS() AS ROWS"
  const sqlQuery = [sql, sql2]

  db.act_transaction("dbclient_j_r", sqlQuery, function (r_code, r_data) {
    if (r_code.code !== code.OK) {
      cb(null, r_code, null)
    } else {
      const result = {
        count: r_data[1][0]["ROWS"],
        info: r_data[0],
      }
      cb(null, r_code, result)
    }
  })
}

/**
 * jackpot_record as A
 * jackpot_type as B
 * jackpot_pool as C
 * sql_where
 * search_gameId(遊戲名稱找遊戲ID, array)
 * downCurrency => 點擊項目幣別
 *
 * @param {object} data 參數物件
 * @param {function} cb callback
 */
bettingDao.getList_JackpotDetail = function (data, cb) {
  logger.info("[getList_JackpotDetail][data]", data)
  const param = { ...data }
  const sqlWhere = []

  var sortKey = "addDate"
  var sortType = conf.SORT_TYPE[0]
  if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
    data.sortKey = data.sortKey.toLowerCase()
    var sort_info = {
      username: "Cid",
      gameid: "GameId",
      jptypeid: "JPTypeId",
      jppoolid: "JPPoolId",
      jpgold: "JPGold",
      currency: "Currency",
      adddate: "AddDate",
    }
    sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
  }
  if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
    sortType = conf.SORT_TYPE[data.sortType]
  }
  var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)
  //時間
  if (param.start_date && param.end_date) {
    sqlWhere.push(`A.AddDate BETWEEN '${param.start_date}' AND '${param.end_date}'`)
  }

  //彩金ID
  if (isEmpty(param.JPTypeId) === false) {
    sqlWhere.push(`A.JPTypeId = '${param.JPTypeId}'`)
  }
  //遊戲編號
  if (isEmpty(param.gameId) === false && parseInt(param.gameId) !== -1) {
    sqlWhere.push(`A.GameId = '${param.gameId}'`)
  }
  //遊戲名稱
  if (isEmpty(param.search_gameId) === false && param.search_gameId.length > 0) {
    if (param.search_gameId.length > 1) {
      sqlWhere.push(`A.GameId IN ('${param.search_gameId.join("','")}')`)
    } else {
      sqlWhere.push(`A.GameId = ('${param.search_gameId}')`)
    }
  }
  //操作者
  switch (param.userLevel) {
    case "AG":
      if (param.isSub == 1) {
        sqlWhere.push(`A.UpId = '${param.upId}'`)
      } else {
        sqlWhere.push(`A.UpId = '${param.cid}'`)
      }
      break
  }

  sqlWhere.push(`A.Currency = ('${param.currency}')`)
  if (isEmpty(param.playerId) === false) {
    sqlWhere.push(`A.Cid = '${param.playerId}'`)
  }

  if (sqlWhere.length == 0) {
    cb(null, { code: code.FAIL, msg: err }, null)
    return
  }

  /**
   *    Limit
   */
  const sqlLimit = ` LIMIT ${(param.page - 1) * param.pageCount} , ${param.page * param.pageCount}`

  /**
   *    SQL
   */
  const sql = sprintf(
    `
        SELECT SQL_CALC_FOUND_ROWS
            'PR' AS userLevel,
            A.Wid,
            A.HallId,
            A.UpId,
            A.Cid,
            A.Id as JPRecordId, 
            A.GameId,
            A.Currency,
            A.ExCurrency,
            A.JPTypeId,
            A.JPPoolId,
            C.Name as JPPoolName,
            B.Comments as JPTypeComments,
            A.JPGold,
            DATE_FORMAT(A.AddDate,'%%Y-%%m-%%d %%H:%%i:%%s' ) AS addDate
        FROM jackpot_record as A
        INNER JOIN jackpot_type as B ON B.Id = A.JPTypeId
        INNER JOIN Jackpot_pool as C ON C.Id = A.JPPoolId
        WHERE %s  
        ${order_by_text}  
        %s
      `,
    sqlWhere.join(" AND "),
    sqlLimit
  )
    .split("\n")
    .join("")

  const sql2 = "SELECT FOUND_ROWS() AS ROWS"
  const sqlQuery = [sql, sql2]

  db.act_transaction("dbclient_j_r", sqlQuery, function (r_code, r_data) {
    if (r_code.code !== code.OK) {
      cb(null, r_code, null)
    } else {
      var data = {
        count: r_data[1][0]["ROWS"],
        info: r_data[0],
      }
      cb(null, r_code, data)
    }
  })
}

bettingDao.getRevenue = function (data, cb) {
  try {
    /*
          搜尋部分依照原方法，如果沒有要另外加入
          預設已有 BetGold,WinGold,JPGold，其他部分根據需求取值
          data除了原本傳的值，另外加入以下資訊
          data:{
              sel_table:'' // 必填。revenue table。select, groupBy 也以這個為主
              select:['cid', 'username', 'currency', 'isdemo', 'uniplayers'], // 可選需要的
              groupBy:['id', 'username', 'currency'] // 可選需要的
          }
        */

    let sql_where = []
    let args = []
    let args2 = []

    //#region select data
    let select_data = {
      cid: "w.Cid AS id",
      username: "w.UserName AS UserName",
      currency: "w.Currency AS Currency",
      isdemo: "w.IsDemo AS IsDemo",
      uniplayers: "count(DISTINCT w.Cid) AS uniPlayers",
    }

    let select = "*"
    let selectArray = []
    if (data["select"] != "" && data["select"] !== undefined) {
      data["select"].forEach((item) => {
        let selectKey = item.toLowerCase()
        selectArray.push(select_data[selectKey])
        select = selectArray.join(",") + ","
      })
    }
    //#endregion

    //#region filter => sql_where
    //時間
    if (data.start_date != "" && data.start_date !== "undefined") {
      sql_where.push("AccountDate >= ?")
      args.push(data.start_date)
    }
    if (data.end_date != "" && data.end_date !== "undefined") {
      sql_where.push("AccountDate <= ?")
      args.push(data.end_date)
    }

    //名稱
    if (typeof data.userName !== "undefined" && data.userName !== "") {
      sql_where.push(" w.UserName like ? ")
      args.push("%" + data.userName + "%")
    }

    //測試 OR 線上
    if (typeof data.isDemo != "undefined" && data.isDemo.length > 0) {
      sql_where.push(" w.IsDemo IN (?) ")
      args.push(data.isDemo.join("','"))
    }

    //level
    switch (data.level) {
      case 2: //hall
        data.userLevel = "HA"
        break
      case 3: //agent
        data.userLevel = "AG"
        break
    }

    //操作者
    switch (data.userLevel) {
      case "HA":
        sql_where.push(" w.HallId = ? ")
        args.push(data.cid)
        break
      case "AG":
        sql_where.push(" w.UpId = ? ")
        args.push(data.cid)
        break
    }

    //hallId
    if (typeof data.hallId != "undefined") {
      var hall_sql = ""
      if (data.hallId.length > 0) {
        hall_sql = " HallId IN( ? ) "
        args.push(data.hallId.join("','"))
      } else {
        hall_sql = " HallId='' "
      }
      sql_where.push(hall_sql)
    }

    //agentId
    if (typeof data.agentId != "undefined") {
      var agentId_sql = ""
      if (data.agentId.length > 0) {
        agentId_sql = " UpId IN( ? ) "
        args.push(data.agentId.join("','"))
      } else {
        agentId_sql = " UpId='' "
      }
      sql_where.push(agentId_sql)
    }

    //遊戲編號
    if (typeof data.gameId !== "undefined" && data.gameId != "") {
      sql_where.push(" w.GameId = ? ")
      args.push(data.gameId)
    }

    //遊戲種類
    if (typeof data.ggId != "undefined" && data.ggId != "") {
      sql_where.push(" w.GGId = ? ")
      args.push(data.ggId)
    }

    if (sql_where.length == 0) {
      cb(null, { code: code.FAIL, msg: err }, null)
      return
    }
    //幣別
    if (typeof data.currency != "undefined" && data.currency != "") {
      sql_where.push(" Currency = ? ")
      args.push(data.currency)
    }
    if (sql_where.length == 0) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }
    //#endregion

    //#region 排序及合併資訊
    let info = {
      id: "w.Cid",
      username: "w.UserName",
      currency: "w.Currency",
      uniplayer: "count(DISTINCT w.Cid)",
      jpgold: `SUM( w.JPGold  /1000 )`,
      betgold: `SUM( w.BetGold /1000 )`,
      effectivebet: `SUM( w.BetGold /1000 - w.JPGold  /1000 )`,
      wingold: `SUM( w.WinGold /1000 - w.JPGold  /1000 )`,
      netwin: `SUM( w.WinGold /1000 - w.JPGold  /1000 - w.BetGold /1000 )`,
      rtp: `(SUM( w.WinGold /1000 ) / SUM( w.BetGold /1000))*100`,
    }

    // 合併
    let group_By_Text = ""
    let groupByArray = []
    if (data["groupBy"] != "" && data["groupBy"] !== undefined) {
      data["groupBy"].forEach((item) => {
        let groupByKey = item.toLowerCase()
        groupByArray.push(info[groupByKey])
        group_By_Text = "GROUP BY " + groupByArray.join(",")
      })
    }

    //排序
    let sortKey = "w.WinGold"
    let sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      sortKey = typeof info[data.sortKey] != "undefined" ? info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    let order_by_text = ` ORDER BY ${sortKey} ${sortType} `
    //#endregion

    //限制筆數
    let sql_limit = `LIMIT ? , ?`
    if (data.isPage === true) {
      args.push((data.page - 1) * data.pageCount)
    } else {
      args.push(data.index)
    }
    args.push(data.pageCount)

    let sql = `
            SELECT SQL_CALC_FOUND_ROWS
            ${select}
            SUM(w.BetGold /1000) AS BetGold,
            SUM(w.WinGold /1000) AS WinGold,
            SUM(w.JPGold  /1000) AS JPGold
            FROM  ${data.sel_table} w
            WHERE ${sql_where.join(" AND ")}
            ${group_By_Text}
            ${order_by_text}
            ${sql_limit}
        `

    let sql2 = "SELECT FOUND_ROWS() AS ROWS;"

    let sql_query = []
    sql_query.push(sql)
    sql_query.push(sql2)

    db.act_query_multi("dbclient_w_r", [sql, sql2], [args, args2], function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        let data = {
          count: r_data[1][0]["ROWS"],
          info: r_data[0],
        }
        cb(null, r_code, data)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][getRevenue] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// bettingDao.getList_TotalPlayersWin = function (data, cb) {
//     try {
//         var parameter = Object.assign({}, data);
//         var sql_where = new Array();

//         //時間
//         if (typeof parameter.start_date !== 'undefined' && typeof parameter.end_date !== 'undefined') {
//             sql_where.push(" (A.AccountDate BETWEEN '" + parameter.start_date + "' AND '" + parameter.end_date + "' ) ");
//         }

//         if (typeof data.userName !== 'undefined' && data.userName !== '') {
//             sql_where.push(" A.UserName like '%" + data.userName + "%' ");
//         }

//         //測試 OR 線上
//         if (typeof parameter.isDemo != 'undefined' && parameter.isDemo.length > 0) {
//             sql_where.push(" A.IsDemo IN ('" + parameter.isDemo.join("','") + "' ) ");
//         }

//         //操作者
//         switch (parameter.userLevel) {
//             case 'HA':
//                 sql_where.push(" A.HallId = '" + parameter.cid + "' ");
//                 break;
//             case 'AG':
//                 sql_where.push(" A.UpId = '" + parameter.cid + "' ");
//                 break;
//         }

//         //遊戲編號
//         if (typeof parameter.gameId !== 'undefined' && parameter.gameId != '') {
//             sql_where.push(" A.GameId = '" + parameter.gameId + "' ");
//         }

//         //遊戲種類
//         if (typeof parameter.ggId != 'undefined' && parameter.ggId != '') {
//             sql_where.push(" A.GGId = '" + parameter.ggId + "' ");
//         }

//         if (sql_where.length == 0) {
//             cb(null, { code: code.FAIL, msg: err }, null);
//             return;
//         }

//         var sortKey = "A.WinGold";
//         var sortType = conf.SORT_TYPE[0];
//         if (typeof data.sortKey !== "undefined" && data.sortKey != '') {
//             data.sortKey = data.sortKey.toLowerCase();
//             var sort_info = {
//                 username: "A.UserName",
//                 currency: "A.Currency",
//                 bet: "SUM(A.BetGold)",
//                 effectivebet: "SUM(A.BetPoint-A.JPPoint)",
//                 win: "SUM(A.WinGold-A.JPGold)",
//                 netwin: "SUM((A.WinGold)-A.BetGold)",
//                 jpgold: "SUM(A.JPGold)",
//                 rtp: "(SUM(A.WinGold)/SUM(A.BetGold))",
//                 cid: "A.Cid"
//             };

//             sortKey = (typeof sort_info[data.sortKey] != 'undefined') ? sort_info[data.sortKey] : sortKey;
//         }
//         if (typeof data.sortType !== "undefined" && ['0', '1'].indexOf(data.sortType.toString()) > -1) {
//             sortType = conf.SORT_TYPE[data.sortType];
//         }
//         var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType);

//         var sql_limit = (data.isPage === true) ? " LIMIT " + (data.page - 1) * data.pageCount + "," + data.pageCount : " LIMIT  " + data.index + "," + data.pageCount; //限制筆數

//         var sql = sprintf("SELECT SQL_CALC_FOUND_ROWS SUM(A.Rounds) as count, " +
//             " A.Cid, A.UpId, A.HallId, SUM(A.BetGold) AS bet,SUM(A.BetPoint-A.JPPoint) AS effectiveBet, SUM(A.BetPoint) AS BetPoint, " +
//             " SUM(A.WinGold-A.JPGold) AS win,SUM(A.JPPoint) AS JPPoint, SUM((A.WinGold)-A.BetGold) AS netWin, " +
//             " IFNULL(FORMAT((SUM(A.WinGold)/SUM(A.BetGold)*100),2),0) AS RTP, SUM(A.JPGold) AS JPGold, " +
//             " A.UserName,A.Currency,A.ExCurrency,A.IsDemo  " +
//             " FROM game_revenue_player A" +
//             " WHERE %s " +
//             " GROUP BY A.Cid " +
//             // " ORDER BY A.WinGold DESC %s" ,
//             " %s %s" ,
//             sql_where.join(' AND '), order_by_text, sql_limit);

//         /*
//         var sql = sprintf("SELECT SQL_CALC_FOUND_ROWS Count(A.Wid) as count,A.Cid,A.UpId,A.HallId,SUM(A.BetGold) AS bet,SUM(A.BetPoint) AS BetPoint,SUM(A.WinGold-A.JPGold) AS win,SUM(A.RealBetPoint) AS RealBetPoint, SUM(A.JPPoint) AS JPPoint, " +
//             " SUM(A.BetGold-(A.WinGold-A.JPGold)) AS netWin, IFNULL(FORMAT((SUM(A.WinGold)/SUM(A.BetGold)*100),2),0) AS RTP, A.UserName,A.Currency,A.ExCurrency,A.IsDemo  " +
//             " FROM wagers_bet A" +
//             " WHERE %s " +
//             " GROUP BY A.Cid " +
//             " ORDER BY A.WinGold DESC %s" , sql_where.join(' AND '),sql_limit);
//         */
//         var sql2 = "SELECT FOUND_ROWS() AS ROWS;";

//         var sql_query = [];
//         sql_query.push(sql);
//         sql_query.push(sql2);

//         db.act_transaction('dbclient_w_r', sql_query, function (r_code, r_data) {
//             if (r_code.code !== code.OK) {
//                 cb(null, r_code, null);
//             } else {
//                 var data = {
//                     count: r_data[1][0]['ROWS'],
//                     info: r_data[0]
//                 }
//                 cb(null, r_code, data);
//             }
//         });
//     } catch (err) {
//         logger.error('[bettingDao][getList_TotalPlayersWin] catch err', err);
//         cb(null, code.FAIL, null);
//     }
// }

// bettingDao.getSettlementReport = function (data, cb) {

//     var sql_where = [];
//     var sql_where_text = "";

//     if (typeof data.cid != 'undefined' && (data.cid != '-1')) {
//         sql_where.push(" r.Cid = '" + data.cid + "' ");
//     }
//     switch (data['user_level']) {
//         case 1: //AD
//             break;
//         case 2: //HA
//             sql_where.push(" r.HallId = '" + data.user_cid + "' ");
//             break;
//         case 3:
//             sql_where.push(" r.UpId = '" + data.user_cid + "' ");
//             break;
//     }
//     //取server range
//     if (typeof data.server_start_date !== 'undefined' && typeof data.server_end_date !== 'undefined') {
//         sql_where.push(" (r.AccountDate >= '" + data.server_start_date + "' AND r.AccountDate < '" + data.server_end_date + "' ) ");
//     }

//     //時差->client端時間
//     var sql_client_local_time = " date_add(r.AccountDate,interval " + data.timeDiff + "  HOUR)  AS client_local_time ";

//     sql_where_text = " 1 AND " + sql_where.join(' AND ');

//     var sql_group_by = [];
//     var field_client_local_date = "";

//     switch (data.showType) {
//         case '1': //year
//             field_client_local_date = " DATE_FORMAT(client_local_time,'%Y') ";
//             break;
//         case '2': //month
//             field_client_local_date = " DATE_FORMAT(client_local_time,'%Y-%m') ";
//             break;
//         case '3': //day
//             field_client_local_date = " DATE_FORMAT(client_local_time,'%Y-%m-%d') ";
//             break;
//     }
//     sql_group_by.push(field_client_local_date);

//     var sql = "SELECT *,  " + field_client_local_date + "  AS client_local_date," +
//         "   FORMAT(SUM((t.betCryDef*t.BetGold/t.cusCryDef)),2) AS converBetGold, FORMAT(SUM((t.betCryDef*t.WinGold/t.cusCryDef)),2) AS converWinGold , " +
//         "   IFNULL(FORMAT((SUM(t.betCryDef*(t.WinGold+t.JPGold)/t.cusCryDef)/SUM(t.betCryDef*t.BetGold/t.cusCryDef))*100,2),0) AS RTP " +
//         " FROM ( " +
//         " SELECT r.Cid,cus.UserName,r.CryDef AS betCryDef, r.Currency AS betCurrency,r.ExCurrency AS betExCurrency,r.BetGold,r.BetPoint,(r.WinGold-r.JPGold) AS WinGold,r.JPGold,r.JPPoint, r.AccountDate," + sql_client_local_time + ", " +
//         "  CASE WHEN  r.ExCurrency = '" + data['currency'] + "' THEN 1 ELSE ( SELECT cusRate.CryDef " +
//         "  FROM game.currency_exchange_rate cusRate " +
//         "  WHERE (cusRate.Currency = '" + data['mainCurrency'] + "' AND cusRate.ExCurrency = r.ExCurrency AND cusRate.EnableTime <= r.AccountDate) " +
//         "  ORDER BY cusRate.EnableTime DESC " +
//         "  LIMIT 0,1) END AS cusCryDef " +
//         " FROM wagers_1." + data['sel_table'] + " r " +
//         " LEFT JOIN game.customer cus ON(cus.Cid=r.Cid) " +
//         " WHERE " + sql_where_text +
//         " ) t  GROUP BY " + sql_group_by.join(",") +
//         " ORDER BY client_local_time DESC " +
//         " limit ?,?";

//     var args = [(data.curPage - 1) * data.pageCount, data.pageCount];

//     db.act_query('dbclient_w_r', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             var res = [];
//             for (var i in r_data) {
//                 var info = {
//                     client_local_date: r_data[i]['client_local_date'],
//                     betGold: r_data[i]['converBetGold'],
//                     WinGold: r_data[i]['converWinGold'],
//                     RTP: r_data[i]['RTP'],
//                     cid: (data.cid != '-1') ? r_data[i]['Cid'] : '',
//                     userName: (data.cid != '-1') ? r_data[i]['UserName'] : ''
//                 };
//                 res.push(info);
//             }
//             cb(null, r_code, res);
//         }
//     });
// }

// bettingDao.getCounts_SettlementReport = function (data, cb) {

//     var sql_where = [];
//     var sql_where_text = "";

//     if (typeof data.cid != 'undefined' && (data.cid != '-1')) {
//         sql_where.push(" r.Cid ='" + data.cid + "' ");
//     }
//     switch (data['user_level']) {
//         case 1: //AD
//             break;
//         case 2: //HA
//             sql_where.push(" r.HallId ='" + data.user_cid + "' ");
//             break;
//         case 3: //AG
//             sql_where.push(" r.UpId ='" + data.user_cid + "' ");
//             break;
//     }
//     //取server range
//     if (typeof data.server_start_date !== 'undefined' && typeof data.server_end_date !== 'undefined') {
//         sql_where.push(" (r.AccountDate >= '" + data.server_start_date + "' AND r.AccountDate < '" + data.server_end_date + "' ) ");
//     }

//     //時差->client端時間
//     var sql_client_local_time = " date_add(r.AccountDate,interval " + data.timeDiff + "  HOUR)  AS client_local_time ";

//     sql_where_text = " 1 AND " + sql_where.join(' AND ');

//     var sql_group_by = [];
//     var field_client_local_date = "";

//     switch (data.showType) {
//         case '1': //year
//             field_client_local_date = " DATE_FORMAT(client_local_time,'%Y') ";
//             break;
//         case '2': //month
//             field_client_local_date = " DATE_FORMAT(client_local_time,'%Y-%m') ";
//             break;
//         case '3': //day
//             field_client_local_date = " DATE_FORMAT(client_local_time,'%Y-%m-%d') ";
//             break;
//     }
//     sql_group_by.push(field_client_local_date);

//     var sql = " SELECT count(*) AS count FROM ( SELECT *, SUM(BetGold) AS totalBetGold,SUM(BetPoint) AS totalBetPoint, SUM(WinGold) AS totalWinGold,SUM(JPPoint) AS totalJPPoint, " +
//         field_client_local_date + " AS client_local_date,FORMAT(SUM((t.betCryDef*t.BetGold/t.cusCryDef)),2) AS converBetGold, FORMAT(SUM((t.betCryDef*t.WinGold/t.cusCryDef)),2) AS converWinGold " +
//         " FROM ( " +
//         " SELECT  r.Cid,cus.UserName,r.CryDef AS betCryDef, r.Currency AS betCurrency,r.ExCurrency AS betExCurrency, r.BetGold,r.BetPoint,(r.WinGold-r.JPGold) AS WinGold ,r.JPPoint,  r.AccountDate," + sql_client_local_time + ", " +
//         "  CASE WHEN r.ExCurrency = '" + data['currency'] + "' THEN 1 " +
//         "  ELSE ( SELECT cusRate.CryDef " +
//         "  FROM game.currency_exchange_rate cusRate " +
//         "  WHERE (cusRate.Currency = '" + data['mainCurrency'] + "' AND cusRate.ExCurrency= r.ExCurrency AND cusRate.EnableTime <= r.AccountDate) " +
//         "  ORDER BY cusRate.EnableTime DESC " +
//         "  LIMIT 0,1) END AS cusCryDef " +
//         " FROM wagers_1." + data['sel_table'] + " r " +
//         " LEFT JOIN game.customer cus ON(cus.Cid=r.Cid) " +
//         " WHERE " + sql_where_text +
//         " ) t  GROUP BY " + sql_group_by.join(",") +
//         " ) T ";
//     var args = [];

//     db.act_query('dbclient_w_r', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             cb(null, r_code, r_data[0]['count']);
//         }
//     });
// }

// bettingDao.getRevenue_byPlayers = function (data, cb) {
//     var sql_where = [];
//     var sql_where_text = "";

//     switch (data['user_level']) {
//         case 1: // HA
//             break;
//         case 2: // AG
//             var userId = (data.user_isSub == 1) ? data.user_hallId : data.user_cid;
//             sql_where.push(" t.HallId ='" + userId + "' ");
//             break;
//     }

//     //AG
//     if (data['user_level'] == 3) {
//         sql_where.push(" t.UpId ='" + data.user_cid + "' ");
//     }
//     //遊戲編號
//     if (typeof data.gameId !== 'undefined' && data.gameId !== '') {
//         sql_where.push(" t.GameId ='" + data.gameId + "' ");
//     }
//     //玩家編號
//     if (typeof data.playerId !== 'undefined' && data.playerId !== '') {
//         sql_where.push(" t.Cid ='" + data.playerId + "' ");
//     }
//     //取server range
//     if (typeof data.server_start_date !== 'undefined' && typeof data.server_end_date !== 'undefined') {
//         sql_where.push(" (w.AddDate >= '" + data.server_start_date + "' AND w.AddDate <= '" + data.server_end_date + "' ) ");
//     }

//     //時差->client端時間
//     var sql_client_local_time = " date_add(w.AddDate,interval " + data.timeDiff + "  HOUR)  AS client_local_time ";

//     sql_where_text = " 1 AND " + sql_where.join(' AND ');

//     var sql = " SELECT t.betCryDef,t.cusCryDef,DATE_FORMAT(t.client_local_time,'%Y-%m-%d') AS client_local_date,count(t.Wid) as rounds, " +
//         " FORMAT(SUM(t.betCryDef*t.BetGold/t.cusCryDef),2) AS converBetGold, FORMAT(SUM(t.betCryDef*t.WinGold/t.cusCryDef),2) AS converWinGold, " +
//         " FORMAT(SUM(t.betCryDef*t.BetGold/t.cusCryDef) - SUM(t.betCryDef*t.WinGold/t.cusCryDef),2) AS converNetWin,  " +
//         " IFNULL(FORMAT((SUM(t.betCryDef*(t.WinGold+t.JPGold)/t.cusCryDef)/ SUM(t.betCryDef*t.BetGold/t.cusCryDef))*100,2),0) AS RTP " +
//         " FROM ( " +
//         " SELECT w.Wid, w.CryDef AS betCryDef, w.Currency AS betCurrency,w.ExCurrency AS betExCurrency, w.BetGold,w.BetPoint,(w.WinGold-w.JPGold) AS WinGold,w.JPGold,w.JPPoint,w.AddDate, " + sql_client_local_time + ", " +
//         "  CASE WHEN w.ExCurrency = '" + data['currency'] + "' THEN 1 ELSE ( SELECT cusRate.CryDef " +
//         "  FROM game.currency_exchange_rate cusRate " +
//         "  WHERE (cusRate.Currency = '" + data['mainCurrency'] + "' AND cusRate.ExCurrency = w.ExCurrency AND cusRate.EnableTime <= w.AddDate) " +
//         "  ORDER BY cusRate.EnableTime DESC " +
//         "  LIMIT 0,1) END AS cusCryDef " +
//         " FROM wagers_1.wagers_bet w " +
//         " WHERE " + sql_where_text +
//         " ) t GROUP BY DATE_FORMAT(client_local_time,'%Y-%m-%d') " +
//         " ORDER BY client_local_time DESC ";
//     var args = [];

//     db.act_query('dbclient_w_r', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             var res = [];
//             for (var i in r_data) {
//                 var info = {
//                     client_local_date: r_data[i]['client_local_date'],
//                     betGold: r_data[i]['converBetGold'],
//                     winGold: r_data[i]['converWinGold'],
//                     netWin: r_data[i]['netWinGold'],
//                     RTP: r_data[i]['RTP']

//                 };
//                 res.push(info);
//             }
//             cb(null, r_code, res);
//         }
//     });
// }

bettingDao.getGameTotalRevenue = function (data, cb) {
  try {
    var sql_where = []
    var sql_where_text = ""
    let sql_where_arg = [
      data["currency"],
      data["mainCurrency"],
      data["mainCurrency"],
      data["currency"],
      data["mainCurrency"],
      data["mainCurrency"],
    ]
    switch (data["user_level"]) {
      case 1: // admin
        sql_where.push(" r.Upid = ? ")
        sql_where_arg.push("-1")
        break
      case 2: // Hall
        var userId = data.user_isSub == 1 ? data.user_hallId : data.user_cid
        sql_where.push(" r.Cid = ? ")
        sql_where_arg.push(userId)
        break
      case 3: // Agent
        sql_where.push(" r.Cid = ? ")
        sql_where_arg.push(data.user_cid)
        break
    }
    //取server range
    if (typeof data.server_start_date !== "undefined" && typeof data.server_end_date !== "undefined") {
      sql_where.push("(r.AccountDate >= ? AND r.AccountDate < ? )")
      sql_where_arg.push(data.server_start_date)
      sql_where_arg.push(data.server_end_date)
    }
    //Cid
    if (typeof data.cId !== "undefined" && data.cId !== "") {
      sql_where.push(" r.Cid = ? ")
      sql_where_arg.push(data.cId)
    }
    //GameId
    if (typeof data.gameId !== "undefined" && data.gameId !== "") {
      sql_where.push(" r.GameId = ? ")
      sql_where_arg.push(data.gameId)
    }
    if (data.isEveryDay && data["everyDayCurrency"] != "Consolidated") {
      sql_where.push(" r.ExCurrency = ? ")
      sql_where_arg.push(data["everyDayCurrency"])
    }
    // var sql_group_by = [];
    var field_client_local_date = ""
    if (data["showType"] == "month") {
      field_client_local_date = "%Y-%m"
    }

    if (data["showType"] == "day") {
      field_client_local_date = "%Y-%m-%d"
    }
    // sql_group_by.push(field_client_local_date);
    // 時差->client端時間
    // var sql_client_local_time = " date_add(r.AccountDate,interval " + data.timeDiff + "  HOUR)  AS client_local_time ";

    sql_where_text = sql_where.join(" AND ")
    /*
            var sql = "SELECT *, " + field_client_local_date + " AS client_local_date," +
                " FORMAT(SUM((t.betCryDef*t.BetGold*t.cusCryDef)),2) AS converBetGold, FORMAT(SUM((t.betCryDef*t.WinGold*t.cusCryDef)),2) AS converWinGold , FORMAT(SUM(t.betCryDef*t.BetGold*t.cusCryDef)-SUM(t.betCryDef*t.WinGold*t.cusCryDef),2) AS netWinGold, " +
                " IFNULL(FORMAT((SUM(t.betCryDef*(t.WinGold+t.JPGold)*t.cusCryDef) / SUM(t.betCryDef*t.BetGold*t.cusCryDef))*100,2),0) AS RTP " +
                " FROM ( " +
                " SELECT r.Cid,cus.UserName,r.CryDef AS betCryDef, r.Currency AS betCurrency, r.ExCurrency AS betExCurrency, r.BetGold,r.BetPoint,(r.WinGold-r.JPGold) AS WinGold ,r.JPPoint,r.JPGold, r.AccountDate," + sql_client_local_time + ", " +
                "  CASE WHEN r.ExCurrency = '" + data.mainCurrency + "' THEN 1 ELSE ( SELECT cusRate.CryDef " +
                "  FROM currency_exchange_rate cusRate " +
                "  WHERE (cusRate.Currency = r.ExCurrency AND cusRate.ExCurrency = '" + data['currency'] + "' AND cusRate.EnableTime <= r.AccountDate) " +
                "  ORDER BY cusRate.EnableTime DESC " +
                "  LIMIT 0,1) END AS cusCryDef " +
                " FROM " + data['sel_table'] + " r " +
                " LEFT JOIN game.customer cus ON(cus.Cid=r.Cid) " +
                " WHERE " + sql_where_text +
                " ) t  GROUP BY " + sql_group_by.join(" , ")
            " ORDER BY client_local_time DESC ";
            */

    var sql = `
            SELECT r.Cid,
            DATE_FORMAT(convert_tz(r.AccountDate,"+00:00",?), '${field_client_local_date}' ) AS client_local_date,
            r.Currency AS betCurrency,
            r.ExCurrency AS betExCurrency,
            SUM(TRUNCATE((r.BetGold/1000),2)) AS BetGold,
            SUM(TRUNCATE((r.JPGold /1000),2)) AS JPGold,
            SUM(TRUNCATE((r.WinGold/1000),2)) AS WinGold,
            CASE WHEN ? = ? THEN 1
            ELSE (
                SELECT cusRate.CryDef
                FROM game.currency_exchange_rate cusRate
                WHERE (cusRate.Currency = ? AND cusRate.ExCurrency = ? AND cusRate.EnableTime <= r.AccountDate)
                ORDER BY cusRate.EnableTime DESC 
                LIMIT 0,1
            ) END AS loginCryDef,
            CASE WHEN r.ExCurrency = ? THEN 1
            ELSE (
                SELECT cusRate.CryDef
                FROM game.currency_exchange_rate cusRate
                WHERE (cusRate.Currency = ? AND cusRate.ExCurrency = r.ExCurrency AND cusRate.EnableTime <= r.AccountDate)
                ORDER BY cusRate.EnableTime DESC LIMIT 0,1
            ) END AS cusCryDef
            FROM wagers_1.${data["sel_table"]} r
            WHERE 1 AND ${sql_where_text}
            GROUP BY client_local_date, betExCurrency
            ORDER BY client_local_date DESC
        `

    const args = [data.hourDiffFormated, ...sql_where_arg]

    console.log("-getGameTotalRevenue sql-", sql)
    console.log("-getGameTotalRevenue args-", args)

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var res = []
        for (var i in r_data) {
          var info = {
            client_local_date: r_data[i]["client_local_date"],
            betGold: r_data[i]["BetGold"] || 0,
            JPGold: r_data[i]["JPGold"] || 0,
            winGold: r_data[i]["WinGold"] || 0,
            loginCryDef: r_data[i]["loginCryDef"],
            cusCryDef: r_data[i]["cusCryDef"],
          }
          res.push(info)
        }
        cb(null, r_code, res)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][getGameTotalRevenue] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/**
 *
 * @param {object} data
 * @param {string} data.startDate
 * @param {string} data.endDate
 * @param {string} data.targetTable
 * @param {string|null} data.cid
 * @param {string=} data.orderBy
 * @param {string=} data.groupBy
 * @param {*} cb
 */
bettingDao.getGamesSummaryData = function (data, cb) {
  const logTag = "[bettingDao][getGamesSummaryData]"

  try {
    const { targetTable, startDate, endDate, cid, orderBy, groupBy } = data

    const additionalSQL = []
    const additionalArgs = []

    if (cid) {
      additionalSQL.push(" AND Cid = ? ")
      additionalArgs.push(cid)
    }

    const orderBySQL = `ORDER BY ${orderBy || "(realBetGold - winGold) DESC"}`
    const groupBySQL = `GROUP BY ${groupBy || "exCurrency, exchangeRate, gameId"}`

    const sql = `
    SELECT r.GameId as gameId, r.ExCurrency as exCurrency, SUM(r.Rounds) as rounds,
      SUM(TRUNCATE(r.RealBetGold/1000,2)) as realBetGold,
      SUM(TRUNCATE(r.WinGold/1000,2)) as winGold,
      SUM(TRUNCATE(r.JPGold/1000,2)) as jpGold,
      SUM(TRUNCATE(r.WinGold/1000,2) - TRUNCATE(JPGold/1000,2)) as payout,
      SUM(TRUNCATE(RealBetGold/1000,2) - (TRUNCATE(r.WinGold/1000,2) - TRUNCATE(JPGold/1000,2))) as netWin,
      SUM(TRUNCATE(RealBetGold/1000,2) - TRUNCATE(r.WinGold/1000,2)) as winLose,
      (
      SELECT CryDef
        FROM game.currency_exchange_rate
        WHERE r.ExCurrency = ExCurrency
        AND r.AccountDate >= EnableTime
        ORDER BY EnableTime DESC LIMIT 1
      ) as exchangeRate
      
    FROM ?? as r
    WHERE AccountDate >= ? AND AccountDate <= ? 
    ${additionalSQL.join(" ")}
    ${groupBySQL}
    ${orderBySQL}
    `
    const args = [targetTable, startDate, endDate, ...additionalArgs]

    logger.info(`${logTag} args ${JSON.stringify(args)}`)

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error(`${logTag} ${inspect(err)}`)
    cb(null, code.FAIL, null)
  }
}

/**
 *
 * @param {object} data
 * @param {string} data.startDate
 * @param {string} data.endDate
 * @param {string=} data.hallId
 * @param {string=} data.agentId
 * @param {string=} data.orderBy
 * @param {string=} data.groupBy
 * @param {*} cb
 */
bettingDao.getPlayerRevenueData = function (data, cb) {
  const logTag = "[bettingDao][getPlayerRevenueData]"

  try {
    const { startDate, endDate, hallId, agentId, orderBy, groupBy } = data

    const additionalSQL = []
    const additionalArgs = []

    if (hallId) {
      additionalSQL.push(" AND HallId = ? ")
      additionalArgs.push(hallId)
    }

    if (agentId) {
      additionalSQL.push(" AND UpId = ? ")
      additionalArgs.push(agentId)
    }

    const orderBySQL = `ORDER BY ${orderBy || "(realBetGold - winGold) DESC"}`
    const groupBySQL = `GROUP BY ${groupBy || "cid, exCurrency"}`

    const sql = `
    SELECT r.Cid as playerId, r.ExCurrency as exCurrency, SUM(r.Rounds) as rounds,
      r.UserName as username,
      SUM(TRUNCATE(r.RealBetGold/1000,2)) as realBetGold,
      SUM(TRUNCATE(r.WinGold/1000,2)) as winGold,
      SUM(TRUNCATE(r.JPGold/1000,2)) as jpGold,
      SUM(TRUNCATE(r.WinGold/1000,2) - TRUNCATE(RealBetGold/1000,2)) as payout,
      SUM(TRUNCATE(RealBetGold/1000,2) - (TRUNCATE(r.WinGold/1000,2) - TRUNCATE(RealBetGold/1000,2))) as netWin,
      SUM(TRUNCATE(TRUNCATE(RealBetGold/1000,2) - r.WinGold/1000,2)) as winLose,
      (
      SELECT CryDef
        FROM game.currency_exchange_rate
        WHERE r.ExCurrency = ExCurrency
        AND r.AccountDate >= EnableTime
        ORDER BY EnableTime DESC LIMIT 1
      ) as exchangeRate
      
    FROM user_revenue_player as r
    WHERE AccountDate >= ? AND AccountDate <= ? 
    ${additionalSQL.join(" ")}
    ${groupBySQL}
    ${orderBySQL}
    `
    const args = [startDate, endDate, ...additionalArgs]

    logger.info(`${logTag} args ${JSON.stringify(args)}`)

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error(`${logTag} ${inspect(err)}`)
    cb(null, code.FAIL, null)
  }
}

/**
 * 取的玩家每個遊戲下注額統計
 *
 * @param {object} data
 */
bettingDao.getUserRevenue_game = function (data, cb) {
  try {
    if (
      typeof data.start_date === "undefined" ||
      typeof data.end_date === "undefined" ||
      typeof data.playerId == "undefined"
    ) {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }

    var sql_where = []
    let argsWhere = []

    if (typeof data.start_date !== "undefined" && typeof data.end_date !== "undefined") {
      sql_where.push("(w.AddDate >= ? AND w.AddDate <= ?)")
      argsWhere.push(data.start_date)
      argsWhere.push(data.end_date)
    }

    if (data.playerId != "") {
      sql_where.push(" w.Cid = ? ")
      argsWhere.push(data.playerId)
    } else {
      var sql_where_upId = ""
      switch (data.level) {
        case 1:
          break
        case 2:
          sql_where_upId = "w.HallId = ? "
          break
        case 3:
          sql_where_upId = "w.UpId = ? "
          break
      }
      if (sql_where_upId != "") {
        sql_where.push(sql_where_upId)
        argsWhere.push(data.upId)
      }
    }

    if (sql_where.length == 0) {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }

    var sortKey = "rounds"
    var sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        gameid: "gameId",
        rounds: "rounds",
        BaseRounds: "BaseRounds",
        name: "UserName",
        groupname: "ggId",
        namec: "NameC",
        nameg: "NameG",
        namee: "NameE",
        currency: "Currency",
        betgold: "SUM(t.BetGold)",
        realbetgold: "SUM(t.RealBetGold)",
        wingold: "SUM(t.WinGold)",
        payoutamount: "SUM(t.WinGold-t.JPGold)",
        jpgold: "SUM(t.JPGold)",
        jpcongoldoriginal: "SUM(t.JPConGoldOriginal)",
        netwin: "(SUM(t.BetGold)-SUM(t.WinGold))",
        rtp: "(SUM(t.WinGold)/SUM(t.BetGold))",
        // netRTP
        ggr: "(SUM(t.WinGold-t.JPGold)-SUM(t.BetGold))",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }

    var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)

    //玩家遊戲幣別
    var sql_limit =
      data.isPage === true
        ? " LIMIT " + (data.page - 1) * data.pageCount + "," + data.pageCount
        : " LIMIT  " + data.index + "," + data.pageCount //限制筆數
    //幣別匯率
    let defaultCurrency = pomelo.app.get("sys_config").main_currency
    let CryDef = data.downCurrency == "Consolidated" ? "t.loginCryDef/t.cusCryDef" : "1"

    let sql_loginCryDef_Where =
      " WHERE (cusRate.Currency = ? AND cusRate.ExCurrency = ? AND cusRate.EnableTime <= w.AddDate) ORDER BY cusRate.EnableTime DESC LIMIT 0,1)  AS loginCryDef, "
    let args_loginCryDef_Where = [defaultCurrency, data["currency"]]

    let sql_custCryDef_Where =
      " WHERE (cusRate.Currency = ? AND cusRate.ExCurrency = w.ExCurrency AND cusRate.EnableTime <= w.AddDate) ORDER BY cusRate.EnableTime DESC LIMIT 0,1)  AS cusCryDef, "
    let args_custCryDef_Where = [defaultCurrency]

    var sql = `
            SELECT SQL_CALC_FOUND_ROWS t.loginCryDef, t.cusCryDef,
            t.Cid,t.UpId,t.HallId, t.NameC, t.NameG, t.NameE, t.gameId, t.ggId, t.rounds, t.BaseRounds, t.UserName, t.Currency,t.ExCurrency,t.IsDemo,t.gameState,
            SUM(t.betGold) AS betGold, SUM(t.jpGold) AS jpGold, SUM(t.betPoint) AS betPoint, SUM(t.jpPoint) AS jpPoint, SUM(t.winGold) AS winGold, 
            SUM(t.realBetGold) AS realBetGold, SUM(t.jpConGoldOriginal) AS jpConGoldOriginal, 
            SUM(TRUNCATE(t.betGold *${CryDef},2) ) AS convertBetGold, 
            SUM(TRUNCATE(t.realBetGold *${CryDef},2) ) AS convertRealBetGold, 
            SUM(TRUNCATE(t.payoutAmount *${CryDef},2)) AS convertPayoutAmount, 
            SUM(TRUNCATE(t.winGold *${CryDef},2) ) AS convertWinGold, 
            SUM(TRUNCATE(t.jpGold *${CryDef},2) ) AS convertJPGold, 
            SUM(TRUNCATE(t.jpConGoldOriginal *${CryDef},4) ) AS convertJPConGoldOriginal, 
            SUM(TRUNCATE(t.NetWin *${CryDef},2) ) AS convertNetWin, 
            (SUM(TRUNCATE(t.winGold *${CryDef},2) )/ SUM( TRUNCATE(t.betGold *${CryDef},2) ) )*100 AS RTP,
            (SUM(TRUNCATE(t.winGold *${CryDef},2) )/ SUM( TRUNCATE(t.betGold *${CryDef},2) ) )*100 AS netRTP
            FROM (
                SELECT
                ( SELECT cusRate.CryDef FROM game.currency_exchange_rate cusRate ${sql_loginCryDef_Where}
                ( SELECT cusRate.CryDef FROM game.currency_exchange_rate cusRate ${sql_custCryDef_Where}
                w.Cid,w.UpId,w.HallId, g.NameC, g.NameG, g.NameE, w.GameId AS gameId, w.GGId AS ggId, count(w.Wid) AS rounds, count(CASE WHEN w.IsFreeGame = 0 THEN 1 ELSE NULL END) AS BaseRounds, w.UserName AS UserName, w.Currency AS Currency,w.ExCurrency,w.IsDemo,
                SUM(TRUNCATE(w.BetGold /1000,2)) AS betGold,
                SUM(TRUNCATE(w.BetPoint /1000,2)) AS betPoint,
                SUM(TRUNCATE(w.RealBetGold /1000,2)) AS realBetGold,
                SUM(TRUNCATE(w.WinGold /1000,2)) AS winGold ,
                SUM(TRUNCATE(w.WinGold /1000,2)-TRUNCATE(w.JPGold /1000,2)) AS payoutAmount ,
                SUM(TRUNCATE(w.JPGold /1000,2)) AS jpGold,
                SUM(TRUNCATE(w.JPPoint /1000,2)) AS jpPoint,
                SUM(TRUNCATE(w.JPConGoldOriginal /1000,4)) AS jpConGoldOriginal,
                SUM(TRUNCATE(w.RealBetGold /1000,2))-SUM(TRUNCATE(w.WinGold /1000,2)) AS NetWin,
                (SUM(w.WinGold)/SUM(w.BetGold))*100 AS RTP,
                (CASE WHEN IsFreeGame = true THEN 'IsFree' WHEN IsBonusGame = true THEN 'IsBonus' ELSE 'IsBase'  END) AS gameState
                FROM wagers_bet w
                LEFT JOIN game.games g ON (w.GameId = g.gameId)
                WHERE IsValid = 1 AND ${sql_where.join(" AND ")}
                GROUP BY w.Cid, w.GameId
            ) t
            GROUP BY t.Cid,t.GameId
            ${order_by_text}
            ${sql_limit}
        `
    let args = [...args_loginCryDef_Where, ...args_custCryDef_Where, ...argsWhere]

    var sql2 = "SELECT FOUND_ROWS() AS ROWS;"
    let args2 = []

    db.act_query_multi("dbclient_w_r", [sql, sql2], [args, args2], function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var data = {
          count: r_data[1][0]["ROWS"],
          info: r_data[0],
        }
        cb(null, r_code, data)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][getUserRevenue_game] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//數據
bettingDao.getBetInfo = function (data, cb) {
  try {
    if (typeof data == "undefined" || typeof data.start_date == "undefined" || typeof data.end_date == "undefined") {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }

    var sql_where = []
    if (data.start_date != "") {
      sql_where.push(sprintf("w.AccountDate >= '%s' ", data.start_date))
    }
    if (data.end_date != "") {
      sql_where.push(sprintf("w.AccountDate <= '%s' ", data.end_date))
    }
    switch (data.level) {
      case 1:
        // Admin 找 user_revenue_hall & 找最上層Hall的帳
        sql_where.push("w.Upid = '-1'")
        break
      case 2:
        // Hall 找 user_revenue_hall & 找自己的帳
        sql_where.push(sprintf("w.Cid = '%s'", data.user_hallId))
        // sql_where.push(sprintf("w.HallId = '%s'", data.user_hallId));
        break
      case 3:
        // Agent 找 user_revenue_agent & 找自己的帳
        sql_where.push(sprintf("w.Cid = '%s'", data.user_agentId))
        // sql_where.push(sprintf("w.UpId = '%s'", data.user_agentId));
        break
    }

    /*
        var sql = sprintf("SELECT FORMAT(count(Wid),0) AS rounds, IFNULL(FORMAT(SUM(BetGold*betCryDef*betCryDef),2),0) AS betGold, \
            IFNULL(FORMAT(SUM(WinGold*betCryDef*betCryDef),2),0) AS winGold, IFNULL(FORMAT(SUM(netWin*betCryDef*betCryDef),2),0) AS GGR \
            FROM ( \
            SELECT w.Wid,w.BetGold,(w.WinGold-w.JPGold) AS WinGold,(w.BetGold-(w.WinGold-w.JPGold)) AS netWin,w.Currency AS betCurrency, w.CryDef AS betCryDef,\
            CASE WHEN w.ExCurrency = '" + data.mainCurrency + "' THEN 1 ELSE ( SELECT CryDef \
            FROM currency_exchange_rate cusRate \
            WHERE (cusRate.ExCurrency = '%s' AND cusRate.EnableTime <= w.AddDate) \
            ORDER BY cusRate.EnableTime DESC \
            LIMIT 0,1 ) END AS cusCryDef  \
            FROM  wagers_bet w \
            WHERE %s  \
        ) t ", data.opCurrency, sql_where.join(" AND "));
        */

    var sql = sprintf(
      "SELECT IFNULL(FORMAT(SUM(Rounds),0),0) AS rounds, IFNULL(FORMAT(SUM(BetGold*betCryDef/cusCryDef),2),0) AS betGold, \
            IFNULL(FORMAT(SUM(WinGold*betCryDef/cusCryDef),2),0) AS winGold, IFNULL(FORMAT(SUM(netWin*betCryDef/cusCryDef),2),0) AS GGR \
            FROM ( \
            SELECT w.Rounds,w.BetGold,(w.WinGold-w.JPGold) AS WinGold,(w.BetGold-(w.WinGold-w.JPGold)) AS netWin,w.Currency AS betCurrency, w.CryDef AS betCryDef,\
            CASE WHEN w.ExCurrency = '" +
        data["mainCurrency"] +
        "' THEN 1 ELSE ( SELECT CryDef \
            FROM game.currency_exchange_rate cusRate \
            WHERE (cusRate.Currency = '%s' AND cusRate.ExCurrency = w.ExCurrency AND cusRate.EnableTime <= w.AccountDate) \
            ORDER BY cusRate.EnableTime DESC \
            LIMIT 0,1 ) END AS cusCryDef  \
            FROM %s w \
            WHERE %s  \
            ) t ",
      data["mainCurrency"],
      "wagers_1." + data.sel_table,
      sql_where.join(" AND ")
    )

    var args = []
    console.log("-getBetInfo sql-", sql)
    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][getBetInfo] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 判斷 MySQL 有無此注單
bettingDao.checkWagerIsExist = function (data, cb) {
  try {
    if (typeof data == "undefined" || typeof data.wId == "undefined") {
      cb(null, { code: code.FAIL, msg: "data || wId is undefined" }, null)
      return
    }

    if (data.wId.length == 0) {
      cb(null, { code: code.OK }, [])
      return
    }

    let wIds = data.wId.join("','")

    var sql = "SELECT Wid FROM wagers_bet WHERE Wid IN('" + wIds + "')"
    var args = []
    console.log("[bettingDao][checkWagerIsExist][sql]", sql)
    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][checkWagerIsExist] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 判斷 Mongo 有無此注單
bettingDao.checkMongoWagerIsExist = function (data, cb) {
  try {
    if (
      typeof data == "undefined" ||
      typeof data.wId == "undefined" ||
      typeof data.tableName == "undefined" ||
      typeof data.dbName == "undefined"
    ) {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }

    if (data.wId.length == 0) {
      cb(null, { code: code.OK }, [])
      return
    }

    const db = getMongoConnection(dbName)
    db.collection(data.tableName)
      .aggregate([
        {
          $match: { _id: { $in: data.wId } },
        },
        {
          $project: {
            _id: 1,
          },
        },
      ])
      .toArray(function (err, res) {
        if (err) {
          console.log("[bettingDao][checkMongoWagerIsExist] mongo err: ", err)
          cb(null, { code: code.DB.QUERY_FAIL }, null)
        } else {
          cb(null, { code: code.OK }, res)
        }
      })
  } catch (err) {
    logger.error("[bettingDao][checkMongoWagerIsExist] catch err", err)
    cb(null, { code: code.FAIL }, null)
  }
}

// 補單
bettingDao.addWagers = function (data, cb) {
  try {
    if (typeof data == "undefined") {
      cb(null, { code: code.FAIL }, null)
      return
    }

    let self = this
    m_async.waterfall(
      [
        function (callBack) {
          if (!data.mysql_flag) {
            callBack(null, { code: code.OK }, null)
            return
          }
          self.addMysqlWagers(data.mysql_list, callBack) // 補 MySQL
        },
        function (r_code, r_data, callBack) {
          if (r_code.code !== code.OK) {
            logger.error("[bettingDao][addWagers] addMysqlWagers fail msg:", r_code.msg)
            cb(null, { code: r_code.code }, null)
            return
          }

          if (!data.mongo_flag) {
            callBack(null, { code: code.OK }, null)
            return
          }

          let mongoDbName = ""

          switch (data.wagerGameType) {
            case 1: // 魚機
              for (let i in data.mongo_list) {
                data.mongo_list[i].createTime = new Date(data.mongo_list[i].createTime + "Z") // 轉為時間型態
              }

              mongoDbName = MONGO_FISH_HUNTER_DB
              break
          }
          self.addMongoWagers(data.mongo_list, mongoDbName, data.mongoTableName.mom, callBack) // 補 Mongo母單
        },
        function (r_code, r_data, callBack) {
          if (r_code.code !== code.OK) {
            logger.error("[bettingDao][addWagers] addMongoWagers fail msg:", r_code.msg)
            cb(null, { code: r_code.code }, null)
            return
          }

          if (typeof data.sub_list == "undefined" || data.sub_list.length == 0) {
            callBack(null, { code: code.OK }, null)
            return
          }
          switch (data.wagerGameType) {
            case 1: // 魚機
              for (let i in data.mongo_list) {
                data.sub_list[i].finishTime = new Date(data.sub_list[i].finishTime + "Z") // 轉為時間型態
              }
              break
          }
          self.addMongoWagers(data.sub_list, data.mongoTableName.sub, callBack) // 補 Mongo子單
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code !== code.OK) {
          logger.error("[bettingDao][addWagers] addMongoWagers sub record fail msg:", r_code.msg)
          cb(null, { code: r_code.code }, null)
          return
        }
        cb(null, { code: code.OK }, null)
      }
    )
  } catch (err) {
    logger.error("[bettingDao][addWagers] catch err", err)
    cb(null, { code: code.FAIL }, null)
  }
}

bettingDao.addMysqlWagers = function (dataList, callBack) {
  try {
    if (dataList.length == 0) {
      callBack(null, { code: code.FAIL, msg: "No wager data." }, null)
      return
    }

    var field = []
    let insert_sql = []
    for (let i in dataList) {
      let insertData = []
      Object.keys(dataList[i]).forEach((item) => {
        if (i == 0) field.push(item)
        insertData.push("'" + dataList[i][item] + "'")
      })
      insert_sql.push("(" + insertData.join(", ") + ")")
    }

    var sql = "INSERT INTO wagers_bet (" + field.join(", ") + ") VALUES " + insert_sql.join(", ")
    let args = []
    db.act_query("dbclient_w_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        callBack(null, { code: r_code.code, msg: "MySQL fail" + JSON.stringify(r_code) }, null)
      } else {
        callBack(null, { code: code.OK }, null)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][addMysqlWagers] catch err", err)
    callBack(null, { code: code.DB.CREATE_FAIL, msg: "Add mysql wagers fail." }, null)
  }
}

bettingDao.addMongoWagers = function (dataList, dbName, tableName, callBack) {
  try {
    if (dataList.length == 0) {
      callBack(null, { code: code.FAIL, msg: "No mongo wager data." }, null)
      return
    }

    const db = getMongoConnection(dbName)
    const collection = db.collection(tableName)
    collection.insertMany(dataList).then((res) => {
      callBack(null, { code: code.OK }, null)
    })
  } catch (err) {
    logger.error("[bettingDao][addMongoWagers] catch err", err)
    callBack(null, { code: code.DB.CREATE_FAIL, msg: "Add mongo wagers fail." }, null)
  }
}

bettingDao.getBetNums_byHour = function (data, cb) {
  try {
    if (
      typeof data == "undefined" ||
      typeof data.start_date == "undefined" ||
      typeof data.end_date == "undefined" ||
      !data.level
    ) {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }
    var sql_where = []
    var sql_where_text = ""
    if (data.start_date != "") {
      sql_where.push(sprintf("w.AccountDate >= '%s' ", data.start_date))
    }
    if (data.end_date != "") {
      sql_where.push(sprintf("w.AccountDate <= '%s' ", data.end_date))
    }

    var select_table = ""
    switch (data.level) {
      case 1: // admin
        select_table = "user_revenue_hall"
        sql_where.push("w.Upid = '-1'")
        break
      case 2: // hall
        select_table = "user_revenue_hall"
        // select_table = 'user_revenue_agent';
        sql_where.push(sprintf("w.Cid = '%s'", data.user_hallId))
        break
      case 3: // agent
        select_table = "user_revenue_agent"
        // select_table = 'user_revenue_player';
        sql_where.push(sprintf("w.Cid = '%s'", data.user_agentId))
        break
    }

    sql_where_text = sql_where.join(" AND ")

    // var sql = sprintf("SELECT count(Wid) AS rounds , DATE_FORMAT(w.AddDate,'%%Y-%%m-%%d') AS serverDate, DATE_FORMAT(w.AddDate,'%%H') AS serverHour, \
    //  DATE_FORMAT(date_add(w.AddDate,interval %i HOUR),'%%Y-%%m-%%d') AS clientDate, DATE_FORMAT(date_add(w.AddDate,interval %i HOUR),'%%H') AS clientHour  \
    //  FROM wagers_bet w WHERE %s \
    //  GROUP BY DATE_FORMAT(w.AddDate,'%%Y-%%m-%%d %%H') \
    //  ORDER BY DATE_FORMAT(w.AddDate,'%%Y-%%m-%%d %%H') ASC ", data.timeDiff, data.timeDiff, sql_where.join(" AND "));

    const sql = `SELECT SUM(Rounds) AS rounds,
        DATE_FORMAT(convert_tz(w.AccountDate, "+00:00", ? ), "%H" ) AS clientLocalHour
        FROM ${select_table} w WHERE ${sql_where_text} 
        GROUP BY clientLocalHour
        ORDER BY clientLocalHour ASC
        `

    const args = [data.hourDiffFormated]

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][getBetNums_byHour] catch err", err)
    cb(null, code.FAIL, null)
  }
}

bettingDao.get_wagersInfo_byWid = function (data, cb) {
  try {
    if (typeof data == "undefined" || typeof data.wid == "undefined") {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }
    var sql_where = []
    var sql_wid = data.wid.length > 0 ? " Wid IN ('" + data.wid.join("','") + "') " : " Wid='' "
    sql_where.push(sql_wid)
    var sql =
      "SELECT Wid,FORMAT(BetGold,2) AS BetGold,FORMAT((WinGold-JPGold),2) AS WinGold,FORMAT((BetGold-(WinGold-JPGold)),2) AS GGR FROM wagers_bet WHERE " +
      sql_where.join(" AND ")
    var args = []
    console.log("-get_wagersInfo_byWid-", sql)
    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][get_wagersInfo_byWid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/**
 * 組成排程統計的SQL語法
 * @param {object}} user_revenue_player
 * @returns
 */
bettingDao.combineUserRevenueSQL = function (user_revenue_player) {
  try {
    var arr = [
      "INSERT INTO " + user_revenue_player["table"] + "(",
      Object.keys(user_revenue_player["map"]).join(","),
      ") SELECT",
      Object.keys(user_revenue_player["map"]).join(","),
      "FROM ( SELECT",
      Object.values(user_revenue_player["map"]).join(","),
      "FROM ",
      user_revenue_player["from_table"],
      // 判斷是否需查詢代理為正式或測試帳號
      user_revenue_player["join"] != undefined ? " LEFT JOIN " + user_revenue_player["join"] : "",
      " WHERE",
      user_revenue_player["where"],
      "GROUP BY",
      user_revenue_player["group"],
      ") tempTable",
      " ON DUPLICATE KEY UPDATE ",
    ]
    var sql = arr.join(" ") + user_revenue_player["duplicate"].join(" , ") + ";"
    return sql
  } catch (err) {
    logger.error("[bettingDao][combineUserRevenueSQL] catch err", err)
  }
}

bettingDao.deleteRevenue = function (data, cb) {
  try {
    let sql_query = []

    let sqlUser = sprintf(
      "DELETE FROM wagers_1.user_revenue_hall WHERE AccountDate >= '%s' AND AccountDate < '%s'",
      data.begin_datetime,
      data.end_datetime
    )
    sql_query.push(sqlUser)

    let sqlGame = sprintf(
      "DELETE FROM wagers_1.game_revenue_hall WHERE AccountDate >= '%s' AND AccountDate < '%s'",
      data.begin_datetime,
      data.end_datetime
    )
    sql_query.push(sqlGame)

    db.act_query_multi("dbclient_w_rw", sql_query, [], function (r_code, r_data) {
      if (r_code.code != code.OK) {
        logger.error("[bettingDao][deleteRevenue] act_transaction failed! err: ", r_code)
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][deleteRevenue] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/**
 * FUNKY 重新結算
 *
 * @param {object} data
 * @param {*} cb
 */
bettingDao.updateFunkyRevenueReport = function (data, cb) {
  try {
    let updateTime_start = timezone.formatTime(Date.now(), "YYYY-MM-DD HH:mm:ss.SSS")
    logger.warn("[bettingDao][updateFunkyRevenueReport][startTime] :", updateTime_start)

    const { startTime, endTime, hallId } = data

    const sql = `
  INSERT INTO
    wagers_1.funky_revenue (
      Id,
      AccountDate,
      StatementDate,
      Fpid,
      GameId,
      Currency,
      WagerCount,
      Username,
      Cid,
      Upid,
      IsTestAccount,
      BetGold,
      RealBetGold,
      WinGold,
      JPGold,
      JPConGoldOriginal
    )
  SELECT
    Id,
    AccountDate,
    StatementDate,
    Fpid,
    GameId,
    Currency,
    COUNT(WagerCount) as WagerCount,
    Username,
    Cid,
    Upid,
    IsTestAccount,
    SUM(BetGold),
    SUM(RealBetGold),
    SUM(WinGold),
    SUM(JPGold),
    SUM(JPConGoldOriginal)
  FROM
    (
      SELECT
        CONCAT_WS(
          "-",
          PlatformWid,
          MAX(left(AddDate, 15)),
          Result,
          GameId,
          Currency,
          Cid
        ) AS Id,
        MAX(CONCAT(LEFT(AddDate, 15), '0:00')) AS AccountDate,
        Result AS StatementDate,
        PlatformWid AS Fpid,
        GameId,
        Currency,
        COUNT(*) AS WagerCount,
        MAX(UserName) AS Username,
        Cid,
        MAX(UpId) AS UpId,
        IsDemo AS IsTestAccount,
        SUM(TRUNCATE(BetGold / 1000, 2) * 1000) AS BetGold,
        SUM(TRUNCATE(RealBetGold / 1000, 2) * 1000) AS RealBetGold,
        SUM(TRUNCATE(WinGold / 1000, 2) * 1000) AS WinGold,
        SUM(TRUNCATE(JPGold / 1000, 2) * 1000) AS JPGold,
        SUM(TRUNCATE(JPConGoldOriginal / 1000, 4) * 1000) AS JPConGoldOriginal
      FROM
        wagers_1.wagers_bet
      WHERE
        AddDate >= ?
        AND AddDate < ?
        AND hallId = ?
        AND IsValid = 1
      GROUP BY
        Cid,
        fpid,
        Result,
        GameId,
        Currency,
        ExCurrency,
        IsDemo,
        (case when GGId != 1 then Wid end),
        (case when GGId = 1 then CycleId end)
    ) AS s
    GROUP BY 
		  Cid,
      fpid,
      StatementDate,
      GameId,
      Currency,
      IsTestAccount
    ON DUPLICATE KEY
  UPDATE
    WagerCount = WagerCount,
    BetGold = BetGold,
    RealBetGold = RealBetGold,
    WinGold = WinGold,
    JPGold = JPGold,
    JPConGoldOriginal = JPConGoldOriginal;`

    const args = [startTime, endTime, hallId]

    db.act_query("dbclient_w_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][updateFunkyRevenueReport] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/**
 * 背景排程 - 每十分鐘更新統計
 * @param {object} data
 * @param {*} cb
 */
bettingDao.updateRevenueReport = function (data, cb) {
  try {
    const sql_query = []
    let sql_text = ""
    let sql = ""
    const begin_datetime = data.begin_datetime
    const end_datetime = data.end_datetime

    let updateTime_start = timezone.formatTime(Date.now(), "YYYY-MM-DD HH:mm:ss.SSS")
    logger.warn("[bettingDao][updateRevenueReport][startTime] :", updateTime_start)

    //user_revenue_player
    const user_revenue_player = {
      table: "wagers_1.user_revenue_player",
      from_table: "wagers_1.wagers_bet AS w ",
      map: {
        Id: "MD5(CONCAT_WS('-',Cid, MAX(left(AddDate,15)),Currency,ExCurrency,CryDef )) AS Id",
        AccountDate: 'MAX(CONCAT(left(AddDate, 15), "0:00")) AS AccountDate',
        Rounds: "COUNT(Cid) AS Rounds",
        BaseRounds: "SUM(IF(IsFreeGame = 0, 1, 0)) AS BaseRounds",
        Cid: "MAX(Cid) AS Cid",
        UserName: "MAX(UserName) AS UserName",
        UpId: "MAX(UpId) AS UpId",
        HallId: "MAX(HallId) AS HallId",
        CryDef: "MAX(CryDef) AS CryDef",
        Currency: "MAX(Currency) AS Currency",
        ExCurrency: "MAX(ExCurrency) AS ExCurrency",
        IsDemo: "MAX(IsDemo) AS IsDemo",
        RealBetGold: "SUM( TRUNCATE(RealBetGold / 1000,2) * 1000 ) AS RealBetGold",
        BetGold: "SUM( TRUNCATE(BetGold / 1000,2) * 1000 ) AS BetGold",
        BetPoint: "SUM( TRUNCATE(BetPoint / 1000,2) *1000 ) AS BetPoint",
        WinGold: "SUM( TRUNCATE(WinGold / 1000,2) *1000 ) AS WinGold",
        JPPoint: "SUM( TRUNCATE(JPPoint / 1000,2) *1000 ) AS JPPoint",
        JPGold: "SUM( TRUNCATE(JPGold / 1000,2) *1000 ) AS JPGold",
        JPConGoldOriginal: "SUM( TRUNCATE(JPConGoldOriginal / 1000,4) * 1000) AS JPConGoldOriginal",
        JPRealBetGold: "SUM(IF(JPConGoldOriginal > 0, TRUNCATE(RealBetGold / 1000,2) * 1000, 0)) AS JPRealBetGold",
      },
      where: "AddDate >= '%s' AND AddDate < '%s' AND IsValid = 1 AND IsDemo = 0",
      group: "w.Cid,left(w.AddDate,15),w.Currency,w.ExCurrency,w.CryDef,w.IsDemo",
      duplicate: [
        "Rounds=tempTable.Rounds",
        "BaseRounds=tempTable.BaseRounds",
        "RealBetGold=tempTable.RealBetGold",
        "BetGold=tempTable.BetGold",
        "BetPoint=tempTable.BetPoint",
        "WinGold=tempTable.WinGold",
        "JPPoint=tempTable.JPPoint",
        "JPGold=tempTable.JPGold",
        "JPConGoldOriginal=tempTable.JPConGoldOriginal",
        "UpId=tempTable.UpId",
        "JPRealBetGold=tempTable.JPRealBetGold",
      ],
    }
    sql = bettingDao.combineUserRevenueSQL(user_revenue_player)
    sql_text = sprintf(sql, begin_datetime, end_datetime)
    logger.info("[bettingDao][updateRevenueReport][user_revenue_player][sql] %s", sql_text)
    sql_query.push(sql_text)

    //user_revenue_agent
    const user_revenue_agent = {
      table: "wagers_1.user_revenue_agent",
      from_table: "wagers_1.user_revenue_player p",
      map: {
        Id: "MD5(CONCAT_WS('-',p.UpId,p.AccountDate,p.Currency,p.ExCurrency,p.CryDef)) AS Id",
        AccountDate: "p.AccountDate",
        Rounds: "SUM(p.Rounds) AS Rounds",
        BaseRounds: "SUM(p.BaseRounds) AS BaseRounds",
        Cid: "p.UpId AS Cid",
        HallId: "MAX(p.HallId) AS HallId",
        CryDef: "p.CryDef",
        Currency: "p.Currency",
        ExCurrency: "p.ExCurrency",
        IsDemo: "MAX(p.IsDemo) AS IsDemo",
        RealBetGold: "SUM( TRUNCATE(RealBetGold / 1000,2) * 1000 ) AS RealBetGold",
        BetGold: "SUM( TRUNCATE(p.BetGold / 1000,2) * 1000 ) AS BetGold",
        BetPoint: "SUM( TRUNCATE(p.BetPoint / 1000,2) *1000 ) AS BetPoint",
        WinGold: "SUM( TRUNCATE(p.WinGold / 1000,2) *1000 ) AS WinGold",
        JPPoint: "SUM( TRUNCATE(p.JPPoint / 1000,2) *1000 ) AS JPPoint",
        JPGold: "SUM( TRUNCATE(p.JPGold / 1000,2) *1000 ) AS JPGold",
        JPConGoldOriginal: "SUM( TRUNCATE(JPConGoldOriginal / 1000,4) * 1000) AS JPConGoldOriginal",
        JPRealBetGold: "SUM(JPRealBetGold) AS JPRealBetGold",
      },
      join: "game.customer c ON c.Cid = p.UpId",
      where: "p.AccountDate >= '%s' AND p.AccountDate < '%s' AND c.IsDemo = 0",
      group: "p.UpId,p.AccountDate,p.Currency,p.ExCurrency,p.CryDef,c.IsDemo",
      duplicate: [
        "Rounds=tempTable.Rounds",
        "BaseRounds=tempTable.BaseRounds",
        "RealBetGold=tempTable.RealBetGold",
        "BetGold=tempTable.BetGold",
        "BetPoint=tempTable.BetPoint",
        "WinGold=tempTable.WinGold",
        "JPPoint=tempTable.JPPoint",
        "JPGold=tempTable.JPGold",
        "JPConGoldOriginal=tempTable.JPConGoldOriginal",
        "JPRealBetGold=tempTable.JPRealBetGold",
      ],
    }
    sql = bettingDao.combineUserRevenueSQL(user_revenue_agent)
    sql_text = sprintf(sql, begin_datetime, end_datetime)
    logger.info("[bettingDao][updateRevenueReport][user_revenue_agent][sql] %s", sql_text)
    sql_query.push(sql_text)

    //user_revenue_hall
    const user_revenue_hall = {
      table: "wagers_1.user_revenue_hall",
      from_table: "wagers_1.user_revenue_agent a",
      map: {
        Id: "MD5(CONCAT_WS('-',a.HallId, a.AccountDate,a.Currency,a.ExCurrency,a.CryDef)) AS Id",
        AccountDate: "AccountDate",
        Rounds: "SUM(a.Rounds) AS Rounds",
        BaseRounds: "SUM(a.BaseRounds) AS BaseRounds",
        Upid: "Upid",
        Cid: "a.HallId AS Cid",
        CryDef: "a.CryDef",
        Currency: "a.Currency",
        ExCurrency: "a.ExCurrency",
        RealBetGold: "SUM( TRUNCATE(RealBetGold / 1000,2) * 1000 ) AS RealBetGold",
        BetGold: "SUM( TRUNCATE(a.BetGold / 1000,2) * 1000 ) AS BetGold",
        BetPoint: "SUM( TRUNCATE(a.BetPoint / 1000,2) * 1000 ) AS BetPoint",
        WinGold: "SUM( TRUNCATE(a.WinGold / 1000,2) * 1000 ) AS WinGold",
        JPPoint: "SUM( TRUNCATE(a.JPPoint / 1000,2) * 1000 ) AS JPPoint",
        JPGold: "SUM( TRUNCATE(a.JPGold / 1000,2) * 1000 ) AS JPGold",
        JPConGoldOriginal: "SUM( TRUNCATE(JPConGoldOriginal / 1000,4) * 1000) AS JPConGoldOriginal",
        JPRealBetGold: "SUM( a.JPRealBetGold ) AS JPRealBetGold",
      },
      join: "game.customer c ON c.Cid = a.HallId",
      where: "a.AccountDate >= '%s' AND a.AccountDate < '%s'",
      group: "a.HallId,a.AccountDate,a.Currency,a.ExCurrency,a.CryDef,a.IsDemo",
      duplicate: [
        "Rounds=tempTable.Rounds",
        "BaseRounds=tempTable.BaseRounds",
        "RealBetGold=tempTable.RealBetGold",
        "BetGold=tempTable.BetGold",
        "BetPoint=tempTable.BetPoint",
        "WinGold=tempTable.WinGold",
        "JPPoint=tempTable.JPPoint",
        "JPGold=tempTable.JPGold",
        "JPConGoldOriginal=tempTable.JPConGoldOriginal",
        "JPRealBetGold=tempTable.JPRealBetGold",
      ],
    }
    sql = bettingDao.combineUserRevenueSQL(user_revenue_hall)
    sql_text = sprintf(sql, begin_datetime, end_datetime)
    logger.info("[bettingDao][updateRevenueReport][user_revenue_hall][sql] %s", sql_text)
    sql_query.push(sql_text)

    //game_revenue_player
    const game_revenue_player = {
      table: "wagers_1.game_revenue_player",
      from_table: "wagers_1.wagers_bet",
      map: {
        Id: "MD5(CONCAT_WS('-', GameId, Cid, MAX(LEFT(AddDate,15)),Currency,ExCurrency,CryDef)) AS Id",
        AccountDate: 'MAX(CONCAT(LEFT(AddDate, 15), "0:00")) AS AccountDate',
        Rounds: "count(Cid) AS Rounds",
        BaseRounds: "SUM(IF(IsFreeGame = 0, 1, 0)) AS BaseRounds",
        GGId: "MAX(GGId) AS GGId",
        GameId: "GameId",
        CryDef: "CryDef",
        Currency: "Currency",
        ExCurrency: "ExCurrency",
        Cid: "Cid",
        UserName: "MAX(UserName) AS UserName",
        UpId: "MAX(UpId) AS UpId",
        HallId: "MAX(HallId) AS HallId",
        IsDemo: "MAX(IsDemo) AS IsDemo",
        RealBetGold: "SUM( TRUNCATE(RealBetGold / 1000,2) * 1000 ) AS RealBetGold",
        BetGold: "SUM( TRUNCATE( BetGold / 1000,2) * 1000 ) AS BetGold",
        BetPoint: "SUM( TRUNCATE( BetPoint / 1000,2) * 1000 ) AS BetPoint",
        WinGold: "SUM( TRUNCATE( WinGold / 1000,2) * 1000 ) AS WinGold",
        JPPoint: "SUM( TRUNCATE( JPPoint / 1000,2) * 1000 ) AS JPPoint",
        JPGold: "SUM( TRUNCATE( JPGold / 1000,2) * 1000 ) AS JPGold",
        JPConGoldOriginal: "SUM( TRUNCATE( JPConGoldOriginal / 1000,4) * 1000) AS JPConGoldOriginal",
        JPRealBetGold: "SUM(IF(JPConGoldOriginal > 0, TRUNCATE(RealBetGold / 1000,2) * 1000, 0)) AS JPRealBetGold",
      },
      where: "AddDate >= '%s' AND AddDate < '%s' AND IsValid = 1 AND IsDemo = 0",
      group: "GameId,Cid,LEFT(AddDate,15),Currency,ExCurrency,CryDef",
      duplicate: [
        "Rounds=tempTable.Rounds",
        "BaseRounds=tempTable.BaseRounds",
        "RealBetGold=tempTable.RealBetGold",
        "BetGold=tempTable.BetGold",
        "BetPoint=tempTable.BetPoint",
        "WinGold=tempTable.WinGold",
        "JPPoint=tempTable.JPPoint",
        "JPGold=tempTable.JPGold",
        "JPConGoldOriginal=tempTable.JPConGoldOriginal",
        "GameId=tempTable.GameId",
        "JPRealBetGold=tempTable.JPRealBetGold",
      ],
    }
    sql = bettingDao.combineUserRevenueSQL(game_revenue_player)
    sql_text = sprintf(sql, begin_datetime, end_datetime)
    logger.info("[bettingDao][updateRevenueReport][game_revenue_player][sql] %s", sql_text)
    sql_query.push(sql_text)

    //game_revenue_agent
    const game_revenue_agent = {
      table: "wagers_1.game_revenue_agent",
      from_table: "wagers_1.game_revenue_player p",
      map: {
        Id: "MD5(CONCAT_WS('-', p.GameId,p.UpId,p.AccountDate,p.Currency,p.ExCurrency,p.CryDef)) AS Id",
        AccountDate: "p.AccountDate",
        Rounds: "SUM(p.Rounds) AS Rounds",
        BaseRounds: "SUM(p.BaseRounds) AS BaseRounds",
        GGId: "MAX(p.GGId) AS GGId",
        GameId: "p.GameId",
        CryDef: "p.CryDef",
        Currency: "p.Currency",
        ExCurrency: "p.ExCurrency",
        Cid: "MAX(p.UpId) AS Cid",
        HallId: "MAX(p.HallId) AS HallId",
        IsDemo: "MAX(p.IsDemo) AS IsDemo",
        RealBetGold: "SUM( TRUNCATE(RealBetGold / 1000,2) * 1000 ) AS RealBetGold",
        BetGold: "SUM( TRUNCATE(p.BetGold / 1000,2) * 1000 ) AS BetGold",
        BetPoint: "SUM( TRUNCATE(p.BetPoint / 1000,2) * 1000 ) AS BetPoint",
        WinGold: "SUM( TRUNCATE(p.WinGold / 1000,2) * 1000 ) AS WinGold",
        JPPoint: "SUM( TRUNCATE(p.JPPoint / 1000,2) * 1000 ) AS JPPoint",
        JPGold: "SUM( TRUNCATE(p.JPGold / 1000,2) * 1000 ) AS JPGold",
        JPConGoldOriginal: "SUM( TRUNCATE(JPConGoldOriginal / 1000 , 4) * 1000 ) AS JPConGoldOriginal",
        JPRealBetGold: "SUM(JPRealBetGold) AS JPRealBetGold",
      },
      join: "game.customer c ON c.Cid = p.UpId",
      where: "p.AccountDate >= '%s' AND p.AccountDate < '%s' AND c.IsDemo = 0",
      group: "p.GameId,p.UpId,p.AccountDate,p.Currency,p.ExCurrency,p.CryDef,c.IsDemo",
      duplicate: [
        "Rounds=tempTable.Rounds",
        "BaseRounds=tempTable.BaseRounds",
        "RealBetGold=tempTable.RealBetGold",
        "BetGold=tempTable.BetGold",
        "BetPoint=tempTable.BetPoint",
        "WinGold=tempTable.WinGold",
        "JPPoint=tempTable.JPPoint",
        "JPGold=tempTable.JPGold",
        "JPConGoldOriginal=tempTable.JPConGoldOriginal",
        "GameId=tempTable.GameId",
        "JPRealBetGold=tempTable.JPRealBetGold",
      ],
    }
    sql = bettingDao.combineUserRevenueSQL(game_revenue_agent)
    sql_text = sprintf(sql, begin_datetime, end_datetime)
    logger.info("[bettingDao][updateRevenueReport][game_revenue_agent][sql] %s", sql_text)
    sql_query.push(sql_text)

    //game_revenue_hall
    const game_revenue_hall = {
      table: "wagers_1.game_revenue_hall",
      from_table: "wagers_1.game_revenue_agent a",
      map: {
        Id: "MD5(CONCAT_WS('-', a.GameId, MAX(a.Cid), a.HallId,a.AccountDate,a.Currency,a.ExCurrency,a.CryDef)) AS Id",
        AccountDate: "AccountDate",
        Rounds: "SUM(a.Rounds) AS Rounds",
        BaseRounds: "SUM(a.BaseRounds) AS BaseRounds",
        GGId: "MAX(a.GGId) AS GGId",
        GameId: "a.GameId",
        CryDef: "a.CryDef",
        Currency: "a.Currency",
        ExCurrency: "a.ExCurrency",
        Upid: "c.Upid AS Upid",
        Cid: " MAX(a.HallId) AS Cid",
        RealBetGold: "SUM( TRUNCATE(RealBetGold / 1000,2) * 1000 ) AS RealBetGold",
        BetGold: "SUM( TRUNCATE(a.BetGold / 1000,2) * 1000 ) AS BetGold",
        BetPoint: "SUM( TRUNCATE(a.BetPoint / 1000,2) * 1000) AS BetPoint",
        WinGold: "SUM( TRUNCATE(a.WinGold / 1000,2) * 1000) AS WinGold",
        JPPoint: "SUM( TRUNCATE(a.JPPoint / 1000,2) * 1000) AS JPPoint",
        JPGold: "SUM( TRUNCATE(a.JPGold / 1000,2) * 1000) AS JPGold",
        JPConGoldOriginal: "SUM( TRUNCATE(JPConGoldOriginal / 1000 , 4) * 1000 ) AS JPConGoldOriginal",
        JPRealBetGold: "SUM(JPRealBetGold) AS JPRealBetGold",
      },
      join: "game.customer c ON c.Cid = a.HallId",
      where: "a.AccountDate >= '%s' AND a.AccountDate < '%s'",
      group: "a.GameId,a.HallId,a.AccountDate,a.Currency,a.ExCurrency,a.CryDef",
      duplicate: [
        "Rounds=tempTable.Rounds",
        "BaseRounds=tempTable.BaseRounds",
        "RealBetGold=tempTable.RealBetGold",
        "BetGold=tempTable.BetGold",
        "BetPoint=tempTable.BetPoint",
        "WinGold=tempTable.WinGold",
        "JPPoint=tempTable.JPPoint",
        "JPGold=tempTable.JPGold",
        "JPConGoldOriginal=tempTable.JPConGoldOriginal",
        "GameId=tempTable.GameId",
        "JPRealBetGold=tempTable.JPRealBetGold",
      ],
    }
    sql = bettingDao.combineUserRevenueSQL(game_revenue_hall)
    sql_text = sprintf(sql, begin_datetime, end_datetime)
    logger.info("[bettingDao][updateRevenueReport][game_revenue_hall][sql] %s", sql_text)
    sql_query.push(sql_text)

    db.act_transaction("dbclient_w_rw", sql_query, function (r_code, r_data) {
      console.log("r_code", JSON.stringify(r_code), JSON.stringify(r_data))

      let updateTime_end = timezone.formatTime(Date.now(), "YYYY-MM-DD HH:mm:ss.SSS")
      logger.warn("[bettingDao][updateRevenueReport][endTime] :", updateTime_end)
      logger.warn(
        "[bettingDao][updateRevenueReport][executionTime] :",
        timezone.isDiff(updateTime_end, updateTime_start) + "ms"
      )

      // 防止 store procedure 執行結帳過久，handler 等待超過 30 秒會 timeout，所以先 cb 出去
      bettingDao.updateContent(updateTime_end.substr(0, 19), cb)

      if (r_code.code === code.OK) {
        bettingDao.updateUserRevenueHall(begin_datetime, end_datetime, function (r_code, r_data) {
          // 更新 system 的 last_revenue_times
          bettingDao.updateContent(updateTime_end.substr(0, 19), function (r_code, r_data) {
            cb(null, r_code, r_data)
          })
        })
      }
    })
  } catch (err) {
    logger.error("[bettingDao][updateRevenueReport] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//取注單資料
bettingDao.get_wagers_byWid = function (data, cb) {
  try {
    //母單+子單
    var sql_query = []
    var sql = ""
    var wagers = []
    if (data.wager_type == 1 || data.wager_type == 2) {
      sql = sprintf("SELECT * FROM wagers_bet WHERE Wid='%s'  ", data.Wid)
      sql_query.push(sql)
      wagers.push("wagers_bet")
    }

    if (data.wager_type == 1 || data.wager_type == 3) {
      sql = sprintf("SELECT * FROM wagers_detail_egame WHERE Wid='%s'  ", data.Wid)
      sql_query.push(sql)
      wagers.push("wagers_detail_egame")
    }

    db.act_transaction("dbclient_w_r", sql_query, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var res_data = []
        wagers.forEach((item, idx) => {
          var info = {}
          info[item] = r_data[idx]
          res_data.push(info)
        })
        cb(null, { code: code.OK }, res_data)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][get_wagers_byWid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 尋找此遊戲是否有母單紀錄
bettingDao.isGameInWagerRecords = function (data, cb) {
  try {
    const sqlString = `SELECT COUNT(GameId) AS gameIdCounts FROM wagers_bet WHERE GameId=${data.gameId}`
    const args = null

    db.act_query("dbclient_w_r", sqlString, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        const { gameIdCounts } = r_data[0]
        const result = gameIdCounts > 0 ? true : false

        cb(null, { code: code.OK }, result)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][isGameInWagerRecords] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//母單
bettingDao.get_wagers_bet_byWid = function (data, cb) {
  try {
    var sql = sprintf(
      "SELECT Wid,IsJP,JPType,Cid,UpId,HallId,GameId,JPGold,DATE_FORMAT(AddDate,'%%Y-%%m-%%d %%H:%%i:%%s') AS AddDate FROM wagers_bet WHERE Wid='%s'  ",
      data.Wid
    )
    var args = []
    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][get_wagers_bet_byWid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

bettingDao.getGameHotCount = function (data, cb) {
  try {
    // 7天內的點擊數
    var end_date = timezone.transTime("", -7, "day")
    var sql =
      "SELECT SUM(Rounds) AS COUNT,GameId FROM game_revenue_hall WHERE GameId IN('" +
      data.gameId.join("','") +
      "') \
                AND AccountDate  >= '" +
      end_date +
      "'\
                GROUP BY GameId \
                ORDER BY COUNT DESC ;"

    /*
        var sql = "SELECT COUNT(bet.Wid) AS COUNT,bet.GameId \
                FROM wagers_1.wagers_bet bet \
                WHERE bet.GameId IN('" + data.gameId.join("','") + "') \
                AND bet.AddDate >= '" + end_date + "'\
                GROUP BY bet.GameId \
                ORDER BY Count DESC ";
                */
    var args = []
    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][getGameHotCount] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 遊戲類別押注貢獻
bettingDao.getContribution_byGame = function (data, cb) {
  try {
    if (
      typeof data.start_date === "undefined" ||
      typeof data.end_date === "undefined" ||
      typeof data.user_level === "undefined" ||
      typeof data.mainCurrency === "undefined" ||
      typeof data.ggIds === "undefined" ||
      typeof data.currency === "undefined"
    ) {
      cb(
        null,
        {
          code: code.FAIL,
          msg: "start_date || end_date || user_level || ggIds || currency || mainCurrency = undefined !!",
        },
        null
      )
      return
    }

    var sql_where = []
    var sql_where_text = ""

    switch (data["user_level"]) {
      case 1: // AD
        data["sel_table"] = "wagers_1.game_revenue_hall"
        sql_where.push(" r.Upid = '-1' ")
        break
      case 2: // HA
        data["sel_table"] = "wagers_1.game_revenue_hall"
        sql_where.push(" r.Cid = '" + data["user_hallId"] + "' ")
        // sql_where.push(" r.HallId = '" + data['user_hallId'] + "' ");
        break
      case 3: // AG
        data["sel_table"] = "wagers_1.game_revenue_agent"
        sql_where.push(" r.Cid = '" + data["user_agentId"] + "' ")
        break
    }

    sql_where.push(" r.GGId IN(" + data["ggIds"] + ")")

    // 取range
    if (typeof data.start_date !== "undefined" && typeof data.end_date !== "undefined") {
      sql_where.push(" (r.AccountDate >= '" + data.start_date + "' AND r.AccountDate <= '" + data.end_date + "' ) ")
    }
    sql_where_text = " 1 AND " + sql_where.join(" AND ")

    var sql =
      " SELECT t.GGID, t.betCryDef, t.betCurrency, t.cusCryDef, FORMAT(SUM(t.betCryDef * t.BetGold / t.cusCryDef),2) AS totalBet " +
      " FROM ( " +
      " SELECT r.GGID, r.CryDef AS betCryDef, r.Currency AS betCurrency, r.ExCurrency AS betExCurrency, r.BetGold, " +
      " CASE WHEN r.ExCurrency = '" +
      data["currency"] +
      "' THEN 1" +
      // 下線幣別 與 登入者幣別 不相同
      " ELSE ( SELECT cusRate.CryDef FROM game.currency_exchange_rate cusRate" +
      " WHERE ( cusRate.Currency = '" +
      data["mainCurrency"] +
      "'  AND cusRate.ExCurrency = r.ExCurrency" +
      " AND cusRate.EnableTime <= r.AccountDate) " +
      " ORDER BY cusRate.EnableTime DESC LIMIT 0,1) END AS cusCryDef" +
      " FROM " +
      data["sel_table"] +
      " r " +
      " WHERE " +
      sql_where_text +
      " ) t  " +
      " GROUP BY GGID, betCurrency "
    var args = []
    console.log("-getContribution_byGame sql -", sql)

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][getContribution_byGame] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 取得 Reseller & subReseller 押注貢獻
bettingDao.getContribution_byBet = function (data, cb) {
  try {
    if (typeof data.start_date === "undefined" || typeof data.end_date === "undefined") {
      cb(null, { code: code.FAIL, msg: "start_date || end_date = undefined !!" }, null)
      return
    }
    var sql_where = []
    var sql_where_text = ""
    let group_by = []
    let group_by_text = ""
    let field = []
    let field_text = ""
    let cus_where_text = ""

    switch (data["user_level"]) {
      case 1: // AD
        data["sel_table"] = "wagers_1.user_revenue_hall"
        sql_where.push(" r.Upid = '-1' ")
        cus_where_text = " Cid = r.Cid AND IsAg = 2 " // 找 Hall userName
        group_by.push("Cid")
        break
      case 2: // HA
        data["sel_table"] = "wagers_1.user_revenue_agent"
        if (typeof data["subResellers"] !== "undefined") {
          // Group by HallId
          // subHall
          // data['sel_table'] = "wagers_1.user_revenue_hall";
          cus_where_text = " Cid = r.HallId AND IsAg = 2 " // 找 Hall userName
          sql_where.push(" r.HallId IN ('" + data["subResellers"].join("','") + "') ")
          group_by.push("HallId")
          field.push("r.HallId")
        } else {
          sql_where.push(" r.HallId = '" + data["user_hallId"] + "' ")
          cus_where_text = " Cid = r.Cid AND IsAg = 3 " // 找 Agent userName
          group_by.push("Cid")
        }
        break
      case 3: // AG
        data["sel_table"] = "wagers_1.user_revenue_agent"
        sql_where.push(" r.Cid = '" + data["user_agentId"] + "' ")
        cus_where_text = " Cid = r.Cid AND IsAg = 3 " // 找 Agent userName
        group_by.push("'Cid'")
        break
    }
    // 取range
    if (typeof data.start_date !== "undefined" && typeof data.end_date !== "undefined") {
      sql_where.push(" (r.AccountDate >= '" + data.start_date + "' AND r.AccountDate <= '" + data.end_date + "' ) ")
    }
    sql_where_text = " 1 AND " + sql_where.join(" AND ")

    if (group_by.length > 0) group_by_text = ", " + group_by.join(", ")
    if (field.length > 0) field_text = field.join("") + ", "
    var sql =
      " SELECT * , FORMAT(SUM(t.betCryDef * t.BetGold / t.cusCryDef),2) AS totalBet" +
      " FROM ( " +
      " SELECT r.Cid, r.CryDef AS betCryDef, r.Currency AS betCurrency, r.ExCurrency AS betExCurrency, r.BetGold,  " +
      field_text +
      " CASE WHEN r.ExCurrency = '" +
      data["currency"] +
      "' THEN 1" +
      " ELSE ( SELECT cusRate.CryDef FROM game.currency_exchange_rate cusRate" +
      " WHERE ( cusRate.Currency = '" +
      data["mainCurrency"] +
      "' AND cusRate.ExCurrency = r.ExCurrency" +
      " AND cusRate.EnableTime <= r.AccountDate )" +
      " ORDER BY cusRate.EnableTime DESC LIMIT 0,1 ) END AS cusCryDef," +
      " ( SELECT UserName FROM game.customer WHERE " +
      cus_where_text +
      " ) AS userName" +
      " FROM " +
      data["sel_table"] +
      " r" +
      " WHERE " +
      sql_where_text +
      " ) t" +
      " GROUP BY betCurrency" +
      group_by_text +
      " ASC"
    var args = []
    console.log("-getContribution_byBet sql -", sql)
    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][getContribution_byBet] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 取得廳主人數貢獻
bettingDao.getContribution_byUserCount = function (data, cb) {
  try {
    if (typeof data.start_date === "undefined" || typeof data.end_date === "undefined") {
      cb(null, { code: code.FAIL, msg: "start_date || end_date = undefined !!" }, null)
      return
    }
    var sql_where = []
    var sql_where_text = ""

    switch (data["user_level"]) {
      case 1: // AD
        sql_where.push(" HallId IN ('" + data["userCountIds"].join("','") + "') ")
        data["group"] = "HallId"
        break
      case 2: // HA
        if (typeof data["subResellers"] !== "undefined") {
          // subHall
          sql_where.push(" HallId IN ('" + data["subResellers"].join("','") + "') ")
          data["group"] = "HallId"
        } else {
          sql_where.push(" HallId ='" + data["userCountIds"].join("','") + "' ")
          data["group"] = "UpId"
        }
        break
      case 3: // AG
        sql_where.push(" UpId ='" + data["userCountIds"].join("','") + "' ")
        data["group"] = "UpId"
        break
    }

    // 取range
    if (typeof data["start_date"] !== "undefined" && typeof data["end_date"] !== "undefined") {
      sql_where.push(" (AccountDate >= '" + data.start_date + "' AND AccountDate <= '" + data.end_date + "' ) ")
    }
    sql_where_text = " 1 AND " + sql_where.join(" AND ")

    var sql_query = sprintf(
      "SELECT HallId, UpId, COUNT(DISTINCT Cid) AS userCount FROM user_revenue_player WHERE %s GROUP BY %s ASC",
      sql_where_text,
      data["group"]
    )
    var args = []
    console.log("-getContribution_byUserCount sql -", sql_query)

    db.act_query("dbclient_w_r", sql_query, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][getContribution_byUserCount] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 更新 system 的 last_revenue_times(執行統計結束時間)
bettingDao.updateContent = function (data, cb) {
  try {
    if (typeof data == "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var table = "system"
    var saveData = {
      content: data,
    }

    var sql_where_text = sprintf(" item = 'last_revenue_times'")

    db.act_update_data("dbclient_g_rw", table, saveData, sql_where_text, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][updateContent] catch err", JSON.stringify(err))
    cb(null, { code: code.FAIL }, null)
  }
}

bettingDao.updateUpid_revenue = function (data, cb) {
  try {
    if (typeof data == "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var sql = "UPDATE wagers_bet SET UpId = ? WHERE Cid = ?"
    var args = [data.New_Upid, data.operate_acct]

    db.act_query("dbclient_w_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[bettingDao][updateUpid_byPlayer] catch err", err)
    cb(null, code.FAIL, null)
  }
}

bettingDao.updateDeleteUpid_revenue = function (data, cb) {
  try {
    if (typeof data == "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var sql_cId = " Cid IN ('" + data.Cids.join("','") + "') "
    var column = ""
    var update_sql = ""
    var del_agent_sql = ""
    var del_hall_sql = ""

    switch (data.Operate_IsAg) {
      case 2:
        del_hall_sql = sprintf("DELETE FROM user_revenue_hall WHERE Cid = '%s'", data.operate_acct)
        break
      case 3:
        column = "HallId = '" + data.New_Upid + "'"
        update_sql = "UPDATE wagers_bet SET " + column + " WHERE " + sql_cId
        del_hall_sql = sprintf("DELETE FROM user_revenue_hall WHERE Cid = '%s'", data.Operate_HallId)
        break
      case 4:
        column = "UpId = '" + data.New_Upid + "'"
        update_sql = "UPDATE wagers_bet SET " + column + " WHERE " + sql_cId
        del_agent_sql = sprintf("DELETE FROM user_revenue_agent WHERE Cid = '%s'", data.Old_Upid)
        del_hall_sql = sprintf("DELETE FROM user_revenue_hall WHERE Cid = '%s'", data.Operate_HallId)
        break
    }

    var sql_query = []
    sql_query.push(update_sql)
    sql_query.push(del_agent_sql)
    sql_query.push(del_hall_sql)

    logger.info("[bettingDao][updateDeleteUpid_revenue][sql] %s", sql_query)
    pomelo.app.get("dbclient_w_rw").getConnection(function (err, connection) {
      if (err) {
        connection.release()
        cb({ code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }

      //-----------------transaction start---------------

      var funcAry = []
      sql_query.forEach(function (sql, index) {
        var temp = function (cb) {
          connection.query(sql, [], function (temp_err, results) {
            if (temp_err) {
              connection.rollback(function () {
                return cb(code.DB.QUERY_FAIL)
              })
            } else {
              return cb(null, code.ok)
            }
          })
        }
        funcAry.push(temp)
      })

      m_async.series(funcAry, function (err, result) {
        if (err) {
          connection.rollback(function (err) {
            connection.release()
            return cb(null, { code: code.DB.UPDATE_FAIL, msg: "" })
          })
        } else {
          connection.commit(function (err, info) {
            if (err) {
              connection.rollback(function (err) {
                connection.release()
                return cb({ code: code.DB.QUERY_FAIL, msg: err })
              })
            } else {
              connection.release()
              return cb(null, { code: code.OK, msg: "" })
            }
          })
        }
      })
      //-----------------transaction end---------------
    })
  } catch (err) {
    logger.error("[bettingDao][updateDeleteUpid_revenue] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// user_revenue_hall 結帳，直到第一層(最上層)時，則不再執行 - 無限階層
bettingDao.updateUserRevenueHall = function (start, end, cb) {
  try {
    let updateTime_start = timezone.formatTime(Date.now(), "YYYY-MM-DD HH:mm:ss.SSS")
    logger.info("[bettingDao][execute store procedure_UserRevenueHall][startTime] :", updateTime_start)
    let sql_query = []
    // call store procedure 執行
    let sql = sprintf("call wagers_1.sp_checkoutUserRevenueHall('%s', '%s')", start, end)
    sql_query.push(sql)

    logger.info("[bettingDao][updateUserRevenueHall][sql] %s [args] %s", sql)

    db.act_transaction("dbclient_w_rw", sql_query, function (r_code, r_data) {
      if (r_code.code != code.OK) {
        logger.error("[bettingDao][execute store procedure_UserRevenueHall] r_code: ", JSON.stringify(r_code))
        cb(null, r_code, null)
      } else {
        let updateTime_end = timezone.formatTime(Date.now(), "YYYY-MM-DD HH:mm:ss.SSS")
        logger.info(
          "[bettingDao][execute store procedure_UserRevenueHall][endTime] : %s , [executionTime] : %s",
          updateTime_end,
          timezone.isDiff(updateTime_end, updateTime_start) + "ms"
        )

        // 執行 game_revenue_hall
        bettingDao.updateGameRevenueHall(start, end, function (r_code, r_data) {
          cb(null, { code: code.OK }, null)
        })
      }
    })
  } catch (err) {
    logger.error("[bettingDao][updateUserRevenueHall] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// game_revenue_hall 結帳，直到第一層(最上層)時，則不再執行 - 無限階層
bettingDao.updateGameRevenueHall = function (start, end, cb) {
  try {
    let updateTime_start = timezone.formatTime(Date.now(), "YYYY-MM-DD HH:mm:ss.SSS")
    logger.info("[bettingDao][execute store procedure_GameRevenueHall][startTime] :", updateTime_start)
    let sql_query = []
    // call store procedure 執行
    let sql = sprintf("call wagers_1.sp_checkoutGameRevenueHall('%s', '%s')", start, end)
    sql_query.push(sql)

    logger.info("[bettingDao][execute store procedure_GameRevenueHall][sql] %s [args] %s", sql)

    db.act_transaction("dbclient_w_rw", sql_query, function (r_code, r_data) {
      if (r_code.code != code.OK) {
        logger.error("[bettingDao][execute store procedure_GameRevenueHall] r_code: ", JSON.stringify(r_code))
        cb(null, r_code, null)
      } else {
        let updateTime_end = timezone.formatTime(Date.now(), "YYYY-MM-DD HH:mm:ss.SSS")

        logger.info(
          "[bettingDao][execute store procedure_GameRevenueHall][endTime] : %s , [executionTime] : %s",
          updateTime_end,
          timezone.isDiff(updateTime_end, updateTime_start) + "ms"
        )

        // 更新 system 的 last_revenue_times
        bettingDao.updateContent(updateTime_end.substr(0, 19), function (r_code, r_data) {
          cb(null, r_code, null)
        })
      }
    })
  } catch (err) {
    logger.error("[bettingDao][execute store procedure_GameRevenueHall] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 刪除所有 revenue 資料 by table
bettingDao.delWagersRevenueData = function (tableList, cb) {
  try {
    if (typeof tableList == "undefined" || tableList.length == 0) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    let sql_text_list = []
    for (let table of tableList) {
      sql_text_list.push("TRUNCATE TABLE wagers_1." + table)
    }

    logger.info("[bettingDao][delWagersRevenueData] sql_text_list: ", sql_text_list)
    db.act_query_multi("dbclient_w_rw", sql_text_list, [], function (r_code, r_data) {
      if (r_code.code != code.OK) {
        logger.error("[bettingDao][delWagersRevenueData] act_transaction failed! err: ", r_code)
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[bettingDao][delWagersRevenueData] catch err", err)
    cb(null, code.FAIL, null)
  }
}

bettingDao.getPlatformWidList = function (data, cb) {
  try {
    const offest = data.currentPage === 1 ? 0 : data.listPerPage * (data.currentPage - 1)
    const limit = data.listPerPage

    const sql = `SELECT Wid, Cid, UpId, HallId, UserName, GameId, ExCurrency, CryDef, IsDemo, IsFreeGame, IsBonusGame, IsJP, JPType, 
    DATE_FORMAT(AddDate,'%Y-%m-%d %H:%i:%s') AS AddDate, GGId, JPGold, BetGold, RealBetGold, WinGold, roundID
    FROM wagers_bet WHERE platformWid = ? AND isValid = 1 ORDER BY AddDate DESC LIMIT ?,?`
    const args = [data.platformWid, offest, limit]

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      if (r_code.code != code.OK) {
        logger.error("[bettingDao][getPlatformWidList] sql err: ", r_code)
        return cb(null, r_code, null)
      }

      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][getPlatformWidList] catch err", inspect(err))
    cb(null, code.FAIL, null)
  }
}

bettingDao.getPlatformWidListCounts = function (data, cb) {
  try {
    const sql = "SELECT COUNT(*) AS counts FROM wagers_bet WHERE platformWid = ? AND isValid = 1"
    const args = [data.platformWid]

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      if (r_code.code != code.OK) {
        logger.error("[bettingDao][getPlatformWidListCounts] sql err: ", r_code)
        return cb(null, r_code, null)
      }

      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][getPlatformWidListCounts] catch err", err)
    cb(null, code.FAIL, null)
  }
}

bettingDao.getGameNames = function (gameId, cb) {
  try {
    const sql = "SELECT NameC, NameG, NameE, NameVN, NameTH, NameID, NameMY, NameJP, NameKR FROM games WHERE gameId = ? LIMIT 1"
    const args = [gameId]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code != code.OK) {
        logger.error("[bettingDao][getGameNames] sql err: ", r_code)
        return cb(null, r_code, null)
      }

      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][getGameNames] catch err", err)
    cb(null, code.FAIL, null)
  }
}

bettingDao.getGameGroupNames = function (ggId, cb) {
  try {
    const sql = "SELECT NameC, NameG, NameE, NameVN, NameTH, NameID, NameMY, NameJP, NameKR FROM game_group WHERE GGId = ? LIMIT 1"
    const args = [ggId]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code != code.OK) {
        logger.error("[bettingDao][getGameGroupNames] sql err: ", r_code)
        return cb(null, r_code, null)
      }

      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][getGameGroupNames] catch err", err)
    cb(null, code.FAIL, null)
  }
}

bettingDao.getWagersByRoundId = function (roundId, cb) {
  try {
    const sql = `SELECT Wid, Cid, UserName, Upid, HallId, GGId, GameId, BetGold, BetPoint,
        WinGold, WinPoint, RealBetPoint, RealBetGold, JPPoint, JPGold, JPConGold, JPConGoldOriginal, Currency, ExCurrency,
        CryDef, IsDemo, IsSingleWallet, IsFreeGame, IsBonusGame, IsJP, JPType, DATE_FORMAT(AddDate,'%Y-%m-%d %H:%i:%s') as AddDate, Repair, roundID,
        CreateTime, JPPoolId, Denom, JPConPoint, PlatformWid 
        FROM wagers_bet WHERE roundID = ? AND IsValid = 1`

    const args = [roundId]

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      if (r_code.code != code.OK) {
        logger.error("[bettingDao][getWagersByRoundId] sql err: ", inspect(r_code))
        return cb(null, r_code, null)
      }

      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][getWagersByRoundId] catch err", inspect(err))
    cb(null, code.FAIL, null)
  }
}

bettingDao.getWagersByCycleId = function (cycleId, cb) {
  try {
    const sql = `SELECT Wid, Cid, UserName, Upid, HallId, GGId, GameId, BetGold, BetPoint,
        WinGold, WinPoint, RealBetPoint, RealBetGold, JPPoint, JPGold, JPConGold, JPConGoldOriginal, Currency, ExCurrency,
        CryDef, IsDemo, IsSingleWallet, IsFreeGame, IsBonusGame, IsJP, JPType, DATE_FORMAT(AddDate,'%Y-%m-%d %H:%i:%s') as AddDate, Repair, roundID,
        CreateTime, JPPoolId, Denom, JPConPoint, PlatformWid 
        FROM wagers_bet WHERE cycleId = ? AND IsValid = 1`

    const args = [cycleId]

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      if (r_code.code != code.OK) {
        logger.error("[bettingDao][getWagersByCycleId] sql err: ", inspect(r_code))
        return cb(null, r_code, null)
      }

      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][getWagersByCycleId] catch err", inspect(err))
    cb(null, code.FAIL, null)
  }
}

/**
 *
 * 指定區間重新結帳
 *
 * @param {string} startTime
 * @param {string} endTime
 * @param {*} cb
 */
bettingDao.accountingWagers = function (startTime, endTime, cb) {
  try {
    const deleteSql = [
      `DELETE FROM wagers_1.user_revenue_player WHERE AccountDate >= ? AND AccountDate < ?`,
      `DELETE FROM wagers_1.user_revenue_agent WHERE AccountDate >= ? AND AccountDate < ?`,
      `DELETE FROM wagers_1.user_revenue_hall WHERE AccountDate >= ? AND AccountDate < ?`,
      `DELETE FROM wagers_1.game_revenue_player WHERE AccountDate >= ? AND AccountDate < ?`,
      `DELETE FROM wagers_1.game_revenue_agent WHERE AccountDate >= ? AND AccountDate < ?`,
      `DELETE FROM wagers_1.game_revenue_hall WHERE AccountDate >= ? AND AccountDate < ?`,
    ]

    const deleteArgs = Array(6).fill([startTime, endTime])

    const insertUserRevenueSql = [
      `INSERT INTO
        wagers_1.user_revenue_player(
          Id,
          AccountDate,
          Rounds,
          BaseRounds,
          Cid,
          UserName,
          UpId,
          HallId,
          CryDef,
          Currency,
          ExCurrency,
          IsDemo,
          RealBetGold,
          BetGold,
          BetPoint,
          WinGold,
          JPPoint,
          JPGold,
          JPConGoldOriginal,
          JPRealBetGold
        )
      SELECT
        Id,
        AccountDate,
        Rounds,
        BaseRounds,
        Cid,
        UserName,
        UpId,
        HallId,
        CryDef,
        Currency,
        ExCurrency,
        IsDemo,
        RealBetGold,
        BetGold,
        BetPoint,
        WinGold,
        JPPoint,
        JPGold,
        JPConGoldOriginal,
        JPRealBetGold
      FROM
        (
          SELECT
            MD5(
              CONCAT_WS(
                '-',
                Cid,
                MAX(left(AddDate, 15)),
                Currency,
                ExCurrency,
                CryDef
              )
            ) AS Id,
            MAX(CONCAT(left(AddDate, 15), "0:00")) AS AccountDate,
            COUNT(Cid) AS Rounds,
            SUM(IF(IsFreeGame = 0, 1, 0)) AS BaseRounds,
            MAX(Cid) AS Cid,
            MAX(UserName) AS UserName,
            MAX(UpId) AS UpId,
            MAX(HallId) AS HallId,
            MAX(CryDef) AS CryDef,
            MAX(Currency) AS Currency,
            MAX(ExCurrency) AS ExCurrency,
            MAX(IsDemo) AS IsDemo,
            SUM(TRUNCATE(RealBetGold / 1000, 2) * 1000) AS RealBetGold,
            SUM(TRUNCATE(BetGold / 1000, 2) * 1000) AS BetGold,
            SUM(TRUNCATE(BetPoint / 1000, 2) * 1000) AS BetPoint,
            SUM(TRUNCATE(WinGold / 1000, 2) * 1000) AS WinGold,
            SUM(TRUNCATE(JPPoint / 1000, 2) * 1000) AS JPPoint,
            SUM(TRUNCATE(JPGold / 1000, 2) * 1000) AS JPGold,
            SUM(TRUNCATE(JPConGoldOriginal / 1000, 4) * 1000) AS JPConGoldOriginal,
            SUM(
              IF(
                JPConGoldOriginal > 0,
                TRUNCATE(RealBetGold / 1000, 2) * 1000,
                0
              )
            ) AS JPRealBetGold
          FROM
            wagers_1.wagers_bet AS w
          WHERE
            AddDate >= ?
            AND AddDate < ?
            AND IsValid = 1
            AND IsDemo = 0
          GROUP BY
            w.Cid,
            left(w.AddDate, 15),
            w.Currency,
            w.ExCurrency,
            w.CryDef,
            w.IsDemo
        ) tempTable ON DUPLICATE KEY
      UPDATE
        Rounds = tempTable.Rounds,
        BaseRounds = tempTable.BaseRounds,
        RealBetGold = tempTable.RealBetGold,
        BetGold = tempTable.BetGold,
        BetPoint = tempTable.BetPoint,
        WinGold = tempTable.WinGold,
        JPPoint = tempTable.JPPoint,
        JPGold = tempTable.JPGold,
        JPConGoldOriginal = tempTable.JPConGoldOriginal,
        UpId = tempTable.UpId,
        JPRealBetGold = tempTable.JPRealBetGold;
      `,
      `INSERT INTO
        wagers_1.user_revenue_agent(
          Id,
          AccountDate,
          Rounds,
          BaseRounds,
          Cid,
          HallId,
          CryDef,
          Currency,
          ExCurrency,
          IsDemo,
          RealBetGold,
          BetGold,
          BetPoint,
          WinGold,
          JPPoint,
          JPGold,
          JPConGoldOriginal,
          JPRealBetGold
        )
      SELECT
        Id,
        AccountDate,
        Rounds,
        BaseRounds,
        Cid,
        HallId,
        CryDef,
        Currency,
        ExCurrency,
        IsDemo,
        RealBetGold,
        BetGold,
        BetPoint,
        WinGold,
        JPPoint,
        JPGold,
        JPConGoldOriginal,
        JPRealBetGold
      FROM
        (
          SELECT
            MD5(
              CONCAT_WS(
                '-',
                p.UpId,
                p.AccountDate,
                p.Currency,
                p.ExCurrency,
                p.CryDef
              )
            ) AS Id,
            p.AccountDate,
            SUM(p.Rounds) AS Rounds,
            SUM(p.BaseRounds) AS BaseRounds,
            p.UpId AS Cid,
            MAX(p.HallId) AS HallId,
            p.CryDef,
            p.Currency,
            p.ExCurrency,
            MAX(p.IsDemo) AS IsDemo,
            SUM(TRUNCATE(RealBetGold / 1000, 2) * 1000) AS RealBetGold,
            SUM(TRUNCATE(p.BetGold / 1000, 2) * 1000) AS BetGold,
            SUM(TRUNCATE(p.BetPoint / 1000, 2) * 1000) AS BetPoint,
            SUM(TRUNCATE(p.WinGold / 1000, 2) * 1000) AS WinGold,
            SUM(TRUNCATE(p.JPPoint / 1000, 2) * 1000) AS JPPoint,
            SUM(TRUNCATE(p.JPGold / 1000, 2) * 1000) AS JPGold,
            SUM(TRUNCATE(JPConGoldOriginal / 1000, 4) * 1000) AS JPConGoldOriginal,
            SUM(JPRealBetGold) AS JPRealBetGold
          FROM
            wagers_1.user_revenue_player p
            LEFT JOIN game.customer c ON c.Cid = p.UpId
          WHERE
            p.AccountDate >= ?
            AND p.AccountDate < ?
            AND c.IsDemo = 0
          GROUP BY
            p.UpId,
            p.AccountDate,
            p.Currency,
            p.ExCurrency,
            p.CryDef,
            c.IsDemo
        ) tempTable ON DUPLICATE KEY
      UPDATE
        Rounds = tempTable.Rounds,
        BaseRounds = tempTable.BaseRounds,
        RealBetGold = tempTable.RealBetGold,
        BetGold = tempTable.BetGold,
        BetPoint = tempTable.BetPoint,
        WinGold = tempTable.WinGold,
        JPPoint = tempTable.JPPoint,
        JPGold = tempTable.JPGold,
        JPConGoldOriginal = tempTable.JPConGoldOriginal,
        JPRealBetGold = tempTable.JPRealBetGold;
      `,
      `INSERT INTO
        wagers_1.user_revenue_hall(
          Id,
          AccountDate,
          Rounds,
          BaseRounds,
          Upid,
          Cid,
          CryDef,
          Currency,
          ExCurrency,
          RealBetGold,
          BetGold,
          BetPoint,
          WinGold,
          JPPoint,
          JPGold,
          JPConGoldOriginal,
          JPRealBetGold
        )
      SELECT
        Id,
        AccountDate,
        Rounds,
        BaseRounds,
        Upid,
        Cid,
        CryDef,
        Currency,
        ExCurrency,
        RealBetGold,
        BetGold,
        BetPoint,
        WinGold,
        JPPoint,
        JPGold,
        JPConGoldOriginal,
        JPRealBetGold
      FROM
        (
          SELECT
            MD5(
              CONCAT_WS(
                '-',
                a.HallId,
                a.AccountDate,
                a.Currency,
                a.ExCurrency,
                a.CryDef
              )
            ) AS Id,
            AccountDate,
            SUM(a.Rounds) AS Rounds,
            SUM(a.BaseRounds) AS BaseRounds,
            Upid,
            a.HallId AS Cid,
            a.CryDef,
            a.Currency,
            a.ExCurrency,
            SUM(TRUNCATE(RealBetGold / 1000, 2) * 1000) AS RealBetGold,
            SUM(TRUNCATE(a.BetGold / 1000, 2) * 1000) AS BetGold,
            SUM(TRUNCATE(a.BetPoint / 1000, 2) * 1000) AS BetPoint,
            SUM(TRUNCATE(a.WinGold / 1000, 2) * 1000) AS WinGold,
            SUM(TRUNCATE(a.JPPoint / 1000, 2) * 1000) AS JPPoint,
            SUM(TRUNCATE(a.JPGold / 1000, 2) * 1000) AS JPGold,
            SUM(TRUNCATE(JPConGoldOriginal / 1000, 4) * 1000) AS JPConGoldOriginal,
            SUM(a.JPRealBetGold) AS JPRealBetGold
          FROM
            wagers_1.user_revenue_agent a
            LEFT JOIN game.customer c ON c.Cid = a.HallId
          WHERE
            a.AccountDate >= ?
            AND a.AccountDate < ?
          GROUP BY
            a.HallId,
            a.AccountDate,
            a.Currency,
            a.ExCurrency,
            a.CryDef,
            a.IsDemo
        ) tempTable ON DUPLICATE KEY
      UPDATE
        Rounds = tempTable.Rounds,
        BaseRounds = tempTable.BaseRounds,
        RealBetGold = tempTable.RealBetGold,
        BetGold = tempTable.BetGold,
        BetPoint = tempTable.BetPoint,
        WinGold = tempTable.WinGold,
        JPPoint = tempTable.JPPoint,
        JPGold = tempTable.JPGold,
        JPConGoldOriginal = tempTable.JPConGoldOriginal,
        JPRealBetGold = tempTable.JPRealBetGold; 
      `,
    ]

    const insertUserRevenueArgs = Array(3).fill([startTime, endTime])

    const insertGameRevenueSql = [
      `INSERT INTO
        wagers_1.game_revenue_player(
          Id,
          AccountDate,
          Rounds,
          BaseRounds,
          GGId,
          GameId,
          CryDef,
          Currency,
          ExCurrency,
          Cid,
          UserName,
          UpId,
          HallId,
          IsDemo,
          RealBetGold,
          BetGold,
          BetPoint,
          WinGold,
          JPPoint,
          JPGold,
          JPConGoldOriginal,
          JPRealBetGold
        )
      SELECT
        Id,
        AccountDate,
        Rounds,
        BaseRounds,
        GGId,
        GameId,
        CryDef,
        Currency,
        ExCurrency,
        Cid,
        UserName,
        UpId,
        HallId,
        IsDemo,
        RealBetGold,
        BetGold,
        BetPoint,
        WinGold,
        JPPoint,
        JPGold,
        JPConGoldOriginal,
        JPRealBetGold
      FROM
        (
          SELECT
            MD5(
              CONCAT_WS(
                '-',
                GameId,
                Cid,
                MAX(LEFT(AddDate, 15)),
                Currency,
                ExCurrency,
                CryDef
              )
            ) AS Id,
            MAX(CONCAT(LEFT(AddDate, 15), "0:00")) AS AccountDate,
            count(Cid) AS Rounds,
            SUM(IF(IsFreeGame = 0, 1, 0)) AS BaseRounds,
            MAX(GGId) AS GGId,
            GameId,
            CryDef,
            Currency,
            ExCurrency,
            Cid,
            MAX(UserName) AS UserName,
            MAX(UpId) AS UpId,
            MAX(HallId) AS HallId,
            MAX(IsDemo) AS IsDemo,
            SUM(TRUNCATE(RealBetGold / 1000, 2) * 1000) AS RealBetGold,
            SUM(TRUNCATE(BetGold / 1000, 2) * 1000) AS BetGold,
            SUM(TRUNCATE(BetPoint / 1000, 2) * 1000) AS BetPoint,
            SUM(TRUNCATE(WinGold / 1000, 2) * 1000) AS WinGold,
            SUM(TRUNCATE(JPPoint / 1000, 2) * 1000) AS JPPoint,
            SUM(TRUNCATE(JPGold / 1000, 2) * 1000) AS JPGold,
            SUM(TRUNCATE(JPConGoldOriginal / 1000, 4) * 1000) AS JPConGoldOriginal,
            SUM(
              IF(
                JPConGoldOriginal > 0,
                TRUNCATE(RealBetGold / 1000, 2) * 1000,
                0
              )
            ) AS JPRealBetGold
          FROM
            wagers_1.wagers_bet
          WHERE
            AddDate >= ?
            AND AddDate < ?
            AND IsValid = 1
            AND IsDemo = 0
          GROUP BY
            GameId,
            Cid,
            LEFT(AddDate, 15),
            Currency,
            ExCurrency,
            CryDef
        ) tempTable ON DUPLICATE KEY
      UPDATE
        Rounds = tempTable.Rounds,
        BaseRounds = tempTable.BaseRounds,
        RealBetGold = tempTable.RealBetGold,
        BetGold = tempTable.BetGold,
        BetPoint = tempTable.BetPoint,
        WinGold = tempTable.WinGold,
        JPPoint = tempTable.JPPoint,
        JPGold = tempTable.JPGold,
        JPConGoldOriginal = tempTable.JPConGoldOriginal,
        GameId = tempTable.GameId,
        JPRealBetGold = tempTable.JPRealBetGold;
      `,
      `INSERT INTO
        wagers_1.game_revenue_agent(
          Id,
          AccountDate,
          Rounds,
          BaseRounds,
          GGId,
          GameId,
          CryDef,
          Currency,
          ExCurrency,
          Cid,
          HallId,
          IsDemo,
          RealBetGold,
          BetGold,
          BetPoint,
          WinGold,
          JPPoint,
          JPGold,
          JPConGoldOriginal,
          JPRealBetGold
        )
      SELECT
        Id,
        AccountDate,
        Rounds,
        BaseRounds,
        GGId,
        GameId,
        CryDef,
        Currency,
        ExCurrency,
        Cid,
        HallId,
        IsDemo,
        RealBetGold,
        BetGold,
        BetPoint,
        WinGold,
        JPPoint,
        JPGold,
        JPConGoldOriginal,
        JPRealBetGold
      FROM
        (
          SELECT
            MD5(
              CONCAT_WS(
                '-',
                p.GameId,
                p.UpId,
                p.AccountDate,
                p.Currency,
                p.ExCurrency,
                p.CryDef
              )
            ) AS Id,
            p.AccountDate,
            SUM(p.Rounds) AS Rounds,
            SUM(p.BaseRounds) AS BaseRounds,
            MAX(p.GGId) AS GGId,
            p.GameId,
            p.CryDef,
            p.Currency,
            p.ExCurrency,
            MAX(p.UpId) AS Cid,
            MAX(p.HallId) AS HallId,
            MAX(p.IsDemo) AS IsDemo,
            SUM(TRUNCATE(RealBetGold / 1000, 2) * 1000) AS RealBetGold,
            SUM(TRUNCATE(p.BetGold / 1000, 2) * 1000) AS BetGold,
            SUM(TRUNCATE(p.BetPoint / 1000, 2) * 1000) AS BetPoint,
            SUM(TRUNCATE(p.WinGold / 1000, 2) * 1000) AS WinGold,
            SUM(TRUNCATE(p.JPPoint / 1000, 2) * 1000) AS JPPoint,
            SUM(TRUNCATE(p.JPGold / 1000, 2) * 1000) AS JPGold,
            SUM(TRUNCATE(JPConGoldOriginal / 1000, 4) * 1000) AS JPConGoldOriginal,
            SUM(JPRealBetGold) AS JPRealBetGold
          FROM
            wagers_1.game_revenue_player p
            LEFT JOIN game.customer c ON c.Cid = p.UpId
          WHERE
            p.AccountDate >= ?
            AND p.AccountDate < ?
            AND c.IsDemo = 0
          GROUP BY
            p.GameId,
            p.UpId,
            p.AccountDate,
            p.Currency,
            p.ExCurrency,
            p.CryDef,
            c.IsDemo
        ) tempTable ON DUPLICATE KEY
      UPDATE
        Rounds = tempTable.Rounds,
        BaseRounds = tempTable.BaseRounds,
        RealBetGold = tempTable.RealBetGold,
        BetGold = tempTable.BetGold,
        BetPoint = tempTable.BetPoint,
        WinGold = tempTable.WinGold,
        JPPoint = tempTable.JPPoint,
        JPGold = tempTable.JPGold,
        JPConGoldOriginal = tempTable.JPConGoldOriginal,
        GameId = tempTable.GameId,
        JPRealBetGold = tempTable.JPRealBetGold;
      `,
      `INSERT INTO
        wagers_1.game_revenue_hall(
          Id,
          AccountDate,
          Rounds,
          BaseRounds,
          GGId,
          GameId,
          CryDef,
          Currency,
          ExCurrency,
          Upid,
          Cid,
          RealBetGold,
          BetGold,
          BetPoint,
          WinGold,
          JPPoint,
          JPGold,
          JPConGoldOriginal,
          JPRealBetGold
        )
      SELECT
        Id,
        AccountDate,
        Rounds,
        BaseRounds,
        GGId,
        GameId,
        CryDef,
        Currency,
        ExCurrency,
        Upid,
        Cid,
        RealBetGold,
        BetGold,
        BetPoint,
        WinGold,
        JPPoint,
        JPGold,
        JPConGoldOriginal,
        JPRealBetGold
      FROM
        (
          SELECT
            MD5(
              CONCAT_WS(
                '-',
                a.GameId,
                MAX(a.Cid),
                a.HallId,
                a.AccountDate,
                a.Currency,
                a.ExCurrency,
                a.CryDef
              )
            ) AS Id,
            AccountDate,
            SUM(a.Rounds) AS Rounds,
            SUM(a.BaseRounds) AS BaseRounds,
            MAX(a.GGId) AS GGId,
            a.GameId,
            a.CryDef,
            a.Currency,
            a.ExCurrency,
            c.Upid AS Upid,
            MAX(a.HallId) AS Cid,
            SUM(TRUNCATE(RealBetGold / 1000, 2) * 1000) AS RealBetGold,
            SUM(TRUNCATE(a.BetGold / 1000, 2) * 1000) AS BetGold,
            SUM(TRUNCATE(a.BetPoint / 1000, 2) * 1000) AS BetPoint,
            SUM(TRUNCATE(a.WinGold / 1000, 2) * 1000) AS WinGold,
            SUM(TRUNCATE(a.JPPoint / 1000, 2) * 1000) AS JPPoint,
            SUM(TRUNCATE(a.JPGold / 1000, 2) * 1000) AS JPGold,
            SUM(TRUNCATE(JPConGoldOriginal / 1000, 4) * 1000) AS JPConGoldOriginal,
            SUM(JPRealBetGold) AS JPRealBetGold
          FROM
            wagers_1.game_revenue_agent a
            LEFT JOIN game.customer c ON c.Cid = a.HallId
          WHERE
            a.AccountDate >= @start_time
            AND a.AccountDate < @end_time
          GROUP BY
            a.GameId,
            a.HallId,
            a.AccountDate,
            a.Currency,
            a.ExCurrency,
            a.CryDef
        ) tempTable ON DUPLICATE KEY
      UPDATE
        Rounds = tempTable.Rounds,
        BaseRounds = tempTable.BaseRounds,
        RealBetGold = tempTable.RealBetGold,
        BetGold = tempTable.BetGold,
        BetPoint = tempTable.BetPoint,
        WinGold = tempTable.WinGold,
        JPPoint = tempTable.JPPoint,
        JPGold = tempTable.JPGold,
        JPConGoldOriginal = tempTable.JPConGoldOriginal,
        GameId = tempTable.GameId,
        JPRealBetGold = tempTable.JPRealBetGold;
      `,
    ]

    const insertGameRevenueArgs = Array(3).fill([startTime, endTime])

    const callSP = [`call sp_checkoutGameRevenueHall(?,?)`, `call sp_checkoutUserRevenueHall(?,?)`]

    const callSPArgs = Array(2).fill([startTime, endTime])

    db.act_query_multi(
      "dbclient_w_rw",
      [...deleteSql, ...insertUserRevenueSql, ...insertGameRevenueSql, ...callSP],
      [...deleteArgs, ...insertUserRevenueArgs, ...insertGameRevenueArgs, ...callSPArgs],
      function (r_code, r_data) {
        if (r_code.code != code.OK) {
          logger.error("[bettingDao][accountingWagers] sql err: ", inspect(r_code))
          return cb(null, r_code, null)
        }

        cb(null, r_code, r_data)
      }
    )
  } catch (err) {
    logger.error("[bettingDao][accountingWagers] catch err", inspect(err))
    cb(null, code.FAIL, null)
  }
}

bettingDao.countAllActivePlayers = function (data, cb) {
  try {
    const { startDate, endDate } = data

    const sql = `SELECT COUNT(DISTINCT Cid) as counts FROM game_revenue_player WHERE AccountDate >= ? AND AccountDate <= ?`

    const args = [startDate, endDate]

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][countAllActivePlayers] catch err", inspect(err))
    cb(null, code.FAIL, null)
  }
}

bettingDao.countAllActivePlayersPerHour = function (data, cb) {
  try {
    const { startDate, endDate, hourDiffFormated, agentIdList = [] } = data

    const additionalSql = agentIdList.length > 0 ? " AND UpId IN ? " : ""

    const sql = `SELECT COUNT(DISTINCT Cid) as counts, DATE_FORMAT(convert_tz(AccountDate, "+00:00", ? ), "%H" ) AS localHour 
       FROM game_revenue_player WHERE AccountDate >= ? AND AccountDate <= ? ${additionalSql} GROUP BY localHour`

    const args = [hourDiffFormated, startDate, endDate, [agentIdList]]

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][countAllActivePlayersPerHour] catch err", inspect(err))
    cb(null, code.FAIL, null)
  }
}

bettingDao.countAllActivePlayerByWagers = function (data, cb) {
  try {
    const { startDate, endDate } = data

    const sql = `SELECT COUNT(DISTINCT Cid) as counts FROM wagers_bet WHERE AddDate >= ? AND AddDate <= ? AND IsDemo = 0`

    const args = [startDate, endDate]

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][countAllActivePlayerByWagers] catch err", inspect(err))
    cb(null, code.FAIL, null)
  }
}

bettingDao.countAgentActivePlayerByWagers = function (data, cb) {
  try {
    const { startDate, endDate, agentIdList } = data

    const sql = `SELECT COUNT(DISTINCT Cid) as counts FROM wagers_bet WHERE AddDate >= ? AND AddDate <= ? AND UpId IN ? AND IsDemo = 0`

    const args = [startDate, endDate, [agentIdList]]

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[bettingDao][countAgentActivePlayerByWagers] catch err", inspect(err))
    cb(null, code.FAIL, null)
  }
}

bettingDao.getFunkyReportPlatformIdList = function (data, cb) {
  const logTag = "[bettingDao][getFunkyReportPlatformIdList]"

  try {
    const sql = `SELECT DISTINCT fpid as platformId
    FROM funky_revenue 
    WHERE AccountDate >= (CURDATE() - INTERVAL 195 DAY)
    `

    const args = []

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error(`${logTag} ${inspect(err)}`)
    cb(null, code.FAIL, null)
  }
}

bettingDao.getFunkyTransactionDateDataCount = function (data, cb) {
  const logTag = "[bettingDao][getFunkyTransactionDateDataCount]"
  const { startDate, endDate, platformId, gameId, currency, playerAccount, accountType } = data

  try {
    const extraSql = []
    const extraArgs = []

    if (platformId) {
      extraSql.push(" AND fpid = ?")
      extraArgs.push(platformId)
    }

    if (gameId) {
      extraSql.push(" AND gameId = ?")
      extraArgs.push(gameId)
    }

    if (currency) {
      extraSql.push(" AND currency = ?")
      extraArgs.push(currency)
    }

    if (playerAccount) {
      extraSql.push(" AND username = ?")
      extraArgs.push(playerAccount)
    }

    if (accountType === 1 || accountType === 0) {
      extraSql.push(" AND IsTestAccount = ?")
      extraArgs.push(accountType)
    }

    const sql = `SELECT count(*) as rows FROM (SELECT count(*) FROM funky_revenue 
    WHERE StatementDate >= ? AND StatementDate <= ? 
    ${extraSql.join(" ")}
    GROUP BY statementDate,cId,gameId,currency) as t;
    `

    const args = [startDate, endDate, ...extraArgs]

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error(`${logTag} ${inspect(err)}`)
    cb(null, code.FAIL, null)
  }
}

bettingDao.getFunkyTransactionDateData = function (data, cb) {
  const logTag = "[bettingDao][getFunkyTransactionDateData]"
  const {
    startDate,
    endDate,
    currentPage,
    listPerPage,
    platformId,
    gameId,
    currency,
    playerAccount,
    sortKey,
    sortOrder,
    accountType,
  } = data

  const rowIndex = (currentPage - 1) * listPerPage
  const order = sortOrder === "DESC" ? "DESC" : "ASC"
  const orderBy = sortKey ? sortKey : "Id"

  const extraSql = []
  const extraArgs = []

  if (platformId) {
    extraSql.push(" AND Fpid = ? ")
    extraArgs.push(platformId)
  }

  if (gameId) {
    extraSql.push(" AND gameId = ?")
    extraArgs.push(gameId)
  }

  if (currency) {
    extraSql.push(" AND currency = ?")
    extraArgs.push(currency)
  }

  if (playerAccount) {
    extraSql.push(" AND username = ?")
    extraArgs.push(playerAccount)
  }

  if (accountType === 1 || accountType === 0) {
    extraSql.push(" AND IsTestAccount = ?")
    extraArgs.push(accountType)
  }

  try {
    const sql = `SELECT StatementDate as statementDate, Fpid as fpId, GameId as gameId, Currency as currency, 
      Cid as cId, Username as username, Upid as upId, IsTestAccount as isTestAccount, 
      SUM(WagerCount) as wagerCount,
      SUM(TRUNCATE(BetGold/1000,2)) as betGold, 
      SUM(TRUNCATE(RealBetGold/1000,2)) as realBetGold,
      SUM(TRUNCATE(WinGold/1000,2)) as winGold,
      SUM(TRUNCATE(JPGold/1000,2)) as jpGold,
      SUM( TRUNCATE(WinGold/1000,2) - TRUNCATE(JPGold/1000,2) ) as payout,
      SUM( TRUNCATE(BetGold/1000,2) - (TRUNCATE(WinGold/1000,2) - TRUNCATE(JPGold/1000,2)) ) as netWin,
      SUM( TRUNCATE(WinGold/1000,2) / TRUNCATE(RealBetGold/1000,2) ) as rtp
    FROM funky_revenue 
    where StatementDate >= ? and StatementDate <= ? 
    ${extraSql.join(" ")}
    GROUP BY statementDate,cId,gameId,currency
    ORDER BY ?? ${order} , id
    LIMIT ?,?;
    `

    const args = [startDate, endDate, ...extraArgs, orderBy, rowIndex, listPerPage]

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error(`${logTag} ${inspect(err)}`)
    cb(null, code.FAIL, null)
  }
}

/**
 *
 *  Funky報表的合計欄位
 *
 *  需轉換為CNY
 *
 * @param {*} data
 * @param {*} cb
 */
bettingDao.getFunkyTransactionDateSumData = function (data, cb) {
  const logTag = "[bettingDao][getFunkyTransactionDateSumData]"
  const { startDate, endDate, platformId, gameId, currency, playerAccount, accountType } = data

  const extraSql = []
  const extraArgs = []

  if (platformId) {
    extraSql.push(" AND Fpid = ? ")
    extraArgs.push(platformId)
  }

  if (gameId) {
    extraSql.push(" AND gameId = ?")
    extraArgs.push(gameId)
  }

  if (currency) {
    extraSql.push(" AND currency = ?")
    extraArgs.push(currency)
  }

  if (playerAccount) {
    extraSql.push(" AND username = ?")
    extraArgs.push(playerAccount)
  }

  if (accountType === 1 || accountType === 0) {
    extraSql.push(" AND IsTestAccount = ?")
    extraArgs.push(accountType)
  }

  try {
    const sql = `SELECT
        SUM(wagerCount) AS wagerCount,
        SUM(TRUNCATE(BetGold * cRate, 2)) AS cnyBetGold,
        SUM(TRUNCATE(RealBetGold * cRate, 2)) AS cnyRealBetGold,
        SUM(TRUNCATE(WinGold * cRate, 2)) AS cnyWinGold,
        SUM(TRUNCATE(JPGold * cRate, 2)) AS cnyJPGold,
        SUM(TRUNCATE((WinGold - JPGold) * cRate, 2)) AS cnyPayout,
        SUM(TRUNCATE((BetGold - (WinGold - JPGold)) * cRate,2)) AS cnyNetWin,

        SUM(BetGold) AS betGold,
        SUM(RealBetGold) AS realBetGold,
        SUM(WinGold) AS winGold,
        SUM(JPGold) AS jpGold,
        SUM(WinGold - JPGold) AS payout,
        SUM(BetGold - (WinGold - JPGold)) AS netWin

      FROM (
        SELECT
          f.currency,
          (
            SELECT TRUNCATE(1 / crydef, 6)
            FROM game.currency_exchange_rate
            WHERE ExCurrency = f.currency AND EnableTime <= f.StatementDate
            ORDER BY EnableTime DESC
            LIMIT 1
          ) AS cRate,
          SUM(WagerCount) as wagerCount,
          SUM(TRUNCATE(BetGold/1000,2)) as betGold, 
          SUM(TRUNCATE(RealBetGold/1000,2)) as realBetGold,
          SUM(TRUNCATE(WinGold/1000,2)) as winGold,
          SUM(TRUNCATE(JPGold/1000,2)) as jpGold,
          SUM( TRUNCATE(WinGold/1000,2) - TRUNCATE(JPGold/1000,2) ) as payout,
          SUM( TRUNCATE(BetGold/1000,2) - (TRUNCATE(WinGold/1000,2) - TRUNCATE(JPGold/1000,2)) ) as netWin
        FROM funky_revenue AS f
        WHERE StatementDate >= ? and StatementDate <= ? 
        ${extraSql.join(" ")}
        GROUP BY f.currency,cRate
      ) t
    `
    const args = [startDate, endDate, ...extraArgs]

    db.act_query("dbclient_w_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error(`${logTag} ${inspect(err)}`)
    cb(null, code.FAIL, null)
  }
}
