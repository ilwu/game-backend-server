var logger = require("pomelo-logger").getLogger("betHandler", __filename)
var m_async = require("async")
var code = require("../../../util/code")
var conf = require("../../../../config/js/conf")
var bettingDao = require("../../../DataBase/bettingDao")
var timezone = require("../../../util/timezone")
var configDao = require("../../../DataBase/configDao")
var userDao = require("../../../DataBase/userDao")
var gameDao = require("../../../DataBase/gameDao")
var logDao = require("../../../DataBase/logDao")
const utils = require("../../../util/utils")
var consts = require("../../../share/consts")
var pomelo = require("pomelo")

const moment = require("moment-timezone")

const { isEmpty } = utils
const { inspect } = require("util")
const requestService = require("../../../services/requestService")
const UserLevelService = require("../../../services/UserLevelService")
const { BN } = require("../../../util/number")

module.exports = function (app) {
  return new Handler(app)
}

var Handler = function (app) {
  this.app = app
}

var handler = Handler.prototype

handler.getDetailFish_BetHistory = function (msg, session, next) {
  try {
    if (typeof msg.data === "undefined" || typeof msg.data.wid === "undefined") {
      next(null, { code: code.BET.PARA_DATA_FAIL, data: [] })
      return
    }

    m_async.waterfall(
      [
        function (cb) {
          bettingDao.getDetailFish_BetHistory(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          if (r_code.code == code.DB.NO_DATA) {
            next(null, { code: code.BET.HISTORY_WAGER_NOT_EXIST, data: [] })
          } else {
            next(null, { code: code.BET.HISTORY_LOAD_FAIL, data: [] })
          }
          return
        } else {
          next(null, { code: code.OK, data: r_data })
          return
        }
      }
    )
  } catch (err) {
    logger.error("[betHandler][getDetailFish_BetHistory] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getDetailSlot_BetHistory = function (msg, session, next) {
  try {
    if (typeof msg.data === "undefined" || typeof msg.data.wid === "undefined") {
      next(null, { code: code.BET.PARA_DATA_FAIL, data: [] })
      return
    }

    m_async.waterfall(
      [
        function (cb) {
          const { wid, isValidOnly = false } = msg.data

          const data = { wid, isValidOnly }

          bettingDao.getDetailSlot_BetHistory(data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          if (r_code.code == code.DB.NO_DATA) {
            next(null, { code: code.BET.HISTORY_WAGER_NOT_EXIST, data: [] })
          } else {
            next(null, { code: code.BET.HISTORY_LOAD_FAIL, data: [] })
          }
          return
        } else {
          next(null, { code: code.OK, data: r_data })
          return
        }
      }
    )
  } catch (err) {
    logger.error("[betHandler][getDetailSlot_BetHistory] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getDetailArcade_BetHistory = function (msg, session, next) {
  try {
    if (typeof msg.data === "undefined" || typeof msg.data.wid === "undefined") {
      next(null, { code: code.BET.PARA_DATA_FAIL, data: [] })
      return
    }

    m_async.waterfall(
      [
        function (cb) {
          bettingDao.getDetailArcade_BetHistory(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          if (r_code.code == code.DB.NO_DATA) {
            next(null, { code: code.BET.HISTORY_WAGER_NOT_EXIST, data: [] })
          } else {
            next(null, { code: code.BET.HISTORY_LOAD_FAIL, data: [] })
          }
          return
        } else {
          next(null, { code: code.OK, data: r_data })
          return
        }
      }
    )
  } catch (err) {
    logger.error("[betHandler][getDetailArcade_BetHistory] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//分銷商/代理報表 getUserRevenue
handler.getUserRevenue = function (msg, session, next) {
  try {
    var self = this

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.start_date === "undefined" ||
      typeof msg.data.end_date === "undefined"
    ) {
      next(null, { code: code.BET.PARA_DATA_FAIL })
      return
    }

    if (typeof msg.data.isPage === "undefined") {
      msg.data["isPage"] = true //預設-有分頁
    }

    if (msg.data.isPage == true) {
      //有分頁
      if (
        typeof msg.data.page === "undefined" ||
        typeof msg.data.pageCount === "undefined" ||
        typeof msg.data.page != "number" ||
        typeof msg.data.pageCount != "number"
      ) {
        next(null, { code: code.BET.PARA_DATA_FAIL })
        return
      }
    }

    //無分頁
    if (msg.data["isPage"] == false) {
      if (typeof msg.data.finish === "undefined") {
        msg.data["finish"] = 0
      } //0:未完成
      if (typeof msg.data.index === "undefined") {
        msg.data["index"] = 0
      } //開始第N筆
      msg.data["pageCount"] = 1000 //每次取得筆數
    }

    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "id"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    var data = msg.data

    var ttlCount_Revenue = 0
    var logs = []
    var userId = 0
    // var userCid = [];
    // var check_user = false;
    var sys_config = self.app.get("sys_config")
    let isAdmin = session.get("level") == 1
    let isHa = session.get("level") == 2
    let isAg = session.get("level") == 3

    let loginId = session.get("cid")
    let loginHallId = session.get("hallId")
    let agentId = session.get("agentId")
    const level = session.get("level")
    m_async.waterfall(
      [
        function (cb) {
          switch (level) {
            case 1: //admin - 全部
              //self.app.rpc.config.configRemote.getCurrenies(userSession, cb); //幣別
              configDao.getCurrencyList(cb) //幣別
              break
            case 2: //hall - 下線(hall 資料表的currencies)
              userId = session.get("cid")
              if (session.get("isSub") == 0) {
                userDao.getHallCurrency_byCid(userId, cb)
              } else {
                //Hall Manager
                const data = {
                  cid: userId,
                }
                userDao.get_subUser_byCid(data, cb)
              }
              break
            case 3: //agent - 自己
              userId = session.get("cid")

              if (session.get("isSub") == 0) {
                //Agent 查詢 Wallet
                gameDao.getWallet({ cid: userId }, cb)
              } else {
                //Agent Manager
                userDao.get_subUser_byCid({ cid: userId }, cb)
              }
              break
          }
        },
        function (r_code, r_currencies, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.LOAD_CURRENCY_FAIL,
              data: null,
            })
            return
          }

          switch (level) {
            case 1:
              data["betCurrency"] = r_currencies.map((x) => x.currency)
              break
            case 2:
              if (r_currencies != "" && session.get("isSub") == 0) {
                data["betCurrency"] = r_currencies.split(",")
              } else {
                const [target] = r_currencies
                data["betCurrency"] = target.Currencies.split(",")
              }
              break
            case 3:
              if (session.get("isSub") == 0) {
                var betCurrency = []
                r_currencies.forEach((item) => {
                  if (item.Currency != null && item.Currency != "") {
                    betCurrency.push(item.Currency)
                  }
                })
                data["betCurrency"] = betCurrency
              } else {
                const [target] = r_currencies
                data["betCurrency"] = target.Currencies.split(",")
              }
              break
          }

          if (isAdmin) {
            cb(null, { code: code.OK }, sys_config.main_currency)
          } else {
            if (isHa) {
              //hall
              userId = session.get("isSub") == 1 ? loginHallId : loginId
              data["user_hallId"] = userId
            } else {
              //agent admin
              userId = session.get("isSub") == 1 ? agentId : loginId
              data["user_hallId"] = loginHallId
            }
            userDao.getCusCurrency_byCid(userId, cb)
          }
        },
        function (r_code, r_currency, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.LOAD_CURRENCY_FAIL, data: null })
            return
          }
          data["currency"] = r_currency //交收者幣別
          let param = {
            cid: userId,
          }
          if (isHa) {
            userDao.getChildList(param, cb) // 取得所有 subReseller Id
          } else {
            if (isAg) data["subResellers"] = userId
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          //搜尋用戶編號
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
            return
          }
          if (r_data) {
            // subReseller Id列表
            let r_subResellers = r_data.pop()

            // 沒有subReseller 或 不是Reseller
            if (!r_subResellers) {
              data["subResellers"] = []
              cb(null, { code: code.OK }, [])
              return
            }

            // subReseller Id列表
            data["subResellers"] = r_subResellers[Object.keys(r_subResellers)[0]].split(",")
            data["subResellers"].splice(0, 1) // 刪除第 0 筆: '$,rUpjbUmzFkAV(自己),7ejdZ2jNAZvQ, ...'
          }

          if (typeof data.userName != "undefined" && data.userName != "") {
            //取user帳號
            let param = {
              userName: data.userName,
              IsSub: 0,
            }
            if (!isAdmin) param["hallList"] = data["subResellers"]
            userDao.getUserIds_byName(param, cb) //搜尋用戶編號
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
            return
          }
          //取得相符合ID
          data["searchData"] = r_data || []
          let searchIds = []
          if (data["searchData"].length > 0) {
            data["searchData"].forEach((item) => {
              if (item["IsAg"] == 2) searchIds.push(item["Cid"]) //先不擋userName能搜尋自己
            })
          } else {
            //如果沒有搜尋帳號
            if (isAdmin) {
              data["upid"] = -1
            } else {
              data["upid"] = userId
            }
          }
          //admin hall 搜尋 hall
          if (!isAg && msg.data.userType == 2) {
            data["sel_table"] = "user_revenue_hall"
            data["user_cid"] = searchIds

            bettingDao.getUserRevenue_v2(data, cb) //list
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          //admin hall 搜尋 agent
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
            return
          }
          if (r_data) {
            ttlCount_Revenue = r_data["count"] // 頁面筆數
            let consolidatedData = {
              //帶入的資料
              downCurrency: msg.data.downCurrency, //判斷是否是合併幣別
              info: r_data["info"], //原始未合併幣別資料
              order: r_data["order"], //取出的排序資料
              level: msg.data["pageLevel"], //層級
              mergeKeys: ["id", "Currency"],
            }
            logs = handler.consolidated(consolidatedData)
          }
          let searchIds = []
          if (data["searchData"].length > 0) {
            data["searchData"].forEach((item) => {
              if (item["IsAg"] == 3 && item["Cid"] != userId) searchIds.push(item["Cid"])
            })
          }
          if (!isAg && msg.data.userType == 3) {
            data["sel_table"] = "user_revenue_agent"
            data["user_cid"] = searchIds
            bettingDao.getUserRevenue_v2(data, cb) //list
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          //admin hall agent 搜尋 player
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
            return
          }
          if (r_data) {
            ttlCount_Revenue = r_data["count"] // 頁面筆數
            let consolidatedData = {
              //帶入的資料
              downCurrency: msg.data.downCurrency, //判斷是否是合併幣別
              info: r_data["info"], //原始未合併幣別資料
              order: r_data["order"], //取出的排序資料
              level: msg.data["pageLevel"], //層級
              mergeKeys: ["id", "Currency"],
            }
            let agents = handler.consolidated(consolidatedData)
            agents.forEach((item) => {
              logs.push(item)
            })
          }
          let searchIds = []
          if (data["searchData"].length > 0) {
            data["searchData"].forEach((item) => {
              if (item["IsAg"] == 4) searchIds.push(item["Cid"])
            })
          }
          if (msg.data.userType == 4) {
            data["sel_table"] = "user_revenue_player"
            data["user_cid"] = searchIds
            bettingDao.getUserRevenue_v2(data, cb) //list
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
          return
        }
        if (r_data) {
          ttlCount_Revenue = r_data["count"] // 頁面筆數
          let consolidatedData = {
            //帶入的資料
            downCurrency: msg.data.downCurrency, //判斷是否是合併幣別
            info: r_data["info"], //原始未合併幣別資料
            order: r_data["order"], //取出的排序資料
            level: msg.data["pageLevel"], //層級
            mergeKeys: ["id", "Currency"],
          }
          let players = handler.consolidated(consolidatedData)
          players.forEach((item) => {
            logs.push(item)
          })
        }

        var data = {}
        if (msg.data.isPage == false) {
          //無分頁

          var finish = ttlCount_Revenue == msg.data["index"] + logs.length ? 1 : 0
          var index = finish == 1 ? null : msg.data["index"] + logs.length

          data = {
            totalCount: ttlCount_Revenue, //總筆數
            finish: finish,
            index: index,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        } else {
          //有分頁

          var ttlCount = ttlCount_Revenue
          var pageCount = msg.data.pageCount
          var pageCur = msg.data.page

          data = {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        }
        next(null, {
          code: code.OK,
          data: data,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getUserRevenue] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// handler.getDetail_BetHistory = function (msg, session, next) {

//     var self = this;
//     //var userSession = {};
//     var detail = [];
//     var gameId = []; //遊戲編號
//     var imageSymbol = {};
//     if (typeof msg.data === 'undefined' || typeof msg.data.wid === 'undefined') {
//         next(null, { code: code.BET.PARA_DATA_FAIL });
//         return;
//     }

//     m_async.waterfall([
//         function (cb) {

//             bettingDao.getDetail_BetHistory(msg.data, cb);
//         }, function (r_code, r_data, cb) {

//             if (r_code.code != code.OK) {
//                 next(null, { code: code.BET.HISTORY_LOAD_FAIL });
//                 return;
//             }

//             for (var i in r_data) {
//                 r_data[i]['betDate'] = timezone.UTCToLocal(r_data[i]['betDate']);
//             }

//             detail = r_data;
//             var GameId = (r_data.length > 0) ? r_data[0]['GameId'] : '';
//             gameId.push(GameId);

//             var param = {   //找遊戲圖片
//                 gameId: gameId,
//                 imageType: 2,
//             }

//             self.app.rpc.game.gameRemote.getGamesImage_admin(session, param, cb);//取圖片

//         }, function (r_code, r_data, cb) {
//             if (r_code.code != code.OK) {
//                 next(null, { code: code.BET.HISTORY_LOAD_FAIL });
//                 return;
//             }

//             r_data.forEach(item => {
//                 var symbol = item.ImageName;
//                 imageSymbol[symbol] = item.FileName
//             });

//             bettingDao.get_Jackpot_byWid(msg.data.wid, cb);  //取jackpot
//         }], function (none, r_code, r_data) {

//         if (r_code.code != code.OK) {
//             next(null, { code: code.BET.HISTORY_LOAD_FAIL });
//             return;
//         }

//         var jp_data = r_data.map(item => {
//             return {
//                 jpId: item.JPId,
//                 jpType: item.JPType,
//                 cid: item.Cid,
//                 gameId: item.GameId,
//                 amount: item.Amount
//             }
//         });

//         var data = {
//             info: detail,
//             imageSymbol: imageSymbol,
//             jp: jp_data
//         }

//         next(null, { code: code.OK, data: data });
//         return;
//     });
// };

//用戶營收-(廳主)
handler.getUserRevenue_hall = function (msg, session, next) {
  try {
    var self = this

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.start_date === "undefined" ||
      typeof msg.data.end_date === "undefined"
    ) {
      next(null, { code: code.BET.PARA_DATA_FAIL })
      return
    }

    if (typeof msg.data.isPage === "undefined") {
      msg.data["isPage"] = true //預設-有分頁
    }

    if (msg.data.isPage == true) {
      //有分頁
      if (
        typeof msg.data.page === "undefined" ||
        typeof msg.data.pageCount === "undefined" ||
        typeof msg.data.page != "number" ||
        typeof msg.data.pageCount != "number"
      ) {
        next(null, { code: code.BET.PARA_DATA_FAIL })
        return
      }
    }

    //無分頁
    if (msg.data["isPage"] == false) {
      if (typeof msg.data.finish === "undefined") {
        msg.data["finish"] = 0
      } //0:未完成
      if (typeof msg.data.index === "undefined") {
        msg.data["index"] = 0
      } //開始第N筆
      msg.data["pageCount"] = 1000 //每次取得筆數
    }

    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "id"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    var data = msg.data
    data["sel_table"] = "user_revenue_hall"
    data["isAg"] = 2

    data["upid"] = -1
    if (
      (session.get("level") == 1 || session.get("level") == 2) &&
      typeof msg.data.cid != "undefined" &&
      msg.data.cid != ""
    ) {
      data["upid"] = msg.data.cid
    }

    let userId = ""
    var ttlCount_Revenue = 0
    var logs = []
    var userCid = []
    var check_user = false
    var sys_config = self.app.get("sys_config")
    const level = session.get("level")
    m_async.waterfall(
      [
        function (cb) {
          switch (level) {
            case 1: //admin - 全部
              //self.app.rpc.config.configRemote.getCurrenies(userSession, cb); //幣別
              configDao.getCurrencyList(cb) //幣別
              break
            case 2: //hall - 下線(hall 資料表的currencies)
              userId = session.get("cid")
              if (session.get("isSub") == 0) {
                userDao.getHallCurrency_byCid(userId, cb)
              } else {
                //Hall Manager
                const data = {
                  cid: userId,
                }
                userDao.get_subUser_byCid(data, cb)
              }
              break
            case 3: //agent - 自己
              userId = session.get("cid")

              if (session.get("isSub") == 0) {
                //Agent 查詢 Wallet
                gameDao.getWallet({ cid: userId }, cb)
              } else {
                //Agent Manager
                userDao.get_subUser_byCid({ cid: userId }, cb)
              }
              break
          }
        },
        function (r_code, r_currencies, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.LOAD_CURRENCY_FAIL,
              data: null,
            })
            return
          }

          switch (level) {
            case 1:
              data["betCurrency"] = r_currencies.map((x) => x.currency)
              break
            case 2:
              if (r_currencies != "" && session.get("isSub") == 0) {
                data["betCurrency"] = r_currencies.split(",")
              } else {
                const [target] = r_currencies
                data["betCurrency"] = target.Currencies.split(",")
              }
              break
            case 3:
              if (session.get("isSub") == 0) {
                var betCurrency = []
                r_currencies.forEach((item) => {
                  if (item.Currency != null && item.Currency != "") {
                    betCurrency.push(item.Currency)
                  }
                })
                data["betCurrency"] = betCurrency
              } else {
                const [target] = r_currencies
                data["betCurrency"] = target.Currencies.split(",")
              }
              break
          }

          //取交收者幣別
          if (session.get("level") === 1) {
            cb(null, { code: code.OK }, sys_config.main_currency)
          } else {
            var userId = 0
            if (session.get("level") == 2) {
              //hall
              userId = session.get("isSub") == 1 ? session.get("hallId") : session.get("cid")
              data["user_hallId"] = userId
            } else {
              //agent
              userId = session.get("cid")
              data["user_hallId"] = session.get("hallId")
            }
            userDao.getCusCurrency_byCid(userId, cb)
          }
        },
        function (r_code, r_currency, cb) {
          //搜尋用戶編號

          if (r_code.code != code.OK) {
            next(null, { code: code.BET.LOAD_CURRENCY_FAIL, data: null })
            return
          }

          data["currency"] = r_currency //交收者幣別

          if (typeof data.userName != "undefined" && data.userName != "") {
            //取user帳號
            check_user = true
            var param = {
              IsAg: data["isAg"],
              userName: data.userName,
            }
            userDao.getUserIds_byName(param, cb) //搜尋用戶編號
          } else {
            check_user = false
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          //取得 ID 並根據點選的項目排序

          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
            return
          }
          if (check_user == true) {
            r_data.forEach((item) => {
              userCid.push(item.Cid)
            })
            data["user_cid"] = userCid
          }
          data["getIds"] = true

          bettingDao.getUserRevenue_v2(data, cb) //list
        },
      ],
      function (none, r_code, r_data) {
        logger.warn("getUserRevenue_hall", JSON.stringify(r_data))

        if (r_code.code != code.OK) {
          next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
          return
        }

        ttlCount_Revenue = r_data.count // 頁面筆數

        let consolidatedData = {
          //帶入的資料
          downCurrency: msg.data.downCurrency, //判斷是否是合併幣別
          info: r_data["info"], //原始未合併幣別資料
          order: r_data["order"], //取出的排序資料
          level: "HA", //層級
          mergeKeys: ["id", "Currency"],
        }
        if (msg.data.tabName == "everyday") consolidatedData["mergeKeys"] = ["accountDate", "Currency"]
        logs = handler.consolidated(consolidatedData)

        var data = {}
        if (msg.data.isPage == false) {
          //無分頁

          var finish = ttlCount_Revenue == msg.data["index"] + logs.length ? 1 : 0
          var index = finish == 1 ? null : msg.data["index"] + logs.length

          data = {
            totalCount: ttlCount_Revenue, //總筆數
            finish: finish,
            index: index,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        } else {
          //有分頁

          var ttlCount = ttlCount_Revenue
          var pageCount = msg.data.pageCount
          var pageCur = msg.data.page

          data = {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        }
        next(null, {
          code: code.OK,
          data: data,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getUserRevenue_hall] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//代理營收-(代理)
handler.getUserRevenue_agent = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.start_date === "undefined" ||
      typeof msg.data.end_date === "undefined" ||
      typeof msg.data.upid === "undefined"
    ) {
      next(null, { code: code.BET.PARA_DATA_FAIL })
      return
    }
    if (typeof msg.data.isPage === "undefined") {
      msg.data["isPage"] = true //預設-有分頁
    }

    if (msg.data.isPage == true) {
      //有分頁
      if (
        typeof msg.data.page === "undefined" ||
        typeof msg.data.pageCount === "undefined" ||
        typeof msg.data.page != "number" ||
        typeof msg.data.pageCount != "number"
      ) {
        next(null, { code: code.BET.PARA_DATA_FAIL })
        return
      }
    }

    //無分頁
    if (msg.data["isPage"] == false) {
      if (typeof msg.data.finish === "undefined") {
        msg.data["finish"] = 0
      } //0:未完成
      if (typeof msg.data.index === "undefined") {
        msg.data["index"] = 0
      } //開始第N筆
      msg.data["pageCount"] = 1000 //每次取得筆數
    }

    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "id"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    var data = msg.data
    data["sel_table"] = "user_revenue_agent"
    data["isAg"] = 3
    var check_user = false

    let userId = ""
    var ttlCount_Revenue = 0
    var logs = []
    var userCid = []
    const level = session.get("level")
    m_async.waterfall(
      [
        function (cb) {
          switch (level) {
            case 1: //admin - 全部
              //self.app.rpc.config.configRemote.getCurrenies(userSession, cb); //幣別
              configDao.getCurrencyList(cb) //幣別
              break
            case 2: //hall - 下線(hall 資料表的currencies)
              userId = session.get("cid")
              if (session.get("isSub") == 0) {
                userDao.getHallCurrency_byCid(userId, cb)
              } else {
                //Hall Manager
                const data = {
                  cid: userId,
                }
                userDao.get_subUser_byCid(data, cb)
              }
              break
            case 3: //agent - 自己
              userId = session.get("cid")

              if (session.get("isSub") == 0) {
                //Agent 查詢 Wallet
                gameDao.getWallet({ cid: userId }, cb)
              } else {
                //Agent Manager
                userDao.get_subUser_byCid({ cid: userId }, cb)
              }
              break
          }
        },
        function (r_code, r_currencies, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.LOAD_CURRENCY_FAIL,
              data: null,
            })
            return
          }

          switch (level) {
            case 1:
              data["betCurrency"] = r_currencies.map((x) => x.currency)
              break
            case 2:
              if (r_currencies != "" && session.get("isSub") == 0) {
                data["betCurrency"] = r_currencies.split(",")
              } else {
                const [target] = r_currencies
                data["betCurrency"] = target.Currencies.split(",")
              }
              break
            case 3:
              if (session.get("isSub") == 0) {
                var betCurrency = []
                r_currencies.forEach((item) => {
                  if (item.Currency != null && item.Currency != "") {
                    betCurrency.push(item.Currency)
                  }
                })
                data["betCurrency"] = betCurrency
              } else {
                const [target] = r_currencies
                data["betCurrency"] = target.Currencies.split(",")
              }
              break
          }

          if (typeof data.userName != "undefined" && data.userName != "") {
            //取user帳號
            check_user = true
            var param = {
              IsAg: data["isAg"],
              userName: data.userName,
            }
            userDao.getUserIds_byName(param, cb) //搜尋用戶編號
          } else {
            check_user = false
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.LOAD_USER_FAIL, data: null })
            return
          }

          //遊戲
          if (check_user == true) {
            r_data.forEach((item) => {
              userCid.push(item.Cid)
            })
            data["user_cid"] = userCid
          }

          gameDao.currencyExchangeRate({}, cb) // 取得所有幣別匯率
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.RATE_NOT_VALID, data: null })
            return
          }

          bettingDao.getUserRevenue_v2(data, cb) //list
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
          return
        }

        ttlCount_Revenue = r_data.count // 頁面筆數

        let consolidatedData = {
          //帶入的資料
          downCurrency: msg.data.downCurrency, //判斷是否是合併幣別
          info: r_data["info"], //原始未合併幣別資料
          order: r_data["order"], //取出的排序資料
          level: "AG", //層級
          mergeKeys: ["id", "Currency"],
        }
        if (msg.data.tabName == "everyday") consolidatedData["mergeKeys"] = ["accountDate", "Currency"]
        logs = handler.consolidated(consolidatedData)

        var data = {}
        if (msg.data.isPage == false) {
          //無分頁
          var finish = ttlCount_Revenue == msg.data["index"] + logs.length ? 1 : 0
          var index = finish == 1 ? null : msg.data["index"] + logs.length

          data = {
            totalCount: ttlCount_Revenue, //總筆數
            finish: finish,
            index: index,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        } else {
          //有分頁

          var ttlCount = ttlCount_Revenue
          var pageCount = msg.data.pageCount
          var pageCur = msg.data.page

          data = {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        }

        next(null, { code: code.OK, data: data })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getUserRevenue_agent] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//代理營收-(玩家)
handler.getUserRevenue_player = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.start_date === "undefined" ||
      typeof msg.data.end_date === "undefined" ||
      typeof msg.data.upid === "undefined"
    ) {
      next(null, { code: code.BET.PARA_DATA_FAIL })
      return
    }
    if (typeof msg.data.isPage === "undefined") {
      msg.data["isPage"] = true //預設-有分頁
    }

    if (msg.data.isPage == true) {
      //有分頁
      if (
        typeof msg.data.page === "undefined" ||
        typeof msg.data.pageCount === "undefined" ||
        typeof msg.data.page != "number" ||
        typeof msg.data.pageCount != "number"
      ) {
        next(null, { code: code.BET.PARA_DATA_FAIL })
        return
      }
    }

    //無分頁
    if (msg.data["isPage"] == false) {
      if (typeof msg.data.finish === "undefined") {
        msg.data["finish"] = 0
      } //0:未完成
      if (typeof msg.data.index === "undefined") {
        msg.data["index"] = 0
      } //開始第N筆
      msg.data["pageCount"] = 1000 //每次取得筆數
    }

    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "id"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    var data = msg.data
    data["sel_table"] = "user_revenue_player"
    data["isAg"] = 4

    let userId = ""
    var ttlCount_Revenue = 0
    var logs = []
    const level = session.get("level")
    //count
    m_async.waterfall(
      [
        function (cb) {
          switch (level) {
            case 1: //admin - 全部
              //self.app.rpc.config.configRemote.getCurrenies(userSession, cb); //幣別
              configDao.getCurrencyList(cb) //幣別
              break
            case 2: //hall - 下線(hall 資料表的currencies)
              userId = session.get("cid")
              if (session.get("isSub") == 0) {
                userDao.getHallCurrency_byCid(userId, cb)
              } else {
                //Hall Manager
                const data = {
                  cid: userId,
                }
                userDao.get_subUser_byCid(data, cb)
              }
              break
            case 3: //agent - 自己
              userId = session.get("cid")

              if (session.get("isSub") == 0) {
                //Agent 查詢 Wallet
                gameDao.getWallet({ cid: userId }, cb)
              } else {
                //Agent Manager
                userDao.get_subUser_byCid({ cid: userId }, cb)
              }
              break
          }
        },
        function (r_code, r_currencies, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.LOAD_CURRENCY_FAIL,
              data: null,
            })
            return
          }

          switch (level) {
            case 1:
              data["betCurrency"] = r_currencies.map((x) => x.currency)
              break
            case 2:
              if (r_currencies != "" && session.get("isSub") == 0) {
                data["betCurrency"] = r_currencies.split(",")
              } else {
                const [target] = r_currencies
                data["betCurrency"] = target.Currencies.split(",")
              }
              break
            case 3:
              if (session.get("isSub") == 0) {
                var betCurrency = []
                r_currencies.forEach((item) => {
                  if (item.Currency != null && item.Currency != "") {
                    betCurrency.push(item.Currency)
                  }
                })
                data["betCurrency"] = betCurrency
              } else {
                const [target] = r_currencies
                data["betCurrency"] = target.Currencies.split(",")
              }
              break
          }

          gameDao.currencyExchangeRate({}, cb) // 取得所有幣別匯率
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.RATE_NOT_VALID, data: null })
            return
          }

          bettingDao.getUserRevenue_v2(data, cb) //list
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
          return
        }

        ttlCount_Revenue = r_data.count // 頁面筆數

        let consolidatedData = {
          //帶入的資料
          downCurrency: msg.data.downCurrency, //判斷是否是合併幣別
          info: r_data["info"], //原始未合併幣別資料
          order: r_data["order"], //取出的排序資料
          level: "PR", //層級
          mergeKeys: ["id", "Currency"],
        }

        if (msg.data.tabName == "everyday") consolidatedData["mergeKeys"] = ["accountDate", "Currency"]
        logs = handler.consolidated(consolidatedData)

        var data = {}
        if (msg.data.isPage == false) {
          //無分頁
          var finish = ttlCount_Revenue == msg.data["index"] + logs.length ? 1 : 0
          var index = finish == 1 ? null : msg.data["index"] + logs.length

          data = {
            totalCount: ttlCount_Revenue, //總筆數
            finish: finish,
            index: index,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        } else {
          //有分頁

          var ttlCount = ttlCount_Revenue
          var pageCount = msg.data.pageCount
          var pageCur = msg.data.page

          data = {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        }

        next(null, {
          code: code.OK,
          data: data,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getUserRevenue_player] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//revenue - 最後 押注紀錄 list
handler.getList_BetHistory_v2 = function (msg, session, next) {
  try {
    var self = this

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.start_date === "undefined" ||
      typeof msg.data.end_date === "undefined" ||
      typeof msg.data.id === "undefined"
    ) {
      next(null, { code: code.BET.PARA_DATA_FAIL })
      return
    }
    if (typeof msg.data.isPage === "undefined") {
      msg.data["isPage"] = true //預設-有分頁
    }

    if (msg.data.isPage == true) {
      //有分頁
      if (
        typeof msg.data.page === "undefined" ||
        typeof msg.data.pageCount === "undefined" ||
        typeof msg.data.page != "number" ||
        typeof msg.data.pageCount != "number"
      ) {
        next(null, { code: code.BET.PARA_DATA_FAIL })
        return
      }
    }

    //無分頁
    if (msg.data["isPage"] == false) {
      if (typeof msg.data.finish === "undefined") {
        msg.data["finish"] = 0
      } //0:未完成
      if (typeof msg.data.index === "undefined") {
        msg.data["index"] = 0
      } //開始第N筆
      msg.data["pageCount"] = 10000 // 每次取得筆數
    }

    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "betDate"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    //var userSession = {};
    var data = msg.data
    var ttlCount_bets = 0
    var gameType = []
    var gameGroup = []
    var games = []

    let gameData = []

    m_async.waterfall(
      [
        function (cb) {
          gameDao.currencyExchangeRate({}, cb) // 取得所有幣別匯率
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.RATE_NOT_VALID, data: null })
            return
          }

          self.app.rpc.config.configRemote.getGameType(session, cb) //取遊戲類別名稱
        },
        function (r_code, r_data, cb) {
          gameType = r_data

          self.app.rpc.config.configRemote.getGameGroup(session, cb) //遊戲種類
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.HISTORY_LOAD_FAIL, data: null })
            return
          }
          gameGroup = r_data

          bettingDao.getList_BetHistory_v2(data, cb) //清單
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.HISTORY_LOAD_FAIL, data: null })
            return
          }

          ttlCount_bets = r_data.count
          games = r_data["info"]

          var gameId = []
          games.forEach((item) => {
            if (gameId.indexOf(item.gameId) == -1) gameId.push(item.gameId)
          })

          gameDao.getGameName_byId({ gameId: gameId }, cb) //清單
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.HISTORY_LOAD_FAIL, data: null })
            return
          }

          gameData = r_data

          const param = games.reduce(
            (acc, cur) => {
              if (acc.haId.includes(cur.hallId) === false) {
                acc.haId.push(cur.hallId)
              }

              return acc
            },
            { haId: [], agId: [] }
          )

          userDao.getUserName_byCid(param, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.BET.HISTORY_LOAD_FAIL,
            data: null,
          })
          return
        }

        let logs = []
        games.forEach((item) => {
          let gameTypeInfo = gameType.filter((gType) => gType.Id == item["gameTypeId"])
          let type = gameTypeInfo.length > 0 ? gameTypeInfo["Value"] : ""
          let gameInfo = gameData.filter((gInfo) => gInfo.GameId == item["gameId"])
          let gameGroupInfo = gameGroup.filter((gGroup) => gGroup.GGId == item["GGId"])

          let cryDef =
            msg.data.downCurrency == "Consolidated"
              ? Number(utils.formatFloat(item["loginCryDef"] / item["cusCryDef"], 6))
              : 1
          let betGold = utils.number.multiply(item["bet"], cryDef)
          let realBetGold = utils.number.multiply(item["realBetGold"], cryDef)
          let jpRealBetGold = utils.number.multiply(item["jpRealBetGold"], cryDef)
          let jpConGoldOriginal = utils.number.multiply(item["jpConGoldOriginal"], cryDef)
          let winGold = utils.number.multiply(item["win"], cryDef)
          let jpGold = utils.number.multiply(item["jpGold"], cryDef)
          let netWin = utils.number.multiply(item["netWin"], cryDef)
          let payoutAmount = utils.number.multiply(item["payoutAmount"], cryDef)

          const dc = r_data.filter((x) => x.Cid === item["hallId"])[0].DC

          let tmp = {
            dc: dc,
            wid: item["wid"],
            roundId: item["roundId"],
            userId: item["userId"],
            memberName: item["memberName"],
            gameId: item["gameId"],
            nameE: gameInfo.length > 0 ? gameInfo[0]["NameE"] : "",
            nameC: gameInfo.length > 0 ? gameInfo[0]["NameC"] : "",
            nameG: gameInfo.length > 0 ? gameInfo[0]["NameG"] : "",
            gameTypeId: item["gameTypeId"],
            gameType: type,
            betGold: betGold,
            realBetGold: realBetGold,
            jpRealBetGold: jpRealBetGold,
            jpConGoldOriginal: jpConGoldOriginal,
            winGold: winGold,
            netWin: netWin,
            payoutAmount: payoutAmount,
            jpGold: jpGold,
            currency: msg.data.downCurrency == "Consolidated" ? "Consolidated" : item["currency"], //投注幣別
            cryDef: item["cryDef"],
            isDemo: item["isDemo"],
            isFree: item["isFree"],
            isBonus: item["isBonus"],
            cycleId: item["cycleId"],
            ggId: item["GGId"],
            groupNameE: gameGroupInfo.length > 0 ? gameGroupInfo[0]["NameE"] : "",
            groupNameG: gameGroupInfo.length > 0 ? gameGroupInfo[0]["NameG"] : "",
            groupNameC: gameGroupInfo.length > 0 ? gameGroupInfo[0]["NameC"] : "",
            gameState: item["gameState"],
            betDate: typeof item["betDate"] === "undefined" ? "" : item["betDate"],
            extraTriggerType: item["extraTriggerType"],
          }

          logs.push(tmp)
        })

        var info = {}
        if (msg.data.isPage == false) {
          //無分頁

          const totalRows = Number(ttlCount_bets)
          const page = Number(msg.data.page)
          const rowsPerPage = Number(msg.data.pageCount)
          const isFinish = totalRows - page * rowsPerPage < 0

          const finish = isFinish ? 1 : 0

          var index = finish == 1 ? null : msg.data["index"] + logs.length

          info = {
            totalCounts: ttlCount_bets, //總筆數
            finish: finish,
            index: index,
            page,
            logs: logs,
            // sum_logs: sum_logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        } else {
          //有分頁

          var ttlCount = ttlCount_bets
          var pageCount = msg.data.pageCount
          var pageCur = msg.data.page

          info = {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            logs: logs,
            // sum_logs: sum_logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        }

        next(null, {
          code: code.OK,
          data: info,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getList_BetHistory_v2] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

/**
 * 遊戲營收
 * @param {object} msg
 * @param {object} session
 * @param {*} next
 */
handler.getGameRevenue_games = function (msg, session, next) {
  try {
    var self = this

    let revenuetype = 0 // 預設遊戲報表進入為0，因為新增分銷商報表有遊戲報表會改為1
    if (typeof msg.data.type !== "undefined" && msg.data.type != "") {
      //分銷商報表裡面遊戲報表進入時改為1
      revenuetype = msg.data.type
    }
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.start_date === "undefined" ||
      typeof msg.data.end_date === "undefined"
    ) {
      next(null, {
        code: code.BET.PARA_DATA_FAIL,
      })
      return
    }

    if (typeof msg.data.isPage === "undefined") {
      msg.data["isPage"] = true //預設-有分頁
    }

    if (msg.data.isPage == true) {
      //有分頁
      if (
        typeof msg.data.page === "undefined" ||
        typeof msg.data.pageCount === "undefined" ||
        typeof msg.data.page != "number" ||
        typeof msg.data.pageCount != "number"
      ) {
        next(null, {
          code: code.BET.PARA_DATA_FAIL,
        })
        return
      }
    }

    //無分頁
    if (msg.data["isPage"] == false) {
      if (typeof msg.data.finish === "undefined") {
        msg.data["finish"] = 0
      } //0:未完成
      if (typeof msg.data.index === "undefined") {
        msg.data["index"] = 0
      } //開始第N筆
      msg.data["pageCount"] = 1000 //每次取得筆數
    }

    msg.data["downCurrency"] = typeof msg.data["downCurrency"] === "undefined" ? "" : msg.data["downCurrency"] //玩家遊戲幣別

    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "betGold"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    var data = msg.data
    let userId = 0
    var ttlCount_Revenue = 0
    var logs = []
    var game_group = []
    var search_user = false
    var search_game = false
    var sys_config = self.app.get("sys_config")
    let level = session.get("level")
    if (revenuetype == 1 && level == 1) {
      //因應分銷商增加遊戲報表admin登入查看分銷商裡面遊戲報表需要轉換
      if (msg.data.userLevel == "HA") {
        level = 2
      }
      if (msg.data.userLevel == "AG") {
        level = 3
      }
    }
    m_async.waterfall(
      [
        function (cb) {
          data["mainCurrency"] = sys_config.main_currency //主幣別

          //取交收者幣別
          switch (level) {
            case 1: //admin - 全部
              //self.app.rpc.config.configRemote.getCurrenies(userSession, cb); //幣別
              configDao.getCurrencyList(cb) //幣別
              break
            case 2: //hall - 下線(hall 資料表的currencies)
              if (
                (session.get("level") == 1 && revenuetype == 1) ||
                (session.get("isSub") == 0 &&
                  revenuetype == 1 &&
                  (msg.data.userLevel == "AG" || msg.data.userLevel == "HA")) ||
                (session.get("isSub") == 1 && revenuetype == 1)
              ) {
                //因應分銷商增加遊戲報表 admin看分銷商 hall看分銷商
                userId = msg.data.upid
              } else {
                //原本遊戲報表走這裡
                userId = session.get("cid")
              }
              if (
                (session.get("isSub") == 0 && revenuetype != 1) ||
                (session.get("level") == 1 && revenuetype == 1) ||
                (session.get("isSub") == 0 && revenuetype == 1 && msg.data.userLevel == "HA")
              ) {
                //這裡其實是特例當admin登入分銷商報表點選幣別是帶入分銷商幣別
                userDao.getHallCurrency_byCid(userId, cb)
              } else if (session.get("isSub") == 0 && revenuetype == 1 && msg.data.userLevel == "AG") {
                userDao.getHallCurrency_byCid(session.get("cid"), cb)
              } else if (session.get("isSub") == 1 && revenuetype == 1) {
                //這裡其實是特例當分銷商管理員登入分銷商報表點選幣別是帶入分銷商幣別
                userDao.getHallCurrency_byCid(session.get("hallId"), cb)
              } else {
                //Hall Manager
                const data = {
                  cid: userId,
                }
                userDao.get_subUser_byCid(data, cb)
              }
              break
            case 3: //agent - 自己
              if (revenuetype == 1) {
                //因應分銷商增加遊戲報表
                userId = msg.data.upid
              } else {
                //原本遊戲報表
                userId = session.get("cid")
              }

              if (session.get("isSub") == 0 || (session.get("level") == 1 && revenuetype == 1)) {
                //Agent 查詢 Wallet
                gameDao.getWallet({ cid: userId }, cb)
              } else {
                //Agent Manager
                userDao.get_subUser_byCid({ cid: userId }, cb)
              }
              break
          }
        },
        function (r_code, r_currencies, cb) {
          if (r_code.code != code.OK || r_code.code != 200) {
            next(null, {
              code: code.BET.LOAD_CURRENCY_FAIL,
              data: null,
            })
            return
          }

          switch (level) {
            case 1:
              data["betCurrency"] = r_currencies.map((x) => x.currency)
              break
            case 2:
              if (
                (r_currencies != "" && session.get("isSub") == 0) ||
                (session.get("level") == 1 && revenuetype == 1) ||
                (session.get("isSub") == 1 && revenuetype == 1)
              ) {
                //這裡其實是特例當admin登入分銷商報表點選幣別是帶入分銷商幣別
                //這裡其實是特例當分銷商管理員登入分銷商報表點選幣別是帶入分銷商幣別
                data["betCurrency"] = r_currencies.split(",")
              } else {
                const [target] = r_currencies
                data["betCurrency"] = target.Currencies.split(",")
              }
              break
            case 3:
              if (session.get("isSub") == 0 || (session.get("level") == 1 && revenuetype == 1)) {
                var betCurrency = []
                r_currencies.forEach((item) => {
                  if (item.Currency != null && item.Currency != "") {
                    betCurrency.push(item.Currency)
                  }
                })
                data["betCurrency"] = betCurrency
              } else {
                const [target] = r_currencies
                data["betCurrency"] = target.Currencies.split(",")
              }
              break
          }

          data["user_level"] = level //交收者身分

          self.app.rpc.config.configRemote.getGameGroup(session, cb) //遊戲種類
        },
        function (r_code, r_data, cb) {
          game_group = r_data

          if (typeof data.userName != "undefined" && data.userName != "") {
            //搜尋帳號
            search_user = true
            var level = data.user_level + 1
            var param = {
              upid: data.upid,
              level: level, //isAg
              userName: data.userName,
            }
            userDao.getCid_byUserName(param, cb)
          } else {
            cb(
              null,
              {
                code: code.OK,
              },
              null
            )
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.GAME_LOAD_FAIL,
              data: null,
            })
            return
          }

          data["search_user"] = search_user
          if (search_user) {
            var userIds = []
            r_data.forEach((item) => {
              userIds.push(item.Cid)
            })
            data["userId"] = userIds
          }

          if (typeof data.gameName != "undefined" && data.gameName != "") {
            //遊戲名搜尋
            search_game = true
            let user_level = ""
            switch (level) {
              case 2:
                //原本判斷是用isSub但是因為分銷商裡面遊戲要進入需要判斷但是admin沒有isSub所以增加
                if (session.get("isSub") == 0 || (session.get("level") == 1 && revenuetype == 1)) {
                  user_level = session.get("cid")
                } else {
                  user_level = session.get("hallId")
                }
                break
              case 3:
                user_level = session.get("hallId")
                break
            }
            var param = {
              haId: user_level,
              level: data.user_level, //(登入者身分-ad:1,ha/sha:2,ag:3)
              gameName: data.gameName,
            }
            gameDao.getGameIdbyGameName(param, cb) //遊戲報表透過遊戲名稱搜尋遊戲編號
          } else {
            cb(
              null,
              {
                code: code.OK,
              },
              null
            )
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            if (r_code.code != code.OK) {
              next(null, {
                code: code.BET.REVENUE_LOAD_FAIL,
                data: null,
              })
              return
            }
          }
          data["search_game"] = search_game

          if (search_game == true) {
            //有使用遊戲搜尋
            data["search_gameId"] = r_data.map((item) => item.GameId)
          }

          //hall id OR agent id
          if (data.user_level == 2) {
            //原本判斷是用isSub但是因為分銷商裡面遊戲要進入需要判斷但是admin沒有isSub所以增加
            if (session.get("isSub") == 0 || (session.get("level") == 1 && revenuetype == 1)) {
              const param = { cid: userId }
              userDao.getChildList(param, cb)
            } else if (session.get("isSub") == 1 && session.get("level") == 2 && revenuetype == 1) {
              //分銷商hall manager
              const param = { cid: msg.data.upid }
              userDao.getChildList(param, cb)
            } else {
              // hall manager
              const param = { cid: session.get("hallId") }
              userDao.getChildList(param, cb)
            }
          } else if (data.user_level == 3) {
            //原本判斷是用isSub但是因為分銷商裡面遊戲要進入需要判斷但是admin沒有isSub所以增加
            if (session.get("isSub") == 0 || (session.get("level") == 1 && revenuetype == 1)) {
              const param = { cid: userId }
              userDao.getChildList(param, cb)
            } else if (session.get("isSub") == 1 && session.get("level") == 3 && revenuetype == 1) {
              const param = { cid: msg.data.upid }
              userDao.getChildList(param, cb)
            } else {
              // agent manager
              const param = { cid: session.get("agentId") }
              userDao.getChildList(param, cb)
            }
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
            return
          }
          let downLineCid = []
          if (r_data) {
            let downLine = r_data.pop()
            downLineCid = downLine[Object.keys(downLine)[0]].split(",")
            downLineCid.splice(0, 1) // 刪除第0 & 1筆: '$,rUpjbUmzFkAV(自己),7ejdZ2jNAZvQ, ...'
          }
          if (data.user_level == 2) {
            data["upId"] = downLineCid
          } else if (data.user_level == 3) {
            if (
              session.get("isSub") == 0 ||
              (session.get("level") == 1 && revenuetype == 1) ||
              (session.get("level") == 3 && revenuetype == 1)
            ) {
              data["upId"] = [userId]
            } else {
              data["upId"] = [session.get("agentId")]
            }
          } else {
            data["upId"] = ["-1"]
          }
          switch (data["user_level"]) {
            case 1: //admin 取 hall
              data["sel_table"] = "game_revenue_hall"
              data["tableUpIdName"] = "UpId"
              break
            case 2: //hall 取 agent
              if (msg.data.userLevel == "AG" && revenuetype == 1) {
                data["sel_table"] = "game_revenue_player"
                data["tableUpIdName"] = "UpId"
              } else {
                data["sel_table"] = "game_revenue_agent"
                data["tableUpIdName"] = "HallId"
              }
              break
            case 3: //agent 取 player
              data["sel_table"] = "game_revenue_player"
              data["tableUpIdName"] = "UpId"
              break
          }
          data["uniPlayersGameId"] = true
          data["select"] = [
            "gameId",
            "gameNameC",
            "gameNameG",
            "gameNameE",
            "GGId",
            "currency",
            "rounds",
            "BaseRounds",
            "uniPlayers",
          ]
          data["outGroupBy"] = ["gameId"]
          data["innerGroupBy"] = ["w.GameId", "Currency"]
          bettingDao.get_Currency_Convert_Revenue(data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.BET.REVENUE_LOAD_FAIL,
            data: null,
          })
          return
        }

        let consolidatedData = {
          //帶入的資料
          downCurrency: msg.data.downCurrency, //判斷是否是合併幣別
          info: r_data["info"], //原始未合併幣別資料
          order: r_data["order"], //取出的排序資料
          level: "GAME", //層級
          mergeKeys: ["gameId", "Currency"],
        }
        logs = handler.consolidated(consolidatedData)
        ttlCount_Revenue = r_data.count

        for (let i in logs) {
          //在分銷商報表增加遊戲報表
          logs[i]["revenuetype"] = revenuetype
          var gameInfo = game_group.filter((group) => parseInt(group.GGId) == parseInt(logs[i]["ggId"])) // 遊戲種類
          logs[i]["groupNameE"] = gameInfo.length > 0 ? gameInfo[0]["NameE"] : ""
          logs[i]["groupNameG"] = gameInfo.length > 0 ? gameInfo[0]["NameG"] : ""
          logs[i]["groupNameC"] = gameInfo.length > 0 ? gameInfo[0]["NameC"] : ""
        }

        var data = {}
        if (msg.data.isPage == false) {
          //無分頁

          var finish = ttlCount_Revenue == msg.data["index"] + logs.length ? 1 : 0
          var index = finish == 1 ? null : msg.data["index"] + logs.length

          data = {
            totalCount: ttlCount_Revenue, //總筆數
            finish: finish,
            index: index,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        } else {
          //有分頁

          var ttlCount = ttlCount_Revenue
          var pageCount = msg.data.pageCount
          var pageCur = msg.data.page

          data = {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        }

        next(null, {
          code: code.OK,
          data: data,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getGameRevenue_games] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.consolidated = function (data) {
  /*let data = { //帶入的資料
        downCurrency: msg.data.downCurrency,//判斷是否是合併幣別
        info: r_data['info'],   //原始未合併幣別資料
        order: r_data['order'], //取出的排序資料
        level: 'PR'             //層級
        mergeKeys: ['id', 'Currency'] // 合併項目以第一個為主，一定要有幣別
        showOriginCurrency: true      //合併幣別時顯示原始幣別
    }
    傳進來的值名稱需相同，且需先/1000，之後維持原值不計算(除了 groupby 時需要的 sum):
    UserName,GGId,UpId,NameE,NameG,NameC,accountDate,uniPlayers,rounds,IsDemo
    id, gameId, BetGold, BetPoint, JPPoint, JPGold, WinGold, Currency, loginCryDef, cusCryDef
    */
  let mergeKey = data["mergeKeys"][0]

  let listOrder = []
  data["order"].forEach((item) => {
    // 先取得排序
    let repeat =
      mergeKey.toLowerCase() == "currency"
        ? listOrder.some((list) => list.Currency == item.Currency)
        : listOrder.some((list) => list[mergeKey] == item[mergeKey] && list.Currency == item.Currency)
    let listOrderData = {}
    data["mergeKeys"].forEach((key) => {
      if (key != "Currency") {
        listOrderData[key] = item[key]
      } else {
        listOrderData["Currency"] =
          data["downCurrency"] != "Consolidated" || data["showOriginCurrency"] ? item["Currency"] : data["downCurrency"]
      }
    })
    if (!repeat) listOrder.push(listOrderData)
  })

  let logs = []

  const convertCurrency = (value, cryDef) => BN(value).times(cryDef).toNumber()

  listOrder.forEach((listData) => {
    data["info"].forEach((item) => {
      if (
        listData[mergeKey] == item[mergeKey] &&
        (listData["Currency"] == item["Currency"] || data.downCurrency == "Consolidated")
      ) {
        // 找到和排序一樣的 第一個 mergeKeys 和幣別

        const loginCryDef = item["loginCryDef"]
        const cusCryDef = item["cusCryDef"]

        const convertCurrencyRate = BN(loginCryDef).div(cusCryDef).dp(6).toNumber()

        const cryDef = data.downCurrency == "Consolidated" ? convertCurrencyRate : 1
        const uniPlayers = item["uniPlayers"] || 0
        const rounds = item["rounds"] || 0
        const BaseRounds = item["BaseRounds"] || 0
        const betGold = convertCurrency(item["BetGold"], cryDef)
        const realBetGold = convertCurrency(item["RealBetGold"], cryDef)
        const jpRealBetGold = convertCurrency(item["JPRealBetGold"], cryDef)
        const jpGold = convertCurrency(item["JPGold"], cryDef)
        const jpConGoldOriginal = convertCurrency(item["JPConGoldOriginal"], cryDef)
        const winGold = convertCurrency(item["WinGold"], cryDef)
        const payoutAmount = BN(winGold).minus(jpGold).dp(4).toNumber()
        const netWin = BN(betGold).minus(payoutAmount).dp(4).toNumber()

        const consolidatedCurrency = data["showOriginCurrency"]
          ? item["Currency"] + " > " + data["loginCurrency"]
          : "Consolidated"

        const currency = data.downCurrency == "Consolidated" ? consolidatedCurrency : item["Currency"]

        const repeat = logs.some((log) => log[mergeKey] == item[mergeKey] && log["currency"] == currency)

        let userLevel = "Hall"
        switch (item["userLevel"]) {
          case 3:
            userLevel = "Agent"
            break
          case 4:
            userLevel = "Player"
            break
        }

        const tmp = {
          // 預設值
          id: item["id"],
          userName: item["UserName"],
          gameId: item["gameId"],
          level: data["level"],
          userLevel: userLevel,
          rounds: item["rounds"],
          BaseRounds: item["BaseRounds"],
          ggId: item["GGId"],
          upId: item["UpId"],
          isDemo: item["IsDemo"],
          uniPlayers: 0,
          betGold: 0,
          realBetGold: 0,
          jpRealBetGold: 0,
          winGold: 0,
          jpGold: 0,
          jpConGoldOriginal: 0,
          netWin: 0,
          rtp: 0,
          rtpNoJP: 0,
          payoutAmount: 0,
          currency: currency,
          gameNameE: item["NameE"], //遊戲名稱
          gameNameG: item["NameG"],
          gameNameC: item["NameC"],
          accountDate: typeof item["accountDate"] === "undefined" ? "" : item["accountDate"],
        }

        if (repeat) {
          //有重複累加
          logs.forEach((log) => {
            if (log[mergeKey] == item[mergeKey] && log["currency"] == currency) {
              log["rounds"] = BN(log["rounds"]).plus(rounds).toNumber()
              log["BaseRounds"] = BN(log["BaseRounds"]).plus(BaseRounds).toNumber()
              log["uniPlayers"] = BN(log["uniPlayers"]).plus(uniPlayers).toNumber()
              log["betGold"] = BN(log["betGold"]).plus(betGold).toNumber()
              log["realBetGold"] = BN(log["realBetGold"]).plus(realBetGold).toNumber()
              log["jpRealBetGold"] = BN(log["jpRealBetGold"]).plus(jpRealBetGold).toNumber()
              log["winGold"] = BN(log["winGold"]).plus(winGold).toNumber()
              log["payoutAmount"] = BN(log["payoutAmount"]).plus(payoutAmount).toNumber()
              log["jpGold"] = BN(log["jpGold"]).plus(jpGold).toNumber()
              log["jpConGoldOriginal"] = BN(log["jpConGoldOriginal"]).plus(jpConGoldOriginal).toNumber()
              log["netWin"] = BN(log["netWin"]).plus(netWin).toNumber()
            }
          })

          logs[0].rtp = BN(logs[0].winGold).div(logs[0].realBetGold).times(100).toNumber()
          logs[0].rtpNoJP = BN(logs[0].payoutAmount).div(logs[0].realBetGold).times(100).toNumber()
        } else {
          //無重複新增
          tmp["rounds"] = rounds
          tmp["BaseRounds"] = BaseRounds
          tmp["uniPlayers"] = uniPlayers
          tmp["betGold"] = betGold
          tmp["realBetGold"] = realBetGold
          tmp["jpRealBetGold"] = jpRealBetGold
          tmp["winGold"] = winGold
          tmp["payoutAmount"] = payoutAmount
          tmp["jpGold"] = jpGold
          tmp["jpConGoldOriginal"] = jpConGoldOriginal
          tmp["netWin"] = netWin

          tmp["rtp"] = BN(winGold).div(realBetGold).times(100).dp(4).toNumber()
          tmp["rtpNoJP"] = BN(payoutAmount).div(realBetGold).times(100).dp(4).toNumber()
          logs.push(tmp)
        }
      }
    })
  })
  return logs
}

//遊戲營收-(廳主)
handler.getGameRevenue_hall = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.start_date === "undefined" ||
      typeof msg.data.end_date === "undefined" ||
      typeof msg.data.gameId === "undefined"
    ) {
      next(null, {
        code: code.BET.PARA_DATA_FAIL,
      })
      return
    }

    if (typeof msg.data.isPage === "undefined") {
      msg.data["isPage"] = true //預設-有分頁
    }

    if (msg.data.isPage == true) {
      //有分頁
      if (
        typeof msg.data.page === "undefined" ||
        typeof msg.data.pageCount === "undefined" ||
        typeof msg.data.page != "number" ||
        typeof msg.data.pageCount != "number"
      ) {
        next(null, {
          code: code.BET.PARA_DATA_FAIL,
        })
        return
      }
    }

    //無分頁
    if (msg.data["isPage"] == false) {
      if (typeof msg.data.finish === "undefined") {
        msg.data["finish"] = 0
      } //0:未完成
      if (typeof msg.data.index === "undefined") {
        msg.data["index"] = 0
      } //開始第N筆
      msg.data["pageCount"] = 1000 //每次取得筆數
    }

    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "betGold"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    var data = msg.data
    var ttlCount_Revenue = 0
    var logs = []

    m_async.waterfall(
      [
        function (cb) {
          gameDao.currencyExchangeRate({}, cb) // 取得所有幣別匯率
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.RATE_NOT_VALID, data: null })
            return
          }

          data["user_level"] = session.get("level") //交收者身分
          data["upId"] = [data["upId"]]
          data["tableUpIdName"] = "upId"
          data["isAg"] = 2
          data["sel_table"] = "game_revenue_hall"
          data["uniPlayersGameId"] = true
          data["select"] = ["cid", "userName", "userLevel", "rounds", "BaseRounds", "currency"]
          data["outGroupBy"] = ["id"]
          data["innerGroupBy"] = ["w.Cid", "Currency"]

          bettingDao.get_Currency_Convert_Revenue(data, cb)

          // bettingDao.getGameRevenue_users_v2(data, cb); //清單
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.BET.REVENUE_LOAD_FAIL,
            data: null,
          })
          return
        }
        ttlCount_Revenue = r_data.count

        let consolidatedData = {
          downCurrency: msg.data.downCurrency,
          info: r_data["info"],
          order: r_data["order"],
          level: "HA",
          mergeKeys: ["id", "Currency"],
        }
        logs = handler.consolidated(consolidatedData)

        var data = {}
        if (msg.data.isPage == false) {
          //無分頁

          var finish = ttlCount_Revenue == msg.data["index"] + logs.length ? 1 : 0
          var index = finish == 1 ? null : msg.data["index"] + logs.length

          data = {
            totalCount: ttlCount_Revenue, //總筆數
            finish: finish,
            index: index,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        } else {
          //有分頁

          var ttlCount = ttlCount_Revenue
          var pageCount = msg.data.pageCount
          var pageCur = msg.data.page

          data = {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        }

        next(null, {
          code: code.OK,
          data: data,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getGameRevenue_hall] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//遊戲營收-(代理)
handler.getGameRevenue_agent = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.start_date === "undefined" ||
      typeof msg.data.end_date === "undefined" ||
      typeof msg.data.upId === "undefined" ||
      typeof msg.data.gameId === "undefined"
    ) {
      next(null, { code: code.BET.PARA_DATA_FAIL })
      return
    }

    if (typeof msg.data.isPage === "undefined") {
      msg.data["isPage"] = true //預設-有分頁
    }
    if (msg.data.isPage == true) {
      //有分頁
      if (
        typeof msg.data.page === "undefined" ||
        typeof msg.data.pageCount === "undefined" ||
        typeof msg.data.page != "number" ||
        typeof msg.data.pageCount != "number"
      ) {
        next(null, { code: code.BET.PARA_DATA_FAIL })
        return
      }
    }

    //無分頁
    if (msg.data["isPage"] == false) {
      if (typeof msg.data.finish === "undefined") {
        msg.data["finish"] = 0
      } //0:未完成
      if (typeof msg.data.index === "undefined") {
        msg.data["index"] = 0
      } //開始第N筆
      msg.data["pageCount"] = 1000 //每次取得筆數
    }

    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "betGold"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    var data = msg.data

    var ttlCount_Revenue = 0
    var logs = []

    m_async.waterfall(
      [
        function (cb) {
          data["user_level"] = session.get("level") //交收者身分

          data["isAg"] = 3
          data["sel_table"] = "game_revenue_agent"
          data["upId"] = [data["upId"]]
          data["tableUpIdName"] = "HallId"
          data["uniPlayersGameId"] = true
          data["select"] = ["cid", "userName", "userLevel", "rounds", "BaseRounds", "currency", "isDemo"]
          data["outGroupBy"] = ["id"]
          data["innerGroupBy"] = ["w.Cid", "Currency"]
          bettingDao.get_Currency_Convert_Revenue(data, cb)
          // bettingDao.getGameRevenue_users_v2(data, cb); //清單
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
          return
        }
        ttlCount_Revenue = r_data.count

        let consolidatedData = {
          downCurrency: msg.data.downCurrency,
          info: r_data["info"],
          order: r_data["order"],
          level: "AG",
          mergeKeys: ["id", "Currency"],
        }
        logs = handler.consolidated(consolidatedData)

        var data = {}
        if (msg.data.isPage == false) {
          //無分頁

          var finish = ttlCount_Revenue == msg.data["index"] + logs.length ? 1 : 0
          var index = finish == 1 ? null : msg.data["index"] + logs.length

          data = {
            totalCount: ttlCount_Revenue, //總筆數
            finish: finish,
            index: index,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        } else {
          //有分頁

          var ttlCount = ttlCount_Revenue
          var pageCount = msg.data.pageCount
          var pageCur = msg.data.page

          data = {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        }

        next(null, { code: code.OK, data: data })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getGameRevenue_agent] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//遊戲營收-(玩家)
handler.getGameRevenue_player = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.start_date === "undefined" ||
      typeof msg.data.end_date === "undefined" ||
      typeof msg.data.upId === "undefined" ||
      typeof msg.data.gameId === "undefined"
    ) {
      next(null, { code: code.BET.PARA_DATA_FAIL })
      return
    }

    if (typeof msg.data.isPage === "undefined") {
      msg.data["isPage"] = true //預設-有分頁
    }

    if (msg.data.isPage == true) {
      //有分頁
      if (
        typeof msg.data.page === "undefined" ||
        typeof msg.data.pageCount === "undefined" ||
        typeof msg.data.page != "number" ||
        typeof msg.data.pageCount != "number"
      ) {
        next(null, { code: code.BET.PARA_DATA_FAIL })
        return
      }
    }

    //無分頁
    if (msg.data["isPage"] == false) {
      if (typeof msg.data.finish === "undefined") {
        msg.data["finish"] = 0
      } //0:未完成
      if (typeof msg.data.index === "undefined") {
        msg.data["index"] = 0
      } //開始第N筆
      msg.data["pageCount"] = 1000 //每次取得筆數
    }

    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "betGold"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    var data = msg.data
    // data['sel_table'] = 'game_revenue_player';
    // data['isAg'] = 4;
    var ttlCount_Revenue = 0
    var logs = []

    m_async.waterfall(
      [
        function (cb) {
          data["user_level"] = session.get("level") //交收者身分

          data["isAg"] = 4
          data["sel_table"] = "game_revenue_player"
          data["upId"] = [data["upId"]]
          data["tableUpIdName"] = "upId"
          data["uniPlayersGameId"] = true
          data["select"] = ["cid", "userName", "userLevel", "rounds", "BaseRounds", "currency", "isDemo"]
          data["outGroupBy"] = ["id"]
          data["innerGroupBy"] = ["w.Cid", "Currency"]

          // 停用 選擇玩家層級報表且為Admin登入時 選擇全部玩家
          data["isActiveAdminPlayerDisplay"] = false
          bettingDao.get_Currency_Convert_Revenue(data, cb)
        },
        // , function (r_code, r_data, cb) {
        //     if (r_code.code != code.OK) {
        //         next(null, { code: code.USR.RATE_NOT_VALID, data: null });
        //         return;
        //     }
        //     r_exchangeRate = r_data; // 所有 幣別 & 匯率

        //     bettingDao.getGameRevenue_users_v2(data, cb); //清單
        // }
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
          return
        }
        ttlCount_Revenue = r_data.count

        let consolidatedData = {
          downCurrency: msg.data.downCurrency,
          info: r_data["info"],
          order: r_data["order"],
          level: "PR",
          mergeKeys: ["id", "Currency"],
        }
        logs = handler.consolidated(consolidatedData)

        var data = {}
        if (msg.data.isPage == false) {
          //無分頁

          var finish = ttlCount_Revenue == msg.data["index"] + logs.length ? 1 : 0
          var index = finish == 1 ? null : msg.data["index"] + logs.length

          data = {
            totalCount: ttlCount_Revenue, //總筆數
            finish: finish,
            index: index,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        } else {
          //有分頁

          var ttlCount = ttlCount_Revenue
          var pageCount = msg.data.pageCount
          var pageCur = msg.data.page

          data = {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        }

        next(null, { code: code.OK, data: data })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getGameRevenue_player] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//遊戲營收 - 取玩家總下注訊息(子單)
// handler.getPlayerBetSum = function (msg, session, next) {
//     var self = this;
//     if (typeof msg.data === 'undefined' || typeof msg.data.start_date === 'undefined' || typeof msg.data.end_date === 'undefined'
//         || typeof msg.data.gameId === 'undefined' || typeof msg.data.wid === 'undefined') {
//         next(null, { code: code.BET.PARA_DATA_FAIL });
//         return;
//     }

//     //時間轉換
//     msg.data.start_date = timezone.UTCToLocal(msg.data.start_date);
//     msg.data.end_date = timezone.UTCToLocal(msg.data.end_date);

//     var data = msg.data;
//     var ttlCount_Revenue = 0;
//     var logs = [];
//     var usrs = {};
//     var records = [];
//     //var userSession = {};

//     m_async.waterfall([
//         function (cb) {

//             //反查玩家
//             bettingDao.getPlayerId_byWid(data, cb); //總筆數

//         }, function (r_code, cid, cb) {

//             if (r_code.code != code.OK) {
//                 next(null, { code: code.BET.PLAYER_NOT_EXIST, data: null });
//                 return;
//             }
//             data['cid'] = cid;

//             bettingDao.getUserBettingCount(data, cb); //清單
//         }], function (none, r_code, r_data) {

//         if (r_code.code != code.OK) {
//             next(null, { code: code.BET.PLAYER_BET_INFO_FAIL, data: null });
//             return;
//         }

//         next(null, {
//             code: code.OK, data: r_data
//         });
//         return;
//     });
// };
handler.getList_BetHistory_v3 = function (msg, session, next) {
  try {
    var self = this
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.level === "undefined" ||
      typeof msg.data.upid === "undefined"
    ) {
      next(null, {
        code: code.BET.PARA_DATA_FAIL,
      })
      return
    }
    if (typeof msg.data.isPage === "undefined") {
      msg.data["isPage"] = true //預設-有分頁
    }

    if (msg.data.isPage == true) {
      //有分頁
      if (
        typeof msg.data.page === "undefined" ||
        typeof msg.data.pageCount === "undefined" ||
        typeof msg.data.page != "number" ||
        typeof msg.data.pageCount != "number"
      ) {
        next(null, {
          code: code.BET.PARA_DATA_FAIL,
        })
        return
      }
    }

    //無分頁
    if (msg.data["isPage"] == false) {
      if (typeof msg.data.finish === "undefined") {
        msg.data["finish"] = 0
      } //0:未完成
      if (typeof msg.data.index === "undefined") {
        msg.data["index"] = 0
      } //開始第N筆
      msg.data["pageCount"] = 1000 //每次取得筆數
    }

    //注單編號 36碼
    if (typeof msg.data.wid !== "undefined" && msg.data.wid.length == 36) {
      msg.data["select_wager"] = true
    } else {
      //時間轉換
      if (typeof msg.data.start_date === "undefined" || typeof msg.data.end_date === "undefined") {
        next(null, {
          code: code.BET.PARA_DATA_FAIL,
        })
        return
      }
      msg.data["select_wager"] = false
    }

    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "betDate"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    var data = msg.data
    var ttlCount_bets = 0
    //var userSession = {};

    var logs = []
    var gameId = []
    var gameType = []
    var check_user = false
    var userCid = []
    var game_group = []
    var opCurrency = ""
    var sum = [] //交收者總計
    var info = []
    var userId = 0
    let search_game = false
    const level = session.get("level")
    m_async.waterfall(
      [
        function (cb) {
          //遊戲投注紀錄新增遊戲名稱搜尋
          if (typeof data.gameName != "undefined" && data.gameName != "") {
            //遊戲名搜尋
            search_game = true
            let user_level = ""
            switch (level) {
              case 2:
                if (session.get("isSub") == 0) {
                  user_level = session.get("cid")
                } else {
                  user_level = session.get("hallId")
                }
                break
              case 3:
                user_level = session.get("hallId")
                break
            }
            var param = {
              haId: user_level,
              level: level, //(登入者身分-ad:1,ha/sha:2,ag:3)
              gameName: data.gameName,
            }
            gameDao.getGameIdbyGameName(param, cb) //彩金報表透過遊戲名稱搜尋遊戲編號
          } else {
            cb(
              null,
              {
                code: code.OK,
              },
              null
            )
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            if (r_code.code != code.OK) {
              next(null, {
                code: code.BET.REVENUE_LOAD_FAIL,
                data: null,
              })
              return
            }
          }
          data["search_game"] = search_game
          if (search_game == true) {
            //有使用遊戲搜尋
            data["search_gameId"] = r_data.map((item) => item.GameId)
          }
          //取系統主幣別
          var param = { item: "main_currency" }
          configDao.getSystemSetting(param, cb)
        },
        function (r_code, r_data, cb) {
          data["mainCurrency"] = r_data //主幣別

          //取交收者幣別
          if (session.get("level") === 1) {
            //admin
            cb(
              null,
              {
                code: code.OK,
              },
              data["mainCurrency"]
            ) //系統主幣別
          } else {
            if (session.get("level") == 2) {
              //hall
              userId = session.get("isSub") == 1 ? session.get("hallId") : session.get("cid")
            } else {
              //agent
              userId = session.get("isSub") == 1 ? session.get("agentId") : session.get("cid")
            }
            userDao.getCusCurrency_byCid(userId, cb)
          }
        },
        function (r_code, r_currency, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.LOAD_CURRENCY_FAIL,
              data: null,
            })
            return
          }
          opCurrency = r_currency //交收者幣別

          self.app.rpc.config.configRemote.getGameType(session, cb) //取遊戲類別名稱
        },
        function (r_code, r_data, cb) {
          gameType = r_data
          let param = { cid: userId }
          userDao.getChildList(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST, data: null })
            return
          }
          if (r_data) {
            // subReseller Id列表
            let r_subResellers = r_data.pop()

            // 沒有subReseller 或 不是Reseller
            if (!r_subResellers) {
              data["subResellers"] = []
              cb(null, { code: code.OK }, [])
              return
            }

            // subReseller Id列表
            data["subResellers"] = r_subResellers[Object.keys(r_subResellers)[0]].split(",")
            data["subResellers"].splice(0, 1) // 刪除第 0 筆: '$,rUpjbUmzFkAV(自己),7ejdZ2jNAZvQ, ...'
          }
          if (typeof data.userName != "undefined" && data.userName != "" && data["level"] != "AG") {
            //查下線名稱
            check_user = true
            let param = {
              IsSub: 0,
              haOrAg: true,
              userName: data.userName,
            }
            userDao.getUserIds_byName(param, cb) //hall 或agent帳號
          } else {
            check_user = false
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST, data: null })
            return
          }
          if (check_user == true) {
            //查詢代理或分銷商帳號
            r_data.forEach((item) => {
              userCid.push(item.Cid)
            })
            data["upIds"] = userCid
          }

          //遊戲種類
          self.app.rpc.config.configRemote.getGameGroup(session, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.GAME.GAME_QUERY_DATA_FAIL, data: null })
            return
          }
          game_group = r_data

          data["opCurrency"] = opCurrency
          bettingDao.getList_BetHistory_sum(data, cb) //(轉換成交收者幣別)加總
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

          bettingDao.getList_BetHistory_v3(data, cb) //清單
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.HISTORY_LOAD_FAIL,
              data: null,
            })
            return
          }
          ttlCount_bets = r_data["count"]
          info = r_data["info"]

          //取hall帳號 及agent帳號
          var HallIds = []
          var AgentIds = []
          info.forEach((item) => {
            if (HallIds.indexOf(item.haId) == -1) HallIds.push(item.haId)
            if (AgentIds.indexOf(item.agId) == -1) AgentIds.push(item.agId)
          })

          var param = {
            haId: HallIds,
            agId: AgentIds,
          }

          userDao.getUserName_byCid(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.HISTORY_LOAD_FAIL,
              data: null,
            })
            return
          }
          console.log("getUserName_byCid r_data", JSON.stringify(r_data))

          for (var i in info) {
            if (gameId.indexOf(info[i]["gameId"].toString()) == -1) {
              gameId.push(info[i]["gameId"].toString())
            }

            var gameTypeInfo = gameType.filter((item) => item.Id == info[i]["gameTypeId"])
            var type = gameTypeInfo.length > 0 ? gameTypeInfo["Value"] : ""
            var group = game_group.filter((item) => item.GGId == info[i]["GGId"])
            var haInfo = r_data.filter((item) => item.Cid == info[i]["haId"])
            var agInfo = r_data.filter((item) => item.Cid == info[i]["agId"])
            const dc = haInfo[0]["DC"]

            var tmp = {
              dc,
              wid: info[i]["wid"],
              roundId: info[i]["roundId"],
              userId: info[i]["userId"],
              memberName: info[i]["memberName"],
              haName: haInfo.length > 0 ? haInfo[0]["UserName"] : "",
              agName: agInfo.length > 0 ? agInfo[0]["UserName"] : "",
              gameId: info[i]["gameId"],
              gameTypeId: info[i]["gameTypeId"],
              gameType: type,
              betGold: info[i]["betGold"],
              realBetGold: info[i]["realBetGold"],
              jpRealBetGold: info[i]["jpRealBetGold"],
              jpConGoldOriginal: info[i]["jpConGoldOriginal"],
              winGold: info[i]["winGold"],
              netWin: info[i]["netWin"],
              payoutAmount: info[i]["payoutAmount"],
              currency: info[i]["currency"],
              cryDef: info[i]["cryDef"],
              isDemo: info[i]["isDemo"],
              isFree: info[i]["isFree"],
              isBonus: info[i]["isBonus"],
              cycleId: info[i]["cycleId"],
              isValid: info[i]["isValid"],
              isJP: info[i]["isJP"],
              jpType: info[i]["jpType"],
              betDate: typeof info[i]["betDate"] === "undefined" ? "" : info[i]["betDate"],
              gameState: info[i]["gameState"],
              ggId: info[i]["GGId"],
              groupNameE: group.length > 0 ? group[0]["NameE"] : "",
              groupNameG: group.length > 0 ? group[0]["NameG"] : "",
              groupNameC: group.length > 0 ? group[0]["NameC"] : "",
              jpGold: info[i]["JPGold"],
              extraTriggerType: info[i]["extraTriggerType"], // 判斷isFreeGame狀態
            }
            logs.push(tmp)
          }
          gameDao.getGameName_byId(
            {
              gameId: gameId,
            },
            cb
          ) //取遊戲名稱
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.HISTORY_LOAD_FAIL,
              data: null,
            })
            return
          }
          for (let i in logs) {
            var gameInfo = r_data.filter((item) => item.GameId == logs[i]["gameId"])
            logs[i]["nameE"] = gameInfo.length > 0 ? gameInfo[0]["NameE"] : ""
            logs[i]["nameG"] = gameInfo.length > 0 ? gameInfo[0]["NameG"] : ""
            logs[i]["nameC"] = gameInfo.length > 0 ? gameInfo[0]["NameC"] : ""
          }

          // 登入者 交收幣別 與 主幣別 相同時,不須找匯率 & 轉換匯率
          if (data["opCurrency"] === data["mainCurrency"]) {
            cb(null, { code: code.OK }, null)
            return
          }
          var param = {
            currency: data["opCurrency"],
            end_date: data["end_date"],
          }
          console.log("currencyExchangeRate param:", JSON.stringify(param))
          gameDao.currencyExchangeRate(param, cb) // 取得登入者幣別匯率
        },
      ],
      function (none, r_code, r_exchangeRate) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.RATE_NOT_VALID,
            data: null,
          })
          return
        }
        if (r_exchangeRate) {
          if (r_exchangeRate.length > 0) r_exchangeRate = r_exchangeRate[0].CryDef // 匯率
          else {
            // DB 找不到該交收幣別幣別的匯率 (有鬼)
            r_exchangeRate = 0
            logger.error("can not find currency exchangeRate !! currency is : ", data["currency"])
          }
        } else {
          // 不須轉換匯率
          r_exchangeRate = 1
        }

        const convertCurrency = (value) => BN(value).times(r_exchangeRate).dp(4).toNumber()

        // 加總轉換成交收者幣別
        const resultSumIno = sum.map((x) => {
          const betGold = convertCurrency(x.converBetGold)
          const realBetGold = convertCurrency(x.converRealBetGold)
          const jpRealBetGold = convertCurrency(x.converJpRealBetGold)
          const jpConGoldOriginal = convertCurrency(x.converJPConGoldOriginal)
          const winGold = convertCurrency(x.converWinGold)
          const payoutAmount = convertCurrency(x.converPayoutAmount)
          const netWin = convertCurrency(x.converNetWin)
          const jpGold = convertCurrency(x.converJPGold)

          return {
            betGold: betGold,
            realBetGold: realBetGold,
            jpRealBetGold: jpRealBetGold,
            jpConGoldOriginal: jpConGoldOriginal,
            jpGold: jpGold,
            winGold: winGold,
            payoutAmount: payoutAmount,
            netWin: netWin,
            rtp: BN(winGold).div(realBetGold).times(100).toNumber(),
            rtpNoJP: BN(payoutAmount).div(realBetGold).times(100).toNumber(),
            winRate: x.winRate,
          }
        })

        var data = {}
        if (msg.data.isPage == false) {
          //無分頁

          var finish = ttlCount_bets == msg.data["index"] + logs.length ? 1 : 0
          var index = finish == 1 ? null : msg.data["index"] + logs.length

          data = {
            totalCount: ttlCount_bets, //總筆數
            finish: finish,
            index: index,
            logs: logs,
            sum: resultSumIno,
            sortKey: sortKey,
            sortType: sortType,
          }
        } else {
          //有分頁

          var ttlCount = ttlCount_bets
          var pageCount = msg.data.pageCount
          var pageCur = msg.data.page
          data = {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            logs: logs,
            sum: resultSumIno,
            sortKey: sortKey,
            sortType: sortType,
          }
        }
        next(null, {
          code: code.OK,
          data: data,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getList_BetHistory_v3] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.bettingRecordPageInit = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};
    var currencies = []
    var re_data = {}
    const level = session.get("level")

    let userId = ""

    m_async.waterfall(
      [
        function (cb) {
          msg.data["backendSystem"] = true
          self.app.rpc.game.gameRemote.getUserJoinGames(session, msg.data, cb) //遊戲名稱
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.GAME_NAME_LOAD_FAIL, data: null })
            return
          }
          re_data["games"] = r_data

          switch (level) {
            case 1: //admin - 全部
              //self.app.rpc.config.configRemote.getCurrenies(userSession, cb); //幣別
              configDao.getCurrencyList(cb) //幣別
              break
            case 2: //hall - 下線(hall 資料表的currencies)
              userId = session.get("cid")
              if (session.get("isSub") == 0) {
                userDao.getHallCurrency_byCid(userId, cb)
              } else {
                //Hall Manager
                const data = {
                  cid: userId,
                }
                userDao.get_subUser_byCid(data, cb)
              }
              break
            case 3: //agent - 自己
              userId = session.get("cid")

              if (session.get("isSub") == 0) {
                //Agent 查詢 Wallet
                gameDao.getWallet({ cid: userId }, cb)
              } else {
                //Agent Manager
                userDao.get_subUser_byCid({ cid: userId }, cb)
              }
              break
          }
        },
        function (r_code, r_currencies, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.CURRENCY_LOAD_FAIL, data: null })
            return
          }
          switch (level) {
            case 1:
              currencies = r_currencies.map((x) => x.currency)
              break
            case 2:
              if (r_currencies != "" && session.get("isSub") == 0) {
                currencies = r_currencies.split(",")
              } else {
                const [target] = r_currencies
                currencies = target.Currencies.split(",")
              }
              break
            case 3:
              if (session.get("isSub") == 0) {
                r_currencies.forEach((item) => {
                  if (item.Currency != null && item.Currency != "") currencies.push(item.Currency)
                })
              } else {
                const [target] = r_currencies
                currencies = target.Currencies.split(",")
              }
              break
          }
          self.app.rpc.config.configRemote.getGameGroup(session, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.CURRENCY_LOAD_FAIL, data: null })
          return
        }

        re_data["currencies"] = currencies
        re_data["game_group"] = r_data

        next(null, {
          code: code.OK,
          data: re_data,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][bettingRecordPageInit] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.transactionPageInit = function (msg, session, next) {
  try {
    var self = this
    if (typeof msg.data === "undefined") {
      next(null, { code: code.BET.PARA_DATA_FAIL })
      return
    }
    if (typeof msg.data.mode === "undefined") {
      msg.data["mode"] = 2 //多錢包
    }

    var re = {}
    //var userSession = {};
    const level = session.get("level")

    let userId = ""

    m_async.waterfall(
      [
        function (cb) {
          msg.data["level"] = conf.USER_LEVEL[session.get("level")]

          if (msg.data["mode"] == 1) {
            //單錢包
            self.app.rpc.game.gameRemote.getUserJoinGames(session, msg.data, cb) //遊戲名稱
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.GAME_NAME_LOAD_FAIL, data: null })
            return
          }

          if (msg.data["mode"] == 1) re["games"] = r_data

          switch (level) {
            case 1: //admin - 全部
              //self.app.rpc.config.configRemote.getCurrenies(userSession, cb); //幣別
              configDao.getCurrencyList(cb) //幣別
              break
            case 2: //hall - 下線(hall 資料表的currencies)
              userId = session.get("cid")
              if (session.get("isSub") == 0) {
                userDao.getHallCurrency_byCid(userId, cb)
              } else {
                //Hall Manager
                const data = {
                  cid: userId,
                }
                userDao.get_subUser_byCid(data, cb)
              }
              break
            case 3: //agent - 自己
              userId = session.get("cid")

              if (session.get("isSub") == 0) {
                //Agent 查詢 Wallet
                gameDao.getWallet({ cid: userId }, cb)
              } else {
                //Agent Manager
                userDao.get_subUser_byCid({ cid: userId }, cb)
              }
              break
          }
        },
      ],
      function (none, r_code, r_currencies) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.CURRENCY_LOAD_FAIL, data: null })
          return
        }
        var currencies = []

        if (r_code.code != code.OK) {
          next(null, { code: code.BET.LOAD_CURRENCY_FAIL, data: null })
          return
        }

        switch (level) {
          case 1:
            currencies = r_currencies.map((x) => x.currency)
            break
          case 2:
            if (r_currencies != "" && session.get("isSub") == 0) {
              currencies = r_currencies.split(",")
            } else {
              const [target] = r_currencies
              currencies = target.Currencies.split(",")
            }
            break
          case 3:
            if (session.get("isSub") == 0) {
              r_currencies.forEach((item) => {
                if (item.Currency != null && item.Currency != "") currencies.push(item.Currency)
              })
            } else {
              const [target] = r_currencies
              currencies = target.Currencies.split(",")
            }
            break
        }
        re["currencies"] = currencies

        next(null, {
          code: code.OK,
          data: re,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][transactionPageInit] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//交易紀錄
handler.getTransactionRecord = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.start_date === "undefined" ||
      typeof msg.data.end_date === "undefined" ||
      typeof msg.data.level === "undefined" ||
      typeof msg.data.upid === "undefined" ||
      typeof msg.data.mode === "undefined" ||
      typeof msg.data.page === "undefined" ||
      typeof msg.data.pageCount === "undefined" ||
      typeof msg.data.page != "number" ||
      typeof msg.data.pageCount != "number"
    ) {
      next(null, { code: code.BET.PARA_DATA_FAIL })
      return
    }
    if (typeof msg.data.isPage == "undefined") {
      msg.data["isPage"] = true //預設-有分頁
    }

    //無分頁
    if (msg.data["isPage"] == false) {
      if (typeof msg.data.finish == "undefined") {
        msg.data["finish"] = 0
      } //0:未完成
      if (msg.data.index == null) {
        msg.data["index"] = 0
      } //開始第N筆
      msg.data["pageCount"] = 1000 //每次取得筆數
    }

    m_async.waterfall(
      [
        function (cb) {
          if (msg.data.mode == 1) {
            //單錢包 mode:1
            get_transaction_record(msg.data, cb)
          } else {
            //多錢包 mode:2
            get_transfer_record(msg.data, cb)
          }
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.BET.TX_LOAD_FAIL, data: null })
          return
        }

        var ttlCount = r_data["count"]
        var pageCount = msg.data.pageCount
        var pageCur = msg.data.page

        var data = {}
        if (msg.data.isPage == false) {
          //無分頁
          var finish = ttlCount == msg.data["index"] + r_data["logs"].length ? 1 : 0
          var index = finish == 1 ? null : msg.data["index"] + r_data["logs"].length
          data = {
            counts: ttlCount, //總筆數
            finish: finish,
            index: index,
            logs: r_data["logs"],
            sortKey: r_data["sortKey"],
            sortType: r_data["sortType"],
          }
        } else {
          data = {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            logs: r_data["logs"],
            sortKey: r_data["sortKey"],
            sortType: r_data["sortType"],
          }
        }
        next(null, {
          code: code.OK,
          data: data,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getTransactionRecord] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 彩金報表頁面 各層級的彩金總和
handler.getJackpotSum = function (msg, session, next) {
  if (isEmpty(msg.data) || isEmpty(msg.data.start_date) || isEmpty(msg.data.end_date) || isEmpty(msg.data.level)) {
    next(null, {
      code: code.DB.PARA_FAIL,
    })
    return
  }

  const data = { ...msg.data, isFilterUsername: false }
  let hallList = null
  //操作者層級
  if ((data.userLevel === "HA" || data.userLevel === "AG") && isEmpty(data.cid)) {
    next(null, {
      code: code.DB.PARA_FAIL,
    })
    return
  }

  /**
   *    處理預設值
   */
  if (isEmpty(data.isPage)) {
    // 預設-有分頁
    Object.assign(data, { isPage: true })
  }

  if (data.isPage === true) {
    if (isEmpty(data.page) || isEmpty(data.pageCount)) {
      next(null, {
        code: code.DB.PARA_FAIL,
      })
      return
    }
  } else {
    if (isEmpty(data.finish)) {
      // 0:未完成
      Object.assign(data, { finish: 0 })
    }

    if (isEmpty(data.index)) {
      // 從第一筆開始
      Object.assign(data, { index: 0 })
    }

    data.pageCount = 1000 //每次取得筆數
  }

  //時間轉換
  data.start_date = timezone.UTCToLocal(data.start_date)
  data.end_date = timezone.UTCToLocal(data.end_date)
  let tempResult = []
  let search_game = false
  const level = session.get("level")
  m_async.waterfall(
    [
      function (cb) {
        //彩金報表新增遊戲名稱搜尋
        if (typeof data.gameName != "undefined" && data.gameName != "") {
          //遊戲名搜尋
          search_game = true
          let user_level = ""
          switch (level) {
            case 2:
              if (session.get("isSub") == 0) {
                user_level = session.get("cid")
              } else {
                user_level = session.get("hallId")
              }
              break
            case 3:
              user_level = session.get("hallId")
              break
          }
          var param = {
            haId: user_level,
            level: data.user_level, //(登入者身分-ad:1,ha/sha:2,ag:3)
            gameName: data.gameName,
          }
          gameDao.getGameIdbyGameName(param, cb) //彩金報表透過遊戲名稱搜尋遊戲編號
        } else {
          cb(
            null,
            {
              code: code.OK,
            },
            null
          )
        }
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.REVENUE_LOAD_FAIL,
              data: null,
            })
            return
          }
        }
        data["search_game"] = search_game
        if (search_game == true) {
          //有使用遊戲搜尋
          data["search_gameId"] = r_data.map((item) => item.GameId)
        }
        if (data.userLevel === "HA") {
          userDao.getChildList(data, cb)
        } else {
          cb(null, { code: code.OK }, [])
        }
      },
      function (r_code, r_result, cb) {
        if (r_code.code !== code.OK) {
          hallList = null
          next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
          return
        } else {
          if (data.userLevel === "HA") {
            // subHall Id列表
            const [childList] = Object.values(r_result[0])
            hallList = childList.split(",")
          }
        }

        if (data.userName) {
          data.isFilterUsername = true
          bettingDao.getUsersByUsername(data, cb)
        } else {
          cb(null, { code: code.OK }, [])
        }
      },
      function (r_code, r_result, cb) {
        if (r_code.code !== code.OK) {
          next(null, {
            code: r_code.code,
            data: r_code.msg,
          })
          return
        }
        let payload = { ...data, hallList }
        if (data.isFilterUsername) {
          const validUserList = r_result.map((x) => x.Cid)
          payload = { ...data, validUserList, hallList }
        }

        bettingDao.getList_JackpotSum(payload, cb)
      },
      function (r_code, r_result, cb) {
        if (r_code.code !== code.OK) {
          next(null, {
            code: r_code.code,
            data: r_code.msg,
          })
          return
        }

        if (r_result.info.length === 0) {
          next(null, { code: code.DB.DATA_EMPTY })
          return
        }

        tempResult = Object.assign({}, r_result)

        const initData = { userList: [], gameList: [] }

        const payload = tempResult.info.reduce((acc, cur) => {
          if (isEmpty(cur.Cid) === false) {
            acc.userList.push(cur.Cid)
            acc.userList.push(cur.HallId)
            acc.userList.push(cur.UpId)
          }

          if (isEmpty(cur.GameId) === false) {
            acc.gameList.push(cur.GameId)
          }
          return acc
        }, initData)

        // 消除重複ID
        payload.userList = [...new Set(payload.userList)]

        bettingDao.getUserAndGameName(payload, cb)
      },
      function (r_code, r_result, cb) {
        if (r_code.code !== code.OK) {
          next(null, {
            code: r_code.code,
            data: r_code.msg,
          })
          return
        }

        if (r_result.length === 0) {
          next(null, { code: code.DB.DATA_EMPTY, data: "User or Game is not found!" })
          return
        }

        // Mapping meta data to result

        // 列表不會有遊戲名稱只有帳號名稱
        const userMapping = new Map(r_result[0].map((x) => [x.Cid, x.Username]))

        const info = tempResult.info.map((x) => {
          const hallUsername = userMapping.get(x.HallId)
          const upperUsername = userMapping.get(x.UpId)
          const username = userMapping.get(x.Cid)

          return { ...x, username, hallUsername, upperUsername }
        })

        tempResult.info = info

        cb(null, { code: 200 }, tempResult)
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code !== code.OK) {
        next(null, {
          code: code.BET.JP_LOAD_FAIL,
          data: null,
        })
        return
      }

      const resultTotalCounts = r_data.count
      const jpDataList = r_data.info
      const currentCounts = data.index + jpDataList.length
      let result = {}

      if (data.isPage == false) {
        //無分頁
        const finish = resultTotalCounts === currentCounts ? 1 : 0
        const index = finish === 1 ? null : currentCounts

        result = {
          totalCount: resultTotalCounts,
          finish,
          index,
          logs: jpDataList,
        }
      } else {
        //有分頁
        result = {
          counts: resultTotalCounts,
          pages: Math.ceil(resultTotalCounts / data.pageCount),
          page_cur: data.page,
          page_count: data.pageCount,
          logs: jpDataList,
        }
      }

      next(null, {
        code: code.OK,
        data: result,
      })
      return
    }
  )
}

// 彩金報表頁面 玩家的彩金詳細列表
handler.getJackpotDetail = function (msg, session, next) {
  if (
    isEmpty(msg.data) ||
    isEmpty(msg.data.start_date) ||
    isEmpty(msg.data.end_date) ||
    isEmpty(msg.data.playerId) ||
    isEmpty(msg.data.page) ||
    isEmpty(msg.data.pageCount)
  ) {
    next(null, { code: code.DB.PARA_FAIL })
    return
  }
  //彩金報表新增排序功能
  var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "addDate"
  var sortType =
    typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
      ? msg.data.sortType
      : 0
  const data = { ...msg.data }

  //操作者層級
  if ((data.userLevel === "HA" || data.userLevel === "AG") && isEmpty(data.cid)) {
    next(null, {
      code: code.DB.PARA_FAIL,
    })
    return
  }

  //時間轉換
  msg.data.start_date = timezone.UTCToLocal(msg.data.start_date)
  msg.data.end_date = timezone.UTCToLocal(msg.data.end_date)

  let ttlCount_bets = 0
  let tempResult = []
  let search_game = false
  const level = session.get("level")
  m_async.waterfall(
    [
      function (cb) {
        //彩金報表新增遊戲名稱搜尋
        if (typeof data.gameName != "undefined" && data.gameName != "") {
          //遊戲名搜尋
          search_game = true
          let user_level = ""
          switch (level) {
            case 2:
              if (session.get("isSub") == 0) {
                user_level = session.get("cid")
              } else {
                user_level = session.get("hallId")
              }
              break
            case 3:
              user_level = session.get("hallId")
              break
          }
          var param = {
            haId: user_level,
            level: data.user_level, //(登入者身分-ad:1,ha/sha:2,ag:3)
            gameName: data.gameName,
          }
          gameDao.getGameIdbyGameName(param, cb) //彩金報表透過遊戲名稱搜尋遊戲編號
        } else {
          cb(
            null,
            {
              code: code.OK,
            },
            null
          )
        }
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.REVENUE_LOAD_FAIL,
              data: null,
            })
            return
          }
        }
        data["search_game"] = search_game
        if (search_game == true) {
          //有使用遊戲搜尋
          data["search_gameId"] = r_data.map((item) => item.GameId)
        }
        bettingDao.getList_JackpotDetail(data, cb) //清單
      },
      function (r_code, r_result, cb) {
        if (r_code.code !== code.OK) {
          next(null, {
            code: r_code.code,
            data: r_code.msg,
          })
          return
        }

        tempResult = Object.assign({}, r_result)
        const initData = { userList: [], gameList: [] }

        const payload = tempResult.info.reduce((acc, cur) => {
          if (isEmpty(cur.Cid) === false) {
            acc.userList.push(cur.Cid)
            acc.userList.push(cur.HallId)
            acc.userList.push(cur.UpId)
          }

          if (isEmpty(cur.GameId) === false) {
            acc.gameList.push(cur.GameId)
          }

          return acc
        }, initData)

        // 消除重複ID
        payload.userList = [...new Set(payload.userList)]

        bettingDao.getUserAndGameName(payload, cb)
      },
      function (r_code, r_result, cb) {
        if (r_code.code !== code.OK) {
          next(null, {
            code: r_code.code,
            data: r_code.msg,
          })
          return
        }

        if (r_result.length === 0) {
          cb(null, { code: 200 }, { count: 0, info: [] })
          return
        }

        // Mapping meta data to result
        const userMapping = new Map(r_result[0].map((x) => [x.Cid, x.Username]))
        const gameMapping = new Map(
          r_result[1].map((x) => [x.GameId, { names: { cn: x.NameC, tw: x.NameG, en: x.NameE }, ggId: x.GGId }])
        )

        const info = tempResult.info.map((x) => {
          const hallUsername = userMapping.get(x.HallId)
          const upperUsername = userMapping.get(x.UpId)
          const username = userMapping.get(x.Cid)
          const gameMeta = gameMapping.get(x.GameId)

          return { ...x, username, hallUsername, upperUsername, gameNames: gameMeta.names, ggId: gameMeta.ggId }
        })

        tempResult.info = info

        cb(null, { code: 200 }, tempResult)
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code != code.OK) {
        next(null, { code: code.BET.JP_LOAD_FAIL, data: null })
        return
      }

      ttlCount_bets = r_data.count

      r_data.info = r_data.info.map((x) => {
        const addDateUTC = timezone.LocalToUTC(x.addDate)
        return Object.assign({}, x, { addDate: addDateUTC })
      })

      next(null, {
        code: code.OK,
        data: {
          counts: ttlCount_bets,
          pages: Math.ceil(ttlCount_bets / data.pageCount),
          page_cur: data.page,
          page_count: data.pageCount,
          logs: r_data.info,
          sortKey: sortKey,
          sortType: sortType,
        },
      })
      return
    }
  )
}

handler.jackpotPageInit = function (msg, session, next) {
  let games = []
  let currencies = []
  m_async.waterfall(
    [
      function (cb) {
        gameDao.getUserJoinGames(msg.data, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, { code: code.BET.GAME_LOAD_FAIL, data: null })
          return
        }

        games = [...r_data]
        if (msg.data.level == "AD") {
          configDao.getCurrencyList(cb)
        } else if (msg.data.isSub == 0) {
          userDao.getWallets(msg.data.cid, cb)
        } else {
          userDao.get_subUser_byCid(msg.data, cb)
        }
      },
    ],
    function (cb, r_code, r_currencies) {
      if (msg.data.level == "AD") {
        currencies = r_currencies.map((x) => x.currency)
      } else if (msg.data.isSub == 0) {
        currencies = r_currencies.map((x) => x.Currency)
      } else {
        const [target] = r_currencies
        currencies = target.Currencies.split(",")
      }
      const result = { games: games, currencies: currencies }
      next(null, { code: code.OK, data: result })
      return
    }
  )
}

/**
 * 統計報表 -> 玩家報表
 *
 * @param {object} msg
 * @param {object} session
 * @param {callback} next
 * @returns
 */
handler.getTotalPlayersWin = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.start_date === "undefined" ||
      typeof msg.data.end_date === "undefined" ||
      typeof msg.data.page === "undefined" ||
      typeof msg.data.pageCount === "undefined" ||
      typeof msg.data.page != "number" ||
      typeof msg.data.pageCount != "number"
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
      })
      return
    }
    if (typeof msg.data.isPage === "undefined") {
      msg.data["isPage"] = true //預設-有分頁
    }

    if (msg.data.isPage == true) {
      //有分頁
      if (
        typeof msg.data.page === "undefined" ||
        typeof msg.data.pageCount === "undefined" ||
        typeof msg.data.page != "number" ||
        typeof msg.data.pageCount != "number"
      ) {
        next(null, {
          code: code.BET.PARA_DATA_FAIL,
        })
        return
      }
    }

    //無分頁
    if (msg.data["isPage"] == false) {
      if (typeof msg.data.finish === "undefined") {
        msg.data["finish"] = 0
      } //0:未完成
      if (typeof msg.data.index === "undefined") {
        msg.data["index"] = 0
      } //開始第N筆
      msg.data["pageCount"] = 1000 //每次取得筆數
    }

    // 有檢查upId但沒用到
    //操作者層級
    if (msg.data.userLevel == "HA" && (typeof msg.data.upId === "undefined" || msg.data.upId == "")) {
      next(null, {
        code: code.DB.PARA_FAIL,
      })
      return
    }
    if (msg.data.userLevel == "AG" && (typeof msg.data.upId === "undefined" || msg.data.upId == "")) {
      next(null, {
        code: code.DB.PARA_FAIL,
      })
      return
    }

    const data = msg.data
    let ttlCount = 0
    let info = {}
    let search_game = false
    const level = session.get("level")
    let userId
    m_async.waterfall(
      [
        function (cb) {
          if (typeof data.gameName != "undefined" && data.gameName != "") {
            //遊戲名搜尋
            search_game = true
            let user_level = ""
            switch (level) {
              case 2:
                if (session.get("isSub") == 0) {
                  user_level = session.get("cid")
                } else {
                  user_level = session.get("hallId")
                }
                break
              case 3:
                user_level = session.get("hallId")
                break
            }
            const param = {
              haId: user_level,
              level: data.user_level, //(登入者身分-ad:1,ha/sha:2,ag:3)
              gameName: data.gameName,
            }
            gameDao.getGameIdbyGameName(param, cb) //玩家報表透過遊戲名稱搜尋遊戲編號
          } else {
            cb(
              null,
              {
                code: code.OK,
              },
              null
            )
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            if (r_code.code != code.OK) {
              next(null, {
                code: code.BET.REVENUE_LOAD_FAIL,
                data: null,
              })
              return
            }
          }
          data["search_game"] = search_game
          if (search_game == true) {
            //有使用遊戲搜尋
            data["search_gameId"] = r_data.map((item) => item.GameId)
          }
          switch (session.get("level")) {
            case 1: //admin - 全部
              configDao.getCurrencyList(cb) //幣別
              break
            case 2: //hall - 下線(hall 資料表的currencies)
              userId = session.get("cid")
              if (session.get("isSub") == 0) {
                userDao.getHallCurrency_byCid(userId, cb)
              } else {
                //Hall Manager
                const data = {
                  cid: userId,
                }
                userDao.get_subUser_byCid(data, cb)
              }
              break
            case 3: //agent - 自己
              userId = session.get("cid")

              if (session.get("isSub") == 0) {
                //Agent 查詢 Wallet
                gameDao.getWallet({ cid: userId }, cb)
              } else {
                //Agent Manager
                userDao.get_subUser_byCid({ cid: userId }, cb)
              }
              break
          }
        },
        function (r_code, r_currencies, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.LOAD_CURRENCY_FAIL,
              data: null,
            })
            return
          }

          switch (session.get("level")) {
            case 1:
              data["betCurrency"] = r_currencies.map((x) => x.currency)
              break
            case 2:
              if (r_currencies != "" && session.get("isSub") == 0) {
                data["betCurrency"] = r_currencies.split(",")
              } else {
                const [target] = r_currencies
                data["betCurrency"] = target.Currencies.split(",")
              }
              break
            case 3:
              if (session.get("isSub") == 0) {
                let betCurrency = []
                r_currencies.forEach((item) => {
                  if (item.Currency != null && item.Currency != "") {
                    betCurrency.push(item.Currency)
                  }
                })
                data["betCurrency"] = betCurrency
              } else {
                const [target] = r_currencies
                data["betCurrency"] = target.Currencies.split(",")
              }
              break
          }
          data["user_level"] = session.get("level") //使用者層級
          if (data["user_level"] == 2) {
            //hall
            userId = session.get("isSub") == 1 ? session.get("hallId") : session.get("cid")
          } else {
            //agent
            userId = session.get("isSub") == 1 ? session.get("agentId") : session.get("cid")
          }
          //hall id OR agent id
          switch (data.user_level) {
            case 2:
              userDao.getChildList({ cid: userId }, cb)
              break
            default:
              cb(null, { code: code.OK }, null)
              break
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.LOAD_USER_FAIL, data: null })
            return
          }
          let downLineCid = []
          if (r_data) {
            let downLine = r_data.pop()
            downLineCid = downLine[Object.keys(downLine)[0]].split(",")
            downLineCid.splice(0, 1) // 刪除第0 & 1筆: '$,rUpjbUmzFkAV(自己),7ejdZ2jNAZvQ, ...'
          }

          data["upId"] = data.user_level == 2 ? downLineCid : [userId]
          data["sel_table"] = "game_revenue_player"
          data["tableUpIdName"] = data["user_level"] == 2 ? "hallId" : "UpId"
          data["select"] = ["cid", "username", "Currency", "rounds"]
          data["outGroupBy"] = ["id", "Currency"]
          data["innerGroupBy"] = ["w.Cid", "Currency"]

          bettingDao.get_Currency_Convert_Revenue(data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.BET.PLAYERS_WIN_LOAD_FAIL,
            data: null,
          })
          return
        }

        ttlCount = r_data.count //總筆數
        let data = {}
        let consolidatedData = {
          //帶入的資料
          downCurrency: msg.data.downCurrency, //判斷是否是合併幣別
          info: r_data["info"], //原始未合併幣別資料
          order: r_data["order"], //取出的排序資料
          level: "PlayerReport", //層級
          mergeKeys: ["id", "Currency"],
          showOriginCurrency: true,
          loginCurrency: msg.data["currency"],
        }
        const logs = handler.consolidated(consolidatedData)

        if (msg.data.isPage == false) {
          //無分頁

          const finish = ttlCount == data["index"] + logs.length ? 1 : 0
          const index = finish == 1 ? null : data["index"] + logs.length

          info = {
            dateRange: conf.dateRange,
            totalCount: ttlCount, //總筆數
            finish: finish,
            index: index,
            logs: logs,
          }
        } else {
          //有分頁

          const pageCount = msg.data.pageCount
          const pageCur = msg.data.page

          info = {
            dateRange: conf.dateRange,
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            logs: logs,
          }
        }
        next(null, {
          code: code.OK,
          data: info,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getTotalPlayersWin] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getTopGames_v2 = function (msg, session, next) {
  const logTag = "[betHandler][getTopGames_v2]"

  try {
    const self = this

    const userLevel = new UserLevelService({ session })

    const { isAdmin, isAgent, isHall, cid } = userLevel.getUserLevelData()

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.timeType === "undefined" ||
      typeof msg.data.hourDiff === "undefined"
    ) {
      next(null, { code: code.DB.PARA_FAIL })
      return
    }

    const hourDiff = msg.data.hourDiff

    // 先由 cache 取查詢結果
    const requestKey = "getTopGames_v2_" + msg.data.timeType

    const cidByCache = isAdmin ? "admin" : cid

    const revenueResult = self.app.localCache.getRevenueResult(requestKey, cidByCache, hourDiff)

    if (revenueResult) {
      next(null, {
        code: code.OK,
        data: revenueResult,
      })
      return
    }

    const queryParams = {
      startDate: "",
      endDate: "",
      cid: cid,
      targetTable: "",
    }

    const resultList = {
      topNetWin: [],
      topRounds: [],
    }

    if (isAdmin) {
      queryParams.cid = null
      queryParams.targetTable = "game_revenue_hall"
    } else if (isHall) {
      queryParams.targetTable = "game_revenue_hall"
    } else if (isAgent) {
      queryParams.targetTable = "game_revenue_agent"
    }

    m_async.waterfall(
      [
        function (cb) {
          const now = new Date()
          const hourDiff = msg.data.hourDiff
          const formatString = "YYYY-MM-DD HH:mm:ss"

          queryParams.endDate = timezone.getTimezoneDate(hourDiff, now).endOf("day").utc().format(formatString)

          switch (msg.data.timeType) {
            case "day":
              queryParams.startDate = timezone.getTimezoneDate(hourDiff, now).startOf("day").utc().format(formatString)
              break
            case "week":
              queryParams.startDate = timezone
                .getTimezoneDate(hourDiff, now)
                .subtract(7, "day")
                .startOf("day")
                .utc()
                .format(formatString)
              break
            case "month":
              queryParams.startDate = timezone
                .getTimezoneDate(hourDiff, now)
                .subtract(30, "day")
                .startOf("day")
                .utc()
                .format(formatString)
              break
          }

          bettingDao.getGamesSummaryData(queryParams, cb) //總收入
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.LOAD_TOP_GAMES_FAIL, data: null })
            return
          }

          // 將資料都轉成CNY，並合併同樣的遊戲+幣別
          const dataMap = r_data.reduce((acc, cur) => {
            const { gameId, rounds, exchangeRate, realBetGold, winGold, netWin, jpGold, payout, winLose } = cur

            // payout = win - jp
            // winLose = realBet - win
            // netWin = realBet - payout
            const convertPayout = BN(payout).div(exchangeRate).dp(2).toNumber()
            const convertWinLose = BN(winLose).div(exchangeRate).dp(2).toNumber()
            const convertNetWin = BN(netWin).div(exchangeRate).dp(2).toNumber()
            const convertRealBetGold = BN(realBetGold).div(exchangeRate).dp(2).toNumber()
            const convertWinGold = BN(winGold).div(exchangeRate).dp(2).toNumber()
            const convertJPGold = BN(jpGold).div(exchangeRate).dp(2).toNumber()

            const key = `${gameId}`

            const defaultData = {
              gameId,
              payout: 0,
              winLose: 0,
              netWin: 0,
              realBetGold: 0,
              winGold: 0,
              jpGold: 0,
              rounds: 0,
            }

            const target = acc.get(key) || defaultData

            target.payout += convertPayout
            target.winLose += convertWinLose
            target.netWin += convertNetWin
            target.realBetGold += convertRealBetGold
            target.winGold += convertWinGold
            target.jpGold += convertJPGold
            target.rounds += rounds

            acc.set(key, target)
            return acc
          }, new Map())

          const list = [...dataMap.values()]

          // 取得遊戲前10淨收益金額
          const topGameNetWinList = list.sort((a, b) => b.netWin - a.netWin).slice(0, 10)

          // 取得遊戲前10注單量
          const topGameRoundsList = list.sort((a, b) => b.rounds - a.rounds).slice(0, 10)

          resultList.topNetWin = [...topGameNetWinList]
          resultList.topRounds = [...topGameRoundsList]

          const gameIdSet = [...topGameNetWinList, ...topGameRoundsList].reduce((acc, cur) => {
            acc.add(cur.gameId)
            return acc
          }, new Set())

          // 取遊戲名稱
          gameDao.getGameName_byId({ gameId: [...gameIdSet.keys()] }, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_data.length > 0) {
          const gameNameMap = r_data.reduce((acc, cur) => {
            const { GameId, NameE, NameG, NameC } = cur
            acc.set(GameId, { nameE: NameE, nameG: NameG, nameC: NameC })
            return acc
          }, new Map())

          const handleGameName = (x) => {
            const gameNames = gameNameMap.get(x.gameId)
            return { ...x, ...gameNames }
          }

          resultList.topNetWin = resultList.topNetWin.map(handleGameName)
          resultList.topRounds = resultList.topRounds.map(handleGameName)
        }

        // 查詢結果存入 cache
        const revenueResult = {
          GGR: resultList.topNetWin,
          currency: "CNY",
          BetNum: resultList.topRounds,
        }

        self.app.localCache.addRevenueResult(requestKey, cidByCache, revenueResult, hourDiff)

        next(null, {
          code: code.OK,
          data: revenueResult,
        })

        return
      }
    )
  } catch (err) {
    logger.error(`${logTag} ${inspect(err)}`)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getTopPlayers = function (msg, session, next) {
  const logTag = "[betHandler][getTopPlayers]"

  try {
    const self = this

    const userLevel = new UserLevelService({ session })

    const { isAdmin, isAgent, isHall, cid } = userLevel.getUserLevelData()

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.timeType === "undefined" ||
      typeof msg.data.hourDiff === "undefined"
    ) {
      next(null, { code: code.DB.PARA_FAIL })
      return
    }

    const hourDiff = msg.data.hourDiff

    // 先由 cache 取查詢結果
    const requestKey = "getTopPlayers_" + msg.data.timeType

    const cidByCache = isAdmin ? "admin" : cid

    const revenueResult = self.app.localCache.getRevenueResult(requestKey, cidByCache, hourDiff)

    if (revenueResult) {
      next(null, {
        code: code.OK,
        data: revenueResult,
      })
      return
    }

    const queryParams = {
      startDate: "",
      endDate: "",
      hallId: isHall ? cid : "",
      agentId: isAgent ? cid : "",
    }

    const resultList = {
      topWinPlayers: [],
      topLosePlayers: [],
    }

    m_async.waterfall(
      [
        function (cb) {
          const now = new Date()
          const hourDiff = msg.data.hourDiff
          const formatString = "YYYY-MM-DD HH:mm:ss"

          queryParams.endDate = timezone.getTimezoneDate(hourDiff, now).endOf("day").utc().format(formatString)

          switch (msg.data.timeType) {
            case "day":
              queryParams.startDate = timezone.getTimezoneDate(hourDiff, now).startOf("day").utc().format(formatString)
              break
            case "week":
              queryParams.startDate = timezone
                .getTimezoneDate(hourDiff, now)
                .subtract(7, "day")
                .startOf("day")
                .utc()
                .format(formatString)
              break
            case "month":
              queryParams.startDate = timezone
                .getTimezoneDate(hourDiff, now)
                .subtract(30, "day")
                .startOf("day")
                .utc()
                .format(formatString)
              break
          }

          bettingDao.getPlayerRevenueData(queryParams, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.LOAD_TOP_PLAYER_FAIL, data: null })
            return
          }

          // 將資料都轉成CNY，並合併同樣的玩家+幣別
          const dataMap = r_data.reduce((acc, cur) => {
            const { playerId, rounds, exchangeRate, realBetGold, winGold, netWin, jpGold, payout, winLose } = cur

            // payout = win - jp
            // winLose = realBet - win
            // netWin = realBet - payout
            const convertPayout = BN(payout).div(exchangeRate).dp(2).toNumber()
            const convertWinLose = BN(winLose).div(exchangeRate).dp(2).toNumber()
            const convertNetWin = BN(netWin).div(exchangeRate).dp(2).toNumber()
            const convertRealBetGold = BN(realBetGold).div(exchangeRate).dp(2).toNumber()
            const convertWinGold = BN(winGold).div(exchangeRate).dp(2).toNumber()
            const convertJPGold = BN(jpGold).div(exchangeRate).dp(2).toNumber()

            const key = `${playerId}`

            const defaultData = {
              playerId,
              payout: 0,
              winLose: 0,
              netWin: 0,
              realBetGold: 0,
              winGold: 0,
              jpGold: 0,
              rounds: 0,
            }

            const target = acc.get(key) || defaultData

            target.payout += convertPayout
            target.winLose += convertWinLose
            target.netWin += convertNetWin
            target.realBetGold += convertRealBetGold
            target.winGold += convertWinGold
            target.jpGold += convertJPGold
            target.rounds += rounds

            acc.set(key, target)
            return acc
          }, new Map())

          const list = [...dataMap.values()]

          // 取得玩家贏錢前10金額 winLose為負表示玩家贏越多
          const topPlayerWinList = list.sort((a, b) => a.winLose - b.winLose).slice(0, 10)

          // 取得玩家輸錢前10金額 winLose為正表示玩家輸越多
          const topPlayerLoseList = list.sort((a, b) => b.winLose - a.winLose).slice(0, 10)

          resultList.topWinPlayers = [...topPlayerWinList]
          resultList.topLosePlayers = [...topPlayerLoseList]

          const playerIdSet = [...topPlayerWinList, ...topPlayerLoseList].reduce((acc, cur) => {
            acc.add(cur.playerId)
            return acc
          }, new Set())

          //取玩家名稱
          userDao.getUser_byId({ level: 4, userId: [...playerIdSet.keys()] }, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.BET.LOAD_TOP_PLAYER_FAIL, data: null })
          return
        }

        if (r_data.length > 0) {
          const playerNameMap = r_data.reduce((acc, cur) => {
            const { Cid, UserName, NickName } = cur
            acc.set(Cid, { username: UserName, nickname: NickName })
            return acc
          }, new Map())

          const handlePlayerName = (x) => {
            const playerData = playerNameMap.get(x.playerId)
            return { ...x, ...playerData }
          }

          resultList.topWinPlayers = resultList.topWinPlayers.map(handlePlayerName)
          resultList.topLosePlayers = resultList.topLosePlayers.map(handlePlayerName)
        }

        // 查詢結果存入 cache
        const revenueResult = {
          win: resultList.topWinPlayers,
          currency: "CNY",
          lose: resultList.topLosePlayers,
        }

        self.app.localCache.addRevenueResult(requestKey, cidByCache, revenueResult, hourDiff)

        next(null, {
          code: code.OK,
          data: revenueResult,
        })
        return
      }
    )
  } catch (err) {
    logger.error(`${logTag} ${inspect(err)}`)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//每天-歷史營收
handler.getGameRevenue_byDate_v2 = function (msg, session, next) {
  try {
    const self = this

    const userLevel = new UserLevelService({ session })

    const { isAdmin, cid } = userLevel.getUserLevelData()

    var thisMonth_startDate = "" // 這個月 起始日期
    let thisMonthEnd = ""
    var lastMonth_startDate = "" // 上個月 起始日期
    var this_month_totalBet = {} // 當月營收
    var last_month_totalBet = {} // 上個月營收
    var data = {
      server_start_date: "",
      server_end_date: "",
      showType: "day", //每日計算
    }

    let isEveryDay = msg.data.isEveryDay

    const hourDiff = msg.data.hourDiff

    // 先由 cache 取查詢結果
    const requestKey = "getGameRevenue_byDate_v2"

    const cidByCache = isAdmin ? "admin" : cid

    const revenueResult = self.app.localCache.getRevenueResult(requestKey, cidByCache, hourDiff)

    if (revenueResult && isEveryDay != true) {
      // 非每日詳情圖表
      next(null, {
        code: code.OK,
        data: revenueResult,
      })
      return
    }

    const now = new Date()
    const formatString = "YYYY-MM-DD HH:mm:ss"

    m_async.waterfall(
      [
        function (cb) {
          //取系統主幣別
          var param = { item: "main_currency" }
          configDao.getSystemSetting(param, cb)
        },
        function (r_code, r_data, cb) {
          data["mainCurrency"] = r_data //主幣別
          data["isEveryDay"] = isEveryDay
          data["everyDayCurrency"] = msg.data.currency
          data["user_level"] = isEveryDay ? msg.data.level : session.get("level")
          data["user_cid"] = isEveryDay ? msg.data.cId : cid
          data["user_isSub"] = session.get("level") == 1 || isEveryDay ? 0 : session.get("isSub")

          switch (data["user_level"]) {
            case 1:
            case 2: // data['sel_table'] = "user_revenue_agent";
              data["sel_table"] = "user_revenue_hall"
              break
            case 3:
              data["sel_table"] = "user_revenue_agent"
              break
          }

          //取交收者幣別 首頁儀錶板 || Admin 登入每日圖表
          if ((!isEveryDay && data["user_level"] == 1) || (isEveryDay && session.get("level") == 1)) {
            let currency =
              isEveryDay && data["everyDayCurrency"] != "Consolidated" ? data["everyDayCurrency"] : data["mainCurrency"]
            cb(null, { code: code.OK }, currency)
          } else {
            var userId = 0
            if (session.get("level") == 2) {
              //hall
              userId = session.get("isSub") == 1 ? session.get("hallId") : session.get("cid")
              data["user_hallId"] = userId
            } else {
              //agent
              userId = session.get("cid")
              data["user_hallId"] = session.get("hallId")
            }
            userDao.getCusCurrency_byCid(userId, cb)
          }
        },
        function (r_code, r_currency, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.LOAD_CURRENCY_FAIL, data: null })
            return
          }
          data["currency"] = r_currency
          data["hourDiffFormated"] = timezone.formatHourDiff(hourDiff)

          const todayStart = timezone.getTimezoneDate(hourDiff, now).startOf("day").utc().format(formatString)
          const todayEnd = timezone.getTimezoneDate(hourDiff, now).endOf("day").utc().format(formatString)

          data.start_date = todayStart
          data.end_date = todayEnd

          // 該月份「初始」日期&時間
          thisMonth_startDate = isEveryDay
            ? msg.data.start
            : timezone.getTimezoneDate(hourDiff, now).startOf("month").utc().format(formatString)

          // 該月份「最後」日期&時間
          thisMonthEnd = isEveryDay
            ? msg.data.end
            : timezone.getTimezoneDate(hourDiff, now).endOf("month").utc().format(formatString)

          data["server_start_date"] = thisMonth_startDate
          data["server_end_date"] = thisMonthEnd

          logger.info("getGameTotalRevenue param for this month:", JSON.stringify(data))

          bettingDao.getGameTotalRevenue(data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code !== code.OK) {
            next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
            return
          }
          this_month_totalBet = r_data // 當月每日營收

          const targetEndDate = isEveryDay ? msg.data.end : now

          const lastMonthStart = timezone
            .getTimezoneDate(hourDiff, targetEndDate)
            .subtract(1, "month")
            .startOf("month")
            .utc()
            .format(formatString)

          const lastMonthEnd = timezone
            .getTimezoneDate(hourDiff, targetEndDate)
            .subtract(1, "month")
            .endOf("month")
            .utc()
            .format(formatString)

          lastMonth_startDate = lastMonthStart
          var lastMonth_EndDate = lastMonthEnd

          data["server_start_date"] = lastMonth_startDate
          data["server_end_date"] = lastMonth_EndDate

          logger.info("getGameTotalRevenue param for last month:", JSON.stringify(data))

          bettingDao.getGameTotalRevenue(data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code !== code.OK) {
          next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
          return
        }
        last_month_totalBet = r_data // 上個月每日營收

        var re_revenue = []

        var this_month_last_day = timezone.getDays(thisMonth_startDate.substr(0, 4), thisMonth_startDate.substr(5, 2)) // 這個月最後一天
        var last_month_last_day = timezone.getDays(lastMonth_startDate.substr(0, 4), lastMonth_startDate.substr(5, 2)) // 上個月最後一天

        var monthDays = this_month_last_day > last_month_last_day ? this_month_last_day : last_month_last_day // 最大天數
        if (isEveryDay) {
          let startDay = Number(msg.data["start"].substr(8, 2))
          let startMonth = Number(msg.data["start"].substr(5, 2))
          let endDay = Number(msg.data["end"].substr(8, 2))
          let endMonth = Number(msg.data["end"].substr(5, 2))
          monthDays = endMonth == startMonth ? endDay - startDay + 1 : this_month_last_day - startDay + endDay
        }
        // 當月總投注
        var this_month_total_betGold = 0
        // 上個月總投注
        var last_month_total_betGold = 0
        for (var i = 0; i < monthDays; i++) {
          // 當月營收
          var thisMonth_date = timezone.transTime(thisMonth_startDate, i, "days").substr(0, 10)
          var thisMonth_info = this_month_totalBet.filter((item) => item.client_local_date == thisMonth_date)

          // 上個月營收
          var lastMonth_date = timezone.transTime(lastMonth_startDate, i, "days").substr(0, 10)
          var lastMonth_info = last_month_totalBet.filter((item) => item.client_local_date == lastMonth_date)

          if (this_month_last_day < last_month_last_day && i + 1 > this_month_last_day) {
            // 前端報表顯示天數不足 31 天, 需補滿 31 天 ex.2019-02月: 29、30、31
            thisMonth_date = thisMonth_startDate.substr(0, 8) + (i + 1)
          }

          let revenue = {
            client_local_date: thisMonth_date,
            betGold: 0,
            JPGold: 0,
            winGold: 0,
            GGR: 0,
            last_month_betGold: 0,
          }

          // 當月總投注、收益
          thisMonth_info.forEach((item) => {
            // 計算用 utils.number 否則會溢位
            let cryDef =
              !isEveryDay || data["everyDayCurrency"] == "Consolidated" ? item["loginCryDef"] / item["cusCryDef"] : 1

            let betGold = utils.number.multiply(item["betGold"], cryDef)
            let JPGold = utils.number.multiply(item["JPGold"], cryDef)
            let winGold = utils.number.sub(
              utils.number.multiply(item["winGold"], cryDef),
              utils.number.multiply(item["JPGold"], cryDef)
            )
            let netWin = utils.number.sub(betGold, winGold)

            revenue["betGold"] = utils.number.add(revenue["betGold"], betGold)
            revenue["JPGold"] = utils.number.add(revenue["JPGold"], JPGold)
            revenue["winGold"] = utils.number.add(revenue["winGold"], winGold)
            revenue["GGR"] = utils.number.add(revenue["GGR"], netWin)

            this_month_total_betGold = utils.number.add(this_month_total_betGold, revenue["betGold"])
          })

          // 上月總投注
          lastMonth_info.forEach((item) => {
            let cryDef =
              !isEveryDay || data["everyDayCurrency"] == "Consolidated" ? item["loginCryDef"] / item["cusCryDef"] : 1
            let betGold = utils.number.multiply(item["betGold"], cryDef)
            revenue["last_month_betGold"] = utils.number.add(revenue["last_month_betGold"], betGold)

            last_month_total_betGold = utils.number.add(last_month_total_betGold, revenue["last_month_betGold"])
          })

          re_revenue.push(revenue)
        }

        // 查詢結果存入 cache, 除每日詳情
        let revenueResult = {
          this_month_total_betGold: this_month_total_betGold,
          last_month_total_betGold: last_month_total_betGold,
          revenue: re_revenue,
        }
        if (isEveryDay != true) self.app.localCache.addRevenueResult(requestKey, cidByCache, revenueResult, hourDiff)

        next(null, {
          code: code.OK,
          data: revenueResult,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getGameRevenue_byDate_v2] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//遊戲營收初始值 (幣別,遊戲種類)
handler.get_gameRevenue_init = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};
    if (typeof msg.data === "undefined") {
      next(null, { code: code.BET.PARA_DATA_FAIL })
      return
    }
    let games = []
    let currencies = []
    let game_group = []
    msg.data.opInclude = typeof msg.data.opInclude === "undefined" ? false : msg.data.opInclude
    const level = session.get("level")

    let userId = ""

    m_async.waterfall(
      [
        function (cb) {
          //遊戲報表新增初始遊戲搜尋
          gameDao.getUserJoinGames(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.GAME_LOAD_FAIL, data: null })
            return
          }
          games = [...r_data]
          switch (level) {
            case 1: //admin - 全部
              //self.app.rpc.config.configRemote.getCurrenies(userSession, cb); //幣別
              configDao.getCurrencyList(cb) //幣別
              break
            case 2: //hall - 下線(hall 資料表的currencies)
              userId = session.get("cid")
              if (session.get("isSub") == 0) {
                userDao.getHallCurrency_byCid(userId, cb)
              } else {
                //Hall Manager
                const data = {
                  cid: userId,
                }
                userDao.get_subUser_byCid(data, cb)
              }
              break
            case 3: //agent - 自己
              userId = session.get("cid")

              if (session.get("isSub") == 0) {
                //Agent 查詢 Wallet
                gameDao.getWallet({ cid: userId }, cb)
              } else {
                //Agent Manager
                userDao.get_subUser_byCid({ cid: userId }, cb)
              }
              break
          }
        },
        function (r_code, r_currencies, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.LOAD_CURRENCY_FAIL, data: null })
            return
          }

          switch (level) {
            case 1:
              currencies = r_currencies.map((x) => x.currency)
              break
            case 2:
              if (r_currencies != "" && session.get("isSub") == 0) {
                currencies = r_currencies.split(",")
              } else {
                const [target] = r_currencies
                currencies = target.Currencies.split(",")
              }
              break
            case 3:
              if (session.get("isSub") == 0) {
                r_currencies.forEach((item) => {
                  if (item.Currency != null && item.Currency != "") currencies.push(item.Currency)
                })
              } else {
                const [target] = r_currencies
                currencies = target.Currencies.split(",")
              }
              break
          }

          self.app.rpc.config.configRemote.getGameGroup(session, cb)
        },
      ],
      function (none, r_code, r_data) {
        game_group = r_data
        next(null, {
          code: code.OK,
          data: {
            currency: currencies,
            game_group: game_group,
            games: games,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][get_gameRevenue_init] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//幣別報表初始資料
handler.currency_report_init = function (msg, session, next) {
  try {
    var currencies = []

    if (typeof msg.data === "undefined") {
      next(null, { code: code.BET.PARA_DATA_FAIL })
      return
    }
    const level = session.get("level")
    let userId = ""

    m_async.waterfall(
      [
        function (cb) {
          switch (level) {
            case 1: //admin - 全部
              configDao.getCurrencyList(cb) //幣別
              break
            case 2: //hall - 下線(hall 資料表的currencies)
              userId = session.get("cid")
              if (session.get("isSub") == 0) {
                userDao.getHallCurrency_byCid(userId, cb)
              } else {
                //Hall Manager
                const data = {
                  cid: userId,
                }
                userDao.get_subUser_byCid(data, cb)
              }
              break
            case 3: //agent - 自己
              userId = session.get("cid")

              if (session.get("isSub") == 0) {
                //Agent 查詢 Wallet
                gameDao.getWallet({ cid: userId }, cb)
              } else {
                //Agent Manager
                userDao.get_subUser_byCid({ cid: userId }, cb)
              }
              break
          }
        },
        function (r_code, r_currencies, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.LOAD_CURRENCY_FAIL, data: null })
            return
          }

          switch (level) {
            case 1:
              currencies = r_currencies.map((x) => x.currency)
              break
            case 2:
              if (r_currencies != "" && session.get("isSub") == 0) {
                currencies = r_currencies.split(",")
              } else {
                const [target] = r_currencies
                currencies = target.Currencies.split(",")
              }
              break
            case 3:
              if (session.get("isSub") == 0) {
                r_currencies.forEach((item) => {
                  if (item.Currency != null && item.Currency != "") currencies.push(item.Currency)
                })
              } else {
                const [target] = r_currencies
                currencies = target.Currencies.split(",")
              }
              break
          }

          cb(null, { code: code.OK }, currencies)
        },
      ],
      function () {
        next(null, {
          code: code.OK,
          data: {
            currency: currencies,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][currency_report_init] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

/**
 * 統計報表 -> 幣別報表
 *
 * @param {object} msg
 * @param {object} session
 * @param {callback} next
 */
handler.get_currency_report = function (msg, session, next) {
  try {
    let ttlCount = 0
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.start_date === "undefined" ||
      typeof msg.data.end_date === "undefined"
    ) {
      next(null, { code: code.BET.PARA_DATA_FAIL })
      return
    }
    if (typeof msg.data.isPage === "undefined") {
      msg.data["isPage"] = true //預設-有分頁
    }

    if (msg.data.isPage == true) {
      //有分頁
      if (
        typeof msg.data.page === "undefined" ||
        typeof msg.data.pageCount === "undefined" ||
        typeof msg.data.page != "number" ||
        typeof msg.data.pageCount != "number"
      ) {
        next(null, { code: code.BET.PARA_DATA_FAIL })
        return
      }
    }

    //排序功能
    let sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "uniPlayers"
    let sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    //無分頁
    if (msg.data["isPage"] == false) {
      if (typeof msg.data.finish === "undefined") {
        msg.data["finish"] = 0
      } //0:未完成
      if (typeof msg.data.index === "undefined") {
        msg.data["index"] = 0
      } //開始第N筆
      msg.data["pageCount"] = 1000 //每次取得筆數
    }

    let data = msg.data

    if (msg.data.start_date != "") {
      data.start_date = msg.data.start_date
    }
    if (msg.data.end_date != "") {
      data.end_date = msg.data.end_date
    }
    let userId = session.get("cid")

    m_async.waterfall(
      [
        function (cb) {
          switch (session.get("level")) {
            case 1: //admin - 全部
              configDao.getCurrencyList(cb) //幣別
              break
            case 2: //hall - 下線(hall 資料表的currencies)
              userId = session.get("cid")
              if (session.get("isSub") == 0) {
                userDao.getHallCurrency_byCid(userId, cb)
              } else {
                //Hall Manager
                const data = {
                  cid: userId,
                }
                userDao.get_subUser_byCid(data, cb)
              }
              break
            case 3: //agent - 自己
              userId = session.get("cid")
              if (session.get("isSub") == 0) {
                //Agent 查詢 Wallet
                gameDao.getWallet(
                  {
                    cid: userId,
                  },
                  cb
                )
              } else {
                //Agent Manager
                userDao.get_subUser_byCid(
                  {
                    cid: userId,
                  },
                  cb
                )
              }
              break
          }
        },
        function (r_code, r_currencies, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.LOAD_CURRENCY_FAIL,
              data: null,
            })
            return
          }

          switch (session.get("level")) {
            case 1:
              data["betCurrency"] = r_currencies.map((x) => x.currency)
              break
            case 2:
              if (r_currencies != "" && session.get("isSub") == 0) {
                data["betCurrency"] = r_currencies.split(",")
              } else {
                const [target] = r_currencies
                data["betCurrency"] = target.Currencies.split(",")
              }
              break
            case 3:
              if (session.get("isSub") == 0) {
                let betCurrency = []
                r_currencies.forEach((item) => {
                  if (item.Currency != null && item.Currency != "") {
                    betCurrency.push(item.Currency)
                  }
                })
                data["betCurrency"] = betCurrency
              } else {
                const [target] = r_currencies
                data["betCurrency"] = target.Currencies.split(",")
              }
              break
          }

          data["user_level"] = session.get("level") //使用者層級
          if (data["user_level"] == 2) {
            //hall
            userId = session.get("isSub") == 1 ? session.get("hallId") : session.get("cid")
          } else {
            //agent
            userId = session.get("isSub") == 1 ? session.get("agentId") : session.get("cid")
          }
          data["cid"] = userId

          //hall id OR agent id
          switch (data.user_level) {
            case 2:
              userDao.getChildList({ cid: userId }, cb)
              break
            default:
              cb(null, { code: code.OK }, null)
              break
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.LOAD_USER_FAIL, data: null })
            return
          }
          let downLineCid = []
          if (r_data) {
            let downLine = r_data.pop()
            downLineCid = downLine[Object.keys(downLine)[0]].split(",")
            downLineCid.splice(0, 1) // 刪除第0 & 1筆: '$,rUpjbUmzFkAV(自己),7ejdZ2jNAZvQ, ...'
          }

          data["upId"] = data.user_level == 2 ? downLineCid : [userId]
          data["sel_table"] = "user_revenue_player"
          data["tableUpIdName"] = data["user_level"] == 2 ? "hallId" : "UpId"
          data["select"] = ["Currency", "uniPlayers", "rounds"]
          data["outGroupBy"] = ["Currency"]
          data["innerGroupBy"] = ["Currency"]

          bettingDao.get_Currency_Convert_Revenue(data, cb) //list
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.BET.LOAD_WAGER_FAIL, data: null })
          return
        }
        ttlCount = r_data.count
        let consolidatedData = {
          //帶入的資料
          downCurrency: msg.data.downCurrency, //判斷是否是合併幣別
          info: r_data["info"], //原始未合併幣別資料
          order: r_data["order"], //取出的排序資料
          level: "Currency", //層級
          mergeKeys: ["Currency"],
          showOriginCurrency: true,
          loginCurrency: msg.data["currency"],
        }
        let logs = handler.consolidated(consolidatedData)
        //因為consolidated處理修改那邊不好合併，在外面另外處理幣別報表合併幣別合併暫時處理
        let logs_merge = {}
        if (msg.data.downCurrency == "Consolidated") {
          if (logs.length > 0) {
            //logs_merge = logs[0];
            logs_merge["uniPlayers"] = 0
            logs_merge["betGold"] = 0
            logs_merge["realBetGold"] = 0
            logs_merge["jpConGoldOriginal"] = 0
            logs_merge["winGold"] = 0
            logs_merge["payoutAmount"] = 0
            logs_merge["jpGold"] = 0
            logs_merge["netWin"] = 0
            logs_merge["rounds"] = 0
            logs_merge["jpRealBetGold"] = 0
          }
          //有重複累加

          for (let i = 0; i < logs.length; i++) {
            logs_merge["uniPlayers"] += logs[i]["uniPlayers"]
            logs_merge["betGold"] += logs[i]["betGold"]
            logs_merge["realBetGold"] += logs[i]["realBetGold"]
            logs_merge["jpConGoldOriginal"] += logs[i]["jpConGoldOriginal"]
            logs_merge["winGold"] += logs[i]["winGold"]
            logs_merge["payoutAmount"] += logs[i]["payoutAmount"]
            logs_merge["jpGold"] += logs[i]["jpGold"]
            logs_merge["netWin"] += logs[i]["netWin"]
            logs_merge["rounds"] += logs[i]["rounds"]
            logs_merge["jpRealBetGold"] += logs[i]["jpRealBetGold"]
          }
          logs_merge["betGold"] = BN(logs_merge["betGold"]).dp(2).toNumber()
          logs_merge["realBetGold"] = BN(logs_merge["realBetGold"]).dp(2).toNumber()
          logs_merge["winGold"] = BN(logs_merge["winGold"]).dp(2).toNumber()
          logs_merge["payoutAmount"] = BN(logs_merge["payoutAmount"]).dp(2).toNumber()
          logs_merge["jpGold"] = BN(logs_merge["jpGold"]).dp(2).toNumber()
          logs_merge["jpConGoldOriginal"] = BN(logs_merge["jpConGoldOriginal"]).dp(4).toNumber()
          logs_merge["netWin"] = BN(logs_merge["netWin"]).dp(2).toNumber()
          logs_merge["jpRealBetGold"] = BN(logs_merge["jpRealBetGold"]).dp(2).toNumber()
          logs_merge["currency"] = "Consolidated"
          logs_merge["rtp"] = BN(logs_merge["winGold"]).div(logs_merge["betGold"]).times(100).dp(2).toNumber() || 0
        }

        let data = {}
        if (msg.data.isPage == false) {
          //無分頁

          var finish = ttlCount == msg.data["index"] + r_data["info"].length ? 1 : 0
          var index = finish == 1 ? null : msg.data["index"] + r_data["info"].length

          data = {
            totalCount: ttlCount, //總筆數
            finish: finish,
            index: index,
            logs: msg.data.downCurrency == "Consolidated" ? [logs_merge] : logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        } else {
          //有分頁

          var pageCount = msg.data.pageCount
          var pageCur = msg.data.page

          data = {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            logs: msg.data.downCurrency == "Consolidated" ? [logs_merge] : logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        }
        next(null, { code: code.OK, data: data })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][get_currency_report] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//單錢包
function get_transaction_record(data, callback) {
  try {
    //排序功能
    var sortKey = typeof data.sortKey !== "undefined" && data.sortKey != "" ? data.sortKey : "TxDate"
    var sortType =
      typeof data.sortType !== "undefined" && data.sortType != "" && [0, 1].indexOf(data.sortType) > -1
        ? data.sortType
        : 0
    var wid = []
    var count = 0 //筆數
    let logs = []

    m_async.waterfall(
      [
        function (cb) {
          bettingDao.getList_TransactionRecord(data, cb) //清單
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            callback(null, r_code)
            return
          }
          logs = r_data["info"]
          count = r_data["count"]
          logs.forEach((item) => {
            wid.push(item.Wid)
          })
          bettingDao.get_wagersInfo_byWid({ wid: wid }, cb) //找注單金額
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          callback(null, r_code)
          return
        }

        var wagers = r_data
        for (var i in logs) {
          var wagerInfo = wagers.filter((item) => item.Wid == logs[i]["Wid"])
          logs[i]["betGold"] = wagerInfo.length > 0 ? wagerInfo[0]["BetGold"] : 0
          logs[i]["GGR"] = wagerInfo.length > 0 ? wagerInfo[0]["GGR"] : 0
          logs[i]["TxDate"] = timezone.LocalToUTC(logs[i]["TxDate"])
        }

        var info = {
          count: count,
          logs: logs,
          sortKey: sortKey,
          sortType: sortType,
        }

        callback(none, r_code, info)
      }
    )
  } catch (err) {
    logger.error("[betHandler][get_transaction_record] catch err", err)
    callback(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//多錢包
function get_transfer_record(data, callback) {
  try {
    //排序功能
    var sortKey = typeof data.sortKey !== "undefined" && data.sortKey != "" ? data.sortKey : "TxDate"
    var sortType =
      typeof data.sortType !== "undefined" && data.sortType != "" && [0, 1].indexOf(data.sortType) > -1
        ? data.sortType
        : 0

    var transferRecord_Info = []

    m_async.waterfall(
      [
        function (cb) {
          bettingDao.getList_TransferRecord(data, cb) //清單
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          callback(null, r_code)
          return
        }
        transferRecord_Info = r_data

        for (var i in transferRecord_Info["info"]) {
          // 顯示 "上線"名稱
          if (transferRecord_Info["info"][i]["AgentName"] == null) {
            transferRecord_Info["info"][i]["AgentName"] = "-"
          }
        }

        var info = {
          count: transferRecord_Info.count,
          logs: transferRecord_Info["info"],
          sortKey: sortKey,
          sortType: sortType,
        }
        callback(none, r_code, info)
      }
    )
  } catch (err) {
    logger.error("[betHandler][get_transfer_record] catch err", err)
    callback(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//代理營收->遊戲層
handler.getUserRevenue_game = function (msg, session, next) {
  try {
    var self = this

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.start_date === "undefined" ||
      typeof msg.data.end_date === "undefined"
    ) {
      next(null, { code: code.BET.PARA_DATA_FAIL })
      return
    }

    if (typeof msg.data.isPage === "undefined") {
      msg.data["isPage"] = true //預設-有分頁
    }

    if (msg.data.isPage == true) {
      //有分頁
      if (
        typeof msg.data.page === "undefined" ||
        typeof msg.data.pageCount === "undefined" ||
        typeof msg.data.page != "number" ||
        typeof msg.data.pageCount != "number"
      ) {
        next(null, { code: code.BET.PARA_DATA_FAIL })
        return
      }
    }

    //無分頁
    if (msg.data["isPage"] == false) {
      if (typeof msg.data.finish === "undefined") {
        msg.data["finish"] = 0
      } //0:未完成
      if (typeof msg.data.index === "undefined") {
        msg.data["index"] = 0
      } //開始第N筆
      msg.data["pageCount"] = 1000 //每次取得筆數
    }

    var data = msg.data
    var ttlCount_Revenue = 0
    var logs = []
    var games = []
    var game_group = []
    let userCidSort = []

    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "rounds"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    //count
    m_async.waterfall(
      [
        function (cb) {
          gameDao.currencyExchangeRate({}, cb) // 取得所有幣別匯率
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.RATE_NOT_VALID, data: null })
            return
          }

          self.app.rpc.config.configRemote.getGameGroup(session, cb) //遊戲種類
        },
        function (r_code, r_data, cb) {
          game_group = r_data

          var userId = session.get("cid")
          if (session.get("level") == 2 && session.get("isSub") == 1) {
            //子帳號
            userId = session.get("hallId")
          }

          data.level = session.get("level")
          data.upId = userId

          bettingDao.getUserRevenue_game(data, cb) //list
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
            return
          }
          ttlCount_Revenue = r_data.count
          games = r_data["info"]

          var gameId = []
          games.forEach((item) => {
            if (gameId.indexOf(item["gameId"]) == -1) gameId.push(item["gameId"])
          })

          gameDao.getGameName_byId({ gameId: gameId }, cb) //取遊戲名稱
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
          return
        }

        if (games.length > 0) {
          games.forEach((item) => {
            userCidSort.push({ id: item.Cid, Currency: item.Currency, gameId: item.gameId })
          })
        }

        const convertCurrency = (value, cryDef) => BN(value).times(cryDef).dp(4).toNumber()

        userCidSort.forEach((userId) => {
          games.forEach((item) => {
            if (
              userId["id"] == item["Cid"] &&
              userId["gameId"] == item["gameId"] &&
              (userId["Currency"] == item["Currency"] || msg.data.downCurrency == "Consolidated")
            ) {
              const group = game_group.filter((x) => x.GGId == item["ggId"])
              const gameInfo = r_data.filter((x) => x.GameId == item["gameId"])

              const loginCryDef = item["loginCryDef"]
              const cusCryDef = item["cusCryDef"]

              const convertCurrencyRate = BN(loginCryDef).div(cusCryDef).dp(6).toNumber()

              const cryDef = msg.data.downCurrency == "Consolidated" ? convertCurrencyRate : 1

              const betGold = convertCurrency(item["betGold"], cryDef)
              const jpGold = convertCurrency(item["jpGold"], cryDef)
              const jpConGoldOriginal = convertCurrency(item["jpConGoldOriginal"], cryDef)
              const realBetGold = convertCurrency(item["realBetGold"], cryDef)
              const winGold = convertCurrency(item["winGold"], cryDef)
              const payoutAmount = BN(winGold).minus(jpGold).toNumber()
              const netWin = BN(betGold).minus(payoutAmount).toNumber()

              const rtp = BN(winGold).div(realBetGold).times(100).dp(4).toNumber() || 0
              const rtpNoJP = BN(payoutAmount).div(realBetGold).times(100).dp(4).toNumber() || 0
              const currency = msg.data.downCurrency == "Consolidated" ? "Consolidated" : item["Currency"]

              const repeat = logs.some(
                (log) => log["id"] == item["Cid"] && log["currency"] == currency && log["gameId"] == item["gameId"]
              )

              const tmp = {
                // 預設值
                id: item["Cid"],
                gameId: item["gameId"],
                rounds: item["rounds"], //場次
                BaseRounds: item["BaseRounds"], //場次
                level: "GAME",
                nameE: gameInfo.length > 0 ? gameInfo[0]["NameE"] : "",
                nameC: gameInfo.length > 0 ? gameInfo[0]["NameC"] : "",
                nameG: gameInfo.length > 0 ? gameInfo[0]["NameG"] : "",
                betGold: 0,
                realBetGold: 0,
                winGold: 0,
                jpPoint: 0,
                jpGold: 0,
                jpConGoldOriginal: 0,
                netWin: 0,
                payoutAmount: 0,
                rtp: 0,
                rtpNoJP: 0,
                ggId: item["ggId"],
                groupNameE: group.length > 0 ? group[0]["NameE"] : "",
                groupNameG: group.length > 0 ? group[0]["NameG"] : "",
                groupNameC: group.length > 0 ? group[0]["NameC"] : "",
                gameState: item["gameState"],
                currency: msg.data.downCurrency == "Consolidated" ? "Consolidated" : item["Currency"], //兌換幣別
              }

              if (repeat) {
                logs.forEach((log) => {
                  if (log["id"] == item["Cid"] && log["currency"] == currency && log["gameId"] == item["gameId"]) {
                    log["betGold"] = BN(log["betGold"]).plus(betGold).toNumber()
                    log["realBetGold"] = BN(log["realBetGold"]).plus(realBetGold).toNumber()
                    log["winGold"] = BN(log["winGold"]).plus(winGold).toNumber()
                    log["payoutAmount"] = BN(log["payoutAmount"]).plus(payoutAmount).toNumber()
                    log["jpGold"] = BN(log["jpGold"]).plus(jpGold).toNumber()
                    log["jpConGoldOriginal"] = BN(log["jpConGoldOriginal"]).plus(jpConGoldOriginal).toNumber()
                    log["netWin"] = BN(log["netWin"]).plus(netWin).toNumber()
                  }
                })

                logs[0].rtp = BN(logs[0].winGold).div(logs[0].realBetGold).times(100).toNumber()
                logs[0].rtpNoJP = BN(logs[0].payoutAmount).div(logs[0].realBetGold).times(100).toNumber()
              } else {
                tmp["betGold"] = betGold
                tmp["realBetGold"] = realBetGold
                tmp["winGold"] = winGold
                tmp["jpGold"] = jpGold
                tmp["jpConGoldOriginal"] = jpConGoldOriginal
                tmp["netWin"] = netWin
                tmp["payoutAmount"] = payoutAmount
                tmp["rtp"] = rtp
                tmp["rtpNoJP"] = rtpNoJP
                logs.push(tmp)
              }
            }
          })
        })

        var info = {}
        if (msg.data.isPage == false) {
          //無分頁

          var finish = ttlCount_Revenue == msg.data["index"] + logs.length ? 1 : 0
          var index = finish == 1 ? null : msg.data["index"] + logs.length

          info = {
            totalCount: ttlCount_Revenue, //總筆數
            finish: finish,
            index: index,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        } else {
          //有分頁

          var ttlCount = ttlCount_Revenue
          var pageCount = msg.data.pageCount
          var pageCur = msg.data.page

          info = {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            logs: logs,
            sortKey: sortKey,
            sortType: sortType,
          }
        }

        next(null, {
          code: code.OK,
          data: info,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getUserRevenue_game] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 取得 cron revenue 的剩餘秒數
function getCronRevenueRemainSeconds() {
  try {
    const cronStartMin = 1
    const cronIntervalMin = 10
    let now = new Date()
    let nowMin = now.getMinutes()
    let nowSec = now.getSeconds()
    return (cronIntervalMin - ((nowMin - cronStartMin + cronIntervalMin) % cronIntervalMin)) * 60 - nowSec
  } catch (err) {
    logger.error("[betHandler][getCronRevenueRemainSeconds] catch err", err)
    return 0
  }
}
// 最後統計時間
var getLastRevenueTimes = function (revenueResult, session, next) {
  try {
    m_async.waterfall(
      [
        function (cb) {
          var param = {
            item: "last_revenue_times",
          }
          configDao.getSystemSetting(param, cb)
        },
      ],
      function (none, r_code, r_data) {
        // 注意 'remain_sec' 不快取
        revenueResult["remain_sec"] = getCronRevenueRemainSeconds()
        revenueResult["last_revenue_times"] = r_data

        next(null, { code: code.OK, data: revenueResult })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getLastRevenueTimes] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
//本日數據
handler.get_today_bet_info = function (msg, session, next) {
  try {
    var data = {}
    var info = {}
    const level = session.get("level")
    var userId = 0

    const self = this

    const userLevel = new UserLevelService({ session })

    const { isAdmin, cid } = userLevel.getUserLevelData()

    const hourDiff = msg.data.hourDiff

    // 先由 cache 取查詢結果
    const requestKey = "get_today_bet_info"

    const cidByCache = isAdmin ? "admin" : cid

    const revenueResult = self.app.localCache.getRevenueResult(requestKey, cidByCache, hourDiff)

    if (revenueResult) {
      // 注意 'remain_sec' 不快取
      // revenueResult['remain_sec'] = getCronRevenueRemainSeconds();
      getLastRevenueTimes(revenueResult, session, next)

      // next(null, {
      //     code: code.OK,
      //     data: revenueResult
      // });
      // return;
    }

    const now = new Date()
    const formatString = "YYYY-MM-DD HH:mm:ss"

    const sumRevenueData = (data) => {
      return data.reduce(
        (acc, cur) => {
          const { loginCryDef, cusCryDef, rounds, BetGold, JPGold, WinGold } = cur

          const convertCurrencyRate = Number(utils.formatFloat(loginCryDef / cusCryDef, 6))

          const count = Number(rounds) || 0
          const winGold = BN(WinGold).toNumber()
          const jpGold = BN(JPGold).toNumber()
          const betGold = BN(BetGold).toNumber()
          const payoutAmount = BN(winGold).minus(jpGold).toNumber()

          acc.rounds += count
          acc.betGold += BN(betGold).times(convertCurrencyRate).toNumber()
          acc.winGold += BN(payoutAmount).times(convertCurrencyRate).toNumber()
          acc.netWin += BN(betGold).minus(payoutAmount).times(convertCurrencyRate).toNumber()

          return acc
        },
        { rounds: 0, betGold: 0, winGold: 0, netWin: 0 }
      )
    }

    m_async.waterfall(
      [
        function (cb) {
          //取系統主幣別
          var param = {
            item: "main_currency",
          }
          configDao.getSystemSetting(param, cb)
        },
        function (r_code, r_data, cb) {
          data["mainCurrency"] = r_data //主幣別

          data["level"] = level

          //取交收者幣別
          if (level === 1) {
            cb(
              null,
              {
                code: code.OK,
              },
              data["mainCurrency"]
            )
          } else {
            if (level == 2) {
              //hall
              userId = session.get("isSub") == 1 ? session.get("hallId") : session.get("cid")
              data["user_hallId"] = userId
            } else {
              //agent
              userId = session.get("isSub") == 1 ? session.get("agentId") : session.get("cid")
              data["user_hallId"] = session.get("hallId")
              data["user_agentId"] = userId
            }
            userDao.getCusCurrency_byCid(userId, cb)
          }
        },
        function (r_code, r_currency, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.LOAD_CURRENCY_FAIL,
              data: null,
            })
            return
          }
          data["opCurrency"] = r_currency

          //     const EasternUS_Time = timezone.serverTime(); // 美東時間

          //     var param = {
          //         level: session.get('level'),
          //         cid: session.get('cid'),
          //         hallId: (session.get('level') == 2 && session.get('isSub') < 1) ? session.get('cid') : session.get('hallId'),
          //         end_date: timezone.serverTime()
          //     };
          //     logDao.getOnlinePlayersNums(param, cb);
          // },
          // function (r_code, r_data, cb) {

          //     if (r_code.code != code.OK) {
          //         next(null, {
          //             code: code.LOG.LOAD_PLAYERS_NUM_FAIL,
          //             data: null
          //         });
          //         return;
          //     }

          //     info['day_numbers'] = r_data;

          const todayStart = timezone.getTimezoneDate(hourDiff, now).startOf("day").utc().format(formatString)
          const todayEnd = timezone.getTimezoneDate(hourDiff, now).endOf("day").utc().format(formatString)

          data.start_date = todayStart
          data.end_date = todayEnd

          console.log("-getBetInfo day-------------", JSON.stringify(data))
          // bettingDao.getBetInfo(data, cb); //當天的
          data["select"] = ["rounds", "BaseRounds", "currency"]
          data["innerGroupBy"] = ["Currency"]
          data["outGroupBy"] = ["currency"]
          data["downCurrency"] = "Consolidated"
          data["currency"] = data["opCurrency"]
          data["user_level"] = level
          data["upId"] = level === 1 ? ["-1"] : [userId]
          switch (level) {
            case 1: //admin 取 hall
              data["sel_table"] = "user_revenue_hall"
              data["tableUpIdName"] = "UpId"
              break
            case 2: //hall 取 agent
              data["sel_table"] = "user_revenue_hall"
              data["tableUpIdName"] = "Cid"
              break
            case 3: //agent 取 player
              data["sel_table"] = "user_revenue_agent"
              data["tableUpIdName"] = "Cid"
              break
          }

          bettingDao.get_Currency_Convert_Revenue({ ...data }, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.REVENUE_LOAD_FAIL,
              data: null,
            })
            return
          }

          const result = sumRevenueData(r_data.info)

          info["day_rounds"] = result.rounds
          info["day_betGold"] = result.betGold
          info["day_winGold"] = result.winGold
          info["day_GGR"] = result.netWin

          const thisMonthStart = timezone.getTimezoneDate(hourDiff, now).startOf("month").utc().format(formatString)

          data["start_date"] = thisMonthStart

          console.log("-getBetInfo month-------------", JSON.stringify(data))

          // bettingDao.getBetInfo(data, cb); //當月的
          bettingDao.get_Currency_Convert_Revenue(data, cb)
        },
        // ,function (r_code, r_data, cb) {
        //     if (r_code.code != code.OK) {
        //         next(null, {
        //             code: code.BET.REVENUE_LOAD_FAIL,
        //             data: null
        //         });
        //         return;
        //     }

        //     // 登入者 交收幣別 與 主幣別 相同時,不須找匯率 & 轉換匯率
        //     if (data['opCurrency'] === data['mainCurrency']) {
        //         cb(null, { code: code.OK }, null);
        //         return;
        //     }
        //     var param = {
        //         currency: data['opCurrency'],
        //         end_date: data['end_date'],
        //     };
        //     console.log('currencyExchangeRate param:', JSON.stringify(param));
        //     gameDao.currencyExchangeRate(param, cb); // 取得登入者幣別匯率
        // }
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.RATE_NOT_VALID,
            data: null,
          })
          return
        }

        const result = sumRevenueData(r_data.info)

        info["month_GGR"] = result.netWin
        info["month_betGold"] = result.betGold // 月總投注

        // if (!!r_exchangeRate) {
        //     if (r_exchangeRate.length > 0)
        //         r_exchangeRate = r_exchangeRate[0].CryDef; // 匯率
        //     else {
        //         // DB 找不到該交收幣別幣別的匯率 (有鬼)
        //         r_exchangeRate = 0;
        //         logger.error('can not find currency exchangeRate !! currency is : ', data['currency']);
        //     }
        // } else {
        //     // 不須轉換匯率
        //     r_exchangeRate = 1;
        // }

        // Object.keys(info).forEach((key) => {
        //     // 人數 & 單量 以外需換算匯率 (日總投注、日總營收、月總投注、月總營收)
        //     if (key !== 'day_numbers' && key !== 'day_rounds') {
        //         info[key] = utils.number.oneThousand(Number(info[key].replace(/\,/g, '')), consts.Math.DIVIDE);
        //         // 幣別與登入者交收幣別不同時 // 匯率換算
        //         if (r_exchangeRate != 1 ) {
        //             info[key] = utils.number.multiply(info[key], r_exchangeRate);
        //         }
        //         info[key] = utils.number.floor(info[key]);
        //     }
        // });

        // 查詢結果存入 cache
        let revenueResult = info
        self.app.localCache.addRevenueResult(requestKey, cidByCache, revenueResult, hourDiff)
        // 注意 'remain_sec' 不快取
        // revenueResult['remain_sec'] = getCronRevenueRemainSeconds();
        getLastRevenueTimes(revenueResult, session, next)

        // next(null, {
        //     code: code.OK,
        //     data: revenueResult
        // });
        // return;
      }
    )
  } catch (err) {
    logger.error("[betHandler][get_today_bet_info] catch err", inspect(err))
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 日單量
handler.get_bet_nums_byhours = function (msg, session, next) {
  try {
    var data = {}

    var todayRounds = {} // 今日單量
    var todayPlayers = {} // 今日人數
    var yesterdayGameRounds = {} // 昨日單量
    var yesterdayPlayers = {} // 昨日人數

    const self = this

    const userLevel = new UserLevelService({ session })

    const { isAdmin, isHall, isAgent, cid } = userLevel.getUserLevelData()

    const hourDiff = msg.data.hourDiff

    // 先由 cache 取查詢結果
    const requestKey = "get_bet_nums_byhours"

    const cidByCache = isAdmin ? "admin" : cid

    const revenueResult = self.app.localCache.getRevenueResult(requestKey, cidByCache, hourDiff)

    if (revenueResult) {
      next(null, {
        code: code.OK,
        data: revenueResult,
      })
      return
    }

    const now = new Date()
    const formatString = "YYYY-MM-DD HH:mm:ss"

    data.hourDiffFormated = timezone.formatHourDiff(hourDiff)

    let agentIdList = []

    m_async.waterfall(
      [
        function (cb) {
          data["level"] = session.get("level")

          if (session.get("level") !== 1) {
            var userId = 0

            if (session.get("level") == 2) {
              //hall
              userId = session.get("isSub") == 1 ? session.get("hallId") : session.get("cid")
              data["user_hallId"] = userId
            } else {
              //agent
              userId = session.get("cid")
              data["user_hallId"] = session.get("hallId")
              data["user_agentId"] = userId
            }
          }

          // 取得底下所有代理id
          if (isHall) {
            userDao.getChildList({ cid, isAg: 3 }, cb)
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.REVENUE_LOAD_FAIL,
              data: null,
            })
            return
          }

          if (isHall) {
            const list = r_data[0].list.split(",")

            // 去掉$和自己
            list.splice(0, 2)

            agentIdList = [...list]
          } else if (isAgent) {
            agentIdList = [cid]
          }

          const todayStart = timezone.getTimezoneDate(hourDiff, now).startOf("day").utc().format(formatString)
          const todayEnd = timezone.getTimezoneDate(hourDiff, now).endOf("day").utc().format(formatString)

          data.start_date = todayStart
          data.end_date = todayEnd

          // 當天的日單量(每小時統計)
          bettingDao.getBetNums_byHour(data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.REVENUE_LOAD_FAIL,
              data: null,
            })
            return
          }
          todayRounds = r_data // 今日單量

          // 今日活躍線上玩家數(每小時)
          const { hourDiffFormated, start_date: startDate, end_date: endDate } = data

          bettingDao.countAllActivePlayersPerHour({ hourDiffFormated, startDate, endDate, agentIdList }, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.REVENUE_LOAD_FAIL,
              data: null,
            })
            return
          }
          todayPlayers = r_data // 今日人數

          const yesterdayStart = timezone
            .getTimezoneDate(hourDiff, now)
            .subtract(1, "day")
            .startOf("day")
            .utc()
            .format(formatString)

          const yesterdayEnd = timezone
            .getTimezoneDate(hourDiff, now)
            .subtract(1, "day")
            .endOf("day")
            .utc()
            .format(formatString)

          data.start_date = yesterdayStart
          data.end_date = yesterdayEnd

          bettingDao.getBetNums_byHour(data, cb) // 昨天的日單量(每小時統計)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.REVENUE_LOAD_FAIL,
              data: null,
            })
            return
          }
          yesterdayGameRounds = r_data // 昨日單量

          // 昨日活躍線上玩家數(每小時)
          const { hourDiffFormated, start_date: startDate, end_date: endDate } = data

          bettingDao.countAllActivePlayersPerHour({ hourDiffFormated, startDate, endDate, agentIdList }, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.BET.REVENUE_LOAD_FAIL,
            data: null,
          })
          return
        }
        yesterdayPlayers = r_data // 昨日人數

        const info = []

        for (let i = 0; i < 24; i++) {
          const hour = i < 10 ? `0${i}` : `${i}`

          const [hourTodayRounds] = todayRounds.filter((x) => x.clientLocalHour === hour)

          const rounds = hourTodayRounds ? hourTodayRounds.rounds : 0

          const [hourTodayPlayers] = todayPlayers.filter((x) => x.localHour === hour)

          const uniPlayers = hourTodayPlayers ? hourTodayPlayers.counts : 0

          const [hourYesterdayRounds] = yesterdayGameRounds.filter((x) => x.clientLocalHour === hour)

          const yesterdayRounds = hourYesterdayRounds ? hourYesterdayRounds.rounds : 0

          const [hourYesterdayPlayers] = yesterdayPlayers.filter((x) => x.localHour === hour)

          const yesterdayUniPlayers = hourYesterdayPlayers ? hourYesterdayPlayers.counts : 0

          const result = { hour, rounds, uniPlayers, yesterdayRounds, yesterdayUniPlayers }

          info.push(result)
        }

        // 查詢結果存入 cache
        let revenueResult = {
          info: info,
        }

        self.app.localCache.addRevenueResult(requestKey, cidByCache, revenueResult, hourDiff)

        next(null, {
          code: code.OK,
          data: revenueResult,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][get_bet_nums_byhours] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 補單
handler.add_wagers_bet = function (msg, session, next) {
  try {
    if (
      typeof msg == "undefined" ||
      typeof msg.data == "undefined" ||
      typeof msg.data.wagerGameType == "undefined" ||
      typeof msg.data.wagerType == "undefined"
    ) {
      next(null, { code: code.BET.PARA_DATA_FAIL, data: null })
      return
    }
    let data = msg.data
    data.wagerType = parseInt(data.wagerType)
    data.wagerGameType = parseInt(data.wagerGameType)
    data["mongoTableName"] = {
      mom: "",
      sub: "",
    }
    switch (data.wagerGameType) {
      case 1: // 魚機
        data["mongoTableName"].mom = "fish_hunter_area_players_history"
        data["mongoTableName"].sub = "fish_hunter_bullets_history"
        break
      case 2: // 棋牌
        // data['mongoTableName'].mom = '';
        // data['mongoTableName'].sub = '';
        break
      case 3: // 老虎機
        // data['mongoTableName'].mom = '';
        // data['mongoTableName'].sub = '';
        break
    }
    let wIds = [] // 母單wId列表
    let sub_wIds = [] // 子單wId列表
    let subIds = [] // 補單子單Id列表
    data["mysql_flag"] = false
    data["mongo_flag"] = false
    if (typeof data.formShow !== "undefined") {
      data.mysql_flag = data.formShow.MySQL
      data.mongo_flag = data.formShow.Mongo
    }

    if (typeof data.mysql_list == "undefined") data["mysql_list"] = []
    if (typeof data.mongo_list == "undefined") data["mongo_list"] = []
    if (typeof data.sub_list == "undefined") data["sub_list"] = []

    switch (data.wagerType) {
      case 1: // 母+子
      case 2: // 母
      case 4: // Log
        // 2: 取 MySQL || Mongo 其中一個 wId
        if (data.mysql_flag) {
          for (let mysql of data.mysql_list) {
            if (wIds.indexOf(mysql.Wid) == -1) wIds.push(mysql.Wid)
          }
          if (data.wagerType == 2) break
        }
        if (data.mongo_flag) {
          for (let mongo of data.mongo_list) {
            if (wIds.indexOf(mongo._id) == -1) wIds.push(mongo._id)
          }
        }
        if (data.wagerType == 2) break

        break
      case 3: // 子
        for (let sub of data.sub_list) {
          // if (wIds.indexOf(sub.wId) == -1) wIds.push(sub.wId); // 無重複的 wId 才找
          // if (sub_wIds.indexOf(sub.wId) == -1) sub_wIds.push(sub.wId);
          subIds.push(sub._id)
        }
        break
    }

    m_async.waterfall(
      [
        function (cb) {
          // 檢查 MySQL 有無此單
          var param = { wId: wIds }
          bettingDao.checkWagerIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            logger.error("[betHandler][add_wagers_bet][checkWagerIsExist] err: ", JSON.stringify(r_code))
            next(null, { code: code.BET.PARA_DATA_FAIL, data: null })
            return
          }
          let mysql_wids = r_data.map((item) => {
            return item.Wid
          })

          let duplicate_wIdList = []
          let notExist_wIdList = []
          let notExist_momWidList = []

          switch (data.wagerType) {
            case 1: // 母+子
            case 2: // 母
            case 4: // Log
              for (let wId of wIds) {
                // 補 MySQL: 檢查 MySQL 是否重複

                // 補 Mongo && 不補 MySQL: 檢查 MySQL 是否存在此母單
                if (data.mongo_flag && !data.mysql_flag && mysql_wids.indexOf(wId) == -1) {
                  notExist_wIdList.push(wId) // MySQL 母單不存在
                }
              }

              if (duplicate_wIdList.length > 0) {
                next(null, { code: code.BET.WAGER_DUPLICATE_MySQL, data: { duplicate_wIdList, notExist_wIdList } })
                return
              }

              if (notExist_wIdList.length > 0) {
                next(null, { code: code.BET.WAGER_NOT_EXIST_MySQL, data: { duplicate_wIdList, notExist_wIdList } })
                return
              }

              if (data.wagerType == 2) break

              break
            case 3: // 子
              // 補 子單: 檢查所有子單的母單是否存在
              for (let wId of sub_wIds) {
                // 不補 MySQL 必須有 MySQL 母單
                if (!data.mysql_flag && mysql_wids.indexOf(wId) == -1) {
                  notExist_momWidList.push(wId)
                }
              }

              if (notExist_momWidList.length > 0) {
                next(null, { code: code.BET.WAGER_NOT_EXIST_MySQL, data: { notExist_momWidList } })
                return
              }
              break
          }

          // 檢查 Mongo 有無此單
          var param = { wId: wIds, wagerGameType: data.wagerGameType, tableName: data.mongoTableName.mom }
          bettingDao.checkMongoWagerIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            logger.error("[betHandler][add_wagers_bet][1.checkMongoWagerIsExist] err: ", JSON.stringify(r_code))
            next(null, { code: code.BET.PARA_DATA_FAIL, data: null })
            return
          }

          let mongo_wids = r_data.map((item) => {
            return item._id
          })

          let duplicate_wIdList = []
          let notExist_wIdList = []
          let notExist_momWidList = []

          switch (data.wagerType) {
            case 1: // 母+子
            case 2: // 母
            case 4: // Log
              for (let wId of wIds) {
                // 補 Mongo: 檢查 Mongo 是否重複
                if (data.mongo_flag && mongo_wids.indexOf(wId) > -1) {
                  duplicate_wIdList.push(wId) // Mongo 母單重複
                }

                // 補 MySQL && 不補 Mongo: 檢查 Mongo 是否存在此母單
                if (data.mysql_flag && !data.mongo_flag && mongo_wids.indexOf(wId) == -1) {
                  notExist_wIdList.push(wId) // Mongo 母單不存在
                }
              }

              if (duplicate_wIdList.length > 0) {
                next(null, { code: code.BET.WAGER_DUPLICATE_Mongo, data: { duplicate_wIdList, notExist_wIdList } })
                return
              }

              if (notExist_wIdList.length > 0) {
                next(null, { code: code.BET.WAGER_NOT_EXIST_Mongo, data: { duplicate_wIdList, notExist_wIdList } })
                return
              }

              if (data.wagerType == 2) break

              break
            case 3: // 子
              // 補 子單: 檢查所有子單的母單是否存在
              for (let wId of sub_wIds) {
                // 不補 Mongo 必須有 Mongo 母單
                if (!data.mongo_flag && mongo_wids.indexOf(wId) == -1) {
                  notExist_momWidList.push(wId)
                }
              }

              if (notExist_momWidList.length > 0) {
                next(null, { code: code.BET.WAGER_NOT_EXIST_Mongo, data: { notExist_momWidList } })
                return
              }
              break
          }

          // 檢查 Mongo子單 有無此單
          var param = { wId: subIds, wagerGameType: data.wagerGameType, tableName: data.mongoTableName.sub }
          bettingDao.checkMongoWagerIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            logger.error("[betHandler][add_wagers_bet][2.checkMongoWagerIsExist] err: ", JSON.stringify(r_code))
            next(null, { code: code.BET.PARA_DATA_FAIL, data: r_data })
            return
          }

          let subRecord_wids = r_data.map((item) => {
            return item._id
          })

          let duplicate_subIdList = []

          switch (data.wagerType) {
            case 1: // 母+子
            case 3: // 子
            case 4: // Log
              // 補 子單: 檢查所有子單是否有重複
              for (let wId of subIds) {
                if (subRecord_wids.indexOf(wId) > -1) {
                  duplicate_subIdList.push(wId)
                } // 子單重複
              }

              if (duplicate_subIdList.length > 0) {
                next(null, { code: code.BET.WAGER_DUPLICATE_MongoSub, data: { duplicate_subIdList } })
                return
              }
              break
          }
          bettingDao.addWagers(data, cb) // 新增注單
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.BET_CREATE_FAIL, data: null })
            return
          }
          var logData = {}
          logData["AdminId"] = session.get("cid") || ""
          logData["UserName"] = session.get("usrName") || ""
          logData["FunctionGroupL"] = "System"
          logData["FunctionAction"] = "AddWager"
          logData["ModifiedType"] = "add"

          logData["RequestMsg"] = JSON.stringify(msg)
          logData["Desc_Before"] = ""
          let after_log = [
            {
              addWager: [
                {
                  mysql_list: data["mysql_list"],
                  mongo_list: data["mongo_list"],
                  sub_list: data["sub_list"],
                },
              ],
            },
          ]
          logData["Desc_After"] = JSON.stringify(after_log)
          logData["IP"] = msg.remoteIP
          console.log("[betHandler][add_wagers_bet] logData: ", logData)
          logDao.add_log_admin(logData, cb)
        },
      ],
      function (none, r_code) {
        if (r_code.code != code.OK) {
          next(null, { code: code.LOG.LOG_ACTION_FAIL, data: null })
          return
        }
        next(null, { code: code.OK, data: null })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][add_wagers_bet] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 重新結帳
handler.settlement = function (msg, session, next) {
  try {
    // 取往前日期 YYYY-MM-DD 00:00:00
    let begin_datetime = timezone.transYesterdayTime("", conf.Statistics_Time)
    begin_datetime = begin_datetime.substr(0, 10) + " 00:00:00"

    // 取當時開機 server 時間 YYYY-MM-DD HH:mm:ss
    let end_datetime = timezone.serverTime()
    let nowMinute = Number(end_datetime.substr(14, 2))
    let dateHour = end_datetime.substr(0, 14)

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

    logger.info("[重新結帳] 補單統計資料 ----updateTime:", begin_datetime, " , end: ", end_datetime)

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
        function (r_code, r_data, cb) {
          console.log("[重新結帳][清除快取]", JSON.stringify(r_code), JSON.stringify(r_data))

          // 通知可以清除儀錶板資料查詢快取
          pomelo.app.rpc.bet.betRemote.onRevenue.toServer("*", function () {
            cb(null)
          })
        },
        function (r_code, r_data, cb) {
          // 通知前端可以取儀錶板資料
          pomelo.app.rpc.connector.backendRemote.onRevenue.toServer("*", function () {
            cb(null)
          })
        },
      ],
      function () {
        logger.info("[重新結帳] -updateRevenueReport End-")
        next(null, { code: code.OK, data: null })
      }
    )
  } catch (err) {
    logger.error("[betHandler][settlement] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 刪除統計
handler.delSettlement = function (msg, session, next) {
  try {
    let executeTime_start = null

    m_async.waterfall(
      [
        function (cb) {
          let tableList = [
            "game_revenue_hall",
            "game_revenue_agent",
            "game_revenue_player",
            "user_revenue_hall",
            "user_revenue_agent",
            "user_revenue_player",
          ]
          executeTime_start = timezone.formatTime(Date.now(), "YYYY-MM-DD HH:mm:ss.SSS")

          bettingDao.delWagersRevenueData(tableList, cb) // 刪除所有 revenue 資料
        },
      ],
      function (none, r_code) {
        if (r_code.code != code.OK) {
          next(null, { code: code.BET.DELETE_REVENUE_FAIL, data: null })
          return
        }

        let executeTime_end = timezone.formatTime(Date.now(), "YYYY-MM-DD HH:mm:ss.SSS")
        let executeTime = timezone.isDiff(executeTime_end, executeTime_start)

        next(null, { code: code.OK, data: executeTime })
      }
    )
  } catch (err) {
    logger.error("[betHandler][delSettlement] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 取得各種貢獻: 遊戲類別押注貢獻 & 廳主押注貢獻 & 廳主人數貢獻
handler.getContribution = function (msg, session, next) {
  try {
    var self = this
    var gameContribution = [] // 遊戲類別押注貢獻
    var hallContribution = [] // 廳主Reseller貢獻 (人數&押注)
    var subResellerContribution = [] // subReseller貢獻 (人數&押注)

    let resellerTree = {} // Reseller Id 樹狀表 ex. { '131': ['R7F6j7YPO74L'] }
    // let subResellers = [];               // subResellers 列表

    var game_bet = [] // 遊戲類別押注貢獻
    var reseller_bet = [] // 廳主Reseller 押注貢獻
    var reseller_userCount = [] // 廳主Reseller 人數貢獻
    var subReseller_bet = [] // subReseller 押注貢獻
    var subReseller_userCount = [] // subReseller 人數貢獻

    var game_group = []

    const now = new Date()
    const hourDiff = msg.data.hourDiff
    const formatString = "YYYY-MM-DD HH:mm:ss"

    const thisMonthStart = timezone.getTimezoneDate(hourDiff, now).startOf("month").utc().format(formatString)
    const todayEnd = timezone.getTimezoneDate(hourDiff, now).endOf("day").utc().format(formatString)

    const data = {
      start_date: thisMonthStart,
      end_date: todayEnd,
    }

    // 先由 cache 取查詢結果
    const requestKey = "getContribution"
    let cidByCache = ""
    if (session.get("level") != 1) {
      cidByCache = session.get("cid")
    }

    const revenueResult = self.app.localCache.getRevenueResult(requestKey, cidByCache, hourDiff)

    if (revenueResult) {
      next(null, {
        code: code.OK,
        data: revenueResult,
      })
      return
    }

    m_async.waterfall(
      [
        function (cb) {
          self.app.rpc.config.configRemote.getGameGroup(session, cb) // 遊戲種類
        },
        function (r_code, r_data, cb) {
          game_group = r_data
          let ggIds = r_data.map((item) => {
            return item.GGId
          })
          data["ggIds"] = ggIds.join(",") // DB內的 GGId

          // 取系統主幣別
          var param = { item: "main_currency" }
          configDao.getSystemSetting(param, cb)
        },
        function (r_code, r_data, cb) {
          data["mainCurrency"] = r_data //主幣別

          data["user_level"] = session.get("level")
          data["user_cid"] = session.get("cid")
          data["user_isSub"] = session.get("level") == 1 ? 0 : session.get("isSub")

          // 取交收者幣別
          if (data["user_level"] == 1) {
            cb(
              null,
              {
                code: code.OK,
              },
              data["mainCurrency"]
            )
          } else {
            var userId = 0
            if (session.get("level") == 2) {
              // Reseller
              // 是Reseller的管理者(user): 用hallId, 不是管理者: 用cid
              userId = session.get("isSub") == 1 ? session.get("hallId") : session.get("cid")
              data["user_hallId"] = userId
            } else {
              // Agent
              userId = session.get("isSub") == 1 ? session.get("agentId") : session.get("cid")
              data["user_hallId"] = session.get("hallId")
              data["user_agentId"] = userId
            }
            data["hall_agent_search_Id"] = userId
            userDao.getCusCurrency_byCid(userId, cb)
          }
        },
        function (r_code, r_currency, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.LOAD_CURRENCY_FAIL, data: null })
            return
          }

          data["currency"] = r_currency

          console.log("getContribution_byGame param:", JSON.stringify(data))
          bettingDao.getContribution_byGame(data, cb) // 取當月遊戲類別GGID押注貢獻
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
            return
          }

          game_bet = r_data // 遊戲類別押注貢獻

          console.log("getContribution_byBet param:", JSON.stringify(data))
          bettingDao.getContribution_byBet(data, cb) // 當月廳主Reseller押注貢獻
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
            return
          }

          reseller_bet = r_data // 廳主Reseller押注貢獻

          switch (data["user_level"]) {
            case 1: // Admin
              console.log("getHallList param:", JSON.stringify(data))
              userDao.getHallList(cb) // 取所有 HallId 不含管理者
              break
            default: // Hall & Agent
              cb(null, { code: code.OK }, [data["hall_agent_search_Id"]])
              break
          }
        },
        function (r_code, hallList, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
            return
          }

          let upid = ""
          let j = 0

          switch (data["user_level"]) {
            case 1: // Admin
              data["userCountIds"] = []
              for (let i in hallList) {
                // 找最上層的 Hall 作為樹狀主 key
                if (hallList[i].Upid == "-1") {
                  resellerTree[hallList[i].Cid] = [] // 初始化
                  data["userCountIds"].push(hallList[i].Cid)
                }
              }
              hallList = hallList.filter((item) => item.Upid !== "-1") // 過濾最上層的 Hall

              // 找最上層 Hall 所有的 subReseller
              while (hallList.length > 0) {
                Object.keys(resellerTree).forEach((key) => {
                  for (let i in hallList) {
                    if (resellerTree[key].indexOf(upid) > -1 || hallList[i].Upid == key) {
                      resellerTree[key].push(hallList[i].Cid)
                      data["userCountIds"].push(hallList[i].Cid)
                      hallList.splice(i, 1)
                    }
                  }
                })
                if (hallList.length > 0) {
                  if (j >= hallList.length) j = 0
                  upid = hallList[j].Upid
                  j++
                }
              }
              break
            default: // Hall & Agent
              // Admin: 最上層ResellerIds || Hall & Agent: self cid
              data["userCountIds"] = hallList
              break
          }
          console.log("getContribution_byUserCount param:", JSON.stringify(data))
          bettingDao.getContribution_byUserCount(data, cb) // 當月廳主Reseller人數貢獻
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
            return
          }

          let param = {
            cid: data["hall_agent_search_Id"],
          }

          switch (data["user_level"]) {
            case 1: // Admin
              Object.keys(resellerTree).forEach((key) => {
                let userCount = 0
                for (let item of r_data) {
                  if (key == item.HallId || resellerTree[key].indexOf(item.HallId) > -1) {
                    // 該 item.HallId 是 頂層 Hall || 有存在於 resellerTree 頂層 Hall 內，加總玩家貢獻
                    userCount = utils.number.add(userCount, item.userCount)
                  }
                }
                // 加總完，該頂層 Hall 資料丟入陣列
                reseller_userCount.push({
                  HallId: key,
                  userCount: userCount,
                })
              })

              // Admin 不需找下線
              cb(null, { code: code.OK }, [])
              break
            case 2: // Reseller
              reseller_userCount = r_data // 廳主Reseller人數貢獻

              console.log("getChildList param:", JSON.stringify(param))
              userDao.getChildList(param, cb) // 取得所有 subReseller Id
              break
            case 3:
              reseller_userCount = r_data // 廳主Reseller人數貢獻

              // Agent 不需找下線
              cb(null, { code: code.OK }, [])
              break
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
            return
          }

          // subReseller Id列表
          let r_subResellers = r_data.pop()

          // 沒有subReseller 或 不是Reseller
          if (!r_subResellers) {
            data["subResellers"] = []
            cb(null, { code: code.OK }, [])
            return
          }

          // subReseller Id列表
          data["subResellers"] = r_subResellers[Object.keys(r_subResellers)[0]].split(",")
          data["subResellers"].splice(0, 2) // 刪除第0 & 1筆: '$,rUpjbUmzFkAV(自己),7ejdZ2jNAZvQ, ...'

          console.log("getContribution_byBet (subReseller) param:", JSON.stringify(data))
          bettingDao.getContribution_byBet(data, cb) // 當月subReseller押注貢獻
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
            return
          }

          subReseller_bet = r_data // 下線subReseller押注貢獻

          // 沒有subReseller
          if (data["subResellers"].length === 0) {
            cb(null, { code: code.OK }, [])
            return
          }

          console.log("getContribution_byUserCount (subReseller) param:", JSON.stringify(data))
          bettingDao.getContribution_byUserCount(data, cb) // 當月subReseller人數貢獻
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.REVENUE_LOAD_FAIL, data: null })
            return
          }

          subReseller_userCount = r_data // subReseller人數貢獻

          // 登入者 交收幣別 與 主幣別 相同時,不須找匯率 & 轉換匯率
          if (data["currency"] === data["mainCurrency"]) {
            cb(null, { code: code.OK }, null)
            return
          }

          console.log("currencyExchangeRate param:", JSON.stringify(data))
          gameDao.currencyExchangeRate(data, cb) // 取得登入者幣別匯率
        },
      ],
      function (none, r_code, r_exchangeRate) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.RATE_NOT_VALID, data: null })
          return
        }

        if (r_exchangeRate) {
          if (r_exchangeRate.length > 0) r_exchangeRate = r_exchangeRate[0].CryDef // 匯率
          else {
            // DB 找不到該交收幣別幣別的匯率 (有鬼)
            r_exchangeRate = 0
            logger.error("can not find currency exchangeRate !! currency is : ", data["currency"])
          }
        } else {
          r_exchangeRate = 1 // 不須轉換匯率
        }

        // 遊戲類別押注貢獻
        for (let i in game_bet) {
          let item = {
            GGID: game_bet[i]["GGID"],
            totalBet: utils.number.oneThousand(Number(game_bet[i]["totalBet"].replace(/,/g, "")), consts.Math.DIVIDE),
          }
          // 幣別與登入者交收幣別不同時
          if (r_exchangeRate != 1 && game_bet[i]["betCurrency"] != data["currency"]) {
            item["totalBet"] = utils.number.multiply(item["totalBet"], r_exchangeRate) // 匯率轉換
          }

          if (i > 0) {
            for (let j in gameContribution) {
              // 有找到GGID時
              if (item["GGID"] == gameContribution[j]["GGID"]) {
                gameContribution[j]["totalBet"] = utils.number.add(gameContribution[j]["totalBet"], item["totalBet"]) // 押注加總
                break
              }
              // 找不到相同GGID時
              else if (j == gameContribution.length - 1) {
                gameContribution.push(item)
              }
            }
          } else {
            gameContribution.push(item)
          }
        }

        var key = "UpId"
        var level = 3
        if (data["user_level"] === 1) {
          key = "HallId" // AD
          level = 2
        }

        // Reseller押注貢獻
        for (let i in reseller_bet) {
          let item = {
            Cid: reseller_bet[i]["Cid"],
            userName: reseller_bet[i]["userName"],
            totalBet: utils.number.oneThousand(
              Number(reseller_bet[i]["totalBet"].replace(/,/g, "")),
              consts.Math.DIVIDE
            ),
            level: level,
          }
          // 幣別與登入者交收幣別不同時 // 匯率換算
          if (r_exchangeRate != 1 && reseller_bet[i]["betCurrency"] != data["currency"]) {
            item["totalBet"] = utils.number.multiply(item["totalBet"], r_exchangeRate)
          }

          // 人數貢獻 Part1: Admin->Reseller, Reseller->Agent, Agent->Agent
          for (let ii in reseller_userCount) {
            if (item["Cid"] == reseller_userCount[ii][key]) {
              item["userCount"] = reseller_userCount[ii]["userCount"]
              break
            }
          }

          if (i > 0) {
            for (let j in hallContribution) {
              // 有找到相同cid時 // 押注加總
              if (item["Cid"] == hallContribution[j]["Cid"]) {
                hallContribution[j]["totalBet"] = utils.number.add(hallContribution[j]["totalBet"], item["totalBet"])
                break
              }
              // 找不到相同cid時 // 新增資料
              else if (j == hallContribution.length - 1) {
                hallContribution.push(item)
              }
            }
          } else {
            // 第一筆 // 新增資料
            hallContribution.push(item)
          }
        }
        // subReseller押注貢獻
        for (let i in subReseller_bet) {
          let item = {
            Cid: subReseller_bet[i]["Cid"],
            HallId: subReseller_bet[i]["HallId"],
            userName: subReseller_bet[i]["userName"],
            totalBet: utils.number.oneThousand(
              Number(subReseller_bet[i]["totalBet"].replace(/,/g, "")),
              consts.Math.DIVIDE
            ),
            level: 2,
          }
          // 幣別與登入者交收幣別不同時 // 匯率換算
          if (r_exchangeRate != 1 && subReseller_bet[i]["betCurrency"] != data["currency"]) {
            item["totalBet"] = utils.number.multiply(item["totalBet"], r_exchangeRate)
          }

          // 人數貢獻 Part2: Reseller->subReseller
          for (let ii in subReseller_userCount) {
            if (item["HallId"] == subReseller_userCount[ii]["HallId"]) {
              item["userCount"] = subReseller_userCount[ii]["userCount"]
              break
            }
          }

          if (i > 0) {
            for (let j in subResellerContribution) {
              // 有找到相同cid時 // 押注加總
              if (item["Cid"] == subResellerContribution[j]["Cid"]) {
                subResellerContribution[j]["totalBet"] = utils.number.add(
                  subResellerContribution[j]["totalBet"],
                  item["totalBet"]
                )
                break
              }
              // 找不到相同cid時 // 新增資料
              else if (j == subResellerContribution.length - 1) {
                subResellerContribution.push(item)
              }
            }
          } else {
            // 第一筆 // 新增資料
            subResellerContribution.push(item)
          }
        }

        // 合併 Reseller & subReseller
        hallContribution = hallContribution.concat(subResellerContribution)

        // =========== 取到小數點第二位, 無條件捨去 ===========
        for (let i in gameContribution) {
          var group = game_group.filter((item) => parseInt(item.GGId) == gameContribution[i]["GGID"])
          gameContribution[i]["groupNameE"] = group.length > 0 ? group[0]["NameE"] : ""
          gameContribution[i]["groupNameG"] = group.length > 0 ? group[0]["NameG"] : ""
          gameContribution[i]["groupNameC"] = group.length > 0 ? group[0]["NameC"] : ""
          gameContribution[i]["totalBet"] = utils.number.floor(gameContribution[i]["totalBet"])
        }

        for (let i in hallContribution) {
          hallContribution[i]["totalBet"] = utils.number.floor(hallContribution[i]["totalBet"])
        }
        // ==================================================

        // 查詢結果存入 cache
        let revenueResult = {
          gameContribution,
          hallContribution,
        }

        self.app.localCache.addRevenueResult(requestKey, cidByCache, revenueResult, hourDiff)

        next(null, {
          code: code.OK,
          data: revenueResult,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[betHandler][getContribution] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

/**
 * 向API Server請求砍單
 *
 * @param {object} msg data
 * @param {string} msg.roundId 遊戲回合，相同的單都會被rollback
 * @param {string} msg.playerId 玩家Id
 * @param {string} msg.gameId 遊戲Id
 * @param {*} session pomelo session
 * @param {*} next callback
 * @returns {object} { code, data? }
 */
handler.rollbackWagers = function (msg, session, next) {
  try {
    const logTag = `[betHandler][rollbackWagers]`

    const data = msg.data

    const { roundId, playerId, gameId } = data

    if (isEmpty(roundId) || isEmpty(playerId) || isEmpty(gameId)) {
      next(null, {
        code: code.BET.PARA_DATA_FAIL,
        data: `${logTag} - parameter error , roundId, playerId, gameId are required`,
      })
      return
    }

    const enpointApiWhiteLabel = conf.API_SERVER_URL + consts.APIServerPlatform.whiteLabel

    let widList = []

    // 於DB搜尋該roundId注單，確認注單內容和傳入參數是否一樣

    m_async.waterfall([
      function (cb) {
        bettingDao.getWagersByRoundId(roundId, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK || !r_data || r_data.length === 0) {
          next(null, { code: code.BET.WAGER_NOT_EXIST, data: `${logTag} - wagers not found with roundId ${roundId}` })
          return
        }

        let validateErrorMessage = ""

        for (const wager of r_data) {
          const { GameId, Cid } = wager

          if (GameId.toString() !== gameId.toString()) {
            validateErrorMessage = `${logTag} - Wid ${wager.Wid} gameId ${GameId} is not the same with parameter ${gameId}`
            break
          }

          if (Cid !== playerId) {
            validateErrorMessage = `${logTag} - Wid ${wager.Wid} playerId ${Cid} is not the same with parameter ${playerId}`
            break
          }
        }

        widList = r_data.map((x) => x.Wid)

        if (validateErrorMessage) {
          next(null, { code: code.DB.PARA_FAIL, data: validateErrorMessage })
          return
        }

        logger.warn(`${logTag} - Will request to api for rollback ${widList.join(" , ")}`)

        userDao.get_player_byCid({ cid: playerId }, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK || !r_data || r_data.length === 0) {
          next(null, { code: code.USR.USER_NOT_EXIST, data: `${logTag} - player not found with cid ${playerId}` })
          return
        }

        const dc = r_data[0].DC

        const payload = { method: "rollback", dc, roundId, playerId, gameId }

        requestService.post(enpointApiWhiteLabel, payload, { callback: cb })
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK || !r_data || r_data.status !== "0000") {
          next(null, {
            code: code.REQUEST.REQUEST_FAILED,
            data: `${logTag} - roundId ${roundId} - api response with failed`,
          })
          return
        }

        // 管理員操作紀錄
        const logData = {
          AdminId: session.get("cid") || "",
          UserName: session.get("usrName") || "",
          FunctionGroupL: "System",
          FunctionAction: "WagerRollback",
          ModifiedType: "delete",
          RequestMsg: JSON.stringify(msg),
          Desc_Before: JSON.stringify([{ data: [{ roundId, gameId, playerId }] }]),
          Desc_After: JSON.stringify([{ data: [{ wid: widList }] }]),
          IP: msg.remoteIP,
        }

        logDao.add_log_admin(logData, cb)

        next(null, {
          code: code.OK,
          data: { roundId, playerId, gameId },
        })

        return
      },
    ])
  } catch (err) {
    logger.error("[betHandler][rollbackWagers] catch err", inspect(err))
    next(null, {
      code: code.FAIL,
    })
    return
  }
}

/**
 * 重新結帳指定區間
 *
 * @param {*} msg
 * @param {*} _session
 * @param {*} next
 * @returns
 */
handler.accountingWagers = function (msg, _session, next) {
  const logTag = `[betHandler][accountingWagers]`

  const data = msg.data

  const { accountingDate } = data

  if (isEmpty(accountingDate)) {
    next(null, {
      code: code.BET.PARA_DATA_FAIL,
      data: `${logTag} - parameter error , accountingDate is required`,
    })
    return
  }

  // 限制最多能選擇到90天前
  const maxDays = 90

  const serverTime = timezone.getServerTime()

  const limitStartTime = serverTime.clone().subtract(maxDays, "day").startOf("date").format("YYYY-MM-DD HH:mm:ss")
  const limitEndTime = serverTime.clone().subtract(1, "hour").startOf("hour").format("YYYY-MM-DD HH:mm:ss")

  const isInValidDateRange = moment(accountingDate).isBetween(limitStartTime, limitEndTime, null, "[]")

  if (isInValidDateRange === false) {
    logger.warn(
      `${logTag} - accountingDate ${accountingDate} is not between limitStartTime ${limitStartTime} ~ limitEndTime ${limitEndTime}`
    )

    next(null, {
      code: code.BET.START_TIME_FAIL,
      data: `${logTag} - accountingDate is not in the valid date range`,
    })
    return
  }

  // 只重算該一小時
  const startTime = moment(accountingDate).startOf("hour").format("YYYY-MM-DD HH:mm:ss")
  const endTime = moment(accountingDate).add(1, "day").startOf("hour").format("YYYY-MM-DD HH:mm:ss")

  // 重新結帳
  const dcFunky = "FUNKY"
  m_async.waterfall([
    function (cb) {
      bettingDao.accountingWagers(startTime, endTime, cb)
    },
    function (r_code, r_data, cb) {
      if (r_code.code != code.OK) {
        next(null, { code: code.FAIL })
        return
      }

      // FUNKY 重新結帳

      // 用 dc 找 hallId
      userDao.getUserByDC({ dc: dcFunky, userLevel: 2 }, cb)
    },
    function (r_code, r_data, cb) {
      if (r_code.code != code.OK) {
        next(null, { code: code.FAIL })
        return
      }

      if (r_data.length === 0) {
        next(null, {
          code: code.OK,
          data: { startTime, endTime },
        })
        return // 找不到 FUNKY
      }

      const funkyHallId = r_data[0].Cid

      bettingDao.updateFunkyRevenueReport(
        {
          startTime,
          endTime,
          hallId: funkyHallId,
        },
        cb
      )
    },
    function (r_code) {
      if (r_code.code != code.OK) {
        next(null, { code: code.FAIL })
        return
      }

      next(null, {
        code: code.OK,
        data: { startTime, endTime },
      })
    },
  ])
}
