var async = require("async")
var code = require("../../../util/code")
var conf = require("../../../../config/js/conf")
var m_md5 = require("md5")
var APICode = require("../../../util/APICode")
const crypto = require("crypto")
var timezone = require("../../../util/timezone")
var agentAPIDao = require("../../../DataBase/agentAPI_Dao")
var sprintf = require("sprintf-js").sprintf
const short = require("short-uuid")
var pomelo = require("pomelo")

module.exports = function (app) {
  return new Handler(app)
}

var Handler = function (app) {
  this.app = app
}

var handler = Handler.prototype

/*
token驗證
-agentname
-token
*/
handler.agent = function (msg, session, next) {
  var self = this
  var param = msg.data

  var before_execution_time = new Date().getTime()

  if (
    typeof param.agentname == "undefined" ||
    param.agentname == "" ||
    typeof param.token == "undefined" ||
    param.token == "" ||
    typeof param.method == "undefined" ||
    param.method == ""
  ) {
    next(null, { error_code: APICode.MISS_PARAM, error_message: "" })
    return
  }

  if (typeof param.method == "undefined" || param.method == "") {
    next(null, { error_code: APICode.MISS_METHOD, error_message: "" })
    return
  }
  // menthod 有 斜線的 取代掉
  param.method = param.method.replace("/", "_")
  async.waterfall(
    [
      function (cb) {
        agentAPIDao.getHallInfo(param, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          //錯誤
          console.log("error sql")
          next(null, { error_code: APICode.MISS_PARAM, error_message: "" })
          return
        }
        if (r_data.length == 0) {
          //查無資料
          console.log("error user")
          next(null, { error_code: APICode.MISS_PARAM, error_message: "" })
          return
        }
        var user = r_data[0]
        if (user.State != "N") {
          //停用或凍結
          console.log("error state")
          next(null, { error_code: APICode.MISS_PARAM, error_message: "" })
          return
        }
        if (user.Token != param.token) {
          //停用或凍結
          console.log("error token")
          next(null, { error_code: APICode.WRONG_TOKEN, error_message: "" })
          return
        }

        param["hall_id"] = user.Cid
        //--------------------------白名單--------------------------------

        //---------------------------白名單--------------------------------

        cb(null, { code: code.OK }, null)
      },
    ],
    function (none, r_code, r_data) {
      //各自method
      if (typeof self[param.method] == "function") {
        console.log("-------enter function ----------------", param.method)

        self[param.method](param, session, function (r_data) {
          var execution_time = new Date().getTime() - before_execution_time
          var res = r_data
          //res
          if (r_data.error_code == 0) {
            //執行成功
            res["error_message"] = null
            res["execution_time"] = execution_time + " ms" //執行時間
          } else {
            //錯誤
            res["error_message"] = ""
          }
          next(null, res)
        })
      } else {
        console.log("error funciton")
        next(null, {
          error_code: APICode.NO_METHOD_FOUND,
          error_message: "",
        })
        return
      }
    }
  )
}

//-------------------------------- PLAYER -------------------------------------------
/*
建立玩家
username M
alias M
currency M 
*/
handler.player = function (msg, session, next) {
  console.log("----------player------------------", JSON.stringify(msg))

  var param = msg

  if (
    typeof param.username == "undefined" ||
    param.username == "" ||
    typeof param.alias == "undefined" ||
    param.alias == "" ||
    typeof param.currency == "undefined" ||
    param.currency == ""
  ) {
    next({ error_code: APICode.MISS_PARAM })
    return
  }
  //------------------start---------------------驗證玩家帳號名稱是否符合

  //------------------end---------------------驗證玩家帳號名稱是否符合

  var agentInfo = {}
  var playerId = short.generate()
  var playerInfo = {}
  async.waterfall(
    [
      function (cb) {
        //找代理
        agentAPIDao.getAgentInfo(param, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          console.log("error sql")
          next({ error_code: APICode.WRONG_AGENT })
          return
        }

        if (r_data.length == 0) {
          console.log("error agent")
          next({ error_code: APICode.WRONG_AGENT })
          return
        }

        agentInfo = r_data[0]

        playerInfo = {
          Cid: playerId,
          UserName: param.username,
          Passwd: m_md5(param.username),
          NickName: param.alias,
          AddDate: timezone.serverTime(),
          IsAg: 4,
          Upid: agentInfo["Upid"],
          HallId: agentInfo["HallId"],
          IsDemo: agentInfo["IsDemo"],
          IsSingleWallet: agentInfo["IsSingleWallet"],
          Currency: agentInfo["Currency"],
          State: "N",
        }

        //查詢有無 重複玩家
        agentAPIDao.checkPlayerIsExist(param, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          console.log("error sql")
          next({ error_code: APICode._03_DUPLICATE_USERNAME })
          return
        }

        if (r_data[0]["num"] > 0) {
          console.log("玩家帳號相同")
          next({ error_code: APICode._03_DUPLICATE_USERNAME })
          return
        }

        agentAPIDao.createPlayer(playerInfo, cb)
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code != code.OK) {
        console.log("error sql")
        next({ error_code: APICode._03_DUPLICATE_USERNAME })
        return
      }

      var res_data = {
        status: 0,
        username: playerInfo["UserName"],
        alias: playerInfo["NickName"],
        sessionId: "",
        currency: playerInfo["Currency"],
        parentid: playerInfo["HallId"],
        createat: playerInfo["AddDate"], //美東時間
        userid: playerInfo["Cid"],
      }

      next({ error_code: 0, data: res_data })
      return
    }
  )
}
/**
 * 玩家是否在玩釣魚機
 * username M 玩家帳號
 */
handler.player_fishing = function (msg, session, next) {
  var param = msg
  var playerInfo = {}
  if (typeof param.username == "undefined" || param.username == "") {
    next({
      error_code: APICode.MISS_PARAM,
    })
    return
  }

  async.waterfall(
    [
      function (cb) {
        agentAPIDao.getPlayerInfo(param, cb) //查詢有無此帳號
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next({
            error_code: APICode._03_NO_PLAYER_FOUND,
          })
          return
        }

        if (r_data.length == 0) {
          //查無資料
          next({
            error_code: APICode._03_NO_PLAYER_FOUND,
          })
          return
        }
        playerInfo = r_data

        cb(null, { code: code.OK }, playerInfo)
      },
    ],
    function (none, r_code, r_data) {
      var res_data = {
        username: playerInfo["Cid"],
        playing: true,
      }

      next({ error_code: 0, data: res_data })
      return
    }
  )
}

//--------------------------------GAME-------------------------------------------
/**
 * 遊戲清單
 * active O ,true:只玩有效的遊戲/false:無效的遊戲 ,空值:全部
 * (1:Jackpot / 2:Slots / 3:table Games / 4:Scratch Cards / 5:Fishing)
 */

handler.game_list = function (msg, session, next) {
  var param = msg
  async.waterfall(
    [
      function (cb) {
        agentAPIDao.getGameList(param, cb) //查詢有無此帳號
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code != code.OK) {
        next({
          error_code: APICode.MISS_PARAM,
        })
        return
      }
      var res_data = r_data.map((item) => {
        return {
          gameid: item.ProjectId,
          gamename: item.NameE,
          active: item.Sw == 1 ? true : false,
          flashcode: "",
          htmlcode: "",
          jpcode: "",
          betlogcode: "",
          dictcode: "",
          category: item.Category,
          createdat: item.CreateDate,
        }
      })

      next({ error_code: 0, data: res_data })
      return
    }
  )
}

/**
 * 遊戲連結
 * active O ,true:只玩有效的遊戲/false:無效的遊戲 ,空值:全部
 * (1:Jackpot / 2:Slots / 3:table Games / 4:Scratch Cards / 5:Fishing)
 */

handler.game_list = function (msg, session, next) {
  var param = msg
  async.waterfall(
    [
      function (cb) {
        agentAPIDao.getGameList(param, cb) //查詢有無此帳號
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code != code.OK) {
        next({
          error_code: APICode.MISS_PARAM,
        })
        return
      }
      var res_data = r_data.map((item) => {
        return {
          gameid: item.ProjectId,
          gamename: item.NameE,
          active: item.Sw == 1 ? true : false,
          flashcode: "",
          htmlcode: "",
          jpcode: "",
          betlogcode: "",
          dictcode: "",
          category: item.Category,
          createdat: item.CreateDate,
        }
      })

      next({ error_code: 0, data: res_data })
      return
    }
  )
}

//------------------------cash-------------------------------------

/**
 * cash/agent_transfer
 */
handler.cash_agent_transfer = function (msg, session, next) {}
/**
 * cash/player_transfer
 */
handler.cash_player_transfer = function (msg, session, next) {}
/**
 * cash/check_transfer
 */
handler.cash_check_transfer = function (msg, session, next) {}
/**
 * cash/agent_balance
 */
handler.cash_agent_balance = function (msg, session, next) {}
/**
 * wagers/bet
 */
handler.wagers_bet = function (msg, session, next) {}
/**
 * cash/player_balance
 */
handler.cash_player_balance = function (msg, session, next) {}

//------------------------wagers-------------------------------------
/**
 * wagers/bet
 */
handler.wagers_bet = function (msg, session, next) {}
/**
 *wagers/modified
 */
handler.wagers_modified = function (msg, session, next) {}
/**
 *wagers/min_report
 */
handler.wagers_min_report = function (msg, session, next) {}
/**
 *wagers/report
 */
handler.wagers_report = function (msg, session, next) {}
/**
 *wagers/unfinished
 */
handler.wagers_unfinished = function (msg, session, next) {}
/**
 *wagers/onewallet_unfinished
 */
handler.wagers_onewallet_unfinished = function (msg, session, next) {}
/**
 *wagers/detail_link
 */
handler.wagers_detail_link = function (msg, session, next) {}
//------------------------JACKPOT-------------------------------------
/**
 * jackpot/history 歷史清單
 */
handler.jackpot_history = function (msg, session, next) {}
