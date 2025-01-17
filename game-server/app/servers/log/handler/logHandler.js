var logger = require("pomelo-logger").getLogger("logHandler", __filename)
var m_async = require("async")
var code = require("../../../util/code")
var conf = require("../../../../config/js/conf")
var timezone = require("../../../util/timezone")
var logDao = require("../../../DataBase/logDao")
var configDao = require("../../../DataBase/configDao")
var userDao = require("../../../DataBase/userDao")
var gameDao = require("../../../DataBase/gameDao")

var schedule = require("pomelo-schedule")

module.exports = function (app) {
  return new Handler(app)
}

var Handler = function (app) {
  this.app = app
}

var handler = Handler.prototype

handler.set_user_logout = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};

    // m_async.waterfall([
    //
    //     function (cb) {
    //
    //         //登出紀錄
    //         var login_data = {
    //             Cid: session.get('cid'),
    //             UserName: session.get('usrName'),
    //             Level: session.get('level'),
    //             IsSub: (session.get('level') == 1) ? '0' : session.get('isSub'),
    //             LType: "OUT",
    //             LDesc: "logout",
    //             IP: msg.remoteIP,
    //             browser: '',
    //             browser_version: '',
    //             os: '',
    //             os_version: '',
    //             isMobile: 0,
    //             isTablet: 0,
    //             isDesktopDevice:0
    //         };
    //         console.log('-set_user_logout data -', JSON.stringify(login_data));
    //         logDao.add_loginout_log(login_data, cb);
    //
    //     }], function (none, r_code) {
    //         if (r_code.code != code.OK) {
    //             next(null, { code: r_code.code, data: null });
    //         }
    //         next(null, { code: code.OK, data: null });
    //         return;
    //
    //     });

    next(null, { code: code.OK, data: null })
    return
  } catch (err) {
    logger.error("[logHandler][set_user_logout] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getActionLog = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};
    var ttlCount = 0
    var pageCount = 0
    var curPage = 0
    var info = []
    var keyWordSearch = false
    var functionAction = []
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
    if (typeof msg.data.keyWord != "undefined" && msg.data.keyWord != "" && msg.data.keyWord.length < 2) {
      next(null, {
        code: code.LOG.KEYWORD_LIMIT,
      })
      return
    }
    if (typeof msg.data.keyWord != "undefined" && msg.data.keyWord != "" && msg.data.keyWord.length >= 2) {
      keyWordSearch = true
    }

    var data_query = {
      curPage: msg.data.page,
      pageCount: msg.data.pageCount,
      userType: msg.data.userType,
      userName: msg.data.userName,
      start_date: "",
      end_date: "",
      keyWordSearch: keyWordSearch,
      keyWord: msg.data.keyWord,
      operatingFunction: typeof msg.data.operatingFunction !== "undefined" ? msg.data.operatingFunction : "",
      operatingAction: typeof msg.data.operatingAction !== "undefined" ? msg.data.operatingAction : "",
      opCid: 0,
      opLevel: "",
      opIsSub: 0,
      opHallId: 0,
      upId: 0,
      sortKey: msg.data.sortKey,
      sortType: msg.data.sortType,
    }

    if (typeof msg.data.start_date != "undefined" && msg.data.start_date != "") {
      data_query.start_date = msg.data.start_date
    }
    if (typeof msg.data.end_date != "undefined" && msg.data.end_date != "") {
      data_query.end_date = msg.data.end_date
    }
    console.log("------------keyWordSearch-----------------", JSON.stringify(keyWordSearch))
    m_async.waterfall(
      [
        function (cb) {
          if (data_query["keyWordSearch"] == true) {
            //取action名稱
            var param = {
              keyWord: msg.data.keyWord,
            }
            configDao.getFunctionActionName(param, cb)
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.LOG.LOG_ACTION_FAIL,
              data: null,
            })
            return
          }
          //操作紀錄查詢
          if (data_query["keyWordSearch"] == true) {
            console.log("keyWordSearch", JSON.stringify(r_data))
            data_query["searchAction"] = r_data.map((item) => item["functionAction"])
          }

          data_query.opCid = session.get("cid")
          data_query.opLevel = session.get("level")
          data_query.opIsSub = session.get("isSub")
          data_query.opHallId = session.get("hallId")

          if (data_query.opLevel == 1 || data_query.opIsSub == 1) {
            cb(
              null,
              {
                code: code.OK,
              },
              null
            )
          } else {
            var param = {
              level: data_query.opLevel,
              hallId: data_query.opHallId,
              cid: data_query.opCid,
              state: true,
            }
            userDao.getUser_byUpId(param, cb) // 取下線&自身帳號
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.LOG.LOG_ACTION_FAIL,
              data: null,
            })
            return
          }

          data_query["cus_id"] = []
          if (data_query.opIsSub != 1) {
            switch (data_query.opLevel) {
              case 2: // hall
                data_query["cus_id"] = r_data
                  .filter((item) => item.level == 2 || item.level == 3)
                  .map((item) => item.Cid)
                break
              case 3: // Agent
                data_query["cus_id"] = r_data.filter((item) => item.level == 3).map((item) => item.Cid)
                break
            }
          }
          if (data_query.opLevel > 1) {
            data_query["cus_id"].push(data_query.opCid) // 搜尋+上自己的帳號
          }
          logDao.get_action_log(data_query, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.LOG.LOG_ACTION_FAIL,
              data: null,
            })
            return
          }
          ttlCount = r_data.count
          r_data["info"].forEach((item) => {
            if (functionAction.indexOf(item["FunctionAction"]) == -1) functionAction.push(item["FunctionAction"])
            //目前管理員操作紀錄
            if (
              item["FunctionAction"] == "DeleteBlackList" ||
              item["FunctionAction"] == "EditBlackList" ||
              item["FunctionAction"] == "AddBlackList" ||
              item["FunctionAction"] == "ViewBlackList"
            ) {
              item["FunctionGroupL"] = "System"
            }
            if (item["FunctionAction"] == "AddGameRtp") {
              item["FunctionAction"] = "AddGameRTP"
            }
            if (item["FunctionAction"] == "EditGameRtp") {
              item["FunctionAction"] = "EditGameRTP"
            }
            if (item["FunctionAction"] == "ViewGameRtp") {
              item["FunctionAction"] = "ViewGameRTP"
            }
            info.push({
              table: item["tableName"],
              Id: item["Id"],
              cid: item["Cid"],
              userName: item["UserName"],
              userType: item["userLevel"],
              userTypeText: item["type"],
              functionGroup: item["FunctionGroupL"],
              functionAction: item["FunctionAction"],
              modifiedType: item["modifiedType"],
              requestMsg: item["RequestMsg"],
              descBefore: item["Desc_Before"],
              descAfter: item["Desc_After"],
              ModifiedDate: item["ModifiedDate"],
            })
          })
          //取action名稱
          var param = {
            functionAction: functionAction,
          }
          configDao.getFunctionActionName(param, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.LOG.LOG_ACTION_FAIL,
            data: null,
          })
          return
        }
        info.forEach((item, idx) => {
          var action = r_data.filter((action) => action.functionAction == item["functionAction"])
          let functionAction = item["functionAction"]
          let nameE, nameG, nameC
          switch (item["functionAction"]) {
            case "CloseOTP":
              nameE = "CloseOTP"
              nameG = "關閉OTP"
              nameC = "关闭OTP"
              break
            case "ResetCreditsList":
              nameE = "ResetCreditsList"
              nameG = "重置信用額度"
              nameC = "重置信用额度"
              break
            case "RenewCreditsList":
              nameE = "RenewCreditsList"
              nameG = "更新信用額度"
              nameC = "更新信用额度"
              break
            case "Transfer":
              switch (item["functionGroup"]) {
                case "System":
                  functionAction = "TransferLine"
                  nameE = "TransferLine"
                  nameG = "轉線"
                  nameC = "转线"
                  break
                case "User":
                  functionAction = "Transfer"
                  nameE = "Transfer"
                  nameG = "轉帳"
                  nameC = "转帐"
                  break
              }
              break
            default:
              if (action.length > 0) {
                // 顯示權限範本翻譯
                nameE = action[0]["nameE"]
                nameG = action[0]["nameG"]
                nameC = action[0]["nameC"]
              } else {
                // 如果不在權限範本且未設定名稱先顯示 functionAction，之後再新增
                nameE = item["functionAction"]
                nameG = item["functionAction"]
                nameC = item["functionAction"]
              }
              break
          }
          info[idx]["functionAction"] = functionAction
          info[idx]["actionNameE"] = nameE
          info[idx]["actionNameG"] = nameG
          info[idx]["actionNameC"] = nameC
        })
        next(null, {
          code: code.OK,
          data: {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: curPage,
            page_count: pageCount,
            logs: info,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[logHandler][getActionLog] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getOperatingRecordDetail = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.table === "undefined" ||
      typeof msg.data.Id === "undefined"
    ) {
      next(null, { code: code.DB.PARA_FAIL })
      return
    }

    m_async.waterfall(
      [
        function (cb) {
          logDao.getOperatingRecordDetail(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.LOG.LOG_ACTION_FAIL, data: null })
          return
        }

        var info = []
        for (var i in r_data) {
          info.push({
            Id: r_data[i]["Id"],
            cid: r_data[i]["Cid"],
            userName: r_data[i]["UserName"],
            userType: r_data[i]["userLevel"],
            userTypeText: r_data[i]["type"],
            functionGroup: r_data[i]["FunctionGroupL"],
            functionAction:
              r_data[i]["FunctionGroupL"].indexOf("System") > -1 && r_data[i]["FunctionAction"].indexOf("Transfer") > -1
                ? "TransferLine"
                : r_data[i]["FunctionAction"],
            modifiedType: r_data[i]["modifiedType"],
            requestMsg: r_data[i]["RequestMsg"],
            descBefore: r_data[i]["Desc_Before"],
            descAfter: r_data[i]["Desc_After"],
            ModifiedDate: r_data[i]["ModifiedDate"],
          })
        }

        next(null, {
          code: code.OK,
          data: {
            logs: info,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[logHandler][getOperatingRecordDetail] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getUserLoginout = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};

    var ttlCount = 0
    var pageCount = 0
    var curPage = 0
    var users = []
    var userDemoInfo = []
    if (
      typeof msg.data === "undefined" ||
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

    var data_query = {
      curPage: msg.data.page,
      pageCount: msg.data.pageCount,
      userType: msg.data.userType,
      userName: msg.data.userName,
      start_date: "",
      end_date: "",
      opCid: 0,
      opLevel: "",
      opIsSub: 0,
      opHallId: 0,
      upId: 0,
      sortKey: msg.data.sortKey,
      sortType: msg.data.sortType,
    }

    if (typeof msg.data.start_date != "undefined" && msg.data.start_date != "") {
      data_query.start_date = msg.data.start_date
    }
    if (typeof msg.data.end_date != "undefined" && msg.data.end_date != "") {
      data_query.end_date = msg.data.end_date
    }

    m_async.waterfall(
      [
        function (cb) {
          data_query.opCid = session.get("cid")
          data_query.opLevel = session.get("level")
          data_query.opIsSub = session.get("isSub")

          if (data_query.opLevel == 1) {
            cb(
              null,
              {
                code: code.OK,
              },
              null
            )
          } else {
            if (data_query.opIsSub) {
              switch (data_query.opLevel) {
                case 2: // subHall  取 hallId
                  data_query.opHallId = session.get("hallId")
                  break
                case 3: // subAgent  取 upId
                  data_query.upId = msg.data.upId
                  break
              }
            }
            var param = {
              level: data_query.opLevel,
              hallId: data_query.opHallId,
              cid: data_query.opCid,
              isSub: data_query.opIsSub,
              upId: data_query.upId,
            }
            userDao.getUser_byUpId(param, cb) // 取下線&自身帳號
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.LOG.LOG_LOGINOUT_FAIL,
              data: null,
            })
            return
          }
          var cus_id = []
          var player_id = []

          if (data_query.opLevel > 1) {
            player_id = r_data.filter((item) => item.level == 4).map((item) => item.Cid)
            cus_id = r_data.filter((item) => item.level == 2 || item.level == 3).map((item) => item.Cid)
          }
          cus_id.push(data_query.opCid) //管理員登入紀錄新增搜尋自己
          if (data_query.userType == "4") {
            data_query["player_id"] = player_id
            logDao.get_player_loginout(data_query, cb) // 玩家登入紀錄
          } else {
            data_query["cus_id"] = cus_id
            logDao.get_user_loginout(data_query, cb) // 管理員登入紀錄
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.LOG.LOG_LOGINOUT_FAIL,
              data: null,
            })
            return
          }
          users = r_data
          var userId = []
          var adminId = []

          users["info"].forEach((item) => {
            if (userId.indexOf(item["Cid"]) == -1 && item["Level"] != 1) {
              userId.push(item["Cid"])
            }
            if (adminId.indexOf(item["Cid"]) == -1 && item["Level"] == 1) {
              adminId.push(item["Cid"])
            }
          })
          userDao.getUserIsDemo(
            {
              user: userId,
              admin: adminId,
            },
            cb
          )
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.LOG.LOG_LOGINOUT_FAIL,
              data: null,
            })
            return
          }
          userDemoInfo = r_data
          var gamesId = []
          if (data_query.userType == "4") {
            users["info"].forEach((item) => {
              if (gamesId.indexOf(item["GameId"].toString()) == -1) gamesId.push(item["GameId"].toString())
            })
          }

          gameDao.getGameName_byId(
            {
              gameId: gamesId,
            },
            cb
          ) //取遊戲名稱清單
        },
      ],
      function (none, r_code, r_data) {
        ttlCount = users.count
        pageCount = msg.data.pageCount
        curPage = msg.data.page

        var info = []
        users["info"].forEach((item) => {
          var userInfo = userDemoInfo.filter(
            (user) => user.Cid.toString() == item["Cid"].toString() && user.IsAg.toString() == item["Level"].toString()
          )

          info.push({
            Id: item["Id"],
            cid: item["Cid"],
            userName: item["UserName"],
            userType: item["Level"],
            userTypeText: item["type"],
            isSub: item["IsSub"],
            actionType: item["LType"],
            actionDate: item["actionDate"],
            isDemo: userInfo.length > 0 ? userInfo[0]["IsDemo"] : "-", //類型
            state: userInfo.length > 0 ? userInfo[0]["State"] : "-",
            browser: item["browser"],
            os: item["os"],
            device: item["device"],
            gameId: item["GameId"],
            IP: item["IP"],
          })
        })

        for (i in info) {
          var gameInfo = r_data.filter((item) => item.GameId == info[i]["gameId"])
          info[i]["nameE"] = gameInfo.length > 0 ? gameInfo[0]["NameE"] : ""
          info[i]["nameG"] = gameInfo.length > 0 ? gameInfo[0]["NameG"] : ""
          info[i]["nameC"] = gameInfo.length > 0 ? gameInfo[0]["NameC"] : ""
        }
        next(null, {
          code: code.OK,
          data: {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: curPage,
            page_count: pageCount,
            logs: info,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[logHandler][getUserLoginout] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//取線上
// handler.getOnlineUsers = function (msg, session, next) {

//     var self = this;
//     //var userSession = {};
//     var sys_config = self.app.get('sys_config');

//     if (typeof msg.data === 'undefined' || typeof msg.data.start_date === 'undefined' || typeof msg.data.end_date === 'undefined') {
//         next(null, { code: code.DB.PARA_FAIL });
//         return;
//     }
//     // (撈取DB的起訖時間) - SERVER 時間
//     var utc_start_date = msg.data.start_date;
//     var utc_end_date = msg.data.end_date;

//     var server_start_date = timezone.UTCToLocal(msg.data.start_date);
//     var server_end_date = timezone.UTCToLocal(msg.data.end_date);

//     var utc_start_time = new Date(utc_start_date.replace(/-/g, '/')).getTime();
//     var server_start_time = new Date(server_start_date.replace(/-/g, '/')).getTime();

//     var serverTimeDiff = (utc_start_time - server_start_time) / 60 / 60 / 1000; //(UTC-server)時差,單位:小時

//     var data_query = {
//         server_start_date: server_start_date,
//         server_end_date: server_end_date,
//         opCid: 0,
//         opLevel: '',
//         opIsSub: 0,
//         opHallId: 0
//     };

//     m_async.waterfall([
//         function (cb) {
//             data_query.opCid = session.get('cid');
//             data_query.opLevel = session.get('level');
//             data_query.opIsSub = session.get('level') == 2 ? session.get('isSub') : 0;

//             if (data_query.opIsSub && data_query.opLevel == 2) {  // subhall  取 hallId
//                 data_query.opHallId = session.get('hallId');
//             }

//             var user_set_param = {
//                 Cid: session.get('cid'),
//                 IsAdmin: (session.get('level') == 1) ? 1 : 0
//             }

//             userDao.get_user_setting_byCid( user_set_param, cb);  //取loginUser的時差
//         }, function (r_code, r_data, cb) {

//             if (r_code.code != code.OK) {
//                 next(null, { code: code.USR.LOAD_SETTING_FAIL, data: null });
//                 return;
//             }

//             var clientTimeDiff = (r_data.length > 0) ? r_data[0]['hourDiff'] : sys_config.time_diff_hour; //無資料->預設美東
//             var totalTimeDiff = parseInt(clientTimeDiff) + parseInt(serverTimeDiff);

//             data_query['timeDiff'] = totalTimeDiff;

//            if(data.opLevel==1){ //admin 不限
//                 cb(null,{code:code.OK},null );
//            }else{
//                userDao.getDownPlayers(data_query,cb); //先取user
//            }
//         },function (r_code, r_data, cb) {

//             if (r_code.code != code.OK) {
//                 next(null, { code: code.USR.LOAD_SETTING_FAIL, data: null });
//                 return;
//             }
//             if(data.opLevel!=1) data_query['players'] = r_data.map(item=>item.Cid);

//             logDao.get_onlineUser(data_query, cb);

//         }], function (none, r_code, r_data) {
//             if (r_code.code != code.OK) {
//                 next(null, { code: code.LOG.LOAD_ONLINE_USER_FAIL, data: null });
//                 return;
//             }

//             var client_start_time = new Date(msg.data.start_date).getTime() + parseInt(data_query['timeDiff']) * 60 * 60 * 1000; //client時間,單位:毫秒
//             var client_end_time = new Date(msg.data.end_date).getTime() + parseInt(data_query['timeDiff']) * 60 * 60 * 1000; //client時間,單位:毫秒

//             var diffDays = ((client_end_time - client_start_time) / (86400 * 1000)).toFixed();

//             var users = [];
//             for (var i = 0; i < diffDays; i++) {

//                 var showDay = new Date(client_start_time + i * 24 * 60 * 60 * 1000);
//                 var client_date = timezone.setTime(showDay).substr(0, 10);
//                 var date_info = r_data.filter(item => item.client_local_date == client_date);
//                 var user = { client_local_date: client_date, count: 0 };

//                 if (date_info.length > 0) {
//                     user['count'] = date_info[0]['count'];
//                 }
//                 users.push(user);
//             }

//             next(null, {
//                 code: code.OK, data: {
//                     users: users
//                 }
//             });
//             return;
//         });
// }

handler.getActionLogType = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};

    m_async.waterfall(
      [
        function (cb) {
          configDao.getActionType(cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.LOG.LOAD_ACTION_TYPE_FAIL, data: null })
          return
        }
        var mainAuth = {}
        r_data.forEach((item) => {
          if (typeof mainAuth[item.FunctionGroupL] == "undefined") {
            mainAuth[item.FunctionGroupL] = Object.assign([])
          }
          var subAuth = {
            action: item.FunctionAction,
            nameE: item.nameE,
            nameG: item.nameG,
            nameC: item.nameC,
          }
          mainAuth[item.FunctionGroupL].push(subAuth)
        })
        console.log("-mainAuth-", JSON.stringify(mainAuth))

        next(null, { code: code.OK, data: mainAuth })
        return
      }
    )
  } catch (err) {
    logger.error("[logHandler][getActionLogType] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//線上玩家數
handler.getOnlinePlayers = function (msg, session, next) {
  var self = this
  let downHallCid = [] //分銷商底下cid包含自己
  m_async.waterfall(
    [
      function (cb) {
        //只有分銷商才需要做查詢底下分銷商行為
        if (session.get("level") == 2 && session.get("isSub") == 0) {
          const param = { cid: session.get("cid") }
          userDao.getChildList(param, cb)
        } else if (session.get("level") == 2 && session.get("isSub") == 1) {
          //分銷商管理員需要處理
          const param = { cid: session.get("hallId") }
          userDao.getChildList(param, cb)
        } else {
          cb(null, { code: code.OK }, null)
        }
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
          return
        }
        let Cid = ""
        if (session.get("level") == 2) {
          let downLine = r_data.pop()
          downHallCid = downLine[Object.keys(downLine)[0]].split(",")
          downHallCid.splice(0, 1) // 刪除第0 & 1筆: '$,rUpjbUmzFkAV(自己),7ejdZ2jNAZvQ, ...'
          Cid = downHallCid
        } else {
          if (session.get("level") == 3 && session.get("isSub") == 0) {
            //代理帳號
            Cid = session.get("cid")
          } else {
            //代理管理員 admin
            Cid = session.get("agentId")
          }
        }
        var param = {
          level: session.get("level"),
          cid: Cid,
        }

        logDao.getOnlinePlayerNumsFromCustomer(param, cb)
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code != code.OK) {
        next(null, {
          code: code.LOG.LOAD_PLAYERS_NUM_FAIL,
          data: null,
        })
        return
      }

      next(null, {
        code: code.OK,
        data: r_data[0],
      })
      return
    }
  )
}

// 開洗分紀錄
handler.getQuotaLog = function (msg, session, next) {
  try {
    var self = this
    var ttlCount = 0
    var pageCount = 0
    var curPage = 0
    var userCid = []
    var gameId = []
    var game_data = []
    var info = []
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
      msg.data["isPage"] = true // 預設-有分頁
    }
    // 無分頁
    if (msg.data["isPage"] === false) {
      if (typeof msg.data.finish === "undefined") {
        msg.data["finish"] = 0
      } // 0:未完成
      if (typeof msg.data.index === "undefined") {
        msg.data["index"] = 0
      } // 開始第N筆
      msg.data["pageCount"] = 1000 //每次取得筆數
    }

    // 排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "AddDate"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    var data_query = {
      curPage: msg.data.page,
      pageCount: msg.data.pageCount,
      userName: msg.data.userName,
      start_date: "",
      end_date: "",
      gameId: msg.data.gameId,
      gameName: msg.data.gameName,
      sortKey: sortKey,
      sortType: sortType,
      isPage: msg.data["isPage"],
      index: msg.data["index"],
      // currency: (typeof msg.data.currency !== 'undefined') ? msg.data.currency : ''
      opCid: 0,
      opLevel: "",
      opIsSub: 0,
      opHallId: 0,
      upId: 0,
    }

    if (typeof msg.data.start_date != "undefined" && msg.data.start_date !== "") {
      data_query.start_date = msg.data.start_date
    }
    if (typeof msg.data.end_date != "undefined" && msg.data.end_date !== "") {
      data_query.end_date = msg.data.end_date
    }
    console.log("------------keyWordSearch-----------------", JSON.stringify(data_query))
    const level = session.get("level")
    let search_game = false
    m_async.waterfall(
      [
        function (cb) {
          //平台交易紀錄新增遊戲名稱搜尋
          if (typeof data_query.gameName != "undefined" && data_query.gameName != "") {
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
              gameName: data_query.gameName,
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
          data_query["search_game"] = search_game
          if (search_game == true) {
            //有使用遊戲搜尋
            data_query["search_gameId"] = r_data.map((item) => item.GameId)
          }
          data_query.opCid = session.get("cid")
          data_query.opLevel = session.get("level")
          data_query.opIsSub = session.get("isSub")

          if (data_query.opLevel == 1) {
            cb(
              null,
              {
                code: code.OK,
              },
              null
            )
          } else {
            if (data_query.opIsSub) {
              switch (data_query.opLevel) {
                case 2: // subHall  取 hallId
                  data_query.opHallId = session.get("hallId")
                  break
                case 3: // subAgent  取 upId
                  data_query.upId = msg.data.upId
                  break
              }
            }
            var param = {
              level: data_query.opLevel,
              hallId: data_query.opHallId,
              cid: data_query.opCid,
              isSub: data_query.opIsSub,
              upId: data_query.upId,
            }
            userDao.getUser_byUpId(param, cb) // 取下線&自身帳號
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.LOG.LOG_LOGINOUT_FAIL,
              data: null,
            })
            return
          }
          data_query["player_id"] = []
          if (data_query.opLevel > 1) {
            data_query["player_id"] = r_data.filter((item) => item.level == 4).map((item) => item.Cid)
          }
          logDao.get_quota_action_log(data_query, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code !== code.OK) {
            next(null, {
              code: code.LOG.LOG_ACTION_FAIL,
              data: null,
            })
            return
          }

          ttlCount = r_data.count
          r_data["info"].forEach((item) => {
            if (gameId.indexOf(item["GameId"].toString()) === -1) {
              gameId.push(item["GameId"].toString())
            }

            if (userCid.indexOf(item["Cid"].toString()) === -1) {
              userCid.push(item["Cid"].toString())
            }

            info.push({
              Id: item["Id"],
              Cid: item["Cid"],
              GameId: item["GameId"],
              OldQuota: item["OldQuota"],
              NewQuota: item["NewQuota"],
              Amount: item["Amount"],
              Currency: item["Currency"],
              CryDef: item["CryDef"],
              IP: item["IP"],
              LDesc: item["LDesc"],
              AddDate: item["AddDate"],
            })
          })

          gameDao.getGameName_byId({ gameId: gameId }, cb) //取遊戲名稱
        },
        function (r_code, r_data, cb) {
          game_data = r_data

          var param = {
            level: 4,
            userId: userCid,
          }
          userDao.getUser_byId(param, cb) // 取user 帳號
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code !== code.OK) {
          next(null, {
            code: code.LOG.LOG_ACTION_FAIL,
            data: null,
          })
          return
        }

        for (i in info) {
          var gameInfo = game_data.filter((item) => item.GameId === info[i]["GameId"])
          info[i]["nameE"] = gameInfo.length > 0 ? gameInfo[0]["NameE"] : ""
          info[i]["nameG"] = gameInfo.length > 0 ? gameInfo[0]["NameG"] : ""
          info[i]["nameC"] = gameInfo.length > 0 ? gameInfo[0]["NameC"] : ""
          info[i]["groupNameE"] = gameInfo.length > 0 ? gameInfo[0]["groupE"] : ""
          info[i]["groupNameG"] = gameInfo.length > 0 ? gameInfo[0]["groupG"] : ""
          info[i]["groupNameC"] = gameInfo.length > 0 ? gameInfo[0]["groupC"] : ""

          var userData = r_data.filter((item) => item.Cid === info[i]["Cid"])
          info[i]["UserName"] = userData.length > 0 ? userData[0]["UserName"] : ""
          info[i]["IsDemo"] = userData.length > 0 ? userData[0]["IsDemo"] : ""
        }

        var data = {}
        if (msg.data.isPage === false) {
          // 無分頁
          var finish = ttlCount === msg.data["index"] + info.length ? 1 : 0
          var index = finish === 1 ? null : msg.data["index"] + info.length
          data = {
            totalCount: ttlCount,
            finish: finish,
            index: index,
            logs: info,
            sortKey: sortKey,
            sortType: sortType,
          }
        } else {
          // 有分頁
          data = {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: curPage,
            page_count: pageCount,
            logs: info,
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
    logger.error("[logHandler][getQuotaLog] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 伺服器紀錄查詢
handler.getServerActionLog = function (msg, session, next) {
  try {
    var self = this
    var ttlCount = 0
    var pageCount = 0
    var curPage = 0
    var info = []
    var keyWordSearch = false
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
    if (typeof msg.data.keyWord != "undefined" && msg.data.keyWord != "" && msg.data.keyWord.length < 2) {
      next(null, {
        code: code.LOG.KEYWORD_LIMIT,
      })
      return
    }
    if (typeof msg.data.keyWord != "undefined" && msg.data.keyWord != "" && msg.data.keyWord.length >= 2) {
      keyWordSearch = true
    }

    var data_query = {
      curPage: msg.data.page,
      pageCount: msg.data.pageCount,
      userType: msg.data.userType,
      userName: msg.data.userName,
      start_date: "",
      end_date: "",
      gameId: msg.data.gameId,
      gameName: msg.data.gameName,
      keyWordSearch: keyWordSearch,
      keyWord: msg.data.keyWord,
      opCid: 0,
      opLevel: "",
      opIsSub: 0,
      opHallId: 0,
      upId: 0,
      sortKey: msg.data.sortKey,
      sortType: msg.data.sortType,
    }

    if (typeof msg.data.start_date != "undefined" && msg.data.start_date != "") {
      data_query.start_date = msg.data.start_date
    }
    if (typeof msg.data.end_date != "undefined" && msg.data.end_date != "") {
      data_query.end_date = msg.data.end_date
    }
    let search_game = false
    const level = session.get("level")
    console.log("------------keyWordSearch-----------------", JSON.stringify(keyWordSearch))
    m_async.waterfall(
      [
        function (cb) {
          //伺服器操作紀錄新增遊戲名稱搜尋
          if (typeof data_query.gameName != "undefined" && data_query.gameName != "") {
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
              gameName: data_query.gameName,
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
          data_query["search_game"] = search_game
          if (search_game == true) {
            //有使用遊戲搜尋
            data_query["search_gameId"] = r_data.map((item) => item.GameId)
          }
          data_query.opCid = session.get("cid")
          data_query.opLevel = session.get("level")
          data_query.opIsSub = session.get("isSub")
          data_query.opHallId = session.get("hallId")

          if (data_query.opLevel == 1 || data_query.opIsSub == 1) {
            cb(
              null,
              {
                code: code.OK,
              },
              null
            )
          } else {
            var param = {
              level: data_query.opLevel,
              hallId: data_query.opHallId,
              cid: data_query.opCid,
              state: true,
            }
            userDao.getUser_byUpId(param, cb) // 取下線&自身帳號
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.LOG.LOG_ACTION_FAIL,
              data: null,
            })
            return
          }

          data_query["cus_id"] = []
          if (data_query.opIsSub != 1) {
            switch (data_query.opLevel) {
              case 2: // hall
                data_query["cus_id"] = r_data
                  .filter((item) => item.level == 2 || item.level == 3)
                  .map((item) => item.Cid)
                break
              case 3: // Agent
                data_query["cus_id"] = r_data.filter((item) => item.level == 3).map((item) => item.Cid)
                break
            }
          }
          if (data_query.opLevel > 1) {
            data_query["cus_id"].push(data_query.opCid) // 搜尋+上自己的帳號
          }
          logDao.get_server_action_log(data_query, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.LOG.LOG_ACTION_FAIL,
              data: null,
            })
            return
          }

          var gameIds = []
          ttlCount = r_data.count
          r_data["info"].forEach((item) => {
            if (gameIds.indexOf(item["GameId"].toString()) == -1) gameIds.push(item["GameId"].toString())
            info.push({
              table: item["tableName"],
              Id: item["Id"],
              cid: item["Cid"],
              userName: item["UserName"],
              actionServer: item["ActionServer"],
              action: item["Action"],
              gameId: item["GameId"],
              ModifiedDate: item["ModifiedDate"],
            })
          })

          gameDao.getGameName_byId({ gameId: gameIds }, cb) //取遊戲名稱清單
        },
      ],
      function (none, r_code, r_data) {
        for (i in info) {
          var gameInfo = r_data.filter((item) => item.GameId == info[i]["gameId"])
          info[i]["nameE"] = gameInfo.length > 0 ? gameInfo[0]["NameE"] : ""
          info[i]["nameG"] = gameInfo.length > 0 ? gameInfo[0]["NameG"] : ""
          info[i]["nameC"] = gameInfo.length > 0 ? gameInfo[0]["NameC"] : ""
        }

        next(null, {
          code: code.OK,
          data: {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: curPage,
            page_count: pageCount,
            logs: info,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[logHandler][getServerActionLog] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 伺服器紀錄明細查詢
handler.getServerRecordDetail = function (msg, session, next) {
  try {
    var self = this
    var info = []

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.table === "undefined" ||
      typeof msg.data.Id === "undefined"
    ) {
      next(null, { code: code.DB.PARA_FAIL })
      return
    }

    m_async.waterfall(
      [
        function (cb) {
          logDao.getServerRecordDetail(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.LOG.LOG_ACTION_FAIL, data: null })
            return
          }

          for (var i in r_data) {
            info.push({
              Id: r_data[i]["Id"],
              cid: r_data[i]["Cid"],
              userName: r_data[i]["UserName"],
              gameId: r_data[i]["GameId"],
              actionServer: r_data[i]["ActionServer"],
              action: r_data[i]["Action"],
              modifiedType: r_data[i]["modifiedType"],
              descBefore: r_data[i]["Desc_Before"],
              descAfter: r_data[i]["Desc_After"],
              ModifiedDate: r_data[i]["ModifiedDate"],
            })
          }

          var gameIds = []
          if (gameIds.indexOf(r_data.GameId) == -1) gameIds.push(r_data.GameId)
          gameDao.getGameName_byId({ gameId: gameIds }, cb) //取遊戲名稱清單
        },
      ],
      function (none, r_code, r_data) {
        for (i in info) {
          var gameInfo = r_data.filter((item) => item.GameId == info[i]["gameId"])
          info[i]["nameE"] = gameInfo.length > 0 ? gameInfo[0]["NameE"] : ""
          info[i]["nameG"] = gameInfo.length > 0 ? gameInfo[0]["NameG"] : ""
          info[i]["nameC"] = gameInfo.length > 0 ? gameInfo[0]["NameC"] : ""
        }

        next(null, {
          code: code.OK,
          data: {
            logs: info,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[logHandler][getServerRecordDetail] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
