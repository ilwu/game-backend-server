var logger = require("pomelo-logger").getLogger("adminHandler", __filename)
var m_async = require("async")
var m_otplib = require("otplib")
var m_md5 = require("md5")
var code = require("../../../util/code")
var conf = require("../../../../config/js/conf")
var timezone = require("../../../util/timezone")
var adminDao = require("../../../DataBase/adminDao")
var userDao = require("../../../DataBase/userDao")
var configDao = require("../../../DataBase/configDao")
var logDao = require("../../../DataBase/logDao")
var gameDao = require("../../../DataBase/gameDao")
var shared = require("../../../util/sharedFunc")
var utils = require("../../../util/utils")
var consts = require("../../../share/consts")

const { inspect } = require("util")

const { default: ShortUniqueId } = require("short-unique-id")
const uid = new ShortUniqueId()

var mailUtil = require("../../../util/mail")
var mailService = require("../../../services/mailService")
var mail = new mailService()

var Authoritylist = []
var Currencies = []
var Levels = []
var otp_token_status = false //判斷OTP狀態
var ip_check_status = false //IP確認暫close

module.exports = function (app) {
  return new Handler(app)
}

var Handler = function (app) {
  this.app = app
}

var handler = Handler.prototype
// handler.getAdminAuth = function (msg, session, next) {

//     var self = this;
//     var token = msg.token;
//     var usrData = {};
//     var data = {};
//     var sys_config = self.app.get('sys_config');
//     var cid = session.get('cid'); //adminId
//     var usrName = session.get('usrName'); //帳號
//     var level = session.get('level'); //層級

//     m_async.waterfall([
//             function (cb) {
//                 adminDao.getOperator_byId(cid, cb);
//             },
//             function (r_code, r_data, cb) {

//                 if (r_code.code != code.OK) {
//                     next(null, {
//                         code: code.USR.USER_NOT_EXIST,
//                         data: null
//                     });
//                     return;
//                 }
//                 data = r_data;
//                 usrData['AdminId'] = data.AdminId;
//                 usrData['UserName'] = data.UserName;
//                 usrData['NickName'] = data.NickName;
//                 usrData['firstLogin'] = data.FirstLogin;
//                 usrData['Currency'] = sys_config.main_currency; //系統主幣別

//                 self.app.rpc.config.configRemote.getAuthority(session, {
//                     authTmpId: data.AuthorityTemplateID
//                 }, cb); //權限
//             },
//             function (r_code, r_authFunc, cb) {
//                 if (r_code.code != code.OK) {
//                     next(null, {
//                         code: code.AUTH.AUTH_INVALID,
//                         data: null
//                     });
//                     return;
//                 }
//                 usrData['Authority'] = r_authFunc;

//             //     self.app.rpc.config.configRemote.getSysLimitTime(session, cb); //系統設定時間
//             // },
//             // function (r_code, r_time, cb) {

//             //     if (r_code.code != code.OK) {
//             //         next(null, {
//             //             code: code.CONFIG.LOAD_LIMIT_TIME_FAIL,
//             //             data: null
//             //         });
//             //         return;
//             //     }
//                 usrData['TimeoutRangeSec'] = sys_config.timeout_range_sec;;
//                 self.app.rpc.config.configRemote.getSysSearchDays(session, cb); //系統天數搜尋
//             },
//             function (r_code, r_data, cb) {

//                 if (r_code.code != code.OK) {
//                     next(null, {
//                         code: code.USR.REARCH_DATE_LIST_FAIL,
//                         data: null
//                     });
//                     return;
//                 }
//                 usrData['SysSearchDays'] = r_data;
//                 var user_set_param = {
//                     Cid: data.AdminId,
//                     IsAdmin: 1
//                 }
//                 userDao.get_user_setting_byCid( user_set_param, cb); //取user_setting
//             },
//             function (r_code, r_data, cb) {

//                 if (r_code.code != code.OK) {
//                     next(null, {
//                         code: code.USR.LOAD_SETTING_FAIL,
//                         data: null
//                     });
//                     return;
//                 }
//                 usrData['countsOfPerPage'] = (r_data.length > 0) ? r_data[0]['countsOfPerPage'] : ""; //每頁筆數
//                 usrData['hourDiff'] = (r_data.length > 0) ? r_data[0]['hourDiff'] : ""; //與UTC 時差
//                 usrData['hourDiff_descE'] = (r_data.length > 0) ? r_data[0]['hourDiff_descE'] : ""; //與UTC 時差 - 英
//                 usrData['hourDiff_descG'] = (r_data.length > 0) ? r_data[0]['hourDiff_descG'] : ""; //與UTC 時差 - 繁中
//                 usrData['hourDiff_descC'] = (r_data.length > 0) ? r_data[0]['hourDiff_descC'] : ""; //與UTC 時差 - 簡中
//                 adminDao.setActionTime(usrData['AdminId'], cb); //紀錄執行時間
//             },
//             function (r_code, cb) {
//                 if (r_code.code != code.OK) {
//                     next(null, {
//                         code: code.USR.ACTION_TIME_FAIL,
//                         data: null
//                     });
//                     return;
//                 }
//                 self.app.rpc.config.configRemote.getAuthType(session, cb); //各層級的權限
//             }
//         ],
//             var ahthType = [];
//         function (none, r_code, r_ahthType) {
//             for (var i in r_ahthType) {
//                 ahthType.push({
//                     id: r_ahthType[i]['id'],
//                     type: r_ahthType[i]['type'],
//                     name: r_ahthType[i]['type']
//                 });
//             }
//             usrData['AuthType'] = ahthType;
//             //----------------登入可查詢的 type -----------------
//             var LoginLogType = Object.assign([], ahthType);
//             LoginLogType.push({
//                 id: 4,
//                 type: 'Player',
//                 name: 'Player'
//             });
//             usrData['LoginLogType'] = LoginLogType;
//             //----------------登入可查詢的 type -----------------
//             next(null, {
//                 code: code.OK,
//                 data: usrData
//             });
//             return;
//         });
// }

/*
admin建立operator
*/
handler.join = function (msg, session, next) {
  try {
    var self = this
    var otpData = {}
    var log_mod_after = []
    var cid = 0
    var user_set_id = 0
    var logData = {}

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "add"
    logData["FunctionGroupL"] = "System"
    logData["FunctionAction"] = "AddSubUserInMainUser"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""

    //var userSession = {};
    var sys_config = self.app.get("sys_config")
    m_async.waterfall(
      [
        function (cb) {
          adminDao.checkOperatorExist(msg.data, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_DUPLICATE,
              data: null,
            }) //重複資料
            return
          }
          var param = {
            email: msg.data.email,
          }

          adminDao.checkUsrMailExist(param, cb) //判斷是否重複MAIL
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.MAIL_DUPLICATE,
              data: null,
            })
            return
          }

          // msg.data['cid'] = translator.generate();
          msg.data["cid"] = uid.randomUUID(12)

          adminDao.createOperator(msg.data, cb)
        },
        function (r_code, r_cid, cb) {
          // function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_CREATE_FAIL,
              data: null,
            })
            return
          }

          cid = r_cid
          // cid = msg.data['cid'];
          //建立 白名單
          var ip_whitelist = []

          var white_user = {
            Cid: cid,
            UserLevel: 1,
            IpType: 1,
            Desc: "",
            Ip: conf.ADMINIP,
            Name: "",
            State: 1,
          }
          ip_whitelist.push(white_user)

          userDao.CreateIPWhiteList(ip_whitelist, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.WHITE_IN_CREATE_FAIL,
              data: null,
            })
            return
          }

          var secret = m_otplib.authenticator.generateSecret() //secret key
          otpData = {
            Cid: cid,
            IsAdmin: 1,
            HallId: -1,
            OTPCode: secret,
          }

          userDao.createUser_newOtpCode(otpData, cb) //建立OTP code
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.OTP_CREATE_FAIL,
              data: null,
            })
            return
          }

          //一筆-user_setting預設值
          var set_param = {
            Cid: cid,
            IsAdmin: 1,
            CountsOfPerPage: sys_config.counts_per_page,
            HourDiff: sys_config.time_diff_hour,
          }

          userDao.set_user_setting(set_param, cb)
        },
        function (r_code, r_id, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.SETTING_CREATE_FAIL,
              data: null,
            })
            return
          }
          user_set_id = r_id

          adminDao.get_admin_byId(
            {
              AdminId: otpData["Cid"],
            },
            cb
          )
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_USER_FAIL,
              data: null,
            })
            return
          }
          // var res_data = [];
          // var info = {
          //     admin: r_data
          // };
          // res_data.push(info);
          // cb(null, {
          //     code: code.OK
          // }, res_data);

          // console.log("handler.join r_data = ", r_data);
          log_mod_after = log_mod_after.concat(r_data)

          userDao.get_OtpCode_byId(otpData, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_OTP_FAIL,
              data: null,
            })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)

          userDao.get_user_setting(
            {
              Uid: user_set_id,
            },
            cb
          ) //user_setting初始
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_SETTING_FAIL,
              data: null,
            })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)

          logData["Desc_After"] = JSON.stringify(log_mod_after)

          // logData['ActionLevel'] = userSession.get('level') || '';
          logData["AdminId"] = session.get("cid") || ""
          logData["UserName"] = session.get("usrName") || ""

          logDao.add_log_admin(logData, cb)
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.LOG_FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][join] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
編輯管理者
*/
handler.modifyOP = function (msg, session, next) {
  try {
    var self = this
    var log_mod_before = []
    var log_mod_after = []

    var logData = {}
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "System"
    logData["FunctionAction"] = "EditSubUserInMainUser"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    // var userSession = {};

    m_async.waterfall(
      [
        function (cb) {
          adminDao.checkOperatorExist_byCid(msg.data.usr, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_NOT_EXIST,
              data: null,
            })
            return
          }

          var param = {
            email: msg.data.usr.email,
            userId: msg.data.usr.cid,
          }
          adminDao.checkUsrMailExist(param, cb) //判斷是否重複MAIL
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.MAIL_DUPLICATE,
              data: null,
            })
            return
          }

          adminDao.get_admin_byId(
            {
              AdminId: msg.data.usr.cid,
            },
            cb
          ) //before-user
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_USER_FAIL,
              data: null,
            })
            return
          }
          log_mod_before = log_mod_before.concat(r_data)

          adminDao.modifyOperator(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_MODIFY_FAIL,
              data: null,
            })
            return
          }

          adminDao.get_admin_byId(
            {
              AdminId: msg.data.usr.cid,
            },
            cb
          ) //after-user
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_OTP_FAIL,
              data: null,
            })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          //  logData['ActionLevel'] = userSession.get('level') || '';
          logData["AdminId"] = session.get("cid") || ""
          logData["UserName"] = session.get("usrName") || ""

          logDao.add_log_admin(logData, cb)
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.LOG_FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][modifyOP] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getDefault_joinHall = function (msg, session, next) {
  try {
    var self = this
    var groups = {}

    var data = {
      isSub: typeof msg.data.cid != "undefined" && msg.data.cid.length > 0 ? 1 : 0,
      authType: 2,
      cid: typeof msg.data.cid != "undefined" && msg.data.cid.length > 0 ? msg.data.cid : 0,
    }
    //var userSession = {};
    var currencies = []
    var game_currency = []
    var wallets = []
    var userInfo = {}
    let jackpot = [] //JP開關狀態
    m_async.waterfall(
      [
        function (cb) {
          userDao.getJackpotSwitchState(data, cb) //讀取彩金開關
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.DB.LOAD_DATA_FAIL,
              data: null,
            })
            return
          }
          jackpot = r_data.map((item) => {
            if (item.IsJackpotEnabled == 1) {
              return true
            } else {
              return false
            }
          })
          gameDao.getGameCountsByGroup_admin(cb)
        },
        function (r_code, r_groups, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.GAME_LOAD_FAIL,
              data: null,
            })
            return
          }
          groups = r_groups

          if (msg.data.type.indexOf("createHall") > -1 && msg.data.cid != 0) {
            // 新增且是最上層的 reseller，則顯示全部的幣別
            userDao.getCusCurrency_byCid(msg.data.cid, cb)
          } else {
            configDao.getCurrencyList(cb)
          }
        },
        function (r_code, r_currencies, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.CURRENCY_LOAD_FAIL, data: null })
            return
          }

          if (typeof r_currencies === "string") {
            currencies.push(r_currencies)
          } else {
            currencies = r_currencies.map((item) => item.currency)
          }

          var data = {
            userId: "",
            tableName: "",
          }
          data["userId"] = msg.data.cid
          data["tableName"] = "wallet"
          userDao.getUpIdByInfiniteClass(data, cb) // 找出上線
        },
        function (r_code, r_data, cb) {
          var param = {
            cid: "",
          }
          userInfo = r_data
          // 找上線的開放幣別
          if (userInfo[0]["UpId"] == -1 || userInfo[0]["UpId"] == null) {
            if (session.get("level") == 1 && msg.data.type.indexOf("createHall") > -1 && msg.data.cid == 0) {
              // Admin 登入而且是新增狀態 cid 而且是 0(最上層 reseller)
              configDao.getCurrencyList(cb)
            } else {
              if ((session.get("level") == 1 || session.get("level") == 2) && msg.data.type.indexOf("editHall") > -1) {
                configDao.getCurrencyList(cb)
              } else {
                // reseller 新增下層 reseller，cid 是自己的 cid
                param["cid"] = userInfo[0]["Cid"]
                gameDao.getWallet(param, cb)
              }
            }
          } else {
            param["cid"] = msg.data.type.indexOf("createHall") > -1 ? userInfo[0]["Cid"] : userInfo[0]["UpId"]
            gameDao.getWallet(param, cb)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.CURRENCY_LOAD_FAIL, data: null })
            return
          }
          if (r_data[0]["currency"] != "" && r_data[0]["currency"] != undefined) {
            game_currency = r_data.map((item) => item.currency)
          } else {
            r_data.forEach((item) => {
              if (item.Currency != null && item.Currency != "") game_currency.push(item.Currency)
            })
          }

          // 找上線的錢包類型
          if (userInfo[0]["UpId"] == -1 || userInfo[0]["UpId"] == null) {
            if (session.get("level") == 1 && msg.data.type.indexOf("createHall") > -1 && msg.data.cid == 0) {
              // Admin 登入而且是新增狀態 cid 而且是 0(最上層 reseller)
              consts.Wallets.forEach((item) => {
                wallets.push({
                  id: item,
                  value: item == 0 ? "MultiWallet" : "SingleWallet",
                })
              })
              cb(null, { code: code.OK }, null)
            } else {
              if ((session.get("level") == 1 || session.get("level") == 2) && msg.data.type.indexOf("editHall") > -1) {
                consts.Wallets.forEach((item) => {
                  wallets.push({
                    id: item,
                    value: item == 0 ? "MultiWallet" : "SingleWallet",
                  })
                })
                cb(null, { code: code.OK }, null)
              } else {
                userDao.findUpId(userInfo[0]["Cid"], cb)
              }
            }
          } else {
            userDao.findUpId(msg.data.type.indexOf("createHall") > -1 ? userInfo[0]["Cid"] : userInfo[0]["UpId"], cb)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_LOAD_FAIL,
              data: null,
            })
            return
          }

          if (r_data != undefined) {
            r_data[0]["IsSingleWallet"].split(",").forEach((item) => {
              wallets.push({
                id: parseInt(item),
                value: item == 0 ? "MultiWallet" : "SingleWallet",
              })
            })
          }
          if (r_data) data["cid"] = r_data[0]["Cid"]
          userDao.getUserAuthorityTemp(data, cb)
        },
      ],
      function (none, r_code, r_auths) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.AUTH.AUTH_TMP_LOAD_FAIL,
            data: null,
          })
          return
        }

        //var authoritys = [];
        //var count = 0;
        // var currency_length = Currencies.length;

        // //系統所有幣別
        // for (count = 0; count < currency_length; count++) {
        //     currencies.push(Currencies[count].currency);
        // }
        const [jackpotSwith] = jackpot
        next(null, {
          code: code.OK,
          data: {
            authorityTemps: r_auths,
            currencies: currencies,
            game_currency: game_currency, // 開放幣別
            game_groups: groups,
            wallets: wallets, // 錢包類型
            jackpot: jackpotSwith, //彩金開關
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][getDefault_joinHall] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getDefault_joinAdmin = function (msg, session, next) {
  try {
    var self = this
    var authoritys = []
    //var userSession = {};
    m_async.waterfall(
      [
        function (cb) {
          self.app.rpc.config.configRemote.getAuthorityTemp(
            session,
            {
              authType: 1,
            },
            cb
          )
        },
      ],
      function (none, r_code, r_auths) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.AUTH.AUTH_TMP_LOAD_FAIL,
            data: null,
          })
          return
        }

        var authoritys = []

        for (let count in r_auths) {
          authoritys.push({
            id: r_auths[count].id,
            desc: r_auths[count].desc,
            note: r_auths[count].note,
          })
        }

        next(null, {
          code: code.OK,
          data: {
            authorityTemps: authoritys,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][getDefault_joinAdmin] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getDefault_joinSha = function (msg, session, next) {
  try {
    var self = this
    var authoritys = []
    var auth_tmp = []
    var ha_authority = []
    var game_currency = []
    var data = {
      isSub: 1,
      authType: msg.data.authType,
      // 非管理者 upId = 0 (authType: 2(hall)、3(agent))；upId 用來判斷查詢的 id 是用上線 id 還是自己的 id
      cid:
        msg.data.upId == 0
          ? msg.data.ownerId
          : !msg.data.isSubLogin && msg.data.upId != 0 && [2, 3].indexOf(msg.data.authType) > -1
          ? msg.data.upId
          : msg.data.ownerId,
    }

    //var userSession = {};
    m_async.waterfall(
      [
        function (cb) {
          let notUserCid = msg.data.authType == 3 ? msg.data.cid : msg.data.ownerId
          var param = {
            isSub: 1,
            authType: msg.data.authType,
            // 非管理者 upId = 0 (authType: 2(hall)、3(agent))；upId 用來判斷查詢的 id 是用上線 id 還是自己的 id
            cid: msg.data.upId == 0 ? notUserCid : msg.data.cid,
          }

          userDao.getUserAuthorityTemp(param, cb) //可加入tmpID
        },
        function (r_code, r_auths, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.AUTH_TMP_LOAD_FAIL,
              data: null,
            })
            return
          }
          auth_tmp = r_auths

          userDao.getUser_authFuncs(data, cb) //ha's tmp
        },
        function (r_code, r_authFuncs, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.AUTH_TMP_LOAD_FAIL,
              data: null,
            })
            return
          }

          ha_authority = r_authFuncs.split(",")
          userDao.get_user_byId({ Cid: data.cid }, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_LOAD_FAIL,
            })
            return
          }

          let isSub = r_data[0]["customer"][0]["IsSub"]

          if (isSub == 1) {
            if (msg.data.isSubLogin) {
              userDao.get_subUser_byCid(data, cb)
            } else {
              if (
                (r_data[0]["customer"][0]["IsAg"] == 2 &&
                  data.cid.indexOf(msg.data.ownerId) > -1 &&
                  r_data[0]["customer"][0]["Upid"] == -1) ||
                (r_data[0]["customer"][0]["IsAg"] == 3 && r_data[0]["customer"][0]["Upid"].indexOf(msg.data.cid) > -1)
              ) {
                // 管理者 下面的管理者
                userDao.get_subUser_byCid(data, cb)
              } else {
                userDao.getGame_currency(msg.data, cb)
              }
            }
          } else {
            gameDao.getWallet(data, cb)
          }
        },
        function (r_code, r_data, cb) {
          if (r_data[0]["Currencies"] != "" && r_data[0]["Currencies"] != undefined) {
            game_currency = r_data[0]["Currencies"].split(",")
          } else {
            r_data.forEach((item) => {
              if (item.Currency != null && item.Currency != "") game_currency.push(item.Currency)
            })
          }

          var param = {
            user_level: "SubHall",
            up_authority: ha_authority,
          }

          self.app.rpc.config.configRemote.getDefault_joinAuthorityFuncs_v3(session, param, cb) //預設的權限
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.AUTH.AUTH_TMP_LOAD_FAIL,
            data: null,
          })
          return
        }

        var sha_auths = r_data["subhall"]

        next(null, {
          code: code.OK,
          data: {
            authorityTemps: auth_tmp,
            currencies: game_currency,
            user_authority: sha_auths, //hall本身的權限 與sha 可加入的權限
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][getDefault_joinSha] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getAdmins = function (msg, session, next) {
  try {
    var self = this
    var users = []
    var ttlCount = 0
    var curPage = 0
    var pageCount = 0

    var data_query = {
      curPage: 0,
      pageCount: 0,
      userName: "",
      isDemo: "",
      states: [],
      start_date: "",
      end_date: "",
    }

    // var userSession = {};

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.page === "undefined" ||
      typeof msg.data.pageCount === "undefined" ||
      typeof msg.data.page != "number" ||
      typeof msg.data.pageCount != "number" ||
      msg.data.page <= 0 ||
      msg.data.pageCount <= 0
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }

    if (typeof msg.data.userName != "undefined") {
      data_query.userName = msg.data.userName
    }

    if (typeof msg.data.nickName != "undefined") {
      data_query.nickName = msg.data.nickName
    }

    if (typeof msg.data.state != "undefined" && typeof msg.data.state === "string") {
      if (msg.data.state != "") data_query.states = msg.data.state.split(",")
    }

    if (typeof msg.data.start_date != "undefined") {
      data_query.start_date = msg.data.start_date
    }

    if (typeof msg.data.end_date != "undefined") {
      data_query.end_date = msg.data.end_date
    }
    if (typeof msg.data.sortKey != "undefined") {
      data_query.sortKey = msg.data.sortKey
    }
    if (typeof msg.data.sortType != "undefined") {
      data_query.sortType = msg.data.sortType
    }
    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "AdminId"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    data_query.curPage = msg.data.page
    data_query.pageCount = msg.data.pageCount

    m_async.waterfall(
      [
        function (cb) {
          adminDao.getOps(data_query, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_LOAD_FAIL,
              data: null,
            })
            return
          }

          ttlCount = r_data["count"]
          users = r_data["info"]

          var cid = []
          for (var i in users) {
            cid.push(users[i]["AdminId"])
          }

          var login_data = {
            cid: cid,
            level: 1,
          }
          self.app.rpc.log.logRemote.getUserLastLogin(session, login_data, cb) //最近登入時間
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_LOAD_FAIL,
              data: null,
            })
            return
          }

          for (let i in users) {
            //最後登入時間
            var login_info = r_data.filter((item) => +item.cid == +users[i]["AdminId"])
            var lastLogin = login_info.length > 0 ? login_info[0]["lastLogin"] : "-"
            users[i]["lastLogin"] = lastLogin
          }

          curPage = data_query.curPage
          pageCount = data_query.pageCount

          configDao.getAuthorityTemps(cb)
        },
      ],
      function (r_code, authTemps) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.FAIL,
            data: null,
          })
          return
        }
        var authoritysIdList = authTemps.authoritysById

        for (let j in users) {
          users[j]["note"] = authoritysIdList[users[j]["AuthorityTemplateID"]].note
        }

        next(null, {
          code: code.OK,
          data: {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: curPage,
            page_count: pageCount,
            usrs: users,
            sortKey: sortKey,
            sortType: sortType,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][getAdmins] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
amdin修改自己密碼
*/
// handler.modifyPassword = function (msg, session, next) {

//     var self = this;
//     var logData = {};
//     logData['IP'] = msg.remoteIP;
//     logData['ModifiedType'] = 'edit';
//     logData['FunctionGroupL'] = "Account";
//     logData['FunctionAction'] = "EditPassword";
//     logData['RequestMsg'] = JSON.stringify(msg);
//     logData['Desc_Before'] = '';
//     var log_mod_before = [];
//     var log_mod_after = [];
//     //var userSession = {};

//     m_async.waterfall([
//         function (cb) {
//             adminDao.checkOperatorExist_byCid(msg.data.usr, cb);
//         },
//         function (r_code, cb) {

//             if (r_code.code != code.OK) {
//                 next(null, {
//                     code: code.USR.USER_NOT_EXIST,
//                     data: null
//                 });
//                 return;
//             }

//             adminDao.getPassword_Cid(msg.data.usr.cid, cb);
//         },
//         function (r_code, r_data, cb) {

//             if (r_code.code != code.OK) {
//                 next(null, {
//                     code: code.USR.LOG_PASSWORD_FAIL,
//                     data: null
//                 });
//                 return;
//             }

//             if (r_data[0]['admin'][0]['Passwd'] != msg.data.usr['password_old']) {
//                 next(null, {
//                     code: code.USR.USER_OLD_PASSWORD_FAIL,
//                     data: null
//                 });
//                 return;
//             }

//             log_mod_before = log_mod_before.concat(r_data);

//             adminDao.modifyPassword(msg.data.usr, cb);
//         },
//         function (r_code, cb) {

//             if (r_code.code != code.OK) {
//                 next(null, {
//                     code: code.USR.USER_MODIFY_FAIL,
//                     data: null
//                 });
//                 return;
//             }
//             adminDao.getPassword_Cid(msg.data.usr.cid, cb);
//         },
//         function (r_code, r_data, cb) {

//             if (r_code.code != code.OK) {
//                 next(null, {
//                     code: code.USR.LOG_PASSWORD_FAIL,
//                     data: null
//                 });
//                 return;
//             }

//             log_mod_after = log_mod_after.concat(r_data);
//             logData['Desc_Before'] = JSON.stringify(log_mod_before);
//             logData['Desc_After'] = JSON.stringify(log_mod_after);

//             logData['AdminId'] = session.get('cid') || '';
//             logData['UserName'] = session.get('usrName') || '';

//             logDao.add_log_admin( logData, cb);
//         }
//     ], function (none, r_code, r_id) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.USR.LOG_FAIL,
//                 data: null
//             });
//             return;
//         }

//         next(null, {
//             code: code.OK
//         });
//         return;
//     })
// };
/*
admin修改operator密碼
*/
// handler.modifyOpPassword = function (msg, session, next) {
//     var self = this;
//     var logData = {};

//     logData['IP'] = msg.remoteIP;
//     logData['ModifiedType'] = 'edit';
//     logData['FunctionGroupL'] = "System";
//     logData['FunctionAction'] = "EditSubUserInMainUser";
//     logData['RequestMsg'] = JSON.stringify(msg);
//     logData['Desc_Before'] = '';
//     var log_mod_before = [];
//     var log_mod_after = [];
//     //var userSession = {};

//     m_async.waterfall([
//         function (cb) {

//             adminDao.checkOperatorExist_byCid(msg.data, cb);
//         },
//         function (r_code, cb) {

//             if (r_code.code != code.OK) {
//                 next(null, {
//                     code: code.USR.USER_NOT_EXIST,
//                     data: null
//                 });
//                 return;
//             }

//             adminDao.getPassword_Cid(msg.data.cid, cb);
//         },
//         function (r_code, r_data, cb) {

//             if (r_code.code != code.OK) {
//                 next(null, {
//                     code: code.USR.LOG_PASSWORD_FAIL,
//                     data: null
//                 });
//                 return;
//             }
//             log_mod_before = log_mod_before.concat(r_data);

//             adminDao.modifyOpPassword(msg.data, cb);
//         },
//         function (r_code, cb) {

//             if (r_code.code != code.OK) {
//                 next(null, {
//                     code: code.USR.USER_MODIFY_FAIL,
//                     data: null
//                 });
//                 return;
//             }
//             adminDao.getPassword_Cid(msg.data.cid, cb);
//         },
//         function (r_code, r_data, cb) {

//             if (r_code.code != code.OK) {
//                 next(null, {
//                     code: code.USR.LOG_PASSWORD_FAIL,
//                     data: null
//                 });
//                 return;
//             }

//             log_mod_after = log_mod_after.concat(r_data);
//             logData['Desc_Before'] = JSON.stringify(log_mod_before);
//             logData['Desc_After'] = JSON.stringify(log_mod_after);

//             logData['AdminId'] = session.get('cid') || '';
//             logData['UserName'] = session.get('usrName') || '';
//             logDao.add_log_admin( logData, cb);
//         }
//     ], function (none, r_code, r_id) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.USR.LOG_FAIL,
//                 data: null
//             });
//             return;
//         }
//         next(null, {
//             code: code.OK
//         });
//         return;
//     })
// };
//權限範本list
handler.getList_AuthorityTmp = function (msg, session, next) {
  try {
    var data = {}
    var self = this
    var ttlCount_authTemp = 0
    //var userSession = {};

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.page === "undefined" ||
      typeof msg.data.pageCount === "undefined" ||
      typeof msg.data.page != "number" ||
      typeof msg.data.pageCount != "number" ||
      msg.data.page === 0 ||
      msg.data.pageCount === 0 ||
      typeof msg.data.cid === "undefined"
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }

    data["curPage"] = msg.data.page
    data["pageCount"] = msg.data.pageCount

    //時間
    if (typeof msg.data.startTime != "undefined") {
      data["start_date"] = msg.data.startTime
    }
    if (typeof msg.data.endTime != "undefined") {
      data["end_date"] = msg.data.endTime
    }

    if (typeof msg.data.typeId != "undefined") {
      data["typeId"] = msg.data.typeId
    }

    if (typeof msg.data.typeName != "undefined" && typeof msg.data.typeName === "string") {
      data["typeName"] = msg.data.typeName
    }

    if (typeof msg.data.cid != "undefined") {
      data["cid"] = msg.data.cid
    }

    if (typeof msg.data.sortKey != "undefined") {
      data["sortKey"] = msg.data.sortKey
    }

    if (typeof msg.data.sortType != "undefined") {
      data["sortType"] = msg.data.sortType
    }

    if (typeof msg.data.key != "undefined") {
      data["key"] = msg.data.key
    }

    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "modifyDate"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    m_async.waterfall(
      [
        function (cb) {
          adminDao.getList_AuthorityTemps(data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.AUTH.AUTH_TMP_LOAD_FAIL,
            data: null,
          })
          return
        }

        var ttlCount = r_data["count"]
        var pageCount = msg.data.pageCount
        var pageCur = msg.data.page

        for (var i in r_data["info"]) {
          r_data["info"][i]["addDate"] = r_data["info"][i]["addDate"] == "" ? "" : r_data["info"][i]["addDate"]
          r_data["info"][i]["modifyDate"] = r_data["info"][i]["modifyDate"] == "" ? "" : r_data["info"][i]["modifyDate"]
        }
        next(null, {
          code: code.OK,
          data: {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            auths: r_data["info"],
            sortKey: sortKey,
            sortType: sortType,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][getList_AuthorityTmp] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
//用在權限範本
handler.getDefault_joinAuthority = function (msg, session, next) {
  try {
    const self = this
    //var userSession = {};

    if (typeof msg.data.cid === "undefined") {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var data = {
      tmpId: typeof msg.data.id === "undefined" ? null : msg.data.id,
    }

    var authTmp = {}
    var authority = []
    var auth_state = false
    var user_level = []
    m_async.waterfall(
      [
        function (cb) {
          adminDao.AuthorityTemplateID(
            {
              cid: msg.data.cid,
            },
            cb
          ) //個人權限
        },
        function (r_code, AuthorityTemplateID, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.AUTH_TMP_LOAD_FAIL,
              data: null,
            })
            return
          }

          if (msg.data.cid == "-1") {
            //admin 登入
            cb(
              null,
              {
                code: code.OK,
              },
              null
            )
          } else {
            // ha 登入
            var tmp_data = {
              cid: msg.data.cid,
            }
            userDao.getUser_authFuncs(tmp_data, cb)
          }
        },
        function (r_code, r_authFuncs, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.AUTH_TMP_LOAD_FAIL,
              data: null,
            })
            return
          }

          if (msg.data.cid != "-1") {
            auth_state = true
            authority = r_authFuncs.split(",")
            user_level = ["SubHall", "Agent"]
          } else {
            user_level = ["Admin", "Hall"]
          }

          var param = {
            user_level: user_level,
            auth_state: auth_state,
            up_authority: authority,
          }

          self.app.rpc.config.configRemote.getDefault_joinAuthorityFuncs_v2(session, param, cb) //----- 各層級預設可選的權限
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.AUTH_TMP_LOAD_FAIL,
              data: null,
            })
            return
          }
          var auth = {}
          if (msg.data.cid != "-1") {
            //hall  (取 subhall,agent)
            Object.keys(r_data).forEach((level) => {
              var level_name = level == "subhall" ? "subuser" : level
              if (["reseller", "agent", "subuser"].indexOf(level_name) > -1) auth[level_name] = r_data[level]
            })
          } else {
            Object.keys(r_data).forEach((level) => {
              var level_name = level == "admin" ? "admin" : level == "subhall" ? "subuser" : level
              if (["admin", "reseller", "agent", "subuser"].indexOf(level_name) > -1) auth[level_name] = r_data[level]
            })
          }
          authTmp = auth
          adminDao.getDetail_AuthorityFunc(data, cb) //編輯頁面-範本info
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.AUTH.AUTH_FUNC_FAIL,
            data: null,
          })
          return
        }

        if (r_code.code == code.OK && r_data !== undefined) {
          var authFunc = {}
          authFunc[r_data["type"]] = authTmp[r_data["type"]]
          authFunc["info"] = r_data
          authTmp = authFunc
        }
        next(null, {
          code: code.OK,
          data: authTmp,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][getDefault_joinAuthority] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
新增權限範本
*/
handler.join_AuthorityTemp = function (msg, session, next) {
  try {
    var logData = {}

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "add"
    logData["FunctionGroupL"] = "System"
    logData["FunctionAction"] = "AddAuthority"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    var log_mod_after = []
    var tmpId = 0
    //var userSession = {};

    if (
      typeof msg.data.name === "undefined" ||
      typeof msg.data.type === "undefined" ||
      typeof msg.data.funcIds === "undefined" ||
      typeof msg.data.type != "number" ||
      typeof msg.data.cid === "undefined" ||
      typeof msg.data.modifyId === "undefined"
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }

    if (typeof msg.data.name != "string" || msg.data.name.length > 32) {
      next(null, {
        code: code.AUTH.AUTH_NAME_FAIL,
        data: null,
      })
      return
    }

    var data = {
      desc: msg.data.name,
      note: typeof msg.data.note === "undefined" ? "" : msg.data.note,
      type: msg.data.type,
      funcIds: msg.data.funcIds,
      cid: msg.data.cid == "-1" ? 0 : msg.data.cid,
      modifyId: msg.data.modifyId,
    }

    m_async.waterfall(
      [
        function (cb) {
          adminDao.joinAuthorityTemp(data, cb)
        },
        function (r_code, r_tmpId, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.AUTH_TMP_CREATE_FAIL,
              data: null,
            })
            return
          }
          tmpId = r_tmpId
          adminDao.getAuthorityTemp_byId(tmpId, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.LOG_TMP_FAIL,
              data: null,
            })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          if (session.get("level") == 1) {
            //admin執行

            logData["AdminId"] = session.get("cid") || ""
            logData["UserName"] = session.get("usrName") || ""

            logDao.add_log_admin(logData, cb)
          } else {
            logData["ActionLevel"] = session.get("level") || ""
            logData["ActionCid"] = session.get("cid") || ""
            logData["ActionUserName"] = session.get("usrName") || ""

            logDao.add_log_customer(logData, cb)
          }
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.AUTH.LOG_FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][join_AuthorityTemp] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
新增權限範本
*/
handler.join_AuthorityTemp_v2 = function (msg, session, next) {
  try {
    var logData = {}
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "add"
    logData["FunctionGroupL"] = "System"
    logData["FunctionAction"] = "AddAuthority"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    var log_mod_after = []
    var tmpId = 0
    //var userSession = {};

    if (
      typeof msg.data.name === "undefined" ||
      typeof msg.data.type === "undefined" ||
      typeof msg.data.funcIds === "undefined" ||
      typeof msg.data.type != "number" ||
      typeof msg.data.cid === "undefined" ||
      typeof msg.data.modifyId === "undefined"
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }

    if (typeof msg.data.name != "string" || msg.data.name.length > 32) {
      next(null, {
        code: code.AUTH.AUTH_NAME_FAIL,
        data: null,
      })
      return
    }

    var data = {
      desc: msg.data.name,
      note: typeof msg.data.note === "undefined" ? "" : msg.data.note,
      type: msg.data.type,
      funcIds: msg.data.funcIds,
      cid: msg.data.cid == "-1" ? 0 : msg.data.cid,
      modifyId: msg.data.modifyId,
    }

    m_async.waterfall(
      [
        function (cb) {
          adminDao.joinAuthorityTemp(data, cb)
        },
        function (r_code, r_tmpId, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.AUTH_TMP_CREATE_FAIL,
              data: null,
            })
            return
          }

          tmpId = r_tmpId
          adminDao.getAuthorityTemp_byId(tmpId, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.LOG_TMP_FAIL,
              data: null,
            })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          if (session.get("level") == 1) {
            logData["AdminId"] = session.get("cid") || ""
            logData["UserName"] = session.get("usrName") || ""

            logDao.add_log_admin(logData, cb)
          } else {
            logData["ActionCid"] = session.get("cid") || ""
            logData["ActionUserName"] = session.get("usrName") || ""
            logData["ActionLevel"] = session.get("level") || ""

            logDao.add_log_customer(logData, cb)
          }
        },
        function (r_code, r_id, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.LOG_FAIL,
              data: null,
            })
            return
          }

          var tmp_data = {
            isSub: msg.data.isSub,
            authType: msg.data.type,
            cid: msg.data.cid == "-1" ? 0 : msg.data.cid,
          }
          userDao.getUserAuthorityTemp(tmp_data, cb)
        },
      ],
      function (none, r_code, r_auths) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.AUTH.AUTH_TMP_LOAD_FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: {
            authorityTemps: r_auths,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][join_AuthorityTemp_v2] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

/*
修改權限範本
*/
handler.modify_AuthorityTemp = function (msg, session, next) {
  try {
    var logData = {}

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "System"
    logData["FunctionAction"] = "EditAuthority"
    logData["RequestMsg"] = JSON.stringify(msg)
    var log_mod_before = []
    var log_mod_after = []
    //var userSession = {};

    if (
      typeof msg.data.id === "undefined" ||
      typeof msg.data.funcIds === "undefined" ||
      typeof msg.data.id != "number" ||
      typeof msg.data.cid === "undefined" ||
      typeof msg.data.modifyId === "undefined"
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }

    var data = {
      tmpId: msg.data.id,
      funcIds: msg.data.funcIds,
      cid: msg.data.cid == "-1" ? 0 : msg.data.cid,
      modifyId: msg.data.modifyId,
    }

    m_async.waterfall(
      [
        function (cb) {
          adminDao.checkAuthorityExist_byTmpId(data.tmpId, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.AUTH_TMP_NOT_EXIST,
              data: null,
            })
            return
          }

          adminDao.getAuthorityTemp_byId(data.tmpId, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.LOG_TMP_FAIL,
              data: null,
            })
            return
          }

          log_mod_before = log_mod_before.concat(r_data)
          adminDao.modifyAuthorityTmp(data, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.AUTH_TMP_MODIFY_FAIL,
              data: null,
            })
            return
          }

          adminDao.getAuthorityTemp_byId(data.tmpId, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.LOG_TMP_FAIL,
              data: null,
            })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          if (session.get("level") == 1) {
            logData["AdminId"] = session.get("cid") || ""
            logData["UserName"] = session.get("usrName") || ""
            logDao.add_log_admin(logData, cb)
          } else {
            logData["ActionLevel"] = session.get("level") || ""
            logData["ActionCid"] = session.get("cid") || ""
            logData["ActionUserName"] = session.get("usrName") || ""
            logDao.add_log_customer(logData, cb)
          }
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.AUTH.LOG_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][modify_AuthorityTemp] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//跑馬燈清單
handler.getList_marquee = function (msg, session, next) {
  try {
    //var userSession = {};

    var data = {}
    var ttlCount_marquee = 0
    if (typeof msg.data === "undefined") {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }
    if (
      typeof msg.data.page === "undefined" ||
      typeof msg.data.pageCount === "undefined" ||
      typeof msg.data.page != "number" ||
      typeof msg.data.pageCount != "number" ||
      msg.data.page === 0 ||
      msg.data.pageCount === 0 ||
      typeof msg.data.cid == "undefined"
    ) {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }

    data["curPage"] = msg.data.page
    data["pageCount"] = msg.data.pageCount

    if (typeof msg.data.locationId != "undefined") {
      data["GameId"] = msg.data.locationId
    }

    if (typeof msg.data.title != "undefined") {
      data["Content"] = msg.data.title
    }

    if (typeof msg.data.startTime != "undefined" && typeof msg.data.endTime != "undefined") {
      data["StartTime"] = msg.data.startTime
      data["StopTime"] = msg.data.endTime
    }

    if (typeof msg.data.priorityId != "undefined") {
      data["Priority"] = msg.data.priorityId
    }

    if (typeof msg.data.cid != "undefined") {
      data["cid"] = msg.data.cid
    }
    if (typeof msg.data.sortKey != "undefined") {
      data["sortKey"] = msg.data.sortKey
    }
    if (typeof msg.data.sortType != "undefined") {
      data["sortType"] = msg.data.sortType
    }

    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "modifyDate"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    m_async.waterfall(
      [
        function (cb) {
          adminDao.getList_marquee(data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.MARQUEE_LOAD_FAIL,
            data: null,
          })
          return
        }

        var ttlCount = r_data["count"]
        var pageCount = msg.data.pageCount
        var pageCur = msg.data.page

        var marquees = []
        r_data["info"].forEach((item) => {
          marquees.push({
            id: item.Id,
            locationId: item.GameId,
            title: item.Content,
            langC: item.LangC,
            langG: item.LangG,
            langE: item.LangE,
            descriptionC: item.DescriptionC,
            descriptionG: item.DescriptionG,
            descriptionE: item.DescriptionE,
            startTime: item.StartTime,
            endTime: item.StopTime,
            priorityId: item.Priority,
            owner: item.owner,
            modifyAccount: item.modifyAccount,
            modifyDate: item.modifyDate,
          })
        })
        /*
            for (count in r_data) {
                marquees.push({
                    id: r_data[count].Id,
                    locationId: r_data[count].GameId,
                    title: r_data[count].Content,
                    langC: r_data[count].LangC,
                    langG: r_data[count].LangG,
                    langE: r_data[count].LangE,
                    startTime: r_data[count].StartTime,
                    endTime: r_data[count].StopTime,
                    priorityId: r_data[count].Priority,
                    owner: r_data[count].owner,
                    modifyAccount: r_data[count].modifyAccount,
                    modifyDate: r_data[count].modifyDate
                });
            }*/

        next(null, {
          code: code.OK,
          data: {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            marquees: marquees,
            sortKey: sortKey,
            sortType: sortType,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][getList_marquee] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//新增跑馬燈
handler.createMarquee = function (msg, session, next) {
  try {
    var self = this
    var logData = {}
    //var userSession = {};

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "add"
    logData["FunctionGroupL"] = "System"
    logData["FunctionAction"] = "AddMarquee"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    var log_mod_after = []

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.cid == "undefined" ||
      typeof msg.data.modifyId == "undefined"
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }

    //轉換時間
    if (typeof msg.data["startTime"] != "undefined") msg.data["startTime"] = timezone.UTCToLocal(msg.data["startTime"])
    if (typeof msg.data["endTime"] != "undefined") msg.data["endTime"] = timezone.UTCToLocal(msg.data["endTime"])

    m_async.waterfall(
      [
        function (cb) {
          adminDao.createMarquee(msg.data, cb)
        },
        function (r_code, r_id, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.MARQUEE_CREATE_FAIL,
              data: null,
            })
            return
          }
          adminDao.getMarquee_byId(
            {
              id: r_id,
            },
            cb
          )
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_MARQUEE_FAIL,
              data: null,
            })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          if (session.get("level") == 1) {
            logData["AdminId"] = session.get("cid") || ""
            logData["UserName"] = session.get("usrName") || ""
            logDao.add_log_admin(logData, cb)
          } else {
            logData["ActionLevel"] = session.get("level") || ""
            logData["ActionCid"] = session.get("cid") || ""
            logData["ActionUserName"] = session.get("usrName") || ""
            logDao.add_log_customer(logData, cb)
          }
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.LOG_FAIL,
            data: null,
          })
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][createMarquee] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//修改跑馬燈
handler.modifyMarquee = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};

    var logData = {}
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "System"
    logData["FunctionAction"] = "EditMarquee"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    var log_mod_before = []
    var log_mod_after = []

    if (typeof msg.data === "undefined" || typeof msg.data.modifyId == "undefined") {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }

    //轉換時間
    if (typeof msg.data["startTime"] != "undefined") msg.data["startTime"] = timezone.UTCToLocal(msg.data["startTime"])
    if (typeof msg.data["endTime"] != "undefined") msg.data["endTime"] = timezone.UTCToLocal(msg.data["endTime"])

    var data = msg.data

    m_async.waterfall(
      [
        function (cb) {
          adminDao.checkMarquee_byId(data, cb)
        },
        function (r_code, r_count, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          adminDao.getMarquee_byId(
            {
              id: data.id,
            },
            cb
          )
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_MARQUEE_FAIL,
              data: null,
            })
            return
          }
          log_mod_before = log_mod_before.concat(r_data)
          adminDao.modifyMarquee(data, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          adminDao.getMarquee_byId(
            {
              id: data.id,
            },
            cb
          )
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_MARQUEE_FAIL,
              data: null,
            })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          if (session.get("level") == 1) {
            logData["AdminId"] = session.get("cid") || ""
            logData["UserName"] = session.get("usrName") || ""
            logDao.add_log_admin(logData, cb)
          } else {
            logData["ActionLevel"] = session.get("level") || ""
            logData["ActionCid"] = session.get("cid") || ""
            logData["ActionUserName"] = session.get("usrName") || ""
            logDao.add_log_customer(logData, cb)
          }
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.MARQUEE_MODIFY_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][modifyMarquee] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// //幣別清單
// handler.getList_currency = function (msg, session, next) {
//     adminDao.getList_currency(msg.data, function (r_none, r_code, r_data) {
//         //console.log('getList_currency--',JSON.stringify(r_code),JSON.stringify(r_data) )
//         if (r_code.code != code.OK) {
//             next(null, { code: r_code.code, data: null });
//         }
//         var currencies = [];
//         for (i in r_data) {
//             currencies.push(r_data[i]['Currency']);
//         }
//         next(null, { code: code.OK, data: { currency: currencies } });
//     });
// };

//幣別 - 初始清單 (幣別,現執行匯率)
handler.getDefaultList_currency = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};
    var rateList = []
    var data = {}
    var mainCurrency = ""
    m_async.waterfall(
      [
        function (cb) {
          self.app.rpc.config.configRemote.getMainCurrency(session, cb) //取主幣別
        },
        function (r_code, r_data, cb) {
          mainCurrency = r_data //主幣別
          data["currency"] = mainCurrency
          adminDao.getCurrenies(cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.CURRENCY_LOAD_FAIL,
              data: null,
            })
            return
          }
          var currency = []
          for (var i in r_data) {
            if (r_data[i]["currency"] != mainCurrency) currency.push(r_data[i]["currency"])
          }
          data["ex_currency"] = currency //兌換幣別

          adminDao.getList_currentRate(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.RATE_LOAD_FAIL,
            data: null,
          })
          return
        }
        var rates = []
        for (var i in r_data) {
          var rateInfo = {
            id: r_data[i]["Id"],
            currency: r_data[i]["Currency"],
            exCurrency: r_data[i]["ExCurrency"],
            rate: r_data[i]["CryDef"],
            enableTime: r_data[i]["EnableTime"],
          }
          rates.push(rateInfo)
        }
        data["rates"] = rates
        next(null, {
          code: code.OK,
          data: data,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][getDefaultList_currency] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
新增匯率
*/
handler.createExchangeRate = function (msg, session, next) {
  try {
    console.log("createExchangeRate data", JSON.stringify(msg))

    var logData = {}
    var currencyId = 0

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "add"
    logData["FunctionGroupL"] = "System"
    logData["FunctionAction"] = "AddExchangeRate"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    var log_mod_after = []
    var currencies = []
    //var userSession = {};

    if (
      typeof msg.data.currency === "undefined" ||
      typeof msg.data.exCurrency === "undefined" ||
      typeof msg.data.rate === "undefined" ||
      typeof msg.data.enableTime === "undefined"
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }

    const limitTime = timezone.getServerTime().valueOf()
    const enableTime = new Date(timezone.transferToISOString(msg.data.enableTime)).valueOf()

    if (enableTime < limitTime) {
      next(null, {
        code: code.USR.RATE_LIMIT_TIME_FAIL,
        data: null,
      })
      return
    }

    var data = msg.data

    m_async.waterfall(
      [
        function (cb) {
          adminDao.checkExchangeRate_duplicateTime(data, cb)
        },
        function (r_code, r_count, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.CURRENCY_QUERY_FAIL,
              data: null,
            })
            return
          }

          if (r_count !== 0) {
            next(null, {
              code: code.USR.RATE_DUPLICATE,
              data: null,
            })
            return
          }

          adminDao.createExchangeRate(data, cb)
        },
        function (r_code, r_id, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.RATE_CREATE_FAIL,
              data: null,
            })
            return
          }
          currencyId = r_id

          adminDao.createCurrency(data, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.CURRENCY_CREATE_FAIL,
              data: null,
            })
            return
          }
          adminDao.getExchangeRate_byId(currencyId, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_RATE_FAIL,
              data: null,
            })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["AdminId"] = session.get("cid") || ""
          logData["UserName"] = session.get("usrName") || ""

          logDao.add_log_admin(logData, cb)
        },
        function (r_code, r_id, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_FAIL,
              data: null,
            })
            return
          }
          adminDao.getCurrenies(cb)
        },
      ],
      function (r_none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.CURRENCY_LOAD_FAIL,
            data: null,
          })
          return
        }

        for (var i in r_data) {
          currencies.push(r_data[i]["currency"])
        }

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
    logger.error("[adminHandler][createExchangeRate] catch err", inspect(err))
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
修改匯率
*/
handler.modifyExchangeRate = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};
    var logData = {}
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "System"
    logData["FunctionAction"] = "EditExchangeRate"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    var log_mod_before = []
    var log_mod_after = []
    var currencies = []

    if (
      typeof msg.data.id === "undefined" ||
      typeof msg.data.mainCurrency === "undefined" ||
      typeof msg.data.rate === "undefined" ||
      typeof msg.data.rate != "number" ||
      typeof msg.data.enableTime === "undefined"
    ) {
      next(null, {
        code: code.USR.CURRENCY_PARA_FAIL,
        data: null,
      })
      return
    }

    const limitTime = timezone.getServerTime().valueOf()
    const enableTime = new Date(timezone.transferToISOString(msg.data.enableTime)).valueOf()

    console.warn("s", timezone.getServerTime().toISOString())
    console.warn("e ", timezone.transferToISOString(msg.data.enableTime))

    console.log("limitTime", limitTime)
    console.log("enableTime", enableTime)
    console.log(enableTime - limitTime)

    if (enableTime < limitTime) {
      next(null, {
        code: code.USR.RATE_LIMIT_TIME_FAIL,
        data: null,
      })
      return
    }

    var data = msg.data
    m_async.waterfall(
      [
        function (cb) {
          adminDao.checkState_modifyExchangeRate(data, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.RATE_NOT_EXIST,
              data: null,
            })
            return
          }

          adminDao.getExchangeRate_byId(data.id, cb) //before -rate
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_RATE_FAIL,
              data: null,
            })
            return
          }
          log_mod_before = log_mod_before.concat(r_data)
          adminDao.modifyExchangeRate(data, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.RATE_MODIFY_FAIL,
              data: null,
            })
            return
          }
          adminDao.getExchangeRate_byId(data.id, cb) //after -rate
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_RATE_FAIL,
              data: null,
            })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)
          logData["AdminId"] = session.get("cid") || ""
          logData["UserName"] = session.get("usrName") || ""

          logDao.add_log_admin(logData, cb)
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.LOG_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][modifyExchangeRate] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getList_exchangeRate = function (msg, session, next) {
  try {
    //var userSession = {};

    var data = msg.data
    if (typeof msg.data === "undefined") {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }

    if (typeof msg.data.isPage === "undefined") {
      data["isPage"] = true //預設-有分頁
    }

    if (data.isPage == true) {
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
    if (data["isPage"] == false) {
      if (typeof msg.data.finish === "undefined") {
        data["finish"] = 0
      } //0:未完成
      if (typeof msg.data.index === "undefined") {
        data["index"] = 0
      } //開始第N筆
      data["pageCount"] = 1000 //每次取得筆數
    }

    if (typeof msg.data.currency != "undefined" && typeof msg.data.currency === "string") {
      data["currency"] = msg.data.currency
    }

    if (typeof msg.data.ex_currency != "undefined" && typeof msg.data.ex_currency === "string") {
      data["ex_currency"] = msg.data.ex_currency
    }

    if (typeof msg.data.state != "undefined") {
      data["state"] = msg.data.state
    }

    if (typeof msg.data.start_date != "undefined" && msg.data.start_date != "") {
      data["start_date"] = timezone.UTCToLocal(msg.data.start_date)
    }

    if (typeof msg.data.end_date != "undefined" && msg.data.end_date != "") {
      data["end_date"] = timezone.UTCToLocal(msg.data.end_date)
    }
    console.log("------getList_exchangeRate data --------", JSON.stringify(data))

    m_async.waterfall(
      [
        function (cb) {
          adminDao.getList_exchangeRate(data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.RATE_LOAD_FAIL,
            data: null,
          })
          return
        }

        const ttlCount = r_data["count"] //總筆數
        const pageCount = data.pageCount
        const pageCur = data.page

        var rates = []
        r_data["info"].forEach((item) => {
          rates.push({
            id: item.Id,
            currency: item.currency,
            ex_currency: item.ex_currency,
            rate: item.CryDef,
            startTime: timezone.LocalToUTC(item.startTime),
            endTime: item.endTime != "-" ? timezone.LocalToUTC(item.endTime) : item.endTime,
            state: item.State,
          })
        })

        if (data.isPage == false) {
          //無分頁

          var finish = ttlCount == data["index"] + rates.length ? 1 : 0
          var index = finish == 1 ? null : data["index"] + rates.length

          data = {
            dateRange: conf.dateRange,
            totalCount: ttlCount, //總筆數
            finish: finish,
            index: index,
            rates: rates,
          }
        } else {
          //有分頁

          data = {
            dateRange: conf.dateRange,
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            rates: rates,
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
    logger.error("[adminHandler][getList_exchangeRate] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//修改 operator OTP
handler.createOTP = function (msg, session, next) {
  try {
    var self = this
    var re_before = []
    var re_after = []
    var logData = {}

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "System"
    logData["FunctionAction"] = "EditOTPInMainUser"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    var log_mod_before = []
    var log_mod_after = []
    //var userSession = {};

    if (typeof msg.data === "undefined" || typeof msg.data.cid === "undefined") {
      next(null, {
        code: code.DB.PARA_FAIL,
      })
      return
    }

    const otpData = {
      Cid: msg.data.cid,
      IsAdmin: 1,
      HallId: -1,
    }

    m_async.waterfall(
      [
        function (cb) {
          adminDao.checkOperatorExist_byCid(msg.data, cb) //判斷有無此user
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_NOT_EXIST,
              data: null,
            })
            return
          }

          userDao.get_OtpCode_log(otpData, cb) //log_before
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_OTP_FAIL,
              data: null,
            })
            return
          }
          log_mod_before = log_mod_before.concat(r_data)

          var secret = m_otplib.authenticator.generateSecret() //secret key

          otpData["OTPCode"] = secret
          userDao.renew_OtpCode(otpData, cb) //del  + create
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.OTP_CREATE_FAIL,
              data: null,
            })
            return
          }

          userDao.get_OtpCode_log(otpData, cb) //log_after
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_OTP_FAIL,
              data: null,
            })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_After"] = JSON.stringify(log_mod_after)
          logData["Desc_Before"] = JSON.stringify(log_mod_before)

          logData["AdminId"] = session.get("cid") || ""
          logData["UserName"] = session.get("usrName") || ""
          logDao.add_log_admin(logData, cb) //insert log
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.LOG_FAIL,
            data: re_before,
          })
          return
        }
        let data = otpData
        if (msg.data.isSelf) {
          data["isSelf"] = msg.data.isSelf
        }
        next(null, {
          code: code.OK,
          data: data,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][createOTP] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//新增兌換幣別
// handler.add_currency = function (msg, session, next) {
//     var self = this;
//     //var userSession = {};

//     var logData = {};
//     logData['IP'] = msg.remoteIP;
//     logData['ModifiedType'] = 'add';
//     logData['FunctionGroupL'] = "System";
//     logData['FunctionAction'] = "AddCurrency";
//     logData['RequestMsg'] = JSON.stringify(msg);
//     logData['Desc_Before'] = '';
//     var log_mod_before = [];
//     var log_mod_after = [];

//     if (typeof msg.data === 'undefined' || typeof msg.data.currency === 'undefined') {
//         next(null, {
//             code: code.USR.CURRENCY_PARA_FAIL,
//             data: null
//         });
//         return;
//     }
//     msg.data.currency = msg.data.currency.toUpperCase();
//     var data = msg.data;
//     m_async.waterfall([
//         function (cb) {

//             adminDao.checkCurrencyExist(data, cb);
//         },
//         function (r_code, cb) {
//             if (r_code.code != code.OK) {
//                 next(null, {
//                     code: code.USR.CURRENCY_DUPLICATE,
//                     data: null
//                 });
//                 return;
//             }
//             adminDao.createCurrency(data, cb);
//         },
//         function (r_code, cb) {
//             if (r_code.code != code.OK) {
//                 next(null, {
//                     code: code.USR.CURRENCY_CREATE_FAIL,
//                     data: null
//                 });
//                 return;
//             }
//             adminDao.get_currency_log(data, cb); //取log
//         },
//         function (r_code, r_data, cb) {
//             if (r_code.code != code.OK) {
//                 next(null, {
//                     code: code.USR.LOAD_CURRENCY_FAIL,
//                     data: null
//                 });
//                 return;
//             }

//             log_mod_after = log_mod_after.concat(r_data);
//             logData['Desc_After'] = JSON.stringify(log_mod_after);

//             logData['AdminId'] = session.get('cid') || '';
//             logData['UserName'] = session.get('usrName') || '';
//             logDao.add_log_admin( logData, cb); //存log

//         }
//     ], function (r_none, r_code, r_data) {
//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.USR.CURRENCY_LOAD_FAIL,
//                 data: null
//             });
//             return;
//         }

//         next(null, {
//             code: code.OK,
//             data: 'Success'
//         });
//         return;
//     });
// }
//修改兌換幣別
// handler.edit_currency = function (msg, session, next) {

//     var self = this;
//     //var userSession = {};
//     var logData = {};
//     logData['IP'] = msg.remoteIP;
//     logData['ModifiedType'] = 'edit';
//     logData['FunctionGroupL'] = "System";
//     logData['FunctionAction'] = "EditCurrency";
//     logData['RequestMsg'] = JSON.stringify(msg);
//     logData['Desc_Before'] = '';
//     var log_mod_before = [];
//     var log_mod_after = [];
//     var currencies = [];

//     if (typeof msg.data === 'undefined' || typeof msg.data.origin_currency === 'undefined' || typeof msg.data.currency === 'undefined') {
//         next(null, {
//             code: code.USR.CURRENCY_PARA_FAIL,
//             data: null
//         });
//         return;
//     }
//     msg.data.origin_currency = msg.data.origin_currency.toUpperCase();
//     msg.data.currency = msg.data.currency.toUpperCase();
//     var data = msg.data;
//     m_async.waterfall([function (cb) {

//         var param = {
//             currency: data.origin_currency
//         }
//         adminDao.get_currency_log(param, cb); //log before -有無此筆資料
//     }, function (r_code, r_data, cb) {
//         var currency = r_data[0]['currency'];
//         if (r_code.code != code.OK || currency.length === 0) {
//             next(null, {
//                 code: code.USR.CURRENCY_NOT_EXIST,
//                 data: null
//             });
//             return;
//         }
//         log_mod_before = log_mod_before.concat(r_data);
//         if (data.origin_currency == data.currency) { //同幣別 -略
//             cb(null, {
//                 code: r_code.code
//             });
//         } else {
//             var param = {
//                 currency: data.currency
//             }
//             adminDao.checkCurrencyExist(data, cb);
//         }
//     }, function (r_code, cb) {
//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.USR.CURRENCY_DUPLICATE,
//                 data: null
//             });
//             return;
//         }

//         adminDao.modifyCurrency(data, cb);
//     }, function (r_code, cb) {
//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.USR.CURRENCY_MODIFY_FAIL,
//                 data: null
//             });
//             return;
//         }
//         adminDao.get_currency_log(data, cb); //log after
//     }, function (r_code, r_data, cb) {
//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.USR.LOAD_CURRENCY_FAIL,
//                 data: null
//             });
//             return;
//         }

//         log_mod_after = log_mod_after.concat(r_data);
//         logData['Desc_Before'] = JSON.stringify(log_mod_before);
//         logData['Desc_After'] = JSON.stringify(log_mod_after);

//         logData['AdminId'] = session.get('cid') || '';
//         logData['UserName'] = session.get('usrName') || '';

//         logDao.add_log_admin( logData, cb); //存log
//     }], function (r_none, r_code, r_data) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.USR.CURRENCY_LOAD_FAIL,
//                 data: null
//             });
//             return;
//         }

//         next(null, {
//             code: code.OK,
//             data: 'Success'
//         });
//         return;
//     });
// }

// 臨時密碼登入
handler.temp_logIn = function (msg, session, next) {
  try {
    var ip = ""
    if (typeof msg.remoteIP != "undefined" && msg.remoteIP.substr(0, 7) == "::ffff:") {
      ip = msg.remoteIP.substr(7)
    }

    var self = this
    var usrData = {}
    var user_level = ""
    var user = {}
    m_async.waterfall(
      [
        function (cb) {
          adminDao.getUser_byUserName_temp(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          user = r_data
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          //var nowTimeStamp = new Date().getTime();
          //r_data.TempEndTime = r_data.TempEndTime.replace(/-/g, '/');
          //var tempEndTimeStamp = new Date(r_data.TempEndTime).getTime();

          var utc_nowTimeStamp = timezone.getTimeStamp() //現在時間UTC timestamp
          var utc_TempEndTime = timezone.LocalToUTC(r_data.TempEndTime) //美東時間 localtime 轉UTC

          var utc_tempEndTimeStamp = timezone.getTimeStamp(utc_TempEndTime)
          if (utc_tempEndTimeStamp < utc_nowTimeStamp) {
            next(null, {
              code: code.USR.TEMP_PASSWORD_TIMEOUT,
              data: null,
            }) //逾期
            return
          }
          //密碼錯誤
          if (user.TempPassword != msg.data.password || user.TempMod != 1) {
            next(null, {
              code: code.USR.USER_PASSWORD_FAIL,
              data: null,
            }) //密碼失敗
            return
          }
          if (r_data.State === "S") {
            next(null, {
              code: code.USR.USER_DISABLE,
              data: null,
            })
            return
          }
          if (r_data.State === "D") {
            next(null, {
              code: code.USR.USER_NOT_EXIST,
              data: null,
            })
            return
          }
          if (r_data.State === "F") {
            next(null, {
              code: code.USR.USER_FREEZE,
              data: null,
            })
            return
          }
          //-----------------------IP----------------------

          var ip_list = {
            UserLevel: 1,
            Cid: user.AdminId,
            IpType: 1,
            Ip: ip,
            State: 1,
          }

          userDao.checkWhiteIp(ip_list, cb) // **白名單 IP**
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK && ip_check_status) {
          next(
            null,
            {
              code: code.USR.USER_IP_FAIL,
            },
            null
          )
          return
        }

        session.bind(user.AdminId + "_" + user.UserName)
        session.set("cid", user.AdminId)
        session.set("usrName", user.UserName)
        session.set("level", 1)
        session.set("agentId", "-1")
        session.pushAll()

        usrData = {
          Cid: user.AdminId,
          UserName: user.UserName,
          State: user.State,
        }
        next(null, {
          code: code.OK,
          data: usrData,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][temp_logIn] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

/*
修改暫時密碼
*/
handler.temp_set_password = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.cid === "undefined" ||
      typeof msg.data.password === "undefined"
    ) {
      next(null, {
        code: code.USR.CURRENCY_PARA_FAIL,
        data: null,
      })
      return
    }

    var self = this
    var logData = {}

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "Account"
    logData["FunctionAction"] = "EditPassword"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""

    var log_mod_before = []
    var log_mod_after = []
    //var userSession = {};

    m_async.waterfall(
      [
        function (cb) {
          adminDao.checkTempUsrExist_byCid(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_NOT_EXIST,
              data: null,
            })
            return
          }

          if (r_data.TempMod != 1) {
            next(null, {
              code: code.USR.USER_PASSWORD_FAIL,
              data: null,
            }) //已變更過
            return
          }

          // var nowTimeStamp = new Date().getTime();
          // r_data.TempEndTime = r_data.TempEndTime.replace(/-/g, '/');
          // var tempEndTimeStamp = new Date(r_data.TempEndTime).getTime();

          var utc_nowTimeStamp = timezone.getTimeStamp() //現在時間UTC timestamp
          var utc_TempEndTime = timezone.LocalToUTC(r_data.TempEndTime) //美東時間 localtime 轉UTC
          var utc_tempEndTimeStamp = timezone.getTimeStamp(utc_TempEndTime)

          if (utc_tempEndTimeStamp < utc_nowTimeStamp) {
            next(null, {
              code: code.USR.TEMP_PASSWORD_TIMEOUT,
              data: null,
            }) //逾期
            return
          }

          adminDao.getPassword_Cid(msg.data.cid, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_PASSWORD_FAIL,
              data: null,
            })
            return
          }

          log_mod_before = log_mod_before.concat(r_data)
          adminDao.modifyPassword_temp(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_MODIFY_FAIL,
              data: null,
            })
            return
          }

          adminDao.getPassword_Cid(msg.data.cid, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_PASSWORD_FAIL,
              data: null,
            })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)
          var user = r_data[0]["admin"]
          logData["AdminId"] = user[0]["AdminId"] || ""
          logData["UserName"] = user[0]["UserName"] || ""
          logDao.add_log_admin(logData, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.LOG_FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: {},
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][temp_set_password] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 取得臨時密碼
handler.getTempPassword = function (msg, session, next) {
  try {
    var self = this
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.mail === "undefined" ||
      typeof msg.data.userName === "undefined"
    ) {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }
    var tempPassword = ""
    var tempEndTime = ""
    var sys_config = self.app.get("sys_config")
    m_async.waterfall(
      [
        function (cb) {
          adminDao.checkIsExist_User(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_NOT_EXIST,
              data: null,
            })
            return
          }

          tempPassword = shared.makeid(6)

          // var tempEndTimeStamp = new Date().getTime() + (parseInt(conf.TEMP_PASSWORD_TIME) * 1000);
          // tempEndTime = timezone.setTime(new Date(tempEndTimeStamp));

          var utc_tempEndTimeStamp = timezone.getTimeStamp() + parseInt(sys_config.temp_password_time_sec) * 1000
          // 時效性為當時時間+1小時
          tempEndTime = timezone.formatTime(utc_tempEndTimeStamp)

          var pwdInfo = {
            Cid: r_data.AdminId,
            TempPassword: m_md5(tempPassword),
            TempEndTime: tempEndTime,
            TempMod: 1,
          }

          adminDao.setTempPassword(pwdInfo, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.CREATE_TEMP_FAIL,
              data: null,
            })
            return
          }

          var subject = ""
          var content = ""
          switch (msg.data.lang) {
            case "tw":
              subject = "重設密碼"
              content = mailUtil.tw(tempEndTime, msg.data.userName, tempPassword, conf.RESET_ADMIN_PWD_URL)
              break
            case "cn":
              subject = "重设密码"
              content = mailUtil.cn(tempEndTime, msg.data.userName, tempPassword, conf.RESET_ADMIN_PWD_URL)
              break
            case "en":
              subject = "Rest Password"
              content = mailUtil.en(tempEndTime, msg.data.userName, tempPassword, conf.RESET_ADMIN_PWD_URL)
              break
          }
          // var content = "<div style=\"margin:0 auto; width:600px\">" +
          //     " <p>DEAR ~ </p>" +
          //     " <p> Please complete the password change process before " + tempEndTime + ". </p>" +
          //     " <p>account name: <font color=\"red\">" + msg.data.userName + "</font></p>" +
          //     " <p>password: <font color=\"red\">" + tempPassword + "</font></p>" +
          //     " <p><a href='" + conf.RESET_ADMIN_PWD_URL + "'>web manage</a></p>" +
          //     "</div> ";

          const mailInfo = {
            to: [msg.data.mail],
            subject: subject,
            content: content,
          }

          mail.sendMail(mailInfo, cb) //發信
        },
      ],
      function (none, r_code) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.MAIL.MAIL_SEND_FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: {},
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][getTempPassword] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//歷史匯率的初始資料
// handler.rate_history_init = function (msg, session, next) {

//     var self = this;
//     var mainCurrency = "";
//     var currencies = [];

//     m_async.waterfall([
//         function (cb) {

//             self.app.rpc.config.configRemote.getMainCurrency(session, cb);

//         },
//         function (r_code, r_data, cb) {
//             mainCurrency = r_data; //主幣別

//             self.app.rpc.config.configRemote.getCurrenies(session, cb); //幣別

//         }
//     ], function (none, r_code, r_currencies) {

//         if (r_code.code != code.OK) {
//             next(null, r_code);
//             return;
//         }

//         for (idx in r_currencies) {
//             if (mainCurrency != r_currencies[idx].currency) currencies.push(r_currencies[idx].currency);
//         }

//         var data = {
//             mainCurrency: mainCurrency,
//             exCurrency: currencies
//         }

//         next(null, {
//             code: code.OK,
//             data: data
//         });
//         return;

//     });
// }
//判斷有無重複登入
handler.check_duplicate_mail = function (msg, session, next) {
  try {
    var self = this
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.mail === "undefined" ||
      msg.data.mail == "" ||
      typeof msg.data.level === "undefined"
    ) {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }

    m_async.waterfall(
      [
        function (cb) {
          if (msg.data.level == "admin") {
            adminDao.checkIsExist_Mail(msg.data, cb)
          } else {
            userDao.checkIsExist_Mail(msg.data, cb)
          }
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, r_code)
          return
        }
        if (r_data[0]["count"] > 0) {
          //重複
          next(null, {
            code: code.USR.MAIL_DUPLICATE,
          })
        } else {
          next(null, {
            code: code.OK,
          })
          return
        }
      }
    )
  } catch (err) {
    logger.error("[adminHandler][check_duplicate_mail] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

/**
 *
 * 沒gameId參數時為建立新遊戲，選取幣別預設的denom，
 * 有gameId參數時，為修改遊戲，選取該遊戲的幣別denom
 *
 *
 * @param {*} msg
 * @param {*} session
 * @param {*} next
 */
handler.edit_game_init = function (msg, session, next) {
  try {
    const { gameId } = msg.data

    m_async.waterfall(
      [
        function (cb) {
          if (gameId) {
            gameDao.get_game_currency_denom_setting({ gameId }, cb)
            return
          }

          configDao.getGameDefaultDenom(cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.CURRENCY_LOAD_FAIL,
            data: null,
          })
          return
        }

        const data = {
          default_denom: r_data,
        }

        next(null, {
          code: code.OK,
          data: data,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][edit_game_init] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//多錢包_轉移資金 (transfer_record)
handler.add_transfer_balance = function (msg, session, next) {
  try {
    var self = this

    if (
      typeof msg === "undefined" ||
      typeof msg.data.uid === "undefined" ||
      msg.data.uid === "" ||
      typeof msg.data.amount === "undefined" ||
      msg.data.amount === "" ||
      typeof msg.data.amount !== "number" ||
      typeof msg.data.type == "undefined" ||
      msg.data.type === "" ||
      typeof msg.remoteIP == "undefined"
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
      })
      return
    }

    //參數amount不是整數或少於0
    if (typeof msg.data.amount !== "number" || msg.data.amount <= 0) {
      next(null, {
        code: code.USR.AMOUNT_NOT_VALID,
      })
      return
    }

    var data = msg.data
    var player = {}
    var logData = {}
    var transferId = 0

    var sys_config = self.app.get("sys_config")
    var main_currency = sys_config.main_currency

    logData["IP"] = msg.remoteIP
    logData["Level"] = data.op_isAg
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "User"
    logData["FunctionAction"] = "Transfer"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    var log_mod_before = []
    var log_mod_after = []

    // 登入者是 Agent，檢查 來源帳號與登入者帳號是否一樣
    if (session.get("level") == 3 && msg.data.loginId.indexOf(msg.data.trans_cid) == -1) {
      next(null, {
        code: code.USR.USER_FAIL,
      })
      return
    }

    // 修改額度處理
    // isSub: 扣錢 (true), 加錢 (false)
    let modifyQuota = (player, isSub, cb) => {
      let useDestinationInfo = false
      if (player.txType == "deposit") {
        // 轉入; 來源帳號扣錢/目標帳號加錢
        if (!isSub) {
          useDestinationInfo = true
        }
      } else {
        // 轉出; 來源帳號加錢/目標帳號扣錢
        if (isSub) {
          useDestinationInfo = true
        }
      }

      let param = {
        amount: isSub ? -player.amount : player.amount,
        cid: useDestinationInfo ? player.playerId : player.trans_cid,
      }
      let isAg = useDestinationInfo ? player.isAg : player.trans_isAg

      if (isAg == 1) {
        // AD
        cb(null, { code: code.OK })
      } else if (isAg != 4) {
        // HA, AG
        param.currency = player.trans_curr
        userDao.modifyWalletQuota(param, (err, r_code, r_data) => {
          if (r_code.code == code.OK && useDestinationInfo) {
            // 更新目標帳號結果額度
            player.balance_after = r_data
          }
          cb(err, r_code)
        })
      } else {
        // PR
        param.hallId = player.hallId
        // 全轉帳 資料庫清0
        if (player.isTransferAll) param.amount = -player.balance_before

        userDao.modifyPlayerQuota(param, (err, r_code, r_data) => {
          if (r_code.code == code.OK && useDestinationInfo) {
            // 更新目標帳號結果額度
            player.balance_after = r_data
          }
          cb(err, r_code)
        })
      }
    }
    let customer_Data = []
    m_async.waterfall(
      [
        function (cb) {
          var user = {
            cid: data.cid,
          }
          // 查詢 customer
          userDao.get_player_byCid(user, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_LOAD_FAIL,
            })
            return
          }
          customer_Data = r_data
          var user = {
            cid: data.cid,
          }
          if (msg.data.isAg != 4) {
            // 查詢 Wallet
            gameDao.getWallet(user, cb)
          } else {
            // 查詢 customer
            userDao.get_player_byCid(user, cb)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_LOAD_FAIL,
            })
            return
          }
          if (msg.data.isAg != 4) {
            r_data.forEach((item) => {
              item["UserName"] = customer_Data[0]["UserName"]
              item["Account"] = item["Cid"]
              item["Quota"] = Number(item["Quota"]) / 1000
              delete item["Cid"]
            })
          } else {
            r_data.forEach((item) => {
              item["Player Account"] = item["Cid"]
              item["Player Name"] = item["UserName"]
              item["Quota"] = Number(item["Quota"]) / 1000
              delete item["Cid"]
              delete item["Upid"]
              delete item["HallId"]
              delete item["UserName"]
              delete item["IsAg"]
            })
          }
          logData["UserName"] = customer_Data[0]["UserName"]

          let before_data = []
          let bData = {
            customer: r_data,
          }
          before_data.push(bData)
          log_mod_before = log_mod_before.concat(before_data)

          player = {
            isAg: customer_Data[0]["IsAg"],
            playerId: customer_Data[0]["Cid"],
            hallId: customer_Data[0]["HallId"],
            agentId: customer_Data[0]["Upid"],
            main_currency: main_currency,
            // currency: r_data[0]['Currency'],
            currency: data.trans_curr,
            // balance_before:  r_data[0]['Quota'],//線上目前金額
            balance_before: data.user_amount_before,
            amount: data.amount,
            txType: data.type,
            uid: data.uid,
            op_cid: data.op_cid, // 操作者帳號
            op_isAg: data.op_isAg, // 操作者層級
            trans_cid: data.trans_cid, // 轉入(出)帳號來源
            trans_curr: data.trans_curr, // 轉入(出)帳號來源 幣別
            trans_isAg: data.trans_isAg, // 轉入(出)帳號來源，層級 1: admin 2: hall 3: agent 4: player
            trans_amount_before: data.trans_from_before, // 轉入(出)帳號來源，轉移前餘額
            isTransferAll: data.isTransferAll, // 判斷是否全轉出
          }

          if (player.trans_isAg == 1) {
            // AD
            cb(null, { code: code.OK }, null)
          } else {
            let param = {
              cid: data.trans_cid,
            }
            userDao.get_user_byId2(param, cb)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_LOAD_FAIL,
            })
            return
          }
          if (player.trans_isAg != 1) {
            if (r_data.length == 0) {
              next(null, {
                code: code.USR.USER_LOAD_FAIL,
              })
              return
            }

            data.trans_isAg = r_data[0]["IsAg"]
            player.trans_isAg = r_data[0]["IsAg"] // 修正來源帳號 IsAg
          }

          // 轉入
          if (data.type == "deposit") {
            if (player.trans_isAg == 1) {
              // admin
              player["trans_amount_after"] = 0
            } else {
              // 轉入(出)帳號來源，轉移後餘額，層級不是 admin 時，才會扣除上線的錢
              player["trans_amount_after"] = utils.number.sub(data.trans_from_before, data.amount)
            }

            player["balance_after"] = utils.number.add(data.user_amount_before, data.amount)
          }

          // 轉出
          if (data.type == "withdraw") {
            if (player.trans_isAg == 1) {
              // 轉入(出)帳號來源 admin
              player["trans_amount_after"] = 0
            } else {
              // 轉入(出)帳號來源，轉移後餘額，層級不是 admin 時，才會加上上線的錢
              player["trans_amount_after"] = utils.number.add(data.trans_from_before, data.amount)
            }
            player["balance_after"] = utils.number.sub(data.user_amount_before, data.amount)
          }

          // if (data.type == 'deposit') {  //營運商 -> 遊戲商(轉入)
          //     player['balance_after'] = r_data[0]['Quota'] + parseInt(data.amount);
          // }
          // if (data.type == 'withdraw') {  //遊戲商 -> 營運商(轉出)
          //     player['balance_after'] = r_data[0]['Quota'] - parseInt(data.amount);
          // }
          if (data.type == "withdraw_all") {
            //遊戲商 -> 營運商
            player["balance_after"] = 0
          }

          if (player["balance_after"] < 0 || player["trans_amount_after"] < 0) {
            next(null, {
              code: code.USR.INSUFFICIENT_FUNDS,
            })
            return
          }
          if (main_currency == player["currency"]) {
            cb(null, { code: code.OK }, [{ CryDef: 1 }])
          } else {
            configDao.getNowExchangeRate({ currency: sys_config.main_currency, exCurrency: player["currency"] }, cb) //取當下匯率
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.RATE_NOT_VALID,
            })
            return
          }

          player["CryDef"] = r_data[0]["CryDef"]
          // player['IsAg'] = data.isAg; // 目標帳號層級

          userDao.checkTransferStatus(player, cb) //判斷有無資料
        },
        function (r_code, r_data, cb) {
          if (r_code.code == code.OK && r_data["State"] == 1) {
            next(null, {
              code: code.USR.DUPLICATE_UID,
            })
            return
          }

          userDao.transferBalance(player, cb) //轉移資金 -新增一筆狀態0
        },
        function (r_code, r_txId, cb) {
          // 先處理扣錢
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.TRANSFER_NOT_VALID,
            })
            return
          }
          transferId = r_txId
          modifyQuota(player, true, cb)
        },
        function (r_code, cb) {
          // 再處理加錢
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.INSUFFICIENT_FUNDS,
            })
            return
          }
          modifyQuota(player, false, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.TRANSFER_NOT_VALID,
            })
            return
          }
          var transferInfo = {
            TxId: transferId,
            State: 1,
          }

          userDao.modifyTransferBalanceStatus(transferInfo, cb) //更新交易紀錄狀態
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.TRANSFER_NOT_VALID,
            })
            return
          }

          var user = {
            cid: data.cid,
          }
          if (player.isAg != 4) {
            // 查詢 Wallet
            gameDao.getWallet(user, cb)
          } else {
            // 查詢 customer
            userDao.get_player_byCid(user, cb)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_LOAD_FAIL,
            })
            return
          }
          if (msg.data.isAg != 4) {
            r_data.forEach((item) => {
              item["UserName"] = customer_Data[0]["UserName"]
              item["Account"] = item["Cid"]
              item["Quota"] = Number(item["Quota"]) / 1000
              delete item["Cid"]
            })
          } else {
            r_data.forEach((item) => {
              item["Player Account"] = item["Cid"]
              item["Player Name"] = item["UserName"]
              item["Quota"] = Number(item["Quota"]) / 1000
              delete item["Cid"]
              delete item["Upid"]
              delete item["HallId"]
              delete item["UserName"]
              delete item["IsAg"]
            })
          }
          let after_data = []
          let aData = {
            customer: r_data,
          }
          after_data.push(aData)
          log_mod_after = log_mod_after.concat(after_data)
          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["ActionLevel"] = session.get("level") || ""
          logData["ActionCid"] = session.get("cid") || ""
          logData["ActionUserName"] = session.get("usrName") || ""

          logDao.add_log_customer(logData, cb)
        },
      ],
      function (none, r_code) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.LOG_FAIL, data: null })
          return
        }

        var res = {
          uid: data.uid,
          amount: data.amount,
          type: data.type,
          balance_before: player["balance_before"],
          balance_after: player["balance_after"],
        }

        next(null, {
          code: code.OK,
          data: res,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][add_transfer_balance] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// var getLevelIdx = function (name) {
//     var idx = 0;
//     if (!!Levels) {
//         var count = 0;
//         var length = Levels.length;

//         for (count = 0; count < length; count++) {
//             if (Levels[count].AuthorityTypeName === name) {
//                 idx = Levels[count].AuthorityType;
//                 break;
//             }
//         }
//     }
//     return idx;
// };

// 檢查有無重複 DC
handler.check_duplicate_dc = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.dc === "undefined" ||
      msg.data.dc == "" ||
      typeof msg.data.level === "undefined"
    ) {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }

    m_async.waterfall(
      [
        function (cb) {
          userDao.checkIsExist_DC(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, r_code)
          return
        }
        if (r_data[0]["count"] > 0) {
          // 重複
          next(null, {
            code: code.USR.DC_DUPLICATE,
          })
        } else {
          next(null, {
            code: code.OK,
          })
          return
        }
      }
    )
  } catch (err) {
    logger.error("[adminHandler][check_duplicate_dc] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 取得編輯初始資訊
handler.getAdminEditInfo_op = function (msg, session, next) {
  try {
    var self = this
    var userInfo = {}

    m_async.waterfall(
      [
        function (cb) {
          adminDao.get_admin_byId(
            {
              AdminId: session.get("cid"),
            },
            cb
          )
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_NOT_EXIST,
              data: null,
            })
            return
          }
          r_data = r_data[0].admin[0]

          userInfo["UserName"] = r_data.UserName
          userInfo["NickName"] = r_data.NickName
          userInfo["AuthorityTemplateID"] = r_data.AuthorityTemplateID
          userInfo["State"] = r_data.State
          userInfo["Email"] = r_data.Email

          self.app.rpc.config.configRemote.getAuthorityTemp(
            session,
            {
              authType: 1,
            },
            cb
          )
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.FAIL,
            data: null,
          })
          return
        }
        userInfo["AuthorityTemplate"] = r_data[0].desc
        userInfo["note"] = r_data[0].note

        next(null, {
          code: code.OK,
          data: userInfo,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][getAdminEditInfo_op] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 關閉 Admin OTP
handler.closeOTP = function (msg, session, next) {
  try {
    var re_before = []
    var logData = {}

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "User"
    logData["FunctionAction"] = "CloseOTP"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    var log_mod_before = []
    var log_mod_after = []

    if (typeof msg.data === "undefined" || typeof msg.data.cid === "undefined") {
      next(null, {
        code: code.DB.PARA_FAIL,
      })
      return
    }

    const otpData = {
      Cid: msg.data.cid,
      IsAdmin: 1,
      HallId: -1,
    }

    m_async.waterfall(
      [
        function (cb) {
          // 判斷有無此user
          adminDao.checkOperatorExist_byCid(msg.data, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_NOT_EXIST,
              data: null,
            })
            return
          }

          userDao.get_OtpCode_log(otpData, cb) // log_before
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_OTP_FAIL,
              data: null,
            })
            return
          }
          log_mod_before = log_mod_before.concat(r_data)

          userDao.close_OtpCode(otpData, cb) //del  + create
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.OTP_CLOSE_FAIL,
              data: null,
            })
            return
          }

          userDao.get_OtpCode_log(otpData, cb) // log_after
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOG_OTP_FAIL,
              data: null,
            })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_After"] = JSON.stringify(log_mod_after)
          logData["Desc_Before"] = JSON.stringify(log_mod_before)

          logData["AdminId"] = session.get("cid") || ""
          logData["UserName"] = session.get("usrName") || ""
          logDao.add_log_admin(logData, cb) // insert log
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.LOG_FAIL,
            data: re_before,
          })
          return
        }

        let data = otpData
        if (msg.data.isSelf) {
          data = {
            OTPCode: otpData["OTPCode"],
            isSelf: msg.data.isSelf,
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
    logger.error("[adminHandler][closeOTP] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 修改其他權限 (上線被調整時，下線權限一起調整)
handler.modify_AuthorityOthers = function (msg, session, next) {
  try {
    var self = this
    var logData = {}
    var params = {}

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "System"
    logData["FunctionAction"] = "EditAuthority"
    logData["RequestMsg"] = JSON.stringify(msg)
    var log_mod_before = []
    var log_mod_after = []

    if (
      typeof msg.data.id === "undefined" ||
      typeof msg.data.funcIds === "undefined" ||
      typeof msg.data.id != "number" ||
      typeof msg.data.cid === "undefined" ||
      typeof msg.data.modifyId === "undefined"
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }

    var data = {
      tmpId: msg.data.id,
      funcIds: msg.data.funcIds,
      cid: msg.data.cid == "-1" ? 0 : msg.data.cid,
      modifyId: msg.data.modifyId,
      addNewItem: msg.data.addNewItem,
    }

    m_async.waterfall(
      [
        function (cb) {
          userDao.getCustsByAuthorityID(data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_LOAD_FAIL,
              data: null,
            })
            return
          }

          params = {
            modifyAuthorityIds: r_data,
          }

          adminDao.getAuthorityOthers(params, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.AUTH_TMP_LOAD_FAIL,
              data: null,
            })
            return
          }

          var res_data = []
          var info = {
            back_office_authority: r_data,
          }
          res_data.push(info)
          log_mod_before = log_mod_before.concat(res_data)

          var authorityData = []
          r_data.forEach((item) => {
            var newFuncIds = {}
            if (msg.data.addNewItem) {
              // 增加 Authority Functions
              newFuncIds = item.AuthorityJson.split(",")
                .concat(msg.data.funcIds.split(","))
                .filter((value) => {
                  if (
                    item.AuthorityJson.split(",").indexOf(value) == -1 ||
                    msg.data.funcIds.split(",").indexOf(value) == -1
                  ) {
                    return value
                  }
                })
            } else {
              // 減少 Authority Functions
              newFuncIds = msg.data.funcIds
                .split(",")
                .concat(item.AuthorityJson.split(","))
                .filter((value) => {
                  if (msg.data.funcIds.split(",").indexOf(value) == -1) {
                    return value
                  }
                })
            }

            authorityData.push({
              cid: item.cid,
              templateId: item.AuthorityTemplateID,
              modifyAuthorityIds: newFuncIds.join(","),
              modifyId: session.get("cid"),
            })
          })

          adminDao.updateAuthorityOthers(authorityData, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.AUTH_TMP_MODIFY_FAIL,
              data: null,
            })
            return
          }

          adminDao.getAuthorityOthers(params, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.AUTH_TMP_LOAD_FAIL,
              data: null,
            })
            return
          }

          var res_data = []
          var info = {
            back_office_authority: r_data,
          }
          res_data.push(info)
          log_mod_after = log_mod_after.concat(res_data)

          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          if (session.get("level") == 1) {
            logData["AdminId"] = session.get("cid") || ""
            logData["UserName"] = session.get("usrName") || ""
            logDao.add_log_admin(logData, cb)
          } else {
            logData["ActionLevel"] = session.get("level") || ""
            logData["ActionCid"] = session.get("cid") || ""
            logData["ActionUserName"] = session.get("usrName") || ""
            logDao.add_log_customer(logData, cb)
          }
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.AUTH.LOG_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[adminHandler][modify_AuthorityOthers] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
