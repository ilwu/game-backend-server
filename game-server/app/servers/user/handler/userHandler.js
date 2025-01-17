var logger = require("pomelo-logger").getLogger("userHandler", __filename)
var m_async = require("async")
var m_otplib = require("otplib")
var m_md5 = require("md5")

var code = require("../../../util/code")
var conf = require("../../../../config/js/conf")
var userDao = require("../../../DataBase/userDao")
var gameDao = require("../../../DataBase/gameDao")
var adminDao = require("../../../DataBase/adminDao")
var configDao = require("../../../DataBase/configDao")
var logDao = require("../../../DataBase/logDao")
var bettingDao = require("../../../DataBase/bettingDao")
var timezone = require("../../../util/timezone")

var shared = require("../../../util/sharedFunc")
const { default: ShortUniqueId } = require("short-unique-id")

var mailService = require("../../../services/mailService")
var mail = new mailService()
var utils = require("../../../util/utils")
var mailUtil = require("../../../util/mail")
var consts = require("../../../share/consts")

var ip_check_status = false

const requestService = require("../../../services/requestService")
const UserLevelService = require("../../../services/UserLevelService")

const uid = new ShortUniqueId()
const short = require("short-uuid")

const { inspect } = require("util")
const { isEmpty } = utils
const { BN } = require("../../../util/number")

const moment = require("moment-timezone")

module.exports = function (app) {
  const redisCache = require("../../../controllers/redisCache")(app)

  return new Handler(app, redisCache)
}

const Handler = function (app, cache) {
  try {
    this.app = app
    this.redisCache = cache

    m_async.parallel(
      {
        C: function (cb) {
          userDao.getLevel(function (err, levels) {
            if (err.code === code.OK) {
              cb(null, levels)
            } else {
              cb(null, null)
            }
          })
        },
      },
      function (errs, results) {
        if (results.C) {
          Levels = results.C
        }
      }
    )
  } catch (err) {
    logger.error("[userHandler][Handler] catch err", err)
  }
}

var handler = Handler.prototype

handler.joinHA = function (msg, session, next) {
  try {
    if (typeof msg === "undefined" || typeof msg.data.usr === "undefined" || typeof msg.data.games === "undefined") {
      next(null, { code: code.FAIL, data: null })
      return
    }
    msg.data.usr.isAg = 2
    msg.data.usr.quota = 0

    var self = this
    var newUser = {}
    var secret = ""
    var logData = {}
    var log_mod_after = []
    var otpData = {}
    //var userSession = {};
    var user_set_id = 0
    var cid = ""
    var sys_config = self.app.get("sys_config")
    msg.data.usr["walletType"] = msg.data.usr.isSingleWallet

    m_async.waterfall(
      [
        function (cb) {
          userDao.checkUsrExist(msg.data.usr, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_DUPLICATE, data: null })
            return
          }
          userDao.checkIsExist_DC(msg.data.usr, cb) // 判斷是否重複 DC
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, r_code)
            return
          }
          if (r_data[0]["count"] > 0) {
            // 重複
            next(null, { code: code.USR.DC_DUPLICATE, data: null })
            return
          }

          cid = uid.randomUUID(12)
          msg.data.usr["cid"] = cid
          userDao.createUser_Hall(msg.data.usr, cb)
        },
        function (r_code, r_cid, cb) {
          newUser = {
            user: {
              cid: r_cid,
              upid: typeof msg.data.usr.upid != "undefined" ? msg.data.usr.upid : -1,
              hallid: typeof msg.data.usr.hallid != "undefined" ? msg.data.usr.hallid : -1,
            },
            games: msg.data.games,
          }

          logData["Cid"] = r_cid
          logData["UserName"] = msg.data.usr.name

          secret = m_otplib.authenticator.generateSecret() //secret key

          otpData = {
            Cid: newUser["user"]["cid"],
            IsAdmin: 0,
            HallId: -1,
            OTPCode: secret,
          }
          userDao.createUser_newOtpCode(otpData, cb) //建立OTP code
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.OTP_CREATE_FAIL, data: null })
            return
          }

          //一筆-user_setting預設值
          var set_param = {
            Cid: logData["Cid"],
            IsAdmin: 0,
            CountsOfPerPage: sys_config.counts_per_page,
            HourDiff: sys_config.time_diff_hour,
          }
          userDao.set_user_setting(set_param, cb)
        },
        function (r_code, r_id, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.SETTING_CREATE_FAIL, data: null })
            return
          }
          user_set_id = r_id

          //建立 白名單
          var ip_whitelist = []
          for (var i in msg.data.usr["ip_whitelist"]) {
            if (
              typeof msg.data.usr["ip_whitelist"][i]["ip"] != "undefined" &&
              msg.data.usr["ip_whitelist"][i]["ip"] != "" &&
              typeof msg.data.usr["ip_whitelist"][i]["type"] != "undefined" &&
              msg.data.usr["ip_whitelist"][i]["type"] != ""
            ) {
              ip_whitelist.push({
                Cid: newUser["user"]["cid"],
                UserLevel: 2,
                IpType: msg.data.usr["ip_whitelist"][i]["type"],
                Desc: msg.data.usr["ip_whitelist"][i]["desc"],
                Ip: msg.data.usr["ip_whitelist"][i]["ip"],
                Name: msg.data.usr["ip_whitelist"][i]["name"],
                State: msg.data.usr["ip_whitelist"][i]["state"],
              })
            }
          }

          if (ip_whitelist.length > 0) {
            userDao.CreateIPWhiteList(ip_whitelist, cb)
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.WHITE_IN_CREATE_FAIL, data: null })
            return
          }

          //--->發送信件 (otp)

          // var service = "backend";
          // var user = msg.data.email; //mail
          // var authParam = m_otplib.authenticator.keynnnnnuri(user, service, secret); //qrcode img url
          // var qr_code_src = "https://chart.googleapis.com/chart?cht=qr&chs=250x250&chl=" + encodeURI(authParam);

          // var content = "<div style=\"margin:0 auto; width:600px\">" +
          //     " <p>DEAR ~ </p>" +
          //     " <p>account name: " + msg.data.usr.name + "</p>" +
          //     " <p>password: " + msg.data.usr.password + "</p>" +
          //     " <p>Awesome OTP: </p>" +
          //     " <p>please add this qr code by Google Authenticator <br>" +
          //     " <img id='qrcode' src='" + qr_code_src + "'></img>" +
          //     " <p><a href='#'>web manage</a></p>" +
          //     "</div > ";

          // mailInfo = {
          //     to: [msg.data.usr.email],
          //     subject: 'Web Auth Info',
          //     content: content
          // }
          // self.app.rpc.mail.mailRemote.sendMail(session, mailInfo, cb);

          // }, function (r_code, cb) {

          // if (r_code.code != code.OK) {
          //     next(null, { code: r_code.code, data: null });
          //     return;
          // }

          self.app.rpc.game.gameRemote.joinGameToHall(session, newUser, cb) //建立遊戲
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.GAME_JOIN_FAIL, data: null })
            return
          }
          //取-user_log
          var search_log = []
          search_log.push({
            table: "customer",
            search_key: "Cid",
            search_value: logData["Cid"],
          })

          search_log.push({
            table: "hall",
            search_key: "HallId",
            search_value: logData["Cid"],
          })

          userDao.get_ha_byId(search_log, cb)
        },
        function (r_code, r_data, cb) {
          //取-user_log
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_USER_FAIL, data: null })
            return
          }
          log_mod_after = r_data
          userDao.get_OtpCode_byId(otpData, cb) //otp_code
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_OTP_FAIL, data: null })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)

          //user_白名單
          var white_user = {
            Cid: newUser["user"]["cid"],
            UserLevel: 2,
            IpType: 1,
            State: [1, 0],
          }
          userDao.get_white_list(white_user, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_WHITE_FAIL, data: null })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)

          userDao.get_games_byCid({ Cid: logData["Cid"] }, cb) //game
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_GAME_FAIL, data: null })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)

          userDao.get_user_setting({ Uid: user_set_id }, cb) //user_setting初始
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_SETTING_FAIL, data: null })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)

          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["ActionCid"] = session.get("cid") || ""
          logData["ActionUserName"] = session.get("usrName") || ""
          logData["ActionLevel"] = session.get("level")
          logData["Level"] = 2
          logData["IP"] = msg.remoteIP
          logData["ModifiedType"] = "add"
          logData["FunctionGroupL"] = "User"
          logData["FunctionAction"] = "AddUser"
          logData["RequestMsg"] = JSON.stringify(msg)
          logData["Desc_Before"] = ""
          logDao.add_log_customer(logData, cb)
        },
        function (r_code, r_data, cb) {
          // 有勾選自動新增帳號(正式代理)
          if (typeof msg.data.usr.addAutos != "undefined" && msg.data.usr.addAutos[0] == 1) {
            msg.data.usr["hallid"] = cid
            msg.data.usr["upid"] = cid
            self.joinAutoAGAccts(msg, session, cb)
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          // 有勾選自動新增帳號(測試代理) ['2', '3'] 或 ['1', '2', '3'] 1: 正式代理 2： 測試代理 3: 測試玩家
          if (
            typeof msg.data.usr.addAutos != "undefined" &&
            (msg.data.usr.addAutos[0] == 2 || msg.data.usr.addAutos[1] == 2)
          ) {
            msg.data.usr["hallid"] = cid
            msg.data.usr["upid"] = cid
            self.joinAutoDemoAGAccts(msg, session, cb)
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.LOG_FAIL, data: null })
          return
        }
        //console.log("------------Success-------------");
        next(null, { code: code.OK, data: "Success" })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][joinHA] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 新增管理者(Hall、Agent)
handler.joinSubHA = function (msg, session, next) {
  try {
    var self = this
    var otpData = {}
    var logData = {}
    var log_mod_after = []
    var currency = ""
    var dc = ""
    var securityKey = ""
    //var userSession = {};
    var sys_config = self.app.get("sys_config")

    var cid = ""
    let isAg
    logData["Level"] = 2
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "add"
    logData["FunctionGroupL"] = "User"
    logData["FunctionAction"] = "AddUser"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""

    m_async.waterfall(
      [
        function (cb) {
          userDao.checkUsrExist(msg.data.usr, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_DUPLICATE, data: null })
            return
          }

          userDao.get_user_byId({ Cid: msg.data.usr.isCreateHall ? msg.data.usr.hallid : msg.data.usr.upid }, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_LOAD_FAIL,
            })
            return
          }

          isAg = r_data[0]["customer"][0]["IsAg"]
          var param = {
            level: isAg,
            userId: isAg == 2 ? msg.data.usr.hallid : msg.data.usr.upid,
          }
          // 從 game.customer 找出 hall、agent 相關的資訊
          userDao.getUser_byId(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_CREATE_FAIL, data: null })
            return
          }

          this.currency = r_data[0]["Currency"]
          this.dc = r_data[0]["DC"]

          var usr_data = {
            cid: uid.randomUUID(12),
            name: msg.data.usr.name,
            password: msg.data.usr.password,
            nickName: msg.data.usr.nickName,
            isSub: 1,
            upid: msg.data.usr.userId != undefined ? msg.data.usr.upid : -1,
            hallid: msg.data.usr.hallid,
            authorityId: msg.data.usr.authorityId,
            isDemo: 0,
            isSingleWallet: 0,
            state: msg.data.usr.state,
            currencies: this.currency,
            ip_whitelist_in: "",
            ip_whitelist_out: "",
            api_outdomain: "",
            halldesc: "test",
            api_hallownername: "",
            email: msg.data.usr.email,
            birthday: typeof msg.data.usr.birthday != "undefined" ? msg.data.usr.birthday.substr(0, 10) : "",
            dc: this.dc,
            game_currency: msg.data.usr.game_currency,
            isAg: isAg,
          }
          userDao.createUser_ResellerOrAgent(usr_data, cb)
        },
        function (r_code, r_cid, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_CREATE_FAIL, data: null })
            return
          }

          logData["Cid"] = r_cid
          logData["UserName"] = msg.data.usr.name

          var secret = m_otplib.authenticator.generateSecret() //secret key

          otpData = {
            Cid: r_cid,
            IsAdmin: 0,
            HallId: msg.data.usr.hallid,
            OTPCode: secret,
          }

          userDao.createUser_newOtpCode(otpData, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.OTP_CREATE_FAIL, data: null })
            return
          }

          //一筆-user_setting預設值
          var set_param = {
            Cid: logData["Cid"],
            IsAdmin: 0,
            CountsOfPerPage: sys_config.counts_per_page,
            HourDiff: sys_config.time_diff_hour,
          }
          userDao.set_user_setting(set_param, cb)
        },
        function (r_code, r_id, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.SETTING_CREATE_FAIL, data: null })
            return
          }

          userDao.get_user_setting({ Uid: r_id }, cb) //user_setting初始
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_SETTING_FAIL, data: null })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)

          userDao.get_user_byId({ Cid: logData["Cid"] }, cb)
        },
        function (r_code, r_data, cb) {
          //user ID
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_USER_FAIL, data: null })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)
          userDao.get_OtpCode_byId(otpData, cb)
        },
        function (r_code, r_data, cb) {
          //user otp
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_OTP_FAIL, data: null })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["ActionLevel"] = session.get("level") || ""
          logData["ActionCid"] = session.get("cid") || ""
          logData["ActionUserName"] = session.get("usrName") || ""

          logDao.add_log_customer(logData, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.LOG_FAIL, data: null })
          return
        }
        next(null, { code: code.OK, data: "Success" })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][joinSubHA] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

/**
 * 修改 Hall 狀態(一般/停用/凍結)
 */
handler.modifyHA = function (msg, session, next) {
  try {
    const userLevel = new UserLevelService({ session })

    const { currentUserLevel } = userLevel.getUserLevelData()

    const userId = msg.data.usr.userId || msg.data.usr.cid
    let hallName = ""
    let stateId = msg.data.usr.state

    var log_mod_before = []
    var log_mod_after = []
    var logData = {}
    var origModifyLvl = -1
    var origState = "U"

    if (session.get("cid") === userId && session.get("level") === 2) {
      //個人明細(不變更game)
      logData["FunctionGroupL"] = "Account"
      logData["FunctionAction"] = "EditAccount"
    } else {
      logData["FunctionGroupL"] = "User"
      logData["FunctionAction"] = "EditUser"
    }

    logData["Level"] = 2
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    logData["Cid"] = userId

    // 是否連動更新下層的hall
    let isUpdateLowerId = false

    //修改後
    var search_log = []
    search_log.push({
      table: "customer",
      search_key: "Cid",
      search_value: logData["Cid"],
    })

    search_log.push({
      table: "hall",
      search_key: "HallId",
      search_value: logData["Cid"],
    })
    //var userSession = {};
    let jackpot_before = [] //JP開關狀態 操作前
    let jackpot_after = [] //JP開關狀態 操作後
    let downHallCid = [] //分銷商底下cid包含自己
    m_async.waterfall(
      [
        function (cb) {
          //只有修改狀態才會取得分銷商子帳號
          if (msg.data.usr.state != null && msg.data.usr.state != "") {
            const param = { cid: userId }
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
          if (msg.data.usr.state != null && msg.data.usr.state != "") {
            let downLine = r_data.pop()
            downHallCid = downLine[Object.keys(downLine)[0]].split(",")
            downHallCid.splice(0, 1) // 刪除第0 & 1筆: '$,rUpjbUmzFkAV(自己),7ejdZ2jNAZvQ, ...'
          }
          userDao.getJackpotSwitchState({ cid: userId }, cb) //讀取彩金開關 操作前
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.DB.LOAD_DATA_FAIL,
              data: null,
            })
            return
          }
          if (msg.data.usr.jackpot != null) {
            //只有帶有jp開關參數走這邊
            const [jp] = r_data.map((item) => {
              return item.IsJackpotEnabled
            })
            jackpot_before = jp
            userDao.updateJackpotSwitchState(msg.data.usr.jackpot, userId, cb)
          } else {
            // 修改狀態或是非編輯分銷商列表走這邊
            cb(null, { code: code.OK })
          }
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.ACTION_TIME_FAIL,
              data: null,
            })
            return
          }
          jackpot_after = msg.data.usr.jackpot
          userDao.checkUsrExist_byCid(userId, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST, data: null })
            return
          }

          userDao.get_ha_byId(search_log, cb) //before -user
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_USER_FAIL, data: null })
            return
          }
          //user帳號
          logData["UserName"] = r_data[0]["customer"][0]["UserName"]
          log_mod_before = log_mod_before.concat(r_data)

          hallName = logData["UserName"]

          // 原修改者 level
          origModifyLvl = r_data[0]["customer"][0]["ModifyLvl"]
          // 原狀態
          origState = r_data[0]["customer"][0]["State"]

          if (session.get("cid") === userId && session.get("level") === 2) {
            //個人明細(不變更game)
            cb(null, { code: code.OK }, null)
          } else {
            userDao.get_games_byCid({ Cid: logData["Cid"] }, cb) //before -game
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_GAME_FAIL, data: null })
            return
          }

          if (session.get("cid") != userId) {
            log_mod_before = log_mod_before.concat(r_data)
          }

          //遊戲的各幣別denom判斷**之後再補 @@

          // 修改者 level
          msg.data.usr.modifyLvl = currentUserLevel
          // 狀態改回與原始狀態不同時，修改者必需小於等於原修改者，否則不可修改
          if (
            typeof msg.data.usr.state != "undefined" &&
            msg.data.usr.state !== origState &&
            session.get("level") > origModifyLvl
          ) {
            cb(null, { code: code.AUTH.AUTH_INVALID }, null)
          } else {
            // 狀態改回 "一般"，則 ModifyLvl 寫 99
            if (msg.data.usr.state === "N") {
              msg.data.usr.modifyLvl = 99
            }

            /* 登入者ID 與 被修改資料的ID 相同時
                    不能修改: 狀態 / 交收幣別 / 開放幣別 / 錢包類型 / 權限 */
            if (userId == session.get("cid")) {
              msg.data.usr["edit"] = "self"
              userDao.getHall_byHallId(userId, cb) // 找該hall修改前的開放幣別
            } else {
              cb(null, { code: code.OK }, null)
            }
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            // next(null, { code: code.USR.LOG_GAME_FAIL, data: null });
            cb(null, r_code, null)
            return
          }

          if (r_data) {
            msg.data.usr["Usable_Currency"] = r_data[0].hall[0].Currencies.split(",") // 原始開放幣別
          }

          userDao.modifyUser_Hall_v2({ ...msg.data.usr, cid: userId, games: msg.data.games }, cb)
        },
        function (r_code, cb) {
          if (r_code.code === code.AUTH.AUTH_INVALID) {
            next(null, { code: r_code.code, data: null })
            return
          } else if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_MODIFY_FAIL, data: null })
            return
          }

          msg.data.usr["isTransferLine"] = false

          // 查詢該hall下面的subHall id list

          userDao.getChildList({ cid: userId }, cb)
        },
        function (r_code, r_data, cb) {
          if (msg.data.games && Array.isArray(msg.data.games)) {
            const gameIdList = msg.data.games.reduce(
              (acc, cur) => {
                const { sw, gameId } = cur

                if (sw) {
                  acc.enabledList.push(gameId)
                } else {
                  acc.disabledList.push(gameId)
                }

                return acc
              },
              { enabledList: [], disabledList: [] }
            )

            const list = r_data[0].list.split(",")

            // 去掉$，此列表已包含自己的cid
            const [, ...lowerIdList] = list

            const params = {
              isUpdateLowerId,
              msgData: msg.data,
              cid: userId,
              lowerIdList,
              gameOpenIds: gameIdList.enabledList,
              gameCloseIds: gameIdList.disabledList,
            }

            gameDao.modifyGameSetting(params, cb)
          } else {
            cb(null, { code: code.OK }, {})
          }
        },
        function (r_code, r_data, cb) {
          //變更USER狀態 ->下線變更包含自己
          if (msg.data.usr.state != null && msg.data.usr.state != "" && downHallCid != "" && downHallCid.length > 0) {
            var param = {
              hallId: downHallCid,
              state: msg.data.usr.state,
            }
            userDao.changeStatusDownUsers(param, cb)
          } else {
            cb(null, { code: code.OK })
          }
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_MODIFY_FAIL, data: null })
          }
          userDao.get_ha_byId(search_log, cb) //after -user
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_USER_FAIL, data: null })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)
          if (session.get("cid") === userId && session.get("level") === 2) {
            //個人明細(不變更game)
            cb(null, { code: code.OK }, null)
          } else {
            userDao.get_games_byCid({ Cid: logData["Cid"] }, cb) //after -game
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_GAME_FAIL, data: null })
            return
          }
          if (session.get("cid") != userId) {
            log_mod_after = log_mod_after.concat(r_data)
          }
          log_mod_before[0].customer[0].jpSwitch = jackpot_before //jp開關修改前寫入log
          log_mod_after[0].customer[0].jpSwitch = jackpot_after //jp開關修改後寫入log

          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["ActionLevel"] = session.get("level") || ""
          logData["ActionCid"] = session.get("cid") || ""
          logData["ActionUserName"] = session.get("usrName") || ""

          logDao.add_log_customer(logData, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.LOG_FAIL, data: null })
          return
        }
        next(null, { code: code.OK, data: { username: hallName, stateId } })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][modifyHA] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

/**
 * 修改 Agent 狀態(一般/停用/凍結)
 */
handler.modifyAG = function (msg, session, next) {
  try {
    const userLevel = new UserLevelService({ session })

    const { isAgent, cid, currentUserLevel } = userLevel.getUserLevelData()

    var self = this
    var log_mod_before = []
    var log_mod_after = []
    var logData = {}
    var origModifyLvl = -1
    var origState = "U"

    const userId = msg.data.usr.userId || msg.data.usr.cid

    logData["Level"] = 3
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    logData["Cid"] = userId

    let agentName = ""
    let stateId = msg.data.usr.state

    m_async.waterfall(
      [
        function (cb) {
          userDao.checkUsrExist_byCid(userId, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST, data: null })
            return
          }

          userDao.get_user_byId({ Cid: userId }, cb) //before -user
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_USER_FAIL, data: null })
            return
          }
          logData["UserName"] = r_data[0]["customer"][0]["UserName"]
          log_mod_before = log_mod_before.concat(r_data)

          agentName = logData["UserName"]

          // 原修改者 level
          origModifyLvl = r_data[0]["customer"][0]["ModifyLvl"]
          // 原狀態
          origState = r_data[0]["customer"][0]["State"]

          if (cid === userId && isAgent) {
            //個人明細修改
            cb(null, { code: code.OK }, null)
          } else {
            userDao.get_games_byCid({ Cid: userId }, cb) //before -game
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_GAME_FAIL, data: null })
            return
          }
          if (cid != userId) {
            log_mod_before = log_mod_before.concat(r_data)
          }
          // 修改者 level
          msg.data.usr.modifyLvl = currentUserLevel
          // 狀態改回與原始狀態不同時，修改者必需小於等於原修改者，否則不可修改
          if (typeof stateId != "undefined" && stateId !== origState && currentUserLevel > origModifyLvl) {
            cb(null, { code: code.AUTH.AUTH_INVALID }, null)
          } else {
            // 狀態改回 "一般"，則 ModifyLvl 寫 99
            if (stateId === "N") {
              msg.data.usr.modifyLvl = 99
            }

            /* 登入者ID 與 被修改資料的ID 相同時
                    不能修改: 狀態 / 交收幣別 / 開放幣別 / 錢包類型 / 權限 */
            if (userId == cid) msg.data.usr["edit"] = "self"

            userDao.modifyUser_Agent({ ...msg.data.usr, cid: userId }, cb)
          }
        },
        function (r_code, cb) {
          if (r_code.code === code.AUTH.AUTH_INVALID) {
            next(null, { code: r_code.code, data: null })
            return
          } else if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_MODIFY_FAIL, data: null })
            return
          }

          //凍結USER ->下線變更
          if (stateId != null && stateId != "") {
            //只有編輯帳號狀態才會進來
            var param = {
              upId: userId,
              state: stateId,
            }
            userDao.changeStatusDownUsers(param, cb)
          } else {
            cb(null, { code: code.OK })
          }
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_MODIFY_FAIL, data: null })
          }

          userDao.get_user_byId({ Cid: userId }, cb) //after -user
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_USER_FAIL, data: null })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)

          if (cid != userId) {
            //個人明細修改
            log_mod_after = log_mod_after.concat(r_data)
          }

          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["ActionLevel"] = session.get("level") || ""
          logData["ActionCid"] = session.get("cid") || ""
          logData["ActionUserName"] = session.get("usrName") || ""

          if (cid === userId && isAgent) {
            //個人明細(不變更game)
            logData["FunctionGroupL"] = "Account"
            logData["FunctionAction"] = "EditAccount"
          } else {
            logData["FunctionGroupL"] = "User"
            logData["FunctionAction"] = "EditUser"
          }

          logDao.add_log_customer(logData, cb)

          const beforeState = origState
          const afterState = msg.data.usr.state

          if ((afterState !== "N") & (beforeState === "N")) {
            const payload = { agentId: userId }

            kickAllPlayer(payload, self.app, cb)
            return
          }

          cb(null, { code: code.OK, data: null })
        },
      ],
      function (none, r_code) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.LOG_FAIL, data: { username: agentName, stateId } })
          return
        }
        next(null, { code: code.OK, data: { username: agentName, stateId } })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][modifyAG] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

/**
 * 修改 Player 狀態(一般/停用/凍結)
 */
handler.modifyPR = function (msg, session, next) {
  try {
    var self = this
    var log_mod_before = []
    var log_mod_after = []
    var logData = {}
    var origModifyLvl = -1
    var origState = "U"

    let playerId = msg.data.usr.userId || msg.data.usr.cid
    let playerName = ""
    let originIsKill = ""

    logData["Level"] = 4
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "User"
    logData["FunctionAction"] = "EditUser"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    logData["Cid"] = playerId

    const enpointApiBsAction = conf.API_SERVER_URL + consts.APIServerPlatform.bsAction

    m_async.waterfall(
      [
        function (cb) {
          userDao.checkUsrExist_byCid(playerId, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST, data: null })
            return
          }
          userDao.get_user_byId({ Cid: playerId }, cb) //before -user
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_USER_FAIL, data: null })
            return
          }

          const {
            customer: [targetPlayer],
          } = r_data[0]

          playerName = targetPlayer.UserName
          originIsKill = Number(targetPlayer.isKill)

          logData["UserName"] = r_data[0]["customer"][0]["UserName"]
          // 原修改者 level
          origModifyLvl = r_data[0]["customer"][0]["ModifyLvl"]
          // 原狀態
          origState = r_data[0]["customer"][0]["State"]

          log_mod_before = log_mod_before.concat(r_data)
          // 修改者 level
          msg.data.usr.modifyLvl = session.get("level")

          // 狀態改回與原始狀態不同時，修改者必需小於等於原修改者，否則不可修改
          if (
            typeof msg.data.usr.state != "undefined" &&
            msg.data.usr.state !== origState &&
            session.get("level") > origModifyLvl
          ) {
            cb(null, { code: code.AUTH.AUTH_INVALID }, null)
          } else {
            // 狀態改回 "一般"，則 ModifyLvl 寫 99
            if (msg.data.usr.state === "N") {
              msg.data.usr.modifyLvl = 99
            }

            userDao.modifyUser_Player({ ...msg.data.usr, cid: playerId }, cb)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code === code.AUTH.AUTH_INVALID) {
            next(null, { code: r_code.code, data: { username: playerName } })
            return
          } else if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_MODIFY_FAIL, data: { username: playerName } })
            return
          }

          userDao.get_user_byId({ Cid: logData["Cid"] }, cb) //after -user
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_USER_FAIL, data: { username: playerName } })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["ActionLevel"] = session.get("level") || ""
          logData["ActionCid"] = session.get("cid") || ""
          logData["ActionUserName"] = session.get("usrName") || ""

          logDao.add_log_customer(logData, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_FAIL, data: { username: playerName } })
            return
          }

          const validIsKillParam = [0, 1]
          const playerIsKillParams = Number(msg.data.usr["isKill"])

          const beforeState = origState
          const afterState = msg.data.usr.state

          if ((afterState !== "N") & (beforeState === "N")) {
            const payload = { userId: playerId }

            kickPlayer(payload, self.app, cb)
          } else if (validIsKillParam.includes(playerIsKillParams) && originIsKill !== playerIsKillParams) {
            const payload = {
              playerId: playerId,
              isKill: playerIsKillParams,
            }

            requestService.post(`${enpointApiBsAction}/v1/player/rtp/premade`, payload, { callback: cb })
          } else {
            cb(null, { code: code.OK })
          }
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.LOG_FAIL, data: { username: playerName } })
          return
        }

        next(null, { code: code.OK, data: { username: playerName, stateId: msg.data.usr.state } })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][modifyPR] catch err", inspect(err))
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.modifySubHA = function (msg, session, next) {
  try {
    var self = this
    var log_mod_before = []
    var log_mod_after = []
    var logData = {}

    logData["Level"] = 2
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "User"
    logData["FunctionAction"] = "EditUser"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    logData["Cid"] = msg.data.usr.cid
    //var userSession = {};

    m_async.waterfall(
      [
        function (cb) {
          userDao.checkUsrExist_byCid(msg.data.usr.cid, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST, data: null })
            return
          }

          userDao.get_user_byId({ Cid: logData["Cid"] }, cb) //before -user
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_USER_FAIL, data: null })
            return
          }
          logData["UserName"] = r_data[0]["customer"][0]["UserName"]
          log_mod_before = log_mod_before.concat(r_data)

          var usr_data = {
            cid: msg.data.usr.cid,
            name: msg.data.usr.name,
            password: msg.data.usr.password,
            nickName: msg.data.usr.nickName,
            isSub: 1,
            authorityId: msg.data.usr.authorityId,
            isDemo: 0,
            isSingleWallet: 0,
            state: msg.data.usr.state,
            currencies: msg.data.usr.game_currency,
            ip_whitelist_in: "",
            ip_whitelist_out: "",
            api_outdomain: "",
            halldesc: "test",
            api_hallownername: "",
            securekey: "",
            email: msg.data.usr.email,
          }

          userDao.modifyUser_SubHall_v2(usr_data, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_MODIFY_FAIL, data: null })
            return
          }

          userDao.get_user_byId({ Cid: logData["Cid"] }, cb) //after -user
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_USER_FAIL, data: null })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["ActionLevel"] = session.get("level") || ""
          logData["ActionCid"] = session.get("cid") || ""
          logData["ActionUserName"] = session.get("usrName") || ""

          logDao.add_log_customer(logData, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.USER_MODIFY_FAIL, data: null })
          return
        }
        next(null, { code: code.OK, data: "Success" })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][modifySubHA] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getHAs = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};
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
      sortKey: "",
      sortType: 1,
      upId: -1,
    }

    if (typeof msg.data.page === "undefined" || typeof msg.data.pageCount === "undefined") {
      next(null, { code: code.DB.PARA_FAIL, data: null })
      return
    } else if (typeof msg.data.page != "number" || typeof msg.data.pageCount != "number") {
      next(null, { code: code.DB.PARA_FAIL, data: null })
      return
    } else if (msg.data.page <= 0 || msg.data.pageCount <= 0) {
      next(null, { code: code.DB.PARA_FAIL, data: null })
      return
    }

    if (typeof msg.data.userName != "undefined") {
      data_query.userName = msg.data.userName
    }

    if (typeof msg.data.isDemo != "undefined") {
      data_query.isDemo = msg.data.isDemo
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

    data_query.level = session.get("level")
    data_query.upId = -1
    if (
      (session.get("level") == 1 || session.get("level") == 2) &&
      typeof msg.data.cid != "undefined" &&
      msg.data.cid != ""
    ) {
      data_query.upId = msg.data.cid
    }

    data_query.curPage = msg.data.page
    data_query.pageCount = msg.data.pageCount
    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "addDate"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    let cid = []

    let externalQuotaList = []

    m_async.waterfall(
      [
        function (cb) {
          adminDao.getDCSettingList({}, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
            return
          }

          externalQuotaList = r_data.reduce((acc, cur) => {
            const { DC, IsExternalQuota } = cur

            if (Number(IsExternalQuota) === 1) {
              acc.push(DC)
            }

            return acc
          }, [])

          userDao.getUsrs_Hall(data_query, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
            return
          }

          ttlCount = r_data.count
          curPage = data_query.curPage
          pageCount = data_query.pageCount
          users = r_data["info"]

          for (var i in users) {
            cid.push(users[i]["cid"])
            var game_currency = []
            if (users[i]["currencies"] != "") {
              game_currency = users[i]["currencies"].split(",")
            }
            users[i]["currencies"] = game_currency

            const isExternalQuota = externalQuotaList.includes(users[i]["DC"])
            users[i]["isExternalQuota"] = isExternalQuota
          }

          var login_data = {
            cid: cid,
            level: 2,
          }

          self.app.rpc.log.logRemote.getUserLastLogin(session, login_data, cb) //最近登入時間
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
          return
        }

        //時區轉換
        for (var i in users) {
          users[i]["addDate"] = users[i]["addDate"] == "0000-00-00 00:00:00" ? "" : users[i]["addDate"]
          //最後登入時間
          var login_info = r_data.filter((item) => item.cid == users[i]["cid"])
          var lastLogin = login_info.length > 0 ? login_info[0]["lastLogin"] : "-"
          users[i]["lastLogin"] = lastLogin
          users[i]["level"] = 2
        }
        next(null, {
          code: code.OK,
          data: {
            externalQuotaList,
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
    logger.error("[userHandler][getHAs] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 取得管理者資料
handler.getSubHAs = function (msg, session, next) {
  try {
    var self = this
    var users = []
    var ttlCount = 0
    var curPage = 0
    var pageCount = 0

    var data_query = {
      curPage: 0,
      pageCount: 0,
      upid: 0,
      userName: "",
      isDemo: "",
      states: [],
      start_date: "",
      end_date: "",
      sortKey: "",
      sortType: 0,
    }

    if (
      typeof msg.data.page === "undefined" ||
      typeof msg.data.pageCount === "undefined" ||
      typeof msg.data.upid === "undefined"
    ) {
      next(null, { code: code.DB.PARA_FAIL, data: null })
      return
    } else if (typeof msg.data.page != "number" || typeof msg.data.pageCount != "number") {
      next(null, { code: code.DB.PARA_FAIL, data: null })
      return
    } else if (msg.data.page <= 0 || msg.data.pageCount <= 0) {
      next(null, { code: code.DB.PARA_FAIL, data: null })
      return
    }

    if (typeof msg.data.userName != "undefined") {
      data_query.userName = msg.data.userName
    }

    if (typeof msg.data.isDemo != "undefined") {
      data_query.isDemo = msg.data.isDemo
    }

    if (typeof msg.data.state != "undefined" && typeof msg.data.state === "string") {
      if (msg.data.state != "") data_query.states = msg.data.state.split(",")
    }

    if (typeof msg.data.start_date != "undefined") {
      data_query.start_date = timezone.UTCToLocal(msg.data.start_date)
    }

    if (typeof msg.data.end_date != "undefined") {
      data_query.end_date = timezone.UTCToLocal(msg.data.end_date)
    }

    if (typeof msg.data.sortKey != "undefined") {
      data_query.sortKey = msg.data.sortKey
    }

    if (typeof msg.data.sortType != "undefined") {
      data_query.sortType = msg.data.sortType
    }

    data_query.curPage = msg.data.page
    data_query.pageCount = msg.data.pageCount
    data_query.upid = msg.data.upid
    data_query.isAg = msg.data.isAg
    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "addDate"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    m_async.waterfall(
      [
        function (cb) {
          userDao.getUsrs_SubHall(data_query, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
            return
          }
          ttlCount = r_data.count
          curPage = data_query.curPage
          pageCount = data_query.pageCount
          users = r_data["info"]

          var cid = []
          for (var i in users) {
            cid.push(users[i]["cid"])
          }
          var login_data = {
            cid: cid,
            level: 2,
          }
          self.app.rpc.log.logRemote.getUserLastLogin(session, login_data, cb) //最近登入時間
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
          return
        }

        //時區轉換
        for (var i in users) {
          users[i]["addDate"] =
            users[i]["addDate"] == "0000-00-00 00:00:00" ? "" : timezone.LocalToUTC(users[i]["addDate"])
          //最後登入時間
          var login_info = r_data.filter((item) => item.cid == users[i]["cid"])
          var lastLogin = login_info.length > 0 ? timezone.LocalToUTC(login_info[0]["lastLogin"]) : "-"
          users[i]["lastLogin"] = lastLogin
          users[i]["level"] = users[i]["IsAg"]
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
    logger.error("[userHandler][getSubHAs] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 取得管理者編輯自己的帳戶資料
handler.getUser = function (msg, session, next) {
  try {
    let data = {
      Cid: msg.data.id,
    }
    m_async.waterfall(
      [
        function (cb) {
          userDao.get_operator_byId(data, cb)
        },
      ],
      function (none, r_code, r_user) {
        next(null, {
          code: code.OK,
          data: r_user[0]["customer"][0],
        })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][getUser] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getDetailHA = function (msg, session, next) {
  try {
    var self = this
    var data = {
      cid: msg.data.cid,
      user_level: session.get("level"),
      ggids: [],
      upid: msg.data.upid != 0 ? msg.data.upid : -1,
      isSub: msg.data.isSub,
    }
    var data_usr = null
    var games = []
    var gamesId = [] //記錄遊戲ID
    var rtps = null
    var denoms = null
    //var userSession = {};
    var company = []
    var game_group = []
    var logs = []
    var ttlCount_Revenue = 0

    m_async.waterfall(
      [
        function (cb) {
          userDao.checkUsrExist_byCid(data.cid, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST, data: null })
            return
          }

          if (msg.data.upid != -1 && msg.data.isSub != 1) {
            data["isSub"] = r_data["IsSub"]
          }

          userDao.getUsrDetail_Hall(data, cb)
        },
        function (r_code, r_usr, cb) {
          data_usr = r_usr

          for (var i in data_usr) {
            data_usr[i]["addDate"] = data_usr[i]["addDate"] == "0000-00-00 00:00:00" ? "" : data_usr[i]["addDate"]
            data_usr[i]["level"] = 2
          }

          //user_白名單
          var white_user = {
            Cid: data.cid,
            UserLevel: 2,
            State: [1, 0],
            sortKey: msg.data.sortKey,
            sortType: msg.data.sortType,
          }

          userDao.get_white_list(white_user, cb) //取資料
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_WHITE_LIST_FAIL, data: null })
            return
          }
          var ip_white_list = []

          for (var i in r_data[0]["ip_white_list"]) {
            ip_white_list.push({
              ipId: r_data[0]["ip_white_list"][i]["IpId"],
              desc: r_data[0]["ip_white_list"][i]["Desc"],
              ip: r_data[0]["ip_white_list"][i]["Ip"],
              type: r_data[0]["ip_white_list"][i]["IpType"],
              name: r_data[0]["ip_white_list"][i]["Name"],
              state: r_data[0]["ip_white_list"][i]["State"],
            })
          }

          for (var i in data_usr) {
            data_usr[i]["ip_whitelist"] = ip_white_list

            var game_currency = []
            if (data_usr[i]["game_currency"] != "" && data_usr[i]["game_currency"] != undefined) {
              game_currency = data_usr[i]["game_currency"].split(",")
            }
            data_usr[i]["game_currency"] = game_currency
          }

          //user_信用資料
          var credits_user = {
            Cid: data.cid,
            sortKey: msg.data.sortKey,
            sortType: msg.data.sortType,
          }

          userDao.get_credits_list(credits_user, cb) // 取信用資料
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.LOG_CREDITS_LIST_FAIL, data: re_before })
          return
        }
        var credits_list = []

        for (var i in r_data[0]["credits_list"]) {
          let alertValue_arr = []
          let notified_arr = []
          for (var k = 0; k < r_data[0]["credits_list"][i]["AlertValue"].split(",").length; k++) {
            if (parseInt(r_data[0]["credits_list"][i]["AlertValue"].split(",")[k]) == 0) {
              continue
            }
            alertValue_arr.push(parseInt(r_data[0]["credits_list"][i]["AlertValue"].split(",")[k]))
          }

          if (r_data[0]["credits_list"][i]["Notified"] != null) {
            for (var m = 0; m < r_data[0]["credits_list"][i]["Notified"].split(",").length; m++) {
              notified_arr.push(parseInt(r_data[0]["credits_list"][i]["Notified"].split(",")[m]))
            }
          }

          credits_list.push({
            currency: r_data[0]["credits_list"][i]["Currency"],
            creditQuota: r_data[0]["credits_list"][i]["CreditQuota"],
            currentQuota: r_data[0]["credits_list"][i]["CurrentQuota"],
            alertValue: alertValue_arr,
            notified: notified_arr,
          })
        }

        for (var i in data_usr) {
          data_usr[i]["credits_list"] = credits_list
        }

        let toData = {
          usr: data_usr,
          games: logs,
        }

        next(null, {
          code: code.OK,
          data: toData,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][getDetailHA] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.joinAG = function (msg, session, next) {
  try {
    var self = this
    var newUser = {}
    //var userSession = {};
    var sys_config = self.app.get("sys_config")

    var otpData = {}
    var logData = {}
    var log_mod_after = []
    var user_set_id = 0
    // var games = [];
    logData["Level"] = 3
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "add"
    logData["FunctionGroupL"] = "User"
    logData["FunctionAction"] = "AddUser"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""

    var hallid = msg.data.usr.hallid
    m_async.waterfall(
      [
        function (cb) {
          userDao.checkUsrExist(msg.data.usr, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_DUPLICATE, data: null })
            return
          }
          var search_log = { Cid: msg.data.usr.hallid }
          userDao.get_user_byId(search_log, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_USER_FAIL, data: null })
            return
          }
          msg.data.usr["cid"] = uid.randomUUID(12) //產生序號
          msg.data.usr["dc"] = r_data[0]["customer"][0]["DC"] // domain
          msg.data.usr["currency"] = r_data[0]["customer"][0]["Currency"] // 取 Hall 的交收幣別
          // msg.data.usr['isSingleWallet'] = r_data[0]["customer"][0]["IsSingleWallet"];

          userDao.createUser_Agent(msg.data.usr, cb)
        },
        function (r_code, r_cid, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_CREATE_FAIL, data: null })
            return
          }

          newUser = {
            user: {
              cid: r_cid,
              upid: typeof msg.data.usr.upid != "undefined" ? msg.data.usr.upid : -1,
              hallid: typeof msg.data.usr.hallid != "undefined" ? msg.data.usr.hallid : -1,
            },
            games: msg.data.games,
          }

          logData["Cid"] = r_cid
          logData["UserName"] = msg.data.usr.name

          var secret = m_otplib.authenticator.generateSecret() //secret key

          otpData = {
            Cid: r_cid,
            IsAdmin: 0,
            HallId: newUser["user"]["hallid"],
            OTPCode: secret,
          }
          userDao.createUser_newOtpCode(otpData, cb) //建立OTP code
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.OTP_CREATE_FAIL, data: null })
            return
          }

          //一筆-user_setting預設值
          var set_param = {
            Cid: logData["Cid"],
            IsAdmin: 0,
            CountsOfPerPage: sys_config.counts_per_page,
            HourDiff: sys_config.time_diff_hour,
          }
          userDao.set_user_setting(set_param, cb)
        },
        function (r_code, r_id, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.SETTING_CREATE_FAIL, data: null })
            return
          }
          user_set_id = r_id

          //  hallid = msg.data.usr.hallid;
          //     self.app.rpc.user.userRemote.get_game_setting_byUserId(userSession, hallid, cb);  //取hall 遊戲資訊
          // }, function (r_code, r_data, cb) {

          //      //取hall 遊戲currency_denom資訊

          //     self.app.rpc.game.gameRemote.joinGameToAgent_v2(userSession, newUser, cb);
          // }, function (r_code, cb) {
          //     if (r_code.code != code.OK) {
          //         next(null, { code: code.USR.GAME_JOIN_FAIL, data: null });
          //         return;
          //     }

          var search_log = { Cid: logData["Cid"] }

          userDao.get_user_byId(search_log, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_USER_FAIL, data: null })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)
          userDao.get_OtpCode_byId(otpData, cb) //otp_code
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_OTP_FAIL, data: null })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)

          //     userDao.get_games_byCid({ Cid: logData['Cid'] }, cb);  //game

          // }, function (r_code, r_data, cb) {
          //     if (r_code.code != code.OK) {
          //         next(null, { code: code.USR.LOG_GAME_FAIL, data: null });
          //         return;
          //     }

          userDao.get_user_setting({ Uid: user_set_id }, cb) //user_setting初始
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_SETTING_FAIL, data: null })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["ActionLevel"] = session.get("level") || ""
          logData["ActionCid"] = session.get("cid") || ""
          logData["ActionUserName"] = session.get("usrName") || ""

          logDao.add_log_customer(logData, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.LOG_FAIL, data: null })
          return
        }

        next(null, { code: code.OK, data: "Success" })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][joinAG] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getAGs = function (msg, session, next) {
  try {
    var self = this

    var users = []
    var ttlCount = 0
    var curPage = 0
    var pageCount = 0

    var data_query = {
      curPage: 0,
      pageCount: 0,
      cid: 0,
      isAg: 3,
      userName: "",
      isDemo: "",
      states: [],
      start_date: "",
      end_date: "",
      sortKey: "",
      sortType: 1,
    }

    if (typeof msg.data.cid === "undefined" || msg.data.cid === "") {
      next(null, { code: code.DB.PARA_FAIL, data: null })
      return
    }

    if (typeof msg.data.page === "undefined" || typeof msg.data.pageCount === "undefined") {
      next(null, { code: code.DB.PARA_FAIL, data: null })
      return
    } else if (typeof msg.data.page != "number" || typeof msg.data.pageCount != "number") {
      next(null, { code: code.DB.PARA_FAIL, data: null })
      return
    } else if (msg.data.page <= 0 || msg.data.pageCount <= 0) {
      next(null, { code: code.DB.PARA_FAIL, data: null })
      return
    }

    if (typeof msg.data.userName != "undefined") {
      data_query.userName = msg.data.userName
    }

    if (typeof msg.data.isDemo != "undefined") {
      data_query.isDemo = msg.data.isDemo
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

    data_query.curPage = msg.data.page
    data_query.pageCount = msg.data.pageCount
    data_query.cid = msg.data.cid

    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "addDate"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    let cid = []
    let lastLoginInfo = []

    let externalQuotaList = []

    m_async.waterfall(
      [
        function (cb) {
          adminDao.getDCSettingList({}, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
            return
          }

          externalQuotaList = r_data.reduce((acc, cur) => {
            const { DC, IsExternalQuota } = cur

            if (Number(IsExternalQuota) === 1) {
              acc.push(DC)
            }

            return acc
          }, [])

          userDao.getUsrs_Agent(data_query, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
            return
          }

          ttlCount = r_data.count
          curPage = data_query.curPage
          pageCount = data_query.pageCount

          users = r_data["info"]

          for (var i in users) {
            cid.push(users[i]["cid"])
            var game_currency = []
            if (users[i]["currency"] != "") {
              game_currency = users[i]["currency"].split(",")
            }
            users[i]["currencies"] = game_currency

            const isExternalQuota = externalQuotaList.includes(users[i]["DC"])
            users[i]["isExternalQuota"] = isExternalQuota
          }
          var login_data = {
            cid: cid,
            level: 3,
          }

          self.app.rpc.log.logRemote.getUserLastLogin(session, login_data, cb) //最近登入時間
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
          return
        }
        for (var i in users) {
          users[i]["addDate"] = users[i]["addDate"] == "0000-00-00 00:00:00" ? "" : users[i]["addDate"]
          //最後登入時間
          var login_info = r_data.filter((item) => item.cid == users[i]["cid"])
          var lastLogin = login_info.length > 0 ? login_info[0]["lastLogin"] : "-"
          users[i]["lastLogin"] = lastLogin
          //agent下線數=玩家數
          if (users[i]["isDemo"] == 1) {
            //測試帳號
            users[i]["players"] = users[i]["downUsers"] + "/" + conf.DEMO_PLAYER_NUMS
          } else {
            users[i]["players"] = users[i]["downUsers"]
          }
          users[i]["level"] = 3
        }

        next(null, {
          code: code.OK,
          data: {
            externalQuotaList,
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
    logger.error("[userHandler][getAGs] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getDetailAG = function (msg, session, next) {
  try {
    var self = this
    var data = {
      cid: msg.data.cid,
      upid: msg.data.upid,
      ggids: [],
      isSub: msg.data.isSub,
      isNextLevel: msg.data.isNextLevel,
    }
    var data_usr = null
    var games = []
    var gamesId = [] //記錄遊戲ID
    var game_group = []
    var rtps = null
    var denoms = null
    //var userSession = {};
    var logs = []
    var ttlCount_Revenue = 0

    m_async.waterfall(
      [
        function (cb) {
          userDao.getUsrDetail_Agent(data, cb)
        },
        function (r_code, r_usr, cb) {
          data_usr = r_usr

          for (var i in data_usr) {
            data_usr[i]["addDate"] = data_usr[i]["addDate"] == "0000-00-00 00:00:00" ? "" : data_usr[i]["addDate"]
            if (data_usr[i]["isDemo"] == 1 && parseInt(data_usr[i]["downUsers"]) >= conf.DEMO_PLAYER_NUMS) {
              data_usr[i]["addPlayer"] = false
            } else {
              data_usr[i]["addPlayer"] = true
            }
            data_usr[i]["level"] = 3
          }

          self.app.rpc.config.configRemote.getGameGroup(session, cb)
        },
        function (r_code, r_groups, cb) {
          game_group = r_groups
          for (var count = 0; count < r_groups.length; count++) {
            data.ggids.push(r_groups[count].GGId)
          }

          data.isPage = true
          data.page = msg.data.page
          data.pageCount = msg.data.pageCount
          data.sel_table = "game_revenue_agent"
          data.user_level = 2

          userDao.getGameRevenue_games_v2Cid(data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.REVENUE_LOAD_FAIL,
              data: null,
            })
            return
          }

          ttlCount_Revenue = r_data.count
          games = r_data["info"]

          games.forEach((item) => {
            if (gamesId.indexOf(item.GameId) == -1) gamesId.push(item.GameId)
          })

          if (msg.data.isPage && ttlCount_Revenue == 0) {
            cb(
              null,
              {
                code: code.OK,
              },
              0
            )
          } else {
            var users = {
              level: data["user_level"],
              hallId: data["user_level"] == 1 ? "-1" : session.get("isSub") == 1 ? session.get("hallId") : data.upid,
              agentId: data["user_level"] == 3 ? session.get("cid") : "-1",
              start_date: "",
              end_date: "",
              gamesId: gamesId,
            }
            bettingDao.getUniquePlayerNums(users, cb) //玩家人數
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.BET.REVENUE_PLAYERS_LOAD_FAIL,
              data: null,
            })
            return
          }

          var gamesId = []
          for (i in games) {
            var players = r_data.filter((item) => item.GameId == games[i]["GameId"])
            var group = game_group.filter((item) => parseInt(item.GGId) == parseInt(games[i]["GGId"]))
            if (gamesId.indexOf(games[i]["GameId"].toString()) == -1) gamesId.push(games[i]["GameId"].toString())

            var tmp = {
              gameId: games[i]["GameId"],
              rounds: games[i]["rounds"], //場次
              uniPlayers: players.length > 0 ? players[0]["num"] : 0,
              level: "GAME",
              currency: games[i]["Currency"],
              betGold: utils.number.floor(
                utils.number.oneThousand(Number(games[i]["BetGold"].replace(/\,/g, "")), consts.Math.DIVIDE)
              ),
              winGold: utils.number.floor(
                utils.number.oneThousand(Number(games[i]["WinGold"].replace(/\,/g, "")), consts.Math.DIVIDE)
              ),
              jpPoint: utils.number.floor(
                utils.number.oneThousand(Number(games[i]["JPPoint"].replace(/\,/g, "")), consts.Math.DIVIDE)
              ),
              jpGold: utils.number.floor(
                utils.number.oneThousand(Number(games[i]["JPGold"].replace(/\,/g, "")), consts.Math.DIVIDE)
              ),
              avgBet: utils.number.floor(
                utils.number.oneThousand(Number(games[i]["avgBet"].replace(/\,/g, "")), consts.Math.DIVIDE)
              ),
              netWin: utils.number.floor(
                utils.number.oneThousand(Number(games[i]["NetWin"].replace(/\,/g, "")), consts.Math.DIVIDE)
              ),
              RTP: games[i]["RTP"],
              accountDate: typeof games[i]["AccountDate"] === "undefined" ? "" : games[i]["AccountDate"],
              ggId: games[i]["GGId"],
              groupNameE: group.length > 0 ? group[0]["NameE"] : "",
              groupNameG: group.length > 0 ? group[0]["NameG"] : "",
              groupNameC: group.length > 0 ? group[0]["NameC"] : "",
            }
            logs.push(tmp)
          }

          gameDao.getGameName_byId(
            {
              gameId: gamesId,
            },
            cb
          ) //取遊戲名稱清單
        },
        /*function (r_code, r_groups, cb) {

                for (var count = 0; count < r_groups.length; count++) {
                    data.ggids.push(r_groups[count].GGId);
                }

                //直接取hall的遊戲
                var param = {
                    cid: data_usr[0]['hallId'],
                    ggids: data.ggids
                }

                self.app.rpc.game.gameRemote.getGames_byGroup_hall(session, param, cb);
            }, function (r_code, r_games, cb) {

                games = r_games;

                var gameId = [];
                games.forEach(item => {
                    gameId.push(item.gameId);
                });

                var param = {
                    gameId: gameId,
                    cid: data_usr[0]['hallId'],
                    currency: data_usr[0]['currency']
                }

                console.log('getGames_Denom_byCid - param', JSON.stringify(param));

                self.app.rpc.game.gameRemote.getGames_Denom_byCid(session, param, cb);  //各遊戲設定的幣別-denom
            }, function (r_code, r_data, cb) {

                var denom_set = {};
                r_data.forEach(item => {
                    var denom = { currency: item.Currency, value: item.Denom };
                    if (typeof denom_set[item.GameId] == 'undefined') {
                        denom_set[item.GameId] = Object.assign([]);
                    }
                    denom_set[item.GameId].push(denom);
                });

                for (var i = 0; i < games.length; i++) {
                    var gameId = games[i]['gameId'];
                    games[i]['denoms_set'] = Object.assign([], denom_set[gameId]);
                    games[i]['denoms'] = [];//原本預選資料;
                }

                configDao.getRTPs_2( cb);
            }, function (r_code, r_rtps, cb) {
                rtps = r_rtps;

                configDao.getDenoms_2( cb);
            }, function (r_code, r_denoms, cb) {
                denoms = r_denoms;

                configDao.getCompany_2( cb);
            }, function (r_code, r_company, cb) {

                var company = r_company;

                var game_count = 0;
                var game_length = games.length;
                var info = [];
                for (game_count = 0; game_count < game_length; game_count++) {
                    var game_data = games[game_count];
                    // var tmp_denoms = game_data.denoms.split(",");  
                    var count = 0, count_1 = 0;
                    var update_denoms = [];
                    var update_rtps = []; 

                    // for (count_1 = 0; count_1 < rtps.length; count_1++) {

                    //     if (Number(tmp_rtps) === Number(rtps[count_1].Id)) {
                    //         update_rtps.push(rtps[count_1]);
                    //         break;
                    //     }
                    // }
        
                    //找rtp_set (hall遊戲中設定的RTP)
                    for (var count_1 = 0; count_1 < rtps.length; count_1++) {
                        if (Number(game_data.rtp_set) === Number(rtps[count_1].Id) ) {
                            update_rtps.push(rtps[count_1]);
                            break;
                        }
                    }

                    /!*
                    for (count = 0; count < 1; count++) {
                        for (var count_1 = 0; count_1 < rtps.length; count_1++) {
                            if (Number(tmp_rtps[count]) === Number(rtps[count_1].Id) && +game_data.rtp_set === +rtps[count_1].Id) {
                                update_rtps.push(rtps[count_1]);
                                break;
                            }
                        }
                    }
                    *!/

                    //   games[game_count].denoms = update_denoms;
                    games[game_count].rtps = update_rtps;
                    var company_data = company.filter(item => parseInt(item.Id) == parseInt(game_data.company));
                    games[game_count]['comapnyName'] = (company_data.length > 0) ? company_data[0]['Value'] : '';
                    if (game_data['sw'] == 1) info.push(games[game_count]);
                }

                cb(null, { code: code.OK, msg: "" }, info);
            }*/
      ],
      function (none, r_code, r_games) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.GAME_LOAD_FAIL, data: null })
          return
        }

        for (i in logs) {
          let gameInfo = r_games.filter((item) => item.GameId == logs[i]["gameId"])
          logs[i]["nameE"] = gameInfo.length > 0 ? gameInfo[0]["NameE"] : ""
          logs[i]["nameG"] = gameInfo.length > 0 ? gameInfo[0]["NameG"] : ""
          logs[i]["nameC"] = gameInfo.length > 0 ? gameInfo[0]["NameC"] : ""
        }

        for (var i in data_usr) {
          var game_currency = []
          if (data_usr[i]["game_currency"] != "" && data_usr[i]["game_currency"] != null) {
            game_currency = data_usr[i]["game_currency"].split(",")
          }
          data_usr[i]["game_currency"] = game_currency
        }
        next(null, {
          code: code.OK,
          data: {
            usr: data_usr,
            games: logs,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][getDetailAG] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.joinPR = function (msg, session, next) {
  try {
    var self = this
    var cid = -1
    var objectID_Hall = null
    var objectID_Agent = null
    //var userSession = {};

    var logData = {}
    var log_mod_after = []

    logData["Level"] = 4
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "add"
    logData["FunctionGroupL"] = "User"
    logData["FunctionAction"] = "AddUser"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""

    m_async.waterfall(
      [
        function (cb) {
          userDao.checkUsrExist(msg.data.usr, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_DUPLICATE, data: null })
            return
          }
          var search_log = { Cid: msg.data.usr.upid }
          userDao.get_user_byId(search_log, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_USER_FAIL, data: null })
            return
          }
          msg.data.usr["cid"] = short.generate()
          msg.data.usr["dc"] = r_data[0]["customer"][0]["DC"] // domain
          msg.data.usr["isSingleWallet"] = r_data[0]["customer"][0]["IsSingleWallet"]

          userDao.createUser_Player(msg.data.usr, cb)
        },
        function (r_code, r_cid, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_CREATE_FAIL, data: null })
            return
          }

          logData["Cid"] = r_cid
          logData["UserName"] = msg.data.usr.name

          var search_log = { Cid: logData["Cid"] }
          userDao.get_user_byId(search_log, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_USER_FAIL, data: null })
            return
          }

          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["ActionLevel"] = session.get("level") || ""
          logData["ActionCid"] = session.get("cid") || ""
          logData["ActionUserName"] = session.get("usrName") || ""

          logDao.add_log_customer(logData, cb)
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.LOG_FAIL, data: null })
          return
        }
        next(null, { code: code.OK, data: "Success" })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][joinPR] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getPRs = function (msg, session, next) {
  try {
    var self = this

    var ttlCount = 0
    var curPage = 0
    var pageCount = 0
    var data_query = {
      curPage: 0,
      pageCount: 0,
      cid: 0,
      isAg: 4,
      userName: "",
      isDemo: "",
      states: [],
      start_date: "",
      end_date: "",
      sortKey: "",
      sortType: 1,
    }

    let userIdList = []
    let userDataList = []
    let loginDateList = []

    if (typeof msg.data.cid === "undefined" || msg.data.cid === "") {
      next(null, { code: code.DB.PARA_FAIL, data: null })
      return
    }
    if (typeof msg.data.page === "undefined" || typeof msg.data.pageCount === "undefined") {
      next(null, { code: code.DB.PARA_FAIL, data: null })
      return
    } else if (typeof msg.data.page != "number" || typeof msg.data.pageCount != "number") {
      next(null, { code: code.DB.PARA_FAIL, data: null })
      return
    } else if (msg.data.page <= 0 || msg.data.pageCount <= 0) {
      next(null, { code: code.DB.PARA_FAIL, data: null })
      return
    }

    if (typeof msg.data.userName != "undefined") {
      data_query.userName = msg.data.userName
    }

    if (typeof msg.data.isDemo != "undefined") {
      data_query.isDemo = msg.data.isDemo
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
    data_query.curPage = msg.data.page
    data_query.pageCount = msg.data.pageCount
    data_query.cid = msg.data.cid
    //排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "addDate"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    let externalQuotaList = []

    const isAdmin = session.get("level") === 1

    const isUnlimitTimeRange = !msg.data.start_date || !msg.data.start_date

    m_async.waterfall(
      [
        function (cb) {
          adminDao.getDCSettingList({}, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
            return
          }

          externalQuotaList = r_data.reduce((acc, cur) => {
            const { DC, IsExternalQuota } = cur

            if (Number(IsExternalQuota) === 1) {
              acc.push(DC)
            }

            return acc
          }, [])

          userDao.getUsrs_Player(data_query, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
            return
          }

          ttlCount = r_data.count

          userIdList = r_data.info.map((x) => x.cid)
          userDataList = [...r_data.info]

          const params = {
            cid: userIdList,
            level: 4,
          }

          // 最近登入時間
          self.app.rpc.log.logRemote.getUserLastLogin(session, params, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code !== code.OK) {
            next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
            return
          }

          loginDateList = [...r_data]

          cb(null, { code: code.OK }, null)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
            return
          }

          const userList = userDataList.map((x) => {
            const [loginInfo] = loginDateList.filter((d) => d.cid === x.cid)

            const lastLoginDate = loginInfo ? loginInfo.lastLogin : "-"

            const currencies = x.currency.split(",")
            const isExternalQuota = externalQuotaList.includes(x.DC)

            const totalRealBetGold = BN(x.totalRealBetGold).div(1000).dp(2).toNumber()
            const totalWinGold = BN(x.totalWinGold).div(1000).dp(2).toNumber()
            const totalJPGold = BN(x.totalJPGold).div(1000).dp(2).toNumber()
            const totalNetWin = BN(totalRealBetGold).minus(totalWinGold).dp(2).toNumber()

            return {
              ...x,
              totalRealBetGold,
              totalWinGold,
              totalJPGold,
              totalNetWin,
              lastLogin: lastLoginDate,
              isExternalQuota,
              currencies,
              level: 4,
            }
          })

          cb(null, { code: code.OK }, userList)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
          return
        }

        next(null, {
          code: code.OK,
          data: {
            externalQuotaList,
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: curPage,
            page_count: pageCount,
            usrs: r_data,
            sortKey: sortKey,
            sortType: sortType,
          },
        })

        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][getPRs] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

/**
 * 取得【玩家明細】
 *
 * @param {*} msg
 * @param {*} session
 * @param {*} next
 */
handler.getDetailPR = function (msg, session, next) {
  const logTag = "[userDao][getUsrDetail_Player]"
  try {
    let data = {
      cid: msg.data.cid,
      upid: msg.data.upid,
      ggids: [],
    }
    let data_usr = null
    const logs = []

    m_async.waterfall(
      [
        function (cb) {
          userDao.getUsrDetail_Player(data, cb)
        },
      ],
      function (none, r_code, r_usr) {
        if (r_code.code != code.OK) {
          logger.error(`${logTag}  ${inspect(r_code)}`)

          next(null, {
            code: code.FAIL,
            data: {
              usr: null,
              games: null,
            },
          })
          return
        }

        data_usr = r_usr

        //時區轉換
        for (let i in data_usr) {
          data_usr[i]["addDate"] = data_usr[i]["addDate"] == "0000-00-00 00:00:00" ? "" : data_usr[i]["addDate"]
          data_usr[i]["level"] = 4
        }
        next(null, {
          code: code.OK,
          data: {
            usr: data_usr,
            games: logs,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][getDetailPR] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getDefault_joinAgent = function (msg, session, next) {
  try {
    var self = this
    var data = {
      cid: msg.data.cid,
      authType: 3,
    }
    //var userSession = {};

    var groups = null

    var hall_authority = []
    var auth_tmp = []
    var isDemoNum = 0
    var game_currency = []
    var wallet_type = []
    m_async.waterfall(
      [
        function (cb) {
          userDao.getIsDemoUsers(data, cb) //找user底下有無測試帳號
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK || r_data.length == 0) {
            next(null, r_code)
            return
          }
          if (r_data.length > 0) {
            isDemoNum = r_data[0]["count"]
          }

          self.app.rpc.game.gameRemote.getGameCounts_AG_byGroup(session, data, cb)
        },
        function (r_code, r_groups, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.GAME_LOAD_FAIL, data: null })
            return
          }
          groups = r_groups
          var param = {
            cid: data.cid,
          }
          if (msg.data.isSub == 0) {
            userDao.getGame_currency(param, cb)
          } else {
            gameDao.getWallet(msg.data, cb)
          }

          // userDao.getGame_currency(param, cb);
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK || r_data.length == 0) {
            next(null, { code: code.USR.CURRENCY_LOAD_FAIL, data: null })
            return
          }
          if (r_data[0]["Currencies"] != "" && r_data[0]["Currencies"] != undefined) {
            game_currency = r_data[0]["Currencies"].split(",")
          } else {
            r_data.forEach((item) => {
              if (item.Currency != null && item.Currency != "") game_currency.push(item.Currency)
            })
          }

          userDao.findUpId(msg.data.cid, cb)
        },

        function (r_code, r_data, cb) {
          // if (r_code.code != code.OK || r_data.length == 0) {
          //     next(null, { code: code.USR.CURRENCY_LOAD_FAIL, data: null });
          //     return;
          // }
          // if (r_data[0]['Currencies'] != '' && r_data[0]['Currencies'] != undefined) {
          //     game_currency = r_data[0]['Currencies'].split(",")
          // } else {
          //     r_data.forEach(item => {
          //         if (item.Currency != null && item.Currency != '') game_currency.push(item.Currency);
          //     });
          // }

          r_data[0]["IsSingleWallet"].split(",").forEach((item) => {
            wallet_type.push({
              id: parseInt(item),
              value: item == 0 ? "MultiWallet" : "SingleWallet",
            })
          })

          userDao.getUserAuthorityTemp(data, cb) //可以選擇的tmp
        },
        function (r_code, r_auths, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.AUTH.AUTH_TMP_LOAD_FAIL, data: null })
            return
          }
          auth_tmp = r_auths
          userDao.getUser_authFuncs(data, cb) //hall's tmp
        },
        function (r_code, r_authFuncs, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.AUTH.AUTH_TMP_LOAD_FAIL, data: null })
            return
          }
          hall_authority = r_authFuncs.split(",")

          var param = {
            user_level: "Agent",
            up_authority: hall_authority,
          }
          self.app.rpc.config.configRemote.getDefault_joinAuthorityFuncs_v3(session, param, cb) //預設的權限(all)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.AUTH_TMP_LOAD_FAIL, data: null })
          return
        }
        var auths = {}
        var agent_auths = r_data["agent"]

        next(null, {
          code: code.OK,
          data: {
            authorityTemps: auth_tmp,
            currencies: game_currency,
            game_groups: groups,
            user_authority: agent_auths, //hall本身的權限 與agent 可加入的權限
            isDemo: isDemoNum > 0 ? true : false,
            wallet_type: wallet_type,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][getDefault_joinAgent] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//(管端)user修改自己的密碼
handler.modifyPassword = function (msg, session, next) {
  try {
    var self = this
    var logData = {}

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "Account"
    logData["FunctionAction"] = "EditPassword"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Cid"] = msg.data.usr.cid
    logData["UserName"] = session.get("usrName")

    var log_mod_before = []
    var log_mod_after = []

    m_async.waterfall(
      [
        function (cb) {
          userDao.checkUsrExist_byCid(msg.data.usr.cid, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST, data: null })
            return
          }
          userDao.getPassword_Cid(msg.data.usr.cid, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_PASSWORD_FAIL, data: null })
            return
          }

          if (r_data[0]["customer"][0]["Passwd"] != msg.data.usr["password_old"]) {
            next(null, { code: code.USR.USER_OLD_PASSWORD_FAIL, data: null })
            return
          }

          log_mod_before = log_mod_before.concat(r_data)
          userDao.modifyPassword(msg.data.usr, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_MODIFY_FAIL, data: null })
            return
          }

          userDao.getPassword_Cid(msg.data.usr.cid, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_PASSWORD_FAIL, data: null })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["ActionLevel"] = session.get("level") || ""
          logData["ActionCid"] = session.get("cid") || ""
          logData["ActionUserName"] = session.get("usrName") || ""

          logDao.add_log_customer(logData, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.LOG_FAIL, data: null })
          return
        }
        next(null, { code: code.OK })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][modifyPassword] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 上線或admin變更用戶的密碼
handler.modifyUserPassword = function (msg, session, next) {
  try {
    var self = this
    var logData = {}

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Cid"] = msg.data.cid
    logData["UserName"] = session.get("usrName")

    var log_mod_before = []
    var log_mod_after = []
    var user = {}
    m_async.waterfall(
      [
        function (cb) {
          userDao.getPassword_Cid(msg.data.cid, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_PASSWORD_FAIL, data: null })
            return
          }
          user = r_data[0]["customer"][0]
          log_mod_before = log_mod_before.concat(r_data)
          userDao.modifyUserPassword(msg.data, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_MODIFY_FAIL, data: null })
            return
          }

          userDao.getPassword_Cid(msg.data.cid, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_PASSWORD_FAIL, data: null })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["ActionLevel"] = session.get("level") || ""
          logData["ActionCid"] = session.get("cid") || ""
          logData["ActionUserName"] = session.get("usrName") || ""

          var FunctionGroupL = ""
          var FunctionAction = ""
          if (user["IsSub"] == 1) {
            //OP
            if (session.get("level") == 1) {
              //admin
              FunctionGroupL = "System"
              FunctionAction = "EditSubUserInMainUser"
            } else {
              //hall
              FunctionGroupL = "User"
              FunctionAction = "EditSubUserInUser"
            }
          } else {
            //other
            FunctionGroupL = "User"
            FunctionAction = "EditUser"
          }
          logData["FunctionGroupL"] = FunctionGroupL
          logData["FunctionAction"] = FunctionAction

          logDao.add_log_customer(logData, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.LOG_FAIL, data: null })
          return
        }
        next(null, { code: code.OK })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][modifyUserPassword] catch err", err)
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
      next(null, { code: code.FAIL, data: null })
      return
    }
    var tempPassword = ""
    var tempEndTime = ""
    var sys_config = self.app.get("sys_config")

    m_async.waterfall(
      [
        function (cb) {
          msg.data["isTransferLine"] = false

          userDao.checkIsExist_User(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST, data: null })
            return
          }

          tempPassword = shared.makeid(6)
          var tempEndTimeStamp = new Date().getTime() + parseInt(sys_config.temp_password_time_sec) * 1000
          // tempEndTime = timezone.setTime(new Date(tempEndTimeStamp));
          tempEndTime = timezone.formatTime(tempEndTimeStamp)

          var pwdInfo = {
            Cid: r_data[0]["Cid"],
            TempPassword: m_md5(tempPassword),
            TempEndTime: tempEndTime,
            TempMod: 1,
          }

          userDao.setTempPassword(pwdInfo, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.CREATE_TEMP_FAIL, data: null })
            return
          }

          var subject = ""
          var content = ""
          switch (msg.data.lang) {
            case "tw":
              subject = "重設密碼"
              content = mailUtil.tw(tempEndTime, msg.data.userName, tempPassword, conf.RESET_USER_PWD_URL)
              break
            case "cn":
              subject = "重设密码"
              content = mailUtil.cn(tempEndTime, msg.data.userName, tempPassword, conf.RESET_USER_PWD_URL)
              break
            case "en":
              subject = "Rest Password"
              content = mailUtil.en(tempEndTime, msg.data.userName, tempPassword, conf.RESET_USER_PWD_URL)
              break
          }
          // var content = "<div style=\"margin:0 auto; width:600px\">" +
          //     " <p>DEAR ~ </p>" +
          //     " <p> Please complete the password change process before " + tempEndTime + ". </p>" +
          //     " <p>account name: " + msg.data.userName + "</p>" +
          //     " <p>password: " + tempPassword + "</p>" +
          //     " <p><a href='" + conf.RESET_USER_PWD_URL + "'>web manage</a></p>" +
          //     "</div > ";

          mailInfo = {
            to: [msg.data.mail],
            subject: subject,
            content: content,
          }

          mail.sendMail(mailInfo, cb) //發信
        },
      ],
      function (none, r_code) {
        if (r_code.code != code.OK) {
          next(null, { code: r_code.code, data: null })
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
    logger.error("[userHandler][getTempPassword] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//新增黑白名單
handler.CreateIPWhiteList = function (msg, session, next) {
  try {
    //console.log('-CreateIPWhiteList msg-', JSON.stringify(msg));
    var self = this
    var re_before = []
    var re_after = []
    var logData = {}
    var user_level = 2 // hall
    var funcAction = "AddWhiteList"

    var log_mod_before = []
    var log_mod_after = []
    //var userSession = {};

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.HallId === "undefined" ||
      typeof msg.data.IP === "undefined"
    ) {
      next(null, { code: code.DB.PARA_FAIL })
      return
    }

    if (typeof msg.data.IP["ip"] != "undefined" && msg.data.IP["ip"] != "") {
      if (msg.data.IP["chkWhiteBlackFlag"]) {
        user_level = 1
        funcAction = "AddBlackList"
      }
    }

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "add"
    logData["FunctionGroupL"] = "User"
    logData["FunctionAction"] = funcAction
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""

    const enpointApiBsAction = conf.API_SERVER_URL + consts.APIServerPlatform.bsAction
    const hallId = msg.data.HallId
    let dc = ""

    m_async.waterfall(
      [
        function (cb) {
          userDao.getUser_byId({ userId: hallId, level: 2 }, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_WHITE_LIST_FAIL, data: null })
            return
          }

          dc = r_data[0].DC

          //BEFORE-user_白名單
          var white_user = {
            Cid: msg.data.HallId,
            UserLevel: user_level,
            State: [1, 0],
            chkFlag: msg.data.IP["chkWhiteBlackFlag"] || "", // 判斷查詢白名單(ip_white_list)或黑名單(ip_black_list)
          }
          userDao.get_white_list(white_user, cb) //取資料
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_WHITE_LIST_FAIL, data: null })
            return
          }

          var ip_white_list = r_data[0]["ip_white_list"]

          var log_before_info = []
          //return 資料
          for (var i in ip_white_list) {
            re_before.push({
              ipId: ip_white_list[i]["IpId"],
              desc: ip_white_list[i]["Desc"],
              ip: ip_white_list[i]["Ip"],
              type: ip_white_list[i]["IpType"],
              name: ip_white_list[i]["Name"],
              state: ip_white_list[i]["State"],
              ggid: msg.data.IP["chkWhiteBlackFlag"] ? ip_white_list[i]["GGID"] : 0,
            })
            log_before_info.push(ip_white_list[i])
          }

          if (msg.data.IP["chkWhiteBlackFlag"]) {
            log_mod_before = log_mod_before.concat({ ip_black_list: log_before_info })
          } else {
            log_mod_before = log_mod_before.concat({ ip_white_list: log_before_info })
          }

          //建立 白名單
          var ip_white_list = []

          if (
            typeof msg.data.IP["ip"] != "undefined" &&
            msg.data.IP["ip"] != "" &&
            typeof msg.data.IP["type"] != "undefined" &&
            msg.data.IP["type"] != ""
          ) {
            ip_white_list.push({
              Cid: msg.data.HallId,
              UserLevel: user_level,
              IpType: msg.data.IP["type"],
              Desc: typeof msg.data.IP.desc === "undefined" ? "" : msg.data.IP.desc,
              Ip: msg.data.IP["ip"],
              Name: msg.data.IP["name"],
              State: msg.data.IP["state"],
              GGID: msg.data.IP["ggid"],
              chkFlag: msg.data.IP["chkWhiteBlackFlag"] || "", // 判斷新增白名單(ip_white_list)或黑名單(ip_black_list)
            })
          }

          userDao.CreateIPWhiteList(ip_white_list, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.WHITE_IN_CREATE_FAIL, data: re_before })
            return
          }
          //AFTER-user_白名單
          var white_user = {
            Cid: msg.data.HallId,
            UserLevel: user_level,
            State: [1, 0],
            chkFlag: msg.data.IP["chkWhiteBlackFlag"] || "", // 判斷查詢白名單(ip_white_list)或黑名單(ip_black_list)
          }
          userDao.get_white_list(white_user, cb) //取資料
        },
        function (r_code, r_data, cb) {
          //console.log('AFTER-get_white_list', JSON.stringify(r_code), JSON.stringify(r_data));

          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_WHITE_LIST_FAIL, data: re_before })
            return
          }

          var ip_white_list = r_data[0]["ip_white_list"]
          var log_after_info = []
          for (var i in ip_white_list) {
            re_after.push({
              ipId: ip_white_list[i]["IpId"],
              desc: ip_white_list[i]["Desc"],
              ip: ip_white_list[i]["Ip"],
              type: ip_white_list[i]["IpType"],
              name: ip_white_list[i]["Name"],
              state: ip_white_list[i]["State"],
              ggid: msg.data.IP["chkWhiteBlackFlag"] ? ip_white_list[i]["GGID"] : 0,
            })
            log_after_info.push(ip_white_list[i])
          }

          if (msg.data.IP["chkWhiteBlackFlag"]) {
            log_mod_after = log_mod_after.concat({ ip_black_list: log_after_info })
          } else {
            log_mod_after = log_mod_after.concat({ ip_white_list: log_after_info })
          }

          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["UserName"] = session.get("usrName") || ""

          if (session.get("level") == 1) {
            logData["AdminId"] = session.get("cid") || ""
            // logData['Desc_Before'] = JSON.stringify(log_mod_before);
            logDao.add_log_admin(logData, cb)
          } else {
            logData["Cid"] = session.get("cid") || ""
            logData["ActionCid"] = session.get("cid") || ""
            logData["AdminId"] = 0
            logData["ActionUserName"] = session.get("usrName") || ""
            logData["ActionLevel"] = session.get("level")
            logData["Level"] = session.get("level")
            logData["IP"] = msg.remoteIP
            logDao.add_log_customer(logData, cb)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_FAIL, data: re_before })
            return
          }

          const payload = {
            cacheKeyList: [`WHITE_LIST:${dc}`],
          }

          requestService.delete(`${enpointApiBsAction}/v1/cache`, payload, { callback: cb })
        },
      ],
      function (node, r_code, r_id) {
        next(null, { code: code.OK, data: re_after })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][CreateIPWhiteList] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
//修改黑白名單
handler.ModifyIPWhiteList = function (msg, session, next) {
  try {
    var self = this
    var re_before = []
    var re_after = []
    var logData = {}
    var user_level = 2 // hall
    var funcAction = "EditWhiteList"

    var log_mod_before = []
    var log_mod_after = []
    //var userSession = {};

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.HallId === "undefined" ||
      typeof msg.data.IP === "undefined" ||
      typeof msg.data.IP["ipId"] === "undefined"
    ) {
      next(null, { code: code.DB.PARA_FAIL })
      return
    }

    if (typeof msg.data != "undefined" && typeof msg.data.HallId != "undefined" && msg.data.IP["ip"] !== "") {
      if (msg.data.IP["chkWhiteBlackFlag"]) {
        user_level = 1
        funcAction = "EditBlackList"
      }
    }

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "User"
    logData["FunctionAction"] = funcAction
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""

    const enpointApiBsAction = conf.API_SERVER_URL + consts.APIServerPlatform.bsAction
    const hallId = msg.data.HallId
    let dc = ""

    m_async.waterfall(
      [
        function (cb) {
          userDao.getUser_byId({ userId: hallId, level: 2 }, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_WHITE_LIST_FAIL, data: null })
            return
          }

          dc = r_data[0].DC

          //BEFORE-user_白名單
          var white_user = {
            Cid: msg.data.HallId,
            UserLevel: user_level,
            State: [1, 0],
            chkFlag: msg.data.IP["chkWhiteBlackFlag"] || "", // 判斷查詢白名單(ip_white_list)或黑名單(ip_black_list)
          }
          userDao.get_white_list(white_user, cb) //取資料
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_WHITE_LIST_FAIL, data: null })
            return
          }

          //log 修改前 資料
          var ip_white_list = r_data[0]["ip_white_list"]
          var log_before_info = []
          for (var i in ip_white_list) {
            //回傳_修改前資料
            re_before.push({
              ipId: ip_white_list[i]["IpId"],
              desc: ip_white_list[i]["Desc"],
              ip: ip_white_list[i]["Ip"],
              type: ip_white_list[i]["IpType"],
              name: ip_white_list[i]["Name"],
              state: ip_white_list[i]["State"],
              ggid: msg.data.IP["chkWhiteBlackFlag"] ? ip_white_list[i]["GGID"] : 0,
            })
            //log 只存要修改的資料
            if (+msg.data.IP.ipId == +ip_white_list[i]["IpId"]) {
              log_before_info.push(ip_white_list[i])
            }
          }

          if (msg.data.IP["chkWhiteBlackFlag"]) {
            log_mod_before = log_mod_before.concat({ ip_black_list: log_before_info })
          } else {
            log_mod_before = log_mod_before.concat({ ip_white_list: log_before_info })
          }

          //要變更的白名單
          var ip_whitelist = []
          if (
            typeof msg.data.IP["ip"] != "undefined" &&
            msg.data.IP["ip"] != "" &&
            typeof msg.data.IP["type"] != "undefined" &&
            msg.data.IP["type"] != ""
          ) {
            ip_whitelist.push({
              Cid: msg.data.HallId,
              UserLevel: user_level,
              Desc: typeof msg.data.IP.desc === "undefined" ? "" : msg.data.IP.desc,
              Ip: msg.data.IP["ip"],
              IpType: msg.data.IP["type"],
              IpId: msg.data.IP["ipId"],
              Name: msg.data.IP["name"],
              State: msg.data.IP["state"],
              GGID: msg.data.IP["ggid"],
              chkFlag: msg.data.IP["chkWhiteBlackFlag"] || "", // 判斷修改白名單(ip_white_list)或黑名單(ip_black_list)
            })
          }
          userDao.ModifyIPWhiteList(ip_whitelist, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.IPWHITELIST_MODIFY_FAIL, data: re_before })
            return
          }

          //AFTER-user_白名單
          var white_user = {
            Cid: msg.data.HallId,
            UserLevel: user_level,
            State: [1, 0],
            chkFlag: msg.data.IP["chkWhiteBlackFlag"] || "", // 判斷查詢白名單(ip_white_list)或黑名單(ip_black_list)
          }
          userDao.get_white_list(white_user, cb) //取資料
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_WHITE_LIST_FAIL, data: re_before })
            return
          }

          var ip_white_list = r_data[0]["ip_white_list"]
          var log_after_info = []

          for (var i in ip_white_list) {
            re_after.push({
              ipId: ip_white_list[i]["IpId"],
              desc: ip_white_list[i]["Desc"],
              ip: ip_white_list[i]["Ip"],
              type: ip_white_list[i]["IpType"],
              name: ip_white_list[i]["Name"],
              state: ip_white_list[i]["State"],
              ggid: msg.data.IP["chkWhiteBlackFlag"] ? ip_white_list[i]["GGID"] : 0,
            })
            if (+msg.data.IP.ipId == +ip_white_list[i]["IpId"]) {
              log_after_info.push(ip_white_list[i])
            }
          }

          if (msg.data.IP["chkWhiteBlackFlag"]) {
            log_mod_after = log_mod_after.concat({ ip_black_list: log_after_info })
          } else {
            log_mod_after = log_mod_after.concat({ ip_white_list: log_after_info })
          }

          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["UserName"] = session.get("usrName") || ""

          if (session.get("level") == 1) {
            logData["AdminId"] = session.get("cid") || ""
            logDao.add_log_admin(logData, cb)
          } else {
            logData["Cid"] = session.get("cid") || ""
            logData["ActionCid"] = session.get("cid") || ""
            logData["AdminId"] = 0
            logData["ActionUserName"] = session.get("usrName") || ""
            logData["ActionLevel"] = session.get("level")
            logData["Level"] = session.get("level")
            logData["IP"] = msg.remoteIP
            logDao.add_log_customer(logData, cb)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_FAIL, data: re_before })
            return
          }

          const payload = {
            cacheKeyList: [`WHITE_LIST:${dc}`],
          }

          requestService.delete(`${enpointApiBsAction}/v1/cache`, payload, { callback: cb })
        },
      ],
      function (node, r_code, r_id) {
        next(null, { code: code.OK, data: re_after })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][ModifyIPWhiteList] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//刪除白名單
handler.DeleteIPWhiteList = function (msg, session, next) {
  try {
    var self = this
    var re_before = []
    var re_after = []
    var logData = {}
    var user_level = 2 // hall
    var funcAction = "DeleteWhiteList"
    var ip_list = null
    var re_list = [] // 刪除後回傳的資料

    var log_mod_before = []
    var log_mod_after = []
    //var userSession = {};

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.HallId === "undefined" ||
      typeof msg.data.IP === "undefined"
    ) {
      next(null, { code: code.DB.PARA_FAIL })
      return
    }

    if (typeof msg.data != "undefined" && typeof msg.data.HallId != "undefined") {
      if (msg.data.chkWhiteBlackFlag) {
        user_level = 1
        funcAction = "DeleteBlackList"
      }
    }

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "delete"
    logData["FunctionGroupL"] = "User"
    logData["FunctionAction"] = funcAction
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""

    const enpointApiBsAction = conf.API_SERVER_URL + consts.APIServerPlatform.bsAction
    const hallId = msg.data.HallId
    let dc = ""

    m_async.waterfall(
      [
        function (cb) {
          userDao.getUser_byId({ userId: hallId, level: 2 }, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_WHITE_LIST_FAIL, data: null })
            return
          }

          dc = r_data[0].DC

          //BEFORE-user_白名單
          var white_user = {
            Cid: msg.data.HallId,
            UserLevel: user_level,
            State: [1, 0],
            chkFlag: msg.data.chkWhiteBlackFlag || "", // 判斷查詢白名單(ip_white_list)或黑名單(ip_black_list)
          }

          userDao.get_white_list(white_user, cb) //取資料
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_WHITE_LIST_FAIL, data: null })
            return
          }

          var ip_white_list = r_data[0]["ip_white_list"]
          var log_before_info = []
          for (var i in ip_white_list) {
            //回傳變更前
            if (ip_white_list[i]["State"] == "1") {
              re_before.push(ip_white_list[i])
            }
            //只找刪除的ID
            if (msg.data.IP.indexOf(ip_white_list[i]["IpId"]) > -1) {
              log_before_info.push({
                ipId: ip_white_list[i]["IpId"],
                desc: ip_white_list[i]["Desc"],
                ip: ip_white_list[i]["Ip"],
                type: ip_white_list[i]["IpType"],
                name: ip_white_list[i]["Name"],
                state: ip_white_list[i]["State"],
                cid: ip_white_list[i]["Cid"],
                ggid: msg.data.chkWhiteBlackFlag ? ip_white_list[i]["GGID"] : 0,
              })
            }
          }

          if (msg.data.chkWhiteBlackFlag) {
            log_mod_before = log_mod_before.concat({ ip_black_list: log_before_info })
          } else {
            log_mod_before = log_mod_before.concat({ ip_white_list: log_before_info })
          }

          // 要變更的白名單
          var ip_whitelist = []

          ip_whitelist.push({
            Cid: msg.data.HallId,
            UserLevel: user_level,
            State: -1,
            IpId: msg.data.IP.join(","),
            modifyFlag: false,
            chkFlag: msg.data.chkWhiteBlackFlag || "", // 判斷刪除白名單(ip_white_list)或黑名單(ip_black_list)
          })

          //console.log('del - ip_whitelist', JSON.stringify(ip_whitelist));

          // 更新要刪除的黑(白)名單state (0:停用 1:啟用 -1:刪除)
          userDao.ModifyIPWhiteList(ip_whitelist, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.IPWHITELISTIN_DELETE_FAIL, data: re_before })
            return
          }

          //AFTER-user_白名單
          var white_user = {
            Cid: msg.data.HallId,
            UserLevel: user_level,
            State: [1, 0],
            chkFlag: msg.data.chkWhiteBlackFlag || "", // 判斷查詢白名單(ip_white_list)或黑名單(ip_black_list)
          }
          userDao.get_white_list(white_user, cb) //取資料
        },
        function (r_code, r_data, cb) {
          ip_list = r_data

          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_WHITE_LIST_FAIL, data: re_before })
            return
          }

          var ip_white_list = r_data[0]["ip_white_list"]
          var log_after_info = []

          for (var i in ip_white_list) {
            //變更後回傳資料
            if (ip_white_list[i]["State"] == "1") {
              re_after.push({
                ipId: ip_white_list[i]["IpId"],
                desc: ip_white_list[i]["Desc"],
                ip: ip_white_list[i]["Ip"],
                type: ip_white_list[i]["IpType"],
                name: ip_white_list[i]["Name"],
                state: ip_white_list[i]["State"],
              })
            }
            //log-只找刪除的ID
            if (msg.data.IP.indexOf(ip_white_list[i]["IpId"]) > -1) {
              log_after_info.push(ip_white_list[i])
            }
          }

          if (msg.data.chkWhiteBlackFlag) {
            log_mod_after = log_mod_after.concat({ ip_black_list: log_after_info })
          } else {
            log_mod_after = log_mod_after.concat({ ip_white_list: log_after_info })
          }
          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["UserName"] = session.get("usrName") || ""

          if (session.get("level") == 1) {
            logData["AdminId"] = session.get("cid") || ""
            logDao.add_log_admin(logData, cb)
          } else {
            logData["Cid"] = session.get("cid") || ""
            logData["ActionCid"] = session.get("cid") || ""
            logData["AdminId"] = 0
            logData["ActionUserName"] = session.get("usrName") || ""
            logData["ActionLevel"] = session.get("level")
            logData["Level"] = session.get("level")
            logData["IP"] = msg.remoteIP
            logDao.add_log_customer(logData, cb)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_FAIL, data: re_before })
            return
          }

          const payload = {
            cacheKeyList: [`WHITE_LIST:${dc}`],
          }

          requestService.delete(`${enpointApiBsAction}/v1/cache`, payload, { callback: cb })
        },
      ],
      function (node, r_code, r_id) {
        var ip_white_list = ip_list[0]["ip_white_list"]
        for (var i in ip_white_list) {
          re_list.push({
            ipId: ip_white_list[i]["IpId"],
            desc: ip_white_list[i]["Desc"],
            ip: ip_white_list[i]["Ip"],
            type: ip_white_list[i]["IpType"],
            name: ip_white_list[i]["Name"],
            state: ip_white_list[i]["State"],
            ggid: msg.data.chkWhiteBlackFlag ? ip_white_list[i]["GGID"] : 0,
          })
        }

        next(null, { code: code.OK, data: re_list })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][DeleteIPWhiteList] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 取得黑白名單list
handler.getWhiteBlackList = function (msg, session, next) {
  try {
    var data_usr = {}
    var user_level = 2 // hall

    if (typeof msg.data === "undefined" || typeof msg.data.IP === "undefined") {
      next(null, { code: code.DB.PARA_FAIL })
      return
    }

    if (typeof msg.data.IP["chkWhiteBlackFlag"] != "undefined" && msg.data.IP["chkWhiteBlackFlag"] !== "") {
      user_level = 1
    }

    m_async.waterfall(
      [
        function (cb) {
          var user_list = {
            Cid: msg.data.IP["adId"],
            UserLevel: user_level,
            State: [1, 0],
            chkFlag: msg.data.IP["chkWhiteBlackFlag"] || "", // 判斷查詢白名單(ip_white_list)或黑名單(ip_black_list)
          }

          userDao.get_white_list(user_list, cb) //取資料
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_WHITE_LIST_FAIL, data: null })
            return
          }
          var ip_white_list = []

          for (var i in r_data[0]["ip_white_list"]) {
            ip_white_list.push({
              ipId: r_data[0]["ip_white_list"][i]["IpId"],
              desc: r_data[0]["ip_white_list"][i]["Desc"],
              ip: r_data[0]["ip_white_list"][i]["Ip"],
              type: r_data[0]["ip_white_list"][i]["IpType"],
              name: r_data[0]["ip_white_list"][i]["Name"],
              state: r_data[0]["ip_white_list"][i]["State"],
              ggid: msg.data.IP["chkWhiteBlackFlag"] ? r_data[0]["ip_white_list"][i]["GGID"] : 0,
            })
          }

          data_usr["ip_blacklist"] = ip_white_list

          cb(null, { code: code.OK, msg: "" }, data_usr)
        },
      ],
      function (none, r_code, r_data) {
        next(null, {
          code: code.OK,
          data: {
            data: r_data,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][getWhiteBlackList] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getWhiteType = function (msg, session, next) {
  try {
    var self = this
    m_async.waterfall(
      [
        function (cb) {
          self.app.rpc.config.configRemote.getWhiteType(session, cb)
        },
      ],
      function (none, r_data) {
        next(null, {
          code: code.OK,
          data: {
            white: r_data,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][getWhiteType] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getBlackType = function (msg, session, next) {
  var self = this
  m_async.waterfall(
    [
      function (cb) {
        self.app.rpc.config.configRemote.getBlackType(session, cb)
      },
    ],
    function (none, r_data) {
      next(null, {
        code: code.OK,
        data: {
          white: r_data,
        },
      })
      return
    }
  )
}

//踢人(暫)
handler.KickUser = function (msg, session, next) {
  try {
    var self = this

    if (typeof msg === "undefined" || typeof msg.data.level === "undefined" || typeof msg.data.userId === "undefined") {
      next(null, { code: code.DB.PARA_FAIL })
      return
    }
    var rid = ""

    //玩家
    if (msg.data.level == 4) {
      kickPlayer(msg.data, self.app, next)
    } else {
      //廳主等
      kickUser(msg.data, self.app, next)
    }
  } catch (err) {
    logger.error("[userHandler][KickUser] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

/**
 * 踢掉代理下的所有玩家
 *
 */
handler.KickAllUser = function (msg, session, next) {
  try {
    const self = this

    if (typeof msg === "undefined" || typeof msg.data.agentId === "undefined") {
      next(null, { code: code.DB.PARA_FAIL })
      return
    }

    kickAllPlayer(msg.data, self.app, next)
  } catch (err) {
    logger.error("[userHandler][KickAllUser] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

/**
 * 踢除玩家
 */
var kickPlayer = async function (data, app, callback) {
  try {
    m_async.waterfall(
      [
        function (done) {
          const enpointApiBsAction = conf.API_SERVER_URL + consts.APIServerPlatform.bsAction

          const param = {
            playerId: data.userId,
          }
          utils.httpPost(`${enpointApiBsAction}/v1/player/kick`, param).nodeify(done)
        },
        function (r_data, done) {
          if (!r_data.status || r_data.status != "0000") {
            done(r_data)
            return
          }
          done(null)
        },
      ],
      function (err) {
        if (err) {
          logger.error("[userHandler][kickPlayer] err: ", err)

          callback(null, { code: code.FAIL, data: null })
          return
        }

        callback(null, { code: code.OK, data: {} })
      }
    )
  } catch (err) {
    logger.error("[userHandler][kickPlayer] err: ", err)

    callback(null, { code: code.FAIL, data: null })
  }
}

/**
 * 踢掉指定代理下所有玩家
 */
const kickAllPlayer = async function (data, app, callback) {
  try {
    m_async.waterfall(
      [
        function (done) {
          const agentPayload = {
            userId: data.agentId,
            level: 3, // agent 的 level 為 3
          }

          userDao.getUser_byId(agentPayload, done) // 檢查 agent
        },
        function (r_code, r_data, cb) {
          if (r_code.code != 200 || r_data.length === 0) {
            callback(null, {
              code: code.USR.USER_NOT_EXIST,
              data: null,
            })
            return
          }
          const enpointApiBsAction = conf.API_SERVER_URL + consts.APIServerPlatform.bsAction

          const param = {
            agentId: data.agentId,
          }

          utils.httpPost(`${enpointApiBsAction}/v1/player/kick/all`, param).nodeify(cb)
        },
        function (r_data, done) {
          if (!r_data.status || r_data.status != "0000") {
            done(r_data)
            return
          }
          done(null)
        },
      ],
      function (err) {
        if (err) {
          logger.error("[userHandler][kickAllPlayer] err: ", err)

          callback(null, { code: code.FAIL, data: null })
          return
        }

        callback(null, { code: code.OK, data: {} })
      }
    )
  } catch (err) {
    logger.error("[userHandler][kickAllPlayer] err: ", err)

    callback(null, { code: code.FAIL, data: null })
  }
}

const kickUser = function (data, app, callback) {
  try {
    let uid = ""
    var sessionService = app.get("backendSessionService")
    var userId = ""
    var userName = ""
    var frontendId = ""
    m_async.waterfall(
      [
        function (cb) {
          userDao.getUser_byId(data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK || r_data.length == 0) {
            callback(null, {
              code: code.USR.KICK_USER_FAIL,
              data: null,
            })
            return
          }

          uid = r_data[0]["Cid"] + "_" + r_data[0]["UserName"]
          userId = r_data[0]["Cid"]
          userName = r_data[0]["UserName"]

          const username = r_data[0]["UserName"]
          const dc = r_data[0]["DC"]

          userDao.kickUser({ username, dc }, cb) //踢人
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            callback(null, {
              code: code.USR.KICK_USER_FAIL,
              data: null,
            })
            return
          }

          var param = {
            userId: userId,
            userName: userName,
          }
          userDao.getUserFrontServerByUserId(param, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_data.length > 0) {
          frontendId = r_data[0]["front_server"]
        }
        sessionService.kickByUid(frontendId, uid, function () {
          console.log("-kickBySid---------", uid)
        })
        callback(null, {
          code: code.OK,
          data: {},
        })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][kickUser] catch err", err)
    callback(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//修改hall(sha) / agent OTP
/**
 * msg.data.type :1-admin,2:ha(/sha),3:ag
 *
 *
 */
handler.createOTP = function (msg, session, next) {
  try {
    var self = this
    var re_before = []
    var re_after = []
    var logData = {}

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "User"
    logData["FunctionAction"] = "EditOTP"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    var log_mod_before = []
    var log_mod_after = []
    //var userSession = {};

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.type === "undefined" ||
      typeof msg.data.cid === "undefined"
    ) {
      next(null, { code: code.DB.PARA_FAIL })
      return
    }

    m_async.waterfall(
      [
        function (cb) {
          userDao.checkUsrExist_byCid(msg.data.cid, cb) //判斷有無
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK || msg.data.type != r_data.IsAg) {
            next(null, { code: code.USR.USER_NOT_EXIST, data: null })
            return
          }

          otpData = {
            Cid: msg.data.cid,
            IsAdmin: 0,
            HallId: msg.data.type != 2 || r_data["IsSub"] == 1 ? r_data["HallId"] : -1,
          }
          userDao.get_OtpCode_log(otpData, cb) //log_before
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_OTP_FAIL, data: null })
            return
          }
          log_mod_before = log_mod_before.concat(r_data)

          var secret = m_otplib.authenticator.generateSecret() //secret key

          otpData["OTPCode"] = secret
          userDao.renew_OtpCode(otpData, cb) //del  + create
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.OTP_CREATE_FAIL, data: null })
            return
          }

          userDao.get_OtpCode_log(otpData, cb) //log_after
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_OTP_FAIL, data: null })
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
          next(null, { code: code.USR.LOG_FAIL, data: re_before })
          return
        }
        let data = otpData["OTPCode"]
        if (msg.data.isSelf) {
          data = {
            OTPCode: otpData["OTPCode"],
            isSelf: msg.data.isSelf,
          }
        }

        next(null, { code: code.OK, data: data })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][createOTP] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.modifyUserSetting = function (msg, session, next) {
  try {
    if (typeof msg.data === "undefined") {
      next(null, {
        code: code.DB.PARA_FAIL,
      })
      return
    }
    self = this
    //var userSession = {};

    var logData = {}
    var log_mod_before = []
    var log_mod_after = []

    var data = {}
    if (typeof msg.data.countsOfPerPage != "undefined") data["CountsOfPerPage"] = msg.data.countsOfPerPage
    if (typeof msg.data.hourDiff != "undefined") data["HourDiff"] = msg.data.hourDiff
    var logData = {}
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "Account"
    logData["FunctionAction"] = "EditAccount"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    //var userSession = {};

    m_async.waterfall(
      [
        function (cb) {
          data["Cid"] = session.get("cid")
          data["IsAdmin"] = session.get("level") == 1 ? 1 : 0

          userDao.get_user_setting(data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_SETTING_FAIL, data: null })
            return
          }
          log_mod_before = log_mod_before.concat(r_data)

          var user_setting = r_data[0]["user_setting"]

          data["Uid"] = user_setting.length == 0 ? 0 : user_setting[0]["Uid"] //新增或修改

          if (data["Uid"] == 0) {
            //新增
            userDao.set_user_setting(data, cb)
          } else {
            //修改
            userDao.modify_user_setting(data, cb)
          }
        },
        function (r_code, r_id, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.MODIFY_SETTING_FAIL, data: null })
            return
          }

          data["Uid"] = r_id

          userDao.get_user_setting({ Uid: r_id }, cb) //user_setting初始
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_SETTING_FAIL, data: null })
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
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.LOG_FAIL, data: null })
          return
        }
        var data = {
          countsOfPerPage: msg.data.countsOfPerPage,
          hourDiff: msg.data.hourDiff,
        }
        next(null, { code: code.OK, data: data })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][modifyUserSetting] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

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
    var hallId = 0

    m_async.waterfall(
      [
        function (cb) {
          userDao.getUser_byUserName_temp(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          user = r_data
          if (r_code.code != code.OK) {
            next(null, { code: r_code.code, data: null })
            return
          }
          var nowTimeStamp = timezone.getTimeStamp()
          r_data.TempEndTime = timezone.LocalToUTC(r_data.TempEndTime)

          var tempEndTimeStamp = timezone.getTimeStamp(r_data.TempEndTime)
          if (tempEndTimeStamp < nowTimeStamp) {
            next(null, { code: code.USR.TEMP_PASSWORD_TIMEOUT, data: null }) //逾期
            return
          }
          //密碼錯誤
          if (user.TempPassword != msg.data.password || user.TempMod != 1) {
            next(null, { code: code.USR.USER_PASSWORD_FAIL, data: null }) //密碼失敗
            return
          }
          if (r_data.State === "S") {
            next(null, { code: code.USR.USER_DISABLE, data: null })
            return
          }
          if (r_data.State === "D") {
            next(null, { code: code.USR.USER_NOT_EXIST, data: null })
            return
          }
          if (r_data.State === "F") {
            next(null, { code: code.USR.USER_FREEZE, data: null })
            return
          }

          userDao.getLevelName_byLevel(r_data.Level, cb)
        },
        function (r_code, r_data, cb) {
          user_level = r_data

          if (user_level === "PR") {
            next(null, { code: code.USR.USER_NOT_EXIST, data: null })
            return
          }
          //-----------------------IP----------------------

          var check_upUser_State = 0
          if (user_level === "HA") {
            //hall
            hallId = user.IsSub == true ? user.Upid : user.Cid
            if (user.IsSub == true) {
              check_upUser_State = 1
            } //subhall
          }
          if (user_level === "AG") {
            //agent
            hallId = user.HallId
            check_upUser_State = 1
          }
          if (check_upUser_State === 1 && r_data.ha_state === "S") {
            //找上層USER 狀態
            next(null, { code: code.USR.UP_USER_DISABLE, data: null })
            return
          }
          var ip_list = {
            UserLevel: r_data.Level,
            Cid: hallId,
            IpType: 1,
            Ip: ip,
            State: 1,
          }
          console.log("ip_list-", JSON.stringify(ip_list))
          userDao.checkWhiteIp(ip_list, cb) // **白名單 IP**
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK && ip_check_status) {
          next(null, { code: code.USR.USER_IP_FAIL }, null)
          return
        }

        session.bind(user.Cid + "_" + user.UserName)
        session.set("cid", user.Cid)
        session.set("usrName", user.UserName)
        session.set("level", user.Level)
        session.set("isSub", user.IsSub)
        session.set("hallId", user_level === "HA" && user.IsSub < 1 ? -1 : user.HallId)
        session.set("agentId", user_level === "AG" && user.IsSub < 1 ? "-1" : user.Upid)
        session.pushAll()

        usrData = {
          Cid: user.Cid,
          Hallid: user_level === "HA" && user.IsSub < 1 ? -1 : user.HallId,
          Upid: user_level === "HA" && user.IsSub < 1 ? -1 : user.Upid,
          UserName: user.UserName,
          IsSub: user.IsSub ? true : false,
          IsDemo: user_level === "HA" ? 0 : user.IsDemo,
          Level: user_level,
          State: user.State,
        }
        next(null, { code: code.OK, data: usrData })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][temp_logIn] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.temp_set_password = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.cid === "undefined" ||
      typeof msg.data.password === "undefined"
    ) {
      next(null, { code: code.USR.CURRENCY_PARA_FAIL, data: null })
      return
    }
    var self = this
    var logData = {}

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "Account"
    logData["FunctionAction"] = "EditPassword"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Cid"] = msg.data.cid
    logData["UserName"] = session.get("usrName")

    var log_mod_before = []
    var log_mod_after = []
    //var userSession = {};

    m_async.waterfall(
      [
        function (cb) {
          userDao.checkTempUsrExist_byCid(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST, data: null })
          }

          if (r_data.TempMod != 1) {
            next(null, { code: code.USR.USER_PASSWORD_FAIL, data: null }) //已變更過
            return
          }

          var nowTimeStamp = timezone.getTimeStamp()
          r_data.TempEndTime = timezone.LocalToUTC(r_data.TempEndTime)

          var tempEndTimeStamp = timezone.getTimeStamp(r_data.TempEndTime)
          if (tempEndTimeStamp < nowTimeStamp) {
            next(null, { code: code.USR.TEMP_PASSWORD_TIMEOUT, data: null }) //逾期
            return
          }

          userDao.getPassword_Cid(msg.data.cid, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_PASSWORD_FAIL, data: null })
            return
          }

          log_mod_before = log_mod_before.concat(r_data)
          userDao.modifyPassword_temp(msg.data, cb)
        },
        function (r_code, r_dta, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_MODIFY_FAIL, data: null })
            return
          }

          userDao.getPassword_Cid(msg.data.cid, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_PASSWORD_FAIL, data: null })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)
          logData["ActionLevel"] = session.get("level") || ""
          logData["ActionCid"] = session.get("cid") || ""
          logData["ActionUserName"] = session.get("usrName") || ""
          logData["Level"] = session.get("level") || 0

          logDao.add_log_customer(logData, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.LOG_FAIL, data: null })
          return
        }

        next(null, { code: code.OK, data: {} })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][temp_set_password] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//取權限範本名細
handler.getAuthTempDetail_v2 = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};
    var info = {}
    var user_level = ""

    m_async.waterfall(
      [
        function (cb) {
          adminDao.getDetail_AuthorityFunc({ tmpId: msg.data.id }, cb) //用戶的權限範本
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.AUTH.AUTH_TMP_LOAD_FAIL, data: null })
            return
          }
          var funId = r_data["funcIds"].split(",") //用戶的權限
          info["id"] = r_data["id"]
          info["name"] = r_data["name"]
          info["note"] = r_data["note"]
          info["type"] = r_data["type"]

          if (r_data["typeId"] == "1") {
            user_level = "Admin"
          }
          if (r_data["typeId"] == "2") {
            if (r_data["cid"] == "0") {
              user_level = "Hall"
            } else {
              user_level = "SubHall"
            }
          }
          if (r_data["typeId"] == "3") {
            user_level = "Agent"
          }

          if (r_data["typeId"] == "5") {
            user_level = "SubHall"
          }

          var param = {
            user_level: user_level,
            up_authority: funId,
          }
          self.app.rpc.config.configRemote.getDefault_joinAuthorityFuncs_v3(session, param, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.AUTH.AUTH_TMP_LOAD_FAIL, data: null })
          return
        }
        info["auths"] = r_data[user_level.toLowerCase()]
        next(null, { code: code.OK, data: info })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][getAuthTempDetail_v2] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.disconnect = function (msg, session, next) {
  var token = msg.data.token
  var self = this
  var userInfo = {}
  m_async.waterfall(
    [
      function (cb) {
        userDao.getUserConnection_byKey({ key: token }, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, r_code)
          return
        }

        if (r_data.length > 0) {
          userInfo = r_data[0]
          //關閉
          var param = {
            userId: userInfo["userId"],
            isOnline: 0,
          }
          if (r_data[0]["isAdmin"] == 1) {
            //admin
            adminDao.modifyAdminOnlineState(param, cb)
          } else {
            userDao.modifyUserOnlineState(param, cb)
          }
        } else {
          next(null, { code: code.OK, data: null })
          return
        }
      },
      function (r_code, r_data, cb) {
        userDao.modifyUserCloseTime(userInfo, cb) //更新執行時間
      },
    ],
    function (none, r_code, r_data) {
      next(null, { code: code.OK, data: r_data })
      return
    }
  )
}

//管端取跑馬燈資訊
handler.marquee_msg = function (msg, session, next) {
  try {
    m_async.waterfall(
      [
        function (cb) {
          var userId = ""

          if (session.get("level") == 1) {
            userId = session.get("cid")
          }

          if (session.get("level") == 2) {
            userId = session.get("isSub") == 1 ? session.get("hallId") : session.get("cid")
          }

          if (session.get("level") == 3) {
            userId = session.get("cid")
          }

          var param = {
            userId: userId,
          }
          userDao.getUserMarquee(param, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.LOAD_MARQUEE_FAIL,
            data: null,
          })
          return
        }

        var data = {
          msg: r_data,
        }

        next(null, {
          code: code.OK,
          data: data,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][marquee_msg] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 關閉 User OTP
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

    m_async.waterfall(
      [
        function (cb) {
          // 判斷有無此user
          userDao.checkUsrExist_byCid(msg.data.cid, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_NOT_EXIST,
              data: null,
            })
            return
          }

          otpData = {
            Cid: msg.data.cid,
            IsAdmin: 0,
            HallId: msg.data.type != 2 || r_data["IsSub"] == 1 ? r_data["HallId"] : "-1",
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
        let data = otpData["OTPCode"]
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
    logger.error("[userHandler][closeOTP] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 查詢 wallet
handler.getUserWallet = function (msg, session, next) {
  try {
    if (typeof msg.data === "undefined" || typeof msg.data.cid === "undefined") {
      next(null, {
        code: code.DB.PARA_FAIL,
      })
      return
    }

    var game_currency = []

    let isSub = 0

    m_async.waterfall(
      [
        function (cb) {
          userDao.get_user_byId({ Cid: msg.data.cid }, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_LOAD_FAIL,
            })
            return
          }

          isSub = r_data[0]["customer"][0]["IsSub"]
          // 取得開放幣別
          if (isSub == 1) {
            userDao.get_subUser_byCid(msg.data, cb)
          } else {
            gameDao.getWallet(msg.data, cb)
          }
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            ccode: r_code.code,
            data: null,
          })
          return
        }
        if (
          r_data[0]["Currencies"] != "" &&
          r_data[0]["Currencies"] != undefined &&
          r_data[0]["Currencies"] != "undefined"
        ) {
          game_currency = r_data[0]["Currencies"].split(",")
        } else {
          r_data.forEach((item) => {
            if (item.Currency != null && item.Currency != "") game_currency.push(item.Currency)
          })
        }

        next(null, {
          code: code.OK,
          data: game_currency,
        })

        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][getUserWallet] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 自動新增正式代理
handler.joinAutoAGAccts = function (msg, session, next) {
  try {
    var self = this
    var firstIdx = 0
    var isSingleMulti = msg.data.usr.walletType
    var acctLeng = isSingleMulti.length > 1 ? isSingleMulti.split(",").length : isSingleMulti.length
    var nameSuffix = "M"

    m_async.waterfall(
      [
        function (cb) {
          firstIdx++
          // 多錢包帳號
          nameSuffix = isSingleMulti.split(",")[0] == 0 ? "M" : "S"
          // ex: KW_AgentM
          msg.data.usr["name"] = msg.data.usr.dc + "_Agent" + nameSuffix
          msg.data.usr["nickName"] = msg.data.usr.dc + "_Agent" + nameSuffix
          msg.data.usr["email"] =
            msg.data.usr.dc + "_Agent" + nameSuffix + "@" + msg.data.usr.dc + "_Agent" + nameSuffix
          msg.data.usr["isSingleWallet"] = isSingleMulti.split(",")[0]
          msg.data.usr["password"] = m_md5("a12345")
          // 權限範本固定給值 3(Agent專用)
          msg.data.usr["authorityId"] = 3

          self.joinAG(msg, session, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_CREATE_FAIL, data: null })
            return
          }

          // 單錢包帳號
          if (firstIdx < acctLeng) {
            nameSuffix = isSingleMulti.split(",")[1] == 0 ? "M" : "S"
            // ex: KW_AgentS
            msg.data.usr["name"] = msg.data.usr.dc + "_Agent" + nameSuffix
            msg.data.usr["nickName"] = msg.data.usr.dc + "_Agent" + nameSuffix
            msg.data.usr["email"] =
              msg.data.usr.dc + "_Agent" + nameSuffix + "@" + msg.data.usr.dc + "_Agent" + nameSuffix
            msg.data.usr["isSingleWallet"] = isSingleMulti.split(",")[1]
            msg.data.usr["password"] = m_md5("a12345")
            // 權限範本固定給值 3(Agent專用)
            msg.data.usr["authorityId"] = 3

            self.joinAG(msg, session, cb)
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.USER_CREATE_FAIL, data: null })
          return
        }

        next(null, { code: code.OK }, null)
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][joinAutoAGAccts] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 自動新增測試代理
handler.joinAutoDemoAGAccts = function (msg, session, next) {
  try {
    var self = this
    var firstIdx = 0
    var isSingleMulti = msg.data.usr.walletType
    var acctLeng = isSingleMulti.length > 1 ? isSingleMulti.split(",").length : isSingleMulti.length
    var nameSuffix = "M"

    m_async.waterfall(
      [
        function (cb) {
          firstIdx++
          // 多錢包帳號
          nameSuffix = isSingleMulti.split(",")[0] == 0 ? "M" : "S"
          // ex: KW_testAgentM
          msg.data.usr["name"] = msg.data.usr.dc + "_testAgent" + nameSuffix
          msg.data.usr["nickName"] = msg.data.usr.dc + "_testAgent" + nameSuffix
          msg.data.usr["email"] =
            msg.data.usr.dc + "_testAgent" + nameSuffix + "@" + msg.data.usr.dc + "_testAgent" + nameSuffix
          msg.data.usr["isSingleWallet"] = isSingleMulti.split(",")[0]
          msg.data.usr["password"] = m_md5("a12345")
          // 權限範本固定給值 3(Agent專用)
          msg.data.usr["authorityId"] = 3
          msg.data.usr["isDemo"] = 1

          self.joinAG(msg, session, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_CREATE_FAIL, data: null })
            return
          }
          msg.data.usr["nameSuffix"] = nameSuffix
          self.joinAutoDemoPRAccts(msg, session, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_CREATE_FAIL, data: null })
            return
          }
          // 單錢包帳號
          if (firstIdx < acctLeng) {
            nameSuffix = isSingleMulti.split(",")[1] == 0 ? "M" : "S"
            // ex: KW_testAgentS
            msg.data.usr["name"] = msg.data.usr.dc + "_testAgent" + nameSuffix
            msg.data.usr["nickName"] = msg.data.usr.dc + "_testAgent" + nameSuffix
            msg.data.usr["email"] =
              msg.data.usr.dc + "_testAgent" + nameSuffix + "@" + msg.data.usr.dc + "_testAgent" + nameSuffix
            msg.data.usr["isSingleWallet"] = isSingleMulti.split(",")[1]
            msg.data.usr["password"] = m_md5("a12345")
            msg.data.usr["upid"] = msg.data.usr.hallid
            // 權限範本固定給值 3(Agent專用)
            msg.data.usr["authorityId"] = 3
            msg.data.usr["isDemo"] = 1

            self.joinAG(msg, session, cb)
          } else {
            next(null, { code: code.OK, data: null })
          }
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_CREATE_FAIL, data: null })
            return
          }

          if (firstIdx < acctLeng) {
            msg.data.usr["nameSuffix"] = nameSuffix
            self.joinAutoDemoPRAccts(msg, session, cb)
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.USER_CREATE_FAIL, data: null })
          return
        }

        next(null, { code: code.OK }, null)
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][joinAutoDemoAGAccts] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 自動新增測試玩家
handler.joinAutoDemoPRAccts = function (msg, session, next) {
  try {
    m_async.waterfall(
      [
        function (cb) {
          msg.data.usr["upid"] = msg.data.usr.cid
          userDao.createUser_BatchPlayers(msg.data.usr, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.USER_CREATE_FAIL, data: null })
          return
        }

        next(null, { code: code.OK, data: null })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][joinAutoDemoPRAccts] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 新增、修改、刪除、重置信用額度
handler.credits = function (msg, session, next) {
  try {
    var self = this
    var re_before = []
    var re_after = []
    var logData = {}
    var funcAction = ""

    if (typeof msg.data.type != "undefined") {
      switch (msg.data.type) {
        case consts.CreditType.ADD:
          funcAction = "AddCreditsList"
          logData["ModifiedType"] = "add"
          break
        case consts.CreditType.MODIFY:
          funcAction = "EditCreditsList"
          logData["ModifiedType"] = "edit"
          break
        case consts.CreditType.DELETE:
          funcAction = "DeleteCreditsList"
          logData["ModifiedType"] = "delete"
          break
        case consts.CreditType.RESET:
          funcAction = "ResetCreditsList"
          logData["ModifiedType"] = "reset"
          break
        default:
          funcAction = "RenewCreditsList"
          logData["ModifiedType"] = "renew"
          break
      }
    }

    var log_mod_before = []
    var log_mod_after = []

    if (typeof msg.data === "undefined" || typeof msg.data.cid === "undefined") {
      next(null, { code: code.DB.PARA_FAIL })
      return
    }

    logData["IP"] = msg.remoteIP
    logData["FunctionGroupL"] = "User"
    logData["FunctionAction"] = funcAction
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""

    let db_credits_list = []
    m_async.waterfall(
      [
        function (cb) {
          // BEFORE_信用額度
          var credit_user = {
            Cid: msg.data.cid,
          }

          userDao.get_credits_list(credit_user, cb) // 取資料
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_CREDITS_LIST_FAIL, data: null })
            return
          }

          db_credits_list = r_data[0]["credits_list"]

          userDao.getColumnsType("game.credit", ["CurrentQuota"], cb) // 取欄位型態
        },
        function (r_code, r_data, cb) {
          var log_before_info = []
          // return 資料
          for (var i in credits_list) {
            let alertValue_arr = []
            let notified_arr = []

            // 50,60,70,80,90 -> [50,60,70,80,90]
            for (var k = 0; k < db_credits_list[i]["AlertValue"].split(",").length; k++) {
              alertValue_arr.push(parseInt(db_credits_list[i]["AlertValue"].split(",")[k]))
            }

            // 50,60,70,80,90 -> [50,60,70,80,90]
            if (db_credits_list[i]["Notified"] != null) {
              for (var m = 0; m < db_credits_list[i]["Notified"].split(",").length; m++) {
                notified_arr.push(parseInt(db_credits_list[i]["Notified"].split(",")[m]))
              }
            }

            re_before.push({
              cid: credits_list[i]["Cid"],
              currency: credits_list[i]["Currency"],
              creditQuota: credits_list[i]["CreditQuota"],
              currentQuota: credits_list[i]["CurrentQuota"],
              alertValue: alertValue_arr,
              notified: notified_arr,
            })

            // log 只存要修改的資料
            if (
              msg.data.type == "add" ||
              (msg.data.type == "modify" && msg.data.credits.currency.indexOf(credits_list[i]["Currency"]) > -1) ||
              msg.data.type == "delete" ||
              msg.data.type == "reset" ||
              msg.data.type == "renew"
            ) {
              log_before_info.push(credits_list[i])
            }
          }

          log_mod_before = log_mod_before.concat({ credits_list: log_before_info })

          // 建立 信用額度
          var credits_list = []

          if (typeof msg.data.type != "undefined") {
            let _creditQuota
            if (msg.data.type == consts.CreditType.ADD || msg.data.type == consts.CreditType.MODIFY) {
              let tableSchema = r_data.pop() // ex. tableSchema.Type = 'decimal(18,6)';
              let db_point_length = tableSchema.Type.substr(tableSchema.Type.indexOf(",") + 1, 1) // db小數點長度
              let db_integer_length = tableSchema.Type.substr(tableSchema.Type.indexOf("(") + 1, 2) - db_point_length // db整數長度

              let amount = msg.data.type == consts.CreditType.ADD ? msg.data.credit : msg.data.credits["creditQuota"]
              if (amount < 0) {
                amount = _creditQuota = Math.abs(amount) // 送負數，轉正
              }

              let amount_str = amount.toString().split(".")

              // 預存入數字: 長度超過 DB 可存長度, 設為最大長度 ex. 99999999999.999999
              if (amount_str[0] && amount_str[0].length > db_integer_length) {
                _creditQuota = "".padStart(db_integer_length, "9") + "." + "".padEnd(db_point_length, "9")
              }
              // 預存入數字: 小數點長度超過 DB 可存長度, 設為最大長度 ex. integer.999999
              if (amount_str[1] && amount_str[1].length > db_point_length) {
                _creditQuota = amount_str[0] + "." + "".padEnd(db_point_length, "9")
              }
            }

            switch (msg.data.type) {
              case consts.CreditType.ADD:
                let allAlertValues = msg.data.alertValues.map((i) => Number(i))
                allAlertValues.push(0)
                // 新增信用額度
                credits_list.push({
                  Cid: msg.data.cid,
                  Currency: msg.data.creditsInCurrency,
                  Credit: _creditQuota || msg.data.credit,
                  // AlertValue: msg.data.alertValues
                  AlertValue: allAlertValues.sort(),
                })
                userDao.CreateCreditsList(credits_list, cb)
                break
              case consts.CreditType.MODIFY:
                let modAlertValues = msg.data.credits["alertValue"]
                modAlertValues.push(0)
                // 修改信用額度
                credits_list.push({
                  Cid: msg.data.cid,
                  Currency: msg.data.credits["currency"],
                  CreditQuota: _creditQuota || msg.data.credits["creditQuota"],
                  AlertValue: modAlertValues.sort(),
                  Notified: "",
                })
                userDao.ModifyCreditsList(credits_list, cb)
                break
              case consts.CreditType.DELETE:
                // 刪除信用額度
                credits_list.push({
                  Cid: msg.data.cid,
                  Currency: msg.data.credits["currency"],
                })
                userDao.DeleteCreditsList(credits_list, cb)
                break
              case consts.CreditType.RESET:
                if (msg.data.credits.currency == null && msg.data.credits == "all") {
                  // 重置信用額度
                  userDao.ResetCreditsList(db_credits_list, cb)
                } else {
                  db_credits_list.forEach((item) => {
                    if (item.Currency == msg.data.credits.currency) {
                      credits_list.push({
                        Cid: msg.data.cid,
                        Currency: item.Currency,
                        CreditQuota: item.CreditQuota,
                        CurrentQuota: item.CurrentQuota,
                        AlertValue: item.AlertValue,
                        Notified: "",
                      })
                      // 重置單一幣別信用額度
                      userDao.ResetCreditsList(credits_list, cb)
                    }
                  })
                }
                break
              default:
                cb(null, { code: code.OK }, null)
                break
            }
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            switch (msg.data.type) {
              case consts.CreditType.ADD:
                // 新增信用額度失敗
                next(null, { code: code.USR.CREDITS_CREATE_FAIL, data: re_before })
                break
              case consts.CreditType.MODIFY:
                // 修改信用額度失敗
                next(null, { code: code.USR.CREDITS_MODIFY_FAIL, data: re_before })
                break
              case consts.CreditType.DELETE:
                // 刪除信用額度失敗
                next(null, { code: code.USR.CREDITS_DELETE_FAIL, data: re_before })
                break
              case consts.CreditType.RESET:
                // 重置信用額度失敗
                next(null, { code: code.USR.CREDITS_RESET_FAIL, data: re_before })
                break
              default:
                break
            }

            return
          }
          // AFTER_信用額度
          var credit_user = {
            Cid: msg.data.cid,
          }

          userDao.get_credits_list(credit_user, cb) // 取資料
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_CREDITS_LIST_FAIL, data: re_before })
            return
          }

          var credits_list = r_data[0]["credits_list"]
          var log_after_info = []

          for (var i in credits_list) {
            let alertValue_arr = []
            let notified_arr = []

            // 50,60,70,80,90 -> [50,60,70,80,90]
            for (var k = 0; k < r_data[0]["credits_list"][i]["AlertValue"].split(",").length; k++) {
              if (parseInt(r_data[0]["credits_list"][i]["AlertValue"].split(",")[k]) == 0) {
                continue
              }
              alertValue_arr.push(parseInt(r_data[0]["credits_list"][i]["AlertValue"].split(",")[k]))
            }

            // 50,60,70,80,90 -> [50,60,70,80,90]
            if (r_data[0]["credits_list"][i]["Notified"] != null) {
              for (var m = 0; m < r_data[0]["credits_list"][i]["Notified"].split(",").length; m++) {
                notified_arr.push(parseInt(r_data[0]["credits_list"][i]["Notified"].split(",")[m]))
              }
            }

            re_after.push({
              cid: credits_list[i]["Cid"],
              currency: credits_list[i]["Currency"],
              creditQuota: credits_list[i]["CreditQuota"],
              currentQuota: credits_list[i]["CurrentQuota"],
              alertValue: alertValue_arr,
              notified: notified_arr,
            })
            if (
              msg.data.type == "add" ||
              (msg.data.type == "modify" && msg.data.credits.currency.indexOf(credits_list[i]["Currency"]) > -1) ||
              msg.data.type == "delete" ||
              msg.data.type == "reset" ||
              msg.data.type == "renew"
            ) {
              log_after_info.push(credits_list[i])
            }
          }

          log_mod_after = log_mod_after.concat({ credits_list: log_after_info })
          if (msg.data.type == "modify" || msg.data.type == "delete" || msg.data.type == "reset") {
            logData["Desc_Before"] = JSON.stringify(log_mod_before)
          }
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["UserName"] = session.get("usrName") || ""

          if (session.get("level") == 1) {
            logData["AdminId"] = session.get("cid") || ""
            logDao.add_log_admin(logData, cb)
          } else {
            logData["Cid"] = session.get("cid") || ""
            logData["ActionCid"] = session.get("cid") || ""
            logData["AdminId"] = 0
            logData["ActionUserName"] = session.get("usrName") || ""
            logData["ActionLevel"] = session.get("level")
            logData["Level"] = session.get("level")
            logData["IP"] = msg.remoteIP
            logDao.add_log_customer(logData, cb)
          }
        },
      ],
      function (node, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.LOG_FAIL, data: re_before })
          return
        }

        next(null, { code: code.OK, data: re_after })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][credits] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 轉帳取上線資料
handler.getUpInfinite = function (msg, session, next) {
  try {
    let userInfo = {}

    m_async.waterfall(
      [
        function (cb) {
          if (msg.data.isAg != 4) {
            // 不是 palyer 才需取上線 wallet 資料
            var data = {
              cid: "",
            }
            data["cid"] = msg.data.cid
            data["tableName"] = msg.data.tableName

            userDao.getConcatCurrForWallet(data, cb) // 取上線幣別和 quota
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.LOAD_CURRENCY_FAIL, data: null })
            return
          }

          userInfo = r_data

          var data = {
            userId: "",
            tableName: "",
          }
          data["userId"] = msg.data.cid
          data["tableName"] = msg.data.tableName

          userDao.getUpIdByInfiniteClass(data, cb) // 取所有上線
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
          return
        }

        const userList = [...r_data]

        next(null, {
          code: code.OK,
          data: {
            usrs: userList,
            top_currencies: userInfo != null && userInfo.length >= 1 ? userInfo[0]["top_currencies"] : "",
            top_quotas: userInfo != null && userInfo.length >= 1 ? userInfo[0]["top_quotas"] : "",
            transfer_type: msg.data.tableName,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][getUpInfinite] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 信用轉帳
handler.add_credit_transfer_balance = function (msg, session, next) {
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
    if (typeof msg.data.amount !== "number") {
      next(null, {
        code: code.USR.AMOUNT_NOT_VALID,
      })
      return
    }

    var data = msg.data
    var player = {}
    var re_before = []
    var logData = {}
    var log_mod_before = []
    var log_mod_after = []
    var sys_config = self.app.get("sys_config")
    var main_currency = sys_config.main_currency

    logData["IP"] = msg.remoteIP
    logData["FunctionGroupL"] = "User"
    logData["FunctionAction"] = "CreditTransfer"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    logData["UserName"] = session.get("usrName") || ""

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
        cid: useDestinationInfo ? player.Cid : player.trans_cid,
      }
      let isAg = useDestinationInfo ? player.isAg : player.trans_isAg
      if (isAg == 1) {
        // AD
        cb(null, { code: code.OK })
      } else {
        param.currency = player.trans_curr
        userDao.ModifyCreditQuota(param, (err, r_code, r_data) => {
          if (r_code.code == code.OK && useDestinationInfo) {
            // 更新目標帳號結果額度
            player.balance_after = r_data
          }
          cb(err, r_code)
        })
      }
    }

    m_async.waterfall(
      [
        function (cb) {
          var user = {
            cid: data.cid,
          }

          userDao.get_player_byCid(user, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK || r_data.length == 0) {
            next(null, {
              code: code.USR.USER_LOAD_FAIL,
            })
            return
          }

          player = {
            isAg: r_data[0]["IsAg"],
            Cid: r_data[0]["Cid"],
            main_currency: main_currency,
            currency: r_data[0]["Currency"],
            balance_before: data.user_amount_before,
            amount: data.amount,
            txType: data.type,
            uid: data.uid,
            trans_cid: data.trans_cid, // 轉入(出)帳號來源
            trans_curr: data.trans_curr, // 轉入(出)帳號來源 幣別
            trans_isAg: data.trans_isAg, // 轉入(出)帳號來源，層級 1: admin 2: hall 3: agent 4: player
            trans_amount_before: data.trans_from_before, // 轉入(出)帳號來源，轉移前餘額
          }
          // 轉入
          if (data.type == "deposit") {
            if (data.isAg != 4) {
              // 1: admin 2: hall 3: agent 4: player
              if (data.trans_isAg == 1) {
                // admin
                player["trans_amount_after"] = 0
              } else {
                // 轉入(出)帳號來源，轉移後餘額，層級不是 admin 時，才會扣除上線的錢
                player["trans_amount_after"] = utils.number.sub(data.trans_from_before, data.amount)
              }

              player["balance_after"] = utils.number.add(data.user_amount_before, data.amount)
            } else {
              player["trans_amount_after"] = utils.number.sub(data.trans_from_before, data.amount)
              player["balance_after"] = utils.number.add(data.user_amount_before, data.amount)
            }
          }

          // 轉出
          if (data.type == "withdraw") {
            if (data.isAg != 4) {
              // 1: admin 2: hall 3: agent 4: player
              if (data.trans_isAg == 1) {
                // 轉入(出)帳號來源 admin
                player["trans_amount_after"] = 0
              } else {
                // 轉入(出)帳號來源，轉移後餘額，層級不是 admin 時，才會加上上線的錢
                player["trans_amount_after"] = utils.number.add(data.trans_from_before, data.amount)
              }

              player["balance_after"] = utils.number.sub(data.user_amount_before, data.amount)
            } else {
              player["trans_amount_after"] = utils.number.add(data.trans_from_before, data.amount)
              player["balance_after"] = utils.number.sub(data.user_amount_before, data.amount)
            }
          }

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

          userDao.get_credits_list(player, cb) // 檢查有無資料
        },
        function (r_code, r_data, cb) {
          // 取上線資料

          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_CREDITS_LIST_FAIL, data: null })
            return
          }

          var log_before_info = []
          for (var i in r_data[0]["credits_list"]) {
            log_before_info.push(r_data[0]["credits_list"][i])
          }
          log_mod_before = log_mod_before.concat({ credits_list: log_before_info })
          logData["Desc_Before"] = JSON.stringify(log_mod_before)

          if (data.trans_isAg != 1) {
            player["Cid"] = player["trans_cid"]
            userDao.get_credits_list(player, cb)
            player["Cid"] = data.cid // 還原
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          // 先處理扣錢
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.LOG_CREDITS_LIST_FAIL, data: null })
            return
          }

          var log_after_info = []
          if (data.trans_isAg != 1) {
            for (var i in r_data[0]["credits_list"]) {
              log_after_info.push(r_data[0]["credits_list"][i])
            }
            log_mod_after = log_mod_after.concat({ credits_list: log_after_info })
            logData["Desc_After"] = JSON.stringify(log_mod_after)
          } else {
            log_mod_after = log_mod_after.concat({ credits_list: log_after_info })
            logData["Desc_After"] = JSON.stringify(log_mod_after)
          }

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

          if (session.get("level") == 1) {
            logData["AdminId"] = session.get("cid") || ""
            logDao.add_log_admin(logData, cb)
          } else {
            logData["Cid"] = session.get("cid") || ""
            logData["ActionCid"] = session.get("cid") || ""
            logData["AdminId"] = 0
            logData["ActionUserName"] = session.get("usrName") || ""
            logData["ActionLevel"] = session.get("level")
            logData["Level"] = session.get("level")
            logData["IP"] = msg.remoteIP
            logDao.add_log_customer(logData, cb)
          }
        },
      ],
      function (none, r_code) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.LOG_FAIL, data: re_before })
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
    logger.error("[userHandler][add_credit_transfer_balance] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 轉線 - 初始清單
handler.getTransferLineList = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.page === "undefined" ||
      typeof msg.data.pageCount === "undefined" ||
      typeof msg.data.page != "number" ||
      typeof msg.data.pageCount != "number" ||
      msg.data.page === 0 ||
      msg.data.pageCount === 0
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }

    var self = this
    var ttlCount = 0 // 總筆數
    var pageCur = msg.data.page // 目前頁數
    var pageCount = msg.data.pageCount // 每頁筆數
    var info = []

    // 排序功能
    var sortKey = typeof msg.data.sortKey !== "undefined" && msg.data.sortKey != "" ? msg.data.sortKey : "Transfer_Date"
    var sortType =
      typeof msg.data.sortType !== "undefined" && msg.data.sortType != "" && [0, 1].indexOf(msg.data.sortType) > -1
        ? msg.data.sortType
        : 0

    m_async.waterfall(
      [
        function (cb) {
          userDao.getTransferLineList(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.TRANSFER_LINE_LOAD_FAIL,
            data: null,
          })
          return
        }
        ttlCount = r_data.count // 總筆數
        info = r_data.info

        next(null, {
          code: code.OK,
          data: {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            sortKey: sortKey,
            sortType: sortType,
            lists: info,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][getTransferLineList] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 檢查轉線層級
handler.checkTransferLineLevel = function (msg, session, next) {
  try {
    if (typeof msg.data.operate_acct === "undefined" || typeof msg.data.target_acct === "undefined") {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }

    var data = {
      userName: "",
      isTransferLine: true,
    }

    var self = this
    var operate = {}
    var target = {}

    m_async.waterfall(
      [
        function (cb) {
          data["userName"] = msg.data.operate_acct
          // 操作帳號是否存在
          userDao.checkIsExist_User(data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST_OPERATE, data: null })
            return
          }

          operate = r_data[0]

          data["userName"] = msg.data.target_acct
          // 目標帳號是否存在
          userDao.checkIsExist_User(data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code == code.DB.DATA_EMPTY) {
            msg.data["name"] = msg.data.target_acct
            // 檢查目標帳號是 Admin
            adminDao.getUser_byUserName_temp(msg.data, cb)
          } else if (r_data && r_data != null) {
            target = r_data[0]
            cb(null, { code: code.OK }, null)
          } else {
            next(null, { code: code.USR.USER_NOT_EXIST_TARGET, data: null })
            return
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST_TARGET, data: null })
            return
          }

          // 操作帳號與目標帳號是一樣的，則不再執行轉線(例：B帳號在A帳號下，則B帳號不能再執行轉線到A帳號下)
          if (operate.Upid != -1) {
            if (operate.Upid.indexOf(target.Cid) > -1) {
              next(null, { code: code.DB.PARA_FAIL, data: null })
              return
            }
          }
          if (r_data && r_data != null) {
            // 目標帳號是 Admin 帳號資訊
            target["Cid"] = r_data.AdminId
            target["IsAg"] = 1
            target["IsSub"] = 0
          }

          // 檢查 操作帳號和目標帳號，是否為同一人
          if (operate.Cid.indexOf(target.Cid) > -1) {
            next(null, {
              code: code.USR.TRANSFER_CANNOT_THE_SAME,
              data: null,
            })
            return
          }

          // 檢查轉線層級
          let checkLvl = checkTransferLineLevel(operate, target)
          if (!checkLvl) {
            next(null, {
              code: code.USR.TRANSFER_LINE_LEVEL_FAIL,
              data: null,
            })
            return
          }

          msg.data["operate_acct"] = operate.Cid
          // 找出操作帳號的轉線時間
          userDao.getTransferLineList(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.TRANSFER_LINE_LOAD_FAIL,
              data: null,
            })
            return
          }

          msg.data["target_acct"] = target.Cid
          if (r_data.count >= 1) {
            // 有執行過轉線
            let startDate = r_data["info"][0]["Transfer_Date"].substr(0, 10)
            let endDate = timezone.serverTime().substr(0, 10)

            // 檢查上一次轉線時間距離要轉線當天，是否有超過7天
            if (timezone.isDiff(endDate, startDate, "days") < sys_config.transfer_day_limit) {
              // 少於 7 天
              next(null, {
                code: code.USR.TRANSFER_LINE_LESS_THAN_DAYS,
                data: null,
              })
              return
            }
          }

          // 查詢目標帳號的權限
          if (operate.IsAg != 4) {
            var data = {
              cid: target.Cid,
              authType: operate.IsAg == 3 && operate.IsSub != 1 ? target.IsAg + 1 : target.IsAg,
            }
            if (target.IsAg == 1) {
              // 目標帳號是 Admin，則權限取操作帳號的權限
              data = {
                cid: operate.Cid,
                authType: operate.IsAg,
              }
            }

            userDao.getUserAuthorityTemp(data, cb)
          } else {
            cb(null, { code: code.OK }, null)
          }
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

        next(null, {
          code: code.OK,
          data: {
            operate: operate,
            target: target,
            authorityTemps: r_data,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][checkTransferLine] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 執行轉線
handler.transferLine = function (msg, session, next) {
  try {
    var self = this
    var operate = {}
    var target = {}
    var cids = []
    var data = {
      userName: "",
      isTransferLine: true,
    }

    var logData = {}
    logData["IP"] = msg.remoteIP
    logData["Level"] = 1
    logData["ModifiedType"] = "add"
    logData["FunctionGroupL"] = "System"
    logData["FunctionAction"] = "Transfer"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    var log_mod_before = []
    var log_mod_after = []

    m_async.waterfall(
      [
        function (cb) {
          data["userName"] = msg.data.operate_acct

          // STEP 1: 找出操作帳號的 Cid
          userDao.checkIsExist_User(data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST_OPERATE, data: null })
            return
          }

          operate = r_data[0]
          let before_data = []
          let bData = {
            customer: r_data,
          }
          before_data.push(bData)
          log_mod_before = log_mod_before.concat(before_data)

          data["userName"] = msg.data.target_acct

          // STEP 2: 找出目標帳號的 Cid
          userDao.checkIsExist_User(data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code == code.DB.DATA_EMPTY) {
            msg.data["name"] = msg.data.target_acct
            // 檢查目標帳號是 Admin
            adminDao.getUser_byUserName_temp(msg.data, cb)
          } else if (r_data && r_data != null) {
            target = r_data[0]
            cb(null, { code: code.OK }, null)
          } else {
            next(null, { code: code.USR.USER_NOT_EXIST_TARGET, data: null })
            return
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST_TARGET, data: null })
            return
          }

          if (r_data && r_data != null) {
            // 目標帳號是 Admin 帳號資訊
            target["Cid"] = -1
          }

          msg.data["operate_acct"] = operate.Cid
          msg.data["target_acct"] = target.Cid

          if (operate.IsSub == 0) {
            switch (operate.IsAg) {
              case 4:
                msg.data["transferLine"] = "player"
                break
              case 3:
                msg.data["transferLine"] = "agent"
                break
              case 2:
                msg.data["transferLine"] = "hall"
                break
            }
          } else {
            msg.data["transferLine"] = target.IsAg == 3 ? "user_ag" : "user_ha"
          }
          msg.data["target_acct_Upid"] = target["Upid"]
          msg.data["target_acct_HallId"] = target["HallId"]

          // STEP 3: 更新操作帳號的 Upid 或 HallId(game.customer)
          userDao.updateUpIdHallId_customer(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_MODIFY_FAIL,
              data: null,
            })
            return
          }

          cb(null, { code: code.OK }, null)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
            return
          }

          if (operate.IsAg == 3 && operate.IsSub == 0) {
            // 操作帳號若是 agent，才需更新 player 的 hallid

            const data = { agentId: msg.data["operate_acct"], hallId: msg.data["target_acct"] }

            // STEP 5: 更新該代理下玩家的hallId
            userDao.updatePlayerHallId(data, cb)
          } else {
            cids.push(msg.data["operate_acct"])
            cb(null, { code: code.OK })
          }
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_MODIFY_FAIL,
              data: null,
            })
            return
          }

          msg.data["Operate_IsAg"] = operate.IsAg
          msg.data["Operate_HallId"] = operate.HallId
          msg.data["Old_Upid"] = operate.Upid
          msg.data["New_Upid"] = target.Cid
          msg.data["syncAccounting"] = msg.data.syncAccounting ? 1 : 0
          msg.data["Transfer_Cid"] = session.get("cid")
          // STEP 6: 更新 upid_transfer
          userDao.createUpdate_UpidTransfer(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.TRANSFER_LINE_FAIL,
              data: null,
            })
            return
          }

          if (msg.data.syncAccounting && operate.IsSub == 0) {
            // 是否同步轉移帳務
            msg.data["Cids"] = cids
            self.transferLineProcessing(msg.data, session, cb)
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.TRANSFER_LINE_FAIL,
              data: null,
            })
            return
          }

          data["userName"] = operate.UserName

          // STEP : 找出操作帳號的 Cid
          userDao.checkIsExist_User(data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST_OPERATE, data: null })
            return
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

        if (msg.data.syncAccounting && operate.IsSub == 0) {
          // 是否同步轉移帳務
          // STEP 7: 重新結帳
          reStatisticsRevenue(next)
        }

        next(null, {
          code: code.OK,
          data: null,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][transferLine] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 執行帳務相關處理(wagers_bet、user_revenue_agent、user_revenue_hall)
handler.transferLineProcessing = function (msg, session, next) {
  try {
    var self = this

    m_async.waterfall(
      [
        function (cb) {
          // 更新 wagers_bet 的 UpID 或 HallId
          bettingDao.updateDeleteUpid_revenue(msg, cb)
        },
      ],
      function (none, r_code, r_data) {
        next(null, { code: code.OK }, null)
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][transferLineProcessing] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 轉線重新結帳
var reStatisticsRevenue = function (callback) {
  try {
    self = this
    let updateTime_start, updateTime_end

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

    updateTime_start = timezone.formatTime(Date.now(), "YYYY-MM-DD HH:mm:ss.SSS")
    logger.info("[userHandler][reStatisticsRevenue][updateTime_start] :", updateTime_start)

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
        updateTime_end = timezone.formatTime(Date.now(), "YYYY-MM-DD HH:mm:ss.SSS")
        logger.info("[userHandler][reStatisticsRevenue][updateTime_end] :", updateTime_end)
        logger.info(
          "[userHandler][reStatisticsRevenue][轉線重新統計(MilliSeconds)][END] :",
          timezone.isDiff(updateTime_end, updateTime_start)
        )
      }
    )
  } catch (err) {
    logger.error("[userHandler][reStatisticsRevenue] catch err", err)
    callback(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// 檢查轉線層級
function checkTransferLineLevel(operate, target) {
  let checkLvl = -1
  try {
    let operate_isAg = operate.IsAg
    let operate_isSub = operate.IsSub
    let target_isAg = target.IsAg
    let target_isSub = target.IsSub
    let checkLvl = false

    // 第一種限制：操作帳號如果是 Hall(IsAg=2)，則目標帳號也只能是 Hall(IsAg=2, IsSub=0) 或是 Admin(IsAg=1)
    //           操作帳號如果是 Hall 管理者(IsAg=2, IsSub=1)，則目標帳號只能是 Hall(IsAg=2, IsSub=0) 或 Agent(IsAg=3, IsSub=0)
    // 第二種限制：操作帳號如果是 Agent(IsAg=3)，則目標帳號只能是 Hall(IsAg=2, IsSub=0)
    //           操作帳號如果是 Agent 管理者(IsAg=3, IsSub=1)，則目標帳號只能是 Agent(IsAg=3v) 或 Hall(IsAg=2, IsSub=0)
    // 第三種限制：操作帳號如果是 Player(IsAg=4)，則目標帳號只能是 Agent(IsAg=3, IsSub=0)

    switch (operate_isAg) {
      case 2:
        switch (operate_isSub) {
          case 0:
            if ((target_isAg == 1 || target_isAg == 2) && target_isSub == 0) {
              checkLvl = true
            }
            break
          case 1:
            if ((target_isAg == 2 && target_isSub == 0) || (target_isAg == 3 && target_isSub == 0)) {
              checkLvl = true
            }
            break
        }
        break
      case 3:
        switch (operate_isSub) {
          case 0:
            if (target_isAg == 2 && target_isSub == 0) {
              checkLvl = true
            }
            break
          case 1:
            if ((target_isAg == 2 && target_isSub == 0) || (target_isAg == 3 && target_isSub == 0)) {
              checkLvl = true
            }
            break
        }
        break
      case 4:
        if (target_isAg == 3 && target_isSub == 0) {
          checkLvl = true
        }
        break
    }

    return checkLvl
  } catch (err) {
    logger.error("[userHandler][checkTransferLineLevel] catch err", err)
    return checkLvl
  }
}

//-----------------------------------開放地區---------------------------------------------

// 取得開放地區列表
// msg.data: {pageCur, pageCount, hallId, sortKey, sortType}
handler.openAreaList = function (msg, session, next) {
  try {
    const maxPageCount = 1000

    // 判斷參數
    if (msg.data.pageCur <= 0 || msg.data.pageCount <= 0) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    msg.data.pageCount = Math.min(msg.data.pageCount, maxPageCount)

    m_async.waterfall(
      [
        function (done) {
          // 取得開放地區列表
          let param = {
            pageCur: msg.data.pageCur,
            pageCount: msg.data.pageCount,
            hallId: msg.data.hallId,
            sortKey: msg.data.sortKey,
            sortType: msg.data.sortType,
          }
          userDao.openAreaList(param, done)
        },
        function (r_code, r_data, done) {
          // r_data: {count, list: [Currency, Countries]}
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 轉換資料
          let resp = { count: r_data.count, list: [] }
          for (let row of r_data.list) {
            resp.list.push({
              currency: row.Currency,
              countries: row.Countries,
            })
          }
          done(null, resp)
        },
      ],
      function (err, r_data) {
        // r_data: {count, list: [currency, countries]}
        if (err) {
          logger.error("[userHandler][openAreaList] err: ", JSON.stringify(err))
          next(null, {
            code: code.FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: {
            pageCur: msg.data.pageCur,
            count: r_data.count,
            list: r_data.list,
          },
        })
      }
    )
  } catch (err) {
    logger.error("[userHandler][openAreaList] err: ", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
// 新增開放地區
// msg.data: {hallId, currency, countries: []}
handler.openAreaAdd = function (msg, session, next) {
  try {
    let logModBefore = [{ open_area: [] }]
    let logModAfter = []
    let userData = {}

    m_async.waterfall(
      [
        function (done) {
          // 取得 user 資料
          let param = {
            cid: msg.data.hallId,
          }
          userDao.get_user_byId2(param, done)
        },
        function (r_code, r_data, done) {
          // r_data: [{Cid, UserName, IsAg}]
          if (r_code.code != code.OK || r_data.length == 0) {
            next(null, {
              code: code.USR.USER_LOAD_FAIL,
            })
            return
          }

          userData = r_data[0]

          // 新增開放地區
          let param = {
            hallId: msg.data.hallId,
            currency: msg.data.currency,
            countries: msg.data.countries.sort().join(","),
          }
          userDao.openAreaAdd(param, done)
        },
        function (r_code, done) {
          if (r_code.code != code.OK) {
            // 資料重複
            next(null, {
              code: code.DB.DATA_DUPLICATE,
              data: null,
            })
            return
          }

          // 取得修改後的資料以紀錄
          let param = {
            hallId: msg.data.hallId,
            currency: msg.data.currency,
            useMaster: true,
          }
          userDao.openAreaGet(param, done)
        },
        function (r_code, r_data, done) {
          // r_data: [{HallId, Currency, Countries}]
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 修改後資訊
          logModAfter.push({ open_area: r_data })

          // 加入操作紀錄
          var logData = {
            Cid: msg.data.hallId,
            UserName: userData.UserName,
            Level: userData.IsAg,
            ActionCid: session.get("cid") || "",
            ActionUserName: session.get("usrName") || "",
            ActionLevel: session.get("level"),
            FunctionGroupL: "User",
            FunctionAction: "AddOpenArea",
            ModifiedType: "add",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: JSON.stringify(logModBefore),
            Desc_After: JSON.stringify(logModAfter),
            IP: msg.remoteIP,
          }
          logDao.add_log_customer(logData, done)
        },
      ],
      function (err) {
        if (err) {
          logger.error("[userHandler][openAreaAdd] err: ", JSON.stringify(err))
          next(null, {
            code: code.FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: {},
        })
      }
    )
  } catch (err) {
    logger.error("[userHandler][openAreaAdd] err: ", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
// 修改開放地區
// msg.data: {hallId, currency, countries: []}
handler.openAreaMod = function (msg, session, next) {
  try {
    let logModBefore = []
    let logModAfter = []
    let userData = {}

    m_async.waterfall(
      [
        function (done) {
          // 取得 user 資料
          let param = {
            cid: msg.data.hallId,
          }
          userDao.get_user_byId2(param, done)
        },
        function (r_code, r_data, done) {
          // r_data: [{Cid, UserName, IsAg}]
          if (r_code.code != code.OK || r_data.length == 0) {
            next(null, {
              code: code.USR.USER_LOAD_FAIL,
            })
            return
          }

          userData = r_data[0]

          // 取得修改前的資料以紀錄
          let param = {
            hallId: msg.data.hallId,
            currency: msg.data.currency,
          }
          userDao.openAreaGet(param, done)
        },
        function (r_code, r_data, done) {
          // r_data: [{HallId, Currency, Countries}]
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 修改前資訊
          logModBefore.push({ open_area: r_data })

          // 修改開放地區
          let param = {
            hallId: msg.data.hallId,
            currency: msg.data.currency,
            countries: msg.data.countries.sort().join(","),
          }
          userDao.openAreaMod(param, done)
        },
        function (r_code, done) {
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 取得修改後的資料以紀錄
          let param = {
            hallId: msg.data.hallId,
            currency: msg.data.currency,
            useMaster: true,
          }
          userDao.openAreaGet(param, done)
        },
        function (r_code, r_data, done) {
          // r_data: [{HallId, Currency, Countries}]
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 修改後資訊
          logModAfter.push({ open_area: r_data })

          // 加入操作紀錄
          var logData = {
            Cid: msg.data.hallId,
            UserName: userData.UserName,
            Level: userData.IsAg,
            ActionCid: session.get("cid") || "",
            ActionUserName: session.get("usrName") || "",
            ActionLevel: session.get("level"),
            FunctionGroupL: "User",
            FunctionAction: "EditOpenArea",
            ModifiedType: "edit",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: JSON.stringify(logModBefore),
            Desc_After: JSON.stringify(logModAfter),
            IP: msg.remoteIP,
          }
          logDao.add_log_customer(logData, done)
        },
      ],
      function (err) {
        if (err) {
          logger.error("[userHandler][openAreaMod] err: ", JSON.stringify(err))
          next(null, {
            code: code.FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: {},
        })
      }
    )
  } catch (err) {
    logger.error("[userHandler][openAreaMod] err: ", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
// 刪除開放地區
// msg.data: {hallId, currency}
handler.openAreaDel = function (msg, session, next) {
  try {
    let logModBefore = []
    let logModAfter = [{ open_area: [] }]
    let userData = {}

    m_async.waterfall(
      [
        function (done) {
          // 取得 user 資料
          let param = {
            cid: msg.data.hallId,
          }
          userDao.get_user_byId2(param, done)
        },
        function (r_code, r_data, done) {
          // r_data: [{Cid, UserName, IsAg}]
          if (r_code.code != code.OK || r_data.length == 0) {
            next(null, {
              code: code.USR.USER_LOAD_FAIL,
            })
            return
          }

          userData = r_data[0]

          // 取得修改前的資料以紀錄
          let param = {
            hallId: msg.data.hallId,
            currency: msg.data.currency,
          }
          userDao.openAreaGet(param, done)
        },
        function (r_code, r_data, done) {
          // r_data: [{HallId, Currency, Countries}]
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 修改前資訊
          logModBefore.push({ open_area: r_data })

          // 刪除開放地區
          let param = {
            hallId: msg.data.hallId,
            currency: msg.data.currency,
          }
          userDao.openAreaDel(param, done)
        },
        function (r_code, done) {
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 加入操作紀錄
          var logData = {
            Cid: msg.data.hallId,
            UserName: userData.UserName,
            Level: userData.IsAg,
            ActionCid: session.get("cid") || "",
            ActionUserName: session.get("usrName") || "",
            ActionLevel: session.get("level"),
            FunctionGroupL: "User",
            FunctionAction: "DeleteOpenArea",
            ModifiedType: "delete",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: JSON.stringify(logModBefore),
            Desc_After: JSON.stringify(logModAfter),
            IP: msg.remoteIP,
          }
          logDao.add_log_customer(logData, done)
        },
      ],
      function (err) {
        if (err) {
          logger.error("[userHandler][openAreaDel] err: ", JSON.stringify(err))
          next(null, {
            code: code.FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: {},
        })
      }
    )
  } catch (err) {
    logger.error("[userHandler][openAreaDel] err: ", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

/**
 * @deprecated
 *
 * @param {*} msg
 * @param {*} session
 * @param {*} next
 */
handler.getCidByParent = function (msg, session, next) {
  try {
    m_async.waterfall(
      [
        function (cb) {
          const targetHallId = msg.data.cid

          userDao.getChildList({ cid: targetHallId }, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.USR.USER_LOAD_FAIL, data: null })
          return
        }

        const list = r_data[0].list.split(",")

        // 去掉$和自己
        const [, , ...result] = list

        next(null, {
          code: code.OK,
          data: result,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][getCidByParent] err: ", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//-----------------------------------域名設定---------------------------------------------
// 域名設定頁面初始資料
handler.domainSettingPageInit = function (msg, session, next) {
  try {
    var self = this

    let respData = {}

    m_async.waterfall(
      [
        function (done) {
          // 取得遊戲類別
          self.app.rpc.config.configRemote.getGameGroup(session, done)
        },
        function (r_code, r_data, done) {
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          respData["gameGroups"] = r_data

          done(null)
        },
      ],
      function (err) {
        if (err) {
          logger.error("[userHandler][domainSettingPageInit] err: ", JSON.stringify(err))
          next(null, {
            code: code.FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: respData,
        })
      }
    )
  } catch (err) {
    logger.error("[userHandler][domainSettingPageInit] err: ", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
// 取得域名設定
// msg.data: {hallId}
// 回傳 data: null 表示未設定
handler.domainSettingGet = function (msg, session, next) {
  try {
    m_async.waterfall(
      [
        function (done) {
          // 取得域名設定
          let param = {
            hallId: msg.data.hallId,
            useMaster: false,
          }
          userDao.domainSettingGet(param, done)
        },
        function (r_code, r_data, done) {
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          if (r_data.length > 0) {
            done(null, r_data[0])
          } else {
            done(null, null)
          }
        },
      ],
      function (err, r_data) {
        if (err) {
          logger.error("[userHandler][domainSettingGet] err: ", JSON.stringify(err))
          next(null, {
            code: code.FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: r_data,
        })
      }
    )
  } catch (err) {
    logger.error("[userHandler][domainSettingGet] err: ", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
// 編輯域名設定
// msg.data: {hallId, url, merchId, merchPwd, apiKey, defAgent,
//            showCreditSymbol, lobbyBalance, showClock, showHelp, oneClickHelp, verifyToken, demoWallet,
//            ggIdSettings: [ggId, singleWalletType, singleWalletClipType, clip, clipReload]}
handler.domainSettingEdit = function (msg, session, next) {
  try {
    const defaultHallId = "MOCK" // 若 hallId 為 'MOCK' 為預設域名設定
    let logModBefore = []
    let logModAfter = []
    let checkHallId = false
    let userData = {}
    let checkDefAgent = false
    let defAgentId = -1

    m_async.waterfall(
      [
        function (done) {
          if (msg.data.hallId === defaultHallId) {
            // 'MOCK' 不檢查 hallId
            checkHallId = true
            done(null, { code: code.OK }, null)
          } else {
            // 取得 hall
            let param = {
              Cid: msg.data.hallId,
            }
            userDao.get_user_byId(param, done)
          }
        },
        function (r_code, r_data, done) {
          // r_data: [customer: [UserName, IsAg, IsSub, ...]]
          if (r_code.code != code.OK) {
            next(null, { code: code.FAIL, data: null })
            return
          }
          if (!checkHallId) {
            r_data = r_data[0].customer
            if (r_data.length === 0 || r_data[0].IsAg != 2 || r_data[0].IsSub != 0) {
              next(null, { code: code.FAIL, data: null })
              return
            }
            userData = r_data[0]
          }

          if (msg.data.defAgent === "" || msg.data.hallId === defaultHallId) {
            // 'MOCK' 不檢查 defAgent
            checkDefAgent = true
            done(null, { code: code.OK }, null)
          } else {
            // 取得 agent
            let param = {
              userName: msg.data.defAgent,
            }
            userDao.getUser_byUserName2(param, done)
          }
        },
        function (r_code, r_data, done) {
          // r_data: [Cid, UserName, Upid, HallId, IsAg, IsSub]
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_NOT_EXIST, data: null })
            return
          }
          if (!checkDefAgent) {
            if (
              r_data.length === 0 ||
              r_data[0].HallId != msg.data.hallId ||
              r_data[0].IsAg != 3 ||
              r_data[0].IsSub != 0
            ) {
              next(null, { code: code.USR.USER_NOT_EXIST, data: null })
              return
            }
            defAgentId = r_data[0].Cid
          }

          // 取得域名設定
          let param = {
            hallId: msg.data.hallId,
            useMaster: false,
          }
          userDao.domainSettingGet(param, done)
        },
        function (r_code, r_data, done) {
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 編輯前資訊
          logModBefore.push({ domain_setting: r_data })

          // 修改域名設定
          let param = {
            ...msg.data,
            defAgentId,
          }
          userDao.domainSettingEdit(param, done)
        },
        function (r_code, done) {
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 取得編輯後的資料以紀錄
          let param = {
            hallId: msg.data.hallId,
            useMaster: true,
          }
          userDao.domainSettingGet(param, done)
        },
        function (r_code, r_data, done) {
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 修改後資訊
          logModAfter.push({ domain_setting: r_data })

          if (msg.data.hallId === defaultHallId) {
            // 'MOCK' 為預設域名設定
            // 加入操作紀錄
            let logData = {
              AdminId: session.get("cid") || "",
              UserName: session.get("usrName") || "",
              FunctionGroupL: "System",
              FunctionAction: "EditDefaultDomainSetting",
              ModifiedType: "edit",
              RequestMsg: JSON.stringify(msg),
              Desc_Before: JSON.stringify(logModBefore),
              Desc_After: JSON.stringify(logModAfter),
              IP: msg.remoteIP,
            }
            logDao.add_log_admin(logData, done)
          } else {
            // 加入操作紀錄
            let logData = {
              Cid: msg.data.hallId,
              UserName: userData.UserName,
              Level: userData.IsAg,
              ActionCid: session.get("cid") || "",
              ActionUserName: session.get("usrName") || "",
              ActionLevel: session.get("level"),
              FunctionGroupL: "User",
              FunctionAction: "EditDomainSetting",
              ModifiedType: "edit",
              RequestMsg: JSON.stringify(msg),
              Desc_Before: JSON.stringify(logModBefore),
              Desc_After: JSON.stringify(logModAfter),
              IP: msg.remoteIP,
            }
            logDao.add_log_customer(logData, done)
          }
        },
      ],
      function (err) {
        if (err) {
          logger.error("[userHandler][domainSettingEdit] err: ", JSON.stringify(err))
          next(null, {
            code: code.FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: {},
        })
      }
    )
  } catch (err) {
    logger.error("[userHandler][domainSettingEdit] err: ", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
// 刪除域名設定
// msg.data: {hallId}
handler.domainSettingDel = function (msg, session, next) {
  try {
    let logModBefore = []
    let logModAfter = []
    let userData = {}

    m_async.waterfall(
      [
        function (done) {
          // 取得 hall
          let param = {
            Cid: msg.data.hallId,
          }
          userDao.get_user_byId(param, done)
        },
        function (r_code, r_data, done) {
          // r_data: [customer: [UserName, IsAg, IsSub, ...]]
          if (r_code.code != code.OK) {
            next(null, { code: code.FAIL, data: null })
            return
          }
          r_data = r_data[0].customer
          if (r_data.length === 0 || r_data[0].IsAg != 2 || r_data[0].IsSub != 0) {
            next(null, { code: code.FAIL, data: null })
            return
          }
          userData = r_data[0]

          // 取得域名設定
          let param = {
            hallId: msg.data.hallId,
            useMaster: false,
          }
          userDao.domainSettingGet(param, done)
        },
        function (r_code, r_data, done) {
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 修改前資訊
          logModBefore.push({ domain_setting: r_data })

          // 刪除域名設定
          let param = {
            hallId: msg.data.hallId,
          }
          userDao.domainSettingDel(param, done)
        },
        function (r_code, done) {
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 取得修改後的資料以紀錄
          let param = {
            hallId: msg.data.hallId,
            useMaster: true,
          }
          userDao.domainSettingGet(param, done)
        },
        function (r_code, r_data, done) {
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 修改後資訊
          logModAfter.push({ domain_setting: r_data })
          // 加入操作紀錄
          let logData = {
            Cid: msg.data.hallId,
            UserName: userData.UserName,
            Level: userData.IsAg,
            ActionCid: session.get("cid") || "",
            ActionUserName: session.get("usrName") || "",
            ActionLevel: session.get("level"),
            FunctionGroupL: "User",
            FunctionAction: "DeleteDomainSetting",
            ModifiedType: "delete",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: JSON.stringify(logModBefore),
            Desc_After: JSON.stringify(logModAfter),
            IP: msg.remoteIP,
          }
          logDao.add_log_customer(logData, done)
        },
      ],
      function (err) {
        if (err) {
          logger.error("[userHandler][domainSettingDel] err: ", JSON.stringify(err))
          next(null, {
            code: code.FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: {},
        })
      }
    )
  } catch (err) {
    logger.error("[userHandler][domainSettingDel] err: ", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getOnlinePlayers = async function (msg, session, next) {
  try {
    const self = this
    const redisCache = self.redisCache

    const userLevel = new UserLevelService({ session })

    const { isAdmin, isAgent, isHall, cid } = userLevel.getUserLevelData()

    const isoString = timezone.getTimezoneDate(0).toISOString()
    const { DEFAULT_DATE_FORMAT } = timezone

    const startDate = moment(isoString).startOf("minute").subtract(1, "minute").format(DEFAULT_DATE_FORMAT)
    const endDate = moment(isoString).startOf("minute").format(DEFAULT_DATE_FORMAT)

    const cacheTime = moment(isoString).startOf("minute").format("YYYY_MM_DD:HH_mm_ss")

    const cacheKey = isAdmin ? `admin:${cacheTime}` : `${cid}:${cacheTime}`

    const cachedData = await redisCache.getActiveOnlinePlayerCounts({ cacheKey })

    const cachedPlayerCounts = isEmpty(cachedData) ? -1 : Number(cachedData)

    if (cachedPlayerCounts >= 0) {
      next(null, { code: code.OK, data: { activePlayerCounts: cachedPlayerCounts } })
      return
    }

    m_async.waterfall(
      [
        function (cb) {
          // 取得底下所有代理id
          if (isHall) {
            userDao.getChildList({ cid, isAg: 3 }, cb)
          } else {
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.USR.USER_FAIL, data: null })
            return
          }

          if (isAdmin) {
            bettingDao.countAllActivePlayerByWagers({ startDate, endDate }, cb)
          } else if (isHall) {
            const list = r_data[0].list.split(",")

            // 去掉$和自己
            list.splice(0, 2)

            const agentIdList = [...list]
            if (agentIdList.length === 0) {
              next(null, { code: code.OK, data: { activePlayerCounts: 0 } })
              return
            }
            bettingDao.countAgentActivePlayerByWagers({ startDate, endDate, agentIdList }, cb)
          } else if (isAgent) {
            const agentIdList = [cid]
            bettingDao.countAgentActivePlayerByWagers({ startDate, endDate, agentIdList }, cb)
          }
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, { code: code.FAIL, data: null })
          return
        }

        const { counts } = r_data[0]

        next(null, { code: code.OK, data: { activePlayerCounts: counts } })

        redisCache.setActiveOnlinePlayerCounts({ cacheKey, counts })

        return
      }
    )
  } catch (err) {
    logger.error("[userHandler][getOnlinePlayers] catch err", inspect(err))
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
