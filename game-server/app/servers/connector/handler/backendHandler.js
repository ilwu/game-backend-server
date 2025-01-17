var logger = require("pomelo-logger").getLogger("backendHandler", __filename)
var async = require("async")
var m_otplib = require("otplib")
var code = require("../../../util/code")
var conf = require("../../../../config/js/conf")
var codeMsg = require("../../../util/codeMsg")
const crypto = require("crypto")
var timezone = require("../../../util/timezone")
var sprintf = require("sprintf-js").sprintf
var userDao = require("../../../DataBase/userDao")
var adminDao = require("../../../DataBase/adminDao")
var logDao = require("../../../DataBase/logDao")
var mailService = require("../../../services/mailService")
var mail = new mailService()
var otp_token_status = true //判斷OTP狀態
var onlinePlayer = []
var pomelo = require("pomelo")
var gameServer = conf.GAME_SERVER
var uploadImageUrl = conf.UPLOAD_IMAGE_URL
var jwt = require("jsonwebtoken")
var consts = require("../../../share/consts")

const { default: ShortUniqueId } = require("short-unique-id")
const uid = new ShortUniqueId()

var ip_check_status = false //暫關白名單確認
var api_token_status = false //暫關_api_token

module.exports = function (app) {
  return new Handler(app)
}

var Handler = function (app) {
  this.app = app
}

var handler = Handler.prototype

/**
 * 進入遊戲時檢查auth id 碼.
 *
 * @param  {Object}   msg     request message
 * @param  {Object}   session current session object
 * @param  {Function} next    next stemp callback
 * @return {Void}
 */

//控端登入 (http方式 取token)
handler.admin_login = function (msg, session, next) {
  console.log("-admin_login msg-", JSON.stringify(msg))

  var self = this
  var ip = ""
  if (typeof msg.remoteIP != "undefined" && msg.remoteIP.substr(0, 7) == "::ffff:") {
    ip = msg.remoteIP.substr(7)
  }
  var sessionService = self.app.get("sessionService")
  var usrData = {}
  var self = this
  var uid = ""
  var data = {}
  var jwt_token = ""
  var sys_config = self.app.get("sys_config")

  async.waterfall(
    [
      function (cb) {
        adminDao.getOperator(msg.data, cb)
      },
      function (r_code, r_data, cb) {
        data = r_data
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.USER_NOT_EXIST,
            data: null,
          })
          return
        }
        if (data.Passwd != msg.data.password) {
          next(null, {
            code: code.USR.USER_PASSWORD_FAIL,
            data: null,
          })
          return
        }

        /*
                                //已登入
                                if (data.IsOnline == 1) {
                                    next(null, { code: code.USR.USER_LOGIN_DUPLICATE, data: null });
                                    return;
                                }
                    */
        var token = m_otplib.authenticator.generate(data.OTPCode) //產生 token

        if ((msg.data.token != token || msg.data.token != 999999) && otp_token_status) {
          next(null, {
            code: code.USR.USER_OTP_FAIL,
            data: null,
          }) //token 比對錯誤
          return
        }
        //停用
        if (data.State === "S") {
          next(null, {
            code: code.USR.USER_DISABLE,
            data: null,
          })
          return
        }

        if (data.State === "D") {
          next(null, {
            code: code.USR.USER_NOT_EXIST,
            data: null,
          })
          return
        }

        var ip_list = {
          UserLevel: 1,
          Cid: data.AdminId,
          IpType: 1,
          Ip: ip,
          State: 1,
        }
        userDao.checkWhiteIp(ip_list, cb) // **白名單 IP**
        // self.app.rpc.user.userRemote.check_white_ip(session, ip_list, cb); // **白名單 IP**
      },
      function (r_code, r_data, cb) {
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
        //判斷有無session
        uid = data.AdminId + "*" + data.UserName
        //duplicate log in
        if (!!sessionService.getByUid(uid)) {
          console.log(" USER_LOGIN_DUPLICATE")
          next(null, {
            code: code.USR.USER_LOGIN_DUPLICATE,
            error: true,
          })
          return
        } else {
          console.log("no USER_LOGIN_DUPLICATE")
        }
        /*
                    session.bind(uid);
                    session.set('cid', data.AdminId);
                    session.set('usrName', data.UserName);
                    session.set('level', 1);
                    session.pushAll();
                    */
        //產生JWT
        var jwt_info = {
          id: data.AdminId,
          user_name: data.UserName,
          level: 1,
          isSub: 0,
          hallId: 0,
          agentId: "-1",
        }
        jwt_token = jwt.sign(jwt_info, sys_config.jwt_key, {
          expiresIn: "1h",
        })

        var param = {
          isAdmin: 1,
          userId: data.AdminId,
          userName: data.UserName,
          token: jwt_token,
          frontendId: "",
        }
        userDao.modifyUserConnectState_v2(param, cb) //更新登入
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.USER_PARA_FAIL,
            data: null,
          })
          return
        }

        //登入紀錄
        var login_data = {
          Cid: data.AdminId,
          UserName: data.UserName,
          Level: 1,
          IsSub: 0,
          LType: "IN",
          LDesc: "login",
          browser: msg.data.browser || "",
          browser_version: msg.data.browser_version || "",
          os: msg.data.os || "",
          os_version: msg.data.os_version || "",
          isMobile: msg.data.isMobile || 0,
          isTablet: msg.data.isTablet || 0,
          isDesktopDevice: msg.data.isDesktopDevice || 0,
          IP: msg.remoteIP,
        }
        logDao.add_loginout_log(login_data, cb)
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code != code.OK) {
        next(null, {
          code: r_code,
          data: null,
        })
        return
      }

      /*
                var rid = uid;
                var channel = self.app.get('channelService').getChannel(rid, true);

                if (!!channel) {
                    var members = channel.getMembers();
                    if (members.indexOf(rid) == -1) {
                        channel.add(rid, session.frontendId); //加入用戶
                    }
                }
            */
      next(null, {
        code: code.OK,
        data: jwt_token,
      })
      return
    }
  )
}

//判斷連線身分
handler.check_user_token = function (msg, session, next) {
  var self = this
  var sys_config = self.app.get("sys_config")
  var token = msg.token
  var sessionService = self.app.get("sessionService")
  var channelService = self.app.get("channelService")
  //判斷token
  if (msg.conn == 1) session.on("closed", onUserLeave.bind(null, self.app))
  var token_timestamp = session.get("token_timestamp")
  var now_timestamp = new Date().getTime()

  //30秒以上再token驗證
  if (token_timestamp == undefined || (token_timestamp != undefined && now_timestamp - token_timestamp >= 30 * 1000)) {
    var user = session.get("cid") + "*" + session.get("usrName")
    if (sessionService.getByUid(user)) {
      session.set("token_timestamp", new Date().getTime()) //更新時間
      session.pushAll()
    }
    //console.log('session token_timestamp----------------------- ',now_timestamp - token_timestamp, token_timestamp,now_timestamp);
    jwt.verify(token, sys_config.jwt_key, function (err, decoded) {
      if (err) {
        //逾期 失效
        var jwt_decoded = jwt.decode(token)
        console.log("------jwt_decoded------------", token, JSON.stringify(jwt_decoded))
        if (
          typeof jwt_decoded.id == "undefined" ||
          typeof jwt_decoded.user_name == "undefined" ||
          typeof jwt_decoded.level == "undefined"
        ) {
          next(null, {
            code: code.TIMEOUT,
          })
          return
        }
        var uid = jwt_decoded.id + "*" + jwt_decoded.user_name
        var rid = jwt_decoded.level == 1 ? "admin" : "user"
        var sid = self.app.get("serverId")
        //----------------------------------------------

        //channel 有無此member 有->kick
        var user_session = sessionService.getByUid(uid)
        if (!!user_session) {
          var serverId = user_session[0].frontendId
          var id = user_session[0].id
          var uid = user_session[0].uid
          sessionService.kick(uid)
          console.log("remove - sessionService", sessionService.getByUid(uid))
          sid = serverId
        }
        var channel = channelService.getChannel(rid, false)
        if (!!channel) {
          var user = channel.getMember(uid)
          console.log("channel mem-01", channel.getMembers())
          if (!!user) {
            channel.leave(uid, sid)
            console.log("channel mem-02", channel.getMembers())
          }
        }
        //----------------------------------------------
        next(null, {
          code: code.TIMEOUT,
        })
        return
      } else {
        async.waterfall(
          [
            function (cb) {
              var param = {
                token: token,
              }
              userDao.getUserInfoByToken(param, cb)
            },
            function (r_code, r_data, cb) {
              if (r_code.code != code.OK || r_data.length == 0) {
                //超過時間清除 或不正常方式連線
                next(null, {
                  code: code.TIMEOUT,
                })
                return
              }

              var uid = decoded.id + "*" + decoded.user_name
              var rid = decoded.level == 1 ? "admin" : "user"

              var param = {
                cid: decoded.id,
                usrName: decoded.user_name,
                level: decoded.level,
                uid: uid,
              }
              if (!sessionService.getByUid(uid)) {
                session.bind(uid)
                session.set("cid", decoded.id)
                session.set("usrName", decoded.user_name)
                session.set("level", decoded.level) //----------- 再修
                session.set("isSub", decoded.isSub)
                session.set("hallId", decoded.hallId)
                session.set("agentId", decoded.agentId)
                session.set("timestamp", new Date().getTime())
                session.set("token_timestamp", new Date().getTime())
                session.pushAll()
              } else {
                // console.log('set token_timestamp -------------', new Date().getTime());
                // session.set('token_timestamp', new Date().getTime()); //更新時間
                // session.pushAll();
              }

              var channel = channelService.getChannel(rid, true)
              if (!!channel) {
                var member = channel.getMember(uid)
                if (!member) {
                  channel.add(uid, session.frontendId) //加入用戶
                }
                console.log("channel mems:", channel.getMembers())
              }
              //更新user 前端serverID
              var param = {
                token: token,
                frontendId: session.frontendId,
              }
              //更新user connect
              userDao.modifyUserInfoByToken(param, cb)
            },
          ],
          function (node, r_code, r_data) {
            next(null, {
              code: code.OK,
            })
            return
          }
        )
      }
    })
  } else {
    next(null, {
      code: code.OK,
    })
    return
  }
}

/*
 * User log out handler
 *
 * @param {Object} app current application
 * @param {Object} session current session object
 *
 */
var onUserLeave = function (app, session) {
  console.log("------------------onUserLeave app---------------", app.get("serverId"))
  if (!session || !session.uid) {
    return
  }
  console.log("session level-", session.get("level"), "serverId-" + app.get("serverId"))

  var rid = session.get("level") == 1 ? "admin" : "user"

  console.log("-leave session uid-", session.uid, "rid-" + rid)

  var channelService = app.get("channelService")
  var sessionService = app.get("sessionService")
  var channel = channelService.getChannel(rid, false)

  console.log("-channelService members before-", rid, channel.getMembers())
  var channel = channelService.getChannel(rid, false)
  if (!!channel) {
    if (channel.getMember(session.uid)) {
      channel.leave(session.uid, app.get("serverId"))
      var param = {
        cid: session.get("cid"),
        level: session.get("level"),
        usrName: session.get("usrName"),
        isSub: session.get("level") == 1 ? 0 : session.get("isSub"),
      }
      add_user_logout_log(param)
    }
  }
  console.log("-channelService members after-", rid, channel.getMembers())
}

var add_user_logout_log = function (data) {
  //登出紀錄
  var login_data = {
    Cid: data.cid,
    UserName: data.usrName,
    Level: data.level,
    IsSub: data.isSub,
    LType: "OUT",
    LDesc: "logout",
    browser: data.browser || "",
    browser_version: data.browser_version || "",
    os: data.os || "",
    os_version: data.os_version || "",
    isMobile: data.isMobile || 0,
    isTablet: data.isTablet || 0,
    isDesktopDevice: data.isDesktopDevice || 0,
    IP: data.IP || "",
  }
  logDao.add_loginout_log(login_data, function (none, r_code, r_data) {
    console.log("user logout log", data.cid, data.usrName)
  })
}

//---------------------------------------------------------------

//管端登入 (http方式 取token)
handler.user_login = function (msg, session, next) {
  console.log("-user_login msg-", JSON.stringify(msg))

  var self = this
  var ip = ""
  if (typeof msg.remoteIP != "undefined" && msg.remoteIP.substr(0, 7) == "::ffff:") {
    ip = msg.remoteIP.substr(7)
  }

  var sessionService = self.app.get("sessionService")
  var user_level = ""
  var user = {}
  var hallId = 0
  var r_id = ""
  var jwt_token = ""
  var sys_config = self.app.get("sys_config")

  async.waterfall(
    [
      function (cb) {
        userDao.getUser_byUserName(msg.data, cb)
      },
      function (r_code, r_data, cb) {
        console.log("getUser_byUserName:", JSON.stringify(r_code), JSON.stringify(r_data))
        user = r_data
        if (r_code.code != code.OK) {
          next(null, {
            code: r_code.code,
            data: null,
          })
          return
        }
        if (r_data.OTPCode == null) {
          next(null, {
            code: code.USR.USER_NOT_EXIST,
            data: null,
          })
          return
        }
        var token = m_otplib.authenticator.generate(r_data.OTPCode) //產生 token
        if ((msg.data.token != token || msg.data.token != 999999) && otp_token_status) {
          next(null, {
            code: code.USR.USER_OTP_FAIL,
            data: null,
          }) //token 比對錯誤
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
          //凍結
          next(null, {
            code: code.USR.USER_FREEZE,
            data: null,
          })
          return
        }

        /*
            if (r_data.IsOnline) { //重複登入
                next(null, { code: code.USR.USER_LOGIN_DUPLICATE, data: null });
                return;
            }*/

        // user_level = getLevelName(r_data.Level);

        userDao.getLevelName_byLevel(r_data.Level, cb)
      },
      function (r_code, r_data, cb) {
        console.log("getLevelName_byLevel:", JSON.stringify(r_code), JSON.stringify(r_data))
        user_level = r_data

        if (user_level === "PR") {
          next(null, {
            code: code.USR.USER_NOT_EXIST,
            data: null,
          })
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
          next(null, {
            code: code.USR.UP_USER_DISABLE,
            data: null,
          })
          return
        }
        var ip_list = {
          UserLevel: 2,
          Cid: hallId,
          IpType: 1,
          Ip: ip,
          State: 1,
        }
        userDao.checkWhiteIp(ip_list, cb) // **白名單 IP**
      },
      function (r_code, r_data, cb) {
        //console.log('checkWhiteIp:',JSON.stringify(r_code),JSON.stringify(r_data)) ;
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

        // r_id = user.Cid + "*" + user.UserName;
        // session.bind(r_id);
        // session.set('cid', user.Cid);
        // session.set('usrName', user.UserName);
        // session.set('level', user.Level);
        // session.set('isSub', user.IsSub);
        // session.set('hallId', (user_level === "HA" && user.IsSub < 1) ? -1 : user.HallId);
        // session.pushAll();

        //判斷有無session
        uid = user.Cid + "*" + user.UserName
        //duplicate log in
        if (!!sessionService.getByUid(uid)) {
          console.log(" USER_LOGIN_DUPLICATE")
          next(null, {
            code: code.USR.USER_LOGIN_DUPLICATE,
            error: true,
          })
          return
        } else {
          console.log("no USER_LOGIN_DUPLICATE")
        }

        //產生JWT
        var jwt_info = {
          id: user.Cid,
          user_name: user.UserName,
          level: user.Level,
          isSub: user.IsSub,
          hallId: user_level === "HA" && user.IsSub < 1 ? -1 : user.HallId,
          agentId: user_level === "AG" && user.IsSub < 1 ? "-1" : user.Upid,
        }

        jwt_token = jwt.sign(jwt_info, sys_config.jwt_key, {
          expiresIn: "1h",
        })

        var param = {
          isAdmin: user.Level,
          userId: user.Cid,
          userName: user.UserName,
          token: jwt_token,
          frontendId: "",
        }
        userDao.modifyUserConnectState_v2(param, cb) //更新登入
      },
      function (r_code, r_data, cb) {
        console.log("modifyUserConnectState_v2:", JSON.stringify(r_code), JSON.stringify(r_data))
        if (r_code.code != code.OK) {
          next(null, {
            code: code.USR.USER_PARA_FAIL,
            data: null,
          })
          return
        }

        //登入紀錄
        var login_data = {
          Cid: user.Cid,
          UserName: user.UserName,
          Level: user.Level,
          IsSub: user.IsSub,
          LType: "IN",
          LDesc: "login",
          browser: msg.data.browser || "",
          browser_version: msg.data.browser_version || "",
          os: msg.data.os || "",
          os_version: msg.data.os_version || "",
          isMobile: msg.data.isMobile || 0,
          isTablet: msg.data.isTablet || 0,
          isDesktopDevice: msg.data.isDesktopDevice || 0,
          IP: msg.remoteIP,
        }
        logDao.add_loginout_log(login_data, cb)
      },
    ],
    function (none, r_code, r_data) {
      console.log("add_loginout_log:", JSON.stringify(r_code), JSON.stringify(r_data))
      if (r_code.code != code.OK) {
        next(null, {
          code: r_code,
          data: null,
        })
        return
      }

      /*
        var rid = uid;
        var channel = self.app.get('channelService').getChannel(rid, true);

        if (!!channel) {
            var members = channel.getMembers();
            if (members.indexOf(rid) == -1) {
                channel.add(rid, session.frontendId); //加入用戶
            }
        }
    */
      next(null, {
        code: code.OK,
        data: jwt_token,
      })
      return
    }
  )
}

//-----------------------------------------------------------------------------------------------------------------
//控端登入 (websocket login + token)
handler.admin_login_v1 = function (msg, session, next) {
  try {
    logger.info("-admin_login_v1 msg-", JSON.stringify(msg))

    var self = this
    var ip = ""
    if (typeof msg.remoteIP != "undefined" && msg.remoteIP.substr(0, 7) == "::ffff:") {
      ip = msg.remoteIP.substr(7)
    }

    var sessionService = self.app.get("sessionService")
    var channelService = self.app.get("channelService")
    var usrData = {}
    var self = this
    var uid = ""
    var data = {}
    var jwt_token = ""
    var sys_config = self.app.get("sys_config")
    var timezone = []

    async.waterfall(
      [
        function (cb) {
          adminDao.getOperator(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          data = r_data
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_NOT_EXIST,
              data: null,
            })
            return
          }
          if (data.Passwd != msg.data.password) {
            next(null, {
              code: code.USR.USER_PASSWORD_FAIL,
              data: null,
            })
            return
          }

          var token = m_otplib.authenticator.generate(data.OTPCode) //產生 token

          // token 不是空值且不是測試版才需檢查 OTP (otp_auth.state = 1)
          if (
            typeof msg.data.token != "undefined" &&
            msg.data.token != "" &&
            msg.data.token != token &&
            otp_token_status &&
            !msg.data.isDemo
          ) {
            logger.info(
              `[admin_login_v1] OTP 驗證失敗 , 輸入帳號=>${msg.data.name}, 驗證碼=>${msg.data.token}, 正確驗證碼=>${token}, ip=>${msg.remoteIP}`
            )
            next(null, {
              code: code.USR.USER_OTP_FAIL,
              data: null,
            }) //token 比對錯誤
            return
          }

          //停用
          if (data.State === "S") {
            next(null, {
              code: code.USR.USER_DISABLE,
              data: null,
            })
            return
          }
          //已刪除
          if (data.State === "D") {
            next(null, {
              code: code.USR.USER_NOT_EXIST,
              data: null,
            })
            return
          }

          var ip_list = {
            UserLevel: 1,
            Cid: data.AdminId,
            IpType: 1,
            Ip: ip,
            State: 1,
          }
          userDao.checkWhiteIp(ip_list, cb) // **白名單 IP**
        },
        function (r_code, r_data, cb) {
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
          //判斷有無session
          const uid = data.AdminId + "*" + data.UserName

          logger.info("[admin_login_v1][sessionService]", sessionService.getByUid(uid))
          //duplicate log in
          if (sessionService.getByUid(uid)) {
            //由session判斷 有無此用戶
            console.log("admin - 重複登入:", uid)
            next(null, {
              code: code.USR.USER_LOGIN_DUPLICATE,
              error: true,
            })
            return
          }

          usrData["AdminId"] = data.AdminId
          usrData["UserName"] = data.UserName
          usrData["OTPCode"] = data.OTPCode
          usrData["isOpenOTP"] = data.OTPState
          usrData["NickName"] = data.NickName
          usrData["firstLogin"] = data.FirstLogin
          usrData["Currency"] = sys_config.main_currency //系統主幣別
          usrData["TimeoutRangeSec"] = sys_config.timeout_range_sec

          console.log("admin - 無重複登入:", uid, session.frontendId)

          session.on("closed", onUserLeave_v2.bind(null, self.app)) //登出,斷線 後續要執行的一些動作

          session.bind(uid)
          session.set("cid", data.AdminId)
          session.set("usrName", data.UserName)
          session.set("level", 1) //admin
          session.set("agentId", "-1")
          session.set("timestamp", new Date().getTime())
          session.set("token_timestamp", new Date().getTime())
          session.set("browser", msg.data.browser)
          session.set("browser_version", msg.data.browser_version)
          session.set("os", msg.data.os)
          session.set("os_version", msg.data.os_version)
          session.set("isMobile", msg.data.isMobile)
          session.set("isTable", msg.data.isTablet)
          session.set("isDesktopDevice", msg.data.isDesktopDevice)
          session.set("IP", msg.remoteIP)
          session.pushAll()

          //加入channel
          var rid = "admin"
          var channel = channelService.getChannel(rid, true)
          if (!!channel) {
            var member = channel.getMember(uid)
            if (!member) {
              channel.add(uid, session.frontendId) //加入用戶
              console.log("admin - channel:", uid, session.frontendId)
            }
            console.log("channel mems:", channel.getMembers())
          }
          self.app.rpc.config.configRemote.getAuthority(session, { authTmpId: data.AuthorityTemplateID }, cb) //登入user的權限
        },
        function (r_code, r_authFunc, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.AUTH.AUTH_INVALID,
              data: null,
            })
            return
          }

          // //凍結user 權限調整
          // var auths = {};
          // if (data.State == 'F') {
          //     Object.keys(r_authFunc).forEach(key => {
          //         var auth = [];
          //         r_authFunc[key].forEach(item => {
          //             if (item.indexOf('view') > -1 || ['EditAccount', 'EditPassword'].indexOf(item) > -1) {
          //                 auth.push(item);
          //             }
          //         });
          //         if (auth.length > 0) {
          //             auths[key] = Object.assign([], auth);
          //         }
          //     });
          // } else {
          //     auths = Object.assign({}, r_authFunc);
          // }

          usrData["Authority"] = r_authFunc

          var user_set_param = {
            Cid: data.AdminId,
            IsAdmin: 1,
          }

          userDao.get_user_setting_byCid(user_set_param, cb) //取user_setting
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOAD_SETTING_FAIL,
              data: null,
            })
            return
          }
          usrData["countsOfPerPage"] = r_data.length > 0 ? r_data[0]["countsOfPerPage"] : "" //每頁筆數
          usrData["hourDiff"] = r_data.length > 0 ? r_data[0]["hourDiff"] : "" //與UTC 時差
          usrData["hourDiff_descE"] = r_data.length > 0 ? r_data[0]["hourDiff_descE"] : "" //與UTC 時差 - 英
          usrData["hourDiff_descG"] = r_data.length > 0 ? r_data[0]["hourDiff_descG"] : "" //與UTC 時差 - 繁中
          usrData["hourDiff_descC"] = r_data.length > 0 ? r_data[0]["hourDiff_descC"] : "" //與UTC 時差 - 簡中
          adminDao.setActionTime(usrData["AdminId"], cb) //紀錄執行時間
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.ACTION_TIME_FAIL,
              data: null,
            })
            return
          }
          self.app.rpc.config.configRemote.getTimezoneSet(session, cb) // 時差設定
        },
        function (r_code, r_timeZone, cb) {
          timezone = r_timeZone
          usrData["timezones"] = timezone

          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.ACTION_TIME_FAIL,
              data: null,
            })
            return
          }
          self.app.rpc.config.configRemote.getAuthType(session, cb) //各層級的權限
        },

        function (r_code, r_ahthType, cb) {
          var ahthType = []
          for (var i in r_ahthType) {
            ahthType.push({
              id: r_ahthType[i]["id"],
              type: r_ahthType[i]["type"],
              name: r_ahthType[i]["type"],
            })
          }
          usrData["AuthType"] = ahthType
          //----------------登入可查詢的 type -----------------
          var LoginLogType = Object.assign([], ahthType)
          LoginLogType.push({
            id: 4,
            type: "Player",
            name: "Player",
          })
          usrData["LoginLogType"] = LoginLogType
          //----------------登入可查詢的 type -----------------
          //產生JWT
          var jwt_info = {
            id: data.AdminId,
            user_name: data.UserName,
            level: 1,
            isSub: 0,
            hallId: 0,
            agentId: "-1",
            browser: msg.data.browser,
            browser_version: msg.data.browser_version,
            os: msg.data.os,
            os_version: msg.data.os_version,
            isMobile: msg.data.isMobile,
            isTable: msg.data.isTablet,
            isDesktopDevice: msg.data.isDesktopDevice,
            remoteIP: msg.remoteIP,
          }
          jwt_token = jwt.sign(jwt_info, sys_config.jwt_key, {
            expiresIn: "1h",
          })

          usrData["token"] = jwt_token //token認證

          var param = {
            isAdmin: 1,
            userId: data.AdminId,
            userName: data.UserName,
            token: jwt_token,
            frontendId: session.frontendId,
          }
          userDao.modifyUserConnectState_v2(param, cb) //更新登入token
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_PARA_FAIL,
              data: null,
            })
            return
          }

          //登入紀錄
          var login_data = {
            Cid: data.AdminId,
            UserName: data.UserName,
            Level: 1,
            IsSub: 0,
            LType: "IN",
            LDesc: "login",
            browser: msg.data.browser || "",
            browser_version: msg.data.browser_version || "",
            os: msg.data.os || "",
            os_version: msg.data.os_version || "",
            isMobile: msg.data.isMobile || 0,
            isTablet: msg.data.isTablet || 0,
            isDesktopDevice: msg.data.isDesktopDevice || 0,
            IP: msg.remoteIP,
          }

          logger.info("[admin_login_v1][login_data]", JSON.stringify(login_data))

          logDao.add_loginout_log(login_data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (none) {
          logger.error("[admin_login_v1][add_loginout_log] ", none)
          logger.error("[admin_login_v1][add_loginout_log]", JSON.stringify(none))
          console.trace(none)
        }

        if (r_code.code != code.OK) {
          next(null, {
            code: r_code,
            data: null,
          })
          return
        }

        usrData["version"] = require("../../../../config/version").version
        next(null, {
          code: code.OK,
          data: usrData,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[backendHandler][admin_login_v1] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
    return
  }
}

//判斷連線身分
handler.check_user_token_v2 = function (msg, session, next) {
  try {
    var self = this
    var sys_config = self.app.get("sys_config")
    var token = msg.token
    var sessionService = self.app.get("sessionService")
    var channelService = self.app.get("channelService")
    //判斷token
    //if (msg.conn == 1) session.on('closed', onUserLeave.bind(null, self.app));
    var token_timestamp = session.get("token_timestamp")
    var now_timestamp = new Date().getTime()
    var serverId = session.frontendId

    //30秒以上再token驗證

    if (
      token_timestamp == undefined ||
      (token_timestamp != undefined && now_timestamp - token_timestamp >= 30 * 1000)
    ) {
      console.log(
        "check_user_token_v2 session token_timestamp----------------------- ",
        now_timestamp - token_timestamp,
        token_timestamp,
        now_timestamp
      )
      var jwt_decoded = jwt.decode(token)
      console.log("------jwt_decoded------------", token, JSON.stringify(jwt_decoded))

      if (
        typeof jwt_decoded.id == "undefined" ||
        typeof jwt_decoded.user_name == "undefined" ||
        typeof jwt_decoded.level == "undefined"
      ) {
        next(null, {
          code: code.USR.TOKEN_FAIL, //token錯誤
        })
        return
      }

      var user = session.get("cid") + "*" + session.get("usrName")
      if (sessionService.getByUid(user)) {
        session.set("token_timestamp", new Date().getTime()) //更新時間
        session.pushAll()
      }

      var sid = self.app.get("serverId")
      async.waterfall(
        [
          function (cb) {
            jwt.verify(token, sys_config.jwt_key, function (err, decoded) {
              cb(null, err, decoded)
            })
          },
          function (r_err, r_decoded, cb) {
            if (r_err) {
              //----------------------------------------------
              //如果逾時
              //channel 有無此member 有->kick
              var uid = jwt_decoded.id + "*" + jwt_decoded.user_name
              var rid = jwt_decoded.level == 1 ? "admin" : "user"
              var channel = channelService.getChannel(rid, false)
              if (channel) {
                var user = channel.getMember(uid)
                console.log("channel mem-01", channel.getMembers())
                if (user) {
                  var uids = [
                    {
                      uid: uid,
                      sid: serverId,
                    },
                  ]
                  var msg = {
                    code: code.TIMEOUT,
                  }
                  channelService.pushMessageByUids("onKick", msg, uids, function (err) {
                    if (err) {
                      console.log("-pushMessageByUids err:", err)
                      return
                    }
                    console.log("-pushMessageByUids kick")
                    channel.leave(uid, serverId)
                    setTimeout(() => {
                      sessionService.kick(uid)
                      console.log("remove - sessionService", sessionService.getByUid(uid))
                    }, 1000)
                  })
                }
              }

              next(null, {
                code: code.TIMEOUT,
              })
              return
            } else {
              cb(null, { code: code.OK }, r_decoded) //token成功
            }
          },
          function (r_code, r_data, cb) {
            var param = {
              token: token,
            }
            userDao.getUserInfoByToken(param, cb)
          },
          function (r_code, r_data, cb) {
            if (r_code.code != code.OK || r_data.length == 0) {
              //超過時間->清除 或不正常方式連線
              next(null, {
                code: code.TIMEOUT,
              })
              return
            }

            //token帳號和資料表不同 -->錯誤
            if (jwt_decoded.id != r_data[0]["userId"] || jwt_decoded.user_name != r_data[0]["userName"]) {
              console.log("---------------------token帳號和資料表不同 -->錯誤?----------------------------")
              next(null, {
                code: code.USR.TOKEN_FAIL,
              })
              return
            }

            var uid = jwt_decoded.id + "*" + jwt_decoded.user_name
            var rid = jwt_decoded.level == 1 ? "admin" : "user"

            var param = {
              cid: jwt_decoded.id,
              usrName: jwt_decoded.user_name,
              level: jwt_decoded.level,
              uid: uid,
            }

            // 用於頁面 reload 時, 會找不到 session, 幫它重建資料
            // 為了作開新分頁功能, 同一個 uid 會綁定到多個 session, 此處判斷已不適用所以拿掉
            //找不到此session
            // if (!sessionService.getByUid(uid)) {
            console.log("用token 做session 綁定 : ", uid)
            session.on("closed", onUserLeave_v2.bind(null, self.app)) //session關閉時 登出
            session.bind(uid)
            session.set("cid", jwt_decoded.id)
            session.set("usrName", jwt_decoded.user_name)
            session.set("level", jwt_decoded.level) //----------- 再修
            session.set("isSub", jwt_decoded.isSub)
            session.set("hallId", jwt_decoded.hallId)
            session.set("agentId", jwt_decoded.agentId)
            session.set("timestamp", new Date().getTime())
            session.set("token_timestamp", new Date().getTime())
            session.set("browser", jwt_decoded.browser)
            session.set("browser_version", jwt_decoded.browser_version)
            session.set("os", jwt_decoded.os)
            session.set("os_version", jwt_decoded.os_version)
            session.set("isMobile", jwt_decoded.isMobile)
            session.set("isTable", jwt_decoded.isTablet)
            session.set("isDesktopDevice", jwt_decoded.isDesktopDevice)
            session.set("IP", jwt_decoded.remoteIP)
            session.pushAll()
            // }

            var channel = channelService.getChannel(rid, true)
            if (!!channel) {
              var member = channel.getMember(uid)
              if (!member) {
                channel.add(uid, session.frontendId) //加入用戶
              }
              console.log("channel mems:", channel.getMembers())
            }
            //更新user 前端serverID
            var param = {
              token: token,
              frontendId: session.frontendId,
            }
            //更新user connect
            userDao.modifyUserInfoByToken(param, cb)
          },
        ],
        function (node, r_code, r_data) {
          next(null, {
            code: code.OK,
          })
          return
        }
      )
    } else {
      next(null, {
        code: code.OK,
      })
      return
    }
  } catch (err) {
    logger.error("[backendHandler][check_user_token_v2] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//管端登入 (http方式 取token)
handler.user_login_v1 = function (msg, session, next) {
  try {
    logger.info("-user_login_v1 msg-", JSON.stringify(msg))
    var self = this
    var ip = ""
    if (typeof msg.remoteIP != "undefined" && msg.remoteIP.substr(0, 7) == "::ffff:") {
      ip = msg.remoteIP.substr(7)
    }

    var sessionService = self.app.get("sessionService")
    var user_level = ""
    var user = {}
    var userState = {}
    var hallId = 0
    var r_id = ""
    var jwt_token = ""
    var sys_config = self.app.get("sys_config")
    var usrData = {}
    var checkState = false
    async.waterfall(
      [
        function (cb) {
          userDao.getUser_byUserName(msg.data, cb)
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
          if (r_data.OTPCode == null) {
            next(null, {
              code: code.USR.USER_NOT_EXIST,
              data: null,
            })
            return
          }
          var token = m_otplib.authenticator.generate(r_data.OTPCode) //產生 token
          // token 不是空值且不是測試版才需檢查 OTP (otp_auth.state = 1)
          if (
            typeof msg.data.token != "undefined" &&
            msg.data.token != "" &&
            msg.data.token != token &&
            otp_token_status &&
            !msg.data.isDemo
          ) {
            logger.info(
              `[user_login_v1] OTP 驗證失敗 , 輸入帳號=>${msg.data.name}, 驗證碼=>${msg.data.token}, 正確驗證碼=>${token}, ip=>${msg.remoteIP}`
            )
            next(null, {
              code: code.USR.USER_OTP_FAIL,
              data: null,
            }) //token 比對錯誤
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
            //凍結
            next(null, {
              code: code.USR.USER_FREEZE,
              data: null,
            })
            return
          }

          /*
                if (r_data.IsOnline) { //重複登入
                    next(null, { code: code.USR.USER_LOGIN_DUPLICATE, data: null });
                    return;
                }*/

          // user_level = getLevelName(r_data.Level);

          // 檢查上線狀態
          if (r_data.Upid == -1 && r_data.HallId == -1) {
            cb(null, { code: code.OK }, null)
          } else {
            var data = {
              userId: user.Cid,
              tableName: "wallet",
            }
            userDao.getUpIdByInfiniteClass(data, cb)
          }
          // userDao.getLevelName_byLevel(r_data.Level,cb);
        },
        function (r_code, r_data, cb) {
          userState = r_data

          if (userState != null) {
            userState.forEach((item) => {
              if (item.State == "S") {
                checkState = true
                return
              }
            })
          }

          userDao.getLevelName_byLevel(user.Level, cb)
        },
        function (r_code, r_data, cb) {
          console.log("getLevelName_byLevel:", JSON.stringify(r_code), JSON.stringify(r_data))
          user_level = r_data

          if (user_level === "PR") {
            next(null, {
              code: code.USR.USER_NOT_EXIST,
              data: null,
            })
            return
          }

          //-----------------------IP----------------------

          var check_upUser_State = 0
          if (user_level === "HA") {
            //hall
            // hallId = (user.IsSub == true) ? user.Upid : user.Cid;
            // if (user.IsSub == true) {
            //     check_upUser_State = 1;
            // } //subhall

            hallId = user.IsSub == 1 ? user.Upid : user.Cid
            check_upUser_State = 1
          }

          if (user_level === "AG") {
            //agent
            hallId = user.HallId
            check_upUser_State = 1
          }
          if (check_upUser_State === 1 && userState != null) {
            //找上層USER 狀態

            if (checkState) {
              next(null, {
                code: code.USR.UP_USER_DISABLE,
                data: null,
              })
              return
            }
          }
          var ip_list = {
            UserLevel: 2,
            Cid: hallId,
            IpType: 1,
            Ip: ip,
            State: 1,
          }
          userDao.checkWhiteIp(ip_list, cb) // **白名單 IP**
        },
        function (r_code, r_data, cb) {
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

          usrData = {
            Cid: user.Cid,
            Hallid: user_level === "HA" && user.IsSub < 1 ? -1 : user.HallId,
            Upid: user.Upid,
            RealName: user.RealName,
            UserName: user.UserName,
            NickName: user.NickName,
            Birthday: user.Birthday,
            Address: user.Address,
            Email: user.Email,
            IsSub: user.IsSub ? true : false,
            IsDemo: user_level === "HA" ? 0 : user.IsDemo,
            Level: user_level,
            State: user.State,
            Currency: user.Currency,
            //Currency: user_level === "HA" ? user.Currencies : user.Currency,
            LastLoginDate: Date.LastLoginDate,
            firstLogin: user.FirstLogin,
            isOpenOTP: user.isOTPState == 0 ? false : true, // 0: 停用(false) 1: 啟用(true)
            OTPCode: user.OTPCode,
            AuthorityTemplateID: user.AuthorityTemplateId,
            dc: user.DC,
          }
          self.app.rpc.config.configRemote.getAuthority(
            session,
            {
              authTmpId: user.AuthorityTemplateId,
            },
            cb
          )
        },
        function (r_code, r_authFunc, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.AUTH_FAIL,
              data: null,
            })
            return
          }
          //凍結user 權限調整
          var auths = {}
          if (user.State == "F") {
            Object.keys(r_authFunc).forEach((key) => {
              var auth = []
              r_authFunc[key].forEach((item) => {
                var auth = []
                if (item.indexOf("view") > -1 || ["EditAccount", "EditPassword"].indexOf(item) > -1) {
                  auth.push(item)
                }
              })
              if (auth.length > 0) {
                auths[key] = Object.assign([], auth)
              }
            })
          } else {
            auths = Object.assign({}, r_authFunc)
          }

          usrData["Authority"] = auths //user權限

          self.app.rpc.config.configRemote.getAuthType(session, cb)
        },
        function (r_code, r_ahthType, cb) {
          var ahthType = []
          for (var i in r_ahthType) {
            var name = r_ahthType[i]["id"] == 1 ? "Admin" : r_ahthType[i]["type"]

            ahthType.push({
              id: r_ahthType[i]["id"],
              type: r_ahthType[i]["type"],
              name: name,
            })
          }

          if (user_level === "HA") {
            usrData["AuthType"] = Object.assign(
              [],
              ahthType.filter((item) => ["Reseller", "Agent"].indexOf(item.type) > -1)
            )
          }

          //----------------登入可查詢的 type -----------------
          var LoginLogType = []

          if (user_level === "HA") {
            LoginLogType = Object.assign([], usrData["AuthType"])
          }
          if (user_level === "AG") {
            LoginLogType = Object.assign(
              [],
              ahthType.filter((item) => ["Agent"].indexOf(item.type) > -1)
            )
          }

          LoginLogType.push({
            id: 4,
            type: "Player",
            name: "Player",
          })

          usrData["LoginLogType"] = Object.assign([], LoginLogType)
          //     //----------------登入可查詢的 type -----------------
          //     self.app.rpc.config.configRemote.getSysLimitTime(session, cb); //系統設定時間

          // },
          // function (r_code, r_time, cb) {
          //     if (r_code.code != code.OK) {
          //         next(null, {
          //             code: code.CONFIG.LOAD_LIMIT_TIME_FAIL,
          //             data: null
          //         });
          //         return;
          //     }

          usrData["TimeoutRangeSec"] == sys_config.timeout_range_sec //登出時間-秒數
          self.app.rpc.config.configRemote.getSysSearchDays(session, cb) //系統天數搜尋
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.REARCH_DATE_LIST_FAIL,
              data: null,
            })
            return
          }
          usrData["SysSearchDays"] = r_data

          var user_set_param = {
            Cid: user.Cid,
            IsAdmin: 0,
          }
          userDao.get_user_setting_byCid(user_set_param, cb) //取user_setting
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.LOAD_SETTING_FAIL,
              data: null,
            })
            return
          }

          usrData["countsOfPerPage"] = r_data.length > 0 ? r_data[0]["countsOfPerPage"] : "" //每頁筆數
          usrData["hourDiff"] = r_data.length > 0 ? r_data[0]["hourDiff"] : "" //與UTC 時差
          usrData["hourDiff_descE"] = r_data.length > 0 ? r_data[0]["hourDiff_descE"] : "" //與UTC 時差 - 英
          usrData["hourDiff_descG"] = r_data.length > 0 ? r_data[0]["hourDiff_descG"] : "" //與UTC 時差 - 繁中
          usrData["hourDiff_descC"] = r_data.length > 0 ? r_data[0]["hourDiff_descC"] : "" //與UTC 時差 - 簡中

          userDao.setActionTime(usrData["Cid"], cb) //紀錄執行時間
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.ACTION_TIME_FAIL,
              data: null,
            })
            return
          }
          self.app.rpc.config.configRemote.getTimezoneSet(session, cb) // 時差設定
        },
        function (r_code, r_timeZone, cb) {
          timezone = r_timeZone
          usrData["timezones"] = timezone

          if (r_code.code != code.OK) {
            next(null, { code: code.USR.ACTION_TIME_FAIL, data: null })
            return
          }

          //判斷有無session
          const uid = user.Cid + "*" + user.UserName
          logger.info("[user_login_v1][sessionService]", sessionService.getByUid(uid))
          //duplicate log in
          if (!!sessionService.getByUid(uid)) {
            console.log("user - 重複登入:", uid)
            next(null, {
              code: code.USR.USER_LOGIN_DUPLICATE,
              error: true,
            })
            return
          }

          //產生JWT
          var jwt_info = {
            id: user.Cid,
            user_name: user.UserName,
            level: user.Level,
            isSub: user.IsSub,
            hallId: user_level === "HA" && user.IsSub < 1 ? -1 : user.HallId,
            agentId: user_level === "AG" && user.IsSub < 1 ? "-1" : user.Upid,
            browser: msg.data.browser,
            browser_version: msg.data.browser_version,
            os: msg.data.os,
            os_version: msg.data.os_version,
            isMobile: msg.data.isMobile,
            isTable: msg.data.isTablet,
            isDesktopDevice: msg.data.isDesktopDevice,
            remoteIP: msg.remoteIP,
          }

          jwt_token = jwt.sign(jwt_info, sys_config.jwt_key, {
            expiresIn: "1h",
          })
          usrData["token"] = jwt_token
          var param = {
            isAdmin: user.Level,
            userId: user.Cid,
            userName: user.UserName,
            token: jwt_token,
            frontendId: session.frontendId,
          }
          userDao.modifyUserConnectState_v2(param, cb) //更新登入
        },
        function (r_code, r_data, cb) {
          console.log("modifyUserConnectState_v2:", JSON.stringify(r_code), JSON.stringify(r_data))
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.USER_PARA_FAIL,
              data: null,
            })
            return
          }

          //登入紀錄
          var login_data = {
            Cid: user.Cid,
            UserName: user.UserName,
            Level: user.Level,
            IsSub: user.IsSub,
            LType: "IN",
            LDesc: "login",
            browser: msg.data.browser || "",
            browser_version: msg.data.browser_version || "",
            os: msg.data.os || "",
            os_version: msg.data.os_version || "",
            isMobile: msg.data.isMobile || 0,
            isTablet: msg.data.isTablet || 0,
            isDesktopDevice: msg.data.isDesktopDevice || 0,
            IP: msg.remoteIP,
          }
          logDao.add_loginout_log(login_data, cb)
        },
      ],
      function (none, r_code, r_data) {
        console.log("add_loginout_log:", JSON.stringify(r_code), JSON.stringify(r_data))
        if (r_code.code != code.OK) {
          next(null, {
            code: r_code,
            data: null,
          })
          return
        }

        /*
            var rid = uid;
            var channel = self.app.get('channelService').getChannel(rid, true);

            if (!!channel) {
                var members = channel.getMembers();
                if (members.indexOf(rid) == -1) {
                    channel.add(rid, session.frontendId); //加入用戶
                }
            }
        */
        usrData["version"] = require("../../../../config/version").version
        next(null, {
          code: code.OK,
          data: usrData,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[backendHandler][user_login_v1] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

var onUserLeave_v2 = function (app, session) {
  console.log("------------------v2 onUserLeave app---------------", app.get("serverId"))
  if (!session || !session.uid) {
    return
  }
  console.log("session level-", session.get("level"), "serverId-" + app.get("serverId"))

  var rid = session.get("level") == 1 ? "admin" : "user"

  console.log("-leave session uid-", session.uid, "rid-" + rid)

  var channelService = app.get("channelService")
  var sessionService = app.get("sessionService")
  var channel = channelService.getChannel(rid, false)

  console.log("-channelService members before-", rid, channel.getMembers())
  var channel = channelService.getChannel(rid, false)
  if (!!channel) {
    if (channel.getMember(session.uid)) {
      channel.leave(session.uid, app.get("serverId"))
      var param = {
        cid: session.get("cid"),
        level: session.get("level"),
        usrName: session.get("usrName"),
        isSub: session.get("level") == 1 ? 0 : session.get("isSub"),
        browser: session.get("browser"),
        browser_version: session.get("browser_version"),
        os: session.get("os"),
        os_version: session.get("os_version"),
        isMobile: session.get("isMobile"),
        isTablet: session.get("isTablet"),
        isDesktopDevice: session.get("isDesktopDevice"),
        IP: session.get("IP"),
      }
      add_user_logout_log(param)
    }
  }
  console.log("-channelService members after-", rid, channel.getMembers())
}

// 檢查 OTP 是否有重設過
handler.checkOTPCode = function (msg, session, next) {
  async.waterfall(
    [
      function (cb) {
        switch (msg.data["type"]) {
          case "Admin":
            msg.data["sel_table"] = "admin"
            break
          case "Agent":
            msg.data["sel_table"] = "customer"
            break
        }

        userDao.check_OtpCode(msg.data, cb)
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code !== code.OK) {
        next(null, {
          code: r_code,
          data: null,
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

// 更換 token 以延時
handler.prolong_token = function (msg, session, next) {
  try {
    // console.log('-prolong_token msg-', JSON.stringify(msg));

    const expireHours = 60 * 60 // 延時秒數

    let jwt_token = msg.token
    let jwt_decoded = {}
    let sysConfig = this.app.get("sys_config")
    let respData = {}
    let now = Date.now()

    async.waterfall(
      [
        function (done) {
          // 建立新 token
          jwt_decoded = jwt.decode(jwt_token)
          // console.log('------jwt_decoded------------', jwt_token, JSON.stringify(jwt_decoded));

          jwt_decoded.exp = Math.floor(now / 1000) + expireHours
          // console.log('------jwt_decoded updated------------', jwt_token, JSON.stringify(jwt_decoded));

          jwt_token = jwt.sign(jwt_decoded, sysConfig.jwt_key)
          // console.log('------new jwt_decoded------------', jwt_token, JSON.stringify(jwt.decode(jwt_token)));

          respData["token"] = jwt_token

          done(null)
        },
        function (done) {
          // DB 使用者連線狀態新加 token
          // 由於開新分頁時, 原本的頁面若換 token, 而只修改 user_connection 的 token, 新頁面將會找不到資料
          let params = {
            frontendId: session.frontendId,
            isAdmin: jwt_decoded.level,
            userId: jwt_decoded.id,
            userName: jwt_decoded.user_name,
            token: respData.token,
          }
          userDao.modifyUserConnectState_v2(params, done)
        },
        function (r_code, r_data, done) {
          // 更新 session timestamp
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          session.set("timestamp", now)
          session.pushAll()

          done(null)
        },
      ],
      function (err) {
        if (err) {
          logger.error("[backendHandler][prolong_token] err: ", JSON.stringify(err))
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
    logger.error("[backendHandler][prolong_token] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
// 線上玩家頁面初始資料
handler.onlinePlayerPageInit = function (msg, session, next) {
  try {
    var self = this

    let levelName = ""
    let respData = {}

    async.waterfall(
      [
        function (done) {
          // 取得 level 名稱
          userDao.getLevelName_byLevel(session.get("level"), done)
        },
        function (r_code, r_data, done) {
          // 取得遊戲種類
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          levelName = r_data

          self.app.rpc.config.configRemote.getGameGroup(session, done)
        },
        function (r_code, r_data, done) {
          //取得遊戲
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          respData["gameGroups"] = r_data

          let reqData = {
            level: levelName,
            upid: "-1",
          }
          let isSub = session.get("isSub")
          if (levelName == "AD") {
            reqData.cid = session.get("cid")
          } else if (levelName == "HA") {
            reqData.cid = isSub == 1 ? session.get("hallId") : session.get("cid")
          } else if (levelName == "AG") {
            reqData.cid = isSub == 1 ? session.get("agentId") : session.get("cid")
            reqData.upid = session.get("hallId")
          }
          self.app.rpc.game.gameRemote.getUserJoinGames(session, reqData, done)
        },
        function (r_code, r_data, done) {
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          respData["games"] = r_data

          done(null)
        },
      ],
      function (err) {
        if (err) {
          logger.error("[backendHandler][onlinePlayerPageInit] err: ", JSON.stringify(err))
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
    logger.error("[backendHandler][onlinePlayerPageInit] err: ", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
// 取得線上玩家列表
// msg.data: {userName, ggId, gameId, accTypeIds, pageCur, pageCount, lang}
handler.onlinePlayerGetList = function (msg, session, next) {
  try {
    var self = this

    const maxPageCount = 1000
    const onlinePlayerNums = self.app.localCache.OnlinePlayerNums // (cid -> {upid, (gameId -> {ggId, [playerId]})})

    let level = session.get("level")
    let myCid = "-1" // 查詢 onlinePlayerNums 用的 cid; AD 用 '-1'
    let playerMap = {} // (playerId -> (gameId -> true)); 考慮之後多遊戲
    // 是否要回傳試玩帳號
    let requestDemo = false
    if (session.get("level") == 1) {
      // AD 才可看到試玩帳號
      if (msg.data.accTypeIds.length === 0) {
        requestDemo = true
      } else if (msg.data.accTypeIds.indexOf(consts.DemoType.demo) !== -1) {
        requestDemo = true
      }
    }

    // 判斷參數
    if (msg.data.pageCur <= 0 || msg.data.pageCount <= 0) {
      next(null, {
        code: code.BET.PARA_DATA_FAIL,
      })
      return
    }
    msg.data.pageCount = Math.min(msg.data.pageCount, maxPageCount)

    async.waterfall(
      [
        function (done) {
          // 由 DB 撈出所有類似 userName 且滿足 level 的 cid 列表
          let isSub = session.get("isSub")
          if (level == 2) {
            // HA
            myCid = isSub == 1 ? session.get("hallId") : session.get("cid")
          } else if (level == 3) {
            // AG
            myCid = isSub == 1 ? session.get("agentId") : session.get("cid")
          }

          // AG 忽略 userName
          if (msg.data.userName.length == 0 || level == 3) {
            done(null, code.OK, [myCid]) // 用自己去撈其下所有在線玩家就好
            return
          }

          // AD 撈 HA, HA 撈 AG
          let param = {
            IsAg: 0,
            userName: msg.data.userName,
          }
          if (level == 1) {
            param.IsAg = 2
          } else if (level == 2) {
            param.IsAg = 3
          }
          userDao.getUserIds_byName(param, (none, r_code, r_data) => {
            if (r_code.code == code.OK) {
              // r_data: [cid]
              r_data = r_data.map((item) => item.Cid)
            }
            done(null, r_code, r_data)
          }) // 子帳號一樣撈出, 最後查詢人數時找不到資料, 會自然過濾掉
        },
        function (r_code, r_data, done) {
          // 篩選只留下所有在我下屬 (包含我) 的 cid 列表
          if (typeof r_code === "object") {
            if (r_code.code != code.OK) {
              done(r_code)
              return
            }
          } else if (r_code != code.OK) {
            done(r_code)
            return
          }

          // r_data: [cid]

          // cids: [cid]
          let cids = r_data
          // AD 不篩選
          if (level != 1) {
            cids = r_data.filter((cid) => {
              let loopCnt = 0
              let nowCId = cid
              while (nowCId && nowCId != "-1") {
                if (nowCId == myCid) {
                  // 往上層找到我則滿足
                  return true
                }
                nowCId = onlinePlayerNums[nowCId] ? onlinePlayerNums[nowCId].upid : "-1"

                loopCnt++
                if (loopCnt >= 1000) {
                  return false
                }
              }
              return false
            })
          }

          done(null, cids)
        },
        function (r_data, done) {
          // 輪巡 cid 列表, 取得滿足遊戲的玩家列表
          // r_data: [cid]

          for (let cid of r_data) {
            let num = onlinePlayerNums[cid]
            if (!num) {
              continue
            }

            // num.games: (gameId -> {ggId, [playerId]})
            for (let gameId in num.games) {
              if (msg.data.ggId != -1 && msg.data.ggId != num.games[gameId].ggId) {
                continue
              }
              if (msg.data.gameId != -1 && msg.data.gameId != gameId) {
                continue
              }
              for (let playerId of num.games[gameId].playerIds) {
                // 不記錄重複的資料
                if (playerMap[playerId] && playerMap[playerId][gameId]) {
                  continue
                }

                playerMap[playerId] = playerMap[playerId] || {}
                playerMap[playerId][gameId] = true
              }
            }
          }

          done(null)
        },
        function (done) {
          // 由 DB 撈出滿足帳號類別的該頁玩家資料
          let players = []
          for (let playerId in playerMap) {
            for (let gameId in playerMap[playerId]) {
              players.push({ playerId, gameId: parseInt(gameId) })
            }
          }
          //#region players 測試資料
          // players = [
          //     { playerId: 'Z1QC2NO0iw6x', gameId: 10003 }, // 重複 cid
          //     { playerId: 'Z1QC2NO0iw6x', gameId: 10004 }, // 重複 cid
          //     { playerId: '2JF4qfaVXgqxdQT3VbgkKG', gameId: 10005 }, // 測試
          //     { playerId: '27AzkiRiREZrxtpYBMxyKv', gameId: 10005 }, // 測試
          //     { playerId: '22V9XtHaGCoGXBWJHvnrhK', gameId: 10002 },
          //     { playerId: '22r9o8mpHsiLgNeUVzBtae', gameId: 10002 },
          //     { playerId: 'DEMO_00001', gameId: 10001 },
          //     { playerId: 'DEMO_00001', gameId: 10004 },
          //     { playerId: 'DEMO_00002', gameId: 10002 },
          //     { playerId: '22PuQKPKHhDoii8RribeGv', gameId: 10004 },
          //     { playerId: '22oV6zUjBcgdey3qRo1fdu', gameId: 10002 },
          //     { playerId: '22LwrHwXxbusDSiXvEqamW', gameId: 10001 },
          //     { playerId: '22JiTZ81RXAdrtYhYstazE', gameId: 10005 },
          //     { playerId: '22eeH7tpdtrUerfMgzxyfH', gameId: 10005 },
          //     { playerId: '22617P4KbxFhdnaax1bM3B', gameId: 10002 },
          //     { playerId: '21nTC7MuH3Gi2u4YvRrFAn', gameId: 10003 }
          // ];
          //#endregion
          let param = {
            players: players,
            accTypeIds: msg.data.accTypeIds,
            pageCur: msg.data.pageCur,
            pageCount: msg.data.pageCount,
            sortKey: msg.data.sortKey,
            sortType: msg.data.sortType,
            lang: msg.data.lang,
            requestDemo,
            hallName: "",
            agentName: "",
          }
          // AD 撈 HA, HA 撈 AG
          if (level == 1) {
            param.hallName = msg.data.upName
          } else if (level == 2) {
            param.agentName = msg.data.upName
          }
          userDao.getOnlinePlayerList(param, done)
        },
        function (r_code, r_data, done) {
          // r_data: {count, list: [PlayerId, Cid, UserName, State, IsDemo, Upid, HallId, Quota, GameName, GameCurrency]}
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // resp: {count, [cid, userName, state, isDemo, upid, hallId, quota, gameCurrency, gameName]}
          let resp = { count: r_data.count, players: [] }
          for (let row of r_data.list) {
            resp.players.push({
              cid: row.Cid || row.PlayerId,
              userName: row.UserName,
              state: row.State,
              isDemo: row.IsDemo !== null ? row.IsDemo : consts.DemoType.demo,
              upid: row.Upid,
              hallId: row.HallId,
              quota: row.Quota,
              gameCurrency: row.GameCurrency,
              gameName: row.GameName,
              hallName: row.HallName,
              agentName: row.AgentName,
            })
          }

          done(null, resp)
        },
      ],
      function (err, r_data) {
        if (err) {
          logger.error("[backendHandler][onlinePlayerGetList] err: ", JSON.stringify(err))
          next(null, {
            code: code.FAIL,
            data: null,
          })
          return
        }

        // r_data: {count, {cid, userName, state, isDemo, upid, hallId, quota, gameCurrency, gameId}}

        next(null, {
          code: code.OK,
          data: {
            pageCur: msg.data.pageCur,
            count: r_data.count,
            list: r_data.players,
          },
        })
      }
    )
  } catch (err) {
    logger.error("[backendHandler][onlinePlayerGetList] err: ", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
