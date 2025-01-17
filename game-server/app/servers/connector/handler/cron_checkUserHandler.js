//var memberSchemas     = require('../../db/schemas/memberSchemas');
//var mongoose          = require('mongoose');
//var memberModule      = mongoose.model('members');

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
var otp_token_status = false //判斷OTP狀態
var onlinePlayer = []
var pomelo = require("pomelo")
var gameServer = conf.GAME_SERVER
var uploadImageUrl = conf.UPLOAD_IMAGE_URL

var ip_check_status = false //暫關白名單確認
var api_token_status = false //暫關_api_token

module.exports = function (app) {
  return new Handler(app)
}

var Handler = function (app) {
  this.app = app
}

var proto = Handler.prototype

/**
 * 進入遊戲時檢查auth id 碼.
 *
 * @param  {Object}   msg     request message
 * @param  {Object}   session current session object
 * @param  {Function} next    next stemp callback
 * @return {Void}
 */

//---------------------------------------------------------------
//閒置時間逾時-> 踢出
var schedule = require("pomelo-schedule")
proto.checkSession = function (app) {
  this.app = app
  self = this

  var sessionService = app.get("sessionService")
  var channelService = app.get("channelService")

  var sys_config = self.app.get("sys_config")
  var now_timestamp = new Date().getTime()

  sessionService.forEachBindedSession(function (session) {
    var last_chkUser_timestamp = session.get("chkUser_timestamp") //上次執行時間
    session.set("chkUser_timestamp", now_timestamp) //更新執行時間

    var serverId = session.frontendId
    var sid = session.id //session id
    var uid = session.uid

    console.log("serverId:" + serverId, "sid:" + sid, "uid:" + uid, "timestamp:" + session.get("timestamp"))
    var now = new Date().getTime()

    if (
      last_chkUser_timestamp == undefined ||
      (last_chkUser_timestamp != undefined && now_timestamp - last_chkUser_timestamp >= 10)
    ) {
      if (now - session.get("timestamp") > sys_config.timeout_range_sec * 1000) {
        //逾時,斷線

        var rid = session.get("level") == 1 ? "admin" : "user"
        var channel = channelService.getChannel(rid, false)

        if (!!channel) {
          var user = channel.getMember(uid)
          console.log("channel mem-", channel.getMembers())

          if (!!user) {
            var route = "onKick"
            var msg = {
              code: code.TIMEOUT,
            }
            var uids = [
              {
                uid: uid,
                sid: serverId,
              },
            ]
            channelService.pushMessageByUids(route, msg, uids, function (err) {
              if (err) {
                console.log("-pushMessageByUids err:", err)
                return
              }
              console.log("-pushMessageByUids kick")
              //  channel.leave(uid, sid);
              //  console.log('channel mem-02', channel.getMembers());
              setTimeout(() => {
                sessionService.kick(uid)
              }, 1000)
            })
          }
        }
      }
    }
  })
  //  console.log('chk count session ',app.get('serverId'), sessionService.getSessionsCount());
}

//if (pomelo.app.get('serverId') == 'connector-server-1') {

// if(conf.CRON==1){
//     schedule.scheduleJob(
//         '0/30 * * * * *',
//         checkSession, pomelo.app
//     );
// }
//}
//---------------------------------------------------------------------------
