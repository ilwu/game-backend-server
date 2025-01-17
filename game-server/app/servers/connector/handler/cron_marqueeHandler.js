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

var pomelo = require("pomelo")

module.exports = function (app) {
  return new Handler(app)
}

var Handler = function (app) {
  this.app = app
}

var proto = Handler.prototype
//跑馬燈訊息 (管端)
var schedule = require("pomelo-schedule")

proto.userMarquee = function (app) {
  this.app = app
  self = this

  let uidSet = {} // (uid -> (sid -> true))
  var sessionService = app.get("sessionService")
  var channelService = app.get("channelService")

  var now_timestamp = new Date().getTime()

  sessionService.forEachBindedSession(function (session) {
    var last_onMarquee_timestamp = session.get("onMarquee_timestamp") //上次執行時間
    session.set("onMarquee_timestamp", now_timestamp) //更新執行時間

    let uid = session.uid
    let sid = session.frontendId

    // 不重複發送
    uidSet[uid] = uidSet[uid] || {}
    if (uidSet[uid][sid]) {
      return
    }
    uidSet[uid][sid] = true

    var rid = session.get("level") == 1 ? "admin" : "user"
    var channel = channelService.getChannel(rid, false)

    if (!!channel) {
      var user = channel.getMember(uid)
      let userId = ""

      if (
        !!user &&
        (last_onMarquee_timestamp == undefined ||
          (last_onMarquee_timestamp != undefined && now_timestamp - last_onMarquee_timestamp >= 10))
      ) {
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

        userDao.getUserMarquee(param, function (none, r_code, r_data) {
          var route = "onMessage"
          var msg = {
            code: code.OK,
            data: {
              msg: r_data,
            },
          }

          //打回前端訊息
          channelService.pushMessageByUids(route, msg, [{ uid, sid }], function (err) {
            if (err) {
              console.log("-pushMessageByUids:", err)
              return
            }
          })
        })
      }
    }
  })
}
// if(conf.CRON==1){
//     schedule.scheduleJob(
//         '0 0 0/30 * * *',
//         userMarquee, pomelo.app
//     );
// }

//---------------------------------------------------------------------------
