var logger = require("pomelo-logger").getLogger("backendRemote", __filename)
var code = require("../../../util/code")

var Remote = function (app) {
  this.app = app
}

module.exports = function (app) {
  return new Remote(app)
}

var proto = Remote.prototype

proto.onRevenue = function (cb) {
  // 通知前端可以取儀錶板資料
  let uidSet = {} // (uid -> (sid -> true))
  let sessionService = this.app.get("sessionService")
  let channelService = this.app.get("channelService")
  sessionService.forEachBindedSession(function (session) {
    let uid = session.uid
    let sid = session.frontendId

    // 不重複發送
    uidSet[uid] = uidSet[uid] || {}
    if (uidSet[uid][sid]) {
      return
    }
    uidSet[uid][sid] = true

    let msg = {
      code: code.OK,
      data: {},
    }

    channelService.pushMessageByUids("notifyRevenue", msg, [{ uid, sid }], function (err) {
      if (err) {
        logger.error("[backendRemote][onRevenue] pushMessageByUids err: ", JSON.stringify(err))
        return
      }
    })
  })

  cb()
}
