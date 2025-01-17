var exp = module.exports
var dispatcher = require("./dispatcher")

exp.chat = function (session, msg, app, cb) {
  var chatServers = app.getServersByType("chat")

  if (!chatServers || chatServers.length === 0) {
    cb(new Error("can not find chat servers."))
    return
  }

  var res = dispatcher.dispatch(session.get("rid"), chatServers)

  cb(null, res.id)
}

exp.auth = function (session, msg, app, cb) {
  var authServer = app.getServersByType("auth")

  if (!authServer || authServer.length === 0) {
    cb(new Error("can not find auth servers."))
    return
  }

  var res = dispatcher.dispatch(session.get("rid"), authServer)

  cb(null, res.id)
}

exp.admin = function (session, msg, app, cb) {
  var adminServer = app.getServersByType("admin")

  if (!adminServer || adminServer.length === 0) {
    cb(new Error("can not find admin servers."))
    return
  }

  console.log("--------dispatch-----------:" + session.get("rid"))

  var res = dispatcher.dispatch(session.get("rid"), adminServer)

  cb(null, res.id)
}

exp.user = function (session, msg, app, cb) {
  var userServer = app.getServersByType("user")

  if (!userServer || userServer.length === 0) {
    cb(new Error("can not find user servers."))
    return
  }

  console.log("--------dispatch-----------:" + session.get("rid"))

  var res = dispatcher.dispatch(session.get("rid"), userServer)

  cb(null, res.id)
}

exp.game = function (session, msg, app, cb) {
  var gameServer = app.getServersByType("game")

  if (!gameServer || gameServer.length === 0) {
    cb(new Error("can not find game servers."))
    return
  }

  console.log("--------dispatch-----------:" + session.get("rid"))

  var res = dispatcher.dispatch(session.get("rid"), gameServer)

  cb(null, res.id)
}

exp.config = function (session, msg, app, cb) {
  var configServer = app.getServersByType("config")

  if (!configServer || configServer.length === 0) {
    cb(new Error("can not find config servers."))
    return
  }

  console.log("--------dispatch-----------:" + session.get("rid"))

  var res = dispatcher.dispatch(session.get("rid"), configServer)

  cb(null, res.id)
}

exp.api = function (session, msg, app, cb) {
  var apiServer = app.getServersByType("config")

  if (!apiServer || apiServer.length === 0) {
    cb(new Error("can not find api servers."))
    return
  }

  console.log("--------dispatch-----------:" + session.get("rid"))

  var res = dispatcher.dispatch(session.get("rid"), apiServer)

  cb(null, res.id)
}

exp.bet = function (session, msg, app, cb) {
  var betServer = app.getServersByType("bet")

  if (!betServer || betServer.length === 0) {
    cb(new Error("can not find bet servers."))
    return
  }

  console.log("--------dispatch-----------:" + session.get("rid"))

  var res = dispatcher.dispatch(session.get("rid"), betServer)

  cb(null, res.id)
}
