var JS_WS_CLIENT_TYPE = "js-websocket"
var JS_WS_CLIENT_VERSION = "0.0.1"
var WebSocketServer = require("websocket").w3cwebsocket
//var Protocol = window.Protocol;
var Protocol = require("../../NetEase-pomelo-protocol/lib/protocol")
var Package = Protocol.Package
var Message = Protocol.Message
var EventEmitter = require("../../component-emitter/index")
//var EventEmitter = window.EventEmitter;
//var protobuf = require("../../pomelonode-pomelo-protobuf/lib/client/protobuf");

var RES_OK = 200
var RES_FAIL = 500
var RES_OLD_CLIENT = 501

if (typeof Object.create !== "function") {
  Object.create = function (o) {
    function F() {}
    F.prototype = o
    return new F()
  }
}

var handlers = {}

function Pomelo() {
  this.protobuf = require("../../pomelonode-pomelo-protobuf/lib/client/protobuf")
  this.socket = null
  this.reqId = 0
  this.callbacks = {}

  //Map from request id to route
  this.routeMap = {}
  this.dict = {} // route string to code
  this.abbrs = {} // code to route string
  this.serverProtos = {}
  this.clientProtos = {}
  this.protoVersion = 0

  this.heartbeatInterval = 0
  this.heartbeatTimeout = 0
  this.nextHeartbeatTimeout = 0
  this.gapThreshold = 100 // heartbeat gap threashold
  this.heartbeatId = null
  this.heartbeatTimeoutId = null

  this.handshakeCallback = null

  this.decode = null
  this.encode = null
  this.disconnectCb = null
  this.handshakeBuffer = {
    sys: {
      type: JS_WS_CLIENT_TYPE,
      version: JS_WS_CLIENT_VERSION,
    },
    user: {},
  }

  this.initCallback = null
  this.localStorageProtos = null
}

Pomelo.prototype = Object.create(EventEmitter.prototype)
Pomelo.prototype.constructor = Pomelo

Pomelo.prototype.init = function (params, cb) {
  this.initCallback = cb
  var host = params.host
  var port = params.port
  this.isConnect = false

  this.encode = params.encode || this.defaultEncode
  this.decode = params.decode || this.defaultDecode

  console.log("encode: " + !!params.encode)

  var url = "ws://" + host
  if (port) {
    url += ":" + port
  }

  this.handshakeBuffer.user = params.user
  this.handshakeCallback = params.handshakeCallback
  this.initWebSocket(url, cb)
}

Pomelo.prototype.initWebSocket = function (url, cb) {
  console.log("connect to " + url)
  var that = this
  //Add protobuf version
  if (this.localStorageProtos && this.protoVersion === 0) {
    var protos = JSON.parse(this.localStorageProtos)

    this.protoVersion = protos.version || 0
    this.serverProtos = protos.server || {}
    this.clientProtos = protos.client || {}

    if (this.protobuf) this.protobuf.init({ encoderProtos: this.clientProtos, decoderProtos: this.serverProtos })
  }
  //Set protoversion
  this.handshakeBuffer.sys.protoVersion = this.protoVersion

  var onopen = function (event) {
    that.isConnect = true
    var obj = Package.encode(Package.TYPE_HANDSHAKE, Protocol.strencode(JSON.stringify(that.handshakeBuffer)))
    that.send(obj)
  }
  var onmessage = function (event) {
    //console.log("------------onmessage-----------------",event);
    processPackage(that, Package.decode(event.data), cb)
    // new package arrived, update the heartbeat timeout
    if (that.heartbeatTimeout) {
      that.nextHeartbeatTimeout = Date.now() + that.heartbeatTimeout
    }
  }
  var onerror = function (event) {
    that.emit("io-error", event)
    console.error("*****socket error******: ", JSON.stringify(event))
  }
  var onclose = function (event) {
    that.isConnect = false
    that.emit("close", event)
    console.error("*****socket close******: ")

    that.disconnectCb && that.disconnectCb()
    that.disconnectCb = null
  }

  const maxMessageSize = 30 * 1024 * 1024

  this.socket = new WebSocketServer(
    url,
    null,
    null,
    null,
    {},
    { maxReceivedFrameSize: maxMessageSize, maxReceivedMessageSize: maxMessageSize }
  )

  this.socket.binaryType = "arraybuffer"
  this.socket.onopen = onopen
  this.socket.onmessage = onmessage
  this.socket.onerror = onerror
  this.socket.onclose = onclose
}

Pomelo.prototype.defaultDecode = function (data) {
  //probuff decode
  var msg = Message.decode(data)

  if (msg.id > 0) {
    msg.route = this.routeMap[msg.id]
    delete this.routeMap[msg.id]
    if (!msg.route) {
      return
    }
  }

  msg.body = deCompose(this, msg)
  return msg
}

Pomelo.prototype.defaultEncode = function (reqId, route, msg) {
  var type = reqId ? Message.TYPE_REQUEST : Message.TYPE_NOTIFY

  //compress message by protobuf
  if (this.clientProtos && this.clientProtos[route]) {
    msg = this.protobuf.encode(route, msg)
  } else {
    msg = Protocol.strencode(JSON.stringify(msg))
  }

  var compressRoute = 0
  if (this.dict && this.dict[route]) {
    route = this.dict[route]
    compressRoute = 1
  }

  return Message.encode(reqId, type, compressRoute, route, msg)
}

Pomelo.prototype.disconnect = function (cb) {
  console.log("-------POMELO Disconnect----------------")
  this.disconnectCb = cb
  if (this.socket) {
    if (this.socket.disconnect) this.socket.disconnect()
    if (this.socket.close) this.socket.close()
    console.log("disconnect")
    this.socket = null
  }

  if (this.heartbeatId) {
    clearTimeout(this.heartbeatId)
    this.heartbeatId = null
  }

  if (this.heartbeatTimeoutId) {
    clearTimeout(this.heartbeatTimeoutId)
    this.heartbeatTimeoutId = null
  }
}

Pomelo.prototype.request = function (route, msg, cb) {
  if (arguments.length === 2 && typeof msg === "function") {
    cb = msg
    msg = {}
  } else {
    msg = msg || {}
  }
  route = route || msg.route
  if (!route) {
    return
  }

  this.reqId++
  this.sendMessage(this.reqId, route, msg)

  this.callbacks[this.reqId] = cb
  this.routeMap[this.reqId] = route
}

Pomelo.prototype.notify = function (route, msg) {
  msg = msg || {}
  sendMessage(0, route, msg)
}

Pomelo.prototype.sendMessage = function (reqId, route, msg) {
  if (this.encode) {
    msg = this.encode(reqId, route, msg)
  }

  var packet = Package.encode(Package.TYPE_DATA, msg)
  this.send(packet)
}

Pomelo.prototype.send = function (packet) {
  //console.log("--------send-----------"+this.isConnect);
  if (this.isConnect === false) return

  this.socket.send(packet.buffer)
}

Pomelo.prototype.heartbeatTimeoutCb = function () {
  var that = this
  var gap = that.nextHeartbeatTimeout - Date.now()
  if (gap > that.gapThreshold) {
    that.heartbeatTimeoutId = setTimeout(that.heartbeatTimeoutCb.bind(that), gap)
  } else {
    console.error("server heartbeat timeout")
    that.emit("heartbeat timeout")
    that.disconnect()
  }
}

var handler = {}

var heartbeat = function (pomelo, data) {
  if (!pomelo.heartbeatInterval) {
    // no heartbeat
    return
  }

  var obj = Package.encode(Package.TYPE_HEARTBEAT)
  if (pomelo.heartbeatTimeoutId) {
    clearTimeout(pomelo.heartbeatTimeoutId)
    pomelo.heartbeatTimeoutId = null
  }

  if (pomelo.heartbeatId) {
    // already in a heartbeat interval
    return
  }

  pomelo.heartbeatId = setTimeout(function () {
    pomelo.heartbeatId = null
    pomelo.send(obj)

    pomelo.nextHeartbeatTimeout = Date.now() + pomelo.heartbeatTimeout
    pomelo.heartbeatTimeoutId = setTimeout(pomelo.heartbeatTimeoutCb.bind(pomelo), pomelo.heartbeatTimeout)
  }, pomelo.heartbeatInterval)
}

var handshake = function (pomelo, data) {
  // console.log('--------------handshake-------------');
  data = JSON.parse(Protocol.strdecode(data))
  if (data.code === RES_OLD_CLIENT) {
    //console.log('------error--------client version not fullfill-------------');
    pomelo.emit("error", "client version not fullfill")
    return
  }

  if (data.code !== RES_OK) {
    // console.log('------error--------handshake fail-------------');
    pomelo.emit("error", "handshake fail")
    return
  }

  handshakeInit(pomelo, data)

  var obj = Package.encode(Package.TYPE_HANDSHAKE_ACK)
  pomelo.send(obj)
  if (pomelo.initCallback) {
    pomelo.initCallback(pomelo.socket)
    pomelo.initCallback = null
  }
}

var onData = function (pomelo, data) {
  var msg = data
  if (pomelo.decode) {
    msg = pomelo.decode(msg)
  }
  processMessage(pomelo, msg)
}

var onKick = function (pomelo, data) {
  pomelo.emit("onKick")
}

handlers[Package.TYPE_HANDSHAKE] = handshake
handlers[Package.TYPE_HEARTBEAT] = heartbeat
handlers[Package.TYPE_DATA] = onData
handlers[Package.TYPE_KICK] = onKick

var processPackage = function (pomelo, msg) {
  handlers[msg.type](pomelo, msg.body)
}

var processMessage = function (pomelo, msg) {
  if (!msg) return

  if (!msg.id) {
    // server push message
    pomelo.emit(msg.route, msg.body)
    return
  }

  //if have a id then find the callback function with the request
  var cb = pomelo.callbacks[msg.id]

  delete pomelo.callbacks[msg.id]
  if (typeof cb !== "function") {
    return
  }

  cb(msg.body)
  return
}

var processMessageBatch = function (pomelo, msgs) {
  for (var i = 0, l = msgs.length; i < l; i++) {
    processMessage(pomelo, msgs[i])
  }
}

var deCompose = function (pomelo, msg) {
  var route = msg.route

  //Decompose route from dict
  if (msg.compressRoute) {
    if (!pomelo.abbrs[route]) {
      return {}
    }

    route = msg.route = pomelo.abbrs[route]
  }

  if (pomelo.serverProtos && pomelo.serverProtos[route]) {
    return pomelo.protobuf.decode(route, msg.body)
  } else {
    return JSON.parse(Protocol.strdecode(msg.body))
  }

  return msg
}

var handshakeInit = function (pomelo, data) {
  if (data.sys && data.sys.heartbeat) {
    pomelo.heartbeatInterval = data.sys.heartbeat * 1000 // heartbeat interval
    pomelo.heartbeatTimeout = pomelo.heartbeatInterval * 2 // max heartbeat timeout
  } else {
    pomelo.heartbeatInterval = 0
    pomelo.heartbeatTimeout = 0
  }

  initData(pomelo, data)

  if (typeof pomelo.handshakeCallback === "function") {
    pomelo.handshakeCallback(data.user)
  }
}

//Initilize data used in pomelo client
var initData = function (pomelo, data) {
  if (!data || !data.sys) {
    return
  }
  var dict = data.sys.dict
  var protos = data.sys.protos

  //Init compress dict
  if (dict) {
    pomelo.dict = dict
    pomelo.abbrs = {}

    for (var route in dict) {
      pomelo.abbrs[dict[route]] = route
    }
  }

  //Init protobuf protos
  if (protos) {
    pomelo.protoVersion = protos.version || 0
    pomelo.serverProtos = protos.server || {}
    pomelo.clientProtos = protos.client || {}
    if (!!pomelo.protobuf) {
      pomelo.protobuf.init({ encoderProtos: protos.client, decoderProtos: protos.server })
    }

    //Save protobuf protos to localStorage
    //window.localStorage.setItem('protos', JSON.stringify(protos));
    pomelo.localStorageProtos = JSON.stringify(protos)
  }
}

module.exports = Pomelo
