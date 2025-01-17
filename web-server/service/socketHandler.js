var router = []

var Socket = require("../lib/components/pomelonode-pomelo-jsclient-websocket/lib/pomelo-client")

function Handle(cb) {
  this.socket = new Socket()
  this.host = "127.0.0.1"
  this.port = 3000
  this.init(cb)
}

Handle.prototype.init = function (cb) {
  var socket = this.socket
  var host = this.host
  var port = this.port
  var self = this

  socket.init(
    {
      host: host,
      port: port, //gate server port
      log: true,
    },
    function () {
      self.sendMsg("gate.gateHandler.queryEntry", { uid: "1" }, function (r_data) {
        if (r_data.code != 200) {
          return
        }
        //console.log('--------queryEntry r_data-------------', JSON.stringify(r_data));
        socket.disconnect(function () {
          // r_data.host = host;
          socket.init({ host: r_data.host, port: r_data.port, log: true }, function () {
            self.sendMsg("connector.entryHandler.enter", { uid: "1" }, function (r_data) {
              console.log("-----------connector response---------:" + host)
              cb()
            })
          })
        })
      })
    }
  )
}

Handle.prototype.on = function (eventId, cb) {
  // console.log('-eventId-----------------', eventId);

  if (typeof this.socket != "undefined") {
    this.socket.on(eventId, cb)
  }
}

Handle.prototype.disconnect = function () {
  if (typeof this.socket != "undefined" && this.socket.isConnect) {
    this.socket.disconnect()
  }
}

Handle.prototype.sendMsg = function (eventId, data, cb) {
  //  console.log('-sendMsg data ----',eventId,JSON.stringify(data))
  this.socket.request(
    eventId,
    {
      rid: 1,
      target: "*",
      data: data.data,
      uid: data.uid, //Load balance check id
      remoteIP: data.remoteIP,
      token: data.token,
      conn: data.conn,
    },
    cb
  )
}

module.exports = Handle

// Handle.prototype.processServerInit = function (socket, eventID, cb, send_data) {
//    //console.log("--------processServerInit-----------");
//     var socket = this.socket;
//     var keepConnect = this.keepConnect;
//     var host = this.host;
//     var port = this.port;
//     var eventId = eventID;
//     //var route = 'gate.gateHandler.queryEntry';
//     socket.init({
//         //host: "35.201.235.26",
//         host: host,
//         port: port,	//gate server port
//         //port:"4000",
//         //host: "35.201.236.172",
//         log: true
//     }, function () {

//         socket.request('gate.gateHandler.queryEntry', {
//             uid: 1 //Load balance check id
//         }, function (data) {

//             if (data.code != 200) {
//                //console.log('[Error][net::connectGateServer] Error code: ' + data.code);
//                 return;
//             }

//            //console.log('[Info][net::connectGateServer] Link gate server success Error code : ' + data.code);
//             socket.disconnect(function () {
//                 data.host = host;
//                 // net.connectServer(data.host, data.port,userInfo);	//connect for connect server
//                 socket.init({
//                     host: data.host,
//                     port: data.port,
//                     log: true
//                 }, function () {
//                     //var route_1 = "connector.APIentryHandler."+msgName;
//                     var route_1 = eventId;
//                    //console.log('[Info][net::connectServer] Link to connect server host: ' + data.host + ' port: ' + data.port);

//                     socket.request(route_1, send_data, function (data) {
//                         if (data.code) {
//                            //console.log('[Error][net::connect] ' + data);
//                             //return;
//                         }
//                        //console.log('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&[Info][net::connectServer] Link connect server success Response code : ' + data);
//                        //console.log('[Info][net::connectServer] Link connect server success Response code : ' + JSON.stringify(data));
//                         if (keepConnect)
//                             socket.disconnect();

//                         cb(null, data);

//                     });
//                 });
//             });
//         });
//     });
// };
