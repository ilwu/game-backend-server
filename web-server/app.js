var express = require("express")
var url = require("url")
const querystring = require("querystring")
var http = require("http")
var bodyParser = require("body-parser")
var WebSocketServer = require("websocket").server
var ServerRequestHandle = require("./service/requestHandler")
const routerRequst = require("./service/routerHandler")
const date = require("./service/date")
var jsonParser = bodyParser.json()
var connections = {}
var web_conn_keys = []

var socket_connect_type = 1 //後台登入--- 0:http 及socket 方式連 ,1: 只用socket

var app = express()

app.all("*", function (req, res, next) {
  res.header("Access-Control-Allow-Origin", "*")
  res.header("Access-Control-Allow-Headers", "Content-Type, Content-Length, Authorization, Accept, X-Requested-With ")
  res.header("Access-Control-Allow-Methods", "PUT, POST, GET, DELETE, OPTIONS")
  next()
})

app.configure(function () {
  app.use(express.methodOverride())
  app.use(express.bodyParser())
  app.use(app.router)
  app.set("view engine", "jade")
  app.set("views", __dirname + "/public")
  app.set("view options", { layout: false })
  app.set("basepath", __dirname + "/public")
})

app.configure("development", function () {
  app.use(express.static(__dirname + "/public"))
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true }))
})

app.configure("production", function () {
  var oneYear = 31557600000
  app.use(express.static(__dirname + "/public", { maxAge: oneYear }))
  app.use(express.errorHandler())
})

app.get("/:api", function (req, res) {
  console.log(" --------get-----request--------- " + req.params.api)
  var msg = req.query

  var pathname = req.params.api
  var query = url.parse(req.url).query
  var parsedQs = querystring.parse(query)
  var curdate = date.time()
  var unixTimestamp = new Date(curdate * 1000)
  var commonTime = unixTimestamp.toLocaleString()

  console.log(" --------get-----request curdate--------- " + JSON.stringify(curdate))
  console.log(" --------get-----request testDate--------- " + JSON.stringify(commonTime))
  console.log(" --------get-----request query--------- " + JSON.stringify(query))
  console.log(" --------get-----request pathname--------- " + JSON.stringify(pathname))
  console.log(" --------get-----request name--------- " + parsedQs.Profile)
  console.log(" --------get-----request parsedQs--------- " + JSON.stringify(parsedQs))

  var query = {
    pathname: pathname,
    data: parsedQs,
    remoteIP: req.connection.remoteAddress,
    //remoteIP: req.header('x-forwarded-for') || req.connection.remoteAddress
  }

  if (typeof routerRequst[pathname] != "undefined") {
    routerRequst[pathname](query, res)
  }
})

//--------------------------------------------------------------API-----------------------------------------------------------------
app.post("/", function (req, res) {
  res.send("POST request to the homepage")
})

app.post("/CreatePlayer", function (req, res) {
  res.send(
    JSON.stringify({
      code: 200,
      url: "http://192.168.28.246:3333/game.html?profile=profilename&wl=agent_1&token=1533536634950&lang=tw&sound=1&cashier_url=&exit_url=&theme=gray",
    })
  )
})

app.post("/LonInPlayer", jsonParser, function (req, res) {
  if (typeof routerRequst["LonInPlayer"] != "undefined") {
    routerRequst["LonInPlayer"](req.body, res)
  }
})

app.post("/GetLobbyUrl", function (req, res) {
  res.send(
    JSON.stringify({
      code: 200,
      url: "http://192.168.28.246:3333/game.html?profile=profilename&wl=agent_1&token=1533536634950&lang=tw&sound=1&cashier_url=&exit_url=&theme=gray",
    })
  )
})

app.post("/getUsrBalance", function (req, res) {
  if (typeof routerRequst["getUsrBalance"] != "undefined") {
    routerRequst["getUsrBalance"](req.body, res)
  }
})

app.post("/transferBalance", function (req, res) {
  if (typeof routerRequst["transferBalance"] != "undefined") {
    routerRequst["transferBalance"](req.body, res)
  }
})

app.post("/GetGameUrl", function (req, res) {
  var pathname = "GetGameUrl"
  var query = req.body
  if (typeof routerRequst[pathname] != "undefined") {
    routerRequst[pathname](query, res)
  }
})

app.post("/:api", jsonParser, function (req, res) {
  var pathname = req.params.api
  //console.log('-pathname-',pathname)
  // console.log("headers = " + JSON.stringify(req.headers));// 包含了各种header，包括x-forwarded-for(如果被代理过的话)
  // console.log("x-forwarded-for = " + req.header('x-forwarded-for'));// 各阶段ip的CSV, 最左侧的是原始ip
  // console.log("ips = " + JSON.stringify(req.ips));// 相当于(req.header('x-forwarded-for') || '').split(',')
  // console.log("remoteAddress = " + req.connection.remoteAddress);// 未发生代理时，请求的ip
  // console.log("ip = " + req.ip);// 同req.connection.remoteAddress, 但是格式要好一些

  //console.log('-api pathname-',pathname );

  var query = req.body
  var query = {
    pathname: pathname,
    remoteIP: req.connection.remoteAddress,
    //  remoteIP: req.header('x-forwarded-for') || req.connection.remoteAddress,
    data: req.body,
  }

  if (typeof routerRequst[pathname] != "undefined") {
    routerRequst[pathname](query, res)
  }
})

console.log("Web server has started.\nPlease log on http://127.0.0.1:2000/index.html")
app.listen(8082)

//-------------------------------------------------------------websocket-----------------------------------------------------------------

var server = http.createServer(function (request, response) {
  response.write("Hello World")
  response.end()
})

server.listen(8081, function () {
  console.log(new Date() + "Socket Server is listening on port 8081")
})

const maxMessageSize = 30 * 1024 * 1024

let wsServer = new WebSocketServer({
  httpServer: server,
  autoAcceptConnections: false,
  maxReceivedFrameSize: maxMessageSize,
  maxReceivedMessageSize: maxMessageSize,
})

function originIsAllowed(origin) {
  // put logic here to detect whether the specified origin is allowed.
  return true
}
//------------------------------------------------ 0 ------------------------------------------------------------
if (socket_connect_type == 0) {
  wsServer.on("request", function (request) {
    //連線

    if (!originIsAllowed(request.origin)) {
      request.reject()
      return
    }

    var protocol = request.httpRequest.headers["sec-websocket-protocol"]

    if (protocol == undefined) {
      request.reject()
      return
    }

    var protocol_value = protocol.split(",")
    if (protocol_value.length != 2) {
      request.reject()
      return
    }

    // var connection = request.accept('echo-protocol', request.origin); //接受連線
    var connection = {}
    var token = protocol_value[1].trim() //socket連線
    var socket_conn = {}

    //未連線
    if (typeof connections[token] == "undefined") {
      console.log("connections undefined")

      createConnection(connection, token, function (re_socket_conn) {
        //判斷此Jwt是否正確
        var data = {
          action: "check_user_token",
          conn: 1,
          token: token,
        }
        socket_conn = re_socket_conn
        socket_conn.tokenAuth(data, function (r_code) {
          console.log("----------auth 1-----------", JSON.stringify(r_code))
          if (r_code.code != 200) {
            connection = request.accept("echo-protocol", request.origin) //接受連線
            connection.send(JSON.stringify(r_code))
            connection.close()
            socket_conn.disconnect()
            socket_conn = undefined
          }

          connections[token] = socket_conn
          if (typeof socket_conn != "undefined") {
            connection = request.accept("echo-protocol", request.origin) //接受連線
            connections[token].setConnect(connection)
          }
          //傳送訊息
          connection.on("message", function (message) {
            if (message.type === "utf8") {
              var oMessage = JSON.parse(message.utf8Data)
              console.log("-oMessage-", JSON.stringify(oMessage))
              oMessage["token"] = token

              var data = {
                action: "check_user_token",
                conn: 2,
                token: token,
              }

              console.log("data2-----------", JSON.stringify(data))

              if (typeof connections[token] != "undefined") {
                connections[token].tokenAuth(data, function (r_code) {
                  console.log("----------auth 2-----------", JSON.stringify(r_code))
                  if (r_code.code != 200) {
                    connection.send(JSON.stringify(r_code))
                    connection.close() //前端websocket
                    socket_conn.disconnect() //後端socket
                    connections[token] = undefined
                  } else {
                    //  connections[token].sendMsg(oMessage);
                    connections[token].sendMsg_v2(oMessage, function (r_data) {
                      if (r_data.action == "request_logout") {
                        connection.close()
                        socket_conn.disconnect()
                        connections[token] = undefined //登出後 清除 連線
                      }
                    })
                  }
                })
              } else {
                //連線有誤
              }
            } else if (message.type === "binary") {
              connection.sendBytes(message.binaryData)
            }
          })
          //F5重整或關閉
          connection.on("close", function (reasonCode, description) {
            if (typeof socket_conn != "undefined") socket_conn.disconnect() //socket close
            if (typeof connection != "undefined") connection.close() //websocket close
            connections[token] = undefined
          })

          connection.on("error", function (reasonCode, description) {
            console.log("error")
          })
        })
      })
    } else {
      //重複連線 (7001-USER_LOGIN_DUPLICATE)
      console.log("connections USER_LOGIN_DUPLICATE ")

      var data = {
        code: 7001,
      }
      connection = request.accept("echo-protocol", request.origin) //接受連線
      connection.send(JSON.stringify(data))
      connection.close()
      return
    }
  })

  var createConnection = function (connection, token, cb) {
    //建立連線
    var socket_conn = new ServerRequestHandle(connection, function () {
      cb(socket_conn)
    })
  }
}

//----------------------------------------------- 1 -------------------------------------------------------------
//新版
if (socket_connect_type == 1) {
  wsServer.on("request", function (request) {
    //連線
    var server_conn = null //後端server socket
    var i = 0
    //拒絕連線
    if (!originIsAllowed(request.origin)) {
      request.reject()
      return
    }

    //接受連線 ALL --------
    //var web_conn = request.accept('echo-protocol', request.origin); //與前端 WEB 建立連線
    var web_conn = null
    var web_conn_key = request.httpRequest.headers["sec-websocket-key"]

    //建立後端socket連線 ->
    server_conn = new ServerRequestHandle(web_conn, function () {
      web_conn = request.accept("echo-protocol", request.origin) //與前端 WEB 建立連線
      server_conn.setConnect(web_conn)

      web_conn_keys.push(web_conn_key)
      console.log("建立 websocket連線完成 ")
      // sendMsg(server_conn, oMessage);

      //傳送訊息
      web_conn.on("message", function (message) {
        if (message.type === "utf8") {
          var oMessage = JSON.parse(message.utf8Data)

          oMessage["web_conn_key"] = web_conn_key
          var web_conn_key_index = web_conn_keys.indexOf(web_conn_key)

          sendMsg(server_conn, oMessage)

          /*  
                    if (web_conn_key_index > -1) { //傳送訊息  
                        console.log('傳送訊息: ' )
                        sendMsg(server_conn, oMessage); 
                    } else { //建立後端socket連線 ->  
                        console.log('建立後端socket連線:',oMessage['action']);   
                            server_conn = new ServerRequestHandle(web_conn, function () {  
                                web_conn_keys.push(web_conn_key); 
                                console.log('建立 socket連線完成 & 傳送訊息:', oMessage['action'])
                                sendMsg(server_conn, oMessage);
                            }); 
                    }  
                    */
        } else if (message.type === "binary") {
          console.log("----------------message.type------------------", message.type)
        }
      })

      //F5重整或關閉
      web_conn.on("close", function (reasonCode, description) {
        console.log("client close:", web_conn_key)
        if (server_conn != null) {
          server_conn.disconnect_v2(web_conn_key)
        }
      })

      web_conn.on("error", function (reasonCode, description) {
        console.log("client error")
      })
    })
  })

  var sendMsg = function (server_conn, oMessage) {
    // console.log('send msg:',JSON.stringify(oMessage));
    server_conn.sendMsg_v3(oMessage, function (r_data) {})
  }
}

//------------------------------------------------------------------------------------------------------------

// var getUserAgent = function () {
//     var keywords = ["Android", "iPhone", "iPod", "iPad", "Windows Phone", "MQQBrowser"];
//     return true;
// };

// var server = http.createServer(function (request, response) {
//     response.write("Hello World");
//     response.end();
// });

// server.listen(8081, function () {
//     console.log((new Date()) + 'Socket Server is listening on port 8081');
// });

// wsServer = new WebSocketServer({
//     httpServer: server,
//     autoAcceptConnections: false
// });

// function originIsAllowed(origin) {
//     // put logic here to detect whether the specified origin is allowed.
//     return true;
// }
// var i = 0;

// // connection.on('connect', function (connection) {
// //     console.log('-------connect-------------', connection);
// // });

// wsServer.on('request', function (request) { //連線
//     if (!originIsAllowed(request.origin)) {
//         // Make sure we only accept requests from an allowed origin
//         request.reject();
//         //console.log((new Date()) + ' Connection from origin ' + request.origin + ' rejected.');
//         return;
//     }

//     console.log('------request--------');
//     var connection = request.accept('echo-protocol', request.origin);
//     var connect_key = request.key;   //socket連線

//     connection.on('message', function (message) {
//         console.log('message - ', message);
//         if (message.type === 'utf8') {
//             //裝置
//             var agent = getUserAgent(request.httpRequest.headers['user-agent']);

//             //console.log("-------remote address-----:" + connection.remoteAddress);
//             //console.log('-------Received Message: ' + typeof message.utf8Data);

//             var oMessage = JSON.parse(message.utf8Data);

//             if (typeof connections[connect_key] != 'undefined') {
//                 connections[connect_key].setConnect(connection);
//                 oMessage['connect_key'] = connect_key;
//                 connections[connect_key].sendMsg(oMessage);
//             } else {
//                 //-------------------------------------
//                 var conn = new ServerRequestHandle(connection, function () {
//                     connections[connect_key] = conn;
//                     oMessage['connect_key'] = connect_key;
//                     connections[connect_key].sendMsg(oMessage);
//                 });
//             }

//         }
//         else if (message.type === 'binary') {
//             connection.sendBytes(message.binaryData);
//         }
//     });

//     //F5重整或關閉
//     connection.on('close', function (reasonCode, description) {
//         /*
//         console.log((new Date()) + ' Peer ' + connection.remoteAddress + ' disconnected.');
//         console.log('close', request.key, connect_key);
//         console.log(connections);
//         var data = {
//             acton: 'request_disconnect',
//             data: {
//                 connect_key: connect_key
//             }
//         };
//         connections[connect_key].disconnect(data);
//         */
//         /*
//                var oMessage = {
//                    action: 'request_offline_user',
//                    connect_key: connect_key
//                }
//                connections[connect_key].sendMsg(oMessage);
//           */
//         //  console.log(connection);
//         //delete connections[ connection.remoteAddress ];
//         //connections[ connection.remoteAddress ] = null;

//     });

//     /*
//     setTimeout(() => {
//         console.log('-----------close connect ---------------');
//         connection.close();
//     }, 5000);
//     */
// });

// var getUserAgent = function () {
//     var keywords = ["Android", "iPhone", "iPod", "iPad", "Windows Phone", "MQQBrowser"];
//     return true;
// };

// /*
// //back_190214
// var server = http.createServer(function (request, response) {
//     response.write("Hello World");
//     response.end();
// });

// server.listen(8081, function () {
//     //console.log((new Date()) + ' Server is listening on port 8081');
// });

// wsServer = new WebSocketServer({
//     httpServer: server,
//     autoAcceptConnections: false
// });

// function originIsAllowed(origin) {
//     // put logic here to detect whether the specified origin is allowed.
//     return true;
// }

// wsServer.on('request', function (request) {
//     if (!originIsAllowed(request.origin)) {
//         // Make sure we only accept requests from an allowed origin
//         request.reject();
//         //console.log((new Date()) + ' Connection from origin ' + request.origin + ' rejected.');
//         return;
//     }
//     //
//     var protocol = request.httpRequest.headers['sec-websocket-protocol'];
//     var protocol_value = protocol.split(",");
//     var connection = request.accept('echo-protocol', request.origin);

//     if (protocol_value.length == 2) { //連線
//         var connect_token = protocol_value[1].trim();
//         var connect_key = connect_token;   //socket連線
//         //建立連線
//         if (typeof connections[connect_key] != 'undefined') {
//             connections[connect_key].setConnect(connection, function (cb) {
//                 onWsConnStatus();
//                 connection.on('message', onWsConnMessage);
//                 connection.on('close', onWsConnClose);
//             });
//         } else {
//             onWsConnStatus();
//             connection.on('message', onWsConnMessage);
//             connection.on('close', onWsConnClose);
//         }
//     }
//     if (protocol_value.length < 2) { //無token
//         request.reject();
//         return;
//     }

//     function onWsConnMessage(message) {
//         var oMessage = {};
//         if (message.type === 'utf8') {
//             oMessage = JSON.parse(message.utf8Data);
//         }
//         else if (message.type === 'binary') {
//             connection.sendBytes(message.binaryData);
//         }

//         if (typeof connections[connect_key] == 'undefined') {
//             var conn = new ServerRequestHandle(connection, function () {
//                 connections[connect_key] = conn;
//                 oMessage['connect_key'] = connect_key;
//                 connections[connect_key].sendMsg(oMessage);
//             });
//         } else {
//             oMessage['connect_key'] = connect_key;
//             connections[connect_key].sendMsg(oMessage);
//         }
//     }

//     function onWsConnClose(reasonCode, description) {
//         console.log((new Date()) + ' Peer ' + connection.remoteAddress + ' disconnected.');

//         var oMessage = {
//             action: 'request_offline_user',
//             connect_key: connect_token
//         }
//         connections[connect_key].sendServer(oMessage);
//         delete connections[connect_key];

//     }

//     function onWsConnStatus(){
//         if (typeof connections[connect_key] == 'undefined') {
//             var conn = new ServerRequestHandle(connection, function () {
//                 connections[connect_key] = conn;
//                 var oMessage = {
//                     action: 'checkUserConnectState',
//                     connect_key: connect_token
//                 }
//                 connections[connect_key].sendServer(oMessage);
//             });
//         } else {
//             oMessage['connect_key'] = connect_key;
//             var oMessage = {
//                 action: 'checkUserConnectState',
//                 connect_key: connect_token
//             }
//             connections[connect_key].sendServer(oMessage);
//         }
//     }

// });
// */
