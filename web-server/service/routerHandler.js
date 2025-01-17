var router_api = []
const socket = require("./socketHandler")
const fs = require("fs")

var getLobby = function (query, res) {
  var parameter = getUrlParams("?" + query)
  parameter["rid"] = 1

  var handler = new socket(function () {
    handler.sendMsg("connector.APIentryHandler.getGameLobby", parameter, function (r_data) {
      if (r_data.code != 200) {
        fs.createReadStream("./public/error.html").pipe(res)
        return
      }

      fs.createReadStream("./public/game.html").pipe(res)
      //fs.createReadStream('http://127.0.0.1:7456/?wl=gabrielwang&userId=140&authId=aaaaa&gameId=4&location=127.0.0.1').pipe(res);
      //res.location('http://127.0.0.1:7456/?wl=gabrielwang&userId=140&authId=aaaaa&gameId=4&location=127.0.0.1');
      //res.redirect('127.0.0.1:7456/?wl=gabrielwang&userId=140&authId=aaaaa&gameId=4&location=127.0.0.1');
      //console.log("--------r_data-----22--------:" + JSON.stringify(r_data));
    })
  })
}

var getGame = function (query, res) {
  //var parameter = query;
  var parameter = getUrlParams("?" + query)
  parameter["rid"] = 1

  //console.log("----------query--------:" + JSON.stringify(parameter));
  var handler = new socket(function () {
    handler.sendMsg("connector.APIentryHandler.getGameURL", parameter, function (r_data) {
      if (r_data.code != 200) {
        //fs.createReadStream('./public/error.html').pipe(res);
        return
      }
      //res.send(JSON.stringify({ code: 200, msgId:'GetGameUrl', url:r_data.data }));
      //res.location("http://127.0.0.1:7456/?wl=undefined&userId=undefined&authId=aaaaa&gameId=undefined&location=127.0.0.1");
      res.redirect("http://" + r_data.data)
      //fs.createReadStream(r_data.data).pipe(res);
      //console.log("--------r_data-------------:" + JSON.stringify(r_data));
    })
  })
  /*
     var handler = new socket( parameter, 'getGameURL', function( none, r_data ){
        //console.log( "--------none-------:"+JSON.stringify(none) );
        //console.log( "--------r_error-------:"+JSON.stringify(r_data) );
         if( r_data.code != 200 ){
             return;
         }
         //fs.createReadStream('./public/game.html').pipe(res);
     } );
 */
}

var loginPlayer = function (data, res) {
  var parameter = data
  //var parameter = getUrlParams('?'+JSON.stringify(query));
  parameter["rid"] = 1

  //console.log("----------query--------:" + JSON.stringify(data));

  var handler = new socket(function () {
    handler.sendMsg("connector.APIentryHandler.loginPlayer", parameter, function (r_data) {
      //console.log("--------r_data-------------:" + JSON.stringify(r_data));
      res.send(JSON.stringify({ code: 200, msgId: "LonInPlayer", token: r_data.token }))
    })
  })

  /*
    var handler = new socket( parameter, 'loginPlayer', function( none, r_data ){
       //console.log( "--------none-------:"+JSON.stringify(none) );
       //console.log( "--------r_error-------:"+JSON.stringify(r_data) );
        if( r_data.code != 200 ){
            return;
        }
        res.send(JSON.stringify({ code: 200, msgId:'LonInPlayer', token:r_data.token }));
        //fs.createReadStream('./public/game.html').pipe(res);
    } );
    */
}

// var playerGameHistory_byDate = function (data, res) {
//     var parameter = data;
//     parameter['rid'] = 1;

//     //console.log("-------playerGameHistory_byDate---query--------:" + JSON.stringify(data));

//     var handler = new socket(function () {
//         handler.sendMsg('connector.APIentryHandler.playerGameHistory_byDate', parameter, function (r_data) {
//             res.send(JSON.stringify({ code: 200, msgId: 'playerGameHistory_byDate', token: r_data.token }));
//         });
//     });
// }

var balance = 1000

var getBalance = function (data, res) {
  var parameter = data
  res.send(
    JSON.stringify({
      code: 200,
      msgId: "getUsrBalance",
      usrId: 127,
      nickName: "test",
      currency: "CN",
      balance: balance,
    })
  )
}

var transferBalance = function (data, res) {
  var parameter = data
  balance += data.win - data.bet
  res.send(
    JSON.stringify({
      code: 200,
      msgId: "transferBalance",
      usrId: 127,
      nickName: "test",
      currency: "CN",
      balance: balance,
    })
  )
}

//---------------------------------------------------API---------------------------------------------------------------------------------

router_api["Get_player"] = "connector.APIentryHandler.get_player"
router_api["Login"] = "connector.APIentryHandler.login_player" //多錢包-登入
router_api["Transfer_balance"] = "connector.APIentryHandler.transfer_balance" //多錢包-該指令用於營運商與遊戲商系統間帳號資金的轉移
router_api["Transfer_list"] = "connector.APIentryHandler.transfer_list"
router_api["Transfer_info"] = "connector.APIentryHandler.get_transfer_byToken"
router_api["Transaction_list"] = "connector.APIentryHandler.transaction_list"
router_api["Transaction_info"] = "connector.APIentryHandler.get_transaction_byToken"
router_api["Get_receipt"] = "connector.APIentryHandler.get_receipt" //多錢包-檢測Transfer_balance指令的請求是成功或失敗
router_api["Logout_player"] = "connector.APIentryHandler.logout_player"
router_api["Repair_bet"] = "connector.APIentryHandler.repair_bet" //單錢包補單 - 該指令用於檢測Repair_bet指令的請求是成功或失敗。
router_api["Get_repair"] = "connector.APIentryHandler.get_repair" //單錢包補單 -
router_api["Game_list"] = "connector.APIentryHandler.game_list" //取得遊戲清單
router_api["Lobby.html"] = "connector.APIentryHandler.get_lobby" //GET:取得遊戲大廳URL
router_api["Game.html"] = "connector.APIentryHandler.get_game" //GET:取得遊戲路徑
router_api["getPlayerWagers"] = "connector.APIentryHandler.getPlayerWagers"
router_api["getPlayerWagersByPlatformWid"] = "connector.APIentryHandler.getPlayerWagersByPlatformWid"
router_api["getPlayerWagersById"] = "connector.APIentryHandler.getPlayerWagersById"
router_api["getPlayerWagersInit"] = "connector.APIentryHandler.getPlayerWagersInit"
router_api["getPlayerWagerDetail"] = "connector.APIentryHandler.getPlayerWagerDetail"
router_api["getPlayerFishWagerDetail"] = "connector.APIentryHandler.getPlayerFishWagerDetail" //Record-快速子注單查詢
router_api["getWagerMetaData"] = "connector.APIentryHandler.getWagerMetaData"
router_api["getPlayerArcadeWagerDetail"] = "connector.APIentryHandler.getPlayerArcadeWagerDetail"

router_api["getUrlParserForVa"] = "connector.APIentryHandler.getUrlParserForVa"

router_api["adminlogin"] = "connector.backendHandler.admin_login"
router_api["userlogin"] = "connector.backendHandler.user_login"

router_api["ip_state"] = "connector.APIentryHandler.ip_state"

//----------------------------------------------2019.05.28------------------------------------------------------------

router_api["agent"] = "connector.agentAPI_Handler.agent"

var routerHandler = module.exports

routerHandler["GetGame"] = getGame
routerHandler["/game.html"] = getLobby

routerHandler["loginPlayer"] = loginPlayer
routerHandler["getUsrBalance"] = getBalance
routerHandler["transferBalance"] = transferBalance

back_sendMsg = function (data, res) {
  var pathname = data.pathname
  console.log("back_sendMsg------", pathname, router_api[pathname], JSON.stringify(data))
  var handler = new socket(function () {
    //先記錄IP及user
    if (typeof data != "undefined" && typeof data.remoteIP != "undefined") {
      data.remoteIP = data.remoteIP.replace("::ffff:", "") //IP修正
    }

    var param = {
      action: "ip_state",
      remoteIP: data.remoteIP,
    }

    handler.sendMsg(router_api["ip_state"], param, function (r_data) {
      if (r_data.code != 200) {
        var return_data = {
          code: "ERROR",
          msg: "Abnormal IP",
        }
        res.send(JSON.stringify(return_data))
      } else {
        handler.sendMsg(router_api[pathname], data, function (r_data) {
          console.log("back_sendMsg res:", JSON.stringify(r_data))
          res.send(JSON.stringify(r_data))
        })
      }
    })
  })
}

Object.keys(router_api).forEach((key) => {
  routerHandler[key] = back_sendMsg
})

function getUrlParams(query) {
  var params = {}
  var url = parseUrl(query)

  for (var i in url) {
    var key = Object.keys(url[i])[0]
    params[key] = url[i][key]
  }
  return params
}

function parseUrl(url) {
  var result = []
  var query = url.split("?")[1]
  var queryArr = query.split("&")
  queryArr.forEach(function (item) {
    var obj = {}
    var value = item.split("=")[1]
    var key = item.split("=")[0]
    obj[key] = value
    result.push(obj)
  })
  return result
}

/*
var get_player = function (data, res) {
    var handler = new socket(function () {
        //先記錄每秒執行次數  成功或失敗  

        handler.api_sendMsg('connector.APIentryHandler.get_player', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
};

var login_player = function (data, res) { 
    var handler = new socket(function () {
        handler.api_sendMsg('connector.APIentryHandler.login_player', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
};

var logout_player = function (data, res) {
    var handler = new socket(function () {
        handler.api_sendMsg('connector.APIentryHandler.logout_player', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
};

var transfer_balance = function (data, res) {
    var handler = new socket(function () {
        handler.api_sendMsg('connector.APIentryHandler.transfer_balance', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
};

var transfer_list = function (data, res) {
    var handler = new socket(function () {
        handler.api_sendMsg('connector.APIentryHandler.transfer_list', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
};

var transaction_list = function (data, res) {
    var handler = new socket(function () {
        handler.api_sendMsg('connector.APIentryHandler.transaction_list', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
};

var get_receipt = function (data, res) {
    var handler = new socket(function () {
        handler.api_sendMsg('connector.APIentryHandler.get_receipt', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
};

var repair_bet = function (data, res) {
    var handler = new socket(function () {
        handler.api_sendMsg('connector.APIentryHandler.repair_bet', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
};

var get_repair = function (data, res) {
    var handler = new socket(function () {
        handler.api_sendMsg('connector.APIentryHandler.get_repair', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
};

var game_list = function (data, res) {
    var handler = new socket(function () {
        handler.api_sendMsg('connector.APIentryHandler.game_list', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
}

var get_lobby = function (data, res) {
    var handler = new socket(function () {
        handler.api_sendMsg('connector.APIentryHandler.get_lobby', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
}

var get_game = function (data, res) {
    var handler = new socket(function () {
        handler.api_sendMsg('connector.APIentryHandler.get_game', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
}

var getUpUserName = function (data, res) {
    var handler = new socket(function () {
        handler.api_sendMsg('connector.APIentryHandler.getUpUserName', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
}; 

var getPlayerWagers = function (data, res) {
    var handler = new socket(function () {
        handler.api_sendMsg('connector.APIentryHandler.getPlayerWagers', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
};

var getPlayerWagersInit = function (data, res) {
    var handler = new socket(function () {
        handler.api_sendMsg('connector.APIentryHandler.getPlayerWagersInit', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
};

var getPlayerWagerDetail = function (data, res) {
    var handler = new socket(function () {
        handler.api_sendMsg('connector.APIentryHandler.getPlayerWagerDetail', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
};

//----------------------------------------------------------------------------

//控端登入
var admin_login = function (data, res) {  
    var handler = new socket(function () {
        handler.sendMsg('connector.backendHandler.admin_login', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });  
};

//管端登入
var user_login = function (data, res) {
    var handler = new socket(function () {
        handler.sendMsg('connector.backendHandler.user_login', data, function (r_data) {
            res.send(JSON.stringify(r_data));
        });
    });
};   
//-------------------------------------------------------------------
*/

/*
//多錢包
routerHandler['Get_player'] = get_player;
routerHandler['Login'] = login_player;
routerHandler['Transfer_balance'] = transfer_balance;
routerHandler['Transfer_list'] = transfer_list;
routerHandler['Transaction_list'] = transaction_list;
routerHandler['Get_receipt'] = get_receipt;
routerHandler['Logout_player'] = logout_player;

//單錢包補單
routerHandler['Repair_bet'] = repair_bet;
routerHandler['Get_repair'] = get_repair;
//大廳-(清單)
routerHandler['Game_list'] = game_list;
//大廳
routerHandler['Lobby.html'] = get_lobby;
routerHandler['Game.html'] = get_game;
routerHandler['adminlogin'] = admin_login;//admin-login 
routerHandler['userlogin'] = user_login;//user-login
//取玩家資料
routerHandler['getPlayerWagers'] = getPlayerWagers;
routerHandler['getPlayerWagersInit'] = getPlayerWagersInit; 
routerHandler['getPlayerWagerDetail'] = getPlayerWagerDetail; 
*/

//TEST
//routerHandler['sendMail'] = sendMail;
//routerHandler['getUpUserName'] = getUpUserName;
