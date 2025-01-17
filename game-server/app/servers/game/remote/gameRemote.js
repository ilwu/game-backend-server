var logger = require("pomelo-logger").getLogger("gameRemote", __filename)
var code = require("../../../util/code")
var m_async = require("async")
var gameDao = require("../../../DataBase/gameDao")
var configDao = require("../../../DataBase/configDao")

module.exports = function (app) {
  return new GameRemote(app)
}

//var companys = [];
//var rtps = [];
//var denoms = [];
//var types = [];
//var groups = [];
//var maths = [];
//var orderTypes = [];

var GameRemote = function (app) {
  this.app = app
  /*
    m_async.parallel({

        company: function (cb) {
            configDao.getCompany(function (err, companys) {
                if (err.code === code.OK) {
                    //AuthorityTemps = authTemps;
                    cb(null, companys);
                } else {
                    cb(null, null);
                }
            });
        },
        rtp: function (cb) {
            configDao.getRTPs(function (err, rtps) {

                if (err.code === code.OK) {
                    cb(null, rtps);
                } else {
                    cb(null, null);
                }

            });
        },
        denom: function (cb) {
            configDao.getDenoms(function (err, denoms) {
                if (err.code === code.OK) {
                    cb(null, denoms);
                } else {
                    cb(null, null);
                }
            });
        },
        type: function (cb) {
            configDao.getGameType(function (err, gameTypes) {
                if (err.code === code.OK) {
                    cb(null, gameTypes);
                } else {
                    cb(null, null);
                }
            });
        },
        group: function (cb) {
            configDao.getGameGroup(function (err, gameGroups) {
                if (err.code === code.OK) {
                    cb(null, gameGroups);
                } else {
                    cb(null, null);
                }
            });
        },
        math: function (cb) {
            configDao.getMathRng(function (err, maths) {
                if (err.code === code.OK) {
                    cb(null, maths);
                } else {
                    cb(null, null);
                }
            });
        },
        orderType: function (cb) {
            configDao.getOrderType(function (err, orderTypes) {
                if (err.code === code.OK) {
                    cb(null, orderTypes);
                } else {
                    cb(null, null);
                }
            });
        }
    }, function (errs, results) {

        if (!!results.company) {
            companys = results.company;
        }

        if (!!results.rtp) {
            rtps = results.rtp;
        }

        if (!!results.denom) {
            denoms = results.denom;
        }

        if (!!results.type) {
            types = results.type;
        }

        if (!!results.group) {
            groups = results.group;
        }

        if (!!results.math) {
            maths = results.math;
        }

        if (!!results.orderType) {
            orderTypes = results.orderType;
        }  
    });
*/
}

var game_remote = GameRemote.prototype

// game_remote.getGameCounts_byGroup = function (cb) {
//     gameDao.getGameCountsByGroup_admin(cb);
// };

// game_remote.getGames_byGroup_hall = function (data, cb) {

//     m_async.waterfall([
//         function (r_cb) {
//             var search_data = {
//                 groups: data.ggids,
//                 cid: typeof data.cid === 'undefined' ? null : data.cid
//             };
//             gameDao.getGames_byGroup_hall(search_data, r_cb);
//         }
//     ], function (none, r_code, r_games) {

//         if (r_code.code != code.OK) {

//             cb(null, { code: r_code.code }, null);
//         }

//         cb(null, { code: code.OK }, r_games);

//     });
// };

game_remote.getGames_Denom_byCid = function (data, cb) {
  try {
    gameDao.getGames_Denom_byCid(data, cb)
  } catch (err) {
    logger.error("[game_remote][getGames_Denom_byCid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

game_remote.getGameCounts_AG_byGroup = function (data, cb) {
  try {
    gameDao.getGameCountsByGroup_agent(data, cb)
  } catch (err) {
    logger.error("[game_remote][getGameCounts_AG_byGroup] catch err", err)
    cb(null, code.FAIL, null)
  }
}

game_remote.joinGameToHall = function (data, cb) {
  try {
    //console.log('---joinGameToHall-----', JSON.stringify(data));
    gameDao.joinGameToHall(data, cb)
  } catch (err) {
    logger.error("[game_remote][joinGameToHall] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// game_remote.joinGameToAgent = function (data, cb) {
//     gameDao.joinGameToAgent(data, cb);
// };

// game_remote.getBetLogs = function (data, next) {
//     /*
//     config_Mongo.getBetlog( msg.data, function( none, r_code, bets ){
//         if( r_code.code != code.OK ){
//            //console.log("------Get bet log Error------msg:");
//             next(null, { code: r_code.code, data:null });
//         }
//         next(null, {code: code.OK, data: bets });
//     } );
//     */
// };

// game_remote.getGameName = function (msg, callback) {

//     //console.log('--game_remote.getGameName', JSON.stringify(msg));

//     if (typeof msg === 'undefined') {
//         callback(null, { code: code.FAIL, msg: null }, null);
//         return;
//     }

//     gameDao.getGameName(msg, callback);
// };

game_remote.getUserJoinGames = function (msg, callback) {
  try {
    if (typeof msg === "undefined") {
      callback(null, { code: code.FAIL, msg: null }, null)
      return
    }

    gameDao.getUserJoinGames(msg, function (none, r_code, r_data) {
      //console.log('getUserJoinGames games re--', JSON.stringify(r_code), JSON.stringify(r_data))
      callback(null, { code: r_code.code, msg: null }, r_data)
      return
    })
  } catch (err) {
    logger.error("[game_remote][getUserJoinGames] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// game_remote.getGameList = function (msg, callback) {
//     if (typeof msg === 'undefined') {
//         callback(null, { code: code.FAIL, msg: null }, null);
//         return;
//     }
//     gameDao.getGameList(msg, function (none, r_code, r_data) {
//         callback(null, { code: r_code.code, msg: null }, r_data);
//         return;
//     });
// };

// game_remote.getGameId_byUser = function (msg, callback) {
//     if (typeof msg === 'undefined') {
//         callback(null, { code: code.FAIL, msg: null }, null);
//         return;
//     }

//     gameDao.getUserGames(msg, function (none, r_code, r_data) {
//         callback(null, { code: r_code.code, msg: null }, r_data);
//         return;
//     });
// }

// game_remote.getUserOwnCategory = function (msg, callback) {
//     if (typeof msg === 'undefined') {
//         callback(null, { code: code.FAIL, msg: null }, null);
//         return;
//     }

//     gameDao.getUserOwnCategory(msg, function (none, r_code, r_data) {
//         callback(null, { code: r_code.code, msg: null }, r_data);
//         return;
//     });
// }

// game_remote.getGamesSortInCategory = function (msg, callback) {
//     if (typeof msg === 'undefined') {
//         callback(null, { code: code.FAIL, msg: null }, null);
//         return;
//     }

//     gameDao.getGamesSortInCategory(msg, function (none, r_code, r_data) {
//         callback(null, { code: r_code.code, msg: null }, r_data);
//         return;
//     });
// }

// game_remote.getGameURL = function (msg, cb) {

//     if (typeof msg === 'undefined' || typeof msg.gameId === 'undefined') {
//         cb(null, { code: code.FAIL, msg: null }, null);
//         return;
//     }
//     var data = {
//         gameId: msg.gameId
//     };

//     gameDao.getGameURL(data, function (none, r_code, r_data) {

//         cb(null, { code: r_code.code, msg: null }, r_data);
//         return;
//     });

// };

// game_remote.getAllGameURL = function (cb) {

//     gameDao.getAllGameURL(function (none, r_code, r_data) {

//         cb(null, { code: r_code.code, msg: null }, r_data);
//         return;
//     });

// };

// game_remote.getGameName_byId = function (msg, cb) {

//     if (typeof msg === 'undefined' || typeof msg.gameId === 'undefined') {
//         cb(null, { code: code.FAIL, msg: null }, null);
//         return;
//     }

//     gameDao.getGameName_byId(msg, function (none, r_code, r_data) {

//         cb(null, { code: r_code.code, msg: null }, r_data);
//         return;
//     });
// };

// game_remote.getGamesInCategory = function (msg, cb) {
//     if (typeof msg === 'undefined' || typeof msg.games === 'undefined') {
//         cb(null, { code: code.FAIL, msg: null }, null);
//         return;
//     }
//     gameDao.getGamesInCategory(msg, function (none, r_code, r_data) {

//         cb(null, { code: r_code.code, msg: null }, r_data);
//         return;
//     });
// };

// game_remote.getCompany = function (cb) {
//     cb(null, { code: code.OK, msg: null }, companys);
//     return;
// }

// game_remote.getGamesImage_admin = function (msg, cb) {
//     gameDao.getGamesImage_admin(msg, function (none, r_code, r_data) {
//         cb(null, { code: r_code.code, msg: null }, r_data);
//         return;
//     });
// }

// game_remote.get_game_currency_denom_setting = function (msg, cb) {
//     gameDao.get_game_currency_denom_setting(msg, function (none, r_code, r_data) {
//         cb(null, { code: r_code.code, msg: null }, r_data);
//         return;
//     });
// }
