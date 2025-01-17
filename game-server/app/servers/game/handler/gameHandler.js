var logger = require("pomelo-logger").getLogger("gameHandler", __filename)
var m_async = require("async")
var code = require("../../../util/code")
var gameDao = require("../../../DataBase/gameDao")
var userDao = require("../../../DataBase/userDao")
var bettingDao = require("../../../DataBase/bettingDao")
var timezone = require("../../../util/timezone")
var configDao = require("../../../DataBase/configDao")
var logDao = require("../../../DataBase/logDao")
var pomelo = require("pomelo")

module.exports = function (app) {
  return new Handler(app)
}

var companys = []
var rtps = []
var denoms = []
var types = []
var groups = []
var maths = []
var orderTypes = []

var Handler = function (app) {
  try {
    this.app = app

    m_async.parallel(
      {
        company: function (cb) {
          configDao.getCompany(function (err, r_companys) {
            if (err.code === code.OK) {
              //AuthorityTemps = authTemps;
              cb(null, r_companys)
            } else {
              cb(null, null)
            }
          })
        },
        rtp: function (cb) {
          configDao.getRTPs(function (err, rtps) {
            if (err.code === code.OK) {
              cb(null, rtps)
            } else {
              cb(null, null)
            }
          })
        },
        denom: function (cb) {
          configDao.getDenoms(function (err, denoms) {
            if (err.code === code.OK) {
              cb(null, denoms)
            } else {
              cb(null, null)
            }
          })
        },
        type: function (cb) {
          configDao.getGameType(function (err, gameTypes) {
            if (err.code === code.OK) {
              cb(null, gameTypes)
            } else {
              cb(null, null)
            }
          })
        },
        group: function (cb) {
          configDao.getGameGroup(function (err, gameGroups) {
            if (err.code === code.OK) {
              cb(null, gameGroups)
            } else {
              cb(null, null)
            }
          })
        },
        math: function (cb) {
          configDao.getMathRng(function (err, maths) {
            if (err.code === code.OK) {
              cb(null, maths)
            } else {
              cb(null, null)
            }
          })
        },
        orderType: function (cb) {
          configDao.getOrderType(function (err, orderTypes) {
            if (err.code === code.OK) {
              cb(null, orderTypes)
            } else {
              cb(null, null)
            }
          })
        },
      },
      function (errs, results) {
        if (!!results.company) {
          companys = results.company
        }

        if (!!results.rtp) {
          rtps = results.rtp
        }

        if (!!results.denom) {
          denoms = results.denom
        }

        if (!!results.type) {
          types = results.type
        }

        if (!!results.group) {
          groups = results.group
        }

        if (!!results.math) {
          maths = results.math
        }

        if (!!results.orderType) {
          orderTypes = results.orderType
        }
      }
    )
  } catch (err) {
    logger.error("[gameHandler][Handler] catch err", err)
  }
}

var handler = Handler.prototype
/* 
handler.getInit = function (msg, session, next) {

    var self = this;
    var companys = [];
    var denoms = [];
    var rtps = [];
    var maths = [];
    var orderTypes = [];
    var types=[];
 
    m_async.waterfall([function (cb) {

        self.app.rpc.config.configRemote.getGameGroup(session, cb);
    }, function (r_code, r_data, cb) { 
        groups = r_data;

        self.app.rpc.config.configRemote.getGameType(session, cb);
    }, function (r_code, r_data, cb) { 
        types = r_data;
        
        self.app.rpc.config.configRemote.getCompany(session, cb);
    },function (r_code, r_data, cb) {
        companys = r_data;

        self.app.rpc.config.configRemote.getDenoms(session, cb);
    },function (r_code, r_data, cb) {
        denoms = r_data;

        self.app.rpc.config.configRemote.getRTPs(session, cb);
    },function (r_code, r_data, cb) {
        rtps = r_data;
 
        self.app.rpc.config.configRemote.getMathRng(session, cb);
    },function (r_code, r_data, cb) {
        maths = r_data;  

        self.app.rpc.config.configRemote.getOrderType(session, cb);
    },function (r_code, r_data, cb) {
        orderTypes = r_data;   

        var params = {
            state: '1'
        }

        gameDao.getListGameTag(params, cb);

    }], function (none, r_code, r_tags) {

        for (var i in r_tags) {
            r_tags[i]['modifyDate'] = timezone.LocalToUTC(r_tags[i]['modifyDate']);
        }
        next(null, {
            code: code.OK, data: {
                companys: companys,
                rtps: rtps,
                denoms: denoms,
                types: types,
                groups: groups,
                maths: maths,
                orderTypes: orderTypes,
                tags: r_tags
            }
        }); 
    });   
};
*/
handler.getInit = function (msg, session, next) {
  try {
    var self = this
    var companys = []
    var denoms = []
    var rtps = []
    var maths = []
    var orderTypes = []
    var types = []
    var groups = []

    m_async.waterfall(
      [
        function (cb) {
          configDao.getGameGroup_2(cb)
        },
        function (r_code, r_data, cb) {
          groups = r_data

          configDao.getGameType_2(cb)
        },
        function (r_code, r_data, cb) {
          types = r_data

          configDao.getCompany_2(cb)
        },
        function (r_code, r_data, cb) {
          companys = r_data

          configDao.getDenoms_2(cb)
        },
        function (r_code, r_data, cb) {
          denoms = r_data

          configDao.getRTPs_2(cb)
        },
        function (r_code, r_data, cb) {
          rtps = r_data

          configDao.getMathRng_2(cb)
        },
        function (r_code, r_data, cb) {
          maths = r_data

          configDao.getOrderType_2(cb)
        },
        function (r_code, r_data, cb) {
          orderTypes = r_data
          var params = {
            state: "1",
          }
          gameDao.getListGameTag(params, cb)
        },
      ],
      function (none, r_code, r_tags) {
        for (var i in r_tags) {
          r_tags[i]["modifyDate"] = timezone.LocalToUTC(r_tags[i]["modifyDate"])
        }
        next(null, {
          code: code.OK,
          data: {
            companys: companys,
            rtps: rtps,
            denoms: denoms,
            types: types,
            groups: groups,
            maths: maths,
            orderTypes: orderTypes,
            tags: r_tags,
          },
        })
      }
    )
  } catch (err) {
    logger.error("[gameHandler][getInit] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
handler.createGame = function (msg, session, next) {
  try {
    console.log("----------createGame-----", JSON.stringify(msg))
    var self = this
    msg.data.mathId = 0
    var gameId = 0

    var logData = {}
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "add"
    logData["FunctionGroupL"] = "Game"
    logData["FunctionAction"] = "AddGame"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""

    var log_mod_after = []
    var sys_config = self.app.get("sys_config")

    if (typeof msg.data.tag_start_date != "undefined" && msg.data.tag_start_date !== "") {
      msg.data.tag_start_date = timezone.UTCToLocal(msg.data.tag_start_date)
    }

    if (typeof msg.data.tag_end_date != "undefined" && msg.data.tag_end_date !== "") {
      msg.data.tag_end_date = timezone.UTCToLocal(msg.data.tag_end_date)
    }

    m_async.waterfall(
      [
        function (cb) {
          msg.data["main_currency"] = sys_config.main_currency

          //遊戲標籤整理
          if (typeof msg.data.tId == "undefined") {
            msg.data["tId"] = 0
          }
          if (msg.data.tId == "") {
            msg.data["tId"] = 0
          }
          if (msg.data["tId"] == 0) {
            msg.data["tag_time_state"] = 0
            msg.data["tag_start_date"] = "0000-00-00 00:00:00"
            msg.data["tag_end_date"] = "0000-00-00 00:00:00"
          }
          if (msg.data["tag_time_state"] == 0) {
            msg.data["tag_start_date"] = "0000-00-00 00:00:00"
            msg.data["tag_end_date"] = "0000-00-00 00:00:00"
          }
          if (msg.data["tag_time_state"] == 1) {
            //有限時間
            msg.data["tag_start_date"] =
              typeof msg.data["tag_start_date"] == "undefined" ? "0000-00-00 00:00:00" : msg.data["tag_start_date"]
            msg.data["tag_end_date"] =
              typeof msg.data["tag_end_date"] == "undefined" ? "0000-00-00 00:00:00" : msg.data["tag_end_date"]
          }

          gameDao.createGame(msg.data, cb)
        },
        function (r_code, r_gameId, cb) {
          console.log("-------------------createGame 1 -----------------------", JSON.stringify(r_code), r_gameId)
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.GAME_CREATE_FAIL,
              data: null,
            })
            return
          }
          gameId = r_gameId
          var param = {
            gameId: gameId,
            denoms: msg.data.denoms,
          }

          gameDao.modifyGameCurrencyDenom(param, cb) //新增currency_denom
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.GAME_DENOM_CREATE_FAIL,
              data: null,
            })
            return
          }

          if (typeof msg.data.image === "undefined") {
            msg.data["image"] = []
          }

          var image_param = {
            image: msg.data["image"],
            gameId: gameId,
          }

          modifyGameImage(image_param, cb)
        },
        function (r_code, cb) {
          console.log("-------------------createGame 2 -----------------------", JSON.stringify(r_code))
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.GAME_CREATE_FAIL,
              data: null,
            })
            return
          }

          gameDao.getGame_ByGameId(
            {
              gameId: gameId,
            },
            cb
          ) //after - game
        },
        function (r_code, r_data, cb) {
          console.log(
            "-------------------createGame 3 -----------------------",
            JSON.stringify(r_code),
            JSON.stringify(r_data)
          )
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.LOG_GAME_FAIL,
              data: null,
            })
            return
          }

          var info = {
            games: r_data,
          }
          var after_log = []
          after_log.push(info)
          log_mod_after = log_mod_after.concat(after_log)

          gameDao.getGameImage_ByGameId(
            {
              gameId: gameId,
            },
            cb
          ) //after - game_image
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.LOG_GAME_FAIL,
              data: null,
            })
            return
          }

          var info = {
            game_image: r_data,
          }
          var after_log = []
          after_log.push(info)
          log_mod_after = log_mod_after.concat(after_log)

          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["AdminId"] = session.get("cid") || ""
          logData["UserName"] = session.get("usrName") || ""
          console.log("add_log_admin param:", JSON.stringify(logData))
          logDao.add_log_admin(logData, cb)
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOG_FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: {
            gameId: gameId,
          },
        })
        return
      }
    )

    // gameDao.createGame(msg.data, function (r_code, gameId) {
    //     if (r_code.code != code.OK) {
    //         next(null, { code: code.GAME.GAME_CREATE_FAIL, data: null });
    //         return;
    //     }
    //     next(null, { code: code.OK, data: "Success" });

    //     //-------------------------------

    //     var gamedata = {
    //         game_id: typeof gameId != 'undefined' ? gameId : -1,
    //         game_name: typeof msg.data.nameC != 'undefined' ? msg.data.nameC : '',
    //         enable: typeof msg.data.sw != 'undefined' ? msg.data.sw : 1,
    //         rtps: typeof msg.data.rtps != 'undefined' ? msg.data.rtps.split(",") : []
    //     };
    //     /*
    //             config_db.create_game( gamedata, function( r_code ){
    //                 if( r_code.code != code.OK ){
    //                    //console.log("-------------mongose----create games fail-----------");
    //                 }
    //             });
    //             */
    //     //-------------------------------

    // });
  } catch (err) {
    logger.error("[gameHandler][createGame] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.deleteGame = async function (msg, session, next) {
  try {
    if (typeof msg.data === "undefined" || typeof msg.data.gameId === "undefined") {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }

    const gameId = msg.data.gameId

    m_async.waterfall(
      [
        (cb) => {
          bettingDao.isGameInWagerRecords(msg.data, cb)
        },
        (r_code, r_data, cb) => {
          if (r_code.code !== code.OK) {
            next(null, {
              code: code.GAME.FIND_GAME_IN_WAGERS_FAIL,
              data: null,
            })
            return
          } else if (r_data === true) {
            next(null, {
              code: code.GAME.CANT_DELETE_GAME_HAVE_BETS,
              data: null,
            })
            return
          }

          gameDao.deleteGame(msg.data, cb)
        },
        (r_code, cb) => {
          if (r_code.code !== code.OK) {
            next(null, {
              code: code.GAME.GAME_DELETE_FAIL,
              data: null,
            })
            return
          }

          const logData = {}
          logData["IP"] = msg.remoteIP
          logData["ModifiedType"] = "delete"
          logData["FunctionGroupL"] = "Game"
          logData["FunctionAction"] = "DeleteGame"
          logData["RequestMsg"] = JSON.stringify(msg)
          logData["Desc_Before"] = JSON.stringify({ gameId })
          logData["Desc_After"] = ""
          logData["AdminId"] = session.get("cid") || ""
          logData["UserName"] = session.get("usrName") || ""

          logDao.add_log_admin(logData, cb)
        },
      ],
      (none, r_code, r_id) => {
        if (r_code.code !== code.OK) {
          next(null, {
            code: code.GAME.LOG_GAME_DELETE_FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: `Success deleted game id ${gameId}`,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[gameHandler][deleteGame] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getGames = function (msg, session, next) {
  try {
    var self = this
    var ttlCount = 0
    var curPage = 0
    var pageCount = 0
    var games = []
    var rtps = []
    var gamesImages = []
    var data_query = {
      curPage: 0,
      pageCount: 0,
      ggIds: [],
      typeIds: [],
      companyIds: [],
      name: "",
    }
    var gamesId = []
    var gamesDenom = []

    var denoms = []
    var sys_config = self.app.get("sys_config")

    if (typeof msg.data.page === "undefined" || typeof msg.data.pageCount === "undefined") {
      next(null, {
        code: code.GAME.GAME_QUERY_DATA_FAIL,
        data: null,
      })
      return
    } else if (typeof msg.data.page != "number" || typeof msg.data.pageCount != "number") {
      next(null, {
        code: code.GAME.GAME_QUERY_DATA_FAIL,
        data: null,
      })
      return
    } else if (msg.data.page <= 0 || msg.data.pageCount <= 0) {
      next(null, {
        code: code.GAME.GAME_QUERY_DATA_FAIL,
        data: null,
      })
      return
    }

    data_query.curPage = msg.data.page
    data_query.pageCount = msg.data.pageCount

    if (typeof msg.data.ggId != "undefined" && typeof msg.data.ggId === "string") {
      if (msg.data.ggId != "" || msg.data.ggId != "") data_query.ggIds = msg.data.ggId.split(",")
    }
    if (typeof msg.data.typeId != "undefined" && typeof msg.data.typeId === "string") {
      if (msg.data.typeId != "" || msg.data.typeId != "") data_query.typeIds = msg.data.typeId.split(",")
    }
    if (typeof msg.data.companyId != "undefined" && typeof msg.data.companyId === "string") {
      if (msg.data.companyId != "" || msg.data.companyId != "") data_query.companyIds = msg.data.companyId.split(",")
    }
    if (typeof msg.data.name != "undefined" && typeof msg.data.name === "string" && msg.data.name != "") {
      data_query.name = msg.data.name
    }
    let DC = ""
    m_async.waterfall(
      [
        function (cb) {
          //透過cid取得dc
          if (session.get("level") != 1) {
            const data = { Cid: session.get("cid") }
            userDao.get_user_byId(data, cb)
          } else {
            cb(
              null,
              {
                code: code.OK,
              },
              null
            )
          }
        },
        function (r_code, r_data, cb) {
          if (session.get("level") != 1) {
            const [[data]] = r_data.map((x) => x.customer)
            DC = data.DC
          }
          data_query["level"] = session.get("level")
          if (session.get("level") === 1) {
            cb(
              null,
              {
                code: code.OK,
              },
              null
            )
          } else {
            var userId = 0
            if (session.get("isSub") == 1 && session.get("level") == 2) {
              //子帳號 -> hallId
              userId = session.get("hallId")
            } else {
              userId = session.get("cid")
            }
            data_query["hallId"] = userId
            gameDao.getUserGames_bySetting(data_query, cb)
          }
        },
        function (r_code, r_data, cb) {
          if (session.get("level") > 1) {
            var gamesId = []
            for (var i in r_data) {
              gamesId.push(r_data[i]["GameId"])
            }
            data_query["games"] = gamesId
          }
          /*
                gameDao.getTTLGamesCount(data_query, cb);
            },
            function (r_code, count, cb) {
                if (r_code.code != code.OK) {
                    next(null, { code: code.GAME.GAME_COUNTS_FAIL, data: null });
                    return;
                }
                */
          gameDao.getGames_admin(data_query, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.GAME_LOAD_FAIL,
              data: null,
            })
            return
          }
          ttlCount = r_data.count

          games = r_data["info"]

          games.forEach((item) => {
            gamesId.push(item.gameId)
          })

          var param = {
            gamesId: msg.data.gamesId,
          }

          gameDao.get_game_currency_denom_setting(param, cb) //取遊戲各幣別denom
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.GAME_LOAD_FAIL,
              data: null,
            })
            return
          }
          gamesDenom = r_data

          gameDao.getGamesImage_admin(
            {
              gameId: gamesId,
            },
            cb
          ) //圖片
        },
        function (r_code, r_data, cb) {
          gamesImages = r_data
          self.app.rpc.config.configRemote.getRTPs(session, cb) //全部RTP
        },
        function (r_code, r_rtps, cb) {
          rtps = r_rtps
          self.app.rpc.config.configRemote.getCompany(session, cb)
          //gameDao.getCompany(cb);
        },
      ],
      function (none, r_code, r_company) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.GAME_LOAD_FAIL,
            data: null,
          })
          return
        }
        var company = Object.assign([], r_company)
        var info = []
        for (var i = 0; i < games.length; i++) {
          var game_data = games[i]
          var tmp_rtps = game_data.rtps.split(",") //預選RTP  [ '6' ]
          var count = 0,
            count_1 = 0
          var update_rtps = []

          // if (tmp_rtps.length > 1) { // 大於 1 即是有做過修改 [ '5', '2' ] -> [ 原始設定的值, 修改過後的值 ]
          //     tmp_rtps = tmp_rtps[1];
          // }

          for (count = 0; count < tmp_rtps.length; count++) {
            for (count_1 = 0; count_1 < rtps.length; count_1++) {
              if (Number(tmp_rtps[count]) === Number(rtps[count_1].Id) /*&& update_rtps.length == 0*/) {
                update_rtps.push(rtps[count_1])
                break
              }
            }
          }
          games[i].rtps = Object.assign([], update_rtps)

          games[i]["image"] = gamesImages
            .filter((img) => game_data.gameId == img.GameId)
            .map(function (img, index, array) {
              return {
                imageType: img.ImageType,
                platformType: img.PlatformType,
                imageName: img.ImageName,
                fileName: img.FileName,
              }
            })
          console.log("tag_start_date-before:", games[i]["tag_start_date"])

          games[i]["tag_start_date"] =
            games[i]["tag_start_date"] == "0000-00-00 00:00:00" ? "" : timezone.LocalToUTC(games[i]["tag_start_date"])
          games[i]["tag_end_date"] =
            games[i]["tag_end_date"] == "0000-00-00 00:00:00" ? "" : timezone.LocalToUTC(games[i]["tag_end_date"])
          console.log("tag_start_date-after:", games[i]["tag_start_date"])

          var company_data = company.filter((item) => parseInt(item.Id) == parseInt(game_data.company))

          games[i]["comapnyName"] = company_data.length > 0 ? company_data[0]["Value"] : ""

          //---------denom-----------------
          //             var denoms = gamesDenom.filter(denom =>
          //                 game_data.gameId == denom.GameId
          //             );
          //             var game_denom = denoms.map(denom => {
          //                 return {
          //                     currency: denom.Currency,
          //                     value: denom.Denom
          //                 }
          //             });
          //             games[i]['denoms'] = game_denom;

          // {currency: "CNY", value: "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17"}
          // game_data: 查詢 games、game_group、game_tag 所得資料
          // gamesDenom: 查詢 game_currency_denom_setting 所得資料
          var game_denoms = gamesDenom
            .filter((denom) => game_data.gameId == denom.GameId && denom.Currency == sys_config.main_currency)
            .map((item) => {
              return {
                currency: item.Currency,
                value: item.Denom,
              }
            })

          games[i].denoms = Object.assign([], game_denoms)

          info.push(games[i])
        }

        curPage = data_query.curPage
        pageCount = data_query.pageCount

        next(null, {
          code: code.OK,
          data: {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: curPage,
            page_count: pageCount,
            games: info,
            DC: DC, //匯出分銷商增加DC
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[gameHandler][getGames] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.modifyGame = function (msg, session, next) {
  let addLogTimes = 0 //確認編輯遊戲寫log次數

  try {
    if (typeof msg.data.gameId === "undefined") {
      next(null, {
        code: code.GAME.GAME_PARA_FAIL,
        data: null,
      })
      return
    }
    msg.data.mathId = msg.data.math
    var game = {}
    var self = this
    var logData = {}

    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "Game"
    logData["FunctionAction"] = "EditGame"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""

    var log_mod_before = []
    var log_mod_after = []
    //var userSession = {};

    m_async.waterfall(
      [
        function (cb) {
          gameDao.getGame_ByGameId(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.GAME_NOT_EXIST,
              data: null,
            })
            return
          }

          game = r_data

          var info = {
            games: r_data,
          }
          var before_log = []
          before_log.push(info)

          log_mod_before = log_mod_before.concat(before_log)

          gameDao.getGameImage_ByGameId(
            {
              gameId: msg.data.gameId,
            },
            cb
          ) //before - game_image
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.GAME_IMAGE_FAIL,
              data: null,
            })
            return
          }

          var info = {
            game_image: r_data,
          }
          var before_log = []
          before_log.push(info)
          log_mod_before = log_mod_before.concat(before_log)

          //check RTPs cant modify
          var t_rtps = game.RTPs.split(",") //原本的
          var s_rtps = typeof msg.data.rtps === "undefined" ? t_rtps : msg.data.rtps.split(",") //要變更的

          // var t_denoms = game.Denoms.split(",");
          // var s_denoms = typeof msg.data.denoms === 'undefined' ? t_denoms : msg.data.denoms.split(",");

          // if (t_denoms.length > s_denoms.length) {
          //     next(null, { code: code.GAME.DENOM_CANT_MODIFY, data: null });
          //     return;
          // }

          // for (var i = 0; i < t_denoms.length; i++) {
          //     if (s_denoms[i] != t_denoms[i]) {
          //         next(null, { code: code.GAME.DENOM_CANT_MODIFY, data: null });
          //         return;
          //     }
          // }
          //start modify game info
          //遊戲標籤整理

          if (msg.data["tId"] == 0) {
            msg.data["tag_time_state"] = 0
            msg.data["tag_start_date"] = "0000-00-00 00:00:00"
            msg.data["tag_end_date"] = "0000-00-00 00:00:00"
          }

          if (msg.data["tag_time_state"] == 0) {
            msg.data["tag_start_date"] = "0000-00-00 00:00:00"
            msg.data["tag_end_date"] = "0000-00-00 00:00:00"
          }

          if (msg.data["tag_time_state"] == 1) {
            //有限時間
            msg.data["tag_start_date"] =
              typeof msg.data["tag_start_date"] == "undefined"
                ? "0000-00-00 00:00:00"
                : timezone.UTCToLocal(msg.data["tag_start_date"])
            msg.data["tag_end_date"] =
              typeof msg.data["tag_end_date"] == "undefined"
                ? "0000-00-00 00:00:00"
                : timezone.UTCToLocal(msg.data["tag_end_date"])
          }

          gameDao.modifyGame_admin(msg.data, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }

          var param = {
            gameId: msg.data.gameId,
          }

          gameDao.get_game_currency_denom_setting(param, cb) //取game 各幣別 denom
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }

          r_data.forEach((item) => {
            var t_denoms = item.Denom.split(",") //遊戲原先的設定

            var denoms = msg.data.denoms.filter((info) => info.currency == item.Currency) //這次修改的
            var s_denoms = t_denoms

            if (denoms.length > 0) {
              console.log("-----denoms--------", denoms)
              s_denoms = denoms[0]["value"].split(",")
            }
            /*  預選的 不用判斷
                                   if (t_denoms.length > s_denoms.length) {
                                       next(null, { code: code.GAME.DENOM_CANT_MODIFY, data: null });
                                       return;
                                   }
               
                                   for (var i = 0; i < s_denoms.length; i++) {
                                       if (t_denoms.indexOf(s_denoms[i]) == -1) {
                                           next(null, { code: code.GAME.DENOM_CANT_MODIFY, data: null });
                                           return;
                                       }
                                   }
                                    */
          })

          var param = {
            gameId: msg.data.gameId,
            denoms: msg.data.denoms,
          }

          gameDao.modifyGameCurrencyDenom(param, cb) //新增currency_denom
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.GAME_DENOM_CREATE_FAIL,
              data: null,
            })
            return
          }

          if (typeof msg.data.image === "undefined") {
            msg.data["image"] = []
          }

          var image_param = {
            gameId: msg.data.gameId,
            image: msg.data.image,
          }

          modifyGameImage(image_param, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.GAME_CREATE_FAIL,
              data: null,
            })
            return
          }

          gameDao.getGame_ByGameId(msg.data, cb) //after-game
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }

          var info = {
            games: r_data,
          }
          var after_log = []
          after_log.push(info)
          log_mod_after = log_mod_after.concat(after_log)

          gameDao.getGameImage_ByGameId(
            {
              gameId: msg.data.gameId,
            },
            cb
          ) //after - game_image
        },
        function (r_code, r_data, cb) {
          addLogTimes++
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          var info = {
            game_image: r_data,
          }
          var after_log = []
          after_log.push(info)
          log_mod_after = log_mod_after.concat(after_log)

          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)
          logData["AdminId"] = session.get("cid") || ""
          logData["UserName"] = session.get("usrName") || ""
          if (addLogTimes == 1) {
            //只寫入一次
            logDao.add_log_admin(logData, cb)
          }
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOG_FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[gameHandler][modifyGame] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// handler.getGameOrder = function (msg, session, next) {
//     var self = this;
//     //var userSession = {};

//     m_async.waterfall([
//         function (cb) {

//             gameDao.getGameOrder(msg.data, cb);

//         }
//     ], function (none, r_code, orders) {

//         if (r_code.code != code.OK) {
//             //console.log("------Get Game Order Error------msg:", r_code.msg);
//             next(null, {
//                 code: code.GAME.GAME_ORDER_FAIL,
//                 data: null
//             });
//             return;
//         }

//         next(null, {
//             code: code.OK,
//             data: orders
//         });
//         return;
//     });

//     /*
//       gameDao.getGameOrder(msg.data, function (none, r_code, orders) {

//           if (r_code.code != code.OK) {
//              //console.log("------Get Game Order Error------msg:", r_code.msg);
//               next(null, { code: code.GAME.GAME_ORDER_FAIL, data: null });
//           }

//           next(null, { code: code.OK, data: orders });
//       });
//   */
// };

// handler.getGameOrder_byLobby = function (msg, session, next) {
//     var self = this;
//     //var userSession = {};

//     m_async.waterfall([
//         function (cb) {

//             gameDao.getGameOrderByLobby(msg.data, cb);

//         }
//     ], function (none, r_code, orders) {

//         if (r_code.code != code.OK) {
//             //console.log("------Get Game Order Error------msg:", r_code.msg);
//             next(null, {
//                 code: r_code.code,
//                 data: null
//             });
//             return;
//         }

//         next(null, {
//             code: code.OK,
//             data: orders
//         });
//         return;
//     });

//     /*
//     gameDao.getGameOrderByLobby(msg.data, function (none, r_code, orders) {

//         if (r_code.code != code.OK) {
//            //console.log("------Get Game Order Error------msg:", r_code.msg);
//             next(null, { code: r_code.code, data: null });
//         }

//         next(null, { code: code.OK, data: orders });
//     });
// */
// };

// handler.modifyGameOrder = function (msg, session, next) {
//     var self = this;
//     var length = msg.data.games.length;

//     if (length === 0) {
//         next(null, {
//             code: code.DB.PARA_FAIL,
//             msg: "Nothing to modify"
//         }, null);
//         return;
//     }
//     //var userSession = {};

//     var logData = {};
//     logData['IP'] = msg.remoteIP;
//     logData['ModifiedType'] = 'sort';
//     logData['FunctionGroupL'] = "Game";
//     logData['FunctionAction'] = "SortGame";
//     logData['RequestMsg'] = JSON.stringify(msg);
//     logData['Desc_Before'] = '';
//     var log_mod_before = [];
//     var log_mod_after = [];

//     m_async.waterfall([
//         function (cb) {

//             gameDao.getGameOrder_User(msg.data, cb); //before
//         },
//         function (r_code, r_data, cb) {

//             if (r_code.code != code.OK) {
//                 next(null, {
//                     code: code.GAME.LOG_GAME_SORT_FAIL,
//                     data: null
//                 });
//                 return;
//             }
//             log_mod_before = log_mod_before.concat(r_data);

//             gameDao.modifyGameOrder(msg.data, cb);
//         },
//         function (r_code, cb) {
//             if (r_code.code != code.OK) {
//                 next(null, {
//                     code: code.GAME.GAME_SORT_FAIL,
//                     data: null
//                 });
//                 return;
//             }

//             gameDao.getGameOrder_User(msg.data, cb); //after
//         },
//         function (r_code, r_data, cb) {
//             if (r_code.code != code.OK) {
//                 next(null, {
//                     code: code.GAME.LOG_GAME_SORT_FAIL,
//                     data: null
//                 });
//                 return;
//             }
//             log_mod_after = log_mod_after.concat(r_data);
//             logData['Desc_Before'] = JSON.stringify(log_mod_before);
//             logData['Desc_After'] = JSON.stringify(log_mod_after);

//             if (session.get('level') == 1) {
//                 logData['AdminId'] = session.get('cid') || '';
//                 logData['UserName'] = session.get('usrName') || '';
//                 logDao.add_log_admin(logData, cb);
//             } else {
//                 logData['ActionLevel'] = session.get('level') || '';
//                 logData['ActionCid'] = session.get('cid') || '';
//                 logData['ActionUserName'] = session.get('usrName') || '';
//                 logDao.add_log_customer(logData, cb);
//             }
//         }
//     ], function (none, r_code, r_id) {
//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_FAIL,
//                 data: null
//             });
//             return;
//         }
//         next(null, {
//             code: code.OK,
//             data: "Success"
//         });
//         return;
//     });
//     /*
//     gameDao.modifyGameOrder(msg.data, function (none, r_code) {

//         if (r_code.code != code.OK) {
//            //console.log("------Get Game Order Error------msg:", r_code.msg);
//             next(null, { code: code.GAME.GAME_ORDER_MODIFY_FAIL, data: null });
//         }

//         next(null, { code: code.OK, data: "Success" });
//     });*/

// };
//for join hall use
/*
原:denoms_set -開hall時遊戲各自設定的
all_denom - 所有
denoms - 建遊戲時預選的
*/
handler.getGames_byGroup_hall = function (msg, session, next) {
  try {
    var self = this
    var games = null
    var rtps = null
    var denoms = null
    //var userSession = {};
    var company = []
    var games_self = null
    let gamesDenom = []

    const { currencies = [] } = msg.data

    // 上層的denom設定
    const upperDenoms = {}
    // 遊戲預設denom設定
    const gameDefaultDenoms = {}

    let gameIdList = []

    m_async.waterfall(
      [
        function (cb) {
          var data = {
            groups: msg.data.ggids.split(","),
            cid: typeof msg.data.cid === "undefined" ? null : msg.data.cid,
            upid: msg.data.upid,
            level: session.get("level"),
          }
          gameDao.getGames_byGroup_hall(data, cb) // 查自己的 game_setting
        },
        function (r_code, r_games, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.GAME_LOAD_FAIL,
              data: null,
            })
            return
          }
          if (msg.data.upid != -1) {
            games_self = r_games
            var params = {
              groups: msg.data.ggids.split(","),
              cid: msg.data.upid,
              upid: msg.data.upid,
              level: session.get("level"),
            }
            gameDao.getGames_byGroup_hall(params, cb) // 查上線的 game_setting
          } else {
            // 最上層 reseller 不需再去查詢上線的 game_setting
            games = r_games
            cb(null, { code: code.OK }, null)
          }
        },
        function (r_code, r_games, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.GAME_LOAD_FAIL,
              data: null,
            })
            return
          }

          if (msg.data.upid != -1) {
            games = r_games
            // 上線和自己的 game_setting 比對，遊戲是開啟或關閉狀態
            for (var i = 0; i < games.length; i++) {
              if (games_self.length == 0) {
                games[i].sw = 0
              }
              for (var j = 0; j < games_self.length; j++) {
                if (games[i].gameId === games_self[j].gameId) {
                  games[i].sw = games_self[j].sw
                  games[i].rtp_set = games_self[j].rtp_set
                  break
                } else {
                  games[i].sw = 0
                }
              }
            }
          }

          gameIdList = games.map((x) => x.gameId)

          self.app.rpc.config.configRemote.getRTPs(session, cb)
        },
        function (r_code, r_rtps, cb) {
          rtps = r_rtps

          self.app.rpc.config.configRemote.getDenoms(session, cb)
        },
        function (r_code, r_denoms, cb) {
          denoms = r_denoms //全
          self.app.rpc.config.configRemote.getCompany(session, cb)
          // gameDao.getCompany(cb);
        },
        function (r_code, r_company, cb) {
          company = Object.assign([], r_company)

          const param = {
            gamesId: msg.data.gamesId,
            currencies: currencies,
          }

          gameDao.get_game_currency_denom_setting(param, cb) //取遊戲各幣別denom
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.GAME_LOAD_FAIL,
              data: null,
            })
            return
          }
          gamesDenom = r_data

          const game_length = games.length

          // 遊戲預設denom
          for (const x of gamesDenom) {
            const { Currency, Denom, GameId } = x

            const result = {
              currency: Currency,
              value: Denom,
            }

            if (!gameDefaultDenoms[GameId]) gameDefaultDenoms[GameId] = []

            gameDefaultDenoms[GameId].push(result)
          }

          for (var i = 0; i < game_length; i++) {
            var game_data = games[i]
            //var tmp_denoms = game_data.denoms.split(",");
            var tmp_rtps = game_data.rtps.split(",")
            var count = 0,
              count_1 = 0
            // var update_denoms = []; //hall各自遊戲設定有的denom
            var update_rtps = []

            for (count = 0; count < tmp_rtps.length; count++) {
              for (count_1 = 0; count_1 < rtps.length; count_1++) {
                if (Number(tmp_rtps[count]) === Number(rtps[count_1].Id)) {
                  update_rtps.push(rtps[count_1])
                  break
                }
              }
            }

            const { gameId } = game_data

            const denom = gameDefaultDenoms[gameId]

            games[i].denoms = denom

            if (!upperDenoms[gameId]) upperDenoms[gameId] = []
            upperDenoms[gameId].push(...denom)

            games[i].rtps = update_rtps
            var company_data = company.filter((item) => parseInt(item.Id) == parseInt(game_data.company))
            games[i]["comapnyName"] = company_data.length > 0 ? company_data[0]["Value"] : ""
          }

          if (typeof msg.data.cid === "undefined") {
            //新增 hall 的init
            cb(
              null,
              {
                code: code.OK,
              },
              games
            )
          } else {
            //編輯 hall 的init

            const data = {
              gameId: gameIdList,
              cid: msg.data.upid,
              currencies: currencies,
            }

            // 上層的denom遊戲設定
            gameDao.getGames_Denom_byCid(data, cb)
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }

          if (typeof msg.data.cid !== "undefined") {
            for (const x of r_data) {
              const { Currency, Denom, GameId } = x

              const data = { currency: Currency, value: Denom }

              if (upperDenoms[GameId] && upperDenoms[GameId].find((x) => x.currency === Currency)) {
                upperDenoms[GameId] = [...upperDenoms[GameId].filter((x) => x.currency !== Currency), data]
              }

              upperDenoms[GameId].push(data)
            }
          }

          if (typeof msg.data.cid === "undefined") {
            cb(null, { code: code.OK }, games)
          } else {
            const data = {
              gameId: gameIdList,
              cid: msg.data.cid,
              currencies: currencies,
            }

            gameDao.getGames_Denom_byCid(data, cb)
          }
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: r_code.code,
            data: null,
          })
          return
        }

        const userDenom = {}

        if (typeof msg.data.cid != "undefined") {
          for (const x of r_data) {
            const { Currency, Denom, GameId } = x

            const data = { currency: Currency, value: Denom }

            if (!userDenom[GameId]) userDenom[GameId] = []

            userDenom[GameId].push(data)
          }

          const gameList = games.map((x) => {
            const { gameId } = x
            return { ...x, denoms_set: userDenom[gameId], denoms: upperDenoms[gameId] }
          })

          games = gameList
        }

        var re_games = {}
        re_games["all_denoms"] = denoms //所有denom
        re_games["all_rtps"] = rtps //所有rtp
        re_games["games"] = games
        next(null, {
          code: code.OK,
          data: re_games,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[gameHandler][getGames_byGroup_hall] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.getGames_byGroup_agent = function (msg, session, next) {
  try {
    var self = this
    var games = null
    //var userSession = {};
    var company = []
    var denoms = []
    var rtps = null
    var init_state = ""
    if (typeof msg.data.cid === "undefined" || typeof msg.data.upid === "undefined") {
      next(null, {
        code: code.GAME.GAME_QUERY_DATA_FAIL,
        data: null,
      })
      return
    }

    m_async.waterfall(
      [
        function (cb) {
          self.app.rpc.config.configRemote.getRTPs(session, cb)
        },
        function (r_code, r_rtps, cb) {
          rtps = r_rtps

          self.app.rpc.config.configRemote.getDenoms(session, cb)
        },
        function (r_code, r_data, cb) {
          denoms = r_data

          self.app.rpc.config.configRemote.getCompany(session, cb)
        },
        function (r_code, r_company, cb) {
          company = Object.assign([], r_company)

          var data = {
            show_user: "agent",
            groups: msg.data.ggids.split(","),
            cid: msg.data.upid,
          }
          gameDao.getGames_byGroup_hall(data, cb) //直接取HALL 遊戲
        },
        function (r_code, r_games, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.USR.GAME_LOAD_FAIL,
              data: null,
            })
            return
          }
          console.log("--getGames_byGroup_agent r_games--", JSON.stringify(r_games))

          games = r_games

          var gamesId = games.map((item) => item.gameId) // 各遊戲的預設denom

          var param = {
            gamesId: msg.data.gamesId,
          }

          gameDao.get_game_currency_denom_setting(param, cb) // 取遊戲各幣別denom

          /*var game_length = games.length;

                for (var i = 0; i < game_length; i++) {
                    var game_data = games[i];
                    var tmp_denoms = game_data.denoms.split(",");
                    var tmp_rtps = game_data.rtp;
                    var count = 0,
                        count_1 = 0;
                    var update_denoms = [];
                    var update_rtps = [];

                    for (count = 0; count < tmp_denoms.length; count++) {
                        for (count_1 = 0; count_1 < denoms.length; count_1++) {
                            if (Number(tmp_denoms[count]) === Number(denoms[count_1].Id)) {
                                update_denoms.push(denoms[count_1]);
                                break;
                            }
                        }
                    }

                    for (count_1 = 0; count_1 < rtps.length; count_1++) {
                        if (Number(tmp_rtps) === Number(rtps[count_1].Id)) {
                            update_rtps.push(rtps[count_1]);
                            break;
                        }
                    }

                    games[i].denoms = []; //(原)預選的denom
                    games[i].rtps = update_rtps;
                    var company_data = company.filter(item => parseInt(item.Id) == parseInt(game_data.company));

                    games[i]['comapnyName'] = (company_data.length > 0) ? company_data[0]['Value'] : '';
                }

                //-------------------------------------------------------

                //新增 找全部 ,編輯-只取幣別的denom
                if (msg.data.cid != msg.data.upid) {
                    init_state = 'edit';
                    userDao.getUserCurrency_byCid(msg.data.cid, cb);
                } else {
                    init_state = 'add';
                    cb(null, {
                        code: code.OK
                    }, null);
                }*/
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }

          gamesDenom = r_data

          var game_count = 0
          var game_length = games.length
          for (var i = 0; i < game_length; i++) {
            var game_data = games[i]
            var tmp_rtps = game_data.rtps.split(",")
            var count = 0,
              count_1 = 0
            var update_rtps = []

            for (count = 0; count < tmp_rtps.length; count++) {
              for (count_1 = 0; count_1 < rtps.length; count_1++) {
                if (Number(tmp_rtps[count]) === Number(rtps[count_1].Id)) {
                  update_rtps.push(rtps[count_1])
                  break
                }
              }
            }

            var default_denoms = gamesDenom
              .filter((denom) => game_data.gameId == denom.GameId)
              .map((item) => ({
                currency: item.Currency,
                value: item.Denom,
              }))

            games[i].denoms = default_denoms //遊戲各自預設的denom
            games[i].rtps = update_rtps
            var company_data = company.filter((item) => parseInt(item.Id) == parseInt(game_data.company))
            games[i]["comapnyName"] = company_data.length > 0 ? company_data[0]["Value"] : ""
          }

          var gameId = []
          games.forEach((item) => {
            gameId.push(item.gameId)
          })

          var param = {
            gameId: gameId,
            cid: msg.data.upid,
          }

          if (init_state == "edit") {
            param["currency"] = r_data[0]["currency"]
          }

          self.app.rpc.game.gameRemote.getGames_Denom_byCid(session, param, cb) //各遊戲設定的幣別-denom
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: r_code.code,
            data: null,
          })
          return
        }

        if (typeof msg.data.cid != "undefined") {
          var denom_set = {}
          r_data.forEach((item) => {
            var denom = {
              currency: item.Currency,
              value: item.Denom,
            }
            if (typeof denom_set[item.GameId] == "undefined") {
              denom_set[item.GameId] = Object.assign([])
            }
            denom_set[item.GameId].push(denom)
          })

          for (var i = 0; i < games.length; i++) {
            var gameId = games[i]["gameId"]
            games[i]["denoms_set"] = Object.assign([], denom_set[gameId])
          }
        }
        var re_games = {}
        re_games["all_denoms"] = denoms //所有denom
        re_games["all_rtps"] = rtps //所有rtp
        re_games["games"] = games
        next(null, {
          code: code.OK,
          data: re_games,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[gameHandler][getGames_byGroup_agent] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// handler.getBetLogs = function (msg, session, next) {
//     /*
//     config_db.getBetlog( msg.data, function( none, r_code, bets ){
//         if( r_code.code != code.OK ){
//            //console.log("------Get bet log Error------msg:");
//             next(null, { code: r_code.code, data:null });
//         }
//         next(null, {code: code.OK, data: bets });
//     } );
//     */
// };

handler.getUserJoinGames = function (msg, session, next) {
  try {
    var self = this
    var currencies = []
    var games = []
    var game_group = []
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.cid === "undefined" ||
      typeof msg.data.upid === "undefined" ||
      typeof msg.data.level === "undefined"
    ) {
      next(null, {
        code: code.GAME.GAME_QUERY_DATA_FAIL,
        data: null,
      })
      return
    }
    const level = session.get("level")
    const isSub = session.get("isSub")
    m_async.waterfall(
      [
        function (cb) {
          self.app.rpc.config.configRemote.getGameGroup(session, cb)
        },
        function (r_code, r_data, cb) {
          game_group = r_data
          if (isSub == 1) {
            if (level == 2) {
              const data = { cid: session.get("hallId"), level: "HA" }
              gameDao.getUserJoinGames(data, cb)
            } else {
              const data = { upid: session.get("agentId"), level: "AG" }
              gameDao.getUserJoinGames(data, cb)
            }
          } else {
            gameDao.getUserJoinGames(msg.data, cb)
          }
        },
        function (r_code, r_data, cb) {
          games = r_data
          switch (level) {
            case 1: //admin - 全部
              configDao.getCurrencyList(cb) //幣別
              break
            case 2: //hall - 下線(hall 資料表的currencies)
              userId = session.get("cid")
              if (session.get("isSub") == 0) {
                userDao.getHallCurrency_byCid(userId, cb)
              } else {
                //Hall Manager
                const data = {
                  cid: userId,
                }
                userDao.get_subUser_byCid(data, cb)
              }
              break
            case 3: //agent - 自己
              userId = session.get("cid")
              const data = {
                cid: userId,
              }
              if (session.get("isSub") == 0) {
                //Agent 查詢 Wallet
                gameDao.getWallet(data, cb)
              } else {
                //Agent Manager
                userDao.get_subUser_byCid(data, cb)
              }
              break
          }
        },
        function (r_code, r_currencies, cb) {
          if (r_code.code != code.OK) {
            next(null, { code: code.BET.LOAD_CURRENCY_FAIL, data: null })
            return
          }
          switch (level) {
            case 1:
              currencies = r_currencies.map((x) => x.currency)
              break
            case 2:
              if (r_currencies != "" && session.get("isSub") == 0) {
                currencies = r_currencies.split(",")
              } else {
                const [target] = r_currencies
                currencies = target.Currencies.split(",")
              }
              break
            case 3:
              if (session.get("isSub") == 0) {
                r_currencies.forEach((item) => {
                  if (item.Currency != null && item.Currency != "") currencies.push(item.Currency)
                })
              } else {
                const [target] = r_currencies
                currencies = target.Currencies.split(",")
              }
              break
          }

          cb(null, { code: code.OK }, currencies)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.GAME_CREATE_FAIL,
            data: null,
          })
          return
        }
        currencies = r_data
        next(null, {
          code: code.OK,
          data: {
            currency: currencies,
            game: games,
            game_group: game_group,
          },
        })
        return
      }
    )

    /*
        gameDao.getUserJoinGames(msg.data, function (none, r_code, games) {
            if (r_code.code != code.OK) {
                next(null, { code: code.GAME.GAME_CREATE_FAIL, data: null });
                return;
            }
            next(null, { code: code.OK, data: games });
        });
        */
  } catch (err) {
    logger.error("[gameHandler][getUserJoinGames] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//新增遊戲類別(by admin)
handler.AddGameCategory = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.nameE === "undefined" ||
      typeof msg.data.nameG === "undefined" ||
      typeof msg.data.nameC === "undefined" ||
      typeof msg.data.state === "undefined"
    ) {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }
    var logData = {}
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "add"
    logData["FunctionGroupL"] = "Game"
    logData["FunctionAction"] = "AddGameCategory"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""

    var log_mod_after = []

    m_async.waterfall(
      [
        function (cb) {
          gameDao.createGameCategory(msg.data, cb)
        },
        function (r_code, categoryId, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.GAME_CATEGORY_CREATE_FAIL,
              data: null,
            })
            return
          }

          gameDao.getGameCategory_ById(categoryId, cb) //after - game_category
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.LOG_GAME_CATEGORY_FAIL,
              data: null,
            })
            return
          }

          var info = {
            game_category: r_data,
          }
          var res_data = []
          res_data.push(info)

          log_mod_after = log_mod_after.concat(res_data)
          logData["Desc_After"] = JSON.stringify(log_mod_after)
          logData["AdminId"] = session.get("cid") || ""
          logData["UserName"] = session.get("usrName") || ""

          logDao.add_log_admin(logData, cb)
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOG_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[gameHandler][AddGameCategory] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//修改遊戲類別(by admin)
handler.ModifyGameCategory = function (msg, session, next) {
  var self = this
  //var userSession = {};

  if (typeof msg.data === "undefined" || typeof msg.data.categoryId === "undefined") {
    next(null, {
      code: code.FAIL,
      data: null,
    })
    return
  }
  var logData = {}
  logData["IP"] = msg.remoteIP
  logData["ModifiedType"] = "edit"
  logData["FunctionGroupL"] = "Game"
  logData["FunctionAction"] = "EditGameCategory"
  logData["RequestMsg"] = JSON.stringify(msg)
  logData["Desc_Before"] = ""
  var log_mod_before = []
  var log_mod_after = []

  m_async.waterfall(
    [
      function (cb) {
        gameDao.getGameCategory_ById(msg.data.categoryId, cb) //before - game_category
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.NO_GAME_CATEGORY,
            data: null,
          })
          return
        }

        if (r_data.length == 0) {
          //無此筆資料
          next(null, {
            code: code.GAME.LOG_GAME_CATEGORY_FAIL,
            data: null,
          })
          return
        }

        var info = {
          game_category: r_data,
        }
        var res_data = []
        res_data.push(info)
        log_mod_before = log_mod_before.concat(res_data)

        gameDao.modifyGameCategory(msg.data, cb)
      },
      function (r_code, categoryId, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME._MODIFY_FAIL,
            data: null,
          })
          return
        }

        gameDao.getGameCategory_ById(msg.data.categoryId, cb) //after - game_category
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOG_GAME_FAIL,
            data: null,
          })
          return
        }

        var info = {
          game_category: r_data,
        }
        var res_data = []
        res_data.push(info)
        log_mod_after = log_mod_after.concat(res_data)

        logData["Desc_Before"] = JSON.stringify(log_mod_before)
        logData["Desc_After"] = JSON.stringify(log_mod_after)
        logData["AdminId"] = session.get("cid") || ""
        logData["UserName"] = session.get("usrName") || ""

        logDao.add_log_admin(logData, cb)
      },
    ],
    function (none, r_code, r_id) {
      if (r_code.code != code.OK) {
        next(null, {
          code: code.GAME.LOG_FAIL,
          data: null,
        })
        return
      }
      next(null, {
        code: code.OK,
        data: "Success",
      })
      return
    }
  )
}

//設定使用者加入類別(by admin) categoryId-num , userId-array (ACTION)
// handler.UserAddToGameCategory = function (msg, session, next) {
//     var self = this;
//     //var userSession = {};

//     if (typeof msg.data === 'undefined' || typeof msg.data.userId === 'undefined' || typeof msg.data.categoryId === 'undefined') {
//         next(null, {
//             code: code.FAIL,
//             data: null
//         });
//         return;
//     }
//     var logData = {};
//     logData['IP'] = msg.remoteIP;
//     logData['ModifiedType'] = 'edit';
//     logData['FunctionGroupL'] = "Game";
//     logData['FunctionAction'] = "AddCategoryInUser";
//     logData['RequestMsg'] = JSON.stringify(msg);
//     logData['Desc_Before'] = '';
//     var log_mod_before = [];
//     var log_mod_after = [];

//     m_async.waterfall([function (cb) {
//         var map_data = {
//             categoryId: msg.data.categoryId
//         }

//         gameDao.getGameCategoryMap(map_data, cb); //before - game_category_map
//     }, function (r_code, r_data, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOAD_GAME_CATEGORY_FAIL,
//                 data: null
//             });
//             return;
//         }

//         var info = {
//             game_category_map: r_data
//         };
//         var res_data = [];
//         res_data.push(info);
//         log_mod_before = log_mod_before.concat(res_data);

//         var map_data = {
//             categoryId: msg.data.categoryId
//         }

//         gameDao.cleanGameCategoryMap(map_data, cb);
//     }, function (r_code, r_data, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.CLEAN_GAME_CATEGORY_MAP_FAIL,
//                 data: null
//             });
//             return;
//         }

//         var map_data = {
//             userId: msg.data.userId,
//             categoryId: [msg.data.categoryId]
//         }

//         gameDao.addGameCategoryMap(map_data, cb);
//     }, function (r_code, r_data, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.ADD_GAME_CATEGORY_MAP_FAIL,
//                 data: null
//             });
//             return;
//         }

//         var map_data = {
//             categoryId: msg.data.categoryId
//         }

//         gameDao.getGameCategoryMap(map_data, cb); //after - game_category_map
//     }, function (r_code, r_data, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_GAME_CATEGORY_FAIL,
//                 data: null
//             });
//             return;
//         }

//         var info = {
//             game_category_map: r_data
//         };
//         var res_data = [];
//         res_data.push(info);

//         log_mod_after = log_mod_after.concat(res_data);
//         logData['Desc_Before'] = JSON.stringify(log_mod_before);
//         logData['Desc_After'] = JSON.stringify(log_mod_after);

//         logData['AdminId'] = session.get('cid') || '';
//         logData['UserName'] = session.get('usrName') || '';

//         logDao.add_log_admin(logData, cb);
//     }], function (none, r_code, r_id) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_FAIL,
//                 data: null
//             });
//             return;
//         }
//         next(null, {
//             code: code.OK,
//             data: "Success"
//         });
//         return;
//     });
// };

//設定使用者加入類別(by admin) categoryId-num , userId-array (ACTION)
handler.UserAddToGameCategory_v2 = function (msg, session, next) {
  var self = this
  //var userSession = {};

  if (
    typeof msg.data === "undefined" ||
    typeof msg.data.userId === "undefined" ||
    typeof msg.data.categoryId === "undefined"
  ) {
    next(null, {
      code: code.FAIL,
      data: null,
    })
    return
  }
  var logData = {}
  logData["IP"] = msg.remoteIP
  logData["ModifiedType"] = "edit"
  logData["FunctionGroupL"] = "Game"
  logData["FunctionAction"] = "AddCategoryInUser"
  logData["RequestMsg"] = JSON.stringify(msg)
  logData["Desc_Before"] = ""
  var log_mod_before = []
  var log_mod_after = []

  m_async.waterfall(
    [
      function (cb) {
        var map_data = {
          categoryId: msg.data.categoryId,
        }

        gameDao.getGameCategoryMap(map_data, cb) //before - game_category_map
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOAD_GAME_CATEGORY_FAIL,
            data: null,
          })
          return
        }

        var info = {
          game_category_map: r_data,
        }
        var res_data = []
        res_data.push(info)
        log_mod_before = log_mod_before.concat(res_data)

        var map_data = {
          categoryId: msg.data.categoryId,
        }

        gameDao.cleanGameCategoryMap(map_data, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.CLEAN_GAME_CATEGORY_MAP_FAIL,
            data: null,
          })
          return
        }
        var addUser = msg.data.userId.filter((item) => item.state == 1).map((item) => item.Cid)
        var map_data = {
          userId: addUser,
          categoryId: [msg.data.categoryId],
        }
        gameDao.addGameCategoryMap(map_data, cb)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.ADD_GAME_CATEGORY_MAP_FAIL,
            data: null,
          })
          return
        }

        var map_data = {
          categoryId: msg.data.categoryId,
        }
        gameDao.getGameCategoryMap(map_data, cb) //after - game_category_map
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOG_GAME_CATEGORY_FAIL,
            data: null,
          })
          return
        }

        var info = {
          game_category_map: r_data,
        }
        var res_data = []
        res_data.push(info)

        log_mod_after = log_mod_after.concat(res_data)
        logData["Desc_Before"] = JSON.stringify(log_mod_before)
        logData["Desc_After"] = JSON.stringify(log_mod_after)

        logData["AdminId"] = session.get("cid") || ""
        logData["UserName"] = session.get("usrName") || ""

        logDao.add_log_admin(logData, cb)
      },
    ],
    function (none, r_code, r_id) {
      if (r_code.code != code.OK) {
        next(null, {
          code: code.GAME.LOG_FAIL,
          data: null,
        })
        return
      }
      next(null, {
        code: code.OK,
        data: "Success",
      })
      return
    }
  )
}

//設定類別加入使用者(by admin) userId-num , categoryId-array  (ACTION)
// handler.GameCategoryAddToUser = function (msg, session, next) {
//     var self = this;
//     //var userSession = {};

//     if (typeof msg.data === 'undefined' || typeof msg.data.userId === 'undefined' || typeof msg.data.categoryId === 'undefined') {
//         next(null, {
//             code: code.FAIL,
//             data: null
//         });
//         return;
//     }
//     var logData = {};
//     logData['IP'] = msg.remoteIP;
//     logData['ModifiedType'] = 'edit';
//     logData['FunctionGroupL'] = "Game";
//     logData['FunctionAction'] = "AddCategoryInUser";
//     logData['RequestMsg'] = JSON.stringify(msg);
//     logData['Desc_Before'] = '';
//     var log_mod_before = [];
//     var log_mod_after = [];

//     m_async.waterfall([function (cb) {

//         var map_data = {
//             userId: msg.data.userId
//         }

//         gameDao.getGameCategoryMap(map_data, cb); //before - game_category_map
//     }, function (r_code, r_data, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOAD_GAME_CATEGORY_FAIL,
//                 data: null
//             });
//             return;
//         }

//         var info = {
//             game_category_map: r_data
//         };
//         var res_data = [];
//         res_data.push(info);
//         log_mod_before = log_mod_before.concat(res_data);

//         var map_data = {
//             userId: msg.data.userId
//         }

//         gameDao.cleanGameCategoryMap(map_data, cb);
//     }, function (r_code, r_data, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.CLEAN_GAME_CATEGORY_MAP_FAIL,
//                 data: null
//             });
//             return;
//         }

//         var map_data = {
//             categoryId: msg.data.categoryId,
//             userId: [msg.data.userId]
//         }

//         gameDao.addGameCategoryMap(map_data, cb);
//     }, function (r_code, r_data, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.ADD_GAME_CATEGORY_MAP_FAIL,
//                 data: null
//             });
//             return;
//         }

//         var map_data = {
//             categoryId: msg.data.categoryId
//         }

//         gameDao.getGameCategoryMap(map_data, cb); //after - game_category_map
//     }, function (r_code, r_data, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_GAME_CATEGORY_FAIL,
//                 data: null
//             });
//             return;
//         }

//         var info = {
//             game_category_map: r_data
//         };
//         var res_data = [];
//         res_data.push(info);

//         log_mod_after = log_mod_after.concat(res_data);
//         logData['Desc_Before'] = JSON.stringify(log_mod_before);
//         logData['Desc_After'] = JSON.stringify(log_mod_after);

//         logData['AdminId'] = session.get('cid') || '';
//         logData['UserName'] = session.get('usrName') || '';

//         logDao.add_log_admin(logData, cb);
//     }], function (none, r_code, r_id) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_FAIL,
//                 data: null
//             });
//             return;
//         }
//         next(null, {
//             code: code.OK,
//             data: "Success"
//         });
//         return;
//     });
// };

//修改狀態 (多筆)
handler.ModifyStatusGameCategory_v2 = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};

    if (typeof msg.data === "undefined") {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }
    var categoryId = []
    var logData = {}
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "Game"
    logData["FunctionAction"] = "EditGameCategory"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    var log_mod_before = []
    var log_mod_after = []

    m_async.waterfall(
      [
        function (cb) {
          msg.data.forEach((item) => {
            categoryId.push(item.categoryId)
          })

          gameDao.getGameCategory_ById(categoryId, cb) //before - game_category
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.LOG_GAME_CATEGORY_FAIL,
              data: null,
            })
            return
          }

          var info = {
            game_category: r_data,
          }
          var res_data = []
          res_data.push(info)
          log_mod_before = log_mod_before.concat(res_data)

          gameDao.modifyGameCategoryState(msg.data, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME._MODIFY_FAIL,
              data: null,
            })
            return
          }

          gameDao.getGameCategory_ById(categoryId, cb) //after - game_category
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.LOG_GAME_CATEGORY_FAIL,
              data: null,
            })
            return
          }

          var info = {
            game_category: r_data,
          }
          var res_data = []
          res_data.push(info)
          log_mod_after = log_mod_after.concat(res_data)

          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["AdminId"] = session.get("cid") || ""
          logData["UserName"] = session.get("usrName") || ""

          logDao.add_log_admin(logData, cb)
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOG_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[gameHandler][ModifyStatusGameCategory_v2] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//修改狀態
// handler.ModifyStatusGameCategory = function (msg, session, next) {
//     var self = this;
//     //var userSession = {};

//     if (typeof msg.data === 'undefined' || typeof msg.data.categoryId === 'undefined' || typeof msg.data.state == 'undefined') {
//         next(null, {
//             code: code.FAIL,
//             data: null
//         });
//         return;
//     }

//     var logData = {};
//     logData['IP'] = msg.remoteIP;
//     logData['ModifiedType'] = 'edit';
//     logData['FunctionGroupL'] = "Game";
//     logData['FunctionAction'] = "EditGameCategory";
//     logData['RequestMsg'] = JSON.stringify(msg);
//     logData['Desc_Before'] = '';
//     var log_mod_before = [];
//     var log_mod_after = [];

//     m_async.waterfall([function (cb) {

//         gameDao.getGameCategory_ById(msg.data.categoryId, cb); //before - game_category
//     }, function (r_code, r_data, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_GAME_CATEGORY_FAIL,
//                 data: null
//             });
//             return;
//         }

//         var info = {
//             game_category: r_data
//         };
//         var res_data = [];
//         res_data.push(info);
//         log_mod_before = log_mod_before.concat(res_data);

//         var categoryInfo = {
//             categoryId: msg.data.categoryId,
//             state: msg.data.state
//         }

//         gameDao.modifyGameCategory(categoryInfo, cb);
//     }, function (r_code, categoryId, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME._MODIFY_FAIL,
//                 data: null
//             });
//             return;
//         }

//         gameDao.getGameCategory_ById(msg.data.categoryId, cb); //after - game_category
//     }, function (r_code, r_data, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_GAME_CATEGORY_FAIL,
//                 data: null
//             });
//             return;
//         }

//         var info = {
//             game_category: r_data
//         };
//         var res_data = [];
//         res_data.push(info);
//         log_mod_after = log_mod_after.concat(res_data);

//         logData['Desc_Before'] = JSON.stringify(log_mod_before);
//         logData['Desc_After'] = JSON.stringify(log_mod_after);

//         logData['AdminId'] = session.get('cid') || '';
//         logData['UserName'] = session.get('usrName') || '';

//         logDao.add_log_admin(logData, cb);
//     }], function (none, r_code, r_id) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_FAIL,
//                 data: null
//             });
//             return;
//         }
//         next(null, {
//             code: code.OK,
//             data: "Success"
//         });
//         return;
//     });
// }
/*
前端已無使用
*/
// handler.DeleteGamesInCategory = function (msg, session, next) {

//     var self = this;
//     //var userSession = {};

//     if (typeof msg.data === 'undefined' || typeof msg.data.categoryId === 'undefined' || typeof msg.data.games === 'undefined') {
//         next(null, {
//             code: code.FAIL,
//             data: null
//         });
//         return;
//     }

//     var logData = {};
//     logData['IP'] = msg.remoteIP;
//     logData['ModifiedType'] = 'delete';
//     logData['FunctionGroupL'] = "Game";
//     logData['FunctionAction'] = "DeleteGameInCategory";
//     logData['RequestMsg'] = JSON.stringify(msg);
//     logData['Desc_Before'] = '';
//     var log_mod_before = [];
//     var log_mod_after = [];
//     var delGames = []; //要刪除的遊戲
//     var addGames = []; //要新增的遊戲
//     gamesId = msg.data.games;

//     m_async.waterfall([function (cb) {

//         msg.data['level'] = session.get('level');
//         if (session.get('level') === 1) {
//             msg.data['hallId'] = -1;
//         } else {
//             var userId = 0;
//             if (session.get('isSub') == 1 && session.get('level') == 2) { //子帳號 -> hallId
//                 userId = session.get("hallId");
//             } else {
//                 userId = session.get("cid");
//             }
//             msg.data['hallId'] = userId;
//         }

//         gameDao.getGameOrder_User_v2(msg.data, cb); //before
//     }, function (r_code, r_data, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_GAME_SORT_FAIL,
//                 data: null
//             });
//             return;
//         }
//         log_mod_before = log_mod_before.concat(r_data);

//         var hall_game_order = r_data[0]['hall_game_order']; //找共有的

//         //console.log('-hall_game_order-', JSON.stringify(hall_game_order));

//         var ordersId = [];
//         for (var i in hall_game_order) {
//             ordersId.push(hall_game_order[i]['GameId']);
//             if (msg.data.games.indexOf(hall_game_order[i]['GameId']) > -1) {
//                 delGames.push(hall_game_order[i]['GameId']);
//             } //要刪除的遊戲ID
//         }
//         /*
//                 for (var i in msg.data.games) {
//                     if (delGames.indexOf(msg.data.games[i]) == -1 && ordersId.indexOf(msg.data.games[i]) == -1) {
//                         addGames.push(msg.data.games[i]); //要新增
//                     }
//                 }
//         */
//         var info = {
//             categoryId: msg.data.categoryId,
//             hallId: msg.data['hallId'],
//             addGames: addGames,
//             delGames: delGames
//         }

//         //console.log('-- DeleteGamesInCategory info --', JSON.stringify(info));
//         gameDao.modifyGameInCategory(info, cb); //刪除遊戲
//     }, function (r_code, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.MODIFY_GAME_IN_CATEGORY_FAIL,
//                 data: null
//             });
//             return;
//         }

//         gameDao.getGameOrder_User_v2(msg.data, cb); //after
//     }, function (r_code, r_data, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_GAME_SORT_FAIL,
//                 data: null
//             });
//             return;
//         }

//         log_mod_after = log_mod_after.concat(r_data);
//         logData['Desc_Before'] = JSON.stringify(log_mod_before);
//         logData['Desc_After'] = JSON.stringify(log_mod_after);

//         if (session.get('level') == 1) {
//             logData['AdminId'] = session.get('cid') || '';
//             logData['UserName'] = session.get('usrName') || '';
//             logDao.add_log_admin(logData, cb);
//         } else {
//             logData['ActionLevel'] = session.get('level') || '';
//             logData['ActionCid'] = session.get('cid') || '';
//             logData['ActionUserName'] = session.get('usrName') || '';
//             logDao.add_log_customer(logData, cb);
//         }

//     }], function (none, r_code, r_id) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_FAIL,
//                 data: null
//             });
//             return;
//         }
//         next(null, {
//             code: code.OK,
//             data: "Success"
//         });
//         return;
//     });
// }

//遊戲類別全部清單(type=1) OR單一USER的遊戲類別全部清單(type=2)
handler.GetCateList_byUser = function (msg, session, next) {
  try {
    var self = this
    var games = []

    if (typeof msg.data === "undefined") {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }
    //預設值
    if (typeof msg.data.type == "undefined" || msg.data.type == "") {
      msg.data.type = 1
    }
    if (msg.data.type == 2 && typeof msg.data.userId === "undefined") {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }

    m_async.waterfall(
      [
        function (cb) {
          msg.data["level"] = session.get("level")

          if (session.get("level") === 1) {
          } else {
            var userId = 0
            if (session.get("isSub") == 1 && session.get("level") == 2) {
              //子帳號 -> hallId
              userId = session.get("hallId")
            } else {
              userId = session.get("cid")
            }
            msg.data["hallId"] = userId
          }

          gameDao.GetCateList_byUser(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOG_GAME_CATEGORY_FAIL,
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
  } catch (err) {
    logger.error("[gameHandler][GetCateList_byUser] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//類別全部USER清單(type=1) OR單一類別的USER全部清單(type=2)
handler.GetUsersInCategory = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};
    var games = []

    if (typeof msg.data === "undefined") {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }
    //預設值
    if (typeof msg.data.type == "undefined" || msg.data.type == "") {
      msg.data.type = 1
    }

    if (typeof msg.data.start_date != "undefined" && msg.data.start_date != "") {
      msg.data.start_date = timezone.UTCToLocal(msg.data.start_date)
    }

    if (typeof msg.data.end_date != "undefined" && msg.data.end_date != "") {
      msg.data.end_date = timezone.UTCToLocal(msg.data.end_date)
    }
    /*
        if (msg.data.type == 2 && typeof msg.data.categoryId === 'undefined') {
            next(null, { code: code.FAIL, data: null });
            return;
        }*/

    m_async.waterfall(
      [
        function (cb) {
          gameDao.GetUsersInCategory(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOG_FAIL,
            data: null,
          })
          return
        }

        for (var i in r_data) {
          r_data[i]["AddDate"] = timezone.LocalToUTC(r_data[i]["AddDate"])
        }

        next(null, {
          code: code.OK,
          data: r_data,
        })
        return
      }
    )
  } catch (err) {
    logger.error("[gameHandler][GetUsersInCategory] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//hall_取排序狀態預設值
handler.GetGameDefaultSort = function (msg, session, next) {
  var self = this
  var games = []
  var userGames = [] //此類別user所有遊戲
  var catGames = [] //此類別所有遊戲
  var notCatGames = [] //未加入此類別的遊戲
  var all_games = []
  var betCounts = []
  if (
    typeof msg.data === "undefined" ||
    typeof msg.data.categoryId === "undefined" ||
    typeof msg.data.sortType === "undefined"
  ) {
    next(null, {
      code: code.FAIL,
      data: null,
    })
    return
  }

  m_async.waterfall(
    [
      function (cb) {
        msg.data["level"] = session.get("level")
        if (session.get("level") === 1) {
          msg.data["hallId"] = -1
        } else {
          var userId = 0
          if (session.get("isSub") == 1 && session.get("level") == 2) {
            //子帳號 -> hallId
            userId = session.get("hallId")
          } else {
            userId = session.get("cid")
          }
          msg.data["hallId"] = userId
        }
        if (msg.data["level"] == 1) {
          GetGameDefaultSort_byAd(msg.data, cb)
        } else {
          GetGameDefaultSort_byHa(msg.data, cb)
        }
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOG_SORT_DEFAULT_FAIL,
            data: null,
          })
          return
        }

        for (var i in r_data) {
          catGames.push(r_data[i]["GameId"])
        } //遊戲類別中的遊戲

        msg.data["catGameId"] = catGames //自訂或類別內的gameID
        var param = {
          gameId: catGames,
        }

        bettingDao.getGameHotCount(param, cb) //熱門度(注單數)
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOG_SORT_DEFAULT_FAIL,
            data: null,
          })
          return
        }
        betCounts = r_data

        if (msg.data.sortType == 2) {
          //熱門排序
          var param = Object.assign(msg.data)
          var sortGameId = betCounts.map((item) => item.GameId)
          param["sortGameId"] = sortGameId
        }

        gameDao.getGameSorting(msg.data, cb) //排序
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOG_SORT_DEFAULT_FAIL,
            data: null,
          })
          return
        }

        games = r_data

        console.log("------------games--------------", JSON.stringify(games))
        //加入count
        for (var i in games) {
          var addDate = games[i]["addDate"]
          games[i]["addDate"] = timezone.LocalToUTC(addDate)
          var hotNums = betCounts.filter((bet) => bet.GameId == games[i]["gameId"])
          games[i]["hotNums"] = hotNums.length > 0 ? hotNums[0]["COUNT"] : 0
        }
        var param = {
          hallId: msg.data["hallId"],
          catGameId: msg.data["catGameId"],
        }

        gameDao.getGames_byId(param, cb) //取未加入的遊戲ID
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code != code.OK) {
        next(null, {
          code: code.GAME.LOG_SORT_DEFAULT_FAIL,
          data: null,
        })
        return
      }
      notCatGames = r_data
      //加入count
      for (var i in notCatGames) {
        var addDate = notCatGames[i]["addDate"]
        notCatGames[i]["addDate"] = timezone.LocalToUTC(addDate)

        // notCatGames[i]['addDate'] = timezone.LocalToUTC(games[i]['addDate']);
        var hotNums = betCounts.filter((bet) => bet.GameId == notCatGames[i]["gameId"])
        notCatGames[i]["hotNums"] = hotNums.length > 0 ? hotNums[0]["COUNT"] : 0
      }

      var game_info = games.concat(notCatGames)
      var info = {
        games: game_info,
        count: game_info.length,
      }
      next(null, {
        code: code.OK,
        data: info,
      })
      return
    }
  )
}

//類別中的遊戲列表
handler.get_sortingGame_list = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};
    var games = [] //user所有遊戲
    var sortGames = [] //排序資料
    if (typeof msg.data === "undefined" || typeof msg.data.categoryId === "undefined") {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }
    m_async.waterfall(
      [
        function (cb) {
          msg.data["level"] = session.get("level")

          if (session.get("level") === 1) {
            msg.data["gameType"] = "default"
            msg.data["hallId"] = -1
            gameDao.getSortGameId_byCatId(msg.data, cb) //取此類別預設遊戲
          } else {
            msg.data["gameType"] = "user"
            var userId = 0
            if (session.get("isSub") == 1 && session.get("level") == 2) {
              //子帳號 -> hallId
              userId = session.get("hallId")
            } else {
              userId = session.get("cid")
            }
            msg.data["hallId"] = userId
            gameDao.getUserGames(msg.data, cb) //取user 擁有的遊戲
          }
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, r_code)
            return
          }

          for (var i in r_data) {
            games.push(r_data[i]["GameId"])
          }

          if (games.length == 0) {
            //無遊戲
            next(null, {
              code: code.OK,
              data: {
                games: [],
                count: 0,
              },
            })
            return
          }

          msg.data["catGameId"] = games //自訂或類別內的gameID
          gameDao.getGameSortingDetail(msg.data, cb) //排序遊戲資料
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOAD_SORT_GAME_FAIL,
            data: null,
          })
          return
        }

        sortGames = []
        for (var i in r_data) {
          sortGames.push({
            gameId: r_data[i]["GameId"],
            gameC: r_data[i]["gameC"],
            gameG: r_data[i]["gameG"],
            gameE: r_data[i]["gameE"],
            gameSort: r_data[i]["gameSort"],
            groupC: r_data[i]["groupC"],
            groupG: r_data[i]["groupG"],
            groupE: r_data[i]["groupE"],
            typeName: r_data[i]["typeName"],
            imageUrl: r_data[i]["ImageUrl"],
          })
        }

        next(null, {
          code: code.OK,
          data: {
            games: sortGames,
            count: sortGames.length,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[gameHandler][get_sortingGame_list] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
//遊戲排序
// handler.editGameSorting = function (msg, session, next) {

//     var self = this;
//     //var userSession = {};
//     var games = []; //user所有遊戲
//     var sortGames = []; //排序資料
//     if (typeof msg.data === 'undefined' || typeof msg.data.categoryId === 'undefined' || typeof msg.data.sortGames === 'undefined') {
//         next(null, {
//             code: code.FAIL,
//             data: null
//         });
//         return;
//     }

//     var logData = {};
//     logData['IP'] = msg.remoteIP;
//     logData['ModifiedType'] = 'sort';
//     logData['FunctionGroupL'] = "Game";
//     logData['FunctionAction'] = "SortGame";
//     logData['RequestMsg'] = JSON.stringify(msg);
//     logData['Desc_Before'] = '';
//     var log_mod_before = [];
//     var log_mod_after = [];

//     m_async.waterfall([function (cb) {

//         msg.data['level'] = session.get('level');
//         if (session.get('level') === 1) {
//             msg.data['gameType'] = "default";
//             msg.data['hallId'] = -1;
//         } else {
//             msg.data['gameType'] = "user";
//             var userId = 0;
//             if (session.get('isSub') == 1 && session.get('level') == 2) { //子帳號 -> hallId
//                 userId = session.get("hallId");
//             } else {
//                 userId = session.get("cid");
//             }
//             msg.data['hallId'] = userId;
//         }

//         gameDao.getGameOrder_User_v2(msg.data, cb); //before
//     }, function (r_code, r_data, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_GAME_SORT_FAIL,
//                 data: null
//             });
//             return;
//         }
//         log_mod_before = log_mod_before.concat(r_data);

//         gameDao.modifyGameOrder_v2(msg.data, cb);
//     }, function (r_code, cb) {
//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.GAME_SORT_FAIL,
//                 data: null
//             });
//             return;
//         }

//         gameDao.getGameOrder_User_v2(msg.data, cb); //after
//     }, function (r_code, r_data, cb) {
//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_GAME_SORT_FAIL,
//                 data: null
//             });
//             return;
//         }
//         log_mod_after = log_mod_after.concat(r_data);
//         logData['Desc_Before'] = JSON.stringify(log_mod_before);
//         logData['Desc_After'] = JSON.stringify(log_mod_after);

//         if (session.get('level') == 1) {

//             logData['AdminId'] = session.get('cid') || '';
//             logData['UserName'] = session.get('usrName') || '';
//             logDao.add_log_admin(logData, cb);
//         } else {

//             logData['ActionLevel'] = session.get('level') || '';
//             logData['ActionCid'] = session.get('cid') || '';
//             logData['ActionUserName'] = session.get('usrName') || '';
//             logDao.add_log_customer(logData, cb);
//         }
//     }], function (none, r_code, r_id) {
//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_FAIL,
//                 data: null
//             });
//             return;
//         }
//         next(null, {
//             code: code.OK,
//             data: "Success"
//         });
//         return;
//     });
// }

//遊戲排序
handler.editGameSorting_v2 = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};
    var games = [] //user所有遊戲
    var sortGames = [] //排序資料
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.categoryId === "undefined" ||
      typeof msg.data.sortGames === "undefined"
    ) {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }

    var logData = {}
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "sort"
    logData["FunctionGroupL"] = "Game"
    logData["FunctionAction"] = "SortGame"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    var log_mod_before = []
    var log_mod_after = []

    m_async.waterfall(
      [
        function (cb) {
          msg.data["level"] = session.get("level")
          if (session.get("level") === 1) {
            msg.data["gameType"] = "default"
            msg.data["hallId"] = -1
          } else {
            msg.data["gameType"] = "user"
            var userId = 0
            if (session.get("isSub") == 1 && session.get("level") == 2) {
              //子帳號 -> hallId
              userId = session.get("hallId")
            } else {
              userId = session.get("cid")
            }
            msg.data["hallId"] = userId
          }
          gameDao.getGameOrder_User_v2(msg.data, cb) //before
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.LOG_GAME_SORT_FAIL,
              data: null,
            })
            return
          }
          log_mod_before = log_mod_before.concat(r_data)
          var sortGames = msg.data.sortGames.filter((item) => item.state == 1)
          msg.data["sortGames"] = sortGames

          gameDao.modifyGameOrder_v2(msg.data, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.GAME_SORT_FAIL,
              data: null,
            })
            return
          }

          gameDao.getGameOrder_User_v2(msg.data, cb) //after
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.LOG_GAME_SORT_FAIL,
              data: null,
            })
            return
          }
          log_mod_after = log_mod_after.concat(r_data)
          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          if (session.get("level") == 1) {
            logData["AdminId"] = session.get("cid") || ""
            logData["UserName"] = session.get("usrName") || ""
            logDao.add_log_admin(logData, cb)
          } else {
            logData["ActionLevel"] = session.get("level") || ""
            logData["ActionCid"] = session.get("cid") || ""
            logData["ActionUserName"] = session.get("usrName") || ""
            logDao.add_log_customer(logData, cb)
          }
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOG_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[gameHandler][editGameSorting_v2] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//遊戲清單,用在加入類別
handler.getGamesInCategory = function (msg, session, next) {
  try {
    var self = this
    var data_query = {
      ggIds: [],
      typeIds: [],
      companyIds: [],
      name: "",
    }
    var tmp_games = []
    var re_games = []
    var companys = []
    var betCounts = []

    if (typeof msg.data.categoryId == "undefined" || msg.data.categoryId == "") {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }
    if (typeof msg.data.ggId != "undefined" && typeof msg.data.ggId === "string") {
      if (msg.data.ggId != "" || msg.data.ggId != "") data_query.ggIds = msg.data.ggId.split(",")
    }
    if (typeof msg.data.typeId != "undefined" && typeof msg.data.typeId === "string") {
      if (msg.data.typeId != "" || msg.data.typeId != "") data_query.typeIds = msg.data.typeId.split(",")
    }
    if (typeof msg.data.companyId != "undefined" && typeof msg.data.companyId === "string") {
      if (msg.data.companyId != "" || msg.data.companyId != "") data_query.companyIds = msg.data.companyId.split(",")
    }
    if (typeof msg.data.name != "undefined" && typeof msg.data.name === "string" && msg.data.name != "") {
      data_query.name = msg.data.name
    }
    var cid = 0
    m_async.waterfall(
      [
        function (cb) {
          data_query["level"] = session.get("level")
          if (session.get("level") === 1) {
            //admin
            cid = -1
            cb(
              null,
              {
                code: code.OK,
              },
              null
            )
          } else {
            var userId = 0
            if (session.get("isSub") == 1 && session.get("level") == 2) {
              //子帳號 -> hallId
              userId = session.get("hallId")
            } else {
              userId = session.get("cid") // hall
            }
            data_query["hallId"] = userId
            cid = userId
            gameDao.getUserGames_bySetting(data_query, cb) //hall可取得的遊戲 (不限遊戲類別)
          }
        },
        function (r_code, r_data, cb) {
          if (session.get("level") > 1) {
            var gamesId = []
            for (var i in r_data) {
              gamesId.push(r_data[i]["GameId"])
            }
            data_query["games"] = gamesId
          }

          self.app.rpc.config.configRemote.getCompany(session, cb)
        },
        function (r_code, r_data, cb) {
          companys = r_data

          data_query["categoryId"] = msg.data.categoryId
          gameDao.getGamesInCategory(data_query, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.GAME_LOAD_FAIL,
              data: null,
            })
            return
          }
          tmp_games = r_data
          // console.log('games-------------------',JSON.stringify(tmp_games));
          var gameId = tmp_games.map((item) => item.gameId)
          //取近10天內點擊數
          var param = {
            gameId: gameId,
          }
          bettingDao.getGameHotCount(param, cb) //熱門度(注單數)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.GAME_LOAD_FAIL,
            data: null,
          })
          return
        }

        betCounts = r_data
        // console.log('betCounts-------------------',JSON.stringify(betCounts));

        for (var i in tmp_games) {
          var company = companys.filter((item) => item.Id == tmp_games[i]["companyId"])
          var companyName = company.length > 0 ? company[0]["Value"] : ""
          var hotNums = betCounts.filter((item) => item.GameId == tmp_games[i]["gameId"])

          re_games.push({
            gameId: tmp_games[i]["gameId"],
            nameC: tmp_games[i]["nameC"],
            nameG: tmp_games[i]["nameG"],
            nameE: tmp_games[i]["nameE"],
            imageUrl: tmp_games[i]["imageUrl"],
            typeId: tmp_games[i]["typeId"],
            typeName: tmp_games[i]["typeName"],
            groupId: tmp_games[i]["GGId"],
            groupNameE: tmp_games[i]["groupNameE"],
            groupNameG: tmp_games[i]["groupNameG"],
            groupNameC: tmp_games[i]["groupNameC"],
            companyId: tmp_games[i]["companyId"],
            companyName: companyName,
            gameSort: tmp_games[i]["gameSort"],
            addDate: timezone.LocalToUTC(tmp_games[i]["addDate"]),
            hotNums: hotNums.length > 0 ? hotNums[0]["COUNT"] : 0,
            state: tmp_games[i]["state"],
          })
        }

        next(null, {
          code: code.OK,
          data: {
            games: re_games,
            count: re_games.length,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[gameHandler][getGamesInCategory] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//加入遊戲於類別中
/*
(session-
level
hallId
)
categoryId
gameId
 
*/
// handler.saveGames_bySorting = function (msg, session, next) {

//     var self = this;
//     //var userSession = {};

//     if (typeof msg.data === 'undefined' || typeof msg.data.categoryId === 'undefined' || typeof msg.data.games === 'undefined') {
//         next(null, {
//             code: code.FAIL,
//             data: null
//         });
//         return;
//     }

//     var logData = {};
//     logData['IP'] = msg.remoteIP;
//     logData['ModifiedType'] = 'sort';
//     logData['FunctionGroupL'] = "Game";
//     logData['FunctionAction'] = "EditGameInCate";
//     logData['RequestMsg'] = JSON.stringify(msg);
//     logData['Desc_Before'] = '';

//     var log_mod_before = [];
//     var log_mod_after = [];
//     var delGames = []; //要刪除的遊戲
//     var addGames = []; //要新增的遊戲
//     gamesId = msg.data.games;

//     m_async.waterfall([function (cb) {

//         msg.data['level'] = session.get('level');
//         if (session.get('level') === 1) {
//             msg.data['hallId'] = -1;
//         } else {
//             var userId = 0;
//             if (session.get('isSub') == 1 && session.get('level') == 2) { //子帳號 -> hallId
//                 userId = session.get("hallId");
//             } else {
//                 userId = session.get("cid");
//             }
//             msg.data['hallId'] = userId;
//         }

//         gameDao.getGameOrder_User_v2(msg.data, cb); //before
//     }, function (r_code, r_data, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_GAME_SORT_FAIL,
//                 data: null
//             });
//             return;
//         }
//         log_mod_before = log_mod_before.concat(r_data);

//         var hall_game_order = r_data[0]['hall_game_order']; //找共有的
//         var ordersId = [];
//         for (var i in hall_game_order) {
//             ordersId.push(hall_game_order[i]['GameId']);
//             if (msg.data.games.indexOf(hall_game_order[i]['GameId']) == -1) {
//                 delGames.push(hall_game_order[i]['GameId']);
//             } //要刪除的遊戲ID
//         }

//         for (var i in msg.data.games) {
//             if (delGames.indexOf(msg.data.games[i]) == -1 && ordersId.indexOf(msg.data.games[i]) == -1) {
//                 addGames.push(msg.data.games[i]); //要新增
//             }
//         }

//         var info = {
//             categoryId: msg.data.categoryId,
//             hallId: msg.data['hallId'],
//             addGames: addGames,
//             delGames: delGames
//         }
//         gameDao.modifyGameInCategory(info, cb); //刪除+新增遊戲
//     }, function (r_code, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.MODIFY_GAME_IN_CATEGORY_FAIL,
//                 data: null
//             });
//             return;
//         }

//         gameDao.getGameOrder_User_v2(msg.data, cb); //after
//     }, function (r_code, r_data, cb) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_GAME_SORT_FAIL,
//                 data: null
//             });
//             return;
//         }

//         log_mod_after = log_mod_after.concat(r_data);
//         logData['Desc_Before'] = JSON.stringify(log_mod_before);
//         logData['Desc_After'] = JSON.stringify(log_mod_after);

//         if (session.get('level') == 1) {
//             logData['AdminId'] = session.get('cid') || '';
//             logData['UserName'] = session.get('usrName') || '';
//             logDao.add_log_admin(logData, cb);
//         } else {
//             logData['ActionLevel'] = session.get('level') || '';
//             logData['ActionCid'] = session.get('cid') || '';
//             logData['ActionUserName'] = session.get('usrName') || '';
//             logDao.add_log_customer(logData, cb);
//         }

//     }], function (none, r_code, r_id) {

//         if (r_code.code != code.OK) {
//             next(null, {
//                 code: code.GAME.LOG_FAIL,
//                 data: null
//             });
//             return;
//         }
//         next(null, {
//             code: code.OK,
//             data: "Success"
//         });
//         return;
//     });
// }

handler.getListGameTag = function (msg, session, next) {
  try {
    var self = this
    //var userSession = {};

    if (typeof msg.data == "undefined") {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }

    var cid = 0
    m_async.waterfall(
      [
        function (cb) {
          gameDao.getListGameTag(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.GAME_TAG_LOAD_FAIL,
            data: null,
          })
          return
        }

        for (var i in r_data) {
          r_data[i]["modifyDate"] = timezone.LocalToUTC(r_data[i]["modifyDate"])
        }

        next(null, {
          code: code.OK,
          data: {
            tags: r_data,
            count: r_data.length,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[gameHandler][getListGameTag] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.addGameTag = function (msg, session, next) {
  try {
    var self = this

    var logData = {}
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "add"
    logData["FunctionGroupL"] = "Game"
    logData["FunctionAction"] = "AddGameTag"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    //var userSession = {};
    var log_mod_after = []

    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.name === "undefined" ||
      typeof msg.data.tagE === "undefined" ||
      typeof msg.data.tagG === "undefined" ||
      typeof msg.data.tagC === "undefined" ||
      typeof msg.data.textColor === "undefined" ||
      typeof msg.data.bgColor === "undefined" ||
      typeof msg.data.state === "undefined"
    ) {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }

    m_async.waterfall(
      [
        function (cb) {
          gameDao.addGameTag(msg.data, cb)
        },
        function (r_code, r_tId, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.GAME_TAG_CREATE_FAIL,
              data: null,
            })
            return
          }
          gameDao.getGameTag_ByTid(
            {
              tId: r_tId,
            },
            cb
          ) //after - game_tag
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.LOG_GAME_TAG_FAIL,
              data: null,
            })
            return
          }

          var after_log = {}
          after_log["game_tag"] = [r_data]
          log_mod_after = log_mod_after.concat(after_log)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["AdminId"] = session.get("cid") || ""
          logData["UserName"] = session.get("usrName") || ""

          logDao.add_log_admin(logData, cb)
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOG_FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[gameHandler][addGameTag] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

handler.editGameTag = function (msg, session, next) {
  try {
    var self = this
    var logData = {}
    logData["IP"] = msg.remoteIP
    logData["ModifiedType"] = "edit"
    logData["FunctionGroupL"] = "Game"
    logData["FunctionAction"] = "EditGameTag"
    logData["RequestMsg"] = JSON.stringify(msg)
    logData["Desc_Before"] = ""
    //var userSession = {};
    var log_mod_before = []
    var log_mod_after = []

    if (typeof msg.data === "undefined" || typeof msg.data.tId === "undefined") {
      next(null, {
        code: code.FAIL,
        data: null,
      })
      return
    }

    m_async.waterfall(
      [
        function (cb) {
          gameDao.getGameTag_ByTid(msg.data, cb) //before - game_tag
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.LOG_GAME_TAG_FAIL,
              data: null,
            })
            return
          }

          var before_log = {}
          before_log["game_tag"] = [r_data]
          log_mod_before = log_mod_before.concat(before_log)

          gameDao.editGameTag(msg.data, cb)
        },
        function (r_code, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.GAME_TAG_CREATE_FAIL,
              data: null,
            })
            return
          }
          gameDao.getGameTag_ByTid(msg.data, cb) //after - game_tag
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.LOG_GAME_TAG_FAIL,
              data: null,
            })
            return
          }

          var after_log = {}
          after_log["game_tag"] = [r_data]
          log_mod_after = log_mod_after.concat(after_log)

          logData["Desc_Before"] = JSON.stringify(log_mod_before)
          logData["Desc_After"] = JSON.stringify(log_mod_after)

          logData["AdminId"] = session.get("cid") || ""
          logData["UserName"] = session.get("usrName") || ""

          logDao.add_log_admin(logData, cb)
        },
      ],
      function (none, r_code, r_id) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOG_FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[gameHandler][editGameTag] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// handler.getGameGroup  = function (msg, session, next) {

//     var self = this;
//     var companys=[];
//     var denoms=[];
//     var rtps = [];
//     var maths = [];
//     var orderTypes = [];
//     var types=[];
//     var groups=[];

//    m_async.waterfall([
//        function (cb) {

//         configDao.getGameGroup_2( cb);
//     }, function (r_code, r_data, cb) {
//         groups = r_data;

//         configDao.getGameType_2( cb);
//     }, function (r_code, r_data, cb) {
//         types = r_data;

//         configDao.getCompany_2( cb);
//     },function (r_code, r_data, cb) {
//         companys = r_data;

//        configDao.getDenoms_2( cb);
//     },function (r_code, r_data, cb) {
//         denoms = r_data;

//        configDao.getRTPs_2( cb);
//     },function (r_code, r_data, cb) {
//         rtps = r_data;

//         configDao.getMathRng_2( cb);
//     },function (r_code, r_data, cb) {
//         maths = r_data;

//        configDao.getOrderType_2( cb);
//     },function (r_code, r_data, cb) {
//         orderTypes = r_data;

//         var params = {
//             state: '1'
//         }

//         gameDao.getListGameTag(params, cb);

//     }], function (none, r_code, r_tags) {

//         for (var i in r_tags) {
//             r_tags[i]['modifyDate'] = timezone.LocalToUTC(r_tags[i]['modifyDate']);
//         }
//         next(null, {
//             code: code.OK, data: {
//                 companys: companys,
//                 rtps: rtps,
//                 denoms: denoms,
//                 types: types,
//                 groups: groups,
//                 maths: maths,
//                 orderTypes: orderTypes,
//                 tags: r_tags
//             }
//         });

//     });
// }

function GetGameDefaultSort_byAd(data, callback) {
  data["gameType"] = "default"
  m_async.waterfall(
    [
      function (cb) {
        gameDao.getSortGameId_byCatId(data, cb)
      },
    ],
    function (none, r_code, r_data) {
      callback(none, r_code, r_data)
    }
  )
}

function GetGameDefaultSort_byHa(data, callback) {
  var games = []
  m_async.waterfall(
    [
      function (cb) {
        gameDao.getUserGames(data, cb) //取user 有的遊戲
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          callback(null, r_code)
          return
        }

        for (var i in r_data) {
          games.push(r_data[i]["GameId"])
        }

        if (games.length == 0) {
          //無遊戲
          callback(null, {
            code: code.OK,
            data: {
              games: [],
              count: 0,
            },
          })
          return
        }
        data["gameId"] = games
        data["gameType"] = "user"
        gameDao.getSortGameId_byCatId(data, cb) //取user自行排序的資料
      },
      function (r_code, r_data, cb) {
        if (r_code.code != code.OK) {
          callback(null, r_code)
          return
        }
        if (r_data.length == 0) {
          data["gameType"] = "default"
          gameDao.getSortGameId_byCatId(data, cb) //user無行排序 找預設
        } else {
          cb(
            null,
            {
              code: code.OK,
            },
            r_data
          )
        }
      },
    ],
    function (none, r_code, r_data) {
      if (r_code.code != code.OK) {
        callback(null, r_code)
        return
      }
      callback(none, r_code, r_data)
    }
  )
}

//圖片上傳要加入的欄位
function modifyGameImage(data, callback) {
  try {
    if (typeof data.image === "undefined" || data.image.length == 0) {
      callback(null, {
        code: code.OK,
      })
    }

    if (data.gameId == 0) {
      callback(null, {
        code: code.DB.PARA_FAIL,
      })
    }

    m_async.waterfall(
      [
        function (cb) {
          gameDao.modifyGameImage(data, cb)
        },
      ],
      function (none, r_code) {
        if (r_code.code != code.OK) {
          callback(null, r_code)
          return
        }
        callback(none, r_code)
      }
    )
  } catch (err) {
    logger.error("[gameHandler][modifyGameImage] catch err", err)
    callback(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

// var getGameSorting_byHot = function (data, cb) {

//     var sortGameId =  data.betCounts.map(item=> item.GameId );
//     data['sortGameId'] = sortGameId;
//     m_async.waterfall([function (cb) {
//         game.getGameSorting_byHot
//     }],function (none, r_code) {

//     });
// }
