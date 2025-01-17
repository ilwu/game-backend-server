const logger = require("pomelo-logger").getLogger("gameDao", __filename)
const code = require("../util/code")
const conf = require("../../config/js/conf")
const pomelo = require("pomelo")
const m_async = require("async")
const db = require("../util/DB")
const sprintf = require("sprintf-js").sprintf
const timezone = require("../util/timezone")

const { inspect } = require("util")

const gameDao = module.exports
// 新增遊戲
gameDao.createGame = function (data, cb) {
  try {
    const denom_text = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17"
    const now = timezone.serverTime()

    const sql = `INSERT INTO games
        ( GGId, Sw, IsJPEnabled, NameC, NameG, NameE, NameTH, NameVN, NameID, NameMY, NameJP, NameKR, GameUrlH5, GameId, GameUrlDesc, Company, MinBet, RTPs, Denoms,
         MathId, TypeId, Tid, TagTimeState, TagStartDate, TagEndDate, GameIP, Reel_X, Reel_Y, CreateDate, ModifyDate, 
         DescC, DescG, DescE, DescVN, DescTH, DescID, DescMY, DescJP, DescKR ) 
         VALUES ( ?, ?, ?, ?, ?, ?,?,?, ?,?, ?,?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
      `
    const args = [
      data.ggId,
      data.sw,
      data.isJPEnable,
      data.nameC,
      data.nameG,
      data.nameE,
      data.nameTH,
      data.nameVN,
      data.nameID,
      data.nameMY,
      data.nameJP,
      data.nameKR,
      data.url,
      data.gameId,
      "",
      data.company,
      data.minBet,
      data.rtps,
      denom_text,
      data.typeId,
      data.tId,
      data.tag_time_state,
      data.tag_start_date,
      data.tag_end_date,
      data.gameIP,
      data.reel_x,
      data.reel_y,
      now,
      now,
      data.descC,
      data.descG,
      data.descE,
      data.descVN,
      data.descTH,
      data.descID,
      data.descMY,
      data.descJP,
      data.descKR,
    ]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, data.gameId)
      }
    })
  } catch (err) {
    logger.error("[gameDao][createGame] catch err", err)
    cb(null, code.FAIL, null)
  }
}

gameDao.deleteGame = (data, cb) => {
  try {
    const sqlQuery = [
      `DELETE FROM game_currency_denom_setting WHERE GameId=${data.gameId}`,
      `DELETE FROM games WHERE gameId=${data.gameId}`,
      `DELETE FROM game_setting WHERE gameId=${data.gameId}`,
    ]

    db.act_transaction("dbclient_g_rw", sqlQuery, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, { code: code.DB.DELETE_FAIL })
      } else {
        cb(null, { code: code.OK, msg: "" })
      }
    })
  } catch (err) {
    logger.error("[gameDao][deleteGame] catch err", err)
    cb(null, code.FAIL, null)
  }
}

gameDao.modifyGameCurrencyDenom = function (data, cb) {
  try {
    var sql_query = []

    data.denoms.forEach((item) => {
      var sql = sprintf(
        "INSERT game_currency_denom_setting (GameId,Currency,Denom) VALUES ('%s','%s','%s') ON DUPLICATE KEY UPDATE Denom = '%s'; ",
        data.gameId,
        item.currency,
        item.value,
        item.value
      )
      sql_query.push(sql)
    })

    db.act_transaction("dbclient_g_rw", sql_query, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, { code: code.DB.CREATE_FAIL })
      } else {
        cb(null, { code: code.OK, msg: "" })
      }
    })
  } catch (err) {
    logger.error("[gameDao][modifyGameCurrencyDenom] catch err", err)
    cb(null, code.FAIL, null)
  }
}

gameDao.getGame_ByGameId = function (data, cb) {
  try {
    var sql = "SELECT * FROM games WHERE GameId = ?"
    var args = [data.gameId]

    console.log("-getGame_ByGameId -", sql, JSON.stringify(args))

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_data && r_data.length === 1) {
        var game = r_data[0]
        cb(null, { code: code.OK, msg: "" }, game)
      } else {
        cb(null, { code: code.DB.DATA_EMPTY, msg: "Invalid Game" }, null)
      }
    })

    // pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
    //     if (err) {
    //         cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
    //         return;
    //     }
    //     connection.query(sql, args, function (err, res) {
    //         connection.release();
    //         if (err) {
    //             cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
    //         } else {
    //             if (res && res.length === 1) {
    //                 var game = res[0];
    //                 cb(null, { code: code.OK, msg: "" }, game);
    //             } else {
    //                 cb(null, { code: code.DB.DATA_EMPTY, msg: "Invalid Game" }, null);
    //             }
    //         }
    //     });
    // });
  } catch (err) {
    logger.error("[gameDao][getGame_ByGameId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

gameDao.joinGameToHall = function (data, cb) {
  try {
    if (typeof data === "undefined" /*|| data.games.length === 0*/) {
      cb({ code: code.DB.CREATE_FAIL, msg: "" })
    }
    var count = 0
    var length = data.games.length
    var sql_insert = []
    var autoSpinJson =
      '{"showLimits":[true,true,true,true,true],"spins":[10,50,100,500,999,-1],"jackpot":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"single":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"loss":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"bet":[80000000,1000000,500000,100000,50000,10000,5000,1000,500]}'
    var exchangeJson = '{"exCredit":true,"exDenom":true}'
    var denomsRunning = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17"
    /*
            for (count = 0; count < length; count++) {
                var insert_text = sprintf("('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')", data.games[count].gameId, data.user.cid, data.user.upid, data.user.hallid, data.games[count].rtps, data.games[count].denoms, data.games[count].denoms, (data.games[count].sw ? 1 : 0), 1, 1, 1, autoSpinJson);
                sql_insert.push(insert_text);
            }
            var sql = 'INSERT INTO game_setting ( GameId,Cid,Upid,Hallid,RTPid,DenomsSetting,DenomsRunning,Sw ,AutoSpinEnable,ExchangeEnable,JackpotEnable,AutoSpinJson)  VALUES ' + sql_insert.join(",");
          */
    const gameDenomList = []

    for (count = 0; count < length; count++) {
      const { sw, gameId, denoms, rtps } = data.games[count]

      // 只新增有開的遊戲
      if (sw) {
        const insert_text = sprintf(
          "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')",
          gameId,
          data.user.cid,
          data.user.upid,
          data.user.hallid,
          rtps,
          sw ? 1 : 0,
          1,
          1,
          1,
          autoSpinJson,
          exchangeJson,
          denomsRunning,
          denomsRunning
        )

        sql_insert.push(insert_text)

        for (const x of denoms) {
          const gameDenomSql = `INSERT INTO game_denom_setting(Cid, GameId, Currency, Denom) VALUES("${data.user.cid}","${gameId}","${x.currency}","${x.value}") ON DUPLICATE KEY UPDATE Denom = "${x.value}"`
          gameDenomList.push(gameDenomSql)
        }
      }
    }

    var sql_query = []
    if (sql_insert.length > 0) {
      var sql =
        "INSERT INTO game_setting ( GameId,Cid,Upid,Hallid,RTPid,Sw,AutoSpinEnable,ExchangeEnable,JackpotEnable,AutoSpinJson ,ExchangeJson, DenomsSetting, DenomsRunning)  VALUES " +
        sql_insert.join(",")
      sql_query.push(sql)
    }
    sql_query = sql_query.concat(gameDenomList)

    db.act_transaction("dbclient_g_rw", sql_query, function (r_code) {
      if (r_code.code !== code.OK) {
        cb(null, { code: code.DB.CREATE_FAIL })
      } else {
        cb(null, { code: code.OK, msg: "" })
      }
    })
  } catch (err) {
    logger.error("[gameDao][joinGameToHall] catch err", inspect(err))
    cb(null, code.FAIL, null)
  }
}

// gameDao.joinGameToAgent = function (data, cb) {

//     if (typeof data === 'undefined') {
//         cb(null, { code: code.DB.CREATE_FAIL, msg: '' });
//         return;
//     }
//     if (data.games.length === 0) {
//         cb(null, { code: code.OK, msg: '' });
//         return;
//     }

//     var count = 0;
//     var length = data.games.length;
//     var sql_insert = [];

//     var autoSpinJson = '{"showLimits":[true,true,true,true,true],"spins":[10,50,100,500,999,-1],"jackpot":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"single":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"loss":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"bet":[80000000,1000000,500000,100000,50000,10000,5000,1000,500]}';

//     for (count = 0; count < length; count++) {
//         var sql_insert_text = sprintf("('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')", data.games[count].gameId, data.user.cid, data.user.upid, data.user.hallid, -1, "", "", (data.games[count].sw ? 1 : 0), 1, 1, 1, autoSpinJson);
//         sql_insert.push(sql_insert_text);
//     }

//     var sql = 'INSERT INTO game_setting ( GameId,Cid,Upid,Hallid,RTPid,DenomsSetting,DenomsRunning,Sw,AutoSpinEnable,ExchangeEnable,JackpotEnable ,AutoSpinJson) ' +
//         'VALUES ' + sql_insert.join(",");

//     var args = [];
//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.query(sql, args, function (err, res) {
//             connection.release();
//             if (err) {
//                 cb(null, { code: code.DB.CREATE_FAIL, msg: err.stack });
//             } else {
//                 cb(null, { code: code.OK, msg: "" });
//             }
//         });
//     });

// };

gameDao.modifyGame_admin = function (data, cb) {
  try {
    var sql_update = []
    if (typeof data.ggId != "undefined") {
      sql_update.push(" GGId = " + data.ggId)
    }
    if (typeof data.sw != "undefined") {
      sql_update.push(" Sw = " + data.sw)
    }
    if (typeof data.isJPEnable != "undefined") {
      sql_update.push(" IsJPEnabled = " + data.isJPEnable)
    }
    if (typeof data.nameC != "undefined") {
      sql_update.push(' NameC = "' + data.nameC + '"')
    }
    if (typeof data.nameG != "undefined") {
      sql_update.push(' NameG = "' + data.nameG + '"')
    }
    if (typeof data.nameE != "undefined") {
      sql_update.push(' NameE = "' + data.nameE + '"')
    }
    if (typeof data.nameTH != "undefined") {
      sql_update.push(' NameTH = "' + data.nameTH + '"')
    }
    if (typeof data.nameVN != "undefined") {
      sql_update.push(' NameVN = "' + data.nameVN + '"')
    }
    if (typeof data.nameID != "undefined") {
      sql_update.push(' NameID = "' + data.nameID + '"')
    }
    if (typeof data.nameMY != "undefined") {
      sql_update.push(' NameMY = "' + data.nameMY + '"')
    }
    if (typeof data.nameJP != "undefined") {
      sql_update.push(' NameJP = "' + data.nameJP + '"')
    }
    if (typeof data.nameKR != "undefined") {
      sql_update.push(' NameKR = "' + data.nameKR + '"')
    }
    // 遊戲描述 簡體中文
    if (typeof data.descC != "undefined") {
      sql_update.push(' DescC = "' + data.descC + '"')
    }
    // 遊戲描述 繁體中文
    if (typeof data.descG != "undefined") {
      sql_update.push(' DescG = "' + data.descG + '"')
    }
    // 遊戲描述 英文
    if (typeof data.descE != "undefined") {
      sql_update.push(' DescE = "' + data.descE + '"')
    }
    // 遊戲描述 越文
    if (typeof data.descVN != "undefined") {
      sql_update.push(' DescVN = "' + data.descVN + '"')
    }
    // 遊戲描述 泰文
    if (typeof data.descTH != "undefined") {
      sql_update.push(' DescTH = "' + data.descTH + '"')
    }
    // 遊戲描述 印尼文
    if (typeof data.descID != "undefined") {
      sql_update.push(' DescID = "' + data.descID + '"')
    }
    // 遊戲描述 馬文
    if (typeof data.descMY != "undefined") {
      sql_update.push(' DescMY = "' + data.descMY + '"')
    }
    // 遊戲描述 日文
    if (typeof data.descJP != "undefined") {
      sql_update.push(' DescJP = "' + data.descJP + '"')
    }
    // 遊戲描述 韓文
    if (typeof data.descKR != "undefined") {
      sql_update.push(' DescKR = "' + data.descKR + '"')
    }

    //遊戲載入的URL
    if (typeof data.gameIP != "undefined") {
      sql_update.push(' GameIP = "' + data.gameIP + '"')
    }
    if (typeof data.desc != "undefined") {
      sql_update.push(' GameUrlDesc = "' + data.desc + '"')
    }
    if (typeof data.company != "undefined") {
      sql_update.push(' Company = "' + data.company + '"')
    }
    if (typeof data.rtps != "undefined") {
      sql_update.push(' RTPs = "' + data.rtps + '"')
    }
    if (typeof data.denoms != "undefined") {
      var denom_text = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17"
      // if (denoms.length > 0) {
      //     denom_text = denoms[0]['value'];
      // } else { //測試
      //     denom_text = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17";
      // }
      sql_update.push(' Denoms = "' + denom_text + '"')
    }

    // if (typeof data.denoms != 'undefined') {
    //     sql_update.push(' Denoms = "' + data.denoms + '"');
    // }
    if (typeof data.typeId != "undefined") {
      sql_update.push(" TypeId = '" + data.typeId + "'")
    }
    // if (typeof data.mathId != 'undefined') {
    //     sql_update.push(' MathId = ' + data.mathId);
    // }
    if (typeof data.reel_x != "undefined") {
      sql_update.push(" Reel_X = '" + data.reel_x + "'")
    }

    if (typeof data.reel_y != "undefined") {
      sql_update.push(" Reel_Y = '" + data.reel_y + "'")
    }
    //MinBet
    if (typeof data.minBet != "undefined") {
      sql_update.push(" MinBet = '" + data.minBet + "'")
    }

    //標籤
    if (typeof data.tId != "undefined") {
      sql_update.push(" Tid = '" + data.tId + "'")
    }

    //標籤時間限制
    if (typeof data.tag_time_state != "undefined") {
      sql_update.push(" TagTimeState = '" + data.tag_time_state + "'")
    }

    //標籤起日
    if (typeof data.tag_start_date != "undefined") {
      sql_update.push(" TagStartDate = '" + data.tag_start_date + "'")
    }

    //標籤迄日
    if (typeof data.tag_end_date != "undefined") {
      sql_update.push(" TagEndDate = '" + data.tag_end_date + "'")
    }

    //專案代號
    if (typeof data.gameId != "undefined") {
      sql_update.push(" GameId = " + data.gameId)
    }

    //修改時間
    var now = timezone.serverTime()
    sql_update.push(' ModifyDate = "' + now + '"')

    if (sql_update.length === 0) {
      cb(null, { code: code.OK, msg: "" })
      return
    }

    var sql = "UPDATE games SET" + sql_update.join(",") + " WHERE GameId = " + data.gameId
    var args = []

    pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()
        if (err) {
          cb(null, { code: code.DB.UPDATE_FAIL, msg: err.stack })
        } else {
          cb(null, { code: code.OK, msg: "" })
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][modifyGame_admin] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// gameDao.modifyGame_agent = function (data, cb) {

//     if (typeof data === 'undefined' || typeof data.games === 'undefined' || typeof data.cid === 'undefined') {
//         cb(null, { code: code.DB.PARA_FAIL }, null);
//     }
//     var data_games = data.games;
//     var count = 0;
//     var length = data_games.length;
//     var sql_insert = [];
//     var sql_update = [];
//     var sql_update_text = "";
//     var args = [];
//     var sql_rtp = '';
//     var sql_denom_set = '';
//     var sql_sw = '';

//     var autoSpinJson = '{"showLimits":[true,true,true,true,true],"spins":[10,50,100,500,999,-1],"jackpot":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"single":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"loss":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"bet":[80000000,1000000,500000,100000,50000,10000,5000,1000,500]}';

//     for (count = 0; count < length; count++) {

//         if (typeof data_games[count].gameId === 'undefined') {
//             cb(null, { code: code.DB.PARA_FAIL }, null);
//         }

//         if (typeof data_games[count].sw != 'undefined') {
//             if (sql_sw === '') {
//                 sql_sw += 'Sw = CASE ';
//             }
//             sql_sw += 'WHEN GameId = ' + data_games[count].gameId + ' AND Cid = "' + data.cid + '" THEN ' + (data_games[count].sw ? 1 : 0) + ' ';
//             if (count === length - 1) {
//                 sql_sw += ' ELSE Sw END';
//             }
//         }

//         var gameId = typeof data_games[count].gameId != 'undefined' ? data_games[count].gameId : -1;
//         var cid = typeof data.cid != 'undefined' ? data.cid : '-1';
//         var upid = typeof data.upid != 'undefined' ? data.upid : '-1';
//         var hallid = typeof data.hallid != 'undefined' ? data.hallid : '-1';
//         var rtp = -1;
//         var sw = typeof data_games[count].sw != 'undefined' ? (data_games[count].sw ? 1 : 0) : 0;

//         var insert_text = sprintf("('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')", gameId, cid, upid, hallid, rtp, '', '', sw, 1, 1, 1, autoSpinJson);
//         sql_insert.push(insert_text);
//     }

//     if (sql_rtp != '') {
//         sql_update.push(sql_rtp);
//     }
//     if (sql_denom_set != '') {
//         sql_update.push(sql_denom_set);
//     }
//     if (sql_sw != '') {
//         sql_update.push(sql_sw);
//     }

//     if (sql_update.length > 0) {
//         sql_update_text = ' ON DUPLICATE KEY UPDATE ' + sql_update.join(",");
//     }

//     var sql = 'INSERT INTO game_setting ( GameId,Cid,Upid,HallId,RTPid,DenomsSetting,DenomsRunning,Sw,AutoSpinEnable,ExchangeEnable,JackpotEnable ,AutoSpinJson ) ' +
//         'VALUES ' + sql_insert.join(",") + sql_update_text;

//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.query(sql, args, function (err, results) {
//             connection.release();
//             if (err) {
//                 cb(null, { code: code.DB.UPDATE_FAIL, msg: err });
//                 return;
//             }

//             cb(null, { code: code.OK, msg: "" });
//         });
//     });

// };

// gameDao.getTTLGamesCount = function (data, cb) {

//     var sql_query = '';
//     var count = 0, length = 0;

//     if (data.ggIds.length != 0) {

//         if (sql_query === '') sql_query = 'WHERE ';
//         sql_query += 'GGId IN (';

//         length = data.ggIds.length;
//         for (count = 0; count < length; count++) {
//             sql_query += data.ggIds[count];
//             if (count != length - 1) {
//                 sql_query += ',';
//             }
//         }
//         sql_query += ' ) ';
//     }
//     if (data.typeIds.length != 0) {
//         if (sql_query === '') sql_query = 'WHERE ';
//         else sql_query += 'AND ';

//         sql_query += 'TypeId IN (';
//         length = data.typeIds.length;
//         for (count = 0; count < length; count++) {
//             sql_query += data.typeIds[count];
//             if (count != length - 1) {
//                 sql_query += ',';
//             }
//         }
//         sql_query += ') ';
//     }
//     if (data.companyIds.length != 0) {
//         if (sql_query === '') sql_query = 'WHERE ';
//         else sql_query += 'AND ';

//         sql_query += 'Company IN (';
//         length = data.companyIds.length;
//         for (count = 0; count < length; count++) {
//             sql_query += data.companyIds[count];
//             if (count != length - 1) {
//                 sql_query += ',';
//             }
//         }
//         sql_query += ') ';
//     }

//     if (data.name != '') {
//         if (sql_query === '') sql_query = 'WHERE ';
//         else sql_query += 'AND ';
//         sql_query += " ( NameC like '%" + data.name + "%' OR NameG like '%" + data.name + "%'  OR  NameE like '%" + data.name + "%' ) ";
//     }
//     //hall取得的遊戲
//     if (data.level > 0 && typeof data.games != 'undefined') {
//         if (sql_query === '') sql_query = 'WHERE ';
//         else sql_query += 'AND ';
//         sql_query += "  GameId IN('" + data.games.join("','") + "' ) ";
//     }
//     var sql = 'SELECT COUNT(*) AS count FROM games ' + sql_query;
//     var args = [];
//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.query(sql, args, function (err, res) {
//             connection.release();
//             if (err) {
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//             } else {

//                 if (res) {
//                     cb(null, { code: code.OK, msg: "" }, res[0].count);
//                 }
//             }
//         });
//     });
// };
//for admin show games
gameDao.getGames_admin = function (data, cb) {
  try {
    var sql_query = ""
    var count = 0,
      length = 0
    var sql_where = []
    if (data.ggIds.length != 0) {
      sql_where.push(" games.GGId IN ('" + data.ggIds.join("','") + "')")
    }

    if (data.typeIds.length != 0) {
      sql_where.push(" games.TypeId IN ('" + data.typeIds.join("','") + "')")
    }
    if (data.companyIds.length != 0) {
      sql_where.push(" games.Company IN ('" + data.companyIds.join("','") + "')")
    }

    if (data.name != "") {
      sql_where.push(
        " ( games.NameC like '%" +
          data.name +
          "%' OR games.NameG like '%" +
          data.name +
          "%'  OR  games.NameE like '%" +
          data.name +
          "%' OR  games.NameTH like '%" +
          data.name +
          "%' OR  games.NameVN like '%" +
          data.name +
          "%' ) "
      )
    }

    //hall取得的遊戲
    if (data.level > 0 && typeof data.games != "undefined") {
      sql_where.push("  games.GameId IN('" + data.games.join("','") + "' ) ")
    }
    var sql_where_text = " 1 "
    if (sql_where.length > 0) sql_where_text += " AND " + sql_where.join(" AND ")

    /*
            var sql = 'SELECT GameId AS gameId,GGId AS ggId,Sw AS sw,IsJPEnabled AS isJPEnable,' +
                'NameC AS nameC,NameG AS nameG,NameE AS nameE,GameUrlH5 AS url,' +
                'GameUrlDesc AS game_desc, Company AS company, MinBet AS minBet, RTPs AS rtps, Denoms AS denoms, Image AS image, MathId AS mathId, TypeId AS typeId FROM games WHERE GameId <= ' +
                '(SELECT GameId FROM games ' + sql_query + 'ORDER BY GameId DESC LIMIT ?,1) ORDER BY GameId DESC LIMIT ?';
                */

    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS games.GameId AS gameId,games.GGId AS ggId,games.Sw AS sw,games.IsJPEnabled AS isJPEnable,games.Reel_X AS reel_X,games.Reel_Y AS reel_Y," +
        " games.NameC AS nameC,games.NameG AS nameG,games.NameE AS nameE,games.NameTH AS nameTH,games.NameVN AS nameVN,games.NameID AS nameID,games.NameMY AS nameMY,games.NameJP AS nameJP, games.NameKR AS nameKR, games.GameUrlH5 AS url, games.GameIP AS gameIP , " +
        ' games.Tid AS tId, DATE_FORMAT(games.TagStartDate,"%%Y-%%m-%%d %%H:%%i:%%s") AS tag_start_date,DATE_FORMAT(games.TagEndDate,"%%Y-%%m-%%d %%H:%%i:%%s") AS tag_end_date, ' +
        " games.GameUrlDesc AS game_desc, games.Company AS company, games.MinBet AS minBet, games.RTPs AS rtps, games.Denoms AS denoms, games.Image AS image, games.MathId AS mathId, games.TypeId AS typeId, " +
        ' g.NameC AS groupC,g.NameG AS groupG,g.NameE AS groupE, IFNULL( t.NameC ,"") AS tagC, IFNULL(t.NameG , "") AS tagG, IFNULL(t.NameE , "") AS tagE,games.TagTimeState AS tag_time_state, ' +
        " games.DescC AS DescC, games.DescG AS DescG, games.DescE AS DescE, games.DescVN AS DescVN,games.DescTH AS DescTH,games.DescID AS DescID,games.DescMY AS DescMY, games.DescJP AS DescJP, games.DescKR AS DescKR " +
        " FROM games " +
        " LEFT JOIN game_group g ON(g.GGId = games.GGId) " +
        " LEFT JOIN game_tag t ON(t.Tid = games.Tid)" +
        " WHERE %s ORDER BY games.GameId DESC LIMIT %i,%i ",
      sql_where_text,
      (data.curPage - 1) * data.pageCount,
      data.pageCount
    )

    console.log("-getGames_admin sql-", sql)

    var sql2 = "SELECT FOUND_ROWS() AS ROWS;"
    var sql_query = []
    sql_query.push(sql)
    sql_query.push(sql2)

    db.act_transaction("dbclient_g_r", sql_query, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var data = {
          count: r_data[1][0]["ROWS"],
          info: r_data[0],
        }
        cb(null, r_code, data)
      }
    })

    // pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
    //     if (err) {
    //         cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
    //         return;
    //     }
    //     connection.query(sql, args, function (err, res) {

    //         connection.release();
    //         if (err) {
    //             //console.log("-------getGames_admin---------------" + err.stack);
    //             cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
    //         } else {
    //             if (res) {
    //                 var games = res;
    //                 cb(null, { code: code.OK, msg: "" }, games);
    //             }
    //         }
    //     });
    // });
  } catch (err) {
    logger.error("[gameDao][getGames_admin] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//hall遊戲
gameDao.getUserGames_bySetting = function (data, cb) {
  try {
    if (typeof data == "undefined" || typeof data.hallId == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: "Nothing to modify" }, null)
      return
    }

    var sql =
      "SELECT s.GameId FROM game_setting s \
                   INNER JOIN games g USING(GameId) \
                   WHERE s.Sw=1 AND s.Cid=? AND g.Sw=1"
    var args = [data.hallId]

    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()
        if (err) {
          //console.log("-------getUserGames_bySetting---------------" + err.stack);
          cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null)
        } else {
          if (res) {
            cb(null, { code: code.OK, msg: "" }, res)
          }
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][getUserGames_bySetting] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//已加入此類別的遊戲清單
gameDao.getGamesInCategory = function (data, cb) {
  try {
    var sql_where = []

    if (typeof data.ggIds != "undefined" && data.ggIds.length != 0) {
      sql_where.push(" g.GGId IN ('" + data.ggIds.join("','") + "') ")
    }

    if (typeof data.typeIds != "undefined" && data.typeIds.length != 0) {
      sql_where.push(" g.TypeId IN ('" + data.typeIds.join("','") + "') ")
    }

    if (data.companyIds.length != 0) {
      sql_where.push(" g.Company IN ('" + data.companyIds.join("','") + "') ")
    }

    if (data.name != "") {
      sql_where.push(
        " ( g.NameC like '%" +
          data.name +
          "%' OR g.NameG like '%" +
          data.name +
          "%'  OR  g.NameE like '%" +
          data.name +
          "%' ) "
      )
    }

    var userId = data.level == 1 ? -1 : data.hallId

    //hall取得的遊戲
    if (data.level > 0 && typeof data.games != "undefined") {
      sql_where.push(" g.GameId IN('" + data.games.join("','") + "' ) ")
    }

    var sql_where_text = " WHERE 1"
    if (sql_where.length > 0) {
      sql_where_text += " AND " + sql_where.join(" AND ")
    }

    var sql =
      " SELECT g.GameId AS gameId,g.GGId AS ggId,g.Sw AS sw,g.IsJPEnabled AS isJPEnable,g.Reel_X AS reel_X,g.Reel_Y AS reel_Y,g.Company AS companyId, " +
      ' g.NameC AS nameC,g.NameG AS nameG,g.NameE AS nameE,g.GameUrlH5 AS url, IFNULL(i.FileName ,"" ) AS imageUrl, ' +
      " gg.NameC AS groupNameC,gg.NameE AS groupNameE,gg.NameG AS groupNameG,t.Value AS typeName, " +
      " g.GameUrlDesc AS game_desc, g.Company AS company, g.MinBet AS minBet, g.RTPs AS rtps, g.Denoms AS denoms, g.Image AS image, g.MathId AS mathId, g.TypeId AS typeId " +
      " ,IFNULL(o.GameOrder,0) AS gameSort, IF(o.GameOrder=0 OR o.GameOrder is NULL ,99999999, o.GameOrder )  AS  otherSort , IF(o.GameOrder IS NULL,0, 1) AS state, DATE_FORMAT(g.CreateDate ,'%Y-%m-%d %H:%i:%s') AS addDate" +
      " FROM games g " +
      " LEFT JOIN game_group gg ON(gg.GGId = g.GGId) " +
      " LEFT JOIN game_type t ON(t.Id = g.TypeId) " +
      " LEFT JOIN game_image i ON(i.GameId=g.GameId AND i.ImageType=1 AND i.PlatformType=1) " +
      " LEFT JOIN hall_game_order o ON(o.GameId= g.GameId AND o.HallId= ? AND o.GameOrderType= ? ) " +
      sql_where_text +
      " ORDER BY otherSort,g.GameId  ASC"
    var args = [userId, data.categoryId]

    console.log("-getGamesInCategory sql-", sql)

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
      return
    })
  } catch (err) {
    logger.error("[gameDao][getGamesInCategory] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//確認此遊戲有無加入某類別
// gameDao.gameCheckInCategory = function (data, cb) {

//     var sql_where = [];
//     if (typeof data.games != 'undefined' ) {
//         sql_where.push(" GameId IN('" + data.games.join("','") + "') ");
//     }

//     if (sql_where.length == 0) {
//         cb(null, { code: code.DB.PARA_FAIL, msg: null }, null);
//         return;
//     }

//     var sql = " SELECT GameId FROM hall_game_order WHERE HallId=? AND GameOrderType=? AND " + sql_where.join(" AND ");
//     var args = [data.hallId, data.categoryId];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code);
//         } else {
//             if (r_data) {
//                 var games = [];
//                 for (var i in r_data) {
//                     games.push(r_data[i]['GameId']);
//                 }
//                 cb(null, { code: code.OK, msg: "" }, games);
//             }
//         }
//     });

//     /*
//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.query(sql, args, function (err, res) {
//             connection.release();
//             if (err) {
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//             } else {
//                 if (res) {
//                     var games = [];
//                     for (var i in res) {
//                         games.push(res[i]['GameId']);
//                     }
//                     cb(null, { code: code.OK, msg: "" }, games);
//                 }
//             }
//         });
//     });
//     */

// }
// gameDao.getTTLGamesCount_hall = function (data, cb) {

//     var sql_where = [];

//     if (data.ggIds.length > 0) {
//         sql_where.push(" g.GGId IN('"+data.ggIds.join("','")+"') ");
//     }
//     if (data.typeIds.length > 0) {
//         sql_where.push(" g.TypeId IN('"+data.typeIds.join("','")+"') ");
//     }
//     if (data.companyIds.length > 0) {
//         sql_where.push(" g.Company IN('"+data.companyIds.join("','")+"') ");
//     }

//     if (data.name != '') {
//         sql_where.push(" ( NameC like '%" + data.name + "%' OR NameG like '%" + data.name + "%'  OR  NameE like '%" + data.name + "%' ) ");
//     }

//     var sql_query = '';
//     var count = 0, length = 0;

//     // if (data.ggIds.length != 0) {

//     //     if (sql_query === '') sql_query = 'WHERE ';
//     //     sql_query += 'GGId IN (';

//     //     length = data.ggIds.length;
//     //     for (count = 0; count < length; count++) {
//     //         sql_query += data.ggIds[count];
//     //         if (count != length - 1) {
//     //             sql_query += ',';
//     //         }
//     //     }
//     //     sql_query += ' ) ';
//     // }
//     // if (data.typeIds.length != 0) {
//     //     if (sql_query === '') sql_query = 'WHERE ';
//     //     else sql_query += 'AND ';

//     //     sql_query += 'TypeId IN (';
//     //     length = data.typeIds.length;
//     //     for (count = 0; count < length; count++) {
//     //         sql_query += data.typeIds[count];
//     //         if (count != length - 1) {
//     //             sql_query += ',';
//     //         }
//     //     }
//     //     sql_query += ') ';
//     // }
//     // if (data.companyIds.length != 0) {
//     //     if (sql_query === '') sql_query = 'WHERE ';
//     //     else sql_query += 'AND ';

//     //     sql_query += 'Company IN (';
//     //     length = data.companyIds.length;
//     //     for (count = 0; count < length; count++) {
//     //         sql_query += data.companyIds[count];
//     //         if (count != length - 1) {
//     //             sql_query += ',';
//     //         }
//     //     }
//     //     sql_query += ') ';
//     // }
//     // if (data.name != '') {
//     //     if (sql_query === '') sql_query = 'WHERE ';
//     //     else sql_query += 'AND ';
//     //     sql_query += " ( NameC like '%" + data.name + "%' OR NameG like '%" + data.name + "%'  OR  NameE like '%" + data.name + "%' ) ";
//     // }

//     var sql = 'SELECT COUNT(*) AS count FROM games g ' + sql_query;
//     var args = [];
//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.query(sql, args, function (err, res) {
//             connection.release();
//             if (err) {
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//             } else {

//                 if (res) {
//                     cb(null, { code: code.OK, msg: "" }, res[0].count);
//                 }
//             }
//         });
//     });
// };

//for admin join hall
gameDao.getGames_byGroup_hall = function (data, cb) {
  try {
    var sql_groupId = ""

    var count = 0
    var length = data.groups.length

    if (length === 0) {
      cb(null, { code: code.DB.QUERY_FAIL, msg: "" }, null)
      return
    }

    for (count = 0; count < length; count++) {
      sql_groupId += " GGId = " + data.groups[count]
      if (count != length - 1) {
        sql_groupId += " OR"
      }
    }

    var sql_cid_1 = ""
    var sql_cid_2 = ""
    var sql_where = ""
    var join_type = " LEFT JOIN"
    var sql_user_agent = ""
    if (data.cid) {
      // 不是最上層 reseller(表示 upid 不為 -1) 或是登入者是 reseller，則只能看到有開的遊戲
      if ((typeof data.show_user != "undefined" && data.show_user == "agent") || data.upid != -1 || data.level == 2) {
        sql_where = " AND Sw=1 "
        join_type = " INNER JOIN "
      }
      sql_cid_1 = ',IFNULL(C.RTPid,-1) AS rtp_set,IFNULL(C.DenomsSetting,"") AS denoms_set,IFNULL(C.Sw,0) AS sw '
      sql_cid_2 =
        join_type +
        ' ( SELECT * FROM game_setting WHERE Cid = "' +
        data.cid +
        '" ' +
        sql_where +
        " ) C ON C.GameId = A.GameId "
    }
    //對於遊戲列表可以從GGId gameId排序
    var sql =
      "SELECT A.GameId AS gameId, A.GGId AS ggId,B.Value AS Type, A.IsJPEnabled AS isJPEnable,g.NameC AS groupNameC,g.NameE AS groupNameE,g.NameG AS groupNameG,A.Reel_X AS reel_x,A.Reel_Y AS reel_y," +
      "A.NameC AS nameC,A.NameG AS nameG,A.NameE AS nameE,A.Company AS company,A.MinBet AS minBet,A.RTPs AS rtps,A.Denoms AS denoms " +
      sql_cid_1 +
      "FROM (SELECT * FROM games WHERE Sw = 1 AND (" +
      sql_groupId +
      ") ORDER BY GGId ,gameId ) A LEFT JOIN game_type B ON A.TypeId = B.Id " +
      sql_cid_2 +
      " LEFT JOIN game_group g ON g.GGId = A.GGId "

    console.log("-getGames_byGroup_hall-", sql)

    var args = []
    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()
        if (err) {
          cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null)
        } else {
          if (res) {
            var games = res
            cb(null, { code: code.OK, msg: "" }, games)
          }
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][getGames_byGroup_hall] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// gameDao.getGames_byGroup_agent_v2 = function (data, cb) {

//     var sql_groupId = '';

//     var count = 0;
//     var length = data.groups.length;

//     if (length === 0) {
//         cb(null, { code: code.DB.QUERY_FAIL, msg: '' }, null);
//         return;
//     }

//     for (count = 0; count < length; count++) {
//         sql_groupId += ' GGId = ' + data.groups[count];
//         if (count != length - 1) {
//             sql_groupId += ' OR';
//         }
//     }

//     var sql_cid_1 = '';

//     var sql = 'SELECT A.GameId AS gameId, A.GGId AS ggId,B.Value AS Type, A.IsJPEnabled AS isJPEnable,g.NameC AS groupNameC,g.NameE AS groupNameE,g.NameG AS groupNameG,A.Reel_X AS reel_x,A.Reel_Y AS reel_y,' +
//         'A.NameC AS nameC,A.NameG AS nameG,A.NameE AS nameE,A.Company AS company,A.MinBet AS minBet,A.RTPs AS rtps,A.Denoms AS denoms ' +
//         ',IFNULL(C.RTPid,-1) AS rtp_set,IFNULL(C.DenomsSetting,"") AS denoms_set,IFNULL(C.Sw,0) AS sw ' +
//         'FROM (SELECT * FROM games WHERE Sw = 1 AND (' + sql_groupId
//         + ') ORDER BY GGId ) A LEFT JOIN game_type B ON A.TypeId = B.Id ' +
//         ' LEFT JOIN ( SELECT * FROM game_setting WHERE Cid = "' + data.hallId + '" ) C ON C.GameId = A.GameId ' +
//         ' LEFT JOIN game_group g ON g.GGId = A.GGId ' +

//         console.log('-getGames_byGroup_agent_v2-', sql);

//     var args = [];
//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.query(sql, args, function (err, res) {
//             connection.release();
//             if (err) {
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//             } else {
//                 if (res) {
//                     var games = res;
//                     cb(null, { code: code.OK, msg: "" }, games);
//                 }
//             }
//         });
//     });
// };

// gameDao.getGames_byGroup_agent = function (data, cb) {

//     var sql_groupId = '';

//     var count = 0;
//     var length = data.groups.length;

//     if (length === 0) {
//         cb(null, { code: code.DB.QUERY_FAIL, msg: '' }, null);
//         return;
//     }

//     for (count = 0; count < length; count++) {
//         sql_groupId += ' GGId = ' + data.groups[count];
//         if (count != length - 1) {
//             sql_groupId += ' OR';
//         }
//     }

//     var sql = 'SELECT A.GameId AS gameId,A.GGId AS ggId,B.Value AS Type,A.IsJPEnabled AS isJPEnable,' +
//         'A.NameC AS nameC,A.NameG AS nameG,A.NameE AS nameE,A.Company AS company,A.MinBet AS minBet,g.NameC AS groupNameC,g.NameE AS groupNameE,g.NameG AS groupNameG,A.Reel_X AS reel_x,A.Reel_Y AS reel_y,' +
//         'D.RTPid AS rtp,D.DenomsRunning AS denoms,IFNULL(C.Sw,0) AS sw ' +
//         'FROM (SELECT * FROM game.games WHERE Sw = 1 AND ( GGId = 0 OR GGId = 1 OR GGId = 2 OR GGId = 3 OR GGId = 4) ORDER BY GGId ) A ' +
//         'LEFT JOIN game.game_type B ON A.TypeId = B.Id ' +
//         'INNER JOIN ( SELECT * FROM game.game_setting WHERE Cid = ? AND Sw = 1 ) D ON D.GameId = A.GameId ' +
//         'LEFT JOIN ( SELECT * FROM game.game_setting WHERE Cid = ? AND Upid = ? ) C ON C.GameId = A.GameId ' +
//         'LEFT JOIN game_group g ON g.GGId = A.GGId ';

//     var args = [data.hallid, data.cid, data.hallid];
//     console.log("---------------------------" + sql, args);
//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.query(sql, args, function (err, res) {
//             connection.release();
//             if (err) {
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//             } else {
//                 if (res) {
//                     var games = res;
//                     cb(null, { code: code.OK, msg: "" }, games);
//                 }
//             }
//         });
//     });
// };

// gameDao.getGameOrder = function (data, cb) {
//     /*
//         var sql = 'SELECT B.GameId AS gameId,IFNULL(A.GameOrder,0) AS gameOrder,B.NameE AS nameE,' +
//                     'B.NameC AS nameC,B.NameG AS nameG '+
//                 'FROM hall_game_order A '+
//                 'RIGHT JOIN games B '+
//                 'ON A.GameId = B.GameId AND A.HallId = ? AND A.GameOrderType = ? '+
//                 'ORDER BY A.GameOrder,B.GameId DESC; ';
//     */
//     var sql = 'SELECT C.gameId, (@row_number:=@row_number + 1) AS gameOrder,  C.nameE, C.nameC, C.nameG ' +
//         'FROM (SELECT B.GameId AS gameId,IFNULL(A.GameOrder,0) AS gameOrder,B.NameE AS nameE, B.NameC AS nameC,B.NameG AS nameG, B.sw ' +
//         'FROM hall_game_order A RIGHT JOIN (SELECT * FROM games WHERE sw = 1) B ' +
//         'ON A.GameId = B.GameId AND A.HallId = ? AND A.GameOrderType = ? ' +
//         'ORDER BY A.GameOrder,B.GameId DESC) C,(SELECT @row_number:=0) AS t ';

//     var args = [data.cid, data.orderType];
//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.query(sql, args, function (err, res) {
//             connection.release();
//             if (err) {
//                 //console.log("-------getGameOrder---------------" + err.stack);
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//             } else {
//                 if (res) {
//                     var games = res;
//                     cb(null, { code: code.OK, msg: "" }, games);
//                 }
//             }
//         });
//     });

// };

// gameDao.getGameOrderByLobby = function (data, cb) {

//     var sql = 'SELECT C.gameId, (@row_number:=@row_number + 1) AS gameOrder,  C.nameE, C.nameC, C.nameG, C.typeId, C.isJPEnable, C.gameUrl, C.imageUrl, C.imageName ' +
//         'FROM (SELECT B.GameId AS gameId,IFNULL(A.GameOrder,0) AS gameOrder,B.NameE AS nameE, B.NameC AS nameC,B.NameG AS nameG,B.TypeId AS typeId,B.IsJPEnabled AS isJPEnable,B.GameUrlH5 AS gameUrl, ' +
//         'B.ImageUrl AS imageUrl, B.ImageName AS imageName ' +
//         'FROM hall_game_order A RIGHT JOIN ( SELECT * FROM games WHERE sw = 1 ) B ' +
//         'ON A.GameId = B.GameId AND A.HallId = ? AND A.GameOrderType = ? ' +
//         'ORDER BY A.GameOrder,B.GameId DESC) C,(SELECT @row_number:=0) AS t ';

//     var args = [-1, data.orderType];
//     //var args = [ ];
//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.query(sql, args, function (err, res) {
//             connection.release();
//             if (err) {
//                 //console.log("-------getGameOrder---------------" + err.stack);
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//             } else {
//                 if (res) {
//                     var games = res;
//                     cb(null, { code: code.OK, msg: "" }, games);
//                 }
//             }
//         });
//     });

// };

// gameDao.modifyGameOrder = function (data, cb) {

//     var sql_case = '';
//     var count = 0;
//     var length = data.games.length;

//     if (length === 0) {
//         cb(null, { code: code.DB.PARA_FAIL, msg: "Nothing to modify" }, null);
//     }

//     for (count = 0; count < length; count++) {
//         var game = data.games[count];
//         sql_case += "('" + data.cid + "','" + game.gameId + "','" + data.orderType + "','" + game.gameOrder + "')";
//         if (count != length - 1) {
//             sql_case += ',';
//         }
//     }

//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }

//         connection.beginTransaction(function (err) {

//             if (err) {
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err });
//                 return;
//             }

//             var sql = 'DELETE FROM hall_game_order WHERE HallId=? AND GameOrderType=?';
//             var args = [data.cid, data.orderType];

//             connection.query(sql, args, function (err, results) {

//                 if (err) {
//                     return connection.rollback(function () {
//                         cb(null, { code: code.DB.QUERY_FAIL, msg: err });
//                     });
//                 }

//                 var sql = 'INSERT INTO hall_game_order ( HallId, GameId, GameOrderType, GameOrder ) VALUES';
//                 sql += sql_case + ';';
//                 var args = [];

//                 connection.query(sql, args, function (err, results) {

//                     if (err) {
//                         return connection.rollback(function () {
//                             cb(null, { code: code.DB.QUERY_FAIL, msg: err });
//                         });
//                     }

//                     connection.commit(function (err) {
//                         connection.release();
//                         if (err) {
//                             return connection.rollback(function () {
//                                 cb(null, { code: code.DB.QUERY_FAIL, msg: err });
//                             });
//                         }
//                         cb(null, { code: code.OK, msg: "" });

//                     });

//                 });
//             });
//         });
//     });

// };

gameDao.getGameCountsByGroup_admin = function (cb) {
  try {
    var sql =
      "SELECT B.GGId AS ggId, IFNULL(A.counts,0) AS counts,B.NameC AS nameC,B.NameG AS nameG, B.NameE AS nameE,B.NameVN AS nameVN,B.NameTH AS nameTH  FROM" +
      "(SELECT GGId AS ggId, COUNT(*) AS counts FROM game.games GROUP BY GGId ) A RIGHT JOIN game.game_group B ON A.ggId = B.GGId"

    var args = []
    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()
        if (err) {
          //console.log("-------getGameCounts error---------------" + err.stack);
          cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null)
        } else {
          if (res) {
            var counts = res
            cb(null, { code: code.OK, msg: "" }, counts)
          }
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][getGameCountsByGroup_admin] catch err", err)
    cb(null, code.FAIL, null)
  }
}

gameDao.getGameCountsByGroup_agent = function (data, cb) {
  try {
    var sql =
      "SELECT D.GGId AS ggId, IFNULL(C.counts,0) AS counts,D.NameC AS nameC,D.NameG AS nameG, D.NameE AS nameE, D.NameVN AS nameVN, D.NameTH AS nameTH " +
      "FROM ( SELECT B.GameId, B.GGId, COUNT(B.GGId) AS counts  FROM game.game_setting A,game.games B  WHERE A.Cid = ? AND A.GameId = B.GameId AND B.Sw = 1 GROUP BY B.GGId ) C " +
      "RIGHT JOIN game.game_group D ON C.GGId = D.GGId "
    var args = [data.cid]

    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()

        if (err) {
          //console.log("-------getGameCounts error---------------" + err.stack);
          cb(null, { code: code.DB.GAME_COUNT_QUERY_FAIL, msg: err.stack }, 0)
        } else {
          if (res) {
            var counts = res
            cb(null, { code: code.OK, msg: "" }, counts)
          }
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][getGameCountsByGroup_agent] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// gameDao.addRTPs = function (data, cb) {

//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {

//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }

//         var sql = 'INSERT INTO rtp_setting ( Value ) VALUES ( ? )';

//         var args = [data.value];

//         connection.query(sql, args, function (err, results) {
//             connection.release();
//             if (err) {
//                 //console.log("-------------INSERT INTO RTP------------fail---------");
//                 return connection.rollback(function () {
//                     cb(null, { code: code.DB.CREATE_FAIL, msg: err });
//                 });
//             }

//             if (err) {
//                 //console.log("-------getGameOrder---------------" + err.stack);
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//             } else {
//                 if (res) {
//                     var games = res;
//                     cb(null, { code: code.OK, msg: "" }, games);
//                 }
//             }
//         });
//     });
// };

// gameDao.getGameName = function (data, cb) {

//     //console.log('gameDao.getGameName', JSON.stringify(data));

//     if (typeof data === 'undefined') {
//         cb(null, { code: code.FAIL, msg: err });
//         return;
//     }

//     var sql_where = [];

//     if (typeof data.name !== 'undefined' && data.name !== '') {
//         sql_where.push(" (NameC like '%" + data.name + "%' OR NameE like '%" + data.name + "%' OR NameG like '%" + data.name + "%') ");
//     }

//     if (typeof data.gameId !== 'undefined' && data.gameId !== '') {

//         var gameId = [];
//         if (typeof data.gameId === 'string') {
//             gameId = data.gameId.split(',');
//         }
//         if (Array.isArray(data.gameId)) {
//             gameId = data.gameId;
//         }
//         sql_where.push(" GameId IN(" + gameId.join(',') + ")");
//     }

//     var sql_where_text = '';
//     if (sql_where.length > 0) {
//         sql_where_text = " WHERE " + sql_where.join(" AND ");
//     }

//     var sql = ' SELECT DISTINCT GameId, NameC, NameE, NameG FROM games ' + sql_where_text;

//     var args = [];
//     //console.log('---------getGameName sql---------', sql);
//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {

//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null);
//             return;
//         }

//         connection.query(sql, args, function (err, res) {
//             connection.release();
//             if (err) {
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//             } else {
//                 if (res) {
//                     cb(null, { code: code.OK, msg: "" }, res);
//                 }
//             }
//         });
//     });
// };

// gameDao.getGameURL = function (data, cb) {

//     if (typeof data === 'undefined' || typeof data.gameId === 'undefined') {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: err });
//         return;
//     }

//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {

//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }

//         var sql = 'SELECT GameUrlH5, GameIP FROM games WHERE GameId = ?';

//         var args = [data.gameId];

//         connection.query(sql, args, function (err, results) {
//             connection.release();

//             if (err) {
//                 //console.log("-------getGameOrder---------------" + err.stack);
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//                 return;
//             }
//             //console.log("---------------------" + JSON.stringify(results));
//             if (results && results.length === 1) {
//                 var key = results[0];
//                 cb(null, { code: code.OK }, key);
//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY, msg: "Game Empty" }, null);
//             }
//         });
//     });
// };

gameDao.getUserJoinGames = function (data, cb) {
  try {
    var sql_where = []
    var sql_where_text = ""
    var sql_join_list = ""
    var sql_field_sw = ""
    if (data.level == "AD") {
      sql_field_sw = "B.sw"
    } else {
      // ha || ag
      var userId = 0
      var upId = -1
      if (data.level == "HA") {
        userId = data.cid
      }
      if (data.level == "AG") {
        userId = data.upid
      }
      if (data.level == "PR") {
        userId = data.hallId
      }
      sql_field_sw = "A.sw"
      sql_join_list = " RIGHT JOIN game_setting A USING(GameId) "
      sql_where.push(` A.Cid = '${userId}' AND B.sw=1 AND A.sw=1 `)
    }
    if (sql_where.length > 0) {
      sql_where_text = " AND " + sql_where.join(" AND ")
    }
    var sql =
      "SELECT DISTINCT B.GameId AS gameId,  B.NameC AS nameC,B.NameG AS nameG,B.NameE AS nameE,B.NameVN AS nameVN,B.NameTH AS nameTH , B.NameID AS nameID, B.NameMY AS nameMY, B.NameJP AS nameJP, B.NameKR AS nameKR,  B.GGId, " +
      sql_field_sw +
      " FROM games B " +
      sql_join_list +
      " WHERE 1 " +
      sql_where_text

    var args = []
    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }
      connection.query(sql, args, function (err, results) {
        connection.release()

        if (err) {
          cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null)
          return
        }
        if (results) {
          cb(null, { code: code.OK }, results)
        } else {
          cb(null, { code: code.DB.DATA_EMPTY, msg: "Game Empty" }, null)
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][getUserJoinGames] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// gameDao.getCounts_GameList = function (data, cb) {

//     if (typeof data === 'undefined' || typeof data.hallId === 'undefined') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: err });
//         return;
//     }

//     var sql_where = [];
//     var sql_where_text = "";
//     if (data.hallId != '') {
//         sql_where.push(" E.Cid ='" + data.hallId + "' ");
//     }

//     sql_where.push('A.Sw=1 ');
//     sql_where_text = (sql_where.length > 0) ? sql_where.join(" AND ") : ' 1 ';

//     var sql = "SELECT count(*) AS count FROM (SELECT A.GameId,A.Reel_X,A.Reel_Y,A.GGId,A.MathId,A.TypeId,A.Sw,A.IsLoaded,A.IsJPEnabled,A.NameC,A.NameG,A.NameE,A.GameUrlH5,A.GameIP,A.GameUrlDesc, " +
//         "  A.SlotRtpSetting,A.SlotRtpRunning,A.Company,A.MinBet,A.RTPs,A.Denoms,A.Image,A.ImageUrl,A.ImageName,B.NameC AS gameGroupC,B.NameG AS gameGroupG,B.NameE AS gameGroupE ,C.Value AS gameType " +
//         " FROM games A " +
//         " LEFT JOIN game_group B USING(GGId)" +
//         " LEFT JOIN game_type C ON(A.TypeId=C.Id) " +
//         " INNER JOIN game_setting E ON(E.Sw=1 AND A.GameId= E.GameId)" +
//         " WHERE " + sql_where_text + " ) T ";

//     var args = [];
//     //console.log('-getCounts_GameList sql-', sql);

//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {

//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.query(sql, args, function (err, results) {
//             connection.release();

//             if (err) {
//                 //console.log("-------getCounts_GameList err---------------" + err.stack);
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//                 return;
//             }

//             //console.log("---------getCounts_GameList result------------" + JSON.stringify(results));
//             if (results) {
//                 cb(null, { code: code.OK }, results[0]['count']);
//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY, msg: "Game Empty" }, null);
//             }
//         });
//     });
// }

// gameDao.getGameList = function (data, cb) {

//     if (typeof data === 'undefined' || typeof data.hallId === 'undefined') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: err });
//         return;
//     }

//     var sql_where = [];
//     var sql_where_text = "";
//     if (data.hallId != '') {
//         sql_where.push(" E.Cid ='" + data.hallId + "' ");
//     }

//     if (typeof data.Game != 'undefined' && data.Game != '') {
//         sql_where.push(" A.GameId ='" + data.Game + "' ");
//     }

//     if (typeof data.game != 'undefined' && data.game != '') {
//         sql_where.push(" A.ProjectId ='" + data.game + "' ");
//     }

//     if (typeof data.ProjectId != 'undefined' && data.ProjectId != '') {
//         sql_where.push(" A.ProjectId ='" + data.ProjectId + "' ");
//     }

//     sql_where.push('A.Sw=1 ');
//     sql_where_text = (sql_where.length > 0) ? sql_where.join(" AND ") : ' 1 ';
//     //目前熱門
//     var sort_info = ["180106", "180107", "180102", "180407", "180405", "180307", "180401", "180507", "180601", "180306",
//         "180610", "180502", "180105", "180506", "180503", "180104", "180103", "180202", "180301", "180304",
//         "180402", "180501", "180305", "180204", "180303", "180404", "180406", "180403", "180206", "180505",
//         "180203", "180101", "180504", "180201", "180302", "180205", "180207", "180608", "180609", "180602",
//         "180801", "180802", "180803", "180804", "180805", "180806", "180807", "180605", "180906", "180907",
//         "190100", "190101", "190102"];

//     var order_by_sql = " ORDER BY FIELD(A.ProjectId,'" + sort_info.join("','") + "')";
//     var sql = " SELECT A.GameId,A.ProjectId,A.GGId,A.TypeId,A.Sw,A.IsJPEnabled,A.NameC,A.NameG,A.NameE,A.GameUrlH5,A.GameIP,A.GameUrlDesc, " +
//         "  A.SlotRtpSetting,A.SlotRtpRunning,A.Company,A.MinBet,A.RTPs,A.Denoms,A.Image,A.ImageUrl,A.ImageName " +
//         " FROM games A " +
//         " INNER JOIN game_setting E ON(E.Sw=1 AND A.GameId=E.GameId)" +
//         " WHERE " + sql_where_text +
//         order_by_sql;
//     var args = [];

//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.query(sql, args, function (err, results) {
//             connection.release();

//             if (err) {
//                 //console.log("-------getGameList err---------------" + err.stack);
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//                 return;
//             }
//             //console.log("---------getGameList result------------" + JSON.stringify(results));
//             if (results) {
//                 cb(null, { code: code.OK }, results);
//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY, msg: "Game Empty" }, null);
//             }
//         });
//     });
// }

// gameDao.getGameOrder_User = function (data, cb) {

//     //console.log('-getGameOrder_User-', JSON.stringify(data));

//     var res_data = [];

//     if (typeof data == 'undefined') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: null }, null);
//         return;
//     }

//     var sql = 'SELECT * FROM hall_game_order WHERE HallId=? AND GameOrderType=?';
//     var args = [data.cid, data.orderType];

//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {

//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.query(sql, args, function (err, results) {
//             connection.release();

//             if (err) {
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//                 return;
//             }

//             if (results) {
//                 var info = { hall_game_order: results };
//                 res_data.push(info);
//                 cb(null, { code: code.OK }, res_data);

//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY, msg: "Game Empty" }, null);
//             }
//         });
//     });
// }

//建立遊戲類別
gameDao.createGameCategory = function (data, cb) {
  try {
    if (
      typeof data == "undefined" ||
      typeof data.nameE == "undefined" ||
      typeof data.nameG == "undefined" ||
      typeof data.nameC == "undefined" ||
      typeof data.state == "undefined"
    ) {
      cb(
        null,
        {
          code: code.DB.PARA_FAIL,
          msg: null,
        },
        null
      )
      return
    }
    if (data.state == "") data.state = 0
    var now = timezone.serverTime()
    var sql = "INSERT INTO game_category (`NameE`, `NameG`, `NameC`, `State`, ModifyDate) VALUES (?,?,?,?,?)"
    var args = [data.nameE, data.nameG, data.nameC, data.state, now]

    pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
      if (err) {
        cb(null, {
          code: code.DB.GET_CONNECT_FAIL,
          msg: err,
        })
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()
        if (err) {
          cb(
            null,
            {
              code: code.DB.CREATE_FAIL,
              msg: err.stack,
            },
            null
          )
        } else {
          cb(
            null,
            {
              code: code.OK,
              msg: "",
            },
            res.insertId
          )
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][createGameCategory] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//修改遊戲類別
gameDao.modifyGameCategory = function (data, cb) {
  if (typeof data == "undefined") {
    cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
    return
  }
  var now = timezone.serverTime()
  var sql_where = []
  var sql_update = []
  if (typeof data.nameE != "undefined") {
    sql_update.push(" NameE = '" + data.nameE + "' ")
  }
  if (typeof data.nameG != "undefined") {
    sql_update.push(" NameG = '" + data.nameG + "' ")
  }
  if (typeof data.nameC != "undefined") {
    sql_update.push(" NameC = '" + data.nameC + "' ")
  }
  if (typeof data.nameVN != "undefined") {
    sql_update.push(" NameVN = '" + data.nameVN + "' ")
  }
  if (typeof data.nameTH != "undefined") {
    sql_update.push(" NameTH = '" + data.nameTH + "' ")
  }
  if (typeof data.nameID != "undefined") {
    sql_update.push(" NameID = '" + data.nameID + "' ")
  }
  if (typeof data.nameMY != "undefined") {
    sql_update.push(" NameMY = '" + data.nameMY + "' ")
  }
  if (typeof data.nameID != "undefined") {
    sql_update.push(" NameJP = '" + data.nameJP + "' ")
  }
  if (typeof data.nameMY != "undefined") {
    sql_update.push(" NameKR = '" + data.nameKR + "' ")
  }

  if (typeof data.state != "undefined") {
    sql_update.push(" State = '" + data.state + "' ")
  }

  if (Array.isArray(data.categoryId) && data.categoryId.length > 0) {
    sql_where.push(" CsId IN('" + data.categoryId.join("','") + "') ")
  }
  if (typeof data.categoryId === "number" && data.categoryId !== "") {
    sql_where.push(" CsId IN('" + data.categoryId + "') ")
  }

  if (sql_where.length == 0 || sql_update.length == 0) {
    cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
    return
  }
  sql_update.push(" ModifyDate =  '" + now + "' ")
  var sql = "UPDATE game_category SET " + sql_update.join(",") + " WHERE " + sql_where.join(" AND ")
  var args = []

  pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
    if (err) {
      cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
      return
    }
    connection.query(sql, args, function (err, results) {
      connection.release()
      if (err) {
        cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null)
        return
      } else {
        cb(null, { code: code.OK }, null)
      }
    })
  })
}

//修改遊戲類別
gameDao.modifyGameCategoryState = function (data, cb) {
  try {
    var now = timezone.serverTime()
    if (typeof data == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }

    var sql_query = []
    data.forEach((item) => {
      var update_sql =
        "UPDATE game_category SET State='" +
        item.state +
        "',ModifyDate =  '" +
        now +
        "'  WHERE CsId='" +
        item.categoryId +
        "' ;"
      sql_query.push(update_sql)
    })

    db.act_transaction("dbclient_g_rw", sql_query, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, null)
      }
    })
  } catch (err) {
    logger.error("[gameDao][modifyGameCategoryState] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//取遊戲類別
gameDao.getGameCategory_ById = function (categoryId, cb) {
  try {
    if (typeof categoryId == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }
    var sql_where = []
    if (Array.isArray(categoryId) && categoryId.length > 0) {
      sql_where.push(" CsId IN('" + categoryId.join("','") + "') ")
    }
    if (typeof categoryId === "number" && categoryId !== "") {
      sql_where.push(" CsId IN('" + categoryId + "') ")
    }
    if (sql_where.length == 0) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }
    var sql = "SELECT * FROM game_category WHERE " + sql_where.join(" AND ")
    var args = []

    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()
        if (res) {
          cb(null, { code: code.OK }, res)
        } else {
          cb(null, { code: code.DB.DATA_EMPTY }, null)
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][getGameCategory_ById] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//取遊戲類別與userID對應表
gameDao.getGameCategoryMap = function (data, cb) {
  if (typeof data == "undefined") {
    cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
    return
  }
  var sql_where = []
  if (typeof data.categoryId !== "undefined" && data.categoryId !== "") {
    sql_where.push(" CsId = '" + data.categoryId + "' ")
  }
  if (typeof data.userId !== "undefined" && data.userId !== "") {
    sql_where.push(" HallId = '" + data.userId + "'  ")
  }
  if (sql_where.length === 0) {
    cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
    return
  }

  var sql = "SELECT * FROM game_category_map WHERE " + sql_where.join(" AND ")
  var args = []

  pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
    if (err) {
      cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
      return
    }
    connection.query(sql, args, function (err, res) {
      connection.release()
      if (res) {
        cb(null, { code: code.OK }, res)
      } else {
        cb(null, { code: code.DB.DATA_EMPTY }, null)
      }
    })
  })
}

//清除-遊戲類別與userID對應表
gameDao.cleanGameCategoryMap = function (data, cb) {
  if (typeof data == "undefined") {
    cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
    return
  }
  var sql_where = []
  if (typeof data.categoryId !== "undefined" && data.categoryId !== "") {
    sql_where.push(" CsId = '" + data.categoryId + "' ")
  }
  if (typeof data.userId !== "undefined" && data.userId !== "") {
    sql_where.push(" HallId = '" + data.userId + "'  ")
  }
  if (sql_where.length === 0) {
    cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
    return
  }

  var sql = "DELETE FROM game_category_map WHERE " + sql_where.join(" AND ")
  var args = []

  pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
    if (err) {
      cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
      return
    }
    connection.query(sql, args, function (err, res) {
      connection.release()
      if (res) {
        cb(null, { code: code.OK }, res)
      } else {
        cb(null, { code: code.DB.DATA_EMPTY }, null)
      }
    })
  })
}

gameDao.addGameCategoryMap = function (data, cb) {
  if (typeof data == "undefined") {
    cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
    return
  }
  //沒有要加入類別的遊戲
  if (data.userId.length == 0) {
    cb(null, { code: code.OK, msg: null }, null)
    return
  }
  var sql_insert = []

  if (
    typeof data.categoryId !== "undefined" &&
    data.categoryId.length > 0 &&
    typeof data.userId !== "undefined" &&
    data.userId.length > 0
  ) {
    var categoryId = data.categoryId
    var userId = data.userId
    for (var i in categoryId) {
      for (var j in userId) {
        sql_insert.push("('" + categoryId[i] + "' , '" + userId[j] + "') ")
      }
    }
  }

  if (sql_insert.length === 0) {
    cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
    return
  }

  var sql = "INSERT INTO game_category_map (CsId,HallId) VALUES " + sql_insert.join(" , ")
  var args = []
  pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
    if (err) {
      cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null)
      return
    }
    connection.query(sql, args, function (err, res) {
      connection.release()
      if (res) {
        cb(null, { code: code.OK }, res)
      } else {
        cb(null, { code: code.DB.DATA_EMPTY }, null)
      }
    })
  })
}

//取遊戲類別(所有)
gameDao.GetCateList_byUser = function (data, cb) {
  try {
    if (typeof data == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }

    var sql_where = []
    var sql_field_userGameNum = ""
    var sql_field_userState = ""
    var sql_field_userNum = ""

    //admin登入
    if (data.level == 1) {
      sql_where.push(" cat.State IN(0,1) ") //只取上下架
      sql_field_userGameNum = " 0 AS userGameNum,"
      sql_field_userNum = " IFNULL(COUNT(DISTINCT c.Cid),0) AS userNum,"
      if (data.type == 1) {
        sql_field_userState = " 0 AS userState"
      } else {
        sql_field_userState =
          "(SELECT count(DISTINCT HallId) AS count FROM game_category_map WHERE HallId = '" +
          data.userId +
          "' AND CsId = cat.CsId) AS userState"
      }
    } else {
      //hall登入
      sql_where.push(" cat.State=1 ") //只能看到上架的
      sql_field_userNum = " 0 AS userNum,"

      //if (data.type == 1) {
      sql_field_userGameNum =
        " (SELECT count(DISTINCT sort.GameId) " +
        "  FROM hall_game_order sort  " +
        "   INNER JOIN game_setting s ON (s.GameId=sort.GameId AND s.Cid=sort.HallId AND s.Sw=1)" +
        "   INNER JOIN games g ON (s.GameId = g.GameId AND g.Sw=1)" +
        "   WHERE s.Cid = '" +
        data.hallId +
        "' AND  sort.GameOrderType = cat.CsId " +
        "  ) AS userGameNum,"

      sql_field_userState =
        "(SELECT count(DISTINCT HallId) AS count FROM game_category_map WHERE HallId = '" +
        data.hallId +
        "' AND CsId = cat.CsId) AS userState"
    }

    var sql_where_text = sql_where.length > 0 ? " 1 AND " + sql_where.join(" AND ") : " 1 "

    var sortKey = "cat.ModifyDate"
    var sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        csid: "cat.CsId",
        state: "cat.State",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)

    var sql =
      " SELECT cat.CsId,cat.NameE AS categoryNameE,cat.NameG AS categoryNameG,cat.NameC AS categoryNameC,cat.State, " +
      " (SELECT count(DISTINCT sort.GameId)  " +
      "  FROM hall_game_order sort   " +
      "  INNER JOIN game_setting s ON (s.GameId=sort.GameId ) " +
      "  INNER JOIN games g ON (s.GameId = g.GameId AND g.Sw=1) " +
      "  WHERE sort.HallId='-1' AND sort.GameOrderType = cat.CsId " +
      "  ) AS defaultGameNum, " +
      sql_field_userGameNum +
      sql_field_userNum +
      sql_field_userState +
      " FROM game_category cat " +
      " LEFT JOIN game_category_map map ON(cat.CsId = map.CsId) " +
      " LEFT JOIN customer c ON(c.Cid = map.HallId AND IsAg=2 AND c.State='N') " +
      " WHERE " +
      sql_where_text +
      " GROUP BY cat.CsId " +
      order_by_text
    var args = []

    console.log("- getList_GameCategory sql-", sql)
    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null)
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()
        if (res) {
          var info = []
          for (var i in res) {
            var catInfo = {}
            catInfo["categoryId"] = res[i]["CsId"]
            catInfo["categoryNameE"] = res[i]["categoryNameE"]
            catInfo["categoryNameG"] = res[i]["categoryNameG"]
            catInfo["categoryNameC"] = res[i]["categoryNameC"]
            catInfo["state"] = res[i]["State"]

            if (data.level == 1) {
              //admin登入
              catInfo["userNum"] = res[i]["userNum"] //加入user的筆數
              catInfo["gameNum"] = res[i]["defaultGameNum"] //加入遊戲的筆數(預設值)
              if (data.type == 2) catInfo["userState"] = res[i]["userState"] //此USER是否加入 (type==2)
              info.push(catInfo)
            } else {
              //hall登入
              catInfo["gameNum"] = res[i]["userGameNum"] > 0 ? res[i]["userGameNum"] : res[i]["defaultGameNum"]
              if (res[i]["userState"] == 1) {
                info.push(catInfo)
              }
            }
          }
          cb(null, { code: code.OK }, info)
        } else {
          cb(null, { code: code.DB.DATA_EMPTY }, null)
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][GetCateList_byUser] catch err", err)
    cb(null, code.FAIL, null)
  }
}

gameDao.GetUsersInCategory = function (data, cb) {
  try {
    if (typeof data == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }

    var sql_where = []
    if (typeof data.state != "undefined") {
      var state = []
      if (typeof data.state === "string" && data.state != "") {
        state = data.state.split(",")
      }
      if (Array.isArray(data.state)) {
        state = data.state
      }
      if (state.length > 0) {
        sql_where.push(" c.State IN('" + state.join("','") + "') ") //user狀態
      }
    }

    if (typeof data.userName != "undefined" && data.userName != "") {
      sql_where.push(" c.UserName like '%" + data.userName + "%' ")
    }

    if (typeof data.start_date != "undefined" && data.start_date != "") {
      sql_where.push(" c.AddDate >= '" + data.start_date + "' ")
    }

    if (typeof data.end_date != "undefined" && data.end_date != "") {
      sql_where.push(" c.AddDate <= '" + data.end_date + "' ")
    }

    var sql_where_text = ""
    if (sql_where.length > 0) sql_where_text = " AND " + sql_where.join(" AND ")

    var sql_field_userState = ""
    if (data.type == 1) {
      sql_field_userState =
        " (SELECT count(DISTINCT CsId) AS count FROM game_category_map WHERE HallId=c.Cid AND CsId = " +
        data.categoryId +
        " ) AS userState "
    } else {
      sql_field_userState = " 0 AS userState "
    }

    var sql =
      "SELECT c.Cid,c.UserName,Date_Format(c.AddDate,'%Y-%m-%d') AS AddDate, " +
      " (SELECT count( DISTINCT(map.CsId) )" +
      "  FROM game_category_map map " +
      "   INNER JOIN game_category cate ON(cate.CsId=map.CsId)" +
      "   WHERE c.Cid = map.HallId" +
      "  ) AS categoryNum ," +
      sql_field_userState +
      " FROM customer c " +
      " WHERE c.IsAg=2 AND c.State='N' " +
      sql_where_text
    var args = []

    console.log("-GetUsersInCategory sql -", sql)

    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null)
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()
        if (res) {
          cb(null, { code: code.OK }, res)
        } else {
          cb(null, { code: code.DB.DATA_EMPTY }, null)
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][GetUsersInCategory] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//取user 有的遊戲
gameDao.getUserGames = function (data, cb) {
  try {
    //console.log('--getUserGames data--', JSON.stringify(data));

    if (typeof data == "undefined" || typeof data.hallId == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }
    var sql =
      "SELECT g.GameId,g.NameC,g.NameG,g.NameE,g.Sw AS gameSw,s.Sw AS setSw " +
      " FROM games g" +
      " INNER JOIN game_setting s ON(s.GameId=g.GameId) " +
      " INNER JOIN customer c ON(c.Cid=s.Cid) " +
      " WHERE c.Cid = ? AND s.Sw = 1 "
    var args = [data.hallId]
    //console.log('-getUserGames sql -', sql, args);
    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null)
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()
        if (res) {
          cb(null, { code: code.OK }, res)
        } else {
          cb(null, { code: code.DB.DATA_EMPTY }, null)
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][getUserGames] catch err", err)
    cb(null, code.FAIL, null)
  }
}

gameDao.getGames_byId = function (data, cb) {
  var where_not_game_id = " AND g.GameId NOT IN('" + data.catGameId.join("','") + "') "

  var sql = ""
  var args = []
  if (data.hallId == -1) {
    sql =
      "SELECT g.GameId AS gameId ,g.NameC AS gameC ,g.NameG AS gameG ,g.NameE AS gameE ,g.GGId AS ggId, g.TypeId AS typeId, g.Company AS companyId,IFNULL(i.FileName,'') AS imageUrl,DATE_FORMAT(g.CreateDate,'%Y-%m-%d %H:%i:%s') AS addDate \
        FROM games g \
        LEFT JOIN game_image i ON(i.GameId=g.GameId AND i.PlatformType=1 AND i.ImageType=1) \
        WHERE g.Sw =1 " +
      where_not_game_id
  } else {
    sql =
      "SELECT g.GameId AS gameId ,g.NameC AS gameC ,g.NameG AS gameG ,g.NameE AS gameE ,g.GGId AS ggId, g.TypeId AS typeId, g.Company AS companyId,DATE_FORMAT(g.CreateDate,'%Y-%m-%d %H:%i:s') AS addDate\
        INNER JOIN game_setting s ON(s.GameId = g.GameId AND s.HallId=? ) \
        FROM games g \
        WHERE g.Sw =1 AND s.Sw AND g.catGameId = 1 " +
      where_not_game_id
    args = [data.hallId]
  }
  console.log("getGames_byId sql ", sql, args)
  pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
    if (err) {
      cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null)
      return
    }
    connection.query(sql, args, function (err, res) {
      connection.release()
      if (res) {
        var info = []
        for (var i in res) {
          info.push({
            gameId: res[i]["gameId"],
            nameC: res[i]["gameC"],
            nameG: res[i]["gameG"],
            nameE: res[i]["gameE"],
            gameSort: 0,
            ggId: res[i]["ggId"],
            typeId: res[i]["typeId"],
            companyId: res[i]["companyId"],
            state: 0,
            imageUrl: res[i]["imageUrl"],
            addDate: res[i]["addDate"],
          })
        }
        cb(null, { code: code.OK }, info)
      } else {
        cb(null, { code: code.DB.QUERY_FAIL }, null)
      }
    })
  })
}

//取預設或uer在單一類別的遊戲ID
gameDao.getSortGameId_byCatId = function (data, cb) {
  try {
    if (typeof data == "undefined" || typeof data.gameType == "undefined" || typeof data.categoryId == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }

    var sql_where = []
    var sql_join = ""
    if (data.gameType == "user") {
      //自行排序
      sql_where.push(" sort.HallId = '" + data.hallId + "' AND sort.HallId = map.HallId ")
      sql_join = " INNER JOIN game_category_map map ON (cate.CsId= map.CsId) "
    } else {
      sql_where.push(" sort.HallId = '-1' ")
    }

    if (typeof data.gameId != "undefined" && data.gameId.length > 0) {
      sql_where.push(" sort.GameId IN('" + data.gameId.join("','") + "') ")
    }

    if (data.categoryId != "") {
      sql_where.push(" cate.CsId  = " + data.categoryId + "  ")
    }

    if (sql_where.length == 0) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var sql =
      "SELECT DISTINCT (sort.GameId)" +
      " FROM game_category cate " +
      sql_join +
      " INNER JOIN hall_game_order sort ON(sort.GameOrderType= cate.CsId ) " +
      " WHERE " +
      sql_where.join(" AND ")
    var args = []

    console.log("--getSortGameId_byCatId sql--", sql)

    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null)
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()
        if (res) {
          cb(null, { code: code.OK }, res)
        } else {
          cb(null, { code: code.DB.DATA_EMPTY }, null)
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][getSortGameId_byCatId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//取預設或uer在單一類別的遊戲排序
gameDao.getGameSorting = function (data, cb) {
  if (typeof data == "undefined" || typeof data.catGameId == "undefined" || typeof data.sortType == "undefined") {
    cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
    return
  }
  var sql = ""
  //系統預設
  if (data.sortType == 0) {
    sql =
      "SELECT DISTINCT g.GameId,g.NameC,g.NameG,g.NameE,sort.GameOrder,g.GGId AS ggId, g.TypeId AS typeId, g.Company AS companyId,IFNULL(i.FileName,'') AS imageUrl ,DATE_FORMAT(g.CreateDate,'%Y-%m-%d %H:%i:s') AS addDate " +
      " FROM games g " +
      " LEFT JOIN hall_game_order sort ON( g.GameId = sort.GameId) " +
      " LEFT JOIN game_image i ON(i.GameId=g.GameId AND i.PlatformType=1 AND i.ImageType=1) " +
      " WHERE sort.HallId = '-1' AND sort.GameOrderType=1 AND g.GameId IN('" +
      data.catGameId.join("','") +
      "')  " +
      " ORDER BY sort.GameOrder DESC"
  }

  //1:最新
  if (data.sortType == 1) {
    sql =
      "SELECT C.GameId,C.NameC,C.NameG,C.NameE,(@row_number:=@row_number + 1) AS GameOrder,C.ggId,C.typeId,C.companyId,imageUrl,addDate " +
      " FROM " +
      " (SELECT g.GameId,g.NameC,g.NameG,g.NameE,g.GGId AS ggId, g.TypeId AS typeId, g.Company AS companyId,IFNULL(i.FileName,'') AS imageUrl ,DATE_FORMAT(g.CreateDate,'%Y-%m-%d %H:%i:%s') AS addDate " +
      " FROM games g " +
      " LEFT JOIN game_image i ON(i.GameId=g.GameId AND i.PlatformType=1 AND i.ImageType=1) " +
      " WHERE g.GameId IN('" +
      data.catGameId.join("','") +
      "')  " +
      " ORDER BY g.GameId DESC )C,(SELECT @row_number:=0) AS t"
  }
  //2:熱門
  if (data.sortType == 2) {
    var sql =
      " SELECT C.GameId,C.NameC,C.NameG,C.NameE, (@row_number:=@row_number + 1) AS GameOrder,C.ggId, C.typeId,C.companyId,imageUrl,C.addDate " +
      " FROM ( " +
      " SELECT g.GameId,g.NameC,g.NameG,g.NameE,g.GGId AS ggId, g.TypeId AS typeId, g.Company AS companyId,IFNULL(i.FileName,'') AS imageUrl ,DATE_FORMAT(g.CreateDate,'%Y-%m-%d %H:%i:%s') AS addDate " +
      " FROM games g " +
      " LEFT JOIN game_image i ON(i.GameId=g.GameId AND i.PlatformType=1 AND i.ImageType=1) " +
      " WHERE g.GameId IN('" +
      data.sortGameId.join("','") +
      "') " +
      " GROUP BY g.GameId  " +
      " ORDER BY FIELD(g.GameId,'" +
      data.sortGameId.join("','") +
      "'))C,(SELECT @row_number:=0) AS t "
  }
  /*
    //2:熱門
    if (data.sortType == 2) {
        sql = "SELECT C.GameId,C.NameC,C.NameG,C.NameE,COUNT,(@row_number:=@row_number + 1) AS GameOrder,C.ggId, C.typeId,C.companyId,imageUrl   " +
            " FROM " +
            " (SELECT g.GameId,g.NameC,g.NameG,g.NameE, COUNT(bet.Wid) AS COUNT, g.GGId AS ggId, g.TypeId AS typeId, g.Company AS companyId,IFNULL(i.FileName,'') AS imageUrl   " +
            " FROM games g " +
            " LEFT JOIN wagers_1.wagers_bet bet ON(g.GameId = bet.GameId)" +
            " LEFT JOIN game_image i ON(i.GameId=g.GameId AND i.PlatformType=1 AND i.ImageType=1) " +
            " WHERE g.GameId IN('" + data.catGameId.join("','") + "') " +
            " GROUP BY g.GameId  " +
            " ORDER BY COUNT(bet.Wid) DESC )C,(SELECT @row_number:=0) AS t ";
    }
    */

  var args = []
  console.log("-getGameSorting sql -", sql)
  pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
    if (err) {
      cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null)
      return
    }
    connection.query(sql, args, function (err, res) {
      connection.release()
      if (res) {
        var info = []
        for (var i in res) {
          info.push({
            gameId: res[i]["GameId"],
            nameC: res[i]["NameC"],
            nameG: res[i]["NameG"],
            nameE: res[i]["NameE"],
            gameSort: res[i]["GameOrder"],
            ggId: res[i]["ggId"],
            typeId: res[i]["typeId"],
            companyId: res[i]["companyId"],
            imageUrl: res[i]["imageUrl"],
            addDate: res[i]["addDate"],
            state: 1,
          })
        }
        cb(null, { code: code.OK }, info)
      } else {
        cb(null, { code: code.DB.DATA_EMPTY }, null)
      }
    })
  })
}

//找排序資料及明細
gameDao.getGameSortingDetail = function (data, cb) {
  try {
    if (
      typeof data == "undefined" ||
      typeof data.catGameId == "undefined" ||
      typeof data.gameType == "undefined" ||
      typeof data.categoryId == "undefined"
    ) {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }
    var sql_where = []
    if (data.gameType == "default") {
      //預設
      sql_where.push(" sort.HallId ='-1' ")
    } else {
      //hall自訂
      sql_where.push(" sort.HallId = '" + data.hallId + "'")
    }
    if (data.catGameId.length > 0) {
      sql_where.push(" sort.GameId IN('" + data.catGameId.join("','") + "') ")
    }
    if (data.categoryId != "") {
      sql_where.push(" sort.GameOrderType = " + data.categoryId)
    }

    if (sql_where.length == 0) {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }
    var sql_where_text = sql_where.length > 0 ? " 1 AND " + sql_where.join(" AND ") : " 1 "

    var sql =
      " SELECT sort.GameId,g.NameC AS gameC,g.NameG AS gameG,g.NameE AS gameE ,sort.GameOrder AS gameSort, g.GGId, gg.NameC AS groupC,gg.NameG AS groupG, gg.NameE AS groupE,t.Value AS typeName,g.ImageUrl" +
      "  FROM hall_game_order sort" +
      "  INNER JOIN games g ON (g.GameId=sort.GameId) " +
      "  LEFT JOIN game_group gg ON (g.GGId=gg.GGId) " +
      "  LEFT JOIN game_type t ON(t.Id = g.TypeId) " +
      "  WHERE " +
      sql_where_text +
      "  ORDER BY sort.GameOrder ASC , g.GameId DESC "
    var args = []

    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null)
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()
        if (res) {
          cb(null, { code: code.OK }, res)
        } else {
          cb(null, { code: code.DB.DATA_EMPTY }, null)
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][getGameSortingDetail] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//排序
gameDao.getGameOrder_User_v2 = function (data, cb) {
  try {
    //console.log('-getGameOrder_User_v2-', JSON.stringify(data));
    var res_data = []
    if (typeof data == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }

    var sql = "SELECT * FROM hall_game_order WHERE HallId=? AND GameOrderType=?"
    var args = [data.hallId, data.categoryId]

    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }
      connection.query(sql, args, function (err, results) {
        connection.release()

        if (err) {
          cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null)
          return
        }

        if (results) {
          var info = { hall_game_order: results }
          res_data.push(info)
          cb(null, { code: code.OK }, res_data)
        } else {
          cb(null, { code: code.DB.DATA_EMPTY, msg: "Game Empty" }, null)
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][getGameOrder_User_v2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//修改排序
gameDao.modifyGameOrder_v2 = function (data, cb) {
  try {
    if (data.sortGames.length === 0) {
      cb(null, { code: code.DB.PARA_FAIL, msg: "Nothing to modify" }, null)
    }
    var sql_case = []
    var games = data.sortGames

    for (var i in games) {
      sql_case.push(
        " ('" +
          data.hallId +
          "','" +
          games[i]["gameId"] +
          "','" +
          data.categoryId +
          "','" +
          games[i]["gameSort"] +
          "') "
      )
    }

    pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }

      connection.beginTransaction(function (err) {
        if (err) {
          cb(null, { code: code.DB.QUERY_FAIL, msg: err })
          return
        }

        var sql = "DELETE FROM hall_game_order WHERE HallId=? AND GameOrderType=?"
        var args = [data.hallId, data.categoryId]

        connection.query(sql, args, function (err, results) {
          if (err) {
            return connection.rollback(function () {
              cb(null, { code: code.DB.QUERY_FAIL, msg: err })
            })
          }

          var sql = "INSERT INTO hall_game_order ( HallId, GameId, GameOrderType, GameOrder ) VALUES"
          sql += sql_case.join(",") + ";"
          var args = []

          connection.query(sql, args, function (err, results) {
            if (err) {
              return connection.rollback(function () {
                cb(null, { code: code.DB.QUERY_FAIL, msg: err })
              })
            }

            connection.commit(function (err) {
              connection.release()
              if (err) {
                return connection.rollback(function () {
                  cb(null, { code: code.DB.QUERY_FAIL, msg: err })
                })
              }
              cb(null, { code: code.OK, msg: "" })
            })
          })
        })
      })
    })
  } catch (err) {
    logger.error("[gameDao][modifyGameOrder_v2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//新增及修改遊戲於類別
// gameDao.modifyGameInCategory = function (data, cb) {

//     if (typeof data == 'undefined' || typeof data.categoryId == 'undefined' || typeof data.hallId == 'undefined') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: "Nothing to modify" }, null);
//     }

//     var sql_case = [];
//     var addGames = data.addGames; //新增的遊戲
//     var delGames = data.delGames; //刪除的遊戲

//     var sql_query = [];
//     if (delGames.length > 0) {
//         sql_query.push(" DELETE FROM hall_game_order WHERE HallId ='" + data.hallId + "' AND GameOrderType=" + data.categoryId + " AND GameId IN('" + delGames.join("','") + "'); ");
//     }
//     if (addGames.length > 0) {
//         for (var i in addGames) {
//             sql_case.push(" (" + data.hallId + "," + addGames[i] + "," + data.categoryId + ",0) ");
//         }
//         sql_query.push(" INSERT INTO hall_game_order ( HallId, GameId, GameOrderType, GameOrder ) VALUES " + sql_case.join(",") + ";");
//     }

//     console.log('- modifyGameInCategory sql_query -', JSON.stringify(sql_query));

//     //-----------------transaction start---------------
//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {

//         if (err) {
//             connection.release();
//             cb({ code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }

//         connection.beginTransaction(function (err) {
//             var funcAry = [];
//             sql_query.forEach(function (sql, index) {
//                 var temp = function (cb) {
//                     connection.query(sql, [], function (temp_err, results) {
//                         if (temp_err) {
//                             connection.rollback(function () {
//                                 //console.log("執行SQL失敗:" + index + "-" + sql + ",ERROR:" + temp_err);
//                                 return cb(code.DB.QUERY_FAIL);
//                             });
//                         } else {
//                             //console.log("執行SQL:" + index + "-" + sql + ",result:" + JSON.stringify(results));
//                             return cb(code.ok, results);
//                         }
//                     })
//                 };
//                 funcAry.push(temp);
//             });

//             m_async.series(funcAry, function (err, result) {
//                 if (err) {
//                     connection.rollback(function (err) {
//                         //console.log("modifyGameInCategory error: " + err);
//                         connection.release();
//                         return cb(null, { code: code.DB.UPDATE_FAIL, msg: '' });
//                     });
//                 } else {
//                     connection.commit(function (err, info) {
//                         //console.log("commit - modifyGameInCategory info: " + JSON.stringify(info));
//                         if (err) {
//                             connection.rollback(function (err) {
//                                 //console.log("commit modifyGameInCategory error: " + err);
//                                 connection.release();
//                                 return cb(null, { code: code.DB.QUERY_FAIL, msg: err });
//                             });
//                         } else {
//                             connection.release();
//                             return cb(null, { code: code.OK, msg: "" });
//                         }
//                     })
//                 }
//             })
//         });
//     });
//     //-----------------transaction end---------------
// }

// gameDao.getUserOwnCategory = function (hallId, cb) {

//     if (typeof hallId == 'undefined' || hallId == '') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: "" }, null);
//     }

//     var sql = " SELECT DISTINCT cate.CsId,cate.NameE,cate.NameG,cate.NameC  " +
//         "  FROM game_category cate " +
//         "  INNER JOIN game_category_map map ON(cate.CsId = map.CsId) " +
//         "  WHERE map.HallId = '" + hallId + "' AND cate.State IN(0,1) ";
//     var args = [];

//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.query(sql, args, function (err, res) {
//             connection.release();
//             if (err) {
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//                 return;
//             }
//             if (res) {
//                 cb(null, { code: code.OK }, res);

//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY, msg: "Game Empty" }, null);
//             }
//         });
//     });
// }

// gameDao.getGamesSortInCategory = function (msg, cb) {

//     if (typeof msg.hallId == 'undefined' || typeof msg.cateId == 'undefined' || typeof msg.gameId == 'undefined') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: "" }, null);
//     }
//     var sort_sql = "";
//     if (msg.gameId.length > 0) {
//         sort_sql = "sort.GameId IN('" + msg.gameId.join("','") + "')";
//     } else {
//         sort_sql = "sort.GameId ='' ";
//     }
//     var sql = " SELECT GROUP_CONCAT(T.GameId ORDER BY T.GameOrder) AS sortId,T.HallId,T.GameOrderType " +
//         " FROM " +
//         " ( SELECT sort.GameId,sort.HallId,sort.GameOrderType,sort.GameOrder " +
//         " FROM hall_game_order sort " +
//         " WHERE sort.GameOrderType IN('" + msg.cateId.join("','") + "') " +
//         " AND sort.HallId IN(?,'-1') " +
//         " AND  " + sort_sql +
//         " ORDER BY sort.HallId ASC , sort.GameOrderType ASC , sort.GameOrder ASC ) T " +
//         " GROUP BY T.HallId DESC, T.GameOrderType ASC ";

//     var args = [msg.hallId];
//     console.log(sql, args);

//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {

//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null);
//             return;
//         }
//         connection.query(sql, args, function (err, res) {
//             connection.release();
//             if (err) {
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//                 return;
//             }
//             if (res) {
//                 cb(null, { code: code.OK }, res);
//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY, msg: "Game Empty" }, null);
//             }
//         });
//     });

// }

// gameDao.getGameURL = function (data, cb) {

//     if (typeof data === 'undefined' || typeof data.gameId === 'undefined') {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: err });
//         return;
//     }

//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, db_connect) {
//         var connection = db_connect;
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }

//         var sql = 'SELECT GameUrlH5, GameIP FROM games WHERE GameId = ?';

//         var args = [data.gameId];
//         //var args = [ ];

//         connection.query(sql, args, function (err, results) {
//             connection.release();

//             if (err) {
//                 //console.log("-------getGameOrder---------------" + err.stack);
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//                 return;
//             }
//             //console.log("---------------------" + JSON.stringify(results));
//             if (results && results.length === 1) {
//                 var key = results[0];
//                 cb(null, { code: code.OK }, key);
//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY, msg: "Game Empty" }, null);
//             }

//         });

//     });

// };

// gameDao.getAllGameURL = function (cb) {

//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, db_connect) {
//         var connection = db_connect;
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }

//         var sql = 'SELECT GameId, GameUrlH5, GameIP FROM games WHERE Sw = 1';

//         var args = [];

//         connection.query(sql, args, function (err, results) {
//             connection.release();

//             if (err) {
//                 //console.log("-------getGameOrder---------------" + err.stack);
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//                 return;
//             }
//             //console.log("---------------------" + JSON.stringify(results));
//             if (results && results.length >= 1) {
//                 var key = results;
//                 cb(null, { code: code.OK }, key);
//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY, msg: "Game Empty" }, null);
//             }
//         });
//     });
// };

gameDao.getGameName_byId = function (data, cb) {
  try {
    if (typeof data === "undefined" || typeof data.gameId === "undefined") {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }

    var sql_where = []
    if (data.gameId.length > 0) {
      sql_where.push(" g.GameId In('" + data.gameId.join("','") + "')")
    } else {
      sql_where.push(" g.GameId ='' ")
    }

    var sql_where_text = " 1 "
    if (sql_where.length > 0) {
      sql_where_text += " AND " + sql_where.join(" AND ")
    }

    var sql =
      "SELECT g.GameId, g.NameC, g.NameG, g.NameE, gg.NameC AS groupC, gg.NameG AS groupG, gg.NameE AS groupE FROM games g LEFT JOIN  game_group gg USING (GGId) WHERE " +
      sql_where_text
    var args = []

    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }
      connection.query(sql, args, function (err, results) {
        connection.release()

        if (err) {
          cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null)
          return
        }
        if (results) {
          cb(null, { code: code.OK }, results)
        } else {
          cb(null, { code: code.DB.DATA_EMPTY, msg: "Game Empty" }, null)
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][getGameName_byId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

gameDao.getListGameTag = function (data, cb) {
  try {
    if (typeof data === "undefined") {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }
    var sql_where = []

    if (typeof data.state != "undefined") {
      var state = []
      if (typeof data.state === "string" && data.state != "") {
        state = data.state.split(",")
      }
      if (Array.isArray(data.state)) {
        state = data.state
      }
      if (state.length > 0) sql_where.push(" State IN('" + state.join("','") + "')")
    }

    if (typeof data.name != "undefined" && data.name != "") {
      sql_where.push(" (Title LIKE '%" + data.name + "%' ) ")
    }

    if (typeof data.tagName != "undefined" && data.tagName != "") {
      sql_where.push(
        " (NameE LIKE '%" +
          data.tagName +
          "%' OR NameG LIKE '%" +
          data.tagName +
          "%' OR NameC LIKE '%" +
          data.tagName +
          "%') "
      )
    }

    var sql_where_text = " 1 "
    if (sql_where.length > 0) {
      sql_where_text += " AND " + sql_where.join(" AND ")
    }

    var sql =
      "SELECT Tid AS tId,Title AS name, NameC AS tagC, NameG AS tagG, NameE AS tagE ,State AS state ,TextColor AS textColor ,BgColor AS bgColor ,DATE_FORMAT(ModifyDate,'%Y-%m-%d %H:%i:%s') AS modifyDate  FROM game_tag WHERE " +
      sql_where_text
    var args = []

    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }
      connection.query(sql, args, function (err, results) {
        connection.release()

        if (err) {
          cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null)
          return
        }
        if (results) {
          cb(null, { code: code.OK }, results)
        } else {
          cb(null, { code: code.DB.DATA_EMPTY, msg: "Game Tag Empty" }, null)
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][getListGameTag] catch err", err)
    cb(null, code.FAIL, null)
  }
}

gameDao.addGameTag = function (data, cb) {
  try {
    if (typeof data === "undefined") {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }
    var now = timezone.serverTime()
    var sql =
      "INSERT INTO game_tag (Title,NameE,NameG,NameC,State,TextColor,BgColor,ModifyDate) VALUES (?,?,?,?,?,?,?,?) "
    var args = [data.name, data.tagE, data.tagG, data.tagC, data.state, data.textColor, data.bgColor, now]

    console.log("-addGameTag sql-", sql, args)

    pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null)
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()
        if (err) {
          cb(null, { code: code.DB.CREATE_FAIL, msg: err.stack }, null)
        } else {
          cb(null, { code: code.OK, msg: "" }, res.insertId)
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][addGameTag] catch err", err)
    cb(null, code.FAIL, null)
  }
}

gameDao.editGameTag = function (data, cb) {
  try {
    if (typeof data === "undefined" || typeof data.tId === "undefined") {
      cb(null, { code: code.FAIL, msg: null })
      return
    }
    var now = timezone.serverTime()
    var sql_update = []

    sql_update.push(" ModifyDate = '" + now + "' ")

    if (typeof data.name != "undefined") {
      sql_update.push(" Title = '" + data.name + "'")
    }
    if (typeof data.tagE != "undefined") {
      sql_update.push(" NameE = '" + data.tagE + "'")
    }
    if (typeof data.tagG != "undefined") {
      sql_update.push(" NameG = '" + data.tagG + "'")
    }
    if (typeof data.tagC != "undefined") {
      sql_update.push(" NameC = '" + data.tagC + "'")
    }
    if (typeof data.state != "undefined") {
      sql_update.push(" State = '" + data.state + "'")
    }
    if (typeof data.bgColor != "undefined") {
      sql_update.push(" BgColor = '" + data.bgColor + "'")
    }
    if (typeof data.textColor != "undefined") {
      sql_update.push(" TextColor = '" + data.textColor + "'")
    }
    var sql_where = []
    if (data.tId != "") {
      sql_where.push(" Tid = " + data.tId)
    }
    if (sql_where.length === 0 || sql_update.length === 0) {
      cb(null, { code: code.FAIL, msg: null })
      return
    }
    var sql_update_text = sql_update.join(",")
    var sql_where_text = sql_where.join(" AND ")

    var sql = "UPDATE game_tag SET " + sql_update_text + " WHERE " + sql_where_text
    console.log("-editGameTag sql-", sql)
    var args = []

    pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()
        if (err) {
          cb(null, { code: code.DB.CREATE_FAIL, msg: err.stack })
        } else {
          cb(null, { code: code.OK, msg: "" })
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][editGameTag] catch err", err)
    cb(null, code.FAIL, null)
  }
}

gameDao.getGameTag_ByTid = function (data, cb) {
  try {
    if (typeof data === "undefined" || typeof data.tId === "undefined") {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }

    var sql = "SELECT * FROM game_tag WHERE Tid =?"
    var args = [data.tId]

    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null)
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()
        if (err) {
          cb(null, { code: code.DB.CREATE_FAIL, msg: err.stack }, null)
        } else {
          cb(null, { code: code.OK, msg: "" }, res)
        }
      })
    })
  } catch (err) {
    logger.error("[gameDao][getGameTag_ByTid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

gameDao.modifyGameImage = function (data, cb) {
  try {
    console.log("--modifyGameImage data --", JSON.stringify(data))
    var sql_query = []
    var sql_delete = sprintf("DELETE FROM game_image WHERE GameId = %s ", data.gameId)
    sql_query.push(sql_delete)

    var sql_insert = []
    data.image.forEach((item) => {
      sql_insert.push(
        sprintf(
          " ('%s','%s','%s','%s','%s','%s') ",
          data.gameId,
          item.imageType,
          item.platformType,
          item.imageName,
          item.fileName,
          item.imageSize == "" ? 0 : item.imageSize
        )
      )
    })

    if (sql_insert.length > 0) {
      var sql_insert_text =
        "INSERT game_image (GameId,ImageType,PlatformType,ImageName,FileName,ImageSize) VALUES " + sql_insert.join(",")
      sql_query.push(sql_insert_text)
    }

    console.log("---sql_query image---", JSON.stringify(sql_query))

    pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }
      //-----------------transaction start---------------
      connection.beginTransaction(function (err) {
        var funcAry = []
        sql_query.forEach(function (sql, index) {
          var temp = function (cb) {
            connection.query(sql, [], function (temp_err, results) {
              if (temp_err) {
                connection.rollback(function () {
                  return cb(code.DB.QUERY_FAIL)
                })
              } else {
                return cb(code.ok, results)
              }
            })
          }
          funcAry.push(temp)
        })

        m_async.series(funcAry, function (err, result) {
          if (err) {
            connection.rollback(function (err) {
              connection.release()
              return cb(null, { code: code.DB.UPDATE_FAIL, msg: "" })
            })
          } else {
            connection.commit(function (err, info) {
              if (err) {
                connection.rollback(function (err) {
                  connection.release()
                  return cb(null, { code: code.DB.QUERY_FAIL, msg: err })
                })
              } else {
                connection.release()
                return cb(null, { code: code.OK, msg: "" })
              }
            })
          }
        })
      })
      //-----------------transaction end---------------
    })
  } catch (err) {
    logger.error("[gameDao][modifyGameImage] catch err", err)
    cb(null, code.FAIL, null)
  }
}

gameDao.getGameImage_ByGameId = function (data, cb) {
  try {
    var sql = "SELECT * FROM game_image WHERE GameId=? "
    var args = [data.gameId]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[gameDao][getGameImage_ByGameId] catch err", err)
    cb(null, code.FAIL, null)
  }
}
gameDao.getGamesImage_admin = function (data, cb) {
  try {
    var sql_query = []
    var gameId_query = " GameId IN ('" + data.gameId.join("','") + "')"
    sql_query.push(gameId_query)

    if (typeof data.imageType != "undefined") {
      sql_query.push("ImageType = '" + data.imageType + "'")
    }
    var sql =
      "SELECT GameId,ImageType,PlatformType,ImageName,FileName,ImageSize FROM game_image WHERE " +
      sql_query.join(" AND ")
    var args = []
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[gameDao][getGamesImage_admin] catch err", err)
    cb(null, code.FAIL, null)
  }
}

gameDao.getGames_Denom_byCid = function (data, cb) {
  try {
    var sql_where = []

    const { currencies = [] } = data

    sql_where.push(" GameId IN ('" + data.gameId.join("','") + "')")
    sql_where.push(" Cid='" + data.cid + "' ")

    if (typeof data.currency != "undefined") {
      sql_where.push(" Currency='" + data.currency + "' ")
    }

    if (currencies.length > 0) {
      sql_where.push(sprintf(" Currency IN('%s') ", data.currencies.join("','")))
    }

    var sql = "SELECT Cid,GameId,Currency,Denom FROM game_denom_setting WHERE " + sql_where.join(" AND ")
    var args = []
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[gameDao][getGames_Denom_byCid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

gameDao.get_game_currency_denom_setting = function (data, cb) {
  try {
    var sql_where = []

    sql_where.push(" 1 ")

    const { currencies = [] } = data

    if (typeof data.gameId != "undefined" && data.gameId != "") {
      sql_where.push(sprintf(" GameId= '%s' ", data.gameId))
    }

    if (typeof data.gamesId != "undefined") {
      sql_where.push(sprintf(" GameId IN('%s') ", data.gamesId.join("','")))
    }

    if (currencies.length > 0) {
      sql_where.push(sprintf(" Currency IN('%s') ", data.currencies.join("','")))
    }

    var sql = "SELECT * FROM game_currency_denom_setting WHERE " + sql_where.join(" AND")
    var args = []

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[gameDao][get_game_currency_denom_setting] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/* 
haId:   hallId
level:  (登入者身分-ad:1,ha/sha:2,ag:3)
gameName: (繁簡英)           
*/
gameDao.getGameIdbyGameName = function (data, cb) {
  try {
    var sql_where = []

    var game_where = sprintf(
      " (g.NameC like '%%%s%%' OR g.NameE like '%%%s%%' OR g.NameG like '%%%s%%' ) ",
      data.gameName,
      data.gameName,
      data.gameName
    )
    sql_where.push(game_where)
    sql_where.push("g.Sw=1")

    var join_table = ""
    if (data.level > 1) {
      join_table = " INNER JOIN game_setting s ON(g.GameId = s.GameId)  "
      sql_where.push(" s.Sw=1 ")
      sql_where.push(" Cid= '" + data.haId + "' ")
    }

    var sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : " 1 "

    var sql = sprintf(
      "SELECT g.GameId  FROM games g \
                    %s WHERE %s \
                    ",
      join_table,
      sql_where_text
    )
    var args = []
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[gameDao][getGameIdbyGameName] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//取預設或uer在單一類別的遊戲排序
// gameDao.getGameSorting_v2 = function (data, cb) {
//     if (typeof data == 'undefined' || typeof data.catGameId == 'undefined' || typeof data.sortType == 'undefined') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: null }, null);
//         return;
//     }
//     var sql = "";

//     //系統預設
//     if (data.sortType == 0) {

//        sql = "SELECT DISTINCT  GROUP_CONCAT(g.GameId)  AS GameId \
//               FROM games g \
//               LEFT JOIN hall_game_order sort ON( g.GameId = sort.GameId) \
//               WHERE sort.HallId = '-1' AND sort.GameOrderType=1 AND g.GameId IN('" + data.catGameId.join("','") + "') \
//               ORDER BY sort.GameOrder DESC";
//     }
//     var args = [];
//     console.log('-getGameSorting sql -', sql);
//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });

// }

// 取得匯率
gameDao.currencyExchangeRate = function (data, cb) {
  try {
    var sql_where = []
    var limit = ""

    if (typeof data["currency"] != "undefined" && typeof data["end_date"] != "undefined") {
      sql_where.push(" ExCurrency = '" + data["currency"] + "'")
      sql_where.push(" EnableTime <= '" + data["end_date"] + "'")
      limit = " LIMIT 0,1"
    }

    var sql_where_text = sql_where.length > 0 ? " WHERE " + sql_where.join(" AND ") : ""

    var sql =
      " SELECT CryDef, ExCurrency FROM currency_exchange_rate" + sql_where_text + " ORDER BY EnableTime DESC " + limit

    var args = []

    logger.info("-currencyExchangeRate sql -", sql)

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[gameDao][currencyExchangeRate] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 取得錢包
gameDao.getWallet = function (data, cb) {
  try {
    if (typeof data == "undefined" || typeof data["cid"] == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }

    var sql = " SELECT * FROM wallet WHERE Cid = '" + data["cid"] + "'"

    var args = []
    console.log("-getWallet sql -", sql)
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[gameDao][getWallet] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 修改遊戲設定
gameDao.modifyGameSetting = function (data, cb) {
  try {
    var autoSpinJson =
      '{"showLimits":[true,true,true,true,true],"spins":[10,50,100,500,999,-1],"jackpot":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"single":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"loss":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"bet":[80000000,1000000,500000,100000,50000,10000,5000,1000,500]}'
    var exchangeJson = '{"exCredit":true,"exDenom":true}'
    var sql_query = []

    const updateIdList = [data.cid, ...data.lowerIdList]

    var sql_upidId = " Cid IN ('" + updateIdList.join("','") + "') "
    var sql_gameId = " GameId IN ('" + data.gameCloseIds.join("','") + "') "

    if (data.gameCloseIds.length > 0) {
      // 若有關閉遊戲，則連同下線一起刪除
      var sql_delete = sprintf("DELETE FROM game_setting WHERE %s AND %s ", sql_gameId, sql_upidId)
      sql_query.push(sql_delete)
    }

    var sql_insert = []
    var sql_update = []
    var sql_update_text = ""

    var sql_rtp = []
    var sql_denom_set = []
    var sql_sw = []
    for (var count = 0; count < data.msgData.games.length; count++) {
      if (typeof data.msgData.games[count].gameId === "undefined" || typeof data.msgData.usr.cid === "undefined") {
        cb({ code: code.DB.UPDATE_FAIL, msg: "GameId or Cid undefined" })
        return
      }

      if (typeof data.msgData.games[count].rtps != "undefined") {
        sql_rtp.push(
          "WHEN GameId=" +
            data.msgData.games[count].gameId +
            ' AND Cid="' +
            data.msgData.usr.cid +
            '" THEN ' +
            data.msgData.games[count].rtps
        )
      }

      if (typeof data.msgData.games[count].denoms != "undefined") {
        sql_denom_set.push(
          "WHEN GameId=" +
            data.msgData.games[count].gameId +
            " AND Cid='" +
            data.msgData.usr.cid +
            "' THEN '" +
            data.msgData.games[count].denoms[0]["value"] +
            "'"
        )
      }

      if (typeof data.msgData.games[count].sw != "undefined") {
        sql_sw.push(
          "WHEN GameId=" +
            data.msgData.games[count].gameId +
            ' AND Cid="' +
            data.msgData.usr.cid +
            '" THEN ' +
            (data.msgData.games[count].sw ? 1 : 0)
        )
      }

      var gameId = typeof data.msgData.games[count].gameId != "undefined" ? data.msgData.games[count].gameId : -1
      var cid = typeof data.msgData.usr.cid != "undefined" ? data.msgData.usr.cid : "-1"
      var upid = typeof data.msgData.usr.upid != "undefined" ? data.msgData.usr.upid : "-1"
      var hallid = typeof data.msgData.usr.hallid != "undefined" ? data.msgData.usr.hallid : "-1"
      var rtp = typeof data.msgData.games[count].rtps != "undefined" ? data.msgData.games[count].rtps : -1
      //game_setting
      var denoms_set =
        typeof data.msgData.games[count].denoms != "undefined" ? data.msgData.games[count].denoms[0]["value"] : ""
      var denoms_run = denoms_set
      var sw = typeof data.msgData.games[count].sw != "undefined" ? (data.msgData.games[count].sw ? 1 : 0) : 0

      if (data.msgData.games[count].sw) {
        var insert_text = sprintf(
          "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')",
          gameId,
          cid,
          upid,
          hallid,
          rtp,
          denoms_set,
          denoms_run,
          sw,
          1,
          1,
          1,
          autoSpinJson,
          exchangeJson
        )
        sql_insert.push(insert_text)

        // 若有新增遊戲，則連同下線一起新增
        if (data.isUpdateLowerId) {
          updateIdList.map((x) => {
            const sqlText = sprintf(
              "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')",
              gameId,
              x,
              cid,
              hallid,
              rtp,
              denoms_set,
              denoms_run,
              sw,
              1,
              1,
              1,
              autoSpinJson,
              exchangeJson
            )
            sql_insert.push(sqlText)
          })
        }
      }
    }

    if (sql_rtp.length > 0) {
      var sql_rtp_text = " RTPid = CASE " + sql_rtp.join(" ") + " ELSE RTPid END "
      sql_update.push(sql_rtp_text)
    }

    if (sql_sw.length > 0) {
      var sql_sw_text = " Sw = CASE " + sql_sw.join(" ") + " ELSE Sw END "
      sql_update.push(sql_sw_text)
    }

    if (sql_insert.length > 0) {
      if (sql_update.length > 0) {
        sql_update_text = " ON DUPLICATE KEY UPDATE " + sql_update.join(" , ")
      }
      var sql =
        "INSERT INTO game_setting (GameId,Cid, Upid, HallId, RTPid, DenomsSetting, DenomsRunning, Sw, AutoSpinEnable, ExchangeEnable, JackpotEnable, AutoSpinJson, ExchangeJson) " +
        "VALUES " +
        sql_insert.join(" , ") +
        sql_update_text
      sql_query.push(sql)
    }

    pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }
      //-----------------transaction start---------------
      connection.beginTransaction(function (err) {
        var funcAry = []
        sql_query.forEach(function (sql, index) {
          var temp = function (cb) {
            connection.query(sql, [], function (temp_err, results) {
              if (temp_err) {
                connection.rollback(function () {
                  return cb(code.DB.QUERY_FAIL)
                })
              } else {
                return cb(code.ok, results)
              }
            })
          }
          funcAry.push(temp)
        })

        m_async.series(funcAry, function (err, result) {
          if (err) {
            connection.rollback(function (err) {
              connection.release()
              return cb(null, { code: code.DB.UPDATE_FAIL, msg: "" })
            })
          } else {
            connection.commit(function (err, info) {
              if (err) {
                connection.rollback(function (err) {
                  connection.release()
                  return cb(null, { code: code.DB.QUERY_FAIL, msg: err })
                })
              } else {
                connection.release()
                return cb(null, { code: code.OK, msg: "" }, null)
              }
            })
          }
        })
      })
      //-----------------transaction end---------------
    })
  } catch (err) {
    logger.error("[gameDao][modifyGameSetting] catch err", inspect(err))
    cb(null, code.FAIL, null)
  }
}

// 取 game_group 資料
gameDao.getGameGroup = function (getColumns, cb) {
  try {
    var sql = "SELECT " + getColumns + " FROM game_group WHERE 1 "

    var args = []
    logger.info("[gameDao][getGameGroup] sql: ", sql)
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[gameDao][getGameGroup] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/**
 * 取得指定遊戲設定資料
 *
 * @param {object} data
 * @param {string} data.gameId
 * @param {*} cb
 */
gameDao.getGameData = function (data, cb) {
  try {
    const { gameId } = data

    const sql = `SELECT gameId, jpTypeId, Reel_X, Reel_Y, GGId, Sw, IsJPEnabled, MinBet, RTPs FROM games WHERE gameId = ?`

    const args = [gameId]

    logger.info("[gameDao][getGameData] args: ", args)

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        logger.error("[gameDao][getGameData] failed ", inspect(r_code))
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[gameDao][getGameData] catch err", inspect(err))
    cb(null, code.FAIL, null)
  }
}

gameDao.getUserEnabledGames = function (data, cb) {
  const logTag = "[gameDao][getUserEnabledGames]"

  try {
    const { cid, ggId } = data

    const optionalSql = ggId ? " AND g.GGId = ?" : ""

    const sql = ` SELECT gs.GameId, g.jpTypeId, g.GGId, g.TypeId, NameC, NameG, NameE, NameVN, NameTH, NameID, NameMY, NameJP, NameKR \
                  FROM game_setting AS gs JOIN games AS g USING(GameId) \
                  WHERE gs.Sw=1 AND gs.Cid=? AND g.Sw=1 ${optionalSql}
                `
    const args = [cid, ggId]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error(`${logTag} ${inspect(err)}`)
    cb(null, code.FAIL, null)
  }
}
