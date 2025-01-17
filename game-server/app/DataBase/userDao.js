var logger = require("pomelo-logger").getLogger("userDao", __filename)
var utils = require("../util/utils")
var code = require("../util/code")
var conf = require("../../config/js/conf")
var db = require("../util/DB")
var pomelo = require("pomelo")
var m_async = require("async")
var sprintf = require("sprintf-js").sprintf
var m_md5 = require("md5")
var timezone = require("../util/timezone")
var short = require("short-uuid")

const { inspect } = require("util")

var userDao = module.exports

/*userDao.createUser = function (data, cb) {

    var sql = 'INSERT INTO customer (`Cid`,`UserName`,`Passwd`,`NickName`,`IsAg`,`IsSub`,`Upid`,`AuthorityTemplateID`,`IsDemo`,' +
        '`IsSingleWallet`,`Currency`,`Quota`,`State`,`RealName`,`Birthday`,`Address`,`Email`,`DC`) ' +
        'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)';
    var args = [data.cid, data.name, data.password, data.nickName, data.isAg, data.isSub, data.upid, data.authorityId, data.isDemo,
    data.isSingWallet, data.currency, data.quota, data.state, data.reelName, data.birthday.substr(0, 10), data.address, data.email, data.dc];

    db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
        cb(null, r_code);
    });

};*/

userDao.createUser_Hall = function (data, cb) {
  //新增彩金開關IsJackpotEnabled
  try {
    var data_user = data
    var cid = ""

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

        const brandId = data.brandId || ""
        const brandTitle = data.brandTitle || ""

        var sql =
          "INSERT INTO customer (Cid, UserName,Passwd,NickName,IsAg,IsSub,Upid,HallId," +
          "AuthorityTemplateID,IsDemo,IsSingleWallet,Quota,State,Email,Birthday,Currency,`DC`,brandId, brandTitle) " +
          "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

        var birthday = data_user.birthday !== null ? data_user.birthday.substr(0, 10) : ""
        var args = [
          data_user.cid,
          data_user.name,
          data_user.password,
          data_user.nickName,
          2,
          data_user.isSub,
          data_user.upid,
          data_user.hallid,
          data_user.authorityId,
          0,
          data_user.isSingleWallet,
          0,
          data_user.state,
          data_user.email,
          birthday,
          data_user.currencies,
          data.dc,
          brandId,
          brandTitle,
        ]

        connection.query(sql, args, function (err, results) {
          if (err) {
            return connection.rollback(function () {
              cb(null, { code: code.DB.CREATE_FAIL, msg: err })
            })
          }

          //cid = results.insertId;
          cid = data_user.cid
          var game_currency =
            typeof data_user.game_currency != "undefined" &&
            data_user.game_currency.length > 0 &&
            Array.isArray(data_user.game_currency)
              ? data_user.game_currency.join(",")
              : data_user.game_currency //遊戲的開放幣別設定

          var sql =
            "INSERT INTO hall (`HallId`,`Nickname`,`Currencies`,`IsJackpotEnabled`,`IPWhitelistOut`,`APIOutDomain`,`HallDesc`,`APIHallOwnerName`,`SecureKey`)" +
            "VALUES (?,?,?,?,?,?,?,?,?)"
          var args = [
            data_user.cid,
            data_user.name,
            game_currency,
            data_user.jackpot,
            data_user.ip_whitelist_out,
            data_user.api_outdomain,
            data_user.halldesc ? data_user.halldesc : "",
            data_user.api_hallownername,
            data_user.securekey,
          ]

          connection.query(sql, args, function (err, results) {
            if (err) {
              return connection.rollback(function () {
                cb(null, { code: code.DB.CREATE_FAIL, msg: err })
              })
            }

            var sql_insert = []
            for (var count = 0; count < data_user.game_currency.length; count++) {
              var insert_text = sprintf("('%s','%s')", data_user.cid, data_user.game_currency[count])
              sql_insert.push(insert_text)
            }

            // ex: INSERT INTO wallet (Cid, Currency) VALUES ('7dT93QK8nYME','CNY'), ('7dT93QK8nYME','INR')
            var sql = "INSERT INTO wallet (Cid, Currency) VALUES " + sql_insert.join(",")

            connection.query(sql, args, function (err, results) {
              if (err) {
                return connection.rollback(function () {
                  connection.release()
                  cb({ code: code.DB.CREATE_FAIL, msg: err })
                })
              }

              connection.commit(function (err) {
                if (err) {
                  return connection.rollback(function () {
                    cb(null, { code: code.DB.QUERY_FAIL, msg: err })
                  })
                }
                connection.release()
                cb(null, { code: code.OK, msg: "" }, cid)
              })
            })
          })
        })
      })
    })
  } catch (err) {
    logger.error("[userDao][createUser_Hall] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.createUser_Agent = function (data, cb) {
  try {
    pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }

      connection.beginTransaction(function (err) {
        const brandId = data.brandId || ""
        const brandTitle = data.brandTitle || ""

        var sql =
          "INSERT INTO customer (`Cid`,`UserName`,`Passwd`,`NickName`,`IsAg`,`IsSub`,`Upid`,`HallId`,`AuthorityTemplateID`," +
          "`IsDemo`,`Currency`,`Quota`,`State`,`Email`,`Birthday`,`DC`, `IsSingleWallet`,`brandId`,`brandTitle`) " +
          "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

        var birthday = data.birthday !== null ? data.birthday.substr(0, 10) : ""
        var args = [
          data.cid,
          data.name,
          data.password,
          data.nickName,
          3,
          0,
          data.upid,
          data.hallid,
          data.authorityId,
          data.isDemo,
          data.currency,
          0,
          data.state,
          data.email,
          birthday,
          data.dc,
          data.isSingleWallet,
          brandId,
          brandTitle,
        ]

        connection.query(sql, args, function (err, results) {
          if (err) {
            return connection.rollback(function () {
              cb(null, { code: code.DB.CREATE_FAIL, msg: err })
            })
          }

          var sql_insert = []
          for (var count = 0; count < data.game_currency.length; count++) {
            var insert_text = sprintf("('%s','%s')", data.cid, data.game_currency[count])
            sql_insert.push(insert_text)
          }

          var sql = "INSERT INTO wallet (Cid, Currency) VALUES " + sql_insert.join(",")

          connection.query(sql, args, function (err, results) {
            if (err) {
              return connection.rollback(function () {
                connection.release()
                cb({ code: code.DB.CREATE_FAIL, msg: err })
              })
            }

            connection.commit(function (err) {
              if (err) {
                return connection.rollback(function () {
                  cb(null, { code: code.DB.QUERY_FAIL, msg: err })
                })
              }
              connection.release()
              cb(null, { code: code.OK, msg: "" }, data.cid)
            })
          })
        })
      })
    })

    // var sql = 'INSERT INTO customer (`Cid`,`UserName`,`Passwd`,`NickName`,`IsAg`,`IsSub`,`Upid`,`HallId`,`AuthorityTemplateID`,' +
    //     '`IsDemo`,`Currency`,`Quota`,`State`,`Email`,`Birthday`,`DC`) ' +
    //     'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)';
    //
    // var birthday = data.birthday !== null ? data.birthday.substr(0, 10) : '';
    // var args = [data.cid, data.name, data.password, data.nickName, 3, 0, data.upid, data.hallid, data.authorityId, data.isDemo, data.currency, 0, data.state, data.email, birthday, data.dc];
    //
    // db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
    //     if (r_code.code !== code.OK) {
    //         cb(null, r_code, null);
    //     } else {
    //         cb(null, r_code, data.cid);
    //     }
    // });
  } catch (err) {
    logger.error("[userDao][createUser_Agent] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.createUser_Player = function (data, cb) {
  try {
    var sql =
      "INSERT INTO customer (`Cid`,`UserName`,`Passwd`,`NickName`,`IsAg`,`IsSub`,`Upid`,`HallId`,`AuthorityTemplateID`," +
      "`IsDemo`,`Currency`,`Quota`,`State`,`RealName`,`Birthday`,`Address`,`Email`,`DC`,`IsSingleWallet`,`brandId`,`brandTitle`) " +
      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

    const brandId = data.brandId || ""
    const brandTitle = data.brandTitle || ""

    var birthday = data.birthday !== null ? data.birthday.substr(0, 10) : ""
    var args = [
      data.cid,
      data.name,
      data.password,
      data.nickName,
      4,
      0,
      data.upid,
      data.hallid,
      "-1",
      data.isDemo,
      data.currency,
      0,
      data.state,
      data.realName,
      birthday,
      data.address,
      data.email,
      data.dc,
      data.isSingleWallet,
      brandId,
      brandTitle,
    ]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, data.cid)
      }
    })
  } catch (err) {
    logger.error("[userDao][createUser_Player] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// userDao.modifyUser_Hall = function (data, cb) {
//     var data_user = data.user;
//     var data_games = typeof data.games === 'undefined' ? [] : data.games;
//     var cid = 0;

//     var autoSpinJson = '{"showLimits":[true,true,true,true,true],"spins":[10,50,100,500,999,-1],"jackpot":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"single":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"loss":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"bet":[80000000,1000000,500000,100000,50000,10000,5000,1000,500]}';
//     var exchangeJson = '{"exCredit":true,"exDenom":true}' ;
//     var denomsRunning = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17";

//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {

//         if (err) {
//             connection.release();
//             cb({ code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.beginTransaction(function (err) {

//             if (err) {
//                 connection.release();
//                 cb({ code: code.DB.QUERY_FAIL, msg: err });
//                 return;
//             }
//             var sql_update = [];
//             var sql_where = [];

//             if (typeof data_user.name != 'undefined') {
//                 sql_where.push('UserName = "' + data_user.name + '"');
//             }

//             if (typeof data_user.cid != 'undefined' && data_user.cid != '') {
//                 sql_where.push('Cid ="' + data_user.cid + '"');
//             }

//             if (typeof data_user.password != 'undefined' && data_user.password != '') {
//                 sql_update.push('Passwd ="' + data_user.password + '"');
//             }

//             if (typeof data_user.nickName != 'undefined' && data_user.nickName != '') {
//                 sql_update.push('NickName ="' + data_user.nickName + '"');
//             }

//             if (typeof data_user.authorityId != 'undefined') {
//                 sql_update.push('AuthorityTemplateID =' + data_user.authorityId);
//             }

//             if (typeof data_user.isSingWallet != 'undefined') {
//                 sql_update.push('IsSingleWallet =' + data_user.isSingWallet);
//             }

//             if (typeof data_user.quota != 'undefined') {
//                 sql_update.push('Quota =' + data_user.quota);
//             }

//             if (typeof data_user.state != 'undefined') {
//                 sql_update.push('State ="' + data_user.state + '"');
//             }

//             if (sql_update.length === 0 || sql_where.length === 0) {
//                 return connection.rollback(function () {
//                     connection.release();
//                     cb({ code: code.DB.PARA_FAIL, msg: '' });
//                 });
//             }

//             var sql = 'UPDATE customer SET ' + sql_update.join(",") + " WHERE " + sql_where.join(" AND ");
//             var args = [];

//             connection.query(sql, args, function (err, results) {

//                 if (err) {
//                     return connection.rollback(function () {
//                         connection.release();
//                         cb({ code: code.DB.UPDATE_FAIL, msg: err });
//                         return;
//                     });
//                 }

//                 var sql_update = [];
//                 var sql_where = [];

//                 if (typeof data_user.cid != 'undefined') {
//                     sql_where.push('HallId ="' + data_user.cid + '"');
//                 }

//                 if (typeof data_user.nickName != 'undefined') {
//                     sql_update.push('NickName ="' + data_user.nickName + '"');
//                 }

//                 if (typeof data_user.currencies != 'undefined') {
//                     sql_update.push('Currencies ="' + data_user.currencies + '"');
//                 }

//                 // if (typeof data_user.ip_whitelist_in != 'undefined') {
//                 //     sql_update.push('IPWhitelistIn ="' + data_user.ip_whitelist_in + '"');
//                 // }

//                 // if (typeof data_user.ip_whitelist_out != 'undefined') {
//                 //     sql_update.push('IPWhitelistOut ="' + data_user.ip_whitelist_out + '"');
//                 // }

//                 if (typeof data_user.api_outdomain != 'undefined') {
//                     sql_update.push('APIOutDomain ="' + data_user.api_outdomain + '"');
//                 }

//                 if (typeof data_user.halldesc != 'undefined') {
//                     sql_update.push('HallDesc ="' + data_user.halldesc + '"');
//                 }

//                 if (typeof data_user.api_hallownername != 'undefined') {
//                     sql_update.push('APIHallOwnerName ="' + data_user.api_hallownername + '"');
//                 }

//                 if (typeof data_user.securekey != 'undefined') {
//                     sql_update.push('SecureKey ="' + data_user.securekey + '"');
//                 }

//                 if (sql_update.length === 0 || sql_where.length === 0) {

//                     connection.commit(function (err) {
//                         if (err) {
//                             return connection.rollback(function () {
//                                 connection.release();
//                                 cb({ code: code.DB.QUERY_FAIL, msg: err });
//                             });
//                         }
//                         connection.release();
//                         cb({ code: code.OK, msg: "" });

//                     });
//                     return;
//                 }

//                 var sql = 'UPDATE hall SET ' + sql_update.join(",") + " WHERE " + sql_where.join(" AND ");
//                 var args = [];

//                 connection.query(sql, args, function (err, results) {

//                     if (err) {
//                         return connection.rollback(function () {
//                             connection.release();
//                             cb({ code: code.DB.UPDATE_FAIL, msg: err });
//                         });
//                     }

//                     if (typeof data_games === 'undefined' || data_games.length === 0) {
//                         connection.commit(function (err) {
//                             if (err) {
//                                 return connection.rollback(function () {
//                                     connection.release();
//                                     cb({ code: code.DB.QUERY_FAIL, msg: err });
//                                 });
//                             }
//                             cb({ code: code.OK, msg: "" });
//                             return;

//                         });
//                         return;
//                     }

//                     var count = 0;
//                     var length = data_games.length;
//                     var sql_insert = [];
//                     var sql_update = [];
//                     var sql_update_text = "";
//                     var args = [];
//                     var sql_rtp = '';
//                     var sql_denom_set = '';
//                     var sql_sw = '';

//                     for (count = 0; count < length; count++) {

//                         if (typeof data_games[count].gameId === 'undefined' || typeof data_user.cid === 'undefined') {
//                             return connection.rollback(function () {
//                                 cb({ code: code.DB.UPDATE_FAIL, msg: 'GameId or Cid undefined' });
//                             });
//                         }

//                         if (typeof data_games[count].rtps != 'undefined') {
//                             if (sql_rtp === '') {
//                                 sql_rtp += 'RTPid = CASE ';
//                             }
//                             sql_rtp += 'WHEN GameId=' + data_games[count].gameId + ' AND Cid="' + data_user.cid + '" THEN ' + data_games[count].rtps + ' ';
//                             if (count === length - 1) {
//                                 sql_rtp += ' END';
//                             }
//                         }

//                         if (typeof data_games[count].denoms != 'undefined') {
//                             if (sql_denom_set === '') {
//                                 sql_denom_set += 'DenomsSetting = CASE ';
//                             }
//                             sql_denom_set += 'WHEN GameId=' + data_games[count].gameId + ' AND Cid="' + data_user.cid + '" THEN "' + data_games[count].denoms + '" ';
//                             if (count === length - 1) {
//                                 sql_denom_set += ' END';
//                             }
//                         }

//                         if (typeof data_games[count].sw != 'undefined') {
//                             if (sql_sw === '') {
//                                 sql_sw += 'Sw = CASE ';
//                             }
//                             sql_sw += 'WHEN GameId=' + data_games[count].gameId + ' AND Cid=' + data_user.cid + ' THEN ' + (data_games[count].sw ? 1 : 0) + ' ';
//                             if (count === length - 1) {
//                                 sql_sw += ' ELSE Sw END';
//                             }
//                         }

//                         var gameId = typeof data_games[count].gameId != 'undefined' ? data_games[count].gameId : -1;
//                         var cid = typeof data_user.cid != 'undefined' ? data_user.cid : '-1';
//                         var upid = typeof data_user.upid != 'undefined' ? data_user.upid : '-1';
//                         var hallid = typeof data_user.hallid != 'undefined' ? data_user.hallid : '-1';
//                         var rtp = typeof data_games[count].rtps != 'undefined' ? data_games[count].rtps : -1;
//                         // var denoms_set = typeof data_games[count].denoms != 'undefined' ? data_games[count].denoms : '';
//                         // var denoms_run = typeof data_games[count].denoms != 'undefined' ? data_games[count].denoms : '';
//                         var sw = typeof data_games[count].sw != 'undefined' ? (data_games[count].sw ? 1 : 0) : 0;

//                         var insert_text = sprintf("('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')", gameId, cid, upid, hallid, rtp, denomsRunning, denomsRunning, sw, 1, 1, 1, autoSpinJson,exchangeJson);
//                         sql_insert.push(insert_text);
//                     }
//                     if (sql_rtp != '') {
//                         sql_update.push(sql_rtp);
//                     }
//                     if (sql_denom_set != '') {
//                         sql_update.push(sql_denom_set);
//                     }
//                     if (sql_sw != '') {
//                         sql_update.push(sql_sw);
//                     }

//                     if (sql_update.length > 0) {
//                         sql_update_text = ' ON DUPLICATE KEY UPDATE ' + sql_update.join(",");
//                     }

//                     var sql = 'INSERT INTO game_setting ( GameId,Cid,Upid,HallId,RTPid,DenomsSetting,DenomsRunning,Sw,AutoSpinEnable,ExchangeEnable,JackpotEnable ,AutoSpinJson, ExchangeJson) ' +
//                         'VALUES ' + sql_insert.join(",") + sql_update_text;

//                     connection.query(sql, args, function (err, results) {

//                         if (err) {
//                             return connection.rollback(function () {
//                                 connection.release();
//                                 cb({ code: code.DB.UPDATE_FAIL, msg: err });
//                             });
//                         }

//                         connection.commit(function (err) {
//                             if (err) {
//                                 return connection.rollback(function () {
//                                     cb({ code: code.DB.QUERY_FAIL, msg: err });
//                                 });
//                             }
//                             connection.release();
//                             cb({ code: code.OK, msg: "" });

//                         });
//                     });
//                 });
//             });
//         });
//     });
// };

userDao.modifyUser_Hall_v2 = function (data, cb) {
  try {
    const data_user = { ...data }

    var data_games = typeof data.games === "undefined" ? [] : data.games
    var autoSpinJson =
      '{"showLimits":[true,true,true,true,true],"spins":[10,50,100,500,999,-1],"jackpot":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"single":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"loss":[80000000,1000000,500000,100000,50000,10000,5000,1000,500],"bet":[80000000,1000000,500000,100000,50000,10000,5000,1000,500]}'
    var exchangeJson = '{"exCredit":true,"exDenom":true}'
    pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
      if (err) {
        connection.release()
        cb({ code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }
      //要執行的-sql
      var sql_query = []
      //sql-customer
      var sql_update_cus = []
      var sql_where_cus = []

      if (typeof data_user.name != "undefined") {
        sql_where_cus.push("UserName = '" + data_user.name + "'")
      }

      if (typeof data_user.cid != "undefined" && data_user.cid != "") {
        sql_where_cus.push("Cid = '" + data_user.cid + "'")
      }

      if (typeof data_user.password != "undefined" && data_user.password != "") {
        sql_update_cus.push("Passwd ='" + data_user.password + "'")
      }

      if (typeof data_user.nickName != "undefined" && data_user.nickName != "") {
        sql_update_cus.push("NickName ='" + data_user.nickName + "'")
      }

      if (typeof data_user.authorityId != "undefined" && typeof data_user["edit"] == "undefined") {
        sql_update_cus.push("AuthorityTemplateID =" + data_user.authorityId)
      }

      if (typeof data_user.isSingleWallet != "undefined" && typeof data_user["edit"] == "undefined") {
        sql_update_cus.push(`IsSingleWallet ='${data_user.isSingleWallet}'`)
      }

      if (typeof data_user.quota != "undefined") {
        sql_update_cus.push("Quota =" + data_user.quota)
      }

      if (typeof data_user.state != "undefined" && typeof data_user["edit"] == "undefined") {
        sql_update_cus.push('State ="' + data_user.state + '"')
      }

      if (typeof data_user.modifyLvl != "undefined") {
        sql_update_cus.push("ModifyLvl =" + data_user.modifyLvl)
      }
      if (typeof data_user.email != "undefined") {
        sql_update_cus.push('Email ="' + data_user.email + '"')
      }

      if (typeof data_user.birthday != "undefined" && data_user.birthday != "") {
        sql_update_cus.push('Birthday ="' + data_user.birthday.substr(0, 10) + '"')
      }

      if (sql_update_cus.length > 0) {
        if (sql_where_cus.length == 0) {
          cb(null, { code: code.DB.PARA_FAIL, msg: "" })
          return
        }
        var sql = "UPDATE customer SET " + sql_update_cus.join(",") + " WHERE " + sql_where_cus.join(" AND ")
        sql_query.push(sql)
      }

      //sql-hall
      var sql_update_ha = []
      var sql_where_ha = []

      if (typeof data_user.cid != "undefined") {
        sql_where_ha.push('HallId = "' + data_user.cid + '"')
      }

      if (typeof data_user.nickName != "undefined") {
        sql_update_ha.push("NickName ='" + data_user.nickName + "'")
      }

      if (typeof data_user.game_currency != "undefined" && typeof data_user["edit"] == "undefined") {
        sql_update_ha.push("Currencies ='" + data_user.game_currency + "'")
      }
      /*
                    if (typeof data_user.ip_whitelist_in != 'undefined') {
                        sql_update_ha.push("IPWhitelistIn ='" + data_user.ip_whitelist_in + "'");
                    }
        
                    if (typeof data_user.ip_whitelist_out != 'undefined') {
                        sql_update_ha.push("IPWhitelistOut ='" + data_user.ip_whitelist_out + "'");
                    }
            */
      if (typeof data_user.api_outdomain != "undefined") {
        sql_update_ha.push("APIOutDomain ='" + data_user.api_outdomain + "'")
      }

      if (typeof data_user.halldesc != "undefined") {
        sql_update_ha.push("HallDesc ='" + data_user.halldesc + "'")
      }

      if (typeof data_user.api_hallownername != "undefined") {
        sql_update_ha.push("APIHallOwnerName ='" + data_user.api_hallownername + "'")
      }

      if (typeof data_user.securekey != "undefined") {
        sql_update_ha.push("SecureKey ='" + data_user.securekey + "'")
      }

      if (sql_update_ha.length > 0) {
        if (sql_where_ha.length == 0) {
          cb({ code: code.DB.PARA_FAIL, msg: "" })
          return
        }
        var sql = "UPDATE hall SET " + sql_update_ha.join(",") + " WHERE " + sql_where_ha.join(" AND ")
        sql_query.push(sql)
      }

      // sql-wallet
      var sql_insert_wallet = []
      if (typeof data_user.game_currency != "undefined") {
        data_user.game_currency.split(",").forEach((item) => {
          var insert_text = sprintf("('%s','%s')", data_user.cid, item)
          sql_insert_wallet.push(insert_text)
        })
        // 多行資料處理，可在 ON DUPLICATE KEY UPDATE 後用 VALUES
        var sql_update_wallet_text = " ON DUPLICATE KEY UPDATE Currency = VALUES(Currency) "

        if (sql_insert_wallet.length > 0) {
          // ex: INSERT INTO wallet ( Cid, Currency) VALUES ('7dT93QK8nYME','CNY'), ('7dT93QK8nYME','INR') ON DUPLICATE KEY UPDATE Currency = VALUES(Currency)
          var sql =
            "INSERT INTO wallet ( Cid, Currency) VALUES " + sql_insert_wallet.join(" , ") + sql_update_wallet_text
          sql_query.push(sql)
        }
      }

      //sql-game
      var sql_insert = []
      var sql_update = []
      var sql_update_text = ""

      var sql_rtp = []
      var sql_denom_set = []
      var sql_sw = []
      // var game_denom = [];

      for (var count = 0; count < data_games.length; count++) {
        if (typeof data_games[count].gameId === "undefined" || typeof data_user.cid === "undefined") {
          cb({ code: code.DB.UPDATE_FAIL, msg: "GameId or Cid undefined" })
          return
        }

        if (typeof data_games[count].rtps != "undefined") {
          sql_rtp.push(
            "WHEN GameId=" +
              data_games[count].gameId +
              ' AND Cid="' +
              data_user.cid +
              '" THEN ' +
              data_games[count].rtps
          )
        }

        if (typeof data_games[count].denoms != "undefined") {
          sql_denom_set.push(
            "WHEN GameId=" +
              data_games[count].gameId +
              " AND Cid='" +
              data_user.cid +
              "' THEN '" +
              data_games[count].denoms[0]["value"] +
              "'"
          )
        }

        if (typeof data_games[count].sw != "undefined") {
          sql_sw.push(
            "WHEN GameId=" +
              data_games[count].gameId +
              ' AND Cid="' +
              data_user.cid +
              '" THEN ' +
              (data_games[count].sw ? 1 : 0)
          )
        }

        var gameId = typeof data_games[count].gameId != "undefined" ? data_games[count].gameId : -1
        var cid = typeof data_user.cid != "undefined" ? data_user.cid : "-1"
        var upid = typeof data_user.upid != "undefined" ? data_user.upid : "-1"
        var hallid = typeof data_user.hallid != "undefined" ? data_user.hallid : "-1"
        var rtp = typeof data_games[count].rtps != "undefined" ? data_games[count].rtps : -1
        //game_setting
        var denoms_set = typeof data_games[count].denoms != "undefined" ? data_games[count].denoms[0]["value"] : ""
        var denoms_run = denoms_set
        var sw = typeof data_games[count].sw != "undefined" ? (data_games[count].sw ? 1 : 0) : 0

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

        if (denoms_set) {
          //遊戲denom設定
          for (let x of data_games[count].denoms) {
            const game_denom_text = `INSERT INTO game_denom_setting(Cid, GameId, Currency, Denom) VALUES("${cid}","${gameId}" , "${x.currency}" ,  "${x.value}"  ) ON DUPLICATE KEY UPDATE Denom = "${x.value}";`

            if (typeof data_user["edit"] == "undefined") {
              sql_query.push(game_denom_text)
            }
            // 修改自己帳戶, 不能新增 "原始開放幣別以外的幣別" (防止送封包來的)
            else if (data_user["Usable_Currency"].indexOf(x.currency) > -1) {
              sql_query.push(game_denom_text)
            }
          }
        }
      }

      if (sql_rtp.length > 0) {
        var sql_rtp_text = " RTPid = CASE " + sql_rtp.join(" ") + " END "
        sql_update.push(sql_rtp_text)
      }
      /*
            //舊denom
            if (sql_denom_set.length > 0) {
                var sql_denom_set_text = ' DenomsSetting = CASE ' + sql_denom_set.join(' ') + ' END ';
                sql_update.push(sql_denom_set_text);
            }
            if (sql_denom_set.length > 0) {
                var denoms_run_text = ' DenomsRunning = CASE ' + sql_denom_set.join(' ') + ' END ';
                sql_update.push(denoms_run_text);
            }
            */
      /*if (sql_sw.length > 0) {
                var sql_sw_text = ' Sw = CASE ' + sql_sw.join(' ') + ' ELSE Sw END ';
                sql_update.push(sql_sw_text);
            }

            if (sql_update.length > 0) { sql_update_text = ' ON DUPLICATE KEY UPDATE ' + sql_update.join(' , '); }
            var sql = 'INSERT INTO game_setting ( GameId,Cid,Upid,HallId,RTPid,DenomsSetting,DenomsRunning,Sw,AutoSpinEnable,ExchangeEnable,JackpotEnable ,AutoSpinJson,ExchangeJson) ' +
                'VALUES ' + sql_insert.join(' , ') + sql_update_text;
            sql_query.push(sql);*/

      /*if (sql_insert.length > 0) {
                for (var count = 0; count < data_games.length; count++) {
                    // 只新增有開的遊戲
                    if (data_games[count].sw) {
                        var sql = 'INSERT INTO game_setting ( GameId,Cid,Upid,HallId,RTPid,DenomsSetting,DenomsRunning,Sw,AutoSpinEnable,ExchangeEnable,JackpotEnable ,AutoSpinJson,ExchangeJson) ' +
                            'VALUES ' + sql_insert.join(' , ') + sql_update_text;
                        sql_query.push(sql);
                    }

                }

            }*/

      //-----------------transaction start---------------
      connection.beginTransaction(function (err) {
        var funcAry = []
        sql_query.forEach(function (sql, index) {
          console.log("----modifyUser_Hall_v2 sql ---", index, sql)
          var temp = function (cb) {
            connection.query(sql, [], function (temp_err, results) {
              if (temp_err) {
                logger.error("userDao.modifyUser_Hall_v2 error", inspect(temp_err))
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
    logger.error("[userDao][modifyUser_Hall_v2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.checkUsrExist = function (data, cb) {
  try {
    var sql = "SELECT COUNT(*) AS count FROM customer WHERE BINARY UserName = ?"
    var args = [data.name]

    console.log("--------checkUsrExis sql------------", sql, JSON.stringify(args))

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data && r_data[0].count > 0) {
          cb(null, { code: code.DB.DATA_DUPLICATE, msg: "User Duplicate" })
        } else {
          cb(null, { code: code.OK })
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][checkUsrExist] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.checkUsrMailExist = function (data, cb) {
  try {
    var sql_where = []
    if (typeof data.userId != "undefined") {
      sql_where.push(" Cid != '" + data.userId + "' ")
    }
    var sql_where_text = sql_where.length > 0 ? " AND " + sql_where.join(" AND ") : ""
    var sql = "SELECT COUNT(*) AS count FROM customer WHERE Email = ? " + sql_where_text
    var args = [data.email]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data && r_data[0].count > 0) {
          cb(null, { code: code.DB.DATA_DUPLICATE, msg: "User Mail Duplicate" })
        } else {
          cb(null, { code: code.OK })
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][checkUsrMailExist] catch err", err)
    cb(null, code.FAIL, null)
  }
}
userDao.checkUsrExist_byCid = function (cid, cb) {
  try {
    var sql = "SELECT COUNT(*) AS count,IsAg,IsSub,Upid,HallId FROM customer WHERE Cid = ?"
    var args = [cid]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data && r_data[0].count === 1) {
          cb(null, { code: code.OK }, r_data[0])
        } else {
          cb(null, { code: code.DB.DATA_EMPTY, msg: "User not exist" })
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][checkUsrExist_byCid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getUsrs_Hall = function (data, cb) {
  try {
    var sql_where = []
    var sql_where_text = ""
    if (data.userName != "") {
      sql_where.push(' A.UserName like "%' + data.userName + '%" ')
    }
    if (data.isDemo != "") {
      sql_where.push(" A.IsDemo = " + data.isDemo)
    }
    if (data.states.length != 0 && data.states != "") {
      sql_where.push(" A.State IN ('" + data.states.join("','") + "')")
    }

    if (data.start_date != "" && data.end_date != "") {
      var start_date = data.start_date
      var end_date = data.end_date

      sql_where.push(' A.AddDate BETWEEN "' + start_date + '" AND "' + end_date + '" ')
    }
    if (typeof data.level !== "undefined" && data.level != "") {
      sql_where.push('  A.Upid = "' + data.upId + '" ')
    }
    if (sql_where.length > 0) {
      sql_where_text = " AND " + sql_where.join(" AND ")
    }

    var sortKey = "A.AddDate"
    var sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        cid: "A.Cid",
        username: "A.UserName",
        state: "A.State",
        downusers: "(SELECT count(*) FROM customer WHERE IsAg = 3 AND Upid = A.Cid )",
        operators: "(SELECT COUNT(*) FROM customer WHERE IsAg = 2 AND IsSub=1 AND Hallid = A.Cid)",
        adddate: "DATE_FORMAT(A.AddDate,'%Y-%m-%d %H:%i:%s')",
        currency: "currencies",
        lastlogin: "DATE_FORMAT(A.LastActDate,'%Y-%m-%d %H:%i:%s')", //分銷商列表修改最近登入 資料庫寫入是這個
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)
    var sql = ""
    /*
            var sql = sprintf('SELECT SQL_CALC_FOUND_ROWS A.Cid AS cid, A.UserName AS userName, A.NickName AS nickName,A.IsDemo AS isDemo, A.AuthorityTemplateID AS authorityId,' +
                'A.State AS state,A.IsSingleWallet AS isSingleWallet, A.IsOnline AS isOnline,DATE_FORMAT(A.AddDate,\'%%Y-%%m-%%d %%H:%%i:%%s\') AS addDate,A.Email AS email,DATE_FORMAT(A.Birthday,\'%%Y-%%m-%%d\') AS birthday,' +
                'B.Currencies AS currencies, B.APIOutDomain AS api_outdomain, B.HallDesc AS halldesc, B.APIHallOwnerName AS api_hallownername,' +
                'B.SecureKey AS securekey,(SELECT count(*) FROM customer WHERE IsAg = 3 AND Upid = A.Cid ) AS downUsers, ' +
                ' (SELECT COUNT(*) FROM customer WHERE IsAg = 2 AND IsSub=1 AND Hallid = A.Cid) AS operators ' +
                'FROM ( SELECT * FROM customer WHERE Upid = -1 AND IsAg = 2 AND IsSub=0 %s ORDER BY Cid DESC LIMIT %i,%i ) A INNER JOIN hall B ON A.Cid = B.HallId %s ',
                sql_where_text, (data.curPage - 1) * data.pageCount, data.pageCount, order_by_text);
        */
    /*
    var sql = sprintf('SELECT SQL_CALC_FOUND_ROWS A.Cid AS cid, A.UserName AS userName, A.NickName AS nickName,A.IsDemo AS isDemo, A.AuthorityTemplateID AS authorityId,' +
        'A.State AS state,A.IsSingleWallet AS isSingleWallet, A.IsOnline AS isOnline,DATE_FORMAT(A.AddDate,\'%%Y-%%m-%%d %%H:%%i:%%s\') AS addDate,A.Email AS email,DATE_FORMAT(A.Birthday,\'%%Y-%%m-%%d\') AS birthday,' +
        'B.Currencies AS currencies, B.APIOutDomain AS api_outdomain, B.HallDesc AS halldesc, B.APIHallOwnerName AS api_hallownername,' +
        'B.SecureKey AS securekey,(SELECT count(*) FROM customer WHERE IsAg = 3 AND Upid = A.Cid ) AS downUsers, ' +
        ' (SELECT COUNT(*) FROM customer WHERE IsAg = 2 AND IsSub=1 AND Hallid = A.Cid) AS operators, ' +
        ' (SELECT COUNT(*) FROM customer WHERE IsAg = 4 AND Hallid = A.Cid) AS players,C.OTPCode' +         
        ' FROM customer A INNER JOIN hall B ON A.Cid = B.HallId ' +
        ' LEFT JOIN otp_auth C ON ( A.Cid = C.Cid AND C.IsAdmin=0) ' +
        ' WHERE A.Upid = "-1" AND A.IsAg = 2 AND A.IsSub=0 %s %s LIMIT %i,%i ',
        sql_where_text, order_by_text, (data.curPage - 1) * data.pageCount, data.pageCount);
    */
    /*if (data.userName == '') {
            sql = sprintf(' SELECT SQL_CALC_FOUND_ROWS A.IsAg As IsAg, A.Cid AS cid, A.HallId AS hallid, A.Upid AS upid, A.UserName AS userName, A.NickName AS nickName,A.IsDemo AS isDemo, A.AuthorityTemplateID AS authorityId, H.Currencies AS game_currency , \
            A.State AS state,A.IsSingleWallet AS isSingleWallet, A.IsOnline AS isOnline,DATE_FORMAT(A.AddDate,"%%Y-%%m-%%d %%H:%%i:%%s") AS addDate,A.Email AS email,A.Birthday AS birthday, A.DC AS dc, \
            A.Currency AS currencies, B.APIOutDomain AS api_outdomain, B.HallDesc AS halldesc, B.APIHallOwnerName AS api_hallownername, \
            B.SecureKey AS securekey,(SELECT count(*) FROM customer WHERE IsAg = 3 AND Upid = A.Cid ) AS downUsers, \
            (SELECT COUNT(*) FROM customer WHERE IsAg = 2 AND IsSub=1 AND Hallid = A.Cid) AS operators, \
            (SELECT COUNT(*) FROM customer WHERE IsAg = 4 AND Hallid = A.Cid) AS players,C.OTPCode \
            FROM customer A INNER JOIN hall B ON A.Cid = B.HallId \
            LEFT JOIN otp_auth C ON ( A.Cid = C.Cid AND C.IsAdmin=0) \
            LEFT JOIN hall H ON (A.Cid = H.HallId) \
            WHERE A.IsAg = 2 AND A.IsSub=0 %s %s LIMIT %i,%i ',
                sql_where_text, order_by_text, (data.curPage - 1) * data.pageCount, data.pageCount);
        }
        else {
            sql = sprintf(' SELECT SQL_CALC_FOUND_ROWS A.IsAg As IsAg, A.Cid AS cid, A.HallId AS hallid, A.Upid AS upid, A.UserName AS userName, A.NickName AS nickName,A.IsDemo AS isDemo, A.AuthorityTemplateID AS authorityId, H.Currencies AS game_currency , \
            A.State AS state,A.IsSingleWallet AS isSingleWallet, A.IsOnline AS isOnline,DATE_FORMAT(A.AddDate,"%%Y-%%m-%%d %%H:%%i:%%s") AS addDate,A.Email AS email,A.Birthday AS birthday, A.DC AS dc, \
            A.Currency AS currencies, B.APIOutDomain AS api_outdomain, B.HallDesc AS halldesc, B.APIHallOwnerName AS api_hallownername, \
            B.SecureKey AS securekey,(SELECT count(*) FROM customer WHERE IsAg = 3 AND Upid = A.Cid ) AS downUsers, \
            (SELECT COUNT(*) FROM customer WHERE IsAg = 2 AND IsSub=1 AND Hallid = A.Cid) AS operators, \
            (SELECT COUNT(*) FROM customer WHERE IsAg = 4 AND Hallid = A.Cid) AS players,C.OTPCode \
            FROM customer A LEFT JOIN hall B ON A.Cid = B.HallId \
            LEFT JOIN otp_auth C ON ( A.Cid = C.Cid AND C.IsAdmin=0) \
            LEFT JOIN hall H ON (A.Cid = H.HallId) \
            WHERE A.IsAg IN(2,3,4) AND A.IsSub=0 %s %s LIMIT %i,%i ',
                sql_where_text, order_by_text, (data.curPage - 1) * data.pageCount, data.pageCount);
        }*/
    sql = sprintf(
      ' SELECT SQL_CALC_FOUND_ROWS A.DC, A.IsAg As IsAg, A.Cid AS cid, A.HallId AS hallid, A.Upid AS upid, A.UserName AS userName, A.NickName AS nickName,A.IsDemo AS isDemo, A.AuthorityTemplateID AS authorityId, H.Currencies AS game_currency , \
            A.State AS state,A.IsSingleWallet AS isSingleWallet, A.IsOnline AS isOnline,DATE_FORMAT(A.AddDate,"%%Y-%%m-%%d %%H:%%i:%%s") AS addDate, DATE_FORMAT(A.LastLoginDate,"%%Y-%%m-%%d %%H:%%i:%%s") AS lastLogin, A.Email AS email,A.Birthday AS birthday, A.DC AS dc, \
            A.Currency AS currencies, B.APIOutDomain AS api_outdomain, B.HallDesc AS halldesc, B.APIHallOwnerName AS api_hallownername, \
            B.SecureKey AS securekey,(SELECT count(*) FROM customer WHERE IsAg = 3 AND Upid = A.Cid ) AS downUsers, \
            (SELECT COUNT(*) FROM customer WHERE IsAg = 2 AND IsSub=1 AND Hallid = A.Cid) AS operators, \
            (SELECT COUNT(*) FROM customer WHERE IsAg = 4 AND Hallid = A.Cid) AS players,C.OTPCode,C.State AS OTPState, A.IsSub, A.Quota AS quota \
            FROM customer A INNER JOIN hall B ON A.Cid = B.HallId \
            LEFT JOIN otp_auth C ON ( A.Cid = C.Cid AND C.IsAdmin=0) \
            LEFT JOIN hall H ON (A.Cid = H.HallId) \
            WHERE A.IsAg = 2 AND A.IsSub=0 %s %s LIMIT %i,%i ',
      sql_where_text,
      order_by_text,
      (data.curPage - 1) * data.pageCount,
      data.pageCount
    )
    var sql2 = "SELECT FOUND_ROWS() AS ROWS;"
    var sql_query = []
    sql_query.push(sql)
    sql_query.push(sql2)

    db.act_transaction("dbclient_g_r", sql_query, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        logger.error(`[userDao][getUsrs_Hall] error ${JSON.stringify(r_code)} ${JSON.stringify(r_data)}`)
        cb(null, r_code, null)
      } else {
        var data = {
          count: r_data[1][0]["ROWS"],
          info: r_data[0],
        }
        cb(null, r_code, data)
      }
    })
  } catch (err) {
    logger.error("[userDao][getUsrs_Hall] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getUsrs_Agent = function (data, cb) {
  try {
    var sql_where = []
    var sql_where_text = ""

    if (data.userName != "") {
      sql_where.push(' A.UserName like "%' + data.userName + '%" ')
    }
    if (data.isDemo != "") {
      sql_where.push(" A.IsDemo = " + data.isDemo)
    }
    if (data.states.length != 0 && data.states != "") {
      sql_where.push(" A.State IN ('" + data.states.join("','") + "')")
    }

    if (data.start_date != "" && data.end_date != "") {
      var start_date = data.start_date
      var end_date = data.end_date
      sql_where.push(' A.AddDate BETWEEN "' + start_date + '" AND "' + end_date + '" ')
    }
    if (sql_where.length > 0) {
      sql_where_text = " AND " + sql_where.join(" AND ")
    }

    var sortKey = "A.AddDate"
    var sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        cid: "A.Cid",
        username: "A.UserName",
        state: "A.State",
        downusers: "(SELECT count(*) FROM customer WHERE IsAg = 4 AND Upid = A.Cid )",
        adddate: "DATE_FORMAT(A.AddDate,'%Y-%m-%d %H:%i:%s')",
        lastlogin: "DATE_FORMAT(A.LastActDate,'%Y-%m-%d %H:%i:%s')", //代理最近登入時間
        isdemo: "A.IsDemo",
        currency: "A.Currency",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }

    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)
    /*
            var sql = sprintf('SELECT SQL_CALC_FOUND_ROWS A.Cid AS cid, A.UserName AS userName, A.NickName AS nickName,A.IsDemo AS isDemo, A.AuthorityTemplateID AS authorityId, ' +
                'A.State AS state, A.IsOnline AS isOnline,DATE_FORMAT(A.AddDate,"%%Y-%%m-%%d %%H:%%i:%%s") AS addDate, A.Upid AS upid ,A.Currency AS currency,A.Email AS email,DATE_FORMAT(A.Birthday,\'%%Y-%%m-%%d\') AS birthday, ' +
                '(SELECT count(*) FROM customer WHERE IsAg = 4 AND Upid = A.Cid ) AS downUsers ' +
                ' FROM ( SELECT * FROM customer WHERE Upid = %i AND IsAg = 3 AND IsSub=0 %s ORDER BY Cid DESC LIMIT %i,%i ) A ',
                data.cid, sql_where_text, (data.curPage - 1) * data.pageCount, data.pageCount, order_by_text); 
                */
    // GROUP_CONCAT: 做到 GROUP BY 並且把資料全部合併成一行。最終顯示結果 CNY,THB
    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS A.DC, A.IsAg AS IsAg, A.HallId AS hallId, A.Upid AS upid, A.Cid AS cid, A.UserName AS userName, A.NickName AS nickName,A.IsDemo AS isDemo, A.AuthorityTemplateID AS authorityId, " +
        'A.State AS state, A.IsOnline AS isOnline,DATE_FORMAT(A.AddDate,"%%Y-%%m-%%d %%H:%%i:%%s") AS addDate,DATE_FORMAT(A.LastLoginDate,"%%Y-%%m-%%d %%H:%%i:%%s") AS lastLogin, A.Upid AS upid ,A.Currency AS currency,GROUP_CONCAT(H.Currency) AS game_currency,A.Email AS email,A.Birthday AS birthday, ' +
        "(SELECT count(*) FROM customer WHERE IsAg = 4 AND Upid = A.Cid ) AS downUsers,C.OTPCode,C.State AS OTPState, A.IsSingleWallet AS isSingleWallet, A.IsSub " +
        " FROM customer A " +
        " LEFT JOIN otp_auth C ON ( A.Cid = C.Cid AND C.IsAdmin=0) " +
        " LEFT JOIN wallet H ON (A.Cid = H.Cid) " +
        ' WHERE A.Upid = "%s" AND  A.IsAg = 3 AND A.IsSub=0  %s GROUP BY A.Cid %s  LIMIT %i,%i ',
      data.cid,
      sql_where_text,
      order_by_text,
      (data.curPage - 1) * data.pageCount,
      data.pageCount
    )

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
  } catch (err) {
    logger.error("[userDao][getUsrs_Agent] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getUsrs_SubHall = function (data, cb) {
  try {
    var sql_where = []
    var sql_where_text = ""

    if (data.userName != "") {
      sql_where.push(' A.UserName like "%' + data.userName + '%" ')
    }
    if (data.isDemo != "") {
      sql_where.push(" A.IsDemo = " + data.isDemo)
    }
    if (data.states.length != 0 && data.states != "") {
      sql_where.push(" A.State IN ('" + data.states.join("','") + "')")
    }

    if (data.start_date != "" && data.end_date != "") {
      var start_date = data.start_date
      var end_date = data.end_date

      sql_where.push(' A.AddDate BETWEEN "' + start_date + '" AND "' + end_date + '" ')
    }

    switch (data.isAg) {
      case 2: // hall
        sql_where.push("A.HallId = '" + data.upid + "' ")
        break
      case 3: // agent
        sql_where.push("A.Upid = '" + data.upid + "' ")
        break
    }

    if (sql_where.length > 0) {
      sql_where_text = " AND " + sql_where.join(" AND ")
    }

    var sortKey = "A.AddDate"
    var sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        cid: "A.Cid",
        username: "A.UserName",
        nickname: "A.NickName",
        state: "A.State",
        adddate: "DATE_FORMAT(A.AddDate,'%Y-%m-%d %H:%i:%s')",
        lastlogin: "DATE_FORMAT(A.LastActDate,'%Y-%m-%d %H:%i:%s')", //管理員最近登入時間
        authdesc: "auth.AuthorityTemplateDesc",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)

    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS A.Cid AS cid, A.HallId AS hallid,A.UpId AS upid, A.UserName AS userName, A.NickName AS nickName,A.IsDemo AS isDemo, A.AuthorityTemplateID AS authorityId,auth.AuthorityTemplateDesc AS authDesc,A.IsAg AS IsAg," +
        " A.State AS state,A.IsSingleWallet AS isSingleWallet, A.IsOnline AS isOnline,DATE_FORMAT(A.AddDate,'%%Y-%%m-%%d %%H:%%i:%%s') AS addDate,DATE_FORMAT(A.LastLoginDate,\"%%Y-%%m-%%d %%H:%%i:%%s\") AS lastLogin,A.Email AS email,A.Birthday AS birthday," +
        " B.Currencies AS currencies, " +
        " C.OTPCode, C.State AS OTPState, A.IsSub " +
        " FROM customer A" +
        " LEFT JOIN otp_auth C ON ( A.Cid = C.Cid AND C.IsAdmin=0) " +
        " LEFT JOIN back_office_authority auth ON(auth.AuthorityTemplateID = A.AuthorityTemplateID) " +
        " INNER JOIN sub_user B ON A.Cid = B.Cid " +
        " WHERE A.IsAg = %s AND A.IsSub = 1 %s %s LIMIT %i,%i ",
      data.isAg,
      sql_where_text,
      order_by_text,
      (data.curPage - 1) * data.pageCount,
      data.pageCount
    )

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
  } catch (err) {
    logger.error("[userDao][getUsrs_SubHall] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// userDao.modifyUser_SubHall = function (data, cb) {

//     var data_user = data;
//     var cid = 0;

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

//             var sql_update = [];
//             var sql_where = [];

//             if (typeof data_user.name != 'undefined') {
//                 sql_where.push('UserName ="' + data_user.name + '"');
//             }

//             if (typeof data_user.cid != 'undefined' && data_user.cid != '') {
//                 sql_where.push('Cid ="' + data_user.cid + '"');
//             }

//             if (typeof data_user.password != 'undefined' && data_user.password != '') {
//                 sql_update.push('Passwd ="' + data_user.password + '"');
//             }

//             if (typeof data_user.nickName != 'undefined' && data_user.nickName != '') {
//                 sql_update.push('NickName ="' + data_user.nickName + '"');
//             }

//             if (typeof data_user.authorityId != 'undefined') {
//                 sql_update.push('AuthorityTemplateID =' + data_user.authorityId);
//             }

//             if (typeof data_user.isSingWallet != 'undefined') {
//                 sql_update.push('IsSingleWallet =' + data_user.isSingWallet);
//             }

//             if (typeof data_user.quota != 'undefined') {
//                 sql_update.push('Quota =' + data_user.quota);
//             }

//             if (typeof data_user.state != 'undefined') {
//                 sql_update.push('State ="' + data_user.state + '"');
//             }

//             if (typeof data_user.birthday != 'undefined' && data_user.birthday != '') {
//                 sql_update.push('Birthday ="' + data_user.birthday.substr(0, 10) + '"');
//             }

//             if (sql_update.length === 0 || sql_where.length === 0) {
//                 return connection.rollback(function () {
//                     cb(null, { code: code.DB.PARA_FAIL, msg: '' });
//                 });
//             }

//             var sql = 'UPDATE customer SET ' + sql_update.join(",") + " WHERE " + sql_where.join(" AND ");
//             var args = [];
//             connection.query(sql, args, function (err, results) {

//                 if (err) {
//                     return connection.rollback(function () {
//                         cb(null, { code: code.DB.UPDATE_FAIL, msg: err });
//                     });
//                 }

//                 var sql_update = [];
//                 var sql_where = [];

//                 if (typeof data_user.cid != 'undefined') {

//                     sql_where.push('HallId ="' + data_user.cid + '"');
//                 }

//                 if (typeof data_user.nickName != 'undefined') {
//                     sql_update.push('NickName ="' + data_user.nickName + '"');
//                 }

//                 if (typeof data_user.currencies != 'undefined') {
//                     sql_update.push('Currencies ="' + data_user.currencies + '"');
//                 }

//                 // if (typeof data_user.ip_whitelist_in != 'undefined') {
//                 //     sql_update.push('IPWhitelistIn ="' + data_user.ip_whitelist_in + '"');
//                 // }

//                 // if (typeof data_user.ip_whitelist_out != 'undefined') {
//                 //     sql_update.push('IPWhitelistOut ="' + data_user.ip_whitelist_out + '"');
//                 // }

//                 if (typeof data_user.api_outdomain != 'undefined') {
//                     sql_update.push('APIOutDomain ="' + data_user.api_outdomain + '"');
//                 }

//                 if (typeof data_user.halldesc != 'undefined') {
//                     sql_update.push('HallDesc ="' + data_user.halldesc + '"');
//                 }

//                 if (typeof data_user.api_hallownername != 'undefined') {
//                     sql_update.push('APIHallOwnerName ="' + data_user.api_hallownername + '"');
//                 }

//                 if (typeof data_user.securekey != 'undefined') {
//                     sql_update.push('SecureKey ="' + data_user.securekey + '"');
//                 }

//                 if (sql_update.length === 0 || sql_where.length === 0) {
//                     return connection.rollback(function () {
//                         cb(null, { code: code.DB.PARA_FAIL, msg: '' });
//                     });
//                 }

//                 var sql = 'UPDATE hall SET ' + sql_update.join(",") + " WHERE " + sql_where.join(" AND ");
//                 var args = [];

//                 connection.query(sql, args, function (err, results) {

//                     if (err) {
//                         return connection.rollback(function () {
//                             cb(null, { code: code.DB.UPDATE_FAIL, msg: err });
//                         });
//                     }
//                     connection.commit(function (err) {
//                         if (err) {
//                             return connection.rollback(function () {
//                                 cb(null, { code: code.DB.QUERY_FAIL, msg: err });
//                             });
//                         }
//                         connection.release();
//                         cb(null, { code: code.OK, msg: "" });

//                     });

//                 });
//             });
//         });
//     });

// };

userDao.modifyUser_SubHall_v2 = function (data, cb) {
  try {
    var data_user = data

    var cid = 0

    //要執行的-sql
    var sql_query = []
    //sql-customer
    var sql_update_cus = []
    var sql_where_cus = []

    if (typeof data_user.name != "undefined") {
      sql_where_cus.push("UserName = '" + data_user.name + "'")
    }

    if (typeof data_user.cid != "undefined" && data_user.cid != "") {
      sql_where_cus.push("Cid = '" + data_user.cid + "'")
    }

    if (typeof data_user.password != "undefined" && data_user.password != "") {
      sql_update_cus.push("Passwd ='" + data_user.password + "'")
    }

    if (typeof data_user.nickName != "undefined" && data_user.nickName != "") {
      sql_update_cus.push("NickName ='" + data_user.nickName + "'")
    }

    if (typeof data_user.authorityId != "undefined") {
      sql_update_cus.push("AuthorityTemplateID =" + data_user.authorityId)
    }

    if (typeof data_user.isSingleWallet != "undefined" && typeof data_user["edit"] == "undefined") {
      sql_update_cus.push("IsSingleWallet ='" + data_user.isSingleWallet + "'")
    }

    if (typeof data_user.quota != "undefined") {
      sql_update_cus.push("Quota =" + data_user.quota)
    }

    if (typeof data_user.state != "undefined") {
      sql_update_cus.push('State ="' + data_user.state + '"')
    }

    if (typeof data_user.email != "undefined") {
      sql_update_cus.push('Email ="' + data_user.email + '"')
    }

    if (typeof data_user.birthday != "undefined" && data_user.birthday != "") {
      sql_update_cus.push('Birthday ="' + data_user.birthday.substr(0, 10) + '"')
    }

    if (sql_update_cus.length > 0) {
      if (sql_where_cus.length == 0) {
        cb({ code: code.DB.PARA_FAIL, msg: "" })
        return
      }
      var sql = "UPDATE customer SET " + sql_update_cus.join(",") + " WHERE " + sql_where_cus.join(" AND ")
      sql_query.push(sql)
    }

    //sql-hall
    var sql_update_ha = []
    var sql_where_ha = []

    if (typeof data_user.cid != "undefined") {
      sql_where_ha.push('HallId ="' + data_user.cid + '"')
    }

    if (typeof data_user.nickName != "undefined") {
      sql_where_ha.push("NickName ='" + data_user.nickName + "'")
    }

    if (typeof data_user.currencies != "undefined") {
      sql_update_ha.push("Currencies ='" + data_user.currencies + "'")
    }

    // if (typeof data_user.ip_whitelist_in != 'undefined') {
    //     sql_update_ha.push("IPWhitelistIn ='" + data_user.ip_whitelist_in + "'");
    // }

    // if (typeof data_user.ip_whitelist_out != 'undefined') {
    //     sql_update_ha.push("IPWhitelistOut ='" + data_user.ip_whitelist_out + "'");
    // }

    if (typeof data_user.api_outdomain != "undefined") {
      sql_update_ha.push("APIOutDomain ='" + data_user.api_outdomain + "'")
    }

    if (typeof data_user.halldesc != "undefined") {
      sql_update_ha.push("HallDesc ='" + data_user.halldesc + "'")
    }

    if (typeof data_user.api_hallownername != "undefined") {
      sql_update_ha.push("APIHallOwnerName ='" + data_user.api_hallownername + "'")
    }

    if (typeof data_user.securekey != "undefined") {
      sql_update_ha.push("SecureKey ='" + data_user.securekey + "'")
    }

    if (sql_update_ha.length > 0) {
      if (sql_where_ha.length == 0) {
        cb({ code: code.DB.PARA_FAIL, msg: "" })
        return
      }
      var sql = "UPDATE hall SET " + sql_update_ha.join(",") + " WHERE " + sql_where_ha.join(" AND ")
      sql_query.push(sql)
    }

    pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
      if (err) {
        connection.release()
        cb({ code: code.DB.GET_CONNECT_FAIL, msg: err })
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
                return cb(null, code.ok)
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
                  return cb({ code: code.DB.QUERY_FAIL, msg: err })
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
    logger.error("[userDao][modifyUser_SubHall_v2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.modifyUser_Agent = function (data, cb) {
  try {
    var data_user = data
    var sql_query = []
    var sql_update = []
    var sql_where = []

    if (typeof data_user.name != "undefined") {
      sql_where.push('UserName ="' + data_user.name + '"')
    }

    if (typeof data_user.cid != "undefined" && data_user.cid != "") {
      sql_where.push('Cid ="' + data_user.cid + '"')
    }

    if (typeof data_user.password != "undefined" && data_user.password != "") {
      sql_update.push('Passwd ="' + data_user.password + '"')
    }

    if (typeof data_user.nickName != "undefined" && data_user.nickName != "") {
      sql_update.push('NickName ="' + data_user.nickName + '"')
    }

    if (typeof data_user.authorityId != "undefined" && typeof data_user["edit"] == "undefined") {
      sql_update.push("AuthorityTemplateID =" + data_user.authorityId)
    }

    if (typeof data_user.isSingWallet != "undefined" && typeof data_user["edit"] == "undefined") {
      sql_update.push("IsSingleWallet =" + data_user.isSingWallet)
    }

    if (typeof data_user.quota != "undefined") {
      sql_update.push("Quota =" + data_user.quota)
    }

    if (typeof data_user.state != "undefined" && typeof data_user["edit"] == "undefined") {
      sql_update.push('State ="' + data_user.state + '"')
    }

    if (typeof data_user.modifyLvl != "undefined") {
      sql_update.push('ModifyLvl ="' + data_user.modifyLvl + '"')
    }
    if (typeof data_user.email != "undefined") {
      sql_update.push('Email ="' + data_user.email + '"')
    }

    if (typeof data_user.birthday != "undefined" && data_user.birthday != "") {
      sql_update.push('Birthday ="' + data_user.birthday.substr(0, 10) + '"')
    }

    if (sql_update.length === 0 || sql_where.length === 0) {
      return cb(null, { code: code.DB.PARA_FAIL, msg: "" })
    }

    if (sql_update.length > 0) {
      if (sql_where.length == 0) {
        cb({ code: code.DB.PARA_FAIL, msg: "" })
        return
      }
      var sql = "UPDATE customer SET " + sql_update.join(",") + " WHERE " + sql_where.join(" AND ")
      sql_query.push(sql)
    }

    pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
      }

      // sql-wallet
      var sql_insert_wallet = []
      if (typeof data_user.game_currency != "undefined") {
        data_user.game_currency.split(",").forEach((item) => {
          var insert_text = sprintf("('%s','%s')", data_user.cid, item)
          sql_insert_wallet.push(insert_text)
        })
        // 多行資料處理，可在 ON DUPLICATE KEY UPDATE 後用 VALUES
        var sql_update_wallet_text = " ON DUPLICATE KEY UPDATE Currency = VALUES(Currency) "

        if (sql_insert_wallet.length > 0) {
          // ex: INSERT INTO wallet ( Cid, Currency) VALUES ('7dT93QK8nYME','CNY'), ('7dT93QK8nYME','INR') ON DUPLICATE KEY UPDATE Currency = VALUES(Currency)
          var sql =
            "INSERT INTO wallet ( Cid, Currency) VALUES " + sql_insert_wallet.join(" , ") + sql_update_wallet_text
          sql_query.push(sql)
        }
      }

      connection.beginTransaction(function (err) {
        var funcAry = []
        sql_query.forEach(function (sql, index) {
          var temp = function (cb) {
            connection.query(sql, [], function (temp_err, results) {
              if (temp_err) {
                connection.rollback(function () {
                  return cb(code.DB.UPDATE_FAIL)
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
                  return cb(null, { code: code.DB.UPDATE_FAIL, msg: err })
                })
              } else {
                connection.release()
                return cb(null, { code: code.OK, msg: "" })
              }
            })
          }
        })
      })
    })
  } catch (err) {
    logger.error("[userDao][modifyUser_Agent] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.modifyUser_Player = function (data, cb) {
  try {
    var data_user = data
    var cid = 0
    var sql_update = []
    var sql_where = []

    if (typeof data_user.name != "undefined") {
      sql_where.push('UserName ="' + data_user.name + '"')
    }

    if (typeof data_user.cid != "undefined" && data_user.cid != "") {
      sql_where.push('Cid ="' + data_user.cid + '"')
    }

    if (typeof data_user.password != "undefined" && data_user.password != "") {
      sql_update.push('Passwd ="' + data_user.password + '"')
    }

    if (typeof data_user.nickName != "undefined") {
      sql_update.push('NickName ="' + data_user.nickName + '"')
    }

    if (typeof data_user.quota != "undefined") {
      sql_update.push("Quota =" + data_user.quota)
    }

    if (typeof data_user.realName != "undefined") {
      sql_update.push('RealName ="' + data_user.realName + '"')
    }

    if (typeof data_user.birthday != "undefined" && data_user.birthday != "") {
      sql_update.push('Birthday ="' + data_user.birthday.substr(0, 10) + '"')
    }

    if (typeof data_user.address != "undefined") {
      sql_update.push('Address ="' + data_user.address + '"')
    }

    if (typeof data_user.email != "undefined") {
      sql_update.push('Email ="' + data_user.email + '"')
    }

    if (typeof data_user.state != "undefined") {
      sql_update.push('State ="' + data_user.state + '"')
    }

    if (typeof data_user.modifyLvl != "undefined") {
      sql_update.push('ModifyLvl ="' + data_user.modifyLvl + '"')
    }

    if (typeof data_user.isKill !== "undefined") {
      const paramsIsKill = [0, 1]

      const isValidParam = paramsIsKill.includes(Number(data_user.isKill))

      if (isValidParam) {
        sql_update.push(`isKill = "${data_user.isKill}" `)
      }
    }

    if (sql_update.length === 0 || sql_where.length === 0) {
      cb(null, { code: code.DB.PARA_FAIL, msg: "" })
      return
    }

    var sql = "UPDATE customer SET " + sql_update.join(",") + " WHERE " + sql_where.join(" AND ")
    var args = []

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][modifyUser_Player] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// userDao.request_getUsers_ha = function (data, cb) {

//     var sql = 'SELECT A.Cid AS cid, A.UserName AS userName, A.NickName AS nickName,A.IsDemo AS isDemo, A.AuthorityTemplateID AS authorityId,' +
//         ' A.State AS state, A.IsOnline AS isOnline, Date_Format(A.AddDate,"%Y-%m-%d %H:%i:%s") AS addDate, A.Upid AS upid ,A.Currency AS currency ,A.Email AS email ,A.Birthday AS Birthday,otp_auth.OTPCode ' +
//         ' FROM ( SELECT * FROM customer WHERE Cid = ? AND IsAg = 3 ) A LEFT JOIN otp_auth ON (otp_auth.Cid=A.Cid  AND otp_auth.IsAdmin=0) ';
//     var args = [data.cid];

//     console.log('-request_getUsers_ha sql-', sql, JSON.stringify(args));

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// };

userDao.getUsrDetail_Agent = function (data, cb) {
  try {
    var sql_join = []
    var sql_join_text = ""
    // 一般使用者或是管理者登入，然後往下一層一層點選時查詢 wallet
    if (data.isSub == 0 || (data.isSub == 1 && data.isNextLevel == 1)) {
      sql_join_text = "GROUP_CONCAT(D.Currency) AS game_currency,"
      sql_join.push(" INNER JOIN wallet D ON D.Cid = A.Cid")
    } else {
      sql_join_text = "GROUP_CONCAT(D.Currencies) AS game_currency,"
      sql_join.push(" LEFT JOIN hall AS D ON (D.HallId = A.Cid)")
    }
    var sql =
      "SELECT A.Cid AS cid,A.HallId AS hallId, A.UserName AS userName, A.NickName AS nickName,A.IsDemo AS isDemo, A.AuthorityTemplateID AS authorityId," +
      " A.State AS state, A.IsOnline AS isOnline, " +
      sql_join_text +
      ' DATE_FORMAT(A.AddDate,"%Y-%m-%d %H:%i:%s") AS addDate, A.Upid AS upid ,A.Currency AS currency ,A.Email AS email,A.Birthday AS birthday,B.OTPCode, B.State AS OTPState, ' +
      " C.IsSingleWallet  AS isSingleWallet, (SELECT count(*) FROM customer WHERE IsAg = 4 AND Upid = A.Cid ) AS downUsers, A.IsSingleWallet AS isSingleWallet " +
      " FROM ( SELECT * FROM customer WHERE Cid = ? AND IsAg = 3 ) A " +
      " LEFT JOIN otp_auth AS B ON (B.Cid = A.Cid AND B.IsAdmin=0) " +
      " LEFT JOIN customer AS C ON (C.Cid = A.HallId AND C.IsAg=2) " +
      sql_join

    var args = [data.cid]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, { code: code.OK }, r_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][getUsrDetail_Agent] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getUsrs_Player = function (data, cb) {
  try {
    var sql_where = []
    var sql_where_text = ""

    if (data.userName != "") {
      sql_where.push(' A.UserName like "%' + data.userName + '%"')
    }
    if (data.isDemo != "") {
      sql_where.push(" A.IsDemo = " + data.isDemo)
    }
    if (data.states.length != 0 && data.states != "") {
      sql_where.push(" A.State IN ('" + data.states.join("','") + "') ")
    }
    if (data.start_date != "" && data.end_date != "") {
      var start_date = data.start_date
      var end_date = data.end_date

      sql_where.push(' A.AddDate BETWEEN "' + start_date + '" AND "' + end_date + '" ')
    }

    if (sql_where.length > 0) {
      sql_where_text = " AND " + sql_where.join(" AND ")
    }
    var sortKey = "A.AddDate"
    var sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        cid: "A.Cid",
        username: "A.UserName",
        realname: "A.RealName",
        nickname: "A.NickName",
        state: "A.State",
        quota: "A.Quota",
        lastlogin: "DATE_FORMAT(A.LastLoginDate,'%Y-%m-%d %H:%i:%s')",
        adddate: "DATE_FORMAT(A.AddDate,'%Y-%m-%d %H:%i:%s')",
        totalrealbetgold: "A.TotalRealBetGold",
        totalwingold: "A.TotalWinGold",
        totaljpgold: "A.TotalJPGold",
        totalnetwin: "(A.TotalRealBetGold - A.TotalWinGold)",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    // var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType);
    var order_by_text = sprintf(" GROUP BY A.Cid ORDER BY %s %s", sortKey, sortType)
    /*
            var sql = 'SELECT A.Cid AS cid, A.UserName AS userName, A.NickName AS nickName,A.IsDemo AS isDemo, A.Upid AS upid, A.HallId AS hallid,A.Quota AS quota, ' +
                'A.State AS state, A.IsOnline AS isOnline,DATE_FORMAT(A.AddDate,"%Y-%m-%d %H:%i:%s") AS addDate, A.Currency AS currency, A.RealName AS realName, DATE_FORMAT(A.Birthday,"%Y-%m-%d") AS birthday, A.Address AS address, A.Email AS email ' +
                ' FROM ( SELECT * FROM customer WHERE Upid = ? AND IsAg = 4 AND IsSub=0' + sql_where_text + ' ORDER BY Cid DESC LIMIT ?,? ) A ' + order_by_text;
            var args = [data.cid, (data.curPage - 1) * data.pageCount, data.pageCount];
        */
    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS A.DC, A.IsAg As IsAg, A.Cid AS cid, A.UserName AS userName, A.NickName AS nickName,A.IsDemo AS isDemo, A.Upid AS upid, A.HallId AS hallid,A.Quota AS quota, " +
        ' A.State AS state, A.IsOnline AS isOnline,DATE_FORMAT(A.AddDate,"%%Y-%%m-%%d %%H:%%i:%%s") AS addDate, DATE_FORMAT(A.LastLoginDate,"%%Y-%%m-%%d %%H:%%i:%%s") AS lastLogin, A.Currency AS currency, A.RealName AS realName, ' +
        "A.Birthday AS birthday, A.Address AS address, A.Email AS email, GROUP_CONCAT(H.Currency) AS game_currency " +
        ", A.TotalRealBetGold as totalRealBetGold, A.TotalWinGold as totalWinGold, A.TotalJPGold as totalJPGold " +
        ", A.isKill as isKill, A.isPromo as isPromo " +
        " FROM customer A LEFT JOIN wallet H ON A.Upid = H.Cid " +
        ' WHERE A.Upid = "%s" AND A.IsAg = 4 AND A.IsSub=0  %s %s LIMIT %i,%i  ',
      data.cid,
      sql_where_text,
      order_by_text,
      (data.curPage - 1) * data.pageCount,
      data.pageCount
    )

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
    // db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
    //     cb(null, r_code, r_data);
    // });
  } catch (err) {
    logger.error("[userDao][getUsrs_Player] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/**
 * 取得【玩家明細】
 *
 * @param {*} data
 * @param {*} cb
 */
userDao.getUsrDetail_Player = function (data, cb) {
  try {
    var sql_where = []
    var sql_where_text = ""

    if (typeof data.userName != "undefined" && data.userName != "") {
      sql_where.push('UserName = "' + data.userName + '"')
    }
    if (typeof data.isDemo != "undefined" && data.isDemo != "") {
      sql_where.push("IsDemo = " + data.isDemo)
    }
    if (typeof data.states != "undefined" && data.states.length != 0 && data.states != "") {
      sql_where.push("State IN ('" + data.states.join("','") + "') ")
    }
    if (
      typeof data.start_date != "undefined" &&
      typeof data.end_date != "undefined" &&
      data.start_date != "" &&
      data.end_date != ""
    ) {
      var start_date = data.start_date
      var end_date = data.end_date

      sql_where.push('AddDate BETWEEN "' + start_date + '" AND "' + end_date + '" ')
    }
    if (sql_where.length > 0) {
      sql_where_text = " AND " + sql_where.join(" AND ")
    }

    var sql =
      "SELECT A.Cid AS cid, A.UserName AS userName, A.NickName AS nickName,A.IsDemo AS isDemo, A.Upid AS upid, A.HallId AS hallid, " +
      'A.State AS state, A.IsOnline AS isOnline,DATE_FORMAT(A.AddDate,"%Y-%m-%d %H:%i:%s") AS addDate, A.Currency AS currency,A.RealName AS realName,A.Birthday AS birthday, A.Address AS address, A.Email AS email,' +
      " A.IsSingleWallet  AS isSingleWallet, " +
      " A.isKill as isKill, A.isPromo as isPromo" +
      " FROM ( SELECT * FROM customer WHERE Cid = ? AND IsAg = 4 AND IsSub = 0" +
      sql_where_text +
      " ) A" +
      " LEFT JOIN customer AS C ON (C.Cid = A.HallId AND C.IsAg=2) "
    var args = [data.cid]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][getUsrDetail_Player] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getUsrDetail_Hall = function (data, cb) {
  try {
    let UpId = ""
    var sql_join_text = ""
    var sql_join = []
    if (data.isSub == 0 || data.isSub == undefined) {
      UpId = data.isSub != 1 ? "Upid = '" + data.upid + "' AND " : ""
      sql_join_text =
        ", B.Currencies AS game_currency, B.APIOutDomain AS api_outdomain, B.HallDesc AS halldesc, B.APIHallOwnerName AS api_hallownername, B.SecureKey AS securekey, A.IsSub "
      sql_join.push(" INNER JOIN hall B ON A.Cid = B.HallId ")
    } else {
      if (data.upid == -1) {
        //Only for subHall admin enter Reseller Detail
        UpId = "HallId = '" + data.upid + "' AND "
      } else {
        //For subHall admin login then enter subHall then enter Reseller Detail
        UpId = data.isSub != 1 ? "Upid = '" + data.upid + "' AND " : ""
        sql_join_text =
          ", B.Currencies AS game_currency, B.APIOutDomain AS api_outdomain, B.HallDesc AS halldesc, B.APIHallOwnerName AS api_hallownername, B.SecureKey AS securekey, A.IsSub "
        sql_join.push(" INNER JOIN hall B ON A.Cid = B.HallId ")
      }
    }

    var sql =
      "SELECT A.HallId AS hallid, A.Upid AS upid, A.Cid AS cid, A.UserName AS userName, A.NickName AS nickName,A.IsDemo AS isDemo, A.AuthorityTemplateID AS authorityId," +
      "A.State AS state,A.IsSingleWallet AS isSingleWallet, A.IsOnline AS isOnline,DATE_FORMAT(A.AddDate,'%Y-%m-%d %H:%i:%s') AS addDate,A.Email AS email,A.Birthday AS birthday,A.DC AS dc," +
      "A.Currency AS currencies , C.OTPCode, C.State AS OTPState " +
      sql_join_text +
      "FROM ( SELECT * FROM customer WHERE " +
      UpId +
      "IsAg = 2 AND Cid = ? ) A " +
      sql_join +
      "LEFT JOIN otp_auth AS C ON (C.Cid = A.Cid AND C.IsAdmin=0) "

    var args = [data.cid]
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][getUsrDetail_Hall] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getGameRevenue_games_v2Cid = function (data, cb) {
  try {
    // console.log('getGameRevenue_games_v2 param', JSON.stringify(data));
    if (typeof data.cid === "undefined") {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }

    var sql_where = []

    if (typeof data.cid !== "undefined") {
      sql_where.push("(w.Cid ='" + data.cid + "')")
    }

    if (sql_where.length == 0) {
      cb(null, { code: code.FAIL, msg: err }, null)
      return
    }

    //haId, agId
    if (typeof data.upid != "undefined") {
      var sql_where_upId = ""
      // hall 登入，編輯自己的帳號資料
      if (data.user_level === 2 && data.upid != -1 && data.sel_table == "game_revenue_hall") {
        sql_where_upId = " w.Upid ='" + data.upid + "'"
      } else if (data.user_level === 2 && data.sel_table == "game_revenue_agent") {
        sql_where_upId = " w.HallId ='" + data.upid + "'"
      }
      if (data.user_level === 3) {
        sql_where_upId = " w.UpId ='" + data.upid + "'"
      }
      if (sql_where_upId != "") sql_where.push(sql_where_upId)
    }
    var sql_where_text = sql_where.join(" AND ")

    var sortKey = "(SUM(w.BetGold)-SUM(w.WinGold-w.JPGold))"
    var sortType = conf.SORT_TYPE[1]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        gameid: "w.GameId",
        rounds: "SUM(w.Rounds)",
        betgold: "SUM(w.BetGold)",
        wingold: "SUM(w.WinGold-w.JPGold)",
        ggr: "(SUM(w.WinGold)-SUM(w.BetGold))",
        rtp: "(SUM(w.WinGold)/SUM(w.BetGold))",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }

    var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)

    var changeCurrency =
      typeof data.downCurrency != "undefined" && data.downCurrency !== "" ? data.downCurrency : data.currency

    var sql_limit =
      data.isPage === true
        ? " LIMIT " + (data.page - 1) * data.pageCount + "," + data.pageCount
        : " LIMIT  " + data.index + "," + data.pageCount //限制筆數

    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS w.GGId,w.Id, w.GameId, w.Cid,  FORMAT(SUM(w.Rounds),0) AS rounds, w.Currency , FORMAT(SUM(w.BetGold),2) AS BetGold, \
        FORMAT(SUM(w.BetPoint),2) AS BetPoint,  FORMAT(SUM(w.WinGold-w.JPGold),2) AS WinGold, FORMAT(SUM(w.JPPoint),2) AS JPPoint,  FORMAT(SUM(w.JPGold),2) AS JPGold,    \
        IFNULL(FORMAT(SUM(w.WinGold)-SUM(w.BetGold),2),0) AS NetWin,    \
        IFNULL(FORMAT((SUM(w.WinGold)/SUM(w.BetGold))*100,2),0) AS RTP, IFNULL(FORMAT(SUM(w.BetGold)/SUM(w.Rounds),2),0) AS avgBet   \
        FROM %s w                                           \
        WHERE 1 AND %s GROUP BY w.GameId,w.Currency   \
        %s %s ",
      data.sel_table,
      sql_where_text,
      order_by_text,
      sql_limit
    )

    var sql2 = "SELECT FOUND_ROWS() AS ROWS;"
    var sql_query = []
    sql_query.push(sql)
    sql_query.push(sql2)

    db.act_transaction("dbclient_w_r", sql_query, function (r_code, r_data) {
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
  } catch (err) {
    logger.error("[userDao][getGameRevenue_games_v2Cid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getUser_byUserName = function (data, cb) {
  try {
    var sql =
      "SELECT A.DC, A.UserName,A.Passwd,A.NickName,A.IsAg AS Level,A.IsSub,A.Upid, A.FirstLogin, A.AuthorityTemplateID AS AuthorityTemplateId,A.IsDemo,A.LastLoginDate," +
      "A.IsSingleWallet,A.Currency,A.Quota,A.State,A.IsOnline,A.Cid,A.HallId,A.Upid,ha.State AS ha_state," +
      "B.Currencies, B.APIOutDomain,B.HallDesc," +
      "A.Birthday AS Birthday,A.Email,A.Address," +
      "B.APIHallOwnerName,B.SecureKey,C.OTPCode,C.State AS isOTPState " +
      "FROM (SELECT * FROM customer WHERE BINARY UserName = ?) AS A " +
      " LEFT JOIN hall AS B ON A.Cid=B.HallId " +
      " LEFT JOIN customer ha ON A.HallId = ha.Cid " +
      " LEFT JOIN otp_auth AS C ON (C.Cid = A.Cid AND C.IsAdmin=0) "

    var args = [data.name]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data && r_data.length === 1) {
          var usr = r_data[0]
          console.log("----------------------", JSON.stringify(usr))
          console.log("----------------------", JSON.stringify(data))
          if (usr.Passwd === data.password) {
            cb(null, { code: code.OK, msg: "" }, usr)
          } else {
            cb(null, { code: code.USR.USER_PASSWORD_FAIL, msg: "Invalid Password" }, null)
          }
        } else {
          cb(null, { code: code.DB.DATA_EMPTY, msg: "Invalid User" }, null)
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][getUser_byUserName] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 以用戶名稱取得用戶資料
// data: {userName}
userDao.getUser_byUserName2 = function (data, cb) {
  try {
    let sqlWhere = " WHERE BINARY UserName = ?"
    let argsWhere = [data.userName]

    var sql = "SELECT Cid, UserName, Upid, HallId, IsAg, IsSub FROM game.customer" + sqlWhere
    var args = [...argsWhere]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][getUser_byUserName2] catch err", err)
    cb(null, { code: code.FAIL }, null)
  }
}

// userDao.getUserPR_byUserName = function (data, cb) {

//     var sql = 'SELECT A.UserName,A.Passwd,A.NickName,A.IsAg AS Level,A.IsSub,A.Upid,' +
//         'A.AuthorityTemplateID AS AuthorityTemplateId,A.IsDemo,A.LastLoginDate,' +
//         'A.IsSingleWallet,A.Currency,A.Quota,A.State,A.IsOnline,A.Cid,A.HallId,' +
//         'B.Currencies, B.APIOutDomain,B.HallDesc,' +
//         'B.APIHallOwnerName,B.SecureKey ' +
//         'FROM (SELECT * FROM customer WHERE UserName = ?) AS A ' +
//         'LEFT JOIN hall AS B ON A.Cid=B.HallId';

//     var args = [data.name];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             if (r_data && r_data.length === 1) {
//                 var usr = r_data[0];
//                 if (usr.Passwd === data.password) {
//                     cb(null, { code: code.OK, msg: "" }, usr);
//                 } else {
//                     cb(null, { code: code.DB.DATA_EMPTY, msg: "Invalid Password" }, null);
//                 }
//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY, msg: "Invalid User" }, null);
//             }
//         }
//     });

// };

// userDao.getAuthorityTemps = function (cb) {

//     var sql = 'SELECT `AuthorityTemplateID`,`AuthorityTemplateDesc`,`AuthorityType`,`AuthorityJson` FROM back_office_authority';
//     var args = [];

//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null);
//             return;
//         }
//         connection.query(sql, args, function (err, res) {
//             connection.release();
//             if (err) {
//                 cb({ code: code.CONFIG.LOAD_CONFIG_AUTHORITY_FAIL, msg: err.stack }, null);
//             } else {
//                 if (res) {
//                     var authoritys = res;
//                     cb({ code: code.OK, msg: "" }, authoritys);
//                 } else {
//                     cb({ code: code.CONFIG.LOAD_CONFIG_AUTHORITY_EMPTY, msg: "Authority Empty" }, null);
//                 }
//             }
//         });
//     });

// };

// userDao.getAuthorityFuncs = function (cb) {

//     var sql = 'SELECT `FunctionID`,`FunctionGroupL`,`FunctionGroupM`,' +
//         '`FunctionGroupS`,`FunctionAction`, `FunctionNameE`, `FunctionNameC`, ' +
//         '`FunctionNameG`, `FunctionPath`,`FunctionDesc`,`FunctionState` FROM back_office_function_list ORDER BY  FunctionID ASC';

//     var args = [];
//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null);
//             return;
//         }
//         connection.query(sql, args, function (err, res) {
//             connection.release();

//             if (err) {
//                 cb({ code: code.CONFIG.LOAD_CONFIG_FUNCTION_FAIL, msg: err.stack }, null);
//             } else {
//                 if (res) {
//                     var functions = res;
//                     cb({ code: code.OK, msg: "" }, functions);
//                 } else {
//                     cb({ code: code.CONFIG.LOAD_CONFIG_FUNCTION_EMPTY, msg: "Authority Empty" }, null);
//                 }
//             }

//         });
//     });
// };

userDao.getLevel = function (cb) {
  try {
    var sql = "SELECT `AuthorityType`,`AuthorityTypeName` FROM back_office_authority_type ORDER BY AuthorityType ASC"

    var args = []
    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null)
        return
      }
      connection.query(sql, args, function (err, res) {
        connection.release()
        if (err) {
          cb({ code: code.CONFIG.LOAD_CONFIG_LEVEL_FAIL, msg: err.stack }, null)
        } else {
          if (res) {
            var levels = res
            cb({ code: code.OK, msg: "" }, levels)
          } else {
            cb({ code: code.CONFIG.LOAD_CONFIG_LEVEL_EMPTY, msg: "Level Empty" }, null)
          }
        }
      })
    })
  } catch (err) {
    logger.error("[userDao][getLevel] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// userDao.getCurrencies_ByCid = function (data, cb) {

//     var sql = ' SELECT Currencies FROM hall WHERE HallId = ?';
//     var args = [data.cid];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });

// };

userDao.modifyPassword = function (data, cb) {
  try {
    if (
      typeof data.cid === "undefined" ||
      typeof data.password_new === "undefined" ||
      typeof data.password_old === "undefined"
    ) {
      cb(null, { code: code.DB.PARA_FAIL }, null)
      return
    }

    if (typeof data.password_new != "string" || typeof data.password_old != "string") {
      cb(null, { code: code.DB.PARA_FAIL }, null)
      return
    }

    if (data.password_new.length > 64 || data.password_old.length > 64) {
      cb(null, { code: code.DB.PARA_FAIL }, null)
      return
    }

    var sql = "UPDATE customer SET Passwd = ?, FirstLogin=1 WHERE Cid = ? AND Passwd = ? "
    var args = [data.password_new, data.cid, data.password_old]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[userDao][modifyPassword] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//修改用戶密碼
userDao.modifyUserPassword = function (data, cb) {
  try {
    if (typeof data.cid === "undefined" || typeof data.password_new === "undefined") {
      cb(null, { code: code.DB.PARA_FAIL }, null)
      return
    }

    if (typeof data.password_new != "string") {
      cb(null, { code: code.DB.PARA_FAIL }, null)
      return
    }

    if (data.password_new.length > 64) {
      cb(null, { code: code.DB.PARA_FAIL }, null)
      return
    }

    var sql = "UPDATE customer SET Passwd = ?, FirstLogin=1 WHERE Cid = ?  "
    var args = [data.password_new, data.cid]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[userDao][modifyUserPassword] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// userDao.getCustomerName = function (data, cb) {

//     if (typeof data === 'undefined' || typeof data.isAg === 'undefined') {
//         cb(null, { code: code.FAIL, msg: err });
//         return;
//     }

//     var sql_where = [];
//     var sql_where_text = '';

//     if (typeof data.isAg !== 'undefined') {
//         sql_where.push(" IsAg = " + data.isAg + " ");
//     }

//     if (typeof data.name !== 'undefined') {
//         sql_where.push(" UserName like '%" + data.name + "%' ");
//     }

//     if (typeof data.cid !== 'undefined' && data.cid != '') {
//         sql_where.push(" Cid IN ('" + data.cid + "') ");
//     }

//     if (sql_where.length > 0) {
//         sql_where_text = " WHERE " + sql_where.join(" AND ");
//     }
//     var sql = ' SELECT Cid,UserName FROM customer ' + sql_where_text;
//     var args = [];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });

// };

userDao.createUser_newOtpCode = function (data, cb) {
  try {
    console.log("-createUser_newOtpCode data-", JSON.stringify(data))
    if (typeof data == "undefined" || typeof data.Cid == "undefined" || typeof data.OTPCode == "undefined") {
      cb(null, { code: code.FAIL, msg: err })
      return
    }

    // 新增帳號， state 寫入 0(停用)
    var sql =
      "INSERT INTO otp_auth(Cid,HallId,IsAdmin,OTPCode,State) VALUES (?,?,?,?,0) ON DUPLICATE KEY UPDATE OTPCode=? "
    var args = [data.Cid, data.HallId, data.IsAdmin, data.OTPCode, data.OTPCode]
    console.log("-createUser_newOtpCode sql-", sql, JSON.stringify(args))
    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[userDao][createUser_newOtpCode] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.checkIsExist_User = function (data, cb) {
  try {
    if (
      typeof data == "undefined" ||
      typeof data.userName == "undefined" ||
      (!data.isTransferLine && typeof data.mail == "undefined")
    ) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var sql_where = []
    var sql_where_text = ""

    if (typeof data.userName != "undefined" && data.userName != "") {
      sql_where.push(" BINARY UserName = '" + data.userName + "' ")
    }
    if (typeof data.mail != "undefined" && data.mail != "") {
      sql_where.push(" Email = '" + data.mail + "' ")
    }

    if (sql_where.length > 0) {
      sql_where_text = " WHERE " + sql_where.join(" AND ")
    }

    var sql = "SELECT Cid, UserName, Upid, HallId, IsAg, IsSub FROM customer " + sql_where_text
    var args = []

    logger.info("[userDao][checkIsExist_User][sql] %s [args] %s", sql, args)
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data && r_data.length == 1) {
          cb(null, { code: code.OK }, r_data)
        } else {
          cb(null, { code: code.DB.DATA_EMPTY }, null)
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][checkIsExist_User] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// userDao.setNewPassword = function (data, cb) {

//     if (typeof data.password == 'undefined' || typeof data.firstLogin == 'undefined' || typeof data.cid == 'undefined') {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: null });
//         return;
//     }
//     var sql = 'UPDATE customer SET  Passwd = ? , FirstLogin = ?  WHERE Cid =? ;';
//     var args = [data.password, data.firstLogin, data.cid];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code);
//         } else {
//             if (r_data.affectedRows) {
//                 cb(null, { code: code.OK });
//             } else {
//                 cb(null, { code: code.DB.QUERY_FAIL });
//             }
//         }
//     });
// }

// userDao.checkUsrExist_byHallId = function (data, cb) {

//     var sql = "SELECT COUNT(*) as count ,IPWhitelistIn   FROM hall WHERE HallId=? ";
//     var args = [data.HallId];

//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.query(sql, args, function (err, res) {
//             connection.release();
//             if (err) {
//                //console.log("-----------------------------" + JSON.stringify(err));
//                 cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
//             } else {
//                 if (res && res[0].count == 1) {
//                     cb(null, { code: code.OK }, res[0]['IPWhitelistIn']);
//                 } else {
//                     cb(null, { code: code.DB.DATA_EMPTY, msg: "User not exist" });
//                 }
//             }
//         });
//     });
// };

userDao.CreateIPWhiteList = function (data, cb) {
  try {
    //console.log('userDao CreateIPWhiteListIn', JSON.stringify(data));
    if (typeof data === "undefined" || !Array.isArray(data)) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var sql_where = []
    var now = timezone.serverTime()
    var sql_from_name = "ip_white_list"
    for (var i in data) {
      if (typeof data[i]["chkFlag"] !== "undefined" && data[i]["chkFlag"] !== "") {
        sql_from_name = "ip_black_list (UserLevel,Cid,IpType,Name,`Desc`,Ip,State,ModifiedDate, GGID) VALUES"
        sql_where.push(
          sprintf(
            "('%s','%s','%s','%s','%s','%s','%s','%s','%s')",
            data[i]["UserLevel"],
            data[i]["Cid"],
            data[i]["IpType"],
            data[i]["Name"],
            data[i]["Desc"],
            data[i]["Ip"],
            data[i]["State"],
            now,
            data[i]["GGID"]
          )
        )
      } else {
        sql_from_name = "ip_white_list (UserLevel,Cid,IpType,Name,`Desc`,Ip,State,ModifiedDate) VALUES"
        sql_where.push(
          sprintf(
            "('%s','%s','%s','%s','%s','%s','%s','%s')",
            data[i]["UserLevel"],
            data[i]["Cid"],
            data[i]["IpType"],
            data[i]["Name"],
            data[i]["Desc"],
            data[i]["Ip"],
            data[i]["State"],
            now
          )
        )
      }
    }

    var sql = " INSERT " + sql_from_name + sql_where.join(" , ")
    var args = []

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][CreateIPWhiteList] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.ModifyIPWhiteList = function (data, cb) {
  try {
    //console.log('-ModifyIPWhiteList-', JSON.stringify(data));
    var sql_update = []
    if (typeof data[0]["Ip"] != "undefined" && data[0]["Ip"] != "") {
      sql_update.push(sprintf(" Ip= '%s' ", data[0]["Ip"]))
    }

    if (typeof data[0]["IpType"] != "undefined" && data[0]["IpType"] != "") {
      sql_update.push(sprintf(" IpType= '%s' ", data[0]["IpType"]))
    }

    if (typeof data[0]["Name"] != "undefined") {
      sql_update.push(sprintf(" `Name`= '%s' ", data[0]["Name"]))
    }

    if (typeof data[0]["Desc"] != "undefined" && data[0]["Desc"] != "") {
      sql_update.push(sprintf(" `Desc`= '%s' ", data[0]["Desc"]))
    }

    if (typeof data[0]["State"] != "undefined") {
      sql_update.push(sprintf(" `State`= '%s' ", data[0]["State"]))
    }
    if (typeof data[0]["GGID"] != "undefined") {
      sql_update.push(sprintf(" `GGID`= '%s' ", data[0]["GGID"]))
    }

    var now = timezone.serverTime()

    var sql_from_name = "ip_white_list"
    if (typeof data[0]["chkFlag"] !== "undefined" && data[0]["chkFlag"] !== "") sql_from_name = "ip_black_list"

    var sql =
      "UPDATE " +
      sql_from_name +
      " SET " +
      sql_update.join(",") +
      ", ModifiedDate = ? WHERE Cid=? AND UserLevel = ? AND IpId IN('" +
      data[0]["IpId"] +
      "')"
    var args = [now, data[0]["Cid"], data[0]["UserLevel"]]
    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][ModifyIPWhiteList] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// userDao.cleanIPList = function (data, cb) {

//     var sql_from_name = "ip_white_list";
//     if (typeof data[0]['chkFlag'] !== 'undefined' && data[0]['chkFlag'] !== '') sql_from_name = "ip_black_list";

//     var sql = "DELETE FROM " + sql_from_name + " WHERE Cid = ? AND UserLevel = ? AND IpId IN('" + data[0]['IpId'] + "')";
//     var args = [data[0]['Cid'], data[0]['UserLevel']];

//     pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         connection.query(sql, args, function (err, res) {
//             connection.release();
//             if (res) {
//                 cb(null, { code: code.OK }, res);
//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY }, null);
//             }
//         });
//     });
// };

userDao.get_white_list = function (data, cb) {
  try {
    if (typeof data === "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var res_data = []
    var sql_where = []

    if (typeof data.Cid != "undefined" && data.Cid != "") {
      sql_where.push(" Cid='" + data.Cid + "'")
    }
    if (typeof data.UserLevel != "undefined" && data.UserLevel != "") {
      sql_where.push(" UserLevel=" + data.UserLevel)
    }
    if (typeof data.IpType != "undefined" && data.IpType != "") {
      sql_where.push(" IpType='" + data.IpType + "' ")
    }
    /*
        if (typeof data.State != 'undefined' && data.State != '') {
            sql_where.push(" State='" + data.State + "' ");
        }
        */
    if (typeof data.State != "undefined") {
      if (Array.isArray(data.State)) {
        sql_where.push(" State IN('" + data.State.join("','") + "') ")
      } else {
        sql_where.push(" State='" + data.State + "' ")
      }
    }
    if (typeof data.Name != "undefined" && data.Name != "") {
      sql_where.push(" Name  ='" + data.Name + "' ")
    }
    if (typeof data.IpId != "undefined" && data.IpId != "") {
      sql_where.push(" Ip IN('" + data.IpId + "') ")
    }

    if (sql_where.length == 0) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }
    var sql_where_text = sql_where.join(" AND ")

    var sql_from_name = "ip_white_list"
    if (typeof data.chkFlag != "undefined" && data.chkFlag !== "") {
      sql_from_name = "ip_black_list"
    }

    let sortType = "DESC"
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    let sortKey = "ModifiedDate"

    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      var sort_info = {
        whiteListName: "Name",
        whiteListType: "IpType",
        whiteListState: "State",
        whiteListIp: "Ip",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }

    var sql = sprintf("SELECT * FROM %s WHERE %s ORDER BY %s %s", sql_from_name, sql_where_text, sortKey, sortType)

    var args = []
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var info = { ip_white_list: r_data }
        res_data.push(info)
        cb(null, { code: code.OK, msg: "" }, res_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][get_white_list] catch err", err)
    cb(null, code.FAIL, null)
  }
}
userDao.getUserAuthorityTemp = function (data, cb) {
  try {
    var sql_where = []

    if (data.authType == 2 && data.isSub == 1) {
      sql_where.push(" AuthorityType IN ( 5, " + data.authType + " )")
    } else {
      sql_where.push(" AuthorityType = " + data.authType)
    }

    if (data.authType == 2 && data.isSub == 0) {
      sql_where.push(" cid = '0' ")
    } else {
      //agent OR sha；因自動新增帳號的關係，agent 權限自動給予 3(Agent專用)，所以查詢權限時需再多一 cid='0' 判斷
      sql_where.push(" (cid = '" + data.cid + "' OR cid = '0')")
    }

    if (sql_where.length == 0) {
      cb(null, { code: code.DB.PARA_FAIL, msg: "" })
      return
    }

    var sql_where_text = sql_where.join(" AND ")

    var sql =
      "SELECT `AuthorityTemplateID`,`AuthorityTemplateDesc`,`AuthorityTemplateNote`,`AuthorityType`,`AuthorityJson` FROM back_office_authority  WHERE " +
      sql_where_text +
      " ORDER BY ModifyDate DESC"
    var args = []

    logger.info("[userDao][getUserAuthorityTemp][sql] %s [args] %s", sql, args)
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var authoritys = []
        for (var i in r_data) {
          authoritys.push({
            id: r_data[i]["AuthorityTemplateID"],
            desc: r_data[i]["AuthorityTemplateDesc"],
            note: r_data[i]["AuthorityTemplateNote"],
          })
        }
        cb(null, { code: code.OK, msg: "" }, authoritys)
      }
    })
  } catch (err) {
    logger.error("[userDao][getUserAuthorityTemp] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getUser_authFuncs = function (data, cb) {
  try {
    var sql =
      "SELECT customer.Cid,customer.AuthorityTemplateID,back_office_authority.AuthorityJson FROM customer INNER JOIN back_office_authority USING(AuthorityTemplateID) WHERE customer.Cid =? "
    var args = [data.cid]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data.length > 0) {
          cb(null, { code: code.OK, msg: "" }, r_data[0]["AuthorityJson"])
        } else {
          cb(null, { code: code.CONFIG.LOAD_CONFIG_AUTHORITY_EMPTY, msg: "Tmp Empty" }, null)
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][getUser_authFuncs] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// userDao.getSecureKey = function (data, cb) {
//     var sql = 'SELECT SecureKey FROM hall WHERE HallId =(SELECT Cid FROM game.customer WHERE UserName = ? AND IsAg = 2)';
//     var args = [data.wl];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             if (r_data && r_data.length === 1) {
//                 cb(null, { code: code.OK }, r_data[0]);
//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY, msg: "SecureKey Empty" }, null);
//             }
//         }
//     });

// };

// userDao.getUserPR_byUserName_1 = function (data, cb) {

//     var sql = 'SELECT Cid,HallId,UserName,Passwd,NickName,Upid,IsDemo,LastLoginDate,IsSingleWallet,IsOnline,Currency,Quota,State' +
//         ' FROM customer WHERE UserName = ? AND Upid = ? AND IsAg = 4 ';
//     var args = [data.name, data.agent];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             if (r_data && r_data.length === 1) {
//                 var usr = r_data[0];
//                 if (usr.Passwd === data.password) {
//                     cb(null, { code: code.OK, msg: "" }, usr);
//                 } else {
//                     cb(null, { code: code.DB.PASSWORD_INVALID, msg: "Invalid Password" }, null);
//                 }
//             } else {
//                 cb(null, { code: code.DB.USER_INVALID, msg: "Invalid User" }, null);
//             }
//         }
//     });
// };
// userDao.getPlayerQuota = function (usrId, cb) {

//     var sql = "SELECT UserName,Quota FROM customer WHERE Cid =? AND IsAg=4 ";
//     var args = [usrId];
//     console.log('userDao.getPlayerQuota----', sql, JSON.stringify(args));
//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         console.log('userDao.getPlayerQuota', JSON.stringify(r_code), JSON.stringify(r_data));
//         cb(null, r_code, r_data);
//     });

// };

// userDao.getUserCurrency_byHallId = function (data, cb) {

//     var sql = "SELECT A.HallId,A.Cid,A.UserName AS playerName,A.Currency" +
//         " FROM customer A" +
//         " WHERE A.IsAg=4 AND A.HallId=? AND A.UserName=? ";
//     var args = [data.hallId, data.userName];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             if (r_data && r_data.length === 1) {
//                 cb(null, { code: code.OK }, r_data[0]['Currency']);
//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY }, null);
//             }
//         }
//     });

// }

// userDao.getUserId_byName = function (msg, cb) {
//     if (typeof msg === 'undefined' || typeof msg.userName === 'undefined' || typeof msg.IsAg === 'undefined') {
//         cb(null, { code: code.FAIL, msg: null }, null);
//         return;
//     }
//     var sql = "SELECT Cid FROM customer WHERE IsAg= ? AND UserName=? ";
//     var args = [msg.IsAg, msg.userName];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             if (r_data && r_data.length === 1) {
//                 cb(null, { code: code.OK }, r_data[0]['Cid']);
//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY }, null);
//             }
//         }
//     });
// }

userDao.getUserIds_byName = function (msg, cb) {
  try {
    if (typeof msg === "undefined" || typeof msg.userName === "undefined") {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }

    let isAg = typeof msg.IsAg !== "undefined" ? "IsAg = ? AND" : ""
    let isSub = typeof msg.IsSub !== "undefined" ? "IsSub = ? AND" : ""
    let hallList = typeof msg.hallList !== "undefined" ? " (HallId IN (?) OR Upid IN (?) OR Cid IN (?)) AND" : "" // 查找所屬的所有下線和玩家名單
    let haOrAg = msg.haOrAg ? "IsAg != 4 AND" : ""
    var sql = `SELECT Cid, UserName, IsAg, HallId FROM customer WHERE ${isAg} ${haOrAg} ${isSub} ${hallList} UserName like ? `

    var args = []
    if (typeof msg.IsAg !== "undefined") args.push(msg.IsAg)
    if (typeof msg.IsSub !== "undefined") args.push(msg.IsSub)
    if (typeof msg.hallList !== "undefined") {
      args.push(msg.hallList)
      args.push(msg.hallList)
      args.push(msg.hallList)
    }
    args.push("%" + msg.userName + "%")

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][getUserIds_byName] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.get_ha_byId = function (msg, cb) {
  try {
    var res_data = []
    var sql_query = []

    for (var i in msg) {
      var sql =
        " SELECT * FROM " + msg[i]["table"] + " WHERE " + msg[i]["search_key"] + "= '" + msg[i]["search_value"] + "'"
      sql_query.push(sql)
    }
    var args = []

    pomelo.app.get("dbclient_g_r").getConnection(function (err, connection) {
      if (err) {
        cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }
      connection.query(sql_query[0], args, function (err, res) {
        if (err) {
          cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null)
        } else {
          if (res) {
            var info = {}
            info[msg[0]["table"]] = res
            res_data.push(info)

            connection.query(sql_query[1], args, function (err, res) {
              connection.release()
              if (err) {
                cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null)
              } else {
                if (res) {
                  var info = {}
                  info[msg[1]["table"]] = res
                  res_data.push(info)
                  cb(null, { code: code.OK }, res_data)
                } else {
                  cb(null, { code: code.DB.DATA_EMPTY }, null)
                }
              }
            })
          } else {
            cb(null, { code: code.DB.DATA_EMPTY }, null)
          }
        }
      })
    })
  } catch (err) {
    logger.error("[userDao][get_ha_byId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.get_OtpCode_byId = function (msg, cb) {
  try {
    if (typeof msg === "undefined" || typeof msg.OTPCode === "undefined") {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }
    var res_data = []
    var sql = " SELECT * FROM otp_auth WHERE Cid = ? AND OTPCode=? "
    var args = [msg.Cid, msg.OTPCode]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var info = { otp_auth: r_data }
        res_data.push(info)
        cb(null, { code: code.OK }, res_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][get_OtpCode_byId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.get_games_byCid = function (msg, cb) {
  try {
    var res_data = []
    var sql = " SELECT * FROM game_setting WHERE Cid = ?  "
    var args = [msg.Cid]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var data = {}
        if (r_data.length > 0) {
          data = {
            GameId: r_data[0]["GameId"],
            Cid: r_data[0]["Cid"],
            Upid: r_data[0]["Upid"],
            Hallid: r_data[0]["Hallid"],
            RTPid: r_data[0]["RTPid"],
            DenomsSetting: r_data[0]["DenomsSetting"],
            DenomsRunning: r_data[0]["DenomsRunning"],
            Amount: r_data[0]["Amount"],
            Sw: r_data[0]["S"],
            AutoSpinEnable: r_data[0]["AutoSpinEnable"],
            ExchangeEnable: r_data[0]["ExchangeEnable"],
            JackpotEnable: r_data[0]["JackpotEnable"],
            AutoSpinJson: JSON.parse(r_data[0]["AutoSpinJson"]),
            Denom_default: r_data[0]["Denom_default"],
            ExchangeJson: JSON.parse(r_data[0]["ExchangeJson"]),
          }
        }
        var info = { game_setting: data }
        res_data.push(info)

        cb(null, { code: code.OK }, res_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][get_games_byCid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.get_operator_byId = function (msg, cb) {
  try {
    var res_data = []
    var sql =
      " SELECT A.AuthorityTemplateID as AuthorityTemplateID, A.UserName as UserName, A.NickName as NickName, A.State as State, A.Upid as Upid, A.Email as Email, A.HallId as HallId, A.isSub as isSub, B.Currencies as Currency FROM customer A INNER JOIN sub_user B ON (A.Cid = B.Cid) WHERE A.Cid=? "
    var args = [msg.Cid]
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var info = { customer: r_data }
        res_data.push(info)
        cb(null, { code: code.OK }, res_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][get_operator_byId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.get_user_byId = function (msg, cb) {
  try {
    var res_data = []
    var sql = " SELECT * FROM customer WHERE Cid=?"
    var args = [msg.Cid]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var info = { customer: r_data }
        res_data.push(info)
        cb(null, { code: code.OK }, res_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][get_user_byId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 以用戶編號取得用戶資料
// data: {cid}
userDao.get_user_byId2 = function (data, cb) {
  try {
    let sqlWhere = " WHERE Cid = ?"
    let argsWhere = [data.cid]

    var sql = "SELECT Cid, UserName, IsAg FROM game.customer" + sqlWhere
    var args = [...argsWhere]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][get_player_byCid] catch err", err)
    cb(null, { code: code.FAIL }, null)
  }
}

userDao.getPassword_Cid = function (cid, cb) {
  try {
    var res_data = []
    if (typeof cid == "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }
    var sql = "SELECT * FROM customer WHERE Cid = ?"
    var args = [cid]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var info = { customer: r_data }
        res_data.push(info)
        cb(null, { code: code.OK }, res_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][getPassword_Cid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.updateJackpotSwitchState = function (jackpot, cid, cb) {
  //修改分銷商彩金開關功能
  try {
    var sql_where = []
    if (typeof cid !== "undefined" && cid !== "") {
      sql_where.push(" HallId = '" + cid + "' ")
    }
    var sql_update = []
    if (typeof jackpot !== "undefined") {
      sql_update.push(" IsJackpotEnabled = '" + jackpot + "' ")
    }

    let sql = "UPDATE hall SET " + sql_update.join(",") + "  WHERE " + sql_where
    let args = []

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[userDao][updateJackpotSwitchState] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getJackpotSwitchState = function (data, cb) {
  //取得分銷商彩金開關功能狀態
  try {
    if (typeof data.cid == "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }
    let sql = "SELECT IsJackpotEnabled FROM hall WHERE HallId=? "
    let args = [data.cid]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, { code: code.OK }, r_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][getJackpotSwitchState] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getHall_byHallId = function (HallId, cb) {
  try {
    var res_data = []
    var sql = "SELECT * FROM hall WHERE HallId=? "
    var args = [HallId]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var info = { hall: r_data }
        res_data.push(info)
        cb(null, { code: code.OK }, res_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][getHall_byHallId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.setActionTime = function (userId, cb) {
  try {
    if (typeof userId == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null })
      return
    }
    var now = timezone.serverTime()
    var sql = "UPDATE customer SET LastActDate = ? WHERE Cid=? "
    var args = [now, userId]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[userDao][setActionTime] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// userDao.getActionTime_2 = function (userId, cb) {

//     var sql = "SELECT DATE_FORMAT(LastActDate,'%Y-%m-%d %H:%i:%s') AS LastActDate FROM customer WHERE Cid=? ";
//     var args = [userId];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             if (r_data.length > 0) {
//                 cb(null, { code: code.OK }, r_data[0]['LastActDate']);
//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY, msg: "User not exist" });
//             }
//         }
//     });
// }

// userDao.getDownUser = function (user_type, cb) {

//     if (typeof user_type === 'undefined' || typeof user_type.cid === 'undefined' || typeof user_type.level === 'undefined') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: null }, null);
//         return;
//     }
//     var sql_where = [];

//     //admin
//     if (user_type.level == '1') {
//         sql_where.push('IsAg = 2 AND IsSub=0  ');
//     }

//     if (user_type.level == '2') {
//         sql_where.push('IsAg = 3 AND HallId ="' + user_type.cid + '"');
//     }

//     if (user_type.level == '3') {
//         sql_where.push('IsAg = 4 AND Upid ="' + user_type.cid + '"');
//     }

//     var sql = "SELECT Cid,UserName FROM customer WHERE " + sql_where.join(" AND ");
//     var args = [];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

userDao.getCusCurrency_byCid = function (userId, cb) {
  try {
    var sql = "SELECT Currency FROM customer WHERE Cid = ? "
    var args = [userId]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data.length > 0) {
          cb(null, { code: code.OK }, r_data[0]["Currency"])
        } else {
          cb(null, { code: code.DB.DATA_EMPTY, msg: "Currency not exist" }, null)
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][getCusCurrency_byCid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getWallets = function (cid, cb) {
  try {
    var sql = "SELECT Currency, Quota FROM wallet WHERE Cid = ?"
    var args = [cid]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data.length > 0) {
          cb(null, { code: code.OK }, r_data)
        } else {
          cb(null, { code: code.DB.DATA_EMPTY, msg: "User not exist" }, null)
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][getWallets] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getHallCurrency_byCid = function (userId, cb) {
  try {
    var sql = "SELECT Currencies AS Currency FROM hall WHERE HallId = ? "
    var args = [userId]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data.length > 0) {
          cb(null, { code: code.OK }, r_data[0]["Currency"])
        } else {
          cb(null, { code: code.DB.DATA_EMPTY, msg: "User not exist" }, null)
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][getHallCurrency_byCid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//(多錢包) 上層USER
// userDao.check_upUsers = function (msg, cb) {

//     if (typeof msg === 'undefined' || typeof msg.wl === 'undefined' || typeof msg.agent === 'undefined') {
//         cb(null, { code: code.FAIL, msg: null }, null);
//         return;
//     }
//  /*
//     var sql = " SELECT hall.Cid, hall.UserName, agent.Cid AS agentId, agent.UpId, agent.HallId, agent.UserName, agent.Currency AS agentCurrency, agent.IsDemo, h.Currencies " +
//         " FROM customer hall " +
//         " LEFT JOIN customer agent ON(hall.Cid=agent.HallId AND agent.IsAg=3) " +
//         " LEFT JOIN hall h ON (hall.Cid=h.HallId) " +
//         " WHERE hall.IsAg=2  AND hall.UserName =? AND agent.UserName =?";
// */
// var sql = " SELECT hall.Cid, hall.UserName, agent.Cid AS agentId, agent.UpId, agent.HallId, agent.UserName, agent.Currency AS agentCurrency, agent.IsDemo, hall.Currency AS haCurrency " +
//         " FROM customer hall " +
//         " LEFT JOIN customer agent ON(hall.Cid=agent.HallId AND agent.IsAg=3) " +
//         " WHERE hall.IsAg=2  AND hall.UserName =? AND agent.UserName =?";

//     var args = [msg.wl, msg.agent];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             if (r_data && r_data.length === 1) {
//                 cb(null, { code: code.OK }, r_data[0]);
//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY }, null);
//             }
//         }
//     });
// }

//(多錢包)判斷玩家是否存在
// userDao.checkPlayerExist = function (data, cb) {

//     if (typeof data === 'undefined') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: null }, null);
//         return;
//     }

//     var sql = 'SELECT count(*) as count,Cid,Upid,HallId FROM customer WHERE UserName = ? AND IsAg=4 ';
//     var args = [data.name];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             if (r_data && r_data[0]['count'] > 0) {
//                 cb(null, { code: code.DB.DATA_DUPLICATE, msg: "User Duplicate" }, r_data[0]);
//             } else {
//                 cb(null, { code: code.OK }, null);
//             }
//         }
//     });
// }

//(多錢包)建立玩家
// userDao.createPlayer = function (data, cb) {

//     if (typeof data === 'undefined' || typeof data.hallId === 'undefined' || typeof data.agentId === 'undefined' || typeof data.currency === 'undefined' || typeof data.name === 'undefined' || typeof data.IsDemo === 'undefined') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: null }, null);
//         return;
//     }
//     var now = timezone.serverTime();
//     var sql = " INSERT customer (Cid,UserName,Passwd,NickName,AddDate,IsAg,IsSub,Upid,HallId,IsDemo,Currency,State) VALUES (?,?,?,?,?,4,0,?,?,?,?,'N'); ";
//     var args = [data.cid, data.name, m_md5(data.name), data.name, now ,data.agentId, data.hallId, data.IsDemo, data.currency];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             cb(null, r_code, r_data.insertId);
//         }
//     });
// }

//(多錢包)玩家資料
userDao.getPlayerInfo = function (data, cb) {
  try {
    if (typeof data === "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }

    var sql_where = []

    const extraArgs = []

    if (typeof data.playerName !== "undefined" && data.playerName !== "") {
      sql_where.push(` UserName = ? `)
      extraArgs.push(data.playerName)
    } else if (typeof data.playerId != "undefined") {
      sql_where.push(` Cid = ? `)
      extraArgs.push(data.playerId)
    }

    if (sql_where.length == 0) {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }

    var sql_where_text = sql_where.join(" AND ")

    const sql = `SELECT * FROM customer WHERE ${sql_where_text} AND IsAg=4 AND HallId = ? AND Upid =?`
    const args = [...extraArgs, data.hallId, data.agentId]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data && r_data.length == 1) {
          cb(null, { code: code.OK }, r_data)
        } else {
          cb(null, { code: code.DB.DATA_EMPTY, msg: "DATA_EMPTY" }, null)
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][getPlayerInfo] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.checkTransferStatus = function (data, cb) {
  try {
    if (typeof data === "undefined" || typeof data.uid === "undefined" || typeof data.hallId === "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }

    var sql_where = []
    if (typeof data.uid !== "undefined" && data.uid !== "") {
      sql_where.push(" tx.Uid = '" + data.uid + "' ")
    }
    if (typeof data.hallId !== "undefined" && data.hallId !== "") {
      //hallId
      sql_where.push(" tx.Cid = '" + data.hallId + "' ")
    }
    if (typeof data.playerId !== "undefined" && data.playerId !== "") {
      sql_where.push(" tx.PlayerId = '" + data.playerId + "' ")
    }

    //sql_where.push(" State = '1' ");

    var sql_where_text = ""
    if (sql_where.length == 0) {
      cb(null, { code: code.FAIL, msg: err }, null)
      return
    }

    var sql_where_text = sql_where.join(" AND ")
    var sql =
      " SELECT cus.UserName AS playerName ,tx.TxId ,tx.Uid,tx.Cid,tx.TxType,tx.PlayerId,tx.Currency,tx.ExCurrency,tx.Amount,tx.Quota_Before,tx.Quota_After,tx.CryDef,tx.TxDate,tx.State,cus.Upid,ag.UserName AS agentName " +
      " FROM transfer_record tx  " +
      " LEFT JOIN customer cus ON(cus.Cid= tx.PlayerId AND cus.IsAg=4) " +
      " LEFT JOIN customer ag ON(ag.Cid= cus.Upid AND ag.IsAg=3) " +
      " WHERE " +
      sql_where_text
    var args = []

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data && r_data.length > 0) {
          cb(null, { code: code.OK }, r_data[0])
        } else {
          cb(null, { code: code.DB.DATA_EMPTY, msg: "DATA_EMPTY" }, null)
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][checkTransferStatus] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.transferBalance = function (data, cb) {
  try {
    if (typeof data === "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }
    var now = timezone.serverTime()
    //新增 紀錄
    var sql =
      "INSERT INTO transfer_record (Uid,Cid,IsAg,TxType,PlayerId,Currency,ExCurrency,Amount,Quota_Before,Quota_After,CryDef,TxDate,State, Trans_Cid, Trans_Amount_Before, Trans_Amount_After, Trans_isAg, OP_Cid, OP_isAg) " +
      " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,?,?,?,?) "

    var args = [
      data.uid,
      data.hallId,
      data.isAg,
      data.txType,
      data.playerId,
      data.main_currency,
      data.currency,
      data.amount,
      data.balance_before,
      data.balance_after,
      data.CryDef,
      now,
      data.trans_cid,
      data.trans_amount_before,
      data.trans_amount_after,
      data.trans_isAg,
      data.op_cid,
      data.op_isAg,
    ]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data.insertId)
      }
    })
  } catch (err) {
    logger.error("[userDao][transferBalance] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//白名單IP判斷
userDao.checkWhiteIp = function (data, cb) {
  try {
    if (typeof data === "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }
    var sql_where = []

    if (typeof data.UserLevel !== "undefined") {
      sql_where.push(" UserLevel = '" + data.UserLevel + "' ")
    }
    if (typeof data.Cid !== "undefined") {
      //hallId
      sql_where.push(" Cid = '" + data.Cid + "' ")
    }
    if (typeof data.IpType !== "undefined") {
      sql_where.push(" IpType = '" + data.IpType + "' ")
    }
    if (typeof data.Ip !== "undefined") {
      sql_where.push(" Ip = '" + data.Ip + "' ")
    }
    if (typeof data.State !== "undefined") {
      sql_where.push(" State = '" + data.State + "' ")
    }

    var sql = "SELECT count(*) AS count FROM ip_white_list WHERE  " + sql_where.join(" AND ")
    var args = []
    console.log("---------------sql---------------------", sql)
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data && r_data[0]["count"] > 0) {
          cb(null, { code: code.OK }, r_data[0]["count"])
        } else {
          cb(null, { code: code.DB.DATA_EMPTY, msg: "DATA_EMPTY" }, null)
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][checkWhiteIp] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.modifyTransferBalanceStatus = function (data, cb) {
  try {
    if (typeof data === "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null })
      return
    }
    var sql_update = []
    if (typeof data.State !== "undefined") {
      sql_update.push(" State = '" + data.State + "' ")
    }

    var sql_where = []
    if (typeof data.TxId !== "undefined" && data.TxId !== "") {
      sql_where.push(" TxId = '" + data.TxId + "' ")
    }

    if (sql_update.length == 0 || sql_where.length == 0) {
      cb(null, { code: code.DB.PARA_FAIL, msg: null })
      return
    }

    var sql = "UPDATE transfer_record SET " + sql_update.join(",") + "  WHERE " + sql_where.join(" AND ")
    var args = []

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[userDao][modifyTransferBalanceStatus] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 修改玩家額度
// data: {cid, hallId, amount}
// 回傳結果額度
userDao.modifyPlayerQuota = async function (data, cb) {
  try {
    let sqlSet = " SET Quota = @quotaResult := (Quota + ?)"
    let argsSet = [data.amount]

    let sqlWhere = " WHERE Cid = ? AND HallId = ? AND (Quota + ?) >= 0"
    let argsWhere = [data.cid, data.hallId, data.amount]

    let sql = "UPDATE game.customer" + sqlSet + sqlWhere
    let args = [...argsSet, ...argsWhere]

    let sql2 = "SELECT @quotaResult as QuotaResult"
    let args2 = []

    db.act_query_multi("dbclient_g_rw", [sql, sql2], [args, args2], function (r_code, r_data) {
      if (r_code.code != code.OK) {
        logger.error("[userDao][modifyPlayerQuota] act_transaction failed! err: ", r_code)
        cb(null, r_code, null)
      } else {
        if (r_data[0].affectedRows == 0) {
          // 額度不足
          cb(null, { code: code.FAIL }, null)
        } else {
          cb(null, r_code, r_data[1][0]["QuotaResult"])
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][modifyPlayerQuota] catch err", err)
    cb(null, { code: code.FAIL }, null)
  }
}

// userDao.getTransferBalanceByTxId = function (data, cb) {

//     var res_data = [];

//     if (typeof data == 'undefined' || typeof data.TxId == 'undefined' || data.TxId == '') {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null);
//         return;
//     }

//     var sql = 'SELECT * FROM transfer_record WHERE TxId=? ';
//     var args = [data.TxId];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             var info = { transfer_record: r_data };
//             res_data.push(info);
//             cb(null, { code: code.OK }, res_data);
//         }
//     });
// }

//多錢包 - 資金轉移紀錄 筆數
// userDao.counts_transfer_list = function (data, cb) {

//     if (typeof data == 'undefined' || typeof data.start_date == 'undefined' || typeof data.end_date == 'undefined') {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null);
//         return;
//     }

//     var sql_where = [];

//     //玩家帳號
//     if (data.player !== '') {
//         sql_where.push(' c.UserName = "' + data.player + '" ');
//     }

//     if (data.hallId !== '') {
//         sql_where.push(' t.Cid = "' + data.hallId + '" ');
//     }

//     if (data.start_date !== '' && data.end_date !== '') {
//         sql_where.push(' t.TxDate BETWEEN "' + data.start_date + '" AND "' + data.end_date + '" ');
//     }

//     if (data.agent !== '') { // ...
//         sql_where.push(" ag.UserName = '" + data.agent + "'");
//     }

//     if (data.type !== '') {
//         if (data.type == 'withdraw') {
//             sql_where.push(" t.TxType like 'withdraw%' ");
//         }
//         if (data.type == 'deposit') {
//             sql_where.push(" t.TxType='deposit' ");
//         }
//     }

//     sql_where.push(" t.State=1 ");

//     if (sql_where.length == 0) {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null);
//         return;
//     }

//     var sql_where_text = sql_where.join(" AND ");

//     var sql = " SELECT count(t.TxId) AS count " +
//         " FROM transfer_record t" +
//         " LEFT JOIN customer c ON(c.Cid = t.PlayerId AND c.IsAg =4 ) " +
//         " LEFT JOIN customer ag ON (c.Upid = ag.Cid AND ag.IsAg =3 ) " +
//         " WHERE " + sql_where_text;

//     var args = [];
//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             if (r_data && r_data.length > 0) {
//                 cb(null, { code: code.OK }, r_data[0]['count']);
//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY, msg: "Transaction Count Empty" }, null);
//             }
//         }
//     });
// }

//多錢包 - 資金轉移紀錄
// userDao.transfer_list = function (data, cb) {

//     if (typeof data == 'undefined' || typeof data.start_date == 'undefined' || typeof data.end_date == 'undefined') {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null);
//         return;
//     }

//     var sql_where = [];

//     //玩家帳號
//     if (data.player !== '') {
//         sql_where.push(' c.UserName = "' + data.player + '" ');
//     }

//     if (data.hallId != '') {
//         sql_where.push(' t.Cid = "' + data.hallId + '" ');
//     }

//     if (data.start_date != '' && data.end_date != '') {
//         sql_where.push(' ( t.TxDate BETWEEN "' + data.start_date + '" AND "' + data.end_date + '") ');
//     }

//     if (data.agent != '') { //....
//         sql_where.push(" ag.UserName = '" + data.agent + "'");
//     }

//     if (data.type !== '') {
//         if (data.type == 'withdraw') {
//             sql_where.push(" t.TxType like 'withdraw%' ");
//         }
//         if (data.type == 'deposit') {
//             sql_where.push(" t.TxType='deposit' ");
//         }
//     }

//     sql_where.push(" t.State=1 ");

//     if (sql_where.length == 0) {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null);
//         return;
//     }

//     var sql_where_text = sql_where.join(" AND ");

//     var sql = " SELECT SQL_CALC_FOUND_ROWS t.TxId,t.Uid,t.Cid,CASE WHEN t.TxType='withdraw_all' THEN 'withdraw' ELSE t.TxType END AS TxType,t.PlayerId,t.Amount,t.CryDef,t.Quota_Before,t.Quota_After,DATE_FORMAT(t.TxDate,'%Y-%m-%d %H:%i:%s') AS TxDate,t.State, " +
//         " c.UserName AS playerName,ag.UserName AS agentName " +
//         " FROM transfer_record t" +
//         " LEFT JOIN customer c ON(c.Cid = t.PlayerId AND c.IsAg =4 ) " +
//         " LEFT JOIN customer ag ON (c.Upid = ag.Cid AND ag.IsAg =3 ) " +
//         " WHERE " + sql_where_text +
//         " ORDER BY t.TxDate DESC " +
//         " LIMIT ?,? ";

//     var args = [(data.page - 1) * data.page_size, data.page_size];

//     db.act_info_rows('dbclient_g_rw', sql, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

userDao.get_OtpCode_log = function (data, cb) {
  try {
    if (typeof data == "undefined" || typeof data.Cid == "undefined" || typeof data.IsAdmin == "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }
    var res_data = []

    var sql = " SELECT * FROM otp_auth  WHERE Cid = ? AND IsAdmin=? "
    var args = [data.Cid, data.IsAdmin]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var info = { otp_auth: r_data }
        res_data.push(info)
        cb(null, { code: code.OK }, res_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][get_OtpCode_log] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.renew_OtpCode = function (data, cb) {
  try {
    if (
      typeof data == "undefined" ||
      typeof data.Cid == "undefined" ||
      typeof data.IsAdmin == "undefined" ||
      typeof data.HallId == "undefined" ||
      typeof data.OTPCode == "undefined"
    ) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var sql_query = []
    //刪除
    var del_sql = sprintf(
      "DELETE FROM otp_auth WHERE Cid = '%s' AND HallId = '%s' AND IsAdmin=%s ",
      data.Cid,
      data.HallId,
      data.IsAdmin
    )
    sql_query.push(del_sql)
    //新增
    var add_sql = sprintf(
      "INSERT otp_auth (Cid,HallId,IsAdmin,OTPCode,`State`) VALUES ('%s','%s','%s','%s',1)  ",
      data.Cid,
      data.HallId,
      data.IsAdmin,
      data.OTPCode
    )
    sql_query.push(add_sql)

    pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
      if (err) {
        connection.release()
        cb({ code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }

      //-----------------transaction start---------------

      var funcAry = []
      sql_query.forEach(function (sql, index) {
        var temp = function (cb) {
          connection.query(sql, [], function (temp_err, results) {
            if (temp_err) {
              connection.rollback(function () {
                return cb(code.DB.QUERY_FAIL)
              })
            } else {
              return cb(null, code.ok)
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
                return cb({ code: code.DB.QUERY_FAIL, msg: err })
              })
            } else {
              connection.release()
              return cb(null, { code: code.OK, msg: "" })
            }
          })
        }
      })
      //-----------------transaction end---------------
    })
  } catch (err) {
    logger.error("[userDao][renew_OtpCode] catch err", err)
    cb(null, code.FAIL, null)
  }
}
userDao.set_user_setting = function (data, cb) {
  try {
    console.log("-set_user_setting data -", JSON.stringify(data))
    /*
        if (typeof data === 'undefined' || typeof data.Cid === 'undefined' || typeof data.IsAdmin === 'undefined' || typeof data.CountsOfPerPage === 'undefined' || typeof data.HourDiff === 'undefined') {
            cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null);
            return;
        }
        */
    if (typeof data === "undefined" || typeof data.Cid === "undefined" || typeof data.IsAdmin === "undefined") {
      cb(
        null,
        {
          code: code.DB.DATA_EMPTY,
          msg: null,
        },
        null
      )
      return
    }
    if (typeof data.CountsOfPerPage == "undefined") {
      data["CountsOfPerPage"] = 0
    }
    if (typeof data.HourDiff == "undefined") {
      data["HourDiff"] = pomelo.app.get("sys_config").time_diff_hour
    }

    var sql = "INSERT INTO user_setting (`Cid`,`IsAdmin`,`CountsOfPerPage`,`HourDiff`) VALUES (?,?,?,?)"
    var args = [data.Cid, data.IsAdmin, data.CountsOfPerPage, data.HourDiff]
    console.log("-set_user_setting sql-", sql)
    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(
          null,
          {
            code: code.OK,
          },
          r_data.insertId
        )
      }
    })
  } catch (err) {
    logger.error("[userDao][set_user_setting] catch err", err)
    cb(null, code.FAIL, null)
  }
}
userDao.modify_user_setting = function (data, cb) {
  try {
    console.log("------modify_user_setting-------------", JSON.stringify(data))
    if (typeof data === "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }
    var sql_update = []
    if (typeof data.CountsOfPerPage !== "undefined" && data.CountsOfPerPage !== "") {
      sql_update.push(" CountsOfPerPage = " + data.CountsOfPerPage)
    }

    if (typeof data.HourDiff !== "undefined" && data.HourDiff !== "") {
      sql_update.push(" HourDiff = " + data.HourDiff)
    }
    var sql_where = []
    if (typeof data.Uid !== "undefined" && data.Uid > 0) {
      sql_where.push(" Uid = '" + data.Uid + "'")
    }
    if (sql_update.length === 0 || sql_where.length === 0) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var sql_update_text = sql_update.join(",")
    var sql_where_text = sql_where.join(",")

    var sql = "UPDATE user_setting SET " + sql_update_text + " WHERE " + sql_where_text
    console.log("-modify_user_setting sql-", sql)
    var args = []
    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, { code: code.OK }, data.Uid)
      }
    })
  } catch (err) {
    logger.error("[userDao][modify_user_setting] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.get_user_setting = function (data, cb) {
  try {
    if (typeof data === "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }
    var sql_where = []
    if (typeof data.Uid !== "undefined" && data.Uid !== "") {
      sql_where.push(" Uid = '" + data.Uid + "'")
    }

    if (
      typeof data.Cid !== "undefined" &&
      data.Cid !== "" &&
      typeof data.IsAdmin !== "undefined" &&
      data.IsAdmin !== ""
    ) {
      sql_where.push(" ( Cid = '" + data.Cid + "' AND  IsAdmin ='" + data.IsAdmin + "' ) ")
    }

    if (sql_where.length === 0) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }
    var sql_where_text = sql_where.join(" AND ")

    var res_data = []
    var sql = "SELECT * FROM user_setting WHERE " + sql_where_text
    var args = []

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var info = { user_setting: r_data }
        res_data.push(info)
        cb(null, { code: code.OK, msg: "" }, res_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][get_user_setting] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.get_user_setting_byCid = function (data, cb) {
  try {
    if (typeof data === "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }
    var sql_where = []
    if (typeof data.Uid !== "undefined" && data.Uid !== "") {
      sql_where.push(" Uid = " + data.Uid)
    }

    if (
      typeof data.Cid !== "undefined" &&
      data.Cid !== "" &&
      typeof data.IsAdmin !== "undefined" &&
      data.IsAdmin !== ""
    ) {
      sql_where.push(" ( Cid = '" + data.Cid + "'  AND  IsAdmin = '" + data.IsAdmin + "' ) ")
    }

    if (sql_where.length === 0) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }
    var sql_where_text = sql_where.join(" AND ")

    var res_data = []
    var sql =
      "SELECT s.Cid AS cid,s.IsAdmin AS isAdmin,s.CountsOfPerPage AS countsOfPerPage ,s.HourDiff AS hourDiff,t.DescE AS hourDiff_descE,t.DescG AS hourDiff_descG,t.DescC AS hourDiff_descC " +
      " FROM user_setting s" +
      " LEFT JOIN timezone_setting t USING(HourDiff) " +
      " WHERE " +
      sql_where_text
    var args = []

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][get_user_setting_byCid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//單錢包 (transaction_record)
// userDao.get_transaction_info = function (data, cb) {

//     if (typeof data === 'undefined') {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null);
//         return;
//     }

//     var sql_where = [];
//     if (typeof data.uid !== 'undefined' && data.uid !== '') {
//         sql_where.push(sprintf(" Uid = '%s' ", data.uid));
//     }

//     if (typeof data.game_bet_id !== 'undefined' && data.game_bet_id !== '') {
//         sql_where.push(sprintf(" Wid = '%s' ", data.game_bet_id));
//     }

//     if (typeof data.txId !== 'undefined' && data.txId !== '') {
//         sql_where.push(sprintf(" TxId = '%s' ", data.txId));
//     }

//     if (sql_where.length === 0) {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null);
//         return;
//     }

//     var sql_where_text = sql_where.join(" AND ");
//     var sql = "SELECT * FROM transaction_record WHERE " + sql_where_text;
//     var args = [];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// userDao.mode_transaction_status = function (data, cb) {

//     if (typeof data === 'undefined') {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null);
//         return;
//     }
//     var sql_where = [];
//     if (typeof data.uid !== 'undefined' && data.uid !== '') {
//         sql_where.push(sprintf(" Uid = '%s' ", data.uid));
//     }
//     if (typeof data.game_bet_id !== 'undefined' && data.game_bet_id !== '') {
//         sql_where.push(" Wid = '" + data.game_bet_id + "'  ");
//     }
//     if (typeof data.txId !== 'undefined' && data.txId !== '') {
//         sql_where.push(sprintf(" TxId = '%s' ", data.txId));
//     }
//     if (sql_where.length === 0) {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null);
//         return;
//     }
//     var sql_where_text = sql_where.join(" AND ");

//     var sql = "UPDATE transaction_record SET State=1  WHERE  " + sql_where_text;
//     var args = [];

//     console.log('-mode_transaction_status sql-', sql);

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// userDao.getUserState = function (data, cb) {

//     if (typeof data === 'undefined') {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null);
//         return;
//     }
//     var sql_where = [];
//     if (typeof data.cid !== 'undefined' && data.cid !== '') {
//         sql_where.push(" cus.Cid = '" + data.cid + "'");
//     }
//     // if (typeof data.hallId !== 'undefined' && data.hallId !== '') {
//     //     sql_where.push(" ha.Cid = " + data.hallId);
//     // }
//     if (sql_where.length === 0) {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null);
//         return;
//     }

//     var sql_where_text = sql_where.join(" AND ");

//     var sql = "SELECT cus.State AS cusState ,ha.State AS haState " +
//         " FROM customer cus " +
//         " LEFT JOIN  customer ha ON(cus.HallId =  ha.Cid ) " +
//         " WHERE  " + sql_where_text;
//     var args = [];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data[0]);
//     });
// }

//設定臨時密碼
userDao.setTempPassword = function (data, cb) {
  try {
    console.log("-setTempPassword data-", JSON.stringify(data))

    if (
      typeof data.Cid == "undefined" ||
      typeof data.TempEndTime == "undefined" ||
      typeof data.TempPassword == "undefined" ||
      typeof data.TempMod == "undefined"
    ) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null })
      return
    }

    var sql = "UPDATE customer SET TempPassword=? , TempEndTime = ?, TempMod = ?  WHERE Cid =? ;"
    var args = [data.TempPassword, data.TempEndTime, data.TempMod, data.Cid]

    console.log("-setTempPassword-", sql, JSON.stringify(args))

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code)
      } else {
        if (r_data.affectedRows) {
          cb(null, { code: code.OK })
        } else {
          cb(null, { code: code.DB.QUERY_FAIL })
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][setTempPassword] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// userDao.getUserOnlineState = function (data, cb) {

//     var sql = "SELECT a.IsOnline,c.connect_key,DATE_FORMAT(c.actTime,'%Y-%m-%d %H:%i:%s') AS actTime \
//              FROM customer a \
//              LEFT JOIN user_connection c ON(c.userId = a.Cid AND c.isAdmin=0) \
//              WHERE a.Cid=? ";
//     var args = [data.userId];
//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// userDao.getUserName_byId = function (data, cb) {

//     var sql = "SELECT user.Cid,user.UserName, IFNULL(account.serialNumber,'') AS serialNumber  \
//                FROM customer user \
//                LEFT JOIN account_record account ON(user.Cid=account.cid AND account.isAg = 3 )  \
//                WHERE user.Cid=? AND user.IsAg =? ";
//     var args = [data.userId, data.isAg];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// userDao.getUserName_byCid = function (data, cb) {

//     var sql_where = [];
//     if (typeof data.isAg != 'undefined') {
//         sql_where.push(" IsAg = '" + data.isAg + "' ");
//     }
//     if (typeof data.cid != 'undefined') {
//         if (Array.isArray(data.cid)) {
//             sql_where.push(" Cid IN ('" + data.cid.join("','") + "') ");
//         }
//         if (typeof data.cid == 'string') {
//             sql_where.push(" Cid IN ('" + data.cid + "') ");
//         }
//     }
//     var sql_where_text = sql_where.join(" AND ");

//     var sql = "SELECT Cid,UserName FROM customer WHERE " + sql_where_text;
//     var args = [];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

userDao.changeStatusDownUsers = function (data, cb) {
  //修改非銷商代理下層所有狀態
  if (typeof data === "undefined") {
    cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
    return
  }
  var sql_where = []
  if (typeof data.hallId !== "undefined" && data.hallId !== "") {
    if (data.hallId.length > 1) {
      sql_where.push(`HallId IN ('${data.hallId.join("','")}')`)
      sql_where.push(`Upid IN ('${data.hallId.join("','")}')`)
      sql_where.push(`Cid IN ('${data.hallId.join("','")}')`)
    } else {
      sql_where.push(`HallId = ('${data.hallId}')`)
      sql_where.push(`Upid = ('${data.hallId}')`)
      sql_where.push(`Cid = ('${data.hallId}')`)
    }
  }

  if (typeof data.upId !== "undefined" && data.upId !== "") {
    //修復代理錯誤
    sql_where.push(`Upid = ('${data.upId}')`)
    sql_where.push(`Cid = ('${data.upId}')`)
  }

  var sql_update = []
  if (typeof data.state !== "undefined" && data.state !== "") {
    sql_update.push(" State ='" + data.state + "' ")
  }
  if (sql_where.length == 0 || sql_update.length == 0) {
    cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
    return
  }
  var sql = "UPDATE customer SET " + sql_update.join(",") + " WHERE " + sql_where.join(" OR ")
  var args = []
  logger.info("[changeStatusDownUsers][SQL] ", sql)
  db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
    cb(null, r_code)
  })
}

userDao.getUser_byUserName_temp = function (data, cb) {
  try {
    var sql =
      "SELECT Cid,UserName,IsAg AS Level ,IsSub,Upid,HallId,Upid,State,TempPassword,DATE_FORMAT(TempEndTime,'%Y-%m-%d %H:%i:%s') AS TempEndTime,TempMod FROM customer WHERE UserName= ? "
    var args = [data.name]
    console.log("getUser_byUserName_temp sql:", sql, JSON.stringify(args))
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code)
      } else {
        if (r_data.length > 0) {
          cb(null, { code: code.OK }, r_data[0])
        } else {
          cb(null, { code: code.DB.DATA_EMPTY }, null)
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][getUser_byUserName_temp] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.checkTempUsrExist_byCid = function (data, cb) {
  try {
    var sql =
      "SELECT  DATE_FORMAT(TempEndTime,'%Y-%m-%d %H:%i:%s') AS TempEndTime,TempMod  FROM customer WHERE  cid= ? "
    var args = [data.cid]
    console.log("checkTempUsrExist_byCid sql:", sql, args)
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code)
      } else {
        if (r_data.length > 0) {
          cb(null, { code: code.OK }, r_data[0])
        } else {
          cb(null, { code: code.DB.DATA_EMPTY }, null)
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][checkTempUsrExist_byCid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.modifyPassword_temp = function (data, cb) {
  try {
    var sql = "UPDATE customer SET Passwd =? ,TempPassword= ?, TempEndTime= ?, TempMod= ?  WHERE  cid= ? "
    var args = [data.password, "", "0000-00-00 00:00:00", 0, data.cid]
    console.log("modifyPassword_temp sql", sql, JSON.stringify(args))

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][modifyPassword_temp] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/**
 * 踢人
 * 
 * @param {*} data 
 * @param {*} cb 
 * @returns 
 */
userDao.kickUser = function (data, cb) {
  try {
    if (
      typeof data.userId === "undefined" ||
      typeof data.level === "undefined" ||
      data.userId == "" ||
      data.level == ""
    ) {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }

    var sql = "UPDATE customer SET IsOnline=0 WHERE Cid=? AND IsAg=? "
    var args = [data.userId, data.level]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][kickUser] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getUser_byId = function (data, cb) {
  try {
    if (typeof data.userId === "undefined" || typeof data.level === "undefined" || data.level == "") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }
    var sql_where = []
    var sql_user = ""
    if (Array.isArray(data.userId)) {
      // sql_user = (data.userId.length > 0) ? " Cid IN ('" + data.userId.join("','") + "')" : " Cid = '' ";
      sql_user = " Cid IN ('" + data.userId.join("','") + "')"
    } else {
      sql_user = " Cid = '" + data.userId + "' "
    }
    sql_where.push(sql_user)
    var sql = "SELECT * FROM customer WHERE " + sql_where.join(" AND ") + " AND IsAg=? "
    var args = [data.level]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][getUser_byId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// userDao.modifyUserConnection = function (data, cb) {

//     if (typeof data === 'undefined' || typeof data.isAdmin === 'undefined' || typeof data.userId === 'undefined' || typeof data.userName === 'undefined' || typeof data.token === 'undefined') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: null }, null);
//         return;
//     }
//     var now = timezone.serverTime();
//     var sql = "INSERT INTO user_connection (isAdmin,userId,userName,connect_key,actTime,closeTime) VALUES (?,?,?,?,?,'0000-00-00 00:00:00') ON DUPLICATE KEY UPDATE connect_key=?,actTime=?,closeTime=? ;";
//     var args = [data.isAdmin, data.userId, data.userName, data.token, now, data.token, now, data.closeTime];
//     console.log('-modifyUserConnection-', sql, JSON.stringify(args));
//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// userDao.getUserConnection_byKey = function (data, cb) {

//     if (typeof data === 'undefined' || typeof data.key === 'undefined' || data.key === '') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: null }, null);
//         return;
//     }

//     var sql = "SELECT isAdmin,userId,userName,actTime FROM user_connection WHERE connect_key = ? ";
//     var args = [data.key];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// userDao.modifyUserOnlineState = function (data, cb) {
//     if (typeof data == 'undefined' || typeof data.userId == 'undefined' || typeof data.isOnline == 'undefined') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: null }, null);
//         return;
//     }
//     var sql = "UPDATE customer SET IsOnline = ? WHERE Cid=?";
//     var args = [data.isOnline, data.userId];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// userDao.getUpUserName = function (data, cb) {

//     if (typeof data == 'undefined' || typeof data.userId == 'undefined') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: null }, null);
//         return;
//     }
//     var sql = "SELECT player.Cid AS playerId,player.UserName AS playerName, ha.Cid AS haId,ha.UserName AS haName,ag.Cid AS agId,ag.UserName AS agName \
//     FROM customer player \
//     LEFT JOIN customer ha ON(ha.Cid=player.HallId AND ha.IsAg=2) \
//     LEFT JOIN customer ag ON(ag.Cid=player.UpId AND ag.IsAg=3) \
//     WHERE player.Cid=? AND player.IsAg=4 ";
//     var args = [data.userId];
//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// userDao.modifyUserCloseTime = function (data, cb) {
//     var now = timezone.serverTime();
//     var sql = "UPDATE user_connection SET closeTime=? WHERE userId=? AND isAdmin=?";
//     var args = [now, data.userId, data.isAdmin];
//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// userDao.modifyUserConnectState = function (data, cb) {

//     var sql_query = [];
//     var sql1 = sprintf(" UPDATE customer SET IsOnline = 1  WHERE Cid = '%s' ", data.userId);
//     var sql2 = sprintf(" UPDATE user_connection SET connect_key = '%s'  WHERE userId = '%s' AND isAdmin = '%s' ", data.token, data.userId, data.isAdmin);
//     sql_query.push(sql1);
//     sql_query.push(sql2);

//     db.act_transaction('dbclient_g_rw', sql_query, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             cb(null, r_code, r_data);
//         }
//     });
// }

userDao.getIsDemoUsers = function (data, cb) {
  try {
    var sql = "SELECT count(*) AS count FROM customer WHERE HallId=? AND IsAg=3 AND IsDemo=1 "
    var args = [data.cid]
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][getIsDemoUsers] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// userDao.getAuthTempDetail = function (data, cb) {

//     if (typeof data == 'undefined' || typeof data.funId == 'undefined') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: null }, null);
//         return;
//     }
//     var sql_where = [];
//     sql_where.push("FunctionState =1 ");
//     sql_where.push("FunctionID IN('" + data.funId.join("','") + "')");

//     var sql = "SELECT FunctionID,LOWER(FunctionGroupL) AS FunctionGroupL,LOWER(FunctionAction) AS FunctionAction FROM back_office_function_list  WHERE  " + sql_where.join(" AND ");
//     var args = [data.id];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// userDao.modifyUserAccount = function (data, cb) {
//     var sql = "INSERT account_record(cid, isAg, serialNumber) VALUES (?,3,?) ON DUPLICATE KEY UPDATE serialNumber=? ";

//     var args = [data.userId, data.serialNumber, data.serialNumber];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// userDao.get_user_connection = function (token, cb) {
//     var sql = "SELECT  isAdmin,userId,userName,connect_key,actTime,Date_FORMAT(closeTime,'%Y/%m/%d %H:%i:%s') AS closeTime FROM user_connection WHERE connect_key=? ";
//     var args = [token];
//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

userDao.getUserIsDemo = function (data, cb) {
  try {
    var sql_userId = " Cid IN ('" + data.user.join("','") + "') "
    var sql_adminId = " AdminId IN ('" + data.admin.join("','") + "') "

    var sql = sprintf(
      "	SELECT Cid, IsDemo,IsAg,State FROM customer WHERE %s \
                    UNION DISTINCT \
                    SELECT AdminId AS Cid, 0 AS IsDemo, 1 AS IsAg,State FROM admin WHERE  %s  ",
      sql_userId,
      sql_adminId
    )
    var args = []

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][getUserIsDemo] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//後台
userDao.getUserMarquee = function (data, cb) {
  try {
    var now = timezone.serverTime()
    var sql =
      "SELECT Id, GameId AS locationId, Content AS content ,StartTime AS startTime,DATE_FORMAT(StartTime,'%Y-%m-%d %H:%i:%s') AS startTime,DATE_FORMAT(StopTime,'%Y-%m-%d %H:%i:%s') AS stopTime,  \
        LangC AS langC,LangE AS langE,LangG AS langG, DescriptionC AS descriptionC, DescriptionE AS descriptionE, DescriptionG AS descriptionG \
        FROM marquee  WHERE Priority NOT IN(0) AND GameId=2 AND (Cid='-1' OR Cid = ?) AND ( startTime<=? AND StopTime>=?) ORDER BY Priority DESC "
    var args = [data.userId, now, now]
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][getUserMarquee] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getGame_currency = function (param, cb) {
  try {
    if (typeof param == "undefined" || typeof param.cid == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }
    var sql = "SELECT Currencies FROM hall WHERE HallId=?"
    var args = [param.cid]
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][getGame_currency] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getCid_byUserName = function (param, cb) {
  try {
    if (typeof param == "undefined" || typeof param.userName == "undefined" || typeof param.level == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }
    var sql_where = []
    sql_where.push(" UserName like '%" + param.userName + "%' ")
    sql_where.push(" isAg = '" + param.level + "' ")
    if (param.level > 2) {
      //hall以上
      sql_where.push(" Upid ='" + param.upid + "'  ")
    }
    var sql = "SELECT Cid FROM customer WHERE " + sql_where.join(" AND ")
    var args = []
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][getCid_byUserName] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// userDao.ModifyUserState = function (param, cb) {
//     if (typeof param == "undefined" || typeof param.state == 'undefined' || typeof param.cid == 'undefined') {
//         cb(null, { code: code.DB.PARA_FAIL, msg: null }, null);
//         return;
//     }

//     var sql = "UPDATE customer SET State=? WHERE Cid=? ";
//     var args = [param.state, param.cid];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });

// }

// userDao.get_game_setting_byUserId = function (cid, cb) {

//     if (typeof cid == "undefined") {
//         cb(null, { code: code.DB.PARA_FAIL, msg: null }, null);
//         return;
//     }

//     var sql = "SELECT * FROM game_setting  WHERE Cid=? ";
//     var args = [cid];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// userDao.getUserCurrency_byCid = function (cid, cb) {

//     if (typeof cid == "undefined") {
//         cb(null, { code: code.DB.PARA_FAIL, msg: null }, null);
//         return;
//     }

//     var sql = "SELECT Currency AS currency FROM customer WHERE Cid=? ";
//     var args = [cid];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

userDao.checkIsExist_Mail = function (data, cb) {
  try {
    var sql_where = []
    if (typeof data.cid != "undefoned" && data.cid != "") {
      sql_where.push(" (Cid !='" + data.cid + "') ")
    }
    var sql_where_text = sql_where.length > 0 ? "AND " + sql_where.join(" AND ") : ""
    var sql = " SELECT count(*) as count FROM customer WHERE Email=? " + sql_where_text
    var args = [data.mail]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][checkIsExist_Mail] catch err", err)
    cb(null, code.FAIL, null)
  }
}
//補jackpot 注單 -判斷有無此資料
userDao.checkJackpotRecordIsExist = function (data, cb) {
  try {
    var sql = "SELECT count(Wid) AS count FROM jackpot_record WHERE Wid=?"
    var args = [data.Wid]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][checkJackpotRecordIsExist] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//補jackpot 注單
userDao.addJackpotRecord = function (data, cb) {
  try {
    var insert_sql = []
    Object.keys(data).forEach((item) => {
      insert_sql.push(item + " = '" + data[item] + "'")
    })
    var sql = "INSERT jackpot_record SET " + insert_sql.join(" , ")
    var args = []
    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][addJackpotRecord] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getJackpotRecord = function (data, cb) {
  try {
    var sql = sprintf("SELECT * FROM jackpot_record WHERE Wid='%s' ", data.Wid)
    var args = []
    var res_data = []
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var info = { jackpot_record: r_data }
        res_data.push(info)
        cb(null, r_code, res_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][getJackpotRecord] catch err", err)
    cb(null, code.FAIL, null)
  }
}
/*
  haId:array
  agId:array
*/
userDao.getUserName_byCid = function (data, cb) {
  try {
    var sql_where = []
    sql_where.push(
      "((Cid IN('" + data.haId.join("','") + "') AND isAg=2) OR (Cid IN('" + data.agId.join("','") + "') AND isAg=3)) "
    )
    var sql_where_text = sql_where.length == 0 ? "1" : sql_where.join(" AND ")
    var sql = "SELECT Cid, UserName, Upid, DC FROM customer WHERE " + sql_where_text
    var args = []
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][getUserName_byCid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getUser_byUpId = function (data, cb) {
  try {
    var sql_where = []

    //ha層級
    if (data.level == 2) {
      var hallId = ""
      if (data.isSub == 1) {
        hallId = data.hallId
      } else {
        hallId = data.cid
      }
      sql_where.push(" HallId = '" + hallId + "' ")
    }
    //ag層級
    if (data.level == 3) {
      var upId = ""
      if (data.isSub == 1) {
        upId = data.upId
      } else {
        upId = data.cid
      }
      sql_where.push(" Upid = '" + upId + "' ")
    }

    if (typeof data.state != "undefined") {
      sql_where.push(" state = 'N' ")
    }

    var sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : " 1 "
    var sql = "SELECT Cid,Upid,HallId,IsAg As level FROM customer WHERE " + sql_where_text
    var args = []

    //var db_link = pomelo.app.get('dbclient_g_rw');
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][getUser_byUpId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.get_player_byCid = function (data, cb) {
  try {
    // var sql_where = [];
    // sql_where.push(" Cid = '" + data.cid + "' ");

    // var sql_where_text = (sql_where.length > 0) ? sql_where.push(" AND ") : " 1 ";

    var sql = "SELECT Cid, UserName, Upid, HallId, Currency, Quota, IsAg, DC FROM customer WHERE Cid = ?"
    var args = [data.cid]
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][get_player_byCid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getLevelName_byLevel = function (data, cb) {
  try {
    var sql = "SELECT AuthorityType,AuthorityTypeName FROM back_office_authority_type WHERE AuthorityType=?"
    var args = [data]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data && r_data.length === 1) {
          var authName = r_data[0]["AuthorityTypeName"]
          cb(
            null,
            {
              code: code.OK,
              msg: "",
            },
            authName
          )
        } else {
          cb(
            null,
            {
              code: code.DB.DATA_EMPTY,
              msg: "Invalid User",
            },
            null
          )
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][getLevelName_byLevel] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//更新使用者連線時間
userDao.modifyUserConnectState_v2 = function (data, cb) {
  try {
    if (
      typeof data === "undefined" ||
      typeof data.isAdmin === "undefined" ||
      typeof data.userId === "undefined" ||
      typeof data.userName === "undefined" ||
      typeof data.token === "undefined"
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
    if (typeof data.isAdmin) var now = timezone.serverTime()

    var sql =
      "INSERT INTO user_connection (front_server,isAdmin,userId,userName,connect_key,actTime,closeTime) \
                   VALUES (?,?,?,?,?,?,'0000-00-00 00:00:00' ) ON DUPLICATE KEY UPDATE connect_key=?, actTime=?,front_server=? ;"
    var args = [
      data.frontendId,
      data.isAdmin,
      data.userId,
      data.userName,
      data.token,
      now,
      data.token,
      now,
      data.frontendId,
    ]
    console.log("-modifyUserConnection-", sql, JSON.stringify(args))
    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][modifyUserConnectState_v2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getUserInfoByToken = function (data, cb) {
  try {
    var sql = sprintf(
      "SELECT isAdmin,userId,userName,connect_key,Date_Format(actTime,'%%Y-%%m-%%d %%H:%%i:%%s') AS actTime FROM user_connection WHERE connect_key = '%s' ",
      data.token
    )
    var args = []
    var res_data = []

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][getUserInfoByToken] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// userDao.getUser_byCid = function (data, cb) {

//     var sql = 'SELECT A.UserName,A.Passwd,A.NickName,A.IsAg AS Level,A.IsSub,A.Upid, A.FirstLogin, A.AuthorityTemplateID AS AuthorityTemplateId,A.IsDemo,A.LastLoginDate,' +
//         'A.IsSingleWallet,A.Currency,A.Quota,A.State,A.IsOnline,A.Cid,A.HallId,ha.State AS ha_state,' +
//         'B.Currencies, B.APIOutDomain,B.HallDesc,' +
//         'A.Birthday AS Birthday,A.Email,A.Address,' +
//         'B.APIHallOwnerName,B.SecureKey,C.OTPCode ' +
//         'FROM (SELECT * FROM customer WHERE BINARY Cid = ?) AS A ' +
//         ' LEFT JOIN hall AS B ON A.Cid=B.HallId ' +
//         ' LEFT JOIN customer ha ON A.HallId = ha.Cid ' +
//         ' LEFT JOIN otp_auth AS C ON (C.Cid = A.Cid AND C.IsAdmin=0) ';

//     var args = [data.userId];

//     console.log('-getUser_byUserName sql-', sql, JSON.stringify(args));

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             if (r_data && r_data.length === 1) {
//                 var usr = r_data[0];
//                 cb(null, { code: code.OK, msg: "" }, usr);
//             } else {
//                 cb(null, { code: code.DB.DATA_EMPTY, msg: "Invalid User" }, null);
//             }
//         }
//     });
// };

userDao.modifyUserInfoByToken = function (data, cb) {
  try {
    var sql = "UPDATE user_connection SET front_server=? WHERE connect_key=?"
    var args = [data.frontendId, data.token]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][modifyUserInfoByToken] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.getUserFrontServerByUserId = function (data, cb) {
  try {
    var sql = "SELECT  front_server FROM user_connection WHERE userId=? AND userName=?"
    var args = [data.userId, data.userName]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][getUserFrontServerByUserId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 更換 token 以延時
// userDao.prolongToken = function (data, cb) {
//     try {
//         var sql = "UPDATE user_connection SET connect_key=? WHERE connect_key=?";
//         var args = [data.newToken, data.oldToken];

//         db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//             cb(null, r_code, r_data);
//         });
//     } catch (err) {
//         logger.error('[userDao][prolongToken] catch err', err);
//         cb(null, code.FAIL, null);
//     }
// }

// userDao.getDownPlayers  = function (data, cb) {
//     var sql_where = [];
//     //admin
//     if ( data.opLevel == 1) { }
//     //ha
//     if ( data.opLevel == 2) {
//         var userId = (data.opIsSub == 1) ? data.opHallId : data.opCid;
//         sql_where.push(" cus.HallId ='" + userId + "'");
//     }
//     //ag
//     if ( data.opLevel == 3) {
//         sql_where.push(" cus.Upid ='" + data.opCid + "'");
//     }
//     sql_where.push(" cus.IsAg = 4");
//     var sql_where_text = (sql_where.length>0) ? sql_where.join(" AND ") :"1";
//     var sql = "SELECT Cid FROM customer cus WHERE " +sql_where_text;
//     var args = [];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// 檢查有無重複 DC
userDao.checkIsExist_DC = function (data, cb) {
  try {
    const sql_where = []
    if (typeof data.cid != "undefined" && data.cid != "") {
      sql_where.push(" (Cid !='" + data.cid + "') ")
    }
    const sql_where_text = sql_where.length > 0 ? "AND " + sql_where.join(" AND ") : ""
    const sql = " SELECT count(*) as count FROM customer WHERE UPPER(DC) = ? " + sql_where_text

    const uppercaseDC = data.dc ? data.dc.toUpperCase() : ""

    const args = [uppercaseDC]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][checkIsExist_DC] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 找上線幣別和 Cid
userDao.findUpId = function (AgCid, cb) {
  try {
    var sql = "SELECT Currency, Cid, HallId, IsSingleWallet FROM customer WHERE Cid = ? "
    var args = [AgCid]
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code != code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data && r_data.length == 1) {
          cb(null, { code: code.OK }, r_data)
        } else {
          cb(null, { code: code.DB.DATA_EMPTY, msg: "DATA_EMPTY" }, null)
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][findUpId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 檢查 OTP 是否有重設過
userDao.check_OtpCode = function (data, cb) {
  var sql_where = []
  switch (data["sel_table"]) {
    case "admin":
      sql_where.push(" (admin.AdminId = otp_auth.Cid) ")
      break
    case "customer":
      var userId = data.user_isSub == 1 || data["user_level"] == 3 ? data.user_hallId : data.user_cid
      sql_where.push(" (customer.Cid = otp_auth.Cid) ")
      break
  }

  var sql =
    "SELECT otp_auth.Cid, otp_auth.State FROM " +
    data["sel_table"] +
    " INNER JOIN otp_auth ON " +
    sql_where +
    " WHERE UserName = ?"
  var args = [data.name]
  db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
    if (r_code.code !== code.OK) {
      cb(null, r_code, null)
    } else {
      if (r_data && r_data.length > 0) {
        cb(null, { code: code.OK }, r_data)
      } else {
        cb(null, { code: code.DB.DATA_EMPTY, msg: "DATA_EMPTY" })
      }
    }
  })
}

// 關閉 OTP
userDao.close_OtpCode = function (data, cb) {
  try {
    if (
      typeof data == "undefined" ||
      typeof data.Cid == "undefined" ||
      typeof data.IsAdmin == "undefined" ||
      typeof data.HallId == "undefined"
    ) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var sql = "UPDATE otp_auth SET State = ? WHERE Cid = ? AND HallId = ? AND IsAdmin = ? "
    var args = [0, data.Cid, data.HallId, data.IsAdmin]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[userDao][close_OtpCode] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 新增 HA、Agent 的管理者
userDao.createUser_ResellerOrAgent = function (data, cb) {
  try {
    var data_user = data
    var cid = ""

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

        const brandId = data.brandId || ""
        const brandTitle = data.brandTitle || ""

        var sql =
          "INSERT INTO customer (Cid, UserName,Passwd,NickName,IsAg,IsSub,Upid,HallId," +
          "AuthorityTemplateID,IsDemo,IsSingleWallet,Quota,State,Email,Birthday,Currency,`DC`,brandId,brandTitle) " +
          "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

        var birthday = data_user.birthday !== null ? data_user.birthday.substr(0, 10) : ""
        var args = [
          data_user.cid,
          data_user.name,
          data_user.password,
          data_user.nickName,
          data_user.isAg,
          data_user.isSub,
          data_user.upid,
          data_user.hallid,
          data_user.authorityId,
          0,
          data_user.isSingleWallet,
          0,
          data_user.state,
          data_user.email,
          birthday,
          data_user.currencies,
          data.dc,
          brandId,
          brandTitle,
        ]

        connection.query(sql, args, function (err, results) {
          if (err) {
            return connection.rollback(function () {
              cb(null, { code: code.DB.CREATE_FAIL, msg: err })
            })
          }

          cid = data_user.cid
          var game_currency =
            typeof data_user.game_currency != "undefined" &&
            data_user.game_currency.length > 0 &&
            Array.isArray(data_user.game_currency)
              ? data_user.game_currency.join(",")
              : data_user.game_currency //遊戲的開放幣別設定

          var sql = "INSERT INTO sub_user (`Cid`,`UserName`,`IsAg`,`IsSub`,`Currencies`)" + "VALUES (?,?,?,?,?)"
          var args = [data_user.cid, data_user.name, data_user.isAg, data_user.isSub, game_currency]

          connection.query(sql, args, function (err, results) {
            if (err) {
              return connection.rollback(function () {
                cb(null, { code: code.DB.CREATE_FAIL, msg: err })
              })
            }

            connection.commit(function (err) {
              if (err) {
                return connection.rollback(function () {
                  cb(null, { code: code.DB.QUERY_FAIL, msg: err })
                })
              }
              connection.release()
              cb(null, { code: code.OK, msg: "" }, cid)
            })
          })
        })
      })
    })
  } catch (err) {
    logger.error("[userDao][createUser_ResellerOrAgent] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 找出與修改權限 id 一樣的使用者 (上線被調整時，下線權限一起調整)
userDao.getCustsByAuthorityID = function (data, cb) {
  try {
    var sql = "SELECT Cid FROM customer WHERE AuthorityTemplateID = ? "
    var args = [data.tmpId]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data.length > 0) r_data = r_data.map((item) => item.Cid)
        cb(null, { code: code.OK }, r_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][getCustsByAuthorityID] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 找出所有上線
// select語句一般用於輸出用戶變量，可以選擇@變量名，用於輸出數據源不是表格的數據。使用select時必須用“:=赋值”
// 用戶變量與數據庫連接有關，在連接中聲明的變量，在存儲過程中創建了用戶變量後一直到數據庫實例接續的時候，變量就會消失
// data = {userId: '',tableName:''}
userDao.getUpIdByInfiniteClass = function (data, cb) {
  try {
    var getColumns
    switch (data["tableName"]) {
      case "wallet":
        getColumns =
          " r.Cid, q.UpId, r.UserName, r.State, GROUP_CONCAT(w.Currency) AS currencies, GROUP_CONCAT(w.Quota) AS quotas \
                    FROM (SELECT @r as _id, (SELECT @r := UpId FROM customer WHERE Cid = _id limit 1) as UpId "
        break
      case "credit":
        getColumns =
          " r.Cid, q.UpId, r.UserName, r.State, GROUP_CONCAT(w.Currency) AS currencies, GROUP_CONCAT(w.CreditQuota) AS quotas \
                    FROM (SELECT @r as _id, (SELECT @r := UpId FROM customer WHERE Cid = _id limit 1) as UpId "
        break
    }

    /*var sql = "SELECT r.Cid, q.UpId, r.UserName, r.State, GROUP_CONCAT(w.Currency) AS currencies, GROUP_CONCAT(w.Quota) AS quotas \
        FROM (SELECT @r as _id, (SELECT @r := UpId FROM customer WHERE Cid = _id limit 1) as UpId \
        FROM (select @r := ?) AS vars, customer WHERE @r <> -1) as q \
        LEFT JOIN customer AS r ON r.Cid = q._id \
        LEFT JOIN wallet AS w ON w.Cid = r.Cid \
        GROUP BY r.Cid DESC";*/
    var sql =
      "SELECT " +
      getColumns +
      " FROM (select @r := ?) AS vars, customer WHERE @r <> -1) as q " +
      " LEFT JOIN customer AS r ON r.Cid = q._id " +
      " LEFT JOIN " +
      data.tableName +
      " AS w ON w.Cid = r.Cid " +
      " GROUP BY r.Cid ORDER BY AddDate DESC"

    var args = [data.userId]
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, { code: code.OK }, r_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][getUpIdByInfiniteClass][catch] err: %s, dc: %s", err)
    cb(null, code.FAIL, null)
  }
}

// 新增多筆玩家資料
userDao.createUser_BatchPlayers = function (data, cb) {
  try {
    pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
      if (err) {
        connection.release()
        cb({ code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }

      var game_currency =
        typeof data.game_currency != "undefined" && data.game_currency.length > 0 && Array.isArray(data.game_currency)
          ? data.game_currency[0]
          : data.game_currency //遊戲的開放幣別設定
      //要執行的-sql
      var sql_query = []
      var sql_insert_prs = []
      for (var i = 0; i < 2; i++) {
        let userName = data.dc + "_testPlayer" + data.nameSuffix + "00" + i
        // 超過字元，取部分字串寫入 DB
        let nickName = userName.length < 40 ? userName : userName.substr(0, 40)
        let realName = userName.length < 20 ? userName : userName.substr(0, 20)
        const brandId = data.brandId || ""
        const brandTitle = data.brandTitle || ""

        var insert_text = sprintf(
          "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')",
          short.generate(),
          userName,
          m_md5("qwe123"),
          nickName,
          4,
          0,
          data.upid,
          data.hallid,
          "-1",
          1,
          game_currency,
          80000,
          "N",
          realName,
          "",
          "",
          "",
          data.dc,
          data.isSingleWallet,
          brandId,
          brandTitle
        )
        sql_insert_prs.push(insert_text)
      }
      // 多行資料處理，可在 ON DUPLICATE KEY UPDATE 後用 VALUES
      var sql_update_prs_text = " ON DUPLICATE KEY UPDATE Cid = VALUES(Cid) "
      if (sql_insert_prs.length > 0) {
        // ex: INSERT INTO wallet ( Cid, Currency) VALUES ('7dT93QK8nYME','CNY'), ('7dT93QK8nYME','INR') ON DUPLICATE KEY UPDATE Currency = VALUES(Currency)
        var sql =
          "INSERT INTO customer (`Cid`,`UserName`,`Passwd`,`NickName`,`IsAg`,`IsSub`,`Upid`,`HallId`,`AuthorityTemplateID`," +
          "`IsDemo`,`Currency`,`Quota`,`State`,`RealName`,`Birthday`,`Address`,`Email`,`DC`,`IsSingleWallet`, `brandId`, `brandTitle`) VALUES " +
          sql_insert_prs.join(" , ") +
          sql_update_prs_text
        sql_query.push(sql)
      }

      //-----------------transaction start---------------
      connection.beginTransaction(function (err) {
        var funcAry = []
        sql_query.forEach(function (sql, index) {
          console.log("----createUser_BatchPlayers sql ---", index, sql)
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
    logger.error("[userDao][createUser_BatchPlayers] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 修改 wallet 額度
// data: {cid, currency, amount}
userDao.modifyWalletQuota = function (data, cb) {
  try {
    let sqlSet = " SET Quota = @quotaResult := (Quota + ?)"
    let argsSet = [data.amount]

    let sqlWhere = " WHERE Cid = ? AND Currency = ? AND (Quota + ?) >= 0"
    let argsWhere = [data.cid, data.currency, data.amount]

    let sql = "UPDATE game.wallet" + sqlSet + sqlWhere
    let args = [...argsSet, ...argsWhere]

    let sql2 = "SELECT @quotaResult as QuotaResult"
    let args2 = []

    db.act_query_multi("dbclient_g_rw", [sql, sql2], [args, args2], function (r_code, r_data) {
      if (r_code.code != code.OK) {
        logger.error("[userDao][modifyWalletQuota] act_transaction failed! err: ", r_code)
        cb(null, r_code, null)
      } else {
        if (r_data[0].affectedRows == 0) {
          // 額度不足
          cb(null, { code: code.FAIL }, null)
        } else {
          cb(null, r_code, r_data[1][0]["QuotaResult"])
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][modifyWalletQuota] catch err", err)
    cb(null, { code: code.FAIL }, null)
  }
}

// 取得信用額度資料
userDao.get_credits_list = function (data, cb) {
  try {
    if (typeof data === "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var res_data = []
    var sql_where = []

    if (typeof data.Cid != "undefined" && data.Cid != "") {
      sql_where.push(" Cid = '" + data.Cid + "'")
    }

    if (sql_where.length == 0) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }
    var sql_where_text = sql_where.join(" AND ")

    let sortType = "DESC"
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    let sortKey = "Currency"

    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      var sort_info = {
        creditCurrency: "Currency",
        CreditQuota: "CreditQuota",
        CurrentQuota: "CurrentQuota",
        Notified: "Notified",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }

    var sql = sprintf("SELECT * FROM credit WHERE %s ORDER BY %s %s", sql_where_text, sortKey, sortType)

    var args = []

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var info = { credits_list: r_data }
        res_data.push(info)
        cb(null, { code: code.OK, msg: "" }, res_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][get_credits_list] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 新增信用額度
userDao.CreateCreditsList = function (data, cb) {
  try {
    if (typeof data === "undefined" || !Array.isArray(data)) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var sql_where = []
    var sql_from_name = ""
    for (var i in data) {
      sql_from_name = " (Cid, Currency, CreditQuota, CurrentQuota, AlertValue) VALUES"
      sql_where.push(
        sprintf(
          "('%s','%s','%s','%s','%s')",
          data[i]["Cid"],
          data[i]["Currency"],
          data[i]["Credit"],
          data[i]["Credit"],
          data[i]["AlertValue"]
        )
      )
    }

    var sql = " INSERT credit" + sql_from_name + sql_where.join(" , ")
    var args = []

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][CreateCreditsList] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 修改信用額度
userDao.ModifyCreditsList = function (data, cb) {
  try {
    if (typeof data === "undefined" || !Array.isArray(data)) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var sql_update = []
    if (typeof data[0]["CreditQuota"] != "undefined" && data[0]["CreditQuota"] != "") {
      sql_update.push(sprintf(" CreditQuota = '%s' ", data[0]["CreditQuota"]))
    }

    if (typeof data[0]["AlertValue"] != "undefined" && data[0]["AlertValue"] != "") {
      sql_update.push(sprintf(" AlertValue = '%s' ", data[0]["AlertValue"]))
    }

    sql_update.push(sprintf(" Notified = '%s' ", data[0]["Notified"]))

    var sql = "UPDATE credit SET " + sql_update.join(",") + " WHERE Cid = ? AND Currency = ? "
    var args = [data[0]["Cid"], data[0]["Currency"]]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][ModifyCreditsList] catch err", err)
    cb(null, code.FAIL, null)
  }
}
// 修改 credit 額度
// data: {cid, currency, amount}
userDao.ModifyCreditQuota = function (data, cb) {
  try {
    let sqlSet = " SET CreditQuota = @quotaResult := (CreditQuota + ?)"
    let argsSet = [data.amount]

    let sqlWhere = " WHERE Cid = ? AND Currency = ? AND (CreditQuota + ?) >= 0"
    let argsWhere = [data.cid, data.currency, data.amount]

    let sql = "UPDATE game.credit" + sqlSet + sqlWhere
    let args = [...argsSet, ...argsWhere]

    let sql2 = "SELECT @quotaResult as QuotaResult"
    let args2 = []

    db.act_query_multi("dbclient_g_rw", [sql, sql2], [args, args2], function (r_code, r_data) {
      if (r_code.code != code.OK) {
        logger.error("[userDao][ModifyCreditQuota] act_transaction failed! err: ", r_code)
        cb(null, r_code, null)
      } else {
        if (r_data[0].affectedRows == 0) {
          // 額度不足
          cb(null, { code: code.FAIL }, null)
        } else {
          cb(null, r_code, r_data[1][0]["QuotaResult"])
        }
      }
    })
  } catch (err) {
    logger.error("[userDao][ModifyCreditQuota] catch err", err)
    cb(null, { code: code.FAIL }, null)
  }
}

// 刪除信用額度
userDao.DeleteCreditsList = function (data, cb) {
  try {
    if (typeof data === "undefined" || !Array.isArray(data)) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var sql = "DELETE FROM credit WHERE Cid = ? AND Currency = ?"
    var args = [data[0]["Cid"], data[0]["Currency"]]

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
  } catch (err) {
    logger.error("[userDao][DeleteCreditsList] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 重置信用額度
userDao.ResetCreditsList = function (data, cb) {
  try {
    if (typeof data === "undefined" || !Array.isArray(data)) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var sql_query = []
    data.forEach((item) => {
      var update_sql =
        "UPDATE credit SET CurrentQuota = '" +
        item.CreditQuota +
        "', Notified = '' WHERE Cid = '" +
        item.Cid +
        "' AND Currency = '" +
        item.Currency +
        "'"
      sql_query.push(update_sql)
    })

    db.act_transaction("dbclient_g_rw", sql_query, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, { code: code.DB.DATA_EMPTY }, null)
      } else {
        cb(null, { code: code.OK }, r_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][ResetCreditsList] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 合併 wallet 裡的幣別和額度字串
userDao.getConcatCurrForWallet = function (data, cb) {
  try {
    if (typeof data == "undefined" || typeof data["cid"] == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }

    var getColumns
    switch (data["tableName"]) {
      case "wallet":
        getColumns = " Cid, GROUP_CONCAT(Currency) AS top_currencies, GROUP_CONCAT(Quota) AS top_quotas "
        break
      case "credit":
        getColumns = " Cid, GROUP_CONCAT(Currency) AS top_currencies, GROUP_CONCAT(CreditQuota) AS top_quotas "
        break
    }
    var sql_where = []
    if (Array.isArray(data["cid"])) {
      sql_where.push(" Cid IN ('" + data["subResalers"].join("','") + "') ")
    } else {
      sql_where.push(" Cid = '" + data["cid"] + "' ")
    }

    // var sql = " SELECT Cid, GROUP_CONCAT(Currency) AS top_currencies, GROUP_CONCAT(Quota) AS top_quotas FROM wallet WHERE " + sql_where + " GROUP BY Cid";
    var sql = " SELECT " + getColumns + " FROM " + data["tableName"] + " WHERE " + sql_where + " GROUP BY Cid"
    var args = []
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][getConcatCurrForWallet] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 取得告警資料 有修改因應需要CreditQuota CurrentQuota
userDao.getCreditNotifyValue = function (cb) {
  try {
    var sql =
      "SELECT cr.Cid, cr.Currency,  cu.UserName, cu.DC, FORMAT((cr.CreditQuota/1000),2) AS CreditQuota, FORMAT((cr.CurrentQuota/1000),2) AS CurrentQuota, cr.AlertValue, FORMAT((cr.CurrentQuota / cr.CreditQuota *100), 2) AS NotifyValue, " +
      "cr.Notified AS originNotify, cu.Email " +
      "FROM credit cr LEFT JOIN customer cu ON cu.Cid = cr.Cid GROUP BY cr.Cid, cr.Currency"
    var args = []

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][getCreditNotifyValue] catch err", err)
    cb(null, code.FAIL, null)
  }
}

userDao.ModifyCreditsListBatch = function (data, cb) {
  try {
    pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
      if (err) {
        connection.release()
        cb({ code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }

      var sql_query = []
      var sql_insert_credit = []

      var sql_update_credit_text =
        " ON DUPLICATE KEY UPDATE Cid = VALUES(Cid), Currency = VALUES(Currency), Notified = VALUES(Notified) "

      data.forEach((item) => {
        var insert_text = sprintf("('%s','%s','%s','%s')", item.Cid, item.Currency, item.AlertValue, item.Notified)
        sql_insert_credit.push(insert_text)
      })

      if (sql_insert_credit.length > 0) {
        var sql =
          "INSERT INTO credit (Cid, Currency, AlertValue, Notified) VALUES " +
          sql_insert_credit.join(" , ") +
          sql_update_credit_text
        sql_query.push(sql)
      }

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
    })
  } catch (err) {
    logger.error("[userDao][ModifyCreditsListBatch] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 根據父查詢子 hallId
userDao.getCid_byParent = function (data, cb) {
  try {
    var params = "cid"
    var sql_where = []
    let cid = ""
    if (typeof data.usr !== "undefined" && !data.usr.isTransferLine) {
      params = "upid"
      sql_where.push(" AND IsAg = 2 ")
      cid = data.usr.cid
    }

    // for getContribution:
    if (typeof data.hall_agent_search_Id !== "undefined") {
      sql_where.push(" AND IsAg = 2 ")
      cid = data.hall_agent_search_Id
    }

    if (typeof data.checkParent !== "undefined") {
      sql_where.push(" AND IsAg = 2 AND IsDemo = 0 ")
      cid = data.cid
    }

    var sql =
      "SELECT Cid, UserName, IsAg" +
      " FROM (SELECT t1.*, IF(FIND_IN_SET(upid, @pids) > 0, @pids := CONCAT(@pids, ','," +
      params +
      "), '0') AS isChild " +
      " FROM (SELECT * FROM game.customer ORDER BY cid ASC) t1,(SELECT @pids := ?) t2 " +
      " ) t3 WHERE isChild != '0' " +
      sql_where

    const args = [cid]

    logger.info("[userDao][getCid_byParent][sql] %s [args] %s", sql, args)
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, { code: code.OK }, r_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][getCid_byParent] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 以 HallId 查底層所有 HallId (不含管理員)
userDao.getChildList = function (data, cb) {
  try {
    if (typeof data == "undefined" || typeof data.cid == "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: "Cid not find." }, null)
      return
    }

    const { isAg = 2, isSub = 0 } = data

    const sql = "SELECT game.getChildList(?,?,?) as list"
    const args = [data.cid, isAg, isSub]

    logger.info("[userDao][getChildList][sql] %s [args] %s", sql, args)
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        logger.error("[userDao][getChildList] r_code: ", JSON.stringify(r_code))
        cb(null, r_code, null)
      } else {
        cb(null, { code: code.OK }, r_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][getChildList] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 取得 Hall 階層關係列表
userDao.getHallList = function (cb) {
  try {
    let sql = "SELECT Cid, DC, Upid" + " FROM game.customer" + " WHERE IsAg = 2 AND IsSub = 0"
    let args = []

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][getHallList] err: ", JSON.stringify(err))
    cb(null, { code: code.FAIL }, null)
  }
}

// 取得轉線所有資料
userDao.getTransferLineList = function (data, cb) {
  try {
    var sql_where = []
    var sql_where_text = ""

    if (typeof data.operate_acct != "undefined" && data.operate_acct != "") {
      sql_where.push(" t.Cid = '" + data.operate_acct + "' ")
    }
    if (sql_where.length > 0) {
      sql_where_text = " WHERE " + sql_where.join(" AND ")
    }

    var sortKey = "Transfer_Date"
    var sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        cid: "Cid",
        transfer_date: "Transfer_Date",
        type: "type",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)

    var sql_limit = sprintf(" LIMIT %i,%i", (data.page - 1) * data.pageCount, data.pageCount)

    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS * FROM ( \
             SELECT t.Cid, oldC.UserName, \
             CASE WHEN oldC.IsAg = 2 and oldC.IsSub = 0 THEN 'Hall' WHEN oldC.IsAg = 3 and oldC.IsSub = 0 THEN 'Agent' WHEN oldC.IsAg = 4 THEN 'Player' ELSE 'SubUser' END AS userTypeText, \
             CASE WHEN t.Old_Upid = -1 THEN '-' ELSE oldU.Cid END AS Old_Upid, \
             CASE WHEN t.Old_Upid = -1 THEN '-' ELSE oldU.UserName END AS Old_UpName, \
             CASE WHEN t.New_Upid = -1 THEN '-' ELSE newU.Cid END AS New_Upid, \
             CASE WHEN t.New_Upid = -1 THEN '-' ELSE newU.UserName END AS New_UpName, \
             ad.AdminId AS Transfer_Cid, ad.UserName AS Transfer_Name, \
             CASE WHEN t.Transfer_IsAg = 1 THEN 'Operator' \
             WHEN t.Transfer_IsAg = 2 THEN 'hall' WHEN t.Transfer_IsAg = 3 THEN 'Agent' ELSE 'Player' END AS Transfer_type, \
             Date_Format(t.Transfer_Date,'%%Y-%%m-%%d %%H:%%i:%%s') AS Transfer_Date, t.Transfer_OnOff \
             FROM game.upid_transfer t \
             LEFT JOIN game.customer oldC ON oldC.Cid = t.Cid \
             LEFT JOIN game.customer oldU ON oldU.Cid = t.Old_Upid \
             LEFT JOIN game.customer newU ON newU.Cid = t.New_Upid \
             LEFT JOIN game.admin ad ON ad.AdminId = t.Transfer_Cid %s) a \
             %s %s ",
      sql_where_text,
      order_by_text,
      sql_limit
    )

    var sql2 = "SELECT FOUND_ROWS() AS ROWS;"
    var sql_query = []
    sql_query.push(sql)
    sql_query.push(sql2)

    logger.info("[userDao][getTransferLineList][sql] %s", sql_query)
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
  } catch (err) {
    logger.error("[userDao][getTransferLineList] catch err", JSON.stringify(err))
    cb(null, { code: code.FAIL }, null)
  }
}

// 更新轉線的 UpId、HallId
userDao.updateUpIdHallId_customer = function (data, cb) {
  try {
    if (typeof data == "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }
    var saveData = {}
    switch (data.transferLine) {
      case "hall":
        saveData = {
          Upid: data.target_acct,
          HallId: data.target_acct_Upid,
          AuthorityTemplateID: data.userPermission,
        }
        break
      case "user_ha":
        saveData = {
          HallId: data.target_acct,
          Upid: "-1",
          IsAg: "2",
          AuthorityTemplateID: data.userPermission,
        }
        break
      case "agent":
        saveData = {
          Upid: data.target_acct,
          HallId: data.target_acct,
          AuthorityTemplateID: data.userPermission,
        }
        break
      case "user_ag":
        saveData = {
          Upid: data.target_acct,
          HallId: data.target_acct_HallId,
          IsAg: "3",
          AuthorityTemplateID: data.userPermission,
        }
        break
      case "player":
        saveData = {
          Upid: data.target_acct,
        }
        break
    }
    var table = "customer"
    var sql_where_text = sprintf(" Cid = '%s'", data.operate_acct)

    db.act_update_data("dbclient_g_rw", table, saveData, sql_where_text, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][modifyUpId_byTransferLine] catch err", JSON.stringify(err))
    cb(null, { code: code.FAIL }, null)
  }
}

// 新增修改轉線資料
userDao.createUpdate_UpidTransfer = function (data, cb) {
  try {
    if (typeof data == "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    var sql =
      "INSERT upid_transfer(Cid, Old_Upid, New_Upid, Transfer_Cid, Transfer_OnOff, Transfer_Date)" +
      " VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE Cid = VALUES(Cid), Old_Upid = VALUES(Old_Upid)," +
      " New_Upid = VALUES(New_Upid), Transfer_Date = ?, Transfer_OnOff = ?"

    var args = [
      data.operate_acct,
      data.Old_Upid,
      data.New_Upid,
      data.Transfer_Cid,
      data.syncAccounting,
      timezone.serverTime(),
      timezone.serverTime(),
      data.syncAccounting,
    ]

    logger.info("[userDao][createUpdate_UpidTransfer][sql] %s [args] %s", sql, args)
    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][createUpdate_UpidTransfer] catch err", JSON.stringify(err))
    cb(null, { code: code.FAIL }, null)
  }
}

/**
 *
 * 更新該代理下面玩家的hallId
 *
 * @param {object} data
 * @param {string} data.agentId 玩家的代理ID
 * @param {string} data.hallId 玩家要更新成的代理商ID
 * @param {*} cb
 */
userDao.updatePlayerHallId = function (data, cb) {
  try {
    if (!data) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    const { agentId, hallId } = data

    if (!agentId || !hallId) {
      cb(null, { code: code.DB.PARA_FAIL }, null)
    }

    pomelo.app.get("dbclient_g_rw").getConnection(function (err, connection) {
      if (err) {
        connection.release()
        cb({ code: code.DB.GET_CONNECT_FAIL, msg: err })
        return
      }

      connection.beginTransaction(function (err) {
        if (err) {
          cb(null, { code: code.DB.QUERY_FAIL, msg: err })
          return
        }

        const sql = `UPDATE customer SET HallId = ? WHERE upid = ?`

        const args = [hallId, agentId]

        connection.query(sql, args, function (err, results) {
          if (err) {
            return connection.rollback(function () {
              cb(null, { code: code.DB.UPDATE_FAIL, msg: err })
            })
          }

          connection.commit(function (err) {
            if (err) {
              return connection.rollback(function () {
                cb(null, { code: code.DB.QUERY_FAIL, msg: err })
              })
            }
            connection.release()
            cb(null, { code: code.OK, msg: "" })
          })
        })
      })
    })
  } catch (err) {
    logger.error("[userDao][updatePlayerHallId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 由 DB 撈出滿足帳號類別的該頁線上玩家資料
// data: {[{playerId, gameId}], accTypeIds, pageCur, pageCount, sortKey, sortType, lang, requestDemo, hallName, agentName}
// 回傳 {count, list: [PlayerId, Cid, UserName, State, IsDemo, Upid, HallId, Quota, GameName, GameCurrency]}
userDao.getOnlinePlayerList = function (data, cb) {
  try {
    if (data.players.length == 0) {
      cb(null, { code: code.OK }, { count: 0, list: [] })
      return
    }

    let sqlWhere = ""
    let argsWhere = []
    if (data.accTypeIds.length !== 0) {
      if (data.requestDemo) {
        sqlWhere = " WHERE (c.IsDemo IN (?) OR c.IsDemo IS NULL)"
      } else {
        sqlWhere = " WHERE c.IsDemo IN (?)"
      }
      argsWhere.push(data.accTypeIds)
    } else if (!data.requestDemo) {
      sqlWhere = " WHERE c.IsDemo IS NOT NULL"
    }
    if (data.hallName != "") {
      if (sqlWhere == "") {
        sqlWhere += " WHERE BINARY c1.UserName = ?"
      } else {
        sqlWhere += " AND BINARY c1.UserName = ?"
      }
      argsWhere.push(data.hallName)
    } else if (data.agentName != "") {
      if (sqlWhere == "") {
        sqlWhere += " WHERE BINARY c2.UserName = ?"
      } else {
        sqlWhere += " AND BINARY c2.UserName = ?"
      }
      argsWhere.push(data.agentName)
    }

    let sqlOrder = " ORDER BY c.Cid"
    let sortInfo = {
      playerId: "c.Cid", // 試玩帳號編號以 NULL 排序
      userName: "UserName",
      userState: "c.State",
      quota: "c.Quota",
      accType: "c.IsDemo",
      gameName: "GameName",
      hallName: "HallName",
      agentName: "AgentName",
    }
    if (sortInfo[data.sortKey]) {
      sqlOrder = ` ORDER BY ${sortInfo[data.sortKey]}`
    }
    if (conf.SORT_TYPE[data.sortType]) {
      sqlOrder += ` ${conf.SORT_TYPE[data.sortType]}`
    }

    let sqlLimit = " LIMIT ?,?"
    let argsLimit = [(data.pageCur - 1) * data.pageCount, data.pageCount]

    // 使用的遊戲名稱欄位
    let gameName = "NameE" // 預設為 en
    if (data["lang"] == "cn") {
      gameName = "NameC"
    } else if (data["lang"] == "tw") {
      gameName = "NameG"
    }

    // 利用暫存表放玩家與遊戲的對應
    const tmpTableName = "tmp_online_player"

    let sql =
      `CREATE TEMPORARY TABLE ${tmpTableName}(` +
      "cid VARCHAR(30) NOT NULL," +
      " gameId INT(11) NOT NULL," +
      " INDEX(cid)" +
      ")"
    let args = []

    let sql2 =
      `INSERT INTO ${tmpTableName}(cid, gameId) VALUES ` +
      data.players.map((p) => `('${p.playerId}',${p.gameId})`).join(",")
    let args2 = []

    // 試玩帳號以 PlayerId 當作 UserName
    let sql3 =
      `SELECT t.cid AS PlayerId, c.Cid, IFNULL(c.UserName, t.cid) AS UserName, c.State, c.IsDemo, c.Upid, c.HallId, c.Quota, g.${gameName} AS GameName, GROUP_CONCAT(w.Currency) AS GameCurrency,` +
      ` IFNULL(c1.UserName, '') AS HallName,` +
      ` IFNULL(c2.UserName, '') AS AgentName` +
      ` FROM ${tmpTableName} t` +
      " LEFT JOIN game.customer c ON t.Cid = c.cid" +
      " LEFT JOIN game.games g ON t.gameId = g.gameId" +
      " LEFT JOIN game.wallet w ON c.Upid = w.Cid" +
      " LEFT JOIN game.customer c1 ON c.HallId = c1.cid AND c1.IsAg = 2 AND c1.IsSub = 0" +
      " LEFT JOIN game.customer c2 ON c.Upid = c2.cid AND c2.IsAg = 3 AND c2.IsSub = 0" +
      sqlWhere +
      " GROUP BY t.cid, g.gameId" +
      sqlOrder +
      sqlLimit
    let args3 = [...argsWhere, ...argsLimit]

    let sql4 =
      "SELECT COUNT(t.cid) AS ROWS" +
      ` FROM ${tmpTableName} t` +
      " LEFT JOIN game.customer c ON t.Cid = c.cid" +
      " LEFT JOIN game.customer c1 ON c.HallId = c1.cid AND c1.IsAg = 2 AND c1.IsSub = 0" +
      " LEFT JOIN game.customer c2 ON c.Upid = c2.cid AND c2.IsAg = 3 AND c2.IsSub = 0" +
      sqlWhere
    let args4 = [...argsWhere]

    let sql5 = `DROP TEMPORARY TABLE ${tmpTableName}`
    let args5 = []

    db.act_query_multi(
      "dbclient_g_r",
      [sql, sql2, sql3, sql4, sql5],
      [args, args2, args3, args4, args5],
      function (r_code, r_data) {
        if (r_code.code !== code.OK) {
          cb(null, r_code, null)
        } else {
          let data = {
            count: r_data[3][0]["ROWS"],
            list: r_data[2],
          }
          cb(null, r_code, data)
        }
      }
    )
  } catch (err) {
    logger.error("[userDao][getOnlinePlayerList] err: ", JSON.stringify(err))
    cb(null, { code: code.FAIL }, null)
  }
}

// 取欄位型態
userDao.getColumnsType = function (tableName, columns, cb) {
  try {
    if (typeof tableName == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: "[getColumnsType] need tableName" }, null)
      return
    }
    if (typeof columns == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: "[getColumnsType] need columns" }, null)
      return
    }
    let tableColumns = "'" + columns.join("','") + "'"
    let sql = sprintf("SHOW COLUMNS FROM %s WHERE field in (%s)", tableName, tableColumns)
    var args = []
    logger.info("[userDao][getColumnsType][sql] %s [args] %s", sql, args)
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, { code: code.OK }, r_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][getColumnsType][catch] err: %s, tableName: %s, columns: %s", err, tableName, columns)
    cb(null, { code: code.FAIL }, null)
  }
}

// 取得管理者資訊(game.sub_user)
userDao.get_subUser_byCid = function (data, cb) {
  try {
    if (typeof data == "undefined" || typeof data.cid == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }
    var sql = "SELECT Currencies FROM sub_user WHERE Cid = ?"
    var args = [data.cid]
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][get_subUser_byCid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//----------------------------------------------開放地區--------------------------------------------------------------------

// 取得開放地區
// data: {hallId, currency, useMaster}
// useMaster: 是否使用 master (寫修改後的 log 時, 若取 slave 的資料時, 可能 slave 還沒寫入, 此時指定它使用 master)
// 回傳 [{HallId, Currency, Countries}]
userDao.openAreaGet = function (data, cb) {
  try {
    let sqlWhere = " WHERE HallId = ? AND Currency = ?"
    let argsWhere = [data.hallId, data.currency]

    let sql = "SELECT HallId, Currency, Countries FROM game.open_area" + sqlWhere
    let args = [...argsWhere]

    db.act_query(data.useMaster ? "dbclient_g_rw" : "dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[userDao][openAreaGet] err: ", err)
    cb(null, { code: code.FAIL }, null)
  }
}
// 取得開放地區列表
// data: {pageCur, pageCount, hallId, sortKey, sortType}
// 回傳 {count, list: [Currency, Countries]}
userDao.openAreaList = function (data, cb) {
  try {
    let sqlWhere = " WHERE HallId = ?"
    let argsWhere = [data.hallId]

    let sqlOrder = " ORDER BY Currency"
    let sortInfo = {
      currency: "Currency",
    }
    if (sortInfo[data.sortKey]) {
      sqlOrder = ` ORDER BY ${sortInfo[data.sortKey]}`
    }
    if (conf.SORT_TYPE[data.sortType]) {
      sqlOrder += ` ${conf.SORT_TYPE[data.sortType]}`
    }

    let sqlLimit = " LIMIT ?,?"
    let argsLimit = [(data.pageCur - 1) * data.pageCount, data.pageCount]

    let sql = "SELECT Currency, Countries FROM game.open_area" + sqlWhere + sqlOrder + sqlLimit
    let args = [...argsWhere, ...argsLimit]

    let sql2 = "SELECT COUNT(Currency) AS ROWS FROM game.open_area" + sqlWhere
    let args2 = [...argsWhere]

    db.act_query_multi("dbclient_g_r", [sql, sql2], [args, args2], function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        let data = {
          count: r_data[1][0]["ROWS"],
          list: r_data[0],
        }
        cb(null, r_code, data)
      }
    })
  } catch (err) {
    logger.error("[userDao][openAreaList] err: ", err)
    cb(null, { code: code.FAIL }, null)
  }
}
// 新增開放地區
// data: {hallId, currency, countries}
userDao.openAreaAdd = function (data, cb) {
  try {
    let saveData = {
      HallId: data.hallId,
      Currency: data.currency,
      Countries: data.countries,
    }

    db.act_insert_data("dbclient_g_rw", "game.open_area", saveData, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[userDao][openAreaAdd] err: ", err)
    cb(null, { code: code.FAIL })
  }
}
// 修改開放地區
// data: {hallId, currency, countries}
userDao.openAreaMod = function (data, cb) {
  try {
    let sqlWhere = "HallId = ? AND Currency = ?"
    let argsWhere = [data.hallId, data.currency]
    let saveData = {
      Countries: data.countries,
    }

    db.act_update_data("dbclient_g_rw", "game.open_area", saveData, sqlWhere, argsWhere, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[userDao][openAreaMod] err: ", err)
    cb(null, { code: code.FAIL })
  }
}
// 刪除開放地區
// data: {hallId, currency}
userDao.openAreaDel = function (data, cb) {
  try {
    let sqlWhere = " WHERE HallId = ? AND Currency = ?"
    let argsWhere = [data.hallId, data.currency]

    let sql = "DELETE FROM game.open_area" + sqlWhere
    let args = [...argsWhere]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[userDao][openAreaDel] err: ", err)
    cb(null, { code: code.FAIL })
  }
}

//----------------------------------------------域名設定--------------------------------------------------------------------

// 取得域名設定
// data: {hallId, useMaster}
// useMaster: 是否使用 master (寫修改後的 log 時, 若取 slave 的資料時, 可能 slave 還沒寫入, 此時指定它使用 master)
// 回傳 [{url, merchId, merchPwd, apiKey, defAgent,
//        showCreditSymbol, lobbyBalance, showClock, showHelp, oneClickHelp, verifyToken, demoWallet,
//        ggIdSettings: [ggId, singleWalletType, singleWalletClipType, clip, clipReload]}]
// 回傳 [] 表示未設定
userDao.domainSettingGet = function (data, cb) {
  try {
    let sqlWhere = " WHERE HallId = ?"
    let argsWhere = [data.hallId]

    let sql =
      "SELECT Url, MerchId, MerchPwd, APIKey," +
      " IF(DefAgentId != -1, (SELECT UserName FROM game.customer WHERE Cid = DefAgentId AND IsAg = 3 AND IsSub = 0), '') AS DefAgent," +
      " ShowCreditSymbol, LobbyBalance, ShowClock, ShowHelp, OneClickHelp, VerifyToken, DemoWallet" +
      " FROM game.domain_setting" +
      sqlWhere
    let args = [...argsWhere]

    let sql2 =
      "SELECT GGId, SingleWalletType, SingleWalletClipType, Clip, ClipReload" +
      " FROM game.domain_ggid_setting" +
      sqlWhere +
      " ORDER BY GGId ASC"
    let args2 = [...argsWhere]

    db.act_query_multi(
      data.useMaster ? "dbclient_g_rw" : "dbclient_g_r",
      [sql, sql2],
      [args, args2],
      function (r_code, r_data) {
        if (r_code.code !== code.OK) {
          cb(null, r_code, null)
        } else {
          let ret = []
          if (r_data[0].length > 0) {
            let data = {
              url: r_data[0][0].Url,
              merchId: r_data[0][0].MerchId,
              merchPwd: r_data[0][0].MerchPwd,
              apiKey: r_data[0][0].APIKey,
              defAgent: r_data[0][0].DefAgent,
              showCreditSymbol: r_data[0][0].ShowCreditSymbol,
              lobbyBalance: r_data[0][0].LobbyBalance,
              showClock: r_data[0][0].ShowClock,
              showHelp: r_data[0][0].ShowHelp,
              oneClickHelp: r_data[0][0].OneClickHelp,
              verifyToken: r_data[0][0].VerifyToken,
              demoWallet: r_data[0][0].DemoWallet,
              ggIdSettings: [],
            }
            for (let row of r_data[1]) {
              data.ggIdSettings.push({
                ggId: row.GGId,
                singleWalletType: row.SingleWalletType,
                singleWalletClipType: row.SingleWalletClipType,
                clip: row.Clip,
                clipReload: row.ClipReload,
              })
            }
            ret.push(data)
          }

          cb(null, r_code, ret)
        }
      }
    )
  } catch (err) {
    logger.error("[userDao][domainSettingGet] err: ", err)
    cb(null, { code: code.FAIL }, null)
  }
}
// 編輯域名設定
// data: {hallId, url, merchId, merchPwd, apiKey, defAgentId,
//        showCreditSymbol, lobbyBalance, showClock, showHelp, oneClickHelp, verifyToken, demoWallet,
//        ggIdSettings: [{ggId, singleWalletType, singleWalletClipType, clip, clipReload}]}
userDao.domainSettingEdit = function (data, cb) {
  try {
    // 新增/修改 domain_setting
    let insertData = {
      HallId: data.hallId,
      Url: data.url,
      MerchId: data.merchId,
      MerchPwd: data.merchPwd,
      APIKey: data.apiKey,
      DefAgentId: data.defAgentId,
      ShowCreditSymbol: data.showCreditSymbol,
      LobbyBalance: data.lobbyBalance,
      ShowClock: data.showClock,
      ShowHelp: data.showHelp,
      OneClickHelp: data.oneClickHelp,
      VerifyToken: data.verifyToken,
      DemoWallet: data.demoWallet,
    }
    let insertKeys = Object.keys(insertData)
    let updateKeys = Object.keys(insertData)
    updateKeys.splice(0, 1) // 不 update primary key
    let sql =
      `INSERT INTO game.domain_setting(${insertKeys.join(",")})` +
      ` VALUES(?)` +
      ` ON DUPLICATE KEY UPDATE ${updateKeys.map((k) => `${k} = VALUES(${k})`).join(",")}`
    let args = [Object.values(insertData)]

    // 刪除 domain_ggid_setting
    let sqlGGId = ""
    let argsGGId = []
    if (data.ggIdSettings.length > 0) {
      sqlGGId = " AND GGId NOT IN (?)"
      argsGGId.push(data.ggIdSettings.map((item) => item.ggId))
    }
    let sql2 =
      `DELETE FROM game.domain_ggid_setting` +
      ` WHERE HallId = ? AND GGId IN (SELECT GGId FROM (SELECT GGId FROM game.domain_ggid_setting WHERE HallId = ?${sqlGGId}) t)`
    let args2 = [data.hallId, data.hallId, ...argsGGId]

    let sqlAarry = [sql, sql2]
    let argsAarry = [args, args2]

    // 批量新增/修改 domain_ggid_setting
    let insertData_ggids = []
    for (let ggIdSetting of data.ggIdSettings) {
      let insertData_ggid = {
        HallId: data.hallId,
        GGId: ggIdSetting.ggId,
        SingleWalletType: ggIdSetting.singleWalletType,
        SingleWalletClipType: ggIdSetting.singleWalletClipType,
        Clip: ggIdSetting.clip,
        ClipReload: ggIdSetting.clipReload,
      }
      insertData_ggids.push(insertData_ggid)
    }
    if (insertData_ggids.length) {
      let insertKeys_ggid = Object.keys(insertData_ggids[0])
      let updateKeys_ggid = Object.keys(insertData_ggids[0])
      updateKeys_ggid.splice(0, 2) // 不 update primary key
      let sql3 =
        `INSERT INTO game.domain_ggid_setting(${insertKeys_ggid.join(",")})` +
        ` VALUES${Array.from({ length: insertData_ggids.length })
          .map((x) => "(?)")
          .join(",")}` +
        ` ON DUPLICATE KEY UPDATE ${updateKeys_ggid.map((k) => `${k} = VALUES(${k})`).join(",")}`
      let args3 = insertData_ggids.map((item) => Object.values(item))
      sqlAarry.push(sql3)
      argsAarry.push(args3)
    }

    db.act_query_multi("dbclient_g_rw", sqlAarry, argsAarry, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[userDao][domainSettingEdit] err: ", err)
    cb(null, { code: code.FAIL }, null)
  }
}
// 刪除域名設定
// data: {hallId}
userDao.domainSettingDel = function (data, cb) {
  try {
    let sqlWhere = " WHERE HallId = ?"
    let argsWhere = [data.hallId]

    let sql = "DELETE FROM game.domain_setting" + sqlWhere
    let args = [...argsWhere]

    let sql2 = "DELETE FROM game.domain_ggid_setting" + sqlWhere
    let args2 = [...argsWhere]

    db.act_query_multi("dbclient_g_rw", [sql, sql2], [args, args2], function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[userDao][domainSettingDel] err: ", err)
    cb(null, { code: code.FAIL }, null)
  }
}

/**
 * 取得指定 DC 的玩家資料
 * @param {object} data
 * @param {*} cb
 */
userDao.getUserByDC = function (data, cb) {
  try {
    const { dc, userLevel = 4 } = data
    const sql = `SELECT Cid,UserName,UpId,HallId FROM customer WHERE DC = ? AND IsAg = ?`
    const args = [dc, userLevel]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[userDao][getUserByDC] err: ", err)
    cb(null, { code: code.FAIL }, null)
  }
}
