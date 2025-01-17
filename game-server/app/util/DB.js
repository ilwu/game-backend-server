var pomelo = require("pomelo")
var m_async = require("async")
var code = require("./code")
var sprintf = require("sprintf-js").sprintf
var db = module.exports

db.act_query = function (db_name, sql, args, cb) {
  // console.log('-sel_query-----', db_name, sql, JSON.stringify(args));
  pomelo.app.get(db_name).getConnection(function (err, connection) {
    if (err) {
      cb({ code: code.DB.GET_CONNECT_FAIL, msg: err }, null)
      return
    }
    let query = connection.query(sql, args, function (err, res) {
      connection.release()
      if (err) {
        cb({ code: code.DB.QUERY_FAIL, msg: err }, null)
      } else {
        //console.log('-sel_query res-', res);
        cb({ code: code.OK }, res)
      }
    })
    console.log("-sel_query- sql:", query.sql)
  })
}

// cb(r_code, r_data)
db.act_query_multi = function (db_name, sqls, args, cb) {
  // console.log('act_query_multi sqls', JSON.stringify(sqls));
  pomelo.app.get(db_name).getConnection(function (err, connection) {
    if (err) {
      connection.release()
      cb({ code: code.DB.GET_CONNECT_FAIL, msg: err })
      return
    }

    let funcAry = []
    let querySqls = []
    sqls.forEach((sql, index) => {
      let temp = function (cb) {
        let query = connection.query(sql, args[index] || [], function (err, res) {
          if (err) {
            cb(err)
          } else {
            cb(null, res)
          }
        })
        querySqls.push(query.sql)
      }
      funcAry.push(temp)
    })

    m_async.series(funcAry, function (err, r_data) {
      connection.release()
      console.log("-act_query_multi- sqls:", JSON.stringify(querySqls))
      if (err) {
        cb({ code: code.DB.UPDATE_FAIL, msg: err }, null)
      } else {
        cb({ code: code.OK, msg: "" }, r_data)
      }
    })
  })
}

db.act_transaction = function (db_name, sql_query, cb) {
  console.log("act_transaction sql_query", JSON.stringify(sql_query))
  pomelo.app.get(db_name).getConnection(function (err, connection) {
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
        console.log("act_transaction err", JSON.stringify(err))
        console.log("act_transaction result", JSON.stringify(result))
        if (err) {
          connection.rollback(function (err) {
            connection.release()
            return cb({ code: code.DB.QUERY_FAIL, msg: "" }, null)
          })
        } else {
          connection.commit(function (err, info) {
            if (err) {
              connection.rollback(function (err) {
                connection.release()
                return cb({ code: code.DB.QUERY_FAIL, msg: err }, null)
              })
            } else {
              connection.release()
              return cb({ code: code.OK }, result)
            }
          })
        }
      })
    })
    //-----------------transaction end---------------
  })
}

/*
取筆數
*/
db.act_info_rows = function (db_name, sql, cb) {
  var self = this
  var sql2 = "SELECT FOUND_ROWS() AS ROWS;"
  var sql_query = []
  sql_query.push(sql)
  sql_query.push(sql2)

  self.act_transaction(db_name, sql_query, function (r_code, r_data) {
    if (r_code.code !== code.OK) {
      cb(r_code, null)
    } else {
      var data = {
        count: r_data[1][0]["ROWS"],
        info: r_data[0],
      }
      cb(r_code, data)
    }
  })
}

/*
判斷有無此欄位名
*/
db.act_getCOLUMNS = function (db_name, table, cb) {
  var sql = sprintf("SHOW COLUMNS FROM %s", table)
  var self = this
  self.act_query(db_name, sql, [], function (r_code, r_data) {
    if (r_code.code !== code.OK) {
      cb(r_code, null)
    } else {
      var fieldAry = r_data.map((item) => item.Field)
      cb(
        {
          code: code.OK,
        },
        fieldAry
      )
    }
  })
}

/*
新增 
db_name: DB名稱
table:資料表
saveData: 要新增的資料
*/
db.act_insert_data = function (db_name, table, saveData, cb) {
  var self = this
  self.act_getCOLUMNS(db_name, table, function (r_code, fieldAry) {
    if (r_code.code !== code.OK) {
      cb(r_code, null)
    } else {
      // 過濾留下合法的欄位
      let fieldData = Object.keys(saveData)
        .filter((key) => fieldAry.indexOf(key) !== -1)
        .reduce((res, key) => ((res[key] = saveData[key]), res), {})

      var sql = `INSERT INTO ${table} SET ?`
      // console.log('act_insert_data-sql', sql);
      self.act_query(db_name, sql, [fieldData], function (r_code, r_data) {
        console.log("insert res", JSON.stringify(r_code), JSON.stringify(r_data))
        if (r_code.code !== code.OK) {
          cb(r_code, null)
        } else {
          cb(r_code, r_data.insertId)
        }
      })
    }
  })
}

/*
更新
db_name: DB名稱
table:資料表
saveData: 要修改的資料
*/

db.act_update_data = function (db_link, table, saveData, sql_where, argsWhere, cb) {
  var self = this
  self.act_getCOLUMNS(db_link, table, function (r_code, fieldAry) {
    if (r_code.code !== code.OK) {
      cb(r_code, null)
    } else {
      // 過濾留下合法的欄位
      let fieldData = Object.keys(saveData)
        .filter((key) => fieldAry.indexOf(key) !== -1)
        .reduce((res, key) => ((res[key] = saveData[key]), res), {})

      var sql = `UPDATE ${table} SET ? WHERE ${sql_where}`
      // console.log('act_update_data-sql', sql);
      self.act_query(db_link, sql, [fieldData, ...argsWhere], function (r_code, r_data) {
        console.log("update res", JSON.stringify(r_code), JSON.stringify(r_data))
        cb(r_code, r_data)
      })
    }
  })
}

// db.act_transaction_2 = function (db_link, sql_query, cb) {
//     db_link.getConnection(function (err, connection) {
//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }
//         //-----------------transaction start---------------
//         connection.beginTransaction(function (err) {
//             var funcAry = [];
//             sql_query.forEach(function (sql, index) {
//                 var temp = function (cb) {
//                     connection.query(sql, [], function (temp_err, results) {
//                         if (temp_err) {
//                             connection.rollback(function () {
//                                 return cb(code.DB.QUERY_FAIL);
//                             });
//                         } else {
//                             return cb(code.ok, results);
//                         }
//                     })
//                 };
//                 funcAry.push(temp);
//             });

//             m_async.series(funcAry, function (err, result) {
//                 console.log('act_transaction err', JSON.stringify(err));
//                 console.log('act_transaction result', JSON.stringify(result));
//                 if (err) {
//                     connection.rollback(function (err) {
//                         connection.release();
//                         return cb({ code: code.DB.QUERY_FAIL, msg: '' }, null);
//                     });
//                 } else {
//                     connection.commit(function (err, info) {
//                         if (err) {
//                             connection.rollback(function (err) {
//                                 connection.release();
//                                 return cb({ code: code.DB.QUERY_FAIL, msg: err }, null);
//                             });
//                         } else {
//                             connection.release();
//                             return cb({ code: code.OK }, result);
//                         }
//                     })
//                 }
//             });
//         });
//         //-----------------transaction end---------------
//     });
// }

//---------------------------------------------------------------------------------------------------------

// db.act_query_2 = function (db_link, sql, args, cb) {
//     db_link.getConnection(function (err, connection) {
//         if (err) {
//             cb({ code: code.DB.GET_CONNECT_FAIL, msg: err }, null);
//             return;
//         }
//         connection.query(sql, args, function (err, res) {
//             connection.release();
//             if (err) {
//                 cb({ code: code.DB.QUERY_FAIL, msg: err }, null);
//             } else {
//                 cb({ code: code.OK }, res);
//             }
//         });
//     });
// }
