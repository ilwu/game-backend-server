const logger = require("pomelo-logger").getLogger("configDao", __filename)
const utils = require("../util/utils")
const code = require("../util/code")
const db = require("../util/DB")
const pomelo = require("pomelo")
const m_async = require("async")
const sprintf = require("sprintf-js").sprintf
const timezone = require("../util/timezone")
const dbUtils = require("../util/dbUtils.js")

const configDao = module.exports

configDao.getCompany = function(cb) {
  try {
    const sql = "SELECT Id,Value FROM game_company WHERE 1 ORDER BY Id ASC ;"
    const args = []
    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getCompany] catch err", err)
    cb(null, code.FAIL, null)
  }
}
configDao.getRTPs = function(cb) {
  try {
    const sql = "SELECT Id,Value FROM game_rtp WHERE 1 ORDER BY Id ASC "
    const args = []
    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getRTPs] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getDenoms = function(cb) {
  try {
    const sql = "SELECT Id,Value FROM game_denom WHERE 1 ORDER BY Id ASC "
    const args = []
    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getDenoms] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getGameType = function(cb) {
  try {
    const sql = "SELECT Id,Type,Value FROM game_type WHERE 1 ORDER BY Value ASC "
    const args = []

    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getGameType] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getGameGroup = function(cb) {
  try {
    const sql = "SELECT GGId,NameC,NameG,NameE,NameVN,NameTH,NameID,NameMY,NameJP,NameKR FROM game_group WHERE 1 "
    const args = []

    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getGameGroup] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getWhiteType = function(cb) {
  try {
    const sql = "SELECT Id,Code FROM white_type WHERE 1 ORDER BY Id ASC "
    const args = []

    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getWhiteType] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getBlackType = function(cb) {
  const sql = "SELECT Id,Code FROM black_type WHERE 1 ORDER BY Id ASC "
  const args = []
  db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
    cb(r_code, r_data)
  })
}

configDao.getOrderType = function(cb) {
  try {
    const sql = "SELECT Id,Value FROM order_type WHERE 1 ORDER BY Id ASC "
    const args = []

    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getOrderType] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getMathRng = function(cb) {
  try {
    const sql = "SELECT Id,Value FROM math_project WHERE 1 ORDER BY Id ASC "
    const args = []

    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getMathRng] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getGameGroup_2 = function(cb) {
  try {
    const sql = "SELECT GGId,NameC,NameG,NameE,NameVN,NameTH,NameID,NameMy FROM game_group WHERE 1 "
    const args = []

    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getGameGroup_2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getGameType_2 = function(cb) {
  try {
    const sql = "SELECT Id,Type,Value FROM game_type WHERE 1 ORDER BY Value ASC "
    const args = []

    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getGameType_2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getCompany_2 = function(cb) {
  try {
    const sql = "SELECT Id,Value FROM game_company WHERE 1 ORDER BY Id ASC ;"
    const args = []
    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getCompany_2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getDenoms_2 = function(cb) {
  try {
    const sql = "SELECT Id,Value FROM game_denom WHERE 1 ORDER BY Id ASC "
    const args = []
    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getDenoms_2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getRTPs_2 = function(cb) {
  try {
    const sql = "SELECT Id,Value FROM game_rtp WHERE 1 ORDER BY Value ASC "
    const args = []
    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getRTPs_2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getMathRng_2 = function(cb) {
  try {
    const sql = "SELECT Id,Value FROM math_project WHERE 1 ORDER BY Id ASC "
    const args = []

    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getMathRng_2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getOrderType_2 = function(cb) {
  try {
    const sql = "SELECT Id,Value FROM order_type WHERE 1 ORDER BY Id ASC "
    const args = []

    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getOrderType_2] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getAuthorityTemps = function(cb) {
  try {
    const sql =
      "SELECT AuthorityTemplateID,AuthorityTemplateDesc,AuthorityType,AuthorityJson, AuthorityTemplateNote FROM back_office_authority"
    const args = []

    pomelo.app.get("dbclient_g_r").getConnection(function(err, connection) {
      if (err) {
        cb(null, {
          code: code.DB.GET_CONNECT_FAIL,
          msg: err,
        })
        return
      }
      connection.query(sql, args, function(err, res) {
        connection.release()

        if (err) {
          cb(
            {
              code: code.CONFIG.LOAD_CONFIG_AUTHORITY_FAIL,
              msg: err.stack,
            },
            null,
          )
          // utils.invokeCallback(cb, { code: code.CONFIG.LOAD_CONFIG_AUTHORITY_FAIL, msg: err.stack }, null);
        } else {
          if (res) {
            const authoritysById = {}
            const authoritysByType = {}
            var i = 0

            for (var i in res) {
              authoritysById[res[i].AuthorityTemplateID] = {
                desc: res[i].AuthorityTemplateDesc,
                type: res[i].AuthorityType,
                funcs: res[i].AuthorityJson,
                note: res[i].AuthorityTemplateNote,
              }

              if (typeof authoritysByType[res[i].AuthorityType] === "undefined") {
                authoritysByType[res[i].AuthorityType] = []
              }

              authoritysByType[res[i].AuthorityType].push({
                id: res[i].AuthorityTemplateID,
                desc: res[i].AuthorityTemplateDesc,
                funcs: res[i].AuthorityJson,
                note: res[i].AuthorityTemplateNote,
              })
            }
            cb(
              {
                code: code.OK,
                msg: "",
              },
              {
                authorityTmpCounts: res.length,
                authoritysById: authoritysById,
                authoritysByType: authoritysByType,
              },
            )
          } else {
            cb(
              {
                code: code.CONFIG.LOAD_CONFIG_AUTHORITY_EMPTY,
                msg: "Authority Empty",
              },
              null,
            )
          }
        }
      })
    })
  } catch (err) {
    logger.error("[configDao][getAuthorityTemps] catch err", err)
    cb(null, code.FAIL, null)
  }
}
// configDao.getAuthorityTemps_v2 = function (data, cb) {

//     var sql = 'SELECT AuthorityTemplateID,AuthorityTemplateDesc,AuthorityType,AuthorityJson FROM back_office_authority WHERE AuthorityTemplateID=? ';
//     var args = [data.tempId];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK || r_data.length == 0) {
//             cb(null, {
//                 code: code.CONFIG.LOAD_CONFIG_AUTHORITY_FAIL,
//                 msg: err.stack
//             }, null);
//         } else {
//             var authoritysById = {};
//             var authoritysByType = {};
//             var i = 0;

//             for (var i in r_data) {
//                 authoritysById[r_data[i].AuthorityTemplateID] = {
//                     desc: r_data[i].AuthorityTemplateDesc,
//                     type: r_data[i].AuthorityType,
//                     funcs: r_data[i].AuthorityJson
//                 };

//                 if (typeof authoritysByType[r_data[i].AuthorityType] === 'undefined') {
//                     authoritysByType[r_data[i].AuthorityType] = [];
//                 }

//                 authoritysByType[r_data[i].AuthorityType].push({
//                     id: r_data[i].AuthorityTemplateID,
//                     desc: r_data[i].AuthorityTemplateDesc,
//                     funcs: r_data[i].AuthorityJson
//                 });
//             }
//             cb(null, {
//                 code: code.OK,
//                 msg: ""
//             }, {
//                 authorityTmpCounts: r_data.length,
//                 authoritysById: authoritysById,
//                 authoritysByType: authoritysByType
//             });
//         }
//     });
// };

configDao.getAuthorityFuncs = function(cb) {
  try {
    const sql =
      "SELECT FunctionID AS id,ParentID AS pId, FunctionType AS funType, LOWER(FunctionGroupL) AS type, LOWER(FunctionGroupM) AS funM, LOWER(FunctionGroupS) AS funS, LOWER(FunctionAction) AS action, " +
      " FunctionNameE AS nameE,FunctionNameG AS nameG,FunctionNameC AS nameC " +
      " FROM back_office_function_list WHERE FunctionState =1 ORDER BY FunctionNameG ASC ,  FunctionID ASC "
    const args = []

    pomelo.app.get("dbclient_g_r").getConnection(function(err, connection) {
      if (err) {
        cb(null, {
          code: code.DB.GET_CONNECT_FAIL,
          msg: err,
        })
        return
      }
      connection.query(sql, args, function(err, res) {
        connection.release()
        if (err) {
          cb(
            {
              code: code.CONFIG.LOAD_CONFIG_FUNCTION_FAIL,
              msg: err.stack,
            },
            null,
          )
        } else {
          if (res) {
            const functions = {}
            var i = 0
            for (var i in res) {
              functions[res[i].id] = res[i]
            }
            cb(
              {
                code: code.OK,
                msg: "",
              },
              {
                funcByArray: res,
                funcById: functions,
              },
            )
          } else {
            cb(
              {
                code: code.CONFIG.LOAD_CONFIG_FUNCTION_EMPTY,
                msg: "Authority Empty",
              },
              null,
            )
          }
        }
      })
    })
  } catch (err) {
    logger.error("[configDao][getAuthorityFuncs] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// configDao.getAuthorityFuncs_v2 = function (cb) {

//     var sql = 'SELECT FunctionID AS id,ParentID AS pId, FunctionType AS funType, LOWER(FunctionGroupL) AS type, LOWER(FunctionGroupM) AS funM, LOWER(FunctionGroupS) AS funS, LOWER(FunctionAction) AS action, ' +
//         ' FunctionNameE AS nameE,FunctionNameG AS nameG,FunctionNameC AS nameC ' +
//         ' FROM back_office_function_list WHERE FunctionState =1 ORDER BY FunctionID ASC ';
//     var args = [];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, {
//                 code: code.CONFIG.LOAD_CONFIG_FUNCTION_EMPTY,
//                 msg: "Authority Empty"
//             }, null);
//         } else {
//             var functions = {};
//             var i = 0;
//             for (var i in r_data) {
//                 functions[r_data[i].id] = r_data[i];
//             }
//             cb(null, {
//                 code: code.OK,
//                 msg: ""
//             }, {
//                 funcByArray: r_data,
//                 funcById: functions
//             });
//         }
//     });

//     // pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
//     //     if (err) {
//     //         cb(null, {
//     //             code: code.DB.GET_CONNECT_FAIL,
//     //             msg: err
//     //         });
//     //         return;
//     //     }
//     //     connection.query(sql, args, function (err, res) {
//     //         connection.release();
//     //         if (err) {
//     //             cb(null,{
//     //                 code: code.CONFIG.LOAD_CONFIG_FUNCTION_FAIL,
//     //                 msg: err.stack
//     //             }, null);
//     //         } else {
//     //             if (res) {
//     //                 var functions = {};
//     //                 var i = 0;
//     //                 for (var i in res) {
//     //                     functions[res[i].id] = res[i];
//     //                 }
//     //                 cb({
//     //                     code: code.OK,
//     //                     msg: ""
//     //                 }, {
//     //                     funcByArray: res,
//     //                     funcById: functions
//     //                 });
//     //             } else {
//     //                 cb(null,{
//     //                     code: code.CONFIG.LOAD_CONFIG_FUNCTION_EMPTY,
//     //                     msg: "Authority Empty"
//     //                 }, null);
//     //             }
//     //         }
//     //     });
//     // });

// };

//權限範本類別
configDao.getLevel = function(cb) {
  try {
    const sql =
      "SELECT AuthorityType AS id," +
      "CASE AuthorityTypeName " +
      "WHEN \"AD\" THEN \"Admin\" " +
      "WHEN \"HA\" THEN \"Reseller\" " +
      "WHEN \"AG\" THEN \"Agent\" " +
      "WHEN \"SUB\" THEN \"SubUser\" " +
      "END AS type " +
      "FROM back_office_authority_type " +
      "WHERE AuthorityType IN(1,2,3,5)"

    const args = []

    pomelo.app.get("dbclient_g_r").getConnection(function(err, connection) {
      if (err) {
        cb(null, {
          code: code.DB.GET_CONNECT_FAIL,
          msg: err,
        })
        return
      }
      connection.query(sql, args, function(err, res) {
        connection.release()
        if (err) {
          cb(
            {
              code: code.DB.QUERY_FAIL,
              msg: err.stack,
            },
            null,
          )
        } else {
          if (res) {
            const levels = res
            cb(
              {
                code: code.OK,
                msg: "",
              },
              levels,
            )
          } else {
            cb(
              {
                code: code.DB.DATA_EMPTY,
                msg: "AUTHTYPE Empty",
              },
              null,
            )
          }
        }
      })
    })
  } catch (err) {
    logger.error("[configDao][getLevel] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//取系統限制時間
configDao.getSystemLimitTime = function(cb) {
  try {
    const sql = " SELECT item,content FROM system WHERE item= 'action_time_limit';"

    const args = []
    pomelo.app.get("dbclient_g_r").getConnection(function(err, connection) {
      if (err) {
        cb(null, {
          code: code.DB.GET_CONNECT_FAIL,
          msg: err,
        })
        return
      }
      connection.query(sql, args, function(err, res) {
        connection.release()
        if (err) {
          cb(
            {
              code: code.DB.QUERY_FAIL,
              msg: err.stack,
            },
            null,
          )
        } else {
          if (res) {
            const sec = +res[0]["content"] * 60
            cb(
              {
                code: code.OK,
                msg: "",
              },
              sec,
            )
          } else {
            cb(
              {
                code: code.DB.DATA_EMPTY,
                msg: "ACTION_TIME_LIMIT Empty",
              },
              null,
            )
          }
        }
      })
    })
  } catch (err) {
    logger.error("[configDao][getSystemLimitTime] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//取系統預設天數
configDao.getSystemSeachDays = function(cb) {
  try {
    const sql = " SELECT item,content FROM system WHERE item= 'search_days';"
    const args = []

    pomelo.app.get("dbclient_g_r").getConnection(function(err, connection) {
      if (err) {
        cb(null, {
          code: code.DB.GET_CONNECT_FAIL,
          msg: err,
        })
        return
      }
      connection.query(sql, args, function(err, res) {
        connection.release()
        if (err) {
          cb(
            {
              code: code.DB.QUERY_FAIL,
              msg: err.stack,
            },
            null,
          )
        } else {
          if (res) {
            cb(
              {
                code: code.OK,
                msg: "",
              },
              res[0]["content"],
            )
          } else {
            cb(
              {
                code: code.DB.DATA_EMPTY,
                msg: "Data Empty",
              },
              null,
            )
          }
        }
      })
    })
  } catch (err) {
    logger.error("[configDao][getSystemSeachDays] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//取系統主幣別
configDao.getSystemMainCurrency = function(cb) {
  try {
    const sql = " SELECT item,content FROM system WHERE item= 'main_currency'; "
    const args = []

    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      if (r_code.code !== code.OK || r_data.length == 0) {
        cb(r_code, null)
      } else {
        cb(r_code, r_data[0]["content"])
      }
    })
  } catch (err) {
    logger.error("[configDao][getSystemMainCurrency] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//取每頁筆數
configDao.getCountsOfPerPage = function(cb) {
  try {
    const sql = " SELECT item,content FROM system WHERE item= 'counts_of_per_page';"
    const args = []

    pomelo.app.get("dbclient_g_r").getConnection(function(err, connection) {
      if (err) {
        cb(null, {
          code: code.DB.GET_CONNECT_FAIL,
          msg: err,
        })
        return
      }
      connection.query(sql, args, function(err, res) {
        connection.release()
        if (err) {
          cb(
            {
              code: code.DB.QUERY_FAIL,
              msg: err.stack,
            },
            null,
          )
        } else {
          if (res) {
            cb(
              {
                code: code.OK,
                msg: "",
              },
              res[0]["content"],
            )
          } else {
            cb(
              {
                code: code.DB.DATA_EMPTY,
                msg: "Data Empty",
              },
              null,
            )
          }
        }
      })
    })
  } catch (err) {
    logger.error("[configDao][getCountsOfPerPage] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getCurrenies = function(cb) {
  try {
    const sql = "SELECT DISTINCT Currency AS currency FROM currency"
    const args = []

    pomelo.app.get("dbclient_g_r").getConnection(function(err, connection) {
      if (err) {
        cb(null, {
          code: code.DB.GET_CONNECT_FAIL,
          msg: err,
        })
        return
      }
      connection.query(sql, args, function(err, res) {
        connection.release()
        if (err) {
          cb(
            {
              code: code.CONFIG.LOAD_CFG_CURRENCY_FAIL,
              msg: err.stack,
            },
            null,
          )
        } else {
          if (res) {
            const currencies = res
            cb(
              {
                code: code.OK,
                msg: "",
              },
              currencies,
            )
          } else {
            cb(
              {
                code: code.CONFIG.LOAD_CFG_CURRENCY_EMPTY,
                msg: "Currency Empty",
              },
              null,
            )
          }
        }
      })
    })
  } catch (err) {
    logger.error("[configDao][getCurrenies] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getActionType = function(cb) {
  try {
    /*
        var sql = " SELECT FunctionGroupL , group_concat( FunctionAction separator  ',') AS FunctionAction FROM back_office_function_list " +
            " WHERE FunctionType IN('A','D','E','O')" +
            " GROUP BY FunctionGroupL ";
        */
    const sql =
      " SELECT FunctionGroupL, FunctionAction, FunctionNameE AS nameE,FunctionNameC AS nameC,FunctionNameG AS nameG FROM back_office_function_list \
        WHERE FunctionType IN('A','D','E','O') AND FunctionState=1 "
    const args = []

    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(null, r_code, r_data)
    })
    // pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
    //     if (err) {
    //         cb(null, {
    //             code: code.DB.GET_CONNECT_FAIL,
    //             msg: err
    //         }, null);
    //         return;
    //     }
    //     connection.query(sql, args, function (err, res) {
    //         connection.release();
    //         if (err) {
    //             cb({
    //                 code: code.DB.QUERY_FAIL,
    //                 msg: err.stack
    //             }, null);
    //         } else {
    //             if (res) {
    //                 cb({
    //                     code: code.OK,
    //                     msg: ""
    //                 }, res);
    //             } else {
    //                 cb({
    //                     code: code.DB.DATA_EMPTY,
    //                     msg: "ACTION_FUNC Empty"
    //                 }, null);
    //             }
    //         }
    //     });
    // });
  } catch (err) {
    logger.error("[configDao][getActionType] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getNowExchangeRate = function(msg, cb) {
  try {
    const now = timezone.serverTime()
    const sql =
      " SELECT Currency,ExCurrency,CryDef FROM currency_exchange_rate WHERE Currency =? AND ExCurrency =? AND EnableTime <= ? ORDER BY EnableTime DESC limit 0,1 "
    const args = [msg.currency, msg.exCurrency, now]

    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(null, r_code, r_data)
    })

    // pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
    //     if (err) {
    //         cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err }, null);
    //         return;
    //     }
    //     connection.query(sql, args, function (err, res) {
    //         connection.release();
    //         if (err) {
    //             cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
    //         } else {
    //             if (res) {
    //                 cb(null, { code: code.OK, msg: "" }, res);
    //             } else {
    //                 cb(null, { code: code.DB.DATA_EMPTY, msg: "Exchange Empty" }, null);
    //             }

    //         }
    //     });
    // });
  } catch (err) {
    logger.error("[configDao][getNowExchangeRate] catch err", err)
    cb(null, code.FAIL, null)
  }
}
//取時區設定
configDao.getTimezoneSet = function(cb) {
  try {
    const sql =
      " SELECT t.HourDiff AS hourDiff,t.DescE AS hourDiff_descE,t.DescG AS hourDiff_descG,t.DescC AS hourDiff_descC  FROM timezone_setting t ORDER BY t.HourDiff ASC  "
    const args = []

    pomelo.app.get("dbclient_g_r").getConnection(function(err, connection) {
      if (err) {
        cb(
          null,
          {
            code: code.DB.GET_CONNECT_FAIL,
            msg: err,
          },
          null,
        )
        return
      }
      connection.query(sql, args, function(err, res) {
        connection.release()
        if (err) {
          cb(
            {
              code: code.DB.QUERY_FAIL,
              msg: err.stack,
            },
            null,
          )
        } else {
          if (res) {
            cb(
              {
                code: code.OK,
                msg: "",
              },
              res,
            )
          } else {
            cb(
              {
                code: code.DB.DATA_EMPTY,
                msg: "timezone Empty",
              },
              null,
            )
          }
        }
      })
    })
  } catch (err) {
    logger.error("[configDao][getTimezoneSet] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// configDao.getAuthData = function (data, cb) {

//     if (typeof data.user_level == 'undefined') {
//         cb(null, code.DB.PARA_FAIL, null);
//     }

//     var sql_where = [];
//     sql_where.push(data.user_level + " = 1 ");

//     if(typeof data.up_authority !='undefined'){
//         sql_where.push( " FunctionID IN ('"+ data.up_authority.join("','")+"') ");
//     }

//     var sql = "  SELECT GROUP_CONCAT( FunctionID ) AS funID FROM back_office_function_list WHERE " + sql_where.join(" AND ");
//     var args = [];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

configDao.getAuthData_byUpUser = function(data, cb) {
  try {
    const sql_where = []
    sql_where.push(data.user_level + " = 1 ")
    if (typeof data.up_authority != "undefined")
      sql_where.push(" FunctionID IN('" + data.up_authority.join("','") + "') ")
    const sql =
      "SELECT GROUP_CONCAT( FunctionID ORDER BY ParentID, FunctionID, FunctionNameG,FunctionGroupL,FunctionGroupM,FunctionGroupS  ASC) AS funID " +
      "FROM back_office_function_list WHERE FunctionState = 1 AND " +
      sql_where.join(" AND ")
    const args = []
    //console.log('-getDefault_joinAuthorityFuncs_v3 getAuthData_byUpUser -', sql);
    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getAuthData_byUpUser] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/*configDao.getExChangeRate = function (cb) {
    var sql = "SELECT Id,Currency,ExCurrency,CryDef,DATE_FORMAT(EnableTime,'%Y-%m-%d %H:%i:%s') AS EnableTime  FROM currency_exchange_rate WHERE 1";
    var args = [];
    db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
        cb(null, r_code, r_data);
    });
}*/

configDao.getGameDefaultDenom = function(cb) {
  try {
    const sql =
      "SELECT df.Currency AS currency ,df.Denom AS value FROM game_default_currency_denom df INNER JOIN currency c USING(Currency) "
    const args = []
    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getGameDefaultDenom] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// ----------------

configDao.getSystemInfo = function(param, cb) {
  try {
    const sql_where = []

    sql_where.push(" item = '" + param.item + "'")
    const sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : "1"
    const sql = "SELECT * FROM system WHERE " + sql_where_text
    const args = []
    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      if (r_code.code != code.OK) {
        cb(null, r_code, null)
      } else {
        const content = r_data[0]["content"]
        cb(null, r_code, content)
      }
    })
  } catch (err) {
    logger.error("[configDao][getSystemInfo] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getCurrencyList = function(cb) {
  try {
    const sql =
      "SELECT content AS currency FROM system \
        WHERE item = 'main_currency' \
        UNION DISTINCT \
        SELECT DISTINCT Currency AS currency FROM currency "

    const args = []
    //var db_link = pomelo.app.get('dbclient_g_rw');
    db.act_query("dbclient_g_r", sql, [], function(r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getCurrencyList] catch err", err)
    cb(null, code.FAIL, null)
  }
}

configDao.getFunctionActionName = function(data, cb) {
  try {
    const sql_where = []
    if (typeof data.functionAction != "undefined") {
      const action_where = " FunctionAction IN ('" + data.functionAction.join("','") + "')"
      sql_where.push(action_where)
    }
    //關鍵字查詢
    if (typeof data.keyWord != "undefined" && data.keyWord != "") {
      const search_field = ["FunctionNameE", "FunctionNameG", "FunctionNameC"]
      const search_key = []
      search_field.forEach((item) => {
        search_key.push(sprintf(" %s like '%%%s%%' ", item, data.keyWord))
      })
      const keyWord_where = sprintf("( %s )", search_key.join(" OR "))
      sql_where.push(keyWord_where)
    }
    sql_where.push(" FunctionState = 1 ")

    const sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : " 1 "

    const sql =
      "SELECT FunctionAction AS functionAction,FunctionNameE AS nameE,FunctionNameG AS nameG,FunctionNameC AS nameC \
                FROM back_office_function_list WHERE " +
      sql_where_text +
      " ORDER BY FunctionNameG ASC"

    const args = []
    //var db_link = pomelo.app.get('dbclient_g_rw');
    db.act_query("dbclient_g_r", sql, [], function(r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[configDao][getFunctionActionName] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//-----------------------
//取系統設定
configDao.getSystemSetting = function(data, cb) {
  try {
    const sql_where = []
    if (typeof data.item != "undefined") sql_where.push("item ='" + data.item + "' ")
    const sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : " 1 "
    const sql = " SELECT item,content FROM system WHERE " + sql_where_text
    const args = []

    db.act_query("dbclient_g_r", sql, args, function(r_code, r_data) {
      if (r_code.code !== code.OK || r_data.length == 0) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data[0]["content"])
      }
    })
  } catch (err) {
    logger.error("[configDao][getSystemSetting] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/**
 * 取得所有 game.system 的參數
 * @returns {Promise<{}>}
 */
configDao.findAllSystemParams = async function() {
  //組 SQL
  const sql = " SELECT item,content FROM system;"
  const args = []

  //查詢
  const dbResult = await dbUtils.query(dbUtils.RESOURECE.GAME_MASTER, sql, args)
  if (!dbResult || dbResult[0].length === 0) {
    return {}
  }

  //整理資料
  const params = {}
  const rows = dbResult[0]
  for (let i = 0; i < rows.length; i++) {
    const item = rows[i].item
    params[item] = rows[i].content
  }
  return params
}
