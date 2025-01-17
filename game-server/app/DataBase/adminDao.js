var logger = require("pomelo-logger").getLogger("adminDao", __filename)
var utils = require("../util/utils")
var code = require("../util/code")
var conf = require("../../config/js/conf")
var db = require("../util/DB")
var pomelo = require("pomelo")
var sprintf = require("sprintf-js").sprintf
var timezone = require("../util/timezone")
var adminDao = module.exports

adminDao.createOperator = function (data, cb) {
  try {
    var sql =
      "INSERT INTO admin (AdminId, `UserName`,`Passwd`,`NickName`,`AuthorityTemplateId`, `State`,`Email`) VALUES (?,?,?,?,?,?,?)"
    var args = [data.cid, data.name, data.password, data.nickName, data.authorityId, data.state, data.email]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, data.cid)
      }
    })
    // redisCache.insertOrUpdateDataAndDelCache('dbclient_g_rw', 'admin', data.name, sql, args, redisCacheCode.SELECT.adminDao, redisCacheCode.TTL.fiveMin, cb);
  } catch (err) {
    logger.error("[adminDao][createOperator] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.checkOperatorExist = function (data, cb) {
  try {
    var sql = "SELECT COUNT(*) AS count FROM admin WHERE UserName = ?"
    var args = [data.name]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code)
      } else {
        if (r_data && r_data[0].count > 0) {
          cb(null, {
            code: code.DB.DATA_DUPLICATE,
            msg: "",
          })
        } else {
          cb(null, {
            code: code.OK,
          })
        }
      }
    })
  } catch (err) {
    logger.error("[adminDao][checkOperatorExist] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.getOperator = function (data, cb) {
  try {
    var sql =
      "SELECT admin.`AdminId`,admin.`UserName`,admin.`Passwd`,admin.`NickName`,admin.`AuthorityTemplateID`,admin.`State`, admin.FirstLogin,admin.IsOnline , otp_auth.OTPCode, otp_auth.State AS OTPState" +
      "  FROM admin " +
      "  INNER JOIN otp_auth ON (admin.AdminId = otp_auth.Cid AND otp_auth.IsAdmin=1)" +
      "  WHERE BINARY UserName = ?"

    var args = [data.name]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data.length > 0) {
          cb(null, r_code, r_data[0])
        } else {
          cb(
            null,
            {
              code: code.DB.DATA_EMPTY,
            },
            null
          )
        }
      }
    })
    // redisCache.getCacheOrDoActQuery('dbclient_g_rw', 'admin', data.name, sql, args, redisCacheCode.SELECT.adminDao, redisCacheCode.TTL.fiveMin, cb);
  } catch (err) {
    logger.error("[adminDao][getOperator] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// adminDao.getOperator_byId = function (cid, cb) {

//     var sql = 'SELECT admin.`AdminId`,admin.`UserName`,admin.`Passwd`,admin.`NickName`,admin.`AuthorityTemplateID`,admin.`State`, admin.FirstLogin,admin.IsOnline , otp_auth.OTPCode' +
//         '  FROM admin ' +
//         '  INNER JOIN otp_auth ON (admin.AdminId = otp_auth.Cid AND otp_auth.IsAdmin=1)' +
//         '  WHERE BINARY AdminId = ?';

//     var args = [cid];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             if (r_data.length > 0) {
//                 cb(null, r_code, r_data[0]);
//             } else {
//                 cb(null, {
//                     code: code.DB.DATA_EMPTY
//                 }, null);
//             }
//         }
//     });
//     // redisCache.getCacheOrDoActQuery('dbclient_g_rw', 'admin', cid, sql, args, redisCacheCode.SELECT.adminDao, redisCacheCode.TTL.fiveMin, cb);
// };

adminDao.getList_AuthorityTemps = function (data, cb) {
  try {
    var sql_where = []
    var sql_where_text = ""
    // var typeId = data.typeId.split(',');
    if (typeof data.typeId != "undefined") {
      sql_where.push(" A.AuthorityType IN (" + data.typeId + ")")
    }

    if (typeof data.typeName != "undefined") {
      sql_where.push(' A.AuthorityTemplateDesc LIKE "%' + data.typeName + '%" ')
    }

    //關鍵字
    if (typeof data.key != "undefined" && data.key != "") {
      sql_where.push(
        '( A.AuthorityTemplateDesc LIKE "%' + data.key + '%"  OR A.AuthorityTemplateNote LIKE "%' + data.key + '%" )'
      )
    }
    //查詢時間
    if (typeof data.start_date != "undefined" && data.start_date != "") {
      sql_where.push(" A.AddDate >= '" + data.start_date + "' ")
    }
    if (typeof data.end_date != "undefined" && data.end_date != "") {
      sql_where.push(" A.AddDate <= '" + data.end_date + "' ")
    }
    //ha
    if (data.cid != "-1") {
      sql_where.push(' A.cid = "' + data.cid + '" ')
    }

    if (sql_where.length > 0) {
      sql_where_text = " WHERE " + sql_where.join(" AND ")
    }
    var sortKey = "modifyDate"
    var sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        id: "id",
        owner: "owner",
        authdesc: "authDesc",
        authnote: "authNote",
        modifydate: "modifyDate",
        modifyaccount: "modifyAccount",
        type: "type",
        usernums: "userNums",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)

    var sql = sprintf(
      'SELECT SQL_CALC_FOUND_ROWS * FROM (SELECT A.AuthorityTemplateID AS id,\
            A.Cid, IFNULL (C.UserName,"-") AS owner,   \
            A.AuthorityTemplateDesc AS authDesc, A.AuthorityTemplateNote AS authNote, \
            IF(Date_Format(A.AddDate,"%%Y-%%m-%%d %%H:%%i:%%s")="0000-00-00 00:00:00","",Date_Format(A.AddDate,"%%Y-%%m-%%d %%H:%%i:%%s")) AS addDate, \
            IF(Date_Format(A.ModifyDate,"%%Y-%%m-%%d %%H:%%i:%%s")="0000-00-00 00:00:00","",Date_Format(A.ModifyDate,"%%Y-%%m-%%d %%H:%%i:%%s")) AS modifyDate, \
            CASE  \
            WHEN (A.Cid >=0  && D.AdminId = A.ModifyId ) THEN D.UserName \
            WHEN  (A.Cid >=0  && E.Cid = A.ModifyId ) THEN E.UserName \
            ELSE "-" \
            END  AS modifyAccount , \
            CASE B.AuthorityTypeName \
            WHEN "AD" THEN (SELECT count(AdminId) FROM admin WHERE AuthorityTemplateID = A.AuthorityTemplateID )   \
            ELSE (SELECT count(Cid) FROM customer WHERE AuthorityTemplateID = A.AuthorityTemplateID )  \
            END userNums, \
            CASE B.AuthorityTypeName    \
            WHEN "AD" THEN "Operator"    \
            WHEN "HA" THEN "Hall"    \
            WHEN "AG" THEN "Agent" \
            WHEN "SUB" THEN "SubUser"    \
            END AS type   \
            FROM game.back_office_authority A    \
            INNER JOIN game.back_office_authority_type B ON  (A.AuthorityType = B.AuthorityType)    \
            LEFT JOIN customer C ON(C.Cid=A.Cid)    \
            LEFT JOIN admin D ON(D.AdminId=A.ModifyId AND D.AdminId>=0 )    \
            LEFT JOIN customer E ON(E.Cid=A.ModifyId AND E.Cid>=0)    \
            %s    \
            ) t  %s LIMIT %s,%s',
      sql_where_text,
      order_by_text,
      (data.curPage - 1) * data.pageCount,
      data.pageCount
    )
    //console.log('-sql-', sql);
    var args = []
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
    logger.error("[adminDao][getList_AuthorityTemps] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// adminDao.getAuthorityFuncs = function (cb) {

//     var sql = 'SELECT `FunctionID`,`FunctionGroupL`,`FunctionGroupM`,' +
//         '`FunctionGroupS`,`FunctionAction`, `FunctionNameE`, `FunctionNameC`, ' +
//         '`FunctionNameG`, `FunctionPath`,`FunctionDesc`,`FunctionState` FROM back_office_function_list ORDER BY  FunctionID ASC';

//     var args = [];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// };

adminDao.getCurrenies = function (cb) {
  try {
    var sql = "SELECT DISTINCT Currency AS currency FROM currency"
    var args = []
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[adminDao][getCurrenies] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// adminDao.getLevel = function (cb) {

//     var sql = 'SELECT `AuthorityType`,`AuthorityTypeName` FROM back_office_authority_type ORDER BY AuthorityType ASC';
//     var args = [];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });

// };

adminDao.getOps = function (data, cb) {
  try {
    var sql_where = []
    var sql_where_text = " 1 "

    if (typeof data.userName != "undefined" && data.userName != "") {
      sql_where.push(" admin.UserName like '%" + data.userName + "%' ")
    }
    if (typeof data.nickName != "undefined" && data.nickName != "") {
      sql_where.push(" admin.NickName like '%" + data.nickName + "%' ")
    }

    if (
      typeof data.start_date != "undefined" &&
      data.start_date != "" &&
      typeof data.end_date != "undefined" &&
      data.end_date != ""
    ) {
      sql_where.push(" (admin.AddDate BETWEEN '" + data.start_date + "' AND '" + data.end_date + "' )  ")
    }

    if (sql_where.length > 0) sql_where_text += " AND " + sql_where.join(" AND ")

    var sortKey = "admin.AdminId"
    var sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        adminid: "admin.AdminId",
        username: "admin.UserName",
        nickname: "admin.NickName",
        authoritytemplate: "auth.AuthorityTemplateDesc",
        state: "admin.State",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && [0, 1].indexOf(data.sortType) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)

    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS admin.AdminId,admin.UserName,admin.Passwd,admin.NickName,admin.AuthorityTemplateID,admin.State,otp_auth.OTPCode,admin.Email,auth.AuthorityTemplateDesc AS AuthorityTemplate,otp_auth.State AS OTPState " +
        " FROM admin " +
        "   LEFT JOIN otp_auth ON( admin.AdminId = otp_auth.Cid AND otp_auth.IsAdmin = 1 ) " +
        "   LEFT JOIN  back_office_authority auth ON(auth.AuthorityTemplateID = admin.AuthorityTemplateID ) " +
        "  WHERE %s %s LIMIT %s,%s ",
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
    logger.error("[adminDao][getOps] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.modifyOperator = function (data, cb) {
  try {
    var data_user = data.usr

    var sql_where = []
    var sql_update = []

    if (typeof data_user.name != "undefined") {
      sql_where.push("UserName =" + data_user.name)
    }

    if (typeof data_user.cid != "undefined" && data_user.cid != "") {
      sql_where.push('AdminId ="' + data_user.cid + '"')
    }

    if (typeof data_user.password != "undefined" && data_user.password != "") {
      sql_update.push('Passwd ="' + data_user.password + '"')
    }

    if (typeof data_user.nickName != "undefined" && data_user.nickName != "") {
      sql_update.push('NickName ="' + data_user.nickName + '"')
    }

    if (typeof data_user.authorityId != "undefined") {
      sql_update.push("AuthorityTemplateID =" + data_user.authorityId)
    }

    if (typeof data_user.state != "undefined") {
      sql_update.push('State ="' + data_user.state + '"')
    }

    if (typeof data_user.email != "undefined") {
      sql_update.push('Email ="' + data_user.email + '"')
    }

    if (sql_update.length === 0 || sql_where.length === 0) {
      cb(null, {
        code: code.DB.PARA_FAIL,
        msg: "",
      })
    }

    var sql = "UPDATE admin SET " + sql_update.join(",") + " WHERE " + sql_where.join(" AND ")
    var args = []

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[adminDao][modifyOperator] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.checkOperatorExist_byCid = function (data, cb) {
  try {
    var sql = "SELECT COUNT(*) AS count FROM admin WHERE AdminId = ?"
    var args = [data.cid]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code)
      } else {
        if (r_data && r_data[0]["count"] > 0) {
          cb(null, {
            code: code.OK,
          })
        } else {
          cb(null, {
            code: code.DB.DATA_EMPTY,
          })
        }
      }
    })
  } catch (err) {
    logger.error("[adminDao][checkOperatorExist_byCid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.checkUsrMailExist = function (data, cb) {
  try {
    var sql_where = []
    if (typeof data.userId != "undefined") {
      sql_where.push(" AdminId != '" + data.userId + "' ")
    }

    var sql_where_text = sql_where.length > 0 ? " AND " + sql_where.join(" AND ") : ""
    var sql = "SELECT COUNT(*) AS count FROM admin WHERE Email = ? " + sql_where_text
    var args = [data.email]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code)
      } else {
        if (r_data && r_data[0]["count"] > 0) {
          cb(null, {
            code: code.DB.DATA_DUPLICATE,
            msg: "User Mail Duplicate",
          })
        } else {
          cb(null, {
            code: code.OK,
          })
        }
      }
    })
  } catch (err) {
    logger.error("[adminDao][checkUsrMailExist] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// adminDao.modifyPassword = function (data, cb) {

//     if (typeof data.cid === 'undefined' || typeof data.password_new === 'undefined' || typeof data.password_old === 'undefined') {
//         cb(null, {
//             code: code.DB.PARA_FAIL
//         }, null);
//         return;
//     }

//     if (typeof data.password_new != 'string' || typeof data.password_old != 'string') {
//         cb(null, {
//             code: code.DB.PARA_FAIL
//         }, null);
//         return;
//     }

//     if (data.password_new.length > 64 || data.password_old.length > 64) {
//         cb(null, {
//             code: code.DB.PARA_FAIL
//         }, null);
//         return;
//     }
//     var sql = 'UPDATE admin SET Passwd = ? , FirstLogin=1 WHERE AdminId = ? AND Passwd = ? ';
//     var args = [data.password_new, data.cid, data.password_old];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code);
//         } else {
//             if (r_data.affectedRows) {
//                 cb(null, {
//                     code: code.OK,
//                     msg: ""
//                 });
//             } else {
//                 cb(null, {
//                     code: code.DB.UPDATE_FAIL,
//                     msg: ""
//                 });
//             }
//         }
//     });
// };

// adminDao.modifyOpPassword = function (data, cb) {

//     if (typeof data.cid === 'undefined' || typeof data.password_new === 'undefined') {
//         cb(null, {
//             code: code.DB.PARA_FAIL
//         }, null);
//         return;
//     }

//     if (typeof data.password_new != 'string') {
//         cb(null, {
//             code: code.DB.PARA_FAIL
//         }, null);
//         return;
//     }

//     if (data.password_new.length > 64) {
//         cb(null, {
//             code: code.DB.PARA_FAIL
//         }, null);
//         return;
//     }
//     var sql = 'UPDATE admin SET Passwd = ? , FirstLogin=1 WHERE AdminId = ?  ';
//     var args = [data.password_new, data.cid];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code);
//         } else {
//             if (r_data.affectedRows) {
//                 cb(null, {
//                     code: code.OK,
//                     msg: ""
//                 });
//             } else {
//                 cb(null, {
//                     code: code.DB.UPDATE_FAIL,
//                     msg: ""
//                 });
//             }
//         }
//     });
// };

adminDao.joinAuthorityTemp = function (data, cb) {
  try {
    var now = timezone.serverTime()

    var sql =
      "INSERT INTO back_office_authority ( AuthorityTemplateDesc , AuthorityTemplateNote, AuthorityType, AuthorityJson , Cid , ModifyId , AddDate, ModifyDate) VALUES (?,?,?,?,?,?,?,?)"
    var args = [data.desc, data.note, data.type, data.funcIds, data.cid, data.modifyId, now, now]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data.insertId)
      }
    })
  } catch (err) {
    logger.error("[adminDao][joinAuthorityTemp] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/*
adminDao.getDetail_AuthorityTemp = function (data, cb) {

    var sql = 'SELECT A.AuthorityTemplateID AS id,' +
        'A.AuthorityTemplateDesc AS authDesc,' +
        'CASE B.AuthorityTypeName ' +
        'WHEN "AD" THEN "Operator" ' +
        'WHEN "HA" THEN "Hall" ' +
        'WHEN "AG" THEN "Agent" ' +
        'END AS type ' +
        'FROM game.back_office_authority A ' +
        'INNER JOIN game.back_office_authority_type B ON  A.AuthorityType = B.AuthorityType ' + sql_where +
        'ORDER BY A.AuthorityType LIMIT ?,?';

    var args = [];
    pomelo.app.get('dbclient_g_rw').getConnection(function (err, connection) {
        if (err) {
            cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
            return;
        }
        connection.query(sql, args, function (err, res) {
            connection.release();
            if (err) {
                cb(null, { code: code.CONFIG.LOAD_CONFIG_AUTHORITY_FAIL, msg: err.stack }, null);
            } else {

                if (res) {
                    var authoritys = res;
                    cb(null, { code: code.OK, msg: "" }, authoritys);
                } else {
                    cb(null, { code: code.CONFIG.LOAD_CONFIG_AUTHORITY_EMPTY, msg: "Authority Empty" }, null);
                }
            }
        });
    });   
};
*/
//範本資料存在?
adminDao.checkAuthorityExist_byTmpId = function (tmpId, cb) {
  try {
    var sql = "SELECT COUNT(*) AS count FROM back_office_authority WHERE AuthorityTemplateID = ?"
    var args = [tmpId]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code)
      } else {
        if (r_data && r_data[0].count > 0) {
          cb(null, {
            code: code.OK,
          })
        } else {
          cb(null, {
            code: code.DB.DATA_EMPTY,
          })
        }
      }
    })
  } catch (err) {
    logger.error("[adminDao][checkAuthorityExist_byTmpId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//修改權限範本
adminDao.modifyAuthorityTmp = function (data, cb) {
  try {
    var sql_update = []
    if (typeof data.funcIds != "undefined") {
      sql_update.push("AuthorityJson='" + data.funcIds + "'")
    }
    if (typeof data.note != "undefined") {
      sql_update.push("AuthorityTemplateNote='" + data.note + "'")
    }
    if (typeof data.modifyId != "undefined") {
      sql_update.push("ModifyId='" + data.modifyId + "'")
    }
    var sql_update_text = ""
    if (sql_update.length == 0) {
      cb(null, {
        code: code.OK,
      })
      return
    } else {
      sql_update_text = sql_update.join(",")
    }
    var now = timezone.serverTime()
    var sql = "UPDATE back_office_authority SET " + sql_update_text + ", ModifyDate=? WHERE AuthorityTemplateID = ?"
    var args = [now, data.tmpId]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code)
      } else {
        if (r_data.affectedRows) {
          cb(null, {
            code: code.OK,
          })
        } else {
          cb(null, {
            code: code.DB.QUERY_FAIL,
          })
        }
      }
    })
  } catch (err) {
    logger.error("[adminDao][modifyAuthorityTmp] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.AuthorityTemplateID = function (data, cb) {
  try {
    if (data.cid == "-1") {
      cb(
        null,
        {
          code: code.OK,
          msg: null,
        },
        null
      )
      return
    }

    var sql = "SELECT AuthorityTemplateID FROM customer WHERE Cid = ?"
    var args = [data.cid]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data[0]["AuthorityTemplateID"])
      }
    })
  } catch (err) {
    logger.error("[adminDao][AuthorityTemplateID] catch err", err)
    cb(null, code.FAIL, null)
  }
}
//範本明細
adminDao.getDetail_AuthorityFunc = function (data, cb) {
  try {
    if (data.tmpId == null) {
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

    var sql =
      "SELECT A.cid, A.AuthorityTemplateID AS id,A.AuthorityTemplateDesc AS name,A.AuthorityTemplateNote AS note, A.AuthorityType AS typeId,A.AuthorityJson AS funcIds, " +
      " CASE B.AuthorityTypeName " +
      ' WHEN "AD" THEN "admin" ' +
      ' WHEN "HA" THEN "reseller" ' +
      ' WHEN "AG" THEN "agent" ' +
      ' WHEN "SUB" THEN "subuser" ' +
      " END AS type " +
      " FROM back_office_authority A " +
      " INNER JOIN game.back_office_authority_type B " +
      " WHERE A.AuthorityType=B.AuthorityType " +
      " AND A.AuthorityTemplateID = ? "
    var args = [data.tmpId]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data[0])
      }
    })
  } catch (err) {
    logger.error("[adminDao][getDetail_AuthorityFunc] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.getList_marquee = function (data, cb) {
  try {
    var sql_where = []
    var sql_where_text = ""

    if (typeof data.GameId != "undefined") {
      sql_where.push(" A.GameId IN (" + data.GameId + ") ")
    }
    if (typeof data.Content != "undefined") {
      sql_where.push(
        " ( A.Content like '%" +
          data.Content +
          "%' OR A.LangC like '%" +
          data.Content +
          "%' OR A.LangG like '%" +
          data.Content +
          "%' OR A.LangE like '%" +
          data.Content +
          "%') "
      )
    }
    if (typeof data.StartTime != "undefined" && typeof data.StopTime != "undefined") {
      var StartTime = data.StartTime
      var StopTime = data.StopTime
      sql_where.push(
        " ( (A.StartTime >= '" +
          StartTime +
          "' AND A.StartTime <= '" +
          StopTime +
          "') OR ( A.StopTime >= '" +
          StartTime +
          "' AND A.StopTime <= '" +
          StopTime +
          "') ) "
      )
    }
    if (typeof data.Priority != "undefined") {
      sql_where.push(" A.Priority IN (" + data.Priority + ") ")
    }
    if (data.cid != -1) {
      sql_where.push(" A.Cid = '" + data.cid + "'  ")
    }
    if (sql_where.length > 0) sql_where_text = " WHERE " + sql_where.join(" AND ")

    var sortKey = "modifyDate"
    var sortType = conf.SORT_TYPE[0]
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        id: " Id",
        locationid: "GameId",
        title: "Content",
        priorityid: "Priority",
        owner: "owner",
        modifyaccount: "modifyAccount",
        modifydate: "modifyDate",
        endtimeformat: "StopTime",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }

    var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)

    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS t.* FROM (SELECT A.Id,A.GameId ,A.Content,A.LangC,A.LangG,A.LangE,DATE_FORMAT(A.StartTime,'%%Y-%%m-%%d %%H:%%i:%%s') AS StartTime,\
                DATE_FORMAT(A.StopTime,'%%Y-%%m-%%d %%H:%%i:%%s') AS StopTime,A.Priority,\
                A.Cid, A.DescriptionE, A.DescriptionG, A.DescriptionC, IFNULL (C.UserName,'-') AS owner, \
                IF(Date_Format(A.ModifyDate,'%%Y-%%m-%%d %%H:%%i:%%s')='0000-00-00 00:00:00','',Date_Format(A.ModifyDate,'%%Y-%%m-%%d %%H:%%i:%%s')) AS modifyDate, \
                CASE WHEN  C.Cid = A.ModifyId THEN E.UserName WHEN  D.AdminId = A.ModifyId THEN D.UserName ELSE '-' END AS modifyAccount \
                FROM game.marquee A \
                LEFT JOIN customer C ON(C.Cid=A.Cid AND C.Cid !='0'  ) \
                LEFT JOIN admin D ON(D.AdminId=A.ModifyId AND D.AdminId !='0') \
                LEFT JOIN customer E ON(E.Cid=A.ModifyId) \
                %s \
                )t  %s limit %s,%s",
      sql_where_text,
      order_by_text,
      (data.curPage - 1) * data.pageCount,
      data.pageCount
    )

    var args = []
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
    logger.error("[adminDao][getList_marquee] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.createMarquee = function (data, cb) {
  try {
    var now = timezone.serverTime()
    var sql =
      "INSERT INTO marquee (Cid, GameId, Content, LangC, LangG, LangE , StartTime , StopTime , Priority, DescriptionE, DescriptionG, DescriptionC,  ModifyId, ModifyDate) VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    var args = [
      data.cid,
      data.locationId,
      data.title,
      data.langC,
      data.langG,
      data.langE,
      data.startTime,
      data.endTime,
      data.priorityId,
      data.descriptionE,
      data.descriptionG,
      data.descriptionC,
      data.modifyId,
      now,
    ]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data.insertId)
      }
    })
  } catch (err) {
    logger.error("[adminDao][createMarquee] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.checkMarquee_byId = function (data, cb) {
  try {
    var sql = "SELECT COUNT(*) AS count FROM marquee WHERE Id = ?"
    var args = [data.id]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data && r_data[0].count > 0) {
          cb(
            null,
            {
              code: code.OK,
            },
            r_data[0].count
          )
        } else {
          cb(
            null,
            {
              code: code.DB.DATA_EMPTY,
            },
            null
          )
        }
      }
    })
  } catch (err) {
    logger.error("[adminDao][checkMarquee_byId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.getMarquee_byId = function (data, cb) {
  try {
    var res_data = []
    var sql = "SELECT * FROM marquee WHERE Id = ?"
    var args = [data.id]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var info = {
          marquee: r_data,
        }
        res_data.push(info)
        cb(
          null,
          {
            code: code.OK,
          },
          res_data
        )
      }
    })
  } catch (err) {
    logger.error("[adminDao][getMarquee_byId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.modifyMarquee = function (data, cb) {
  try {
    var now = timezone.serverTime()
    var sql_where = []
    var sql_update = []

    if (typeof data.id != "undefined") {
      sql_where.push(" Id=" + data.id)
    }
    if (typeof data.locationId != "undefined") {
      sql_update.push(" GameId = '" + data.locationId + "'")
    }

    if (typeof data.title != "undefined") {
      sql_update.push(" Content = '" + data.title + "'")
    }

    if (typeof data.langC != "undefined") {
      sql_update.push(" LangC = '" + data.langC + "'")
    }

    if (typeof data.langG != "undefined") {
      sql_update.push(" LangG = '" + data.langG + "'")
    }

    if (typeof data.langE != "undefined") {
      sql_update.push(" LangE = '" + data.langE + "'")
    }

    if (typeof data.startTime != "undefined") {
      sql_update.push(" StartTime = '" + data.startTime + "'")
    }

    if (typeof data.endTime != "undefined") {
      sql_update.push(" StopTime = '" + data.endTime + "'")
    }

    if (typeof data.priorityId != "undefined") {
      sql_update.push(" Priority = '" + data.priorityId + "'")
    }

    if (typeof data.descriptionE != "undefined") {
      sql_update.push(" DescriptionE = '" + data.descriptionE + "'")
    }

    if (typeof data.descriptionG != "undefined") {
      sql_update.push(" DescriptionG = '" + data.descriptionG + "'")
    }

    if (typeof data.descriptionC != "undefined") {
      sql_update.push(" DescriptionC = '" + data.descriptionC + "'")
    }

    sql_update.push(" ModifyId = '" + data.modifyId + "' ")
    sql_update.push(" ModifyDate ='" + now + "' ")

    if (sql_where.length == 0 || sql_update.length == 0) {
      return connection.rollback(function () {
        cb(null, {
          code: code.DB.QUERY_FAIL,
          msg: "",
        })
      })
    }

    var sql = "UPDATE marquee SET " + sql_update.join(",") + " WHERE " + sql_where.join(" AND ")
    var args = []

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[adminDao][modifyMarquee] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.checkExchangeRate_duplicateTime = function (data, cb) {
  try {
    var sql =
      "SELECT COUNT(*) AS count FROM currency_exchange_rate WHERE (Currency = ? AND ExCurrency = ? AND EnableTime = ?) "
    var args = [data.currency, data.exCurrency, data.enableTime]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data[0]["count"])
      }
    })
  } catch (err) {
    logger.error("[adminDao][checkExchangeRate_duplicateTime] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.createExchangeRate = function (data, cb) {
  try {
    var sql =
      "INSERT INTO currency_exchange_rate (Currency,ExCurrency,CryDef,EnableTime) " +
      "      SELECT * " +
      "      FROM ( SELECT ?,?,?,?) AS tmp " +
      "      WHERE NOT EXISTS ( " +
      "        SELECT Currency,CryDef,EnableTime " +
      "         FROM currency_exchange_rate " +
      "        WHERE (Currency = ? AND ExCurrency = ? AND CryDef = ? AND EnableTime = ?) " +
      "       ) " +
      "      LIMIT 1 "
    var args = [
      data.currency,
      data.exCurrency,
      data.rate,
      data.enableTime,
      data.currency,
      data.exCurrency,
      data.rate,
      data.enableTime,
    ]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data.insertId)
      }
    })
  } catch (err) {
    logger.error("[adminDao][createExchangeRate] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// adminDao.checkCurrencyExist = function (data, cb) {

//     var sql = "SELECT count(Currency) as count FROM currency WHERE Currency=? ";
//     var args = [data.currency];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             if (r_data && r_data[0].count > 0) {
//                 cb(null, {
//                     code: code.DB.DATA_DUPLICATE,
//                     msg: " Duplicate"
//                 });
//             } else {
//                 cb(null, {
//                     code: code.OK
//                 });
//             }
//         }
//     });

// };

adminDao.createCurrency = function (data, cb) {
  try {
    if (typeof data.desc === "undefined") data["desc"] = ""
    var sql =
      " INSERT INTO currency (Currency,`Desc`) " +
      "       SELECT * FROM (SELECT ?,?) AS tmp " +
      "       WHERE NOT EXISTS ( " +
      "           SELECT Currency FROM currency WHERE Currency = ? " +
      "       ) LIMIT 1"

    var args = [data.currency, data.desc, data.currency]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[adminDao][createCurrency] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.modifyCurrency = function (data, cb) {
  var sql = "UPDATE currency SET Currency=?, `Desc`=? WHERE Currency = ?"
  var args = [data.currency, data.desc, data.origin_currency]

  db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
    cb(null, r_code)
  })
}

// adminDao.get_currency_log = function (data, cb) {

//     var res_data = [];
//     var sql = "SELECT * FROM currency WHERE Currency =? ";
//     var args = [data.currency];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             var info = {
//                 currency: r_data
//             };
//             res_data.push(info);
//             cb(null, {
//                 code: code.OK
//             }, res_data);
//         }
//     });
// };

adminDao.getExchangeRate_byId = function (id, cb) {
  try {
    var res_data = []
    var sql = "SELECT * FROM currency_exchange_rate WHERE Id = ? "
    var args = [id]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var info = {
          currency_exchange_rate: r_data,
        }
        res_data.push(info)
        cb(
          null,
          {
            code: code.OK,
          },
          res_data
        )
      }
    })
  } catch (err) {
    logger.error("[adminDao][getExchangeRate_byId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.getList_currentRate = function (data, cb) {
  try {
    /*
        var sql = "SELECT C.Currency, R.Id,R.CryDef,DATE_FORMAT(R.EnableTime,'%Y-%m-%d %H:%i:%s') AS EnableTime" +
            "      FROM currency C " +
            "      INNER JOIN currency_exchange_rate R " +
            "      WHERE (C.Currency,R.EnableTime) IN ( " +
            "        SELECT Currency, MAX(EnableTime) AS EnableTime " +
            "        FROM currency_exchange_rate " +
            "        WHERE  EnableTime <= NOW() " +
            "        GROUP BY Currency ); ";
            */

    let sortType = "DESC"
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }

    let sortKey = "EnableTime"
    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      var sort_info = {
        Currency: "Currency",
        ExCurrency: "ExCurrency",
        CryDef: "CryDef",
        EnableTime: "EnableTime",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }

    var now = timezone.serverTime()
    var sql = sprintf(
      "SELECT Currency,ExCurrency, CryDef, DATE_FORMAT(EnableTime,'%%Y-%%m-%%d %%H:%%i:%%s') AS EnableTime  \
            FROM currency_exchange_rate \
            WHERE (Currency, ExCurrency,EnableTime) IN ( \
            SELECT R.Currency, R.ExCurrency, MAX(R.EnableTime) AS EnableTime \
            FROM currency_exchange_rate R \
            INNER JOIN currency C ON (C.Currency = R.Currency ) \
            INNER JOIN currency exC ON (exC.Currency = R.Currency ) \
            WHERE R.EnableTime <= '%s' \
            GROUP BY R.Currency, R.ExCurrency\
             ) ORDER BY %s %s ",
      now,
      sortKey,
      sortType
    )
    var args = []
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[adminDao][getList_currentRate] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/*
狀態 state-
 0:未執行
 1:執行中
 2:已執行
*/
adminDao.getList_exchangeRate = function (data, cb) {
  try {
    var sql_where = new Array()
    var currency = new Array()
    var enable_Time_limit = ""

    if (typeof data.state != "undefined" && data.state.length > 0) {
      sql_where.push(" x.State IN('" + data.state.join("','") + "') ")
    }

    if (typeof data.currency != "undefined" && data.currency != "") {
      sql_where.push(" x.currency  ='" + data.currency + "' ")
    }

    if (typeof data.ex_currency != "undefined" && data.ex_currency != "") {
      sql_where.push(" x.ex_currency  ='" + data.ex_currency + "' ")
    }

    if (
      typeof data.start_date != "undefined" &&
      data.start_date != "" &&
      typeof data.end_date != "undefined" &&
      data.end_date != ""
    ) {
      sql_where.push(" (x.startTime BETWEEN '" + data.start_date + "' AND '" + data.end_date + "' ) ")
    } else {
      enable_Time_limit = " AND startTime > date_sub(curdate(),interval 5 month) "
    }

    if (sql_where.length == 0) {
      sql_where.push(" 1 ")
    }

    var sql_limit =
      data.isPage === true
        ? " LIMIT " + (data.page - 1) * data.pageCount + "," + data.pageCount
        : " LIMIT  " + data.index + "," + data.pageCount //限制筆數

    var now = timezone.serverTime()
    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS x.Id, x.currency, x.ex_currency, x.CryDef, x.startTime, IFNULL(x.endTime,'-') AS endTime, x.State FROM " +
        " (SELECT R.Id, R.Currency AS currency, R.ExCurrency AS ex_currency, R.CryDef, DATE_FORMAT(R.EnableTime,'%%Y-%%m-%%d %%H:00') AS startTime, " +
        "     (SELECT DATE_FORMAT(A.EnableTime,'%%Y-%%m-%%d %%H:00') " +
        "     FROM currency_exchange_rate A " +
        "     WHERE R.EnableTime < A.EnableTime AND Currency = R.Currency AND ExCurrency= R.ExCurrency " +
        "     ORDER BY A.EnableTime ASC " +
        "     LIMIT 0,1) AS endTime,  " +
        "    CASE WHEN R.EnableTime > '%s' THEN '0' WHEN R.EnableTime < '%s' AND  (R.EnableTime) IN ( " +
        "    SELECT MAX(B.EnableTime) AS EnableTime " +
        "    FROM currency_exchange_rate B " +
        "    WHERE B.EnableTime <= '%s'  AND Currency = R.Currency AND ExCurrency = R.ExCurrency " +
        "    ) THEN '1' ELSE '2' END AS State " +
        "    FROM currency_exchange_rate R " +
        "    WHERE  1 " +
        "    HAVING State IN(0,1) OR (State=2 %s ) " +
        "    ORDER BY  R.Currency,R.EnableTime DESC) x " +
        "  WHERE %s " +
        "   %s ",
      now,
      now,
      now,
      enable_Time_limit,
      sql_where.join(" AND "),
      sql_limit
    )

    var args = []

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
    logger.error("[adminDao][getList_exchangeRate] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.checkState_modifyExchangeRate = function (data, cb) {
  try {
    var now = timezone.serverTime()
    var sql =
      "SELECT COUNT(*) AS count FROM currency_exchange_rate WHERE Id = ? AND EnableTime > ? + INTERVAL 10 MINUTE  "
    var args = [data.id, now]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data && r_data[0].count > 0) {
          cb(null, {
            code: code.OK,
          })
        } else {
          cb(null, {
            code: code.DB.DATA_EMPTY,
          })
        }
      }
    })
  } catch (err) {
    logger.error("[adminDao][checkState_modifyExchangeRate] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.modifyExchangeRate = function (data, cb) {
  try {
    var sql_where = []
    var sql_update = []

    if (typeof data.id != "undefined") {
      sql_where.push(" Id=" + data.id)
    }
    // if (typeof data.exCurrency != 'undefined') {
    //     sql_where.push(" ExCurrency='" + data.exCurrency + "'");
    // }

    if (typeof data.enableTime != "undefined") {
      sql_update.push(" EnableTime = '" + data.enableTime + "'")
    }

    if (typeof data.rate != "undefined") {
      sql_update.push(" CryDef = '" + data.rate + "'")
    }

    if (sql_where.length == 0 || sql_update.length == 0) {
      return connection.rollback(function () {
        cb(null, {
          code: code.DB.QUERY_FAIL,
          msg: "",
        })
      })
    }

    var sql = "UPDATE currency_exchange_rate SET " + sql_update.join(",") + " WHERE " + sql_where.join(" AND ")
    var args = []

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data.affectedRows) {
          cb(null, {
            code: code.OK,
          })
        } else {
          cb(null, {
            code: code.DB.QUERY_FAIL,
          })
        }
      }
    })
  } catch (err) {
    logger.error("[adminDao][modifyExchangeRate] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// adminDao.checkIsExist_Admin = function (data, cb) {
//     if (typeof data == 'undefined' || typeof data.userName == 'undefined' || typeof data.mail == 'undefined') {
//         cb(null, {
//             code: code.DB.DATA_EMPTY,
//             msg: null
//         }, null);
//         return;
//     }
//     var sql = "SELECT AdminId FROM admin WHERE UserName = ? AND Email=?  ";
//     var args = [data.userName, data.mail];
//
//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             if (r_data && r_data.length == 1) {
//                 cb(null, {
//                     code: code.OK
//                 }, {
//                     AdminId: r_data[0]['AdminId']
//                 });
//             } else {
//                 cb(null, {
//                     code: code.DB.DATA_EMPTY
//                 }, null);
//             }
//         }
//     });
// }

// adminDao.setNewPassword = function (data, cb) {

//     if (typeof data.password == 'undefined' || typeof data.firstLogin == 'undefined' || typeof data.cid == 'undefined') {
//         cb(null, {
//             code: code.DB.DATA_EMPTY,
//             msg: null
//         }, null);
//         return;
//     }
//     var sql = 'UPDATE admin SET  Passwd = ? , FirstLogin = ?  WHERE AdminId =? ;';
//     var args = [data.password, data.firstLogin, data.cid];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code);
//         } else {
//             if (r_data.affectedRows) {
//                 cb(null, {
//                     code: code.OK
//                 });
//             } else {
//                 cb(null, {
//                     code: code.DB.QUERY_FAIL
//                 });
//             }
//         }
//     });
// }

adminDao.get_admin_byId = function (data, cb) {
  try {
    var res_data = []
    if (typeof data == "undefined") {
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
    var sql = "SELECT * FROM admin WHERE AdminId = ? "
    var args = [data.AdminId]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var info = {
          admin: r_data,
        }
        res_data.push(info)
        cb(
          null,
          {
            code: code.OK,
          },
          res_data
        )
      }
    })
    // redisCache.getCacheOrDoActQuery('dbclient_g_rw', 'admin', data.AdminId, sql, args, redisCacheCode.SELECT.adminDao, redisCacheCode.TTL.fiveMin, cb);
  } catch (err) {
    logger.error("[adminDao][get_admin_byId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.getPassword_Cid = function (cid, cb) {
  try {
    var res_data = []
    if (typeof cid == "undefined") {
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
    var sql = "SELECT * FROM admin WHERE AdminId = ?"
    var args = [cid]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var info = {
          admin: r_data,
        }
        res_data.push(info)
        cb(
          null,
          {
            code: code.OK,
          },
          res_data
        )
      }
    })
    // redisCache.getCacheOrDoActQuery('dbclient_g_rw', 'admin', cid, sql, args, redisCacheCode.SELECT.adminDao, redisCacheCode.TTL.fiveMin, cb);
  } catch (err) {
    logger.error("[adminDao][getPassword_Cid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.getAuthorityTemp_byId = function (tmpId, cb) {
  try {
    var res_data = []
    if (typeof tmpId == "undefined") {
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
    var sql = "SELECT * FROM back_office_authority WHERE AuthorityTemplateID = ?"
    var args = [tmpId]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var info = {
          back_office_authority: r_data,
        }
        res_data.push(info)
        cb(
          null,
          {
            code: code.OK,
          },
          res_data
        )
      }
    })
  } catch (err) {
    logger.error("[adminDao][getAuthorityTemp_byId] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.setActionTime = function (userId, cb) {
  try {
    if (typeof userId == "undefined") {
      cb(null, {
        code: code.DB.PARA_FAIL,
        msg: null,
      })
      return
    }
    var now = timezone.serverTime()
    var sql = "UPDATE admin SET LastActDate = ?  WHERE AdminId =? "
    var args = [now, userId]
    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[adminDao][setActionTime] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// adminDao.getActionTime = function (userId, cb) {

//     var sql = "SELECT UNIX_TIMESTAMP(LastActDate) AS LastActDate FROM admin WHERE AdminId = ? ";
//     var args = [userId];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             cb(null, r_code, r_data[0]['LastActDate']);
//         }
//     });
// }

// adminDao.getActionTime_2 = function (userId, cb) {

//     var sql = "SELECT DATE_FORMAT(LastActDate,'%Y-%m-%d %H:%i:%s') AS LastActDate FROM admin WHERE AdminId = ? ";
//     var args = [userId];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             cb(null, r_code, r_data[0]['LastActDate']);
//         }
//     });
// }

adminDao.getUser_byUserName_temp = function (data, cb) {
  try {
    var sql =
      "SELECT *,TempPassword,DATE_FORMAT(TempEndTime,'%Y-%m-%d %H:%i:%s') AS TempEndTime,TempMod FROM admin WHERE UserName= ? "
    var args = [data.name]
    logger.info("[adminDao][getUser_byUserName_temp][sql] %s [args] %s", sql, args)
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code)
      } else {
        if (r_data.length > 0) {
          cb(
            null,
            {
              code: code.OK,
            },
            r_data[0]
          )
        } else {
          cb(
            null,
            {
              code: code.DB.DATA_EMPTY,
            },
            null
          )
        }
      }
    })
  } catch (err) {
    logger.error("[adminDao][getUser_byUserName_temp] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.checkTempUsrExist_byCid = function (data, cb) {
  try {
    var sql =
      "SELECT DATE_FORMAT(TempEndTime,'%Y-%m-%d %H:%i:%s') AS TempEndTime,TempMod  FROM admin WHERE  AdminId = ? "
    var args = [data.cid]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code)
      } else {
        if (r_data.length > 0) {
          cb(
            null,
            {
              code: code.OK,
            },
            r_data[0]
          )
        } else {
          cb(
            null,
            {
              code: code.DB.DATA_EMPTY,
            },
            null
          )
        }
      }
    })
  } catch (err) {
    logger.error("[adminDao][checkTempUsrExist_byCid] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//設定臨時密碼
adminDao.setTempPassword = function (data, cb) {
  try {
    //console.log('-setTempPassword data-', JSON.stringify(data));

    if (
      typeof data.Cid == "undefined" ||
      typeof data.TempEndTime == "undefined" ||
      typeof data.TempPassword == "undefined" ||
      typeof data.TempMod == "undefined"
    ) {
      cb(null, {
        code: code.DB.DATA_EMPTY,
        msg: null,
      })
      return
    }

    var sql = "UPDATE admin SET TempPassword=? , TempEndTime = ?, TempMod = ?  WHERE AdminId =? ;"
    var args = [data.TempPassword, data.TempEndTime, data.TempMod, data.Cid]

    //console.log('-setTempPassword-', sql, JSON.stringify(args));

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code)
      } else {
        if (r_data.affectedRows) {
          cb(null, {
            code: code.OK,
          })
        } else {
          cb(null, {
            code: code.DB.QUERY_FAIL,
          })
        }
      }
    })
  } catch (err) {
    logger.error("[adminDao][setTempPassword] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.modifyPassword_temp = function (data, cb) {
  try {
    var sql = "UPDATE admin SET Passwd =? ,TempPassword= ?, TempEndTime= ?, TempMod= ?  WHERE  AdminId = ? "
    var args = [data.password, "", "0000-00-00 00:00:00", 0, data.cid]
    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[adminDao][modifyPassword_temp] catch err", err)
    cb(null, code.FAIL, null)
  }
}

adminDao.checkIsExist_User = function (data, cb) {
  try {
    if (typeof data == "undefined" || typeof data.userName == "undefined" || typeof data.mail == "undefined") {
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
    var sql = "SELECT AdminId FROM admin WHERE UserName = ? AND Email=?  "
    var args = [data.userName, data.mail]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        if (r_data && r_data.length == 1) {
          cb(
            null,
            {
              code: code.OK,
            },
            {
              AdminId: r_data[0]["AdminId"],
            }
          )
        } else {
          cb(
            null,
            {
              code: code.DB.DATA_EMPTY,
            },
            null
          )
        }
      }
    })
  } catch (err) {
    logger.error("[adminDao][checkIsExist_User] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// adminDao.modifyAdminOnlineState = function (data, cb) {
//     if (typeof data == 'undefined' || typeof data.userId == 'undefined' || typeof data.isOnline == 'undefined') {
//         cb(null, {
//             code: code.DB.PARA_FAIL,
//             msg: null
//         }, null);
//         return;
//     }
//     var sql = "UPDATE admin SET IsOnline = ?  WHERE AdminId=?";
//     var args = [data.isOnline, data.userId];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// adminDao.getUserOnlineState = function (data, cb) {

//     var sql = "SELECT a.IsOnline,c.connect_key,DATE_FORMAT(c.actTime,'%Y-%m-%d %H:%i:%s') AS actTime \
//              FROM admin a \
//              LEFT JOIN user_connection c ON(c.userId = a.AdminId AND c.isAdmin=1) \
//              WHERE a.AdminId=? ";
//     var args = [data.userId];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// adminDao.modifyUserConnectState = function (data, cb) {

//     var sql_query = [];
//     var sql1 = sprintf(" UPDATE admin SET IsOnline = 1  WHERE AdminId = '%s' ", data.userId);
//     var sql2 = sprintf(" UPDATE user_connection SET connect_key = '%s'  WHERE userId = '%s' AND isAdmin = '%s' ", data.token, data.userId, data.isAdmin);
//     sql_query.push(sql1);
//     sql_query.push(sql2);
//     //console.log('--------------sql_query----------------', JSON.stringify(sql_query));
//     db.act_transaction('dbclient_g_rw', sql_query, function (r_code, r_data) {
//         //console.log('-modifyUserConnectState-', JSON.stringify(r_code), JSON.stringify(r_data));
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             cb(null, r_code, r_data);
//         }
//     });
// }

// adminDao.get_user_connection = function (token, cb) {
//     var sql = "SELECT connect.isAdmin,connect.userId,connect.userName,connect.connect_key,DATE_FORMAT(connect.closeTime,'%Y-%m-%d %H:%i:%s') AS closeTime,a.IsOnline  \
//                 FROM user_connection connect \
//                 LEFT JOIN admin a ON(connect.isAdmin=1 AND a.AdminId=connect.userId) \
//                 WHERE connect.connect_key=?     ";
//     var args = [token];
//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

// adminDao.close_user_connection = function (data, cb) {
//     var now = timezone.serverTime();
//     var sql_query = [];
//     var sql1 = sprintf("UPDATE user_connection SET closeTime = '%s' WHERE connect_key='%s' ",now, data.token);
//     var sql2 = sprintf("UPDATE admin SET IsOnline=0 WHERE AdminId='%s' ", data.cid);
//     sql_query.push(sql1);
//     sql_query.push(sql2);

//     db.act_transaction('dbclient_g_rw', sql_query, function (r_code, r_data) {
//         //console.log('-modifyUserConnectState-', JSON.stringify(r_code), JSON.stringify(r_data));
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             cb(null, r_code, r_data);
//         }
//     });
// }

adminDao.checkIsExist_Mail = function (data, cb) {
  try {
    var sql_where = []
    if (typeof data.cid != "undefoned" && data.cid != "") {
      sql_where.push(" (AdminId !='" + data.cid + "') ")
    }
    var sql_where_text = sql_where.length > 0 ? "AND " + sql_where.join(" AND ") : ""
    var sql = " SELECT count(*) as count FROM admin WHERE Email=? " + sql_where_text
    var args = [data.mail]

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[adminDao][checkIsExist_Mail] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 找出欲修改權限的其他使用者權限相關資料 (上線被調整時，下線權限一起調整)
adminDao.getAuthorityOthers = function (data, cb) {
  try {
    var sql_where = []
    sql_where.push(" cid IN ('" + data.modifyAuthorityIds.join("','") + "') ")
    var sql =
      "SELECT cid, AuthorityTemplateID, AuthorityJson FROM back_office_authority WHERE " + sql_where.join(" AND ")
    var args = []

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[adminDao][getAuthorityOthers] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 修改其他使用者相關權限範本 (上線被調整時，下線權限一起調整)
adminDao.updateAuthorityOthers = function (data, cb) {
  try {
    var sql_query = []
    data.forEach((item) => {
      var update_sql =
        "UPDATE back_office_authority SET AuthorityJson='" +
        item.modifyAuthorityIds +
        "', ModifyId = '" +
        item.modifyId +
        "', ModifyDate = '" +
        timezone.serverTime() +
        "' WHERE cid='" +
        item.cid +
        "' AND AuthorityTemplateID='" +
        item.templateId +
        "'"
      sql_query.push(update_sql)
    })

    db.act_transaction("dbclient_g_rw", sql_query, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[adminDao][updateAuthorityOthers] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// adminDao.getAdminIsDemo = function (data, cb) {
//     var sql_where = [];
//     var sql_userId = " AdminId IN('" + data.userId.join("','") + "') ";
//     sql_where.push(sql_userId);
//     var sql_where_text = (sql_where.length > 0) ? " 1 " : sql_where.join(" AND ");
//     var sql = "SELECT AdminId AS Cid,'-' AS IsDemo,'1' AS IsAg,State FROM admin WHERE " + sql_where_text;
//     var args = [];
//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }
// adminDao.getTTLCounts_Admin = function (cb) {
//     var sql = 'SELECT COUNT(*) AS count FROM admin';
//     var args = [];
//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             cb(null, r_code, r_data[0].count);
//         }
//     });
// };

// adminDao.getCounts_AuthorityTemps = function (data, cb) {
//     var sql_where = [];
//     var sql_where_text = "";
//     if (typeof data.typeId != 'undefined') {
//         sql_where.push(' AuthorityType IN (' + data.typeId + ')');
//     }
//     if (typeof data.typeName != 'undefined') {
//         sql_where.push(' AuthorityTemplateDesc LIKE "%' + data.typeName + '%" ');
//     }
//     //hall
//     if (data.cid != '-1') {
//         sql_where.push(' cid = "' + data.cid + '" ');
//     }
//     if (sql_where.length > 0) {
//         sql_where_text = " WHERE " + sql_where.join(' AND ');
//     }
//     var sql = 'SELECT COUNT(*) AS count FROM game.back_office_authority ' + sql_where_text;
//     var args = [];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             cb(null, r_code, r_data[0].count);
//         }
//     });
// };

// adminDao.getCounts_marquee = function (data, cb) {

//     var sql_where = [];
//     var sql_where_text = "";

//     if (typeof data.GameId != 'undefined') {
//         sql_where.push(" GameId IN (" + data.GameId + ") ");
//     }
//     if (typeof data.Content != 'undefined') {
//         sql_where.push(" ( Content like '%" + data.Content + "%' OR LangC like '%" + data.Content + "%' OR LangG like '%" + data.Content + "%' OR LangE like '%" + data.Content + "%') ");
//     }

//     if (typeof data.StartTime != 'undefined' && typeof data.StopTime != 'undefined') {
//         var StartTime = data.StartTime;
//         var StopTime = data.StopTime;
//         sql_where.push(" ( StartTime >= '" + StartTime + "' AND StopTime <= '" + StopTime + "') ");
//     }

//     if (typeof data.Priority != 'undefined') {
//         sql_where.push(" Priority IN (" + data.Priority + ") ");
//     }

//     if (data.cid != -1) {
//         sql_where.push(" Cid =" + data.cid + "  ");
//     }

//     if (sql_where.length > 0) sql_where_text = " WHERE " + sql_where.join(" AND ");

//     var sql = 'SELECT COUNT(*) AS count FROM game.marquee ' + sql_where_text;
//     var args = [];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             cb(null, r_code, r_data[0].count);
//         }
//     });
// };
// adminDao.getCounts_exchangeRate = function (data, cb) {
//     var sql_where = new Array();
//     var currency = new Array();
//     var enable_Time_limit = "";
//     if (typeof data.state != 'undefined' && data.state.length > 0) {
//         sql_where.push(" x.State IN('" + data.state.join("','") + "') ");
//     }

//     if (typeof data.currency != 'undefined' && data.currency != '') {
//         sql_where.push(" x.currency  ='" + data.currency + "' ");
//     }

//     if (typeof data.start_date != 'undefined' && data.start_date != '' && typeof data.end_date != 'undefined' && data.end_date != '') {
//         sql_where.push(" (x.startTime BETWEEN '" + data.start_date + "' AND '" + data.end_date + "' ) ");
//     } else {
//         enable_Time_limit = " AND R.EnableTime > date_sub(curdate(),interval 5 month) ";
//     }

//     if (sql_where.length == 0) {
//         sql_where.push(" 1 ");
//     }

//     var sql = "SELECT count(*) AS count  FROM " +
//         " (SELECT R.Id,R.Currency AS currency,R.CryDef, DATE_FORMAT(R.EnableTime,'%Y-%m-%d %H:00') AS startTime, " +
//         "     (SELECT DATE_FORMAT(A.EnableTime,'%Y-%m-%d %H:00') " +
//         "     FROM currency_exchange_rate A " +
//         "     WHERE R.EnableTime < A.EnableTime AND Currency = R.Currency " +
//         "     ORDER BY A.EnableTime ASC " +
//         "     LIMIT 0,1) AS endTime,  " +
//         "    CASE WHEN R.EnableTime > NOW() THEN '0' WHEN R.EnableTime < NOW() AND  (R.EnableTime) IN ( " +
//         "    SELECT MAX(B.EnableTime) AS EnableTime " +
//         "    FROM currency_exchange_rate B " +
//         "    WHERE B.EnableTime <= NOW() AND Currency = R.Currency  " +
//         "    ) THEN '1' ELSE '2' END AS State " +
//         "    FROM currency_exchange_rate R " +
//         "    WHERE  1 " +
//         "    HAVING State IN(0,1) OR (State=2 " + enable_Time_limit + " ) " +
//         "    ORDER BY R.EnableTime DESC) x " +
//         " WHERE " + sql_where.join(" AND ");

//     var args = [];

//     db.act_query('dbclient_g_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             cb(null, r_code, r_data[0]['count']);
//         }
//     });
// };

adminDao.getDCSettingList = function (data, cb) {
  try {
    const sql = `SELECT DC, ApiHandler, Endpoint, ApiKey, IsExternalGameToken, IsExternalQuota, ExtraJSON
            FROM dc_setting`

    const args = []

    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code != code.OK) {
        logger.error("[adminDao][getDCSettingList] sql err: ", inspect(r_code))
        return cb(null, r_code, null)
      }

      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[adminDao][getDCSettingList] catch err", inspect(err))
    cb(null, code.FAIL, null)
  }
}
