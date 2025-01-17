var logger = require("pomelo-logger").getLogger("logDao", __filename)
var code = require("../util/code")
var pomelo = require("pomelo")

const db = require("../util/DB")
const sprintf = require("sprintf-js").sprintf
const timezone = require("../util/timezone")
const conf = require("../../config/js/conf")

const logDao = module.exports

//log_admin表
logDao.add_log_admin = function (data, cb) {
  try {
    console.log("---------- add_log_admin -----------", JSON.stringify(data))
    const insert_sql = []
    const args = []

    const now = timezone.serverTime()
    const payload = { ModifiedDate: now, Status: 1, ...data }

    console.log(payload)

    for (const [key, value] of Object.entries(payload)) {
      insert_sql.push(`${key} = ?`)
      args.push(value)
    }

    console.log(insert_sql)
    console.log(args)

    const sql = "INSERT log_admin SET " + insert_sql.join(",")

    db.act_query("dbclient_l_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data.insertId)
      }
    })
  } catch (err) {
    logger.error("[logDao][add_log_admin] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//log_customer表
logDao.add_log_customer = function (data, cb) {
  try {
    var now = timezone.serverTime()
    var insert_sql = []
    Object.keys(data).forEach((item) => {
      insert_sql.push(sprintf("%s = '%s' ", item, data[item]))
    })
    insert_sql.push("ModifiedDate= '" + now + "' ")

    var sql = "INSERT log_customer SET " + insert_sql.join(",")
    var args = []
    //console.log('--add_log_customer--', sql);
    db.act_query("dbclient_l_rw", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data.insertId)
      }
    })
  } catch (err) {
    logger.error("[logDao][add_log_customer] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//log_player表
// logDao.add_log_player = function (data, cb) {
//     //console.log('-add_log_player data-', JSON.stringify(data));
//     var insert_sql = [];
//     Object.keys(data).forEach(item => {
//         insert_sql.push(sprintf("%s='%s'", item, data[item]));
//     })
//     var now = timezone.serverTime();
//     insert_sql.push(" ModifiedDate= '"+now+"' ");

//     var sql = " INSERT log_player SET " + insert_sql.join(",");
//     var args = [];

//     db.act_query('dbclient_l_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             cb(null, r_code, r_data.insertId);
//         }
//     });
// }

//修改成功/失敗的狀態
// logDao.mod_log_status = function (data, cb) {

//     pomelo.app.get('dbclient_l_rw').getConnection(function (err, connection) {

//         if (err) {
//             cb(null, { code: code.DB.GET_CONNECT_FAIL, msg: err });
//             return;
//         }

//     });
// }

logDao.add_loginout_log = function (data, cb) {
  try {
    data["browser"] = typeof data.browser == "undefined" ? "" : data.browser
    data["browser_version"] = typeof data.browser_version == "undefined" ? "" : data.browser_version
    data["os"] = typeof data.os == "undefined" ? "" : data.os
    data["os_version"] = typeof data.os_version == "undefined" ? "" : data.os_version
    data["isMobile"] = typeof data.isMobile == "undefined" ? 0 : data.isMobile
    data["isTablet"] = typeof data.isTablet == "undefined" ? 0 : data.isTablet
    data["isDesktopDevice"] = typeof data.isDesktopDevice == "undefined" ? 0 : data.isDesktopDevice

    var now = timezone.serverTime()

    var sql =
      " INSERT log_customer_loginout SET Cid=?, UserName=?, Level=?, IsSub=?, LType=?, LDesc=?, IP=?,\
        browser=?,browser_version=?,os=?,os_version=?,isMobile=?,isTablet=?,isDesktopDevice=?,Date=?; "
    var args = [
      data.Cid,
      data.UserName,
      data.Level,
      data.IsSub,
      data.LType,
      data.LDesc,
      data.IP,
      data.browser,
      data.browser_version,
      data.os,
      data.os_version,
      data.isMobile,
      data.isTablet,
      data.isDesktopDevice,
      now,
    ]

    db.act_query("dbclient_l_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[logDao][add_loginout_log] catch err", err)
    cb(null, code.FAIL, null)
  }
}

logDao.get_action_log = function (data, cb) {
  try {
    console.log("get_action_log", JSON.stringify(data))
    var self = this

    var curPage = 0
    var pageCount = 0

    var sql_where = []
    var sql_where_text = ""

    if (typeof data == "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    if (typeof data.userType != "undefined" && data.userType != "") {
      sql_where.push(" t.userLevel IN(" + data.userType + ") ")
    }

    if (typeof data.userName != "undefined" && data.userName != "") {
      sql_where.push(" t.UserName like '%" + data.userName + "%' ")
    }

    if (
      typeof data.start_date != "undefined" &&
      data.start_date != "" &&
      typeof data.end_date != "undefined" &&
      data.end_date != ""
    ) {
      sql_where.push(" (t.ModifiedDate BETWEEN '" + data.start_date + "' AND '" + data.end_date + "' )  ")
    }

    if (typeof data.operatingFunction != "undefined" && data.operatingFunction != "") {
      sql_where.push(" t.FunctionGroupL ='" + data.operatingFunction + "' ")
    }

    if (typeof data.operatingAction != "undefined" && data.operatingAction != "") {
      sql_where.push(" t.FunctionAction ='" + data.operatingAction + "' ")
    }

    if (typeof data.actionType != "undefined" && data.actionType.length > 0) {
      sql_where.push(" actionType IN ('" + data.actionType.join("','") + "') ")
    }

    if (typeof data.cus_id != "undefined" && data.cus_id.length > 0) {
      sql_where.push(" t.ActionCid IN('" + data.cus_id.join("','") + "') ")
    }

    var where_keyWord = ""
    var where_keyWord_admin = ""
    var where_keyWord_user = ""
    var where_keyWord_player = ""

    if (typeof data.keyWord != "undefined" && data.keyWord != "") {
      where_keyWord = sprintf(
        " WHERE (%%s.Desc_Before like '%%%%%s%%%%' OR %%s.Desc_After like '%%%%%s%%%%' ) ",
        data.keyWord,
        data.keyWord
      )
      where_keyWord_admin = sprintf(where_keyWord, "log_admin", "log_admin")
      where_keyWord_user = sprintf(where_keyWord, "log_customer", "log_customer")

      if (data.keyWordSearch == true) {
        var search_sql = sprintf(" AND (%%s.FunctionAction IN('%s') ) ", data.searchAction.join("','"))

        where_keyWord_admin += sprintf(search_sql, "log_admin")
        where_keyWord_user += sprintf(search_sql, "log_customer")
      }
    }

    let sortType = "DESC"
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    let sortKey = "ModifiedDate"

    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        username: "UserName",
        level: "userLevel",
        function: "FunctionGroupL",
        actionname: "FunctionAction",
        date: "ModifiedDate",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }

    sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : " 1 "

    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS * FROM ( \
            (SELECT 'log_admin' AS tableName,1 AS actionType, log_admin.Id, log_admin.AdminId AS Cid, log_admin.UserName, 1 AS userLevel, 'Operator' AS type, log_admin.FunctionGroupL, log_admin.FunctionAction,\
            log_admin.ModifiedType, DATE_FORMAT(log_admin.ModifiedDate,'%%Y-%%m-%%d %%H:%%i:%%s') AS ModifiedDate, \
            0 As ActionCid \
            FROM log_admin %s) \
            UNION DISTINCT \
            (SELECT 'log_customer' AS tableName,1 AS actionType, log_customer.Id, log_customer.ActionCid AS Cid,log_customer.ActionUserName AS UserName, log_customer.ActionLevel AS userLevel,\
            CASE \
            WHEN log_customer.ActionLevel = 1 THEN 'Operator'  \
            WHEN log_customer.ActionLevel = 2 THEN 'Hall'  \
            WHEN log_customer.ActionLevel = 3 THEN 'Agent' \
            END AS type , \
            log_customer.FunctionGroupL, log_customer.FunctionAction,log_customer.ModifiedType, DATE_FORMAT(log_customer.ModifiedDate,'%%Y-%%m-%%d %%H:%%i:%%s') AS ModifiedDate, \
            log_customer.ActionCid As ActionCid \
            FROM log_customer\
            %s ) \
            ) t \
            WHERE %s \
            ORDER BY %s %s \
            LIMIT %s,%s ",
      where_keyWord_admin,
      where_keyWord_user,
      sql_where_text,
      sortKey,
      sortType,
      (data.curPage - 1) * data.pageCount,
      data.pageCount
    )

    var sql2 = "SELECT FOUND_ROWS() AS ROWS;"
    var sql_query = []
    sql_query.push(sql)
    sql_query.push(sql2)

    console.log("----------get_action_log sql-------------", sql)

    db.act_transaction("dbclient_l_r", sql_query, function (r_code, r_data) {
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
    logger.error("[logDao][get_action_log] catch err", err)
    cb(null, code.FAIL, null)
  }
}

logDao.getOperatingRecordDetail = function (data, cb) {
  try {
    var self = this

    var sql_where = []

    if (
      typeof data == "undefined" ||
      typeof data.Id == "undefined" ||
      data.Id == "" ||
      typeof data.table == "undefined" ||
      data.table == ""
    ) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    if (typeof data.Id != "undefined" && data.Id != "") {
      sql_where.push(" t.Id IN('" + data.Id + "') ")
    }

    if (typeof data.table != "undefined" && data.table != "") {
      sql_where.push(" t.tableName ='" + data.table + "'  ")
    }

    var sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : " 1 "

    var log_sql = ""
    switch (data.table) {
      case "log_admin":
        log_sql =
          " (SELECT 'log_admin' AS tableName, log_admin.Id, log_admin.AdminId AS Cid, log_admin.UserName, 1 AS userLevel, 'Operator' AS type, log_admin.FunctionGroupL, log_admin.FunctionAction," +
          " log_admin.ModifiedType,log_admin.RequestMsg ,log_admin.Desc_Before,log_admin.Desc_After , DATE_FORMAT(log_admin.ModifiedDate,'%Y-%m-%d %H:%i:%s') AS ModifiedDate " +
          " FROM log_admin) "
        break
      case "log_customer":
        log_sql =
          " (SELECT 'log_customer' AS tableName, log_customer.Id, IF(log_customer.AdminId > 0, log_customer.AdminId, log_customer.ActionCid) AS Cid, " +
          " IF(log_customer.AdminId > 0, log_customer.AdminUserName, log_customer.ActionUserName) AS UserName, log_customer.Level AS userLevel," +
          " CASE " +
          " WHEN log_customer.Level = 2 THEN 'Hall'  " +
          " WHEN log_customer.Level = 3 THEN 'Agent' " +
          " END AS type , " +
          " log_customer.FunctionGroupL, log_customer.FunctionAction,log_customer.ModifiedType, log_customer.RequestMsg ,log_customer.Desc_Before ,log_customer.Desc_After , DATE_FORMAT(log_customer.ModifiedDate,'%Y-%m-%d %H:%i:%s') AS ModifiedDate " +
          " FROM log_customer) "
        break
    }

    var sql = "SELECT * FROM ( " + log_sql + " ) t " + " WHERE " + sql_where_text + " ORDER BY ModifiedDate DESC  "

    var args = []

    db.act_query("dbclient_l_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[logDao][getOperatingRecordDetail] catch err", err)
    cb(null, code.FAIL, null)
  }
}
// logDao.get_loginout = function (data, cb) {
//     var self = this;

//     var curPage = 0;
//     var pageCount = 0;

//     // var sql_where = [];
//     // var sql_where_text = "";

//     if (typeof data == 'undefined') {
//         cb(null, {
//             code: code.DB.DATA_EMPTY,
//             msg: null
//         }, null);
//         return;
//     }

//     var sql_where_cus = [];
//     var sql_where_player = [];

//     if (typeof data.userType != 'undefined' && data.userType != '') {
//       //  sql_where.push(" t.level IN('" + data.userType + "') ");
//         sql_where_cus.push( "cus.Level IN(" + data.userType + ")");
//         sql_where_player.push( "4 IN(" + data.userType + ")");
//     }

//     if (typeof data.userName != 'undefined' && data.userName != '') {
//         //sql_where.push(" t.UserName like '%" + data.userName + "%' ");
//         sql_where_cus.push( "cus.UserName LIKE '%" + data.userName + "%' ");
//         sql_where_player.push( "player.UserName LIKE '%" + data.userName + "%' ");
//     }

//     if (typeof data.start_date != 'undefined' && data.start_date != '' && typeof data.end_date != 'undefined' && data.end_date != '') {
//         //sql_where.push(" (t.actionDate BETWEEN '" + data.start_date + "' AND '" + data.end_date + "' )  ");
//         sql_where_cus.push( " (cus.Date BETWEEN '" + data.start_date + "' AND '" + data.end_date + "' )  ");
//         sql_where_player.push( " (player.Date BETWEEN '" + data.start_date + "' AND '" + data.end_date + "') ");
//     }

//     /*
//         //admin
//         if (typeof data.opLevel == 1) { }

//         //ha
//         if (typeof data.opLevel == 2) {
//             var where_text = (data.opIsSub == 1) ? " customer.HallId = '" + data.opHallId + "'" : " customer.HallId = '" + data.opCid + "'";
//             sql_where.push(where_text);
//         }
//         //ag
//         if (typeof data.opLevel == 3) {
//             sql_where.push(" customer.Upid = '" + data.opCid + "' ");
//         }
//     */

//     if (data.opLevel > 1) {
//         sql_where_cus.push("  cus.Cid IN('" + data.cus_id.join("','") + "')");
//         sql_where_player.push("  player.Cid IN('" + data.cus_id.join("','") + "')");
//     }

//     //sql_where_text = (sql_where.length > 0) ? sql_where.join(" AND ") : " 1 ";

//     var sql_where_cus_text =  (sql_where_cus.length > 0) ? sql_where_cus.join(" AND ") : " 1 ";
//     var sql_where_player_text =  (sql_where_player.length > 0) ? sql_where_player.join(" AND ") : " 1 ";

//     /*
//         var sql = sprintf(" SELECT SQL_CALC_FOUND_ROWS * " +
//             "	FROM (	" +
//             "	(SELECT cus.Id,cus.Cid,cus.UserName,cus.Level,cus.IsSub,cus.LType,Date_Format(cus.Date,'%%Y-%%m-%%d %%H:%%i:%%s') AS actionDate, 	" +
//             "	CASE WHEN cus.Level=1 THEN 'Operator' WHEN cus.Level=2 THEN 'Hall' WHEN cus.Level=3 THEN 'Agent' END AS type, " +
//             "	customer.Upid,game.customer.HallId,game.customer.IsAg	" +
//             "	FROM log_customer_loginout cus	" +
//             "	LEFT JOIN game.customer ON(game.customer.Cid=cus.Cid AND cus.Level=game.customer.IsAg) ) 	" +
//             "	UNION DISTINCT	" +
//             "	(SELECT player.Id,player.Cid,player.UserName,4 AS LEVEL, 0 AS IsSub,player.LType,Date_Format(player.Date,'%%Y-%%m-%%d %%H:%%i:%%s') AS actionDate,'Player' AS type, " +
//             "	customer.Upid,game.customer.HallId,game.customer.IsAg	" +
//             "	FROM log_player_loginout player	" +
//             "	LEFT JOIN game.customer ON(game.customer.Cid=player.Cid AND game.customer.IsAg=4)) 	" +
//             "	) t	WHERE %s " +
//             "	ORDER BY actionDate DESC  " +
//             "	LIMIT %s,%s ", sql_where_text, (data.curPage - 1) * data.pageCount, data.pageCount);
//     */

//     var sql = sprintf(" SELECT SQL_CALC_FOUND_ROWS * " +
//         "	FROM (	" +
//         "	(SELECT cus.Id,cus.Cid,cus.UserName,cus.Level,cus.IsSub,cus.LType,Date_Format(cus.Date,'%%Y-%%m-%%d %%H:%%i:%%s') AS actionDate, 	" +
//         "	CASE WHEN cus.Level=1 THEN 'Operator' WHEN cus.Level=2 THEN 'Hall' WHEN cus.Level=3 THEN 'Agent' END AS type, " +
//         "   cus.browser, cus.os ,CASE WHEN cus.isMobile = 1 THEN 'mobile'  WHEN cus.isTablet = 1 THEN 'tablet'  WHEN cus.isDesktopDevice = 1 THEN 'desktop device' ELSE 'unknown' END AS device " +
//         "	FROM log_customer_loginout cus " +
//         "   WHERE %s )" +
//         "	UNION DISTINCT	" +
//         "	(SELECT player.Id,player.Cid,player.UserName,4 AS LEVEL, 0 AS IsSub,player.LType,Date_Format(player.Date,'%%Y-%%m-%%d %%H:%%i:%%s') AS actionDate, 'Player' AS type, " +
//         "   '' AS browser, player.os AS os, CASE WHEN player.isMobile=1 THEN 'mobile' END AS device "+
//         "	FROM log_player_loginout player	" +
//         "   WHERE %s " +
//         "	)) t " +
//         "	ORDER BY actionDate DESC  " +
//         "	LIMIT %s,%s ", sql_where_cus_text, sql_where_player_text,   (data.curPage - 1) * data.pageCount, data.pageCount);
//     var sql2 = "SELECT FOUND_ROWS() AS ROWS;";
//     var sql_query = [];
//     sql_query.push(sql);
//     sql_query.push(sql2);

//     db.act_transaction('dbclient_l_rw', sql_query, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             var data = {
//                 count: r_data[1][0]['ROWS'],
//                 info: r_data[0]
//             }
//             cb(null, r_code, data);
//         }
//     });
// }

logDao.get_user_loginout = function (data, cb) {
  try {
    var curPage = 0
    var pageCount = 0

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

    var sql_where_cus = []

    if (typeof data.userType != "undefined" && data.userType != "") {
      //修改管理員登入紀錄邏輯
      const userType = data.userType.split(",")
      if (userType.includes("5")) {
        //判斷是否含有管理員
        if (userType.length > 1) {
          sql_where_cus.push(
            "(Level IN(" + data.userType + ")" + " OR " + "Level IN(" + data.userType + ") AND IsSub = 1)"
          )
        } else {
          sql_where_cus.push("(Level IN (2,3)" + " AND " + "IsSub = 1)")
        }
      } else {
        sql_where_cus.push("(Level IN(" + data.userType + ")" + " AND " + "IsSub = 0)")
      }
    }

    if (typeof data.userName != "undefined" && data.userName != "") {
      sql_where_cus.push("UserName LIKE '%" + data.userName + "%' ")
    }

    if (
      typeof data.start_date != "undefined" &&
      data.start_date != "" &&
      typeof data.end_date != "undefined" &&
      data.end_date != ""
    ) {
      sql_where_cus.push(" (Date BETWEEN '" + data.start_date + "' AND '" + data.end_date + "' )  ")
    }

    if (data.opLevel > 1) {
      sql_where_cus.push("  Cid IN('" + data.cus_id.join("','") + "')")
    }

    var sql_where_cus_text = sql_where_cus.length > 0 ? sql_where_cus.join(" AND ") : " 1 "

    let sortType = "DESC"
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    let sortKey = "actionDate"

    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        id: "Id",
        cid: "Cid",
        username: "t.UserName",
        level: "t.type",
        ip: "ip",
        logintype: "LType",
        gameid: "GameId",
        browser: "browser",
        os: "os",
        device: "device",
        date: "actionDate",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }

    var sql = sprintf(
      " SELECT SQL_CALC_FOUND_ROWS * FROM " +
        "   (SELECT Id,Cid,UserName,Level,IsSub,LType,Date_Format(Date,'%%Y-%%m-%%d %%H:%%i:%%s') AS actionDate, 	" +
        "	CASE WHEN Level=1 THEN 'Operator' WHEN Level=2 THEN 'Hall' WHEN Level=3 THEN 'Agent' END AS type, IP, " +
        "   browser, os ,CASE WHEN isMobile = 1 THEN 'mobile' WHEN isTablet = 1 THEN 'tablet'  WHEN isDesktopDevice = 1 THEN 'web' ELSE '' END AS device " +
        "	FROM log_customer_loginout " +
        "   WHERE %s ) t " +
        "	ORDER BY %s %s  " +
        "	LIMIT %s,%s ",
      sql_where_cus_text,
      sortKey,
      sortType,
      (data.curPage - 1) * data.pageCount,
      data.pageCount
    )
    var sql2 = "SELECT FOUND_ROWS() AS ROWS;"
    var sql_query = []
    sql_query.push(sql)
    sql_query.push(sql2)

    db.act_transaction("dbclient_l_r", sql_query, function (r_code, r_data) {
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
    logger.error("[logDao][get_user_loginout] catch err", err)
    cb(null, code.FAIL, null)
  }
}

logDao.get_player_loginout = function (data, cb) {
  try {
    var curPage = 0
    var pageCount = 0

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

    var sql_where_player = []

    if (typeof data.userName != "undefined" && data.userName != "") {
      sql_where_player.push("UserName LIKE '%" + data.userName + "%' ")
    }

    if (
      typeof data.start_date != "undefined" &&
      data.start_date != "" &&
      typeof data.end_date != "undefined" &&
      data.end_date != ""
    ) {
      sql_where_player.push(" (Date BETWEEN '" + data.start_date + "' AND '" + data.end_date + "') ")
    }

    if (data.opLevel > 1) {
      sql_where_player.push(" Cid IN('" + data.player_id.join("','") + "')")
    }

    var sql_where_player_text = sql_where_player.length > 0 ? sql_where_player.join(" AND ") : " 1 "

    let sortType = "DESC"
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    let sortKey = "actionDate"

    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        id: "Id",
        cid: "Cid",
        username: "t.UserName",
        country: "ip",
        logintype: "LType",
        gameid: "GameId",
        browser: "browser",
        os: "os",
        device: "device",
        date: "actionDate",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }

    var sql = sprintf(
      " SELECT SQL_CALC_FOUND_ROWS * FROM (SELECT Id, Cid, UserName, 4 AS Level, 0 AS IsSub, LType, GameId, " +
        "   Date_Format(Date,'%%Y-%%m-%%d %%H:%%i:%%s') AS actionDate, 'Player' AS type, IP, " +
        "   browser, os, CASE WHEN isMobile=1 THEN 'mobile' ELSE 'web' END AS device " +
        "	FROM log_player_loginout " +
        "   WHERE %s ) t " +
        "	ORDER BY %s %s " +
        "	LIMIT %s,%s ",
      sql_where_player_text,
      sortKey,
      sortType,
      (data.curPage - 1) * data.pageCount,
      data.pageCount
    )
    var sql2 = "SELECT FOUND_ROWS() AS ROWS;"
    var sql_query = []
    sql_query.push(sql)
    sql_query.push(sql2)

    db.act_transaction("dbclient_l_r", sql_query, function (r_code, r_data) {
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
    logger.error("[logDao][get_player_loginout] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// logDao.get_onlineUser = function (data, cb) {
//     var self = this;

//     var curPage = 0;
//     var pageCount = 0;

//     var sql_where = [];
//     var sql_where_text = "";

//     if (typeof data == 'undefined') {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null);
//         return;
//     }
// /*
//     //ad
//     if (data.opLevel == 1) {
//     }
//     //ha
//     if (typeof data.opLevel == 2) {
//         var userId = (data.opIsSub == 1) ? data.opHallId : data.opCid;
//         sql_where.push(" cus.HallId ='" + userId + "'");
//     }
//     //pr
//     if (typeof data.opLevel == 3) {
//         sql_where.push(" cus.Upid ='" + data.opCid + "'");
//     }
// */

//     if (typeof data.server_start_date != 'undefined' && data.server_start_date != '' && typeof data.server_end_date != 'undefined' && data.server_end_date != '') {
//         sql_where.push(" ( Date >= '" + data.server_start_date + "' AND Date < '" + data.server_end_date + "' ) ");
//     }
// /*
//     //admin
//     if (typeof data.opLevel == 1) { }
//     //ha
//     if (typeof data.opLevel == 2) {
//         var where_text = (data.opIsSub == 1) ? " customer.HallId = '" + data.opHallId + "'" : " customer.HallId = '" + data.opCid + "'";
//         sql_where.push(where_text);
//     }
//     //ag
//     if (typeof data.opLevel == 3) {
//         sql_where.push(" customer.Upid = '" + data.opCid + "' ");
//     }
// */
//   if(typeof data.players  != 'undefined'  )  sql_where.push(" log.Cid IN('" + data.players.join("','") + "') ");
//     sql_where_text = (sql_where.length > 0) ? sql_where.join(" AND ") : " 1 ";
// /*
//     var sql = "SELECT *,count(DISTINCT Cid) AS count, DATE_FORMAT(client_local_time ,'%Y-%m-%d') AS client_local_date " +
//         " FROM  " +
//         " ( " +
//         " SELECT log.Cid, log.Date, date_add(log.Date,interval " + data.timeDiff + " HOUR) AS client_local_time  " +
//         " FROM log_player_loginout log " +
//         " LEFT JOIN game.customer cus ON(cus.Cid = log.Cid AND cus.IsAg=4) " +
//         " WHERE 1 AND " + sql_where.join(" AND ") +
//         " ) T " +
//         " GROUP BY DATE_FORMAT(client_local_time ,'%Y-%m-%d')  " +
//         " ORDER BY DATE_FORMAT(client_local_time ,'%Y-%m-%d') DESC ";
// */

//  var sql = "SELECT *,count(DISTINCT Cid) AS count, DATE_FORMAT(client_local_time ,'%Y-%m-%d') AS client_local_date " +
//          " FROM  " +
//          " ( " +
//          " SELECT log.Cid, log.Date, date_add(log.Date,interval " + data.timeDiff + " HOUR) AS client_local_time  " +
//          " FROM log_player_loginout log " +
//          " WHERE " + sql_where.join(" AND ") +
//          " ) T " +
//          " GROUP BY DATE_FORMAT(client_local_time ,'%Y-%m-%d')  " +
//          " ORDER BY DATE_FORMAT(client_local_time ,'%Y-%m-%d') DESC ";

//     var args = [(data.curPage - 1) * data.pageCount, data.pageCount];

//     db.act_query('dbclient_l_rw', sql, args, function (r_code, r_data) {
//         cb(null, r_code, r_data);
//     });
// }

logDao.getUserLastLogin = function (data, cb) {
  try {
    var self = this

    if (typeof data === "undefined" || typeof data.cid === "undefined" || typeof data.level === "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }
    var sql_where = []
    sql_where.push(" Cid IN ('" + data.cid.join("','") + "') ")

    var table_name = ""
    if (data.level == 4) {
      table_name = "log_player_loginout"
    } else {
      table_name = "log_customer_loginout"
      sql_where.push(sprintf(" Level=%s ", data.level))
    }
    var sql_where_text = " "
    if (sql_where.length > 0) sql_where_text += " AND " + sql_where.join(" OR ") //因為層級使寫死2造成代理管理員會撈不到，所以先改為OR

    var sql =
      "SELECT Cid AS cid, MAX(DATE_FORMAT(Date,'%Y-%m-%d %H:%i:%s')) AS lastLogin " +
      " FROM " +
      table_name +
      " WHERE LType='IN'  " +
      sql_where_text +
      " GROUP BY Cid"
    var args = []

    db.act_query("dbclient_l_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[logDao][getUserLastLogin] catch err", err)
    cb(null, code.FAIL, null)
  }
}

logDao.getOnlinePlayers = function (data, cb) {
  try {
    var self = this

    if (typeof data == "undefined" || typeof data.start_date == "undefined" || typeof data.end_date == "undefined") {
      cb(null, { code: code.DB.PARA_FAIL, msg: null }, null)
      return
    }

    var sql_where = []
    if (data.start_date != "") {
      sql_where.push(sprintf("l.Date >= '%s' ", data.start_date))
    }
    if (data.end_date != "") {
      sql_where.push(sprintf("l.Date <= '%s' ", data.end_date))
    }
    switch (data.level) {
      case 2: //hall
        sql_where.push(sprintf("l.HallId= '%s' ", data.user_hallId))
        break
      case 3: //agent
        sql_where.push(sprintf("l.UpId= '%s' ", data.user_agentId))
        break
    }
    sql_where.push(" l.LType='IN' ")

    const sql = `SELECT count(DISTINCT l.Cid) AS uniPlayers , DATE_FORMAT(convert_tz(l.Date, "+00:00", ? ), "%H" ) AS clientLocalHour
        FROM  log_player_loginout l WHERE ${sql_where.join(" AND ")}
        GROUP BY clientLocalHour
        ORDER BY clientLocalHour ASC`

    const args = [data.hourDiffFormated]

    console.log("-getOnlinePlayers sql-", sql)

    db.act_query("dbclient_l_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[logDao][getOnlinePlayers] catch err", err)
    cb(null, code.FAIL, null)
  }
}

/*logDao.renewExChangeRate = function (data, cb) {
    if (data.length == 0) {
        cb(null, { code: code.OK, msg: null }, null);
        return;
    }
    var sql = "DELETE FROM currency_exchange_rate WHERE 1";
    var insert_sql = [];
    data.forEach(item => {
        insert_sql.push(sprintf("(%s,'%s','%s','%s','%s')", item.Id, item.Currency, item.ExCurrency, item.CryDef, item.EnableTime));
    });
    var insert_sql_text = insert_sql.join(" , ");
    var sql2 = "INSERT currency_exchange_rate (Id,Currency,ExCurrency,CryDef,EnableTime) VALUES " + insert_sql_text;
    var sql_query = [];
    sql_query.push(sql);
    sql_query.push(sql2);
    console.log('-sql_query-', sql_query);
    db.act_transaction('dbclient_g_rw', sql_query, function (r_code, r_data) {
        if (r_code.code !== code.OK) {
            cb(null, r_code, null);
        } else {
            cb(null, r_code, r_data);
        }
    });
}*/
logDao.getOnlinePlayerNumsFromCustomer = function (data, cb) {
  try {
    var sql_where = []
    switch (data.level) {
      case 1: //admin取全部
        sql_where.push(" IsOnline = 1 ")
        sql_where.push(" IsAg = 4 ")
        break
      case 2: //hall
        sql_where.push(" IsOnline = 1 ")
        sql_where.push(" IsAg = 4 ")
        if (data.cid.length > 1) {
          sql_where.push(`HallId IN ('${data.cid.join("','")}')`)
        } else {
          sql_where.push(" HallId ='" + data.cid + "' ")
        }
        break
      case 3: //agent
        sql_where.push(" IsOnline = 1 ")
        sql_where.push(" IsAg = 4 ")
        sql_where.push(" UpId ='" + data.cid + "' ")
        break
    }

    var sql = "SELECT COUNT(*) AS num  \
                FROM customer\
                WHERE " + sql_where.join(" AND ")
    var args = []
    db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var num = r_data.length > 0 ? r_data : 0
        cb(null, r_code, num)
      }
    })
  } catch (err) {
    logger.error("[logDao][getOnlinePlayerNumsFromCustomer] catch err", err)
    cb(null, code.FAIL, null)
  }
}
logDao.getOnlinePlayersNums = function (data, cb) {
  try {
    var sql_where = []
    sql_where.push(" LType ='IN' ")

    switch (data.level) {
      case 1: //admin取全部
        break
      case 2: //hall
        sql_where.push(" HallId ='" + data.hallId + "' ")
        break
      case 3: //agent
        sql_where.push(" HallId ='" + data.hallId + "' AND  UpId ='" + data.cid + "' ")
        break
    }

    if (typeof data.end_date !== "undefined") {
      sql_where.push(" Date <= '" + data.end_date + "'  ")
    }
    //var now = timezone.serverTime();
    var sql = sprintf(
      "SELECT COUNT(*) AS num FROM \
                ( SELECT t.*,DATE_FORMAT(logout.Date,'%%Y-%%m-%%d %%H:%%i:%%s') AS logoutTime \
                FROM  ( \
                SELECT HallId,Upid,Cid,MAX(DATE_FORMAT(Date,'%%Y-%%m-%%d %%H:%%i:%%s')) AS loginTime \
                FROM log_player_loginout\
                WHERE %s \
                GROUP BY Cid \
                ) t\
                LEFT JOIN log_player_loginout logout ON(logout.Cid=t.Cid AND logout.Date > t.loginTime AND logout.LType ='OUT') \
                ) x  \
                WHERE x.logoutTime IS NULL ",
      sql_where.join(" AND ")
    )
    var args = []

    db.act_query("dbclient_l_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        var num = r_data.length > 0 ? r_data[0]["num"] : 0
        cb(null, r_code, num)
      }
    })
  } catch (err) {
    logger.error("[logDao][getOnlinePlayersNums] catch err", err)
    cb(null, code.FAIL, null)
  }
}

logDao.get_quota_action_log = function (data, cb) {
  try {
    var self = this
    var curPage = 0
    var pageCount = 0
    var sql_where = []
    var sql_where_text = ""

    if (typeof data == "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    if (typeof data.userName != "undefined" && data.userName != "") {
      sql_where.push(" t.UserName like '" + data.userName + "%' ")
    }

    if (
      typeof data.start_date != "undefined" &&
      data.start_date != "" &&
      typeof data.end_date != "undefined" &&
      data.end_date != ""
    ) {
      sql_where.push(" (t.AddDate BETWEEN '" + data.start_date + "' AND '" + data.end_date + "' )  ")
    }

    if (typeof data.gameId != "undefined" && data.gameId != "") {
      sql_where.push(" t.GameId ='" + data.gameId + "' ")
    }

    //遊戲名稱
    if (typeof data.search_gameId != "undefined" && data.search_gameId != "" && data.search_gameId.length > 0) {
      if (data.search_gameId.length > 1) {
        sql_where.push(`t.GameId IN ('${data.search_gameId.join("','")}')`)
      } else {
        sql_where.push(`t.GameId = ('${data.search_gameId}')`)
      }
    }
    // if (typeof data.currency != 'undefined' && data.currency != '') {
    //     sql_where.push(" t.Currency ='" + data.currency + "' ");
    // }

    if (typeof data.player_id != "undefined" && data.player_id.length > 0) {
      sql_where.push(" t.Cid IN('" + data.player_id.join("','") + "')")
    }

    sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : " 1 "

    var sortKey = " t.AddDate"
    var sortType = conf.SORT_TYPE[0]

    if (typeof data.sortKey !== "undefined" && data.sortKey !== "") {
      data.sortKey = data.sortKey.toLowerCase()

      var sort_info = {
        adddate: "t.AddDate",
        username: "t.UserName",
        gameid: "t.GameId",
        IP: "t.IP",
        country: "t.IP",
        currency: "t.Currency",
        crydef: "t.CryDef",
        oldquota: "t.OldQuota",
        newquota: "t.NewQuota",
        amount: "t.Amount",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    var order_by_text = sprintf(" ORDER BY %s %s", sortKey, sortType)
    var sql_limit =
      data.isPage === true
        ? " LIMIT " + (data.curPage - 1) * data.pageCount + "," + data.pageCount
        : " LIMIT  " + data.index + "," + data.pageCount //限制筆數

    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS * FROM ( \
            (SELECT log_quota.Id, log_quota.Cid, log_quota.UserName, log_quota.GameId, log_quota.Currency, log_quota.CryDef, log_quota.OldQuota, log_quota.NewQuota, log_quota.Amount, \
            log_quota.IP, log_quota.LDesc, DATE_FORMAT(log_quota.AddDate,'%%Y-%%m-%%d %%H:%%i:%%s') AS AddDate \
            FROM log_quota) \
            ) t \
            WHERE %s %s %s ",
      sql_where_text,
      order_by_text,
      sql_limit
    )

    var sql2 = "SELECT FOUND_ROWS() AS ROWS;"
    var sql_query = []
    sql_query.push(sql)
    sql_query.push(sql2)

    db.act_transaction("dbclient_l_r", sql_query, function (r_code, r_data) {
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
    logger.error("[logDao][get_quota_action_log] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// 伺服器紀錄查詢
logDao.get_server_action_log = function (data, cb) {
  try {
    console.log("get_server_action_log", JSON.stringify(data))

    var self = this

    var curPage = 0
    var pageCount = 0

    var sql_where = []
    var sql_where_text = ""

    if (typeof data == "undefined") {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    if (typeof data.userName != "undefined" && data.userName != "") {
      sql_where.push(" t.UserName like '%" + data.userName + "%' ")
    }

    if (
      typeof data.start_date != "undefined" &&
      data.start_date != "" &&
      typeof data.end_date != "undefined" &&
      data.end_date != ""
    ) {
      sql_where.push(" (t.ModifiedDate BETWEEN '" + data.start_date + "' AND '" + data.end_date + "' )  ")
    }

    if (typeof data.gameId != "undefined" && data.gameId != "") {
      sql_where.push(" (t.GameId = '" + data.gameId + "')  ")
    }

    //遊戲名稱
    if (typeof data.search_gameId != "undefined" && data.search_gameId != "" && data.search_gameId.length > 0) {
      if (data.search_gameId.length > 1) {
        sql_where.push(`t.GameId IN ('${data.search_gameId.join("','")}')`)
      } else {
        sql_where.push(`t.GameId = ('${data.search_gameId}')`)
      }
    }

    if (typeof data.cus_id != "undefined" && data.cus_id.length > 0) {
      sql_where.push(" t.Cid IN('" + data.cus_id.join("','") + "') ")
    }

    var where_keyWord = ""
    var where_keyWord_server = ""

    if (typeof data.keyWord != "undefined" && data.keyWord != "" && data.keyWordSearch == true) {
      where_keyWord = sprintf(" WHERE (%%s.Action like '%%%%%s%%%%' ) ", data.keyWord, data.keyWord)
      where_keyWord_server = sprintf(where_keyWord, "log_server", "log_server")
    }

    sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : " 1 "

    let sortType = "DESC"
    if (typeof data.sortType !== "undefined" && ["0", "1"].indexOf(data.sortType.toString()) > -1) {
      sortType = conf.SORT_TYPE[data.sortType]
    }
    let sortKey = "ModifiedDate"

    if (typeof data.sortKey !== "undefined" && data.sortKey != "") {
      data.sortKey = data.sortKey.toLowerCase()
      var sort_info = {
        username: "UserName",
        gameid: "GameId",
        server: "ActionServer",
        operatingfunction: "Action",
        date: "ModifiedDate",
      }
      sortKey = typeof sort_info[data.sortKey] != "undefined" ? sort_info[data.sortKey] : sortKey
    }

    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS * FROM ( \
        (SELECT 'log_server' AS tableName, log_server.Id, log_server.Cid, log_server.UserName, log_server.ActionServer, log_server.Action, \
        log_server.GameId, DATE_FORMAT(log_server.ModifiedDate,'%%Y-%%m-%%d %%H:%%i:%%s') AS ModifiedDate \
        FROM log_server %s) \
        ) t \
        WHERE %s \
        ORDER BY %s %s  \
        LIMIT %s,%s ",
      where_keyWord_server,
      sql_where_text,
      sortKey,
      sortType,
      (data.curPage - 1) * data.pageCount,
      data.pageCount
    )

    var sql2 = "SELECT FOUND_ROWS() AS ROWS;"
    var sql_query = []
    sql_query.push(sql)
    sql_query.push(sql2)

    console.log("----------get_server_action_log sql-------------", sql)

    db.act_transaction("dbclient_l_r", sql_query, function (r_code, r_data) {
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
    logger.error("[logDao][get_server_action_log][catch] err: %s, dc: %s", err, dc)
    cb(null, { code: code.FAIL }, null)
  }
}

// 伺服器紀錄明細查詢
logDao.getServerRecordDetail = function (data, cb) {
  try {
    var self = this

    var sql_where = []

    if (
      typeof data == "undefined" ||
      typeof data.Id == "undefined" ||
      data.Id == "" ||
      typeof data.table == "undefined" ||
      data.table == ""
    ) {
      cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null)
      return
    }

    if (typeof data.Id != "undefined" && data.Id != "") {
      sql_where.push(" t.Id IN('" + data.Id + "') ")
    }

    var sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : " 1 "

    var log_sql =
      " (SELECT 'log_server' AS tableName, log_server.Id, log_server.Cid, log_server.UserName, log_server.GameId, log_server.ActionServer, log_server.Action, " +
      " log_server.Desc_Before, log_server.Desc_After , DATE_FORMAT(log_server.ModifiedDate,'%Y-%m-%d %H:%i:%s') AS ModifiedDate " +
      " FROM log_server) "

    var sql = "SELECT * FROM ( " + log_sql + " ) t " + " WHERE " + sql_where_text + " ORDER BY ModifiedDate DESC  "

    var args = []
    db.act_query("dbclient_l_r", sql, args, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[logDao][getServerRecordDetail][catch] err: %s, dc: %s", err, dc)
  }
}
/*
logDao.get_table_Info = function (msg, cb) {

    var info = [];
   //console.log('-get_table_Info msg--', msg);
    for (var i in msg) {

        var sql = " SELECT * FROM " + msg[i]['table'] + " WHERE " + msg[i]['search_key'] + "=" + msg[i]['search_value'];

        var args = [];

       //console.log('sql', sql);
        pomelo.app.get(msg[i]['db']).getConnection(function (err, connection) {
            connection.query(sql, args, function (err, res) {
                connection.release();
                if (err) {
                    cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
                } else {

                    if (res) {
                        info.push(res);
                       //console.log('-get_table_Info res--', res);
                        //  cb(null, { code: code.OK }, res[0]);
                    } else {
                        cb(null, { code: code.DB.DATA_EMPTY }, null);
                    }
                }
            });
        });
    }
   //console.log('-get_table_Info info--', info);
    cb(null, { code: code.OK }, info);
}
*/

// logDao.getCounts_action_log = function (data, cb) {

//     var self = this;
//     var ttlCount = 0;
//     var sql_where = [];
//     var sql_where_text = " 1 ";

//     if (typeof data == 'undefined') {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null);
//         return;
//     }
//     if (typeof data.userType != 'undefined' && data.userType != '') {
//         sql_where.push(" t.userLevel IN(" + data.userType + ") ");
//     }

//     if (typeof data.userName != 'undefined' && data.userName != '') {
//         sql_where.push(" t.UserName like '%" + data.userName + "%' ");
//     }

//     if (typeof data.start_date != 'undefined' && data.start_date != '' && typeof data.end_date != 'undefined' && data.end_date != '') {
//         sql_where.push(" (t.ModifiedDate BETWEEN '" + data.start_date + "' AND '" + data.end_date + "' )  ");
//     }

//     if (typeof data.operatingFunction != 'undefined' && data.operatingFunction != '') {
//         sql_where.push(" t.FunctionGroupL ='" + data.operatingFunction + "' ");
//     }

//     if (typeof data.operatingAction != 'undefined' && data.operatingAction != '') {
//         sql_where.push(" t.FunctionAction ='" + data.operatingAction + "' ");
//     }

//     sql_where_text = (sql_where.length > 0) ? sql_where.join(" AND ") : " 1 ";

//     var sql = " SELECT COUNT(*) AS count FROM ( " +
//         " (SELECT 'log_admin' AS tableName,log_admin.Id,  log_admin.AdminId AS Cid, log_admin.UserName, 1 AS userLevel, 'Operator' AS type, log_admin.FunctionGroupL, log_admin.FunctionAction," +
//         " log_admin.ModifiedType, DATE_FORMAT(log_admin.ModifiedDate,'%Y-%m-%d %H:%i:%s') AS ModifiedDate " +
//         " FROM log_admin) " +
//         " UNION DISTINCT" +
//         " (SELECT 'log_customer' AS tableName,log_customer.Id, IF(log_customer.AdminId > 0, log_customer.AdminId, log_customer.ActionCid) AS Cid, IF(log_customer.AdminId > 0, log_customer.AdminUserName, log_customer.ActionUserName) AS UserName, IF(log_customer.AdminId > 0, 1, game.customer.IsAg) AS userLevel," +
//         " CASE " +
//         " WHEN IF(log_customer.AdminId > 0, 1, game.customer.IsAg)=1 THEN 'Operator'  " +
//         " WHEN IF(log_customer.AdminId > 0, 1, game.customer.IsAg)=2 THEN 'Hall'  " +
//         " WHEN IF(log_customer.AdminId > 0, 1, game.customer.IsAg)=3 THEN 'Agent' " +
//         " END AS type , " +
//         "  log_customer.FunctionGroupL, log_customer.FunctionAction,log_customer.ModifiedType,  DATE_FORMAT(log_customer.ModifiedDate,'%Y-%m-%d %H:%i:%s') AS ModifiedDate " +
//         " FROM log_customer" +
//         " LEFT JOIN game.customer ON(game.customer.Cid = log_customer.ActionCid)" +
//         "  )" +
//         " ) t " +
//         " WHERE " + sql_where_text;

//     var args = [];

//     db.act_query('dbclient_l_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             cb(null, r_code, r_data[0].count);
//         }
//     });
// }

// logDao.getCounts_loginout = function (data, cb) {

//     //console.log('-getCounts_loginout-', JSON.stringify(data));

//     var self = this;

//     var curPage = 0;
//     var pageCount = 0;

//     var sql_where = [];
//     var sql_where_text = "";

//     if (typeof data == 'undefined') {
//         cb(null, { code: code.DB.DATA_EMPTY, msg: null }, null);
//         return;
//     }

//     if (typeof data.userType != 'undefined' && data.userType != '') {
//         sql_where.push(" t.level IN(" + data.userType + ") ");
//     }

//     if (typeof data.userName != 'undefined' && data.userName != '') {
//         sql_where.push(" t.UserName like '%" + data.userName + "%' ");
//     }

//     if (typeof data.start_date != 'undefined' && data.start_date != '' && typeof data.end_date != 'undefined' && data.end_date != '') {
//         sql_where.push(" (t.actionDate BETWEEN '" + data.start_date + "' AND '" + data.end_date + "' )  ");
//     }

//     //admin
//     if (typeof data.opLevel == 1) { }
//     //ha
//     if (typeof data.opLevel == 2) {
//         var where_text = (data.opIsSub == 1) ? " customer.HallId = " + data.opHallId : " customer.HallId = " + data.opCid;
//         sql_where.push(where_text);
//     }
//     //ag
//     if (typeof data.opLevel == 3) {
//         sql_where.push(" customer.Upid = " + data.opCid + " ");
//     }

//     sql_where_text = (sql_where.length > 0) ? sql_where.join(" AND ") : " 1 ";

//     var sql = " SELECT COUNT(*) AS count " +
//         "	FROM (	" +
//         "	(SELECT cus.Id,cus.Cid,cus.UserName,cus.Level,cus.IsSub,cus.LType,Date_Format(cus.Date,'%Y-%m-%d %H:%i:%s') AS actionDate, 	" +
//         "	CASE WHEN cus.Level=1 THEN 'Operator' WHEN cus.Level=2 THEN 'Hall' WHEN cus.Level=3 THEN 'Agent' END AS type," +
//         "	customer.Upid,game.customer.HallId,game.customer.IsAg	" +
//         "	FROM log_customer_loginout cus	" +
//         "	LEFT JOIN game.customer ON(game.customer.Cid=cus.Cid AND cus.Level=game.customer.IsAg) ) 	" +
//         "	UNION DISTINCT	" +
//         "	(SELECT player.Id,player.Cid,player.UserName,4 AS LEVEL, 0 AS IsSub,player.LType,Date_Format(player.Date,'%Y-%m-%d %H:%i:%s') AS actionDate,'Player' AS type,	" +
//         "	customer.Upid,game.customer.HallId,game.customer.IsAg	" +
//         "	FROM log_player_loginout player	" +
//         "	LEFT JOIN game.customer ON(game.customer.Cid=player.Cid AND game.customer.IsAg=4)) 	" +
//         "	) t	WHERE " + sql_where_text;

//     var args = [];

//     db.act_query('dbclient_l_rw', sql, args, function (r_code, r_data) {
//         if (r_code.code !== code.OK) {
//             cb(null, r_code, null);
//         } else {
//             cb(null, r_code, r_data[0]['count']);
//         }
//     });
// }

/*
logDao.get_table_Info = function (msg, cb) {

    var info = [];
   //console.log('-get_table_Info msg--', msg);
    for (var i in msg) {

        var sql = " SELECT * FROM " + msg[i]['table'] + " WHERE " + msg[i]['search_key'] + "=" + msg[i]['search_value'];

        var args = [];

       //console.log('sql', sql);
        pomelo.app.get(msg[i]['db']).getConnection(function (err, connection) {
            connection.query(sql, args, function (err, res) {
                connection.release();
                if (err) {
                    cb(null, { code: code.DB.QUERY_FAIL, msg: err.stack }, null);
                } else {

                    if (res) {
                        info.push(res);
                       //console.log('-get_table_Info res--', res);
                        //  cb(null, { code: code.OK }, res[0]);
                    } else {
                        cb(null, { code: code.DB.DATA_EMPTY }, null);
                    }
                }
            });
        });
    }
   //console.log('-get_table_Info info--', info);
    cb(null, { code: code.OK }, info);
}
*/
