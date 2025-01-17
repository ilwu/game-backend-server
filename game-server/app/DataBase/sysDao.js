var logger = require("pomelo-logger").getLogger("sysDao", __filename)
var utils = require("../util/utils")
var code = require("../util/code")
var conf = require("../../config/js/conf")
var db = require("../util/DB")
var pomelo = require("pomelo")
var sprintf = require("sprintf-js").sprintf
var timezone = require("../util/timezone")

var sysDao = module.exports

//-------------------------------遊戲公司------------------------------------------------
sysDao.getGameCompanyList = function (data, cb) {
  try {
    var sql_limit = sprintf("LIMIT %i,%i", (data.page - 1) * data.pageCount, data.pageCount)
    var sql = sprintf("SELECT SQL_CALC_FOUND_ROWS Id,Value FROM game_company WHERE 1 ORDER BY Id ASC %s ", sql_limit)

    db.act_info_rows("dbclient_g_r", sql, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][getGameCompanyList] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//判斷是否有資料
sysDao.checkGameCompanyIsExist = function (data, cb) {
  try {
    var sql_where = []

    if (data.type == 1) {
      if (typeof data.company == "undefined" || data.company == "") {
        cb(null, {
          code: code.DB.PARA_FAIL,
          data: null,
        })
        return
      }
      sql_where.push(" Value = '" + data.company + "'")
    }

    if (data.type == 2) {
      if (typeof data.Id == "undefined" || data.Id == "") {
        cb(null, {
          code: code.DB.PARA_FAIL,
          data: null,
        })
        return
      }
      sql_where.push(" Id = '" + data.Id + "' ")
    }

    if (data.type == 3) {
      if (typeof data.Id == "undefined" || data.Id == "" || typeof data.company == "undefined" || data.company == "") {
        cb(null, {
          code: code.DB.PARA_FAIL,
          data: null,
        })
        return
      }
      sql_where.push(" (Id != '" + data.Id + "' AND Value = '" + data.company + "') ")
    }

    var sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : "1"

    var sql = "SELECT * FROM game_company WHERE " + sql_where_text
    db.act_query("dbclient_g_r", sql, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][checkGameCompanyIsExist] catch err", err)
    cb(null, code.FAIL, null)
  }
}

sysDao.createGameCompany = function (data, cb) {
  try {
    var table = "game_company"
    var saveData = {
      Value: data.company,
    }
    db.act_insert_data("dbclient_g_rw", table, saveData, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][createGameCompany] catch err", err)
    cb(null, code.FAIL, null)
  }
}

sysDao.modifyGameCompany = function (data, cb) {
  try {
    var table = "game_company"
    var saveData = {
      Value: data.company,
    }
    var sql_where_text = sprintf(" Id= '%s'", data.Id)
    db.act_update_data("dbclient_g_rw", table, saveData, sql_where_text, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][modifyGameCompany] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//--------------------------------RTP------------------------------------------------
sysDao.getGameRtpList = function (data, cb) {
  try {
    var sql_limit = sprintf("LIMIT %i,%i", (data.page - 1) * data.pageCount, data.pageCount)
    var sql = sprintf("SELECT SQL_CALC_FOUND_ROWS Id,Value FROM game_rtp WHERE 1 ORDER BY Id ASC %s", sql_limit)

    db.act_info_rows("dbclient_g_r", sql, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][getGameRtpList] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//判斷是否有資料
sysDao.checkGameRtpIsExist = function (data, cb) {
  try {
    var sql_where = []

    if (data.type == 1) {
      if (typeof data.rtp == "undefined" || data.rtp == "") {
        cb(null, {
          code: code.DB.PARA_FAIL,
          data: null,
        })
        return
      }
      sql_where.push(" Value = '" + data.rtp + "'")
    }

    if (data.type == 2) {
      if (typeof data.Id == "undefined" || data.Id == "") {
        cb(null, {
          code: code.DB.PARA_FAIL,
          data: null,
        })
        return
      }
      sql_where.push(" Id = '" + data.Id + "' ")
    }

    if (data.type == 3) {
      if (typeof data.Id == "undefined" || data.Id == "" || typeof data.rtp == "undefined" || data.rtp == "") {
        cb(null, {
          code: code.DB.PARA_FAIL,
          data: null,
        })
        return
      }
      sql_where.push(" (Id != '" + data.Id + "' AND Value = '" + data.rtp + "') ")
    }

    var sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : "1"

    var sql = "SELECT * FROM game_rtp WHERE " + sql_where_text
    db.act_query("dbclient_g_r", sql, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][checkGameRtpIsExist] catch err", err)
    cb(null, code.FAIL, null)
  }
}

sysDao.createGameRtp = function (data, cb) {
  try {
    var table = "game_rtp"
    var saveData = {
      Value: data.rtp,
    }
    db.act_insert_data("dbclient_g_rw", table, saveData, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][createGameRtp] catch err", err)
    cb(null, code.FAIL, null)
  }
}

sysDao.modifyGameRtp = function (data, cb) {
  try {
    var table = "game_rtp"
    var saveData = {
      Value: data.rtp,
    }
    var sql_where_text = sprintf(" Id= '%s'", data.Id)

    db.act_update_data("dbclient_g_rw", table, saveData, sql_where_text, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][modifyGameRtp] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//----------------------------------遊戲種類----------------------------------------------

sysDao.getGameGroupList = function (data, cb) {
  try {
    var sql_limit = sprintf("LIMIT %i,%i", (data.page - 1) * data.pageCount, data.pageCount)
    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS GGId AS ggId,NameC AS nameC,NameG AS nameG,NameE AS nameE,NameVN AS nameVN,NameTH AS nameTH,NameID AS nameID,NameMY AS nameMY,NameJP AS nameJP,NameKR AS nameKR FROM game_group WHERE 1 ORDER BY GGId ASC %s",
      sql_limit
    )

    db.act_info_rows("dbclient_g_r", sql, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][getGameGroupList] catch err", err)
    cb(null, code.FAIL, null)
  }
}
//判斷是否有資料
sysDao.checkGameGroupIsExist = function (data, cb) {
  try {
    var sql_where = []

    if (data.type == 2) {
      if (typeof data.ggId == "undefined" || data.ggId == "") {
        cb(null, {
          code: code.DB.PARA_FAIL,
          data: null,
        })
        return
      }
      sql_where.push(" GGId = '" + data.ggId + "' ")
    }

    var sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : "1"
    var sql = "SELECT * FROM game_group WHERE " + sql_where_text

    db.act_query("dbclient_g_r", sql, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][checkGameGroupIsExist] catch err", err)
    cb(null, code.FAIL, null)
  }
}
// 新增遊戲種類
sysDao.createGameGroup = function (data, cb) {
  try {
    var table = "game_group"
    var saveData = {
      NameE: data.nameE,
      NameG: data.nameG,
      NameC: data.nameC,
      NameVN: data.nameVN,
      NameTH: data.nameTH,
      NameID: data.nameID,
      NameMY: data.nameMY,
      NameJP: data.nameJP,
      NameKR: data.nameKR,
      Type: data.typeId,
    }

    db.act_insert_data("dbclient_g_rw", table, saveData, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][createGameGroup] catch err", err)
    cb(null, code.FAIL, null)
  }
}
// 寫入遊戲種類修改
sysDao.modifyGameGroup = function (data, cb) {
  try {
    var table = "game_group"
    var saveData = {
      NameE: data.nameE,
      NameG: data.nameG,
      NameC: data.nameC,
      NameVN: data.nameVN,
      NameTH: data.nameTH,
      NameID: data.nameID,
      NameMY: data.nameMY,
      NameJP: data.nameJP,
      NameKR: data.nameKR,
      Type: data.typeId,
    }
    var sql_where_text = sprintf(" GGId= '%s'", data.ggId)
    db.act_update_data("dbclient_g_rw", table, saveData, sql_where_text, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][modifyGameGroup] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//-----------------------------------類型---------------------------------------------
sysDao.getGameTypeList = function (data, cb) {
  try {
    var sql_limit = sprintf("LIMIT %i,%i", (data.page - 1) * data.pageCount, data.pageCount)
    // var sql = sprintf("SELECT SQL_CALC_FOUND_ROWS Id, Value AS name,Type AS typeId FROM game_type WHERE 1 ORDER BY Id ASC %s",sql_limit);
    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS t.Id, t.Value AS name, t.Type AS typeId, g.nameC, g.nameG, g.nameE FROM game_type t " +
        "LEFT JOIN game_group g ON g.GGId = t.Type WHERE 1 ORDER BY Id ASC %s",
      sql_limit
    )
    db.act_info_rows("dbclient_g_r", sql, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][getGameTypeList] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//判斷是否有資料
sysDao.checkGameTypeIsExist = function (data, cb) {
  try {
    var sql_where = []

    if (data.type == 1) {
      if (typeof data.name == "undefined" || data.name == "") {
        cb(null, {
          code: code.DB.PARA_FAIL,
          data: null,
        })
        return
      }
      sql_where.push(" Value = '" + data.name + "'")
    }
    if (data.type == 2) {
      if (typeof data.Id == "undefined" || data.Id == "") {
        cb(null, {
          code: code.DB.PARA_FAIL,
          data: null,
        })
        return
      }
      sql_where.push(" Id = '" + data.Id + "' ")
    }
    if (data.type == 3) {
      if (typeof data.Id == "undefined" || data.Id == "" || typeof data.name == "undefined" || data.name == "") {
        cb(null, {
          code: code.DB.PARA_FAIL,
          data: null,
        })
        return
      }
      sql_where.push(" (Id != '" + data.Id + "' AND Value = '" + data.name + "') ")
    }

    var sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : "1"
    var sql = "SELECT * FROM game_type WHERE " + sql_where_text

    db.act_query("dbclient_g_r", sql, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][checkGameTypeIsExist] catch err", err)
    cb(null, code.FAIL, null)
  }
}

sysDao.createGameType = function (data, cb) {
  try {
    var table = "game_type"
    var saveData = {
      Value: data.name,
      Type: data.typeId,
    }

    db.act_insert_data("dbclient_g_rw", table, saveData, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][createGameType] catch err", err)
    cb(null, code.FAIL, null)
  }
}

sysDao.modifyGameType = function (data, cb) {
  try {
    var table = "game_type"
    var saveData = {
      Value: data.name,
      Type: data.typeId,
    }
    var sql_where_text = sprintf(" Id= '%s'", data.Id)

    db.act_update_data("dbclient_g_rw", table, saveData, sql_where_text, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][modifyGameType] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//-----------------------------------幣別---------------------------------------------
sysDao.getCurrencyList = function (data, cb) {
  try {
    var sql_limit = sprintf("LIMIT %i,%i", (data.page - 1) * data.pageCount, data.pageCount)
    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS Currency AS currency,IFNULL(`Desc`,'') AS `desc`, symbolCode AS symbolCode, showK AS showK FROM currency WHERE 1 ORDER BY Currency ASC %s",
      sql_limit
    )

    db.act_info_rows("dbclient_g_r", sql, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][getCurrencyList] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//判斷是否有資料
sysDao.checkCurrencyIsExist = function (data, cb) {
  try {
    var sql_where = []

    if (data.type == 1) {
      if (typeof data.currency == "undefined" || data.currency == "") {
        cb(null, {
          code: code.DB.PARA_FAIL,
          data: null,
        })
        return
      }
      sql_where.push(" Currency = '" + data.currency + "'")
    }

    var sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : "1"
    var sql = "SELECT * FROM currency WHERE " + sql_where_text

    db.act_query("dbclient_g_r", sql, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][checkCurrencyIsExist] catch err", err)
    cb(null, code.FAIL, null)
  }
}

sysDao.createCurrency = function (data, cb) {
  try {
    var table = "currency"
    var saveData = {
      Currency: data.currency,
      Desc: typeof data.desc == "undefined" ? "" : data.desc,
      symbolCode: data.symbolCode,
      showK: data.showK ? 1 : 0,
    }

    db.act_insert_data("dbclient_g_rw", table, saveData, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][createCurrency] catch err", err)
    cb(null, code.FAIL, null)
  }
}

sysDao.modifyCurrency = function (data, cb) {
  try {
    var table = "currency"
    var saveData = {
      Currency: data.new_currency,
      Desc: typeof data.desc == "undefined" ? "" : data.desc,
      symbolCode: data.symbolCode,
      showK: data.showK ? 1 : 0,
    }
    var sql_where_text = sprintf(" Currency = '%s'", data.currency)
    db.act_update_data("dbclient_g_rw", table, saveData, sql_where_text, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][modifyCurrency] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//--------------------------------Denom------------------------------------------------
sysDao.getGameDenomList = function (data, cb) {
  try {
    var sql_limit = sprintf("LIMIT %i,%i", (data.page - 1) * data.pageCount, data.pageCount)
    var sql = sprintf("SELECT SQL_CALC_FOUND_ROWS Id,Value FROM game_denom WHERE 1 ORDER BY Id ASC %s", sql_limit)
    db.act_info_rows("dbclient_g_r", sql, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][getGameDenomList] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//判斷是否有資料
sysDao.checkGameDenomIsExist = function (data, cb) {
  try {
    var sql_where = []

    if (data.type == 1) {
      if (typeof data.denom == "undefined" || data.denom == "") {
        cb(null, {
          code: code.DB.PARA_FAIL,
          data: null,
        })
        return
      }
      sql_where.push(" Value = '" + data.denom + "'")
    }

    if (data.type == 2) {
      if (typeof data.Id == "undefined" || data.Id == "") {
        cb(null, {
          code: code.DB.PARA_FAIL,
          data: null,
        })
        return
      }
      sql_where.push(" Id = '" + data.Id + "' ")
    }

    if (data.type == 3) {
      if (typeof data.Id == "undefined" || data.Id == "" || typeof data.denom == "undefined" || data.denom == "") {
        cb(null, {
          code: code.DB.PARA_FAIL,
          data: null,
        })
        return
      }
      sql_where.push(" (Id != '" + data.Id + "' AND Value = '" + data.denom + "') ")
    }

    var sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : "1"

    var sql = "SELECT * FROM game_denom WHERE " + sql_where_text

    db.act_query("dbclient_g_r", sql, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][checkGameDenomIsExist] catch err", err)
    cb(null, code.FAIL, null)
  }
}

sysDao.createGameDenom = function (data, cb) {
  try {
    var table = "game_denom"
    var saveData = {
      Value: data.denom,
    }
    db.act_insert_data("dbclient_g_rw", table, saveData, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][createGameDenom] catch err", err)
    cb(null, code.FAIL, null)
  }
}

sysDao.modifyGameDenom = function (data, cb) {
  try {
    var table = "game_denom"
    var saveData = {
      Value: data.denom,
    }
    var sql_where_text = sprintf(" Id= '%s'", data.Id)

    db.act_update_data("dbclient_g_rw", table, saveData, sql_where_text, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][modifyGameDenom] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//----------------------------------------------各幣別預設Denom--------------------------------------------------------------------

sysDao.getGameDefaultDenomList = function (data, cb) {
  try {
    var sql_limit = sprintf("LIMIT %i,%i", (data.page - 1) * data.pageCount, data.pageCount)
    var sql = sprintf(
      "SELECT SQL_CALC_FOUND_ROWS Currency AS currency,Denom AS denom FROM game_default_currency_denom WHERE 1 ORDER BY Currency ASC %s",
      sql_limit
    )
    db.act_info_rows("dbclient_g_r", sql, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][getGameDefaultDenomList] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//判斷是否有資料
sysDao.checkGameDefaultDenomIsExist = function (data, cb) {
  try {
    var sql_where = []

    if (data.type == 1) {
      if (typeof data.currency == "undefined" || data.currency == "") {
        cb(null, {
          code: code.DB.PARA_FAIL,
          data: null,
        })
        return
      }
      sql_where.push(" Currency = '" + data.currency + "'")
    }

    var sql_where_text = sql_where.length > 0 ? sql_where.join(" AND ") : "1"
    var sql = "SELECT * FROM game_default_currency_denom WHERE " + sql_where_text

    db.act_query("dbclient_g_r", sql, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][checkGameDefaultDenomIsExist] catch err", err)
    cb(null, code.FAIL, null)
  }
}

sysDao.createGameDefaultDenom = function (data, cb) {
  try {
    var table = "game_default_currency_denom"
    var saveData = {
      Currency: data.currency,
      Denom: data.denom.join(","),
    }

    db.act_insert_data("dbclient_g_rw", table, saveData, function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][createGameDefaultDenom] catch err", err)
    cb(null, code.FAIL, null)
  }
}

sysDao.modifyGameDefaultDenom = function (data, cb) {
  try {
    var table = "game_default_currency_denom"
    var saveData = {
      Denom: data.denom.join(","),
    }
    var sql_where_text = sprintf(" Currency= '%s'", data.currency)

    db.act_update_data("dbclient_g_rw", table, saveData, sql_where_text, [], function (r_code, r_data) {
      cb(null, r_code, r_data)
    })
  } catch (err) {
    logger.error("[sysDao][modifyGameDefaultDenom] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//----------------------------------------------區域網段--------------------------------------------------------------------

// 取得區域網段
// data: {ip, useMaster}
// useMaster: 是否使用 master (寫修改後的 log 時, 若取 slave 的資料時, 可能 slave 還沒寫入, 此時指定它使用 master)
// 回傳 [{Ip, Country, State}]
sysDao.ipCountryGet = function (data, cb) {
  try {
    let sqlWhere = " WHERE Ip = ?"
    let argsWhere = [data.ip]

    let sql = "SELECT Ip, Country, State FROM game.ip_country" + sqlWhere
    let args = [...argsWhere]

    db.act_query(data.useMaster ? "dbclient_g_rw" : "dbclient_g_r", sql, args, function (r_code, r_data) {
      if (r_code.code !== code.OK) {
        cb(null, r_code, null)
      } else {
        cb(null, r_code, r_data)
      }
    })
  } catch (err) {
    logger.error("[sysDao][ipCountryGet] err: ", err)
    cb(null, { code: code.FAIL }, null)
  }
}
// 取得區域網段列表
// data: {pageCur, pageCount}
// 回傳 {count, list: [Ip, Country, State]}
sysDao.ipCountryList = function (data, cb) {
  try {
    let sqlOrder = " ORDER BY UpdateTime DESC"

    let sqlLimit = " LIMIT ?,?"
    let argsLimit = [(data.pageCur - 1) * data.pageCount, data.pageCount]

    let sql = "SELECT Ip, Country, State FROM game.ip_country" + sqlOrder + sqlLimit
    let args = [...argsLimit]

    let sql2 = "SELECT COUNT(Ip) AS ROWS FROM game.ip_country"
    let args2 = []

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
    logger.error("[sysDao][ipCountryList] err: ", err)
    cb(null, { code: code.FAIL }, null)
  }
}
// 新增區域網段
// data: {ip, ipType, ipStart, ipEnd, country, state}
sysDao.ipCountryAdd = function (data, cb) {
  try {
    let now = timezone.serverTime()

    let saveData = {
      Ip: data.ip,
      IpType: data.ipType,
      IpStart: data.ipStart,
      IpEnd: data.ipEnd,
      Country: data.country,
      State: data.state,
      UpdateTime: now,
    }

    db.act_insert_data("dbclient_g_rw", "game.ip_country", saveData, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[sysDao][ipCountryAdd] err: ", err)
    cb(null, { code: code.FAIL })
  }
}
// 修改區域網段
// data: {ip, country, state}
sysDao.ipCountryMod = function (data, cb) {
  try {
    let now = timezone.serverTime()

    let sqlWhere = "Ip = ?"
    let argsWhere = [data.ip]
    let saveData = {
      Country: data.country,
      State: data.state,
      UpdateTime: now,
    }

    db.act_update_data("dbclient_g_rw", "game.ip_country", saveData, sqlWhere, argsWhere, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[sysDao][ipCountryMod] err: ", err)
    cb(null, { code: code.FAIL })
  }
}
// 刪除區域網段
// data: {ip}
sysDao.ipCountryDel = function (data, cb) {
  try {
    let sqlWhere = " WHERE Ip = ?"
    let argsWhere = [data.ip]

    let sql = "DELETE FROM game.ip_country" + sqlWhere
    let args = [...argsWhere]

    db.act_query("dbclient_g_rw", sql, args, function (r_code, r_data) {
      cb(null, r_code)
    })
  } catch (err) {
    logger.error("[sysDao][ipCountryDel] err: ", err)
    cb(null, { code: code.FAIL })
  }
}
