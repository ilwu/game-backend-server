var utils = require("../util/utils")
var code = require("../util/code")
var conf = require("../../config/js/conf")
var pomelo = require("pomelo")
var m_async = require("async")
var db = require("../util/DB")
var sprintf = require("sprintf-js").sprintf
var timezone = require("../util/timezone")

var agentAPIDao = module.exports

agentAPIDao.getHallInfo = function (data, cb) {
  var sql =
    "SELECT c.Cid,c.UserName,c.State, h.Token  FROM customer c \
               INNER JOIN hall_token h ON(c.Cid=h.HallId) \
               WHERE  c.UserName=? AND c.IsAg=2 "
  var args = [data.agentname]

  db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
    cb(null, r_code, r_data)
  })
}

agentAPIDao.getAgentInfo = function (data, cb) {
  var sql =
    "SELECT c.HallId,c.Upid,c.Cid,c.UserName,c.Currency,c.IsDemo,c.IsSingleWallet FROM customer c \
               WHERE c.HallId IN(SELECT Cid FROM customer WHERE UserName= ? AND IsAg=2 AND State='N') AND c.IsAg=3 AND c.Currency=? "

  var args = [data.agentname, data.currency]

  db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
    cb(null, r_code, r_data)
  })
}

agentAPIDao.checkPlayerIsExist = function (data, cb) {
  var sql = "SELECT count(*) AS num FROM customer c  WHERE UserName=? AND c.IsAg=4 "

  var args = [data.username]

  db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
    cb(null, r_code, r_data)
  })
}

agentAPIDao.createPlayer = function (data, cb) {
  var table = "customer"
  var saveData = data

  db.act_insert_data("dbclient_g_rw", table, saveData, function (r_code, r_data) {
    cb(null, r_code, r_data)
  })
}

agentAPIDao.getPlayerInfo = function (data, cb) {
  var sql = "SELECT Cid,State FROM customer c WHERE HallId=? AND UserName=? AND c.IsAg=4 "

  var args = [data.hall_id, data.username]

  db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
    cb(null, r_code, r_data)
  })
}

agentAPIDao.getGameList = function (data, cb) {
  console.log("param", JSON.stringify(data))
  var sql_where = []
  if (typeof data.active != "undefined") {
    if (data.active == true || data.active == "true") {
      sql_where.push(" s.Sw=1 ")
    }
    if (data.active == false || data.active == "false") {
      sql_where.push(" s.Sw=0 ")
    }
  }

  sql_where.push(" g.Sw =1 ") //只撈上架的遊戲
  sql_where.push(sprintf(" s.Cid ='%s' ", data.hall_id))

  var sql_where_text = sql_where.length == 0 ? " 1 " : sql_where.join(" AND ")

  var sql =
    "SELECT g.GameId,g.ProjectId,g.NameC,g.NameG,g.NameE, s.Sw,DATE_FORMAT(g.CreateDate ,'%Y-%m-%d %H:%i:%s') AS CreateDate,Category \
               FROM games g \
               INNER JOIN game_setting s ON(g.GameId = s.GameId) \
               INNER JOIN game_group gg ON(g.GGId = gg.GGId) WHERE " +
    sql_where_text
  var args = []
  db.act_query("dbclient_g_r", sql, args, function (r_code, r_data) {
    cb(null, r_code, r_data)
  })
}
