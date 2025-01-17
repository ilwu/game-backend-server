var logger = require("pomelo-logger").getLogger("logRemote", __filename)
var code = require("../../../util/code")
var m_async = require("async")
var logDao = require("../../../DataBase/logDao")
var pomelo = require("pomelo")

module.exports = function (app) {
  return new logRemote(app)
}

var logRemote = function (app) {
  this.app = app
}

var log_remote = logRemote.prototype

// log_remote.add_log_admin = function (msg, cb) {

//     if (typeof msg === 'undefined') {
//         cb(null, { code: code.FAIL, msg: null }, null);
//         return;
//     }
//     logDao.add_log_admin(msg, function (none, r_code, r_data) {
//         //console.log('--------add_log_admin cb : ', JSON.stringify(none), JSON.stringify(r_code), JSON.stringify(r_data));
//         cb(null, { code: r_code.code, msg: null }, r_data);
//         return;
//     });
// };

// log_remote.add_log_customer = function (msg, cb) {

//     if (typeof msg === 'undefined') {
//         cb(null, { code: code.FAIL, msg: null }, null);
//         return;
//     }
//     logDao.add_log_customer(msg, function (none, r_code, r_data) {
//         cb(null, { code: r_code.code, msg: null }, r_data);
//         return;
//     });
// };

// log_remote.add_loginout_log = function (msg, cb) {
//     if (typeof msg === 'undefined') {
//         cb(null, { code: code.FAIL, msg: null }, null);
//         return;
//     }
//     logDao.add_loginout_log(msg, function (none, r_code, r_data) {
//         cb(null, { code: r_code.code, msg: null }, r_data);
//         return;
//     });
// };

// log_remote.set_user_logout = function (msg, cb) { //登出
//     var self = this;
//     if (typeof msg === 'undefined') {
//         cb(null, { code: code.FAIL, msg: null });
//     }

//     logDao.add_loginout_log(msg, function (none, r_code, r_data) {

//         //後面session 關閉
//         //channel 踢出

//         if (r_code.code != code.OK) {
//             cb(null, { code: r_code.code, data: null });
//         }
//         cb(null, { code: code.TIMEOUT, data: null });

//     });
// }

//玩家log
// log_remote.add_log_player = function (msg, cb) {

//     if (typeof msg === 'undefined') {
//         cb(null, { code: code.FAIL, msg: null }, null);
//         return;
//     }
//     logDao.add_log_player(msg, cb);
// };

//最後登入時間
log_remote.getUserLastLogin = function (msg, cb) {
  try {
    if (typeof msg === "undefined") {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }
    logDao.getUserLastLogin(msg, cb)
  } catch (err) {
    logger.error("[log_remote][getUserLastLogin] catch err", err)
    cb(null, code.FAIL, null)
  }
}

log_remote.getOnlinePlayers = function (msg, cb) {
  try {
    if (typeof msg === "undefined") {
      cb(null, { code: code.FAIL, msg: null }, null)
      return
    }
    logDao.getOnlinePlayers(msg, cb)
  } catch (err) {
    logger.error("[log_remote][getOnlinePlayers] catch err", err)
    cb(null, code.FAIL, null)
  }
}
