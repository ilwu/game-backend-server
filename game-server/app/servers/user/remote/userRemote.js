var m_async = require("async")
var code = require("../../../util/code")
var userDao = require("../../../DataBase/userDao")
var adminDao = require("../../../DataBase/adminDao")
var timezone = require("../../../util/timezone")

// var Authoritylist = [];
// var Levels = [];
var ip_check_status = false //IP確認暫close

module.exports = function (app) {
  return new UserRemote(app)
}

var UserRemote = function (app) {
  this.app = app
}

var user_remote = UserRemote.prototype
/*
玩家登入
*/
// user_remote.loginPlayer = function (data, callback) {
//     /*
//      var authData = {
//      ts:         Number(ts),
//      wl:         wl,
//      userName:   userName,
//      password:   password,
//      agent:      agent
//      };
//      */
//     var self = this;
//     var usrData = {};
//     console.log("-------------loginPlayer----------------" + JSON.stringify(data));
//     m_async.waterfall([
//         function (cb) {
//             userDao.getUserPR_byUserName_1({
//                 name: data.name,
//                 agent: data.agent,
//                 password: data.password
//             }, cb);
//         },
//         function (r_code, data, cb) {
//             console.log("--------data--------:" + JSON.stringify(data));
//             console.log("--------r_code--------:" + JSON.stringify(r_code));
//             if (r_code.code != code.OK) {
//                 cb(null, { code: r_code.code, data: null });
//                 return;
//             }

//             if (data.State === "S") {
//                 cb(null, { code: code.DB.USER_DISABLE, data: null });
//                 return;
//             }

//             if (data.State === "D") {
//                 cb(null, { code: code.USR.USR_INVALID, data: null });
//                 return;
//             }

//             if (data.State === "F") {
//                 cb(null, { code: code.DB.USER_FREEZE, data: null });
//                 return;
//             }

//             if (data.IsOnline) {
//                 cb(null, { code: code.USR.USR_LOGIN_DUPLICATE, data: null });
//                 return;
//             }

//             var curDate = new Date();

//             cb(null, { code: code.OK }, {
//                 cid: data.Cid, time: curDate.getFullYear() + '/'
//                     + curDate.getMonth() + '/'
//                     + curDate.getDate() + ' '
//                     + curDate.getHours() + ':'
//                     + curDate.getMinutes() + ':'
//                     + curDate.getSeconds()
//             });

//         }
//     ], function (none, r_code, r_data) {

//         if (r_code.code != code.OK) {
//             callback(null, { code: r_code.code });
//         }
//         callback(null, { code: code.OK }, r_data);

//     })
// };

// user_remote.joinHA = function (msg, session, next) {
//     var self = this;
//     m_async.waterfall([
//         function (r_cb) {
//             userDao.checkUsrExist(msg.data.user, r_cb);
//         },
//         function (r_code, r_cb) {
//             if (r_code.code != code.OK) {
//                 next(null, { code: r_code.code, data: null });
//                 return;
//             }
//             userDao.createUser_Hall(msg.data, r_cb);
//         },
//         function (r_code, r_cid, r_cb) {

//             var data = {
//                 user: {
//                     cid: r_cid,
//                     upid: typeof msg.data.user.upid != 'undefined' ? msg.data.user.upid : -1,
//                     hallid: typeof msg.data.user.hallid != 'undefined' ? msg.data.user.hallid : -1
//                 },
//                 games: msg.data.games
//             };
//             self.app.rpc.game.gameRemote.joinGameToHall(session, data, r_cb);
//         }], function (none, r_code) {
//             if (r_code.code != code.OK) {
//                 next(null, { code: r_code.code, data: null });
//                 return;
//             }
//             next(null, { code: code.OK, data: "Success" });
//             return;
//         })
// };

// user_remote.getUserSession = function (msg, callback) {

//     var self = this;
//     var userSession = {};
//     var lastActSec = 0;
//     var sysLimitSec = 0;
//     var hallId = 0;
//     var UserLevel = 0;
//     ip = "";
//     var sys_config = self.app.get('sys_config');
//     if (typeof msg.remoteIP != 'undefined' && msg.remoteIP.substr(0, 7) == "::ffff:") {
//         ip = msg.remoteIP.substr(7);
//     }
//     m_async.waterfall([
//         function (cb) {
//             var sessionService = self.app.get('backendSessionService');
//             var connectors = self.app.getServersByType('connector');

//             // console.log('----------------connectors------------', connectors)
//             // console.log('----------------sessionService------------', sessionService)
//             for (var i in connectors) {
//                 sessionService.getByUid(connectors[i].id, msg.uid, function (err, sessions) {
//                     //    console.log('----------------session------------', sessions)
//                     if (!!sessions) {
//                         var session = sessions[0];

//                         if (typeof session.uid != 'undefined') {
//                             cb(null, session);
//                             return;
//                         } else {
//                             //    console.log('--- no session ---- ')
//                         }
//                     } else {
//                         //   console.log('--- err session ---- ')
//                     }
//                 });
//             }
//         }, function (session, cb) {
//             userSession = session;

//             if (+userSession.get('level') == 1) {
//                 UserLevel = 1;
//                 hallId = userSession.get('cid');
//             } else {
//                 UserLevel = 2;
//                 if (+userSession.get('level') == 3) {
//                     hallId = userSession.get('hallId');
//                 } else {
//                     hallId = (userSession.get('isSub') == 1) ? userSession.get('hallId') : userSession.get('cid');
//                 }
//             }

//             var param = {
//                 userId: userSession.get('cid'),
//                 isAdmin: UserLevel
//             }

//             //找user狀態
//             if (userSession.get('level') == 1) {
//                 adminDao.getUserOnlineState(param, cb);
//             } else {
//                 userDao.getUserOnlineState(param, cb);
//             }
//         }, function (r_code, r_data, cb) {
//             if (r_code.code != code.OK || r_data.length == 0) {
//                 kickUser_byId(userSession, self);
//                 return;
//             } else {

//                 cb(null, { code: code.OK }, null);

//             }
//         }, function (r_code, r_data, cb) {
//             var param = {
//                 cid: userSession.get('cid')
//             }
//             if (userSession.get('level') > 1) {
//                 userDao.getUserState(param, cb); //判斷USER狀態及上線狀態
//             } else {
//                 cb(null, { code: code.OK }, null);
//             }
//         }, function (r_code, r_data, cb) {
//             if (r_code.code != code.OK) {
//                 callback(null, { code: code.USR.USER_DISABLE, data: null });
//                 return;
//             }
//             if (userSession.get('level') > 1) { //hall , agent
//                 if (r_data.cusState == 'S') {
//                     kickUser_byId(userSession, self);

//                 }
//                 if ((+userSession.get('level') == 3 || (userSession.get('isSub') == 1)) && r_data.haState == 'S') {

//                     kickUser_byId(userSession, self);

//                 }
//             }

//             var ip_list = {
//                 UserLevel: UserLevel,
//                 Cid: hallId,
//                 IpType: 1,
//                 Ip: ip,
//                 State: 1
//             }
//             userDao.checkWhiteIp(ip_list, cb);

//         }, function (r_code, r_data, cb) {
//             //IP-非白名單
//             if (r_code.code != code.OK && ip_check_status) {
//                 callback(null, { code: code.USR.USER_IP_FAIL, data: null });
//                 return;
//             }
//             //取user上次執行時間
//             if (+userSession.get('level') === 1) {
//                 adminDao.getActionTime_2(userSession.get('cid'), cb); //admin
//             } else {
//                 userDao.getActionTime_2(userSession.get('cid'), cb);
//             }
//         }, function (r_code, r_time, cb) {
//             if (r_code.code != code.OK) {
//                 callback(null, { code: code.USR.GET_ACTION_TIME_FAIL, data: null });
//                 return;
//             }

//             lastActSec = timezone.getTimeStamp(r_time)/1000; //美東時間 轉成timestamp/1000 (秒)

//         //     self.app.rpc.config.configRemote.getSysLimitTime(userSession, cb); //取系統執行時間
//         // }, function (r_code, r_data, cb) {

//           //  console.log('-------------------sys_config-----------------------',JSON.stringify(sys_config));
//             sysLimitSec = sys_config.timeout_range_sec;

//             var nowSec = timezone.getTimeStamp() / 1000; //UTC timestamp

//             console.log('lastActSec--------', lastActSec );
//             console.log('nowSec--------', nowSec );
//             console.log('sysLimitSec--------', sysLimitSec );
//             if (sysLimitSec < (nowSec - lastActSec)) { //timeout  秒數

//                 var logoutUser = {
//                     Cid: userSession.get('cid'),
//                     UserName: userSession.get('usrName'),
//                     Level: userSession.get('level'),
//                     IsSub: (+userSession.get('level') == 1) ? 0 : userSession.get('isSub'),
//                     LType: "OUT",
//                     LDesc: "logout",
//                     IP: msg.remoteIP
//                 }
//                 self.app.rpc.log.logRemote.set_user_logout(userSession, logoutUser, cb);
//             } else {
//                 cb(null, { code: code.OK });
//             }

//         }, function (r_code, cb) {

//             if (r_code.code != code.OK) {
//                 callback(null, { code: r_code.code, data: null });
//                 return;
//             }
//             if (r_code.code == code.TIMEOUT) { //登出
//                 callback(null, { code: r_code.code, data: null });
//                 return;
//             }
//             //console.log('--- userSession level', +userSession.get('level'));
//             //-------------------------------------------------------

//             if (+userSession.get('level') === 1) {
//                 adminDao.setActionTime(userSession.get('cid'), cb); //admin
//             } else {
//                 userDao.setActionTime(userSession.get('cid'), cb);
//             }
//         }], function (r_none, r_code) {

//             if (r_code.code != code.OK) {
//                 callback(null, { code: code.USR.ACTION_TIME_FAIL, data: null });
//                 return;
//             }
//             var session = {
//                 id: userSession.id,
//                 frontendId: userSession.frontendId,
//                 uid: userSession.uid
//             }
//             callback(null, { code: r_code.code }, session);
//             return;

//         });
// }
// function kickUser_byId(session, self) {

//     //改為isonline=0 踢出
//     m_async.waterfall([
//         function (cb) {
//             //關閉
//             var param = {
//                 userId: session.get('cid'),
//                 isOnline: 0
//             }
//             if (session.get('level') == 1) { //admin
//                 adminDao.modifyAdminOnlineState( param, cb);
//             } else {
//                 userDao.modifyUserOnlineState(param, cb);
//             }
//         }], function (none, r_code, r_data) {
//             var rid = session.get('cid') + "_" + session.get('usrName');
//             var channel = self.app.get('channelService').getChannel(rid, true);

//             if (!!channel) {
//                 channel.add(rid, session.frontendId);
//             }
//             var param = {
//                 route: 'onKick',
//                 code: code.USR.UP_USER_DISABLE
//             };
//             channel.pushMessage(param);
//         });
// }
