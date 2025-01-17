//var module_usrDB = require('../../../DataBase/user_db');
const db_member = require("../../../DataBase/userDao")
var code = require("../../../util/code")
const async = require("async")
const crypto = require("crypto")

module.exports = function (app) {
  return new AuthRemote(app)
}

var AuthRemote = function (app) {
  this.app = app
  //this.channelService = app.get('channelService');
}

var authRemote = AuthRemote.prototype

//取得會員資料 for app登入用
/*
authRemote.logIn = function( name, password, cb ){
   ////console.log("--------------------name:"+name+"-------password:"+password);
    module_usrDB.getUser( name, password, function( err, usr ){
        if( err.code != code.OK ){
            cb(null,err,null);
            return;
        }

        cb(null,{code:code.OK}, usr );
    } );

};
//取得會員資料 for 驗證用
authRemote.getUsrById = function( usrId, cb ){

    module_usrDB.getUsrById( usrId, function( err, usr ){
        if( err.code != code.OK ){
            cb(null,err,null);
            return;
        }

        cb(null,{code:code.OK}, usr );
    } );

};
//創建auth id
authRemote.createAuthId = function( usrId, cd ){
    //console.log("----------createAuthId-----------");
    module_usrDB.createAuthId( usrId, function( err, authId ){
        if( err.code != code.OK ){
            cd(null,err,null);
            return;
        }

        cd(null,{code:code.OK}, authId );
    } );
};
//更新auth id
authRemote.updateAuthId = function( usrId, cd  ){
    module_usrDB.updateAuthId( usrId, function( err, authId ){
        if( err.code != code.OK ){
            cd(null,err,null);
            return;
        }

        cd(null,{code:code.OK}, authId );
    } );
};
*/
// authRemote.authToken = function (wl, auth_token, auth_data, cb) {

//    //console.log("--------wl----------:" + wl);
//    console.log("--------auth_token----------:" + auth_token);
//    console.log("--------auth_data----------:" + JSON.stringify(auth_data));

//     var typeWl = typeof wl;
//    //console.log("--------typeWl----------:" + typeWl);
//     if (typeWl === 'undefined' || typeWl != 'string') {
//         cb(null, { code: code.AUTH.WL_INVALID });
//         return;
//     }

//     async.waterfall([
//         function (cb) {
//             // auth aid
//             var queryData = {
//                 wl: wl
//             };
//             db_member.getSecureKey(queryData, cb);
//         }, function (r_err, r_securekey, r_cb) {

//             // query player info by user id
//             if (r_err.code != code.OK) {
//                 r_cb(null, { code: r_err.code });
//                 return;
//             }

//             var targetToken = crypto.createHash('SHA256').update(auth_data + r_securekey.SecureKey, 'utf8').digest('hex');

//            console.log("------------wl-------:" + wl);
//            console.log("------------targetToken-------:" + targetToken);
//            console.log("------------auth_token-------:" + auth_token);

//             if (targetToken != auth_token) {
//                //console.log("------------fail-------", code.AUTH.AUTH_INVALID);
//                 r_cb(null, { code: code.AUTH.AUTH_INVALID });
//                 return;
//             } else {
//                //console.log("------------success-------");
//                 r_cb(null, { code: code.OK });
//                 return;
//             }
//         }

//     ], function (none, err) {
//        //console.log('--------authToken--------->', JSON.stringify(none), JSON.stringify(err));
//         cb(null, { code: err.code });
//     });

// };

// authRemote.createPlayerToken = function (data, cb) {

//     var md5 = crypto.createHash('md5');
//    //console.log("--------createPlayerToken-------------:" + JSON.stringify(data));
//     var message = JSON.stringify(data);
//     var token = md5.update(message, 'utf8').digest('hex');

//    //console.log(token);
//     cb(null, { code: code.OK }, token);

// };

//建立redis token
// authRemote.getHash = function (wl, token, cb) {
//     async.waterfall([
//         function (cb) {
//             // auth aid
//             var queryData = {
//                 wl: wl
//             };
//             db_member.getSecureKey(queryData, cb);
//         },
//         function (r_err, r_securekey, r_cb) {

//             // query player info by user id
//             if (r_err.code != code.OK) {
//                 r_cb(null, {
//                     code: r_err.code
//                 },null);
//             }
//             var targetToken = crypto.createHash('SHA256').update(token + r_securekey.SecureKey, 'utf8').digest('hex');
//             console.log('targetToken', targetToken);
//             r_cb(null, {
//                 code: code.OK
//             },targetToken);
//         }
//     ], function (none, err,data) {
//         cb(null,err,data);
//     });
// };
