//var memberSchemas     = require('../../db/schemas/memberSchemas');
//var mongoose          = require('mongoose');
//var memberModule      = mongoose.model('members');

var async = require("async")
var code = require("../../../util/code")

module.exports = function (app) {
  return new Handler(app)
}

var Handler = function (app) {
  this.app = app
}

var handler = Handler.prototype

/**
 * 進入遊戲時檢查auth id 碼.
 *
 * @param  {Object}   msg     request message
 * @param  {Object}   session current session object 前端Session  - FrontendSession
 * @param  {Function} next    next stemp callback
 * @return {Void}
 */

handler.enter = function (msg, session, next) {
  console.log("----------------------")
  console.log("-------enter----------")
  console.log("----------------------")
  console.log("----------------------" + JSON.stringify(msg))

  var self = this
  var rid = msg.rid //不知名ＩＤ
  var aid = msg.authId //驗證ID
  var uid = msg.uid //使用者ＩＤ
  var agentid = "" //業者ＩＤ

  var sessionService = self.app.get("sessionService")

  /*
    if (!aid) {       
        next(new Error('invalid entry request: empty aid'), { code: code.FAIL });
        return;
    }
    */

  //duplicate log in
  if (!!sessionService.getByUid(uid)) {
    next(null, {
      code: 500,
      error: true,
    })
    return
  }

  session.set("rid", rid)
  //session.bind(uid);
  session.pushAll()
  console.log("--------enter uid-----------", uid, sessionService.getByUid(uid))
  session.on("closed", onUserLeave.bind(null, self.app))
  next(null, { code: code.OK, player: null })
  /*
        var usrInfo = {};
        async.waterfall([
                function( cb ){
                    self.app.rpc.auth.authRemote.getUsrById( session, uid, cb );
                },function( err, user, cb ) {
                    // query player info by user id
                    if( err.code != code.OK ) {
                        next(null, { code: err.code });
                        return;
                    }
    
                    if(!user) {
                        next(null, { code: err.code });
                        return;
                    }
    
                    usrInfo = user;
    
                    if( typeof usrInfo.authData === "undefined" ){
                        next(null, { code: code.AUTH.AUTH_FAIL });
                        return;
                    }else if( usrInfo.authData.auth_id != aid ){
                        next(null, { code: code.AUTH.AUTH_FAIL });
                        return;
                    }else{
    
                        if( usrInfo.ancestorsId.length > 1 ){
                            agentid = usrInfo.ancestorsId[1];
                        }
                       //console.log("------------location-----start-------");
                        //cb({code:code.OK});
                        self.app.rpc.gameLogic.gameLogicRemote.getGameLocation( session, gid, rtpid, agentid, cb );
                    }
    
                }, function( err, location, cb ) {
                   //console.log("------------location-----callback-------"+JSON.stringify(err));
                    if( err.code != code.OK ){
                        next( { code: err.code });
                        return;
                    }
    
                   //console.log("------------location------------game:"+location.game_location+"---------agent:"+location.agent_location);
    
                    session.bind( uid );
                    session.set('rid', rid);
                    session.set( 'gid', gid );
                    session.set( 'aid', aid );
                    session.set( 'agentid', agentid );
                    session.set( 'amt', usrInfo.balance );
                    session.set( 'gamelocation', location.game_location );
                    session.set( 'agentlocation', location.agent_location );
                    session.set( 'coinin', 0 );
                    session.set( 'coinout', 0 );
                    session.set( 'denom', '1:1' );
                    session.set( 'credit', usrInfo.balance );
                    session.set( 'parentId', usrInfo.parentId );
                    session.set( 'ancestors', JSON.stringify(usrInfo.ancestorsId) );
                    session.set( 'freegame', JSON.stringify({
                        triggers:0,
                        counts:[],
                        wins:0
                    }) );
                    session.on('closed', onUserLeave.bind(null, self.app));
                    session.pushAll(cb);
    
                    //self.app.rpc.gameLogic.gameLogicRemote.add( session, uid, self.app.get('serverId'), rid, true,  cb );
                    //cb({code:code.OK});
                },function( cb ){
                    //if( err.code != code.OK ){
                        //console.log()
                    //    next( { code: code.FAIL });
                    //    return;
                    //}
    
                    cb( {code:code.OK} );
                }
        ],function(err) {
    
                if (err.code != code.OK) {
                    next(err, {code: code.FAIL});
                    return;
                }
    
                var data = {
                    usrId: usrInfo._id,
                    balance: usrInfo.balance,
                    authId: usrInfo.authData.auth_id
                };
                next(null, {code: code.OK, player: usrInfo ? data : null});
            }
        );
    
    */
  /*
        session.push('rid', function(err) {
            if(err) {
                console.error('set rid for session service failed! error is : %j', err.stack);
            }
        });
    */

  //Todo : 資料驗證需要傳送到 Auth Server 檢驗,先暫時在此實做功能

  //read Sample Code
  /*
    memberModule.findById('59e954bfa249296c5790d51a').exec(function (err, r_data) {
		var sttMsg    = JSON.parse(JSON.stringify(msg));
		var sttMember = JSON.parse(JSON.stringify(r_data));

        self.app.rpc.gameLogic.gameLogicRemote.add(session,uid, self.app.get('serverId'), rid, true, function(users){
            next(null,sttMember);	//回傳資料庫 _id
        });

        if((msg.username == sttMember.user_name) && (msg.userPassword == sttMember.password))
		//if(!err)
		{
           //console.log('[Info][Connect:entryHandler] user : '+msg.username+' check success');
            self.app.rpc.gameLogic.gameLogicRemote.add(session,uid, self.app.get('serverId'), rid, true, function(users){
                next(null,sttMember);	//回傳資料庫 _id
            });
            */
  /*
     //put user into channel
     self.app.rpc.chat.chatRemote.add(session, uid, self.app.get('serverId'), rid, true, function(users){
          next(null, {users:users});
     });*/
  /*
}else{
   //console.log('[Error][Connect:entryHandler] user : '+msg.username+' check error');
    //next(null,'[Error]user verify error');	//回傳錯誤代碼
}
});
*/
}

/*
app 會員登入取得auth id  

 */
handler.logIn = function (msg, session, next) {
  //console.log("-------------log in request----------:"+JSON.stringify(msg));

  var self = this
  var rid = msg.rid
  var usr_name = msg.username
  var usr_password = msg.userPassword

  session.set("rid", rid)
  //var sessionService = self.app.get('sessionService');
  //next(null, {user:"aaaaa"});
  //duplicate log in
  /*
     if( !! sessionService.getByUid(uid)) {
     next(null, {
     code: 500,
     error: true
     });
     return;
     }
     */

  var usrInfo = {}
  var authId = 0

  async.waterfall(
    [
      function (cb) {
        // auth aid
        self.app.rpc.auth.authRemote.logIn(session, usr_name, usr_password, cb)
      },
      function (err, user, cb) {
        // query player info by user id
        if (err.code != code.OK) {
          next(null, {
            code: err.code,
            //error: true
          })
          return
        }

        if (!user) {
          next(null, {
            code: err.code,
            //error: true
          })
          return
        }

        usrInfo = user

        if (typeof usrInfo.authData === "undefined") {
          self.app.rpc.auth.authRemote.createAuthId(session, usrInfo._id, cb)
        } else {
          var curDate = new Date()
          if (usrInfo.authData.expired <= curDate) {
            self.app.rpc.auth.authRemote.updateAuthId(session, usrInfo._id, cb)
          } else {
            cb({ code: code.OK }, usrInfo, cb)
          }
        }
        //uid = user._id;

        //self.app.rpc.auth.authRemote.createAuthId( usrInfo._id, cb );
      },
      function (err, authId, cb) {
        if (err.code != code.OK) {
          next(null, {
            code: err.code,
            // error: true
          })
          return
        }
        //if( typeof usrInfo.authData === "undefined" ){
        //    usrInfo.authData.push({auth_id:authId});
        //}else{
        var data = {
          auth_id: authId,
        }
        usrInfo.authData = data
        //}

        cb({ code: code.OK })
      },
    ],
    function (err) {
      if (err.code != code.OK) {
        next(err, { code: code.FAIL })
        return
      }

      var data = {
        usrId: usrInfo._id,
        balance: usrInfo.balance,
        authId: usrInfo.authData.auth_id,
      }
      next(null, { code: code.OK, player: usrInfo ? data : null })
      return
    }
  )

  /*
        session.bind(uid);
        session.set('rid', rid);
        session.push('rid', function(err) {
            if(err) {
                console.error('set rid for session service failed! error is : %j', err.stack);
            }
        });
        session.on('closed', onUserLeave.bind(null, self.app));
    */
}

/**
 * User log out handler
 *
 * @param {Object} app current application
 * @param {Object} session current session object
 *
 */
var onUserLeave = function (app, session) {
  //console.log("------------------onUserLeave---------------",app, session.uid);
  if (!session || !session.uid) {
    return
  }

  //app.rpc.chat.chatRemote.kick(session, session.uid, app.get('serverId'), session.get('rid'), null);
  //app.rpc.gameLogic.gameLogicRemote.kick(session, session.uid, app.get('serverId'), session.get('rid'), null);
}
