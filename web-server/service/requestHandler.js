//var Pomelo = require("../lib/components/pomelonode-pomelo-jsclient-websocket/lib/pomelo-client");

var m_conn = require("./connectManage")

const Socket = require("./socketHandler")

var api_function = []
//api_function['request_adminlogin'] = "admin.adminHandler.logIn";
//api_function['request_userlogin'] = "user.userHandler.logIn";

api_function["request_createOperator"] = "admin.adminHandler.join"
api_function["request_modifyUser_op"] = "admin.adminHandler.modifyOP"
api_function["request_getAdminEditInfo_op"] = "admin.adminHandler.getAdminEditInfo_op" // 帳號編輯的初始資料
api_function["request_getUsers_op"] = "admin.adminHandler.getAdmins"
api_function["request_userlogin_pr"] = "user.userHandler.logInPR"

api_function["request_createUser_ha"] = "user.userHandler.joinHA"
api_function["request_createUser_ag"] = "user.userHandler.joinAG"
api_function["request_createUser_pr"] = "user.userHandler.joinPR"
api_function["request_createUser_sha"] = "user.userHandler.joinSubHA"

api_function["request_modifyUser_ha"] = "user.userHandler.modifyHA"
api_function["request_modifyUser_sha"] = "user.userHandler.modifySubHA"
api_function["request_modifyUser_ag"] = "user.userHandler.modifyAG"
api_function["request_modifyUser_pr"] = "user.userHandler.modifyPR"

api_function["request_getUsers_ha"] = "user.userHandler.getHAs"
api_function["request_getUsers_sha"] = "user.userHandler.getSubHAs"
api_function["request_getUsers_pr"] = "user.userHandler.getPRs"
api_function["request_getUsers_ag"] = "user.userHandler.getAGs"
api_function["request_getUser"] = "user.userHandler.getUser" // 管理者編輯自己的帳號

api_function["request_joinAgentPage_init"] = "user.userHandler.getDefault_joinAgent"
api_function["request_gamePage_init"] = "game.gameHandler.getInit"
api_function["request_game_group"] = "game.gameHandler.getInit"
//api_function['request_game_group'] = "game.gameHandler.getGameGroup";

api_function["request_deleteGame"] = "game.gameHandler.deleteGame" // 遊戲列表 刪除
api_function["request_createGame"] = "game.gameHandler.createGame" // 遊戲列表 新增
api_function["request_modifyGame"] = "game.gameHandler.modifyGame" // 遊戲列表 編輯

api_function["request_getGameInfo_op"] = "game.gameHandler.getGames" // 取得遊戲資料(遊戲列表)
api_function["request_getGameOrder"] = "game.gameHandler.getGameOrder"
api_function["request_modifyGameOrder"] = "game.gameHandler.modifyGameOrder"
api_function["request_joinHallPage_init"] = "admin.adminHandler.getDefault_joinHall"
api_function["request_getGames_joinHa"] = "game.gameHandler.getGames_byGroup_hall"

api_function["request_getGames_joinAg"] = "game.gameHandler.getGames_byGroup_agent"
api_function["request_getBetlogs"] = "bet.betHandler.getList_BetHistory_v2"
api_function["request_getBetlogs_v3"] = "bet.betHandler.getList_BetHistory_v3"
api_function["request_getUserDetail_ha"] = "user.userHandler.getDetailHA"

api_function["request_getUserDetail_ag"] = "user.userHandler.getDetailAG"
api_function["request_joinPageInit_sha"] = "admin.adminHandler.getDefault_joinSha"
api_function["request_joinPageInit_op"] = "admin.adminHandler.getDefault_joinAdmin"
api_function["request_getUserDetail_pr"] = "user.userHandler.getDetailPR"
api_function["request_getGameOrder_lobby"] = "game.gameHandler.getGameOrder_byLobby"

api_function["request_getBetDetail"] = "bet.betHandler.getDetail_BetHistory"
api_function["request_getBetDetailFish"] = "bet.betHandler.getDetailFish_BetHistory"
api_function["request_getBetDetailSlot"] = "bet.betHandler.getDetailSlot_BetHistory"
api_function["request_getBetDetailArcade"] = "bet.betHandler.getDetailArcade_BetHistory"
api_function["request_getListAuthTmps"] = "admin.adminHandler.getList_AuthorityTmp"

api_function["request_getDefaultJoinAuth"] = "admin.adminHandler.getDefault_joinAuthority"
api_function["request_joinAuthTmp"] = "admin.adminHandler.join_AuthorityTemp"
api_function["request_joinAuthTmp_v2"] = "admin.adminHandler.join_AuthorityTemp_v2"
api_function["request_modifyAuthTmp"] = "admin.adminHandler.modify_AuthorityTemp"
api_function["request_getListMarquee"] = "admin.adminHandler.getList_marquee"
api_function["request_modifyAuth_Others"] = "admin.adminHandler.modify_AuthorityOthers"

api_function["request_createMarquee"] = "admin.adminHandler.createMarquee"
api_function["request_modifyMarquee"] = "admin.adminHandler.modifyMarquee"
api_function["request_createExchangeRate"] = "admin.adminHandler.createExchangeRate"
api_function["request_modifyExchangeRate"] = "admin.adminHandler.modifyExchangeRate"
api_function["request_DefaultListCurrency"] = "admin.adminHandler.getDefaultList_currency"

api_function["request_getRateRecord"] = "admin.adminHandler.getList_exchangeRate"
// api_function['request_getDownUsers_currency'] = "bet.betHandler.getDownUsersCurrency";
api_function["request_gameRevenue_init"] = "bet.betHandler.get_gameRevenue_init"
api_function["request_userRevenue_init"] = "bet.betHandler.get_gameRevenue_init"

api_function["request_getUserRevenue_ha"] = "bet.betHandler.getUserRevenue_hall"
api_function["request_getUserRevenue_ag"] = "bet.betHandler.getUserRevenue_agent"
api_function["request_getUserRevenue_pr"] = "bet.betHandler.getUserRevenue_player"
api_function["request_getUserRevenue_game"] = "bet.betHandler.getUserRevenue_game"
api_function["request_getUserRevenue"] = "bet.betHandler.getUserRevenue"

api_function["request_getGameRevenue_ha"] = "bet.betHandler.getGameRevenue_hall"
api_function["request_getGameRevenue_ag"] = "bet.betHandler.getGameRevenue_agent"
api_function["request_getGameRevenue_pr"] = "bet.betHandler.getGameRevenue_player"

api_function["request_getGameRevenue_game_byAdmin"] = "bet.betHandler.getGameRevenue_games"
api_function["request_getGameRevenue_game_byHall"] = "bet.betHandler.getGameRevenue_games"
api_function["request_getGameRevenue_game_byAgent"] = "bet.betHandler.getGameRevenue_games"

api_function["request_player_betSum"] = "bet.betHandler.getPlayerBetSum"
api_function["request_bettingRecordPage_init"] = "bet.betHandler.bettingRecordPageInit"

api_function["request_transactionPage_init"] = "bet.betHandler.transactionPageInit"
api_function["request_getTransactionRecord"] = "bet.betHandler.getTransactionRecord"
api_function["request_createIPWhiteList"] = "user.userHandler.CreateIPWhiteList"
api_function["request_modifyIPWhiteList"] = "user.userHandler.ModifyIPWhiteList"
api_function["request_deleteIPWhiteList"] = "user.userHandler.DeleteIPWhiteList"

api_function["request_getWhiteBlackList"] = "user.userHandler.getWhiteBlackList"
api_function["request_getJackpotSum"] = "bet.betHandler.getJackpotSum"
api_function["request_getJackpotDetail"] = "bet.betHandler.getJackpotDetail"
api_function["request_jackpotPage_init"] = "bet.betHandler.jackpotPageInit"
api_function["request_exchangeRatePage_init"] = "bet.betHandler.exchangeRatePageInit"
api_function["request_getTotalPlayersWin"] = "bet.betHandler.getTotalPlayersWin"

// api_function['request_getTotalRevenue'] = "bet.betHandler.getTotalRevenue";
// api_function['request_getMonthRevenueHistory'] = "bet.betHandler.getMonthRevenueHistory";
api_function["request_getTopGames"] = "bet.betHandler.getTopGames_v2"
api_function["request_getTopPlayers"] = "bet.betHandler.getTopPlayers"
api_function["request_getOnlineUsers"] = "log.logHandler.getOnlineUsers"
api_function["request_getOnlinePlayers"] = "user.userHandler.getOnlinePlayers"
api_function["request_get_down_user"] = "user.userHandler.getDownUser"

api_function["request_logout"] = "log.logHandler.set_user_logout"
api_function["request_settlement_report"] = "bet.betHandler.getSettlementReport"
api_function["request_action_log"] = "log.logHandler.getActionLog"
api_function["request_getOperatingRecordDetail"] = "log.logHandler.getOperatingRecordDetail"
api_function["request_user_loginout"] = "log.logHandler.getUserLoginout"

api_function["request_quota_log"] = "log.logHandler.getQuotaLog" // 開洗分紀錄
api_function["request_white_type"] = "user.userHandler.getWhiteType"
api_function["request_black_type"] = "user.userHandler.getBlackType"
api_function["request_action_log_type"] = "log.logHandler.getActionLogType"
api_function["request_kick_user"] = "user.userHandler.KickUser"
api_function["request_kick_all_user"] = "user.userHandler.KickAllUser"
api_function["request_add_gameCategory"] = "game.gameHandler.AddGameCategory"
api_function["request_modify_gameCategory"] = "game.gameHandler.ModifyGameCategory" // 修改遊戲類別

api_function["request_userAddToGameCategory"] = "game.gameHandler.UserAddToGameCategory_v2"
api_function["request_gameCategoryAddToUser"] = "game.gameHandler.GameCategoryAddToUser"
// api_function['request_modifyStatusGameCategory'] = "game.gameHandler.ModifyStatusGameCategory";
api_function["request_modifyStatusGameCategory"] = "game.gameHandler.ModifyStatusGameCategory_v2"
api_function["request_getGamesInCategory"] = "game.gameHandler.getGamesInCategory"
api_function["request_saveGames_bySorting"] = "game.gameHandler.saveGames_bySorting"

api_function["request_getUsersInCategory"] = "game.gameHandler.GetUsersInCategory"
api_function["request_getCateList_byUser"] = "game.gameHandler.GetCateList_byUser"
api_function["request_getGameDefaultSort"] = "game.gameHandler.GetGameDefaultSort"
api_function["request_get_sortingGame_list"] = "game.gameHandler.get_sortingGame_list"
api_function["request_edit_GameSorting"] = "game.gameHandler.editGameSorting_v2"

api_function["request_deleteGamesInCategory"] = "game.gameHandler.DeleteGamesInCategory"
api_function["request_create_OP_OTP"] = "admin.adminHandler.createOTP" //重新產生OTP (admin)
api_function["request_create_USR_OTP"] = "user.userHandler.createOTP" //重新產生OTP (user)
api_function["request_add_gameTag"] = "game.gameHandler.addGameTag" //新增遊戲標籤
api_function["request_edit_gameTag"] = "game.gameHandler.editGameTag" //編輯遊戲標籤

api_function["request_getList_gameTag"] = "game.gameHandler.getListGameTag" //遊戲標籤清單
api_function["request_getGameList_byUser"] = "game.gameHandler.getUserJoinGames" //取下線遊戲清單s
api_function["request_user_setting_init"] = "user.userHandler.getUserSettingInit" //使用者設定初始資料
api_function["request_modify_user_setting"] = "user.userHandler.modifyUserSetting" //修改使用者設定
api_function["request_getGameRevenue_byDate"] = "bet.betHandler.getGameRevenue_byDate_v2" //圖表(每日的GGR及RTP)

//api_function['request_getRevenue_byPlayers'] = "bet.betHandler.getRevenue_byPlayers";  //圖表(玩家報表-每日GGR及RTP)
api_function["request_add_currency"] = "admin.adminHandler.add_currency" //新增兌換幣別
api_function["request_edit_currency"] = "admin.adminHandler.edit_currency" //修改兌換幣別
api_function["request_currency_report_init"] = "bet.betHandler.currency_report_init" //幣別報表初始
api_function["request_get_currency_report"] = "bet.betHandler.get_currency_report" //幣別報表

api_function["request_getTempPassword_admin"] = "admin.adminHandler.getTempPassword" //取得密碼信件-Admin
api_function["request_getTempPassword_user"] = "user.userHandler.getTempPassword" //取得密碼信件-USER

api_function["request_temp_adminlogin"] = "admin.adminHandler.temp_logIn" //臨時密碼登入
api_function["request_temp_userlogin"] = "user.userHandler.temp_logIn" //臨時密碼登入

api_function["request_temp_set_password_admin"] = "admin.adminHandler.temp_set_password"
api_function["request_temp_set_password_user"] = "user.userHandler.temp_set_password"

api_function["request_getAgent_currency"] = "user.userHandler.getAgent_currency"
api_function["request_userlogin_pr_forTest"] = "user.userHandler.logInPR_Test"
api_function["request_today_bet_info"] = "bet.betHandler.get_today_bet_info"
api_function["request_bet_nums_byhours"] = "bet.betHandler.get_bet_nums_byhours"
api_function["request_offline_user"] = "user.userHandler.offline_user"
api_function["request_user_state"] = "user.userHandler.user_state"

//api_function['request_authTempDetail'] = "user.userHandler.getAuthTempDetail";
api_function["request_authTempDetail"] = "user.userHandler.getAuthTempDetail_v2"

api_function["request_modifyPassword_user"] = "user.userHandler.modifyPassword" //user改自己密碼
api_function["request_modifyPassword_op"] = "admin.adminHandler.modifyPassword" //admin改自己密碼

api_function["request_modifyUserPassword"] = "user.userHandler.modifyUserPassword" //(admin/user)改用戶密碼
api_function["request_modifyOpPassword"] = "admin.adminHandler.modifyOpPassword" //(admin)改operator密碼

api_function["request_getPlayerAccount"] = "user.userHandler.getPlayerAccount"
// api_function['request_login_op_auth'] = "admin.adminHandler.getAdminAuth";
// api_function['request_login_user_auth'] = "user.userHandler.getUserAuth";
api_function["checkUserConnectState"] = "user.userHandler.checkUserConnectState"
api_function["request_disabled_user"] = "user.userHandler.disabled_user"
api_function["request_check_duplicate_mail"] = "admin.adminHandler.check_duplicate_mail"
api_function["request_rate_history_init"] = "admin.adminHandler.rate_history_init" //歷史匯率初始
api_function["request_edit_game_init"] = "admin.adminHandler.edit_game_init"
api_function["request_add_wagers_bet"] = "bet.betHandler.add_wagers_bet" // 新增注單
api_function["request_settlement"] = "bet.betHandler.settlement" // 重新結帳
api_function["request_accounting_wagers"] = "bet.betHandler.accountingWagers" // 刪除指定區間結帳資料且重新結帳
//遊戲公司
api_function["request_game_company_list"] = "admin.systemHandler.game_company_list"
api_function["request_create_game_company"] = "admin.systemHandler.create_game_company"
api_function["request_modify_game_company"] = "admin.systemHandler.modify_game_company"
//類型
api_function["request_game_type_list"] = "admin.systemHandler.game_type_list"
api_function["request_create_game_type"] = "admin.systemHandler.create_game_type"
api_function["request_modify_game_type"] = "admin.systemHandler.modify_game_type"
//RTP
api_function["request_game_rtp_list"] = "admin.systemHandler.game_rtp_list"
api_function["request_create_game_rtp"] = "admin.systemHandler.create_game_rtp"
api_function["request_modify_game_rtp"] = "admin.systemHandler.modify_game_rtp"
//幣別
api_function["request_currency_list"] = "admin.systemHandler.currency_list"
api_function["request_create_currency"] = "admin.systemHandler.create_currency"
api_function["request_modify_currency"] = "admin.systemHandler.modify_currency"
//denom
api_function["request_game_denom_list"] = "admin.systemHandler.game_denom_list"
api_function["request_create_game_denom"] = "admin.systemHandler.create_game_denom"
api_function["request_modify_game_denom"] = "admin.systemHandler.modify_game_denom"
//遊戲種類
api_function["request_game_group_list"] = "admin.systemHandler.game_group_list" // 系統設定/遊戲種類
api_function["request_create_game_group"] = "admin.systemHandler.create_game_group" // 創建遊戲種類
api_function["request_modify_game_group"] = "admin.systemHandler.modify_game_group" // 修改遊戲種類
//遊戲幣別初始denom資料
api_function["request_game_default_denom_init"] = "admin.systemHandler.game_default_denom_init"
api_function["request_game_default_denom_list"] = "admin.systemHandler.game_default_denom_list"
api_function["request_create_game_default_denom"] = "admin.systemHandler.create_game_default_denom"
api_function["request_modify_game_default_denom"] = "admin.systemHandler.modify_game_default_denom"

api_function["request_transfer_balance"] = "admin.adminHandler.add_transfer_balance"

//取跑馬燈訊息
api_function["request_marquee_msg"] = "user.userHandler.marquee_msg"

//管控端連線判斷
api_function["check_user_token"] = "connector.backendHandler.check_user_token"
api_function["request_login_op_auth"] = "admin.adminHandler.getAdminAuth" //admin登入後取資料
api_function["request_login_user_auth"] = "user.userHandler.getUserAuth" //user登入後取資料

//新版 登入
api_function["request_adminlogin"] = "connector.backendHandler.admin_login_v1"
api_function["request_userlogin"] = "connector.backendHandler.user_login_v1"
api_function["check_user_token_v2"] = "connector.backendHandler.check_user_token_v2"

// 取得各種貢獻: 遊戲類別押注貢獻 & 廳主押注貢獻 & 廳主人數貢獻
api_function["request_getContribution"] = "bet.betHandler.getContribution"

// 檢查有無重複 DC
api_function["request_check_duplicate_dc"] = "admin.adminHandler.check_duplicate_dc"
// 檢查 OTP 是否有重設過
api_function["request_checkOTPCode"] = "connector.backendHandler.checkOTPCode"
// 關閉 Admin OTP
api_function["request_close_OP_OTP_Code"] = "admin.adminHandler.closeOTP"
// 關閉 User OTP
api_function["request_close_USER_OTP_Code"] = "user.userHandler.closeOTP"
// 取得錢包
api_function["request_getUserWallet_ag"] = "user.userHandler.getUserWallet"
// 伺服器紀錄查詢
api_function["request_server_action_log"] = "log.logHandler.getServerActionLog"
// 伺服器紀錄明細查詢
api_function["request_getServerRecordDetail"] = "log.logHandler.getServerRecordDetail"
// 新增、修改、刪除、重置信用額度
api_function["request_credits"] = "user.userHandler.credits"
// 轉帳取上線資料
api_function["request_getUpInfinite"] = "user.userHandler.getUpInfinite"
// 信用轉帳
api_function["request_credit_transfer_balance"] = "user.userHandler.add_credit_transfer_balance"
// 更換 token 以延時
api_function["request_prolong_token"] = "connector.backendHandler.prolong_token"
// 所有轉線資料
api_function["request_getTransferLineList"] = "user.userHandler.getTransferLineList"
// 檢查轉線層級
api_function["request_check_transferLine_level"] = "user.userHandler.checkTransferLineLevel"
// 更新轉線
api_function["request_transfer_line"] = "user.userHandler.transferLine"
// 線上玩家頁面初始資料
api_function["request_onlinePlayerPageInit"] = "connector.backendHandler.onlinePlayerPageInit"
// 取得線上玩家列表
api_function["request_onlinePlayerGetList"] = "connector.backendHandler.onlinePlayerGetList"
// 取得區域網段列表
api_function["request_ip_country_list"] = "admin.systemHandler.ipCountryList"
// 新增區域網段
api_function["request_ip_country_add"] = "admin.systemHandler.ipCountryAdd"
// 修改區域網段
api_function["request_ip_country_mod"] = "admin.systemHandler.ipCountryMod"
// 刪除區域網段
api_function["request_ip_country_del"] = "admin.systemHandler.ipCountryDel"
// 取得開放地區列表
api_function["request_open_area_list"] = "user.userHandler.openAreaList"
// 新增開放地區
api_function["request_open_area_add"] = "user.userHandler.openAreaAdd"
// 修改開放地區
api_function["request_open_area_mod"] = "user.userHandler.openAreaMod"
// 刪除開放地區
api_function["request_open_area_del"] = "user.userHandler.openAreaDel"
// 查詢下層資料
api_function["request_getCidByParent"] = "user.userHandler.getCidByParent"
// 域名設定頁面初始資料
api_function["request_domain_setting_page_init"] = "user.userHandler.domainSettingPageInit"
// 取得域名設定
api_function["request_domain_setting_get"] = "user.userHandler.domainSettingGet"
// 編輯域名設定
api_function["request_domain_setting_edit"] = "user.userHandler.domainSettingEdit"
// 刪除域名設定
api_function["request_domain_setting_del"] = "user.userHandler.domainSettingDel"
// 刪除統計 request_delSettlement
api_function["request_delSettlement"] = "bet.betHandler.delSettlement"

api_function["request_rollback_wagers"] = "bet.betHandler.rollbackWagers"

api_function["request_funky_report_search_options"] = "bet.funkyHandler.getFunkyReportSearchOptions"
api_function["request_funky_report_transaction_date"] = "bet.funkyHandler.getFunkyReportTransactionDate"

function Handle(web_conn, cb) {
  console.log("建立與game server的websocket 連線")
  this.web_conn = web_conn
  this.socket = new Socket(cb)
  //this.setWebMsssage( web_conn);
}

Handle.prototype.setConnect = function (web_conn) {
  this.web_conn = web_conn
  this.setWebMsssage(web_conn)
}

Handle.prototype.setWebMsssage = function (web_conn) {
  // var web_conn = this.web_conn;

  //跑馬燈訊息
  this.socket.on("onMessage", function (data) {
    // console.log('receive onMessage ', JSON.stringify(data));
    web_conn.send(
      JSON.stringify({
        action: "marquee",
        code: 200,
        data: data.data,
      })
    )
  })

  //踢人
  this.socket.on("onKick", function (data) {
    var code = 7006
    if (typeof data != "undefined" && typeof data.code != "undefined") code = data.code

    web_conn.send(
      JSON.stringify({
        action: "kick",
        code: code,
      })
    )
    web_conn.close() //websocket 關閉
  })

  //通知可以取儀表板數據
  this.socket.on("notifyRevenue", function (data) {
    web_conn.send(
      JSON.stringify({
        action: "notifyRevenue",
        code: 200,
        data: data.data,
      })
    )
  })

  //線上玩家數
  this.socket.on("onlinePlayers", function (data) {
    // console.log('receive onlinePlayers ', JSON.stringify(data));

    web_conn.send(
      JSON.stringify({
        action: "onlinePlayers",
        code: 200,
        data: data.data,
      })
    )
  })
}

Handle.prototype.disconnect = function (web_conn) {
  this.socket.disconnect()
}

Handle.prototype.disconnect_v2 = function (web_conn_key) {
  m_conn.removeKey(web_conn_key) //刪除此key資料
  this.socket.disconnect()
}

Handle.prototype.sendMsg = function (data) {
  var router = api_function[data.action]
  var connect = this.web_conn
  //console.log('------------connect ---------------------',connect);
  var uid = ""

  if (["request_adminlogin", "request_userlogin", "request_offline_user"].indexOf(data.action) == -1) {
    uid = data.id + "_" + data.userName
  }
  var remoteIP = connect.remoteAddress

  var sendData = {
    data: data.data,
    uid: uid,
    remoteIP: remoteIP,
    token: data.token,
  }

  if (typeof router != "undefined") {
    this.socket.sendMsg(router, sendData, function (r_data) {
      //回傳msg
      connect.send(
        JSON.stringify({
          action: data.action,
          code: r_data.code,
          data: r_data.data,
        })
      )

      if (r_data.code != 200) {
        return
      }
    })
  } else {
  }
}

Handle.prototype.sendMsg_v2 = function (data, callback) {
  var connect = this.web_conn
  var router = api_function[data.action]
  if (["request_adminlogin", "request_userlogin"].indexOf(data.action) == -1) {
    uid = data.id + "_" + data.userName
  }

  var remoteIP = connect.remoteAddress

  var sendData = {
    data: data.data,
    uid: uid,
    remoteIP: remoteIP,
    token: data.token,
  }

  if (typeof router != "undefined") {
    this.socket.sendMsg(router, sendData, function (r_data) {
      //回傳msg
      connect.send(
        JSON.stringify({
          action: data.action,
          code: r_data.code,
          data: r_data.data,
        })
      )

      if (r_data.code != 200) {
        return
      }

      callback({
        action: data.action,
      })
    })
  } else {
  }
}

//新版連線方式
Handle.prototype.sendMsg_v3 = function (data, callback) {
  var self = this
  console.log("-sendMsg_v3-", JSON.stringify(data))
  var router = api_function[data.action]
  //console.log("---web_conn----",this.web_conn)
  var connect = this.web_conn

  var uid = ""
  var web_conn_key = data["web_conn_key"] //client websocket connect key
  var web_token = data["token"] //client 傳送的tokens
  var remoteIP = connect.remoteAddress

  var sendData = {
    data: data.data,
    uid: uid,
    remoteIP: remoteIP,
    token: data.token,
  }
  var login_action = [
    "request_adminlogin",
    "request_userlogin",
    "request_checkOTPCode",
    "request_create_OP_OTP",
    "request_create_USR_OTP",
    "request_getTempPassword_admin",
    "request_temp_adminlogin",
    "request_temp_set_password_admin",
    "request_getTempPassword_user",
    "request_temp_userlogin",
    "request_temp_set_password_user",
  ]
  //非登入 用token判斷
  if (login_action.indexOf(data.action) == -1) {
    // var conn_key = m_conn.getKeyByToken(web_token) ; //無token  或 此web_conn 不同於 token
    var erro_code = 0
    var token_err = 0

    //斷線 token 會清掉 ----> game server 比對有無此表token
    if (web_token == undefined || web_token == "") {
      token_err++
      erro_code = 7068 //無token -用戶失效
    }
    // else if(conn_key!='' && conn_key != web_conn_key){ //有token 但不同key (重複登入)
    //     token_err ++;
    //     erro_code = 7001;
    // }

    if (token_err > 0) {
      console.log("token_err>0")
      //回傳WEB msg
      connect.send(
        JSON.stringify({
          action: data.action,
          code: erro_code, //用戶token失效,未傳token,逾期,編碼錯誤
          data: data.data,
        })
      )
      // m_conn.removeKey(web_conn_key); //刪除此key資料
      self.web_conn.close() //client的 websocket 關閉
      self.socket.disconnect()
    } else {
      //由server判斷 token格式 ,是否逾期  ....
      //判斷此Jwt是否正確

      var check_token_action = "check_user_token_v2"

      var check_token_data = {
        action: check_token_action,
        token: web_token,
      }

      self.socket.sendMsg(api_function[check_token_action], check_token_data, function (r_data) {
        //token成功做綁定 + 後續action request
        if (r_data.code == 200) {
          // m_conn.addToken(web_conn_key,web_token);

          self.socket.sendMsg(router, sendData, function (r_data2) {
            //console.log('response -------------',JSON.stringify(r_data2));
            self.callback_to_web(connect, data.action, r_data2)
            //登出 逾時 關閉連線
            if (r_data2.code == 502 || data.action == "request_logout") {
              // m_conn.removeKey(web_conn_key); //刪除此key資料
              self.web_conn.close()
              self.socket.disconnect()
            } else if (r_data2.code == 200 && data.action == "request_prolong_token") {
              // 更換 token 以延時
              // console.log("request_prolong_token response: " + r_data2.data.token);
              // m_conn.addToken(web_conn_key, r_data2.data.token);
            }
          })
        } else {
          //token失敗 ->斷線
          // m_conn.removeKey(web_conn_key); //刪除此key資料
          self.web_conn.close() //client的 websocket 關閉
        }
      })
    }
  }

  //登入action
  if (login_action.indexOf(data.action) > -1) {
    this.socket.sendMsg(router, sendData, function (r_data) {
      //回傳client
      self.callback_to_web(connect, data.action, r_data)
      //登入成功做綁定
      if (r_data.code == 200) {
        console.log("登入成功:", web_conn_key)
        //  m_conn.addToken(web_conn_key,r_data.data.token );
      }
    })
  }
}
//認證
Handle.prototype.tokenAuth = function (data, callback) {
  var router = api_function[data.action]
  var connect = this.web_conn
  var uid = ""
  var remoteIP = connect.remoteAddress
  var sendData = {
    data: data.data,
    uid: uid,
    remoteIP: remoteIP,
    conn: data.conn,
    token: data.token,
  }
  console.log("-----tokenAuth-------", router, JSON.stringify(sendData))
  if (typeof router != "undefined") {
    this.socket.sendMsg(router, sendData, function (r_data) {
      callback(r_data)
    })
  } else {
  }
}

Handle.prototype.sendMsg_byAuth = function (data, callback) {
  var router = api_function[data.action]
  var connect = this.web_conn
  if (["request_adminlogin", "request_userlogin"].indexOf(data.action) == -1) {
    uid = data.id + "_" + data.userName
  }

  //判斷有無此筆資料
  var conn_state = m_conn.check()
}
Handle.prototype.callback_to_web = function (connect, action, data) {
  //回傳WEB msg
  connect.send(
    JSON.stringify({
      action: action,
      code: data.code,
      data: data.data,
    })
  )
  return {
    action: action,
    code: data.code,
  }
}

module.exports = Handle
