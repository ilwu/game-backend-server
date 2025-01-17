var logger = require("pomelo-logger").getLogger("systemHandler", __filename)
var m_async = require("async")
var code = require("../../../util/code")
var conf = require("../../../../config/js/conf")
var timezone = require("../../../util/timezone")
var sysDao = require("../../../DataBase/sysDao")
var logDao = require("../../../DataBase/logDao")
var configDao = require("../../../DataBase/configDao")
var net = require("net")
var IPAddress = require("ip-address")

const { inspect } = require("util")
const consts = require("../../../share/consts")
const requestService = require("../../../services/requestService")

module.exports = function (app) {
  return new Handler(app)
}

var Handler = function (app) {
  this.app = app
}

var handler = Handler.prototype

//-------------------------------遊戲公司------------------------------------------------
/*
遊戲公司-list
data:{
   pageCount : 每頁筆數
   page : 目前第N頁
}
ERR-LOAD_GAME_COMPANY_FAIL
*/
handler.game_company_list = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.page === "undefined" ||
      typeof msg.data.pageCount === "undefined" ||
      typeof msg.data.page != "number" ||
      typeof msg.data.pageCount != "number" ||
      msg.data.page === 0 ||
      msg.data.pageCount === 0
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var ttlCount = 0 //總筆數
    var pageCur = msg.data.page //目前頁數
    var pageCount = msg.data.pageCount //每頁筆數
    var info = []

    m_async.waterfall(
      [
        function (cb) {
          //-----------------------------------------------------------------------------
          sysDao.getGameCompanyList(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOAD_GAME_COMPANY_FAIL,
            data: null,
          })
          return
        }
        ttlCount = r_data.count //總筆數
        info = r_data.info
        next(null, {
          code: code.OK,
          data: {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            info: info,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][game_company_list] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
遊戲公司-新增
data:{
   company : 公司名稱 必填
}
ERR-GAME_COMPANY_DUPLICATE
ERR-CREATE_GAME_COMPANY_FAIL
*/
handler.create_game_company = function (msg, session, next) {
  try {
    if (typeof msg.data === "undefined" || typeof msg.data.company === "undefined" || msg.data.company === "") {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var log_mod_after = []
    m_async.waterfall(
      [
        function (cb) {
          //----------------------------------------------------------------
          var param = {
            type: 1,
            company: msg.data.company,
          }
          sysDao.checkGameCompanyIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          if (r_data.length > 0) {
            next(null, {
              code: code.GAME.GAME_COMPANY_DUPLICATE,
              data: null,
            })
            return
          }
          sysDao.createGameCompany(msg.data, cb) //新增
        },
        function (r_code, insertId, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.CREATE_GAME_COMPANY_FAIL,
              data: null,
            })
            return
          }
          var param = {
            type: 2,
            Id: insertId,
          }
          sysDao.checkGameCompanyIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          var info = {
            game_company: r_data,
          }
          var log = []
          log.push(info)
          log_mod_after = log_mod_after.concat(log)

          var logData = {
            IP: msg.remoteIP,
            ModifiedType: "add",
            FunctionGroupL: "System",
            FunctionAction: "AddGameCompany",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: "",
            Desc_After: JSON.stringify(log_mod_after),
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
          }
          logDao.add_log_admin(logData, cb) //新增
        },
      ],
      function (none, r_code, insertId) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.CREATE_GAME_COMPANY_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][create_game_company] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

/*
遊戲公司-修改
ERR-GAME_COMPANY_DUPLICATE
ERR-MODIFY_GAME_COMPANY_FAIL
*/
handler.modify_game_company = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.Id === "undefined" ||
      msg.data.Id === "" ||
      typeof msg.data.company === "undefined" ||
      msg.data.company === ""
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var log_mod_before = []
    var log_mod_after = []

    m_async.waterfall(
      [
        function (cb) {
          //----------------------------------------------------------------
          var param = {
            type: 3,
            Id: msg.data.Id,
            company: msg.data.company,
          }
          sysDao.checkGameCompanyIsExist(param, cb) //判斷是否重複數值
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }

          if (r_data.length > 0) {
            next(null, {
              code: code.GAME.GAME_COMPANY_DUPLICATE,
              data: null,
            })
            return
          }

          var param = {
            type: 2,
            Id: msg.data.Id,
          }
          sysDao.checkGameCompanyIsExist(param, cb) //判斷是否有此筆資料
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          if (r_data.length == 0) {
            //查無資料
            next(null, {
              code: code.GAME.GAME_COMPANY_NOT_EXIST,
              data: null,
            })
            return
          }
          var info = {
            game_company: r_data,
          }
          var log = []
          log.push(info)
          log_mod_before = log_mod_before.concat(log)
          sysDao.modifyGameCompany(msg.data, cb) //修改
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.MODIFY_GAME_COMPANY_FAIL,
              data: null,
            })
            return
          }
          var param = {
            type: 2,
            Id: msg.data.Id,
          }
          sysDao.checkGameCompanyIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          var info = {
            game_company: r_data,
          }
          var log = []
          log.push(info)
          log_mod_after = log_mod_after.concat(log)

          var logData = {
            IP: msg.remoteIP,
            ModifiedType: "edit",
            FunctionGroupL: "System",
            FunctionAction: "EditGameCompany",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: "",
            Desc_After: JSON.stringify(log_mod_after),
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
          }
          logDao.add_log_admin(logData, cb) //新增 log
        },
      ],
      function (none, r_code, insertId) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.MODIFY_GAME_COMPANY_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][modify_game_company] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
//--------------------------------RTP------------------------------------------------
/*
RTP-list
ERR-LOAD_GAME_RTP_FAIL
*/
handler.game_rtp_list = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.page === "undefined" ||
      typeof msg.data.pageCount === "undefined" ||
      typeof msg.data.page != "number" ||
      typeof msg.data.pageCount != "number" ||
      msg.data.page === 0 ||
      msg.data.pageCount === 0
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var ttlCount = 0 //總筆數
    var pageCur = msg.data.page //目前頁數
    var pageCount = msg.data.pageCount //每頁筆數
    var info = []

    m_async.waterfall(
      [
        function (cb) {
          //-----------------------------------------------------------------------------
          sysDao.getGameRtpList(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOAD_GAME_RTP_FAIL,
            data: null,
          })
          return
        }
        ttlCount = r_data.count //總筆數
        info = r_data.info
        next(null, {
          code: code.OK,
          data: {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            info: info,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][game_rtp_list] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
RTP-新增
ERR-GAME_RTP_DUPLICATE
ERR-CREATE_GAME_RTP_FAIL
*/
handler.create_game_rtp = function (msg, session, next) {
  try {
    if (typeof msg.data === "undefined" || typeof msg.data.rtp === "undefined" || msg.data.rtp === "") {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var log_mod_after = []
    m_async.waterfall(
      [
        function (cb) {
          //----------------------------------------------------------------
          var param = {
            type: 1,
            rtp: msg.data.rtp,
          }
          sysDao.checkGameRtpIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          if (r_data.length > 0) {
            next(null, {
              code: code.GAME.GAME_RTP_DUPLICATE,
              data: null,
            })
            return
          }
          sysDao.createGameRtp(msg.data, cb) //新增
        },
        function (r_code, insertId, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.CREATE_GAME_RTP_FAIL,
              data: null,
            })
            return
          }
          var param = {
            type: 2,
            Id: insertId,
          }
          sysDao.checkGameRtpIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          var info = {
            game_rtp: r_data,
          }
          var log = []
          log.push(info)
          log_mod_after = log_mod_after.concat(log)

          var logData = {
            IP: msg.remoteIP,
            ModifiedType: "add",
            FunctionGroupL: "System",
            FunctionAction: "AddGameRtp",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: "",
            Desc_After: JSON.stringify(log_mod_after),
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
          }
          logDao.add_log_admin(logData, cb) //新增
        },
      ],
      function (none, r_code, insertId) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.CREATE_GAME_RTP_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][create_game_rtp] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
RTP-修改
-Id,rtp
ERR-GAME_RTP_DUPLICATE
ERR-MODIFY_GAME_RTP_FAIL
*/
handler.modify_game_rtp = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.Id === "undefined" ||
      msg.data.Id === "" ||
      typeof msg.data.rtp === "undefined" ||
      msg.data.rtp === ""
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var log_mod_before = []
    var log_mod_after = []

    m_async.waterfall(
      [
        function (cb) {
          //----------------------------------------------------------------
          var param = {
            type: 3,
            Id: msg.data.Id,
            rtp: msg.data.rtp,
          }
          sysDao.checkGameRtpIsExist(param, cb) //判斷是否重複數值
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }

          if (r_data.length > 0) {
            next(null, {
              code: code.GAME.GAME_RTP_DUPLICATE,
              data: null,
            })
            return
          }

          var param = {
            type: 2,
            Id: msg.data.Id,
          }
          sysDao.checkGameRtpIsExist(param, cb) //判斷是否有此筆資料
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          if (r_data.length == 0) {
            //查無資料
            next(null, {
              code: code.GAME.GAME_RTP_NOT_EXIST,
              data: null,
            })
            return
          }
          var info = {
            game_rtp: r_data,
          }
          var log = []
          log.push(info)
          log_mod_before = log_mod_before.concat(log)
          sysDao.modifyGameRtp(msg.data, cb) //修改
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.MODIFY_GAME_RTP_FAIL,
              data: null,
            })
            return
          }
          var param = {
            type: 2,
            Id: msg.data.Id,
          }
          sysDao.checkGameRtpIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          var info = {
            game_rtp: r_data,
          }
          var log = []
          log.push(info)
          log_mod_after = log_mod_after.concat(log)

          var logData = {
            IP: msg.remoteIP,
            ModifiedType: "edit",
            FunctionGroupL: "System",
            FunctionAction: "EditGameRtp",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: "",
            Desc_After: JSON.stringify(log_mod_after),
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
          }
          logDao.add_log_admin(logData, cb) //新增 log
        },
      ],
      function (none, r_code, insertId) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.MODIFY_GAME_RTP_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][modify_game_rtp] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
//----------------------------------遊戲種類----------------------------------------------
/*
遊戲種類-list
list : ggId, nameC, nameG, nameE, typeId, typeName
ERR - LOAD_GAME_GROUP_FAIL
*/
handler.game_group_list = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.page === "undefined" ||
      typeof msg.data.pageCount === "undefined" ||
      typeof msg.data.page != "number" ||
      typeof msg.data.pageCount != "number" ||
      msg.data.page === 0 ||
      msg.data.pageCount === 0
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var ttlCount = 0 //總筆數
    var pageCur = msg.data.page //目前頁數
    var pageCount = msg.data.pageCount //每頁筆數
    var info = []

    m_async.waterfall(
      [
        function (cb) {
          //-----------------------------------------------------------------------------
          sysDao.getGameGroupList(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOAD_GAME_GROUP_FAIL,
            data: null,
          })
          return
        }
        ttlCount = r_data.count //總筆數
        info = r_data.info
        next(null, {
          code: code.OK,
          data: {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            info: info,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][game_group_list] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
遊戲種類-新增
info : nameC,nameG,nameE,nameVN,nameTH,typeId
ERR - GAME_GROUP_DUPLICATE 
ERR - CREATE_GAME_GROUP_FAIL 
*/
handler.create_game_group = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.nameC === "undefined" ||
      msg.data.nameC === "" ||
      typeof msg.data.nameG === "undefined" ||
      msg.data.nameG === "" ||
      typeof msg.data.nameE === "undefined" ||
      msg.data.nameE === "" ||
      typeof msg.data.nameVN === "undefined" ||
      msg.data.nameVN === "" ||
      typeof msg.data.nameTH === "undefined" ||
      msg.data.nameTH === "" ||
      typeof msg.data.nameID === "undefined" ||
      msg.data.nameID === "" ||
      typeof msg.data.nameMY === "undefined" ||
      msg.data.nameMY === "" ||
      typeof msg.data.nameJP === "undefined" ||
      msg.data.nameJP === "" ||
      typeof msg.data.nameKR === "undefined" ||
      msg.data.nameKR === ""
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var log_mod_after = []
    m_async.waterfall(
      [
        function (cb) {
          //----------------------------------------------------------------
          sysDao.createGameGroup(msg.data, cb) //新增
        },
        function (r_code, insertId, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.CREATE_GAME_GROUP_FAIL,
              data: null,
            })
            return
          }
          var param = {
            type: 2,
            ggId: insertId,
          }
          sysDao.checkGameGroupIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          var info = {
            game_group: r_data,
          }
          var log = []
          log.push(info)
          log_mod_after = log_mod_after.concat(log)

          var logData = {
            IP: msg.remoteIP,
            ModifiedType: "add",
            FunctionGroupL: "System",
            FunctionAction: "AddGameGroup",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: "",
            Desc_After: JSON.stringify(log_mod_after),
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
          }
          logDao.add_log_admin(logData, cb) //新增
        },
      ],
      function (none, r_code, insertId) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.CREATE_GAME_GROUP_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][create_game_group] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
遊戲種類-修改
info : ggId,nameC,nameG,nameE,typeId
ERR - GAME_GROUP_DUPLICATE
ERR - MODIFY_GAME_GROUP_FAIL
*/
handler.modify_game_group = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.ggId === "undefined" ||
      typeof msg.data.nameC === "undefined" ||
      msg.data.nameC === "" ||
      typeof msg.data.nameG === "undefined" ||
      msg.data.nameG === "" ||
      typeof msg.data.nameE === "undefined" ||
      msg.data.nameE === "" ||
      typeof msg.data.nameVN === "undefined" ||
      msg.data.nameVN === "" ||
      typeof msg.data.nameTH === "undefined" ||
      msg.data.nameTH === "" ||
      typeof msg.data.nameID === "undefined" ||
      msg.data.nameID === "" ||
      typeof msg.data.nameMY === "undefined" ||
      msg.data.nameMY === "" ||
      typeof msg.data.nameJP === "undefined" ||
      msg.data.nameJP === "" ||
      typeof msg.data.nameKR === "undefined" ||
      msg.data.nameKR === ""
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var log_mod_before = []
    var log_mod_after = []

    m_async.waterfall(
      [
        function (cb) {
          //----------------------------------------------------------------
          var param = {
            type: 2,
            ggId: msg.data.ggId,
          }
          sysDao.checkGameGroupIsExist(param, cb) //判斷是否有此筆
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          if (r_data.length == 0) {
            //查無資料
            next(null, {
              code: code.GAME.GAME_GROUP_NOT_EXIST,
              data: null,
            })
            return
          }
          var info = {
            game_group: r_data,
          }
          var log = []
          log.push(info)
          log_mod_before = log_mod_before.concat(log)

          sysDao.modifyGameGroup(msg.data, cb) //修改
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.MODIFY_GAME_GROUP_FAIL,
              data: null,
            })
            return
          }
          var param = {
            type: 2,
            ggId: msg.data.ggId,
          }
          sysDao.checkGameGroupIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          var info = {
            game_group: r_data,
          }
          var log = []
          log.push(info)
          log_mod_after = log_mod_after.concat(log)

          var logData = {
            IP: msg.remoteIP,
            ModifiedType: "edit",
            FunctionGroupL: "System",
            FunctionAction: "EditGameGroup",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: "",
            Desc_After: JSON.stringify(log_mod_after),
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
          }
          logDao.add_log_admin(logData, cb) //新增 log
        },
      ],
      function (none, r_code, insertId) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.MODIFY_GAME_GROUP_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][modify_game_group] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
//-----------------------------------類型---------------------------------------------
/*
類型-list
list : Id,name,typeId
ERR - LOAD_GAME_TYPE_FAIL
*/
handler.game_type_list = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.page === "undefined" ||
      typeof msg.data.pageCount === "undefined" ||
      typeof msg.data.page != "number" ||
      typeof msg.data.pageCount != "number" ||
      msg.data.page === 0 ||
      msg.data.pageCount === 0
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var ttlCount = 0 //總筆數
    var pageCur = msg.data.page //目前頁數
    var pageCount = msg.data.pageCount //每頁筆數
    var info = []

    m_async.waterfall(
      [
        function (cb) {
          //-----------------------------------------------------------------------------
          sysDao.getGameTypeList(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOAD_GAME_TYPE_FAIL,
            data: null,
          })
          return
        }
        ttlCount = r_data.count //總筆數
        info = r_data.info
        next(null, {
          code: code.OK,
          data: {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            info: info,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][game_type_list] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
類型-新增
info : typeId,name
ERR - GAME_TYPE_DUPLICATE
ERR - CREATE_GAME_TYPE_FAIL
*/
handler.create_game_type = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.name === "undefined" ||
      msg.data.name === "" ||
      typeof msg.data.typeId == "undefined"
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var log_mod_after = []
    m_async.waterfall(
      [
        function (cb) {
          //----------------------------------------------------------------
          var param = {
            type: 1,
            name: msg.data.name,
          }
          sysDao.checkGameTypeIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          if (r_data.length > 0) {
            next(null, {
              code: code.GAME.GAME_TYPE_DUPLICATE,
              data: null,
            })
            return
          }
          sysDao.createGameType(msg.data, cb) //新增
        },
        function (r_code, insertId, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.CREATE_GAME_TYPE_FAIL,
              data: null,
            })
            return
          }
          var param = {
            type: 2,
            Id: insertId,
          }
          sysDao.checkGameTypeIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          var info = {
            game_type: r_data,
          }
          var log = []
          log.push(info)
          log_mod_after = log_mod_after.concat(log)

          var logData = {
            IP: msg.remoteIP,
            ModifiedType: "add",
            FunctionGroupL: "System",
            FunctionAction: "AddGameType",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: "",
            Desc_After: JSON.stringify(log_mod_after),
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
          }
          logDao.add_log_admin(logData, cb) //新增
        },
      ],
      function (none, r_code, insertId) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.CREATE_GAME_TYPE_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][create_game_type] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
類型-修改
info : Id,name,typeId
ERR - GAME_TYPE_DUPLICATE
ERR - MODIFY_GAME_TYPE_FAIL
*/
handler.modify_game_type = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.Id === "undefined" ||
      msg.data.Id === "" ||
      typeof msg.data.name === "undefined" ||
      msg.data.name === "" ||
      typeof msg.data.typeId === "undefined"
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var log_mod_before = []
    var log_mod_after = []

    m_async.waterfall(
      [
        function (cb) {
          //----------------------------------------------------------------
          var param = {
            type: 3,
            Id: msg.data.Id,
            name: msg.data.name,
          }
          sysDao.checkGameTypeIsExist(param, cb) //判斷是否重複數值
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }

          if (r_data.length > 0) {
            next(null, {
              code: code.GAME.GAME_TYPE_DUPLICATE,
              data: null,
            })
            return
          }

          var param = {
            type: 2,
            Id: msg.data.Id,
          }
          sysDao.checkGameTypeIsExist(param, cb) //判斷是否重複數值
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          if (r_data.length == 0) {
            //查無資料
            next(null, {
              code: code.GAME.GAME_TYPE_NOT_EXIST,
              data: null,
            })
            return
          }

          var info = {
            game_type: r_data,
          }
          var log = []
          log.push(info)
          log_mod_before = log_mod_before.concat(log)
          sysDao.modifyGameType(msg.data, cb) //修改
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.MODIFY_GAME_TYPE_FAIL,
              data: null,
            })
            return
          }
          var param = {
            type: 2,
            Id: msg.data.Id,
          }
          sysDao.checkGameTypeIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          var info = {
            game_type: r_data,
          }
          var log = []
          log.push(info)
          log_mod_after = log_mod_after.concat(log)

          var logData = {
            IP: msg.remoteIP,
            ModifiedType: "edit",
            FunctionGroupL: "System",
            FunctionAction: "EditGameType",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: "",
            Desc_After: JSON.stringify(log_mod_after),
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
          }
          logDao.add_log_admin(logData, cb) //新增 log
        },
      ],
      function (none, r_code, insertId) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.MODIFY_GAME_TYPE_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][modify_game_type] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
//-----------------------------------幣別---------------------------------------------
/*
幣別-list
list : currency,desc
ERR - LOAD_CURRENCY_FAIL
*/
handler.currency_list = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.page === "undefined" ||
      typeof msg.data.pageCount === "undefined" ||
      typeof msg.data.page != "number" ||
      typeof msg.data.pageCount != "number" ||
      msg.data.page === 0 ||
      msg.data.pageCount === 0
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var ttlCount = 0 //總筆數
    var pageCur = msg.data.page //目前頁數
    var pageCount = msg.data.pageCount //每頁筆數
    var info = []

    m_async.waterfall(
      [
        function (cb) {
          //-----------------------------------------------------------------------------
          sysDao.getCurrencyList(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOAD_CURRENCY_FAIL,
            data: null,
          })
          return
        }
        ttlCount = r_data.count //總筆數
        info = r_data.info
        next(null, {
          code: code.OK,
          data: {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            info: info,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][currency_list] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
幣別-新增
info : currency,desc
ERR - CURRENCY_DUPLICATE
ERR - CREATE_CURRENCY_FAIL
*/
handler.create_currency = function (msg, session, next) {
  try {
    console.log("create_currency", JSON.stringify(msg))
    if (typeof msg.data === "undefined" || typeof msg.data.currency === "undefined" || msg.data.currency.length > 10) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    msg.data.currency = msg.data.currency.toUpperCase() //幣別轉大寫
    var self = this
    var log_mod_after = []

    m_async.waterfall(
      [
        function (cb) {
          //     userSession = session;
          //----------------------------------------------------------------
          var param = {
            type: 1,
            currency: msg.data.currency,
          }
          sysDao.checkCurrencyIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          if (r_data.length > 0) {
            next(null, {
              code: code.GAME.CURRENCY_DUPLICATE,
              data: null,
            })
            return
          }
          sysDao.createCurrency(msg.data, cb) //新增
        },
        function (r_code, insertId, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.CREATE_CURRENCY_FAIL,
              data: null,
            })
            return
          }
          var param = {
            type: 2,
            currency: msg.data.currency,
          }
          sysDao.checkCurrencyIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          var info = {
            game_rtp: r_data,
          }
          var log = []
          log.push(info)
          log_mod_after = log_mod_after.concat(log)

          var logData = {
            IP: msg.remoteIP,
            ModifiedType: "add",
            FunctionGroupL: "System",
            FunctionAction: "AddCurrency",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: "",
            Desc_After: JSON.stringify(log_mod_after),
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
          }
          logDao.add_log_admin(logData, cb) //新增
        },
      ],
      function (none, r_code, insertId) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.CREATE_CURRENCY_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][create_currency] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
幣別-修改
info : currency, new_currency , desc
ERR - CURRENCY_DUPLICATE
ERR - MODIFY_CURRENCY_FAIL
*/
handler.modify_currency = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.currency === "undefined" ||
      msg.data.currency.length != 3 ||
      typeof msg.data.new_currency === "undefined" ||
      msg.data.new_currency.length != 3
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var log_mod_before = []
    var log_mod_after = []

    m_async.waterfall(
      [
        function (cb) {
          //----------------------------------------------------------------
          var param = {
            type: 1,
            currency: msg.data.new_currency,
          }
          sysDao.checkCurrencyIsExist(param, cb) //判斷是否重複數值
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          if (msg.data.new_currency != msg.data.currency && r_data.length > 0) {
            //重複幣別
            next(null, {
              code: code.GAME.CURRENCY_DUPLICATE,
              data: null,
            })
            return
          }

          //----------------------------------------------------------------
          var param = {
            type: 1,
            currency: msg.data.currency,
          }
          sysDao.checkCurrencyIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          if (r_data.length == 0) {
            next(null, {
              code: code.GAME.CURRENCY_NOT_EXIST,
              data: null,
            })
            return
          }
          var info = {
            currency: r_data,
          }
          var log = []
          log.push(info)
          log_mod_before = log_mod_before.concat(log)
          sysDao.modifyCurrency(msg.data, cb) //修改
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.MODIFY_CURRENCY_FAIL,
              data: null,
            })
            return
          }
          var param = {
            type: 1,
            currency: msg.data.new_currency,
          }
          sysDao.checkCurrencyIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          var info = {
            currency: r_data,
          }
          var log = []
          log.push(info)
          log_mod_after = log_mod_after.concat(log)

          var logData = {
            IP: msg.remoteIP,
            ModifiedType: "edit",
            FunctionGroupL: "System",
            FunctionAction: "EditCurrency",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: "",
            Desc_After: JSON.stringify(log_mod_after),
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
          }
          logDao.add_log_admin(logData, cb) //新增 log
        },
      ],
      function (none, r_code, insertId) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.MODIFY_CURRENCY_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][modify_currency] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
//-----------------------------------Denom---------------------------------------------
/*
Denom-list
ERR - LOAD_GAME_DENOM_FAIL
*/
handler.game_denom_list = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.page === "undefined" ||
      typeof msg.data.pageCount === "undefined" ||
      typeof msg.data.page != "number" ||
      typeof msg.data.pageCount != "number" ||
      msg.data.page === 0 ||
      msg.data.pageCount === 0
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var ttlCount = 0 //總筆數
    var pageCur = msg.data.page //目前頁數
    var pageCount = msg.data.pageCount //每頁筆數
    var info = []

    m_async.waterfall(
      [
        function (cb) {
          //-----------------------------------------------------------------------------
          sysDao.getGameDenomList(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOAD_GAME_DENOM_FAIL,
            data: null,
          })
          return
        }
        ttlCount = r_data.count //總筆數
        info = r_data.info
        next(null, {
          code: code.OK,
          data: {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            info: info,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][game_denom_list] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
Denom-新增
ERR - GAME_DENOM_DUPLICATE 
ERR - CREATE_GAME_DENOM_FAIL
*/
handler.create_game_denom = function (msg, session, next) {
  try {
    if (typeof msg.data === "undefined" || typeof msg.data.denom === "undefined" || msg.data.denom === "") {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }

    const enpointApiBsAction = conf.API_SERVER_URL + consts.APIServerPlatform.bsAction

    var log_mod_after = []
    m_async.waterfall(
      [
        function (cb) {
          //----------------------------------------------------------------
          var param = {
            type: 1,
            denom: msg.data.denom,
          }
          sysDao.checkGameDenomIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          if (r_data.length > 0) {
            next(null, {
              code: code.GAME.GAME_DENOM_DUPLICATE,
              data: null,
            })
            return
          }
          sysDao.createGameDenom(msg.data, cb) //新增
        },
        function (r_code, insertId, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.CREATE_GAME_DENOM_FAIL,
              data: null,
            })
            return
          }
          var param = {
            type: 2,
            Id: insertId,
          }
          sysDao.checkGameDenomIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          var info = {
            game_denom: r_data,
          }
          var log = []
          log.push(info)
          log_mod_after = log_mod_after.concat(log)

          var logData = {
            IP: msg.remoteIP,
            ModifiedType: "add",
            FunctionGroupL: "System",
            FunctionAction: "AddGameDenom",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: "",
            Desc_After: JSON.stringify(log_mod_after),
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
          }
          logDao.add_log_admin(logData, cb) //新增
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.CREATE_GAME_DENOM_FAIL,
              data: null,
            })
            return
          }

          const payload = {
            cacheKeyList: ["DENOM_LIST"],
          }

          requestService.delete(`${enpointApiBsAction}/v1/cache`, payload, { callback: cb })
        },
      ],
      function (none, r_code, insertId) {
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][create_game_denom] catch err", inspect(err))
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
Denom-修改
ERR - GAME_DENOM_DUPLICATE
ERR - MODIFY_GAME_DENOM_FAIL
*/
handler.modify_game_denom = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.Id === "undefined" ||
      msg.data.Id === "" ||
      typeof msg.data.denom === "undefined" ||
      msg.data.denom === ""
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    const enpointApiBsAction = conf.API_SERVER_URL + consts.APIServerPlatform.bsAction

    var log_mod_before = []
    var log_mod_after = []

    m_async.waterfall(
      [
        function (cb) {
          //----------------------------------------------------------------
          var param = {
            type: 3,
            Id: msg.data.Id,
            denom: msg.data.denom,
          }
          sysDao.checkGameDenomIsExist(param, cb) //判斷是否重複數值
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }

          if (r_data.length > 0) {
            next(null, {
              code: code.GAME.GAME_DENOM_DUPLICATE,
              data: null,
            })
            return
          }
          var param = {
            type: 2,
            Id: msg.data.Id,
          }
          sysDao.checkGameDenomIsExist(param, cb) //判斷是否此筆
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }

          if (r_data.length == 0) {
            next(null, {
              code: code.GAME.GAME_DENOM_NOT_EXIST,
              data: null,
            })
            return
          }

          var info = {
            game_denom: r_data,
          }
          var log = []
          log.push(info)
          log_mod_before = log_mod_before.concat(log)
          sysDao.modifyGameDenom(msg.data, cb) //修改
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.MODIFY_GAME_DENOM_FAIL,
              data: null,
            })
            return
          }
          var param = {
            type: 2,
            Id: msg.data.Id,
          }
          sysDao.checkGameDenomIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          var info = {
            game_denom: r_data,
          }
          var log = []
          log.push(info)
          log_mod_after = log_mod_after.concat(log)

          var logData = {
            IP: msg.remoteIP,
            ModifiedType: "edit",
            FunctionGroupL: "System",
            FunctionAction: "EditGameDenom",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: "",
            Desc_After: JSON.stringify(log_mod_after),
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
          }
          logDao.add_log_admin(logData, cb) //新增 log
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.MODIFY_GAME_DENOM_FAIL,
              data: null,
            })
            return
          }

          const payload = {
            cacheKeyList: ["DENOM_LIST"],
          }

          requestService.delete(`${enpointApiBsAction}/v1/cache`, payload, { callback: cb })
        },
      ],
      function (none, r_code, insertId) {
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][modify_game_denom] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//-----------------------------------各幣別預設Denom---------------------------------------------
/*
各幣別清單
*/
handler.game_default_denom_init = function (msg, session, next) {
  try {
    var self = this
    var currency = []
    var denom = []
    m_async.waterfall(
      [
        function (cb) {
          //-----------------------------------------------------------------------------
          configDao.getCurrencyList(cb) //幣別清單
        },
        function (r_code, r_data, cb) {
          currency = r_data
          self.app.rpc.config.configRemote.getDenoms(session, cb) //幣別清單
        },
      ],
      function (none, r_code, r_data) {
        denom = r_data
        next(null, {
          code: code.OK,
          data: {
            currency: currency,
            denom: denom,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][game_default_denom_init] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

/*
各幣別預設Denom-list
list : currency ,denom(array-Id,Value)
ERR - LOAD_GAME_DEFAULT_DENOM_FAIL
*/
handler.game_default_denom_list = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.page === "undefined" ||
      typeof msg.data.pageCount === "undefined" ||
      typeof msg.data.page != "number" ||
      typeof msg.data.pageCount != "number" ||
      msg.data.page === 0 ||
      msg.data.pageCount === 0
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var ttlCount = 0 //總筆數
    var pageCur = msg.data.page //目前頁數
    var pageCount = msg.data.pageCount //每頁筆數
    var info = []
    var all_denom = []

    m_async.waterfall(
      [
        function (cb) {
          //-----------------------------------------------------------------------------
          self.app.rpc.config.configRemote.getDenoms(session, cb) //幣別清單
        },
        function (r_code, r_data, cb) {
          all_denom = r_data

          sysDao.getGameDefaultDenomList(msg.data, cb)
        },
      ],
      function (none, r_code, r_data) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.LOAD_GAME_DEFAULT_DENOM_FAIL,
            data: null,
          })
          return
        }
        ttlCount = r_data.count //總筆數
        info = r_data.info

        //denom --
        info.forEach((item, idx) => {
          var default_denom = item.denom.split(",")
          var denoms = all_denom.filter((item) => default_denom.indexOf(item.Id.toString()) > -1)
          info[idx]["denom"] = denoms
        })

        next(null, {
          code: code.OK,
          data: {
            counts: ttlCount,
            pages: Math.ceil(ttlCount / pageCount),
            page_cur: pageCur,
            page_count: pageCount,
            info: info,
          },
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][game_default_denom_list] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
各幣別預設Denom-新增
info : currency, denom(0,1,2)
ERR - GAME_DEFAULT_DENOM_DUPLICATE
ERR - CREATE_GAME_DEFAULT_DENOM_FAIL
*/
handler.create_game_default_denom = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.currency === "undefined" ||
      msg.data.currency === "" ||
      typeof msg.data.denom === "undefined"
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var all_denom = []
    var log_mod_after = []
    m_async.waterfall(
      [
        function (cb) {
          //----------------------------------------------------------------
          var param = {
            type: 1,
            currency: msg.data.currency,
          }
          sysDao.checkGameDefaultDenomIsExist(param, cb) //此幣別是否已新增?
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          if (r_data.length > 0) {
            next(null, {
              code: code.GAME.GAME_DEFAULT_DENOM_DUPLICATE,
              data: null,
            })
            return
          }
          self.app.rpc.config.configRemote.getDenoms(session, cb) //幣別清單
        },
        function (r_code, r_data, cb) {
          all_denom = r_data

          var all_denom_id = all_denom.map((item) => item.Id)

          var denoms = msg.data.denom.split(",")

          var add_denom = denoms.filter((denom) => all_denom_id.indexOf(parseInt(denom)) > -1)

          msg.data["denom"] = add_denom
          sysDao.createGameDefaultDenom(msg.data, cb) //新增
        },
        function (r_code, insertId, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.CREATE_GAME_DEFAULT_DENOM_FAIL,
              data: null,
            })
            return
          }
          var param = {
            type: 1,
            currency: msg.data.currency,
          }
          sysDao.checkGameDefaultDenomIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          var info = {
            game_type: r_data,
          }
          var log = []
          log.push(info)
          log_mod_after = log_mod_after.concat(log)

          var logData = {
            IP: msg.remoteIP,
            ModifiedType: "add",
            FunctionGroupL: "System",
            FunctionAction: "AddGameDefaultDenom",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: "",
            Desc_After: JSON.stringify(log_mod_after),
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
          }
          logDao.add_log_admin(logData, cb) //新增
        },
      ],
      function (none, r_code, insertId) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.CREATE_GAME_DEFAULT_DENOM_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][create_game_default_denom] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
/*
各幣別預設Denom-修改
info - currency,denom
ERR - GAME_DEFAULT_DENOM_DUPLICATE
ERR - MODIFY_GAME_DEFAULT_DENOM_FAIL
*/
handler.modify_game_default_denom = function (msg, session, next) {
  try {
    if (
      typeof msg.data === "undefined" ||
      typeof msg.data.currency === "undefined" ||
      msg.data.currency === "" ||
      typeof msg.data.denom === "undefined"
    ) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    var self = this
    var all_denom = []
    var log_mod_before = []
    var log_mod_after = []

    m_async.waterfall(
      [
        function (cb) {
          //----------------------------------------------------------------
          var param = {
            type: 1,
            currency: msg.data.currency,
          }
          sysDao.checkGameDefaultDenomIsExist(param, cb) //判斷是否有此筆
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          if (r_data.length == 0) {
            //查無資料
            next(null, {
              code: code.GAME.GAME_DEFAULT_DENOM_NOT_EXIST,
              data: null,
            })
            return
          }
          var info = {
            game_default_currency_denom: r_data,
          }
          var log = []
          log.push(info)
          log_mod_before = log_mod_before.concat(log)
          self.app.rpc.config.configRemote.getDenoms(session, cb) //幣別清單
        },
        function (r_code, r_data, cb) {
          all_denom = r_data

          var all_denom_id = all_denom.map((item) => item.Id)
          var denoms = msg.data.denom.split(",")

          var add_denom = denoms.filter((denom) => all_denom_id.indexOf(parseInt(denom)) > -1)

          msg.data["denom"] = add_denom

          sysDao.modifyGameDefaultDenom(msg.data, cb) //修改
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: code.GAME.MODIFY_GAME_DEFAULT_DENOM_FAIL,
              data: null,
            })
            return
          }
          var param = {
            type: 1,
            currency: msg.data.currency,
          }
          sysDao.checkGameDefaultDenomIsExist(param, cb)
        },
        function (r_code, r_data, cb) {
          if (r_code.code != code.OK) {
            next(null, {
              code: r_code.code,
              data: null,
            })
            return
          }
          var info = {
            game_group: r_data,
          }
          var log = []
          log.push(info)
          log_mod_after = log_mod_after.concat(log)

          var logData = {
            IP: msg.remoteIP,
            ModifiedType: "edit",
            FunctionGroupL: "System",
            FunctionAction: "EditGameDefaultDenom",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: "",
            Desc_After: JSON.stringify(log_mod_after),
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
          }
          logDao.add_log_admin(logData, cb) //新增 log
        },
      ],
      function (none, r_code, insertId) {
        if (r_code.code != code.OK) {
          next(null, {
            code: code.GAME.MODIFY_GAME_DEFAULT_DENOM_FAIL,
            data: null,
          })
          return
        }
        next(null, {
          code: code.OK,
          data: "Success",
        })
        return
      }
    )
  } catch (err) {
    logger.error("[systemHandler][modify_game_default_denom] catch err", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//-----------------------------------區域網段---------------------------------------------

// 解析 CIDR 格式; 支援 ipv4、v6 (含壓縮格式), 與不指定 prefix 格式
// 但不支持 IPv4-mapped IPv6
// 輸入範例: 192.168.1.0/8、2001:db8:0:200::7、2001:db8:0:200:0:0:0:7/32
// 回傳: {ipType, startAddr, endAddr}
// ipType: 0 (v4)、1 (v6)
// startAddr, endAddr: v4 用 toHex(), v6 用 canonicalForm(); 都是完整的固定長度, 才好用資料庫比對
// 任何錯誤回傳 null
function parseCIDR(addr) {
  try {
    // 取得 ip 部分
    var fields = addr.match(/^([a-fA-F0-9:.]+)(\/([0-9]*))?$/)
    if (fields === null) {
      return null
    }
    let ip = fields[1]

    // 取得 ip type
    let ipType = -1
    let ipProto = net.isIP(ip)

    if (ipProto === 4) {
      ipType = 0
      let ipaddr = new IPAddress.Address4(addr)

      return {
        ipType,
        startAddr: ipaddr.startAddress().toHex(),
        endAddr: ipaddr.endAddress().toHex(),
      }
    } else if (ipProto === 6) {
      ipType = 1
      let ipaddr = new IPAddress.Address6(addr)

      if (ipaddr.is4()) {
        logger.warn("[systemHandler][parseCIDR] err: addr is IPv4-mapped IPv6")
        return null
      }

      return {
        ipType,
        startAddr: ipaddr.startAddress().canonicalForm(),
        endAddr: ipaddr.endAddress().canonicalForm(),
      }
    } else {
      logger.warn("[systemHandler][parseCIDR] err: unknown protocol")
      return null
    }
  } catch (err) {
    // ip-address 若遇到不合法的輸入會丟例外
    logger.warn("[systemHandler][parseCIDR] err: ", err)
    return null
  }
}

// 取得區域網段列表
// msg.data: {pageCur, pageCount}
handler.ipCountryList = function (msg, session, next) {
  try {
    const maxPageCount = 1000

    // 判斷參數
    if (msg.data.pageCur <= 0 || msg.data.pageCount <= 0) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }
    msg.data.pageCount = Math.min(msg.data.pageCount, maxPageCount)

    m_async.waterfall(
      [
        function (done) {
          // 取得區域網段列表
          let param = {
            pageCur: msg.data.pageCur,
            pageCount: msg.data.pageCount,
          }
          sysDao.ipCountryList(param, done)
        },
        function (r_code, r_data, done) {
          // r_data: {count, list: [Ip, Country, State]}
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 轉換資料
          let resp = { count: r_data.count, list: [] }
          for (let row of r_data.list) {
            resp.list.push({
              ip: row.Ip,
              country: row.Country,
              state: row.State,
            })
          }
          done(null, resp)
        },
      ],
      function (err, r_data) {
        // r_data: {count, list: [ip, country, state]}
        if (err) {
          logger.error("[systemHandler][ipCountryList] err: ", JSON.stringify(err))
          next(null, {
            code: code.FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: {
            pageCur: msg.data.pageCur,
            count: r_data.count,
            list: r_data.list,
          },
        })
      }
    )
  } catch (err) {
    logger.error("[systemHandler][ipCountryList] err: ", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
// 新增區域網段
// msg.data: {ip, country, state}
handler.ipCountryAdd = function (msg, session, next) {
  try {
    let logModBefore = [{ open_area: [] }]
    let logModAfter = []

    let cidrInfo = parseCIDR(msg.data.ip)
    if (!cidrInfo) {
      next(null, {
        code: code.DB.PARA_FAIL,
        data: null,
      })
      return
    }

    m_async.waterfall(
      [
        function (done) {
          // 新增區域網段
          let param = {
            ip: msg.data.ip,
            ipType: cidrInfo.ipType,
            ipStart: cidrInfo.startAddr,
            ipEnd: cidrInfo.endAddr,
            country: msg.data.country,
            state: msg.data.state,
          }
          sysDao.ipCountryAdd(param, done)
        },
        function (r_code, done) {
          if (r_code.code != code.OK) {
            // 資料重複
            next(null, {
              code: code.DB.DATA_DUPLICATE,
              data: null,
            })
            return
          }

          // 取得修改後的資料以紀錄
          let param = {
            ip: msg.data.ip,
            useMaster: true,
          }
          sysDao.ipCountryGet(param, done)
        },
        function (r_code, r_data, done) {
          // r_data: [{Ip, Country, State}]
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 修改後資訊
          logModAfter.push({ ip_country: r_data })

          // 加入操作紀錄
          var logData = {
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
            FunctionGroupL: "System",
            FunctionAction: "AddIPCountry",
            ModifiedType: "add",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: JSON.stringify(logModBefore),
            Desc_After: JSON.stringify(logModAfter),
            IP: msg.remoteIP,
          }
          logDao.add_log_admin(logData, done)
        },
      ],
      function (err) {
        if (err) {
          logger.error("[systemHandler][ipCountryAdd] err: ", JSON.stringify(err))
          next(null, {
            code: code.FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: {},
        })
      }
    )
  } catch (err) {
    logger.error("[systemHandler][ipCountryAdd] err: ", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
// 修改區域網段
// msg.data: {ip, country, state}
handler.ipCountryMod = function (msg, session, next) {
  try {
    let logModBefore = []
    let logModAfter = []

    m_async.waterfall(
      [
        function (done) {
          // 取得修改前的資料以紀錄
          let param = {
            ip: msg.data.ip,
          }
          sysDao.ipCountryGet(param, done)
        },
        function (r_code, r_data, done) {
          // r_data: [{Ip, Country, State}]
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 修改前資訊
          logModBefore.push({ ip_country: r_data })

          // 修改區域網段
          let param = {
            ip: msg.data.ip,
            country: msg.data.country,
            state: msg.data.state,
          }
          sysDao.ipCountryMod(param, done)
        },
        function (r_code, done) {
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 取得修改後的資料以紀錄
          let param = {
            ip: msg.data.ip,
            useMaster: true,
          }
          sysDao.ipCountryGet(param, done)
        },
        function (r_code, r_data, done) {
          // r_data: [{Ip, Country, State}]
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 修改後資訊
          logModAfter.push({ ip_country: r_data })

          // 加入操作紀錄
          var logData = {
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
            FunctionGroupL: "System",
            FunctionAction: "EditIPCountry",
            ModifiedType: "edit",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: JSON.stringify(logModBefore),
            Desc_After: JSON.stringify(logModAfter),
            IP: msg.remoteIP,
          }
          logDao.add_log_admin(logData, done)
        },
      ],
      function (err) {
        if (err) {
          logger.error("[systemHandler][ipCountryMod] err: ", JSON.stringify(err))
          next(null, {
            code: code.FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: {},
        })
      }
    )
  } catch (err) {
    logger.error("[systemHandler][ipCountryMod] err: ", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
// 刪除區域網段
// msg.data: {ip}
handler.ipCountryDel = function (msg, session, next) {
  try {
    let logModBefore = []
    let logModAfter = [{ open_area: [] }]

    m_async.waterfall(
      [
        function (done) {
          // 取得修改前的資料以紀錄
          let param = {
            ip: msg.data.ip,
          }
          sysDao.ipCountryGet(param, done)
        },
        function (r_code, r_data, done) {
          // r_data: [{Ip, Country, State}]
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 修改前資訊
          logModBefore.push({ ip_country: r_data })

          // 刪除區域網段
          let param = {
            ip: msg.data.ip,
          }
          sysDao.ipCountryDel(param, done)
        },
        function (r_code, done) {
          if (r_code.code != code.OK) {
            done(r_code)
            return
          }

          // 加入操作紀錄
          var logData = {
            AdminId: session.get("cid") || "",
            UserName: session.get("usrName") || "",
            FunctionGroupL: "System",
            FunctionAction: "DeleteIPCountry",
            ModifiedType: "delete",
            RequestMsg: JSON.stringify(msg),
            Desc_Before: JSON.stringify(logModBefore),
            Desc_After: JSON.stringify(logModAfter),
            IP: msg.remoteIP,
          }
          logDao.add_log_admin(logData, done)
        },
      ],
      function (err) {
        if (err) {
          logger.error("[systemHandler][ipCountryDel] err: ", JSON.stringify(err))
          next(null, {
            code: code.FAIL,
            data: null,
          })
          return
        }

        next(null, {
          code: code.OK,
          data: {},
        })
      }
    )
  } catch (err) {
    logger.error("[systemHandler][ipCountryDel] err: ", err)
    next(null, {
      code: code.FAIL,
      data: null,
    })
  }
}
