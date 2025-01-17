var logger = require("pomelo-logger").getLogger("configRemote", __filename)
var code = require("../../../util/code")
var m_async = require("async")
var configDao = require("../../../DataBase/configDao")
module.exports = function (app) {
  return new ConfigRemote(app)
}

var companys = []
var rtps = []
var denoms = []
var types = []
var groups = []
var maths = []
var orderTypes = []
var authorityTemps = []
var authorityFuncs = []
var levels = []
var sysLimitSec = 0
var ActionType = []
var tags = []
var searchDays = []
var timezoneSet = []
var CountsOfPerPage = []
var mainCurrency = "" //系統主幣別

var ConfigRemote = function (app) {
  this.app = app

  getConfigureData_Game()
  getConfigureData_User()
}

var getConfigureData_User = function () {
  try {
    m_async.parallel(
      {
        authTemp: function (cb) {
          configDao.getAuthorityTemps(function (err, authTemps) {
            if (err.code === 200) {
              cb(null, authTemps)
            } else {
              cb(null, null)
            }
          })
        },
        authFun: function (cb) {
          configDao.getAuthorityFuncs(function (err, authFuncs) {
            if (err.code === 200) {
              cb(null, authFuncs)
            } else {
              cb(null, null)
            }
          })
        },
        level: function (cb) {
          configDao.getLevel(function (err, levels) {
            if (err.code === 200) {
              cb(null, levels)
            } else {
              cb(null, null)
            }
          })
        },
        currency: function (cb) {
          configDao.getCurrenies(function (err, currencies) {
            //全部幣別
            if (err.code === 200) {
              cb(null, currencies)
            } else {
              cb(null, null)
            }
          })
        },
        sysLimitTime: function (cb) {
          configDao.getSystemLimitTime(function (err, sysLimitSec) {
            if (err.code === 200) {
              cb(null, sysLimitSec)
            } else {
              cb(null, null)
            }
          })
        },
        // getActionType: function (cb) {
        //     configDao.getActionType(function (err, actionType) {
        //         if (err.code === 200) {
        //             cb(null, actionType);
        //         } else {
        //             cb(null, null);
        //         }
        //     });
        // },
        getSearchDays: function (cb) {
          configDao.getSystemSeachDays(function (err, searchDays) {
            if (err.code === 200) {
              cb(null, searchDays)
            } else {
              cb(null, null)
            }
          })
        },
        getCountsOfPerPage: function (cb) {
          configDao.getCountsOfPerPage(function (err, pageCounts) {
            if (err.code === 200) {
              cb(null, pageCounts)
            } else {
              cb(null, null)
            }
          })
        },
        getTimezoneSet: function (cb) {
          configDao.getTimezoneSet(function (err, timezoneHours) {
            if (err.code === 200) {
              cb(null, timezoneHours)
            } else {
              cb(null, null)
            }
          })
        },
        getMainCurrency: function (cb) {
          configDao.getSystemMainCurrency(function (err, mainCurrency) {
            if (err.code === 200) {
              cb(null, mainCurrency)
            } else {
              cb(null, null)
            }
          })
        },
      },
      function (errs, results) {
        if (!!results.authTemp) {
          authorityTemps = results.authTemp
        }

        if (!!results.authFun) {
          authorityFuncs = results.authFun
        }

        if (!!results.level) {
          levels = results.level
        }

        if (!!results.currency) {
          Currencies = results.currency
        }

        if (!!results.sysLimitTime) {
          sysLimitSec = results.sysLimitTime
        }

        // if (!!results.getActionType) {
        //     ActionType = results.getActionType;
        // }

        if (!!results.getSearchDays) {
          searchDays = results.getSearchDays.split(",")
        }

        if (!!results.getTimezoneSet) {
          timezoneSet = results.getTimezoneSet
        }

        if (!!results.getMainCurrency) {
          mainCurrency = results.getMainCurrency
        }

        if (!!results.getCountsOfPerPage) {
          CountsOfPerPage = results.getCountsOfPerPage.split(",")
        }

        //console.log('--ActionType--',ActionType)
        //console.log("-------------------" + JSON.stringify(authorityFuncs));
        //console.log("---------tmps----------" + JSON.stringify(authorityTemps));
      }
    )
  } catch (err) {
    logger.error("[configRemote][getConfigureData_User] catch err", err)
  }
}

var getConfigureData_Game = function () {
  try {
    m_async.parallel(
      {
        company: function (cb) {
          configDao.getCompany(function (err, companys) {
            if (err.code === code.OK) {
              //AuthorityTemps = authTemps;
              cb(null, companys)
            } else {
              cb(null, null)
            }
          })
        },
        rtp: function (cb) {
          configDao.getRTPs(function (err, rtps) {
            if (err.code === code.OK) {
              cb(null, rtps)
            } else {
              cb(null, null)
            }
          })
        },
        denom: function (cb) {
          configDao.getDenoms(function (err, denoms) {
            if (err.code === code.OK) {
              cb(null, denoms)
            } else {
              cb(null, null)
            }
          })
        },
        type: function (cb) {
          configDao.getGameType(function (err, gameTypes) {
            if (err.code === code.OK) {
              cb(null, gameTypes)
            } else {
              cb(null, null)
            }
          })
        },
        group: function (cb) {
          configDao.getGameGroup(function (err, gameGroups) {
            if (err.code === code.OK) {
              cb(null, gameGroups)
            } else {
              cb(null, null)
            }
          })
        },
        math: function (cb) {
          configDao.getMathRng(function (err, maths) {
            if (err.code === code.OK) {
              cb(null, maths)
            } else {
              cb(null, null)
            }
          })
        },
        orderType: function (cb) {
          configDao.getOrderType(function (err, orderTypes) {
            if (err.code === code.OK) {
              cb(null, orderTypes)
            } else {
              cb(null, null)
            }
          })
        },
      },
      function (errs, results) {
        if (!!results.company) {
          companys = results.company
        }

        if (!!results.rtp) {
          rtps = results.rtp
        }

        if (!!results.denom) {
          denoms = results.denom
        }

        if (!!results.type) {
          types = results.type
        }

        if (!!results.group) {
          groups = results.group
        }

        if (!!results.math) {
          maths = results.math
        }

        if (!!results.orderType) {
          orderTypes = results.orderType
        }
      }
    )
  } catch (err) {
    logger.error("[configRemote][getConfigureData_Game] catch err", err)
  }
}

var config_remote = ConfigRemote.prototype

config_remote.getRTPs = function (cb) {
  try {
    configDao.getRTPs(function (r_code, r_data) {
      cb(null, { code: r_code.code }, r_data)
    })
    //cb(null, { code: code.OK, msg: "" }, rtps);
  } catch (err) {
    logger.error("[configRemote][getRTPs] catch err", err)
    cb(null, code.FAIL, null)
  }
}

config_remote.getDenoms = function (cb) {
  try {
    configDao.getDenoms(function (r_code, r_data) {
      cb(null, { code: r_code.code }, r_data)
    })
    // cb(null, { code: code.OK, msg: "" }, denoms);
  } catch (err) {
    logger.error("[configRemote][getDenoms] catch err", err)
    cb(null, code.FAIL, null)
  }
}

config_remote.getCompany = function (cb) {
  try {
    configDao.getCompany(function (r_code, r_data) {
      cb(null, { code: r_code.code }, r_data)
    })
  } catch (err) {
    logger.error("[configRemote][getCompany] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// config_remote.getOrderType = function (cb) {
//     configDao.getOrderType(function(r_code, r_data){
//         cb(null, { code: r_code.code }, r_data);
//     });
// };

config_remote.getGameGroup = function (cb) {
  try {
    configDao.getGameGroup(function (r_code, r_data) {
      cb(null, { code: r_code.code }, r_data)
    })
    cb(null, { code: code.OK, msg: "" }, groups)
  } catch (err) {
    logger.error("[configRemote][getGameGroup] catch err", err)
    cb(null, code.FAIL, null)
  }
}

config_remote.getGameType = function (cb) {
  try {
    configDao.getGameType(function (r_code, r_data) {
      cb(null, { code: r_code.code }, r_data)
    })
  } catch (err) {
    logger.error("[configRemote][getGameType] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// config_remote.addRTPs = function (data, cb) {
//     // gameDao.
// };

// config_remote.getMathRng = function (cb) {
//     configDao.getMathRng(function(r_code, r_data){
//         cb(null, { code: r_code.code }, r_data);
//     });
// }
/*
//各層級範本權限
config_remote.getDefault_joinAuthorityFuncs = function (cb) {
    var authData = {};
    var authLevel = {};

    var admin_authData = {};
    var admin_authList = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81];
    var admin_authList = getAuthData('Admin');
    for (i in admin_authList) {
        var funs = authorityFuncs.funcById[admin_authList[i]];

        if (typeof admin_authData[funs.type] === 'undefined') {
            admin_authData[authorityFuncs.funcById[admin_authList[i]].type] = [];
        }
        admin_authData[authorityFuncs.funcById[admin_authList[i]].type].push({ id: admin_authList[i], action: authorityFuncs.funcById[admin_authList[i]].action });
    }

    authData['operator'] = admin_authData;

    var hall_authData = {};
    var hall_authList = [1, 2, 3, 4, 8, 9, 10, 11, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 31, 32, 33, 34, 35, 40, 41, 42, 43, 47, 51, 52, 54];

    for (i in hall_authList) {

        var funs = authorityFuncs.funcById[hall_authList[i]];

        if (typeof hall_authData[funs.type] === 'undefined') {
            hall_authData[authorityFuncs.funcById[hall_authList[i]].type] = [];
        }
        hall_authData[authorityFuncs.funcById[hall_authList[i]].type].push({ id: hall_authList[i], action: authorityFuncs.funcById[hall_authList[i]].action });
    }

    authData['hall'] = hall_authData;

    var agent_authData = {};
    var agent_authList = [1, 2, 3, 4, 8, 9, 10, 31, 32, 33, 34, 35, 40, 41, 42, 54];

    for (i in agent_authList) {

        var funs = authorityFuncs.funcById[agent_authList[i]];

        if (typeof agent_authData[funs.type] === 'undefined') {
            agent_authData[authorityFuncs.funcById[agent_authList[i]].type] = [];
        }
        agent_authData[authorityFuncs.funcById[agent_authList[i]].type].push({ id: agent_authList[i], action: authorityFuncs.funcById[agent_authList[i]].action });
    }

    authData['agent'] = agent_authData;
    cb(null, { code: code.OK }, authData);
};
*/

//各層級範本權限
config_remote.getDefault_joinAuthorityFuncs_v2 = function (data, callback) {
  try {
    var authData = {}
    /*
        var param = {
            user_level: '',
        }

        if (typeof data != 'undefined' && typeof data.auth_state != 'undefined' && data.auth_state == true && typeof data.up_authority != 'undefined') {
            param['up_authority'] = data.up_authority;
    
        data.user_level.forEach(async (level, idx) => {
            param['user_level'] = level;
            configDao.getAuthData_byUpUser(param, function (r_none, r_code, r_data) {
                var user_authList = r_data[0]['funID'].split(",");
                var user_level = level.toLowerCase();
                authData[user_level] = Object.assign({}, setUserAuth(user_authList, authorityFuncs));
                console.log('-user_level-::::',JSON.stringify(authData));
                if (idx == data.user_level.length - 1) {
                    callback(null, {
                        code: code.OK
                    }, authData);
                }
            });
        });
        */

    var param = {
      user_level: "",
    }
    if (
      typeof data != "undefined" &&
      typeof data.auth_state != "undefined" &&
      data.auth_state == true &&
      typeof data.up_authority != "undefined"
    ) {
      param["up_authority"] = data.up_authority
    }

    m_async.waterfall(
      [
        function (cb) {
          //admin
          param["user_level"] = "Admin"
          configDao.getAuthData_byUpUser(param, cb)
        },
        function (r_code, r_data, cb) {
          var user_authList = r_data[0]["funID"].split(",")
          authData["admin"] = Object.assign({}, setUserAuth(user_authList, authorityFuncs))
          param["user_level"] = "Hall"
          configDao.getAuthData_byUpUser(param, cb)
        },
        function (r_code, r_data, cb) {
          var user_authList = r_data[0]["funID"].split(",")
          authData["reseller"] = Object.assign({}, setUserAuth(user_authList, authorityFuncs))
          param["user_level"] = "SubHall"
          configDao.getAuthData_byUpUser(param, cb)
        },
        function (r_code, r_data, cb) {
          var user_authList = r_data[0]["funID"].split(",")
          authData["subhall"] = Object.assign({}, setUserAuth(user_authList, authorityFuncs))
          param["user_level"] = "Agent"
          configDao.getAuthData_byUpUser(param, cb)
        },
      ],
      function (none, r_code, r_data) {
        var user_authList = r_data[0]["funID"].split(",")
        authData["agent"] = Object.assign({}, setUserAuth(user_authList, authorityFuncs))
        callback(
          null,
          {
            code: code.OK,
          },
          authData
        )
      }
    )
  } catch (err) {
    logger.error("[configRemote][getDefault_joinAuthorityFuncs_v2] catch err", err)
    callback(null, {
      code: code.FAIL,
      data: null,
    })
  }
}

//各層級範本權限
config_remote.getDefault_joinAuthorityFuncs_v3 = function (data, cb) {
  try {
    var authData = {}
    m_async.waterfall(
      [
        function (cb) {
          configDao.getAuthData_byUpUser(data, cb) //上層的權限
        },
      ],
      function (none, r_code, r_data) {
        var user_authList = r_data[0]["funID"].split(",")
        var user_level = data["user_level"].toLowerCase()
        authData[user_level] = Object.assign({}, setUserAuth(user_authList, authorityFuncs))
        // authData[user_level] = Object.assign({}, setUserAuth_v2(user_authList, authorityFuncs));
        cb(null, { code: code.OK }, authData)
      }
    )
  } catch (err) {
    logger.error("[configRemote][getDefault_joinAuthorityFuncs_v3] catch err", err)
    cb(null, code.FAIL, null)
  }
}

//排列整齊
// var setUserAuth_v2 = function (user_authList, authorityFuncs) {
//     var user_authData = {};
//     for (i in user_authList) {
//         var funs = authorityFuncs.funcById[user_authList[i]];

//         if (typeof user_authData[funs.type] === 'undefined') {
//             user_authData[authorityFuncs.funcById[user_authList[i]].type] = [];
//         }
//         user_authData[authorityFuncs.funcById[user_authList[i]].type].push({ id: parseInt(user_authList[i]), action: authorityFuncs.funcById[user_authList[i]].action });
//     }
//     return user_authData;
// }
//新增.編輯.檢視
var setUserAuth = function (user_authList, authorityFuncs) {
  try {
    var user_authData = {}
    //main
    for (i in user_authList) {
      var funs = authorityFuncs.funcById[user_authList[i]]
      if (typeof funs.type != "undefined") {
        if (typeof user_authData[funs.type] === "undefined") {
          user_authData[authorityFuncs.funcById[user_authList[i]].type] = {}
        }
        if (funs.funM != "-" && typeof user_authData[funs.type]["Info"] === "undefined") {
          user_authData[funs.type]["Info"] = {}
        }
        if (funs.funM != "-" && typeof user_authData[funs.type]["Info"][funs.funM] === "undefined") {
          user_authData[funs.type]["Info"][funs.funM] = {}
        }
        if (funs.funM != "-" && typeof user_authData[funs.type]["Info"][funs.funM]["Info"] === "undefined") {
          user_authData[funs.type]["Info"][funs.funM]["Info"] = []
        }

        if (funs.funM == "-") {
          //main
          user_authData[funs.type]["id"] = user_authList[i].toString()
          user_authData[funs.type]["pId"] = funs.pId
          user_authData[funs.type]["nameE"] = funs.nameE
          user_authData[funs.type]["nameG"] = funs.nameG
          user_authData[funs.type]["nameC"] = funs.nameC
          user_authData[funs.type]["funType"] = funs.funType
          user_authData[funs.type]["action"] = funs.action
        } else if (funs.funS == "-") {
          //sub

          user_authData[funs.type]["Info"][funs.funM]["id"] = user_authList[i].toString()
          user_authData[funs.type]["Info"][funs.funM]["pId"] = funs.pId
          user_authData[funs.type]["Info"][funs.funM]["nameE"] = funs.nameE
          user_authData[funs.type]["Info"][funs.funM]["nameG"] = funs.nameG
          user_authData[funs.type]["Info"][funs.funM]["nameC"] = funs.nameC
          user_authData[funs.type]["Info"][funs.funM]["funType"] = funs.funType
          user_authData[funs.type]["Info"][funs.funM]["action"] = funs.action

          if (typeof user_authData[funs.type]["id"] === "undefined") {
            var pId_info = authorityFuncs.funcById[funs.pId]
            user_authData[funs.type]["id"] = pId_info.id.toString()
            user_authData[funs.type]["pId"] = pId_info.pId
            user_authData[funs.type]["nameE"] = pId_info.nameE
            user_authData[funs.type]["nameG"] = pId_info.nameG
            user_authData[funs.type]["nameC"] = pId_info.nameC
            user_authData[funs.type]["funType"] = pId_info.funType
            user_authData[funs.type]["action"] = pId_info.action
          } //補上層資料
        } else {
          user_authData[funs.type]["Info"][funs.funM]["Info"].push({
            id: user_authList[i].toString(),
            pId: funs.pId,
            nameE: funs.nameE,
            nameG: funs.nameG,
            nameC: funs.nameC,
            funType: funs.funType,
            action: funs.action,
          })
          if (typeof user_authData[funs.type]["Info"][funs.funM]["id"] === "undefined") {
            var pId_info = authorityFuncs.funcById[funs.pId]
            user_authData[funs.type]["Info"][funs.funM]["id"] = pId_info.id.toString()
            user_authData[funs.type]["Info"][funs.funM]["pId"] = pId_info.pId
            user_authData[funs.type]["Info"][funs.funM]["nameE"] = pId_info.nameE
            user_authData[funs.type]["Info"][funs.funM]["nameG"] = pId_info.nameG
            user_authData[funs.type]["Info"][funs.funM]["nameC"] = pId_info.nameC
            user_authData[funs.type]["Info"][funs.funM]["funType"] = pId_info.funType
            user_authData[funs.type]["Info"][funs.funM]["action"] = pId_info.action

            if (typeof user_authData[funs.type]["id"] === "undefined") {
              var ppId_info = authorityFuncs.funcById[pId_info.pId]
              if (ppId_info != undefined) {
                user_authData[funs.type]["id"] = ppId_info.id.toString()
                user_authData[funs.type]["pId"] = ppId_info.pId
                user_authData[funs.type]["nameE"] = ppId_info.nameE
                user_authData[funs.type]["nameG"] = ppId_info.nameG
                user_authData[funs.type]["nameC"] = ppId_info.nameC
                user_authData[funs.type]["funType"] = ppId_info.funType
                user_authData[funs.type]["action"] = ppId_info.action
              }
            } //補上層資料
          }
        }
      }
    }
    return user_authData
  } catch (err) {
    logger.error("[configRemote][setUserAuth] catch err", err)
  }
}
// config_remote.getAuthorityTmpDetail_OP = function (data, cb) {

//     if (typeof data.authTmpId === 'undefined') {
//         cb(null, { code: code.CONFIG.LOAD_CONFIG_AUTHORITY_FAIL }, null);
//         return;
//     }

//     var authTmp = authorityFuncs.funcById[data.authTmpId];

//     if (typeof authTmp === 'undefined') {
//         cb(null, { code: code.CONFIG.LOAD_CONFIG_AUTHORITY_FAIL }, null);
//         return;
//     }

//     cb(null, { code: code.OK }, { init: authorityFuncs.funcById });

// };

config_remote.getAuthorityTemp = function (data, cb) {
  try {
    if (typeof data.authType === "undefined") {
      cb(null, { code: code.CONFIG.LOAD_CONFIG_AUTHORITY_FAIL }, null)
      return
    }

    var authTmp = authorityTemps.authoritysByType[data.authType]

    if (typeof authTmp === "undefined") {
      cb(null, { code: code.CONFIG.LOAD_CONFIG_AUTHORITY_FAIL }, null)
      return
    }

    cb(null, { code: code.OK }, authTmp)
  } catch (err) {
    logger.error("[configRemote][getAuthorityTemp] catch err", err)
    cb(null, code.FAIL, null)
  }
}

config_remote.getAuthority = function (data, cb) {
  //for log in data
  try {
    if (typeof data.authTmpId === "undefined") {
      cb(null, { code: code.CONFIG.LOAD_CONFIG_AUTHORITY_FAIL }, null)
      return
    }

    configDao.getAuthorityTemps(function (err, authTemps) {
      if (err.code === 200) {
        authorityTemps = authTemps

        var authTmp = authorityTemps.authoritysById[data.authTmpId]
        var authData = {}
        var authList = authTmp.funcs.split(",")

        for (i in authList) {
          var funs = authorityFuncs.funcById[authList[i]]

          if (typeof funs != "undefined" && typeof authData[funs.type] === "undefined") {
            authData[funs.type] = []
          }
          if (typeof funs != "undefined") {
            authData[funs.type].push(authorityFuncs.funcById[authList[i]].action)
          }
        }
        cb(null, { code: code.OK }, authData)
      }
    })
  } catch (err) {
    logger.error("[configRemote][getAuthority] catch err", err)
    cb(null, code.FAIL, null)
  }
  /*
    var authTmp = authorityTemps.authoritysById[data.authTmpId];

    if (typeof authTmp === 'undefined') {
        cb(null, { code: code.CONFIG.LOAD_CONFIG_AUTHORITY_FAIL }, null);
        return;
    }

    var authData = {};
    var authList = authTmp.funcs.split(",");

    for (i in authList) {

        var funs = authorityFuncs.funcById[authList[i]];

        if (typeof authData[funs.type] === 'undefined') {
            authData[authorityFuncs.funcById[authList[i]].type] = [];
        }
        authData[authorityFuncs.funcById[authList[i]].type].push(authorityFuncs.funcById[authList[i]].action);
    }

    cb(null, { code: code.OK }, authData);
    */
}

// config_remote.getAuthority_v2 = function (data, cb) { //for log in data

//     if (typeof data.authTmpId === 'undefined') {
//         cb(null, {
//             code: code.CONFIG.LOAD_CONFIG_AUTHORITY_FAIL
//         }, null);
//         return;
//     }
//     var authorityFuncs = [];
//     var authorityTemps ={};
//     m_async.waterfall([
//         function (cb) {
//             configDao.getAuthorityFuncs_v2(cb);
//         },
//         function (r_code, r_data, cb) {
//             authorityFuncs = r_data;
//             var param = {
//                 tempId: data.authTmpId
//             };
//             configDao.getAuthorityTemps_v2(param, cb);
//         }
//     ], function (none, r_code, r_data) {

//         if (r_code.code === 200) {
//             authorityTemps = r_data;

//             var authTmp = authorityTemps.authoritysById[data.authTmpId];
//             var authData = {};
//             var authList = authTmp.funcs.split(",");

//             for (i in authList) {
//                 var funs = authorityFuncs.funcById[authList[i]];

//                 if (typeof funs != 'undefined' && typeof authData[funs.type] === 'undefined') {
//                     authData[funs.type] = [];
//                 }
//                 if (typeof funs != 'undefined') {
//                     authData[funs.type].push(authorityFuncs.funcById[authList[i]].action);
//                 }
//             }
//             cb(null, {
//                 code: code.OK
//             }, authData);
//         }
//     });
// };

// config_remote.getTTLCounts_AuthorityTemps = function (cb) {
//     //console.log("---------config_remote---------------" + authorityTemps.length);
//     cb(null, { code: code.OK }, authorityTemps.authorityTmpCounts);
// };

config_remote.getAuthType = function (cb) {
  try {
    //console.log("---------getAuthType---------------" + JSON.stringify(levels));
    cb(null, { code: code.OK }, levels)
  } catch (err) {
    logger.error("[configRemote][getAuthType] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// config_remote.getCurrenies = function (cb) {
//     //console.log("---------getCurrenies---------------" + JSON.stringify(Currencies));
//     cb(null, { code: code.OK }, Currencies);
// };

// config_remote.getSysLimitTime = function (cb) {
//     //console.log("---------getSysLimitTime---------------" + JSON.stringify(sysLimitSec));
//     cb(null, { code: code.OK }, sysLimitSec);
// };

config_remote.getWhiteType = function (cb) {
  try {
    configDao.getWhiteType(cb)
  } catch (err) {
    logger.error("[configRemote][getWhiteType] catch err", err)
    cb(null, code.FAIL, null)
  }
}
config_remote.getBlackType = function (cb) {
  configDao.getBlackType(cb)
}
config_remote.getActionType = function (cb) {
  configDao.getActionType(function (none, r_code, r_data) {
    var mainAuth = {}
    r_data.forEach((item) => {
      if (typeof mainAuth[item.FunctionGroupL] == "undefined") {
        mainAuth[item.FunctionGroupL] = Object.assign([])
      }
      var subAuth = {
        action: item.FunctionAction,
        nameE: item.nameE,
        nameG: item.nameG,
        nameC: item.nameG,
      }
      mainAuth[item.FunctionGroupL].push(subAuth)
    })
    cb(null, r_code, mainAuth)
  })
}

// config_remote.get_now_exchange_rate = function (msg, cb) {

//     if (typeof msg === 'undefined') {
//         cb(null, { code: code.FAIL, msg: null }, null);
//         return;
//     }

//     configDao.getNowExchangeRate(msg, cb);
// }

// config_remote.getGameTags = function (cb) {
//     cb(null, { code: code.OK, msg: "" }, tags);
// }

config_remote.getSysSearchDays = function (cb) {
  try {
    cb(null, { code: code.OK, msg: "" }, searchDays)
  } catch (err) {
    logger.error("[configRemote][getSysSearchDays] catch err", err)
    cb(null, code.FAIL, null)
  }
}

config_remote.getTimezoneSet = function (cb) {
  try {
    cb(null, { code: code.OK, msg: "" }, timezoneSet)
  } catch (err) {
    logger.error("[configRemote][getTimezoneSet] catch err", err)
    cb(null, code.FAIL, null)
  }
}

// config_remote.getCountsOfPerPage = function (cb) {
//     cb(null, { code: code.OK, msg: "" }, CountsOfPerPage);
// }

config_remote.getMainCurrency = function (cb) {
  try {
    configDao.getSystemMainCurrency(function (err, mainCurrency) {
      if (err.code === 200) {
        cb(null, { code: code.OK, msg: "" }, mainCurrency)
      } else {
        cb(null, { code: code.DB.DATA_EMPTY, msg: "" }, null)
      }
    })
  } catch (err) {
    logger.error("[configRemote][getMainCurrency] catch err", err)
    cb(null, code.FAIL, null)
  }
}
