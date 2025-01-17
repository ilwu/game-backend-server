const m_async = require("async")
const code = require("../../util/code.js")
const configDao = require("../../DataBase/configDao")

const config = module.exports

const info = {}


config.findSystemConfigAsync = async function() {
  //查詢所有系統參數
  const allSystemParams = await configDao.findAllSystemParams()
  const myParamNames = [
    "jwt_key",
    "main_currency",
    "counts_of_per_page",
    "counts_per_page",
    "action_time_limit",
    "time_diff",
    "temp_password_time",
    "request_max_num",
    "clean_disable_ip_sec",
    "transfer_day_limit",
  ]

  for (let myParamName of myParamNames) {
    if(allSystemParams.hasOwnProperty(myParamName) === false){
      console.log(`config.init() - 無法取得系統參數 [${myParamName}]`)
      continue
    }
    const paramValue = allSystemParams[myParamName]
    //判斷參數為空
    if (!paramValue || `${paramValue}`.trim() === ""){
      console.log(`config.init() - 系統參數為空 [${myParamName}]`)
      continue
    }
    info[myParamName] = paramValue
  }
  console.log(`\n---sys info---\n ${JSON.stringify(info, null, 2)}`)
  return info
}

/**
 *
 * @param app
 * @param cb
 * @deprecated
 */
config.init = function(app, cb) {
  m_async.parallel(
    {
      jwt_key: function(cb) {
        const param = {
          item: "jwt_key",
        }
        configDao.getSystemInfo(param, function(none, r_code, r_data) {
          if (r_code.code === 200) {
            cb(null, r_data)
          } else {
            cb(null, null)
          }
        })
      },

      main_currency: function(cb) {
        const param = {
          item: "main_currency",
        }
        configDao.getSystemInfo(param, function(none, r_code, r_data) {
          if (r_code.code === 200) {
            cb(null, r_data)
          } else {
            cb(null, null)
          }
        })
      },

      counts_of_per_page: function(cb) {
        //每頁可選擇筆數的list
        const param = {
          item: "counts_of_per_page",
        }
        configDao.getSystemInfo(param, function(none, r_code, r_data) {
          if (r_code.code === 200) {
            cb(null, r_data)
          } else {
            cb(null, null)
          }
        })
      },
      counts_per_page: function(cb) {
        //user 建立時的預設美頁筆數
        const param = {
          item: "counts_per_page",
        }
        configDao.getSystemInfo(param, function(none, r_code, r_data) {
          if (r_code.code === 200) {
            cb(null, r_data)
          } else {
            cb(null, null)
          }
        })
      },
      action_time_limit: function(cb) {
        const param = {
          item: "action_time_limit",
        }
        configDao.getSystemInfo(param, function(none, r_code, r_data) {
          if (r_code.code === 200) {
            cb(null, r_data)
          } else {
            cb(null, null)
          }
        })
      },
      time_diff: function(cb) {
        const param = {
          item: "time_diff",
        }
        configDao.getSystemInfo(param, function(none, r_code, r_data) {
          if (r_code.code === 200) {
            cb(null, r_data)
          } else {
            cb(null, null)
          }
        })
      },
      temp_password_time: function(cb) {
        const param = {
          item: "temp_password_time",
        }
        configDao.getSystemInfo(param, function(none, r_code, r_data) {
          if (r_code.code === 200) {
            cb(null, r_data)
          } else {
            cb(null, null)
          }
        })
      },
      request_max_num: function(cb) {
        //每秒最多request筆數 (IP 判斷)
        const param = {
          item: "request_max_num",
        }
        configDao.getSystemInfo(param, function(none, r_code, r_data) {
          if (r_code.code === 200) {
            cb(null, r_data)
          } else {
            cb(null, null)
          }
        })
      },
      clean_disable_ip_sec: function(cb) {
        //每秒最多request筆數 (IP 判斷)
        const param = {
          item: "clean_disable_ip_sec",
        }
        configDao.getSystemInfo(param, function(none, r_code, r_data) {
          if (r_code.code === 200) {
            cb(null, r_data)
          } else {
            cb(null, null)
          }
        })
      },
      transfer_day_limit: function(cb) {
        // 轉線天數限制
        const param = {
          item: "transfer_day_limit",
        }
        configDao.getSystemInfo(param, function(none, r_code, r_data) {
          if (r_code.code === 200) {
            cb(null, r_data)
          } else {
            cb(null, null)
          }
        })
      },
    },
    function(errs, results) {
      if (!!results.main_currency) {
        info["main_currency"] = results.main_currency
      }

      if (!!results.jwt_key) {
        info["jwt_key"] = results.jwt_key
      }

      if (!!results.counts_of_per_page) {
        info["counts_of_per_page"] = results.counts_of_per_page
      }

      if (!!results.counts_per_page) {
        info["counts_per_page"] = results.counts_per_page //每頁預設筆數
      }

      if (!!results.action_time_limit) {
        info["timeout_range_sec"] = results.action_time_limit * 60 //改成秒數
      }

      if (!!results.time_diff) {
        info["time_diff_hour"] = results.time_diff //時差->小時
      }

      if (!!results.temp_password_time) {
        info["temp_password_time_sec"] = results.temp_password_time //臨時密碼時限 (秒)
      }

      if (!!results.transfer_day_limit) {
        info["transfer_day_limit"] = results.transfer_day_limit // 轉線天數限制
      }

      if (!!results.request_max_num) {
        info["request_max_num"] = results.request_max_num //每秒最多request筆數 (IP 判斷)
      }
      if (!!results.clean_disable_ip_sec) {
        info["clean_disable_ip_sec"] = results.clean_disable_ip_sec //清除disable IP
      }

      console.log("---sys info---", JSON.stringify(info))
      cb(info)
    },
  )
}
