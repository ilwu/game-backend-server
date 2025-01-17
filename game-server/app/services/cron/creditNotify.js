var logger = require("pomelo-logger").getLogger("creditNotify", __filename)
var schedule = require("pomelo-schedule")
var conf = require("../../../config/js/conf")

var m_async = require("async")
var userDao = require("../../DataBase/userDao")
var configDao = require("../../DataBase/configDao")
var mailUtil = require("../../util/mail")
var mailService = require("../../services/mailService")
var mail = new mailService()

var creditNotify = function (app) {
  try {
    this.app = app
    self = this
    let origin_notify = []
    let alertMail

    m_async.waterfall(
      [
        function (cb) {
          var param = {
            item: "credit_alert_mail",
          }
          configDao.getSystemInfo(param, cb)
        },
        function (r_code, r_data, cb) {
          alertMail = r_data

          userDao.getCreditNotifyValue(cb)
        },
        function (r_code, r_data, cb) {
          let updateBatch = []
          let batchData = {}
          for (let rd of r_data) {
            rd.AlertValue.split(",").forEach((alertValue) => {
              if (parseInt(rd.NotifyValue) <= parseInt(alertValue) /*|| parseInt(rd.NotifyValue)  == 0*/) {
                origin_notify.push(parseInt(alertValue))
              }
              batchData = {
                Cid: rd.Cid,
                Currency: rd.Currency,
                AlertValue: rd.AlertValue,
                Notified: origin_notify.sort(), // 10,20,30,40,50
              }
            })

            // 排除已發送過的值
            let result = rd.AlertValue.split(",").filter((item) => {
              if (rd.originNotify != null && rd.originNotify != "") {
                // 設定值小於已經發送的值
                return item < rd.originNotify.split(",")[0]
              } else {
                return item
              }
            })

            result = result.map((i) => Number(i))
            // 寄送 Mail。Math.max.apply：找出陣列中最大的值
            if (
              (rd.originNotify == "" && origin_notify.length >= 1) ||
              (rd.NotifyValue <= Math.max.apply(rd.NotifyValue, result) && origin_notify.length >= 1)
            ) {
              // send a mail to the person set by the system
              let content_mail = mailUtil.credit_to_mail(rd.Cid, rd.UserName, rd.Currency, rd.DC, origin_notify[0])
              let mailInfo_mail = {
                to: [alertMail],
                subject: "Credit limit alert(信用額度告警)",
                content: content_mail,
              }
              mail.sendMail(mailInfo_mail, cb) // 告警 mail

              // mail to reseller
              let content_reseller = mailUtil.credit_to_reseller(
                rd.Cid,
                rd.UserName,
                rd.Currency,
                rd.DC,
                origin_notify[0]
              )
              let mailInfo_reseller = {
                to: [rd.Email],
                subject: "Credit limit alert_Reseller(信用額度告警)",
                content: content_reseller,
              }
              mail.sendMail(mailInfo_reseller, cb) // 發信給 reseller
            }

            updateBatch.push(batchData)
            origin_notify = []
            result = []
          }
          // 更新 Notified
          userDao.ModifyCreditsListBatch(updateBatch, cb)
        },
      ],
      function (none, r_code) {
        console.log("-creditNotify-", JSON.stringify(r_code))
      }
    )
  } catch (err) {
    logger.error("[creditNotify][creditNotify] catch err", err)
  }
}

module.exports = function (app) {
  return new creditNotify(app)
}

if (conf.CRON == 1) {
  schedule.scheduleJob("0 1,11,21,31,41,51 * * * *", creditNotify, {})
}
