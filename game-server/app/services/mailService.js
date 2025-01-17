var logger = require("pomelo-logger").getLogger("mailService", __filename)
var code = require("../util/code")
var mail = require("../util/mail")
var m_nodemailer = require("nodemailer") //寄信
var m_async = require("async")

module.exports = function () {
  return new mailService()
}

var mailService = function () {}

mailService.prototype.sendMail = function (data, cb) {
  try {
    if (
      typeof data === "undefined" ||
      typeof data.to === "undefined" ||
      typeof data.subject === "undefined" ||
      typeof data.content === "undefined"
    ) {
      cb(null, { code: code.MAIL.MAIL_PARA_FAIL })
      return
    }

    //帳密設定
    var mailTransport = m_nodemailer.createTransport({
      service: "Gmail",
      // host: mail.HOST,
      // port: mail.PORT,
      // secure: false,
      auth: {
        user: mail.AUTH_USER,
        pass: mail.AUTH_PASS,
      },
    })

    //寄信
    mailTransport.sendMail(
      {
        from: mail.FROM_ADDRESS,
        to: data.to.join(","),
        subject: data.subject,
        html: data.content,
      },
      function (err, info) {
        console.log("Message sent: %s", JSON.stringify(info))
        if (err) {
          cb(null, { code: code.MAIL.MAIL_SEND_FAIL })
          return
        }
        cb(null, { code: code.OK })
      }
    )
  } catch (err) {
    logger.error("[mailService][sendMail] catch err", err)
  }
}
