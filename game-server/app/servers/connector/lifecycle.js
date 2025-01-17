"use strict"

var conf = require("../../../config/js/conf")
var schedule = require("pomelo-schedule")

module.exports.afterStartup = function (app, cb) {
  let redisCache = require("../../controllers/redisCache")(app)
  app.set("redisCache", redisCache, true)

  if (conf.CRON == 1) {
    let cronCheckUserHandler = require("./handler/cron_checkUserHandler")(app)
    schedule.scheduleJob("0/30 * * * * *", cronCheckUserHandler.checkSession, app)

    let cronMarqueeHandler = require("./handler/cron_marqueeHandler")(app)
    schedule.scheduleJob("0 0 0/30 * * *", cronMarqueeHandler.userMarquee, app)
  }

  cb()
}
