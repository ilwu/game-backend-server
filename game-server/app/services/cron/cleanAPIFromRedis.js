var schedule = require("pomelo-schedule")
var conf = require("../../../config/js/conf")
var pomelo = require("pomelo")
////////////////////////////////////////////////////////////////////
//清除逾時的token
var del_api_info_from_redis = function (app) {
  let redis = require("redis")
  var redis_client = app.get("redis_client")
  var now = new Date().getTime()
  //redis_client.del('api_token');
  redis_client.get("api_token", (error, result) => {
    if (error) {
      console.log("--error--", error)
      throw error
    }
    var api_tokens = {}

    if (result != null) {
      //已加
      api_tokens = JSON.parse(result)
      // console.log('-result',api_tokens);
    }

    Object.keys(api_tokens).forEach((key) => {
      var end_time = api_tokens[key]["end_time"]
      if (now > end_time) {
        delete api_tokens[key]
        //  console.log('api_tokens',JSON.stringify(api_tokens));
      } //超過時間 刪除
    })
    redis_client.set("api_token", JSON.stringify(api_tokens), redis.print) //2.更新 token
  })
}

if (conf.CRON == 1) {
  schedule.scheduleJob("0/3 * * * * *", del_api_info_from_redis, pomelo.app)
}
