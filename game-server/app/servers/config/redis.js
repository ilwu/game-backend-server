let redis = require("redis")

var redisClient = module.exports

redisClient.init = function (config, cb) {
  var client = redis.createClient(config.port, config.host, {})
  client.on("connect", () => {
    console.log("Redis client connected")
  })

  cb(client)
}
