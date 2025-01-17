var pomelo = require("pomelo")
var fs = require("fs")

exports.load = function (config_name) {
  // 定义默认环境
  var env = "development"

  if (!!pomelo.app) {
    // 获取pomelo运行环境
    env = pomelo.app.get("env")
  }

  // 配置存放路径
  var config_path = __dirname + "/../../config/" + config_name + ".json"

  var config_list
  if (fs.existsSync(config_path)) {
    config_list = require(config_path)
  } else {
    config_list = require("../../config/" + env + "/" + config_name + ".json")
  }

  if (!config_list) {
    throw new Error("config is empty")
  }

  return config_list
}
