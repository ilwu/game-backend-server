var code = require("../../../util/code")
var adminDao = require("../../../DataBase/adminDao")

module.exports = function (app) {
  return new Backend_usr(app)
}

var Backend_usr = function (app) {
  this.app = app
}

var backend_usr = Backend_usr.prototype
