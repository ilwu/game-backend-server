var betlogSchema = require("./Schema/betHistorySchemas")
var memberSchema = require("./Schema/memberSchemas")

var utils = require("../util/utils")
var code = require("../util/code")
var mongoose = require("mongoose")
var betHistory_DB = mongoose.model("betlog")
var memberDB = mongoose.model("member")
var accDB = module.exports

accDB.createBethistory = function (betInfo, usrInfo, cb) {
  //var authId = mongoose.Types.ObjectId();
  //var betDB_module = new betHistory_DB();

  //betDB_module.user_id = usrId;
  //betDB_module.auth_id = authId;
  //betDB_module.created = created;
  //betDB_module.expired = expired;

  betHistory_DB.create(
    {
      round_id: 0,
      user_id: usrInfo._id,
      parent_id: usrInfo.parent_id,
      ancestorsId: usrInfo.ancestorsId,
      game_name: betInfo.game_name,
      game_serial: betInfo.game_serial,
      bet: betInfo.bet,
      win: betInfo.win,
      denom: betInfo.denom,
      rng: betInfo.rng,
    },
    function (err) {
      if (err) {
        utils.invokeCallback(cb, { code: code.FAIL, msg: "" }, null)
      } else {
        utils.invokeCallback(cb, { code: code.OK, msg: "" }, null)
      }
    }
  )
}

accDB.updateUsrBalance = function (usrId, balance, cb) {
  memberDB.update(
    { _id: mongoose.mongo.ObjectId(usrId) },
    {
      $set: {
        balance: balance,
      },
    },
    function (err) {
      if (err) {
        utils.invokeCallback(cb, { code: code.FAIL, msg: "" }, null)
      } else {
        utils.invokeCallback(cb, { code: code.OK, msg: "" }, null)
      }
    }
  )
}
