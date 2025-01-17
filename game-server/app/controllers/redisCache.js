var redisCacheCode = require("../../share/redis/redisCacheCode")
var redisSetting = require("./../../config/redisCache")
var redis = require("then-redis")
var logger = require("pomelo-logger").getLogger("redisCache", __filename)
var C = require("../../share/constant")

const DB_SELECT = {
  API_CACHE: 0,
  REQ_DEF: 1,
}

const REQDEF = "reqDef:"
const ONLINE_PLAYER_NUM_KEY = "onlinePlayerNum"

const GAME_DATA = "GAME_DATA:"
const ACTIVE_ONLINE_PLAYER_COUNTS = "ACTIVE_ONLINE_PLAYER_COUNTS:"

const COMMON_TTL = 10 * 60
const ONE_DAY_TTL = 24 * 60 * 60

const Controller = function (app) {
  this.app = app

  //寫
  this.redisMaster = redis.createClient({
    host: redisSetting.master.host,
    port: redisSetting.master.port,
    database: redisSetting.master.db,
  })
  //讀
  this.redisSlave = redis.createClient({
    host: redisSetting.master.host,
    port: redisSetting.master.port,
    database: redisSetting.master.db,
  })
}

module.exports = function (app) {
  return new Controller(app)
}

const proto = Controller.prototype

proto.getRedisMaster = function () {
  this.redisMaster.select(redisSetting.master.db)
  return this.redisMaster
}
proto.getRedisSlave = function () {
  this.redisSlave.select(redisSetting.master.db)
  return this.redisSlave
}

// --- game data ---

proto.getGameData = async function (data) {
  const self = this
  const db = self.redisSlave
  db.select(DB_SELECT.API_CACHE)

  const { gameId } = data

  const key = `${GAME_DATA}${gameId}`

  const cache = await db.get(key)

  if (cache) {
    const result = JSON.parse(cache)

    return { ...result, isFromCache: true }
  }

  return cache
}

proto.setGameData = function (data) {
  const self = this
  const db = self.redisMaster
  db.select(DB_SELECT.API_CACHE)

  const { gameId } = data

  const key = `${GAME_DATA}${gameId}`

  db.setex(key, ONE_DAY_TTL, JSON.stringify(data))
}

//取Key
proto.getRedisCacheKey = function (table_name, uniqueKey) {
  return [table_name, uniqueKey].join(":")
}

//優先取REDIS的緩存，若無才取DB資料並寫入REDIS
proto.getCacheOrDoActQuery = function (db_name, table_name, uniqueKey, sql, args, selectDB, ttl, cb) {
  let key = this.getRedisCacheKey(table_name, uniqueKey)
  this.redisSlave.select(selectDB)
  this.redisSlave.get(key, function (err, res) {
    if (err) logger.warn("[redisCache][getCacheOrDoActQuery] err : ", err)

    //缓存存在
    if (res)
      // 直接回傳
      cb(null, { code: code.OK }, JSON.parse(res))
    //向DB執行Action並寫入REDIS
    else this.doActQueryAndSetCache(db_name, table_name, uniqueKey, sql, args, selectDB, ttl, cb)
  })
}

//更新資料有機會影響到REDIS緩存時，統一清掉緩存下次重取避免多台同時操作REDIS寫入時造成lock err
proto.insertOrUpdateDataAndDelCache = function (db_name, table_name, uniqueKey, sql, args, selectDB, ttl, cb) {
  //不管insert還是update都需清掉緩存
  try {
    let key = this.getRedisCacheKey(table_name, uniqueKey)
    this.redisMaster.select(selectDB)
    this.redisMaster.del(key)
  } catch (e) {
    logger.warn("[redisCache][insertOrUpdateDataAndDelCache], e: ", e)
  }
  //向DB執行Action並寫入REDIS
  this.doActQueryAndSetCache(db_name, table_name, uniqueKey, sql, args, selectDB, ttl, cb)
}

//取DB資料並寫入REDIS
proto.doActQueryAndSetCache = function (db_name, table_name, uniqueKey, sql, args, selectDB, ttl, cb) {
  let isSelect = sql.toLowerCase().indexOf("select") >= 0 && sql.toUpperCase().indexOf("SELECT") >= 0
  db.act_query(db_name, sql, args, function (r_code, r_data) {
    if (r_code.code !== code.OK) {
      cb(null, r_code, null)
    } else {
      try {
        if (isSelect) {
          //寫到緩存
          this.redisMaster.select(selectDB)
          let key = this.getRedisCacheKey(table_name, r_data[0][redisCacheCode.TABLE_KEY[table_name]])
          this.redisMaster.set(key, JSON.stringify(r_data[0]), "EX", ttl ? ttl : -1)
        }
      } catch (e) {
        logger.warn("[redisCache][doActQueryAndSetCache], e: ", e)
      }
      if (r_data.length > 0) cb(null, r_code, r_data[0])
      else cb(null, r_code, r_data)
    }
  })
}

//防止惡意連續事件請求
proto.checkRequestDef = async function (playerId, requestDefData) {
  try {
    //檢查是否已被逞罰
    let lockKey = REQDEF + requestDefData.lockKey + playerId
    this.redisSlave.select(DB_SELECT.REQ_DEF)
    let checkLock = await this.redisSlave.get(lockKey)
    //事件請求太多次的逞罰
    if (checkLock) return { code: C.REQUEST_TOO_SOON }

    //計算請求次數
    let redisKey = REQDEF + requestDefData.redisKey + playerId
    this.redisMaster.select(DB_SELECT.REQ_DEF)
    let requestTime = await this.redisMaster.incr(redisKey)
    if (requestTime == 1)
      //第一次要設定過期時間
      await this.redisMaster.expire(redisKey, requestDefData.TTL)
    else if (requestTime >= requestDefData.requestCount) {
      //超過將記錄清掉，也避免高並發時第一次未設定到expire的問題
      this.redisMaster.del(redisKey)

      //設定逞罰紀錄
      this.redisMaster.set(lockKey, 0)
      this.redisMaster.expire(lockKey, requestDefData.lockTime)
      //事件請求太多次的逞罰
      return { code: C.REQUEST_TOO_SOON }
    }
    return { code: C.OK }
  } catch (err) {
    logger.error("[redisCache][checkRequestDef] err: ", err)
  }
}

// 取得線上玩家人數列表
// 回傳 [{gameId, ggId, dc, agentId, [playerId]}]
proto.getOnlinePlayerNums = async function () {
  let nums = []
  try {
    await this.redisSlave.select(DB_SELECT.API_CACHE)

    let redisKey = `${ONLINE_PLAYER_NUM_KEY}`

    let res = await this.redisSlave.hgetall(redisKey)
    if (res) {
      for (let gameId in res) {
        let items = JSON.parse(res[gameId])
        for (let item of items) {
          item.gameId = parseInt(gameId)
          nums.push(item)
        }
      }
    }
    return nums
  } catch (err) {
    logger.error("[redisCache][getOnlinePlayerNums] err: ", err)
  }
}

// 取得有在玩遊戲的玩家數量

proto.getActiveOnlinePlayerCounts = async function (data) {
  const self = this
  const db = self.redisSlave
  db.select(DB_SELECT.API_CACHE)

  // cid + end time => testcid:2022_03_11:09_25_30
  const { cacheKey } = data

  const key = `${ACTIVE_ONLINE_PLAYER_COUNTS}${cacheKey}`

  const cache = await db.get(key)

  return cache
}

proto.setActiveOnlinePlayerCounts = function (data) {
  const self = this
  const db = self.redisMaster
  db.select(DB_SELECT.API_CACHE)

  const { cacheKey, counts } = data

  const key = `${ACTIVE_ONLINE_PLAYER_COUNTS}${cacheKey}`

  db.setex(key, COMMON_TTL, counts)
}
