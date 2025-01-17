/**
 *  預設Server路徑捨棄mongodbConfig，方便部署時替換路徑
 */

const mongo = require("mongoose")

const mongoOptions = { useUnifiedTopology: true, useNewUrlParser: true }

const DB_URL_FISH = "mongodb://192.168.100.24:27017/fishHunter"
const DB_URL_SLOT = "mongodb://192.168.100.24:27017/slot"
const DB_URL_ARCADE = "mongodb://192.168.100.24:27017/slot"

// Mongo 的資料庫對應
const databases = new Map([
  ["fishHunter", { url: DB_URL_FISH, conn: null }],
  ["slot", { url: DB_URL_SLOT, conn: null }],
  ["arcade", { url: DB_URL_ARCADE, conn: null }],
])

/**
 *
 * @param {string} dbName Mongo的資料庫名稱
 *
 * @returns {mongo.Connection} Mongo的連線
 */
function getMongoConnection(dbName) {
  const { conn } = databases.get(dbName)
  return conn
}

/**
 *
 * @param {string} url Mongo的連線字串
 * @param {mongo.ConnectionOptions} options Mongo的ConnectionOptions
 *
 * @returns {mongo.Connection} Mongo的連線
 */
function createConnection(url, options) {
  const conn = mongo.createConnection(url, options)

  conn.on("connected", () => {
    console.log("Mongoose connection open to " + url)
  })

  conn.on("error", (err) => {
    console.error("Mongoose connection error: " + err)
  })

  conn.on("disconnected", () => {
    console.log("Mongoose connection disconnected")
  })

  return conn
}

for (let [key, value] of databases.entries()) {
  const conn = createConnection(value.url, mongoOptions)
  databases.set(key, { ...value, conn: conn })
}

module.exports = {
  getMongoConnection,
}
