const mysql = require("mysql2")

module.exports.RESOURECE = {
  GAME_MASTER: "mysql_game_rw",
  GAME_SLAVE: "mysql_game_r",
  WAGERS_MASTER: "mysql_wagers_rw",
  WAGERS_SLAVE: "mysql_wagers_r",
  LOG_MASTER: "mysql_log_rw",
  LOG_SLAVE: "mysql_log_r",
  JP_MASTER: "mysql_jackpot_rw",
  JP_SLAVE: "mysql_jackpot_r",
}

const _resourceDbPool = {}

module.exports.init = function(app) {
  for (let resourceName in this.RESOURECE) {
    const resourceConfigName = this.RESOURECE[resourceName]
    const resourceConfig = app.get(resourceConfigName)
    if (!resourceConfig) {
      console.log(`resourceConfig not found! [${resourceConfigName}]`)
      continue
    }


    // 建立連接池組態
    const dbPool = mysql.createPool({
      host: resourceConfig.host,          // 數據庫服務器地址
      user: resourceConfig.user,          // 數據庫用戶名
      password: resourceConfig.password,  // 數據庫密碼
      database: resourceConfig.database,  // 數據庫名稱
      waitForConnections: true,           // 當設置為 true 時，連接池將在無可用連接時將連接請求放入隊列，等待可用連接。當設置為 false 時，將立即返回錯誤。默認為 true
      connectionLimit: 10,                // 連接池中的連接數量上限。默認值通常為 10
      queueLimit: 0,                      // 等待連接的最大隊列限制。如果設置為 0，則隊列長度為無限制。默認為 0。
    })


    // 為了方便使用 promise，可以使用 .promise() 函數
    _resourceDbPool[resourceConfigName] = dbPool.promise()
  }
}

module.exports.query = async (dbName, sql, args) => {
  const conn = await _resourceDbPool[dbName]
  return await conn.query(sql, [...args])
}
