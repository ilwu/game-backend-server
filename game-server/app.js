const pomelo = require("pomelo")
const routeUtil = require("./app/util/routeUtil")
const configUtil = require("./app/util/configUtil.js")
//var status = require('pomelo-status-plugin'); //記得在game server資料夾裡安裝 npm install pomelo-status-plugin
const mysql = require("./app/DataBase/mysql/mysql")
const dbUtil = require("./app/util/dbUtils.js")
const system = require("./app/servers/config/system.js")

/**
 * Init app for client.
 */
const app = pomelo.createApp()
app.set("name", "backend-service")

// app configuration
app.configure("production|development", "connector", function () {
  app.set("connectorConfig", {
    connector: pomelo.connectors.hybridconnector,
    //heartbeat: 300,
    useDict: true,
    useProtobuf: true,
  })
  /*
        app.set('sessionConfig', {
            singleSession: true
        });
    */
  app.load(require("./app/components/localCache"))
})

app.configure("production|development", "gate", function () {
  app.set("connectorConfig", {
    connector: pomelo.connectors.hybridconnector,
    useProtobuf: true,
  })
})

app.configure("production|development", "bet", function () {
  app.load(require("./app/components/localCache_bet"))
})

app.configure("development|production", function () {
  // var  db_dir = "";
  //db 移至全域
  app.loadConfig("mysql_game_r", app.getBase() + "/config/" + app.get("env") + "/mysql_game_r.json")
  app.loadConfig("mysql_game_rw", app.getBase() + "/config/" + app.get("env") + "/mysql_game_rw.json")
  app.loadConfig("mysql_wagers_r", app.getBase() + "/config/" + app.get("env") + "/mysql_wagers_r.json") //唯讀
  app.loadConfig("mysql_wagers_rw", app.getBase() + "/config/" + app.get("env") + "/mysql_wagers_rw.json") //讀寫
  app.loadConfig("mysql_log_r", app.getBase() + "/config/" + app.get("env") + "/mysql_log_r.json")
  app.loadConfig("mysql_log_rw", app.getBase() + "/config/" + app.get("env") + "/mysql_log_rw.json")
  app.loadConfig("mysql_jackpot_r", app.getBase() + "/config/" + app.get("env") + "/mysql_jackpot_r.json")
  app.loadConfig("mysql_jackpot_rw", app.getBase() + "/config/" + app.get("env") + "/mysql_jackpot_rw.json")

  const sqlConfig_game_r = app.get("mysql_game_r")
  const sqlConfig_game_rw = app.get("mysql_game_rw")
  const sqlConfig_wagers_r = app.get("mysql_wagers_r")
  const sqlConfig_wagers_rw = app.get("mysql_wagers_rw")
  const sqlConfig_log_r = app.get("mysql_log_r")
  const sqlConfig_log_rw = app.get("mysql_log_rw")
  const sqlConfig_jackpot_r = app.get("mysql_jackpot_r")
  const sqlConfig_jackpot_rw = app.get("mysql_jackpot_rw")

  const dbclient_game_r = mysql.init(sqlConfig_game_r)
  const dbclient_game_rw = mysql.init(sqlConfig_game_rw)
  const dbclient_wagers_r = mysql.init(sqlConfig_wagers_r)
  const dbclient_wagers_rw = mysql.init(sqlConfig_wagers_rw)
  const dbclient_log_r = mysql.init(sqlConfig_log_r)
  const dbclient_log_rw = mysql.init(sqlConfig_log_rw)
  const dbclient_jackpot_r = mysql.init(sqlConfig_jackpot_r)
  const dbclient_jackpot_rw = mysql.init(sqlConfig_jackpot_rw)

  app.set("dbclient_g_r", dbclient_game_r)
  app.set("dbclient_g_rw", dbclient_game_rw)
  app.set("dbclient_w_r", dbclient_wagers_r)
  app.set("dbclient_w_rw", dbclient_wagers_rw)
  app.set("dbclient_l_r", dbclient_log_r)
  app.set("dbclient_l_rw", dbclient_log_rw)
  app.set("dbclient_j_r", dbclient_jackpot_r)
  app.set("dbclient_j_rw", dbclient_jackpot_rw)

  dbUtil.init(app)

  //系統初始資料------------------------
  system.findSystemConfigAsync().then((systemConfigs) => {
    app.set("sys_config", systemConfigs)
  })

  //redis server
  // app.loadConfig('sys_redis', app.getBase() + '/config/development/redis.json');
  // var sys_redis = app.get('sys_redis');
  // require(app.getBase() + '/app/servers/config/redis').init(sys_redis, function (client) {
  //     app.set('redis_client',client);
  // }) ;

  // 监控请求响应时间，如果超时就给出警告
  app.filter(pomelo.timeout())
})

// Configure for backend game server
app.configure("production|development", function () {
  // route configures
  app.route("config", routeUtil.config)
  // filter configures
  app.filter(pomelo.timeout())
})

// Configure for backend admin server
app.configure("production|development", function () {
  // route configures
  app.route("admin", routeUtil.admin)
  // filter configures
  app.filter(pomelo.timeout())
})

// Configure for backend user server
app.configure("production|development", function () {
  // route configures
  app.route("user", routeUtil.user)
  // filter configures
  app.filter(pomelo.timeout())
})

// Configure for backend game server
app.configure("production|development", function () {
  // route configures
  app.route("game", routeUtil.game)

  // filter configures
  app.filter(pomelo.timeout())
})

// Configure for backend api server
// app.configure('production|development', function () {
//     // route configures
//     app.route('api', routeUtil.api);

//     // filter configures
//     app.filter(pomelo.timeout());
// });

// Configure for backend bet server
app.configure("production|development", function () {
  // route configures
  app.route("bet", routeUtil.bet)
  // filter configures
  app.filter(pomelo.timeout())
})

app.configure("production|development", "master", function () {
  // rpc 使用
  app.set("proxyConfig", {
    bufferMsg: true,
    interval: 30,
    lazyConnection: true,
    timeout: 15 * 1000,
    failMode: "failfast",
  })
  app.load(pomelo.proxy, app.get("proxyConfig"))

  const revenue = require("./app/services/cron/revenue") //Revenue - 每10分鐘更新一次
  // var modRate = require('./app/services/cron/modRate'); //匯率 - 每0.5HR更新一次
  // var cleanAPIFromRedis = require('./app/services/cron/cleanAPIFromRedis'); //清除redis - 每3sec更新一次

  // backend server開機時自動檢查未結算的注單補結算 Job
  const statisticsService = require("./app/services/statisticsService")

  // 信用告警通知(信用額度使用到達告警值時發送mail) - 每 10 分鐘檢查一次
  const creditNotify = require("./app/services/cron/creditNotify")
})

const authFilter = require("./app/servers/filter/authFilter")
app.configure("production|development", "admin|user|game|bet|log", function () {
  app.filter(authFilter())
})

app.configure("all", function () {
  //db 移至全域
  // 载入mongodb数据库的配置
  //var mongodb_config = configUtil.load('mongodb');

  //mongoose.Promise = global.Promise;

  //var db = mongoose.connect('mongodb://'+mongodb_config.host+':'+ mongodb_config.port+'/'+mongodb_config.database, mongodb_config.options );
  //var db = mongoose.connect(mongodb_config.host, mongodb_config.database, mongodb_config.port, mongodb_config.options).connection;
  //發生 錯誤DeprecationWarning: `open()` is deprecated in mongoose >= 4.11.0, use `openUri()` instead, or set the `useMongoClient` option if using `connect()` or `createConnection()`.
  //要在mongodb.json 裡options 增加 "useMongoClient":true,
  //console.log('host '+mongodb_config.host+" db "+mongodb_config.database+" port "+mongodb_config.port+" options "+mongodb_config.options);
  // 当数据库连接失败和成功时的处理
  //db.on('error', console.error.bind(console, 'connection error:'));
  //db.once('open', function callback() {
  //   //console.log(' test DB connect success');

  //});

  //监控请求响应时间，如果超时就给出警告

  app.filter(pomelo.timeout())
})

// start app
app.start()

process.on("uncaughtException", function (err) {
  console.error(" Caught exception: " + err.stack)
})

/*
const redis = require('redis');
const client = redis.createClient(); // this creates a new client

client.on('connect', () => {
  console.log('Redis client connected');
});
client.set('foo', 'bar', redis.print);
client.get('foo', (error, result) => {
  if (error) {
    console.log(error);
    throw error;
  }
  console.log('GET result ->' + result);
});
 
*/
