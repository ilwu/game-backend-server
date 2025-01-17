// var _poolModule = require('generic-pool');
var mysql = require("mysql")

/*
var createMysqlPool = function( app ){

    var sqlConfig = app.get('mysql');
    //console.log("-------createMysqlPool-------------"+JSON.stringify(sqlConfig));

    return _poolModule.createPool({
        name: 'mysql',
        create: function() {
            //console.log("-------createMysqlPool-------------host:"+sqlConfig.host);
            //console.log("-------createMysqlPool-------------user:"+sqlConfig.user);
            //console.log("-------createMysqlPool-------------password:"+sqlConfig.password);
            //console.log("-------createMysqlPool-------------database:"+sqlConfig.database);
            var client = mysql.createConnection({
                host:sqlConfig.host,
                user:sqlConfig.user,
                password:sqlConfig.password,
                database:sqlConfig.database
            });
            return client;
        },
        destroy: function(client) {
            client.end();
        },
        max: 10,
        idleTimeoutMillis : 30000,
        log : false
    });

};

*/

var createMysqlPool = function (sqlConfig) {
  //var sqlConfig = app.get('mysql');
  return mysql.createPool({
    connectionLimit: 3,
    host: sqlConfig.host,
    user: sqlConfig.user,
    port: sqlConfig.port,
    password: sqlConfig.password,
    database: sqlConfig.database,
  })
}

exports.createMysqlPool = createMysqlPool
