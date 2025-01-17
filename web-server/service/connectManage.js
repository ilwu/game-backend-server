var conn = module.exports
/* { 
    web_conn_key : token
} 
*/
var connections = {}

//移除此key
conn.removeKey = function (key) {
  if (typeof key == "undefined" || key == "") {
    return false
  }
  delete connections[key]
  return true
}

conn.getKeyByToken = function (token) {
  if (typeof token == "undefined" || token == "") {
    return false
  }
  var re_key = ""
  Object.keys(connections).forEach((key) => {
    if (connections[key] == token) {
      re_key = key
    }
  })
  return re_key
}

conn.addToken = function (key, token) {
  if (typeof key == "undefined" || key == "" || typeof token == "undefined" || token == "") {
    return false
  }
  connections[key] = token
  return true
}
