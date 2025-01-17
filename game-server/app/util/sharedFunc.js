var shared = module.exports

/*
 * 将一个数组分成几个同等长度的数组
 * array[分割的原数组]
 * size[每个子数组的长度]
 */

shared.sliceArray = function (array, size) {
  var result = []
  for (var x = 0; x < Math.ceil(array.length / size); x++) {
    var start = x * size
    var end = start + size
    result.push(array.slice(start, end))
  }
  //console.log('-result-',JSON.stringify(result));
  return result
}

shared.delcommafy = function (num) {
  if (num.trim() == "") {
    return ""
  }
  num = num.replace(/,/gi, "")
  return parseFloat(num)
}

shared.makeid = function (num) {
  var text = ""
  var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
  for (var i = 0; i < num; i++) text += possible.charAt(Math.floor(Math.random() * possible.length))
  return text
}
