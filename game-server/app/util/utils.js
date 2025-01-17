var request = require("request")
var P = require("bluebird")

var utils = module.exports

// control variable of func "myPrint"
var isPrintFlag = false
// var isPrintFlag = true;

utils.isEmpty = (val) => {
  /**
   *  只檢查 undefined , null , empty string
   *
   *  false和0不會為empty
   *
   *  Array與Object不檢查
   *
   */

  return typeof val !== "boolean" && typeof val !== "number" && !val
}

utils.SQLBuilder = function () {
  this.limit = (data) => {
    const { isPage, page, pageCount, index } = data

    // 預設目前為第一頁
    const currentPageIndex = index || 0

    const result =
      isPage === true ? ` LIMIT ${(page - 1) * pageCount} , ${pageCount}` : ` LIMIT ${currentPageIndex} , ${pageCount}`

    return result
  }
}

/**
 * 這函式會把陣列中的物件合併成一個{key:value}
 *
 * @param arr Array<{key,value}>
 *
 */
utils.mergeKeyValuePairs = function (arr) {
  return arr.reduce((acc, cur) => {
    const { key, value } = cur
    acc = { ...acc, ...{ [key]: value } }
    return acc
  }, {})
}

/**
 * Check and invoke callback function
 */
utils.invokeCallback = function (cb) {
  if (!!cb && typeof cb === "function") {
    cb.apply(null, Array.prototype.slice.call(arguments, 1))
  }
}

/**
 * clone an object
 */
utils.clone = function (origin) {
  if (!origin) {
    return
  }

  var obj = {}
  for (var f in origin) {
    if (origin.hasOwnProperty(f)) {
      obj[f] = origin[f]
    }
  }
  return obj
}

utils.size = function (obj) {
  if (!obj) {
    return 0
  }

  var size = 0
  for (var f in obj) {
    if (obj.hasOwnProperty(f)) {
      size++
    }
  }

  return size
}

// print the file name and the line number ~ begin
function getStack() {
  var orig = Error.prepareStackTrace
  Error.prepareStackTrace = function (_, stack) {
    return stack
  }
  var err = new Error()
  Error.captureStackTrace(err, arguments.callee)
  var stack = err.stack
  Error.prepareStackTrace = orig
  return stack
}

function getFileName(stack) {
  return stack[1].getFileName()
}

function getLineNumber(stack) {
  return stack[1].getLineNumber()
}

utils.myPrint = function () {
  if (isPrintFlag) {
    var len = arguments.length
    if (len <= 0) {
      return
    }
    var stack = getStack()
    var aimStr = "'" + getFileName(stack) + "' @" + getLineNumber(stack) + " :\n"
    for (var i = 0; i < len; ++i) {
      aimStr += arguments[i] + " "
    }
    console.log("\n" + aimStr)
  }
}
// print the file name and the line number ~ end

utils.number = {
  /**
   * 函數，加法函數，用來得到精確的加法結果
   * 説明：javascript的加法結果會有誤差，在兩個浮點數相加的時候會比較明顯。這個函數返回較為精確的加法結果。
   * 參數：arg1：第一個加數；arg2第二個加數；
   * 返回值：兩數相加的結果
   * */
  add: function (arg1, arg2) {
    arg1 = arg1.toString()
    arg2 = arg2.toString()
    let arg1Arr = arg1.split(".")
    let arg2Arr = arg2.split(".")
    let d1 = arg1Arr.length == 2 ? arg1Arr[1] : ""
    let d2 = arg2Arr.length == 2 ? arg2Arr[1] : ""
    let maxLen = Math.max(d1.length, d2.length)
    let m = Math.pow(10, maxLen)
    return Number(((arg1 * m + arg2 * m) / m).toFixed(maxLen))
  },
  /**
   * 函數：減法函數，用來得到精確的減法結果
   * 説明：函數返回較為精確的減法結果。
   * 參數：arg1：第一個加數；arg2第二個加數；d要保留的小數位數（可以不傳此參數，如果不傳則不處理小數位數
   * 返回值：兩數相減的結果
   * */
  sub: function (arg1, arg2) {
    return this.add(arg1, -Number(arg2))
  },
  // ------------------------------------------------------------------ //
  _getDecimalLength: function (value) {
    var list = (value + "").split(".") // ['100', '111']
    var result = 0
    if (list[1] !== undefined && list[1].length > 0) {
      result = list[1].length
    }
    return result // 回傳小數點的長度'111'-> list[1].length = 3
  },
  // ------------------------------------------------------------------ //
  /**
   *減法方法
   *subtract(67, 66.9)   // => 0.1  OK
   */
  subtract: function (value1, value2) {
    var max = Math.max(this._getDecimalLength(value1), this._getDecimalLength(value2))
    var k = Math.pow(10, max)
    return (this.workMultiply(value1, k) - this.workMultiply(value2, k)) / k
  },
  /**
   *乘法方法
   *multiply(66.9, 100) // => 6690
   */
  workMultiply: function (value1, value2) {
    var intValue1 = +(value1 + "").replace(".", "")
    var intValue2 = +(value2 + "").replace(".", "")
    var decimalLength = this._getDecimalLength(value1) + this._getDecimalLength(value2)

    var result = (intValue1 * intValue2) / Math.pow(10, decimalLength)

    return result
  },
  /**
   * 多個數值乘法
   * */
  multiply: function (..._val) {
    let result = 1
    while (_val.length > 0) {
      result = this.workMultiply(result, +_val.shift())
    }
    return result
  },
  /**
   *除法方法
   *divide(100.599, 20.3) // => 4.955615763546798
   */
  workDivide: function (value1, value2) {
    var intValue1 = +(value1 + "").replace(".", "")
    var intValue2 = +(value2 + "").replace(".", "")
    var decimalLength = this._getDecimalLength(value2) - this._getDecimalLength(value1)

    var result = this.workMultiply(intValue1 / intValue2, Math.pow(10, decimalLength))

    return result
  },
  divide: function (..._val) {
    let result = _val.shift()

    while (_val.length > 0) {
      result = this.workDivide(result, _val.shift())
    }
    return result
  },
  /**
   * 取到小數點第二位, 無條件捨去
   */
  floor: function (value) {
    if (value < 0) {
      return this.divide(Math.floor(this.multiply(value, -100)), -100)
    }
    return this.divide(Math.floor(this.multiply(value, 100)), 100)
  },
  /**
   * 解決浮點數計算問題, 存入 *1000 取出 /1000
   * @param { 0.3 } val
   * @param { "/ or *" } doWhat
   */
  oneThousand: function (val, doWhat) {
    switch (doWhat) {
      case "*":
        return this.multiply(val, 1000)
      case "/":
        return this.divide(val, 1000)
      default:
        return val
    }
  },
}

exports.httpPost = function (url, params) {
  return new P((resolve, reject) => {
    request(
      {
        url: url,
        method: "POST",
        json: true,
        headers: {
          "content-type": "application/json",
        },
        body: params,
      },
      function (error, response, body) {
        if (!error && response.statusCode == 200) {
          resolve(body)
        } else {
          reject({ error: error, response: response })
        }
      }
    )
  })
}

exports.getGameStatus = function (status = 0) {
  const result = {
    isNormalGame: status === 0,
    isFreeGame: status === 1,
    isBonusGame: status === 2,
  }

  return result
}

/**
 *
 * @param value 數字或可以轉成數字的字串
 * @param digits 指定的小數位數，預設是第二位
 *
 * @returns 無條件捨去到指定小數位數的字串
 */
const formatFloat = (value, precision = 2) => {
  return +Number(value)
    .toFixed(precision + 2)
    .slice(0, -2)
}

exports.formatFloat = formatFloat
