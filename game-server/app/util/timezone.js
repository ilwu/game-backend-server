const moment = require("moment-timezone")
const timezone = module.exports
const conf = require("../../config/js/conf")

moment.tz.setDefault(conf.TIME_ZONE_SET)

timezone.DEFAULT_DATE_FORMAT = "YYYY-MM-DD HH:mm:ss"

timezone.getServerTime = (time) => {
  return time ? moment(time).tz(conf.TIME_ZONE_SET) : moment().tz(conf.TIME_ZONE_SET)
}

timezone.UTCToLocal = function (time) {
  if (time == "" || time == "undefined") return ""
  if (time == "0000-00-00 00:00:00" || time == "0000-00-00") return ""
  if (time.length == 10 && /^[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}/.test(time)) time += " 00:00:00"

  //time轉換
  /*
    var converDate = time.replace(/-/g, '/');
    var utcTime = new Date(converDate);   
    var localTime = utcTime.getTime() - (utcTime.getTimezoneOffset() * 60 * 1000); //時間差
    var d = new Date(localTime);
    return self.setTime(d);
    */

  time = time.replace(" ", "T")
  return moment.utc(time).tz(conf.TIME_ZONE_SET).format("YYYY-MM-DD HH:mm:ss")
}

timezone.LocalToUTC = function (time) {
  if (time == "" || time == "undefined") return ""
  if (time == "0000-00-00 00:00:00" || time == "0000-00-00") return ""
  if (time.length == 10 && /^[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}/.test(time)) time += " 00:00:00"
  /*
    //time轉換
    var converDate = time.replace(/-/g, '/');
    var localtime = new Date(converDate);
    var utcTime = localtime.getTime() + (localtime.getTimezoneOffset() * 60 * 1000); //時間差
    var d = new Date(utcTime); 
    return self.setTime(d);
    */
  time = time.replace(" ", "T")
  return moment(time).tz(conf.TIME_ZONE_SET).utc().format("YYYY-MM-DD HH:mm:ss")
}

timezone.setTime = function (d) {
  var d_Year = d.getFullYear().toString()
  var d_Mon = conver((d.getMonth() + 1).toString())
  var d_Date = conver(d.getDate().toString())

  var d_Hour = conver(d.getHours().toString())
  var d_Min = conver(d.getMinutes().toString())
  var d_Sec = conver(d.getSeconds().toString())

  var modTime = []

  modTime.push([d_Year, d_Mon, d_Date].join("-"))
  modTime.push([d_Hour, d_Min, d_Sec].join(":"))

  var modTime_text = modTime.join(" ")
  return modTime_text
}

//月,日,時,分,秒 (個位數) 前面補0
function conver(time) {
  return time.length == 1 ? "0" + time : time
}

timezone.getTimeStamp = function (time) {
  return time != undefined ? moment.utc(time).valueOf() : moment().valueOf()
}

timezone.formatTime = function (timestamp, formatField) {
  formatField = formatField == "" || formatField == undefined ? "YYYY-MM-DD HH:mm:ss" : formatField
  return moment(timestamp).format(formatField)
}

//分鐘:時間-UTC
timezone.utcOffset = function () {
  return moment().tz(conf.TIME_ZONE_SET).utcOffset()
}
/*
timeUnit: days , hours , minutes ,seconds
*/
timezone.transTime = function (time, timeDiff, timeUnit) {
  var moment_obj = time == "" ? moment() : moment(time)
  if (timeUnit == undefined || timeUnit == "") {
    timeUnit = "hour"
  }
  return moment_obj.tz(conf.TIME_ZONE_SET).add(timeDiff, timeUnit).format("YYYY-MM-DD HH:mm:ss")
}

// 取前一天日期
timezone.transYesterdayTime = function (time, timeDiff) {
  var moment_obj = time == "" ? moment() : moment(time)
  return moment_obj.tz(conf.TIME_ZONE_SET).subtract(timeDiff, "days").format("YYYY-MM-DD HH:mm:ss")
}

// 取上個月第一天
timezone.transMonthFirstdayTime = function (time, timeDiff) {
  var moment_obj = time == "" ? moment() : moment(time)
  return moment_obj.startOf("month").subtract(timeDiff, "month").format("YYYY-MM-DD HH:mm:ss")
}

// 取上個月最後一天
timezone.transMonthLastdayTime = function (time, timeDiff) {
  var moment_obj = time == "" ? moment() : moment(time)
  return moment_obj.endOf("month").subtract(timeDiff, "month").endOf("month").format("YYYY-MM-DD HH:mm:ss")
}

//取日期
timezone.getDays = function (year, month) {
  if (year == undefined || month == undefined) {
    //當月份日期
    year = moment().tz(conf.TIME_ZONE_SET).format("YYYY")
    month = moment().tz(conf.TIME_ZONE_SET).format("MM")
  }
  return moment(year + "-" + month, "YYYY-MM").daysInMonth()
}

timezone.serverTime = function (formatField) {
  formatField = formatField == "" || formatField == undefined ? "YYYY-MM-DD HH:mm:ss" : formatField
  return moment().tz(conf.TIME_ZONE_SET).format(formatField)
}

// 查詢是否早於後面的時間
timezone.isBefore = function (time1, time2) {
  var moment_obj = time2 == "" ? moment() : moment(time2)
  return moment(time1).isBefore(time2)
}

// 查詢是否和後面的時間相等
timezone.isSame = function (time1, time2) {
  var moment_obj = time2 == "" ? moment() : moment(time2)
  return moment(time1).isSame(time2)
}

// 取得兩個日期之間的時間差
timezone.isDiff = function (time1, time2, param) {
  if (param != undefined && param == "days") {
    // 返回天
    return moment(time1).diff(time2, param)
  } else {
    // 返回毫秒
    return moment(time1).diff(time2)
  }
}

timezone.timezoneMap = new Map([
  [-5, "America/Bogota"],
  [-4, "America/Caracas"],
  [0, "Atlantic/Reykjavik"],
  [8, "Asia/Taipei"],
])

timezone.transferToISOString = (dateString) => {
  if (!dateString) return ""

  let t = dateString.split(" ").join("T") + ".000Z"

  return t
}

timezone.toUTCFormatString = (date = new Date(), formaString = "YYYY-MM-DD HH:mm:ss") => {
  return moment(date).utc().format(formaString)
}

timezone.getMappedTimezoneString = (diffHours) => {
  return timezone.timezoneMap.get(diffHours)
}

timezone.getTimezoneDate = (selectedTimezoneDiffHours, date = new Date(), keepLocalTime = false) => {
  const timezoneString = timezone.getMappedTimezoneString(selectedTimezoneDiffHours)

  const result = moment(date).tz(timezoneString, keepLocalTime)

  return result
}

timezone.formatHourDiff = (hourDiff) => {
  const numberSymbol = Number(hourDiff) >= 0 ? "+" : "-"

  const hour = Math.abs(hourDiff).toString().padStart(2, "0")

  const result = `${numberSymbol}${hour}:00`

  return result
}
