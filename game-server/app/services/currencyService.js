/**
 *
 * 回應廠商DS的特例幣別代碼
 *
 * isReverse = true 我方幣別代碼轉成廠商幣別代碼
 *
 * isReverse = false 廠商幣別代碼轉成我方幣別代碼
 *
 * @param {string} currencyCode
 * @param {boolean} isReverse
 * @returns currencyCode
 */
const getCurrencyCodeOnDS = (currencyCode, isReverse) => {
  const list = [
    ["1VND", "VND"],
    ["kVND", "VND2"],
  ]

  const currencyMap = isReverse === true ? new Map(list.map((x) => x.reverse())) : new Map(list)

  return currencyMap.get(currencyCode) || currencyCode
}

/**
 *
 * 回應廠商Funky的特例幣別代碼
 *
 * isReverse = true 我方幣別代碼轉成廠商幣別代碼
 *
 * isReverse = false 廠商幣別代碼轉成我方幣別代碼
 *
 * @param {string} currencyCode
 * @param {boolean} isReverse
 * @returns currencyCode
 */
const getCurrencyCodeOnFunky = (currencyCode, isReverse) => {
  const list = [
    ["AUD", "AUD2"],
    ["CAD", "CAD2"],
    ["CHF", "CHF2"],
    ["CNY", "CNY2"],
    ["EUR", "EUR2"],
    ["GBP", "GBP2"],
    ["HKD", "HKD2"],
    ["IDR", "IDR2"],
    ["INR", "INR2"],
    ["JPY", "JPY2"],
    ["KRW", "KRW2"],
    ["MMK", "MMK2"],
    ["MYR", "MYR2"],
    ["NOK", "NOK2"],
    ["NZD", "NZD2"],
    ["PHP", "PHP2"],
    ["SEK", "SEK2"],
    ["SGD", "SGD2"],
    ["THB", "THB2"],
    ["USD", "USD2"],
    ["VND", "VND2"],
    ["ZAR", "ZAR2"],
    ["ZWD", "ZWD2"],
  ]

  const currencyMap = isReverse === true ? new Map(list.map((x) => x.reverse())) : new Map(list)

  return currencyMap.get(currencyCode) || currencyCode
}

/**
 *
 * 針對不同DC的幣別，對應到我方的幣別代碼
 *
 * isReverse = true 我方幣別代碼轉成廠商幣別代碼
 *
 * isReverse = false 廠商幣別代碼轉成我方幣別代碼
 *
 * @param {string} dc
 * @param {string} currencyCode
 * @param {boolean} isReverse
 * @returns currencyCode
 */
const mappingCurrencyCode = (dc, currencyCode, isReverse = false) => {
  switch (dc) {
    case "DS":
      return getCurrencyCodeOnDS(currencyCode, isReverse)
    case "FUNKY":
      return getCurrencyCodeOnFunky(currencyCode, isReverse)
    default:
      return currencyCode
  }
}

module.exports = {
  mappingCurrencyCode,
}
