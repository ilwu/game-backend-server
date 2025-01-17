/**
 *
 * 取得固定格式的錯誤訊息
 *
 * @param {object} data
 * @param {string} data.requestId
 * @param {string} data.logTag 辨別來源函式
 * @param {string} data.message 要紀錄的錯誤訊息
 * @param {string} data.sourceDataString 原始請求資料payload
 *
 * @returns errorMessage
 */
const getErrorMessage = (data) => {
  const { requestId, logTag, message, sourceDataString } = data

  const errorMessage = `${requestId} ${logTag} 
  
  message => ${message}

  source data => ${sourceDataString}
  `
  return errorMessage
}

module.exports = { getErrorMessage }
