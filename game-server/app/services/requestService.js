const fetch = require("node-fetch")
const HttpsProxyAgent = require("https-proxy-agent")
const logger = require("pomelo-logger").getLogger("requestService", __filename)
const { inspect } = require("util")

/**
 *
 * @param {string} url 目標網址
 * @param {*=} payload 要傳送的資料
 * @param {object} config 設定
 * @param {*} config.callback callback 為舊pomelo框架特別回應，預設為promise
 * @param {string} config.method http methods
 * @param {object} config.headers http headers
 * @param {boolean} config.isActiveProxy 是否啟用proxy
 * @param {string} config.proxyUrl 啟用的proxy url
 * @param {string} config.requestId
 * @param {boolean} config.isLogPayload
 * @param {boolean} config.isLogHeaders
 *
 */
const request = async (url, payload, config = {}) => {
  const {
    callback,
    method,
    headers,
    isActiveProxy,
    proxyUrl,
    requestId = "",
    isLogPayload = true,
    isLogHeaders = false,
  } = config

  try {
    const httpMethod = method ? method : "POST"
    const body = JSON.stringify(payload)

    if (payload && isLogPayload === true) {
      logger.info(`${requestId} ${httpMethod} ${url} payload => ${body}`)
    }

    const res = await fetch(url, {
      method: httpMethod,
      body: body,
      headers: headers ? headers : { "Content-Type": "application/json" },
      agent: isActiveProxy ? new HttpsProxyAgent(proxyUrl) : null,
    })

    if (isLogHeaders) {
      logger.info(`${requestId} ${url} \n ${inspect(res.headers)}`)
    }

    const { status } = res

    const raw = await res.text()

    logger.info(`${requestId} ${httpMethod} ${status} ${url} response raw => ${raw}`)

    const result = JSON.parse(raw)

    if (callback) {
      callback(null, { code: 200 }, result)
      return
    } else {
      return result
    }
  } catch (err) {
    logger.error(`${requestId} request url ${url} with ${JSON.stringify(payload)} failed,\n\n ${inspect(err)}`)

    if (callback) {
      callback(null, { cdoe: "9999" }, err)
      return
    }

    return null
  }
}

module.exports = {
  post: (url, payload, options = {}) => {
    return request(url, payload, { ...options, method: "POST" })
  },
  delete: (url, payload, options = {}) => {
    return request(url, payload, { ...options, method: "DELETE" })
  },
}
