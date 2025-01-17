const BigNumber = require("bignumber.js")

const BN = BigNumber.clone({ ROUNDING_MODE: BigNumber.ROUND_FLOOR })

module.exports = {
  BN,
}
