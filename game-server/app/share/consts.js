module.exports = {
  // 數學符號
  Math: {
    ADD: "+", // 加
    SUB: "-", // 減
    MULTIPLY: "*", // 乘
    DIVIDE: "/", // 除
  },

  CreditType: {
    ADD: "add",
    MODIFY: "modify",
    DELETE: "delete",
    RESET: "reset",
  },

  Wallets: [0, 1],

  APIServerPlatform: {
    bsAction: "/bsAction",
    whiteLabel: "/whiteLabel",
  },

  BSActionMethod: {
    kickUser: "kickUser",
  },

  whiteLabelMethod: {
    rollback: "rollback",
  },

  DemoType: {
    normal: 0, // 正式帳號
    test: 1, // 測試帳號
    demo: 2, // 試玩帳號，不寫帳
  },
}
