module.exports = {
  SELECT: {
    DEFAULT: 0,
    adminDao: 1,
    userDao: 2,
  },

  TTL: {
    level1: 5 * 60, //5min
    level2: 10 * 60, //10min
  },

  TABLE: {
    admin: "admin",
    customer: "customer",
  },

  TABLE_KEY: {
    admin: "AdminId",
    customer: "Cid",
  },
}
