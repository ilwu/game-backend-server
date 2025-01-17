module.exports = {
  CRON: 0,
  SORT_TYPE: {
    0: "DESC",
    1: "ASC",
  },

  USER_LEVEL: {
    1: "AD",
    2: "HA",
    3: "AG",
  },
  TIME_ZONE_SET: "America/Danmarkshavn",
  ADMINIP: "111.235.135.54",
  dateRange: 30,
  searchDays: 6,
  RESET_ADMIN_PWD_URL: "http://10.0.5.236:30343/templogin",
  RESET_USER_PWD_URL: "http://10.0.5.236:30911/templogin",
  GAME_SERVER: "http://35.229.216.209:3001",
  UPLOAD_IMAGE_URL: "http://192.168.29.120:8101",
  Statistics_Time: 7,

  // api server
  API_SERVER_URL: "http://127.0.0.1:8083",
  // api server 處理page jumper(轉址)
  API_SERVER_PARSER_URL: "http://127.0.0.1:8084",
}
