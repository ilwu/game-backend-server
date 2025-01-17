# Backend Server

## 需求

- Node.js 16+ ( LTS 版本 )

## 安裝

### 全域套件

```bash
npm i -g pomelo
```

### 套件

可以選擇使用手動進入個別資料夾安裝，或使用腳本安裝

### 手動安裝

```bash
cd ./game-server

npm ci

cd ..

cd ./web-server

npm ci
```

### 腳本安裝 Linux , Mac

```bash
bash npm-install.sh
```

### 腳本安裝 Windows

```bash
npm-install.bat
```

## 啟動

使用兩個終端機視窗分別啟動

game-server

```bash
cd ./game-server

pomelo start
```

web-server

```bash
cd ./web-server

node app
```

## 連線

### MySQL

設定檔案

- 開發環境 `game-server/config/development/mysql*`
- 一般環境 `game-server/config/production/mysql*`

### MongoDB

設定檔案

- `game-server/app/util/mongoDB.js`

### Redis

設定檔案

- 開發環境 `game-server/config/development/redis.json`
- 一般環境 `game-server/config/production/redis.json`

## 設定

檔案位置

- `game-server/config/js/conf.js`

參數

- `CRON`

  0 為關閉排程，1 為開啟

- `TIME_ZONE_SET`

  固定為 UTC+0 的`America/Danmarkshavn`，請勿修改

- `API_SERVER_URL`

  API Server 位置，主要請求 WebConnector 的/bsAction

- `API_SERVER_PARSER_URL`

  API Server 的 pageJumper 位置，處理轉址用，目前只有 VA 子單服務會用到
