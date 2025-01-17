::npm-install.bat
@echo off
::install web server dependencies && game server dependencies
cd web-server && npm ci && cd .. && cd game-server && npm ci