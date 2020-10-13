@echo OFF
title %~nx0

set YYYY-MM-DD=%DATE:~6,4%-%DATE:~3,2%-%DATE:~0,2%
set HH-mm-ss=%TIME:~0,8%
echo [INFO] %YYYY-MM-DD% %HH-mm-ss% Starting the WLST script...

set PATH=%PATH%;%ORACLE_HOME%\oracle_common\common\bin

call wlst manageJmsQueues.py -skipWLSModuleScanning
pause