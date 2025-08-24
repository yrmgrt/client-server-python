@echo off 

pushd "%~dp0"
python -m PyInstaller --onefile -i favicon.ico -n ClientServer.exe --optimize 2 -p "%CD%;%CD%\src"
copy /y dist\ClientServer.exe "%CD%\ClientServer.exe"
popd 