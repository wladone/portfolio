; Simple NSIS installer for SysMonDesktop
!define APPNAME "SysMonDesktop"
!define COMPANY "SysMon"
!define VERSION "0.1.0"

OutFile "..\..\dist\${APPNAME}-Setup-${VERSION}.exe"
InstallDir "$PROGRAMFILES\${APPNAME}"
ShowInstDetails nevershow
ShowUninstDetails nevershow

Page directory
Page instfiles
UninstPage uninstConfirm
UninstPage instfiles

Section "Install"
  SetOutPath "$INSTDIR"
  File /oname=${APPNAME}.exe "..\..\dist\SysMonDesktop.exe"
  CreateShortCut "$SMPROGRAMS\${APPNAME}.lnk" "$INSTDIR\${APPNAME}.exe"
  WriteUninstaller "$INSTDIR\Uninstall.exe"
SectionEnd

Section "Uninstall"
  Delete "$SMPROGRAMS\${APPNAME}.lnk"
  Delete "$INSTDIR\${APPNAME}.exe"
  Delete "$INSTDIR\Uninstall.exe"
  RMDir "$INSTDIR"
SectionEnd

