@echo off
setlocal enabledelayedexpansion

echo =====================================================================
echo Compiling TrustBank Problem 4 (Weekend vs Weekday Transaction Volume)
echo Author:  Lenkapothula Nithish Kumar Goud (HT No: 160124733327)
echo =====================================================================

REM Ensure bin directory exists
if not exist "bin" mkdir bin
del /Q bin\trustbank\problem4\*.class 2>nul

REM Retrieve Hadoop Classpath
for /f "delims=" %%i in ('hadoop classpath') do set HADOOP_CP=%%i

if "%HADOOP_CP%"=="" (
    echo Error: Unable to resolve Hadoop classpath. Ensure HADOOP_HOME is configured.
    exit /b 1
)

REM Resolve javac: prefer JAVA_HOME or jdk-17 if available
set JAVAC_CMD=javac
if exist "C:\Users\HP\Tools\jdk-17\bin\javac.exe" (
    set JAVAC_CMD="C:\Users\HP\Tools\jdk-17\bin\javac.exe"
) else if defined JAVA_HOME (
    if exist "%JAVA_HOME%\bin\javac.exe" set JAVAC_CMD="%JAVA_HOME%\bin\javac.exe"
)

echo Compiling Java source files using %JAVAC_CMD% (Target: Java 17)...
%JAVAC_CMD% --release 17 -cp "%HADOOP_CP%" -d bin src\trustbank\problem4\*.java

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Compilation failed!
    exit /b %ERRORLEVEL%
)

echo Packaging compiled classes into problem4.jar...
jar -cvf problem4.jar -C bin .

if %ERRORLEVEL% EQU 0 (
    echo [SUCCESS] problem4.jar created successfully!
) else (
    echo [ERROR] JAR packaging failed!
    exit /b %ERRORLEVEL%
)

echo =====================================================================
