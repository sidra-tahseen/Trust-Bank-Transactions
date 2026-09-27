@echo off

echo =====================================================================
echo Executing TrustBank Problem 4 MapReduce Job
echo Author: Lenkapothula Nithish Kumar Goud (HT No: 160124733327)
echo =====================================================================

set INPUT_PATH=%~1
if "%INPUT_PATH%"=="" set INPUT_PATH=..\dataset\transactions.csv

set OUTPUT_PATH=%~2
if "%OUTPUT_PATH%"=="" set OUTPUT_PATH=output

if not exist "problem4.jar" (
    echo problem4.jar not found! Running compile.bat first...
    call compile.bat
    if errorlevel 1 exit /b 1
)

echo Input Dataset:  %INPUT_PATH%
echo Output Target:  %OUTPUT_PATH%

call hadoop jar problem4.jar trustbank.problem4.WeekendWeekdayDriver "%INPUT_PATH%" "%OUTPUT_PATH%"

if errorlevel 1 (
    echo.
    echo [ERROR] MapReduce job execution failed!
    exit /b 1
)

echo.
echo =====================================================================
echo JOB RESULTS (from %OUTPUT_PATH%\part-r-00000):
echo =====================================================================
if exist "%OUTPUT_PATH%\part-r-00000" (
    type "%OUTPUT_PATH%\part-r-00000"
)

echo =====================================================================
