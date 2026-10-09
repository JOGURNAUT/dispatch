@echo off
REM Windows entry point. The Makefile needs `make`, which Windows does not ship
REM and which is exactly the kind of thing that fails for the first time while
REM someone is watching. A .cmd runs from both PowerShell and cmd with no
REM install and no execution-policy prompt, which a .ps1 would hit.
REM
REM In PowerShell this is `.\run <target>`, not `run <target>`: PowerShell does
REM not resolve commands from the current directory.
REM
REM   .\run demo      pipeline end to end, then the marts and the results page
REM   .\run prove     show the logic runs with no engine installed
REM   .\run test      the test suite
REM   .\run marts     build the dbt models and print the finding
REM   .\run report    write docs\results.html from the warehouse
REM   .\run clean     delete generated data

setlocal
cd /d "%~dp0"

REM Explicit dispatch. `goto :%~1 2>nul || goto :unknown` looks tidier and is
REM not reliable: a goto to a missing label aborts the script with a non-zero
REM exit that no branch here chose, so `.\run` with no argument printed its help
REM and then reported failure.
if "%~1"==""        goto :help
if /i "%~1"=="help" goto :help
if /i "%~1"=="demo"   goto :demo
if /i "%~1"=="published" goto :published
if /i "%~1"=="prove"  goto :prove
if /i "%~1"=="test"   goto :test
if /i "%~1"=="marts"  goto :marts
if /i "%~1"=="report" goto :report
if /i "%~1"=="clean"  goto :clean
goto :unknown

:published
REM The run the live page and the resume quote: 20,000 trips, 125,352 events.
REM `demo` is the fast one for a walkthrough; this is the one whose numbers
REM match what is written down. Reproducing a published figure on demand is a
REM different claim from showing a pipeline work, and both are worth having.
python -m generator.produce --trips 20000 --days 14 --out data/raw/events.jsonl || exit /b 1
python -m transforms.run_pipeline all --source data/raw/events.jsonl --as-of 2026-09-16T00:00:00 || exit /b 1
python -m transforms.load_dimension --demo || exit /b 1
python scripts/build_marts.py || exit /b 1
python scripts/build_report.py || exit /b 1
exit /b 0

:demo
REM Everything a walkthrough needs, in one command. Chained rather than left as
REM four steps to remember in order: the report reads the marts, the marts read
REM the warehouse, and getting that order wrong in front of someone is the whole
REM reason this file exists.
python -m generator.produce --trips 5000 --days 14 --out data/raw/events.jsonl || exit /b 1
python -m transforms.run_pipeline all --source data/raw/events.jsonl --as-of 2026-09-16T00:00:00 || exit /b 1
python -m transforms.load_dimension --demo || exit /b 1
python scripts/build_marts.py || exit /b 1
python scripts/build_report.py || exit /b 1
exit /b 0

:prove
python scripts/prove_separation.py
exit /b %errorlevel%

:test
python -m pytest tests/ -q
exit /b %errorlevel%

:marts
python scripts/build_marts.py
exit /b %errorlevel%

:report
python scripts/build_report.py
exit /b %errorlevel%

:clean
if exist data\bronze       rmdir /s /q data\bronze
if exist data\silver       rmdir /s /q data\silver
if exist data\raw          rmdir /s /q data\raw
if exist data\_checkpoints rmdir /s /q data\_checkpoints
if exist data\warehouse.db del /q data\warehouse.db
echo cleaned
exit /b 0

:unknown
echo Unknown target "%~1".
echo.
call :usage
exit /b 1

:help
call :usage
exit /b 0

:usage
echo.
echo   In PowerShell these need the .\ prefix:  .\run demo
echo.
echo   demo      pipeline end to end, marts, results page    (~5s, 5k trips)
echo   published 20k trips, the numbers on the live page     (~40s)
echo   prove    show the logic runs with no engine installed           (~13s)
echo   test     the test suite                                         (~8s)
echo   marts    build the dbt models and print the finding
echo   report   write docs\results.html from the warehouse
echo   clean    delete generated data
echo.
goto :eof
