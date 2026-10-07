@echo off
REM Windows entry point. The Makefile needs `make`, which Windows does not ship
REM and which is exactly the kind of thing that fails for the first time while
REM someone is watching. A .cmd file runs from both PowerShell and cmd with no
REM install and no execution-policy prompt, which a .ps1 would hit.
REM
REM   run demo      generate events and run the pipeline end to end
REM   run prove     show the logic runs with no engine installed
REM   run test      the test suite
REM   run marts     build the dbt marts and print the finding
REM   run report    write docs\results.html from the warehouse
REM   run clean     delete generated data

setlocal
cd /d "%~dp0"

if "%~1"==""        goto :help
if /i "%~1"=="help" goto :help
goto :%~1 2>nul || goto :unknown

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
goto :eof

:prove
python scripts/prove_separation.py
goto :eof

:test
python -m pytest tests/ -q
goto :eof

:marts
python scripts/build_marts.py
goto :eof

:report
python scripts/build_report.py
goto :eof

:clean
if exist data\bronze      rmdir /s /q data\bronze
if exist data\silver      rmdir /s /q data\silver
if exist data\raw         rmdir /s /q data\raw
if exist data\_checkpoints rmdir /s /q data\_checkpoints
if exist data\warehouse.db del /q data\warehouse.db
echo cleaned
goto :eof

:unknown
echo Unknown command "%~1".
echo.

:help
echo.
echo   run demo      generate events and run the pipeline end to end  (~7s)
echo   run prove     show the logic runs with no engine installed     (~13s)
echo   run test      the test suite                                   (~12s)
echo   run marts     build the dbt marts and print the finding
echo   run report    write docs\results.html from the warehouse
echo   run clean     delete generated data
echo.
goto :eof
