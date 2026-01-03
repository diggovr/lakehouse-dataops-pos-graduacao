@echo off
SET AWS_PROFILE=etl_engenharia_dados
SET BASE_PATH=C:\Users\Administrator\Desktop\Lakehouse\GitHub\lakehouse-dataops-pos-graduacao-1
SET LOG_PATH=%BASE_PATH%\logs

IF NOT EXIST %LOG_PATH% mkdir %LOG_PATH%

SET LOG_FILE=%LOG_PATH%\pipeline_%DATE:~10,4%-%DATE:~4,2%-%DATE:~7,2%.log

echo ===== PIPELINE START %DATE% %TIME% ===== >> %LOG_FILE%

echo [STEP 1] Legado -> Raw >> %LOG_FILE%
python %BASE_PATH%\etl\legado_to_raw\upload_legado.py >> %LOG_FILE% 2>&1

echo [STEP 2] Raw -> Trusted >> %LOG_FILE%
python %BASE_PATH%\etl\raw_to_trusted\raw_to_trusted.py >> %LOG_FILE% 2>&1

echo [STEP 3] Trusted -> Curated >> %LOG_FILE%
python %BASE_PATH%\etl\trusted_to_curated\run_ctas_athena.py >> %LOG_FILE% 2>&1

echo ===== PIPELINE END %DATE% %TIME% ===== >> %LOG_FILE%
