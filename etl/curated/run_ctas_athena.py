import boto3
import time
import os
from pathlib import Path

# =========================
# CONFIGURAÇÕES
# =========================
AWS_PROFILE = "etl_engenharia_dados"
AWS_REGION = "us-east-1"

ATHENA_DATABASE = "lakehouse_curated"
ATHENA_OUTPUT = "s3://entreasas-athena-query-results/"

# =========================
# RESOLVER PATH DO PROJETO
# =========================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_FOLDER = PROJECT_ROOT / "sql" / "curated_ctas"

# =========================
# SESSION AWS
# =========================
session = boto3.Session(
    profile_name=AWS_PROFILE,
    region_name=AWS_REGION
)

athena = session.client("athena")

# =========================
# FUNÇÃO EXECUTAR QUERY
# =========================
def executar_query(sql_file: Path):

    print(f"\n🚀 Executando: {sql_file.name}")

    with open(sql_file, "r", encoding="utf-8") as f:
        query = f.read()

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Database": ATHENA_DATABASE
        },
        ResultConfiguration={
            "OutputLocation": ATHENA_OUTPUT
        }
    )

    execution_id = response["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(
            QueryExecutionId=execution_id
        )["QueryExecution"]["Status"]["State"]

        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        time.sleep(2)

    if status == "SUCCEEDED":
        print(f"✅ SUCESSO: {sql_file.name}")
    else:
        reason = athena.get_query_execution(
            QueryExecutionId=execution_id
        )["QueryExecution"]["Status"].get("StateChangeReason")
        print(f"❌ ERRO em {sql_file.name}")
        print(reason)
        raise Exception("Falha na execução do Athena")

# =========================
# EXECUÇÃO
# =========================
if __name__ == "__main__":

    if not SQL_FOLDER.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {SQL_FOLDER}")

    sql_files = sorted(SQL_FOLDER.glob("*.sql"))

    if not sql_files:
        raise Exception("Nenhum arquivo SQL encontrado")

    for sql in sql_files:
        executar_query(sql)

    print("\n🎉 TODAS AS CTAS EXECUTADAS COM SUCESSO")
