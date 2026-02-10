"""
Função AWS Lambda para ingestão diária de dados brutos.

Esta função foi projetada para ser acionada por um agendamento (por exemplo,
diariamente via Amazon EventBridge) para ingerir dados do mercado de ações
do dia útil anterior.
"""

import json
import logging
import os
from datetime import date, timedelta

# Assumindo que o pacote de implantação da Lambda inclui o conteúdo do
# diretório 'src' e que ele é adicionado ao PYTHONPATH.
from ingestion.WebScrapping import B3Scraper

# Configura o logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------
# Variáveis de Ambiente com Padrões
# ---------------------------------------------------------
DEFAULT_TICKERS = "GOLL4,AZUL4,EMBR3,EVEB31"
DEFAULT_S3_BUCKET = "aeronaticalverifier-s3"
DEFAULT_RAW_PREFIX = "raw"

TICKERS = os.getenv("TICKERS", DEFAULT_TICKERS)
S3_BUCKET = os.getenv("S3_BUCKET", DEFAULT_S3_BUCKET)
RAW_PREFIX = os.getenv("RAW_PREFIX", DEFAULT_RAW_PREFIX)


def lambda_handler(event, context):
    """
    Handler da AWS Lambda para ingestão diária de dados de ações da B3.

    Ingere dados do dia útil anterior (D-1) e os salva em um bucket S3
    particionado em formato Parquet.

    Variáveis de Ambiente:
    - TICKERS: Lista de tickers de ações separada por vírgula (ex: "GOLL4,AZUL4").
    - S3_BUCKET: O bucket S3 para armazenar os dados brutos.
    - RAW_PREFIX: O prefixo dentro do bucket para os dados brutos (ex: "raw").
    """
    try:
        # Padrão da indústria: ingerir o dia útil anterior (D-1)
        target_date = date.today() - timedelta(days=1)
        dt_partition = target_date.strftime("%Y-%m-%d")

        # Pula fins de semana
        if target_date.weekday() > 4:  # 5=Sáb, 6=Dom
            message = f"Ingestão pulada: {dt_partition} é um fim de semana."
            logger.info(message)
            return {"statusCode": 200, "body": json.dumps({"message": message})}

        logger.info(f"Iniciando ingestão para a data: {dt_partition}")
        logger.info(f"Tickers: {TICKERS}")
        logger.info(f"S3 Bucket: s3://{S3_BUCKET}/{RAW_PREFIX}")

        scraper = B3Scraper(
            tickers=TICKERS,
            period="5d",  # Usa uma janela pequena para garantir que os dados D-1 estejam disponíveis
            interval="1d",
        )

        uris = scraper.save_to_s3_partitioned(
            bucket=S3_BUCKET, prefix=RAW_PREFIX, dt=dt_partition
        )

        for uri in uris:
            logger.info(f"Ingerido: {uri}")

        message = f"Ingestão de {len(uris)} partição(ões) para {dt_partition} concluída com sucesso."
        return {
            "statusCode": 200,
            "body": json.dumps({"message": message, "uris": uris}),
        }

    except Exception as e:
        logger.error(f"Falha na ingestão: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}