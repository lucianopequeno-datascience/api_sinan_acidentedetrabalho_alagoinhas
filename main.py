import os
from pysus.online_data import SINAN
import pandas as pd
from google.cloud import storage
from datetime import datetime

def run_oda_pipeline():
    # 1. Configurações
    BUCKET_NAME = "dados_alagoinhas_bronze"
    DESTINATION_FOLDER = "saude/acidentes_trabalho"
    COD_ALAGOINHAS = "290070"
    
    print("Conectando ao SINAN...")
    sinan = SINAN.SINAN().load()
    
    # 2. Busca apenas 2026
    print("Buscando dados de 2026...")
    # O código de agravo para Acidente de Trabalho no SINAN via PySUS é "ACID"
    arquivos = sinan.get_files(dis_code="ACGR", year=2026)
    
    if not arquivos:
        print("Nenhum arquivo de 2026 encontrado no servidor.")
        return

    # 3. Download e Filtro
    print(f"Baixando {arquivos[0].name}...")
    arquivo_baixado = arquivos[0].download()
    df = arquivo_baixado.to_dataframe()
    
    # Filtrando pelo Município de Residência
    df_alagoinhas = df[df['ID_MN_RESI'] == COD_ALAGOINHAS]
    
    if df_alagoinhas.empty:
        print("Nenhum dado novo para Alagoinhas nesta carga.")
        return

    # 4. Preparação do arquivo para o Storage
    local_filename = "acidentes_trabalho_alagoinhas_2026.csv"
    df_alagoinhas.to_csv(local_filename, index=False, sep=';', encoding='utf-8')
    
    # 5. Upload para o Cloud Storage
    print(f"Subindo {local_filename} para o bucket {BUCKET_NAME}...")
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{DESTINATION_FOLDER}/{local_filename}")
    
    blob.upload_from_filename(local_filename)
    print(f"Sucesso! Arquivo disponível em {DESTINATION_FOLDER}/{local_filename}")

if __name__ == "__main__":
    run_oda_pipeline()