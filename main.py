import os
import pandas as pd
from pysus.online_data import SINAN
from google.cloud import storage
from datetime import datetime

def run_oda_pipeline():
    # 1. Configurações
    BUCKET_NAME = "dados_alagoinhas_bronze"
    DESTINATION_FOLDER = "saude/acidentes_trabalho"
    COD_ALAGOINHAS = "290070"
    ANOS = range(2015, 2027) # De 2015 até 2026
    
    print("Conectando ao SINAN...")
    # Versão mais estável para chamada do PySUS
    sinan = SINAN()
    
    lista_dfs = []
    
    # 2. Loop para Série Histórica
    for ano in ANOS:
        print(f"Buscando dados de {ano}...")
        try:
            # ACGR: Acidente de Trabalho Grave
            arquivos = sinan.get_files(dis_code="ACGR", year=ano)
            
            if not arquivos:
                print(f"⚠️ Nenhum arquivo para {ano} encontrado.")
                continue

            # Download e Conversão
            arquivo_baixado = arquivos[0].download()
            df_temp = arquivo_baixado.to_dataframe()
            
            # Filtro Alagoinhas (Nota: a coluna pode variar entre ID_MN_RESI ou ID_MUNIC_RES)
            col_munic = 'ID_MN_RESI' if 'ID_MN_RESI' in df_temp.columns else 'ID_MUNIC_RES'
            
            df_filtrado = df_temp[df_temp[col_munic].astype(str).str.startswith(COD_ALAGOINHAS)]
            
            if not df_filtrado.empty:
                df_filtrado['ANO_REFERENCIA'] = ano
                lista_dfs.append(df_filtrado)
                print(f"✅ {len(df_filtrado)} registros encontrados para {ano}.")
            else:
                print(f"ℹ️ Sem registros de Alagoinhas em {ano}.")
                
        except Exception as e:
            print(f"❌ Erro ao processar ano {ano}: {e}")

    # 3. Consolidação e Upload
    if not lista_dfs:
        print("Nenhum dado encontrado em toda a série histórica.")
        return

    df_final = pd.concat(lista_dfs, ignore_index=True)
    local_filename = "acidentes_trabalho_serie_historica.csv"
    
    # Salvando localmente
    df_final.to_csv(local_filename, index=False, sep=';', encoding='utf-8')
    
    # 4. Upload para o Cloud Storage
    try:
        print(f"Subindo série histórica para o bucket {BUCKET_NAME}...")
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"{DESTINATION_FOLDER}/{local_filename}")
        
        blob.upload_from_filename(local_filename)
        print(f"🚀 Sucesso! Série histórica disponível no Storage.")
        
        # Limpeza do arquivo local (boa prática no Cloud Run)
        os.remove(local_filename)
    except Exception as e:
        print(f"❌ Erro no upload para o Storage: {e}")

if __name__ == "__main__":
    run_oda_pipeline()