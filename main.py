import os
import pandas as pd
from pysus.online_data import SINAN  # Importa o módulo
from google.cloud import storage
import warnings

warnings.filterwarnings('ignore')

def run_oda_pipeline():
    BUCKET_NAME = "dados_alagoinhas_bronze"
    DESTINATION_FOLDER = "saude/acidentes_trabalho"
    COD_ALAGOINHAS = "290070"
    ANOS = range(2015, 2027) 
    
    print("Conectando ao SINAN...")
    # CORREÇÃO: Chamando a classe SINAN dentro do módulo SINAN
    instancia_sinan = SINAN.SINAN() 
    
    lista_dfs = []
    
    for ano in ANOS:
        try:
            print(f"Buscando arquivos para o ano {ano}...")
            arquivos = instancia_sinan.get_files(dis_code="ACGR", year=ano)
            
            if not arquivos:
                print(f"⚠️ Sem arquivos para {ano}.")
                continue

            # Download e conversão
            df_temp = arquivos[0].download().to_dataframe()
            
            # Identificação dinâmica da coluna de município
            col_munic = None
            for c in ['ID_MN_RESI', 'ID_MUNIC_RES', 'ID_MN_RES']:
                if c in df_temp.columns:
                    col_munic = c
                    break
            
            if col_munic:
                df_filtrado = df_temp[df_temp[col_munic].astype(str).str.startswith(COD_ALAGOINHAS)]
                if not df_filtrado.empty:
                    df_filtrado['ANO_REFERENCIA'] = ano
                    lista_dfs.append(df_filtrado)
                    print(f"✅ {len(df_filtrado)} registros em {ano}.")
            
        except Exception as e:
            print(f"❌ Erro no ano {ano}: {e}")

    if lista_dfs:
        df_final = pd.concat(lista_dfs, ignore_index=True)
        local_filename = "acidentes_trabalho_serie_historica.csv"
        df_final.to_csv(local_filename, index=False, sep=';', encoding='utf-8')
        
        # Upload para Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"{DESTINATION_FOLDER}/{local_filename}")
        blob.upload_from_filename(local_filename)
        print("🚀 Série histórica enviada com sucesso!")
        os.remove(local_filename)
    else:
        print("Nenhum dado processado.")

if __name__ == "__main__":
    run_oda_pipeline()