import pandas as pd
from access_db import ConfiguracionConexion, AccessDB

def leer_datos_oracle():
    print("Iniciando conexión a Oracle...")
    
    conn_config = ConfiguracionConexion(config_id="DWRAC", ruta='config_acceso.yaml')                
    db = AccessDB(conn_config)                
    
    tabla = "DWVEG_ORT.RMG_DIM_DISTANCIA"
    
    query = f"""
        SELECT LOC_ORIGEN, LOC_DESTINO, DISTANCIA_KM, TIEMPO_MIN 
        FROM {tabla}
    """
    
    print(f"Leyendo datos de la tabla: {tabla}...")
    
    
    df = db.get_dataframe(query)
    
    print("\n✅ Datos leídos exitosamente.")
    print(f"Total filas: {len(df)}")
    print(df.head())
    
    return df

if __name__ == "__main__":
    try:
        datos = leer_datos_oracle()
    except Exception as e:
        print(f"\n❌ Ocurrió un error: {e}")