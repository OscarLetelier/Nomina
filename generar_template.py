import os
import glob
import pandas as pd
import numpy as np

def separar_nombres_y_apellidos(texto):
    """
    Divide una cadena de texto que representa un nombre completo en Nombres y Apellidos.
    
    Aplica una heurística basada en el conteo de palabras para determinar la separación:
    - 1 palabra: Se asume como Nombre.
    - 2 palabras: 1 Nombre + 1 Apellido.
    - 3 palabras: 1 Nombre + 2 Apellidos.
    - 4 o más palabras: 2 Nombres + El resto de palabras como Apellidos.
    
    Args:
        texto (str): Cadena de texto con el nombre del usuario.
        
    Returns:
        pd.Series: Serie de pandas indexable con dos posiciones [Nombre, Apellido].
    """
    if pd.isna(texto) or str(texto).strip() == "":
        return pd.Series(["", ""])
    
    partes = str(texto).strip().split()
    total = len(partes)
    
    if total == 1:
        return pd.Series([partes[0], ""])
    elif total == 2:
        return pd.Series([partes[0], partes[1]])
    elif total == 3:
        return pd.Series([partes[0], f"{partes[1]} {partes[2]}"])
    else:
        nombres = " ".join(partes[:2])
        apellidos = " ".join(partes[2:])
        return pd.Series([nombres, apellidos])

def formatear_nomina_cliente():
    """
    Función principal que orquesta la importación, normalización y exportación de nóminas.
    
    Flujo de ejecución:
    1. Verifica la existencia del directorio de entrada.
    2. Itera sobre cada archivo .xlsx y sus respectivas hojas (tabs).
    3. Normaliza los encabezados usando un diccionario de traducción (alias).
    4. Concatena todos los datos en un único DataFrame en memoria.
    5. Deduplica los registros utilizando el RUT como identificador único.
    6. Aplica reglas de negocio específicas (separación de nombres, formato RBD).
    7. Filtra el DataFrame resultante contra la estructura estricta del template oficial.
    8. Exporta el archivo consolidado.
    """
    
    # --- CONFIGURACIÓN DE RUTAS ---
    carpeta_entrada = 'archivos_cliente'
    archivo_salida = 'Template_Listo_Para_Subir.xlsx'

    # Validar y crear directorio de trabajo si no existe
    if not os.path.exists(carpeta_entrada):
        os.makedirs(carpeta_entrada)
        print(f"[INFO] Se creó el directorio de trabajo '{carpeta_entrada}'.")
        print("[INFO] Mueva los archivos Excel del cliente a esta carpeta y vuelva a ejecutar.")
        return

    archivos_excel = glob.glob(f"{carpeta_entrada}/*.xlsx")
    
    if not archivos_excel:
        print(f"[ERROR] No se encontraron archivos Excel (.xlsx) en '{carpeta_entrada}'.")
        return

    # --- DEFINICIÓN DE ESTRUCTURAS ---
    columnas_template = [
        'nombre', 'apellido', 'rut', 'correo', 'direccion', 'telefono', 
        'fecha de ingreso', 'fecha de nacimiento', 'cargo', 'tipo de usuario', 
        'centro de trabajo', 'subempresa', 'empresacontratista', 
        'rut supervisor', 'correo de bienvenida', 'área'
    ]

    # Diccionario de equivalencias para estandarizar los nombres de columnas entrantes
    alias_columnas = {
        'nombre_completo': ['nombre completo', 'nombres y apellidos', 'nombre trabajador', 'trabajador', 'colaborador', 'empleado', 'nombre'],
        'correo': ['email', 'correo electronico', 'e-mail', 'mail'],
        'rut': ['rut trabajador', 'rut empleado', 'identificacion', 'run', 'r.u.t'],
        'telefono': ['celular', 'telefono', 'fono', 'tel'],
        'fecha de ingreso': ['fecha ingreso', 'ingreso', 'fecha contratacion', 'fecha de inicio de contrato', 'fecha inicio contrato', 'inicio de contrato', 'inicio contrato'],
        'fecha de nacimiento': ['fecha nacimiento', 'nacimiento', 'fecha de nac', 'cumpleaños'],
        'área': ['area', 'departamento', 'seccion'],
        'codigo_rbd_temp': ['rbd informado por vero', 'rbd', 'codigo rbd'],
        'nombre_rbd_temp': ['nombre rbd', 'establecimiento', 'colegio']
    }

    # Invertir diccionario para iteración eficiente de Pandas (llave: alias, valor: columna oficial)
    mapeo_traduccion = {}
    for columna_oficial, lista_posibles in alias_columnas.items():
        for alternativo in lista_posibles:
            mapeo_traduccion[alternativo] = columna_oficial

    lista_dataframes_procesados = []

    print(f"[INFO] Iniciando procesamiento de {len(archivos_excel)} archivo(s).")

    # --- FASE 1: EXTRACCIÓN Y NORMALIZACIÓN INICIAL ---
    for ruta_archivo in archivos_excel:
        nombre_archivo = os.path.basename(ruta_archivo)
        print(f"[INFO] Leyendo archivo: {nombre_archivo}")
        
        try:
            # sheet_name=None carga todas las hojas en un diccionario
            diccionario_hojas = pd.read_excel(ruta_archivo, sheet_name=None, dtype=str)
        except Exception as e:
            print(f"[ERROR] Fallo al procesar {nombre_archivo}. Detalle: {e}")
            continue

        for nombre_hoja, df_hoja in diccionario_hojas.items():
            if df_hoja.empty:
                continue
                
            print(f"       Procesando hoja '{nombre_hoja}' ({len(df_hoja)} registros).")

            # Normalización de encabezados: pasar a string, limpiar espacios, minúsculas y quitar tildes
            df_hoja.columns = df_hoja.columns.astype(str).str.strip().str.lower()
            df_hoja.columns = (df_hoja.columns
                               .str.replace('á', 'a')
                               .str.replace('é', 'e')
                               .str.replace('í', 'i')
                               .str.replace('ó', 'o')
                               .str.replace('ú', 'u'))
            
            # Aplicar mapeo y eliminar columnas duplicadas dentro de la misma hoja
            df_hoja.rename(columns=mapeo_traduccion, inplace=True)
            df_hoja = df_hoja.loc[:, ~df_hoja.columns.duplicated()]
            
            lista_dataframes_procesados.append(df_hoja)

    if not lista_dataframes_procesados:
        print("[ERROR] No se extrajeron datos válidos de los archivos proporcionados.")
        return

    # --- FASE 2: CONSOLIDACIÓN Y DEDUPLICACIÓN ---
    df_cliente = pd.concat(lista_dataframes_procesados, ignore_index=True)
    print(f"\n[INFO] Registros totales consolidados (sin filtrar): {len(df_cliente)}")

    if 'rut' in df_cliente.columns:
        # Estandarización de formato RUT para validación estricta
        df_cliente['rut'] = df_cliente['rut'].astype(str).str.strip().str.upper()
        df_cliente['rut'] = df_cliente['rut'].replace(['NAN', 'NONE', 'NULL'], np.nan)
        
        total_antes = len(df_cliente)
        
        # Eliminar duplicados globales conservando el primer registro encontrado
        df_cliente.drop_duplicates(subset=['rut'], keep='first', inplace=True)
        # Eliminar filas donde el RUT sea nulo (identificador primario requerido)
        df_cliente.dropna(subset=['rut'], inplace=True)
        
        registros_descartados = total_antes - len(df_cliente)
        print(f"[INFO] Deduplicación completada: {registros_descartados} registros ignorados (duplicados o sin RUT).")

    # --- FASE 3: APLICACIÓN DE REGLAS DE NEGOCIO ---
    
    # Regla 1: Separar nombres si vienen unificados
    if 'nombre_completo' in df_cliente.columns:
        print("[INFO] Ejecutando heurística de separación de nombres y apellidos.")
        df_cliente[['nombre', 'apellido']] = df_cliente['nombre_completo'].apply(separar_nombres_y_apellidos)

    # Regla 2: Formatear y unificar Centro de Trabajo (RBD)
    if 'codigo_rbd_temp' in df_cliente.columns and 'nombre_rbd_temp' in df_cliente.columns:
        print("[INFO] Procesando formato de Centro de Trabajo (Código + Nombre RBD).")
        df_cliente['codigo_rbd_temp'] = df_cliente['codigo_rbd_temp'].fillna('').astype(str).str.strip()
        df_cliente['nombre_rbd_temp'] = df_cliente['nombre_rbd_temp'].fillna('').astype(str).str.strip().str.title()
        
        # Limpieza residual de nulos convertidos a texto
        df_cliente['codigo_rbd_temp'] = df_cliente['codigo_rbd_temp'].replace('Nan', '')
        df_cliente['nombre_rbd_temp'] = df_cliente['nombre_rbd_temp'].replace('Nan', '')

        # Concatenación condicional para evitar "123 - " si falta el nombre
        df_cliente['centro de trabajo'] = df_cliente.apply(
            lambda row: f"{row['codigo_rbd_temp']} - {row['nombre_rbd_temp']}" 
            if row['codigo_rbd_temp'] and row['nombre_rbd_temp'] 
            else row['codigo_rbd_temp'] + row['nombre_rbd_temp'], 
            axis=1
        )

    # --- FASE 4: ENSAMBLAJE FINAL Y EXPORTACIÓN ---
    df_final = pd.DataFrame(columns=columnas_template)

    # Traspasar únicamente las columnas que coinciden con el template oficial
    columnas_coincidentes = [col for col in df_cliente.columns if col in columnas_template]
    for col in columnas_coincidentes:
        df_final[col] = df_cliente[col]

    # Limpieza visual del DataFrame final
    df_final.dropna(how='all', inplace=True)
    df_final.fillna('', inplace=True)

    print(f"\n[INFO] Guardando {len(df_final)} usuarios en template oficial: {archivo_salida}")
    df_final.to_excel(archivo_salida, index=False)
    print("[INFO] Proceso finalizado con éxito.")

if __name__ == "__main__":
    formatear_nomina_cliente()