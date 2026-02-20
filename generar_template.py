import os
import glob
import pandas as pd
import numpy as np

def separar_nombres_y_apellidos(texto):
    """
    Evalúa una cadena de texto y extrae de forma heurística los nombres y apellidos.
    
    Parámetros:
        texto (str): Cadena original que contiene el nombre completo del trabajador.
        
    Retorna:
        tuple: (nombres, apellidos) separados según la cantidad de palabras detectadas.
               Si el valor es nulo, retorna dos cadenas vacías.
    """
    if pd.isna(texto) or str(texto).strip() == "":
        return "", ""
        
    partes = str(texto).strip().split()
    total_palabras = len(partes)
    
    if total_palabras == 1:
        return partes[0], ""
    elif total_palabras == 2:
        return partes[0], partes[1]
    elif total_palabras == 3:
        return partes[0], f"{partes[1]} {partes[2]}"
    else:
        # Para 4 o más palabras, asume los dos primeros como nombres y el resto como apellidos
        nombres = " ".join(partes[:2])
        apellidos = " ".join(partes[2:])
        return nombres, apellidos

def formatear_nomina_cliente():
    """
    Controlador principal del proceso ETL (Extract, Transform, Load) para nóminas de clientes.
    
    Fases del proceso:
    1. Extracción: Lectura iterativa de múltiples archivos Excel (.xlsx) y sus respectivas hojas.
    2. Consolidación y Auditoría: Unión de datos, limpieza estructural y exclusión documentada de duplicados/nulos.
    3. Reglas de Negocio: Transformación de nombres, estandarización de cargos y unificación de centros de trabajo.
    4. Ensamblaje: Formateo final hacia la plantilla estricta de 16 columnas y aplicación de estilos condicionales.
    """
    
    # --- CONFIGURACIÓN DE RUTAS Y CONSTANTES ---
    carpeta_entrada = 'archivos_cliente'
    archivo_salida = 'Template_Listo_Para_Subir1.xlsx'
    archivo_excluidos = 'Reporte_Registros_Excluidos.xlsx'

    # Validación de directorio de origen
    if not os.path.exists(carpeta_entrada):
        os.makedirs(carpeta_entrada)
        print(f"[WARNING] Directorio '{carpeta_entrada}' no encontrado. Se ha creado automáticamente.")
        print("[INFO] Por favor, deposite los archivos Excel en el directorio y ejecute nuevamente.")
        return

    archivos_excel = glob.glob(f"{carpeta_entrada}/*.xlsx")
    if not archivos_excel:
        print(f"[ERROR] No se encontraron archivos con extensión .xlsx en '{carpeta_entrada}'.")
        return

    # Estructura estricta requerida por la base de datos de destino
    columnas_template = [
        'nombre', 'apellido', 'rut', 'correo', 'direccion', 'telefono', 
        'fecha de ingreso', 'fecha de nacimiento', 'cargo', 'tipo de usuario', 
        'centro de trabajo', 'subempresa', 'empresacontratista', 
        'rut supervisor', 'correo de bienvenida', 'área'
    ]

    # Diccionario de equivalencias (Mapping) para normalizar las variaciones de los clientes
    alias_columnas = {
        'nombre_completo': ['nombre completo', 'nombres y apellidos', 'nombre trabajador', 'trabajador', 'colaborador', 'empleado'],
        'nombre': ['nombres', 'primer nombre', 'nombre', 'nombre(s)'],
        'apellido': ['apellidos', 'apellidos trabajador', 'apellido', 'apellido(s)'],
        'correo': ['email', 'correo electronico', 'e-mail', 'mail'],
        'rut': ['rut trabajador', 'rut empleado', 'identificacion', 'run', 'r.u.t', 'id empleado', 'id_empleado'],
        'telefono': ['celular', 'telefono', 'fono', 'tel'],
        'fecha de ingreso': ['fecha ingreso', 'ingreso', 'fecha contratacion', 'fecha de inicio de contrato', 'fecha inicio contrato', 'inicio de contrato', 'inicio contrato'],
        'fecha de nacimiento': ['fecha nacimiento', 'nacimiento', 'fecha de nac', 'cumpleaños'],
        'área': ['area', 'departamento', 'seccion'],
        'centro de trabajo': ['centro de trabajo', 'sucursal / rbd', 'sucursal rbd', 'sucursal', 'lugar de trabajo'],
        'codigo_rbd_temp': ['rbd informado por vero', 'rbd', 'codigo rbd'],
        'nombre_rbd_temp': ['nombre rbd', 'establecimiento', 'colegio']
    }

    # Inversión de diccionario para optimización de búsqueda (Llave: Alias del cliente -> Valor: Nombre oficial)
    mapeo_traduccion = {}
    for col_oficial, lista_posibles in alias_columnas.items():
        for alternativo in lista_posibles:
            mapeo_traduccion[alternativo] = col_oficial

    lista_dataframes_procesados = []
    print(f"[INFO] Iniciando procesamiento de {len(archivos_excel)} archivo(s) detectado(s).")

    # --- FASE 1: EXTRACCIÓN Y ESTANDARIZACIÓN INICIAL ---
    for ruta_archivo in archivos_excel:
        nombre_archivo = os.path.basename(ruta_archivo)
        try:
            # Lectura en modo texto para prevenir distorsión de formatos numéricos (ej. pérdida de ceros en RUT/Teléfono)
            diccionario_hojas = pd.read_excel(ruta_archivo, sheet_name=None, dtype=str)
        except Exception as error:
            print(f"[ERROR] Imposible procesar el archivo '{nombre_archivo}'. Detalle técnico: {error}")
            continue

        for nombre_hoja, df_hoja in diccionario_hojas.items():
            if df_hoja.empty:
                continue
            
            # Trazabilidad: Registrar de qué archivo y hoja proviene cada bloque de datos
            df_hoja['origen_hoja'] = f"{nombre_archivo} -> {nombre_hoja}"
                
            # Normalización de encabezados (minúsculas, sin espacios iniciales/finales, sin tildes)
            df_hoja.columns = df_hoja.columns.astype(str).str.strip().str.lower()
            df_hoja.columns = (df_hoja.columns
                               .str.replace('á', 'a').str.replace('é', 'e')
                               .str.replace('í', 'i').str.replace('ó', 'o').str.replace('ú', 'u'))
            
            # Aplicación de mapeo de alias y purga de columnas duplicadas por error del cliente
            df_hoja.rename(columns=mapeo_traduccion, inplace=True)
            df_hoja = df_hoja.loc[:, ~df_hoja.columns.duplicated()]
            
            lista_dataframes_procesados.append(df_hoja)

    if not lista_dataframes_procesados:
        print("[WARNING] No se extrajeron datos útiles de los archivos proporcionados.")
        return

    # --- FASE 2: CONSOLIDACIÓN GLOBAL Y AUDITORÍA DE DATOS ---
    df_cliente = pd.concat(lista_dataframes_procesados, ignore_index=True)
    
    if 'rut' in df_cliente.columns:
        # Estandarización estricta de la clave primaria (RUT)
        df_cliente['rut'] = df_cliente['rut'].astype(str).str.strip().str.upper()
        df_cliente['rut'] = df_cliente['rut'].replace(['NAN', 'NONE', 'NULL', ''], np.nan)
        
        # Identificación de registros carentes de clave primaria
        mask_sin_rut = df_cliente['rut'].isna()
        df_sin_rut = df_cliente[mask_sin_rut].copy()
        if not df_sin_rut.empty:
            df_sin_rut['MOTIVO_RECHAZO'] = 'RECHAZO CRÍTICO: Clave primaria (RUT) ausente o inválida.'

        # Identificación de registros duplicados basados en la clave primaria
        df_con_rut = df_cliente[~mask_sin_rut]
        mask_duplicados = df_con_rut.duplicated(subset=['rut'], keep='first')
        df_duplicados = df_con_rut[mask_duplicados].copy()
        if not df_duplicados.empty:
            df_duplicados['MOTIVO_RECHAZO'] = 'RECHAZO POR DUPLICIDAD: RUT ya ingresado en un registro o archivo anterior.'

        # Consolidación del reporte de excepciones
        lista_excluidos = []
        if not df_sin_rut.empty: lista_excluidos.append(df_sin_rut)
        if not df_duplicados.empty: lista_excluidos.append(df_duplicados)

        if lista_excluidos:
            df_excluidos_final = pd.concat(lista_excluidos, ignore_index=True)
            # Reorganización de columnas para priorizar la visualización del motivo de rechazo
            columnas_ordenadas = ['MOTIVO_RECHAZO', 'origen_hoja'] + [c for c in df_excluidos_final.columns if c not in ['MOTIVO_RECHAZO', 'origen_hoja']]
            df_excluidos_final = df_excluidos_final[columnas_ordenadas]
            df_excluidos_final.to_excel(archivo_excluidos, index=False)
            print(f"[INFO] Auditoría de datos completada: Se excluyeron {len(df_excluidos_final)} registros inconsistentes.")
            print(f"[INFO] Reporte de exclusiones generado satisfactoriamente en: {archivo_excluidos}")

        # Aislamiento de la base de datos validada (sin duplicados y con RUT)
        df_cliente = df_con_rut[~mask_duplicados].copy()

    # --- FASE 3: APLICACIÓN DE REGLAS DE NEGOCIO ---
    
    # 3.1 Normalización de Nombres y Apellidos
    if 'nombre_completo' in df_cliente.columns:
        # Caso A: El cliente provee una columna explícita de nombre completo.
        separados = df_cliente['nombre_completo'].apply(separar_nombres_y_apellidos)
        df_cliente['nombre'] = separados.apply(lambda x: x[0])
        df_cliente['apellido'] = separados.apply(lambda x: x[1])
        
    elif 'nombre' in df_cliente.columns:
        # Caso B: Lógica deductiva. Verificación de existencia de columna de apellidos.
        if 'apellido' not in df_cliente.columns or df_cliente['apellido'].replace('', np.nan).dropna().empty:
            separados = df_cliente['nombre'].apply(separar_nombres_y_apellidos)
            df_cliente['nombre'] = separados.apply(lambda x: x[0])
            df_cliente['apellido'] = separados.apply(lambda x: x[1])

    # 3.2 Estandarización a Mayúsculas de la columna Cargo
    if 'cargo' in df_cliente.columns:
        df_cliente['cargo'] = df_cliente['cargo'].fillna('').astype(str).str.strip().str.upper()
        # Limpieza residual de nulos convertidos a cadenas de texto
        df_cliente['cargo'] = df_cliente['cargo'].replace(['NAN', 'NONE', 'NULL'], '')

    # 3.3 Consolidación de Centro de Trabajo (Código + Nombre)
    if 'codigo_rbd_temp' in df_cliente.columns and 'nombre_rbd_temp' in df_cliente.columns:
        codigo = df_cliente['codigo_rbd_temp'].fillna('').astype(str).str.strip().replace('Nan', '')
        nombre = df_cliente['nombre_rbd_temp'].fillna('').astype(str).str.strip().str.title().replace('Nan', '')

        # Concatenación condicional para evitar separadores huérfanos
        serie_concatenada = pd.Series(
            np.where((codigo != '') & (nombre != ''), codigo + ' - ' + nombre, codigo + nombre),
            index=df_cliente.index
        )

        # Inyección de datos respetando una potencial columna ya unificada enviada por el cliente
        if 'centro de trabajo' not in df_cliente.columns:
            df_cliente['centro de trabajo'] = serie_concatenada
        else:
            df_cliente['centro de trabajo'] = df_cliente['centro de trabajo'].replace('', np.nan).fillna(serie_concatenada)

    # --- FASE 4: ENSAMBLAJE FINAL Y APLICACIÓN DE ESTILOS ---
    
    # Restauración de índices para asegurar la alineación de la matriz
    df_cliente.reset_index(drop=True, inplace=True)
    
    # Restructuración estricta hacia el Template Oficial
    df_final = df_cliente.reindex(columns=columnas_template)
    
    # Conversión a object genérico para prevenir colisiones de tipado al inyectar cadenas vacías
    df_final = df_final.astype(object)
    df_final.fillna('', inplace=True)

    def aplicar_resaltado_correo_ausente(fila):
        """
        Función de estilo (CSS condicional) para el motor de exportación de Pandas.
        Aplica fondo amarillo a las filas donde el campo de correo electrónico se encuentre vacío.
        """
        correo_valor = str(fila.get('correo', '')).strip().lower()
        if correo_valor in ['', 'nan', 'none', 'null']:
            return ['background-color: #FFFF00'] * len(fila)
        return [''] * len(fila)

    print(f"[INFO] Procediendo a la exportación final de {len(df_final)} registros validados.")
    
    try:
        df_final.style.apply(aplicar_resaltado_correo_ausente, axis=1).to_excel(archivo_salida, index=False, engine='openpyxl')
        print(f"[SUCCESS] Exportación concluida correctamente. Archivo generado: '{archivo_salida}'.")
    except Exception as error:
        print(f"[ERROR] Ocurrió un fallo durante la escritura del archivo final. Detalle técnico: {error}")

if __name__ == "__main__":
    formatear_nomina_cliente()