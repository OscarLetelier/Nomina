---

# 📄 Procesador y Consolidador de Nóminas

Este script de Python automatiza la limpieza, unificación y estandarización de múltiples archivos Excel enviados por clientes. Transforma datos desordenados en una plantilla única y estructurada.

## ✨ Características Principales

    1. Lectura Multi-Archivo y Multi-Hoja:** Procesa automáticamente todos los Excel (y todas sus pestañas) depositados en una carpeta específica.
    2. Deduplicación Inteligente:** Identifica y elimina registros duplicados utilizando el RUT como identificador único, sin importar en qué archivo u hoja se encuentren.
    3. Separación de Nombres:** Detecta columnas unificadas (ej. "Nombre Completo") y las divide heurísticamente en "Nombre" y "Apellido".
    4. Mapeo de Alias:** Traduce automáticamente los nombres de columnas de los clientes (ej. "email", "correo electrónico", "mail") a la estructura oficial del sistema.
    5. Estandarización de Centro de Trabajo:** Fusiona las columnas de "Código RBD" y "Nombre RBD" en el formato requerido (`Código - Nombre`).

---

## 🛠️ Requisitos Previos

Para ejecutar esta herramienta, necesitas tener instalado **Python 3.x** en tu computadora. Además, el script depende de dos librerías externas para el manejo de datos y archivos Excel:

1. `pandas` (Motor de análisis y manipulación de datos)
2. `openpyxl` (Motor para leer y escribir archivos `.xlsx`)

Puedes instalar ambas librerías abriendo tu terminal o línea de comandos y ejecutando:

```bash
pip install pandas openpyxl

```

---

### Paso 1: Preparación del entorno

Coloca el script `generar_template_multiarhivo.py` en una carpeta de tu preferencia. Ejecútalo por primera vez abriendo tu terminal en esa ubicación y corriendo:

```bash
python3 generar_template_multiarhivo.py

```

_Nota: La primera vez que lo ejecutes, el script creará automáticamente una carpeta llamada `archivos_cliente` y se detendrá._

### Paso 2: Carga de datos

Copia o mueve todos los archivos Excel (`.xlsx`) dentro de la nueva carpeta `archivos_cliente`.

### Paso 3: Ejecución

Vuelve a ejecutar el script en tu terminal:

```bash
python3 generar_template_multiarhivo.py

```

### Paso 4: Resultado

El script leerá todo, limpiará los datos, aplicará las reglas de negocio y, al finalizar, generará un nuevo archivo llamado **`Template_Listo_Para_Subir.xlsx`** Este es tu archivo final, limpio y estandarizado.

---

## 📂 Estructura de Carpetas Esperada

Tu directorio de trabajo debería verse así antes de la ejecución final:

```text
📁 Tu_Carpeta_De_Proyecto/
│
├── generar_template_multiarhivo.py   # El script principal
├── README.md                         # Este archivo de instrucciones
│
└── 📁 archivos_cliente/              # Carpeta donde depositas los Excel
    ├── nomina_parte_1.xlsx
    ├── nomina_parte_2.xlsx
    └── rezagados.xlsx

```

---

## ⚠️ Notas Importantes

- **Formato de Archivos:** El script solo procesa archivos con extensión `.xlsx`. Si el cliente envía un `.csv` o un `.xls` antiguo, guárdalo como `.xlsx` desde Excel antes de procesarlo.
- **El RUT es obligatorio:** Cualquier fila que no contenga un RUT válido en el Excel del cliente será ignorada por el sistema, ya que es el identificador único requerido.

---
