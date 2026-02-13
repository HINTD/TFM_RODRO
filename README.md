# Código para la optimización de rutas (VRP con Pickup y Delivery)

## Descripcción del proyecto 
Este script forma parte del TFM de Rodrigo Martin Garcia e intenta resolver el Problema de Rutas de Vehículos con Recogidas y Entregas (VRPDP) utilizando **python** y **Google OR-Tools**. El obejtivo principal es optimizar las rutas de camiones que salen desde el depósito de Sigüeiro. Estos camiones salen de nuestro depósito, entregan mercancía en diferentes tiendas y finalmente recogen materiales de vuelta. Para ello tendrán que respetar una serie de restricciones (ventanas de tiempo, capacidad de los camiones, tipo de carga...).

## Descripccion de los bloques de código

### 1. 'get_data_from_sql()'
Esta función nos permite obtener los datos de nuestros servidores de Oracle. Además de ello convierte las tablas de distancias y tiempos en matrices de datos para poder ser utilizadas por el programa.

### 2. `create_data_model()`
Prepara y estructura los datos para poder ser usados con OR-Tools. Aquí se define:
* Las matrices de distancia y tiempo.
* Las capacidades de los vehiculos.
* Lógica de Carga: Entregas (demanda negativa) y Recogidas (demanda positiva).

### 3. 'main()'
Configura las restricciones de capacidad, tiempo, la estrategia de búsqueda Gided Local Search (Encuentra la ruta más eficiente en cuestion de distancia y tiempo)

## 4. 'generate_map()'
Crea un mapa para poder visualizar donde esta cada punto (diferencia entre recogida y descarga)

### 3. `print_solution()`
Se encarga de interpretar los resultados del algoritmo
