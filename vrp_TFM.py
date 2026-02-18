"""VRP con múltiples vehículos, capacidad, ventanas de tiempo y pickup y delivery."""

from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import pandas as pd
from access_db import ConfiguracionConexion, AccessDB
import folium
import random
import numpy as np
import requests
import time

# --- FUNCIONES DE OBTENCIÓN DE DATOS ---

def get_data_from_sql():
    """Lee matrices desde Oracle y las prepara para OR-Tools."""
    
    conn_config = ConfiguracionConexion(config_id="DWRAC", ruta='config_acceso.yaml')
    db = AccessDB(conn_config)
    
    # Traemos distancias
    TABLA_RUTAS = "DWVEG_ORT.RMG_DIM_DISTANCIA"
    query = f"""
        SELECT LOC_ORIGEN, LOC_DESTINO, DISTANCIA_KM, TIEMPO_MIN, COMPATIBILIDAD_SN
        FROM {TABLA_RUTAS}
    """
    df_dist = db.get_dataframe(query)

    if df_dist.empty:
        raise Exception(f"La tabla {TABLA_RUTAS} está vacía.")

    # Traemos coordenadas 
    TABLA_COORDS = "DWVEG_ORT.RMG_DIM_LOCALIZACION"  
    query_coords = f"""
        SELECT LOC_ID, LATITUD, LONGITUD
        FROM {TABLA_COORDS}
    """
    df_coords = db.get_dataframe(query_coords)
    # Diccionario auxiliar de coordenadas por ID físico
    coords_dict = {row['LOC_ID']: (row['LATITUD'], row['LONGITUD']) for _, row in df_coords.iterrows()}

    # --- NUEVA SECCIÓN: Ventanas Temporales ---
    TABLA_VENTANAS = "DWVEG_ORT.RMG_FACT_SLA_REDUX" 
    query_v = f"""
        SELECT CLIENTE_ID, MINIMO, MAXIMO 
        FROM {TABLA_VENTANAS} 
        WHERE PROCESO_ID = 'PMG'
    """
    df_v = db.get_dataframe(query_v)
    
    def convertir_hora_a_minutos(hora_str):
        """Convierte formato HH:MM:SS a total de minutos desde las 00:00."""
        try:
            if pd.isna(hora_str) or str(hora_str).strip() == "":
                return 0
            partes = str(hora_str).split(':')
            return int(partes[0]) * 60 + int(partes[1])
        except (ValueError, IndexError):
            return 0

    # visits_list almacenará cada tarea de visita (nodos virtuales si hay ventanas separadas)
    visits_list = []
    
    # Definimos el depósito como la primera visita (Indice 0)
    NODO_BASE = "A00010"
    visits_list.append({'loc_id': NODO_BASE, 'start': 0, 'end': 1440, 'type': 'depot'})

    # Agrupamos por cliente para gestionar ventanas múltiples
    for cliente_id, group in df_v.groupby('CLIENTE_ID'):
        id_limpio = str(int(cliente_id)).zfill(5)
        node_id_real = f"C{id_limpio}"
        
        # Extraemos todas las ventanas de este cliente
        client_windows = []
        for _, row in group.iterrows():
            client_windows.append([convertir_hora_a_minutos(row['MINIMO']), 
                                  convertir_hora_a_minutos(row['MAXIMO'])])
        
        client_windows.sort() # Ordenamos por hora de inicio

        if not client_windows: continue

        # Lógica de fusión/separación de ventanas
        merged = []
        curr_start, curr_end = client_windows[0]
        
        for next_start, next_end in client_windows[1:]:
            if next_start <= curr_end: # Si se solapan, hacemos intersección (lo más restrictivo)
                curr_start = max(curr_start, next_start)
                curr_end = min(curr_end, next_end)
            else: # Si no se solapan, guardamos la anterior y empezamos una nueva visita
                merged.append((curr_start, curr_end))
                curr_start, curr_end = next_start, next_end
        merged.append((curr_start, curr_end))

        # Creamos una visita (nodo) por cada ventana resultante
        for win in merged:
            if win[0] <= win[1]: # Solo si la ventana es válida
                visits_list.append({'loc_id': node_id_real, 'start': win[0], 'end': win[1], 'type': 'client'})

    # Añadimos los almacenes de recogida (Axxx)
    pickup_locs = df_dist[df_dist['LOC_DESTINO'].str.startswith('A')]['LOC_DESTINO'].unique()
    for loc in pickup_locs:
        if loc != NODO_BASE:
            visits_list.append({'loc_id': loc, 'start': 0, 'end': 1440, 'type': 'pickup'})

    # --- CONSTRUCCIÓN DE MATRICES BASADAS EN VISITAS (Nodos Virtuales) ---
    num_visits = len(visits_list)
    dist_matrix = np.zeros((num_visits, num_visits))
    time_matrix = np.zeros((num_visits, num_visits))
    
    # Diccionario para búsqueda rápida de distancias físicas
    dist_lookup = df_dist.set_index(['LOC_ORIGEN', 'LOC_DESTINO'])[['DISTANCIA_KM', 'TIEMPO_MIN', 'COMPATIBILIDAD_SN']].to_dict('index')
    PENALIZACION = 5000000

    for i in range(num_visits):
        for j in range(num_visits):
            if i == j:
                continue
            
            loc_i = visits_list[i]['loc_id']
            loc_j = visits_list[j]['loc_id']
            
            # Si es la misma ubicación física, distancia 0
            if loc_i == loc_j:
                dist_matrix[i][j] = 0
                time_matrix[i][j] = 0
                continue

            res = dist_lookup.get((loc_i, loc_j))
            if res:
                # Aplicamos la restricción de compatibilidad
                if res['COMPATIBILIDAD_SN'] == 'N':
                    dist_matrix[i][j] = PENALIZACION
                    time_matrix[i][j] = PENALIZACION
                else:
                    dist_matrix[i][j] = res['DISTANCIA_KM']
                    time_matrix[i][j] = res['TIEMPO_MIN']
            else:
                dist_matrix[i][j] = PENALIZACION
                time_matrix[i][j] = PENALIZACION

    # Mapeos requeridos por el resto del código
    idx_to_node = {i: v['loc_id'] for i, v in enumerate(visits_list)}
    node_coords = {i: coords_dict.get(v['loc_id'], (42.9, -8.4)) for i, v in enumerate(visits_list)}
    windows_final = [(v['start'], v['end']) for v in visits_list]
    
    return dist_matrix.round().astype(int).tolist(), time_matrix.round().astype(int).tolist(), {}, node_coords, idx_to_node, windows_final, visits_list


def create_data_model():
    """Define los datos del problem."""
    data = {}
    
    dist_matrix, time_matrix, _, node_coords, idx_to_node, windows_final, visits_list = get_data_from_sql()
    
    num_nodes = len(dist_matrix)
    data['idx_to_node'] = idx_to_node # Guardamos la traducción en data
    data['node_coords'] = node_coords
    data["distance_matrix"] = dist_matrix
    data["time_matrix"] = time_matrix
    data["depot"] = 0 # El depósito es siempre el primer elemento de visits_list

    # Cantidad de carga a depositar en cada entrega.
    demands = [0] * num_nodes
    delivery_nodes = []
    pickup_nodes = []
    
    for i, v in enumerate(visits_list):
        if v['type'] == 'client':
            demands[i] = -1          # ENTREGA: El camión suelta carga
            delivery_nodes.append(i)
        elif v['type'] == 'pickup':
            demands[i] = 1           # RECOGIDA: El camión suma carga
            pickup_nodes.append(i)
   
    data["demands"] = demands
    data["delivery_nodes"] = delivery_nodes
    data["pickup_nodes"] = pickup_nodes

    # Capacidades de los vehículos
    data["num_vehicles"] = 15
    data["vehicle_capacities"] = [100] * data["num_vehicles"]
    
    # Ventanas de tiempo finales
    data["time_windows"] = windows_final
    
    return data

# --- FUNCIONES DE SALIDA Y VISUALIZACIÓN ---

def print_solution(data, manager, routing, solution):
    """Imprime rutas, carga y tiempo de llegada a cada nodo usando IDs de Oracle."""
    total_distance = 0
    time_dimension = routing.GetDimensionOrDie("Time")
    capacity_dimension = routing.GetDimensionOrDie("Capacity")
    
    for vehicle_id in range(data["num_vehicles"]):
        index = routing.Start(vehicle_id)
        plan_output = f"Ruta vehículo {vehicle_id}:\n"
        route_distance = 0
        nodes_visited = 0
        
        while not routing.IsEnd(index):
            nodes_visited += 1
            node_index = manager.IndexToNode(index)
            node_id = data['idx_to_node'][node_index]
            
            load_var = solution.Value(capacity_dimension.CumulVar(index))
            time_var = solution.Value(time_dimension.CumulVar(index))
            
            # Formateamos el tiempo de minutos a HH:MM para leerlo mejor
            horas = time_var // 60
            mins = time_var % 60
            plan_output += f"{node_id}(Carga={load_var}, Hora={horas:02d}:{mins:02d}) -> "
            
            previous_index = index
            index = solution.Value(routing.NextVar(index))
            route_distance += routing.GetArcCostForVehicle(previous_index, index, vehicle_id)
            
        # Para el último nodo (regreso al depósito)
        node_index_final = manager.IndexToNode(index)
        node_id_final = data['idx_to_node'][node_index_final]
        time_var = solution.Value(time_dimension.CumulVar(index))
        horas = time_var // 60
        mins = time_var % 60
        plan_output += f"{node_id_final}(Hora={horas:02d}:{mins:02d})\n"
        plan_output += f"Distancia de la ruta: {route_distance}km\n"
        
        if nodes_visited > 1:
            print(plan_output)
            total_distance += route_distance
            
    print(f"Distancia total de todas las rutas: {total_distance}km")

def generate_map(data, manager, routing, solution):
    """Genera un mapa interactivo con iconos diferenciados para entregas y recogidas."""
    depot_coords = data['node_coords'][data['depot']]
    m = folium.Map(location=depot_coords, zoom_start=12)

    colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 
              'cadetblue', 'darkpurple', 'pink', 'lightblue', 'black']

    for vehicle_id in range(data["num_vehicles"]):
        index = routing.Start(vehicle_id)
        route_coords = []
        color = colors[vehicle_id % len(colors)]
        
        while not routing.IsEnd(index):
            node_index = manager.IndexToNode(index)
            node_id = data['idx_to_node'][node_index] 
            coords = data['node_coords'][node_index]
            route_coords.append(coords)
            
            if node_index == data['depot']:
                icon_type, icon_color, label = 'home', 'black', "DEPÓSITO"
            elif node_index in data['delivery_nodes']:
                icon_type, icon_color, label = 'arrow-down', color, f"ENTREGA ({node_id})"
            elif node_index in data['pickup_nodes']:
                icon_type, icon_color, label = 'arrow-up', color, f"RECOGIDA ({node_id})"
            else:
                icon_type, icon_color, label = 'ban', 'gray', f"Nodo {node_id}"

            folium.Marker(
                location=coords, 
                popup=f"{label}<br>Vehículo: {vehicle_id}", 
                icon=folium.Icon(color=icon_color, icon=icon_type, prefix='fa')
            ).add_to(m)
            
            index = solution.Value(routing.NextVar(index))
        
        node_index = manager.IndexToNode(index)
        route_coords.append(data['node_coords'][node_index])
        
        if len(route_coords) > 2:
            folium.PolyLine(
                route_coords, 
                color=color, 
                weight=3, 
                opacity=0.7,
                tooltip=f"Ruta Vehículo {vehicle_id}"
            ).add_to(m)

    m.save("mapa_rutas.html")
    print("Mapa generado correctamente en 'mapa_rutas.html'")

# --- BLOQUE PRINCIPAL DE EJECUCIÓN ---

def main():
    data = create_data_model()
    
    manager = pywrapcp.RoutingIndexManager(len(data["distance_matrix"]), data["num_vehicles"],
                                           data["depot"])
    
    routing = pywrapcp.RoutingModel(manager)
    
    # Costo por distancia
    def distance_callback(from_index, to_index):
        return data["distance_matrix"][manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]
    
    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)
    
    # Restricción de capacidad
    def demand_callback(from_index):
        return data["demands"][manager.IndexToNode(from_index)]
    
    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_callback_index,
        0,
        data["vehicle_capacities"],  
        False,
        "Capacity"
    )
    
    capacity_dimension = routing.GetDimensionOrDie("Capacity")
    for vehicle_id in range(data["num_vehicles"]):
        start_index = routing.Start(vehicle_id)
        capacity_dimension.CumulVar(start_index).SetValue(data["vehicle_capacities"][vehicle_id])

    # Restricción de tiempo
    def time_callback(from_index, to_index):
        return data["time_matrix"][manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]
    
    time_callback_index = routing.RegisterTransitCallback(time_callback)
    
    routing.AddDimension(
        time_callback_index,
        1440,        # slack (tiempo de espera permitido en la puerta)
        1440,        # tiempo máximo total (24 horas)
        False,
        "Time"
    )
    time_dimension = routing.GetDimensionOrDie("Time")
    
    # Aplicar ventanas de tiempo
    for node_index, (start, end) in enumerate(data["time_windows"]):
        index = manager.NodeToIndex(node_index)
        if start <= end:
            time_dimension.CumulVar(index).SetRange(start, end)

    # Forzar Entregas antes que Recogidas
    def pickup_count_callback(from_index):
        node = manager.IndexToNode(from_index)
        return 1 if node in data["pickup_nodes"] else 0

    pickup_count_index = routing.RegisterUnaryTransitCallback(pickup_count_callback)
    routing.AddDimension(pickup_count_index, 0, len(data["pickup_nodes"]) + 1, True, "PickupSequence")
    sequence_dimension = routing.GetDimensionOrDie("PickupSequence")

    for d in data["delivery_nodes"]:
        d_index = manager.NodeToIndex(d)
        sequence_dimension.CumulVar(d_index).SetMax(0)

    # Penalizaciones para nodos imposibles
    penalty = 100000
    for node in range(0, len(data["distance_matrix"])):
        if node != data["depot"]:
            routing.AddDisjunction([manager.NodeToIndex(node)], penalty)

    # Parámetros de búsqueda
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_MOST_CONSTRAINED_ARC)
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
    search_parameters.time_limit.seconds = 30
    
    solution = routing.SolveWithParameters(search_parameters)
    
    if solution:
        print("SOLUCIÓN ENCONTRADA")
        print_solution(data, manager, routing, solution)
        generate_map(data, manager, routing, solution) 
    else:
        print(" No se encontró solución.")
        
if __name__ == "__main__":
    main()