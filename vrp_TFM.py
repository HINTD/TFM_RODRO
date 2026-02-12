"""VRP con múltiples vehículos, capacidad, ventanas de tiempo y pickup y delivery."""


from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import pandas as pd
from access_db import ConfiguracionConexion, AccessDB


#Obtenemos los datos
def get_data_from_sql():
    """Lee matrices desde Oracle y las prepara para OR-Tools."""
    
    conn_config = ConfiguracionConexion(config_id="DWRAC", ruta='config_acceso.yaml')
    db = AccessDB(conn_config)
    
    TABLA_RUTAS = "DWVEG_ORT.RMG_DIM_DISTANCIA"
    query = f"""
        SELECT LOC_ORIGEN, LOC_DESTINO, DISTANCIA_KM, TIEMPO_MIN, COMPATIBILIDAD_SN
        FROM {TABLA_RUTAS}
    """
    df = db.get_dataframe(query)
    


    if df.empty:
        raise Exception(f"La tabla {TABLA_RUTAS} está vacía.")

    all_nodes = pd.concat([df['LOC_ORIGEN'], df['LOC_DESTINO']]).unique()
    node_to_idx = {node: i for i, node in enumerate(all_nodes)}
    
    df['idx_origen'] = df['LOC_ORIGEN'].map(node_to_idx)
    df['idx_destino'] = df['LOC_DESTINO'].map(node_to_idx)
    
    dist_matrix = df.pivot(index='idx_origen', columns='idx_destino', values='DISTANCIA_KM').fillna(0).round().astype(int).values.tolist()
    time_matrix = df.pivot(index='idx_origen', columns='idx_destino', values='TIEMPO_MIN').fillna(0).round().astype(int).values.tolist()
    
    print(f"✅ Matrices cargadas: {len(dist_matrix)} nodos.")
    
    return dist_matrix, time_matrix, node_to_idx


def create_data_model():
    """Define los datos del problema."""
    data = {}
    
    dist_matrix, time_matrix, node_to_idx = get_data_from_sql()
    
    #NNumero de nodos
    num_nodes = len(dist_matrix)
    data['node_to_idx']=node_to_idx


    # Matriz de distancias entre nodos
    data["distance_matrix"] = dist_matrix


    # Matriz de tiempos
    data["time_matrix"] = time_matrix


    # Cantidad de carga a depositar en cada entrega. Creamos una lista del tamaño correcto. Luego ajustamos el tamaño
    mitad = num_nodes // 2
    demands = [0] * num_nodes
    delivery_nodes = []
    pickup_nodes = []

    for i in range(1, num_nodes):
        if i <= mitad:
            demands[i] = -1          # ENTREGA: El camión suelta carga
            delivery_nodes.append(i)
        else:
            demands[i] = 1           # RECOGIDA: El camión suma carga
            pickup_nodes.append(i)
    
    data["demands"] = demands
    # Definimos que nodos son las entregas y cuales son las recogidas (ejemplo)
    data["delivery_nodes"] = delivery_nodes
    data["pickup_nodes"] = pickup_nodes

    
    # Capacidades de los vehículos
    data["num_vehicles"] = 15
    data["vehicle_capacities"] = [100]*data["num_vehicles"]
    
    # Ventanas de tiempo (depósito + tiendas)
    data["time_windows"] = [(0, 14400)] * num_nodes
    data["depot"] = 0
    
    return data

def print_solution(data, manager, routing, solution):
    """Imprime rutas, carga y tiempo de llegada a cada nodo."""
    total_distance = 0
    time_dimension = routing.GetDimensionOrDie("Time")
    capacity_dimension = routing.GetDimensionOrDie("Capacity")
    
    for vehicle_id in range(data["num_vehicles"]):
        index = routing.Start(vehicle_id)
        plan_output = f"Ruta vehículo {vehicle_id}:\n"
        route_distance = 0
        
        while not routing.IsEnd(index):
            node_index = manager.IndexToNode(index)
            # Leemos la carga real gestionada por el motor de OR-Tools
            load_var = solution.Value(capacity_dimension.CumulVar(index))
            time_var = solution.Value(time_dimension.CumulVar(index))
            
            plan_output += f"{node_index}(Carga={load_var}, Tiempo={time_var}) -> "
            
            previous_index = index
            index = solution.Value(routing.NextVar(index))
            route_distance += routing.GetArcCostForVehicle(previous_index, index, vehicle_id)
            
        load_var = solution.Value(capacity_dimension.CumulVar(index))
        time_var = solution.Value(time_dimension.CumulVar(index))
        plan_output += f"{manager.IndexToNode(index)}(Carga={load_var}, Tiempo={time_var})\n"
        plan_output += f"Distancia de la ruta: {route_distance}m\n"
        
        if route_distance > 0:
            print(plan_output)
            total_distance += route_distance
            
    print(f"Distancia total de todas las rutas: {total_distance}m")

def main():
    data = create_data_model()
    
    manager = pywrapcp.RoutingIndexManager(len(data["distance_matrix"]),
                                           data["num_vehicles"],
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
        0,  # null capacity slack
        data["vehicle_capacities"],  
        False,  # Debe ser False para permitir que el vehículo empiece con carga (SetValue)
        "Capacity"
    )
    
    capacity_dimension = routing.GetDimensionOrDie("Capacity")

    # Forzamos a cada vehículo a empezar LLENO
    for vehicle_id in range(data["num_vehicles"]):
        start_index = routing.Start(vehicle_id)
        capacity_dimension.CumulVar(start_index).SetValue(data["vehicle_capacities"][vehicle_id])

    # Restricción de tiempo
    def time_callback(from_index, to_index):
        return data["time_matrix"][manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]
    
    time_callback_index = routing.RegisterTransitCallback(time_callback)
    routing.AddDimension(
        time_callback_index,
        30,    # slack permitido (margen de espera)
        20000, # tiempo máximo (aumentado para cubrir rutas largas)
        False,
        "Time"
    )
    time_dimension = routing.GetDimensionOrDie("Time")
    
    # Aplicar ventanas de tiempo
    for node_index, (start, end) in enumerate(data["time_windows"]):
        index = manager.NodeToIndex(node_index)
        time_dimension.CumulVar(index).SetRange(start, end)

    # REGLA: Forzar Entregas antes que Recogidas (Evitar Pickup -> Delivery)
    # Usamos una dimensión de secuencia: cada recogida suma 1 punto.
    def pickup_count_callback(from_index):
        node = manager.IndexToNode(from_index)
        return 1 if node in data["pickup_nodes"] else 0

    pickup_count_index = routing.RegisterUnaryTransitCallback(pickup_count_callback)
    routing.AddDimension(pickup_count_index, 0, len(data["pickup_nodes"]) + 1, True, "PickupSequence")
    sequence_dimension = routing.GetDimensionOrDie("PickupSequence")

    # En cada entrega, el contador de recogidas debe ser 0 para asegurar que no se ha recogido nada antes
    for d in data["delivery_nodes"]:
        d_index = manager.NodeToIndex(d)
        sequence_dimension.CumulVar(d_index).SetMax(0)

    # Añadimos penalizaciones para permitir omitir nodos si son imposibles de alcanzar (evita el "No se encontró solución")
    penalty = 100000
    for node in range(1, len(data["distance_matrix"])):
        routing.AddDisjunction([manager.NodeToIndex(node)], penalty)

    # Estrategia inicial de búsqueda
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    # Cambiamos a una estrategia más robusta para 400 nodos
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION)
    # Añadimos metaheurística para mejorar la solución inicial
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
    search_parameters.time_limit.seconds = 30
    
    solution = routing.SolveWithParameters(search_parameters)
    
    if solution:
        print_solution(data, manager, routing, solution)
    else:
        print("⚠ No se encontró solución.")

if __name__ == "__main__":
    main()