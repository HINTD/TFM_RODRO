#!/usr/bin/env python3
"""VRP con ventanas de tiempo y capacidad."""

from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

def create_data_model():
    data = {}
    # Distancias entre nodos
    data["distance_matrix"] = [
        [0, 548, 776, 696, 582, 274, 502, 194, 308, 194, 536, 502, 388, 354, 468, 776, 662],
        [548, 0, 684, 308, 194, 502, 730, 354, 696, 742, 1084, 594, 480, 674, 1016, 868, 1210],
        [776, 684, 0, 992, 878, 502, 274, 810, 468, 742, 400, 1278, 1164, 1130, 788, 1552, 754],
        [696, 308, 992, 0, 114, 650, 878, 502, 844, 890, 1232, 514, 628, 822, 1164, 560, 1358],
        [582, 194, 878, 114, 0, 536, 764, 388, 730, 776, 1118, 400, 514, 708, 1050, 674, 1244],
        [274, 502, 502, 650, 536, 0, 228, 308, 194, 240, 582, 776, 662, 628, 514, 1050, 708],
        [502, 730, 274, 878, 764, 228, 0, 536, 194, 468, 354, 1004, 890, 856, 514, 1278, 480],
        [194, 354, 810, 502, 388, 308, 536, 0, 342, 388, 730, 468, 354, 320, 662, 742, 856],
        [308, 696, 468, 844, 730, 194, 194, 342, 0, 274, 388, 810, 696, 662, 320, 1084, 514],
        [194, 742, 742, 890, 776, 240, 468, 388, 274, 0, 342, 536, 422, 388, 274, 810, 468],
        [536, 1084, 400, 1232, 1118, 582, 354, 730, 388, 342, 0, 878, 764, 730, 388, 1152, 354],
        [502, 594, 1278, 514, 400, 776, 1004, 468, 810, 536, 878, 0, 114, 308, 650, 274, 844],
        [388, 480, 1164, 628, 514, 662, 890, 354, 696, 422, 764, 114, 0, 194, 536, 388, 730],
        [354, 674, 1130, 822, 708, 628, 856, 320, 662, 388, 730, 308, 194, 0, 342, 422, 536],
        [468, 1016, 788, 1164, 1050, 514, 514, 662, 320, 274, 388, 650, 536, 342, 0, 764, 194],
        [776, 868, 1552, 560, 674, 1050, 1278, 742, 1084, 810, 1152, 274, 388, 422, 764, 0, 798],
        [662, 1210, 754, 1358, 1244, 708, 480, 856, 514, 468, 354, 844, 730, 536, 194, 798, 0],
    ]

    # Demandas de entregas
    data["demands"] = [0, -1, -1, -2, -4, -2, -4, -8, -8, -1, -2, -1, -2, -4, -4, -8, -8]
    data["vehicle_capacities"] = [15, 15, 15, 15]

    # Matriz de tiempos (puedes usar la misma que la distancia para simplificar)
    data["time_matrix"] = [
        [0, 6, 9, 8, 7, 3, 6, 2, 3, 2, 6, 6, 4, 4, 5, 9, 7],
        [6, 0, 8, 3, 2, 6, 8, 4, 8, 8, 13, 7, 5, 8, 12, 10, 14],
        [9, 8, 0, 11, 10, 6, 3, 9, 5, 8, 4, 15, 14, 13, 9, 18, 9],
        [8, 3, 11, 0, 1, 7, 10, 6, 10, 10, 14, 6, 7, 9, 14, 6, 16],
        [7, 2, 10, 1, 0, 6, 9, 4, 8, 9, 13, 4, 6, 8, 12, 8, 14],
        [3, 6, 6, 7, 6, 0, 2, 3, 2, 2, 7, 9, 7, 7, 6, 12, 8],
        [6, 8, 3, 10, 9, 2, 0, 6, 2, 5, 4, 12, 10, 10, 6, 15, 5],
        [2, 4, 9, 6, 4, 3, 6, 0, 4, 4, 8, 5, 4, 3, 7, 8, 10],
        [3, 8, 5, 10, 8, 2, 2, 4, 0, 3, 4, 9, 8, 7, 3, 13, 6],
        [2, 8, 8, 10, 9, 2, 5, 4, 3, 0, 4, 6, 5, 4, 3, 9, 5],
        [6, 13, 4, 14, 13, 7, 4, 8, 4, 4, 0, 10, 9, 8, 4, 13, 4],
        [6, 7, 15, 6, 4, 9, 12, 5, 9, 6, 10, 0, 1, 3, 7, 3, 10],
        [4, 5, 14, 7, 6, 7, 10, 4, 8, 5, 9, 1, 0, 2, 6, 4, 8],
        [4, 8, 13, 9, 8, 7, 10, 3, 7, 4, 8, 3, 2, 0, 4, 5, 6],
        [5, 12, 9, 14, 12, 6, 6, 7, 3, 3, 4, 7, 6, 4, 0, 9, 2],
        [9, 10, 18, 6, 8, 12, 15, 8, 13, 9, 13, 3, 4, 5, 9, 0, 9],
        [7, 14, 9, 16, 14, 8, 5, 10, 6, 5, 4, 10, 8, 6, 2, 9, 0],
    ]
    

    # Ventanas de tiempo para cada nodo (depósito + tiendas)
    data["time_windows"] =  [
        (0, 50),  # depot
        (0, 50),  # 1
        (0, 50),  # 2
        (0, 50),  # 3
        (0, 50),  # 4
        (0, 50),  # 5
        (0, 50),  # 6
        (0, 50),  # 7
        (0, 50),  # 8
        (0, 50),  # 9
        (0, 50),  # 10
        (0, 50),  # 11
        (0, 50),  # 12
        (0, 50),  # 13
        (0, 50),  # 14
        (0, 50),  # 15
        (0, 50),  # 16
    ]

    data["num_vehicles"] = 4
    data["depot"] = 0
    return data

def print_solution(data, manager, routing, solution):
    total_distance = 0
    time_dimension = routing.GetDimensionOrDie("Time")
    for vehicle_id in range(data["num_vehicles"]):
        index = routing.Start(vehicle_id)
        plan_output = f"Ruta vehículo {vehicle_id}:\n"
        while not routing.IsEnd(index):
            node_index = manager.IndexToNode(index)
            time_var = time_dimension.CumulVar(index)
            plan_output += f"{node_index}(Tiempo={solution.Min(time_var)}) -> "
            index = solution.Value(routing.NextVar(index))
        plan_output += f"{manager.IndexToNode(index)}\n"
        print(plan_output)

def main():
    data = create_data_model()
    manager = pywrapcp.RoutingIndexManager(len(data["distance_matrix"]), data["num_vehicles"], data["depot"])
    routing = pywrapcp.RoutingModel(manager)

    # Costo por distancia
    def distance_callback(from_index, to_index):
        return data["distance_matrix"][manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]
    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # Capacidad
    def demand_callback(from_index):
        return -data["demands"][manager.IndexToNode(from_index)]
    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_callback_index,
        0,
        data["vehicle_capacities"],
        True,
        "Capacity"
    )

    # Ventanas de tiempo
    def time_callback(from_index, to_index):
        return data["time_matrix"][manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]
    time_callback_index = routing.RegisterTransitCallback(time_callback)

    routing.AddDimension(
        time_callback_index,
        5,  # tiempo de espera permitido
        50, # tiempo máximo del vehículo
        False,
        "Time"
    )
    time_dimension = routing.GetDimensionOrDie("Time")

    # Aplicar ventanas de tiempo a nodos
    for node_index, (start, end) in enumerate(data["time_windows"]):
        index = manager.NodeToIndex(node_index)
        time_dimension.CumulVar(index).SetRange(start, end)

    # Parámetros de búsqueda
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    search_parameters.time_limit.seconds = 5

    solution = routing.SolveWithParameters(search_parameters)
    if solution:
        print_solution(data, manager, routing, solution)
    else:
        print("⚠ No se encontró solución.")

if __name__ == "__main__":
    main()