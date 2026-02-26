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

# FUNCIONES DE OBTENCIÓN DE DATOS

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
        SELECT LOC_ID,TIENDA, LATITUD, LONGITUD
        FROM {TABLA_COORDS}
    """
    df_coords = db.get_dataframe(query_coords)
    
    # Diccionario auxiliar de coordenadas por ID físico
    coords_dict = {row['LOC_ID']: (row['LATITUD'], row['LONGITUD']) for _, row in df_coords.iterrows()}
   
    # Sumamos los MCE por tienda independientemente del proceso
    TABLA_NECESIDADES = "DWVEG_ORT.TEMP_NECESIDADES"
    query_mce = f"""
        SELECT CLIENTE_ID, SUM(MCE) as TOTAL_MCE
        FROM {TABLA_NECESIDADES}
        WHERE DIA_ID = TO_DATE('15/09/2023', 'DD/MM/YYYY')
        GROUP BY CLIENTE_ID
    """
    df_mce = db.get_dataframe(query_mce)

    # Diccionario para cruzar carga: ID_LIMPIO -> MCE
    mce_lookup = {str(int(row['CLIENTE_ID'])).zfill(5): row['TOTAL_MCE'] for _, row in df_mce.iterrows()}

    # --- AUDITORÍA DE CARGA (Añade esto aquí) ---
    mce_total_sql = df_mce['TOTAL_MCE'].sum()
    mce_total_lookup = sum(mce_lookup.values())
    
    print(f"\n[AUDITORÍA SQL]")
    print(f"Total MCE en DataFrame (SQL): {mce_total_sql}")
    print(f"Total MCE en Diccionario (Lookup): {mce_total_lookup}")
    print(f"Número de clientes únicos en SQL: {len(df_mce)}")
    print(f"Número de clientes en Diccionario: {len(mce_lookup)}")
    
    if mce_total_sql != mce_total_lookup:
        print("⚠️ ALERTA: Estás perdiendo MCE al crear el diccionario. Revisa si hay CLIENTE_ID duplicados o nulos.")
    # -----------------------------------

    # Traemos los tiempos de descarga ---
    TABLA_TIEMPOS = "DWVEG_ORT.TEMP_ANALISIS_TIEMPOS_DESCARGA"
    query_tiempos = f"""
        SELECT CLIENTE_ID, MIN_CLIENTE_AVG
        FROM {TABLA_TIEMPOS}
        WHERE TIPO_RUTA = 'ESTANDAR' AND LOC_ORIGEN_ID = 10
    """
    df_tiempos = db.get_dataframe(query_tiempos)

    # Limpiamos nulos por si acaso y creamos diccionario: ID_LIMPIO -> MIN_CLIENTE_AVG
    df_tiempos['MIN_CLIENTE_AVG'] = df_tiempos['MIN_CLIENTE_AVG'].fillna(0)
    tiempos_lookup = {str(int(row['CLIENTE_ID'])).zfill(5): int(row['MIN_CLIENTE_AVG']) for _, row in df_tiempos.iterrows()}

    # Ventanas Temporales
    TABLA_VENTANAS = "DWVEG_ORT.RMG_FACT_SLA_REDUX" 
    # Traemos todos los procesos para poder diferenciarlos en el mapa
    query_v = f"""
        SELECT CLIENTE_ID, MINIMO, MAXIMO, PROCESO_ID
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
    visits_list.append({'loc_id': NODO_BASE, 'start': 0, 'end': 1440, 'type': 'depot', 'proceso': 'BASE', 'mce': 0, 'service_time': 0})

    # Agrupamos por cliente para gestionar ventanas múltiples

    # 1. Agrupamos las ventanas PMG por cliente para tenerlas a mano
    ventanas_por_cliente = {}
    for cliente_id, group in df_v.groupby('CLIENTE_ID'):
        id_limpio = str(int(cliente_id)).zfill(5)
        ventanas_por_cliente[id_limpio] = group

    # 2. ITERAMOS SOBRE TODA LA CARGA, NO SOBRE LAS VENTANAS
    for id_limpio, carga_real in mce_lookup.items():
        if carga_real <= 0:
            continue  # Si no pide nada, lo ignoramos
            
        node_id_real = f"C{id_limpio}"
        tiempo_descarga_real = tiempos_lookup.get(id_limpio, 0)

        # CASO A: El cliente TIENE ventanas PMG definidas en la base de datos
        if id_limpio in ventanas_por_cliente:
            group = ventanas_por_cliente[id_limpio]
            for _, row in group.iterrows():
                v_inicio = convertir_hora_a_minutos(row['MINIMO'])
                v_fin = convertir_hora_a_minutos(row['MAXIMO'])

                if v_inicio == 0 and v_fin == 0:
                    v_fin = 1440 

                visits_list.append({
                    'loc_id': node_id_real, 
                    'start': v_inicio, 
                    'end': v_fin, 
                    'type': 'client',
                    'proceso': row['PROCESO_ID'],
                    'mce': carga_real, 
                    'service_time': tiempo_descarga_real 
                })
                
        # CASO B: El cliente NO TIENE ventanas (es Seco, Congelado, o no está en la tabla)
        else:
            visits_list.append({
                'loc_id': node_id_real, 
                'start': 0, 
                'end': 1440, # Ventana COMODÍN de 24 horas
                'type': 'client',
                'proceso': 'ASUMIDO_COMO_PMG', # Esta etiqueta te ayudará a verlo en el mapa
                'mce': carga_real, 
                'service_time': tiempo_descarga_real 
            })

    # Añadimos los almacenes de recogida (Axxx)
    pickup_locs = df_dist[df_dist['LOC_DESTINO'].str.startswith('A')]['LOC_DESTINO'].unique()
    for loc in pickup_locs:
        if loc != NODO_BASE:
            visits_list.append({'loc_id': loc, 'start': 0, 'end': 1440, 'type': 'pickup', 'proceso': 'RECOGIDA', 'mce': 0, 'service_time': 0})

    # CONSTRUCCIÓN DE MATRICES BASADAS EN VISITAS ---
    num_visits = len(visits_list)
    dist_matrix = np.zeros((num_visits, num_visits))
    time_matrix = np.zeros((num_visits, num_visits))
    
    # Diccionario para búsqueda rápida de distancias físicas
    dist_lookup = df_dist.set_index(['LOC_ORIGEN', 'LOC_DESTINO'])[['DISTANCIA_KM', 'TIEMPO_MIN', 'COMPATIBILIDAD_SN']].to_dict('index')
    PENALIZACION = 5000000

    for i in range(num_visits):
        for j in range(num_visits):
            if i == j: continue
            loc_i = visits_list[i]['loc_id']
            loc_j = visits_list[j]['loc_id']
            if loc_i == loc_j: continue

            res = dist_lookup.get((loc_i, loc_j))
            if res:
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
    
    return dist_matrix.round().astype(int).tolist(), time_matrix.round().astype(int).tolist(), node_coords, idx_to_node, windows_final, visits_list

def create_data_model():
    """Define los datos del problem."""
    dist_matrix, time_matrix, node_coords, idx_to_node, windows_final, visits_list = get_data_from_sql()
    
    data = {}
    data['idx_to_node'] = idx_to_node
    data['node_to_idx'] = {v: k for k, v in idx_to_node.items()} 
    data['node_coords'] = node_coords
    data["distance_matrix"] = dist_matrix
    data["time_matrix"] = time_matrix
    data["depot"] = 0 

    # Cantidad de carga a depositar en cada entrega (USANDO MCE REALES)
    num_nodes = len(dist_matrix)
    demands = [0] * num_nodes
    service_times = [0] * num_nodes 
    delivery_nodes = []
    pickup_nodes = []
    
    for i, v in enumerate(visits_list):
        service_times[i] = v['service_time'] 
        
        if v['type'] == 'client':
            demands[i] = -int(v['mce'])
            delivery_nodes.append(i)
        elif v['type'] == 'pickup':
            demands[i] = 0 
            pickup_nodes.append(i)
   
    data["demands"] = demands
    data["service_times"] = service_times 
    data["delivery_nodes"] = delivery_nodes
    data["pickup_nodes"] = pickup_nodes

    data["num_vehicles"] = 150
    data["vehicle_capacities"] = [33] * data["num_vehicles"] 
    
    data["time_windows"] = windows_final
    data["visits_list"] = visits_list 
    
    return data

# FUNCIONES DE SALIDA Y VISUALIZACIÓN

def print_solution(data, manager, routing, solution):
    """Imprime rutas, carga, tiempo de trayecto y cantidad de vehículos usados."""
    total_distance = 0
    total_time = 0
    total_load_delivered = 0  # Contador global de carga 
    vehicles_used = 0 
    time_dimension = routing.GetDimensionOrDie("Time")
    capacity_dimension = routing.GetDimensionOrDie("Capacity")
    
    print(f"\n" + "="*30)
    print("DETALLE DE LAS RUTAS")
    print("="*30)

    for vehicle_id in range(data["num_vehicles"]):
        index = routing.Start(vehicle_id)
        
        if routing.IsEnd(solution.Value(routing.NextVar(index))):
            continue
            
        vehicles_used += 1
        plan_output = f"🚚 Ruta vehículo {vehicle_id}:\n"
        route_distance = 0
        nodes_visited = 0
        
        start_time = solution.Value(time_dimension.CumulVar(index))
        
        while not routing.IsEnd(index):
            nodes_visited += 1
            node_index = manager.IndexToNode(index)
            node_id = data['idx_to_node'][node_index]
            
            load_var = solution.Value(capacity_dimension.CumulVar(index))
            time_var = solution.Value(time_dimension.CumulVar(index))
            
            # Sumar carga si es un cliente (valor absoluto de la demanda)
            total_load_delivered += abs(data['demands'][node_index])
            
            horas = time_var // 60
            mins = time_var % 60
            plan_output += f"{node_id}(Carga={load_var}, Hora={horas:02d}:{mins:02d}) -> "
            
            previous_index = index
            index = solution.Value(routing.NextVar(index))
            route_distance += routing.GetArcCostForVehicle(previous_index, index, vehicle_id)
            
        node_index_final = manager.IndexToNode(index)
        node_id_final = data['idx_to_node'][node_index_final]
        end_time = solution.Value(time_dimension.CumulVar(index))
        
        horas_f = end_time // 60
        mins_f = end_time % 60
        
        duration = end_time - start_time
        dur_h = duration // 60
        dur_m = duration % 60
        
        plan_output += f"{node_id_final}(Hora={horas_f:02d}:{mins_f:02d})\n"
        plan_output += f"    Distancia: {route_distance}km |  Duración: {dur_h}h {dur_m}min\n"
        
        print(plan_output)
        total_distance += route_distance
        total_time += duration
    # Ver qué nodos no se visitaron 

    nodos_ignorados = []
    for node_index in range(1, len(data['demands'])):
        if solution.Value(routing.ActiveVar(manager.NodeToIndex(node_index))) == 0:
            nodos_ignorados.append(data['idx_to_node'][node_index])
    
    if nodos_ignorados:
        print("\n" + "="*50)
        print("🔍 MOTIVOS DE NODOS NO VISITADOS")
        print("="*50)
        for node_id in nodos_ignorados:
            motivo = analizar_causa_descarte(node_id, data, manager)
            print(f"📍 Nodo {node_id}: {motivo}")
        print("="*50 + "\n")
    else:
        print("\n✅ ¡Éxito! Todos los nodos han sido incluidos en las rutas.")
#####################################

    print("="*30)
    print(f"RESUMEN FINAL:")
    print(f"Vehículos utilizados: {vehicles_used}")
    print(f"Carga total entregada: {total_load_delivered} MCE") # Mostrar carga total 
    print(f"Distancia total: {total_distance}km")
    print(f"Tiempo total en ruta: {total_time // 60}h {total_time % 60}min")
    print("="*30)

def generate_map(data, manager, routing, solution):
    """Genera un mapa interactivo con popups enriquecidos, control de capas, leyenda y iconos de recogida,
    incluyendo ahora la visualización de nodos no visitados."""
    start_map_time = time.time()
    depot_coords = data['node_coords'][data['depot']]
    m = folium.Map(location=depot_coords, zoom_start=10, tiles="cartodbpositron")

    time_dimension = routing.GetDimensionOrDie("Time")
    capacity_dimension = routing.GetDimensionOrDie("Capacity")

    loading_screen = """
    <div id="loading-overlay" style="position: fixed; top: 0; left: 0; width: 100%; height: 100%; 
        background: rgba(255,255,255,0.95); z-index: 10000; display: flex; flex-direction: column;
        align-items: center; justify-content: center; font-family: sans-serif;">
        <div style="border: 8px solid #f3f3f3; border-top: 8px solid #e6194b; border-radius: 50%; 
            width: 60px; height: 60px; animation: spin 1s linear infinite;"></div>
        <h2 style="margin-top: 20px; color: #333;">Cargando Visualización TFM...</h2>
        <p style="color: #666;">Renderizando rutas y puntos de entrega</p>
    </div>
    <style>
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
    </style>
    <script>
        window.onload = function() {
            setTimeout(function() {
                document.getElementById('loading-overlay').style.display = 'none';
            }, 1000); 
        };
    </script>
    """
    m.get_root().html.add_child(folium.Element(loading_screen))

    colors = ['#e6194b', '#3cb44b', '#ffe119', '#4363d8', '#f58231', '#911eb4', '#46f0f0', '#f032e6', '#bcf60c', '#fabebe', '#008080', '#e6beff', '#9a6324', '#800000']
    
    legend_html = '''
     <div style="position: fixed; bottom: 50px; left: 50px; width: 180px; max-height: 250px; 
                  border:2px solid grey; z-index:9999; font-size:12px;
                  background-color:white; opacity: 0.9; padding: 10px; border-radius:5px;
                  overflow-y: auto;">
     <p style="margin-top:0; border-bottom: 1px solid #ccc;"><b>Leyenda Vehículos</b></p>
    '''

    TIENDAS_ESPECIALES = []

    # RUTAS ACTIVAS 
    for vehicle_id in range(data["num_vehicles"]):
        index = routing.Start(vehicle_id)
        if routing.IsEnd(solution.Value(routing.NextVar(index))):
            continue 
            
        color = colors[vehicle_id % len(colors)]
        vehicle_group = folium.FeatureGroup(name=f"Camión {vehicle_id}").add_to(m)
        legend_html += f'<p style="margin:2px;"><i class="fa fa-truck" style="color:{color}"></i> Vehículo {vehicle_id}</p>'
        
        route_coords = []
        previous_node_index = None  
        previous_start_time = 0

        while not routing.IsEnd(index):
            node_index = manager.IndexToNode(index)
            node_id = data['idx_to_node'][node_index]
            
            original_coords = data['node_coords'][node_index]
            lat, lon = original_coords[0], original_coords[1]
            info = data['visits_list'][node_index]
            
            if info['type'] == 'pickup':
                lat += 0.00008  
                lon += 0.00008  
            elif info['type'] == 'client':
                lat -= 0.00008  
                lon -= 0.00008  
            
            coords_visual = [lat, lon]
            route_coords.append(coords_visual) 
            
            start, end = data['time_windows'][node_index]
            time_val = solution.Value(time_dimension.CumulVar(index))
            load_actual = solution.Value(capacity_dimension.CumulVar(index))
            mce_operacion = abs(data['demands'][node_index])
            
            service_time_display = data['service_times'][node_index]
            exit_time = time_val + service_time_display
            
            if previous_node_index is None:
                wait_time = 0
            else:
                tiempo_viaje = data["time_matrix"][previous_node_index][node_index]
                tiempo_servicio_previo = data["service_times"][previous_node_index]
                hora_llegada = previous_start_time + tiempo_servicio_previo + tiempo_viaje
                wait_time = max(0, time_val - hora_llegada)
            
            previous_node_index = node_index
            previous_start_time = time_val

            popup_content = f"""
                <div style="min-width: 180px; font-family: Arial, sans-serif;">
                    <b style="color:{color}; font-size: 14px;">Vehículo: {vehicle_id}</b><br>
                    <b>ID Nodo:</b> {node_id}<br>
                    <hr style="margin: 5px 0;">
                    <b>➡️ Hora Entrada (Servicio):</b> {time_val//60:02d}:{time_val%60:02d}<br>
                    <b style="color:red;">⏳ Tiempo de Espera:</b> {wait_time} min<br>
                    <b>⬅️ Hora Salida:</b> {exit_time//60:02d}:{exit_time%60:02d}<br>
                    <b>⏱️ Tiempo Descarga:</b> {service_time_display} min<br>
                    <b>📦 Carga en Camión:</b> {load_actual} MCE<br>
                    <b>📥 Deja/Recoge:</b> {mce_operacion} MCE<br>
                    <hr style="margin: 5px 0;">
                    <b>Proceso:</b> {info['proceso']}<br>
                    <b>Ventana:</b> {start//60:02d}:{start%60:02d} - {end//60:02d}:{end%60:02d}
                </div>
            """
            
            if node_index == data['depot']:
                icon_name, icon_color, extra_prefix = 'home', 'black', 'fa'
            elif info['type'] == 'pickup':
                icon_name, icon_color, extra_prefix = 'archive', color, 'fa'
            elif node_id in TIENDAS_ESPECIALES:
                icon_name, icon_color, extra_prefix = 'star', 'orange', 'fa'
            else:
                icon_name, icon_color, extra_prefix = 'shopping-cart', color, 'fa'

            folium.Marker(
                location=coords_visual,
                popup=folium.Popup(popup_content, max_width=300),
                icon=folium.Icon(color='white', icon_color=icon_color, icon=icon_name, prefix=extra_prefix)
            ).add_to(vehicle_group)
            
            index = solution.Value(routing.NextVar(index))
        
        node_index_final = manager.IndexToNode(index)
        route_coords.append(data['node_coords'][node_index_final])
        folium.PolyLine(route_coords, color=color, weight=4, opacity=0.7).add_to(vehicle_group)

    #NUEVA SECCIÓN: NODOS NO VISITADOS 
    unvisited_group = folium.FeatureGroup(name="❌ NODOS NO VISITADOS").add_to(m)
    legend_html += '<hr style="margin:5px 0;"><p style="margin:2px; color:red;"><b>⚠️ No Visitados</b></p>'
    
    for node_index in range(1, len(data['demands'])):
        if solution.Value(routing.ActiveVar(manager.NodeToIndex(node_index))) == 0:
            node_id = data['idx_to_node'][node_index]
            coords = data['node_coords'][node_index]
            info = data['visits_list'][node_index]
            
            # Usamos tu función de análisis de descartes
            motivo = analizar_causa_descarte(node_id, data, manager)
            
            popup_fail = f"""
                <div style="min-width: 200px; font-family: Arial, sans-serif;">
                    <b style="color:red; font-size: 14px;">⚠️ NODO NO VISITADO</b><br>
                    <b>ID:</b> {node_id}<br>
                    <hr style="margin: 5px 0;">
                    <b style="color:darkred;">Motivo:</b> {motivo}<br>
                    <b>Carga solicitada:</b> {abs(data['demands'][node_index])} MCE<br>
                    <b>Ventana:</b> {info['start']//60:02d}:{info['start']%60:02d} - {info['end']//60:02d}:{info['end']%60:02d}<br>
                    <b>Proceso:</b> {info['proceso']}
                </div>
            """
            
            folium.Marker(
                location=coords,
                popup=folium.Popup(popup_fail, max_width=300),
                icon=folium.Icon(color='lightgray', icon_color='red', icon='exclamation-triangle', prefix='fa')
            ).add_to(unvisited_group)

    # CIERRE DE LEYENDA Y SCRIPTS 
    legend_html += '</div>'
    m.get_root().html.add_child(folium.Element(legend_html))
    folium.LayerControl(collapsed=False).add_to(m)
    
    toggle_script = """
    <script>
    function toggleAllLayers() {
        var checkboxes = document.querySelectorAll('.leaflet-control-layers-selector');
        checkboxes.forEach(function(checkbox) {
            checkbox.click();
        });
    }
    </script>
    <div style="position: fixed; top: 20px; left: 50%; transform: translateX(-50%); 
                z-index: 1000; background: white; padding: 2px; border: 2px solid #666; 
                border-radius: 8px; box-shadow: 0px 2px 5px rgba(0,0,0,0.2);">
        <button onclick="toggleAllLayers()" style="cursor: pointer; padding: 10px 20px; 
                font-weight: bold; background-color: #f8f9fa; border: none; border-radius: 5px;">
            👁️ Activar/Desactivar Todas las Rutas
        </button>
    </div>
    """
    m.get_root().html.add_child(folium.Element(toggle_script))
    m.save("mapa_rutas.html")
# BLOQUE PRINCIPAL DE EJECUCIÓN 

def exportar_auditoria_excel(data, manager, routing, solution):
    """Genera un Excel comparando la demanda solicitada vs la entregada por tienda."""
    print("\n📊 Generando Excel de auditoría de MCE...")
    
    resultados = []
    
    # Recorremos todos los nodos (saltando el depósito, índice 0)
    for node_index in range(1, len(data['demands'])):
        node_id = data['idx_to_node'][node_index]
        
        # La demanda que pedía este nodo (en valor absoluto)
        mce_solicitado = abs(data['demands'][node_index])
        
        # Comprobamos si el modelo decidió meter este nodo en la ruta
        fue_visitado = solution.Value(routing.ActiveVar(manager.NodeToIndex(node_index))) == 1
        
        # Si se visitó, se entregó todo. Si no, se entregó 0.
        mce_entregado = mce_solicitado if fue_visitado else 0
        
        resultados.append({
            "ID_Tienda": node_id,
            "MCE_Solicitado": mce_solicitado,
            "MCE_Entregado": mce_entregado,
            "Diferencia": mce_solicitado - mce_entregado
        })
        
    # Convertimos a DataFrame
    df_auditoria = pd.DataFrame(resultados)
    
    # Agrupamos por ID_Tienda sumando los MCE (por si la tienda se dividió en varias ventanas)
    df_resumen = df_auditoria.groupby("ID_Tienda").agg({
        "MCE_Solicitado": "sum",
        "MCE_Entregado": "sum",
        "Diferencia": "sum"
    }).reset_index()
    
    # Ordenamos para ver primero las que tienen mayor diferencia
    df_resumen = df_resumen.sort_values(by="Diferencia", ascending=False)
    
    # Añadimos una fila de totales al final para que cuadre con tu terminal
    totales = pd.DataFrame({
        "ID_Tienda": ["TOTAL"],
        "MCE_Solicitado": [df_resumen["MCE_Solicitado"].sum()],
        "MCE_Entregado": [df_resumen["MCE_Entregado"].sum()],
        "Diferencia": [df_resumen["Diferencia"].sum()]
    })
    df_resumen = pd.concat([df_resumen, totales], ignore_index=True)
    
    # Guardamos en Excel con un manejo de errores básico
    nombre_archivo = "Auditoria_MCE_TFM.xlsx"
    try:
        df_resumen.to_excel(nombre_archivo, index=False)
        print(f"✅ Excel guardado exitosamente como: {nombre_archivo}")
    except PermissionError:
        print(f"❌ ERROR: No se pudo guardar el Excel. Por favor, cierra '{nombre_archivo}' si lo tienes abierto y vuelve a intentarlo.")
    except Exception as e:
        print(f"❌ Ocurrió un error inesperado al guardar el Excel: {e}")

def main():
    print("\n" + "="*20)
    print("CARGANDO...")
    print("="*20)
    
    start_time_total = time.time()
    data = create_data_model()
    
    manager = pywrapcp.RoutingIndexManager(len(data["distance_matrix"]), data["num_vehicles"], data["depot"])
    routing = pywrapcp.RoutingModel(manager)
    
    def distance_callback(from_index, to_index):
        return data["distance_matrix"][manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]
    
    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)
    
    def demand_callback(from_index):
        return data["demands"][manager.IndexToNode(from_index)]
    
    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(demand_callback_index, 0, data["vehicle_capacities"], False, "Capacity")

    capacity_dimension = routing.GetDimensionOrDie("Capacity")
    for vehicle_id in range(data["num_vehicles"]):
        start_index = routing.Start(vehicle_id)
        capacity_dimension.CumulVar(start_index).SetValue(data["vehicle_capacities"][vehicle_id])

    # Actualizamos el callback de tiempo para incluir tiempo de descarga 
    def time_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        
        tiempo_viaje = data["time_matrix"][from_node][to_node]
        tiempo_servicio = data["service_times"][from_node]
        
        return tiempo_viaje + tiempo_servicio
    
    time_callback_index = routing.RegisterTransitCallback(time_callback)
    
    routing.AddDimension(time_callback_index, 60, 1440, False, "Time")
    time_dimension = routing.GetDimensionOrDie("Time")

    #Jornada laboral
    for vehicle_id in range(data["num_vehicles"]):
        time_dimension.SetSpanUpperBoundForVehicle(720, vehicle_id)

    # Configuración de Ventanas Temporales
    for node_index, (start, end) in enumerate(data["time_windows"]):
        index = manager.NodeToIndex(node_index)
        if start <= end:
            if node_index == data['depot']:
                time_dimension.CumulVar(index).SetRange(0, 1440)
            else:
                time_dimension.CumulVar(index).SetRange(start, end)

    def pickup_count_callback(from_index):
        node = manager.IndexToNode(from_index)
        return 1 if node in data["pickup_nodes"] else 0

    pickup_count_index = routing.RegisterUnaryTransitCallback(pickup_count_callback)
    routing.AddDimension(pickup_count_index, 0, len(data["pickup_nodes"]) + 1, True, "PickupSequence")
    sequence_dimension = routing.GetDimensionOrDie("PickupSequence")

    for d in data["delivery_nodes"]:
        d_index = manager.NodeToIndex(d)
        sequence_dimension.CumulVar(d_index).SetMax(0)

    # --- BLOQUE CORREGIDO: DISYUNCIONES AGRUPADAS POR TIENDA ---
    penalty = 10000000
    nodos_por_tienda = {}

    # Clasificamos los índices de los nodos por su ID de tienda físico
    for i, v in enumerate(data["visits_list"]):
        if i == 0: continue # El depósito no se penaliza
        
        if v['type'] == 'client':
            id_real = v['loc_id']
            if id_real not in nodos_por_tienda:
                nodos_por_tienda[id_real] = []
            nodos_por_tienda[id_real].append(manager.NodeToIndex(i))
        
        elif v['type'] == 'pickup':
            # Las recogidas (Axxx) se gestionan de forma individual
            routing.AddDisjunction([manager.NodeToIndex(i)], penalty)

    # AQUÍ ESTÁ LA MAGIA: Le decimos que de todas las ventanas de una tienda, 
    # solo elija UNA (el '1' al final es la clave).
    for tienda_id, indices in nodos_por_tienda.items():
        routing.AddDisjunction(indices, penalty, 1)
    
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (routing_enums_pb2.FirstSolutionStrategy.PATH_MOST_CONSTRAINED_ARC)
    search_parameters.local_search_metaheuristic = (routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
    search_parameters.time_limit.seconds = 75
    
   #############################
    solution = routing.SolveWithParameters(search_parameters)
    end_time_total = time.time()
    
    if solution:
        print("SOLUCIÓN ENCONTRADA")
        print(f" Tiempo Total Proceso: {end_time_total - start_time_total:.2f}s")
        print_solution(data, manager, routing, solution)
        generate_map(data, manager, routing, solution) 
        exportar_auditoria_excel(data, manager, routing, solution)
    else:
        print("\n No se encontró una solución viable en el tiempo establecido.")

# Nos dice por que no se visitan ciertos nodos
def analizar_causa_descarte(node_id, data, manager):
    # node_to_idx ya nos da el índice entero del nodo
    idx = data['node_to_idx'][node_id] 
    
    # Usamos abs() porque en demands los clientes están en negativo
    mce = abs(data['demands'][idx])
    ventana = data['time_windows'][idx]
    depot_idx = data['depot']
    
    # 1. Causa: Capacidad
    if mce > max(data['vehicle_capacities']):
        return f" CARGA: Pide {mce} MCE y el camión es de {max(data['vehicle_capacities'])}."

    # 2. Causa: Incompatibilidad
    dist_desde_deposito = data['distance_matrix'][depot_idx][idx]
    if dist_desde_deposito >= 5000000:
        return " COMPATIBILIDAD: Marcado como 'N' en SQL."

    # 3. Causa: Ventana Cerrada
    if ventana[0] == 0 and ventana[1] == 0:
        return "⏰ HORARIO: Ventana 00:00 - 00:00 (Cerrado)."

    # 4. Causa: Imposibilidad Temporal
    tiempo_viaje_minimo = data['time_matrix'][depot_idx][idx]
    if tiempo_viaje_minimo > ventana[1]:
        return f" TIEMPO: Tarda {tiempo_viaje_minimo} min, pero el cliente cierra en el min {ventana[1]}."

    return "PENALIZACIÓN/NO HAY QUE DEJAR CARGA"


if __name__ == "__main__":
    main()

