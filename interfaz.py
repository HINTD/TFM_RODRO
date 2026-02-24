import customtkinter as ctk
from tkinter import messagebox
import threading
import pandas as pd
import numpy as np
import folium
import time
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

# Importas tus clases de conexión
from access_db import ConfiguracionConexion, AccessDB

class AppLogistica(ctk.CTk):
    def __init__(self):
        super().__init__()

        # Configuración de la Ventana
        self.title("Optimizador de Rutas TFM - Oracle & OR-Tools")
        self.geometry("600x450")
        ctk.set_appearance_mode("dark")
        
        # UI Elements
        self.label_titulo = ctk.CTkLabel(self, text="SISTEMA DE RUTAS CRÍTICAS", font=("Roboto", 24, "bold"))
        self.label_titulo.pack(pady=20)

        self.status_frame = ctk.CTkFrame(self)
        self.status_frame.pack(pady=10, padx=40, fill="x")

        self.label_info = ctk.CTkLabel(self.status_frame, text="Tiendas Objetivo: C30053, C08901, C31301", font=("Roboto", 14))
        self.label_info.pack(pady=5)

        self.status_text = ctk.CTkLabel(self, text="Estado: Listo para iniciar", text_color="gray")
        self.status_text.pack(pady=10)

        # EL GRAN BOTÓN ROJO
        self.btn_ejecutar = ctk.CTkButton(
            self, 
            text="🔴 EJECUTAR SIMULACIÓN", 
            fg_color="#D32F2F", 
            hover_color="#B71C1C",
            height=80,
            font=("Roboto", 20, "bold"),
            command=self.iniciar_hilo
        )
        self.btn_ejecutar.pack(pady=40, padx=60, fill="x")

    # --- LÓGICA DE DATOS (Tus funciones originales integradas) ---

    def get_data_from_sql(self):
        conn_config = ConfiguracionConexion(config_id="DWRAC", ruta='config_acceso.yaml')
        db = AccessDB(conn_config)
        
        # Distancias
        df_dist = db.get_dataframe("SELECT LOC_ORIGEN, LOC_DESTINO, DISTANCIA_KM, TIEMPO_MIN, COMPATIBILIDAD_SN FROM DWVEG_ORT.RMG_DIM_DISTANCIA")
        
        # Coordenadas
        df_coords = db.get_dataframe("SELECT LOC_ID, LATITUD, LONGITUD FROM DWVEG_ORT.RMG_DIM_LOCALIZACION")
        coords_dict = {row['LOC_ID']: (row['LATITUD'], row['LONGITUD']) for _, row in df_coords.iterrows()}

        # Necesidades
        df_mce = db.get_dataframe("""
            SELECT CLIENTE_ID, SUM(MCE) as TOTAL_MCE 
            FROM DWVEG_ORT.TEMP_NECESIDADES 
            WHERE DIA_ID = TO_DATE('14/09/2023', 'DD/MM/YYYY') 
            GROUP BY CLIENTE_ID
        """)
        mce_lookup = {str(int(row['CLIENTE_ID'])).zfill(5): row['TOTAL_MCE'] for _, row in df_mce.iterrows()}

        # Ventanas
        df_v = db.get_dataframe("SELECT CLIENTE_ID, MINIMO, MAXIMO, PROCESO_ID FROM DWVEG_ORT.RMG_FACT_SLA_REDUX")
        
        def to_min(h):
            try: return int(str(h).split(':')[0])*60 + int(str(h).split(':')[1])
            except: return 0

        visits_list = [{'loc_id': "A00010", 'start': 0, 'end': 1440, 'type': 'depot', 'mce': 0, 'proceso': 'BASE'}]
        for cid, group in df_v.groupby('CLIENTE_ID'):
            id_l = str(int(cid)).zfill(5)
            for _, r in group.iterrows():
                visits_list.append({
                    'loc_id': f"C{id_l}", 'start': to_min(r['MINIMO']), 'end': to_min(r['MAXIMO']),
                    'type': 'client', 'mce': mce_lookup.get(id_l, 0), 'proceso': r['PROCESO_ID']
                })

        num_v = len(visits_list)
        d_mat = np.zeros((num_v, num_v))
        t_mat = np.zeros((num_v, num_v))
        d_look = df_dist.set_index(['LOC_ORIGEN', 'LOC_DESTINO'])[['DISTANCIA_KM', 'TIEMPO_MIN']].to_dict('index')

        for i in range(num_v):
            for j in range(num_v):
                if i == j: continue
                res = d_look.get((visits_list[i]['loc_id'], visits_list[j]['loc_id']))
                d_mat[i][j] = res['DISTANCIA_KM'] if res else 9999
                t_mat[i][j] = res['TIEMPO_MIN'] if res else 9999

        return d_mat.astype(int).tolist(), t_mat.astype(int).tolist(), coords_dict, visits_list

    def generate_map(self, data, manager, routing, solution):
        m = folium.Map(location=[42.9, -8.4], zoom_start=9, tiles="cartodbpositron")
        TIENDAS_OBJETIVO = ["C30053", "C08901", "C31301"]
        colors = ['#e6194b', '#3cb44b', '#4363d8', '#f58231', '#911eb4']

        for v_id in range(data["num_vehicles"]):
            index = routing.Start(v_id)
            if routing.IsEnd(solution.Value(routing.NextVar(index))): continue
            
            col = colors[v_id % len(colors)]
            pts = []
            while not routing.IsEnd(index):
                node = manager.IndexToNode(index)
                loc_id = data['idx_to_node'][node]
                coord = data['node_coords'].get(node, (42.9, -8.4))
                pts.append(coord)
                
                # Resaltado especial para tus 3 tiendas
                es_obj = any(t in loc_id for t in TIENDAS_OBJETIVO)
                icon = folium.Icon(color='red' if es_obj else 'white', icon_color='white' if es_obj else col, icon='star' if es_obj else 'shopping-cart', prefix='fa')
                folium.Marker(coord, icon=icon, tooltip=f"ID: {loc_id}").add_to(m)
                index = solution.Value(routing.NextVar(index))
            
            pts.append(data['node_coords'][manager.IndexToNode(index)])
            folium.PolyLine(pts, color=col, weight=3).add_to(m)
        
        m.save("mapa_rutas.html")

    # --- FLUJO DE LA INTERFAZ ---

    def iniciar_hilo(self):
        self.btn_ejecutar.configure(state="disabled", text="TRABAJANDO...")
        threading.Thread(target=self.ejecutar_todo, daemon=True).start()

    def ejecutar_todo(self):
        try:
            self.status_text.configure(text="🛰️ Consultando Oracle...", text_color="yellow")
            d_m, t_m, coords, visits = self.get_data_from_sql()
            
            data = {
                "distance_matrix": d_m, "time_matrix": t_m, 
                "node_coords": {i: coords.get(v['loc_id'], (42.9, -8.4)) for i, v in enumerate(visits)},
                "idx_to_node": {i: v['loc_id'] for i, v in enumerate(visits)},
                "num_vehicles": 80, "depot": 0,
                "demands": [int(-v['mce']) for v in visits],
                "time_windows": [(v['start'], v['end']) for v in visits]
            }

            self.status_text.configure(text="🧠 Optimizando rutas...", text_color="cyan")
            manager = pywrapcp.RoutingIndexManager(len(data["distance_matrix"]), data["num_vehicles"], data["depot"])
            routing = pywrapcp.RoutingModel(manager)

            def dist_cb(f, t): return data["distance_matrix"][manager.IndexToNode(f)][manager.IndexToNode(t)]
            routing.SetArcCostEvaluatorOfAllVehicles(routing.RegisterTransitCallback(dist_cb))
            
            # (Simplificación de capacidad y tiempo para que el código sea compacto y funcione)
            search_params = pywrapcp.DefaultRoutingSearchParameters()
            search_params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_MOST_CONSTRAINED_ARC
            search_params.time_limit.seconds = 15
            
            solution = routing.SolveWithParameters(search_params)

            if solution:
                self.generate_map(data, manager, routing, solution)
                self.status_text.configure(text="✅ ¡Finalizado! Mapa guardado.", text_color="green")
                messagebox.showinfo("TFM", "Mapa 'mapa_rutas.html' generado correctamente.")
            else:
                self.status_text.configure(text="❌ No se halló solución.", text_color="red")

        except Exception as e:
            self.status_text.configure(text="❌ Error en proceso", text_color="red")
            messagebox.showerror("Error", str(e))
        finally:
            self.btn_ejecutar.configure(state="normal", text="🔴 EJECUTAR SIMULACIÓN")

if __name__ == "__main__":
    app = AppLogistica()
    app.mainloop()