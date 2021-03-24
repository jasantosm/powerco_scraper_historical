import pandas as pd
import time
from bs4 import BeautifulSoup

from mysql_service import insert_day, get_days
from sqlalchemy import create_engine



def transform():
    days_raw = get_days()

    days_list = []
    
    for n in range(len(days_raw)):
        titles = days_raw[n]['titles'].split('|')
        t_raw = days_raw[n]['tables'].split('^_^')
        date = days_raw[n]['date']

        day_dict = {}
        day_dict['date'] = date


        # 0. Generacion

        try:
            generacion = pd.read_html(t_raw[0], index_col=0)[0]
            day_dict['generacion_total_programada_redespacho'] = generacion.loc['GENERACION'].loc['Programada Redespacho (GWh)']
            day_dict['generacion_total_programada_despacho'] = generacion.loc['GENERACION'].loc['Programada Despacho (GWh)']
            day_dict['generacion_total_real'] = generacion.loc['GENERACION'].loc['Real (GWh)']
        except:
            day_dict['generacion_total_programada_redespacho'] = 'ND'
            day_dict['generacion_total_programada_despacho'] = 'ND'
            day_dict['generacion_total_real'] = 'ND'
            print(f'Error en generacion el dia: {date}')
            
            

        # 1. Intercambios internacionales
        try:
            intercambios_internacionales = pd.read_html(t_raw[1], index_col=0)[0]
            day_dict['importacion_programada_redespacho'] = intercambios_internacionales.loc['Importaciones'].loc['Programada Redespacho (GWh)']
            day_dict['importacion__real'] = intercambios_internacionales.loc['Importaciones'].loc['Real (GWh)']
            day_dict['exportacion_programada_redespacho'] = intercambios_internacionales.loc['Exportaciones'].loc['Programada Redespacho (GWh)']
            day_dict['exportacion__real'] = intercambios_internacionales.loc['Exportaciones'].loc['Real (GWh)']
        except:
            day_dict['importacion_programada_redespacho'] = 'ND'
            day_dict['importacion__real'] = 'ND'
            day_dict['exportacion_programada_redespacho'] = 'ND'
            day_dict['exportacion__real'] = 'ND'
            print(f'Error en intercambios internacionales el dia: {date}')
            
            
        # 2. Disponibilidad
        try:
            disponibilidad = pd.read_html(t_raw[2], index_col=0)[0]
            day_dict['disponibilidad_real'] = disponibilidad.loc['DISPONIBILIDAD'].loc['Real (MW)']
        except:
            day_dict['disponibilidad_real'] = 'ND'
            print(f'Error en disponibilidad el dia: {date}')
            
            
        # 3. Demanda no atendida
        try:
            demanda_no_atendida = pd.read_html(t_raw[3], index_col=0)[0]
            day_dict['demanda_no_atendida'] = demanda_no_atendida.loc['Total Demanda no atendida -SIN-'].loc['MWh']
        except:
            day_dict['demanda_no_atendida'] = 'ND'
            print(f'Error en demanda no atendida el dia: {date}')
            
            
        # 7. Costos
        try:
            demanda_no_atendida = pd.read_html(t_raw[7], index_col=0)[0]
            day_dict['costo_marginal_promedio_redespacho'] = demanda_no_atendida.loc['Costo Marginal Promedio del Redespacho ($/kWh)'].loc['$/kWh']
        except:
            day_dict['costo_marginal_promedio_redespacho'] = 'ND'
            print(f'Error en costo marginal promedio el dia: {date}')
            
        # 8. Aportes
        try:
            aportes = pd.read_html(t_raw[9], index_col=0)[0]
            indices = aportes.index

            indice_aportes = ''
            columna_aportes = ''

            for indice in indices:
                if str(indice) == 'TOTAL -SIN-':
                    indice_aportes = 'TOTAL -SIN-'
                    columna_aportes = 'GWh'
                elif str(indice) == 'Total SIN':
                    indice_aportes = 'Total SIN'
                    columna_aportes = "Caudal GWh"
                    
            day_dict['aportes_hidricos'] = aportes.loc[indice_aportes].loc[columna_aportes]
        except:
            day_dict['aportes_hidricos'] = "ND"
            print(f'Error en aportes el dia: {date}')
            
        # 9. Reservas
        try:
            
            reservas = pd.read_html(t_raw[10], index_col=0)[0]
            
            indices = reservas.index

            indice_reservas = ''
            columna_reservas = ''

            for indice in indices:
                if str(indice) == 'TOTAL -SIN-':
                    indice_reservas = 'TOTAL -SIN-'

                elif str(indice) == 'Total SIN':
                    indice_reservas = 'Total SIN'



            columnas = reservas.columns

            for columna in columnas:
                if str(columna) == 'Volumen Util Diario GWh':
                    columna_reservas_1 = 'Volumen Util Diario GWh'
                elif str(columna) == 'Volumen Util Diario GWh(1)':
                    columna_reservas_1 = 'Volumen Util Diario GWh(1)'

            for columna in columnas:
                if str(columna) == 'Volumen GWh':
                    columna_reservas_2 = 'Volumen GWh'
                elif str(columna) == 'Volumen GWh(4)':
                    columna_reservas_2 = 'Volumen GWh(4)'


            day_dict['volumen_util_diario'] = reservas.loc[indice_reservas].loc[columna_reservas_1]
            day_dict['volumen'] = reservas.loc[indice_reservas].loc[columna_reservas_2]
            
        except:
            day_dict['volumen_util_diario'] = 'ND'
            day_dict['volumen'] = 'ND'
            print(f'Error en volumen: {date}')
        
        days_list.append(day_dict)
        print('\n')
        print('Dia transformado: ', date)
    
    return days_list

def to_mysql():
    df = pd.DataFrame(transform())
    

    #Clave GCP SQL Instance: qju7lep86r1L4Nod
    engine = create_engine('mysql+mysqldb://root:qju7lep86r1L4Nod@34.86.123.197:3306/xmdata', echo = False)
    df.to_sql(name = 'data_prepared', con = engine, if_exists = 'append', index = False)

def main():
    to_mysql()

if __name__ == "__main__":
    main()