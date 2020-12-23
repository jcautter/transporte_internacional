import psycopg2
import sys
import numpy as np
from datetime import datetime, date
import os
import shutil
import pandas as pd
import pandas.io.sql as psql
import numpy as np

## Setup Role Functions ##

def connection_setup():
    ## Setup Database Connection
    conn = psycopg2.connect(
        host='host',
        database='database',
        user='user',
        password='passaword',
        port='port'
    )
    cur = conn.cursor()
    return conn, cur

def log_setup(path):
    ## Setup log file
    try:
        os.remove(path)
        fc = open(path, 'a')
    except Exception as  FileNotFoundError:
        fc = open(path, 'a')
    return fc

def setup_control_tables():
    data = {
        'tables': [
            'whs_controle_trafego_internacional_sem_gp',
            'whs_controle_trafego_internacional_com_gp',
            'whs_cont_trafego_internacional_sem_gp',
            'whs_cont_trafego_internacional_com_gp',
        ],
        'whs_controle_trafego_internacional_sem_gp': {
            'name': 'Sem GP Inicial e Sem Final',
            'table': 'whs_controle_trafego_internacional_sem_gp',
            'ant': False,
            'pos': False,
        },
        'whs_controle_trafego_internacional_com_gp': {
            'name': 'Sem GP Inicial e Com Final',
            'table': 'whs_controle_trafego_internacional_com_gp',
            'ant': False,
            'pos': True,
        },
        'whs_cont_trafego_internacional_sem_gp': {
            'name': 'Sem GP Inicial e Com Final',
            'table': 'whs_cont_trafego_internacional_sem_gp',
            'ant': True,
            'pos': False,
        },
        'whs_cont_trafego_internacional_com_gp': {
            'name': 'Sem GP Inicial e Com Final',
            'table': 'whs_cont_trafego_internacional_com_gp',
            'ant': True,
            'pos': True,
        },
    }
    return data

def get_display():
    return False

## Integration Role Functions ##

def log_print(log, msg):
    ## Log print
    if get_display():
        print(msg)
    log.write(f'{datetime.now()}: {msg}\n')

def query_execute(conn, cur, query):
    ## Query Execute and Commit
    cur.execute(query)
    conn.commit()

def dataframe_read_sql(conn, query):
    return pd.read_sql_query(sql, conn)

def dataframe_to_sql(conn, df, table):
    df.to_sql(table, con=conn)

## Flux Role Functions ##

def query_delete_data_from_control(table, dt_ciclo=None):
    ## build query delete from control
    query = "delete from " + table + " where 1=1 "
    if dt_ciclo is not None:
        query += f"and dt_ciclo >= '{dt_ciclo}' " 

def query_copy_data_to_control(table, dt_ciclo=None):
    ## build query insert to control
    query = "insert into " + table + " (\
                    id_trafego_internacional,\
                    id_empresa,\
                    id_pais,\
                    id_usuario,\
                    id_arquivo_origem,\
                    id_acordo,\
                    qt_chamada,\
                    vl_tarifa,\
                    qt_minutos,\
                    vl_liquido,\
                    dt_ciclo,\
                    dt_chamada,\
                    dt_criacao,\
                    dt_atualizacao,\
                    cd_debito_credito,\
                    cd_sentido,\
                    tx_descritor,\
                    tp_associacao,\
                    cd_eot_rel,\
                    ds_terminacao,\
                    cd_poi,\
                    tp_trafego_internacional\
                )\
                select\
                    id_trafego_internacional,\
                    id_empresa,\
                    id_pais,\
                    id_usuario,\
                    id_arquivo_origem,\
                    null,\
                    qt_chamada,\
                    vl_tarifa,\
                    qt_minutos,\
                    vl_liquido,\
                    dt_ciclo,\
                    dt_chamada,\
                    dt_criacao,\
                    dt_atualizacao,\
                    cd_debito_credito,\
                    cd_sentido,\
                    tx_descritor,\
                    null,\
                    cd_eot_rel,\
                    ds_terminacao,\
                    cd_poi,\
                    tp_trafego_internacional\
                from\
                    whs_trafego_internacional\
                where\
                    1 = 1 "
    if dt_ciclo is not None:
        query += f"and dt_ciclo >= '{dt_ciclo}' "
    return query

def load_new_data(conn, cur, log, tables:list, dt_ciclo=None):
    ## Load new data to control
    for table in tables:
        # Delete data
        query = query_delete_data_from_control(table, dt_ciclo)
        log_print(log, f'Table {table} trying delete data')
        try:
            query_execute(conn, cur, query)
            log_print(f'Table {table} data deleted')
        except Exception as err:
            log_print(err)
        # Insert data
        query = query_copy_data_to_control(table, dt_ciclo)
        log_print(log, f'Table {table} trying insert data')
        try:
            query_execute(conn, cur, query)
            log_print(log, f'Table {table} data inserted')
        except Exception as err:
            log_print(log, err)

def query_acordos(ant=False, pos=False):
    if ant:
        ant_txt = '1'
    else:
        ant_txt = '0'

    if pos:
        pos_txt = '1'
    else:
        pos_txt = '0'

    query = "SELECT \
        a.id_acordo_internacional,\
        a.id_empresa,\
        a.id_pais,\
        a.dt_vigencia_inicial + interval '1 month' * a.qt_mes_periodo_antecipacao * " + ant_txt + " dt_vigencia_inicial,\
        a.dt_vigencia_final + interval '1 month - 1 day' + interval '1 month' * a.qt_mes_periodo_carencia * " + pos_txt + " dt_vigencia_final,\
        a.fg_credito_debito,\
        a.fg_compromisso,\
        '_'+t.tp_mascara as tp_mascara,\
        a.qt_minutos,\
        a.tp_contrato,\
        a.id_ref_receita, \
        a.tp_trafego_internacional, \
        0 as qt_minutos_associados, \
        case when a.id_ref_receita is not null then 0 else null end qt_minutos_receita \
    FROM whs_acordo_internacional a \
    LEFT JOIN whs_tipo_trafego t \
        ON a.id_tp_trafego_comum = t.id_tipo_trafego \
    WHERE 1 = 1\
        AND a.fg_ativo <> 0 \
        AND a.fg_antecipado = 'N' \
    ORDER BY \
        a.fg_compromisso DESC, \
        a.dt_vigencia_final, \
        a.tp_contrato ASC, \
        a.dt_vigencia_final, \
        a.vl_tarifa DESC, \
        a.tp_contrato"
    return query

## Business Role Funcion ##

def get_trafic_dataset(conn, table):
    return dataframe_read_sql(conn, 'select * from ' + table)

def get_acordos_dataset(conn, ant=False, pos=False):
    return dataframe_read_sql(conn, query_acordos(ant, pos))

def get_id_acordo_for_trafic(row, df):
    return df.loc[
        (True)
        & (df['id_empresa'] == row['id_empresa'])
        & (df['id_pais'] == row['id_pais'])
        & (df['fg_credito_debito'] == row['cd_debito_credito'])
        & (row['tx_descritor'].match(r'^ '+df['tp_mascara'].str.replace('_', '.'))
        & (row['dt_chamada'] >= df['dt_vigencia_inicial']) & (row['dt_chamada'] <= df['dt_vigencia_final'])
        & (df['qt_minutos_associados'] < df['qt_minutos'])
        & (
            (df['id_ref_receita'].isna())
            | (df['qt_minutos_associados'] < df['qt_minutos_receita'])
        )& (
            (df['tp_trafego_internacional'] == 'TOTAL')
            | (df['tp_trafego_internacional'] == row['tp_trafego_internacional'])
        )
    ].iloc[0]

def trafic_divide(df, row, i, val):
    df.at[i, 'qt_minutos'] = row['qt_minutos'] - val
    row['qt_minutos'] = val
    return pd.concat([
        df.iloc[:i], 
        pd.DataFrame(row).T, 
        df.iloc[i:]]
        ).reset_index().drop(['index'], 1)

def associate(df, row, i, df_acordo):
    result = False
    check = True
    try:
        acordo = get_id_acordo_for_trafic(row, df_acordo)
    except Exception as err:
        #print(err)
        check = False
        
    # chech True has Id Acordo
    traf = 0
    if check:
        try:
            traf = df[df['id_acordo']==acordo['id_acordo_internacional']]['qt_minutos'].sum()
            traf_acordo = acordo[['qt_minutos', 'qt_minutos_receita']].T.dropna().values.min()
        except Exception as err:
            #print(err)
            pass
        
    # chech True has Id Acordo
    if check and traf_acordo > traf:
        if traf_acordo < traf + row['qt_minutos']:
            df = trafic_divide(df, row, i, traf_acordo - traf)
            result = True
        row['id_acordo'] = acordo['id_acordo_internacional']
    # When set a id acordo
    if not pd.isna(row['id_acordo']):
        df.at[i, 'id_acordo'] = row['id_acordo']
        df_acordo.loc[df_acordo['id_acordo_internacional']==row['id_acordo'], 'qt_minutos_associados'] += row['qt_minutos']
        df_acordo.loc[df_acordo['id_ref_receita']==row['id_acordo'], 'qt_minutos_receita'] += row['qt_minutos']
    return df, result, df_acordo

def process(conn, cur, log, processes_info):
    log_print(log, 'Carregando dados de acordo')
    df_contrato = get_acordos_dataset(conn, ant=processes_info['ant'], pos=processes_info['pos'])
    log_print(log, 'Carregando dados de trafego da tabela ' + processes_info['table'])
    df_trafego = get_trafic_dataset(conn, processes_info['table'])
    fg_break = True
    log_print(log, 'Inicio do fluxo de associação de trafego da tabela ' + processes_info['table'])
    while fg_break:
        fg_break = False
        for i, row in df_trafego.iterrows():
            if pd.isna(row['id_acordo']):
                df, fg_break, df_contrato = associate(df_trafego, row, i, df_contrato)
                if fg_break:
                    df_trafego = df
                    break
    log_print(log, 'Fim do fluxo de associação de trafego da tabela ' + processes_info['table'])
    query_execute(conn, cur, 'truncate table ' + processes_info['table'])
    log_print(log, 'Carga do trafego da tabela ' + processes_info['table'])
    dataframe_to_sql(conn, df_trafego, processes_info['table'])

def PROCESSA(opcao):
    conn, cur = set_connection()
    path_add = '/trd0/etl/shortfall/Rotina_hml' + r'/LOGS'
    path_log = path_add + "/cargalog.txt"

    log = log_setup(path_log)
    log_print(log, f'Starting process >> opcao: {opcao}')
    
    control_tables = setup_control_tables()
    
    # load new data
    log_print(log, 'Load data into control tables')
    load_new_data(control_tables['tables'])
    
    # execution of each process
    for table in control_tables['tables']:
        process(conn, cur, log, control_tables[table])
    log_print(log, 'Finishing process')
    shutil.copyfile(path_log, path_add + '/cargalog_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + '.txt')