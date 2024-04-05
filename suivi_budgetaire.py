import snowflake.snowpark
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, lit, replace, coalesce, iff
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import sproc, sql_expr, is_boolean
import snowflake.snowpark.types as types
import streamlit as st
from streamlit_extras.st_keyup import st_keyup
import os
import json
import pandas as pd
import configparser
import unicodedata
import re
import locale
import random
from mitosheet.streamlit.v1 import spreadsheet
import datetime
import time


# Function to create Snowflake Session to connect to Snowflake
def create_session():
    # Chemin vers le fichier de configuration SnowSQL
    snowsql_config_file = os.path.expanduser("~\\.snowsql\\config")

    # Créer un objet ConfigParser
    config_parser = configparser.ConfigParser()

    # Lire le fichier de configuration SnowSQL
    config_parser.read(snowsql_config_file)

    # Nom de la connexion à récupérer
    nom_connexion = "connections.my_SYNERGY_Lab"

    # Accéder à la section de configuration correspondant à la connexion spécifique
    connection_section = config_parser[nom_connexion]
    
    # Récupérer les informations de connexion spécifiques à cette connexion
    snowflake_account = connection_section.get('accountname')
    snowflake_user = connection_section.get('username')
    snowflake_password = connection_section.get('password')
    snowflake_role = connection_section.get('rolename')
    snowflake_warehouse = connection_section.get('warehousename')
    #snowflake_database = connection_section.get('dbname')
    #snowflake_schema = connection_section.get('schemaname')
    snowflake_database = 'ICADE'
    snowflake_schema = 'PROTOTYPES'
    
    connection_parameters = {
        "account": snowflake_account,
        "user": snowflake_user,
        "password": snowflake_password.replace("'", ''),
        "role": snowflake_role,  # optional
        "warehouse": snowflake_warehouse,  # optional
        "database": snowflake_database,  # optional
        "schema": snowflake_schema,  # optional
    }
        
    if "snowpark_session" not in st.session_state:
        session = Session.builder.configs(connection_parameters).create()
        st.session_state['snowpark_session'] = session
    else:
        session = st.session_state['snowpark_session']
    return session

def load_bm_table(session,table_name,financial_year=2024):
    if 'df_all_budgets' not in st.session_state:
        #st.session_state['df_budget'] = session.table(table_name)
        abm_df=session.sql(f" SELECT  CODE_TYPE_FLUX,TYPE_FLUX \
                                    ,CODE_IMPUTATION, IMPUTATION \
                                    ,ANNEE, CODE_MOIS, MOIS \
                                    ,M_PREVISIONNEL, M_REEL \
                            FROM ICADE.PROTOTYPES.{table_name} \
                            ORDER BY CODE_TYPE_FLUX,CODE_IMPUTATION,ANNEE,CODE_MOIS" )
        st.session_state['df_all_budgets']=abm_df
        # financial years list
        spdf_fy=abm_df.select(col("ANNEE")).distinct()
        pddf_fy=spdf_fy.to_pandas()
        st.session_state['financial_years_list']=pddf_fy.sort_values(by=['ANNEE'], ascending=False).loc[:,"ANNEE"].to_list()
        # budget monitoring dataframe 
        st.session_state['financial_year']=financial_year
        st.session_state['df_budget']=abm_df.filter(col("ANNEE") == financial_year)   
        
        update_var_pivot(st.session_state['df_budget'])
        
        st.session_state["data_editor_forecasted_budget_key"]=0
        st.session_state["data_editor_forecasted_budget"]="data_editor_forecasted_budget_0"
        st.session_state['validate_save']=None
        st.session_state['go_save']=False
                   
        #st.write('initialisation')
         
    
# def load_fy_list():
#     if 'financial_years_list' not in st.session_state:
#         abm_df=st.session_state['df_all_budgets']
#         spdf_fy=abm_df.select(col("ANNEE")).distinct()
#         pddf_fy=spdf_fy.to_pandas()
#         st.session_state['financial_years_list']=pddf_fy.sort_values(by=['ANNEE'], ascending=False).loc[:,"ANNEE"].to_list()

        
def save_changes(efb_spdf):
    #efb_spdf = st.session_state['edited_forecasted_budget_spdf']
    # Convertir les colonnes de type LongType contenant des valeurs nulles en DoubleType
    for column in st.session_state['months_quoted']: 
        efb_spdf = efb_spdf.withColumn(column, efb_spdf[column].cast("DOUBLE"))
    
    unpivot_forecasted_budget_df=efb_spdf.unpivot("M_PREVISIONNEL","MOIS", st.session_state['months_quoted']) \
                                                                    .withColumn("MOIS", replace("MOIS","'",""))
    unpivot_months_fb_df = unpivot_forecasted_budget_df.join(st.session_state['spdf_months'], on=["MOIS"], how="inner")
    
    abm_df=st.session_state['df_all_budgets']      
    updated_df=abm_df.join(unpivot_months_fb_df, on=["CODE_TYPE_FLUX","CODE_IMPUTATION","ANNEE","CODE_MOIS"], how="leftouter", rsuffix="_L", lsuffix="_R") \
                     .select(    col("CODE_TYPE_FLUX"),col("TYPE_FLUX_R").alias("TYPE_FLUX"), \
                                col("CODE_IMPUTATION"),col("IMPUTATION_R").alias("IMPUTATION"), \
                                col("ANNEE"),col("CODE_MOIS"),col("MOIS_R").alias("MOIS"), col("MOIS_L"), \
                                iff(col("MOIS_L").isNull(),col("M_PREVISIONNEL_R"),col("M_PREVISIONNEL_L")).alias("M_PREVISIONNEL") \
                                ,"M_PREVISIONNEL_R","M_PREVISIONNEL_L",col("M_REEL") ) \
                     .drop("M_PREVISIONNEL_R","M_PREVISIONNEL_L","MOIS_L") \
                     .sort([col("CODE_TYPE_FLUX").asc(), col("CODE_IMPUTATION").asc(), col("ANNEE").asc(), col("CODE_MOIS").asc()])
    
    # budget monitoring dataframes    
    st.session_state['df_all_budgets']=updated_df
    st.session_state['df_budget']=updated_df.filter(col("ANNEE") == st.session_state['financial_year'])
    
    update_var_pivot(st.session_state['df_budget'])
    
    return updated_df
 
def update_table():
    st.session_state['validate_save']=True

def nothing_table():
    st.session_state['validate_save']=False

def cancel_changes():
    new_key=random.randint(1, 10)
    while new_key == st.session_state["data_editor_forecasted_budget_key"]:
        new_key = random.randint(1, 10)
    st.session_state["data_editor_forecasted_budget_key"]=new_key
    st.session_state["data_editor_forecasted_budget"]="data_editor_forecasted_budget_"+str(new_key) 
    #st.write('reset')
    
def select_change():
    abm_df=st.session_state['df_all_budgets']
    # budget monitoring dataframe
    st.session_state['df_budget']=abm_df.filter(col("ANNEE") == st.session_state['sel_financial_year'])
    st.session_state['financial_year']=st.session_state['sel_financial_year']
        
    update_var_pivot(st.session_state['df_budget'])
                                                                    
                                                                    
def update_var_pivot(bm_df):
    # months lists
    spdf_months=bm_df.select(col("CODE_MOIS"),col("MOIS")).distinct()
    st.session_state['spdf_months']=spdf_months
    pddf_months = spdf_months.to_pandas()
    st.session_state['months_list']=pddf_months.sort_values(by=['CODE_MOIS']).loc[:,"MOIS"].to_list()
    st.session_state['months_quoted']=["'{}'".format(month) for month in st.session_state['months_list']]

@st.cache_data
def get_forecast_budget(_abm_df,p_year):
    df_budget=_abm_df.filter(col("ANNEE") == p_year)
    st.session_state['df_budget']=df_budget
    # forecasted budget
    return df_budget.drop("CODE_MOIS","M_REEL").pivot("MOIS", st.session_state['months_list']).sum("M_PREVISIONNEL") \
                                               .sort(df_budget["CODE_TYPE_FLUX"],df_budget["CODE_IMPUTATION"],df_budget["ANNEE"]) \
                                               .collect() 
                                                                                           
        
# 
# main procedure
# 

def main():
    
       
    # Set page config
    st.set_page_config(layout="wide") 

    session = create_session()

    # create table
    # pdf_sb = pd.read_csv("suivi_budgetaire.csv", sep=";", header=0)
    # spdf_sb = session.create_dataframe(pdf_sb)
    # spdf_sb.write.mode("overwrite").save_as_table("SUIVI_BUDGETAIRE")
    # spdf_sb
    
    # 
    # Initialization -> Loading budget monitoring data
    # 
    load_bm_table(session,"SUIVI_BUDGETAIRE")
    
    # get the financial years
    #load_fy_list()
     
    st.header("Mise à jour du budget prévisionnel")
    
    begin = st.sidebar.container()
        
    m = st.markdown("""
        <style>
        div.stButton > button:first-child {
            background-color: #5B8EBC;
            color: white;
            border-radius:10px;
            font-size:20px;
            font-weight: bold;
            margin: auto;
            display: block;
        }

        div.stButton > button:hover {
            background:linear-gradient(to bottom, #ce1126 5%, #ff5a5a 100%);
            background-color:#ce1126;
        }

        div.stButton > button:active {
            position:relative;
            top:3px;
        }

        </style>""", unsafe_allow_html=True)
    
    with st.sidebar.container():            
        
        st.session_state['button_reset']=st.sidebar.button("Reset", type="primary", on_click=cancel_changes, use_container_width = True)
        if st.session_state['button_reset']:
            #st.write(':red[Changes cancelled !]')
            placeholder = st.empty()
            placeholder.write(':red[Changes cancelled !]')
            time.sleep(1)
            placeholder.empty()
      
    Third = st.sidebar.container()      
    Fourth = st.sidebar.container()
    
    financial_year_sel = begin.selectbox(
        "Pour quelle année d'exercice ?",
        st.session_state['financial_years_list'],
        index=0,
        key = 'sel_financial_year',
        on_change=select_change, 
        placeholder="Selection de l'année ...",
    )
    
    tab1, tab2 = st.tabs(["Forecasting", "Overview"])
    
    with tab1: 
        
       
        dataset_fb=get_forecast_budget(st.session_state['df_all_budgets'],st.session_state['sel_financial_year'])
            
        with st.form("data_editor_form"):      
            edited_forecasted_budget_df = st.data_editor(
                            dataset_fb,
                            hide_index=True, 
                            num_rows='fixed',
                            disabled=[("CODE_TYPE_FLUX"),("TYPE_FLUX"),("CODE_IMPUTATION"),("IMPUTATION"),("ANNEE")],
                            column_config={
                                "CODE_TYPE_FLUX": None,
                                "TYPE_FLUX": "Flux",
                                "CODE_IMPUTATION": None,
                                "IMPUTATION": "Imputation",
                                "ANNEE": None,
                                "MOIS": None,
                                "'janvier'": st.column_config.NumberColumn(
                                    "Janvier Prev",
                                    help="Montant prévisionnel pour le mois de janvier",
                                    min_value=-10000000,
                                    max_value=10000000,
                                    step=1,
                                    format="%d €",
                                ),
                                "'février'": st.column_config.NumberColumn(
                                    "Février Prev",
                                    help="Montant prévisionnel pour le mois de février",
                                    min_value=-10000000,
                                    max_value=10000000,
                                    step=1,
                                    format="%d €",
                                ),
                                "'mars'": st.column_config.NumberColumn(
                                    "Mars Prev",
                                    help="Montant prévisionnel pour le mois de mars",
                                    min_value=-10000000,
                                    max_value=10000000,
                                    step=1,
                                    format="%d €",
                                ),
                                "'avril'": st.column_config.NumberColumn(
                                    "Avril Prev",
                                    help="Montant prévisionnel pour le mois de avril",
                                    min_value=-10000000,
                                    max_value=10000000,
                                    step=1,
                                    format="%d €",
                                ),
                                "'mai'": st.column_config.NumberColumn(
                                    "Mai Prev",
                                    help="Montant prévisionnel pour le mois de mai",
                                    min_value=-10000000,
                                    max_value=10000000,
                                    step=1,
                                    format="%d €",
                                ),
                                "'juin'": st.column_config.NumberColumn(
                                    "Juin Prev",
                                    help="Montant prévisionnel pour le mois de juin",
                                    min_value=-10000000,
                                    max_value=10000000,
                                    step=1,
                                    format="%d €",
                                ),
                                "'juillet'": st.column_config.NumberColumn(
                                    "Juillet Prev",
                                    help="Montant prévisionnel pour le mois de juillet",
                                    min_value=-10000000,
                                    max_value=10000000,
                                    step=1,
                                    format="%d €",
                                ),
                                "'août'": st.column_config.NumberColumn(
                                    "Août Prev",
                                    help="Montant prévisionnel pour le mois d'août",
                                    min_value=-10000000,
                                    max_value=10000000,
                                    step=1,
                                    format="%d €",
                                ),
                                "'septembre'": st.column_config.NumberColumn(
                                    "Septembre Prev",
                                    help="Montant prévisionnel pour le mois de septembre",
                                    min_value=-10000000,
                                    max_value=10000000,
                                    step=1,
                                    format="%d €",
                                ),
                                "'octobre'": st.column_config.NumberColumn(
                                    "Octobre Prev",
                                    help="Montant prévisionnel pour le mois d'octobre",
                                    min_value=-10000000,
                                    max_value=10000000,
                                    step=1,
                                    format="%d €",
                                ),
                                "'novembre'": st.column_config.NumberColumn(
                                    "Novembre Prev",
                                    help="Montant prévisionnel pour le mois de novembre",
                                    min_value=-10000000,
                                    max_value=10000000,
                                    step=1,
                                    format="%d €",
                                ),
                                "'décembre'": st.column_config.NumberColumn(
                                    "Décembre Prev",
                                    help="Montant prévisionnel pour le mois de décembre",
                                    min_value=-10000000,
                                    max_value=10000000,
                                    step=1,
                                    format="%d €",
                                )
                            },
                            use_container_width=True, 
                            key=st.session_state["data_editor_forecasted_budget"],
                            height = 700)
            submit_button = st.form_submit_button("Save", use_container_width=True)

        if submit_button:
            Third.write(':orange[Changes in progress !]')
            st.session_state['go_save']=True
            st.write('Mises à jour : ', st.session_state[st.session_state["data_editor_forecasted_budget"]]['edited_rows'])
            Third.write(" Voulez-vous confirmer ?")
            col1, col2 = Third.columns(2)
            col1.button('Oui', type="secondary", on_click=update_table)
            col2.button('Non', type="primary", on_click=nothing_table)

                
        if st.session_state['go_save']:
            
            placeholder_vs = Fourth.empty() 
            
            if st.session_state['validate_save']:
                updated_spdf=save_changes(session.create_dataframe(edited_forecasted_budget_df))
                updated_spdf.write.mode("overwrite").save_as_table('SUIVI_BUDGETAIRE')
                st.session_state['validate_save']=None
                st.session_state['go_save']=False
                placeholder_vs.write(':green[Changes saved in the table !]')
                time.sleep(1)
                                
            if st.session_state['validate_save']==False:
                st.session_state['validate_save']=None
                st.session_state['go_save']=False
                placeholder_vs.write(':blue[No Changes in the table !]')
                time.sleep(1)
            
            placeholder_vs.empty() 
                
        
                        
    with tab2:
                    
        st.subheader("Elaborer un tableau synthétique avec montants Réels v Prévisionnels")
        
        # pddf_budget=st.session_state['df_budget'].to_pandas()
        # st.write('pddf_budget : ',  pddf_budget)
        
        # pddf_budget_prev=pddf_budget.drop('M_REEL', axis=1).rename(columns={'M_PREVISIONNEL': 'MONTANT'})
        # pddf_budget_reel=pddf_budget.drop('M_PREVISIONNEL', axis=1).rename(columns={'M_REEL': 'MONTANT'})
        # pddf_budget_prev['TYPE_MONTANT'] ="Prév."
        # pddf_budget_prev['ANNEE'] = pddf_budget_prev['ANNEE'].astype(str)
        # pddf_budget_prev['CODE_MOIS'] = pddf_budget_prev['CODE_MOIS'].astype(str)
        # pddf_budget_reel['TYPE_MONTANT'] ="Réel"
        # pddf_budget_reel['ANNEE'] = pddf_budget_reel['ANNEE'].astype(str)
        # pddf_budget_reel['CODE_MOIS'] = pddf_budget_reel['CODE_MOIS'].astype(str)
        
        # pddf_budget_change=pd.concat([pddf_budget_prev, pddf_budget_reel], ignore_index=True)
        
        # #pddf_budget = pddf_budget.set_index(['CODE_TYPE_FLUX', 'TYPE_FLUX', 'CODE_IMPUTATION', 'IMPUTATION'])
        # # pivot_budget_pddf=pd.pivot_table(pddf_budget_reel, values=['M_PREVISIONNEL','M_REEL'], \
        # #                                               index=['CODE_TYPE_FLUX', 'TYPE_FLUX', 'CODE_IMPUTATION', 'IMPUTATION'], \
        # #                                               columns=['ANNEE', 'CODE_MOIS', 'MOIS'] )
        # # pivot_budget_pddf=pd.pivot_table(pddf_budget_change, values='MONTANT', \
        # #                                               index=['CODE_TYPE_FLUX', 'TYPE_FLUX', 'CODE_IMPUTATION', 'IMPUTATION'], \
        # #                                               columns=['ANNEE', 'CODE_MOIS', 'MOIS','TYPE_MONTANT'] )
        # pivot_budget_pddf=pd.pivot_table(pddf_budget_change, values='MONTANT', \
        #                                               index=['TYPE_FLUX', 'IMPUTATION'], \
        #                                               columns=['ANNEE','CODE_MOIS','MOIS','TYPE_MONTANT'] )
        # st.write('pivot_budget_pddf : ',  pivot_budget_pddf)
        
        # st.table(pivot_budget_pddf)
        
        # st.title('Tesla Stock Volume Analysis')

        # CSV_URL = 'https://raw.githubusercontent.com/plotly/datasets/master/tesla-stock-price.csv'
        # new_dfs, code = spreadsheet(CSV_URL)
        
        # st.write(code)
        
        # st.write(new_dfs)
        
        
# 
# run it!
# 

if __name__ == '__main__':
  main()