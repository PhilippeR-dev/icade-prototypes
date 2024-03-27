import snowflake.snowpark
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import sproc, sql_expr, is_boolean
import streamlit as st
from streamlit_extras.st_keyup import st_keyup
import os
import json
import pandas as pd
import configparser
import unicodedata
import re


# Function to create Snowflake Session to connect to Snowflake
def create_session():
    # Chemin vers le fichier de configuration SnowSQL
    snowsql_config_file = os.path.expanduser("~\\.snowsql\\config")

    # Créer un objet ConfigParser
    config_parser = configparser.ConfigParser()

    # Lire le fichier de configuration SnowSQL
    config_parser.read(snowsql_config_file)

    # Nom de la connexion à récupérer
    nom_connexion = "connections.my_EDHEC_Lab"

    # Accéder à la section de configuration correspondant à la connexion spécifique
    connection_section = config_parser[nom_connexion]
    
    # Récupérer les informations de connexion spécifiques à cette connexion
    snowflake_account = connection_section.get('accountname')
    snowflake_user = connection_section.get('username')
    snowflake_password = connection_section.get('password')
    snowflake_role = connection_section.get('rolename')
    snowflake_warehouse = connection_section.get('warehousename')
    snowflake_database = connection_section.get('dbname')
    #snowflake_schema = connection_section.get('schemaname')
    snowflake_schema = 'PUBLIC'
    
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


def load_ref_list(session_df_name,list_name,column_name):
    if list_name not in st.session_state:
        pd_df_entity = st.session_state[session_df_name].select(col(column_name)).to_pandas()
        entity_list = pd_df_entity.loc[:,column_name].to_list()
        st.session_state[list_name] = entity_list
       
def remove_special_chars(N):
    # Normaliser la chaîne en utilisant la forme décomposée Unicode (NFD)
    normalized = unicodedata.normalize('NFD', N)
    # Remplacer tous les caractères diacritiques par une chaîne vide
    removed = re.sub(r'[\u0300-\u036f]', '', normalized)
    # Remplacer les tirets et les espaces par une chaîne vide
    replaced = removed.replace('-', '').replace(' ', '')
    return replaced
        
def get_dataset_places_postcodes(session_snow,p_country_code,p_city_name="n/a",p_postcode="n/a",p_df_country=None):

    v_city_name_modified=remove_special_chars(p_city_name).upper()
    v_postcode_modified=remove_special_chars(p_postcode).upper()
    if (p_city_name == "n/a" or v_city_name_modified == '') and (p_postcode == "n/a" or v_postcode_modified != ''):
        df=session_snow.sql("SELECT  COUNTRY_CODE, COUNTRY_NAME, \
                                     PLACE_ID, LOCALITY, NEUTRAL_LOCALITY, \
                                     POSTCODE_ID, POSTCODE, NEUTRAL_POSTCODE, \
                                     REGION_ID, REGION_NAME \
                             FROM REF_DEV.PUBLIC.COUNTRY_PLACES_POSTCODES_SEARCH \
                             WHERE COUNTRY_CODE = ? \
                            ", params=[p_country_code] )
    elif (p_city_name != "n/a" and v_city_name_modified != '') and p_postcode == "n/a": 
        df=p_df_country.filter(col("NEUTRAL_LOCALITY").startswith(v_city_name_modified))
    elif p_city_name == "n/a" and (p_postcode != "n/a" and v_postcode_modified != '') :
        df=p_df_country.filter(col("NEUTRAL_POSTCODE").startswith(v_postcode_modified))    
    elif (p_city_name != "n/a" and v_city_name_modified != '') and (p_postcode != "n/a" and v_postcode_modified != ''):
        df=p_df_country.filter((col("NEUTRAL_LOCALITY").startswith(v_city_name_modified)) & (col("NEUTRAL_POSTCODE").startswith(v_postcode_modified)))          

    return df              
    
      
        
# 
# main procedure
# 

def main():
    
    # Set page config
    st.set_page_config(layout="wide") 

    session = create_session()
    
    # 
    # Initialization -> Loading country data
    # 
    if 'df_COUNTRIES' not in st.session_state:
        st.session_state['df_COUNTRIES']=session.sql("  SELECT   ISO||' '||NAME_EN as SEARCH_COUNTRY \
                                                                ,ISO as COUNTRY_CODE \
                                                                ,NAME_EN as COUNTRY_NAME \
                                                        FROM REF_DEV.REF_GEOGRAPHIQUE_SAS.DATA_LANGUAGES \
                                                        WHERE SOVEREIGN ='' \
                                                        ORDER BY 1" )
        
    load_ref_list('df_COUNTRIES','countries_list','SEARCH_COUNTRY')
   
    #
    # Initialization -> Loading data for towns and postcodes
    #  
    if 'df_PLACES_POSTCODES_DISPLAY' not in st.session_state:
        st.session_state['country_code'] = 'FR'
        st.session_state["country_key"] = 'FR France'
        df_PLACES_POSTCODES_FR=get_dataset_places_postcodes(session,st.session_state['country_code'])
        df_PLACES_POSTCODES_DISPLAY=df_PLACES_POSTCODES_FR.with_column("SELBOX", lit(False)) \
                                                          .filter(sql_expr("NEUTRAL_LOCALITY LIKE 'PARIS%'")) \
                                                          .sort(col("NEUTRAL_LOCALITY").asc(),col("NEUTRAL_POSTCODE").asc()) \
                                                          .limit(50)
        st.session_state['df_PLACES_POSTCODES_COUNTRY']=df_PLACES_POSTCODES_FR 
        st.session_state['df_PLACES_POSTCODES_FR']=df_PLACES_POSTCODES_FR
        st.session_state['df_PLACES_POSTCODES_DISPLAY']=df_PLACES_POSTCODES_DISPLAY
    
     
    #
    # Update of the list of towns/postcodes by country
    #
    b_country_input = False      
    if 'selbox_country' in st.session_state and 'country_key' in st.session_state and st.session_state['selbox_country'] != st.session_state['country_key']:
        b_country_input = True 
    elif 'selbox_country' in st.session_state and 'country_key' not in st.session_state:
        b_country_input = True
        
     
        
    if b_country_input:
        df_pd_country_code=st.session_state['df_COUNTRIES'] \
                                .select(col("COUNTRY_CODE")) \
                                .filter(col("SEARCH_COUNTRY") == st.session_state['selbox_country']) \
                                .to_pandas()
        country_code=df_pd_country_code.iloc[0, 0]
        if country_code == 'FR':
            df_PLACES_POSTCODES=st.session_state['df_PLACES_POSTCODES_FR']
            st.session_state['df_PLACES_POSTCODES_COUNTRY']=df_PLACES_POSTCODES
        else:
            st.session_state['df_PLACES_POSTCODES_COUNTRY']=get_dataset_places_postcodes(session,country_code)
            if 'city_key' not in st.session_state and 'postcode_key' not in st.session_state:
                df_PLACES_POSTCODES=st.session_state['df_PLACES_POSTCODES_COUNTRY']
            elif 'city_key' in st.session_state and 'postcode_key' not in st.session_state:
                df_PLACES_POSTCODES=get_dataset_places_postcodes(   session,
                                                                    country_code,
                                                                    p_city_name=st.session_state['city_key'],
                                                                    p_df_country=st.session_state['df_PLACES_POSTCODES_COUNTRY']    )
            elif 'city_key' not in st.session_state and 'postcode_key' in st.session_state:
                df_PLACES_POSTCODES=get_dataset_places_postcodes(   session,
                                                                    country_code,
                                                                    p_postcode=st.session_state['postcode_key'],
                                                                    p_df_country=st.session_state['df_PLACES_POSTCODES_COUNTRY']    )
            elif 'city_key' in st.session_state and 'postcode_key' in st.session_state:
                df_PLACES_POSTCODES=get_dataset_places_postcodes(   session,
                                                                    country_code,
                                                                    p_city_name=st.session_state['city_key'],
                                                                    p_postcode=st.session_state['postcode_key'],
                                                                    p_df_country=st.session_state['df_PLACES_POSTCODES_COUNTRY']    )
                    
        
        st.session_state['df_PLACES_POSTCODES_DISPLAY']=df_PLACES_POSTCODES.with_column("SELBOX", lit(False)) \
                                                                           .sort(col("NEUTRAL_LOCALITY").asc(),col("NEUTRAL_POSTCODE").asc()) \
                                                                           .limit(50)
        st.session_state['country_code'] = country_code
        st.session_state['country_key'] = st.session_state['selbox_country']
        
    
    #
    # Update of the list of towns/postcodes by city or postcode
    # 

    b_city_input = False
    b_postcode_input = False
    if 'city_key' in st.session_state and 'city_name_key' in st.session_state and st.session_state['city_key'] != st.session_state['city_name_key']:
        b_city_input = True
    elif 'city_key' in st.session_state and 'city_name_key' not in st.session_state:
        b_city_input = True  
    elif 'postcode_key' in st.session_state and 'postcode_code_key' in st.session_state and st.session_state['postcode_key'] != st.session_state['postcode_code_key']:
        b_postcode_input = True
    elif 'postcode_key' in st.session_state and 'postcode_code_key' not in st.session_state:
        b_postcode_input = True     
    
    if b_city_input or b_postcode_input:
                           
        if b_city_input:
            vcity_name_search=st.session_state['city_key']
        elif 'city_name_key' in st.session_state:
            vcity_name_search=st.session_state['city_name_key']
        else:
            vcity_name_search=''

        if b_postcode_input:
            vpostcode_search=st.session_state['postcode_key']
        elif 'postcode_code_key' in st.session_state:
            vpostcode_search=st.session_state['postcode_code_key']
        else:
            vpostcode_search=''
                                
        ##
        ## define reload dataframe 
        ##
                
        if vcity_name_search != '' and vpostcode_search != '': 
            df_PLACES_POSTCODES=get_dataset_places_postcodes(   session,
                                                                st.session_state['country_code'],
                                                                p_city_name=vcity_name_search,
                                                                p_postcode=vpostcode_search,
                                                                p_df_country=st.session_state['df_PLACES_POSTCODES_COUNTRY']    )    
        elif vcity_name_search != '' and vpostcode_search == '':
            df_PLACES_POSTCODES=get_dataset_places_postcodes(   session,
                                                                st.session_state['country_code'],
                                                                p_city_name=vcity_name_search,
                                                                p_df_country=st.session_state['df_PLACES_POSTCODES_COUNTRY']    )     
        elif vcity_name_search == '' and vpostcode_search != '':
            df_PLACES_POSTCODES=get_dataset_places_postcodes(   session,
                                                                st.session_state['country_code'],
                                                                p_postcode=vpostcode_search,
                                                                p_df_country=st.session_state['df_PLACES_POSTCODES_COUNTRY']    )
        else:
            df_PLACES_POSTCODES=get_dataset_places_postcodes(   session,
                                                                st.session_state['country_code']   )                 
 
        st.session_state['df_PLACES_POSTCODES_DISPLAY']=df_PLACES_POSTCODES.with_column("SELBOX", lit(False)) \
                                                                           .sort(col("NEUTRAL_LOCALITY").asc(),col("NEUTRAL_POSTCODE").asc()) \
                                                                           .limit(50)   
        st.session_state['city_name_key']=vcity_name_search
        st.session_state['postcode_code_key']=vpostcode_search

    
    st.header("Selection of towns in the geographic reference database")
    
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
        
        countries_sel = begin.selectbox(
            'Which country for your address?',
            st.session_state['countries_list'],              
            #default = st.session_state['msel_default_list'],  
            key = 'selbox_country',
            index=st.session_state['countries_list'].index('FR France'),                              
            #on_change=selectbox_country_change,                        
            placeholder = 'Choose your country'
        )
            
        place_sel = st_keyup('Which city for your address?'
                                #,on_change=text_city_change
                                ,key="city_key"
                                ,placeholder = 'Choose your city'
                                ,debounce=500)
        
        poscode_sel = st_keyup('Which postcode for your address?'
                                #,on_change=text_postcode_change
                                ,key="postcode_key"
                                ,placeholder = 'Choose your postcode'
                                ,debounce=500)
        
        with st.sidebar.container():
        
            edited_locality_postcode_list = st.sidebar.data_editor(
                    st.session_state['df_PLACES_POSTCODES_DISPLAY'],
                    hide_index=True, 
                    num_rows='fixed',
                    disabled=[("PLACE_ID"),("LOCALITY"),("POSTCODE"),("REGION_ID"),("REGION_NAME")],
                    column_config={
                        "COUNTRY_CODE": None,
                        "COUNTRY_NAME": None,
                        "PLACE_ID": None,
                        "LOCALITY": "City Name",
                        "NEUTRAL_LOCALITY": None,
                        "POSTCODE_ID": None,
                        "POSTCODE": "PostCode",
                        "NEUTRAL_POSTCODE": None,
                        "REGION_ID": None,
                        "REGION_NAME": None,
                        "SELBOX" : "Select ?"
                    },
                    use_container_width=True, 
                    key='data_editor_locality_postcode_list', 
                    height = 500)
            
    
    b_de_update = False
    b_de_select_exists = False
    if 'data_editor_locality_postcode_list' in st.session_state:
        b_de_update = len(st.session_state["data_editor_locality_postcode_list"]['edited_rows'])>0
        if b_de_update:
            Id_DE_select_list=[key for (key,value) in sorted(st.session_state["data_editor_locality_postcode_list"]['edited_rows'].items()) if value["SELBOX"] == True ]
            if len(Id_DE_select_list)>0:
                Id_DE_select_box=Id_DE_select_list[0]
                v_place_id = st.session_state['df_PLACES_POSTCODES_DISPLAY'].collect()[Id_DE_select_box]['PLACE_ID']
                v_postcode_id = st.session_state['df_PLACES_POSTCODES_DISPLAY'].collect()[Id_DE_select_box]['POSTCODE_ID']
                st.session_state['df_PLACES_POSTCODES_DISPLAY2'] = st.session_state['df_PLACES_POSTCODES_DISPLAY'].filter((col("PLACE_ID") == v_place_id) & (col("POSTCODE_ID") == v_postcode_id))
                b_de_select_exists = True
            else:
                st.session_state['df_PLACES_POSTCODES_DISPLAY2'] = st.session_state['df_PLACES_POSTCODES_DISPLAY']  
        else:
            st.session_state['df_PLACES_POSTCODES_DISPLAY2'] = st.session_state['df_PLACES_POSTCODES_DISPLAY']  
    else:
        st.session_state['df_PLACES_POSTCODES_DISPLAY2'] = st.session_state['df_PLACES_POSTCODES_DISPLAY']
           
    if b_de_update != True or b_de_select_exists != True:
        edited_df_result = st.data_editor(
                    st.session_state['df_PLACES_POSTCODES_DISPLAY2'],
                    hide_index=True, 
                    num_rows='dynamic',
                    column_config={
                            "COUNTRY_CODE": "Country Name",
                            "COUNTRY_NAME": "Country Code",
                            "PLACE_ID": "Place Id",
                            "LOCALITY": "City Name",
                            "NEUTRAL_LOCALITY": "Neutral Locality",
                            "POSTCODE_ID": "Postcode Id",
                            "POSTCODE": "PostCode",
                            "NEUTRAL_POSTCODE": "Neutral Postcode",
                            "REGION_ID": "Region Id",
                            "REGION_NAME": "Region Name",
                            "SELBOX" : None
                        }, 
                    key='data_editor', 
                    height = 700 )
    else:
        st.subheader("Information on the selected location : ")
        i_country_code = st.text_input("Country Code", disabled=True, value=st.session_state['df_PLACES_POSTCODES_DISPLAY2'].collect()[0]['COUNTRY_CODE'])
        i_country_name = st.text_input("Country Name", disabled=True, value=st.session_state['df_PLACES_POSTCODES_DISPLAY2'].collect()[0]['COUNTRY_NAME'])
        i_place_id = st.text_input("Place Id", disabled=True, value=st.session_state['df_PLACES_POSTCODES_DISPLAY2'].collect()[0]['PLACE_ID'])
        i_locality = st.text_input("City Name", disabled=True, value=st.session_state['df_PLACES_POSTCODES_DISPLAY2'].collect()[0]['LOCALITY'])
        i_postcode_id = st.text_input("Postcode Id", disabled=True, value=st.session_state['df_PLACES_POSTCODES_DISPLAY2'].collect()[0]['POSTCODE_ID'])
        i_poscode = st.text_input("Postcode", disabled=True, value=st.session_state['df_PLACES_POSTCODES_DISPLAY2'].collect()[0]['POSTCODE'])
        i_region_id = st.text_input("Region Id", disabled=True, value=st.session_state['df_PLACES_POSTCODES_DISPLAY2'].collect()[0]['REGION_ID'])
        i_region_name = st.text_input("Region Name", disabled=True, value=st.session_state['df_PLACES_POSTCODES_DISPLAY2'].collect()[0]['REGION_NAME'])      

    
# 
# run it!
# 

if __name__ == '__main__':
  main()