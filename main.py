from fastapi import FastAPI
import pandas as pd
from fastapi.responses import JSONResponse
# from Modelo.modelo_ML import recomendacion  # Importar la función de recomendación

app = FastAPI()

# Variables globales para los datasets, inicialmente vacías
movie_api = None
movie_cast = None
movie_crew = None
movies_filt = None

# Función para cargar los datasets cuando sean necesarios
def load_datasets():
    global movie_api, movie_cast, movie_crew, movies_filt
    if movie_api is None:
        movie_api = pd.read_parquet('datasets/movie_dataset_final.parquet', engine='pyarrow')
    if movie_cast is None:
        movie_cast = pd.read_parquet('datasets/movie_cast.parquet', engine='pyarrow')
    if movie_crew is None:
        movie_crew = pd.read_parquet('datasets/movie_crew.parquet', engine='pyarrow')
    if movies_filt is None:
        movies_filt = pd.read_parquet('datasets/movie_modelo.parquet')


#FUNCIONES
def f_filmaciones_mes(df, mes, column):
    mes= mes.capitalize()
    pelis_mes= df[df[column]==mes ]   

    len_films = len(pelis_mes)
    return f"{len_films} cantidad de películas fueron estrenadas en el mes de {mes}"

def f_filmaciones_dia(df,day,column ):
    dias_semana = {0:'Lunes', 1:'Martes', 2:'Miercoles', 3:'Jueves', 4: 'Viernes', 
               5:'Sabado', 6:'Domingo'}
    day = day.capitalize()

    if day not in dias_semana.values():
        return f"Error: {day} no es un día válido. Ingrese un día de la semana en español."


    df['release_dia'] =df[column].dt.day_of_week.map(dias_semana)
    films_day =  df[df['release_dia']== day]
    len_films = len(films_day)
    return f"{len_films} cantidad de películas fueron estrenadas en los días {day}"

def f_score_titulo( df, titulo ):
    #Filtra fila con el titulo de la pelicula
    titulo=titulo.title()

    film_row = df[ df["title"] == titulo]

    if film_row.empty:
        return f"Error: Película {titulo} no encontrada."
    
    anio_estreno= film_row["release_date"].dt.year.values[0]
    score = round(film_row["popularity"].values[0],2)

    return f"La película '{titulo}' fue estrenada en el año {anio_estreno} con un score/popularidad de {score}"

def f_votos_titulo( df, titulo ):

    titulo=titulo.title()
    #Filtra fila con el titulo de la pelicula
    film_row = df[ df["title"] == titulo]

    if film_row.empty:
        return f"Error: Película {titulo} no encontrada."
    
    anio_estreno= film_row["release_date"].dt.year.values[0]
    cant_voto= film_row["vote_count"].values[0]
    prom_voto = film_row["vote_average"].values[0]

    if cant_voto >=2000 :
        return f"La película {titulo} fue estrenada en el año {anio_estreno}. La misma cuenta con un total de {cant_voto} valoraciones, con un promedio de {prom_voto}"

    else:
        return f"La película {titulo} no cuenta con más de 2000 valoraciones. "

def f_get_actor(df, actor):
    actor = actor.title()

    #Se filtra dataframe con el nombre del actor
    df_actor = df[df["name_actor"] == actor]

    if df_actor.empty:
        return f"El actor '{actor}' no se encuentra en la base de datos. Ingrese otro por favor."
        
    cantidad_films = df_actor.shape[0]
    retorno = df ['return'].sum()
    promedio = round(retorno/cantidad_films , 6)

    return f"El actor {actor} ha participado de {cantidad_films} cantidad de filmaciones. El mismo ha conseguido un retorno de {retorno} con un promedio de {promedio}"

# def f_get_director(df, name):
#     name = name.title()

#     # Dataframe solo de directores
#     director_movie_crew =df[df["job_crew"]== 'Director']

#     # Se obtiene el dataframe con columnas necesarias
#     columnas = [ 'IdMovie', 'name_crew','return' ,'title','release_date','budget', 'revenue']
#     director_movie_crew = director_movie_crew[columnas]

#     # Se renombra columas
#     colum_name = {'name_crew':'Nombre_director', 'title':'Titulo','release_date':'Fecha_Estreno','budget': 'Costo', 'revenue':'Recaudación', 'return':'Retorno'}
#     director_movie_crew.rename(columns = colum_name, inplace = True)

#     #Columna Ganancia
#     director_movie_crew['Ganancia'] = director_movie_crew["Recaudación"] -director_movie_crew["Costo"]

#     #Se filtra dataframe con el nombre del actor
#     df_director = director_movie_crew[director_movie_crew["Nombre_director"] == name]

#     #Se obtiene diccionario final 
#     dict_director= df_director.drop( ['IdMovie','Nombre_director', 'Retorno' ], axis=1).to_dict(orient='records')

#     if df_director.empty:
#         return f"El director '{name}' no se encuentra en la base de datos. Ingrese otro por favor."


#     prom_retorno = df_director['Retorno'].mean()

#     return {
#             'Director': name, 
#             'Promedio Éxito': prom_retorno, 
#             'Películas': dict_director
#             }

# API
@app.get("/inicio")
async def ruta_prueba():
    return "Hola, esto es una prueba"

@app.get("/Cantidad_Filmaciones_Mes/{mes}")
def cantidad_filmaciones_mes(mes:str):
    """
    Input:
    - Mes del año. (str)

    Output:
    - Mensaje: 'n cantidad de películas fueron estrenadas en el mes de X.'
    """
    load_datasets()
    return {"message":f_filmaciones_mes(movie_api,mes,'release_meses')}

@app.get("/Cantidad_Filmaciones_Dia/{dia}")
def cantidad_filmaciones_dia(dia:str):
    """
    Input:
    - Día de la semana. (str)

    Output:
    - Mensaje: 'n cantidad de películas fueron estrenadas en los días X.'
    """
    load_datasets()
    return {"message":f_filmaciones_dia(movie_api,dia,'release_date')}

@app.get("/Score_Titulo/{titulo}")
def Score_Titulo(titulo:str):
    """
    Input:
    - Titulo de la película. (str)

    Output:
    - Mensaje: 'La película X fue estrenada en el año ___ con un score/popularidad de __.'
    """
    load_datasets()
    return {"message":f_score_titulo(movie_api,titulo)}

@app.get("/Votos_Titulo/{titulo}")
def Votos_Titulo(titulo:str):
    """
    Input:
    - Titulo de la película. (str)

    Output:
    - Mensaje: 'La película X fue estrenada en el año ___. La misma cuenta con un total de __ valoraciones, con un promedio de __'
    """
    load_datasets()
    return {"message":f_votos_titulo(movie_api,titulo)}

@app.get("/Get_Actor/{actor}")
def Get_Actor(actor:str):
    """
    Input:
    - Nombre del actor (str)

    Output:
    - Mensaje: 'El actor X ha participado de n cantidad de filmaciones, el mismo ha conseguido un retorno de __ con un promedio de __ por filmación'
     """
    load_datasets()
    return {"message":f_get_actor(movie_cast,actor)}

# @app.get("/Get_Director/{director}")
# def Get_Director(director:str):
#     """
#     Input:
#     - Nombre del director (str)

#     Output:
#     - Nombre del director.
#     - Promedio Éxito.
#     - Lista de películas
#      """
#     load_datasets()
#     return {"message":f_get_director(movie_crew,director)}


# @app.get("/Recomendacion/{titulo}")
# def Recomendacion(titulo:str):
#     """
#     Input:
#     - Titulo de la película (str)

#     Output:
#     - Lista de películas recomendadas
#     """
#     load_datasets()
#     lista_movies= recomendacion(titulo, movies_filt)

#     if lista_movies == 0: 
#         return f"Error: Película '{titulo}' no encontrada en el dataset."

#     return {"message":lista_movies}

