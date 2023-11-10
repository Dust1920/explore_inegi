import pandas as pd
import requests
import json
import warnings
from numpy import nan

class INEGI_BI:
    def __init__(self, token):
        self.__token = token 
        self.__url_base = 'https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/'
        self.__url_indicador =  self.__url_base + 'INDICATOR/'
        self._indicadores = list() 
        self._bi = list() 
        self._columnas = list() 
        self.__clave_entidad = None

    def __call_api_request(self, url_api):
        requests_result = requests.get(url_api, headers={"User-Agent":self.__token})
        try:
            assert requests_result.status_code == 200, 'Favor de revisar sus parametros o token asignado, ya que no se encontró información.'
            data_serie = json.loads(requests_result.text)
            return data_serie
        except requests.exceptions.Timeout:
            warnings.warn('El tiempo de consulta se ha agotado. Favor de intentar mas tarde.')
        except requests.exceptions.TooManyRedirects:
                warnings.warn('Fallo inesperado. Favor de intentar mas tarde.')
        except requests.exceptions.RequestException as e:
                warnings.warn('Error. favor de reportar a: .')
            #raise SystemExit(e)

    def __busca_bi(self, indicador):
        if len(indicador) < 10: 
            return 'BIE'
        else: 
            return 'BISE'

    def __datos_json_api(self, indicador, bi):
        bi = self.__busca_bi(indicador)
        url_api = '{}{}/es/{}/false/{}/2.0/{}?type=json'.format(self.__url_indicador, indicador,  self.__clave_entidad, bi, str(self.__token))
        datos = self.__call_api_request(url_api)
        return datos['Series'][0], bi

    def __json_a_df(self, datos, bi):
        serie = datos.pop('OBSERVATIONS')

        obs_totales = len(serie)
        dic = {'periodo':[serie[i]['TIME_PERIOD'] for i in range(obs_totales)],
                'valor':[float(serie[i]['OBS_VALUE']) if serie[i]['OBS_VALUE'] is not None else nan for i in range(obs_totales)]}
        df = pd.DataFrame.from_dict(dic)
        
        frecuencia = datos['FREQ']
        #factor, period = self.__frecuancias_dict[bi].get(frecuencia)
        df.set_index(df.periodo,inplace=True, drop=True)
        df = df.drop(['periodo'],axis=1)
        
        datos['BI'] = bi
        meta = pd.DataFrame.from_dict(datos, orient='index', columns=['valor'])
        return df, meta

    def __definir_cve_ent(self, entidad):
        cve_base = '0700'
        if entidad == '00': 
            self.__clave_entidad = cve_base
            return
        if len(entidad[2:5]) == 0: 
            self.__clave_entidad = '{}00{}'.format(cve_base, entidad[:2])
        else: 
            self.__clave_entidad = '{}00{}0{}'.format(cve_base, entidad[:2], entidad[2:5])
            
    def _consulta(self, inicio, fin, bi, metadatos):
        if isinstance(self._indicadores, str): self._indicadores = [self._indicadores]
        if isinstance(self._bi, str): self._bi = [self._bi]
        if isinstance(self._columnas, str): self._columnas = [self._indicadores]
        
        lista_df = list()
        meta_dfs = list()
        
        for i in range(len(self._indicadores)):
            indicador = self._indicadores[i]
            data, bi = self. __datos_json_api(indicador, bi)
            df, meta = self.__json_a_df(data, bi)
            if bi == 'BIE': 
                df = df[::-1]
            lista_df.append(df)
            meta_dfs.append(meta)
        df = pd.concat(lista_df,axis=1)
        meta = pd.concat(meta_dfs, axis=1)

        try: 
            df.columns = self._columnas
            meta.columns = self._columnas
        except: 
            warnings.warn('Los nombres no coinciden con el número de indicadores')
            df.columns = self._indicadores
            meta.columns = self._indicadores

        if metadatos is False: 
            return df[inicio:fin] 
        else: 
            return df[inicio:fin], meta

    def obtener_datos(self, indicadores: 'str|list', clave_area: str = '00', inicio: str = None, 
                        fin: str = None, bi: str = None, metadatos: bool = False):
        self._indicadores = indicadores
        self._columnas = indicadores
        #if nombres is not None: 
        #self._columnas = nombres
        self.__definir_cve_ent(clave_area)
        return self._consulta(inicio, fin, bi, metadatos)

    # Metadatos
    def _consultar_catalogo(self, clave, id, bi):
        url_api = '{}{}/{}/es/{}/2.0/{}/?type=json'.format(self.__url_base, clave, id, bi, self.__token)
        request_api = requests.get(url_api)
        datos = json.loads(request_api.text)
        return pd.DataFrame(datos['CODE'])

    def catalogo_indicadores(self, bi: str, indicador: str = None):
        if indicador is None: indicador = 'null'
        return self._consultar_catalogo('CL_INDICATOR', indicador, bi)

    def consulta_metadatos(self, metadatos: 'DataFrame|dict'):
        if isinstance(metadatos, dict): metadatos = pd.DataFrame.from_dict(dict)
        n_df = metadatos.copy(deep=True)
        for col in metadatos.columns:
            bi = metadatos.loc['BI',col]
            for idx in metadatos.index: 
                if idx in ['LASTUPDATE','BI']: continue
                id = metadatos.loc[idx,col]
                if id is None or len(id) == 0: continue
                if idx == 'INDICADOR': 
                    clave = 'CL_INDICATOR'
                else: 
                    clave = 'CL_{}'.format(idx)
                try:
                    desc = self._consultar_catalogo(clave, id, bi)
                    n_df.loc[idx,col] = desc.iloc[0,1]
                except: 
                    n_df.loc[idx,col] = 'La información no existe'
        return n_df
    
TOKEN = "987d176a-706d-74f4-b29b-e420e09f5b22"
API_INEGI_BI= INEGI_BI(TOKEN)

datos, metadatos_claves = API_INEGI_BI.obtener_datos(indicadores = "1002000002",
clave_area = '01',
inicio = '1980', 
fin = '2020',
metadatos = True)
