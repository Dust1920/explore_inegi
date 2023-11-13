import pandas as pd
import numpy as np

enadid = pd.read_excel('fd_enadid_2018_2014_2018.xlsx', sheet_name=None)
dictsheets = enadid.keys()

def t_header(data: pd.DataFrame):
    indicate_remove = data[
    'INEGI. Encuesta Nacional de la Dinámica Demográfica 2014-2018. Descriptor de base de explotación '].isna()
    indicate_remove = list(indicate_remove)
    indicate_remove
    remove = 0
    t = 0
    while remove < 3:
        if indicate_remove[t] == True:
            remove += 1
        t += 1
    return t

def remove_header(t, data: pd.DataFrame):
    remove_index = np.arange(0, t)
    data = data.drop(index = remove_index)

def comparelists(listaA: list, listaB: list):
    d_index = []
    if len(listaA) == len(listaB):
        for i in range(len(listaA)):
            if listaA[i] != listaB[i]:
                d_index.append(i)
    return d_index

def couple_cols(actual: pd.DataFrame, new: pd.DataFrame):
    now_cols = list(actual.columns)
    new_cols = list(new.columns)
    if len(now_cols) != len(new_cols):
        return -1
    for i in range(len(now_cols)):
        now_cols[i] = str(now_cols[i]) 
        new_cols[i] = str(new_cols[i])
    mod_cols = comparelists(now_cols, new_cols)
    new_cols = {new_cols[i] : now_cols[i] for i in mod_cols}
    new = new.rename(columns=new_cols)
    return new

def df_blocks(ldf):
    superdf = pd.DataFrame()
    for q, df_test in enumerate(ldf):
        # print(q)
        df_cols = list(df_test.iloc[1,:])
        if df_cols[2] == 'Nemónico_PE':
            df_cols[2] = 'Mnemónico_PE'
        df_test.columns = df_cols
        df_name = df_test.iloc[0, 1]
        df_test.loc[:, ['SubNombre']] = df_name
        if q == 0:
            df_test = df_test.drop(columns = df_test.columns[0]).iloc[4: ,:]
        else:
            df_test = df_test.drop(columns = df_test.columns[0]).iloc[3: ,:]
            df_test = couple_cols(superdf, df_test)
        df_clean = df_test.dropna(axis= 0, how='all').ffill()
        # print(df_clean)
        superdf = pd.concat([superdf, df_clean])
    return superdf

all_df = pd.DataFrame()
for sheet in dictsheets:  
    print(sheet)
    tab_df = enadid[sheet]
    tab_df['Unnamed: 0'] = tab_df.isna().sum(axis = 1)
    n_cols = tab_df['Unnamed: 0'].max()
    sub_tables = tab_df[tab_df['Unnamed: 0'] == (n_cols - 1)]
    sub_tables = sub_tables[~sub_tables['INEGI. Encuesta Nacional de la Dinámica Demográfica 2014-2018. Descriptor de base de explotación '].isna()]
    names = sub_tables['INEGI. Encuesta Nacional de la Dinámica Demográfica 2014-2018. Descriptor de base de explotación '].to_list()
    sub_index = np.array(sub_tables.index)
    sub_index = sub_index[sub_index > t_header(tab_df)]
    subdfs = []
    for i in range(len(sub_index) - 1):
        subdfs.append(tab_df.iloc[sub_index[i]:sub_index[i + 1],:])
    if sub_index[i + 1] == sub_index[-1]:
        subdfs.append(tab_df.iloc[sub_index[i + 1]:,:])
    sub_all_df = df_blocks(subdfs)
    sub_all_df.set_index('ID',inplace=True)
    sub_all_df['Hoja'] = sheet
    all_df = pd.concat([all_df, sub_all_df])
all_df.to_csv('enadid_all.csv', encoding= 'utf8')