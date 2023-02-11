import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class sql_analise():
    def __init__(self):
        # configuração de credenciais
        host = 'host.docker.internal'
        user = 'postgres'
        password = 'postgrespw'
        port = '32768'

        self.conn = psycopg2.connect(host=host, user=user, password=password, port=32768, database="postgres")
    def quais_esquemas_temos(self, schema, tabela):
        cursor = self.conn.cursor()
        query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = " \
                f"'{schema}' AND table_name = '{tabela}'"
        cursor.execute(query)
        # Armazena o resultado da consulta em uma lista
        columns = [row[0] for row in cursor.fetchall()]
        print(columns)
        self.conn.commit()
        cursor.close()
        return columns
    def count_dados(self, dados):
        cursor = self.conn.cursor()
        cursor.execute(f"""
            select count({dados}), {dados} from desenv_processos.processos_movimento pm 
            group by {dados}
            """)
        self.conn.commit()
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[f'count({dados})', f'{dados}'])
        cursor.close()
        print(df)
        return df

    def grafico(self, df, dados):
        df.plot(kind='bar', x=dados, y=f'count({dados})')
        mean = df[f'count({dados})'].mean()
        plt.ylim(mean - 1500, mean + 1500)
        plt.xlabel('staus')
        plt.ylabel('Contagem')
        plt.title(f'Contagem de {dados}')
        plt.axhline(mean, color='red', linestyle='dashed')
        plt.show()

if __name__ == "__main__":
    sq = sql_analise()
    dados_tabela = sq.quais_esquemas_temos('desenv_processos', 'processos_movimento')
    # para a coluna data, indice 1 cuidado são 10000 itens
    for colunas in dados_tabela[2:]:
        df = sq.count_dados(colunas)
        sq.grafico(df, dados=colunas)