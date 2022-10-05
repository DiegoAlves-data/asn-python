# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sn

# COMMAND ----------

spark_dataframe = spark.table("silver_olist.order_items")
df = spark_dataframe.toPandas()

# COMMAND ----------

type(df)

# COMMAND ----------

df.head()

# COMMAND ----------

df[ df["vlPrice"] > 100 ]#

# COMMAND ----------

# Criar colunas e fazer um filtro (coluna com um logaritmo criado a partir do numpy)

df[ "log_Price"] = np.log(df["vlPrice"])
df

# COMMAND ----------

# Renomear colunas
# Metodo 1
# df = df.rename( columns= {"log_Price":"vlPriceLog"})
# Metodo 2
df.rename( columns= {"log_Price":"vlPriceLog"}, inplace = True)
df

# COMMAND ----------

plt.hist( df["vlPrice"])
plt.grid(True)
plt.title("Histograma para Preço de Produto")
plt.xlabel("Valores de Produto")
plt.ylabel("Frequencia")
plt.show()

# COMMAND ----------

plt.hist( df["vlPriceLog"])
plt.grid(True)
plt.title("Histograma para Preço de Produto (Log)")
plt.xlabel("Valores de Produto em Log")
plt.legend(["Valores em Log"])
plt.ylabel("Frequencia")
plt.show()

# COMMAND ----------

df["vlTotalPrice"] = df["vlPrice"] + df["vlFreight"]

df["vlTotalPrice"] = np.log(df["vlPrice"] + df["vlFreight"])
df.head()

# COMMAND ----------

# Ordenar o Data Frame

df = df.sort_values( by="vlTotalPrice")
df = df.reset_index(drop=True)

# COMMAND ----------

# Forma mais elegante de fazer a ordenação

df = (df.sort_values(by = "vlTotalPrice") # aqui ordenamos
      .reset_index(drop=True)) # aqui resetamos os índices

df

# COMMAND ----------

# Separar campos categóricos dos campos numéricos

condicao_numerico = (df.dtypes == "int64") | (df.dtypes == "float32") # retorna uma sérei booleana

var_nums = df.dtypes[condicao_numerico].index.tolist()
df[var_nums].describe()

# COMMAND ----------

df[var_nums].corr()

# COMMAND ----------

# conversões de tipo de dados

df["idOrderItem"] = df["idOrderItem"].astype("float")
df.dtypes

# COMMAND ----------

correlacao = df[var_nums].corr()
sn.heatmap(correlacao)


# COMMAND ----------

df = spark.table("silver_olist.products").toPandas()

# COMMAND ----------

np.sort( df["descCategoryName"].dropna().unique())

# COMMAND ----------

# não devemos fazer isso, pois loops são bem lentos

categoria = []
for i in df["descCategoryName"].fillna(""): #fillna para ele iterar com dados que não tenham nenhuma informação, preenchendo para solucionar problema de nonetype
    if "ferramenta" in i: 
        categoria.append("ferramenta")
    else:
        categoria.append(i)
        
categoria

# COMMAND ----------

# Criar uma função que agrega as categorias, quando houver várias categorias abaixo de uma mesma eu irei add à categoria maior, se não tiver o nome proposto ela irá manter o nome original
# alternativa pra isso e criar uma função que faça isso - ela padroniza 

def nova_categoria(x):
    if "ferramenta" in x: 
        return "ferramenta"
    else: 
        return x

# COMMAND ----------

nova_categoria("ferramenta_garagem") #resultado da função

# COMMAND ----------

# aplicar a função a todos os elementos da série sem precisar fazer um for

df["descCategoryName"].fillna("").apply(nova_categoria)

# COMMAND ----------

def nova_categoria(x):
    if "ferramenta" in x: 
        return "ferramenta"
    elif "casa"in x: 
        return "casa"
    elif "arte" in x:
        return "arte"
    elif "esporte" in x: 
        return "esporte"
    else:
        return x

# COMMAND ----------

# forma reduzida de fazer função com vários parametros

def nova_categoria(x, *args):
    for i in args:
        if i in x:
            return i
        else:
            return x

# COMMAND ----------

categorias_chaves = ["ferramenta", "arte", "casa", "esporte"]
nova_categoria("perfumaria", *categorias_chaves )

# COMMAND ----------

categorias_chaves = ["ferramenta", "arte", "casa", "esporte", "moveis"]
df["descCategoryName"].fillna("").apply(nova_categoria, args=categorias_chaves)
df.head(15)

# COMMAND ----------

# Quando uso um apply eu devo dizer se ele vai ser aplicado às linhas ou às colunas
