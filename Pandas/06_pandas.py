# Databricks notebook source
# Transformando o spark tabel em pandas data frame
df_produto = spark.table("silver_olist.products").toPandas()
df_order_items = spark.table("silver_olist.order_items").toPandas()
df_sellers = spark.table("silver_olist.sellers").toPandas()


df_order_items.head()

# COMMAND ----------

# fazendo o join - Se o nome das duas colunas das duas tabelas for igual, usamos o "on", se forem diferentes devo usar o "right on ou lef on"

df_join = df_order_items.merge(df_produto, how="left", on="idProduct") #quando as tabelas estão com mesmo nome de colunas

# df_order_items.merge(df_produto, how="left", left_on="idProduct", right_on="idProduct") #segunda forma

# COMMAND ----------

# Como fazer a união de mais uma tabela - devo sempre fazer em pares; 

df_join_1 = df_order_items.merge(df_produto, how="left", on="idProduct")
df_join_2 = df_join_1.merge (df_sellers, how = "left", on= "idSeller", )

df_join_2.rename(columns = {"desCity": "desCitySeller", 
                            "descState": "descStateSeller", 
                            "nrZipPrefi": "nrZipPrefixSeller" })

df_join_2

# COMMAND ----------

# forma mais bonita para se fazer - encadeando os comandos

df_result = (df_order_items.merge(df_produto, how="left", on="idProduct")
                          .merge (df_sellers, how = "left", on= "idSeller", )
                          .rename(columns = {"desCity": "desCitySeller", 
                                             "descState": "descStateSeller", 
                                             "nrZipPrefi": "nrZipPrefixSeller" }))


df_result.head()

# COMMAND ----------

# Group By - categoria mais vendida - o group by deve sempre trazer algum agregador: soma, média etc etc; 

df_result = (df_order_items.merge(df_produto, how="left", on="idProduct")
                          .merge (df_sellers, how = "left", on= "idSeller", )
                          .rename(columns = {"desCity": "desCitySeller", 
                                             "descState": "descStateSeller", 
                                             "nrZipPrefi": "nrZipPrefixSeller" }))


(df_result.groupby( by= ["descCategoryName"] ) [["idOrder"]] # este é o campo que será agregado, agrupa por descCategoryName, fazendo calculo em ["idOrder"]
          .count()                                           # realiza a contagem de IdOrder
          .reset_index()                                     # transforma descCategory em coluna do resultado
          .rename(columns={"idOrder":"qtIdOrder"}))          # renomeia o IdOrder para qtIdOrder

# COMMAND ----------

df_result = (df_order_items.merge(df_produto, how="left", on="idProduct")
                          .merge (df_sellers, how = "left", on= "idSeller", )
                          .rename(columns = {"desCity": "desCitySeller", 
                                             "descState": "descStateSeller", 
                                             "nrZipPrefi": "nrZipPrefixSeller" }))


df_new = (df_result.groupby( by= ["descCategoryName"] ) [["idOrder", "idSeller"]] # este é o campo que será agregado, agrupa por descCategoryName, fazendo calculo em ["idOrder"]
          .nunique()                                           # conta os id únicos em cada categoria e id de sellers também
          .reset_index()                                     # transforma descCategory em coluna do resultado
          .rename(columns={"idOrder":"qtIdOrder"}))          # renomeia o IdOrder para qtIdOrder

df_new[]

# COMMAND ----------

df_result = (df_order_items.merge(df_produto, how="left", on="idProduct")
                          .merge (df_sellers, how = "left", on= "idSeller", )
                          .rename(columns = {"desCity": "desCitySeller", 
                                             "descState": "descStateSeller", 
                                             "nrZipPrefi": "nrZipPrefixSeller" }))


df_new = (df_result.groupby( by= ["descCategoryName"] ) [["idOrder", "idSeller"]] # este é o campo que será agregado, agrupa por descCategoryName, fazendo calculo em ["idOrder"]
          .describe()                                           # cria um multi célula
          .reset_index()                                     # transforma descCategory em coluna do resultado
          .rename(columns={"idOrder":"qtIdOrder"}))          # renomeia o IdOrder para qtIdOrder

df_new

# COMMAND ----------

df_result = (df_order_items.merge(df_produto, how="left", on="idProduct")
                          .merge (df_sellers, how = "left", on= "idSeller", )
                          .rename(columns = {"desCity": "desCitySeller", 
                                             "descState": "descStateSeller", 
                                             "nrZipPrefi": "nrZipPrefixSeller" }))


df_new = (df_result.groupby( by= ["descCategoryName"] ) [["idOrder", "idSeller"]] # este é o campo que será agregado, agrupa por descCategoryName, fazendo calculo em ["idOrder"]
          .describe()                                           # cria um multi célula
          .reset_index()                                     # transforma descCategory em coluna do resultado
          .rename(columns={"idOrder":"qtIdOrder"}))          # renomeia o IdOrder para qtIdOrder

columns = ["descCategoryName"]

df_new.droplevel(0, axis=1) #dropa o multilevel

# COMMAND ----------

df_result = (df_order_items.merge(df_produto, how="left", on="idProduct")
                          .merge (df_sellers, how = "left", on= "idSeller", )
                          .rename(columns = {"desCity": "desCitySeller", 
                                             "descState": "descStateSeller", 
                                             "nrZipPrefi": "nrZipPrefixSeller" }))


df_new = (df_result.groupby(by=["descCategoryName"])[['idOrder', 'idSeller']]  # Agrupa por descCategoryName, fazendo calculo em ['idOrder']
                  .agg( {"idOrder": ['count'],
                         "idSeller": 'nunique'})                               # Realiza a contagem de ['idOrder']
                  .reset_index())                    # Renomeia o idOrder para qtIdOrder
df_new

# COMMAND ----------


