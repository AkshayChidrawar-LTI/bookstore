# Databricks notebook source
# MAGIC %run ./000_Libraries

# COMMAND ----------

def create_sc(ws,sc_name,sc_arn):
    try:
        ws.storage_credentials.create(
            name=sc_name,
            aws_iam_role=AwsIamRole(role_arn=sc_arn)
            )
        print(f"\nStorage Credential '{sc_name}' assuming below role is created. \n'{sc_arn}'")
    except Exception as e:
        print(f"\nError occured while creating Storage Credential '{sc_name}' : \n{e}")

def create_el(ws,el_name,el_loc,el_sc):
    try:
        ws.external_locations.create(
            name=el_name
            ,url=el_loc
            ,credential_name=el_sc
            )
        print(f"\nExternal Location '{el_name}' pointing to below path is created. \n'{el_loc}'")
    except Exception as e:
        print(f"\nError occured while creating External Location '{el_name}' : \n{e}")

def create_ct(ct_name,ct_loc):
    try:
        spark.sql(f"create catalog {ct_name} managed location '{ct_loc}'")
        print(f"\nCatalog '{ct_name}' at below location is created. \n'{ct_loc}")
    except Exception as e:
        print(f"\nError occured while creating Catalog '{ct_name}' : \n{e}")

def create_db(db_name,db_loc):
    try:
        spark.sql(f"create database {db_name} managed location '{db_loc}'")
        print(f"\nDatabase '{db_name}' at below location is created. \n'{db_loc}'")
    except Exception as e:
        print(f"\nError occured while creating Database '{db_name}' : \n{e}")

def create_tbl(tbl_name,tbl_schema,tbl_loc):
    try:
        spark.sql(f"create table {tbl_name} {tbl_schema} LOCATION '{tbl_loc}'")
        print(f"\nTable '{tbl_name}' is created at below location and schema. \n'{tbl_loc}'\n{tbl_schema}'")
    except Exception as e:
        print(f"\nError occured while creating Table '{tbl_name}' : \n{e}")

def drop_el(ws,user_name,el_name: str = None):
    if el_name is not None:
        try:
            eloc = ws.external_locations.get(name=el_name)
            if eloc.owner == user_name:
                url = eloc.url
                ws.external_locations.delete(name=el_name)
                print(f"\nExternal Location '{el_name}' pointing to below path is dropped. \n'{url}'")
            else:
                print(f"\nOwner of External Location '{el_name}' is not '{user_name}'.")
        except Exception as e:
            print(f"\nError occured while dropping External Location '{el_name}' : \n{e}")
    else:
        elocs = ws.external_locations.list()
        for eloc in elocs:
            try:
                if eloc.owner == user_name:
                    el_name = eloc.name
                    url = eloc.url
                    ws.external_locations.delete(name=el_name)
                    print(f"\nExternal Location '{el_name}' pointing to below path is dropped. \n'{url}'")
            except Exception as e:
                print(f"\nError occured while dropping External Location '{el_name}' : \n{e}")

def drop_sc(ws,user_name,sc_name: str = None):
    if sc_name is not None:
        try:
            if sc.owner == user_name:
                sc = ws.storage_credentials.get(name=sc_name)
                arn = sc.aws_iam_role.role_arn
                ws.storage_credentials.delete(name=sc_name)
                print(f"\nStorage Credential '{sc_name}' assuming below role is dropped. \n'{arn}'")
            else:
                print(f"\nOwner of Storage Credential '{sc_name}' is not '{user_name}'.")
        except Exception as e:
            print(f"\nError occured while dropping Storage Credential '{sc_name}' : \n{e}")
    else:
        scs = ws.storage_credentials.list()
        for sc in scs:
            try:
                if sc.owner == user_name:
                    sc_name = sc.name
                    arn = sc.aws_iam_role.role_arn
                    ws.storage_credentials.delete(name=sc_name)
                    print(f"\nStorage Credential '{sc_name}' assuming below role is dropped. \n'{arn}'")
            except Exception as e:
                print(f"\nError occured while dropping Storage Credential '{sc_name}' : \n{e}")

#delete only specified catalog, not all of them.
def drop_ct(ws,user_name,ct_name):
    exists = any(ct_name == ct.name for ct in ws.catalogs.list()) #any returns True if at least 1 match.
    if exists:
        try:
            ct_desc = spark.sql(f"DESCRIBE CATALOG EXTENDED {ct_name}")
            ct_owner = ct_desc.filter(ct_desc['info_name'] == 'Owner').select('info_value').collect()[0][0]
            if ct_owner == user_name:
                ct_loc = ct_desc.filter(ct_desc['info_name'] == 'Storage Root').select('info_value').collect()[0][0]
                spark.sql(f"DROP Catalog {ct_name} CASCADE")
                print(f"\nCatalog '{ct_name}' from below location is dropped. \n'{ct_loc}'")
            else:
                print(f"\nOwner of Catalog '{ct_name}' is not '{user_name}'.")
        except Exception as e:
            print(f"\nError occured while dropping Catalog '{ct_name}' : \n{e}")
    else:
        print(f"\nCatalog '{ct_name}' does not exist. Thus, nothing to drop.")
        return

# delete only specified database, otherwise delete all databases (Note: Only those databases are accessible which are a part of the currently active catalog).
def drop_db(user_name,db_name: str = None):
    if db_name is not None:
        try:
            db_desc = spark.sql(f"DESCRIBE DATABASE EXTENDED {db_name}")
            db_owner = db_desc.filter(db_desc['database_description_item'] == 'Owner').select('database_description_value').collect()[0][0]
            if db_owner == user_name:
                db_loc = db_desc.filter(db_desc['database_description_item'] == 'RootLocation').select('database_description_value').collect()[0][0]
                spark.sql(f"DROP DATABASE {db_name} CASCADE")
                print(f"\nDatabase '{db_name}' from below location is dropped. \n'{db_loc}'")
            else:
                print(f"\nOwner of Database '{db_name}' is not '{user_name}'.")
        except Exception as e:
            print(f"\nError occured while dropping Database ' '{db_name}' : \n{e}")
    else:
        dbs = spark.sql("SHOW DATABASES").collect()
        for db in dbs:
            try:
                db_name = db.Database
                db_desc = spark.sql(f"DESCRIBE DATABASE EXTENDED {db_name}")

                if db_desc.Owner == user_name:
                    db_loc = db_desc.filter(db_desc['database_description_item'] == 'RootLocation').select('database_description_value').collect()[0][0]
                    spark.sql(f"DROP DATABASE {db_name} CASCADE")
                    print(f"\nDatabase '{db_name}' from below location is dropped. \n'{db_loc}'")
            except Exception as e:
                print(f"\nError occured while dropping Database ' '{db_name}' : \n{e}")

# delete only specified table, otherwise delete all table (Note: Only those tables are accessible which are a part of the currently active database).
def drop_tbl(user_name,tbl_name: str = None):
    if tbl_name is not None:
        try:
            tbl_desc = spark.sql(f"DESCRIBE TABLE EXTENDED {tbl_name}")
            tbl_owner = tbl_desc.filter(tbl_desc['col_name'] == 'Owner').select('data_type').collect()[0][0]   
            if tbl_owner == user_name:
                tbl_loc = tbl_desc.filter(tbl_desc['col_name'] == 'Location').select('data_type').collect()[0][0]
                spark.sql(f"DROP TABLE {tbl_name}")
                print(f"\nTable {tbl_name} ' from below location is dropped. \n{tbl_loc}")
        except Exception as e:
            print(f"\nError occured while dropping Table ' '{tbl_name}' : \n{e}")
    else:
        tbls = spark.sql("SHOW TABLES").collect()
        for tbl in tbls:
            try:
                tbl_name = tbl.tableName
                tbl_desc = spark.sql(f"DESCRIBE TABLE EXTENDED {tbl_name}")
                tbl_owner = tbl_desc.filter(tbl_desc['col_name'] == 'Owner').select('data_type').collect()[0][0]
                if tbl_owner == user_name:
                    tbl_loc = tbl_desc.filter(tbl_desc['col_name'] == 'Location').select('data_type').collect()[0][0]
                    spark.sql(f"DROP TABLE {tbl_name}")
                    print(f"\nTable '{tbl_name}' from below location is dropped. \n{tbl_loc}")
            except Exception as e:
                print(f"\nError occured while dropping Table '{tbl_name}' : \n{e}")


# COMMAND ----------

def ReadMasterFile(FileName_DWH):

    Entitys = spark.createDataFrame(pd.read_excel(FileName_DWH,sheet_name='Entitys'))
    InfoSchema = spark.createDataFrame(pd.read_excel(FileName_DWH,sheet_name='InfoSchema')).createOrReplaceTempView('InfoSchema_vw')
    hostname = Entitys\
        .select('Entity_Value')\
        .filter(F.col('Entity_Name') == 'hostname')\
        .collect()[0]['Entity_Value']

    workspace = WorkspaceClient(host=hostname)

    sc_el = Entitys\
        .select('Entity_Name','Entity_Value')\
        .filter((F.col('Entity_Type') == 'Bucket for rawdata') | (F.col('Entity_Type') == 'Bucket for databricks'))

    bucketfordata = Entitys\
        .select('Entity_Name')\
        .filter(F.col('Entity_Type') == 'Bucket for databricks')\
        .collect()[0]['Entity_Name']
    path = 's3://'+bucketfordata+'/'

    Objects = spark.sql(f"""
                    select distinct
                            'Catalog' as Object_Type
                            ,Catalog_Name as Object_Name
                            ,concat('{path}',Catalog_Name,'/') as Object_Location
                    from  InfoSchema_vw
                    union
                    select distinct
                            'Database' as Object_Type
                            ,concat(Catalog_Name,'.',Database_Name) as Object_Name
                            ,concat('{path}',Catalog_Name,'/',Database_Name,'/') as Object_Location
                    from  InfoSchema_vw
                    """)

    Tables = spark.sql(f"""
                    select  concat(Catalog_Name,'.',Database_Name,'.',Table_Name) as Table_Name
                            ,concat('{path}',Catalog_Name,'/',Database_Name,'/',Table_Name,'/') as Table_Location
                            ,concat(Column_Name,' ',Data_Type) as Column_Def
                    from    InfoSchema_vw
                    where   Table_Name is not null
                    """)\
                        .groupby('Table_Name','Table_Location')\
                        .agg(F.concat_ws(',',F.collect_list('Column_Def'))\
                        .alias('Table_Schema'))\
                        .withColumn('Table_Schema', F.concat(F.lit("("), F.col("Table_Schema"), F.lit(")")))

    catalog = Objects\
        .select('Object_Name')\
        .filter(F.col('Object_Type') == 'Catalog')\
        .collect()[0]['Object_Name']

    sc_el = sc_el.toPandas()
    Objects = Objects.toPandas()
    Tables = Tables.toPandas()

    return workspace,catalog,sc_el,Objects,Tables

def Create(workspace,sc_el,Objects,Tables):
    print(f"\n***Creating Storage Credentials and External Locations***\n")
    for _,row in sc_el.iterrows():
        bucket = row['Entity_Name']
        arn = row['Entity_Value']
        sc_name = 'sc_' + bucket
        sc_arn = arn
        create_sc(workspace,sc_name,sc_arn)
        el_name = 'el_' + bucket
        el_loc = 's3://'+ bucket + '/'
        el_sc = 'sc_' + bucket
        create_el(workspace,el_name,el_loc,el_sc)
    print(f"\n***Creating Catalog and Databases***\n")
    for _,row in Objects.iterrows():
        Object_Type = row['Object_Type']
        Object_Name = row['Object_Name']
        Object_Location = row['Object_Location']
        if Object_Type == 'Catalog':
            create_ct(Object_Name,Object_Location)
        elif Object_Type == 'Database':
            create_db(Object_Name,Object_Location)
    print(f"\n***Creating Tables***\n")
    for _,row in Tables.iterrows():
        Table_Name = row['Table_Name']
        Table_Schema = row['Table_Schema']
        Table_Location = row['Table_Location']
        create_tbl(Table_Name,Table_Schema,Table_Location)

def Cleanup(workspace,user_name,catalog):
    try:
        print(f"\nDrop catalog '{catalog}' if already exists (must be owned by '{user_name}')")
        drop_ct(workspace,user_name,catalog)
        print(f"\nDrop existing External locations (if any) owned by '{user_name}'")
        drop_el(workspace,user_name)
        print(f"\nDrop existing Storage Credentials (if any) owned by '{user_name}'")
        drop_sc(workspace,user_name)
    except Exception as e:
        print(f"\nOperation faced below Exception(s) : {e}")
        raise

def Setup(FileName_DWH):
    user_name = spark.sql(f"SELECT current_user()").collect()[0][0]
    workspace,catalog,sc_el,Objects,Tables = ReadMasterFile(FileName_DWH)
    Cleanup(workspace,user_name,catalog)
    print(f"Creating Storage Credentials, External Locations")
    Create(workspace,sc_el,Objects,Tables)


# COMMAND ----------

dbutils.notebook.exit('')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exit

# COMMAND ----------


def drop_ct(ct_name: str = None):
    if ct_name is not None:
        try:
            ct_desc = spark.sql(f"DESCRIBE CATALOG EXTENDED {ct_name}").collect()
            ct_loc = next((row.value for row in ct_desc if row.col_name == 'Managed Location'),'NA')
            spark.sql(f"DROP Catalog {ct_name} CASCADE")
            print(f"\nCatalog '{ct_name}' from below location is dropped. \n'{ct_loc}'")
        except Exception as e:
            print(f"Error occured while dropping Catalog ' '{ct_name}' : \n{e}")
    else:
        cts = spark.sql("SHOW CATALOGS").collect()
        for ct in cts:
            try:
                ct_name = ct.Catalog
                ct_desc = spark.sql(f"DESCRIBE CATALOG EXTENDED {ct_name}").collect()
                ct_loc = next((row.value for row in ct_desc if row.col_name == 'Managed Location'),'NA')
                spark.sql(f"DROP Catalog {ct_name} CASCADE")
                print(f"\nCatalog '{ct_name}' from below location is dropped. \n'{ct_loc}'")
            except Exception as e:
                print(f"Error occured while dropping Catalog ' '{ct_name}' : \n{e}")

Tables_Schema = pd.DataFrame()
    Tables_Schema['Qlfd_TableName'] = Tables['Catalog_Name']+'.'+Tables['Database_Name']+'.'+Tables['Table_Name']
    Tables_Schema['Column_Def'] = Tables['Column_Name']+' '+Tables['Data_Type']    

    Tables_Schema = Tables_Schema\
    .groupby('Qlfd_TableName')['Column_Def']\
    .agg(lambda x: '(' + ','.join(x) + ')')\
    .reset_index()

    for _,row in Tables_Schema.iterrows():
        Qlfd_TableName = row['Qlfd_TableName']
        Column_Schema = row['Column_Schema']
        create_tbl(Qlfd_TableName,Column_Schema)

def CreateEntitys(ws,scs,els,Objects,Tables):
    for index, row in Entitys.iterrows():
        Object_Type = row['Entity_Type']
        Entity_Name = row['Object_Name']
        Object_Value= row['Object_Value']
        if (Entity_Type == 'Storage Credential'):
            sc_name = 'sc_' + Object_Name
            sc_arn = Object_Value
            create_sc(ws,sc_name,sc_arn)
        elif (Object_Type == 'External Location'):
            el_name = 'el_' + Entity_Name
            el_loc = Object_Value
            el_sc = 'sc_' + urlparse(el_loc).netloc.split('-')[1] 
            #Entity_Value.split('//')[1].split('-')[1].strip('/')
            create_el(ws,el_name,el_loc,el_sc)

def Create_Objects():    
    
    for _,row in Ct_Db.iterrows():
        Object_Type = row['Object_Type']
        Object_Name = row['Object_Name']
        Object_Value= row['Object_Value']
        if (Object_Type == 'Storage Credential'):
            sc_name = 'sc_' + Object_Name
            sc_arn = Object_Value
            create_sc(ws,sc_name,sc_arn)
        elif (Object_Type == 'External Location'):
            el_name = 'el_' + Object_Name
            el_loc = Object_Value
        create_tbl(Qlfd_TableName,Column_Schema)



    for _,row in Tables_Schema.iterrows():
        Qlfd_TableName = row['Qlfd_TableName']
        Column_Schema = row['Column_Schema']
        create_tbl(Qlfd_TableName,Column_Schema)

    print(f"\nTables created based on below schema: \n")
    display(Tables_Schema)