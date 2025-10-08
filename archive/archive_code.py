
def appendTo_DF(df,new_row)->pd.DataFrame:
    df = pd.concat([df,pd.DataFrame([new_row])],ignore_index=True)
    return df

def get_user():
    return spark.sql("SELECT current_user()").collect()[0][0]

    ddl = f"\nCREATE {otype.upper()} {oname}"
    if schema:
        ddl += f"\n{schema}"
    ddl += f"\nMANAGED LOCATION '{oloc}';\n"
    print(f"\nBelow script is generated in the repository to create {otype} '{oname}'. \n{oscript}")
    save_script(oscript,ddl)


def generate_py_sc_el(path_ddl,hostname,bucket_name,bucket_arn):
    sc_name = 'sc_' + bucket_name
    sc_arn = bucket_arn
    sc_bucket_script = os.path.join(path_ddl, f'sc_{bucket_name}.py')
    el_name = 'el_' + bucket_name
    el_loc = 's3://'+ bucket_name + '/'
    el_bucket_script = os.path.join(path_ddl, f'el_{bucket_name}.py')

    sc_py = f"""
ws = WorkspaceClient(host='{hostname}')
try:
    ws.storage_credentials.create(
        name='{sc_name}',
        aws_iam_role=AwsIamRole(role_arn='{sc_arn}')
        )
    print(f"\\nStorage Credential '{sc_name}' assuming below role is created. \\n'{sc_arn}'")

except Exception as e:
    print(f"\\nException occured while creating Storage Credential for bucket '{bucket_name}': \\n{{e}}")
"""
    print(f"\nBelow script is generated in the repository to create a storage crendetial '{sc_name}' for the bucket '{bucket_name}'. \n'{sc_bucket_script}' ")
    save_script(sc_bucket_script,sc_py)

    el_py = f"""
ws = WorkspaceClient(host='{hostname}')
try:
    ws.external_locations.create(
        name='{el_name}'
        ,url='{el_loc}'
        ,credential_name={sc_name}
        )
    print(f"\\nExternal Location '{el_name}' pointing to below path is created. \\n'{el_loc}'")

except Exception as e:
    print(f"\\nException occured while creating External Location '{el_name}' for bucket '{bucket_name}': \\n{{e}}")
"""
    print(f"\nBelow script is generated in the repository to create an external location '{el_name}' for the bucket '{bucket_name}'. \n'{el_bucket_script}' ")
    save_script(el_bucket_script,el_py)

def drop_ct(ws,current_user):
    cts = spark.sql("SHOW CATALOGS").collect()
    for ct in cts:
        try:
            ct_name = ct.catalog
            ct_desc = spark.sql(f"DESCRIBE CATALOG EXTENDED {ct_name}")
            ct_owner = ct_desc.filter(ct_desc['info_name'] == 'Owner').select('info_value').collect()[0][0]
            if ct_owner == current_user:
                ct_loc = ct_desc.filter(ct_desc['info_name'] == 'Storage Root').select('info_value').collect()[0][0]
                spark.sql(f"DROP CATALOG {ct_name} CASCADE")
                print(f"\nCatalog '{ct_name}' is dropped from below location. \n'{ct_loc}'")
        except Exception as e:
            print(f"\nException occured while dropping Catalog '{ct_name}' : \n{e}")

def drop_el(ws,current_user):
    elocs = ws.external_locations.list()
    for eloc in elocs:
        try:
            if eloc.owner == current_user:
                el_name = eloc.name
                url = eloc.url
                ws.external_locations.delete(name=el_name)
                print(f"\nExternal Location '{el_name}' pointing to below path is dropped. \n'{url}'")
        except Exception as e:
            print(f"\nException occured while dropping External Location '{el_name}' : \n{e}")

def drop_sc(ws,current_user):
    scs = ws.storage_credentials.list()
    for sc in scs:
        try:
            if sc.owner == current_user:
                sc_name = sc.name
                ws.storage_credentials.delete(name=sc_name)
                print(f"\nStorage Credential '{sc_name}' is dropped.")
        except Exception as e:
            print(f"\nException occured while dropping Storage Credential '{sc_name}' : \n{e}")

if otype == 'TABLE':
        attribute_name = 'col_name'
        attribute_value = 'data_type'
        cascade = f"" 
    elif otype == 'DATABASE':
        attribute_name = 'database_description_item'
        attribute_value = 'database_description_value'
        cascade = f"CASCADE" 
    elif otype == 'CATALOG':
        attribute_name = 'info_name'
        attribute_value = 'info_value'
        cascade = f"CASCADE"

 oowner = spark.sql(f"DESCRIBE {otype} EXTENDED {oname}").filter(f"{attribute_name} = 'Owner'").select(f"{attribute_value}").collect()[0][0]

 if oowner == current_user:
            spark.sql(f"{drop_script}")
            print(f"\n{otype} '{oname}' is dropped.")

def drop_el(el_name,ws,current_user):
    try:
        if ws.external_locations.get(name=el_name).owner == current_user:
            ws.external_locations.delete(name=el_name)
            print(f"\nExternal Location '{el_name}' is dropped.")
        else:
            raise
    except Exception as e:
        print(f"\nException occured while dropping External Location {el_name}' : \n{e}")

def drop_sc(sc_name,ws,current_user):
    try:
        if ws.storage_credentials.get(name=sc_name).owner == current_user:
            ws.storage_credentials.delete(name=sc_name)
            print(f"\nStorage Credential '{sc_name}' is dropped.")
        else:
            raise
    except Exception as e:
        print(f"\nException occured while dropping Storage Credential '{sc_name}' : \n{e}")


def remove_ddlscripts(path_ddl,list_of_directories):
    for folder in list_of_directories:
        folder_path = get_path(path_ddl,folder)
        for item in os.listdir(folder_path):
            item_path = get_path(folder_path,item)
            if item_path.endswith('.gitkeep'):
                continue
            elif item_path.endswith('.py'):
                print(f"\nBelow script is deleted: \n'{item_path}'")
                os.remove(item_path)
        print(f"\nCLEANUP COMPLETED: '{folder_path}'")

def drop_object(otype,oname):
    if otype in ['CATALOG','DATABASE']:
        cascade = 'CASCADE'
    elif otype in ['TABLE']:
        cascade = ''
    script = f"""
try:
    spark.sql(f"DROP {otype} {oname} {cascade}")
    print(f"\\n{otype} '{oname}' is dropped.")
except Exception as e:
    print(f"\\nException occured while dropping {otype} '{oname}' : \n{{e}}")
"""
    save_script(oddl,script)

def drop_el(ws,el_name):
    try:
        ws.external_locations.delete(name=el_name)
        print(f"\nExternal Location '{el_name}' is dropped.")
    except Exception as e:
        print(f"\nException occured while dropping External Location {el_name}' : \n{e}")

def drop_sc(ws,sc_name):
    try:
        ws.storage_credentials.delete(name=sc_name)
        print(f"\nStorage Credential '{sc_name}' is dropped.")
    except Exception as e:
        print(f"\nException occured while dropping Storage Credential '{sc_name}' : \n{e}")

def ExecuteScripts(self):
        path_ddl = self.dict_repo['path_ddl']
        order_of_script_execution = self.order_of_script_execution
        for folder in order_of_script_execution:
            folder_path = get_path(path_ddl,folder)
            for item in os.listdir(folder_path):
                item_path = get_path(folder_path,item)
                if item_path.endswith('.gitkeep'):
                    continue
                elif item_path.endswith('.py'):
                    exec(open(item_path).read())
            print(f"\nEXECUTION COMPLETED: '{folder_path}'")

self.order_of_script_execution = ['buckets/storage_credentials','buckets/external_locations','catalogs','databases','tables']
self.order_of_object_deletion = ['TABLE','DATABASE','CATALOG','external_location','storage_credential']

def CleanupObjects(self):
        workspace   = self.workspace
        path_ddl    = self.dict_repo['path_ddl']
        objects_tracker = self.objects_tracker
        list_of_directories = self.order_of_script_execution
        order_of_object_deletion = self.order_of_object_deletion

        sortedTracker = get_sortedTracker(objects_tracker,order_of_object_deletion)
        for _,item in sortedTracker.iterrows():
            object_type = item['object_type']
            object_name = item['object_name']
            object_ddl = item['object_ddl']
            if object_type in ['CATALOG','DATABASE','TABLE']:
                drop_object(object_type,object_name)
            if object_type in ['storage_credential']:
                drop_sc(workspace,object_name)
            if object_type in ['external_location']:
                drop_el(workspace,object_name)
            os.remove(object_ddl)
            print(f"\nBelow script is deleted: \n'{object_ddl}'")
        print(f"\nCLEANUP COMPLETED")

print(f"\nSCRIPT CREATION COMPETED: \n{path_ddl_catalogs}\n{path_ddl_databases}\n")
print(f"\nSCRIPT CREATION COMPETED: \n{path_ddl_tables}\n")
print(f"\nSCRIPT CREATION COMPETED: \n{path_ddl_storage_credentials}\n{path_ddl_external_locations}\n")

self.workspace = get_workspace(self.dict_meta['host'],self.dict_meta['token'])

if os.path.exists(sc_ddl):
                print(f"Below file already exists. Skipping save. \n'{sc_ddl}'")
            else:
if os.path.exists(el_ddl):
                print(f"Below file already exists. Skipping save. \n'{el_ddl}'")
        else:
if os.path.exists(catalog_ddl):
                print(f"Below file already exists. Skipping save. \n'{catalog_ddl}'")
        else:
if os.path.exists(database_ddl):
                    print(f"Below file already exists. Skipping save. \n'{database_ddl}'")
            else:
if os.path.exists(table_ddl):
                    print(f"Below file already exists. Skipping save. \n'{table_ddl}'")
            else:  


def save_script(file_name,script):
    with open(file_name, 'w') as file:
        file.write(script)

def generate_sc(sc_ddl,host,token,sc_name,bucket_arn,DDLtype):
    if DDLtype =='CREATE':
        script = f"""
ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.storage_credentials.create(
        name='{sc_name}',
        aws_iam_role=AwsIamRole(role_arn='{bucket_arn}')
        )
    print(f"\\nstorage credential '{sc_name}' is created.")
except Exception as e:
    print(f"\\nException occured while creating storage credential '{sc_name}': \\n{{e}}")
"""
    elif DDLtype =='DROP':
        script = f"""
ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.storage_credentials.delete(name=sc_name)
    print(f"\nStorage Credential '{sc_name}' is dropped.")
except Exception as e:
    print(f"\nException occured while dropping Storage Credential '{sc_name}' : \n{{e}}")
"""
    save_script(sc_ddl,script)

def generate_el(el_ddl,host,token,el_name,el_loc,sc_name,DDLtype):
    if DDLtype =='CREATE':
        script = f"""
ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.external_locations.create(
        name='{el_name}'
        ,url='{el_loc}'
        ,credential_name='{sc_name}'
        )
    print(f"\\nexternal location '{el_name}' is created.")
except Exception as e:
    print(f"\\nException occured while creating external location '{el_name}': \\n{{e}}")
"""
    elif DDLtype =='DROP':
        script = f"""
ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.external_locations.delete(name=el_name)
    print(f"\nExternal Location '{el_name}' is dropped.")
except Exception as e:
    print(f"\nException occured while dropping External Location {el_name}' : \n{{e}}")
"""
    save_script(el_ddl,script)

def generate_ddl(oddl,otype,oname,oloc,DDLtype,schema:str=None):
    if DDLtype =='CREATE':
        script_ddl_create = f"CREATE {otype} {oname}"
        if otype=='TABLE'and schema:
            script_ddl_schema = f"\n{schema}"
            script_ddl_location = f"\nLOCATION '{oloc}';\n"
        if otype in ['CATALOG','DATABASE']:
            script_ddl_schema = ''
            script_ddl_location = f"\nMANAGED LOCATION '{oloc}';\n"
        script_ddl = script_ddl_create+script_ddl_schema+script_ddl_location
        script = f"""
try:
    spark.sql(\"\"\"{script_ddl}\"\"\")
    print(f"\\n{otype} '{oname}' is created at below location. \\n'{oloc}'")
except Exception as e:
    print(f"\\nException occured while creating {otype} '{oname}' : \\n{{e}}")
"""
    elif DDLtype =='DROP':
        if otype in ['CATALOG','DATABASE']:
            cascade = 'CASCADE'
        elif otype in ['TABLE']:
            cascade = ''
        script_ddl = f"DROP {otype} {oname} {cascade}"
        script = f"""
try:
    spark.sql(\"\"\"{script_ddl}\"\"\")
    print(f"\\n{otype} '{oname}' is dropped.")
except Exception as e:
    print(f"\\nException occured while dropping {otype} '{oname}' : \n{{e}}")
"""
    save_script(oddl,script)

sc_name = 'sc_'+bucket_name
            sc_ddl = get_path(path_ddl_storage_credentials,sc_name+'_'+DDLtype+'.py')
            generate_sc(DDLtype,sc_ddl,host,token,sc_name,bucket_arn)
            print(f"\nBelow script generated to {DDLtype} storage crendetial '{sc_name}' for bucket '{bucket_name}'. \n'{sc_ddl}'")
            addto_Tracker(self,'storage_credential',sc_name,sc_ddl,{"bucket_name": bucket_name,"bucket_arn": bucket_arn})

            el_name = 'el_' + bucket_name
            el_loc = 's3://'+ bucket_name + '/'
            el_ddl = get_path(path_ddl_external_locations,el_name+'_'+DDLtype+'.py')
            generate_el(el_ddl,host,token,el_name,el_loc,sc_name,DDLtype)
            print(f"\nBelow script generated to {DDLtype} an external location '{el_name}' for bucket '{bucket_name}'. \n'{el_ddl}'")
            addto_Tracker(self,'external_location',el_name,el_ddl,{"points_to": el_loc,"storage_credential": sc_name})

        for catalog in dwh_structure['catalogs']:
            object_type = 'CATALOG'
            catalog_name = catalog['name']
            catalog_loc = get_path(root,catalog_name)
            catalog_ddl = get_path(path_ddl_catalogs,catalog_name+'_'+DDLtype+'.py')
            generate_ddl(catalog_ddl,object_type,catalog_name,catalog_loc,DDLtype)
            print(f"\nBelow script generated to {DDLtype} catalog '{catalog_name}'. \n'{catalog_ddl}'")
            addto_Tracker(self,object_type,catalog_name,catalog_ddl,{"location": catalog_loc})
            for database in catalog['databases']:
                object_type = 'DATABASE'
                database_name = catalog['name']+'.'+database['name']
                database_loc = get_path(root,get_sloc(database_name))
                database_ddl = get_path(path_ddl_databases,get_fname(database_name)+'_'+DDLtype+'.py')
                generate_ddl(database_ddl,object_type,database_name,database_loc,DDLtype)
                print(f"\nBelow script generated to {DDLtype} database '{database_name}'. \n'{database_ddl}'")
                addto_Tracker(self,object_type,database_name,database_ddl,{"location": database_loc})

        for tableschema in os.listdir(path_schema):
            if tableschema.endswith('.yml'):
                fc = read_yaml(get_path(path_schema,tableschema))
                object_type = 'TABLE'
                table_name = fc['catalog']+'.'+fc['database']+'.'+fc['table']
                table_loc = get_path(root,get_sloc(table_name))
                table_ddl = get_path(path_ddl_tables,get_fname(table_name)+'_'+DDLtype+'.py')
                table_schema = yaml_to_schema(fc)
                generate_ddl(table_ddl,object_type,table_name,table_loc,DDLtype,table_schema)
                print(f"\nBelow script generated to {DDLtype} table '{table_name}'. \n'{table_ddl}'")
                addto_Tracker(self,object_type,table_name,table_ddl,{"location": table_loc,"schema": table_schema})
        print(f"GENERATION COMPLETED: {DDLtype}")

    def ExecuteScripts(self,DDLtype):
        objects_tracker = self.objects_tracker
        order_of_objects = self.order_of_objects
        if DDLtype=='CREATE':
            sort_order = sorted(order_of_objects,reverse=True)
        elif DDLtype=='DROP':
            sort_order = order_of_objects
        sortedTracker = get_sortedTracker(objects_tracker,sort_order)
        sortedTracker = sortedTracker[sortedTracker['object_ddl'].str.contains(DDLtype)]
        for _,item in sortedTracker.iterrows():
            object_ddl = item['object_ddl']
            exec(open(object_ddl).read())
        print(f"\nEXECUTION COMPLETED: {DDLtype}")

def Generate_DROP_scripts(self):
        host            = self.dict_meta['host']
        token           = self.dict_meta['token']
        sortedTracker   = self.get_ObjectsTracker(AscFlag=True)
        for _,item in sortedTracker.iterrows():
            object_type = item['object_type']
            object_name = item['object_name']
            object_ddl  = item['object_ddl'].replace('_CREATE.py','_DROP.py')
            if object_type in ['storage_credential','external_location']:
                generate_DROP_scripts(object_type,object_name,object_ddl,host=host,token=token)
            elif object_type in ['CATALOG','DATABASE','TABLE']:
                generate_DROP_scripts(object_type,object_name,object_ddl)
        print(f"\nSCRIPT GENERATION COMPLETED: DROP")


    def generate_ReadStream_Feed(feed_batch,RecordSchema):
        readQuery = (
            spark.readStream
            .format('cloudFiles')
            .option('cloudFiles.format', 'json')
            .schema(RecordSchema)
            .load(feed_batch)
            .select(
                F.col('topic')
                ,F.col('key').cast('string')
                ,F.col('value').cast('string')
                ,(F.col('timestamp')/1000).cast('timestamp').alias('create_ts')
                ,F.input_file_name().alias('source_file')
                ,F.current_timestamp().alias('insert_ts')
                )
        )
        return readQuery

    def generate_WriteStream_FeedToTopicBronzeTbl(readQuery,topic_name,topic_ValueSchema,topic_BronzeTblName):
        writeToBronzeTbl = (
            readQuery
            .filter(F.col('topic')==topic_name)
            .withColumn('v',F.from_json(F.col('value'),topic_ValueSchema))
            .select('key','create_ts','source_file','insert_ts','v.*')
            .writeStream
            .option('mergeSchema',True)
            .trigger(availableNow=True)
            .table(topic_BronzeTblName)
        )
        writeToBronzeTbl.awaitTermination()
        print(f"'\nSource: ' {feed_batch} '\nSchema: ' {topic_ValueSchema} '\nTarget: '{topic_BronzeTblName}'\n'")

    def ingest_Stream_TopicToBronzeTbl():
        readQuery = generate_ReadStream_Feed(feed_batch,RecordSchema)
        for topic in list_of_topics:
            topic_ValueSchema = locals()[f'{topic}_ValueSchema']
            topic_BronzeTblName = f'ct_bookstore.db_bronze.tbl_{topic}'
            generate_WriteStream_FeedToTopicBronzeTbl(readQuery,topic,topic_ValueSchema,topic_BronzeTblName)

self.list_of_objects = ['TABLE','DATABASE','CATALOG','external_location','storage_credential']
self.list_of_databases = ['db_bronze','db_silver','db_gold']

def getCatSorted_DF(df,column_name,list_of_objects,AscFlag:bool=True)->pd.DataFrame:
    df = df.copy()
    df[column_name] = pd.Categorical(
        df[column_name]
        ,categories=list_of_objects
        ,ordered=True
        )
    df.sort_values([column_name],ascending=AscFlag,inplace=True)
    return df

    def get_ObjectsTracker(self,AscFlag):
        sorted_ObjectsTracker = getCatSorted_DF(self.objects_tracker,'object_type',self.list_of_objects,AscFlag)
        sorted_ObjectsTracker = getCatSorted_DF(self.objects_tracker,'object_name',self.list_of_databases,True)
        sorted_ObjectsTracker = sorted_ObjectsTracker.sort_values(by=['object_type','object_name'],kind='stable')
        return sorted_ObjectsTracker
    
def save_script(file_name,script):
    with open(file_name, 'w') as file:
        file.write(script)

def remove_script(file_name,logger):
    try:
        dbutils.fs.rm(file_name,recursive=True)
        logger.info(f"Success: Script removed: '{file_name}'")
    except Exception as e:
        logger.warning(f"Failure: Error occured while removing file '{file_name}:'\n{e}")

def execute_script(script_file,logger):
    with open(script_file) as f:
        code = f.read()
    exec(code, {'logger': logger})

def generate_CREATE_scripts(object_type,object_name,object_ddl,logger,**kwargs):
    host = kwargs.get('host','')
    token = kwargs.get('token','')
    bucket_arn = kwargs.get('bucket_arn','')
    el_loc = kwargs.get('el_loc','')
    el_sc = kwargs.get('el_sc','')
    table_loc = kwargs.get('table_loc','')
    table_schema = kwargs.get('table_schema','')

    if object_type == 'storage_credential':
        script = f"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import AwsIamRole

ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.storage_credentials.create(
        name='{object_name}',
        aws_iam_role=AwsIamRole(role_arn='{bucket_arn}')
        )
    logger.info(f"\\nSuccess: CREATE {object_type} '{object_name}'")
except Exception as e:
    logger.critical(f"\\nFailure: CREATE {object_type} '{object_name}': \\n{{e}}")
"""
    elif object_type == 'external_location':
        script = f"""
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.external_locations.create(
        name='{object_name}'
        ,url='{el_loc}'
        ,credential_name='{el_sc}'
        )
    logger.info(f"\\nSuccess: CREATE {object_type} '{object_name}': \\n'{el_loc}'")
except Exception as e:
    logger.critical(f"\\nFailure: CREATE {object_type} '{object_name}': \\n{{e}}")
"""
    elif object_type in ['CATALOG','DATABASE','TABLE']:
        script_ddl_create = f"CREATE {object_type} {object_name}"
        if object_type in ['CATALOG','DATABASE']:
            script_ddl_schema = ''
        elif object_type == 'TABLE':
            script_ddl_schema = f"\n{table_schema};\n"
        script_ddl = script_ddl_create+script_ddl_schema
        script = f"""
try:
    spark.sql(f\"\"\"{script_ddl}\"\"\")
    logger.info(f"\\nSuccess: CREATE {object_type} '{object_name}'")
except Exception as e:
    logger.error(f"\\nFailure: CREATE {object_type} '{object_name}': \\n{{e}}")
"""
    save_script(object_ddl,script)
    logger.info(f"\nBelow script generated to CREATE {object_type} '{object_name}'. \n'{object_ddl}'")

def generate_DROP_scripts(object_type,object_name,object_ddl,logger,**kwargs):
    host = kwargs.get('host')
    token = kwargs.get('token')

    if object_type == 'storage_credential':
        script = f"""
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.storage_credentials.delete(name='{object_name}')
    logger.info(f"\\nSuccess: DROP {object_type} '{object_name}'")
except Exception as e:
    logger.critical(f"\\nFailure: DROP {object_type} '{object_name}': \\n{{e}}")
"""
    elif object_type == 'external_location':
        script = f"""
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.external_locations.delete(name='{object_name}')
    logger.info(f"\\nSuccess: DROP {object_type} '{object_name}'")
except Exception as e:
    logger.critical(f"\\nFailure: DROP {object_type} '{object_name}': \\n{{e}}")
"""
    elif object_type in ['CATALOG','DATABASE','TABLE']:
        if object_type in ['CATALOG','DATABASE']:
            cascade = 'CASCADE'
        elif object_type in ['TABLE']:
            cascade = ''
        script_ddl = f"DROP {object_type} {object_name} {cascade}"
        script = f"""
try:
    spark.sql(f\"\"\"{script_ddl}\"\"\")
    logger.info(f"\\nSuccess: DROP {object_type} '{object_name}'")
except Exception as e:
    logger.error(f"\\nFailure: DROP {object_type} '{object_name}': \\n{{e}}")
"""
    save_script(object_ddl,script)
    logger.info(f"\nBelow script generated to DROP {object_type} '{object_name}'. \n'{object_ddl}'")

class ScriptManager():
    def __init__(
        self
        ,logger: str
        object_type: str
        ,object_name: str
        ,object_ddl: str
        ,**kwargs
    ):
        self.logger = logger

    def generate_CREATE_script(self):    
        object_type = self.object_type
        object_name = self.object_name
        object_ddl  = self.object_ddl
        workspace   = self.kwargs.get('workspace','')
        aws_iam_role= self.kwargs.get('aws_iam_role','')
        el_loc      = self.kwargs.get('el_loc','')
        el_sc       = self.kwargs.get('el_sc','')
        table_schema= self.kwargs.get('table_schema','')
            
        success_message = f"\\nSuccess: CREATE {object_type} '{object_name}'"
        failure_message = f"\\nFailure: CREATE {object_type} '{object_name}': \\n{{e}}"

        if object_type == 'storage_credential':
            script = script_for_create_sc(workspace,object_name,aws_iam_role)
        elif object_type == 'external_location':
            script = script_for_create_el(workspace,object_name,el_loc,el_sc)
        elif object_type in ['CATALOG','DATABASE','TABLE']:
            script = script_for_create_cdt(object_type,object_name,table_schema)
        script = wrapper_for_TryExcept(script,success_message,failure_message)

        return script

    def save_script(self):
        try:
            with open(self.object_ddl, 'w') as file:
                file.write(self.script)
                self.logger.info(f"Success: script saved: '{self.object_ddl}'")
        except Exception as e:
            self.logger.error(f"Failure: Error occured while saving script '{self.object_ddl}:'\n{e}")
    
    def execute_script(self):
        with open(script_file) as f:
            code = f.read()
        exec(code,{'logger':self.logger})

    def remove_script(self,script_file):
        try:
            dbutils.fs.rm(script_file,recursive=True)
            self.logger.info(f"Success: script removed: '{script_file}'")
        except Exception as e:
            self.logger.error(f"Failure: Error occured while removing script '{script_file}:'\n{e}")

        save_script(object_ddl,script)
        self.logger.info(f"\nBelow script generated to CREATE {object_type} '{object_name}'. \n'{object_ddl}'")

    def process_CREATE_script(self):
        

def generate_DROP_scripts(object_type,object_name,object_ddl,logger,**kwargs):
    host = kwargs.get('host')
    token = kwargs.get('token')

    if object_type == 'storage_credential':
        script = f"""
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.storage_credentials.delete(name='{object_name}')
    logger.info(f"\\nSuccess: DROP {object_type} '{object_name}'")
except Exception as e:
    logger.critical(f"\\nFailure: DROP {object_type} '{object_name}': \\n{{e}}")
"""
    elif object_type == 'external_location':
        script = f"""
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.external_locations.delete(name='{object_name}')
    logger.info(f"\\nSuccess: DROP {object_type} '{object_name}'")
except Exception as e:
    logger.critical(f"\\nFailure: DROP {object_type} '{object_name}': \\n{{e}}")
"""
    elif object_type in ['CATALOG','DATABASE','TABLE']:
        if object_type in ['CATALOG','DATABASE']:
            cascade = 'CASCADE'
        elif object_type in ['TABLE']:
            cascade = ''
        script_ddl = f"DROP {object_type} {object_name} {cascade}"
        script = f"""
try:
    spark.sql(f\"\"\"{script_ddl}\"\"\")
    logger.info(f"\\nSuccess: DROP {object_type} '{object_name}'")
except Exception as e:
    logger.error(f"\\nFailure: DROP {object_type} '{object_name}': \\n{{e}}")
"""
    save_script(object_ddl,script)
    logger.info(f"\nBelow script generated to DROP {object_type} '{object_name}'. \n'{object_ddl}'")

def wrapper_for_TryExcept(script,success_message,failure_message):
        script = f"""
try:
    {script}
    logger.info({success_message})
except Exception as e:
    logger.error({failure_message})
"""

    if object_type in func_map:
        create_func,drop_func = func_map[object_type]
        create_script = wrapper_for_TryExcept(create_func(),success_messages["CREATE"],failure_messages["CREATE"])
        drop_script   = wrapper_for_TryExcept(drop_func(),success_messages["DROP"],failure_messages["DROP"])
    else:
        raise ValueError(f"Unsupported object_type: {object_type}")
    return create_script,drop_script

@try_except("\nSuccess: {self.repository_file} \nRepository:\n {self.get_Repository()}"
                ,"Failure: Error occurred while reading file {self.repository_file}:\n{e}")

@try_except("\nSuccess: {self.metadata_file} \nMetadata:\n {self.get_Metadata()}"
                ,"Failure: Error occurred while reading file {self.metadata_file}:\n{e}")

 @try_except("Success: {DDLType} ObjectTracker: '{table_name}'"
                ,"Failure: Error occurred while {DDLType} ObjectTracker: '{table_name}':\n{e}")

# self.objects_tracker = appendTo_DF(self.objects_tracker,{'object_type':object_type,'object_name':object_name,'object_ddl_CREATE':object_ddl_CREATE,'object_ddl_DROP':object_ddl_DROP})
# self.objects_tracker = appendTo_DF(self.objects_tracker,{'object_type':object_type,'object_name':object_name,'object_ddl_CREATE':object_ddl_CREATE,'object_ddl_DROP':object_ddl_DROP})
# self.objects_tracker = appendTo_DF(self.objects_tracker,{'object_type':object_type,'object_name':object_name,'object_ddl_CREATE':object_ddl_CREATE,'object_ddl_DROP':object_ddl_DROP})
# self.objects_tracker = appendTo_DF(self.objects_tracker,{'object_type':object_type,'object_name':object_name,'object_ddl_CREATE':object_ddl_CREATE,'object_ddl_DROP':object_ddl_DROP})

    @try_except("\nSuccess: Metadata:\n {result})"
                ,"\nFailure: Error occurred while reading file '{metadata_file}' :\n{e}")

    @try_except("\nSuccess: Repository:\n {result}"
            ,"\nFailure: Error occurred while reading file '{repository_file}' :\n{e}")

%sql
CREATE TABLE IF NOT EXISTS workspace.default.bronze_to_silver_last_processed
(
  topic STRING,
  last_processed_ts TIMESTAMP
);

%sql
insert into workspace.default.bronze_to_silver_last_processed values 
('books',NULL)
,('customers',NULL)
,('orders',NULL);


def books_scdType2_upsert(spark):
  spark.sql(f"""
            MERGE INTO bookstore.silver.books dt
            USING (
              SELECT NULL AS merge_key, b.*
              FROM bookstore.bronze.books b
              WHERE b.insert_ts > (
                SELECT last_processed_ts
                FROM workspace.default.bronze_to_silver_last_processed
                WHERE topic = 'books'
                )lp
              UNION ALL
              SELECT b.book_id AS merge_key, b.*
              FROM bookstore.bronze.books b
              INNER JOIN bookstore.silver.books t
              ON t.book_id = b.book_id
              AND t.isActive = 'Y'
              AND t.start_date <> b.updated
            )st
            ON dt.book_id = st.merge_key

            WHEN MATCHED AND dt.isActive = 'Y' AND dt.start_date <> st.updated THEN
              UPDATE SET
              isActive = 'N',
              end_date = st.updated

            WHEN NOT MATCHED THEN
              INSERT (book_id, title, author, price, isActive, start_date, end_date)
              VALUES (st.book_id, st.title, st.author, st.price, 'Y', st.updated, NULL);
            
            #update tracker
            UPDATE workspace.default.bronze_to_silver_last_processed dt
            SET last_processed_ts = (
              SELECT max(insert_ts) AS max_insert_ts
              FROM bookstore.bronze.books
            )st
            WHERE topic = 'books'

            """)

  latest_version = spark.sql(f"""
                           select max(version) 
                           from (describe history bookstore.bronze.books) t
                           where operation = 'STREAMING UPDATE'
                           """).collect()[0][0]
  tbl_version = latest_version
  display(spark.sql(f"""
                    select * except (_change_type,_commit_version,_commit_timestamp) 
                    from table_changes('bookstore.bronze.books',{tbl_version},{tbl_version})
                    """))