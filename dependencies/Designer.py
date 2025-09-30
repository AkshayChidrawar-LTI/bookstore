# Databricks notebook source
# MAGIC %run ./Helper

# COMMAND ----------

class Initializer:
    def __init__(
        self
        ,repository_file: str
        ,metadata_file: str
        ,logger_for_initializer: logging.Logger
    ):
        self.repository_file = repository_file
        self.metadata_file = metadata_file
        self.logger_for_initializer = logger_for_initializer

    def get_Repository(self)-> dict:
        repository = read_yaml(self.repository_file)
        home = get_path(repository['project']['workspacehome'],repository['project']['reponame'])
        dict_repo = {
            "path_ddl": get_path(home,repository['path']['ddl'])
            ,"path_schema": get_path(home,repository['path']['schema'])
            ,"path_log": get_path(home,repository['path']['log'])
        }
        return dict_repo

    def get_Metadata(self)->dict:
        metadata = read_yaml(self.metadata_file)
        dict_meta = {
            "host":metadata['project']['host']
            ,"token":metadata['project']['token']
            ,"buckets":metadata['buckets']
            ,"dwh_structure":metadata['dwh_structure']
            ,"table_schemas": metadata['table_schemas']
            ,"objects_tracker": metadata['objects_tracker']
        }
        return dict_meta

    def manage_ObjectsTracker(self,DDLType,**kwargs):
        object_type = kwargs.get('object_type','')
        object_name = kwargs.get('object_name','')
        object_ddl_CREATE = kwargs.get('object_ddl_CREATE','')
        object_ddl_DROP = kwargs.get('object_ddl_DROP','')
        object_status= kwargs.get('object_status','')
        script_status= kwargs.get('script_status','')

        fc = read_yaml(self.dict_meta['objects_tracker'])
        table_name = fc['catalog']+'.'+fc['database']+'.'+fc['table']

        if DDLType == 'CREATE_objects_tracker':
            table_schema = yaml_to_schema(fc['columns'])
            exec(script_for_create_cdt('TABLE',table_name,table_schema))
        elif DDLType == 'APPEND_object':
            spark.sql(f"""
                      INSERT INTO {table_name}
                      VALUES ('{object_type}','{object_name}','{object_ddl_CREATE}','{object_ddl_DROP}',NULL,NULL)
                      """)
        elif DDLType == 'UPDATE_script_status_GENERATED':
            spark.sql(f"""
                      UPDATE {table_name}
                      SET script_status = 'GENERATED'
                      WHERE object_name = '{object_name}'
                      """)
        elif DDLType == 'UPDATE_object_status_CREATED':
            spark.sql(f"""
                      UPDATE {table_name}
                      SET object_status = 'CREATED'
                      WHERE object_name = '{object_name}'
                      """)
        elif DDLType == 'UPDATE_object_status_DROPPED':
            spark.sql(f"""
                      UPDATE {table_name}
                      SET object_status = 'DROPPED'
                      WHERE object_name = '{object_name}'
                      """)
        elif DDLType == 'UPDATE_script_status_REMOVED':
            spark.sql(f"""
                      UPDATE {table_name}
                      SET script_status = 'REMOVED'
                      WHERE object_name = '{object_name}'
                      """)
        elif DDLType == 'DROP_objects_tracker':
            exec(script_for_drop_cdt('TABLE',table_name))
        elif DDLType == '':
            return table_name

    def Initialize(self):
        logger = self.logger_for_initializer
        try:
            dict_repo = self.get_Repository()
            logger.info(f"Success: Repository:\n{dict_repo}")
            dict_meta = self.get_Metadata()
            logger.info(f"Success: Metadata:\n{dict_meta}")
            table_name = self.manage_ObjectsTracker('CREATE_objects_tracker')
            logger.info(f"Success: CREATE ObjectsTracker: {table_name}")
            logger.info("Success: Initializer executed successfully.")
        except Exception as e:
            logger.error(f"Failure: Initializer failed at {e}")

# COMMAND ----------

class ScriptGenerator:
    def __init__(
        self
        ,Initializer: Initializer
        ,logger_for_scripting: logging.Logger
    ):
        self.Initializer = Initializer
        self.logger_for_scripting = logger_for_scripting

    def GenerateScripts(self):
        logger     = self.logger_for_scripting
        dict_repo  = self.Initializer.get_Repository()
        dict_meta  = self.Initializer.get_Metadata()
        manage_ObjectsTracker = self.Initializer.manage_ObjectsTracker()

        workspace  = get_Workspace(host=dict_meta['host'],token=dict_meta['token'])

        path_ddl    = dict_repo['path_ddl']
        path_schema = dict_repo['path_schema']

        buckets     = dict_meta['buckets']
        dwh_structure=dict_meta['dwh_structure']
        table_schemas=dict_meta['table_schemas']

        path_ddl_storage_credentials= get_path(path_ddl,'buckets/storage_credentials')
        path_ddl_external_locations = get_path(path_ddl,'buckets/external_locations')
        path_ddl_catalogs           = get_path(path_ddl,'catalogs')
        path_ddl_databases          = get_path(path_ddl,'databases')
        path_ddl_tables             = get_path(path_ddl,'tables')

        for bucket in buckets:
            bucket_name = bucket['name']
            aws_iam_role = AwsIamRole(role_arn = bucket['iam_role'])

            object_type = 'storage_credential'
            object_name = 'sc_'+bucket_name
            object_ddl_CREATE = get_path(path_ddl_storage_credentials,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_storage_credentials,object_name+'_DROP'+'.py')
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,logger,aws_iam_role=aws_iam_role)
            manage_ObjectsTracker('APPEND_object',object_type=object_type,object_name=object_name,object_ddl_CREATE=object_ddl_CREATE,object_ddl_DROP=object_ddl_DROP,object_status=NULL,script_status=NULL)
            manage_ObjectsTracker('UPDATE_script_status_GENERATED',object_name=object_name)

            object_type = 'external_location'
            object_name = 'el_' + bucket_name
            object_ddl_CREATE = get_path(path_ddl_external_locations,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_external_locations,object_name+'_DROP'+'.py')
            el_loc = get_bucketlink(bucket_name)
            el_sc = 'sc_'+bucket_name
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,logger,workspace=workspace,el_loc=el_loc,el_sc=el_sc)
            manage_ObjectsTracker('APPEND_object',object_type=object_type,object_name=object_name,object_ddl_CREATE=object_ddl_CREATE,object_ddl_DROP=object_ddl_DROP,object_status=NULL,script_status=NULL)
            manage_ObjectsTracker('UPDATE_script_status_GENERATED',object_name=object_name)

        for catalog in dwh_structure['catalogs']:
            object_type = 'CATALOG'
            object_name = catalog['name']
            object_ddl_CREATE = get_path(path_ddl_catalogs,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_catalogs,object_name+'_DROP'+'.py')
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,logger)
            manage_ObjectsTracker('APPEND_object',object_type=object_type,object_name=object_name,object_ddl_CREATE=object_ddl_CREATE,object_ddl_DROP=object_ddl_DROP,object_status=NULL,script_status=NULL)
            manage_ObjectsTracker('UPDATE_script_status_GENERATED',object_name=object_name)

            for database in catalog['databases']:
                object_type = 'DATABASE'
                object_name = catalog['name']+'.'+database['name']
                object_ddl_CREATE = get_path(path_ddl_databases,get_fname(object_name)+'_CREATE'+'.py')
                object_ddl_DROP = get_path(path_ddl_databases,get_fname(object_name)+'_DROP'+'.py')
                GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,logger)
                manage_ObjectsTracker('APPEND_object',object_type=object_type,object_name=object_name,object_ddl_CREATE=object_ddl_CREATE,object_ddl_DROP=object_ddl_DROP,object_status=NULL,script_status=NULL)
                manage_ObjectsTracker('UPDATE_script_status_GENERATED',object_name=object_name)

        for item in table_schemas:
            fc = read_yaml(get_path(path_schema,item['name']))
            object_type = 'TABLE'
            object_name = fc['catalog']+'.'+fc['database']+'.'+fc['table']
            object_ddl_CREATE = get_path(path_ddl_tables,get_fname(object_name)+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_tables,get_fname(object_name)+'_DROP'+'.py')
            table_schema = yaml_to_schema(fc['columns'])
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,logger,table_schema=table_schema)
            manage_ObjectsTracker('APPEND_object',object_type=object_type,object_name=object_name,object_ddl_CREATE=object_ddl_CREATE,object_ddl_DROP=object_ddl_DROP,object_status=NULL,script_status=NULL)
            manage_ObjectsTracker('UPDATE_script_status_GENERATED',object_name=object_name)

        print(f"\nEND OF GENERATING SCRIPTS (CREATE & DROP). Refer below log file for more details.\n'{logger.handlers[0].baseFilename}'")


# COMMAND ----------

class ScriptExecutor:
    def __init__(
        self
        ,manage_ObjectsTracker: Callable
        ,logger_for_setup: logging.Logger
        ,logger_for_cleanup: logging.Logger
    ):
        self.manage_ObjectsTracker = manage_ObjectsTracker
        self.logger_for_setup = logger_for_setup
        self.logger_for_cleanup = logger_for_cleanup

    def Setup(self):
        logger = self.logger_for_setup
        df = spark.table(self.tbl_objects_tracker)
        for item in df.collect():
            object_ddl_CREATE = item['object_ddl_CREATE']
            execute_script(object_ddl_CREATE,logger,{'workspace':self.workspace})
            self.manage_ObjectsTracker('UPDATE_object_status_CREATED',object_ddl_CREATE=object_ddl_CREATE)
        print(f"\nEND OF SETUP. Refer below log file for more details.\n'{logger.handlers[0].baseFilename}'")

    def Cleanup(self):
        logger = self.logger_for_cleanup
        path_log = self.dict_repo['path_log']

        objects = spark.table(self.tbl_objects_tracker).collect()[::-1]
        for object in objects:
            object_ddl_CREATE = object['object_ddl_CREATE']
            object_ddl_DROP = object['object_ddl_DROP']
            execute_script(object_ddl=object_ddl_DROP,logger=logger)
            remove_script(object_ddl=object_ddl_CREATE,logger=logger)
            remove_script(object_ddl=object_ddl_DROP,logger=logger)
        for log_file in path_log:
            remove_script(object_ddl=log_file,logger=logger)
        remove_loggers()
        print(f"\nEND OF CLEANUP. Refer below log file for more details.\n'{logger.handlers[0].baseFilename}'")

# COMMAND ----------

# DBTITLE 1,Class
class ProjectSetup:
    def __init__(
        self
        ,ProjectName: str
        ,repository_file: str
        ,metadata_file: str
    ):
        self.ProjectName= ProjectName

        self.logger_for_initializer = get_Logger(get_path(self.dict_repo['path_log'],'initializer.log'),'a')
        self.logger_for_scripting = get_Logger(get_path(self.dict_repo['path_log'],'scripting.log'),'a')
        self.logger_for_setup = get_Logger(get_path(self.dict_repo['path_log'],'setup.log'),'a')
        self.logger_for_cleanup = get_Logger(get_path(self.dict_repo['path_log'],'cleanup.log'),'a')

        self.Initializer    = Initializer(repository_file,metadata_file,self.logger_for_initializer)
        self.ScriptGenerator= ScriptGenerator(self.Initializer,self.logger_for_scripting,self.logger_for_setup,self.logger_for_cleanup)
        self.GenerateScripts= self.ScriptGenerator.GenerateScripts
        self.Setup          = self.ScriptGenerator.Setup
        self.Cleanup        = self.ScriptGenerator.Cleanup
