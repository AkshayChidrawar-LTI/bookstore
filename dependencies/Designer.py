# Databricks notebook source
# MAGIC %run ./Helper

# COMMAND ----------

class Initializer:
    def __init__(
        self
        ,repository_file: str
        ,metadata_file: str
        ,logger_for_initializer: Logger
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
    
    def create_ObjectsTracker(self):
        logger = self.logger_for_initializer
        fc = read_yaml(self.dict_meta['objects_tracker'])
        table_name = fc['catalog']+'.'+fc['database']+'.'+fc['table']
        table_schema = yaml_to_schema(fc['columns'])
        create_ObjectsTracker(table_name,table_schema)
        try:
            if action == 'CREATE':
                spark.sql(f"CREATE TABLE {table_name} {table_schema}")
            elif action == 'DROP':
                spark.sql(f"DROP TABLE {table_name}")
            logger.info(f"Success: Objects Tracker created.")
        except Exception as e:
            logger.error(f"Failure: Error occured while creating Objects Tracker: \n{e}")
        

# COMMAND ----------

class ScriptManager:
    def __init__(
        self
        ,dict_repo: dict
        ,dict_meta: dict
        ,logger_for_scripting: Logger
        ,logger_for_setup: Logger
        ,logger_for_cleanup: Logger
    ):
        self.dict_repo  = dict_repo
        self.dict_meta  = dict_meta
        self.logger_for_scripting = logger_for_scripting
        self.logger_for_execution = logger_for_execution

    self.workspace  = get_Workspace(host=self.dict_meta['host'],token=self.dict_meta['token'])
    
    def GenerateScripts(self):
        logger      = self.logger_for_scripting
        workspace   = self.workspace

        path_ddl    = self.dict_repo['path_ddl']
        path_schema = self.dict_repo['path_schema']

        buckets     = self.dict_meta['buckets']
        dwh_structure=self.dict_meta['dwh_structure']
        table_schemas=self.dict_meta['table_schemas']

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
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,logger,workspace=workspace,aws_iam_role=aws_iam_role)
            # self.objects_tracker = appendTo_DF(self.objects_tracker,{'object_type':object_type,'object_name':object_name,'object_ddl_CREATE':object_ddl_CREATE,'object_ddl_DROP':object_ddl_DROP})

            object_type = 'external_location'
            object_name = 'el_' + bucket_name
            object_ddl_CREATE = get_path(path_ddl_external_locations,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_external_locations,object_name+'_DROP'+'.py')
            el_loc = get_bucketlink(bucket_name)
            el_sc = 'sc_'+bucket_name
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,logger,workspace=workspace,el_loc=el_loc,el_sc=el_sc)
            # self.objects_tracker = appendTo_DF(self.objects_tracker,{'object_type':object_type,'object_name':object_name,'object_ddl_CREATE':object_ddl_CREATE,'object_ddl_DROP':object_ddl_DROP})

        for catalog in dwh_structure['catalogs']:
            object_type = 'CATALOG'
            object_name = catalog['name']
            object_ddl_CREATE = get_path(path_ddl_catalogs,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_catalogs,object_name+'_DROP'+'.py')
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,logger)
            # self.objects_tracker = appendTo_DF(self.objects_tracker,{'object_type':object_type,'object_name':object_name,'object_ddl_CREATE':object_ddl_CREATE,'object_ddl_DROP':object_ddl_DROP})
            
            for database in catalog['databases']:
                object_type = 'DATABASE'
                object_name = catalog['name']+'.'+database['name']
                object_ddl_CREATE = get_path(path_ddl_databases,get_fname(object_name)+'_CREATE'+'.py')
                object_ddl_DROP = get_path(path_ddl_databases,get_fname(object_name)+'_DROP'+'.py')
                GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,logger)
                # self.objects_tracker = appendTo_DF(self.objects_tracker,{'object_type':object_type,'object_name':object_name,'object_ddl_CREATE':object_ddl_CREATE,'object_ddl_DROP':object_ddl_DROP})

        for item in table_schemas:
            fc = read_yaml(get_path(path_schema,item['name']))
            object_type = 'TABLE'
            object_name = fc['catalog']+'.'+fc['database']+'.'+fc['table']
            object_ddl_CREATE = get_path(path_ddl_tables,get_fname(object_name)+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_tables,get_fname(object_name)+'_DROP'+'.py')
            table_schema = yaml_to_schema(fc['columns'])
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,logger,table_schema=table_schema)
            # self.objects_tracker = appendTo_DF(self.objects_tracker,{'object_type':object_type,'object_name':object_name,'object_ddl_CREATE':object_ddl_CREATE,'object_ddl_DROP':object_ddl_DROP})

        print(f"\nEND OF GENERATING SCRIPTS (CREATE & DROP). Refer below log file for more details.\n'{logger.handlers[0].baseFilename}'")
    
    def Setup(self):
        logger = self.logger_for_setup        
        for _,item in self.objects_tracker.iterrows():
            execute_script(item['object_ddl_CREATE'],logger)
        print(f"\nEND OF SETUP. Refer below log file for more details.\n'{logger.handlers[0].baseFilename}'")

    def Cleanup(self):
        logger = self.logger_for_cleanup
        path_log = self.dict_repo['path_log']
        
        sortedTracker = self.objects_tracker.iloc[::-1]
        for _,item in sortedTracker.iterrows():
            object_ddl_CREATE = item['object_ddl_CREATE']
            object_ddl_DROP = item['object_ddl_DROP']
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

        self.Initializer= Initializer(repository_file,metadata_file,log_file)
        self.dict_repo  = self.Initializer.get_Repository()
        self.dict_meta  = self.Initializer.get_Metadata()
        self.workspace  = self.Initializer.workspace
        
        self.logger_for_initializer = get_Logger(get_path(self.dict_repo['path_log'],'initializer.log'),'a')
        self.logger_for_scripting = get_Logger(get_path(self.dict_repo['path_log'],'scripting.log'),'a')
        self.logger_for_setup = get_Logger(get_path(self.dict_repo['path_log'],'setup.log'),'a')
        self.logger_for_cleanup = get_Logger(get_path(self.dict_repo['path_log'],'cleanup.log'),'a')
        
        self.ScriptManager  = ScriptManager(self.dict_repo,self.dict_meta,self.logger_for_scripting,self.logger_for_setup,self.logger_for_cleanup)
        self.GenerateScripts= self.ScriptManager.GenerateScripts
        self.Setup          = self.ScriptManager.Setup
        self.Cleanup        = self.ScriptManager.Cleanup
    
    
