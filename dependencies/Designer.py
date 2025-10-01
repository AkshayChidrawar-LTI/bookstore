# Databricks notebook source
# MAGIC %run ./Helper

# COMMAND ----------

class Initializer:
    def __init__(
        self
        ,repository_file: str
        ,metadata_file: str
    ):
        self.repository_file = repository_file
        self.metadata_file = metadata_file
        self.logger_for_initializer = get_Logger(get_path(self.get_Repository()['path_log'],'initializer.log'),'a')
        self.dict_repo = None
        self.dict_meta = None
        self.workspace = None
        self.tbl_ObjectsTracker = None

    def get_Repository(self)-> dict:
        repository = read_yaml(self.repository_file)
        home = get_path(repository['project']['workspacehome'],repository['project']['reponame'])
        return {
            "path_ddl": get_path(home,repository['path']['ddl'])
            ,"path_schema": get_path(home,repository['path']['schema'])
            ,"path_log": get_path(home,repository['path']['log'])
        }

    def get_Metadata(self)->dict:
        metadata = read_yaml(self.metadata_file)
        return {
            "host":metadata['project']['host']
            ,"token":metadata['project']['token']
            ,"buckets":metadata['buckets']
            ,"dwh_structure":metadata['dwh_structure']
            ,"table_schemas": metadata['table_schemas']
            ,"objects_tracker": metadata['objects_tracker']
        }

    def manage_ObjectsTracker(self,**kwargs):
        DDLType = kwargs.get('DDLType','')
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
        elif DDLType == 'UPDATE_script_status':
            spark.sql(f"""
                      UPDATE {table_name}
                      SET script_status = '{script_status}'
                      WHERE object_name = '{object_name}'OR object_ddl_DROP = '{object_ddl_DROP}'
                      """)
        elif DDLType == 'UPDATE_object_status':
            spark.sql(f"""
                      UPDATE {table_name}
                      SET object_status = '{object_status}'
                      WHERE object_ddl_CREATE = '{object_ddl_CREATE}' OR object_ddl_DROP = '{object_ddl_DROP}'
                      """)
        elif DDLType == 'DROP_objects_tracker':
            exec(script_for_drop_cdt(object_type='TABLE',object_name=table_name))
        elif DDLType == '':
            return table_name

    def Initialize(self):
        try:
            self.dict_repo = self.get_Repository()
            self.logger_for_initializer.info(f"Success: Repository:\n {json.dumps(self.dict_repo,indent=4)}")
            self.dict_meta = self.get_Metadata()
            self.logger_for_initializer.info(f"Success: Metadata:\n{json.dumps(self.dict_meta,indent=4)}")
            self.workspace = get_Workspace(host=self.dict_meta['host'],token=self.dict_meta['token'])
            self.logger_for_initializer.info(f"Success: Workspace object:\n{self.workspace}")
            self.tbl_ObjectsTracker = self.manage_ObjectsTracker(DDLType='CREATE_objects_tracker')
            self.logger_for_initializer.info(f"Success: CREATE ObjectsTracker: {self.tbl_ObjectsTracker}")
            self.logger_for_initializer.info("Success: Initializer executed successfully.")
        except Exception as e:
            self.logger_for_initializer.error(f"Failure: Initializer failed at {e}")



# COMMAND ----------

class ScriptGenerator:
    def __init__(
        self
        ,dict_repo: dict
        ,dict_meta: dict
        ,manage_ObjectsTracker: Callable
        ,logger_for_scripting: logging.Logger
    ):
        self.dict_repo = dict_repo
        self.dict_meta = dict_meta
        self.manage_ObjectsTracker = manage_ObjectsTracker
        self.logger_for_scripting = logger_for_scripting

    def GenerateScripts(self):
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
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,self.logger_for_scripting,aws_iam_role=aws_iam_role)
            self.manage_ObjectsTracker(DDLType='APPEND_object',object_type=object_type,object_name=object_name,object_ddl_CREATE=object_ddl_CREATE,object_ddl_DROP=object_ddl_DROP,object_status=NULL,script_status=NULL)
            self.manage_ObjectsTracker(DDLType='UPDATE_script_status',script_status='GENERATED',object_name=object_name)

            object_type = 'external_location'
            object_name = 'el_' + bucket_name
            object_ddl_CREATE = get_path(path_ddl_external_locations,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_external_locations,object_name+'_DROP'+'.py')
            el_loc = get_bucketlink(bucket_name)
            el_sc = 'sc_'+bucket_name
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,self.logger_for_scripting,workspace=workspace,el_loc=el_loc,el_sc=el_sc)
            self.manage_ObjectsTracker(DDLType='APPEND_object',object_type=object_type,object_name=object_name,object_ddl_CREATE=object_ddl_CREATE,object_ddl_DROP=object_ddl_DROP,object_status=NULL,script_status=NULL)
            self.manage_ObjectsTracker(DDLType='UPDATE_script_status',script_status='GENERATED',object_name=object_name)

        for catalog in dwh_structure['catalogs']:
            object_type = 'CATALOG'
            object_name = catalog['name']
            object_ddl_CREATE = get_path(path_ddl_catalogs,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_catalogs,object_name+'_DROP'+'.py')
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,self.logger_for_scripting)
            self.manage_ObjectsTracker(DDLType='APPEND_object',object_type=object_type,object_name=object_name,object_ddl_CREATE=object_ddl_CREATE,object_ddl_DROP=object_ddl_DROP,object_status=NULL,script_status=NULL)
            self.manage_ObjectsTracker(DDLType='UPDATE_script_status',script_status='GENERATED',object_name=object_name)

            for database in catalog['databases']:
                object_type = 'DATABASE'
                object_name = catalog['name']+'.'+database['name']
                object_ddl_CREATE = get_path(path_ddl_databases,get_fname(object_name)+'_CREATE'+'.py')
                object_ddl_DROP = get_path(path_ddl_databases,get_fname(object_name)+'_DROP'+'.py')
                GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,self.logger_for_scripting)
                self.manage_ObjectsTracker(DDLType='APPEND_object',object_type=object_type,object_name=object_name,object_ddl_CREATE=object_ddl_CREATE,object_ddl_DROP=object_ddl_DROP,object_status=NULL,script_status=NULL)
                self.manage_ObjectsTracker(DDLType='UPDATE_script_status',script_status='GENERATED',object_name=object_name)

        for item in table_schemas:
            fc = read_yaml(get_path(path_schema,item['name']))
            object_type = 'TABLE'
            object_name = fc['catalog']+'.'+fc['database']+'.'+fc['table']
            object_ddl_CREATE = get_path(path_ddl_tables,get_fname(object_name)+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_tables,get_fname(object_name)+'_DROP'+'.py')
            table_schema = yaml_to_schema(fc['columns'])
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,self.logger_for_scripting,table_schema=table_schema)
            self.manage_ObjectsTracker(DDLType='APPEND_object',object_type=object_type,object_name=object_name,object_ddl_CREATE=object_ddl_CREATE,object_ddl_DROP=object_ddl_DROP,object_status=NULL,script_status=NULL)
            self.manage_ObjectsTracker(DDLType='UPDATE_script_status',script_status='GENERATED',object_name=object_name)

        print(f"\nEND OF GENERATING SCRIPTS (CREATE & DROP). Refer below log file for more details.\n'{self.logger_for_scripting.handlers[0].baseFilename}'")


# COMMAND ----------

class ProjectManager:
    def __init__(
        self
        ,workspace: WorkspaceClient
        ,manage_ObjectsTracker: Callable
        ,logger_for_setup: logging.Logger
        ,logger_for_cleanup: logging.Logger
    ):
        self.workspace = workspace
        self.manage_ObjectsTracker = manage_ObjectsTracker
        self.logger_for_setup = logger_for_setup
        self.logger_for_cleanup = logger_for_cleanup
        self.tbl_ObjectsTracker = self.manage_ObjectsTracker()

    def Setup(self):
        records = spark.table(self.tbl_ObjectsTracker)
        for record in records.collect():
            object_ddl_CREATE = record['object_ddl_CREATE']
            execute_script(object_ddl_CREATE,self.logger_for_setup,{'workspace':self.workspace})
            self.manage_ObjectsTracker(DDLType='UPDATE_object_status',object_status='CREATED',object_ddl_CREATE=object_ddl_CREATE)
        print(f"\nEND OF SETUP. Refer below log file for more details.\n'{self.logger_for_setup.handlers[0].baseFilename}'")

    def Cleanup(self):
        confirm = input("WARNING: This will drop all database objects and their associated DDL scripts. Refer below table for necessary details. Type 'yes' to proceed: \n'{self.tbl_ObjectsTracker}'")
        if confirm.lower() != 'yes':
            print("Cleanup operation aborted by user.")
            return
        records = spark.table(self.tbl_ObjectsTracker).collect()[::-1]
        for record in records:
            object_ddl_CREATE = record['object_ddl_CREATE']
            object_ddl_DROP = record['object_ddl_DROP']
            execute_script(object_ddl=object_ddl_DROP,logger=self.logger_for_cleanup)
            self.manage_ObjectsTracker(DDLType='UPDATE_object_status',object_status='DROPPED',object_ddl_DROP=object_ddl_DROP)
            remove_script(object_ddl=object_ddl_CREATE,logger=self.logger_for_cleanup)
            remove_script(object_ddl=object_ddl_DROP,logger=self.logger_for_cleanup)
            self.manage_ObjectsTracker(DDLType='UPDATE_script_status',script_status='REMOVED',object_ddl_DROP=object_ddl_DROP)
        print(f"\nEND OF CLEANUP. Refer below log file for more details.\n'{self.logger_for_cleanup.handlers[0].baseFilename}'")

    def Purge(self):
        path_log = os.path.dirname(self.logger_for_cleanup.handlers[0].baseFilename)
        confirm = input("WARNING: PROCEED WITH CAUTION. This will drop the ObjectsTracker table '{self.tbl_ObjectsTracker}' and all log files present in below path. Type 'yes' to proceed: \n'{path_log}'")
        if confirm.lower() != 'yes':
            print("Purge operation aborted by user.")
            return
        try:
            self.manage_ObjectsTracker(DDLType='DROP_objects_tracker')
            for log_file in path_log:
                remove_script(object_ddl=log_file)
            remove_loggers()
            print(f"\nPurge completed.")
        except Exception as e:
            print(f"\nPurge failed with exception: \n{e}")

# COMMAND ----------

# DBTITLE 1,Class
class ProjectSetup:
    def __init__(
        self
        ,ProjectName: str
        ,repository_file: str
        ,metadata_file: str
    ):
        self.ProjectName = ProjectName
        self.repository_file = repository_file
        self.metadata_file = metadata_file

        self.Initializer = Initializer(self.repository_file,self.metadata_file)
        self.Initializer.Initialize()

        self.logger_for_scripting   = get_Logger(get_path(self.Initializer.dict_repo['path_log'],'scripting.log'),'a')
        self.logger_for_setup       = get_Logger(get_path(self.Initializer.dict_repo['path_log'],'setup.log'),'a')
        self.logger_for_cleanup     = get_Logger(get_path(self.Initializer.dict_repo['path_log'],'cleanup.log'),'a')

        self.ScriptGenerator= ScriptGenerator(self.Initializer.dict_repo,self.Initializer.dict_meta,self.Initializer.manage_ObjectsTracker,self.logger_for_scripting)
        self.ProjectManager = ProjectManager(self.Initializer.workspace,self.Initializer.manage_ObjectsTracker,self.logger_for_setup,self.logger_for_cleanup)

