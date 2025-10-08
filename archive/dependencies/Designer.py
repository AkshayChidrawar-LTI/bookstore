# Databricks notebook source
# MAGIC %run ./Helper

# COMMAND ----------

class Initializer:
    def __init__(
        self
        ,repository_file: str
        ,metadata_file: str
        ,ObjectsTracker: ObjectsTracker
        ,logger_for_initializer: logging.Logger
    ):
        self.repository_file = repository_file
        self.metadata_file = metadata_file
        self.ObjectsTracker = ObjectsTracker
        self.logger_for_initializer = logger_for_initializer
        self.dict_repo = None
        self.dict_meta = None
        self.workspace = None

    def get_Repository(self,logger)-> dict:
        try:
            repository = read_yaml(self.repository_file)
            home = get_path(repository['project']['workspacehome'],repository['project']['reponame'])
            dict_repo = {
                "path_ddl": get_path(home,repository['path']['ddl'])
                ,"path_schema": get_path(home,repository['path']['schema'])
                ,"path_log": get_path(home,repository['path']['log'])
            }
            logger.info(f"Success: Repository:\n {dict_repo}")
            return dict_repo
        except Exception as e:
            logger.error(f"Failure: Error occurred while reading repository '{self.repository_file}' :\n{e}")

    def get_Metadata(self,logger)->dict:
        try:
            metadata = read_yaml(self.metadata_file)
            dict_meta = {
                "host":metadata['project']['host']
                ,"token":metadata['project']['token']
                ,"buckets":metadata['buckets']
                ,"dwh_structure":metadata['dwh_structure']
                ,"table_schemas": metadata['table_schemas']
                ,"objects_tracker": metadata['objects_tracker']
            }
            logger.info(f"Success: Metadata:\n {dict_meta}")
            return dict_meta
        except Exception as e:
            logger.error(f"Failure: Error occurred while reading metadata '{self.metadata_file}' :\n{e}")

    

# COMMAND ----------

class ScriptGenerator:
    def __init__(
        self
        ,dict_repo: dict
        ,dict_meta: dict
        ,ObjectsTracker: ObjectsTracker
        ,logger_for_scripting: logging.Logger
    ):
        self.dict_repo = dict_repo
        self.dict_meta = dict_meta
        self.ObjectsTracker = ObjectsTracker
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
            bucket_arn = bucket['iam_role']

            object_type = 'storage_credential'
            object_name = 'sc_'+bucket_name
            object_ddl_CREATE = get_path(path_ddl_storage_credentials,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_storage_credentials,object_name+'_DROP'+'.py')
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,self.logger_for_scripting,bucket_arn=bucket_arn)
            self.ObjectsTracker.maintain_objects_tracker(DDLType='APPEND_object',object_type=object_type,object_name=object_name,object_ddl_CREATE=object_ddl_CREATE,object_ddl_DROP=object_ddl_DROP,object_status=None,script_status=None,logger=self.logger_for_scripting)
            self.ObjectsTracker.maintain_objects_tracker(DDLType='UPDATE_script_status',script_status='GENERATED',object_name=object_name,logger=self.logger_for_scripting)

            object_type = 'external_location'
            object_name = 'el_' + bucket_name
            object_ddl_CREATE = get_path(path_ddl_external_locations,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_external_locations,object_name+'_DROP'+'.py')
            el_loc = get_bucketlink(bucket_name)
            el_sc = 'sc_'+bucket_name
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,self.logger_for_scripting,el_loc=el_loc,el_sc=el_sc)
            self.ObjectsTracker.maintain_objects_tracker(DDLType='APPEND_object',object_type=object_type,object_name=object_name,object_ddl_CREATE=object_ddl_CREATE,object_ddl_DROP=object_ddl_DROP,object_status=None,script_status=None,logger=self.logger_for_scripting)
            self.ObjectsTracker.maintain_objects_tracker(DDLType='UPDATE_script_status',script_status='GENERATED',object_name=object_name,logger=self.logger_for_scripting)

        for catalog in dwh_structure['catalogs']:
            object_type = 'CATALOG'
            object_name = catalog['name']
            object_ddl_CREATE = get_path(path_ddl_catalogs,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_catalogs,object_name+'_DROP'+'.py')
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,self.logger_for_scripting)
            self.ObjectsTracker.maintain_objects_tracker(DDLType='APPEND_object',object_type=object_type,object_name=object_name,object_ddl_CREATE=object_ddl_CREATE,object_ddl_DROP=object_ddl_DROP,object_status=None,script_status=None,logger=self.logger_for_scripting)
            self.ObjectsTracker.maintain_objects_tracker(DDLType='UPDATE_script_status',script_status='GENERATED',object_name=object_name,logger=self.logger_for_scripting)

            for database in catalog['databases']:
                object_type = 'DATABASE'
                object_name = catalog['name']+'.'+database['name']
                object_ddl_CREATE = get_path(path_ddl_databases,get_fname(object_name)+'_CREATE'+'.py')
                object_ddl_DROP = get_path(path_ddl_databases,get_fname(object_name)+'_DROP'+'.py')
                GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,self.logger_for_scripting)
                self.ObjectsTracker.maintain_objects_tracker(DDLType='APPEND_object',object_type=object_type,object_name=object_name,object_ddl_CREATE=object_ddl_CREATE,object_ddl_DROP=object_ddl_DROP,object_status=None,script_status=None,logger=self.logger_for_scripting)
                self.ObjectsTracker.maintain_objects_tracker(DDLType='UPDATE_script_status',script_status='GENERATED',object_name=object_name,logger=self.logger_for_scripting)

        for item in table_schemas:
            fc = read_yaml(get_path(path_schema,item['name']))
            object_type = 'TABLE'
            object_name = fc['catalog']+'.'+fc['database']+'.'+fc['table']
            object_ddl_CREATE = get_path(path_ddl_tables,get_fname(object_name)+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_tables,get_fname(object_name)+'_DROP'+'.py')
            table_schema = yaml_to_schema(fc['columns'])
            GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,self.logger_for_scripting,table_schema=table_schema)
            self.ObjectsTracker.maintain_objects_tracker(DDLType='APPEND_object',object_type=object_type,object_name=object_name,object_ddl_CREATE=object_ddl_CREATE,object_ddl_DROP=object_ddl_DROP,object_status=None,script_status=None,logger=self.logger_for_scripting)
            self.ObjectsTracker.maintain_objects_tracker(DDLType='UPDATE_script_status',script_status='GENERATED',object_name=object_name,logger=self.logger_for_scripting)

        print(f"\nEND OF GENERATING SCRIPTS (CREATE & DROP). Refer below log file for more details.\n'{get_log_file(self.logger_for_scripting)}'")


# COMMAND ----------

class ProjectManager:
    def __init__(
        self
        ,ProjectName: str
        ,workspace: WorkspaceClient
        ,ObjectsTracker: ObjectsTracker
        ,logger_for_setup: logging.Logger
        ,logger_for_cleanup: logging.Logger
        ,logger_for_purge: logging.Logger
    ):
        self.ProjectName = ProjectName
        self.workspace = workspace
        self.ObjectsTracker = ObjectsTracker
        self.logger_for_setup = logger_for_setup
        self.logger_for_cleanup = logger_for_cleanup
        self.logger_for_purge = logger_for_purge

    def Setup(self):
        records = self.ObjectsTracker.getData_objects_tracker()
        for record in records.collect():
            object_name = record['object_name']
            object_ddl_CREATE = record['object_ddl_CREATE']
            try:
                execute_script(object_ddl=object_ddl_CREATE,context={'spark':spark,'workspace':self.workspace},logger=self.logger_for_setup)
                self.ObjectsTracker.maintain_objects_tracker(DDLType='UPDATE_object_status',object_status='CREATED',object_name=object_name,logger=self.logger_for_setup)
            except Exception as e:
                self.logger_for_setup.error(f"Failed to execute SETUP for {object_name}: {e}")
        print(f"\nEND OF SETUP. Refer below log file for more details.\n'{get_log_file(self.logger_for_setup)}'")

    def Cleanup(self):
        displayHTML(
            f"<h3>WARNING: PROCEED WITH CAUTION.</h3>This will drop below items:<br><br>"
            f"1. <b>All DWH objects</b><br>"
            f"2. <b>All associated DDL scripts</b>:<br><br>{self.ObjectsTracker.getData_objects_tracker()}"
            )
        confirm = input(f"Type 'yes' to proceed:")
        if confirm.lower() != 'yes':
            print("Cleanup operation aborted by user.")
            return
        records = self.ObjectsTracker.getData_objects_tracker().collect()[::-1]
        for record in records:
            object_name = record['object_name']
            object_ddl_CREATE = record['object_ddl_CREATE']
            object_ddl_DROP = record['object_ddl_DROP']
            try:
                execute_script(object_ddl=object_ddl_DROP,context={'spark':spark,'workspace':self.workspace},logger=self.logger_for_cleanup)
                self.ObjectsTracker.maintain_objects_tracker(DDLType='UPDATE_object_status',object_status='DROPPED',object_name=object_name,logger=self.logger_for_cleanup)
                remove_script(object_ddl=object_ddl_CREATE,logger=self.logger_for_cleanup)
                remove_script(object_ddl=object_ddl_DROP,logger=self.logger_for_cleanup)
                self.ObjectsTracker.maintain_objects_tracker(DDLType='UPDATE_script_status',script_status='REMOVED',object_name=object_name,logger=self.logger_for_cleanup)
            except Exception as e:
                self.logger_for_setup.error(f"Failed to execute CLEANUP for {object_name}: {e}")
        print(f"END OF CLEANUP. Refer below log file for more details:\n'{get_log_file(self.logger_for_cleanup)}'")

    def Purge(self):
        path_log = os.path.dirname(get_log_file(self.logger_for_purge))
        displayHTML(
            f"<h3>WARNING: PROCEED WITH CAUTION.</h3>This will drop following items:<br><br>"
            f"1. <b>ObjectsTracker table</b>: <br>{self.ObjectsTracker.tbl_objects_tracker}<br><br>"
            f"2. <b>All loggers and log files </b>: <br>{path_log}<br><br>"
            f"3. <b>Instance as a whole </b>: <br>{self.ProjectName}<br><br>"
            )
        confirm = input("\nType 'yes' to proceed: ")
        if confirm.lower() != 'yes':
            print("Purge operation aborted by user.")
            return
        try:
            self.ObjectsTracker.manage_objects_tracker(DDLType='DROP',logger=self.logger_for_purge)
            remove_log_files(path_log,self.logger_for_purge)
            remove_loggers(self.logger_for_purge)
            del self.ProjectName
            gc.collect()
            print(f"\nEND OF PURGE. Refer below log file for more details.\n'{get_log_file(self.logger_for_purge)}'")
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

        self.ObjectsTracker = ObjectsTracker()

        self.Initializer = Initializer(repository_file=self.repository_file
                                       ,metadata_file=self.metadata_file
                                       ,ObjectsTracker=self.ObjectsTracker
                                       ,logger_for_initializer=create_Logger('logger_for_initializer',get_path('/Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/log','initializer.log'),'a'))
        self.Initializer.Initialize()

        self.logger_for_scripting   = create_Logger('logger_for_scripting',get_path(self.Initializer.dict_repo['path_log'],'scripting.log'),'w')
        self.logger_for_setup       = create_Logger('logger_for_setup',get_path(self.Initializer.dict_repo['path_log'],'setup.log'),'w')
        self.logger_for_cleanup     = create_Logger('logger_for_cleanup',get_path(self.Initializer.dict_repo['path_log'],'cleanup.log'),'w')
        self.logger_for_purge       = create_Logger('logger_for_purge',get_path(self.Initializer.dict_repo['path_log'],'purge.log'),'w')

        self.ScriptGenerator= ScriptGenerator(dict_repo=self.Initializer.dict_repo
                                              ,dict_meta=self.Initializer.dict_meta
                                              ,ObjectsTracker=self.ObjectsTracker
                                              ,logger_for_scripting=self.logger_for_scripting)

        self.ProjectManager = ProjectManager(ProjectName=self.ProjectName
                                             ,workspace=self.Initializer.workspace
                                             ,ObjectsTracker=self.ObjectsTracker
                                             ,logger_for_setup=self.logger_for_setup
                                             ,logger_for_cleanup=self.logger_for_cleanup
                                             ,logger_for_purge=self.logger_for_purge)

