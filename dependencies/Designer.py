# Databricks notebook source
# MAGIC %run ./Helper

# COMMAND ----------

# DBTITLE 1,Class
class ProjectSetup:
    def __init__(
        self
        ,ProjectName: str
        ,repository_file: str
        ,metadata_file: str
        ,logFileName: str
    ):
        self.ProjectName= ProjectName
        self.logFileName= logFileName
        self.logger     = setLogger(self.logFileName)
        self.repository = read_yaml(repository_file)
        self.metadata   = read_yaml(metadata_file)
        self.dict_repo  = self.get_Repository()
        self.dict_meta  = self.get_Metadata()
        self.objects_tracker = pd.DataFrame(columns=['object_type','object_name','object_ddl_CREATE','object_ddl_DROP','object_properties'])

    def get_Repository(self)-> dict:
        home = get_path(self.repository['project']['workspacehome'],self.repository['project']['reponame'])
        return {
            "path_ddl": get_path(home, self.repository['path']['ddl'])
            ,"path_schema": get_path(home, self.repository['path']['schema'])
        }

    def get_Metadata(self)->dict:
        return {
            "host":self.metadata['project']['host']
            ,"token":self.metadata['project']['token']
            ,"root":self.metadata['project']['root']
            ,"buckets":self.metadata['buckets']
            ,"dwh_structure":self.metadata['dwh_structure']
            ,"table_schemas": self.metadata['table_schemas']
            ,"objects_tracker": self.metadata['objects_tracker']
        }

    def get_ObjectTracker(self):
        fc = read_yaml(self.dict_meta['objects_tracker'])
        object_name = fc['catalog']+'.'+fc['database']+'.'+fc['table']
        table_schema = yaml_to_schema(fc['columns'])
        object_ddl_CREATE = get_path(path_ddl_tables,get_fname(object_name)+'_CREATE'+'.py')
        object_ddl_DROP = get_path(path_ddl_tables,get_fname(object_name)+'_DROP'+'.py')
        table_schema = yaml_to_schema(fc['columns'])
            generate_CREATE_scripts(object_type,object_name,object_ddl_CREATE,logger,table_schema=table_schema)
            generate_DROP_scripts(object_type,object_name,object_ddl_DROP,logger)
        


    def GenerateScripts(self):
        path_ddl    = self.dict_repo['path_ddl']
        path_schema = self.dict_repo['path_schema']
        host        = self.dict_meta['host']
        token       = self.dict_meta['token']
        root        = get_bucketlink(self.dict_meta['root'])
        buckets     = self.dict_meta['buckets']
        dwh_structure=self.dict_meta['dwh_structure']
        table_schemas=self.dict_meta['table_schemas']
        logger      = self.logger

        path_ddl_storage_credentials = get_path(path_ddl,'buckets/storage_credentials')
        path_ddl_external_locations = get_path(path_ddl,'buckets/external_locations')
        path_ddl_catalogs = get_path(path_ddl,'catalogs')
        path_ddl_databases = get_path(path_ddl,'databases')
        path_ddl_tables = get_path(path_ddl,'tables')

        for bucket in buckets:
            bucket_name = bucket['name']
            bucket_arn = bucket['iam_role']

            object_type = 'storage_credential'
            object_name = 'sc_'+bucket_name
            object_ddl_CREATE = get_path(path_ddl_storage_credentials,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_storage_credentials,object_name+'_DROP'+'.py')
            generate_CREATE_scripts(object_type,object_name,object_ddl_CREATE,logger,host=host,token=token,bucket_arn=bucket_arn)
            generate_DROP_scripts(object_type,object_name,object_ddl_DROP,logger,host=host,token=token)
            self.objects_tracker = appendTo_DF(self.objects_tracker,{'object_type':object_type,'object_name':object_name,'object_ddl_CREATE':object_ddl_CREATE,'object_ddl_DROP':object_ddl_DROP,'object_properties':{'bucket_name':bucket_name,'bucket_arn':bucket_arn}})

            object_type = 'external_location'
            object_name = 'el_' + bucket_name
            object_ddl_CREATE = get_path(path_ddl_external_locations,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_external_locations,object_name+'_DROP'+'.py')
            el_loc = get_bucketlink(bucket_name)
            el_sc = 'sc_'+bucket_name
            generate_CREATE_scripts(object_type,object_name,object_ddl_CREATE,logger,host=host,token=token,el_loc=el_loc,el_sc=el_sc)
            generate_DROP_scripts(object_type,object_name,object_ddl_DROP,logger,host=host,token=token)
            self.objects_tracker = appendTo_DF(self.objects_tracker,{'object_type':object_type,'object_name':object_name,'object_ddl_CREATE':object_ddl_CREATE,'object_ddl_DROP':object_ddl_DROP,'object_properties':{"el_loc":el_loc,"el_sc":el_sc}})

        for catalog in dwh_structure['catalogs']:
            object_type = 'CATALOG'
            object_name = catalog['name']
            object_ddl_CREATE = get_path(path_ddl_catalogs,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_catalogs,object_name+'_DROP'+'.py')
            generate_CREATE_scripts(object_type,object_name,object_ddl_CREATE,logger)
            generate_DROP_scripts(object_type,object_name,object_ddl_DROP,logger)
            self.objects_tracker = appendTo_DF(self.objects_tracker,{'object_type':object_type,'object_name':object_name,'object_ddl_CREATE':object_ddl_CREATE,'object_ddl_DROP':object_ddl_DROP,'object_properties':{}})
            for database in catalog['databases']:
                object_type = 'DATABASE'
                object_name = catalog['name']+'.'+database['name']
                object_ddl_CREATE = get_path(path_ddl_databases,get_fname(object_name)+'_CREATE'+'.py')
                object_ddl_DROP = get_path(path_ddl_databases,get_fname(object_name)+'_DROP'+'.py')
                generate_CREATE_scripts(object_type,object_name,object_ddl_CREATE,logger)
                generate_DROP_scripts(object_type,object_name,object_ddl_DROP,logger)
                self.objects_tracker = appendTo_DF(self.objects_tracker,{'object_type':object_type,'object_name':object_name,'object_ddl_CREATE':object_ddl_CREATE,'object_ddl_DROP':object_ddl_DROP,'object_properties':{}})

        for item in table_schemas:
            fc = read_yaml(get_path(path_schema,item['name']))
            object_type = 'TABLE'
            object_name = fc['catalog']+'.'+fc['database']+'.'+fc['table']
            object_ddl_CREATE = get_path(path_ddl_tables,get_fname(object_name)+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_tables,get_fname(object_name)+'_DROP'+'.py')
            table_schema = yaml_to_schema(fc['columns'])
            generate_CREATE_scripts(object_type,object_name,object_ddl_CREATE,logger,table_schema=table_schema)
            generate_DROP_scripts(object_type,object_name,object_ddl_DROP,logger)
            self.objects_tracker = appendTo_DF(self.objects_tracker,{'object_type':object_type,'object_name':object_name,'object_ddl_CREATE':object_ddl_CREATE,'object_ddl_DROP':object_ddl_DROP,'object_properties':{'table_schema':table_schema}})

        print(f"\nEND OF GENERATING SCRIPTS (CREATE & DROP). Refer below log file for more details.\n'{self.logFileName}'")

    def CreateObjects(self):
        for _,item in self.objects_tracker.iterrows():
            object_ddl_CREATE = item['object_ddl_CREATE']
            execute_script(object_ddl_CREATE,self.logger)
        print(f"\nEND OF CREATING OBJECTS. Refer below log file for more details.\n'{self.logFileName}'")

    def DropObjectsAndScripts(self):
        sortedTracker = self.objects_tracker.iloc[::-1]
        for _,item in sortedTracker.iterrows():
            object_ddl_CREATE = item['object_ddl_CREATE']
            object_ddl_DROP = item['object_ddl_DROP']
            execute_script(object_ddl_DROP,self.logger)
            remove_script(object_ddl_CREATE,{'logger':self.logger})
            remove_script(object_ddl_DROP,{'logger':self.logger})
        print(f"\nEND OF DROPPING OBJECTS AND SCRIPTS. Refer below log file for more details.\n'{self.logFileName}'")
