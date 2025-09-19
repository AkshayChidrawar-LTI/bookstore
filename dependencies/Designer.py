# Databricks notebook source
# MAGIC %run ./Helpers

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
        self.repository = read_yaml(repository_file)
        self.metadata = read_yaml(metadata_file)
        self.dict_repo = self.get_Repository()
        self.dict_meta = self.get_Metadata()
        self.objects_tracker = pd.DataFrame(columns=['object_type','object_name','object_loc','object_ddl_CREATE','object_ddl_DROP','object_properties'])
        self.list_of_objects = ['TABLE','DATABASE','CATALOG','external_location','storage_credential']

    def get_Repository(self)-> dict:
        home = get_path(self.repository['project']['workspacehome'],self.repository['project']['reponame'])
        return {
            "path_ddl": get_path(home, self.repository['path']['ddl'])
            ,"path_schema": get_path(home, self.repository['path']['schema'])
        }

    def get_Metadata(self)->dict:
        return{
            "host":self.metadata['project']['host']
            ,"token":self.metadata['project']['token']
            ,"root":self.metadata['project']['root']
            ,"buckets":self.metadata['buckets']
            ,"dwh_structure":self.metadata['dwh_structure']
        }

    def get_ObjectsTracker(self,AscFlag):
        return get_sortedTracker(self.objects_tracker,self.list_of_objects,AscFlag)

    def GenerateScripts(self):
        path_ddl    = self.dict_repo['path_ddl']
        path_schema = self.dict_repo['path_schema']
        host        = self.dict_meta['host']
        token       = self.dict_meta['token']
        root        = get_bucketlink(self.dict_meta['root'])
        buckets     = self.dict_meta['buckets']
        dwh_structure=self.dict_meta['dwh_structure']

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
            object_loc = get_bucketlink(bucket_name)
            object_ddl_CREATE = get_path(path_ddl_storage_credentials,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_storage_credentials,object_name+'_DROP'+'.py')
            generate_CREATE_scripts(object_type,object_name,object_loc,object_ddl_CREATE,host=host,token=token,bucket_arn=bucket_arn)
            generate_DROP_scripts(object_type,object_name,object_ddl_DROP,host=host,token=token)
            addto_Tracker(self,object_type,object_name,object_loc,object_ddl_CREATE,object_ddl_DROP,object_properties={"bucket_name": bucket_name,"bucket_arn": bucket_arn})

            object_type = 'external_location'
            object_name = 'el_' + bucket_name
            object_loc = get_bucketlink(bucket_name)
            object_ddl_CREATE = get_path(path_ddl_external_locations,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_external_locations,object_name+'_DROP'+'.py')
            el_sc = 'sc_'+bucket_name
            generate_CREATE_scripts(object_type,object_name,object_loc,object_ddl_CREATE,host=host,token=token,el_sc=el_sc)
            generate_DROP_scripts(object_type,object_name,object_ddl_DROP,host=host,token=token)
            addto_Tracker(self,object_type,object_name,object_loc,object_ddl_CREATE,object_ddl_DROP,object_properties={"storage_credential": el_sc})

        for catalog in dwh_structure['catalogs']:
            object_type = 'CATALOG'
            object_name = catalog['name']
            object_loc = get_path(root,object_name)
            object_ddl_CREATE = get_path(path_ddl_catalogs,object_name+'_CREATE'+'.py')
            object_ddl_DROP = get_path(path_ddl_catalogs,object_name+'_DROP'+'.py')
            generate_CREATE_scripts(object_type,object_name,object_loc,object_ddl_CREATE)
            generate_DROP_scripts(object_type,object_name,object_ddl_DROP)
            addto_Tracker(self,object_type,object_name,object_loc,object_ddl_CREATE,object_ddl_DROP)
            for database in catalog['databases']:
                object_type = 'DATABASE'
                object_name = catalog['name']+'.'+database['name']
                object_loc = get_path(root,get_sloc(object_name))
                object_ddl_CREATE = get_path(path_ddl_databases,get_fname(object_name)+'_CREATE'+'.py')
                object_ddl_DROP = get_path(path_ddl_databases,get_fname(object_name)+'_DROP'+'.py')
                generate_CREATE_scripts(object_type,object_name,object_loc,object_ddl_CREATE)
                generate_DROP_scripts(object_type,object_name,object_ddl_DROP)
                addto_Tracker(self,object_type,object_name,object_loc,object_ddl_CREATE,object_ddl_DROP)

        for tableschema in os.listdir(path_schema):
            if tableschema.endswith('.yml'):
                fc = read_yaml(get_path(path_schema,tableschema))
                object_type = 'TABLE'
                object_name = fc['catalog']+'.'+fc['database']+'.'+fc['table']
                object_loc = get_path(root,get_sloc(object_name))
                object_ddl_CREATE = get_path(path_ddl_tables,get_fname(object_name)+'_CREATE'+'.py')
                object_ddl_DROP = get_path(path_ddl_tables,get_fname(object_name)+'_DROP'+'.py')
                table_schema = yaml_to_schema(fc)
                generate_CREATE_scripts(object_type,object_name,object_loc,object_ddl_CREATE,table_schema=table_schema)
                generate_DROP_scripts(object_type,object_name,object_ddl_DROP)
                addto_Tracker(self,object_type,object_name,object_loc,object_ddl_CREATE,object_ddl_DROP,object_properties={"schema": table_schema})
        print(f"\nALL SCRIPTS GENERATED: CREATE and DROP")

    def CreateObjects(self):
        sortedTracker = self.get_ObjectsTracker(AscFlag=False)
        for _,item in sortedTracker.iterrows():
            object_type = item['object_type']
            object_name = item['object_name']
            object_ddl_CREATE = item['object_ddl_CREATE']
            exec(open(object_ddl_CREATE).read())
        print(f"\nALL OBJECTS CREATED.")

    def DropObjectsAndScripts(self):
        sortedTracker = self.get_ObjectsTracker(AscFlag=True)
        for _,item in sortedTracker.iterrows():
            object_type = item['object_type']
            object_name = item['object_name']
            object_ddl_CREATE = item['object_ddl_CREATE']
            object_ddl_DROP = item['object_ddl_DROP']
            exec(open(object_ddl_DROP).read())
            remove_script(object_ddl_CREATE)
            remove_script(object_ddl_DROP)
        print(f"\nALL OBJECTS DROPPED AND SCRIPTS REMOVED.")
