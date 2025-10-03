################################ CREATE ######################################
def script_for_create_sc(object_name,bucket_arn):
    script = f"""
from databricks.sdk.service.catalog import AwsIamRole
workspace.storage_credentials.create(
    name='{object_name}'
    ,aws_iam_role=AwsIamRole(role_arn = '{bucket_arn}')
    )
"""
    return script
#----------------------------------------------------------------------
def script_for_create_el(object_name,el_loc,el_sc):
    script = f"""
workspace.external_locations.create(
    name='{object_name}'
    ,url='{el_loc}'
    ,credential_name='{el_sc}'
    )
"""
    return script
#----------------------------------------------------------------------
def script_for_create_cdt(object_type,object_name,table_schema=''):
    script = f"""
spark.sql(f\"\"\" CREATE {object_type} {object_name} \\n{table_schema};\\n \"\"\")
"""
    return script
################################## DROP ####################################
def script_for_drop_sc(object_name):
    script = f"""
workspace.storage_credentials.delete(name='{object_name}')
"""
    return script
#----------------------------------------------------------------------
def script_for_drop_el(object_name):
    script = f"""
workspace.external_locations.delete(name='{object_name}')
"""
    return script
#----------------------------------------------------------------------
def script_for_drop_cdt(object_type,object_name):
    cascade = '' if object_type == 'TABLE' else 'CASCADE'
    script = f"spark.sql(\"DROP {object_type} {object_name} {cascade};\")\n"
    return script
#----------------------------------------------------------------------
def generate_script(object_type,object_name,**kwargs):
    workspace   = kwargs.get('workspace','')
    bucket_arn  = kwargs.get('bucket_arn','')
    el_loc      = kwargs.get('el_loc','')
    el_sc       = kwargs.get('el_sc','')
    table_schema= kwargs.get('table_schema','')

    func_map = {
        'storage_credential': (
            lambda:script_for_create_sc(object_name,bucket_arn)
            ,lambda:script_for_drop_sc(object_name)
            )
        ,'external_location': (
            lambda:script_for_create_el(object_name,el_loc,el_sc)
            ,lambda:script_for_drop_el(object_name)
            )
        ,'CATALOG':(
            lambda:script_for_create_cdt(object_type,object_name)
            ,lambda:script_for_drop_cdt(object_type,object_name)
            )
        ,'DATABASE':(
            lambda: script_for_create_cdt(object_type,object_name)
            ,lambda: script_for_drop_cdt(object_type,object_name)
            )
        ,'TABLE':(
            lambda: script_for_create_cdt(object_type,object_name,table_schema)
            ,lambda: script_for_drop_cdt(object_type,object_name)
            )
        }
    if object_type in func_map:
        create_func,drop_func = func_map[object_type]
        create_script = create_func()
        drop_script   = drop_func()
    return create_script,drop_script
#--------------------------------------------------------------------------
def save_script(object_ddl,script):
    with open(object_ddl,'w') as file:
        file.write(script)
#---------------------------------------------------------------------------
def GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,logger,**kwargs):
    create_script,drop_script = generate_script(object_type=object_type,object_name=object_name,**kwargs)
    save_script(object_ddl=object_ddl_CREATE,script=create_script)
    save_script(object_ddl=object_ddl_DROP,script=drop_script)
