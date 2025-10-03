def maintain_objects_tracker(DDLType,**kwargs):
    object_type = kwargs.get('object_type','')
    object_name = kwargs.get('object_name','')
    object_ddl_CREATE = kwargs.get('object_ddl_CREATE','')
    object_ddl_DROP = kwargs.get('object_ddl_DROP','')
    object_status= kwargs.get('object_status','')
    script_status= kwargs.get('script_status','')

    if DDLType == 'CREATE':
        spark.sql(f"""
                CREATE TABLE workspace.default.tbl_objects_tracker
                (object_type STRING,object_name STRING,object_ddl_CREATE STRING,object_ddl_DROP STRING,object_status STRING,script_status STRING)
                """)
    elif DDLType == 'GET_table_name':
        return f"workspace.default.tbl_objects_tracker"
    elif DDLType == 'GETDATA':
        return spark.table('workspace.default.tbl_objects_tracker')
    elif DDLType == 'DROP':
        spark.sql(f"DROP TABLE workspace.default.tbl_objects_tracker")
    elif DDLType == 'APPEND_object':
        spark.sql(f"""
                INSERT INTO workspace.default.tbl_objects_tracker
                VALUES (
                    '{object_type}'
                    ,'{object_name}'
                    ,'{object_ddl_CREATE}'
                    ,'{object_ddl_DROP}'
                    ,'{object_status}'
                    ,'{script_status}')
                    """)
    elif DDLType == 'UPDATE_script_status':
        spark.sql(f"""
                UPDATE workspace.default.tbl_objects_tracker
                SET script_status = '{script_status}'
                WHERE object_name = '{object_name}'
                """)
    elif DDLType == 'UPDATE_object_status':
            spark.sql(f"""
                    UPDATE workspace.default.tbl_objects_tracker
                    SET object_status = '{object_status}'
                    WHERE object_name = '{object_name}'
                    """)
