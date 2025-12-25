"""
Deafult value can only be assigned to Keyword argument.

Deafult value assigned -->
    argument passed during function call: overrides default value.
    argument not passed during function call:  carry forward default value.
    
Deafult value not assigned -->
    argument passed during function call: works normal.
    argument not passed during function call:  throws error.

In Python, arguments to a function can be passed in several ways:

Positional arguments --> must be provided in a specific order, thus keywords not required.
    discrete: number of arguments is fixed.
    continuous (*args): number of arguments varies and passed as a tuple.

Keyword arguments --> , must be specified by a keyword (name), thus order not required.
    discrete: number of arguments is fixed.
    continuous (**kwargs): number of arguments varies and passed as a dict. 

All 'Positional-discrete' arguments must be provided before the 'continous' ones (*args or **kwargs), because it is impossible to identify the former sitting among the later (variable length). If any 'discrete' arguments provided after the continous ones (*args or **kwargs), then they must be 'keyword', for correct identification.  
Continuous arguments (*args or **kwargs) can be unpacked further inside the function logic based on requirements.

"""

def example_func(a, b=2, *args, c, **kwargs):
    pass

example_func(
    1,                # positional argument 'a'
    b=3,              # keyword argument 'b' (overrides default value b=2)
    4, 5, 6,          # extra positional arguments (go into *args as a tuple)
    c='required',     # keyword-only argument 'c'
    d=7, e=8          # extra keyword arguments (go into **kwargs as a dict)
)

"""


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
