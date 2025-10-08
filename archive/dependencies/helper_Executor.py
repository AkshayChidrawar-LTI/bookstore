import os

def execute_script(object_ddl,context={}):
    with open(object_ddl) as f:
        code = f.read()
    exec(code,context)

def remove_script(object_ddl):
    os.remove(object_ddl)
