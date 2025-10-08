import yaml

def read_yaml(file_path)->dict|list:
    with open(file_path, 'r') as f:
        fc = yaml.safe_load(f)
    return fc

def null_check(cnull):
    if cnull:
        return ''
    else:
        return 'not null'

def yaml_to_schema(columns):
    schema = f"("
    for col in columns:
        if col['type'] == 'array' and col['item']['type'] == 'struct':
            schema += f"\n" + col['name'] + ' array <struct<'
            for itemcol in col['item']['columns']:
                schema += f"\n\t{itemcol['name']} {itemcol['type']} {null_check(itemcol['nullable'])},"
            schema = schema.rstrip(',') + '>>'
        else:
            schema += f"\n{col['name']} {col['type']} {null_check(col['nullable'])},"
    schema = schema.rstrip(',') + f"\n)"
    return schema