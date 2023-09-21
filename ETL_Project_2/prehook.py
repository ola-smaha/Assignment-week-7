import os
from database_handler import execute_query, create_connection, close_connection,return_data_as_df, return_create_statement_from_df
from lookups import ErrorHandling, PreHookSteps, SQLTablesToReplicate, InputTypes, SourceName, ETLStep, DestinationName
from logging_handler import show_error_message
from misc_handler import get_sql_files

# append the execute sql folder
# execute_sql_prehook
# execute_sql_hook
def execute_sql_folder_prehook(db_session, sql_command_directory_path, target_schema):
    sql_files = get_sql_files(sql_command_directory_path)
    for file in sql_files:
        if '_prehook' in file:
            with open(os.path.join(sql_command_directory_path,file), 'r') as f:
                sql_query = f.read()
                sql_query = sql_query.replace('target_schema', target_schema.value)
                return_val = execute_query(db_session= db_session, query= sql_query)
                if not return_val == ErrorHandling.NO_ERROR:
                    raise Exception(f"{PreHookSteps.EXECUTE_SQL_QUERY.value} = SQL File Error on SQL FILE = " +  str(file))
    
def return_tables_by_schema(schema_name):
    schema_tables = list()
    tables = [table.value for table in SQLTablesToReplicate]
    for table in tables:
        if table.split('.')[0] == schema_name:
            schema_tables.append(table.split('.')[1])
    return schema_tables

def return_lookup_items_as_dict(lookup_item):
    enum_dict = {str(item.name).lower():item.value.replace(item.name.lower() + "_","") for item in lookup_item}
    return enum_dict
    

def create_sql_staging_table_index(db_session,source_name, table_name, index_val):
    query = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{index_val} ON {source_name}.{table_name} ({index_val});"
    execute_query(db_session,query)

def create_sql_staging_tables(db_session, source_name):
    source_name = source_name.value
    tables = return_tables_by_schema(source_name)
    for table in tables:
        staging_query = f"""
                SELECT * FROM {source_name}.{table} LIMIT 1
        """
        staging_df = return_data_as_df(db_session= db_session, input_type= InputTypes.SQL, file_executor= staging_query)
        columns = list(staging_df.columns)
        dst_table = f"stg_{source_name}_{table}"
        create_stmt = return_create_statement_from_df(staging_df, DestinationName.Datawarehouse.value, dst_table)
        return_val = execute_query(db_session=db_session, query= create_stmt)
        if return_val != ErrorHandling.NO_ERROR:
            raise Exception(f'{PreHookSteps.CREATE_SQL_STAGING.value} = error executing query of table {table}.')
        create_sql_staging_table_index(db_session, DestinationName.Datawarehouse.value, dst_table, columns[0])


def execute_prehook(sql_command_directory_path = './SQL_Commands'):
    step_name = ""
    try:
        db_session = create_connection()
        execute_sql_folder_prehook(db_session, sql_command_directory_path, ETLStep.PRE_HOOK, DestinationName.Datawarehouse) 
        create_sql_staging_tables(db_session,SourceName.DVD_RENTAL)
        close_connection(db_session)
    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.PREHOOK_SQL_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception(f"Important Step Failed step = {step_name}")