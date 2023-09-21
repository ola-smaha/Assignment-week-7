import os

def get_sql_files(sql_file_directory):
    sql_files = []
    for file in os.listdir(sql_file_directory):
        if file.endswith('.sql'):
            sql_files.append(file)
    return sorted(sql_files)