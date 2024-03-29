sas_source_code_tables_data = [
    {
      'name': 'i94cit_res',
      'value': 'i94cntyl',
      'columns': ['code', 'country']
    },
    {
      'name': 'i94port',
      'value': 'i94prtl',
      'columns': ['code', 'port']
    },
    {
      'name': 'i94mode',
      'value': 'i94model',
      'columns': ['code', 'mode']
    },
    {
      'name': 'i94addr',
      'value': 'i94addrl',
      'columns': ['code', 'addr']
    },
    {
      'name': 'i94visa',
      'value': 'I94VISA',
      'columns': ['code', 'type']
    }
]

copy_s3_keys = [
    {
      'name': 'immigration',
      'key': 'sas_data',
      'file_format': 'parquet',
      'sep': ''
    },
    {
      'name': 'us_cities_demographics',
      'key': 'data/us-cities-demographics.csv',
      'file_format': 'csv',
      'sep': ';'
    },
    {
      'name': 'airport_codes',
      'key': 'data/airport-codes_csv.csv',
      'file_format': 'csv',
      'sep': ','
    },
    {
      'name': 'world_temperature',
      'key': 'data/GlobalLandTemperaturesByCity.csv',
      'file_format': 'csv',
      'sep': ','
    },
]

dq_checks = [
    {'check_sql': "SELECT COUNT(*) FROM i94cit_res WHERE code is null", 'expected_result': 0},
    {'check_sql': "SELECT COUNT(*) FROM i94port WHERE code is null", 'expected_result': 0},
    {'check_sql': "SELECT COUNT(*) FROM i94mode WHERE code is null", 'expected_result': 0},
    {'check_sql': "SELECT COUNT(*) FROM i94addr WHERE code is null", 'expected_result': 0},
    {'check_sql': "SELECT COUNT(*) FROM i94visa WHERE code is null", 'expected_result': 0}
]