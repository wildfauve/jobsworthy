from jobsworthy.util import error

messages = {
    'table_name_not_configured': """Table_name class property not provided""",

    'error_identity_merge_condition_not_implemented': """The repository requires an identity_merge_condition function 
    to perform a delta merge. This function takes the name of the baseline and the name of the updates used in the merge.
    Return a delta table condition that contains an identity column name (or sub column name). """,

    'df_not_in_stream': """Dataframe is not in a Stream.  Can't write stream.""",

    'writing_stream_without_setting_writer': """Attempting to write to a stream without setting up a stream writer.
    When constructing the hive repo set the stream_writer attribute using either add hive_repo.StreamHiveWriter
    or hive_repo.StreamFileWriter.""",

    'temp_table_not_configured': """temp_table_name class property not provided""",

    'hive_stream_writer_not_available': """hive_repo.StreamHiveWriter can not be used (probably because in Test), use 
    hive_repo.StreamFileWriter instead.""",

    'no_schema_provided_on_create_df': """There is no schema found on the repo.  Creating a DF using a schema will fail.
    Provide a schema via the class attribute 'schema' or the method 'schema_()'.  Schemas may either be defined in
    the dict or structtype form.""",

    'no_schema_defined': """Attempting to build the schema but no schema is provided.  Implement the data property
    schema or the instance method schema_as_dict""",

    'checkpoint_root_not_supported': """Jobsworthy: use of checkpoint_root not supported since version 0.4.0.  
    Use db_path_override_for_checkpoint instead.""",

    'delta_location_configured_incorrectly': """The Delta Location is configured incorrectly.  This is probably due to 
    db_path_override_for_checkpoint missing when in testing.  Either fix the config or disable the check by 
    removing the call to running_in_test() from the JobConfig.""",

    "db_path_not_configured": """Jobsworth: db path not configured.  Set db_file_system_path_root on the HIVE config""",

    "domain_data_product_not_configured": """When using the DbNamingConventionDomainBased, the domain_name and the 
    data_product_name must be set on the job config.""",

    "using_partitioning_without_a_create_schema": """Attempted to create an unmanaged table with partitions, but
    no schema was provided. Implement repo.schema_as_dict().""",

    "namespace_not_provided": """When configuring a property using DataAgreementType, a namespace must be provided""",

    "require_delta_table_reader_but_reader_is_not_for_delta_table": """Requested a delta table read, but the configured
    reader is not a DeltaTableReader.""",

    "delta_table_not_returned": """Tried to read Delta Table but nothing returned""",

    "cosmos_additional_spark_options_as_dict_no_longer_supported": """The declaration of the CosmosDB 
     class attribute additional_spark_options as a Dict but should bea collection of 
     spark_util.SparkOption collection instead.""",

    "cosmos_stream_read_requires_schema_but_none_provided": """Cosmos stream reader requires schema, but no schema 
    defined on class.""",

    "hive_table_can_not_be_created_no_protocol_provided": """Request for table creation can not be performed as no 
    table creation protocol has defined.  Either initialise the table with a table_creation_protocol or initialise
    the table class attribute table_creation_protocol"""
}


def checkpoint_root_not_supported():
    raise error.RepoConfigError(messages[checkpoint_root_not_supported.__name__])


def table_name_not_configured():
    return error.RepoConfigError(messages[table_name_not_configured.__name__])


def error_identity_merge_condition_not_implemented():
    return error.RepoConfigError(messages[error_identity_merge_condition_not_implemented.__name__])


def df_not_in_stream():
    return error.NotAStreamError(messages[df_not_in_stream.__name__])


def writing_stream_without_setting_writer():
    return error.RepoConfigError(messages[writing_stream_without_setting_writer.__name__])


def temp_table_not_configured():
    return error.RepoConfigError(messages[temp_table_not_configured.__name__])


def hive_stream_writer_not_available():
    return error.RepoConfigError(messages[hive_stream_writer_not_available.__name__])


def no_schema_provided_on_create_df():
    return error.RepoWriteError(messages[no_schema_provided_on_create_df.__name__])


def no_schema_defined():
    return error.RepoConfigError(messages[no_schema_defined.__name__])


def delta_location_configured_incorrectly():
    return error.RepoConfigError(messages[delta_location_configured_incorrectly.__name__])


def require_delta_table_reader_but_reader_is_not_for_delta_table():
    return error.RepoConfigError(messages[require_delta_table_reader_but_reader_is_not_for_delta_table.__name__])


def db_path_not_configured():
    return error.RepoConfigError(messages[db_path_not_configured.__name__])


def domain_data_product_not_configured():
    return error.RepoConfigError(messages[domain_data_product_not_configured.__name__])


def using_partitioning_without_a_create_schema():
    return error.RepoConfigError(messages[using_partitioning_without_a_create_schema.__name__])


def namespace_not_provided():
    return error.RepoConfigError(messages[namespace_not_provided.__name__])


def cosmos_additional_spark_options_as_dict_no_longer_supported():
    return error.RepoConfigError(messages[cosmos_additional_spark_options_as_dict_no_longer_supported.__name__])


def hive_table_can_not_be_created_no_protocol_provided():
    return error.RepoConfigError(messages[hive_table_can_not_be_created_no_protocol_provided.__name__])


def delta_table_not_returned():
    return error.RepoConfigError(messages[delta_table_not_returned.__name__])


def cosmos_stream_read_requires_schema_but_none_provided():
    return error.RepoConfigError(messages[cosmos_stream_read_requires_schema_but_none_provided.__name__])