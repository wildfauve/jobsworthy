from jobsworthy import repo


def test_merge_options():
    options = [repo.SparkOption.MERGE_SCHEMA, repo.SparkOption.COSMOS_INFER_SCHEMA]

    assert repo.SparkOption.function_based_options(options) == {'spark.cosmos.read.inferSchema.enabled': 'true',
                                                           'mergeSchema': 'true'}


def test_options_to_spark_options():
    options = [repo.SparkOption.MERGE_SCHEMA, repo.SparkOption.COSMOS_INFER_SCHEMA]

    expected = [('spark.databricks.delta.schema.autoMerge.enabled', 'true'),
                ('spark.cosmos.read.inferSchema.enabled', 'true')]

    assert repo.SparkOption.options_to_spark_options(options) == expected


def test_options_to_spark_option_names():
    options = [repo.SparkOption.MERGE_SCHEMA, repo.SparkOption.COSMOS_INFER_SCHEMA]

    expected = ['spark.databricks.delta.schema.autoMerge.enabled', 'spark.cosmos.read.inferSchema.enabled']

    assert repo.SparkOption.options_to_spark_option_names(options) == expected


def test_cosmos_additional_options_as_dict():
    options = [
        repo.SparkOption.COSMOS_INFER_SCHEMA,
        repo.SparkOption.COSMOS_ITEM_OVERWRITE,
        repo.SparkOption.COSMOS_READ_PARTITION_DEFAULT,
        repo.SparkOption.COSMOS_CHANGE_FEED_INCREMENTAL
    ]

    expected = {'spark.cosmos.changeFeed.mode': 'Incremental', 'spark.cosmos.read.partitioning.strategy': 'Default',
                'spark.cosmos.write.strategy': 'ItemOverwrite', 'spark.cosmos.read.inferSchema.enabled': 'true'}

    assert repo.SparkOption.function_based_options(options) == expected
