def test_df(spark):
    return spark.read.json("tests/fixtures/table1_rows.json", multiLine=True, prefersDecimal=True)


def test_df_2(spark):
    return spark.read.json("tests/fixtures/table1_rows_2.json", multiLine=True, prefersDecimal=True)