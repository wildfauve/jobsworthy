from tests.shared import tables


def test_read_table(test_db):
    my_table = tables.MyHiveTable(db=test_db)
    my_table.write_append(tables.my_table_df(test_db))

    df = my_table.read()
    assert df.columns == ['id', 'isDeleted', 'name', 'pythons', 'season']

    sketches = set([(row.id, row.name) for row in df.select(df.id, df.name).collect()])

    expected_results = set([('https://example.nz/montyPython/sketches/thePiranhaBrothers', 'The Piranha Brothers'),
                        ('https://example.nz/montyPython/sketches/theSpanishInquisition', 'The Spanish Inquisition')])

    assert sketches == expected_results

