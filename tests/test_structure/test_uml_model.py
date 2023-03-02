from jobsworthy.structure import puml


def test_parse_puml_dataproduct_model():
    result = puml.make("tests/fixtures/uml_model.puml")

    assert result.is_right()

    value = result.value

    assert len(value.klass_model.klasses) == 4

    expected = {'DataProductTable', 'Identity', 'StructColumn1', 'Type'}

    assert {k.name for k in value.klass_model.klasses} == expected


def test_table_generates_schema():
    result = puml.make("tests/fixtures/uml_model.puml")

    expected_schema_class = ['from jobsworthy import structure as S', 'from pyspark.sql import types as T',
                             'from . import vocab', '\n',
                             '(S.Table(vocab=vocab.vocab, vocab_directives=[S.VocabDirective.RAISE_WHEN_TERM_NOT_FOUND])',
                             '.column()  # Identity',
                             ".string('myDpNs.@id', False)",
                             '\n',
                             '.column()  # Type',
                             ".array('myDpNs.@type', T.StringType, False)",
                             '\n',
                             '.column()  # StructColumn1',
                             ".struct('aStruct1.structColumn1', False)",
                             ".string('aStruct1.type', False)",
                             ".string('aStruct1.name', False)",
                             ".long('aStruct1.aLong', False)",
                             ".decimal('aStruct1.anAmount', T.DecimalType(11, 5),  False)",
                             ".decimal('aStruct1.anOptionalAmount', T.DecimalType(11, 5),  True)",
                             ".array('aStruct1.multipleOptionalAmounts', T.DecimalType(11, 5), True)",
                             '.end_struct()',
                             '\n',
                             ')']

    assert result.value.schema == expected_schema_class


def test_table_generates_vocab():
    result = puml.make("tests/fixtures/uml_model.puml")

    expected_vocab = {
        'myDpNs': {
            '@id': {'term': '@id'},
            '@type': {'term': '@type'}
        },
        'aStruct1': {
            'structColumn1': {'term': 'structColumn1'},
            'type': {'term': 'type'},
            'name': {'term': 'name'},
            'aLong': {'term': 'aLong'},
            'anAmount': {'term': 'anAmount'},
            'anOptionalAmount': {'term': 'anOptionalAmount'},
            'multipleOptionalAmounts': {'term': 'multipleOptionalAmounts'}
        }
    }

    assert result.value.vocab == expected_vocab


def test_parse_puml_dataproduct_model_with_database():
    result = puml.make("tests/fixtures/uml_model_with_db.puml")

    assert result.is_right()

    value = result.value

    assert len(value.klass_model.klasses) == 5

    expected = {'DomainDatabase', 'DataProductTable', 'Identity', 'StructColumn1', 'Type'}

    assert {k.name for k in value.klass_model.klasses} == expected


def test_parse_puml_dataproduct_model_with_database_generated_table_schema():
    result = puml.make("tests/fixtures/uml_model_with_db.puml")

    expected_schema_class = ['from jobsworthy import structure as S',
                             'from pyspark.sql import types as T',
                             'from . import vocab',
                             '\n',
                             '(S.Table(vocab=vocab.vocab, vocab_directives=[S.VocabDirective.RAISE_WHEN_TERM_NOT_FOUND])',
                             '.column()  # Identity',
                             ".string('myDpNs.@id', False)",
                             '\n',
                             '.column()  # Type',
                             ".array('myDpNs.@type', T.StringType, False)",
                             '\n',
                             '.column()  # StructColumn1',
                             ".struct('aStruct1.structColumn1', False)",
                             ".string('aStruct1.type', False)",
                             ".string('aStruct1.name', False)",
                             ".long('aStruct1.aLong', False)",
                             ".decimal('aStruct1.anAmount', T.DecimalType(11, 5),  False)",
                             ".decimal('aStruct1.anOptionalAmount', T.DecimalType(11, 5),  True)",
                             ".array('aStruct1.multipleOptionalAmounts', T.DecimalType(11, 5), True)",
                             '.end_struct()',
                             '\n', ')']

    assert result.value.schema == expected_schema_class


def test_generate_repo_definition():
    result = puml.make("tests/fixtures/uml_model_with_db.puml")

    assert result.is_right()

    value = result.value

    expected = {'from jobsworthy import repo', '\n', 'class DomainDatabase(repo.Database):', '    pass', '\n',
                'class DataProductTable(repo.HiveRepo):', '    pass'}
    assert set(value.repo) == expected


def test_generate_puml_from_dataframe_print_schema():
    result = puml.puml_maker("tests/fixtures/print_schema_output.txt",
                             model_name="cbor-dataproduct-model")

    assert result.is_right()

    value = result.value

    expected = ['@startuml cbor-dataproduct-model\n', '!theme amiga\n', 'class root <<table>> {', '}\n',
                'class dateTime <<column>> {', '    --meta--', '    isAtColumnPosition: 0', '    ===',
                '    dateTime: Optional[StringType]', '}\n', 'class identity <<column>> {', '    --meta--',
                '    isAtColumnPosition: 1', '    ===', '    identity: Optional[StringType]', '}\n',
                'class aNumber <<column>> {', '    --meta--', '    isAtColumnPosition: 2', '    ===',
                '    aNumber: Optional[LongType]', '}\n', 'class aDecimalNumber <<column>> {', '    --meta--',
                '    isAtColumnPosition: 3', '    ===', '    aDecimalNumber: Optional[DecimalType(20,5)]', '}\n',
                'class aPrimitiveArray <<column>> {', '    --meta--', '    isAtColumnPosition: 4', '    ===',
                '    aPrimitiveArray: Optional[Array[StringType]]', '}\n', 'class aStruct <<column>> {', '    --meta--',
                '    isAtColumnPosition: 5', '    ===', '    id: Optional[StringType]',
                '    type: Optional[StringType]', '    label: Optional[StringType]', '}\n',
                'class aStructArray <<column>> {', '    --meta--', '    isAtColumnPosition: 6', '    ===', '}\n',
                'class aStructArrayArrayStruct <<array_struct>> {', '    type: Optional[StringType]',
                '    label: Optional[StringType]', '}\n', 'class aNestedStruct <<column>> {', '    --meta--',
                '    isAtColumnPosition: 7', '    ===', '    id: Optional[StringType]',
                '    type: Optional[Array[StringType]]', '    units: Optional[DecimalType(20,6)]', '}\n',
                'class complexDate <<struct>> {', '    year: Optional[IntegerType]', '    month: Optional[IntegerType]',
                '    day: Optional[IntegerType]', '    time_zone: Optional[StringType]', '}\n',
                'class pricesArray <<struct>> {', '    id: Optional[StringType]',
                '    type: Optional[Array[StringType]]', '    amount: Optional[DecimalType(20,6)]',
                '    currencyUri: Optional[StringType]', '    observedDateTime: Optional[StringType]',
                '    label: Optional[StringType]', '}\n', 'class anotherNestedStructWithTheSameStruct <<column>> {',
                '    --meta--', '    isAtColumnPosition: 8', '    ===', '}\n', 'class run <<column>> {', '    --meta--',
                '    isAtColumnPosition: 9', '    ===', '    runIdentity: Optional[StringType]', '}\n',
                'root --> dateTime ', 'root --> identity ', 'root --> aNumber ', 'root --> aDecimalNumber ',
                'root --> aPrimitiveArray ', 'root --> aStruct ', 'root --> aStructArray ', 'root --> aNestedStruct ',
                'root --> anotherNestedStructWithTheSameStruct ', 'root --> run ',
                'aStructArray --> "1..*" aStructArrayArrayStruct ', 'aNestedStruct --> complexDate ',
                'aNestedStruct --> pricesArray ', 'anotherNestedStructWithTheSameStruct --> complexDate ', '@enduml']

    # print("\n".join(value.puml))

    assert value.puml == expected
