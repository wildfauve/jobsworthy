from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

from jobsworthy.structure import vocab_util as V

common_vocab = {
    "*":
        {
            "sfo-prt:hasNAVDateTime": {
                'hasDataProductTerm': "navDateTime"
            },
            "dateUTCDimension": {
                'hasDataProductTerm': "dateUTCDimension"
            },
            "timeDimension": {
                'month': {'hasDataProductTerm': "navDateMonth"},
                'day': {'hasDataProductTerm': "navDateDay"}
            },
            "lcc-lr:isMemberOf": {
                'hasDataProductTerm': "member",
                'hasMeta': {'term': "lcc-lr:isMemberOf"}
            },
            "lcc-lr:hasTag": {
                'hasDataProductTerm': "label",
                'hasMeta': {'term': "lcc-lr:hasTag"}
            },
            "skos:notation": {
                'hasDataProductTerm': "notation",
                'hasMeta': {'term': "skos:notation"}
            },
            "@id": {
                'hasDataProductTerm': "id"
            },
            "@type": {
                'hasDataProductTerm': "type"
            }
        }
}


def build_field(term, field_type, metadata, nullable) -> StructField:
    return StructField(term, field_type, metadata=metadata, nullable=nullable)


def build_string_field(vocab_path, vocab, nullable) -> StructField:
    term, meta = V.term_and_meta(vocab_path, vocab)
    return build_field(term, StringType(), metadata=meta, nullable=nullable)


def build_decimal_field(vocab_path, vocab, decimal_type, nullable) -> StructField:
    term, meta = V.term_and_meta(vocab_path, vocab)
    return build_field(term, decimal_type, metadata=meta, nullable=nullable)


def build_long_field(vocab_path, vocab, nullable) -> StructField:
    term, meta = V.term_and_meta(vocab_path, vocab)
    return build_field(term, LongType(), metadata=meta, nullable=nullable)


def build_array_field(vocab_path, vocab, struct_type, nullable) -> StructField:
    term, meta = V.term_and_meta(vocab_path, vocab)
    return build_field(term, ArrayType(struct_type), metadata=meta, nullable=nullable)


def build_struct_field(vocab_path: str, vocab, struct_type: StructType, nullable: bool) -> StructField:
    term, meta = V.term_and_meta(vocab_path, vocab)
    return build_field(term, struct_type, metadata=meta, nullable=nullable)


# to build a field with type as a parameter.
# e.g. build_type_field('path',vocab, IntegerType(), nullable=False) creates an integer type field
def build_type_field(vocab_path, vocab, field_type, nullable) -> StructField:
    term, meta = V.term_and_meta(vocab_path, vocab)
    return build_field(term, field_type, metadata=meta, nullable=nullable)


at_id = build_string_field("*.@id", common_vocab, nullable=False)

optional_at_id = build_string_field("*.@id", common_vocab, nullable=True)

at_type = build_string_field("*.@type", common_vocab, nullable=False)

at_type_array = build_array_field("*.@type", common_vocab, StringType(), nullable=False)

label = build_string_field("*.lcc-lr:hasTag", common_vocab, nullable=True)

type_id_label_struct = StructType([
    at_id,
    at_type,
    label
])

type_id_struct = StructType([
    at_id,
    at_type,
])

type_label_struct = StructType([
    at_type,
    label
])

id_struct = StructType([
    at_id
])
