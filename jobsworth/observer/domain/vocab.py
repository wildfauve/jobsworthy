vocab = {
    "": {},
    "run": {
        "hasDataProductTerm": "run",
        "sfo-lin:hasRunTime": {"hasDataProductTerm": "hasRunTime"},
        "sfo-lin:runDateUTC": {"hasDataProductTerm": "hasRunDateUTC"},
        "sfo-lin:isRunOf": {"hasDataProductTerm": "isRunOf"},
        "sfo-lin:hasTrace": {"hasDataProductTerm": "hasTrace"},
        "sfo-lin:hasStartTime": {"hasDataProductTerm": "hasStartTime"},
        "sfo-lin:hasEndTime": {"hasDataProductTerm": "hasEndTime"},
        "sfo-lin:hasRunState": {"hasDataProductTerm": "hasRunState"},
        "sfo-lin:hasInputs": {
            "hasDataProductTerm": "hasInputs",
            "sfo-lin:hasLocation": {"hasDataProductTerm": "hasLocation"},
            "sfo-lin:hasName": {"hasDataProductTerm": "hasName"}
        },
        "sfo-lin:hasOutputs": {
            "hasDataProductTerm": "hasOutputs",
            "sfo-lin:hasLocation": {"hasDataProductTerm": "hasLocation"},
            "sfo-lin:hasName": {"hasDataProductTerm": "hasName"}
        },
        "sfo-lin:hasMetrics": {
            "hasDataProductTerm": "hasMetrics"
        }
    },
    "*": {
        "@id": {
            'hasDataProductTerm': "id"
        },
        "@type": {
            'hasDataProductTerm': "type"
        },
        "lcc-lr:hasTag": {
            'hasDataProductTerm': "label",
            'hasMeta': {'term': "lcc-lr:hasTag"}
        }
    }
}
