from jobsworthy import repo

def test_default_option():
    reader_options = repo.ReaderSwitch.merge_options(defaults={repo.ReaderSwitch.GENERATE_DF_ON})

    assert reader_options == {repo.ReaderSwitch.GENERATE_DF_ON}

def test_overrides_the_default_option():
    reader_options = repo.ReaderSwitch.merge_options(defaults={repo.ReaderSwitch.GENERATE_DF_ON},
                                                     overrides={repo.ReaderSwitch.GENERATE_DF_OFF})

    assert reader_options == {repo.ReaderSwitch.GENERATE_DF_OFF}

def test_merges_default_and_overrides():
    defaults = {repo.ReaderSwitch.GENERATE_DF_ON, repo.ReaderSwitch.READ_STREAM_WITH_SCHEMA_OFF}
    overrides = {repo.ReaderSwitch.GENERATE_DF_OFF}

    reader_options = repo.ReaderSwitch.merge_options(defaults=defaults, overrides=overrides)

    assert reader_options == {repo.ReaderSwitch.GENERATE_DF_OFF, repo.ReaderSwitch.READ_STREAM_WITH_SCHEMA_OFF}


def test_merges_all_overrides_when_no_defaults():
    overrides = {repo.ReaderSwitch.GENERATE_DF_OFF, repo.ReaderSwitch.READ_STREAM_WITH_SCHEMA_OFF}

    reader_options = repo.ReaderSwitch.merge_options(defaults=set(), overrides=overrides)

    assert reader_options == {repo.ReaderSwitch.GENERATE_DF_OFF, repo.ReaderSwitch.READ_STREAM_WITH_SCHEMA_OFF}