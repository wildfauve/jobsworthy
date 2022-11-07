from jobsworthy.util import env

def test_env_name():
    assert env.Env.env == "test"

def test_specific_env():
    assert not env.Env.is_local()