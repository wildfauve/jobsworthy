from dependency_injector.wiring import Provide, inject

from tests.shared.di import TestContainer

@inject
def db(repo=Provide[TestContainer.database]):
    return repo


@inject
def spark(session=Provide[TestContainer.session]):
    return session


@inject
def secrets_provider(provider=Provide[TestContainer.secrets_provider]):
    return provider
