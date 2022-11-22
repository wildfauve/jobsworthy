from dependency_injector.wiring import Provide, inject

from tests.shared.di import LocalContainer

@inject
def db(repo=Provide[LocalContainer.database]):
    return repo


@inject
def spark(session=Provide[LocalContainer.session]):
    return session


@inject
def secrets_provider(provider=Provide[LocalContainer.secrets_provider]):
    return provider

