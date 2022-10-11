from jobsworth.util import singleton

class InitState(singleton.Singleton):

    state = []

    def add_state(self, thing):
        self.state.append(thing)
