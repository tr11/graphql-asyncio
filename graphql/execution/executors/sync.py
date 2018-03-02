
class SyncExecutor(object):

    def execute(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)
