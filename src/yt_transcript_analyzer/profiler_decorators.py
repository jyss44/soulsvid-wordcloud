import cProfile


def profileit(filename=None):
    def profileit_inner(func):
        def wrapper(*args, **kwargs):
            datafn = func.__name__ + ".profile" if not filename else filename# Name the data file sensibly
            prof = cProfile.Profile()
            retval = prof.runcall(func, *args, **kwargs)
            prof.dump_stats(datafn)
            return retval

        return wrapper
    return profileit_inner
