import collections
import contextlib
import threading


_reentrant_lock = threading.Lock()
_one_time_lock = threading.Lock()


@contextlib.contextmanager
def reentrant(obj, attr, value):
    """
    Makes time-aware patch on the attribute of the object without locking like in `unittest.mock.patch`, the context
    will leak system-wide.
    However, if no `await` happens after obtaining the context, and no threads are getting the same attribute,
    it guarantees that the attribute will have the desired value.
    Effectively guarantees to restore original value after all contexts are destroyed.
    No protection from interleaving foreign code doing same.
    """

    with _reentrant_lock:
        contexts = getattr(obj, f"__{attr}__contexts__", collections.OrderedDict())
        if not contexts:
            contexts[1] = getattr(obj, attr)
            setattr(obj, f"__{attr}__contexts__", contexts)
        context_id = len(contexts) + 1
        contexts[context_id] = value
        setattr(obj, attr, value)

    yield

    with _reentrant_lock:
        last_context_id = next(reversed(contexts))
        del contexts[context_id]
        if last_context_id == context_id:
            setattr(obj, attr, next(reversed(contexts.values())))
        if len(contexts) == 1:
            delattr(obj, f"__{attr}__contexts__")


@contextlib.contextmanager
def one_time(obj, attr, value):
    """
    More lightweight implementation, only sets the attribute once — in outer context.
    Effectively guarantees to restore original value after all contexts are destroyed.
    """

    with _one_time_lock:
        if not hasattr(obj, f"__{attr}__value__"):
            setattr(obj, f"__{attr}__value__", getattr(obj, attr))
            setattr(obj, attr, value)
        setattr(obj, f"__{attr}__count__", getattr(obj, f"__{attr}__count__", 0) + 1)

    yield

    with _one_time_lock:
        count = getattr(obj, f"__{attr}__count__") - 1
        setattr(obj, f"__{attr}__count__", count)
        if not count:
            setattr(obj, attr, getattr(obj, f"__{attr}__value__"))
            delattr(obj, f"__{attr}__value__")
            delattr(obj, f"__{attr}__count__")


@contextlib.contextmanager
def permanent(obj, attr, value):
    """
    Most lightweight implementation.
    """

    setattr(obj, attr, value)

    yield
