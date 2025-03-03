from typing import Callable, TypeVar, Any
import contextlib
from functools import wraps

from sqlalchemy.orm.scoping import ScopedSession

from app.db.database import ProdSessionLocal


T = TypeVar("T")
SessionProd = ScopedSession(ProdSessionLocal)


@contextlib.contextmanager
def create_session(Session: ScopedSession):
    """
    Contextmanager that will create and teardown a session.
    """
    session = Session()
    try:
        yield session
        session.expunge_all()
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


# TODO: use Param + Concatenate value instead of ... and Any
# return value in wrapper
def provide_session(session_class):
    """
    Function decorator that provides a session if it isn't provided.
    If you want to reuse a session or run the function as part of a
    database transaction, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any):
            arg_session = "session"

            func_params = func.__code__.co_varnames
            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(
                args
            )
            session_in_kwargs = arg_session in kwargs

            if session_in_kwargs or session_in_args:
                return func(*args, **kwargs)
            else:
                with create_session(Session=session_class) as session:
                    kwargs[arg_session] = session
                    return func(*args, **kwargs)

        return wrapper

    return decorator
