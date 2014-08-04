from simpleflow.futures import AbstractFuture
from concurrent.futures import Future as PythonFuture


class Future(PythonFuture, AbstractFuture):
    """Future impl for local execution

    The `concurrent.futures.Future` from python is itself a concrete impl
    rather than an abstract class or an interface. So a multiple inheritance
    is used to work with our interface `simpleflow.futures.AbstractFuture`.

    This class inherits both the abstracts methods from `AbstractFuture` and
    concrete methods from `concurrent.futures.Future`. The `AbstractFuture`
    interface is designed so that python's Future 'implements' its methods.
    Since python's Future is placed left to the `AbstractFuture` in inheritance
    declaration, runtime method resolution will pick the concrete methods.

    This Future class is interoperable with python's builtin executors.
    """
    def finished(self):
        return self.done()