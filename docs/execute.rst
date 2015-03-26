.. _execute:

Execution of Tasks as Programs
==============================

Introduction
-------------

The :py:mod:`simpleflow.execute` module allows to define functions that will be
executed as a program.

There are two modes:

- Convert the definition of a fonction as a command line.
- Execute a Python function in another process.

Please refer to the :py:mod:`simpleflow.tests.test_activity` test module for
further examples.

Executing a function as a command line
--------------------------------------

Let's take the example of ``ls``:

.. code::

        @execute.program()
        def ls():
            pass

Calling ``ls()`` in Python will execute the ``ls`` command. Here the purpose of
the function definition is only to describe the command line. The reason for
this is to map a call in a workflow definition to a program to execute on the
command line. The program may be written in any language whereas the workflow
definition is in Python.

Executing a Python function in another process
----------------------------------------------

The rationale for this feature is to execute a function with another
interpreter (such as pypy) or in another environment (virtualenv).

.. code::

    @execute.python(interpreter='pypy')
    def inc(xs):
        return [x + 1 for x in xs]

Calling ``inc(range(10))`` in Python will execute the function with the
``pypy`` interpreter found in the ``$PATH``.


Limitations
-----------

The main limitation comes from the need to serialize the arguments and the
return values to pass them as strings. Hence all arguments and return values
must be convertible into JSON values.
