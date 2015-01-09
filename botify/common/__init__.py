# The lines below are required to share the ``botify`` namespace with other
# packages.  For example, being able to install on the same environment
# ``botify.common`` and ``botify.other_package``.
try:
    __import__('pkg_resources').declare_namespace(__name__)
except ImportError:
    __path__ = __import__('pkgutil').extend_path(__path__, __name__)


from ._version import VERSION

__version__ = VERSION
