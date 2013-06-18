import re
import copy
import itertools


from pyparsing import ParseException
from BQL.parser.properties_mapping import query_to_python


class ResourceTypeValidator(object):
    MANDATORY_RULES_FIELDS = ('query', 'value')

    def __init__(self, settings):
        self.settings = settings

    def _validate(self):
        if hasattr(self, '_is_valid'):
            return None

        self._compiled_settings = copy.copy(self.settings)
        self._errors = {'host': [],
                        'query': [],
                        'field': []
                        }
        self._is_valid = False

        for host, rules in self._compiled_settings.iteritems():
            if re.search('^(.+)\*', host):
                self._errors['host'].append('Host %s should contains wildcard only at the beginning' % host)
            for i, rule in enumerate(rules):
                for field in self.MANDATORY_RULES_FIELDS:
                    if field not in rule.keys():
                        self._errors['field'].append('%s is mandatory in host %s rule %d' % (field, host, i))
                try:
                    query = query_to_python(rule['query'])
                    # add compiled query
                    rule['query'] = query
                except ParseException, e:
                    self._errors['query'].append('Error in host %s rule %d : %s' % (host, i, e))
        self._is_valid = len(self.errors) == 0

    def is_valid(self):
        self._validate()
        return self._is_valid

    def compile(self):
        self._validate()
        if self.is_valid():
            return self._compiled_settings

    @property
    def errors(self):
        return [e for e in itertools.chain(*self._errors.values())]

    @property
    def host_errors(self):
        return self._errors['host']

    @property
    def query_errors(self):
        return self._errors['query']

    @property
    def field_errors(self):
        return self._errors['field']
