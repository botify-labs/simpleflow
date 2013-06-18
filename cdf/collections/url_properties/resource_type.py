import re
import itertools


from BQL.parser.properties_mapping import validate_query_grammar, query_to_python


def compile_resource_type_settings(settings):
    """
    Return a list of dictionnaries

    Format :
    [
        {'query': lambda ..,
         'value': 'value',
         'rule_id': 'xxx'},
        {'query': lambda ...,
         'inherits_from: 'xxx',
         'rule_id': 'yyy'}
    ]
    """
    compiled_rules = []
    r = ResourceTypeSettingsValidator(settings)
    if not r.is_valid():
        # Raise an exception with errors ?
        return False

    for host, rules in settings.iteritems():
        for rule in rules:
            if host.startswith('*.'):
                host_query = 'ENDS(host, "%s")' % host[1:]
            else:
                host_query = 'host = "%s"' % host

            # if rule inherits from another one, we don't need to check again the host
            _rule = {'query': query_to_python(' AND '.join((host_query, rule['query'])) if not 'inherits_from' in rule else rule['query']),
                     'value': rule['value']
                     }
            for field in ('inherits_from', 'rule_id', 'abstract'):
                if field in rule:
                    _rule[field] = rule[field]
            compiled_rules.append(_rule)

    return compiled_rules


class ResourceTypeSettingsValidator(object):
    ALLOWED_RULES_FIELDS = ('query', 'value', 'abstract', 'rule_id', 'inherits_from')
    MANDATORY_RULES_FIELDS = ('query', 'value')

    """
    Validates a settings

    -- settings : a dictionnary where keys are hostnames and values are tuples of rules
        {'host1': (rule1, rule2...), 'host2': (rule3, rule4..)}

        Rule format :
        {'query': 'A BQL query',
         'value': 'a_value',
        }

        ====
        HOST
        ====
        We can add a wildcard (ex: *.mysite.com) at the beginning of an host to match multiple hosts.
        The wildcard must be directly followed by a dot.

        ====
        RULE
        ====
        A `rule_id` field can be set to let the rule being inherited in another one
        If the rule must not be matched but only its children, add `abstract` field to True

        The inherited rule set `inherits_from `field with the rule_id as value
    """
    def __init__(self, settings):
        self.settings = settings

    def _run(self):
        if hasattr(self, '_is_valid'):
            return None

        self._compiled_rules = []
        self._errors = {'host': [],
                        'query': [],
                        'field': []
                        }
        self._is_valid = False

        for host, rules in self.settings.iteritems():
            if re.search('^(.+)\*', host):
                self._errors['host'].append('Host %s should contains wildcard only at the beginning' % host)
            elif host.startswith('*') and not host.startswith('*.'):
                self._errors['host'].append('Wildcard at %s should be directly followed by a dot' % host)

            for i, rule in enumerate(rules):
                # Check fields names
                for field in rule.keys():
                    if field not in self.ALLOWED_RULES_FIELDS:
                        self._errors['field'].append('`%s` is not a valid field in host %s rule %d' % (field, host, i))

                # Check for mandatory fields
                for field in self.MANDATORY_RULES_FIELDS:
                    if field not in rule.keys():
                        self._errors['field'].append('`%s` is mandatory in host %s rule %d' % (field, host, i))

                # Check query validity
                if 'query' in rule:
                    _valid, _msg = validate_query_grammar(rule['query'])
                    if not _valid:
                        self._errors['query'].append('Error in query, host %s rule %d : %s' % (host, i, _msg))

        self._is_valid = len(self.errors) == 0

    def is_valid(self):
        self._run()
        return self._is_valid

    @property
    def errors(self):
        self._run()
        return [e for e in itertools.chain(*self._errors.values())]

    @property
    def host_errors(self):
        self._run()
        return self._errors['host']

    @property
    def query_errors(self):
        self._run()
        return self._errors['query']

    @property
    def field_errors(self):
        self._run()
        return self._errors['field']
