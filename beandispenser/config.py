from ConfigParser import ConfigParser, NoOptionError

class ConfigValueError(Exception):
    pass

class ConfigValue(object):

    def __init__(self, section, key, value):

        if key[:3] == 'on_' and value not in ['delete', 'bury', 'release']:
            raise ConfigValueError('[{0} {1}] value must be on of delete, bury, release'.format(section, key))

        self._section = section
        self._key = key
        self._value = value

    def __int__(self):
        if self._key in ['workers', 'port']:
            return int(self._value)
        raise ConfigValueTypeError()

    def __str__(self):
        return self._value

    def __repr__(self):
        return self._value        

    def __unicode__(self):
        return self._value


class Config(ConfigParser):
    """Just a simple way of getting None back if the config doesn't exist
    """
    def get(self, section, key, default=None):
        try:
            value = ConfigParser.get(self, section, key)
            return ConfigValue(section, key, value)
        except NoOptionError:
            return None