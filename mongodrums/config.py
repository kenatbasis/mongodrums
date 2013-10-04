from deltaburke import ConfigManager

CONFIG_NAMESPACE = '__mongodrums__'

ConfigManager().load(
    {
        'instrument': {
            'sample_frequency': 0.1,
            'filter_packages': ['pymongo', 'mongoengine']
        },
        'collector': {
            'addr': '127.0.0.1',
            'port': 63333,
            'session': None,
            'mongo_uri': 'mongodb://127.0.0.1:27017/mongodrums_profile'
        },
        'pusher': {
            'addr': '127.0.0.1',
            'port': 63333
        },
        'index_profile_sink': {
            'mongo_uri': 'mongodb://127.0.0.1:27017/mongodrums_profile'
        },
        'query_profile_sink': {
            'mongo_uri': 'mongodb://127.0.0.1:27017/mongodrums_profile'
        }
    }, False, CONFIG_NAMESPACE)

def get_config():
    """ Get the current config

    """
    return ConfigManager().get_config(CONFIG_NAMESPACE)


def configure(sources, notify=True):
    """ Configure pymongo instrumentation

    :param sources:     load configuration from sources where sources can be a
                        list containing dicts or config URIs that will be
                        queried

    """
    ConfigManager().load(sources, notify, CONFIG_NAMESPACE)


def update(sources, notify=True):
    """ Update pymongo instrumentation configuration

    :param sources:     load configuration from sources where sources can be a
                        list containing dicts or config URIs that will be
                        queried, and merge them into the current config

    """
    ConfigManager().merge(sources, notify, CONFIG_NAMESPACE)


def register_update_callback(callback):
    """ Register callbacks for config changes

    :param callback:    the callback to be called on config update. it will
                        be called with the updated config as the first and only
                        argument

    """
    ConfigManager().register_update_callback(callback, CONFIG_NAMESPACE)


def unregister_update_callback(callback):
    """ Unregister callbacks for config changes

    :param callback:    the callback to unregister

    """
    ConfigManager().unregister_update_callback(callback, CONFIG_NAMESPACE)
