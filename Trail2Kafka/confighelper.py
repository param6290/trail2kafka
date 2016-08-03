__author__ = 'ravi.shekhar'

"""This is the config file parser module. The sole responsibility of this module is to
    1. act as the powerhouse of configuration.
    2. parse the ini file.
    3. provide the configuration dictionary to the overall application.

"""

import os
from ConfigParser import SafeConfigParser
from datetime import datetime

# Expose these entities from the module
__all__ = ['PollableConfiguration', 'NonPollableConfiguration']


# return the current date time
def _date_format():
    return datetime.now().strftime("%d%m%Y")


# return the project root.
def _project_root():
    return os.path.dirname(os.path.abspath(__file__))


# return the project configuration file absolute path.
def _project_config_path():
    return _project_root() + "/conf/" + "config.ini"


class ConfigurationBase(object):
    """Configuration Base class for the overall application.

    """

    def __init__(self):
        self._config_dict = {}

    # I want to make this method abstract and force every other subclass to define it their way.
    def parse_config(self):
        pass

    def get_conf_dict(self):
        pass


class PollableConfiguration(ConfigurationBase):
    """Pollable Configuration implementation class for overall application.

    """

    def __init__(self):
        """

        :param config_dict:
        :return:
        """
        super(NonPollableConfiguration, self).__init__()
        self._parser = SafeConfigParser()

    # private method
    def _parse_config(self):
        pass
        try:
            self._parser.read(_project_config_path())
        except:
            # log this.
            print("Issue in Configuration Parsing")
            pass

    # private method
    def _prepare_config_dict(self):
        self._config_dict["PROJECT_ROOT"] = _project_root()
        self._config_dict["FILE_NAME"] = self._parser.get('Application', 'FILE_NAME') + _date_format() + ".csv"
        self._config_dict["FILE_PATH"] = self._parser.get('Application', 'FILE_PATH')
        self._config_dict["QUEUE_MASTER_SIZE"] = self._parser.get('Application', 'QUEUE_MASTER_SIZE')
        self._config_dict["LOG_ROOT"] = self._parser.get('Log', 'LOG_ROOT') + '/'
        self._config_dict["LOG_DIRNAME"] = self._parser.get('Log', 'LOG_DIRNAME')
        self._config_dict["LOG_FILENAME"] = self._parser.get('Log', 'LOG_FILENAME')
        self._config_dict["LOG_LEVEL"] = self._parser.get('Log', 'LOG_LEVEL')
        self._config_dict["END_TIME"] = self._parser.get('Application', 'END_TIME')
        self._config_dict["ALARM_INTERVAL_SECONDS"] = int(self._parser.get('Application', 'ALARM_INTERVAL_SECONDS'))
        self._config_dict["CLOCK_REST_INTERVAL_SECONDS"] = int(
            self._parser.get('Application', 'CLOCK_REST_INTERVAL_SECONDS'))

        #  Parsing Kafka Properties.
        self._config_dict["KP_bootstrap_servers"] = self._parser.get('Kafka', 'bootstrap_servers')
        self._config_dict["KP_topic"] = self._parser.get('Kafka', 'topic')
        self._config_dict["KP_partition"] = self._parser.get('Kafka', 'partition')
        self._config_dict["KP_client_id"] = self._parser.get('Kafka', 'client_id')
        self._config_dict["KP_acks"] = self._parser.get('Kafka', 'acks')
        self._config_dict["KP_compression_type"] = self._parser.get('Kafka', 'compression_type')
        self._config_dict["KP_retries"] = self._parser.get('Kafka', 'retries')
        self._config_dict["KP_linger_ms"] = self._parser.get('Kafka', 'linger_ms')
        self._config_dict["KP_retries"] = self._parser.get('Kafka', 'retries')
        self._config_dict["KP_partitioner"] = self._parser.get('Kafka', 'partitioner')
        self._config_dict["KP_max_block_ms"] = self._parser.get('Kafka', 'max_block_ms')
        self._config_dict["KP_max_request_size"] = self._parser.get('Kafka', 'max_request_size')
        self._config_dict["KP_metadata_max_age_ms"] = self._parser.get('Kafka', 'metadata_max_age_ms')
        self._config_dict["KP_retry_backoff_ms"] = self._parser.get('Kafka', 'retry_backoff_ms')
        self._config_dict["KP_request_timeout_ms"] = self._parser.get('Kafka', 'request_timeout_ms')
        self._config_dict["KP_receive_buffer_bytes"] = self._parser.get('Kafka', 'receive_buffer_bytes')
        self._config_dict["KP_send_buffer_bytes"] = self._parser.get('Kafka', 'send_buffer_bytes')
        self._config_dict["KP_reconnect_backoff_ms"] = self._parser.get('Kafka', 'reconnect_backoff_ms')
        self._config_dict["KP_max_in_flight_requests_per_connection"] = \
            self._parser.get('Kafka', 'max_in_flight_requests_per_connection')

        # Parsing Logging Properties.
        self._config_dict["LOG_ROOT"] = self._parser.get('Log', 'LOG_ROOT')
        self._config_dict["LOG_DIRNAME"] = self._parser.get('Log', 'LOG_DIRNAME')
        self._config_dict["LOG_FILENAME"] = self._parser.get('Log', 'LOG_FILENAME')
        self._config_dict["LOG_LEVEL"] = self._parser.get('Log', 'LOG_LEVEL')

    def parse_config(self):
        """
        Client Facing: parse the configuration file actually.
        :return:
        """
        self._parse_config()
        self._prepare_config_dict()
        print self._config_dict


class NonPollableConfiguration(ConfigurationBase):
    """Pollable Configuration implementation class for overall application.

    """

    def __init__(self):
        """

        :param config_dict:
        :return:
        """
        super(NonPollableConfiguration, self).__init__()
        self._parser = SafeConfigParser()

    # private method
    def _parse_config(self):
        pass
        try:
            self._parser.read(_project_config_path())
        except:
            # log this.
            print("Issue in Configuration Parsing")
            pass

    # private method
    def _prepare_config_dict(self):
        self._config_dict["PROJECT_ROOT"] = _project_root()
        self._config_dict["FILE_NAME"] = self._parser.get('Application', 'FILE_NAME') + _date_format() + ".csv"
        self._config_dict["FILE_PATH"] = self._parser.get('Application', 'FILE_PATH')
        self._config_dict["MASTER_QUEUE_SIZE"] = self._parser.get('Application', 'MASTER_QUEUE_SIZE')
        self._config_dict["FAILED_BUCKET_QUEUE_SIZE"] = self._parser.get('Application', 'FAILED_BUCKET_QUEUE_SIZE')
        self._config_dict["LOG_ROOT"] = self._parser.get('Log', 'LOG_ROOT') + '/'
        self._config_dict["LOG_DIRNAME"] = self._parser.get('Log', 'LOG_DIRNAME')
        self._config_dict["LOG_FILENAME"] = self._parser.get('Log', 'LOG_FILENAME')
        self._config_dict["LOG_LEVEL"] = self._parser.get('Log', 'LOG_LEVEL')
        self._config_dict["END_TIME"] = self._parser.get('Application', 'END_TIME')
        self._config_dict["ALARM_INTERVAL_SECONDS"] = int(self._parser.get('Application', 'ALARM_INTERVAL_SECONDS'))
        self._config_dict["CLOCK_REST_INTERVAL_SECONDS"] = int(
            self._parser.get('Application', 'CLOCK_REST_INTERVAL_SECONDS'))

        #  Parsing Kafka Properties.
        self._config_dict["KP_topic"] = self._parser.get('Kafka', 'topic')
        self._config_dict["KP_partition"] = int(self._parser.get('Kafka', 'partition'))
        self._config_dict["KP_bootstrap_servers"] = self._parser.get('Kafka', 'bootstrap_servers')
        self._config_dict["KP_client_id"] = self._parser.get('Kafka', 'client_id')
        self._config_dict["KP_acks"] = int(self._parser.get('Kafka', 'acks'))
        self._config_dict["KP_compression_type"] = self._parser.get('Kafka', 'compression_type')
        self._config_dict["KP_retries"] = int(self._parser.get('Kafka', 'retries'))
        self._config_dict["KP_linger_ms"] = self._parser.get('Kafka', 'linger_ms')
        self._config_dict["KP_partitioner"] = self._parser.get('Kafka', 'partitioner')
        self._config_dict["KP_batch_size"] = int(self._parser.get('Kafka', 'batch_size'))
        self._config_dict["KP_buffer_memory"] = int(self._parser.get('Kafka', 'buffer_memory'))
        self._config_dict["KP_max_block_ms"] = self._parser.get('Kafka', 'max_block_ms')
        self._config_dict["KP_max_request_size"] = self._parser.get('Kafka', 'max_request_size')
        self._config_dict["KP_metadata_max_age_ms"] = self._parser.get('Kafka', 'metadata_max_age_ms')
        self._config_dict["KP_retry_backoff_ms"] = self._parser.get('Kafka', 'retry_backoff_ms')
        self._config_dict["KP_request_timeout_ms"] = self._parser.get('Kafka', 'request_timeout_ms')
        self._config_dict["KP_receive_buffer_bytes"] = self._parser.get('Kafka', 'receive_buffer_bytes')
        self._config_dict["KP_send_buffer_bytes"] = self._parser.get('Kafka', 'send_buffer_bytes')
        self._config_dict["KP_reconnect_backoff_ms"] = self._parser.get('Kafka', 'reconnect_backoff_ms')
        self._config_dict["KP_max_in_flight_requests_per_connection"] = \
            self._parser.get('Kafka', 'max_in_flight_requests_per_connection')

        # Parsing Logging Properties.
        self._config_dict["LOG_ROOT"] = self._parser.get('Log', 'LOG_ROOT')
        self._config_dict["LOG_DIRNAME"] = self._parser.get('Log', 'LOG_DIRNAME')
        self._config_dict["LOG_FILENAME"] = self._parser.get('Log', 'LOG_FILENAME')
        self._config_dict["LOG_LEVEL"] = self._parser.get('Log', 'LOG_LEVEL')

    def parse_config(self):
        """
        Client Facing: parse the configuration file actually.
        :return:
        """
        self._parse_config()
        self._prepare_config_dict()

    def get_conf_dict(self):
        return self._config_dict


if __name__ == "__main__":
    non_pollable_config = NonPollableConfiguration()
    non_pollable_config.parse_config()
    CONFIG_DICT = non_pollable_config.get_conf_dict()
    print CONFIG_DICT
