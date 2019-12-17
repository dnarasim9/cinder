# All Rights Reserved.
# Copyright 2019 HoneycombData Inc

#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Volume driver for HoneycombData HStor Storage Array.

"""

import functools
import re
import requests
import six

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import units

from cinder import exception
from cinder.i18n import _
from cinder.volume.drivers.san import san

DRIVER_VERSION = "1.0.0"
REST_API_VERSION = 1

LOG = logging.getLogger(__name__)

hcd_opts = [
    cfg.StrOpt('hcd_svip', help='Overrides default cluster SVIP with the one specified.'),
    cfg.StrOpt('hcd_client_id', help='HoneycombData HStor client ID for auth'),
    cfg.StrOpt('hcd_client_secret', help='HoneycombData HStor client secret for auth')
]

opt_group = cfg.OptGroup(name='honeycombdata-hstor')
cfg.CONF.register_group(opt_group)
cfg.CONF.register_opts(hcd_opts, group=opt_group)


class HoneycombDataDriverException(exception.VolumeDriverException):
    message = _("HoneycombData Cinder Driver exception")


class HoneycombDataAPIException(exception.VolumeBackendAPIException):
    message = _("Bad response from HoneycombData API")


class HoneycombDataVolumeBusyException(exception.VolumeIsBusy):
    message = _("HoneycombData Cinder Driver: Volume Busy")


class HoneycombDataDuplicateVolumeNameException(exception.Duplicate):
    message = _("Detected more than one volume with name %(vol_name)s")


class HoneycombDataRetryableException(exception.VolumeBackendAPIException):
    message = _("Retryable HoneycombData Exception encountered")


class HoneycombDataBaseVolumeDriver(san.SanDriver):

    """OpenStack driver to enable HoneycombData HStor controller"""

    VERSION = DRIVER_VERSION

    # ThirdPartySystems wiki page
    CI_WIKI_NAME = "HoneycombData_HStor_CI"

    def __init__(self, *args, **kwargs):
        super(HoneycombDataBaseVolumeDriver, self).__init__(*args, **kwargs)
        self.hcd_client = None
        self.verify = False

    @staticmethod
    def get_driver_options():
        return hcd_opts

    def _check_config(self):
        """Ensure that the cinder config file has the required fields"""
        required_config = ['san_ip', 'san_login', 'san_password', 'hcd_client_id', 'hcd_client_secret', 'hcd_svip']
        for attr in required_config:
            if not getattr(self.configuration, attr, None):
                raise exception.InvalidInput(reason=_('%s is not set.') % attr)

    def do_setup(self, context):
        """Setup the HoneycombData HStor REST API client"""
        self._check_config()
        # Setup hcd API client
        try:
            self.hcd_client = HoneycombDataRestClient(
                mvip=self.configuration.san_ip,
                username=self.configuration.san_login,
                password=self.configuration.san_password,
                client_id=self.configuration.hcd_client_id,
                client_secret=self.configuration.hcd_client_secret,
                svip=self.configuration.hcd_svip,
                verify=self.verify)
        except Exception:
            LOG.error('Failed to create REST client. Check san ip, username, password '
                      'and make sure the HStor version is compatible')
            raise

    def _get_model_info(self, volume_name):
        """Get model info for the volume"""
        return (
            {'provider_location': self._get_provider_location(volume_name),
             'provider_auth': None})

    def create_volume(self, volume):
        """Create a new volume."""
        self.hcd_client.create_vol(volume)
        return self._get_model_info(volume['name'])

    # TODO: Need to implement the below abstract methods
    def delete_volume(self, volume):
        pass

    def create_cloned_volume(self, volume, src_vref):
        pass

    def initialize_connection(self, volume, connector):
        pass

    def terminate_connection(self, volume, connector, **kwargs):
        pass

    def failover_host(self, context, volumes, secondary_id=None, groups=None):
        pass

    def failover_completed(self, context, active_backend_id=None):
        pass

    def enable_replication(self, context, group, volumes):
        pass

    def disable_replication(self, context, group, volumes):
        pass

    def failover_replication(self, context, group, volumes, secondary_backend_id=None):
        pass

    def get_replication_updates(self, context):
        pass

    def create_group(self, context, group):
        pass

    def delete_group(self, context, group, volumes):
        pass

    def update_group(self, context, group, add_volumes=None, remove_volumes=None):
        pass

    def create_group_from_src(self, context, group, volumes, group_snapshot=None, snapshots=None, source_group=None,
                              source_vols=None):
        pass

    def create_group_snapshot(self, context, group_snapshot, snapshots):
        pass

    def delete_group_snapshot(self, context, group_snapshot, snapshots):
        pass

    def create_volume_from_backup(self, volume, backup):
        pass


def _connection_checker(func):
    """Decorator to re-establish and re-run the api if session has expired"""

    @functools.wraps(func)
    def recursive_connection_checker(self, *args, **kwargs):
        for attempts in range(2):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                if attempts < 1 and (re.search("Failed to execute",
                                               six.text_type(e))):
                    LOG.info('Session might have expired, Trying to relogin...')
                    self.get_access_token()
                    continue
                else:
                    LOG.error('Re-throwing Exception %s', e)
                    raise

    return recursive_connection_checker


class HoneycombDataRestClient(object):

    """Invokes HoneycombData's HStor REST API calls"""

    def __init__(self, api_version=REST_API_VERSION, *args, **kwargs):
        self.access_token = None
        self.headers = None
        self.cluster_id = None
        self.mvip = kwargs['mvip']
        self.username = kwargs['username']
        self.password = kwargs['password']
        self.client_id = kwargs['client_id']
        self.client_secret = kwargs['client_secret']
        self.svip = kwargs['svip']
        self.verify = kwargs['verify']
        self.api_version = 'v{}'.format(api_version)
        self.mgmt_server_port = 8443
        self.base_endpoint = 'https://{}:{}'.format(self.mvip, self.mgmt_server_port)
        self.access_token_endpoint = self.base_endpoint + '/oauth/token'
        self.clusters_endpoint = self.base_endpoint + '/{}/clusters'.format(self.api_version)
        self.volumes_endpoint = self.base_endpoint + '/{}/volumes'.format(self.api_version)
        self.get_access_token()
        self.create_cluster()

    def get_access_token(self):
        response = requests.post(self.access_token_endpoint,
                                 verify=self.verify,
                                 auth=(self.client_id, self.client_secret),
                                 data={'grant_type': 'password', 'username': self.username,
                                       'password': self.password}
                                 )
        if response.status_code != 200:
            msg = _("Failed to login for user %s"), self.username
            raise HoneycombDataAPIException(msg)
        self.access_token = response.json()['access_token']
        self.headers = {'authorization': 'Bearer {}'.format(self.access_token)}

    def create_cluster(self):
        data = {
            "cluster": {
                "clusterName": 'cinder-cluster',
                "minClusterSize": 3,
                "replicationFactor": 3,
                "virtualIp": self.svip,
                "type": 0
            }
        }
        json_response = self.post(endpoint=self.clusters_endpoint, data=data)
        self.cluster_id = json_response.get('clusterId')
        return json_response

    def create_vol(self, volume):
        data = {
            "clusterId": self.cluster_id,
            "volumeName": volume.get('display_name', ''),
            "volumeSize": int(volume.size * units.Gi),
            "blockSize": 4096
        }
        json_response = self.post(endpoint=self.volumes_endpoint, data=data)
        return json_response

    @_connection_checker
    def post(self, endpoint, data):
        response = requests.post(endpoint,
                                 verify=self.verify,
                                 headers=self.headers,
                                 json=data
                                 )
        if response.status_code not in [200, 201, 202]:
            msg = _("Failed to execute api {} : {} : {}").format(endpoint, response.json()['message'],
                                                                 response.status_code)
            raise HoneycombDataAPIException(msg)
        return response.json()
