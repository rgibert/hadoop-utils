#!/usr/bin/env python

'''
Dynamic inventory script to pull component details from Ambari
'''

import os
import sys
import argparse
import json
import requests
import re

class AmbariInventory(object):

    def __init__(self):
        # Get the cluster name from the OS environment variable AMBARI_CLUSTER_NAME
        self.cluster_name = os.environ['AMBARI_CLUSTER_NAME']
        self.uri = os.environ['AMBARI_URI']
        self.ambari_user = os.environ['AMBARI_USER_NAME']
        self.ambari_pass = os.environ['AMBARI_USER_PASS']

        args = self.process_args()
        service_list = self.get_service_list()
        #service_list = json.load(open('ansible-inventory-sample.json'))

        ambari_inv = self.generate_ambari_inventory(service_list)

        # Called with `--list`.
        if args.list:
            print json.dumps(ambari_inv)
        elif args.host:
            # Not implemented, we're embedding _meta in --list
            print json.dumps(ambari_inv)


    def process_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--list', action = 'store_true')
        parser.add_argument('--host', action = 'store')
        return parser.parse_args()

    # Handle API calls to Ambari
    def ambari_get(self, path):
        return requests.get(self.uri + '/api/v1/clusters/' + self.cluster_name + path, auth=(self.ambari_user, self.ambari_pass), verify=False)

    # hosts/services/components from Ambari
    def get_service_list(self):
        services = {}

        result = self.ambari_get('/services')

        if result.status_code == requests.codes.ok:
            for item in json.loads(result.text)['items']:
                services[item['ServiceInfo']['service_name']] = {}

            for service in services:
                result = self.ambari_get('/services/' + service + '/components')

                if result.status_code == requests.codes.ok:
                    for item in json.loads(result.text)['items']:
                        component_name = item['ServiceComponentInfo']['component_name']
                        services[service][component_name] = []

                        result = self.ambari_get('/services/' + service + '/components/' + component_name + '?fields=host_components/HostRoles/host_name')

                        if result.status_code == requests.codes.ok:
                            for host in json.loads(result.text)['host_components']:
                                services[service][component_name].append(host['HostRoles']['host_name'])

        return services

    def generate_ambari_inventory(self, services):
        ambari_host = re.sub(r'https?:\\', '', self.uri)
        ambari_host = re.sub(r':\d+', '', ambari_host)

        # Default inventory
        inventory = {
            'all': {
                'hosts': []
            },
            self.cluster_name: {
                'children': []
            },
            'hadoop': {
                'children': [self.cluster_name]
            },
            'ambari': {
                'agent': {
                    'children': ['hadoop']
                },
                'server': {
                    'hosts': [ambari_host]
                }
            },
            '_meta': {
                'hostvars': {}
            }
        }

        # Loop over services
        for service_k, service_v in services.iteritems():
            # convert service_k & _v to lower case
            service_k = service_k.lower()

            # Add the service group as a child of the cluster group
            if self.cluster_name + '-' + service_k not in inventory[self.cluster_name]['children']:
                inventory[self.cluster_name]['children'].append(self.cluster_name + '-' + service_k)

            # Create a group for the service which will have all the service components as children
            inventory[self.cluster_name + '-' + service_k] = {
                'children': [],
                'vars': {}
            }

            inventory[service_k] = {
                'children': [self.cluster_name + '-' + service_k]
            }

            # Loop over components
            for component_k, component_v in service_v.iteritems():
                # convert component_k & _v to lower case
                component_k = component_k.lower()

                # strip doubling up names of services/components (eg: hdfs-hdfs_client becomes hdfs-client)
                component_k = re.sub(service_k + '_', '', component_k)

                # rename component / service name duplication for clients
                if (service_k == 'pig' and component_k == 'pig') or (service_k == 'slider' and component_k == 'slider') or (service_k == 'sqoop' and component_k == 'sqoop'):
                    component_k = 'client'

                elif (service_k == 'infra_solr' and component_k = 'infra_solr'):
                    component_k = 'infra_solr_server'

                if self.cluster_name + '-' + service_k + '-' + component_k not in inventory[self.cluster_name + '-' + service_k]['children']:
                    inventory[self.cluster_name + '-' + service_k]['children'].append(self.cluster_name + '-' + service_k + '-' + component_k)

                inventory[self.cluster_name + '-' + service_k + '-' + component_k] = {
                    'hosts': [],
                    'vars': {}
                }

                inventory[service_k + '-' + component_k] = {
                    'children': [self.cluster_name + '-' + service_k + '-' + component_k]
                }

                for host in component_v:
                    inventory[self.cluster_name + '-' + service_k + '-' + component_k]['hosts'].append(host)

                    if host not in inventory['all']['hosts']:
                        inventory['all']['hosts'].append(host)

        return inventory

if __name__ == "__main__":
    AmbariInventory()
