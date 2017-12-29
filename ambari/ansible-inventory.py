#!/usr/bin/env python

'''
Dynamic inventory script to pull component details from Ambari
'''

import os
import sys
import argparse
import json
import requests

class AmbariInventory(object):

    def __init__(self):
        # TODO: load settings from a config file
        self.cluster_name = 'Sandbox'
        self.ambari_host = 'localhost'
        self.uri = 'http://localhost:8080'
        self.ambari_user = 'admin'
        self.ambari_pass = 'admin'

        args = self.process_args()
        service_list = self.get_service_list()
        print service_list

        # Called with `--list`.
        if args.list:
            json.dumps(self.generate_ambari_inventory(service_list))
        elif args.host:
            json.dumps(self.generate_ambari_inventory(service_list))


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
        services = {
            'ambari': {
                'server': [self.ambari_host]
            }
        }

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
            'ambari-agent': {
                'children': ['hadoop']
            },
            '_meta': {
                'hostvars': {}
            }
        }

        # Loop over services
        for service_k, service_v in services.iteritems():
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
