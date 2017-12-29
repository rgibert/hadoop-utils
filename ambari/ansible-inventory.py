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
        self.cluster_name = ''
        self.ambari_host = ''
        self.uri = ''
        self.ambari_user = ''
        self.ambari_pass = ''

        args = process_args()

        service_list = get_service_list()

        # Called with `--list`.
        if args.list:
            json.dumps(generate_ambari_inventory(service_list))
        elif args.host:


    def process_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--list', action = 'store_true')
        parser.add_argument('--host', action = 'store')
        return parser.parse_args()

    # Handle API calls to Ambari
    def ambari_get(self, path):
        return requests.get(uri + '/api/v1/clusters/' + self.cluster_name + path, auth=(ambari_user, ambari_pass), verify=False)

    # hosts/services/components from Ambari
    def get_service_list(self):
        services = {
            'ambari': {
                self.ambari_host
            }
        }

        for item in ambari_get('/services')['items']:
            services[item['ServiceInfo']['service_name']] = {}

        for service in services:
            for item in ambari_get('/services/' + service + '/components')['items']:
                component_name = item['ServiceComponentInfo']['component_name']

                services[service][component_name] = []

                for host in ambari_get('/services/' + service + '/components/' + component_name + '?fields=host_components/HostRoles/host_name')['host_components']:
                    services[service][component_name].append(host['HostRoles']['host_name'])

        return services

    def generate_ambari_inventory(self, services):
        # Default inventory
        inventory = {
            'all': {
                'hosts': []
            },
            cluster: {
                'children': []
            },
            'hadoop': {
                'children': [cluster]
            },
            'ambari-agent': {
                'children': ['hadoop']
            }
            '_meta': {
                'hostvars': {}
            }
        }

        # Loop over services
        for service_k, service_v in services.iteritems():
            # Add the service group as a child of the cluster group
            if cluster + '-' + service_k not in inventory[cluster]['children']:
                inventory[cluster]['children'].append(cluster + '-' + service_k)

            # Create a group for the service which will have all the service components as children
            inventory[cluster + '-' + service_k] = {
                'hosts' = [],
                'vars' = {}
            }

            inventory[service_k] = {
                'children': [cluster + '-' + service_k]
            }

            # Loop over components
            for component_k, component_v in service_v.iteritems():
                if cluster + '-' + service_k + '-' + component_k not in inventory[cluster + '-' + service_k]['children']:
                    inventory[cluster + '-' + service_k]['children'].append(cluster + '-' + service_k + '-' + component_k)

                inventory[cluster + '-' + service_k + '-' + component_k] = {
                    'hosts' = [],
                    'vars' = {}
                }

                inventory[service_k + '-' + component_k] = {
                    'children': [cluster + '-' + service_k + '-' + component_k]
                }

                for host in component_v:
                    inventory[cluster + '-' + service_k + '-' + component_k]['hosts'].append(host)

                    if host not in inventory['all']['hosts']:
                        inventory['all']['hosts'].append(host)

        return inventory

if __name__ == "__main__":
    AmbariInventory()
