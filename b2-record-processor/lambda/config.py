# -*- coding: utf-8 -*-
# Ecoation RawProcessor Configuration
# 
# Created by: Farzad Khandan (farzadkhandan@ecoation.com)
#
from base.cloud_provider import CloudProviderFactory
from base.proxy import RecordProcessorProxy

from providers.aws import aws

# System cloud provider
CLOUD_PROVIDER_NAME = 'aws'

# Cloud Providers
# register new cloud providers here
CloudProviderFactory.register('aws', aws)

# Cloud configuration
SYS_CLOUD_PROVIDER = CloudProviderFactory.get_provider(CLOUD_PROVIDER_NAME)

# System Proxy
SYS_PROXY = RecordProcessorProxy
SYS_PROXY.register_cloud_provider(SYS_CLOUD_PROVIDER)

