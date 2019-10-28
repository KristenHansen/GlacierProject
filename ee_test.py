#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 12:22:22 2017

@author: kylehasenstab
"""

#%%
import os
import ee
from os.path import join as pathjoin
home_path = '/Users/kylehasenstab/Documents/Biostat/GlacierDev/'
os.chdir(home_path + 'Code')
path = os.path.abspath('../')

#%%

MY_SERVICE_ACCOUNT = 'glaciersucsd@glaciersucsd.iam.gserviceaccount.com'
MY_PRIVATE_KEY_FILE = 'Code/GlaciersUCSD-7b59736d605b.json'

keyfile = pathjoin(path, MY_PRIVATE_KEY_FILE)
ee.Initialize(ee.ServiceAccountCredentials(MY_SERVICE_ACCOUNT, keyfile))