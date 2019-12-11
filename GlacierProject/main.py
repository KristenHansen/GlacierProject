from GlacierProject.query import *
from GlacierProject.gee import *

def prep_joined(glimsid_list, datadir):
	'''
	Gets a subset of joined to prepare for querying
	:param glimsid_list: list of GLIMS IDs to query
	:param datadir: data directory
	'''
	joined = load_train_set(datadir + 'joined/joined.shp')
	return joined[joined.glac_id.isin(glimsid_list)]

def single_glacier(glims_id, subset):
	'''
	Queries a dictionary of glacier data, requests data
	from GEE
	:param glims_id: GLIMS ID to query
	:param subset: subset training set from prep_joined
	'''
	queried = id_query(glims_id, subset)
	ee_download(glims_id, queried, landsat=True, dem=True, begDate='2000-01-01', endDate='2014-01-01')
	# sends init req to GEE for metadata
	# creates drive location, adds metadata
	# sends request to GEE

def run_pipeline(glimsid_list, datadir):
	'''
	Runs the data extraction pipeline
	:param glimsid_list: GLIMS IDs to pass through pipeline
	:param datadir: data directory
	'''
	train_set = prep_joined(glimsid_list, datadir)
	for glims_id in glimsid_list:
		single_glacier(glims_id, train_set)
