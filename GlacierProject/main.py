from query import *

def prep_joined(glims_list):
	'''
	Gets a subset of joined
	:param glims_list: 
	'''
	joined = load_train_set('../data/joined/joined.shp')
	return joined[joined.glac_id.isin(glims_list)]

def single_glacier(glims_id, subset):
	queried = query(glims_id, subset)
	# sends init req to GEE for metadata
	# creates drive location, adds metadata
	# sends request to GEE

def run_pipeline(glims_list):
	train_set = prep_joined(glims_list)
	for glims_id in glims_list:
		single_glacier(glims_id, train_set)
