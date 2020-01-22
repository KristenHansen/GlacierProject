from GlacierProject.query import *
from GlacierProject.gee import *
import threading
from multiprocessing.dummy import Pool as ThreadPool

def authenticate():
	# Earth Engine authentication
	try:
		ee.Initialize()
		print('The Earth Engine package initialized successfully!')
	except ee.EEException:
		print('The Earth Engine package failed to initialize!')

	# Google Drive API authentication

def prep_joined(glimsid_list, datadir):
	'''
	Gets a subset of joined to prepare for querying
	:param glimsid_list: list of GLIMS IDs to query
	:param datadir: data directory
	'''
	joined = load_train_set(datadir + 'joined/joined.shp')
	return joined[joined.glac_id.isin(glimsid_list)]

def single_glacier(
	glims_id, 
	subset, 
	begDate='1984-01-01', 
    endDate='2019-01-01', 
    cloud_tol=20, 
    landsat=True, 
    dem=True):
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

def run_pipeline(glims_id_input, datadir, delim=None):
	'''
	Runs the data extraction pipeline
	:param glims_id_input: GLIMS IDs to pass through pipeline; either python list or text filepath
	:param datadir: data directory
	:param delim: delimiter to split on if glims_id_input is a text file
	'''
	# TODO: authenticate first

	if delim:
		# read in list from text file
		with open(glims_id_input) as fh:
			ids_list = fh.read().split(delim)
	else:
		ids_list = glims_id_input

	train_set = prep_joined(ids_list, datadir)
	pool = ThreadPool(2)
	pool.map(lambda glims_id: single_glacier(glims_id, train_set), ids_list)
