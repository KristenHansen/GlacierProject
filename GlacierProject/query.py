'''
TODO: query.py does _________________-
'''

import pandas as pd
import numpy as np
import geopandas as gpd
import fiona
from shapely.geometry import Point

def open_glims_shp(fp, cols, outp=None, chunksize=50000):
    '''
    Open glims shapefile, keeping only most recent observations

    :param fp: filepath to glims_polygons.shp
    :param cols: columns to keep
    :param outp: output filepath
    :param chunksize: chunksize for reading in shapefile in fiona
    '''

    file = fiona.open(fp)
    
    def reader(file, cols, chunksize):
        ''' 
        Read in shapefile, keeping only columns in usecols 

        :param cols: columns to keep
        :param r: portions of file to open
        '''
        out = []
        for k, feature in enumerate(file):
            if (k + 1) % chunksize:
                f = {k: feature[k] for k in ['geometry']}
                f['properties'] = {k: feature['properties'][k] for k in cols}
                out.append(f)
            else:
                yield gpd.GeoDataFrame.from_features(out).drop_duplicates('glac_id', keep='last')
                out = []
                
    for chunk in reader(file, cols, chunksize):
        try:
            glims = glims.append(chunk, ignore_index=True)
        except NameError:
            glims = chunk
    
    glims.crs = {'init' :'epsg:4326'}

    if outp:
        glims.to_file(outp)
    
    return glims

def read_glims_gdf(fp, usecols=None, outp=None):
    '''
    Read in the glims shapefile
    :param fp: filepath of either glims_gdf.shp or glims_polygons.shp
    :param outp: outut filepath of glims_gdf.shp if fp == glims_polygons.shp
    '''
    if outp:                                        
        glims_gdf = open_glims_shp(fp, usecols, outp=outp)       # opens the raw shp file
    else:
        glims_gdf = gpd.read_file(fp)                            # reads in cleaned shp file
        glims_gdf.crs = {'init' :'epsg:4326'}
    
    return glims_gdf

def read_wgms_gdf(*filepaths, outp=None):
    '''
    Read in the wgms file as a GeoDataFrame
    :param filepaths: filepaths of wanted wgms datasets
    :param outp: output filepath of wgms_gdf
    '''
    for f in filepaths:
        df = pd.read_csv(f, encoding='latin1')
        try:
            wgms = wgms.merge(df)
        except NameError:
            wgms = df
            
    geometry = [Point(xy) for xy in zip(wgms['LONGITUDE'], wgms['LATITUDE'])]
    crs = {'init': 'epsg:4326'}

    wgms_gdf = gpd.GeoDataFrame(wgms, crs=crs, geometry=geometry)

    if outp:
        wgms_gdf.to_file(outp)

    return wgms_gdf

def sjoin(glims_gdf=None, wgms_gdf=None, glims_fp=None, wgms_fp = None, outp=None):
    '''
    Spatially join glims and wgms datasets
    :param glims_gdf: glims_gdf output from read_glims_gdf()
    :param wgms_gdf: wgms_gdf output from read_wgms_gdf()
    :param glims_fp: filepath of glims_gdf
    :param wgms_fp: list filepaths for wgms_gdf (wA and wAA)
    :param outp: output filepath of joined.shp
    '''
    # if input are filepaths not df objects
    if glims_fp:
        glims_gdf = read_glims_gdf(glims_fp)
        wAfp, wAAfp = wgms_fp
        wgms_gdf = read_wgms_gdf(wAfp, wAAfp)

    joined = gpd.sjoin(glims_gdf, wgms_gdf, op='intersects')

    if outp:
        joined.to_file(outp)
    
    return joined

def load_train_set(fp):
    '''
    Load in training set for querying
    :param fp: filepath of joined.shp
    '''
    try:
        return gpd.read_file(fp)
    except:
        # incomplete
        # glims_gdf = read_glims_gdf()                # load glims gdf
        # wgms_gdf = read_wgms_gdf()                  # load wgms gdf
        # joined = sjoin(glims_gdf, wgms_gdf)         # spatial join
        # os.mkdir('../data/joined')
        # joined.to_file(fp)
        # return joined
        return

def id_query(glims_id, subset):
    '''
    Query info from given ID
    :param id: glims ID to query
    :param subset: subset of joined GeoDataFrame
    '''
    subs = subset[subset.glac_id == glims_id]
    coords = list(zip(*np.asarray(subs.geometry.squeeze().exterior.coords.xy)))
    bbox = list(zip(*np.asarray(subs.envelope.squeeze().exterior.coords.xy)))
    to_drop = ['geometry', 'GLIMS_ID', 'LATITUDE', 'LONGITUDE', 'WGMS_ID']
    dct = dict(subs.drop(columns=to_drop).squeeze())
    dct['coords'] = coords
    dct['bbox'] = bbox
    return dct

if __name__ == '__main__':
    # example
    glims_fp = 'data/glims_polys/glims_polys.shp'
    wA_fp = 'data/WGMS-FoG-2018-11-A-GLACIER.csv'
    wAA_fp = 'data/WGMS-FoG-2018-11-AA-GLACIER-ID-LUT.csv'
    glims_gdf = read_glims_gdf(glims_fp)                     # load glims gdf
    wgms_gdf = read_wgms_gdf(wA_fp, wAA_fp)                  # load wgms gdf
    joined = sjoin(glims_gdf, wgms_gdf)                      # spatial join
    glac_dict = id_query('G222647E59132N', joined)               # query ID info from joined



