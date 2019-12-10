import pandas as pd
import numpy as np
import geopandas as gpd
import fiona
from shapely.geometry import Point

def open_glims_shp(fp, usecols, outp=None, chunksize=50000):
    '''
    Open glims shapefile, keeping only most recent observations

    :param fp: fp to glims_polygons.shp
    :param usecols: columns to keep
    :param outp: outpath
    :param chunksize: chunksize for reading in shp file in fiona
    '''
    file = fiona.open(fp)

    def records(usecols, r):
        ''' 
        read in shp file, keeping only cols in usecols 

        :param usecols: columns to keep
        :param r: portions of file to open
        '''
        source = file[r[0]: r[1]]
        for feature in source:
            f = {k: feature[k] for k in ['geometry']}
            f['properties'] = {k: feature['properties'][k] for k in usecols}
            yield f
    
    # first df
    df = (
        gpd
        .GeoDataFrame
        .from_features(records(usecols, [0, chunksize]))
        .drop_duplicates('glac_id', keep='last')
    )
    
    # append chunks, dropping duplicates, until last chunk
    c = chunksize + 1
    to_append = (
        gpd.GeoDataFrame
        .from_features(records(usecols, [c, c + chunksize - 1]))
    )

    while len(to_append) == chunksize:
        df = df.append(to_append, ignore_index=True).drop_duplicates('glac_id', keep='last')
        c += chunksize

    # last chunk
    df = (
        df
        .append(gpd.GeoDataFrame.from_features(records(usecols, [c, None])), ignore_index=True)
        .drop_duplicates('glac_id', keep='last')
    )

    if outp:
        df.crs = {'init' :'epsg:4326'}
        df.to_file(outp)
    
    return df

def read_glims_gdf(fp, usecols=None, outp=None):
    '''
    Read in the glims shp file
    :param fp: fp of either glims_gdf.shp or glims_polygons.shp
    :param outp: outp of glims_gdf.shp if fp == glims_polygons.shp
    '''
    if outp:                                        
        glims_gdf = open_glims_shp(fp, usecols, outp=outp)       # opens the raw shp file
    else:
        glims_gdf = gpd.read_file(fp)                   # reads in cleaned shp file
        glims_gdf.crs = {'init' :'epsg:4326'}
    
    return glims_gdf

def read_wgms_gdf(wAfp, wAAfp, outp=None):
    '''
    Read in the wgms file as a gpd
    :param wAfp: fp of wgms A
    :param wAAfp: fp of wgms AA
    :param outp: outp of wgms_gdf
    '''
    wgmsA = pd.read_csv(wAfp, encoding='latin1')        # wgms A df
    wgmsAA = pd.read_csv(wAAfp, encoding='latin1')      # wgms AA df
    clean_regex = '^UNNAMED .*|NO-.*'

    # select wanted columns from wgms A
    wantedA = 'POLITICAL_UNIT NAME WGMS_ID LATITUDE LONGITUDE PRIM_CLASSIFIC'.split()
    wA = wgmsA.copy()[wantedA].replace(clean_regex, np.NaN,  regex=True)    # clean names

    # select wanted columns from wgms AA
    wantedAA = 'POLITICAL_UNIT NAME WGMS_ID GLIMS_ID'.split()
    wAA = wgmsAA.copy()[wantedAA].replace(clean_regex, np.NaN,  regex=True) # clean names

    valley = wA.copy()[wA.PRIM_CLASSIFIC == 5]                              # get valley glaciers
    wgms = valley.merge(wAA)

    geometry = [Point(xy) for xy in zip(wgms.LONGITUDE, wgms.LATITUDE)]     # convert to gdf
    wgms_gdf = wgms.drop(['LONGITUDE', 'LATITUDE'], axis=1)
    crs = {'init': 'epsg:4326'}
    
    wgms_gdf = gpd.GeoDataFrame(wgms, crs=crs, geometry=geometry)

    if outp:
        wgms_gdf.to_file(outp)
    
    return wgms_gdf

def sjoin(glims_gdf=None, wgms_gdf=None, glims_fp=None, wgms_fp = None, outp=None):
    '''
    Spatially join glims and wgms
    :param glims_gdf: glims_gdf output from read_glims_gdf()
    :param wgms_gdf: wgms_gdf output from read_wgms_gdf()
    :param glims_fp: fp of glims_gdf
    :param wgms_fp: list fps for wgms_gdf (wA and wAA)
    :param outp: outp of joined.shp
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
    :param fp: fp of joined.shp
    '''
    try:
        return gpd.read_file(fp)
    except:
        # incomplete
        # glims_gdf = read_glims_gdf()                # load glims gdf
        # wgms_gdf = read_wgms_gdf()                  # load wgms gdf
        # joined = sjoin(glims_gdf, wgms_gdf)         # spatial join
        # joined.to_file(fp)
        # return joined
        return

def id_query(glims_id, subset):
    '''
    Query info from given ID
    :param id: glims ID to query
    :param subset: subset of joined gdf
    '''
    subs = subset[subset.glac_id == glims_id]
    coords = list(zip(*np.asarray(subs.geometry.squeeze().exterior.coords.xy)))
    bbox = list(zip(*np.asarray(subs.envelope.squeeze().exterior.coords.xy)))
    to_drop = ['geometry', 'GLIMS_ID', 'LATITUDE', 'LONGITUDE', 'index_right', 'WGMS_ID', 'PRIM_CLASSIFIC']
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



