import pandas as pd
import geopandas as gpd
import fiona

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
    df = df.append(gpd.GeoDataFrame
                .from_features(records(cols, [c, None])), ignore_index=True)
                .drop_duplicates('glac_id', keep='last')

    if outp:
        df.crs = {'init' :'epsg:4326'}
        df.to_file(outp)
    
    return df

def read_glims_gdf(fp, outp=None):
    '''
    Read in the glims shp file
    :param fp: fp of either glims_gdf.shp or glims_polygons.shp
    :param outp: outp of glims_gdf.shp if fp == glims_polygons.shp
    '''
    if outp:                                        
        glims_gdf = open_glims_shp(fp, outp=outp)       # opens the raw shp file
    else:
        glims_gdf = gpd.read_file(fp)                   # reads in cleaned shp file
    
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

    if outp:
        wgms_gdf.to_file(outp)
    
    return gpd.GeoDataFrame(wgms, crs=crs, geometry=geometry)

def sjoin(glims_gdf=None, wgms_gdf=None, glims_fp=None, wgms_fp = None, outp=None):
    '''
    Spatially join glims and wgms
    :param glims_gdf: glims_gdf output from read_glims_gdf()
    :param wgms_gdf: wgms_gdf output from read_wgms_gdf()
    :param glims_fp: fp of glims_gdf
    :param wgms_fp: fp of wgms_gdf
    :param outp: outp of joined.shp
    '''
    # if input are filepaths not df objects
    if glims_fp:
        glims_gdf = read_glims_gdf(glims_fp)
        wgms_gdf = read_wgms_gdf(wgms_fp)

    joined = gpd.sjoin(wgms_gdf, glims_gdf, op='intersects')

    if outp:
        joined.to_file(outp)
    
    return joined

def load_train_set(fp, cols=['colnames']):
    '''
    Load in training set for querying
    :param fp: fp of joined.shp
    :param cols: columns to load
    '''
    try:
        return gpd.read_file(fp)[cols]
    except:
        glims_gdf = read_glims_gdf()                # load glims gdf
        wgms_gdf = read_wgms_gdf()                  # load wgms gdf
        joined = sjoin(glims_gdf, wgms_gdf)         # spatial join
        pass

def query(id, cols=['colnames'], joined=None, fp=None):
    '''
    Query info from given ID
    :param id: glims ID to query
    :param joined: joined gdf
    :param fp: fp of joined.shp
    :param cols: columns to load
    '''
    joined = load_train_set(fp)[cols]
    
    subset = glims_gdf[glims_gdf.glac_id == id]
    subset = (
        subset
        .assign(bbox=subset.envelope.astype(str),
                poly=subset.geometry.astype(str))
        .drop(columns=['anlys_time', 'geometry'])
    )
    return dict(subset.squeeze())

if __name__ == '__main__':
    # glims_gdf = read_glims_gdf()                # load glims gdf
    # wgms_gdf = read_wgms_gdf()                  # load wgms gdf
    # joined = sjoin(glims_gdf, wgms_gdf)         # spatial join
    glac_dict = query(id, joined)               # query ID info from joined



