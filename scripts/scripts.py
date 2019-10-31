import pandas as pd
import geopandas as gpd
import fiona

def open_glims_shp(fp, cols, to_file=False, outp=None):
    '''Open glims shapefile'''
    file = fiona.open(fp)

    def records(usecols, r):
        source = file[r[0]: r[1]]
        for feature in source:
            f = {k: feature[k] for k in ['geometry']}
            f['properties'] = {k: feature['properties'][k] for k in usecols}
            yield f
    
    df = (
        gpd
        .GeoDataFrame
        .from_features(records(cols, [0, 50000]))
        .drop_duplicates('glac_id', keep='last')
    )
    
    c = 50001
    counter = 1

    while c != 900001:
        print('Finished Job', c)

        df = df.append(gpd.GeoDataFrame
                .from_features(records(cols, [c, c + 49999])), ignore_index=True)
                .drop_duplicates('glac_id', keep='last')
        c += 50000
    
    df = df.append(gpd.GeoDataFrame
                .from_features(records(cols, [900001, None])), ignore_index=True)
                .drop_duplicates('glac_id', keep='last')

    if to_file:
        df.to_file(outp)
    else:
        return df

def read_glims_gdf(fp, to_file=False, outp=None):
    '''Read in the glims shp file'''
    glims_gdf = gpd.read_file(fp)
    glims_gdf.crs = {'init' :'epsg:4326'}

    if to_file:
        glims_gdf.to_file(outp)
    else:
        return glims_gdf

def read_wgms_gdf(wAfp, wAAfp, to_file=False, outp=None):
    '''Read in the wgms file as a gpd'''
    wgmsA = pd.read_csv(wAfp, encoding='latin1')
    wgmsAA = pd.read_csv(wAAfp, encoding='latin1')

    wantedA = 'POLITICAL_UNIT NAME WGMS_ID LATITUDE LONGITUDE PRIM_CLASSIFIC'.split()
    wA = wgmsA.copy()[wantedA].replace('^UNNAMED .*|NO-.*', np.NaN,  regex=True)    # clean names

    wantedAA = 'POLITICAL_UNIT NAME WGMS_ID GLIMS_ID'.split()
    wAA = wgmsAA.copy()[wantedAA].replace('^UNNAMED .*|NO-.*', np.NaN,  regex=True) # clean names

    valley = wA.copy()[wA.PRIM_CLASSIFIC == 5]
    wgms = valley.merge(wAA)

    geometry = [Point(xy) for xy in zip(wgms.LONGITUDE, wgms.LATITUDE)]

    wgms_gdf = wgms.drop(['LONGITUDE', 'LATITUDE'], axis=1)
    crs = {'init': 'epsg:4326'}

    if to_file:
        wgms_gdf.to_file(outp)
    else:
        return gpd.GeoDataFrame(wgms, crs=crs, geometry=geometry)

def sjoin(glims_fp, wgms_fp, to_file=False, outp=None):
    '''Spatially join glims and wgms'''
    glims_gdf = gpd.read_file(glims_fp)
    wgms_gdf = gpd.read_file(wgms_fp)

    joined = gpd.sjoin(wgms_gdf, glims_gdf, op='within')

    if to_file:
        joined.to_file(outp)
    else:
        return joined

def load_train_set(fp):
    '''Load in training set for querying'''
    try:
        return gpd.read_file(fp)
    except:
        # download files, run previous code
        pass

def query(id, fp):
    '''Query info from given ID'''
    glims_gdf = load_train_set(fp)
    
    subset = glims_gdf[glims_gdf.glac_id == id]
    subset = (
        subset
        .assign(bbox=subset.envelope.astype(str),
                poly=subset.geometry.astype(str))
        .drop(columns=['anlys_time', 'geometry'])
    )
    return dict(subset.squeeze())