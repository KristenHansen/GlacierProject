def cloudscore(image):
    import ee
    try:
        ee.Initialize()
        print('The Earth Engine package initialized successfully!')
    except ee.EEException as e:
        print('The Earth Engine package failed to initialize!')
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise
    cloud = ee.Algorithms.Landsat.simpleCloudScore(image).select('cloud')
    cloudiness = cloud.reduceRegion(ee.Reducer.mean(),
                                    geometry=region,
                                    scale=30)
    image = image.set(cloudiness)
    return image


def ee_download(glacierID, glacierObject, begDate = '1984-01-01', endDate = '2019-01-01', cloud_tol = 15):
    #Earliest begin date is '1984-01-01'
    def cloudscore(image):
        import ee
        try:
            ee.Initialize()
            print('The Earth Engine package initialized successfully!')
        except ee.EEException as e:
            print('The Earth Engine package failed to initialize!')
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
        cloud = ee.Algorithms.Landsat.simpleCloudScore(image).select('cloud')
        cloudiness = cloud.reduceRegion(ee.Reducer.mean(),
                                        geometry=region,
                                        scale=30)
        image = image.set(cloudiness)
        return image

    from datetime import date
    import ee
    try:
        ee.Initialize()
        print('The Earth Engine package initialized successfully!')
    except ee.EEException as e:
        print('The Earth Engine package failed to initialize!')
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise
    #Prepare for Landsat Download
    region = ee.Geometry.Polygon(glacierObject['boundingbox'])
    #DEM Download
    DEM = ee.Image("USGS/SRTMGL1_003")
    DEM = DEM.clip(region)
    task = ee.batch.Export.image.toDrive(
        image = DEM.clip(region),
        scale = 30, region = region.bounds().getInfo()['coordinates'], folder = str(glacierObject['GlimsID']), fileNamePrefix = 'DEM')
    task.start()
    print("DEM sent to drive")

    #Landsat 8 download
    if date.fromisoformat(endDate) > date.fromisoformat("2013-01-01"):
        col = ee.ImageCollection('LANDSAT/LC08/C01/T1_TOA')
        col = col.filterDate('2013-01-01',endDate)
        col = col.filterBounds(region)
        count = col.size()

        withCloudiness = col.map(algorithm = cloudscore)

        filteredCollection = withCloudiness.filter(ee.Filter.lt('cloud', cloud_tol))
        filteredCollection = filteredCollection.select(['B1', 'B2', 'B3', 'B4', 'B5', 'B6'])
        collectionList = filteredCollection.toList(col.size())
        collectionSize = collectionList.size().getInfo()

        for i in range(collectionSize):
            image = ee.Image(collectionList.get(i)).clip(region)
            filename = image.get("DATE_ACQUIRED")

            #print(date.getInfo())
            task = ee.batch.Export.image.toDrive(
                image = ee.Image(collectionList.get(i)).clip(region),
                scale = 30, region = region.bounds().getInfo()['coordinates'], folder = str(glacierObject['GlimsID']), fileNamePrefix = str(filename.getInfo()))
            task.start()
        print("Landsat 8 images sent")
        glacierObject['L8Dates'] = filteredCollection.get("DATE_ACQUIRED").getInfo()
    # Landsat 7 download
    if date.fromisoformat("1999-01-01") > date.fromisoformat(begDate):
        col2 = ee.ImageCollection('LANDSAT/LE07/C01/T1_TOA')
        col2 = col2.filterDate(begDate,endDate)
        col2 = col2.filterBounds(region)
        count = col2.size()


    else:
        col2 = ee.ImageCollection('LANDSAT/LE07/C01/T1_TOA')
        col2 = col2.filterDate('1999-01-01',endDate)
        col2 = col2.filterBounds(region)
        count = col2.size()

    withCloudiness = col2.map(algorithm = cloudscore)

    filteredCollection = withCloudiness.filter(ee.Filter.lt('cloud', cloud_tol))
    filteredCollection = filteredCollection.select(['B1', 'B2', 'B3', 'B4', 'B5', 'B6_VCID_1'])
    collectionList = filteredCollection.toList(col2.size())
    collectionSize = filteredCollection.size().getInfo()

    for i in range(collectionSize):
        image = ee.Image(collectionList.get(i)).clip(region)
        filename = image.get("DATE_ACQUIRED")
        task = ee.batch.Export.image.toDrive(
            image = ee.Image(collectionList.get(i)).clip(region),
            scale = 30, region = region.bounds().getInfo()['coordinates'], folder = str(glacierObject['GlimsID']), fileNamePrefix = str(filename.getInfo()))
        task.start()
    print("Landsat 7 images sent")
    glacierObject['L7Dates'] = filteredCollection.get("DATE_ACQUIRED").getInfo()
    #Landsat 5 download
    if date.fromisoformat(endDate) < date.fromisoformat('2012-05-01'):
        col = ee.ImageCollection('LANDSAT/LT05/C01/T1_TOA')\
                .filterDate(begDate,endDate)\
                .filterBounds(region)

        count = col.size()
    else:

        col = ee.ImageCollection('LANDSAT/LT05/C01/T1_TOA')\
                .filterDate(begDate,'2012-05-01')\
                .filterBounds(region)

        count = col.size()

    withCloudiness = col.map(algorithm = cloudscore)

    filteredCollection = withCloudiness.filter(ee.Filter.lt('cloud', cloud_tol))
    filteredCollection = filteredCollection.select(['B1', 'B2', 'B3', 'B4', 'B5', 'B6'])
    collectionList = filteredCollection.toList(col2.size())
    collectionSize = filteredCollection.size().getInfo()

    for i in range(collectionSize):
        image = ee.Image(collectionList.get(i)).clip(region)
        filename = image.get("DATE_ACQUIRED")
        task = ee.batch.Export.image.toDrive(
            image = ee.Image(collectionList.get(i)).clip(region),
            scale = 30, region = region.bounds().getInfo()['coordinates'], folder = str(glacierObject['GlimsID']), fileNamePrefix = str(filename.getInfo()))
        task.start()
    print("Landsat 5 images sent")
    glacierObject['L5Dates'] = filteredCollection.get("DATE_ACQUIRED").getInfo()
    #Need to add a list of all dates of landsats to glacier glacierObject
    #Add an address of the files DEM and landsat
    glacierObject['fileaddress'] = str(glacierObject['GlimsID'])
    

dct = dict()
dct['GlimsID'] = "G098570E39226N"
dct['boundingbox'] = [[98.54723199999999, 39.210651], [98.59022299999999, 39.210651], [98.59022299999999, 39.243147], [98.54723199999999, 39.243147], [98.54723199999999, 39.210651]]


ee_download("G098570E39226N", dct, cloud_tol = 20)
