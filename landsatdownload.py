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


def ee_download(glacierID, glacierObject, begDate = '1984-01-01', endDate = '2019-01-01', cloud_tol = 15, landsat = True, dem = True):
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
    import csv
    from pydrive.auth import GoogleAuth
    from pydrive.drive import GoogleDrive
    import os
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
    if dem == True:
        DEM = ee.Image("USGS/SRTMGL1_003")
        DEM = DEM.clip(region)
        task = ee.batch.Export.image.toDrive(
            image = DEM.clip(region),
            scale = 30, region = region.bounds().getInfo()['coordinates'], folder = str(glacierObject['GlimsID']), fileNamePrefix = 'DEM')
        task.start()
        print("DEM sent to drive")
    if landsat == True:
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
            L8Dates = []
            for i in range(collectionSize):
                image = ee.Image(collectionList.get(i)).clip(region)
                filename = image.get("DATE_ACQUIRED")
                L8Dates.append(str(filename.getInfo()))
                task = ee.batch.Export.image.toDrive(
                    image = ee.Image(collectionList.get(i)).clip(region),
                    scale = 30, region = region.bounds().getInfo()['coordinates'], folder = str(glacierObject['GlimsID']), fileNamePrefix = str(filename.getInfo()))
                task.start()
            print("Landsat 8 images sent")
            glacierObject['L8Dates'] = L8Dates
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
        L7Dates = []
        for i in range(collectionSize):
            image = ee.Image(collectionList.get(i)).clip(region)
            filename = image.get("DATE_ACQUIRED")
            L7Dates.append(str(filename.getInfo()))
            task = ee.batch.Export.image.toDrive(
                image = ee.Image(collectionList.get(i)).clip(region),
                scale = 30, region = region.bounds().getInfo()['coordinates'], folder = str(glacierObject['GlimsID']), fileNamePrefix = str(filename.getInfo()))
            task.start()
        print("Landsat 7 images sent")
        glacierObject['L7Dates'] = L7Dates
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
        L5Dates = []
        for i in range(collectionSize):
            image = ee.Image(collectionList.get(i)).clip(region)
            filename = image.get("DATE_ACQUIRED")
            L5Dates.append(str(filename.getInfo()))
            task = ee.batch.Export.image.toDrive(
                image = ee.Image(collectionList.get(i)).clip(region),
                scale = 30, region = region.bounds().getInfo()['coordinates'], folder = str(glacierObject['GlimsID']), fileNamePrefix = str(filename.getInfo()))
            task.start()
        print("Landsat 5 images sent")
        glacierObject['L5Dates'] = L5Dates


    glacierObject['fileaddress'] = str(glacierObject['GlimsID'])


    csv_file = str(glacierObject['GlimsID']) + ".csv"
    csv_columns = ['GlimsID', 'boundingbox','L8Dates','L7Dates','L5Dates', 'fileaddress']
    with open(csv_file, "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = csv_columns)
        writer.writeheader()
        writer.writerow(glacierObject)



    g_login = GoogleAuth()
    g_login.LocalWebserverAuth()
    drive = GoogleDrive(g_login)

    file=drive.CreateFile(glacierObject)
    file.SetContentFile(str(glacierID) + ".csv")
    file.Upload()



dct = dict()
#Bayi
dct['GlimsID'] = "G098570E39226N"
dct['boundingbox'] = [[98.54723199999999, 39.210651], [98.59022299999999, 39.210651], [98.59022299999999, 39.243147], [98.54723199999999, 39.243147], [98.54723199999999, 39.210651]]
ee_download("G098570E39226N", dct, cloud_tol = 20, landsat = True, dem = True)

#McCall Glacier
# dct['GlimsID'] = "G216152E69302N"
# dct['boundingbox'] = [[-143.860976, 69.276937],[-143.779992, 69.276937],[-143.779992, 69.334028],[-143.860976, 69.334028]]

#noName
# dct['GlimsID'] = "G073183E39306N"
# dct['boundingbox'] = [[73.169516, 39.297678], [73.191774, 39.297678], [73.191774, 39.316767], [73.169516, 39.316767], [73.169516, 39.297678]]
# ee_download("G073183E39306N", dct, cloud_tol = 20, landsat = True, dem = True)

# ee_download("G216152E69302N", dct, cloud_tol = 20, landsat = False, dem = False)



#Name CN5Y725D0005
# dct['GlimsID'] = "G088313E43812N"
# dct['boundingbox']=[[88.274114, 43.789814], [88.341902, 43.789814], [88.341902, 43.826003], [88.274114, 43.826003], [88.274114, 43.789814]]
# ee_download("G088313E43812N", dct, cloud_tol = 20, landsat = False, dem = False)

#Names CN5Y730C0029 Urumqi Source Glacier No1 W Br
# dct['GlimsID'] = "G086801E43117N"
# dct['boundingbox'] = [[86.795518, 43.110825],[86.81219299999999, 43.110825], [86.81219299999999, 43.120259], [86.795518, 43.120259], [86.795518, 43.110825]]
# ee_download("G086801E43117N", dct, cloud_tol = 20, landsat = True, dem = True)
