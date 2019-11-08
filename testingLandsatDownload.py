#San Diego
#boundingbox = [[-117.37041015624999, 33.04062508350601],[-117.37041015624999, 32.523378160768985],[-116.90898437499999, 32.523378160768985],[-116.90898437499999, 33.04062508350601]]
#McCall Glacier
#boundingbox = [[-143.860976, 69.276937],[-143.779992, 69.276937],[-143.779992, 69.334028],[-143.860976, 69.334028]]
#noName G073183E39306N
#boundingbox = [[73.169516, 39.297678], [73.191774, 39.297678], [73.191774, 39.316767], [73.169516, 39.316767], [73.169516, 39.297678]]
#Bayi Glacier
boundingbox =  [[98.54723199999999, 39.210651], [98.59022299999999, 39.210651], [98.59022299999999, 39.243147], [98.54723199999999, 39.243147], [98.54723199999999, 39.210651]]

import ee
try:
  ee.Initialize()
  print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
  print('The Earth Engine package failed to initialize!')
except:
  print("Unexpected error:", sys.exc_info()[0])
  raise


region = ee.Geometry.Polygon(boundingbox);
print(region.getInfo())
## Landsat 8
col = ee.ImageCollection('LANDSAT/LC08/C01/T1_TOA')
col = col.filterDate('2013-01-01','2016-01-01')
col = col.filterBounds(region)
#col = col.select(['B1','B2','B3','B4','B5','B6', 'B7', 'B10'])
#col = col.filter(ee.Filter.lt('CLOUD_COVER_LAND', 9))
count = col.size()
print(count.getInfo())
DEM = ee.Image("USGS/SRTMGL1_003")
DEM = DEM.clip(region)
# task = ee.batch.Export.image.toDrive(
#     image = DEM.clip(region),
#     scale = 30, region = region.bounds().getInfo()['coordinates'], folder = "DEMtest", fileNamePrefix = 'DEMtest1')
# task.start()


#collectionList = col.toList(col.size())
#collectionSize = collectionList.size().getInfo()
#print(collectionSize)

def cloudscore(image):
  cloud = ee.Algorithms.Landsat.simpleCloudScore(image).select('cloud')
  cloudiness = cloud.reduceRegion(ee.Reducer.mean(),
    geometry = region,
    scale = 30)
  image = image.set(cloudiness)
  return image

withCloudiness = col.map(algorithm = cloudscore)

filteredCollection = withCloudiness.filter(ee.Filter.lt('cloud', 20))
print(filteredCollection.size().getInfo())
filteredCollection = filteredCollection.select(['B1', 'B2', 'B3', 'B4', 'B5', 'B6'])
collectionList = filteredCollection.toList(col.size())
collectionSize = collectionList.size().getInfo()
# print(collectionSize)
for i in range(collectionSize):
    task = ee.batch.Export.image.toDrive(
        image = ee.Image(collectionList.get(i)).clip(region),
        scale = 30, region = region.bounds().getInfo()['coordinates'], folder = "L8BayiGlacier01130116", fileNamePrefix = 'BayiL8' + str(i + 1))
    task.start()

# Landsat 7
col2 = ee.ImageCollection('LANDSAT/LE07/C01/T1_TOA')
col2 = col2.filterDate('1999-01-01','2006-01-01')
col2 = col2.filterBounds(region)
        #.select(['B1','B2','B3','B4','B5','B6'])

count = col2.size()
print(count.getInfo())

withCloudiness = col2.map(algorithm = cloudscore)

filteredCollection = withCloudiness.filter(ee.Filter.lt('cloud', 20))
print(filteredCollection.size().getInfo())
filteredCollection = filteredCollection.select(['B1', 'B2', 'B3', 'B4', 'B5', 'B6_VCID_1'])
collectionList = filteredCollection.toList(col2.size())
collectionSize = filteredCollection.size().getInfo()
print(collectionSize)
# for i in range(collectionSize):
#     task = ee.batch.Export.image.toDrive(
#         image = ee.Image(collectionList.get(i)).clip(region),
#         scale = 30, region = region.bounds().getInfo()['coordinates'],
#         folder = "L7BayiGlacier01990106", fileNamePrefix = 'BayiL7' + str(i + 1), fileFormat = "GeoTIFF")
#     task.start()




## Landsat 5
col = ee.ImageCollection('LANDSAT/LT05/C01/T1_TOA')\
        .filterDate('1990-03-01','1998-12-31')\
        .filterBounds(region)

count = col.size()
print(count.getInfo())
withCloudiness = col.map(algorithm = cloudscore)

filteredCollection = withCloudiness.filter(ee.Filter.lt('cloud', 20))
print(filteredCollection.size().getInfo())
filteredCollection = filteredCollection.select(['B1', 'B2', 'B3', 'B4', 'B5', 'B6'])
collectionList = filteredCollection.toList(col.size())
collectionSize = filteredCollection.size().getInfo()
print(collectionSize)
# for i in range(collectionSize):
#     task = ee.batch.Export.image.toDrive(
#         image = ee.Image(collectionList.get(i)).clip(region),
#         scale = 30, region = region.bounds().getInfo()['coordinates'],
#         folder = "L5BayiGlacier8498", fileNamePrefix = 'BayiL5' + str(i + 1), fileFormat = "GeoTIFF")
#     task.start()
