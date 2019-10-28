boundingbox = [[-117.37041015624999, 33.04062508350601],[-117.37041015624999, 32.523378160768985],[-116.90898437499999, 32.523378160768985],[-116.90898437499999, 33.04062508350601]]

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
col = ee.ImageCollection('LANDSAT/LC08/C01/T1_SR')\
        .filterDate('2013-01-01','2016-01-01')\
        .filterBounds(region)\
        .select(['B1','B2','B3','B4','B5','B6'])\
        .filter(ee.Filter.lt('CLOUD_COVER', 5))
count = col.size()
print(count.getInfo())


collectionList = col.toList(col.size())
collectionSize = collectionList.size().getInfo()
print(collectionSize)
for i in range(collectionSize):
    task = ee.batch.Export.image.toDrive(
        image = ee.Image(collectionList.get(i)).clip(region),
        scale = 30, region = region.bounds().getInfo()['coordinates'], folder = "L8GEEtest", fileNamePrefix = 'imageL8' + str(i + 1))
    task.start()

## Landsat 7
# col = ee.ImageCollection('LANDSAT/LE07/C01/T1_SR')\
#         .filterDate('1999-01-01','2000-01-01')\
#         .filterBounds(region)\
#         .select(['B1','B2','B3','B4','B5','B6'])
#         #.sort("CLOUD_COVER", False)
# count = col.size()
# print(count.getInfo())
#
#
# collectionList = col.toList(col.size())
# collectionSize = collectionList.size().getInfo()
# print(collectionSize)
# for i in range(collectionSize):
#     task = ee.batch.Export.image.toDrive(
#         image = ee.Image(collectionList.get(i)).clip(region),
#         scale = 30, region = region.bounds().getInfo()['coordinates'], folder = "GEEtest", fileNamePrefix = 'imageL7' + str(i + 1))
#     task.start()

## Landsat 5
# col = ee.ImageCollection('LANDSAT/LT05/C01/T1_SR')\
#         .filterDate('1984-03-01','1998-12-31')\
#         .filterBounds(region)\
#         .select(['B1','B2','B3','B4','B5','B6'])
#         #.sort("CLOUD_COVER", False)
# count = col.size()
#
# collectionList = col.toList(col.size())
# collectionSize = collectionList.size().getInfo()
# print(collectionSize)
# for i in range(collectionSize):
#     task = ee.batch.Export.image.toDrive(
#         image = ee.Image(collectionList.get(i)).clip(region),
#         scale = 30, region = region.bounds().getInfo()['coordinates'], folder = "GEEtest", fileNamePrefix = 'imageL5' + str(i + 1))
#     task.start()



# import geetools
# from geetools import batch
# from geetools import tools
# tasks = batch.imagecollection.toDrive(col, 'landsat', region=region, scale=30)


# import os
# import sys
# import gee2drive
# [head,tail]=os.path.split(gee2drive.__file__)
# os.chdir(head)
# sys.path.append(head)
# from export import exp
# exp(col, folderpath = "landsat",start = "1999-01-01",end = "2000-01-01",geojson = ee.Geometry.Polygon([[[-117.37041015624999, 33.04062508350601],[-117.37041015624999, 32.523378160768985],[-116.90898437499999, 32.523378160768985],[-116.90898437499999, 33.04062508350601]]]), bandnames="['B1','B2', 'B3', 'B4', 'B5', 'B6']",operator="bb",typ="ImageCollection")
