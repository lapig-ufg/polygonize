import os
import sys
import time
import warnings
from glob import glob

import numpy
from decouple import config
from osgeo import gdal, ogr, osr
from pathos.multiprocessing import ProcessingPool

from atlas.config import logger
from atlas.functions import normalize_field_value

warnings.filterwarnings('ignore')

# Variaveis globais
#  Database connections
PG_CONNECTION = config('PG_CONNECTION')
BD_TABLE = 'pasture_quality_col8_S100'


def create_connection():
    DRIVER = ogr.GetDriverByName('PostgreSQL')
    DATA_SOURCE = DRIVER.CreateDataSource(PG_CONNECTION)
    return DATA_SOURCE


def create_layer(vector_layer):
    input_feature_def = vector_layer.GetLayerDefn()
    ds = create_connection()
    table_srs = osr.SpatialReference()
    table_srs.ImportFromEPSG(4674)
    layer = ds.CreateLayer(
        BD_TABLE,
        srs=table_srs,
        geom_type=ogr.wkbMultiPolygon,
        options=['FID=gid', 'GEOMETRY_NAME=geom', 'SPATIAL_INDEX=GIST'],
    )

    area_field = ogr.FieldDefn('area_ha', ogr.OFTReal)
    area_field.SetWidth(32)
    area_field.SetPrecision(8)

    classe_field = ogr.FieldDefn('classe', ogr.OFTInteger)
    classe_field.SetWidth(1)

    year_field = ogr.FieldDefn('year', ogr.OFTInteger)
    year_field.SetWidth(4)

    index_field = ogr.FieldDefn('index', ogr.OFTInteger)
    index_field.SetWidth(5)

    layer.CreateField(index_field)
    layer.CreateField(area_field)
    layer.CreateField(classe_field)
    layer.CreateField(year_field)

    for i in range(input_feature_def.GetFieldCount()):
        input_field = input_feature_def.GetFieldDefn(i)
        layer.CreateField(input_field)

    ds.Destroy()


def polygonize(
    input_feature,
    input_feature_srs,
    field_names,
    input_value_raster,
    out_lyr,
    fid,
    year,
):
    timestart = time.time()
    memory_driver = ogr.GetDriverByName('Memory')
    memory_ds = memory_driver.CreateDataSource('tempDS')

    # name = input_feature.GetField(str(sys.argv[4]))
    input_geometry = input_feature.GetGeometryRef()

    temp_vector_layer = memory_ds.CreateLayer(
        'tempLayer', input_feature_srs, input_geometry.GetGeometryType()
    )
    polygon_feature = ogr.Feature(temp_vector_layer.GetLayerDefn())
    polygon_feature.SetGeometry(input_geometry)
    temp_vector_layer.CreateFeature(polygon_feature)

    raster = gdal.Open(input_value_raster)

    raster_srs = osr.SpatialReference()
    raster_srs.ImportFromWkt(raster.GetProjectionRef())

    transform = raster.GetGeoTransform()
    xOrigin = transform[0]
    yOrigin = transform[3]
    pixelWidth = transform[1]
    pixelHeight = transform[5]

    if input_geometry.GetGeometryName() == 'MULTIPOLYGON':
        count = 0
        pointsX = []
        pointsY = []
        for polygon in input_geometry:
            geomInner = input_geometry.GetGeometryRef(count)
            ring = geomInner.GetGeometryRef(0)
            numpoints = ring.GetPointCount()
            for p in range(numpoints):
                lon, lat, z = ring.GetPoint(p)
                pointsX.append(lon)
                pointsY.append(lat)
            count += 1
    elif input_geometry.GetGeometryName() == 'POLYGON':
        ring = input_geometry.GetGeometryRef(0)
        numpoints = ring.GetPointCount()
        pointsX = []
        pointsY = []
        for p in range(numpoints):
            lon, lat, z = ring.GetPoint(p)
            pointsX.append(lon)
            pointsY.append(lat)

    else:
        logger.error(
            f'ERROR: Geometry needs to be either Polygon or Multipolygon'
        )

    xmin = min(pointsX)
    xmax = max(pointsX)
    ymin = min(pointsY)
    ymax = max(pointsY)

    xmin = xOrigin + abs(round((xOrigin - xmin) / pixelWidth)) * pixelWidth
    ymax = yOrigin + abs(round((yOrigin - ymax) / pixelHeight)) * pixelHeight

    xoff = int((xmin - xOrigin) / pixelWidth)
    yoff = int((yOrigin - ymax) / pixelWidth)
    xcount = int((xmax - xmin) / pixelWidth)
    ycount = int((ymax - ymin) / pixelWidth)

    # temp_raster_layer = gdal.GetDriverByName('GTiff').Create('teste.tif', xcount, ycount, 1, gdal.GDT_Byte)
    temp_raster_layer = gdal.GetDriverByName('MEM').Create(
        '', xcount, ycount, gdal.GDT_Byte
    )
    temp_raster_layer.SetGeoTransform(
        (xmin, pixelWidth, 0, ymax, 0, pixelHeight)
    )
    temp_raster_layer.SetProjection(raster_srs.ExportToWkt())

    gdal.RasterizeLayer(
        temp_raster_layer, [1], temp_vector_layer, burn_values=[1], options=[]
    )

    input_raster_band = raster.GetRasterBand(1)
    temp_raster_band = temp_raster_layer.GetRasterBand(1)

    input_raster_data = input_raster_band.ReadAsArray(
        xoff, yoff, xcount, ycount
    ).astype(numpy.byte)
    temp_raster_data = temp_raster_band.ReadAsArray(
        0, 0, xcount, ycount
    ).astype(numpy.byte)

    input_raster_data[input_raster_data == 255] = 0
    temp_raster_data = input_raster_data * temp_raster_data

    temp_raster_band.WriteArray(temp_raster_data)
    temp_raster_band.FlushCache()

    dst_ds = memory_driver.CreateDataSource('tempVec')
    dst_layer = dst_ds.CreateLayer('tempVec', srs=raster_srs)

    dn = ogr.FieldDefn('dn', ogr.OFTInteger)
    dst_layer.CreateField(dn)

    if out_lyr is None:
        raise Exception('Layer not found')

    gdal.Polygonize(
        temp_raster_band, temp_raster_band, dst_layer, 0, [], callback=None
    )

    defn = out_lyr.GetLayerDefn()

    albers = osr.SpatialReference()
    albers.ImportFromProj4(
        '+proj=aea +lat_1=-2 +lat_2=-22 +lat_0=-12 +lon_0=-54 +x_0=0 +y_0=0 +ellps=aust_SA +units=m +no_defs'
    )

    osrTransform = osr.CoordinateTransformation(raster_srs, albers)

    geometryClasses = {}

    for feat in dst_layer:

        pixel_value = feat.GetField('dn')

        if pixel_value not in geometryClasses:
            geometryClasses[pixel_value] = {
                'geometry': ogr.Geometry(ogr.wkbMultiPolygon),
                'area': 0.0,
                'count': 0,
            }

        geometryClasses[pixel_value]['count'] += 1

        if feat.geometry():
            geometryClasses[pixel_value]['geometry'].AddGeometry(
                feat.geometry()
            )
            feat.geometry().Transform(osrTransform)

            geometryClasses[pixel_value]['area'] = (
                geometryClasses[pixel_value]['area']
                + feat.geometry().GetArea()
            )
    listClass = []
    for featClass in geometryClasses.keys():
        listClass.append(featClass)
        geometry = geometryClasses[featClass]['geometry']
        area = geometryClasses[featClass]['area']

        geoTrans_albers = osr.CoordinateTransformation(
            input_feature_srs, albers
        )

        geoTrans_inv_albers = osr.CoordinateTransformation(
            albers, input_feature_srs
        )

        geometry.Transform(geoTrans_albers)

        simplifiedTopo = geometry.SimplifyPreserveTopology(100)
        simplifiedTopo.Transform(geoTrans_inv_albers)

        # import ipdb; ipdb.set_trace()

        if fid != 2:

            if simplifiedTopo.IsValid() == False:
                simplifiedTopo = simplifiedTopo.MakeValid()
                simplifiedTopo = simplifiedTopo.Buffer(0)

        out_feat = ogr.Feature(defn)

        if simplifiedTopo.GetGeometryType() == ogr.wkbMultiPolygon:
            out_feat.SetGeometry(simplifiedTopo)
        else:
            out_feat.SetGeometryDirectly(
                ogr.ForceToMultiPolygon(simplifiedTopo)
            )

        out_feat.SetField('index', fid)
        out_feat.SetField('area_ha', area / 10000)
        out_feat.SetField('classe', featClass)
        out_feat.SetField('year', year)

        for field_name in field_names:
            if field_name == ['BIOMA']:
                value = normalize_field_value(
                    str(input_feature.GetField(field_name))
                )
                out_feat.SetField(field_name, value)
            else:
                out_feat.SetField(
                    field_name, str(input_feature.GetField(field_name))
                )

        try:
            out_lyr.CreateFeature(out_feat)
        except Exception as e:
            logger.error(
                f'ERROR_CREATE_FEATURE: {fid} | feature class {featClass:>10} | msg: {e}'
            )

    timeend = time.time() - timestart
    logger.info(
        f"Inserted: {fid} | {input_feature.GetField('CD_GEOCMU'):>10} | Year: {year} | Classes: {listClass} | Time Execution: {timeend:.2f}"
    )


def feature_loop(args):

    input_zone_polygon, input_value_raster_path = args

    logger.add(
        f'logs/{BD_TABLE}.log',
        format='[{time} | {level:<6}] {module}.{function}:{line} {message}',
        rotation='500 MB',
    )
    logger.add(
        f'logs/{BD_TABLE}_error.log',
        format='[{time} | {level:<6}] {module}.{function}:{line} {message}',
        level='WARNING',
        rotation='500 MB',
    )
    SHP = ogr.Open(input_zone_polygon)
    VECTOR_LAYER = SHP.GetLayer()

    fids = range(VECTOR_LAYER.GetFeatureCount())

    dataStore = create_connection()
    layerLocal = dataStore.GetLayerByName(BD_TABLE)

    if layerLocal is None:
        create_layer(VECTOR_LAYER)
        logger.info(f'Layer created! ^-^ ')
    dfn = VECTOR_LAYER.GetLayerDefn()
    field_names = [
        dfn.GetFieldDefn(i).GetName() for i in range(dfn.GetFieldCount())
    ]
    del SHP
    del VECTOR_LAYER
    dataStore.Destroy()
    del layerLocal

    def parallelProcess(args):
        SHP = ogr.Open(input_zone_polygon)
        VECTOR_LAYER = SHP.GetLayer()

        dataStore = create_connection()
        LAYER = dataStore.GetLayerByName(BD_TABLE)

        input_value_raster, fid, year, field_names = args

        _doc = {
            '_id': f'{input_value_raster};{fid};{year};{field_names}',
            'args': args,
            'status': 'Pending',
        }

        logger.debug(
            f"""
                input_value_raster: {input_value_raster}, 
                fid: {fid} 
                year: {year}
                field_names: {field_names}"""
        )
        try:
            # logger.info(f"raster: {input_value_raster} | fid: {fid} | year: {year} | field_names: {field_names}")
            input_feature = VECTOR_LAYER.GetFeature(fid)
            input_feature_srs = VECTOR_LAYER.GetSpatialRef()
            # logger.info(f"Runing Parallel: {fid} | CD_GEOCMU: {input_feature.GetField('CD_GEOCMU'):>10} | YEAR: {year}")
            polygonize(
                input_feature,
                input_feature_srs,
                field_names,
                input_value_raster,
                LAYER,
                fid,
                year,
                _doc,
            )
        except Exception as e:
            logger.exception(
                f"ERROR -> FID: {fid} | CD_GEOCMU: {input_feature.GetField('CD_GEOCMU')} | YEAR: {year} | msg: {e}."
            )
        finally:
            del SHP
            del VECTOR_LAYER
            del dataStore
            del LAYER

    # # parallelProcess((input_value_raster, 5, year, field_names))
    num_cores = int((os.cpu_count() * 2) - 5)

    with ProcessingPool(nodes=int(num_cores)) as workers:
        result = workers.map(
            parallelProcess,
            [
                (file, fid, get_year(file), field_names)
                for fid in fids
                for file in glob(input_value_raster_path)
            ],
        )

    logger.info(f'Finished! ┗(＾0＾) ┓')


if __name__ == '__main__':
    args = (sys.argv[1], sys.argv[2])
    feature_loop(args)
