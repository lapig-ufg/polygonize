import os
import sys
from datetime import datetime
import warnings


import numpy
from osgeo import gdal, ogr, osr
from pathos.multiprocessing import ProcessingPool
from pymongo import MongoClient

from atlas.config import logger, PG_CONNECTION, MONGO
from atlas.functions import set_status, Status, get_complete, normalize_field_value

warnings.filterwarnings('ignore')

# Variaveis globais
#  Database connections
BD_TABLE = 'pasture_col8'

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
    _doc
):
    timestart = datetime.now()
    _doc['mensagem'] = 'init polygonize'
    logger.info(f'init polygonize {_doc["_id"]}')
    _doc['start'] = timestart
    set_status(_doc, Status.RUNNING, BD_TABLE)
    
    
    memory_driver = ogr.GetDriverByName('Memory')
    memory_ds = memory_driver.CreateDataSource('tempDS')

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
    multi = ogr.Geometry(ogr.wkbMultiPolygon)
    area = 0.0

    osrTransform = osr.CoordinateTransformation(raster_srs, albers)

    for feat in dst_layer:
        if feat.geometry():
            multi.AddGeometry(feat.geometry())
            feat.geometry().Transform(osrTransform)

            area = area + feat.geometry().GetArea()

    out_feat = ogr.Feature(defn)

    if multi.IsValid() == False:
        multi = multi.MakeValid()
        multi = multi.Buffer(0)

    if multi.GetGeometryType() == ogr.wkbPolygon:
        out_feat.SetGeometryDirectly(ogr.ForceToMultiPolygon(multi))
    else:
        out_feat.SetGeometry(multi)

    out_feat.SetField('index', fid)
    out_feat.SetField('area_ha', area / 10000)
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
        end_date = datetime.now()
        timeend = end_date - timestart
        _doc['mensagem'] = f'{fid} save'
        _doc['time'] = timeend.total_seconds()
        _doc['time_str'] = str(timeend)
        _doc['end_date'] = end_date
        set_status(_doc, Status.COMPLETE, BD_TABLE)
        logger.success(f'{fid} save')
    except Exception as e:
        _doc['mensagem'] = f'ERROR_CREATE_FEATURE: {fid} | feature class {out_feat:>10} | msg: {e}'
        set_status(_doc, Status.ERROR, BD_TABLE)
        logger.error(
            f'ERROR_CREATE_FEATURE: {fid} | feature class {out_feat:>10} | msg: {e}'
        )
    logger.info(
        f"Inserted: {fid} | {input_feature.GetField('CD_GEOCMU'):>10} | Year: {year} | Time Execution: {timeend}"
    )



def feature_loop(_docs):

    input_zone_polygon, input_value_raster_path, prefix, sufix, field_names = _docs[0]['args']

    SHP = ogr.Open(input_zone_polygon)
    VECTOR_LAYER = SHP.GetLayer()



    dataStore = create_connection()
    layerLocal = dataStore.GetLayerByName(BD_TABLE)

    if layerLocal is None:
        create_layer(VECTOR_LAYER)
        logger.info(f'Layer created! ^-^ ')


    del SHP
    del VECTOR_LAYER
    dataStore.Destroy()
    del layerLocal


    def parallelProcess(_doc):
        if get_complete(_doc, BD_TABLE):
            return True
        input_zone_polygon, input_value_raster, fid, year, field_names = _doc['args']
        
        SHP = ogr.Open(input_zone_polygon)
        VECTOR_LAYER = SHP.GetLayer()

        dataStore = create_connection()
        LAYER = dataStore.GetLayerByName(BD_TABLE)

        
        try:

            input_feature = VECTOR_LAYER.GetFeature(fid)
            input_feature_srs = VECTOR_LAYER.GetSpatialRef()
            polygonize(
                input_feature,
                input_feature_srs,
                field_names,
                input_value_raster,
                LAYER,
                fid,
                year,
                _doc
            )
        except Exception as e:
            _doc['mensagem'] = f'ERROR: FID: {fid} | CD_GEOCMU: {input_feature.GetField("CD_GEOCMU")} | YEAR: {year} | msg: {e}.'
            set_status(_doc, Status.ERROR, BD_TABLE)
            logger.exception(
                f"ERROR -> FID: {fid} | CD_GEOCMU: {input_feature.GetField('CD_GEOCMU')} | YEAR: {year} | msg: {e}."
            )
            return False
        finally:
            del SHP
            del VECTOR_LAYER
            del dataStore
            del LAYER
        return True
    
    def loop(list_docs):
        result = []
        for _doc in list_docs:
            result.append(parallelProcess(_doc))
        return all(result)

    # parallelProcess((input_value_raster, 986, year, field_names))
    num_cores = os.cpu_count() - 2

    logger.info('init parallel process')
    while True:
        with MongoClient(MONGO) as client:
            db = client['polygonize']
            collection = db[BD_TABLE]
            _docs = list(collection.find({'status': Status.PENDING.value}).limit(100_000))
            pipeline = [
                {
                    '$group': {
                        '_id': '$status',
                        'count': {
                            '$sum': 1
                        }
                    }
                }
            ]
            # Execute a agregação e obtenha os resultados
            resultado_agregacao = list(collection.aggregate(pipeline))
            status = {i["_id"]:i["count"] for i in resultado_agregacao}
            
            N_PENDING = status.get(Status.PENDING.value,0)
            N_RUNNING = status.get(Status.RUNNING.value,0)
            N_COMPLETE = status.get(Status.COMPLETE.value,0)
            N_ERROR = status.get(Status.ERROR.value,0)
            
            
            logger.info(f'{N_PENDING} pending | {N_RUNNING} running | {N_COMPLETE} complete | {N_ERROR} error')
            
        if not _docs:
            break
        
        chunk = int(len(_docs)/num_cores)
        list_docs = []
        for start in range(0, len(_docs), chunk):
            end = start + chunk
            if end > chunk:
                end = chunk
            _docs_slice = _docs[start:end]
            list_docs.append(_docs_slice)
        
        with ProcessingPool(nodes=int(num_cores)) as workers:
            result = workers.map(
                loop,
                list_docs
                )

    logger.info(f'Finished! ┗(＾0＾) ┓')


if __name__ == '__main__':
    feature_loop(sys.argv[1], sys.argv[2], sys.argv[3])