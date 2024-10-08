
from pymongo import MongoClient
from osgeo import ogr
from glob import glob
import os.path 

from atlas.config import logger, MONGO
from atlas.functions import get_year

def creat_doc_loop(args):
    input_value_raster,input_zone_polygon, fid, year, field_names = args
    return {
            '_id': f'{input_value_raster};{input_zone_polygon};{fid};{year};{field_names}',
            'args': args,
            'status':'Pending',
            
        }
def creat_feature_loop(args, database):
    input_zone_polygon, input_value_raster_path, prefix, sufix = args
    logger.debug(args)
    
    if not os.path.isfile(input_zone_polygon):
        logger.error(f'File {input_zone_polygon} not found')
        return []
    raster = glob(input_value_raster_path)
    logger.info(f'{input_value_raster_path}, {raster}')
    if not os.path.isfile(raster[0]):
        logger.error(f'File {raster} not found')
        return []
    
    SHP = ogr.Open(input_zone_polygon)
    VECTOR_LAYER = SHP.GetLayer()

    fids = range(VECTOR_LAYER.GetFeatureCount())
    logger.debug(f'fids: {fids}')
    dfn = VECTOR_LAYER.GetLayerDefn()
    field_names = [
        dfn.GetFieldDefn(i).GetName() for i in range(dfn.GetFieldCount())
    ]

    logger.debug(MONGO)
    with MongoClient(MONGO) as client:
        db = client["polygonize"]
        collection = db[database]
        _docs = [creat_doc_loop((input_zone_polygon,file, fid, get_year(file,prefix,sufix), field_names)) for fid in fids for file in glob(input_value_raster_path)]
        try:
            collection.insert_many(_docs)
            logger.info(f'Insert in database {database} with args {args}')
        except Exception as e:
            logger.error(f'Error to insert in database {database} with args {args} and error {e}')
    return _docs
