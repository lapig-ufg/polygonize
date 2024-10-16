from atlas.creat_quee import creat_feature_loop
from atlas.pasture_vigor_polygonize import feature_loop
from atlas.pasture_vigor_polygonize_simplified import feature_loop as feature_loop_s100

import os 
from dotenv import load_dotenv
load_dotenv() 


args = (
    os.getenv('REGIAO'),
    os.getenv('VIGOR_FILES'),
    os.getenv('VIGOR_PREFIX'),
    os.getenv('VIGOR_SUFIX')
)

datas = creat_feature_loop(args, f"{os.getenv('VIGOR','pasture_vigor_col9')}")
feature_loop(datas)

datas_s1oo = creat_feature_loop(args, f"{os.getenv('VIGOR','pasture_vigor_col9')}_s100")
feature_loop_s100(datas)