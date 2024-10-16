from atlas.creat_quee import creat_feature_loop
from atlas.pasture_polygonize import feature_loop
from atlas.pasture_polygonize_simplified import feature_loop as feature_loop_s100
import os 
from dotenv import load_dotenv
load_dotenv() 


args = (
    os.getenv('REGIAO'),
    os.getenv('PASTURE_FILES'),
    os.getenv('PASTURE_PREFIX'),
    os.getenv('PASTURE_SUFIX')
)
datas = creat_feature_loop(args, f"{os.getenv('PASTURE','pasture_col9')}")
feature_loop(datas)

datas_s1oo = creat_feature_loop(args, f"{os.getenv('PASTURE','pasture_col9')}_s100")
feature_loop_s100(datas)




# feature_loop(('../Shapefile/regions_v26052023.shp','../Pastagem_Col8/pasture_br_Y*_COL9_atlas.tif', '../Pastagem_Col8/pasture_br_Y','_COL8_atlas_sirgas.tif'))
