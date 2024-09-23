from atlas.creat_quee import creat_feature_loop
from atlas.pasture_polygonize import feature_loop

args = (
    '/data/shape/regiao.shp',
    '/data/pasture_col9/pasture_br_Y*_COL9_atlas.tif',
    '/data/pasture_col9/pasture_br_Y',
    '_COL9_atlas.tif',
)
datas = creat_feature_loop(args, 'pasture_col9')
feature_loop(datas)




# feature_loop(('../Shapefile/regions_v26052023.shp','../Pastagem_Col8/pasture_br_Y*_COL9_atlas.tif', '../Pastagem_Col8/pasture_br_Y','_COL8_atlas_sirgas.tif'))
