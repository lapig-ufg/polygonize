from atlas.creat_quee import creat_feature_loop
from atlas.pasture_polygonize import feature_loop

args = (
    '../Shapefile/regions_v19102023.shp',
    '../Pastagem_Col8/pasture_br_Y*_COL8_atlas_sirgas.tif',
    '../Pastagem_Col8/pasture_br_Y',
    '_COL8_atlas_sirgas.tif',
)
datas = creat_feature_loop(args, 'pasture_col8')
feature_loop(datas)




# feature_loop(('../Shapefile/regions_v26052023.shp','../Pastagem_Col8/pasture_br_Y*_COL8_atlas_sirgas.tif', '../Pastagem_Col8/pasture_br_Y','_COL8_atlas_sirgas.tif'))
