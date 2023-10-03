from atlas.creat_quee import creat_feature_loop
from atlas.pasture_polygonize_simplified import \
    feature_loop as feature_loop_s100  # prepara para muilt core

args = (
    '../Shapefile/regions_v26052023.shp',
    '../Pastagem_Col8/pasture_br_Y*_COL8_atlas_sirgas.tif',
    '../Pastagem_Col8/pasture_br_Y',
    '_COL8_atlas_sirgas.tif',
)
creat_feature_loop(args, 'pasture_col8_s100')
#feature_loop_s100(args)


from atlas.pasture_polygonize import feature_loop

# feature_loop(('../Shapefile/regions_v26052023.shp','../Pastagem_Col8/pasture_br_Y*_COL8_atlas_sirgas.tif', '../Pastagem_Col8/pasture_br_Y','_COL8_atlas_sirgas.tif'))
