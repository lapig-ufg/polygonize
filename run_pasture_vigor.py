from atlas.creat_quee import creat_feature_loop
from atlas.pasture_vigor_polygonize_simplified import feature_loop

args = (
    '../Shapefile/regions_v05_12_2023.shp',
    '../Pastagem_Col8/pasture_br_Y*_COL8_atlas_sirgas.tif',
    '../Pastagem_Col8/pasture_br_Y',
    '_COL8_atlas_sirgas.tif',
)
datas = creat_feature_loop(args, 'pasture_vigor_col8_S100')
feature_loop(datas)