from atlas.creat_quee import creat_feature_loop
from atlas.pasture_vigor_polygonize import feature_loop


#pasture_cvp_veg_evi_mod13q1_2004_Brazil_col8_atlas_sirgas
args = (
    '../Shapefile/regions_v05_12_2023.shp',
    '../Pastagem_Vigor_Col8/pasture_cvp_veg_evi_mod13q1_*_Brazil_col8_atlas_sirgas.tif',
    '../Pastagem_Vigor_Col8/pasture_cvp_veg_evi_mod13q1_',
    '_Brazil_col8_atlas_sirgas.tif',
)
datas = creat_feature_loop(args, 'pasture_vigor_col8')
feature_loop(datas)