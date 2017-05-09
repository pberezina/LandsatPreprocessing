import pandas as pd
import numpy as np
import gdal, os


def acquireMetadata():
    def change_type(x):
        try:
            return float(x.strip('"'))
        except ValueError:
            return str(x.strip('"'))
        except AttributeError:
            return None
    try:
        for file in os.listdir(path):
            if file.endswith("MTL.txt"):
                data = os.path.join(path, file)
        df = pd.read_csv(data, sep=" = ", header=None, names=['Parameter', 'Value'], engine='python')
        df = df[(df.Parameter != 'GROUP') & (df.Parameter != 'END_GROUP') & (df.Parameter != 'END')].copy()
        df.set_index('Parameter', inplace=True)
        df['Value'] = df['Value'].apply(change_type)
        return df
    except:
        print('Error loading metadata')


def execute(band):
    band = str(band)
    for file in os.listdir(path):
        if file.endswith(band + '.TIF'):
            inRaster = gdal.Open(os.path.join(path, file))
            arr = inRaster.ReadAsArray()
            arr = arr.astype('float32')
    geotrans = inRaster.GetGeoTransform()
    proj = inRaster.GetProjection()
    cols = inRaster.RasterXSize
    rows = inRaster.RasterYSize

    if int(band) in range(10):
        # calculate reflectance
        M_ro = md.get_value('REFLECTANCE_MULT_BAND_' + band, 'Value')
        A_ro = md.get_value('REFLECTANCE_ADD_BAND_' + band, 'Value')
        SE = md.get_value('SUN_ELEVATION', 'Value')
        for i in ((0, 4000), (4000, arr.shape[1])):
            arr[:, i[0]:i[1]] = (M_ro * arr[:, i[0]:i[1]] + A_ro) / np.sin(SE)
        outName = "B" + band + '_Refl.TIF'
    else:
        # calculate brigthness temperature in celcius
        M_lambda = md.get_value("RADIANCE_MULT_BAND_" + band, 'Value')
        A_lambda = md.get_value("RADIANCE_ADD_BAND_" + band, 'Value')
        K2 = md.get_value("K2_CONSTANT_BAND_" + band, 'Value')
        K1 = md.get_value("K1_CONSTANT_BAND_" + band, 'Value')
        for i in ((0, 4000), (4000, arr.shape[1])):
            arr[:, i[0]:i[1]] = K2 / np.log(K1 / (M_lambda * arr[:, i[0]:i[1]] + A_lambda) + 1) - 273.15
        outName = "B" + band + '_Temp.TIF'

    nodataval = arr[0, 0]
    arr[arr == nodataval] = np.nan

    driver = gdal.GetDriverByName('GTiff')
    outRaster = driver.Create(os.path.join(path, outName), cols, rows, 1, gdal.GDT_Float32)
    outRaster.SetGeoTransform(geotrans)
    outRaster.SetProjection(proj)
    outRaster.GetRasterBand(1).WriteArray(arr)
    outRaster.FlushCache()


path = r"D:\GIS\Landsat8 Data"
md = acquireMetadata()
for band_no in range(1,3): execute(band_no)