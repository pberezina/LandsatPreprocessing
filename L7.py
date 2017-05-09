import pandas as pd
import numpy as np
import time, gdal, os
import dask.array as da


def acquireMetadata(path):
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


def calcRadiance(path, md, band):
    for file in os.listdir(path):
        if band == '61' or band == '62':
            if file.endswith(band + ".TIF"):
                inRaster = gdal.Open(os.path.join(path, file))
        else:
            if file.endswith(band + '0.TIF'):
                inRaster = gdal.Open(os.path.join(path, file))
    daskarr = da.from_array(inRaster.ReadAsArray(), chunks=1000)
    geotrans = inRaster.GetGeoTransform()
    proj = inRaster.GetProjection()
    cols = inRaster.RasterXSize
    rows = inRaster.RasterYSize
    inRaster = None

    Qcalmax = md.get_value('QCALMAX_BAND' + band, 'Value')
    Qcalmin = md.get_value('QCALMIN_BAND' + band, 'Value')
    Lmax = md.get_value('LMAX_BAND' + band, 'Value')
    Lmin = md.get_value('LMIN_BAND' + band, 'Value')
    offset = ((Lmax - Lmin) / (Qcalmax - Qcalmin))
    Rad = offset * (daskarr - Qcalmin) + Lmin
    Rad = Rad.compute()

    outName = "B" + band + '_Rad.TIF'
    driver = gdal.GetDriverByName('GTiff')
    outRaster = driver.Create(os.path.join(path, outName), cols, rows, 1, gdal.GDT_Float32)
    outRaster.SetGeoTransform(geotrans)
    outRaster.SetProjection(proj)
    outRaster.GetRasterBand(1).WriteArray(Rad)
    outRaster.FlushCache()
    return outName


def calcReflectance(path, md, band, radianceRaster, solarDist, ESUN, solarElevation):
    RadRaster = gdal.Open(os.path.join(path, radianceRaster))
    daskarr = da.from_array(RadRaster.ReadAsArray(), chunks=1000)
    geotrans = RadRaster.GetGeoTransform()
    proj = RadRaster.GetProjection()
    cols = RadRaster.RasterXSize
    rows = RadRaster.RasterYSize
    RadRaster = None

    solarZenith = ((90.0 - (float(solarElevation))) * np.pi) / 180
    Refl = np.pi * solarDist ** 2 * daskarr / (ESUN * np.cos(solarZenith))
    Refl = Refl.compute()

    outName = 'B' + band + '_Refl.TIF'
    driver = gdal.GetDriverByName('GTiff')
    outRaster = driver.Create(os.path.join(path, outName), cols, rows, 1, gdal.GDT_Float32)
    outRaster.SetGeoTransform(geotrans)
    outRaster.SetProjection(proj)
    outRaster.GetRasterBand(1).WriteArray(Refl)
    outRaster.FlushCache()
    return outName


def calcSolarDist(jday):
    f = open('d.csv', "r")
    lines = f.readlines()[2:]

    distances = []
    for x in range(len(lines)):
        distances.append(float(lines[x].strip().split(',')[1]))
    f.close()

    jday = int(jday)
    dist = distances[jday - 1]
    return dist

def calcJDay(date):
    dt = date.rsplit("-")
    t = time.mktime((int(dt[0]), int(dt[1]), int(dt[2]), 0, 0, 0, 0, 0, 0))
    jday = time.gmtime(t)[7]
    return jday

def getESUN(bandNum, SIType):
    SIType = SIType
    ESUN = {}
    if SIType == 'ETM+ Thuillier':
        ESUN = {'b1':1997,'b2':1812,'b3':1533,'b4':1039,'b5':230.8,'b7':84.90,'b8':1362}
    if SIType == 'ETM+ ChKur':
        ESUN = {'b1':1970,'b2':1842,'b3':1547,'b4':1044,'b5':225.7,'b7':82.06,'b8':1369}
    if SIType == 'LPS ACAA Algorithm':
        ESUN = {'b1':1969,'b2':1840,'b3':1551,'b4':1044,'b5':225.7,'b7':82.06,'b8':1368}
    if SIType == 'Landsat 5 ChKur':
        ESUN = {'b1':1957,'b2':1825,'b3':1557,'b4':1033,'b5':214.9,'b7':80.72}
    if SIType == 'Landsat 4 ChKur':
        ESUN = {'b1':1957,'b2':1826,'b3':1554,'b4':1036,'b5':215,'b7':80.67}

    bandNum = str(bandNum)
    return ESUN[bandNum]


bandList = [2]
path = r"D:\GIS\Landsat7 Data"
metadata = acquireMetadata(path)
SIType = 'ETM+ Thuillier'
for band in bandList:
    band = str(band)
    radianceRaster = calcRadiance(path, metadata, band)
    calcReflectance(path, metadata, band, radianceRaster, calcSolarDist(calcJDay(metadata.get_value('ACQUISITION_DATE', 'Value'))), getESUN("b" + band, SIType), metadata.get_value('SUN_ELEVATION', 'Value'))