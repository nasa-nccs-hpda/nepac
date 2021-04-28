import certifi
import datetime
import numpy as np
import os
import pandas
import urllib3
from urllib.parse import urlencode
import warnings
import xarray as xr

from nepac.model.CmrProcess import CmrProcess


# -----------------------------------------------------------------------------
# class Retriever
#
# This is a base class for each retriever NEPAC Data Retriever needs. This
# class implements methods considered to be useful to all data retrievers.
# -----------------------------------------------------------------------------
class Retriever(object):

    # This is encoded from John Moisan's "NEPAC Input Control.docx".
    MISSION_DATASETS = {
        'MODIS-Aqua': ['chlor_a', 'ipar', 'Kd_490', 'par', 'pic', 'poc',
                       'Rrs_412', 'Rrs_443', 'Rrs_469', 'Rrs_488', 'Rrs_531',
                       'Rrs_547', 'Rrs_555', 'Rrs_645', 'Rrs_667', 'Rrs_678'],

        'CZCS': ['chlor_a', 'Kd_490', 'Rrs_443', 'Rrs_520', 'Rrs_550',
                 'Rrs_670'],

        'GOCI': ['chlor_a', 'Kd_490', 'poc', 'Rrs_412', 'Rrs_443', 'Rrs_490',
                 'Rrs_555', 'Rrs_660', 'Rrs_680'],

        'HICO': ['Kd_490', 'pic', 'poc', 'Rrs_353', 'Rsf_358', 'Rrs_364',
                 'Rrs_370', 'Rrs_375', 'Rrs_381', 'Rrs_387', 'Rrs_393',
                 'Rrs_398', 'Rrs_404', 'Rrs_410', 'Rrs_416', 'Rrs_421',
                 'Rrs_427', 'Rrs_433', 'Rrs_438', 'Rrs_444', 'Rrs_450',
                 'Rrs_456', 'Rrs_461', 'Rrs_467', 'Rrs_473', 'Rrs_479',
                 'Rrs_484', 'Rrs_490', 'Rrs_496', 'Rrs_501', 'Rrs_507',
                 'Rrs_513', 'Rrs_519', 'Rrs_524', 'Rrs_530', 'Rrs_536',
                 'Rrs_542', 'Rrs_547', 'Rrs_553', 'Rrs_559', 'Rrs_564',
                 'Rrs_570', 'Rrs_576', 'Rrs_582', 'Rrs_587', 'Rrs_593',
                 'Rrs_599', 'Rrs_605', 'Rrs_610', 'Rrs_616', 'Rrs_622',
                 'Rrs_627', 'Rrs_633', 'Rrs_639', 'Rrs_645', 'Rrs_650',
                 'Rrs_656', 'Rrs_662', 'Rrs_668', 'Rrs_673', 'Rrs_679',
                 'Rrs_685', 'Rrs_690', 'Rrs_696', 'Rrs_702', 'Rrs_708',
                 'Rrs_713', 'Rrs_719', 'chlor_a'],

        'OCTS': ['chlor_a', 'Kd_490', 'par', 'pic', 'poc', 'Rrs_412',
                 'Rrs_443', 'Rrs_490', 'Rrs_516', 'Rrs_565', 'Rrs_667'],

        'SeaWiFS': ['chlor_a', 'Kd_490', 'par', 'pic', 'poc', 'Rrs_412',
                    'Rrs_443', 'Rrs_490', 'Rrs_555', 'Rrs_670'],

        'MODIS-Terra': ['chlor_a', 'ipar', 'Kd_490', 'par', 'pic', 'poc',
                        'Rrs_412', 'Rrs_443', 'Rrs_469', 'Rrs_488', 'Rrs_531',
                        'Rrs_547', 'Rrs_555', 'Rrs_645', 'Rrs_667', 'Rrs_678'],

        'VIIRS-SNPP': ['chlor_a', 'Kd_490', 'par', 'pic', 'poc', 'Rrs_410',
                       'Rrs_443', 'Rrs_486', 'Rrs_551', 'Rrs_671'],

        'VIIRS-JPSS1': ['chlor_a', 'Kd_490', 'par', 'pic', 'poc', 'Rrs_411',
                        'Rrs_445', 'Rrs_489', 'Rrs_556', 'Rrs_667'],

        'OC-CCI': ['Rrs_412', 'Rrs_443', 'Rrs_490', 'Rrs_510', 'Rrs_560',
                   'Rrs_665', 'Rrs_412_rmsd', 'Rrs_443_rmsd', 'Rrs_490_rmsd',
                   'Rrs_510_rmsd', 'Rrs_560_rmsd', 'Rrs_665_rmsd', 'kd_490'],

        'OI-SST': ['sst'],

        'BO-SSW': ['tau', 'taux', 'tauy'],

        'PO-SST': ['analysed_sst'],

        'ETOPO1-BED': ['z'],

        'ETOPO1-ICE': ['z']
    }

    # This is encoded from John Moisan's "NEPAC Input Control.docx".
    MISSION_DATES = {
        'MODIS-Aqua': pandas.date_range('2002-07-04', datetime.date.today()),
        'CZCS': pandas.date_range('1978-10-30', '1986-06-22'),
        'GOCI': pandas.date_range('2011-04-01', datetime.date.today()),
        'HICO': pandas.date_range('2009-09-25', '2014-09-13'),
        'OCTS': pandas.date_range('1996-11-01', '1997-06-30'),
        'SeaWiFS': pandas.date_range('1997-09-04', '2010-12-11'),
        'MODIS-Terra': pandas.date_range('2000-02-24', datetime.date.today()),
        'VIIRS-SNPP': pandas.date_range('2012-01-02', datetime.date.today()),
        'VIIRS-JPSS1': pandas.date_range('2017-11-29', datetime.date.today()),
        'OC-CCI': pandas.date_range('1997-09-04', '2020-12-31'),
        'OI-SST': pandas.date_range('1981-09-01', datetime.date.today()),
        'BO-SSW': pandas.date_range('1987-07-09', '2011-09-30'),
        'PO-SST': pandas.date_range('2016-01-01', datetime.date.today()),
        'ETOPO1-BED': pandas.date_range('1981-09-01', datetime.date.today()),
        'ETOPO1-ICE': pandas.date_range('1981-09-01', datetime.date.today())
    }

    # This is encoded from John Moisan's "NEPAC Input Control.docx".
    MISSION_DUMMY_DATASETS = {
        'MODIS-Aqua': 'MODISA.nc',
        'CZCS': 'CZCS.nc',
        'GOCI': 'GOCI.nc',
        'HICO': 'HICO.nc',
        'OCTS': 'OCTS.nc',
        'SeaWiFS': 'SEAWIFS.nc',
        'MODIS-Terra': 'MODIST.nc',
        'VIIRS-SNPP': 'VIIRSSNPP.nc',
        'VIIRS-JPSS1': 'VIIRSJPSS1.nc',
        'OC-CCI': 'OCCCI.nc',
        'OI-SST': 'OISST.nc',
        'BO-SSW': 'BOSSW.nc',
        'PO-SST': 'POSST.nc',
        'ETOPO1-BED': 'ETOPO1_Bed_g_gmt4.grd',
        'ETOPO1-ICE': 'ETOPO1_Ice_g_gmt4.grd'
    }

    DATASET_PATHS = 'nepac/model/datasets/'

    # Current flag to mask out of our data extraction process.
    CURRENT_FLAG = 'land'

    # Options to mask out of our data extraction process.
    L2_FLAGS_MASKS = {
        'default_prodfail': 1073742610,
        'prodfail': 1073741824,
        'default': 786,
        'default-land': 784,
        'land': 2}

    # NetCDF Subdataset group which houses all nav data.
    NAVIGATION_GROUP = 'navigation_data'

    # NetCDF Subdataset group which houses all geophysical data.
    GEOPHYSICAL_GROUP = 'geophysical_data'

    # Range for valid lon/lat
    LATITUDE_RANGE = (-90, 90)
    LONGITUDE_RANGE = (-180, 180)

    # ---
    # Indices given to NepacProcess if an error has occured attempting
    # to retrieve a non-georeferenced observation dataset.
    # ---
    PIXEL_ERROR_IDX = -1

    # -------------------------------------------------------------------------
    # __init__
    # -------------------------------------------------------------------------
    def __init__(self,
                 mission,
                 dateTime,
                 outputDirectory='.'):
        self._error = False
        self._mission = mission
        self._dateTime = dateTime
        self._outputDirectory = outputDirectory

    # -------------------------------------------------------------------------
    # buildRequest()
    #
    # Build a dictionary based off of parameters given on init.
    # This dictionary will be used to encode the http request to search
    # and subset a THREADS-based subsetting service.
    # -------------------------------------------------------------------------
    @staticmethod
    def buildRequest(dateTime, dateFormat, subDatasets, lonLat,
                     eclipticLon=False):
        requestList = []
        temporalWindow = CmrProcess.buildTemporalWindow(dateTime,
                                                        dateFormat)
        spatialWindow = Retriever.buildSpatialWindow(lonLat,
                                                     eclipticLon)
        for subDataset in subDatasets:
            requestList.append(('var', subDataset))
        for key, point in spatialWindow.items():
            requestList.append((key, point))
        requestList.append(('horizStride', '1'))
        requestList.append(('time_start', temporalWindow[0]))
        requestList.append(('time_end', temporalWindow[1]))
        requestList.append(('timeStride', '1'))
        return requestList

    # -------------------------------------------------------------------------
    # buildSpatialWindow()
    #
    # Build a dictionary based off of the location (lon, lat) that is a full
    # 2-degree window in all directions
    # -------------------------------------------------------------------------
    @staticmethod
    def buildSpatialWindow(lonLat, eclipticLon=False):
        lon = float(lonLat[0])
        if eclipticLon:
            lon = lon % 360
        lat = float(lonLat[1])
        spatialWindow = {}
        spatialWindow['east'] = str(lon + 1)
        spatialWindow['west'] = str(lon - 1)
        spatialWindow['north'] = str(lat + 1)
        spatialWindow['south'] = str(lat - 1)
        return spatialWindow

    # -------------------------------------------------------------------------
    # _sendRequest()
    #
    # Send an http request to a THREDDS-based NetCDF subset server.
    # Write data to disk. Catch any errors encountered, flag it.
    # -------------------------------------------------------------------------
    def sendRequest(self, requestList, outputPath, customURL=None):
        if self._error:
            return True
        with urllib3.PoolManager(cert_reqs='CERT_REQUIRED',
                                 ca_certs=certifi.where()) as httpPoolManager:
            encodedRequest = urlencode(requestList)
            urlToUse = self.BASE_URL if not customURL else customURL
            requestUrl = '{}?{}'.format(urlToUse, encodedRequest)
            try:
                request = httpPoolManager.request('GET',
                                                  requestUrl,
                                                  preload_content=False)
            except urllib3.exceptions.MaxRetryError:
                return True
            if self.catchHTTPError(int(request.status)):
                request.release_conn()
                return True
            with open(outputPath, 'wb') as outputFile:
                while True:
                    data = request.read(self.BUFFER_SIZE)
                    if not data:
                        break
                    outputFile.write(data)
            request.release_conn()
            return self._error

    # -------------------------------------------------------------------------
    # _extractAndMergeDataset()
    #
    # Given a dataset file, read into memory in a format that is NepacProcess
    # friendly. This particular dataset does require extra merging and/or
    # re-indexing.
    #
    # We attempt to catch whatever errors we come across, if an error is
    # encountered, flag it, and use a backup 'dummy' dataset.
    # -------------------------------------------------------------------------
    @staticmethod
    def extractAndMergeDataset(missionFile, removeFile=True, mission=None,
                               error=False):
        # Preemptive error check. Don't run below code if error.
        if error:
            missionFile, removeFile = Retriever.getDummyDataset(mission)
        try:
            dataArrayGeo = xr.open_dataset(missionFile,
                                           group=Retriever.GEOPHYSICAL_GROUP)
        except OSError:
            error = True
            missionFile, removeFile = Retriever.getDummyDataset(mission)
            dataArrayGeo = xr.open_dataset(
                missionFile,
                group=Retriever.GEOPHYSICAL_GROUP)
        try:
            dataArrayNav = xr.open_dataset(
                missionFile,
                group=Retriever.NAVIGATION_GROUP)
        except OSError:
            error = True
            missionFile, removeFile = Retriever.getDummyDataset(mission)
            dataArrayNav = xr.open_dataset(
                missionFile,
                group=Retriever.NAVIGATION_GROUP)

        dataArrayMerged = xr.merge(
            [dataArrayNav.latitude,
             dataArrayNav.longitude,
             dataArrayGeo])
        dataArrayGeo = None
        dataArrayNav = None

        if removeFile:
            os.remove(missionFile)

        return dataArrayMerged, None, error

    # -------------------------------------------------------------------------
    # getDummyDataset
    #
    # If an error has occured, instead of returning an error, return a "dummy"
    # dataset that mirrors the missions' file hierarchy so as to keep
    # NepacProcess running smooth even if an error was caught.
    # -------------------------------------------------------------------------
    @ staticmethod
    def getDummyDataset(mission):
        missionFile = os.path.join(Retriever.DATASET_PATHS,
                                   Retriever.MISSION_DUMMY_DATASETS[mission])
        removeFile = False
        return missionFile, removeFile

    # -------------------------------------------------------------------------
    # extractDataset()
    #
    # Given a dataset file, read into memory in a format that is NepacProcess
    # friendly. This particular dataset does not require extra merging and/or
    # re-indexing.
    #
    # We attempt to catch whatever errors we come across, if an error is
    # encountered, flag it, and use a backup 'dummy' dataset.
    # -------------------------------------------------------------------------
    @ staticmethod
    def extractDataset(missionFile, mission=None, latLonIndexing=True,
                       removeFile=True, error=False):
        # Preemptive check for an error. Don't run code below if error.
        if error:
            error = True
            missionFile, removeFile = Retriever.getDummyDataset(mission)
            print(missionFile)  # TMP
            print(removeFile)  # TMP

        try:
            dataset = xr.open_dataset(missionFile)
        except OSError:
            # Something happened, use a backup dataset
            error = True
            missionFile, removeFile = Retriever.getDummyDataset(mission)
            dataset = xr.open_dataset(missionFile)
        if not latLonIndexing:
            # For sanity's sake, rename these to their proper name.
            renamedDataset = dataset.rename_dims({'x': 'lon', 'y': 'lat'})
            renamedDataset = dataset.rename({'x': 'lon', 'y': 'lat'})
            if removeFile:
                os.remove(missionFile)
            dataset.close()
            return renamedDataset, latLonIndexing, error
        if removeFile:
            try:
                os.remove(missionFile)
            except FileNotFoundError:
                warnings.warn('Tried to remove file, none found.')
        return dataset, latLonIndexing, error

    # -------------------------------------------------------------------------
    # geoLocate()
    #
    # Given a non-georeferenced dataset, find the closest location and get
    # idxs for that location. Check if location found is within spatial window.
    # If the pixel found is spatially out of bounds, return no_data indices,
    # letting NepacProcess know that an error has occured.
    # -------------------------------------------------------------------------
    @staticmethod
    def geoLocate(dataset, lat, lon, quiet=True, error=False):
        if error:
            return Retriever.PIXEL_ERROR_IDX, Retriever.PIXEL_ERROR_IDX
        xIdx, yIdx = Retriever.getLatLonIndex(dataset,
                                              lat,
                                              lon)
        foundLatLon = (float(dataset.latitude[xIdx][yIdx]),
                       float(dataset.longitude[xIdx][yIdx]))
        latLonFound = Retriever.checkLatLonOutOfWindow((lat, lon),
                                                       foundLatLon)
        if not latLonFound:
            xIdx, yIdx = (-1, -1)
        if not quiet:
            print('True:  Lat={:.04} Lon={:.04}'.format(lat, lon))
            print('Found: Lat={:.04} Lon={:.04}'.format(
                foundLatLon[0],
                foundLatLon[1]))
        return xIdx, yIdx

    # -------------------------------------------------------------------------
    # validateRequestedFile()
    #
    # Make sure a file download via HTTP request actually exists.
    # -------------------------------------------------------------------------
    @staticmethod
    def validateRequestedFile(path, mission, error=False):
        if not os.path.exists(path):
            msg = 'Could not download requested file from mission: {}'.format(
                mission)
            warnings.warn(msg)
            return True
        return error

    # -------------------------------------------------------------------------
    # getLatLonIndex()
    #
    # Given a dataset that is not geo-referenced and a location, find the
    # closest location in the dataset and return the idxs for that spot.
    # -------------------------------------------------------------------------
    @staticmethod
    def getLatLonIndex(xrDset, lat, lon):
        nanarray = np.empty(xrDset.l2_flags.shape)
        nanarray.fill(np.nan)
        condition = (
            (xrDset.l2_flags &
             Retriever.L2_FLAGS_MASKS[Retriever.CURRENT_FLAG])
            > 0)
        condLats = np.where(condition,
                            nanarray, xrDset.latitude)
        condLons = np.where(condition,
                            nanarray, xrDset.longitude)
        diffSquared = (condLats - lat)**2 + (condLons - lon)**2
        argm = np.nanargmin(diffSquared)
        x_loc, y_loc = np.unravel_index(argm, diffSquared.shape)
        return x_loc, y_loc

    # -------------------------------------------------------------------------
    # checkLatLonOutOfWindow()
    #
    # Make sure that the location found in dataset is within desired spatial
    # window.
    # -------------------------------------------------------------------------
    @staticmethod
    def checkLatLonOutOfWindow(trueLatLon, foundLatLon, patience=0.5):
        latlonFoundOutOfBounds = True
        for i, latLon in enumerate(trueLatLon):
            abs_diff = abs(latLon - foundLatLon[i])
            if abs_diff > patience:
                latlonFoundOutOfBounds = False
        return latlonFoundOutOfBounds

    # -------------------------------------------------------------------------
    # isValidDataSet()
    # -------------------------------------------------------------------------
    @ staticmethod
    def isValidDataSet(mission, dataset):
        if dataset in Retriever.MISSION_DATASETS[mission]:
            return True
        else:
            return False

    # -------------------------------------------------------------------------
    # catchHTTPError()
    # -------------------------------------------------------------------------
    @staticmethod
    def catchHTTPError(status):
        return status < 600 and status >= 400

    # -------------------------------------------------------------------------
    # _validate()
    # -------------------------------------------------------------------------
    @staticmethod
    def validate(mission, dateTime, error=False):

        dateRangeNoHMS = datetime.datetime(dateTime.year,
                                           dateTime.month,
                                           dateTime.day)

        # Validate mission.
        if mission not in Retriever.MISSION_DATASETS:

            msg = 'Invalid mission: ' + str(mission) + \
                '.  Valid missions: ' + \
                str(Retriever.MISSION_DATASETS.keys())

            raise RuntimeError(msg)

        # Validate date is in date range of mission
        if dateRangeNoHMS not in Retriever.MISSION_DATES[mission]:

            msg = 'Invalid date: ' + str(dateTime) + \
                '. All values will be no_data values. ' + \
                'Valid date: ' + \
                str(Retriever.MISSION_DATES[mission])
            warnings.warn(msg)
            return True
        return error

    # -------------------------------------------------------------------------
    # _validateLonLat
    # -------------------------------------------------------------------------

    @staticmethod
    def validateLonLat(lonLat, error=False):

        lon = float(lonLat[0])
        lat = float(lonLat[1])

        if not Retriever.LONGITUDE_RANGE[0] <= lon \
                <= Retriever.LONGITUDE_RANGE[1]:

            msg = 'Invalid longitude: ' + str(lon) + \
                '. Valid longitude: ' + \
                str(Retriever.LONGITUDE_RANGE)

            warnings.warn(msg)
            return True

        if not Retriever.LATITUDE_RANGE[0] <= lat \
                <= Retriever.LATITUDE_RANGE[1]:

            msg = 'Invalid latitude: ' + str(lat) + \
                '. Valid latitude: ' + \
                str(Retriever.LATITUDE_RANGE)

            warnings.warn(msg)
            return True
        return error
