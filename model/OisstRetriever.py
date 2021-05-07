import os

from nepac.model.Retriever import Retriever


# -----------------------------------------------------------------------------
# class OisstRetriever
#
# https://www.ncdc.noaa.gov/oisst/optimum-interpolation-sea-
# surface-temperature-oisst-v21
# https://www.ncdc.noaa.gov/oisst/data-access
# -----------------------------------------------------------------------------
class OisstRetriever(Retriever):
    SPECIAL_VALUE_FUNCTION = False
    LAT_LON_INDEXING = True
    GEOREFERENCED = True
    BUFFER_SIZE = 1024
    DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    BASE_URL = 'https://www.ncei.noaa.gov/thredds/ncss/OisstBase/' + \
        'NetCDF/V2.1/AVHRR'
    SUBDATASETS = ['sst']

    # -------------------------------------------------------------------------
    # __init__
    # -------------------------------------------------------------------------
    def __init__(self,
                 mission,
                 dateTime,
                 dummyPath,
                 lonLat=None,
                 subDatasets=['sst'],
                 outputDirectory='.'):

        super().__init__(mission,
                         dateTime,
                         outputDirectory)

        self.OUTPUT_FILE_DEF = 'oi_sst_subset_{}.nc'.format(
            self._dateTime.strftime('%Y%m%d%H%M%S'))

        self._error = self.validate(mission, dateTime, error=self._error)
        self._error = self.validateLonLat(lonLat, error=self._error)
        self._lonLat = lonLat
        self._dummyPath = dummyPath
        self._subDatasets = subDatasets

    # -------------------------------------------------------------------------
    # run()
    #
    # Given a set of parameters on init (time, location, mission), search for
    # and download the most relevant file. This uses OI-SST's NetCDF subset
    # tool to subset a file given our parameters.
    # -------------------------------------------------------------------------
    def run(self):
        requestList = Retriever.buildRequest(self._dateTime,
                                             self.DATE_FORMAT,
                                             self._subDatasets,
                                             self._lonLat,
                                             eclipticLon=True)

        outputPath = os.path.join(self._outputDirectory,
                                  self.OUTPUT_FILE_DEF)

        self._error = self.sendRequest(requestList,
                                       outputPath,
                                       customURL=self._buildURL())

        self._error = self.validateRequestedFile(outputPath,
                                                 self._mission,
                                                 error=self._error)

        return self.extractDataset(outputPath,
                                   self._dummyPath,
                                   latLonIndexing=self.LAT_LON_INDEXING,
                                   mission=self._mission,
                                   error=self._error)

    # -------------------------------------------------------------------------
    # _buildURL()
    #
    # Build a URL given a datetime for OI-SST-specific THREADSS SERVER
    # -------------------------------------------------------------------------
    def _buildURL(self):
        year = self._dateTime.year
        month = self._dateTime.month
        day = self._dateTime.day
        fullURL = '{}/{:04}{:02}/oisst-avhrr-v02r01.{:04}{:02}{:02}.nc'.format(
            self.BASE_URL, year, month, year, month, day)
        return fullURL
