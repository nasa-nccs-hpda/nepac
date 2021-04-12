import os

from nepac.model.RetrieverBase import Retriever


# -----------------------------------------------------------------------------
# class PosstRetriever
#
# https://podaac.jpl.nasa.gov/dataset/AVHRR_OI-NCEI-L4-GLOB-v2.1
# https://podaac.jpl.nasa.gov/forum/viewtopic.php?f=5&t=219
# -----------------------------------------------------------------------------
class PosstRetriever(Retriever):
    SPECIAL_VALUE_FUNCTION = True
    KELVIN_SUBTRACTION_VAL = 273.15
    GEOREFERENCED = True
    LAT_LON_INDEXING = True
    BUFFER_SIZE = 1024
    DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    BASE_URL = 'https://thredds.jpl.nasa.gov/thredds/ncss/OceanTemperature'
    DATASET = 'AVHRR_OI-NCEI-L4-GLOB-v2.1.nc'
    SUBDATASETS = ['analysed_sst']

    # -------------------------------------------------------------------------
    # __init__
    # -------------------------------------------------------------------------
    def __init__(self,
                 mission,
                 dateTime,
                 lonLat=None,
                 subDatasets=['analysed_sst'],
                 outputDirectory='.'):

        super().__init__(mission,
                         dateTime,
                         outputDirectory)

        self._outputFile = 'po_sst_subset_{}.nc'.format(
            self._dateTime.strftime('%Y%m%d%H%M%S'))

        self._error = self.validate(mission, dateTime, error=self._error)
        self._error = self.validateLonLat(lonLat, error=self._error)
        self._lonLat = lonLat
        self._subDatasets = subDatasets

    # -------------------------------------------------------------------------
    # run()
    #
    # Given a set of parameters on init (time, location, mission), search for
    # and download the most relevant file. This uses OC-CCI's NetCDF subset
    # tool to subset a file given our parameters.
    #
    # relevancePlace is used so in the future if we ever need to query a result
    # that is not the most relevant. (E.g. The most relevant file was too
    # cloudy, etc)
    # -------------------------------------------------------------------------
    def run(self):
        requestList = Retriever.buildRequest(self._dateTime,
                                             self.DATE_FORMAT,
                                             self._subDatasets,
                                             self._lonLat,
                                             eclipticLon=False)

        outputPath = os.path.join(self._outputDirectory,
                                  self._outputFile)

        self._error = self.sendRequest(requestList,
                                       outputPath,
                                       customURL=self._buildUrl())

        self._error = self.validateRequestedFile(outputPath,
                                                 self._mission,
                                                 error=self._error)

        return self.extractDataset(outputPath,
                                   mission=self._mission,
                                   latLonIndexing=self.LAT_LON_INDEXING,
                                   error=self._error)

    # -------------------------------------------------------------------------
    # _buildUrl()
    # -------------------------------------------------------------------------
    def _buildUrl(self):
        return os.path.join(self.BASE_URL, self.DATASET)

    # -------------------------------------------------------------------------
    # retrieverValueFunction()
    # -------------------------------------------------------------------------
    def retrieverValueFunction(self, value):
        return float(value - self.KELVIN_SUBTRACTION_VAL)
