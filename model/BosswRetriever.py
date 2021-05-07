import os

from nepac.model.Retriever import Retriever


# -----------------------------------------------------------------------------
# class BosswRetriever
#
# Blended Ocean Sea Surface Wind (Daily) data retriever
#
# https://www.ncdc.noaa.gov/data-access/marineocean-data/
# blended-global/blended-sea-winds
# -----------------------------------------------------------------------------
class BosswRetriever(Retriever):
    SPECIAL_VALUE_FUNCTION = False
    GEOREFERENCED = True
    LAT_LON_INDEXING = True
    BUFFER_SIZE = 1024
    DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    BASE_URL = 'https://www.ncei.noaa.gov/thredds/ncss/uv/daily-strs'
    SUBDATASETS = ['tau', 'taux', 'tauy']

    # -------------------------------------------------------------------------
    # __init__
    # -------------------------------------------------------------------------
    def __init__(self,
                 mission,
                 dateTime,
                 dummyPath,
                 lonLat=None,
                 subDatasets=['tau', 'taux', 'tauy'],
                 outputDirectory='.'):

        super().__init__(mission,
                         dateTime,
                         outputDirectory)

        self.OUTPUT_FILE_DEF = 'bo_ssw_subset_{}.nc'.format(
            self._dateTime.strftime('%Y%m%d%H%M%S'))
        self._error = self.validate(mission,
                                    dateTime,
                                    error=self._error)
        self._error = self.validateLonLat(lonLat,
                                          error=self._error)
        self._dummyPath = dummyPath
        self._lonLat = lonLat
        self._subDatasets = subDatasets

    # -------------------------------------------------------------------------
    # run()
    #
    # Given a set of parameters on init (time, location, mission), search for
    # and download the most relevant file. This uses THREADSS' NetCDF subset
    # tool to subset a file given our parameters.
    # -------------------------------------------------------------------------
    def run(self):
        requestList = self.buildRequest(self._dateTime,
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
                                   mission=self._mission,
                                   latLonIndexing=self.LAT_LON_INDEXING,
                                   error=self._error)

    # -------------------------------------------------------------------------
    # _buildURL()
    #
    # Build a URL given a datetime for BO-SSW-specific THREADSS server.
    # -------------------------------------------------------------------------
    def _buildURL(self):
        year = self._dateTime.year
        month = self._dateTime.month
        day = self._dateTime.day
        fullURL = '{}/{:04}/tauxy{:04}{:02}{:02}.nc'.format(
            self.BASE_URL, year, year, month, day)
        return fullURL
