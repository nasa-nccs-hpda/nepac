import certifi
import os
import re
import urllib3
import warnings

from nepac.model.libraries.obdaac_download import httpdl
from nepac.model.Retriever import Retriever


# -----------------------------------------------------------------------------
# class OcSWFHICOCTRetriever
#
# https://oceandata.sci.gsfc.nasa.gov
# https://oceancolor.gsfc.nasa.gov/data/download_methods/#api
# -----------------------------------------------------------------------------
class OcSWFHICOCTRetriever(Retriever):

    SPECIAL_VALUE_FUNCTION = False

    # Observation processing level to look for.
    PROCESSING_LEVEL = 'L2'

    # Part of a URL used to download OB DAAC files.
    OBDAAC_GETFILE_UR = '/ob/getfile/'

    # Regex pattern to filter out valid OB DAAC L2 files from HTTP responses.
    FILE_PATTERN = "[0-9]*[0-9a-z]\.[0-9A-Z]*[_][0-9A-Z]*[_][O][C]\.[a-z]*"

    # Types of files we do not want to search through.
    INVALID_STR = {
        'SeaWiFS': 'MLAC',
        'OCTS': 'MLAC'
    }

    BASE_URL = 'oceandata.sci.gsfc.nasa.gov'
    GEOREFERENCED = False
    LAT_LON_INDEXING = True

    # NetCDF Subdataset group which houses all nav data.
    NAVIGATION_GROUP = 'navigation_data'

    # NetCDF Subdataset group which houses all geophysical data.
    GEOPHYSICAL_GROUP = 'geophysical_data'

    # Placeholder index variable if no valid location was found.
    NO_DATA_IDX = -1

    # -------------------------------------------------------------------------
    # __init__
    # -------------------------------------------------------------------------
    def __init__(self,
                 mission,
                 dateTime,
                 dummyPath,
                 lonLat=None,
                 dayNightFlag='',
                 outputDirectory='.'):

        super().__init__(mission,
                         dateTime,
                         outputDirectory)

        self._error = self.validateLonLat(lonLat, error=self._error)
        self._error = self.validate(mission, dateTime, error=self._error)

        self._lonLat = lonLat
        self._dummyPath = dummyPath
        self._dayNightFlag = dayNightFlag
        self.xIdx = None
        self.yIdx = None

    # -------------------------------------------------------------------------
    # run()
    #
    # Given a set of parameters on init (time, location, mission), search for
    # and download the most relevant file. This uses HTTP lists of files on
    # a per-day basis. We don't know which file has the righ orbit, so we
    # begin to test each Global file.
    # -------------------------------------------------------------------------
    def run(self):
        fileList = self.getFileLinks()
        dataset, self.xIdx, self.yIdx = self.runDownloadTestExtractGroup(
            fileList,
            error=self._error
        )
        return dataset, None, self._error

    # -------------------------------------------------------------------------
    # _getFileLinks()
    #
    # Get (HTTP) all files from the OB DAAAC given a certain instrument and
    # date.
    # -------------------------------------------------------------------------
    def getFileLinks(self):

        if self._error:
            return ['ERROR.nc']

        url = self.buildRequestURL()
        fileList = []

        with urllib3.PoolManager(cert_reqs='CERT_REQUIRED',
                                 ca_certs=certifi.where()) as httpPoolMan:

            try:
                response = httpPoolMan.request('GET', url)
            except urllib3.exceptions.MaxRetryError:
                self._error = True
                return ['ERROR.nc']

            data = response.data.decode('utf-8')
            fileList = self.matchResponseFiles(data)

        if len(fileList) == 0:
            fileList.append('ERROR.nc')
            self._error = True

        return fileList

    # -------------------------------------------------------------------------
    # _buildRequestURL()
    #
    # Build a HTTP request to get all files for a certain instrument given
    # a date.
    # -------------------------------------------------------------------------
    def buildRequestURL(self):
        julienDay = self._dateTime.timetuple()
        julienDay = julienDay.tm_yday
        url = '{}/{}/{}/{:04}/{:03}'.format(
            self.BASE_URL,
            self._mission,
            self.PROCESSING_LEVEL,
            self._dateTime.year,
            julienDay)
        return url

    # -------------------------------------------------------------------------
    # _matchResponseFiles()
    #
    # Search for valid links to observation files. Only add links to list
    # if they are not invalid file types.
    # -------------------------------------------------------------------------
    def matchResponseFiles(self, response):
        fileList = []
        searchStr = self.BASE_URL + self.OBDAAC_GETFILE_UR + \
            self._mission[0] + self.FILE_PATTERN
        dataMatch = re.findall(searchStr, response)
        for link in dataMatch:
            if self.INVALID_STR[self._mission] not in link:
                fileList.append(link)
        return fileList

    # -------------------------------------------------------------------------
    # _runDownloadTestExtractGroup()
    # -------------------------------------------------------------------------
    def runDownloadTestExtractGroup(self, fileList, error=False):
        dataset = None
        if error:
            dataset, _, self._error = self.extractAndMergeDataset(
                fileList[0],
                self._dummyPath,
                removeFile=False,
                mission=self._mission,
                error=error
            )

            return dataset, self.NO_DATA_IDX, self.NO_DATA_IDX

        for ocFileUrl in fileList:

            fileURL = ocFileUrl.split('.gov')[1]
            fileName = ocFileUrl.split('getfile/')[1]

            request_status = httpdl(self.BASE_URL,
                                    fileURL,
                                    localpath=self._outputDirectory,
                                    uncompress=True)

            if not self.catchHTTPError(request_status):

                filePath = os.path.join(self._outputDirectory,
                                        fileName)

                dataset, _, self._error = self.extractAndMergeDataset(
                    filePath,
                    self._dummyPath,
                    removeFile=True,
                    mission=self._mission,
                    error=error
                )

                trueLonLat = (float(self._lonLat[0]),
                              float(self._lonLat[1]))

                xIdx, yIdx = self.geoLocate(dataset,
                                            trueLonLat[1],
                                            trueLonLat[0],
                                            quiet=True)

                if not xIdx == self.NO_DATA_IDX \
                        and not yIdx == self.NO_DATA_IDX:
                    return dataset, xIdx, yIdx

            # File not found (client error).
            else:

                msg = 'Client or server error: ' + str(request_status) + \
                    '. ' + fileName
                warnings.warn(msg)
                self._error = True

                dataset, _, self._error = self.extractAndMergeDataset(
                    fileList[0],
                    self._dummyPath,
                    removeFile=False,
                    mission=self._mission,
                    error=error
                )

                return dataset, self.NO_DATA_IDX, self.NO_DATA_IDX

        return dataset, self.NO_DATA_IDX, self.NO_DATA_IDX
