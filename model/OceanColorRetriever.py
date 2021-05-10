import os
import warnings

from nepac.model.libraries.obdaac_download import httpdl
from nepac.model.CmrProcess import CmrProcess
from nepac.model.Retriever import Retriever


# -----------------------------------------------------------------------------
# class OceanColorRetriever
#
# https://oceandata.sci.gsfc.nasa.gov
# https://oceancolor.gsfc.nasa.gov/data/download_methods/#api
# -----------------------------------------------------------------------------
class OceanColorRetriever(Retriever):

    SPECIAL_VALUE_FUNCTION = False
    BASE_URL = 'oceandata.sci.gsfc.nasa.gov'
    GEOREFERENCED = False
    LAT_LON_INDEXING = True

    # NetCDF Subdataset group which houses all nav data.
    NAVIGATION_GROUP = 'navigation_data'

    # NetCDF Subdataset group which houses all geophysical data.
    GEOPHYSICAL_GROUP = 'geophysical_data'

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

        self._error = self.validate(mission,
                                    dateTime,
                                    error=self._error)
        self._error = self.validateLonLat(lonLat, error=self._error)

        self._lonLat = lonLat
        self._dummyPath = dummyPath
        self._dayNightFlag = dayNightFlag

    # -------------------------------------------------------------------------
    # run()
    #
    # Given a set of parameters on init (time, location, mission), search for
    # and download the most relevant file. This uses CMR to search metadata for
    # relevant matches, it then uses the ODBAAC download script to pull the
    # best match from the OB.DAAC.
    # -------------------------------------------------------------------------
    def run(self):

        cmrRequest = CmrProcess(self._mission,
                                self._dateTime,
                                self._lonLat,
                                error=self._error)

        fileURL, fileName, cmrRequestDict, self._error = cmrRequest.run()

        if self._error:
            return self.extractAndMergeDataset(
                'ERROR',
                self._dummyPath,
                removeFile=False,
                mission=self._mission,
                error=self._error
            )

        fileURL = fileURL.split('.gov/cmr')[1]
        fileURL = '/ob'+fileURL

        # Download the data set.
        request_status = httpdl(OceanColorRetriever.BASE_URL,
                                fileURL,
                                localpath=self._outputDirectory,
                                uncompress=True)

        # File was retrieved, or file was already present.
        if request_status == 0 or request_status == 200 \
                or request_status == 304:

            filePath = os.path.join(self._outputDirectory,
                                    fileName)

            self._error = self.validateRequestedFile(filePath, self._mission)

            return self.extractAndMergeDataset(
                filePath,
                self._dummyPath,
                removeFile=True,
                mission=self._mission,
                error=self._error
            )

        # File not found (client error).
        else:

            msg = 'Client or server error: ' + str(request_status) + \
                '. ' + fileName
            self._error = True
            warnings.warn(msg)

            return self.extractAndMergeDataset(
                'ERROR',
                removeFile=False,
                mission=self._mission,
                error=self._error
            )
