import os

from nepac.model.Retriever import Retriever


# -----------------------------------------------------------------------------
# class EtopoRetriever
#
# ETOPO1 data retriever
# -----------------------------------------------------------------------------
class EtopoRetriever(Retriever):
    SPECIAL_VALUE_FUNCTION = False
    LAT_LON_INDEXING = False
    GEOREFERENCED = True

    ETOPO1_MISSION_DICTIONARY = {
        'ETOPO1-BED': 'ETOPO1_Bed_g_gmt4.grd',
        'ETOPO1-ICE': 'ETOPO1_Ice_g_gmt4.grd'
    }

    # -------------------------------------------------------------------------
    # __init__
    # -------------------------------------------------------------------------
    def __init__(self,
                 mission,
                 dateTime,
                 dummyPath,
                 lonLat=None,
                 outputDirectory='.'):

        super().__init__(mission,
                         dateTime,
                         outputDirectory)

        self._error = self.validateLonLat(lonLat, error=self._error)
        self._error = self.validate(mission, dateTime, error=self._error)
        self._lonLat = lonLat
        self._dummyPath = dummyPath

    # -------------------------------------------------------------------------
    # run()
    #
    # Given a set of parameters on init (time, location, mission), search for
    # and return the path to the desired dataset.
    # -------------------------------------------------------------------------
    def run(self):

        outputPath = os.path.join(
            self._dummyPath,
            self.ETOPO1_MISSION_DICTIONARY[self._mission]
        )

        self._error = self.validateRequestedFile(outputPath,
                                                 self._mission,
                                                 error=self._error)

        return self.extractDataset(outputPath,
                                   self._dummyPath,
                                   latLonIndexing=self.LAT_LON_INDEXING,
                                   removeFile=False,
                                   mission=self._mission,
                                   error=self._error)
