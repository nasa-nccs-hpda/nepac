import os

from nepac.model.Retriever import Retriever


# -----------------------------------------------------------------------------
# class OccciRetriever
#
# https://ccdatahub.ipsl.fr/ocean-colour-climate-change-initiative-oc-cci/
# https://docs.pml.space/share/s/okB2fOuPT7Cj2r4C5sppDg
# -----------------------------------------------------------------------------
class OccciRetriever(Retriever):
    SPECIAL_VALUE_FUNCTION = False
    GEOREFERENCED = True
    LAT_LON_INDEXING = True
    BUFFER_SIZE = 1024
    DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    BASE_URL = 'https://rsg.pml.ac.uk/thredds/ncss/CCI_ALL-v5.0-DAILY'
    SUBDATASETS = ['Rrs_412', 'Rrs_443', 'Rrs_490', 'Rrs_510', 'Rrs_560',
                   'Rrs_665', 'Rrs_412_rmsd', 'Rrs_443_rmsd', 'Rrs_490_rmsd',
                   'Rrs_510_rmsd', 'Rrs_560_rmsd', 'Rrs_665_rmsd', 'kd_490']

    # -------------------------------------------------------------------------
    # __init__
    # -------------------------------------------------------------------------
    def __init__(self,
                 mission,
                 dateTime,
                 dummyPath,
                 lonLat=None,
                 subDatasets=['Rrs_412', 'Rrs_443', 'Rrs_490', 'Rrs_510',
                              'Rrs_560', 'Rrs_665', 'Rrs_412_rmsd',
                              'Rrs_443_rmsd', 'Rrs_490_rmsd', 'Rrs_510_rmsd',
                              'Rrs_560_rmsd', 'Rrs_665_rmsd', 'kd_490'],
                 outputDirectory='.'):

        super().__init__(mission,
                         dateTime,
                         outputDirectory)

        self._outputFile = 'oc_cci_subset_{}.nc'.format(
            self._dateTime.strftime('%Y%m%d%H%M%S')
        )

        self._error = self.validate(mission,
                                    dateTime,
                                    error=self._error)
        self._error = self.validateLonLat(lonLat,
                                          error=self._error)
        self._lonLat = lonLat
        self._dummyPath = dummyPath
        self._subDatasets = subDatasets

    # -------------------------------------------------------------------------
    # run()
    #
    # Given a set of parameters on init (time, location, mission), search for
    # and download the most relevant file. This uses OC-CCI's NetCDF subset
    # tool to subset a file given our parameters.
    # -------------------------------------------------------------------------
    def run(self):
        requestList = self.buildRequest(self._dateTime,
                                        self.DATE_FORMAT,
                                        self._subDatasets,
                                        self._lonLat,
                                        eclipticLon=False)

        outputPath = os.path.join(self._outputDirectory,
                                  self._outputFile)

        self._error = self.sendRequest(requestList,
                                       outputPath)

        self._error = self.validateRequestedFile(outputPath,
                                                 self._mission,
                                                 error=self._error)

        return self.extractDataset(outputPath,
                                   self._dummyPath,
                                   latLonIndexing=self.LAT_LON_INDEXING,
                                   mission=self._mission,
                                   error=self._error)
