import datetime
import tempfile
import unittest

from nepac.model.EtopoRetriever import EtopoRetriever


# -----------------------------------------------------------------------------
# class EtopoRetrieverTestCase
#
# singularity shell -B /explore,/panfs,/tmp
# /explore/nobackup/people/iluser/ilab_containers/nepac-2.2.0.sif
# cd to the directory containing nepac
# export PYTHONPATH=`pwd`:`pwd`/nepac
# python -m unittest discover model/tests/
# python -m unittest model.tests.test_EtopoRetriever
# -----------------------------------------------------------------------------
class EtopoRetrieverTestCase(unittest.TestCase):

    NEPAC_DISK_DATASETS = '/usr/local/ilab/nepac_datasets'

    # -------------------------------------------------------------------------
    # testInit
    # -------------------------------------------------------------------------
    def testInit(self):
        validDateTime = datetime.datetime(2004, 1, 1)
        invalidDateTime = datetime.datetime(1970, 1, 1)
        validLocation = ('-76.51005', '39.07851')

        # Test invalid mission.
        with self.assertRaisesRegex(RuntimeError, 'Invalid mission:'):

            EtopoRetriever('invalidMission', validDateTime,
                           self.NEPAC_DISK_DATASETS, validLocation)

        rt = EtopoRetriever('ETOPO1-BED', invalidDateTime,
                            self.NEPAC_DISK_DATASETS, validLocation)
        self.assertTrue(rt._error)
        # Test valid everything.
        EtopoRetriever('ETOPO1-ICE', validDateTime,
                       self.NEPAC_DISK_DATASETS, validLocation)

    # -------------------------------------------------------------------------
    # testIsValidDataSet
    # -------------------------------------------------------------------------
    def testIsValidDataSet(self):

        self.assertFalse(EtopoRetriever.isValidDataSet(
            'ETOPO1-BED',
            'invalid'))

        self.assertTrue(EtopoRetriever.isValidDataSet(
            'ETOPO1-BED',
            'z'))

    # -------------------------------------------------------------------------
    # testIsValidLocation
    # -------------------------------------------------------------------------
    def testIsValidLocation(self):
        validDateTime = datetime.datetime(year=2018,
                                          month=12,
                                          day=11,
                                          hour=13)
        invalidLonLocation = ('-200', '20')
        invalidLatLocation = ('-180', '180')
        validLocation = ('-76.51005', '39.07851')

        rt = EtopoRetriever('ETOPO1-ICE',
                            validDateTime,
                            self.NEPAC_DISK_DATASETS,
                            invalidLonLocation)
        self.assertTrue(rt._error)

        rt = EtopoRetriever('ETOPO1-BED',
                            validDateTime,
                            self.NEPAC_DISK_DATASETS,
                            invalidLatLocation)
        self.assertTrue(rt._error)

        EtopoRetriever('ETOPO1-BED', validDateTime,
                       self.NEPAC_DISK_DATASETS, validLocation)

    # -------------------------------------------------------------------------
    # testRun
    # -------------------------------------------------------------------------
    def testRun(self):
        tmp_directory = tempfile.gettempdir()

        # ---
        # Test valid date time.
        # We test multiple missions due to differing date ranges.
        # ---
        validEtopoDt = datetime.datetime(year=2018,
                                         month=10,
                                         day=31,
                                         hour=18,
                                         minute=45)
        validEtopoLoc = ('13.30553', '36.42652')

        etopoBedR = EtopoRetriever('ETOPO1-BED',
                                   validEtopoDt,
                                   self.NEPAC_DISK_DATASETS,
                                   validEtopoLoc,
                                   outputDirectory=tmp_directory)
        etopoBedR.run()

        etopoIceR = EtopoRetriever('ETOPO1-ICE',
                                   validEtopoDt,
                                   self.NEPAC_DISK_DATASETS,
                                   validEtopoLoc,
                                   outputDirectory=tmp_directory)
        etopoIceR.run()
