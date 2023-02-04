import datetime
import tempfile
import unittest

from nepac.model.BosswRetriever import BosswRetriever


# -----------------------------------------------------------------------------
# class OceanColorRetrieverTestCase
#
# singularity shell -B /explore,/panfs,/tmp
# /explore/nobackup/people/iluser/ilab_containers/nepac-2.2.0.sif
# cd to the directory containing nepac
# export PYTHONPATH=`pwd`:`pwd`/nepac
# python -m unittest discover model/tests/
# python -m unittest model.tests.test_OceanColorRetriever
# -----------------------------------------------------------------------------
class OceanColorRetrieverTestCase(unittest.TestCase):

    NEPAC_DISK_DATASETS = '/usr/local/ilab/nepac_datasets'

    # -------------------------------------------------------------------------
    # testInit
    # -------------------------------------------------------------------------
    def testInit(self):
        validDateTime = datetime.datetime(2010, 1, 1)
        invalidDateTime = datetime.date.today()
        validLocation = ('-76.51005', '39.07851')

        # Test invalid mission.
        with self.assertRaisesRegex(RuntimeError, 'Invalid mission:'):

            BosswRetriever(
                'invalidMission', validDateTime,
                self.NEPAC_DISK_DATASETS, validLocation)

        rt = BosswRetriever('BO-SSW', invalidDateTime,
                            self.NEPAC_DISK_DATASETS, validLocation)
        self.assertTrue(rt._error)

        # Test valid everything.
        BosswRetriever('BO-SSW', validDateTime,
                       self.NEPAC_DISK_DATASETS, validLocation)

    # -------------------------------------------------------------------------
    # testIsValidDataSet
    # -------------------------------------------------------------------------
    def testIsValidDataSet(self):

        self.assertFalse(BosswRetriever.isValidDataSet(
            'BO-SSW',
            'invalid'))

        self.assertTrue(BosswRetriever.isValidDataSet(
            'BO-SSW',
            'tau'))

    # -------------------------------------------------------------------------
    # testIsValidLocation
    # -------------------------------------------------------------------------
    def testIsValidLocation(self):
        validDateTime = datetime.datetime(year=2010,
                                          month=12,
                                          day=11,
                                          hour=13)
        invalidLonLocation = ('-200', '20')
        invalidLatLocation = ('-180', '180')
        validLocation = ('-76.51005', '39.07851')

        rt = BosswRetriever('BO-SSW',
                            validDateTime,
                            self.NEPAC_DISK_DATASETS,
                            invalidLonLocation)
        self.assertTrue(rt._error)

        rt = BosswRetriever('BO-SSW',
                            validDateTime,
                            self.NEPAC_DISK_DATASETS,
                            invalidLatLocation)
        self.assertTrue(rt._error)

        BosswRetriever('BO-SSW', validDateTime,
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
        validBosswDt = datetime.datetime(year=2010,
                                         month=10,
                                         day=31,
                                         hour=18,
                                         minute=45)
        validBosswLoc = ('13.30553', '36.42652')

        bosswR = BosswRetriever('BO-SSW',
                                validBosswDt,
                                self.NEPAC_DISK_DATASETS,
                                validBosswLoc,
                                outputDirectory=tmp_directory)
        bosswR.run()
