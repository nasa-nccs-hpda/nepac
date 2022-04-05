import datetime
import os
import tarfile
import tempfile
import unittest

from nepac.model.BosswRetriever import BosswRetriever


# -----------------------------------------------------------------------------
# class OceanColorRetrieverTestCase
#
# singularity shell -B /att
# /adapt/nobackup/people/iluser/containers/ilab-nepac-2.0.0.sif
# cd to the directory containing nepac
# export PYTHONPATH=`pwd`:`pwd`/nepac
# python -m unittest discover model/tests/
# python -m unittest model.tests.test_OceanColorRetriever
# -----------------------------------------------------------------------------
class OceanColorRetrieverTestCase(unittest.TestCase):

    # -------------------------------------------------------------------------
    # testInit
    # -------------------------------------------------------------------------
    def testInit(self):
        tmpDir = tempfile.gettempdir()
        tmpDataDir = os.path.join(tmpDir, 'dummy_dir')
        if not os.path.exists(tmpDataDir):
            os.mkdir(tmpDataDir)
        pathToDummySet = \
            '/adapt/nobackup/projects/ilab/data/NEPAC/nepac_datasets.tar.gz'
        if not os.path.exists(os.path.join(tmpDataDir, 'BOSSW.nc')):
            tar = tarfile.open(pathToDummySet)
            tar.extractall(path=tmpDataDir)
        validDateTime = datetime.datetime(2010, 1, 1)
        invalidDateTime = datetime.date.today()
        validLocation = ('-76.51005', '39.07851')

        # Test invalid mission.
        with self.assertRaisesRegex(RuntimeError, 'Invalid mission:'):

            BosswRetriever(
                'invalidMission', validDateTime, tmpDataDir, validLocation)

        rt = BosswRetriever('BO-SSW', invalidDateTime,
                            tmpDataDir, validLocation)
        self.assertTrue(rt._error)

        # Test valid everything.
        BosswRetriever('BO-SSW', validDateTime, tmpDataDir, validLocation)

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
        tmpDir = tempfile.gettempdir()
        tmpDataDir = os.path.join(tmpDir, 'dummy_dir')
        if not os.path.exists(tmpDataDir):
            tmpDataDir = os.mkdir(tmpDataDir)
        pathToDummySet = \
            '/adapt/nobackup/projects/ilab/data/NEPAC/nepac_datasets.tar.gz'
        if not os.path.exists(os.path.join(tmpDataDir, 'BOSSW.nc')):
            tar = tarfile.open(pathToDummySet)
            tar.extractall(path=tmpDataDir)
        validDateTime = datetime.datetime(year=2010,
                                          month=12,
                                          day=11,
                                          hour=13)
        invalidLonLocation = ('-200', '20')
        invalidLatLocation = ('-180', '180')
        validLocation = ('-76.51005', '39.07851')

        rt = BosswRetriever('BO-SSW',
                            validDateTime,
                            tmpDataDir,
                            invalidLonLocation)
        self.assertTrue(rt._error)

        rt = BosswRetriever('BO-SSW',
                            validDateTime,
                            tmpDataDir,
                            invalidLatLocation)
        self.assertTrue(rt._error)

        BosswRetriever('BO-SSW', validDateTime, tmpDataDir, validLocation)

    # -------------------------------------------------------------------------
    # testRun
    # -------------------------------------------------------------------------
    def testRun(self):
        tmpDir = tempfile.gettempdir()
        tmpDataDir = os.path.join(tmpDir, 'dummy_dir')
        if not os.path.exists(tmpDataDir):
            tmpDataDir = os.mkdir(tmpDataDir)
        pathToDummySet = \
            '/adapt/nobackup/projects/ilab/data/NEPAC/nepac_datasets.tar.gz'
        if not os.path.exists(os.path.join(tmpDataDir, 'BOSSW')):
            tar = tarfile.open(pathToDummySet)
            tar.extractall(path=tmpDataDir)
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
                                tmpDataDir,
                                validBosswLoc,
                                outputDirectory=tmp_directory)
        bosswR.run()
