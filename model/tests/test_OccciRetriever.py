import datetime
import os
import tarfile
import tempfile
import unittest

from nepac.model.OccciRetriever import OccciRetriever


# -----------------------------------------------------------------------------
# class OccciRetrieverTestCase
#
# singularity shell -B /att
# /adapt/nobackup/people/iluser/containers/ilab-nepac-2.0.0.simg
# cd to the directory containing nepac
# export PYTHONPATH=`pwd`:`pwd`/nepac
# python -m unittest discover model/tests/
# python -m unittest model.tests.test_OccciRetriever
# -----------------------------------------------------------------------------
class OccciColorRetrieverTestCase(unittest.TestCase):

    # -------------------------------------------------------------------------
    # testInit
    # -------------------------------------------------------------------------
    def testInit(self):
        tmpDir = tempfile.gettempdir()
        tmpDataDir = os.path.join(tmpDir, 'dummy_dir')
        if not os.path.exists(tmpDataDir):
            os.mkdir(tmpDataDir)
        pathToDummySet = \
            '/adapt/nobackup/people/cssprad1/nepac_datasets.tar.gz'
        if not os.path.exists(os.path.join(tmpDataDir, 'OCCCI.nc')):
            tar = tarfile.open(pathToDummySet)
            tar.extractall(path=tmpDataDir)
        validDateTime = datetime.datetime(2004, 1, 1)
        invalidDateTime = datetime.datetime(1970, 1, 1)
        validLocation = ('-76.51005', '39.07851')

        # Test invalid mission.
        with self.assertRaisesRegex(RuntimeError, 'Invalid mission:'):

            OccciRetriever('invalidMission', validDateTime, tmpDataDir,
                           validLocation)

        rt = OccciRetriever('OC-CCI', invalidDateTime, tmpDataDir,
                            validLocation)
        self.assertTrue(rt._error)
        # Test valid everything.
        OccciRetriever('OC-CCI', validDateTime, tmpDataDir, validLocation)

    # -------------------------------------------------------------------------
    # testIsValidDataSet
    # -------------------------------------------------------------------------
    def testIsValidDataSet(self):

        self.assertFalse(OccciRetriever.isValidDataSet(
            'OC-CCI',
            'invalid'))

        self.assertTrue(OccciRetriever.isValidDataSet(
            'OC-CCI',
            'Rrs_412'))

    # -------------------------------------------------------------------------
    # testIsValidLocation
    # -------------------------------------------------------------------------
    def testIsValidLocation(self):
        tmpDir = tempfile.gettempdir()
        tmpDataDir = os.path.join(tmpDir, 'dummy_dir')
        if not os.path.exists(tmpDataDir):
            tmpDataDir = os.mkdir(tmpDataDir)
        pathToDummySet = \
            '/adapt/nobackup/people/cssprad1/nepac_datasets.tar.gz'
        if not os.path.exists(os.path.join(tmpDataDir, 'OCCCI.nc')):
            tar = tarfile.open(pathToDummySet)
            tar.extractall(path=tmpDataDir)
        validDateTime = datetime.datetime(year=2018,
                                          month=12,
                                          day=11,
                                          hour=13)
        invalidLonLocation = ('-200', '20')
        invalidLatLocation = ('-180', '180')
        validLocation = ('-76.51005', '39.07851')

        rt = OccciRetriever('OC-CCI',
                            validDateTime,
                            tmpDataDir,
                            invalidLonLocation)
        self.assertTrue(rt._error)

        rt = OccciRetriever('OC-CCI',
                            validDateTime,
                            tmpDataDir,
                            invalidLatLocation)
        self.assertTrue(rt._error)

        OccciRetriever('OC-CCI', validDateTime, tmpDataDir, validLocation)

    # -------------------------------------------------------------------------
    # testRun
    # -------------------------------------------------------------------------
    def testRun(self):
        tmpDir = tempfile.gettempdir()
        tmpDataDir = os.path.join(tmpDir, 'dummy_dir')
        if not os.path.exists(tmpDataDir):
            tmpDataDir = os.mkdir(tmpDataDir)
        pathToDummySet = \
            '/adapt/nobackup/people/cssprad1/nepac_datasets.tar.gz'
        if not os.path.exists(os.path.join(tmpDataDir, 'OCCCI.nc')):
            tar = tarfile.open(pathToDummySet)
            tar.extractall(path=tmpDataDir)
        tmp_directory = tempfile.gettempdir()

        # ---
        # Test valid date time.
        # We test multiple missions due to differing date ranges.
        # ---
        validOccciDt = datetime.datetime(year=2018,
                                         month=10,
                                         day=31,
                                         hour=18,
                                         minute=45)
        validOccciLoc = ('13.30553', '36.42652')

        occciOCR = OccciRetriever('OC-CCI',
                                  validOccciDt,
                                  tmpDataDir,
                                  validOccciLoc,
                                  outputDirectory=tmp_directory)
        occciOCR.run()
