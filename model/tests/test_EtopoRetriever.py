import datetime
import os
import tarfile
import tempfile
import unittest

from nepac.model.EtopoRetriever import EtopoRetriever


# -----------------------------------------------------------------------------
# class EtopoRetrieverTestCase
#
# singularity shell -B /att
# /adapt/nobackup/people/iluser/containers/ilab-nepac-2.0.0.simg
# cd to the directory containing nepac
# export PYTHONPATH=`pwd`:`pwd`/nepac
# python -m unittest discover model/tests/
# python -m unittest model.tests.test_EtopoRetriever
# -----------------------------------------------------------------------------
class EtopoRetrieverTestCase(unittest.TestCase):

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
        if not os.path.exists(os.path.join(tmpDataDir,
                                           'ETOPO1_Bed_g_gmt4.grd')):
            tar = tarfile.open(pathToDummySet)
            tar.extractall(path=tmpDataDir)
        validDateTime = datetime.datetime(2004, 1, 1)
        invalidDateTime = datetime.datetime(1970, 1, 1)
        validLocation = ('-76.51005', '39.07851')

        # Test invalid mission.
        with self.assertRaisesRegex(RuntimeError, 'Invalid mission:'):

            EtopoRetriever('invalidMission', validDateTime,
                           tmpDataDir, validLocation)

        rt = EtopoRetriever('ETOPO1-BED', invalidDateTime,
                            tmpDataDir, validLocation)
        self.assertTrue(rt._error)
        # Test valid everything.
        EtopoRetriever('ETOPO1-ICE', validDateTime, tmpDataDir, validLocation)

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
        tmpDir = tempfile.gettempdir()
        tmpDataDir = os.path.join(tmpDir, 'dummy_dir')
        if not os.path.exists(tmpDataDir):
            tmpDataDir = os.mkdir(tmpDataDir)
        pathToDummySet = \
            '/adapt/nobackup/people/cssprad1/nepac_datasets.tar.gz'
        if not os.path.exists(os.path.join(tmpDataDir,
                                           'ETOPO1_Bed_g_gmt4.grd')):
            tar = tarfile.open(pathToDummySet)
            tar.extractall(path=tmpDataDir)
        validDateTime = datetime.datetime(year=2018,
                                          month=12,
                                          day=11,
                                          hour=13)
        invalidLonLocation = ('-200', '20')
        invalidLatLocation = ('-180', '180')
        validLocation = ('-76.51005', '39.07851')

        rt = EtopoRetriever('ETOPO1-ICE',
                            validDateTime,
                            tmpDataDir,
                            invalidLonLocation)
        self.assertTrue(rt._error)

        rt = EtopoRetriever('ETOPO1-BED',
                            validDateTime,
                            tmpDataDir,
                            invalidLatLocation)
        self.assertTrue(rt._error)

        EtopoRetriever('ETOPO1-BED', validDateTime, tmpDataDir, validLocation)

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
        if not os.path.exists(os.path.join(tmpDataDir,
                                           'ETOPO1_Bed_g_gmt4.grd')):
            tar = tarfile.open(pathToDummySet)
            tar.extractall(path=tmpDataDir)
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
                                   tmpDataDir,
                                   validEtopoLoc,
                                   outputDirectory=tmp_directory)
        etopoBedR.run()

        etopoIceR = EtopoRetriever('ETOPO1-ICE',
                                   validEtopoDt,
                                   tmpDataDir,
                                   validEtopoLoc,
                                   outputDirectory=tmp_directory)
        etopoIceR.run()
