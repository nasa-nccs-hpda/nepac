import datetime
import os
import unittest
import tarfile
import tempfile

from nepac.model.PosstRetriever import PosstRetriever


# -----------------------------------------------------------------------------
# class PosstRetrieverTestCase
#
# singularity shell -B /att
# /adapt/nobackup/people/iluser/containers/ilab-nepac-2.0.0.simg
# cd to the directory containing nepac
# export PYTHONPATH=`pwd`:`pwd`/nepac
# python -m unittest discover model/tests/
# python -m unittest model.tests.test_PosstRetrieverTestCase
# -----------------------------------------------------------------------------
class PosstRetrieverTestCase(unittest.TestCase):

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
        if not os.path.exists(os.path.join(tmpDataDir, 'POSST.nc')):
            tar = tarfile.open(pathToDummySet)
            tar.extractall(path=tmpDataDir)
        validDateTime = datetime.datetime(2020, 1, 1)
        invalidDateTime = datetime.datetime(1970, 1, 1)
        validLocation = ('-76.51005', '39.07851')

        # Test invalid mission.
        with self.assertRaisesRegex(RuntimeError, 'Invalid mission:'):

            PosstRetriever('invalid', validDateTime, tmpDataDir, validLocation)

        rt = PosstRetriever('PO-SST', invalidDateTime,
                            tmpDataDir, validLocation)
        self.assertTrue(rt._error)

        # Test valid everything.
        PosstRetriever('PO-SST', validDateTime, tmpDataDir, validLocation)

    # -------------------------------------------------------------------------
    # testIsValidDataSet
    # -------------------------------------------------------------------------
    def testIsValidDataSet(self):

        self.assertFalse(PosstRetriever.isValidDataSet(
            'PO-SST',
            'invalid'))

        self.assertTrue(PosstRetriever.isValidDataSet(
            'PO-SST',
            'analysed_sst'))

    # -------------------------------------------------------------------------
    # testIsValidLocation
    # -------------------------------------------------------------------------
    def testIsValidLocation(self):
        tmpDir = tempfile.gettempdir()
        tmpDataDir = os.path.join(tmpDir, 'dummy_dir')
        if not os.path.exists(tmpDataDir):
            tmpDataDir = os.mkdir(tmpDataDir)
        pathToDummySet =  \
            '/adapt/nobackup/projects/ilab/data/NEPAC/nepac_datasets.tar.gz'
        if not os.path.exists(os.path.join(tmpDataDir, 'POSST.nc')):
            tar = tarfile.open(pathToDummySet)
            tar.extractall(path=tmpDataDir)
        validDateTime = datetime.datetime(year=2018,
                                          month=12,
                                          day=11,
                                          hour=13)
        invalidLonLocation = ('-200', '20')
        invalidLatLocation = ('-180', '180')
        validLocation = ('-76.51005', '39.07851')

        rt = PosstRetriever('PO-SST',
                            validDateTime,
                            tmpDataDir,
                            invalidLonLocation)
        self.assertTrue(rt._error)

        rt = PosstRetriever('PO-SST',
                            validDateTime,
                            tmpDataDir,
                            invalidLatLocation)
        self.assertTrue(rt._error)
        PosstRetriever('PO-SST', validDateTime, tmpDataDir, validLocation)

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
        if not os.path.exists(os.path.join(tmpDataDir, 'POSST.nc')):
            tar = tarfile.open(pathToDummySet)
            tar.extractall(path=tmpDataDir)
        tmp_directory = tempfile.gettempdir()
        # ---
        # Test valid date time.
        # We test multiple missions due to differing date ranges.
        # ---
        validModisDt = datetime.datetime(year=2018,
                                         month=10,
                                         day=31,
                                         hour=18,
                                         minute=45)
        validModisLoc = ('13.30553', '36.42652')

        posstR = PosstRetriever('PO-SST',
                                validModisDt,
                                tmpDataDir,
                                validModisLoc,
                                outputDirectory=tmp_directory)
        posstR.run()
