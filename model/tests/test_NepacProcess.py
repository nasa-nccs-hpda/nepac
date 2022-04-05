import os
import tarfile
import tempfile
import unittest

from nepac.model.NepacProcess import NepacProcess


# -----------------------------------------------------------------------------
# class NepacProcessTestCase
#
# singularity shell -B /att \
# /adapt/nobackup/people/iluser/containers/ilab-nepac-2.0.0.simg
# cd to the directory containing nepac
# export PYTHONPATH=`pwd`:`pwd`/core:`pwd`/nepac
# python -m unittest discover model/tests/
# python -m unittest nepac.model.tests.test_NepacProcess
# -----------------------------------------------------------------------------
class NepacProcessTestCase(unittest.TestCase):

    NO_DATA = -9999
    ERRORED_DATA = -9998

    IN_FILE1 = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'nepacInputOne.csv')

    IN_FILE2 = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'nepacInputTwo.csv')

    MISSION_DICT1 = {'MODIS-Aqua': ['Rrs_412'],
                     'MODIS-Terra': ['chlor_a', 'ipar', 'Kd_490',
                                     'Rrs_412', 'Rrs_443',
                                     'Rrs_547', 'Rrs_555'],
                     'VIIRS-JPSS1': ['Rrs_556', 'Rrs_667'],
                     'VIIRS-SNPP': ['Rrs_671']
                     }
    MISSION_DICT2 = {'GOCI': ['Rrs_443', 'Rrs_412'],
                     'MODIS-Terra': ['Rrs_443'],
                     'VIIRS-JPSS1': ['Rrs_445'],
                     'VIIRS-SNPP': ['Rrs_443'],
                     'OC-CCI': ['Rrs_443'],
                     'OI-SST': ['sst'],
                     'ETOPO1-BED': ['z'],
                     'BO-SSW': ['tau']}

    BAD_MISSION_DICT = {'MODIS-Aqua': ['Rrs_440']}

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
        # Input file does not exist.
        with self.assertRaisesRegex(RuntimeError, '.*does not exist.*'):

            NepacProcess('bogusFile',
                         None,
                         None,
                         tmpDataDir,
                         NepacProcessTestCase.NO_DATA,
                         NepacProcessTestCase.ERRORED_DATA)

        # Valid input file, no mission dictionary.
        with self.assertRaisesRegex(ValueError, 'A mission-to'):

            NepacProcess(NepacProcessTestCase.IN_FILE1,
                         None,
                         '.',
                         tmpDataDir,
                         NepacProcessTestCase.NO_DATA,
                         NepacProcessTestCase.ERRORED_DATA)

        # Valid input file and mission dictionary, invalid output directory.
        with self.assertRaisesRegex(ValueError, 'An output directory'):

            NepacProcess(NepacProcessTestCase.IN_FILE1,
                         NepacProcessTestCase.MISSION_DICT1,
                         None,
                         tmpDataDir,
                         NepacProcessTestCase.NO_DATA,
                         NepacProcessTestCase.ERRORED_DATA)

        with self.assertRaisesRegex(ValueError, 'Invalid data set'):

            NepacProcess(NepacProcessTestCase.IN_FILE1,
                         NepacProcessTestCase.BAD_MISSION_DICT,
                         '.',
                         tmpDataDir,
                         NepacProcessTestCase.NO_DATA,
                         NepacProcessTestCase.ERRORED_DATA)

        # Valid everything.
        NepacProcess(NepacProcessTestCase.IN_FILE1,
                     NepacProcessTestCase.MISSION_DICT1,
                     '.',
                     tmpDataDir,
                     NepacProcessTestCase.NO_DATA,
                     NepacProcessTestCase.ERRORED_DATA)

        NepacProcess(NepacProcessTestCase.IN_FILE2,
                     NepacProcessTestCase.MISSION_DICT2,
                     '.',
                     tmpDataDir,
                     NepacProcessTestCase.NO_DATA,
                     NepacProcessTestCase.ERRORED_DATA)

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
        if not os.path.exists(os.path.join(tmpDataDir, 'BOSSW.nc')):
            tar = tarfile.open(pathToDummySet)
            tar.extractall(path=tmpDataDir)
        # Valid everything
        np1 = NepacProcess(NepacProcessTestCase.IN_FILE1,
                           NepacProcessTestCase.MISSION_DICT1,
                           '.',
                           tmpDataDir,
                           NepacProcessTestCase.NO_DATA,
                           NepacProcessTestCase.ERRORED_DATA)
        np1.run()

        np2 = NepacProcess(NepacProcessTestCase.IN_FILE2,
                           NepacProcessTestCase.MISSION_DICT2,
                           '.',
                           tmpDataDir,
                           NepacProcessTestCase.NO_DATA,
                           NepacProcessTestCase.ERRORED_DATA)
        np2.run()
