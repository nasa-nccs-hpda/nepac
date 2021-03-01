import os
import unittest

from nepac.model.NepacProcess import NepacProcess


# -----------------------------------------------------------------------------
# class NepacProcessTestCase
#
# singularity shell -B /att \
# /att/nobackup/iluser/containers/ilab-core-5.0.0.simg
# cd to the directory containing nepac
# export PYTHONPATH=`pwd`:`pwd`/core:`pwd`/nepac
# python -m unittest discover model/tests/
# python -m unittest nepac.model.tests.test_NepacProcess
# -----------------------------------------------------------------------------
class NepacProcessTestCase(unittest.TestCase):

    IN_FILE1 = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'nepacInputOne.csv')

    IN_FILE2 = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'nepacInputTwo.csv')

    MISSION_DICT1 = {'MODIS-Aqua': ['Rrs_443', 'Rrs_412'],
                     'MODIS-Terra': ['Rrs_443'],
                     'VIIRS-JPSS1': ['Rrs_445'],
                     'VIIRS-SNPP': ['Rrs_443']}
    MISSION_DICT2 = {'GOCI': ['Rrs_443', 'Rrs_412'],
                     'MODIS-Terra': ['Rrs_443'],
                     'VIIRS-JPSS1': ['Rrs_445'],
                     'VIIRS-SNPP': ['Rrs_443']}

    BAD_MISSION_DICT = {'MODIS-Aqua': ['Rrs_440']}

    # -------------------------------------------------------------------------
    # testInit
    # -------------------------------------------------------------------------
    def testInit(self):

        # Input file does not exist.
        with self.assertRaisesRegex(RuntimeError, '.*does not exist.*'):

            NepacProcess('bogusFile', None, None)

        # Valid input file, no mission dictionary.
        with self.assertRaisesRegex(ValueError, 'A mission-to'):

            NepacProcess(NepacProcessTestCase.IN_FILE1, None, '.')

        # Valid input file and mission dictionary, invalid output directory.
        with self.assertRaisesRegex(ValueError, 'An output directory'):

            NepacProcess(NepacProcessTestCase.IN_FILE1,
                         NepacProcessTestCase.MISSION_DICT1,
                         None)

        with self.assertRaisesRegex(ValueError, 'Invalid data set'):

            NepacProcess(NepacProcessTestCase.IN_FILE1,
                         NepacProcessTestCase.BAD_MISSION_DICT,
                         '.')

        # Valid everything.
        NepacProcess(NepacProcessTestCase.IN_FILE1,
                     NepacProcessTestCase.MISSION_DICT1,
                     '.')
        NepacProcess(NepacProcessTestCase.IN_FILE2,
                     NepacProcessTestCase.MISSION_DICT2,
                     '.')

    # -------------------------------------------------------------------------
    # testRun
    # -------------------------------------------------------------------------
    def testRun(self):

        # Valid everything
        np1 = NepacProcess(NepacProcessTestCase.IN_FILE1,
                           NepacProcessTestCase.MISSION_DICT1,
                           '.')
        np1.run()

        np2 = NepacProcess(NepacProcessTestCase.IN_FILE2,
                           NepacProcessTestCase.MISSION_DICT2,
                           '.')
        np2.run()
