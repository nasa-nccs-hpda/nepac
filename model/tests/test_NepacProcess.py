
import os
import unittest

from nepac.model.NepacProcess import NepacProcess

# -----------------------------------------------------------------------------
# class NepacProcessTestCase
#
# singularity shell -B /att /att/nobackup/iluser/containers/ilab-core-5.0.0.simg
# cd to the directory containing nepac
# export PYTHONPATH=`pwd`:`pwd`/core:`pwd`/nepac
# python -m unittest discover model/tests/
# python -m unittest nepac.model.tests.test_NepacProcess
# -----------------------------------------------------------------------------


class NepacProcessTestCase(unittest.TestCase):

    IN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           'nepacInput.csv')

    MISSION_DICT = {'MODIS-Aqua': ['iPAR']}

    # -------------------------------------------------------------------------
    # testInit
    # -------------------------------------------------------------------------
    def testInit(self):

        # Input file does not exist.
        with self.assertRaisesRegex(RuntimeError, '.*does not exist.*'):

            NepacProcess('bogusFile', None, None)

        # Valid input file, no mission dictionary.
        with self.assertRaisesRegex(ValueError, 'A mission-to'):

            NepacProcess(NepacProcessTestCase.IN_FILE, None, '.')

        # Valid input file and mission dictionary, invalid output directory.
        with self.assertRaisesRegex(ValueError, 'An output directory'):

            NepacProcess(NepacProcessTestCase.IN_FILE,
                         NepacProcessTestCase.MISSION_DICT,
                         None)

        # Valid everything.
        NepacProcess(NepacProcessTestCase.IN_FILE,
                     NepacProcessTestCase.MISSION_DICT,
                     '.')

    # -------------------------------------------------------------------------
    # testRun
    # -------------------------------------------------------------------------
    def testRun(self):

        np = NepacProcess(NepacProcessTestCase.IN_FILE,
                          NepacProcessTestCase.MISSION_DICT,
                          '.')

        np.run()
