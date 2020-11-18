
import unittest

import datetime
import os

from nepac.model.OceanColorRetriever import OceanColorRetriever

# -----------------------------------------------------------------------------
# class OceanColorRetrieverTestCase
#
# singularity shell -B /att
# /att/nobackup/iluser/containers/ilab-core-5.0.0.simg
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

        validDateTime = datetime.datetime(2004, 1, 1, 0, 0, 0)
        invalidDateTime = datetime.datetime(2000, 1, 1, 0, 0, 0)

        # Test invalid mission.
        with self.assertRaisesRegex(RuntimeError, 'Invalid mission:'):

            OceanColorRetriever('invalidMission', 'iPAR', validDateTime)

        # Test valid mission and invalid data set.
        with self.assertRaisesRegex(RuntimeError, 'Invalid data set:'):

            OceanColorRetriever('MODIS-Aqua', 'someDataSet', validDateTime)

        with self.assertRaisesRegex(RuntimeError, 'Invalid date:'):
            OceanColorRetriever('MODIS-Aqua', 'iPAR', invalidDateTime)

        # Test valid everything.
        OceanColorRetriever('MODIS-Aqua', 'iPAR', validDateTime)

    # -------------------------------------------------------------------------
    # testRun
    # -------------------------------------------------------------------------
    def testRun(self):

        validDateTime = datetime.datetime(2004, 1, 1, 2, 10)
        invalidDateTime = datetime.datetime(2004, 1, 1, 0, 5)

        # File name of the file we're downloading in the
        # valid test case
        fileName = 'A2004001021000.L2_LAC_OC.nc'

        # Make sure for testing purposes we can download
        # without duplicating if doing repeated tests
        if os.path.exists(fileName):
            os.system("rm " + fileName)

        # Test invalid date time
        with self.assertRaisesRegex(RuntimeError, 'File not found:'):
            OceanColorRetriever('MODIS-Aqua', 'iPAR', invalidDateTime).run()

        # Test valid date time
        OceanColorRetriever('MODIS-Aqua', 'iPAR', validDateTime).run()
