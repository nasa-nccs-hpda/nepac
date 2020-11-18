
import unittest

import datetime

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

        # Test invalid date time
        with self.assertRaisesRegex(RuntimeError, 'File not found:'):
            OceanColorRetriever('MODIS-Aqua', 'iPAR', invalidDateTime).run()

        # Test valid date time
        OceanColorRetriever('MODIS-Aqua', 'iPAR', validDateTime).run()
        OceanColorRetriever('MODIS-Terra', 'iPAR', datetime.datetime(
            2001, 1, 1, 0, 0)).run()
        OceanColorRetriever('CZCS', 'Kd_490', datetime.datetime(
            1978, 10, 30, 12, 48, 34)).run()
        OceanColorRetriever('GOCI', 'Kd_490', datetime.datetime(
            2011, 4, 1, 00, 16, 41)).run()
