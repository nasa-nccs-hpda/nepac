import datetime
import shutil
import unittest

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

        validDateTime = datetime.datetime(2004, 1, 1)
        invalidDateTime = datetime.datetime(2000, 1, 1)

        # Test invalid mission.
        with self.assertRaisesRegex(RuntimeError, 'Invalid mission:'):

            OceanColorRetriever('invalidMission', validDateTime)

        with self.assertRaisesRegex(RuntimeError, 'Invalid date:'):
            OceanColorRetriever('MODIS-Aqua', invalidDateTime)

        # Test valid everything.
        OceanColorRetriever('MODIS-Aqua', validDateTime)

    # -------------------------------------------------------------------------
    # testIsValidDataSet
    # -------------------------------------------------------------------------
    def testIsValidDataSet(self):

        validDateTime = datetime.datetime(2004, 1, 1)

        assert(OceanColorRetriever('MODIS-Aqua',
                                   validDateTime).isValidDataSet(
                                       'MODIS-Aqua',
                                       'invalid') is False)

        assert(OceanColorRetriever('MODIS-Aqua',
                                   validDateTime).isValidDataSet(
                                       'MODIS-Aqua',
                                       'iPAR') is True)

    # -------------------------------------------------------------------------
    # testRun
    # -------------------------------------------------------------------------
    def testRun(self):

        outputDirectory = 'tmpTestOutputDirectory'

        # Test invalid date time
        with self.assertRaisesRegex(RuntimeError, 'File not found:'):
            OceanColorRetriever('MODIS-Aqua', datetime.datetime(
                2004, 1, 1, 0, 5)).run(outputDirectory)

        # Test valid date time
        # We test multiple missions due to differing date ranges.
        OceanColorRetriever('MODIS-Aqua', datetime.datetime(
            2004, 1, 1, 2, 10)).run(outputDirectory)
        OceanColorRetriever('MODIS-Terra', datetime.datetime(
            2001, 1, 1, 0, 0)).run(outputDirectory)
        OceanColorRetriever('CZCS', datetime.datetime(
            1978, 10, 30, 12, 48, 34)).run(outputDirectory)
        OceanColorRetriever('GOCI', datetime.datetime(
            2011, 4, 1, 00, 16, 41)).run(outputDirectory)

        shutil.rmtree(outputDirectory, ignore_errors=True)
