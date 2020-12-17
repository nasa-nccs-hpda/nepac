import datetime
import shutil
import time
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

        self.assertFalse(OceanColorRetriever.isValidDataSet(
            'MODIS-Aqua',
            'invalid'))

        self.assertTrue(OceanColorRetriever.isValidDataSet(
            'MODIS-Aqua',
            'ipar'))

    # -------------------------------------------------------------------------
    # testRun
    # -------------------------------------------------------------------------
    def testRun(self):

        tmp_directory = 'tmpTestOutputDirectory' + str(time.time())

        # Test invalid date time, which results in a 400 level error.
        with self.assertRaisesRegex(RuntimeError, 'Client or server error:'):
            ocr_0 = OceanColorRetriever('MODIS-Aqua',
                                        datetime.datetime(2004, 1, 1, 0, 5),
                                        tmp_directory)
            ocr_0.run()

        # --------------------------------------------------------------------
        # Test valid date time.
        # We test multiple missions due to differing date ranges.
        # --------------------------------------------------------------------
        ocr_1 = OceanColorRetriever('MODIS-Aqua',
                                    datetime.datetime(2004, 1, 1, 2, 10),
                                    tmp_directory)
        ocr_1.run()

        ocr_2 = OceanColorRetriever('MODIS-Terra',
                                    datetime.datetime(2001, 1, 1, 0, 0),
                                    tmp_directory)
        ocr_2.run()

        ocr_3 = OceanColorRetriever('CZCS',
                                    datetime.datetime(
                                        1978, 10, 30, 12, 48, 34),
                                    tmp_directory)
        ocr_3.run()

        ocr_4 = OceanColorRetriever('GOCI',
                                    datetime.datetime(2011, 4, 1, 00, 16, 41),
                                    tmp_directory)
        ocr_4.run()

        shutil.rmtree(tmp_directory, ignore_errors=True)
