import datetime
import unittest
import tempfile

from nepac.model.OisstRetriever import OisstRetriever


# -----------------------------------------------------------------------------
# class OisstRetrieverTestCase
#
# singularity shell -B /att
# /att/nobackup/iluser/containers/ilab-nepac-2.0.0.simg
# cd to the directory containing nepac
# export PYTHONPATH=`pwd`:`pwd`/nepac
# python -m unittest discover model/tests/
# python -m unittest model.tests.test_OisstRetrieverTestCase
# -----------------------------------------------------------------------------
class OisstRetrieverTestCase(unittest.TestCase):

    # -------------------------------------------------------------------------
    # testInit
    # -------------------------------------------------------------------------
    def testInit(self):

        validDateTime = datetime.datetime(2020, 1, 1)
        invalidDateTime = datetime.datetime(1970, 1, 1)
        validLocation = ('-76.51005', '39.07851')

        # Test invalid mission.
        with self.assertRaisesRegex(RuntimeError, 'Invalid mission:'):

            OisstRetriever('invalid', validDateTime, validLocation)

        rt = OisstRetriever('OI-SST', invalidDateTime, validLocation)
        self.assertTrue(rt._error)
        # Test valid everything.
        OisstRetriever('OI-SST', validDateTime, validLocation)

    # -------------------------------------------------------------------------
    # testIsValidDataSet
    # -------------------------------------------------------------------------
    def testIsValidDataSet(self):

        self.assertFalse(OisstRetriever.isValidDataSet(
            'OI-SST',
            'invalid'))

        self.assertTrue(OisstRetriever.isValidDataSet(
            'OI-SST',
            'sst'))

    # -------------------------------------------------------------------------
    # testIsValidLocation
    # -------------------------------------------------------------------------
    def testIsValidLocation(self):

        validDateTime = datetime.datetime(year=2018,
                                          month=12,
                                          day=11,
                                          hour=13)
        invalidLonLocation = ('-200', '20')
        invalidLatLocation = ('-180', '180')
        validLocation = ('-76.51005', '39.07851')

        rt = OisstRetriever('OI-SST',
                            validDateTime,
                            invalidLonLocation)
        self.assertTrue(rt._error)

        rt = OisstRetriever('OI-SST',
                            validDateTime,
                            invalidLatLocation)
        self.assertTrue(rt._error)

        OisstRetriever('OI-SST', validDateTime, validLocation)

    # -------------------------------------------------------------------------
    # testRun
    # -------------------------------------------------------------------------
    def testRun(self):

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

        oisstR = OisstRetriever('OI-SST',
                                validModisDt,
                                validModisLoc,
                                outputDirectory=tmp_directory)
        oisstR.run()
