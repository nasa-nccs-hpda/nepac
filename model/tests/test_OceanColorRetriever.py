import datetime
import unittest
import tempfile

from nepac.model.OceanColorRetriever import OceanColorRetriever


# -----------------------------------------------------------------------------
# class OceanColorRetrieverTestCase
#
# singularity shell -B /att
# /att/nobackup/iluser/containers/ilab-nepac-2.0.0.simg
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
        validLocation = ('-76.51005', '39.07851')

        # Test invalid mission.
        with self.assertRaisesRegex(RuntimeError, 'Invalid mission:'):

            OceanColorRetriever('invalidMission', validDateTime, validLocation)

        rt = OceanColorRetriever('MODIS-Aqua', invalidDateTime, validLocation)
        self.assertTrue(rt._error)
        # Test valid everything.
        OceanColorRetriever('MODIS-Aqua', validDateTime, validLocation)

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

        rt = OceanColorRetriever('VIIRS-SNPP',
                                 validDateTime,
                                 invalidLonLocation)
        self.assertTrue(rt._error)

        rt = OceanColorRetriever('VIIRS-JPSS1',
                                 validDateTime,
                                 invalidLatLocation)
        self.assertTrue(rt._error)

        OceanColorRetriever('VIIRS-SNPP', validDateTime, validLocation)

    # -------------------------------------------------------------------------
    # testRun
    # -------------------------------------------------------------------------
    def testRun(self):

        tmp_directory = tempfile.gettempdir()

        # Test invalid date time.
        invalidDt = datetime.datetime.today()
        invalidLoc = ('-77.1739', '38.6082')

        invalidModisaOCR = OceanColorRetriever(
                'MODIS-Aqua',
                invalidDt,
                invalidLoc,
                outputDirectory=tmp_directory)
        invalidModisaOCR.run()
        self.assertTrue(invalidModisaOCR._error)

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

        modisaOCR = OceanColorRetriever('MODIS-Aqua',
                                        validModisDt,
                                        validModisLoc,
                                        outputDirectory=tmp_directory)
        modisaOCR.run()

        modistOCR = OceanColorRetriever('MODIS-Terra',
                                        validModisDt,
                                        validModisLoc,
                                        outputDirectory=tmp_directory)
        modistOCR.run()

        validCzcsDt = datetime.datetime(year=1985,
                                        month=10,
                                        day=7,
                                        hour=15,
                                        minute=50)
        validCzcsLoc = ('-77.1739', '38.6082')
        czcsOCR = OceanColorRetriever('CZCS',
                                      validCzcsDt,
                                      validCzcsLoc,
                                      outputDirectory=tmp_directory)
        czcsOCR.run()

        gociDt = datetime.datetime(year=2011,
                                   month=4,
                                   day=1,
                                   hour=00,
                                   minute=16)
        validLocGOCI = ('131.2670', '39.5092')

        gociOCR = OceanColorRetriever('GOCI',
                                      gociDt,
                                      validLocGOCI,
                                      outputDirectory=tmp_directory)
        gociOCR.run()
