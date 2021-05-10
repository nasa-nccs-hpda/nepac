import datetime
import tempfile
import unittest

from nepac.model.OcSWFHICOCTRetriever import OcSWFHICOCTRetriever


# -----------------------------------------------------------------------------
# class OcSWFHICORetriever
#
# singularity shell -B /att
# /att/nobackup/iluser/containers/ilab-nepac-2.0.0.simg
# cd to the directory containing nepac
# export PYTHONPATH=`pwd`:`pwd`/nepac
# python -m unittest discover model/tests/
# python -m unittest model.tests.test_OcSWFHICRetriever
# -----------------------------------------------------------------------------
class OsSWFHICOCTRetriever(unittest.TestCase):

    # -------------------------------------------------------------------------
    # testInit
    # -------------------------------------------------------------------------
    def testInit(self):

        validDateTime = datetime.datetime(2002, 1, 1)
        invalidDateTime = datetime.datetime(2012, 1, 1)
        validLocation = ('-76.51005', '39.07851')

        # Test invalid mission.
        with self.assertRaisesRegex(RuntimeError, 'Invalid mission:'):

            OcSWFHICOCTRetriever('invalidMission', validDateTime,
                                 validLocation)

        # Test invalid datetime
        rt = OcSWFHICOCTRetriever('SeaWiFS', invalidDateTime, validLocation)
        self.assertTrue(rt._error)

        # Test valid everything.
        OcSWFHICOCTRetriever('SeaWiFS', validDateTime, validLocation)

    # -------------------------------------------------------------------------
    # testIsValidDataSet
    # -------------------------------------------------------------------------
    def testIsValidDataSet(self):

        self.assertFalse(OcSWFHICOCTRetriever.isValidDataSet(
            'SeaWiFS',
            'invalid'))

        self.assertTrue(OcSWFHICOCTRetriever.isValidDataSet(
            'SeaWiFS',
            'Rrs_412'))

    # -------------------------------------------------------------------------
    # testIsValidLocation
    # -------------------------------------------------------------------------
    def testIsValidLocation(self):

        validDateTime = datetime.datetime(year=2009,
                                          month=12,
                                          day=11,
                                          hour=13)
        invalidLonLocation = ('-200', '20')
        invalidLatLocation = ('-180', '180')
        validLocation = ('-76.51005', '39.07851')

        rt = OcSWFHICOCTRetriever('SeaWiFS',
                                  validDateTime,
                                  invalidLonLocation)
        self.assertTrue(rt._error)

        rt = OcSWFHICOCTRetriever('SeaWiFS',
                                  validDateTime,
                                  invalidLatLocation)
        self.assertTrue(rt._error)

        OcSWFHICOCTRetriever('SeaWiFS', validDateTime, validLocation)

    # -------------------------------------------------------------------------
    # testRun
    # -------------------------------------------------------------------------
    def testRun(self):

        tmp_directory = tempfile.gettempdir()

        # ---
        # Test valid date time.
        # We test multiple missions due to differing date ranges.
        # ---
        validSWFDt = datetime.datetime(year=2001,
                                       month=10,
                                       day=31,
                                       hour=18,
                                       minute=45)

        validLoc = ('13.30553', '36.42652')

        swfOCR = OcSWFHICOCTRetriever('SeaWiFS',
                                      validSWFDt,
                                      validLoc,
                                      outputDirectory=tmp_directory)
        swfOCR.run()

        validOCTSDt = datetime.datetime(year=1997,
                                        month=5,
                                        day=28,
                                        hour=18,
                                        minute=45)

        octsOCR = OcSWFHICOCTRetriever('OCTS',
                                       validOCTSDt,
                                       validLoc,
                                       outputDirectory=tmp_directory)
        octsOCR.run()

        validHICODt = datetime.datetime(year=2010,
                                        month=10,
                                        day=7,
                                        hour=15,
                                        minute=50)

        hicoOCR = OcSWFHICOCTRetriever('HICO',
                                       validHICODt,
                                       validLoc,
                                       outputDirectory=tmp_directory)
        hicoOCR.run()