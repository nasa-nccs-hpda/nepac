import datetime
import unittest
import tempfile

from nepac.model.EtopoRetriever import EtopoRetriever


# -----------------------------------------------------------------------------
# class EtopoRetrieverTestCase
#
# singularity shell -B /att
# /att/nobackup/iluser/containers/ilab-nepac-2.0.0.simg
# cd to the directory containing nepac
# export PYTHONPATH=`pwd`:`pwd`/nepac
# python -m unittest discover model/tests/
# python -m unittest model.tests.test_EtopoRetriever
# -----------------------------------------------------------------------------
class EtopoRetrieverTestCase(unittest.TestCase):

    # -------------------------------------------------------------------------
    # testInit
    # -------------------------------------------------------------------------
    def testInit(self):

        validDateTime = datetime.datetime(2004, 1, 1)
        invalidDateTime = datetime.datetime(1970, 1, 1)
        validLocation = ('-76.51005', '39.07851')

        # Test invalid mission.
        with self.assertRaisesRegex(RuntimeError, 'Invalid mission:'):

            EtopoRetriever('invalidMission', validDateTime, validLocation)

        rt = EtopoRetriever('ETOPO1-BED', invalidDateTime, validLocation)
        self.assertTrue(rt._error)
        # Test valid everything.
        EtopoRetriever('ETOPO1-ICE', validDateTime, validLocation)

    # -------------------------------------------------------------------------
    # testIsValidDataSet
    # -------------------------------------------------------------------------
    def testIsValidDataSet(self):

        self.assertFalse(EtopoRetriever.isValidDataSet(
            'ETOPO1-BED',
            'invalid'))

        self.assertTrue(EtopoRetriever.isValidDataSet(
            'ETOPO1-BED',
            'z'))

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

        rt = EtopoRetriever('ETOPO1-ICE',
                            validDateTime,
                            invalidLonLocation)
        self.assertTrue(rt._error)

        rt = EtopoRetriever('ETOPO1-BED',
                            validDateTime,
                            invalidLatLocation)
        self.assertTrue(rt._error)

        EtopoRetriever('ETOPO1-BED', validDateTime, validLocation)

    # -------------------------------------------------------------------------
    # testRun
    # -------------------------------------------------------------------------
    def testRun(self):

        tmp_directory = tempfile.gettempdir()

        # ---
        # Test valid date time.
        # We test multiple missions due to differing date ranges.
        # ---
        validEtopoDt = datetime.datetime(year=2018,
                                         month=10,
                                         day=31,
                                         hour=18,
                                         minute=45)
        validEtopoLoc = ('13.30553', '36.42652')

        etopoBedR = EtopoRetriever('ETOPO1-BED',
                                   validEtopoDt,
                                   validEtopoLoc,
                                   outputDirectory=tmp_directory)
        etopoBedR.run()

        etopoIceR = EtopoRetriever('ETOPO1-ICE',
                                   validEtopoDt,
                                   validEtopoLoc,
                                   outputDirectory=tmp_directory)
        etopoIceR.run()
