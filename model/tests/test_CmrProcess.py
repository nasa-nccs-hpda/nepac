import datetime
import unittest

from nepac.model.CmrProcess import CmrProcess


# -----------------------------------------------------------------------------
# class CmrProcessTestCase
#
# singularity shell -B /att
# /att/nobackup/iluser/containers/ilab-nepac-2.0.0.simg
# cd to the directory containing nepac
# export PYTHONPATH=`pwd`:`pwd`/nepac
# python -m unittest discover model/tests/
# python -m unittest model.tests.test_CmrProcess.py
# -----------------------------------------------------------------------------
class CmrProcessTestCase(unittest.TestCase):

    mission = 'VIIRS-JPSS1'
    validDateTime = datetime.datetime(2018, 1, 1)
    invalidDateTime = datetime.datetime.today()
    validLocation = ('-76.51005', '39.07851')
    invalidLocationLon = ('181.0001', '39.07851')
    invalidLocationLat = ('-76.51005', '-91.00001')

    # -------------------------------------------------------------------------
    # testInit
    # -------------------------------------------------------------------------
    def testInit(self):

        with self.assertRaisesRegex(RuntimeError, 'Invalid longitude'):
            CmrProcess(CmrProcessTestCase.mission,
                       CmrProcessTestCase.validDateTime,
                       CmrProcessTestCase.invalidLocationLon)

        with self.assertRaisesRegex(RuntimeError, 'Invalid latitude'):
            CmrProcess(CmrProcessTestCase.mission,
                       CmrProcessTestCase.validDateTime,
                       CmrProcessTestCase.invalidLocationLat)

    # -------------------------------------------------------------------------
    # testInit
    # -------------------------------------------------------------------------
    def testRun(self):
        cmrRequestViirs = CmrProcess(self.mission,
                                     self.validDateTime,
                                     self.validLocation)
        cmrRequestViirs.run()

        cmrRequestModis = CmrProcess('MODIS-Terra',
                                     self.validDateTime,
                                     self.validLocation)
        cmrRequestModis.run()
