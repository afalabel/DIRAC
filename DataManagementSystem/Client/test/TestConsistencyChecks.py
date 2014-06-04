import unittest
from mock import Mock, patch
from DIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks
import datetime



class UtilitiesTestCase( unittest.TestCase ):
  """ Base class for the Consistency Checks test cases
  """
  def setUp( self ):
    self.tcMock = Mock()
    
    self.tcMock.getTransformation.return_value = {'OK': True,
                                                 'Value': {'AgentType': 'Automatic',
                                                           'AuthorDN': 'AuthorDN',
                                                           'AuthorGroup': 'lhcb_prmgr',
                                                           'Body': 'Body\n',
                                                           'CreationDate': datetime.datetime(2014, 5, 5, 8, 59, 36),
                                                           'Description': 'prodDescription',
                                                           'EventsPerTask': 0L,
                                                           'FileMask': '',
                                                           'GroupSize': 1L,
                                                           'InheritedFrom': 0L,
                                                           'LastUpdate': datetime.datetime(2014, 5, 10, 6, 45, 3),
                                                           'LongDescription': 'prodDescription',
                                                           'MaxNumberOfTasks': 0L,
                                                           'Plugin': 'ByRunWithFlush',
                                                           'Status': 'Idle',
                                                           'TransformationFamily': '21239',
                                                           'TransformationGroup': 'Real Data/Reco14/Stripping20/WG-CharmConfig-Swimming-2012-v14r11',
                                                           'TransformationID': 36297L,
                                                           'TransformationName': 'Request_21239_DataSwimming_Real DataReco14Stripping20WG-CharmConfig-Swimming-2012-v14r11p2-KSKK-LL-D0-REJ_EventType_90000000_CHARMTOBESWUM_1.xml',
                                                           'Type': 'DataSwimming'},
                                                 'rpcStub': (('Transformation/TransformationManager',
                                                              {'delegatedDN': 'delegatedDN',
                                                               'delegatedGroup': 'lhcb_prmgr',
                                                               'keepAliveLapse': 150,
                                                               'skipCACheck': False,
                                                               'timeout': 1800}),
                                                             'getTransformation',
                                                             (36297, False))}


    self.tcMock.getTransformationFiles.return_value = {'OK' : True,
                                                       'Value':
                                                       [{'ErrorCount': 1L,
                                                        'FileID': 78804554L,
                                                        'InsertedTime': datetime.datetime(2014, 5, 5, 9, 2, 37),
                                                        'LFN': '/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw',
                                                        'LastUpdate': datetime.datetime(2014, 5, 5, 11, 47, 42),
                                                        'RunNumber': 133732L,
                                                        'Status': 'Processed',
                                                        'TargetSE': 'Unknown',
                                                        'TaskID': 1058L,
                                                        'TransformationID': 36297L,
                                                        'UsedSE': 'IN2P3-BUFFER'},
                                                       {'ErrorCount': 1L,
                                                        'FileID': 78804555L,
                                                        'InsertedTime': datetime.datetime(2014, 5, 5, 9, 2, 37),
                                                        'LFN': '/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw',
                                                        'LastUpdate': datetime.datetime(2014, 5, 5, 11, 3, 50),
                                                        'RunNumber': 133732L,
                                                        'Status': 'Processed',
                                                        'TargetSE': 'Unknown',
                                                        'TaskID': 908L,
                                                        'TransformationID': 36297L,
                                                        'UsedSE': 'IN2P3-BUFFER'}]
                                                       }

    self.dmMock = Mock()
    self.dmMock.getReplicas.return_value = {'OK': True,
                                            'Value': {'Failed': {},
                                                      'Successful': {'/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw': {'CERN-RAW': 'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw',
                                                                                                                                                'CNAF-RAW': 'srm://storm-fe-lhcb.cr.cnaf.infn.it/tape/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw'},
                                                                     '/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw': {'CERN-RAW': 'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw',
                                                                                                                                                'CNAF-RAW': 'srm://storm-fe-lhcb.cr.cnaf.infn.it/tape/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw'}
                                                                     }
                                                      }
                                            }

    
    self.dmMock.getCatalogFileMetadata.return_value={'OK': True,
                                                     'Value': {'Failed': {},
                                                               'Successful': {'/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw': 
                                                                                                    {'CheckSumType': 'AD',
                                                                                                     'Checksum': '379aa433',
                                                                                                     'CreationDate': datetime.datetime(2012, 11, 18, 12, 58, 13),
                                                                                                     'GUID': '1d22c7c8-3175-11e2-866d-00259011312c',
                                                                                                     'Mode': 436,
                                                                                                     'ModificationDate': datetime.datetime(2012, 11, 18, 12, 58, 13),
                                                                                                     'NumberOfLinks': 1,
                                                                                                     'Size': 3145732036,
                                                                                                     'Status': '-'},
                                                                              
                                                                              '/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw': 
                                                                                                    {'CheckSumType': 'AD',
                                                                                                     'Checksum': '91324a09',
                                                                                                     'CreationDate': datetime.datetime(2012, 11, 18, 12, 58, 24),
                                                                                                     'GUID': '3bfbb8da-3175-11e2-866e-00259011312c',
                                                                                                     'Mode': 436,
                                                                                                     'ModificationDate': datetime.datetime(2012, 11, 18, 12, 58, 24),
                                                                                                     'NumberOfLinks': 1,
                                                                                                     'Size': 3145788944,
                                                                                                     'Status': '-'}}}}

    
# >>> import pprint
# >>> pprint.pprint(res)
# {'OK': True,
#  'Value': {'Failed': {},
#            'Successful': {'/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/115464/115464_0000000031.raw': {'CERN-RAW': 'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/115464/115464_0000000031.raw',
#                                                                                                      'SARA-RAW': 'srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/115464/115464_0000000031.raw'}}}}
# >>> res = rm.getReplicas(['/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0001/00020350_00010578_1.dimuon.dst'])
# >>> res
# {'OK': True, 'Value': {'Successful': {'/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0001/00020350_00010578_1.dimuon.dst': {'CNAF-DST': 'srm://storm-fe-lhcb.cr.cnaf.infn.it/disk/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0001/00020350_00010578_1.dimuon.dst', 'CERN-DST-EOS': 'srm://srm-eoslhcb.cern.ch/eos/lhcb/grid/prod/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0001/00020350_00010578_1.dimuon.dst', 'IN2P3-DST': 'srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0001/00020350_00010578_1.dimuon.dst', 'GRIDKA_M-DST': 'srm://lhcbsrm-kit.gridka.de/pnfs/gridka.de/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0001/00020350_00010578_1.dimuon.dst', 'CERN-ARCHIVE': 'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/archive/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0001/00020350_00010578_1.dimuon.dst'}}, 'Failed': {}}}
# >>> pprint.pprint(res)
# {'OK': True,
#  'Value': {'Failed': {},
#            'Successful': {'/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0001/00020350_00010578_1.dimuon.dst': {'CERN-ARCHIVE': 'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/archive/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0001/00020350_00010578_1.dimuon.dst',
#                                                                                                              'CERN-DST-EOS': 'srm://srm-eoslhcb.cern.ch/eos/lhcb/grid/prod/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0001/00020350_00010578_1.dimuon.dst',
#                                                                                                              'CNAF-DST': 'srm://storm-fe-lhcb.cr.cnaf.infn.it/disk/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0001/00020350_00010578_1.dimuon.dst',
#                                                                                                              'GRIDKA_M-DST': 'srm://lhcbsrm-kit.gridka.de/pnfs/gridka.de/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0001/00020350_00010578_1.dimuon.dst',
#                                                                                                              'IN2P3-DST': 'srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0001/00020350_00010578_1.dimuon.dst'}}}}


  def test_checkFC2SE_givenProdID(self):
    print "\nTest check FC2SE given a Production ID"
    self.cc = ConsistencyChecks( transClient = self.tcMock, dm = self.dmMock )
    self.cc.prod = 36297
    self.cc.checkFC2SE()
    
  def test_checkFC2SE_givenLFNlist(self):
    print "\nTest check FC2SE given an LFN list"
    self.cc = ConsistencyChecks( transClient = self.tcMock, dm = self.dmMock )
    self.cc.lfns = ['/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000003.raw','/lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/132545/132545_0000000012.raw']
    self.cc.checkFC2SE()     
    
  def test_checkTS2FC_givenProdID(self):
    print "\nTest check TS2FC given an LFN list"
    self.cc = ConsistencyChecks( transClient = self.tcMock, dm = self.dmMock )
    self.cc.prod = 36297
    self.cc.checkTS2FC()      
        
    
    
  def tearDown( self ):
    pass
  

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(UtilitiesTestCase)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  