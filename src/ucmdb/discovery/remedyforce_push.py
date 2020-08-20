# coding=utf-8
import time

import logger


def reload_modules():
    import remedyforce_client
    import salesforce_client
    import remedyforce_push_parser
    import remedyforce_connection_manager
    import rest_requests
    reload(rest_requests)

    reload(salesforce_client)
    reload(remedyforce_client)
    reload(remedyforce_push_parser)
    reload(remedyforce_connection_manager)


reload_modules()
import remedyforce_push_parser
from remedyforce_connection_manager import FrameworkBasedConnectionDataManager


def DiscoveryMain(Framework):
    logger.info('#' * 10 + 'Begin push to Remedyforce' + '#' * 10)
    testConnection = Framework.getTriggerCIData('testConnection')
    if testConnection == 'true':
        return test_connection(Framework)
    t0 = time.time()
    logger.info('Start at ', t0)
    addResult = Framework.getTriggerCIData('addResult')
    updateResult = Framework.getTriggerCIData('updateResult')
    deleteResult = Framework.getTriggerCIData('deleteResult')

    logger.debug('=============Add Result\n', addResult)
    logger.debug('Add Result End==================\n')

    logger.debug('=============Update Result Start\n', updateResult)
    logger.debug('Update Result End===================\n')

    logger.debug('=============Delete Result\n', deleteResult)
    logger.debug('Delete Result End===================\n')
    try:
        client = login(Framework)
        logger.debug('Login successfully.')
    except:
        logger.debugException('Login failed')
        raise Exception('Failed to connect to Remedyforce.')
    push_handler = PushHandler(client)
    try:
        push_handler.handle(addResult, updateResult, deleteResult)
    except:
        print logger.prepareFullStackTrace('')
        logger.debugException('')
    t1 = time.time()
    logger.info('End at ', t1)
    logger.info('Total time spent ', t1 - t0)
    logger.info('#' * 10 + 'End push to Remedyforce' + '#' * 10)

    return emptyVector()


def test_connection(Framework):
    logger.info('Test connection ...')
    try:
        login(Framework)
        logger.info('Connection success.')
        return emptyVector()
    except Exception, e:
        logger.debugException('Failed to connect to Remedyforce')
        raise Exception('Failed to connect to Remedyforce:%s' % e.message)


def emptyVector():
    from appilog.common.system.types.vectors import ObjectStateHolderVector
    return ObjectStateHolderVector()


def login(Framework):
    connection_mgr = FrameworkBasedConnectionDataManager(Framework)
    return connection_mgr.getClient()


class PushHandler(object):
    def __init__(self, client):
        """
        @type client: remedyforce_client.RemedyForceClient
        @return:
        """
        super(PushHandler, self).__init__()
        self.client = client
        self.add_cis = []
        self.add_links = []
        self.update_cis = []
        self.update_links = []
        self.delete_cis = []
        self.delete_links = []
        self.result_ci_id_map = {}
        self.ci_map = {}

    def prepare(self, addResult, updateResult, deleteResult):
        if addResult:
            self.add_cis, self.add_links = get_ci_and_rel(addResult)
        if updateResult:
            self.update_cis, self.update_links = get_ci_and_rel(updateResult)
        if deleteResult:
            self.delete_cis, self.delete_links = get_ci_and_rel(deleteResult)

    def handle(self, addResult, updateResult, deleteResult):
        self.prepare(addResult, updateResult, deleteResult)
        self.handleAdd()
        self.handleUpdate()
        self.handleDelete()

    def handleAddOrUpdate(self, cis, links):
        if cis:
            for ci in cis:
                data = ci.getData()
                logger.debug('Push CI:', ci)
                result_id = None
                try:
                    result_id = self.client.upsertObject(ci.getName(), ci.getId(), data)
                except:
                    print logger.prepareFullStackTrace('')
                    logger.debugException('')
                logger.debug('Remedyforce ID is:', result_id)

                if not result_id:
                    result_id = self.client.getObjectByUniqueId(ci.getId())
                if result_id:
                    self.result_ci_id_map[ci.getId()] = result_id
                    self.ci_map[ci.getId()] = ci
        if links:
            for link in links:
                logger.debug('Push relationship:', link)
                source_id = self.result_ci_id_map.get(link.getCIEnd1())
                if not source_id:
                    source_id = self.client.getObjectByUniqueId(link.getCIEnd1())
                destination_id = self.result_ci_id_map.get(link.getCIEnd2())
                if not destination_id:
                    destination_id = self.client.getObjectByUniqueId(link.getCIEnd2())
                data = link.getData()
                if source_id and destination_id:
                    result_id = None
                    try:
                        logger.debug('Insert:', link.getName())
                        logger.debug('Insert source:', source_id)
                        logger.debug('Insert destination:', destination_id)
                        logger.debug('Insert data:', data)

                        result_id = self.client.upsertRelationship(link.getName(), source_id, destination_id,
                                                                   link.getId(), data)
                    except:
                        print logger.prepareFullStackTrace('')
                        logger.debugException('')
                    logger.debug('Remedyforce ID is', result_id)
                else:
                    logger.debug('Discard isolated CI.')

    def handleAdd(self):
        logger.info('Handle add, ci count:', len(self.add_cis))
        logger.info('Handle add, link count:', len(self.add_links))
        self.handleAddOrUpdate(self.add_cis, self.add_links)

    def handleUpdate(self):
        logger.info('Handle update, ci count:', len(self.update_cis))
        logger.info('Handle update, link count:', len(self.update_links))
        self.handleAddOrUpdate(self.update_cis, self.update_links)

    def handleDelete(self):
        logger.info('Handle delete, ci count:', len(self.delete_cis))
        logger.info('Handle delete, link count:', len(self.delete_links))
        if self.delete_cis:
            for ci in self.delete_cis:
                logger.debug('Delete ci:', ci)
                remedyforce_id = self.client.getObjectByUniqueId(ci.getMamId())
                if remedyforce_id:
                    logger.debug('Delete ci from remedyforce:', remedyforce_id)
                    try:
                        self.client.deleteObject(remedyforce_id)
                    except:
                        logger.debugException('Failed to delete ci')
        if self.delete_links:
            for link in self.delete_links:
                logger.debug('Delete relationship:', link)
                remedyforce_id = self.client.getRelationshipByUniqueId(link.getMamId())
                if remedyforce_id:
                    logger.debug('Delete rel from remedyforce:', remedyforce_id)
                    try:
                        self.client.deleteRelationship(remedyforce_id)
                    except:
                        logger.debugException('Failed to delete rel')


def get_ci_and_rel(addXml):
    return remedyforce_push_parser.parse(addXml)
