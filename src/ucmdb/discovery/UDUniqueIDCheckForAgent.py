#coding=utf-8
import logger

import InventoryUtils
import LockUtils
from java.lang import Boolean

JOB_PARAM_SKIP_UD_UUID_CHECK = "SkipUUIDCheck"

from com.hp.ucmdb.discovery.probe.agents.probemgr.workflow.state import WorkflowStepStatus


def StepMain(Framework):
    InventoryUtils.executeStep(Framework, UDUniqueIDCheck, InventoryUtils.STEP_REQUIRES_CONNECTION, InventoryUtils.STEP_REQUIRES_LOCK)

def UDUniqueIDCheck(Framework):

    nodeGUID = Framework.getDestinationAttribute('nodeGUID')

    client = Framework.getConnectedClient()
    client_options = LockUtils.getClientOptionsMap(client)
    logger.debug("Get client options map for UdUid:", client_options)
    remoteGUID = client_options.get(InventoryUtils.AGENT_OPTION_CALLHOME_GUID)

    if (nodeGUID and nodeGUID != 'NA' and remoteGUID and nodeGUID == remoteGUID):
        Framework.setStepExecutionStatus(WorkflowStepStatus.SUCCESS)
    else:
        logger.debug("node UdUid:", nodeGUID)
        logger.warn("GUID is different, may be they are different hosts!")

        Framework.reportWarning("GUID is different, may be they are different hosts!")

        skipUUDCheck = Framework.getParameter(JOB_PARAM_SKIP_UD_UUID_CHECK)
        if not skipUUDCheck or not len(skipUUDCheck):
            skipUUDCheck = False
        else:
            skipUUDCheck = Boolean.valueOf(skipUUDCheck)

        if skipUUDCheck:
            logger.warn("SkipUUIDCheck option is enabled, the job would proceed even if the UUID is different with cached one")
            Framework.setStepExecutionStatus(WorkflowStepStatus.SUCCESS)
        else:
            Framework.setStepExecutionStatus(WorkflowStepStatus.CANCEL)