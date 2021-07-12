#coding=utf-8
import InventoryUtils
import LockUtils
from java.lang import Boolean
from com.hp.ucmdb.discovery.common import CollectorsConstants
from com.hp.ucmdb.discovery.probe.agents.probemgr.workflow.state import WorkflowStepStatus
import logger

AGENT_DRIVEN_FLOW_OPTION_ENABLED = 'SCHEDULER_Enabled'
JOB_PARAM_ABORT_ON_ADI_ENABLED = 'abortOnAgentDrivenFlowDetected'

def StepMain(Framework):
    InventoryUtils.executeStep(Framework, CheckAdiModeEnabled, InventoryUtils.STEP_REQUIRES_CONNECTION, InventoryUtils.STEP_DOESNOT_REQUIRES_LOCK)

def CheckAdiModeEnabled(Framework):
    client = Framework.getConnectedClient()
    agentOptions = LockUtils.getClientOptionsMap(client)
    logger.debug('Going to check if remote agent is operating in agent driven mode')
    rawAdiFlowEnabled = agentOptions.get(AGENT_DRIVEN_FLOW_OPTION_ENABLED) or "false"
    adiFlowEnabled = Boolean.parseBoolean(rawAdiFlowEnabled)
    
    rawShouldFailOnAdiEnabled = Framework.getParameter(JOB_PARAM_ABORT_ON_ADI_ENABLED) or "true"
    shouldFailOnAdiEnabled = Boolean.parseBoolean(rawShouldFailOnAdiEnabled)
    logger.debug('Remote agent is operating in agent driven mode %s' % adiFlowEnabled)
    if adiFlowEnabled and shouldFailOnAdiEnabled:
        logger.debug('Since AD mode is %s and shouldFailOnAdiEnabled is %s. Will abort this trigger.' % (adiFlowEnabled, shouldFailOnAdiEnabled))
        Framework.setStepExecutionStatus(WorkflowStepStatus.FAILURE)
        logger.reportError('Remote Agent is operating in Agent Driven Mode. Aborting trigger.') 
    else:  
        logger.debug('Since AD mode is %s and shouldFailOnAdiEnabled is %s. Will continue current flow' % (adiFlowEnabled, shouldFailOnAdiEnabled))
        Framework.setStepExecutionStatus(WorkflowStepStatus.SUCCESS)