/*package com.ericsson.eniq.poc.shortlivedjob;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.cloud.task.listener.TaskExecutionListener;
import org.springframework.cloud.task.repository.TaskExecution;

public class TaskListener implements TaskExecutionListener {
	
	private static final Logger LOG = LogManager.getLogger(TaskListener.class);

	@Override
	public void onTaskEnd(TaskExecution taskExecution) {
		LOG.log(Level.INFO, "task ended : " + taskExecution.getArguments());

	}

	@Override
	public void onTaskFailed(TaskExecution taskExecution, Throwable throwable) {
		LOG.log(Level.INFO, "task failed : " + taskExecution.getErrorMessage(), throwable);

	}

	@Override
	public void onTaskStartup(TaskExecution taskExecution) {
		LOG.log(Level.INFO, "task started : " + taskExecution.getArguments());

	}

}
*/