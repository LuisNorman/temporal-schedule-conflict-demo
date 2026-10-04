package com.example;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * The trivial workflow a Temporal schedule can run. We only care that the schedule exists and can
 * be updated. The workflow is scheduled with a long enough interval that it does not actually run
 * during the demo.
 */
@WorkflowInterface
public interface HeartbeatWorkflow {
  @WorkflowMethod
  void heartbeat();
}
