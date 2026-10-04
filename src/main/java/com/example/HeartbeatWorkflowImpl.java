package com.example;

public class HeartbeatWorkflowImpl implements HeartbeatWorkflow {
  @Override
  public void heartbeat() {
    // No-op. The demo cares about schedule updates, not workflow execution.
  }
}
