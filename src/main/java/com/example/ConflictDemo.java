package com.example;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.schedules.Schedule;
import io.temporal.client.schedules.ScheduleActionStartWorkflow;
import io.temporal.client.schedules.ScheduleClient;
import io.temporal.client.schedules.ScheduleClientOptions;
import io.temporal.client.schedules.ScheduleHandle;
import io.temporal.client.schedules.ScheduleIntervalSpec;
import io.temporal.client.schedules.ScheduleOptions;
import io.temporal.client.schedules.ScheduleSpec;
import io.temporal.client.schedules.ScheduleState;
import io.temporal.client.schedules.ScheduleUpdate;
import io.temporal.client.schedules.ScheduleUpdateInput;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exercises the conflict-token retry behavior of {@link ScheduleHandle#update} against a real
 * Temporal server.
 *
 * <p>Setup: start a local Temporal dev server with {@code docker-compose up -d}. Then run this
 * class with {@code ./gradlew run}. The demo prints a timeline of concurrent updates. With the
 * fixed SDK, every update eventually succeeds and the SDK logs a retry on the loser of each race.
 * With an unfixed SDK the losers surface {@code FAILED_PRECONDITION: conflict token mismatch}.
 */
public final class ConflictDemo {

  private static final Logger log = LoggerFactory.getLogger(ConflictDemo.class);
  private static final String TASK_QUEUE = "schedule-conflict-demo";
  private static final int CONCURRENT_UPDATERS = 5;

  public static void main(String[] args) throws Exception {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    try {
      ScheduleClient scheduleClient =
          ScheduleClient.newInstance(service, ScheduleClientOptions.newBuilder().build());

      log.info("=== Scenario 1: forced stale conflict token ===");
      runForcedConflictScenario(scheduleClient);

      log.info("=== Scenario 2: {} concurrent updaters ===", CONCURRENT_UPDATERS);
      runConcurrentScenario(scheduleClient);
    } finally {
      service.shutdownNow();
      service.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /**
   * Deterministic reproducer: inside the outer updater, fire a second {@code handle.update} on the
   * same schedule. That inner update bumps the server-side state and invalidates the token the
   * outer update is holding. If the server enforces conflict tokens, the outer update sees
   * FAILED_PRECONDITION and the SDK retries. If the server does not enforce yet, the outer update
   * succeeds silently and the updater is invoked only once.
   */
  private static void runForcedConflictScenario(ScheduleClient client) {
    String scheduleId = "conflict-demo-forced-" + UUID.randomUUID();
    ScheduleHandle handle = createSchedule(client, scheduleId);
    try {
      AtomicInteger invocations = new AtomicInteger();
      handle.update(
          (ScheduleUpdateInput input) -> {
            int n = invocations.incrementAndGet();
            log.info("outer updater invocation #{}", n);
            if (n == 1) {
              log.info("firing inner update to invalidate the outer token");
              handle.update(
                  (ScheduleUpdateInput innerInput) ->
                      new ScheduleUpdate(
                          Schedule.newBuilder(innerInput.getDescription().getSchedule())
                              .setState(ScheduleState.newBuilder().setPaused(false).build())
                              .build()));
            }
            return new ScheduleUpdate(input.getDescription().getSchedule());
          });
      log.info("outer update returned; updater invoked {} time(s)", invocations.get());
      if (invocations.get() > 1) {
        log.info(
            "Retry fired: the outer update's stale token was rejected and the SDK re-described.");
      } else {
        log.info(
            "No retry observed. Server likely does not yet enforce conflict tokens on this build.");
      }
    } finally {
      handle.delete();
    }
  }

  private static void runConcurrentScenario(ScheduleClient client) throws InterruptedException {
    String scheduleId = "conflict-demo-concurrent-" + UUID.randomUUID();
    ScheduleHandle handle = createSchedule(client, scheduleId);
    try {
      runConcurrentUpdates(handle);
    } finally {
      handle.delete();
    }
  }

  private static ScheduleHandle createSchedule(ScheduleClient client, String scheduleId) {
    WorkflowOptions wfOptions =
        WorkflowOptions.newBuilder()
            .setWorkflowId("heartbeat-" + scheduleId)
            .setTaskQueue(TASK_QUEUE)
            .build();
    Schedule schedule =
        Schedule.newBuilder()
            .setAction(
                ScheduleActionStartWorkflow.newBuilder()
                    .setWorkflowType(HeartbeatWorkflow.class)
                    .setOptions(wfOptions)
                    .build())
            .setSpec(
                ScheduleSpec.newBuilder()
                    .setIntervals(
                        Collections.singletonList(new ScheduleIntervalSpec(Duration.ofHours(24))))
                    .build())
            .setState(ScheduleState.newBuilder().setPaused(true).build())
            .build();
    return client.createSchedule(scheduleId, schedule, ScheduleOptions.newBuilder().build());
  }

  private static void runConcurrentUpdates(ScheduleHandle handle) throws InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_UPDATERS);
    CountDownLatch startGate = new CountDownLatch(1);
    CountDownLatch doneGate = new CountDownLatch(CONCURRENT_UPDATERS);
    AtomicInteger succeeded = new AtomicInteger();
    AtomicInteger failed = new AtomicInteger();
    AtomicInteger totalUpdaterInvocations = new AtomicInteger();
    List<Throwable> errors = Collections.synchronizedList(new java.util.ArrayList<>());

    for (int i = 0; i < CONCURRENT_UPDATERS; i++) {
      int worker = i;
      executor.submit(
          () -> {
            try {
              startGate.await();
              handle.update(
                  (ScheduleUpdateInput input) -> {
                    int invocation = totalUpdaterInvocations.incrementAndGet();
                    log.info(
                        "worker-{} updater invoked (global invocation #{})", worker, invocation);
                    Schedule current = input.getDescription().getSchedule();
                    boolean pausedNow = current.getState().isPaused();
                    Schedule next =
                        Schedule.newBuilder(current)
                            .setState(ScheduleState.newBuilder().setPaused(!pausedNow).build())
                            .build();
                    return new ScheduleUpdate(next);
                  });
              succeeded.incrementAndGet();
              log.info("worker-{} update succeeded", worker);
            } catch (Throwable t) {
              failed.incrementAndGet();
              errors.add(t);
              log.warn("worker-{} update failed: {}", worker, t.toString());
            } finally {
              doneGate.countDown();
            }
          });
    }

    log.info("Releasing {} concurrent updaters", CONCURRENT_UPDATERS);
    startGate.countDown();
    doneGate.await();
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    int retries = totalUpdaterInvocations.get() - succeeded.get();
    log.info("---- results ----");
    log.info("concurrent updaters   : {}", CONCURRENT_UPDATERS);
    log.info("updates succeeded     : {}", succeeded.get());
    log.info("updates failed        : {}", failed.get());
    log.info("total updater invokes : {}", totalUpdaterInvocations.get());
    log.info("retries observed      : {}", retries);
    if (!errors.isEmpty()) {
      log.info("first error: {}", errors.get(0).toString());
    }

    if (failed.get() > 0) {
      log.info(
          "Some updates failed. Expected on an SDK that does not retry conflict tokens (issue #2809).");
    } else if (retries > 0) {
      log.info("All updates succeeded. The SDK transparently retried on {} race(s).", retries);
    } else {
      log.info(
          "All updates succeeded with no retries. Either the race did not collide or the server is "
              + "not yet enforcing conflict tokens on this build.");
    }
  }

  private ConflictDemo() {}
}
