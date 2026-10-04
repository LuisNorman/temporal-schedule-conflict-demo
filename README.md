# temporal-schedule-conflict-demo

A reproducer for [temporalio/sdk-java#2809](https://github.com/temporalio/sdk-java/issues/2809) and a sanity check that the fix wires through end to end.

## The bug

`ScheduleHandle.update` in the Temporal Java SDK did not retry when the server rejected an `UpdateSchedule` request with `FAILED_PRECONDITION: conflict token mismatch` (introduced by [temporalio/temporal#8474](https://github.com/temporalio/temporal/pull/8474)). That rejection fires when another client updates the same schedule between a client's `Describe` and `Update`. The Javadoc on `ScheduleHandle.update` already promised that the callback "can be called multiple times in case of conflict", but the implementation never retried, so the callers saw a generic `ScheduleException` instead.

## The fix

On each attempt the SDK now:

1. Captures the `conflict_token` from `DescribeScheduleResponse`.
2. Sends it on `UpdateScheduleRequest`.
3. On a `FAILED_PRECONDITION` whose status description contains "conflict token", re-describes the schedule and invokes the user's callback again with the current state.
4. Caps the retry budget at 10 attempts and surfaces the last server error when exhausted.

See the PR for the SDK change.

## What this demo shows

* The patched SDK compiles against a real Temporal server, creates and updates a schedule, and tears it down cleanly.
* The SDK plumbing for conflict tokens is wired end to end: `DescribeSchedule` -> `UpdateScheduleInput#getConflictToken` -> `UpdateScheduleRequest#conflict_token`.
* When the server returns `FAILED_PRECONDITION: conflict token mismatch`, the SDK retries the user's callback transparently.

## What this demo does not yet show

Temporal is in the middle of migrating schedules to a new backend called the **CHASM Scheduler**. [temporalio/temporal#8474](https://github.com/temporalio/temporal/pull/8474) wired `conflict_token` enforcement into that new path, with the frontend falling back to the legacy V1 scheduler when CHASM is not yet available for a schedule. On the server builds in use today (1.32.0 ships V1 as the default for existing schedules), the forced-conflict scenario below is accepted by V1 silently and no retry fires. The SDK fix is forward compatible: once a schedule routes to CHASM the server will return `FAILED_PRECONDITION` on a stale token and the SDK will retry transparently.

If you want to see the retry fire today, point the demo at a Temporal server built from `temporalio/temporal` main (or any build that routes schedules through CHASM).

## Prerequisites

* JDK 21.
* The `temporal` CLI on `PATH`:
  ```
  brew install temporal
  ```
* A local checkout of [temporalio/sdk-java](https://github.com/temporalio/sdk-java) on the `schedule-conflict-token-retry` branch with the SDK fix, published to Maven local:
  ```
  cd /path/to/sdk-java
  ./gradlew :temporal-sdk:publishToMavenLocal :temporal-serviceclient:publishToMavenLocal
  ```
  The demo depends on `io.temporal:temporal-sdk:1.41.0-SNAPSHOT` from the local Maven cache.

## Running

Start the Temporal dev server in one shell:

```
temporal server start-dev --log-level warn
```

Run the demo in another:

```
./gradlew run
```

## What to expect

Two scenarios run.

### Scenario 1: forced stale conflict token

Inside the outer `handle.update` callback the demo fires a second `handle.update` on the same schedule. That inner update bumps the server-side state and invalidates the token the outer update is holding. On a server that enforces #8474 the outer update sees `FAILED_PRECONDITION` and the SDK retries, so the outer callback is invoked twice. On a server that does not enforce yet the outer update succeeds silently and the callback is invoked once.

```
=== Scenario 1: forced stale conflict token ===
outer updater invocation #1
firing inner update to invalidate the outer token
outer updater invocation #2         <-- only prints on a server with #8474
```

### Scenario 2: five concurrent updaters

Five worker threads start together and each run their own `handle.update`. The number of retries you see depends on the server (does it enforce?) and on the race (do the updates actually collide?). The results line reports the exact count:

```
---- results ----
concurrent updaters   : 5
updates succeeded     : 5
updates failed        : 0
total updater invokes : 5
retries observed      : 0
All updates succeeded with no retries. Either the race did not collide or the server
is not yet enforcing conflict tokens on this build.
```

## File layout

* `src/main/java/com/example/HeartbeatWorkflow.java` - trivial scheduled workflow interface.
* `src/main/java/com/example/HeartbeatWorkflowImpl.java` - no-op workflow implementation. The schedule is paused on creation and runs every 24 hours, so the workflow never actually executes during the demo.
* `src/main/java/com/example/ConflictDemo.java` - entry point and the two scenarios.
* `src/main/resources/simplelogger.properties` - routes `ScheduleHandleImpl` logging to DEBUG so retries are visible.

## Related

* Server PR: [temporalio/temporal#8474](https://github.com/temporalio/temporal/pull/8474)
* SDK issue: [temporalio/sdk-java#2809](https://github.com/temporalio/sdk-java/issues/2809)
* SDK fix branch: `schedule-conflict-token-retry`
