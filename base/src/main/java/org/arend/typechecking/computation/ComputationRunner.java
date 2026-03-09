package org.arend.typechecking.computation;

import org.arend.util.ComputationInterruptedException;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class ComputationRunner<T> {
  private static CancellationIndicator CANCELLATION_INDICATOR = UnstoppableCancellationIndicator.INSTANCE;
  private static final Lock lock = new ReentrantLock();
  private static final ScheduledExecutorService WATCHDOG = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "arend-computation-watchdog");
    t.setDaemon(true);
    return t;
  });
  public static final long COMPUTATION_TIMEOUT_SECONDS = 30;

  public static void checkCanceled() throws ComputationInterruptedException {
    CANCELLATION_INDICATOR.checkCanceled();
  }

  public static CancellationIndicator getCancellationIndicator() {
    return CANCELLATION_INDICATOR;
  }

  public static void resetCancellationIndicator() {
    CANCELLATION_INDICATOR = UnstoppableCancellationIndicator.INSTANCE;
  }

  public static boolean isCancellationIndicatorSet() {
    return CANCELLATION_INDICATOR != UnstoppableCancellationIndicator.INSTANCE;
  }

  protected T computationInterrupted() {
    return null;
  }

  public static void lock(CancellationIndicator cancellationIndicator) {
    lock.lock();
    if (cancellationIndicator != null) {
      CANCELLATION_INDICATOR = cancellationIndicator;
    }
  }

  public static void unlock() {
    CANCELLATION_INDICATOR = UnstoppableCancellationIndicator.INSTANCE;
    lock.unlock();
  }

  public T run(CancellationIndicator cancellationIndicator, Supplier<T> runnable) {
    lock(cancellationIndicator);
    final CancellationIndicator activeIndicator = CANCELLATION_INDICATOR;
    ScheduledFuture<?> watchdog = WATCHDOG.schedule((Runnable) activeIndicator::cancel, COMPUTATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    try {
      return runnable.get();
    } catch (ComputationInterruptedException ignored) {
      return computationInterrupted();
    } finally {
      watchdog.cancel(false);
      unlock();
    }
  }
}
