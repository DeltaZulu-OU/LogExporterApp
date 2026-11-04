/*
Technitium DNS Server
Copyright (C) 2025  Shreyas Zare (shreyas@technitium.com)
Copyright (C) 2025  Zafer Balkan (zafer@zaferbalkan.com)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

*/

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace LogForwarder.Sinks
{
    public sealed class SinkDispatcher : IDisposable
    {
        #region variables

        private readonly Lock _sync = new Lock();
        private readonly Dictionary<Type, SinkWorker> _workers =
            new Dictionary<Type, SinkWorker>();
        private SinkWorker[] _workerSnapshot = Array.Empty<SinkWorker>();
        private readonly Dictionary<Type, SinkDeliveryCounters> _retiredCounters = new();

        private readonly TimeProvider _timeProvider;
        private readonly Func<double> _jitterSample;
        private bool _disposed;

        public SinkDispatcher(TimeProvider? timeProvider = null, Func<double>? jitterSample = null)
        {
            _timeProvider = timeProvider ?? TimeProvider.System;
            _jitterSample = jitterSample ?? Random.Shared.NextDouble;
        }

        #endregion

        #region IDisposable

        public void Dispose()
        {
            SinkWorker[] workers;

            lock (_sync)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
                workers = _workerSnapshot;
                _workers.Clear();
                Volatile.Write(ref _workerSnapshot, Array.Empty<SinkWorker>());
            }

            foreach (var worker in workers)
            {
                worker.Dispose();
            }
        }

        #endregion

        #region public

        public void Add(
            IOutputSink sink,
            int queueCapacity,
            Action<Exception>? onError = null,
            Action<string>? onLog = null)
        {
            Add(sink, new SinkBatchOptions { QueueCapacity = queueCapacity }, onError, onLog);
        }

        public void Add(
            IOutputSink sink,
            SinkBatchOptions options,
            Action<Exception>? onError = null,
            Action<string>? onLog = null)
        {
            ArgumentNullException.ThrowIfNull(sink);
            ArgumentNullException.ThrowIfNull(options);
            try
            {
                options.Validate();
                lock (_sync)
                {
                    ObjectDisposedException.ThrowIf(_disposed, this);

                    if (_workers.ContainsKey(sink.GetType()))
                    {
                        throw new InvalidOperationException(
                            $"Strategy of type {sink.GetType().Name} already registered.");
                    }

                    _workers.Add(
                        sink.GetType(),
                        new SinkWorker(sink, options, onError, onLog, _timeProvider, _jitterSample));
                    RefreshSnapshotLocked();
                }
            }
            catch
            {
                sink.Dispose();
                throw;
            }
        }

        public void Remove(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            SinkWorker? worker;

            lock (_sync)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                _workers.Remove(type, out worker);
                RefreshSnapshotLocked();
            }

            if (worker is not null)
            {
                worker.Dispose();
                lock (_sync)
                {
                    _retiredCounters.TryGetValue(type, out var earlier);
                    _retiredCounters[type] = AddCounters(earlier, worker.Counters);
                }
            }
        }

        private static SinkDeliveryCounters AddCounters(SinkDeliveryCounters a, SinkDeliveryCounters b) => new(
            a.DroppedEvents + b.DroppedEvents,
            a.SuccessfulEvents + b.SuccessfulEvents,
            a.FailedEvents + b.FailedEvents,
            a.SuccessfulBatches + b.SuccessfulBatches,
            a.RetriedAttempts + b.RetriedAttempts,
            a.FailedBatches + b.FailedBatches);

        public SinkDeliveryCounters GetCounters(Type sinkType)
        {
            ArgumentNullException.ThrowIfNull(sinkType);
            lock (_sync)
            {
                var hasRetired = _retiredCounters.TryGetValue(sinkType, out var retired);
                if (_workers.TryGetValue(sinkType, out var worker))
                    return AddCounters(retired, worker.Counters);
                if (hasRetired)
                    return retired;
                throw new KeyNotFoundException($"No sink registered for {sinkType.Name}.");
            }
        }

        public bool Any()
        {
            lock (_sync)
            {
                return !_disposed && _workers.Count > 0;
            }
        }

        public Task DispatchAsync(IReadOnlyList<LogEntry> logs, CancellationToken token)
        {
            if (logs == null || logs.Count == 0 || token.IsCancellationRequested)
            {
                return Task.CompletedTask;
            }

            var workers = Volatile.Read(ref _workerSnapshot);

            if (workers.Length == 0)
            {
                return Task.CompletedTask;
            }

            foreach (var worker in workers)
            {
                worker.Enqueue(logs);
            }

            return Task.CompletedTask;
        }

        public Task DrainAsync() => DrainCoreAsync(timeout: null);

        public Task<bool> DrainAsync(TimeSpan timeout)
        {
            if (timeout <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(timeout));
            }

            return DrainCoreAsync(timeout);
        }

        private async Task<bool> DrainCoreAsync(TimeSpan? timeout)
        {
            var workers = Volatile.Read(ref _workerSnapshot);

            if (workers.Length == 0)
            {
                return true;
            }

            var tasks = new Task[workers.Length];

            for (var i = 0; i < workers.Length; i++)
            {
                workers[i].Complete();
                tasks[i] = workers[i].Completion;
            }

            var completion = Task.WhenAll(tasks);

            if (timeout is null)
            {
                await completion.ConfigureAwait(false);
                return true;
            }

            try
            {
                await completion.WaitAsync(timeout.Value).ConfigureAwait(false);
                return true;
            }
            catch (TimeoutException)
            {
                foreach (var worker in workers)
                {
                    worker.Abort();
                }

                return false;
            }
        }

        #endregion

        private void RefreshSnapshotLocked()
        {
            var snapshot = new SinkWorker[_workers.Count];
            _workers.Values.CopyTo(snapshot, 0);
            Volatile.Write(ref _workerSnapshot, snapshot);
        }

        private sealed class SinkWorker : IDisposable
        {
            private static readonly TimeSpan DropLogInterval = TimeSpan.FromSeconds(5);

            private readonly TimeProvider _timeProvider;
            private readonly Func<double> _jitterSample;
            private readonly IOutputSink _sink;
            private readonly IBatchRetryPolicy? _retryPolicy;
            private readonly bool _replayEnabled;
            private readonly SinkBatchOptions _options;
            private long _totalDroppedEvents;
            private long _successfulEvents;
            private long _failedEvents;
            private long _successfulBatches;
            private long _retriedAttempts;
            private long _failedBatches;
            private readonly Channel<LogEntry> _queue;
            private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();
            private readonly Action<Exception>? _onError;
            private readonly Action<string>? _onLog;

            private long _droppedEvents;
            private long _lastDropTicks = DateTime.UtcNow.Ticks;
            private long _lastErrorTicks;
            private int _disposed;

            public SinkWorker(
                IOutputSink sink,
                SinkBatchOptions options,
                Action<Exception>? onError,
                Action<string>? onLog,
                TimeProvider timeProvider,
                Func<double> jitterSample)
            {
                _timeProvider = timeProvider;
                _jitterSample = jitterSample;
                _sink = sink;
                _retryPolicy = sink as IBatchRetryPolicy;
                // Evaluate once during registration: a malformed policy fails before
                // any events can be accepted, instead of losing an active batch.
                _replayEnabled = _retryPolicy?.IsReplayEnabled == true;
                _options = options;
                _onError = onError;
                _onLog = onLog;

                _queue = Channel.CreateBounded<LogEntry>(
                    new BoundedChannelOptions(options.QueueCapacity)
                    {
                        SingleReader = true,
                        SingleWriter = false,
                        FullMode = BoundedChannelFullMode.DropWrite
                    },
                    _ => OnDropped());

                Completion = RunAsync();
            }

            public Task Completion { get; }

            public SinkDeliveryCounters Counters => new(
                Interlocked.Read(ref _totalDroppedEvents),
                Interlocked.Read(ref _successfulEvents),
                Interlocked.Read(ref _failedEvents),
                Interlocked.Read(ref _successfulBatches),
                Interlocked.Read(ref _retriedAttempts),
                Interlocked.Read(ref _failedBatches));

            public void Enqueue(IReadOnlyList<LogEntry> logs)
            {
                if (Volatile.Read(ref _disposed) != 0)
                {
                    return;
                }

                for (var i = 0; i < logs.Count; i++)
                {
                    _queue.Writer.TryWrite(logs[i]);
                }
            }

            public void Complete() => _queue.Writer.TryComplete();

            public void Abort() => _cancellation.Cancel();

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) != 0)
                {
                    return;
                }

                Complete();

                if (!Completion.IsCompleted)
                {
                    _cancellation.Cancel();
                }

                var deferCancellationDispose = false;

                try
                {
                    Completion
                        .WaitAsync(TimeSpan.FromSeconds(1), CancellationToken.None)
                        .GetAwaiter()
                        .GetResult();
                }
                catch (OperationCanceledException) when (_cancellation.IsCancellationRequested)
                {
                    // Expected after requesting worker cancellation during disposal.
                }
                catch (TimeoutException)
                {
                    // Forced shutdown is bounded. Dispose the sink below to release its I/O,
                    // but keep the cancellation source alive until the worker actually exits.
                    deferCancellationDispose = true;
                }
                finally
                {
                    if (deferCancellationDispose)
                    {
                        try
                        {
                            DisposeSink();
                        }
                        finally
                        {
                            _ = Completion.ContinueWith(
                                static (_, state) =>
                                    ((CancellationTokenSource)state!).Dispose(),
                                _cancellation,
                                CancellationToken.None,
                                TaskContinuationOptions.ExecuteSynchronously,
                                TaskScheduler.Default);
                        }
                    }
                    else
                    {
                        _cancellation.Dispose();
                        DisposeSink();
                    }
                }
            }

            private void DisposeSink()
            {
                try
                {
                    _sink.Dispose();
                }
                catch (Exception ex)
                {
                    ReportError(ex);
                }
            }

            private async Task RunAsync()
            {
                var batch = new List<LogEntry>(_options.BatchSize);

                try
                {
                    while (await _queue.Reader
                        .WaitToReadAsync(_cancellation.Token)
                        .ConfigureAwait(false))
                    {
                        while (batch.Count < _options.BatchSize &&
                               _queue.Reader.TryRead(out var entry))
                        {
                            batch.Add(entry);
                        }

                        if (batch.Count < _options.BatchSize &&
                            !_queue.Reader.Completion.IsCompleted)
                        {
                            await FillBatchUntilDelayAsync(batch).ConfigureAwait(false);
                        }

                        if (batch.Count == 0)
                        {
                            continue;
                        }

                        try
                        {
                            await ExportWithRetryAsync(batch).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (_cancellation.IsCancellationRequested)
                        {
                            return;
                        }
                        catch (Exception ex)
                        {
                            ReportError(ex);
                        }
                        finally
                        {
                            batch.Clear();
                        }
                    }
                }
                catch (OperationCanceledException) when (_cancellation.IsCancellationRequested)
                {
                    // Expected during worker shutdown; do not fault Completion.
                }
                catch (Exception ex)
                {
                    ReportError(ex);
                }
            }

            private static void CancelCancellationSource(object? state)
            {
                try
                {
                    ((CancellationTokenSource)state!).Cancel();
                }
                catch (ObjectDisposedException)
                {
                    // A queued timer callback may outlive disposal.
                }
            }

            private async Task FillBatchUntilDelayAsync(List<LogEntry> batch)
            {
                // One cancelable availability wait at a time. The same deadline spans
                // the whole partial batch rather than restarting after each event.
                using var deadline = CancellationTokenSource.CreateLinkedTokenSource(_cancellation.Token);
                using var flushTimer = _timeProvider.CreateTimer(
                    CancelCancellationSource,
                    deadline, _options.BatchDelay, Timeout.InfiniteTimeSpan);

                try
                {
                    while (batch.Count < _options.BatchSize &&
                           await _queue.Reader.WaitToReadAsync(deadline.Token).ConfigureAwait(false))
                    {
                        while (batch.Count < _options.BatchSize &&
                               _queue.Reader.TryRead(out var entry))
                        {
                            batch.Add(entry);
                        }
                    }
                }
                catch (OperationCanceledException) when (
                    deadline.IsCancellationRequested && !_cancellation.IsCancellationRequested)
                {
                    // Partial-batch flush deadline reached; export what was collected.
                }
            }

            private async Task ExportWithRetryAsync(List<LogEntry> batch)
            {
                var started = _timeProvider.GetTimestamp();
                var delay = _options.InitialRetryDelay;
                // Do not replay whole batches unless the target explicitly opts into replay.
                // HTTP, append-only file, console and syslog may have partially committed work.
                while (true)
                {
                    _cancellation.Token.ThrowIfCancellationRequested();
                    using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(_cancellation.Token);
                    using var attemptTimer = _timeProvider.CreateTimer(
                        CancelCancellationSource,
                        attemptCts, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                    if (!TryArmAttemptDeadline(attemptTimer, started, batch.Count))
                    {
                        return;
                    }

                    try
                    {
                        await _sink.ExportAsync(batch, attemptCts.Token).ConfigureAwait(false);
                        // A successful return is an acknowledgment. A deadline expiring
                        // immediately afterward cannot retroactively invalidate it.
                        Interlocked.Add(ref _successfulEvents, batch.Count);
                        Interlocked.Increment(ref _successfulBatches);
                        return;
                    }
                    catch (OperationCanceledException) when (_cancellation.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (OperationCanceledException) when (attemptCts.IsCancellationRequested)
                    {
                        // An attempt timed out: no further attempts are permitted after the deadline.
                        RecordTerminalFailure(batch.Count);
                        return;
                    }
                    catch (Exception ex)
                    {
                        ReportError(ex);
                        var remaining = _options.RetryTimeLimit - _timeProvider.GetElapsedTime(started);
                        if (!ShouldRetry(ex, remaining, batch.Count))
                        {
                            return;
                        }

                        // Retry only an explicitly replay-safe batch, without overtaking it.
                        var jitter = 0.75 + _jitterSample() * 0.5;
                        var wait = TimeSpan.FromMilliseconds(Math.Min(remaining.TotalMilliseconds,
                            delay.TotalMilliseconds * jitter));
                        Interlocked.Increment(ref _retriedAttempts);
                        await Task.Delay(wait, _timeProvider, _cancellation.Token).ConfigureAwait(false);
                        delay = TimeSpan.FromMilliseconds(Math.Min(_options.MaxRetryDelay.TotalMilliseconds,
                            delay.TotalMilliseconds * 2));
                    }
                }
            }

            private bool ShouldRetry(Exception error, TimeSpan remaining, int count)
            {
                try
                {
                    if (_replayEnabled && remaining > TimeSpan.Zero &&
                        _retryPolicy!.CanRetry(error))
                    {
                        return true;
                    }
                }
                catch (Exception policyError)
                {
                    // A faulty policy cannot bypass delivery accounting.
                    ReportError(policyError);
                }

                RecordTerminalFailure(count);
                return false;
            }

            private bool TryArmAttemptDeadline(ITimer attemptTimer, long started, int count)
            {
                if (!_replayEnabled || _options.RetryTimeLimit <= TimeSpan.Zero)
                {
                    return true;
                }

                var remaining = _options.RetryTimeLimit - _timeProvider.GetElapsedTime(started);
                if (remaining <= TimeSpan.Zero)
                {
                    RecordTerminalFailure(count);
                    return false;
                }

                attemptTimer.Change(remaining, Timeout.InfiniteTimeSpan);
                return true;
            }

            private void RecordTerminalFailure(int count)
            {
                Interlocked.Add(ref _failedEvents, count);
                Interlocked.Increment(ref _failedBatches);
                ReportTerminalLoss(count);
            }

            private void ReportTerminalLoss(int count)
            {
                try
                {
                    _onLog?.Invoke($"{_sink.GetType().Name} batch delivery unconfirmed for {count} log entries; some records may have been delivered.");
                }
                catch { /* Reporting must not terminate worker. */ }
            }

            private void OnDropped()
            {
                Interlocked.Increment(ref _totalDroppedEvents);
                Interlocked.Increment(ref _droppedEvents);

                var nowTicks = DateTime.UtcNow.Ticks;
                var lastTicks = Volatile.Read(ref _lastDropTicks);

                if (new TimeSpan(nowTicks - lastTicks) >= DropLogInterval &&
                    Interlocked.CompareExchange(ref _lastDropTicks, nowTicks, lastTicks) == lastTicks)
                {
                    var dropped = Interlocked.Exchange(ref _droppedEvents, 0);
                    try
                    {
                        _onLog?.Invoke(
                            $"{_sink.GetType().Name} queue full; dropped {dropped} log entries over last {DropLogInterval.TotalSeconds:F0}s.");
                    }
                    catch
                    {
                        // Observability must never break sink isolation.
                    }
                }
            }

            private void ReportError(Exception ex)
            {
                var nowTicks = DateTime.UtcNow.Ticks;
                var lastTicks = Volatile.Read(ref _lastErrorTicks);

                if (lastTicks != 0 &&
                    new TimeSpan(nowTicks - lastTicks) < DropLogInterval)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref _lastErrorTicks, nowTicks, lastTicks) != lastTicks)
                {
                    return;
                }

                try
                {
                    _onError?.Invoke(ex);
                }
                catch
                {
                    // Error reporting must not terminate this sink worker.
                }
            }
        }
    }
}
