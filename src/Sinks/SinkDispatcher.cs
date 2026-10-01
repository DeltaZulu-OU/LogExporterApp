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

namespace LogExporter.Sinks
{
    public sealed class SinkDispatcher : IDisposable
    {
        #region variables

        private const int BULK_INSERT_COUNT = 1000;
        private static readonly TimeSpan BatchDelay = TimeSpan.FromMilliseconds(25);

        private readonly Lock _sync = new Lock();
        private readonly Dictionary<Type, SinkWorker> _workers =
            new Dictionary<Type, SinkWorker>();
        private SinkWorker[] _workerSnapshot = Array.Empty<SinkWorker>();

        private bool _disposed;

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
            ArgumentNullException.ThrowIfNull(sink);

            try
            {
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
                        new SinkWorker(sink, queueCapacity, onError, onLog));
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

            worker?.Dispose();
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

            private readonly IOutputSink _sink;
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
                int queueCapacity,
                Action<Exception>? onError,
                Action<string>? onLog)
            {
                _sink = sink;
                _onError = onError;
                _onLog = onLog;

                _queue = Channel.CreateBounded<LogEntry>(
                    new BoundedChannelOptions(Math.Max(1, queueCapacity))
                    {
                        SingleReader = true,
                        SingleWriter = false,
                        FullMode = BoundedChannelFullMode.DropWrite
                    },
                    _ => OnDropped());

                Completion = RunAsync();
            }

            public Task Completion { get; }

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
                var batch = new List<LogEntry>(BULK_INSERT_COUNT);

                try
                {
                    while (await _queue.Reader
                        .WaitToReadAsync(_cancellation.Token)
                        .ConfigureAwait(false))
                    {
                        while (batch.Count < BULK_INSERT_COUNT &&
                               _queue.Reader.TryRead(out var entry))
                        {
                            batch.Add(entry);
                        }

                        if (batch.Count < BULK_INSERT_COUNT &&
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
                            await _sink.ExportAsync(batch, _cancellation.Token)
                                .ConfigureAwait(false);
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

            private async Task FillBatchUntilDelayAsync(List<LogEntry> batch)
            {
                var delayTask = Task.Delay(BatchDelay, _cancellation.Token);

                while (batch.Count < BULK_INSERT_COUNT &&
                       !_queue.Reader.Completion.IsCompleted)
                {
                    var waitToReadTask = _queue.Reader
                        .WaitToReadAsync(_cancellation.Token)
                        .AsTask();

                    var completed = await Task
                        .WhenAny(waitToReadTask, delayTask)
                        .ConfigureAwait(false);

                    if (completed == delayTask ||
                        !await waitToReadTask.ConfigureAwait(false))
                    {
                        break;
                    }

                    while (batch.Count < BULK_INSERT_COUNT &&
                           _queue.Reader.TryRead(out var entry))
                    {
                        batch.Add(entry);
                    }
                }
            }

            private void OnDropped()
            {
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
