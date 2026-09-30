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

        private bool _disposed;

        #endregion

        #region IDisposable

        public void Dispose()
        {
            List<SinkWorker> workers;

            lock (_sync)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
                workers = new List<SinkWorker>(_workers.Values);
                _workers.Clear();
            }

            Exception? failure = null;

            foreach (var worker in workers)
            {
                try
                {
                    worker.Dispose();
                }
                catch (Exception ex)
                {
                    failure ??= ex;
                }
            }

            if (failure is not null)
            {
                throw failure;
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

            List<SinkWorker> workers;

            lock (_sync)
            {
                if (_disposed || _workers.Count == 0)
                {
                    return Task.CompletedTask;
                }

                workers = new List<SinkWorker>(_workers.Values);
            }

            foreach (var worker in workers)
            {
                worker.Enqueue(logs);
            }

            return Task.CompletedTask;
        }

        public async Task DrainAsync()
        {
            List<SinkWorker> workers;

            lock (_sync)
            {
                if (_workers.Count == 0)
                {
                    return;
                }

                workers = new List<SinkWorker>(_workers.Values);
            }

            var tasks = new Task[workers.Count];

            for (var i = 0; i < workers.Count; i++)
            {
                workers[i].Complete();
                tasks[i] = workers[i].Completion;
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        #endregion

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

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) != 0)
                {
                    return;
                }

                Complete();

                try
                {
                    Completion.GetAwaiter().GetResult();
                }
                catch (OperationCanceledException) when (_cancellation.IsCancellationRequested)
                {
                }
                finally
                {
                    _cancellation.Cancel();
                    _cancellation.Dispose();
                    _sink.Dispose();
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
                            await Task.Delay(BatchDelay, _cancellation.Token)
                                .ConfigureAwait(false);

                            while (batch.Count < BULK_INSERT_COUNT &&
                                   _queue.Reader.TryRead(out var nextEntry))
                            {
                                batch.Add(nextEntry);
                            }
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
                }
                catch (Exception ex)
                {
                    ReportError(ex);
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
