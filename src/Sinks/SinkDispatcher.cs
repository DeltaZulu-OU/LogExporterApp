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
using System.Threading.Tasks;

namespace LogExporter.Sinks
{
    public sealed class SinkDispatcher : IDisposable
    {
        #region variables

        private readonly object _sync = new object();
        private readonly Dictionary<Type, IOutputSink> _sinks =
            new Dictionary<Type, IOutputSink>();
        private readonly List<IOutputSink> _retiredSinks =
            new List<IOutputSink>();

        private int _activeDispatches;
        private bool _disposed;

        #endregion

        #region IDisposable

        public void Dispose()
        {
            List<IOutputSink>? disposeNow = null;

            lock (_sync)
            {
                if (_disposed)
                    return;

                _disposed = true;

                foreach (IOutputSink sink in _sinks.Values)
                    _retiredSinks.Add(sink);

                _sinks.Clear();

                if (_activeDispatches == 0 && _retiredSinks.Count > 0)
                {
                    disposeNow = new List<IOutputSink>(_retiredSinks);
                    _retiredSinks.Clear();
                }
            }

            DisposeSinks(disposeNow);
        }

        #endregion

        #region public

        public void Add(IOutputSink sink)
        {
            ArgumentNullException.ThrowIfNull(sink);

            lock (_sync)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);

                if (!_sinks.TryAdd(sink.GetType(), sink))
                    throw new InvalidOperationException(
                        $"Strategy of type {sink.GetType().Name} already registered.");
            }
        }

        public void Remove(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            IOutputSink? disposeNow = null;

            lock (_sync)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);

                if (_sinks.Remove(type, out IOutputSink? existing))
                {
                    if (_activeDispatches == 0)
                        disposeNow = existing;
                    else
                        _retiredSinks.Add(existing);
                }
            }

            disposeNow?.Dispose();
        }

        public bool Any()
        {
            lock (_sync)
            {
                return !_disposed && _sinks.Count > 0;
            }
        }

        /// <summary>
        /// Executes all configured export strategies for the current batch.
        ///
        /// ADR: ExportManager synchronously awaits each strategy's ExportAsync task.
        /// This guarantees predictable backpressure and ensures no spillover work
        /// continues after shutdown. Strategies are responsible for honoring
        /// cancellation so shutdown stays bounded.
        /// </summary>
        public async Task DispatchAsync(IReadOnlyList<LogEntry> logs, CancellationToken token)
        {
            if (logs == null || logs.Count == 0 || token.IsCancellationRequested)
                return;

            List<IOutputSink> snapshot;

            lock (_sync)
            {
                if (_disposed || _sinks.Count == 0)
                    return;

                snapshot = new List<IOutputSink>(_sinks.Values);
                _activeDispatches++;
            }

            List<IOutputSink>? disposeNow = null;

            try
            {
                List<Task> tasks = new List<Task>(snapshot.Count);

                foreach (IOutputSink sink in snapshot)
                    tasks.Add(sink.ExportAsync(logs, token));

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            finally
            {
                lock (_sync)
                {
                    _activeDispatches--;

                    if (_activeDispatches == 0 && _retiredSinks.Count > 0)
                    {
                        disposeNow = new List<IOutputSink>(_retiredSinks);
                        _retiredSinks.Clear();
                    }
                }

                DisposeSinks(disposeNow);
            }
        }

        private static void DisposeSinks(List<IOutputSink>? sinks)
        {
            if (sinks is null)
                return;

            foreach (IOutputSink sink in sinks)
                sink.Dispose();
        }

        #endregion
    }
}
