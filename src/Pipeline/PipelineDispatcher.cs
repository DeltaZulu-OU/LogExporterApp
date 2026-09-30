/*
Technitium DNS Server
Copyright (C) 2025  Shreyas Zare
Copyright (C) 2025  Zafer Balkan

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

namespace LogExporter.Pipeline
{
    /// <summary>
    /// <para>Dispatches pipeline actions to all configured IPipelineProcessor strategies.</para>
    /// <para>
    /// ADR: Meta is synchronous and in-process, so this dispatcher
    /// executes strategies sequentially to keep ordering deterministic.
    /// Each processor is isolated with its own exception boundary so that
    /// one faulty processor cannot break the pipeline.
    /// </para>
    /// </summary>
    public sealed class PipelineDispatcher : IDisposable
    {
        #region variables

        private readonly Lock _sync = new Lock();
        private readonly Dictionary<Type, IPipelineProcessor> _processors =
            new Dictionary<Type, IPipelineProcessor>();

        private bool _disposed;

        #endregion

        #region IDisposable

        public void Dispose()
        {
            lock (_sync)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;

                foreach (var enricher in _processors.Values)
                {
                    try
                    {
                        enricher.Dispose();
                    }
                    catch
                    {
                        // At this point we cannot rely on any logging infrastructure.
                        // Best-effort only: swallow to avoid secondary failures.
                    }
                }

                _processors.Clear();
            }
        }

        #endregion

        #region public

        public void Add(IPipelineProcessor processor)
        {
            ArgumentNullException.ThrowIfNull(processor);

            lock (_sync)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);

                if (_processors.Remove(processor.GetType(), out var existing))
                {
                    try
                    {
                        existing.Dispose();
                    }
                    catch
                    {
                        // Ignore disposal failure; new instance still becomes active.
                    }
                }

                _processors.Add(processor.GetType(), processor);
            }
        }

        public void Remove(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            lock (_sync)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);

                if (_processors.Remove(type, out var existing))
                {
                    try
                    {
                        existing.Dispose();
                    }
                    catch
                    {
                        // Isolation: disposal of one processor must not affect others.
                    }
                }
            }
        }

        public bool Any()
        {
            lock (_sync)
            {
                return !_disposed && _processors.Count > 0;
            }
        }

        /// <summary>
        /// Runs all configured enrichment strategies on a single log entry.
        /// Errors are reported to the optional error callback but never thrown.
        /// </summary>
        public void Run(LogEntry logEntry, Action<Exception>? onError = null)
        {
            if (logEntry == null)
            {
                return;
            }

            lock (_sync)
            {
                if (_disposed || _processors.Count == 0)
                {
                    return;
                }

                foreach (var processor in _processors.Values)
                {
                    try
                    {
                        processor.Process(logEntry);
                    }
                    catch (Exception ex)
                    {
                        onError?.Invoke(ex);
                    }
                }
            }
        }

        #endregion
    }
}
