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

using Microsoft.IO;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace LogExporter.Sinks
{
    public sealed class HttpSink : IOutputSink
    {
        #region variables

        private readonly Uri _endpoint;
        private readonly HttpClient _httpClient;
        private readonly RecyclableMemoryStreamManager _memoryManager = new();
        private bool _disposed;

        #endregion variables

        #region constructor

        /// <remarks>
        /// <see cref="Uri.TryCreate(string, UriKind, out Uri)"/> accepts schemes such as <c>ftp</c> and
        /// <c>file</c>, which <see cref="HttpClient"/> rejects only on the first export. Checking the
        /// scheme here reports the mistake when the configuration is loaded. The client is disposed
        /// before throwing because a failed constructor leaves no instance for the caller to dispose.
        /// </remarks>
        public HttpSink(string endpoint, Dictionary<string, string?>? headers = null)
        {
            if (!Uri.TryCreate(endpoint, UriKind.Absolute, out Uri? uri) ||
                (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps))
            {
                throw new ArgumentException(
                    $"'{endpoint}' is not a valid endpoint. An absolute http:// or https:// URL is required.");
            }

            _endpoint = uri;
            _httpClient = new HttpClient();

            if (headers == null)
            {
                return;
            }

            foreach (var kv in headers)
            {
                if (_httpClient.DefaultRequestHeaders.TryAddWithoutValidation(kv.Key, kv.Value))
                    continue;

                _httpClient.Dispose();
                throw new FormatException($"'{kv.Key}' is not a valid HTTP request header.");
            }
        }

        #endregion constructor

        #region IDisposable

        public void Dispose()
        {
            if (_disposed)
                return;

            _httpClient.Dispose();
            _disposed = true;
        }

        #endregion IDisposable

        #region public

        public async Task ExportAsync(IReadOnlyList<LogEntry> logs, CancellationToken token)
        {
            // ADR: Once disposed, this strategy must not attempt any I/O. The background
            // worker may still flush a few batches while shutdown is in progress. Treating
            // late calls as no-ops avoids spurious ObjectDisposedExceptions during normal
            // teardown.
            if (_disposed || logs.Count == 0 || token.IsCancellationRequested)
                return;

            using RecyclableMemoryStream ms = _memoryManager.GetStream("HttpExport-Batch");

            // Use Stream overload explicitly to avoid ambiguity
            NdjsonSerializer.WriteBatch(ms, logs);

            ms.Position = 0;

            using StreamContent content = new StreamContent(ms);
            content.Headers.Add("Content-Type", "application/x-ndjson");

            using HttpResponseMessage response = await _httpClient
                .PostAsync(_endpoint, content, token)
                .ConfigureAwait(false);

            // Fail if server rejects logs
            response.EnsureSuccessStatusCode();
        }

        #endregion public
    }
}