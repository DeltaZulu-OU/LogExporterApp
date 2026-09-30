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
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.IO;

namespace LogExporter.Sinks
{
    public sealed class HttpSink : IOutputSink
    {
        #region variables

        private readonly Uri _endpoint;
        private readonly HttpClient _httpClient;
        private static readonly string Hostname = Dns.GetHostName();
        private static readonly JsonSerializerOptions HttpSerializerOptions = CreateHttpSerializerOptions();

        private readonly RecyclableMemoryStreamManager _memoryManager = new();
        private bool _disposed;

        #endregion variables

        #region constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="HttpSink"/> class.
        /// </summary>
        /// <param name="endpoint">The absolute HTTP or HTTPS endpoint URL.</param>
        /// <param name="headers">Optional collection of default HTTP request headers.</param>
        /// <remarks>
        /// <see cref="Uri.TryCreate(string, UriKind, out Uri)"/> accepts schemes such as <c>ftp</c> and
        /// <c>file</c>, which <see cref="HttpClient"/> rejects only on the first export. Checking the
        /// scheme here reports the mistake when the configuration is loaded. The client is disposed
        /// before throwing because a failed constructor leaves no instance for the caller to dispose.
        /// </remarks>
        public HttpSink(string endpoint, Dictionary<string, string?>? headers = null)
        {
            if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) ||
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
                {
                    continue;
                }

                _httpClient.Dispose();
                throw new FormatException($"'{kv.Key}' is not a valid HTTP request header.");
            }
        }

        #endregion constructor

        #region IDisposable

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

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
            if (_disposed || logs == null || logs.Count == 0 || token.IsCancellationRequested)
            {
                return;
            }
#pragma warning disable RCS1261 // Resource can be disposed asynchronously
            using var ms = _memoryManager.GetStream("HttpExport-Batch");
#pragma warning restore RCS1261 // Resource can be disposed asynchronously

            // HTTP collectors need the responding cluster member in each event.
            NdjsonSerializer.WriteBatch(ms, logs, HttpSerializerOptions);

            ms.Position = 0;

            using var request = new HttpRequestMessage(HttpMethod.Post, _endpoint)
            {
                Content = new StreamContent(ms)
            };
            request.Content.Headers.Add("Content-Type", "application/x-ndjson");

            using var response = await _httpClient
                .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token)
                .ConfigureAwait(false);

            // Fail if server rejects logs
            response.EnsureSuccessStatusCode();
        }

        #endregion public

        private static JsonSerializerOptions CreateHttpSerializerOptions()
        {
            var resolver = new DefaultJsonTypeInfoResolver();
            resolver.Modifiers.Add(typeInfo =>
            {
                if (typeInfo.Type != typeof(LogEntry))
                {
                    return;
                }

                var hostname = typeInfo.CreateJsonPropertyInfo(typeof(string), "hostname");
                hostname.Get = _ => Hostname;
                typeInfo.Properties.Add(hostname);
            });

            var options = new JsonSerializerOptions(LogEntry.DnsLogSerializerOptions.Default)
            {
                TypeInfoResolver = resolver
            };

            return options;
        }
    }
}