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

using Nager.PublicSuffix;
using Nager.PublicSuffix.RuleProviders;
using Nager.PublicSuffix.RuleProviders.CacheProviders;
using System;
using System.Collections.Concurrent;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace LogExporter.Pipeline
{

    public partial class Normalize
    {
        /// <summary>
        /// <para>
        /// Thread-safe cache for parsed domain information using the SIEVE eviction algorithm.
        /// SIEVE provides better scan resistance than LRU, making it ideal for DNS workloads
        /// where one-time queries (typos, DGA domains) are common.
        /// </para>
        /// <para>
        /// Reference: "SIEVE is Simpler than LRU: an Efficient Turn-Key Eviction Algorithm for
        /// Web Caches" (NSDI '24)
        /// </para>
        /// </summary>
        internal sealed class DomainCache
        {
            #region variables

            private const int MaxSize = 10000;
            private const int StringPoolMaxSize = 10000;
            private static readonly DomainInfo Empty = new DomainInfo();

            // ADR: PSL loading is best-effort and must never block the enrichment consumer.
            private static readonly HttpClient _pslHttpClient = new HttpClient();
            private static readonly Task<DomainParser?> _parserTask = InitializeParserAsync();

            private readonly ConcurrentDictionary<string, CacheNode> _cache =
                new ConcurrentDictionary<string, CacheNode>(StringComparer.OrdinalIgnoreCase);
            private readonly ConcurrentDictionary<string, string> _stringPool =
                new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            private readonly Lock _evictionLock = new Lock();

            // SIEVE data structures
            private CacheNode? _head;
            private CacheNode? _tail;
            private CacheNode? _hand;
            #endregion

            #region public
            public DomainInfo GetOrAdd(string domainName)
            {
                if (string.IsNullOrWhiteSpace(domainName))
                {
                    return Empty;
                }

                // Fast path: try cache lookup with original name first (case-insensitive)
                if (_cache.TryGetValue(domainName, out var node))
                {
                    node.Visited = true;
                    return node.Domain;
                }

                // NormalizeConfig only if needed, using string pool to reduce allocations
                var normalizedName = GetPooledNormalizedName(domainName);

                // Check cache again with normalized name (may differ from original)
                if (!ReferenceEquals(normalizedName, domainName) &&
                    _cache.TryGetValue(normalizedName, out node))
                {
                    node.Visited = true;
                    return node.Domain;
                }

                if (!TryParse(domainName, out var domain))
                {
                    return Empty;
                }

                AddToCache(normalizedName, domain);
                return domain;
            }


            public void Clear()
            {
                lock (_evictionLock)
                {
                    _cache.Clear();
                    _stringPool.Clear();
                    _head = null;
                    _tail = null;
                    _hand = null;
                }
            }

            #endregion

            #region private

            /// <summary>
            /// Returns a pooled, normalized version of the domain name to reduce allocations.
            /// If the name is already normalized, returns the original string.
            /// </summary>
            private string GetPooledNormalizedName(string name)
            {
                if (!NeedsNormalization(name))
                {
                    return name;
                }

                var normalized = name.ToLowerInvariant().TrimEnd('.');

                // Try to get from pool, or add if not present
                if (_stringPool.TryGetValue(normalized, out var pooled))
                {
                    return pooled;
                }

                // Limit pool size to prevent unbounded growth
                if (_stringPool.Count < StringPoolMaxSize)
                {
                    _stringPool.TryAdd(normalized, normalized);
                }

                return normalized;
            }

            /// <summary>
            /// Checks if the domain name needs normalization (has uppercase or trailing dot).
            /// </summary>
            private static bool NeedsNormalization(string name)
            {
                if (name.Length > 0 && name[^1] == '.')
                {
                    return true;
                }

                foreach (var c in name)
                {
                    if (c >= 'A' && c <= 'Z')
                    {
                        return true;
                    }
                }

                return false;
            }

            private static bool TryParse(string name, out DomainInfo domain)
            {
                if (!_parserTask.IsCompletedSuccessfully)
                {
                    domain = Empty;
                    return false;
                }

                var parser = _parserTask.Result;
                if (parser == null)
                {
                    domain = Empty;
                    return true;
                }

                try
                {
                    domain = parser.Parse(name) ?? Empty;
                }
                catch
                {
                    // Parsing errors are intentionally ignored because PSL is optional.
                    domain = Empty;
                }

                return true;
            }

            private static async Task<DomainParser?> InitializeParserAsync()
            {
                try
                {
                    var cacheProvider = new LocalFileSystemCacheProvider();
                    var ruleProvider = new CachedHttpRuleProvider(cacheProvider, _pslHttpClient);
                    await ruleProvider.BuildAsync().ConfigureAwait(false);
                    return new DomainParser(ruleProvider);
                }
                catch
                {
                    return null;
                }
            }

            private void AddToCache(string key, DomainInfo domain)
            {
                lock (_evictionLock)
                {
                    if (_cache.ContainsKey(key))
                    {
                        return;
                    }

                    while (_cache.Count >= MaxSize)
                    {
                        Evict();
                    }

                    var newNode = new CacheNode(key, domain);
                    InsertAtHead(newNode);
                    _cache[key] = newNode;
                }
            }

            private void InsertAtHead(CacheNode node)
            {
                node.Next = _head;
                node.Prev = null;

                _head?.Prev = node;

                _head = node;

                _tail ??= node;

                _hand ??= node;
            }

            private void Evict()
            {
                _hand ??= _tail;

                while (_hand != null)
                {
                    if (!_hand.Visited)
                    {
                        var victim = _hand;
                        _hand = _hand.Prev ?? _tail;
                        RemoveNode(victim);
                        _cache.TryRemove(victim.Key, out _);
                        return;
                    }

                    _hand.Visited = false;
                    _hand = _hand.Prev ?? _tail;
                }
            }

            private void RemoveNode(CacheNode node)
            {
                if (node.Prev != null)
                {
                    node.Prev.Next = node.Next;
                }
                else
                {
                    _head = node.Next;
                }

                if (node.Next != null)
                {
                    node.Next.Prev = node.Prev;
                }
                else
                {
                    _tail = node.Prev;
                }

                if (_hand == node)
                {
                    _hand = node.Prev ?? _tail;
                }
            }

            private class CacheNode
            {
                public readonly string Key;
                public readonly DomainInfo Domain;
                public volatile bool Visited;
                public CacheNode? Next;
                public CacheNode? Prev;

                public CacheNode(string key, DomainInfo domain)
                {
                    Key = key;
                    Domain = domain;
                }
            }
            #endregion
        }
    }

}