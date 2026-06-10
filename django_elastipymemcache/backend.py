"""
Backend for django cache — with transparent chunking for large values.

Values over CHUNK_THRESHOLD are automatically split into multiple
Memcached keys and reassembled on read. No compression.
No changes needed in any service code.
"""
import logging
import math
import pickle
import socket
import time
from functools import wraps

from django.core.cache import InvalidCacheBackendError
from django.core.cache.backends.memcached import BaseMemcachedCache
from djpymemcache import client as djpymemcache_client

from .client import ConfigurationEndpointClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Chunking configuration
# ---------------------------------------------------------------------------
CHUNK_THRESHOLD = 100 * 1024        # 100 KB — values above this get chunked
CHUNK_SIZE = 100 * 1024             # 100 KB — max size per chunk
MAX_ALLOWED_SIZE = 5 * 1024 * 1024  # 5 MB — hard reject above this
CHUNK_KEY_SEPARATOR = "::chunk::"
CHUNKED_MARKER = "__chunked__"

# Set to False in production once timing data is collected
LOG_TIMING = True


def invalidate_cache_after_error(f):
    """
    Catch any exception and invalidate internal cache with list of nodes
    """
    @wraps(f)
    def wrapper(self, *args, **kwds):
        try:
            return f(self, *args, **kwds)
        except Exception as e:
            logger.warning('MemcachedError: %s', e, exc_info=True)
            self.clear_cluster_nodes_cache()
            return None  # Treat as a cache miss
    return wrapper


class ElastiPymemcache(BaseMemcachedCache):
    """
    Backend for Amazon ElastiCache (memcached) with auto discovery mode
    it used pymemcache.

    Includes transparent chunking: values over CHUNK_THRESHOLD are split
    into multiple keys and reassembled on read. No compression.
    """
    def __init__(self, server, params):
        params['OPTIONS'] = params.get('OPTIONS', {})
        params['OPTIONS'].setdefault('ignore_exc', True)

        self._cluster_timeout = params['OPTIONS'].pop(
            'cluster_timeout',
            socket._GLOBAL_DEFAULT_TIMEOUT,
        )
        self._ignore_cluster_errors = params['OPTIONS'].pop(
            'ignore_cluster_errors',
            False,
        )

        super().__init__(
            server,
            params,
            library=djpymemcache_client,
            value_not_found_exception=ValueError,
        )

        if len(self._servers) > 1:
            raise InvalidCacheBackendError(
                'ElastiCache should be configured with only one server '
                '(Configuration Endpoint)',
            )
        try:
            host, port = self._servers[0].split(':')
            port = int(port)
        except ValueError:
            raise InvalidCacheBackendError(
                'Server configuration should be in format IP:Port',
            )

        self.configuration_endpoint_client = ConfigurationEndpointClient(
            (host, port),
            ignore_cluster_errors=self._ignore_cluster_errors,
            **self._options,
        )

    def clear_cluster_nodes_cache(self):
        """Clear internal cache with list of nodes in cluster"""
        if hasattr(self, '_client'):
            del self._client

    def get_cluster_nodes(self):
        try:
            return self.configuration_endpoint_client \
                .get_cluster_info()['nodes']
        except (
            OSError,
            socket.gaierror,
            socket.timeout,
        ) as e:
            logger.warn(
                'Cannot connect to cluster %s, err: %s',
                self.configuration_endpoint_client.server,
                e,
            )
            return []

    @property
    def _cache(self):
        if getattr(self, '_client', None) is None:
            self._client = self._lib.Client(
                self.get_cluster_nodes(),
                **self._options,
            )
        return self._client

    # ==================================================================
    # Chunking helpers
    # ==================================================================

    def _is_manifest(self, value):
        return isinstance(value, dict) and value.get(CHUNKED_MARKER) is True

    def _chunk_key(self, parent_key, index):
        return "{}{}{}".format(parent_key, CHUNK_KEY_SEPARATOR, index)

    # ==================================================================
    # GET — with chunk reassembly
    # ==================================================================

    @invalidate_cache_after_error
    def get(self, key, default=None, version=None):
        t_start = time.perf_counter()

        value = super().get(key, default=default, version=version)

        if value is None or value is default:
            if LOG_TIMING:
                t_total = (time.perf_counter() - t_start) * 1000
                logger.info(
                    "[TIMING] GET MISS key='%s' total=%.3fms",
                    key, t_total,
                )
            return default

        # Check if this is a chunked manifest
        if self._is_manifest(value):
            result = self._get_chunked(key, value, version)

            if LOG_TIMING:
                t_total = (time.perf_counter() - t_start) * 1000
                logger.info(
                    "[TIMING] GET HIT (chunked) key='%s' chunks=%d "
                    "original_size=%d total=%.3fms",
                    key, value.get("num_chunks", 0),
                    value.get("original_size", 0), t_total,
                )

            return result if result is not None else default
 
        # Normal value — return as-is
        if LOG_TIMING:
            t_total = (time.perf_counter() - t_start) * 1000
            logger.info(
                "[TIMING] GET HIT (normal) key='%s' total=%.3fms",
                key, t_total,
            )
        return value

    def _get_chunked(self, parent_key, manifest, version=None):
        num_chunks = manifest["num_chunks"]
        chunk_keys = [self._chunk_key(parent_key, i) for i in range(num_chunks)]

        try:
            chunk_data = super().get_many(chunk_keys, version=version)
        except Exception as e:
            logger.error("cache.get_many failed for key '%s': %s", parent_key, e)
            return None

        if not chunk_data or len(chunk_data) != num_chunks:
            missing = [k for k in chunk_keys if k not in (chunk_data or {})]
            logger.warning(
                "Chunked cache miss: %d/%d chunks missing for key '%s'. Missing: %s",
                len(missing), num_chunks, parent_key, missing[:3],
            )
            self._delete_chunked(parent_key, num_chunks, version)
            return None

        # Reassemble bytes in order
        assembled = b"".join(chunk_data[k] for k in chunk_keys)

        # Unpickle back to original Python object
        try:
            return pickle.loads(assembled)
        except Exception as e:
            logger.error("Unpickle failed for key '%s': %s", parent_key, e)
            self._delete_chunked(parent_key, num_chunks, version)
            return None

    # ==================================================================
    # SET — with chunking for large values
    # ==================================================================

    @invalidate_cache_after_error
    def set(self, key, value, timeout=None, version=None):
        t_start = time.perf_counter()

        # Pickle the value to measure its size
        try:
            pickled = pickle.dumps(value)
        except Exception as e:
            logger.error("Failed to pickle value for key '%s': %s", key, e)
            return None

        pickled_size = len(pickled)

        # Hard reject if way too large
        if pickled_size > MAX_ALLOWED_SIZE:
            logger.error(
                "Cache write REJECTED: key '%s' is %d bytes (%.1f KB). Max allowed: %d bytes.",
                key, pickled_size, pickled_size / 1024, MAX_ALLOWED_SIZE,
            )
            return None

        # Small value — write normally (no chunking)
        if pickled_size <= CHUNK_THRESHOLD:
            result = super().set(key, value, timeout, version)

            if LOG_TIMING:
                t_total = (time.perf_counter() - t_start) * 1000
                logger.info(
                    "[TIMING] SET (normal) key='%s' pickled_size=%d (%.1f KB) total=%.3fms",
                    key, pickled_size, pickled_size / 1024, t_total,
                )
            return result

        # Large value — chunk and store
        result = self._set_chunked(key, pickled, pickled_size, timeout, version)

        if LOG_TIMING:
            t_total = (time.perf_counter() - t_start) * 1000
            logger.info(
                "[TIMING] SET (chunked) key='%s' pickled_size=%d (%.1f KB) total=%.3fms",
                key, pickled_size, pickled_size / 1024, t_total,
            )
        return result

    def _set_chunked(self, key, pickled_bytes, pickled_size, timeout, version):
        num_chunks = math.ceil(pickled_size / CHUNK_SIZE)

        manifest = {
            CHUNKED_MARKER: True,
            "num_chunks": num_chunks,
            "original_size": pickled_size,
        }

        # Build chunks
        chunk_dict = {}
        for i in range(num_chunks):
            start = i * CHUNK_SIZE
            end = start + CHUNK_SIZE
            chunk_dict[self._chunk_key(key, i)] = pickled_bytes[start:end]

        # Store all chunks
        try:
            failed = super().set_many(chunk_dict, timeout, version)
        except Exception as e:
            logger.error("cache.set_many failed for key '%s': %s", key, e)
            return None

        if failed:
            logger.error(
                "Failed to store %d/%d chunks for key '%s'.",
                len(failed), num_chunks, key,
            )
            # Clean up successfully stored chunks
            successful = [k for k in chunk_dict if k not in failed]
            if successful:
                try:
                    super().delete_many(successful, version)
                except Exception:
                    pass
            return None

        # Store manifest at the original key
        try:
            super().set(key, manifest, timeout, version)
        except Exception as e:
            logger.error("Failed to store manifest for key '%s': %s", key, e)
            # Clean up all chunks
            try:
                super().delete_many(list(chunk_dict.keys()), version)
            except Exception:
                pass
            return None

        logger.info(
            "Chunked write: key '%s' → %d chunks, %d bytes (%.1f KB).",
            key, num_chunks, pickled_size, pickled_size / 1024,
        )
        return True

    # ==================================================================
    # DELETE — with chunk cleanup
    # ==================================================================

    @invalidate_cache_after_error
    def delete(self, key, version=None):
        try:
            value = super().get(key, version=version)
        except Exception:
            value = None

        if value is not None and self._is_manifest(value):
            if LOG_TIMING:
                logger.info(
                    "[TIMING] DELETE (chunked) key='%s' chunks=%d",
                    key, value.get("num_chunks", 0),
                )
            self._delete_chunked(key, value["num_chunks"], version)
        else:
            if LOG_TIMING:
                logger.info("[TIMING] DELETE (normal) key='%s'", key)
            super().delete(key, version)

    def _delete_chunked(self, parent_key, num_chunks, version=None):
        keys_to_delete = [parent_key] + [
            self._chunk_key(parent_key, i) for i in range(num_chunks)
        ]
        try:
            super().delete_many(keys_to_delete, version)
        except Exception as e:
            logger.error("Chunked delete failed for key '%s': %s", parent_key, e)

    # ==================================================================
    # Other methods — unchanged
    # ==================================================================

    @invalidate_cache_after_error
    def add(self, *args, **kwargs):
        return super().add(*args, **kwargs)

    @invalidate_cache_after_error
    def get_many(self, *args, **kwargs):
        return super().get_many(*args, **kwargs)

    @invalidate_cache_after_error
    def set_many(self, *args, **kwargs):
        return super().set_many(*args, **kwargs)

    @invalidate_cache_after_error
    def delete_many(self, *args, **kwargs):
        return super().delete_many(*args, **kwargs)

    @invalidate_cache_after_error
    def incr(self, *args, **kwargs):
        return super().incr(*args, **kwargs)

    @invalidate_cache_after_error
    def decr(self, *args, **kwargs):
        return super().decr(*args, **kwargs)