"""
Backend for django cache
"""
import logging
import socket
from functools import wraps

from django.core.cache import InvalidCacheBackendError
from django.core.cache.backends.memcached import BaseMemcachedCache
from djpymemcache import client as djpymemcache_client

from .client import ConfigurationEndpointClient

logger = logging.getLogger(__name__)


def invalidate_cache_after_error(f):
    """
    Catch any exception and invalidate internal cache with list of nodes
    """
    @wraps(f)
    def wrapper(self, *args, **kwds):
        try:
            return f(self, *args, **kwds)
        except Exception:
            self.clear_cluster_nodes_cache()
            raise
    return wrapper


class ElastiPymemcache(BaseMemcachedCache):
    """
    Backend for Amazon ElastiCache (memcached) with auto discovery mode
    it used pymemcache
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

    @invalidate_cache_after_error
    def add(self, *args, **kwargs):
        return super().add(*args, **kwargs)

    @invalidate_cache_after_error
    def get(self, *args, **kwargs):
        return super().get(*args, **kwargs)

    @invalidate_cache_after_error
    def set(self, *args, **kwargs):
        return super().set(*args, **kwargs)

    @invalidate_cache_after_error
    def delete(self, *args, **kwargs):
        return super().delete(*args, **kwargs)

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
