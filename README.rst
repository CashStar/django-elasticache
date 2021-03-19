=======================
django-elastipymemcache
=======================

.. index: README
.. image:: https://travis-ci.org/harikitech/django-elastipymemcache.svg?branch=master
    :target: https://travis-ci.org/harikitech/django-elastipymemcache
.. image:: https://codecov.io/gh/harikitech/django-elastipymemcache/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/harikitech/django-elastipymemcache

Purpose
-------

Simple Django cache backend for Amazon ElastiCache (memcached based). It uses
`pymemcache <https://github.com/pinterest/pymemcache>`_ and sets up a connection to each
node in the cluster using
`auto discovery <http://docs.aws.amazon.com/AmazonElastiCache/latest/UserGuide/AutoDiscovery.html>`_.
Originally forked from `django-elasticache <https://github.com/gusdan/django-elasticache>`_.

Requirements
------------

* pymemcache
* Django>=2.2
* django-pymemcache>=1.0

Installation
------------

Get it from `pypi <http://pypi.python.org/pypi/django-elastipymemcache>`_::

    pip install django-elastipymemcache

Usage
-----

Your cache backend should look something like this::

    CACHES = {
        'default': {
            'BACKEND': 'django_elastipymemcache.backend.ElastiPymemcache',
            'LOCATION': '[configuration endpoint]:11211',
            'OPTIONS': {
              'ignore_exc': True, # pymemcache Client params
              'ignore_cluster_errors': True, # ignore get cluster info error
            }
        }
    }

Testing
-------

Run the tests like this::

    nosetests
