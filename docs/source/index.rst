=====================================
rAPId-sdk
=====================================

The rAPId-sdk is a lightweight Python wrapper for the 10 Downing Streets `rAPId api project <https://github.com/no10ds/rapid-api>`_.
rAPId aims to create a consistent, secure, interoperable data storage and sharing interfaces (APIs).

The sdk is a standalone Python library that can provide easy programmatic access to the core rAPId functionality. It can handle
programmatic schema creation and updation using modern Python classes and data structures.

Installation
============

Install the sdk easily with `pip`::

   $ pip install rapid-sdk

How to Use
==========

Once installed into your project the first thing you will want to do is create an instance of the rAPId class.

In order for your code to connect to rAPId you will need your rAPId `client_id`, `client_secret` and `url` values. By default the
authentication module will try and read these from your environment variables as `RAPID_CLIENT_ID`, `RAPID_CLIENT_SECRET` and `RAPID_URL`
respectively. Alternatively you can create your own instance of the rAPId authentication class.::

   from rapid import Rapid
   from rapid import RapidAuth

   rapid_authentication = RapidAuth()
   rapid = Rapid(auth=rapid_authentication)

If you do not want to use environment variables (however this is discouraged as secrets should always be kept safe), you can pass the
values directly to the class as follows.::

   rapid_authentication = RapidAuth(
      client_id="RAPID_CLIENT_ID",
      client_secret="RAPID_CLIENT_SECRET",
      url="RAPID_URL"
   )

API Documentation
=================

.. toctree::
   :maxdepth: 2
   :glob:

   api/rapid
   api/auth
   api/items
   api/patterns

Search
======
* :ref:`search`
