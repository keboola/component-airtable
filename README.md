Airtable extractor
==================

First draft of Airtable extractor.

**Table of contents:**

[TOC]

Functionality notes
===================

Prerequisites
=============

Get the API token, register application, etc.


Supported endpoints
===================

If you need more endpoints, please submit your request to
[ideas.keboola.com](https://ideas.keboola.com/)

Configuration
=============

##Configuration Schema
 - Debug (debug) - [OPT] 
 - API key (#api_key) - [REQ] 
 - Base ID (base_id) - [REQ] 


##Configuration Schema
 - Table name (table_name) - [REQ] 
 - Filter by formula (filter_by_formula) - [OPT] 
 - Fields (fields) - [OPT] 
 - Incremental loading (incremental_loading) - [OPT] 


Sample Configuration
=============
```json
{
    "parameters": {
        "debug": true,
        "#api_key": "SECRET_VALUE",
        "base_id": "appxDZ88j6DBq80NU",
        "table_name": "Order items",
        "filter_by_formula": "DATETIME_DIFF(NOW(), CREATED_TIME(), 'minutes') < 130",
        "fields": [
            "Order",
            "Product",
            "Amount"
        ],
        "incremental_loading": true
    }
}
```

Output
======

List of tables, foreign keys, schema.

Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path in
the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/)