Airtable extractor
==================

This is an extractor that allows you to extract tables from Airtable bases.

**Table of contents:**

[TOC]

<!-- Functionality notes
=================== -->

Prerequisites
=============

You must know your Airtable API key and ID of the base.

- You can obtain the API key from your [Airtable account overview page](https://airtable.com/account). Since read only access is sufficient, we recommend you create a read only account and provide its API key. For instructions how to do that, see the [this Airtable support article](https://support.airtable.com/hc/en-us/articles/360056249614).
- To obtain the base ID, open your base API documentation from the [Airtable API documentation list](https://airtable.com/api) and find the base ID in your base's API documentation.

You must also know the names of the tables in your Airtable base you want to extract. If you want to use filtering or download just a subset of the given table's fields (columns), you also need to provide the filter [formula](https://support.airtable.com/hc/en-us/articles/203255215-Formula-Field-Reference) and/or the names of the fields (columns) you want to download respectively.


<!-- Supported endpoints
===================

If you need more endpoints, please submit your request to
[ideas.keboola.com](https://ideas.keboola.com/) -->

Configuration
=============

## Configuration schema
 - Debug (debug) - [OPT] Whether you want to run the configuration in debug mode.
 - API key (#api_key) - [REQ] The API key to authenticate the connection with Airtable.
 - Base ID (base_id) - [REQ] The ID of the base you want to extract tables from.


## Configuration row schema
 - Table name (table_name) - [REQ] The name of the table in Airtable base you want to download.
 - Filter by formula (filter_by_formula) - [OPT] A predicate (expression that evaluates to true or false) [Airtable field formula](https://support.airtable.com/hc/en-us/articles/203255215-Formula-Field-Reference).
 - Fields (fields) - [OPT] The fields you want to download. You may leave this empty to download all fields.
 - Incremental loading (incremental_loading) - [OPT] Whether incremental loading should be used.


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
<!-- List of tables, foreign keys, schema. -->
Output for each configuration row will consist of the main table being extracted as well as child tables created from certain fields of the main table.

- If a field's JSON representation consists of a list of objects, it will be omitted from the main table and instead a row for each such object will be created in a child table named `{table_name}__{field_name}`.
- If a field's JSON representation consists of a list of simple values, it will be represented as a JSON string in the output table.
- If a field's JSON representation consists of an object, it will be flattened into its table as columns named `{table_field_name}_{object_key}`.

Development
===========

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