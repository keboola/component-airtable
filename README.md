Airtable extractor
==================

This is an extractor that allows you to extract tables from Airtable bases.

**Table of contents:**

[TOC]

<!-- Functionality notes
=================== -->

Prerequisites
=============

Create Airtable PAT token.

- You can create the PAT token in the [Airtable developer hub](https://airtable.com/account). Create read only access for following scopes: `data.records:read` and `schema.bases:read`
- For more information about PAT tokens, see [the documentation](https://support.airtable.com/docs/creating-and-using-api-keys-and-access-tokens).



Configuration
=============

1. [Create a new configuration](https://help.keboola.com/components/#creating-component-configuration) of the Airtable
   datasource.
2. In the authorization section, enter the obtained PAT token. See prerequisites section.
3. Create a new configuration row.

4. Select `Base ID`. To reload available Bases click the `RELOAD AVAILABLE BASE IDS` button.
5. Select `Table` you wish to Sync. To reload available tables for selected Base click the `RELOAD AVAILABLE TABLES`
   button.
6. Optionally, insert a custom `Filter Formula`. For syntax please refer [here](https://support.airtable.com/docs/formula-field-reference).
   1. e.g. `DATETIME_DIFF(NOW(), CREATED_TIME(), 'minutes') < 130`
7. Optionally, select subset of fields you wish to sync. If left empty, all fields are downloaded.
8. Configure `Destination` section
    1. Optionally, set the resulting `Storage Table Name`. If left empty, name of the source table will be automatically
       used.
    2. Select `Load Type`. If Full Load is used, the destination table will be overwritten every run. If Incremental
       load is used, data will be "upserted" into the destination table.



## Configuration parameters
 - Debug (debug) - [OPT] Whether you want to run the configuration in debug mode.
 - API key (#api_key) - [REQ] The API key to authenticate the connection with Airtable.
 - Base ID (base_id) - [REQ] The ID of the base you want to extract tables from.
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