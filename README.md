Linkhouse
=========

Automatically create nodes and relationships based on company numbers within a [Neo4j] (http://neo4j.com/) database using [Companies House data] (https://developer.companieshouse.gov.uk/api/docs/).

When running, it looks for all nodes that have a property named 'companyNumber' -- each one of those numbers is looked up using the Companies House API. New nodes are then created for each officer (both secretaries and directors) if they don't already exist, and relationships created connecting those nodes back to the original one holding the company number.

Requires a recent version of [Node JS] (http://nodejs.org/).

Uses a configuration file `config.json` -- an example of what fields are required is in `config.example.json`.

Install the dependencies with `npm install`, then run `node linkhouse`.
