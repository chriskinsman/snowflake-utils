  
  A collection of utilties to use with the snowflake database.

## Installation

```bash
npm install snowflake-utils --save
```

## Features

  * Wrapper method to execute SQL query and return results with lower cased column names
  * Ability to grab 'production' database from REDIS
  * Ability to set 'production' database to REDIS

## Background

We are users of snowflake and currently use a model where we build a new data warehouse each night and then transparently replace the previous nights warehouse by flipping a REDIS key.  These utilities make this use case very simple.

## People

The author is [Chris Kinsman](https://github.com/chriskinsman)

## License

  [MIT](LICENSE)

