#! /usr/bin/env node
'use strict';

const SnowflakeUtils = require('../index');
const Cache = require('../lib/cache');
const cache = new Cache(process.env.SNOWFLAKE_REDISHOST, process.env.SNOWFLAKE_REDISPORT);
const snowflake = new SnowflakeUtils(process.env.SNOWFLAKE_ACCOUNT, process.env.SNOWFLAKE_USERNAME, process.env.SNOWFLAKE_PASSWORD, process.env.SNOWFLAKE_REDISHOST, process.env.SNOWFLAKE_REDISPORT);
const warehouse = 'demo_wh';

const async = require('async');

const Tests = {};

Tests.ConnectSpecificDatabase = function ConnectSpecificDatabase(test) {
    snowflake.execute(warehouse, 'select table_name from information_schema.tables', [], {database: 'demo_db'}, function (err, rows) {
        test.ifError(err);
        test.done();    
    });
};

Tests.ConnectRedisDatabase = function ConnectRedisDatabase(test) {
    const testKey = 'tests';
    const database = 'DEMO_DB';
    async.series([
        function (done) {
            cache.addValueToCache(testKey, database, done, 60);
        },
        function (done) {
            snowflake.execute(warehouse, 'select CURRENT_DATABASE() as database', [], { rediskey: testKey }, function (err, rows) {
                test.equal(rows[0].database, database, 'Database not matched');                
                done(err);                
            });
        }
    ], function (err, result) {
        test.ifError(err);
        test.done();
    });
};

Tests.SetCurrentDatabase = function SetCurrentDatabase(test) {
    const database = 'foobar';
    async.series([
        function (done) {
            snowflake.setCurrentDatabase(database, done);
        },
        function (done) {
            cache.getFromCache('production', function (err, result) {
                test.equal(result, database, 'Wrong value set');
                done(err);
            });
        }
    ], function (err, result) {
        test.ifError(err);
        test.done();
    });
};

Tests.GetCurrentDatabase = function GetCurrentDatabase(test) {
    const database = 'barfoo';
    async.series([
        function (done) {
            cache.addValueToCache('production', database, done, 60);
        },
        function (done) {
            snowflake.getCurrentDatabase(function (err, result) {
                test.equal(result, database, 'Database not matched');
                done(err);
            });
        }
    ], function (err, result) {
        test.ifError(err);
        test.done();
    });
};

module.exports = Tests;