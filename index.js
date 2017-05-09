'use strict';

const Cache = require('./lib/cache');

const debug = require('debug')('snowflake-utils');
const async = require('async');
const snowflake = require('snowflake-sdk');

module.exports = function (account, username, password, redisHost, redisPort) {
    const _cache = new Cache(redisHost, redisPort);
    const _account = account;
    const _username = username;
    const _password = password;
    const _self = this;
    const _production = 'production';

    function _getConnection(warehouse, options, callback) {
        var connectionInfo = {
            account: _account,
            username: _username,
            password: _password,
            warehouse: warehouse,
            schema: options.schema || 'PUBLIC' 
        };

        async.series([
            function (done) {
                if (options.database) {
                    connectionInfo.database = options.database;
                    return setImmediate(done);
                }
                else {
                    _getCurrentDatabase(options.rediskey, function (err, databaseName) {
                        if (!err && databaseName) {
                            connectionInfo.database = databaseName;
                            return done();
                        }
                        else {
                            return done(err);
                        }
                    });
                }
            },
            function (done) {
                var connection = snowflake.createConnection(connectionInfo);

                async.retry(connection.connect.bind(connection), function (err, conn) {
                    if (err) {
                        debug('Unable to connect: ' + err.message);
                        done(err);
                    } else {
                        done(null, conn);
                    }
                });
            }
        ], function (err, results) {
            if (err) {
                return callback(err);
            }
            else {
                return callback(null, results[1]);
            }
        });
    }

    function _getCurrentDatabase(key, callback) {
        _cache.getFromCache(key, function (err, databaseName) {
            if (!err && databaseName) {
                return callback(null, databaseName);
            }
            else {
                return callback(err || 'No database name found');
            }
        });
    }

    function _lowerCaseObjectKeys(source) {
        var key, keys = Object.keys(source);
        var n = keys.length;
        var target = {};
        while (n--) {
            key = keys[n];
            target[key.toLowerCase()] = source[key];
        }

        return target;
    }

    function _executeStatement(connection, query, params, callback) {
        var statement = connection.execute({
            sqlText: query,
            binds: params
        });

        var rows = [];
        statement.streamRows()
            .on('error', function (err) {
                return callback(err);
            })
            .on('data', function (row) {
                // Snowflake returns all columns as upper case
                // which breaks downstream code that relies upon
                // everything all lower case
                rows.push(_lowerCaseObjectKeys(row));
            })
            .on('end', function () {
                //logger.profile(query);
                return callback(null, rows);
            });

    }

    _self.execute = function (warehouse, query, params, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = {};
        }

        // Default to third party
        options.rediskey = options.rediskey || _production;
        _getConnection(warehouse, options, function (err, connection) {
            if (err) {
                debug("execute:connection err: " + err);
                return callback(err);
            }
            else {
                _executeStatement(connection, query, params, callback);
            }
        });
    };    

    _self.setCurrentDatabase = function setCurrentDatabase(database, callback) {
        _cache.addValueToCache(_production, database, callback, 60);  
    };

    _self.getCurrentDatabase = function getCurrentDatabase(callback) {
        _getCurrentDatabase(_production, callback);
    };
};



