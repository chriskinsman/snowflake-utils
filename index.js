'use strict';

const Cache = require('./lib/cache');

const debug = require('debug')('snowflake-utils');
const async = require('async');
const snowflake = require('snowflake-sdk');
const _production = 'snowshovel-utils:production';

let Utils =  function (account, username, password, redisHost, redisPort) {
    this._cache = new Cache(redisHost, redisPort);
    this._account = account;
    this._username = username;
    this._password = password;
};

Utils.prototype._getConnection = function _getConnection(warehouse, options, callback) {
    const connectionInfo = {
        account: this._account,
        username: this._username,
        password: this._password,
        warehouse: warehouse,
        schema: options.schema || 'PUBLIC'
    };

    const self = this;    
    async.series([
        function (done) {
            if (options.database) {
                connectionInfo.database = options.database;
                return setImmediate(done);
            }
            else {
                self._getCurrentDatabase(options.rediskey, function (err, databaseName) {
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
            const connection = snowflake.createConnection(connectionInfo);

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
};

Utils.prototype._getCurrentDatabase = function _getCurrentDatabase(key, callback) {
    this._cache.getFromCache(key, function (err, databaseName) {
        if (!err && databaseName) {
            return callback(null, databaseName);
        }
        else {
            return callback(err || 'No database name found');
        }
    });
};

Utils.prototype._lowerCaseObjectKeys =  function _lowerCaseObjectKeys(source) {
    let key, keys = Object.keys(source);
    let n = keys.length;
    let target = {};
    while (n--) {
        key = keys[n];
        target[key.toLowerCase()] = source[key];
    }

    return target;
};

Utils.prototype._executeStatement = function _executeStatement(connection, query, params, callback) {
    const statement = connection.execute({
        sqlText: query,
        binds: params
    });

    let rows = [];
    const self = this;
    statement.streamRows()
        .on('error', function (err) {
            return callback(err);
        })
        .on('data', function (row) {
            // Snowflake returns all columns as upper case
            // which breaks downstream code that relies upon
            // everything all lower case
            rows.push(self._lowerCaseObjectKeys(row));
        })
        .on('end', function () {
            //logger.profile(query);
            return callback(null, rows);
        });

};

Utils.prototype.execute = function (warehouse, query, params, options, callback) {    
    if (typeof options === 'function') {
        callback = options;
        options = {};
    }
    const self = this;

    // Default to third party
    options.rediskey = options.rediskey || _production;
    self._getConnection(warehouse, options, function (err, connection) {
        if (err) {
            debug("execute:connection err: " + err);
            return callback(err);
        }
        else {
            self._executeStatement(connection, query, params, callback);
        }
    });
};    

Utils.prototype.setCurrentDatabase = function setCurrentDatabase(database, callback) {
    this._cache.addValueToCache(_production, database, callback, 60);
};

Utils.prototype.getCurrentDatabase = function getCurrentDatabase(callback) {
    this._getCurrentDatabase(_production, callback);
};

module.exports = Utils;
