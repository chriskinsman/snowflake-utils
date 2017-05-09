'use strict';

const debug = require('debug')('snowflake-utils:cache');

const redis = require('redis');
const async = require('async');


module.exports = function (host, port) {
    const defaultCacheExpiration = 3600;

    let redisClient;
    function _getRedisClient() {
        if (!redisClient) {
            redisClient = redis.createClient(port, host);
        }

        return redisClient;
    }

    this.getFromCache = function (key, callback) {
        var redis = _getRedisClient();
        // Attempt to get the value
        redis.get(key, function (err, val) {
            // If we didn't get an err return the value
            if (!err && val !== null) {
                var value;
                try {
                    value = JSON.parse(val);
                } catch (e) {
                    return callback('Redis value is not valid JSON.');
                }

                return callback(null, value);
            }
            else if (err) {
                return callback(err);
            }
            else {
                return callback(null, null);
            }
        });
    };


    this.addValueToCache = function (key, value, callback, expiresInSeconds) {
        this.addToCache(key, (cb) => { return setImmediate(cb, null, value); }, callback, expiresInSeconds);
    };

    this.addToCache = function (key, getFunc, callback, expiresInSeconds) {
        expiresInSeconds = expiresInSeconds || defaultCacheExpiration;

        getFunc(function (err, result) {
            if (err) {
                debug("Error retrieving value for cache key: " + key + ', err: ' + err);
                return callback(err);
            }
            else if (result !== undefined && result !== null) {
                // Save the value in cache
                _getRedisClient().setex(key, expiresInSeconds, JSON.stringify(result), function (err) {
                    if (err) {
                        debug("redis addtocache error saving key: " + key + ", err: " + err);
                    }

                    return callback(null, result);
                });
            }
            else {
                return callback(null, null);
            }
        });
    };

    // WARNING this likely only needs to be used for testing.
    // This method is needed because of https://github.com/caolan/nodeunit/issues/330
    this._closeConnection = function () {
        _getRedisClient().end(true);
    };
};




