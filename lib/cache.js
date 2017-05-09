'use strict';

const debug = require('debug')('snowflake-utils:cache');

const redis = require('redis');
const async = require('async');


const Cache = function Cache(host, port) {
    this._host = host;
    this._port = port;
    this._defaultCacheExpiration = 3600;
    this._redisClient = null;
};

Cache.prototype._getRedisClient = function _getRedisClient() {
    if (!this._redisClient) {
        this._redisClient = redis.createClient(this._port, this._host);
    }

    return this._redisClient;
};

Cache.prototype.getFromCache = function getFromCache(key, callback) {
    var redis = this._getRedisClient();
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

Cache.prototype.addValueToCache = function addValueToCache(key, value, callback, expiresInSeconds) {
    this.addToCache(key, (cb) => { return setImmediate(cb, null, value); }, callback, expiresInSeconds);
};

Cache.prototype.addToCache = function addToCache(key, getFunc, callback, expiresInSeconds) {
    var self = this;
    expiresInSeconds = expiresInSeconds || this._defaultCacheExpiration;

    getFunc(function (err, result) {
        if (err) {
            debug("Error retrieving value for cache key: " + key + ', err: ' + err);
            return callback(err);
        }
        else if (result !== undefined && result !== null) {
            // Save the value in cache
            self._getRedisClient().setex(key, expiresInSeconds, JSON.stringify(result), function (err) {
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
Cache.prototype._closeConnection = function _closeConnection() {
    this._getRedisClient().end(true);
};


module.exports = Cache;


