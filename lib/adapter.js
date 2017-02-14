
var _ = require('underscore');
var mssql = require('mssql');
var Query = require('./query');
var sql = require('./sql.js');
var utils = require('./utils');

module.exports = (function () {

    /**
     * MssqlAdapter
     *
     * @module      :: MSSQL Adapter
     * @description :: MSSQL database adapter for Sails.js
     * @docs        :: https://github.com/sergeibelov/sails-mssql
     *
     * @syncable    :: true
     * @schema      :: true
     */

    var dbs = {};

    var adapter = {

        identity: 'sails-mssql-pzversion',
        syncable: true,
        schema: true,

        registerCollection: function (collection, cb) {

            var def = _.clone(collection);
            var key = def.identity;
            var definition = def.definition || {};

            // Set a default Primary Key
            var pkName = 'id';

            // Set the Primary Key Field
            for(var attribute in definition) {

                if(!definition[attribute].hasOwnProperty('primaryKey')) continue;

                // Check if custom primaryKey value is falsy
                if(!definition[attribute].primaryKey) continue;

                // Set the pkName to the custom primaryKey value
                pkName = attribute;
            }

            // Set the primaryKey on the definition object
            def.primaryKey = pkName;

            // Store the definition for the model identity
            if(dbs[key]) return cb();
            dbs[key.toString()] = def;

            return cb();

        },

        query: function(collectionName, statement, data, cb) {

            if (_.isFunction(data)) {
                //cb = data;
                //data = null;
                return data(new Error('invalid server parameters'));
            }

            var conn = new mssql.Connection(marshalConfig(dbs[collectionName].config, data));
            conn.connect(marshalConfig(dbs[collectionName].config,data), function(err) {

                if (err) return cb(err);
                var request = new mssql.Request(conn);
                request.query(statement, function(err, recordset) {
                    cb(err, recordset);
                });
            });
        },

        teardown: function(cb) {
            cb();
        },

        describe: function(collectionName, cb) {
            var tableName = dbs[collectionName].identity;
            dbs[collectionName].schema = {};
        },

        define: function(collectionName, definition, cb) {
            cb(null,{});
        },

        drop: function(collectionName, cb) {
            cb(null,{});
        },

        create: function(collectionName, data, cb) {

            Object.keys(data).forEach(function(value) {
                data[value] = utils.prepareValue(data[value]);
            });

            var statement = sql.insertQuery(dbs[collectionName].identity, data);
            var accountid = data.accountid;
            delete data.accountid;
            var conn = new mssql.Connection(marshalConfig(dbs[collectionName].config, accountid));
            conn.connect(marshalConfig(dbs[collectionName].config), function __CREATE__(err) {

                if (err) return cb(err);

                var request = new mssql.Request(conn);
                request.query(statement, function(err, recordsets) {

                    var recordset = recordsets[0];
                    var model = data;
                    if (recordset.id) {
                        model = _.extend({}, data, {
                            id: recordset.id
                        });
                    }

                    var _query = new Query(dbs[collectionName].definition);
                    var values = _query.cast(model);
                    cb(err, values);

                });
            });
        },

        addAttribute: function (collectionName, attrName, attrDef, cb) {

            cb();

        },

        removeAttribute: function (collectionName, attrName, cb) {

            cb();

        },

        find: function(collectionName, options, cb) {

            // Check if this is an aggregate query and that there is something to return
            if(options.groupBy || options.sum || options.average || options.min || options.max) {
                if(!options.sum && !options.average && !options.min && !options.max) {
                    return cb(new Error('Cannot groupBy without a calculation'));
                }
            }

            var user = options.__user;
            if (user){
                delete options.__user;
            }
            else {
                if (options.where && options.where.__user) {
                    user = options.where.__user;
                    delete options.where.__user;
                    if (Object.keys(options.where).length==0)
                        delete(options["where"]);
                }
                else
                    return cb(new Error('invalid server parameters'));
            }
            
            var statement = sql.selectQuery(dbs[collectionName].identity, options);
            var conn = new mssql.Connection(marshalConfig(dbs[collectionName].config, user));
            conn.connect(function __FIND__(err) {

                if (err) return cb(err);

                var request = new mssql.Request(conn);
                request.query(statement, function(err, recordset) {

                    if (err) return cb(err);
                    cb(null, recordset);

                });

            });
        },

        update: function(collectionName, options, values, cb) {

            var tableName = dbs[collectionName].identity;
            var criteria = sql.serializeOptions(dbs[collectionName].identity, options);
            var pk = dbs[collectionName].primaryKey;
            var statement = 'SELECT [' + pk + '] FROM ' + tableName + ' ' + criteria;
            var accountid = values.accountid;
            delete values.accountid;
            
            var conn = new mssql.Connection(marshalConfig(dbs[collectionName].config, accountid));
            conn.connect(marshalConfig(dbs[collectionName].config), function __UPDATE__(err) {

                if (err) return cb(err);

                var request = new mssql.Request(conn);
                request.query(statement, function(err, recordset) {

                    if (err) return cb(err);

                    if (recordset.length === 0) {
                        return cb(null, []);
                    }

                    var pks = [];
                    recordset.forEach(function(row) {
                        pks.push(row[pk]);
                    });

                    Object.keys(values).forEach(function(value) {
                        values[value] = utils.prepareValue(values[value]);
                    });

                    statement = 'UPDATE [' + tableName + '] SET ' + sql.updateCriteria(dbs[collectionName].identity, values) + ' ';
                    statement += sql.serializeOptions(dbs[collectionName].identity, options);

                    request.query(statement, function(err, recordset) {

                        if (err) return cb(err);

                        var criteria;

                        if(pks.length === 1) {
                            criteria = { where: {}, limit: 1 };
                            criteria.where[pk] = pks[0];
                        } else {
                            criteria = { where: {}};
                            criteria.where[pk] = pks;
                        }

                        adapter.find(collectionName, criteria, function(err, models) {

                            if (err) return cb(err);
                            var values = [];
                            var _query = new Query(dbs[collectionName].definition);

                            models.forEach(function(item) {
                                values.push(_query.cast(item));
                            });

                            cb(err, values);
                        });
                    });
                });
            });
        },

        destroy: function(collectionName, options, cb) {

            var tableName = dbs[collectionName].identity;
            var statement = 'DELETE FROM [' + tableName + '] OUTPUT deleted.*';
            statement += sql.serializeOptions(dbs[collectionName].identity, options);

            mssql.connect(marshalConfig(dbs[collectionName].config), function __DELETE__(err,recordset) {

                if (err) return cb(err);
                cb(null, recordset);

            });
        },

    };

    function marshalConfig(config,accountid) {
        return {
            user: config.user,
            password: config.password,
            server: "10.0.0.121",
            port: config.port || 1433,
            database: "test",
            timeout: config.timeout || 5000,
            pool: {
                max: (config.pool && config.pool.max) ? config.pool.max : 10,
                min: (config.pool && config.pool.min) ? config.pool.min : 0,
                idleTimeoutMillis: (config.pool && config.pool.idleTimeout) ? config.pool.idleTimeout : 30000
            },
            options: {
                appName: 'sails.js'
            }
        };
    }

    return adapter;

})();