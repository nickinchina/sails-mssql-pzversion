
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
                return data(new Error('invalid server parameters'));
            }
            var conn = new mssql.Connection(marshalConfig(dbs[collectionName].config, data));
            conn.connect(function(err) {
                if (err) return cb(err);
                    var request = new mssql.Request(conn);
                    if (_.isObject(statement)){
                        statement.parameters.forEach(function(i){
                            if (!!i.length)
                                request.input(i.name, mssql[i.type](i.length), i.value);
                            else
                                request.input(i.name, mssql[i.type](), i.value);
                        })
                        request.execute(statement.sp).then(function(recordsets) {
                                cb(null, recordsets);
                            }).catch(function(err) {
                                cb(err);
                            });
                    }
                    else 
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
            var user = data.__user;
            delete data.__user;
            delete data.accountid;
            
            Object.keys(data).forEach(function(value) {
                data[value] = utils.prepareValue(data[value]);
            });
            var statement = sql.insertQuery(dbs[collectionName].identity, data, dbs[collectionName].definition);
            
            var conn = new mssql.Connection(marshalConfig(dbs[collectionName].config, user));
            conn.connect(function __CREATE__(err, recordset) {

                if (err) return cb(err);

                var request = new mssql.Request(conn);
                request.query(statement, function(err, recordsets) {
                    if (err) return cb(err);
                    var recordset = recordsets[0];
                    var model = data;
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

            var user;
            if (options.where && options.where.__user) {
                user = options.where.__user;
                delete options.where.__user;
                delete options.where.accountid;
                if (Object.keys(options.where).length==0)
                    delete(options["where"]);
            }
            else
                return cb(new Error('invalid server parameters'));
            
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
            var user = options.where.__user;
            delete options.where.__user;
            delete values.id;
            
            var tableName = dbs[collectionName].identity;
            var criteria = sql.serializeOptions(dbs[collectionName].identity, options);
            
            var conn = new mssql.Connection(marshalConfig(dbs[collectionName].config, user));
            conn.connect(function __UPDATE__(err) {

                if (err) return cb(err);

                var request = new mssql.Request(conn);
                var statement = 'UPDATE pz.[' + tableName + '] SET ' + sql.updateCriteria(dbs[collectionName].identity, values, dbs[collectionName].definition) + ' output inserted.*';
                statement += sql.serializeOptions(dbs[collectionName].identity, options);
                console.log(statement);
                request.query(statement, function(err, recordset) {
                    if (err) return cb(err);
                    cb(null, recordset);
                });
            });
        },

        destroy: function(collectionName, options, cb) {
                console.log(options);
            var user = options.where.__user;
            delete options.where.__user;
            
            var tableName = dbs[collectionName].identity;
            var conn = new mssql.Connection(marshalConfig(dbs[collectionName].config, user));
            
            conn.connect(function __DELETE__(err) {
                if (err) return cb(err);
                
                var request = new mssql.Request(conn);
                var statement = 'DELETE FROM pz.[' + tableName + '] OUTPUT deleted.*';
                statement += sql.serializeOptions(dbs[collectionName].identity, options);
                console.log(statement);
                request.query(statement, function(err, recordset) {
                    if (err) return cb(err);
                    cb(null, recordset);
                });

            });
        },

    };

    function marshalConfig(config,user) {
        console.log('marshalConfig', user)
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