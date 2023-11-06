
var _ = require('underscore');
var mssql = require('mssql');
var Query = require('./query');
var sql = require('./sql.js');
var utils = require('./utils');
var translateTableName = function (tableName){
    switch (tableName){
        case "pz_competitor":
        case "pz_customer":
        case "pz_employee":
        case "pz_store":
        case "pz_taxauthority":
        case "pz_fuelterminal":
        case "pz_vendor":
            return "pz_entity";
        default:
            return tableName;
    }
}

var isTriggerBased = function(tableName){
    switch (tableName.toLowerCase()) {
        case 'pz.pz_product':
        case 'pz.pz_productprice':
        case 'hq.reports_schedule':
        case 'acc.qbfileperstore':
        case 'pz.pz_entity':
        case 'pz.pz_taxons':
        case 'pz.vw_mop':
        //Modified by Lucky, on Dec.31,2020 PRIME-2405
        case 'pz.pz_fuelcharge':
        case 'pz.pz_fuelcharge_schedule':            
        //Modified by Lucky, on Jan.11,2021 PRIME-2405
        case 'pz.pz_productrebate':
        case 'pz.pz_productrebate_schedule':
        case 'hq.pz_user':
        //Added by Lucky, on Mar.5,2021 PRIME-2597
        case 'trans.pz_bol':
            return true;
        default:
            return false;
    }
}

var hasApprove = function(tableName){
    switch (tableName.toLowerCase()) {
        case 'trans.pz_bol':
        case 'trans.pz_eft':
        case 'trans.pz_ccs':
            return true;
        default:
            return false;
    }
}

//added by lily on 11/12/2020,PRIME-2248
var findTable = function(tableName){
    switch (tableName.toLowerCase()) {
        case 'hq.pz_terminal':
            return 'hq.vw_terminal';
        default:
            return tableName;
    }
}
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
                        var rinfo;
                        if (statement.info){
                            rinfo = '';
                            request.on('info', function(info){
                                if (rinfo.length>0) rinfo+='\r\n'
                                rinfo += info
                            })
                        }
                        
                        request.execute(statement.sp).then(function(recordsets) {
                                cb(null, recordsets,rinfo);
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

        updateAddress: function(conn, values, cb){
            var request = new mssql.Request(conn);
            var address__address = values.address__address;
            delete values.address__address;
            if (_.isObject(address__address)) address__address = JSON.stringify(address__address);
            request.input('address', mssql.NVarChar(4001), address__address);
            request.execute('pz.sp_update_address', function(err, recordsets, returnValue) {
                cb(err,recordsets[0]);
            });
        },
        create: function(collectionName, data, cb) {
            var user = data.__user;
            delete data.__user;
            if (sql.skipKey(collectionName,"accountid")) {
                delete data.accountid;
            }
            
            Object.keys(data).forEach(function(value) {
                data[value] = utils.prepareValue(data[value]);
            });
            
            var address__address = !!dbs[collectionName].definition.address__address;
            
            var conn = new mssql.Connection(marshalConfig(dbs[collectionName].config, user));
            conn.connect(function __CREATE__(err, recordset) {

                if (err) return cb(err);

                function __create(address){
                    if (!!address) {
                        data.address = address.map(function(i) {return i.id}).join(',');
                    }
                    var statement = sql.insertQuery(translateTableName(dbs[collectionName].identity), data, dbs[collectionName].definition,isTriggerBased);
                    var request = new mssql.Request(conn);
                    request.query(statement, function(err, recordsets) {
                        if (err) return cb(err);
                        // var model = data;
                        // var _query = new Query(dbs[collectionName].definition);
                        // var values = _query.cast(model);
                        if (!!address)recordsets[0].address__address = address;
                        cb(err, recordsets[0]);
    
                    });
                }
                
                if (address__address)
                    adapter.updateAddress(conn, data, function(err, result){
                        if (err) return cb(err);
                        __create(result);
                    })
                else
                    __create();
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
            else {
                //table export.FtpUploadInfo
                if (collectionName.substr(0,3)!='hq.' && collectionName.substr(0,7)!='export.')
                    return cb(new Error('invalid server parameters'));
            }
            
            
            var address__address = !!dbs[collectionName].definition.address__address;
            options.address__address = address__address;
            
            var t = dbs[collectionName].identity;
            //if (t=="hq.pz_terminal") t = "hq.vw_terminal";
            t = findTable(t);
            var statement = sql.selectQuery(t, options);

            var conn = new mssql.Connection(marshalConfig(dbs[collectionName].config, user));
            conn.connect(function __FIND__(err) {

                if (err) return cb(err);

                var request = new mssql.Request(conn);
                
                request.query(statement, function(err, recordset) {

                    if (err) return cb(err);
                    if (address__address) {
                        recordset.forEach(function(i){
                            if (!!i.address__address) i.address__address = JSON.parse(i.address__address)
                            else i.address__address=[];
                        })
                    }
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
            var address__address = !!dbs[collectionName].definition.address__address;
            
            var conn = new mssql.Connection(marshalConfig(dbs[collectionName].config, user));
            conn.connect(function __UPDATE__(err) {

                if (err) return cb(err);
                var findT = undefined;
                if (tableName.indexOf('.')<0) {
                    findT = "pz." + tableName;
                    tableName = "pz." + translateTableName(tableName);
                }
                
                function __update(address) {
                    if (!!address) {
                        values.address = address.map(function(i) {return i.id}).join(',');
                    }
                    var request = new mssql.Request(conn);
                    var statement;
                    var where = sql.serializeOptions(dbs[collectionName].identity, options);

                    if (!findT)
                        findT = findTable(tableName);

                    var tHasApprove = hasApprove(tableName);
                    if (isTriggerBased(tableName)  || tableName != findT || tHasApprove){
                        statement ='UPDATE ' + tableName + ' SET ' + sql.updateCriteria(dbs[collectionName].identity, values, dbs[collectionName].definition) + ' ' + where + (tHasApprove?' AND approvedAt IS NULL;' : ';');
                        statement += 'SELECT * FROM ' + findT + ' ' + where + ';';
                    }
                    else {
                        statement ='UPDATE ' + tableName + ' SET ' + sql.updateCriteria(dbs[collectionName].identity, values, dbs[collectionName].definition) + ' output inserted.* ';
                        statement += where;
                    }
                    
                    request.query(statement, function(err, recordset) {
                        console.log(tableName, err);
                        if (err || tableName == 'pz_store') {
                            console.log(statement);
                            console.log(options);
                            console.log(values);
                        }
                        if (err) return cb(err);
                        var r = recordset[0];
                        delete r.password;
                        if (!!address)r.address__address = address;
                        cb(null, r);
                    });
                }
                
                if (address__address)
                    adapter.updateAddress(conn, values, function(err, result){
                        if (err) return cb(err);
                        __update(result);
                    })
                else
                    __update();
            });
        },

        destroy: function(collectionName, options, cb) {
            var user = options.where.__user;
            delete options.where.__user;
            
            var tableName = dbs[collectionName].identity;
            if (tableName.indexOf('.')<0) {
                findT = "pz." + tableName;
                tableName = "pz." + translateTableName(tableName);
            }
            var conn = new mssql.Connection(marshalConfig(dbs[collectionName].config, user));
            
            conn.connect(function __DELETE__(err) {
                if (err) return cb(err);
                
                var request = new mssql.Request(conn);
                if (tableName == "hq.pz_user"||tableName == "hq.reports_schedule") delete options.where.accountid;
                var statement = 'DELETE FROM ' + tableName + ' ';
                if (!isTriggerBased(tableName)) statement += 'OUTPUT deleted.* ';
                statement += sql.serializeOptions(dbs[collectionName].identity, options);
                //console.log('destroy statement',statement)
                request.query(statement, function(err, recordset) {
                    if (err) return cb(err);
                    cb(null, recordset);
                });

            });
        },

    };

    function marshalConfig(config,user) {
        var server = config.hq ? (config.hq_host||'10.0.0.99'):(user.sqlserver||"10.0.0.121");
        var port = config.hq ?(config.hq_port||31433) :(config.port || 1433);
        var database = config.hq?(config.hq_db||'s2k'): user.dbname||"kwstest";
        console.log(config, server, port, database);
        return {
            user: config.user,
            password: config.password,
            server:  server,
            port: port,
            database:database,
            connectionTimeout: config.connectionTimeout || 30000,
            requestTimeout: config.requestTimeout || 120000,
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
