
var _ = require('underscore');
_.str = require('underscore.string');
var utils = require('./utils');
var skipKey = function(collectionName, key){
	switch (collectionName){
		case 'hq.pz_user':
		case 'hq.pz_user_oauth':
        case 'hq.pz_device':
		case 'hq.pz_user_group':
		case 'hq.reports_schedule':
		case 'hq.vw_terminal':
            return false;
        default:
            return key=="accountid";
	}
}
var sql = {
    skipKey:skipKey,
	escapeId: function (val) {
		return "[" + val.replace(/'/g, "''") + "]";
	},

	escape: function(val, stringifyObjects, timeZone) {
  
		if (val === undefined || val === null) {
			return 'NULL';
		}

		switch (typeof val) {
			case 'boolean': return (val) ? '1' : '0'; 
			case 'number': return val + '';
		}

		if (typeof val === 'object') {
			val = val.toString();
		}

		val = val.replace(/[\"\']/g, function(s) {
			switch(s) {
				case "\'": return "''";
				case '\"': return "''";
				default: return " ";
			}
		});

		return "'" + val + "'";
	},

	normalizeSchema: function (schema) {
		return _.reduce(schema, function(memo, field) {
			
			// Marshal mssql DESCRIBE to waterline collection semantics
			var attrName = field.ColumnName;
			var type = field.TypeName;

			memo[attrName] = {
				type: type,
				//defaultsTo: field.Default
			};

			memo[attrName].autoIncrement = field.AutoIncrement;
			memo[attrName].primaryKey = field.PrimaryKey;
			memo[attrName].unique = field.Unique;
			memo[attrName].indexed = field.Indexed;
			memo[attrName].nullable = field.Nullable;

			return memo;
		}, {});
	},

	// @returns ALTER query for adding a column
	addColumn: function (collectionName, attrName, attrDef) {
		var tableName = collectionName;
		var columnDefinition = sql._schema(collectionName, attrDef, attrName);
		return 'ALTER TABLE ' + tableName + ' ADD ' + columnDefinition;
	},

	// @returns ALTER query for dropping a column
	removeColumn: function (collectionName, attrName) {
    	var tableName = collectionName;
    	attrName = attrName;
    	return 'ALTER TABLE ' + tableName + ' DROP COLUMN ' + attrName;
	},

	selectQuery: function (collectionName, options) {
		var tableName = collectionName;
		if (tableName.indexOf('.')<0) tableName = "pz." + tableName;
		var query = utils.buildSelectStatement(options, tableName);
		return query += sql.serializeOptions(tableName, options);
	},

	insertQuery: function (collectionName, data, definition) {
		var tableName = collectionName;
		if (tableName.indexOf('.')<0) tableName = "pz." + tableName;
    	//return 'INSERT INTO ' + tableName + ' ' + '(' + sql.attributes(collectionName, data) + ')  VALUES (' + 
    	switch (tableName.toLowerCase()) {
            case 'pz.pz_product':
            case 'pz.pz_productprice':
            case 'hq.reports_schedule':
            case 'acc.qbfileperstore':
            case 'pz.pz_entity':
            	return 'declare @t table(id int); INSERT INTO ' + tableName + ' (' + sql.attributes(collectionName, data) + ')  output inserted.id into @t VALUES (' +
					sql.values_v2(collectionName, data,definition) + ');SELECT * FROM ' + tableName + ' WHERE id in (select id from @t)';
            default:
		    	return 'INSERT INTO ' + tableName + ' ' + '(' + sql.attributes(collectionName, data) + ')  OUTPUT inserted.* VALUES (' + 
		    		sql.values_v2(collectionName, data,definition) + ')';
    	}
	},

	// Create a schema csv for a DDL query
	schema: function(collectionName, attributes) {
		return sql.build(collectionName, attributes, sql._schema);
	},

	_schema: function(collectionName, attribute, attrName) {

    	attrName = attrName;
    	var type = sqlTypeCast(attribute.type);

		if (attribute.primaryKey) {

			// If type is an integer, set auto increment
			if(type === 'INT') {
				return attrName + ' ' + type + ' IDENTITY(1,1) PRIMARY KEY';
			}

			// Just set NOT NULL on other types
			return attrName + ' VARCHAR(255) NOT NULL PRIMARY KEY';
		}

		// Process UNIQUE field
		if (attribute.unique) {
			return attrName + ' ' + type + ' UNIQUE';
		}

		return attrName + ' ' + type + ' ';
	},

	// Create an attribute csv for a DQL query
	attributes: function(collectionName, attributes) {
		return sql.build(collectionName, attributes, sql.prepareAttribute);
	},
	values_v2:function(collectionName, values, definition){
		var separator = separator || ', ';
		var $sql = '';
		_.each(values, function(value, key) {
			if (!skipKey(collectionName,key)) {
				$sql += sql.prepareValue(collectionName, value, key, definition[key]);
	
				// (always append separator)
				$sql += separator;
			}
		});

		return _.str.rtrim($sql, separator);
	},
	// Create a value csv for a DQL query
	// key => optional, overrides the keys in the dictionary
	values: function(collectionName, values, key) {
		return sql.build(collectionName, values, sql.prepareValue, ', ', key);
	},

	updateCriteria: function(collectionName, values, definition) {
		var separator = ', ';
		var $sql = '';
		
		_.each(values, function(value, key) {
			if (!skipKey(collectionName,key)) {
				$sql += sql.prepareUpdate(collectionName, value, key, definition);
				$sql += separator;
			}
		});

		return _.str.rtrim($sql, separator);
	},
	prepareUpdate: function(collectionName, value, key, definition) {
		var attrStr = sql.prepareAttribute(collectionName, value, key);
		var valueStr = sql.prepareValue(collectionName, value, key, definition[key]);
		if (_.isNull(value)) {
			return attrStr + " = NULL";
		} else return attrStr + "=" + valueStr;
	},

	prepareCriterion: function(collectionName, value, key, parentKey) {
		
		if (validSubAttrCriteria(value)) {
			return sql.where(collectionName, value, null, key);
		}

		// Build escaped attr and value strings using either the key,
		// or if one exists, the parent key
		var attrStr, valueStr;


		// Special comparator case
		if (parentKey) {

			attrStr = sql.prepareAttribute(collectionName, value, parentKey);
			valueStr = sql.prepareValue(collectionName, value, parentKey);

			// Why don't we strip you out of those bothersome apostrophes?
			var nakedButClean = _.str.trim(valueStr,'\'');

			if (key === '<' || key === 'lessThan') return attrStr + '<' + valueStr;
			else if (key === '<=' || key === 'lessThanOrEqual') return attrStr + '<=' + valueStr;
			else if (key === '>' || key === 'greaterThan') return attrStr + '>' + valueStr;
			else if (key === '>=' || key === 'greaterThanOrEqual') return attrStr + '>=' + valueStr;
			else if (key === '!' || key === 'not') {
				if (value === null) return attrStr + ' IS NOT NULL';
				else return attrStr + '<>' + valueStr;
			}
			else if (key === 'like') return attrStr + ' LIKE \'' + nakedButClean + '\'';
			else if (key === 'contains') return attrStr + ' LIKE \'%' + nakedButClean + '%\'';
			else if (key === 'startsWith') return attrStr + ' LIKE \'' + nakedButClean + '%\'';
			else if (key === 'endsWith') return attrStr + ' LIKE \'%' + nakedButClean + '\'';
			else throw new Error('Unknown comparator: ' + key);
		} else {
			attrStr = sql.prepareAttribute(collectionName, value, key);
			valueStr = sql.prepareValue(collectionName, value, key);
			if (_.isNull(value)) {
				return attrStr + " IS NULL";
			} else return attrStr + "=" + valueStr;
		}
	},

	prepareValue: function(collectionName, value, attrName, definition) {
		if (definition && definition.type && definition.type.toLowerCase()=='json'){
			if (!!value)
				return  "'" + JSON.stringify(_.isString(value)?JSON.parse(value):value) +  "'" ;
			return "NULL";
		}
			
		// Cast dates to SQL
		if (_.isDate(value)) {
			value = toSqlDate(value);
		}

		// Cast functions to strings
		if (_.isFunction(value)) {
			value = value.toString();
		}

		// Escape (also wraps in quotes)
		return sql.escape(value);
	},

	prepareAttribute: function(collectionName, value, attrName) {
		return '[' + attrName + ']';
	},

	// Starting point for predicate evaluation
	// parentKey => if set, look for comparators and apply them to the parent key
	where: function(collectionName, where, key, parentKey) {
		return sql.build(collectionName, where, sql.predicate, ' AND ', undefined, parentKey);
	},

	// Recursively parse a predicate calculus and build a SQL query
	predicate: function(collectionName, criterion, key, parentKey) {

		var queryPart = '';

		if (parentKey) {
			return sql.prepareCriterion(collectionName, criterion, key, parentKey);
		}

		// OR
		if (key.toLowerCase() === 'or') {
			queryPart = sql.build(collectionName, criterion, sql.where, ' OR ');
			return ' ( ' + queryPart + ' ) ';
		}

		// AND
		else if (key.toLowerCase() === 'and') {
			queryPart = sql.build(collectionName, criterion, sql.where, ' AND ');
			return ' ( ' + queryPart + ' ) ';
		}

		// IN
		else if (_.isArray(criterion)) {
			queryPart = sql.prepareAttribute(collectionName, null, key) + " IN (" + sql.values(collectionName, criterion, key) + ")";
      		return queryPart;
		}

		// LIKE
		else if (key.toLowerCase() === 'like') {
			return sql.build(collectionName, criterion, function(collectionName, value, attrName) {
				var attrStr = sql.prepareAttribute(collectionName, value, attrName);
				if (_.isRegExp(value)) {
					throw new Error('RegExp not supported');
        		}
				var valueStr = sql.prepareValue(collectionName, value, attrName);
				// Handle escaped percent (%) signs [encoded as %%%]
				valueStr = valueStr.replace(/%%%/g, '\\%');

				return attrStr + " LIKE " + valueStr;
			}, ' AND ');
		}

		// NOT
		else if (key.toLowerCase() === 'not') {
			throw new Error('NOT not supported yet!');
		}

		// Basic criteria item
		else {
			return sql.prepareCriterion(collectionName, criterion, key);
		}

	},

	serializeOptions: function(collectionName, options) {
		console.log('serializeOptions collectionName, options');

		var queryPart = '';

		if (options.where) {
			var w = sql.where(collectionName, options.where);
			if (w && w.length>1)
				queryPart += 'WHERE ' + w + ' ';
		}

		if (options.groupBy) {
			queryPart += 'GROUP BY ';

			// Normalize to array
			if(!Array.isArray(options.groupBy)) options.groupBy = [options.groupBy];
			options.groupBy.forEach(function(key) {
				queryPart += key + ', ';
			});

			// Remove trailing comma
			queryPart = queryPart.slice(0, -2) + ' ';
    	}

		if (options.sort) {
			queryPart += 'ORDER BY ';

			// Sort through each sort attribute criteria
			_.each(options.sort, function(direction, attrName) {

				queryPart += sql.prepareAttribute(collectionName, null, attrName) + ' ';

				// Basic MongoDB-style numeric sort direction
				if (direction === 1) {
					queryPart += 'ASC, ';
				} else {
					queryPart += 'DESC, ';
				}
			});

			// Remove trailing comma
			if(queryPart.slice(-2) === ', ') {
				queryPart = queryPart.slice(0, -2) + ' ';
			}
		}

		return queryPart;
	},	

	build: function(collectionName, collection, fn, separator, keyOverride, parentKey) {

		separator = separator || ', ';
		var $sql = '';
		
		_.each(collection, function(value, key) {
			if (!skipKey(collectionName,key)) {
				$sql += fn(collectionName, value, keyOverride || key, parentKey);
	
				// (always append separator)
				$sql += separator;
			}
		});

		return _.str.rtrim($sql, separator);
	}

};

// Cast waterline types into SQL data types
function sqlTypeCast(type) {

	type = type && type.toLowerCase();

	switch (type) {
		case 'string': return 'NVARCHAR(255)';
		case 'text':
		case 'array':
		case 'json': return 'NTEXT';
		case 'boolean': return 'BIT';
		case 'int':
		case 'integer': return 'INT';
		case 'float':
		case 'double': return 'FLOAT';
		case 'date': return 'DATE';
		case 'time': return 'TIME';
		case 'datetime': return 'DATETIME';
		case 'xml': return 'XML';
		default:
			console.error("Unregistered type given: " + type);
			return "TEXT";
	}
}

function wrapInQuotes(val) {
	return '"' + val + '"';
}

function toSqlDate(date) {
	date = date.getUTCFullYear() + '-' +
		('00' + (date.getUTCMonth()+1)).slice(-2) + '-' +
		('00' + date.getUTCDate()).slice(-2) + ' ' +
		('00' + date.getUTCHours()).slice(-2) + ':' +
		('00' + date.getUTCMinutes()).slice(-2) + ':' +
		('00' + date.getUTCSeconds()).slice(-2);

	return date;
}

function validSubAttrCriteria(c) {
	return _.isObject(c) && (
	!_.isUndefined(c.not) || !_.isUndefined(c.greaterThan) || !_.isUndefined(c.lessThan) ||
	!_.isUndefined(c.greaterThanOrEqual) || !_.isUndefined(c.lessThanOrEqual) || !_.isUndefined(c['<']) ||
	!_.isUndefined(c['<=']) || !_.isUndefined(c['!']) || !_.isUndefined(c['>']) || !_.isUndefined(c['>=']) ||
	!_.isUndefined(c.startsWith) || !_.isUndefined(c.endsWith) || !_.isUndefined(c.contains) || !_.isUndefined(c.like));
}

module.exports = sql;