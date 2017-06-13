  var AWS = require('aws-sdk');
  AWS.config.loadFromPath('./config.json');

  if (!AWS.config.credentials || !AWS.config.credentials.accessKeyId)
    throw 'Need to update config.json to specify your access key!';

  var db = new AWS.DynamoDB();

  function keyvaluestore(table) {
    this.inx = -1;
    this.LRU = require("lru-cache");
    this.cache = this.LRU({ max: 500 });
    this.tableName = table;
  };

  /**
   * Initialize the tables
   * 
   */
  keyvaluestore.prototype.init = function(whendone) {
    var tableName = this.tableName;
    var initCount = this.initCount;
    var self = this;
    
    db.listTables(function(err, data) {
      if (err) 
        console.log(err, err.stack);
      else {
        console.log("Connected to AWS DynamoDB");
        var tables = data.TableNames.toString().split(",");
        console.log("Tables in DynamoDB: " + tables);
        if (tables.indexOf(tableName) == -1) {
            throw 'Table '+tableName+' does not exists!';
        } else {
          self.initCount(whendone);
        }
      }
    }
    );
  }

  /**
   * Gets the count of how many rows are in the table
   * 
   */
  keyvaluestore.prototype.initCount = function(whendone) {
    var self = this;
    var params = {
        TableName: self.tableName,
        Select: 'COUNT'
    };
    
    db.scan(params, function(err, data) {
      if (err){
        console.log(err, err.stack);
      }
      else {
        self.inx = data.ScannedCount;

        console.log("Found " + self.inx + " indexed entries in " + self.tableName);
        whendone();
      }
    });

  }

  /**
   * Get result(s) by key
   * 
   * @param search
   * 
   * Callback returns a list of objects with keys "inx" and "value"
   */
  
keyvaluestore.prototype.get = function(search, callback) {
    var self = this;
    
    if (self.cache.get(search))
      callback(null, self.cache.get(search));
    else {
      var params = {
          KeyConditions: {
            keyword: {
              ComparisonOperator: 'EQ',
              AttributeValueList: [ { S: search}, ]
            }
          },
          TableName: self.tableName,
          AttributesToGet: [ 'inx', 'value', 'key' ]
      };
      
      db.query(params, function(err, data) {
        if (err || data.Items.length == 0) {
                console.log("Error o 0 elementos encontrados");
        	callback(err, null);
        }
        else {
          var items = [];
          for (var i = 0; i < data.Items.length; i++) {
            items.push({"inx": data.Items[i].inx.N, "value": data.Items[i].value.S, "key": data.Items[i].key});
          }
          self.cache.set(search, items);
          callback(err, items);
        }
      });
    }
  };

  /**
   * Test if search key has a match
   * 
   * @param search
   * @return
   */
  keyvaluestore.prototype.exists = function(search, callback) {
    var self = this;
    
    if (self.cache.get(search))
      callback(null, self.cache.get(search));
    else
      module.exports.get(search, function(err, data) {
        if (err)
          callback(err, null);
        else
          callback(err, (data == null) ? false : true);
      });
  };

  /**
   * Get result set by key prefix
   * @param search
   *
   * Callback returns a list of objects with keys "inx" and "value"
   */
  module.exports.getPrefix = function(search, callback) {
    var self = this;
    var params = {
        KeyConditions: {
          keyword: {
            ComparisonOperator: 'BEGINS_WITH',
            AttributeValueList: [ { S: search} ]
          }
        },
        TableName: self.tableName,
        AttributesToGet: [ 'value' ]
    };

    db.query(params, function(err, data) {
      if (err || data.Items.length == 0)
        callback(err, null);
      else {
        var items = [];
        for (var i = 0; i < data.Items.length; i++) {
          items.push({"inx": data.Items[i].inx.N, "value": data.Items[i].value.S});
        }
        callback(err, items);
      }
    });
  }

  module.exports = keyvaluestore;
