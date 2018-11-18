/**
 * Wood Plugin Module.
 * 数据模型
 * by jlego on 2018-11-18
 */
const Model = require('./src/model');
const { catchErr, error } = require('wood-util')();
const Redis = require('wood-redis')();

module.exports = (app, config = {}) => {
  if(app){
    app.models = app.models || new Map();
    app.Model = function(_tableName, fields, select = {}) {
      let nameArr = _tableName.split('.'),
        dbName = nameArr.length > 1 ? nameArr[0] : 'master',
        tableName = nameArr.length > 1 ? nameArr[1] : nameArr[0];
      if(tableName){
        if(app.models.has(tableName)){
          let _model = app.models.get(tableName);
          if(_model) _model.resetData();
          return _model;
        }
        if (tableName && fields) {
          let theModel = new Model({
            tableName,
            fields,
            select
          });
          theModel.redis = new Redis(tableName);
          theModel.db = new app.Mongo(tableName, dbName);
          app.models.set(tableName, theModel);
          theModel._init();
          return app.models.get(tableName);
        }
      }
      return Model;
    };
  }
  return Model;
}
