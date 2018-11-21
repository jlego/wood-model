/**
 * Wood Plugin Module.
 * 数据模型
 * by jlego on 2018-11-18
 */
const Model = require('./src/model');

module.exports = (app = {}, config = {}) => {
  const { Util } = require('wood-util')(app);
  const { Redis } = require('wood-redis')(app);
  const { Mongo } = require('wood-mongo')(app);
  app._models = new Map();
  app.Model = function(_tableName, fields, select = {}, primarykey) {
    let nameArr = _tableName.split('.'),
      dbName = nameArr.length > 1 ? nameArr[0] : 'master',
      tableName = nameArr.length > 1 ? nameArr[1] : nameArr[0];
    if(tableName){
      if(app._models.has(tableName)){
        let _model = app._models.get(tableName);
        if(_model) _model.resetData();
        return _model;
      }
      if (tableName && fields) {
        let theModel = new Model({
          tableName,
          fields,
          select,
          primarykey
        });
        theModel.redis = new Redis(tableName);
        theModel.db = new Mongo(tableName, dbName);
        app._models.set(tableName, theModel);
        theModel._init();
        return app._models.get(tableName);
      }
    }
    return Model;
  };
  if(app.addAppProp) app.addAppProp('Model', app.Model);
  return app;
}
