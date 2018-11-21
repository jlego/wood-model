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
  app.Model = function({tableName, fields, select = {}, primarykey}) {
    let nameArr = tableName.split('.'),
      dbName = nameArr.length > 1 ? nameArr[0] : 'master',
      _tableName = nameArr.length > 1 ? nameArr[1] : nameArr[0];
    if(_tableName){
      if(app._models.has(_tableName)){
        let _model = app._models.get(_tableName);
        if(_model) _model.resetData();
        return _model;
      }
      if (_tableName && fields) {
        let theModel = new Model({
          tableName: _tableName,
          fields,
          select,
          primarykey
        });
        theModel.redis = new Redis(_tableName);
        theModel.db = new Mongo(_tableName, dbName);
        app._models.set(_tableName, theModel);
        theModel._init();
        return app._models.get(_tableName);
      }
    }
    return Model;
  };
  if(app.addAppProp) app.addAppProp('Model', app.Model);
  return app;
}
