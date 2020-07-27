// 数据模型基类
// by YuRonghui 2020-7-22
const { Query } = require('wood-query')();
const { Util } = require('wood-util')();
const skipLimit = 50000; //限制不能超过50000条数据跳过

class Model {
  constructor(opts = {}) {
    this.tableName = opts.tableName || '';
    this.primarykey = opts.primarykey || '_id'; //默认主键名
    this.fields = opts.fields || {};
    this.select = opts.select || {};
    this.addLock = opts.addLock || false;
    this.relation = {};
  }

  // 设置getter和setter
  _get_set() {
    let obj = {}, fieldMap = this.fields.fieldMap;
    for (let key in fieldMap) {
      obj[key] = {
        get() {
          return fieldMap[key].value || fieldMap[key].default;
        },
        set(val) {
          fieldMap[key].value = val;
        }
      }
    }
    return obj;
  }

  init() {
    let fields = this.fields.data || {};
    for (let key in fields) {
      let item = fields[key];
      // 建索引
      if (item.index) {
        let indexField = {}, indexOption = {};
        if(typeof item.index === 'object'){
          indexOption = item.index;
        }
        indexField[key] = item.index == 'text' ? item.index : 1 ;
        this.db.index(indexField, indexOption);
      }
      //表关联
      if (item.key && item.as && item.from) {
        if (item) {
          this.relation[key] = item;
        }
      }
    }
    return Object.create(this, this._get_set());
  }

  // 创建索引
  createIndex(data = {}, opts = {}) {
    if (!Util.isEmpty(data)) this.db.collection.ensureIndex(data, opts);
  }

  // 删除索引
  removeIndex(name) {
    if (name) this.db.collection.dropIndex(name);
  }

  // 重置数据
  resetData() {
    this.fields.resetData();
  }

  // 设置数据
  setData(target, value) {
    this.fields.setData(target, value);
  }

  // 获取模型数据
  getData(data) {
    return this.fields.getData(data);
  }

  // 是否新的
  isNew() {
    return !this.getData()[this.primarykey];
  }

  //新增数据
  async create(data = {}) {
    const { error } = WOOD;
    if(!data || Util.isEmpty(data)) throw error('create方法的参数data不能为空');
    this.setData(data);
    let err = this.fields.validate();
    if (err) throw error(err);
    let result = await this.db.create(this.getData());
    return { _id: result.insertedId };
  }

  // 更新数据
  async update(data = {}, value = {}, isFindOneAndUpdate) {
    const { catchErr, error } = WOOD;
    if (!data || Util.isEmpty(value)) throw error('update方法的参数data不能为空');
    this.setData(value);  
    let err = this.fields.validate(value),
      hasSet = false;
    if (err) throw error(err);
    let newData = this.getData(value);
    let method = isFindOneAndUpdate ? 'findOneAndUpdate' : 'update';
    hasSet = JSON.stringify(value).indexOf('$') >= 0;
    const result = await this.db[method](data, hasSet ? value : { $set: newData });
    if(isFindOneAndUpdate){
      return result.value;
    }else{
      return { updated: result.result.nModified };
    }
  }

  // 更新数据, 结果返回当前记录
  async findOneAndUpdate(data = {}, value = {}) {
    return this.update(data, value, true);
  }

  // 保存数据
  async save() {
    const { error } = WOOD;
    let data = this.getData();
    let err = this.fields.validate(data);
    if (err) throw error(err);
    return await this.db.collection.save(data);
  }

  //删除数据
  async remove(data) {
    const { error } = WOOD;
    if (!data) return false;
    const result =  await this.db.remove(data);
    if (result.result.n === 0) throw error('无对应数据');
    return { _id: data._id };
  }

  //清空数据
  async clear() {
    return await this.db.clear();
  }

  // 执行查询
  exec(oper = 'find', data) {
    const { error } = WOOD;
    if (this.db[oper]) {
      if (data.aggregate.length) {
        return this.db.aggregate(data);
      } else {
        return this.db[oper](data);
      }
    }
    throw error(error_code.error_nodata);
  }

  // 查询单条记录
  async findOne(data) {
    let query = data, idObj = {};
    if(!data._isQuery){
      if (typeof data === 'number') {
        idObj[this.primarykey] = data;
        query = Query(idObj);
      } else if (typeof data === 'object') {
        query = Query(data);
      }
    }
    if (!Util.isEmpty(this.select)) query.select(this.select);
    if (!Util.isEmpty(this.relation)) query.populate(this.relation);
    let result = await this.exec('findOne', query.toJSON());
    return Array.isArray(result) ? result[0] : result;
  }

  // 查询数据列表
  async findList(data) {
    if (!data) throw error('findList方法参数data不能为空');
    let query = data._isQuery ? data : Query(data);
    let _data = query.toJSON();
    let limitNum = _data.limit == undefined ? 100 : Number(_data.limit);
    let page = Number(query.page) || 1;
    let skipNum = limitNum * (page - 1)
    let count = await this.db.count(query.toJSON());
    query.skip(skipNum > skipLimit ? skipLimit : skipNum)
    query.limit(limitNum)
    if (!Util.isEmpty(this.select)) query.select(this.select);
    if (!Util.isEmpty(this.relation)) query.populate(this.relation);
    let lists = await this.exec('find', query.toJSON());
    return {
      total: Number(count),
      list: lists || []
    };
  }

}
module.exports = Model;
