// 数据模型基类
// by YuRonghui 2018-11-18
const { Query } = require('wood-query')();
const { Util } = require('wood-util')();
const mongodb = require('mongodb');
const ObjectId = mongodb.ObjectID;
const largelimit = 20000; //限制不能超过2万条数据返回
const _timeout = 0;
const _KeyTimeout = 60 * 1; //设置listkey过期时间，秒

class Model {
  constructor(opts = {}) {
    this.tableName = opts.tableName || '';
    this.primarykey = opts.primarykey || '_id'; //默认主键名
    this.fields = opts.fields || {};
    this.select = opts.select || {};
    this.addLock = opts.addLock || true;
    this.relation = {};
  }

  // 设置getter和setter
  _get_set() {
    let obj = {}, fieldMap = this.fields.fieldMap;
    for (let key in fieldMap) {
      obj[key] = {
        get() {
          return fieldMap[key].value || fieldMap[key].defaultValue;
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
    const { catchErr, error } = WOOD;
    if(!data || Util.isEmpty(data)) throw error('create方法的参数data不能为空');
    this.setData(data);
    if (this.primarykey === 'rowid' || data.rowid == 0){
      let id = await this.db.rowid();
      this.setData('rowid', id);
    }
    let err = this.fields.validate();
    if (err) throw error(err);
    const lock = this.addLock ? await this.redis.lock() : true;
    let result = await this.db.create(this.getData());
    if(this.addLock) catchErr(this.redis.unlock(lock));
    return {_id: result.insertedId};
  }

  // 更新数据
  async update(data = {}, value = {}, isFindOneAndUpdate) {
    const { catchErr, error } = WOOD;
    if (!data || Util.isEmpty(value)) throw error('update方法的参数data不能为空');
    this.setData(value);
    let err = this.fields.validate(value),
      hasSet = false;
    if (err) throw error(err);
    let lock = this.addLock ? await this.redis.lock() : true;
    let method = isFindOneAndUpdate ? 'findOneAndUpdate' : 'update';
    hasSet = JSON.stringify(value).indexOf('$') === 0;
    const result = await this.db[method](data, hasSet ? value : { $set: this.getData(value) });
    if(this.addLock) catchErr(this.redis.unlock(lock));
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
  async save(_data) {
    const { catchErr, error } = WOOD;
    let data = _data || this.getData(false);
    if (Util.isEmpty(data) || !data) throw error('save方法的data为空');
    let err = this.fields.validate(data);
    if (err) throw error(err);
    const lock = await this.redis.lock();
    const result = await this.db.collection.save(data);
    if(this.addLock) catchErr(this.redis.unlock(lock));
    return result;
  }

  //删除数据
  async remove(data) {
    const { catchErr, error } = WOOD;
    if (!data) return false;
    const lock = await this.redis.lock();
    const result =  await this.db.remove(data);
    if(this.addLock) catchErr(this.redis.unlock(lock));
    if (result.result.n === 0) throw error('无对应数据');
    return {};
  }

  //清空数据
  async clear() {
    const { catchErr, error } = WOOD;
    const lock = await this.redis.lock();
    let result = await this.db.clear();
    if(this.addLock) catchErr(this.redis.unlock(lock));
    return result;
  }

  // 执行查询
  exec(oper = 'find', data) {
    const { error } = WOOD;
    if (this.db[oper]) {
      if (data.aggregate.length) {
        return this.db.aggregate(data.aggregate);
      } else {
        return this.db[oper](data);
      }
    }
    throw error(error_code.error_nodata);
  }

  // 查询单条记录
  async findOne(data) {
    const { catchErr, error } = WOOD;
    const hasLock = this.addLock ? await this.redis.hasLock() : false;
    if (!hasLock) {
      let query = data, idObj = {};
      if(!data._isQuery){
        query = Query();
        if (typeof data === 'number') {
          idObj[this.primarykey] = data;
          query.where(idObj);
        } else if (typeof data === 'object') {
          query.where(data);
        }
      }
      if (!Util.isEmpty(this.select)) query.select(this.select);
      if (!Util.isEmpty(this.relation)) query.populate(this.relation);
      let result = await this.exec('findOne', query.toJSON());
      return Array.isArray(result) ? result[0] : result;
    } else {
      await new Promise((resolve, reject) => {
        setTimeout(() => {
          resolve(true);
        }, _timeout);
      });
      return this.findOne(data);
    }
  }

  // 查询数据列表
  async findList(data, cacheKey = '') {
    const { catchErr, error } = WOOD;
    if (!data) throw error('findList方法参数data不能为空');
    let hasLock = this.addLock ? await this.redis.hasLock() : false;
    if (!hasLock) {
      let hasKey = false, largepage = 1;
      let page = data.page || 1;
      let query = data._isQuery ? data : Query(data);
      let _data = query.toJSON();
      let limit = _data.limit == undefined ? 20 : Number(_data.limit),
        idObj = {};
      largepage = _data.largepage || 1;
      page = page % Math.ceil(largelimit / limit) || 1;
      hasKey = await this.redis.existKey(cacheKey); //key是否存在
      if (hasKey) {
        let startIndex = (page - 1) * limit;
        idObj[this.primarykey] = await this.redis.listSlice(cacheKey, startIndex, startIndex + limit - 1);
        idObj[this.primarykey] = idObj[this.primarykey].map(item => parseInt(item));
      }
      query.where(idObj);
      if (!Util.isEmpty(this.select)) query.select(this.select);
      if (!Util.isEmpty(this.relation)) query.populate(this.relation);
      let counts = this.db.count(query),
        lists = this.exec('find', query.toJSON());
      const count = await counts;
      const docs = await lists;
      // 缓存id
      if (cacheKey && !hasKey && docs.length) {
        if (docs.length >= largelimit) {
          largepage = largepage || 1;
          let startNum = (largepage - 1) * largelimit;
          docs = docs.slice(startNum, startNum + largelimit);
        }
        await this.redis.listPush(cacheKey, docs.map(item => {
          let itemVal = item[this.primarykey] || 0;
          if(this.primarykey === '_id'){
            return itemVal.toString();
          }else{
            return itemVal;
          }
        }));
        this.redis.setKeyTimeout(cacheKey, _KeyTimeout); //设置listkey一小时后过期
        return this.findList(data, cacheKey);
      }
      return {
        count: Number(count),
        list: docs || []
      };
    }else{
      await new Promise((resolve, reject) => {
        setTimeout(() => {
          resolve(true);
        }, _timeout);
      });
      return this.findList(data, cacheKey);
    }
  }
}

module.exports = Model;
