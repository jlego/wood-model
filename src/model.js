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
    this.addLock = opts.addLock || false;
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
  getData(hasVirtualField = true) {
    return this.fields.getData(hasVirtualField);
  }

  // 是否新的
  isNew() {
    return !this.getData()[this.primarykey];
  }

  //新增数据
  async create(data = {}) {
    const { catchErr, error } = WOOD;
    if (!data) throw error('create方法的参数data不能为空');
    if(!Util.isEmpty(data)) this.setData(data);
    if (this.primarykey === 'rowid' || data.rowid == 0){
      let id = await this.db.rowid();
      this.setData('rowid', id);
    }
    let err = this.fields.validate();
    if (err) throw error(err);
    const lock = this.addLock ? await catchErr(this.redis.lock()) : {data: 1};
    if (lock.err) throw error(lock.err);
    let result = await catchErr(this.db.create(this.getData()));
    if(this.addLock) catchErr(this.redis.unlock(lock.data));
    if(result.err) throw error(result.err);
    let resultData = {};
    resultData._id = result.data.insertedId;
    return resultData;
  }

  // 更新数据
  async update(data = {}, value = {}, isFindOneAndUpdate) {
    const { catchErr, error } = WOOD;
    if (!data) throw error('update方法的参数data不能为空');
    if (!Util.isEmpty(value)) {
      let err = this.fields.validate(data),
        hasSet = false;
      if (err) throw error(err);
      let lock = this.addLock ? await catchErr(this.redis.lock()) : {data: 1};
      if (lock.err) throw error(lock.err);
      let method = isFindOneAndUpdate ? 'findOneAndUpdate' : 'update';
      hasSet = value.indexOf('$') === 0;
      const result = await catchErr(this.db[method](data, hasSet ? value : { $set: value }));
      if(this.addLock) catchErr(this.redis.unlock(lock.data));
      if (result.err) throw error(result.err);
      if(isFindOneAndUpdate){
        return result.data.value;
      }else{
        return result.data;
      }
    }
    throw error(false);
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
    const lock = await catchErr(this.redis.lock());
    if (lock.err) throw error(lock.err);
    const result = await catchErr(this.db.collection.save(data));
    if (result.err) throw error(result.err);
    return result.data;
  }

  //删除数据
  async remove(data) {
    const { catchErr, error } = WOOD;
    if (!data) return false;
    const lock = await catchErr(this.redis.lock());
    if (lock.err) throw error(lock.err);
    const result =  await catchErr(this.db.remove(data));
    if (result.err) throw error(result.err);
    if (result.data.result.n === 0) throw error('无对应数据');
    return {};
  }

  //清空数据
  async clear() {
    const { catchErr, error } = WOOD;
    const lock = await catchErr(this.redis.lock());
    if (lock.err) throw error(lock.err);
    return this.db.clear();
  }

  // 执行查询
  exec(oper = 'find', data) {
    const { catchErr, error } = WOOD;
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
    const hasLock = this.addLock ? await catchErr(this.redis.hasLock()) : {};
    if(hasLock.err) throw error(hasLock.err);
    if (!hasLock.data) {
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
      let result = await catchErr(this.exec('findOne', query.toJSON()));
      if(result.err) throw error(result.err);
      return Array.isArray(result.data) ? result.data[0] : result.data;
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
    let hasLock = this.addLock ? await catchErr(this.redis.hasLock()) : {};
    if(hasLock.err) throw error(hasLock.err);
    if (!hasLock.data) {
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
        if(this.primarykey === 'rowid'){
          idObj[this.primarykey] = idObj[this.primarykey].map(item => parseInt(item));
        }
      }
      query.where(idObj);
      if (!Util.isEmpty(this.select)) query.select(this.select);
      if (!Util.isEmpty(this.relation)) query.populate(this.relation);
      let counts = this.db.count(query),
        lists = this.exec('find', query.toJSON());
      const countResult = await catchErr(counts);
      const docsResult = await catchErr(lists);
      if (docsResult.err || countResult.err) throw error(docsResult.err || countResult.err);
      let docs = docsResult.data;
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
        return this.findList(data, '');
      }
      return {
        count: Number(countResult.data),
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
