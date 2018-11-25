// 数据模型基类
// by YuRonghui 2018-11-18
const { Query } = require('wood-query')();
const { Util } = require('wood-util')();
const cluster = require('cluster');
const mongodb = require('mongodb');
const ObjectId = mongodb.ObjectID;
const largelimit = 20000; //限制不能超过2万条数据返回
const _timeout = 0;
const _KeyTimeout = 60 * 1; //设置listkey过期时间，秒
const { catchErr, error } = WOOD;

class Model {
  constructor(opts = {}) {
    this._hasdata = false;
    this.tableName = opts.tableName || '';
    this.primarykey = opts.primarykey || '_id'; //默认主键名
    this.fields = opts.fields || {};
    this.select = opts.select || {};
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

  _init() {
    let fields = this.fields.data || {};
    for (let key in fields) {
      let item = fields[key];
      if (key == '_id') continue;
      // 建索引
      if (item.index) {
        let indexField = {};
        indexField[key] = item.index == 'text' ? item.index : 1 ;
        this.db.index(indexField);
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
  createIndex(opts = {}) {
    if (!Util.isEmpty(opts)) this.db.collection.ensureIndex(opts);
  }

  // 删除索引
  removeIndex(name) {
    if (name) this.db.collection.dropIndex(name);
  }

  // 重置数据
  resetData() {
    this.fields.resetData();
    this._hasdata = false;
  }

  // 设置数据
  setData(target, value) {
    this.fields.setData(target, value);
    this._hasdata = true;
  }

  // 获取模型数据
  getData(hasVirtualField = true) {
    return this.fields.getData(hasVirtualField);
  }

  // 是否新的
  isNew(data) {
    return !data[this.primarykey];
  }

  //新增数据
  async create(data = {}, addLock = true, hascheck = true) {
    if (!data) throw error('create方法的参数data不能为空');
    if(!Util.isEmpty(data)) this.setData(data);
    if (this.primarykey === 'rowid' || data.rowid == 0){
      let id = await this.db.rowid();
      this.setData('rowid', id);
      if (config.isDebug) console.warn('新增id: ', id);
    }
    let err = hascheck ? this.fields.validate() : false;
    if (err) throw error(err);
    const lock = addLock ? await catchErr(this.redis.lock()) : {data: 1};
    if (lock.data) {
      let result = await catchErr(this.db.create(this.getData()));
      if(addLock) this.redis.unlock(lock.data);
      if(result.err) throw error(result.err);
      return result.data;
    }else{
      throw error(lock.err);
    }
  }

  // 更新数据
  async update(data = {}, addLock = true, hascheck = true, isFindOneAndUpdate) {
    if (!data) throw error('update方法的参数data不能为空');
    if(!Util.isEmpty(data)) this.setData(data);
    if (!this.isNew()) {
      let err = hascheck ? this.fields.validate() : false,
        hasSet = false,
        id = data[this.primarykey];
      if (err) {
        throw error(err);
      } else {
        let lock = addLock ? await catchErr(this.redis.lock()) : {data: 1};
        if (lock.data) {
          delete data[this.primarykey];
          let keys = Object.keys(data),
            method = isFindOneAndUpdate ? 'findOneAndUpdate' : 'update',
            idObj = {};
          hasSet = keys[0].indexOf('$') === 0;
          idObj[this.primarykey] = id;
          const result = await catchErr(this.db[method](idObj, hasSet ? data : { $set: data }));
          if(addLock) this.redis.unlock(lock.data);
          if (result.data){
            return isFindOneAndUpdate ? result.data : idObj;
          }else{
            throw error(result.err);
          }
        }else{
          throw error(lock.err);
        }
      }
    }
    throw error(false);
  }
  // 更新数据, 结果返回当前记录
  async findOneAndUpdate(data = {}, addLock = true, hascheck = true) {
    return this.update(data, addLock, hascheck, true);
  }

  // 保存数据
  async save() {
    let data = this.getData(false);
    if (Util.isEmpty(data) || !data) throw error('save方法的data为空');
    if (!this.isNew()) {
      const updateOk = await catchErr(this.update());
      if (updateOk.err) throw error(updateOk.err);
      return updateOk.data;
    } else {
      const result = await catchErr(this.create());
      if (result.err) throw error(result.err);
      return result.data;
    }
  }

  //删除数据
  async remove(data) {
    if (!data) return false;
    const lock = await catchErr(this.redis.lock());
    if (lock.err) {
      throw error(lock.err);
    }else{
      return this.db.remove(data);
    }
  }

  //清空数据
  async clear() {
    const lock = await catchErr(this.redis.lock());
    if (lock.err) {
      throw error(lock.err);
    }else{
      return this.db.clear();
    }
  }

  // 执行查询
  exec(oper = 'find', data) {
    if (config.isDebug) console.warn(`data ${oper}: ${JSON.stringify(data)}`);
    if (this.db[oper]) {
      if (data.aggregate.length) {
        return this.db.aggregate(data.aggregate);
      } else {
        return this.db[oper](data);
      }
    }
    return error(error_code.error_nodata);
  }

  // 查询单条记录
  async findOne(data, addLock = true) {
    const hasLock = addLock ? await catchErr(this.redis.hasLock()) : {};
    if(hasLock.err){
      throw error(hasLock.err);
    }else{
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
        if(result.err){
          throw error(result.err);
        }else{
          return Array.isArray(result.data) ? result.data[0] : result.data;
        }
      } else {
        await new Promise((resolve, reject) => {
          setTimeout(() => {
            resolve(true);
          }, _timeout);
        });
        return this.findOne(data, addLock);
      }
    }
  }

  // 查询数据列表
  async findList(data, cacheKey = '', addLock = true) {
    if (!data) throw error('findList方法参数data不能为空');
    let hasLock = addLock ? await catchErr(this.redis.hasLock()) : {};
    if(hasLock.err){
      throw error(hasLock.err);
    }else{
      if (!hasLock.data) {
        let hasKey = false, largepage = 1;
        let query = data._isQuery ? data : Query(data);
        let _data = query.toJSON();
        let limit = _data.limit == undefined ? 20 : Number(_data.limit),
          page = _data.page || 1,
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
        if (!isEmpty(this.select)) query.select(this.select);
        if (!isEmpty(this.relation)) query.populate(this.relation);
        let counts = this.db.count(query),
          lists = this.exec('find', query.toJSON());
        const countResult = await catchErr(counts);
        const docsResult = await catchErr(lists);
        if (docsResult.err || countResult.err) {
          throw error(docsResult.err || countResult.err);
        }else{
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
            return this.findList(data, '', addLock);
          }
          return {
            count: Number(countResult.data),
            list: docs || []
          };
        }
      }else{
        await new Promise((resolve, reject) => {
          setTimeout(() => {
            resolve(true);
          }, _timeout);
        });
        return this.findList(data, cacheKey, addLock);
      }
    }
  }
}

module.exports = Model;
