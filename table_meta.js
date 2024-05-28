const mongoose = require("mongoose");
const { ObjectId } = mongoose.Types;

const fieldTypeMapping = {
  string: 1,
  number: 2,
  boolean: 7,
  date: 5,
  ObjectId: 1,
};

const getTableMeta = async (ip, port, dbName, tableName, username, password) => {
  try {

    const uri = `mongodb://${username}:${password}@${ip}:${port}/${dbName}?authSource=admin`;


    await mongoose.connect(uri, {
      serverSelectionTimeoutMS: 5000,
    });
    console.log("已连接到 MongoDB");


    const db = mongoose.connection.db;


    const allDocuments = await db.collection(tableName).find().toArray();
    console.log("所有文档:", allDocuments);

    if (allDocuments.length === 0) {
      console.log("集合中没有文档");
      return {
        tableName: tableName,
        fields: [],
        fieldMapping: {},
      };
    }


    const fieldTypes = {};


    allDocuments.forEach((doc) => {
      Object.keys(doc).forEach((fieldName) => {
        if (!fieldTypes[fieldName]) {
          fieldTypes[fieldName] = new Set();
        }
        let fieldType = typeof doc[fieldName];
        if (doc[fieldName] instanceof ObjectId) {
          fieldType = "ObjectId";
        } else if (fieldType === "object" && doc[fieldName] instanceof Date) {
          fieldType = "date";
        }
        fieldTypes[fieldName].add(fieldType);
      });
    });


    const fields = [];
    const fieldMapping = {};
    let fieldIdCounter = 1;


    if (fieldTypes["_id"]) {
      fields.push({
        fieldId: `fid_${fieldIdCounter}`,
        fieldName: "_id",
        fieldType: fieldTypeMapping["ObjectId"],
        isPrimary: true,
        description: "",
        property: {},
      });
      fieldMapping["_id"] = `fid_${fieldIdCounter++}`;
      delete fieldTypes["_id"];
    }


    Object.keys(fieldTypes).forEach((fieldName) => {
      let types = fieldTypes[fieldName];
      let rawFieldType = "string";

      // 优先选择非 undefined 类型
      if (types.has("string")) {
        rawFieldType = "string";
      } else if (types.has("number")) {
        rawFieldType = "number";
      } else if (types.has("boolean")) {
        rawFieldType = "boolean";
      } else if (types.has("date")) {
        rawFieldType = "date";
      } else if (types.has("ObjectId")) {
        rawFieldType = "ObjectId";
      }

      let fieldType = fieldTypeMapping[rawFieldType];
      const fieldId = `fid_${fieldIdCounter++}`;
      fields.push({
        fieldId: fieldId,
        fieldName: fieldName,
        fieldType: fieldType,
        isPrimary: false,
        description: "",
        property: fieldType === 2 ? { formatter: "#,##0.00" } : {}, 
      });
      fieldMapping[fieldName] = fieldId;
    });

    console.log("字段信息:", fields);
    console.log("字段映射:", fieldMapping);


    return {
      tableName: tableName,
      fields: fields,
      fieldMapping: fieldMapping,
    };
  } catch (err) {
    console.log(ip, port, dbName, tableName, username, password);

    console.error("连接 MongoDB 失败", err);
    return {
      tableName: tableName,
      fields: [],
      fieldMapping: {},
      error: err.message,
    };
  } finally {

    await mongoose.disconnect();
    console.log("连接已断开");
  }
};


module.exports = { getTableMeta };
