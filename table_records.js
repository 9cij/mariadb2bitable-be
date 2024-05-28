const mongoose = require("mongoose");
const { ObjectId } = mongoose.Types;


const globalFieldsToConvert = new Set();

const getTableRecords = async (
  ip,
  port,
  dbName,
  tableName,
  pageToken,
  username,
  password,
  maxPageSize,
  fieldMapping,
) => {
  let connection;
  try {

    const uri = `mongodb://${username}:${password}@${ip}:${port}/${dbName}?authSource=admin`;

    connection = await mongoose.connect(uri, {
      serverSelectionTimeoutMS: 5000,
    });
    console.log("已连接到 MongoDB", pageToken);


    const db = mongoose.connection.db;


    const pageSize = maxPageSize;


    pageToken = pageToken === "" ? 0 : Number(pageToken);
    const skip = pageToken * pageSize;


    const documents = await db
      .collection(tableName)
      .find()
      .skip(skip)
      .limit(pageSize)
      .toArray();
    console.log("获取的文档:", documents);


    if (!documents || documents.length === 0) {
      console.log("集合中没有文档");
      return {
        nextPageToken: null,
        hasMore: false,
        records: [],
      };
    }


    documents.forEach((doc) => {
      Object.keys(doc).forEach((key) => {
        if (typeof doc[key] === "string") {
          globalFieldsToConvert.add(key);
        }
      });
    });


    const records = documents.map((doc, index) => {
      let data = {};


      let primaryId = `record_${skip + index + 1}`;
      if (doc._id) {
        primaryId = doc._id.toHexString();
      }

      Object.keys(doc).forEach((key) => {
        let value = doc[key];


        if (value instanceof ObjectId) {
          value = value.toHexString();
        }


        if (typeof value === "object" && value !== null) {
          if (Array.isArray(value)) {
            value = { ids: value, idType: "UnknownType" };
          } else if (value.latitude && value.longitude) {
            value = { latitude: value.latitude, longitude: value.longitude };
          }
        }


        if (globalFieldsToConvert.has(key)) {
          value = String(value);
        }


        const fieldId = fieldMapping[key];
        if (fieldId) {
          data[fieldId] = value;
        }
      });


      const filteredData = Object.fromEntries(
        Object.entries(data).filter(([_, v]) => v !== undefined),
      );

      console.log({
        primaryId: primaryId,
        data: filteredData,
      });

      return {
        primaryId: primaryId,
        data: filteredData,
      };
    });


    const totalRecords = await db.collection(tableName).countDocuments();
    const hasMore = skip + pageSize < totalRecords;
    pageToken = hasMore ? pageToken + 1 : null;
    let pageTokenString = String(pageToken);


    return {
      nextPageToken: pageTokenString,
      hasMore: hasMore,
      records: records,
    };
  } catch (err) {
    console.error("连接 MongoDB 失败", err);
    return {
      nextPageToken: null,
      hasMore: false,
      records: [],
      error: err.message,
    };
  } finally {
    if (connection) {

      await mongoose.disconnect();
      console.log("连接已断开");
    }
  }
};


module.exports = { getTableRecords };
