const mariadbClient = require("./pgsql");

const fieldTypeMapping = {
  string: 1,
  number: 2,
  boolean: 7,
  date: 5,
};

const getTableNamesFromQuery = (query) => {
  const regex = /from\s+([a-zA-Z0-9_]+)/gi;
  let match;
  const tableNames = new Set();

  while ((match = regex.exec(query)) !== null) {
    tableNames.add(match[1]);
  }

  return Array.from(tableNames).join("-");
};

const getTableMeta = async (ip, port, dbName, sqlquery, username, password) => {
  const client = new mariadbClient(ip, port, username, password, dbName);

  try {
    const rows = await client.searchData(sqlquery);
    if (rows.length === 0) {
      console.log("集合中没有文档");
      return {
        tableName: getTableNamesFromQuery(sqlquery),
        fields: [],
        fieldMapping: {},
      };
    }

    const fieldTypes = {};
    Object.keys(rows[0]).forEach((fieldName) => {
      if (!fieldTypes[fieldName]) {
        fieldTypes[fieldName] = new Set();
      }
      let fieldType = typeof rows[0][fieldName];
      if (fieldType === "object" && rows[0][fieldName] instanceof Date) {
        fieldType = "date";
      }
      fieldTypes[fieldName].add(fieldType);
    });

    const fields = [];
    const fieldMapping = {};
    let fieldIdCounter = 1;
    let primaryKeySet = false;

    Object.keys(fieldTypes).forEach((fieldName, index) => {
      let types = fieldTypes[fieldName];
      let rawFieldType = "string";

      if (types.has("string")) {
        rawFieldType = "string";
      } else if (types.has("number")) {
        rawFieldType = "number";
      } else if (types.has("boolean")) {
        rawFieldType = "boolean";
      } else if (types.has("date")) {
        rawFieldType = "date";
      }

      let fieldType = fieldTypeMapping[rawFieldType];
      const fieldId = `fid_${fieldIdCounter++}`;
      const property = {};

      if (fieldType === 2) {
        property.formatter = "#,##0.00";
      } else if (fieldType === 5) {
        property.formatter = "yyyy/MM/dd";
      }

      fields.push({
        fieldId: fieldId,
        fieldName: fieldName,
        fieldType: fieldType,
        isPrimary: !primaryKeySet && (fieldName === "id" || index === 0),
        description: "",
        property: property,
      });
      fieldMapping[fieldName] = fieldId;

      if (!primaryKeySet && (fieldName === "id" || index === 0)) {
        primaryKeySet = true;
      }
    });

    console.log("字段信息:", fields);
    console.log("字段映射:", fieldMapping);

    return {
      tableName: getTableNamesFromQuery(sqlquery),
      fields: fields,
      fieldMapping: fieldMapping,
    };
  } catch (err) {
    console.log(ip, port, dbName, sqlquery, username, password);
    console.error("连接 MariaDB 失败", err);
    return {
      tableName: getTableNamesFromQuery(sqlquery),
      fields: [],
      fieldMapping: {},
      error: err.message,
    };
  } finally {
    await client.close();
    console.log("连接已断开");
  }
};

module.exports = { getTableMeta };
