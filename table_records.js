const mariadbClient = require("./pgsql");

const globalFieldsToConvert = new Set();

const getTableRecords = async (
  ip,
  port,
  dbName,
  sqlquery,
  pageToken,
  username,
  password,
  maxPageSize,
  fieldMapping,
) => {
  const client = new mariadbClient(ip, port, username, password, dbName);
  try {
    const pageSize = maxPageSize;
    pageToken = pageToken === "" ? 0 : Number(pageToken);
    const offset = pageToken * pageSize;


    const cleanQuery = sqlquery.replace(/;$/, '');

    const paginatedQuery = `${cleanQuery} LIMIT ${pageSize} OFFSET ${offset}`;
    const rows = await client.searchData(paginatedQuery);
    console.log("获取的行:", rows);

    if (!rows || rows.length === 0) {
      console.log("没有数据");
      return {
        nextPageToken: null,
        hasMore: false,
        records: [],
      };
    }

    const records = rows.map((row, index) => {
      let data = {};

      let primaryId = `record_${offset + index + 1}`;
      if (row.id) {
        primaryId = String(row.id);
      }

      Object.keys(row).forEach((key) => {
        let value = row[key];

        if (value instanceof Date) {

          value = value.getTime();
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

      return {
        primaryId: primaryId,
        data: filteredData,
      };
    });

    const totalRecordsQuery = `SELECT COUNT(*) as count FROM (${cleanQuery}) as total`;
    const totalRecordsResult = await client.searchData(totalRecordsQuery);
    const totalRecords = totalRecordsResult[0].count;
    const hasMore = offset + pageSize < totalRecords;
    pageToken = hasMore ? pageToken + 1 : null;
    let pageTokenString = String(pageToken);

    return {
      nextPageToken: pageTokenString,
      hasMore: hasMore,
      records: records,
    };
  } catch (err) {
    console.error("连接 MariaDB 失败", err);
    return {
      nextPageToken: null,
      hasMore: false,
      records: [],
      error: err.message,
    };
  } finally {
    await client.close();
    console.log("连接已断开");
  }
};

module.exports = { getTableRecords };
