const mariadb = require("mariadb");

class mariadbClient {
    pool;
    constructor(ip, port, username, password) {
        this.pool = mariadb.createPool({
            host: ip,
            database: "mysql", // 连接到默认的 mysql 数据库
            user: username, // 用户名
            password: password, // 密码
            port: parseInt(port), // 端口号
            connectionLimit: 20, // 连接池最大连接数
        });
    }

    // 关闭连接池
    async close() {
        await this.pool.end();
    }

    // 查询数据
    async searchData(sqlString) {
        let conn;
        try {
            conn = await this.pool.getConnection();
            const rows = await conn.query(sqlString);
            return rows;
        } catch (err) {
            console.error("Query error", err.stack);
            throw err;
        } finally {
            if (conn) conn.release(); // 释放连接回连接池
        }
    }

    // 获取所有数据库名称
    async getAllDatabases() {
        const sql = "SHOW DATABASES;";
        return await this.searchData(sql);
    }

    // 获取指定数据库下的所有表名
    async getTablesInDatabase(databaseName) {
        const sql = `SHOW TABLES FROM \`${databaseName}\`;`;
        return await this.searchData(sql);
    }
}

module.exports = mariadbClient;
