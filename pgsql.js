const mariadb = require("mariadb");

class mariadbClient {
    pool;
    constructor(ip, port, username, password, dbname) {
        this.pool = mariadb.createPool({
            host: ip,
            database: dbname,
            user: username, 
            password: password,
            port: parseInt(port),
            connectionLimit: 20,
        });
    }

    async close() {
        await this.pool.end();
    }

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
            if (conn) conn.release(); 
        }
    }

    async getAllDatabases() {
        const sql = "SHOW DATABASES;";
        return await this.searchData(sql);
    }

    async getTablesInDatabase(databaseName) {
        const sql = `SHOW TABLES FROM \`${databaseName}\`;`;
        return await this.searchData(sql);
    }
}

module.exports = mariadbClient;
