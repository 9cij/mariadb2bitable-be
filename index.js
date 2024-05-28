const express = require("express");
const mongoose = require("mongoose");
const path = require("path");
const fs = require("fs");
const os = require("os");
const axios = require("axios");
const low = require("lowdb");
const FileSync = require("lowdb/adapters/FileSync");
const mariadbClient = require("./pgsql");

const { getTableMeta } = require("./table_meta.js");
const { getTableRecords } = require("./table_records.js");
const { judgeEncryptSignValid } = require("./request_sign.js");

const app = express();

app.use(express.json());

const adapter = new FileSync("db.json");
const db = low(adapter);

db.defaults({ records: [] }).write();

app.get("/", async (req, res) => {
    const serverIp = getServerIpAddress();
    const publicIp = await getPublicIpAddress();

    res.send(`
      <h1>hello world</h1><br>
      服务器运行的公网IP为: ${publicIp}，需告知用户将其添加进白名单`);
});

function getServerIpAddress() {
    const interfaces = os.networkInterfaces();
    let serverIp = "";

    for (const interfaceName in interfaces) {
        const addresses = interfaces[interfaceName];
        for (const address of addresses) {
            if (address.family === "IPv4" && !address.internal) {
                serverIp = address.address;
                break;
            }
        }
        if (serverIp) {
            break;
        }
    }

    return serverIp;
}

async function getPublicIpAddress() {
    try {
        const response = await axios.get("https://api.ipify.org/?format=json");
        return response.data.ip;
    } catch (error) {
        console.error("未能获取公网IP地址:", error);
        return null;
    }
}

app.get("/meta.json", (req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");

    fs.readFile(
        path.join(__dirname, "./public/meta.json"),
        "utf8",
        (err, data) => {
            res.set("Content-Type", "application/json");
            res.status(200).send(data);
        },
    );
});

app.post("/api/table_meta", async (req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");

    if (!judgeEncryptSignValid(req)) {
        return res.status(401).json({
            code: 1,
            message: "加密判断结果失败，请检查token",
        });
    }

    const { params } = req.body;
    const parsedParams = JSON.parse(params);
    const datasourceConfigString = parsedParams.datasourceConfig;
    const { loginAccount, loginPassword } = JSON.parse(datasourceConfigString);

    try {
        console.log("table_meta的请求数据", req.body);

        const userRecord = db
            .get("records")
            .find({ loginAccount, loginPassword })
            .value();

        if (!userRecord) {
            return res.status(404).json({
                code: 1,
                message: "用户未找到",
            });
        }

        const {
            IP: ip,
            PORT: port,
            DBNAME: dbName,
            TABLENAME: tableName,
            UNAME: username,
            PWD: password,
        } = userRecord;

        const metaData = await getTableMeta(
            ip,
            port,
            dbName,
            tableName,
            username,
            password,
        );
        console.log("重要", metaData);

        const { fieldMapping } = metaData;
        db.get("records")
            .find({ loginAccount, loginPassword })
            .assign({ fieldMapping })
            .write();

        const result = {
            code: 0,
            message: "POST请求成功",
            data: {
                tableName: metaData.tableName,
                fields: metaData.fields,
            },
        };

        res.status(200).json(result);
    } catch (error) {
        res.status(500).json({
            code: 1,
            message: "服务器错误",
            error: error.message,
        });
    }
});

app.post("/api/records", async (req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");

    if (!judgeEncryptSignValid(req)) {
        return res.status(401).json({
            code: 1,
            message: "加密判断结果失败，请检查token",
        });
    }

    const { params } = req.body;
    const { pageToken = "0", maxPageSize = 1000 } = JSON.parse(params);
    const parsedParams = JSON.parse(params);
    const datasourceConfigString = parsedParams.datasourceConfig;
    const { loginAccount, loginPassword } = JSON.parse(datasourceConfigString);

    try {
        console.log("table_records 的请求数据", req.body);

        const userRecord = db
            .get("records")
            .find({ loginAccount, loginPassword })
            .value();

        if (!userRecord) {
            return res.status(404).json({
                code: 1,
                message: "用户未找到",
            });
        }

        console.log("查询到的用户记录", userRecord);

        const { IP, PORT, DBNAME, TABLENAME, UNAME, PWD, fieldMapping } =
            userRecord;

        const ip = IP;
        const port = PORT;
        const dbName = DBNAME;
        const tableName = TABLENAME;
        const username = UNAME;
        const password = PWD;

        const recordsData = await getTableRecords(
            ip,
            port,
            dbName,
            tableName,
            pageToken,
            username,
            password,
            maxPageSize,
            fieldMapping,
        );

        console.log("重要", recordsData, pageToken);
        const result = { code: 0, message: "POST请求成功", data: recordsData };

        res.status(200).json(result);
    } catch (error) {
        console.log(error);
        res.status(500).json({
            code: 1,
            message: "服务器错误",
            error: error.message,
        });
    }
});

app.get("/preset", (req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    const {
        ip,
        port,
        dbName,
        sqlQuery,
        username,
        password,
        loginAccount,
        loginPassword,
    } = req.query;
    console.log("接收到的参数", {
        ip,
        port,
        dbName,
        sqlQuery,
        username,
        password,
        loginAccount,
        loginPassword,
    });

    if (
        !ip ||
        !port ||
        !dbName ||
        !sqlQuery ||
        !loginAccount ||
        !loginPassword
    ) {
        return res.status(400).json({ code: 1, message: "缺少必要的参数" });
    }

    const timestamp = new Date().toISOString();

    const existingRecord = db
        .get("records")
        .find({ loginAccount, loginPassword })
        .value();

    if (existingRecord) {
        db.get("records")
            .find({ loginAccount, loginPassword })
            .assign({
                IP: ip,
                PORT: port,
                UNAME: username,
                PWD: password,
                DBNAME: dbName,
                SQLQuery: sqlQuery,
                TIME: timestamp,
            })
            .write();

        res.status(200).json({
            code: 0,
            message: "用户数据已更新",
            data: {
                ip,
                port,
                username,
                password,
                dbName,
                sqlQuery,
                loginAccount,
                loginPassword,
            },
        });
    } else {
        db.get("records")
            .push({
                IP: ip,
                PORT: port,
                UNAME: username,
                PWD: password,
                DBNAME: dbName,
                SQLQuery: sqlQuery,
                loginAccount: loginAccount,
                loginPassword: loginPassword,
                TIME: timestamp,
            })
            .write();

        res.status(200).json({
            code: 0,
            message: "用户数据已创建",
            data: {
                ip,
                port,
                username,
                password,
                dbName,
                sqlQuery,
                loginAccount,
                loginPassword,
            },
        });
    }
});

app.get("/connectDB", async (req, res) => {
    const { ip, port, username, password } = req.query;
    res.setHeader("Access-Control-Allow-Origin", "*");

    async function listDatabasesAndCollections(ip, port, username, password) {
        const db = new mariadbClient(ip, port, username, password);

        try {
            const databasesResult = await db.getAllDatabases();
            const databases = databasesResult.map((row) => row.Database);

            const databaseTables = {};
            for (const database of databases) {
                const tablesResult = await db.getTablesInDatabase(database);
                const tables = tablesResult.map((row) => Object.values(row)[0]);
                databaseTables[database] = tables;
            }

            return { databases: databaseTables };
        } catch (err) {
            console.error("发生错误:", err);
            throw err;
        } finally {
            await db.close();
            console.log("连接已断开");
        }
    }

    try {
        const result = await listDatabasesAndCollections(
            ip,
            port,
            username,
            password,
        );
        res.json(result);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get("/userdata", async (req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");

    const { loginAccount, loginPassword } = req.query;
    console.log("接收到的参数", { loginAccount, loginPassword });

    const existingRecord = db
        .get("records")
        .find({ loginAccount, loginPassword })
        .value();

    if (existingRecord) {
        res.status(200).json({
            code: 1,
            message: "用户已存在",
            data: existingRecord,
        });
    } else {
        res.status(200).json({
            code: 0,
            message: "用户不存在",
            data: {},
        });
    }
});

app.listen(3000, () => {
    console.log("Express server initialized");
});
