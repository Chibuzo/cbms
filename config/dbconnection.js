const oracleDb = require('oracledb');
const { config } = require('../config/config');

oracleDb.outFormat = oracleDb.OUT_FORMAT_OBJECT;
oracleDb.initOracleClient({ libDir: 'C:\\nid\\instantclient_19_10' });

(async function () {
    try {
        await oracleDb.createPool({
            ...config[process.env.NODE_ENV],
            poolTimeout: 60
        });

        console.log('Connection pool started');
    } catch (err) {
        console.log(err)
        throw new Error('Unable to create connection pool');
    }
})();

const getConnection = async () => {
    try {
        return oracleDb.getConnection();
    } catch (err) {
        console.log(err)
        throw new Error('Unable to connect to database');
    }
}


module.exports = {
    getConnection
}