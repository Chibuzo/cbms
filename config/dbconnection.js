const oracleDb = require('oracledb');
const { config } = require('../config/config');

oracleDb.outFormat = oracleDb.OUT_FORMAT_OBJECT;

const db = (() => {
    let connection;

    return {
        async getConnection() {
            try {
                if (!connection) {
                    oracleDb.initOracleClient({ libDir: 'C:\\oracle\\instantclient_19_10' });
                    connection = await oracleDb.getConnection(config[process.env.NODE_ENV]);
                    console.log('Connected!');
                }
                return connection;
            } catch (err) {
                console.log(err);
                return null;
            }
        }
    }
})();


module.exports = {
    getConnection: db.getConnection
}