require('dotenv').config();

const config = {
    "development": {
        "user": process.env.DB_USER,
        "password": process.env.DB_PASSWORD,
        "connectString": `${process.env.DB_HOST}:${process.env.DB_PORT}/${process.env.DB_SERVICE}`,
    },
    "test": {
        "user": "root",
        "password": null,
        "connectString": "",
    },
    "production": {
        "user": process.env.DB_USER,
        "password": process.env.DB_PASSWORD,
        "connectString": `${process.env.DB_HOST}:${process.env.DB_PORT}/${process.env.DB_SERVICE}`,
    }
}

module.exports = { config };