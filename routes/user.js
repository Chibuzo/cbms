const routes = require('express').Router();
// const { handleError } = require('../helpers/errorHandler');
const { post } = require('../helpers/APIRequest')(process.env.AUTH_URL);


routes.post('/login', async (req, res) => {
    try {
        const response = await post('/users/login', req.body);
        res.status(200).json({ status: true, data: response.data });
    } catch (err) {
        res.status(err.statusCode || 500).json({ status: false, message: err.message });
    }
});

routes.post('/register', async (req, res) => {
    try {
        const response = await post('/users/register', req.body);
        res.status(201).json({ status: true, data: response.data });
    } catch (err) {
        res.status(err.statusCode || 500).json({ status: false, message: err.message });
    }
});

module.exports = routes;