const routes = require('express').Router();
const { fetchEmployees } = require('../services/employeeService');

routes.get('/', async (req, res, next) => {
    try {
        const { employees, metadata } = await fetchEmployees(parseInt(req.query.page, 10));
        res.status(200).json({ status: true, data: { employees, metadata } });
    } catch (err) {
        console.log(err)
        res.status(err.statusCode || 500).json({ status: false, message: err.message });
    }
});

module.exports = routes;