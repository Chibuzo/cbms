const routes = require('express').Router();
const { fetchEmployees, fetchEmployeesByGradeScale } = require('../services/employeeService');
const verifyToken = require('../middlewares/verifyToken');


routes.get('/', verifyToken, async (req, res, next) => {
    try {
        const { page = 0, month, year, head } = req.query;
        const { employees, metadata } = await fetchEmployees(parseInt(page, 10), month, year, head);
        let data = { employees };
        if (metadata) data.metadata = metadata;
        res.status(200).json({ status: true, data });
    } catch (err) {
        res.status(err.statusCode || 500).json({ status: false, message: err.message });
    }
});

routes.get('/gradescale', verifyToken, async (req, res, next) => {
    try {
        const employees = await fetchEmployeesByGradeScale();
        res.status(200).json({ status: true, data: { employees } });
    } catch (err) {
        res.status(err.statusCode || 500).json({ status: false, message: err.message });
    }
});

module.exports = routes;