const routes = require('express').Router();
const { fetchEmployees, fetchEmployeesByGradeScale } = require('../services/employeeService');

routes.get('/', async (req, res, next) => {
    try {
        const { page, month, year } = req.query;
        const { employees, metadata } = await fetchEmployees(parseInt(page || 1, 10), month, year);
        res.status(200).json({ status: true, data: { employees, metadata } });
    } catch (err) {
        res.status(err.statusCode || 500).json({ status: false, message: err.message });
    }
});

routes.get('/gradescale', async (req, res, next) => {
    try {
        const employees = await fetchEmployeesByGradeScale();
        res.status(200).json({ status: true, data: { employees } });
    } catch (err) {
        res.status(err.statusCode || 500).json({ status: false, message: err.message });
    }
});

module.exports = routes;