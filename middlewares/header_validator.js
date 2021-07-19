module.exports = (req, res, next) => {
    if (req.method.toLowerCase() == "post" && req.headers['content-type'] !== 'application/json') {
        res.status(400).json({ status: 'Error', message: 'Acceptable content-type header is missing' });
    }

    next();
}