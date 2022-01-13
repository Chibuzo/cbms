const jwt = require('jsonwebtoken');

module.exports = (req, res, next) => {
    try {
        if (!req.headers.authorization) return res.sendStatus(403);
        const token = req.headers.authorization.split(" ")[1];
        if (!token) return res.sendStatus(401); // Unauthorized

        jwt.verify(token, process.env.JWT_SECRET, (err, payload) => {
            if (err) {
                return res.sendStatus(403); // forbidden
            }
            req.user = payload;
            next();
        });
    } catch (err) {
        next(err);
    }
}