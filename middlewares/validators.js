const { body, check, validationResult } = require('express-validator');

module.exports = {
    validate: (req, res, next) => {
        const raw_errors = validationResult(req);

        if (raw_errors.isEmpty()) {
            return next();
        }

        const errors = raw_errors.errors.map(err => ({ message: err.msg }));

        return res.status(422).json({ status: 'error', errors });
    },
}