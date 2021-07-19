const express = require('express');
const app = express();
require('dotenv').config();
const apiRoutes = require('./routes');
const { handleError, ErrorHandler } = require('./helpers/errorHandler');

app.use(express.json());

app.get('/', (req, res) => {
    res.status(200).json({ message: 'Welcome to my api!' });
});

//app.use('/api', header_validation, apiRoutes);
app.use('/api', apiRoutes);

// catch 404 routes
app.use((req, res, next) => {
    throw new ErrorHandler(404, "Route not found!");
});


app.use((err, req, res, next) => {
    handleError(err, res);
});

app.set('port', process.env.PORT);

app.listen(app.get('port'), () => {
    console.log('App listening on port ' + process.env.PORT);
});