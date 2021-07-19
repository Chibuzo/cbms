const routes = require('express').Router();
const swaggerJsdoc = require('swagger-jsdoc');
const swaggerUi = require('swagger-ui-express');
const employeeRoutes = require('./employee');
const header_validation = require('../middlewares/header_validator');

routes.get('/', (req, res) => {
    res.status(200).json({ message: 'What are you looking for here??!!' });
});

routes.use('/employees', employeeRoutes);


// Swagger set up
const options = {
    swaggerDefinition: {
        openapi: '3.0.0',
        info: {
            title: "Star wars API",
            version: "1.0.0",
            description:
                "This application offers a simple REST api for listing starwars' movies, posting comment on movies and fetching the character set for a movie."
        },
        servers: [
            {
                url: "http://149.202.58.198:3000/api"
            },
            {
                url: "http://localhost:3000/api"
            }
        ]
    },
    apis: ['./docs/*.yml']
};
const specs = swaggerJsdoc(options);
routes.use("/docs", swaggerUi.serve);
routes.get("/docs", swaggerUi.setup(specs, { explorer: true }));

module.exports = routes;