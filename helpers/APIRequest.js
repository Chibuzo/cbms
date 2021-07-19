const axios = require('axios');
const { ErrorHandler } = require('../helpers/errorHandler');

const APIRequest = api_url => {
    let option = {
        baseURL: api_url,
        headers: { 'Content-Type': 'application/json' }
    };

    const get = async (url, params = {}) => {
        option.params = params;
        try {
            const response = await axios.get(url, option);
            return response.data;
        } catch (err) {
            throw new ErrorHandler(err.response.status, err.response.data.detail);
        }
    }

    const post = (url, body = {}) => {
        try {
            const response = await axios.post(url, body, option);
            return response.data;
        } catch (err) {
            throw new ErrorHandler(err.response.status, err.response.data.detail);
        }
    }

    return {
        get,
        post
    }
}

module.exports = APIRequest;