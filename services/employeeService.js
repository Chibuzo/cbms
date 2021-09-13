const { getConnection } = require('../config/dbconnection');
const { ErrorHandler } = require('../helpers/errorHandler');

const fetchEmployees = async (page = 1, month, year) => {
    if (!Number.isInteger(page) || page < 1) {
        throw new ErrorHandler(400, 'Invalid page. Page must be a valid integer, greater 0');
    }

    const query_params = [];
    let date_query = '';

    if (month) {
        const MONTH = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
        if (!MONTH.includes(month.toUpperCase())) throw new ErrorHandler(400, 'Invalid month. Please use a 3 character prefix for months. Eg JAN');
        date_query += "AND to_char(pay.effective_date, 'MON') = :month ";
        query_params.push(month.toUpperCase());
    }
    if (year) {
        if (year.length !== 2) throw new ErrorHandler(400, 'Invalid year. Please use a 2 character year. Eg 21');
        date_query += "AND to_char(pay.effective_date, 'YY') = :year ";
        query_params.push(year);
    }
    const limit = 100;
    const offset = page === 1 ? 1 : (page - 1) * limit;
    query_params.push(offset, limit);

    const query = `
        select pay.full_name, per.sex "GENDER", per.national_identifier "ID_NO", per.employee_number, ass.grade_id "GRADE_CODE", grd.name "GRADE_NAME",
        ele.element_type_id "PAY_CODE", ele.element_name "PAYCODE_NAME",ele.attribute1 "COST_CODE",
        CASE WHEN pay.debit_amount = 0 THEN pay.credit_amount ELSE pay.debit_amount END AS AMOUNT,
        ass.effective_start_date "PROMOTION_DATE", ass.effective_start_date "END_PERIOD", ass.effective_start_date "NOTCHING_MONTH",
        ass.job_id "JOB_ID", job.name "POSITION_NAME",
        decode (ass.employment_category , 'PERMP', 'Permanent and Pensionable','CONP' , 'Contract and Pensionable','CON','Contract',
        'PEN','Pensioners','TEMP','Temporary') "POSITION_TYPE", to_char(pay.effective_date, 'YYYY') "PAYROLL_YEAR", to_char(pay.effective_date, 'MON') "PAYROLL_MONTH",
        pay.concatenated_segments "CONCATENATED_SEGMENT", substr(pay.segment2, 0,3) "HEAD", pay.segment4 "ACCOUNT_TYPE",
        substr(pay.segment2, 4,2) "COST_CENTRE",
        substr(pay.segment2, 6,2) "SUB_COST_CENTRE",
        substr(pay.segment3, 4,2) "PROGRAMME",
        substr(pay.segment3, 6,2) "SUB_PROGRAMME",
        pay.segment5 "FUND_SOURCE",
        pay.segment6 "DONOR", pay.segment7 "PROJECT_CODE",pay.segment8 "ACTIVITY", pay.segment9 "ECONOMIC_INDICATOR",pay.segment10 "LOCATION",
        pay.segment9 || '|' || substr(pay.segment2, 0,3) || '|' || substr(pay.segment2, 4,2) || '|' || substr(pay.segment3, 0,3) || '|' || substr(pay.segment3, 4,2)
        || '|' || pay.segment4 || '|' || pay.segment5 || '|' || pay.segment6 || '|' || pay.segment7 || '|' || pay.segment8 || '|' || pay.segment10 "GLACCOUNT"
        FROM pay_costing_details_v pay
        join per_all_people_f per on (pay.person_id = per.person_id)
        join per_all_assignments_f ass on (ass.assignment_id=pay.assignment_id)
        join pay_element_types_f ele on (ele.element_type_id=pay.element_type_id)
        join per_grades grd on (grd.grade_id = ass.grade_id)
        join per_jobs job on (job.job_id = ass.job_id)
        where pay.segment10 is not null ${date_query}
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`;

    const db = await getConnection();
    const [result, total_count] = await Promise.all([
        db.execute(query, query_params),
        countRecords(date_query, month, year)
    ]);
    const employees = result.rows;

    const metadata = {
        page,
        limit,
        page_count: result.rows.length,
        total_pages: Math.ceil(total_count / limit),
        total_count
    }

    if (!employees) throw new ErrorHandler(404, 'Employees not found');

    return { employees, metadata };
};

const countRecords = async (date_query, month, year) => {
    const query = `
        select COUNT(*) num
        FROM pay_costing_details_v pay
        join per_all_people_f per on (pay.person_id = per.person_id)
        join per_all_assignments_f ass on (ass.assignment_id=pay.assignment_id)
        join pay_element_types_f ele on (ele.element_type_id=pay.element_type_id)
        where pay.segment10 is not null ${date_query}`;

    const query_params = [];
    if (month) query_params.push(month.toUpperCase());
    if (year) query_params.push(year);

    const db = await getConnection();
    const result = await db.execute(query, query_params);
    const count = result.rows[0];
    return count.NUM;
}


module.exports = {
    fetchEmployees
}