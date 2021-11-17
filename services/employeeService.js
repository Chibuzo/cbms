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
    const limit = 500;
    const offset = page === 1 ? 0 : ((page - 1) * limit) + 1;
    query_params.push(offset, limit);

    const query = `
        SELECT DISTINCT per.employee_number,
        pay.full_name, per.sex "GENDER", per.national_identifier "ID_NO", ass.grade_id "GRADE_CODE", grd.name "GRADE_NAME",
        spi.sequence "NOTCH",
        ele.element_type_id "PAY_CODE", ele.element_name "PAYCODE_NAME",ele.attribute1 "COST_CODE",
        CASE WHEN pay.debit_amount = 0 THEN pay.credit_amount ELSE pay.debit_amount END AS AMOUNT,
        to_char(per.original_date_of_hire, 'DD-MON') || '-' || extract(year from sysdate) "PROMOTION_DATE",
        to_char(pay.effective_date, 'DD/MM/YYYY') "END_PERIOD",
        to_char(per.original_date_of_hire, 'MON') "NOTCHING_MONTH",
        ass.job_id "JOB_ID", job.name "POSITION_NAME",
        decode (ass.employment_category , 'PERMP', 'Permanent and Pensionable','CONP' , 'Contract and Pensionable','CON','Contract',
        'PEN','Pensioners','TEMP','Temporary') "POSITION_TYPE", to_char(pay.effective_date, 'YYYY') "PAYROLL_YEAR", to_char(pay.effective_date, 'MM') "PAYROLL_MONTH",
        substr(pay.segment2, 0,3) "HEAD", pay.segment4 "ACCOUNT_TYPE",
        substr(pay.segment2, 4,2) "COST_CENTRE",
        substr(pay.segment2, 6,2) "SUB_COST_CENTRE",
        substr(pay.segment3, 4,2) "PROGRAMME",
        substr(pay.segment3, 6,2) "SUB_PROGRAMME",
        pay.segment5 "FUND_SOURCE",
        pay.segment6 "DONOR", pay.segment7 "PROJECT_CODE",pay.segment8 "ACTIVITY", pay.segment9 "ECONOMIC_INDICATOR",pay.segment10 "LOCATION",
        pay.segment9 || '|' || substr(pay.segment2, 0,3) || '|' || pay.segment4 || '|' || substr(pay.segment2, 4,2) || '|' || substr(pay.segment2, 6,2) || '|' || substr(pay.segment3, 6,2) || '|' || substr(pay.segment3, 4,2)
        || '|' || pay.segment5 || '|' || pay.segment6 || '|' || pay.segment7 || '|' || pay.segment8 || '|' || pay.segment10 "GLACCOUNT"
        FROM per_all_people_f per
        join per_all_assignments_f ass on (per.person_id=ass.person_id)
        join pay_costing_details_v pay on (pay.person_id=ass.person_id)
        join per_all_assignments_f ass on (ass.assignment_id=pay.assignment_id)
        join pay_element_types_f ele on (ele.element_type_id=pay.element_type_id)
        join per_grades grd on (grd.grade_id = ass.grade_id)
        join per_jobs job on (job.job_id = ass.job_id)
        join per_spinal_point_placements_f spn on (spn.assignment_id = ass.assignment_id)
        join per_spinal_point_steps_f spi on (spi.step_id = spn.step_id)
        WHERE pay.segment10 is not null ${date_query} AND ele.attribute1 like '1%'
        AND ass.EFFECTIVE_END_DATE > sysdate
        AND per.effective_end_date > sysdate
        AND spn.effective_end_date > sysdate 
        ORDER BY per.employee_number 
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
        FROM
        (SELECT DISTINCT per.employee_number,
        pay.full_name, ass.grade_id "GRADE_CODE", grd.name "GRADE_NAME",
        spi.sequence "NOTCH",
        ele.element_type_id "PAY_CODE", ele.element_name "PAYCODE_NAME",ele.attribute1 "COST_CODE",
        CASE WHEN pay.debit_amount = 0 THEN pay.credit_amount ELSE pay.debit_amount END AS AMOUNT,
        to_char(per.original_date_of_hire, 'DD-MON') || '-' || extract(year from sysdate) "PROMOTION_DATE",
        to_char(pay.effective_date, 'DD/MM/YYYY') "END_PERIOD",
        to_char(per.original_date_of_hire, 'MON') "NOTCHING_MONTH",
        ass.job_id "JOB_ID", job.name "POSITION_NAME",
        decode (ass.employment_category , 'PERMP', 'Permanent and Pensionable','CONP' , 'Contract and Pensionable','CON','Contract',
        'PEN','Pensioners','TEMP','Temporary') "POSITION_TYPE", to_char(pay.effective_date, 'YYYY') "PAYROLL_YEAR", to_char(pay.effective_date, 'MM') "PAYROLL_MONTH",
        substr(pay.segment2, 0,3) "HEAD", pay.segment4 "ACCOUNT_TYPE",
        substr(pay.segment2, 4,2) "COST_CENTRE",
        substr(pay.segment2, 6,2) "SUB_COST_CENTRE",
        substr(pay.segment3, 4,2) "PROGRAMME",
        substr(pay.segment3, 6,2) "SUB_PROGRAMME",
        pay.segment5 "FUND_SOURCE",
        pay.segment6 "DONOR", pay.segment7 "PROJECT_CODE",pay.segment8 "ACTIVITY", pay.segment9 "ECONOMIC_INDICATOR",pay.segment10 "LOCATION",
        pay.segment9 || '|' || substr(pay.segment2, 0,3) || '|' || pay.segment4 || '|' || substr(pay.segment2, 4,2) || '|' || substr(pay.segment2, 6,2) || '|' || substr(pay.segment3, 6,2) || '|' || substr(pay.segment3, 4,2)
        || '|' || pay.segment5 || '|' || pay.segment6 || '|' || pay.segment7 || '|' || pay.segment8 || '|' || pay.segment10 "GLACCOUNT"
        FROM per_all_people_f per
        join per_all_assignments_f ass on (per.person_id=ass.person_id)
        join pay_costing_details_v pay on (pay.person_id=ass.person_id)
        join per_all_assignments_f ass on (ass.assignment_id=pay.assignment_id)
        join pay_element_types_f ele on (ele.element_type_id=pay.element_type_id)
        join per_grades grd on (grd.grade_id = ass.grade_id)
        join per_jobs job on (job.job_id = ass.job_id)
        join per_spinal_point_placements_f spn on (spn.assignment_id = ass.assignment_id)
        join per_spinal_point_steps_f spi on (spi.step_id = spn.step_id)
        WHERE pay.segment10 is not null ${date_query} AND ele.attribute1 like '1%'
        AND ass.EFFECTIVE_END_DATE > sysdate
        AND per.effective_end_date > sysdate
        AND spn.effective_end_date > sysdate)`;

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