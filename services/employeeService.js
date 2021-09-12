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
        select pay.full_name, per.sex "Gender", per.national_identifier "ID No", per.employee_number, ass.grade_id "Grade Code", grd.name "Grade Name",
        ele.element_type_id "Pay Code", ele.element_name "PayCode Name",ele.attribute1 "Cost Code" ,pay.credit_amount "Credit Amount", pay.debit_amount
        "Debit Amount", ass.effective_start_date "Promotion Date", ass.effective_start_date "End Period", ass.effective_start_date "Notching Month",
        ass.job_id "Job ID", job.name "Position Name",
        decode (ass.employment_category , 'PERMP', 'Permanent and Pensionable','CONP' , 'Contract and Pensionable','CON','Contract',
        'PEN','Pensioners','TEMP','Temporary') "Posiition Type", to_char(pay.effective_date, 'YYYY') "Payroll Year", to_char(pay.effective_date, 'MON') "Payroll Month",
        pay.concatenated_segments "CONCATENATED SEGMENT", substr(pay.segment2, 0,3) "Head",pay.segment4 "Account Type",
        substr(pay.segment2, 4,2) "Cost Centre",
        substr(pay.segment2, 6,2) "Sub Cost Centre",
        substr(pay.segment3, 4,2) "Programme",
        substr(pay.segment3, 6,2) "Sub Programme",
        pay.segment5 "Fund Source",
        pay.segment6 "Donor", pay.segment7 "Project Code",pay.segment8 "Activity",pay.segment9 "Economic Indicator",pay.segment10 "Location",
        pay.segment9 || '|' || substr(pay.segment2, 0,3) || '|' || substr(pay.segment2, 4,2) || '|' || substr(pay.segment3, 0,3) || '|' || substr(pay.segment3, 4,2)
        || '|' || pay.segment4 || '|' || pay.segment5 || '|' || pay.segment6 || '|' || pay.segment7 || '|' || pay.segment8 || '|' || pay.segment10 "GLAccount"
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