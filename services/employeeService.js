const { getConnection } = require('../config/dbconnection');
const { ErrorHandler } = require('../helpers/errorHandler');
const DB = process.env.DB;


const fetchEmployees = async (page = 1) => {
    if (typeof page !== 'number' || page < 1) {
        throw new ErrorHandler(400, 'Invalid page. Page must be a valid integer, greater 0');
    }
    const limit = 1000;
    const offset = page === 1 ? 0 : (page - 1) * limit;

    const query = `SELECT 
    ppf.employee_number,
        ppf.full_name,
        ppa.EFFECTIVE_DATE,
        TP.PERIOD_NAME,
        sum(decode(ety.element_name, 'NSS Basic Salary', TO_NUMBER(rrv.result_value), 0)) Salary,
        CASE WHEN paf.EFFECTIVE_END_DATE = to_date('12/31/4712', 'MM/DD/YYYY') THEN
            concat(concat(concat(concat(extract(month from paf.EFFECTIVE_START_DATE), '/'), extract(day from paf.EFFECTIVE_START_DATE)), '/'), extract(year from sysdate))
        ELSE to_char(paf.EFFECTIVE_END_DATE)
        END NOTCHING_MONTH,
        pos.NAME POSITION_NAME,
        pos.POSITION_TYPE,
        o.NAME POSITION_CATEGORY,
        sum(decode(ety.element_name, 'NSS Pension Deduction', TO_NUMBER(rrv.result_value), 0)) PF_Employee
    FROM per_people_x ppf,
        per_assignments_x paf,
            pay_assignment_actions pas,
                pay_payroll_actions ppa,
                    pay_run_results rr,
                        pay_run_result_values rrv,
                            pay_element_types_f ety,
                                pay_input_values_F I,
                                    PER_TIME_PERIODS TP,
                                    hr_all_positions_f pos,
                                    per_grades g,
                                    hr_all_organization_units o
    WHERE ppf.person_id = paf.person_id
    AND paf.assignment_id = pas.assignment_id
    AND pas.assignment_action_id = rr.assignment_action_id
    AND ppa.payroll_action_id = pas.payroll_action_id
    AND rr.element_type_id = ety.element_type_id
    AND i.element_type_id = ety.element_type_id
    AND rrv.run_result_id = rr.run_result_id
    AND rrv.input_value_id = i.input_value_id
    AND TP.TIME_PERIOD_ID = PPA.TIME_PERIOD_ID
    AND i.name = 'Pay Value'
    AND pos.position_id = paf.position_id
    AND g.grade_id = paf.grade_id
    AND o.organization_id = paf.organization_id 
    group by ppf.full_name, ppa.TIME_PERIOD_ID, effective_date, pos.name, pos.POSITION_TYPE, o.name,
    ppf.employee_number, ppf.person_id, TP.period_name, paf.EFFECTIVE_END_DATE, paf.EFFECTIVE_START_DATE
    order by ppa.EFFECTIVE_DATE
    OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`;

    // person id: 1151
    // and ppf.employee_number = '18397'
    const db = await getConnection();
    // d.execute('SELECT COUNT(DISTINCT person_id) num FROM per_people_x pp')
    const result = await db.execute(query, [offset, limit]);
    const employees = result.rows;
    // console.log(employees)
    const metadata = {
        page,
        limit,
        page_count: result.rows.length,
        total_count: 45000
    }

    if (!employees) throw new ErrorHandler(404, 'Employees not found');

    return { employees, metadata };
};

const fetchPersonAssignement = async person_id => {
    const sql = `
        SELECT
            g.NAME grade,
            j.name job,
            ast.user_status assignment_type,
            p.name position,
            a.SUPERVISOR_ID,
            a.ASSIGNMENT_TYPE,
            a.ASSIGNMENT_NUMBER,
            a.EFFECTIVE_START_DATE,
            bg.TYPE business_group,
            pr.payroll_name,
            o.name organization,
            pg.group_name,
            l.description location
        FROM HR.PER_ALL_ASSIGNMENTS_F a
            LEFT OUTER JOIN HR.PER_GRADES g USING(GRADE_ID)
            LEFT JOIN HR.PER_JOBS j using(job_id)
            LEFT JOIN hR.PER_ASSIGNMENT_STATUS_TYPES ast using(ASSIGNMENT_STATUS_TYPE_ID)
            LEFT JOIN hr.HR_ALL_POSITIONS_F p using (position_id)
            LEFT JOIN HR.PER_NUMBER_GENERATION_CONTROLS bg USING(BUSINESS_GROUP_ID)
            LEFT JOIN HR.PAY_ALL_PAYROLLS_F pr USING(PAYROLL_ID)
            LEFT JOIN HR.HR_ALL_ORGANIZATION_UNITS o ON o.ORGANIZATION_ID = a.ORGANIZATION_ID
            LEFT JOIN HR.PAY_PEOPLE_GROUPS pg USING(PEOPLE_GROUP_ID)
            LEFT JOIN HR.HR_LOCATIONS_ALL l USING(LOCATION_ID)
        WHERE a.PERSON_ID = :person_id
        ORDER BY a.EFFECTIVE_END_DATE DESC
    `;

    const db = await getConnection();
    const result = await db.execute(sql, [person_id]);
    return result.rows[0];
};


module.exports = {
    fetchEmployees
}
