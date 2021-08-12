const { getConnection } = require('../config/dbconnection');
const { ErrorHandler } = require('../helpers/errorHandler');
const DB = process.env.DB;
const salary_element_name = 'CS Basic Salary';

const fetchEmployees = async (page = 1) => {
    if (!Number.isInteger(page) || page < 1) {
        throw new ErrorHandler(400, 'Invalid page. Page must be a valid integer, greater 0');
    }
    const limit = 10;
    const offset = page === 1 ? 1 : (page - 1) * limit;

    const query = `SELECT 
        ppf.employee_number,
        paf.assignment_id,
        ppf.full_name,
        ppa.EFFECTIVE_DATE,
        TP.PERIOD_NAME,
        CASE WHEN paf.EFFECTIVE_END_DATE = to_date('12/31/4712', 'MM/DD/YYYY') THEN
            concat(concat(concat(concat(extract(month from paf.EFFECTIVE_START_DATE), '/'), extract(day from paf.EFFECTIVE_START_DATE)), '/'),
            extract(year from sysdate))
        ELSE to_char(paf.EFFECTIVE_END_DATE)
        END NOTCHING_MONTH,
        pos.NAME POSITION_NAME,
        pos.POSITION_TYPE,
        o.NAME POSITION_CATEGORY,
        o.NAME ORGANIZATION,
        o.ATTRIBUTE5 MINISTRY,
        SUBSTR(pak.segment2, 4, 2) COST_CENTER,
        SUBSTR(pak.segment2, 6, 2) SUBCOST_CENTER,
        SUBSTR(pak.segment3, 4, 2) PROGRAMME,
        SUBSTR(pak.segment3, 6, 2) SUB_PROGRAMME
    FROM per_people_x ppf,
        per_assignments_x paf,
        pay_payroll_actions ppa,
        PER_TIME_PERIODS TP,
        hr_all_positions_f pos,
        per_grades g,
        hr_all_organization_units o,
        PAY_COST_ALLOCATIONS_F pca,
        PAY_COST_ALLOCATION_KEYFLEX pak
    WHERE ppf.person_id = paf.person_id
    AND TP.TIME_PERIOD_ID = PPA.TIME_PERIOD_ID
    AND pos.position_id = paf.position_id
    AND g.grade_id = paf.grade_id
    AND o.organization_id = paf.organization_id
    AND pca.assignment_id = paf.assignment_id
    AND pak.cost_allocation_keyflex_id = pca.cost_allocation_keyflex_id
    GROUP by ppf.full_name, paf.assignment_id, ppa.TIME_PERIOD_ID, effective_date, pos.name, pos.POSITION_TYPE, o.name,
    ppf.employee_number, ppf.person_id, TP.period_name, paf.EFFECTIVE_END_DATE, paf.EFFECTIVE_START_DATE, o.ATTRIBUTE5, pak.segment2, pak.segment3
    order by ppa.EFFECTIVE_DATE
    OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`;

    //  PER_ORG_STRUCTURE_ELEMENTS pose,
    // AND pose.organization_id_parent = paf.organization_id
    // person id: 1151
    // and ppf.employee_number = '18397'
    const db = await getConnection();
    const result = await db.execute(query, [offset, limit]);
    const _employees = result.rows;

    const employees = await Promise.all(_employees.map(async employee => {
        const details = await fetchSalaryDetails(employee.ASSIGNMENT_ID, db);
        delete employee.ASSIGNMENT_ID;

        const salaryObj = details.find(d => d.ELEMENT_NAME === salary_element_name);
        const salary = salaryObj ? Number(salaryObj.RESULT_VALUE) : 0;
        return {
            ...employee,
            BASIC_SALARY: salary,
            NET_SALARY: calculateTotalSalary(details, salary),
        }
    }));
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

const fetchSalaryDetails = async (assignment_id, db) => {
    // JOIN pay_payroll_actions ppa ON ppa.payroll_action_id = paa.payroll_action_id
    const sql = `
        SELECT distinct ety.element_type_id, ety.element_name, ety.element_information_category, rrv.result_value
        FROM pay_assignment_actions paa
            JOIN pay_run_results prr ON prr.assignment_action_id = paa.assignment_action_id
            JOIN pay_run_result_values rrv ON rrv.run_result_id = prr.run_result_id
            JOIN pay_element_types_f ety ON ety.element_type_id = prr.element_type_id
            JOIN pay_input_values_F i ON i.element_type_id = ety.element_type_id
        WHERE paa.assignment_id = :assignment_id AND i.name = 'Pay Value'`;

    const result = await db.execute(sql, [assignment_id]);
    return result.rows;
};

const calculateTotalSalary = (details, salary) => {
    return details.reduce((total, val) => {
        const result_value = Number(val.RESULT_VALUE);
        if (val.ELEMENT_INFORMATION_CATEGORY && val.ELEMENT_INFORMATION_CATEGORY.includes('EARNINGS')) return total + result_value;
        else if (val.ELEMENT_INFORMATION_CATEGORY && val.ELEMENT_INFORMATION_CATEGORY.includes('DEDUCTIONS')) return total - result_value;
        else return total;
    }, salary);
}

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
