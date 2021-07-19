const { getConnection } = require('../config/dbconnection');

module.exports = {
    fetch: async (db_table, fields = '*', criteria, params = [], options) => {
        const db = await getConnection();
        const colums = Array.isArray(fields) ? fields.join(', ') : fields;
        const query = `SELECT ${colums} FROM ${db_table}`;
        if (criteria) query += ` WHERE ${criteria},`
        const result = await db.execute(query, params, options);
        return result.rows;
    }
}
    < dataTemplate name = "LST_PAYSLIP_XML" >
	<properties>
		<property name="xml_tag_case" value="upper"/>
	</properties>
	<parameters>
	<parameter name="P_PERIOD" dataType="character"/>
	<parameter name="P_EMPLOYEE_NUMBER" dataType="character"/>
	</parameters>
	<lexicals>
	</lexicals>
	<dataQuery>
	<sqlStatement name="Q_PERSONAL">
		  <![CDATA[ 
  SELECT PAP.FULL_NAME,
         to_char(SYSDATE,'DD-MON-YY') TODAY_DATE,
         PAP.ATTRIBUTE1 STAFF_NUMBER,
         pap.NATIONAL_IDENTIFIER,
         (SELECT jb.name
            FROM APPS.per_jobs_tl jb
           WHERE jb.job_id = pass.job_id AND ROWNUM = 1)
            Job_name,
         PAP.ATTRIBUTE2 EMPLOYEE_REFERENCE,
		 to_char(PAP.EFFECTIVE_START_DATE,'DD-MON-YY') Employment_date,
		 to_char(PAP.DATE_OF_BIRTH,'DD-MON-YY') DATE_OF_BIRTH,
         (SELECT NAME DEPT
            FROM APPS.HR_ORGANIZATION_UNITS_V hao
           WHERE hao.ORGANIZATION_ID = pass.ORGANIZATION_ID AND ROWNUM = 1)
            ORGANIZATION,
         (SELECT LOCATION_CODE
            FROM APPS.HR_LOCATIONS_ALL hao
           WHERE hao.location_ID = pass.location_ID           --AND ROWNUM = 1
                                                   )
            LOCATION_CODE,
         (SELECT PEA.SEGMENT1 || '-' || SEGMENT3 || ':' || SEGMENT5 bank
            FROM --APPS.CE_BANK_BRANCHES_V CBB,
                 APPS.PAY_PERSONAL_PAYMENT_METHODS_F PPPMF,
                 APPS.PER_ALL_ASSIGNMENTS_F PAAF,
                 APPS.PAY_ORG_PAYMENT_METHODS_F POPMF,
                 APPS.PAY_EXTERNAL_ACCOUNTS PEA
           WHERE     1 = 1
                 --and PPPMF.PRIORITY = 1
                 AND PPPMF.ASSIGNMENT_ID = PAAF.ASSIGNMENT_ID
                 AND PPPMF.EXTERNAL_ACCOUNT_ID = PEA.EXTERNAL_ACCOUNT_ID
                 AND PPPMF.ORG_PAYMENT_METHOD_ID = POPMF.ORG_PAYMENT_METHOD_ID
                 AND PAAF.ASSIGNMENT_NUMBER IS NOT NULL
                 AND PAAF.ASSIGNMENT_ID = pass.ASSIGNMENT_ID
                 AND ROWNUM = 1)
            EMPLOYEE_BANK,
         (SELECT    ADDRESS_LINE_1
                 || ' '
                 || ADDRESS_LINE_2
                 || ' '
                 || ADDRESS_LINE_3
                 || ' '
                 || TOWN_OR_CITY
                 || ' '
                 || REGION_1
                 || ' '
                 || REGION_2
                 || ' '
                 || COUNTRY
                    ORG_LOCATION_ADDRESS
            FROM APPS.HR_LOCATIONS_ALL hao
           WHERE hao.location_ID = pass.location_ID           --AND ROWNUM = 1
                                                   )
            LOCATION_ADDRESS,
         ------------------
         ---------------------
         ------------
         PAP.EMPLOYEE_NUMBER,
         PAP.PERSON_ID,
         PAP.LAST_NAME,
         PAP.FIRST_NAME,
         PAP.MIDDLE_NAMES,
         pap.sex gender,
         PAP.EMAIL_ADDRESS,
         (SELECT COST_ALLOCATION_KEYFLEX_ID
            FROM APPS.HR_ORGANIZATION_UNITS_V hao
           WHERE hao.ORGANIZATION_ID = pass.ORGANIZATION_ID AND ROWNUM = 1)
            COST_ALLOCATION,
         (SELECT ORGANIZATION_TYPE
            FROM APPS.HR_ORGANIZATION_UNITS_V hao
           WHERE hao.ORGANIZATION_ID = pass.ORGANIZATION_ID AND ROWNUM = 1)
            ORGANIZATION_TYPE,
         (SELECT PAYROLL_NAME
            FROM APPS.pay_payrolls_f X
           WHERE     X.PAYROLL_ID = pass.PAYROLL_ID
                 AND X.EFFECTIVE_END_DATE >= SYSDATE)
            Payroll_group,
         (SELECT USER_PERSON_TYPE
            FROM APPS.per_person_types
           WHERE person_type_id = pap.person_type_id)
            Employment_type,
         (SELECT pos.namE POSITION_NAME
            FROM APPS.hr_all_positions_f_tl pos
           WHERE pos.position_id = pass.position_id AND ROWNUM = 1)
            Position,
         pass.organization_id,
         paa.ASSIGNMENT_ID,
         pap.BUSINESS_GROUP_ID,
         paa.ASSIGNMENT_ACTION_ID,
         LPAD (pass.ASSIGNMENT_NUMBER, 5, ' ') assignment_number,
         pass.LOCATION_ID,
         TRIM (SUBSTR (pp.NAME,
                       (  INSTR (pp.name,
                                 '.',
                                 1,
                                 1)
                        + 1),
                       (  INSTR (TRIM (pp.name),
                                 '.',
                                 1,
                                 2)
                        - (  INSTR (pp.name,
                                    '.',
                                    1,
                                    1)
                           + 1))))
            Designation,
		 to_char(PPA.DATE_EARNED,'DD-MON-YY') DATE_EARNED,
         paa.PAYROLL_ACTION_ID,
         ppa.TIME_PERIOD_ID,
         ppa.PAYROLL_ID,
		to_char(PPA.PAY_ADVICE_DATE,'DD-MON-YY') PAY_ADVICE_DATE,
         pap.expense_check_send_to_address,
            SUBSTR (pap.first_name, 1, 1)
         || ' '
         || SUBSTR (pap.middle_names, 1, 1)
            initials,
         INITCAP (pap.TITLE) Title
  FROM apps.pay_assignment_actions paa,
         apps.pay_payroll_actions ppa,
         apps.per_all_assignments_f pass,
        apps.per_all_people_f pap,
        apps.hr_all_organization_units hr,
         apps.per_all_assignments_f paaf,
         apps.per_all_positions pp
   WHERE     paa.payroll_action_id = ppa.payroll_action_id
         AND pap.person_id = paaf.person_id
         AND paaf.position_id = pp.position_id(+)
         AND paa.assignment_id = pass.assignment_id
         AND pass.person_id = pap.person_id
         AND pass.organization_id = hr.organization_id
         AND ppa.action_type = 'R'
         AND ppa.action_status = 'C'
         AND pass.assignment_type = 'E'
         AND pass.primary_flag = 'Y'
         AND ppa.date_earned BETWEEN pass.effective_start_date
                                 AND pass.effective_end_date
         AND ppa.date_earned BETWEEN pap.effective_start_date
                                 AND pap.effective_end_date
         AND ppa.date_earned BETWEEN paaf.effective_start_date
                                 AND paaf.effective_end_date
         AND ppa.date_earned = :P_PERIOD
         AND paaf.payroll_id =
                (SELECT payroll_id
                   FROM per_all_assignments_f paaf2
                  WHERE     paaf2.assignment_id = paaf.assignment_id
                        AND TO_DATE ( :P_PERIOD) BETWEEN paaf2.effective_start_date
                                                     AND paaf2.effective_end_date)
         AND employee_number = NVL ( :P_EMPLOYEE_NUMBER, employee_number)
--AND LPAD(employee_number,6,' ') BETWEEN LPAD(:P_employee_from,6,' ') AND LPAD(:P_employee_to,6,' ')
ORDER BY LPAD (pap.employee_number, 6, ' ')
	  ]]>
  </sqlStatement>
  	<sqlStatement name="Q_MESSAGES">
	  <![CDATA[ 
			SELECT PAYROLL_ACTION_ID, PAY_ADVICE_MESSAGE
				FROM APPS.PAY_PAYROLL_ACTIONS
            WHERE payroll_action_id = :PAYROLL_ACTION_ID
		]]>
	</sqlStatement>
	<sqlStatement name="Q_DEDUCTONS">
	  <![CDATA[ 		
		  SELECT *
	  FROM (SELECT REPORT_NAME,
				   NVL(to_number(RESULT_VALUE), 0) RESULT_VALUE1,
				   ASSIGNMENT_ACTION_ID,
				   DECODE(CLASSIFICATION_NAME,'Voluntary Deductions','Statutory Deductions and Others','Statutory Deductions and Others')CLASSIFICATION_NAME
			  FROM APPS.XXAPP_PAY_ASSG_ACTIONS_V
			  WHERE CLASSIFICATION_NAME IN ('Voluntary Deductions') 
			  --and report_name in ('Ammortisation 13th Month')
								   )
	 WHERE ASSIGNMENT_ACTION_ID = :ASSIGNMENT_ACTION_ID
		]]>
	</sqlStatement>
    	<sqlStatement name="Q_TOT_DEDUCTONS">
	  <![CDATA[ 		
		 SELECT NVL(SUM(RESULT_VALUE1),0) TOTAL_DEDUCTION
	  FROM (
	  SELECT REPORT_NAME,
				   NVL(to_number(RESULT_VALUE), 0) RESULT_VALUE1,
				   ASSIGNMENT_ACTION_ID,
				   DECODE(CLASSIFICATION_NAME,'Voluntary Deductions','Statutory Deductions and Others','Statutory Deductions and Others')CLASSIFICATION_NAME
			  FROM XXAPP_PAY_ASSG_ACTIONS_V
			  WHERE CLASSIFICATION_NAME IN ('Voluntary Deductions') 
			  --and report_name in ('Ammortisation 13th Month')
								   )
	 WHERE ASSIGNMENT_ACTION_ID = :ASSIGNMENT_ACTION_ID
		]]>
	</sqlStatement>
	<sqlStatement name="Q_PAYMENT">
	  <![CDATA[ 
		select *
  from (
   SELECT REPORT_NAME,
				   NVL(to_number(RESULT_VALUE), 0) RESULT_VALUE,
				   ASSIGNMENT_ACTION_ID,
               DECODE(CLASSIFICATION_NAME,'Direct Net','Earnings,Accrued and Advance Payments','Earnings,Accrued and Advance Payments')CLASSIFICATION_NAME
			  FROM XXAPP_PAY_ASSG_ACTIONS_V
			  WHERE CLASSIFICATION_NAME IN ('Earnings') 
			  --and report_name in ('Basic Salary Retro')
   )
 where assignment_action_id = :ASSIGNMENT_ACTION_ID
		]]>
	</sqlStatement>
		<sqlStatement name="Q_TOT_PAYMENT">
	  <![CDATA[ 
		 select NVL(SUM(RESULT_VALUE1),0) TOTAL_PAYMENT
  from (
   SELECT REPORT_NAME,
				   NVL(to_number(RESULT_VALUE), 0) RESULT_VALUE1,
				   ASSIGNMENT_ACTION_ID,
               DECODE(CLASSIFICATION_NAME,'Direct Net','Earnings,Accrued and Advance Payments','Earnings,Accrued and Advance Payments')CLASSIFICATION_NAME
			  FROM XXAPP_PAY_ASSG_ACTIONS_V
			  WHERE CLASSIFICATION_NAME IN ('Earnings') 
			  --and report_name in ('Basic Salary Retro')
   )
 where assignment_action_id = :ASSIGNMENT_ACTION_ID
		]]>
	</sqlStatement>
	<sqlStatement name="Q_ACCOUNTS">
		  <![CDATA[ 
			SELECT PAYMENT_METHOD, NVL(PAYMENT_AMOUNT,0) PAYMENT_AMOUNT 
			FROM (SELECT   PAYMENT_METHOD,
					  PAYMENT_AMOUNT,
					  ASSIGNMENT_ACTION_ID				  
		FROM PAY_EXTERNAL_ACCOUNTS_V)
		where ASSIGNMENT_ACTION_ID = :ASSIGNMENT_ACTION_ID
				 ]]>
	</sqlStatement>	
	<sqlStatement name="Q_PERIOD">
	  <![CDATA[ 
	 	SELECT trim(TO_CHAR(TO_DATE(:P_PERIOD,'DD-MON-YY'), 'Month'))||','||TO_CHAR(TO_DATE(:P_PERIOD,'DD-MON-YY'), 'RRRR')  PERIOD_NAME  from dual
	         ]]>
	</sqlStatement>	
		</dataQuery>
	<dataStructure>
			<group name="G_PERSONAL" source="Q_PERSONAL">
			   <element name="TODAY_DATE" value="TODAY_DATE"/>
				<element name="STAFF_NUMBER" value="STAFF_NUMBER"/>
				<element name="EMPLOYEE_REFERENCE" value="EMPLOYEE_REFERENCE"/>
				<element name="EMPLOYEE_NUMBER" value="EMPLOYEE_NUMBER"/>
				<element name="PERSON_ID" value="PERSON_ID"/>
				<element name="LAST_NAME" value="LAST_NAME"/>
				<element name="FIRST_NAME" value="FIRST_NAME"/>
				<element name="MIDDLE_NAMES" value="MIDDLE_NAMES"/>
				<element name="FULL_NAME" value="FULL_NAME"/>
				<element name="GENDER" value="GENDER"/>
				<element name="DATE_OF_BIRTH" value="DATE_OF_BIRTH"/>
				<element name="EMAIL_ADDRESS" value="EMAIL_ADDRESS"/>
				<element name="EMPLOYMENT_DATE" value="EMPLOYMENT_DATE"/>
				<element name="COST_ALLOCATION" value="COST_ALLOCATION"/>
				<element name="ORGANIZATION" value="ORGANIZATION"/>
				<element name="ORGANIZATION_TYPE" value="ORGANIZATION_TYPE"/>
				<element name="PAYROLL_GROUP" value="PAYROLL_GROUP"/>
				<element name="LOCATION_CODE" value="LOCATION_CODE"/>
				<element name="LOCATION_ADDRESS" value="LOCATION_ADDRESS"/>
				<element name="EMPLOYMENT_TYPE" value="EMPLOYMENT_TYPE"/>
				<element name="POSITION" value="POSITION"/>
				<element name="JOB_NAME" value="JOB_NAME"/>
				<element name="EMPLOYEE_BANK" value="EMPLOYEE_BANK"/>
				<element name="NATIONAL_IDENTIFIER" value="NATIONAL_IDENTIFIER"/>
				<element name="ORGANIZATION_ID" value="ORGANIZATION_ID"/>
				<element name="ASSIGNMENT_ID" value="ASSIGNMENT_ID"/>
				<element name="BUSINESS_GROUP_ID" value="BUSINESS_GROUP_ID"/>
				<element name="ASSIGNMENT_ACTION_ID" value="ASSIGNMENT_ACTION_ID"/>
				<element name="ASSIGNMENT_NUMBER" value="ASSIGNMENT_NUMBER"/>
				<element name="LOCATION_ID" value="LOCATION_ID"/>
				<element name="DESIGNATION" value="DESIGNATION"/>
				<element name="DATE_EARNED" value="DATE_EARNED"/>
				<element name="PAYROLL_ACTION_ID" value="PAYROLL_ACTION_ID"/>
				<element name="TIME_PERIOD_ID" value="TIME_PERIOD_ID"/>
				<element name="PAYROLL_ID" value="PAYROLL_ID"/>
				<element name="PAY_ADVICE_DATE" value="PAY_ADVICE_DATE"/>
				<element name="EXPENSE_CHECK_SEND_TO_ADDRESS" value="EXPENSE_CHECK_SEND_TO_ADDRESS"/>
				<element name="INITIALS" value="INITIALS"/>
				<element name="TITLE" value="TITLE"/>
			<group name="G_MESSAGES" source="Q_MESSAGES">
					 <element name="PAYROLL_ACTION_ID" value="PAYROLL_ACTION_ID"/>
					 <element name="PAY_ADVICE_MESSAGE" value="PAY_ADVICE_MESSAGE"/>
			</group>	
		    <group name="G_DEDUCTONS" source="Q_DEDUCTONS">
					 <element name="REPORT_NAME" value="REPORT_NAME"/>
					 <element name="RESULT_VALUE1" value="RESULT_VALUE1"/>
					 <!-- <element name="ASSIGNMENT_ACTION_ID" value="ASSIGNMENT_ACTION_ID"/> -->
					 <element name="CLASSIFICATION_NAME" value="CLASSIFICATION_NAME"/>
			</group>
            <group name="G_TOT_DEDUCTONS" source="Q_TOT_DEDUCTONS">
					 <element name="TOTAL_DEDUCTION" value="TOTAL_DEDUCTION"/>
			</group>
			<group name="G_PAYMENT" source="Q_PAYMENT">
					 <element name="REPORT_NAME" value="REPORT_NAME"/>
					 <element name="RESULT_VALUE" value="RESULT_VALUE"/>
					 <!-- <element name="ASSIGNMENT_ACTION_ID" value="ASSIGNMENT_ACTION_ID"/> -->
					 <element name="CLASSIFICATION_NAME" value="CLASSIFICATION_NAME"/>
			</group>
			<group name="G_TOT_PAYMENT" source="Q_TOT_PAYMENT">
					 <element name="TOTAL_PAYMENT" value="TOTAL_PAYMENT"/>
        	</group>
			<group name="G_ACCOUNTS" source="Q_ACCOUNTS">
			    	 <element name="PAYMENT_METHOD" value="PAYMENT_METHOD"/>
					 <element name="PAYMENT_AMOUNT" value="PAYMENT_AMOUNT"/>
					 <!--  <element name="ASSIGNMENT_ACTION_ID" value="ASSIGNMENT_ACTION_ID"/> -->
			</group>
			<group name="G_PERIOD" source="Q_PERIOD">
					 <element name="PERIOD_NAME" value="PERIOD_NAME"/>
			</group>
			</group>
			
		</dataStructure>
</dataTemplate >