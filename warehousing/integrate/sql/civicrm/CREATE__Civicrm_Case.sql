CREATE TABLE civicrm__case (
    id INT NOT NULL PRIMARY KEY,
    case_type_id INT,
    case_type_name VARCHAR(100),
    contact_id INT,
    start_date DATE,
    case_status_id INT,
    case_status_name VARCHAR(255),
    FOREIGN KEY (contact_id) REFERENCES civicrm__contact(id),
)

INSERT INTO civicrm__case(id, case_type_id, contact_id, case_type_name, start_date, case_status_id, case_status_name)
SELECT
    c.id,
    c.case_type_id,
    ccc.contact_id,
    cct.name AS case_type_name,
    c.start_date,
    c.status_id AS case_status_id,
    cs.label AS case_status_name
FROM datalake_civicrm.dbo.civicrm_case c
JOIN datalake_civicrm.dbo.civicrm_case_type cct
    ON cct.id = c.case_type_id
JOIN datalake_civicrm.dbo.civicrm_case_contact ccc 
    ON ccc.case_id = c.id
JOIN datalake_civicrm.dbo.civicrm_contact con
    ON con.id = ccc.contact_id
    AND con.is_deleted = 0
JOIN datalake_civicrm.dbo.civicrm_option_value cs
    ON cs.value = c.status_id 
    AND cs.option_group_id = 27
WHERE c.is_deleted = 0
;
