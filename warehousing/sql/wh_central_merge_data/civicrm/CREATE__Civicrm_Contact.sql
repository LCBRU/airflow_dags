CREATE TABLE civicrm__contact (
    id INT NOT NULL PRIMARY KEY,
    first_name VARCHAR(100),
    middle_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(10),
    birth_date DATE,
    nhs_number VARCHAR(255),
    uhl_system_number VARCHAR(255)
)

INSERT INTO civicrm__contact(id, first_name, middle_name, last_name, gender, birth_date, nhs_number, uhl_system_number)
SELECT
    cc.id,
    cc.first_name,
    cc.middle_name,
    cc.last_name,
    g.label AS gender,
    cc.birth_date,
    cvci.nhs_number_1 AS nhs_number,
    cvci.uhl_s_number_2 AS uhl_system_number
FROM datalake_civicrm.dbo.civicrm_contact cc
LEFT JOIN datalake_civicrm.dbo.civicrm_option_value g
    ON g.value = cc.gender_id
    AND g.option_group_id = 3
LEFT JOIN datalake_civicrm.dbo.civicrm_value_contact_ids_1 cvci
    ON cvci.entity_id = cc.id 
WHERE cc.id IN (
    SELECT DISTINCT contact_id
    FROM datalake_civicrm.dbo.civicrm_case_contact ccc 
) AND cc.is_deleted = 0
