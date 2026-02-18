SELECT a.contract_id, zipcode, tariff_code,
 power_kw, client_type_description FROM (SELECT *
FROM kettle_lab.con_contract_dim) a
left join ( SELECT *  FROM eae.con_client_type_dim) b
on a.client_type_id=b.client_type_id;