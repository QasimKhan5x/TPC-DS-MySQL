select  s_store_name
      ,sum(ss_net_profit)
 from store_sales
     ,date_dim
     ,store,
     (select ca_zip
     from (
      SELECT substr(ca_zip,1,5) ca_zip
      FROM customer_address
      WHERE substr(ca_zip,1,5) IN (
                          '81435','22224','90010','44621','14827','42861',
                          '18290','66294','50218','82186','39234',
                          '43509','12113','84118','28161','91672',
                          '79498','90380','68940','65276','58728',
                          '25762','15856','46157','95850','28142',
                          '69218','63831','43583','23137','46114',
                          '91989','16297','15568','11649','83176',
                          '33342','57677','89661','59962','96145',
                          '58698','38399','59056','46570','12350',
                          '96877','86884','16341','91349','23696',
                          '10032','83116','93914','73352','13126',
                          '28269','97294','23711','22086','51149',
                          '69386','22472','69598','77028','22508',
                          '60551','10196','33344','53740','65930',
                          '68851','21764','17961','66695','92896',
                          '36458','13069','62794','36852','98833',
                          '95891','80325','70927','25326','51369',
                          '64630','37559','11471','79669','28873',
                          '13403','37619','42458','56057','58128',
                          '70074','17366','90284','60877','62809',
                          '55815','38819','29248','14126','22267',
                          '72960','27028','36115','44073','86724',
                          '17407','85410','41258','27849','16872',
                          '90083','99972','25247','23432','24671',
                          '40688','16949','24737','51355','61201',
                          '10514','53791','67024','47989','12712',
                          '46862','15563','78456','45391','10550',
                          '46251','96023','73207','40344','84966',
                          '47023','88100','98045','98346','32290',
                          '38646','36074','51601','95139','41803',
                          '10237','21339','70728','21836','64545',
                          '21237','75452','99189','26942','35179',
                          '86313','24821','55410','73421','21192',
                          '20427','18055','78378','49679','21258',
                          '21610','22571','25115','13255','64735',
                          '48631','48444','35463','68482','95703',
                          '24467','51074','70406','89906','77348',
                          '94364','96585','22201','57051','22756',
                          '79292','79696','12433','65194','39136',
                          '31532','74137','20873','40387','27821',
                          '54310','70830','40753','97401','38735',
                          '65389','84800','57511','62676','51634',
                          '94492','27866','45775','14513','86048',
                          '74917','33296','21996','75517','73132',
                          '22262','46498','17239','70452','29499',
                          '62593','83072','92137','69140','33990',
                          '88582','60993','36041','17474','47398',
                          '35766','70985','17105','26383','93622',
                          '27239','24545','88370','55742','35937',
                          '74603','70204','41637','14008','79502',
                          '25447','97233','50330','46248','81660',
                          '76411','53266','65127','28944','65204',
                          '73035','23438','36620','66315','94545',
                          '96715','47745','30230','86539','12998',
                          '19229','13853','23980','40819','14958',
                          '45682','12779','21102','47687','17244',
                          '12798','60093','26455','30255','53966',
                          '59987','48379','26540','51666','16451',
                          '72188','89700','63817','48482','66519',
                          '99644','21519','58527','10486','90632',
                          '30352','22345','61477','47247','97474',
                          '82044','82256','50910','36794','45102',
                          '23774','74041','77188','16066','74208',
                          '91153','29981','56750','20782','82176',
                          '67074','82037','36416','78006','65912',
                          '71176','93590','66716','35051','38269',
                          '69962','76831','11123','13744','24950',
                          '10781','60925','41726','29305','70196',
                          '60855','51524','92950','30395','87908',
                          '40708','47535','34060','55570','66693',
                          '18720','65555','77810','25630','94553',
                          '22879','52404','32932','41443','51649',
                          '15877','26714','44155','65136','87422',
                          '75258','97669','46032','11082','22104',
                          '60833','89419','41627','73977','21900',
                          '99009','36713','64883','57000','68542',
                          '50541','62648','57399','21745','48948',
                          '94852','67586','61919','14902','24471',
                          '53271','14631','44393','13462','13313',
                          '35892','49873','98593','23882')
     intersect
      select ca_zip
      from (SELECT substr(ca_zip,1,5) ca_zip,count(*) cnt
            FROM customer_address, customer
            WHERE ca_address_sk = c_current_addr_sk and
                  c_preferred_cust_flag='Y'
            group by ca_zip
            having count(*) > 10)A1)A2) V1
 where ss_store_sk = s_store_sk
  and ss_sold_date_sk = d_date_sk
  and d_qoy = 2 and d_year = 2002
  and (substr(s_zip,1,2) = substr(V1.ca_zip,1,2))
 group by s_store_name
 order by s_store_name
 limit 100;