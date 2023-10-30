
select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 819 and 819+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'midnight' or i_color = 'pale') and 
        (i_units = 'Bundle' or i_units = 'Case') and
        (i_size = 'small' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'navy' or i_color = 'navajo') and
        (i_units = 'Gross' or i_units = 'Tsp') and
        (i_size = 'petite' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'sandy' or i_color = 'cornsilk') and
        (i_units = 'Cup' or i_units = 'Oz') and
        (i_size = 'economy' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'beige' or i_color = 'bisque') and
        (i_units = 'Gram' or i_units = 'Tbl') and
        (i_size = 'small' or i_size = 'extra large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'thistle' or i_color = 'brown') and 
        (i_units = 'Unknown' or i_units = 'Each') and
        (i_size = 'small' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'almond' or i_color = 'dodger') and
        (i_units = 'N/A' or i_units = 'Carton') and
        (i_size = 'petite' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'lime' or i_color = 'burlywood') and
        (i_units = 'Bunch' or i_units = 'Pallet') and
        (i_size = 'economy' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'light' or i_color = 'plum') and
        (i_units = 'Dram' or i_units = 'Lb') and
        (i_size = 'small' or i_size = 'extra large')
        )))) > 0
 order by i_product_name
 limit 100;


