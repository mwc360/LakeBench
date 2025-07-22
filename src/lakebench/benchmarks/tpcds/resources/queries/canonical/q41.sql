SELECT DISTINCT
  (
    i_product_name
  )
FROM item AS i1
WHERE
  i_manufact_id BETWEEN 953 AND 953 + 40
  AND (
    SELECT
      COUNT(*) AS item_cnt
    FROM item
    WHERE
      (
        i_manufact = i1.i_manufact
        AND (
          (
            i_category = 'Women'
            AND (
              i_color = 'yellow' OR i_color = 'coral'
            )
            AND (
              i_units = 'Unknown' OR i_units = 'Dozen'
            )
            AND (
              i_size = 'large' OR i_size = 'medium'
            )
          )
          OR (
            i_category = 'Women'
            AND (
              i_color = 'green' OR i_color = 'navajo'
            )
            AND (
              i_units = 'Pound' OR i_units = 'Dram'
            )
            AND (
              i_size = 'economy' OR i_size = 'small'
            )
          )
          OR (
            i_category = 'Men'
            AND (
              i_color = 'violet' OR i_color = 'ivory'
            )
            AND (
              i_units = 'Lb' OR i_units = 'Box'
            )
            AND (
              i_size = 'extra large' OR i_size = 'N/A'
            )
          )
          OR (
            i_category = 'Men'
            AND (
              i_color = 'seashell' OR i_color = 'chartreuse'
            )
            AND (
              i_units = 'Tsp' OR i_units = 'Case'
            )
            AND (
              i_size = 'large' OR i_size = 'medium'
            )
          )
        )
      )
      OR (
        i_manufact = i1.i_manufact
        AND (
          (
            i_category = 'Women'
            AND (
              i_color = 'cornsilk' OR i_color = 'cream'
            )
            AND (
              i_units = 'Tbl' OR i_units = 'Ounce'
            )
            AND (
              i_size = 'large' OR i_size = 'medium'
            )
          )
          OR (
            i_category = 'Women'
            AND (
              i_color = 'red' OR i_color = 'orange'
            )
            AND (
              i_units = 'Bundle' OR i_units = 'Carton'
            )
            AND (
              i_size = 'economy' OR i_size = 'small'
            )
          )
          OR (
            i_category = 'Men'
            AND (
              i_color = 'hot' OR i_color = 'indian'
            )
            AND (
              i_units = 'Ton' OR i_units = 'Oz'
            )
            AND (
              i_size = 'extra large' OR i_size = 'N/A'
            )
          )
          OR (
            i_category = 'Men'
            AND (
              i_color = 'gainsboro' OR i_color = 'peach'
            )
            AND (
              i_units = 'Bunch' OR i_units = 'N/A'
            )
            AND (
              i_size = 'large' OR i_size = 'medium'
            )
          )
        )
      )
  ) > 0
ORDER BY
  i_product_name