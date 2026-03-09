\# Data Product: clickstream



Owner: Matvei Rayko  

Domain: clickstream / recommendations  

Source data:

\- Kafka topic `clicks`



Intermediate data:

\- Kafka topic `aggregated\_clicks`



Output data:

\- Redis key `top\_products`

\- Redis key `top\_products\_list`



Description:

Информационный продукт clickstream хранит события, связанные с кликами пользователей, агрегированные данные о окнах и ранжированные продукты для предоставления рекомендаций.

