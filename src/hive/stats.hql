select 
 a.status, 
 sum(a.bytesize), 
 100 * (sum(a.bytesize) / b.total)
from 
 loggy_man a, 
 (select sum(bytesize) total from loggy_man) b 
-- where host = '190.144.111.59'
group by a.status, b.total;


