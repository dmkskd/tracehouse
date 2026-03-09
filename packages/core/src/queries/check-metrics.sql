-- Run this to see available metrics in asynchronous_metric_log
SELECT DISTINCT metric 
FROM system.asynchronous_metric_log 
WHERE metric LIKE '%CPU%' OR metric LIKE '%Load%' OR metric LIKE '%Memory%' OR metric LIKE '%Read%' OR metric LIKE '%Write%' OR metric LIKE '%Disk%'
ORDER BY metric;
