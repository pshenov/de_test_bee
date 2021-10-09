# 1
SELECT emp_w.emp_name, emp_w.job_name, emp_w.dep_id, emp_w.salary as worker_salary, emp_m.salary as manager_salary
FROM employees emp_w, employees emp_m
WHERE emp_m.emp_id = emp_w.manager_id AND emp_w.salary > emp_m.salary

# 2
SELECT emp_1.emp_name, emp_1.job_name, emp_1.dep_id, emp_1.salary
FROM employees emp_1
WHERE emp_1.salary = (SELECT MIN(salary) FROM employees emp_2
                      WHERE emp_2.dep_id = emp_1.dep_id)
ORDER BY emp_1.salary DESC

# 3
SELECT dep_id
FROM employees
GROUP BY dep_id
HAVING COUNT(*) > 3

# 4
SELECT emp_1.emp_name, emp_1.job_name, dep.dep_name
FROM employees emp_1
JOIN department dep ON emp_1.dep_id = dep.dep_id
WHERE NOT EXISTS (SELECT NULL FROM employees emp_2
                  WHERE emp_1.manager_id = emp_2.emp_id
                  AND emp_1.dep_id=emp_2.dep_id)

# 5
SELECT emp_name, job_name, dep_id, (current_date - hire_date) as experience,
RANK() OVER( PARTITION BY dep_id ORDER BY (current_date - hire_date) DESC)
from employees

# 6
SELECT CASE
    WHEN Salary >= 800 AND Salary <= 1300  THEN 1
    WHEN Salary >= 1301 AND Salary <=1500  THEN 2
    WHEN salary >= 1501 AND salary <= 2100 THEN 3
    WHEN salary >= 2101 AND salary <= 3100 THEN 4
    WHEN salary >= 3101 AND salary <= 9999 THEN 5
    ELSE 0
END as grade, COUNT(*) count
FROM employees
GROUP BY CASE
    WHEN Salary >= 800 AND Salary <= 1300  THEN 1
    WHEN Salary >= 1301 AND Salary <=1500  THEN 2
    WHEN salary >= 1501 AND salary <= 2100 THEN 3
    WHEN salary >= 2101 AND salary <= 3100 THEN 4
    WHEN salary >= 3101 AND salary <= 9999 THEN 5
    ELSE 0
END
ORDER BY grade



