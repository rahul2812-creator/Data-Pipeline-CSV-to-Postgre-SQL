df.write \
.format("jdbc") \
.option("url", "jdbc:postgresql://postgres:5432/employee_db") \
.option("dbtable", "employees_clean") \
.option("user", "admin") \
.option("password", "admin") \
.option("driver", "org.postgresql.Driver") \
.mode("append") \
.save()