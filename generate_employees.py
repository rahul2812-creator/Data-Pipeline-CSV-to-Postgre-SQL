from faker import Faker
import random, csv
from datetime import datetime, timedelta


fake = Faker()


with open('employees_raw.csv', 'w', newline='') as f:
writer = csv.writer(f)
writer.writerow([
'employee_id','first_name','last_name','email','hire_date','job_title',
'department','salary','manager_id','address','city','state','zip_code',
'birth_date','status'
])


for i in range(1000):
hire_date = fake.date_between(start_date='-5y', end_date='+2y')
salary = random.choice([
f"${random.randint(40000,120000):,}",
random.randint(40000,120000),
None
])
email = random.choice([
fake.email(),
fake.first_name() + '@company',
fake.email().upper()
])


writer.writerow([
1000+i,
fake.first_name(),
fake.last_name().upper(),
email,
hire_date,
fake.job(),
random.choice(['IT','HR','Finance','Analytics']),
salary,
random.choice([None,2001,2002]),
fake.address(),
fake.city(),
fake.state_abbr(),
fake.postcode(),
fake.date_of_birth(minimum_age=22, maximum_age=60),
random.choice(['Active','Inactive'])
])