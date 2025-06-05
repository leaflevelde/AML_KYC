
import csv
import random
from faker import Faker
from datetime import datetime
import sys
import os


def get_date_from_args():
    if len(sys.argv) > 1:
        try:
            # Try to parse the date argument
            date = datetime.strptime(sys.argv[1], "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            print("Invalid date format. Use YYYY-MM-DD.")
            sys.exit(1)
    else:
        # Use current date if no argument is provided
        date = datetime.now().strftime("%Y-%m-%d")
    return date


rep_date = get_date_from_args()


fake = Faker()
Faker.seed(0)

# Function to generate a person-type customer record
def generate_person_customer(rep_date):
    citizenship = "US" if random.random() < 0.9 else fake.country()
    # Split the address into components
    address = fake.address().split("\n")
    street = address[0]
    city_state_zip = address[1]
    country = 'United States of America'

    return {
        "uuid": fake.uuid4(),
        "full_name": fake.name(),
        "street": street,
        "city_state_zip": city_state_zip,
       # "state": state,
       # "zip_code": zip,
        "country": country,
        "occupation": fake.job(),
        "start_date": rep_date,
        "customer_type": "Person",
        "ssn": fake.ssn().replace("-", ""),
        "phone_number": fake.phone_number(),
        "email": fake.email(),
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=90),
        "citizenship": citizenship,
        "as_rep_date": rep_date,
        
    }

# Function to generate a bank account-type record
def generate_bank_account(rep_date):
    account_types = ["Checking", "Savings", "CreditCard", "AutoLoan", "StudentLoan"]
    source_system_codes = [fake.lexify("??").upper() for _ in range(10)]
    
    #hack to ensure account numbers are unique across multiple runs
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")  # Current date and time to milliseconds
    random_digits = str(fake.unique.random_number(digits=4, fix_len=True))
    account_number = f"{timestamp}{random_digits}"  # Combine timestamp and random digits
    #
    return {
        "account_number": account_number,
        #"open_date": fake.date_between(start_date='-30y', end_date='today'),
        "open_date": rep_date,
        "account_type": random.choice(account_types),
        "source_system_code": random.choice(source_system_codes),
        "account_mailing_address": fake.address().replace("\n", ", "),
        "account_status": "Open",
        "as_rep_date": rep_date
    }

# Function to create relationships between customers and accounts. 75% will be single owner, 25% will be joint owners
def generate_relationships(customers, accounts, rep_date):
    relationships = []
    for account in accounts:
        if random.random() < 0.75:
            customer = random.choice(customers)
            relationships.append({
                "customer_number": customer["uuid"],
                "account_number": account["account_number"],
                "start_date": rep_date,
                "end_date": None,
                "relationship_type": "Owner"
            })
        else:
            customer1, customer2 = random.sample(customers, 2)
            relationships.append({
                "customer_number": customer1["uuid"],
                "account_number": account["account_number"],
                "start_date": rep_date,
                "end_date": None,
                "relationship_type": "Joint Owner"
            })
            relationships.append({
                "customer_number": customer2["uuid"],
                "account_number": account["account_number"],
                "start_date": rep_date,
                "end_date": None,
                "relationship_type": "Joint Owner"
            })
    return relationships

# Main script
if __name__ == "__main__":
    number_of_persons = 100
    number_of_accounts = 200

    # Generate data
    customers = [generate_person_customer(rep_date) for _ in range(number_of_persons)]
    accounts = [generate_bank_account(rep_date) for _ in range(number_of_accounts)]
    relationships = generate_relationships(customers, accounts, rep_date)




    # Export to CSV
    output_dir = "/tmp/"
    with open(os.path.join(output_dir, "customers.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=customers[0].keys())
        writer.writeheader()
        writer.writerows(customers)

    with open(os.path.join(output_dir, "accounts.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=accounts[0].keys())
        writer.writeheader()
        writer.writerows(accounts)

    with open(os.path.join(output_dir, "relationships.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=relationships[0].keys())
        writer.writeheader()
        writer.writerows(relationships)

    print("Data generation complete. Files saved: customers.csv, accounts.csv, relationships.csv")



