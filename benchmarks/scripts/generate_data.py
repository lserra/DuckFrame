import csv
import os
import random

base = os.path.dirname(os.path.abspath(__file__))
proj = os.path.join(base, "..", "..")
data_dir = os.path.join(proj, "benchmarks", "data")
os.makedirs(data_dir, exist_ok=True)

names = [
    "Alice",
    "Bob",
    "Carol",
    "David",
    "Eve",
    "Frank",
    "Grace",
    "Hank",
    "Iris",
    "Jack",
    "Karen",
    "Leo",
    "Mia",
    "Noah",
    "Olivia",
    "Paul",
    "Quinn",
    "Rita",
    "Sam",
    "Tina",
]
countries = [
    "Brazil",
    "USA",
    "Germany",
    "Japan",
    "Canada",
    "UK",
    "France",
    "India",
    "Australia",
    "Mexico",
]
departments = [
    "Engineering",
    "Sales",
    "Marketing",
    "Finance",
    "HR",
    "Support",
    "Product",
    "Legal",
]

rng = random.Random(42)
for n in [100_000, 1_000_000]:
    fn = os.path.join(data_dir, f"bench_{n//1000}k.csv")
    print(f"Generating {fn} ({n} rows)...")
    with open(fn, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "age", "country", "department", "salary"])
        for i in range(n):
            w.writerow(
                [
                    i + 1,
                    rng.choice(names),
                    22 + rng.randint(0, 39),
                    rng.choice(countries),
                    rng.choice(departments),
                    f"{40000 + rng.random() * 120000:.2f}",
                ]
            )
    print(f"  Done: {fn}")

fn = os.path.join(data_dir, "departments.csv")
print(f"Generating {fn}...")
with open(fn, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["department", "budget", "location"])
    for d, b, l in zip(
        departments,
        [5000000, 3000000, 2500000, 4000000, 1500000, 2000000, 3500000, 1800000],
        ["SF", "NYC", "London", "SF", "NYC", "Berlin", "Tokyo", "NYC"],
    ):
        w.writerow([d, b, l])
print(f"  Done: {fn}")
print("ALL DONE")
