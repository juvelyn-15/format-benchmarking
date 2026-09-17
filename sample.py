input_file = "./data/raw/flights.csv"
output_file = "./data/raw/flights_1mb.csv"

target_size = 1 * 1024 * 1024  # 1 MB

size = 0

with open(input_file, "rb") as infile, open(output_file, "wb") as outfile:
    for line in infile:
        outfile.write(line)
        size += len(line)

        if size >= target_size:
            break

print(f"Created: {output_file}")
print(f"Size: {size / (1024 * 1024):.2f} MB")