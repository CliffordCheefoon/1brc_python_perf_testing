def read_and_split_file(input_file, output_file, lines_count=100_000_000):
    """Read the top N lines from input file and write to output file."""
    with (
        open(input_file, "r", encoding="utf-8") as infile,
        open(output_file, "w", encoding="utf-8") as outfile,
    ):
        for i, line in enumerate(infile):
            if i >= lines_count:
                break
            outfile.write(line)


if __name__ == "__main__":
    input_path = "data/measurements.txt"
    output_path = "data/measurements_100_mill.txt"
    read_and_split_file(input_path, output_path)
