with open("tests\\time_space_eval\\results.log", "r") as file:
    for line in file:
        if line.startswith("app-1  |"):
            print(line, end="")